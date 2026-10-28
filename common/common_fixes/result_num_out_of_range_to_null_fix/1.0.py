"""Result-num out-of-range to null fix (v1.0).

Nulls out ``RESULT_NUM`` values in LAB_RESULT_CM that cannot be safely summed
by the PCORnet EDC Data Characterization (DC) SAS harness.  SAS stores numeric
values as 8-byte IEEE-754 doubles (max magnitude ~1.8e308).  The DC part-2
``LAB_L3_LOINC_RESULT_NUM`` query runs ``PROC MEANS SUM(RESULT_NUM)`` per LOINC
group over the entire (very large) table.  When a group contains ``inf``,
``nan``, or extreme finite magnitudes, the running sum overflows and SAS raises
``ERROR: A floating point exception has occurred``, aborting the DC run.

Upstream formatters treat RESULT_NUM as a free-form numeric passthrough.
Python's ``float()`` accepts ``"1E400"`` (-> ``inf``), ``"nan"``, and huge
finite values, so these can reach RESULT_NUM verbatim.  This fix scrubs them.

A RESULT_NUM value is set to null when ANY of the following holds:

    * It is non-empty but does not parse as a double (garbage numeric text).
    * It parses to ``NaN``.
    * Its absolute value exceeds ``MAX_ABS_RESULT_NUM`` (covers ``inf`` and
      any implausibly large finite magnitude).

Legitimately-null and in-range values are left untouched.  No rows are
deleted.  A diagnostics file records the rows whose RESULT_NUM was nulled.

Parameters
----------
src1 : str
    Not used.  The fix always operates on the hardcoded ``RESULT_NUM`` field.
src2 : str
    Not used.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from commonFunctions import CommonFuncitons

# Values with magnitude above this are treated as out-of-range and nulled.
# Real lab RESULT_NUM values never approach this; it leaves enormous headroom
# below the SAS double overflow point (~1.8e308) even when summed across
# hundreds of millions of rows.
MAX_ABS_RESULT_NUM: float = 1e15


def _result_num_is_out_of_range(column: Column) -> Column:
    """Return a boolean Column that is True when RESULT_NUM must be nulled.

    A value is out of range when it is non-empty but unparseable as a double,
    parses to NaN, or has an absolute value exceeding ``MAX_ABS_RESULT_NUM``
    (which also captures positive/negative infinity).  Null and empty values
    return False (they are left as-is by the caller).

    Args:
        column: The RESULT_NUM Column to test.

    Returns:
        A boolean Column expression.
    """
    trimmed = F.trim(column)
    as_double = trimmed.cast("double")

    is_present = column.isNotNull() & (trimmed != "")
    fails_to_parse = as_double.isNull()
    is_nan = F.isnan(as_double)
    is_too_large = F.abs(as_double) > F.lit(MAX_ABS_RESULT_NUM)

    return is_present & (fails_to_parse | is_nan | is_too_large)


cf: CommonFuncitons = CommonFuncitons("NotUsed")

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-t", "--table")
parser.add_argument("-p", "--partner")
parser.add_argument("-ftm", "--fixed_table_name")
parser.add_argument("-i", "--input_path")
parser.add_argument("-o", "--output_path")
parser.add_argument("-sr1", "--src1")
parser.add_argument("-sr2", "--src2")

args = parser.parse_args()
input_data_folder: str = args.data_folder
examined_table_name: str = args.table
input_partner_name: str = args.partner
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark: SparkSession = cf.get_spark_session("result_num_out_of_range_to_null_fix")

this_fix_name: str = "result_num_out_of_range_to_null_fix_1.0"
examined_table_name_parsed: str = examined_table_name.replace("mapped_", "").replace(".csv", "")

try:
    cf.print_fixer_status(
        current_count="**",
        total_count="**",
        fix_type="common",
        fix_name=this_fix_name,
    )

    lab_df = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    out_of_range = _result_num_is_out_of_range(F.col("RESULT_NUM"))

    # Capture the rows we are about to null for diagnostics before mutating.
    modified_rows = lab_df.filter(out_of_range)

    lab_df = lab_df.withColumn(
        "RESULT_NUM",
        F.when(out_of_range, F.lit(None)).otherwise(F.col("RESULT_NUM")),
    )

    # Cache to materialize before writing (prevents EOFException from overwrite).
    lab_df = lab_df.cache()
    lab_df.count()

    cf.write_pyspark_output_file(
        payspark_df=modified_rows,
        output_file_name=f"{examined_table_name.replace('.csv', '')}_result_num_out_of_range_nulled_.csv",
        output_data_folder_path=(
            output_data_folder_path
            + "/fixers_output/"
            + examined_table_name_parsed
            + "_fixer/"
            + this_fix_name
            + "/"
        ),
    )

    # Write the corrected table back.
    cf.write_pyspark_output_file(
        payspark_df=lab_df,
        output_file_name=fixed_table_name,
        output_data_folder_path=output_data_folder_path,
    )

    spark.stop()

except Exception as e:
    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job=this_fix_name,
        text=str(e),
    )
