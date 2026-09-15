"""Norm range normalization fix (v1.0).

Normalizes the four NORM fields (NORM_MODIFIER_LOW, NORM_MODIFIER_HIGH,
NORM_RANGE_LOW, NORM_RANGE_HIGH) in LAB_RESULT_CM so that they satisfy
the DC_3_10 "fully specified normal range" patterns.

DC_3_10 requires one of three valid patterns:
    1. Both bounds:  NORM_MODIFIER_LOW = EQ, NORM_MODIFIER_HIGH = EQ,
                     NORM_RANGE_LOW not null, NORM_RANGE_HIGH not null
    2. Lower-only:   NORM_MODIFIER_LOW in (GT, GE), NORM_MODIFIER_HIGH = NO,
                     NORM_RANGE_LOW not null, NORM_RANGE_HIGH is null
    3. Upper-only:   NORM_MODIFIER_HIGH in (LT, LE), NORM_MODIFIER_LOW = NO,
                     NORM_RANGE_HIGH not null, NORM_RANGE_LOW is null

The fix applies four sequential transformation rules:

    Rule 1 -- Both ranges numeric with NI modifiers: set both modifiers to EQ.
    Rule 2 -- Both modifiers EQ but only one range populated: convert to
              one-sided pattern (GE/NO for lower-only, NO/LE for upper-only).
    Rule 3 -- Upper-only pattern cleanup: if NORM_MODIFIER_HIGH is LT/LE and
              NORM_RANGE_LOW is NI/null/empty, set NORM_RANGE_LOW = null and
              NORM_MODIFIER_LOW = NO.
    Rule 4 -- Lower-only pattern cleanup: if NORM_MODIFIER_LOW is GT/GE and
              NORM_RANGE_HIGH is NI/null/empty, set NORM_RANGE_HIGH = null and
              NORM_MODIFIER_HIGH = NO.

Rules 1, 3, 4 are direct translations of the SQL fixes from the PCORnet EDC
DC_3_10 remediation guidance.  Rule 2 handles the case where an upstream fix
(e.g. ``result_modifier_309_fix``) has already set both modifiers to EQ
regardless of whether both range values exist.

Parameters
----------
src1 : str
    Not used.  The fix always operates on the four hardcoded NORM field names.
src2 : str
    Not used.

The fix does not delete any rows.  It only modifies NORM_MODIFIER and
NORM_RANGE field values.  A diagnostics file records which rows were modified.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from commonFunctions import CommonFuncitons


def _is_numeric_value(column: Column) -> Column:
    """Return a boolean Column that is True when the value is a parseable number.

    Uses ``cast('double')`` to detect numeric strings, matching the semantics
    of SQL Server's ``ISNUMERIC()``.  Null, empty, and non-numeric strings
    return False.

    Args:
        column: The PySpark Column to test.

    Returns:
        A boolean Column expression.
    """
    return F.trim(column).cast("double").isNotNull()


def _is_null_or_empty_or_ni(column: Column) -> Column:
    """Return a boolean Column that is True when the value is null, empty, whitespace-only, or 'NI'.

    This matches the SQL fix's treatment of ``NORM_RANGE_LOW = 'NI'`` as
    equivalent to null (no value present).

    Args:
        column: The PySpark Column to test.

    Returns:
        A boolean Column expression.
    """
    return (
        column.isNull()
        | (F.trim(column) == "")
        | (F.upper(F.trim(column)) == "NI")
    )


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

spark: SparkSession = cf.get_spark_session("norm_range_normalization_fix")

this_fix_name: str = "norm_range_normalization_fix_1.0"
examined_table_name_parsed: str = examined_table_name.replace("mapped_", "").replace(".csv", "")

try:
    cf.print_fixer_status(
        current_count="**",
        total_count="**",
        fix_type="common",
        fix_name=this_fix_name,
    )

    lab_df = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    # Snapshot rows before modification so we can capture what changed for diagnostics.
    # We tag each row with a hash of the 4 NORM fields to detect changes after all rules.
    original_norm_cols = F.concat_ws(
        "|",
        F.coalesce(F.col("NORM_MODIFIER_LOW"), F.lit("__NULL__")),
        F.coalesce(F.col("NORM_MODIFIER_HIGH"), F.lit("__NULL__")),
        F.coalesce(F.col("NORM_RANGE_LOW"), F.lit("__NULL__")),
        F.coalesce(F.col("NORM_RANGE_HIGH"), F.lit("__NULL__")),
    )
    lab_df = lab_df.withColumn("_norm_snapshot", original_norm_cols)

    # -------------------------------------------------------------------------
    # Rule 1: Both ranges numeric, both modifiers NI -> set both modifiers EQ
    # SQL: WHERE ISNUMERIC(NORM_RANGE_LOW)=1 AND ISNUMERIC(NORM_RANGE_HIGH)=1
    #        AND NORM_MODIFIER_HIGH='NI' AND NORM_MODIFIER_LOW='NI'
    #      SET NORM_MODIFIER_HIGH='EQ', NORM_MODIFIER_LOW='EQ'
    # -------------------------------------------------------------------------
    rule_1 = (
        _is_numeric_value(F.col("NORM_RANGE_LOW"))
        & _is_numeric_value(F.col("NORM_RANGE_HIGH"))
        & (F.col("NORM_MODIFIER_LOW") == "NI")
        & (F.col("NORM_MODIFIER_HIGH") == "NI")
    )

    lab_df = lab_df.withColumn(
        "NORM_MODIFIER_LOW",
        F.when(rule_1, F.lit("EQ")).otherwise(F.col("NORM_MODIFIER_LOW")),
    ).withColumn(
        "NORM_MODIFIER_HIGH",
        F.when(rule_1, F.lit("EQ")).otherwise(F.col("NORM_MODIFIER_HIGH")),
    )

    # -------------------------------------------------------------------------
    # Rule 2: Both modifiers EQ but only one range populated -> one-sided
    #
    # 2a: EQ/EQ with NORM_RANGE_LOW numeric, NORM_RANGE_HIGH null/empty
    #     -> NORM_MODIFIER_LOW = GE, NORM_MODIFIER_HIGH = NO
    # 2b: EQ/EQ with NORM_RANGE_HIGH numeric, NORM_RANGE_LOW null/empty
    #     -> NORM_MODIFIER_LOW = NO, NORM_MODIFIER_HIGH = LE
    # -------------------------------------------------------------------------
    both_eq = (F.col("NORM_MODIFIER_LOW") == "EQ") & (F.col("NORM_MODIFIER_HIGH") == "EQ")

    rule_2a = both_eq & _is_numeric_value(F.col("NORM_RANGE_LOW")) & _is_null_or_empty_or_ni(F.col("NORM_RANGE_HIGH"))
    rule_2b = both_eq & _is_numeric_value(F.col("NORM_RANGE_HIGH")) & _is_null_or_empty_or_ni(F.col("NORM_RANGE_LOW"))

    lab_df = (
        lab_df
        .withColumn(
            "NORM_MODIFIER_LOW",
            F.when(rule_2a, F.lit("GE"))
            .when(rule_2b, F.lit("NO"))
            .otherwise(F.col("NORM_MODIFIER_LOW")),
        )
        .withColumn(
            "NORM_MODIFIER_HIGH",
            F.when(rule_2a, F.lit("NO"))
            .when(rule_2b, F.lit("LE"))
            .otherwise(F.col("NORM_MODIFIER_HIGH")),
        )
        .withColumn(
            "NORM_RANGE_HIGH",
            F.when(rule_2a, F.lit(None))
            .otherwise(F.col("NORM_RANGE_HIGH")),
        )
        .withColumn(
            "NORM_RANGE_LOW",
            F.when(rule_2b, F.lit(None))
            .otherwise(F.col("NORM_RANGE_LOW")),
        )
    )

    # -------------------------------------------------------------------------
    # Rule 3: Upper-only cleanup
    # SQL: WHERE (NORM_MODIFIER_HIGH='LT' OR NORM_MODIFIER_HIGH='LE')
    #        AND NORM_RANGE_LOW='NI'
    #      SET NORM_RANGE_LOW=NULL, NORM_MODIFIER_LOW='NO'
    # -------------------------------------------------------------------------
    rule_3 = (
        F.col("NORM_MODIFIER_HIGH").isin("LT", "LE")
        & _is_null_or_empty_or_ni(F.col("NORM_RANGE_LOW"))
    )

    lab_df = lab_df.withColumn(
        "NORM_RANGE_LOW",
        F.when(rule_3, F.lit(None)).otherwise(F.col("NORM_RANGE_LOW")),
    ).withColumn(
        "NORM_MODIFIER_LOW",
        F.when(rule_3, F.lit("NO")).otherwise(F.col("NORM_MODIFIER_LOW")),
    )

    # -------------------------------------------------------------------------
    # Rule 4: Lower-only cleanup
    # SQL: WHERE (NORM_MODIFIER_LOW='GT' OR NORM_MODIFIER_LOW='GE')
    #        AND NORM_RANGE_HIGH='NI'
    #      SET NORM_RANGE_HIGH=NULL, NORM_MODIFIER_HIGH='NO'
    # -------------------------------------------------------------------------
    rule_4 = (
        F.col("NORM_MODIFIER_LOW").isin("GT", "GE")
        & _is_null_or_empty_or_ni(F.col("NORM_RANGE_HIGH"))
    )

    lab_df = lab_df.withColumn(
        "NORM_RANGE_HIGH",
        F.when(rule_4, F.lit(None)).otherwise(F.col("NORM_RANGE_HIGH")),
    ).withColumn(
        "NORM_MODIFIER_HIGH",
        F.when(rule_4, F.lit("NO")).otherwise(F.col("NORM_MODIFIER_HIGH")),
    )

    # -------------------------------------------------------------------------
    # Diagnostics: capture rows that were actually modified
    # -------------------------------------------------------------------------
    new_norm_cols = F.concat_ws(
        "|",
        F.coalesce(F.col("NORM_MODIFIER_LOW"), F.lit("__NULL__")),
        F.coalesce(F.col("NORM_MODIFIER_HIGH"), F.lit("__NULL__")),
        F.coalesce(F.col("NORM_RANGE_LOW"), F.lit("__NULL__")),
        F.coalesce(F.col("NORM_RANGE_HIGH"), F.lit("__NULL__")),
    )

    modified_rows = lab_df.filter(F.col("_norm_snapshot") != new_norm_cols)

    # Cache to materialize before writing (prevents EOFException from overwrite)
    lab_df = lab_df.drop("_norm_snapshot").cache()
    row_count = lab_df.count()

    modified_for_diag = modified_rows.drop("_norm_snapshot")

    cf.write_pyspark_output_file(
        payspark_df=modified_for_diag,
        output_file_name=f"{examined_table_name.replace('.csv', '')}_norm_range_normalized_.csv",
        output_data_folder_path=(
            output_data_folder_path
            + "/fixers_output/"
            + examined_table_name_parsed
            + "_fixer/"
            + this_fix_name
            + "/"
        ),
    )

    # Write the corrected table back
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
