"""Bad code to type-override fix (v1.0).

For rows whose code column matches a curated bad-code list, set the
corresponding type column to a configurable target value. The list
lives in ``common/lists/<src1>.csv`` (header-less, one code per line)
and the target columns + replacement value are passed via ``src2`` as
a comma-separated triple ``CODE_COLUMN,TYPE_COLUMN,TARGET_VALUE``.

This fix was originally created to flip ``OBSGEN_TYPE`` to
``"UD_LC_COMPONENT"`` on OBS_GEN rows whose ``OBSGEN_CODE`` is in
the ``bad_obsgen_codes`` list (initial entry: LOINC ``LG41856-2``,
"Smoking-related terms"), but the implementation is generic and can be
reused for any (table, code-column, type-column, replacement-value)
combination by supplying a different list name and ``src2`` triple.

Behavior
--------
* Reads ``common/lists/<src1>.csv`` (one code per line, no header).
* Reads the current fixed table at ``output_path + fixed_table_name``.
* For rows where ``CODE_COLUMN`` is in the list, overwrites
  ``TYPE_COLUMN`` with the literal string ``TARGET_VALUE``. All other
  rows pass through unchanged.
* Writes a diagnostics CSV containing the matching rows in their
  pre-modification state (so the original ``TYPE_COLUMN`` value is
  preserved for audit).

Parameters
----------
src1 : str
    Bad-code list name without ``.csv`` extension (e.g.
    ``bad_obsgen_codes``).
src2 : str
    Comma-separated ``CODE_COLUMN,TYPE_COLUMN,TARGET_VALUE`` triple
    (e.g. ``OBSGEN_CODE,OBSGEN_TYPE,UD_LC_COMPONENT``).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from commonFunctions import CommonFuncitons
import argparse


cf: CommonFuncitons = CommonFuncitons('NotUsed')

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
source_list_name: str = args.src1
src2_parts: list = args.src2.split(",")
code_column: str = src2_parts[0].strip()
type_column: str = src2_parts[1].strip()
target_value: str = src2_parts[2].strip()
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark: SparkSession = cf.get_spark_session("bad_obsgen_codes_to_ot_fix")

this_fix_name: str = 'common_bad_obsgen_codes_to_ot_fix_1.0'
examined_table_name_parsed: str = examined_table_name.replace('mapped_', '').replace('.csv', '')

try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=f"{this_fix_name} ({source_list_name} -> {target_value})"
    )

    # Read the curated bad-code list from common/lists/<src1>.csv (header-less,
    # one code per line). Mirrors the loading idiom used by list_remover_fix.
    source_list_path: str = f'common/lists/{source_list_name}.csv'
    source_list_df = spark.read.load(
        source_list_path,
        format="csv", inferSchema="false", header="false", quote='"'
    )
    source_list: list = [row['_c0'] for row in source_list_df.collect()]

    # Read the current fixed table.
    examined_table = spark.read.load(
        output_data_folder_path + fixed_table_name,
        format="csv", sep="\t", inferSchema="false", header="true", quote='"'
    )

    # Identify rows whose code is in the bad-code list. These rows will have
    # their type column overwritten with the target value.
    matching_condition = col(code_column).isin(source_list)

    # Capture the matching rows in their pre-modification state for audit.
    matching_rows = examined_table.filter(matching_condition)

    # Overwrite the type column with the target value for matching rows;
    # leave all other rows unchanged.
    fixed_table = examined_table.withColumn(
        type_column,
        when(matching_condition, lit(target_value)).otherwise(col(type_column))
    )

    # Write the diagnostics file (pre-modification state of changed rows).
    cf.write_pyspark_output_file(
        payspark_df=matching_rows,
        output_file_name=f"{examined_table_name.replace('.csv', '')}_set_to_{target_value}_{source_list_name}.csv",
        output_data_folder_path=output_data_folder_path + "/fixers_output/"
                                + examined_table_name_parsed + "_fixer/" + this_fix_name + "/"
    )

    # Write the corrected table back (overwrites the fixed file).
    cf.write_pyspark_output_file(
        payspark_df=fixed_table,
        output_file_name=fixed_table_name,
        output_data_folder_path=output_data_folder_path
    )

    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job=this_fix_name,
        text=str(e)
    )
