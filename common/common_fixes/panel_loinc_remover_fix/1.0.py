"""Panel LOINC remover fix (DC_1_20).

Removes rows from a CDM table where the LOINC code is classified as a panel-type
LOINC. Panel-order LOINC codes represent orders for groups of tests (e.g., "CBC
panel") rather than individual results. The PCORnet EDC DC_1_20 check flags tables
where panel LOINCs exceed 5% of all LOINC-coded records.

Panel source — PINNED SNAPSHOT
---------------------------------------------------
This fix reads the panel-LOINC set from a **pinned snapshot CSV** in
``common/lists/<src1>.csv`` (header-less, one LOINC per line) rather than from the
live Snowflake reference ``CHARACTERIZATION.EDC_REF.LOINC_REF_RAW``.

Rationale: a live LOINC reference can be re-classified between refresh cycles --
individual-result LOINCs (e.g. 3137-7 respiratory rate, 9279-1, 39156-5 BMI,
8479-8, ...) may become flagged ``PANEL_TYPE='Panel'``. Driving the fix from a live
reference then deletes legitimate individual-result rows and produces DC 4.01 /
DC 4.03 persistence exceptions. Pinning the panel set (only 85354-9 "Blood
pressure panel") keeps the behavior stable: panel rows are removed (DC 1.20) while
no individual-result codes are dropped.

Parameters via argparse:
    -sr1 (src1): Pinned panel-LOINC list name (without ``.csv``), resolved against
                 ``common/lists/<src1>.csv`` (e.g. "panel_loinc_snapshot").
    -sr2 (src2): Comma-separated pair "CODE_COLUMN,TYPE_COLUMN" identifying the
                 LOINC code column and the type column used to filter for LOINC-coded
                 rows (e.g., "OBSCLIN_CODE,OBSCLIN_TYPE" for OBS_CLIN,
                 "LAB_LOINC,LAB_LOINC" for LAB_RESULT_CM).
                 Only rows where TYPE_COLUMN == 'LC' are evaluated for panel removal.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, broadcast
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
panel_list_name: str = args.src1
src2_parts: list = args.src2.split(",")
code_column: str = src2_parts[0].strip()
type_column: str = src2_parts[1].strip()
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark: SparkSession = cf.get_spark_session("panel_loinc_remover_fix")

this_fix_name: str = 'common_panel_loinc_remover_fix_1.0'
examined_table_name_parsed: str = examined_table_name.replace('mapped_', '').replace('.csv', '')

# The type value that indicates LOINC-coded rows in PCORnet CDM
LOINC_TYPE_VALUE: str = 'LC'


try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=this_fix_name
    )

    # Read the current fixed table
    fixed_table = spark.read.load(
        output_data_folder_path + fixed_table_name,
        format="csv", sep="\t", inferSchema="false", header="true", quote='"'
    )

    # Read the PINNED panel-LOINC snapshot from common/lists/<src1>.csv
    # (header-less, one LOINC per line). Mirrors the loading idiom used by
    # list_remover_fix, replacing the previous live Snowflake reference query.
    panel_list_path: str = f'common/lists/{panel_list_name}.csv'
    panel_list_df = spark.read.load(
        panel_list_path, format="csv", inferSchema="false", header="false", quote='"'
    )
    panel_loinc_codes: list = [row['_c0'] for row in panel_list_df.collect()]

    # Split: only evaluate LOINC-coded rows (TYPE_COLUMN == 'LC') for panel removal.
    # Non-LOINC rows pass through unconditionally.
    loinc_rows = fixed_table.filter(upper(col(type_column)) == LOINC_TYPE_VALUE)
    non_loinc_rows = fixed_table.filter(
        (col(type_column).isNull()) | (upper(col(type_column)) != LOINC_TYPE_VALUE)
    )

    # Among LOINC rows, separate panel rows (in the pinned list) from non-panel rows.
    panel_rows = loinc_rows.filter(col(code_column).isin(panel_loinc_codes))
    non_panel_loinc_rows = loinc_rows.filter(
        (col(code_column).isNull()) | (~col(code_column).isin(panel_loinc_codes))
    )

    # Combine non-LOINC rows + non-panel LOINC rows = the kept rows
    good_rows = non_loinc_rows.unionByName(non_panel_loinc_rows)

    # Write the cleaned table back (overwrites the fixed file)
    cf.write_pyspark_output_file(
        payspark_df=good_rows,
        output_file_name=fixed_table_name,
        output_data_folder_path=output_data_folder_path
    )

    # Write removed panel rows to diagnostics for audit
    cf.write_pyspark_output_file(
        payspark_df=panel_rows,
        output_file_name=f"{examined_table_name.replace('.csv', '')}_removed_panel_loincs.csv",
        output_data_folder_path=output_data_folder_path + "/fixers_output/" + examined_table_name_parsed + "_fixer/" + this_fix_name + "/"
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
