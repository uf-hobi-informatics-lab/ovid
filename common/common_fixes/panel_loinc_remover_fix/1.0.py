"""Panel LOINC remover fix (DC_1_20).

Removes rows from a CDM table where the LOINC code is classified as a panel-type
LOINC in the EDC reference table (CHARACTERIZATION.EDC_REF.LOINC_REF_RAW).

Panel-order LOINC codes represent orders for groups of tests (e.g., "CBC panel")
rather than individual results. The PCORnet EDC DC_1_20 check flags tables where
panel LOINCs exceed a threshold percentage of all LOINC-coded records.

Parameters via argparse:
    -sr1 (src1): Snowflake DB server (e.g., "your-account.region")
    -sr2 (src2): Comma-separated pair "CODE_COLUMN,TYPE_COLUMN" identifying the
                 LOINC code column and the type column used to filter for LOINC-coded
                 rows (e.g., "OBSCLIN_CODE,OBSCLIN_TYPE" for OBS_CLIN,
                 "LAB_LOINC,LAB_LOINC" for LAB_RESULT_CM).
                 Only rows where TYPE_COLUMN == 'LC' are evaluated for panel removal.
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, broadcast
from commonFunctions import CommonFuncitons
import argparse


cf = CommonFuncitons('NotUsed')

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
input_data_folder       = args.data_folder
examined_table_name     = args.table
input_partner_name      = args.partner
db_server               = args.src1
src2_parts              = args.src2.split(",")
code_column             = src2_parts[0].strip()
type_column             = src2_parts[1].strip()
input_data_folder_path  = args.input_path
output_data_folder_path = args.output_path
fixed_table_name        = args.fixed_table_name

spark = cf.get_spark_session("panel_loinc_remover_fix")

this_fix_name = 'common_panel_loinc_remover_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

# Snowflake coordinates for the EDC LOINC reference table
SF_DB_NAME     = 'CHARACTERIZATION'
SF_SCHEMA_NAME = 'EDC_REF'
SF_TABLE_NAME  = 'LOINC_REF_RAW'

# The type value that indicates LOINC-coded rows in PCORnet CDM
LOINC_TYPE_VALUE = 'LC'


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

    # Query the EDC LOINC reference table from Snowflake
    loinc_ref = cf.read_from_snowflake(
        db_server=db_server,
        db_name=SF_DB_NAME,
        schema_name=SF_SCHEMA_NAME,
        table_name=SF_TABLE_NAME
    )

    # Extract panel-type LOINC codes from the reference
    panel_loincs = (
        loinc_ref
        .filter(upper(col("PANEL_TYPE")) == "PANEL")
        .select(col("LOINC_NUM").alias("PANEL_LOINC_NUM"))
        .distinct()
    )

    # Broadcast the panel LOINC list since it is much smaller than the main table
    panel_loincs_bc = broadcast(panel_loincs)

    # Split: only evaluate LOINC-coded rows (TYPE_COLUMN == 'LC') for panel removal.
    # Non-LOINC rows pass through unconditionally.
    loinc_rows = fixed_table.filter(upper(col(type_column)) == LOINC_TYPE_VALUE)
    non_loinc_rows = fixed_table.filter(
        (col(type_column).isNull()) | (upper(col(type_column)) != LOINC_TYPE_VALUE)
    )

    # Among LOINC rows, separate panel rows from non-panel rows
    panel_rows = loinc_rows.join(
        panel_loincs_bc,
        loinc_rows[code_column] == panel_loincs_bc["PANEL_LOINC_NUM"],
        how="inner"
    ).drop("PANEL_LOINC_NUM")

    non_panel_loinc_rows = loinc_rows.join(
        panel_loincs_bc,
        loinc_rows[code_column] == panel_loincs_bc["PANEL_LOINC_NUM"],
        how="left_anti"
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
