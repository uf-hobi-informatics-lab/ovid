"""Null date to HARVEST refresh fix (v1.0).

Backfills a null date field with the corresponding HARVEST REFRESH date.
This is the mirror image of ``future_date_to_null_fix``: where that fix
*nullifies* dates beyond the HARVEST cutoff, this fix *populates* null
dates with the HARVEST cutoff.

A common use case is ENROLLMENT.ENR_END_DATE: a null
``observation_period_end_date`` in OMOP typically means the patient is
still under active observation.  Setting ENR_END_DATE to the HARVEST
REFRESH_ENROLLMENT_DATE means "enrolled through at least the data
delivery cutoff", which ensures the row survives the EDC's lookback
period filter.

Parameters
----------
src1 : str
    The name of the date column to backfill (e.g. ``ENR_END_DATE``).
src2 : str
    The name of the HARVEST REFRESH field whose value will be used as
    the backfill date (e.g. ``REFRESH_ENROLLMENT_DATE``).

The fix always reads ``mapped_harvest.csv`` from the mapper output path
(``input_path``) to obtain the backfill date.  Only rows where the
target date column is null are modified; non-null values are left
unchanged.  A diagnostics file records which rows were backfilled.
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
date_field_name: str = args.src1
harvest_refresh_field: str = args.src2
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark: SparkSession = cf.get_spark_session("null_date_to_harvest_refresh_fix")

this_fix_name: str = 'null_date_to_harvest_refresh_fix_1.0'
examined_table_name_parsed: str = examined_table_name.replace('mapped_', '').replace('.csv', '')

HARVEST_FILE_NAME: str = 'mapped_harvest.csv'

try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=f"{this_fix_name} ({date_field_name})"
    )

    # Read the current fixed table
    examined_table = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    # Read the harvest table (single-row) from mapper output to get the backfill date
    harvest_table = cf.spark_read(input_data_folder_path + HARVEST_FILE_NAME, spark)
    harvest_row = harvest_table.select(harvest_refresh_field).first()

    if harvest_row is None or harvest_row[0] is None:
        # No harvest date available -- nothing to backfill with, pass through unchanged
        cf.write_pyspark_output_file(
            payspark_df=examined_table,
            output_file_name=fixed_table_name,
            output_data_folder_path=output_data_folder_path
        )
        spark.stop()

    else:
        harvest_date_str: str = str(harvest_row[0])

        # Identify rows where the date field is null
        null_date_condition = col(date_field_name).isNull()

        # Capture rows that will be modified (for diagnostics) -- before modification
        null_rows = examined_table.filter(null_date_condition)

        # Set the date field to the harvest refresh date where it is null
        fixed_table = examined_table.withColumn(
            date_field_name,
            when(
                null_date_condition,
                lit(harvest_date_str)
            ).otherwise(col(date_field_name))
        )

        # Write diagnostics -- rows that had null dates (original values, before backfill)
        cf.write_pyspark_output_file(
            payspark_df=null_rows,
            output_file_name=f"{examined_table_name.replace('.csv', '')}_null_{date_field_name}_.csv",
            output_data_folder_path=output_data_folder_path + "/fixers_output/"
                                    + examined_table_name_parsed + "_fixer/" + this_fix_name + "/"
        )

        # Write the corrected table back
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
