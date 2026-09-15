"""Future date to null fix (v1.0).

Sets a date field to null if its value is after the corresponding HARVEST
REFRESH date for the table.  The HARVEST refresh date represents the data
delivery cutoff -- any date beyond it is illogical and should be nullified.

Parameters
----------
src1 : str
    The name of the date column to examine (e.g. ``EXT_PAT_START_DATE``).
src2 : str
    The name of the HARVEST REFRESH field to use as the cutoff date
    (e.g. ``REFRESH_PRESCRIBING_DATE``).

The fix always reads ``mapped_harvest.csv`` from the mapper output path
(``input_path``) to obtain the cutoff date.  Rows whose date exceeds the
cutoff have that single field set to null; the rows themselves are *not*
deleted.  A diagnostics file records which rows were modified.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, when
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
input_data_folder = args.data_folder
examined_table_name = args.table
input_partner_name = args.partner
date_field_name = args.src1
harvest_refresh_field = args.src2
input_data_folder_path = args.input_path
output_data_folder_path = args.output_path
fixed_table_name = args.fixed_table_name

spark = cf.get_spark_session("future_date_to_null_fix")

this_fix_name = 'future_date_to_null_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

HARVEST_FILE_NAME = 'mapped_harvest.csv'

try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=f"{this_fix_name} ({date_field_name})"
    )

    # Read the current fixed table
    examined_table = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    # Read the harvest table (single-row) from mapper output to get the cutoff date
    harvest_table = cf.spark_read(input_data_folder_path + HARVEST_FILE_NAME, spark)
    harvest_row = harvest_table.select(harvest_refresh_field).first()

    if harvest_row is None or harvest_row[0] is None:
        # No harvest date available -- nothing to compare against, pass through unchanged
        cf.write_pyspark_output_file(
            payspark_df=examined_table,
            output_file_name=fixed_table_name,
            output_data_folder_path=output_data_folder_path
        )
        spark.stop()

    else:
        harvest_date_str = str(harvest_row[0])
        harvest_date_lit = to_date(lit(harvest_date_str))

        # Identify rows where the date field is after the harvest refresh date
        future_date_condition = to_date(col(date_field_name)) > harvest_date_lit

        # Capture rows that will be modified (for diagnostics) -- before modification
        bad_rows = examined_table.filter(
            col(date_field_name).isNotNull() & future_date_condition
        )

        # Set the date field to null where it exceeds the harvest cutoff
        fixed_table = examined_table.withColumn(
            date_field_name,
            when(
                col(date_field_name).isNotNull() & future_date_condition,
                lit(None)
            ).otherwise(col(date_field_name))
        )

        # Write diagnostics -- rows that had future dates (original values, before nullification)
        cf.write_pyspark_output_file(
            payspark_df=bad_rows,
            output_file_name=f"{examined_table_name.replace('.csv', '')}_future_{date_field_name}_.csv",
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
