

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import col, when, lit
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
input_data_folder         = args.data_folder
examined_table_name       = args.table
input_partner_name        = args.partner
date_fields_csv           = args.src1
encounter_file_name       = args.src2
input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name
spark = cf.get_spark_session("copy_admit_date_or_delete")

this_fix_name = 'copy_admit_date_or_delete_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')
date_fields = [field.strip() for field in date_fields_csv.split(',')]


try:

    cf.print_fixer_status(

        current_count = '**',
        total_count   = '**',
        fix_type      = 'common',
        fix_name      = this_fix_name

        )

    examined_table = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    encounter_table = cf.spark_read(input_data_folder_path + encounter_file_name, spark)

    encounter_lookup = encounter_table.select('ENCOUNTERID', 'ADMIT_DATE').dropDuplicates(['ENCOUNTERID'])

    encounter_lookup = encounter_lookup.withColumnRenamed('ADMIT_DATE', '_ADMIT_DATE_LOOKUP')

    original_columns = examined_table.columns

    joined_df = examined_table.join(encounter_lookup, on='ENCOUNTERID', how='left')

    fixed_df = joined_df
    for date_field in date_fields:
        fixed_df = fixed_df.withColumn(
            date_field,
            when(col(date_field).isNull(), col('_ADMIT_DATE_LOOKUP')).otherwise(col(date_field))
        )

    null_condition = None
    for date_field in date_fields:
        field_is_null = col(date_field).isNull()
        if null_condition is None:
            null_condition = field_is_null
        else:
            null_condition = null_condition | field_is_null

    bad_rows = fixed_df.filter(null_condition).select(original_columns)
    good_rows = fixed_df.filter(~null_condition).select(original_columns)


    cf.write_pyspark_output_file(
                            payspark_df = good_rows,
                            output_file_name = fixed_table_name,
                            output_data_folder_path = output_data_folder_path)


    cf.write_pyspark_output_file(
                            payspark_df = bad_rows,
                            output_file_name = f"{examined_table_name.replace('.csv', '')}_null_{'_'.join(date_fields)}_.csv",
                            output_data_folder_path = output_data_folder_path + "/fixers_output/" + examined_table_name_parsed + "_fixer/" + this_fix_name + "/")



    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner_name.lower(),
                            job     = this_fix_name,
                            text    = str(e))
