

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse


cf =CommonFuncitons('NotUsed')


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
field_to_examine_name     = args.src1
input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name
spark = cf.get_spark_session("illogical_dates_more_than_5_days_fix")

this_fix_name = 'custom_illogical_dates_more_than_5_days_fix1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')


try:

    cf.print_fixer_status(

        current_count = '**',
        total_count   ='**',
        fix_type      = 'custom', 
        fix_name      = this_fix_name
        
        )

    examined_table                = cf.spark_read(output_data_folder_path+fixed_table_name,spark)


    fixed_table = examined_table.withColumn(field_to_examine_name,when(datediff(col("ADMIT_DATE"), col(field_to_examine_name)) > 5, col("ADMIT_DATE")).otherwise(col(field_to_examine_name)))
                                                                                                                                                                                                        
    bad_rows = examined_table.filter(datediff(col("ADMIT_DATE"), col(field_to_examine_name)) > 5)


    cf.write_pyspark_output_file(
                            payspark_df = bad_rows,
                            output_file_name = f"{examined_table_name.replace('.csv','')}_null_{field_to_examine_name}_.csv" ,
                            output_data_folder_path= output_data_folder_path+ "/fixers_output/"+examined_table_name_parsed+"_fixer/"+this_fix_name+"/")

                                                                                 

    cf.write_pyspark_output_file(
                            payspark_df = fixed_table,
                            output_file_name = fixed_table_name ,
                            output_data_folder_path= output_data_folder_path)



    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner_name.lower(),
                            job     = this_fix_name,
                            text    = str(e))


