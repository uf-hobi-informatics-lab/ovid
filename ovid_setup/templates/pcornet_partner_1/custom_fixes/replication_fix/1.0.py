

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
encounter_table_name      = args.src1
input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name
spark = cf.get_spark_session("patid_orphans_fix")

this_fix_name = 'custom_replication_fix_1.0'

examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')

try:



    encounter_table         = spark.read.load(input_data_folder_path+encounter_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    
    small_encounter_table         = encounter_table.select (

                                    encounter_table["ENCOUNTERID"].alias('SOURCE_ENCOUNTERID'),
                                    encounter_table["ADMIT_DATE"].alias('SOURCE_ADMIT_DATE'),
                                    encounter_table["ENC_TYPE"].alias('SOURCE_ENC_TYPE'),
                                     )
    
                                                                                           
                                                                                                            

    input_table             = spark.read.load(input_data_folder_path+examined_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')



    joined_df = input_table.join(small_encounter_table, input_table['ENCOUNTERID']==small_encounter_table['SOURCE_ENCOUNTERID'], how="inner")



    fixed_df = joined_df.withColumn('ADMIT_DATE', col("SOURCE_ADMIT_DATE")).withColumn('ENC_TYPE', col("SOURCE_ENC_TYPE")).drop('SOURCE_ENC_TYPE').drop('SOURCE_ADMIT_DATE').drop('SOURCE_ENCOUNTERID')

    bad_records = joined_df.filter(
                        (col("ENC_TYPE") != col("SOURCE_ENC_TYPE")) |
                        (col("ADMIT_DATE") != col("SOURCE_ADMIT_DATE")))

                                                                                              


    cf.write_pyspark_output_file(
                            payspark_df = fixed_df,
                            output_file_name = fixed_table_name ,
                            output_data_folder_path= output_data_folder_path)


    cf.write_pyspark_output_file(
                            payspark_df = bad_records,
                            output_file_name = f"{examined_table_name.replace('.csv','')}_rows_with_replications_issues_.csv" ,
                            output_data_folder_path= output_data_folder_path+ "/fixers_output/"+examined_table_name_parsed+"_fixer/"+this_fix_name+"/")





    cf.print_fixer_status(

        current_count = '**',
        total_count   ='**',
        fix_type      = 'custom', 
        fix_name      = this_fix_name

        )


    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner_name.lower(),
                            job     = this_fix_name)

    cf.print_with_style(str(e), 'danger red')



