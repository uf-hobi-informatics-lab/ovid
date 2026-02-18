

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
spark = cf.get_spark_session("encounterid_orphans_fix")

this_fix_name = 'common_encounterid_orphans_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')


try:

    cf.print_fixer_status(

        current_count = '**',
        total_count   ='**',
        fix_type      = 'common', 
        fix_name      = this_fix_name
        
        )

    encounter_table         = spark.read.load(input_data_folder_path+encounter_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    small_encounter_table         = encounter_table.select (

                                    encounter_table["ENCOUNTERID"].alias('SOURCE_ENCOUNTERID')
                                        )

                                                                                            
                                                                                                            
                                                                                                        

    input_table             = spark.read.load(output_data_folder_path+fixed_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

                                                                                                        


    joined_df = input_table.join(small_encounter_table, input_table['ENCOUNTERID']==small_encounter_table['SOURCE_ENCOUNTERID'], how="left")


    bad_records_with_counts = (
        joined_df.filter(col('SOURCE_ENCOUNTERID').isNull())
                .groupBy('ENCOUNTERID')
                .agg(count("*").alias("count")))


    fixed_df = joined_df.withColumn(
        "ENCOUNTERID",
        when(col("SOURCE_ENCOUNTERID").isNull(), lit(None)).otherwise(col("ENCOUNTERID"))
    ).drop("SOURCE_ENCOUNTERID")
    
    fixed_df = fixed_df.select(input_table.columns)


    cf.write_pyspark_output_file(
                            payspark_df = bad_records_with_counts,
                            output_file_name = f"{examined_table_name.replace('.csv','')}_missing_encounterids_.csv" ,
                            output_data_folder_path= output_data_folder_path+ "/fixers_output/"+examined_table_name_parsed+"_fixer/"+this_fix_name+"/")
                                                                                 

    cf.write_pyspark_output_file(
                            payspark_df = fixed_df,
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


