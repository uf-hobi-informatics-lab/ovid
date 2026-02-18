

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
provider_table_name      = args.src1
provider_field_name      = args.src2

input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name
spark = cf.get_spark_session("providerid_orphans_fix")

this_fix_name = 'common_providerid_orphans_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')


try:

    cf.print_fixer_status(

        current_count = '**',
        total_count   ='**',
        fix_type      = 'common', 
        fix_name      = this_fix_name
        
        )


    provider_table         = spark.read.load(input_data_folder_path+provider_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    
    small_provider_table         = provider_table.select (

                                    provider_table["PROVIDERID"].alias('SOURCE_PROVIDERID')
                                     )
    
                                                                                           
                                                                                                           
                                                                                                      

    input_table             = spark.read.load(output_data_folder_path+fixed_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

                                                                                                     


    joined_df = input_table.join(small_provider_table, input_table[provider_field_name]==small_provider_table['SOURCE_PROVIDERID'], how="left")


    # bad_records = joined_df.filter(col('SOURCE_PROVIDERID').isNull()).select(provider_field_name)
    # bad_records_with_counts = bad_records.groupBy(provider_field_name).count()

    bad_records_with_counts = (
        joined_df.filter(col('SOURCE_PROVIDERID').isNull())
                .groupBy(provider_field_name)
                .agg(count("*").alias("count")))


    # fixed_df = joined_df.drop(provider_field_name)

    # fixed_df =  fixed_df.withColumnRenamed( 'SOURCE_PROVIDERID', provider_field_name)

                                 
    # original_columns = input_table.columns
    # updated_columns = [provider_field_name if c == "SOURCE_PROVIDERID" else c for c in original_columns]
    # fixed_df = fixed_df.select(*updated_columns)


    fixed_df = joined_df.withColumn(
        provider_field_name,
        when(col("SOURCE_PROVIDERID").isNull(), lit(None)).otherwise(col(provider_field_name))
    ).drop("SOURCE_PROVIDERID")
    
    fixed_df = fixed_df.select(input_table.columns)


    cf.write_pyspark_output_file(
                            payspark_df = bad_records_with_counts,
                            output_file_name = f"{examined_table_name.replace('.csv','')}_missing_providerids_.csv" ,
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
                            text = str(e)
                            )
