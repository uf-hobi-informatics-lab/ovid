
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-p", "--partner_name")

args = parser.parse_args()
input_data_folder = args.data_folder
partner_name = args.partner_name
cf =CommonFuncitons(partner_name)
# Create SparkSession
spark = cf.get_spark_session("ovid")



try:

    ###################################################################################################################################
    
    # Loading the condition_occurrence table to be converted to the condition table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    condition_table_name   = '*Condition*'




    ###################################################################################################################################

    # This function will determine the condition status based on the condition end date
    ###################################################################################################################################





    ###################################################################################################################################

    #Converting the fileds to PCORNet condition Format

    ###################################################################################################################################

    try:
        condition_in = spark.read.load(input_data_folder_path+condition_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        condition = condition_in.select( 


                                condition_in['conditionid'].alias('CONDITIONID'),
                                condition_in['patid'].alias('PATID'),
                                condition_in['encounterid'].alias('ENCOUNTERID'),
                                cf.format_date_udf(condition_in['report_date']).alias('REPORT_DATE'),
                                cf.format_date_udf(condition_in['resolve_date']).alias('RESOLVE_DATE'),
                                cf.format_date_udf(condition_in['onset_date']).alias('ONSET_DATE'),
                                condition_in['condition_status'].alias('CONDITION_STATUS'),
                                condition_in['raw_condition'].alias('CONDITION'),
                                condition_in['condition_type'].alias('CONDITION_TYPE'),
                                condition_in['condition_source'].alias('CONDITION_SOURCE'),
                                condition_in['raw_condition_status'].alias('RAW_CONDITION_STATUS'),
                                condition_in['raw_condition_type'].alias('RAW_CONDITION_TYPE'),
                                condition_in['raw_condition_source'].alias('RAW_CONDITION_SOURCE'),
                                condition_in['raw_condition'].alias('RAW_CONDITION'),

        )
    except:

        condition_in = spark.read.load(input_data_folder_path+condition_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        condition = condition_in.select( 


                                condition_in['CONDITIONID'].alias('CONDITIONID'),
                                condition_in['PATID'].alias('PATID'),
                                condition_in['ENCOUNTERID'].alias('ENCOUNTERID'),
                                cf.format_date_udf(condition_in['REPORT_DATE']).alias('REPORT_DATE'),
                                cf.format_date_udf(condition_in['RESOLVE_DATE']).alias('RESOLVE_DATE'),
                                cf.format_date_udf(condition_in['ONSET_DATE']).alias('ONSET_DATE'),
                                condition_in['CONDITION_STATUS'].alias('CONDITION_STATUS'),
                                condition_in['RAW_CONDITION'].alias('CONDITION'),
                                condition_in['CONDITION_TYPE'].alias('CONDITION_TYPE'),
                                condition_in['CONDITION_SOURCE'].alias('CONDITION_SOURCE'),
                                condition_in['RAW_CONDITION_STATUS'].alias('RAW_CONDITION_STATUS'),
                                condition_in['RAW_CONDITION_TYPE'].alias('RAW_CONDITION_TYPE'),
                                condition_in['RAW_CONDITION_SOURCE'].alias('RAW_CONDITION_SOURCE'),
                                condition_in['RAW_CONDITION'].alias('RAW_CONDITION'),

        )



    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = condition,
                        output_file_name = "formatted_condition.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'condition_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')


