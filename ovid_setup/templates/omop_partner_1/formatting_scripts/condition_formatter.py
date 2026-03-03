###################################################################################################################################

# This script will convert an OMOP condition_occurrence table to a PCORnet format as the condition table

###################################################################################################################################

import pyspark
from pyspark import SparkConf, SparkContext
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

    ## Using person since partner is submitting OMOP data, can also use wildcards for multiple files with the same name
    condition_occurrence_table_name       = 'Condition_Occurrence.txt'

    condition_occurrence = spark.read.load(input_data_folder_path+condition_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

   
    filter_values = ["EHR problem list entry"] # Only rows where condition_data_origin is in this list will convert to the CONDITION table
    filtered_condition_occurrence = condition_occurrence.filter(col("condition_data_origin").isin(filter_values))



    ###################################################################################################################################

    # This function will determine the condition status based on the condition end date
    ###################################################################################################################################



    def PCORI_condition_status( condition_end_date):
            if condition_end_date is None or condition_end_date == '':
                return "AC"
            else:
                return "RS"

    PCORI_condition_status_udf = udf(PCORI_condition_status, StringType())


    ###################################################################################################################################

    #Converting the fileds to PCORNet condition Format

    ###################################################################################################################################

    condition = filtered_condition_occurrence.select(  
                                                    filtered_condition_occurrence['condition_occurrence_id'].alias("CONDITIONID"),
                                                    filtered_condition_occurrence['person_id'].alias("PATID"),
                                                    filtered_condition_occurrence['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    filtered_condition_occurrence['condition_start_date'].alias("REPORT_DATE"),
                                                    filtered_condition_occurrence['condition_end_date'].alias("RESOLVE_DATE"),
                                                    lit("").alias("ONSET_DATE"),
                                                    PCORI_condition_status_udf(filtered_condition_occurrence['condition_end_date']).alias("CONDITION_STATUS"),
                                                    filtered_condition_occurrence['condition_code'].alias("CONDITION"),
                                                    filtered_condition_occurrence['condition_code_type'].alias("CONDITION_TYPE"),
                                                    lit("HC").alias("CONDITION_SOURCE"),
                                                    filtered_condition_occurrence['condition_status'].alias("RAW_CONDITION_STATUS"),
                                                    filtered_condition_occurrence['condition_code'].alias("RAW_CONDITION"),
                                                    filtered_condition_occurrence['condition_code'].alias("RAW_CONDITION_TYPE"),
                                                    filtered_condition_occurrence['poa'].alias("RAW_CONDITION_SOURCE"),

                                                
                                                    
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








