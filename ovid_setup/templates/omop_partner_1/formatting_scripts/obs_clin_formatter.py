###################################################################################################################################

# This script will convert an OMOP observation table to a PCORnet format as the obs_clin table

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

    ###################################################################################################################################

    # Loading the observation table to be converted to the obs_clin table

###################################################################################################################################

  input_data_folder_path               = f'/data/{input_data_folder}/'
  formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


  observation_table_name   = 'Observation.txt'

  observation = spark.read.load(input_data_folder_path+observation_table_name,format="csv", sep=",", inferSchema="false", header="true", quote= '"')

  filter_values = ["Observation from Measurement", "Observation from measurement"] # Only rows where observation_data_origin is in this list will convert to obsclin
  filtered_observation = observation.filter(col("observation_data_origin").isin(filter_values))


    ###################################################################################################################################
  def return_if_float(val):
        if val is None:
            return None
        else:
            try:
                return float(val)
            except (ValueError, TypeError):
                return None


  return_if_float_udf = udf(return_if_float, StringType())

  ###################################################################################################################################
  
  def check_for_result_num(value):
    if value is None:
        return None
    else:    
        if '/' in value:
            # If the value contains a '/', assume it's a blood pressure reading and should be in the result text column
            return None
        else:
            # Process other numeric or float values
            try:
                numeric_value = float(value)
                return value
                # Do something with the numeric value here
            except ValueError:
                return None
            

  check_for_result_num_udf = udf(check_for_result_num, StringType())

  ###################################################################################################################################

  def check_for_result_text(value):
    if value is None:
        return None
    else:
        if '/' in value:
        # If the value contains a '/', assume it's a blood pressure reading and should be in the result text column
            return value
        else:
        # Process other numeric or float values
            try:
                numeric_value = float(value)
                return None
            # Do something with the numeric value here
            except ValueError:
                return None 
            
  check_for_result_text_udf = udf(check_for_result_text, StringType())

  ###################################################################################################################################


  def get_time_from_datetime(val_time):
        # Parse the input string into a datetime object
        datetime_object = datetime.strptime(val_time, "%Y-%m-%d %H:%M:%S")

        # Format the datetime object as a string in "yyyy-mm-dd" format
        formatted_time = datetime_object.strftime("%H:%M")

        return formatted_time
    
  convert_and_format_time_udf = udf(get_time_from_datetime, StringType()) 



    ###################################################################################################################################

    #Converting the fileds to PCORNet obs_clin Format

    ###################################################################################################################################

  obs_clin = filtered_observation.select(           filtered_observation['observation_id'].alias("OBSCLINID"),
                                                    filtered_observation['person_id'].alias("PATID"),
                                                    filtered_observation['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    filtered_observation['provider_id'].alias("OBSCLIN_PROVIDERID"),
                                                    filtered_observation['observation_start_date'].alias("OBSCLIN_START_DATE"),
                                                    convert_and_format_time_udf(filtered_observation['observation_start_datetime']).alias("OBSCLIN_START_TIME"),
                                                    filtered_observation['observation_end_date'].alias("OBSCLIN_STOP_DATE"),
                                                    convert_and_format_time_udf(filtered_observation['observation_end_datetime']).alias("OBSCLIN_STOP_TIME"),
                                                    filtered_observation['observation_code_type'].alias("OBSCLIN_TYPE"),
                                                    filtered_observation['observation_code'].alias("OBSCLIN_CODE"),
                                                    filtered_observation['qualifier'].alias("OBSCLIN_RESULT_QUAL"),
                                                    check_for_result_text_udf(filtered_observation['value_as_number']).alias("OBSCLIN_RESULT_TEXT"),
                                                    filtered_observation['observation_code_type'].alias("OBSCLIN_RESULT_SNOMED"),
                                                    check_for_result_num_udf(filtered_observation['value_as_number']).alias("OBSCLIN_RESULT_NUM"),
                                                    lit('OT').alias("OBSCLIN_RESULT_MODIFIER"),
                                                    filtered_observation['unit'].alias("OBSCLIN_RESULT_UNIT"),
                                                    filtered_observation['observation_data_origin'].alias("OBSCLIN_SOURCE"),
                                                    lit('').alias("OBSCLIN_ABN_IND"),
                                                    filtered_observation['observation_code'].alias("RAW_OBSCLIN_NAME"),
                                                    filtered_observation['observation_code'].alias("RAW_OBSCLIN_CODE"),
                                                    filtered_observation['observation_code_type'].alias("RAW_OBSCLIN_TYPE"),
                                                    concat(col("value_as_string"),lit(' - '), col("value_as_number")).alias("RAW_OBSCLIN_RESULT"),
                                                    filtered_observation['qualifier_source_value'].alias("RAW_OBSCLIN_MODIFIER"),
                                                    filtered_observation['unit_source_value'].alias("RAW_OBSCLIN_UNIT"),



                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

  cf.write_pyspark_output_file(
                      payspark_df = obs_clin,
                      output_file_name = "formatted_obs_clin.csv",
                      output_data_folder_path= formatter_output_data_folder_path)


  spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'obs_clin_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')



  