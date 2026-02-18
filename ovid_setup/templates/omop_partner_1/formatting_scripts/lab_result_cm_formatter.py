###################################################################################################################################

# This script will convert an OMOP measurement table to a PCORnet format as the lab_result_cm table

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

    # Loading the measurement table to be converted to the lab_result_cm table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    measurement_table_name   = 'Measurement.txt'

    measurement = spark.read.load(input_data_folder_path+measurement_table_name,format="csv", sep=",", inferSchema="false", header="true", quote= '"')


    ###################################################################################################################################

    # This function will rertun lab_result_loc base on the measurement_source_value

    ###################################################################################################################################


    def get_lab_result_loc( measurement_source_value):
            if measurement_source_value is None:
                return 'UN'
            else:
                if measurement_source_value[0:4] == ' POC':
                    return 'P'
                else:
                    return 'L'

    get_lab_result_loc_udf = udf(get_lab_result_loc, StringType())

    ###################################################################################################################################


    def remove_nulls(value_as_number):
            if value_as_number == 'NULL':
                return  None
            else:
                return value_as_number

    remove_nulls_udf = udf(remove_nulls, StringType())

    ###################################################################################################################################

    def get_time_from_datetime(val_time):
        # Parse the input string into a datetime object
        datetime_object = datetime.strptime(val_time, "%Y-%m-%d %H:%M:%S")

        # Format the datetime object as a string in "yyyy-mm-dd" format
        formatted_time = datetime_object.strftime("%H:%M")

        return formatted_time
    
    convert_and_format_time_udf = udf(get_time_from_datetime, StringType())  

    ###################################################################################################################################


    def format_result_num( val):
        try:
            float(val)
            return val
        except:

            try:
                 val = val.replace('-','').replace('+','')
                 float(val)
                 return val
            except:
            #Not a float
                return None
        

    format_result_num_udf = udf(format_result_num, StringType())

    ###################################################################################################################################

    #Converting the fileds to PCORNet lab_result_cm Format

    ###################################################################################################################################

    lab_result_cm = measurement.select(             measurement['measurement_id'].alias("LAB_RESULT_CM_ID"),
                                                    measurement['person_id'].alias("PATID"),
                                                    measurement['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    measurement['measurement_code'].alias("SPECIMEN_SOURCE"),
                                                    measurement['measurement_code'].alias("LAB_LOINC"),
                                                    lit('OD').alias("LAB_RESULT_SOURCE"),
                                                    measurement['measurement_loinc_source'].alias("LAB_LOINC_SOURCE"),
                                                    measurement['priority'].alias("PRIORITY"),
                                                    get_lab_result_loc_udf('measurement_code_source_value').alias("RESULT_LOC"),
                                                    lit('').alias("LAB_PX"),
                                                    lit('').alias("LAB_PX_TYPE"),
                                                    measurement['measurement_order_date'].alias("LAB_ORDER_DATE"),
                                                    measurement['measurement_date'].alias("SPECIMEN_DATE"),
                                                    convert_and_format_time_udf(measurement['measurement_datetime']).alias("SPECIMEN_TIME"),
                                                    measurement['measurement_date'].alias("RESULT_DATE"),
                                                    convert_and_format_time_udf(measurement['measurement_datetime']).alias("RESULT_TIME"),
                                                    measurement['value_as_string'].alias("RESULT_QUAL"),
                                                    lit('').alias("RESULT_SNOMED"),
                                                    format_result_num_udf(measurement['value_as_number']).alias("RESULT_NUM"),
                                                    measurement['operator'].alias("RESULT_MODIFIER"),
                                                    measurement['unit'].alias("RESULT_UNIT"),
                                                    measurement['range_low'].alias("NORM_RANGE_LOW"),
                                                    measurement['operator'].alias("NORM_MODIFIER_LOW"),
                                                    measurement['range_high'].alias("NORM_RANGE_HIGH"),
                                                    measurement['operator'].alias("NORM_MODIFIER_HIGH"),
                                                    measurement['abn_ind'].alias("ABN_IND"),
                                                    measurement['measurement_code_source_value'].alias("RAW_LAB_NAME"),
                                                    measurement['measurement_code_source_value'].alias("RAW_LAB_CODE"),
                                                    lit('').alias("RAW_PANEL"),
                                                    measurement['value_source_value'].alias("RAW_RESULT"),
                                                    measurement['unit_source_value'].alias("RAW_UNIT"),
                                                    lit('').alias("RAW_ORDER_DEPT"),
                                                    lit('').alias("RAW_FACILITY_CODE")
                                
                                                                                        
                                                
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                      payspark_df = lab_result_cm,
                      output_file_name = "formatted_lab_result_cm.csv",
                      output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'lab_result_cm_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')



