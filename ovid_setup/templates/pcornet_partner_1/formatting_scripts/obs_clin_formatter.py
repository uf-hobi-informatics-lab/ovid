###################################################################################################################################

# This script will take in the PCORnet formatted raw OBS_CLIN file, do the necessary transformations, and output the formatted PCORnet OBS_CLIN file

###################################################################################################################################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import pickle
from pyspark.sql import SparkSession
import argparse

###################################################################################################################################
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-p", "--partner_name")

args = parser.parse_args()
input_data_folder = args.data_folder
partner_name = args.partner_name
cf =CommonFuncitons(partner_name)
# Create SparkSession
spark = cf.get_spark_session("ovid")
###################################################################################################################################
###################################################################################################################################
# 
###################################################################################################################################


def clean_pcornet_partner_1_result_qual( result_qual):

    result_qual_list = [
         "DETECTED",
         "NOT DETECTED",
         "POSITIVE",
         "NEGATIVE",
         "BORDERLINE",
         "HIGH",
         "LOW",
         "NORMAL",
         "ABNORMAL",
         "UNDETERMINED",
         "NO INFROMATION",
         "UNKNOWN",
         "OT",
         "OTHER",

    ]


    if result_qual == None or result_qual == 'NONE' or result_qual == "":

        return "NI"
    
    elif result_qual not in result_qual_list:
                 
                 return 'OT'
    
    else: 
                 
                 return result_qual

clean_pcornet_partner_1_result_qual_udf = udf(clean_pcornet_partner_1_result_qual, StringType())


###################################################################################################################################
# 
###################################################################################################################################

def return_numeric_or_none(value):

    try:
        return float(value)
    except:
        return None
    

return_numeric_or_none_udf = udf(return_numeric_or_none, StringType())


###################################################################################################################################
# 
###################################################################################################################################



try:
        
    ###################################################################################################################################

    # Loading the observation table to be converted to the obs_clin table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    obs_clin_table_name   = '*Clinial*'


    ###################################################################################################################################

    #Converting the fileds to PCORNet obs_clin Format

    ###################################################################################################################################

    try:


        obs_clin_table_name   = '*Clinial*'
          
        obs_clin_in= spark.read.load(input_data_folder_path+obs_clin_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        obs_clin = obs_clin_in.select(


                                obs_clin_in['obsclinid'].alias('OBSCLINID'),
                                obs_clin_in['patid'].alias('PATID'),
                                obs_clin_in['encounterid'].alias('ENCOUNTERID'),
                                obs_clin_in['obsclin_providerid'].alias('OBSCLIN_PROVIDERID'),
                                cf.format_date_udf(obs_clin_in['obsclin_start_date']).alias('OBSCLIN_START_DATE'),
                                cf.get_time_from_datetime_udf(obs_clin_in['obsclin_start_time']).alias('OBSCLIN_START_TIME'),
                                cf.format_date_udf(obs_clin_in['obsclin_stop_date']).alias('OBSCLIN_STOP_DATE'),
                                cf.get_time_from_datetime_udf(obs_clin_in['obsclin_stop_time']).alias('OBSCLIN_STOP_TIME'),
                                obs_clin_in['obsclin_type'].alias('OBSCLIN_TYPE'),
                                obs_clin_in['obsclin_code'].alias('OBSCLIN_CODE'),
                                clean_pcornet_partner_1_result_qual_udf(obs_clin_in['obsclin_result_qual']).alias('OBSCLIN_RESULT_QUAL'),
                                obs_clin_in['obsclin_result_text'].alias('OBSCLIN_RESULT_TEXT'),
                                obs_clin_in['obsclin_result_snomed'].alias('OBSCLIN_RESULT_SNOMED'),
                                return_numeric_or_none_udf(obs_clin_in['obsclin_result_num']).alias('OBSCLIN_RESULT_NUM'),
                                obs_clin_in['obsclin_result_modifier'].alias('OBSCLIN_RESULT_MODIFIER'),
                                obs_clin_in['obsclin_result_unit'].alias('OBSCLIN_RESULT_UNIT'),
                                obs_clin_in['obsclin_source'].alias('OBSCLIN_SOURCE'),
                                obs_clin_in['obsclin_abn_ind'].alias('OBSCLIN_ABN_IND'),
                                obs_clin_in['raw_obsclin_name'].alias('RAW_OBSCLIN_NAME'),
                                obs_clin_in['raw_obsclin_code'].alias('RAW_OBSCLIN_CODE'),
                                obs_clin_in['raw_obsclin_type'].alias('RAW_OBSCLIN_TYPE'),
                                obs_clin_in['raw_obsclin_result'].alias('RAW_OBSCLIN_RESULT'),
                                obs_clin_in['raw_obsclin_modifier'].alias('RAW_OBSCLIN_MODIFIER'),
                                obs_clin_in['raw_obsclin_unit'].alias('RAW_OBSCLIN_UNIT'),
        )
    except:

        obs_clin_table_name   = '*Clinical*'
        obs_clin_in= spark.read.load(input_data_folder_path+obs_clin_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        obs_clin = obs_clin_in.select(


                                obs_clin_in['OBSCLINID'].alias('OBSCLINID'),
                                obs_clin_in['PATID'].alias('PATID'),
                                obs_clin_in['ENCOUNTERID'].alias('ENCOUNTERID'),
                                obs_clin_in['OBSCLIN_PROVIDERID'].alias('OBSCLIN_PROVIDERID'),
                                cf.format_date_udf(obs_clin_in['OBSCLIN_DATE']).alias('OBSCLIN_START_DATE'),
                                cf.get_time_from_datetime_udf(obs_clin_in['OBSCLIN_TIME']).alias('OBSCLIN_START_TIME'),
                                cf.format_date_udf(obs_clin_in['OBSCLIN_DATE']).alias('OBSCLIN_STOP_DATE'),
                                cf.get_time_from_datetime_udf(obs_clin_in['OBSCLIN_TIME']).alias('OBSCLIN_STOP_TIME'),
                                obs_clin_in['OBSCLIN_TYPE'].alias('OBSCLIN_TYPE'),
                                obs_clin_in['OBSCLIN_CODE'].alias('OBSCLIN_CODE'),
                                clean_pcornet_partner_1_result_qual_udf(obs_clin_in['OBSCLIN_RESULT_QUAL']).alias('OBSCLIN_RESULT_QUAL'),
                                obs_clin_in['OBSCLIN_RESULT_TEXT'].alias('OBSCLIN_RESULT_TEXT'),
                                obs_clin_in['OBSCLIN_RESULT_SNOMED'].alias('OBSCLIN_RESULT_SNOMED'),
                                return_numeric_or_none_udf(obs_clin_in['OBSCLIN_RESULT_NUM']).alias('OBSCLIN_RESULT_NUM'),
                                obs_clin_in['OBSCLIN_RESULT_MODIFIER'].alias('OBSCLIN_RESULT_MODIFIER'),
                                obs_clin_in['OBSCLIN_RESULT_UNIT'].alias('OBSCLIN_RESULT_UNIT'),
                                obs_clin_in['OBSCLIN_SOURCE'].alias('OBSCLIN_SOURCE'),
                                lit("").alias('OBSCLIN_ABN_IND'),
                                obs_clin_in['RAW_OBSCLIN_NAME'].alias('RAW_OBSCLIN_NAME'),
                                obs_clin_in['RAW_OBSCLIN_CODE'].alias('RAW_OBSCLIN_CODE'),
                                obs_clin_in['RAW_OBSCLIN_TYPE'].alias('RAW_OBSCLIN_TYPE'),
                                obs_clin_in['RAW_OBSCLIN_RESULT'].alias('RAW_OBSCLIN_RESULT'),
                                obs_clin_in['RAW_OBSCLIN_MODIFIER'].alias('RAW_OBSCLIN_MODIFIER'),
                                obs_clin_in['RAW_OBSCLIN_UNIT'].alias('RAW_OBSCLIN_UNIT'),
        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf =CommonFuncitons('UFH')
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


