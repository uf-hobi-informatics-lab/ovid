
###################################################################################################################################

# This script will convert an PCORnet like obs_gen table to a PCORnet format as the obs_gen table

###################################################################################################################################


import os
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons


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

    # Loading the raw_obs_gen table to be converted to the obs_gen table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

    obs_gen_table_name   = "OBS_GEN_*.txt"

    raw_obs_gen = spark.read.load(input_data_folder_path+obs_gen_table_name,format="csv", sep="|", inferSchema="false", header="true", quote= '"')


    ###################################################################################################################################

    obs_gen = raw_obs_gen.select(                   
                                                    raw_obs_gen['OBSGENID'].alias("OBSGENID"),
                                                    raw_obs_gen['PATID'].alias("PATID"),
                                                    raw_obs_gen['ENCOUNTERID'].alias("ENCOUNTERID"),
                                                    raw_obs_gen['OBSGEN_PROVIDERID'].alias("OBSGEN_PROVIDERID"),
                                                    raw_obs_gen['OBSGEN_START_DATE'].alias("OBSGEN_START_DATE"),
                                                    raw_obs_gen['OBSGEN_START_TIME'].alias("OBSGEN_START_TIME"),
                                                    raw_obs_gen['OBSGEN_STOP_DATE'].alias("OBSGEN_STOP_DATE"),
                                                    raw_obs_gen['OBSGEN_STOP_TIME'].alias("OBSGEN_STOP_TIME"),
                                                    raw_obs_gen['OBSGEN_TYPE'].alias("OBSGEN_TYPE"),
                                                    raw_obs_gen['OBSGEN_CODE'].alias("OBSGEN_CODE"),
                                                    raw_obs_gen['OBSGEN_RESULT_QUAL'].alias("OBSGEN_RESULT_QUAL"),
                                                    raw_obs_gen['OBSGEN_RESULT_TEXT'].alias("OBSGEN_RESULT_TEXT"),
                                                    raw_obs_gen['OBSGEN_RESULT_NUM'].alias("OBSGEN_RESULT_NUM"),
                                                    raw_obs_gen['OBSGEN_RESULT_MODIFIER'].alias("OBSGEN_RESULT_MODIFIER"),
                                                    raw_obs_gen['OBSGEN_RESULT_UNIT'].alias("OBSGEN_RESULT_UNIT"),     
                                                    raw_obs_gen['OBSGEN_RESULT_UNIT'].alias("OBSGEN_TABLE_MODIFIED"),                                           
                                                    raw_obs_gen['OBSGEN_RESULT_UNIT'].alias("OBSGEN_ID_MODIFIED"),                                           
                                                    raw_obs_gen['OBSGEN_SOURCE'].alias("OBSGEN_SOURCE"),
                                                    raw_obs_gen['OBSGEN_ABN_IND'].alias("OBSGEN_ABN_IND"),
                                                    raw_obs_gen['RAW_OBSGEN_NAME'].alias("RAW_OBSGEN_NAME"),
                                                    raw_obs_gen['RAW_OBSGEN_CODE'].alias("RAW_OBSGEN_CODE"),
                                                    raw_obs_gen['RAW_OBSGEN_TYPE'].alias("RAW_OBSGEN_TYPE"),
                                                    raw_obs_gen['RAW_OBSGEN_RESULT'].alias("RAW_OBSGEN_RESULT"),
                                                    raw_obs_gen['RAW_OBSGEN_UNIT'].alias("RAW_OBSGEN_UNIT"),

                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = obs_gen,
                        output_file_name = "formatted_obs_gen.csv",
                        output_data_folder_path= formatter_output_data_folder_path)



    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'obs_gen_formatter.py' ,
                            text = str(e)
                            )


