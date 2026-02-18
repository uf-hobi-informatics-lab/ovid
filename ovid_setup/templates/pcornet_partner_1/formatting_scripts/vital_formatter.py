###################################################################################################################################

# This script will take in the PCORnet formatted raw VITAL file, do the necessary transformations, and output the formatted PCORnet VITAL file

###################################################################################################################################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
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


def retun_if_numeric( val):
    try:
        float(val)
        return val
    except:
        #Not a float
        return None
    

retun_if_numeric_udf = udf(retun_if_numeric, StringType())

def copy_and_remove_letters(val):
    """
    Copy values from source columns and
    remove any letters (found in FH vitals)
    """
    if val is None:
        return None
    stripped = str(val).strip('eE').strip()
    return stripped

copy_and_remove_letters_udf = udf(copy_and_remove_letters, StringType())




try:
        

    ###################################################################################################################################

    # Loading the raw vital table 

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

    vital_table_name            = '*Vital*'


    ###################################################################################################################################

    #Converting the fields to PCORNet vital Format

    ###################################################################################################################################
   
    try:

        vital_in = spark.read.load(input_data_folder_path+vital_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        vital = vital_in.select(


                            vital_in['vitalid'].alias('VITALID'),
                            vital_in['patid'].alias('PATID'),
                            vital_in['encounterid'].alias('ENCOUNTERID'),
                            cf.format_date_udf(vital_in['measure_date']).alias('MEASURE_DATE'),
                            cf.get_time_from_datetime_udf(vital_in['measure_time']).alias('MEASURE_TIME'),
                            vital_in['vital_source'].alias('VITAL_SOURCE'),
                            retun_if_numeric_udf(vital_in['ht']).alias('HT'),
                            retun_if_numeric_udf(vital_in['wt']).alias('WT'),
                            retun_if_numeric_udf(vital_in['diastolic']).alias('DIASTOLIC'),
                            retun_if_numeric_udf(vital_in['systolic']).alias('SYSTOLIC'),
                            retun_if_numeric_udf(vital_in['original_bmi']).alias('ORIGINAL_BMI'),
                            vital_in['bp_position'].alias('BP_POSITION'),
                            vital_in['smoking'].alias('SMOKING'),
                            vital_in['tobacco'].alias('TOBACCO'),
                            vital_in['tobacco_type'].alias('TOBACCO_TYPE'),
                            vital_in['raw_diastolic'].alias('RAW_DIASTOLIC'),
                            vital_in['raw_systolic'].alias('RAW_SYSTOLIC'),
                            vital_in['raw_bp_position'].alias('RAW_BP_POSITION'),
                            vital_in['raw_smoking'].alias('RAW_SMOKING'),
                            vital_in['raw_tobacco'].alias('RAW_TOBACCO'),
                            vital_in['raw_tobacco_type'].alias('RAW_TOBACCO_TYPE'),
        )
    except:
        vital_in = spark.read.load(input_data_folder_path+vital_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        vital = vital_in.select(


                            vital_in['VITALID'].alias('VITALID'),
                            vital_in['PATID'].alias('PATID'),
                            vital_in['ENCOUNTERID'].alias('ENCOUNTERID'),
                            cf.format_date_udf(vital_in['MEASURE_DATE']).alias('MEASURE_DATE'),
                            cf.get_time_from_datetime_udf(vital_in['MEASURE_TIME']).alias('MEASURE_TIME'),
                            vital_in['VITAL_SOURCE'].alias('VITAL_SOURCE'),
                            retun_if_numeric_udf(vital_in['HT']).alias('HT'),
                            retun_if_numeric_udf(vital_in['WT']).alias('WT'),
                            retun_if_numeric_udf(vital_in['DIASTOLIC']).alias('DIASTOLIC'),
                            retun_if_numeric_udf(vital_in['SYSTOLIC']).alias('SYSTOLIC'),
                            retun_if_numeric_udf(vital_in['ORIGINAL_BMI']).alias('ORIGINAL_BMI'),
                            vital_in['BP_POSITION'].alias('BP_POSITION'),
                            vital_in['SMOKING'].alias('SMOKING'),
                            vital_in['TOBACCO'].alias('TOBACCO'),
                            vital_in['TOBACCO_TYPE'].alias('TOBACCO_TYPE'),
                            vital_in['RAW_DIASTOLIC'].alias('RAW_DIASTOLIC'),
                            vital_in['RAW_SYSTOLIC'].alias('RAW_SYSTOLIC'),
                            vital_in['RAW_BP_POSITION'].alias('RAW_BP_POSITION'),
                            vital_in['RAW_SMOKING'].alias('RAW_SMOKING'),
                            vital_in['RAW_TOBACCO'].alias('RAW_TOBACCO'),
                            vital_in['RAW_TOBACCO_TYPE'].alias('RAW_TOBACCO_TYPE'),
        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = vital,
                        output_file_name = "formatted_vital.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'vital_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')
