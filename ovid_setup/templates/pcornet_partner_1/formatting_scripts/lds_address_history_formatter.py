###################################################################################################################################

# This script will take in the PCORnet formatted raw LDS_ADDRESS_HISTORY file, do the necessary transformations, and output the formatted PCORnet LDS_ADDRESS_HISTORY file

###################################################################################################################################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse
import re


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
#This function will only return a valid zip 5 code
###################################################################################################################################

def validate_zip5(input_str):

    if input_str == None:

        return None
    
    
    # Regular expression pattern for 5-digit ZIP code
    zip_pattern = r'^\d{5}$'
    
    # Check if input matches the ZIP code pattern
    if re.match(zip_pattern, input_str):
        return input_str  # Return the input if it's a valid ZIP code
    else:
        return None  # Return None if input is not a valid ZIP code


validate_zip5_udf = udf(validate_zip5, StringType())



###################################################################################################################################
#This function will only return a valid zip 9 code
###################################################################################################################################

def validate_zip9(input_str):

    if input_str == None:

        return None

    input_str = input_str.replace('-','')
    # Regular expression pattern for 5-digit ZIP code
    zip_pattern = r'^\d{9}$'
    
    # Check if input matches the ZIP code pattern
    if re.match(zip_pattern, input_str):
        return input_str  # Return the input if it's a valid ZIP code
    else:
        return None  # Return None if input is not a valid ZIP code


validate_zip9_udf = udf(validate_zip9, StringType())


###################################################################################################################################
#This function will only return a valid US STATE
###################################################################################################################################

def clean_address_state(input_address_state):


    if input_address_state == None:

        return None

    # Set of valid US state abbreviations
    us_state_abbr = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }
    
    # Check if input is in the set of valid state abbreviations
    if input_address_state.upper() in us_state_abbr:
        return input_address_state.upper()  # Return the input if it's a valid state abbreviation
    else:
        return None  # Return None if input is not a valid state abbreviation


clean_address_state_udf = udf(clean_address_state, StringType())




try: 

    ###################################################################################################################################

    # Loading the lds_address_history table 

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    # input_data_folder_path               = f'/app/partners/pcornet_partner_1/data/input/{input_data_folder}/'  


    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    lds_address_history_table_name         = '*Address*'


    lds_address_history_in = spark.read.load(input_data_folder_path+lds_address_history_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')

    ###################################################################################################################################

    #Converting the fileds to PCORNet lds_address_history Format

    ###################################################################################################################################
    try:

        lds_address_history_in = spark.read.load(input_data_folder_path+lds_address_history_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        lds_address_history = lds_address_history_in.select(
            lds_address_history_in['addressid'].alias('ADDRESSID'),
            lds_address_history_in['patid'].alias('PATID'),
            lds_address_history_in['address_use'].alias('ADDRESS_USE'),
            lds_address_history_in['address_type'].alias('ADDRESS_TYPE'),
            lds_address_history_in['address_preferred'].alias('ADDRESS_PREFERRED'),
            lds_address_history_in['address_city'].alias('ADDRESS_CITY'),
            clean_address_state_udf(lds_address_history_in['address_state']).alias('ADDRESS_STATE'),
            lds_address_history_in['ADDRESS_COUNTY'].alias('ADDRESS_COUNTY'),
            validate_zip5_udf(lds_address_history_in['address_zip5']).alias('ADDRESS_ZIP5'),
            validate_zip9_udf(lds_address_history_in['address_zip9']).alias('ADDRESS_ZIP9'),
            cf.format_date_udf(lds_address_history_in['address_period_start']).alias('ADDRESS_PERIOD_START'),
            cf.format_date_udf(lds_address_history_in['address_period_end']).alias('ADDRESS_PERIOD_END')
        )
    except:
        lds_address_history_in = spark.read.load(input_data_folder_path+lds_address_history_table_name,format="csv", sep=",", inferSchema="false", header="true", quote= '"')
        lds_address_history = lds_address_history_in.select(
            lds_address_history_in['ADDRESSID'].alias('ADDRESSID'),
            lds_address_history_in['PATID'].alias('PATID'),
            lds_address_history_in['ADDRESS_USE'].alias('ADDRESS_USE'),
            lds_address_history_in['ADDRESS_TYPE'].alias('ADDRESS_TYPE'),
            lds_address_history_in['ADDRESS_PREFERRED'].alias('ADDRESS_PREFERRED'),
            lds_address_history_in['ADDRESS_CITY'].alias('ADDRESS_CITY'),
            clean_address_state_udf(lds_address_history_in['ADDRESS_STATE']).alias('ADDRESS_STATE'),
            lds_address_history_in['ADDRESS_COUNTY'].alias('ADDRESS_COUNTY'),
            validate_zip5_udf(lds_address_history_in['ADDRESS_ZIP5']).alias('ADDRESS_ZIP5'),
            validate_zip9_udf(lds_address_history_in['ADDRESS_ZIP9']).alias('ADDRESS_ZIP9'),
            cf.format_date_udf(lds_address_history_in['ADDRESS_PERIOD_START']).alias('ADDRESS_PERIOD_START'),
            cf.format_date_udf(lds_address_history_in['ADDRESS_PERIOD_END']).alias('ADDRESS_PERIOD_END')
        )


    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = lds_address_history,
                        output_file_name = "formatted_lds_address_history.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()




except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'lds_address_history_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')





