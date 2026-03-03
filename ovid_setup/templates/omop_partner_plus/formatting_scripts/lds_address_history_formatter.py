###################################################################################################################################

# This script will convert an OMOP lds_address_history table to a PCORnet format as the lds_address_history table

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

    # Loading the lds_address_history table to be converted to the lds_address_history table

###################################################################################################################################
    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    lds_address_history_table_name         = 'address.csv'
    location_table_name                    = 'location.csv'


    lds_address_history = spark.read.load(input_data_folder_path+lds_address_history_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    location = spark.read.load(input_data_folder_path+location_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').select("location_id", "city", "state", "zip")
    location = location.select(location['location_id'].alias("location_id_right"), location['city'], location['state'], location['zip'] )


    joined_lds_address_history = lds_address_history.join(location, location['location_id_right']==lds_address_history['location_id'], how='left')

    ###################################################################################################################################

    #Converting the fileds to PCORNet lds_address_history Format

    ###################################################################################################################################

    lds_address_history = joined_lds_address_history.select(            
                                                    joined_lds_address_history['address_id'].alias("ADDRESSID"),
                                                    joined_lds_address_history['person_id'].alias("PATID"),
                                                    joined_lds_address_history['address_use'].alias("ADDRESS_USE"),
                                                    joined_lds_address_history['address_type'].alias("ADDRESS_TYPE"),
                                                    joined_lds_address_history['address_preferred'].alias("ADDRESS_PREFERRED"),
                                                    joined_lds_address_history['city'].alias("ADDRESS_CITY"), 
                                                    joined_lds_address_history['state'].alias("ADDRESS_STATE"),
                                                    joined_lds_address_history['zip'].alias("ADDRESS_ZIP5"),
                                                    lit('').alias("ADDRESS_ZIP9"), # need to change this and change the location table definition or location_sup to include zip_5 and zip_9s
                                                    lit('').alias("ADDRESS_COUNTY"),
                                                    joined_lds_address_history['address_period_start'].alias("ADDRESS_PERIOD_START"),
                                                    joined_lds_address_history['address_period_end'].alias("ADDRESS_PERIOD_END"),                                          
                                                    
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
