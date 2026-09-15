###################################################################################################################################

# This script will take in the PCORnet formatted raw PRIVATE_ADDRESS_GEOCODE file, do the necessary transformations, and output the formatted PCORnet PRIVATE_ADDRESS_GEOCODE file

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


try:

    ###################################################################################################################################

    # Loading the private_address_geocode table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    # input_data_folder_path               = f'/app/partners/pcornet_partner_1/data/input/{input_data_folder}/'

    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    private_address_geocode_table_name   = '*Private_Address_Geocode*'


    ###################################################################################################################################

    #Converting the fields to PCORNet private_address_geocode Format

    ###################################################################################################################################

    try:

        private_address_geocode_in = spark.read.load(input_data_folder_path+private_address_geocode_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        private_address_geocode = private_address_geocode_in.select(

                                private_address_geocode_in['geocodeid'].alias('GEOCODEID'),
                                private_address_geocode_in['addressid'].alias('ADDRESSID'),
                                private_address_geocode_in['geocode_state'].alias('GEOCODE_STATE'),
                                private_address_geocode_in['geocode_county'].alias('GEOCODE_COUNTY'),
                                private_address_geocode_in['geocode_longitude'].alias('GEOCODE_LONGITUDE'),
                                private_address_geocode_in['geocode_latitude'].alias('GEOCODE_LATITUDE'),
                                private_address_geocode_in['geocode_block'].alias('GEOCODE_BLOCK'),
                                private_address_geocode_in['geocode_tract'].alias('GEOCODE_TRACT'),
                                private_address_geocode_in['geocode_group'].alias('GEOCODE_GROUP'),
                                private_address_geocode_in['geocode_zcta'].alias('GEOCODE_ZCTA'),
                                private_address_geocode_in['geocode_custom'].alias('GEOCODE_CUSTOM'),
                                private_address_geocode_in['geocode_custom_text'].alias('GEOCODE_CUSTOM_TEXT'),
                                private_address_geocode_in['shapefile'].alias('SHAPEFILE'),
                                private_address_geocode_in['geo_accuracy'].alias('GEO_ACCURACY'),
                                private_address_geocode_in['geo_prov_ref'].alias('GEO_PROV_REF'),
                                cf.format_date_udf(private_address_geocode_in['assignment_date']).alias('ASSIGNMENT_DATE'),

        )
    except:
        private_address_geocode_in = spark.read.load(input_data_folder_path+private_address_geocode_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        private_address_geocode = private_address_geocode_in.select(

                                private_address_geocode_in['GEOCODEID'].alias('GEOCODEID'),
                                private_address_geocode_in['ADDRESSID'].alias('ADDRESSID'),
                                private_address_geocode_in['GEOCODE_STATE'].alias('GEOCODE_STATE'),
                                private_address_geocode_in['GEOCODE_COUNTY'].alias('GEOCODE_COUNTY'),
                                private_address_geocode_in['GEOCODE_LONGITUDE'].alias('GEOCODE_LONGITUDE'),
                                private_address_geocode_in['GEOCODE_LATITUDE'].alias('GEOCODE_LATITUDE'),
                                private_address_geocode_in['GEOCODE_BLOCK'].alias('GEOCODE_BLOCK'),
                                private_address_geocode_in['GEOCODE_TRACT'].alias('GEOCODE_TRACT'),
                                private_address_geocode_in['GEOCODE_GROUP'].alias('GEOCODE_GROUP'),
                                private_address_geocode_in['GEOCODE_ZCTA'].alias('GEOCODE_ZCTA'),
                                private_address_geocode_in['GEOCODE_CUSTOM'].alias('GEOCODE_CUSTOM'),
                                private_address_geocode_in['GEOCODE_CUSTOM_TEXT'].alias('GEOCODE_CUSTOM_TEXT'),
                                private_address_geocode_in['SHAPEFILE'].alias('SHAPEFILE'),
                                private_address_geocode_in['GEO_ACCURACY'].alias('GEO_ACCURACY'),
                                private_address_geocode_in['GEO_PROV_REF'].alias('GEO_PROV_REF'),
                                cf.format_date_udf(private_address_geocode_in['ASSIGNMENT_DATE']).alias('ASSIGNMENT_DATE'),

        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = private_address_geocode,
                        output_file_name = "formatted_private_address_geocode.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'private_address_geocode_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')
