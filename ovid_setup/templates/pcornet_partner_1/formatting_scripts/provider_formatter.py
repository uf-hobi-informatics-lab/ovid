###################################################################################################################################

# This script will take in the PCORnet formatted raw PROVIDER file, do the necessary transformations, and output the formatted PCORnet PROVIDER file

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

    # Loading the provider table to be converted to the pcornet_provider table

    ###################################################################################################################################
    input_data_folder_path               = f'/data/{input_data_folder}/'
    # input_data_folder_path               = f'/app/partners/pcornet_partner_1/data/input/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    provider_table_name       = '*Provider*'

    provider_in = spark.read.load(input_data_folder_path+provider_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')


    ###################################################################################################################################

    #Converting the fileds to PCORNet pcornet_provider Format

    ###################################################################################################################################
    try:

        provider_in = spark.read.load(input_data_folder_path+provider_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        pcornet_provider = provider_in.select(

            
                        provider_in['providerid'].alias('PROVIDERID'),
                        provider_in['provider_sex'].alias('PROVIDER_SEX'),
                        lit('NI').alias('PROVIDER_SPECIALTY_PRIMARY'),
                        provider_in['provider_npi'].alias('PROVIDER_NPI'),
                        provider_in['provider_npi_flag'].alias('PROVIDER_NPI_FLAG'),
                        provider_in['raw_provider_specialty_primary'].alias('RAW_PROVIDER_SPECIALTY_PRIMARY'),

        )

    except:

        provider_in = spark.read.load(input_data_folder_path+provider_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        pcornet_provider = provider_in.select(

            
                        provider_in['PROVIDERID'].alias('PROVIDERID'),
                        provider_in['PROVIDER_SEX'].alias('PROVIDER_SEX'),
                        lit('NI').alias('PROVIDER_SPECIALTY_PRIMARY'),
                        provider_in['PROVIDER_NPI'].alias('PROVIDER_NPI'),
                        provider_in['PROVIDER_NPI_FLAG'].alias('PROVIDER_NPI_FLAG'),
                        provider_in['RAW_PROVIDER_SPECIALTY_PRIMARY'].alias('RAW_PROVIDER_SPECIALTY_PRIMARY'),

        )
    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################
    cf.write_pyspark_output_file(
                        payspark_df = pcornet_provider,
                        output_file_name = "formatted_provider.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'provider_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')














