###################################################################################################################################

# This script will convert an OMOP omop_provider table to a PCORnet format as the pcornet_provider table

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

    # Loading the omop_provider table to be converted to the pcornet_provider table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    omop_provider_table_name       = 'Providers.txt'

    omop_provider = spark.read.load(input_data_folder_path+omop_provider_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    ###################################################################################################################################

    #Converting the fileds to PCORNet pcornet_provider Format

    ###################################################################################################################################

    def get_provider_npi_flag(npi):
        try:

            if len(npi)== 10 and npi.isnumeric():
                return "Y"
            else:
                return "N"
        except:
            return "N"


    get_provider_npi_flag_udf = udf(get_provider_npi_flag, StringType())

    ###################################################################################################################################

    #Converting the fileds to PCORNet pcornet_provider Format

    ###################################################################################################################################

    provider = omop_provider.select(                omop_provider['provider_id'].alias("PROVIDERID"),
                                                    omop_provider['gender'].alias("PROVIDER_SEX"),
                                                    omop_provider['provider_specialty'].alias("PROVIDER_SPECIALTY_PRIMARY"),
                                                    omop_provider['npi'].alias("PROVIDER_NPI"),
                                                    get_provider_npi_flag_udf(omop_provider['npi']).alias("PROVIDER_NPI_FLAG"),
                                                    omop_provider['provider_specialty_source_value'].alias("RAW_PROVIDER_SPECIALTY_PRIMARY"),
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                      payspark_df = provider,
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
















