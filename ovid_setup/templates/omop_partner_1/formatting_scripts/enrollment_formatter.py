###################################################################################################################################

# This script will convert an OMOP obsrevation_period table to a PCORnet format as the enrollment table

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

    # Loading the obsrevation_period table to be converted to the enrollment table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    observation_period_table_name       = 'Observation_Period.txt'

    observation_period = spark.read.load(input_data_folder_path+observation_period_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')




    ###################################################################################################################################

    #Converting the fileds to PCORNet enrollment Format

    ###################################################################################################################################

    enrollment = observation_period.select(        observation_period['person_id'].alias("PATID"),
                                                   observation_period['observation_period_start_date'].alias("ENR_START_DATE"),
                                                   observation_period['observation_period_end_date'].alias("ENR_END_DATE"),
                                                   observation_period['chart'].alias("CHART"),
                                                   observation_period['enrollment_basis'].alias("ENR_BASIS"),
                                                   
                                                    )

    ###################################################################################################################################

# Create the output file

###################################################################################################################################

    cf.write_pyspark_output_file(
                      payspark_df = enrollment,
                      output_file_name = "formatted_enrollment.csv",
                      output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'enrollment_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')


