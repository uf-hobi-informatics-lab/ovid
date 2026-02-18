###################################################################################################################################

# This script will convert an OMOP obsrevation_period table to a PCORnet format as the enrollment table

###################################################################################################################################


import pyspark
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

    # Loading the obsrevation_period table to be converted to the enrollment table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'


    obsrevation_period_table_name       = 'observation_period.csv'
    obsrevation_period_sup_table_name   = 'observation_period_sup.csv'

    obsrevation_period = spark.read.load(input_data_folder_path+obsrevation_period_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    obsrevation_period_sup = spark.read.load(input_data_folder_path+obsrevation_period_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')\
                                        .withColumnRenamed("observation_period_id", "observation_period_sup_id")


    joined_obsrevation_period = obsrevation_period.join(obsrevation_period_sup, obsrevation_period_sup['observation_period_sup_id']==obsrevation_period['observation_period_id'], how = 'left')
                                                    
                                                

    ###################################################################################################################################

    #Converting the fileds to PCORNet enrollment Format

    ###################################################################################################################################

    enrollment = joined_obsrevation_period.select(  joined_obsrevation_period['person_id'].alias("PATID"),
                                                    joined_obsrevation_period['observation_period_start_date'].alias("ENR_START_DATE"),
                                                    joined_obsrevation_period['observation_period_end_date'].alias("ENR_END_DATE"),
                                                    joined_obsrevation_period['chart'].alias("CHART"),
                                                    joined_obsrevation_period['enrollment_basis'].alias("ENR_BASIS"),
                                                    
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
                            partner = 'omop_partner_plus',
                            job     = 'enrollment_formatter.py' ,
                            text    = str(e))








