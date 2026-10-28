###################################################################################################################################

# This script will convert an OMOP person table to a PCORnet format as the demographic table

###################################################################################################################################


import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse
import sys


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

    # Loading the person table to be converted to the pat_relationship table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'


    pat_relationship_table_name       = 'person_relationship.csv'


    pat_relationship = spark.read.load(input_data_folder_path+pat_relationship_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


    ###################################################################################################################################

    #Converting the fileds to PCORNet pat_relationship Format

    ###################################################################################################################################

    pat_relationship = pat_relationship.select(     pat_relationship['person_id_1'].alias("PATID_1"),
                                                    pat_relationship['person_id_2'].alias("PATID_2"),
                                                    pat_relationship['relationship_type'].alias("RELATIONSHIP_TYPE"),
                                                    pat_relationship['relationship_start'].alias("RELATIONSHIP_START"),
                                                    pat_relationship['relationship_end'].alias("RELATIONSHIP_END")


                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################


    cf.write_pyspark_output_file(
                        payspark_df = pat_relationship,
                        output_file_name = "formatted_pat_relationship.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'pat_relationship_formatter.py' ,
                            text    = str(e))







