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
from pyspark.sql import functions as F

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

        # Loading the person table to be converted to the demographic table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

    ## Using person since partner is submitting OMOP data, can also use wildcards for multiple files with the same name
    person_table_name       = 'Person.txt'
    #lds_address_table       = 'Lds_address_history.txt'

    person = spark.read.load(input_data_folder_path+person_table_name,format="csv", sep=",", inferSchema="false", header="true", quote= '"')
    #address = spark.read.load(input_data_folder_path+lds_address_table,format="csv", sep=",", inferSchema="false", header="true", quote= '"')

        ###################################################################################################################################

    def get_date_from_datetime(val_date):
            # Parse the input string into a datetime object
            datetime_object = datetime.strptime(val_date, "%Y-%m-%d %H:%M:%S")

            # Format the datetime object as a string in "yyyy-mm-dd" format
            formatted_date = datetime_object.strftime("%Y-%m-%d")

            return formatted_date
        
    convert_and_format_date_udf = udf(get_date_from_datetime, StringType())


    def get_time_from_datetime(val_time):
            # Parse the input string into a datetime object
            datetime_object = datetime.strptime(val_time, "%Y-%m-%d %H:%M:%S")

            # Format the datetime object as a string in "yyyy-mm-dd" format
            formatted_time = datetime_object.strftime("%H:%M")

            return formatted_time
        
    convert_and_format_time_udf = udf(get_time_from_datetime, StringType())   

        ###################################################################################################################################
    """    def get_most_recent_zipcode(person, address):
                # Join person and address dataframes on person_id
                joined_df = person.join(address, person.person_id == address.person_id, "inner")

                # Group by person_id and find the row with the maximum start_date for each group
                result_df = joined_df.groupBy("person_id").agg(
                F.max("start_date").alias("most_recent_start_date"),
                F.first("ZIP_CODE").alias("most_recent_zip")  # Assuming ZIP_CODE is the same for each person_id in the group
                )

                # Collect the result as a list of dictionaries
                result_list = result_df.collect()

                # Convert the result list to a dictionary mapping person_id to the most recent ZIP_CODE
                result_dict = {row["person_id"]: row["most_recent_zip"] for row in result_list}

                return result_dict"""
            
        

        ###################################################################################################################################

        #Converting the fileds to PCORNet demographic Format

        ###################################################################################################################################
    demographic = person.select(                        person['person_id'].alias("PATID"),
                                                        convert_and_format_date_udf(person['birth_datetime']).alias("BIRTH_DATE"),
                                                        convert_and_format_time_udf(person['birth_datetime']).alias("BIRTH_TIME"),
                                                        person['sex'].alias("SEX"),
                                                        person['sexual_orientation'].alias("SEXUAL_ORIENTATION"),
                                                        person['sex'].alias("GENDER_IDENTITY"),
                                                        person['ethnicity_source_value'].alias("HISPANIC"),
                                                        person['race'].alias("RACE"),
                                                        lit('N').alias("BIOBANK_FLAG"),
                                                        person['spoken_language'].alias("PAT_PREF_LANGUAGE_SPOKEN"),
                                                        person['sex'].alias("RAW_SEX"),
                                                        lit("").alias("RAW_SEXUAL_ORIENTATION"),
                                                        lit("").alias("RAW_GENDER_IDENTITY"),
                                                        person['ethnicity_source_value'].alias("RAW_HISPANIC"),
                                                        person['race_source_value'].alias("RAW_RACE"),
                                                        person['spoken_language_source_value'].alias("RAW_PAT_PREF_LANGUAGE_SPOKEN"),
                                                        lit("").alias("ZIP_CODE")



                                                            )

        ###################################################################################################################################

        # Create the output file

        ###################################################################################################################################


    cf.write_pyspark_output_file(
                            payspark_df = demographic,
                            output_file_name = "formatted_demographic.csv",
                            output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'demographic_formatter.py' ,
                            text    = str(e))


    

