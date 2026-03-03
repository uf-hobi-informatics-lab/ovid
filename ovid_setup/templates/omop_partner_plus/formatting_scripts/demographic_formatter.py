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

    # Loading the person table to be converted to the demographic table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'


    person_table_name       = 'person.csv'
    person_sup_table_name   = 'person_sup.csv'

    person = spark.read.load(input_data_folder_path+person_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    person_sup = spark.read.load(input_data_folder_path+person_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("person_id", "person_sup_id")
    concept = cf.spark_read(concept_table_path,spark)

    # person.show()
    # person_sup.show()

    # sys.exit()

    gender_concept = concept.filter(concept.domain_id == 'Gender').withColumnRenamed("concept_code", "gender_concept_code")
    ethnicity_concept = concept.filter(concept.domain_id == 'Ethnicity').withColumnRenamed("concept_code", "ethnicity_concept_code")
    race_concept = concept.filter(concept.domain_id == 'Race').withColumnRenamed("concept_code", "race_concept_code")

    joined_person = person.join(gender_concept, gender_concept['concept_id']==person['gender_concept_id'], how='left').drop("concept_id")\
                                                .join(ethnicity_concept, ethnicity_concept['concept_id']==person['ethnicity_concept_id'], how='left')\
                                                .join(race_concept, race_concept['concept_id']== person['race_concept_id'], how = 'left')\
                                                .join(person_sup, person_sup['person_sup_id']== person['person_id'], how = 'left')
                                                






    ###################################################################################################################################

    #Converting the fileds to PCORNet demographic Format

    ###################################################################################################################################

    demographic = joined_person.select(             joined_person['person_id'].alias("PATID"),
                                                    cf.format_date_udf("birth_datetime").alias("BIRTH_DATE"),
                                                    cf.format_time_udf("birth_datetime").alias("BIRTH_TIME"),
                                                    joined_person['gender_concept_code'].alias("SEX"),
                                                    joined_person['sexual_orientation'].alias("SEXUAL_ORIENTATION"),
                                                    joined_person['sex'].alias("GENDER_IDENTITY"),
                                                    joined_person['ethnicity_concept_code'].alias("HISPANIC"),
                                                    joined_person['race_concept_code'].alias("RACE"),
                                                    lit('N').alias("BIOBANK_FLAG"),
                                                    joined_person['spoken_language'].alias("PAT_PREF_LANGUAGE_SPOKEN"),
                                                    joined_person['sex_source_value'].alias("RAW_SEX"),
                                                    joined_person['sexual_orientation_source_value'].alias("RAW_SEXUAL_ORIENTATION"),
                                                    joined_person['gender_source_value'].alias("RAW_GENDER_IDENTITY"),
                                                    joined_person['ethnicity_source_value'].alias("RAW_HISPANIC"),
                                                    joined_person['race_source_value'].alias("RAW_RACE"),
                                                    joined_person['spoken_language_source_value'].alias("RAW_PAT_PREF_LANGUAGE_SPOKEN"),
                                                    lit('').alias("ZIP_CODE"),


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
                            partner = 'omop_partner_plus',
                            job     = 'demographic_formatter.py' ,
                            text    = str(e))







