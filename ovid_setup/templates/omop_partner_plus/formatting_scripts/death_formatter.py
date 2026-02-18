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
    # Loading the omop_death table to be converted to the pcornet_death table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping
    ###################################################################################################################################

    input_data_folder_path                  = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path       = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                      = f'/app/common/cdm/omop_5_3/CONCEPT.csv'


    omop_death_table_name = 'death.csv'
    omop_death_sup_table_name = 'death_sup.csv'


    omop_death = spark.read.load(input_data_folder_path+omop_death_table_name, format="csv", sep="\t", inferSchema="false", header="true", quote='"')
    omop_death_sup = spark.read.load(input_data_folder_path+omop_death_sup_table_name, format="csv", sep="\t", inferSchema="false", header="true", quote='"')\
                                .withColumnRenamed("person_id", "person_sup_id")
    
    concept = cf.spark_read(concept_table_path,spark)

    death_source = concept.filter(concept.domain_id == 'Type Concept').withColumnRenamed("concept_name", "death_source_name")

    joined_omop_death = omop_death.join(omop_death_sup, omop_death_sup['person_sup_id']== omop_death['person_id'], how = 'left')\
                                  .join(death_source, death_source['concept_id'] == omop_death['death_type_concept_id'], how = 'left')


    ###################################################################################################################################
    # Converting the fields to PCORNet pcornet_death Format
    ###################################################################################################################################

    death = joined_omop_death.select(
        joined_omop_death['person_id'].alias("PATID"),
        joined_omop_death['death_date'].alias("DEATH_DATE"),
        joined_omop_death['death_date_impute'].alias("DEATH_DATE_IMPUTE"),
        joined_omop_death['death_source_name'].alias("DEATH_SOURCE"),
        joined_omop_death['death_match_confidence'].alias("DEATH_MATCH_CONFIDENCE"),
    )

    ###################################################################################################################################
    # Create the output files
    ###################################################################################################################################

    cf.write_pyspark_output_file(
        payspark_df=death,
        output_file_name="formatted_death.csv",
        output_data_folder_path=formatter_output_data_folder_path)



    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name,
                            job     = 'death_formatter.py' ,
                            text    = str(e))
