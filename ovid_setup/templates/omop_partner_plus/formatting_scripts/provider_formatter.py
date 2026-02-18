###################################################################################################################################

# This script will convert an OMOP omop_provider table to a PCORnet format as the pcornet_provider table

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




try:


###################################################################################################################################

# Loading the omop_provider table to be converted to the pcornet_provider table
# loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'

    omop_provider_table_name       = 'provider.csv'
    omop_provider_sup_table_name   = 'provider_sup.csv'



    concept = cf.spark_read(concept_table_path,spark)
    provider_specialty_concept = concept.filter((col("domain_id") == "Provider") & ((col("vocabulary_id") == "NUCC"))).withColumnRenamed("concept_code", "provider_specialty_concept_code")
    geneder_concept            = concept.filter(concept.domain_id == 'Gender').withColumnRenamed("concept_code", "gender_concept_code")
    omop_provider     = spark.read.load(input_data_folder_path+omop_provider_table_name    ,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    omop_provider_sup = spark.read.load(input_data_folder_path+omop_provider_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    omop_provider_sup = omop_provider_sup.withColumnRenamed("provider_id", "provider_id_sup")





    ###################################################################################################################################

    #Converting the fileds to PCORNet pcornet_provider Format

    ###################################################################################################################################


    joined_omop_provider  = omop_provider.join(omop_provider_sup, omop_provider_sup['provider_id_sup']             == omop_provider['provider_id'], how='left')\
                                        .join(provider_specialty_concept,provider_specialty_concept['concept_id'] == omop_provider['specialty_concept_id'],        how = 'left')\
                                        .join(geneder_concept,  geneder_concept['concept_id']                     == omop_provider['gender_concept_id'],   how = 'left')\



    pcornet_provider = joined_omop_provider.select( joined_omop_provider['provider_id'].alias("PROVIDERID"),
                                                    joined_omop_provider['gender_concept_code'].alias("PROVIDER_SEX"),
                                                    joined_omop_provider['provider_specialty_concept_code'].alias("PROVIDER_SPECIALTY_PRIMARY"),
                                                    joined_omop_provider['npi'].alias("PROVIDER_NPI"),
                                                    get_provider_npi_flag_udf(joined_omop_provider['npi']).alias("PROVIDER_NPI_FLAG"),
                                                    joined_omop_provider['specialty_source_value'].alias("RAW_PROVIDER_SPECIALTY_PRIMARY"),
                                                    
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
                            partner = partner_name,
                            job     = 'provider_formatter.py' ,
                            text    = str(e))
















