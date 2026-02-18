###################################################################################################################################

# This script will convert an OMOP procedure_occurrence table to a PCORnet format as the procedures table

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
input_data_folder = args.data_folder


try:

    ###################################################################################################################################

    # Loading the procedure_occurrence table to be converted to the procedures table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'

    procedure_occurrence_table_name   = 'procedure_occurrence.csv'
    visit_occurrence_table_name       = 'visit_occurrence.csv'


    procedure_occurrence = spark.read.load(input_data_folder_path+procedure_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
                                                                                                                                        


    visit_occurrence = spark.read.load(input_data_folder_path+visit_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    visit_occurrence = visit_occurrence.withColumnRenamed("visit_occurrence_id", "source_visit_occurrence_id").withColumnRenamed("person_id", "procedure_person_id")\
                                                                                                            .withColumnRenamed("provider_id", "visit_provider_id")



    concept = cf.spark_read(concept_table_path,spark)
    procedure_concept = concept.filter(concept.domain_id == 'Procedure').withColumnRenamed("concept_code", "procedure_concept_code")\
                                                                        .withColumnRenamed("concept_name", "procedure_name")\
                                                                        .withColumnRenamed("vocabulary_id", "procedure_vocabulry_id")

    visit_concept = concept.filter(concept.domain_id == 'Visit').withColumnRenamed("concept_name", "visit_concept_name")




    joined_procedure_occurrence   = procedure_occurrence.join(procedure_concept, procedure_concept['concept_id']     ==procedure_occurrence['procedure_concept_id'], how='left')\
                                                        .join(visit_occurrence,     visit_occurrence['source_visit_occurrence_id'] == procedure_occurrence['visit_occurrence_id'],        how = 'left')\
                                                        .join(visit_concept,  visit_concept['concept_id']      == visit_occurrence['visit_concept_id'],   how = 'left')\






    ###################################################################################################################################

    #Converting the fileds to PCORNet procedures Format

    ###################################################################################################################################

    procedures = joined_procedure_occurrence.select(joined_procedure_occurrence['procedure_occurrence_id'].alias("PROCEDURESID"),
                                                    joined_procedure_occurrence['person_id'].alias("PATID"),
                                                    joined_procedure_occurrence['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    joined_procedure_occurrence['visit_concept_name'].alias("ENC_TYPE"),
                                                    cf.format_date_udf(joined_procedure_occurrence['visit_start_date']).alias("ADMIT_DATE"),
                                                    joined_procedure_occurrence['provider_id'].alias("PROVIDERID"),
                                                    cf.format_date_udf(joined_procedure_occurrence['procedure_date']).alias("PX_DATE"),
                                                    joined_procedure_occurrence['procedure_concept_code'].alias("PX"),
                                                    joined_procedure_occurrence['procedure_vocabulry_id'].alias("PX_TYPE"),
                                                    lit('OD').alias("PX_SOURCE"),
                                                    lit('').alias("PPX"),
                                                    joined_procedure_occurrence['procedure_source_value'].alias("RAW_PX"),
                                                    joined_procedure_occurrence['vocabulary_id'].alias("RAW_PX_TYPE"),
                                                    lit('').alias("RAW_PPX"),                                             
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = procedures,
                        output_file_name = "formatted_procedures.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'procedures_formatter.py' ,
                            text    = str(e))







