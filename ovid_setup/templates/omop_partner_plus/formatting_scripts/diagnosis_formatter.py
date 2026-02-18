###################################################################################################################################

# This script will convert an OMOP condition_occurrence table to a PCORnet format as the diagnosis table

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

    # Loading the condition_occurrence, visit_occurrence and the supplemental diagnosis table to be converted to the diagnosis table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'
    
    

    condition_occurrence_table_name   = 'condition_occurrence.csv'
    visit_occurrence_table_name       = 'visit_occurrence.csv'
    condition_occurrence_sup_table_name          = 'condition_occurrence_sup.csv'

    concept = cf.spark_read(concept_table_path,spark)

    

    condition_occurrence = spark.read.load(input_data_folder_path+condition_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    
    visit_occurrence = spark.read.load(input_data_folder_path+visit_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').select("person_id", "visit_occurrence_id", "visit_start_date", "visit_concept_id", "provider_id")
    visit_occurrence = visit_occurrence.withColumnRenamed("visit_occurrence_id", "source_visit_occurrence_id").withColumnRenamed("person_id", "visit_person_id")\
                                                                                                              .withColumnRenamed("provider_id", "visit_provider_id")\
                                                                                                              
    condition_occurrence_sup = spark.read.load(input_data_folder_path+condition_occurrence_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    condition_occurrence_sup = condition_occurrence_sup.withColumnRenamed("person_id", "person_sup_id")\
                                 .withColumnRenamed("condition_occurrence_id", "condition_sup_id")

    
    filter_values = ["32840", "32865", "38000245"]  # Only rows where condition_type_concept_id are not in ["32840", "32865", "38000245"] "
    filtered_condition_occurrence = condition_occurrence.filter(~col("condition_type_concept_id").isin(filter_values))


    #for DX:
    #concept_code will pull the source DX identifier from the source vocabulary, whereas concept_name will pull the description in text form of the diagnosis
    dx_concept = concept.filter(concept.domain_id == 'Condition').withColumnRenamed("concept_code", "diagnosis_concept_code")\
                                                                .withColumnRenamed("concept_name", "raw_diagnosis_concept")\
                                                                .withColumnRenamed("vocabulary_id", "dx_vocabulary")
    #for DX_ORIGIN:
    dx_origin_concept = concept.filter(concept.domain_id == 'Type Concept').withColumnRenamed("concept_name", "diagnosis_origin_concept_code")
    
    #FOR ENC_TYPE
    visit_enc_type = concept.filter(concept.domain_id == 'Visit').withColumnRenamed("concept_name", "visit_enc_type_code")

    joined_condition_occurrence = filtered_condition_occurrence.join(visit_occurrence, visit_occurrence['source_visit_occurrence_id'] == filtered_condition_occurrence['visit_occurrence_id'], how='left')\
                                            .join(dx_concept, dx_concept['concept_id'] == filtered_condition_occurrence['condition_concept_id'], how='left')\
                                            .join(dx_origin_concept,  dx_origin_concept['concept_id'] == filtered_condition_occurrence['condition_type_concept_id'], how='left')\
                                            .join(visit_enc_type, visit_enc_type['concept_id'] == visit_occurrence['visit_concept_id'], how='left')\
                                            .join(condition_occurrence_sup, condition_occurrence_sup['condition_sup_id'] == filtered_condition_occurrence["condition_occurrence_id"], how='left')


        

    ###################################################################################################################################

    #Converting the fileds to PCORNet Diagnosis Format

    ###################################################################################################################################

    diagnosis = joined_condition_occurrence.select( 
        
                                                    joined_condition_occurrence['condition_occurrence_id'].alias("DIAGNOSISID"),
                                                    joined_condition_occurrence['person_id'].alias("PATID"),
                                                    joined_condition_occurrence['source_visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    joined_condition_occurrence['visit_enc_type_code'].alias("ENC_TYPE"),
                                                    cf.format_date_udf(joined_condition_occurrence['visit_start_date']).alias("ADMIT_DATE"),
                                                    joined_condition_occurrence['visit_provider_id'].alias("PROVIDERID"),
                                                    joined_condition_occurrence['diagnosis_concept_code'].alias("DX"),
                                                    joined_condition_occurrence['dx_vocabulary'].alias("DX_TYPE"),
                                                    cf.format_date_udf(joined_condition_occurrence['condition_start_date']).alias("DX_DATE"),
                                                    joined_condition_occurrence['condition_status_source_value'].alias("DX_SOURCE"),
                                                    joined_condition_occurrence['diagnosis_origin_concept_code'].alias("DX_ORIGIN"),
                                                    lit('').alias("PDX"),
                                                    joined_condition_occurrence['Poa'].alias("DX_POA"),
                                                    joined_condition_occurrence['raw_diagnosis_concept'].alias("RAW_DX"),
                                                    joined_condition_occurrence['dx_vocabulary'].alias("RAW_DX_TYPE"),
                                                    joined_condition_occurrence['condition_status_source_value'].alias("RAW_DX_SOURCE"),
                                                    lit('').alias("RAW_PDX"),
                                                    joined_condition_occurrence['Poa'].alias("RAW_DX_POA"),
                                            
                                                
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = diagnosis,
                        output_file_name = "formatted_diagnosis.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name,
                            job     = 'diagnosis_formatter.py' )

    cf.print_with_style(str(e), 'danger red')






