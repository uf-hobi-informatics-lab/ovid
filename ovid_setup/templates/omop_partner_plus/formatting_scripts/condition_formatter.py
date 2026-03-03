###################################################################################################################################

# This script will convert an OMOP condition_occurrence table to a PCORnet format as the condition table

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
    
    # Loading the condition_occurrence and visit_occurrence table to be converted to the condition table

    ###################################################################################################################################



    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'

    condition_occurrence_table_name         = 'condition_occurrence.csv'
    visit_occurrence_table_name             = 'visit_occurrence.csv'
    #condition_occurrence_sup_table_name     = 'condition_sup.csv'

    concept = cf.spark_read(concept_table_path,spark)

    condition_occurrence = spark.read.load(input_data_folder_path+condition_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    
    visit_occurrence = spark.read.load(input_data_folder_path+visit_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    visit_occurrence = visit_occurrence.withColumnRenamed("visit_occurrence_id", "source_visit_occurrence_id").withColumnRenamed("person_id", "visit_person_id")\
                                                                                                              .withColumnRenamed("provider_id", "visit_provider_id")\

    
    #for CONDITION:
    condition_concept = concept.filter(concept.domain_id == 'Condition').withColumnRenamed("concept_code", "condition_concept_code")\
                                                                        .withColumnRenamed("concept_name", "condition_concept_name")\
                                                                .withColumnRenamed("vocabulary_id", "condition_vocabulary_id")

    #for RAW_CONDITION_SOURCE:
    condition_concept_source = concept.filter(concept.domain_id == 'Type Concept').withColumnRenamed("concept_name", "raw_condition_source")\
                                                                

    filter_values = ["32840", "32865", "38000245"] # Only rows where condition_type_concept_id are in condition_type_concept_id, or 32840: "EHR problem list", or 32865: "Patient self-report"
    #both concept id's above are from the OMOP Type Concept Vocabulary
    filtered_condition_occurrence = condition_occurrence.filter(col("condition_type_concept_id").isin(filter_values))

    joined_condition_occurrence = filtered_condition_occurrence.join(visit_occurrence, visit_occurrence['source_visit_occurrence_id'] == filtered_condition_occurrence['visit_occurrence_id'], how='left')\
                                            .join(condition_concept, condition_concept['concept_id'] == filtered_condition_occurrence['condition_concept_id'], how='left')\
                                            .join(condition_concept_source,  condition_concept_source['concept_id'] == filtered_condition_occurrence['condition_type_concept_id'], how='left')\
                                            #.join(condition_sup, condition_sup['condition_sup_id'] == filtered_condition_occurrence["condition_occurrence_id"], how='left')



    ###################################################################################################################################

    # This function will determine the condition status based on the condition end date
    ###################################################################################################################################



    def PCORI_condition_status( condition_end_date):
            if condition_end_date is None or condition_end_date == '':
                return "AC"
            else:
                return "RS"

    PCORI_condition_status_udf = udf(PCORI_condition_status, StringType())


    ###################################################################################################################################

    #Converting the fileds to PCORNet condition Format

    ###################################################################################################################################

    condition = joined_condition_occurrence.select(  
                                                    joined_condition_occurrence['condition_occurrence_id'].alias("CONDITIONID"),
                                                    joined_condition_occurrence['person_id'].alias("PATID"),
                                                    joined_condition_occurrence['source_visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    cf.format_date_udf(joined_condition_occurrence['visit_start_date']).alias("REPORT_DATE"),
                                                    cf.format_date_udf(joined_condition_occurrence['condition_end_date']).alias("RESOLVE_DATE"),
                                                    cf.format_date_udf(joined_condition_occurrence['condition_start_date']).alias("ONSET_DATE"),
                                                    PCORI_condition_status_udf(joined_condition_occurrence['condition_end_date']).alias("CONDITION_STATUS"),
                                                    joined_condition_occurrence['condition_concept_code'].alias("CONDITION"),
                                                    joined_condition_occurrence['condition_vocabulary_id'].alias("CONDITION_TYPE"),
                                                    lit("HC").alias("CONDITION_SOURCE"),
                                                    PCORI_condition_status_udf(joined_condition_occurrence['condition_end_date']).alias("RAW_CONDITION_STATUS"),
                                                    joined_condition_occurrence['condition_concept_name'].alias("RAW_CONDITION"),
                                                    joined_condition_occurrence['condition_vocabulary_id'].alias("RAW_CONDITION_TYPE"),
                                                    joined_condition_occurrence['raw_condition_source'].alias("RAW_CONDITION_SOURCE"),

                                                
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = condition,
                        output_file_name = "formatted_condition.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'condition_formatter.py',
                            text    = str(e))








