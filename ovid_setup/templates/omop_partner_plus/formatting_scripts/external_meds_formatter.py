###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the external_meds table

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

    # Loading the drug_exposure table to be converted to the external_meds table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/omop_cdm/CONCEPT.csv'


    drug_exposure_table_name       = 'drug_exposure.csv'
    drug_exposure_sup_table_name   = 'drug_exposure_sup.csv'

    drug_exposure = spark.read.load(input_data_folder_path+drug_exposure_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    drug_exposure_sup = spark.read.load(input_data_folder_path+drug_exposure_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("drug_exposure_id", "drug_exposure_id_sup")

    concept      = cf.spark_read_omop_concept(concept_table_path,spark)
    drug_concept = concept.filter((concept.domain_id == 'Drug') & (concept.vocabulary_id == 'RxNorm') ).withColumnRenamed("concept_name", "drug_concept_name").withColumnRenamed("vocabulary_id", "drug_vocabulary_id").withColumnRenamed("concept_code", "drug_concept_code").withColumnRenamed("concept_id", "drug_concept_lookup_id")

    external_meds_drug_type_concept = concept.filter((col("domain_id") == "Type Concept")).withColumnRenamed("concept_id", "external_meds_drug_type_concept_lookup_id")

    route_concept = concept.filter(concept.domain_id == 'Route').withColumnRenamed("concept_name", "route_concept_name").withColumnRenamed("concept_id", "route_concept_lookup_id")


    # 32843 = OMOP Type Concept "External CDM instance" (medications sourced outside the partner EHR)
    filtered_drug_exposure = drug_exposure.filter(drug_exposure.drug_type_concept_id.isin('32843'))

    joined_drug_exposure = filtered_drug_exposure.join(external_meds_drug_type_concept, external_meds_drug_type_concept['external_meds_drug_type_concept_lookup_id']==filtered_drug_exposure['drug_type_concept_id'], how='inner')\
                                            .join(drug_exposure_sup, drug_exposure_sup['drug_exposure_id_sup']== filtered_drug_exposure['drug_exposure_id'], how = 'left')\
                                            .join(drug_concept, drug_concept['drug_concept_lookup_id']==filtered_drug_exposure['drug_concept_id'], how='left')\
                                            .join(route_concept, route_concept['route_concept_lookup_id']== filtered_drug_exposure['route_concept_id'], how = 'left')


    ###################################################################################################################################

    #Converting the fields to PCORNet external meds Format

    ###################################################################################################################################

    external_meds = joined_drug_exposure.select(    joined_drug_exposure['drug_exposure_id'].alias("EXTMEDID"),
                                                    joined_drug_exposure['person_id'].alias("PATID"),
                                                    joined_drug_exposure['drug_exposure_record_date'].alias("EXT_RECORD_DATE"),
                                                    joined_drug_exposure['drug_exposure_start_date'].alias("EXT_PAT_START_DATE"),
                                                    joined_drug_exposure['drug_exposure_end_date'].alias("EXT_END_DATE"),
                                                    joined_drug_exposure['drug_exposure_end_date'].alias("EXT_PAT_END_DATE"),
                                                    joined_drug_exposure['days_supply'].alias("EXT_DOSE"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("EXT_DOSE_ORDERED_UNIT"),
                                                    lit(None).cast(StringType()).alias("EXT_DOSE_FORM"),
                                                    joined_drug_exposure['route_concept_name'].alias("EXT_ROUTE"),
                                                    lit('02').alias("EXT_BASIS"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RXNORM_CUI"),
                                                    lit('OD').alias("EXTMED_SOURCE"),
                                                    joined_drug_exposure['drug_concept_name'].alias("RAW_EXT_MED_NAME"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RAW_RXNORM_CUI"),
                                                    lit(None).cast(StringType()).alias("RAW_EXT_NDC"),
                                                    joined_drug_exposure['days_supply'].alias("RAW_EXT_DOSE"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("RAW_EXT_DOSE_UNIT"),
                                                    joined_drug_exposure['route_source_value'].alias("RAW_EXT_ROUTE"),
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = external_meds,
                        output_file_name = "formatted_external_meds.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'external_meds_formatter.py' ,
                            text    = str(e))
