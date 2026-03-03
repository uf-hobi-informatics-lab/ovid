###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the dispensing table

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

    # Loading the drug_exposure table to be converted to the dispensing table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'

    drug_exposure_table_name       = 'drug_exposure.csv'
    drug_exposure_sup_table_name   = 'drug_exposure_sup.csv'

    drug_exposure = spark.read.load(input_data_folder_path+drug_exposure_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    drug_exposure_sup = spark.read.load(input_data_folder_path+drug_exposure_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("drug_exposure_id",      "drug_exposure_id_sup")

    concept       = cf.spark_read(concept_table_path,spark)
    drug_concept = concept.filter(concept.domain_id == 'Drug').withColumnRenamed("concept_name", "drug_concept_name").withColumnRenamed("vocabulary_id", "drug_vocabulary_id").withColumnRenamed("concept_code", "drug_concept_code")
    dispensed_drug_type_concept = concept.filter((col("domain_id") == "Type Concept") & (col("concept_name").rlike(r"(?i)dispens")))

    joined_drug_exposure = drug_exposure.join(dispensed_drug_type_concept, dispensed_drug_type_concept['concept_id']==drug_exposure['drug_type_concept_id'], how='inner').drop("concept_id")\
                                            .join(drug_concept, drug_concept['concept_id']==drug_exposure['drug_concept_id'], how='left').drop("concept_id")\
                                            .join(drug_exposure_sup, drug_exposure_sup['drug_exposure_id_sup']== drug_exposure['drug_exposure_id'], how = 'left')\




    ###################################################################################################################################

    #Converting the fileds to PCORNet dispensing Format

    ###################################################################################################################################

    dispensing = joined_drug_exposure.select(       joined_drug_exposure['drug_exposure_id'].alias("DISPENSINGID"),
                                                    joined_drug_exposure['person_id'].alias("PATID"),
                                                    lit('').alias("PRESCRIBINGID"),
                                                    cf.format_date_udf(joined_drug_exposure['drug_exposure_start_date']).alias("DISPENSE_DATE"),
                                                    joined_drug_exposure['drug_concept_code'].alias("NDC"),
                                                    lit('OD').alias("DISPENSE_SOURCE"),
                                                    lit('').alias('DISPENSE_SUP'),
                                                    joined_drug_exposure['quantity'].alias("DISPENSE_AMT"),
                                                    joined_drug_exposure['dose'].alias("DISPENSE_DOSE_DISP"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("DISPENSE_DOSE_DISP_UNIT"),
                                                    joined_drug_exposure['sig'].alias("DISPENSE_ROUTE"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RAW_NDC"),
                                                    joined_drug_exposure['dose'].alias("RAW_DISPENSE_DOSE_DISP"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("RAW_DISPENSE_DOSE_DISP_UNIT"),
                                                    joined_drug_exposure['sig'].alias("RAW_DISPENSE_ROUTE"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RAW_NDC_CODE"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RAW_RXNORM_CODE"),

                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    #################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = dispensing,
                        output_file_name = "formatted_dispensing.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'dispensing_formatter.py' ,
                            text    = str(e))








