###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the med_admin table

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

    # Loading the drug_exposure table to be converted to the med_admin table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'

    drug_exposure_table_name   = 'drug_exposure.csv'
    drug_exposure_sup_table_name   = 'drug_exposure_sup.csv'

    drug_exposure = spark.read.load(input_data_folder_path+drug_exposure_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    drug_exposure_sup = spark.read.load(input_data_folder_path+drug_exposure_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("drug_exposure_id",      "drug_exposure_id_sup")

    concept       = cf.spark_read(concept_table_path,spark)
    drug_concept = concept.filter(concept.domain_id == 'Drug').withColumnRenamed("concept_name", "drug_concept_name").withColumnRenamed("vocabulary_id", "drug_vocabulary_id").withColumnRenamed("concept_code", "drug_concept_code")
    med_admin_drug_type_concept = concept.filter((col("concept_class_id") == "Drug Type") & (col("concept_name").rlike(r"(?i)admin") ))
    route_concept = concept.filter(concept.domain_id == 'Route').withColumnRenamed("concept_name", "route_concept_name")





    joined_drug_exposure = drug_exposure.join(med_admin_drug_type_concept, med_admin_drug_type_concept['concept_id']==drug_exposure['drug_type_concept_id'], how='inner').drop("concept_id")\
                                            .join(drug_concept, drug_concept['concept_id']==drug_exposure['drug_concept_id'], how='left').drop("concept_id")\
                                            .join(drug_exposure_sup, drug_exposure_sup['drug_exposure_id_sup']== drug_exposure['drug_exposure_id'], how = 'left')\
                                            .join(route_concept, route_concept['concept_id']== drug_exposure['route_concept_id'], how = 'left')

                                            



    ###################################################################################################################################

    #Converting the fileds to PCORNet med_admin Format

    ###################################################################################################################################

    med_admin = joined_drug_exposure.select(      joined_drug_exposure['drug_exposure_id'].alias("MEDADMINID"),
                                                    joined_drug_exposure['person_id'].alias("PATID"),
                                                    joined_drug_exposure['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    lit('').alias("PRESCRIBINGID"),
                                                    joined_drug_exposure['provider_id'].alias("MEDADMIN_PROVIDERID"),
                                                    joined_drug_exposure['drug_exposure_start_date'].alias("MEDADMIN_START_DATE"),
                                                    cf.get_time_from_datetime_udf(joined_drug_exposure['drug_exposure_start_datetime']).alias("MEDADMIN_START_TIME"),
                                                    joined_drug_exposure['drug_exposure_end_date'].alias("MEDADMIN_STOP_DATE"),
                                                    cf.get_time_from_datetime_udf(joined_drug_exposure['drug_exposure_end_datetime']).alias("MEDADMIN_STOP_TIME"),
                                                    joined_drug_exposure['drug_vocabulary_id'].alias("MEDADMIN_TYPE"),
                                                    joined_drug_exposure['drug_concept_code'].alias("MEDADMIN_CODE"),
                                                    joined_drug_exposure['days_supply'].alias("MEDADMIN_DOSE_ADMIN"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("MEDADMIN_DOSE_ADMIN_UNIT"),
                                                    joined_drug_exposure['route_concept_name'].alias("MEDADMIN_ROUTE"),
                                                    lit('OD').alias("MEDADMIN_SOURCE"),
                                                    joined_drug_exposure['drug_concept_name'].alias("RAW_MEDADMIN_MED_NAME"),
                                                    joined_drug_exposure['drug_source_value'].alias("RAW_MEDADMIN_CODE"),
                                                    joined_drug_exposure['days_supply'].alias("RAW_MEDADMIN_DOSE_ADMIN"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("RAW_MEDADMIN_DOSE_ADMIN_UNIT"),
                                                    joined_drug_exposure['route_source_value'].alias("RAW_MEDADMIN_ROUTE"),

                                                    
                                                    
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = med_admin,
                        output_file_name = "formatted_med_admin.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'med_admin_formatter.py',
                            text = str(e) )

    # cf.print_with_style(str(e), 'danger red')








