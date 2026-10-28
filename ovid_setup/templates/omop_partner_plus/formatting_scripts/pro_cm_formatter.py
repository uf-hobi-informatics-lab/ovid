###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the prescribing table

###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
from ovid_logging import log_error
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

    # Loading the drug_exposure table to be converted to the prescribing table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/omop_cdm/CONCEPT.csv'


    pro_cm_table_name   = 'pro_cm.csv'
    visit_occurrence_table_name       = 'visit_occurrence.csv'
   

    pro_cm = spark.read.load(input_data_folder_path+pro_cm_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    #pro_cm = spark.read.parquet(input_data_folder_path+pro_cm_table_name,)
   

    visit_occurrence = spark.read.load(input_data_folder_path+visit_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    visit_occurrence = visit_occurrence.withColumnRenamed("visit_occurrence_id", "source_visit_occurrence_id").withColumnRenamed("person_id", "visit_person_id")\
                                                                                                              .withColumnRenamed("provider_id", "visit_provider_id")\
    

    #concept       = cf.spark_read(concept_table_path,spark)
    #pro_cm_concept = concept.filter(concept.domain_id == 'Observation').withColumnRenamed("concept_name", "pro_cm_concept_name").withColumnRenamed("vocabulary_id", "pro_cm_vocabulary_id").withColumnRenamed("concept_code", "pro_cm_concept_code")
    #drug_concept = broadcast(drug_concept)

    
    


    joined_pro_cm = pro_cm.join(visit_occurrence, visit_occurrence['source_visit_occurrence_id'] == pro_cm['visit_occurrence_id'], how='left')\
                                            
                                           




    ###################################################################################################################################

    #Converting the fields to PCORNet prescribing Format

    ###################################################################################################################################

    pro_cm = joined_pro_cm.select(    joined_pro_cm['pro_cm_id'].alias("PRO_CM_ID"),
                                                    joined_pro_cm['person_id'].alias("PATID"),
                                                    joined_pro_cm['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    
                                                    joined_pro_cm['pro_date'].alias("PRO_DATE"),
                                                    substring(joined_pro_cm['pro_time'],1,5).alias("PRO_TIME"),
                                                    joined_pro_cm['pro_type'].alias("PRO_TYPE"),
                                                                                                                          
                                                    
                                                    joined_pro_cm['pro_name'].alias("PRO_NAME"),
                                                    joined_pro_cm['pro_code'].alias("PRO_CODE"),
                                                    joined_pro_cm['pro_response_text'].alias("PRO_RESPONSE_TEXT"),
                                                    joined_pro_cm['pro_response_num'].alias("PRO_RESPONSE_NUM"),
                                                    joined_pro_cm['pro_method'].alias("PRO_METHOD"),
                                                    joined_pro_cm['pro_mode'].alias("PRO_MODE"),
                                                    joined_pro_cm['pro_cat'].alias("PRO_CAT"),
                                                    joined_pro_cm['pro_source'].alias("PRO_SOURCE"),
                                                    joined_pro_cm['pro_version'].alias("PRO_VERSION"),
                                                    joined_pro_cm['pro_seq'].alias("PRO_SEQ"),
                                                    joined_pro_cm['pro_fullname'].alias("PRO_FULLNAME"),
                                                    joined_pro_cm['pro_text'].alias("PRO_TEXT"),
                                                    
                                                    
                                                        )
    

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = pro_cm,
                        output_file_name = "formatted_pro_cm.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    # Persist the full traceback to the OVID log file directly so the concrete
    # error survives regardless of the transient failure-queue/display race in
    # the parent process (see 2026-09-21 silent-failure fix).
    log_error(
        f"pro_cm_formatter.py failed for partner={partner_name} "
        f"folder={input_data_folder}: {e}",
        exc_info=True,
    )
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'pro_cm_formatter.py' ,
                            text    = str(e))





