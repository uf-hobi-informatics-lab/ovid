###################################################################################################################################

# This script will convert an OMOP visit_occurrence table to a PCORnet format as the Encounter table

###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
from itertools import chain
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





def get_time_from_datetime_omop_partner_plus(datetime_str):
        if datetime_str == None or datetime_str =='':
            return None

        return datetime_str[11:16]

get_time_from_datetime_omop_partner_plus_udf = udf(get_time_from_datetime_omop_partner_plus, StringType())


###################################################################################################################################

# This function will take visit_start_date and output the DRG type

###################################################################################################################################

def get_drg_type(visit_start_date):
    if visit_start_date == None or visit_start_date =='':
            return None
    
    visit_start_date = datetime.strptime(visit_start_date, "%Y-%m-%d")

    cutoff_date = datetime.strptime("2007-10-01", "%Y-%m-%d")

    if visit_start_date < cutoff_date:
            return "01"
    else:
            return "02"


get_drg_type_udf = udf(get_drg_type, StringType())

###################################################################################################################################

# This function will take visit_start_date and output the RAW DRG type

###################################################################################################################################

def get_raw_drg_type(visit_start_date):
    if visit_start_date == None or visit_start_date =='':
            return None
    visit_start_date = datetime.strptime(visit_start_date, "%Y-%m-%d")

    cutoff_date = datetime.strptime("2007-10-01", "%Y-%m-%d")

    if visit_start_date < cutoff_date:
            return "CMS-DRG (old system)"
    else:
            return "MS-DRG (current system)"


get_raw_drg_type_udf = udf(get_raw_drg_type, StringType())



try:

        ###################################################################################################################################

        # Loading the visit_occurrence table to be converted to the encounter table
        # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

        ###################################################################################################################################

        input_data_folder_path               = f'/data/{input_data_folder}/'
        formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
        concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'


        visit_occurrence_table_name       = 'visit_occurrence.csv'
        visit_occurrence_sup_table_name   = 'visit_occurrence_sup.csv'

        care_site_table_name              = 'care_site.csv'
        location_table_name               = 'location.csv'
        visit_payer_table_name            = 'visit_payer.csv'




        visit_occurrence = spark.read.load(input_data_folder_path+visit_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
        visit_occurrence_sup = spark.read.load(input_data_folder_path+visit_occurrence_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("visit_occurrence_id",      "visit_occurrence_id_sup")\

        concept       = cf.spark_read(concept_table_path,spark)
        visit_concept = concept.filter(concept.domain_id == 'Visit').withColumnRenamed("concept_name", "visit_concept_name")
        admitting_concept = concept.filter(concept.domain_id == 'Place of service').withColumnRenamed("concept_name", "admitting_concept_name")
        discharge_to_concept = concept.filter(concept.domain_id == 'Place of service').withColumnRenamed("concept_name", "discharge_to_concept_name")

        care_site        = spark.read.load(input_data_folder_path+care_site_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("care_site_id",      "source_care_site_id")
        location         = spark.read.load(input_data_folder_path+location_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("location_id",        "source_location_id")
        visit_payer      = spark.read.load(input_data_folder_path+visit_payer_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


        visit_payer_primary_data = visit_payer.filter(col("payer_type").isin(['Primary','primary','Primary Payer','Primary payer']))\
                                                .withColumnRenamed("payer_type",     "primary_payer_type")\
                                                .withColumnRenamed("plan_class",     "primary_plan_class")\
                                                .withColumnRenamed("plan_name",      "primary_plan_name")\
                                                .withColumnRenamed("visit_occurrence_id",      "visit_occurrence_id_payer_primary")\
                                                .withColumnRenamed("visit_payer_id", "primary_visit_payer_id")


        visit_payer_secondary_data = visit_payer.filter(col("payer_type").isin(['Secondary','secondary','Secondary Payer','Secondary payer']))\
                                                .withColumnRenamed("payer_type",     "secondary_payer_type")\
                                                .withColumnRenamed("plan_class",     "secondary_plan_class")\
                                                .withColumnRenamed("plan_name",      "secondary_plan_name")\
                                                .withColumnRenamed("visit_occurrence_id",      "visit_occurrence_id_payer_secondary")\
                                                .withColumnRenamed("visit_payer_id", "secondary_visit_payer_id")



        joined_visit_occurrence = visit_occurrence.join(visit_concept, visit_concept['concept_id']==visit_occurrence['visit_concept_id'], how='left').drop("concept_id")\
                                                .join(visit_occurrence_sup, visit_occurrence_sup['visit_occurrence_id_sup']== visit_occurrence['visit_occurrence_id'], how = 'left')\
                                                .join(visit_payer_primary_data, visit_payer_primary_data['visit_occurrence_id_payer_primary']== visit_occurrence['visit_occurrence_id'], how = 'left')\
                                                .join(visit_payer_secondary_data, visit_payer_secondary_data['visit_occurrence_id_payer_secondary']== visit_occurrence['visit_occurrence_id'], how = 'left')\
                                                .join(care_site, care_site['source_care_site_id']== visit_occurrence['care_site_id'], how = 'left')\
                                                .join(location, location['source_location_id']== care_site['location_id'], how = 'left')\
                                                .join(admitting_concept, admitting_concept['concept_id']== visit_occurrence['admitting_source_concept_id'], how = 'left')\
                                                .join(discharge_to_concept, discharge_to_concept['concept_id']== visit_occurrence['discharge_to_concept_id'], how = 'left')\
                                                



        ###################################################################################################################################

        #Converting the fileds to PCORNet Encounter Format

        ###################################################################################################################################



        encounter = joined_visit_occurrence.select(     joined_visit_occurrence['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                        joined_visit_occurrence["person_id"].alias("PATID"),
                                                        cf.format_date_udf(joined_visit_occurrence["visit_start_date"]).alias("ADMIT_DATE"),
                                                        cf.format_time_udf(joined_visit_occurrence['visit_start_datetime']).alias("ADMIT_TIME"),
                                                        cf.format_date_udf(joined_visit_occurrence["visit_end_date"]).alias("DISCHARGE_DATE"),
                                                        get_time_from_datetime_omop_partner_plus_udf(joined_visit_occurrence["visit_end_datetime"]).alias("DISCHARGE_TIME"),
                                                        joined_visit_occurrence["provider_id"].alias("PROVIDERID"),
                                                        joined_visit_occurrence["zip"].alias("FACILITY_LOCATION"),
                                                        joined_visit_occurrence['visit_concept_name'].alias("ENC_TYPE"),
                                                        joined_visit_occurrence['care_site_id'].alias("FACILITYID"),
                                                        joined_visit_occurrence["discharge_status"].alias('DISCHARGE_DISPOSITION'),
                                                        joined_visit_occurrence["discharge_to_source_value"].alias('DISCHARGE_STATUS'),
                                                        joined_visit_occurrence["drg"].alias("DRG"),
                                                        get_drg_type_udf(joined_visit_occurrence["visit_start_date"]).alias("DRG_TYPE"),
                                                        joined_visit_occurrence["admitting_concept_name"].alias('ADMITTING_SOURCE'),
                                                        joined_visit_occurrence["primary_payer_type"].alias("PAYER_TYPE_PRIMARY"),
                                                        joined_visit_occurrence["secondary_payer_type"].alias("PAYER_TYPE_SECONDARY"),
                                                        joined_visit_occurrence["place_of_service_source_value"].alias("FACILITY_TYPE"),
                                                        joined_visit_occurrence["care_site_id"].alias("RAW_SITEID"),
                                                        joined_visit_occurrence["visit_concept_name"].alias("RAW_ENC_TYPE"),
                                                        joined_visit_occurrence["discharge_status"].alias("RAW_DISCHARGE_DISPOSITION"),
                                                        joined_visit_occurrence["discharge_to_concept_name"].alias("RAW_DISCHARGE_STATUS"),
                                                        get_raw_drg_type_udf(joined_visit_occurrence["visit_start_date"]).alias("RAW_DRG_TYPE"),
                                                        joined_visit_occurrence["admitting_source_value"].alias("RAW_ADMITTING_SOURCE"),
                                                        joined_visit_occurrence["care_site_id"].alias("RAW_FACILITY_TYPE"),
                                                        joined_visit_occurrence["primary_plan_class"].alias("RAW_PAYER_TYPE_PRIMARY"),
                                                        joined_visit_occurrence["primary_plan_name"].alias("RAW_PAYER_NAME_PRIMARY"),
                                                        joined_visit_occurrence["primary_visit_payer_id"].alias("RAW_PAYER_ID_PRIMARY"),
                                                        joined_visit_occurrence["secondary_plan_class"].alias("RAW_PAYER_TYPE_SECONDARY"),
                                                        joined_visit_occurrence["secondary_plan_name"].alias("RAW_PAYER_NAME_SECONDARY"),
                                                        joined_visit_occurrence["secondary_visit_payer_id"].alias("RAW_PAYER_ID_SECONDARY"),



                                                        )

        ###################################################################################################################################

        # Create the output file 

        ###################################################################################################################################


        cf.write_pyspark_output_file(
                        payspark_df = encounter,
                        output_file_name = "formatted_encounter.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

        spark.stop()




except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name,
                            job     = 'encounter_formatter.py' ,
                            text    = str(e))




