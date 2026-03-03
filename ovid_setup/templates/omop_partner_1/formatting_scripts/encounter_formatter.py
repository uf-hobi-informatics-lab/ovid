###################################################################################################################################

# This script will convert an OMOP visit_occurrence table to a PCORnet format as the Encounter table

###################################################################################################################################

import pyspark
from pyspark import SparkConf, SparkContext
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

try:
    ###################################################################################################################################

    # Loading the visit_occurrence table to be converted to the encounter table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    visit_occurrence_table_name       = 'Visit_Occurrence.txt'
    care_site_table_name              = 'CareSites.txt'
    location_table_name               = 'Locations.txt'
    visit_payer_table_name            = 'Visit_Payer.txt'




    visit_occurrence = spark.read.load(input_data_folder_path+visit_occurrence_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    care_site        = spark.read.load(input_data_folder_path+care_site_table_name,format="csv", sep=",", inferSchema="false", header="true", quote= '"')
    #location         = spark.read.load(input_data_folder_path+location_table_name,format="csv", sep="\t", inferSchema="true", header="true", quote= '"')
    location = spark.read.load(input_data_folder_path + location_table_name, format="csv", sep="\t", inferSchema="false", header="true", quote='"').select("zip_5")
    visit_payer      = spark.read.load(input_data_folder_path+visit_payer_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


    care_site_data = care_site.collect()
    # location_data = location.collect()
    location_data = location.take(location.count())



    ###################################################################################################################################

    #Create the mapping dictionary for zip_5 using the location and the care_site tables

    ###################################################################################################################################


    facility_zip_5_dic = {}

    for row1, row2 in zip(care_site_data, location_data):
        key = row1.care_site_id
        value = row2.zip_5
        facility_zip_5_dic[key] = value


    mapping_facility_zip_5_dic = create_map([lit(x) for x in chain(*facility_zip_5_dic.items())])

    ###################################################################################################################################

    #Create the mapping dictionary for palce_of_service using the care_site table

    ###################################################################################################################################


    place_of_service_dict = {}

    for row in care_site_data:
        key = row.care_site_id
        value = row.place_of_service
        place_of_service_dict[key] = value

    mapping_place_of_service_dict = create_map([lit(x) for x in chain(*place_of_service_dict.items())])



    ###################################################################################################################################

    #Create the mapping dictionaries for payers data using the visit_payer table

    ###################################################################################################################################

    visit_payer_primary_data = visit_payer.filter(col("payer_type").isin(['Primary','primary','Primary Payer']))
    visit_payer_primary_data = visit_payer_primary_data.collect()

    PCORI_enc_payer_plan_class_primary_dict = {row["visit_occurrence_id"]: row["plan_class"] for row in visit_payer_primary_data}
    PCORI_enc_payer_plan_class_primary_dict_udf = udf(lambda visit_occurrence_id: PCORI_enc_payer_plan_class_primary_dict.get(visit_occurrence_id, None), StringType())

    PCORI_enc_payer_plan_name_primary_dict = {row["visit_occurrence_id"]: row["plan_name"] for row in visit_payer_primary_data}
    PCORI_enc_payer_plan_name_primary_dict_udf = udf(lambda visit_occurrence_id: PCORI_enc_payer_plan_name_primary_dict.get(visit_occurrence_id, None), StringType())

    PCORI_enc_payer_id_primary_dict = {row["visit_occurrence_id"]: row["visit_payer_id"] for row in visit_payer_primary_data}
    PCORI_enc_payer_id_primary_dict_udf = udf(lambda visit_occurrence_id: PCORI_enc_payer_id_primary_dict.get(visit_occurrence_id, None), StringType())


    visit_payer_secondary_data = visit_payer.filter(col("payer_type").isin(['Secondary','secondary','Secondary Payer']))
    visit_payer_secondary_data = visit_payer_secondary_data.collect()

    PCORI_enc_payer_plan_class_secondary_dict = {row["visit_occurrence_id"]: row["plan_class"] for row in visit_payer_secondary_data}
    PCORI_enc_payer_plan_class_secondary_dict_udf = udf(lambda visit_occurrence_id: PCORI_enc_payer_plan_class_secondary_dict.get(visit_occurrence_id, None), StringType())

    PCORI_enc_payer_plan_name_secondary_dict = {row["visit_occurrence_id"]: row["plan_name"] for row in visit_payer_secondary_data}
    PCORI_enc_payer_plan_name_secondary_dict_udf = udf(lambda visit_occurrence_id: PCORI_enc_payer_plan_name_secondary_dict.get(visit_occurrence_id, None), StringType())

    PCORI_enc_payer_id_secondary_dict = {row["visit_occurrence_id"]: row["visit_payer_id"] for row in visit_payer_secondary_data}
    PCORI_enc_payer_id_secondary_dict_udf = udf(lambda visit_occurrence_id: PCORI_enc_payer_id_secondary_dict.get(visit_occurrence_id, None), StringType())

    ###################################################################################################################################

    # This function will take visit_start_date and output the DRG type

    ###################################################################################################################################

    def get_drg_type(visit_start_date):
        if visit_start_date is None or visit_start_date == '':
            return None

        try:
            visit_start_date = datetime.strptime(visit_start_date, "%Y-%m-%d")  # Assuming the date format is YYYY-MM-DD
        except ValueError:
            return None  # Handle incorrect date format

        cutoff_date = datetime.strptime("2007-10-01", "%Y-%m-%d").date()

        if visit_start_date.date() < cutoff_date:
            return "01"
        else:
            return "02"

    get_drg_type_udf = udf(get_drg_type, StringType())

    ###################################################################################################################################

    # This function will take visit_start_date and output the RAW DRG type

    ###################################################################################################################################

    def get_raw_drg_type(visit_start_date):
        if visit_start_date is None or visit_start_date == '':
            return None

        visit_start_date = datetime.strptime(visit_start_date, "%Y-%m-%d")  # Convert to datetime object

        cutoff_date = datetime.strptime("2007-10-01", "%Y-%m-%d")

        if visit_start_date < cutoff_date:
            return "CMS-DRG (old system)"
        else:
            return "MS-DRG (current system)"


    get_raw_drg_type_udf = udf(get_raw_drg_type, StringType())  

    ###################################################################################################################################

    def get_time_from_datetime(val_time):
        # Parse the input string into a datetime object
        datetime_object = datetime.strptime(val_time, "%Y-%m-%d %H:%M:%S")

        # Format the datetime object as a string in "yyyy-mm-dd" format
        formatted_time = datetime_object.strftime("%H:%M")

        return formatted_time
    
    convert_and_format_time_udf = udf(get_time_from_datetime, StringType())   

    ###################################################################################################################################

    #Converting the fileds to PCORNet Encounter Format

    ###################################################################################################################################


    encounter = visit_occurrence.select(            visit_occurrence['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    visit_occurrence["person_id"].alias("PATID"),
                                                    visit_occurrence["visit_start_date"].alias("ADMIT_DATE"),
                                                    convert_and_format_time_udf(visit_occurrence['visit_start_datetime']).alias("ADMIT_TIME"),
                                                    visit_occurrence["visit_end_date"].alias("DISCHARGE_DATE"),
                                                    convert_and_format_time_udf(visit_occurrence["visit_end_datetime"]).alias("DISCHARGE_TIME"),
                                                    visit_occurrence["provider_id"].alias("PROVIDERID"),
                                                    mapping_facility_zip_5_dic[col("care_site_id")].alias("FACILITY_LOCATION"),
                                                    visit_occurrence['visit_type'].alias("ENC_TYPE"),
                                                    visit_occurrence['care_site_id'].alias("FACILITYID"),
                                                    visit_occurrence["discharge_status"].alias('DISCHARGE_DISPOSITION'),
                                                    visit_occurrence["discharge_to"].alias('DISCHARGE_STATUS'),
                                                    visit_occurrence["drg"].alias("DRG"),
                                                    get_drg_type_udf(visit_occurrence["visit_start_date"]).alias("DRG_TYPE"),
                                                    visit_occurrence["admitted"].alias('ADMITTING_SOURCE'),
                                                    PCORI_enc_payer_plan_class_primary_dict_udf(col('visit_occurrence_id')).alias("PAYER_TYPE_PRIMARY"),
                                                    PCORI_enc_payer_plan_class_secondary_dict_udf(col('visit_occurrence_id')).alias("PAYER_TYPE_SECONDARY"),
                                                    mapping_place_of_service_dict[col("care_site_id")].alias("FACILITY_TYPE"),
                                                    visit_occurrence["care_site_id"].alias("RAW_SITEID"),
                                                    visit_occurrence["visit_type"].alias("RAW_ENC_TYPE"),
                                                    visit_occurrence["discharge_status"].alias("RAW_DISCHARGE_DISPOSITION"),
                                                    visit_occurrence["discharge_to"].alias("RAW_DISCHARGE_STATUS"),
                                                    get_raw_drg_type_udf(visit_occurrence["visit_start_date"]).alias("RAW_DRG_TYPE"),
                                                    visit_occurrence["admitted_source_value"].alias("RAW_ADMITTING_SOURCE"),
                                                    visit_occurrence["care_site_id"].alias("RAW_FACILITY_TYPE"),
                                                    PCORI_enc_payer_plan_class_primary_dict_udf(col('visit_occurrence_id')).alias("RAW_PAYER_TYPE_PRIMARY"),
                                                    PCORI_enc_payer_plan_name_primary_dict_udf(col('visit_occurrence_id')).alias("RAW_PAYER_NAME_PRIMARY"),
                                                    PCORI_enc_payer_id_primary_dict_udf(col('visit_occurrence_id')).alias("RAW_PAYER_ID_PRIMARY"),
                                                    PCORI_enc_payer_plan_class_secondary_dict_udf(col('visit_occurrence_id')).alias("RAW_PAYER_TYPE_SECONDARY"),
                                                    PCORI_enc_payer_plan_name_secondary_dict_udf(col('visit_occurrence_id')).alias("RAW_PAYER_NAME_SECONDARY"),
                                                    PCORI_enc_payer_id_secondary_dict_udf(col('visit_occurrence_id')).alias("RAW_PAYER_ID_SECONDARY"),



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
                            partner = partner_name.lower(),
                            job     = 'encounter_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')






