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


def pcornet_partner_1_discharge_disposition( key):
    """
    Discharge disposition lookup table contains 65 choices
    after discussing with FH all but 638666 and 638667 are
    considered discharged alive. We are doing a dictionary lookup
    for the 2 values listed above and if that returns nothing
    we return alive as the status.
    """
    dict_search = {
        "30000000|638666": "E",
        "30000000|638667": "E",
    }

    result = "A"

    if key is not None:
        # if the key is not mapped we assume discharge status is alive
        result = dict_search.get(str(key), result)

    return result

pcornet_partner_1_discharge_disposition_udf = udf(pcornet_partner_1_discharge_disposition, StringType())


try:


    ###################################################################################################################################

    # Loading the visit_occurrence table to be converted to the encounter table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    encounter_table_name       = '*Encounter*'

    encounter_in = spark.read.load(input_data_folder_path+encounter_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')

    ###################################################################################################################################

    #Converting the fileds to PCORNet Encounter Format

    ###################################################################################################################################
    try:
        encounter_in = spark.read.load(input_data_folder_path+encounter_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        encounter = encounter_in.select(

                                encounter_in['encounterid'].alias('ENCOUNTERID'),
                                encounter_in['patid'].alias('PATID'),
                                cf.format_date_udf(encounter_in['admit_date']).alias('ADMIT_DATE'),
                                cf.get_time_from_datetime_udf(encounter_in['admit_time']).alias('ADMIT_TIME'),
                                cf.format_date_udf(encounter_in['discharge_date']).alias('DISCHARGE_DATE'),
                                cf.get_time_from_datetime_udf(encounter_in['discharge_time']).alias('DISCHARGE_TIME'),
                                encounter_in['providerid'].alias('PROVIDERID'),
                                encounter_in['facility_location'].alias('FACILITY_LOCATION'),
                                encounter_in['raw_enc_type'].alias('ENC_TYPE'),
                                encounter_in['facilityid'].alias('FACILITYID'),
                                pcornet_partner_1_discharge_disposition_udf(encounter_in['raw_discharge_disposition']).alias('DISCHARGE_DISPOSITION'),
                                encounter_in['raw_discharge_status'].alias('DISCHARGE_STATUS'),
                                encounter_in['drg'].alias('DRG'),
                                encounter_in['raw_drg_type'].alias('DRG_TYPE'),
                                encounter_in['admitting_source'].alias('ADMITTING_SOURCE'),
                                encounter_in['payor_type_primary'].alias('PAYER_TYPE_PRIMARY'),
                                encounter_in['payor_type_secondary'].alias('PAYER_TYPE_SECONDARY'),
                                lit('HOSPITAL_COMMUNITY').alias('FACILITY_TYPE'),
                                encounter_in['raw_siteid'].alias('RAW_SITEID'),
                                encounter_in['raw_enc_type'].alias('RAW_ENC_TYPE'),
                                encounter_in['raw_discharge_disposition'].alias('RAW_DISCHARGE_DISPOSITION'),
                                encounter_in['raw_discharge_status'].alias('RAW_DISCHARGE_STATUS'),
                                encounter_in['raw_drg_type'].alias('RAW_DRG_TYPE'),
                                encounter_in['raw_admitting_source'].alias('RAW_ADMITTING_SOURCE'),
                                encounter_in['raw_facility_type'].alias('RAW_FACILITY_TYPE'),
                                encounter_in['raw_payer_type_primary'].alias('RAW_PAYER_TYPE_PRIMARY'),
                                encounter_in['raw_payer_name_primary'].alias('RAW_PAYER_NAME_PRIMARY'),
                                encounter_in['raw_payer_id_primary'].alias('RAW_PAYER_ID_PRIMARY'),
                                encounter_in['raw_payer_type_secondary'].alias('RAW_PAYER_TYPE_SECONDARY'),
                                encounter_in['raw_payor_name_secondary'].alias('RAW_PAYER_NAME_SECONDARY'),
                                encounter_in['raw_payor_id_secondary'].alias('RAW_PAYER_ID_SECONDARY'),
                                encounter_in['covid19_positive_flag'].alias('COVID19_POSITIVE_FLAG'),
                                encounter_in['vent_flag'].alias('VENT_FLAG'),
                                cf.format_date_udf(encounter_in['covid19_positive_report_date']).alias('COVID19_POSITIVE_REPORT_DATE'),
                                encounter_in['icu_flag'].alias('ICU_FLAG'),

        )
    except:

        encounter_in = spark.read.load(input_data_folder_path+encounter_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        encounter = encounter_in.select(

                                encounter_in['ENCOUNTERID'].alias('ENCOUNTERID'),
                                encounter_in['PATID'].alias('PATID'),
                                cf.format_date_udf(encounter_in['ADMIT_DATE']).alias('ADMIT_DATE'),
                                cf.get_time_from_datetime_udf(encounter_in['ADMIT_TIME']).alias('ADMIT_TIME'),
                                cf.format_date_udf(encounter_in['DISCHARGE_DATE']).alias('DISCHARGE_DATE'),
                                cf.get_time_from_datetime_udf(encounter_in['DISCHARGE_TIME']).alias('DISCHARGE_TIME'),
                                encounter_in['PROVIDERID'].alias('PROVIDERID'),
                                encounter_in['FACILITY_LOCATION'].alias('FACILITY_LOCATION'),
                                encounter_in['RAW_ENC_TYPE'].alias('ENC_TYPE'),
                                encounter_in['FACILITYID'].alias('FACILITYID'),
                                pcornet_partner_1_discharge_disposition_udf(encounter_in['DISCHARGE_DISPOSITION']).alias('DISCHARGE_DISPOSITION'),
                                encounter_in['RAW_DISCHARGE_STATUS'].alias('DISCHARGE_STATUS'),
                                encounter_in['DRG'].alias('DRG'),
                                encounter_in['RAW_DRG_TYPE'].alias('DRG_TYPE'),
                                encounter_in['ADMITTING_SOURCE'].alias('ADMITTING_SOURCE'),
                                encounter_in['PAYER_TYPE_PRIMARY'].alias('PAYER_TYPE_PRIMARY'),
                                encounter_in['PAYER_TYPE_SECONDARY'].alias('PAYER_TYPE_SECONDARY'),
                                lit('HOSPITAL_COMMUNITY').alias('FACILITY_TYPE'),
                                encounter_in['RAW_SITEID'].alias('RAW_SITEID'),
                                encounter_in['RAW_ENC_TYPE'].alias('RAW_ENC_TYPE'),
                                encounter_in['RAW_DISCHARGE_DISPOSITION'].alias('RAW_DISCHARGE_DISPOSITION'),
                                encounter_in['RAW_DISCHARGE_STATUS'].alias('RAW_DISCHARGE_STATUS'),
                                encounter_in['RAW_DRG_TYPE'].alias('RAW_DRG_TYPE'),
                                encounter_in['RAW_ADMITTING_SOURCE'].alias('RAW_ADMITTING_SOURCE'),
                                encounter_in['RAW_FACILITY_TYPE'].alias('RAW_FACILITY_TYPE'),
                                encounter_in['RAW_PAYER_TYPE_PRIMARY'].alias('RAW_PAYER_TYPE_PRIMARY'),
                                encounter_in['RAW_PAYER_NAME_PRIMARY'].alias('RAW_PAYER_NAME_PRIMARY'),
                                encounter_in['RAW_PAYER_ID_PRIMARY'].alias('RAW_PAYER_ID_PRIMARY'),
                                encounter_in['RAW_PAYER_TYPE_SECONDARY'].alias('RAW_PAYER_TYPE_SECONDARY'),
                                encounter_in['RAW_PAYER_NAME_SECONDARY'].alias('RAW_PAYER_NAME_SECONDARY'),
                                encounter_in['RAW_PAYER_ID_SECONDARY'].alias('RAW_PAYER_ID_SECONDARY'),
                                encounter_in['COVID19_POSITIVE_FLAG'].alias('COVID19_POSITIVE_FLAG'),
                                encounter_in['VENT_FLAG'].alias('VENT_FLAG'),
                                cf.format_date_udf(encounter_in['COVID19_POSITIVE_REPORT_DATE']).alias('COVID19_POSITIVE_REPORT_DATE'),
                                encounter_in['ICU_FLAG'].alias('ICU_FLAG'),
        )

    ###################################################################################################################################

    # Create the output file 

    ###################################################################################################################################

    cf =CommonFuncitons('UFH')
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





