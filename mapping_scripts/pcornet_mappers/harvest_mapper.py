###################################################################################################################################
# This script will map a PCORNet harvest table 
###################################################################################################################################

 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import importlib
import sys
# from partners import partners_list
from itertools import chain
import argparse



###################################################################################################################################
# parsing the input arguments to select the partner name
###################################################################################################################################


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--partner")
parser.add_argument("-f", "--data_folder")
args = parser.parse_args()
input_partner = args.partner.lower()
input_data_folder = args.data_folder

cf =CommonFuncitons(input_partner)

# spin the pyspak cluster and
spark = cf.get_spark_session("harvest_mapper")
 
try:

    ###################################################################################################################################
    # Test if the partner name is valid or not
    ###################################################################################################################################


    if  not cf.valid_partner_name(input_partner):

        print("Error: Unrecognized partner "+input_partner+" !!!!!")
        sys.exit()

    else:



    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################


        # deduplicated_data_folder_path = '/app/partners/'+input_partner.lower()+'/data/deduplicator_output/'+ input_data_folder+'/'
        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        unmapped_harvest    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_harvest.csv", spark)







    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        harvest = unmapped_harvest.select(              
            

                                    unmapped_harvest["NETWORKID"].alias("NETWORKID"),
                                    unmapped_harvest["NETWORK_NAME"].alias("NETWORK_NAME"),
                                    unmapped_harvest["DATAMARTID"].alias("DATAMARTID"),
                                    unmapped_harvest["DATAMART_NAME"].alias("DATAMART_NAME"),
                                    unmapped_harvest["DATAMART_PLATFORM"].alias("DATAMART_PLATFORM"),
                                    unmapped_harvest["CDM_VERSION"].alias("CDM_VERSION"),
                                    unmapped_harvest["DATAMART_CLAIMS"].alias("DATAMART_CLAIMS"),
                                    unmapped_harvest["DATAMART_EHR"].alias("DATAMART_EHR"),
                                    unmapped_harvest["BIRTH_DATE_MGMT"].alias("BIRTH_DATE_MGMT"),
                                    unmapped_harvest["ENR_START_DATE_MGMT"].alias("ENR_START_DATE_MGMT"),
                                    unmapped_harvest["ENR_END_DATE_MGMT"].alias("ENR_END_DATE_MGMT"),
                                    unmapped_harvest["ADMIT_DATE_MGMT"].alias("ADMIT_DATE_MGMT"),
                                    unmapped_harvest["DISCHARGE_DATE_MGMT"].alias("DISCHARGE_DATE_MGMT"),
                                    unmapped_harvest["DX_DATE_MGMT"].alias("DX_DATE_MGMT"),
                                    unmapped_harvest["PX_DATE_MGMT"].alias("PX_DATE_MGMT"),
                                    unmapped_harvest["RX_ORDER_DATE_MGMT"].alias("RX_ORDER_DATE_MGMT"),
                                    unmapped_harvest["RX_START_DATE_MGMT"].alias("RX_START_DATE_MGMT"),
                                    unmapped_harvest["RX_END_DATE_MGMT"].alias("RX_END_DATE_MGMT"),
                                    unmapped_harvest["DISPENSE_DATE_MGMT"].alias("DISPENSE_DATE_MGMT"),
                                    unmapped_harvest["LAB_ORDER_DATE_MGMT"].alias("LAB_ORDER_DATE_MGMT"),
                                    unmapped_harvest["SPECIMEN_DATE_MGMT"].alias("SPECIMEN_DATE_MGMT"),
                                    unmapped_harvest["RESULT_DATE_MGMT"].alias("RESULT_DATE_MGMT"),
                                    unmapped_harvest["MEASURE_DATE_MGMT"].alias("MEASURE_DATE_MGMT"),
                                    unmapped_harvest["ONSET_DATE_MGMT"].alias("ONSET_DATE_MGMT"),
                                    unmapped_harvest["REPORT_DATE_MGMT"].alias("REPORT_DATE_MGMT"),
                                    unmapped_harvest["RESOLVE_DATE_MGMT"].alias("RESOLVE_DATE_MGMT"),
                                    unmapped_harvest["PRO_DATE_MGMT"].alias("PRO_DATE_MGMT"),
                                    unmapped_harvest["DEATH_DATE_MGMT"].alias("DEATH_DATE_MGMT"),
                                    unmapped_harvest["MEDADMIN_START_DATE_MGMT"].alias("MEDADMIN_START_DATE_MGMT"),
                                    unmapped_harvest["MEDADMIN_STOP_DATE_MGMT"].alias("MEDADMIN_STOP_DATE_MGMT"),
                                    unmapped_harvest["OBSCLIN_START_DATE_MGMT"].alias("OBSCLIN_START_DATE_MGMT"),
                                    unmapped_harvest["OBSCLIN_STOP_DATE_MGMT"].alias("OBSCLIN_STOP_DATE_MGMT"),
                                    unmapped_harvest["OBSGEN_START_DATE_MGMT"].alias("OBSGEN_START_DATE_MGMT"),
                                    unmapped_harvest["OBSGEN_STOP_DATE_MGMT"].alias("OBSGEN_STOP_DATE_MGMT"),
                                    unmapped_harvest["ADDRESS_PERIOD_START_MGMT"].alias("ADDRESS_PERIOD_START_MGMT"),
                                    unmapped_harvest["ADDRESS_PERIOD_END_MGMT"].alias("ADDRESS_PERIOD_END_MGMT"),
                                    unmapped_harvest["VX_RECORD_DATE_MGMT"].alias("VX_RECORD_DATE_MGMT"),
                                    unmapped_harvest["VX_ADMIN_DATE_MGMT"].alias("VX_ADMIN_DATE_MGMT"),
                                    unmapped_harvest["VX_EXP_DATE_MGMT"].alias("VX_EXP_DATE_MGMT"),
                                    unmapped_harvest["REFRESH_DEMOGRAPHIC_DATE"].alias("REFRESH_DEMOGRAPHIC_DATE"),
                                    unmapped_harvest["REFRESH_ENROLLMENT_DATE"].alias("REFRESH_ENROLLMENT_DATE"),
                                    unmapped_harvest["REFRESH_ENCOUNTER_DATE"].alias("REFRESH_ENCOUNTER_DATE"),
                                    unmapped_harvest["REFRESH_DIAGNOSIS_DATE"].alias("REFRESH_DIAGNOSIS_DATE"),
                                    unmapped_harvest["REFRESH_PROCEDURES_DATE"].alias("REFRESH_PROCEDURES_DATE"),
                                    unmapped_harvest["REFRESH_VITAL_DATE"].alias("REFRESH_VITAL_DATE"),
                                    unmapped_harvest["REFRESH_DISPENSING_DATE"].alias("REFRESH_DISPENSING_DATE"),
                                    unmapped_harvest["REFRESH_LAB_RESULT_CM_DATE"].alias("REFRESH_LAB_RESULT_CM_DATE"),
                                    unmapped_harvest["REFRESH_CONDITION_DATE"].alias("REFRESH_CONDITION_DATE"),
                                    unmapped_harvest["REFRESH_PRO_CM_DATE"].alias("REFRESH_PRO_CM_DATE"),
                                    unmapped_harvest["REFRESH_PRESCRIBING_DATE"].alias("REFRESH_PRESCRIBING_DATE"),
                                    unmapped_harvest["REFRESH_PCORNET_TRIAL_DATE"].alias("REFRESH_PCORNET_TRIAL_DATE"),
                                    unmapped_harvest["REFRESH_DEATH_DATE"].alias("REFRESH_DEATH_DATE"),
                                    unmapped_harvest["REFRESH_DEATH_CAUSE_DATE"].alias("REFRESH_DEATH_CAUSE_DATE"),
                                    unmapped_harvest["REFRESH_MED_ADMIN_DATE"].alias("REFRESH_MED_ADMIN_DATE"),
                                    unmapped_harvest["REFRESH_OBS_CLIN_DATE"].alias("REFRESH_OBS_CLIN_DATE"),
                                    unmapped_harvest["REFRESH_PROVIDER_DATE"].alias("REFRESH_PROVIDER_DATE"),
                                    unmapped_harvest["REFRESH_OBS_GEN_DATE"].alias("REFRESH_OBS_GEN_DATE"),
                                    unmapped_harvest["REFRESH_HASH_TOKEN_DATE"].alias("REFRESH_HASH_TOKEN_DATE"),
                                    unmapped_harvest["REFRESH_LDS_ADDRESS_HX_DATE"].alias("REFRESH_LDS_ADDRESS_HX_DATE"),
                                    unmapped_harvest["REFRESH_IMMUNIZATION_DATE"].alias("REFRESH_IMMUNIZATION_DATE"), 
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################



        cf.write_pyspark_output_file(
                        payspark_df = harvest,
                        output_file_name = "mapped_harvest.csv",
                        output_data_folder_path= mapped_data_folder_path)

        spark.stop()

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'harvest_mapper.py' ,
                            text = str(e)
                            )

 