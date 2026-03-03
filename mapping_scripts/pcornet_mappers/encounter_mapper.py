###################################################################################################################################
# This script will map a PCORNet Encounter table 
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

spark = cf.get_spark_session("encounter_mapper")

 
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

        partner_dictionaries_path = "partners."+input_partner+".dictionaries"
        partner_dictionaries = importlib.import_module(partner_dictionaries_path)

        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        unmapped_encounter    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_encounter.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        
        mapping_enc_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_enc_type_dict.items())])
        mapping_discharge_disposition_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_discharge_disposition_dict.items())])
        mapping_discharge_status_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_discharge_status_dict.items())])
        mapping_admitting_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_admitting_source_dict.items())])
        mapping_payer_type_secondary_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_payer_type_secondary_dict.items())])
        mapping_payer_type_primary_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_payer_type_primary_dict.items())])

        mapping_facility_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_facility_type_dict.items())])
        mapping_drg_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.encounter_drg_type_dict.items())])



    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        encounter = unmapped_encounter.select(              
            
            

                                    cf.encrypt_id_udf(unmapped_encounter['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    cf.encrypt_id_udf(unmapped_encounter['PATID']).alias("PATID"),
                                    unmapped_encounter['ADMIT_DATE'].alias("ADMIT_DATE"),
                                    unmapped_encounter['ADMIT_TIME'].alias("ADMIT_TIME"),
                                    unmapped_encounter['DISCHARGE_DATE'].alias("DISCHARGE_DATE"),
                                    unmapped_encounter['DISCHARGE_TIME'].alias("DISCHARGE_TIME"),
                                    cf.encrypt_id_udf(unmapped_encounter['PROVIDERID']).alias("PROVIDERID"),
                                    unmapped_encounter['FACILITY_LOCATION'].alias("FACILITY_LOCATION"),
                                    coalesce(mapping_enc_type_dict[upper(col("ENC_TYPE"))],col('ENC_TYPE')).alias("ENC_TYPE"),
                                    unmapped_encounter['FACILITYID'].alias("FACILITYID"),
                                    coalesce(mapping_discharge_disposition_dict[upper(col("DISCHARGE_DISPOSITION"))],col('DISCHARGE_DISPOSITION')).alias("DISCHARGE_DISPOSITION"),
                                    coalesce(mapping_discharge_status_dict[upper(col("DISCHARGE_STATUS"))],col('DISCHARGE_STATUS')).alias("DISCHARGE_STATUS"),
                                    unmapped_encounter['DRG'].alias("DRG"),
                                    coalesce(mapping_drg_type_dict[upper(col("DRG_TYPE"))],col('DRG_TYPE')).alias("DRG_TYPE"),
                                    coalesce(mapping_admitting_source_dict[upper(col("ADMITTING_SOURCE"))],col('ADMITTING_SOURCE')).alias("ADMITTING_SOURCE"),
                                    coalesce(mapping_payer_type_primary_dict[upper(col("PAYER_TYPE_PRIMARY"))],col('PAYER_TYPE_PRIMARY')).alias("PAYER_TYPE_PRIMARY"),
                                    coalesce(mapping_payer_type_secondary_dict[upper(col("PAYER_TYPE_SECONDARY"))],col('PAYER_TYPE_SECONDARY')).alias("PAYER_TYPE_SECONDARY"),
                                    coalesce(mapping_facility_type_dict[upper(col("FACILITY_TYPE"))],col('FACILITY_TYPE')).alias("FACILITY_TYPE"),
                                    unmapped_encounter['RAW_SITEID'].alias("RAW_SITEID"),
                                    unmapped_encounter['RAW_ENC_TYPE'].alias("RAW_ENC_TYPE"),
                                    unmapped_encounter['RAW_DISCHARGE_DISPOSITION'].alias("RAW_DISCHARGE_DISPOSITION"),
                                    unmapped_encounter['RAW_DISCHARGE_STATUS'].alias("RAW_DISCHARGE_STATUS"),
                                    unmapped_encounter['RAW_DRG_TYPE'].alias("RAW_DRG_TYPE"),
                                    unmapped_encounter['RAW_ADMITTING_SOURCE'].alias("RAW_ADMITTING_SOURCE"),
                                    unmapped_encounter['RAW_FACILITY_TYPE'].alias("RAW_FACILITY_TYPE"),
                                    unmapped_encounter['RAW_PAYER_TYPE_PRIMARY'].alias("RAW_PAYER_TYPE_PRIMARY"),
                                    unmapped_encounter['RAW_PAYER_NAME_PRIMARY'].alias("RAW_PAYER_NAME_PRIMARY"),
                                    unmapped_encounter['RAW_PAYER_ID_PRIMARY'].alias("RAW_PAYER_ID_PRIMARY"),
                                    unmapped_encounter['RAW_PAYER_TYPE_SECONDARY'].alias("RAW_PAYER_TYPE_SECONDARY"),
                                    unmapped_encounter['RAW_PAYER_NAME_SECONDARY'].alias("RAW_PAYER_NAME_SECONDARY"),
                                    unmapped_encounter['RAW_PAYER_ID_SECONDARY'].alias("RAW_PAYER_ID_SECONDARY"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_encounter['ENCOUNTERID'].alias("JOIN_FIELD"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################


        encounter_with_additional_fileds = cf.append_additional_fields(
            mapped_df = encounter,
            file_name = "deduplicated_encounter.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "ENCOUNTERID",
            spark = spark)



        cf.write_pyspark_output_file(
                        payspark_df = encounter_with_additional_fileds,
                        output_file_name = "mapped_encounter.csv",
                        output_data_folder_path= mapped_data_folder_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'encounter_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')