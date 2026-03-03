###################################################################################################################################
# This script will map a PCORNet immunization table 
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

spark = cf.get_spark_session("immunization_mapper")

try:

    ###################################################################################################################################
    # Test if the partner name is valid or not
    ###################################################################################################################################


    if  not cf.valid_partner_name(input_partner):

        print("Error: Unrecognized partner "+input_partner+" !!!!!")
        sys.exit()

    else:



    ###################################################################################################################################
    # Load the config file for the selected partner
    ###################################################################################################################################

        partner_dictionaries_path = "partners."+input_partner+".dictionaries"
        partner_dictionaries = importlib.import_module(partner_dictionaries_path)

        # deduplicated_data_folder_path = '/app/partners/'+input_partner.lower()+'/data/deduplicator_output/'+ input_data_folder+'/'
        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'



    ###################################################################################################################################
    # spin the pyspak cluster and
    # Loading the unmapped immunization table
    ###################################################################################################################################


        unmapped_immunization    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_immunization.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
    
        mapping_vx_code_type = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_code_type_dict.items())])
        mapping_vx_status = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_status_dict.items())])
        mapping_vx_status_reason = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_status_reason_dict.items())])
        mapping_vx_source = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_source_dict.items())])
        mapping_vx_dose_unit = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_dose_unit_dict.items())])
        mapping_vx_route = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_route_dict.items())])
        mapping_vx_body_site = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_body_site_dict.items())])
        mapping_vx_manufacturer = create_map([lit(x) for x in chain(*partner_dictionaries.immunization_vx_manufacturer_dict.items())])

    
    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped immunization table
    ###################################################################################################################################


        immunization = unmapped_immunization.select(              
            
                                    cf.encrypt_id_udf(unmapped_immunization['IMMUNIZATIONID']).alias("IMMUNIZATIONID"),
                                    cf.encrypt_id_udf(unmapped_immunization['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_immunization['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    cf.encrypt_id_udf(unmapped_immunization['PROCEDURESID']).alias("PROCEDURESID"),
                                    cf.encrypt_id_udf(unmapped_immunization['VX_PROVIDERID']).alias("VX_PROVIDERID"),
                                    unmapped_immunization['VX_RECORD_DATE'].alias("VX_RECORD_DATE"),
                                    unmapped_immunization['VX_ADMIN_DATE'].alias("VX_ADMIN_DATE"),
                                    coalesce(mapping_vx_code_type[upper(col("VX_CODE_TYPE"))],col('VX_CODE_TYPE')).alias("VX_CODE_TYPE"),
                                    unmapped_immunization['VX_CODE'].alias("VX_CODE"),
                                    coalesce(mapping_vx_status[upper(col("VX_STATUS"))],col('VX_STATUS')).alias("VX_STATUS"),
                                    coalesce(mapping_vx_status_reason[upper(col("VX_STATUS_REASON"))],col('VX_STATUS_REASON')).alias("VX_STATUS_REASON"),
                                    coalesce(mapping_vx_source[upper(col("VX_SOURCE"))],col('VX_SOURCE')).alias("VX_SOURCE"),
                                    unmapped_immunization['VX_DOSE'].alias("VX_DOSE"),
                                    coalesce(mapping_vx_dose_unit[col("VX_DOSE_UNIT")],col('VX_DOSE_UNIT')).alias("VX_DOSE_UNIT"),
                                    coalesce(mapping_vx_route[upper(col("VX_ROUTE"))],col('VX_ROUTE')).alias("VX_ROUTE"),
                                    coalesce(mapping_vx_body_site[upper(col("VX_BODY_SITE"))],col('VX_BODY_SITE')).alias("VX_BODY_SITE"),
                                    coalesce(mapping_vx_manufacturer[upper(col("VX_MANUFACTURER"))],col('VX_MANUFACTURER')).alias("VX_MANUFACTURER"),
                                    unmapped_immunization['VX_LOT_NUM'].alias("VX_LOT_NUM"),
                                    unmapped_immunization['VX_EXP_DATE'].alias("VX_EXP_DATE"),
                                    unmapped_immunization['RAW_VX_NAME'].alias("RAW_VX_NAME"),
                                    unmapped_immunization['RAW_VX_CODE'].alias("RAW_VX_CODE"),
                                    unmapped_immunization['RAW_VX_CODE_TYPE'].alias("RAW_VX_CODE_TYPE"),
                                    unmapped_immunization['RAW_VX_DOSE'].alias("RAW_VX_DOSE"),
                                    unmapped_immunization['RAW_VX_DOSE_UNIT'].alias("RAW_VX_DOSE_UNIT"),
                                    unmapped_immunization['RAW_VX_ROUTE'].alias("RAW_VX_ROUTE"),
                                    unmapped_immunization['RAW_VX_BODY_SITE'].alias("RAW_VX_BODY_SITE"),
                                    unmapped_immunization['RAW_VX_STATUS'].alias("RAW_VX_STATUS"),
                                    unmapped_immunization['RAW_VX_STATUS_REASON'].alias("RAW_VX_STATUS_REASON"),
                                    unmapped_immunization['RAW_VX_MANUFACTURER'].alias("RAW_VX_MANUFACTURER"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_immunization['IMMUNIZATIONID'].alias("JOIN_FIELD"),
                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        immunization_with_additional_fileds = cf.append_additional_fields(
            mapped_df = immunization,
            file_name = "deduplicated_immunization.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "IMMUNIZATIONID",
            spark = spark)

        cf.write_pyspark_output_file(
                        payspark_df = immunization_with_additional_fileds,
                        output_file_name = "mapped_immunization.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()

except Exception as e:

    spark.stop()

    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'immunization_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')