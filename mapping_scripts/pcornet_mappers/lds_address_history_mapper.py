###################################################################################################################################
# This script will map a PCORNet lds_address_history table 
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
spark = cf.get_spark_session("lds_address_history_mapper") 









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

        # deduplicated_data_folder_path = '/app/partners/'+input_partner.lower()+'/data/deduplicator_output/'+ input_data_folder+'/'
        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'

    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        unmapped_lds_address_history    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_lds_address_history.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_address_use_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lds_address_history_address_use_dict.items())])
        mapping_address_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lds_address_history_address_type_dict.items())])
        mapping_address_preferred_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lds_address_history_address_preferred_dict.items())])
        mapping_address_current_address_flag_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lds_address_history_current_address_flag_dict.items())])
        mapping_address_address_state_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lds_address_history_address_state_dict.items())])
        mapping_ruca_zip_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_ruca_zip_mapping().items())])
        mapping_zip_to_state_fips_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_state_fips_from_zip().items())])
        mapping_county_to_countyFips_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_countyFips_from_county().items())])

    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        lds_address_history = unmapped_lds_address_history.select(              
            
                                    cf.encrypt_id_udf(unmapped_lds_address_history['ADDRESSID']).alias("ADDRESSID"),
                                    cf.encrypt_id_udf(unmapped_lds_address_history['PATID']).alias("PATID"),
                                    coalesce(mapping_address_use_dict[upper(col('ADDRESS_USE'))],col('ADDRESS_USE')).alias("ADDRESS_USE"),
                                    coalesce(mapping_address_type_dict[upper(col('ADDRESS_TYPE'))],col('ADDRESS_TYPE')).alias("ADDRESS_TYPE"),
                                    coalesce(mapping_address_preferred_dict[upper(col('ADDRESS_PREFERRED'))],col('ADDRESS_PREFERRED')).alias("ADDRESS_PREFERRED"),
                                    unmapped_lds_address_history['ADDRESS_CITY'].alias('ADDRESS_CITY'),
                                    coalesce(mapping_address_address_state_dict[upper(col('ADDRESS_STATE'))],col('ADDRESS_STATE')).alias("ADDRESS_STATE"),
                                    unmapped_lds_address_history['ADDRESS_ZIP5'].alias('ADDRESS_ZIP5'),
                                    unmapped_lds_address_history['ADDRESS_ZIP9'].alias('ADDRESS_ZIP9'),
                                    unmapped_lds_address_history['ADDRESS_COUNTY'].alias('ADDRESS_COUNTY'),
                                    unmapped_lds_address_history['ADDRESS_PERIOD_START'].alias("ADDRESS_PERIOD_START"),
                                    unmapped_lds_address_history['ADDRESS_PERIOD_END'].alias("ADDRESS_PERIOD_END"),  
                                    # coalesce(mapping_zip_to_state_fips_dict[upper(col('ADDRESS_ZIP5'))],lit('')).alias("STATE_FIPS"),
                                    # coalesce(mapping_county_to_countyFips_dict[upper(col('ADDRESS_COUNTY'))],lit('')).alias("COUNTY_FIPS"),
                                    unmapped_lds_address_history['STATE_FIPS'].alias('STATE_FIPS'),
                                    unmapped_lds_address_history['COUNTY_FIPS'].alias('COUNTY_FIPS'),
                                    coalesce(mapping_ruca_zip_dict[upper(col('ADDRESS_ZIP5'))],lit('')).alias("RUCA_ZIP"),
                                    coalesce(mapping_address_current_address_flag_dict[upper(col('CURRENT_ADDRESS_FLAG'))],col('CURRENT_ADDRESS_FLAG')).alias("CURRENT_ADDRESS_FLAG"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_lds_address_history['ADDRESSID'].alias("JOIN_FIELD"),



                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        lds_address_history_with_additional_fileds = cf.append_additional_fields(
            mapped_df = lds_address_history,
            file_name = "deduplicated_lds_address_history.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "ADDRESSID",
            spark = spark)

        cf.write_pyspark_output_file(
                        payspark_df = lds_address_history_with_additional_fileds,
                        output_file_name = "mapped_lds_address_history.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'lds_address_history_mapper.py' ,
                            text = str(e)
                            )

    cf.print_with_style(str(e), 'danger red')