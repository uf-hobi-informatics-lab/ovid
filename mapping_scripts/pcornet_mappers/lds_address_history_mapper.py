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



def get_state_fips_from_geocoding(STATE_FIPS_ZIP9, STATE_FIPS_ZIP5):
    if STATE_FIPS_ZIP9 is None and STATE_FIPS_ZIP5 is None:
        return None
    elif STATE_FIPS_ZIP9 is not None:
        return STATE_FIPS_ZIP9
    else:
        return STATE_FIPS_ZIP5
get_state_fips_from_geocoding_udf = udf(get_state_fips_from_geocoding, StringType())



def get_countyFips_from_geocoding(COUNTY_FIPS_ZIP9, COUNTY_FIPS_ZIP5):
    if COUNTY_FIPS_ZIP9 is None and COUNTY_FIPS_ZIP5 is None:
        return None
    elif COUNTY_FIPS_ZIP9 is not None:
        return COUNTY_FIPS_ZIP9
    else:
        return COUNTY_FIPS_ZIP5
get_countyFips_from_geocoding_udf = udf(get_countyFips_from_geocoding, StringType())


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
        geocoding_table_path                   = f'/app/common/lists/FULL_GEOCODING_202603_16.csv'

    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        unmapped_lds_address_history    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_lds_address_history.csv", spark)

        geocoding   = spark.read.load(geocoding_table_path,format="csv", sep=",", inferSchema="false", header="true", quote= '"')
        geocoding = geocoding.withColumn(
            "FULL_GEOCODE_COUNTY",
            concat(col("GEOCODE_STATE"), col("GEOCODE_COUNTY"))
            )
        geocoding_zip5 = (
            geocoding
            .withColumn("ZIP9", col("ZIP9").cast("string"))
            .filter(col("ZIP9").isNull() | (trim(col("ZIP9")) == ""))
            .select(
                col("ZIP5").cast("string").alias("NEW_ZIP5"),
                col("GEOCODE_STATE").alias("STATE_FIPS_ZIP5"),
                col("FULL_GEOCODE_COUNTY").alias("COUNTY_FIPS_ZIP5"),
                
            )
        )

        joined_geocoding = (
            unmapped_lds_address_history
            .join(
                geocoding,
                geocoding["ZIP9"] == unmapped_lds_address_history["ADDRESS_ZIP9"],
                how="left"
            )
            .join(
                geocoding_zip5,
                geocoding_zip5["NEW_ZIP5"] == unmapped_lds_address_history["ADDRESS_ZIP5"],
                how="left"
            )
        )




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
        mapping_zip_to_state_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_state_from_zip().items())])
        mapping_zip_to_county_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_county_from_zip().items())])
        mapping_zip_to_city_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_city_from_zip().items())])

    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        lds_address_history = joined_geocoding.select(              
            
                                    cf.encrypt_id_udf(joined_geocoding['ADDRESSID']).alias("ADDRESSID"),
                                    cf.encrypt_id_udf(joined_geocoding['PATID']).alias("PATID"),
                                    coalesce(mapping_address_use_dict[upper(col('ADDRESS_USE'))],col('ADDRESS_USE')).alias("ADDRESS_USE"),
                                    coalesce(mapping_address_type_dict[upper(col('ADDRESS_TYPE'))],col('ADDRESS_TYPE')).alias("ADDRESS_TYPE"),
                                    coalesce(mapping_address_preferred_dict[upper(col('ADDRESS_PREFERRED'))],col('ADDRESS_PREFERRED')).alias("ADDRESS_PREFERRED"),
                                    # joined_geocoding['ADDRESS_CITY'].alias('ADDRESS_CITY'),
                                    coalesce(when(col('ADDRESS_CITY') != lit(''), col('ADDRESS_CITY')), mapping_zip_to_city_dict[upper(col('ADDRESS_ZIP5'))], lit('')).alias("ADDRESS_CITY"),
                                    coalesce(mapping_address_address_state_dict[upper(col('ADDRESS_STATE'))], when(col('ADDRESS_STATE') != lit(''), col('ADDRESS_STATE')), mapping_zip_to_state_dict[upper(col('ADDRESS_ZIP5'))], lit('')).alias("ADDRESS_STATE"),

                                    # coalesce(mapping_address_address_state_dict[upper(col('ADDRESS_STATE'))],col('ADDRESS_STATE')).alias("ADDRESS_STATE"),
                                    joined_geocoding['ADDRESS_ZIP5'].alias('ADDRESS_ZIP5'),
                                    joined_geocoding['ADDRESS_ZIP9'].alias('ADDRESS_ZIP9'),
                                    joined_geocoding['ADDRESS_COUNTY'].alias('ADDRESS_COUNTY'),
                                    joined_geocoding['ADDRESS_PERIOD_START'].alias("ADDRESS_PERIOD_START"),
                                    joined_geocoding['ADDRESS_PERIOD_END'].alias("ADDRESS_PERIOD_END"),  
                                    # coalesce(mapping_zip_to_state_fips_dict[upper(col('ADDRESS_ZIP5'))],lit('')).alias("STATE_FIPS"),
                                    # coalesce(mapping_county_to_countyFips_dict[upper(col('ADDRESS_COUNTY'))],lit('')).alias("COUNTY_FIPS"),
                                    get_state_fips_from_geocoding_udf(joined_geocoding['GEOCODE_STATE'],joined_geocoding['STATE_FIPS_ZIP5']).alias('STATE_FIPS'),
                                    get_countyFips_from_geocoding_udf(joined_geocoding['FULL_GEOCODE_COUNTY'],joined_geocoding['COUNTY_FIPS_ZIP5']).alias('COUNTY_FIPS'),
                                    coalesce(mapping_ruca_zip_dict[upper(col('ADDRESS_ZIP5'))],lit('')).alias("RUCA_ZIP"),
                                    coalesce(mapping_address_current_address_flag_dict[upper(col('CURRENT_ADDRESS_FLAG'))],col('CURRENT_ADDRESS_FLAG')).alias("CURRENT_ADDRESS_FLAG"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    joined_geocoding['ADDRESSID'].alias("JOIN_FIELD"),



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

