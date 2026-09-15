###################################################################################################################################
# This script will map a PCORNet private_address_geocode table 
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
spark = cf.get_spark_session("private_address_geocode_mapper") 









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


        unmapped_private_address_geocode    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_private_address_geocode.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################


    ###################################################################################################################################
    # Apply the mappings: encrypt ADDRESSID to match private_address_history, add operational fields
    ###################################################################################################################################

        private_address_geocode = unmapped_private_address_geocode.select(
                                    cf.encrypt_id_udf(unmapped_private_address_geocode['GEOCODEID']).alias("GEOCODEID"),
                                    cf.encrypt_id_udf(unmapped_private_address_geocode['ADDRESSID']).alias("ADDRESSID"),
                                    unmapped_private_address_geocode['GEOCODE_STATE'].alias("GEOCODE_STATE"),
                                    unmapped_private_address_geocode['GEOCODE_COUNTY'].alias("GEOCODE_COUNTY"),
                                    unmapped_private_address_geocode['GEOCODE_LONGITUDE'].alias("GEOCODE_LONGITUDE"),
                                    unmapped_private_address_geocode['GEOCODE_LATITUDE'].alias("GEOCODE_LATITUDE"),
                                    unmapped_private_address_geocode['GEOCODE_BLOCK'].alias("GEOCODE_BLOCK"),
                                    unmapped_private_address_geocode['GEOCODE_TRACT'].alias("GEOCODE_TRACT"),
                                    unmapped_private_address_geocode['GEOCODE_GROUP'].alias("GEOCODE_GROUP"),
                                    unmapped_private_address_geocode['GEOCODE_ZCTA'].alias("GEOCODE_ZCTA"),
                                    unmapped_private_address_geocode['GEOCODE_CUSTOM'].alias("GEOCODE_CUSTOM"),
                                    unmapped_private_address_geocode['GEOCODE_CUSTOM_TEXT'].alias("GEOCODE_CUSTOM_TEXT"),
                                    unmapped_private_address_geocode['SHAPEFILE'].alias("SHAPEFILE"),
                                    unmapped_private_address_geocode['GEO_ACCURACY'].alias("GEO_ACCURACY"),
                                    unmapped_private_address_geocode['GEO_PROV_REF'].alias("GEO_PROV_REF"),
                                    unmapped_private_address_geocode['ASSIGNMENT_DATE'].alias("ASSIGNMENT_DATE"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################



        cf.write_pyspark_output_file(
                        payspark_df = private_address_geocode,
                        output_file_name = "mapped_private_address_geocode.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'private_address_geocode_mapper.py' ,
                            text = str(e)
                            )

    cf.print_with_style(str(e), 'danger red')