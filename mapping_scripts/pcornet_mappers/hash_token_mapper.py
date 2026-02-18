###################################################################################################################################
# This script will map a PCORNet hash_token table 
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
spark = cf.get_spark_session("hash_token_mapper")
 
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

        # deduplicated_data_folder_path = '/app/partners/'+input_partner.lower()+'/data/formatter_output/'+ input_data_folder+'/'
        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        unmapped_hash_token    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_hash_token.csv", spark)




    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        hash_token = unmapped_hash_token.select(              
            
                                    cf.encrypt_id_udf(unmapped_hash_token['PATID']).alias("PATID"),
                                    unmapped_hash_token['TOKEN_01'],
                                    unmapped_hash_token['TOKEN_02'],
                                    unmapped_hash_token['TOKEN_03'],
                                    unmapped_hash_token['TOKEN_04'],
                                    unmapped_hash_token['TOKEN_05'],
                                    unmapped_hash_token['TOKEN_06'],
                                    unmapped_hash_token['TOKEN_07'],
                                    unmapped_hash_token['TOKEN_08'],
                                    unmapped_hash_token['TOKEN_09'],
                                    unmapped_hash_token['TOKEN_12'],
                                    unmapped_hash_token['TOKEN_14'],
                                    unmapped_hash_token['TOKEN_15'],
                                    unmapped_hash_token['TOKEN_16'],
                                    unmapped_hash_token['TOKEN_17'],
                                    unmapped_hash_token['TOKEN_18'],
                                    unmapped_hash_token['TOKEN_23'],
                                    unmapped_hash_token['TOKEN_24'],
                                    unmapped_hash_token['TOKEN_25'],
                                    unmapped_hash_token['TOKEN_26'],
                                    unmapped_hash_token['TOKEN_29'],
                                    unmapped_hash_token['TOKEN_30'],
                                    unmapped_hash_token['TOKEN_101'],
                                    unmapped_hash_token['TOKEN_102'],
                                    unmapped_hash_token['TOKEN_103'],
                                    unmapped_hash_token['TOKEN_104'],
                                    unmapped_hash_token['TOKEN_105'],
                                    unmapped_hash_token['TOKEN_106'],
                                    unmapped_hash_token['TOKEN_107'],
                                    unmapped_hash_token['TOKEN_108'],
                                    unmapped_hash_token['TOKEN_109'],
                                    unmapped_hash_token['TOKEN_110'],
                                    unmapped_hash_token['TOKEN_111'],
                                    unmapped_hash_token['TOKEN_ENCRYPTION_KEY'],
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),



                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################


        cf.write_pyspark_output_file(
                        payspark_df = hash_token,
                        output_file_name = "mapped_hash_token.csv",
                        output_data_folder_path= mapped_data_folder_path)

        spark.stop()

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'hash_token_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')