##################################################################################################################################
# This script will map a PCORNet demographic table 
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
spark = cf.get_spark_session("demographic_mapper")
 
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
    # Loading the unmapped demographic table
    ###################################################################################################################################

        unmapped_demographic = cf.spark_read(deduplicated_data_folder_path + "deduplicated_demographic.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_sex_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_sex_dict.items())])
        mapping_sexual_orientation_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_sexual_orientation_dict.items())])
        mapping_gender_identity_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_gender_identity_dict.items())])
        mapping_hispanic_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_hispanic_dict.items())])
        mapping_race_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_dict.items())])
        mapping_biobank_flag_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_biobank_flag_dict.items())])
        mapping_pat_pref_language_spoken_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_pat_pref_language_spoken_dict.items())])
        mapping_race_eth_missing_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_missing_dict.items())])
        mapping_race_eth_ai_an_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_ai_an_dict.items())])
        mapping_race_eth_asian_dict    = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_asian_dict.items())])
        mapping_race_eth_black_dict    = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_black_dict.items())])
        mapping_race_eth_hispanic_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_hispanic_dict.items())])
        mapping_race_eth_me_na_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_me_na_dict.items())])
        mapping_race_eth_nh_pi_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_nh_pi_dict.items())])
        mapping_race_eth_white_dict = create_map([lit(x) for x in chain(*partner_dictionaries.demographic_race_eth_white_dict.items())])


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        demographic = unmapped_demographic.select(              
            
                                    cf.encrypt_id_udf(unmapped_demographic['PATID']).alias("PATID"),
                                    unmapped_demographic['BIRTH_DATE'].alias("BIRTH_DATE"),
                                    unmapped_demographic['BIRTH_TIME'].alias("BIRTH_TIME"),
                                    coalesce(mapping_sex_dict[upper(col('SEX'))],col('SEX')).alias("SEX"),
                                    coalesce(mapping_sexual_orientation_dict[upper(col('SEXUAL_ORIENTATION'))],col('SEXUAL_ORIENTATION')).alias("SEXUAL_ORIENTATION"),
                                    coalesce(mapping_gender_identity_dict[upper(col('GENDER_IDENTITY'))],col('GENDER_IDENTITY')).alias("GENDER_IDENTITY"),
                                    coalesce(mapping_hispanic_dict[upper(col('HISPANIC'))],col('HISPANIC')).alias("HISPANIC"),
                                    coalesce(mapping_race_dict[upper(col('RACE'))],col('RACE')).alias("RACE"),
                                    coalesce(mapping_race_eth_missing_dict[upper(col('RACE_ETH_MISSING'))],col('RACE_ETH_MISSING')).alias("RACE_ETH_MISSING"),
                                    coalesce(mapping_race_eth_ai_an_dict[upper(col('RACE_ETH_AI_AN'))],col('RACE_ETH_AI_AN')).alias("RACE_ETH_AI_AN"),
                                    coalesce(mapping_race_eth_asian_dict[upper(col('RACE_ETH_ASIAN'))],col('RACE_ETH_ASIAN')).alias("RACE_ETH_ASIAN"),
                                    coalesce(mapping_race_eth_black_dict[upper(col('RACE_ETH_BLACK'))],col('RACE_ETH_BLACK')).alias("RACE_ETH_BLACK"),
                                    coalesce(mapping_race_eth_hispanic_dict[upper(col('RACE_ETH_HISPANIC'))],col('RACE_ETH_HISPANIC')).alias("RACE_ETH_HISPANIC"),
                                    coalesce(mapping_race_eth_me_na_dict[upper(col('RACE_ETH_ME_NA'))],col('RACE_ETH_ME_NA')).alias("RACE_ETH_ME_NA"),
                                    coalesce(mapping_race_eth_nh_pi_dict[upper(col('RACE_ETH_NH_PI'))],col('RACE_ETH_NH_PI')).alias("RACE_ETH_NH_PI"),
                                    coalesce(mapping_race_eth_white_dict[upper(col('RACE_ETH_WHITE'))],col('RACE_ETH_WHITE')).alias("RACE_ETH_WHITE"),
                                    coalesce(mapping_biobank_flag_dict[upper(col('BIOBANK_FLAG'))],col('BIOBANK_FLAG')).alias("BIOBANK_FLAG"),
                                    coalesce(mapping_pat_pref_language_spoken_dict[upper(col('PAT_PREF_LANGUAGE_SPOKEN'))],col('PAT_PREF_LANGUAGE_SPOKEN')).alias("PAT_PREF_LANGUAGE_SPOKEN"),               
                                    unmapped_demographic['RAW_SEX'].alias("RAW_SEX"),
                                    unmapped_demographic['RAW_SEXUAL_ORIENTATION'].alias("RAW_SEXUAL_ORIENTATION"),
                                    unmapped_demographic['RAW_GENDER_IDENTITY'].alias("RAW_GENDER_IDENTITY"),
                                    unmapped_demographic['RAW_HISPANIC'].alias("RAW_HISPANIC"),
                                    unmapped_demographic['RAW_RACE'].alias("RAW_RACE"),
                                    unmapped_demographic['RAW_PAT_PREF_LANGUAGE_SPOKEN'].alias("RAW_PAT_PREF_LANGUAGE_SPOKEN"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_demographic['ZIP_CODE'].alias("ZIP_CODE"),
                                    unmapped_demographic['PATID'].alias("JOIN_FIELD"),
                                

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        demographic_with_additional_fileds = cf.append_additional_fields(
            mapped_df = demographic,
            file_name = "deduplicated_demographic.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "PATID",
            spark = spark)
 

        cf.write_pyspark_output_file(
                        payspark_df = demographic_with_additional_fileds,
                        output_file_name = "mapped_demographic.csv",
                        output_data_folder_path= mapped_data_folder_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'demographic_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')