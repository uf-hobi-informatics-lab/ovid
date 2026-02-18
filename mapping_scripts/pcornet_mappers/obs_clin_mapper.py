###################################################################################################################################
# This script will map a PCORNet obs_clin table 
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
spark = cf.get_spark_session("obs_clin_mapper")

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


        unmapped_obs_clin    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_obs_clin.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_obsclin_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_clin_obsclin_type_dict.items())])
        mapping_obsclin_result_qual_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_clin_obsclin_result_qual_dict.items())])
        mapping_obsclin_result_modifier_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_clin_obsclin_result_modifier_dict.items())])
        mapping_obsclin_result_unit_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_clin_obsclin_result_unit_dict.items())])
        mapping_obsclin_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_clin_obsclin_source_dict.items())])
        mapping_obsclin_abn_ind_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_clin_obsclin_abn_ind_dict.items())])




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        obs_clin = unmapped_obs_clin.select(              
            
            
                                    cf.encrypt_id_udf(concat(unmapped_obs_clin['OBSCLINID'], lit('OBS_CLIN'))).alias("OBSCLINID"),
                                    cf.encrypt_id_udf(unmapped_obs_clin['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_obs_clin['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    cf.encrypt_id_udf(unmapped_obs_clin['OBSCLIN_PROVIDERID']).alias("OBSCLIN_PROVIDERID"),
                                    unmapped_obs_clin['OBSCLIN_START_DATE'].alias("OBSCLIN_START_DATE"),
                                    unmapped_obs_clin['OBSCLIN_START_TIME'].alias("OBSCLIN_START_TIME"),
                                    unmapped_obs_clin['OBSCLIN_STOP_DATE'].alias("OBSCLIN_STOP_DATE"),
                                    unmapped_obs_clin['OBSCLIN_STOP_TIME'].alias("OBSCLIN_STOP_TIME"),
                                    coalesce(mapping_obsclin_type_dict[upper(col("OBSCLIN_TYPE"))],col('OBSCLIN_TYPE')).alias("OBSCLIN_TYPE"),
                                    unmapped_obs_clin['OBSCLIN_CODE'].alias("OBSCLIN_CODE"),
                                    coalesce(mapping_obsclin_result_qual_dict[upper(col("OBSCLIN_RESULT_QUAL"))],col('OBSCLIN_RESULT_QUAL')).alias("OBSCLIN_RESULT_QUAL"),
                                    unmapped_obs_clin['OBSCLIN_RESULT_TEXT'].alias("OBSCLIN_RESULT_TEXT"),
                                    unmapped_obs_clin['OBSCLIN_RESULT_SNOMED'].alias("OBSCLIN_RESULT_SNOMED"),
                                    unmapped_obs_clin['OBSCLIN_RESULT_NUM'].alias("OBSCLIN_RESULT_NUM"),
                                    coalesce(mapping_obsclin_result_modifier_dict[upper(col("OBSCLIN_RESULT_MODIFIER"))],col('OBSCLIN_RESULT_MODIFIER')).alias("OBSCLIN_RESULT_MODIFIER"),
                                    coalesce(mapping_obsclin_result_unit_dict[col("OBSCLIN_RESULT_UNIT")],col('OBSCLIN_RESULT_UNIT')).alias("OBSCLIN_RESULT_UNIT"),
                                    coalesce(mapping_obsclin_source_dict[upper(col("OBSCLIN_SOURCE"))],col('OBSCLIN_SOURCE')).alias("OBSCLIN_SOURCE"),
                                    coalesce(mapping_obsclin_abn_ind_dict[upper(col("OBSCLIN_ABN_IND"))],col('OBSCLIN_ABN_IND')).alias("OBSCLIN_ABN_IND"),
                                    unmapped_obs_clin['RAW_OBSCLIN_NAME'].alias("RAW_OBSCLIN_NAME"),
                                    unmapped_obs_clin['RAW_OBSCLIN_CODE'].alias("RAW_OBSCLIN_CODE"),
                                    unmapped_obs_clin['RAW_OBSCLIN_TYPE'].alias("RAW_OBSCLIN_TYPE"),
                                    unmapped_obs_clin['RAW_OBSCLIN_RESULT'].alias("RAW_OBSCLIN_RESULT"),
                                    unmapped_obs_clin['RAW_OBSCLIN_MODIFIER'].alias("RAW_OBSCLIN_MODIFIER"),
                                    unmapped_obs_clin['RAW_OBSCLIN_UNIT'].alias("RAW_OBSCLIN_UNIT"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_obs_clin['OBSCLINID'].alias("JOIN_FIELD"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        obs_clin_with_additional_fileds = cf.append_additional_fields(
            mapped_df = obs_clin,
            file_name = "deduplicated_obs_clin.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "OBSCLINID",
            spark = spark)

        cf.write_pyspark_output_file(
                        payspark_df = obs_clin_with_additional_fileds,
                        output_file_name = "mapped_obs_clin.csv",
                        output_data_folder_path= mapped_data_folder_path)

        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'obs_clin_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')