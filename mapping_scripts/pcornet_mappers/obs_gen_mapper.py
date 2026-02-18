###################################################################################################################################
# This script will map a PCORNet obs_gen table 
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
spark = cf.get_spark_session("obs_gen_mapper") 

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
        #deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' + 'generated_deduplicates' + '/'
        # deduplicated_data_folder_path = '/app/partners/'+input_partner.lower()+'/data/deduplicator_output/'+ input_data_folder+'/'
        #deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' + 'generated_deduplicates' + '/'
        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################



        unmapped_obs_gen    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_obs_gen.csv", spark)



    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_obsgen_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_gen_obsgen_type_dict.items())])
        mapping_obsgen_result_qual_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_gen_obsgen_result_qual_dict.items())])
        mapping_obsgen_result_modifier_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_gen_obsgen_result_modifier_dict.items())])
        mapping_obsgen_result_unit_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_gen_obsgen_result_unit_dict.items())])
        mapping_obsgen_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_gen_obsgen_source_dict.items())])
        mapping_obsgen_abn_ind_dict = create_map([lit(x) for x in chain(*partner_dictionaries.obs_gen_obsgen_abn_ind_dict.items())])





    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        obs_gen = unmapped_obs_gen.select(              
            
            
                                    cf.encrypt_id_udf(concat(unmapped_obs_gen['OBSGENID'],lit('OBS_GEN'))).alias("OBSGENID"),
                                    cf.encrypt_id_udf(unmapped_obs_gen['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_obs_gen['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    cf.encrypt_id_udf(unmapped_obs_gen['OBSGEN_PROVIDERID']).alias("OBSGEN_PROVIDERID"),
                                    unmapped_obs_gen['OBSGEN_START_DATE'].alias("OBSGEN_START_DATE"),
                                    unmapped_obs_gen['OBSGEN_START_TIME'].alias("OBSGEN_START_TIME"),
                                    unmapped_obs_gen['OBSGEN_STOP_DATE'].alias("OBSGEN_STOP_DATE"),
                                    unmapped_obs_gen['OBSGEN_STOP_TIME'].alias("OBSGEN_STOP_TIME"),
                                    coalesce(mapping_obsgen_type_dict[upper(col("OBSGEN_TYPE"))],col('OBSGEN_TYPE')).alias("OBSGEN_TYPE"),
                                    unmapped_obs_gen['OBSGEN_CODE'].alias("OBSGEN_CODE"),
                                    coalesce(mapping_obsgen_result_qual_dict[upper(col("OBSGEN_RESULT_QUAL"))],col('OBSGEN_RESULT_QUAL')).alias("OBSGEN_RESULT_QUAL"),
                                    unmapped_obs_gen['OBSGEN_RESULT_TEXT'].alias("OBSGEN_RESULT_TEXT"),
                                    unmapped_obs_gen['OBSGEN_RESULT_NUM'].alias("OBSGEN_RESULT_NUM"),
                                    coalesce(mapping_obsgen_result_modifier_dict[upper(col("OBSGEN_RESULT_MODIFIER"))],col('OBSGEN_RESULT_MODIFIER')).alias("OBSGEN_RESULT_MODIFIER"),
                                    coalesce(mapping_obsgen_result_unit_dict[col("OBSGEN_RESULT_UNIT")],col('OBSGEN_RESULT_UNIT')).alias("OBSGEN_RESULT_UNIT"),
                                    unmapped_obs_gen['OBSGEN_TABLE_MODIFIED'].alias("OBSGEN_TABLE_MODIFIED"),
                                    unmapped_obs_gen['OBSGEN_ID_MODIFIED'].alias("OBSGEN_ID_MODIFIED"),                             
                                    coalesce(mapping_obsgen_source_dict[upper(col("OBSGEN_SOURCE"))],col('OBSGEN_SOURCE')).alias("OBSGEN_SOURCE"),
                                    coalesce(mapping_obsgen_abn_ind_dict[upper(col("OBSGEN_ABN_IND"))],col('OBSGEN_ABN_IND')).alias("OBSGEN_ABN_IND"),
                                    unmapped_obs_gen['RAW_OBSGEN_NAME'].alias("RAW_OBSGEN_NAME"),
                                    unmapped_obs_gen['RAW_OBSGEN_CODE'].alias("RAW_OBSGEN_CODE"),
                                    unmapped_obs_gen['RAW_OBSGEN_TYPE'].alias("RAW_OBSGEN_TYPE"),
                                    unmapped_obs_gen['RAW_OBSGEN_RESULT'].alias("RAW_OBSGEN_RESULT"),
                                    unmapped_obs_gen['RAW_OBSGEN_UNIT'].alias("RAW_OBSGEN_UNIT"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_obs_gen['OBSGENID'].alias("JOIN_FIELD"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        obs_gen_with_additional_fileds = cf.append_additional_fields(
            mapped_df = obs_gen,
            file_name = "deduplicated_obs_gen.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "OBSGENID",
            spark = spark)


        cf.write_pyspark_output_file(
                        payspark_df = obs_gen_with_additional_fileds,
                        output_file_name = "mapped_obs_gen.csv",
                        output_data_folder_path= mapped_data_folder_path)

        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'obs_gen_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')