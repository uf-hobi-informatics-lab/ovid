###################################################################################################################################
# This script will map a PCORNet pcornet_trial table 
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
spark = cf.get_spark_session("pcornet_trial_mapper")
 
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


        unmapped_pcornet_trial    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_pcornet_trial.csv", spark)


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        pcornet_trial = unmapped_pcornet_trial.select(              
            
                                    cf.encrypt_id_udf(unmapped_pcornet_trial['PATID']).alias("PATID"),
                                    unmapped_pcornet_trial['TRIALID'].alias("TRIALID"),
                                    unmapped_pcornet_trial['PARTICIPANTID'].alias("PARTICIPANTID"),
                                    unmapped_pcornet_trial['TRIAL_SITEID'].alias("TRIAL_SITEID"),
                                    unmapped_pcornet_trial['TRIAL_ENROLL_DATE'].alias("TRIAL_ENROLL_DATE"),
                                    unmapped_pcornet_trial['TRIAL_END_DATE'].alias("TRIAL_END_DATE"),
                                    unmapped_pcornet_trial['TRIAL_WITHDRAW_DATE'].alias("TRIAL_WITHDRAW_DATE"),
                                    unmapped_pcornet_trial['TRIAL_INVITE_CODE'].alias("TRIAL_INVITE_CODE"),                                    
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_pcornet_trial['PATID'].alias("JOIN_FIELD"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        pcornet_trial_with_additional_fileds = cf.append_additional_fields(
            mapped_df = pcornet_trial,
            file_name = "deduplicated_pcornet_trial.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "PATID",
            spark = spark)



        cf.write_pyspark_output_file(
                        payspark_df = pcornet_trial_with_additional_fileds,
                        output_file_name = "mapped_pcornet_trial.csv",
                        output_data_folder_path= mapped_data_folder_path)

        spark.stop()

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'pcornet_trial_mapper.py' ,
                            text = str(e)
                            )

