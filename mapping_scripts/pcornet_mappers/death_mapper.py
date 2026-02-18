###################################################################################################################################
# This script will map a PCORNet death table 
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
spark = cf.get_spark_session("death_mapper")
 
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


        unmapped_death    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_death.csv", spark)




    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_death_date_impute_dict = create_map([lit(x) for x in chain(*partner_dictionaries.death_death_date_impute_dict.items())])
        mapping_death_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.death_death_source_dict.items())])
        mapping_death_match_confidence_dict = create_map([lit(x) for x in chain(*partner_dictionaries.death_death_match_confidence_dict.items())])




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        death = unmapped_death.select(              
            
                                    cf.encrypt_id_udf(unmapped_death['PATID']).alias("PATID"),
                                    unmapped_death['DEATH_DATE'].alias("DEATH_DATE"),
                                    coalesce(mapping_death_date_impute_dict[upper(col('DEATH_DATE_IMPUTE'))],col('DEATH_DATE_IMPUTE')).alias("DEATH_DATE_IMPUTE"),
                                    coalesce(mapping_death_source_dict[upper(col('DEATH_SOURCE'))],col('DEATH_SOURCE')).alias("DEATH_SOURCE"),
                                    coalesce(mapping_death_match_confidence_dict[upper(col('DEATH_MATCH_CONFIDENCE'))],col('DEATH_MATCH_CONFIDENCE')).alias("DEATH_MATCH_CONFIDENCE"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_death['PATID'].alias("JOIN_FIELD"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        death_with_additional_fileds = cf.append_additional_fields(
            mapped_df = death,
            file_name = "deduplicated_death.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "PATID",
            spark = spark)



        cf.write_pyspark_output_file(
                        payspark_df = death_with_additional_fileds,
                        output_file_name = "mapped_death.csv",
                        output_data_folder_path= mapped_data_folder_path)

        spark.stop()

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'death_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')