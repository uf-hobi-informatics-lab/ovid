###################################################################################################################################
# This script will map a PCORNet procedures table 
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
spark = cf.get_spark_session("procedures_mapper") 

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

        unmapped_procedures    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_procedures.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_procedures_enc_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.procedures_enc_type_dict.items())])
        mapping_px_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.procedures_px_type_dict.items())])
        mapping_px_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.procedures_px_source_dict.items())])
        mapping_ppx_dict = create_map([lit(x) for x in chain(*partner_dictionaries.procedures_ppx_dict.items())])






    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        procedures = unmapped_procedures.select(              
            
            
                                    cf.encrypt_id_udf(unmapped_procedures['PROCEDURESID']).alias("PROCEDURESID"),
                                    cf.encrypt_id_udf(unmapped_procedures['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_procedures['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    coalesce(mapping_procedures_enc_type_dict[upper(col("ENC_TYPE"))],col('ENC_TYPE')).alias("ENC_TYPE"),
                                    unmapped_procedures['ADMIT_DATE'].alias("ADMIT_DATE"),
                                    cf.encrypt_id_udf(unmapped_procedures['PROVIDERID']).alias("PROVIDERID"),
                                    unmapped_procedures['PX_DATE'].alias("PX_DATE"),
                                    unmapped_procedures['PX'].alias("PX"),
                                    coalesce(mapping_px_type_dict[upper(col("PX_TYPE"))],col('PX_TYPE')).alias("PX_TYPE"),
                                    coalesce(mapping_px_source_dict[upper(col("PX_SOURCE"))],col('PX_SOURCE')).alias("PX_SOURCE"),
                                    coalesce(mapping_ppx_dict[upper(col("PPX"))],col('PPX')).alias("PPX"),
                                    unmapped_procedures['RAW_PX'].alias("RAW_PX"),
                                    unmapped_procedures['RAW_PX_TYPE'].alias("RAW_PX_TYPE"),
                                    unmapped_procedures['RAW_PPX'].alias("RAW_PPX"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_procedures['PROCEDURESID'].alias("JOIN_FIELD"),

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        procedures_with_additional_fileds = cf.append_additional_fields(
            mapped_df = procedures,
            file_name = "deduplicated_procedures.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "PROCEDURESID",
            spark = spark)

        cf.write_pyspark_output_file(
                        payspark_df = procedures_with_additional_fileds,
                        output_file_name = "mapped_procedures.csv",
                        output_data_folder_path= mapped_data_folder_path)

        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'procedures_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')