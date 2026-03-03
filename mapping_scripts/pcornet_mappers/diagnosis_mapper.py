###################################################################################################################################
# This script will map a PCORNet diagnosis table 
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
spark = cf.get_spark_session("diagnosis_mapper")
 
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


        unmapped_diagnosis    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_diagnosis.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_diagnosis_enc_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.diagnosis_enc_type_dict.items())])
        mapping_dx_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.diagnosis_dx_type_dict.items())])
        mapping_dx_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.diagnosis_dx_source_dict.items())])
        mapping_dx_origin_dict = create_map([lit(x) for x in chain(*partner_dictionaries.diagnosis_dx_origin_dict.items())])
        mapping_pdx_dict = create_map([lit(x) for x in chain(*partner_dictionaries.diagnosis_pdx_dict.items())])
        mapping_dx_poa_dict = create_map([lit(x) for x in chain(*partner_dictionaries.diagnosis_dx_poa_dict.items())])






    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        diagnosis = unmapped_diagnosis.select(              
            
            
                                    cf.encrypt_id_udf(unmapped_diagnosis['DIAGNOSISID']).alias("DIAGNOSISID"),
                                    cf.encrypt_id_udf(unmapped_diagnosis['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_diagnosis['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    coalesce(mapping_diagnosis_enc_type_dict[upper(col("ENC_TYPE"))],col('ENC_TYPE')).alias("ENC_TYPE"),
                                    unmapped_diagnosis['ADMIT_DATE'].alias("ADMIT_DATE"),
                                    cf.encrypt_id_udf(unmapped_diagnosis['PROVIDERID']).alias("PROVIDERID"),
                                    unmapped_diagnosis['DX'].alias("DX"),
                                    coalesce(mapping_dx_type_dict[upper(col("DX_TYPE"))],col('DX_TYPE')).alias("DX_TYPE"),
                                    unmapped_diagnosis['DX_DATE'].alias("DX_DATE"),
                                    coalesce(mapping_dx_source_dict[upper(col("DX_SOURCE"))],col('DX_SOURCE')).alias("DX_SOURCE"),
                                    coalesce(mapping_dx_origin_dict[upper(col("DX_ORIGIN"))],col('DX_ORIGIN')).alias("DX_ORIGIN"),
                                    coalesce(mapping_pdx_dict[upper(col("PDX"))],col('PDX')).alias("PDX"),
                                    coalesce(mapping_dx_poa_dict[upper(col("DX_POA"))],col('DX_POA')).alias("DX_POA"),
                                    unmapped_diagnosis['RAW_DX'].alias("RAW_DX"),
                                    unmapped_diagnosis['RAW_DX_TYPE'].alias("RAW_DX_TYPE"),
                                    unmapped_diagnosis['RAW_DX_SOURCE'].alias("RAW_DX_SOURCE"),
                                    unmapped_diagnosis['RAW_PDX'].alias("RAW_PDX"),
                                    unmapped_diagnosis['RAW_DX_POA'].alias("RAW_DX_POA"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_diagnosis['DIAGNOSISID'].alias("JOIN_FIELD"),
                                

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        diagnosis_with_additional_fields = cf.append_additional_fields(
            mapped_df = diagnosis,
            file_name = "deduplicated_diagnosis.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "DIAGNOSISID",
            spark = spark)


        cf.write_pyspark_output_file(
                        payspark_df = diagnosis_with_additional_fields,
                        output_file_name = "mapped_diagnosis.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'diagnosis_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')