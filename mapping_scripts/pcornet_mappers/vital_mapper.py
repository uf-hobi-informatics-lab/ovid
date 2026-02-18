###################################################################################################################################
# This script will map a PCORNet vital table 
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
spark = cf.get_spark_session("vital_mapper")
 

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


        unmapped_vital    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_vital.csv", spark)



    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_bp_position_dict = create_map([lit(x) for x in chain(*partner_dictionaries.vital_bp_position_dict.items())])
        mapping_vital_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.vital_vital_source_dict.items())])
        mapping_smoking_dict = create_map([lit(x) for x in chain(*partner_dictionaries.vital_smoking_dict.items())])
        mapping_tobacco_dict = create_map([lit(x) for x in chain(*partner_dictionaries.vital_tobacco_dict.items())])
        mapping_tobacco_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.vital_tobacco_type_dict.items())])

    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        vital = unmapped_vital.select( 


                                    cf.encrypt_id_udf(unmapped_vital['VITALID']).alias("VITALID"),
                                    cf.encrypt_id_udf(unmapped_vital['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_vital['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    unmapped_vital['MEASURE_DATE'].alias("MEASURE_DATE"),
                                    unmapped_vital['MEASURE_TIME'].alias("MEASURE_TIME"),
                                    coalesce(mapping_vital_source_dict[upper(col('VITAL_SOURCE'))],col('VITAL_SOURCE')).alias("VITAL_SOURCE"),
                                    unmapped_vital['HT'].alias("HT"),
                                    unmapped_vital['WT'].alias("WT"),
                                    unmapped_vital['DIASTOLIC'].alias("DIASTOLIC"),
                                    unmapped_vital['SYSTOLIC'].alias("SYSTOLIC"),
                                    unmapped_vital['ORIGINAL_BMI'].alias("ORIGINAL_BMI"),
                                    coalesce(mapping_bp_position_dict[upper(col('BP_POSITION'))],col('BP_POSITION')).alias("BP_POSITION"),
                                    coalesce(mapping_smoking_dict[upper(col('SMOKING'))],col('SMOKING')).alias("SMOKING"),
                                    coalesce(mapping_tobacco_dict[upper(col('TOBACCO'))],col('TOBACCO')).alias("TOBACCO"),
                                    coalesce(mapping_tobacco_type_dict[upper(col('TOBACCO_TYPE'))],col('TOBACCO_TYPE')).alias("TOBACCO_TYPE"),
                                    unmapped_vital['RAW_DIASTOLIC'].alias("RAW_DIASTOLIC"),
                                    unmapped_vital['RAW_SYSTOLIC'].alias("RAW_SYSTOLIC"),
                                    unmapped_vital['RAW_BP_POSITION'].alias("RAW_BP_POSITION"),
                                    unmapped_vital['RAW_SMOKING'].alias("RAW_SMOKING"),
                                    unmapped_vital['RAW_TOBACCO'].alias("RAW_TOBACCO"),
                                    unmapped_vital['RAW_TOBACCO_TYPE'].alias("RAW_TOBACCO_TYPE"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_vital['VITALID'].alias("JOIN_FIELD"),

                                    
                                

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        vital_with_additional_fileds = cf.append_additional_fields(
            mapped_df = vital,
            file_name = "deduplicated_vital.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "VITALID",
            spark = spark)


        cf.write_pyspark_output_file(
                        payspark_df = vital_with_additional_fileds,
                        output_file_name = "mapped_vital.csv",
                        output_data_folder_path= mapped_data_folder_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'vital_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')