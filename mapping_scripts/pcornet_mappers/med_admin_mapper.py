###################################################################################################################################
# This script will map a PCORNet med_admin table 
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
spark = cf.get_spark_session("med_admin_mapper")
 
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
        # deduplicated_data_folder_path = '/app/partners/'+input_partner.lower()+'/data/deduplicator_output/'+ input_data_folder+'/'
        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        unmapped_med_admin    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_med_admin.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_medadmin_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.med_admin_medadmin_type_dict.items())])
        mapping_medadmin_dose_admin_unit_dict = create_map([lit(x) for x in chain(*partner_dictionaries.med_admin_medadmin_dose_admin_unit_dict.items())])
        mapping_medadmin_route_dict = create_map([lit(x) for x in chain(*partner_dictionaries.med_admin_medadmin_route_dict.items())])
        mapping_medadmin_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.med_admin_medadmin_source_dict.items())])






    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        med_admin = unmapped_med_admin.select(              
            
            
                                    cf.encrypt_id_udf(unmapped_med_admin['MEDADMINID']).alias("MEDADMINID"),
                                    cf.encrypt_id_udf(unmapped_med_admin['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_med_admin['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    cf.encrypt_id_udf(unmapped_med_admin['PRESCRIBINGID']).alias("PRESCRIBINGID"),
                                    cf.encrypt_id_udf(unmapped_med_admin['MEDADMIN_PROVIDERID']).alias("MEDADMIN_PROVIDERID"),
                                    unmapped_med_admin['MEDADMIN_START_DATE'].alias("MEDADMIN_START_DATE"),
                                    unmapped_med_admin['MEDADMIN_START_TIME'].alias("MEDADMIN_START_TIME"),
                                    unmapped_med_admin['MEDADMIN_STOP_DATE'].alias("MEDADMIN_STOP_DATE"),
                                    unmapped_med_admin['MEDADMIN_STOP_TIME'].alias("MEDADMIN_STOP_TIME"),
                                    coalesce(mapping_medadmin_type_dict[upper(col("MEDADMIN_TYPE"))],col('MEDADMIN_TYPE')).alias("MEDADMIN_TYPE"),
                                    unmapped_med_admin['MEDADMIN_CODE'].alias("MEDADMIN_CODE"),                                    
                                    unmapped_med_admin['MEDADMIN_DOSE_ADMIN'].alias("MEDADMIN_DOSE_ADMIN"),
                                    coalesce(mapping_medadmin_dose_admin_unit_dict[col("MEDADMIN_DOSE_ADMIN_UNIT")],col('MEDADMIN_DOSE_ADMIN_UNIT')).alias("MEDADMIN_DOSE_ADMIN_UNIT"),
                                    coalesce(mapping_medadmin_route_dict[upper(col("MEDADMIN_ROUTE"))],col('MEDADMIN_ROUTE')).alias("MEDADMIN_ROUTE"),
                                    coalesce(mapping_medadmin_source_dict[upper(col("MEDADMIN_SOURCE"))],col('MEDADMIN_SOURCE')).alias("MEDADMIN_SOURCE"),
                                    unmapped_med_admin['RAW_MEDADMIN_MED_NAME'].alias("RAW_MEDADMIN_MED_NAME"),
                                    unmapped_med_admin['RAW_MEDADMIN_CODE'].alias("RAW_MEDADMIN_CODE"),
                                    unmapped_med_admin['RAW_MEDADMIN_DOSE_ADMIN'].alias("RAW_MEDADMIN_DOSE_ADMIN"),
                                    unmapped_med_admin['RAW_MEDADMIN_DOSE_ADMIN_UNIT'].alias("RAW_MEDADMIN_DOSE_ADMIN_UNIT"),
                                    unmapped_med_admin['RAW_MEDADMIN_ROUTE'].alias("RAW_MEDADMIN_ROUTE"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_med_admin['MEDADMINID'].alias("JOIN_FIELD"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        med_admin_with_additional_fileds = cf.append_additional_fields(
            mapped_df = med_admin,
            file_name = "deduplicated_med_admin.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "MEDADMINID",
            spark = spark)


        cf.write_pyspark_output_file(
                        payspark_df = med_admin_with_additional_fileds,
                        output_file_name = "mapped_med_admin.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'med_admin_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')