###################################################################################################################################
# This script will map a PCORNet dispensing table 
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


try:
 

    ###################################################################################################################################
    # Test if the partner name is valid or not
    ###################################################################################################################################


    if  not cf.valid_partner_name(input_partner):

        print("Error: Unrecognized partner "+input_partner+" !!!!!")
        sys.exit()

    else:



    ###################################################################################################################################
    # Load the config file for the selected partner
    ###################################################################################################################################

        partner_dictionaries_path = "partners."+input_partner+".dictionaries"
        partner_dictionaries = importlib.import_module(partner_dictionaries_path)

        # deduplicated_data_folder_path = '/app/partners/'+input_partner.lower()+'/data/deduplicator_output/'+ input_data_folder+'/'
        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'




    ###################################################################################################################################
    # spin the pyspak cluster and
    # Loading the unmapped dispensing table
    ###################################################################################################################################

        spark = cf.get_spark_session("dispensing_mapper")

        #unmapped_dispensing = spark.read.option("inferSchema", "false").load(deduplicated_data_folder_path+"deduplicated_dispensing.csv",format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
    
        unmapped_dispensing  = cf.spark_read(deduplicated_data_folder_path+"deduplicated_dispensing.csv", spark)

    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################

        mapping_dispense_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.dispensing_dispense_source_dict.items())])
        mapping_dispense_dose_disp_unit_dict = create_map([lit(x) for x in chain(*partner_dictionaries.dispensing_dispense_dose_disp_unit_dict.items())])
        mapping_dispense_route_dict = create_map([lit(x) for x in chain(*partner_dictionaries.dispensing_dispense_route_dict.items())])
        


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped dispensing table
    ###################################################################################################################################


        dispensing = unmapped_dispensing.select(              
            
                                    cf.encrypt_id_udf(unmapped_dispensing['DISPENSINGID']).alias("DISPENSINGID"),
                                    cf.encrypt_id_udf(unmapped_dispensing['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_dispensing['PRESCRIBINGID']).alias("PRESCRIBINGID"),
                                    unmapped_dispensing['DISPENSE_DATE'].alias("DISPENSE_DATE"),
                                    unmapped_dispensing['NDC'].alias("NDC"),
                                    mapping_dispense_source_dict[upper(col("DISPENSE_SOURCE"))].alias("DISPENSE_SOURCE"),
                                    unmapped_dispensing['DISPENSE_SUP'].alias("DISPENSE_SUP"),
                                    unmapped_dispensing['DISPENSE_AMT'].alias("DISPENSE_AMT"),
                                    unmapped_dispensing['DISPENSE_DOSE_DISP'].alias("DISPENSE_DOSE_DISP"),
                                    mapping_dispense_dose_disp_unit_dict[upper(col("DISPENSE_DOSE_DISP_UNIT"))].alias("DISPENSE_DOSE_DISP_UNIT"),
                                    mapping_dispense_route_dict[upper(col("DISPENSE_ROUTE"))].alias("DISPENSE_ROUTE"),
                                    unmapped_dispensing['RAW_NDC'].alias("RAW_NDC"),
                                    unmapped_dispensing['RAW_DISPENSE_DOSE_DISP'].alias("RAW_DISPENSE_DOSE_DISP"),
                                    unmapped_dispensing['RAW_DISPENSE_DOSE_DISP_UNIT'].alias("RAW_DISPENSE_DOSE_DISP_UNIT"),
                                    unmapped_dispensing['RAW_DISPENSE_ROUTE'].alias("RAW_DISPENSE_ROUTE"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_dispensing['DISPENSINGID'].alias("JOIN_FIELD"),
                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
       
        dispensing_with_additional_fileds = cf.append_additional_fields(
            mapped_df = dispensing,
            file_name = "deduplicated_dispensing.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "DISPENSINGID",
            spark = spark)

        cf.write_pyspark_output_file(
                        payspark_df = dispensing_with_additional_fileds,
                        output_file_name = "mapped_dispensing.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'dispensing_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')
