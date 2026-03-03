###################################################################################################################################
# This script will map a PCORNet prescribing table 
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
spark = cf.get_spark_session("prescribing_mapper")
 
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
    # Loading the unmapped prescribing table
    ###################################################################################################################################


    
        unmapped_prescribing    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_prescribing.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_rx_dose_ordered_unit_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_dose_ordered_unit_dict.items())])
        mapping_rx_dose_form_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_dose_form_dict.items())])
        mapping_rx_prn_flag_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_prn_flag_dict.items())])
        mapping_rx_route_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_route_dict.items())])
        mapping_rx_basis_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_basis_dict.items())])
        mapping_rx_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_source_dict.items())])
        mapping_rx_frequency_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_frequency_dict.items())])
        mapping_rx_dispense_as_written_dict = create_map([lit(x) for x in chain(*partner_dictionaries.prescribing_rx_dispense_as_written_dict.items())])






    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped prescribing table
    ###################################################################################################################################


        prescribing = unmapped_prescribing.select(              
            
            
                                    cf.encrypt_id_udf(unmapped_prescribing['PRESCRIBINGID']).alias("PRESCRIBINGID"),
                                    cf.encrypt_id_udf(unmapped_prescribing['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_prescribing['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    cf.encrypt_id_udf(unmapped_prescribing['RX_PROVIDERID']).alias("RX_PROVIDERID"),                             
                                    unmapped_prescribing['RX_ORDER_DATE'].alias("RX_ORDER_DATE"),
                                    unmapped_prescribing['RX_ORDER_TIME'].alias("RX_ORDER_TIME"),                              
                                    unmapped_prescribing['RX_START_DATE'].alias("RX_START_DATE"),
                                    unmapped_prescribing['RX_END_DATE'].alias("RX_END_DATE"), 
                                    unmapped_prescribing['RX_DOSE_ORDERED'].alias("RX_DOSE_ORDERED"),
                                    coalesce(mapping_rx_dose_ordered_unit_dict[col("RX_DOSE_ORDERED_UNIT")], col("RX_DOSE_ORDERED_UNIT")).alias("RX_DOSE_ORDERED_UNIT"),
                                    unmapped_prescribing['RX_QUANTITY'].alias("RX_QUANTITY"),
                                    coalesce(mapping_rx_dose_form_dict[upper(col("RX_DOSE_FORM"))],col('RX_DOSE_FORM')).alias("RX_DOSE_FORM"),
                                    unmapped_prescribing['RX_REFILLS'].alias("RX_REFILLS"),
                                    unmapped_prescribing['RX_DAYS_SUPPLY'].alias("RX_DAYS_SUPPLY"),
                                    coalesce(mapping_rx_frequency_dict[upper(col("RX_FREQUENCY"))],col('RX_FREQUENCY')).alias("RX_FREQUENCY"),
                                    coalesce(mapping_rx_prn_flag_dict[upper(col("RX_PRN_FLAG"))],col('RX_PRN_FLAG')).alias("RX_PRN_FLAG"),
                                    coalesce(mapping_rx_route_dict[upper(col("RX_ROUTE"))],col('RX_ROUTE')).alias("RX_ROUTE"),
                                    coalesce(mapping_rx_basis_dict[upper(col("RX_BASIS"))],col('RX_BASIS')).alias("RX_BASIS"),
                                    unmapped_prescribing['RXNORM_CUI'].alias("RXNORM_CUI"),
                                    coalesce(mapping_rx_source_dict[upper(col("RX_SOURCE"))],col('RX_SOURCE')).alias("RX_SOURCE"),
                                    coalesce(mapping_rx_dispense_as_written_dict[upper(col("RX_DISPENSE_AS_WRITTEN"))],col('RX_DISPENSE_AS_WRITTEN')).alias("RX_DISPENSE_AS_WRITTEN"),
                                    unmapped_prescribing['RAW_RX_MED_NAME'].alias("RAW_RX_MED_NAME"),
                                    unmapped_prescribing['RAW_RX_FREQUENCY'].alias("RAW_RX_FREQUENCY"),
                                    unmapped_prescribing['RAW_RXNORM_CUI'].alias("RAW_RXNORM_CUI"),
                                    unmapped_prescribing['RAW_RX_QUANTITY'].alias("RAW_RX_QUANTITY"),
                                    unmapped_prescribing['RAW_RX_NDC'].alias("RAW_RX_NDC"),
                                    unmapped_prescribing['RAW_RX_DOSE_ORDERED'].alias("RAW_RX_DOSE_ORDERED"),
                                    unmapped_prescribing['RAW_RX_DOSE_ORDERED_UNIT'].alias("RAW_RX_DOSE_ORDERED_UNIT"),
                                    unmapped_prescribing['RAW_RX_ROUTE'].alias("RAW_RX_ROUTE"),
                                    unmapped_prescribing['RAW_RX_REFILLS'].alias("RAW_RX_REFILLS"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_prescribing['PRESCRIBINGID'].alias("JOIN_FIELD"),
                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        prescribing_with_additional_fileds = cf.append_additional_fields(
            mapped_df = prescribing,
            file_name = "deduplicated_prescribing.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "PRESCRIBINGID",
            spark = spark)

        cf.write_pyspark_output_file(
                        payspark_df = prescribing_with_additional_fileds,
                        output_file_name = "mapped_prescribing.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'prescribing_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')