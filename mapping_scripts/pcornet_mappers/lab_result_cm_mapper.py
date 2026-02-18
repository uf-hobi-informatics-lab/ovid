###################################################################################################################################
# This script will map a PCORNet lab_result_cm table 
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

spark = cf.get_spark_session("lab_result_cm_mapper")

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

 
        unmapped_lab_result_cm    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_lab_result_cm.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
       
        mapping_specimen_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_specimen_source_dict.items())])
        mapping_lab_result_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_lab_result_source_dict.items())])
        mapping_priority_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_priority_dict.items())])
        mapping_result_loc_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_result_loc_dict.items())])
        mapping_lab_px_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_lab_px_type_dict.items())])
        mapping_result_qual_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_result_qual_dict.items())])
        mapping_result_modifier_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_result_modifier_dict.items())])
        mapping_result_modifier_low_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_norm_modifier_low_dict.items())])
        mapping_result_modifier_high_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_norm_modifier_high_dict.items())])
        mapping_result_unit_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_result_unit_dict.items())])
        mapping_abn_ind_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_abn_ind_dict.items())])
        mapping_lab_loinc_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.lab_result_cm_lab_loinc_source_dict.items())])


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        lab_result_cm = unmapped_lab_result_cm.select(              
            
            

 
                                    cf.encrypt_id_udf(concat(unmapped_lab_result_cm['LAB_RESULT_CM_ID'],lit('LAB_RESULT_CM'))).alias("LAB_RESULT_CM_ID"),
                                    cf.encrypt_id_udf(unmapped_lab_result_cm['PATID']).alias("PATID"),
                                    cf.encrypt_id_udf(unmapped_lab_result_cm['ENCOUNTERID']).alias("ENCOUNTERID"),
                                    coalesce(mapping_specimen_source_dict[upper(col("SPECIMEN_SOURCE"))],col('SPECIMEN_SOURCE')).alias("SPECIMEN_SOURCE"),
                                    unmapped_lab_result_cm['LAB_LOINC'].alias("LAB_LOINC"),
                                    coalesce(mapping_lab_result_source_dict[upper(col("LAB_RESULT_SOURCE"))],col('LAB_RESULT_SOURCE')).alias("LAB_RESULT_SOURCE"),
                                    coalesce(mapping_lab_loinc_source_dict[upper(col("LAB_LOINC_SOURCE"))],col('LAB_LOINC_SOURCE')).alias("LAB_LOINC_SOURCE"),
                                    coalesce(mapping_priority_dict[upper(col("PRIORITY"))],col('PRIORITY')).alias("PRIORITY"),
                                    coalesce(mapping_result_loc_dict[upper(col("RESULT_LOC"))],col('RESULT_LOC')).alias("RESULT_LOC"),
                                    unmapped_lab_result_cm['LAB_PX'].alias("LAB_PX"),
                                    coalesce(mapping_lab_px_type_dict[upper(col("LAB_PX_TYPE"))],col('LAB_PX_TYPE')).alias("LAB_PX_TYPE"),
                                    unmapped_lab_result_cm['LAB_ORDER_DATE'].alias("LAB_ORDER_DATE"),
                                    unmapped_lab_result_cm['SPECIMEN_DATE'].alias("SPECIMEN_DATE"),
                                    unmapped_lab_result_cm['SPECIMEN_TIME'].alias("SPECIMEN_TIME"),
                                    unmapped_lab_result_cm['RESULT_DATE'].alias("RESULT_DATE"),
                                    unmapped_lab_result_cm['RESULT_TIME'].alias("RESULT_TIME"),
                                    coalesce(mapping_result_qual_dict[upper(col("RESULT_QUAL"))],col('RESULT_QUAL')).alias("RESULT_QUAL"),
                                    unmapped_lab_result_cm['RESULT_SNOMED'].alias("RESULT_SNOMED"),
                                    unmapped_lab_result_cm['RESULT_NUM'].alias("RESULT_NUM"),
                                    coalesce(mapping_result_modifier_dict[upper(col("RESULT_MODIFIER"))],col('RESULT_MODIFIER')).alias("RESULT_MODIFIER"),
                                    coalesce(mapping_result_unit_dict[col("RESULT_UNIT")],col('RESULT_UNIT')).alias("RESULT_UNIT"),
                                    unmapped_lab_result_cm['NORM_RANGE_LOW'].alias("NORM_RANGE_LOW"),
                                    coalesce(mapping_result_modifier_low_dict[upper(col("NORM_MODIFIER_LOW"))],col('NORM_MODIFIER_LOW')).alias("NORM_MODIFIER_LOW"),
                                    unmapped_lab_result_cm['NORM_RANGE_HIGH'].alias("NORM_RANGE_HIGH"),
                                    coalesce(mapping_result_modifier_high_dict[upper(col("NORM_MODIFIER_HIGH"))],col('NORM_MODIFIER_HIGH')).alias("NORM_MODIFIER_HIGH"),                               
                                    coalesce(mapping_abn_ind_dict[upper(col("ABN_IND"))],col('ABN_IND')).alias("ABN_IND"),
                                    unmapped_lab_result_cm['RAW_LAB_NAME'].alias("RAW_LAB_NAME"),
                                    unmapped_lab_result_cm['RAW_LAB_CODE'].alias("RAW_LAB_CODE"),
                                    unmapped_lab_result_cm['RAW_PANEL'].alias("RAW_PANEL"),
                                    unmapped_lab_result_cm['RAW_RESULT'].alias("RAW_RESULT"),
                                    unmapped_lab_result_cm['RAW_UNIT'].alias("RAW_UNIT"),
                                    unmapped_lab_result_cm['RAW_ORDER_DEPT'].alias("RAW_ORDER_DEPT"),
                                    unmapped_lab_result_cm['RAW_FACILITY_CODE'].alias("RAW_FACILITY_CODE"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_lab_result_cm['LAB_RESULT_CM_ID'].alias("JOIN_FIELD"),
                                    

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        
        lab_result_cm_with_additional_fileds = cf.append_additional_fields(
            mapped_df = lab_result_cm,
            file_name = "deduplicated_lab_result_cm.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "LAB_RESULT_CM_ID",
            spark = spark)

        cf.write_pyspark_output_file(
                        payspark_df = lab_result_cm_with_additional_fileds,
                        output_file_name = "mapped_lab_result_cm.csv",
                        output_data_folder_path= mapped_data_folder_path)


        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'lab_result_cm_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')