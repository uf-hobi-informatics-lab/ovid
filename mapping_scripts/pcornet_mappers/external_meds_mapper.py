###################################################################################################################################
# This script will map a PCORNet external_meds table 
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
spark = cf.get_spark_session("external_meds_mapper")
 
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

        deduplicated_data_folder_path = '/app/partners/' + input_partner.lower() + '/data/' + input_data_folder + '/deduplicator_output/' 
        mapped_data_folder_path    = '/app/partners/'+input_partner.lower()+'/data/' + input_data_folder + '/mapper_output/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        unmapped_external_meds    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_external_meds.csv", spark)


    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_ext_dose_ordered_unit_dict = create_map([lit(x) for x in chain(*partner_dictionaries.external_meds_ext_dose_ordered_unit_dict.items())])
        mapping_ext_dose_form_dict = create_map([lit(x) for x in chain(*partner_dictionaries.external_meds_ext_dose_form_dict.items())])
        mapping_ext_route_dict = create_map([lit(x) for x in chain(*partner_dictionaries.external_meds_ext_route_dict.items())])
        mapping_ext_basis_dict = create_map([lit(x) for x in chain(*partner_dictionaries.external_meds_ext_basis_dict.items())])
        mapping_extmed_source_dict = create_map([lit(x) for x in chain(*partner_dictionaries.external_meds_extmed_source_dict.items())])






    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        external_meds = unmapped_external_meds.select(              
            
            
                                    cf.encrypt_id_udf(unmapped_external_meds['EXTMEDID']).alias("EXTMEDID"),
                                    cf.encrypt_id_udf(unmapped_external_meds['PATID']).alias("PATID"),
                                    unmapped_external_meds['EXT_RECORD_DATE'].alias("EXT_RECORD_DATE"),
                                    unmapped_external_meds['EXT_PAT_START_DATE'].alias("EXT_PAT_START_DATE"),
                                    unmapped_external_meds['EXT_END_DATE'].alias("EXT_END_DATE"),
                                    unmapped_external_meds['EXT_PAT_END_DATE'].alias("EXT_PAT_END_DATE"),
                                    unmapped_external_meds['EXT_DOSE'].alias("EXT_DOSE"),
                                    coalesce(mapping_ext_dose_ordered_unit_dict[col("EXT_DOSE_ORDERED_UNIT")],col('EXT_DOSE_ORDERED_UNIT')).alias("EXT_DOSE_ORDERED_UNIT"),
                                    coalesce(mapping_ext_dose_form_dict[upper(col("EXT_DOSE_FORM"))],col('EXT_DOSE_FORM')).alias("EXT_DOSE_FORM"),
                                    coalesce(mapping_ext_route_dict[upper(col("EXT_ROUTE"))],col('EXT_ROUTE')).alias("EXT_ROUTE"),
                                    coalesce(mapping_ext_basis_dict[upper(col("EXT_BASIS"))],col('EXT_BASIS')).alias("EXT_BASIS"),
                                    unmapped_external_meds['RXNORM_CUI'].alias("RXNORM_CUI"),
                                    coalesce(mapping_extmed_source_dict[upper(col("EXTMED_SOURCE"))],col('EXTMED_SOURCE')).alias("EXTMED_SOURCE"),
                                    unmapped_external_meds['RAW_EXT_MED_NAME'].alias("RAW_EXT_MED_NAME"),
                                    unmapped_external_meds['RAW_RXNORM_CUI'].alias("RAW_RXNORM_CUI"),
                                    unmapped_external_meds['RAW_EXT_NDC'].alias("RAW_EXT_NDC"),
                                    unmapped_external_meds['RAW_EXT_DOSE'].alias("RAW_EXT_DOSE"),
                                    unmapped_external_meds['RAW_EXT_DOSE_UNIT'].alias("RAW_EXT_DOSE_UNIT"),
                                    unmapped_external_meds['RAW_EXT_ROUTE'].alias("RAW_EXT_ROUTE"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                    unmapped_external_meds['EXTMEDID'].alias("JOIN_FIELD"),


                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        external_meds_with_additional_fileds = cf.append_additional_fields(
            mapped_df = external_meds,
            file_name = "deduplicated_external_meds.csv",
            deduplicated_data_folder_path = deduplicated_data_folder_path,
            join_field = "EXTMEDID",
            spark = spark)


        cf.write_pyspark_output_file(
                        payspark_df = external_meds_with_additional_fileds,
                        output_file_name = "mapped_external_meds.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'external_meds_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')