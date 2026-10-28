###################################################################################################################################

# This script will convert an OMOP external_meds table to a PCORnet format as the external_meds table

###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-p", "--partner_name")

args = parser.parse_args()
input_data_folder = args.data_folder
partner_name = args.partner_name
cf =CommonFuncitons(partner_name)
# Create SparkSession
spark = cf.get_spark_session("ovid")


try: 


    ###################################################################################################################################

    # Loading the external_meds table to be converted to the external_meds table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'


    external_meds_table_name   = 'external_meds.csv'

    external_meds = spark.read.load(input_data_folder_path+external_meds_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


  

    
    ###################################################################################################################################

    #Converting the fields to PCORNet external meds Format

    ###################################################################################################################################

    external_meds = external_meds.select(           external_meds['externalmedid'].alias("EXTMEDID"),
                                                    external_meds['patid'].alias("PATID"),
                                                    external_meds['ext_record_date'].alias("EXT_RECORD_DATE"),
                                                    external_meds['ext_pat_start_date'].alias("EXT_PAT_START_DATE"),
                                                    external_meds['ext_end_date'].alias("EXT_END_DATE"),
                                                    external_meds['ext_pat_end_date'].alias("EXT_PAT_END_DATE "),
                                                    external_meds['ext_dose'].alias("EXT_DOSE"),
                                                    external_meds['ext_dose_ordered_unit'].alias("EXT_DOSE_ORDERED_UNIT"),
                                                    external_meds['ext_dose_form'].alias("EXT_DOSE_FORM"),
                                                    external_meds['ext_route'].alias("EXT_ROUTE"),
                                                    external_meds['ext_basis'].alias("EXT_BASIS"),
                                                    external_meds['rxnorm_cui'].alias("RXNORM_CUI"),
                                                    external_meds['extmed_source'].alias("EXTMED_SOURCE"),
                                                    external_meds['raw_ext_med_name'].alias("RAW_EXT_MED_NAME"),
                                                    external_meds['raw_rxnorm_cui'].alias("RAW_RXNORM_CUI"),
                                                    external_meds['raw_ext_ndc'].alias("RAW_EXT_NDC"),
                                                    external_meds['raw_ext_dose'].alias("RAW_EXT_DOSE"),
                                                    external_meds['raw_ext_dose_unit'].alias("RAW_EXT_DOSE_UNIT"),
                                                    external_meds['raw_ext_route'].alias("RAW_EXT_ROUTE"),
                                                    

                                                    
                                                    
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = external_meds,
                        output_file_name = "formatted_external_meds.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'external_meds_formatter.py' ,
                            text    = str(e))





