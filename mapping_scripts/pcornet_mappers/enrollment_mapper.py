###################################################################################################################################
# This script will map a PCORNet enrollment table 
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

spark = cf.get_spark_session("enrollment_mapper")

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

        unmapped_enrollment    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_enrollment.csv", spark)



    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_chart_dict = create_map([lit(x) for x in chain(*partner_dictionaries.enrollment_chart_dict.items())])
        mapping_enr_basis_dict = create_map([lit(x) for x in chain(*partner_dictionaries.enrollment_enr_basis_dict.items())])
        


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        enrollment = unmapped_enrollment.select(              
            
                                    cf.encrypt_id_udf(unmapped_enrollment['PATID']).alias("PATID"),
                                    unmapped_enrollment['ENR_START_DATE'].alias("ENR_START_DATE"),
                                    unmapped_enrollment['ENR_END_DATE'].alias("ENR_END_DATE"),
                                    coalesce(mapping_chart_dict[upper(col('CHART'))],col('CHART')).alias("CHART"),
                                    coalesce(mapping_enr_basis_dict[upper(col('ENR_BASIS'))],col('ENR_BASIS')).alias("ENR_BASIS"),
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        cf.write_pyspark_output_file(
                        payspark_df = enrollment,
                        output_file_name = "mapped_enrollment.csv",
                        output_data_folder_path= mapped_data_folder_path)



        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'enrollment_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')
