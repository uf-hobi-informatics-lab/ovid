###################################################################################################################################
# This script will map a PCORNet pat_relationship table 
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

input_data_folder = args.data_folder


cf =CommonFuncitons(input_partner)

# spin the pyspak cluster and
spark = cf.get_spark_session("pat_relationship_mapper")


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


        unmapped_pat_relationship    = cf.spark_read(deduplicated_data_folder_path+"deduplicated_pat_relationship.csv", spark)



    ###################################################################################################################################
    # create the mapping from the dictionaries
    ###################################################################################################################################
        mapping_pat_relationship_type_dict = create_map([lit(x) for x in chain(*partner_dictionaries.pat_relationship_relationship_type_dict.items())])



    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        pat_relationship = unmapped_pat_relationship.select(              
            
            
                                    cf.encrypt_id_udf(unmapped_pat_relationship['PATID_1']).alias("PATID_1"),
                                    cf.encrypt_id_udf(unmapped_pat_relationship['PATID_2']).alias("PATID_2"),
                                    coalesce(mapping_pat_relationship_type_dict[upper(col("RELATIONSHIP_TYPE"))],col('RELATIONSHIP_TYPE')).alias("RELATIONSHIP_TYPE"),
                                    unmapped_pat_relationship['RELATIONSHIP_START'].alias("RELATIONSHIP_START"),
                                    unmapped_pat_relationship['RELATIONSHIP_END'].alias("RELATIONSHIP_END"),                         
                                    cf.get_current_time_udf().alias("UPDATED"),
                                    lit(input_partner.upper()).alias("SOURCE"),
                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################


        cf.write_pyspark_output_file(
                        payspark_df = pat_relationship,
                        output_file_name = "mapped_pat_relationship.csv",
                        output_data_folder_path= mapped_data_folder_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner,
                            job     = 'pat_relationship_mapper.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')