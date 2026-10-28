###################################################################################################################################

# This script will format the hash_token dataset to a PCORnet formatted hash_token table.

###################################################################################################################################

import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from datetime import date, datetime
from pyspark.sql.functions import udf, lit
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

    # Loading the hash_token dataset to be formatted to the PCORnet hash_token table.

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

    hash_token_table_name       = 'hash_token*.csv'


    # Ancillary export: drop a timestamped Datavant match-ready file into the
    # shared inbox (DATAVANT_MATCH_INBOX_DIR) for downstream match-ID generation.
    # This runs both '--from <DATAVANT_KEY>' and '--to datavant' transforms and
    # never raises on failure; the PCORnet pipeline continues either way.

    unformatted_hash_token = spark.read.load(input_data_folder_path+hash_token_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    ###################################################################################################################################
    # Declare functions if needed here.
    ###################################################################################################################################


    ###################################################################################################################################

    # Converting the fields to PCORNet hash_token format.

    ###################################################################################################################################

    pcornet_hash_token = unformatted_hash_token.select(      
                                                    unformatted_hash_token['person_id'].alias("PATID"),
                                                    unformatted_hash_token['Token_1'].alias("TOKEN_01"),
                                                    unformatted_hash_token['Token_2'].alias("TOKEN_02"),
                                                    unformatted_hash_token['Token_3'].alias("TOKEN_03"),
                                                    unformatted_hash_token['Token_4'].alias("TOKEN_04"),
                                                    unformatted_hash_token['Token_5'].alias("TOKEN_05"),
                                                    unformatted_hash_token['Token_6'].alias("TOKEN_06"),
                                                    unformatted_hash_token['Token_7'].alias("TOKEN_07"),
                                                    unformatted_hash_token['Token_8'].alias("TOKEN_08"),
                                                    unformatted_hash_token['Token_9'].alias("TOKEN_09"),
                                                    unformatted_hash_token['Token_12'].alias("TOKEN_12"),
                                                    unformatted_hash_token['Token_14'].alias("TOKEN_14"),
                                                    unformatted_hash_token['Token_15'].alias("TOKEN_15"),
                                                    unformatted_hash_token['Token_16'].alias("TOKEN_16"),
                                                    unformatted_hash_token['Token_17'].alias("TOKEN_17"),
                                                    unformatted_hash_token['Token_18'].alias("TOKEN_18"),
                                                    unformatted_hash_token['Token_23'].alias("TOKEN_23"),
                                                    unformatted_hash_token['Token_24'].alias("TOKEN_24"),
                                                    unformatted_hash_token['Token_25'].alias("TOKEN_25"),
                                                    unformatted_hash_token['Token_26'].alias("TOKEN_26"),
                                                    unformatted_hash_token['Token_29'].alias("TOKEN_29"),
                                                    unformatted_hash_token['Token_30'].alias("TOKEN_30"),
                                                    unformatted_hash_token['Token_101'].alias("TOKEN_101"),
                                                    unformatted_hash_token['Token_102'].alias("TOKEN_102"),
                                                    unformatted_hash_token['Token_103'].alias("TOKEN_103"),
                                                    unformatted_hash_token['Token_104'].alias("TOKEN_104"),
                                                    unformatted_hash_token['Token_105'].alias("TOKEN_105"),
                                                    unformatted_hash_token['Token_106'].alias("TOKEN_106"),
                                                    unformatted_hash_token['Token_107'].alias("TOKEN_107"),
                                                    unformatted_hash_token['Token_108'].alias("TOKEN_108"),
                                                    unformatted_hash_token['Token_109'].alias("TOKEN_109"),
                                                    unformatted_hash_token['Token_110'].alias("TOKEN_110"),
                                                    unformatted_hash_token['Token_111'].alias("TOKEN_111"),           
                                                    unformatted_hash_token['token_encryption_key'].alias("TOKEN_ENCRYPTION_KEY"),                                                  
                                                )
    ###################################################################################################################################

    # Create the output file.

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = pcornet_hash_token,
                        output_file_name = "formatted_original_hash_token.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    # Once the script has finished, time to shutdown the resources used to run the PySpark Cluster.
    spark.stop()
    
except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'original_hash_token_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')    
    










