###################################################################################################################################

# This script will take in the PCORnet formatted raw HASH_TOKEN file, do the necessary transformations, and output the formatted PCORnet HASH_TOKEN file

###################################################################################################################################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse


###################################################################################################################################
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-p", "--partner_name")

args = parser.parse_args()
input_data_folder = args.data_folder
partner_name = args.partner_name
cf =CommonFuncitons(partner_name)
# Create SparkSession
spark = cf.get_spark_session("ovid")
###################################################################################################################################


try:

    ###################################################################################################################################

    # Loading the raw hash_token table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    # input_data_folder_path               = f'/app/partners/pcornet_partner_1/data/input/{input_data_folder}/'

    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    hash_token_table_name   = '*Hash_Token*'


    ###################################################################################################################################

    #Converting the fields to PCORNet hash_token Format

    ###################################################################################################################################

    try:

        hash_token_in = spark.read.load(input_data_folder_path+hash_token_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        hash_token = hash_token_in.select(

                                hash_token_in['patid'].alias('PATID'),
                                hash_token_in['token_01'].alias('TOKEN_01'),
                                hash_token_in['token_02'].alias('TOKEN_02'),
                                hash_token_in['token_03'].alias('TOKEN_03'),
                                hash_token_in['token_04'].alias('TOKEN_04'),
                                hash_token_in['token_05'].alias('TOKEN_05'),
                                hash_token_in['token_06'].alias('TOKEN_06'),
                                hash_token_in['token_07'].alias('TOKEN_07'),
                                hash_token_in['token_08'].alias('TOKEN_08'),
                                hash_token_in['token_09'].alias('TOKEN_09'),
                                hash_token_in['token_12'].alias('TOKEN_12'),
                                hash_token_in['token_14'].alias('TOKEN_14'),
                                hash_token_in['token_15'].alias('TOKEN_15'),
                                hash_token_in['token_16'].alias('TOKEN_16'),
                                hash_token_in['token_17'].alias('TOKEN_17'),
                                hash_token_in['token_18'].alias('TOKEN_18'),
                                hash_token_in['token_23'].alias('TOKEN_23'),
                                hash_token_in['token_24'].alias('TOKEN_24'),
                                hash_token_in['token_25'].alias('TOKEN_25'),
                                hash_token_in['token_26'].alias('TOKEN_26'),
                                hash_token_in['token_29'].alias('TOKEN_29'),
                                hash_token_in['token_30'].alias('TOKEN_30'),
                                hash_token_in['token_101'].alias('TOKEN_101'),
                                hash_token_in['token_102'].alias('TOKEN_102'),
                                hash_token_in['token_103'].alias('TOKEN_103'),
                                hash_token_in['token_104'].alias('TOKEN_104'),
                                hash_token_in['token_105'].alias('TOKEN_105'),
                                hash_token_in['token_106'].alias('TOKEN_106'),
                                hash_token_in['token_107'].alias('TOKEN_107'),
                                hash_token_in['token_108'].alias('TOKEN_108'),
                                hash_token_in['token_109'].alias('TOKEN_109'),
                                hash_token_in['token_110'].alias('TOKEN_110'),
                                hash_token_in['token_111'].alias('TOKEN_111'),
                                hash_token_in['token_encryption_key'].alias('TOKEN_ENCRYPTION_KEY'),

        )
    except:
        hash_token_in = spark.read.load(input_data_folder_path+hash_token_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        hash_token = hash_token_in.select(

                                hash_token_in['PATID'].alias('PATID'),
                                hash_token_in['TOKEN_01'].alias('TOKEN_01'),
                                hash_token_in['TOKEN_02'].alias('TOKEN_02'),
                                hash_token_in['TOKEN_03'].alias('TOKEN_03'),
                                hash_token_in['TOKEN_04'].alias('TOKEN_04'),
                                hash_token_in['TOKEN_05'].alias('TOKEN_05'),
                                hash_token_in['TOKEN_06'].alias('TOKEN_06'),
                                hash_token_in['TOKEN_07'].alias('TOKEN_07'),
                                hash_token_in['TOKEN_08'].alias('TOKEN_08'),
                                hash_token_in['TOKEN_09'].alias('TOKEN_09'),
                                hash_token_in['TOKEN_12'].alias('TOKEN_12'),
                                hash_token_in['TOKEN_14'].alias('TOKEN_14'),
                                hash_token_in['TOKEN_15'].alias('TOKEN_15'),
                                hash_token_in['TOKEN_16'].alias('TOKEN_16'),
                                hash_token_in['TOKEN_17'].alias('TOKEN_17'),
                                hash_token_in['TOKEN_18'].alias('TOKEN_18'),
                                hash_token_in['TOKEN_23'].alias('TOKEN_23'),
                                hash_token_in['TOKEN_24'].alias('TOKEN_24'),
                                hash_token_in['TOKEN_25'].alias('TOKEN_25'),
                                hash_token_in['TOKEN_26'].alias('TOKEN_26'),
                                hash_token_in['TOKEN_29'].alias('TOKEN_29'),
                                hash_token_in['TOKEN_30'].alias('TOKEN_30'),
                                hash_token_in['TOKEN_101'].alias('TOKEN_101'),
                                hash_token_in['TOKEN_102'].alias('TOKEN_102'),
                                hash_token_in['TOKEN_103'].alias('TOKEN_103'),
                                hash_token_in['TOKEN_104'].alias('TOKEN_104'),
                                hash_token_in['TOKEN_105'].alias('TOKEN_105'),
                                hash_token_in['TOKEN_106'].alias('TOKEN_106'),
                                hash_token_in['TOKEN_107'].alias('TOKEN_107'),
                                hash_token_in['TOKEN_108'].alias('TOKEN_108'),
                                hash_token_in['TOKEN_109'].alias('TOKEN_109'),
                                hash_token_in['TOKEN_110'].alias('TOKEN_110'),
                                hash_token_in['TOKEN_111'].alias('TOKEN_111'),
                                hash_token_in['TOKEN_ENCRYPTION_KEY'].alias('TOKEN_ENCRYPTION_KEY'),

        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = hash_token,
                        output_file_name = "formatted_hash_token.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'hash_token_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')
