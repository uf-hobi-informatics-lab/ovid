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


   # Loading the omop_hash_token table to be converted to the pcornet_hash_token table
   # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping


   ###################################################################################################################################

   input_data_folder_path               = f'/data/{input_data_folder}/'
   formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

   datavant_hash_token_table_name       = 'hash_token.csv'

   ###################################################################################################################################


   #Converting the fileds to PCORNet pcornet_hash_token Format


   ###################################################################################################################################


   datavant_hash_token = spark.read.load(input_data_folder_path+datavant_hash_token_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


   hash_token = datavant_hash_token.select(                 
           datavant_hash_token['PAT_ID'].alias('PATID'),
           datavant_hash_token['token_1'].alias('TOKEN_01'),
           datavant_hash_token['token_2'].alias('TOKEN_02'),
           datavant_hash_token['token_3'].alias('TOKEN_03'),
           datavant_hash_token['token_4'].alias('TOKEN_04'),
           datavant_hash_token['token_5'].alias('TOKEN_05'),
           datavant_hash_token['token_6'].alias('TOKEN_06'),
           datavant_hash_token['token_7'].alias('TOKEN_07'),
           datavant_hash_token['token_8'].alias('TOKEN_08'),
           datavant_hash_token['token_9'].alias('TOKEN_09'),
           datavant_hash_token['token_12'].alias('TOKEN_12'),
           datavant_hash_token['token_14'].alias('TOKEN_14'),
           datavant_hash_token['token_15'].alias('TOKEN_15'),
           datavant_hash_token['token_16'].alias('TOKEN_16'),
           datavant_hash_token['token_17'].alias('TOKEN_17'),
           datavant_hash_token['token_18'].alias('TOKEN_18'),
           datavant_hash_token['token_23'].alias('TOKEN_23'),
           datavant_hash_token['token_24'].alias('TOKEN_24'),
           datavant_hash_token['token_25'].alias('TOKEN_25'),
           datavant_hash_token['token_26'].alias('TOKEN_26'),
           datavant_hash_token['token_29'].alias('TOKEN_29'),
           datavant_hash_token['token_30'].alias('TOKEN_30'),
           datavant_hash_token['token_101'].alias('TOKEN_101'),
           datavant_hash_token['token_102'].alias('TOKEN_102'),
           datavant_hash_token['token_103'].alias('TOKEN_103'),
           datavant_hash_token['token_104'].alias('TOKEN_104'),
           datavant_hash_token['token_105'].alias('TOKEN_105'),
           datavant_hash_token['token_106'].alias('TOKEN_106'),
           datavant_hash_token['token_107'].alias('TOKEN_107'),
           datavant_hash_token['token_108'].alias('TOKEN_108'),
           datavant_hash_token['token_109'].alias('TOKEN_109'),
           datavant_hash_token['token_110'].alias('TOKEN_110'),
           datavant_hash_token['token_111'].alias('TOKEN_111'),
           datavant_hash_token['token_encryption_key'].alias('TOKEN_ENCRYPTION_KEY'),
           


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


