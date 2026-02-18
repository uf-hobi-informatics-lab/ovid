###################################################################################################################################

#  

###################################################################################################################################


import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse
import os





partner_name = 'pcornet_partner_1'
mapped_table_name ='mapped_hash_token.csv'
fixed_table_name = 'fixed_hash_token.csv'

cf =CommonFuncitons(partner_name.upper())

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
args = parser.parse_args()
input_data_folder = args.data_folder

#Create SparkSession



fixes = {

    # "1": {"fix_type":"common",    "fix_name":"patid_orphans_fix",   "version":"1.0", "src1":"mapped_demographic.csv", "src2":"NotUsed"},
    # "2": {"fix_type":"custom",    "fix_name":"patid_orphans",   "version":"2.0"},
    # "3": {"fix_type":"custom",    "fix_name":"patid_orphans",   "version":"1.0"},



}

try: 

    mapper_data_folder_path           = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/mapper_output/'
    fixer_output_data_folder_path     = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/fixer_output/'


    if  os.path.exists(mapper_data_folder_path+mapped_table_name):


        cf.copy_data(mapped_table_name,fixed_table_name, mapper_data_folder_path,fixer_output_data_folder_path  )


        for fix  in fixes:

                cf.apply_fix(
                
                        fix_type = fixes[fix]["fix_type"],
                        fix_name = fixes[fix]["fix_name"],
                        version  = fixes[fix]["version"],
                        input_path  = mapper_data_folder_path,
                        output_path = fixer_output_data_folder_path,
                        fixed_table_name = fixed_table_name,
                        input_data_folder= input_data_folder,
                        partner_name= partner_name,
                        mapped_table_name=mapped_table_name,
                        src1= fixes[fix]['src1'],
                        src2= fixes[fix]['src2'],
                        
                        )

    else:

        message = f"{mapper_data_folder_path}{mapped_table_name}: No such file or directory"

        cf.print_with_style(message, 'warning orange')

except Exception as e:

    
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'hash_token_fixer.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')








