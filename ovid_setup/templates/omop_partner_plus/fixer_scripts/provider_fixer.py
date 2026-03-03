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


def apply_fix(fix_type, fix_name, version, input_path, output_path,fixed_table_name, src1, src2):
    print(fix_type)

    if fix_type == 'custom':
        partner_fix_path = f"partners.{partner_name.lower()}.custom_fixes.{fix_name}.{version}"
        partner_diction = importlib.import_module(partner_fix_path)


    if fix_type == 'common':

        print('common')
        # cf.print_run_status(0, 0, f'{table}_deduplicator.py', os.path.split(input_folder)[1], partner)

        partner_fix_path = f"/app/common/common_fixes/{fix_name}/{version}.py"

        command = ["python", partner_fix_path, '-f', input_data_folder, '-t', mapped_table_name , '-p', partner_name,'-ftm',fixed_table_name, '-i',input_path,'-o',output_path, '-sr1', src1, '-sr2', src2]

        subprocess.run(" ".join(command), shell=True)



partner_name = 'omop_partner_plus'
mapped_table_name ='mapped_provider.csv'
fixed_table_name = 'fixed_provider.csv'

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
                            job     = 'provider_fixer.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')








