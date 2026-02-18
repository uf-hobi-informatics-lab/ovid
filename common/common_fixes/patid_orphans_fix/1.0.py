

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse


cf =CommonFuncitons('NotUsed')


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-t", "--table")
parser.add_argument("-p", "--partner")
parser.add_argument("-ftm", "--fixed_table_name")
parser.add_argument("-i", "--input_path")
parser.add_argument("-o", "--output_path")
parser.add_argument("-sr1", "--src1")
parser.add_argument("-sr2", "--src2")

args = parser.parse_args()
input_data_folder         = args.data_folder
examined_table_name       = args.table
input_partner_name        = args.partner
patid_source_table        = args.src1
patid_examined_field_name = args.src2
input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name
spark = cf.get_spark_session("patid_orphans_fix")

this_fix_name = 'common_patid_orphans_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')


if 'PATID' not in patid_examined_field_name:

    patid_examined_field_name = 'PATID'


try:



    cf.print_fixer_status(

        current_count = '**',
        total_count   ='**',
        fix_type      = 'common', 
        fix_name      = this_fix_name
        
        )

    patid_source_table      = spark.read.load(input_data_folder_path+patid_source_table,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    # patid_source_table.show()

    input_table             = spark.read.load(output_data_folder_path+fixed_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


    # input_table.show()

    existing_patids =  patid_source_table.select( patid_source_table['PATID'].alias('SOURCE_PATID')).distinct()
    patids_to_be_examined =  input_table.select( input_table[patid_examined_field_name].alias('TO_BE_EXAMINED_PATID')).distinct()


    orphan_patid = patids_to_be_examined.subtract(existing_patids)
    # orphan_patid.show()

    # shared_patids = patids_to_be_examined.join(existing_patids, existing_patids['SOURCE_PATID']==patids_to_be_examined['TO_BE_EXAMINED_PATID'], how="inner")

  
  
    
    fixed_table = input_table.join(existing_patids, existing_patids['SOURCE_PATID']==input_table[patid_examined_field_name], how='left_semi')



    cf.write_pyspark_output_file(
                            payspark_df = orphan_patid,
                            output_file_name = f"{examined_table_name.replace('.csv','')}_missing_patids.csv" ,
                            output_data_folder_path= output_data_folder_path+ "/fixers_output/"+examined_table_name_parsed+"_fixer/"+this_fix_name+"/")


    cf.write_pyspark_output_file(
                            payspark_df = fixed_table,
                            output_file_name = fixed_table_name ,
                            output_data_folder_path= output_data_folder_path)











    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner_name.lower(),
                            job     = 'common_patid_orphans_fix_1.0' ,
                            text    = str(e))



