###################################################################################################################################

# This script will take in the PCORnet formatted raw PROCEDURES file, do the necessary transformations, and output the formatted PCORnet PROCEDURES file

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

def fh_ppx(raw_ppx):

    """
    Mapping for PPX - Per Duke 12/11/2019
    0 -                           UN=Unknown
    1 -                           P=Principal;
    2 -                           S=Secondary
    3-999                    OT=Other
    """
    ppx = None
    if raw_ppx == '0': ppx = 'S'
    elif raw_ppx == '1': ppx = 'P'
    elif raw_ppx == '2': ppx = 'S'
    elif raw_ppx >= '3': ppx = 'OT'
    else: ppx = 'UN'
    



    return ppx


fh_ppx_udf = udf(fh_ppx, StringType())





try: 

    ###################################################################################################################################

    # Loading the procedure table to be converted to the procedures table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'



    procedures_table_name   = '*Procedure*'



    ###################################################################################################################################

    #Converting the fileds to PCORNet procedures Format

    ###################################################################################################################################

    try:

        procedures_in = spark.read.load(input_data_folder_path+procedures_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        procedures = procedures_in.select(



                        procedures_in['proceduresid'].alias('PROCEDURESID'),
                        procedures_in['patid'].alias('PATID'),
                        procedures_in['encounterid'].alias('ENCOUNTERID'),
                        procedures_in['enc_type'].alias('ENC_TYPE'),
                        cf.format_date_udf(procedures_in['admit_date']).alias('ADMIT_DATE'),
                        procedures_in['providerid'].alias('PROVIDERID'),
                        cf.format_date_udf(procedures_in['px_date']).alias('PX_DATE'),
                        procedures_in['raw_px'].alias('PX'),
                        procedures_in['raw_px_type'].alias('PX_TYPE'),
                        lit('OD').alias('PX_SOURCE'),
                        fh_ppx_udf(procedures_in['raw_ppx']).alias('PPX'),               
                        lit("").alias('RENDERING_PROVIDERID'),
                        procedures_in['raw_px'].alias('RAW_PX'),
                        procedures_in['raw_px_type'].alias('RAW_PX_TYPE'),
                        procedures_in['raw_ppx'].alias('RAW_PPX'),

        )
    except:
        procedures_in = spark.read.load(input_data_folder_path+procedures_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
        procedures = procedures_in.select(



                        procedures_in['PROCEDURESID'].alias('PROCEDURESID'),
                        procedures_in['PATID'].alias('PATID'),
                        procedures_in['ENCOUNTERID'].alias('ENCOUNTERID'),
                        procedures_in['ENC_TYPE'].alias('ENC_TYPE'),
                        cf.format_date_udf(procedures_in['ADMIT_DATE']).alias('ADMIT_DATE'),
                        procedures_in['PROVIDERID'].alias('PROVIDERID'),
                        cf.format_date_udf(procedures_in['PX_DATE']).alias('PX_DATE'),
                        procedures_in['PX'].alias('PX'),
                        procedures_in['RAW_PX_TYPE'].alias('PX_TYPE'),
                        lit('OD').alias('PX_SOURCE'),
                        fh_ppx_udf(procedures_in['RAW_PPX']).alias('PPX'),               
                        lit("").alias('RENDERING_PROVIDERID'),
                        procedures_in['RAW_PX'].alias('RAW_PX'),
                        procedures_in['RAW_PX_TYPE'].alias('RAW_PX_TYPE'),
                        procedures_in['RAW_PPX'].alias('RAW_PPX'),

        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf =CommonFuncitons('UFH')
    cf.write_pyspark_output_file(
                        payspark_df = procedures,
                        output_file_name = "formatted_procedures.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'procedures_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')




