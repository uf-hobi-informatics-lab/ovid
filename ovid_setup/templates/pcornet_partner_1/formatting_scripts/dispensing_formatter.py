###################################################################################################################################

# This script will take in the PCORnet formatted raw DISPENSING file, do the necessary transformations, and output the formatted PCORnet DISPENSING file

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

def format_result_num_fh( val):
    try:
        float(val)
        return val
    except:
        #Not a float
        return None
    

format_result_num_fh_udf = udf(format_result_num_fh, StringType())




try:

    ###################################################################################################################################

    # Loading the raw dispensing table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    # input_data_folder_path               = f'/app/partners/pcornet_partner_1/data/input/{input_data_folder}/'

    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    dispensing_table_name   = '*Dispensing*'


    ###################################################################################################################################

    #Converting the fields to PCORNet dispensing Format

    ###################################################################################################################################

    try:

        dispensing_in = spark.read.load(input_data_folder_path+dispensing_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        dispensing = dispensing_in.select(


                                dispensing_in['dispensingid'].alias('DISPENSINGID'),
                                dispensing_in['patid'].alias('PATID'),
                                dispensing_in['prescribingid'].alias('PRESCRIBINGID'),
                                cf.format_date_udf(dispensing_in['dispense_date']).alias('DISPENSE_DATE'),
                                dispensing_in['ndc'].alias('NDC'),
                                dispensing_in['dispense_source'].alias('DISPENSE_SOURCE'),
                                format_result_num_fh_udf(dispensing_in['dispense_sup']).alias('DISPENSE_SUP'),
                                format_result_num_fh_udf(dispensing_in['dispense_amt']).alias('DISPENSE_AMT'),
                                format_result_num_fh_udf(dispensing_in['dispense_dose_disp']).alias('DISPENSE_DOSE_DISP'),
                                dispensing_in['dispense_dose_disp_unit'].alias('DISPENSE_DOSE_DISP_UNIT'),
                                dispensing_in['dispense_route'].alias('DISPENSE_ROUTE'),
                                dispensing_in['raw_ndc'].alias('RAW_NDC'),
                                dispensing_in['raw_dispense_dose_disp'].alias('RAW_DISPENSE_DOSE_DISP'),
                                dispensing_in['raw_dispense_dose_disp_unit'].alias('RAW_DISPENSE_DOSE_DISP_UNIT'),
                                dispensing_in['raw_dispense_route'].alias('RAW_DISPENSE_ROUTE'),

        )
    except:
        dispensing_in = spark.read.load(input_data_folder_path+dispensing_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        dispensing = dispensing_in.select(


                                dispensing_in['DISPENSINGID'].alias('DISPENSINGID'),
                                dispensing_in['PATID'].alias('PATID'),
                                dispensing_in['PRESCRIBINGID'].alias('PRESCRIBINGID'),
                                cf.format_date_udf(dispensing_in['DISPENSE_DATE']).alias('DISPENSE_DATE'),
                                dispensing_in['NDC'].alias('NDC'),
                                dispensing_in['DISPENSE_SOURCE'].alias('DISPENSE_SOURCE'),
                                format_result_num_fh_udf(dispensing_in['DISPENSE_SUP']).alias('DISPENSE_SUP'),
                                format_result_num_fh_udf(dispensing_in['DISPENSE_AMT']).alias('DISPENSE_AMT'),
                                format_result_num_fh_udf(dispensing_in['DISPENSE_DOSE_DISP']).alias('DISPENSE_DOSE_DISP'),
                                dispensing_in['DISPENSE_DOSE_DISP_UNIT'].alias('DISPENSE_DOSE_DISP_UNIT'),
                                dispensing_in['DISPENSE_ROUTE'].alias('DISPENSE_ROUTE'),
                                dispensing_in['RAW_NDC'].alias('RAW_NDC'),
                                dispensing_in['RAW_DISPENSE_DOSE_DISP'].alias('RAW_DISPENSE_DOSE_DISP'),
                                dispensing_in['RAW_DISPENSE_DOSE_DISP_UNIT'].alias('RAW_DISPENSE_DOSE_DISP_UNIT'),
                                dispensing_in['RAW_DISPENSE_ROUTE'].alias('RAW_DISPENSE_ROUTE'),

        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################
    cf.write_pyspark_output_file(
                        payspark_df = dispensing,
                        output_file_name = "formatted_dispensing.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'dispensing_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')
