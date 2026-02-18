###################################################################################################################################

# This script will take in the PCORnet formatted raw LAB_RESULT_CM file, do the necessary transformations, and output the formatted PCORnet LAB_RESULT_CM file

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

def remove_prefix_num( val):
    """
    This assumes a value of ########|VAL
    where there is a number, followed by a pipe,
    followed by the value we want to use
    """

    try:

        if isinstance(val, float):
            return val
        else:
            if '|' in val:
                return val.split('|')[1]
            else:
                return None
    except:
        return None

remove_prefix_num_udf = udf(remove_prefix_num, StringType())



def flh_lab_result_date( result_date, specimen_date, lab_order_date):  # noqa
    if result_date != '':
        return result_date

    if specimen_date != '':
        return specimen_date

    if lab_order_date != '':
        return lab_order_date

    return '1900-01-01'

flh_lab_result_date_udf = udf(flh_lab_result_date, StringType())



def format_result_num_fh( val):
    try:
        float(val)
        return val
    except:
        #Not a float
        return None
    

format_result_num_fh_udf = udf(format_result_num_fh, StringType())



def clean_pcornet_partner_1_result_qual( result_qual):

    result_qual_list = [
         "DETECTED",
         "NOT DETECTED",
         "POSITIVE",
         "NEGATIVE",
         "BORDERLINE",
         "HIGH",
         "LOW",
         "NORMAL",
         "ABNORMAL",
         "UNDETERMINED",
         "NO INFROMATION",
         "UNKNOWN",
         "OT",
         "OTHER",

    ]


    if result_qual == None or result_qual == 'NONE' or result_qual == "":

        return "NI"
    
    elif result_qual not in result_qual_list:
                 
                 return 'OT'
    
    else: 
                 
                 return result_qual

    

clean_pcornet_partner_1_result_qual_udf = udf(clean_pcornet_partner_1_result_qual, StringType())




try: 

    ###################################################################################################################################
    
    # Loading the raw lab_result_cm table

    ###################################################################################################################################
    input_data_folder_path               = f'/data/{input_data_folder}/'
    # input_data_folder_path               = f'/app/partners/pcornet_partner_1/data/input/{input_data_folder}/'  

    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    labs_table_name   = '*Lab*'


    ###################################################################################################################################

    #Converting the fileds to PCORNet lab_result_cm Format

    ###################################################################################################################################

    try:
         
        lab_result_cm_in = spark.read.load(input_data_folder_path+labs_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        lab_result_cm = lab_result_cm_in.select(

                        lab_result_cm_in['lab_result_cm_id'].alias('LAB_RESULT_CM_ID'),
                        lab_result_cm_in['patid'].alias('PATID'),
                        lab_result_cm_in['encounterid'].alias('ENCOUNTERID'),
                        lab_result_cm_in['lab_loinc'].alias('SPECIMEN_SOURCE'),
                        lab_result_cm_in['lab_loinc'].alias('LAB_LOINC'),
                        lit('OD').alias('LAB_RESULT_SOURCE'),
                        lit('LM').alias('LAB_LOINC_SOURCE'),
                        lab_result_cm_in['priority'].alias('PRIORITY'),
                        lab_result_cm_in['result_loc'].alias('RESULT_LOC'),
                        remove_prefix_num_udf(lab_result_cm_in['lab_px']).alias('LAB_PX'),
                        lab_result_cm_in['lab_px_type'].alias('LAB_PX_TYPE'),
                        cf.format_date_udf(lab_result_cm_in['lab_order_date']).alias('LAB_ORDER_DATE'),
                        cf.format_date_udf(lab_result_cm_in['specimen_date']).alias('SPECIMEN_DATE'),
                        cf.get_time_from_datetime_udf(lab_result_cm_in['specimen_time']).alias('SPECIMEN_TIME'),
                        flh_lab_result_date_udf(lab_result_cm_in['result_date'],lab_result_cm_in['specimen_date'],lab_result_cm_in['lab_order_date']).alias('RESULT_DATE'),
                        cf.get_time_from_datetime_udf(lab_result_cm_in['result_time']).alias('RESULT_TIME'),
                        clean_pcornet_partner_1_result_qual_udf(lab_result_cm_in['result_qual']).alias('RESULT_QUAL'),
                        lab_result_cm_in['result_snomed'].alias('RESULT_SNOMED'),
                        format_result_num_fh_udf(lab_result_cm_in['raw_result']).alias('RESULT_NUM'),
                        lab_result_cm_in['result_modifier'].alias('RESULT_MODIFIER'),
                        lab_result_cm_in['result_unit'].alias('RESULT_UNIT'),
                        lab_result_cm_in['norm_range_low'].alias('NORM_RANGE_LOW'),
                        lab_result_cm_in['norm_modifier_low'].alias('NORM_MODIFIER_LOW'),
                        lab_result_cm_in['norm_range_high'].alias('NORM_RANGE_HIGH'),
                        lab_result_cm_in['norm_modifier_high'].alias('NORM_MODIFIER_HIGH'),
                        lab_result_cm_in['abn_ind'].alias('ABN_IND'),
                        lab_result_cm_in['raw_lab_name'].alias('RAW_LAB_NAME'),
                        lab_result_cm_in['raw_lab_code'].alias('RAW_LAB_CODE'),
                        lab_result_cm_in['raw_panel'].alias('RAW_PANEL'),
                        lab_result_cm_in['raw_result'].alias('RAW_RESULT'),
                        lab_result_cm_in['raw_unit'].alias('RAW_UNIT'),
                        lab_result_cm_in['raw_order_dept'].alias('RAW_ORDER_DEPT'),
                        lab_result_cm_in['raw_facility_code'].alias('RAW_FACILITY_CODE'),
        )
    except:
        
        lab_result_cm_in = spark.read.load(input_data_folder_path+labs_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
        lab_result_cm = lab_result_cm_in.select(

                    lab_result_cm_in['LAB_RESULTS_CM_ID'].alias('LAB_RESULT_CM_ID'),
                    lab_result_cm_in['PATID'].alias('PATID'),
                    lab_result_cm_in['ENCOUNTERID'].alias('ENCOUNTERID'),
                    lab_result_cm_in['LAB_LOINC'].alias('SPECIMEN_SOURCE'),
                    lab_result_cm_in['LAB_LOINC'].alias('LAB_LOINC'),
                    lit('OD').alias('LAB_RESULT_SOURCE'),
                    lit('LM').alias('LAB_LOINC_SOURCE'),
                    lab_result_cm_in['PRIORITY'].alias('PRIORITY'),
                    lab_result_cm_in['RESULT_LOC'].alias('RESULT_LOC'),
                    remove_prefix_num_udf(lab_result_cm_in['LAB_PX']).alias('LAB_PX'),
                    lab_result_cm_in['LAB_PX_TYPE'].alias('LAB_PX_TYPE'),
                    cf.format_date_udf(lab_result_cm_in['LAB_ORDER_DATE']).alias('LAB_ORDER_DATE'),
                    cf.format_date_udf(lab_result_cm_in['SPECIMEN_DATE']).alias('SPECIMEN_DATE'),
                    cf.get_time_from_datetime_udf(lab_result_cm_in['SPECIMEN_TIME']).alias('SPECIMEN_TIME'),
                    flh_lab_result_date_udf(lab_result_cm_in['RESULT_DATE'],lab_result_cm_in['SPECIMEN_DATE'],lab_result_cm_in['LAB_ORDER_DATE']).alias('RESULT_DATE'),
                    cf.get_time_from_datetime_udf(lab_result_cm_in['RESULT_TIME']).alias('RESULT_TIME'),
                    clean_pcornet_partner_1_result_qual_udf(lab_result_cm_in['RESULT_QUAL']).alias('RESULT_QUAL'),
                    lab_result_cm_in['RESULT_SNOMED'].alias('RESULT_SNOMED'),
                    format_result_num_fh_udf(lab_result_cm_in['RAW_RESULT']).alias('RESULT_NUM'),
                    lab_result_cm_in['RESULT_MODIFIER'].alias('RESULT_MODIFIER'),
                    lab_result_cm_in['RESULT_UNIT'].alias('RESULT_UNIT'),
                    lab_result_cm_in['NORM_RANGE_LOW'].alias('NORM_RANGE_LOW'),
                    lab_result_cm_in['NORM_MODIFIER_LOW'].alias('NORM_MODIFIER_LOW'),
                    lab_result_cm_in['NORM_RANGE_HIGH'].alias('NORM_RANGE_HIGH'),
                    lab_result_cm_in['NORM_MODIFIER_HIGH'].alias('NORM_MODIFIER_HIGH'),
                    lab_result_cm_in['ABN_IND'].alias('ABN_IND'),
                    lab_result_cm_in['RAW_LAB_NAME'].alias('RAW_LAB_NAME'),
                    lab_result_cm_in['RAW_LAB_CODE'].alias('RAW_LAB_CODE'),
                    lab_result_cm_in['RAW_PANEL'].alias('RAW_PANEL'),
                    lab_result_cm_in['RAW_RESULT'].alias('RAW_RESULT'),
                    lab_result_cm_in['RAW_UNIT'].alias('RAW_UNIT'),
                    lab_result_cm_in['RAW_ORDER_DEPT'].alias('RAW_ORDER_DEPT'),
                    lab_result_cm_in['RAW_FACIlitY_CODE'].alias('RAW_FACILITY_CODE'),
    )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = lab_result_cm,
                        output_file_name = "formatted_lab_result_cm.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()





except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'lab_result_cm_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')


