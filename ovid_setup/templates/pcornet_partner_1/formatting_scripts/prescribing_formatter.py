###################################################################################################################################

# This script will take in the PCORnet formatted raw PRESCRIBING file, do the necessary transformations, and output the formatted PCORnet PRESCRIBING file

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


def ot_or_ni( val):
    if val:
        return 'OT'

    return 'NI'

ot_or_ni_udf = udf(ot_or_ni,StringType())





try:

    ###################################################################################################################################

    # Loading the drug_exposure table to be converted to the prescribing table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    # input_data_folder_path               = f'/app/partners/pcornet_partner_1/data/input/{input_data_folder}/'

    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    prescribing_table_name   = '*Prescribing*'


    ###################################################################################################################################

    #Converting the fields to PCORNet prescribing Format

    ###################################################################################################################################
    
    try:

        prescribing_in = spark.read.load(input_data_folder_path+prescribing_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        prescribing = prescribing_in.select(


                                prescribing_in['prescribingid'].alias('PRESCRIBINGID'),
                                prescribing_in['patid'].alias('PATID'),
                                prescribing_in['encounterid'].alias('ENCOUNTERID'),
                                prescribing_in['rx_providerid'].alias('RX_PROVIDERID'),
                                cf.format_date_udf(prescribing_in['rx_order_date']).alias('RX_ORDER_DATE'),
                                cf.get_time_from_datetime_udf(prescribing_in['rx_order_time']).alias('RX_ORDER_TIME'),
                                cf.format_date_udf(prescribing_in['rx_start_date']).alias('RX_START_DATE'),
                                cf.format_date_udf(prescribing_in['rx_end_date']).alias('RX_END_DATE'),
                                format_result_num_fh_udf(prescribing_in['raw_rx_dose_ordered']).alias('RX_DOSE_ORDERED'),
                                prescribing_in['raw_rx_dose_ordered_unit'].alias('RX_DOSE_ORDERED_UNIT'),
                                format_result_num_fh_udf(prescribing_in['raw_rx_quantity']).alias('RX_QUANTITY'),
                                prescribing_in['raw_rxnorm_cui'].alias('RX_DOSE_FORM'),
                                format_result_num_fh_udf(prescribing_in['raw_rx_refills']).alias('RX_REFILLS'),
                                format_result_num_fh_udf(prescribing_in['rx_days_supply']).alias('RX_DAYS_SUPPLY'),
                                prescribing_in['raw_rx_frequency'].alias('RX_FREQUENCY'),
                                prescribing_in['rx_prn_flag'].alias('RX_PRN_FLAG'),
                                prescribing_in['raw_rx_route'].alias('RX_ROUTE'),
                                prescribing_in['rx_basis'].alias('RX_BASIS'),
                                prescribing_in['raw_rxnorm_cui'].alias('RXNORM_CUI'),
                                prescribing_in['rx_source'].alias('RX_SOURCE'),
                                lit('NI').alias('RX_DISPENSE_AS_WRITTEN'),
                                prescribing_in['raw_rx_med_name'].alias('RAW_RX_MED_NAME'),
                                prescribing_in['raw_rx_frequency'].alias('RAW_RX_FREQUENCY'),
                                prescribing_in['raw_rxnorm_cui'].alias('RAW_RXNORM_CUI'),
                                prescribing_in['raw_rx_quantity'].alias('RAW_RX_QUANTITY'),
                                prescribing_in['raw_rx_ndc'].alias('RAW_RX_NDC'),
                                prescribing_in['raw_rx_dose_ordered'].alias('RAW_RX_DOSE_ORDERED'),
                                prescribing_in['raw_rx_dose_ordered_unit'].alias('RAW_RX_DOSE_ORDERED_UNIT'),
                                prescribing_in['raw_rx_route'].alias('RAW_RX_ROUTE'),
                                prescribing_in['raw_rx_refills'].alias('RAW_RX_REFILLS'),

        )
    except:
        prescribing_in = spark.read.load(input_data_folder_path+prescribing_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        prescribing = prescribing_in.select(


                                prescribing_in['PRESCRIBINGID'].alias('PRESCRIBINGID'),
                                prescribing_in['PATID'].alias('PATID'),
                                prescribing_in['ENCOUNTERID'].alias('ENCOUNTERID'),
                                prescribing_in['RX_PROVIDERID'].alias('RX_PROVIDERID'),
                                cf.format_date_udf(prescribing_in['RX_ORDER_DATE']).alias('RX_ORDER_DATE'),
                                cf.get_time_from_datetime_udf(prescribing_in['RX_ORDER_TIME']).alias('RX_ORDER_TIME'),
                                cf.format_date_udf(prescribing_in['RX_START_DATE']).alias('RX_START_DATE'),
                                cf.format_date_udf(prescribing_in['RX_END_DATE']).alias('RX_END_DATE'),
                                format_result_num_fh_udf(prescribing_in['RAW_RX_DOSE_ORDERED']).alias('RX_DOSE_ORDERED'),
                                prescribing_in['RAW_RX_DOSE_ORDERED_UNIT'].alias('RX_DOSE_ORDERED_UNIT'),
                                format_result_num_fh_udf(prescribing_in['RAW_RX_QUANTITY']).alias('RX_QUANTITY'),
                                prescribing_in['RAW_RXNORM_CUI'].alias('RX_DOSE_FORM'),
                                format_result_num_fh_udf(prescribing_in['RAW_RX_REFILLS']).alias('RX_REFILLS'),
                                format_result_num_fh_udf(prescribing_in['RX_DAYS_SUPPLY']).alias('RX_DAYS_SUPPLY'),
                                prescribing_in['RAW_RX_FREQUENCY'].alias('RX_FREQUENCY'),
                                prescribing_in['RX_PRN_FLAG'].alias('RX_PRN_FLAG'),
                                prescribing_in['RAW_RX_ROUTE'].alias('RX_ROUTE'),
                                prescribing_in['RX_BASIS'].alias('RX_BASIS'),
                                prescribing_in['RAW_RXNORM_CUI'].alias('RXNORM_CUI'),
                                prescribing_in['RX_SOURCE'].alias('RX_SOURCE'),
                                lit('NI').alias('RX_DISPENSE_AS_WRITTEN'),
                                prescribing_in['RAW_RX_MED_NAME'].alias('RAW_RX_MED_NAME'),
                                prescribing_in['RAW_RX_FREQUENCY'].alias('RAW_RX_FREQUENCY'),
                                prescribing_in['RAW_RXNORM_CUI'].alias('RAW_RXNORM_CUI'),
                                prescribing_in['RAW_RX_QUANTITY'].alias('RAW_RX_QUANTITY'),
                                prescribing_in['RAW_RX_NDC'].alias('RAW_RX_NDC'),
                                prescribing_in['RAW_RX_DOSE_ORDERED'].alias('RAW_RX_DOSE_ORDERED'),
                                prescribing_in['RAW_RX_DOSE_ORDERED_UNIT'].alias('RAW_RX_DOSE_ORDERED_UNIT'),
                                prescribing_in['RAW_RX_ROUTE'].alias('RAW_RX_ROUTE'),
                                prescribing_in['RAW_RX_REFILLS'].alias('RAW_RX_REFILLS'),

        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################
    cf.write_pyspark_output_file(
                        payspark_df = prescribing,
                        output_file_name = "formatted_prescribing.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'prescribing_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')




