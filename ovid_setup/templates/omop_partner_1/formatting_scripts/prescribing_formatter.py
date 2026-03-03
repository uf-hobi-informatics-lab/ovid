###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the prescribing table

###################################################################################################################################
import pyspark
from pyspark import SparkConf, SparkContext
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

# Loading the drug_exposure table to be converted to the prescribing table

###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    drug_exposure_table_name   = 'Drug_Exposure.txt'

    drug_exposure = spark.read.load(input_data_folder_path+drug_exposure_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    filter_values = ["Prescription written","Prescription","Home-Med","Prescription dispensed in pharmacy"]
    filtered_drug_exposure = drug_exposure.filter(col("drug_type").isin(filter_values))



    ###################################################################################################################################

    # This function will attemp to return the integer value of an input

    ###################################################################################################################################

    def split_and_return_integer(val):

        try:
            return float(val)
        except:

            try:
                num = val.split(' ')[0].strip()
                return float(num)
            except:
                return 0

    split_and_return_integer_udf = udf(split_and_return_integer, StringType())

    ###################################################################################################################################

    def check_and_return_number(value):
        try:
            num_int = int(value)  # Try converting to int
            return num_int
        except ValueError:
        
            try:
                num_float = float(value)  # Try converting to float
                return num_float
            except ValueError:
                return None  # Return None if conversion to int or float fails
    
    check_and_return_number_udf = udf(check_and_return_number, StringType())

    ###################################################################################################################################

    def get_time_from_datetime(val_time):

        if val_time  == None:
            return None
        # Parse the input string into a datetime object
        datetime_object = datetime.strptime(val_time, "%Y-%m-%d %H:%M:%S")

        # Format the datetime object as a string in "yyyy-mm-dd" format
        formatted_time = datetime_object.strftime("%H:%M")

        return formatted_time
    
    convert_and_format_time_udf = udf(get_time_from_datetime, StringType()) 


    ###################################################################################################################################

    def get_prn_flag(value):
        if value is None:
            return None
        else:
            if '*as needed*' in value:
                return 'Y'
            else:
                return 'N'
    get_prn_flag_udf = udf(get_prn_flag, StringType())
    ###################################################################################################################################

    #Converting the fields to PCORNet prescribing Format

    ###################################################################################################################################

    prescribing = filtered_drug_exposure.select(    filtered_drug_exposure['drug_exposure_id'].alias("PRESCRIBINGID"),
                                                    filtered_drug_exposure['person_id'].alias("PATID"),
                                                    filtered_drug_exposure['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    filtered_drug_exposure['provider_id'].alias("RX_PROVIDERID"),
                                                    filtered_drug_exposure['drug_exposure_start_date'].alias("RX_ORDER_DATE"),
                                                    convert_and_format_time_udf(filtered_drug_exposure['drug_exposure_start_datetime']).alias("RX_ORDER_TIME"),
                                                    filtered_drug_exposure['drug_exposure_start_date'].alias("RX_START_DATE"),
                                                    filtered_drug_exposure['drug_exposure_end_date'].alias("RX_END_DATE"),
                                                    filtered_drug_exposure['dose_ordered'].alias("RX_DOSE_ORDERED"),
                                                    filtered_drug_exposure['dose_unit'].alias("RX_DOSE_ORDERED_UNIT"),
                                                    split_and_return_integer_udf(filtered_drug_exposure['quantity']).alias("RX_QUANTITY"),
                                                    lit('UN').alias("RX_DOSE_FORM"),
                                                    check_and_return_number_udf(filtered_drug_exposure['refills']).alias("RX_REFILLS"),
                                                    filtered_drug_exposure['days_supply'].alias("RX_DAYS_SUPPLY"),
                                                    filtered_drug_exposure['rx_frequency'].alias("RX_FREQUENCY"),
                                                    get_prn_flag_udf(filtered_drug_exposure['rx_frequency']).alias("RX_PRN_FLAG"),
                                                    filtered_drug_exposure['route'].alias("RX_ROUTE"),
                                                    filtered_drug_exposure['drug_type'].alias("RX_BASIS"),
                                                    filtered_drug_exposure['drug_code'].alias("RXNORM_CUI"),
                                                    lit('OD').alias("RX_SOURCE"),
                                                    lit('OT').alias("RX_DISPENSE_AS_WRITTEN"),
                                                    filtered_drug_exposure['drug_code_source_value'].alias("RAW_RX_MED_NAME"),
                                                    filtered_drug_exposure['rx_frequency'].alias("RAW_RX_FREQUENCY"),
                                                    filtered_drug_exposure['drug_code'].alias("RAW_RXNORM_CUI"),
                                                    filtered_drug_exposure['quantity'].alias("RAW_RX_QUANTITY"),
                                                    filtered_drug_exposure['drug_code'].alias("RAW_RX_NDC"),
                                                    filtered_drug_exposure['dose_ordered_source_value'].alias("RAW_RX_DOSE_ORDERED"),
                                                    filtered_drug_exposure['dose_unit'].alias("RAW_RX_DOSE_ORDERED_UNIT"),
                                                    filtered_drug_exposure['route_source_value'].alias("RAW_RX_ROUTE"),
                                                    filtered_drug_exposure['refills'].alias("RAW_RX_REFILLS"),

                                                    
                                                    
                                                    
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









