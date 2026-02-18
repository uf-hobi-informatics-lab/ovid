###################################################################################################################################

# This script will convert an OMOP measurement table to a PCORnet format as the lab_result_cm table

###################################################################################################################################

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


###################################################################################################################################

# This function will rertun lab_result_loc base on the measurement_source_value

###################################################################################################################################


def get_lab_result_loc( measurement_source_value):

    try:
        if measurement_source_value[0:3] == 'POC':
            return 'P'
        else:
            return 'L'
    except:
        return 'L'

get_lab_result_loc_udf = udf(get_lab_result_loc, StringType())


def get_time_from_datetime_omop_partner_plus(datetime_str):
        if datetime_str == None or datetime_str =='':
            return None

        return datetime_str[11:16]

get_time_from_datetime_omop_partner_plus_udf = udf(get_time_from_datetime_omop_partner_plus, StringType())


try:


    ###################################################################################################################################

    # Loading the measurement table to be converted to the lab_result_cm table

    ###################################################################################################################################
    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'


    measurement_table_name       = 'measurement.csv'
    measurement_sup_table_name   = 'measurement_sup.csv'

    measurement     = spark.read.load(input_data_folder_path+measurement_table_name,    format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


    measurement_sup = spark.read.load(input_data_folder_path+measurement_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    measurement_sup = measurement_sup.withColumnRenamed("measurement_id", "measurement_id_sup")

    concept = cf.spark_read(concept_table_path,spark)
    measurement_concept = concept.filter((col("domain_id") == "Measurement") & ((col("concept_class_id") != "Clinical Observation"))).withColumnRenamed("concept_code", "measurement_concept_code").withColumnRenamed("concept_name", "measurement_name")
    meas_value_concept  = concept.filter(concept.domain_id == 'Meas Value').withColumnRenamed("concept_code", "meas_value_concept_code")
    meas_value_operator_concept  = concept.filter(concept.domain_id == 'Meas Value Operator').withColumnRenamed("concept_name", "meas_value_operator_concept_name")
    unit_concept  = concept.filter(concept.domain_id == 'Unit').withColumnRenamed("concept_code", "unit_concept_code")



    joined_measurement = measurement.join(measurement_concept, measurement_concept['concept_id']     ==measurement['measurement_concept_id'], how='inner')\
                                    .join(measurement_sup,     measurement_sup['measurement_id_sup'] == measurement['measurement_id'],        how = 'left')\
                                    .join(meas_value_concept,  meas_value_concept['concept_id']      == measurement['value_as_concept_id'],   how = 'left')\
                                    .join(meas_value_operator_concept,  meas_value_operator_concept['concept_id'] == measurement['operator_concept_id'],   how = 'left')\
                                    .join(unit_concept,  unit_concept['concept_id'] == measurement['unit_concept_id'],   how = 'left')\
                                    
    ###################################################################################################################################

    #Converting the fileds to PCORNet lab_result_cm Format

    ###################################################################################################################################

    lab_result_cm = joined_measurement.select(      joined_measurement['measurement_id'].alias("LAB_RESULT_CM_ID"),
                                                    joined_measurement['person_id'].alias("PATID"),
                                                    joined_measurement['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    joined_measurement['measurement_concept_code'].alias("SPECIMEN_SOURCE"),
                                                    joined_measurement['measurement_concept_code'].alias("LAB_LOINC"),
                                                    lit('OD').alias("LAB_RESULT_SOURCE"),
                                                    joined_measurement['measurement_loinc_source'].alias("LAB_LOINC_SOURCE"),
                                                    joined_measurement['priority'].alias("PRIORITY"),
                                                    get_lab_result_loc_udf(joined_measurement['measurement_concept_code']).alias("RESULT_LOC"),
                                                    lit('').alias("LAB_PX"),
                                                    lit('').alias("LAB_PX_TYPE"),
                                                    cf.format_date_udf(joined_measurement['measurement_order_date']).alias("LAB_ORDER_DATE"),
                                                    cf.format_date_udf(joined_measurement['measurement_date']).alias("SPECIMEN_DATE"),
                                                    cf.format_time_udf(joined_measurement['measurement_datetime']).alias("SPECIMEN_TIME"),
                                                    cf.format_date_udf(joined_measurement['measurement_date']).alias("RESULT_DATE"),
                                                    cf.format_time_udf(joined_measurement['measurement_datetime']).alias("RESULT_TIME"),
                                                    joined_measurement['meas_value_concept_code'].alias("RESULT_QUAL"),
                                                    lit('').alias("RESULT_SNOMED"),
                                                    joined_measurement['value_as_number'].alias("RESULT_NUM"),
                                                    joined_measurement['meas_value_operator_concept_name'].alias("RESULT_MODIFIER"),
                                                    joined_measurement['unit_concept_code'].alias("RESULT_UNIT"),
                                                    joined_measurement['range_low'].alias("NORM_RANGE_LOW"),
                                                    joined_measurement['meas_value_operator_concept_name'].alias("NORM_MODIFIER_LOW"),
                                                    joined_measurement['range_high'].alias("NORM_RANGE_HIGH"),
                                                    joined_measurement['meas_value_operator_concept_name'].alias("NORM_MODIFIER_HIGH"),
                                                    lit('').alias("ABN_IND"),
                                                    joined_measurement['measurement_name'].alias("RAW_LAB_NAME"),
                                                    joined_measurement['measurement_source_value'].alias("RAW_LAB_CODE"),
                                                    lit('').alias("RAW_PANEL"),
                                                    joined_measurement['value_source_value'].alias("RAW_RESULT"),
                                                    joined_measurement['unit_source_value'].alias("RAW_UNIT"),
                                                    lit('').alias("RAW_ORDER_DEPT"),
                                                    lit('').alias("RAW_FACILITY_CODE"),
                                
                                                                                                                                
                                                    
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
                            partner = 'omop_partner_plus',
                            job     = 'lab_result_cm_formatter.py',
                            text = str(e)
 )



