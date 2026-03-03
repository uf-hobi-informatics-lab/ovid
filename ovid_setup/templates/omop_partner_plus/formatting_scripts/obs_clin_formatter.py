###################################################################################################################################

# This script will convert an OMOP measurement table to a PCORnet format as the obs_clin table

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




try:


    ###################################################################################################################################

    # Loading the measurement table to be converted to the obs_clin table

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
    measurement_concept = concept.filter((col("domain_id") == "Measurement") & ((col("concept_class_id") == "Clinical Observation")))\
                                                .withColumnRenamed("concept_code", "measurement_concept_code")\
                                                .withColumnRenamed("concept_name", "measurement_name")\
                                                .withColumnRenamed("vocabulary_id", "measurement_vocabulary_id")
    
    meas_value_concept  = concept.filter(concept.domain_id == 'Meas Value').withColumnRenamed("concept_code", "meas_value_concept_code")
    meas_value_operator_concept  = concept.filter(concept.domain_id == 'Meas Value Operator').withColumnRenamed("concept_name", "meas_value_operator_concept_name")
    unit_concept  = concept.filter(concept.domain_id == 'Unit').withColumnRenamed("concept_code", "unit_concept_code")



    joined_measurement = measurement.join(measurement_concept, measurement_concept['concept_id']     ==measurement['measurement_concept_id'], how='inner')\
                                    .join(measurement_sup,     measurement_sup['measurement_id_sup'] == measurement['measurement_id'],        how = 'left')\
                                    .join(meas_value_concept,  meas_value_concept['concept_id']      == measurement['value_as_concept_id'],   how = 'left')\
                                    .join(meas_value_operator_concept,  meas_value_operator_concept['concept_id'] == measurement['operator_concept_id'],   how = 'left')\
                                    .join(unit_concept,  unit_concept['concept_id'] == measurement['unit_concept_id'],   how = 'left')\
                                    
    ###################################################################################################################################

    #Converting the fileds to PCORNet obs_clin Format

    ###################################################################################################################################


    


    obs_clin = joined_measurement.select(           joined_measurement['measurement_id'].alias("OBSCLINID"),
                                                    joined_measurement['person_id'].alias("PATID"),
                                                    joined_measurement['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    joined_measurement['provider_id'].alias("OBSCLIN_PROVIDERID"),
                                                    cf.format_date_udf(joined_measurement['measurement_date']).alias("OBSCLIN_START_DATE"),
                                                    cf.format_time_udf(joined_measurement['measurement_datetime']).alias("OBSCLIN_START_TIME"),
                                                    cf.format_date_udf(joined_measurement['measurement_date']).alias("OBSCLIN_STOP_DATE"),
                                                    cf.format_time_udf(joined_measurement['measurement_datetime']).alias("OBSCLIN_STOP_TIME"),                                            
                                                    joined_measurement['measurement_vocabulary_id'].alias("OBSCLIN_TYPE"),
                                                    joined_measurement['measurement_concept_code'].alias("OBSCLIN_CODE"),
                                                    joined_measurement['meas_value_concept_code'].alias("OBSCLIN_RESULT_QUAL"),
                                                    joined_measurement['measurement_concept_code'].alias("OBSCLIN_RESULT_TEXT"),
                                                    joined_measurement['measurement_concept_code'].alias("OBSCLIN_RESULT_SNOMED"),
                                                    joined_measurement['value_as_number'].alias("OBSCLIN_RESULT_NUM"),
                                                    joined_measurement['meas_value_operator_concept_name'].alias("OBSCLIN_RESULT_MODIFIER"),
                                                    joined_measurement['unit_concept_code'].alias("OBSCLIN_RESULT_UNIT"),
                                                    lit('OD').alias("OBSCLIN_SOURCE"),
                                                    lit('').alias("OBSCLIN_ABN_IND"),
                                                    joined_measurement['measurement_concept_code'].alias("RAW_OBSCLIN_NAME"),
                                                    joined_measurement['measurement_concept_code'].alias("RAW_OBSCLIN_CODE"),
                                                    joined_measurement['measurement_vocabulary_id'].alias("RAW_OBSCLIN_TYPE"),
                                                    concat(col("measurement_concept_code"),lit(' - '), col("value_as_number")).alias("RAW_OBSCLIN_RESULT"),
                                                    joined_measurement['meas_value_operator_concept_name'].alias("RAW_OBSCLIN_MODIFIER"),
                                                    joined_measurement['unit_source_value'].alias("RAW_OBSCLIN_UNIT"),



                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = obs_clin,
                        output_file_name = "formatted_obs_clin.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()




except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'obs_clin_formatter.py',
                            text = str(e)
 )



