###################################################################################################################################

# This script will convert an OMOP observation table to a PCORnet format as the obs_gen table

###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import pickle
from pyspark.sql import SparkSession
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



def get_time_from_datetime_omop_partner_plus(datetime_str):
        if datetime_str == None or datetime_str =='':
            return None

        return datetime_str[11:16]

get_time_from_datetime_omop_partner_plus_udf = udf(get_time_from_datetime_omop_partner_plus, StringType())


try: 


    ###################################################################################################################################

    # Loading the observation table to be converted to the obs_gen table

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    =  f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'

    observation_table_name       = 'observation.csv'
    observation_sup_table_name   = 'observation_sup.csv'
    
    concept       = cf.spark_read(concept_table_path,spark)

    observation_concept = concept.filter(concept.domain_id == 'Observation').withColumnRenamed("concept_code", "observation_concept_code").withColumnRenamed("vocabulary_id", "observation_vocabulary_id")
    unit_concept = concept.filter(concept.domain_id == 'Unit').withColumnRenamed("concept_code", "unit_concept_code")

    observation = spark.read.load(input_data_folder_path+observation_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    observation_sup = spark.read.load(input_data_folder_path+observation_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')\
                                                                                        .withColumnRenamed("observation_id", "observation_sup_id")

    # filter_values = ["LOINC"] # Only rows where observation_data_origin is in this list will convert to OBSGEN
    # joined_observation = observation.filter(~col("observation_code_type").isin(filter_values))


    joined_observation = observation.join(observation_concept, observation_concept['concept_id']==observation['observation_concept_id'], how='left').drop("concept_id")\
                                                .join(observation_sup, observation_sup['observation_sup_id']== observation['observation_id'], how = 'left')\
                                                .join(unit_concept, unit_concept['concept_id']== observation['unit_concept_id'], how = 'left')





    ###################################################################################################################################

    #Converting the fileds to PCORNet obs_gen Format
 
    ###################################################################################################################################

    obs_gen = joined_observation.select(           joined_observation['observation_id'].alias("OBSGENID"),
                                                    joined_observation['observation_id'].alias("PATID"),
                                                    joined_observation['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    joined_observation['provider_id'].alias("OBSGEN_PROVIDERID"),
                                                    cf.format_date_udf(joined_observation['observation_start_date']).alias("OBSGEN_START_DATE"),
                                                    cf.format_time_udf(joined_observation['observation_start_datetime']).alias("OBSGEN_START_TIME"),
                                                    cf.format_date_udf(joined_observation['observation_start_date']).alias("OBSGEN_STOP_DATE"),
                                                    cf.format_time_udf(joined_observation['observation_start_datetime']).alias("OBSGEN_STOP_TIME"),
                                                    joined_observation['observation_vocabulary_id'].alias("OBSGEN_TYPE"),
                                                    joined_observation['observation_concept_code'].alias("OBSGEN_CODE"),
                                                    joined_observation['qualifier_source_value'].alias("OBSGEN_RESULT_QUAL"),
                                                    joined_observation['observation_concept_code'].alias("OBSGEN_RESULT_TEXT"),
                                                    joined_observation['observation_concept_code'].alias("OBSGEN_RESULT_SNOMED"),
                                                    joined_observation['value_as_number'].alias("OBSGEN_RESULT_NUM"),
                                                    joined_observation['qualifier_source_value'].alias("OBSGEN_RESULT_MODIFIER"),
                                                    joined_observation['unit_concept_code'].alias("OBSGEN_RESULT_UNIT"),
                                                    lit('').alias("OBSGEN_TABLE_MODIFIED"),
                                                    lit('').alias("OBSGEN_ID_MODIFIED"),
                                                    joined_observation['observation_data_origin'].alias("OBSGEN_SOURCE"),
                                                    joined_observation['abn_ind'].alias("OBSGEN_ABN_IND"),
                                                    joined_observation['observation_concept_code'].alias("RAW_OBSGEN_NAME"),
                                                    joined_observation['observation_concept_code'].alias("RAW_OBSGEN_CODE"),
                                                    joined_observation['observation_vocabulary_id'].alias("RAW_OBSGEN_TYPE"),
                                                    concat(col("value_as_string"),lit(' - '), col("value_as_number")).alias("RAW_OBSGEN_RESULT"),
                                                    joined_observation['qualifier_source_value'].alias("RAW_OBSGEN_MODIFIER"),
                                                    joined_observation['unit_source_value'].alias("RAW_OBSGEN_UNIT")

                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = obs_gen,
                        output_file_name = "formatted_obs_gen.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'obs_gen_formatter.py' ,
                            text    = str(e))





