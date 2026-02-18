###################################################################################################################################

# This script will convert an OMOP measurement table to a PCORnet format as the vital table

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

# Loading the measurement table to be converted to the vital table

###################################################################################################################################


try: 

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'


    measurement_table_name            = 'measurement.csv'
    measurement_sup_table_name        = 'measurement_sup.csv'


    observation_table_name            = 'observation.csv'

    measurement = spark.read.load(input_data_folder_path+measurement_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    # measurement_sup = spark.read.load(input_data_folder_path+measurement_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed('measurement_id','measurement_sup_id')

    # joined_measurement = measurement.join(measurement_sup, measurement_sup["measurement_sup_id"]== measurement['measurement_id'], how = 'left')

    observation = spark.read.load(input_data_folder_path+observation_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("measurement_id", "measurement_sup_id")

    concept       = cf.spark_read(concept_table_path,spark)




    ht_concept_id = ['37020737','3035463','3036277','3036798','3013842','3014149','3008989','3015514','1003232','1003304','1029031','3023540','3019171','36304231','37020651','1003850']

    wt_concept_id = ['3003176','3002072','3020933','3020110','3005565','3004464','3002756','3001647','3023704','3037845','3033534','3033528','3021838','3001912','3000749','3033350','3034205',
                '3034737','3022724','3013673','3011956','3017459','3018298','3035618','3036221','3009664','3012860','3018372','3008484','3022484','3013747','40761330','3013853',
                '3011043','3026659','3027492','3010914','3028543','3009617','3010147','3015644','3019336','3022281','3005422','3010220','3011054','21492642','3026600','3013131','3042378',
                '40759177','40759213','40759214','43533987','36305329','1029318','3025315','40760186','3043735','3046620','3046286','3046309','3046001','3028871','3029459','3028864',
                '46234683','1003088','1003261','1003206','3014618','3015141','3008900','3007541','3014140','3008128','3015512','3016054','3016341','3008303','3013762','3023166','40771967',
                '40771968','1003642','1004141','1004122']

    smoking_tobacco_concept_id = ['3477063','3465894','3466598','3171662','4132133','4218197','4282779','4005823','3434950','3422059','762498','762500','45765920',
                             '4218741','3323369','4224317','3235632','3450269']

    diastolic_concept_id = ['3034703','3019962','3013940','3012888']

    systolic_concept_id = ['3018586','3035856','3009395','3004249']

    bmi_concept_id = ['40762636','3038553','45581059','45600349','45552098','45600350']

    measurement_filter_values_codes = ht_concept_id + wt_concept_id + bmi_concept_id + diastolic_concept_id + systolic_concept_id

    filtered_measurement = measurement.filter(trim(col("measurement_concept_id")).isin(measurement_filter_values_codes))
    filtered_observation = observation.filter(trim(col("observation_concept_id")).isin(smoking_tobacco_concept_id))


    # Create a new column for each category based on the measurement_code

    filtered_measurement = filtered_measurement.withColumn('HT', when(col('measurement_concept_id').isin(ht_concept_id), col('value_as_number')).otherwise(None))
    filtered_measurement = filtered_measurement.withColumn('WT', when(col('measurement_concept_id').isin(wt_concept_id), col('value_as_number')).otherwise(None))
    filtered_measurement = filtered_measurement.withColumn('BMI', when(col('measurement_concept_id').isin(bmi_concept_id), col('value_as_number')).otherwise(None))
    filtered_measurement = filtered_measurement.withColumn('DIASTOLIC', when(col('measurement_concept_id').isin(diastolic_concept_id), col('value_as_number')).otherwise(None))
    filtered_measurement = filtered_measurement.withColumn('SYSTOLIC', when(col('measurement_concept_id').isin(systolic_concept_id), col('value_as_number')).otherwise(None))

    filtered_observation = filtered_observation.withColumn('SMOKING', when(col('observation_concept_id').isin(smoking_tobacco_concept_id), col('value_as_string')).otherwise(None))
    filtered_observation = filtered_observation.withColumn('TOBACCO', when(col('observation_concept_id').isin(smoking_tobacco_concept_id), col('value_as_string')).otherwise(None))



                                            

    ###################################################################################################################################

    #Converting the fileds to PCORNet vital Format

    ###################################################################################################################################

    vital_from_measurement_uncombined = filtered_measurement.select(    
                                                    filtered_measurement['measurement_id'].alias("VITALID"),
                                                    filtered_measurement['person_id'].alias("PATID"),
                                                    filtered_measurement['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    filtered_measurement['measurement_date'].alias("MEASURE_DATE"),
                                                    cf.get_time_from_datetime_udf(measurement['measurement_datetime']).alias("MEASURE_TIME"),
                                                    lit("OD").alias("VITAL_SOURCE"),
                                                    filtered_measurement['HT'].alias("HT"),
                                                    filtered_measurement['WT'].alias("WT"),
                                                    filtered_measurement['DIASTOLIC'].alias("DIASTOLIC"),
                                                    filtered_measurement['SYSTOLIC'].alias("SYSTOLIC"),
                                                    filtered_measurement['BMI'].alias("ORIGINAL_BMI"),
                                                    lit('').alias("BP_POSITION"),
                                                    lit('').alias("SMOKING"),
                                                    lit('').alias("TOBACCO"),
                                                    lit('').alias("TOBACCO_TYPE"),
                                                    filtered_measurement['DIASTOLIC'].alias("RAW_DIASTOLIC"),
                                                    filtered_measurement['SYSTOLIC'].alias("RAW_SYSTOLIC"),
                                                    lit('').alias("RAW_BP_POSITION"),
                                                    lit('').alias("RAW_SMOKING"),
                                                    lit('').alias("RAW_TOBACCO"),
                                                    lit('').alias("RAW_TOBACCO_TYPE"),
                                                    )
    

    vital_from_observation_uncombined = filtered_observation.select(    
                                                    filtered_observation['observation_id'].alias("VITALID"),
                                                    filtered_observation['person_id'].alias("PATID"),
                                                    filtered_observation['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    cf.format_date_udf(filtered_observation['observation_start_date']).alias("MEASURE_DATE"),
                                                    cf.format_time_udf(filtered_observation['observation_start_datetime']).alias("MEASURE_TIME"),
                                                    lit("OD").alias("VITAL_SOURCE"),
                                                    lit('').alias("HT"),
                                                    lit('').alias("WT"),
                                                    lit('').alias("DIASTOLIC"),
                                                    lit('').alias("SYSTOLIC"),
                                                    lit('').alias("ORIGINAL_BMI"),
                                                    lit('').alias("BP_POSITION"),
                                                    filtered_observation['SMOKING'].alias("SMOKING"),
                                                    filtered_observation['TOBACCO'].alias("TOBACCO"),
                                                    lit('').alias("TOBACCO_TYPE"),
                                                    lit('').alias("RAW_DIASTOLIC"),
                                                    lit('').alias("RAW_SYSTOLIC"),
                                                    lit('').alias("RAW_BP_POSITION"),
                                                    filtered_observation['SMOKING'].alias("RAW_SMOKING"),
                                                    filtered_observation['TOBACCO'].alias("RAW_TOBACCO"),
                                                    lit('').alias("RAW_TOBACCO_TYPE"),
                                                    )


    vital = vital_from_measurement_uncombined.union(vital_from_measurement_uncombined)

    # Columns used to group the data
    key_columns = ["ENCOUNTERID", "PATID"]

    # Columns to aggregate
    columns_to_aggregate = [col for col in vital.columns if col not in key_columns]

    # Create aggregation expressions
    aggregation_expr = [first(when(col(column_name) != "", col(column_name)), ignorenulls =True).alias(column_name) 
                        for column_name in columns_to_aggregate]

    # Group and aggregate
    grouped_vital = vital.groupBy(*key_columns).agg(*aggregation_expr)


    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = grouped_vital,
                        output_file_name = "formatted_vital.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'vital_formatter.py' ,
                            text    = str(e))
