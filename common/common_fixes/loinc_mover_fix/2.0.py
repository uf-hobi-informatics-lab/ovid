import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from commonFunctions import CommonFuncitons
import argparse
import os
import pyspark.sql.types as T
import subprocess
import shutil
from pyspark.sql import functions as F



def  proces_table(df, df_source_table_name, loinc_code_location_df, fixed_data_folder_work_directory_path):

    lab_result_cm_from_df_output_name = 'lab_result_cm_from_'+df_source_table_name+'.csv'
    obs_clin_from_df_output_name = 'obs_clin_from_'+df_source_table_name+'.csv'
    obs_gen_from_df_output_name = 'obs_gen_from_'+df_source_table_name+'.csv'


    common_in_lab = df.join(loinc_code_location_df, df['LOINC']== loinc_code_location_df['loinc_num'], how = 'inner').filter(col('found_in_lab') == '1')
    common_in_obs_clin = df.join(loinc_code_location_df, df['LOINC']== loinc_code_location_df['loinc_num'], how = 'inner').filter(col('found_in_obs_clin') == '1')
    common_in_obs_gen = df.join(loinc_code_location_df, df['LOINC']== loinc_code_location_df['loinc_num'], how = 'inner').filter((col('found_in_obs_clin') == '0') & (col('found_in_obs_gen') == '1'))



    lab_result_cm_from_df = common_in_lab.select(

                                    common_in_lab['ID'].alias("LAB_RESULT_CM_ID"),
                                    common_in_lab['PATID'].alias("PATID"),
                                    common_in_lab['ENCOUNTERID'].alias("ENCOUNTERID"),
                                    common_in_lab['SPECIMEN_SOURCE'].alias("SPECIMEN_SOURCE"),
                                    common_in_lab['LOINC'].alias("LAB_LOINC"),
                                    common_in_lab['RESULT_SOURCE'].alias("LAB_RESULT_SOURCE"),
                                    common_in_lab['LOINC_SOURCE'].alias("LAB_LOINC_SOURCE"),
                                    common_in_lab['PRIORITY'].alias("PRIORITY"),
                                    common_in_lab['RESULT_LOC'].alias("RESULT_LOC"),
                                    common_in_lab['PX'].alias("LAB_PX"),
                                    common_in_lab['PX_TYPE'].alias("LAB_PX_TYPE"),
                                    common_in_lab['ORDER_DATE'].alias("LAB_ORDER_DATE"),
                                    common_in_lab['SPECIMEN_DATE'].alias("SPECIMEN_DATE"),
                                    common_in_lab['SPECIMEN_TIME'].alias("SPECIMEN_TIME"),
                                    common_in_lab['RESULT_DATE'].alias("RESULT_DATE"),
                                    common_in_lab['RESULT_TIME'].alias("RESULT_TIME"),
                                    common_in_lab['RESULT_QUAL'].alias("RESULT_QUAL"),
                                    common_in_lab['RESULT_SNOMED'].alias("RESULT_SNOMED"),
                                    common_in_lab['RESULT_NUM'].alias("RESULT_NUM"),
                                    common_in_lab['RESULT_MODIFIER'].alias("RESULT_MODIFIER"),
                                    common_in_lab['RESULT_UNIT'].alias("RESULT_UNIT"),
                                    common_in_lab['NORM_RANGE_LOW'].alias("NORM_RANGE_LOW"),
                                    common_in_lab['NORM_MODIFIER_LOW'].alias("NORM_MODIFIER_LOW"),
                                    common_in_lab['NORM_RANGE_HIGH'].alias("NORM_RANGE_HIGH"),
                                    common_in_lab['NORM_MODIFIER_HIGH'].alias("NORM_MODIFIER_HIGH"),                               
                                    common_in_lab['ABN_IND'].alias("ABN_IND"),
                                    common_in_lab['RAW_NAME'].alias("RAW_LAB_NAME"),
                                    common_in_lab['RAW_CODE'].alias("RAW_LAB_CODE"),
                                    common_in_lab['RAW_PANEL'].alias("RAW_PANEL"),
                                    common_in_lab['RAW_RESULT'].alias("RAW_RESULT"),
                                    common_in_lab['RAW_UNIT'].alias("RAW_UNIT"),
                                    common_in_lab['RAW_ORDER_DEPT'].alias("RAW_ORDER_DEPT"),
                                    common_in_lab['RAW_FACILITY_CODE'].alias("RAW_FACILITY_CODE"),
                                    common_in_lab['UPDATED'].alias("UPDATED"),
                                    common_in_lab['SOURCE'].alias("SOURCE")
                                    
    )


    obs_clin_from_df = common_in_obs_clin.select(



                                    common_in_obs_clin['ID'].alias("OBSCLINID"),
                                    common_in_obs_clin['PATID'].alias("PATID"),
                                    common_in_obs_clin['ENCOUNTERID'].alias("ENCOUNTERID"),
                                    common_in_obs_clin['PROVIDERID'].alias("OBSCLIN_PROVIDERID"),
                                    common_in_obs_clin['RESULT_DATE'].alias("OBSCLIN_START_DATE"),
                                    common_in_obs_clin['RESULT_TIME'].alias("OBSCLIN_START_TIME"),
                                    common_in_obs_clin['END_DATE'].alias("OBSCLIN_STOP_DATE"),
                                    common_in_obs_clin['END_TIME'].alias("OBSCLIN_STOP_TIME"),
                                    common_in_obs_clin['PX_TYPE'].alias("OBSCLIN_TYPE"),
                                    common_in_obs_clin['LOINC'].alias("OBSCLIN_CODE"),
                                    common_in_obs_clin['RESULT_QUAL'].alias("OBSCLIN_RESULT_QUAL"),
                                    common_in_obs_clin['RESULT_TEXT'].alias("OBSCLIN_RESULT_TEXT"),
                                    common_in_obs_clin['RESULT_SNOMED'].alias("OBSCLIN_RESULT_SNOMED"),
                                    common_in_obs_clin['RESULT_NUM'].alias("OBSCLIN_RESULT_NUM"),
                                    common_in_obs_clin['RESULT_MODIFIER'].alias("OBSCLIN_RESULT_MODIFIER"),
                                    common_in_obs_clin['RESULT_UNIT'].alias("OBSCLIN_RESULT_UNIT"),
                                    F.when(common_in_obs_clin['RESULT_SOURCE'] == 'OD', 'DR').otherwise(common_in_obs_clin['RESULT_SOURCE']).alias("OBSCLIN_SOURCE"),
                                    common_in_obs_clin['ABN_IND'].alias("OBSCLIN_ABN_IND"),
                                    common_in_obs_clin['RAW_NAME'].alias("RAW_OBSCLIN_NAME"),
                                    common_in_obs_clin['RAW_CODE'].alias("RAW_OBSCLIN_CODE"),
                                    common_in_obs_clin['RAW_TYPE'].alias("RAW_OBSCLIN_TYPE"),
                                    common_in_obs_clin['RAW_RESULT'].alias("RAW_OBSCLIN_RESULT"),
                                    common_in_obs_clin['RAW_MODIFIER'].alias("RAW_OBSCLIN_MODIFIER"),
                                    common_in_obs_clin['RAW_UNIT'].alias("RAW_OBSCLIN_UNIT"),
                                    common_in_obs_clin['UPDATED'].alias("UPDATED"),
                                    common_in_obs_clin['SOURCE'].alias("SOURCE"),

                                    
    )


    obs_gen_from_df = common_in_obs_gen.select(

                                    common_in_obs_gen['ID'].alias("OBSGENID"),
                                    common_in_obs_gen['PATID'].alias("PATID"),
                                    common_in_obs_gen['ENCOUNTERID'].alias("ENCOUNTERID"),
                                    common_in_obs_gen['PROVIDERID'].alias("OBSGEN_PROVIDERID"),
                                    common_in_obs_gen['RESULT_DATE'].alias("OBSGEN_START_DATE"),
                                    common_in_obs_gen['RESULT_TIME'].alias("OBSGEN_START_TIME"),
                                    common_in_obs_gen['END_DATE'].alias("OBSGEN_STOP_DATE"),
                                    common_in_obs_gen['END_TIME'].alias("OBSGEN_STOP_TIME"),
                                    common_in_obs_gen['PX_TYPE'].alias("OBSGEN_TYPE"),
                                    common_in_obs_gen['LOINC'].alias("OBSGEN_CODE"),
                                    common_in_obs_gen['RESULT_QUAL'].alias("OBSGEN_RESULT_QUAL"),
                                    common_in_obs_gen['RESULT_TEXT'].alias("OBSGEN_RESULT_TEXT"),
                                    common_in_obs_gen['RESULT_NUM'].alias("OBSGEN_RESULT_NUM"),
                                    common_in_obs_gen['RESULT_MODIFIER'].alias("OBSGEN_RESULT_MODIFIER"),
                                    common_in_obs_gen['RESULT_UNIT'].alias("OBSGEN_RESULT_UNIT"),
                                    common_in_obs_gen['TABLE_MODIFIED'].alias("OBSGEN_TABLE_MODIFIED"),
                                    common_in_obs_gen['ID_MODIFIED'].alias("OBSGEN_ID_MODIFIED"),                             
                                    common_in_obs_gen['RESULT_SOURCE'].alias("OBSGEN_SOURCE"),
                                    common_in_obs_gen['ABN_IND'].alias("OBSGEN_ABN_IND"),
                                    common_in_obs_gen['RAW_NAME'].alias("RAW_OBSGEN_NAME"),
                                    common_in_obs_gen['RAW_CODE'].alias("RAW_OBSGEN_CODE"),
                                    common_in_obs_gen['RAW_TYPE'].alias("RAW_OBSGEN_TYPE"),
                                    common_in_obs_gen['RAW_RESULT'].alias("RAW_OBSGEN_RESULT"),
                                    common_in_obs_gen['RAW_UNIT'].alias("RAW_OBSGEN_UNIT"),
                                    common_in_obs_gen['UPDATED'].alias("UPDATED"),
                                    common_in_obs_gen['SOURCE'].alias("SOURCE"),


                                    
    )

    
    cf.write_pyspark_output_file(
                        payspark_df = lab_result_cm_from_df,
                        output_file_name = lab_result_cm_from_df_output_name,
                        output_data_folder_path= fixed_data_folder_work_directory_path)

    cf.write_pyspark_output_file(
                        payspark_df = obs_clin_from_df,
                        output_file_name = obs_clin_from_df_output_name,
                        output_data_folder_path= fixed_data_folder_work_directory_path)


    cf.write_pyspark_output_file(
                        payspark_df = obs_gen_from_df,
                        output_file_name = obs_gen_from_df_output_name,
                        output_data_folder_path= fixed_data_folder_work_directory_path)









cf = CommonFuncitons('NotUsed')

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder", help="Data folder name")
parser.add_argument("-ftm", "--fixed_table_name")
parser.add_argument("-i", "--input_path", help="Input folder path (fixer output folder)")
parser.add_argument("-o", "--output_path", help="Output folder path")
parser.add_argument("-p", "--partner", help="Partner name")
parser.add_argument("-t", "--table", help="Table name (not used for loinc mover fix)", nargs='?', default="loinc")
parser.add_argument("-sr1", "--src1", help="Source 1 (not used)", nargs='?', default="NotUsed")
parser.add_argument("-sr2", "--src2", help="Source 2 (not used)", nargs='?', default="NotUsed")
args = parser.parse_args()

input_data_folder = args.data_folder
input_data_folder_path = args.input_path
output_data_folder_path = args.output_path
input_partner_name = args.partner


spark = cf.get_spark_session("loinc_mover_fix")

this_fix_name = 'common_loinc_mover_fix_2.0'

# Initialize a report list to store counts per step
report_data = []

try:
    cf.print_fixer_status(
        current_count="**",
        total_count="**",
        fix_type="common",
        fix_name=this_fix_name
    )

    fixed_data_folder_path= f'/app/partners/{input_partner_name}/data/{input_data_folder}/fixer_output/'
    fixed_data_folder_work_directory_path= f'{fixed_data_folder_path}loinc_mover_work_directory/'

    lab_result_cm_path = f'/app/partners/{input_partner_name}/data/{input_data_folder}/fixer_output/fixed_lab_result_cm.csv'
    obs_clin_path = f'/app/partners/{input_partner_name}/data/{input_data_folder}/fixer_output/fixed_obs_clin.csv'
    obs_gen_path = f'/app/partners/{input_partner_name}/data/{input_data_folder}/fixer_output/fixed_obs_gen.csv'
    loinc_code_location_path = f'/app/common/cdm/loinc_code_location.csv'

    loinc_code_location            = cf.spark_read(loinc_code_location_path, spark)


################################################################################################################################################
########################################       LAB_RESULT_CM                              ######################################################
################################################################################################################################################
    try:
        lab_result_cm      = cf.spark_read(lab_result_cm_path, spark)
        lab_result_cm_with_no_loinc_found =  lab_result_cm.join(loinc_code_location, loinc_code_location['loinc_num']==lab_result_cm['LAB_LOINC'], how = 'left').filter(col('loinc_num').isNull() )

        cf.write_pyspark_output_file(
                            payspark_df = lab_result_cm_with_no_loinc_found,
                            output_file_name = 'lab_result_cm_no_loinc_found.csv',
                            output_data_folder_path= fixed_data_folder_work_directory_path)
        

        common_from_lab_result = lab_result_cm.select(

                                        lab_result_cm["LAB_RESULT_CM_ID"].alias('ID'),
                                        lab_result_cm["PATID"].alias('PATID'),
                                        lab_result_cm["ENCOUNTERID"].alias('ENCOUNTERID'),
                                        lit('').alias('PROVIDERID'),
                                        lab_result_cm["SPECIMEN_SOURCE"].alias('SPECIMEN_SOURCE'),
                                        lab_result_cm["LAB_LOINC"].alias('LOINC'),
                                        lab_result_cm["LAB_RESULT_SOURCE"].alias('RESULT_SOURCE'),
                                        lab_result_cm["LAB_LOINC_SOURCE"].alias('LOINC_SOURCE'),
                                        lab_result_cm["PRIORITY"].alias('PRIORITY'),
                                        lab_result_cm["RESULT_LOC"].alias('RESULT_LOC'),
                                        lab_result_cm["LAB_PX"].alias('PX'),
                                        lab_result_cm["LAB_PX_TYPE"].alias('PX_TYPE'),
                                        lab_result_cm["LAB_ORDER_DATE"].alias('ORDER_DATE'),
                                        lab_result_cm["SPECIMEN_DATE"].alias('SPECIMEN_DATE'),
                                        lab_result_cm["SPECIMEN_TIME"].alias('SPECIMEN_TIME'),
                                        lab_result_cm["RESULT_DATE"].alias('RESULT_DATE'),
                                        lab_result_cm["RESULT_TIME"].alias('RESULT_TIME'),
                                        lit('').alias('END_DATE'),
                                        lit('').alias('END_TIME'),                                   
                                        lab_result_cm["RESULT_QUAL"].alias('RESULT_QUAL'),
                                        lab_result_cm["RESULT_SNOMED"].alias('RESULT_SNOMED'),
                                        lab_result_cm["RESULT_NUM"].alias('RESULT_NUM'),
                                        lab_result_cm["RESULT_MODIFIER"].alias('RESULT_MODIFIER'),
                                        lab_result_cm["RESULT_UNIT"].alias('RESULT_UNIT'),
                                        lab_result_cm["NORM_RANGE_LOW"].alias('NORM_RANGE_LOW'),
                                        lab_result_cm["NORM_MODIFIER_LOW"].alias('NORM_MODIFIER_LOW'),
                                        lab_result_cm["NORM_RANGE_HIGH"].alias('NORM_RANGE_HIGH'),
                                        lab_result_cm["NORM_MODIFIER_HIGH"].alias('NORM_MODIFIER_HIGH'),                               
                                        lab_result_cm["ABN_IND"].alias('ABN_IND'),
                                        lab_result_cm["RAW_LAB_NAME"].alias('RAW_NAME'),
                                        lab_result_cm["RAW_LAB_CODE"].alias('RAW_CODE'),
                                        lab_result_cm["RAW_PANEL"].alias('RAW_PANEL'),
                                        lab_result_cm["RAW_RESULT"].alias('RAW_RESULT'),
                                        lab_result_cm["RAW_UNIT"].alias('RAW_UNIT'),
                                        lab_result_cm["RAW_ORDER_DEPT"].alias('RAW_ORDER_DEPT'),
                                        lab_result_cm["RAW_FACILITY_CODE"].alias('RAW_FACILITY_CODE'),
                                        lab_result_cm["UPDATED"].alias('UPDATED'),
                                        lab_result_cm["SOURCE"].alias('SOURCE'),
                                        lit('').alias('RESULT_TEXT'),
                                        lit('').alias('RAW_TYPE'),
                                        lit('').alias('RAW_MODIFIER'),
                                        lit('').alias('TABLE_MODIFIED'),
                                        lit('').alias('ID_MODIFIED'),
                                        lit('lab_result_cm').alias('mapped_from'),
                                        

        )


        proces_table(
                    df = common_from_lab_result,
                    df_source_table_name= 'lab_result_cm',
                    loinc_code_location_df= loinc_code_location,
                    fixed_data_folder_work_directory_path = fixed_data_folder_work_directory_path
                    )

    except Exception as e:

            spark.stop()
            cf.print_failure_message(
                                    folder  = input_data_folder,
                                    partner=input_partner_name.lower(),
                                    job='common_loinc_mover_fix_2.0',
                                    text    = str(e))

################################################################################################################################################
########################################       LAB_RESULT_CM                              ######################################################
################################################################################################################################################
    try:
        obs_clin           = cf.spark_read(obs_clin_path, spark)
        obs_clin_with_no_loinc_found =  obs_clin.join(loinc_code_location, loinc_code_location['loinc_num']==obs_clin['OBSCLIN_CODE'], how = 'left').filter(col('loinc_num').isNull() )


        cf.write_pyspark_output_file(
                            payspark_df = obs_clin_with_no_loinc_found,
                            output_file_name = 'obs_clin_with_no_loinc_found.csv',
                            output_data_folder_path= fixed_data_folder_work_directory_path)
        
        common_from_obs_clin = obs_clin.select(

                                        obs_clin["OBSCLINID"].alias('ID'),
                                        obs_clin["PATID"].alias('PATID'),
                                        obs_clin["ENCOUNTERID"].alias('ENCOUNTERID'),
                                        obs_clin["OBSCLIN_PROVIDERID"].alias('PROVIDERID'),
                                        lit('').alias('SPECIMEN_SOURCE'),
                                        obs_clin["OBSCLIN_CODE"].alias('LOINC'),
                                        obs_clin["OBSCLIN_SOURCE"].alias('RESULT_SOURCE'),
                                        lit('').alias('LOINC_SOURCE'),
                                        lit('').alias('PRIORITY'),
                                        lit('').alias('RESULT_LOC'),
                                        lit('').alias('PX'),
                                        obs_clin["OBSCLIN_TYPE"].alias('PX_TYPE'),
                                        lit('').alias('ORDER_DATE'),
                                        lit('').alias('SPECIMEN_DATE'),
                                        lit('').alias('SPECIMEN_TIME'),
                                        obs_clin["OBSCLIN_START_DATE"].alias('RESULT_DATE'),
                                        obs_clin["OBSCLIN_START_TIME"].alias('RESULT_TIME'),
                                        obs_clin["OBSCLIN_STOP_DATE"].alias('END_DATE'),
                                        obs_clin["OBSCLIN_STOP_TIME"].alias('END_TIME'),  
                                        obs_clin["OBSCLIN_RESULT_QUAL"].alias('RESULT_QUAL'),
                                        obs_clin["OBSCLIN_RESULT_SNOMED"].alias('RESULT_SNOMED'),
                                        obs_clin["OBSCLIN_RESULT_NUM"].alias('RESULT_NUM'),
                                        obs_clin["OBSCLIN_RESULT_MODIFIER"].alias('RESULT_MODIFIER'),
                                        obs_clin["OBSCLIN_RESULT_UNIT"].alias('RESULT_UNIT'),
                                        lit('').alias('NORM_RANGE_LOW'),
                                        lit('').alias('NORM_MODIFIER_LOW'),
                                        lit('').alias('NORM_RANGE_HIGH'),
                                        lit('').alias('NORM_MODIFIER_HIGH'),                               
                                        obs_clin["OBSCLIN_ABN_IND"].alias('ABN_IND'),
                                        obs_clin["RAW_OBSCLIN_NAME"].alias('RAW_NAME'),
                                        obs_clin["RAW_OBSCLIN_CODE"].alias('RAW_CODE'),
                                        lit('').alias('RAW_PANEL'),
                                        obs_clin["RAW_OBSCLIN_RESULT"].alias('RAW_RESULT'),
                                        obs_clin["RAW_OBSCLIN_UNIT"].alias('RAW_UNIT'),
                                        lit('').alias('RAW_ORDER_DEPT'),
                                        lit('').alias('RAW_FACILITY_CODE'),
                                        obs_clin["UPDATED"].alias('UPDATED'),
                                        obs_clin["SOURCE"].alias('SOURCE'),
                                        obs_clin["OBSCLIN_RESULT_TEXT"].alias('RESULT_TEXT'),
                                        obs_clin["RAW_OBSCLIN_TYPE"].alias('RAW_TYPE'),
                                        obs_clin["RAW_OBSCLIN_MODIFIER"].alias('RAW_MODIFIER'),
                                        lit('').alias('TABLE_MODIFIED'),
                                        lit('').alias('ID_MODIFIED'),
                                        lit('obs_clin').alias('mapped_from'),
                                        

        )

        proces_table(
                    df = common_from_obs_clin,
                    df_source_table_name= 'obs_clin',
                    loinc_code_location_df= loinc_code_location,
                    fixed_data_folder_work_directory_path = fixed_data_folder_work_directory_path
                    )

    except Exception as e:

        spark.stop()
        cf.print_failure_message(
                                    folder  = input_data_folder,
                                    partner=input_partner_name.lower(),
                                    job='common_loinc_mover_fix_2.0',   
                                    text    = str(e))

################################################################################################################################################
########################################       LAB_RESULT_CM                              ######################################################
################################################################################################################################################

    try:

        obs_gen            = cf.spark_read(obs_gen_path, spark)
        obs_gen_with_no_loinc_found =  obs_gen.join(loinc_code_location, loinc_code_location['loinc_num']==obs_gen['OBSGEN_CODE'], how = 'left').filter(col('loinc_num').isNull() )

        cf.write_pyspark_output_file(
                            payspark_df = obs_gen_with_no_loinc_found,
                            output_file_name = 'obs_gen_with_no_loinc_found.csv',
                            output_data_folder_path= fixed_data_folder_work_directory_path)


        common_from_obs_gen = obs_gen.select(

                                        obs_gen["OBSGENID"].alias('ID'),
                                        obs_gen["PATID"].alias('PATID'),
                                        obs_gen["ENCOUNTERID"].alias('ENCOUNTERID'),
                                        obs_gen["OBSGEN_PROVIDERID"].alias('PROVIDERID'),
                                        lit('').alias('SPECIMEN_SOURCE'),
                                        obs_gen["OBSGEN_CODE"].alias('LOINC'),
                                        obs_gen["OBSGEN_SOURCE"].alias('RESULT_SOURCE'),
                                        lit('').alias('LOINC_SOURCE'),
                                        lit('').alias('PRIORITY'),
                                        lit('').alias('RESULT_LOC'),
                                        lit('').alias('PX'),
                                        obs_gen["OBSGEN_TYPE"].alias('PX_TYPE'),
                                        lit('').alias('ORDER_DATE'),
                                        lit('').alias('SPECIMEN_DATE'),
                                        lit('').alias('SPECIMEN_TIME'),
                                        obs_gen["OBSGEN_START_DATE"].alias('RESULT_DATE'),
                                        obs_gen["OBSGEN_START_TIME"].alias('RESULT_TIME'),
                                        obs_gen["OBSGEN_STOP_DATE"].alias('END_DATE'),
                                        obs_gen["OBSGEN_STOP_TIME"].alias('END_TIME'),  
                                        obs_gen["OBSGEN_RESULT_QUAL"].alias('RESULT_QUAL'),
                                        lit('').alias('RESULT_SNOMED'),
                                        obs_gen["OBSGEN_RESULT_NUM"].alias('RESULT_NUM'),
                                        obs_gen["OBSGEN_RESULT_MODIFIER"].alias('RESULT_MODIFIER'),
                                        obs_gen["OBSGEN_RESULT_UNIT"].alias('RESULT_UNIT'),
                                        lit('').alias('NORM_RANGE_LOW'),
                                        lit('').alias('NORM_MODIFIER_LOW'),
                                        lit('').alias('NORM_RANGE_HIGH'),
                                        lit('').alias('NORM_MODIFIER_HIGH'),                               
                                        obs_gen["OBSGEN_ABN_IND"].alias('ABN_IND'),
                                        obs_gen["RAW_OBSGEN_NAME"].alias('RAW_NAME'),
                                        obs_gen["RAW_OBSGEN_CODE"].alias('RAW_CODE'),
                                        lit('').alias('RAW_PANEL'),
                                        obs_gen["RAW_OBSGEN_RESULT"].alias('RAW_RESULT'),
                                        obs_gen["RAW_OBSGEN_UNIT"].alias('RAW_UNIT'),
                                        lit('').alias('RAW_ORDER_DEPT'),
                                        lit('').alias('RAW_FACILITY_CODE'),
                                        obs_gen["UPDATED"].alias('UPDATED'),
                                        obs_gen["SOURCE"].alias('SOURCE'),
                                        obs_gen["OBSGEN_RESULT_TEXT"].alias('RESULT_TEXT'),
                                        obs_gen["RAW_OBSGEN_TYPE"].alias('RAW_TYPE'),
                                        lit('').alias('RAW_MODIFIER'),
                                        obs_gen["OBSGEN_TABLE_MODIFIED"].alias('TABLE_MODIFIED'),
                                        obs_gen["OBSGEN_ID_MODIFIED"].alias('ID_MODIFIED'),
                                        lit('obs_gen').alias('mapped_from'),
                                        

        )    


        proces_table(
                    df = common_from_obs_gen,
                    df_source_table_name= 'obs_gen',
                    loinc_code_location_df= loinc_code_location,
                    fixed_data_folder_work_directory_path = fixed_data_folder_work_directory_path
                    )
    

    except Exception as e:

        spark.stop()
        cf.print_failure_message(
                                folder  = input_data_folder,
                                partner=input_partner_name.lower(),
                                job='common_loinc_mover_fix_2.0',
                                text    = str(e))





    cf.awk_merge(
        input_directory = fixed_data_folder_work_directory_path,
        output_directory =fixed_data_folder_path,
        prefix = 'lab_result_cm',
        output_name = 'fixed_lab_result_cm.csv'
        )

    cf.awk_merge(
        input_directory = fixed_data_folder_work_directory_path,
        output_directory =fixed_data_folder_path,
        prefix = 'obs_clin',
        output_name = 'fixed_obs_clin.csv'

    )

    cf.awk_merge(
        input_directory = fixed_data_folder_work_directory_path,
        output_directory =fixed_data_folder_path,
        prefix = 'obs_gen',
        output_name = 'fixed_obs_gen.csv'

    )

    cf.delete_directory(
        directory =fixed_data_folder_work_directory_path
    )



    spark.stop()

except Exception as e:
    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job='common_loinc_mover_fix_2.0',
        text    = str(e))