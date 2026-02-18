import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from commonFunctions import CommonFuncitons
import argparse
import os
import pyspark.sql.types as T
import subprocess
import shutil

cf = CommonFuncitons('NotUsed')

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder", help="Data folder name")
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

this_fix_name = 'common_loinc_mover_fix_1.0'

# Initialize a report list to store counts per step
report_data = []

try:
    cf.print_fixer_status(
        current_count="**",
        total_count="**",
        fix_type="common",
        fix_name=this_fix_name
    )

    # -------------------------------------
    # Read the required lab_result_cm file
    # -------------------------------------
    lab_path = os.path.join(input_data_folder_path, "fixed_lab_result_cm.csv")
    if not os.path.exists(lab_path):
        raise Exception(f"Required file fixed_lab_result_cm.csv not found at {lab_path}")
    lab_df = spark.read.load(
        lab_path,
        format="csv", sep="\t", inferSchema="false", header="true", quote='"'
    )

    # ----------------------------------------------------------------------------
    # Define schemas for obs_clin and obs_gen (using your new column definitions)
    # ----------------------------------------------------------------------------
    obs_clin_schema = T.StructType([
        T.StructField("OBSCLINID", T.StringType(), True),
        T.StructField("PATID", T.StringType(), True),
        T.StructField("ENCOUNTERID", T.StringType(), True),
        T.StructField("OBSCLIN_PROVIDERID", T.StringType(), True),
        T.StructField("OBSCLIN_START_DATE", T.StringType(), True),
        T.StructField("OBSCLIN_START_TIME", T.StringType(), True),
        T.StructField("OBSCLIN_STOP_DATE", T.StringType(), True),
        T.StructField("OBSCLIN_STOP_TIME", T.StringType(), True),
        T.StructField("OBSCLIN_TYPE", T.StringType(), True),
        T.StructField("OBSCLIN_CODE", T.StringType(), True),
        T.StructField("OBSCLIN_RESULT_QUAL", T.StringType(), True),
        T.StructField("OBSCLIN_RESULT_TEXT", T.StringType(), True),
        T.StructField("OBSCLIN_RESULT_SNOMED", T.StringType(), True),
        T.StructField("OBSCLIN_RESULT_NUM", T.StringType(), True),
        T.StructField("OBSCLIN_RESULT_MODIFIER", T.StringType(), True),
        T.StructField("OBSCLIN_RESULT_UNIT", T.StringType(), True),
        T.StructField("OBSCLIN_SOURCE", T.StringType(), True),
        T.StructField("OBSCLIN_ABN_IND", T.StringType(), True),
        T.StructField("RAW_OBSCLIN_NAME", T.StringType(), True),
        T.StructField("RAW_OBSCLIN_CODE", T.StringType(), True),
        T.StructField("RAW_OBSCLIN_TYPE", T.StringType(), True),
        T.StructField("RAW_OBSCLIN_RESULT", T.StringType(), True),
        T.StructField("RAW_OBSCLIN_MODIFIER", T.StringType(), True),
        T.StructField("RAW_OBSCLIN_UNIT", T.StringType(), True),
        T.StructField("UPDATED", T.StringType(), True),
        T.StructField("SOURCE", T.StringType(), True)
    ])

    obs_gen_schema = T.StructType([
        T.StructField("OBSGENID", T.StringType(), True),
        T.StructField("PATID", T.StringType(), True),
        T.StructField("ENCOUNTERID", T.StringType(), True),
        T.StructField("OBSGEN_PROVIDERID", T.StringType(), True),
        T.StructField("OBSGEN_START_DATE", T.StringType(), True),
        T.StructField("OBSGEN_START_TIME", T.StringType(), True),
        T.StructField("OBSGEN_STOP_DATE", T.StringType(), True),
        T.StructField("OBSGEN_STOP_TIME", T.StringType(), True),
        T.StructField("OBSGEN_TYPE", T.StringType(), True),
        T.StructField("OBSGEN_CODE", T.StringType(), True),
        T.StructField("OBSGEN_RESULT_QUAL", T.StringType(), True),
        T.StructField("OBSGEN_RESULT_TEXT", T.StringType(), True),
        T.StructField("OBSGEN_RESULT_NUM", T.StringType(), True),
        T.StructField("OBSGEN_RESULT_MODIFIER", T.StringType(), True),
        T.StructField("OBSGEN_RESULT_UNIT", T.StringType(), True),
        T.StructField("OBSGEN_TABLE_MODIFIED", T.StringType(), True),
        T.StructField("OBSGEN_ID_MODIFIED", T.StringType(), True),
        T.StructField("OBSGEN_SOURCE", T.StringType(), True),
        T.StructField("OBSGEN_ABN_IND", T.StringType(), True),
        T.StructField("RAW_OBSGEN_NAME", T.StringType(), True),
        T.StructField("RAW_OBSGEN_CODE", T.StringType(), True),
        T.StructField("RAW_OBSGEN_TYPE", T.StringType(), True),
        T.StructField("RAW_OBSGEN_RESULT", T.StringType(), True),
        T.StructField("RAW_OBSGEN_UNIT", T.StringType(), True),
        T.StructField("UPDATED", T.StringType(), True),
        T.StructField("SOURCE", T.StringType(), True)
    ])

    # ---------------------------------------------------------
    # Load or create empty DataFrames for obs_clin and obs_gen
    # ---------------------------------------------------------
    obs_clin_path = os.path.join(input_data_folder_path, "fixed_obs_clin.csv")
    if os.path.exists(obs_clin_path):
        obs_clin_df = spark.read.load(
            obs_clin_path,
            format="csv", sep="\t", inferSchema="false", header="true", quote='"'
        )
    else:
        cf.print_with_style("File fixed_obs_clin.csv not found. A fixed_obs_clin.csv file will be created.", "warning orange")
        obs_clin_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=obs_clin_schema)

    obs_gen_path = os.path.join(input_data_folder_path, "fixed_obs_gen.csv")
    if os.path.exists(obs_gen_path):
        obs_gen_df = spark.read.load(
            obs_gen_path,
            format="csv", sep="\t", inferSchema="false", header="true", quote='"'
        )
    else:
        cf.print_with_style("File fixed_obs_gen.csv not found. A fixed_obs_gen.csv file will be created.", "warning orange")
        obs_gen_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=obs_gen_schema)

    # ----------------------------
    # Read the LOINC mapping file
    # ----------------------------
    loinc_mapping_path = "/app/common/lists/lab_loinc_location.csv"
    loinc_df = spark.read.load(
        loinc_mapping_path,
        format="csv", sep=",", inferSchema="false", header="true", quote='"'
    )

    # ------------------------------------------------
    # LAB_RESULT_CM -> OBS_CLIN (for classtype = '2')
    # ------------------------------------------------
    loinc2 = loinc_df.filter(col("classtype") == "2")
    lab_to_obs_clin = lab_df.join(loinc2, lab_df["LAB_LOINC"] == loinc2["loinc_num"], "inner") \
        .select(
            col("LAB_RESULT_CM_ID").alias("OBSCLINID"),
            col("PATID"),
            col("ENCOUNTERID"),
            lit(None).alias("OBSCLIN_PROVIDERID"),
            col("RESULT_DATE").alias("OBSCLIN_START_DATE"),
            col("RESULT_TIME").alias("OBSCLIN_START_TIME"),
            lit(None).alias("OBSCLIN_STOP_DATE"),
            lit(None).alias("OBSCLIN_STOP_TIME"),
            lit("LC").alias("OBSCLIN_TYPE"),
            col("LAB_LOINC").alias("OBSCLIN_CODE"),
            col("RESULT_QUAL").alias("OBSCLIN_RESULT_QUAL"),
            lit(None).alias("OBSCLIN_RESULT_TEXT"),
            col("RESULT_SNOMED").alias("OBSCLIN_RESULT_SNOMED"),
            col("RESULT_NUM").alias("OBSCLIN_RESULT_NUM"),
            col("RESULT_MODIFIER").alias("OBSCLIN_RESULT_MODIFIER"),
            col("RESULT_UNIT").alias("OBSCLIN_RESULT_UNIT"),
            lit("HC").alias("OBSCLIN_SOURCE"),
            col("ABN_IND").alias("OBSCLIN_ABN_IND"),
            col("RAW_LAB_NAME").alias("RAW_OBSCLIN_NAME"),
            col("RAW_LAB_CODE").alias("RAW_OBSCLIN_CODE"),
            lit(None).alias("RAW_OBSCLIN_TYPE"),
            col("RAW_RESULT").alias("RAW_OBSCLIN_RESULT"),
            lit(None).alias("RAW_OBSCLIN_MODIFIER"),
            col("RAW_UNIT").alias("RAW_OBSCLIN_UNIT"),
            col("UPDATED"),
            col("SOURCE")
        )
    count1 = lab_to_obs_clin.count()
    report_data.append({
        "step": "LAB_RESULT_CM -> OBS_CLIN",
        "source_table": "LAB_RESULT_CM",
        "target_table": "OBS_CLIN",
        "records_moved": count1
    })


    # Remove those records from lab_df
    lab_df = lab_df.join(
        lab_to_obs_clin.select(col("OBSCLINID").alias("LAB_RESULT_CM_ID")),
        on="LAB_RESULT_CM_ID", how="leftanti"
    )
    # Union new records with existing obs_clin; force matching column order
    common_cols_obs_clin = lab_to_obs_clin.columns
    obs_clin_df = obs_clin_df.select(common_cols_obs_clin)
    obs_clin_df = obs_clin_df.union(lab_to_obs_clin)

    # -------------------------------------------------------
    # LAB_RESULT_CM -> OBS_GEN (for classtype in ('3', '4'))
    # -------------------------------------------------------
    loinc34 = loinc_df.filter(col("classtype").isin("3", "4"))
    lab_to_obs_gen = lab_df.join(loinc34, lab_df["LAB_LOINC"] == loinc34["loinc_num"], "inner") \
        .select(
            col("LAB_RESULT_CM_ID").alias("OBSGENID"),
            col("PATID"),
            col("ENCOUNTERID"),
            lit(None).alias("OBSGEN_PROVIDERID"),
            col("RESULT_DATE").alias("OBSGEN_START_DATE"),
            col("RESULT_TIME").alias("OBSGEN_START_TIME"),
            lit(None).alias("OBSGEN_STOP_DATE"),
            lit(None).alias("OBSGEN_STOP_TIME"),
            lit("LC").alias("OBSGEN_TYPE"),
            col("LAB_LOINC").alias("OBSGEN_CODE"),
            col("RESULT_QUAL").alias("OBSGEN_RESULT_QUAL"),
            lit(None).alias("OBSGEN_RESULT_TEXT"),
            col("RESULT_NUM").alias("OBSGEN_RESULT_NUM"),
            col("RESULT_MODIFIER").alias("OBSGEN_RESULT_MODIFIER"),
            col("RESULT_UNIT").alias("OBSGEN_RESULT_UNIT"),
            lit(None).alias("OBSGEN_TABLE_MODIFIED"),
            lit(None).alias("OBSGEN_ID_MODIFIED"),
            lit("HC").alias("OBSGEN_SOURCE"),
            col("ABN_IND").alias("OBSGEN_ABN_IND"),
            col("RAW_LAB_NAME").alias("RAW_OBSGEN_NAME"),
            col("RAW_LAB_CODE").alias("RAW_OBSGEN_CODE"),
            lit(None).alias("RAW_OBSGEN_TYPE"),
            col("RAW_RESULT").alias("RAW_OBSGEN_RESULT"),
            col("RAW_UNIT").alias("RAW_OBSGEN_UNIT"),
            col("UPDATED"),
            col("SOURCE")
        )
    count2 = lab_to_obs_gen.count()
    report_data.append({
        "step": "LAB_RESULT_CM -> OBS_GEN",
        "source_table": "LAB_RESULT_CM",
        "target_table": "OBS_GEN",
        "records_moved": count2
    })

    # Remove those records from lab_df
    lab_df = lab_df.join(
        lab_to_obs_gen.select(col("OBSGENID").alias("LAB_RESULT_CM_ID")),
        on="LAB_RESULT_CM_ID", how="leftanti"
    )
    common_cols_obs_gen = lab_to_obs_gen.columns
    obs_gen_df = obs_gen_df.select(common_cols_obs_gen)
    obs_gen_df = obs_gen_df.union(lab_to_obs_gen)

    # --------------------------------------------------------------------------------
    # STEP 3: OBS_CLIN -> LAB_RESULT_CM (for classtype = '1' and OBSCLIN_TYPE = 'LC')
    # --------------------------------------------------------------------------------
    loinc1 = loinc_df.filter(col("classtype") == "1")
    obs_clin_to_lab = obs_clin_df.join(
        loinc1, obs_clin_df["OBSCLIN_CODE"] == loinc1["loinc_num"], "inner"
    ).filter(col("OBSCLIN_TYPE") == "LC") \
     .select(
        col("OBSCLINID").alias("LAB_RESULT_CM_ID"),
        col("PATID"),
        col("ENCOUNTERID"),
        lit(None).alias("SPECIMEN_SOURCE"),
        col("OBSCLIN_CODE").alias("LAB_LOINC"),
        col("OBSCLIN_SOURCE").alias("LAB_RESULT_SOURCE"),
        lit(None).alias("LAB_LOINC_SOURCE"),
        lit(None).alias("PRIORITY"),
        lit(None).alias("RESULT_LOC"),
        lit(None).alias("LAB_PX"),
        lit(None).alias("LAB_PX_TYPE"),
        lit(None).alias("LAB_ORDER_DATE"),
        lit(None).alias("SPECIMEN_DATE"),
        lit(None).alias("SPECIMEN_TIME"),
        col("OBSCLIN_START_DATE").alias("RESULT_DATE"),
        col("OBSCLIN_START_TIME").alias("RESULT_TIME"),
        col("OBSCLIN_RESULT_QUAL").alias("RESULT_QUAL"),
        col("OBSCLIN_RESULT_SNOMED").alias("RESULT_SNOMED"),
        col("OBSCLIN_RESULT_NUM").alias("RESULT_NUM"),
        col("OBSCLIN_RESULT_MODIFIER").alias("RESULT_MODIFIER"),
        col("OBSCLIN_RESULT_UNIT").alias("RESULT_UNIT"),
        lit(None).alias("NORM_RANGE_LOW"),
        lit(None).alias("NORM_MODIFIER_LOW"),
        lit(None).alias("NORM_RANGE_HIGH"),
        lit(None).alias("NORM_MODIFIER_HIGH"),
        col("OBSCLIN_ABN_IND").alias("ABN_IND"),
        col("RAW_OBSCLIN_NAME").alias("RAW_LAB_NAME"),
        col("RAW_OBSCLIN_CODE").alias("RAW_LAB_CODE"),
        lit(None).alias("RAW_PANEL"),
        col("RAW_OBSCLIN_RESULT").alias("RAW_RESULT"),
        col("RAW_OBSCLIN_UNIT").alias("RAW_UNIT"),
        lit(None).alias("RAW_ORDER_DEPT"),
        lit(None).alias("RAW_FACILITY_CODE"),
        col("UPDATED"),
        col("SOURCE")
    )
    count3 = obs_clin_to_lab.count()
    report_data.append({
        "step": "OBS_CLIN -> LAB_RESULT_CM",
        "source_table": "OBS_CLIN",
        "target_table": "LAB_RESULT_CM",
        "records_moved": count3
    })
    obs_clin_df = obs_clin_df.join(
        obs_clin_to_lab.select(col("LAB_RESULT_CM_ID").alias("OBSCLINID")),
        on="OBSCLINID", how="leftanti"
    )
    lab_df = lab_df.unionByName(obs_clin_to_lab, allowMissingColumns=True)

    # ----------------------------------------------------------------------
    # OBS_GEN -> LAB_RESULT_CM (for classtype = '1' and OBSGEN_TYPE = 'LC')
    # ----------------------------------------------------------------------
    obs_gen_to_lab = obs_gen_df.join(
        loinc1, obs_gen_df["OBSGEN_CODE"] == loinc1["loinc_num"], "inner"
    ).filter(col("OBSGEN_TYPE") == "LC") \
     .select(
        col("OBSGENID").alias("LAB_RESULT_CM_ID"),
        col("PATID"),
        col("ENCOUNTERID"),
        lit(None).alias("SPECIMEN_SOURCE"),
        col("OBSGEN_CODE").alias("LAB_LOINC"),
        col("OBSGEN_SOURCE").alias("LAB_RESULT_SOURCE"),
        lit(None).alias("LAB_LOINC_SOURCE"),
        lit(None).alias("PRIORITY"),
        lit(None).alias("RESULT_LOC"),
        lit(None).alias("LAB_PX"),
        lit(None).alias("LAB_PX_TYPE"),
        lit(None).alias("LAB_ORDER_DATE"),
        lit(None).alias("SPECIMEN_DATE"),
        lit(None).alias("SPECIMEN_TIME"),
        col("OBSGEN_START_DATE").alias("RESULT_DATE"),
        col("OBSGEN_START_TIME").alias("RESULT_TIME"),
        col("OBSGEN_RESULT_QUAL").alias("RESULT_QUAL"),
        lit(None).alias("RESULT_SNOMED"),
        col("OBSGEN_RESULT_NUM").alias("RESULT_NUM"),
        col("OBSGEN_RESULT_MODIFIER").alias("RESULT_MODIFIER"),
        col("OBSGEN_RESULT_UNIT").alias("RESULT_UNIT"),
        lit(None).alias("NORM_RANGE_LOW"),
        lit(None).alias("NORM_MODIFIER_LOW"),
        lit(None).alias("NORM_RANGE_HIGH"),
        lit(None).alias("NORM_MODIFIER_HIGH"),
        col("OBSGEN_ABN_IND").alias("ABN_IND"),
        col("RAW_OBSGEN_NAME").alias("RAW_LAB_NAME"),
        col("RAW_OBSGEN_CODE").alias("RAW_LAB_CODE"),
        lit(None).alias("RAW_PANEL"),
        col("RAW_OBSGEN_RESULT").alias("RAW_RESULT"),
        col("RAW_OBSGEN_UNIT").alias("RAW_UNIT"),
        lit(None).alias("RAW_ORDER_DEPT"),
        lit(None).alias("RAW_FACILITY_CODE"),
        col("UPDATED"),
        col("SOURCE")
    )
    count4 = obs_gen_to_lab.count()
    report_data.append({
        "step": "OBS_GEN -> LAB_RESULT_CM",
        "source_table": "OBS_GEN",
        "target_table": "LAB_RESULT_CM",
        "records_moved": count4
    })
    obs_gen_df = obs_gen_df.join(
        obs_gen_to_lab.select(col("LAB_RESULT_CM_ID").alias("OBSGENID")),
        on="OBSGENID", how="leftanti"
    )
    lab_df = lab_df.unionByName(obs_gen_to_lab, allowMissingColumns=True)

    # -------------------------------------------------------------------------
    # OBS_CLIN -> OBS_GEN (for classtype in ('3','4') and OBSCLIN_TYPE = 'LC')
    # -------------------------------------------------------------------------
    obs_clin_to_obs_gen = obs_clin_df.join(
        loinc34, obs_clin_df["OBSCLIN_CODE"] == loinc34["loinc_num"], "inner"
    ).filter(col("OBSCLIN_TYPE") == "LC") \
     .select(
        col("OBSCLINID").alias("OBSGENID"),
        col("PATID"),
        col("ENCOUNTERID"),
        col("OBSCLIN_PROVIDERID").alias("OBSGEN_PROVIDERID"),
        col("OBSCLIN_START_DATE").alias("OBSGEN_START_DATE"),
        col("OBSCLIN_START_TIME").alias("OBSGEN_START_TIME"),
        col("OBSCLIN_STOP_DATE").alias("OBSGEN_STOP_DATE"),
        col("OBSCLIN_STOP_TIME").alias("OBSGEN_STOP_TIME"),
        col("OBSCLIN_TYPE").alias("OBSGEN_TYPE"),
        col("OBSCLIN_CODE").alias("OBSGEN_CODE"),
        col("OBSCLIN_RESULT_QUAL").alias("OBSGEN_RESULT_QUAL"),
        col("OBSCLIN_RESULT_TEXT").alias("OBSGEN_RESULT_TEXT"),
        col("OBSCLIN_RESULT_NUM").alias("OBSGEN_RESULT_NUM"),
        col("OBSCLIN_RESULT_MODIFIER").alias("OBSGEN_RESULT_MODIFIER"),
        col("OBSCLIN_RESULT_UNIT").alias("OBSGEN_RESULT_UNIT"),
        lit(None).alias("OBSGEN_TABLE_MODIFIED"),
        lit(None).alias("OBSGEN_ID_MODIFIED"),
        col("OBSCLIN_SOURCE").alias("OBSGEN_SOURCE"),
        col("OBSCLIN_ABN_IND").alias("OBSGEN_ABN_IND"),
        col("RAW_OBSCLIN_NAME").alias("RAW_OBSGEN_NAME"),
        col("RAW_OBSCLIN_CODE").alias("RAW_OBSGEN_CODE"),
        col("RAW_OBSCLIN_TYPE").alias("RAW_OBSGEN_TYPE"),
        col("RAW_OBSCLIN_RESULT").alias("RAW_OBSGEN_RESULT"),
        col("RAW_OBSCLIN_UNIT").alias("RAW_OBSGEN_UNIT"),
        col("UPDATED"),
        col("SOURCE")
    )
    count5 = obs_clin_to_obs_gen.count()
    report_data.append({
        "step": "OBS_CLIN -> OBS_GEN",
        "source_table": "OBSCLIN",
        "target_table": "OBS_GEN",
        "records_moved": count5
    })
    obs_clin_df = obs_clin_df.join(
        obs_clin_to_obs_gen.select(col("OBSGENID").alias("OBSCLINID")),
        on="OBSCLINID", how="leftanti"
    )
    obs_gen_df = obs_gen_df.unionByName(obs_clin_to_obs_gen, allowMissingColumns=True)

    # ------------------------------------------
    # Write out the final CSV files to NEW files
    # ------------------------------------------
    cf.write_pyspark_output_file(
        payspark_df=lab_df,
        output_file_name="new_fixed_lab_result_cm.csv",
        output_data_folder_path=output_data_folder_path
    )
    cf.write_pyspark_output_file(
        payspark_df=obs_clin_df,
        output_file_name="new_fixed_obs_clin.csv",
        output_data_folder_path=output_data_folder_path
    )
    cf.write_pyspark_output_file(
        payspark_df=obs_gen_df,
        output_file_name="new_fixed_obs_gen.csv",
        output_data_folder_path=output_data_folder_path
    )
    cf.write_pyspark_output_file(
        payspark_df=spark.createDataFrame(report_data, 
            T.StructType([
                T.StructField("step", T.StringType(), True),
                T.StructField("source_table", T.StringType(), True),
                T.StructField("target_table", T.StringType(), True),
                T.StructField("records_moved", T.StringType(), True)
            ])
        ),
        output_file_name="new_loinc_mover_report.csv",
        output_data_folder_path=output_data_folder_path
    )

    # ----------------------------------------------------------------------
    # Now move the new files to overwrite the existing fixed files
    # ----------------------------------------------------------------------
    files_to_replace = [
        ("new_fixed_lab_result_cm.csv", "fixed_lab_result_cm.csv"),
        ("new_fixed_obs_clin.csv", "fixed_obs_clin.csv"),
        ("new_fixed_obs_gen.csv", "fixed_obs_gen.csv"),
        ("new_loinc_mover_report.csv", "loinc_mover_report.csv")
    ]

    for new_file, fixed_file in files_to_replace:
        new_file_path = os.path.join(output_data_folder_path, new_file)
        fixed_file_path = os.path.join(output_data_folder_path, fixed_file)
        # Remove the fixed file if it exists
        if os.path.exists(fixed_file_path):
            os.remove(fixed_file_path)
        # Move the new file to the fixed file path (this removes the new file)
        shutil.move(new_file_path, fixed_file_path)

    spark.stop()

except Exception as e:
    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job='common_loinc_mover_fix_1.0',
        text    = str(e))
