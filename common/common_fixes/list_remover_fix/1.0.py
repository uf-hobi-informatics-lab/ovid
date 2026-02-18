import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from commonFunctions import CommonFuncitons
import argparse
import os, shutil  # NEW

cf = CommonFuncitons('NotUsed')
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-t", "--table")
parser.add_argument("-p", "--partner")
parser.add_argument("-ftm", "--fixed_table_name")
parser.add_argument("-i", "--input_path")
parser.add_argument("-o", "--output_path")
parser.add_argument("-sr1", "--src1")  # list name, e.g., 'loinc_list' or 'snomed_pii_list'
parser.add_argument("-sr2", "--src2")  # column name, e.g., 'OBSGEN_CODE'
args = parser.parse_args()

input_data_folder       = args.data_folder
examined_table_name     = args.table
input_partner_name      = args.partner
source_list_name        = args.src1        # list to drop against
source_column           = args.src2        # column to test membership
input_data_folder_path  = args.input_path
output_data_folder_path = args.output_path
fixed_table_name        = args.fixed_table_name

spark = cf.get_spark_session("list_remover_fix")

examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')

# Run-scoped marker to decide snomed pass1 vs pass2 (avoids old folders affecting label)
base_fixers_dir = f"{output_data_folder_path}/fixers_output/{examined_table_name_parsed}_fixer"
marker_path = os.path.join(base_fixers_dir, ".snomed_pass1.marker")

lower = (source_list_name or "").lower()
if "loinc" in lower:
    label = "loinc"
elif "snomed" in lower:
    if os.path.exists(marker_path):
        label = "snomed_pass2"
    else:
        label = "snomed_pass1"
        # Fresh start for this run: remove any old pass1 dir to avoid stale appends
        pass1_fix_name = "common_list_remover_snomed_pass1_fix_1.0"
        pass1_dir = os.path.join(base_fixers_dir, pass1_fix_name)
        try:
            shutil.rmtree(pass1_dir)
        except FileNotFoundError:
            pass
        os.makedirs(base_fixers_dir, exist_ok=True)
        open(marker_path, "w").close()  # create marker
else:
    label = "list"

this_fix_name = f"common_list_remover_{label}_fix_1.0"

source_list_path = f'common/lists/{source_list_name}.csv'
source_list_df = spark.read.load(source_list_path, format="csv", inferSchema="false", header="false", quote='"')
source_list = [row['_c0'] for row in source_list_df.collect()]

try:
    cf.print_fixer_status(current_count='**', total_count='**', fix_type='common', fix_name=this_fix_name)

    input_table = spark.read.load(
        output_data_folder_path + fixed_table_name,
        format="csv", sep="\t", inferSchema="false", header="true", quote='"'
    )

    dropped_codes = input_table.filter(col(source_column).isin(source_list))
    cleaned_table = input_table.filter((col(source_column).isNull()) | (~col(source_column).isin(source_list)))

    #print("this is the df of dropped codes:")
    #dropped_codes.show()

    final_name = f"{examined_table_name.replace('.csv','')}_dropped_{source_list_name}.csv"

    if label == "snomed_pass2":
        # Append pass2 rows to the pass1 file created in this run
        pass1_fix_name = "common_list_remover_snomed_pass1_fix_1.0"
        pass1_dir = os.path.join(base_fixers_dir, pass1_fix_name)
        os.makedirs(pass1_dir, exist_ok=True)
        pass1_file = os.path.join(pass1_dir, final_name)

        rows = dropped_codes.collect()  # OK for small drop sets
        cols = dropped_codes.columns
        write_header = not os.path.exists(pass1_file)
        with open(pass1_file, "a", encoding="utf-8") as f:
            if write_header:
                f.write("\t".join(cols) + "\n")
            for r in rows:
                f.write("\t".join("" if r[c] is None else str(r[c]) for c in cols) + "\n")

        # Clear marker so a future run starts at pass1 again
        try:
            os.remove(marker_path)
        except FileNotFoundError:
            pass

    else:
        # Original behavior for loinc and snomed_pass1
        cf.write_pyspark_output_file(
            payspark_df=dropped_codes,
            output_file_name=final_name,
            output_data_folder_path=os.path.join(base_fixers_dir, this_fix_name) + "/"
        )

    # Write cleaned table (unchanged)
    cf.write_pyspark_output_file(
        payspark_df=cleaned_table,
        output_file_name=fixed_table_name,
        output_data_folder_path=output_data_folder_path
    )

    spark.stop()

except Exception as e:
    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job=this_fix_name,
        text=str(e)
    )