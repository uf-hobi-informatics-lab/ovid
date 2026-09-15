"""Remove unreferenced PROVIDER rows from the PROVIDER table (v1.0).

Deletes rows from the PROVIDER table whose PROVIDERID never appears as a
provider reference in any other PCORnet table that contains a provider-id
column. These unreferenced provider rows are orphans: no ENCOUNTER,
DIAGNOSIS, PROCEDURES, PRESCRIBING, MED_ADMIN, OBS_CLIN, OBS_GEN or
IMMUNIZATION record points back to them.

This is distinct from the `providerid_orphans_fix` common fix, which does
the opposite direction (NULLs out dangling provider references in the
clinical tables); this fix cleans the PROVIDER table itself.

The set of referencing tables and their provider-id columns are supplied as
a comma-separated list of ``table:column`` pairs in ``src1``. Each
referencing table is read from the mapped output folder (``input_path``),
which is stable and present before the fix stage runs.

IMPORTANT SAFETY RULE:
Every table listed in ``src1`` for a partner is a table that partner is
*expected* to deliver. If ANY of those configured tables is missing at fix
time, the fix ABORTS WITHOUT DELETING anything. A missing-but-expected
table could contain the only reference to a provider, so deleting providers
while that table is absent would drop legitimate provider rows. Because no
partner delivers the full universe of provider-referencing tables, ``src1``
is tailored per-partner to exactly the tables that partner delivers.

Behavior
--------
- Verifies every ``src1`` table exists under ``input_path``. If any is
  missing, logs a warning and exits without modifying ``fixed_provider.csv``.
- Reads the provider table being fixed from ``output_path`` (the
  ``fixed_table_name``, e.g. ``fixed_provider.csv``).
- Builds a single distinct set of "referenced" provider ids from the union
  of the provider-id columns across all configured referencing tables.
- Keeps only provider rows whose PROVIDERID is referenced (orphans removed)
  and writes the filtered provider table back to ``output_path``.
- Writes a diagnostics CSV listing the removed orphaned provider ids into
  the fixer diagnostics folder for audit.

Parameters
----------
src1 : str
    Comma-separated ``mapped_<table>.csv:<PROVIDERID_COLUMN>`` pairs, e.g.
    ``mapped_encounter.csv:PROVIDERID,mapped_diagnosis.csv:PROVIDERID,...``.
src2 : str
    Not used by this fix (ignored).
"""

import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from commonFunctions import CommonFuncitons


cf = CommonFuncitons('NotUsed')

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-t", "--table")
parser.add_argument("-p", "--partner")
parser.add_argument("-ftm", "--fixed_table_name")
parser.add_argument("-i", "--input_path")
parser.add_argument("-o", "--output_path")
parser.add_argument("-sr1", "--src1")
parser.add_argument("-sr2", "--src2")

args = parser.parse_args()
input_data_folder: str = args.data_folder
examined_table_name: str = args.table
input_partner_name: str = args.partner
referencing_tables_csv: str = args.src1
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark = cf.get_spark_session("remove_unreferenced_provider_fix")

this_fix_name = 'common_remove_unreferenced_provider_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

PROVIDER_ID_COLUMN = 'PROVIDERID'
REFERENCED_PROVIDER_ID_ALIAS = 'REFERENCED_PROVIDERID'
EXAMINED_PROVIDER_ID_ALIAS = 'TO_BE_EXAMINED_PROVIDERID'


def parse_referencing_tables(referencing_tables_csv: str) -> list[tuple[str, str]]:
    """Parse the comma-separated ``table:column`` pairs from ``src1``.

    Args:
        referencing_tables_csv: Comma-separated ``mapped_<table>.csv:<column>``
            pairs supplied via src1.

    Returns:
        A list of ``(mapped_table_file_name, provider_id_column_name)`` tuples.
    """
    referencing_pairs: list[tuple[str, str]] = []
    for raw_pair in referencing_tables_csv.split(','):
        pair = raw_pair.strip()
        if not pair:
            continue
        table_file, column_name = pair.split(':')
        referencing_pairs.append((table_file.strip(), column_name.strip()))
    return referencing_pairs


def find_missing_referencing_tables(
    referencing_tables: list[tuple[str, str]], input_data_folder_path: str
) -> list[str]:
    """Return the referencing-table files that are absent under the input path.

    Args:
        referencing_tables: ``(table_file, provider_column)`` pairs to check.
        input_data_folder_path: Mapped output folder the tables are read from.

    Returns:
        The list of table file names that do not exist on disk (empty if all
        expected tables are present).
    """
    missing_tables: list[str] = []
    for table_file, _provider_column in referencing_tables:
        if not os.path.exists(input_data_folder_path + table_file):
            missing_tables.append(table_file)
    return missing_tables


def read_referenced_provider_ids(
    referencing_tables: list[tuple[str, str]], input_data_folder_path: str
) -> DataFrame:
    """Build the distinct set of referenced provider ids across all tables.

    Args:
        referencing_tables: ``(table_file, provider_column)`` pairs to union.
        input_data_folder_path: Mapped output folder the tables are read from.

    Returns:
        A single-column DataFrame of distinct, non-null referenced provider
        ids aliased as ``REFERENCED_PROVIDERID``.
    """
    referenced_provider_ids: DataFrame | None = None
    for table_file, provider_column in referencing_tables:
        referencing_table = spark.read.load(
            input_data_folder_path + table_file,
            format="csv", sep="\t", inferSchema="false", header="true", quote='"',
        )
        referenced_column = referencing_table.select(
            referencing_table[provider_column].alias(REFERENCED_PROVIDER_ID_ALIAS)
        ).filter(col(REFERENCED_PROVIDER_ID_ALIAS).isNotNull())

        if referenced_provider_ids is None:
            referenced_provider_ids = referenced_column
        else:
            referenced_provider_ids = referenced_provider_ids.unionByName(
                referenced_column, allowMissingColumns=True
            )

    return referenced_provider_ids.select(REFERENCED_PROVIDER_ID_ALIAS).distinct()


try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=this_fix_name,
    )

    referencing_tables = parse_referencing_tables(referencing_tables_csv)

    missing_tables = find_missing_referencing_tables(
        referencing_tables, input_data_folder_path
    )

    if not referencing_tables:
        cf.print_with_style(
            f"{this_fix_name}: no referencing tables configured in src1 for "
            f"{input_partner_name.lower()}; skipping provider orphan removal.",
            'warning orange',
        )
        spark.stop()
    elif missing_tables:
        cf.print_with_style(
            f"{this_fix_name}: expected referencing table(s) missing for "
            f"{input_partner_name.lower()}: {', '.join(missing_tables)}. "
            f"Aborting without deleting any PROVIDER rows to avoid removing "
            f"providers referenced by an unprocessed table.",
            'warning orange',
        )
        spark.stop()
    else:

        provider_table = spark.read.load(
            output_data_folder_path + fixed_table_name,
            format="csv", sep="\t", inferSchema="false", header="true", quote='"',
        )

        referenced_provider_ids = read_referenced_provider_ids(
            referencing_tables, input_data_folder_path
        )

        provider_ids_to_examine = provider_table.select(
            provider_table[PROVIDER_ID_COLUMN].alias(EXAMINED_PROVIDER_ID_ALIAS)
        ).filter(col(EXAMINED_PROVIDER_ID_ALIAS).isNotNull())

        orphaned_provider_ids = provider_ids_to_examine.subtract(
            referenced_provider_ids.select(
                col(REFERENCED_PROVIDER_ID_ALIAS).alias(EXAMINED_PROVIDER_ID_ALIAS)
            )
        )

        fixed_provider_table = provider_table.join(
            referenced_provider_ids,
            referenced_provider_ids[REFERENCED_PROVIDER_ID_ALIAS]
            == provider_table[PROVIDER_ID_COLUMN],
            how='left_semi',
        )
        fixed_provider_table = fixed_provider_table.select(provider_table.columns)

        diagnostics_path = (
            output_data_folder_path + "fixers_output/" + examined_table_name_parsed
            + "_fixer/" + this_fix_name + "/"
        )
        cf.write_pyspark_output_file(
            payspark_df=orphaned_provider_ids,
            output_file_name=(
                examined_table_name.replace('.csv', '') + "_unreferenced_providerids.csv"
            ),
            output_data_folder_path=diagnostics_path,
        )

        cf.write_pyspark_output_file(
            payspark_df=fixed_provider_table,
            output_file_name=fixed_table_name,
            output_data_folder_path=output_data_folder_path,
        )

        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job=this_fix_name,
        text=str(e),
    )
