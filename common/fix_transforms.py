"""DataFrame implementations of the common fixes used by the single-pass fix chain.

Each transform takes the current table as a DataFrame and returns a
``FixResult`` (the fixed DataFrame plus diagnostics DataFrames) instead of
reading and rewriting ``fixed_<table>.csv`` itself. ``fix_chain.run_fix_chain``
strings them together in one Spark session (see that module for the I/O model).

Output file names, diagnostics folders and row semantics match the legacy
``common/common_fixes/<fix>/1.0.py`` scripts, with these deliberate differences:

* ``encounterid_orphans_fix`` / ``providerid_orphans_fix`` join against the
  DISTINCT reference ids, so duplicate ids in the reference table can no
  longer multiply rows.
* ``copy_admit_date_or_delete_fix`` joins the encounter table only for rows
  whose date is actually null (previously the whole table was shuffled).
* ``patterns_remover_fix``: the legacy script built its regex by iterating a
  DataFrame, which yields a Column object, so the regex was
  ``.*Column<'AGAS Reviewed by'>.*`` and never matched anything. That no-op
  behavior is kept unless ``settings.PATTERNS_REMOVER_APPLY_PATTERNS`` is True,
  in which case the patterns are matched literally and case-insensitively.
"""

import os
from dataclasses import dataclass, field
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import StringType

from settings import PATTERNS_REMOVER_APPLY_PATTERNS


_MODULE_LISTS_DIRECTORY: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lists")
COMMON_LISTS_DIRECTORY: str = (
    _MODULE_LISTS_DIRECTORY if os.path.isdir(_MODULE_LISTS_DIRECTORY) else "/app/common/lists"
)

NORM_RANGE_MAX_LENGTH: int = 10
UNKNOWN_MODIFIER_VALUES: tuple[str, ...] = ("", "NI", "UN", "OT")
MODIFIER_FIELDS: tuple[str, ...] = ("RESULT_MODIFIER", "NORM_MODIFIER_LOW", "NORM_MODIFIER_HIGH")
RANGE_FIELDS: tuple[str, ...] = ("NORM_RANGE_LOW", "NORM_RANGE_HIGH")
VALUESET_SENTINEL: str = "OT"
SNOMED_PASS1_FIX_LABEL: str = "common_list_remover_snomed_pass1_fix_1.0"


@dataclass
class FixContext:
    """State shared by all fixes of one table chain.

    Attributes:
        spark: Active SparkSession.
        examined_table_name: Mapped table file name, e.g. ``mapped_obs_clin.csv``.
        input_path: Mapper output folder (reference tables are read from here).
        output_path: Fixer output folder.
        snomed_pass_count: Number of snomed list_remover fixes seen so far.
    """

    spark: SparkSession
    examined_table_name: str
    input_path: str
    output_path: str
    snomed_pass_count: int = 0

    @property
    def table_stem(self) -> str:
        """Return the mapped table name without ``.csv`` (e.g. ``mapped_obs_clin``)."""
        return self.examined_table_name.replace(".csv", "")

    @property
    def parsed_table_name(self) -> str:
        """Return the bare table name (e.g. ``obs_clin``)."""
        return self.examined_table_name.replace("mapped_", "").replace(".csv", "")

    def diagnostics_directory(self, fix_label: str) -> str:
        """Return the diagnostics folder used by the legacy fix scripts.

        Args:
            fix_label: Fix folder name, e.g. ``common_patid_orphans_fix_1.0``.

        Returns:
            The folder path, with a trailing slash.
        """
        return f"{self.output_path}/fixers_output/{self.parsed_table_name}_fixer/{fix_label}/"


@dataclass
class Diagnostic:
    """One diagnostics output of a fix.

    Attributes:
        data_frame: Rows to write.
        file_name: Output file name.
        fix_label: Diagnostics folder name.
        append: Append to an existing file instead of overwriting (snomed pass 2).
    """

    data_frame: DataFrame
    file_name: str
    fix_label: str
    append: bool = False


@dataclass
class FixResult:
    """Result of one fix transform.

    Attributes:
        fixed: The fixed table.
        diagnostics: Diagnostics to write (computed from the pre-fix table).
        checkpoint_after: True when the fix involves a shuffle join, so the
            chain should materialize ``fixed`` before the next fix.
        finish: Optional two-phase form. When set, ``fixed`` is a staging
            frame: the chain materializes it once, then calls
            ``finish(staged)`` to get the real result. This avoids running an
            expensive transform (e.g. a Python UDF) once for the diagnostics
            and again for the fixed table.
    """

    fixed: DataFrame
    diagnostics: list[Diagnostic] = field(default_factory=list)
    checkpoint_after: bool = False
    finish: Optional[Callable[[DataFrame], "FixResult"]] = None


def read_tab_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """Read a tab-delimited CSV with header, all columns as strings.

    Args:
        spark: Active SparkSession.
        file_path: CSV path.

    Returns:
        The DataFrame.

    Raises:
        FileNotFoundError: If the file does not exist (treated as a skipped fix).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Path does not exist: {file_path}")
    return spark.read.load(file_path, format="csv", sep="\t", inferSchema="false", header="true", quote='"')


def read_value_list(spark: SparkSession, list_name: str) -> list[Optional[str]]:
    """Read ``common/lists/<list_name>.csv`` (header-less, one value per line).

    Args:
        spark: Active SparkSession.
        list_name: List name without ``.csv``.

    Returns:
        The values, in file order.

    Raises:
        FileNotFoundError: If the list file does not exist.
    """
    list_path: str = os.path.join(COMMON_LISTS_DIRECTORY, f"{list_name}.csv")
    if not os.path.exists(list_path):
        raise FileNotFoundError(f"Path does not exist: {list_path}")
    rows = spark.read.load(list_path, format="csv", inferSchema="false", header="false", quote='"').collect()
    return [row["_c0"] for row in rows]


def patid_orphans(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Delete rows whose PATID is not in the reference table (src1).

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: Reference table file name in the mapper output, e.g. ``mapped_demographic.csv``.
        src2: PATID column to examine; anything not containing ``PATID`` means ``PATID``.

    Returns:
        The FixResult.
    """
    field_name: str = src2 if src2 and "PATID" in src2 else "PATID"
    source_table: DataFrame = read_tab_csv(context.spark, context.input_path + src1)
    existing_patids: DataFrame = source_table.select(F.col("PATID").alias("SOURCE_PATID")).distinct()
    examined_patids: DataFrame = data_frame.select(F.col(field_name).alias("TO_BE_EXAMINED_PATID")).distinct()
    orphan_patids: DataFrame = examined_patids.join(
        existing_patids,
        examined_patids["TO_BE_EXAMINED_PATID"].eqNullSafe(existing_patids["SOURCE_PATID"]),
        "left_anti",
    )
    fixed: DataFrame = data_frame.join(
        existing_patids, data_frame[field_name] == existing_patids["SOURCE_PATID"], "left_semi"
    )
    diagnostic = Diagnostic(orphan_patids, f"{context.table_stem}_missing_patids.csv", "common_patid_orphans_fix_1.0")
    return FixResult(fixed, [diagnostic], checkpoint_after=True)


def _null_orphan_ids(data_frame: DataFrame, reference: DataFrame, field_name: str,
                     source_field_name: str) -> tuple[DataFrame, DataFrame]:
    """Null out ids that are missing from a reference id set.

    Args:
        data_frame: Current table.
        reference: Single-column DataFrame of distinct reference ids named ``source_field_name``.
        field_name: Column of ``data_frame`` to check.
        source_field_name: Name of the reference id column.

    Returns:
        ``(fixed, missing_id_counts)``.
    """
    joined: DataFrame = data_frame.join(reference, data_frame[field_name] == reference[source_field_name], "left")
    # Aggregate first, then anti-join: shuffles (id, count) pairs instead of
    # whole rows. Null ids never match, so they are reported, as before.
    id_counts: DataFrame = data_frame.groupBy(field_name).agg(F.count("*").alias("count"))
    missing_counts: DataFrame = id_counts.join(
        reference, id_counts[field_name] == reference[source_field_name], "left_anti"
    )
    fixed: DataFrame = joined.withColumn(
        field_name,
        F.when(F.col(source_field_name).isNull(), F.lit(None).cast("string")).otherwise(F.col(field_name)),
    ).select(*data_frame.columns)
    return fixed, missing_counts


def encounterid_orphans(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Set ENCOUNTERID to null when it is not in the encounter table (src1).

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: Encounter table file name, e.g. ``mapped_encounter.csv``.
        src2: Not used.

    Returns:
        The FixResult.
    """
    encounter_table: DataFrame = read_tab_csv(context.spark, context.input_path + src1)
    encounter_ids: DataFrame = encounter_table.select(F.col("ENCOUNTERID").alias("SOURCE_ENCOUNTERID")).distinct()
    fixed, missing_counts = _null_orphan_ids(data_frame, encounter_ids, "ENCOUNTERID", "SOURCE_ENCOUNTERID")
    diagnostic = Diagnostic(missing_counts, f"{context.table_stem}_missing_encounterids_.csv",
                            "common_encounterid_orphans_fix_1.0")
    return FixResult(fixed, [diagnostic], checkpoint_after=True)


def providerid_orphans(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Set a provider id column (src2) to null when it is not in the provider table (src1).

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: Provider table file name, e.g. ``mapped_provider.csv``.
        src2: Provider id column, e.g. ``OBSCLIN_PROVIDERID``.

    Returns:
        The FixResult.
    """
    provider_table: DataFrame = read_tab_csv(context.spark, context.input_path + src1)
    provider_ids: DataFrame = F.broadcast(
        provider_table.select(F.col("PROVIDERID").alias("SOURCE_PROVIDERID")).distinct()
    )
    fixed, missing_counts = _null_orphan_ids(data_frame, provider_ids, src2, "SOURCE_PROVIDERID")
    diagnostic = Diagnostic(missing_counts, f"{context.table_stem}_missing_providerids_.csv",
                            "common_providerid_orphans_fix_1.0")
    return FixResult(fixed, [diagnostic])


def list_remover(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Delete rows whose column src2 is in ``common/lists/<src1>.csv``.

    The first snomed list in a chain writes the ``snomed_pass1`` diagnostics.
    The second one appends to the same file, as the legacy marker logic did.

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: List name, e.g. ``loinc_list`` or ``snomed_pii_list``.
        src2: Column to test.

    Returns:
        The FixResult.
    """
    lowered: str = (src1 or "").lower()
    append: bool = False
    if "loinc" in lowered:
        label: str = "loinc"
    elif "snomed" in lowered:
        context.snomed_pass_count += 1
        label = "snomed_pass1" if context.snomed_pass_count == 1 else "snomed_pass2"
        append = context.snomed_pass_count > 1
    else:
        label = "list"
    fix_label: str = SNOMED_PASS1_FIX_LABEL if append else f"common_list_remover_{label}_fix_1.0"

    values: list[Optional[str]] = read_value_list(context.spark, src1)
    dropped: DataFrame = data_frame.filter(F.col(src2).isin(values))
    cleaned: DataFrame = data_frame.filter(F.col(src2).isNull() | (~F.col(src2).isin(values)))
    diagnostic = Diagnostic(dropped, f"{context.table_stem}_dropped_{src1}.csv", fix_label, append=append)
    return FixResult(cleaned, [diagnostic])


def patterns_remover(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Delete rows whose column src2 contains any pattern in ``common/lists/<src1>.csv``.

    Patterns are literal substrings, matched case-insensitively. Blank lines are ignored.

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: Pattern list name, e.g. ``patterns_list``.
        src2: Column to test, e.g. ``RAW_LAB_NAME``.

    Returns:
        The FixResult.
    """
    diagnostics_label: str = "common_patterns_remover_fix_1.0"
    diagnostics_name: str = f"{context.table_stem}_dropped_patterns.csv"
    if not PATTERNS_REMOVER_APPLY_PATTERNS:
        # Legacy behavior: the 1.0 script never matched anything (see module docstring).
        return FixResult(data_frame, [Diagnostic(data_frame.limit(0), diagnostics_name, diagnostics_label)])

    patterns: list[str] = [
        pattern.strip() for pattern in read_value_list(context.spark, src1)
        if pattern is not None and pattern.strip() and "\\E" not in pattern
    ]
    if not patterns:
        return FixResult(data_frame, [Diagnostic(data_frame.limit(0), diagnostics_name, diagnostics_label)])

    regex_pattern: str = "(?i)(" + "|".join(f"\\Q{pattern}\\E" for pattern in patterns) + ")"
    matches: Column = F.col(src2).rlike(regex_pattern)
    dropped: DataFrame = data_frame.filter(matches)
    cleaned: DataFrame = data_frame.filter(F.col(src2).isNull() | (~matches))
    return FixResult(cleaned, [Diagnostic(dropped, diagnostics_name, diagnostics_label)])


def _any_null(column_names: list[str]) -> Column:
    """Return a Column that is True when any of the given columns is null.

    Args:
        column_names: Column names.

    Returns:
        A boolean Column.
    """
    condition: Column = F.col(column_names[0]).isNull()
    for column_name in column_names[1:]:
        condition = condition | F.col(column_name).isNull()
    return condition


def copy_admit_date_or_delete(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Backfill null date fields from the encounter ADMIT_DATE, else delete the row.

    Only rows with a null date field are joined to the encounter table.

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: Comma-separated date fields, e.g. ``OBSCLIN_START_DATE``.
        src2: Encounter table file name, e.g. ``mapped_encounter.csv``.

    Returns:
        The FixResult.
    """
    date_fields: list[str] = [date_field.strip() for date_field in src1.split(",")]
    encounter_table: DataFrame = read_tab_csv(context.spark, context.input_path + src2)
    encounter_lookup: DataFrame = (
        encounter_table.select("ENCOUNTERID", "ADMIT_DATE")
        .dropDuplicates(["ENCOUNTERID"])
        .withColumnRenamed("ADMIT_DATE", "_ADMIT_DATE_LOOKUP")
    )
    original_columns: list[str] = data_frame.columns
    has_null_date: Column = _any_null(date_fields)

    complete_rows: DataFrame = data_frame.filter(~has_null_date)
    backfilled: DataFrame = data_frame.filter(has_null_date).join(encounter_lookup, on="ENCOUNTERID", how="left")
    for date_field in date_fields:
        backfilled = backfilled.withColumn(date_field, F.coalesce(F.col(date_field), F.col("_ADMIT_DATE_LOOKUP")))

    still_null: Column = _any_null(date_fields)
    bad_rows: DataFrame = backfilled.filter(still_null).select(*original_columns)
    good_rows: DataFrame = backfilled.filter(~still_null).select(*original_columns)
    fixed: DataFrame = complete_rows.unionByName(good_rows)
    diagnostic = Diagnostic(bad_rows, f"{context.table_stem}_null_{'_'.join(date_fields)}_.csv",
                            "copy_admit_date_or_delete_fix_1.0")
    return FixResult(fixed, [diagnostic], checkpoint_after=True)


def delete_records_after_death(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Delete rows whose date field (src1) is after the patient's DEATH_DATE.

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: Date column to examine.
        src2: Death table file name, e.g. ``mapped_death.csv``.

    Returns:
        The FixResult.
    """
    death_table: DataFrame = read_tab_csv(context.spark, context.input_path + src2)
    death_lookup: DataFrame = (
        death_table.select("PATID", "DEATH_DATE")
        .dropDuplicates(["PATID"])
        .withColumnRenamed("DEATH_DATE", "_DEATH_DATE_LOOKUP")
    )
    original_columns: list[str] = data_frame.columns
    joined: DataFrame = data_frame.join(F.broadcast(death_lookup), on="PATID", how="left")
    after_death: Column = (
        F.col("_DEATH_DATE_LOOKUP").isNotNull()
        & F.col(src1).isNotNull()
        & (F.to_date(F.col(src1)) > F.to_date(F.col("_DEATH_DATE_LOOKUP")))
    )
    # A null comparison (unparseable date) keeps the row, as in the legacy fix.
    good_rows: DataFrame = joined.filter(~F.coalesce(after_death, F.lit(False))).select(*original_columns)
    bad_rows: DataFrame = joined.filter(after_death).select(*original_columns)
    diagnostic = Diagnostic(bad_rows, f"{context.table_stem}_after_death_{src1}_.csv",
                            "delete_records_after_death_fix_1.0")
    return FixResult(good_rows, [diagnostic])


def shorten_range_value(value: Optional[str]) -> Optional[str]:
    """Re-format a normal-range bound so it fits in 10 characters.

    Args:
        value: The raw NORM_RANGE_LOW / NORM_RANGE_HIGH string.

    Returns:
        ``value`` unchanged when it already fits; otherwise a plain decimal of
        at most 10 characters, or ``None`` when it cannot fit or is not numeric.
    """
    if value is None or len(value) <= NORM_RANGE_MAX_LENGTH:
        return value
    try:
        number = float(value)
    except ValueError:
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    for decimals in range(6, -1, -1):
        text = f"{number:.{decimals}f}"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        if text == "-0":
            text = "0"
        if len(text) <= NORM_RANGE_MAX_LENGTH:
            return text
    return None


shorten_range_value_udf = F.udf(shorten_range_value, StringType())


def _is_numeric(column: Column) -> Column:
    """Return a Column that is True when the value parses as a number.

    Args:
        column: Column to test.

    Returns:
        A boolean Column (False for NULL / empty / non-numeric).
    """
    return F.coalesce(F.trim(column).cast("double").isNotNull(), F.lit(False))


def result_modifier_normalization(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Normalize RESULT_MODIFIER / NORM_MODIFIER_* and cap NORM_RANGE_* at 10 characters.

    Same rules as ``common_fixes/result_modifier_normalization_fix/1.0.py``.

    Args:
        context: Chain context.
        data_frame: Current lab table.
        src1: Not used.
        src2: Not used.

    Returns:
        The FixResult (diagnostics are aggregated before -> after counts).
    """
    lab_df: DataFrame = data_frame
    for field_name in MODIFIER_FIELDS + RANGE_FIELDS:
        lab_df = lab_df.withColumn(f"_orig_{field_name}", F.col(field_name))

    unknown_modifier: Column = (
        F.col("RESULT_MODIFIER").isNull() | F.upper(F.trim(F.col("RESULT_MODIFIER"))).isin(*UNKNOWN_MODIFIER_VALUES)
    )
    lab_df = lab_df.withColumn(
        "RESULT_MODIFIER",
        F.when(_is_numeric(F.col("RESULT_NUM")) & unknown_modifier, F.lit("EQ")).otherwise(F.col("RESULT_MODIFIER")),
    )
    lab_df = lab_df.withColumn("NORM_RANGE_LOW", shorten_range_value_udf(F.col("NORM_RANGE_LOW"))) \
                   .withColumn("NORM_RANGE_HIGH", shorten_range_value_udf(F.col("NORM_RANGE_HIGH")))

    has_low: Column = _is_numeric(F.col("NORM_RANGE_LOW"))
    has_high: Column = _is_numeric(F.col("NORM_RANGE_HIGH"))
    lab_df = (
        lab_df
        .withColumn("NORM_MODIFIER_LOW", F.when(has_low & has_high, F.lit("EQ")).when(has_low, F.lit("GE"))
                    .when(has_high, F.lit("NO")).otherwise(F.lit("NI")))
        .withColumn("NORM_MODIFIER_HIGH", F.when(has_low & has_high, F.lit("EQ")).when(has_low, F.lit("NO"))
                    .when(has_high, F.lit("LE")).otherwise(F.lit("NI")))
        .withColumn("NORM_RANGE_LOW", F.when(has_low, F.col("NORM_RANGE_LOW")).otherwise(F.lit(None).cast("string")))
        .withColumn("NORM_RANGE_HIGH", F.when(has_high, F.col("NORM_RANGE_HIGH")).otherwise(F.lit(None).cast("string")))
    )

    def finish(staged: DataFrame) -> FixResult:
        """Build the diagnostics and fixed table from the materialized staging frame."""
        range_status = [
            F.when(~F.col(f"_orig_{name}").eqNullSafe(F.col(name)), F.lit("changed")).otherwise(F.lit("same"))
            .alias(f"{name}_STATUS")
            for name in RANGE_FIELDS
        ]
        diagnostics_df: DataFrame = (
            staged.select(
                *[F.col(f"_orig_{name}").alias(f"ORIGINAL_{name}") for name in MODIFIER_FIELDS],
                *[F.col(name).alias(f"NEW_{name}") for name in MODIFIER_FIELDS],
                *range_status,
            )
            .groupBy(
                *[f"ORIGINAL_{name}" for name in MODIFIER_FIELDS],
                *[f"NEW_{name}" for name in MODIFIER_FIELDS],
                *[f"{name}_STATUS" for name in RANGE_FIELDS],
            )
            .count()
            .withColumnRenamed("count", "RECORD_N")
        )
        fixed: DataFrame = staged.drop(*[f"_orig_{name}" for name in MODIFIER_FIELDS + RANGE_FIELDS])
        diagnostic = Diagnostic(diagnostics_df, f"{context.table_stem}_result_modifier_normalized_counts.csv",
                                "common_result_modifier_normalization_fix_1.0")
        return FixResult(fixed, [diagnostic])

    return FixResult(lab_df, [], finish=finish)


def enforce_valueset_to_ot(context: FixContext, data_frame: DataFrame, src1: str, src2: str) -> FixResult:
    """Set populated values of column src2 that are not in valueset src1 to ``OT``.

    Args:
        context: Chain context.
        data_frame: Current table.
        src1: Valueset list name, e.g. ``valueset_result_unit``.
        src2: Target column, e.g. ``RESULT_UNIT``.

    Returns:
        The FixResult (diagnostics hold the changed rows before modification).
    """
    target_column: str = (src2 or "").strip()
    allowed_values: list[Optional[str]] = read_value_list(context.spark, src1)
    out_of_spec: Column = F.col(target_column).isNotNull() & (~F.col(target_column).isin(allowed_values))
    changed_rows: DataFrame = data_frame.filter(out_of_spec)
    fixed: DataFrame = data_frame.withColumn(
        target_column, F.when(out_of_spec, F.lit(VALUESET_SENTINEL)).otherwise(F.col(target_column))
    )
    diagnostic = Diagnostic(
        changed_rows,
        f"{context.table_stem}_{target_column}_set_to_{VALUESET_SENTINEL}_{src1}.csv",
        "common_enforce_valueset_to_ot_fix_1.0",
    )
    return FixResult(fixed, [diagnostic])


FixTransform = Callable[[FixContext, DataFrame, str, str], FixResult]

# Common fixes (version 1.0) that can run inside the single-pass fix chain.
FIX_TRANSFORMS: dict[str, FixTransform] = {
    "patid_orphans_fix": patid_orphans,
    "encounterid_orphans_fix": encounterid_orphans,
    "providerid_orphans_fix": providerid_orphans,
    "list_remover_fix": list_remover,
    "patterns_remover_fix": patterns_remover,
    "copy_admit_date_or_delete_fix": copy_admit_date_or_delete,
    "delete_records_after_death_fix": delete_records_after_death,
    "result_modifier_normalization_fix": result_modifier_normalization,
    "enforce_valueset_to_ot_fix": enforce_valueset_to_ot,
}
