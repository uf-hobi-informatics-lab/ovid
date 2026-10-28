"""Single-pass fix chain: run a table's common fixes in one Spark session.

Legacy model (``CommonFuncitons.apply_fix``): one Python subprocess and one
Spark session per fix. Each fix re-read the whole ``fixed_<table>.csv``
(usually twice) and rewrote it as CSV. On a very large table (~850 GB) that
meant about 16 full CSV scans and 8 full CSV rewrites per table, and the stage
needed about 3x the table size of free disk.

Chain model (``run_partner_fixes`` with ``settings.FIX_CHAIN_MODE = True``):

* ``mapped_<table>.csv`` is read once, straight from the mapper output (no
  ``cp`` into the fixer output folder).
* Each supported fix (``fix_transforms.FIX_TRANSFORMS``) is a DataFrame
  transformation. Its diagnostics are written as soon as it runs.
* After fixes that shuffle (orphan joins, admit-date backfill) the state is
  checkpointed to zstd Parquet under ``fixer_output/tmp_chain_<table>/``,
  about 5-10x smaller than the CSV. Only the newest checkpoint is kept.
* The fixed table is written once, at the end, with the atomic parallel
  merge in ``write_pyspark_output_file``.
* Fixes without a transform (custom fixes, other common fixes) still run
  through ``apply_fix`` as a subprocess. The chain flushes its state to
  ``fixed_<table>.csv`` first, then continues from the result.

On any failure no ``fixed_<table>.csv`` is left for upload: either it was
never written, or the intermediate file is quarantined as ``FAILED_...``.

``run_legacy_fix_cli`` is the entry point of the ``common_fixes/<fix>/1.0.py``
scripts. It runs one transform on the current ``fixed_<table>.csv``, using the
same CLI arguments as before.
"""

import argparse
import os
import shutil
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from commonFunctions import CommonFuncitons
from fix_transforms import FIX_TRANSFORMS, Diagnostic, FixContext, FixResult, read_tab_csv
from ovid_logging import get_logger
from settings import FIX_CHAIN_CHECKPOINT_COMPRESSION, FIX_CHAIN_MODE

SUPPORTED_FIX_VERSION: str = "1.0"
SNOMED_MARKER_FILE_NAME: str = ".snomed_pass1.marker"


def _is_chain_supported(fix: dict[str, str]) -> bool:
    """Return True when a fix entry can run in-process as a DataFrame transform.

    Args:
        fix: Partner fixer entry with fix_type, fix_name, version, src1 and src2.

    Returns:
        True for common fixes of version 1.0 that have a transform.
    """
    return (
        fix.get("fix_type") == "common"
        and fix.get("version") == SUPPORTED_FIX_VERSION
        and fix.get("fix_name") in FIX_TRANSFORMS
    )


class _CheckpointStore:
    """Parquet checkpoints of the chain state; only the newest one is kept."""

    def __init__(self, spark: SparkSession, directory: str) -> None:
        """Create (or reset) the checkpoint directory.

        Args:
            spark: Active SparkSession.
            directory: Directory for the checkpoints. Removed and recreated.
        """
        self.spark: SparkSession = spark
        self.directory: str = directory
        self.stage_number: int = 0
        self.live_paths: list[str] = []
        shutil.rmtree(directory, ignore_errors=True)
        os.makedirs(directory, exist_ok=True)

    def materialize(self, data_frame: DataFrame) -> DataFrame:
        """Write a DataFrame to Parquet and return a DataFrame reading it back.

        Older checkpoints are deleted afterwards: the returned DataFrame does
        not depend on them.

        Args:
            data_frame: DataFrame to materialize.

        Returns:
            A DataFrame over the new checkpoint.
        """
        self.stage_number += 1
        stage_path: str = os.path.join(self.directory, f"stage_{self.stage_number:02d}")
        data_frame.write.mode("overwrite").option("compression", FIX_CHAIN_CHECKPOINT_COMPRESSION).parquet(stage_path)
        for old_path in self.live_paths:
            shutil.rmtree(old_path, ignore_errors=True)
        self.live_paths = [stage_path]
        return self.spark.read.parquet(stage_path)

    def cleanup(self) -> None:
        """Delete every checkpoint and the checkpoint directory."""
        shutil.rmtree(self.directory, ignore_errors=True)
        self.live_paths = []


def _write_diagnostics(cf: CommonFuncitons, context: FixContext, diagnostics: list[Diagnostic]) -> None:
    """Write the diagnostics files of one fix.

    Args:
        cf: CommonFuncitons instance.
        context: Chain context.
        diagnostics: Diagnostics to write.
    """
    for diagnostic in diagnostics:
        folder: str = context.diagnostics_directory(diagnostic.fix_label)
        os.makedirs(folder, exist_ok=True)
        if diagnostic.append:
            cf.append_pyspark_output_file(diagnostic.data_frame, diagnostic.file_name, folder)
        else:
            cf.write_pyspark_output_file(
                payspark_df=diagnostic.data_frame,
                output_file_name=diagnostic.file_name,
                output_data_folder_path=folder,
            )


def _apply_transform(cf: CommonFuncitons, context: FixContext, checkpoints: _CheckpointStore,
                     data_frame: DataFrame, fix: dict[str, str], checkpoint_result: bool) -> DataFrame:
    """Apply one fix transform, write its diagnostics, and return the fixed DataFrame.

    Args:
        cf: CommonFuncitons instance.
        context: Chain context.
        checkpoints: Checkpoint store.
        data_frame: Current table.
        fix: Partner fixer entry.
        checkpoint_result: Materialize the result when the fix asks for it.
            False for the last fix: the final CSV write materializes it anyway.

    Returns:
        The fixed DataFrame.
    """
    transform = FIX_TRANSFORMS[fix["fix_name"]]
    result: FixResult = transform(context, data_frame, fix.get("src1", ""), fix.get("src2", ""))
    if result.finish is not None:
        result = result.finish(checkpoints.materialize(result.fixed))
    _write_diagnostics(cf, context, result.diagnostics)
    if result.checkpoint_after and checkpoint_result:
        return checkpoints.materialize(result.fixed)
    return result.fixed


def _remove_stale_outputs(fixer_output_path: str, fixed_table_name: str, parsed_table_name: str) -> None:
    """Delete leftovers of earlier runs for one table before the chain starts.

    Removes the fixed table, its ``FAILED_`` quarantine copy, ``tmp_`` merge
    directories, ``.partial`` merge files and the legacy snomed pass marker.

    Args:
        fixer_output_path: Fixer output folder.
        fixed_table_name: Fixed table file name.
        parsed_table_name: Bare table name, e.g. ``obs_clin``.
    """
    logger = get_logger()
    stale_paths: list[str] = [
        os.path.join(fixer_output_path, fixed_table_name),
        os.path.join(fixer_output_path, f"FAILED_{fixed_table_name}"),
        os.path.join(fixer_output_path, f"tmp_{fixed_table_name}"),
        os.path.join(fixer_output_path, f"{fixed_table_name}.partial"),
        os.path.join(fixer_output_path, "fixers_output", f"{parsed_table_name}_fixer", SNOMED_MARKER_FILE_NAME),
    ]
    for stale_path in stale_paths:
        try:
            if os.path.isdir(stale_path):
                shutil.rmtree(stale_path)
                logger.info(f"Removed stale fixer output {stale_path}")
            elif os.path.lexists(stale_path):
                os.remove(stale_path)
                logger.info(f"Removed stale fixer output {stale_path}")
        except OSError:
            logger.error(f"Could not remove stale fixer output {stale_path}", exc_info=True)


def run_fix_chain(cf: CommonFuncitons, spark: SparkSession, partner_name: str, input_data_folder: str,
                  mapped_table_name: str, fixed_table_name: str, mapper_output_path: str,
                  fixer_output_path: str, fixes: list[dict[str, str]]) -> None:
    """Run every fix of one table and write ``fixed_<table>.csv`` once.

    Args:
        cf: CommonFuncitons instance.
        spark: Active SparkSession.
        partner_name: Partner name, e.g. ``partnera``.
        input_data_folder: Batch folder, e.g. ``PARTNER/20260915``.
        mapped_table_name: Mapped table file name, e.g. ``mapped_obs_clin.csv``.
        fixed_table_name: Fixed table file name, e.g. ``fixed_obs_clin.csv``.
        mapper_output_path: Mapper output folder (trailing slash).
        fixer_output_path: Fixer output folder (trailing slash).
        fixes: Fix entries, in order.

    Raises:
        Exception: The first fatal fix error. Any intermediate fixed table is
            quarantined as ``FAILED_<fixed_table_name>`` first.
    """
    logger = get_logger()
    os.makedirs(fixer_output_path, exist_ok=True)
    context = FixContext(spark=spark, examined_table_name=mapped_table_name,
                         input_path=mapper_output_path, output_path=fixer_output_path)
    _remove_stale_outputs(fixer_output_path, fixed_table_name, context.parsed_table_name)
    checkpoints = _CheckpointStore(spark, os.path.join(fixer_output_path, f"tmp_chain_{fixed_table_name}"))
    fixed_file_path: str = os.path.join(fixer_output_path, fixed_table_name)
    total_fixes: int = len(fixes)
    current_fix_name: str = "fix_chain"

    logger.info(f"Fix chain start: partner={partner_name} folder={input_data_folder} "
                f"table={mapped_table_name} fixes={total_fixes}")
    try:
        current_df: DataFrame = read_tab_csv(spark, mapper_output_path + mapped_table_name)
        for fix_number, fix in enumerate(fixes, start=1):
            current_fix_name = fix["fix_name"]
            if not _is_chain_supported(fix):
                # Flush the chain state, run the fix as a subprocess, continue from its output.
                cf.write_pyspark_output_file(payspark_df=current_df, output_file_name=fixed_table_name,
                                             output_data_folder_path=fixer_output_path)
                checkpoints.cleanup()
                cf.apply_fix(fix_type=fix["fix_type"], fix_name=fix["fix_name"], version=fix["version"],
                             input_path=mapper_output_path, output_path=fixer_output_path,
                             fixed_table_name=fixed_table_name, mapped_table_name=mapped_table_name,
                             input_data_folder=input_data_folder, partner_name=partner_name,
                             src1=fix.get("src1", ""), src2=fix.get("src2", ""))
                checkpoints = _CheckpointStore(spark, checkpoints.directory)
                # Snapshot into Parquet so the final write never reads the file it replaces.
                current_df = checkpoints.materialize(read_tab_csv(spark, fixed_file_path))
                continue

            cf.print_fixer_status(current_count=fix_number, total_count=total_fixes, fix_type="common",
                                  fix_name=f"common_{current_fix_name}_1.0 (chain) {fix.get('src2', '')}".strip())
            try:
                current_df = _apply_transform(cf, context, checkpoints, current_df, fix,
                                              checkpoint_result=fix_number < total_fixes)
            except FileNotFoundError as missing_error:
                # Same as the legacy scripts: a missing optional reference is an
                # orange warning and the fix is skipped.
                logger.warning(f"Fix {current_fix_name} skipped for {mapped_table_name}: {missing_error}")
                cf.print_failure_message(folder=input_data_folder, partner=partner_name.lower(),
                                         job=f"common_{current_fix_name}_1.0", text=str(missing_error))

        cf.write_pyspark_output_file(payspark_df=current_df, output_file_name=fixed_table_name,
                                     output_data_folder_path=fixer_output_path)
        logger.info(f"Fix chain done: partner={partner_name} table={fixed_table_name}")
    except Exception:
        logger.error(f"Fix chain failed at {current_fix_name} for {partner_name} {input_data_folder} "
                     f"{mapped_table_name}", exc_info=True)
        cf._quarantine_failed_fixed_table(fixer_output_path, fixed_table_name, current_fix_name, SUPPORTED_FIX_VERSION)
        raise
    finally:
        checkpoints.cleanup()


def _run_legacy_fix_loop(cf: CommonFuncitons, partner_name: str, input_data_folder: str,
                         mapped_table_name: str, fixed_table_name: str, mapper_output_path: str,
                         fixer_output_path: str, fixes: list[dict[str, str]]) -> None:
    """Run fixes the pre-2026-10-13 way: copy the mapped file, one subprocess per fix.

    Args:
        cf: CommonFuncitons instance.
        partner_name: Partner name.
        input_data_folder: Batch folder.
        mapped_table_name: Mapped table file name.
        fixed_table_name: Fixed table file name.
        mapper_output_path: Mapper output folder.
        fixer_output_path: Fixer output folder.
        fixes: Fix entries, in order.
    """
    cf.copy_data(mapped_table_name, fixed_table_name, mapper_output_path, fixer_output_path)
    for fix in fixes:
        cf.apply_fix(fix_type=fix["fix_type"], fix_name=fix["fix_name"], version=fix["version"],
                     input_path=mapper_output_path, output_path=fixer_output_path,
                     fixed_table_name=fixed_table_name, input_data_folder=input_data_folder,
                     partner_name=partner_name, mapped_table_name=mapped_table_name,
                     src1=fix["src1"], src2=fix["src2"])


def run_partner_fixes(partner_name: str, input_data_folder: str, mapped_table_name: str,
                      fixed_table_name: str, fixes: dict[str, dict[str, str]],
                      chain_mode: Optional[bool] = None) -> None:
    """Entry point for partner ``<table>_fixer.py`` scripts.

    Args:
        partner_name: Partner name, e.g. ``partnera``.
        input_data_folder: Batch folder, e.g. ``PARTNER/20260915``.
        mapped_table_name: Mapped table file name.
        fixed_table_name: Fixed table file name.
        fixes: Partner ``fixes`` dict (``"1": {...}, "2": {...}``), run in key order.
        chain_mode: Override ``settings.FIX_CHAIN_MODE``.

    Raises:
        Exception: When a fix fails. The caller reports it.
    """
    cf = CommonFuncitons(partner_name.upper())
    mapper_output_path: str = f"/app/partners/{partner_name.lower()}/data/{input_data_folder}/mapper_output/"
    fixer_output_path: str = f"/app/partners/{partner_name.lower()}/data/{input_data_folder}/fixer_output/"
    ordered_fixes: list[dict[str, str]] = [fixes[key] for key in sorted(fixes, key=lambda key: int(key))]

    if not os.path.exists(mapper_output_path + mapped_table_name):
        cf.print_with_style(f"{mapper_output_path}{mapped_table_name}: No such file or directory", "warning orange")
        return

    use_chain: bool = FIX_CHAIN_MODE if chain_mode is None else chain_mode
    if not use_chain:
        _run_legacy_fix_loop(cf, partner_name, input_data_folder, mapped_table_name, fixed_table_name,
                             mapper_output_path, fixer_output_path, ordered_fixes)
        return

    spark: SparkSession = cf.get_spark_session(f"{fixed_table_name.replace('.csv', '')}_fix_chain")
    try:
        run_fix_chain(cf, spark, partner_name, input_data_folder, mapped_table_name, fixed_table_name,
                      mapper_output_path, fixer_output_path, ordered_fixes)
    finally:
        spark.stop()


def _legacy_snomed_pass_count(fix_name: str, src1: str, context: FixContext) -> int:
    """Return the snomed pass count already done, using the legacy marker file.

    A standalone ``list_remover_fix`` subprocess cannot see the previous
    subprocess, so the pass-1 / pass-2 state lives in a marker file. Pass 1
    resets the pass-1 diagnostics folder and creates the marker; pass 2
    appends to that file and removes the marker.

    Args:
        fix_name: Fix name.
        src1: Fix src1 (list name).
        context: Fix context.

    Returns:
        0 when this run is pass 1, 1 when it is pass 2.
    """
    if fix_name != "list_remover_fix" or "snomed" not in (src1 or "").lower():
        return 0
    base_directory: str = f"{context.output_path}/fixers_output/{context.parsed_table_name}_fixer"
    marker_path: str = os.path.join(base_directory, SNOMED_MARKER_FILE_NAME)
    if os.path.exists(marker_path):
        os.remove(marker_path)
        return 1
    shutil.rmtree(os.path.join(base_directory, "common_list_remover_snomed_pass1_fix_1.0"), ignore_errors=True)
    os.makedirs(base_directory, exist_ok=True)
    open(marker_path, "w").close()
    return 0


def run_legacy_fix_cli(fix_name: str) -> None:
    """Run one fix on ``fixed_<table>.csv`` with the legacy ``apply_fix`` CLI arguments.

    Args:
        fix_name: Key of ``fix_transforms.FIX_TRANSFORMS``.
    """
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

    cf = CommonFuncitons("NotUsed")
    job_name: str = f"common_{fix_name}_1.0"
    spark: SparkSession = cf.get_spark_session(fix_name)
    context = FixContext(spark=spark, examined_table_name=args.table,
                         input_path=args.input_path, output_path=args.output_path)
    checkpoints: Optional[_CheckpointStore] = None
    try:
        cf.print_fixer_status(current_count="**", total_count="**", fix_type="common", fix_name=job_name)
        context.snomed_pass_count = _legacy_snomed_pass_count(fix_name, args.src1, context)
        checkpoints = _CheckpointStore(spark, os.path.join(args.output_path, f"tmp_chain_{args.fixed_table_name}"))
        input_df: DataFrame = read_tab_csv(spark, args.output_path + args.fixed_table_name)
        fixed_df: DataFrame = _apply_transform(cf, context, checkpoints, input_df,
                                               {"fix_name": fix_name, "src1": args.src1, "src2": args.src2},
                                               checkpoint_result=False)
        cf.write_pyspark_output_file(payspark_df=fixed_df, output_file_name=args.fixed_table_name,
                                     output_data_folder_path=args.output_path)
    except Exception as error:
        get_logger().error(f"{job_name} failed on {args.fixed_table_name}", exc_info=True)
        cf.print_failure_message(folder=args.data_folder, partner=(args.partner or "").lower(),
                                 job=job_name, text=str(error))
    finally:
        if checkpoints is not None:
            checkpoints.cleanup()
        spark.stop()
