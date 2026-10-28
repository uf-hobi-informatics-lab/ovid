
###################################################################################################################################
# This class will host all the common functions used in the mappings by diffrenet mapping scripts
###################################################################################################################################



from itertools import chain
from Crypto.Hash import SHA
from Crypto.Cipher import XOR
from base64 import b64decode, b64encode
from pyspark.sql.functions import udf, col, count, desc, upper, round, lit, length, max as spark_max, date_format, Column, to_date, coalesce, when, trim, regexp_replace
from pyspark.sql import SparkSession 
from pyspark.sql.window import Window
from pyspark import SparkConf, SparkContext
import pyspark
import time
import subprocess 
from pyspark.sql.types import StructType, StructField, StringType, NullType, DateType, FloatType, TimestampType, DecimalType, DoubleType
import sys
from ovid_secrets import * 
from settings import *
from pcornet_cdm import PcornetCDM
from pyspark.conf import SparkConf
import pandas as pd
from urllib import parse
from datetime import datetime, timedelta
import pytz
from dateutil import parser as dp
import pyspark.sql.functions as F
import os
import builtins
import importlib
import openpyxl
from PIL import Image
import re
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import glob
import shutil
import math
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl.utils.exceptions import IllegalCharacterError
from functools import reduce, wraps
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.hyperlink import Hyperlink
import fnmatch
import csv
import errno
import tempfile
from typing import Optional, Tuple
from ovid_logging import get_logger as _get_ovid_logger

# Environment variable through which apply_fix() passes each fix subprocess the
# path of its private failure-marker file (see _record_fix_failure).
FIX_FAILURE_MARKER_ENV_VAR: str = 'OVID_FIX_FAILURE_MARKER'


###################################################################################################################################
# Constants used by the raw_distinct_values feature
###################################################################################################################################

# Maximum number of distinct values written per column to the PDF report.
# Full data is always written to per-column CSV files on disk.
RAW_DV_TOP_N: int = 1000

# Maximum distinct values to .collect() to driver for source-value coverage.
# Above this threshold, we fall back to a Spark-side count-only analysis.
RAW_DV_SOURCE_VALUE_COLLECT_LIMIT: int = 1_000_000

# Filename substrings to skip when '-t all' is used. These are OMOP reference
# vocabulary tables, not partner observation data; including them would dominate
# the distinct-values report. The user can still target them by exact name.
RAW_DV_VOCAB_EXCLUSIONS: tuple = (
    "concept_relationship",
    "concept_ancestor",
    "concept_class",
    "concept_synonym",
    "drug_strength",
    "vocabulary",
    "domain",
    "relationship",
)

# Allowed raw file extensions.
RAW_DV_ALLOWED_EXTENSIONS: tuple = (".csv", ".tsv", ".txt")

# PDF report style constants. Black and white only: black text and rules,
# light-grey backgrounds (still grayscale-printable). Reportlab imports are
# guarded so commonFunctions.py still loads when reportlab is absent (the
# raw_distinct_values report generator will fail loudly with a clear error
# pointing the user to rebuild the Docker image).
try:
    from reportlab.lib.pagesizes import letter as _RL_LETTER
    from reportlab.lib import colors as _RL_COLORS
    from reportlab.lib.units import inch as _RL_INCH
    _REPORTLAB_AVAILABLE: bool = True
    RAW_DV_PDF_PAGE_SIZE = _RL_LETTER
    RAW_DV_PDF_MARGIN: float = 0.5 * _RL_INCH
    RAW_DV_PDF_HEADER_BG = _RL_COLORS.HexColor("#E8E8E8")   # 10% grey
    RAW_DV_PDF_ALT_ROW_BG = _RL_COLORS.HexColor("#F7F7F7")  # 5% grey
    RAW_DV_PDF_GRID_COLOR = _RL_COLORS.black
except ImportError:
    _REPORTLAB_AVAILABLE = False
    RAW_DV_PDF_PAGE_SIZE = None
    RAW_DV_PDF_MARGIN = 36.0
    RAW_DV_PDF_HEADER_BG = None
    RAW_DV_PDF_ALT_ROW_BG = None
    RAW_DV_PDF_GRID_COLOR = None

RAW_DV_PDF_BODY_FONT: str = "Helvetica"
RAW_DV_PDF_BODY_FONT_BOLD: str = "Helvetica-Bold"
RAW_DV_PDF_BODY_FONT_ITALIC: str = "Helvetica-Oblique"
RAW_DV_PDF_MONO_FONT: str = "Courier"
RAW_DV_PDF_MONO_FONT_BOLD: str = "Courier-Bold"
RAW_DV_PDF_BODY_FONT_SIZE: int = 9
RAW_DV_PDF_TITLE_FONT_SIZE: int = 18
RAW_DV_PDF_FILE_TITLE_FONT_SIZE: int = 14
RAW_DV_PDF_COL_HEADING_FONT_SIZE: int = 11
RAW_DV_PDF_TABLE_HEADER_FONT_SIZE: int = 9
RAW_DV_PDF_TABLE_BODY_FONT_SIZE: int = 8
RAW_DV_PDF_NARROW_TABLE_FONT_SIZE: int = 7   # used for 7-col concept tables
RAW_DV_PDF_GRID_WIDTH: float = 0.5


###################################################################################################################################
# Retry decorator for resilient Snowflake uploads - handles transient network/DNS failures
###################################################################################################################################
def retry_with_backoff(max_retries=3, base_delay=30, max_delay=300):
    """
    Decorator that retries a function with exponential backoff.
    Useful for handling transient network failures like DNS resolution errors.
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds between retries (default: 30)
        max_delay: Maximum delay in seconds between retries (default: 300)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    retries += 1
                    if retries > max_retries:
                        _get_ovid_logger().warning(f"[RETRY] All {max_retries} retry attempts failed for {func.__name__}")
                        raise
                    delay = min(base_delay * (2 ** (retries - 1)), max_delay)
                    _get_ovid_logger().warning(f"[RETRY] Attempt {retries}/{max_retries} failed: {str(e)[:200]}")
                    _get_ovid_logger().warning(f"[RETRY] Retrying {func.__name__} in {delay} seconds...")
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator





class CommonFuncitons:


    def __init__(self, partner):




        self.partner = partner # specifying the partner / used for the hashing
        self.get_time_from_datetime_udf = udf(self.get_time_from_datetime, StringType())
        self.get_date_from_datetime_udf = udf(self.get_date_from_datetime, StringType())
        self.format_date_udf = udf(self.format_date, StringType())
        self.format_time_udf = udf(self.format_time, StringType())
        self.encrypt_id_udf = self._build_encrypt_id_udf()
        self.get_current_time_udf = udf(self.get_current_time, StringType())
        self.get_date_from_date_str_with_default_value_udf = udf(self.get_date_from_date_str_with_default_value, StringType())
        self.get_datetime_from_date_str_with_default_value_udf = udf(self.get_datetime_from_date_str_with_default_value, StringType())
        self.copy_if_float_udf= udf(self.copy_if_float, StringType())
        self.return_if_float_udf = udf(self.return_if_float, StringType())
        self.get_valid_zip5_udf = udf(self.get_valid_zip5, StringType())
  






    

    


###################################################################################################################################
# This function will return the xor hashed value based on the partner name and input value/id
###################################################################################################################################

    # Partner -> encryption prefix lookup. Constant for the life of the process;
    # hoisted to a class attribute so it is not rebuilt on every encrypt call.
    _PARTNER_ENCRYPTION_VALUE_DICT: dict[str, str] = {
        'AVH': 'AVH',
        'BND': 'BND',
        'CHP': 'CHP',
        'CHT': 'CHT',
        'EMY': 'EMY',
        'FLM': 'FLM',
        'NCH': 'NCH',
        'ORH': 'ORH',
        'ORL': 'ORL',
        'TMH': 'TMH',
        'TMA': 'TMA',
        'TMC': 'TMC',
        'UAB': 'UAB',
        'UFH': 'UFH',
        'UMI': 'UMI',
        'USF': 'USF',
        'TGH': 'TGH',
        'AMS': 'AMS',
        'UCI': 'UCI',
        'UMN': 'UMN',
        "VRT": "VRT",
        "BAY": "BAY",
        'omop_partner_1': 'omop_partner_1',
        'omop_partner_plus': 'omop_partner_plus',
        'pcornert_partner_1': 'pcornert_partner_1',
        'suncoast': 'suncoast',
        'NURSING_UFH': 'UFH',
        'NURSING_AVH': 'AVH',
        'TUMOR_UFH': 'UFH',
    }

    @staticmethod
    def xor_encrypt_exact(data: bytes, key: bytes) -> bytes:
        """Reproduce PyCryptodome's legacy XOR cipher byte-for-byte.

        Args:
            data: Plaintext bytes to encrypt.
            key: Key bytes; truncated to 32 bytes to match PyCryptodome.

        Returns:
            The XOR-ciphered bytes, identical to the retired ``Crypto.Cipher.XOR``
            output for the same inputs.
        """
        # 1. Convert key to bytes
        if not isinstance(key, (bytes, bytearray)):
            key = bytes(key)

        # 2. Truncate key to 32 bytes (PyCryptodome requirement)
        key = key[:32]

        # 3. Convert data to bytes
        if not isinstance(data, (bytes, bytearray)):
            data = data.encode()

        # 4. XOR each byte (identical to underlying C extension)
        key_len = len(key)
        return bytes([data[i] ^ key[i % key_len] for i in range(len(data))])

    def _build_encrypt_id_udf(self):
        """Build the ``encrypt_id`` Spark UDF with per-run constants hoisted out.

        The encryption prefix (partner-derived) and the SHA hashing key
        (seed-file-derived) are constant for the entire run, so they are
        computed exactly once here and captured in the row-level closure.
        This avoids the previous per-row ``open()`` of the seed file plus
        ``SHA.new(...).digest()`` recomputation that ran for every ID value on
        every table (hundreds of millions of filesystem reads on large tables).

        The produced base64 output is byte-for-byte identical to the previous
        implementation: same prefix resolution (``None`` when the partner is not
        in the lookup), same SHA-1 key, same XOR cipher, same base64 encoding.

        Returns:
            A ``pyspark.sql.functions.udf`` returning ``StringType`` that encrypts
            a single id value, returning ``None`` for ``None`` input.
        """
        # Resolve the partner prefix once. Preserve legacy behaviour: an unknown
        # partner yields the literal string "None" in the plaintext prefix.
        partner_encryption_value = self._PARTNER_ENCRYPTION_VALUE_DICT.get(
            self.partner.upper(), None
        )
        prefix = f"{partner_encryption_value}"

        # Read the seed and derive the hashing key once per run.
        encryption_seed_file_path = "/ovid_credentials/" + SEED_FILE_NAME
        with open(encryption_seed_file_path, "r") as seed_file:
            seed = seed_file.read().strip()
        hashing_key = SHA.new(seed.encode('utf-8')).digest()

        xor_encrypt_exact = CommonFuncitons.xor_encrypt_exact

        def _encrypt_id(id: Optional[str]) -> Optional[str]:
            """Encrypt a single id value using the hoisted prefix and key.

            Args:
                id: The raw id to encrypt, or ``None``.

            Returns:
                The base64-encoded ciphertext, or ``None`` when ``id`` is ``None``.
            """
            if id is None:
                return None
            plaintext = f"{prefix}{id}".encode()
            encrypted = xor_encrypt_exact(plaintext, hashing_key)
            return b64encode(encrypted).decode()

        return udf(_encrypt_id, StringType())

    def encrypt_id(cls, id):

            if id is None:

                return None
            else: 


                partner_encryption_value_dict = cls._PARTNER_ENCRYPTION_VALUE_DICT


                encryption_seed_file_path = "/ovid_credentials/" + SEED_FILE_NAME

                with open(encryption_seed_file_path, "r") as f:
                    SEED = f.read().strip()


                partner_encryption_value = partner_encryption_value_dict.get(cls.partner.upper(), None)

                HASHING_KEY = SHA.new(SEED.encode('utf-8')).digest()

                plaintext = f"{partner_encryption_value}{id}".encode()

                encrypted = cls.xor_encrypt_exact(plaintext, HASHING_KEY)

                return b64encode(encrypted).decode()

###################################################################################################################################
# This function will return the current datetime
###################################################################################################################################

   

    @classmethod
    def get_current_time(cls):


        current_utc_datetime = datetime.utcnow()
        new_york_tz = pytz.timezone('America/New_York')

        current_ny_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(new_york_tz)

        return current_ny_datetime.strftime("%Y-%m-%d %H:%M:%S")



###################################################################################################################################
# This function will merge pyspark dataframe into one
###################################################################################################################################

   
    @classmethod
    def awk_merge(cls,input_directory, output_directory, prefix, output_name ):

        output_file_path = output_directory +  output_name
        # Update the awk command to use the correct Python variable for the output path
        command = f"awk 'FNR==1 && NR!=1{{next}} 1' {input_directory}/{prefix}*.csv > {output_file_path}"
        subprocess.run(command, shell=True, check=True)


###################################################################################################################################
# This function will remove a directory
###################################################################################################################################

   
    @classmethod
    def delete_directory(cls,directory ):

        command = ["rm","-r", directory]
        # Execute the command
        subprocess.run(" ".join(command), shell=True)

###################################################################################################################################
# This function will only return a valid zip 5 or a null 
###################################################################################################################################

    @classmethod

    def get_valid_zip5(cls, zip5):
        """
        Returns the zip code if it's a valid 5-digit string, otherwise returns None.
        """
        if isinstance(zip5, str) and re.fullmatch(r"\d{5}", zip5):
            return zip5
        return None 
###################################################################################################################################
# This function will output a pyspark dataframe and combine all the sub files into one file and delete the temprary files
###################################################################################################################################

   
    @classmethod
    def write_pyspark_output_file(cls, payspark_df, output_file_name: str, output_data_folder_path: str) -> None:
        """Write a DataFrame as one tab-delimited CSV file (header + rows).

        Spark writes header-less part files to ``tmp_<output_file_name>``.
        The parts are then concatenated in parallel (kernel-side
        ``copy_file_range``) into ``<output_file_name>.partial``, which is
        atomically renamed onto the final path. Each part is deleted as soon
        as it has been copied, so peak disk use stays near 1x the table size.

        Before 2026-10-13 the parts were merged by one single-threaded
        ``awk ... > output`` redirect. That truncated the target before reading
        (the same file the fix had just read), needed ~2x the table size of
        free disk, and left ``tmp_`` directories behind when it failed ("No space left on device"
        on very large tables).

        Args:
            payspark_df: DataFrame to write. The name is kept for the
                ~700 existing keyword callers (it should be ``pyspark_df``).
            output_file_name: Output file name, e.g. ``fixed_obs_clin.csv``.
            output_data_folder_path: Output folder (with trailing slash).

        Raises:
            Exception: Any Spark write or file-merge error. The final file is
                left untouched, and the temporary directory and partial file
                are removed.
        """
        pyspark_df = payspark_df
        work_directory: str = output_data_folder_path + "tmp_" + output_file_name
        output_file_path: str = os.path.join(output_data_folder_path, output_file_name).replace('tmp_', '')
        partial_file_path: str = output_file_path + PARTIAL_OUTPUT_SUFFIX

        pyspark_df = pyspark_df.select([
            F.lit(None).cast('string').alias(field.name)
            if isinstance(field.dataType, NullType)
            else field.name
            for field in pyspark_df.schema
            ])
        header_line: str = "\t".join(pyspark_df.columns) + "\n"

        try:
            pyspark_df.write.format("csv").option("header", False).option("nullValue", None) \
                .option("delimiter", "\t").option("quote", "").mode("overwrite").save(work_directory)

            part_file_paths: list[str] = sorted(glob.glob(os.path.join(work_directory, "part-*.csv")))
            cls._concatenate_part_files(part_file_paths, partial_file_path, header_line)
            os.replace(partial_file_path, output_file_path)
        finally:
            cls._remove_path_quietly(work_directory)
            cls._remove_path_quietly(partial_file_path)

    @classmethod
    def append_pyspark_output_file(cls, pyspark_df, output_file_name: str, output_data_folder_path: str) -> None:
        """Append a DataFrame's rows to a tab-delimited CSV, creating it if needed.

        Used by the fix chain for the snomed pass-2 diagnostics. These used to
        be ``collect()``-ed to the driver; now they are written by Spark and
        then appended.

        Args:
            pyspark_df: DataFrame to append (same columns as the existing file).
            output_file_name: Target file name.
            output_data_folder_path: Target folder (with trailing slash).

        Raises:
            Exception: Any Spark write or file I/O error.
        """
        target_path: str = os.path.join(output_data_folder_path, output_file_name)
        if not os.path.exists(target_path):
            cls.write_pyspark_output_file(payspark_df=pyspark_df, output_file_name=output_file_name,
                                          output_data_folder_path=output_data_folder_path)
            return
        rows_file_name: str = f"append_{os.getpid()}_{output_file_name}"
        rows_path: str = os.path.join(output_data_folder_path, rows_file_name)
        try:
            cls.write_pyspark_output_file(payspark_df=pyspark_df, output_file_name=rows_file_name,
                                          output_data_folder_path=output_data_folder_path)
            with open(rows_path, 'rb') as rows_file, open(target_path, 'ab') as target_file:
                rows_file.readline()  # skip the header
                shutil.copyfileobj(rows_file, target_file, 64 * 1024 * 1024)
        finally:
            cls._remove_path_quietly(rows_path)

    @classmethod
    def _concatenate_part_files(cls, part_file_paths: list[str], output_file_path: str,
                                header_line: str) -> None:
        """Concatenate Spark part files behind a header line, in parallel.

        Every part is copied straight to its final byte offset with
        ``os.copy_file_range`` (falling back to ``pread``/``pwrite``), using
        ``FIXED_FILE_MERGE_MAX_WORKERS`` threads. Each part is deleted once it
        has been copied.

        Args:
            part_file_paths: Part files, in output order.
            output_file_path: File to create (truncated if it exists).
            header_line: Header text, including the trailing newline.

        Raises:
            OSError: If a read, write, or copy fails (e.g. disk full).
        """
        header_bytes: bytes = header_line.encode('utf-8')
        part_sizes: list[int] = [os.path.getsize(path) for path in part_file_paths]
        part_offsets: list[int] = []
        next_offset: int = len(header_bytes)
        for part_size in part_sizes:
            part_offsets.append(next_offset)
            next_offset += part_size

        output_fd: int = os.open(output_file_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            written: int = 0
            while written < len(header_bytes):
                written += os.write(output_fd, header_bytes[written:])
            os.ftruncate(output_fd, next_offset)

            def copy_part(part_index: int) -> None:
                cls._copy_file_to_offset(part_file_paths[part_index], output_fd,
                                         part_sizes[part_index], part_offsets[part_index])
                os.remove(part_file_paths[part_index])

            if part_file_paths:
                worker_count: int = builtins.min(FIXED_FILE_MERGE_MAX_WORKERS, len(part_file_paths))
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    for future in as_completed([executor.submit(copy_part, index)
                                                for index in range(len(part_file_paths))]):
                        future.result()
        finally:
            os.close(output_fd)

    @classmethod
    def _copy_file_to_offset(cls, source_path: str, output_fd: int, size: int, output_offset: int) -> None:
        """Copy a whole file into an open file descriptor at a given offset.

        Args:
            source_path: File to copy.
            output_fd: Destination file descriptor, opened for writing.
            size: Number of bytes to copy (the size of ``source_path``).
            output_offset: Byte offset in the destination to write at.

        Raises:
            OSError: On read/write failure or if the source is shorter than ``size``.
        """
        chunk_size: int = 64 * 1024 * 1024
        source_fd: int = os.open(source_path, os.O_RDONLY)
        try:
            copied: int = 0
            use_copy_file_range: bool = hasattr(os, 'copy_file_range')
            while copied < size:
                chunk_bytes: int = builtins.min(chunk_size, size - copied)
                if use_copy_file_range:
                    try:
                        transferred: int = os.copy_file_range(source_fd, output_fd, chunk_bytes,
                                                              copied, output_offset + copied)
                    except OSError as copy_error:
                        if copy_error.errno not in (errno.EXDEV, errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP):
                            raise
                        use_copy_file_range = False
                        continue
                else:
                    data: bytes = os.pread(source_fd, chunk_bytes, copied)
                    transferred = os.pwrite(output_fd, data, output_offset + copied) if data else 0
                if transferred <= 0:
                    raise OSError(f"Short copy from {source_path}: copied {copied} of {size} bytes")
                copied += transferred
        finally:
            os.close(source_fd)

    @classmethod
    def _remove_path_quietly(cls, path: str) -> None:
        """Remove a file or directory tree if it exists, logging failures.

        Args:
            path: File or directory to remove.
        """
        try:
            if os.path.isdir(path) and not os.path.islink(path):
                shutil.rmtree(path)
            elif os.path.lexists(path):
                os.remove(path)
        except OSError:
            _get_ovid_logger().error(f"Failed to remove temporary path {path}", exc_info=True)


###################################################################################################################################
# This function will 
###################################################################################################################################

           
    def copy_data(cls,input_file_name,output_file_name,data_folder_path,output_data_folder_path ):

        if not os.path.exists(output_data_folder_path):
            os.makedirs(output_data_folder_path)

        command = ["cp", "-p", os.path.join(data_folder_path, input_file_name), os.path.join(output_data_folder_path,output_file_name)]
                # Execute the command
        subprocess.run(" ".join(command), shell=True)



###################################################################################################################################
# Geocoding reference (ZIP9 -> Census state/county/tract/block, RUCA).
# Ships in the repo gzip-compressed as common/lists/FULL_GEOCODING_<version>.csv.gz (the
# uncompressed CSV is ~430 MB, over GitHub's per-file limit). Spark reads .gz transparently
# and open_geocoding_file() handles it for the pure-Python readers, so decompressing is
# optional: an uncompressed .csv next to it is preferred when present.
###################################################################################################################################

    GEOCODING_FILE_BASENAME = 'FULL_GEOCODING_202603_16'

    @classmethod
    def get_geocoding_path(cls, lists_dir='/app/common/lists'):
        base = os.path.join(lists_dir, cls.GEOCODING_FILE_BASENAME)
        for candidate in (base + '.csv', base + '.csv.gz'):
            if os.path.exists(candidate):
                return candidate
        raise FileNotFoundError(
            f"Geocoding reference not found: expected {base}.csv or {base}.csv.gz "
            "(see common/lists/README.md)."
        )

    @classmethod
    def open_geocoding_file(cls):
        import gzip
        path = cls.get_geocoding_path()
        if path.endswith('.gz'):
            return gzip.open(path, 'rt', newline='')
        return open(path, 'r', newline='')


###################################################################################################################################
# This function will retun the zip to ruca dictionary
###################################################################################################################################

           
    def get_ruca_zip_mapping(cls):

        zip_to_ruca_file_path =  '/app/common/lists/RUCA-codes-2020-zipcode.xlsx'

        zip_to_ruca_file_wb = load_workbook(zip_to_ruca_file_path, data_only=True)

        ruca_2020_zip_code_data_ws = zip_to_ruca_file_wb["RUCA 2020 ZIP Code Data"]
        
        zip_to_ruca = {}



        header = [cell.value for cell in ruca_2020_zip_code_data_ws[2]]

        zip_idx = header.index("ZIPCode") + 1

        ruca_idx = header.index("PrimaryRUCA") + 1


        for row in ruca_2020_zip_code_data_ws.iter_rows(min_row=3, values_only=False):

            zipcode = row[zip_idx - 1].value

            primary_ruca = row[ruca_idx - 1].value

            if zipcode is not None:
                
                zip_to_ruca[str(zipcode).zfill(5)] = primary_ruca

        return zip_to_ruca


###################################################################################################################################
# This function will retun the zip to ruca dictionary
###################################################################################################################################

           
    def get_countyFips_from_county(cls):

        county_fips_path =  '/app/common/lists/RUCA-codes-2020-tract.xlsx'

        zcounty_fips_file_wb = load_workbook(county_fips_path, data_only=True)

        county_fips_ws = zcounty_fips_file_wb["RUCA2020 Tract Data"]
        
        county_to_fips = {}



        header = [cell.value for cell in county_fips_ws[2]]

        county_idx = header.index("CountyName20") + 1

        countyFIPS20_idx = header.index("CountyFIPS20") + 1


        for row in county_fips_ws.iter_rows(min_row=3, values_only=False):

            county = row[county_idx - 1].value

            countyFIPS20 = row[countyFIPS20_idx - 1].value

            if county is not None:
                
                county_to_fips[county.replace(" County","").upper()] = countyFIPS20

        return county_to_fips


##################################################################################################################################
# This function will retun the zip to ruca dictionary
###################################################################################################################################

           
    def get_state_fips_from_zip(cls):


        # Hardcoded state abbreviation to 2-digit FIPS mapping
        # This replaces the fragile UrbanAreaName20 parsing approach
        state_to_fips = {
            "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
            "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
            "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
            "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
            "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
            "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
            "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
            "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
            "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
            "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
            "DC": "11", "PR": "72", "VI": "78", "GU": "66", "AS": "60",
            "MP": "69",
        }


        # getting zip to state from RUCA ZIP code sheet


        zip_to_ruca_file_path =  '/app/common/lists/RUCA-codes-2020-zipcode.xlsx'

        zip_to_ruca_file_wb = load_workbook(zip_to_ruca_file_path, data_only=True)

        ruca_2020_zip_code_data_ws = zip_to_ruca_file_wb["RUCA 2020 ZIP Code Data"]
        
        zip_to_state = {}



        header = [cell.value for cell in ruca_2020_zip_code_data_ws[2]]

        zip_idx = header.index("ZIPCode") + 1

        state_idx = header.index("State") + 1


        for row in ruca_2020_zip_code_data_ws.iter_rows(min_row=3, values_only=False):

            zipcode = row[zip_idx - 1].value

            state = row[state_idx - 1].value

            if zipcode is not None:
                
                zip_to_state[str(zipcode).zfill(5)] = state



        zip_to_fips = {

            z: state_to_fips[s]
            for z, s in zip_to_state.items()
            if s in state_to_fips
        }



        return zip_to_fips


###################################################################################################################################
# This function will return the zip5 to 5-digit county FIPS dictionary from the geocoding CSV
# For ZIP5s that span multiple counties, the most frequent county is selected
###################################################################################################################################

    @classmethod
    def get_countyFips_from_zip(cls):

        import csv
        from collections import Counter

        # .csv or .csv.gz -- see get_geocoding_path()

        zip5_county_counts = {}  # {zip5: Counter({county_fips_5digit: count})}

        with cls.open_geocoding_file() as f:
            reader = csv.DictReader(f)
            for row in reader:
                zip9 = (row.get('ZIP9') or '').strip()
                geocode_state = (row.get('GEOCODE_STATE') or '').strip()
                geocode_county = (row.get('GEOCODE_COUNTY') or '').strip()

                # Only process ZIP9 rows that have valid state and county
                if not zip9 or not geocode_state or not geocode_county:
                    continue

                zip5 = zip9[:5]
                county_fips_5digit = geocode_state + geocode_county  # e.g. "36" + "061" = "36061"

                if zip5 not in zip5_county_counts:
                    zip5_county_counts[zip5] = Counter()
                zip5_county_counts[zip5][county_fips_5digit] += 1

        # For each ZIP5, pick the most common county FIPS
        zip_to_county_fips = {}
        for zip5, counter in zip5_county_counts.items():
            most_common_county = counter.most_common(1)[0][0]
            zip_to_county_fips[zip5] = most_common_county

        return zip_to_county_fips


###################################################################################################################################
# This function will return the zip5 to state abbreviation dictionary from the RUCA ZIP code sheet
###################################################################################################################################

    @classmethod
    def get_state_from_zip(cls):

        zip_to_ruca_file_path = '/app/common/lists/RUCA-codes-2020-zipcode.xlsx'
        zip_to_ruca_file_wb = load_workbook(zip_to_ruca_file_path, data_only=True)
        ruca_2020_zip_code_data_ws = zip_to_ruca_file_wb["RUCA 2020 ZIP Code Data"]

        header = [cell.value for cell in ruca_2020_zip_code_data_ws[2]]
        zip_idx = header.index("ZIPCode") + 1
        state_idx = header.index("State") + 1

        zip_to_state = {}
        for row in ruca_2020_zip_code_data_ws.iter_rows(min_row=3, values_only=False):
            zipcode = row[zip_idx - 1].value
            state = row[state_idx - 1].value
            if zipcode is not None and state is not None:
                zip_to_state[str(zipcode).zfill(5)] = state

        return zip_to_state


###################################################################################################################################
# This function will return the zip5 to city/place name dictionary from the RUCA ZIP code sheet (POName column)
###################################################################################################################################

    @classmethod
    def get_city_from_zip(cls):

        zip_to_ruca_file_path = '/app/common/lists/RUCA-codes-2020-zipcode.xlsx'
        zip_to_ruca_file_wb = load_workbook(zip_to_ruca_file_path, data_only=True)
        ruca_2020_zip_code_data_ws = zip_to_ruca_file_wb["RUCA 2020 ZIP Code Data"]

        header = [cell.value for cell in ruca_2020_zip_code_data_ws[2]]
        zip_idx = header.index("ZIPCode") + 1
        po_idx = header.index("POName") + 1

        zip_to_city = {}
        for row in ruca_2020_zip_code_data_ws.iter_rows(min_row=3, values_only=False):
            zipcode = row[zip_idx - 1].value
            po_name = row[po_idx - 1].value
            if zipcode is not None and po_name is not None:
                zip_to_city[str(zipcode).zfill(5)] = po_name

        return zip_to_city


###################################################################################################################################
# This function will return the zip5 to ZCTA dictionary from the geocoding CSV
# For ZIP5s that span multiple ZCTAs, the most frequent ZCTA is selected
###################################################################################################################################

    @classmethod
    def get_zcta_from_zip(cls):

        import csv
        from collections import Counter

        # .csv or .csv.gz -- see get_geocoding_path()

        zip5_zcta_counts = {}  # {zip5: Counter({zcta: count})}

        with cls.open_geocoding_file() as f:
            reader = csv.DictReader(f)
            for row in reader:
                zip9 = (row.get('ZIP9') or '').strip()
                zcta = (row.get('ZCTA') or '').strip()

                # Only process ZIP9 rows that have a valid ZCTA
                if not zip9 or not zcta:
                    continue

                zip5 = zip9[:5]

                if zip5 not in zip5_zcta_counts:
                    zip5_zcta_counts[zip5] = Counter()
                zip5_zcta_counts[zip5][zcta] += 1

        # For each ZIP5, pick the most common ZCTA
        zip_to_zcta = {}
        for zip5, counter in zip5_zcta_counts.items():
            zip_to_zcta[zip5] = counter.most_common(1)[0][0]

        return zip_to_zcta


###################################################################################################################################
# This function will return the 2-digit state FIPS to 2-letter state abbreviation dictionary
###################################################################################################################################

    @classmethod
    def get_state_abbrev_from_fips(cls):

        fips_to_state = {
            "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
            "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
            "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
            "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
            "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
            "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
            "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
            "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
            "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
            "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
            "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
            "78": "VI",
        }

        return fips_to_state


###################################################################################################################################
# This function will return the 5-digit county FIPS to county name dictionary from the RUCA Tract sheet
# Strips common suffixes like " County", " Parish", " Borough", etc. for cleaner names
###################################################################################################################################

    @classmethod
    def get_countyName_from_fips(cls):

        county_fips_path = '/app/common/lists/RUCA-codes-2020-tract.xlsx'
        wb = load_workbook(county_fips_path, data_only=True)
        ws = wb["RUCA2020 Tract Data"]

        header = [cell.value for cell in ws[2]]
        fips_idx = header.index("CountyFIPS20") + 1
        name_idx = header.index("CountyName20") + 1

        suffixes_to_strip = [
            " City and Borough", " Census Area", " Municipality",
            " Municipio", " Borough", " Parish", " County", " city",
        ]

        fips_to_name = {}
        for row in ws.iter_rows(min_row=3, values_only=False):
            fips_val = row[fips_idx - 1].value
            name_val = row[name_idx - 1].value
            if fips_val is not None and name_val is not None:
                fips_str = str(int(fips_val)).zfill(5)
                clean_name = name_val
                for suffix in suffixes_to_strip:
                    if clean_name.endswith(suffix):
                        clean_name = clean_name[: -len(suffix)]
                        break
                fips_to_name[fips_str] = clean_name

        return fips_to_name


###################################################################################################################################
# This function will return the zip5 to county name dictionary
# Chains: ZIP5 -> county FIPS (from geocoding CSV) -> county name (from RUCA Tract sheet)
# Strips common suffixes like " County", " Parish", " Borough", etc. for cleaner names
###################################################################################################################################

    @classmethod
    def get_county_from_zip(cls):

        # Step 1: ZIP5 -> 5-digit county FIPS (reuse existing function)
        zip_to_fips = cls.get_countyFips_from_zip()

        # Step 2: Build county FIPS -> county name lookup from RUCA Tract sheet
        county_fips_path = '/app/common/lists/RUCA-codes-2020-tract.xlsx'
        wb = load_workbook(county_fips_path, data_only=True)
        ws = wb["RUCA2020 Tract Data"]

        header = [cell.value for cell in ws[2]]
        fips_idx = header.index("CountyFIPS20") + 1
        name_idx = header.index("CountyName20") + 1

        suffixes_to_strip = [
            " City and Borough", " Census Area", " Municipality",
            " Municipio", " Borough", " Parish", " County", " city",
        ]

        fips_to_name = {}
        for row in ws.iter_rows(min_row=3, values_only=False):
            fips_val = row[fips_idx - 1].value
            name_val = row[name_idx - 1].value
            if fips_val is not None and name_val is not None:
                fips_str = str(int(fips_val)).zfill(5)
                # Strip known geographic suffixes for cleaner county names
                clean_name = name_val
                for suffix in suffixes_to_strip:
                    if clean_name.endswith(suffix):
                        clean_name = clean_name[: -len(suffix)]
                        break
                fips_to_name[fips_str] = clean_name

        # Step 3: Chain ZIP5 -> FIPS -> county name
        zip_to_county = {}
        for z, f in zip_to_fips.items():
            if f in fips_to_name:
                zip_to_county[z] = fips_to_name[f]

        return zip_to_county



###################################################################################################################################
# This function will output the mapping gap for the passed formatted file name
###################################################################################################################################

   
    @classmethod
    def deduplicate(cls,partner,file_name,table_name, file_path,folder_name):

        DO_NOT_DEDUPLICATE_LIST = ['TUMOR_DATE']

        spark = cls.get_spark_session('generate mapping report')

        deduplicated_files_path = file_path.replace('formatter', 'deduplicator')
        duplicates_file_path = os.path.join(deduplicated_files_path, 'duplicate_values')
        duplicates_file_name  = file_name.lower()+'_duplicates.csv'
        deduplicated_file_name  = 'deduplicated_'+file_name.lower()+'.csv'

        # Create the deduplicated files directory if it doesn't exist
        if not os.path.exists(deduplicated_files_path):
            os.makedirs(deduplicated_files_path)

        if not os.path.exists(duplicates_file_path):
            os.makedirs(duplicates_file_path)

        input_file = f"{file_path}formatted_{file_name.lower()}.csv"

        df = cls.spark_read(input_file, spark)
        
        row_count = df.count()

        if file_name in DO_NOT_DEDUPLICATE_LIST:

            cls.write_pyspark_output_file(df,deduplicated_file_name, deduplicated_files_path )
            spark.stop()
            return None

        if row_count > AWK_PASSBY_TRESHHOLD :

            spark = cls.get_spark_session('generate mapping report')

            try: 

                if file_name == 'ENROLLMENT':
                                
                    columns_to_consider = [df.columns[0], df.columns[1], df.columns[4]]

                            
                elif file_name == 'DEATH_CAUSE':
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4]]

           
                elif file_name == 'PAT_RELATIONSHIP':
                                
                    columns_to_consider = [df.columns[0], df.columns[1]]

                elif file_name == 'PCORNET_TRIAL':
                                
                    columns_to_consider = [df.columns[0], df.columns[1], df.columns[2]]


                elif file_name == 'FACT_RELATIONSHIP':
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4]]

                elif file_name == 'GOAL':
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4], df.columns[5]]

                elif file_name == 'TREATMENTTEAM_NURSE':
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4], df.columns[5]]

                elif file_name == 'PROBLEM_CONTACT':
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4], df.columns[5]]

                elif file_name == 'INTERVENTION_CONTACT' :
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4], df.columns[5], df.columns[6]]


                elif file_name == 'GOAL_CONTACT' :
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4], df.columns[5], df.columns[6]]


                elif file_name == 'UNIT' :
                                
                    columns_to_consider = [df.columns[0], df.columns[1],df.columns[2],df.columns[3], df.columns[4], df.columns[5], df.columns[6]]  
                elif file_name in ('HASH_TOKEN', 'DEATH', 'VERITAS_EXPANDED') and partner.lower() == 'vrt':

                    columns_to_consider = df.columns  # deduplicate by entire row

                else:
                    columns_to_consider = [df.columns[0]]



                deduplicated_df = df.dropDuplicates(columns_to_consider)

                duplicates_df = df.groupBy(*columns_to_consider).agg(count("*").alias("count")).filter("count > 1")

                cls.write_pyspark_output_file(deduplicated_df,deduplicated_file_name, deduplicated_files_path )
                cls.write_pyspark_output_file(duplicates_df,duplicates_file_name, duplicates_file_path )

                spark.stop()

            except Exception as e:

                spark.stop()
                
                cls.print_with_style(str(e), 'danger red')



        else:

            # Set up the deduplicated files path

            spark.stop()

            deduplicated_output_file = f"{deduplicated_files_path}deduplicated_{file_name.lower()}.csv"
            duplicates_output_file = f"{duplicates_file_path}/{file_name.lower()}_duplicates.csv"



            # Construct the gawk script based on the file_name
            if file_name == 'ENROLLMENT':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $5]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $5] > 1 {{ print > "{duplicates_output_file}" }}
                '''
            
            elif file_name == 'DEATH_CAUSE':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name == 'PAT_RELATIONSHIP':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name == 'PCORNET_TRIAL':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name == 'GOAL':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name == 'TREATMENTTEAM_NURSE':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name == 'FACT_RELATIONSHIP':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5] > 1 {{ print > "{duplicates_output_file}" }}
                '''
            elif file_name == 'PROBLEM_CONTACT':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5, $6]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5, $6] > 1 {{ print > "{duplicates_output_file}" }}
                '''
            elif file_name == 'INTERVENTION_CONTACT' :
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5, $6, $7]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5, $6, $7] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name == 'GOAL_CONTACT' :
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5, $6, $7]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5, $6, $7] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name == 'UNIT' :
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1, $2, $3, $4, $5, $6, $7]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1, $2, $3, $4, $5, $6, $7] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            elif file_name in ('HASH_TOKEN', 'DEATH', 'VERITAS_EXPANDED') and partner.lower() == 'vrt':
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$0]++ {{ print > "{deduplicated_output_file}" }}
                seen[$0] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            else:
                gawk_script = f'''
                BEGIN {{ FS = OFS = "\t" }}
                NR == 1 {{ header = $0; print header > "{deduplicated_output_file}"; print header > "{duplicates_output_file}"; next }}
                !seen[$1]++ {{ print > "{deduplicated_output_file}" }}
                seen[$1] > 1 {{ print > "{duplicates_output_file}" }}
                '''

            # Construct the full shell command
            command = f"awk '{gawk_script}' {input_file}"

            subprocess.run(command, shell=True, check=True)




###################################################################################################################################
# This function will 
###################################################################################################################################

    @classmethod
    def dv_match(cls,partner_name,folder,dv_mapping_table_path,fixed_table_path,table_name,output_path,dv_mapper_output_path ):

            spark = cls.get_spark_session('DV MATCH')


            if table_name != 'PROVIDER':

            

                try:

                    fixed_table      = cls.spark_read(fixed_table_path, spark)
                    dv_mapping_table = cls.spark_read(dv_mapping_table_path, spark)


                    joind_df = fixed_table.join(dv_mapping_table, dv_mapping_table["ENCRYPTED_PATID"] == fixed_table['PATID'], how = 'inner')

                    joind_df.show()

                except Exception as e:
                    cls.print_with_style(str(e), 'danger red')

                finally:
                    spark.stop()
            else:

                spark.stop()





###################################################################################################################################
# This function will 
###################################################################################################################################

    @classmethod

    def datavant_transfer_tokens_to_pcornet(cls,input_file_path, file_name, output_file_path,folder,partner):

        datavant_working_directory = f'/app/partners/{partner}/data/datavant_output/{folder}/'

        if os.path.exists(datavant_working_directory):
            shutil.rmtree(datavant_working_directory)

        os.makedirs(datavant_working_directory)



        matching_files = glob.glob(input_file_path+file_name, recursive=True)
        file_names_only = [os.path.basename(f) for f in matching_files]


        partner_settings_path = "partners."+partner+".partner_settings"
        partner_settings = importlib.import_module(partner_settings_path)


        for file_name in file_names_only:


            with open("/ovid_credentials/" +  DV_CREDENTIALS_FILE_NAME, "rb") as cred_file:
                process = subprocess.Popen(
                    [ 
                        "/ovid_credentials/" + DV_TRANSFORMATION_SCRIPT_FILE_NAME,
                        "transform-tokens",
                        "--to", "pcornet",
                        "--from", partner_settings.DATAVANT_KEY,
                        "-s", DATAVANT_SITE,
                        "-i", input_file_path + file_name,
                        "-o", datavant_working_directory
                    ],
                    stdin=cred_file,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=datavant_working_directory
                )
                stdout, stderr = process.communicate()
                if stdout:
                    _get_ovid_logger().info(f"[DATAVANT stdout] {stdout.decode()}")
                if stderr:
                    _get_ovid_logger().warning(f"[DATAVANT stderr] {stderr.decode()}")

###################################################################################################################################
# This function splits the input file into chunks, runs Datavant in parallel on each chunk,
# and combines the output files into a single result file.
###################################################################################################################################

    @classmethod
    def datavant_transfer_tokens_to_pcornet_parallel(
        cls,
        input_file_path: str,
        file_name: str,
        output_file_path: str,
        folder: str,
        partner: str,
    ) -> None:
        """Run the Datavant token transform in parallel by chunking the input file.

        Parallel version of datavant_transfer_tokens_to_pcornet(). Splits the input
        file into chunks based on file size, runs the Datavant CLI on each chunk
        concurrently using threads, then combines the per-chunk outputs into a single
        result file (``<base>_out<ext>``) in the partner's datavant working directory.

        Chunk sizing is driven by the settings constants HASH_TOKEN_CHUNK_SIZE_TARGET
        and HASH_TOKEN_MAX_CHUNKS: num_chunks = clamp(ceil(file_size / target), 1, max).
        When only a single chunk is needed (small file), it falls back to the
        sequential datavant_transfer_tokens_to_pcornet().

        Args:
            input_file_path: Directory containing the input file.
            file_name: Name of the input file (e.g. 'raw_formatted_hash_token.csv').
            output_file_path: Directory for the final combined output.
            folder: Data folder name (used for path construction).
            partner: Partner name (e.g. 'vrt').

        Returns:
            None.

        Raises:
            FileNotFoundError: If the input file or an expected chunk output is missing.
            ValueError: If the input file has no data rows.
            RuntimeError: If the awk split or a Datavant chunk process fails.
        """

        try:
            _ovid_logger = _get_ovid_logger()
            _ovid_logger.info("[PARALLEL DATAVANT] === ENTERING datavant_transfer_tokens_to_pcornet_parallel ===")
            _ovid_logger.info(f"[PARALLEL DATAVANT] input_file_path = {input_file_path}")
            _ovid_logger.info(f"[PARALLEL DATAVANT] file_name = {file_name}")
            _ovid_logger.info(f"[PARALLEL DATAVANT] output_file_path = {output_file_path}")
            _ovid_logger.info(f"[PARALLEL DATAVANT] folder = {folder}")
            _ovid_logger.info(f"[PARALLEL DATAVANT] partner = {partner}")
            sys.stdout.flush()

            CHUNK_SIZE_TARGET = HASH_TOKEN_CHUNK_SIZE_TARGET
            MAX_CHUNKS = HASH_TOKEN_MAX_CHUNKS

            datavant_working_directory = f'/app/partners/{partner}/data/datavant_output/{folder}/'
            _ovid_logger.info(f"[PARALLEL DATAVANT] datavant_working_directory = {datavant_working_directory}")

            if os.path.exists(datavant_working_directory):
                _ovid_logger.info("[PARALLEL DATAVANT] Removing existing datavant_working_directory")
                shutil.rmtree(datavant_working_directory)
            _ovid_logger.info("[PARALLEL DATAVANT] Creating datavant_working_directory")
            os.makedirs(datavant_working_directory)
            _ovid_logger.info("[PARALLEL DATAVANT] datavant_working_directory created successfully")

            # --- Resolve the input file, supporting glob patterns ---
            # vrt passes a concrete name ('raw_formatted_hash_token.csv'); the
            # Shape-A partners pass a glob (e.g. '*ONEFL_UFH_HASH_TOKENS*.csv').
            # Mirror the sequential function's glob.glob resolution so both work.
            candidate = os.path.join(input_file_path, file_name)
            if os.path.isfile(candidate):
                full_input_file = candidate
            else:
                matches = sorted(glob.glob(candidate, recursive=True))
                matches = [m for m in matches if os.path.isfile(m)]
                if not matches:
                    raise FileNotFoundError(f"Input file not found: {candidate}")
                if len(matches) > 1:
                    _ovid_logger.warning(
                        f"[PARALLEL DATAVANT] Glob '{file_name}' matched {len(matches)} files; "
                        f"using the first: {matches[0]}"
                    )
                full_input_file = matches[0]

            # Work with the resolved concrete file name for chunk / output naming.
            file_name = os.path.basename(full_input_file)
            _ovid_logger.info(f"[PARALLEL DATAVANT] full_input_file = {full_input_file}")

            # --- Determine number of chunks based on file size ---
            file_size = os.path.getsize(full_input_file)
            num_chunks = min(max(1, math.ceil(file_size / CHUNK_SIZE_TARGET)), MAX_CHUNKS)

            _ovid_logger.info(f"[PARALLEL DATAVANT] Input file size: {file_size / (1024*1024):.1f} MB, splitting into {num_chunks} chunk(s)")

            # --- If only 1 chunk, fall back to the original sequential method ---
            if num_chunks == 1:
                _ovid_logger.info("[PARALLEL DATAVANT] File is small enough for single-process execution, using sequential method")
                cls.datavant_transfer_tokens_to_pcornet(input_file_path, file_name, output_file_path, folder, partner)
                return

            # --- Split the file into chunks using awk (stream-based, no memory overhead) ---
            base_name = os.path.splitext(file_name)[0]  # e.g. 'raw_formatted_hash_token'
            ext = os.path.splitext(file_name)[1]          # e.g. '.csv'

            # Count total lines using wc -l (fast, stream-based)
            _ovid_logger.info("[PARALLEL DATAVANT] Counting lines with wc -l...")
            wc_result = subprocess.run(['wc', '-l', full_input_file], capture_output=True, text=True)
            total_lines = int(wc_result.stdout.strip().split()[0])
            total_rows = total_lines - 1  # subtract header
            _ovid_logger.info(f"[PARALLEL DATAVANT] Total lines: {total_lines}, data rows: {total_rows}")

            if total_rows <= 0:
                raise ValueError(f"Input file has no data rows: {full_input_file}")

            rows_per_chunk = math.ceil(total_rows / num_chunks)

            _ovid_logger.info(f"[PARALLEL DATAVANT] rows per chunk: {rows_per_chunk}")

            # Place chunk files under the partner's datavant working directory
            # (guaranteed writable and freshly created above). The input_file_path
            # may be a read-only raw mount (e.g. /data/<folder>/ for Shape-A partners),
            # so we must not write chunks there.
            chunks_dir = os.path.join(datavant_working_directory, 'datavant_chunks')
            _ovid_logger.info(f"[PARALLEL DATAVANT] chunks_dir = {chunks_dir}")
            _ovid_logger.info(f"[PARALLEL DATAVANT] Parent dir exists = {os.path.exists(input_file_path)}")
            _ovid_logger.info(f"[PARALLEL DATAVANT] Parent dir writable = {os.access(input_file_path, os.W_OK)}")
            os.makedirs(chunks_dir, exist_ok=True)
            _ovid_logger.info("[PARALLEL DATAVANT] chunks_dir created successfully")

            # Use awk to split the file in a single streaming pass
            # - Line 1 (header) is saved and prepended to each chunk file
            # - Data rows are distributed evenly across chunk files
            # - Each chunk file is named: base_name_chunk_NNN.ext
            awk_script = (
                'NR==1 { header=$0; next } '
                '{ '
                '  chunk_idx = int((NR-2) / rows_per_chunk); '
                '  if (chunk_idx >= num_chunks) chunk_idx = num_chunks - 1; '
                '  fname = chunks_dir "/" base "_chunk_" sprintf("%03d", chunk_idx+1) ext; '
                '  if (!(fname in opened)) { '
                '    print header > fname; '
                '    opened[fname] = 1; '
                '  } '
                '  print >> fname; '
                '}'
            )

            awk_cmd = [
                'awk',
                '-v', f'num_chunks={num_chunks}',
                '-v', f'chunks_dir={chunks_dir}',
                '-v', f'base={base_name}',
                '-v', f'ext={ext}',
                '-v', f'rows_per_chunk={rows_per_chunk}',
                awk_script,
                full_input_file
            ]

            _ovid_logger.info(f"[PARALLEL DATAVANT] Running awk to split file into {num_chunks} chunks...")
            awk_result = subprocess.run(awk_cmd, capture_output=True, text=True)
            if awk_result.returncode != 0:
                raise RuntimeError(f"awk split failed (rc={awk_result.returncode}): {awk_result.stderr}")
            _ovid_logger.info("[PARALLEL DATAVANT] awk split completed successfully")

            # Build the list of chunk file paths (only those that were actually created)
            chunk_input_paths = []
            for i in range(num_chunks):
                chunk_file_name = f"{base_name}_chunk_{i+1:03d}{ext}"
                chunk_file_path = os.path.join(chunks_dir, chunk_file_name)
                if os.path.isfile(chunk_file_path):
                    chunk_size = os.path.getsize(chunk_file_path)
                    chunk_input_paths.append(chunk_file_path)
                    _ovid_logger.info(f"[PARALLEL DATAVANT] Chunk {i+1}: {chunk_file_path} ({chunk_size / (1024*1024):.1f} MB)")

            actual_num_chunks = len(chunk_input_paths)
            _ovid_logger.info(f"[PARALLEL DATAVANT] Created {actual_num_chunks} chunk file(s)")

            # --- Load partner settings for the Datavant key ---
            partner_settings_path = "partners." + partner + ".partner_settings"
            partner_settings = importlib.import_module(partner_settings_path)

            # --- Prepare worker arguments: each chunk gets its own output subdirectory ---
            chunk_output_dirs = []
            for i in range(actual_num_chunks):
                chunk_output_dir = os.path.join(datavant_working_directory, f'chunk_{i+1:03d}')
                chunk_output_dirs.append(chunk_output_dir)

            # --- Define the worker function for running Datavant on a single chunk ---
            def run_datavant_on_chunk(chunk_input_path, chunk_output_dir):
                """Runs the Datavant CLI on a single chunk file. Thread-safe since work is in subprocess."""
                os.makedirs(chunk_output_dir, exist_ok=True)

                with open("/ovid_credentials/" + DV_CREDENTIALS_FILE_NAME, "rb") as cred_file:
                    process = subprocess.Popen(
                        [
                            "/ovid_credentials/" + DV_TRANSFORMATION_SCRIPT_FILE_NAME,
                            "transform-tokens",
                            "--to", "pcornet",
                            "--from", partner_settings.DATAVANT_KEY,
                            "-s", DATAVANT_SITE,
                            "-i", chunk_input_path,
                            "-o", chunk_output_dir
                        ],
                        stdin=cred_file,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=chunk_output_dir
                    )
                    stdout, stderr = process.communicate()

                return (chunk_output_dir, process.returncode, stdout, stderr)

            # --- Run Datavant in parallel using threads ---
            _ovid_logger.info(f"[PARALLEL DATAVANT] Launching {actual_num_chunks} parallel Datavant processes...")
            futures = {}
            with ThreadPoolExecutor(max_workers=actual_num_chunks) as executor:
                for i in range(actual_num_chunks):
                    future = executor.submit(run_datavant_on_chunk, chunk_input_paths[i], chunk_output_dirs[i])
                    futures[future] = i

                results = [None] * actual_num_chunks
                for future in as_completed(futures):
                    idx = futures[future]
                    results[idx] = future.result()
                    _ovid_logger.info(f"[PARALLEL DATAVANT] Thread {idx+1}/{actual_num_chunks} completed")

            # --- Report results ---
            for chunk_output_dir, returncode, stdout, stderr in results:
                if stdout:
                    _ovid_logger.info(f"[PARALLEL DATAVANT stdout] {stdout.decode()}")
                if stderr:
                    _ovid_logger.warning(f"[PARALLEL DATAVANT stderr] {stderr.decode()}")
                if returncode != 0:
                    raise RuntimeError(
                        f"Datavant process failed for chunk in {chunk_output_dir} with return code {returncode}"
                    )

            _ovid_logger.info("[PARALLEL DATAVANT] All Datavant processes completed successfully")

            # --- Combine output files into a single result ---
            # Datavant appends '_out' before the extension, e.g. 'raw_formatted_hash_token_chunk_001_out.csv'
            output_combined_file = os.path.join(datavant_working_directory, f"{base_name}_out{ext}")

            with open(output_combined_file, 'w') as out_f:
                header_written = False
                for i, chunk_output_dir in enumerate(chunk_output_dirs):
                    chunk_base = os.path.splitext(os.path.basename(chunk_input_paths[i]))[0]
                    chunk_out_name = f"{chunk_base}_out{ext}"
                    chunk_out_path = os.path.join(chunk_output_dir, chunk_out_name)

                    if not os.path.isfile(chunk_out_path):
                        raise FileNotFoundError(
                            f"Expected Datavant output file not found: {chunk_out_path}"
                        )

                    with open(chunk_out_path, 'r') as chunk_out_f:
                        chunk_header = chunk_out_f.readline()
                        if not header_written:
                            out_f.write(chunk_header)
                            header_written = True
                        # Write remaining lines (data rows)
                        for line in chunk_out_f:
                            out_f.write(line)

            _ovid_logger.info(f"[PARALLEL DATAVANT] Combined output written to {output_combined_file}")

            # --- Cleanup: remove chunk input files and chunk output subdirectories ---
            shutil.rmtree(chunks_dir, ignore_errors=True)
            for chunk_output_dir in chunk_output_dirs:
                shutil.rmtree(chunk_output_dir, ignore_errors=True)

            _ovid_logger.info("[PARALLEL DATAVANT] Cleanup complete")

        except Exception as e:
            _ovid_logger.error(f"[PARALLEL DATAVANT] FATAL ERROR: {e}", exc_info=True)
            sys.stdout.flush()
            sys.stderr.flush()
            raise

###################################################################################################################################
# Datavant "to datavant" export
#
# Runs two Datavant transform-tokens invocations back-to-back and drops a
# timestamped, source-tagged result file into
#   DATAVANT_MATCH_INBOX_DIR
# so a downstream operator can later generate Datavant match IDs without
# re-running the partner-side transforms.
#
# This export is an ancillary side-channel. Failures MUST NOT break the
# PCORnet hash_token pipeline; every exception path here is logged and
# swallowed.
###################################################################################################################################

    # Map of Datavant partner keys (as configured in partner_settings.DATAVANT_KEY)
    # to the short source codes used to populate the 'source' column appended to
    # the exported match-ready file. Populate this with your own partner keys,
    # e.g. {"my_partner_key": "MPK"}. Left empty in the public distribution;
    # unmapped keys fall back to the source code "UNKNOWN".
    _DATAVANT_KEY_TO_SOURCE_CODE = {}

    @classmethod
    def datavant_export_to_datavant_inbox(
        cls,
        input_file_path : str,
        file_name       : str,
        folder          : str,
        partner         : str,
    ) -> None:
        """Produce a Datavant match-ready file for downstream match-ID generation.

        Runs two Datavant transform-tokens invocations back-to-back on the
        partner's raw hash_token file:

            1. ``--from <DATAVANT_KEY> -s <DATAVANT_SITE>`` (partner tokens ->
               intermediate CRN tokens)
            2. ``--to datavant        -s <DATAVANT_SITE>`` (intermediate CRN
               tokens -> Datavant match tokens)

        The final output is postprocessed to append a ``source`` column
        (tab-delimited), optionally including an extra ``source_site_name``
        column for partners configured in
        DATAVANT_SOURCE_SITE_NAME_OVERRIDES. The result is moved to
        ``DATAVANT_MATCH_INBOX_DIR`` with a timestamped filename of the form
        ``<partner>_<folder>_hash_token_<YYYYMMDD_HHMMSS>.csv``.

        This is a best-effort side-channel: any error is logged and the
        function returns without raising so the caller's PCORnet pipeline is
        never affected.

        Args:
            input_file_path : Directory containing the raw hash_token file
                              (e.g. ``/data/25r1/``).
            file_name       : Filename pattern (glob) for the raw hash_token
                              file, e.g. ``'*hash_token*.csv'`` or
                              ``'*ONEFL_UFH_HASH_TOKENS*.csv'``.
            folder          : Data folder / batch tag (e.g. ``'25r1'``).
            partner         : Lowercase partner code (e.g. ``'ufh'``,
                              ``'umi'``).

        Returns:
            None. All errors are logged via ovid_logging AND mirrored to
            stderr (so the parent pipelines/etl.py process captures them
            via its subprocess stderr pipe); the caller is expected to
            continue on failure. A ``EXPORT_FAILED_<timestamp>.txt`` marker
            is dropped inside ``for_datavant_match/`` on every failure
            branch so future investigators are never left with an empty
            directory and no diagnostic trail.
        """
        logger = _get_ovid_logger()

        # ovid_logging is NOT initialized inside partner formatter subprocesses
        # (pipelines/etl.py::run_formatter spawns them as plain `python` procs
        # without a setup_logging call), so `logger.*` calls here go to a
        # NullHandler and never reach the ovid_logs/*.log file. Mirroring
        # every message to stderr guarantees the parent's subprocess-stderr
        # capture (commonFunctions.run_subprocess) writes them into the
        # ovid log as `[subprocess stderr]` lines, keeping the sidecar
        # observable in production.
        def _log(level: str, msg: str) -> None:
            """Mirror a diagnostic line to both ovid_logger and stderr."""
            tagged = f"[DATAVANT MATCH EXPORT] {msg}"
            if level == "error":
                logger.error(tagged)
            elif level == "warning":
                logger.warning(tagged)
            else:
                logger.info(tagged)
            try:
                print(tagged, file=sys.stderr, flush=True)
            except OSError:
                pass

        # The working directory is set up before any failure branch can fire
        # so the marker-writer below always has a place to land its file.
        work_dir = f"/app/partners/{partner}/data/datavant_output/{folder}/for_datavant_match/"

        def _write_failure_marker(reason: str) -> None:
            """Drop a ``EXPORT_FAILED_<ts>.txt`` marker inside ``work_dir``.

            The marker is the primary diagnostic trail visible to operators
            when they list ``for_datavant_match/`` and see it otherwise
            empty. Non-fatal: any error writing the marker is logged but
            swallowed.
            """
            try:
                os.makedirs(work_dir, exist_ok=True)
                marker_ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
                marker_path = os.path.join(work_dir, f"EXPORT_FAILED_{marker_ts}.txt")
                with open(marker_path, "w", encoding="utf-8") as marker_fh:
                    marker_fh.write(
                        f"partner={partner}\n"
                        f"folder={folder}\n"
                        f"timestamp={marker_ts}\n"
                        f"reason={reason}\n"
                    )
            except OSError:
                _log("warning",
                     f"Could not write failure marker for partner={partner} folder={folder}")

        try:
            _log("info", f"Starting export for partner={partner} folder={folder}")

            # Resolve the partner's Datavant key from partner_settings.
            partner_settings_path = "partners." + partner + ".partner_settings"
            partner_settings      = importlib.import_module(partner_settings_path)
            datavant_key          = getattr(partner_settings, "DATAVANT_KEY", "") or ""
            datavant_key          = datavant_key.strip()

            if not datavant_key:
                _log("warning",
                     f"partner={partner} has an empty DATAVANT_KEY; skipping to_datavant export.")
                # Not a failure -- partner intentionally has no Datavant key.
                # No marker needed since this is an expected no-op.
                return

            # Locate the raw input file(s). Partners frequently deliver the
            # hash-token table split across many partition shards (e.g. AVH
            # ships ~21 `Epic_Hash_Datavant_*.txt_N_N_N.csv` shards and TMH
            # ships 4 `TMH_PCORI_Epic_Link_N.txt` files). Historically only
            # `matching_files[0]` was processed, silently dropping every other
            # shard from the published match-export file. We now concatenate
            # all shards (header once + every shard's body) into a single
            # combined input so the downstream Datavant transforms and the
            # published match file cover the complete partner population.
            matching_files = sorted(
                glob.glob(os.path.join(input_file_path, file_name), recursive=True),
                key=os.path.basename,
            )
            if not matching_files:
                _log("error",
                     f"No files matched {file_name} in {input_file_path}; aborting export.")
                _write_failure_marker(
                    f"glob {file_name!r} in {input_file_path!r} returned no matches")
                return

            # Timestamp shared by all output artifacts for this run.
            timestamp        = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Sanitize forward slashes in `folder` (e.g. "BND/20260618") so
            # the value can be safely embedded as a filename component
            # without creating an accidental subdirectory in the output
            # path. The Datavant CLI rejects `-o` values whose parent
            # directory does not already exist ("The specified output
            # argument is not a valid file path or directory"), so an
            # unsanitized `bnd_BND/20260618_hash_token_...csv` fails with
            # rc=2 before the CLI can even open its own log files.
            folder_slug      = folder.replace("/", "_").replace("\\", "_")
            base_output_name = f"{partner}_{folder_slug}_hash_token_{timestamp}"

            os.makedirs(work_dir, exist_ok=True)

            # Resolve the single raw input path. When the partner delivers a
            # single file we use it directly; when it is split across shards we
            # concatenate them into one combined input inside work_dir so the
            # Datavant transforms below see the complete population.
            if len(matching_files) == 1:
                raw_input_file = matching_files[0]
                _log("info", f"Raw input file (single shard): {raw_input_file}")
            else:
                combined_input_file = os.path.join(
                    work_dir, f"{base_output_name}_combined_input.csv")
                if os.path.exists(combined_input_file):
                    try:
                        os.remove(combined_input_file)
                    except OSError:
                        _log("warning",
                             f"Could not remove stale combined input {combined_input_file}")
                raw_input_file = cls._datavant_concat_shards(
                    shard_files = matching_files,
                    dest_path   = combined_input_file,
                    logger      = logger,
                )
                if raw_input_file is None or not os.path.isfile(raw_input_file):
                    _log("error",
                         f"Failed to combine {len(matching_files)} shards for "
                         f"partner={partner} folder={folder}; aborting export.")
                    _write_failure_marker(
                        f"shard concat failed for {len(matching_files)} files matching "
                        f"{file_name!r} in {input_file_path!r}")
                    return
                _log("info",
                     f"Raw input file (combined {len(matching_files)} shards): {raw_input_file}")

            from_partner_file = os.path.join(work_dir, f"{base_output_name}_from_partner.csv")
            to_datavant_file  = os.path.join(work_dir, f"{base_output_name}.csv")

            # Log paths (kept alongside the working file for debugging).
            dev_log_path   = os.path.join(work_dir, f"{base_output_name}_dev.log")
            token_log_path = os.path.join(work_dir, f"{base_output_name}_token.log")

            # Remove any pre-existing artifacts; Datavant CLI refuses to overwrite.
            for stale in (from_partner_file, to_datavant_file):
                if os.path.exists(stale):
                    try:
                        os.remove(stale)
                    except OSError:
                        _log("warning", f"Could not remove stale file {stale}")

            # Detect delimiter from the raw file (mirrors runner logic).
            input_delim_flag = cls._datavant_detect_delimiter_flag(raw_input_file)

            # -------- Step 1: partner -> intermediate CRN tokens --------
            args_from = [
                "/ovid_credentials/" + DV_TRANSFORMATION_SCRIPT_FILE_NAME,
                "transform-tokens",
                "--from",           datavant_key,
                "-s",               DATAVANT_SITE,
                "-i",               raw_input_file,
                "-o",               from_partner_file,
                "--dev-log-path",   dev_log_path,
                "--token-log-path", token_log_path,
                "--input-format",   "csv",
                "--delimiter",      input_delim_flag,
                "--num-threads",    str(DATAVANT_MATCH_NUM_THREADS),
            ]
            rc = cls._datavant_run_cli(args_from, cwd=work_dir, logger=logger,
                                       tag="MATCH EXPORT --from")
            if rc != 0 or not os.path.isfile(from_partner_file):
                _log("error",
                     f"--from transform failed for partner={partner} (rc={rc}). Aborting export.")
                _write_failure_marker(
                    f"--from step failed: rc={rc}, from_partner_file_exists="
                    f"{os.path.isfile(from_partner_file)}")
                return

            # -------- Step 2: intermediate -> datavant match tokens --------
            intermediate_delim_flag = cls._datavant_detect_delimiter_flag(from_partner_file)
            args_to = [
                "/ovid_credentials/" + DV_TRANSFORMATION_SCRIPT_FILE_NAME,
                "transform-tokens",
                "--to",             "datavant",
                "-s",               DATAVANT_SITE,
                "-i",               from_partner_file,
                "-o",               to_datavant_file,
                "--dev-log-path",   dev_log_path,
                "--token-log-path", token_log_path,
                "--delimiter",      intermediate_delim_flag,
                "--num-threads",    str(DATAVANT_MATCH_NUM_THREADS),
            ]
            rc = cls._datavant_run_cli(args_to, cwd=work_dir, logger=logger,
                                       tag="MATCH EXPORT --to datavant")
            if rc != 0 or not os.path.isfile(to_datavant_file):
                _log("error",
                     f"--to datavant transform failed for partner={partner} (rc={rc}). Aborting export.")
                _write_failure_marker(
                    f"--to datavant step failed: rc={rc}, to_datavant_file_exists="
                    f"{os.path.isfile(to_datavant_file)}")
                return

            # -------- Step 3: postprocess (append 'source' column) --------
            source_code = cls._DATAVANT_KEY_TO_SOURCE_CODE.get(
                datavant_key,
                cls._DATAVANT_KEY_TO_SOURCE_CODE.get(datavant_key.lower(), "UNKNOWN"),
            )
            # Optional extra ``source_site_name`` column, driven by config. A
            # partner whose DATAVANT_KEY matches an entry in
            # DATAVANT_SOURCE_SITE_NAME_OVERRIDES gets the mapped literal
            # appended; all others get no extra column.
            source_site_name_value = ""
            for key_fragment, override_value in DATAVANT_SOURCE_SITE_NAME_OVERRIDES.items():
                if key_fragment in datavant_key:
                    source_site_name_value = override_value
                    break
            cls._datavant_postprocess_source_column(
                target_file            = to_datavant_file,
                source_code            = source_code,
                source_site_name_value = source_site_name_value,
                logger                 = logger,
                site_name              = datavant_key,
            )

            # -------- Step 4: publish to the shared inbox --------
            os.makedirs(DATAVANT_MATCH_INBOX_DIR, exist_ok=True)
            published_path = os.path.join(
                DATAVANT_MATCH_INBOX_DIR,
                f"{base_output_name}.csv",
            )
            try:
                shutil.move(to_datavant_file, published_path)
                _log("info", f"Published {published_path} (source={source_code})")
            except OSError:
                # Fallback: try copy if move across filesystems fails.
                _log("warning", "shutil.move failed; falling back to copy.")
                try:
                    shutil.copy2(to_datavant_file, published_path)
                    _log("info", f"Copied to {published_path} (source={source_code})")
                except OSError as copy_exc:
                    _log("error",
                         f"Failed to publish {to_datavant_file} to {published_path}: {copy_exc}")
                    _write_failure_marker(
                        f"publish step failed: could not move or copy {to_datavant_file} "
                        f"to {published_path} ({copy_exc})")

        except Exception as exc:
            # Never propagate; ancillary export. Also log traceback via ovid_logger
            # (in case logging is initialized) and mirror the summary line to stderr
            # so the parent process's log at least records that something went wrong.
            logger.error(
                "[DATAVANT MATCH EXPORT] Unexpected failure for partner=%s folder=%s",
                partner, folder, exc_info=True,
            )
            try:
                print(
                    f"[DATAVANT MATCH EXPORT] Unexpected failure for partner={partner} "
                    f"folder={folder}: {type(exc).__name__}: {exc}",
                    file=sys.stderr, flush=True,
                )
            except OSError:
                pass
            _write_failure_marker(
                f"unexpected exception: {type(exc).__name__}: {exc}")

    @classmethod
    def _datavant_concat_shards(
        cls,
        shard_files : list,
        dest_path   : str,
        logger,
    ) -> Optional[str]:
        """Concatenate multiple partition shards into one combined CSV.

        Writes the header from the first shard exactly once, then streams the
        body (every line after the header) of each shard in order. Partners
        that deliver a hash-token table split across many partition shards
        (e.g. AVH's ~21 ``Epic_Hash_Datavant_*.txt_N_N_N.csv`` files, TMH's 4
        ``TMH_PCORI_Epic_Link_N.txt`` files) are recombined here so downstream
        Datavant transforms operate on the complete population rather than a
        single shard.

        The first shard's header is treated as canonical. If any subsequent
        shard's header does not match, a warning is logged (headers are
        expected to be identical across shards for a given delivery) but the
        shard's body is still appended, since the Datavant CLI reads columns
        positionally/by-name against the single header written up front.

        Args:
            shard_files : Ordered list of absolute paths to the shard files.
                          Must contain at least one path.
            dest_path   : Absolute path of the combined output file to write.
            logger      : Logger instance for progress and error reporting.

        Returns:
            ``dest_path`` on success, or ``None`` if the concatenation failed
            (e.g. an unreadable shard or a write error). On failure any partial
            output at ``dest_path`` is removed.
        """
        if not shard_files:
            logger.error(
                "[DATAVANT MATCH EXPORT] _datavant_concat_shards called with no shards.")
            return None

        canonical_header = None
        try:
            with open(dest_path, "w", encoding="utf-8", newline="") as out_fh:
                for shard_index, shard_path in enumerate(shard_files):
                    with open(shard_path, "r", encoding="utf-8",
                              errors="replace", newline="") as in_fh:
                        header_line = in_fh.readline()
                        if not header_line:
                            logger.warning(
                                "[DATAVANT MATCH EXPORT] Shard %s is empty; skipping.",
                                shard_path,
                            )
                            continue
                        if shard_index == 0:
                            canonical_header = header_line
                            out_fh.write(header_line)
                        elif header_line != canonical_header:
                            logger.warning(
                                "[DATAVANT MATCH EXPORT] Shard %s header differs from the "
                                "first shard's header; appending its body anyway.",
                                shard_path,
                            )
                        # Stream the remaining body lines of this shard.
                        for body_line in in_fh:
                            out_fh.write(body_line)
        except OSError:
            logger.error(
                "[DATAVANT MATCH EXPORT] Failed to concatenate shards into %s",
                dest_path, exc_info=True,
            )
            try:
                if os.path.exists(dest_path):
                    os.remove(dest_path)
            except OSError:
                pass
            return None

        logger.info(
            "[DATAVANT MATCH EXPORT] Combined %d shards into %s",
            len(shard_files), dest_path,
        )
        return dest_path

    @classmethod
    def _datavant_detect_delimiter_flag(cls, file_path: str) -> str:
        """Return the ``--delimiter`` flag value for the Datavant CLI.

        Inspects the first line of ``file_path`` and returns ``','`` if commas
        outnumber tabs, otherwise the literal two-character string ``'\\t'``
        which the Datavant CLI expects for tab-delimited files. Defaults to
        tab on any error.

        Args:
            file_path : Absolute path to the file whose delimiter is being
                        inspected.

        Returns:
            Either ``','`` or the literal string ``'\\t'`` (two characters:
            backslash + t), suitable for passing verbatim to
            ``--delimiter``.
        """
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
                first_line = fh.readline()
            if not first_line:
                return "\\t"
            if first_line.count(",") > first_line.count("\t"):
                return ","
            return "\\t"
        except OSError:
            _get_ovid_logger().warning(
                "[DATAVANT MATCH EXPORT] Delimiter detection failed for %s; defaulting to tab.",
                file_path, exc_info=True,
            )
            return "\\t"

    @classmethod
    def _datavant_run_cli(
        cls,
        args   : list,
        cwd    : str,
        logger,
        tag    : str,
    ) -> int:
        """Run the Datavant CLI with credentials piped in via stdin.

        Args:
            args   : Full argv list; args[0] must be the Datavant executable.
            cwd    : Working directory for the subprocess.
            logger : Logger instance to record stdout/stderr against.
            tag    : Short label included in log messages for correlation.

        Returns:
            The subprocess return code, or ``-1`` if the process could not
            be started at all.

        Note:
            Stdout/stderr from the Datavant CLI are mirrored to both the
            supplied ovid_logger and Python's stderr. The stderr mirror is
            required because partner formatter subprocesses do not
            initialize ovid_logging (pipelines/etl.py::run_formatter spawns
            them without setup_logging), so logger.* calls disappear into
            a NullHandler. Writing to sys.stderr guarantees the parent
            process's subprocess-stderr capture surfaces the CLI's own
            error messages in the ovid log.
        """
        try:
            with open("/ovid_credentials/" + DV_CREDENTIALS_FILE_NAME, "rb") as cred_file:
                process = subprocess.Popen(
                    args,
                    stdin  = cred_file,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.PIPE,
                    cwd    = cwd,
                )
                stdout, stderr = process.communicate()

            if stdout:
                stdout_text = stdout.decode(errors="ignore")
                logger.info("[%s stdout] %s", tag, stdout_text)
                try:
                    print(f"[{tag} stdout] {stdout_text}", file=sys.stderr, flush=True)
                except OSError:
                    pass
            if stderr:
                stderr_text = stderr.decode(errors="ignore")
                logger.warning("[%s stderr] %s", tag, stderr_text)
                try:
                    print(f"[{tag} stderr] {stderr_text}", file=sys.stderr, flush=True)
                except OSError:
                    pass
            return process.returncode
        except (OSError, subprocess.SubprocessError) as exc:
            logger.error("[%s] Failed to run Datavant CLI: args=%s", tag, args, exc_info=True)
            try:
                print(f"[{tag}] Failed to run Datavant CLI: {type(exc).__name__}: {exc}",
                      file=sys.stderr, flush=True)
            except OSError:
                pass
            return -1

    # Canonical value that MUST populate the pre-existing TOKEN_ENCRYPTION_KEY
    # column of every partner's Datavant match-export file. The Datavant
    # transform-tokens CLI passes this column through untouched, but some
    # partners ship raw hash-token files whose TOKEN_ENCRYPTION_KEY value has
    # been munged upstream. Normalizing to a single literal at postprocess time
    # keeps the downstream match-ID pipeline invariant across all partners.
    # Configured via DATAVANT_TOKEN_ENCRYPTION_KEY_VALUE in common/settings.py
    # (derived from DATAVANT_SITE by default).
    _CANONICAL_TOKEN_ENCRYPTION_KEY_VALUE = DATAVANT_TOKEN_ENCRYPTION_KEY_VALUE

    @classmethod
    def _datavant_postprocess_source_column(
        cls,
        target_file            : str,
        source_code            : str,
        source_site_name_value : str,
        logger,
        site_name              : str = "",
    ) -> None:
        """Normalize identity columns and append ``source`` (tab-delimited).

        Postprocesses a Datavant ``--to`` output file:

        * When ``source_site_name_value`` is non-empty, the header gains two
          columns (``source_site_name``, ``source``) and each data row is
          appended with ``(source_site_name_value, source_code)``.
        * Otherwise the header/rows gain a single ``source`` column populated
          with ``source_code``.

        In addition, this method **unconditionally normalizes** the two
        identity columns that must be identical across every partner's
        published match-export file:

        * Any pre-existing ``TOKEN_ENCRYPTION_KEY`` column (case-insensitive
          header match) has every data-row value overwritten with the
          canonical literal ``DATAVANT_TOKEN_ENCRYPTION_KEY_VALUE`` (from
          common/settings.py).
        * Any pre-existing ``SOURCE_SITE_NAME`` column (case-insensitive
          header match) has every data-row value overwritten with the
          ``site_name`` argument (i.e. the partner's ``DATAVANT_KEY``).

        This override protects downstream match-ID generation from partner
        files whose raw ``TOKEN_ENCRYPTION_KEY`` / ``SOURCE_SITE_NAME``
        columns arrive munged. It runs for every partner -- it is a no-op
        rewrite for files whose values are already canonical.

        If either column is absent from the header, that override is
        silently skipped.

        The output is always tab-delimited regardless of the input
        delimiter. Rewrite is atomic via a ``.tmp`` file + ``shutil.move``.

        Args:
            target_file            : Path to the Datavant --to output file.
                                     Rewritten in place.
            source_code            : Uppercase short code (e.g. ``'UFH'``).
            source_site_name_value : When non-empty, an extra
                                     ``source_site_name`` column is appended
                                     with this literal. Empty string disables
                                     the extra column.
            logger                 : Logger instance for progress/errors.
            site_name              : Partner ``DATAVANT_KEY``. Used as the
                                     canonical ``SOURCE_SITE_NAME`` value when
                                     that column is present. Empty string
                                     disables the site-name override.
        """
        tmp_path = target_file + ".tmp"

        # Detect the delimiter of the Datavant output (usually tab, but the
        # runner script preserves the input delimiter so mirror that).
        try:
            with open(target_file, "r", encoding="utf-8", errors="replace") as inf:
                first_line = inf.readline()
        except OSError:
            logger.error(
                "[DATAVANT MATCH EXPORT] Could not read %s for postprocessing.",
                target_file, exc_info=True,
            )
            return

        if not first_line:
            logger.warning(
                "[DATAVANT MATCH EXPORT] Output file %s is empty; skipping postprocessing.",
                target_file,
            )
            return

        in_delim = "," if "," in first_line else "\t"

        # Locate the pre-existing TOKEN_ENCRYPTION_KEY / SOURCE_SITE_NAME
        # column indices from the header row (case-insensitive). Either
        # column may legitimately be absent (e.g. UMI has 34 raw columns
        # and no SOURCE_SITE_NAME until this method appends it), in which
        # case the corresponding override is skipped.
        header_cells       = next(csv.reader([first_line], delimiter=in_delim), [])
        token_key_col_idx  = None
        site_name_col_idx  = None
        for header_idx, header_cell in enumerate(header_cells):
            header_cell_normalized = header_cell.strip().lower()
            if header_cell_normalized == "token_encryption_key":
                token_key_col_idx = header_idx
            elif header_cell_normalized == "source_site_name":
                site_name_col_idx = header_idx

        canonical_token_key   = cls._CANONICAL_TOKEN_ENCRYPTION_KEY_VALUE
        canonical_site_name   = site_name or ""
        override_site_name    = site_name_col_idx is not None and bool(canonical_site_name)
        override_token_key    = token_key_col_idx is not None

        def _normalize_row(data_row: list) -> list:
            """Overwrite the identity columns in-place on a copy of ``data_row``.

            Uses the header-derived indices computed above so mislabeled
            or missing columns are silently skipped instead of raising
            IndexError on short rows.
            """
            normalized = list(data_row)
            if override_token_key and token_key_col_idx < len(normalized):
                normalized[token_key_col_idx] = canonical_token_key
            if override_site_name and site_name_col_idx < len(normalized):
                normalized[site_name_col_idx] = canonical_site_name
            return normalized

        try:
            with open(target_file, "r", encoding="utf-8", errors="replace", newline="") as inf, \
                 open(tmp_path,    "w", encoding="utf-8", newline="") as outf:
                reader = csv.reader(inf, delimiter=in_delim)
                writer = csv.writer(outf, delimiter="\t", lineterminator=os.linesep)

                if source_site_name_value:
                    for i, row in enumerate(reader):
                        if i == 0:
                            writer.writerow(list(row) + ["source_site_name", "source"])
                        else:
                            writer.writerow(_normalize_row(row) + [source_site_name_value, source_code])
                    logger.info(
                        "[DATAVANT MATCH EXPORT] Applied source_site_name postprocessing to %s "
                        "(normalized TOKEN_ENCRYPTION_KEY=%s, appended source_site_name=%s)",
                        target_file, override_token_key, source_site_name_value,
                    )
                else:
                    for i, row in enumerate(reader):
                        if i == 0:
                            writer.writerow(list(row) + ["source"])
                        else:
                            writer.writerow(_normalize_row(row) + [source_code])
                    logger.info(
                        "[DATAVANT MATCH EXPORT] Appended 'source=%s' column to %s "
                        "(normalized TOKEN_ENCRYPTION_KEY=%s, SOURCE_SITE_NAME=%s)",
                        source_code, target_file, override_token_key, override_site_name,
                    )
        except OSError:
            logger.error(
                "[DATAVANT MATCH EXPORT] Postprocessing failed for %s",
                target_file, exc_info=True,
            )
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            return

        try:
            shutil.move(tmp_path, target_file)
        except OSError:
            logger.error(
                "[DATAVANT MATCH EXPORT] Could not replace %s with postprocessed tmp.",
                target_file, exc_info=True,
            )
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass

###################################################################################################################################
# This function will 
###################################################################################################################################

    @classmethod

    def apply_fix(cls, fix_type: str, fix_name: str, version: str, input_path: str, output_path: str,
                  fixed_table_name: str, mapped_table_name: str, input_data_folder: str,
                  partner_name: str, src1: str, src2: str) -> None:
        """Run one fix script as a subprocess and fail loudly if it fails.

        Since 2026-10-04 a failed fix (non-zero exit, or a non-warning message
        reported via ``print_failure_message``, which the fix records in a
        private marker file named by ``FIX_FAILURE_MARKER_ENV_VAR``) quarantines
        the partially fixed table as ``FAILED_<fixed_table_name>`` and raises,
        so the partner fixer's ``except`` block reports it and the remaining
        fixes for that table are skipped. Previously the chain kept running on
        a partially fixed file and the upload stage shipped it (DC 1.08 orphan
        PATIDs).

        Args:
            fix_type: ``common`` or ``custom``.
            fix_name: Fix directory name.
            version: Fix version (script file name without ``.py``).
            input_path: Mapper output folder (or fixer output for multi-table fixes).
            output_path: Fixer output folder.
            fixed_table_name: Fixed table file name, e.g. ``fixed_diagnosis.csv``.
            mapped_table_name: Mapped table file name, e.g. ``mapped_diagnosis.csv``.
            input_data_folder: Batch folder, e.g. ``PARTNER/20260915``.
            partner_name: Partner name.
            src1: First fix parameter.
            src2: Second fix parameter.

        Raises:
            RuntimeError: If the fix subprocess fails.
        """

        if fix_type == 'custom':

            partner_fix_path = f"/app/partners/{partner_name.lower()}/custom_fixes/{fix_name}/{version}.py"


        if fix_type == 'common':



            partner_fix_path = f"/app/common/common_fixes/{fix_name}/{version}.py"

        command = ["python", partner_fix_path, '-f', input_data_folder, '-t', mapped_table_name, '-p', partner_name, '-ftm', fixed_table_name, '-i', input_path, '-o', output_path, '-sr1', src1, '-sr2', src2]
        
        # Pass overall start time to subprocess via environment variable
        # First check if we have it locally, otherwise inherit from parent's environment
        import os
        env = os.environ.copy()
        if cls._overall_start_time:
            env['OVID_OVERALL_START_TIME'] = cls._overall_start_time.isoformat()
        # If _overall_start_time is None, the env already has OVID_OVERALL_START_TIME from parent (if set)

        # Route through cls.run_subprocess() instead of raw subprocess.run()
        # so the PYSPARK_GATEWAY_PORT / PYSPARK_GATEWAY_SECRET /
        # PYSPARK_PIN_THREAD scrub from the 2026-06-11 SPARK-2243 cascade
        # fix is applied to fix-script subprocesses too.
        #
        # Without it, fix subprocesses inherit the parent ETL driver's
        # gateway env vars (set by the spark-submit that launched the
        # parent), attach to the parent JVM's py4j gateway via
        # SparkSession.builder.getOrCreate(), and -- when the fix's Spark
        # job aborts (e.g. the panel_loinc_remover_fix EOFException
        # observed on EMY 20260610) -- the fix's `spark.stop()` in its
        # except block tears down the *parent* driver's JavaSparkContext.
        # Subsequent ETL phases (multi_tables_fixer, then upload) then
        # fail with `'NoneType' object has no attribute 'setCallSite'`
        # and `Cannot call methods on a stopped SparkContext`, which is
        # exactly what happened to the EMY 20260610 batch's CONDITION
        # upload at 11:47:56 EDT after panel_loinc_remover_fix crashed
        # at 11:36:52 EDT.
        #
        # run_subprocess() also captures stdout/stderr to the OVID log
        # file, which is the same behaviour the fix-script subprocesses
        # already expected (they communicate fix status and failure
        # messages via file queues, not stdout). shell=False is the
        # correct mode here since `command` is a list.
        # Failure detection uses a private per-fix marker file rather than the
        # shared failure queue: the parent ETL's display timer thread drains
        # and CLEARS that queue within ~100 ms of the failure flag appearing,
        # so diffing it here raced and lost (a patid_orphans_fix failure
        # went undetected and the chain continued on the unfixed file).
        failure_marker_path: str = cls._create_fix_failure_marker()
        env[FIX_FAILURE_MARKER_ENV_VAR] = failure_marker_path
        try:
            result = cls.run_subprocess(command, env=env, shell=False)
            fatal_failure_count: int = cls._count_fatal_failure_messages(failure_marker_path)
        finally:
            cls._remove_fix_failure_marker(failure_marker_path)

        if result.returncode != 0 or fatal_failure_count > 0:
            cls._quarantine_failed_fixed_table(output_path, fixed_table_name, fix_name, version)
            raise RuntimeError(
                f"Fix {fix_type}/{fix_name}/{version} failed on {fixed_table_name} "
                f"(return code {result.returncode}, failure messages {fatal_failure_count}). "
                f"Remaining fixes for this table were skipped; re-run the fix stage."
            )

    @classmethod
    def _create_fix_failure_marker(cls) -> str:
        """Create an empty, private failure-marker file for one fix subprocess.

        The path is handed to the fix via ``FIX_FAILURE_MARKER_ENV_VAR``;
        ``print_failure_message`` appends one record per failure to it. Unlike
        the shared failure queue, nothing else reads or clears this file.

        Returns:
            Absolute path of the marker file.
        """
        marker_dir: str = os.path.join(cls._get_sync_dir(), 'fix_failure_markers')
        os.makedirs(marker_dir, exist_ok=True)
        marker_fd, marker_path = tempfile.mkstemp(prefix='fix_', suffix='.log', dir=marker_dir)
        os.close(marker_fd)
        return marker_path

    @classmethod
    def _remove_fix_failure_marker(cls, marker_path: str) -> None:
        """Delete a fix failure-marker file, logging (not raising) on error.

        Args:
            marker_path: Path returned by ``_create_fix_failure_marker``.

        Returns:
            None.
        """
        try:
            os.remove(marker_path)
        except FileNotFoundError:
            pass
        except OSError:
            _get_ovid_logger().error(
                f"Failed to remove fix failure marker {marker_path}", exc_info=True
            )

    @classmethod
    def _record_fix_failure(cls, text: str) -> None:
        """Append a failure record to the fix failure marker, if one is set.

        Called by ``print_failure_message``. No-op outside an ``apply_fix``
        subprocess (env var unset). Records are separated by a NUL byte so
        multi-line exception text stays one record.

        Args:
            text: Failure detail text (exception message).

        Returns:
            None.
        """
        marker_path: Optional[str] = os.environ.get(FIX_FAILURE_MARKER_ENV_VAR)
        if not marker_path:
            return
        try:
            with open(marker_path, 'a') as marker_file:
                marker_file.write(text.replace('\0', ' ') + '\0')
        except OSError:
            _get_ovid_logger().error(
                f"Failed to write fix failure marker {marker_path}", exc_info=True
            )

    @classmethod
    def _count_fatal_failure_messages(cls, marker_path: str) -> int:
        """Count failure records in a fix marker that represent a real failure.

        ``print_failure_message`` also reports "Path does not exist" conditions
        (rendered as orange warnings), e.g. an optional reference table that a
        partner does not deliver. Those are not treated as fatal so existing
        fix chains keep their current behavior for missing optional inputs.

        Args:
            marker_path: Path of the fix's failure-marker file.

        Returns:
            The number of records that are not missing-path warnings. An
            unreadable marker counts as one failure (fail closed).
        """
        try:
            with open(marker_path, 'r') as marker_file:
                content: str = marker_file.read()
        except OSError:
            _get_ovid_logger().error(
                f"Failed to read fix failure marker {marker_path}", exc_info=True
            )
            return 1
        fatal_count: int = 0
        for record in content.split('\0'):
            if not record:
                continue
            if "Path does not exist" in record or "does not exist in" in record:
                continue
            fatal_count += 1
        return fatal_count

    @classmethod
    def _quarantine_failed_fixed_table(cls, output_path: str, fixed_table_name: str,
                                       fix_name: str, version: str) -> None:
        """Rename a partially fixed table so it cannot be uploaded by mistake.

        Fixes have been observed crashing mid-chain (e.g.
        ``patid_orphans_fix``) while the remaining fixes kept running and the
        upload stage shipped the half-fixed file. The fixed file is renamed to
        ``FAILED_<fixed_table_name>``, which the upload stage's ``fixed_*``
        file scan ignores.

        Args:
            output_path: Fixer output folder containing the fixed table.
            fixed_table_name: File name of the fixed table (e.g. ``fixed_diagnosis.csv``).
            fix_name: Name of the fix that failed, for logging.
            version: Version of the fix that failed, for logging.

        Returns:
            None.
        """
        fixed_file_path: str = os.path.join(output_path, fixed_table_name)
        if not os.path.isfile(fixed_file_path):
            return
        quarantined_path: str = os.path.join(output_path, f"FAILED_{fixed_table_name}")
        try:
            os.replace(fixed_file_path, quarantined_path)
            _get_ovid_logger().error(
                f"Fix {fix_name}/{version} failed; quarantined {fixed_file_path} -> {quarantined_path}"
            )
        except OSError:
            _get_ovid_logger().error(
                f"Fix {fix_name}/{version} failed and {fixed_file_path} could not be quarantined",
                exc_info=True,
            )

###################################################################################################################################
# This function will output the mapping gap for the passed formatted file name
###################################################################################################################################

    @classmethod
    def get_mapping_gap(cls,partner,file_name,table_name, file_path,folder_name):

        
        try: 

            complete_file_path = file_path+'/formatted_'+file_name.lower()+'.csv'
            partner_dictionaries_path = "partners."+partner+".dictionaries"
            partner_dictionaries = importlib.import_module(partner_dictionaries_path)
            
            master_url = os.environ["SPARK_URL"].strip().replace('"', '')
            spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()
            # spark = SparkSession.builder.master("spark://spark-master:7077").appName("MAPPING GAP").getOrCreate()

            df = cls.spark_read(complete_file_path, spark)
            df.cache()
            row_count= df.count()

            mapping_dicionaries_list = []

            for key, value in partner_dictionaries.__dict__.items():
                

                if  ((file_name.lower() in key and 'death_cause' not in key) or  (file_name.lower() in key and file_name.lower()=='death_cause') ) and "_dict" in key:
                    
                    # get the field name


                    prefix = '@' #logic added to only remove the first word from the dictinary name

                    temp_key = prefix+key


                    field_name = temp_key.replace(prefix+file_name.lower()+'_',"").replace('_dict',"").upper()

                    

                    # get the aggreated data from the formatter for the field 

                    # try:

                    aggregated_values = cls.get_aggregate_value_from_table(df,row_count,field_name)


                    # get and output the aggreated data that has mapping 

                
                    dictionary_dataframe = spark.createDataFrame(value.items(), ["dict_key", "dict_value"])
            

                    if 'UNIT' in field_name:
                        joined_df = aggregated_values.join(dictionary_dataframe, dictionary_dataframe['dict_key']==aggregated_values[field_name], how ="left").orderBy(desc("count"))
                    else:
                        joined_df = aggregated_values.join(dictionary_dataframe, upper(dictionary_dataframe['dict_key'])==upper(aggregated_values[field_name]), how ="left").orderBy(desc("count"))
                

                    cls.diagnos_dictionary(dictionary_dataframe, file_path, key, file_name.lower())

                    cls.diagnos_mapping(joined_df, file_path, key, field_name, dictionary_dataframe, file_name.lower())

                    mapping_dicionaries_list.append(key)     

            spark.stop()

        except Exception as e:

            spark.stop()

            cls.print_failure_message(
                                    folder  = folder_name,
                                    partner = partner.lower(),
                                    job     = 'mmapping_gap' ,
                                    text = str(e)
                                    )

###################################################################################################################################
# This function will upload a pyspark df to mssql table in a specified server/dabase
###################################################################################################################################

    @classmethod


    def read_from_snowflake(cls, db_server,db_name, schema_name, table_name ):



        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

        spark = SparkSession.builder \
                        .master("spark://master:7077") \
                        .appName("Upload") \
                        .getOrCreate()  



        pkb = cls.get_pkb()

        sf_options = {
            "sfURL": f"{db_server}.snowflakecomputing.com",
            "sfDatabase": db_name,
            "sfSchema": schema_name,
            "sfStage": SF_STAGE_NAME,
            "sfRole": SF_ROLE,
            "sfUser": SF_DB_USER,
            "pem_private_key": pkb  # <== path to your .p8 private key
        }
        
        

        get_tables_names_query = f"""
        SELECT *
        FROM {table_name}
        """




        table_df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sf_options) \
            .option("query", get_tables_names_query) \
            .load()
        
        return table_df
###################################################################################################################################
# This function 
###################################################################################################################################

    @classmethod
    def get_pkb(cls):


            with open(SF_PRIVATE_KEY_PASSPHRASE_PATH, "r") as f:
                sf_private_key_passphrase = f.readline().strip()


            with open(SF_PRIVATE_KEY_PATH, "rb") as key:
                private_key = serialization.load_pem_private_key(
                    key.read(),
                    password=sf_private_key_passphrase.encode() if sf_private_key_passphrase else None,
                    backend=default_backend()
                )
        

            pkb = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            pkb = pkb.decode("UTF-8")
            pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", "")

            return pkb

###################################################################################################################################
# This function will output the mapping gap for the passed formatted file name
###################################################################################################################################

   

    @classmethod
    def get_mothly_counts(cls,partner,file_name,table_name, file_path,folder_name):

        

        complete_file_path = file_path+'/formatted_'+file_name.lower()+'.csv'
        
        master_url = os.environ["SPARK_URL"].strip().replace('"', '')
        spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()
        # spark = SparkSession.builder.master("spark://spark-master:7077").appName("MONTHLY COUNT").getOrCreate()

        monthly_counts_out_file_path = file_path.replace('formatter','monthly_count')+table_name+'/'


        df = cls.spark_read(complete_file_path, spark)

        for col_name in df.columns:

            if 'DATE' in col_name:

                monthly_counts = df.groupBy(date_format(col_name, "yyyy-MM").alias("year_month")) \
                   .agg(count("*").alias("count")) \
                   .orderBy("year_month",  ascending=False)
                
                output_file_name = col_name.lower()+'_mothly_count.csv'

                cls.write_pyspark_output_file(monthly_counts,output_file_name, monthly_counts_out_file_path )




 ###################################################################################################################################
# This function will output the mapping gap for the passed formatted file name
###################################################################################################################################

   

    @classmethod
    def get_distinct_values(cls,partner,file_name,table_name, file_path,folder_name):

        bypass_files_list = ['fact_relationship']
        if file_name.lower() not in bypass_files_list:

            complete_file_path = file_path+'/formatted_'+file_name.lower()+'.csv'
            
            master_url = os.environ["SPARK_URL"].strip().replace('"', '')
            spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()


            # spark = SparkSession.builder.master("spark://spark-master:7077").appName("MONTHLY COUNT").getOrCreate()
            spark.conf.set("spark.sql.shuffle.partitions", 2000)  # or higher


            distinct_values_out_file_path = file_path.replace('formatter','distinct_values')+table_name+'/'


            df = cls.spark_read(complete_file_path, spark)

            total_count = df.count()

            for col_name in df.columns:

                if not col_name.endswith("IDsdfd") :

                    distinct_values_count = df.groupBy(col_name) \
                    .agg(count("*").alias("Count")) \
                    .withColumn("Percent", F.round((F.col("Count") / total_count * 100), 2))\
                    .orderBy("Count",  ascending=False)
                    
                    output_file_name = col_name.lower()+'_distinct_values.csv'

                    cls.write_pyspark_output_file(distinct_values_count,output_file_name, distinct_values_out_file_path )



           
###################################################################################################################################
# Raw-file distinct values: helpers + main entry points
###################################################################################################################################

    @classmethod
    def _raw_dv_sniff_delimiter(cls, file_path: str) -> str:
        """Sniff the field delimiter of a raw CSV/TSV/TXT file from its first line.

        Detects four delimiters in priority order: tab, pipe, semicolon, comma.
        The priority is chosen so that OMOP-source comma files (the historical
        default) and OMOP-intermediate tab files keep their existing behaviour,
        while PCORnet-native partner deliveries (pipe-delimited by long-standing
        convention) and European-style semicolon exports are now also handled
        correctly. Comma is the final fallback so any single-column file or any
        file whose header genuinely contains none of the other three delimiters
        still parses without raising.

        Args:
            file_path: Absolute path to the raw file.

        Returns:
            One of ``"\\t"``, ``"|"``, ``";"``, or ``","`` based on the first
            character class found in the header line. Defaults to ``","`` when
            the file cannot be opened.
        """
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
                first_line = fh.readline()
            if "\t" in first_line:
                return "\t"
            if "|" in first_line:
                return "|"
            if ";" in first_line:
                return ";"
            return ","
        except (IOError, OSError) as exc:
            _get_ovid_logger().error(f"[raw_dv] could not sniff delimiter for {file_path}: {exc}", exc_info=True)
            return ","


    @classmethod
    def _raw_dv_spark_read(cls, file_paths, spark: SparkSession):
        """Read one or more raw CSV/TSV files into a single Spark DataFrame.

        Accepts either a single path (legacy callers) or a list of paths.
        When a list is supplied, every file must use the same delimiter and
        the same column header order; otherwise a ``ValueError`` is raised
        so the caller can surface the misconfiguration to the user without
        producing a silently-misaligned union.

        Args:
            file_paths: Either an absolute path string (legacy) or a non-empty
                list of absolute path strings (group mode).
            spark: Active SparkSession.

        Returns:
            A Spark DataFrame with all columns typed as string (no inference).
            When ``file_paths`` is a list the returned DataFrame is the row
            union of every file, read in a single ``spark.read.csv()`` call.

        Raises:
            ValueError: If the supplied list is empty, or if the files
                disagree on delimiter or column headers.
        """
        # Normalize to a list internally; remember whether the caller passed
        # a string so we can keep the same `spark.read.csv(path, ...)` shape
        # they always used (a single string is fine; a list of one is also
        # accepted by Spark and produces the same DataFrame).
        if isinstance(file_paths, str):
            paths_list = [file_paths]
        elif isinstance(file_paths, (list, tuple)):
            paths_list = [str(p) for p in file_paths]
        else:
            raise ValueError(
                f"_raw_dv_spark_read: unsupported file_paths type {type(file_paths).__name__}"
            )

        if not paths_list:
            raise ValueError("_raw_dv_spark_read: file_paths is empty")

        # Sniff the delimiter from the first file. Every other file in the
        # list must agree, otherwise we abort with a clear message.
        first_delim = cls._raw_dv_sniff_delimiter(paths_list[0])
        if len(paths_list) > 1:
            mismatched_delims = [
                p for p in paths_list[1:] if cls._raw_dv_sniff_delimiter(p) != first_delim
            ]
            if mismatched_delims:
                first_disp = "TAB" if first_delim == "\t" else repr(first_delim)
                raise ValueError(
                    f"raw_dv group contains files with mixed delimiters "
                    f"(first file uses {first_disp}; mismatched files: "
                    f"{', '.join(os.path.basename(p) for p in mismatched_delims)})"
                )

        # Validate header equality across all files before unioning. Spark's
        # `spark.read.csv(list_of_paths, header=True)` requires identical
        # headers; if it doesn't get them it can silently misalign columns.
        # We detect the disagreement up front and abort with a precise diff.
        if len(paths_list) > 1:
            try:
                first_cols = spark.read.csv(
                    paths_list[0],
                    sep=first_delim,
                    header=True,
                    quote='"',
                    inferSchema=False,
                    multiLine=False,
                    escape='"',
                ).columns
            except Exception as exc:
                raise ValueError(
                    f"raw_dv group: could not read header of {os.path.basename(paths_list[0])}: {exc}"
                )

            for p in paths_list[1:]:
                try:
                    other_cols = spark.read.csv(
                        p,
                        sep=first_delim,
                        header=True,
                        quote='"',
                        inferSchema=False,
                        multiLine=False,
                        escape='"',
                    ).columns
                except Exception as exc:
                    raise ValueError(
                        f"raw_dv group: could not read header of {os.path.basename(p)}: {exc}"
                    )
                if other_cols != first_cols:
                    only_in_first = [c for c in first_cols if c not in other_cols]
                    only_in_other = [c for c in other_cols if c not in first_cols]
                    raise ValueError(
                        f"raw_dv group: schema mismatch between "
                        f"{os.path.basename(paths_list[0])} and "
                        f"{os.path.basename(p)} "
                        f"(only in first: {only_in_first or '[]'}; "
                        f"only in second: {only_in_other or '[]'})"
                    )

        return spark.read.csv(
            paths_list,
            sep=first_delim,
            header=True,
            quote='"',
            inferSchema=False,
            multiLine=False,
            escape='"',
        )


    @classmethod
    def _raw_dv_load_concept_table(cls, concept_table_path: str, spark: SparkSession):
        """Load the OMOP CONCEPT table for concept_id enrichment.

        The CONCEPT table is loaded with auto-sniffed delimiter and projected
        to the columns needed for enrichment. Returns None if the file does not
        exist (graceful degradation: enrichment is skipped, distinct counts still run).

        Args:
            concept_table_path: Absolute path to CONCEPT.csv (omop format).
            spark: Active SparkSession.

        Returns:
            Spark DataFrame with columns concept_id, concept_name, concept_code,
            vocabulary_id, domain_id; or None if the file is missing.
        """
        if not os.path.isfile(concept_table_path):
            _get_ovid_logger().warning(
                f"[raw_dv] CONCEPT table not found at {concept_table_path}; running without concept enrichment."
            )
            return None

        try:
            concept_df = cls._raw_dv_spark_read(concept_table_path, spark)
            # Build a case-insensitive map from lowercase canonical name -> actual
            # column name as it appears in the file. The standard Athena OMOP
            # CONCEPT export uses uppercase headers (CONCEPT_ID, CONCEPT_NAME,
            # ...); some downstream pipelines normalize to lowercase. Accept
            # either by lowering the lookup key, then project to canonical
            # lowercase names so all downstream join/select code (which
            # references concept_id / concept_code / vocabulary_id / domain_id /
            # concept_name) works regardless of the source file's column casing.
            actual_by_lower = {c.lower(): c for c in concept_df.columns}
            wanted = ["concept_id", "concept_name", "concept_code", "vocabulary_id", "domain_id"]
            missing = [c for c in wanted if c not in actual_by_lower]
            if missing:
                _get_ovid_logger().warning(
                    f"[raw_dv] CONCEPT table is missing columns {missing} "
                    f"(case-insensitive search; available columns: "
                    f"{sorted(concept_df.columns)}); concept enrichment disabled."
                )
                return None
            return concept_df.select(
                *[F.col(f"`{actual_by_lower[c]}`").alias(c) for c in wanted]
            )
        except Exception as exc:
            _get_ovid_logger().error(
                f"[raw_dv] failed to load CONCEPT table from {concept_table_path}: {exc}",
                exc_info=True,
            )
            return None


    @classmethod
    def _raw_dv_match_files(cls, raw_input_dir: str, table_patterns: list) -> list:
        """Discover and filter raw files in the input directory.

        Files are matched against user-supplied table patterns using fnmatch
        (case-insensitive). Patterns match either the full filename without
        extension, the date-stripped stem, or that stem after removing a
        leading ``SS_`` prefix (so ``condition_occurrence`` matches
        ``SS_condition_occurrence_11192025.csv``).

        Pattern semantics:
            * **Plain pattern (no glob meta-characters)** -- treated as a
              SUBSTRING match: the pattern is wrapped as ``*pattern*`` before
              fnmatch. So ``-t measurement`` matches ``measurement.csv``,
              ``measurement_sup.csv``, ``cerner_measurement.csv``, AND
              ``UM_..._measurement.csv``. The user is responsible for narrowing
              with a more specific pattern when they don't want supplemental
              or source-system-prefixed variants (e.g. ``-t 'measurement.csv'``
              or a glob like ``-t 'measurement[!_]*'``).
            * **Glob pattern (contains ``*``, ``?``, or ``[``)** -- used
              verbatim with fnmatch. So ``-t '*meas*'`` or
              ``-t 'measurement[!_]*'`` retain their literal glob semantics.
            * **``'all'``** -- profiles every csv/tsv/txt in the directory,
              skipping OMOP reference vocab tables (RAW_DV_VOCAB_EXCLUSIONS).

        Args:
            raw_input_dir: Absolute path to the raw data directory.
            table_patterns: List of fnmatch glob patterns or 'all'.

        Returns:
            Sorted list of absolute file paths matching the patterns.
        """
        if not os.path.isdir(raw_input_dir):
            return []

        candidates = []
        for entry in sorted(os.listdir(raw_input_dir)):
            full_path = os.path.join(raw_input_dir, entry)
            if not os.path.isfile(full_path):
                continue
            ext = os.path.splitext(entry)[1].lower()
            if ext not in RAW_DV_ALLOWED_EXTENSIONS:
                continue
            candidates.append((entry, full_path))

        run_all = any(p.lower() == "all" for p in table_patterns)
        matched: list = []

        # Helper: detect glob meta-characters. When present, the pattern is
        # used verbatim with fnmatch; otherwise we wrap it as *pattern*.
        def _has_glob_meta(p: str) -> bool:
            return any(ch in p for ch in ("*", "?", "["))

        for filename, full_path in candidates:
            stem = os.path.splitext(filename)[0]
            # strip a trailing date suffix like _20251119 or _11192025 to make
            # patterns like 'condition_occurrence' match 'SS_condition_occurrence_11192025'
            stripped = re.sub(r"_(?:\d{6,8})$", "", stem)
            stripped = re.sub(r"^SS_", "", stripped, flags=re.IGNORECASE)

            if run_all:
                if any(skip in filename.lower() for skip in RAW_DV_VOCAB_EXCLUSIONS):
                    continue
                matched.append(full_path)
                continue

            for pattern in table_patterns:
                pat_lower = pattern.lower()
                # Plain patterns (no glob meta-chars) become substring matches.
                effective = pat_lower if _has_glob_meta(pat_lower) else f"*{pat_lower}*"
                if (
                    fnmatch.fnmatch(filename.lower(), effective)
                    or fnmatch.fnmatch(stem.lower(), effective)
                    or fnmatch.fnmatch(stripped.lower(), effective)
                ):
                    matched.append(full_path)
                    break

        return matched


    @classmethod
    def _raw_dv_match_files_grouped(
        cls,
        raw_input_dir: str,
        table_patterns: list,
    ) -> list:
        """Discover and filter raw files, grouping by user-supplied pattern.

        Same matching rules as ``_raw_dv_match_files()`` (substring for plain
        patterns, fnmatch for glob patterns, ``RAW_DV_VOCAB_EXCLUSIONS`` for
        the ``'all'`` shortcut). Unlike the flat helper, this returns one
        group per user-supplied pattern: every file that matches the pattern
        is collected into a single group whose ``group_name`` is taken from
        the pattern itself. The intent is that ``-t measurement`` produces
        one group named ``measurement`` containing every measurement-named
        file in the input directory, so chunked tables are profiled as a
        single logical entity.

        Special-case: when the user passes ``-t all``, the function falls
        back to one group per file (``group_name`` derived via
        :meth:`_raw_dv_table_stem`). This preserves the historical
        per-file rendering for the ``all`` shortcut where unioning every
        non-vocab file would produce a meaningless schema and an unusable
        report.

        Files matched by multiple patterns are claimed by the first
        matching pattern (preserves the user's argument order, mirroring
        the ``break``-on-first-match semantics of the flat helper).

        Args:
            raw_input_dir: Absolute path to the raw data directory.
            table_patterns: List of fnmatch glob patterns or the literal
                string ``'all'``.

        Returns:
            Ordered list of group dicts, each shaped like::

                {
                    "group_name": str,
                    "pattern": str,
                    "files": [absolute_path, ...],
                }

            Empty groups are omitted. Group order matches the user's
            ``-t`` argument order (or filesystem order for ``-t all``).
        """
        if not os.path.isdir(raw_input_dir):
            return []

        candidates = []
        for entry in sorted(os.listdir(raw_input_dir)):
            full_path = os.path.join(raw_input_dir, entry)
            if not os.path.isfile(full_path):
                continue
            ext = os.path.splitext(entry)[1].lower()
            if ext not in RAW_DV_ALLOWED_EXTENSIONS:
                continue
            candidates.append((entry, full_path))

        run_all = any(p.lower() == "all" for p in table_patterns)

        # `-t all` keeps the historical one-group-per-file rendering.
        if run_all:
            groups: list = []
            for filename, full_path in candidates:
                if any(skip in filename.lower() for skip in RAW_DV_VOCAB_EXCLUSIONS):
                    continue
                groups.append(
                    {
                        "group_name": cls._raw_dv_table_stem(full_path),
                        "pattern": "all",
                        "files": [full_path],
                    }
                )
            return groups

        def _has_glob_meta(p: str) -> bool:
            return any(ch in p for ch in ("*", "?", "["))

        # Build one group per user-supplied pattern, in user-argument order.
        # A file is claimed by the first matching pattern only.
        claimed: set = set()
        groups: list = []
        used_group_names: dict = {}  # name -> count, for de-duplication
        for pattern in table_patterns:
            pat_lower = pattern.lower()
            effective = pat_lower if _has_glob_meta(pat_lower) else f"*{pat_lower}*"

            files_in_group: list = []
            for filename, full_path in candidates:
                if full_path in claimed:
                    continue
                stem = os.path.splitext(filename)[0]
                stripped = re.sub(r"_(?:\d{6,8})$", "", stem)
                stripped = re.sub(r"^SS_", "", stripped, flags=re.IGNORECASE)
                if (
                    fnmatch.fnmatch(filename.lower(), effective)
                    or fnmatch.fnmatch(stem.lower(), effective)
                    or fnmatch.fnmatch(stripped.lower(), effective)
                ):
                    files_in_group.append(full_path)
                    claimed.add(full_path)

            if not files_in_group:
                continue

            base_name = pat_lower
            if base_name in used_group_names:
                used_group_names[base_name] += 1
                group_name = f"{base_name}_{used_group_names[base_name]}"
            else:
                used_group_names[base_name] = 1
                group_name = base_name

            groups.append(
                {
                    "group_name": group_name,
                    "pattern": pattern,
                    "files": files_in_group,
                }
            )

        return groups


    @classmethod
    def _raw_dv_match_columns(cls, df_columns: list, field_patterns: list) -> list:
        """Filter a list of dataframe columns by user-supplied glob patterns.

        Args:
            df_columns: All columns present in the source DataFrame.
            field_patterns: List of fnmatch glob patterns or 'all'.

        Returns:
            Ordered list of matched column names (preserves df_columns order).
        """
        run_all = any(p.lower() == "all" for p in field_patterns)
        if run_all:
            return list(df_columns)

        matched: list = []
        for col_name in df_columns:
            for pattern in field_patterns:
                if fnmatch.fnmatch(col_name.lower(), pattern.lower()):
                    matched.append(col_name)
                    break
        return matched


    @classmethod
    def _raw_dv_table_stem(cls, file_path: str) -> str:
        """Derive a clean table stem from a raw filename.

        Strips directory, extension, leading 'SS_' prefix, and trailing date
        suffix (8-digit YYYYMMDD or 6-digit MMDDYY style).

        Args:
            file_path: Absolute path to the raw file.

        Returns:
            Cleaned stem suitable for use as a section title or filename.
        """
        base = os.path.basename(file_path)
        stem = os.path.splitext(base)[0]
        stem = re.sub(r"^SS_", "", stem, flags=re.IGNORECASE)
        stem = re.sub(r"_(?:\d{6,8})$", "", stem)
        return stem


    @classmethod
    def _raw_dv_column_priority(cls, col_name: str) -> int:
        """Return the priority bucket used to sort columns within a file section.

        Lower values come first. Doctors want vocabulary/type-mix overviews
        first (`*_type_concept_id`), then the actual concept distributions
        (`*_concept_id`), then source-side concepts and codes.

        Args:
            col_name: Column name from the raw source file.

        Returns:
            Integer 0..4 indicating sort priority (0 = first).
        """
        n = col_name.lower()
        if n.endswith("_type_concept_id"):
            return 0
        if n.endswith("_source_concept_id"):
            return 2
        if n.endswith("_concept_id"):
            return 1
        if n.endswith("_source_value"):
            return 3
        return 4


    @classmethod
    def _raw_dv_pdf_link_label(cls, col_name: str, kind: str) -> str:
        """Return a descriptive link label for a column TOC/mini-TOC entry.

        Args:
            col_name: Column name (case-insensitive suffix detection).
            kind: Either ``"block"`` (the distinct-values table) or
                ``"vocab"`` (the vocabulary-breakdown table).

        Returns:
            A self-explanatory string suitable for use as a hyperlink label.
        """
        n = col_name.lower()
        if kind == "vocab":
            return f"{col_name} \u2014 vocabulary mix"

        # kind == "block" (default for any unknown kind)
        if n.endswith("_type_concept_id"):
            return f"{col_name} \u2014 type breakdown (top values, vocabulary)"
        if n.endswith("_source_concept_id"):
            return f"{col_name} \u2014 distinct source concept ids"
        if n.endswith("_concept_id"):
            return f"{col_name} \u2014 distinct concepts"
        if n.endswith("_source_value"):
            return f"{col_name} \u2014 distinct source values"
        return f"{col_name} \u2014 distinct values"


    @classmethod
    def get_raw_distinct_values(
        cls,
        partner: str,
        folder_name: str,
        group_name: str,
        file_paths: list,
        field_patterns: list,
        concept_table_path: str,
    ) -> dict:
        """Compute distinct value counts for selected columns of a logical group.

        A *group* is one or more raw input files that share a logical
        identity (typically because the user requested them via a single
        ``-t`` pattern that matched multiple chunked files such as
        ``measurement_1.csv`` and ``measurement_2.csv``). All files in the
        group are unioned at read time and profiled as a single table.

        For each selected column, writes a CSV with columns
        (value, count, percent) ordered by count desc to:
        partners/{partner}/data/{folder_name}/raw_distinct_values/{group_name}/{column}_distinct_values.csv

        For columns whose name ends in '_concept_id', enriches the output with
        concept_name, concept_code, vocabulary_id, and domain_id from the OMOP
        CONCEPT table (if available).

        For columns whose name ends in '_source_value', additionally computes
        coverage of the distinct source values against the CONCEPT table's
        concept_code field, written to a small _coverage CSV.

        Also writes a per-group ``_summary.csv`` (one row per profiled
        column) and ``_group_meta.csv`` (group identity + source file list)
        so the PDF rebuild helper can reconstruct group context without
        re-running Spark.

        Args:
            partner: Partner identifier (lowercased).
            folder_name: Data delivery folder (e.g. 'UCI/20251121').
            group_name: Logical group name; used as the output subdirectory
                and as the section title in the PDF report. For groups of
                a single file this typically equals the user's ``-t`` pattern
                (or the file stem, when ``-t all`` is used).
            file_paths: Non-empty list of absolute paths to the raw input
                files in this group. Profiling is done on the row union.
            field_patterns: List of fnmatch glob patterns for column selection.
            concept_table_path: Absolute path to the OMOP CONCEPT.csv (may be missing).

        Returns:
            Dict describing the work done, used by the report generator::

                {
                    'group_name': str,
                    'pattern': str,            # informational; from caller, may be ''
                    'source_files': list[str], # absolute paths
                    'file_path': str,          # alias = source_files[0] (back-compat)
                    'file_stem': str,          # alias = group_name (back-compat)
                    'output_dir': str,
                    'total_rows': int,         # row count across the unioned group
                    'columns': list[dict],     # one entry per profiled column
                    'errors': list[str],
                }
        """
        # Normalize and guard arguments. We accept legacy single-string
        # callers transparently; other callers must always pass a list.
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        elif not isinstance(file_paths, (list, tuple)):
            file_paths = list(file_paths)
        file_paths = [str(p) for p in file_paths]

        first_file = file_paths[0] if file_paths else ""

        result: dict = {
            "group_name": group_name,
            "pattern": "",
            "source_files": list(file_paths),
            # Back-compat aliases for callers that still expect the
            # single-file return shape (notably the resume helper script).
            "file_path": first_file,
            "file_stem": group_name,
            "output_dir": "",
            "total_rows": 0,
            "columns": [],
            "errors": [],
        }

        if not file_paths:
            err = "no source files supplied to get_raw_distinct_values"
            _get_ovid_logger().error(f"[raw_dv] {err}")
            result["errors"].append(f"fatal:{err}")
            return result

        try:
            output_dir = (
                f"/app/partners/{partner.lower()}/data/{folder_name}/raw_distinct_values/{group_name}/"
            )
            os.makedirs(output_dir, exist_ok=True)
            result["output_dir"] = output_dir

            master_url = os.environ["SPARK_URL"].strip().replace('"', "")
            spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()
            spark.conf.set("spark.sql.shuffle.partitions", 2000)

            df = cls._raw_dv_spark_read(file_paths, spark)
            total_rows = df.count()
            result["total_rows"] = total_rows

            if total_rows == 0:
                _get_ovid_logger().warning(
                    f"[raw_dv] group '{group_name}' ({len(file_paths)} file(s)) has zero rows; skipping."
                )
                # Still emit the group meta so the PDF/rebuild helper can
                # show the empty group instead of silently dropping it.
                cls._raw_dv_write_group_meta(output_dir, group_name, "", file_paths, total_rows)
                return result

            selected_cols = cls._raw_dv_match_columns(df.columns, field_patterns)
            if not selected_cols:
                _get_ovid_logger().warning(
                    f"[raw_dv] no columns in group '{group_name}' matched field patterns {field_patterns}"
                )
                cls._raw_dv_write_group_meta(output_dir, group_name, "", file_paths, total_rows)
                return result

            concept_df = cls._raw_dv_load_concept_table(concept_table_path, spark)
            concept_codes_set: set = set()
            if concept_df is not None:
                # Cache once per group; the set is needed only for source_value
                # coverage, so collect lazily.
                concept_df = concept_df.cache()

            for col_name in selected_cols:
                try:
                    is_concept_id = col_name.lower().endswith("_concept_id")
                    is_source_value = col_name.lower().endswith("_source_value")

                    base_dv = (
                        df.groupBy(col_name)
                        .agg(count("*").alias("count"))
                        .withColumn("percent", F.round((F.col("count") / total_rows * 100), 4))
                    )

                    if is_concept_id and concept_df is not None:
                        # Cast both sides to string for a robust join; concept_id in
                        # raw OMOP files is text, and CONCEPT.csv is read as string.
                        enriched = (
                            base_dv.alias("dv")
                            .join(
                                concept_df.alias("c"),
                                F.col(f"dv.`{col_name}`") == F.col("c.concept_id"),
                                "left",
                            )
                            .select(
                                F.col(f"dv.`{col_name}`").alias(col_name),
                                F.col("dv.count"),
                                F.col("dv.percent"),
                                F.col("c.concept_name").alias("concept_name"),
                                F.col("c.concept_code").alias("concept_code"),
                                F.col("c.vocabulary_id").alias("vocabulary_id"),
                                F.col("c.domain_id").alias("domain_id"),
                            )
                            .orderBy(F.col("count").desc())
                        )
                    else:
                        enriched = base_dv.orderBy(F.col("count").desc())

                    output_csv_name = f"{col_name.lower()}_distinct_values.csv"
                    cls.write_pyspark_output_file(enriched, output_csv_name, output_dir)

                    # Build summary stats. Pull a small sample (top row) to driver
                    # via a separate small action to avoid recomputing the whole
                    # groupBy after writing.
                    distinct_count = base_dv.count()
                    null_count_row = (
                        base_dv.filter(
                            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")
                        )
                        .agg(F.sum("count").alias("null_count"))
                        .collect()
                    )
                    null_count_val = null_count_row[0]["null_count"] if null_count_row else 0
                    null_count_val = int(null_count_val) if null_count_val is not None else 0

                    top_row = base_dv.orderBy(F.col("count").desc()).limit(1).collect()
                    if top_row:
                        top_value = top_row[0][col_name]
                        top_count = int(top_row[0]["count"])
                        top_percent = float(top_row[0]["percent"])
                    else:
                        top_value, top_count, top_percent = None, 0, 0.0

                    column_info = {
                        "column": col_name,
                        "is_concept_id": is_concept_id,
                        "is_source_value": is_source_value,
                        "distinct_count": int(distinct_count),
                        "null_count": null_count_val,
                        "null_pct": builtins.round((null_count_val / total_rows * 100), 4) if total_rows else 0.0,
                        "top_value": top_value if top_value is not None else "",
                        "top_count": top_count,
                        "top_percent": top_percent,
                        "csv_path": os.path.join(output_dir, output_csv_name),
                        "coverage_pct": None,
                        "coverage_unmapped_count": None,
                        "coverage_top_unmapped": [],
                    }

                    # Source value vs concept_code coverage
                    if is_source_value and concept_df is not None:
                        try:
                            distinct_values_df = (
                                df.select(F.col(col_name).alias("raw_value"))
                                .filter(F.col("raw_value").isNotNull() & (F.trim(F.col("raw_value")) != ""))
                                .distinct()
                            )
                            distinct_count_estimate = distinct_values_df.count()

                            if distinct_count_estimate <= RAW_DV_SOURCE_VALUE_COLLECT_LIMIT:
                                # Safe to materialize
                                if not concept_codes_set:
                                    concept_codes_set = {
                                        r["concept_code"]
                                        for r in concept_df.select("concept_code").distinct().collect()
                                        if r["concept_code"] is not None
                                    }
                                raw_values = [
                                    r["raw_value"]
                                    for r in distinct_values_df.collect()
                                    if r["raw_value"] is not None
                                ]
                                matched = [v for v in raw_values if v in concept_codes_set]
                                unmapped = [v for v in raw_values if v not in concept_codes_set]
                                coverage_pct = (
                                    builtins.round(len(matched) / len(raw_values) * 100, 4)
                                    if raw_values
                                    else 0.0
                                )
                                column_info["coverage_pct"] = coverage_pct
                                column_info["coverage_unmapped_count"] = len(unmapped)
                                column_info["coverage_top_unmapped"] = unmapped[:20]
                            else:
                                # Too many distinct values - use Spark anti-join
                                unmapped_df = distinct_values_df.join(
                                    concept_df.select(F.col("concept_code").alias("raw_value")).distinct(),
                                    on="raw_value",
                                    how="left_anti",
                                )
                                unmapped_count = unmapped_df.count()
                                coverage_pct = (
                                    builtins.round(
                                        (distinct_count_estimate - unmapped_count)
                                        / distinct_count_estimate
                                        * 100,
                                        4,
                                    )
                                    if distinct_count_estimate
                                    else 0.0
                                )
                                column_info["coverage_pct"] = coverage_pct
                                column_info["coverage_unmapped_count"] = int(unmapped_count)
                                column_info["coverage_top_unmapped"] = []
                        except Exception as cov_exc:
                            _get_ovid_logger().error(
                                f"[raw_dv] coverage analysis failed for {col_name} in group '{group_name}': {cov_exc}",
                                exc_info=True,
                            )
                            result["errors"].append(f"coverage:{col_name}:{cov_exc}")

                    result["columns"].append(column_info)

                except Exception as col_exc:
                    _get_ovid_logger().error(
                        f"[raw_dv] failed to profile column {col_name} in group '{group_name}': {col_exc}",
                        exc_info=True,
                    )
                    result["errors"].append(f"column:{col_name}:{col_exc}")

            # Sort columns by priority so the per-group _summary.csv, the
            # page-1 TOC, and the per-group rendered section all show
            # *_type_concept_id first, then *_concept_id, then
            # *_source_concept_id, then *_source_value, then everything
            # else. Within each tier, original source-file column order is
            # preserved (stable sort + index tiebreaker).
            result["columns"] = [
                c for _, _, c in sorted(
                    [
                        (cls._raw_dv_column_priority(c["column"]), idx, c)
                        for idx, c in enumerate(result["columns"])
                    ],
                    key=lambda t: (t[0], t[1]),
                )
            ]

            # Per-group _summary.csv (small enough for pandas)
            try:
                summary_rows = [
                    {
                        "column": c["column"],
                        "total_rows": total_rows,
                        "distinct_count": c["distinct_count"],
                        "null_count": c["null_count"],
                        "null_pct": c["null_pct"],
                        "top_value": c["top_value"],
                        "top_count": c["top_count"],
                        "top_percent": c["top_percent"],
                        "is_concept_id": c["is_concept_id"],
                        "is_source_value": c["is_source_value"],
                        "coverage_pct": c["coverage_pct"]
                        if c["coverage_pct"] is not None
                        else "",
                    }
                    for c in result["columns"]
                ]
                pd.DataFrame(summary_rows).to_csv(
                    os.path.join(output_dir, "_summary.csv"),
                    sep="\t",
                    index=False,
                )
            except Exception as sum_exc:
                _get_ovid_logger().error(
                    f"[raw_dv] failed to write _summary.csv for group '{group_name}': {sum_exc}",
                    exc_info=True,
                )
                result["errors"].append(f"summary:{sum_exc}")

            # Per-group _group_meta.csv (records identity + source files so
            # the rebuild helper can reconstruct context without re-running
            # the Spark distinct-value computation).
            cls._raw_dv_write_group_meta(
                output_dir, group_name, result.get("pattern", ""), file_paths, total_rows
            )

            return result

        except Exception as exc:
            _get_ovid_logger().error(
                f"[raw_dv] get_raw_distinct_values failed for group '{group_name}' "
                f"({len(file_paths)} file(s)): {exc}",
                exc_info=True,
            )
            cls.print_failure_message(
                folder=folder_name,
                partner=partner,
                job=f"raw_distinct_values group '{group_name}'",
                text=str(exc),
            )
            result["errors"].append(f"fatal:{exc}")
            return result


    @classmethod
    def _raw_dv_write_group_meta(
        cls,
        output_dir: str,
        group_name: str,
        pattern: str,
        file_paths: list,
        total_rows: int,
    ) -> None:
        """Write ``_group_meta.csv`` describing the logical group.

        The file lets the PDF rebuild helper reconstruct group identity
        (group name, originating ``-t`` pattern, list of source files,
        union row count) without re-running the Spark distinct-value
        computation. Failures are logged but never raised: the meta file
        is a convenience artifact, not a correctness requirement.

        Args:
            output_dir: Group output directory (where _summary.csv lives).
            group_name: Logical group name (matches subdir name).
            pattern: User's ``-t`` pattern that produced this group; may
                be the empty string when not known by the caller.
            file_paths: Absolute paths of every file in the group.
            total_rows: Row count across the unioned group.
        """
        try:
            meta_rows = [
                {
                    "group_name": group_name,
                    "pattern": pattern,
                    "source_file": os.path.basename(p),
                    "source_file_path": p,
                    "group_total_rows": total_rows,
                }
                for p in file_paths
            ] or [
                {
                    "group_name": group_name,
                    "pattern": pattern,
                    "source_file": "",
                    "source_file_path": "",
                    "group_total_rows": total_rows,
                }
            ]
            pd.DataFrame(meta_rows).to_csv(
                os.path.join(output_dir, "_group_meta.csv"),
                sep="\t",
                index=False,
            )
        except Exception as exc:
            _get_ovid_logger().error(
                f"[raw_dv] failed to write _group_meta.csv for group '{group_name}': {exc}",
                exc_info=True,
            )


###################################################################################################################################
# Raw distinct values: PDF report generator (black & white, with clickable TOC and per-page return-to-TOC links)
###################################################################################################################################

    @classmethod
    def _raw_dv_pdf_make_link_paragraph(
        cls,
        text: str,
        dest_name: str,
        font_name: str = RAW_DV_PDF_BODY_FONT_BOLD,
        font_size: int = RAW_DV_PDF_BODY_FONT_SIZE,
    ):
        """Build a Paragraph that renders ``text`` as a black-underlined internal hyperlink.

        The link target is a named destination created elsewhere in the document
        via ``canvas.bookmarkPage(dest_name)``.

        Args:
            text: Display text for the link.
            dest_name: Name of the destination bookmark to jump to.
            font_name: Font family for the link text.
            font_size: Font size in points.

        Returns:
            A reportlab Paragraph flowable.
        """
        from reportlab.platypus import Paragraph
        from reportlab.lib.styles import ParagraphStyle

        style = ParagraphStyle(
            name="RawDvLink",
            fontName=font_name,
            fontSize=font_size,
            leading=font_size + 2,
            textColor=_RL_COLORS.black,
        )
        # XML-escape the display text. Underline + black color overrides
        # reportlab's default blue link rendering.
        safe_text = (
            str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        markup = f'<a href="#{dest_name}"><u>{safe_text}</u></a>'
        return Paragraph(markup, style)


    @classmethod
    def _raw_dv_pdf_wrap_cell(
        cls,
        text,
        font_name: str = RAW_DV_PDF_MONO_FONT,
        font_size: int = RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
        raw_markup: bool = False,
    ):
        """Wrap a piece of text in a Paragraph so reportlab Tables wrap long lines.

        Plain string cells in reportlab Tables do NOT wrap; a long concept_name
        will overflow the cell. Wrapping in a Paragraph forces text reflow.

        Args:
            text: Cell content (any type; coerced to string).
            font_name: Font family for the cell.
            font_size: Font size in points.
            raw_markup: When False (default), the text is treated as plain
                content and ``&``/``<``/``>`` are XML-escaped. When True, the
                text is passed through unmodified so callers can include
                inline markup like ``<b>...</b>``. Callers using ``raw_markup``
                are responsible for escaping any user-supplied substrings.

        Returns:
            A reportlab Paragraph flowable.
        """
        from reportlab.platypus import Paragraph
        from reportlab.lib.styles import ParagraphStyle

        style = ParagraphStyle(
            name="RawDvCell",
            fontName=font_name,
            fontSize=font_size,
            leading=font_size + 1,
            textColor=_RL_COLORS.black,
            wordWrap="CJK",  # break inside long unbroken tokens
        )
        if text is None:
            text = ""
        s = str(text)
        if not raw_markup:
            s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return Paragraph(s, style)


    @classmethod
    def _raw_dv_pdf_build_table(
        cls,
        headers: list,
        body_rows: list,
        col_widths: list,
        body_font_size: int,
    ):
        """Build a reportlab LongTable for a column block with B&W styling.

        Args:
            headers: List of header column labels.
            body_rows: List of body rows (each a list of cells, may contain Paragraph or str).
            col_widths: Pre-computed column widths in points.
            body_font_size: Body font size for non-Paragraph cells.

        Returns:
            A reportlab LongTable flowable.
        """
        from reportlab.platypus import LongTable, TableStyle

        # Wrap the headers in Paragraphs so they wrap if narrow.
        header_row = [
            cls._raw_dv_pdf_wrap_cell(
                h,
                font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                font_size=RAW_DV_PDF_TABLE_HEADER_FONT_SIZE,
            )
            for h in headers
        ]
        data = [header_row] + body_rows

        tbl = LongTable(data, colWidths=col_widths, repeatRows=1)

        style_cmds = [
            # Header row styling
            ("BACKGROUND", (0, 0), (-1, 0), RAW_DV_PDF_HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), _RL_COLORS.black),
            ("FONTNAME", (0, 0), (-1, 0), RAW_DV_PDF_BODY_FONT_BOLD),
            ("FONTSIZE", (0, 0), (-1, 0), RAW_DV_PDF_TABLE_HEADER_FONT_SIZE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (-1, 0), "LEFT"),
            # Grid
            ("GRID", (0, 0), (-1, -1), RAW_DV_PDF_GRID_WIDTH, RAW_DV_PDF_GRID_COLOR),
            # Body padding
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]

        # Alternating-row banding (5% grey on every other body row)
        for body_idx in range(len(body_rows)):
            if body_idx % 2 == 1:
                row = body_idx + 1  # +1 because data has the header at index 0
                style_cmds.append(
                    ("BACKGROUND", (0, row), (-1, row), RAW_DV_PDF_ALT_ROW_BG)
                )

        tbl.setStyle(TableStyle(style_cmds))
        return tbl


    @classmethod
    def _raw_dv_pdf_build_column_block(
        cls,
        col_info: dict,
        file_idx: int,
        col_idx: int,
        usable_width: float,
        df_col=None,
        truncated_hint: bool = False,
    ) -> list:
        """Build the flowables for one column block (heading, stats, data table, footnote).

        Args:
            col_info: One entry from get_raw_distinct_values()'s columns list.
            file_idx: Index of the parent file (for bookmark naming).
            col_idx: Index of this column within the file (for bookmark naming).
            usable_width: Available content width on the page in points.
            df_col: Optional pre-loaded pandas DataFrame for the per-column CSV.
                When provided, the function skips re-reading the CSV. Used by
                callers that have already loaded the CSV (e.g. to build a vocab
                breakdown for the same column).
            truncated_hint: If df_col was already truncated to RAW_DV_TOP_N rows,
                pass True so the truncation footnote still renders. Ignored when
                df_col is None (the function reads and detects truncation itself).

        Returns:
            List of reportlab flowables.
        """
        from reportlab.platypus import Paragraph, Spacer
        from reportlab.lib.styles import ParagraphStyle

        flowables: list = []
        is_concept = col_info["is_concept_id"]

        # Heading paragraph with anchor bookmark embedded via reportlab's <a name="...">
        heading_style = ParagraphStyle(
            name="RawDvColHeading",
            fontName=RAW_DV_PDF_BODY_FONT_BOLD,
            fontSize=RAW_DV_PDF_COL_HEADING_FONT_SIZE,
            leading=RAW_DV_PDF_COL_HEADING_FONT_SIZE + 3,
            textColor=_RL_COLORS.black,
            spaceAfter=2,
        )
        bookmark_name = f"file_{file_idx}_col_{col_idx}"
        col_name_safe = (
            str(col_info["column"])
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        flowables.append(
            Paragraph(
                f'<a name="{bookmark_name}"/>{col_name_safe}',
                heading_style,
            )
        )

        # Stats line
        stats_style = ParagraphStyle(
            name="RawDvColStats",
            fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
            fontSize=RAW_DV_PDF_BODY_FONT_SIZE - 1,
            leading=RAW_DV_PDF_BODY_FONT_SIZE + 1,
            textColor=_RL_COLORS.black,
            spaceAfter=4,
        )
        stats_parts = [
            f"total rows = {col_info.get('total_rows_for_column', 0):,}"
            if col_info.get("total_rows_for_column") is not None
            else None,
            f"distinct = {col_info['distinct_count']:,}",
            f"null = {col_info['null_count']:,} ({col_info['null_pct']}%)",
        ]
        if col_info.get("coverage_pct") is not None:
            stats_parts.append(f"source-value coverage = {col_info['coverage_pct']}%")
        stats_line = "  |  ".join(p for p in stats_parts if p)
        flowables.append(Paragraph(stats_line, stats_style))

        # Build the table
        if is_concept:
            headers = [
                "value",
                "count",
                "percent",
                "concept_name",
                "concept_code",
                "vocabulary_id",
                "domain_id",
            ]
            # 7 cols: value gets 1.0", count 0.7", percent 0.6", concept_name 2.6",
            # concept_code 1.0", vocabulary_id 0.8", domain_id 0.8" -> 7.5"
            col_widths = [
                1.0 * _RL_INCH,
                0.7 * _RL_INCH,
                0.6 * _RL_INCH,
                2.6 * _RL_INCH,
                1.0 * _RL_INCH,
                0.8 * _RL_INCH,
                0.8 * _RL_INCH,
            ]
            body_font_size = RAW_DV_PDF_NARROW_TABLE_FONT_SIZE
        else:
            headers = ["value", "count", "percent"]
            # 3 cols: value 4.5", count 1.5", percent 1.5" -> 7.5"
            col_widths = [
                4.5 * _RL_INCH,
                1.5 * _RL_INCH,
                1.5 * _RL_INCH,
            ]
            body_font_size = RAW_DV_PDF_TABLE_BODY_FONT_SIZE

        # Adjust the widths if the page's usable width is smaller than 7.5"
        sum_widths = sum(col_widths)
        if sum_widths > usable_width:
            scale = usable_width / sum_widths
            col_widths = [w * scale for w in col_widths]

        # Resolve the per-column data: either use the caller-supplied DataFrame
        # (e.g. when the vocab-breakdown helper already loaded it) or read it
        # from disk. Detection of truncation only happens when we read it ourselves.
        csv_path = col_info["csv_path"]
        body_rows: list = []
        truncated = bool(truncated_hint)
        try:
            if df_col is None and os.path.isfile(csv_path):
                df_col = pd.read_csv(
                    csv_path,
                    sep="\t",
                    dtype=str,
                    keep_default_na=False,
                    na_values=[""],
                    nrows=RAW_DV_TOP_N + 1,
                )
                if len(df_col) > RAW_DV_TOP_N:
                    truncated = True
                    df_col = df_col.head(RAW_DV_TOP_N)

            # Concept-enriched columns: re-sort the (already-truncated) top-N
            # rows by vocabulary_id desc, domain_id desc, percent desc so
            # doctors can browse concepts grouped by vocabulary. Falls back
            # silently to the original count-desc order when sort keys are
            # absent (e.g. CONCEPT enrichment was skipped). Note that this
            # reorders the top-N-BY-COUNT rows by vocabulary -- it does NOT
            # surface the top concepts WITHIN each vocabulary that fell
            # below the global count cap.
            if (
                is_concept
                and df_col is not None
                and {"vocabulary_id", "domain_id", "percent"}.issubset(df_col.columns)
            ):
                try:
                    df_col = (
                        df_col.assign(
                            _pct_numeric=pd.to_numeric(
                                df_col["percent"], errors="coerce"
                            ).fillna(0.0)
                        )
                        .sort_values(
                            by=["vocabulary_id", "domain_id", "_pct_numeric"],
                            ascending=[False, False, False],
                            na_position="last",
                        )
                        .drop(columns=["_pct_numeric"])
                        .reset_index(drop=True)
                    )
                except Exception as sort_exc:
                    _get_ovid_logger().warning(
                        f"[raw_dv] failed to apply vocabulary sort for "
                        f"{col_info['column']}: {sort_exc}"
                    )

            if df_col is not None:
                for _, row in df_col.iterrows():
                    cells = [
                        cls._raw_dv_pdf_wrap_cell(
                            row.get(col_info["column"], ""),
                            font_name=RAW_DV_PDF_MONO_FONT,
                            font_size=body_font_size,
                        ),
                        cls._raw_dv_pdf_wrap_cell(
                            row.get("count", ""),
                            font_name=RAW_DV_PDF_MONO_FONT,
                            font_size=body_font_size,
                        ),
                        cls._raw_dv_pdf_wrap_cell(
                            row.get("percent", ""),
                            font_name=RAW_DV_PDF_MONO_FONT,
                            font_size=body_font_size,
                        ),
                    ]
                    if is_concept:
                        cells.extend(
                            [
                                cls._raw_dv_pdf_wrap_cell(
                                    row.get("concept_name", ""),
                                    font_name=RAW_DV_PDF_BODY_FONT,
                                    font_size=body_font_size,
                                ),
                                cls._raw_dv_pdf_wrap_cell(
                                    row.get("concept_code", ""),
                                    font_name=RAW_DV_PDF_MONO_FONT,
                                    font_size=body_font_size,
                                ),
                                cls._raw_dv_pdf_wrap_cell(
                                    row.get("vocabulary_id", ""),
                                    font_name=RAW_DV_PDF_BODY_FONT,
                                    font_size=body_font_size,
                                ),
                                cls._raw_dv_pdf_wrap_cell(
                                    row.get("domain_id", ""),
                                    font_name=RAW_DV_PDF_BODY_FONT,
                                    font_size=body_font_size,
                                ),
                            ]
                        )
                    body_rows.append(cells)
        except Exception as read_exc:
            _get_ovid_logger().error(
                f"[raw_dv] failed to read {csv_path} into PDF: {read_exc}",
                exc_info=True,
            )
            body_rows = []

        if body_rows:
            flowables.append(
                cls._raw_dv_pdf_build_table(headers, body_rows, col_widths, body_font_size)
            )
        else:
            note_style = ParagraphStyle(
                name="RawDvNote",
                fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
                fontSize=RAW_DV_PDF_BODY_FONT_SIZE,
                leading=RAW_DV_PDF_BODY_FONT_SIZE + 2,
                textColor=_RL_COLORS.black,
            )
            flowables.append(
                Paragraph(f"(no per-column CSV found at {csv_path})", note_style)
            )

        if truncated:
            remaining = max(col_info["distinct_count"] - RAW_DV_TOP_N, 0)
            note_style = ParagraphStyle(
                name="RawDvTrunc",
                fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
                fontSize=RAW_DV_PDF_BODY_FONT_SIZE - 1,
                leading=RAW_DV_PDF_BODY_FONT_SIZE + 1,
                textColor=_RL_COLORS.black,
                spaceBefore=2,
            )
            flowables.append(
                Paragraph(
                    f"... and {remaining:,} more values "
                    f"(full data in {os.path.basename(csv_path)})",
                    note_style,
                )
            )

        flowables.append(Spacer(1, 0.18 * _RL_INCH))
        return flowables


    @classmethod
    def _raw_dv_column_has_vocab_columns(cls, csv_path: str) -> bool:
        """Return True iff a per-column distinct-values CSV carries vocab enrichment.

        Used as a unified gate for the page-1 TOC, the per-file mini-TOC, and
        the per-file section builder so that links to the vocabulary-mix
        breakdown bookmark (``file_X_col_Y_vocab``) are only emitted when the
        breakdown will actually render. The breakdown renderer
        (``_raw_dv_pdf_build_vocab_breakdown``) requires a ``vocabulary_id``
        column in the per-column CSV; when CONCEPT enrichment was skipped or
        failed for any reason, the column is absent and the renderer returns an
        empty flowable list. Without this gate, those callers would emit links
        to a non-existent destination, causing reportlab to fail PDF
        finalization with "format not resolved".

        Args:
            csv_path: Absolute path to the per-column CSV file.

        Returns:
            True if the file exists and its header includes ``vocabulary_id``;
            False on missing file, unreadable header, or absent column.
        """
        if not csv_path or not os.path.isfile(csv_path):
            return False
        try:
            header_df = pd.read_csv(
                csv_path,
                sep="\t",
                dtype=str,
                keep_default_na=False,
                na_values=[""],
                nrows=0,
            )
            return "vocabulary_id" in header_df.columns
        except Exception as exc:
            _get_ovid_logger().warning(
                f"[raw_dv] failed to inspect header of {csv_path} "
                f"for vocab-column gate: {exc}"
            )
            return False


    @classmethod
    def _raw_dv_pdf_load_column_csv(cls, csv_path: str):
        """Read a per-column CSV (capped at RAW_DV_TOP_N rows) into a pandas DataFrame.

        Returns a tuple ``(df_or_None, truncated_bool)``. ``df_or_None`` is None
        when the file is missing or cannot be parsed; ``truncated_bool`` is True
        when the file had more than RAW_DV_TOP_N rows.

        Args:
            csv_path: Absolute path to the per-column CSV file.

        Returns:
            Tuple of (DataFrame|None, truncated_flag).
        """
        if not os.path.isfile(csv_path):
            return None, False
        try:
            df_col = pd.read_csv(
                csv_path,
                sep="\t",
                dtype=str,
                keep_default_na=False,
                na_values=[""],
                nrows=RAW_DV_TOP_N + 1,
            )
            truncated = len(df_col) > RAW_DV_TOP_N
            if truncated:
                df_col = df_col.head(RAW_DV_TOP_N)
            return df_col, truncated
        except Exception as exc:
            _get_ovid_logger().error(
                f"[raw_dv] failed to read {csv_path}: {exc}",
                exc_info=True,
            )
            return None, False


    @classmethod
    def _raw_dv_pdf_build_vocab_breakdown(
        cls,
        col_info: dict,
        file_idx: int,
        col_idx: int,
        usable_width: float,
        df_col,
        total_rows: int,
    ) -> list:
        """Build the flowables for a vocabulary/domain breakdown table.

        Aggregates the per-column CSV by ``(vocabulary_id, domain_id)`` and
        renders a small table showing how many rows fall into each vocabulary
        bucket. Rows with null/empty vocabulary_id are collapsed into a single
        "(unmapped)" row pinned at the bottom.

        Args:
            col_info: One entry from get_raw_distinct_values()'s columns list.
            file_idx: Index of the parent file (for bookmark naming).
            col_idx: Index of this column within the file (for bookmark naming).
            usable_width: Available content width on the page in points.
            df_col: Pre-loaded pandas DataFrame of the per-column CSV.
            total_rows: Total row count of the source file (denominator for %).

        Returns:
            List of reportlab flowables. Empty list if no breakdown can be built
            (e.g. df_col missing vocabulary_id column).
        """
        from reportlab.platypus import Paragraph, Spacer
        from reportlab.lib.styles import ParagraphStyle

        # If the per-column CSV doesn't carry vocabulary_id (CONCEPT enrichment
        # was off or the column wasn't enriched), skip the breakdown silently.
        if df_col is None or "vocabulary_id" not in df_col.columns:
            return []

        flowables: list = []
        bookmark_name = f"file_{file_idx}_col_{col_idx}_vocab"
        col_name_safe = (
            str(col_info["column"])
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

        # Heading + intro
        heading_style = ParagraphStyle(
            name="RawDvVocabHeading",
            fontName=RAW_DV_PDF_BODY_FONT_BOLD,
            fontSize=RAW_DV_PDF_COL_HEADING_FONT_SIZE,
            leading=RAW_DV_PDF_COL_HEADING_FONT_SIZE + 3,
            textColor=_RL_COLORS.black,
            spaceAfter=2,
        )
        flowables.append(
            Paragraph(
                f'<a name="{bookmark_name}"/>'
                f"Vocabulary mix \u2014 {col_name_safe}",
                heading_style,
            )
        )

        intro_style = ParagraphStyle(
            name="RawDvVocabIntro",
            fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
            fontSize=RAW_DV_PDF_BODY_FONT_SIZE - 1,
            leading=RAW_DV_PDF_BODY_FONT_SIZE + 1,
            textColor=_RL_COLORS.black,
            spaceAfter=4,
        )
        flowables.append(
            Paragraph(
                "Distribution of <b>vocabulary_id</b> &times; <b>domain_id</b> "
                "across all rows of this column. The vocab profile reveals the "
                "source-system mix (e.g. SNOMED vs ICD10 vs CPT4) without "
                "having to read every row.",
                intro_style,
            )
        )

        # Aggregate. The 'count' column in the per-column CSV is the row-count
        # for each distinct concept_id; summing it by (vocabulary_id, domain_id)
        # yields the row-count per vocabulary.
        try:
            df = df_col.copy()
            # Coerce count to int; treat unparseable as 0.
            df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype("int64")
            # Mark unmapped rows (null/empty vocabulary).
            if "domain_id" not in df.columns:
                df["domain_id"] = ""
            df["__vocab_norm"] = df["vocabulary_id"].fillna("").astype(str).str.strip()
            df["__domain_norm"] = df["domain_id"].fillna("").astype(str).str.strip()
            df["__is_unmapped"] = df["__vocab_norm"].isin(["", "None"])

            grouped = (
                df.groupby(["__is_unmapped", "__vocab_norm", "__domain_norm"], dropna=False)["count"]
                .sum()
                .reset_index()
            )

            # Split into mapped (sorted by count desc) and unmapped (single row at bottom)
            mapped = grouped[~grouped["__is_unmapped"]].sort_values("count", ascending=False)
            unmapped_total = int(grouped.loc[grouped["__is_unmapped"], "count"].sum())
        except Exception as agg_exc:
            _get_ovid_logger().error(
                f"[raw_dv] vocab breakdown aggregation failed for {col_info['column']}: {agg_exc}",
                exc_info=True,
            )
            return flowables  # heading + intro already appended; just stop here

        # Build body rows
        body_rows: list = []

        def _pct(n: int) -> str:
            if total_rows <= 0:
                return "0.0%"
            return f"{(n / total_rows * 100):.2f}%"

        body_font_size = RAW_DV_PDF_TABLE_BODY_FONT_SIZE
        for _, r in mapped.iterrows():
            n = int(r["count"])
            body_rows.append(
                [
                    cls._raw_dv_pdf_wrap_cell(
                        r["__vocab_norm"] or "(unknown)",
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=body_font_size,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        r["__domain_norm"] or "\u2014",
                        font_name=RAW_DV_PDF_BODY_FONT,
                        font_size=body_font_size,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        f"{n:,}",
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=body_font_size,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        _pct(n),
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=body_font_size,
                    ),
                ]
            )

        if unmapped_total > 0:
            body_rows.append(
                [
                    cls._raw_dv_pdf_wrap_cell(
                        "(unmapped)",
                        font_name=RAW_DV_PDF_BODY_FONT_ITALIC,
                        font_size=body_font_size,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        "\u2014",
                        font_name=RAW_DV_PDF_BODY_FONT,
                        font_size=body_font_size,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        f"{unmapped_total:,}",
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=body_font_size,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        _pct(unmapped_total),
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=body_font_size,
                    ),
                ]
            )

        # Total row (bold)
        grand_total = int(mapped["count"].sum()) + unmapped_total
        body_rows.append(
            [
                cls._raw_dv_pdf_wrap_cell(
                    "<b>Total</b>",
                    font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                    font_size=body_font_size,
                    raw_markup=True,
                ),
                cls._raw_dv_pdf_wrap_cell(
                    "",
                    font_name=RAW_DV_PDF_BODY_FONT,
                    font_size=body_font_size,
                ),
                cls._raw_dv_pdf_wrap_cell(
                    f"<b>{grand_total:,}</b>",
                    font_name=RAW_DV_PDF_MONO_FONT_BOLD,
                    font_size=body_font_size,
                    raw_markup=True,
                ),
                cls._raw_dv_pdf_wrap_cell(
                    f"<b>{_pct(grand_total)}</b>",
                    font_name=RAW_DV_PDF_MONO_FONT_BOLD,
                    font_size=body_font_size,
                    raw_markup=True,
                ),
            ]
        )

        # If there's nothing meaningful to show (no mapped + no unmapped rows
        # and Total = 0), just emit a small note instead of an empty table.
        if not mapped.shape[0] and unmapped_total == 0:
            note_style = ParagraphStyle(
                name="RawDvVocabEmpty",
                fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
                fontSize=RAW_DV_PDF_BODY_FONT_SIZE,
                leading=RAW_DV_PDF_BODY_FONT_SIZE + 2,
                textColor=_RL_COLORS.black,
            )
            flowables.append(
                Paragraph(
                    "(no vocabulary data available for this column \u2014 "
                    "CONCEPT enrichment may have been skipped)",
                    note_style,
                )
            )
            flowables.append(Spacer(1, 0.12 * _RL_INCH))
            return flowables

        headers = ["vocabulary_id", "domain_id", "rows", "% of file"]
        # 4 cols: 1.6" + 1.6" + 1.4" + 0.9" = 5.5" -> fits in 7.5" usable width
        col_widths = [
            1.6 * _RL_INCH,
            1.6 * _RL_INCH,
            1.4 * _RL_INCH,
            0.9 * _RL_INCH,
        ]
        sum_widths = sum(col_widths)
        if sum_widths > usable_width:
            scale = usable_width / sum_widths
            col_widths = [w * scale for w in col_widths]

        flowables.append(
            cls._raw_dv_pdf_build_table(headers, body_rows, col_widths, body_font_size)
        )
        flowables.append(Spacer(1, 0.15 * _RL_INCH))
        return flowables


    @classmethod
    def _raw_dv_pdf_build_file_section(
        cls,
        fr: dict,
        file_idx: int,
        usable_width: float,
    ) -> list:
        """Build the flowables for one source file's section.

        Includes a bookmark on the file title, a mini-TOC of column-name links,
        and one column block per profiled column. For ``*_concept_id`` (non-type)
        and ``*_source_concept_id`` columns, a small vocabulary-mix breakdown
        table is rendered immediately above the column block, with its own
        bookmark and TOC entry. Columns are rendered in priority order
        (type concepts first, then concepts, then source concepts, then
        source values, then anything else).

        Args:
            fr: One entry from file_results (return value of get_raw_distinct_values()).
            file_idx: Index of this file (for bookmark naming).
            usable_width: Available content width on the page in points.

        Returns:
            List of reportlab flowables (starts with a PageBreak so the section
            always begins on a new page).
        """
        from reportlab.platypus import Paragraph, PageBreak, Spacer
        from reportlab.lib.styles import ParagraphStyle

        flowables: list = [PageBreak()]

        # Section title: prefer the logical group name (new shape) and fall
        # back to the source filename for legacy single-file results.
        group_name = fr.get("group_name") or fr.get("file_stem") or ""
        source_files = fr.get("source_files") or [fr.get("file_path", "")]
        source_files = [s for s in source_files if s]

        if group_name:
            section_label = group_name
        elif source_files:
            section_label = os.path.basename(source_files[0])
        else:
            section_label = "(unknown)"

        # When the group contains multiple files, append the count to the
        # title for at-a-glance visibility (e.g. "measurement (3 files)").
        if len(source_files) > 1:
            section_label = f"{section_label} ({len(source_files)} files)"

        file_label_safe = (
            section_label.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )

        # Defensive re-sort: even if the caller already sorted, re-apply the
        # priority order so this function is robust against unsorted callers.
        original_columns = fr.get("columns", []) or []
        sorted_columns = [
            c for _, _, c in sorted(
                [
                    (cls._raw_dv_column_priority(c["column"]), idx, c)
                    for idx, c in enumerate(original_columns)
                ],
                key=lambda t: (t[0], t[1]),
            )
        ]

        # First, process all per-column sections to create bookmarks
        column_flowables: list = []
        for col_idx, col_info in enumerate(sorted_columns):
            col_info_with_total = dict(col_info)
            col_info_with_total["total_rows_for_column"] = fr["total_rows"]

            df_col, truncated = cls._raw_dv_pdf_load_column_csv(col_info["csv_path"])

            if (
                cls._raw_dv_column_priority(col_info["column"]) in (1, 2)
                and cls._raw_dv_column_has_vocab_columns(col_info.get("csv_path", ""))
            ):  # *_concept_id or *_source_concept_id with CONCEPT enrichment available
                # The breakdown aggregates over the full per-column CSV (not the
                # truncated top-N), so re-load without nrows. If the column has
                # millions of distinct concept_ids the per-column CSV is still
                # bounded by the underlying file's distinct count, which is
                # tractable in pandas.
                try:
                    full_df = pd.read_csv(
                        col_info["csv_path"],
                        sep="\t",
                        dtype=str,
                        keep_default_na=False,
                        na_values=[""],
                    )
                except Exception as exc:
                    _get_ovid_logger().error(
                        f"[raw_dv] failed to load full CSV for vocab breakdown of {col_info['column']}: {exc}",
                        exc_info=True,
                    )
                    full_df = df_col  # fall back to truncated frame

                column_flowables.extend(
                    cls._raw_dv_pdf_build_vocab_breakdown(
                        col_info_with_total,
                        file_idx,
                        col_idx,
                        usable_width,
                        full_df,
                        fr["total_rows"],
                    )
                )

            column_flowables.extend(
                cls._raw_dv_pdf_build_column_block(
                    col_info_with_total,
                    file_idx,
                    col_idx,
                    usable_width,
                    df_col=df_col,
                    truncated_hint=truncated,
                )
            )

        # File title with bookmark
        title_style = ParagraphStyle(
            name="RawDvFileTitle",
            fontName=RAW_DV_PDF_BODY_FONT_BOLD,
            fontSize=RAW_DV_PDF_FILE_TITLE_FONT_SIZE,
            leading=RAW_DV_PDF_FILE_TITLE_FONT_SIZE + 3,
            textColor=_RL_COLORS.black,
            spaceAfter=2,
        )
        flowables.append(
            Paragraph(
                f'<a name="file_{file_idx}"/>{file_label_safe}',
                title_style,
            )
        )

        # Meta line
        meta_style = ParagraphStyle(
            name="RawDvFileMeta",
            fontName=RAW_DV_PDF_BODY_FONT,
            fontSize=RAW_DV_PDF_BODY_FONT_SIZE,
            leading=RAW_DV_PDF_BODY_FONT_SIZE + 2,
            textColor=_RL_COLORS.black,
            spaceAfter=4,
        )
        flowables.append(
            Paragraph(
                f"Total rows: <b>{fr['total_rows']:,}</b>  |  "
                f"Columns profiled: <b>{len(sorted_columns)}</b>  |  "
                f"Top {RAW_DV_TOP_N:,} distinct values shown per column "
                f"(full data in per-column CSV files).",
                meta_style,
            )
        )

        # When the group contains multiple files, list them so the reader
        # can see exactly which physical inputs contributed to this section.
        # Single-file groups omit the line entirely (the section title
        # already shows the relevant identity).
        if len(source_files) > 1:
            source_list_style = ParagraphStyle(
                name="RawDvFileSources",
                fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
                fontSize=RAW_DV_PDF_BODY_FONT_SIZE - 1,
                leading=RAW_DV_PDF_BODY_FONT_SIZE + 1,
                textColor=_RL_COLORS.black,
                spaceAfter=4,
            )
            sources_safe = [
                os.path.basename(s)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                for s in source_files
            ]
            flowables.append(
                Paragraph(
                    f"Source files ({len(source_files)}): "
                    + ", ".join(sources_safe),
                    source_list_style,
                )
            )

        # Decide upfront which columns will get a vocab breakdown so the mini-TOC
        # can include matching entries. Both conditions must hold: the column
        # name suffix puts it in the breakdown-eligible tier (priority 1 or 2),
        # and its per-column CSV actually carries vocabulary_id (i.e. CONCEPT
        # enrichment ran successfully). Without the second check the mini-TOC
        # could emit links to bookmarks that the breakdown helper silently
        # skipped, causing reportlab "format not resolved" errors.
        def _column_has_vocab_breakdown(col_info_local: dict) -> bool:
            p = cls._raw_dv_column_priority(col_info_local["column"])
            if p not in (1, 2):  # 1=*_concept_id, 2=*_source_concept_id
                return False
            return cls._raw_dv_column_has_vocab_columns(
                col_info_local.get("csv_path", "")
            )

        # Mini-TOC: column-name links (interleaved with vocab-mix links where applicable).
        if sorted_columns:
            mini_style = ParagraphStyle(
                name="RawDvMiniToc",
                fontName=RAW_DV_PDF_BODY_FONT,
                fontSize=RAW_DV_PDF_BODY_FONT_SIZE - 1,
                leading=RAW_DV_PDF_BODY_FONT_SIZE + 1,
                textColor=_RL_COLORS.black,
                spaceAfter=6,
            )
            link_parts = []
            for col_idx, col_info in enumerate(sorted_columns):
                col_name = str(col_info["column"])
                col_safe = (
                    col_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                )
                if _column_has_vocab_breakdown(col_info):
                    link_parts.append(
                        f'<a href="#file_{file_idx}_col_{col_idx}_vocab"><u>{col_safe} (vocab)</u></a>'
                    )
                link_parts.append(
                    f'<a href="#file_{file_idx}_col_{col_idx}"><u>{col_safe}</u></a>'
                )
            flowables.append(
                Paragraph("Jump to: " + "  &middot;  ".join(link_parts), mini_style)
            )

        flowables.append(Spacer(1, 0.10 * _RL_INCH))
        
        # Now add the per-column flowables (which contain the bookmarks)
        flowables.extend(column_flowables)

        return flowables


    @classmethod
    def _raw_dv_pdf_build_coverage_section(
        cls,
        coverage_rows: list,
        usable_width: float,
    ) -> list:
        """Build the flowables for the source-value coverage summary section.

        Args:
            coverage_rows: List of (file_result, column_info) tuples for *_source_value
                columns that have coverage stats.
            usable_width: Available content width on the page in points.

        Returns:
            List of reportlab flowables (starts with a PageBreak).
        """
        from reportlab.platypus import Paragraph, PageBreak
        from reportlab.lib.styles import ParagraphStyle

        flowables: list = [PageBreak()]

        title_style = ParagraphStyle(
            name="RawDvCovTitle",
            fontName=RAW_DV_PDF_BODY_FONT_BOLD,
            fontSize=RAW_DV_PDF_FILE_TITLE_FONT_SIZE,
            leading=RAW_DV_PDF_FILE_TITLE_FONT_SIZE + 3,
            textColor=_RL_COLORS.black,
            spaceAfter=8,
        )
        flowables.append(
            Paragraph(
                '<a name="source_value_coverage"/>Source-Value Coverage Summary',
                title_style,
            )
        )

        intro_style = ParagraphStyle(
            name="RawDvCovIntro",
            fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
            fontSize=RAW_DV_PDF_BODY_FONT_SIZE - 1,
            leading=RAW_DV_PDF_BODY_FONT_SIZE + 1,
            textColor=_RL_COLORS.black,
            spaceAfter=8,
        )
        flowables.append(
            Paragraph(
                "For each *_source_value column, this table shows how many of "
                "its distinct raw values appear as a concept_code in the OMOP "
                "CONCEPT vocabulary. Higher coverage indicates the column is a "
                "structured code; lower coverage suggests free-text or local codes.",
                intro_style,
            )
        )

        headers = [
            "File",
            "Column",
            "Distinct",
            "Found",
            "Not Found",
            "Coverage %",
            "Top 20 Unmapped",
        ]
        col_widths = [
            1.6 * _RL_INCH,
            1.2 * _RL_INCH,
            0.7 * _RL_INCH,
            0.6 * _RL_INCH,
            0.7 * _RL_INCH,
            0.7 * _RL_INCH,
            2.0 * _RL_INCH,
        ]
        # Scale to fit
        sum_widths = sum(col_widths)
        if sum_widths > usable_width:
            scale = usable_width / sum_widths
            col_widths = [w * scale for w in col_widths]

        body_rows = []
        for fr, c in coverage_rows:
            distinct = c["distinct_count"]
            unmapped = c["coverage_unmapped_count"] or 0
            found = max(distinct - unmapped, 0)
            top_unmapped_str = "; ".join(
                str(v) for v in c.get("coverage_top_unmapped", [])
            )
            body_rows.append(
                [
                    cls._raw_dv_pdf_wrap_cell(
                        os.path.basename(fr["file_path"]),
                        font_name=RAW_DV_PDF_BODY_FONT,
                        font_size=RAW_DV_PDF_NARROW_TABLE_FONT_SIZE,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        c["column"],
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=RAW_DV_PDF_NARROW_TABLE_FONT_SIZE,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        f"{distinct:,}",
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=RAW_DV_PDF_NARROW_TABLE_FONT_SIZE,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        f"{found:,}",
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=RAW_DV_PDF_NARROW_TABLE_FONT_SIZE,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        f"{unmapped:,}",
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=RAW_DV_PDF_NARROW_TABLE_FONT_SIZE,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        f"{c['coverage_pct']}",
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=RAW_DV_PDF_NARROW_TABLE_FONT_SIZE,
                    ),
                    cls._raw_dv_pdf_wrap_cell(
                        top_unmapped_str,
                        font_name=RAW_DV_PDF_MONO_FONT,
                        font_size=RAW_DV_PDF_NARROW_TABLE_FONT_SIZE,
                    ),
                ]
            )

        flowables.append(
            cls._raw_dv_pdf_build_table(
                headers, body_rows, col_widths, RAW_DV_PDF_NARROW_TABLE_FONT_SIZE
            )
        )
        return flowables


    @classmethod
    def generate_raw_distinct_values_report(
        cls,
        partner: str,
        folder_name: str,
        file_results: list,
    ) -> str:
        """Build a single PDF report from a list of per-file get_raw_distinct_values results.

        The report is black-and-white only and contains:
          - Page 1: cover with title, partner, folder, generated timestamp,
            files-profiled count, and a clickable file-level Table of Contents.
          - One section per source file, starting on a new page, containing a
            file title (clickable destination from the TOC), a mini-TOC of
            column-name links, and per-column data tables (top RAW_DV_TOP_N rows,
            with concept_name/code enrichment for *_concept_id columns).
          - A final source-value coverage summary section if any *_source_value
            columns were profiled.
          - Every page (except page 1) carries a footer "Return to TOC" link
            and a "Page X of Y" page number.

        Args:
            partner: Partner identifier (lowercased internally).
            folder_name: Data delivery folder (used in output path and filename).
            file_results: List of dicts from get_raw_distinct_values().

        Returns:
            Absolute path to the generated PDF file, or an empty string on failure.
        """
        # Guard against missing reportlab in the running container.
        if not _REPORTLAB_AVAILABLE:
            msg = (
                "reportlab is not installed in the container. "
                "Add `reportlab` to ovid_setup/ovid_image/requirements.txt and "
                "rebuild the OVID Docker image."
            )
            _get_ovid_logger().error(f"[raw_dv] {msg}")
            cls.print_failure_message(
                folder=folder_name,
                partner=partner,
                job="raw_distinct_values report",
                text=msg,
            )
            return ""

        try:
            from reportlab.platypus import (
                BaseDocTemplate,
                Paragraph,
                Spacer,
                PageTemplate,
                Frame,
                PageBreak,
            )
            from reportlab.lib.styles import ParagraphStyle

            folder_label = (
                folder_name.split("/")[-1] if "/" in folder_name else folder_name
            )
            output_root = (
                f"/app/partners/{partner.lower()}/data/{folder_name}/raw_distinct_values/"
            )
            os.makedirs(output_root, exist_ok=True)
            pdf_path = os.path.join(
                output_root,
                f"{partner.lower()}_{folder_label}_raw_distinct_values_report.pdf",
            )

            # Assign each result a stable index used in bookmark names.
            # Results are keyed by their position in `file_results`, which is
            # robust whether the caller produced one entry per file (legacy
            # / `-t all`) or one entry per logical group (current default).
            # Skip empty results (no profiled columns).
            file_idx_by_pos: dict = {}
            next_idx = 0
            for pos, fr in enumerate(file_results):
                if not fr.get("columns"):
                    continue
                file_idx_by_pos[pos] = next_idx
                next_idx += 1

            def _result_label(fr_dict: dict) -> str:
                """Return the human-readable label for a result entry.

                Prefers the new `group_name` field, then `file_stem`
                (back-compat alias), then the basename of the first source
                file. Multi-file groups get a ``(N files)`` suffix so the
                TOC distinguishes single-file from grouped results.
                """
                base = (
                    fr_dict.get("group_name")
                    or fr_dict.get("file_stem")
                    or os.path.basename(fr_dict.get("file_path", ""))
                    or "(unknown)"
                )
                sources = fr_dict.get("source_files") or []
                if len(sources) > 1:
                    return f"{base} ({len(sources)} files)"
                return base

            page_w, page_h = RAW_DV_PDF_PAGE_SIZE
            margin = RAW_DV_PDF_MARGIN
            usable_width = page_w - 2 * margin
            usable_height = page_h - 2 * margin

            # Document and frame setup
            doc = BaseDocTemplate(
                pdf_path,
                pagesize=RAW_DV_PDF_PAGE_SIZE,
                leftMargin=margin,
                rightMargin=margin,
                topMargin=margin,
                bottomMargin=margin,
                title=f"OVID Raw Distinct Values - {partner} - {folder_label}",
                author="OVID",
            )
            frame = Frame(
                margin,
                margin,
                usable_width,
                usable_height,
                leftPadding=0,
                rightPadding=0,
                topPadding=0,
                bottomPadding=0,
                id="content",
            )

            # Per-page footer + return-to-TOC link.
            # We deliberately use a single-pass build (no canvasmaker) because
            # reportlab's two-pass NumberedCanvas pattern corrupts named
            # destination references in the second pass (every <a href="#dest">
            # ends up pointing at the same final page). The footer therefore
            # shows "Page X" only, not "Page X of Y" -- the simpler form is
            # acceptable and lets clickable navigation work reliably.
            def _on_page_callback(canvas, doc):
                """Per-page setup that runs during the (single) build pass.

                Places the 'toc' named destination on page 1, and on every
                other page draws a footer with the page number, a clickable
                "Return to Table of Contents" link, and an underline beneath
                the link text.
                """
                page_num = canvas.getPageNumber()
                if page_num == 1:
                    canvas.bookmarkPage("toc")
                    # Footer on page 1: just the page number.
                    canvas.setFont(RAW_DV_PDF_BODY_FONT, RAW_DV_PDF_BODY_FONT_SIZE - 1)
                    canvas.setFillColor(_RL_COLORS.black)
                    canvas.drawRightString(
                        page_w - margin,
                        margin / 2,
                        f"Page {page_num}",
                    )
                    return

                # Pages 2..N: page number on the right, return link on the left.
                canvas.setFont(RAW_DV_PDF_BODY_FONT, RAW_DV_PDF_BODY_FONT_SIZE - 1)
                canvas.setFillColor(_RL_COLORS.black)
                canvas.drawRightString(
                    page_w - margin,
                    margin / 2,
                    f"Page {page_num}",
                )

                link_text = "Return to Table of Contents"
                canvas.setFont(
                    RAW_DV_PDF_BODY_FONT_BOLD,
                    RAW_DV_PDF_BODY_FONT_SIZE - 1,
                )
                text_x = margin
                text_y = margin / 2
                canvas.drawString(text_x, text_y, link_text)
                # Visible underline (still grayscale)
                text_w = canvas.stringWidth(
                    link_text,
                    RAW_DV_PDF_BODY_FONT_BOLD,
                    RAW_DV_PDF_BODY_FONT_SIZE - 1,
                )
                canvas.line(
                    text_x,
                    text_y - 1,
                    text_x + text_w,
                    text_y - 1,
                )
                # Pad the click area slightly so it's easy to hit.
                rect = (
                    text_x - 2,
                    text_y - 4,
                    text_x + text_w + 2,
                    text_y + RAW_DV_PDF_BODY_FONT_SIZE + 2,
                )
                canvas.linkRect(
                    "Return to TOC",
                    "toc",
                    rect,
                    relative=0,
                    thickness=0,
                )

            page_template = PageTemplate(
                id="default",
                frames=[frame],
                onPage=_on_page_callback,
            )
            doc.addPageTemplates([page_template])

            # ---------- Build the flowables ----------
            story: list = []

            # Cover / TOC
            title_style = ParagraphStyle(
                name="RawDvTitle",
                fontName=RAW_DV_PDF_BODY_FONT_BOLD,
                fontSize=RAW_DV_PDF_TITLE_FONT_SIZE,
                leading=RAW_DV_PDF_TITLE_FONT_SIZE + 4,
                textColor=_RL_COLORS.black,
                spaceAfter=10,
            )
            story.append(Paragraph("OVID Raw Distinct Values Report", title_style))

            meta_style = ParagraphStyle(
                name="RawDvCoverMeta",
                fontName=RAW_DV_PDF_BODY_FONT,
                fontSize=RAW_DV_PDF_BODY_FONT_SIZE,
                leading=RAW_DV_PDF_BODY_FONT_SIZE + 2,
                textColor=_RL_COLORS.black,
            )
            story.append(Paragraph(f"<b>Partner:</b> {partner}", meta_style))
            story.append(Paragraph(f"<b>Folder:</b> {folder_name}", meta_style))
            story.append(
                Paragraph(
                    f"<b>Generated:</b> "
                    f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
                    meta_style,
                )
            )
            story.append(
                Paragraph(
                    f"<b>Sections profiled:</b> {len(file_idx_by_pos)}",
                    meta_style,
                )
            )
            story.append(Spacer(1, 0.25 * _RL_INCH))

            # File-level TOC table
            toc_section_style = ParagraphStyle(
                name="RawDvTocHeader",
                fontName=RAW_DV_PDF_BODY_FONT_BOLD,
                fontSize=RAW_DV_PDF_FILE_TITLE_FONT_SIZE,
                leading=RAW_DV_PDF_FILE_TITLE_FONT_SIZE + 3,
                textColor=_RL_COLORS.black,
                spaceAfter=6,
            )
            story.append(Paragraph("Table of Contents", toc_section_style))

            from reportlab.platypus import Table, TableStyle

            toc_headers = [
                cls._raw_dv_pdf_wrap_cell(
                    "#",
                    font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                    font_size=RAW_DV_PDF_TABLE_HEADER_FONT_SIZE,
                ),
                cls._raw_dv_pdf_wrap_cell(
                    "Section (click to jump)",
                    font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                    font_size=RAW_DV_PDF_TABLE_HEADER_FONT_SIZE,
                ),
                cls._raw_dv_pdf_wrap_cell(
                    "Distinct",
                    font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                    font_size=RAW_DV_PDF_TABLE_HEADER_FONT_SIZE,
                ),
                cls._raw_dv_pdf_wrap_cell(
                    "Of File",
                    font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                    font_size=RAW_DV_PDF_TABLE_HEADER_FONT_SIZE,
                ),
            ]
            toc_rows = [toc_headers]
            file_row_indices: list = []  # indices of "file" rows (for shading)

            for pos, fr in enumerate(file_results):
                if pos not in file_idx_by_pos:
                    continue
                f_idx = file_idx_by_pos[pos]
                file_label = _result_label(fr)

                # File row (bold, light-grey background)
                file_row_indices.append(len(toc_rows))
                toc_rows.append(
                    [
                        cls._raw_dv_pdf_wrap_cell(
                            f"{f_idx + 1}",
                            font_name=RAW_DV_PDF_MONO_FONT_BOLD,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                        ),
                        cls._raw_dv_pdf_make_link_paragraph(
                            file_label,
                            f"file_{f_idx}",
                            font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                        ),
                        cls._raw_dv_pdf_wrap_cell(
                            f"<b>{len(fr.get('columns', []))} cols</b>",
                            font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                            raw_markup=True,
                        ),
                        cls._raw_dv_pdf_wrap_cell(
                            f"<b>{fr['total_rows']:,} rows</b>",
                            font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                            raw_markup=True,
                        ),
                    ]
                )

                # Per-column rows (sorted by priority).
                sorted_cols = [
                    c for _, _, c in sorted(
                        [
                            (cls._raw_dv_column_priority(c["column"]), idx, c)
                            for idx, c in enumerate(fr.get("columns", []) or [])
                        ],
                        key=lambda t: (t[0], t[1]),
                    )
                ]
                for col_idx, col_info in enumerate(sorted_cols):
                    # Only emit a vocab-mix TOC link if the breakdown will
                    # actually render. The breakdown requires vocabulary_id in
                    # the per-column CSV; when CONCEPT enrichment is skipped or
                    # fails, the column is absent and the breakdown helper
                    # returns []. Linking to a non-existent destination causes
                    # reportlab to fail PDF finalization with
                    # "format not resolved".
                    has_vocab = (
                        cls._raw_dv_column_priority(col_info["column"]) in (1, 2)
                        and cls._raw_dv_column_has_vocab_columns(
                            col_info.get("csv_path", "")
                        )
                    )

                    # Vocab-mix row (only for *_concept_id and *_source_concept_id)
                    if has_vocab:
                        toc_rows.append(
                            [
                                cls._raw_dv_pdf_wrap_cell(
                                    "",  # blank under file's #
                                    font_name=RAW_DV_PDF_MONO_FONT,
                                    font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                                ),
                                cls._raw_dv_pdf_make_link_paragraph(
                                    "      "  # indent under file row
                                    + cls._raw_dv_pdf_link_label(
                                        col_info["column"], "vocab"
                                    ),
                                    f"file_{f_idx}_col_{col_idx}_vocab",
                                    font_name=RAW_DV_PDF_BODY_FONT,
                                    font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                                ),
                                cls._raw_dv_pdf_wrap_cell(
                                    "",  # vocab table doesn't have a single distinct count
                                    font_name=RAW_DV_PDF_MONO_FONT,
                                    font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                                ),
                                cls._raw_dv_pdf_wrap_cell(
                                    "",
                                    font_name=RAW_DV_PDF_MONO_FONT,
                                    font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                                ),
                            ]
                        )

                    # Distinct-values block row
                    toc_rows.append(
                        [
                            cls._raw_dv_pdf_wrap_cell(
                                "",
                                font_name=RAW_DV_PDF_MONO_FONT,
                                font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                            ),
                            cls._raw_dv_pdf_make_link_paragraph(
                                "      "
                                + cls._raw_dv_pdf_link_label(
                                    col_info["column"], "block"
                                ),
                                f"file_{f_idx}_col_{col_idx}",
                                font_name=RAW_DV_PDF_BODY_FONT,
                                font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                            ),
                            cls._raw_dv_pdf_wrap_cell(
                                f"{col_info['distinct_count']:,}",
                                font_name=RAW_DV_PDF_MONO_FONT,
                                font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                            ),
                            cls._raw_dv_pdf_wrap_cell(
                                f"null {col_info['null_pct']}%",
                                font_name=RAW_DV_PDF_MONO_FONT,
                                font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                            ),
                        ]
                    )

            # If we have any *_source_value coverage rows, append a TOC row for the coverage section.
            coverage_rows = [
                (fr, c)
                for fr in file_results
                for c in fr.get("columns", [])
                if c["is_source_value"] and c["coverage_pct"] is not None
            ]
            if coverage_rows:
                file_row_indices.append(len(toc_rows))
                toc_rows.append(
                    [
                        cls._raw_dv_pdf_wrap_cell(
                            "*",
                            font_name=RAW_DV_PDF_MONO_FONT_BOLD,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                        ),
                        cls._raw_dv_pdf_make_link_paragraph(
                            "Source-Value Coverage Summary",
                            "source_value_coverage",
                            font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                        ),
                        cls._raw_dv_pdf_wrap_cell(
                            f"<b>{len(coverage_rows)} cols</b>",
                            font_name=RAW_DV_PDF_BODY_FONT_BOLD,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                            raw_markup=True,
                        ),
                        cls._raw_dv_pdf_wrap_cell(
                            "",
                            font_name=RAW_DV_PDF_MONO_FONT,
                            font_size=RAW_DV_PDF_TABLE_BODY_FONT_SIZE,
                        ),
                    ]
                )

            toc_col_widths = [
                0.4 * _RL_INCH,
                4.7 * _RL_INCH,
                1.0 * _RL_INCH,
                1.4 * _RL_INCH,
            ]
            sum_widths = sum(toc_col_widths)
            if sum_widths > usable_width:
                scale = usable_width / sum_widths
                toc_col_widths = [w * scale for w in toc_col_widths]

            # Use LongTable instead of Table so it page-breaks if the TOC overflows.
            from reportlab.platypus import LongTable as _RL_LongTable
            toc_table = _RL_LongTable(toc_rows, colWidths=toc_col_widths, repeatRows=1)
            toc_style_cmds = [
                # Header row
                ("BACKGROUND", (0, 0), (-1, 0), RAW_DV_PDF_HEADER_BG),
                ("FONTNAME", (0, 0), (-1, 0), RAW_DV_PDF_BODY_FONT_BOLD),
                ("FONTSIZE", (0, 0), (-1, 0), RAW_DV_PDF_TABLE_HEADER_FONT_SIZE),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), RAW_DV_PDF_GRID_WIDTH, RAW_DV_PDF_GRID_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
            # File rows get the same light-grey background as the header to make
            # them visually distinct from the indented column rows below them.
            for fr_idx in file_row_indices:
                toc_style_cmds.append(
                    ("BACKGROUND", (0, fr_idx), (-1, fr_idx), RAW_DV_PDF_HEADER_BG)
                )
            toc_table.setStyle(TableStyle(toc_style_cmds))
            story.append(toc_table)

            # Per-section flowables (one section per result entry).
            for pos, fr in enumerate(file_results):
                if pos not in file_idx_by_pos:
                    continue
                f_idx = file_idx_by_pos[pos]
                try:
                    story.extend(
                        cls._raw_dv_pdf_build_file_section(fr, f_idx, usable_width)
                    )
                except Exception as file_exc:
                    _get_ovid_logger().error(
                        f"[raw_dv] failed to build PDF section for "
                        f"{_result_label(fr)}: {file_exc}",
                        exc_info=True,
                    )
                    err_style = ParagraphStyle(
                        name="RawDvFileError",
                        fontName=RAW_DV_PDF_BODY_FONT_ITALIC,
                        fontSize=RAW_DV_PDF_BODY_FONT_SIZE,
                        leading=RAW_DV_PDF_BODY_FONT_SIZE + 2,
                        textColor=_RL_COLORS.black,
                    )
                    story.append(PageBreak())
                    story.append(
                        Paragraph(
                            f"(error building section for "
                            f"{_result_label(fr)}: {file_exc})",
                            err_style,
                        )
                    )

            # Coverage section
            if coverage_rows:
                story.extend(
                    cls._raw_dv_pdf_build_coverage_section(coverage_rows, usable_width)
                )

            # Build with two-pass NumberedCanvas for "Page X of Y" footers.
            # Single-pass build; the page footer (page number + return link)
            # is drawn by the onPage callback on the PageTemplate.
            doc.build(story)

            _get_ovid_logger().info(f"[raw_dv] PDF report written to {pdf_path}")
            return pdf_path

        except Exception as exc:
            _get_ovid_logger().error(
                f"[raw_dv] generate_raw_distinct_values_report failed: {exc}",
                exc_info=True,
            )
            cls.print_failure_message(
                folder=folder_name,
                partner=partner,
                job="raw_distinct_values report",
                text=str(exc),
            )
            return ""


###################################################################################################################################
# This function will output the mapping gap for the passed formatted file name
###################################################################################################################################

   

    @classmethod
    def get_mothly_counts(cls,partner,file_name,table_name, file_path,folder_name):

        

        complete_file_path = file_path+'/formatted_'+file_name.lower()+'.csv'
        
        master_url = os.environ["SPARK_URL"].strip().replace('"', '')
        spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()
        # spark = SparkSession.builder.master("spark://spark-master:7077").appName("MONTHLY COUNT").getOrCreate()

        monthly_counts_out_file_path = file_path.replace('formatter','monthly_count')+table_name+'/'


        df = cls.spark_read(complete_file_path, spark)

        for col_name in df.columns:

            if 'DATE' in col_name:

                monthly_counts = df.groupBy(date_format(col_name, "yyyy-MM").alias("year_month")) \
                   .agg(count("*").alias("count")) \
                   .orderBy("year_month",  ascending=False)
                
                output_file_name = col_name.lower()+'_mothly_count.csv'

                cls.write_pyspark_output_file(monthly_counts,output_file_name, monthly_counts_out_file_path )

          


###################################################################################################################################
# This function will diagnos a given dictionary data frame
########################################################################################################

    @classmethod
    def diagnos_dictionary(cls,dictionary_dataframe, file_path, file_name, table_name):

        # getting values that mapped to null or some flavor of null like 'OT, 'NI', and "UN
        filtered_df = dictionary_dataframe.filter((col("dict_value").isin("NI", "OT", "UN")) | (col("dict_value").isNull()) | (col("dict_value") == ""))

        out_file_path = file_path.replace('formatter','mapping_gap')+"/"+table_name+"/mapping_to_flavor_of_null/"
        output_file_name = file_name+"_flavor_of_null.csv"


        cls.write_pyspark_output_file(filtered_df, output_file_name, out_file_path )
           


###################################################################################################################################
# This function will diagnos a given joind dictionary dataframe
########################################################################################################

    @classmethod
    def diagnos_mapping(cls,joined_df, file_path, file_name, field_name, dictionary_dataframe, table_name):


        try:
            # output all mapping
            all_values_df = joined_df.select(joined_df[field_name],joined_df['dict_value'],joined_df['count'],joined_df['percent'])
            all_mapping_out_file_path = file_path.replace('formatter','mapping_gap')+"/"+table_name+"/all_mapping/"
            all_mapping_output_file_name = file_name+"_all_mapping.csv"

            cls.write_pyspark_output_file(all_values_df, all_mapping_output_file_name, all_mapping_out_file_path )


            
            # output missing mapping
            cdm_value_set = PcornetCDM.get_cdm_valueset_df(field_name)
            cdm_value_set = [x.strip() for x in cdm_value_set] # remove extra spaces from the values list

            missing_values_df = all_values_df.filter(col("dict_value").isNull() )
            
            
            missing_values_df_and_not_part_of_cdm = missing_values_df.filter(~col(field_name).isin(cdm_value_set))
            
            missing_mapping_out_file_path = file_path.replace('formatter','mapping_gap')+"/"+table_name+"/missing_mapping/"
            missing_mapping_output_file_name = file_name+"_missing_mapping.csv"
            cls.write_pyspark_output_file(missing_values_df_and_not_part_of_cdm, missing_mapping_output_file_name, missing_mapping_out_file_path )

            # output suggested mapping

            
            to_be_added_field = missing_values_df_and_not_part_of_cdm.select(

                    missing_values_df_and_not_part_of_cdm[field_name].alias('dict_key'),
                    lit('').alias('dict_value'),


            )

            unioned_df= dictionary_dataframe.union(to_be_added_field)
            suggested_mapping_out_file_path = file_path.replace('formatter','mapping_gap')+"/"+table_name+"/suggested_mapping/"

            out_file_name = suggested_mapping_out_file_path+"suggested_"+file_name+".py"

            result = os.makedirs(os.path.dirname(out_file_name), exist_ok=True)

            formatted_text = unioned_df.rdd \
                .map(lambda row: f'       {cls.process_key(row["dict_key"],row["dict_value"],file_name, 20)}  :  "{row["dict_value"]}",') 
            

            formatted_text= formatted_text.collect()
            

            # Join the formatted strings with newline characters
            formatted_text = "\n".join(formatted_text)

            # Write the formatted text to a text file
        
            first_line = "\n\n"+file_name+ " = {\n\n"
            last_line = "   \n\n}"

            with open(out_file_name, "w+") as file:
                file.write(first_line)
                file.write(formatted_text)
                file.write(last_line)


        except Exception as e:

            

            cls.print_with_style(str(e), 'danger red')
            
            
    
###################################################################################################################################
# 
########################################################################################################

    @classmethod
    def process_row(cls,row, file):


            _get_ovid_logger().debug(f"Processing row: {row}")


            key = cls.append_spaces(row.dict_key, 20),

            text = row.dict_value + '\n'

            file.write(text)


                
               

###################################################################################################################################
# 
########################################################################################################

    @classmethod
    def process_key(cls, key, value,file_name, size):

            key = str(key)

            if 'unit' not in file_name and 'UNIT' not in file_name:

                key = key.upper()

            key = '"'+key+'"'


            if value == "" or value == None:

                key = '# '+key

            while len(key) < size:

                key = key + ' '

            return key





###################################################################################################################################
# This function return and aggreated dataframe for a given table
###################################################################################################################################



    @classmethod
    def get_aggregate_value_from_table(cls, df, row_count, field_name):
        aggregated_df = df.groupBy(field_name).agg(count("*").alias("count"))

        if row_count != 0:
            percent_col = round((col("count") / row_count) * 100, 2).alias("percent")
        else:
            percent_col = lit(0).alias("percent")

        return aggregated_df.select(col(field_name), col("count"), percent_col)





###################################################################################################################################
# This function will
###################################################################################################################################

    @classmethod
    def db_upload(cls,db, db_server,file_name,table_name, file_path,folder_name,db_type,upload_type,merge_db,merge_schema,  partner_name):

        if db_type.upper() == 'PG':
            cls.db_upload_postgress(db, db_server,file_name,table_name, file_path,folder_name)

        elif db_type.upper() == 'MSSQL':
            cls.db_upload_mssql(db, db_server,file_name,table_name, file_path,folder_name)

        elif db_type.upper() == 'SF':
            cls.db_upload_snowflake(db, db_server,file_name,table_name, file_path,folder_name,upload_type,merge_db,merge_schema,partner_name)

        else:
            raise ValueError(f"Unrecognized database type: '{db_type}'. Valid types are: PG, MSSQL, SF")




###################################################################################################################################
# This function will
###################################################################################################################################

    @classmethod
    def db_upload_cdm_complementary_tables(cls,db, db_server,folder_name,db_type, mappers_type, upload_type, merge_db, merge_schema):

        if db_type.upper() == 'PG':
            cls.db_upload_postgress(db, db_server,schemas[0],file_name,table_name, file_path,folder_name)

        if db_type.upper() == 'MSSQL':
            cls.db_upload_mssql(db, db_server,schemas[0],file_name,table_name, file_path,folder_name)

        if db_type.upper() == 'SF':
            cls.db_upload_cdm_complementary_tables_snowflake(db, db_server,folder_name, mappers_type, upload_type, merge_db, merge_schema)



###################################################################################################################################
# This function will upload a pyspark df to mssql table in a specified server/dabase
###################################################################################################################################

    @classmethod
    def _get_harmonized_id_field_widths(cls, spark, db, schema_name, sf_options):
        """Compute per-schema harmonized widths for shared undefined-length ID fields.

        Queries ``INFORMATION_SCHEMA.COLUMNS`` for the maximum declared character
        length of each harmonized ID field (the keys of
        ``harmonized_filed_sizes_dict``) across all tables already present in the
        target schema. Empty complementary tables are then created with these
        widths so that every table declares an identical length for PATID and its
        siblings (PCORnet DC 1.15a field-length harmonization).

        Args:
            spark: Active SparkSession.
            db (str): Snowflake database name.
            schema_name (str): Target schema holding the partner's uploaded tables.
            sf_options (dict): Base Snowflake connector options (reused; the query
                is issued against ``INFORMATION_SCHEMA``).

        Returns:
            dict[str, int]: Map of ID field name to the max declared width found
            among the existing tables. Empty when the lookup fails or no tables
            exist yet (callers then fall back to ``harmonized_filed_sizes_dict``).
        """
        id_field_names = list(harmonized_filed_sizes_dict.keys())
        if not id_field_names:
            return {}

        in_list = ", ".join(f"'{name}'" for name in id_field_names)
        widths_query = f"""
        SELECT COLUMN_NAME, MAX(CHARACTER_MAXIMUM_LENGTH) AS MAX_LEN
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND COLUMN_NAME IN ({in_list})
          AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL
        GROUP BY COLUMN_NAME
        """

        overrides: dict = {}
        try:
            options = dict(sf_options)
            options["sfSchema"] = "INFORMATION_SCHEMA"
            widths_df = spark.read \
                .format("net.snowflake.spark.snowflake") \
                .options(**options) \
                .option("query", widths_query) \
                .load()
            for row in widths_df.collect():
                max_len = row["MAX_LEN"]
                if max_len is not None:
                    overrides[row["COLUMN_NAME"]] = int(max_len)
        except Exception as e:
            # Non-fatal: fall back to the global harmonized floor.
            cls.print_with_style(
                f"_get_harmonized_id_field_widths: width lookup failed for "
                f"{db}.{schema_name}; using harmonized defaults ({e})",
                'warning orange'
            )
            return {}

        return overrides

    @classmethod


    def db_upload_cdm_complementary_tables_snowflake(cls, db, db_server, folder_name, mappers_type, upload_type, merge_db, merge_schema):

        if '/' in folder_name:
            folder_name = folder_name.split('/')[1]

        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        if upload_type == 'merge':
            schema_name = f"OVID_{folder_name.upper()}_merged_with_{merge_db}_{merge_schema}"
        else:
            schema_name = f"OVID_{folder_name.upper()}"



        spark = cls.get_spark_session("Upload")


        pkb = cls.get_pkb()

        sf_options = {
            "sfURL": f"{db_server}.snowflakecomputing.com",
            "sfDatabase": db,
            "sfSchema": "INFORMATION_SCHEMA",
            "sfStage": SF_STAGE_NAME,
            "sfRole": SF_ROLE,
            "sfUser": SF_DB_USER,
            "pem_private_key": pkb  # <== path to your .p8 private key
        }
        
        

        get_tables_names_query = f"""
        SELECT TABLE_NAME
        FROM TABLES
        WHERE TABLE_SCHEMA = '{schema_name}'
        """




        existing_tables_df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sf_options) \
            .option("query", get_tables_names_query) \
            .load()


        tables_specs_path = f'/app/mapping_scripts/{mappers_type}/tables_specs.csv'

        cdm_df = spark.read.load(tables_specs_path,format="csv", sep=",", inferSchema="false", header="true", quote= '"')

        cdm_tables_names_df =  cdm_df.select("TABLE_NAME").distinct()


        missing_tables_names_df = cdm_tables_names_df.join(existing_tables_df, existing_tables_df["TABLE_NAME"] == cdm_tables_names_df["TABLE_NAME"], "left_anti")



        # Collect the table names into a Python list
        missing_table_names = [row["TABLE_NAME"] for row in missing_tables_names_df.collect()]

        # DC 1.15a: empty complementary tables must declare the same width for
        # shared undefined-length ID fields (PATID, PATID_1, PATID_2,
        # ENCOUNTERID, PROVIDERID, ...) as the partner's already-uploaded
        # populated tables. Otherwise an empty stub falls back to the global
        # harmonized floor (e.g. PATID=64) while populated tables carry the
        # partner's real width (e.g. 128), tripping the field-length
        # harmonization check. We derive a per-run, per-schema override map from
        # the max declared column length across the existing tables.
        field_size_overrides = cls._get_harmonized_id_field_widths(
            spark, db, schema_name, sf_options
        )

        # Iterate over the names
        for table in missing_table_names:
            
            schema = cls.get_snowflake_schema(table, spark, mappers_type, field_size_overrides=field_size_overrides)
            empty_df = spark.createDataFrame([], schema)

            sf_options = {
                "sfURL": f"{db_server}.snowflakecomputing.com",
                "sfDatabase": db,
                "sfSchema": schema_name,
                "sfStage": SF_STAGE_NAME,
                "sfRole": SF_ROLE,
                "sfUser": SF_DB_USER,
                "pem_private_key": pkb  # <== path to your .p8 private key
            }

            # Write DataFrame to Snowflake with retry logic for transient errors
            cls._snowflake_write_with_retry_simple(
                df=empty_df,
                sf_options=sf_options,
                table_name=table,
                schema=schema,
                schema_name=schema_name,
                mode="ignore"
            )
            

                    

###################################################################################################################################
# This function will upload a pyspark df to mssql table in a specified server/dabase
###################################################################################################################################

    @classmethod
    def merge_dfs(cls, merge_from_df,merge_into_df,table_name ):

        table_primary_key_array =  PCORNET_PRIMARY_KEYS.get(table_name)

        join_condition = [
                    merge_into_df[k] == merge_from_df[k]
                    for k in table_primary_key_array
                ]

        missing_rows = (
            merge_from_df.alias("from")
            .join(
                merge_into_df.alias("into"),
                on=join_condition,
                how="left_anti"
                )
            )
        

        merged_df = merge_into_df.unionByName(missing_rows)


        return merged_df



###################################################################################################################################
# Zip-transition-aware merge for Ovid-built address-history tables
###################################################################################################################################

    @classmethod
    def merge_address_history_with_zip_transitions(
        cls,
        incoming_df: "pyspark.sql.DataFrame",
        existing_df: "pyspark.sql.DataFrame",
        enforce_single_current: bool = False,
    ) -> "pyspark.sql.DataFrame":
        """Reconcile an Ovid-built address-history table against its prior state.

        Used for ``LDS_ADDRESS_HISTORY`` and ``PRIVATE_ADDRESS_HISTORY`` of
        partners where Ovid synthesizes the address history (e.g. from the
        demographic ZIP) rather than receiving it from the partner:

        - at merge-upload time for partners flagged
          ``OVID_BUILDS_LDS_ADDRESS_HISTORY = True`` (emy, tmh), and
        - at fix time via ``append_to_existing_lds_address_hx_fix`` v2.0 for
          full-refresh (``no_merge``) partners that still need the address
          history carried across cycles (nch).

        Because the rows are rebuilt every cycle, a plain primary-key anti-join
        merge (see :meth:`merge_dfs`) cannot express address moves and loses
        patients that drop out of the current build. Rules, per ``PATID``
        (ZIP comparisons are NULL-safe on ``ADDRESS_ZIP5``; "current" means
        ``CURRENT_ADDRESS_FLAG`` is not ``'N'``):

        1. **Continuation** - an incoming row whose (PATID, ZIP5) matches an
           existing current row is dropped; the existing row (and its
           ``ADDRESSID``) is kept and its period is widened: the earlier
           non-null ``ADDRESS_PERIOD_START`` wins and a non-null
           ``ADDRESS_PERIOD_END`` is extended to the later end date (an open,
           NULL end stays open).
        2. **Move** - an existing current row for a PATID that appears in the
           incoming data with no incoming row of the same ZIP is closed:
           ``ADDRESS_PERIOD_END`` is set to the patient's latest incoming
           ``ADDRESS_PERIOD_START`` (never earlier than its own start, never
           later than an existing end) and ``CURRENT_ADDRESS_FLAG`` = ``'N'``.
           The incoming new-ZIP row is kept as a new row.
        3. **ADDRESSID collision** - when a kept incoming row reuses an
           existing ``ADDRESSID`` (e.g. ADDRESSID derived from a stable
           ENCOUNTERID while the ZIP changed), the incoming row keeps the
           ``ADDRESSID`` (it is the one the current geocode references) and the
           existing row is re-keyed to a deterministic
           ``md5('closed|ADDRESSID|ZIP5|START')`` id so the primary key stays
           unique.
        4. Existing rows for patients absent from the incoming data, and
           existing rows already closed (``'N'``), are carried through
           unchanged; incoming rows for new patients are added.
        5. Optional (``enforce_single_current=True``): after reconciling, at
           most one row per PATID keeps ``CURRENT_ADDRESS_FLAG='Y'`` - the row
           with the latest start (then open/latest end, incoming before
           existing, highest ADDRESSID); the others are flagged ``'N'``.

        The result is the full reconciled table, intended to *replace* the
        table (not to be anti-join-unioned again). Date columns keep the data
        type of ``incoming_df`` (``DateType`` at upload time, ``yyyy-MM-dd``
        strings at fix time).

        Args:
            incoming_df: The freshly built Ovid address-history DataFrame for
                this batch. Must contain at least ``ADDRESSID``, ``PATID``,
                ``ADDRESS_ZIP5``, ``ADDRESS_PERIOD_START`` and
                ``ADDRESS_PERIOD_END``.
            existing_df: The prior address-history table, with the same column
                set (and date-column types) as ``incoming_df``.
            enforce_single_current: When ``True``, keep only one current
                (``'Y'``) row per PATID (rule 5). Only applied when the table
                has a ``CURRENT_ADDRESS_FLAG`` column.

        Returns:
            A DataFrame with the same columns (in the same order) as
            ``incoming_df`` containing the reconciled rows.

        Raises:
            ValueError: If the two DataFrames do not share an identical column
                set (the caller is expected to align columns beforehand).
        """
        output_columns = incoming_df.columns
        if set(output_columns) != set(existing_df.columns):
            raise ValueError(
                "merge_address_history_with_zip_transitions requires incoming and "
                "existing DataFrames to share the same columns; "
                f"incoming-only={set(output_columns) - set(existing_df.columns)}, "
                f"existing-only={set(existing_df.columns) - set(output_columns)}"
            )

        has_current_flag = "CURRENT_ADDRESS_FLAG" in output_columns
        start_type = incoming_df.schema["ADDRESS_PERIOD_START"].dataType
        end_type = incoming_df.schema["ADDRESS_PERIOD_END"].dataType
        null_zip_key = "\u0000"

        def with_zip_key(data_frame: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
            """Add a NULL-safe ZIP5 join key column ``_zip_key``."""
            return data_frame.withColumn(
                "_zip_key", coalesce(col("ADDRESS_ZIP5").cast("string"), lit(null_zip_key))
            )

        def as_date(column_name: str) -> Column:
            """Parse a date column that may be DateType or a yyyy-MM-dd string."""
            return to_date(col(column_name).cast("string"), "yyyy-MM-dd")

        existing_keyed = with_zip_key(existing_df.select(*output_columns))
        incoming_keyed = with_zip_key(incoming_df.select(*output_columns))

        if has_current_flag:
            existing_is_current = ~F.upper(
                coalesce(col("CURRENT_ADDRESS_FLAG"), lit(""))
            ).eqNullSafe(lit("N"))
        else:
            existing_is_current = lit(True)

        existing_keyed = existing_keyed.withColumn("_is_current", existing_is_current)

        # Per (PATID, ZIP) span of the incoming rows; its presence marks an
        # existing row at the same ZIP as "continued" (rule 1).
        incoming_zip_spans = incoming_keyed.groupBy("PATID", "_zip_key").agg(
            F.min(as_date("ADDRESS_PERIOD_START")).alias("_in_min_start"),
            F.max(as_date("ADDRESS_PERIOD_END")).alias("_in_max_end"),
            lit(1).alias("_zip_seen"),
        )

        # Per PATID latest incoming start; its presence marks the patient as
        # present in the incoming data, and its value closes moved rows (rule 2).
        incoming_patid_latest = incoming_keyed.groupBy("PATID").agg(
            F.max(as_date("ADDRESS_PERIOD_START")).alias("_in_latest_start"),
            lit(1).alias("_patid_seen"),
        )

        current_existing_keys = (
            existing_keyed.filter(col("_is_current"))
            .select("PATID", "_zip_key")
            .distinct()
        )

        # Rule 1: drop incoming rows that continue an existing current address.
        surviving_incoming = incoming_keyed.join(
            current_existing_keys, on=["PATID", "_zip_key"], how="left_anti"
        )

        existing_annotated = (
            existing_keyed
            .join(incoming_zip_spans, on=["PATID", "_zip_key"], how="left")
            .join(incoming_patid_latest, on="PATID", how="left")
        )

        is_continued = col("_is_current") & col("_zip_seen").isNotNull()
        is_moved = (
            col("_is_current")
            & col("_zip_seen").isNull()
            & col("_patid_seen").isNotNull()
        )

        existing_start = as_date("ADDRESS_PERIOD_START")
        existing_end = as_date("ADDRESS_PERIOD_END")

        # F.least / F.greatest skip NULLs, so a missing existing start is
        # filled from the incoming span.
        continued_start = F.least(existing_start, col("_in_min_start"))
        # A NULL (open) end on a current row stays open.
        continued_end = when(
            existing_end.isNull(), existing_end
        ).otherwise(F.greatest(existing_end, col("_in_max_end")))
        moved_end = F.greatest(
            existing_start, F.least(existing_end, col("_in_latest_start"))
        )

        def date_out(date_column: Column, target_type: "pyspark.sql.types.DataType") -> Column:
            """Render a computed DateType column back to the input column type."""
            if isinstance(target_type, DateType):
                return date_column
            return date_format(date_column, "yyyy-MM-dd").cast(target_type)

        new_start = when(is_continued, date_out(continued_start, start_type)).otherwise(
            col("ADDRESS_PERIOD_START")
        )
        new_end = (
            when(is_continued, date_out(continued_end, end_type))
            .when(is_moved, date_out(moved_end, end_type))
            .otherwise(col("ADDRESS_PERIOD_END"))
        )

        replacements: dict[str, Column] = {
            "ADDRESS_PERIOD_START": new_start,
            "ADDRESS_PERIOD_END": new_end,
        }
        if has_current_flag:
            replacements["CURRENT_ADDRESS_FLAG"] = when(is_moved, lit("N")).otherwise(
                col("CURRENT_ADDRESS_FLAG")
            )

        reconciled_existing = existing_annotated.select(
            *[
                replacements.get(column_name, col(column_name)).alias(column_name)
                for column_name in output_columns
            ]
        )

        # Rule 3: re-key existing rows whose ADDRESSID is reused by a kept
        # incoming row, so the primary key stays unique.
        colliding_ids = (
            surviving_incoming.select("ADDRESSID").distinct().withColumn("_id_collides", lit(1))
        )
        reconciled_existing = reconciled_existing.join(colliding_ids, on="ADDRESSID", how="left")
        rekeyed_id = F.md5(
            F.concat_ws(
                "|",
                lit("closed"),
                col("ADDRESSID").cast("string"),
                coalesce(col("ADDRESS_ZIP5").cast("string"), lit("")),
                coalesce(col("ADDRESS_PERIOD_START").cast("string"), lit("")),
            )
        )
        reconciled_existing = reconciled_existing.withColumn(
            "ADDRESSID",
            when(col("_id_collides").isNotNull(), rekeyed_id).otherwise(col("ADDRESSID")),
        )

        final_df = (
            reconciled_existing.select(*output_columns).withColumn("_from_incoming", lit(0))
            .unionByName(
                surviving_incoming.select(*output_columns).withColumn("_from_incoming", lit(1))
            )
        )

        # Rule 5: at most one current row per patient.
        if enforce_single_current and has_current_flag:
            is_flag_y = F.upper(coalesce(col("CURRENT_ADDRESS_FLAG"), lit(""))) == lit("Y")
            current_rank_window = Window.partitionBy("PATID", "_is_y").orderBy(
                as_date("ADDRESS_PERIOD_START").desc_nulls_last(),
                as_date("ADDRESS_PERIOD_END").desc_nulls_first(),
                col("_from_incoming").desc(),
                col("ADDRESSID").desc_nulls_last(),
            )
            final_df = (
                final_df
                .withColumn("_is_y", is_flag_y)
                .withColumn("_current_rank", F.row_number().over(current_rank_window))
                .withColumn(
                    "CURRENT_ADDRESS_FLAG",
                    when(col("_is_y") & (col("_current_rank") > 1), lit("N")).otherwise(
                        col("CURRENT_ADDRESS_FLAG")
                    ),
                )
            )

        return final_df.select(*output_columns)



###################################################################################################################################
# This function will upload a pyspark df to mssql table in a specified server/dabase
###################################################################################################################################

    @classmethod

    def get_sf_schema(cls, df, table_name, mappers_type=None):






        """
        Generate Snowflake schema from a Spark DataFrame based on PCORnet CDM.

        Args:
            df (DataFrame): Spark DataFrame containing the table data.
            table_name (str): Name of the table to get CDM info.
            mappers_type (Optional[str]): The partner's ``mappers_type`` (from
                ``partner_settings.py``). When provided and a matching secondary
                CDM exists (e.g. ``nursing_mappers``), its field definitions
                supplement the PCORnet CDM so that non-PCORnet tables (such as
                the nursing tables) get correct types — notably ``RDBMS Date``
                fields become Snowflake ``DATE`` instead of ``VARCHAR``. PCORnet
                definitions always take precedence on field-name collisions.

        Returns:
            List[StructField]: Snowflake schema fields.
        """
        schema_sf = []
        cdm_fields = []

        try:
            spark = cls.get_spark_session("Get Schema")

            # Get CDM fields for the table
            cdm_df = PcornetCDM.get_cdm_df(spark)
            table_cdm_df = cdm_df.filter(cdm_df["TABLE_NAME"] == table_name)
            table_cdm = table_cdm_df.collect()

            # Build CDM field lookup for data types
            cdm_lookup = {row["FIELD_NAME"]: row["RDBMS_DATA_TYPE"] for row in table_cdm}

            # Supplement with a secondary (non-PCORnet) CDM when the partner's
            # mapper type provides one. This is how nursing tables — which are
            # absent from pcornet_cdm_v70.csv — get their date fields typed as
            # DATE. PCORnet definitions win on any field-name collision, so
            # existing PCORnet-partner behavior is unchanged.
            secondary_cdm_df = PcornetCDM.get_secondary_cdm_df(spark, mappers_type)
            if secondary_cdm_df is not None:
                secondary_rows = secondary_cdm_df.filter(
                    secondary_cdm_df["TABLE_NAME"] == table_name
                ).collect()
                for row in secondary_rows:
                    cdm_lookup.setdefault(row["FIELD_NAME"], row["RDBMS_DATA_TYPE"])


            # Single-pass max-length computation across every column that needs a
            # measured width. Previously each column triggered its own full-table
            # scan (df.select(max(length(col))).collect()) inside
            # get_max_feild_size_from_df / get_df_field_config, so a 30-column
            # table cost ~30 full CSV re-reads just to size VARCHARs. Computing
            # them all in one .select([...]).collect() turns that into a single
            # scan (mirrors the raw-upload path optimization).
            max_lengths = cls._compute_max_field_sizes(df, df.columns)

            # Build schema in the exact order of DataFrame columns
            for field in df.columns:
                if field in cdm_lookup:
                    # CDM field - use CDM data type
                    schema_sf.append(
                        cls.get_df_field_config(df, field, cdm_lookup[field], max_lengths=max_lengths)
                    )
                elif field.upper() == 'UPDATED':
                    schema_sf.append(StructField("UPDATED", StringType(), metadata={"maxlength": 19}))
                elif field.upper() == 'SOURCE':
                    schema_sf.append(StructField("SOURCE", StringType(), metadata={"maxlength": 20}))
                else:
                    # Extra non-CDM field
                    field_size = int(cls.get_max_feild_size_from_df(df, field, max_lengths=max_lengths))
                    schema_sf.append(StructField(field, StringType(), metadata={"maxlength": field_size}))


            # spark.stop()

        except Exception as e:
            # spark.stop()
            cls.print_failure_message(
                folder='',
                partner='',
                job='',
                text=str(e)
            )

        return schema_sf






    @classmethod
    def _snowflake_write_with_retry(cls, df, sf_options, table_name, schema_type, db_schema_name, max_retries=3, base_delay=30):
        """
        Internal helper to write DataFrame to Snowflake with retry logic.
        Handles transient network failures like DNS resolution errors.
        """
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        retries = 0
        last_exception = None
        
        while retries <= max_retries:
            try:
                df.repartition(10).write \
                    .format(SNOWFLAKE_SOURCE_NAME) \
                    .options(**sf_options) \
                    .option("on_error", "CONTINUE") \
                    .option("dbtable", table_name.upper()) \
                    .option("createTableColumnTypes", schema_type) \
                    .option("useSchema", db_schema_name) \
                    .mode("overwrite") \
                    .save()
                return  # Success - exit the function
            except Exception as e:
                last_exception = e
                error_str = str(e).lower()
                # Only retry on transient network errors
                if any(err in error_str for err in ['unknownhostexception', 'sockettimeoutexception', 
                                                      'connectionreset', 'connection reset', 
                                                      'networkunreachable', 'temporaryerror',
                                                      'no response or invalid response']):
                    retries += 1
                    if retries > max_retries:
                        _get_ovid_logger().warning(f"[RETRY] All {max_retries} retry attempts failed for Snowflake upload of {table_name}")
                        raise
                    delay = min(base_delay * (2 ** (retries - 1)), 300)
                    _get_ovid_logger().warning(f"[RETRY] Attempt {retries}/{max_retries} failed with transient error: {str(e)[:200]}")
                    _get_ovid_logger().warning(f"[RETRY] Retrying Snowflake upload for {table_name} in {delay} seconds...")
                    time.sleep(delay)
                else:
                    # Non-transient error - don't retry
                    raise
        raise last_exception

    @classmethod
    def _snowflake_write_with_retry_simple(cls, df, sf_options, table_name, schema, schema_name, mode="ignore", max_retries=3, base_delay=30):
        """
        Internal helper to write DataFrame to Snowflake with retry logic (simple version).
        Used for creating empty tables with ignore mode.
        """
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        retries = 0
        last_exception = None
        
        while retries <= max_retries:
            try:
                df.write \
                    .format(SNOWFLAKE_SOURCE_NAME) \
                    .options(**sf_options) \
                    .option("dbtable", table_name.upper()) \
                    .option("createTableColumnTypes", schema) \
                    .option("useSchema", schema_name) \
                    .mode(mode) \
                    .save()
                return  # Success - exit the function
            except Exception as e:
                last_exception = e
                error_str = str(e).lower()
                # Only retry on transient network errors
                if any(err in error_str for err in ['unknownhostexception', 'sockettimeoutexception', 
                                                      'connectionreset', 'connection reset', 
                                                      'networkunreachable', 'temporaryerror',
                                                      'no response or invalid response']):
                    retries += 1
                    if retries > max_retries:
                        _get_ovid_logger().warning(f"[RETRY] All {max_retries} retry attempts failed for Snowflake upload of {table_name}")
                        raise
                    delay = min(base_delay * (2 ** (retries - 1)), 300)
                    _get_ovid_logger().warning(f"[RETRY] Attempt {retries}/{max_retries} failed with transient error: {str(e)[:200]}")
                    _get_ovid_logger().warning(f"[RETRY] Retrying Snowflake upload for {table_name} in {delay} seconds...")
                    time.sleep(delay)
                else:
                    # Non-transient error - don't retry
                    raise
        raise last_exception

    @classmethod



# ###################################################################################################################################
# # This function will upload a pyspark df to snowflake table in a specified server/dabase
# ###################################################################################################################################

#     def db_upload_snowflake(cls, db, db_server, file_name, table_name, file_path, folder_name, upload_type,merge_db,merge_schema):

#         if '/' in folder_name:
#             folder_name = folder_name.split('/')[1]

#         SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        
#         try:
            
#             spark = cls.get_spark_session("Upload")
#             spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

#             # custom_schema = StructType(schema)

#             # file_df = spark.read.load(file_path+file_name+'*', format="csv", sep="\t", header="true", quote='"')

#             file_df = cls.spark_read(file_path+file_name+'*', spark)






#             pkb = cls.get_pkb()

            
#             if upload_type == 'merge' and table_name.upper()!= 'HARVEST' :

#                 db_schema_name = f"OVID_{folder_name}_merged_with_{merge_db}_{merge_schema}"

#                 sf_options = {
#                 "sfURL": f"{db_server}.snowflakecomputing.com",
#                 "sfDatabase": db,
#                 "sfSchema": db_schema_name,
#                 "sfStage": SF_STAGE_NAME,
#                 "sfRole": SF_ROLE,
#                 "sfUser": SF_DB_USER,
#                 "pem_private_key": pkb  # <== path to your .p8 private key
#             }


#                 merge_df = (
#                         spark.read
#                             .format(SNOWFLAKE_SOURCE_NAME)
#                             .options(**sf_options)
#                             .option("dbtable", f"{merge_db}.{merge_schema}.{table_name}")
#                             .load()
#                     )



#                 # merge_df.show()

#                 file_cols = file_df.columns

#                 merge_df = merge_df.select(
#                     *[col(c) for c in file_cols if c in merge_df.columns]
#                 )


#                 # merge_df.show()


#                 merged_df = cls.merge_dfs(
                    
#                     merge_from_df = merge_df,
#                     merge_into_df = file_df,
#                     table_name    = table_name )


#                 schema_type = cls.get_sf_schema(merged_df, table_name)
#                 custom_schema = StructType(schema_type)

#                 conn = cls.get_sf_connection(db_server)
#                 cur = conn.cursor()
#                 pkb = cls.get_pkb()

#                 cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{db_schema_name}")


#                 for field in schema_type:
#                     if isinstance(field.dataType, DateType) and field.name in merged_df.columns:
#                         merged_df = merged_df.withColumn(field.name, to_date(col(field.name), "yyyy-MM-dd"))

#                     if isinstance(field.dataType, DoubleType) and field.name in merged_df.columns:
#                         merged_df = merged_df.withColumn(field.name, col(field.name).cast(DoubleType()))

#                 merged_df_with_schema = spark.createDataFrame(merged_df.rdd, custom_schema)

#                 # Write DataFrame to Snowflake with retry logic
#                 cls._snowflake_write_with_retry(
#                     df=merged_df_with_schema,
#                     sf_options=sf_options,
#                     table_name=table_name,
#                     schema_type=schema_type,
#                     db_schema_name=db_schema_name
#                 )



#             else:


#                 schema_type = cls.get_sf_schema(file_df,table_name)




#                 # # print(schema_sf)
#                 # sys.exit(schema_type)

#                 custom_schema = StructType(schema_type)


#                 db_schema_name = f'OVID_{folder_name}'
                

#                 conn = cls.get_sf_connection(db_server)
#                 cur = conn.cursor()
#                 pkb = cls.get_pkb()
#                 cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{db_schema_name}")

#                 for field in schema_type:
#                     if isinstance(field.dataType, DateType) and field.name in file_df.columns:
#                         file_df = file_df.withColumn(field.name, to_date(col(field.name), "yyyy-MM-dd"))

#                     if isinstance(field.dataType, DoubleType) and field.name in file_df.columns:
#                         file_df = file_df.withColumn(field.name, col(field.name).cast(DoubleType()))

#                 file_df_with_schema = spark.createDataFrame(file_df.rdd, custom_schema)

#                 sf_options = {
#                 "sfURL": f"{db_server}.snowflakecomputing.com",
#                 "sfDatabase": db,
#                 "sfSchema": db_schema_name,
#                 "sfStage": SF_STAGE_NAME,
#                 "sfRole": SF_ROLE,
#                 "sfUser": SF_DB_USER,
#                 "pem_private_key": pkb  # <== path to your .p8 private key
#             }

#                 # Write DataFrame to Snowflake with retry logic
#                 cls._snowflake_write_with_retry(
#                     df=file_df_with_schema,
#                     sf_options=sf_options,
#                     table_name=table_name,
#                     schema_type=schema_type,
#                     db_schema_name=db_schema_name
#                 )
                
#             spark.stop()
#             cur.close()
#             conn.close()
#         except Exception as e:

#             spark.stop()            
#             cls.print_with_style(str(e), 'danger red')




    def db_upload_snowflake(cls, db, db_server, file_name, table_name, file_path, folder_name, upload_type,merge_db,merge_schema, partner_name):

        # print('1')
        partner_settings_path = "partners."+partner_name+".partner_settings"
        partner_settings = importlib.import_module(partner_settings_path)

        # Used to load a secondary CDM (e.g. nursing) so non-PCORnet tables get
        # correct Snowflake types (dates as DATE). None for pure PCORnet partners.
        mappers_type = getattr(partner_settings, 'mappers_type', None)

        if '/' in folder_name:
            folder_name = folder_name.split('/')[1]

        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        
        try:
            
            # print('2')

            spark = cls.get_spark_session("Upload")
            spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

            # custom_schema = StructType(schema)

            file_df = spark.read.load(file_path+file_name+'*', format="csv", sep="\t", header="true", quote='"')

            # Cache the parsed CSV so the single schema-inference scan
            # (get_sf_schema -> _compute_max_field_sizes) is reused by the
            # subsequent Snowflake write instead of re-reading + re-parsing the
            # file from disk a second time. Unpersisted in the finally block.
            file_df = file_df.cache()

            pkb = cls.get_pkb()

            if upload_type == 'merge':
                
                # upload OVID data to snowflake
                schema_type = cls.get_sf_schema(file_df,table_name,mappers_type)
                custom_schema = StructType(schema_type)

                db_schema_name = f"OVID_{folder_name}_merged_with_{merge_db}_{merge_schema}"
                
                conn = cls.get_sf_connection(db_server)
                cur = conn.cursor()

                try:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{db_schema_name}")
                
                finally:
                    cur.close()
                    conn.close()

                for field in schema_type:
                    if isinstance(field.dataType, DateType) and field.name in file_df.columns:
                        file_df = file_df.withColumn(field.name, to_date(col(field.name), "yyyy-MM-dd"))

                    if isinstance(field.dataType, DoubleType) and field.name in file_df.columns:
                        file_df = file_df.withColumn(field.name, col(field.name).cast(DoubleType()))

                file_df_with_schema = spark.createDataFrame(file_df.rdd, custom_schema)

                # build createTableColumnTypes as a proper string
                #column_type_mappings = {
                #    DateType: "DATE",
                #    DoubleType: "DOUBLE",
                #    StringType: "VARCHAR",
                #    TimestampType: "TIMESTAMP",
                #}
                
                #column_type_parts = []
                
                #for field in schema_type:
                #    sf_type = column_type_mappings.get(type(field.dataType), "VARCHAR")
                #    column_type_parts.append(f"{field.name} {sf_type}")
                
                #create_table_column_types_str = ", ".join(column_type_parts)

                sf_options = {
                    "sfURL": f"{db_server}.snowflakecomputing.com",
                    "sfDatabase": db,
                    "sfSchema": db_schema_name,
                    "sfStage": SF_STAGE_NAME,
                    "sfRole": SF_ROLE,
                    "sfUser": SF_DB_USER,
                    "pem_private_key": pkb  # <== path to your .p8 private key
                }

                cls.global_schema_name = db_schema_name

                cls._snowflake_write_with_retry(
                    df=file_df_with_schema,
                    sf_options=sf_options,
                    table_name=table_name,
                    schema_type=schema_type,
                    db_schema_name=db_schema_name
                )
                _get_ovid_logger().debug("Snowflake write with retry completed (step 3)")

                # skip merge logic for HARVEST — it should only contain the new OVID data
                if table_name.upper() != 'HARVEST':

                    # check if the table exists in the merge-from schema before attempting to load it
                    conn_check = cls.get_sf_connection(db_server)
                    cur_check = conn_check.cursor()
                    try:
                        cur_check.execute(f"""
                            SELECT COUNT(*) FROM {db}.INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = '{partner_settings.MERGE_SCHEMA_NAME}'
                            AND TABLE_NAME = '{table_name.upper()}'
                        """)
                        table_exists = cur_check.fetchone()[0] > 0
                    finally:
                        cur_check.close()
                        conn_check.close()

                    if not table_exists:
                        spark.stop()
                        message = f"{table_name} does not exist in {partner_settings.MERGE_SCHEMA_NAME} schema. Skipping merge for this table."
                        cls.print_failure_message(
                            folder=folder_name,
                            partner=partner_name,
                            job=f"merge {table_name.lower()}",
                            text=message
                        )
                        return

                    # load PUBLIC data
                    merge_schema_df = (
                                        spark.read
                                            .format(SNOWFLAKE_SOURCE_NAME)
                                            .options(**sf_options)
                                            .option("dbtable", f"{db}.{partner_settings.MERGE_SCHEMA_NAME}.{table_name}")
                                            .load()
                                        )
                    
                    # find common columns in both OVID and PUBLIC
                    common_columns = [c for c in file_df_with_schema.columns if c in merge_schema_df.columns]

                    # filter to string columns only
                    string_common_columns = [
                        c for c in common_columns
                        if isinstance(file_df_with_schema.schema[c].dataType, StringType)
                    ]
                    # compute max lengths for OVID string columns (single pass)
                    if string_common_columns:
                        max_lengths = file_df_with_schema.select(
                            [spark_max(length(col(c))).alias(c) for c in string_common_columns]
                        ).collect()[0]

                        ovid_lengths = {c: (max_lengths[c] or 0) for c in string_common_columns}
                    
                    else:
                        ovid_lengths = {}

                    # column lengths in PUBLIC
                    conn = cls.get_sf_connection(db_server)
                    cur = conn.cursor()
                    # print('4')

                    try:
                        query_public = f"""
                            SELECT
                                COLUMN_NAME,
                                DATA_TYPE,
                                COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) AS LENGTH
                            FROM {db}.INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = '{partner_settings.MERGE_SCHEMA_NAME}'
                            AND TABLE_NAME = '{table_name.upper()}'
                            AND DATA_TYPE IN ('TEXT', 'VARCHAR')
                        """
                        
                        # print(query_public)  
                        cur.execute(query_public)
                        public_lengths = {row[0]: row[2] for row in cur.fetchall()}
                    
                    finally:
                        cur.close()
                        conn.close()

                    # compare and identify columns that need resizing
                    columns_to_resize = [
                        (c, ovid_lengths[c], public_lengths[c])
                        for c in string_common_columns
                        if c in ovid_lengths
                        and c in public_lengths
                        and public_lengths[c] is not None
                        and ovid_lengths[c] < public_lengths[c]
                    ]
                    # print('5')
         
                    # print comparison summary
                    # print(f"Found {len(columns_to_resize)} columns to resize:")
                    # for col_name, ovid_len, public_len in columns_to_resize:
                    #     print(f"  {col_name}: OVID={ovid_len} -> PUBLIC={public_len}")
                        
                    # alter column lengths in snowflake
                    if columns_to_resize:
                        conn = cls.get_sf_connection(db_server)
                        cur = conn.cursor()
                        
                        try:
                            for col_name, ovid_len, public_len in columns_to_resize:
                                
                                query = f"""
                                    ALTER TABLE {db}.{db_schema_name}.{table_name.upper()} 
                                    ALTER COLUMN {col_name} SET DATA TYPE VARCHAR({public_len})
                                """
                                
                                # print(f"Executing: {query}")
                                cur.execute(query)
                            
                            # print(f"Successfully resized {len(columns_to_resize)} columns")
                        
                        finally:
                            cur.close()
                            conn.close()
                    
                    # else:
                    #     print("No column needs resizing")

                    # find columns in OVID but not in PUBLIC
                    columns_in_ovid_only = list(set(file_df_with_schema.columns) - set(merge_schema_df.columns))
                    # print('6')

                    # merge if all OVID columns are in PUBLIC
                    if len(columns_in_ovid_only) == 0:

                        # Zip-transition-aware reconciliation for Ovid-built
                        # address-history tables (partner submits incrementally
                        # and Ovid builds the LDS/PRIVATE address history). For
                        # these partners a plain PK-anti-join carry-forward
                        # cannot express address moves, so instead of the
                        # INSERT ... WHERE NOT EXISTS path we reconcile in Spark
                        # (close the old ADDRESS_PERIOD_END on a ZIP change, keep
                        # the incoming new-ZIP row) and overwrite the merged
                        # schema table with the result.
                        ovid_builds_address_history = getattr(
                            partner_settings, 'OVID_BUILDS_LDS_ADDRESS_HISTORY', False
                        )
                        zip_transition_tables = ('LDS_ADDRESS_HISTORY', 'PRIVATE_ADDRESS_HISTORY')

                        if ovid_builds_address_history and table_name.upper() in zip_transition_tables:

                            _get_ovid_logger().info(
                                "Zip-transition merge for %s (partner=%s, folder=%s): "
                                "reconciling incoming vs existing %s.%s",
                                table_name.upper(), partner_name, folder_name,
                                partner_settings.MERGE_SCHEMA_NAME, table_name.upper(),
                            )

                            # Align the existing (PUBLIC) frame to the OVID column
                            # order before reconciling; merge_schema_df already
                            # contains every OVID column (guaranteed by the
                            # columns_in_ovid_only == 0 check above).
                            existing_aligned = merge_schema_df.select(*file_df_with_schema.columns)

                            reconciled_df = cls.merge_address_history_with_zip_transitions(
                                incoming_df=file_df_with_schema,
                                existing_df=existing_aligned,
                            )

                            cls._snowflake_write_with_retry(
                                df=reconciled_df,
                                sf_options=sf_options,
                                table_name=table_name,
                                schema_type=schema_type,
                                db_schema_name=db_schema_name,
                            )
                            _get_ovid_logger().info(
                                "Zip-transition merge completed for %s (partner=%s)",
                                table_name.upper(), partner_name,
                            )

                        else:

                            # build a column list in OVID order
                            ovid_columns = file_df_with_schema.columns
                            columns_list = ", ".join(ovid_columns)

                            # build the concat statement for primary keys
                            primary_keys = PCORNET_PRIMARY_KEYS.get(table_name)
                            primary_keys_join = " AND ".join([f"public.{pk} = ovid.{pk}" for pk in primary_keys])

                            # insert PUBLIC into OVID by selecting columns in OVID order
                            conn = cls.get_sf_connection(db_server)
                            cur = conn.cursor()

                            try:
                                query = f"""
                                    INSERT INTO {db}.{db_schema_name}.{table_name.upper()} ({columns_list})
                                    SELECT {columns_list} FROM {merge_db}.{merge_schema}.{table_name.upper()} public
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM {db}.{db_schema_name}.{table_name.upper()} ovid
                                        WHERE {primary_keys_join}
                                    )
                                """

                                # print(query)
                                cur.execute(query)

                            finally:
                                cur.close()
                                conn.close()

                    # stop if not all columns in OVID exist in PUBLIC
                    else:

                        spark.stop()
                        message = ", ".join(columns_in_ovid_only) + " from OVID do not exist in PUBLIC"
                        cls.print_with_style(message, 'warning orange')


            else:
                # print('7')

                schema_type = cls.get_sf_schema(file_df,table_name,mappers_type)
                custom_schema = StructType(schema_type)


                db_schema_name = f'OVID_{folder_name}'

                cls.global_schema_name = db_schema_name

                conn = cls.get_sf_connection(db_server)
                cur = conn.cursor()
                pkb = cls.get_pkb()
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{db_schema_name}")

                for field in schema_type:
                    if isinstance(field.dataType, DateType) and field.name in file_df.columns:
                        file_df = file_df.withColumn(field.name, to_date(col(field.name), "yyyy-MM-dd"))

                    if isinstance(field.dataType, DoubleType) and field.name in file_df.columns:
                        file_df = file_df.withColumn(field.name, col(field.name).cast(DoubleType()))

                file_df_with_schema = spark.createDataFrame(file_df.rdd, custom_schema)

                sf_options = {
                "sfURL": f"{db_server}.snowflakecomputing.com",
                "sfDatabase": db,
                "sfSchema": db_schema_name,
                "sfStage": SF_STAGE_NAME,
                "sfRole": SF_ROLE,
                "sfUser": SF_DB_USER,
                "pem_private_key": pkb  # <== path to your .p8 private key
            }

                # Write DataFrame to Snowflake
                cls._snowflake_write_with_retry(
                    df=file_df_with_schema,
                    sf_options=sf_options,
                    table_name=table_name,
                    schema_type=schema_type,
                    db_schema_name=db_schema_name
                )
                
            spark.stop()
            cur.close()
            conn.close()
        except Exception as e:

            spark.stop()            
            cls.print_with_style(str(e), 'danger red')
            raise



###################################################################################################################################
# Post-merge replication reconciliation (PCORnet DC 1.10)
###################################################################################################################################

    @classmethod
    def reconcile_replication_post_merge(
        cls,
        db: str,
        db_server: str,
        merged_schema_name: str,
    ) -> None:
        """Re-synchronise ENC_TYPE and ADMIT_DATE in DIAGNOSIS/PROCEDURES after a merge.

        The merge path (``db_upload_snowflake`` with ``upload_type == 'merge'``)
        carries forward old PUBLIC rows whose primary key is absent from the new
        batch via an ``INSERT ... WHERE NOT EXISTS``. Each table is merged on its
        own primary key independently, so a corrected ENCOUNTER row (matched on
        ENCOUNTERID) can win while old DIAGNOSIS/PROCEDURES rows (matched on
        DIAGNOSISID/PROCEDURESID) carry forward untouched, still holding the stale
        ENC_TYPE/ADMIT_DATE that were replicated at a previous fix stage. That
        mismatch reproduces PCORnet Data Check 1.10 (replication errors between
        ENCOUNTER, DIAGNOSIS and PROCEDURES) in the merged data.

        This method runs once, after every table for a batch has been merged, and
        overwrites ENC_TYPE and ADMIT_DATE in the merged DIAGNOSIS and PROCEDURES
        tables with the authoritative values from the merged ENCOUNTER table
        (matched on ENCOUNTERID). It is a no-op when the merged schema does not
        contain the ENCOUNTER table or a given child table.

        Args:
            db: The Snowflake database that holds the merged schema.
            db_server: The Snowflake account/server used by ``get_sf_connection``.
            merged_schema_name: The merged schema name produced by
                ``db_upload_snowflake`` (``OVID_<folder>_merged_with_<db>_<schema>``).

        Returns:
            None. Progress and per-table row counts are logged via ovid_logging;
            errors are logged and re-raised.
        """
        logger = _get_ovid_logger()

        conn = cls.get_sf_connection(db_server)
        try:
            cur = conn.cursor()
            try:
                # Confirm the authoritative ENCOUNTER table is present in the
                # merged schema before attempting any child reconciliation. The
                # database identifier is internal config (interpolated); the
                # schema/table names are bound parameters.
                table_exists_query = (
                    f"SELECT COUNT(*) FROM {db}.INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
                )
                cur.execute(
                    table_exists_query,
                    (merged_schema_name.upper(), "ENCOUNTER"),
                )
                if cur.fetchone()[0] == 0:
                    logger.info(
                        "reconcile_replication_post_merge: no ENCOUNTER table in "
                        "%s.%s - skipping replication reconciliation",
                        db, merged_schema_name,
                    )
                    return

                for child_table in ("DIAGNOSIS", "PROCEDURES"):

                    cur.execute(
                        table_exists_query,
                        (merged_schema_name.upper(), child_table),
                    )
                    if cur.fetchone()[0] == 0:
                        logger.info(
                            "reconcile_replication_post_merge: %s not present in "
                            "%s.%s - skipping",
                            child_table, db, merged_schema_name,
                        )
                        continue

                    # NULL-safe comparison so rows are only rewritten when they
                    # actually diverge from the merged ENCOUNTER row. Identifiers
                    # (db/schema/table) are internal config, not user input.
                    update_query = f"""
                        UPDATE {db}.{merged_schema_name}.{child_table} AS tgt
                        SET ENC_TYPE = enc.ENC_TYPE,
                            ADMIT_DATE = enc.ADMIT_DATE
                        FROM {db}.{merged_schema_name}.ENCOUNTER AS enc
                        WHERE tgt.ENCOUNTERID = enc.ENCOUNTERID
                          AND (
                                NOT EQUAL_NULL(tgt.ENC_TYPE, enc.ENC_TYPE)
                             OR NOT EQUAL_NULL(tgt.ADMIT_DATE, enc.ADMIT_DATE)
                          )
                    """
                    cur.execute(update_query)
                    rows_updated = cur.rowcount if cur.rowcount is not None else -1
                    logger.info(
                        "reconcile_replication_post_merge: reconciled %s rows in "
                        "%s.%s.%s against merged ENCOUNTER",
                        rows_updated, db, merged_schema_name, child_table,
                    )

            finally:
                cur.close()
        except Exception as exc:
            logger.error(
                "reconcile_replication_post_merge failed for %s.%s: %s",
                db, merged_schema_name, exc, exc_info=True,
            )
            raise
        finally:
            conn.close()



###################################################################################################################################
# Raw input file upload: dispatch by db_type
###################################################################################################################################

    @classmethod
    def db_upload_raw(cls, db: str, db_server: str, file_path: str, table_name: str,
                      folder_name: str, db_type: str, partner_name: str) -> None:
        """Upload a raw input file (pre-format) to the configured database.

        Routes to the correct database backend based on ``db_type``. Currently
        only Snowflake is supported; PostgreSQL and Microsoft SQL Server raise
        ``NotImplementedError`` as their ``db_upload_postgress`` /
        ``db_upload_mssql`` counterparts are stubs.

        Args:
            db: Target database name (e.g. ``PARTNER_RAW_PROD``).
            db_server: Snowflake account locator (e.g.
                ``your-account.region``).
            file_path: Absolute path to the raw input file inside the
                Spark master container (e.g. ``/data/PARTNER/20260319/measurement_20260319.csv``).
            table_name: Snowflake table name. Verbatim from the input
                filename, uppercased, extension stripped (e.g.
                ``SS_MEASUREMENT_20260319``).
            folder_name: Folder argument as supplied by the user
                (e.g. ``EMY/20260319``). Only the basename is used to
                construct the Snowflake schema name.
            db_type: One of ``SF``, ``PG``, ``MSSQL`` (case-insensitive).
            partner_name: Partner short-name (e.g. ``emy``). Currently
                informational only, kept on the signature so future
                partner-aware behavior (e.g. RAW_{partner}_{folder}
                schema naming) can be wired in without changing callers.

        Raises:
            ValueError: If ``db_type`` is not recognized.
            NotImplementedError: If ``db_type`` is ``PG`` or ``MSSQL``.
        """
        if db_type.upper() == 'SF':
            cls.db_upload_raw_snowflake(db, db_server, file_path, table_name,
                                        folder_name, partner_name)
        elif db_type.upper() in ('PG', 'MSSQL'):
            raise NotImplementedError(
                f"db_upload_raw: db_type '{db_type}' is not yet supported. "
                f"Only Snowflake (SF) raw uploads are implemented."
            )
        else:
            raise ValueError(
                f"Unrecognized database type: '{db_type}'. Valid types are: SF"
            )


###################################################################################################################################
# Raw input file upload to Snowflake
###################################################################################################################################

    @classmethod
    def db_upload_raw_snowflake(cls, db: str, db_server: str, file_path: str,
                                table_name: str, folder_name: str,
                                partner_name: str) -> None:
        """Upload a raw input file to Snowflake under ``{db}.RAW_{folder}.{table}``.

        Mirrors the high-level shape of :meth:`db_upload_snowflake` but
        skips every CDM-aware step: there is no ``pcornet_cdm_v70.csv``
        lookup, no ``harmonized_filed_sizes_dict`` reconciliation, no
        merge logic, and no CDM complementary tables. All columns are
        uploaded as ``VARCHAR(N)`` where ``N`` is the per-column maximum
        observed character length, computed in a single distributed pass
        over the file.

        Args:
            db: Target Snowflake database name.
            db_server: Snowflake account locator.
            file_path: Absolute path to the raw input file inside the
                Spark master container.
            table_name: Final Snowflake table name (uppercase, no extension).
            folder_name: Folder argument as supplied by the user (e.g.
                ``EMY/20260319``). Only the basename (``20260319``) is
                used to build the schema name.
            partner_name: Partner short-name (informational; preserved on
                the signature for symmetry with :meth:`db_upload_snowflake`).

        Raises:
            Exception: Re-raises any exception encountered during the
                Spark read, schema computation, ``CREATE SCHEMA``, or
                Snowflake write so the caller can surface it to the user
                via ``print_failure_message``.
        """
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

        # Schema name: RAW_{folder_basename}, uppercased.
        # Folder may be passed as 'EMY/20260319' or just '20260319'.
        folder_base = folder_name.split('/')[-1] if '/' in folder_name else folder_name
        db_schema_name = f"RAW_{folder_base.upper()}"

        spark = None
        conn = None
        cur = None
        try:
            spark = cls.get_spark_session("Raw Upload")
            spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

            # Sniff delimiter (comma vs tab) using the raw_distinct_values helper.
            delim = cls._raw_dv_sniff_delimiter(file_path)

            # Read raw file as all-strings; no inferred schema.
            #
            # ignoreLeadingWhiteSpace=True is defense-in-depth against
            # malformed upstream headers that emit a literal whitespace
            # character after every separator (e.g. `"externalmedid",
            # "patid", "ext_record_date", ...`). Without this option Spark
            # treats the leading space as data and preserves the surrounding
            # quotes as part of the column name, yielding columns like
            # `"patid"` (with embedded `"`) that the Snowflake JDBC writer
            # then emits unquoted in the CREATE TABLE DDL as
            # `""patid"" VARCHAR(N), ...` -- which Snowflake parses as a
            # bare `""` token followed by `VARCHAR(...)`, raising
            # `syntax error ... unexpected '""'` and `unexpected 'VARCHAR'`.
            # Observed on the EMY 20260610 external_meds delivery; matches
            # the equivalent fix already applied to the EMY external_meds
            # CDM formatter on 2026-06-11. Clean deliveries are unaffected
            # by the option (it is a no-op when no leading whitespace is
            # present after the separator).
            file_df = spark.read.csv(
                file_path,
                sep=delim,
                header=True,
                quote='"',
                inferSchema=False,
                multiLine=False,
                escape='"',
                ignoreLeadingWhiteSpace=True,
            )

            columns = file_df.columns
            if not columns:
                raise ValueError(
                    f"db_upload_raw_snowflake: file {file_path} produced an "
                    f"empty DataFrame (no columns). Check delimiter/header."
                )

            # Single-pass max-length computation across all columns.
            # Mirrors the merge-upload optimization at the existing CDM
            # upload path (commonFunctions.py:4936-4938).
            max_lengths_row = file_df.select(
                [spark_max(length(col(c))).alias(c) for c in columns]
            ).collect()[0]

            schema_type = []
            for c in columns:
                raw_len = max_lengths_row[c]
                # Empty / all-null columns: clamp to 1 so Snowflake accepts
                # the VARCHAR width. Same behaviour as
                # get_max_feild_size_from_df.
                field_size = max(int(raw_len) if raw_len is not None else 0, 1)
                schema_type.append(
                    StructField(c, StringType(), metadata={"maxlength": field_size})
                )

            custom_schema = StructType(schema_type)
            file_df_with_schema = spark.createDataFrame(file_df.rdd, custom_schema)

            # Ensure the target schema exists.
            pkb = cls.get_pkb()
            conn = cls.get_sf_connection(db_server)
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{db_schema_name}")

            sf_options = {
                "sfURL": f"{db_server}.snowflakecomputing.com",
                "sfDatabase": db,
                "sfSchema": db_schema_name,
                "sfStage": SF_STAGE_NAME,
                "sfRole": SF_ROLE,
                "sfUser": SF_DB_USER,
                "pem_private_key": pkb,
            }

            cls.global_schema_name = db_schema_name

            # Reuse the retry-aware writer used by the CDM upload path
            # for consistent transient-error handling.
            cls._snowflake_write_with_retry(
                df=file_df_with_schema,
                sf_options=sf_options,
                table_name=table_name,
                schema_type=schema_type,
                db_schema_name=db_schema_name,
            )

        except Exception as e:
            cls.print_with_style(str(e), 'danger red')
            raise
        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                _get_ovid_logger().error(
                    "db_upload_raw_snowflake: failed to close Snowflake cursor",
                    exc_info=True,
                )
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                _get_ovid_logger().error(
                    "db_upload_raw_snowflake: failed to close Snowflake connection",
                    exc_info=True,
                )
            try:
                if spark is not None:
                    spark.stop()
            except Exception:
                _get_ovid_logger().error(
                    "db_upload_raw_snowflake: failed to stop Spark session",
                    exc_info=True,
                )



###################################################################################################################################
# This function 
###################################################################################################################################

    @classmethod
    def get_pkb(cls):

            SF_PRIVATE_KEY_PASSPHRASE_PATH =  "/ovid_credentials/" + SF_PRIVATE_KEY_PASSPHRASE_FILE_NAME

            with open(SF_PRIVATE_KEY_PASSPHRASE_PATH, "r") as f:
                sf_private_key_passphrase = f.readline().strip()

            SF_PRIVATE_KEY_PATH =  "/ovid_credentials/" + SF_PRIVATE_KEY_FILE_NAME

            with open(SF_PRIVATE_KEY_PATH, "rb") as key:
                private_key = serialization.load_pem_private_key(
                    key.read(),
                    password=sf_private_key_passphrase.encode() if sf_private_key_passphrase else None,
                    backend=default_backend()
                )
        

            pkb = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            pkb = pkb.decode("UTF-8")
            pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", "")

            return pkb


###################################################################################################################################
# This function 
###################################################################################################################################

    @classmethod
    def create_scheduler_db(cls):
       
        
        try:
         
            conn = cls.get_sf_connection(SCHEDULER_DB_SERVER)
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE {SCHEDULER_DB_NAME}")
            cur.execute(f"CREATE SCHEMA {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}")
            cur.execute(f"{CREATE_BATCH_TABLE_DLL}")
            cur.execute(f"{CREATE_BATCH_TABLE_FILE_DLL}")

            conn.close()

              
                
        except Exception as e:

            cls.print_with_style(str(e), 'danger red')




###################################################################################################################################
# This function 
###################################################################################################################################

    @classmethod
    def execute_query(cls, query):
       
        
        try:
        
            conn = cls.get_sf_connection(SCHEDULER_DB_SERVER)
            cur = conn.cursor()
            result = cur.execute(query).fetchall()
   
            cur.close()
            conn.close()

            return result            
        except Exception as e:

            cls.print_with_style(str(e), 'danger red')


###################################################################################################################################
# This function 
###################################################################################################################################

    @classmethod
    def get_sf_connection(cls, DB_SERVER):

        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        
        SF_PRIVATE_KEY_PASSPHRASE_PATH =  "/ovid_credentials/" + SF_PRIVATE_KEY_PASSPHRASE_FILE_NAME

        with open(SF_PRIVATE_KEY_PASSPHRASE_PATH, "r") as f:
                sf_private_key_passphrase = f.readline().strip()

        SF_PRIVATE_KEY_PATH =  "/ovid_credentials/" + SF_PRIVATE_KEY_FILE_NAME

        with open(SF_PRIVATE_KEY_PATH, "rb") as key:
                private_key = serialization.load_pem_private_key(
                    key.read(),
                    password=sf_private_key_passphrase.encode() if sf_private_key_passphrase else None,
                    backend=default_backend()
                )
            
            # Convert to DER format for Snowflake
        private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            # Connect to Snowflake using private key
        conn = snowflake.connector.connect(
                user=SF_DB_USER,
                private_key=private_key_bytes,
                account=DB_SERVER
            )
        
        return conn

###################################################################################################################################
# This function 
###################################################################################################################################

    @classmethod
    def check_scheduler_db_exists(cls):

        try:
            conn = cls.get_sf_connection(SCHEDULER_DB_SERVER)
            cursor = conn.cursor()
            try:
                query = f"SHOW DATABASES LIKE '{SCHEDULER_DB_NAME.upper()}'"
                cursor.execute(query)  # Snowflake stores names in uppercase
                result = cursor.fetchone()  # Fetch once and store result
            finally:
                cursor.close()
                conn.close()

            return result is not None
        
        except Exception as e:
            _get_ovid_logger().error(f"Error checking database existence: {e}")
            return False



###################################################################################################################################
# This function will upload a pyspark df to mssql table in a specified server/dabase
###################################################################################################################################

    @classmethod
    def db_upload_mssql(cls,db, db_server,schema,file_name,table_name, file_path,folder_name):


        try:

            master_url = os.environ["SPARK_URL"].strip().replace('"', '')
            spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()
            # spark = SparkSession.builder.master("spark://spark-master:7077").appName("MSSQL Upload").getOrCreate()

            conf = SparkConf()
            conf.set("spark.driver.extraClassPath", "ovid_setup/jars/mssql-jdbc-driver.jar")
            file_df = spark.read.option("inferSchema", "false").load(file_path+file_name+'*',format="csv", sep="\t", inferSchema="false", header="true",  quote= '"')
            
            string_columns = [f'CAST(`{col}` AS STRING) AS `{col}`' for col in file_df.columns]
            result_df = file_df.selectExpr(*string_columns)

            
            jdbc_url = f"jdbc:sqlserver://{db_server};databaseName={db}"
            
            if schema != None:
                properties = {
                    "user": MSSQL_DB_USER,
                    "password": MSSQL_DB_PASS,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "trustServerCertificate": "true",
                    "createTableColumnTypes": schema}
            else:
                properties = {
                    "user": MSSQL_DB_USER,
                    "password": MSSQL_DB_PASS,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "trustServerCertificate": "true"}


            table_name = f"OVID_{folder_name}_{table_name}"
            
            result_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

            spark.stop()
        except Exception as e:

            spark.stop()
            

            cls.print_with_style(str(e), 'danger red')




###################################################################################################################################
# This function will upload a pyspark df to postgress table in a specified server/dabase
###################################################################################################################################

    @classmethod
    def db_upload_postgress(cls,db, db_server,schema,file_name,table_name, file_path,folder_name):
                #Boot Cluster
        start = time.time()
        date_format_pattern = "yyyy-MM-dd"
        master_url = os.environ["SPARK_URL"].strip().replace('"', '')
        spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()
        # spark = SparkSession.builder.master("spark://spark-master:7077").appName("PSQL Upload").getOrCreate()

        try:
            conf = SparkConf()
            conf.set("spark.driver.extraClassPath", "ovid_setup/jars/postgresql-42.7.4.jar")

            # Parse schema to get column names and types
            column_types = {}
            for line in schema.strip().split(",\n"):
                match = re.match(r"(\w+)\s+(\w+)", line.strip())
                if match:
                    column, dtype = match.groups()
                    column_types[column] = dtype

            # Load your file into a DataFrame
            file_df = spark.read.option("inferSchema", "false").csv(file_path + file_name + '*', sep="\t", header=True, quote='"')


            # Create casting expressions based on parsed schema
            casting_expressions = [
                f'CAST(`{col}` AS {"STRING" if dtype.startswith("VARCHAR") else dtype}) AS `{col}`'
                for col, dtype in column_types.items()
            ]

            # Apply casting expressions
            result_df = file_df.selectExpr(*casting_expressions)
            
            if schema != None:
                properties = {
                    "user": PG_DB_USER,
                    "password": PG_DB_PASS,
                    "driver": "org.postgresql.Driver",
                    "stringtype":"unspecified",
                    "createTableColumnTypes": schema}
            else:
                properties = {
                    "user": PG_DB_USER,
                    "password": PG_DB_PASS,
                    "driver": "org.postgresql.Driver",
                   }


            table_name = f"OVID_{folder_name}_{table_name}"
            jdbc_url = f"jdbc:postgresql://{db_server}/{db}"
            result_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

            spark.stop()
        except Exception as e:

             spark.stop()
            

             cls.print_with_style(str(e), 'danger red')



###################################################################################################################################
# This function will return a list of folder in a given path
###################################################################################################################################
    @classmethod
    def get_folders_list(cls, path):

       
        
        folders_names = os.listdir(path)

        folders_names_list = [file for file in folders_names if file]

        return folders_names_list
###################################################################################################################################
# This 
###################################################################################################################################
    @classmethod
    def clean_excel_value(cls,val):

        if isinstance(val, str):

            # Remove illegal Unicode control characters (0x00 to 0x1F except newline/tab)
            val = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", val)

        return val
###################################################################################################################################
# This function generate a mapping report as a form of an excel file
###################################################################################################################################


    @classmethod
    def generate_mapping_report(cls,mapping_gap_output_folder_path, folder, partner):


        try:

     
            if '/' in folder :
                folder = folder.split('/')[1]


            spark = cls.get_spark_session('generate mapping report')

            empty_columns = pd.DataFrame(columns=[""] * 2)
            # Path to the Excel file
            excel_file_path = mapping_gap_output_folder_path + partner+'_'+folder+"_mapping_report.xlsx"


            # Initialize a workbook object
            wb = openpyxl.Workbook()



            tables_list = cls.get_folders_list(mapping_gap_output_folder_path)

            for table in tables_list :

            


                if 'mapping_report' not in table:

                    safe_sheet_name = re.sub(r'[:\\/*?[\]#]', '_', table)[:31]  # Excel allows max 31 characters

                    ws = wb.create_sheet(title=safe_sheet_name)

                    combined_pd = empty_columns


                    table_all_mappings_path = mapping_gap_output_folder_path +"/"+safe_sheet_name+"/all_mapping/"

                    table_all_mapping_files_list = cls.get_folders_list(table_all_mappings_path)

                    for mapping_file in table_all_mapping_files_list:

                        mapping_df =  cls.spark_read(table_all_mappings_path+mapping_file, spark)

                        mapping_df = mapping_df.limit(1000000)


                        pandas_df = mapping_df.toPandas()
                        combined_pd = pd.concat([combined_pd, empty_columns, pandas_df], axis=1)


                        for col_idx, col_name in enumerate(combined_pd.columns, 1):
                            ws.cell(row=1, column=col_idx, value=col_name)


                        for row_idx, row in enumerate(combined_pd.values, 2):  # Start from row 2 (after header row)
                            for col_idx, value in enumerate(row, 1):

                                value = cls.clean_excel_value(value)

                                cell = ws.cell(row=row_idx, column=col_idx, value=value)

                                # Calculate the width of the cell based on the length of the value
                                if isinstance(value, str):
                                    width = len(value)
                                else:
                                    width = len(str(value))

                                # Update the column width if necessary
                                if width > ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width:
                                    ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = width


            if 'Sheet' in wb.sheetnames:
                # Remove the sheet
                wb.remove(wb['Sheet'])

            wb.save(excel_file_path)




            spark.stop()


        except Exception as e:

            spark.stop()
            cls.print_failure_message(
                                    folder  = folder,
                                    partner = partner.lower(),
                                    job     = 'mapping_report' ,
                                    text = str(e)
                                    )




###################################################################################################################################
# This function will return a pysark dataframe for a givin file path
###################################################################################################################################


    @classmethod
    def spark_read(cls,file_path, spark):

        # return spark.read.option("inferSchema", "false").load(file_path,format="csv",   sep="\t", inferSchema="false", header="true",  quote= '"')


        return spark.read.csv(
                file_path,
                sep="\t",
                header=True,
                quote='"',
                inferSchema=False,
                multiLine=False,
                escape="\""
            )


###################################################################################################################################
# This function reads the OMOP CONCEPT.csv file and normalises every column name to
# lowercase so that downstream partner-formatter code referencing concept.domain_id /
# col("vocabulary_id") / etc. resolves regardless of the file's source casing.
###################################################################################################################################


    @classmethod
    def spark_read_omop_concept(cls, file_path, spark):
        """Read the OMOP CONCEPT.csv file with case-normalised column headers.

        The standard Athena OMOP CONCEPT export ships with UPPERCASE column
        headers (CONCEPT_ID, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CODE, ...).
        Partner formatters reference these columns in lowercase. This helper
        reads the file via cf.spark_read and renames every column to lowercase
        so that downstream `.domain_id` / `col("vocabulary_id")` references
        resolve regardless of the file's source casing.

        Use this method for /app/common/omop_cdm/CONCEPT.csv only. Other OMOP
        files in that directory (CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR, ...)
        already ship in lowercase and should continue to use cf.spark_read.

        Args:
            file_path: Absolute path to CONCEPT.csv.
            spark: Active SparkSession.

        Returns:
            A Spark DataFrame whose column names are all lowercase.
        """
        df = cls.spark_read(file_path, spark)
        return df.toDF(*[c.lower() for c in df.columns])




###################################################################################################################################
# This function will append any additional fields that are not in the Pcornet CDM to the mapper output
###################################################################################################################################

   

    @classmethod
    def append_additional_fields(cls,mapped_df, file_name, deduplicated_data_folder_path,join_field, spark ):


        # read the deduplicated file
        deduplicated_file = spark.read.load(deduplicated_data_folder_path+file_name,format="csv", sep="\t", inferSchema="true", header="true", quote= '"')
        
        if len(deduplicated_file.columns) + 3 > len(mapped_df.columns) :

            #get the headers from the deduplicated and the mapped files for comparison
            deduplicated_file_columns = deduplicated_file.columns
            mapped_df_columns = mapped_df.columns


            # Identify common headers
            common_headers = set(mapped_df_columns).intersection(deduplicated_file_columns)
            common_headers_without_the_join_field = [item for item in common_headers if item != join_field]

            # Drop common headers from the second DataFrame
            additional_fields_data = deduplicated_file.drop(*common_headers_without_the_join_field)
            
            #Renaming the join field to avoid conflect during the join
            join_field_temp_name = join_field+"_new"

            additional_fields_data = additional_fields_data.withColumnRenamed(join_field, join_field_temp_name)
            
            # Join DataFrames based on the "Name" column
            appended_payspark_data_frame = mapped_df.join(additional_fields_data, mapped_df["JOIN_FIELD"]==additional_fields_data[join_field_temp_name])
        
        #Dropping the temporary join fields

            columns_to_drop = ["JOIN_FIELD",join_field_temp_name ]
            appended_payspark_data_frame = appended_payspark_data_frame.drop(*columns_to_drop)

            return appended_payspark_data_frame
        
        else:

            return mapped_df.drop("JOIN_FIELD")
        


###################################################################################################################################
# This function will convert all known time formats to the '%H:%M'format
###################################################################################################################################

    @staticmethod
    def format_time( val):
        """ Convenience method to try all known formats """

        try:
            parsed_time = dp.parse(val)
        except Exception as exc:
           
            return ""
        return parsed_time.strftime('%H:%M')


###################################################################################################################################
# This will ensure the value is a float or it will return None
###################################################################################################################################

    @staticmethod
    def copy_if_float( val):
        """ Convenience method to try all known formats """
        if not val:
            return None
        try:
            return float(val)
        except:

            return None



###################################################################################################################################
# This function will convert all known date formats to the '%Y-%m-%d format
###################################################################################################################################

   
    @staticmethod
    def format_date( val):
        """ Convenience method to try all known formats """
        _get_ovid_logger().debug(f"format_date input: {val}")
        try:
            parsed_date = dp.parse(val)
        except:

            _get_ovid_logger().debug(f"format_date parse failed for: {val}")
           
            return None

        return parsed_date.strftime('%Y-%m-%d')




###################################################################################################################################
# This function will return the time portion from a datetime value
###################################################################################################################################

   
    @staticmethod
    def get_time_from_datetime(  dateTime):
        
            try:
                
                return str(dateTime.time().strftime('%H:%M:%S'))[0:5]
            except:
                return None

###################################################################################################################################
# This function will return the date portion from a datetime value
###################################################################################################################################

   
    @staticmethod
    def get_date_from_datetime(  dateTime):
        
        try:
            
            return str(dateTime.strftime("%Y-%m-%d"))
        except:
            return None

        
  
###################################################################################################################################
# This function will validate if a giving partenr name is valid or not
###################################################################################################################################

   

    @classmethod
    def valid_partner_name(cls, input_partner_name ):

        
        partners_root = "/app/partners"

        if not os.path.exists(partners_root):
            return None

        partners_list = [
            name for name in os.listdir(partners_root)
            if os.path.isdir(os.path.join(partners_root, name))
        ]

        if input_partner_name in partners_list:
            return True
        else:
            return None
###################################################################################################################################
# This function will create and return a spark session
###################################################################################################################################

    # Process-level cache for the common/*.py distribution zip so it is built
    # at most once per process instead of on every get_spark_session() call.
    _common_pyfiles_zip_path: Optional[str] = None

    @classmethod
    def _ensure_common_pyfiles_zip(cls) -> Optional[str]:
        """Build (once per process) the zip of common/*.py for executor imports.

        Spark's ``addPyFile`` distributes these modules to executor Python paths
        so cloudpickled UDFs can deserialize. The zip content is constant for the
        life of the process, so it is built lazily on first use and cached on the
        class for every subsequent ``get_spark_session()`` call.

        Returns:
            The path to the built zip, or ``None`` if no common/*.py files exist.
        """
        if cls._common_pyfiles_zip_path is not None:
            return cls._common_pyfiles_zip_path

        import zipfile
        import tempfile

        common_py_files = glob.glob('/app/common/*.py')
        if not common_py_files:
            return None

        tmp_zip = os.path.join(tempfile.gettempdir(), 'ovid_common.zip')
        with zipfile.ZipFile(tmp_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for source_file in common_py_files:
                zip_file.write(source_file, os.path.basename(source_file))

        cls._common_pyfiles_zip_path = tmp_zip
        return tmp_zip

    @classmethod
    def get_spark_session(cls, appName):
        # Non-destructive: return the existing in-process SparkContext when
        # one is already active, otherwise create a new one. Do NOT stop a
        # pre-existing active context here.
        #
        # History: an earlier version of this helper (2026-06-11) tried to
        # tear down any pre-existing `_active_spark_context` before calling
        # getOrCreate(), reasoning that a leaked py4j gateway from a parent
        # process could otherwise trigger SPARK-2243 ("Only one SparkContext
        # should be running in this JVM"). That destructive teardown was an
        # over-correction: the actual cross-process gateway-leak path is
        # already closed at the subprocess boundary by the
        # PYSPARK_GATEWAY_PORT / PYSPARK_GATEWAY_SECRET / PYSPARK_PIN_THREAD
        # env-var scrub in cls.run_subprocess() (and apply_fix() routes
        # through run_subprocess() so fix-script subprocesses inherit the
        # scrub too). With that protection in place, every SparkContext
        # this function encounters in the active slot is the SAME parent
        # process's live context, NOT a leaked sibling -- and stopping it
        # destroys any DataFrame the caller is currently holding.
        #
        # Concrete failure mode the destructive variant introduced
        # (observed in the EMY 20260610 batch's CONDITION upload at
        # 19:10:24 EDT on 2026-06-11): db_upload_snowflake() calls
        # cls.get_spark_session("Upload"), then spark.read.load(...) to
        # produce a lazy `file_df`, then cls.get_sf_schema(file_df, ...)
        # which itself called cls.get_spark_session("Get Schema"). The
        # second call's teardown stopped the "Upload" context that owned
        # `file_df`, so the next access (`file_df.rdd` at line 4888) raised
        # `Cannot call methods on a stopped SparkContext`. Reverting to a
        # plain getOrCreate() restores the working pre-2026-06-11 behaviour
        # for this re-entrant pattern without re-opening the cascade
        # vulnerability that the 2026-06-11 entry was trying to address.

        spark = SparkSession.builder \
            .master(os.environ["SPARK_URL"]) \
            .appName(appName) \
            .config("spark.driver.memory", DEFAULT_SUBPROCESS_DRIVER_MEMORY) \
            .config("spark.executor.memory", DEFAULT_SUBPROCESS_EXECUTOR_MEMORY) \
            .config("spark.executor.extraJavaOptions", f"-Xss{DEFAULT_SPARK_THREAD_STACK_SIZE}") \
            .config("spark.driver.extraJavaOptions", f"-Xss{DEFAULT_SPARK_THREAD_STACK_SIZE}") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.shuffle.partitions", str(DEFAULT_SHUFFLE_PARTITIONS)) \
            .config("spark.serializer", DEFAULT_SPARK_SERIALIZER) \
            .config("spark.sql.autoBroadcastJoinThreshold", DEFAULT_AUTO_BROADCAST_JOIN_THRESHOLD) \
            .config("spark.task.maxFailures", "12") \
            .config("spark.stage.maxConsecutiveAttempts", "8") \
            .config("spark.excludeOnFailure.enabled", "true") \
            .config("spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor", "2") \
            .config("spark.excludeOnFailure.task.maxTaskAttemptsPerNode", "4") \
            .config("spark.speculation", "true") \
            .config("spark.speculation.multiplier", "1.5") \
            .config("spark.speculation.quantile", "0.9") \
            .config("spark.files", "") \
            .config("spark.submit.pyFiles", "") \
            .config("spark.executorEnv.PYTHONPATH", "/app/common:/app") \
            .getOrCreate()
        spark.sparkContext.setLogLevel('OFF')

        # Ensure workers can import common modules (e.g. commonFunctions)
        # when deserializing UDFs sent via cloudpickle.
        # /app/common is volume-mounted on all nodes; addPyFile makes Spark
        # distribute it to executor Python paths. The zip is built once per
        # process (cached below) rather than rebuilt on every session creation.
        try:
            common_zip_path = cls._ensure_common_pyfiles_zip()
            if common_zip_path is not None:
                spark.sparkContext.addPyFile(common_zip_path)
        except Exception:
            _get_ovid_logger().warning(
                "Failed to distribute common/*.py via addPyFile; relying on "
                "executorEnv.PYTHONPATH (/app/common:/app) as the primary "
                "import mechanism.",
                exc_info=True,
            )

        return spark

###################################################################################################################################
    @classmethod
    def print_failure_message(cls, folder,partner, job, text):

        # Log the failure to the OVID log file
        try:
            _ovid_logger = _get_ovid_logger()
            _ovid_logger.error(f"FAILURE: Partner={partner}, Folder={folder}, Job={job} -- {text}")
        except Exception:
            pass  # Don't let logging failures break output

        # Record the failure for apply_fix (private marker; no-op outside a fix).
        cls._record_fix_failure(str(text))

        # Build the failure message string
        current_utc_datetime = datetime.utcnow()
        new_york_tz = pytz.timezone('America/New_York')

        current_ny_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(new_york_tz)

        formatted_ny_datetime = current_ny_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        message = formatted_ny_datetime


        while len(message) < 25:
            message = message + ' '

        # Add placeholder for Job number to align with normal output
        message = message + "           "  # Space for "Job: XX/YY"

        while len(message) < 39:
            message = message + ' '


        message = message + "--Partner: " + partner

    
        while len(message) < 55:
            message = message + ' '


        message = message + "--Folder: " + folder

    
        while len(message) < 80:
            message = message + ' '
        

        message = message+' --Running: '+ job+ ' Failed!!!' 

        while len(message) < 125:
            message = message + ' '

        while len(message) < 145:
            message = message + '.'

        if "Path does not exist" in text or "does not exist in" in text:

            color = 'warning orange'

        else:

            color = 'danger red'

        # Determine the ANSI color codes for styling
        bold_code = '\033[1m'
        reset_code = '\033[0m'
        if color == 'warning orange':
            color_code = '\033[38;5;214m'
        else:  # danger red
            color_code = '\033[0;31m'
        
        styled_message = bold_code + color_code + message + reset_code
        styled_text = bold_code + color_code + text + reset_code

        # Check if we're in the parent process (has active display) or a subprocess
        if cls._display_lines:
            # Parent process: stop timer, add failure to display buffer, and finalize
            cls._stop_timer_thread()
            cls._write_failure_flag()
            
            # Add failure lines to buffer before the blank separator
            separator_idx = len(cls._display_lines) - 2
            if separator_idx < 1:
                separator_idx = 1
            cls._display_lines.insert(separator_idx, styled_message)
            cls._display_lines.insert(separator_idx + 1, styled_text)
            cls._render()
        else:
            # Subprocess: write to failure queue and set failure flag
            # The parent process timer thread will pick these up
            cls._write_failure_flag()
            cls._write_to_failure_queue(styled_message, styled_text)




###################################################################################################################################
# This 
###################################################################################################################################
    @classmethod
    def _compute_max_field_sizes(cls, df, columns: list[str]) -> dict[str, str]:
        """Compute the max string length of each column in a single table scan.

        Replaces the previous pattern of one full-table scan per column. All
        requested columns are aggregated in one ``select(...).collect()`` call.
        Null/empty columns are clamped to "1" so Snowflake accepts the VARCHAR
        width (matching the per-column fallback the old code used).

        Args:
            df: The Spark DataFrame to measure.
            columns: The column names to compute max lengths for.

        Returns:
            A mapping of column name to its max length as a string (>= "1").
        """
        if not columns:
            return {}

        max_row = df.select(
            [spark_max(length(col(c))).alias(c) for c in columns]
        ).collect()[0]

        sizes: dict[str, str] = {}
        for c in columns:
            raw_len = max_row[c]
            try:
                if int(raw_len) >= 1:
                    sizes[c] = str(raw_len)
                else:
                    sizes[c] = "1"
            except (TypeError, ValueError):
                sizes[c] = "1"
        return sizes

    @classmethod
    def get_max_feild_size_from_df(cls, df, field_name, max_lengths: Optional[dict[str, str]] = None):
        """Return the max string length of a column.

        Args:
            df: The Spark DataFrame containing the column.
            field_name: The column to measure.
            max_lengths: Optional precomputed map of column -> max length (from
                ``_compute_max_field_sizes``). When provided and it contains
                ``field_name``, the value is returned without a Spark action,
                avoiding a redundant full-table scan.

        Returns:
            The max length as a string (>= "1").
        """
        if max_lengths is not None and field_name in max_lengths:
            return max_lengths[field_name]

        max_length = str(df.select(spark_max(length(df[field_name]))).collect()[0][0])

        try:
            if int(max_length) >= 1:
               pass
        except (TypeError, ValueError):
            max_length = "1"


        return max_length





###################################################################################################################################
# This 
###################################################################################################################################
    @classmethod
    def get_field_config(cls, file_path, field_name, field_data_type, spark):

        harmonized_size =0 

        field_max_size = int(cls.get_max_feild_size_from_df(file_path,field_name, spark))


        if 'RDBMS Text' in field_data_type:

                field_type = StringType()

                cdm_field_size_list = field_data_type.split('(')

                cdm_field_size = cdm_field_size_list[len(cdm_field_size_list)-1].replace(')','')

                if 'x' in cdm_field_size:
                    
                    if 'RAW' in field_name:


                        cdm_field_size  = cls.get_max_feild_size_from_df(file_path,field_name, spark)


                    else:
                    

                        cdm_field_size = harmonized_filed_sizes_dict.get(field_name,0)
                
                

                if int(cdm_field_size) > int(field_max_size):
                        field_size = cdm_field_size
                else:
                        field_size = field_max_size





                field_config = StructField(field_name, field_type, metadata={"maxlength":int(field_size)})


                    

        if 'RDBMS Date' in field_data_type:

            field_type = DateType()

            field_config =   StructField(field_name, field_type, True)   

        if 'RDBMS Number' in field_data_type:

            field_type = DoubleType() 

            field_config =    StructField(field_name, field_type, True)   


        return field_config




###################################################################################################################################
# This 
###################################################################################################################################


    @classmethod
    def get_df_field_config(cls, df, field_name, field_data_type, max_lengths: Optional[dict[str, str]] = None):
        """
        Generate a StructField for Snowflake schema based on CDM field info and actual DataFrame.

        Args:
            df (DataFrame): Spark DataFrame containing the table data.
            field_name (str): Field/column name.
            field_data_type (str): CDM RDBMS data type (e.g., 'RDBMS Text(50)').
            max_lengths (Optional[dict[str, str]]): Precomputed column -> max length
                map from ``_compute_max_field_sizes``. When supplied, avoids a
                per-column full-table scan.

        Returns:
            StructField: Field configuration for Snowflake schema.
        """
        harmonized_size = 0

        # Get max size from the DataFrame
        field_max_size = int(cls.get_max_feild_size_from_df(df, field_name, max_lengths=max_lengths))

        if 'RDBMS Text' in field_data_type:
            field_type = StringType()

            # Extract CDM field size from string like "RDBMS Text(50)"
            cdm_field_size_list = field_data_type.split('(')
            cdm_field_size = cdm_field_size_list[-1].replace(')', '')

            # Handle "x" notation or RAW fields
            if 'x' in cdm_field_size:
                if 'RAW' in field_name.upper():
                    cdm_field_size = cls.get_max_feild_size_from_df(df, field_name, max_lengths=max_lengths)
                else:
                    cdm_field_size = harmonized_filed_sizes_dict.get(field_name, 0)

            # Pick the larger of CDM size vs actual data size
            # field_size = max(int(cdm_field_size), int(field_max_size))

            if int(cdm_field_size) > int(field_max_size):

                field_size = int(cdm_field_size)
            else:

                field_size = int(field_max_size)

            field_config = StructField(field_name, field_type, metadata={"maxlength": int(field_size)})

        elif 'RDBMS Date' in field_data_type:
            field_type = DateType()
            field_config = StructField(field_name, field_type, True)

        elif 'RDBMS Number' in field_data_type:
            field_type = DoubleType()
            field_config = StructField(field_name, field_type, True)

        else:
            # Default to string if type is unknown
            field_config = StructField(field_name, StringType(), True)

        return field_config


###################################################################################################################################
# This 
###################################################################################################################################
    @classmethod
    def get_field_config_snowflake(cls, field_name, field_data_type, spark, field_size_overrides=None):

        """Build a Snowflake StructField for one CDM field.

        Args:
            field_name (str): CDM field name.
            field_data_type (str): CDM RDBMS type string (e.g. ``RDBMS Text(x)``).
            spark: Active SparkSession (unused, kept for signature compatibility).
            field_size_overrides (Optional[dict[str, int]]): Optional per-run map
                of ``field_name -> harmonized width``. When the field has an
                undefined CDM length (``x`` notation, non-RAW) and an override is
                supplied, the override wins over ``harmonized_filed_sizes_dict``.
                Used to size empty complementary tables so their ID columns match
                the width already present in the partner's populated tables
                (PCORnet DC 1.15a field-length harmonization).

        Returns:
            StructField: The Snowflake field configuration.
        """

        if 'RDBMS Text' in field_data_type:

                field_type = StringType()

                cdm_field_size_list = field_data_type.split('(')

                cdm_field_size = cdm_field_size_list[len(cdm_field_size_list)-1].replace(')','')

                if 'x' in cdm_field_size:
                    
                    if 'RAW' in field_name:


                        cdm_field_size  = 20


                    else:

                        # Prefer a per-run harmonized override (matches the width
                        # already declared by this partner's populated tables) so
                        # empty complementary tables do not diverge -> DC 1.15a.
                        if field_size_overrides and field_name in field_size_overrides:
                            cdm_field_size = field_size_overrides[field_name]
                        else:
                            cdm_field_size = harmonized_filed_sizes_dict.get(field_name,20)
                
                





                field_config = StructField(field_name, field_type, metadata={"maxlength":int(cdm_field_size)})


                    

        if 'RDBMS Date' in field_data_type:

            field_type = DateType()

            field_config =   StructField(field_name, field_type, True)   

        if 'RDBMS Number' in field_data_type:

            field_type = DoubleType() 

            field_config =    StructField(field_name, field_type, True)   


        return field_config


###################################################################################################################################
# This 
###################################################################################################################################
    @classmethod
    def get_schema_line(cls, file_path, field_name, field_data_type, spark):

        harmonized_size =0 

        field_max_size = int(cls.get_max_feild_size_from_df(file_path,field_name, spark))


        if 'RDBMS Text' in field_data_type:

                schema_type = 'VARCHAR'

                cdm_field_size_list = field_data_type.split('(')

                cdm_field_size = cdm_field_size_list[len(cdm_field_size_list)-1].replace(')','')

                if 'x' in cdm_field_size:
                    
                    if 'RAW' in field_name:


                        cdm_field_size  = cls.get_max_feild_size_from_df(file_path,field_name, spark)

                    else:
                    

                        cdm_field_size = harmonized_filed_sizes_dict.get(field_name,0)
                


                if int(cdm_field_size) > int(field_max_size):
                        field_size = cdm_field_size
                else:
                        field_size = field_max_size



                schema_line =     field_name +' '+ schema_type+"("+str(field_size)+"),\n"


                    

        if 'RDBMS Date' in field_data_type:

            schema_type = 'DATE'

            schema_line =     field_name +' '+ schema_type+",\n"

        if 'RDBMS Number' in field_data_type:

            schema_type = 'FLOAT'

            schema_line =     field_name +' '+ schema_type+",\n"


        return schema_line

        


        


###################################################################################################################################
# This function will adjust the schema fields depend on the size specified by the user or the max size in the field
###################################################################################################################################
    @classmethod
    def get_schemas(cls, file_name, table_path):






        file_path = table_path+'fixed_'+file_name.lower()+".csv"

        schema_mssql = ""
        schema_sf    = []
        schemas      = []
        cdm_fields   = []

        try:

            spark = cls.get_spark_session("Get Schema")


            cdm_df = PcornetCDM.get_cdm_df(spark)

            table_cdm_df = cdm_df.filter(cdm_df["TABLE_NAME"]==file_name)

            table_cdm = table_cdm_df.collect()

            fixed_table_df = cls.spark_read(file_path,spark)
    



            for row in table_cdm:


                schema_mssql = schema_mssql + '\n' + cls.get_schema_line(file_path,row["FIELD_NAME"],row["RDBMS_DATA_TYPE"] , spark) 
                schema_sf.append( cls.get_field_config(file_path,row["FIELD_NAME"],row["RDBMS_DATA_TYPE"] , spark) )
                cdm_fields.append(row["FIELD_NAME"])



            

            schema_mssql = schema_mssql +"UPDATED VARCHAR(19),\n"
            schema_mssql = schema_mssql +"SOURCE VARCHAR(20)"
            schema_sf.append(StructField("UPDATED", StringType(), metadata={"maxlength":19}),)
            schema_sf.append(StructField("SOURCE", StringType(), metadata={"maxlength":20}),)

            

            for field in fixed_table_df.columns:

                if field not in cdm_fields and field.upper() not in ['SOURCE','UPDATED']:

                    field_size = int(cls.get_max_feild_size_from_df(file_path, field, spark))

                    schema_sf.append(StructField(field, StringType(), metadata={"maxlength":field_size}),)
                    schema_mssql = schema_mssql +F",\n{field} VARCHAR({field_size})"


            schemas =  [schema_mssql,schema_sf]

            spark.stop()

        except Exception as e:
           
            spark.stop() 
            cf.print_failure_message(
                                    folder  = '',
                                    partner = '',
                                    job     = '' ,
                                    text = str(e)
                                    )

        return schemas


###################################################################################################################################
# This function will get snowflake schema for a table
###################################################################################################################################
    @classmethod
    def get_snowflake_schema(cls, table_name, spark, mappers_type=None, field_size_overrides=None):

            schema_sf = []
            cdm_df = PcornetCDM.get_cdm_df(spark)

            table_cdm_df = cdm_df.filter(cdm_df["TABLE_NAME"]==table_name)

            # table_cdm_df.show() 

            cdm_rows = list(table_cdm_df.collect())

            # Supplement with the secondary (non-PCORnet) CDM so empty
            # complementary tables for nursing partners are created with the
            # correct columns/types instead of only UPDATED+SOURCE. PCORnet
            # definitions take precedence on field-name collisions.
            secondary_cdm_df = PcornetCDM.get_secondary_cdm_df(spark, mappers_type)
            if secondary_cdm_df is not None:
                existing_field_names = {row["FIELD_NAME"] for row in cdm_rows}
                secondary_rows = secondary_cdm_df.filter(
                    secondary_cdm_df["TABLE_NAME"] == table_name
                ).collect()
                for row in secondary_rows:
                    if row["FIELD_NAME"] not in existing_field_names:
                        cdm_rows.append(row)
                        existing_field_names.add(row["FIELD_NAME"])

            for row in cdm_rows:


                schema_sf.append( cls.get_field_config_snowflake(row["FIELD_NAME"],row["RDBMS_DATA_TYPE"] , spark, field_size_overrides=field_size_overrides) )


            schema_sf.append(StructField("UPDATED", StringType(), metadata={"maxlength":100}),)
            schema_sf.append(StructField("SOURCE", StringType(), metadata={"maxlength":20}),)
            schema_sf = StructType(schema_sf)

            return schema_sf     




###################################################################################################################################
# This function will verify the passed value is a float or it will return null
###################################################################################################################################
    @classmethod

    def return_if_float(cls, val):
        try:
            float(val)
            return val
        except:
            #Not a float
            return None
    

###################################################################################################################################
# This function will get all the created mappers python script and return a list of thier names
###################################################################################################################################
    # ===============================================================================================
    # LINE-BUFFER DISPLAY SYSTEM
    #
    # All terminal output for the progress display goes through a single _render() method.
    # The display state is maintained in _display_lines (a list of strings).
    # The timer thread and main thread both update _display_lines and call _render().
    # A threading lock (_display_lock) ensures only one render happens at a time.
    #
    # For subprocess/fixer output, the subprocess writes lines to a file-based queue
    # (one line per file in .ovid_sync/fixer_queue/). The parent's timer thread polls
    # this queue and adds lines to the display buffer. This ensures only the parent
    # process ever writes to the terminal.
    # ===============================================================================================

    # Class variables for the line-buffer display system
    _display_lines = []           # The line buffer - single source of truth for what's on screen
    _rendered_line_count = 0      # How many lines are currently rendered on screen
    _display_lock = None          # threading.Lock - guards all terminal writes
    _timer_running = False
    _timer_thread = None
    _overall_start_time = None
    _current_job_start = None
    _current_job_message = ""     # The base job message (without time)
    _subprocess_running = False   # True when a subprocess (formatter/fixer) is running
    _fixer_lines_consumed = 0     # How many fixer queue files we've already read
    _failure_lines_consumed = 0   # How many failure queue files we've already read
    _job_finalized = False        # True after job line has been printed as permanent output

    @classmethod
    def _get_display_lock(cls):
        """Get or create the display lock."""
        import threading
        if cls._display_lock is None:
            cls._display_lock = threading.Lock()
        return cls._display_lock

    @classmethod
    def _render(cls):
        """Render the current display buffer to the terminal.
        
        This is the ONLY method that writes to the terminal for progress display.
        It moves the cursor up to clear previously rendered lines, then reprints
        all lines from the buffer. Protected by _display_lock.
        
        Cursor convention: each line is printed with a trailing newline.
        After rendering N lines, the cursor sits at column 0 of line N+1
        (the line AFTER the last rendered line). On the next render, we
        move up exactly _rendered_line_count lines to reach the first line.
        """
        import sys
        lock = cls._get_display_lock()
        with lock:
            # Move cursor back to the start of the rendered area.
            # After previous render the cursor is at column 0 of the line
            # AFTER all rendered lines (because every line ended with \n).
            # So we move up _rendered_line_count lines to get to the first line.
            if cls._rendered_line_count > 0:
                sys.stdout.write(f'\033[{cls._rendered_line_count}A')

            # Print all lines from buffer, each terminated with \n
            for line in cls._display_lines:
                sys.stdout.write('\033[2K' + line + '\n')
            
            # If we previously rendered MORE lines than now, clear the leftover lines
            extra = cls._rendered_line_count - len(cls._display_lines)
            if extra != 0:
                try:
                    _ovid_logger = _get_ovid_logger()
                    if _ovid_logger:
                        import re as _re
                        _ansi_re = _re.compile(r'\033\[[0-9;]*m')
                        _stripped = [_ansi_re.sub('', l)[:80] for l in cls._display_lines]
                        _ovid_logger.debug(
                            f"[render] Line count changed: "
                            f"prev={cls._rendered_line_count} -> "
                            f"now={len(cls._display_lines)}, "
                            f"lines={_stripped}"
                        )
                except Exception:
                    pass
            for _ in range(extra):
                sys.stdout.write('\033[2K\n')
            
            # Update _rendered_line_count to the number of lines we just printed.
            # Next render will move up exactly this many lines.
            cls._rendered_line_count = len(cls._display_lines)
            
            # Move cursor back up to sit right after the last rendered line
            # Actually, we want to be _rendered_line_count lines below where we
            # started. The cursor is already there if no extra clearing happened.
            # If extra clearing happened, move back up.
            if extra > 0:
                sys.stdout.write(f'\033[{extra}A')
            
            sys.stdout.flush()

    @classmethod
    def _finalize_for_next_job(cls):
        """Finalize the current display: print job line (with final time) as permanent output,
        clear all ephemeral lines (blank, total elapsed), and reset for next job.
        
        Fixer lines ARE printed as permanent output (they describe completed work).
        
        Uses same cursor convention as _render(): cursor starts at column 0 of the line
        AFTER all rendered lines. We move up _rendered_line_count to reach the first line.
        """
        import sys
        lock = cls._get_display_lock()
        with lock:
            if cls._rendered_line_count > 0:
                # Move up to the first rendered line
                sys.stdout.write(f'\033[{cls._rendered_line_count}A')
            
            # Count how many permanent lines we'll print (job line + fixer lines)
            permanent_lines = 0

            # Print the finalized job line with final time (as permanent output)
            if cls._display_lines:
                # The first line is always the job line
                sys.stdout.write('\033[2K' + cls._display_lines[0] + '\n')
                permanent_lines += 1
                
                # Print fixer lines as permanent output.
                # Layout: [job_line, *fixer_lines, blank, elapsed]
                for i in range(1, len(cls._display_lines)):
                    line = cls._display_lines[i]
                    # Stop at blank separator line or Total Elapsed line
                    if line.strip() == '' or 'Total Elapsed Time:' in line:
                        break
                    sys.stdout.write('\033[2K' + line + '\n')
                    permanent_lines += 1
            
            # Clear any remaining lines that were rendered but are now discarded
            leftover = cls._rendered_line_count - permanent_lines
            for _ in range(leftover):
                sys.stdout.write('\033[2K\n')
            
            # Move cursor back up to sit right after the permanent lines
            if leftover > 0:
                sys.stdout.write(f'\033[{leftover}A')
            
            sys.stdout.flush()

            # Reset display state
            cls._display_lines = []
            cls._rendered_line_count = 0
            cls._job_finalized = True

    @classmethod
    def _build_time_strings(cls):
        """Calculate and return (job_time_str, total_time_str) for the current moment."""
        current_time = datetime.utcnow()
        
        job_duration = current_time - cls._current_job_start
        job_total_seconds = job_duration.total_seconds()
        job_minutes, job_seconds = divmod(job_total_seconds, 60)
        job_time_str = f"{int(job_minutes):02d}:{job_seconds:04.1f}"
        
        total_elapsed = current_time - cls._overall_start_time
        total_total_seconds = total_elapsed.total_seconds()
        total_minutes, total_seconds = divmod(total_total_seconds, 60)
        total_time_str = f"{int(total_minutes):02d}:{total_seconds:04.1f}"
        
        return job_time_str, total_time_str

    @classmethod
    def _build_job_line(cls, job_time_str):
        """Build the formatted job line with ANSI color codes."""
        green_code = '\033[1m\033[92m'
        white_code = '\033[97m'
        reset_code = '\033[0m'
        return f"{green_code}{cls._current_job_message}{reset_code} {white_code}{job_time_str}{reset_code}"

    @classmethod
    def _build_total_line(cls, total_time_str):
        """Build the Total Elapsed Time line.

        Total Elapsed Time is right-aligned so it lines up with the job timer above.
        """
        white_code = '\033[97m'
        reset_code = '\033[0m'
        padding = " " * 126
        return f"{padding}{white_code}Total Elapsed Time: {total_time_str}{reset_code}"

    @classmethod
    def _rebuild_display_lines(cls, job_time_str, total_time_str, fixer_lines=None):
        """Rebuild the _display_lines buffer with updated times.
        
        Layout:
          [0]       Job line (with job_time_str)
          [1..N]    Fixer lines (if any)
          [N+1]     Blank separator line
          [N+2]     Total Elapsed Time
        """
        lines = [cls._build_job_line(job_time_str)]
        if fixer_lines:
            lines.extend(fixer_lines)
        lines.append("")  # Blank separator
        lines.append(cls._build_total_line(total_time_str))
        cls._display_lines = lines

    @classmethod
    def _get_current_fixer_lines(cls):
        """Extract fixer lines from the current display buffer.

        Fixer lines sit right after the job line (index 0), up to the
        blank separator line.
        """
        fixer_lines = []
        for i in range(1, len(cls._display_lines)):
            line = cls._display_lines[i]
            if line.strip() == '' or 'Total Elapsed Time:' in line:
                break
            fixer_lines.append(line)
        return fixer_lines

    # --- Sync directory and file-based queue for subprocess communication ---

    @classmethod
    def _get_sync_dir(cls):
        """Get the sync directory path, creating it if needed."""
        import os
        work_dir = os.environ.get('OVID_WORK_DIR', '/app')
        sync_dir = os.path.join(work_dir, '.ovid_sync')
        os.makedirs(sync_dir, exist_ok=True)
        return sync_dir

    @classmethod
    def _get_fixer_queue_dir(cls):
        """Get the fixer queue directory path, creating it if needed."""
        import os
        sync_dir = cls._get_sync_dir()
        queue_dir = os.path.join(sync_dir, 'fixer_queue')
        os.makedirs(queue_dir, exist_ok=True)
        return queue_dir

    @classmethod
    def _clear_fixer_queue(cls) -> None:
        """Remove all files in the fixer queue directory and reset the consumed cursor.

        Called at subprocess setup/teardown so stale fixer status lines from a
        previous run/subprocess are never replayed into the current display.

        Returns:
            None.
        """
        import os
        import glob as glob_module
        try:
            queue_dir = cls._get_fixer_queue_dir()
            for f in glob_module.glob(os.path.join(queue_dir, '*.txt')):
                os.remove(f)
        except OSError:
            _get_ovid_logger().error(
                "Failed to clear fixer queue directory", exc_info=True
            )
        cls._fixer_lines_consumed = 0

    @classmethod
    def _write_to_fixer_queue(cls, styled_line: str) -> None:
        """Write a styled fixer status line to the fixer queue (called from a subprocess).

        Each line is written to a separate zero-padded numbered file so the parent
        can read them in order. An exclusive file lock ensures unique sequence
        numbers across concurrent subprocesses.

        Args:
            styled_line: The ANSI-styled status line to enqueue.

        Returns:
            None.
        """
        import os
        import fcntl
        queue_dir = cls._get_fixer_queue_dir()
        lock_path = os.path.join(cls._get_sync_dir(), 'queue.lock')

        lock_fd = None
        try:
            lock_fd = open(lock_path, 'a+')
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)

            # Count existing files to determine next sequence number
            existing = [f for f in os.listdir(queue_dir) if f.endswith('.txt')]
            seq = len(existing)

            # Write the line to a numbered file
            line_path = os.path.join(queue_dir, f'{seq:06d}.txt')
            with open(line_path, 'w') as f:
                f.write(styled_line)
        except OSError:
            _get_ovid_logger().error(
                "Failed to write to fixer queue", exc_info=True
            )
        finally:
            if lock_fd:
                try:
                    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
                    lock_fd.close()
                except OSError:
                    _get_ovid_logger().error(
                        "Failed to release fixer queue lock", exc_info=True
                    )

    @classmethod
    def _poll_fixer_queue(cls):
        """Poll the fixer queue for new lines. Returns list of new styled lines (may be empty)."""
        import os
        new_lines = []
        try:
            queue_dir = cls._get_fixer_queue_dir()
            files = sorted([f for f in os.listdir(queue_dir) if f.endswith('.txt')])
            
            # Only read files we haven't consumed yet
            for f in files[cls._fixer_lines_consumed:]:
                path = os.path.join(queue_dir, f)
                try:
                    with open(path, 'r') as fh:
                        line = fh.read().rstrip('\n')
                        if line:
                            new_lines.append(line)
                except:
                    pass
            
            cls._fixer_lines_consumed = len(files)
        except:
            pass
        return new_lines

    # --- Failure flag (file-based, for subprocess to signal failure to parent) ---

    @classmethod
    def _strip_ansi(cls, text: str) -> str:
        """Remove ANSI SGR escape sequences from a string.

        Failure-queue messages are stored with ANSI color/bold codes so they
        render correctly on the terminal. When persisting those same messages
        to the plain-text OVID log file the escape sequences are noise, so they
        are stripped here.

        Args:
            text: The (possibly ANSI-styled) string to clean.

        Returns:
            The input string with all ``\\x1b[...m`` escape sequences removed.
        """
        import re
        return re.sub(r'\x1b\[[0-9;]*m', '', text or '')

    @classmethod
    def _log_failure_messages(cls, failure_messages: list[tuple[str, str]]) -> None:
        """Persist drained failure-queue messages to the OVID log file.

        A subprocess (formatter/fixer/mapper) that catches its own exception and
        calls :meth:`print_failure_message` writes the real error text to the
        failure queue and then exits with code 0. Previously the timer thread
        drained and cleared that queue on the failure path without ever writing
        the text to the persistent log, so the concrete exception was lost (the
        "fails silently" symptom). This helper logs each drained message at ERROR
        level so the operational detail survives in the log file.

        Args:
            failure_messages: List of ``(message, text)`` tuples as returned by
                :meth:`_poll_failure_queue`.

        Returns:
            None.
        """
        if not failure_messages:
            return
        try:
            _ovid_logger = _get_ovid_logger()
        except Exception:
            return
        for message, text in failure_messages:
            clean_message = cls._strip_ansi(message).strip()
            clean_text = cls._strip_ansi(text).strip()
            if clean_text:
                _ovid_logger.error(f"SUBPROCESS FAILURE: {clean_message} -- {clean_text}")
            elif clean_message:
                _ovid_logger.error(f"SUBPROCESS FAILURE: {clean_message}")

    @classmethod
    def _get_failure_flag_path(cls):
        """Get path for failure flag file."""
        import os
        sync_dir = cls._get_sync_dir()
        return os.path.join(sync_dir, 'failure_flag')

    @classmethod
    def _write_failure_flag(cls):
        """Write failure flag file to signal parent process that failure message was printed."""
        try:
            flag_path = cls._get_failure_flag_path()
            with open(flag_path, 'w') as f:
                f.write('1')
        except:
            pass

    @classmethod
    def _check_failure_flag(cls):
        """Check if failure flag file exists (subprocess printed failure message)."""
        import os
        try:
            flag_path = cls._get_failure_flag_path()
            return os.path.exists(flag_path)
        except:
            return False

    @classmethod
    def _clear_failure_flag(cls) -> None:
        """Remove the failure flag file if present.

        Returns:
            None.
        """
        import os
        try:
            flag_path = cls._get_failure_flag_path()
            if os.path.exists(flag_path):
                os.remove(flag_path)
        except OSError:
            _get_ovid_logger().error(
                "Failed to clear failure flag file", exc_info=True
            )

    @classmethod
    def clear_stale_sync_files(cls) -> None:
        """Remove stale failure-queue files and the failure flag from previous runs.

        Called at pipeline startup to prevent old failure messages from being
        replayed into the current run's live display. Because the sync directory
        is shared across runs (and its queue files may be owned by the in-container
        UID or root), any queue file that cannot be removed is defensively
        truncated to empty as a fallback so it can never contribute a displayable
        line to ``_poll_failure_queue``.

        Returns:
            None.
        """
        import os
        import glob as glob_module

        cls._clear_failure_flag()
        cls._clear_failure_queue()

        # Defensive fallback: neutralize any queue file that survived removal
        # (e.g. owned by a different UID). An empty *.txt yields no displayable
        # failure line, so a leftover file can no longer leak into the display.
        for queue_dir in (cls._get_failure_queue_dir(), cls._get_fixer_queue_dir()):
            for leftover in glob_module.glob(os.path.join(queue_dir, '*.txt')):
                try:
                    with open(leftover, 'w') as f:
                        f.truncate(0)
                except OSError:
                    _get_ovid_logger().debug(
                        f"Could not neutralize stale sync file: {leftover}",
                        exc_info=True,
                    )
        cls._failure_lines_consumed = 0
        cls._fixer_lines_consumed = 0

    # --- Failure message queue (file-based, for subprocess to send failure messages to parent) ---

    @classmethod
    def _get_failure_queue_dir(cls):
        """Get the failure message queue directory path, creating it if needed."""
        import os
        sync_dir = cls._get_sync_dir()
        queue_dir = os.path.join(sync_dir, 'failure_queue')
        os.makedirs(queue_dir, exist_ok=True)
        return queue_dir

    @classmethod
    def _clear_failure_queue(cls) -> None:
        """Remove all files in the failure message queue and reset the consumed cursor.

        Returns:
            None.
        """
        import os
        import glob as glob_module
        try:
            queue_dir = cls._get_failure_queue_dir()
            for f in glob_module.glob(os.path.join(queue_dir, '*.txt')):
                os.remove(f)
        except OSError:
            _get_ovid_logger().error(
                "Failed to clear failure queue directory", exc_info=True
            )
        cls._failure_lines_consumed = 0

    @classmethod
    def _write_to_failure_queue(cls, styled_message: str, styled_text: str) -> None:
        """Write a failure message to the failure queue (called from a subprocess).

        Writes two lines to a zero-padded numbered file: the styled message and
        the styled detail text. An exclusive file lock ensures unique sequence
        numbers across concurrent subprocesses.

        Args:
            styled_message: The ANSI-styled failure header line.
            styled_text: The ANSI-styled failure detail line (may be empty).

        Returns:
            None.
        """
        import os
        import fcntl
        queue_dir = cls._get_failure_queue_dir()
        lock_path = os.path.join(cls._get_sync_dir(), 'failure_queue.lock')

        lock_fd = None
        try:
            lock_fd = open(lock_path, 'a+')
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)

            existing = [f for f in os.listdir(queue_dir) if f.endswith('.txt')]
            seq = len(existing)

            line_path = os.path.join(queue_dir, f'{seq:06d}.txt')
            with open(line_path, 'w') as f:
                f.write(styled_message + '\n' + styled_text)
        except OSError:
            _get_ovid_logger().error(
                "Failed to write to failure queue", exc_info=True
            )
        finally:
            if lock_fd:
                try:
                    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
                    lock_fd.close()
                except OSError:
                    _get_ovid_logger().error(
                        "Failed to release failure queue lock", exc_info=True
                    )

    @classmethod
    def _poll_failure_queue(cls, advance_cursor: bool = True) -> list[tuple[str, str]]:
        """Poll the failure queue for messages not yet consumed by this run.

        Mirrors ``_poll_fixer_queue``: files are read in sorted order and a
        consumed cursor (``_failure_lines_consumed``) is advanced so an
        already-displayed failure is never re-emitted within a run. This makes
        correctness independent of whether the queue directory has been cleared,
        preventing the stale cross-run replay that produced mixed-date frames.

        Args:
            advance_cursor: When ``True`` (default) the consumed cursor is
                advanced so callers on the display path do not re-render the
                same failure. Pass ``False`` for read-only consumers (e.g.
                logging in ``run_subprocess``) that must not starve the
                subsequent display poll in ``resume_timer_after_subprocess``.

        Returns:
            A list of ``(message, text)`` tuples for newly seen failure files.
        """
        import os
        failures: list[tuple[str, str]] = []
        try:
            queue_dir = cls._get_failure_queue_dir()
            files = sorted([f for f in os.listdir(queue_dir) if f.endswith('.txt')])

            # Only read files we haven't consumed yet
            for f in files[cls._failure_lines_consumed:]:
                path = os.path.join(queue_dir, f)
                try:
                    with open(path, 'r') as fh:
                        content = fh.read()
                        if not content:
                            # Neutralized/empty leftover: skip, no displayable line
                            continue
                        parts = content.split('\n', 1)
                        if len(parts) == 2:
                            failures.append((parts[0], parts[1]))
                        elif len(parts) == 1:
                            failures.append((parts[0], ''))
                except OSError:
                    _get_ovid_logger().error(
                        f"Failed to read failure queue file: {path}", exc_info=True
                    )

            if advance_cursor:
                cls._failure_lines_consumed = len(files)
        except OSError:
            _get_ovid_logger().error(
                "Failed to poll failure queue directory", exc_info=True
            )
        return failures

    # --- Timer thread ---

    @classmethod
    def _start_timer_thread(cls):
        """Start a background thread that updates the timer display."""
        import threading
        
        cls._timer_running = True
        cls._timer_thread = threading.Thread(target=cls._timer_loop, daemon=True)
        cls._timer_thread.start()

    @classmethod
    def _stop_timer_thread(cls):
        """Stop the timer thread. Does NOT finalize display - caller handles that."""
        if hasattr(cls, '_timer_running') and cls._timer_running:
            cls._timer_running = False
            if hasattr(cls, '_timer_thread') and cls._timer_thread:
                cls._timer_thread.join(timeout=1)

    @classmethod
    def _timer_loop(cls):
        """Background loop that updates the timer display every 100ms."""
        import time as time_module
        
        while cls._timer_running:
            cls._update_timer_display()
            time_module.sleep(0.1)

    @classmethod
    def _update_timer_display(cls):
        """Update the display buffer with current times and re-render.
        
        This method:
        1. Calculates current job time and total elapsed time
        2. Polls the fixer queue for new lines (if subprocess is running)
        3. Polls the failure queue for new messages (if subprocess is running)
        4. Rebuilds the display buffer with all current data
        5. Calls _render() to update the terminal
        """
        if not cls._timer_running:
            return

        # Check if failure occurred - stop updating if so
        if cls._subprocess_running and cls._check_failure_flag():
            cls._timer_running = False
            # Drain and clear the failure queue on the failure path so leftover
            # files never survive to the next run sharing this sync dir (the
            # stale cross-run replay that produced mixed-date frames). The parent
            # failure path (print_failure_message) renders directly into
            # _display_lines, so consuming here does not drop any display.
            #
            # A subprocess-originated failure, however, ONLY exists in the queue
            # (the formatter caught its own exception, wrote the real error text
            # to the queue, and exited 0). Draining without logging discarded
            # that text -- the "fails silently" symptom. Log the drained messages
            # to the persistent OVID log before clearing so the concrete error is
            # never lost.
            #
            # Logging alone is not enough: this timer-thread branch fires (~10x/s)
            # before run_formatter's post-subprocess resume_timer_after_subprocess()
            # gets a chance, so it consumes and clears the queue first. Previously
            # it returned without inserting the messages into _display_lines or
            # calling _render(), so the styled error text reached the log but NEVER
            # the terminal ("errors don't display"). We now render the drained
            # failure messages into the live display here -- mirroring the normal
            # path (below) and resume_timer_after_subprocess() -- before clearing.
            try:
                drained_failures = cls._poll_failure_queue()
                cls._log_failure_messages(drained_failures)
                if drained_failures:
                    try:
                        job_time_str, total_time_str = cls._build_time_strings()
                    except Exception:
                        job_time_str, total_time_str = "00:00.0", "00:00.0"
                    # Ensure the display buffer exists so we have somewhere to
                    # insert the failure lines (it normally does, but rebuild
                    # defensively to match resume_timer_after_subprocess()).
                    existing_fixer_lines = cls._get_current_fixer_lines()
                    cls._rebuild_display_lines(
                        job_time_str,
                        total_time_str,
                        existing_fixer_lines if existing_fixer_lines else None,
                    )
                    for msg, text in drained_failures:
                        # The blank separator is second-to-last, total elapsed is
                        # last; insert the failure lines just before the separator.
                        separator_idx = len(cls._display_lines) - 2
                        if separator_idx < 1:
                            separator_idx = 1
                        cls._display_lines.insert(separator_idx, msg)
                        if text:
                            cls._display_lines.insert(separator_idx + 1, text)
                    cls._render()
                cls._clear_failure_queue()
            except OSError:
                _get_ovid_logger().error(
                    "Failed to drain failure queue on failure path", exc_info=True
                )
            return

        # Calculate current times
        try:
            job_time_str, total_time_str = cls._build_time_strings()
        except:
            return

        # If subprocess is running, poll for new fixer lines and failure messages
        if cls._subprocess_running:
            new_fixer_lines = cls._poll_fixer_queue()
            failure_messages = cls._poll_failure_queue()
        else:
            new_fixer_lines = []
            failure_messages = []

        # Get existing fixer lines from current buffer
        existing_fixer_lines = cls._get_current_fixer_lines()
        
        # Add any new fixer lines
        all_fixer_lines = existing_fixer_lines + new_fixer_lines

        # Rebuild the display buffer with updated times and all fixer lines
        cls._rebuild_display_lines(job_time_str, total_time_str, all_fixer_lines if all_fixer_lines else None)

        # If there are failure messages, add them to the display buffer
        # (insert before the blank separator line)
        if failure_messages:
            # Persist to the log as well as the display so the error text
            # survives after the transient queue is cleared.
            cls._log_failure_messages(failure_messages)
            for msg, text in failure_messages:
                # The blank separator is second-to-last, total elapsed is last
                separator_idx = len(cls._display_lines) - 2
                cls._display_lines.insert(separator_idx, msg)
                if text:
                    cls._display_lines.insert(separator_idx + 1, text)
            # Clear the failure queue after consuming
            cls._clear_failure_queue()

        # Render to terminal (atomic operation under lock)
        cls._render()

    # --- Public API methods (same signatures as before) ---

    @classmethod
    def print_run_status(cls, job_num, total_jobs_count, job, folder, partner, subprocess_mode=False):
        """Print job status and start the live timer display.
        
        This is the main entry point called by pipelines for each job step.
        It finalizes the previous job's display, then starts a new one.
        """
        # Log the job status to the OVID log file
        try:
            _ovid_logger = _get_ovid_logger()
            _ovid_logger.info(f"Job {job_num}/{total_jobs_count} -- Partner: {partner} -- Folder: {folder} -- Running: {job}")
        except Exception:
            pass  # Don't let logging failures break output

        # Stop any existing timer thread
        cls._stop_timer_thread()
        
        # Finalize previous job's display (prints job line as permanent output, clears ephemeral lines)
        if cls._display_lines:
            cls._finalize_for_next_job()

        # Clear failure flag from previous job
        cls._clear_failure_flag()
        
        # Set subprocess mode
        cls._subprocess_running = subprocess_mode
        cls._job_finalized = False

        current_time = datetime.utcnow()
        
        # Initialize overall start time on first job
        if cls._overall_start_time is None:
            cls._overall_start_time = current_time
        
        # Record new job start time
        cls._current_job_start = current_time

        # Build the job message string
        current_utc_datetime = datetime.utcnow()
        new_york_tz = pytz.timezone('America/New_York')
        current_ny_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(new_york_tz)
        formatted_ny_datetime = current_ny_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        message = formatted_ny_datetime
        while len(message) < 25:
            message = message + ' '

        message = message + 'Job: '+str(job_num)+'/'+str(total_jobs_count)
        while len(message) < 39:
            message = message + ' '

        message = message + "--Partner: " + partner
        while len(message) < 55:
            message = message + ' '

        message = message + "--Folder: " + folder
        while len(message) < 80:
            message = message + ' '

        message = message+' --Running: '+job 
        while len(message) < 125:
            message = message + ' '

        while len(message) < 145:
            message = message + '.'

        # Store the message for timer updates
        cls._current_job_message = message
        
        # Build initial display buffer and render immediately
        job_time_str, total_time_str = cls._build_time_strings()
        cls._rebuild_display_lines(job_time_str, total_time_str)
        cls._render()

        # Start the timer thread to keep updating the display
        cls._start_timer_thread()

    @classmethod
    def pause_timer_for_subprocess(cls, work_dir=None):
        """Prepare for subprocess execution. Sets up the file-based queue for fixer lines.
        
        Called by pipelines before running a subprocess (formatter/fixer/mapper/dc_check).
        The timer continues running in the parent and polls the fixer queue.
        """
        import os
        
        # Set work directory for sync files
        if work_dir:
            os.environ['OVID_WORK_DIR'] = work_dir
        
        # Clear any stale fixer queue files and reset counter
        cls._clear_fixer_queue()
        cls._clear_failure_queue()
        
        # Ensure subprocess mode is set
        cls._subprocess_running = True

    @classmethod
    def resume_timer_after_subprocess(cls):
        """Resume after subprocess completes. Picks up any remaining fixer/failure queue items."""
        
        # Final poll to pick up any remaining items the timer thread may not have seen
        new_fixer_lines = cls._poll_fixer_queue()
        failure_messages = cls._poll_failure_queue()
        
        if new_fixer_lines or failure_messages:
            try:
                job_time_str, total_time_str = cls._build_time_strings()
            except:
                job_time_str, total_time_str = "00:00.0", "00:00.0"
            
            existing_fixer_lines = cls._get_current_fixer_lines()
            all_fixer_lines = existing_fixer_lines + new_fixer_lines
            cls._rebuild_display_lines(job_time_str, total_time_str, all_fixer_lines if all_fixer_lines else None)
            
            if failure_messages:
                for msg, text in failure_messages:
                    separator_idx = len(cls._display_lines) - 2
                    cls._display_lines.insert(separator_idx, msg)
                    if text:
                        cls._display_lines.insert(separator_idx + 1, text)
                cls._clear_failure_queue()
            
            cls._render()
        
        cls._subprocess_running = False
        
        # Clean up queue directories
        try:
            cls._clear_fixer_queue()
            cls._clear_failure_queue()
        except:
            pass

    @classmethod
    def prepare_for_subprocess_output(cls):
        """Public method: Prepare display for subprocess output.
        Call this from the parent process before running a subprocess that will print fix lines."""
        cls.pause_timer_for_subprocess()

    @classmethod
    def resume_after_subprocess_output(cls):
        """Public method: Resume display after subprocess output.
        Call this from the parent process after a subprocess that printed fix lines completes."""
        cls.resume_timer_after_subprocess()

    @classmethod
    def _print_job_line(cls, prev_job_time_str=None):
        """Legacy method for compatibility."""
        pass

    @classmethod
    def _print_job_with_timer(cls):
        """Legacy method - redirects to _print_job_line."""
        cls._print_job_line()

    @classmethod  
    def update_job_timer(cls):
        """Update the running timer display. Call this periodically during job execution."""
        pass  # Now handled by background thread

    @classmethod  
    def finish_job_timer(cls):
        """Finalize the job timer when job completes. Prints final time and moves to next line."""
        cls._stop_timer_thread()
        if cls._display_lines:
            cls._finalize_for_next_job()

    @classmethod
    def _get_job_times(cls):
        """Get job duration and total elapsed time strings."""
        return cls._build_time_strings()

    @classmethod
    def reset_job_timer(cls):
        """Reset the job timer (call at start of new ETL run)."""
        cls._stop_timer_thread()
        if cls._display_lines:
            cls._finalize_for_next_job()
        cls._overall_start_time = None
        cls._current_job_start = None
        cls._current_job_message = ""
        cls._timer_running = False
        cls._timer_thread = None
        cls._subprocess_running = False
        cls._display_lines = []
        cls._rendered_line_count = 0
        cls._fixer_lines_consumed = 0
        cls._job_finalized = False

    @classmethod
    def run_subprocess(cls, command, env=None, shell=False):
        """Run a subprocess with stdout/stderr captured to the OVID log file.
        
        Subprocess output is NOT echoed to the terminal to avoid corrupting the
        line-buffer display. Instead, output is written to the OVID log file.
        Fixer status lines and failure messages are communicated via file queues.
        
        If the subprocess exits with a non-zero return code and did NOT write
        a failure message to the queue itself, this method writes the stderr
        output as a failure message so it appears in the display.
        
        Args:
            command: Command list or string to execute.
            env: Optional environment dict for the subprocess.
            shell: Whether to run with shell=True (default False).
            
        Returns:
            subprocess.CompletedProcess result
        """
        import subprocess
        
        try:
            _ovid_logger = _get_ovid_logger()
        except Exception:
            _ovid_logger = None

        # Strip leaked Spark gateway env vars before launching the child.
        # The parent ETL/analysis/dq_checks driver runs under spark-submit,
        # which exports PYSPARK_GATEWAY_PORT / PYSPARK_GATEWAY_SECRET /
        # PYSPARK_PIN_THREAD pointing at the parent JVM's py4j gateway.
        # When env=os.environ.copy() is passed to subprocess.run() those
        # vars are inherited by every formatter/fixer/mapper subprocess,
        # which then tries to attach to the parent gateway via
        # SparkSession.builder.getOrCreate() instead of starting its own
        # JVM. That works silently while the parent gateway is healthy,
        # but the moment ANY formatter raises before reaching spark.stop()
        # the gateway state becomes unrecoverable and every subsequent
        # subprocess fails in <1 second with "Only one SparkContext should
        # be running in this JVM (see SPARK-2243)" -- the cascade observed
        # in the EMY 20260610 batch (jobs 22, 23, 24 collapsed within
        # 4 seconds after job 21's malformed-header crash). Scrubbing the
        # vars forces every subprocess to start a clean JVM gateway and
        # contains the blast radius of any single formatter crash.
        if env is not None:
            for _gw_var in (
                'PYSPARK_GATEWAY_PORT',
                'PYSPARK_GATEWAY_SECRET',
                'PYSPARK_PIN_THREAD',
            ):
                env.pop(_gw_var, None)

        result = subprocess.run(
            command,
            env=env,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Log captured output
        if _ovid_logger:
            if result.stdout and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    _ovid_logger.info(f"[subprocess stdout] {line}")
            if result.stderr and result.stderr.strip():
                for line in result.stderr.strip().split('\n'):
                    _ovid_logger.warning(f"[subprocess stderr] {line}")
        
        # If subprocess failed and didn't write its own failure message,
        # display the error in the progress display
        if result.returncode != 0 and not cls._check_failure_flag():
            error_text = (result.stderr or result.stdout or 'Unknown error').strip()
            # Truncate to first meaningful line for display
            error_lines = [l for l in error_text.split('\n') if l.strip()]
            display_error = error_lines[0] if error_lines else 'Subprocess failed'
            
            # Determine command name for the message
            if isinstance(command, list):
                cmd_name = command[-1] if command else 'unknown'
            else:
                # Shell command string - extract the script name
                cmd_name = str(command).split()[-1] if command else 'unknown'
            
            # Style as danger red
            bold_code = '\033[1m'
            color_code = '\033[0;31m'
            reset_code = '\033[0m'
            styled_message = bold_code + color_code + f"FAILED: {cmd_name}" + reset_code
            styled_text = bold_code + color_code + display_error + reset_code
            
            cls._write_failure_flag()
            
            if cls._subprocess_running:
                # In subprocess mode: write to failure queue for timer thread / resume to pick up
                cls._write_to_failure_queue(styled_message, styled_text)
            elif cls._display_lines:
                # In normal mode with active display: add directly to buffer and re-render
                separator_idx = len(cls._display_lines) - 2
                if separator_idx < 1:
                    separator_idx = 1
                cls._display_lines.insert(separator_idx, styled_message)
                cls._display_lines.insert(separator_idx + 1, styled_text)
                cls._render()
        elif cls._check_failure_flag():
            # The subprocess signalled failure itself (it caught its own
            # exception, called print_failure_message() -- which sets the flag
            # and enqueues the real error text -- and then exited, often with
            # code 0). run_subprocess therefore takes neither the success path
            # nor the branch above. Drain the queued failure text and persist it
            # to the log so the concrete error is recorded even though the child
            # returned "success". The messages are left in the queue for the
            # display/timer path to render; _log_failure_messages only reads.
            try:
                queued_failures = cls._poll_failure_queue(advance_cursor=False)
                cls._log_failure_messages(queued_failures)
            except OSError:
                if _ovid_logger:
                    _ovid_logger.error(
                        "Failed to read failure queue in run_subprocess", exc_info=True
                    )
            if _ovid_logger:
                _ovid_logger.error(
                    "Subprocess reported failure via failure flag "
                    f"(return code {result.returncode})"
                )

        return result

###################################################################################################################################
# This function will print a text with matrix green color
###################################################################################################################################


    @classmethod
    def print_with_style(cls, text, color, newline=True):

        if color == 'matrix green':

            bold_code = '\033[1m'
            color_code = '\033[92m'
            reset_code = '\033[0m'  # Reset to default style
       
        if color == 'danger red':

            bold_code = '\033[1m'
            color_code = RED='\033[0;31m'
            reset_code = '\033[0m'  # Reset to default style


        if color == 'warning orange':

            bold_code = '\033[1m'
            color_code = '\033[38;5;214m'  # Orange
            reset_code = '\033[0m'  # Reset to default style


        if color == 'blue':
            bold_code = '\033[1m'
            color_code = BLUE='\033[0;34m'
            reset_code = '\033[0m'  # Reset to default style

        if color == 'pomegranate':
            bold_code = '\033[1m'
            color_code =  POMEGRANATE='\033[0;91m'
            reset_code = '\033[0m'  # Reset to default style

        if color == 'dark pink':
            bold_code = '\033[1m'
            color_code =   DARK_PINK='\033[0;35m'
            reset_code = '\033[0m'  # Reset to default style
   
        # Print styled text (with or without newline)
        if newline:
            print(bold_code + color_code + text + reset_code, flush=True)
        else:
            print(bold_code + color_code + text + reset_code, end='', flush=True)
        # Also log the plain text (without ANSI codes) to the OVID log file
        try:
            _get_ovid_logger().info(text)
        except Exception:
            pass

###################################################################################################################################
# This function will print a text with matrix green color
###################################################################################################################################


    @classmethod
    def print_fixer_status(cls, current_count, total_count, fix_type, fix_name):
        """Print fixer status line. Called from subprocess - writes to file queue.
        
        The parent process's timer thread polls the queue and adds these lines
        to the display buffer. This avoids any cross-process terminal contention.
        """
        
        current_utc_datetime = datetime.utcnow()
        new_york_tz = pytz.timezone('America/New_York')

        current_ny_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(new_york_tz)

        formatted_ny_datetime = current_ny_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        message = formatted_ny_datetime

        while len(message) < 25:
            message = message + ' '

        message = message + 'Fix: '+str(current_count)+'/'+str(total_count)
        while len(message) < 39:
            message = message + ' '

        message = message + "--Fix type: " + fix_type + ' '

    
        while len(message) < 80:
            message = message + ' '


        message = message + "--Fix Name: " + fix_name + ' '


        while len(message) < 125:
            message = message + ' '

        while len(message) < 145:
            message = message + '.'

        # Apply dark pink styling (same as print_with_style 'dark pink')
        bold_code = '\033[1m'
        color_code = '\033[0;35m'
        reset_code = '\033[0m'
        styled_line = bold_code + color_code + message + reset_code

        # Write to file queue instead of printing to terminal
        # The parent process timer thread will pick this up and render it
        cls._write_to_fixer_queue(styled_line)

##################################################################################################################################
# 
###################################################################################################################################

    @staticmethod
    def get_date_from_date_str_with_default_value(  input_date):

            try:
                
                return  str(datetime.strptime(input_date, "%Y-%m-%d").date())
            except:
                return '1900-01-01'        

 
###################################################################################################################################
# T
###################################################################################################################################



   
    @staticmethod
    def get_datetime_from_date_str_with_default_value(  input_date):
        
        try:
            
            return str(datetime.strptime(input_date+ " 00:00:00","%Y-%m-%d %H:%M:%S"))
        except:
            return '1900-01-01 00:00:00'    
        

# ###################################################################################################################################
# # T
# ###################################################################################################################################



   
    @classmethod
    def display_logo(  cls):
        

        img = Image.open('common/ovid_logo.png')
        img.show() 



# ######################################################################################################################################
# This method is exclusively for CHP encounter types.  This function is used to determine the correct ENC_TYPE for the ENCOUNTER TABLE, 
# the DIAGNOSIS TABLE AND the PROCEDURES TABLE. 
# ######################################################################################################################################


    @classmethod
    def get_encounter_type(cls,form_code: Column,type_bill_cd: Column,revenue_cd: Column,hcpcs_cd: Column,
        place_of_service_cd: Column,mod1_cd: Column,mod2_cd: Column,mod3_cd: Column,mod4_cd: Column,) -> Column:
        
        def _ut(c: Column) -> Column:
            return F.upper(F.trim(F.coalesce(c, F.lit(""))))

        def _prefix_in(c: Column, prefixes) -> Column:
            pat = "^(?:" + "|".join(re.escape(p) for p in prefixes) + ")"
            return _ut(c).rlike(pat)

        def _in_set(c: Column, values) -> Column:
            return _ut(c).isin(*values)

        def _numeric_between(c: Column, lo: int, hi: int) -> Column:
            num = F.regexp_replace(F.coalesce(F.trim(c), F.lit("")), r"[^0-9]", "")
            is_num = num.rlike("^[0-9]+$")
            ci = num.cast("int")
            return is_num & (ci >= F.lit(lo)) & (ci <= F.lit(hi))

        def _any_mod_in(mods, vals) -> Column:
            checks = [_in_set(m, vals) for m in mods]
            return reduce(lambda a, b: a | b, checks, F.lit(False))

        fc_u = _in_set(form_code, ["U"])  # Institutional
        fc_h = _in_set(form_code, ["H"])  # Professional

        # Shared cues
        rev_is_ed = _prefix_in(revenue_cd, ["045", "0981"])
        rev_is_os = _in_set(revenue_cd, ["0760", "0761", "0762", "0769"])
        hcpcs_is_ed = _numeric_between(hcpcs_cd, 99281, 99285)
        hcpcs_is_os = _in_set(hcpcs_cd, ["G0244", "G0378", "99217", "99218", "99219", "99220", "99224", "99225", "99226"])

        # Telehealth cues (include G0)
        pos_is_tele = _in_set(place_of_service_cd, ["02", "10"]) | _any_mod_in(
            [mod1_cd, mod2_cd, mod3_cd, mod4_cd], ["GT", "GQ", "95", "G0"]
        )

        # Type of Bill groups (prefix)
        tb_ip = _prefix_in(type_bill_cd, ["11", "12", "41", "84", "011", "012", "041", "084"])
        tb_is = _prefix_in(
            type_bill_cd,
            ["15", "16", "18", "21", "22", "25", "26", "28", "31", "32", "35", "36", "38", "42", "81", "82", "85", "86",
            "015", "016", "018", "021", "022", "025", "026", "028", "031", "032", "035", "036", "038", "042", "081", "082", "085", "086"],
        )
        tb_av = _prefix_in(type_bill_cd, ["13", "14", "23", "24", "33", "34", "7", "83", "013", "014", "023", "024", "033", "034", "07", "083"])
        tb_nonblank = F.length(_ut(type_bill_cd)) > 0

        # Professional cues
        pos_is_ic = _in_set(place_of_service_cd, ["21", "23", "25"])
        hcpcs_is_ic_consult = _numeric_between(hcpcs_cd, 99251, 99255)

        # Institutional path (use your required precedence: IE/EI > IP > IS > ED > OS > AV > TH > OT)
        expr = (
            F.when(fc_u & tb_ip & (rev_is_ed | hcpcs_is_ed), F.lit("EI"))
            .when(fc_u & tb_ip, F.lit("IP"))
            .when(fc_u & tb_is, F.lit("IS"))
            .when(fc_u & (rev_is_ed | hcpcs_is_ed), F.lit("ED"))
            .when(fc_u & (rev_is_os | hcpcs_is_os), F.lit("OS"))
            .when(fc_u & tb_av, F.lit("AV"))
            .when(fc_u & pos_is_tele, F.lit("TH"))
            .when(fc_u & tb_nonblank, F.lit("OT"))
        )

        # Professional path (precedence: ED > IC > OS > TH > AV > OT)
        expr = (
            expr
            .when(fc_h & hcpcs_is_ed, F.lit("ED"))
            .when(fc_h & (pos_is_ic | hcpcs_is_ic_consult), F.lit("IC"))
            .when(fc_h & hcpcs_is_os, F.lit("OS"))
            .when(fc_h & pos_is_tele, F.lit("TH"))
            .when(fc_h & (_in_set(place_of_service_cd, ["03","05","07","09","11","12","13","14","15","16","17","18","19","20","22","33","49","50","71","72"])
                        | _in_set(hcpcs_cd, ["99202","99203","99204","99205","99211","99212","99213","99214","99215","99241","99242","99243","99244","99245",
                                            "99341","99342","99343","99344","99345","99347","99348","99349","99350","99381","99382","99383","99384","99385",
                                            "99386","99387","99391","99392","99393","99394","99395","99396","99397","99401","99402","99403","99404","99411",
                                            "99412","99429","99455","99456","99483","G0402","G0438","G0439","G0463","T1015"])),
                F.lit("AV"))
            .when(
                fc_h & _in_set(
                    place_of_service_cd,
                    ["", " ", "01", "1", "10", "12", "13", "14", "15", "16", "27", "28", "29", "30", "31", "32",
                    "34", "41", "42", "51", "52", "54", "55", "56", "61", "81", "99", "XX", "FI"],
                ),
                F.lit("OT"),
            )
            .when(fc_h, F.lit("AV"))  # final fallback for H
        )

        return expr.otherwise(F.lit("NI"))

