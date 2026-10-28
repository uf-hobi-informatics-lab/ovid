# common/lists/

Reference lookup tables used by Ovid mappers, fixers, and partner formatters.

## Contents

| File                                                         | Tracked in git | Used by                                                                                                                |
|--------------------------------------------------------------|----------------|-----------------------------------------------------------------------------------------------------------------------|
| `2025_01_23_PCORnet_Common_Data_Model_v7dot0_parseable.xlsx` | Yes            | `common/pcornet_cdm.py` (CDM v7.0 schema + valueset reader)                                                            |
| `RUCA-codes-2020-tract.xlsx`                                 | Yes            | RUCA-2020 tract-level lookup for `private_address_geocode_formatter.py`                                                |
| `RUCA-codes-2020-zipcode.xlsx`                               | Yes            | RUCA-2020 zipcode-level lookup for `private_address_geocode_formatter.py`                                              |
| `bad_obsgen_codes.csv`                                       | Yes            | OBSGEN code blacklist consumed by `bad_obsgen_codes_to_ot_fix` (sets `OBSGEN_TYPE='OT'` for matching rows)              |
| `lab_loinc_location.csv`                                     | Yes            | LAB_RESULT_CM specimen-source lookup                                                                                   |
| `loinc_list.csv`                                             | Yes            | LOINC reference list                                                                                                    |
| `panel_loinc_snapshot.csv`                                   | Yes            | Pinned panel-LOINC set consumed by `panel_loinc_remover_fix` (DC_1_20). Replaces the live Snowflake `LOINC_REF_RAW` query to avoid deleting individual-result LOINCs reclassified as panels between cycles. |
| `patterns_list.csv`                                          | Yes            | Pattern matching for fixers                                                                                            |
| `snomed_pii_list.csv`                                        | Yes            | SNOMED PII filter list                                                                                                 |
| `zipcode_for_pyspark.csv`                                    | Yes            | Zipcode lookup for PySpark formatters                                                                                  |
| **`FULL_GEOCODING_202603_16.csv.gz`**                        | **Yes (gzip, ~56 MB)** | **Required for address tables. See below.**                                                                  |

## `FULL_GEOCODING_202603_16.csv.gz` — geocoding reference

ZIP9 → Census geography lookup (state / county / tract / block-group FIPS,
ZCTA, RUCA-2020). It is used to populate `STATE_FIPS`, `COUNTY_FIPS` and
`RUCA_ZIP` in `LDS_ADDRESS_HISTORY` / `PRIVATE_ADDRESS_HISTORY` and the
`PRIVATE_ADDRESS_GEOCODE` table.

The uncompressed CSV is ~430 MB, which exceeds GitHub's 100 MB per-file
limit, so it is committed **gzip-compressed**:

```
common/lists/FULL_GEOCODING_202603_16.csv.gz
```

No manual step is required. `CommonFuncitons.get_geocoding_path()` resolves
the file and prefers an uncompressed `FULL_GEOCODING_202603_16.csv` when one
is present (faster Spark reads), otherwise the `.csv.gz`. Spark reads gzip
natively and `CommonFuncitons.open_geocoding_file()` handles it for the
pure-Python readers. Optionally decompress once for faster reads:

```
gunzip -k common/lists/FULL_GEOCODING_202603_16.csv.gz
```

(The uncompressed `.csv` is gitignored.)

### What depends on this file

- `common/commonFunctions.py` — `get_countyFips_from_zip()`, `get_zcta_from_zip()`
- `mapping_scripts/pcornet_mappers/lds_address_history_mapper.py`
- `mapping_scripts/pcornet_mappers/private_address_history_mapper.py`
- `private_address_geocode_formatter.py` in the partner templates / partner folders

### Behavior when missing

`get_geocoding_path()` raises `FileNotFoundError` naming both expected paths.
The file is required for any batch that produces `LDS_ADDRESS_HISTORY`,
`PRIVATE_ADDRESS_HISTORY`, or `PRIVATE_ADDRESS_GEOCODE` output.

### File format

Comma-separated, header row, one row per ZIP9 (plus ZIP5-only rows with an
empty `ZIP9`):

```
ZIP9,X,Y,ZCTA,GEOCODE_STATE,GEOCODE_COUNTY,GEOCODE_TRACT,GEOCODE_BLOCK,GEOCODE_GROUP,GEOID20,
GEOCODE_TRACT_11dig,GEOCODE_BLOCK_15dig,GEOCODE_GROUP_1dig,GEOCODE_GROUP_7dig,TRACT_GEOID,RUCA_2020,ZIP5
```

The `GEO_PROV_REF` value emitted by the geocode formatters is
`"FULL_GEOCODING_202603/RUCA-2020"`.
