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
| `patterns_list.csv`                                          | Yes            | Pattern matching for fixers                                                                                            |
| `snomed_pii_list.csv`                                        | Yes            | SNOMED PII filter list                                                                                                 |
| `zipcode_for_pyspark.csv`                                    | Yes            | Zipcode lookup for PySpark formatters                                                                                  |
| **`FULL_GEOCODING_202603_16.csv`**                           | **No (gitignored, ~430 MB)** | **Required at install time. See below.**                                                                |

## `FULL_GEOCODING_202603_16.csv` — required external file

The geocoding lookup table is **gitignored** (see root `.gitignore` rule
`common/lists/FULL_GEOCODING_*.csv`) because it is large (~430 MB) and ships
separately from the codebase. It must be downloaded and placed at this path
**before running any ETL job that uploads address tables**:

```
common/lists/FULL_GEOCODING_202603_16.csv
```

### Where to obtain

The geocoding reference file is distributed separately from this repository
because of its size and licensing. Obtain it from your organization's
geocoding data source and place it at the path shown above. Contact the
project maintainers (see the root `README.md`) if you need guidance on
sourcing an equivalent geocoding reference table.

### What depends on this file

The path `/app/common/lists/FULL_GEOCODING_202603_16.csv` is hardcoded in
22 active modules across the codebase:

- `common/commonFunctions.py` (two read sites in the address-history /
  geocode upload path).
- `mapping_scripts/pcornet_mappers/lds_address_history_mapper.py`
- `mapping_scripts/pcornet_mappers/private_address_history_mapper.py`
- Every partner's `private_address_geocode_formatter.py`.

### Behavior when missing

If the file is absent at runtime, the mappers and formatters above will
fail when Spark tries to read the path. Errors will surface as
`FileNotFoundException` / `Path does not exist` exceptions in the OVID log
file. There is no graceful degradation — the file is required for any
batch that produces `LDS_ADDRESS_HISTORY`, `PRIVATE_ADDRESS_HISTORY`, or
`PRIVATE_ADDRESS_GEOCODE` output.

### File format

CSV with header row. Approximate schema (geocoded address records keyed
by address line + zipcode, joined into the address pipeline by
`commonFunctions.py` and the per-partner geocode formatters). The
`GEO_PROV_REF` value emitted by the geocode formatters is
`"FULL_GEOCODING_202603/RUCA-2020"`, indicating the data provenance.
