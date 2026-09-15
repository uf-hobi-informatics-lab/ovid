# common/omop_cdm/

Reference OMOP vocabulary tables used by Ovid features that need standard
concept lookups.

## Contents (gitignored)

| File          | Used by                                                           | Required? |
|---------------|-------------------------------------------------------------------|-----------|
| `CONCEPT.csv` | `analysis -j raw_distinct_values` for `_concept_id` enrichment    | Optional  |

The OMOP `CONCEPT.csv` is **gitignored** (see root `.gitignore` rule
`common/omop_cdm/*.csv`) because it is large (~2 GB) and ships separately
from the codebase.

## Where to get `CONCEPT.csv`

The official OMOP standardized vocabularies can be downloaded from
https://athena.ohdsi.org/. Place the unzipped `CONCEPT.csv` directly in this
directory:

```
common/omop_cdm/CONCEPT.csv
```

The file is expected to have these columns (standard Athena export, comma- or
tab-delimited; the loader auto-sniffs):

```
concept_id, concept_name, domain_id, vocabulary_id, standard_concept,
concept_code, valid_start_date, valid_end_date, invalid_reason, concept_class_id
```

Only `concept_id`, `concept_name`, `concept_code`, `vocabulary_id`, and
`domain_id` are read.

## Behavior when missing

If `CONCEPT.csv` is not present, `raw_distinct_values` runs without concept
enrichment: distinct counts are still produced for every column, but
`_concept_id` columns will not get `concept_name` / `concept_code` /
`vocabulary_id` / `domain_id` columns in the report, and `*_source_value`
columns will not get coverage analysis. A warning is logged.
