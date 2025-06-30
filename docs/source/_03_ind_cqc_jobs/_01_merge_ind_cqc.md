# Merge Independent CQC Data

This job filters the cleaned CQC location dataset to independent sector locations only and then merges cleaned datasets from various other datasets (ASC-WDS, CQC PIR, Capacity Tracker and ONS) into a single, enriched view of **independent sector CQC locations**.

---

## Overview

**Script location**:
`projects/_03_independent_cqc/_01_merge/jobs/merge_ind_cqc_data.py`

**Trigger**:
This job is typically run as part of the {doc}`../_03_ind_cqc_pipelines/ind_cqc_filled_post_estimates` or manually for development purposes.

**Output**:
A Parquet dataset of merged independent sector CQC locations, partitioned by year, month, day, and import date.

---

## Input Parameters

| Parameter | Description | Athena name | Job documentation |
|---|---|---|---|
| `cleaned_cqc_location_source` | S3 path to cleaned CQC location dataset | `dataset_locations_api_cleaned` | TODO |
| `cleaned_cqc_pir_source` | S3 path to cleaned CQC PIR dataset | `dataset_pir_cleaned` | TODO |
| `cleaned_ascwds_workplace_source` | S3 path to cleaned ASC-WDS workplace dataset | `dataset_workplace_cleaned` | TODO |
| `cleaned_ct_non_res_source` | S3 path to cleaned Capacity Tracker non-res dataset | `dataset_capacity_tracker_non_residential_cleaned` | TODO |
| `cleaned_ct_care_home_source` | S3 path to cleaned Capacity Tracker care home dataset | `dataset_capacity_tracker_care_home_cleaned` | TODO |
| `destination`                   | S3 output path for the merged dataset | `dataset_ind_cqc_merged_data` | |

---

## How It Works

1. **Initialisation**
   Spark is configured, and input paths are parsed using `utils.collect_arguments`.

2. **Read Input Data**
   Only the necessary columns are read from:
   - Cleaned CQC location data
   - Cleaned ASC-WDS workplace data
   - Cleaned CQC PIR data
   - Cleaned Capacity Tracker (care home and non-residential)

3. **Filter**
   CQC data is filtered to include only independent sector locations.

4. **Join**
   The other datasets are joined onto the CQC locations using:
   - `location_id`
   - aligned `import_date` (using `cleaning_utils.add_aligned_date_column`)
   - optional `care_home` flag (when applicable)

5. **Write Output**
   The merged dataset is written to the specified S3 destination in Parquet format, partitioned by `year`, `month`, `day`, and `import_date`.


**Data Flow**

```{mermaid}
graph TD
    A[Cleaned CQC Locations]:::format --> B[Filter To Independent Sector]:::format
    B --> J[Join Data Into CQC DF]:::format
    C[Cleaned ASCWDS Workplace]:::format --> J
    D[Cleaned CQC PIR]:::format --> J
    E[Cleaned CT Non-Res]:::format --> J
    F[Cleaned CT Care Home]:::format --> J
    J --> W[Write to Parquet]:::format

    classDef format fill:#F5FAFD,stroke:#005EB8,stroke-width:2px;
```

---

## Functions

### Functions in job

```{eval-rst}
.. automodule:: projects._03_independent_cqc._01_merge.jobs.merge_ind_cqc_data
   :members:
   :undoc-members:
   :exclude-members: DataFrame
   :show-inheritance:
   :member-order: bysource
```

---

## Functions from other scripts

```{eval-rst}
.. autofunction:: utils.utils.read_from_parquet
.. autofunction:: utils.utils.select_rows_with_value
.. autofunction:: utils.cleaning_utils.add_aligned_date_column
.. autofunction:: utils.utils.write_to_parquet
.. autofunction:: utils.utils.collect_arguments
```
