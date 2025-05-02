# Ingest

This folder contains all scripts related to the initial ingestion, cleaning, and validation of datasets used across all other projects.

## Data Sources

We currently ingest and clean data from the following sources:

- **Adult Social Care Workforce Data Set (ASCWDS)**
- **Capacity Tracker**
- **CQC Registered Locations**
- **CQC Provider Information Return (PIR)**
- **Direct Payment Recipients (DPR)**
- **Office for National Statistics Postcode Directory (ONS PD)**

## Processes in this module

- **Ingest raw data** from various formats and sources.
- **Clean and prepare datasets** by:
  - Filtering irrelevant data
  - Handling nulls
  - Standardising formats (e.g. date columns)
- **Run validations** using [PyDeequ](https://github.com/awslabs/deequ) to ensure data quality and schema consistency.

This is the foundation for the entire data pipeline.
