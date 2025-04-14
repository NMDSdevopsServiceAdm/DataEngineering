# Independent CQC Registered Estimates Pipelines

We have a suite of pipelines which ingest, clean and model our independent CQC registered estimates. This page gives a high level summary of these pipelines and links out to more detailed descriptions of each pipeline on other pages.

## Bronze level data

### Ingest ASCWDS
The {doc}`data_ingestion_pipelines/ingest_ascwds` pipeline is triggered whenever new files are placed in the raw ASCWDS data bucket. This happens automatically four times per month as new ASCWDS data files are produced.

### Ingest CQC PIR
The {doc}`data_ingestion_pipelines/ingest_cqc_pir` pipeline is triggered whenever new files are placed in the raw CQC PIR data bucket. This happens manually once per month after the CQC send us the monthly excel file.

### Bulk download CQC API pipeline
The {doc}`data_ingestion_pipelines/bulk_download_cqc_api_pipeline` is triggered four times per month, on the first, eighth, fifteenth and twenty third of each month. It calls the CQC APIs and pulls in a complete dataset of all providers and locations at that point in time.

#### Bronze validation pipeline
The {doc}`data_ingestion_pipelines/bronze_validation_pipeline` is called within the {doc}`data_ingestion_pipelines/bulk_download_cqc_api_pipeline`. It runs basic data quality checks on each of our raw datasets and reports back on any changes.

## Silver and Gold level data

### Ind CQC Filled Post Estimate Pipeline
The {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline` manages the jobs and related pipelines which cover our cleaning and modelling processes. It is triggered at the end of the {doc}`data_ingestion_pipelines/bulk_download_cqc_api_pipeline`. 

#### Silver Validation Pipeline
The {doc}`ind_cqc_pipelines/silver_validation_pipeline` is triggered as part of the {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline`. It runs data quality checks on each of our cleaned datasets and reports back on any changes.

#### Coverage Pipeline
The {doc}`ind_cqc_pipelines/coverage_pipeline` controls the steps for producing internal datasets from the cleaned data. It does not contain any estimates. It is triggered as part of the {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline`.

#### Gold Validation Pipeline
The {doc}`ind_cqc_pipelines/gold_validation_pipeline` is triggered as part of the {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline`. It runs data quality checks on each of our merged, filtered and modelled datasets and reports back on any changes.

### Ingest and clean capacity tracker data pipeline
The {doc}`ind_cqc_pipelines/ingest_and_clean_capacity_tracker_data_pipeline` manages the ingestion and cleaning of capcity tracker data and uses this to run additional diagnostics on the independent cqc filled posts estimates.

#### Capacity tracker silver validation pipeline
The {doc}`ind_cqc_pipelines/capacity_tracker_silver_validation_pipeline` is called within the {doc}`ind_cqc_pipelines/ingest_and_clean_capacity_tracker_data_pipeline`. It runs basic data quality checks on each of our raw datasets and reports back on any changes.