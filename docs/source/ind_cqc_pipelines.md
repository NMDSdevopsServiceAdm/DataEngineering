# Independent CQC Registered Estimates Pipelines

We have a suite of pipelines which ingest, clean and model our independent CQC registered estimates. This page gives a high level summary of these pipelines and links out to more detailed descriptions of each pipeline on other pages.

## Bronze level data

### Ingest ASCWDS
This pipeline is triggered whenever new files are placed in the raw ASCWDS data bucket. This happens automatically four times per month as new ASCWDS data files are produced.

### Ingest CQC PIR
This pipeline is triggered whenever new files are placed in the raw CQC PIR data bucket. This happens manually once per month after the CQC send us the monthly excel file.

### Bulk download CQC API pipeline
This pipeline is triggered four times per month, on the first, eighth, fifteenth and twenty third of each month. It calls the CQC APIs and pulls in a complete dataset of all providers and locations at that point in time.

#### Bronze validation pipeline
This pipeline is called within the Bulk download CQC API pipeline. It runs basic data quality checks on each of our raw datasets and reports back on any changes.

## Silver and Gold level data

### Ind CQC Filled Post Estimate Pipeline
The {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline` manages the jobs and related pipelines which cover our cleaning and modelling processes. It is triggered at the end of the bulk download CQC API pipeline. 

#### Silver Validation Pipeline
This pipeline is triggered as part of the {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline`. It runs data quality checks on each of our cleaned datasets and reports back on any changes.

#### Coverage Pipeline
This pipeline controls the steps for producing internal datasets from the cleaned data. It does not contain any estimates. It is triggered as part of the {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline`.

#### Gold Validation Pipeline
This pipeline is triggered as part of the {doc}`ind_cqc_pipelines/ind_cqc_filled_post_estimate_pipeline`. It runs data quality checks on each of our merged, filtered and modelled datasets and reports back on any changes.

### Ingest and clean capacity tracker data pipeline
This pipeline manages the ingestion and cleaning of capcity tracker data and uses this to run additional diagnostics on the independent cqc filled posts estimates.

#### Capacity tracker silver validation pipeline
This pipeline is called within the Ingest and clean capacity tracker data pipeline. It runs basic data quality checks on each of our raw datasets and reports back on any changes.