# Ind CQC Filled Post Estimate Pipeline

This pipeline manages the jobs and related pipelines which cover our cleaning and modelling processes. It is triggered at the end of the bulk download CQC API pipeline. 

## Inputs
This pipeline takes in six datasets, cleans them and joins them in preparation for modelling. 

| Description | s3 location | Athena name |
|:------------|:------------|:------------|
|ASCWDS workplace data|s3://sfc-[branch]-datasets/domain=ASCWDS/dataset=workplace/|dataset_workplace|
|ASCWDS worker data|s3://sfc-[branch]-datasets/domain=ASCWDS/dataset=worker/|dataset_worker|
|CQC locations data|s3://sfc-[branch]-datasets/domain=CQC/dataset=locations_api/|dataset_locations_api|
|CQC providers data|s3://sfc-[branch]-datasets/domain=CQC/dataset=providers_api/|dataset_providers_api|
|CQC PIR data|s3://sfc-[branch]-datasets/domain=CQC/dataset=pir/|dataset_pir|
|ONS postcode directory data|s3://sfc-[branch]-datasets/domain=ONS/dataset=postcode_directory/|dataset_postcode_directory|

## Bronze to silver transformation layer

The bronze to silver transformation layer of the pipeline cleans the individual datasets and creates a small number of additional datasets for internal use. The diagram below shows this workflow.

```{figure} ../../diagrams/ind_cqc_bronze_to_silver.png
:scale: 100 %
:alt: Bronze to Silver Transformation Layer Step Functions

**The Bronze to Silver Transformation Layer**
```

This layer contains the following jobs:
- {doc}`../ind_cqc_jobs/clean_ascwds_workplace_data`
- {doc}`../ind_cqc_jobs/clean_ascwds_worker_data`
- {doc}`../ind_cqc_jobs/reconciliation` (for internal use only)
- {doc}`../ind_cqc_jobs/clean_pir_data`
- {doc}`../ind_cqc_jobs/clean_ons_data`
- {doc}`../ind_cqc_jobs/clean_cqc_provider_data`
- {doc}`../ind_cqc_jobs/clean_cqc_locations_data`

After these jobs have been run, the {doc}`silver_validation_pipeline` runs.

### Silver datasets
This pipeline outputs a cleaned version of each of these datasets.

| Description | s3 location | Athena name |
|:------------|:------------|:------------|
|clean ASCWDS workplace data|s3://sfc-[branch]-datasets/domain=ASCWDS/dataset=workplace_cleaned/|dataset_workplace_cleaned|
|clean ASCWDS worker data|s3://sfc-[branch]-datasets/domain=ASCWDS/dataset=worker_cleaned/|dataset_worker_cleaned|
|clean CQC locations data|s3://sfc-[branch]-datasets/domain=CQC/dataset=locations_api_cleaned/|dataset_locations_api_cleaned|
|clean CQC providers data|s3://sfc-[branch]-datasets/domain=CQC/dataset=providers_api_cleaned/|dataset_providers_api_cleaned|
|clean CQC PIR data|s3://sfc-[branch]-datasets/domain=CQC/dataset=pir_cleaned/|dataset_pir_cleaned|
|clean ONS postcode directory data|s3://sfc-[branch]-datasets/domain=ONS/dataset=postcode_directory_cleaned/|dataset_postcode_directory_cleaned|
|ASCWDS workplace data cleaned for reconciliaiton - for internal use only|s3://sfc-[branch]-datasets/domain=SfC/dataset=sfc_workplace_for_reconciliation/|dataset_sfc_workplace_for_reconciliation|
|parents data - for internal use only|s3://sfc-[branch]-datasets/domain=SfC/dataset=sfc_reconciliation_parents/|dataset_sfc_reconciliation_parents|
|singles and subs - for internal use only|s3://sfc-[branch]-datasets/domain=SfC/dataset=sfc_reconciliation_singles_and_subs/|dataset_sfc_reconciliation_singles_and_subs|

## Silver to gold transformation layer

The silver to gold transformation layer of the pipeline merges and filters the raw data and then fills gaps using a range of models. It also triggers the {doc}`coverage_pipeline`, runs diagnostic and archiving functions, and triggers the Glue crawlers. The diagram below shows this workflow.

```{figure} ../../diagrams/ind_cqc_silver_to_gold.png
:scale: 80 %
:alt: Silver to Gold Transformation Layer Step Functions

**The Silver to Gold Transformation Layer**
```

This layer contains the following jobs:
- {doc}`../ind_cqc_jobs/merge_ind_cqc_data`
- {doc}`../ind_cqc_jobs/clean_ind_cqc_data`
- {doc}`../ind_cqc_jobs/estimate_missing_ascwds_ind_cqc_filled_posts`
- {doc}`../ind_cqc_jobs/prepare_features_care_home_ind_cqc`
- {doc}`../ind_cqc_jobs/prepare_features_non_res_ascwds_ind_cqc`
- {doc}`../ind_cqc_jobs/prepare_features_non_res_pir_ind_cqc`
- {doc}`../ind_cqc_jobs/estimate_ind_cqc_filled_posts`
- {doc}`../ind_cqc_jobs/estimate_ind_cqc_filled_posts_by_job_role`
- {doc}`../ind_cqc_jobs/diagnostics_on_known_filled_posts`
- {doc}`../ind_cqc_jobs/archive_filled_posts_estimates`

After the Diagnostics on known filled posts job has been run, the {doc}`gold_validation_pipeline` runs.

### Gold level datasets
It also outputs a dataset at every stage of estimation. The two datasets containing our final outputs are in bold below.

| Description | s3 location | Athena name |
|:------------|:------------|:------------|
|merged ind cqc data|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_merged_data/|dataset_ind_cqc_merged_data|
|cleaned ind cqc data|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/|dataset_ind_cqc_cleaned_data|
|imputed ind cqc data|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/|dataset_ind_cqc_estimated_missing_ascwds_filled_posts|
|features for the care home model|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_care_home/|dataset_ind_cqc_features_care_home|
|features for the non residential without dormancy model|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_without_dormancy/|dataset_ind_cqc_features_non_res_ascwds_without_dormancy|
|features for the non residential with dormancy model|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_inc_dormancy/|dataset_ind_cqc_features_non_res_ascwds_inc_dormancy|
|features for the PIR model|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_pir/|dataset_ind_cqc_features_non_res_pir|
|model metrics for all ML models|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_ml_model_metrics/|dataset_ind_cqc_ml_model_metrics|
|**filled posts estimates**|**s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/**|**dataset_ind_cqc_estimated_filled_posts**|
|**filled posts estimates split by job role**|**s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_by_job_role/**|**dataset_ind_cqc_estimated_filled_posts_by_job_role**|
|detailed diagnostics comparing filled posts estimates to known ASCWDS data|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_diagnostics/|dataset_ind_cqc_estimated_filled_posts_diagnostics|
|summary diagnostics comparing filled posts estimates to known ASCWDS data|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_diagnostics_summary/|dataset_ind_cqc_estimated_filled_posts_diagnostics_summary|
|archive dataset|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=archived_monthly_filled_posts/|dataset_archived_monthly_filled_posts|
