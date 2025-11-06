# Changelog

All notable changes to this project will be documented in this file.


## [Unreleased]

### Added
- Added provider name into the merged dataframe within the CQC Coverage job.

- New function added to merge the old CQC ratings and the new assessment ratings.

- Polars version of the estimates by job role job and added job to new step function for ind cqc estimates.

- Implemented complex validation for [validate_delta_locations_api_cleaned](projects/_01_ingest/cqc_api/fargate/validate_delta_locations_api_cleaned.py), includes:
  - split into dimensions table with separate validation
  - Pointblank translation of helper functions and new expressions.

- Function to clean Capacity Tracker care home data by nulling when posts to beds ratio is outside thresholds.

- Model retraining process using Polars and scikit-learn in Fargate

- Tagging added by default to all Terraform elements

- Plan of action for new CQC Ingestion process.

- Added validation script for flatten CQC location process and test script for this validation.

- Added Postcode Matching call within cqc_location_4_full_clean job.

### Changed
- Migrated Polars validation scripts over to use PointBlank (compatible with >= Python 3.11), so far:
  - locations_raw

- Updated glue script and step function parameters for flatten_cqc_ratings job with CQC_delta datasets.

- Updated reconciliation job parameters in glue script to be consistent with SFC-Internal step function.

- Updated CQC Locations Cleaning to work with delta data

- Created dimensions for imputed values in CQC Location Cleaning, and separated this from the main fact table
  - The dimensions are rejoined to the fact table in the downstream steps where they are needed

- Removed unused columns from CQC Providers data

- Formatted imports throughout codebase

- Added CQC assessments into both the ratings for data requests and benchmarks datasets.

- Removed the original Step Functions now the replacement ones are fully operational.

- Removed usage of the raw location schema with hardcoded column names and updated all dependent jobs to reference the standardised polars schema with column name references.

- Added a third argument calculate_filled_posts_per_bed_ratio so it can be used for either ASC-WDS posts or Capacity Tracker posts.

- Removed the deduplication of Capacity Tracker data and used the cleaned Capacity Tracker care home data for imputation.

- Moved the calculation for estimating all Capacity Tracker non-residential filled posts from the diagnostics job to the estimates job.

- Updated the glue script job parameters for SFC-Internal jobs to match SFC-Internal step function.

- Tidy up StepFunctions Terraform code to include a `for_each` to create (most) pipelines dynamically, see the [guide](./terraform/pipeline/step-functions/README.md).

- Explicitly set `required_providers` in `pipeline/main.tf`, in order to version-lock the Terraform AWS provider to the minor version specified.

- Moved the filtering of `Social Care Org`, `Registered` and `Independent` sector CQC locations into the [IND CQC Merge](projects/_03_independent_cqc/_01_merge/jobs/merge_ind_cqc_data.py) job.

- Copies the full prod data into branch for testing purposes as opposed to the small subset currently used.

- Updated the build_snapshot_table_from_delta() function to retrieve the latest available data when the CQC API has not been executed on the step function run date.

- Only check postcodes match for locations who provide social care and are registered at that point in time.

- Removed validation check for a maximum of 500 beds in the CQC clean locations validation script.

- Updated docker file for create_dataset_snapshot lambda function to explicitly copy polars_utils while running.

- Removed locations from CQC delta clean when registration status or location type are null.

- Added placeholder tasks for new processes in [Transform-CQC-Data step function](./terraform/pipeline/step-functions/dynamic/Transform-CQC-Data.json).

- Changed CQC location clean validation script expected row count to exclude raw data adjustment locations.

- Created a job to flatten CQC location data to remove some unwanted data and simplify complex struct columns.

- Created a job to build a full flattened snapshot for each CQC location import data.

- Created a job to clean the full CQC location data to remove some unwanted data, join in ONS postcode data and split registered and deregistered locations.

- Moved the preparation of worker job role data into its own polars task in the ind CQC estimates pipeline.

- Decided not to convert the pyspark utils impute_historic_relationships and get_relationships_where_type_is_predecessor.
  Instead I have:
  Added relationships to flatten_struct_fields to extract types into new column relationships_types.
  Added relationships_types to impute_missing_values to back/forward fill gaps.

- Converted cleaning util add_related_location_flag from pyspark to polars. Called the util in cqc_locations_4_full_clean.py script.

- Reverted merge_ind_cqc_data job to state on 15th September, before using dimension datasets.

- Converted clean_and_impute_registration_date function from pyspark to polars.

- Added call for premade polars util extract_registered_manager_names to cqc_locations_2_flatten, which will create
  registered_manager_names column. Also added registered_manager_names column to impute_missing_values in cqc_locations_4_full_clean.

- Changed the behaivour of impute_missing_values in locations_4_clean_utils.
  Empty lists were being copied in the forward/backward fill, since it looks for non-null values.
  I've added another loop to go through the schema, if a columns datatype is a list, then replace empty lists in that
  column with null.
  Then the imputation happens as before, but only populated lists are used for filling.

- Converted cleaning util classify_specialisms from pyspark to polars.

- Converted function remove_specialist_colleges from pyspark to polars.

- Changed the behaviour of extract_registered_manager_names so it discards names when given or family name is null.

- Called Postcode Matching function from the new cleaning job. 

### Improved
- Moved postcode corrections dictionary into a csv file in s3.


## [v2025.08.0] - 09/09/2025

### Added
- New function added within flatten_cqc_ratings_job to flatten the new assessment column which is now used by CQC to publish the ratings data.

- Added current_lsoa21 column to the IND CQC pipeline. This column is now included across all jobs, ensuring it is present the Archive outputs.

- Added a new generic sink_to_parquet function in polars_utils for saving LazyFrames to s3.


### Changed
- Expanded acronyms in documentation.

- Removed providers dataset from clean locations job as it's no longer used.

- Updated [read_from_parquet()](utils/utils.py) function with a new optional schema parameter.

- Refactored [CQC API pipeline](terraform/pipeline/step-functions/CQC-API-Pipeline.json) to use delta model in Polars, including:
  - delta download tasks using CQC changes API
  - tasks for download written in Polars within ECS tasks
  - refactored Master & CQC-API StepFunctions to handle flow and separate concerns
  - downstream IND CQC and Coverage pipelines wired up to Master StepFunction
  - legacy bulk download pipeline disconnected from downstream processing but kept in place for reconciliation purposes
  - CQC locations cleaning uses delta model data

- Created an [SfC Internal pipeline](terraform/pipeline/step-functions/SfCInternal-StepFunction.json) step function which contains all the internal Skills for Care jobs in one pipeline.

- Updated [Error Notification lambda](lambdas/error_notifications/error_notifications.py) to handle ECS task failures.

- Split the `Master-Ingest` step function into
  - [ingestion only orchestrator](terraform/pipeline/step-functions/CQCAndASCWDSOrchestrator-StepFunction.json) to align CQC API and ASCWDS ingestion
  - [Workforce Intelligence](terraform/pipeline/step-functions/WorkforceIntelligence-StepFunction.json) pipeline for post-ingestion transformations
- Moved the deduplication and imputation of Capacity Tracker data from diagnostics_on_capacity_tracker to impute_ind_cqc_ascwds_and_pir.

- Upgraded all Python source code to 3.11, including:
  - resetting package versions using pipenv
  - upgrading to PySpark 3.5
  - upgrading Glue jobs to 5.0 (default Python version is 3.11)

- Removed recode_unknown_codes_to_null function call at preperation step of assessment data within flatten_cqc_ratings job.

- Stopped the filtering of non social care, deregistered and specialist college locations in the cleaning of delta locations data.
  The removal of these not required locations happens in merge_ind_cqc_data job.

- Added tests for raw data adjustment function is_valid_location.

- Added removal of invalid locations to cqc_locations_2_flatten task and its validation check comparator dataframe.

- Converted CQC cleaning functions allocate_primary_service_type and realign_carehome_column_with_primary_service from pyspark to polars.
  The code exists in a cleaning utils script now, instead of in the job script.

### Improved


## [v2025.07.0] - 13/08/2025

### Added
- Added a lambda function to check if two datasets are equal

- Added a lambda function to create a full snapshot from a delta dataset

- Created a `STYLEGUIDE.md` file with guidance on code organisation, folder structure, naming conventions, utility function locations and unit test conventions.

- Added tool to create delta datasets from full datasets (where we store full snapshots for every timepoint)
  - Added support for Care Quality Commission Providers application programming interface (API)
  - Added support for Care Quality Commission Locations application programming interface (API)

- Parallel data ingestion, cleaning and validation pipeline from Delta model:
  - Master-Ingest StepFunction to manage overall system flow for new delta pipeline
  - Refactored Care Quality Commission and Adult Social Care Workforce Data Set StepFunctions to include Crawlers and error handling
  - Includes parallel EventTrigger, SNS Topic and Crawlers, operating on a parallel temporary CQC_delta dataset for reconciliation purposes

- Refactor ingestion jobs to use Polars:
  -  Care Quality Commission Delta Providers
  -  Care Quality Commission Delta Locations

- Added unit test coverage

### Changed
- Update the version of Care Quality Commission ratings data

- Updated the Care Quality Commission locations schema to include a new assessment field for storing the latest Care Quality Commission ratings. Modified the function that builds the full locations dataset from the delta dataset to use this updated schema, ensuring the newly added column is included.

- Moved evaluation of Care Quality Commission Sector into Location cleaning script to remove unnecessary dependency.

### Improved
- Deduplicated Capacity Tracker data so it's more in line with the Adult Social Care Workforce Data Set and Provider Information Return process

## [v2025.05.0] - 18/06/2025
This version marks the codebase used for the publication of the Size and Structure 2025 report.

### Added
- Included related_location as a new feature in the non-residential with and without dormancy models to better distinguish genuinely new services from previously registered ones.

- Added a pre-commit hook to enforce linting, docstring standards, and to block accidental commits of .show() statements.

- Developed dataset-specific Step Functions triggered on new S3 file uploads. Each runs ingestion, validation, and cleaning in sequence for more timely error detection and fresher data availability.

- Incorporated more Provider Information Return data into the non-residential dataset.

- A lower level of service breakdowns to include all the categories we group to in our publications.

### Changed
- Reorganised file structure: introduced a projects-based layout with scoped jobs/, tests/, and utils/ directories. This improves navigation and code ownership across datasets and processes.

- Retrained the linear regression model from v2.0.0 to v2.0.1 to include more recent data.

### Improved
- Switched the care home, non-residential without dormancy, and non-residential with dormancy models from Gradient Boosted Trees (GBT) to linear regression. The GBT models were overfitting and unstable at location level; the new models offer better explainability and more stable trends.

- Replaced the rate of change feature in our models with a rolling average trendline to reduce bias towards open locations and better reflect real trends, especially closures and new openings.

- Revised our interpolation approach to only interpolate across gaps of up to 6 months. Previously, longer gaps caused trends to flatten unnaturally between known values.

- Replaced the binary is_dormant feature with a continuous time_since_dormant metric in the non-residential with dormancy model, improving prediction smoothness for post-dormancy growth.

- Reduced rolling periods in trend and model features from 6 months to 3 months, allowing the trends to respond more quickly to genuine shifts in the data.

- Refined postcode matching logic. The new multi-step approach attempts: exact match, historical match, mapped replacement, and truncated match before failing, improving match rates and reducing pipeline failures.

- Enhanced filtering of grouped Adult Social Care Workforce Data Set submissions where providers may be submitting their entire workforce into only one of their locations. These are now identified and the larger than expected values are nulled to prevent over-exaggerating the size of the workforce.


## [v2025.03.0] - 10/04/2025
Initial tagged release of the codebase.

This version marks the start of formal versioning and release tracking.
All previous work prior to this was unversioned.

### Added
- All code created up until this point in time
