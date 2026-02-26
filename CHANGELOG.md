# Changelog

All notable changes to this project will be documented in this file.


## [Unreleased]

### Added
- Converted archive job from pyspark to polars.

- Created polars job script for clean ind cqc filled posts. Script only reads and writes data and has comments
  for each util call to be added to the script as it's developed.
  Added this job to the estimates pipeline in parallel with pyspark version.

### Changed
- Switched test runner to pytest in CI, so that a shared session fixture for a spark configuration optimised for tests
  can be used. Has resulted in 70% reduction in runtime for current pyspark tests in CI, and improvement on local
  Windows machines. https://github.com/NMDSdevopsServiceAdm/DataEngineering/pull/1219

- Refactored the rate of change to filter rows instead of nulling values to speed up processing time and make the
  filtering process in a follow-up PR much easier to implement.

- Changed the forward-filling of the last known value from two months for all locations to using a length of time base on the location size.

- Changed imputation to apply nominal changes instead of converting and applying a ratio.

- Removed minimum value requirement from `merge_columns_in_order`.

- Moved `set_min_value` from model predictions and imputation to after the estimates column is produced.

- Replaced the static PySpark modelling code for non-residential with dormancy with the auto-retraining sklearn equivalent.

### Fixed


## [v2026.01.0] - 12/02/2026

### Added
- A new function to remove longitudinal outliers from CT data. The function flags the outliers based on absolute difference in median values and removes the ourlier value.

- Converted util functions select_rows_with_value and select_rows_with_non_null_value from spark to polars.

- Polars job [model_03_predict](projects/_03_independent_cqc/_04_model/fargate/model_03_predict.py) to load a specified model then generate and save predictions.

- Created a utils function `enrich_with_model_predictions` to read, transform and join model predictions into the Independent CQC DataFrame.

- Polars job [validate_model_01_features](projects/_03_independent_cqc/_04_model/fargate/validate_model_01_features.py) to validate features data. Associated unit tests also added.
  - Updated polars job to validate non_res_with_dormancy_model features and updated associated unit tests.
  - Updated polars job to validate care_home_model features and updated associated unit tests.
  - Updated polars job to validate non_res_without_dormancy_model features and updated associated unit tests.

- Added three validation rules for primary_service_second_level column to check expected values.

- Python package [aws-mfa-v2] (https://pypi.org/project/aws-mfa-v2/) to allow for terraform to handle MFA authentication in a cross role account when used locally.

- Converted the validate_merged_ind_cqc_data job script from PyDeequ to Pointblank

- Converted the validate_merge_coverage_data job script from PyDeequ to Pointblank.

- Converted ascwds_filled_posts_calculator utils folder to polars within Clean Ind CQC Job.

- Added a new utils file for all the inline function within clean_ind_cqc_filled_posts.py and converted them to Polars.

- Converted `filtering_utils.py`, `forward_fill_latest_known_value.py` and `utils.py` within clean Ind CQC job to polars.

### Changed
- Remove interim/demo model preprocessing/retraining code.

- Removed model metrics from [estimate_ind_cqc_filled_posts](projects/_03_independent_cqc/_06_estimate_filled_posts/jobs/estimate_ind_cqc_filled_posts.py) job. The replacement models create models as part of the model training/testing process.

- Moved the `convert_care_home_ratios_to_posts` function to the impute [utils](projects/_03_independent_cqc/_03_impute/utils/utils.py) file to align with the jobs it's called in.

- Migrated the original `PySpark` Independent CQC estimates pipeline step `_01_merge` to be run in `Polars` for notable increases in efficiency.

- Refactored two similar functions (`insert_predictions_into_pipeline` and `prepare_predictions_for_join`) into one (`join_model_predictions`).

- Moved calculate_care_home_status_count function from estimate filled posts utils to clean_ind_cqc_filled_posts job.

- In the clean ASC-WDS workplace job, a new function was added to select only the required columns before saving the cleaned ASC-WDS workplace data.

- Removed filters from run_postcode_matching function. Data is already filtered before passed to this function.

- Added unit tests for combine_non_res_with_and_without_dormancy_models function to check expected columns and row count.

- Replaced the static PySpark modelling code for care homes and non-residential without dormancy with the auto-retraining sklearn equivalent.

### Fixed
- Added a new test account in ASC-WDS to the list of test_accounts in [clean_ascwds_workplace_data](projects\_01_ingest\ascwds\jobs\clean_ascwds_workplace_data.py)



## [v2025.12.0] - 06/01/2026

### Added
- Called the forward_fill_latest_known_value function on pir_people_directly_employed_dedup column to replicate the forward fill process for PIR Data.

- Polars job [model_02_train](projects/_03_independent_cqc/_04_model/fargate/model_02_train.py) to train, test and save a specified model.


## [v2025.11.0] - 18/12/2025

### Added
- Added provider name into the merged dataframe within the CQC Coverage job.

- New function added to merge the old CQC ratings and the new assessment ratings.

- Polars version of the estimates by job role job and added job to new step function for ind cqc estimates.

- Implemented complex validation for [validate_delta_locations_api_cleaned](projects/_01_ingest/cqc_api/fargate/validate_delta_locations_api_cleaned.py), includes:
  - split into dimensions table with separate validation
  - Pointblank translation of helper functions and new expressions.

- Function to clean Capacity Tracker care home data by nulling when posts to beds ratio is outside thresholds.

- Tagging added by default to all Terraform elements

- Added current_msoa21 column to the IND CQC pipeline.

- Created a new folder and file structure to contain the model re-training process files.

- Added placeholder tasks for model retraining step function in the Ind CQC pipeline which will point to the
  separate model retraining step function.

- During deployment with CircleCI, copy the `ind_cqc_06_estimated_filled_posts` dataset from main into branch prefixed with `main_` to help with comparing dev outputs to main.

- Created model versioning functions to get the last number and save model metadata.

- Created the S3 paths for the modelling process to call on for loading and saving data.

- Generalised the `update_filtering_rule` function to run against different datasets.

- Created a function to handle to various Capacity Tracker non-residential cleaning steps.

- Added ML model registry.

- Added function to validate model definitions used in tasks are stored in the model registry.

- Created Polars jobs to prepare care home and non residential features.

- Added NHS Capacity Tracker filter to remove repeated submissions at location level after a set length of time.

- Added a function to extrapolate/repeat the last known ASC-WDS filled posts value into the following two import dates.

### Changed
- Used `isort` extension to consistently format imports throughout codebase and going forwards.

- Included the newer CQC `assessments` column to get the latest ratings information from CQC and incorporated into existing jobs.

- Added an argument to generalise `calculate_filled_posts_per_bed_ratio` to run against any column.

- Removed the deduplication of Capacity Tracker data and used the cleaned Capacity Tracker care home data for imputation.

- Moved the calculations for estimating all Capacity Tracker (care home and non-residential) filled posts from the diagnostics job to the estimate filled posts job.

- Updated the glue script job parameters for SFC-Internal jobs to match SFC-Internal step function.

- Tidy up StepFunctions Terraform code to include a `for_each` to create (most) pipelines dynamically, see the [guide](./terraform/pipeline/step-functions/README.md).

- Explicitly set `required_providers` in `pipeline/main.tf`, in order to version-lock the Terraform AWS provider to the minor version specified.

- Copy full production data into the branches for testing purposes as opposed to a small subset of data.

- Removed validation check for a maximum of 500 beds in the CQC clean locations validation script.

- Removed locations from CQC delta clean when registration status or location type are null.

- Added placeholder tasks for new processes in [Transform-CQC-Data step function](./terraform/pipeline/step-functions/dynamic/Transform-CQC-Data.json).

- Changed CQC location clean validation script expected row count to exclude raw data adjustment locations.

- Created a job to flatten CQC location data to extract only the required information from complex struct columns.

- Created a function `convert_delta_to_full` to build a full snapshot for each CQC import date from a delta dataset and called for both locations and providers datasets.

- Moved the preparation of worker job role data into its own polars task in the ind CQC estimates pipeline.

- Converted cleaning util add_related_location_flag from pyspark to polars. Called the util in cqc_locations_4_full_clean.py script.

- Changed the behaivour of `impute_missing_values` in locations_4_clean_utils.
  - Empty lists were being copied in the forward/backward fill, since it looks for non-null values.
  - Added a loop to go through the schema and if the columns datatype is a list, then replace empty lists in that
  column with null.

- Changed the behaviour of extract_registered_manager_names so it discards names when given or family name is null.

- Updated all the references of delta cqc data to point to new version 3.1.1.

- Moved creation of cqc_locations_import_date from flatten to clean job so it populates all time periods correctly.

- Refactored the Transform CQC pipeline to be more efficient.

- Removed all previous `CQC` jobs, utils and tests no longer being used as they are now replaced by a `CQC_delta` version.

- Removed lambdas no longer being used (`check_dataset_equality` and `create_dataset_snapshot`)

- Updated input parameter for the flatten CQC ratings job. Ratings columns are now retrieved from the raw data and joined with the latest locations snapshot, which is the new input source for the job.

- Converted all our references of `CQC_delta` back to `CQC` now the transition period is complete.

- Changed the `remove_duplicate_cqc_care_homes` function as followed:
  - changed function name to `remove_dual_registration_cqc_care_homes`.
  - updated docstring with information explaining dual registrations from CQC.
  - changed how ASC-WDS data is copied across dual registrations to coalesce the orginal value and the max over a window.
  - added `location_id` as a fallback column to distinguish identical locations consistantly by selecting the first on numerically (to replicate CQC's approach).

- `cqc_locations_4_full_clean` now exports two files:
  - one for use in the CQC Independent Sector pipeline which applies filtering such as `Registered`, `Independent`, `Social Care Org` only.
  - a second file which contains the entiritely of the most recent CQC snapshot, containing all locations who have any been registered with CQC. This dataset is used in the various jobs in the `sfc_internal` pipeline.

- Updated the SfC Internal jobs based on the new dataset saved above.

- Changed the CQC API delta download process as follows:
  - Get all column data from the API (previously only returned columns in our schema).
  - Create a 'primed generator' using our known schema to Polars can infer known column types from that.
  - Normalise struct columns so struct changes are able to join into our existing data.
  - Cast all columns contained in our existing schema to match existing types.
  - Store all raw CQC API delta data in `dataset_delta_locations_api` / `dataset_delta_providers_api`.
  - Import only required columns in downstream jobs.

- Fixed validation failures for CQC ingestion jobs caused by complex columns in the DataFrame, which prevented PointBlank from generating output reports.
  - Updated the validation script to replicate the newly added filter from the cleaning process, ensuring row count validations now pass.
  - Introduced a new function that creates boolean flag columns for complex-type columns and removes the original complex columns from the source DataFrame. These flag columns are now used in PointBlank validations to check for True values.


### Improved
- Migrated the original `PySpark` CQC API ingestion process to be run in `Polars` for notable increases in efficiency.

- Migrated the original `PyDeequ` CQC API ingestion validation scripts to use PointBlank (compatible with >= Python 3.11).

- Moved postcode corrections dictionary into a csv file in s3.

- Fixed `add_previous_value_column` in the rate of change trendline model to return the value/null from the previous import_date only, not the last known value from any point in time.



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

- Moved the filter to remove invalid locaitonids from cqc_locaitons_2_delta_flatten to cqc_locations_4_full_clean.

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
