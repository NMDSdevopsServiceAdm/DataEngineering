resource "aws_glue_catalog_database" "glue_catalog_database" {
  name        = "${local.workspace_prefix}-${var.glue_database_name}"
  description = "Database for all datasets belonging to the ${local.workspace_prefix} environment."
}

module "csv_to_parquet_job" {
  source          = "../modules/glue-job"
  script_name     = "csv_to_parquet.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
    "--delimiter"   = ","

  }
}

module "spss_csv_to_parquet_job" {
  source          = "../modules/glue-job"
  script_name     = "spss_csv_to_parquet.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
  }
}

module "ingest_capacity_tracker_data_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_capacity_tracker_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
  }
}

module "ingest_cqc_pir_data_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_cqc_pir_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--source"      = ""
    "--destination" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir/"
  }
}

module "clean_cqc_pir_data_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_cqc_pir_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--cqc_pir_source"              = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir/"
    "--cleaned_cqc_pir_destination" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir_cleaned/"
  }
}

module "ingest_ascwds_dataset_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_ascwds_dataset.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
  }
}

module "clean_ascwds_worker_job" {
  source            = "../modules/glue-job"
  script_name       = "clean_ascwds_worker_data.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  worker_type       = "G.1X"
  number_of_workers = 4
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "3.0"

  job_parameters = {
    "--ascwds_worker_source"            = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--ascwds_workplace_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace_cleaned/"
    "--ascwds_worker_destination"       = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker_cleaned/"
  }
}

module "clean_capacity_tracker_care_home_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_capacity_tracker_care_home_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--capacity_tracker_care_home_source"              = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home/"
    "--cleaned_capacity_tracker_care_home_destination" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home_cleaned/"
  }
}

module "clean_capacity_tracker_non_res_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_capacity_tracker_non_res_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--capacity_tracker_non_res_source"              = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential/"
    "--cleaned_capacity_tracker_non_res_destination" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential_cleaned/"
  }
}

module "clean_ascwds_workplace_job" {
  source            = "../modules/glue-job"
  script_name       = "clean_ascwds_workplace_data.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  worker_type       = "G.1X"
  number_of_workers = 4
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "3.0"

  job_parameters = {
    "--ascwds_workplace_source"                  = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cleaned_ascwds_workplace_destination"     = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace_cleaned/"
    "--workplace_for_reconciliation_destination" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_workplace_for_reconciliation/"
  }
}


module "ingest_ons_data_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_ons_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--source"      = ""
    "--destination" = "${module.datasets_bucket.bucket_uri}/domain=ONS/"
  }
}


module "clean_ons_data_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_ons_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--ons_source"              = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory/"
    "--cleaned_ons_destination" = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory_cleaned/"
  }
}

module "archive_filled_posts_estimates_job" {
  source          = "../modules/glue-job"
  script_name     = "archive_filled_posts_estimates.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--estimate_ind_cqc_filled_posts_source"     = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/"
    "--monthly_filled_posts_archive_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=archived_monthly_filled_posts/"
    "--annual_filled_posts_archive_destination"  = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=archived_annual_filled_posts/"
  }
}


module "prepare_features_non_res_ascwds_ind_cqc_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_features_non_res_ascwds_ind_cqc.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--ind_cqc_filled_posts_cleaned_source"                          = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/"
    "--non_res_ascwds_inc_dormancy_ind_cqc_features_destination"     = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_inc_dormancy/"
    "--non_res_ascwds_without_dormancy_ind_cqc_features_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_without_dormancy/"
  }
}


module "prepare_features_non_res_pir_ind_cqc_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_features_non_res_pir_ind_cqc.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--ind_cqc_cleaned_data_source"              = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/"
    "--non_res_pir_ind_cqc_features_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_pir/"
  }
}

module "clean_ind_cqc_filled_posts_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_ind_cqc_filled_posts.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--merged_ind_cqc_source"       = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_merged_data/"
    "--cleaned_ind_cqc_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
  }
}

module "bulk_cqc_providers_download_job" {
  source          = "../modules/glue-job"
  script_name     = "bulk_download_cqc_providers.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--destination_prefix" = "${module.datasets_bucket.bucket_uri}"
    "--additional-python-modules" : "ratelimit==2.2.1,"
  }
}

module "bulk_cqc_locations_download_job" {
  source          = "../modules/glue-job"
  script_name     = "bulk_download_cqc_locations.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--destination_prefix" = "${module.datasets_bucket.bucket_uri}"
    "--additional-python-modules" : "ratelimit==2.2.1,"
  }
}


module "ingest_dpr_external_data_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_dpr_external_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--external_data_source"      = ""
    "--external_data_destination" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external/version=2024.01/"
  }
}

module "ingest_dpr_survey_data_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_dpr_survey_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--survey_data_source"      = ""
    "--survey_data_destination" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey/version=2024.01/"
  }
}

module "prepare_dpr_external_data_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_dpr_external_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"
  job_parameters = {
    "--direct_payments_source" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external/version=2024.01/"
    "--destination"            = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_external_prepared/version=2024.01/"
  }
}

module "prepare_dpr_survey_data_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_dpr_survey_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"
  job_parameters = {
    "--survey_data_source" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey/version=2024.01/"
    "--destination"        = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_survey_prepared/version=2024.01/"
  }
}

module "merge_dpr_data_job" {
  source          = "../modules/glue-job"
  script_name     = "merge_dpr_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"
  job_parameters = {
    "--direct_payments_external_data_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_external_prepared/version=2024.01/"
    "--direct_payments_survey_data_source"   = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_survey_prepared/version=2024.01/"
    "--destination"                          = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_merged/version=2024.01/"
  }
}

module "estimate_direct_payments_job" {
  source          = "../modules/glue-job"
  script_name     = "estimate_direct_payments.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"
  job_parameters = {
    "--direct_payments_merged_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_merged/version=2024.01/"
    "--destination"                   = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_estimates/version=2024.01/"
    "--summary_destination"           = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_estimates_summary/version=2024.01/"
  }
}

module "split_pa_filled_posts_into_icb_areas_job" {
  source          = "../modules/glue-job"
  script_name     = "split_pa_filled_posts_into_icb_areas.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"
  job_parameters = {
    "--postcode_directory_source" = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory_cleaned/"
    "--pa_filled_posts_souce"     = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_estimates/version=2024.01/"
    "--destination"               = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_estimates_by_icb/version=2024.01/"
  }
}

module "flatten_cqc_ratings_job" {
  source          = "../modules/glue-job"
  script_name     = "flatten_cqc_ratings.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_location_source"           = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/version=2.1.0/"
    "--ascwds_workplace_source"       = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cqc_ratings_destination"       = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_cqc_ratings_for_data_requests/"
    "--benchmark_ratings_destination" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_cqc_ratings_for_benchmarks/"
  }
}

module "clean_cqc_provider_data_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_cqc_provider_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_provider_source"  = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers_api/version=2.0.0/"
    "--cqc_provider_cleaned" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers_api_cleaned/"
  }
}

module "clean_cqc_location_data_job" {
  source            = "../modules/glue-job"
  script_name       = "clean_cqc_location_data.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  worker_type       = "G.2X"
  number_of_workers = 5

  job_parameters = {
    "--cqc_location_source"                   = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/version=2.1.0/"
    "--cleaned_cqc_provider_source"           = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers_api_cleaned/"
    "--cleaned_ons_postcode_directory_source" = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory_cleaned/"
    "--cleaned_cqc_location_destination"      = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api_cleaned/"
  }
}

module "reconciliation_job" {
  source          = "../modules/glue-job"
  script_name     = "reconciliation.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_location_api_source"                    = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/version=2.1.0/"
    "--ascwds_reconciliation_source"               = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_workplace_for_reconciliation/"
    "--reconciliation_single_and_subs_destination" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_reconciliation_singles_and_subs"
    "--reconciliation_parents_destination"         = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_reconciliation_parents"
  }
}

module "merge_ind_cqc_data_job" {
  source            = "../modules/glue-job"
  script_name       = "merge_ind_cqc_data.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "3.0"
  worker_type       = "G.2X"
  number_of_workers = 5

  job_parameters = {
    "--cleaned_cqc_location_source"     = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api_cleaned/"
    "--cleaned_cqc_pir_source"          = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir_cleaned/"
    "--cleaned_ascwds_workplace_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace_cleaned/"
    "--destination"                     = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_merged_data/"
  }
}

module "merge_coverage_data_job" {
  source          = "../modules/glue-job"
  script_name     = "merge_coverage_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--cleaned_cqc_location_source"         = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api_cleaned/"
    "--workplace_for_reconciliation_source" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_workplace_for_reconciliation/"
    "--cqc_ratings_source"                  = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_cqc_ratings_for_data_requests/"
    "--merged_coverage_destination"         = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_merged_coverage_data/"
    "--reduced_coverage_destination"        = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_monthly_coverage_data/"
  }
}

module "validate_locations_api_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_locations_api_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_cqc_location_source"      = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/version=2.1.0/"
    "--cleaned_cqc_locations_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api_cleaned/"
    "--report_destination"           = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_locations_api_cleaned/"
  }
}

module "validate_providers_api_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_providers_api_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_cqc_provider_source"      = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers_api/version=2.0.0/"
    "--cleaned_cqc_providers_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers_api_cleaned/"
    "--report_destination"           = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_providers_api_cleaned/"
  }
}

module "validate_pir_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_pir_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_cqc_pir_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir_cleaned/"
    "--report_destination"     = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_pir_cleaned/"
  }
}

module "validate_ascwds_workplace_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_ascwds_workplace_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ascwds_workplace_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace_cleaned/"
    "--report_destination"              = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_workplace_cleaned/"
  }
}

module "validate_ascwds_worker_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_ascwds_worker_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ascwds_worker_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker_cleaned/"
    "--report_destination"           = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_worker_cleaned/"
  }
}

module "validate_postcode_directory_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_postcode_directory_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_postcode_directory_source"     = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory/"
    "--cleaned_postcode_directory_source" = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory_cleaned/"
    "--report_destination"                = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_postcode_directory_cleaned/"
  }
}

module "validate_merged_ind_cqc_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_merged_ind_cqc_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_cqc_location_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api_cleaned/"
    "--merged_ind_cqc_source"       = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_merged_data/"
    "--report_destination"          = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_merged_data/"
  }
}

module "validate_merge_coverage_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_merge_coverage_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_cqc_location_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api_cleaned/"
    "--merged_coverage_data_source" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_merged_coverage_data/"
    "--report_destination"          = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_sfc_merged_coverage_data/"
  }
}

module "validate_cleaned_ind_cqc_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_cleaned_ind_cqc_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--merged_ind_cqc_source"  = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_merged_data/"
    "--cleaned_ind_cqc_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--report_destination"     = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_cleaned_data/"
  }
}

module "validate_estimated_missing_ascwds_filled_posts_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_estimated_missing_ascwds_filled_posts_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ind_cqc_source"                       = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--estimated_missing_ascwds_filled_posts_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/"
    "--report_destination"                           = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_estimated_missing_ascwds_filled_posts/"
  }
}

module "validate_care_home_ind_cqc_features_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_care_home_ind_cqc_features_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ind_cqc_source"            = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--care_home_ind_cqc_features_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_care_home/"
    "--report_destination"                = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_features_care_home/"
  }
}

module "validate_non_res_ascwds_inc_dormancy_ind_cqc_features_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_non_res_ascwds_inc_dormancy_ind_cqc_features_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ind_cqc_source"                              = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--non_res_ascwds_inc_dormancy_ind_cqc_features_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_inc_dormancy/"
    "--report_destination"                                  = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_features_non_res_ascwds_inc_dormancy/"
  }
}


module "validate_non_res_ascwds_without_dormancy_ind_cqc_features_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_non_res_ascwds_without_dormancy_ind_cqc_features_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ind_cqc_source"                                  = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--non_res_ascwds_without_dormancy_ind_cqc_features_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_without_dormancy/"
    "--report_destination"                                      = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_features_non_res_ascwds_without_dormancy/"
  }
}


module "validate_non_res_pir_ind_cqc_features_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_non_res_pir_ind_cqc_features_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ind_cqc_source"              = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/"
    "--non_res_pir_ind_cqc_features_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_pir/"
    "--report_destination"                  = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_non_res_pir_ind_cqc_features/"
  }
}


module "validate_estimated_ind_cqc_filled_posts_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_estimated_ind_cqc_filled_posts_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ind_cqc_source"                = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--estimated_ind_cqc_filled_posts_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/"
    "--report_destination"                    = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_estimated_filled_posts/"
  }
}


module "validate_estimated_ind_cqc_filled_posts_by_job_role_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_estimated_ind_cqc_filled_posts_by_job_role_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--cleaned_ind_cqc_source"                            = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--estimated_ind_cqc_filled_posts_by_job_role_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_by_job_role/"
    "--report_destination"                                = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_ind_cqc_estimated_filled_posts_by_job_role/"
  }
}

module "validate_ascwds_workplace_raw_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_ascwds_workplace_raw_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_ascwds_workplace_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--report_destination"          = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_workplace_raw/"
  }
}

module "validate_ascwds_worker_raw_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_ascwds_worker_raw_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_ascwds_worker_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--report_destination"       = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_worker_raw/"
  }
}

module "validate_locations_api_raw_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_locations_api_raw_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_cqc_location_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/version=2.1.0/"
    "--report_destination"      = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_locations_api_raw/"
  }
}

module "validate_providers_api_raw_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_providers_api_raw_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_cqc_provider_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers_api/version=2.0.0/"
    "--report_destination"      = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_providers_api_raw/"
  }
}

module "validate_pir_raw_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_pir_raw_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_cqc_pir_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir/"
    "--report_destination" = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_pir_raw/"
  }
}

module "validate_postcode_directory_raw_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_postcode_directory_raw_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--raw_postcode_directory_source" = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory/"
    "--report_destination"            = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_postcode_directory_raw/"
  }
}

module "prepare_features_care_home_ind_cqc_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_features_care_home_ind_cqc.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--ind_cqc_filled_posts_cleaned_source"    = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/"
    "--care_home_ind_cqc_features_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_care_home/"
  }
}

module "estimate_missing_ascwds_ind_cqc_filled_posts_job" {
  source            = "../modules/glue-job"
  script_name       = "estimate_missing_ascwds_ind_cqc_filled_posts.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  worker_type       = "G.1X"
  number_of_workers = 4
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket

  job_parameters = {
    "--cleaned_ind_cqc_source"                       = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_cleaned_data/"
    "--estimated_missing_ascwds_ind_cqc_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/"
  }
}

module "estimate_ind_cqc_filled_posts_job" {
  source            = "../modules/glue-job"
  script_name       = "estimate_ind_cqc_filled_posts.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  worker_type       = "G.1X"
  number_of_workers = 4
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket

  job_parameters = {
    "--estimate_missing_ascwds_filled_posts_data_source" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/"
    "--care_home_features_source"                        = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_care_home/"
    "--care_home_model_source"                           = "${module.pipeline_resources.bucket_uri}/models/care_home_filled_posts_prediction/5.0.0/"
    "--non_res_with_dormancy_features_source"            = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_inc_dormancy/"
    "--non_res_with_dormancy_model_source"               = "${module.pipeline_resources.bucket_uri}/models/non_residential_with_dormancy_prediction/3.0.0/"
    "--non_res_without_dormancy_features_source"         = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_ascwds_without_dormancy/"
    "--non_res_without_dormancy_model_source"            = "${module.pipeline_resources.bucket_uri}/models/non_residential_without_dormancy_prediction/3.0.0/"
    "--non_res_pir_linear_regression_features_source"    = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_features_non_res_pir/"
    "--non_res_pir_linear_regression_model_source"       = "${module.pipeline_resources.bucket_uri}/models/non_res_pir_linear_regression_prediction/2.0.0/"
    "--estimated_ind_cqc_destination"                    = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/"
    "--ml_model_metrics_destination"                     = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_ml_model_metrics/"
  }
}

module "estimate_ind_cqc_filled_posts_by_job_role_job" {
  source          = "../modules/glue-job"
  script_name     = "estimate_ind_cqc_filled_posts_by_job_role.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--estimated_ind_cqc_filled_posts_source"                  = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/"
    "--estimated_ind_cqc_filled_posts_by_job_role_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_by_job_role/"
  }
}

module "diagnostics_on_known_filled_posts_job" {
  source          = "../modules/glue-job"
  script_name     = "diagnostics_on_known_filled_posts.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--estimate_filled_posts_source"    = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/"
    "--diagnostics_destination"         = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_diagnostics/"
    "--summary_diagnostics_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_diagnostics_summary/"
    "--charts_destination"              = "${module.datasets_bucket.bucket_name}"
  }
}

module "diagnostics_on_capacity_tracker_job" {
  source          = "../modules/glue-job"
  script_name     = "diagnostics_on_capacity_tracker.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--estimate_filled_posts_source"              = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/"
    "--capacity_tracker_care_home_source"         = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home_cleaned/"
    "--capacity_tracker_non_res_source"           = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential_cleaned/"
    "--care_home_diagnostics_destination"         = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=capacity_tracker_care_home_diagnostics/"
    "--care_home_summary_diagnostics_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=capacity_tracker_care_home_diagnostics_summary/"
    "--non_res_diagnostics_destination"           = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=capacity_tracker_non_residential_diagnostics/"
    "--non_res_summary_diagnostics_destination"   = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=capacity_tracker_non_residential_diagnostics_summary/"
  }
}

module "validate_cleaned_capacity_tracker_care_home_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_cleaned_capacity_tracker_care_home_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--capacity_tracker_care_home_source"         = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home/"
    "--capacity_tracker_care_home_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home_cleaned/"
    "--report_destination"                        = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_capacity_tracker_care_home_cleaned_data/"
  }
}

module "validate_cleaned_capacity_tracker_non_res_data_job" {
  source          = "../modules/glue-job"
  script_name     = "validate_cleaned_capacity_tracker_non_res_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "4.0"

  job_parameters = {
    "--capacity_tracker_non_res_source"         = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential/"
    "--capacity_tracker_non_res_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential_cleaned/"
    "--report_destination"                      = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=data_quality_report_capacity_tracker_non_residential_cleaned_data/"
  }
}

module "ascwds_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ASCWDS"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "data_engineering_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "data_engineering"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "ind_cqc_filled_posts_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ind_cqc_filled_posts"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "data_validation_reports_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "data_validation_reports"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "cqc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "CQC"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "sfc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "SfC"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "ons_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ONS"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
  exclusions                   = ["dataset=postcode-directory-field-lookups/**"]
}

module "dpr_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "DPR"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "capacity_tracker_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "capacity_tracker"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}


