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

module "ingest_cqc_care_directory_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_cqc_care_directory.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--source"               = ""
    "--provider_destination" = ""
    "--location_destination" = ""
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
  number_of_workers = 2
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "3.0"

  job_parameters = {
    "--ascwds_worker_source"            = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--ascwds_workplace_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace_cleaned/"
    "--ascwds_worker_destination"       = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker_cleaned/"
  }
}

module "clean_ascwds_workplace_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_ascwds_workplace_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--ascwds_workplace_source"              = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cleaned_ascwds_workplace_destination" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace_cleaned/"
    "--coverage_file_destination"            = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=coverage/"
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


module "prepare_non_res_ind_cqc_features_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_non_res_ind_cqc_features.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--ind_cqc_filled_posts_cleaned_source"  = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=cleaned_ind_cqc_data/"
    "--non_res_ind_cqc_features_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=non_res_ind_cqc_features/"
  }
}

module "clean_ind_cqc_filled_posts_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_ind_cqc_filled_posts.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--merged_ind_cqc_source"       = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=merged_ind_cqc_data/"
    "--cleaned_ind_cqc_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=cleaned_ind_cqc_data/"
  }
}

module "bulk_cqc_providers_download_job" {
  source           = "../modules/glue-job"
  script_name      = "bulk_download_cqc_providers.py"
  glue_role        = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket  = module.pipeline_resources
  datasets_bucket  = module.datasets_bucket
  trigger          = true
  trigger_schedule = "cron(30 01 01,08,15,23 * ? *)"
  glue_version     = "3.0"

  job_parameters = {
    "--destination_prefix" = "${module.datasets_bucket.bucket_uri}"
    "--additional-python-modules" : "ratelimit==2.2.1,"
  }
}

module "bulk_cqc_locations_download_job" {
  source           = "../modules/glue-job"
  script_name      = "bulk_download_cqc_locations.py"
  glue_role        = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket  = module.pipeline_resources
  datasets_bucket  = module.datasets_bucket
  trigger          = true
  trigger_schedule = "cron(30 01 01,08,15,23 * ? *)"
  glue_version     = "3.0"

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

module "flatten_cqc_ratings_job" {
  source          = "../modules/glue-job"
  script_name     = "flatten_cqc_ratings.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_location_source"           = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/version=2.0.0/"
    "--cqc_provider_source"           = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers_api/version=2.0.0/"
    "--ascwds_workplace_source"       = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cqc_ratings_destination"       = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=cqc_ratings/"
    "--benchmark_ratings_destination" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=benchmark_ratings/"
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
    "--cqc_location_source"                   = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/version=2.0.0/"
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
    "--cqc_location_api_source"                    = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations_api/"
    "--ascwds_coverage_source"                     = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=coverage/"
    "--reconciliation_single_and_subs_destination" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=reconciliation/singles_and_subs"
    "--reconciliation_parents_destination"         = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=reconciliation/parents"
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
    "--destination"                     = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=merged_ind_cqc_data/"
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
    "--merged_ind_cqc_source"       = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=merged_ind_cqc_data/"
    "--report_destination"          = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=merged_ind_cqc_data_report/"
  }
}

module "prepare_care_home_ind_cqc_features_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_care_home_ind_cqc_features.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--ind_cqc_filled_posts_cleaned_source"    = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=cleaned_ind_cqc_data/"
    "--care_home_ind_cqc_features_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=care_home_ind_cqc_features/"
  }
}


module "estimate_ind_cqc_filled_posts_job" {
  source          = "../modules/glue-job"
  script_name     = "estimate_ind_cqc_filled_posts.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cleaned_ind_cqc_source"        = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=cleaned_ind_cqc_data/"
    "--care_home_features_source"     = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=care_home_ind_cqc_features/"
    "--care_home_model_source"        = "${module.pipeline_resources.bucket_uri}/models/care_home_filled_posts_prediction/3.1.0/"
    "--estimated_ind_cqc_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=estimated_ind_cqc_filled_posts/"
    "--ml_model_metrics_destination"  = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ml_model_metrics/"
  }
}

module "estimate_ind_cqc_filled_posts_by_job_role_job" {
  source          = "../modules/glue-job"
  script_name     = "estimate_ind_cqc_filled_posts_by_job_role.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--estimated_ind_cqc_filled_posts_source"                  = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=estimated_ind_cqc_filled_posts/"
    "--estimated_ind_cqc_filled_posts_by_job_role_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=estimated_ind_cqc_filled_posts_by_job_role/"
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
  schedule                     = "cron(00 07 01,08,15,23 * ? *)"
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


