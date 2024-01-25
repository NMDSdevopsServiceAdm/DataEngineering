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
    "--care_home_source"      = ""
    "--care_home_destination" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=capacity_tracker_care_home/"
    "--non_res_source"        = ""
    "--non_res_destination"   = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=capacity_tracker_non_residential/"
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

module "denormalise_ons_data_job" {
  source          = "../modules/glue-job"
  script_name     = "denormalise_ons_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  job_parameters = {
    "--ons_source"    = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode-directory/"
    "--lookup_source" = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode-directory-field-lookups/"
    "--destination"   = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode-directory-denormalised/"
  }
}

module "prepare_locations_job" {
  source            = "../modules/glue-job"
  script_name       = "prepare_locations.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  worker_type       = "G.2X"
  number_of_workers = 6
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "3.0"
  job_parameters = {
    "--workplace_source"    = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cqc_location_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations-api/"
    "--cqc_provider_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers-api/"
    "--pir_source"          = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir/"
    "--ons_source"          = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode-directory-denormalised/"
    "--destination"         = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared/version=1.0.0/"
  }
}


module "worker_tracking_job" {
  source          = "../modules/glue-job"
  script_name     = "worker_tracking.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"


  job_parameters = {
    "--source_ascwds_workplace" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--source_ascwds_worker"    = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--destination"             = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=worker_tracking/version=1.0.0/"
  }
}


module "locations_care_home_feature_engineering_job" {
  source          = "../modules/glue-job"
  script_name     = "locations_care_home_feature_engineering.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--prepared_locations_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared_cleaned/version=1.0.0/"
    "--destination"               = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_ml_features_care_homes/2.0.0/"
  }
}


module "locations_non_res_feature_engineering_job" {
  source          = "../modules/glue-job"
  script_name     = "locations_non_res_feature_engineering.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--prepared_locations_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared_cleaned/version=1.0.0/"
    "--destination"               = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_ml_features_non_res/2.0.0/"
  }
}

module "job_role_breakdown_job" {
  source          = "../modules/glue-job"
  script_name     = "job_role_breakdown.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--job_estimates_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_estimates/version=2.0.0/"
    "--worker_source"        = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--destination"          = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_role_breakdown/version=1.0.0/"
  }
}

module "prepare_locations_cleaned_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_locations_cleaned.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--prepared_locations_source"              = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared/version=1.0.0/"
    "--prepared_locations_cleaned_destination" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared_cleaned/version=1.0.0/"
  }
}

module "estimate_job_counts_job" {
  source          = "../modules/glue-job"
  script_name     = "estimate_job_counts.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--prepared_locations_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared_cleaned/version=1.0.0/"
    "--carehome_features_source"          = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_ml_features_care_homes/2.0.0/"
    "--nonres_features_source"            = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_ml_features_non_res/2.0.0/"
    "--destination"                       = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_estimates/version=2.0.0/"
    "--care_home_model_directory"         = "${module.pipeline_resources.bucket_uri}/models/care_home_with_nursing_historical_jobs_prediction/2.1.0/"
    "--non_res_with_pir_model_directory"  = "${module.pipeline_resources.bucket_uri}/models/non_residential_with_pir_jobs_prediction/2.0.0/"
    "--metrics_destination"               = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=model_metrics/"
  }
}

module "create_job_estimates_diagnostics" {
  source          = "../modules/glue-job"
  script_name     = "create_job_estimates_diagnostics.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--estimate_job_counts_source"              = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_estimates/version=2.0.0/"
    "--capacity_tracker_care_home_source"       = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=capacity_tracker_care_home/"
    "--capacity_tracker_non_residential_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=capacity_tracker_non_residential/"
    "--diagnostics_destination"                 = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_estimates_diagnostics/version=0.1.0/"
    "--residuals_destination"                   = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_estimates_residuals/version=0.1.0/"
    "--description_of_change"                   = ""
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
    "--external_data_destination" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external/version=2023.01/"
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
    "--survey_data_destination" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey/version=2023.01/"
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
    "--direct_payments_source" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external/version=2023.01/"
    "--destination"            = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_external_prepared/version=2023.01/"
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
    "--survey_data_source" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey/version=2023.01/"
    "--destination"        = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_survey_prepared/version=2023.01/"
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
    "--direct_payments_external_data_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_external_prepared/version=2023.01/"
    "--direct_payments_survey_data_source"   = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_survey_prepared/version=2023.01/"
    "--destination"                          = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_merged/version=2023.01/"
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
    "--direct_payments_merged_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_merged/version=2023.01/"
    "--destination"                   = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_estimates/version=2023.01/"
    "--summary_destination"           = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=direct_payments_estimates_summary/version=2023.01/"
  }
}

module "clean_cqc_provider_data_job" {
  source          = "../modules/glue-job"
  script_name     = "clean_cqc_provider_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_provider_source"  = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers-api/"
    "--cqc_provider_cleaned" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers-api-cleaned/"
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

module "cqc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "CQC"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  schedule                     = "cron(00 07 * * ? *)"
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "ons_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ONS"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
  exclusions                   = ["dataset=postcode-directory-field-lookups/**"]
}

module "ons_lookups_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ONS"
  name_postfix                 = "_lookups"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
  exclusions                   = ["dataset=postcode-directory/**", "dataset=postcode-directory-denormalised/**"]
  table_level                  = 4
}

module "dpr_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "DPR"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

