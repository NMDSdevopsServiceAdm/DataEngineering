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

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
    "--delimiter"   = ","

  }
}

module "ingest_ascwds_dataset_job" {
  source          = "../modules/glue-job"
  script_name     = "ingest_ascwds_dataset.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

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
  glue_version    = "3.0"

  job_parameters = {
    "--source"      = ""
    "--destination" = "${module.datasets_bucket.bucket_uri}/domain=ONS/"
  }
}

module "prepare_locations_job" {
  source            = "../modules/glue-job"
  script_name       = "prepare_locations.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  glue_version      = "3.0"
  worker_type       = "G.2X"
  number_of_workers = 6
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket

  job_parameters = {
    "--workplace_source"    = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cqc_location_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations-api/"
    "--cqc_provider_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers-api/"
    "--pir_source"          = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir/"
    "--destination"         = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared/version=1.0.0/"
  }
}

module "worker_tracking_job" {
  source          = "../modules/glue-job"
  script_name     = "worker_tracking.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--source_ascwds_workplace" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--source_ascwds_worker"    = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--destination"             = ""
  }
}


module "locations_feature_engineering_job" {
  source          = "../modules/glue-job"
  script_name     = "locations_feature_engineering.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--prepared_locations_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared/version=1.0.0/"
    "--destination"               = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_ml_features/version=1.0.0/"
  }
}

module "prepare_workers_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_workers.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--source"      = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--destination" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=workers_prepared/version=1.0.0/"
  }
}

module "job_role_breakdown_job" {
  source          = "../modules/glue-job"
  script_name     = "job_role_breakdown.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--job_estimates_source" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_estimates/version=1.0.0/"
    "--worker_source"        = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--destination"          = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_role_breakdown/version=1.0.0/"
  }
}

module "estimate_job_counts_job" {
  source          = "../modules/glue-job"
  script_name     = "estimate_job_counts.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "3.0"

  job_parameters = {
    "--prepared_locations_source"   = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_prepared/version=1.0.0/"
    "--prepared_locations_features" = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=locations_ml_features/version=1.0.0/"
    "--destination"                 = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=job_estimates/version=1.0.0/"
    "--care_home_model_directory"   = "${module.pipeline_resources.bucket_uri}/models/care_home_with_nursing_historical_jobs_prediction/"
    "--metrics_destination"         = "${module.datasets_bucket.bucket_uri}/domain=data_engineering/dataset=model_metrics/"
  }
}

module "bulk_cqc_providers_download_job" {
  source           = "../modules/glue-job"
  script_name      = "bulk_download_cqc_providers.py"
  glue_role        = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket  = module.pipeline_resources
  datasets_bucket  = module.datasets_bucket
  trigger          = true
  trigger_schedule = "cron(30 01 01 * ? *)"

  job_parameters = {
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
  trigger_schedule = "cron(30 01 01 * ? *)"

  job_parameters = {
    "--additional-python-modules" : "ratelimit==2.2.1,"
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
  exclusions                   = ["dataset=postcode-directory/**"]
  table_level                  = 4
}
