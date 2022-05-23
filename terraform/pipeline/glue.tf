resource "aws_glue_catalog_database" "glue_catalog_database" {
  name = "${terraform.workspace}-${var.glue_database_name}"
}
module "csv_to_parquet_job" {
  source          = "../modules/glue-job"
  script_name     = "csv_to_parquet.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources

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

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
  }
}

module "prepare_locations_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_locations.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources

  job_parameters = {
    "--environment"         = "prod"
    "--workplace_source"    = "s3://sfc-data-engineering/domain=ASCWDS/dataset=workplace/"
    "--cqc_location_source" = "s3://sfc-data-engineering/domain=CQC/dataset=locations-api/"
    "--cqc_provider_source" = "s3://sfc-data-engineering/domain=CQC/dataset=providers-api/"
    "--pir_source"          = "s3://sfc-data-engineering/domain=CQC/dataset=pir/"
    "--destination"         = ""
  }
}

module "job_role_breakdown_job" {
  source          = "../modules/glue-job"
  script_name     = "job_role_breakdown.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources

  job_parameters = {
    "--job_estimates_source" = ""
    "--worker_source"        = ""
    "--destination"          = ""
  }
}

module "estimate_2021_jobs_job" {
  source          = "../modules/glue-job"
  script_name     = "estimate_2021_jobs.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources

  job_parameters = {
    "--job_estimates_source"      = ""
    "--prepared_locations_source" = ""
    "--destination"               = ""
  }
}

module "bulk_cqc_providers_download_job" {
  source          = "../modules/glue-job"
  script_name     = "bulk_download_cqc_providers.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources

  job_parameters = {
    "--additional-python-modules" : "ratelimit==2.2.1,"
  }
}

module "bulk_cqc_locations_download_job" {
  source          = "../modules/glue-job"
  script_name     = "bulk_download_cqc_locations.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources

  job_parameters = {
    "--additional-python-modules" : "ratelimit==2.2.1,"
  }
}

module "ascwds_crawler" {
  source                       = "../modules/glue-crawler"
  datset_for_crawler           = "ASCWDS"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  data_path                    = var.ascwds_root_data_location
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}

module "data_engineering_crawler" {
  source                       = "../modules/glue-crawler"
  datset_for_crawler           = "DATA_ENGINEERING"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  data_path                    = var.data_engineering_root_data_location
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}

module "cqc_crawler" {
  source                       = "../modules/glue-crawler"
  datset_for_crawler           = "CQC"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  data_path                    = var.cqc_root_data_location
  schedule                     = "cron(00 07 * * ? *)"
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}
