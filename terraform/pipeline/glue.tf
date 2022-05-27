resource "aws_glue_catalog_database" "glue_catalog_database" {
  name        = "${terraform.workspace}-${var.glue_database_name}"
  description = "Database for all datasets belonging to the ${terraform.workspace} environment."
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

module "prepare_locations_job" {
  source          = "../modules/glue-job"
  script_name     = "prepare_locations.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--workplace_source"    = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cqc_location_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=locations-api/"
    "--cqc_provider_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=providers-api/"
    "--pir_source"          = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir/"
    "--destination"         = ""
  }
}

module "worker_tracking_job" {
  source          = "../modules/glue-job"
  script_name     = "worker_tracking.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--source_start_workplace_file" = "s3://sfc-main-datasets/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2021/month=03/day=31/import_date=20210331/"
    "--source_start_worker_file" = "s3://sfc-main-datasets /domain=ASCWDS/dataset=worker/version=0.0.1/year=2021/month=03/day=31/import_date=20210331/"
    "--source_end_workplace_file" = "s3://sfc-main-datasets/domain=ASCWDS/dataset=workplace/version=1.0.0/year=2022/month=04/day=01/import_date=20220401/"
    "--source_end_worker_file" = "s3://sfc-main-datasets /domain=ASCWDS/dataset=worker/version=1.0.0/year=2022/month=04/day=01/import_date=20220401/"
    "--destination" = "s3://skillsforcare/test_worker_tracking_2021_to_2022/"
  }
}

module "job_role_breakdown_job" {
  source          = "../modules/glue-job"
  script_name     = "job_role_breakdown.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

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
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--prepared_locations_source" = ""
    "--destination"               = ""
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
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}

module "data_engineering_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "data_engineering"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}

module "cqc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "CQC"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  schedule                     = "cron(00 07 * * ? *)"
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}
