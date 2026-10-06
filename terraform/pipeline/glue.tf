resource "aws_glue_catalog_database" "glue_catalog_database" {
  name        = "${local.workspace_prefix}-${var.glue_database_name}"
  description = "Database for all datasets belonging to the ${local.workspace_prefix} environment."
}

module "flatten_cqc_ratings_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_02_sfc_internal/cqc_ratings/jobs"
  script_name     = "flatten_cqc_ratings.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_full_snapshot_source"       = "${module.datasets_bucket.bucket_uri}/domain=01_cqc/dataset=locations_04_latest_snapshot/"
    "--cqc_locations_api_delta_source" = "${module.datasets_bucket.bucket_uri}/domain=01_cqc/dataset=locations_01_delta_api/version=3.1.7/"
    "--ascwds_workplace_source"        = "${module.datasets_bucket.bucket_uri}/domain=01_ascwds/dataset=workplace/"
    "--cqc_ratings_destination"        = "${module.datasets_bucket.bucket_uri}/domain=02_sfc/dataset=sfc_cqc_ratings_for_data_requests/"
    "--benchmark_ratings_destination"  = "${module.datasets_bucket.bucket_uri}/domain=02_sfc/dataset=sfc_cqc_ratings_for_benchmarks/version=2.0.0/"
  }
}

module "ascwds_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "01_ascwds"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "ind_cqc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "03_ind_cqc"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "publication_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "99_publication"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "cqc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "01_cqc"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "sfc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "02_sfc"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "ons_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "01_ons"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
  exclusions                   = ["dataset=postcode-directory-field-lookups/**"]
}

module "dpr_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "04_dpr"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "capacity_tracker_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "01_capacity_tracker"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}
