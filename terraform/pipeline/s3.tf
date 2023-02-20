module "pipeline_resources" {
  source                  = "../modules/s3-bucket"
  bucket_name             = "${local.workspace_prefix}-pipeline-resources"
  empty_bucket_on_destroy = local.is_development_environment
}

module "datasets_bucket" {
  source                  = "../modules/s3-bucket"
  bucket_name             = "${local.workspace_prefix}-datasets"
  empty_bucket_on_destroy = local.is_development_environment
}