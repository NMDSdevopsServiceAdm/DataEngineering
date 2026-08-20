module "pipeline_resources" {
  source                  = "../modules/s3-bucket"
  bucket_name             = "${local.workspace_prefix}-pipeline-resources"
  empty_bucket_on_destroy = local.is_development_environment
  enable_versioning       = !local.is_development_environment # only version main
}

module "datasets_bucket" {
  source                  = "../modules/s3-bucket"
  bucket_name             = "${local.workspace_prefix}-datasets"
  empty_bucket_on_destroy = local.is_development_environment
  enable_versioning       = !local.is_development_environment # only version main
}

resource "aws_s3_bucket_policy" "cross_account_access_read_only" {
  count = local.workspace_prefix == "main" ? 1 : 0

  bucket = module.datasets_bucket.bucket_name
  policy = file("policy-documents/sfc-main-datasets.cross-account-access.json")
}

module "sample_raw_bucket" {
  count = local.workspace_prefix == "main" ? 1 : 0

  source                  = "../modules/s3-bucket"
  bucket_name             = "${local.workspace_prefix}-sample-raw-data"
  empty_bucket_on_destroy = false
  enable_versioning       = false
}

resource "aws_s3_bucket_policy" "sample_raw_bucket_cross_account_read_only" {
  count = local.workspace_prefix == "main" ? 1 : 0

  bucket = module.sample_raw_bucket[0].bucket_name
  policy = file("policy-documents/sfc-main-sample-raw-data.cross-account-access.json")
}
