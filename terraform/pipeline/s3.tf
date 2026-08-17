module "pipeline_resources" {
  source                  = "../modules/s3-bucket"
  bucket_name             = "${local.workspace_prefix}-pipeline-resources"
  empty_bucket_on_destroy = local.is_development_environment
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

# Per-branch equivalent of prod's hand-managed sfc-data-engineering-raw bucket, so raw
# ingest steps (and their EventBridge triggers) can be tested on a branch. Never created
# for main - prod's raw bucket stays hand-managed and untouched.
module "raw_bucket" {
  count                   = local.is_development_environment ? 1 : 0
  source                  = "../modules/s3-bucket"
  bucket_name             = "${local.workspace_prefix}-raw"
  empty_bucket_on_destroy = local.is_development_environment
  enable_versioning       = false
}

# Prod's sfc-data-engineering-raw bucket has this configured outside this repo, since the
# bucket itself is hand-managed rather than created by this Terraform.
resource "aws_s3_bucket_notification" "raw_bucket_eventbridge" {
  count       = local.is_development_environment ? 1 : 0
  bucket      = module.raw_bucket[0].bucket_name
  eventbridge = true
}
