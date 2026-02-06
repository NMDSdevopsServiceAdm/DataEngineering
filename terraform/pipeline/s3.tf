module "pipeline_resources" {
  source      = "../modules/s3-bucket"
  bucket_name = "${local.workspace_prefix}-pipeline-resources"
}

module "datasets_bucket" {
  source      = "../modules/s3-bucket"
  bucket_name = "${local.workspace_prefix}-datasets"
}

resource "aws_s3_bucket_policy" "cross_account_access_read_only" {
  count = local.is_main_environment ? 0 : 1

  bucket = module.datasets_bucket.bucket_name
  policy = file("policy-documents/sfc-main-datasets.cross-account-access.json")
}
