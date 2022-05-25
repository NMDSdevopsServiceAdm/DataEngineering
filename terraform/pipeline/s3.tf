module "pipeline_resources" {
  source                  = "../modules/s3-bucket"
  bucket_name             = "pipeline-resources"
  empty_bucket_on_destroy = true
}

module "datasets_bucket" {
  source                  = "../modules/s3-bucket"
  bucket_name             = "datasets"
  empty_bucket_on_destroy = false
}
