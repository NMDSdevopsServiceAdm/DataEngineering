module "pipeline_resources" {
  source      = "../modules/s3-bucket"
  bucket_name = "pipeline-resources"
}

module "datasets_bucket" {
  source      = "../modules/s3-bucket"
  bucket_name = "datasets"
}

module "adams_test_bucket" {
  source      = "../modules/s3-bucket"
  bucket_name = "adams_test_bucket"
}
