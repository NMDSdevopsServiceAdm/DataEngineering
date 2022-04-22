module "pipeline_resources" {
  source = "../modules/s3-bucket"
  bucket_name = "pipeline-resources"
}
