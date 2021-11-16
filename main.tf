provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region
}


resource "aws_s3_bucket" "data_engineering_bucket" {
  bucket = var.bucket_name
  acl    = var.acl_value
}
