resource "aws_s3_bucket" "data_engineering_bucket" {
  bucket = var.bucket_name
  acl    = var.acl_value
}
