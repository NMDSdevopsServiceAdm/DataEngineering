provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region
}

resource "aws_s3_bucket" "data_engineering_bucket" {
  bucket = var.bucket_name
  acl    = var.acl_value
}

resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name = var.glue_db_name
}

resource "aws_glue_crawler" "aws_glue_crawler" {
  database_name = var.glue_db_name
  name          = var.glue_db_crawler_name
  role          = var.glue_iam_role

  s3_target {
    path = "s3://${var.bucket_name}"
  }
}
