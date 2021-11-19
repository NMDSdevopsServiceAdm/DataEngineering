provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region
}

# --- S3 --- #

resource "aws_s3_bucket" "data_engineering_bucket" {
  bucket = var.bucket_name
  acl    = var.acl_value
}

# --- Glue --- #

resource "aws_iam_role" "glue_service_iam_role" {
  name               = "AWSGlueServiceRole-data-engineerng"
  assume_role_policy = data.aws_iam_policy_document.glue_service_iam_policy.json
}

data "aws_iam_policy_document" "glue_service_iam_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "example-AWSGlueServiceRole" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.glue_service_iam_role.name
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

resource "aws_glue_dev_endpoint" "glue_dev_endpoint" {
  name     = "data-engineering-dev-endpoint"
  role_arn = aws_iam_role.glue_service_iam_role.arn
}



