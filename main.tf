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

resource "aws_iam_policy" "glue_service_data_engineering_policy" {
  name        = "glue_service_data_engineering_policy"
  path        = "/"
  description = "The iam policy for the glue service"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        "Resource" : [
          "arn:aws:s3:::sfc-data-engineering/*",
          "arn:aws:s3:::sfc-data-engineering-raw/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "AWSGlueServiceRole_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.glue_service_iam_role.name
}

resource "aws_iam_role_policy_attachment" "AWSGlueDataBrewServiceRole_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueDataBrewServiceRole"
  role       = aws_iam_role.glue_service_iam_role.name
}


resource "aws_iam_role_policy_attachment" "glue_service_data_engineering_policy_attachment" {
  policy_arn = aws_iam_policy.glue_service_data_engineering_policy.arn
  role       = aws_iam_role.glue_service_iam_role.name
}
resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name = var.glue_db_name
}

resource "aws_glue_crawler" "aws_glue_crawler" {
  database_name = var.glue_db_name
  name          = var.glue_db_crawler_name
  role          = aws_iam_role.glue_service_iam_role.arn


  s3_target {
    path = var.ascwds_data_location
  }

  configuration = jsonencode(
    {
      "Version" : 1.0,
      "Grouping" = {
        "TableLevelConfiguration" = 3,
        "TableGroupingPolicy" : "CombineCompatibleSchemas"
      }
    }
  )
}

resource "aws_glue_dev_endpoint" "glue_dev_endpoint" {
  name     = "data-engineering-dev-endpoint"
  role_arn = aws_iam_role.glue_service_iam_role.arn
}

resource "aws_glue_job" "csv_to_parquet_job" {
  name              = "csv_to_parquet_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "3.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }
  command {
    script_location = "${var.scripts_location}csv_to_parquet.py"

  }

  default_arguments = {
    "--TempDir"     = var.glue_temp_dir
    "--source"      = ""
    "--destination" = ""
    "--delimiter"   = "|"
  }
}

resource "aws_glue_job" "format_fields_job" {
  name              = "format_fields_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "3.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }

  command {
    script_location = "${var.scripts_location}format_fields.py"
  }

  default_arguments = {
    "--TempDir"     = var.glue_temp_dir
    "--source"      = ""
    "--destination" = ""
  }
}




