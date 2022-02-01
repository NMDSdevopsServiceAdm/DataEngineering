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
  name               = "AWSGlueServiceRole-data-engineering"
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

resource "aws_glue_crawler" "aws_glue_crawler_ascwds" {
  database_name = var.glue_db_name
  name          = "${var.glue_db_crawler_prepend}ASCWDS"
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

resource "aws_glue_crawler" "aws_glue_crawler_cqc" {
  database_name = var.glue_db_name
  name          = "${var.glue_db_crawler_prepend}CQC"
  role          = aws_iam_role.glue_service_iam_role.arn
  schedule      = "cron(00 07 * * ? *)"
  s3_target {
    path = var.cqc_data_location
  }
  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  schema_change_policy {
    delete_behavior = "DEPRECATE_IN_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_glue_job" "csv_to_parquet_job" {
  name              = "csv_to_parquet_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }
  command {
    script_location = "${var.scripts_location}csv_to_parquet.py"
  }

  default_arguments = {
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--TempDir"     = var.glue_temp_dir
    "--source"      = ""
    "--destination" = ""
    "--delimiter"   = "|"
  }
}

resource "aws_glue_job" "ingest_ascwds_dataset" {
  name              = "ingest_ascwds_dataset_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }

  command {
    script_location = "${var.scripts_location}ingest_ascwds_dataset.py"
  }

  default_arguments = {
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--TempDir"     = var.glue_temp_dir
    "--source"      = ""
    "--destination" = ""
    "--delimiter"   = "|"
  }
}

resource "aws_glue_job" "prepare_locations_job" {
  name              = "prepare_locations_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }

  command {
    script_location = "${var.scripts_location}prepare_locations.py"
  }

  default_arguments = {
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--TempDir"             = var.glue_temp_dir
    "--workplace_source"    = ""
    "--cqc_location_source" = ""
    "--cqc_provider_source" = ""
    "--destination"         = ""
  }
}

resource "aws_glue_job" "bulk_cqc_providers_download_job" {
  name              = "bulk_cqc_providers_download_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 1
  }

  command {
    script_location = "${var.scripts_location}bulk_download_cqc_providers.py"
  }

  default_arguments = {
    "--TempDir" = var.glue_temp_dir
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--additional-python-modules" : "ratelimit==2.2.1,"
  }
}

resource "aws_glue_trigger" "monthly_bulk_download_providers_trigger" {
  name     = "monthly_bulk_download_providers_trigger"
  schedule = "cron(30 01 04 * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.bulk_cqc_providers_download_job.name
  }
}

resource "aws_glue_job" "bulk_cqc_locations_download_job" {
  name              = "bulk_cqc_locations_download_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 1
  }

  command {
    script_location = "${var.scripts_location}bulk_download_cqc_locations.py"
  }

  default_arguments = {
    "--TempDir" = var.glue_temp_dir
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--additional-python-modules" : "ratelimit==2.2.1,"
  }
}

resource "aws_glue_trigger" "monthly_bulk_download_locations_trigger" {
  name     = "monthly_bulk_download_locations_trigger"
  schedule = "cron(30 01 05 * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.bulk_cqc_locations_download_job.name
  }
}

# --- Sagemaker --- #
resource "aws_iam_role" "notebook_iam_role" {
  name               = "sm-notebook-iam-role-data-engineering"
  assume_role_policy = data.aws_iam_policy_document.sagemaker_iam_policy.json
  path               = "/service-role/"
}

data "aws_iam_policy_document" "sagemaker_iam_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy_attachment" "sm_notebook_policy_attachment" {
  name       = "sm-notebook-policy-attachment"
  roles      = [aws_iam_role.notebook_iam_role.name]
  policy_arn = aws_iam_policy.sagemaker_data_engineering_policy.arn
}

resource "aws_iam_policy" "sagemaker_data_engineering_policy" {
  name        = "sagemaker_data_engineering_policy"
  path        = "/"
  description = "The iam policy for sagemaker service - utilised by jupyter notebooks."

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Action" : [
          "s3:ListBucket"
        ],
        "Effect" : "Allow",
        "Resource" : [
          "arn:aws:s3:::sfc-data-engineering*",
          "arn:aws:s3:::aws-glue*",
          "arn:aws:s3:::aws-glue-jes-prod-eu-west-2-assets"
        ]
      },
      {
        "Action" : [
          "s3:GetObject"
        ],
        "Effect" : "Allow",
        "Resource" : [
          "arn:aws:s3:::sfc-data-engineering*",
          "arn:aws:s3:::aws-glue*",
          "arn:aws:s3:::aws-glue-jes-prod-eu-west-2-assets*"
        ]
      },
      {
        "Action" : [
          "logs:CreateLogStream",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents",
          "logs:CreateLogGroup"
        ],
        "Effect" : "Allow",
        "Resource" : [
          "arn:aws:logs:eu-west-2:344210435447:log-group:/aws/sagemaker/*",
          "arn:aws:logs:eu-west-2:344210435447:log-group:/aws/sagemaker/*:log-stream:aws-glue-*"
        ]
      },
      {
        "Action" : [
          "glue:UpdateDevEndpoint",
          "glue:GetDevEndpoint",
          "glue:GetDevEndpoints"
        ],
        "Effect" : "Allow",
        "Resource" : [
          "arn:aws:glue:eu-west-2:*"
        ]
      },
      {
        "Action" : [
          "sagemaker:ListTags"
        ],
        "Effect" : "Allow",
        "Resource" : [
          "arn:aws:sagemaker:eu-west-2:*"
        ]
      }
    ]
  })
}
