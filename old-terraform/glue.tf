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
          "arn:aws:s3:::skillsforcare/*",

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

resource "aws_glue_crawler" "aws_glue_crawler_data_engineering" {
  database_name = var.glue_db_name
  name          = "${var.glue_db_crawler_prepend}DATA_ENGINEERING"
  role          = aws_iam_role.glue_service_iam_role.arn
  s3_target {
    path = var.data_engineering_data_location
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
    "--pir_source"          = ""
    "--destination"         = ""
  }
}

resource "aws_glue_job" "job_role_breakdown_job" {
  name              = "job_role_breakdown_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }
  command {
    script_location = "${var.scripts_location}job_role_breakdown.py"
  }

  default_arguments = {
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--TempDir"              = var.glue_temp_dir
    "--job_estimates_source" = ""
    "--worker_source"        = ""
    "--destination"          = ""
  }
}

resource "aws_glue_job" "ethnicity_breakdown_job" {
  name              = "ethnicity_breakdown_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }
  command {
    script_location = "${var.scripts_location}ethnicity_breakdown.py"
  }

  default_arguments = {
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--TempDir"                       = var.glue_temp_dir
    "--job_roles_per_location_source" = ""
    "--cqc_locations_prepared_source" = ""
    "--ons_source"                    = ""
    "--worker_source"                 = ""
    "--census_source"                 = ""
    "--destination"                   = ""
  }
}

resource "aws_glue_job" "estimate_2021_jobs_job" {
  name              = "estimate_2021_jobs_job"
  role_arn          = aws_iam_role.glue_service_iam_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  execution_property {
    max_concurrent_runs = 5
  }

  command {
    script_location = "${var.scripts_location}estimate_2021_jobs.py"
  }

  default_arguments = {
    "--extra-py-files" : "s3://sfc-data-engineering/scripts/dependencies/dependencies.zip"
    "--TempDir"                   = var.glue_temp_dir
    "--prepared_locations_source" = ""
    "--destination"               = ""
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
  schedule = "cron(30 01 01 * ? *)"
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
  schedule = "cron(30 01 01 * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.bulk_cqc_locations_download_job.name
  }
}
