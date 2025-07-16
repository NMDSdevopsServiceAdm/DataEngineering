resource "aws_iam_role" "sfc_glue_service_iam_role" {
  name               = "${local.workspace_prefix}-glue_service_iam_role"
  assume_role_policy = data.aws_iam_policy_document.glue_service_assume_role_policy.json
}

data "aws_iam_policy_document" "glue_service_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "AWSGlueServiceRole_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}


resource "aws_iam_policy" "glue_crawler_logging_policy" {
  name        = "${local.workspace_prefix}-logging_policy"
  path        = "/"
  description = "Iam logging policy crawlers on ${local.workspace_prefix} environment"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource" : [
          "arn:aws:logs:*:*:log-group:/aws-glue/*"
        ],
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_crawler_logging_policy_attachment" {
  policy_arn = aws_iam_policy.glue_crawler_logging_policy.arn
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}

resource "aws_iam_policy" "glue_job_s3_data_engineering_policy" {
  name        = "${local.workspace_prefix}-glue_job_bucket_access_policy"
  path        = "/"
  description = "Iam policy for the all glue jobs on workspace: ${local.workspace_prefix}"

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
          "arn:aws:s3:::${module.pipeline_resources.bucket_name}/*",
          "arn:aws:s3:::${module.datasets_bucket.bucket_name}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_job_s3_policy_attachment" {
  policy_arn = aws_iam_policy.glue_job_s3_data_engineering_policy.arn
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}

resource "aws_iam_policy" "glue_jobs_read_raw_s3_data_policy" {
  name        = "${local.workspace_prefix}-glue_job_read_raw_s3_bucket_access_policy"
  path        = "/"
  description = "Iam policy for the all glue jobs on workspace: ${local.workspace_prefix} to read the raw data"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:ListBucket",
          "s3:GetObject"
        ],
        "Resource" : [
          "arn:aws:s3:::sfc-data-engineering-raw/*",
          "arn:aws:s3:::sfc-main-datasets/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_jobs_read_raw_s3_data_policy_attachment" {
  policy_arn = aws_iam_policy.glue_jobs_read_raw_s3_data_policy.arn
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}

resource "aws_iam_policy" "query_all_in_athena" {
  name        = "${terraform.workspace}-query-all-in-athena"
  path        = "/"
  description = "All read and list privileges for any athena resources"

  policy = templatefile("policy-documents/query-all-in-athena.json", { account_id = data.aws_caller_identity.current.account_id })
}

resource "aws_iam_role_policy_attachment" "glue_job_cqc_api_primary_key_secrets_attachment" {
  policy_arn = aws_iam_policy.retrieve_cqc_api_primary_key_secret.arn
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}

resource "aws_iam_policy" "retrieve_cqc_api_primary_key_secret" {
  name        = "${terraform.workspace}-retrieve-cqc-api-primary-key-secret"
  path        = "/"
  description = "Retrieves a secret specific to the Bulk download jobs"

  policy = templatefile("policy-documents/retrieve-specific-secret.json", { secret_arn = local.cqc_api_primary_key_secret_arn })
}

resource "aws_iam_policy" "glue_get_job_runs" {
  name        = "${terraform.workspace}-glue-get-job-runs"
  path        = "/"
  description = "Retrieves the job runs for specific glue jobs"

  policy = templatefile("policy-documents/glue-get-job-runs.json")
}

resource "aws_iam_role_policy_attachment" "glue_get_job_runs_attachment" {
  policy_arn = aws_iam_policy.glue_get_job_runs.arn
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}