locals {
  shortened_glue_job_name = substr("${local.job_name}", 0, 55)
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

resource "aws_iam_role" "sfc_glue_service_iam_role" {
  name               = "${local.shortened_glue_job_name}_iam_role"
  assume_role_policy = data.aws_iam_policy_document.glue_service_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "AWSGlueServiceRole_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}

resource "aws_iam_policy" "glue_job_s3_data_engineering_policy" {
  name        = "${local.shortened_glue_job_name}_bucket_access_policy"
  path        = "/"
  description = "Iam policy for branch buckets on workspace: ${local.workspace_prefix}"

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
          "arn:aws:s3:::${var.resource_bucket.bucket_name}/*",
          "arn:aws:s3:::${var.datasets_bucket.bucket_name}/*"
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
  name        = "${local.workspace_prefix}-${local.job_name}_read_raw_s3_bucket_access_policy"
  path        = "/"
  description = "Iam policy for workspace: ${local.workspace_prefix} to read the raw and main data"

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