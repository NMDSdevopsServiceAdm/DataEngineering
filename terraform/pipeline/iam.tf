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

data "aws_secretsmanager_secret" "cqc_api_primary_key" {
  name = var.secret_name
}

resource "aws_iam_policy" "retrieve_cqc_api_primary_key_secret" {
  name        = "${terraform.workspace}-retrieve-cqc-api-primary-key-secret"
  path        = "/"
  description = "Retrieves a secret specific to the Bulk download jobs"

  policy = templatefile("policy-documents/retrieve-specific-secret.json", { secret_arn = data.aws_secretsmanager_secret.cqc_api_primary_key.arn })
}

resource "aws_iam_group" "can_assume_admin_role" {
  count = local.workspace_prefix == "main" ? 1 : 0
  name  = "CanAssumeAdminRole"
}

resource "aws_iam_group_policy" "can_assume_admin_role_access_policy" {
  count  = local.workspace_prefix == "main" ? 1 : 0
  group  = aws_iam_group.can_assume_admin_role[0].name
  policy = data.aws_iam_policy_document.admin_role_access_policy[0].json
}

data "aws_iam_policy_document" "admin_role_access_policy" {
  count = local.workspace_prefix == "main" ? 1 : 0
  statement {
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.admin_role[0].arn]
  }
}

data "aws_iam_policy_document" "admin_role_assume_role_policy" {
  count = local.workspace_prefix == "main" ? 1 : 0
  statement {
    actions = ["sts:AssumeRole"]

    # This is a trust policy against the root user, it does not allow every user access to the role, they still need the `sts:AssumeRole` permission against their user
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::344210435447:root"]
    }
  }
}

resource "aws_iam_role" "admin_role" {
  count              = local.workspace_prefix == "main" ? 1 : 0
  name               = "Admin"
  assume_role_policy = data.aws_iam_policy_document.admin_role_assume_role_policy[0].json
}

resource "aws_iam_role_policy_attachment" "admin_role_administrator_access_policy_attach" {
  role       = aws_iam_role.admin_role.arn
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}
