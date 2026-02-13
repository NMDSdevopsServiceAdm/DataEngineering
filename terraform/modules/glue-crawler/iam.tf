data "aws_iam_policy_document" "glue_crawler_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "sfc_glue_crawler_iam_role" {
  name               = "${local.workspace_prefix}-${var.dataset_for_crawler}_crawler_iam_role"
  assume_role_policy = data.aws_iam_policy_document.glue_crawler_assume_role_policy.json
}

resource "aws_iam_policy" "glue_crawler_logging_policy" {
  name        = "${local.workspace_prefix}-${var.dataset_for_crawler}_logging_policy"
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
  role       = aws_iam_role.sfc_glue_crawler_iam_role.name
}

resource "aws_iam_policy" "glue_crawler_s3_data_engineering_policy" {
  name        = "${local.workspace_prefix}-${var.dataset_for_crawler}_bucket_access_policy"
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

resource "aws_iam_role_policy_attachment" "glue_crawler_s3_policy_attachment" {
  policy_arn = aws_iam_policy.glue_crawler_s3_data_engineering_policy.arn
  role       = aws_iam_role.sfc_glue_crawler_iam_role.name
}


resource "aws_iam_role_policy_attachment" "AWSGlueServiceRole_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.sfc_glue_crawler_iam_role.name
}
