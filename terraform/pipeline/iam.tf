resource "aws_iam_role" "sfc_glue_service_iam_role" {
  name               = "${terraform.workspace}-glue_service_iam_role"
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
  name        = "${terraform.workspace}-logging_policy"
  path        = "/"
  description = "Iam logging policy crawlers on ${terraform.workspace} environment"

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
  name        = "${terraform.workspace}-glue_job_bucket_access_policy"
  path        = "/"
  description = "Iam policy for the all glue jobs on workspace: ${terraform.workspace}"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:*"
        ],
        "Resource" : [
          "arn:aws:s3:::${module.pipeline_resources.bucket_name}/*",
          "arn:aws:s3:::${module.datasets_bucket.bucket_name}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_job_s3_policy_attachment" {
  policy_arn = aws_iam_policy.glue_job_s3_data_engineering_policy.arn
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}


