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
  name               = "${local.workspace_prefix}-glue_crawler_iam_role"
  assume_role_policy = data.aws_iam_policy_document.glue_crawler_assume_role_policy.json
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
  role       = aws_iam_role.sfc_glue_crawler_iam_role.name
}

