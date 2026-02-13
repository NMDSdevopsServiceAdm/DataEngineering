locals {
  workspace_prefix = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
}

data "archive_file" "lambda_notification" {
  type        = "zip"
  source_dir  = "../../lambdas/${var.lambda_name}"
  output_path = "../../lambdas/${var.lambda_name}.zip"
}

resource "aws_s3_object" "lambda_notification" {
  bucket      = var.resource_bucket
  key         = "lambda-scripts/${var.lambda_name}.py"
  source      = data.archive_file.lambda_notification.output_path
  acl         = "private"
  source_hash = data.archive_file.lambda_notification.output_base64sha256
}

data "aws_iam_policy_document" "lambda_notification_assume_role" {
  statement {
    actions = [
      "sts:AssumeRole"
    ]
    principals {
      identifiers = [
        "lambda.amazonaws.com"
      ]
      type = "Service"
    }
  }
}

resource "aws_iam_role" "lambda_notification" {
  name               = "${local.workspace_prefix}-${var.lambda_name}-lambda_notification"
  assume_role_policy = data.aws_iam_policy_document.lambda_notification_assume_role.json
}

data "aws_iam_policy_document" "lambda_notification_policy" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:logs:*:*:*"
    ]
  }

  statement {
    actions = [
      "states:SendTaskSuccess",
      "states:SendTaskFailure"
    ]
    effect = "Allow"
    resources = [
      "*"
    ]
  }

  statement {
    actions = [
      "SNS:ListTopics",
      "SNS:ListTagsForResource"
    ]
    effect = "Allow"
    resources = [
      "*"
    ]
  }

  statement {
    actions = [
      "SNS:Publish"
    ]
    effect = "Allow"
    resources = [
      var.sns_topic_arn
    ]
  }
}

resource "aws_iam_policy" "lambda_notification" {
  name   = "${local.workspace_prefix}-${var.lambda_name}-lambda"
  policy = data.aws_iam_policy_document.lambda_notification_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_notification" {
  role       = aws_iam_role.lambda_notification.name
  policy_arn = aws_iam_policy.lambda_notification.arn
}

resource "aws_lambda_function" "lambda_notification" {
  role             = aws_iam_role.lambda_notification.arn
  handler          = "${var.lambda_name}.main"
  runtime          = "python3.11"
  timeout          = 15
  function_name    = "${local.workspace_prefix}-notification-lambda"
  s3_bucket        = var.resource_bucket
  s3_key           = aws_s3_object.lambda_notification.key
  source_code_hash = data.archive_file.lambda_notification.output_base64sha256

  environment {
    variables = {
      SNS_TOPIC_ARN = var.sns_topic_arn
    }
  }
}

resource "aws_cloudwatch_log_group" "lambda_notification" {
  name              = "/aws/lambda/${aws_lambda_function.lambda_notification.function_name}"
  retention_in_days = 30
}
