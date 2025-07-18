data "archive_file" "error_notification_lambda" {
  type        = "zip"
  source_dir  = "../../lambdas/error_notifications"
  output_path = "../../lambdas/error_notifications.zip"
}

resource "aws_s3_object" "error_notification_lambda" {
  bucket      = module.pipeline_resources.bucket_name
  key         = "lambda-scripts/error_notifications.py"
  source      = data.archive_file.error_notification_lambda.output_path
  acl         = "private"
  source_hash = data.archive_file.error_notification_lambda.output_base64sha256
}

resource "aws_lambda_function" "error_notification_lambda" {
  role             = aws_iam_role.error_notification_lambda.arn
  handler          = "error_notifications.main"
  runtime          = "python3.9"
  function_name    = "${local.workspace_prefix}-glue-failure-notification"
  s3_bucket        = module.pipeline_resources.bucket_name
  s3_key           = aws_s3_object.error_notification_lambda.key
  source_code_hash = data.archive_file.error_notification_lambda.output_base64sha256

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.pipeline_failures.arn
    }
  }
}

data "aws_iam_policy_document" "error_notification_lambda_assume_role" {
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

resource "aws_iam_role" "error_notification_lambda" {
  name               = "${local.workspace_prefix}-error-notification-lambda"
  assume_role_policy = data.aws_iam_policy_document.error_notification_lambda_assume_role.json
}

data "aws_iam_policy_document" "error_notification_lambda" {
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
      aws_sns_topic.pipeline_failures.arn
    ]
  }
}

resource "aws_iam_policy" "error_notification_lambda" {
  name   = "${local.workspace_prefix}-error-notification-lambda"
  policy = data.aws_iam_policy_document.error_notification_lambda.json
}

resource "aws_iam_role_policy_attachment" "error_notification_lambda" {
  role       = aws_iam_role.error_notification_lambda.name
  policy_arn = aws_iam_policy.error_notification_lambda.arn
}

