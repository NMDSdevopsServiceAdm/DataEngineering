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
  timeout          = 15
  function_name    = "${local.workspace_prefix}-job-failure-notification"
  s3_bucket        = module.pipeline_resources.bucket_name
  s3_key           = aws_s3_object.error_notification_lambda.key
  source_code_hash = data.archive_file.error_notification_lambda.output_base64sha256

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.pipeline_failures.arn
    }
  }
}

resource "aws_lambda_function" "create_snapshot_lambda" {
  role          = aws_iam_role.create_snapshot_lambda.arn
  function_name = "${local.workspace_prefix}-create-full-snapshot"
  package_type  = "Image"
  image_uri     = "${data.aws_caller_identity.current.account_id}.dkr.ecr.eu-west-2.amazonaws.com/lambda/create-snapshot@${data.aws_ecr_image.create_dataset_snapshot.image_digest}"
  memory_size   = 10240
  timeout       = 300
}

resource "aws_lambda_function" "check_datasets_equal" {
  role          = aws_iam_role.check_datasets_equal.arn
  function_name = "${local.workspace_prefix}-check-datasets-equal"
  package_type  = "Image"
  image_uri     = "${data.aws_caller_identity.current.account_id}.dkr.ecr.eu-west-2.amazonaws.com/lambda/check-datasets-equal@${data.aws_ecr_image.check_datasets_equal.image_digest}"
  memory_size   = 2048
  timeout       = 60
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

data "aws_iam_policy_document" "create_snapshot_lambda_assume_role" {
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

data "aws_iam_policy_document" "check_datasets_equal_assume_role" {
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

resource "aws_iam_role" "create_snapshot_lambda" {
  name               = "${local.workspace_prefix}-create-snapshot-lambda"
  assume_role_policy = data.aws_iam_policy_document.create_snapshot_lambda_assume_role.json
}

resource "aws_iam_role" "check_datasets_equal" {
  name               = "${local.workspace_prefix}-check-datasets-equal"
  assume_role_policy = data.aws_iam_policy_document.check_datasets_equal_assume_role.json
}


data "aws_iam_policy_document" "create_snapshot_lambda" {
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
      "s3:Get*",
      "s3:List*",
      "s3:Describe*"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "s3:Put*"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::sfc-${local.workspace_prefix}-datasets/domain=CQC_delta/dataset=full_providers_api/version=3.0.0/*",
      "arn:aws:s3:::sfc-${local.workspace_prefix}-datasets/domain=CQC_delta/dataset=full_locations_api/version=3.0.0/*",
      "arn:aws:s3:::sfc-${local.workspace_prefix}-datasets/domain=CQC_delta/dataset=full_locations_api_cleaned/*"
    ]
  }
}


data "aws_iam_policy_document" "check_datasets_equal" {
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
      "s3:Get*",
      "s3:List*",
      "s3:Describe*"
    ]
    effect    = "Allow"
    resources = ["*"]
  }
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

resource "aws_iam_policy" "create_snapshot_lambda" {
  name   = "${local.workspace_prefix}-create-snapshot-lambda"
  policy = data.aws_iam_policy_document.create_snapshot_lambda.json
}

resource "aws_iam_policy" "check_datasets_equal" {
  name   = "${local.workspace_prefix}-check-datasets-equal"
  policy = data.aws_iam_policy_document.check_datasets_equal.json
}

resource "aws_iam_role_policy_attachment" "error_notification_lambda" {
  role       = aws_iam_role.error_notification_lambda.name
  policy_arn = aws_iam_policy.error_notification_lambda.arn
}

resource "aws_iam_role_policy_attachment" "create_snapshot_lambda" {
  role       = aws_iam_role.create_snapshot_lambda.name
  policy_arn = aws_iam_policy.create_snapshot_lambda.arn
}

resource "aws_iam_role_policy_attachment" "check_datasets_equal" {
  role       = aws_iam_role.check_datasets_equal.name
  policy_arn = aws_iam_policy.check_datasets_equal.arn
}
