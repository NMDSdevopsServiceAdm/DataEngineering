data "aws_s3_bucket" "datasets_bucket" {
  bucket = var.dataset_bucket_name
}

data "aws_caller_identity" "current" {}

data "aws_sns_topic" "pipeline_failures" {
  name = "pipeline_failures"
}

data "aws_lambda_function" "error_notification_lambda" {
  function_name = "error_notification_lambda"
}

data "aws_ssm_parameter" "providers_last_run" {
  name = "providers_last_run"
}

data "aws_ssm_parameter" "locations_last_run" {
  name = "locations_last_run"
}

data "aws_ecs_cluster" "polars_cluster" {
  cluster_name = "polars_cluster"
}

resource "aws_iam_role" "step_function_iam_role" {
  name               = "${local.workspace_prefix}-AWSStepFunction-role"
  assume_role_policy = data.aws_iam_policy_document.step_function_iam_policy.json
}


resource "aws_iam_policy" "step_function_iam_policy" {
  name        = "${local.workspace_prefix}-step_function_iam_policy"
  path        = "/"
  description = "IAM Policy for step functions"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ],
        "Resource" : [
          "*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:StartCrawler",
          "glue:StartJobRun",
          "glue:GetJobRun"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "states:StartExecution",
          "states:ListExecutions"
        ],
        "Resource" : [
          "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:stateMachine:*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "states:DescribeExecution",
          "states:StopExecution"
        ],
        "Resource" : [
          "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:execution:*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
        ],
        "Resource" : "arn:aws:events:eu-west-2:${data.aws_caller_identity.current.account_id}:rule/StepFunctions*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "SNS:Publish"
        ],
        "Resource" : "${data.aws_sns_topic.pipeline_failures.arn}"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "lambda:InvokeFunction"
        ],
        "Resource" : [
          "${data.aws_lambda_function.error_notification_lambda.arn}*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetObject",
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ],
        "Resource" : [
          "${data.aws_s3_bucket.datasets_bucket}/*",
          data.aws_s3_bucket.datasets_bucket
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ssm:PutParameter",
          "ssm:GetParameter",
        ],
        "Resource" : [
          data.aws_ssm_parameter.providers_last_run.arn,
          data.aws_ssm_parameter.locations_last_run.arn
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ecs:RunTask"
        ],
        "Resource" : "*",
        "Condition" : {
          "ArnEquals" : { "ecs:cluster" : data.aws_ecs_cluster.polars_cluster.arn }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : "iam:PassRole",
        "Resource" : "*",
        "Condition" : {
          "ArnEquals" : { "ecs:cluster" : data.aws_ecs_cluster.polars_cluster.arn }
          "StringLike" : {
            "iam:PassedToService" = [
              "ecs-tasks.amazonaws.com",
              "events.amazonaws.com"
            ]
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "AWSStepFunctionRole_data_engineering_policy_attachment" {
  policy_arn = aws_iam_policy.step_function_iam_policy.arn
  role       = aws_iam_role.step_function_iam_role.name
}

data "aws_iam_policy_document" "step_function_iam_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}
