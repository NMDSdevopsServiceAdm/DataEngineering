data "aws_caller_identity" "current" {}

resource "aws_iam_role" "step_function_iam_role" {
  name               = "${local.workspace_prefix}-${var.pipeline_name}-role"
  assume_role_policy = data.aws_iam_policy_document.step_function_iam_policy.json
}

resource "aws_iam_policy" "step_function_iam_policy" {
  name        = "${local.workspace_prefix}-${var.pipeline_name}_iam_policy"
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
        "Resource" : "arn:aws:sns:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.workspace_prefix}-pipeline-failures"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "lambda:InvokeFunction"
        ],
        "Resource" : [
          "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:error_notification_lambda"
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
          "arn:aws:s3:::${local.workspace_prefix}-datasets",
          "arn:aws:s3:::${local.workspace_prefix}-datasets/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ssm:PutParameter",
          "ssm:GetParameter",
        ],
        "Resource" : [
          "arn:aws:ssm:eu-west-2:${data.aws_caller_identity.current.account_id}:parameter/${local.workspace_prefix}/cqc-locations-last-run",
          "arn:aws:ssm:eu-west-2:${data.aws_caller_identity.current.account_id}:parameter/${local.workspace_prefix}/cqc-providers-last-run"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ecs:RunTask"
        ],
        "Resource" : [
          "arn:aws:ecs:eu-west-2:${data.aws_caller_identity.current.account_id}:task-definition/${local.workspace_prefix}-cqc-api-task:*",
          "arn:aws:ecs:eu-west-2:${data.aws_caller_identity.current.account_id}:task-definition/${local.workspace_prefix}-_03_independent_cqc-task:*",
          "arn:aws:ecs:eu-west-2:${data.aws_caller_identity.current.account_id}:task-definition/${local.workspace_prefix}-_03_independent_cqc_model-task:*",
          "arn:aws:ecs:eu-west-2:${data.aws_caller_identity.current.account_id}:cluster/${local.workspace_prefix}-cluster"
        ]
      },
      {
        Effect = "Allow",
        Action = "iam:PassRole",
        Resource = [
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/*"
        ],
        Condition = {
          StringLike = {
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
