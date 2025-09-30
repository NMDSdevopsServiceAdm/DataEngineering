resource "aws_iam_role" "ecs_task_execution_role" {
  name_prefix = "${local.workspace_prefix}-ecs-exec-role-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task_role" {
  name_prefix = "${local.workspace_prefix}-ecs-role-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "ecs_task_role_policy_attach_s3" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}

resource "aws_iam_policy" "s3_read_write_policy" {
  name_prefix = "${local.workspace_prefix}-s3-read-write-policy-"
  description = "IAM policy for S3 read/write on specific buckets."
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowS3ReadWriteOnSpecificBuckets",
        Effect = "Allow",
        Action = [
          "s3:Get*",
          "s3:PutObject*",
          "s3:List*"
        ],
        Resource = [
          "arn:aws:s3:::sfc-${local.workspace_prefix}-datasets/*",
          "arn:aws:s3:::sfc-${local.workspace_prefix}-datasets"
        ]
      }
    ]
  })
}

data "aws_secretsmanager_secret" "cqc_api_primary_key" {
  name = var.secret_name
}

resource "aws_iam_policy" "secretsmanager_read_policy" {
  name_prefix = "${local.workspace_prefix}-secretsmanager-read-policy-"
  description = "IAM policy for Secrets Manager read access to a specific secret."
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowSecretsManagerReadSpecificSecret",
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue"
        ],
        Resource = data.aws_secretsmanager_secret.cqc_api_primary_key.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_role_policy_read_secr" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.secretsmanager_read_policy.arn
}

resource "aws_iam_role" "sfn_execution_role" {
  name_prefix = "${local.workspace_prefix}-sfn-exec-role-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "states.${var.region}.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "sfn_ecs_policy" {
  name_prefix = "${local.workspace_prefix}-sfn-ecs-policy-"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "ecs:RunTask"
        ],
        Resource = [
          aws_ecs_task_definition.polars_task.arn,
          var.cluster_arn
        ]
      },
      {
        Effect = "Allow",
        Action = "iam:PassRole",
        Resource = [
          aws_iam_role.ecs_task_execution_role.arn,
          aws_iam_role.ecs_task_execution_role.arn,
          aws_iam_role.ecs_task_role.arn
        ],
        Condition = {
          StringLike = {
            "iam:PassedToService" = [
              "ecs-tasks.amazonaws.com",
              "events.amazonaws.com"
            ]
          }
        }
      },
      {
        Effect = "Allow",
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule"
        ],
        Resource = [
          "arn:aws:events:${var.region}:${local.account_id}:rule/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "states:StartExecution",
          "states:StopExecution",
          "states:DescribeExecution",
          "states:ListExecutions"
        ],
        Resource = [
          "*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/stepfunctions/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sfn_ecs_policy_attachment" {
  role       = aws_iam_role.sfn_execution_role.name
  policy_arn = aws_iam_policy.sfn_ecs_policy.arn
}
