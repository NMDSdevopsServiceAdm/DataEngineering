resource "aws_iam_role" "ecs_task_execution_role" {
  name_prefix = "${local.resource_prefix}-ecs-exec-role-"

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
  name_prefix = "${local.resource_prefix}-ecs-role-"

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


resource "aws_iam_role_policy_attachment" "ecs_task_role_policy" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}

resource "aws_iam_policy" "s3_read_write_policy" {
  name        = "${local.resource_prefix}-s3-read-write-policy"
  description = "IAM policy for S3 read/write on specific buckets."
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowS3ReadWriteOnSpecificBuckets",
        Effect = "Allow",
        Action = [
          "s3:GetObject*",
          "s3:PutObject*",
          "s3:ListObjects"
        ],
        Resource = [
          "arn:aws:s3:::spike-polars-data/*",
          "arn:aws:s3:::spike-polars-data"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "secretsmanager_read_policy" {
  name        = "${local.resource_prefix}-secretsmanager-read-policy"
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
        Resource = var.secret_name,
      }
    ]
  })
}


