locals {
  ecr_repository_name = "${var.name_prefix}-${var.function_name}"
  image_tag           = "latest"
}

# ECR Repository to store the Docker image
resource "aws_ecr_repository" "lambda_image" {
  name                 = local.ecr_repository_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Build and push the Docker image
resource "null_resource" "docker_image" {
  triggers = {
    docker_file = filesha256("${var.docker_context_path}/Dockerfile")
    source_dir  = sha256(join("", [for f in fileset("${var.docker_context_path}", "**") : filesha256("${var.docker_context_path}/${f}")]))
  }

  provisioner "local-exec" {
    command = <<EOF
      aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com
      cd ${var.docker_context_path}
      docker build -t ${aws_ecr_repository.lambda_image.repository_url}:${local.image_tag} .
      docker push ${aws_ecr_repository.lambda_image.repository_url}:${local.image_tag}
    EOF
  }

  depends_on = [aws_ecr_repository.lambda_image]
}

# Get the latest image digest
data "aws_ecr_image" "lambda_image" {
  repository_name = aws_ecr_repository.lambda_image.name
  image_tag       = local.image_tag
  depends_on      = [null_resource.docker_image]
}

# Lambda function
resource "aws_lambda_function" "function" {
  function_name = "${var.name_prefix}-${var.function_name}"
  role          = aws_iam_role.lambda_role.arn
  timeout       = var.timeout
  memory_size   = var.memory_size
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_image.repository_url}@${data.aws_ecr_image.lambda_image.image_digest}"

  environment {
    variables = var.environment_variables
  }
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${var.name_prefix}-${var.function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Basic Lambda execution policy
resource "aws_iam_policy" "lambda_basic" {
  name        = "${var.name_prefix}-${var.function_name}-basic"
  description = "Basic Lambda execution policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# S3 access policy
resource "aws_iam_policy" "lambda_s3" {
  count       = var.s3_access ? 1 : 0
  name        = "${var.name_prefix}-${var.function_name}-s3"
  description = "S3 access policy for Lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = var.s3_bucket_arns
      }
    ]
  })
}

# Attach policies to role
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_basic.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  count      = var.s3_access ? 1 : 0
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3[0].arn
}

# Additional policy attachments
resource "aws_iam_role_policy_attachment" "additional_policies" {
  for_each   = toset(var.additional_policy_arns)
  role       = aws_iam_role.lambda_role.name
  policy_arn = each.value
}

data "aws_caller_identity" "current" {}