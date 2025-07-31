resource "aws_ecs_task_definition" "polars_task_definition" {
  family                   = "${local.resource_prefix}-task"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "4096"
  memory                   = "16384"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "${local.resource_prefix}-container",
      image     = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com/${var.ecr_repo_name}:latest",
      essential = true,
      cpu       = 4096,
      memory    = 16384,
      environment = [
        { "name" = "AWS_REGION", "value" = var.region },
        { "name" = "CQC_SECRET_NAME", "value" = var.secret_name }
      ]
      logConfiguration = {
        logDriver = "awslogs",
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ecs_task_log_group.name,
          "awslogs-region"        = var.region,
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

}