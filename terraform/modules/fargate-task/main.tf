resource "aws_ecs_cluster" "polars_cluster" {
  name = "${local.workspace_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_task_definition" "ecs_task" {
  family                   = "${local.workspace_prefix}-${var.task_name}-task"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu_size
  memory                   = var.ram_size
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name        = "${var.task_name}-container",
      image       = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com/${var.ecr_repo_name}:${var.tag_name}",
      essential   = true,
      cpu         = var.cpu_size,
      memory      = var.ram_size,
      environment = var.environment
      logConfiguration = {
        logDriver = "awslogs",
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ecs_task_log_group.name,
          "awslogs-region"        = var.region,
          "awslogs-stream-prefix" = "ecs"
        }
      }
      command = ["default"]
    }
  ])

}

resource "aws_cloudwatch_log_group" "ecs_task_log_group" {
  name              = "/ecs/${local.workspace_prefix}-${var.task_name}-task-logs"
  retention_in_days = 7
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "map-public-ip-on-launch"
    values = ["true"]
  }
}

resource "aws_security_group" "ecs_task_sg" {
  name_prefix = "${var.task_name}-ecs-sg-"
  description = "Security group for ECS tasks"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"] # Allow all outbound traffic (needed for CloudWatch/ECR)
  }
}