resource "aws_ecs_task_definition" "ecs_task" {
  family                   = "${local.workspace_prefix}-${var.task_name}-task"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu_size
  memory                   = var.ram_size
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn
  ephemeral_storage {
    size_in_gib = var.ephemeral_storage_size
  }

  volume {
    name = "polars_temp_storage"
    # For Fargate, leaving this empty uses the ephemeral storage
  }

  # LEAVE commented for easy future adjustment, default should be 20
  # ephemeral_storage {
  #   size_in_gib = var.ephemeral_storage_size
  # }

  volume {
    name = "polars_temp_storage"
    # For Fargate, leaving this empty uses the ephemeral storage
  }

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "${var.task_name}-container",
      image     = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com/${var.ecr_repo_name}:${var.tag_name}",
      essential = true,
      cpu       = var.cpu_size,
      memory    = var.ram_size,

      environment = concat(var.environment, [
        { name = "POLARS_TEMP_DIR", value = "/polars_scratch" },
      ])

      logConfiguration = {
        logDriver = "awslogs",
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ecs_task_log_group.name,
          "awslogs-region"        = var.region,
          "awslogs-stream-prefix" = "ecs"
        }
      }

      # Mount the volume to the path Polars expects
      mountPoints = [
        {
          sourceVolume  = "polars_temp_storage"
          containerPath = "/polars_scratch"
          readOnly      = false
        }
      ]

      command = ["default"]
    }
  ])

}
