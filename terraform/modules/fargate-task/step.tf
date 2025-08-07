resource "aws_sfn_state_machine" "polars_task_step_function" {
  name     = "${local.workspace_prefix}-cqc-provider-sf"
  role_arn = aws_iam_role.sfn_execution_role.arn

  definition = jsonencode({
    Comment = "Runs the CQC API Provider ingestion task and waits for it to complete."
    StartAt = "RunCQCIngestion"
    States = {
      RunCQCIngestion = {
        Type = "Parallel",
        Branches = [
          {
            StartAt = "RunProviders",
            States = {
              RunProviders = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = var.cluster_arn,
                  TaskDefinition = aws_ecs_task_definition.polars_task.arn,
                  LaunchType     = "FARGATE",
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.public.ids,
                      SecurityGroups = [aws_security_group.ecs_task_sg.id],
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [
                      {
                        Name = "cqc-api-container",
                        "Command" = [
                          "delta_download_cqc_providers.py",
                          "--destination_prefix",
                          "s3://spike-polars-data",
                          "--start_timestamp",
                          "2025-08-01T00:30:31.301Z",
                          "--end_timestamp",
                          "2025-08-08T00:00:31.301Z"
                        ]
                      }
                    ]
                  }
                }
                End = true
              },
            }
          },
          {
            StartAt = "RunLocations",
            States = {
              RunLocations = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = var.cluster_arn,
                  TaskDefinition = aws_ecs_task_definition.polars_task.arn,
                  LaunchType     = "FARGATE",
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.public.ids,
                      SecurityGroups = [aws_security_group.ecs_task_sg.id],
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [
                      {
                        Name = "cqc-api-container",
                        "Command" = [
                          "delta_download_cqc_locations.py",
                          "--destination_prefix",
                          "s3://spike-polars-data",
                          "--start_timestamp",
                          "2025-08-01T00:30:31.301Z",
                          "--end_timestamp",
                          "2025-08-08T00:00:31.301Z"
                        ]
                      }
                    ]
                  }
                }
                End = true
              },
            }
          }
        ]
        End = true
      }
    }
  })
}