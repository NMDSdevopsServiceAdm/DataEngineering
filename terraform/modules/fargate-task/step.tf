# resource "aws_sfn_state_machine" "polars_task_step_function" {
#   name     = "${local.workspace_prefix}-cqc-provider-sf"
#   role_arn = aws_iam_role.sfn_execution_role.arn
#
#   definition = jsonencode({
#     Comment = "Runs the CQC API Provider ingestion task and waits for it to complete."
#     StartAt = "RunCQCIngestion"
#     States = {
#       RunCQCIngestion = {
#         Type     = "Task"
#         Resource = "arn:aws:states:::ecs:runTask.sync"
#         Parameters = {
#           Cluster        = var.cluster_arn,
#           TaskDefinition = aws_ecs_task_definition.polars_task.arn,
#           LaunchType     = "FARGATE",
#           NetworkConfiguration = {
#             AwsvpcConfiguration = {
#               Subnets        = data.aws_subnets.public.ids,
#               SecurityGroups = [aws_security_group.ecs_task_sg.id],
#               AssignPublicIp = "ENABLED"
#             }
#           }
#           Overrides = {
#             ContainerOverrides = [
#               {
#                 Name        = "cqc-api-container",
#                 "Command.$" = "$.arguments"
#               }
#             ]
#           }
#         }
#         End = true
#       }
#     }
#   })
# }