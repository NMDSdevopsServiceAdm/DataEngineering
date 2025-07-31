resource "aws_cloudwatch_log_group" "ecs_task_log_group" {
  name              = "/ecs/${local.resource_prefix}-task-logs"
  retention_in_days = 7
}