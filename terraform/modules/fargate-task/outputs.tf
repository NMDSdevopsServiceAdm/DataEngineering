output "task_arn" {
  value = aws_ecs_task_definition.polars_task.arn
}

output "subnet_ids" {
  value = data.aws_subnets.public.ids
}

output "security_group_id" {
  value = aws_security_group.ecs_task_sg.id
}

output "task_exc_role_arn" {
  value = aws_iam_role.ecs_task_execution_role.arn
}

output "task_role_arn" {
  value = aws_iam_role.ecs_task_role.arn
}