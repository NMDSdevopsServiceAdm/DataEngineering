output "task_arn" {
  value = aws_ecs_task_definition.polars_task.arn
}

output "subnet_ids" {
  value = data.aws_subnets.public.ids
}

output "security_group_id" {
  value = aws_security_group.ecs_task_sg.id
}