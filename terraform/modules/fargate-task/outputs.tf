output "task_arn" {
  description = "ARN of the ECS task"
  value       = aws_ecs_task_definition.ecs_task.arn
}

output "security_group_id" {
  description = "ID of the ECS task security group"
  value       = aws_security_group.ecs_task_sg.id
}

output "task_exc_role_arn" {
  description = "ARN of the ECS task execution IAM role"
  value       = aws_iam_role.ecs_task_execution_role.arn
}

output "task_role_arn" {
  description = "ARN of the ECS task IAM role"
  value       = aws_iam_role.ecs_task_role.arn
}

output "subnet_ids" {
  description = "Public subnet IDs for the ECS task"
  value       = data.aws_subnets.public.ids
}
