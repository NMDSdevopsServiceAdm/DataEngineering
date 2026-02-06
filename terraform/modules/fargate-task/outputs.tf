output "task_arn" {
  value = aws_ecs_task_definition.ecs_task.arn
  description = "ARN of the created ECS task"
}

output "security_group_id" {
  value = aws_security_group.ecs_task_sg.id
  description = "ID of the ECS task security group"
}

output "task_exc_role_arn" {
  value = aws_iam_role.ecs_task_execution_role.arn
  description = "ARN of the ECS task execution IAM role"
}

output "task_role_arn" {
  value = aws_iam_role.ecs_task_role.arn
  description = "ARN of the ECS task IAM role"
}
