output "pipeline_arn" {
  description = "The ARN of an step function"
  value       = aws_sfn_state_machine.step_function.id
}