output "parameter_name" {
  description = "The name of a parameter"
  value       = aws_ssm_parameter.api_call_last_run.name
}