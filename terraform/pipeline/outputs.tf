output "pipeline_resources_bucket_name" {
  value       = module.pipeline_resources.bucket_name
  description = "The name of the pipeline resources bucket within the workspace"
}

output "datasets_bucket_name" {
  value       = module.datasets_bucket.bucket_name
  description = "The name of the datasets bucket within in the workspace"
}

output "account_id" {
  value       = data.aws_caller_identity.current.account_id
  description = "The current caller's account id"
}

output "caller_arn" {
  value       = data.aws_caller_identity.current.arn
  description = "The current caller's ARN"
}

output "caller_user" {
  value       = data.aws_caller_identity.current.user_id
  description = "The current caller's user ID"
}