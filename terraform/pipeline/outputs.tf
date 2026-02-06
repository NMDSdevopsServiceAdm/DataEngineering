output "pipeline_resources_bucket_name" {
  description = "The name of the pipeline resources bucket within the workspace"
  value       = module.pipeline_resources.bucket_name
}

output "datasets_bucket_name" {
  description = "The name of the datasets bucket within in the workspace"
  value       = module.datasets_bucket.bucket_name
}

output "account_id" {
  description = "The current caller's account id"
  value       = data.aws_caller_identity.current.account_id
}

output "caller_arn" {
  description = "The current caller's ARN"
  value       = data.aws_caller_identity.current.arn
}

output "caller_user" {
  description = "The current caller's user ID"
  value       = data.aws_caller_identity.current.user_id
}