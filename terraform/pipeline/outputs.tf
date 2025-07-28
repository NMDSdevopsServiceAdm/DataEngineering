output "pipeline_resources_bucket_name" {
  value = module.pipeline_resources.bucket_name
}

output "datasets_bucket_name" {
  value = module.datasets_bucket.bucket_name
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "caller_arn" {
  value = data.aws_caller_identity.current.arn
}

output "caller_user" {
  value = data.aws_caller_identity.current.user_id
}