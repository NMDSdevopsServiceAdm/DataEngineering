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

output "image_digest" {
  value = data.aws_ecr_image.create_dataset_snapshot.image_digest
}