output "bucket_uri" {
  description = "The s3 URI of ans3 bucket"
  value       = "s3://${aws_s3_bucket.s3_bucket.bucket}"
}

output "bucket_name" {
  description = "The name of an s3 bucket"
  value       = aws_s3_bucket.s3_bucket.bucket
}

output "bucket_arn" {
  description = "The ARN of an S3 bucket"
  value       = aws_s3_bucket.s3_bucket.arn
}
