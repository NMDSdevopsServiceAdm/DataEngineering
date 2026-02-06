output "bucket_uri" {
  value       = "s3://${aws_s3_bucket.s3_bucket.bucket}"
  description = "The s3 URI of ans3 bucket"
}

output "bucket_name" {
  value       = aws_s3_bucket.s3_bucket.bucket
  description = "The name of an s3 bucket"
}

output "bucket_arn" {
  value       = aws_s3_bucket.s3_bucket.arn
  description = "The ARN of an S3 bucket"
}
