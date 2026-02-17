output "bucket_uri" {
  value = "s3://${aws_s3_bucket.s3_bucket.bucket}"
}

output "bucket_name" {
  value = aws_s3_bucket.s3_bucket.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.s3_bucket.arn
}
