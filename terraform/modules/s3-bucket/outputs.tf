output "bucket_uri" {
  value = "s3://${aws_s3_bucket.s3_bucket.bucket}"
}

