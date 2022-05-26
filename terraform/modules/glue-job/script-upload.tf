resource "aws_s3_object" "job_script" {
  bucket = var.resource_bucket.bucket_name
  key    = "scripts/${var.script_name}"
  source = "../../jobs/${var.script_name}"

  etag = filemd5("../../jobs/${var.script_name}")
}
