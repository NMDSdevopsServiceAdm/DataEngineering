resource "aws_s3_object" "job_script" {
  bucket = var.resource_bucket.bucket_name
  key    = "scripts/${var.script_name}"
  source = abspath("${var.script_dir}/${var.script_name}")

  etag = filemd5(abspath("${var.script_dir}/${var.script_name}"))
}
