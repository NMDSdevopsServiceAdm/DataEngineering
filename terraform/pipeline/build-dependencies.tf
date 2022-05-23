data "archive_file" "dependencies" {
  type        = "zip"
  output_path = "dependencies/dependencies.zip"

  source {
    content  = "${path.cwd}../../schemas/cqc_location_schema.py"
    filename = "schemas/cqc_location_schema.py"
  }
  source {
    content  = "${path.cwd}../../schemas/cqc_provider_schema.py"
    filename = "schemas/cqc_provider_schema.py"
  }
}

resource "aws_s3_object" "dependencies" {
  bucket = module.pipeline_resources.bucket_name
  key    = "dependencies/dependencies.zip"
  source = "dependencies/dependencies.zip"

  depends_on = [
    data.archive_file.dependencies
  ]
}
