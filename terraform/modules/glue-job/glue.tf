resource "aws_glue_job" "glue_job" {
  name              = local.job_name
  role_arn          = var.glue_role.arn
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 2
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 5
  }
  command {
    script_location = "${var.resource_bucket.bucket_uri}/scripts/${var.script_name}"
  }

  default_arguments = merge(var.job_parameters,
    {
      "--extra-py-files"                   = "${var.resource_bucket.bucket_uri}/dependencies/dependencies.zip"
      "--TempDir"                          = "${var.resource_bucket.bucket_uri}/temp/"
      "--enable-continuous-cloudwatch-log" = "true"
  })
}
