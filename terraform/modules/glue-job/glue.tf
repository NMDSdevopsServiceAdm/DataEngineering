resource "aws_glue_job" "glue_job" {
  name              = local.job_name
  role_arn          = var.glue_role.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 5
  }
  command {
    script_location = "${var.resource_bucket.bucket_uri}/scripts/${var.script_name}"
  }

  default_arguments = merge(var.job_parameters,
    {
      "--extra-py-files"                   = "${var.resource_bucket.bucket_uri}/dependencies/dependencies.zip,${var.resource_bucket.bucket_uri}/dependencies/pydeequ-1.4.0.zip"
      "--extra-jars"                       = "${var.resource_bucket.bucket_uri}/dependencies/deequ-2.0.7-spark-3.3.jar"
      "--TempDir"                          = "${var.resource_bucket.bucket_uri}/temp/"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-auto-scaling"              = var.worker_type == "Standard" ? "false" : "true"
      "--conf"                             = "spark.sql.sources.partitionColumnTypeInference.enabled=false"
  })
}
