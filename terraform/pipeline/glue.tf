module "test_job" {
  source = "../modules/glue-job"
  script_name = "test_script.py"
  glue_role_arn = aws_iam_role.glue_service_iam_role.arn
  resource_bucket_uri = module.pipeline_resources.bucket_uri

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
    "--delimiter"   = ","
  
  }
}