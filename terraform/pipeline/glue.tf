resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name = "${terraform.workspace}-${var.glue_database_name}"
}
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

module "ascwds_crawler" {
  source = "../modules/glue-crawler"
  datset_for_crawler = "ASCWDS"
  glue_crawler_iam_role = aws_iam_role.glue_service_iam_role.arn
  data_path = var.ascwds_root_data_location
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}

module "data_engineering_crawler" {
  source = "../modules/glue-crawler"
  datset_for_crawler = "DATA_ENGINEERING"
  glue_crawler_iam_role = aws_iam_role.glue_service_iam_role.arn
  data_path = var.data_engineering_root_data_location
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}

module "cqc_crawler" {
  source = "../modules/glue-crawler"
  datset_for_crawler = "CQC"
  glue_crawler_iam_role = aws_iam_role.glue_service_iam_role.arn
  data_path = var.cqc_root_data_location
  schedule = "cron(00 07 * * ? *)"
  workspace_glue_database_name = "${terraform.workspace}-${var.glue_database_name}"
}