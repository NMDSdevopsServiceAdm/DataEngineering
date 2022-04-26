module "ascwds_crawler" {
  source = "../modules/glue-crawler"
  datset_for_crawler = "ASCWDS"
  glue_crawler_iam_role = aws_iam_role.glue_service_iam_role.arn
  data_path = var.ascwds_root_data_location
}

module "data_engineering_crawler" {
  source = "../modules/glue-crawler"
  datset_for_crawler = "DATA_ENGINEERING"
  glue_crawler_iam_role = aws_iam_role.glue_service_iam_role.arn
  data_path = var.data_engineering_root_data_location
}

module "cqc_crawler" {
  source = "../modules/glue-crawler"
  datset_for_crawler = "CQC"
  glue_crawler_iam_role = aws_iam_role.glue_service_iam_role.arn
  data_path = var.cqc_root_data_location
  schedule = "cron(00 07 * * ? *)"
}