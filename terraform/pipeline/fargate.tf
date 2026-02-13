module "cqc-api" {
  source        = "../modules/fargate-task"
  task_name     = "cqc-api"
  ecr_repo_name = "fargate/cqc"
  tag_name      = terraform.workspace
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" },
    { "name" : "CQC_SECRET_NAME", "value" : "cqc_api_primary_key" }
  ]
}

module "_02_sfc_internal" {
  source        = "../modules/fargate-task"
  task_name     = "_02_sfc_internal"
  ecr_repo_name = "fargate/02_sfc_internal"
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
  tag_name = terraform.workspace
}

module "_03_independent_cqc" {
  source        = "../modules/fargate-task"
  task_name     = "_03_independent_cqc"
  ecr_repo_name = "fargate/03_independent_cqc"
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
  tag_name = terraform.workspace
}

module "_03_independent_cqc_model" {
  source        = "../modules/fargate-task"
  task_name     = "_03_independent_cqc_model"
  ecr_repo_name = "fargate/03_independent_cqc_model"
  tag_name      = terraform.workspace
  cpu_size      = 8192
  ram_size      = 32768
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
}
