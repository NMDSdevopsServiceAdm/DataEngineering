module "cqc-api" {
  source        = "../modules/fargate-task"
  task_name     = "cqc-api"
  ecr_repo_name = "fargate/cqc"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  tag_name      = terraform.workspace
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" },
    { "name" : "CQC_SECRET_NAME", "value" : "cqc_api_primary_key" }
  ]
}

module "_03_independent_cqc" {
  source        = "../modules/fargate-task"
  task_name     = "_03_independent_cqc"
  ecr_repo_name = "fargate/03_independent_cqc"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
  tag_name = terraform.workspace
}

module "_03_independent_cqc_model" {
  source        = "../modules/fargate-task"
  task_name     = "_03_independent_cqc_model"
  ecr_repo_name = "fargate/03_independent_cqc_model"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  tag_name      = terraform.workspace
  cpu_size      = 8192
  ram_size      = 32768
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
}
