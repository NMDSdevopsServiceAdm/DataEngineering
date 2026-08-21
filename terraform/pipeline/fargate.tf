resource "aws_ecs_cluster" "polars_cluster" {
  name = "${local.workspace_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster" "model_cluster" {
  name = "${local.workspace_prefix}-model-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

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

module "ascwds" {
  source        = "../modules/fargate-task"
  task_name     = "ascwds"
  ecr_repo_name = "fargate/ascwds"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  tag_name      = terraform.workspace
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" },
  ]
}

module "_02_sfc_internal" {
  source        = "../modules/fargate-task"
  task_name     = "_02_sfc_internal"
  ecr_repo_name = "fargate/02_sfc_internal"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
  tag_name = terraform.workspace
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

# Throwaway, for one memory investigation into the job role imputation step. Reuses the
# real job's image and matches its sizing so peak memory reflects the true boundary.
# POLARS_VERBOSE has to be set here: Polars' Rust core reads it once, before any Python
# in the task runs. Remove with _03_impute_prototype.py.
module "_03_independent_cqc_prototype" {
  source        = "../modules/fargate-task"
  task_name     = "_03_independent_cqc_prototype"
  ecr_repo_name = "fargate/03_independent_cqc"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  cpu_size      = 8192
  ram_size      = 61440
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" },
    { "name" : "POLARS_VERBOSE", "value" : "1" }
  ]
  tag_name = terraform.workspace
}

module "_03_independent_cqc_model" {
  source        = "../modules/fargate-task"
  task_name     = "_03_independent_cqc_model"
  ecr_repo_name = "fargate/03_independent_cqc_model"
  cluster_arn   = aws_ecs_cluster.model_cluster.arn
  tag_name      = terraform.workspace
  cpu_size      = 8192
  ram_size      = 32768
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
}

module "_04_direct_payments" {
  source        = "../modules/fargate-task"
  task_name     = "_04_direct_payments"
  ecr_repo_name = "fargate/04_direct_payments"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
  tag_name = terraform.workspace
}

module "_07_workforce_characteristics" {
  source        = "../modules/fargate-task"
  task_name     = "_07_workforce_characteristics"
  ecr_repo_name = "fargate/07_workforce_characteristics"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
  tag_name = terraform.workspace
}
