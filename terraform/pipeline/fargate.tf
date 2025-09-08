resource "aws_ecs_cluster" "polars_cluster" {
  name = "${local.workspace_prefix}-cluster"

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
}

module "model_retrain" {
  source        = "../modules/fargate-task"
  task_name     = "model-retrain"
  ecr_repo_name = "fargate/model-retrain"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  tag_name      = latest
}