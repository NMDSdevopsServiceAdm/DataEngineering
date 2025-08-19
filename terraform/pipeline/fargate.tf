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
}

module "demo-polars" {
  source        = "../modules/fargate-task"
  task_name     = "demo-polars"
  ecr_repo_name = "898-dev/cqc"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
}