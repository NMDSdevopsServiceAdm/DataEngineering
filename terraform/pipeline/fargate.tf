resource "aws_ecs_cluster" "polars_cluster" {
  name = "${local.workspace_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

module "cqc-provider-api" {
  source      = "../modules/fargate-task"
  task_name   = "delta_download_cqc_providers"
  cluster_arn = aws_ecs_cluster.polars_cluster.arn
}

module "cqc-location-api" {
  source      = "../modules/fargate-task"
  task_name   = "delta_download_cqc_locations"
  cluster_arn = aws_ecs_cluster.polars_cluster.arn
}