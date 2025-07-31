resource "aws_ecs_cluster" "polars_cluster" {
  name = "${local.workspace_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

module "cqc-provider-api" {
  source    = "../modules/fargate-task"
  task_name = "cqc-api-providers-ingest"

}