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

module "_01_ingest" {
  source         = "../modules/fargate-task"
  task_name      = "_01_ingest"
  ecr_repo_name  = "fargate/01_ingest"
  cluster_arn    = aws_ecs_cluster.polars_cluster.arn
  tag_name       = terraform.workspace
  raw_bucket_arn = "arn:aws:s3:::${local.raw_bucket_name}"
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

# THROWAWAY - for the 2094 EmpStat ratio-filter memory comparison (dedup
# counts, on 2094-empstat, vs raw counts, this branch - both .over()-based).
# Reuses the real _03_independent_cqc image (same ecr_repo_name), sized to
# match it (cpu_size/ram_size default to the same 8192/61440), but with its
# own task definition so POLARS_VERBOSE=1 only applies here, not to the real
# job. Delete this module (and its 3 wiring points in step-function.tf) once
# the comparison concludes.
module "_03_independent_cqc_empstat_diagnostics" {
  source        = "../modules/fargate-task"
  task_name     = "empstat-diagnostics"
  ecr_repo_name = "fargate/03_independent_cqc"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
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

module "_99_publication" {
  source        = "../modules/fargate-task"
  task_name     = "_99_publication"
  ecr_repo_name = "fargate/99_publication"
  cluster_arn   = aws_ecs_cluster.polars_cluster.arn
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" }
  ]
  tag_name = terraform.workspace
}
