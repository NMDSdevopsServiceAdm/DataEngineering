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
    { "AWS_REGION" : "eu-west-2" },
    { "CQC_SECRET_NAME" : "cqc_api_primary_key" }
  ]
}

module "model_retrain" {
  source        = "../modules/fargate-task"
  task_name     = "model-retrain"
  ecr_repo_name = "fargate/model-retrain"
  cluster_arn   = aws_ecs_cluster.model_cluster.arn
  tag_name      = "latest"
  environment = [
    { "AWS_REGION" : "eu-west-2" },
    { "MODEL_RETRAIN_TOPIC_ARN" : aws_sns_topic.model_retrain.arn },
    { "MODEL_S3_BUCKET" : module.pipeline_resources.bucket_name },
    { "MODEL_S3_PREFIX" : "models" }
  ]
}