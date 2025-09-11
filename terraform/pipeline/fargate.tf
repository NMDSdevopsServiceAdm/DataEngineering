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

module "model_retrain" {
  source        = "../modules/fargate-task"
  task_name     = "model-retrain"
  ecr_repo_name = "fargate/model-retrain"
  cluster_arn   = aws_ecs_cluster.model_cluster.arn
  tag_name      = "latest"
  cpu_size      = 8192
  ram_size      = 32768
  environment = [
    { "name" : "AWS_REGION", "value" : "eu-west-2" },
    { "name" : "MODEL_RETRAIN_TOPIC_ARN", "value" : aws_sns_topic.model_retrain.arn },
    { "name" : "MODEL_S3_BUCKET", "value" : module.pipeline_resources.bucket_name },
    { "name" : "MODEL_S3_PREFIX", "value" : "models" },
    { "name" : "ENVIRONMENT", "value" : terraform.workspace == "default" ? "prod" : "dev" },
    { "name" : "MODEL_RETRAIN_S3_SOURCE_BUCKET", "value" : module.datasets_bucket.bucket_name }
  ]
}