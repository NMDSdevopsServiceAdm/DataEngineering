data "aws_caller_identity" "current" {}

resource "aws_sns_topic" "pipeline_failures" {
  name = "${local.workspace_prefix}-pipeline-failures"
}

module "providers_last_run" {
  source      = "../modules/parameter-store"
  cqc_dataset = "providers"
}

module "locations_last_run" {
  source      = "../modules/parameter-store"
  cqc_dataset = "locations"
}

module "error_notification_lambda" {
  source          = "../modules/lambda-notification"
  lambda_name     = "error_notifications"
  resource_bucket = module.pipeline_resources.bucket_name
  sns_topic_arn   = aws_sns_topic.pipeline_failures.arn
}