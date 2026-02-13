module "error_notification_lambda" {
  source          = "../modules/lambda-notification"
  lambda_name     = "error_notifications"
  resource_bucket = module.pipeline_resources.bucket_name
  sns_topic_arn   = aws_sns_topic.pipeline_failures.arn
}