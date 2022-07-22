resource "aws_sns_topic" "pipeline_failures" {
  name = "${local.workspace_prefix}-pipeline-failures"
}

resource "aws_sns_topic_subscription" "pipeline_failures_test_email" {
  endpoint  = "test@madetech.com"
  protocol  = "email"
  topic_arn = aws_sns_topic.pipeline_failures.arn
}