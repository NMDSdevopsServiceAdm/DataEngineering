resource "aws_sns_topic" "pipeline_failures" {
  name = "${local.workspace_prefix}-pipeline-failures"
}