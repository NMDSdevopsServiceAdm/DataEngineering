resource "aws_sns_topic" "pipeline_failures" {
  name = "${local.workspace_prefix}-pipeline-failures"
}

resource "aws_sns_topic" "pipeline_failures_delta" {
  name = "${local.workspace_prefix}-pipeline-failures-delta"
}