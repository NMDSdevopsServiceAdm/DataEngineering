resource "aws_sns_topic" "pipeline_failures" {
  name = "${local.workspace_prefix}-pipeline-failures"
}

resource "aws_sns_topic" "model_retrain" {
  name = "${local.workspace_prefix}-model-retrain"
}
