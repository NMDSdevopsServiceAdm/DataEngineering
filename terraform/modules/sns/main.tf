locals {
  workspace_prefix = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
}

resource "aws_sns_topic" "sns_topic" {
  name = "${local.workspace_prefix}-${var.topic_name}"
}