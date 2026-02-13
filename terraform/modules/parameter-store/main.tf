locals {
  workspace_prefix = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
}

resource "aws_ssm_parameter" "api_call_last_run" {
  name  = "/${local.workspace_prefix}/cqc-${var.cqc_dataset}-last-run"
  type  = "String"
  value = timeadd(timestamp(), "-24h")
  lifecycle {
    ignore_changes = [value, ]
  }
}