resource "aws_ssm_parameter" "providers_last_run" {
  name  = "/${local.workspace_prefix}/cqc-providers-last-run"
  type  = "String"
  value = timeadd(timestamp(), "-24h")
  lifecycle {
    ignore_changes = [value, ]
  }
}

resource "aws_ssm_parameter" "locations_last_run" {
  name  = "/${local.workspace_prefix}/cqc-locations-last-run"
  type  = "String"
  value = timeadd(timestamp(), "-24h")
  lifecycle {
    ignore_changes = [value, ]
  }
}
