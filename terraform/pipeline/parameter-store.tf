resource "aws_ssm_parameter" "providers_last_run" {
  name      = "/${local.workspace_prefix}/providers-last-run"
  type      = "String"
  value     = timeadd(timestamp(), "-24h")
  overwrite = false
}

resource "aws_ssm_parameter" "locations_last_run" {
  name      = "/${local.workspace_prefix}/locations-last-run"
  type      = "String"
  value     = timeadd(timestamp(), "-24h")
  overwrite = false
}
