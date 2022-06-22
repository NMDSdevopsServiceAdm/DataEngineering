locals {
  workspace_prefix = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
  name             = "${local.workspace_prefix}-${replace(var.script_name, ".py", "")}"
  job_name         = "${local.name}_job"
  trigger_name     = "${local.name}_trigger"
}
