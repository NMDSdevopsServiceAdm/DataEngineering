locals {
  workspace_prefix    = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
  is_main_environment = local.workspace_prefix == "main"
}