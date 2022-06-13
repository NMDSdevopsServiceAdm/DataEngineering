locals {
  workspace_prefix = lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-"))
}