data "aws_caller_identity" "current" {}

locals {
  workspace_prefix = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 23)
  account_id       = data.aws_caller_identity.current.account_id
}