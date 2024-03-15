provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region
}

terraform {
  backend "s3" {
    bucket         = "sfc-terraform-state"
    key            = "statefiles/workspace=default/terraform.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

data "aws_secretsmanager_secret_version" "partner_code" {
  secret_id = "arn:aws:secretsmanager:eu-west-2:344210435447:secret:partner_code-ewi2qz"
}

locals {
  workspace_prefix           = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
  is_development_environment = local.workspace_prefix != "main"
  partner_code               = jsondecode(data.aws_secretsmanager_secret_version.partner_code.secret_string)["partner_code"]
}

data "aws_caller_identity" "current" {}
