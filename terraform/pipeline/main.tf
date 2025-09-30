provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region

  default_tags {
    tags = {
      Branch      = terraform.workspace
      Owner       = "Data Engineering"
      CreatedWith = "Terraform"
      Repository  = "https://github.com/NMDSdevopsServiceAdm/DataEngineering"
    }
  }
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

locals {
  workspace_prefix               = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
  is_development_environment     = local.workspace_prefix != "main"
  cqc_api_primary_key_secret_arn = "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:cqc_api_primary_key-mK4hzZ"
}

data "aws_caller_identity" "current" {}
