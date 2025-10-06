provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region

  default_tags {
    tags = {
      Branch       = terraform.workspace
      Owner        = "Data Engineering"
      CreatedWith  = "Terraform"
      Repository   = "https://github.com/NMDSdevopsServiceAdm/DataEngineering"
      IsProduction = local.workspace_prefix == "main"
    }
  }
}

terraform {
  backend "s3" {
    # Bucket defined in ../*.s3.tfbackend
    key            = "statefiles/workspace=default/terraform.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

locals {
  workspace_prefix           = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
  is_development_environment = local.workspace_prefix != "main"
}

data "aws_caller_identity" "current" {}
