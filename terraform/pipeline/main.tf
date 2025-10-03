terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      # This is needed to ensure that we are running the same version of the terraform provider as circleCI
      # CircleCI will reinitialise with the latest provider version each time, whereas our local terraform would only
      # update when we pass `-update` explicitly into `terraform init`
      version = "~> 6.15.0"
    }
  }
}

provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region
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
