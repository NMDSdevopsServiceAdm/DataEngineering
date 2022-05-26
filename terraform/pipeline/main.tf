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

output "s3_resources_bucket_name" {
  value = module.pipeline_resources.bucket_name
}
