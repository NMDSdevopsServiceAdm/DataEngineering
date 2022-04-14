terraform {
  backend "s3" {
    bucket         = "sfc-terraform-state"
    key            = "statefiles/workspace=test/terraform.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "terraform-locks"
    encrypt        = true

  }
}

