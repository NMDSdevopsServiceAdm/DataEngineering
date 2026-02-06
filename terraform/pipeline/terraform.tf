terraform {
  required_version = "1.13.5"
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