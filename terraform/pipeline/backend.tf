terraform {
  backend "s3" {
    # Bucket defined in ../*.s3.tfbackend
    key          = "statefiles/workspace=default/terraform.tfstate"
    region       = "eu-west-2"
    use_lockfile = true
    encrypt      = true
  }
}
