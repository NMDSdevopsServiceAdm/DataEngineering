provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region
  alias      = "prod"
  profile    = "prod"
}

provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.region
  alias      = "non-prod"
  profile    = "non-prod"
}

terraform {
  backend "s3" {
    # Bucket defined in ../*.s3.tfbackend
    key          = "statefiles/workspace=default/backend.tfstate"
    region       = "eu-west-2"
    use_lockfile = true
    encrypt      = true
  }
}

resource "aws_s3_bucket" "terraform_state" {
  bucket = var.bucket
}

resource "aws_s3_bucket_versioning" "terraform_state_versioning" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state_encryption" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
