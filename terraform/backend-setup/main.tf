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
    key            = "statefiles/workspace=default/backend.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "terraform-locks"
    encrypt        = true
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

resource "aws_dynamodb_table" "terraform_locks" {
  name         = "terraform-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"
  attribute {
    name = "LockID"
    type = "S"
  }
}
