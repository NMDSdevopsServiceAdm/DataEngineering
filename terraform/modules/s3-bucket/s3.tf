resource "aws_s3_bucket" "s3_bucket" {
  bucket        = "sfc-${var.bucket_name}"
  force_destroy = var.empty_bucket_on_destroy
}

resource "aws_s3_bucket_acl" "s3_bucket_acl" {
  bucket     = aws_s3_bucket.s3_bucket.id
  acl        = "private"
  depends_on = [aws_s3_bucket_ownership_controls.s3_bucket_acl_ownership]
}

resource "aws_s3_bucket_versioning" "s3_bucket_versioning" {
  bucket = aws_s3_bucket.s3_bucket.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

variable "enable_versioning" {
  type    = bool
  default = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "s3_bucket_encryption" {
  bucket = aws_s3_bucket.s3_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "s3_bucket_acl_ownership" {
  bucket = aws_s3_bucket.s3_bucket.id
  rule {
    object_ownership = "ObjectWriter"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "s3_bucket_lifecycle" {
  count      = length(var.expiration_rules) > 0 ? 1 : 0
  bucket     = aws_s3_bucket.s3_bucket.id
  depends_on = [aws_s3_bucket_versioning.s3_bucket_versioning]

  dynamic "rule" {
    for_each = var.expiration_rules
    content {
      id     = rule.value.id
      status = "Enabled"

      filter {
        prefix = rule.value.prefix
      }

      expiration {
        days = rule.value.expiration_days
      }

      # Without this, an expired object on a versioned bucket only gets a
      # delete marker on its current version - the bytes stay as a
      # noncurrent version indefinitely, so the storage is never reclaimed.
      noncurrent_version_expiration {
        noncurrent_days = 1
      }
    }
  }
}
