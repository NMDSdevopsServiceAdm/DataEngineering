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


resource "aws_s3_bucket_lifecycle_configuration" "s3_bucket_abandoned_delete_markers" {
  # Terraform can't remove delete markers which don't have a non-current version with the same version id.
  # This expires these markers (effectively deleting any delete markers that don't have a non-current version).
  # Must have bucket versioning enabled first
  depends_on = [aws_s3_bucket_versioning.s3_bucket_versioning]

  bucket = aws_s3_bucket.s3_bucket.id

  rule {
    id = "abandoned_delete_markers"

    expiration {
      expired_object_delete_marker = true
    }

    status = "Enabled"
  }
}
