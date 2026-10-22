# Hai bucket riêng cho raw và lakehouse; chặn public, giữ version cũ có hạn, dọn tmp/.
data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "data" {
  for_each = toset(["raw", "lakehouse"])
  # Tên S3 duy nhất toàn cầu: thêm account ID như project_id bên GCP.
  bucket        = "${var.name_prefix}-${data.aws_caller_identity.current.account_id}-${each.value}"
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = aws_s3_bucket.data
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Tắt ACL, quyền chỉ đi qua IAM (giống uniform_bucket_level_access).
resource "aws_s3_bucket_ownership_controls" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  # Lifecycle phải đợi versioning bật xong mới áp được luật noncurrent.
  depends_on = [aws_s3_bucket_versioning.data]

  rule {
    id     = "expire-noncurrent"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_retention_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-tmp"
    status = "Enabled"
    filter {
      prefix = "tmp/"
    }
    expiration {
      days = var.temp_retention_days
    }
  }
}
