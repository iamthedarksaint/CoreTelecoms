
resource "aws_s3_bucket" "raw_data" {
  bucket        = "${var.raw_bucket_name}-${var.environment}"
  force_destroy = var.force_destroy_buckets

  tags = merge(
    local.common_tags,
    {
      Name  = "Raw Data Lake"
      Layer = "Bronze"
    }
  )
}

resource "aws_s3_bucket_versioning" "raw_versioning" {
  bucket = aws_s3_bucket.raw_data.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_encryption" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw_block" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"
    
    filter {
         prefix = "raw/" 
    }

    transition {
      days          = var.lifecycle_glacier_days
      storage_class = "GLACIER"
    }

    transition {
      days          = var.lifecycle_deep_archive_days
      storage_class = "DEEP_ARCHIVE"
    }

    expiration {
      days = var.lifecycle_expiration_days
    }
  }

  rule {
    id     = "cleanup-incomplete-uploads"
    status = "Enabled"
     filter {
         prefix = "raw/" 
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ============================================================================
# STAGING DATA BUCKET (SILVER LAYER)
# ============================================================================
resource "aws_s3_bucket" "staging_data" {
  bucket        = "${var.staging_bucket_name}-${var.environment}"
  force_destroy = var.force_destroy_buckets

  tags = merge(
    local.common_tags,
    {
      Name  = "Staging Data Lake"
      Layer = "Silver"
    }
  )
}

resource "aws_s3_bucket_versioning" "staging_versioning" {
  bucket = aws_s3_bucket.staging_data.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "staging_encryption" {
  bucket = aws_s3_bucket.staging_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "staging_block" {
  bucket = aws_s3_bucket.staging_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================================
# PROCESSED DATA BUCKET (GOLD LAYER)
# ============================================================================
resource "aws_s3_bucket" "processed_data" {
  bucket        = "${var.processed_bucket_name}-${var.environment}"
  force_destroy = var.force_destroy_buckets

  tags = merge(
    local.common_tags,
    {
      Name  = "Processed Data Lake"
      Layer = "Gold"
    }
  )
}

resource "aws_s3_bucket_versioning" "processed_versioning" {
  bucket = aws_s3_bucket.processed_data.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_encryption" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "processed_block" {
  bucket = aws_s3_bucket.processed_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}