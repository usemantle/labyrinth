resource "aws_s3_bucket" "data" {
  bucket_prefix = "${var.project_name}-data-"
  force_destroy = true

  tags = { Name = "${var.project_name}-data" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
