terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}
provider "aws" { region = var.aws_region }
variable "aws_region" { type = string; default = "ap-south-1" }
resource "random_id" "suffix" { byte_length = 4 }
resource "aws_s3_bucket" "log_archive" {
  bucket = "demo-log-analytics-${random_id.suffix.hex}"
  tags = { Project = "log-analytics-learning" }
}
resource "aws_s3_bucket_public_access_block" "private" {
  bucket = aws_s3_bucket.log_archive.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}
output "archive_bucket" { value = aws_s3_bucket.log_archive.bucket }
