# Terraform Outputs

# S3 Buckets
output "raw_bucket_name" {
  description = "Name of the raw data bucket"
  value       = aws_s3_bucket.raw_data.id
}

output "staging_bucket_name" {
  description = "Name of the staging data bucket"
  value       = aws_s3_bucket.staging_data.id
}

output "processed_bucket_name" {
  description = "Name of the processed data bucket"
  value       = aws_s3_bucket.processed_data.id
}

# VPC
output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.main.id
}

output "vpc_cidr" {
  description = "CIDR block of the VPC"
  value       = aws_vpc.main.cidr_block
}

# Subnets
output "public_subnet_ids" {
  description = "IDs of public subnets"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "IDs of private subnets"
  value       = aws_subnet.private[*].id
}

output "database_subnet_ids" {
  description = "IDs of database subnets"
  value       = aws_subnet.database[*].id
}

output "db_subnet_group_name" {
  description = "Name of the DB subnet group"
  value       = aws_db_subnet_group.main.name
}

# Security Groups
output "airflow_security_group_id" {
  description = "ID of Airflow security group"
  value       = aws_security_group.airflow.id
}

output "data_warehouse_security_group_id" {
  description = "ID of data warehouse security group"
  value       = aws_security_group.data_warehouse.id
}

output "application_security_group_id" {
  description = "ID of application security group"
  value       = aws_security_group.application.id
}

# IAM
output "data_engineer_user_name" {
  description = "IAM user name for data engineer"
  value       = var.create_data_engineer_user ? aws_iam_user.data_engineer[0].name : null
}

output "data_engineer_access_key_id" {
  description = "Access Key ID (SENSITIVE)"
  value       = var.create_data_engineer_user ? aws_iam_access_key.data_engineer[0].id : null
  sensitive   = true
}

output "data_engineer_secret_access_key" {
  description = "Secret Access Key (SENSITIVE)"
  value       = var.create_data_engineer_user ? aws_iam_access_key.data_engineer[0].secret : null
  sensitive   = true
}

# Account Info
output "aws_region" {
  description = "AWS region"
  value       = local.region
}

output "aws_account_id" {
  description = "AWS account ID"
  value       = local.account_id
}