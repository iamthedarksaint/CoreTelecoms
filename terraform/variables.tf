
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-north-1" 
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "coretelecoms"
}

# S3 Bucket Configuration
variable "raw_bucket_name" {
  description = "S3 bucket name for raw data (bronze layer)"
  type        = string
  default     = "coretelecoms-raw-data"
}

variable "staging_bucket_name" {
  description = "S3 bucket name for staging data (silver layer)"
  type        = string
  default     = "coretelecoms-staging-data"
}

variable "processed_bucket_name" {
  description = "S3 bucket name for processed data (gold layer)"
  type        = string
  default     = "coretelecoms-processed-data"
}

variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "enable_encryption" {
  description = "Enable S3 bucket encryption"
  type        = bool
  default     = true
}

# Lifecycle Configuration
variable "lifecycle_glacier_days" {
  description = "Days before transitioning to Glacier"
  type        = number
  default     = 90
}

variable "lifecycle_deep_archive_days" {
  description = "Days before transitioning to Glacier Deep Archive"
  type        = number
  default     = 180
}

variable "lifecycle_expiration_days" {
  description = "Days before expiring objects"
  type        = number
  default     = 365
}

# VPC Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24"]
}

variable "database_subnet_cidrs" {
  description = "CIDR blocks for database subnets"
  type        = list(string)
  default     = ["10.0.21.0/24", "10.0.22.0/24"]
}

variable "enable_nat_gateway" {
  description = "Enable NAT Gateway for private subnets"
  type        = bool
  default     = true
}

variable "enable_vpn_gateway" {
  description = "Enable VPN Gateway"
  type        = bool
  default     = false
}

# IAM Configuration
variable "create_data_engineer_user" {
  description = "Whether to create data engineer IAM user"
  type        = bool
  default     = true
}

variable "data_engineer_username" {
  description = "Username for data engineer IAM user"
  type        = string
  default     = "coretelecoms-data-engineer"
}

# Airflow Configuration
variable "create_airflow_role" {
  description = "Whether to create Airflow execution role"
  type        = bool
  default     = true
}

# Force destroy (use with caution)
variable "force_destroy_buckets" {
  description = "Allow destroying buckets with content (DANGEROUS)"
  type        = bool
  default     = false
}

# Security
variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access resources"
  type        = list(string)
  default     = ["0.0.0.0/0"]  
}

# Tagging
variable "additional_tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}