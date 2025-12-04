# Security Groups for CoreTelecoms Data Platform

# ============================================================================
# SECURITY GROUP - AIRFLOW
# ============================================================================
resource "aws_security_group" "airflow" {
  name        = "${var.project_name}-airflow-sg-${var.environment}"
  description = "Security group for Airflow"
  vpc_id      = aws_vpc.main.id

  # Ingress - Airflow Webserver
  ingress {
    description = "Airflow Webserver"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Ingress - SSH
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Egress - Allow all outbound
  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-airflow-sg-${var.environment}"
    }
  )
}

# ============================================================================
# SECURITY GROUP - DATA WAREHOUSE (Snowflake/Redshift access)
# ============================================================================
resource "aws_security_group" "data_warehouse" {
  name        = "${var.project_name}-dw-sg-${var.environment}"
  description = "Security group for data warehouse access"
  vpc_id      = aws_vpc.main.id

  # Ingress - PostgreSQL (for Redshift or local PostgreSQL)
  ingress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Ingress - MySQL (if using RDS MySQL)
  ingress {
    description = "MySQL"
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Egress - Allow all outbound
  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-dw-sg-${var.environment}"
    }
  )
}

# ============================================================================
# SECURITY GROUP - APPLICATION (General application layer)
# ============================================================================
resource "aws_security_group" "application" {
  name        = "${var.project_name}-app-sg-${var.environment}"
  description = "Security group for application layer"
  vpc_id      = aws_vpc.main.id

  # Ingress - HTTP
  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Ingress - HTTPS
  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Egress - Allow all outbound
  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-app-sg-${var.environment}"
    }
  )
}

# ============================================================================
# SECURITY GROUP - VPC ENDPOINTS
# ============================================================================
resource "aws_security_group" "vpc_endpoints" {
  name        = "${var.project_name}-vpce-sg-${var.environment}"
  description = "Security group for VPC endpoints"
  vpc_id      = aws_vpc.main.id

  # Ingress - HTTPS from VPC
  ingress {
    description = "HTTPS from VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Egress - Allow all outbound
  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-vpce-sg-${var.environment}"
    }
  )
}