
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-vpc-${var.environment}"
    }
  )
}


resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-igw-${var.environment}"
    }
  )
}

# ============================================================================
# PUBLIC SUBNETS
# ============================================================================
resource "aws_subnet" "public" {
  count                   = length(var.public_subnet_cidrs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-public-subnet-${count.index + 1}-${var.environment}"
      Type = "Public"
      Tier = "Public"
    }
  )
}

# ============================================================================
# PRIVATE SUBNETS (for application layer)
# ============================================================================
resource "aws_subnet" "private" {
  count             = length(var.private_subnet_cidrs)
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-private-subnet-${count.index + 1}-${var.environment}"
      Type = "Private"
      Tier = "Application"
    }
  )
}

# ============================================================================
# DATABASE SUBNETS (for data warehouse and databases)
# ============================================================================
resource "aws_subnet" "database" {
  count             = length(var.database_subnet_cidrs)
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.database_subnet_cidrs[count.index]
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-database-subnet-${count.index + 1}-${var.environment}"
      Type = "Private"
      Tier = "Database"
    }
  )
}

# ============================================================================
# ELASTIC IPs FOR NAT GATEWAYS
# ============================================================================
resource "aws_eip" "nat" {
  count  = var.enable_nat_gateway ? length(var.public_subnet_cidrs) : 0
  domain = "vpc"

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-nat-eip-${count.index + 1}-${var.environment}"
    }
  )

  depends_on = [aws_internet_gateway.main]
}

# ============================================================================
# NAT GATEWAYS
# ============================================================================
resource "aws_nat_gateway" "main" {
  count         = var.enable_nat_gateway ? length(var.public_subnet_cidrs) : 0
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-nat-${count.index + 1}-${var.environment}"
    }
  )

  depends_on = [aws_internet_gateway.main]
}

# ============================================================================
# ROUTE TABLE - PUBLIC
# ============================================================================
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-public-rt-${var.environment}"
      Type = "Public"
    }
  )
}

# ============================================================================
# ROUTE TABLE - PRIVATE
# ============================================================================
resource "aws_route_table" "private" {
  count  = length(var.private_subnet_cidrs)
  vpc_id = aws_vpc.main.id

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-private-rt-${count.index + 1}-${var.environment}"
      Type = "Private"
    }
  )
}

# Add NAT Gateway routes to private route tables
resource "aws_route" "private_nat_gateway" {
  count                  = var.enable_nat_gateway ? length(var.private_subnet_cidrs) : 0
  route_table_id         = aws_route_table.private[count.index].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.main[count.index].id
}

# ============================================================================
# ROUTE TABLE - DATABASE
# ============================================================================
resource "aws_route_table" "database" {
  count  = length(var.database_subnet_cidrs)
  vpc_id = aws_vpc.main.id

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-database-rt-${count.index + 1}-${var.environment}"
      Type = "Database"
    }
  )
}

# Add NAT Gateway routes to database route tables
resource "aws_route" "database_nat_gateway" {
  count                  = var.enable_nat_gateway ? length(var.database_subnet_cidrs) : 0
  route_table_id         = aws_route_table.database[count.index].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.main[count.index % length(var.public_subnet_cidrs)].id
}

# ============================================================================
# ROUTE TABLE ASSOCIATIONS
# ============================================================================
resource "aws_route_table_association" "public" {
  count          = length(var.public_subnet_cidrs)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = length(var.private_subnet_cidrs)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

resource "aws_route_table_association" "database" {
  count          = length(var.database_subnet_cidrs)
  subnet_id      = aws_subnet.database[count.index].id
  route_table_id = aws_route_table.database[count.index].id
}

# ============================================================================
# DB SUBNET GROUP (for RDS/Aurora if needed)
# ============================================================================
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group-${var.environment}"
  subnet_ids = aws_subnet.database[*].id

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-db-subnet-group-${var.environment}"
    }
  )
}

# ============================================================================
# VPC ENDPOINTS FOR S3 (for private access)
# ============================================================================
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${local.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = concat(
    [aws_route_table.public.id],
    aws_route_table.private[*].id,
    aws_route_table.database[*].id
  )

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-s3-endpoint-${var.environment}"
    }
  )
}

# ============================================================================
# VPC FLOW LOGS (for network monitoring)
# ============================================================================
resource "aws_flow_log" "main" {
  iam_role_arn    = aws_iam_role.vpc_flow_log.arn
  log_destination = aws_cloudwatch_log_group.vpc_flow_log.arn
  traffic_type    = "ALL"
  vpc_id          = aws_vpc.main.id

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-vpc-flow-logs-${var.environment}"
    }
  )
}

resource "aws_cloudwatch_log_group" "vpc_flow_log" {
  name              = "/aws/vpc/${var.project_name}-${var.environment}"
  retention_in_days = 30

  tags = local.common_tags
}

resource "aws_iam_role" "vpc_flow_log" {
  name = "${var.project_name}-vpc-flow-log-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "vpc-flow-logs.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "vpc_flow_log" {
  name = "vpc-flow-log-policy"
  role = aws_iam_role.vpc_flow_log.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}