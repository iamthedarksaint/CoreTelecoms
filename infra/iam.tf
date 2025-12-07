# IAM Roles and Policies for CoreTelecoms Data Platform

# ============================================================================
# DATA ENGINEER IAM USER
# ============================================================================
resource "aws_iam_user" "data_engineer" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = var.data_engineer_username

  tags = merge(
    local.common_tags,
    {
      Name = "Data Engineer User"
      Role = "DataEngineer"
    }
  )
}

# Access keys for data engineer (output securely)
resource "aws_iam_access_key" "data_engineer" {
  count = var.create_data_engineer_user ? 1 : 0
  user  = aws_iam_user.data_engineer[0].name
}

# ============================================================================
# S3 DATA LAKE ACCESS POLICY
# ============================================================================
resource "aws_iam_policy" "s3_data_lake_access" {
  name        = "${var.project_name}-s3-data-lake-access-${var.environment}"
  description = "Allow full access to data lake S3 buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListAllBuckets"
        Effect = "Allow"
        Action = [
          "s3:ListAllMyBuckets",
          "s3:GetBucketLocation"
        ]
        Resource = "*"
      },
      {
        Sid    = "DataLakeBucketAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          aws_s3_bucket.staging_data.arn,
          aws_s3_bucket.processed_data.arn
        ]
      },
      {
        Sid    = "DataLakeObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "${aws_s3_bucket.raw_data.arn}/*",
          "${aws_s3_bucket.staging_data.arn}/*",
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# ============================================================================
# SECRETS MANAGER ACCESS POLICY
# ============================================================================
resource "aws_iam_policy" "secrets_manager_access" {
  name        = "${var.project_name}-secrets-manager-access-${var.environment}"
  description = "Allow access to Secrets Manager for credentials"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SecretsManagerRead"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
          "secretsmanager:ListSecrets"
        ]
        Resource = "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:${var.project_name}/*"
      },
      {
        Sid    = "SecretsManagerWrite"
        Effect = "Allow"
        Action = [
          "secretsmanager:CreateSecret",
          "secretsmanager:UpdateSecret",
          "secretsmanager:PutSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:${var.project_name}/*"
      }
    ]
  })

  tags = local.common_tags
}

# ============================================================================
# SSM PARAMETER STORE ACCESS POLICY
# ============================================================================
resource "aws_iam_policy" "ssm_parameter_access" {
  name        = "${var.project_name}-ssm-parameter-access-${var.environment}"
  description = "Allow access to SSM Parameter Store"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SSMParameterRead"
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParametersByPath",
          "ssm:DescribeParameters"
        ]
        Resource = "arn:aws:ssm:${local.region}:${local.account_id}:parameter/${var.project_name}/*"
      },
      {
        Sid    = "SSMParameterWrite"
        Effect = "Allow"
        Action = [
          "ssm:PutParameter",
          "ssm:DeleteParameter"
        ]
        Resource = "arn:aws:ssm:${local.region}:${local.account_id}:parameter/${var.project_name}/*"
      }
    ]
  })

  tags = local.common_tags
}

# ============================================================================
# GLUE/ATHENA ACCESS POLICY (for querying data lake)
# ============================================================================
resource "aws_iam_policy" "glue_athena_access" {
  name        = "${var.project_name}-glue-athena-access-${var.environment}"
  description = "Allow access to Glue and Athena for data catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/${var.project_name}*",
          "arn:aws:glue:${local.region}:${local.account_id}:table/${var.project_name}*/*"
        ]
      },
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup"
        ]
        Resource = "*"
      }
    ]
  })

  tags = local.common_tags
}

# ============================================================================
# ATTACH POLICIES TO DATA ENGINEER USER
# ============================================================================
resource "aws_iam_user_policy_attachment" "data_engineer_s3" {
  count      = var.create_data_engineer_user ? 1 : 0
  user       = aws_iam_user.data_engineer[0].name
  policy_arn = aws_iam_policy.s3_data_lake_access.arn
}

resource "aws_iam_user_policy_attachment" "data_engineer_secrets" {
  count      = var.create_data_engineer_user ? 1 : 0
  user       = aws_iam_user.data_engineer[0].name
  policy_arn = aws_iam_policy.secrets_manager_access.arn
}

resource "aws_iam_user_policy_attachment" "data_engineer_ssm" {
  count      = var.create_data_engineer_user ? 1 : 0
  user       = aws_iam_user.data_engineer[0].name
  policy_arn = aws_iam_policy.ssm_parameter_access.arn
}

resource "aws_iam_user_policy_attachment" "data_engineer_glue_athena" {
  count      = var.create_data_engineer_user ? 1 : 0
  user       = aws_iam_user.data_engineer[0].name
  policy_arn = aws_iam_policy.glue_athena_access.arn
}

# ============================================================================
# AIRFLOW EXECUTION ROLE
# ============================================================================
resource "aws_iam_role" "airflow_execution_role" {
  count = var.create_airflow_role ? 1 : 0
  name  = "${var.project_name}-airflow-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "airflow.amazonaws.com",
            "airflow-env.amazonaws.com",
            "ec2.amazonaws.com"
          ]
        }
      }
    ]
  })

  tags = merge(
    local.common_tags,
    {
      Name = "Airflow Execution Role"
      Role = "ServiceRole"
    }
  )
}

# Attach policies to Airflow role
resource "aws_iam_role_policy_attachment" "airflow_s3" {
  count      = var.create_airflow_role ? 1 : 0
  role       = aws_iam_role.airflow_execution_role[0].name
  policy_arn = aws_iam_policy.s3_data_lake_access.arn
}

resource "aws_iam_role_policy_attachment" "airflow_secrets" {
  count      = var.create_airflow_role ? 1 : 0
  role       = aws_iam_role.airflow_execution_role[0].name
  policy_arn = aws_iam_policy.secrets_manager_access.arn
}

resource "aws_iam_role_policy_attachment" "airflow_ssm" {
  count      = var.create_airflow_role ? 1 : 0
  role       = aws_iam_role.airflow_execution_role[0].name
  policy_arn = aws_iam_policy.ssm_parameter_access.arn
}

# CloudWatch Logs policy for Airflow
resource "aws_iam_role_policy" "airflow_cloudwatch" {
  count = var.create_airflow_role ? 1 : 0
  name  = "airflow-cloudwatch-logs"
  role  = aws_iam_role.airflow_execution_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/airflow/*"
      }
    ]
  })
}