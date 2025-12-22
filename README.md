# CoreTelecoms Unified Customer Experience Data Platform


## 📋 Table of Contents
- [Project Overview](#project-overview)
- [Business Problem](#business-problem)
- [Solution Architecture](#solution-architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Pipeline Architecture](#pipeline-architecture)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Data Quality & Testing](#data-quality--testing)
- [Monitoring & Alerts](#monitoring--alerts)
- [CI/CD Pipeline](#cicd-pipeline)
- [Infrastructure as Code](#infrastructure-as-code)
- [Key Features](#key-features)
- [Future Enhancements](#future-enhancements)
- [Contributors](#contributors)

---

## 🎯 Project Overview

CoreTelecoms, a leading US telecom company, faced a critical customer retention crisis due to fragmented complaint management systems. This project delivers a **production-grade, unified data platform** that consolidates customer complaints from multiple channels into a single source of truth for analytics, machine learning, and business insights.

![Alt text](docs/coretelecoms%20architecture.png)

### Key Achievements
- ✅ Unified 5 disparate data sources into a single data warehouse
- ✅ Automated daily ingestion of 100K+ complaint records
- ✅ Reduced reporting time from days to minutes
- ✅ Enabled real-time analytics and ML-driven insights
- ✅ Implemented full CI/CD with infrastructure as code

---

## 💼 Business Problem

### Challenges
CoreTelecoms struggled with:
- **Data Silos**: Complaints scattered across social media, call centers, and web forms
- **Manual Processes**: Reporting team manually compiled spreadsheets
- **Delayed Insights**: Reports took days to generate
- **Data Quality Issues**: Inconsistent formats, naming conventions, and missing values
- **Customer Churn**: Inability to identify and address complaint patterns quickly

### Impact
- Lost revenue due to customer churn
- Poor customer satisfaction scores
- Inefficient complaint resolution
- Limited visibility into operational metrics

---

## 🏗️ Solution Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│   AWS S3     │  Google      │   AWS S3     │   PostgreSQL RDS   │
│  Customers   │  Sheets      │  Call Logs   │   Web Forms        │
│   (CSV)      │  Agents      │  (CSV)       │   (Tables)         │
│              │              │  Social      │                    │
│              │              │  Media       │                    │
│              │              │  (JSON)      │                    │
└──────┬───────┴──────┬───────┴──────┬───────┴────────┬───────────┘
       │              │              │                │
       └──────────────┴──────────────┴────────────────┘
                      │
                      ▼
       ┌──────────────────────────────────────┐
       │      ORCHESTRATION LAYER              │
       │         Apache Airflow                │
       │    (Containerized with Docker)        │
       └──────────┬───────────────────┬────────┘
                  │                   │
                  ▼                   ▼
       ┌──────────────────┐  ┌──────────────────┐
       │   RAW DATA LAYER │  │  EXTRACTION      │
       │      AWS S3      │  │  & VALIDATION    │
       │    (Parquet)     │  │  - Data Quality  │
       │                  │  │  - Metadata      │
       └──────────┬───────┘  └──────────────────┘
                  │
                  ▼
       ┌──────────────────────────────────────┐
       │       DATA WAREHOUSE                  │
       │         Snowflake                     │
       │   ┌──────────────────────────────┐   │
       │   │      RAW SCHEMA              │   │
       │   │  - agents                    │   │
       │   │  - customers                 │   │
       │   │  - call_logs                 │   │
       │   │  - social_media              │   │
       │   │  - web_forms                 │   │
       │   └──────────────────────────────┘   │
       └──────────┬───────────────────────────┘
                  │
                  ▼
       ┌──────────────────────────────────────┐
       │      TRANSFORMATION LAYER             │
       │           dbt Core                    │
       │   ┌──────────────────────────────┐   │
       │   │   STAGING MODELS             │   │
       │   │   - stg_agents               │   │
       │   │   - stg_customers            │   │
       │   │   - stg_call_logs            │   │
       │   │   - stg_social_media         │   │
       │   │   - stg_web_forms            │   │
       │   └──────────────────────────────┘   │
       │   ┌──────────────────────────────┐   │
       │   │   INTERMEDIATE MODELS        │   │
       │   │   - int_complaints_unified   │   │
       │   └──────────────────────────────┘   │
       │   ┌──────────────────────────────┐   │
       │   │   MART MODELS                │   │
       │   │   - fct_complaints           │   │
       │   │   - dim_customers            │   │
       │   │   - dim_agents               │   │
       │   └──────────────────────────────┘   │
       └──────────┬───────────────────────────┘
                  │
                  ▼
       ┌──────────────────────────────────────┐
       │       CONSUMPTION LAYER               │
       │   - BI Dashboards (Power BI/Tableau) │
       │   - ML Models                         │
       │   - Analytics APIs                    │
       └──────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

### Core Technologies
| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 3.0.6 | Workflow management and scheduling |
| **Data Warehouse** | Snowflake | Centralized data storage |
| **Transformation** | dbt Core 1.8.0 | Data modeling and transformation |
| **Object Storage** | AWS S3 | Raw data lake storage |
| **Containerization** | Docker & Docker Compose | Environment consistency |
| **CI/CD** | GitHub Actions | Automated testing and deployment |
| **IaC** | Terraform | Infrastructure provisioning |
| **Programming** | Python 3.12 | ETL logic and data processing |

### Key Python Libraries
- `apache-airflow-providers-amazon` - AWS integration
- `apache-airflow-providers-snowflake` - Snowflake connector
- `boto3` - AWS SDK
- `pandas` - Data manipulation
- `pyarrow` - Parquet file handling
- `gspread` - Google Sheets API
- `psycopg2-binary` - PostgreSQL connector

---

## 📁 Project Structure

```
coretelecoms-data-platform/
│
├── .github/
│   └── workflows/
│       ├── ci.yml                 # Continuous Integration
│       └── cd.yml                 # Continuous Deployment
│
├── dags/
│   ├── customer_complaints_pipeline.py  # Main Airflow DAG
│   └── service_account_key.json        # Google Sheets credentials
│
├── include/
│   └── scripts/
│       ├── extract/
│       │   ├── s3_extractor.py        # S3 data extraction
│       │   ├── google_sheets_extractor.py
│       │   └── postgres_extractor.py
│       ├── load/
│       │   ├── s3_loader.py           # Load to S3
│       │   └── warehouse_loader.py    # Snowflake DDL
│       └── utils/
│           ├── logger.py              # Logging configuration
│           ├── config.py              # Configuration management
│           ├── aws_utils.py           # AWS utilities
│           └── data_quality.py        # Data validation
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_agents.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_call_logs.sql
│   │   │   ├── stg_social_media.sql
│   │   │   └── stg_web_forms.sql
│   │   ├── intermediate/
│   │   │   └── int_complaints_unified.sql
│   │   └── marts/
│   │       ├── fct_complaints.sql
│   │       ├── dim_customers.sql
│   │       └── dim_agents.sql
│   ├── tests/
│   │   └── data_quality_tests.yml
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── terraform/
│   ├── main.tf                    # Main Terraform configuration
│   ├── variables.tf               # Variable definitions
│   ├── outputs.tf                 # Output values
│   ├── providers.tf               # Provider configurations
│   ├── modules/
│   │   ├── s3/
│   │   ├── iam/
│   │   └── networking/
│   └── backend.tf                 # Remote state configuration
│
├── data/
│   ├── customers/                 # Local customer data
│   ├── call_logs/                 # Local call log data
│   ├── social_media/              # Local social media data
│   └── agents/                    # Local agent data
│
├── config/
│   └── airflow.cfg               # Airflow configuration
│
├── logs/                         # Airflow logs
│
├── docker-compose.yml            # Docker services configuration
├── Dockerfile                    # Airflow image definition
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
├── .gitignore                    # Git ignore rules
└── README.md                     # This file
```

---

## 📊 Data Sources

### 1. **Customers** 
- **Format**: CSV
- **Location**: AWS S3 (`s3://coretelecoms-source-data/customers/`)
- **Frequency**: Static
- **Key Fields**: `customer_id`, `name`, `gender`, `date_of_birth`, `email`, `address`

### 2. **Agents**
- **Format**: Google Sheets
- **Location**: Private Google Spreadsheet
- **Frequency**: Static
- **Key Fields**: `agent_id`, `name`, `experience`, `state`

### 3. **Call Center Logs**
- **Format**: CSV
- **Location**: AWS S3 (`s3://coretelecoms-source-data/call_logs/`)
- **Frequency**: Daily
- **Key Fields**: `call_id`, `customer_id`, `agent_id`, `complaint_category`, `resolution_status`, `call_start_time`, `call_end_time`

### 4. **Social Media Complaints**
- **Format**: JSON
- **Location**: AWS S3 (`s3://coretelecoms-source-data/social_media/`)
- **Frequency**: Daily
- **Key Fields**: `complaint_id`, `customer_id`, `agent_id`, `platform`, `complaint_category`, `resolution_status`

### 5. **Website Forms**
- **Format**: PostgreSQL Tables
- **Location**: AWS RDS PostgreSQL
- **Frequency**: Daily (New table per day: `web_form_request_YYYY_MM_DD`)
- **Key Fields**: `form_id`, `customer_id`, `agent_id`, `complaint_type`, `resolution_status`, `submission_date`

---

## 🔄 Pipeline Architecture

### Data Flow Stages

#### Stage 1: Extraction
```python
# Parallel extraction from all sources
extract_agent()          # Google Sheets → DataFrame
extract_customer()       # Local CSV → File List
extract_call_logs()      # Local CSV → File List
extract_social_media()   # Local JSON → File List
extract_web_forms()      # PostgreSQL → DataFrame List
```

#### Stage 2: Data Lake Ingestion
```python
# Load to S3 Raw Layer (Parquet format)
load_agent()           # → s3://bucket/agents/agents.parquet
load_customer()        # → s3://bucket/customers/data.parquet
load_call_logs()       # → s3://bucket/call_logs/year=YYYY/month=MM/day=DD/
load_social_media()    # → s3://bucket/social_media/year=YYYY/month=MM/day=DD/
```

#### Stage 3: Data Warehouse Loading
```sql
-- COPY INTO from S3 to Snowflake
COPY INTO CORETELECOMS.RAW.agents FROM s3://...
COPY INTO CORETELECOMS.RAW.customers FROM s3://...
COPY INTO CORETELECOMS.RAW.call_logs FROM s3://...
COPY INTO CORETELECOMS.RAW.social_media FROM s3://...
```

#### Stage 4: Transformation (dbt)
```bash
dbt run    # Execute all transformations
dbt test   # Run data quality tests
```

### DAG Structure
```
start
  └──> setup_database
         └──> [create_agents_table, create_customers_table, 
               create_call_logs_table, create_social_media_table]
                └──> tables_created
                       └──> [extract_agent, extract_customer,
                             extract_call_logs, extract_social_media]
                              └──> [load_agent, load_customer,
                                    load_call_logs, load_social_media]
                                     └──> [load_agents_to_snowflake,
                                           load_customers_to_snowflake,
                                           load_call_logs_to_snowflake,
                                           load_social_media_to_snowflake]
                                            └──> dbt_run
                                                   └──> dbt_test
                                                          └──> end
```

---

## 🚀 Setup Instructions

### Prerequisites
- Docker Desktop installed
- AWS Account with access keys
- Snowflake account
- Google Cloud Service Account (for Sheets API)
- GitHub account
- Terraform CLI installed

### Step 1: Clone Repository
```bash
git clone https://github.com/iamthedarksaint/CoreTelecoms.git
cd CoreTelecoms
```

### Step 2: Environment Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env
```

**Required Environment Variables:**
```bash
# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=eu-north-1

# Snowflake Credentials
SNOWFLAKE_ACCOUNT=your_account.region.aws
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_DATABASE=CORETELECOMS
SNOWFLAKE_WAREHOUSE=CORETELECOM
SNOWFLAKE_SCHEMA=RAW

# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### Step 3: Google Sheets Setup
```bash
1. Create a service account in Google Cloud Console
2. Enable Google Sheets API
3. Download service account JSON key
4. Place it as: dags/service_account_key.json
5. Share your Google Sheet with the service account email
```

### Step 4: Infrastructure Provisioning (Terraform)
```bash
cd terraform

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Apply infrastructure
terraform apply

cd ..
```

### Step 5: Build and Start Docker Containers
```bash
# Build the custom Airflow image
docker-compose build --no-cache

# Start all services
docker-compose up -d

# Check container status
docker-compose ps

# View logs
docker-compose logs -f airflow-apiserver
```

### Step 6: Configure Airflow Connections
```bash
# Access Airflow UI at http://localhost:8080
# Login: airflow / airflow

# Add Connections via UI (Admin → Connections):

# 1. aws_source
Type: Amazon Web Services
AWS Access Key ID: your_key
AWS Secret Access Key: your_secret
Region: eu-north-1

# 2. aws_dest
Type: Amazon Web Services
AWS Access Key ID: your_key
AWS Secret Access Key: your_secret
Region: eu-north-1

# 3. snowflake_default
Type: Snowflake
Account: your_account
Login: your_username
Password: your_password
Schema: RAW
Database: CORETELECOMS
Warehouse: CORETELECOM
Role: ACCOUNTADMIN
```

### Step 7: Verify dbt Setup
```bash
# Enter Airflow worker container
docker-compose exec airflow-worker bash

# Check dbt installation
dbt --version

# Test dbt connection
cd /opt/airflow/dbt
dbt debug

exit
```

---

## ▶️ Running the Pipeline

### Manual Trigger
1. Navigate to Airflow UI: `http://localhost:8080`
2. Find DAG: `customer_complaints_pipeline`
3. Toggle DAG to **ON**
4. Click **Play button** → Trigger DAG

### Scheduled Execution
The pipeline runs **daily at midnight** automatically (configured with `schedule="@daily"`).

### Monitor Execution
```bash
# View real-time logs
docker-compose logs -f airflow-scheduler

# Check task status in Airflow UI
# Navigate to: DAGs → customer_complaints_pipeline → Grid View
```

### Re-run Failed Tasks
1. Click on the failed task box
2. Select **Clear**
3. Task will automatically retry

---

## ✅ Data Quality & Testing

### Extraction Layer Validation
- **Row count checks**: Ensures data is extracted
- **Required column validation**: Verifies expected schema
- **Data type validation**: Confirms correct types
- **Null checks**: Identifies missing critical values

### dbt Data Tests
```yaml
# tests/data_quality_tests.yml
models:
  - name: fct_complaints
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - complaint_id
            - source_system
    columns:
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      
      - name: agent_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_agents')
              field: agent_id
      
      - name: resolution_status
        tests:
          - accepted_values:
              values: ['Resolved', 'Pending', 'Escalated']
```

### Data Quality Metrics
- **Completeness**: % of non-null values
- **Uniqueness**: Duplicate detection
- **Consistency**: Cross-source validation
- **Timeliness**: Data freshness checks
- **Accuracy**: Business rule validation

---

## 📈 Monitoring & Alerts

### Airflow Monitoring
- **Web UI**: Task status, logs, and metrics at `http://localhost:8080`
- **Health checks**: Container-level health monitoring
- **Flower**: Celery worker monitoring at `http://localhost:5555` (optional)

### Alert Configuration
```python
# In DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@coretelecoms.com']
}
```

### Logging
- **Centralized logs**: `/opt/airflow/logs/`
- **Task-level logging**: Available per task execution
- **Custom logger**: Structured logging with timestamps

---

## 🔄 CI/CD Pipeline

### GitHub Actions Workflows

#### Continuous Integration (`.github/workflows/ci.yml`)
[View the main configuration file](.github/workflows/sample.yaml)

### GitHub Secrets Required
```
DOCKER_USERNAME          # Docker Hub username
DOCKER_PASSWORD          # Docker Hub password
AWS_ACCESS_KEY_ID        # AWS access key
AWS_SECRET_ACCESS_KEY    # AWS secret key
SNOWFLAKE_ACCOUNT        # Snowflake account
SNOWFLAKE_PASSWORD       # Snowflake password
```

---

## 🏗️ Infrastructure as Code

### Terraform Modules
[View the main configuration file](infra/main.tf))

### Terraform Commands
```bash
# Initialize
terraform init

# Plan changes
terraform plan -var-file="environments/dev.tfvars"

# Apply infrastructure
terraform apply -var-file="environments/dev.tfvars"

# Destroy infrastructure
terraform destroy -var-file="environments/dev.tfvars"
```

---

## ⭐ Key Features

### 1. **Production-Grade Architecture**
- Containerized services for consistency
- Separate compute, storage, and orchestration layers
- Scalable to handle millions of records

### 2. **Data Quality First**
- Built-in validation at every stage
- Automated dbt tests
- Metadata tracking for auditability

### 3. **Idempotent Pipeline**
- Safe to re-run without duplication
- Incremental loading for daily data
- Full refresh for static datasets

### 4. **Comprehensive Monitoring**
- Real-time task status in Airflow UI
- Email alerts on failures
- Detailed logging at each step

### 5. **Developer-Friendly**
- Clear code organization
- Extensive documentation
- Easy local development with Docker

### 6. **Cost-Optimized**
- Parquet format reduces storage by 80%
- Incremental processing minimizes compute
- S3 lifecycle policies for archival

---

## 🔮 Future Enhancements

### Short-Term (1-3 months)
- [ ] Real-time streaming ingestion with Kafka
- [ ] Advanced ML models for churn prediction
- [ ] Power BI/Tableau dashboards
- [ ] Slack/Email notifications for SLA breaches
- [ ] Data lineage tracking with OpenLineage

### Medium-Term (3-6 months)
- [ ] Multi-region replication
- [ ] Data masking for PII compliance
- [ ] Advanced analytics with Spark
- [ ] A/B testing framework
- [ ] Self-service data portal

### Long-Term (6+ months)
- [ ] Real-time recommendation engine
- [ ] Predictive maintenance alerts
- [ ] Customer sentiment analysis
- [ ] Integration with CRM systems
- [ ] Multi-cloud support (AWS + Azure + GCP)

---

## 📚 Documentation

### Additional Resources
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)

### Project Documentation
- [Architecture Decision Records (ADRs)](./docs/architecture/)
- [Data Dictionary](./docs/data_dictionary.md)
- [API Documentation](./docs/api.md)
- [Troubleshooting Guide](./docs/troubleshooting.md)

---

## 👥 Contributors

- **Your Name** - Data Engineer - [@iamthedarksaint](https://github.com/iamthedarksaint)

---

## 🙏 Acknowledgments

- CoreTelecoms for the business case
- Apache Airflow community
- dbt Labs for the transformation framework
- Snowflake for the data warehouse platform

---

## 📞 Contact & Support

For questions, issues, or contributions:
- **Email**: bojzino128@gmail.com

---

**Built with ❤️ for CoreTelecoms**