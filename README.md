CORETELECOMS PROJECT

Problem: Customer retention crisis due to fragmented complaint data
Value proposition: unified data platform to reduce customer churn

Data Sources:
OWNER	DESCRIPTION	FORMAT	DATA LOCATION	FREQUENCY
CUSTOMERS	Customer data with attributes like customer id, name, gender, phone, location etc.	csv	S3 bucket - https://eu-north-1.console.aws.amazon.com/s3/buckets/core-telecoms-data-lake?region=eu-north-1&prefix=customers/&showversions=false	Static
AGENTS	Lookup table for customer care agents.	Google Spread Sheet	Spread sheet - https://docs.google.com/spreadsheets/d/17IXo7TjDSSHaFobGG9hcqgbsNKTaqgyctWGnwDeNkIQ/edit?gid=0#gid=0	Static
CALL CENTER LOGS	Daily customer call log with complaint type, agent ID, resolution status, and duration.	csv	s3 bucket - https://eu-north-1.console.aws.amazon.com/s3/buckets/core-telecoms-data-lake?region=eu-north-1&prefix=call+logs/&showversions=false	Daily Populated
SOCIAL MEDIA	Complaints across social media platforms, containing customer & agent id, platform, issue type etc.	Json	s3 bucket - https://eu-north-1.console.aws.amazon.com/s3/buckets/core-telecoms-data-lake?region=eu-north-1&prefix=social_medias/&showversions=false	Daily Populated
WEBSITE COMPLAINT FORMS	Customer-submitted forms with customer id, agent id, complaint type, and resolution status.	Database Table	Transactional Postgres Database - https://eu-north-1.console.aws.amazon.com/systems-manager/parameters/?region=eu-north-1&tab=Table#list_parameter_filters=Name:Contains:%2Fcoretelecomms%2Fdatabase%2F	Daily Populated

Project Plan:
Project Architecture:
Data Sources: 
1.	AWS S3: Customers, Call logs, Social Media 
2.	Google sheet: Agents
3.	PostgreSQL: Web Forms
Extraction Layer: 
•	Python scripts: boto3 (s3), gspread (Google), psycopg2 (Postgres)
Raw Data Lake:
•	AWS S3 – Parquet
o	/raw/customers/
o	/raw/agents/
o	/raw/call_logs/
o	/raw/social_media/
o	/raw/web_forms/
Data Warehouse:
•	Bronze/ Raw Layer
o	raw_customers
o	raw_agents
o	raw_call_logs
o	raw_social_media
o	raw_web_forms
•	Silver/ Staging layer – DBT models
o	stg_customers
o	stg_agents
o	stg_complaints
•	Gold/Analytics layer – DBT marts
o	dim_customers
o	dim_agents
o	fact_complaints
o	agg_daily_metrics
Analytics & Business Intelligence
•	Dashboards
•	Reports
Orchestration layer - Apache Airflow
•	DAG – Customer_complaints_pipeline
o	Extract >> Load >> dbt run >> dbt test >> Notify
o	Schedule: 
o	Retries:
Infrastructures and Deployments
•	Terraform – Infrastructure as a Code
o	S3 Bucket
o	IAM Roles
o	Redshift
o	Secrets
•	Docker Container 
o	Python Codes
o	Airflow
o	Dbt
•	CI/CD – Github Actions
o	Linting
o	Testing
o	Build/Push

Slide Presentation:
•	Title: Project name
•	Problem: CoreTelecoms customer churn crisis
•	Solutions: Unified data platform architecture
•	Architecture Diagram
•	Tech Stack: Why they were used
•	Data Flow: 
•	Key Features: orchestration, testing, ci/cd, iac
•	Implementation: What I built vs what I’d build
•	Challenges vs learning: Time constraint
•	Q and A
