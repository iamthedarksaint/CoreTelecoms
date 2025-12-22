FROM apache/airflow:3.0.6

USER airflow

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir \
    boto3 \
    google-auth==2.29.0 \
    google-auth-oauthlib==1.2.0 \
    gspread==6.0.0 \
    apache-airflow-providers-amazon>=9.13.0 \
    apache-airflow-providers-snowflake>=5.7.0 \
    snowflake-connector-python \
    psycopg2-binary \
    python-dotenv==1.0.1 \
    pyyaml==6.0.1\
    pandas \
    numpy \
    awswrangler\
    pyarrow


