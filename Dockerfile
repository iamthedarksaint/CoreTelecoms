FROM apache/airflow:3.0.6

# RUN apt update && apt install -y python3 python3-pip build-essential libpq-dev

COPY requirements.txt ./requirements.txt

RUN  pip install -r requirements.txt

