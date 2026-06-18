FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt