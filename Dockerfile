ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.10.0
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN pip install --no-cache-dir python-dotenv

COPY ./src /opt/airflow/src
COPY ./data/spotify_dataset.csv /opt/airflow/data/spotify_dataset.csv
COPY ./drive_config /opt/airflow/drive_config

ENV PYTHONPATH=/opt/airflow/src:${PYTHONPATH}