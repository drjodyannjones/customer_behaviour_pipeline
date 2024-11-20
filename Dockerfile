FROM python:3.10-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED 1

ENV AIRFLOW_HOME /app/airflow

ENV DBT_DIR=$AIRFLOW_HOME/dbt_lewagon
ENV DBT_TARGET_DIR=$DBT_DIR/target
ENV DBT_PROFILES_DIR=$DBT_DIR
ENV DBT_VERSION=1.1.1

WORKDIR $AIRFLOW_HOME

COPY airflow_scripts airflow_scripts
RUN chmod +x airflow_scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main
