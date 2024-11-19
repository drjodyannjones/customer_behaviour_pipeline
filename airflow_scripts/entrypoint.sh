#!/usr/bin/env bash

airflow db upgrade

airflow users create -r Admin -u airflow -p airflow -e admin@example.com -f admin -l airflow

airflow_scripts/init_connections.sh

airflow webserver
