#!/bin/bash

# Check if the file exists and read the connection string from it
if [ ! -f /etc/environment/AIRFLOW__DATABASE__SQL_ALCHEMY_CONN ]; then
  echo "Error: /etc/DBURL/AIRFLOW__DATABASE__SQL_ALCHEMY_CONN file does not exist"
  exit 1
fi

# Read the connection string from the file
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$(cat /tmp/DBURL)

# Optionally, print the environment variable to verify
echo "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is set to: $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

# Start Airflow scheduler and webserver
exec "$@"
