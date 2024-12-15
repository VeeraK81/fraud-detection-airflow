#!/bin/bash

# Read the contents of the DBURL secret file into the environment variable
if [ -f /tmp/FD_DBURL ]; then
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$(cat /tmp/FD_DBURL)
else
  echo "Error: /tmp/FD_DBURL not found."
  exit 1
fi


airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin


rm /tmp/FD_DBURL 

# Execute the passed command (this will run the Airflow command or any other passed command)
exec "$@"
