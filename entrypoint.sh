#!/bin/bash

# Read the contents of the DBURL secret file into the environment variable
if [ -f /tmp/DBURL ]; then
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$(cat /tmp/DBURL)
else
  echo "Error: /tmp/DBURL not found."
  exit 1
fi

# Print the environment variable for debugging (optional)
echo "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is set to: $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

# Execute the passed command (this will run the Airflow command or any other passed command)
exec "$@"
