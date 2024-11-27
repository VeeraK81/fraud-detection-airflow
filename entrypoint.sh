#!/bin/bash

# Source the /etc/environment file so that all variables are available for runtime
source /etc/environment

# Start Airflow scheduler and webserver
exec "$@"