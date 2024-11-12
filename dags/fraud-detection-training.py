import boto3
import time
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
# from airflow.providers.amazon.aws.operators.ec2 import (
#     EC2CreateInstanceOperator,
#     EC2TerminateInstanceOperator,
# )
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# import paramiko
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import mlflow
import pandas as pd
from io import StringIO
import logging
import os
import csv
import json
from airflow.models import Variable

# Jenkins Configuration: Load from Airflow Variables
# JENKINS_URL = Variable.get("JENKINS_URL")
# JENKINS_USER = Variable.get("JENKINS_USER")
# JENKINS_TOKEN = Variable.get("JENKINS_TOKEN")
# JENKINS_JOB_NAME = Variable.get("JENKINS_JOB_NAME")

# Get AWS connection details from Airflow
# aws_conn = BaseHook.get_connection('aws_default')  # Use the Airflow AWS connection
# aws_access_key_id = aws_conn.login
# aws_secret_access_key = aws_conn.password
# region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')  # Default to 'eu-west-3'

# Retrieve other env variables for MLFlow to run
# MLFLOW_TRACKING_URI=Variable.get("MLFLOW_TRACKING_URI")
# MLFLOW_EXPERIMENT_ID=Variable.get("MLFLOW_EXPERIMENT_ID")
# AWS_ACCESS_KEY_ID= aws_access_key_id
# AWS_SECRET_ACCESS_KEY=aws_secret_access_key


# if not all([JENKINS_URL, JENKINS_USER, JENKINS_TOKEN]):
#     raise ValueError("Missing one or more Jenkins configuration environment variables")


# Constants for S3 configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_KEY = os.getenv("S3_KEY")

# DAG configuration
DAG_ID = 'fraud_detection_training_dag'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    default_args=default_args,
    description="Upload or append data to S3 for fraud detection training",
    catchup=False,
    tags=['fraud-detection-training'],
) as dag:

    @task
    def transaction_consume(**kwargs):
        """
        Fetch transaction data from the DAG run configuration.
        """
        config = kwargs.get('dag_run').conf
        transaction_data = config.get('transaction_data')
        
        if transaction_data is None:
            logging.warning("No transaction data found in config. Please check dag_run.conf.")
        else:
            logging.info(f"Received transaction data: {transaction_data}")
        
        return transaction_data

    # @task
    # def upload_or_append_to_s3(transaction_data):
    #     """
    #     Uploads transaction data to S3, appending to an existing file if present.
    #     """
    #     if not transaction_data:
    #         logging.error("No transaction data provided, aborting S3 upload.")
    #         return
        
    #     s3_hook = S3Hook(aws_conn_id="aws_default")
    #     transaction_data_str = json.dumps(transaction_data)

    #     # Check if the file already exists in S3
    #     try:
    #         if s3_hook.check_for_key(S3_KEY, bucket_name=S3_BUCKET_NAME):
    #             # Read existing data and append the new data
    #             existing_data = s3_hook.read_key(S3_KEY, bucket_name=S3_BUCKET_NAME)
    #             updated_data = existing_data + "\n" + transaction_data_str
    #             logging.info("Appending data to existing file in S3.")
    #         else:
    #             # If the file doesn't exist, use the new data as the file content
    #             updated_data = transaction_data_str
    #             logging.info("Creating new file in S3 with transaction data.")

    #         # Write the updated data back to S3
    #         s3_hook.load_string(
    #             string_data=updated_data,
    #             key=S3_KEY,
    #             bucket_name=S3_BUCKET_NAME,
    #             replace=True  # Overwrite existing file if it exists
    #         )
    #         logging.info(f"Data successfully uploaded to {S3_BUCKET_NAME}/{S3_KEY}")
        
    #     except Exception as e:
    #         logging.error(f"Failed to upload data to S3: {str(e)}")
    #         raise
    @task
    def upload_or_append_to_s3(transaction_data):
        """
        Uploads transaction data to S3 in CSV format, appending to an existing file if present.
        """
        if not transaction_data:
            logging.error("No transaction data provided, aborting S3 upload.")
            return

        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        try:
            # Step 1: Check if the CSV file already exists in S3
            csv_rows = []
            try:
                if s3_hook.check_for_key(S3_KEY, bucket_name=S3_BUCKET_NAME):
                    # Read existing CSV data from S3
                    existing_data = s3_hook.read_key(S3_KEY, bucket_name=S3_BUCKET_NAME)
                    existing_csv = StringIO(existing_data)
                    reader = csv.reader(existing_csv)
                    csv_rows = list(reader)
                    logging.info("Appending data to existing CSV file in S3.")
                else:
                    logging.info("Creating new CSV file in S3 with transaction data.")
            except Exception as e:
                logging.error(f"Error reading existing CSV from S3: {str(e)}")
                raise

            # Step 2: Prepare the transaction data as a row to append
            if not csv_rows:
                # If CSV is empty or doesn't exist, add headers as the first row
                csv_rows.append(transaction_data.keys())
            
            # Append the transaction data as a new row
            csv_rows.append(transaction_data.values())

            # Step 3: Write the updated CSV data back to S3
            output = StringIO()
            writer = csv.writer(output)
            writer.writerows(csv_rows)
            
            s3_hook.load_string(
                string_data=output.getvalue(),
                key=S3_KEY,
                bucket_name=S3_BUCKET_NAME,
                replace=True  # Overwrite the existing file
            )
            logging.info(f"Data successfully uploaded to {S3_BUCKET_NAME}/{S3_KEY}")

        except Exception as e:
            logging.error(f"Failed to upload data to S3: {str(e)}")
            raise

    # Define the task dependencies
    transaction_data = transaction_consume()
    upload_or_append_to_s3(transaction_data)
