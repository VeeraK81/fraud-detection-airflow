import boto3
import pandas as pd
import requests
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import os

# Fetch AWS credentials from Airflow connection
aws_conn = BaseHook.get_connection('aws_default')
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')

# Constants and Variables for your DAG
BUCKET_NAME = Variable.get("BUCKET_NAME")
RESULT_FILE_KEY = Variable.get("RESULT_FILE_KEY")
LOCAL_FILE_PATH = Variable.get("LOCAL_FILE_PATH")

# Evidently Cloud Configuration
EVIDENTLY_API_TOKEN = Variable.get("EVIDENTLY_API_TOKEN")
EVIDENTLY_BASE_URL = Variable.get("EVIDENTLY_BASE_URL")
EVIDENTLY_PROJECT_ID = Variable.get("EVIDENTLY_PROJECT_ID")

# Headers for authentication
headers = {
    "Authorization": f"Bearer {EVIDENTLY_API_TOKEN}",
    "Content-Type": "application/json"
}

# DAG Configuration
DAG_ID = 'fd_training_data_reporting'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

    
@task
def download_and_send_data():
# """Download data from S3 and send to Evidently Cloud"""
    try:
        # Define local directory path
        local_dir = "/tmp"
        file_name = "cv_results.csv"
        local_file_path = os.path.join(local_dir, file_name)

        # Make sure the /tmp directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # Create an S3 hook
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        # Download file from S3
        s3_hook.download_file(
            bucket_name=BUCKET_NAME,
            key=RESULT_FILE_KEY,  # Correct S3 path
            local_path=local_file_path
        )
        
        print(f"File downloaded successfully to {local_file_path}")
        
        # Now send the data to Evidently Cloud
        send_data_to_evidently_cloud(local_file_path)
        
    except Exception as e:
        print(f"Error occurred while downloading or sending data: {str(e)}")
        raise e

@task
def send_data_to_evidently_cloud(file_path: str):
# """Send data to Evidently AI Cloud Workspace"""
    try:
        # Load the training data from the downloaded file
        data = pd.read_csv(file_path)

        # Convert the DataFrame to a JSON format
        data_json = data.to_json(orient="records")

        # Construct the API endpoint for uploading data
        upload_data_url = f"{EVIDENTLY_BASE_URL}/projects/{EVIDENTLY_PROJECT_ID}/datasets"

        # Send the data to Evidently Cloud
        response = requests.post(upload_data_url, json={"data": data_json, "dataset_name": "cv_results_data"})

        # Check the response status
        if response.status_code == 200:
            print("Data successfully sent to Evidently AI Cloud!")
        else:
            print(f"Failed to send data to Evidently. Status code: {response.status_code}, Response: {response.text}")
        
    except Exception as e:
        print(f"Error occurred while sending data to Evidently Cloud: {str(e)}")
        raise e

    # Define the DAG
with DAG(
    'fd_training_data_reporting',
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 11, 23)},
    schedule_interval=None,  # Or any appropriate schedule
    catchup=False,
) as dag:
    download_and_send_data()
