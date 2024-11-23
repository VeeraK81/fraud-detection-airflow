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

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    default_args=default_args,
    description="Send training data to Evidently AI Cloud",
    catchup=False,
    tags=['S3', 'Evidently'],
) as dag:
    
    @task
    def download_data_from_s3():
        """Download the cv_results.csv file from S3."""
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
            filename = s3_hook.download_file(
                bucket_name=BUCKET_NAME,
                key=RESULT_FILE_KEY  # Correct S3 path
            )
            return filename

        except Exception as e:
            print(f"Error occurred during S3 file download: {str(e)}")
            raise
        

    @task
    def generate_and_upload_report(filename):
        """Generate Evidently report and send it to the Evidently Cloud."""
        
        print("filename", filename)
        try:
            # Load the training data from the downloaded file
            data = pd.read_csv(LOCAL_FILE_PATH)
            print(data.head())
            
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

        
    # Define task dependencies
    download_task = download_data_from_s3()
    reporting_task = generate_and_upload_report(download_task)

    # Ensure tasks run in the correct order
    download_task >> reporting_task
