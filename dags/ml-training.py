import boto3
import time
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.hooks.base import BaseHook
import paramiko
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import mlflow
import pandas as pd
from io import StringIO

# Jenkins Configuration: Load from Airflow Variables
JENKINS_URL = Variable.get("JENKINS_URL")
JENKINS_USER = Variable.get("JENKINS_USER")
JENKINS_TOKEN = Variable.get("JENKINS_TOKEN")
JENKINS_JOB_NAME = Variable.get("JENKINS_JOB_NAME")

# Get AWS connection details from Airflow
aws_conn = BaseHook.get_connection('aws_default')  # Use the Airflow AWS connection
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')  # Default to 'eu-west-3'

# Retrieve other env variables for MLFlow to run
MLFLOW_TRACKING_URI=Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID=Variable.get("MLFLOW_EXPERIMENT_ID")
AWS_ACCESS_KEY_ID= aws_access_key_id
AWS_SECRET_ACCESS_KEY=aws_secret_access_key


if not all([JENKINS_URL, JENKINS_USER, JENKINS_TOKEN]):
    raise ValueError("Missing one or more Jenkins configuration environment variables")

# DAG Configuration
DAG_ID = 'jenkins_ml_training_dag'
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
    description="Poll Jenkins and run ML training",
    catchup=False,
    tags=['jenkins', 'ml-training'],
) as dag:

    # Step 1: Poll Jenkins Job Status
    @task
    def poll_jenkins_job():
        """Poll Jenkins for the job status and check for successful build."""
        import requests
        import time

        # Step 1: Get the latest build number from the job API
        job_url = f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/api/json"
        response = requests.get(job_url, auth=(JENKINS_USER, JENKINS_TOKEN))
        if response.status_code != 200:
            raise Exception(f"Failed to query Jenkins API: {response.status_code}")

        job_info = response.json()
        latest_build_number = job_info['lastBuild']['number']

        # Step 2: Poll the latest build's status
        build_url = f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/{latest_build_number}/api/json"

        while True:
            response = requests.get(build_url, auth=(JENKINS_USER, JENKINS_TOKEN))
            if response.status_code == 200:
                build_info = response.json()
                if not build_info['building']:  # Build is finished
                    if build_info['result'] == 'SUCCESS':
                        print("Jenkins build successful!")
                        return True
                    else:
                        raise Exception("Jenkins build failed!")
            else:
                raise Exception(f"Failed to query Jenkins API: {response.status_code}")
            
            time.sleep(30)  # Poll every 30 seconds

    
    @task
    def log_metrics_to_mlflow():
        """Log metrics and model details to MLflow after Jenkins job completion."""
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.start_run(experiment_id=MLFLOW_EXPERIMENT_ID)

        mlflow.log_param("model_type", "RandomForest")
        accuracy = 0.95  # Replace with actual metric values
        mlflow.log_metric("accuracy", accuracy)

        mlflow.end_run()
        print("Metrics logged to MLflow.")
        # return accuracy
        

    # @task
    # def upload_metrics_to_s3(accuracy):
    #     """Upload metrics data directly to S3 without saving a CSV file locally."""
    #     metrics_data = {
    #         "experiment_id": MLFLOW_EXPERIMENT_ID,
    #         "model_type": "RandomForest",
    #         "accuracy": accuracy,
    #     }

    #     # Convert metrics to DataFrame and then to CSV in memory
    #     df = pd.DataFrame([metrics_data])
    #     csv_buffer = StringIO()
    #     df.to_csv(csv_buffer, index=False)
    #     csv_buffer.seek(0)  # Move to the beginning of the StringIO buffer

    #     # Upload to S3 using boto3
    #     s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
    #                               aws_secret_access_key=aws_secret_access_key,
    #                               region_name=region_name)

    #     s3_object_name = "mlflow_metrics/mlflow_metrics.csv"
    #     s3_client.put_object(Bucket="flow-bucket-ml", Key=s3_object_name, Body=csv_buffer.getvalue())
    #     print("Metrics uploaded to S3.")
    
    
    
    # @task
    # def save_log_to_file(log_content):
    #     """Save the Jenkins log to a local file."""
    #     local_file_path = "/tmp/jenkins_build_log.txt"
    #     with open(local_file_path, 'w') as f:
    #         f.write(str(log_content))
    #     return local_file_path

    # # Task Chaining (DAG Workflow)
    jenkins_poll = poll_jenkins_job()
    accuracy = log_metrics_to_mlflow()
    # getfile = save_log_to_file(jenkins_poll)
    # s3_upload = upload_metrics_to_s3(accuracy)

    # Set task dependencies
    jenkins_poll >> accuracy 