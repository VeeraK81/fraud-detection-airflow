import boto3
import time
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG

from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from evidently.ui.workspace.cloud import CloudWorkspace

from evidently.report import Report

from evidently import metrics
from evidently.metric_preset import DataQualityPreset
from evidently.metric_preset import DataDriftPreset

from evidently.test_suite import TestSuite
from evidently.tests import *
from evidently.test_preset import DataDriftTestPreset
from evidently.tests.base_test import TestResult, TestStatus
from evidently.ui.dashboards import DashboardPanelPlot
from evidently.ui.dashboards import DashboardPanelTestSuite
from evidently.ui.dashboards import PanelValue
from evidently.ui.dashboards import PlotType
from evidently.ui.dashboards import ReportFilter
from evidently.ui.dashboards import TestFilter
from evidently.ui.dashboards import TestSuitePanelType
from evidently.renderers.html_widgets import WidgetSize


aws_conn = BaseHook.get_connection('aws_default')  # Use the Airflow AWS connection
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')  # Default to 'eu-west-3'


AWS_ACCESS_KEY_ID= aws_access_key_id
AWS_SECRET_ACCESS_KEY=aws_secret_access_key
BUCKET_NAME = Variable.get("BUCKET_NAME")
FILE_KEY = Variable.get("FILE_KEY")
ARTIFACT_ROOT = Variable.get("ARTIFACT_ROOT")


# # Define constants for Evidently configuration
# EVIDENTLY_BASE_URL = "https://api.evidentlyai.cloud"
# EVIDENTLY_PROJECT_ID = "019358ac-77c0-7138-a389-4ce4de470692"

# # S3 Bucket and Key Details (Updated for cv_results.csv)
# RESULT_FILE_KEY = "training-models/3/76a94082e1884f68b2a7f63d61424109/artifacts/cv_results.csv"
# LOCAL_FILE_PATH = "/tmp/cv_results.csv"  # Temp location to save file from S3

# Define constants for Evidently configuration
EVIDENTLY_BASE_URL = Variable.get("EVIDENTLY_BASE_URL")
EVIDENTLY_PROJECT_ID = Variable.get("EVIDENTLY_PROJECT_ID")
EVIDENTLY_API_TOKEN = Variable.get("EVIDENTLY_API_TOKEN")

# S3 Bucket and Key Details (Updated for cv_results.csv)
RESULT_FILE_KEY = Variable.get("RESULT_FILE_KEY")
LOCAL_FILE_PATH = Variable.get("LOCAL_FILE_PATH")



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
            # Initialize S3 Hook
            s3_hook = S3Hook(aws_conn_id='aws_default')
            
            # Download the file from S3
            s3_hook.download_file(
                key=RESULT_FILE_KEY,
                bucket_name=BUCKET_NAME,
                local_path=LOCAL_FILE_PATH
            )
            
            print(f"File downloaded from S3 and saved to {LOCAL_FILE_PATH}")

        except Exception as e:
            print(f"Error occurred during S3 file download: {str(e)}")
            raise

    @task
    def generate_and_upload_report():
        """Generate Evidently report and send it to the Evidently Cloud."""
        try:
            # Load the training data from the downloaded file
            data = pd.read_csv(LOCAL_FILE_PATH)
            
            # Set up Evidently Cloud Workspace
            workspace = CloudWorkspace(
                base_url=EVIDENTLY_BASE_URL,
                api_token=EVIDENTLY_API_TOKEN,
                workspace_id=EVIDENTLY_PROJECT_ID
            )

            # Initialize report with metrics you want
            report = Report(metrics=[DataQualityPreset(), DataDriftPreset()])

            # Calculate report using the data (assuming current data is the same as reference)
            report.run(reference_data=data, current_data=data)

            # Generate a report as an HTML or JSON file locally
            report_path = "/tmp/evidently_report.html"
            report.save_html(report_path)
            
            print(f"Evidently report generated and saved to {report_path}")

            # Optionally, upload the report back to S3 or handle it further
            s3_hook = S3Hook(aws_conn_id='aws_default')
            report_key = "reports/evidently_report.html"
            s3_hook.load_file(
                filename=report_path,
                key=report_key,
                bucket_name=BUCKET_NAME,
                replace=True
            )
            print(f"Report uploaded to S3 bucket {BUCKET_NAME} with key {report_key}")

        except Exception as e:
            print(f"Error occurred while generating Evidently report: {str(e)}")
            raise
        
    # Define task dependencies
    download_task = download_data_from_s3()
    reporting_task = generate_and_upload_report()

    # Ensure tasks run in the correct order
    download_task >> reporting_task