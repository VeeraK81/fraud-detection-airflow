# import boto3
# import pandas as pd
# import requests
# from datetime import datetime
# from airflow.decorators import task
# from airflow.models.dag import DAG
# from airflow.hooks.base import BaseHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.models import Variable
# import os
# from evidently.ui.workspace.cloud import CloudWorkspace
# from evidently.report import Report



# # Fetch AWS credentials from Airflow connection
# aws_conn = BaseHook.get_connection('aws_default')
# aws_access_key_id = aws_conn.login
# aws_secret_access_key = aws_conn.password
# region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')

# # Constants and Variables for your DAG
# BUCKET_NAME = Variable.get("BUCKET_NAME")
# RESULT_FILE_KEY = Variable.get("RESULT_FILE_KEY")
# LOCAL_FILE_PATH = Variable.get("LOCAL_FILE_PATH")

# # Evidently Cloud Configuration
# EVIDENTLY_API_TOKEN = Variable.get("EVIDENTLY_API_TOKEN")
# EVIDENTLY_BASE_URL = Variable.get("EVIDENTLY_BASE_URL")
# EVIDENTLY_PROJECT_ID = Variable.get("EVIDENTLY_PROJECT_ID")

# # Headers for authentication
# headers = {
#     "Authorization": f"Bearer {EVIDENTLY_API_TOKEN}",
#     "Content-Type": "application/json"
# }


# # DAG Configuration
# DAG_ID = 'fd_training_data_reporting'
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# # Define the DAG
# with DAG(
#     dag_id=DAG_ID,
#     schedule_interval=None,
#     default_args=default_args,
#     description="Send training data to Evidently AI Cloud",
#     catchup=False,
#     tags=['S3', 'Evidently'],
# ) as dag:
    
#     @task
#     def download_data_from_s3():
#         """Download the cv_results.csv file from S3."""
#         try:
#             # Define local directory path
#             local_dir = "/tmp"
#             file_name = "cv_results.csv"
#             local_file_path = os.path.join(local_dir, file_name)

#             # Make sure the /tmp directory exists
#             if not os.path.exists(local_dir):
#                 os.makedirs(local_dir)

#             # Create an S3 hook
#             s3_hook = S3Hook(aws_conn_id="aws_default")
            
#             # Download file from S3
#             filename = s3_hook.download_file(
#                 bucket_name=BUCKET_NAME,
#                 key=RESULT_FILE_KEY  # Correct S3 path
#             )
#             print("filename", filename)
#             return filename

#         except Exception as e:
#             print(f"Error occurred during S3 file download: {str(e)}")
#             raise
        

#     @task
#     def generate_and_upload_report(filename):
#         """Generate Evidently report and send it to the Evidently Cloud."""
        
#         print("filename", filename)
#         try:
#             # Load the training data from the downloaded file
#             data = pd.read_csv(filename)
#             print(data.head())
            
#              # Convert the DataFrame to a JSON format
#             data_json = data.to_json(orient="records")

            
#             ws = CloudWorkspace(
#             token=EVIDENTLY_API_TOKEN,
#             url="https://app.evidently.cloud")

#             project = ws.get_project(EVIDENTLY_PROJECT_ID)
            
#             project.send_data(
#                 data_json=data_json,
#                 dataset_name="cv_results_data"
#             )
#             print("Data successfully sent to Evidently AI Cloud!")

#         except Exception as e:
#             print(f"Error occurred while sending data to Evidently Cloud: {str(e)}")
#             raise e

        
#     # Define task dependencies
#     download_task = download_data_from_s3()
#     reporting_task = generate_and_upload_report(download_task)

#     # Ensure tasks run in the correct order
#     download_task >> reporting_task

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import pickle
import requests
import pandas as pd
from evidently import ColumnMapping

from evidently.ui.workspace.cloud import CloudWorkspace
from evidently.report import Report
from evidently.metrics import *
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, RegressionPreset

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
import json
import mlflow
from airflow.models import Variable

# Evidently Cloud API
EVIDENTLY_BASE_URL = Variable.get("EVIDENTLY_BASE_URL")
EVIDENTLY_PROJECT_ID = Variable.get("EVIDENTLY_PROJECT_ID")
EVIDENTLY_API_TOKEN = Variable.get("EVIDENTLY_API_TOKEN")
MLFLOW_TRACKING_URI=Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID=Variable.get("MLFLOW_EXPERIMENT_ID")

# Calculate metrics using Evidently
def calculate_metrics(**kwargs):
    # Set MLflow tracking URI
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # Retrieve the runs from a specific experiment
    runs = mlflow.search_runs(experiment_ids=MLFLOW_EXPERIMENT_ID)

    # Filter out the GridSearchCV runs and get the latest and reference run IDs
    gs_run_id = runs[runs['tags.estimator_name'] == "GridSearchCV"]
    latest_run_id = gs_run_id.iloc[0]['run_id']
    reference_run_id = gs_run_id.iloc[1]['run_id']

    # Retrieve metrics from MLflow for the current and reference runs
    reference_data = mlflow.get_run(reference_run_id).data.metrics
    current_data = mlflow.get_run(latest_run_id).data.metrics
    

    # Convert the metrics into a DataFrame for Evidently
    reference_df = pd.DataFrame([reference_data])
    current_df = pd.DataFrame([current_data])
    
    print("latest_run_id", latest_run_id)
    print("reference_run_id", reference_run_id)
    print("reference_df", reference_df)

    # Create a column mapping for Evidently (adjust according to your data)
    data = [reference_df, current_df]

    # Push the metrics to Evidently using Airflow XCom
    kwargs['ti'].xcom_push(key='evidently_metrics', value=data)
    
    

# Send metrics to Evidently Cloud
def send_to_evidently(**kwargs):
    data = kwargs['ti'].xcom_pull(key='evidently_metrics', task_ids='calculate_metrics')
    reference_df = data[0]
    current_df = data[1]
    
    print("reference_df", reference_df)
    print("current_df", current_df)
    # If you have specific target or prediction columns, you can define them here
    column_mapping = ColumnMapping(
        prediction='prediction',  # Specify prediction column name if available
        target='is_fraud',       # Specify target column name if available
        numerical_features=list(reference_df.columns),  # Assuming all are numerical
        categorical_features=None
    )
    
   
    ws = CloudWorkspace(
    token=EVIDENTLY_API_TOKEN,
    url=EVIDENTLY_BASE_URL
    )
    
     # Create an Evidently report for data drift
    report = Report(metrics=[DataDriftPreset()])

    # Run the report using the reference and current data
    report.run(reference_data=reference_df, current_data=current_df, column_mapping=column_mapping)
    
    # Retrieve the metrics as a dictionary
    metrics = report.as_dict()
    
    
    # Send report to Evidently Cloud
    projectId = EVIDENTLY_PROJECT_ID
    
    ws.add_report(projectId, report)
    
    
    

# Define the DAG
dag = DAG(
    'fd_training_data_reporting',
    description='Retrieve reference and current models from S3 and send metrics to Evidently',
    schedule_interval='@daily',  # Adjust schedule as needed
    start_date=datetime(2024, 11, 25),
    catchup=False,
)

# Task to calculate metrics using Evidently
calculate_metrics_task = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics,
    provide_context=True,
    dag=dag,
)

# Task to send metrics to Evidently Cloud
send_metrics_to_evidently_task = PythonOperator(
    task_id='send_metrics_to_evidently',
    python_callable=send_to_evidently,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
calculate_metrics_task >> send_metrics_to_evidently_task

