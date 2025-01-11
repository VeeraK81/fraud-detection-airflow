from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
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

