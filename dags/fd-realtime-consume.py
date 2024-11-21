import boto3
import time
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
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
from airflow.models.dag import DAG
import numpy as np


MODEL_URI = Variable.get("MODEL_URI")
S3_KEY=Variable.get("S3_KEY")
S3_BUCKET_NAME=Variable.get("S3_BUCKET_NAME")


# DAG configuration
DAG_ID = 'fd_data_consume_dag'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 18),
    'retries': 2,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    default_args=default_args,
    description="Upload or append data to S3 for fraud detection training",
    catchup=False,
    tags=['fraud-detection-upload-s3'],
) as dag:
    
    # Your custom database URL (Neon Tech Postgres connection)
    @task
    def query_postgres(**kwargs):
        """
        Fetch transaction data from the DAG run configuration.
        """
        config = kwargs.get('dag_run').conf
        
        id =""
        trans_num = ""
        
        if config is None:
            logging.warning("No transaction data found in config. Please check dag_run.conf.")
        else:
            logging.info(f"Received transaction data: {config}")
            id = config.get('id')
            trans_num = config.get('trans_num')
                
        try:
            # Use PostgresHook to connect to the Neon database
            hook = PostgresHook(postgres_conn_id="postgres_neon")
            conn = hook.get_conn()  # Get the connection object
            cursor = conn.cursor()

            
            # Sample query with the transaction ID
            query = f"SELECT * FROM transaction WHERE id={id} and trans_num='{trans_num}';"
            
            cursor.execute(query)
            result = cursor.fetchall()  # Get the query result
            cursor.close()
            
            # Log and return the result to store it in XCom
            logging.info("Query result: %s", result)
            return result  # This will be stored in XCom
        except Exception as e:
            logging.error(f"Failed to query the database: {e}")
            return None
        
        
        
    @task
    def mlflow_predict(data):
        """
        Use an MLflow model to predict based on the data fetched from the database.
        """
        if not data:
            logging.error("No data received for prediction.")
            return None

        try:
            # Convert data to a pandas DataFrame with appropriate columns
            df = pd.DataFrame(data, columns=["id","trans_date_trans_time","cc_num","merchant","category",
                                             "amt","first","last","gender","street","city","state","zip",
                                             "lat","long","city_pop","job","dob","trans_num","unix_time","merch_lat","merch_long", "is_fraud"])
            
            print("df :--", df.head())

            # Feature engineering
            df['distance_to_merchant'] = np.sqrt((df['lat'] - df['merch_lat'])**2 + (df['long'] - df['merch_long'])**2)
            df['trans_dayofweek'] = pd.to_datetime(df['trans_date_trans_time']).dt.dayofweek
            df['trans_hour'] = pd.to_datetime(df['trans_date_trans_time']).dt.hour

            # Prepare the features for prediction
            features = ['amt', 'distance_to_merchant', 'city_pop', 'trans_dayofweek', 'trans_hour']
            X = df[features]

            logging.info(f"Data for prediction: {X}")
        except Exception as e:
            logging.error(f"Error converting data to DataFrame or feature engineering: {e}")
            return None

        try:
            # Retrieve AWS credentials from the aws_s3_default connection using S3Hook
            s3_hook = S3Hook(aws_conn_id='aws_s3_default')

            # Set up environment variables for AWS credentials from the connection
            aws_access_key_id, aws_secret_access_key = s3_hook.get_credentials().access_key, s3_hook.get_credentials().secret_key

            # Set environment variables for MLflow to use
            os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key

            # Use the S3 model URI pointing to your specific model
            model_uri ="s3://flow-bucket-ml/training-models/3/4f6b131867bd484e88de2234eb15e868/artifacts/fraud_detection_model/"
            model = mlflow.pyfunc.load_model(model_uri)
            # Get the dependencies of the model
            model_dependencies = mlflow.pyfunc.get_model_dependencies(model_uri)

            # Predict using the MLflow model
            predictions = model.predict(X)

            # Log predictions
            logging.info(f"Predictions: {predictions}")
            return predictions.tolist()  # Return predictions for potential further use
        except Exception as e:
            logging.error(f"Failed to perform prediction: {e}")
            return None
    
    
    @task
    def process_result(query_result, predictions):
        if query_result:
            logging.info(f"Processing query result: {query_result}")
            
            for data in query_result:
                # for x in range(19):
                #     print(f"data[{x}]: {data[x]}")
                
                transformed_data = [
                    data[0],  # ID
                    data[1].strftime('%Y-%m-%d %H:%M:%S'),  # Timestamp formatted to string
                    str(data[2]),  # Long ID
                    data[3],  # Name
                    data[4],
                    str(data[5]),  # Amount (2.86 as string)
                    data[6],  # First name
                    data[7],  # Last name
                    data[8],  # Gender
                    data[9],
                    data[10], 
                    data[11],
                    data[12],  
                    str(data[13]),  # Latitude
                    str(data[14]),  # Longitude
                    str(data[15]),  # Employee ID
                    data[16],  # Job title
                    data[17].strftime('%Y-%m-%d'),  # Birthdate formatted as date string
                    data[18],  # Hashed ID
                    data[19],
                    float(data[20]),
                    float(data[21]),
                    predictions[0]
                ]

            return transformed_data
                
        else:
            logging.warning("No query result to process.")
            return None
    
    
    
    @task
    def upload_or_append_to_s3(transaction_data):
        """
        Uploads transaction data to S3 in CSV format, appending to an existing file if present.
        """
        
        S3_KEY={S3_KEY}
        S3_BUCKET_NAME={S3_BUCKET_NAME}
        
        
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

            
            # Append the transaction data as a new row
            csv_rows.append(transaction_data)

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
        
    
    
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def update_database_processed(**kwargs):
        """
        Update transaction data from the DAG run configuration.
        """
        config = kwargs.get('dag_run').conf
        task_instance = kwargs['ti']
        id = config.get('id')
        trans_num = config.get('trans_num')
        
        # Fetch prediction results using XCom
        prediction_results = task_instance.xcom_pull(task_ids='mlflow_predict')
        
        try:
            # Use PostgresHook to connect to the Neon database
            hook = PostgresHook(postgres_conn_id="postgres_neon")
            conn = hook.get_conn()  # Get the connection object
            cursor = conn.cursor()

            # Sample query with the transaction ID
            query = f"UPDATE transaction SET is_fraud = {prediction_results[0]} WHERE id={id} and trans_num='{trans_num}';"
            
            cursor.execute(query)
            conn.commit()  # Commit the changes to the database
            cursor.close()
            
            # Log the update
            logging.info("Transaction updated successfully.")
            
        except Exception as e:
            logging.error(f"Failed to update the database: {e}")
            raise
        
    


    # Define the tasks and task dependencies
    query_task = query_postgres()
    prediction_results = mlflow_predict(query_task)
    process_task = process_result(query_task, prediction_results)  # Pass the result to the next task
    upload_s3 = upload_or_append_to_s3(process_task)
    upload_transaction_postgres = update_database_processed()

    query_task >> prediction_results >> process_task >> upload_s3 >> upload_transaction_postgres
