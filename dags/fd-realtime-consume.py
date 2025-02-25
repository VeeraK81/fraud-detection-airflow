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
from airflow.utils.dates import days_ago
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

# S3 Configuration: Load from Airflow Variables
MODEL_URI = Variable.get("MODEL_URI")
S3_KEY=Variable.get("S3_KEY")
S3_BUCKET_NAME=Variable.get("S3_BUCKET_NAME")


# DAG configuration
DAG_ID = 'fd_data_consume_dag'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 0 * * *',
    default_args=default_args,
    description="Upload or append data to S3 for fraud detection training",
    catchup=False,
    tags=['fraud-detection-upload-s3'],
) as dag:
    
    # Send query to database to retrieve transactions
    @task
    def query_postgres(**kwargs):
        """
        Fetch transaction data from the DAG run configuration.
        """
        config = kwargs.get('dag_run').conf
        
        # create empty variables  
        id =""
        trans_num = ""
        
        if config is None:
            logging.warning("No transaction data found in config. Please check dag_run.conf.")
        else:
            logging.info(f"Received transaction data: {config}")
            id = config.get('transaction_id')
            trans_num = config.get('trans_num')
                
        try:
            # Use PostgresHook to connect to the database
            hook = PostgresHook(postgres_conn_id="postgres_neon")
            conn = hook.get_conn()  # Get the connection object
            
            # Using a context manager to ensure that cursor is closed after use
            with conn.cursor() as cursor:
                # Parameterized query to prevent SQL injection
                query = '''
                    SELECT 
                        t.transaction_id AS id,
                        t.trans_date AS trans_date_trans_time,
                        c.cc_num,
                        m.merchant_name AS merchant,
                        m.category,
                        t.amount AS amt,
                        c.first_name AS first,
                        c.last_name AS last,
                        c.gender,
                        c.street,
                        c.city,
                        c.state,
                        c.zip,
                        c.lat,
                        c.long,
                        c.city_pop,
                        c.job,
                        c.dob,
                        t.trans_num,
                        t.unix_time,
                        lm.lat AS merch_lat,
                        lm.long AS merch_long,
                        is_fraud
                    FROM 
                        Transactions t
                        LEFT JOIN Customers c ON t.customer_id = c.customer_id
                        LEFT JOIN Merchants m ON t.merchant_id = m.merchant_id
                        LEFT JOIN Products p ON t.product_id = p.product_id
                        LEFT JOIN Location l ON t.location_id = l.location_id
                        LEFT JOIN Payments pay ON t.transaction_id = pay.transaction_id
                        LEFT JOIN Location lm ON m.location_id = lm.location_id
                        LEFT JOIN Refunds r ON t.transaction_id = r.transaction_id
                        LEFT JOIN Chargebacks cb ON t.transaction_id = cb.transaction_id
                        LEFT JOIN Subscriptions sub ON c.customer_id = sub.customer_id
                    WHERE t.transaction_id = %s AND t.trans_num = %s;
                '''
                
                # Execute the query with parameters to avoid SQL injection
                cursor.execute(query, (id, trans_num))
                
                # Fetch the result
                result = cursor.fetchall()
                
                # Log and return the result to store it in XCom
                logging.info("Query result: %s", result)
                return result  # This will be stored in XCom

        except Exception as e:
            logging.error(f"Failed to query the database: {e}")
            return None
        
        
    # Download model and predict
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
            s3_hook = S3Hook(aws_conn_id='aws_default')

            # Set up environment variables for AWS credentials from the connection
            aws_access_key_id, aws_secret_access_key = s3_hook.get_credentials().access_key, s3_hook.get_credentials().secret_key

            # Set environment variables for MLflow to use
            os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key

            # Use the S3 model URI pointing to your specific model
            model = mlflow.pyfunc.load_model(MODEL_URI)
            # Get the dependencies of the model
            model_dependencies = mlflow.pyfunc.get_model_dependencies(MODEL_URI)

            # Predict using the MLflow model
            predictions = model.predict(X)

            # Log predictions
            logging.info(f"Predictions: {predictions}")
            return predictions.tolist()  # Return predictions for potential further use
        except Exception as e:
            logging.error(f"Failed to perform prediction: {e}")
            return None
    
    
    # It processes query results and predictions, transforming them into a structured format.
    @task
    def process_result(query_result, predictions):
        # Check if there is a query result to process
        if query_result:
            logging.info(f"Processing query result: {query_result}")
            
            # Iterate through each row of the query result
            for data in query_result:
                # Example of inspecting individual fields in the data (commented out)
                # for x in range(19):
                #     print(f"data[{x}]: {data[x]}")
                
                # Transform the data into a desired structure with specific formatting
                transformed_data = [
                    data[0],  # ID
                    data[1].strftime('%Y-%m-%d %H:%M:%S'),  # Timestamp formatted as a string
                    str(data[2]),  # Long ID converted to a string
                    data[3],  # Name
                    data[4],  # Field 5
                    str(data[5]),  # Amount (e.g., 2.86) converted to a string
                    data[6],  # First name
                    data[7],  # Last name
                    data[8],  # Gender
                    data[9],  # Field 10
                    data[10],  # Field 11
                    data[11],  # Field 12
                    data[12],  # Field 13
                    str(data[13]),  # Latitude converted to a string
                    str(data[14]),  # Longitude converted to a string
                    str(data[15]),  # Employee ID converted to a string
                    data[16],  # Job title
                    data[17].strftime('%Y-%m-%d'),  # Birthdate formatted as a date string
                    data[18],  # Hashed ID
                    data[19],  # Field 20
                    float(data[20]),  # Field 21 converted to a float
                    float(data[21]),  # Field 22 converted to a float
                    predictions[0]  # Prediction value (first element in predictions)
                ]

            return transformed_data  # Return the processed and transformed data
                
        else:
            # Log a warning if there is no query result to process
            logging.warning("No query result to process.")
            return None
    
    
    # It processes append data to S3 bucket
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
        
    
    # It processes update results and predictions
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def update_database_processed(**kwargs):
        """
        Update transaction data from the DAG run configuration.
        """
        config = kwargs.get('dag_run').conf
        task_instance = kwargs['ti']
        id = config.get('transaction_id')
        trans_num = config.get('trans_num')
        
        # Fetch prediction results using XCom
        prediction_results = task_instance.xcom_pull(task_ids='mlflow_predict')
        
        try:
            # Use PostgresHook to connect to the Neon database
            hook = PostgresHook(postgres_conn_id="postgres_neon")
            conn = hook.get_conn()  # Get the connection object
            cursor = conn.cursor()

            # Sample query to update the transaction record with fraud detection status
            query_update = """
                UPDATE transactions 
                SET is_fraud = %s 
                WHERE transaction_id = %s AND trans_num = %s;
            """
            cursor.execute(query_update, (prediction_results[0], id, trans_num))
            conn.commit()  # Commit the changes to the database
            
            # Purposefully set prediction value as 1 in order to check email alert.  It should be removed.
            prediction_results[0]=1
            
            if prediction_results and any(pred == 1 for pred in prediction_results):
                logging.info("Fraud detected. Sending email notification.")
                # Sample query to insert a new record in the transaction_fraud_detection table
                query_insert = """
                    INSERT INTO transactions_fraud_detection (id, trans_num, is_fraud)
                    VALUES (%s, %s, %s);
                """
                cursor.execute(query_insert, (id, trans_num, prediction_results[0]))
                conn.commit()  # Commit the changes to the database
            else:
                logging.info("No fraud detected.")

            cursor.close()

            # Log the update
            logging.info("Transaction updated and fraud detection logged successfully.")

        except Exception as e:
            logging.warning(f"Failed to update the database: {e}")
            return None
        
    
    # Define the tasks and task dependencies
    query_task = query_postgres()
    prediction_results = mlflow_predict(query_task)    
    process_task = process_result(query_task, prediction_results)  # Pass the result to the next task
    upload_s3 = upload_or_append_to_s3(process_task)
    upload_transaction_postgres = update_database_processed()

    # Define the task flow sequence in the pipeline
    query_task >> prediction_results >> process_task >> upload_s3 >> upload_transaction_postgres
    
