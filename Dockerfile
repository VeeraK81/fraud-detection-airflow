# Use an official Airflow image as base
FROM apache/airflow:2.7.0

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=300
ENV AIRFLOW__WEBSERVER__WORKER_CLASS=gevent
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=7860
ENV AWS_DEFAULT_REGION=eu-west-3
ENV AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth
ENV AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
# Set environment variables for SMTP

# Switch user
USER root

# COPY DAGS & PEM Key
COPY ./dags /opt/airflow/dags


RUN --mount=type=secret,id=SERVER_SECRETS,mode=0444 \
    cat /run/secrets/SERVER_SECRETS > /opt/airflow/ml-training-key.pem && \
    chmod 600 /opt/airflow/ml-training-key.pem

# Initialize the Airflow database (PostgreSQL in this case)
# IF YOU WANT TO HAVE THAT RUNNING IN HUGGINGFACE, YOU NEED TO HARD CODE THE VALUE HERE UNFORTUNATELY
# DON'T STAGE THAT IN A PRIVATE REPO BECAUSE THE ENV VARIABLE IS HARD CODED IN PLAIN TEXT
# IF YOU STAGE THAT IN HUGGING FACE SPACE, YOU DON'T HAVE A CHOICE THOUGH
# SO MAKE SURE YOUR SPACE IS PRIVATE
# GET POSTGRES URL FROM HUGGING FACE SECRETs
RUN --mount=type=secret,id=DBURL,mode=0444,required=true \
    cat /run/secrets/DBURL > /tmp/DBURL 

    
RUN usermod -u 1000 airflow

# Ensure correct permissions for the .pem file
RUN chmod 400 /opt/airflow/ml-training-key.pem \
   && chown airflow /opt/airflow/ml-training-key.pem

USER airflow

ENV BACKEND_STORE_URI=$MLFLOW_BACKEND_STORE

# Install any additional dependencies if needed
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

USER root

# Copy entrypoint script and ensure correct permissions
COPY --chown=root:root entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


ENTRYPOINT ["/entrypoint.sh"]
USER airflow

# Expose the necessary ports (optional if Hugging Face already handles port exposure)
EXPOSE 7860

# Start Airflow webserver and scheduler within the same container
CMD ["bash", "-c", "airflow scheduler & airflow webserver"]
