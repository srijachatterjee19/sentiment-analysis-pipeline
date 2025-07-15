from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_bigquery_sentiment',
    default_args=default_args,
    description='Run Apache Spark job to process sentiment data',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'bigquery', 'sentiment'],
) as dag:

    run_spark_job = BashOperator(
        task_id='submit_spark_job',
        bash_command="""
        /spark/bin/spark-submit \
        --jars https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
        /opt/spark_job.py
        """,
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/spark_job/key.json'
        }
    )

    run_spark_job
