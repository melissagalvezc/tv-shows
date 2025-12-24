"""
Airflow DAG for TV Shows data extraction.

This DAG:
1. Invokes the TV Shows Sync Lambda function to extract data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeOperator
from airflow.utils.dates import days_ago
import json

DAG_ID = 'tv_shows_extract'
SCHEDULE_INTERVAL = None
START_DATE = days_ago(1)

LAMBDA_FUNCTION_NAME = 'tv-shows-sync-prod-sync'
AWS_REGION = 'us-east-1'

LAMBDA_PAYLOAD = json.dumps({
    "backfill": "false"
})

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='TV Shows data extraction via Lambda function',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    tags=['tv-shows', 'lambda', 'data-extraction'],
)

extract_task = LambdaInvokeOperator(
    task_id='extract_tv_shows_data',
    function_name=LAMBDA_FUNCTION_NAME,
    payload=LAMBDA_PAYLOAD,
    aws_conn_id='aws_default',
    region_name=AWS_REGION,
    dag=dag,
)

