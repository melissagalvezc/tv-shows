"""
Airflow DAG for TV Shows ETL pipeline.

This DAG:
1. Extracts data by invoking the TV Shows Sync Lambda function
2. Loads the data from S3 into PostgreSQL database (raw schema)
3. Transforms data using dbt models (staging, marts, reporting)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import json
import logging
import os

logger = logging.getLogger(__name__)

DAG_ID = "tv_shows_etl"
LAMBDA_FUNCTION_NAME = "tv-shows-sync-prod-sync"
AWS_REGION = "us-east-1"
S3_BUCKET = "tv-shows-data"
S3_PREFIX = "tv_shows_data/api"
NEON_CONN_ID = "neon_postgres"
DBT_PROJECT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dbt")
DBT_PROFILES_DIR = os.path.join(DBT_PROJECT_DIR)

LAMBDA_PAYLOAD = json.dumps({"backfill": "false"})

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

def to_postgres(**context):
    """Load today's JSON file from S3 into PostgreSQL database."""
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    postgres_hook = PostgresHook(postgres_conn_id=NEON_CONN_ID)
    
    # Get today's date in the format used in S3 path: YYYY/MM/DD
    today = datetime.now().strftime("%Y/%m/%d")
    today_prefix = f"{S3_PREFIX}/load_date={today}/"
    
    # Note: S3_PREFIX is "tv_shows_data/api", so full path is:
    # tv_shows_data/api/load_date=YYYY/MM/DD/tv_shows_api_TIMESTAMP.json
    
    logger.info(f"Looking for files from today ({today}) in s3://{S3_BUCKET}/{today_prefix}")
    
    # List all objects with today's prefix
    keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=today_prefix)
    
    if not keys:
        error_msg = f"No hay archivo de hoy ({today}) en s3://{S3_BUCKET}/{today_prefix}"
        logger.warning(error_msg)
        raise ValueError(error_msg)
    
    # Filter JSON files
    json_keys = [k for k in keys if k.endswith(".json")]
    if not json_keys:
        error_msg = f"No hay archivo JSON de hoy ({today}) en s3://{S3_BUCKET}/{today_prefix}"
        logger.warning(error_msg)
        raise ValueError(error_msg)
    
    # Get the latest file from today (in case there are multiple)
    latest_key = sorted(json_keys)[-1]
    logger.info(f"File found for today: {latest_key}")
    
    # Download and parse the content
    content = s3_hook.read_key(key=latest_key, bucket_name=S3_BUCKET)
    data = json.loads(content)
    
    shows = data.get("shows", [])
    if not shows:
        logger.warning("No shows found in the JSON data")
        return
    
    logger.info(f"Loading {len(shows)} shows as raw JSON into PostgreSQL database")
    
    insert_sql = """
    INSERT INTO raw.tv_shows_raw (show_data, source_file)
    VALUES (%s::jsonb, %s)
    """
    
    inserted_count = 0
    
    for show in shows:
        try:
            # Insert the show JSON as-is (raw, no transformations)
            show_json = json.dumps(show)
            postgres_hook.run(insert_sql, parameters=(show_json, latest_key))
            inserted_count += 1
        except Exception as e:
            logger.error(f"Error inserting show {show.get('id')}: {str(e)}")
            raise
    
    logger.info(f"Successfully loaded {inserted_count} shows as raw JSON into PostgreSQL database")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="TV Shows ETL pipeline: Extract, Load, and Transform with dbt",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tv-shows", "etl", "lambda", "s3", "postgres", "dbt"],
) as dag:
    
    # Sync Group: Extract and Load data
    with DAG.task_group("sync") as sync_group:
        # Extract: Invoke Lambda to fetch data and upload to S3
        extract_task = LambdaInvokeFunctionOperator(
            task_id="extract_tv_shows_data",
            function_name=LAMBDA_FUNCTION_NAME,
            payload=LAMBDA_PAYLOAD,
            aws_conn_id="aws_conn",
        )
        
        # Load: Get today's file from S3 and load into PostgreSQL database
        load_data = PythonOperator(
            task_id="to_postgres",
            python_callable=to_postgres,
        )
        
        # Set dependencies within sync group
        extract_task >> load_data
    
    # DBT Group: Transform data using dbt models
    # Cosmos will create individual tasks for each dbt model
    profile_config = ProfileConfig(
        profile_name="tv_shows_dbt",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=NEON_CONN_ID,
            profile_args={
                "schema": "staging",
            },
        ),
    )
    
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR,
    )
    
    execution_config = ExecutionConfig(
        dbt_executable_path="dbt",
    )
    
    dbt_group = DbtTaskGroup(
        group_id="dbt",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )
    
   
    sync_group >> dbt_group
