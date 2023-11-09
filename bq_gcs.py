# Libraries
import json
import os
import datetime
import pendulum
import pandas as pd

# Airflow
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator


# --------------------------------------------------------------------------------
# Scheduling
# --------------------------------------------------------------------------------
LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")

DAG_NAME = "bq_gcs"# DAG name (proposed format: lowercase underscore). Should be unique.
DAG_START_DATE = datetime.datetime(2023, 4, 1, tzinfo=LOCAL_TZ) # Startdate. When setting the "catchup" parameter to True, you can perform a backfill when you insert a specific date here like datetime(2021, 6, 20)
DAG_SCHEDULE_INTERVAL = "0 9 * * *" # Cron notation -> see https://airflow.apache.org/scheduler.html#dag-runs
DAG_CATCHUP = False # When set to true, DAG will start running from DAG_START_DATE instead of current date
DAG_PAUSED_UPON_CREATION = True # Defaults to False. When set to True, uploading a DAG for the first time, the DAG doesn't start directly 
DAG_MAX_ACTIVE_RUNS = 5 # Configure efficiency: Max. number of active runs for this DAG. Scheduler will not create new active DAG runs once this limit is hit. 

# --------------------------------------------------------------------------------
# Default DAG arguments
# --------------------------------------------------------------------------------

default_dag_args = {
    'owner': 'airflow',
    'start_date': DAG_START_DATE,
    'depends_on_past': False,
    'email': models.Variable.get('email_monitoring'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=30)
}

# --------------------------------------------------------------------------------
# Workflow configuration
# --------------------------------------------------------------------------------

AIRFLOW_TEMP_FOLDER = "/home/airflow/gcs/data/{}/".format(DAG_NAME)

GCP_CONNECTION_ID = 'xxxx'
GCP_GCS_BUCKET = 'xxxx'
GCP_BQ_PROJECT = 'xxxx'

GCP_BQ_EXPORTS = [{
	"name": "campaign_boxspring",
	"sql": """
		SELECT * FROM `project.dataset.table` WHERE order_date = '{run_date}'
	""",
	"table": "project.dataset.table",
	"folder": "folder_name",
	"file_name": "file_name"
}]

# --------------------------------------------------------------------------------
# Repeatable functions
# --------------------------------------------------------------------------------

# Create tables from SQL
def bigquery_formatted_table(i, **kwargs):
	return  BigQueryExecuteQueryOperator(
			task_id="bigquery_formatted_table_" + GCP_BQ_EXPORTS[i]["name"],
			sql=GCP_BQ_EXPORTS[i]["sql"].format(
				run_date="{{ ds }}"
			),
			use_legacy_sql=False,
			write_disposition="WRITE_TRUNCATE",
			allow_large_results=True,
			destination_dataset_table=GCP_BQ_EXPORTS[i]["table"] + "_{{ ds_nodash }}",
			gcp_conn_id=GCP_CONNECTION_ID,
			location="EU")

def bigquery_to_gcs(i, **kwargs):
	return BigQueryToGCSOperator(
		task_id="bigquery_to_gcs_" + GCP_BQ_EXPORTS[i]["name"],
		source_project_dataset_table=GCP_BQ_EXPORTS[i]["table"] + "_{{ ds_nodash }}",
		destination_cloud_storage_uris=[
			"gs://" + GCP_GCS_BUCKET + "/" + GCP_BQ_EXPORTS[i]["folder"] + "/" + GCP_BQ_EXPORTS[i]["file_name"] + "{{ next_ds_nodash }}.csv"
		],
		export_format='CSV',
		print_header=True,
		field_delimiter=',',
		compression="NONE",
		gcp_conn_id=GCP_CONNECTION_ID,
		task_concurrency=2)

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

with models.DAG(
		DAG_NAME,
		schedule_interval=DAG_SCHEDULE_INTERVAL,
		catchup=DAG_CATCHUP,
		max_active_runs = DAG_MAX_ACTIVE_RUNS,
		is_paused_upon_creation = DAG_PAUSED_UPON_CREATION,
		default_args=default_dag_args) as dag:

	start = DummyOperator(
		task_id='start',
		trigger_rule='all_success')

	complete = DummyOperator(
		task_id='complete',
		trigger_rule='all_success')
	
	# Set basic / dependend tasks
	for i, val in enumerate(GCP_BQ_EXPORTS):
		start \
		>> bigquery_formatted_table(i) \
		>> bigquery_to_gcs(i) \
		>> complete
