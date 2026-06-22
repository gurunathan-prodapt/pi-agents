# Apache Airflow DAG for k_ausd_adressen.ksh migration
# Replaces legacy job k_ausd_adressen.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
from airflow.models.variable import Variable

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='k_ausd_adressen_dag',
    default_args=default_args,
    description='Orchestrates BigQuery Stored Procedure for address data preparation',
    schedule_interval=None, # Define your schedule, e.g., '0 3 * * *' for daily at 3 AM UTC
    catchup=False,
    tags=['bigquery', 'data_preparation'],
) as dag:
    # Define BigQuery project and dataset from Airflow Variables or environment
    # Ensure these variables are set in your Airflow environment
    # e.g., BQ_PROJECT_ID, BQ_DATASET_ID for the stored procedure, BQ_METRICS_DATASET_ID
    BQ_PROJECT_ID = Variable.get("BQ_PROJECT_ID", default="your-gcp-project-id")
    BQ_DATASET_ID = Variable.get("BQ_DATASET_ID", default="dataset") # Dataset where the SP resides
    BQ_SP_NAME = "sp_ausd_adressen_main"

    # The 'logical_date' (execution_date for Airflow < 2.2) is often used as the key date.
    # We'll format it to YYYYMMDD as required by the stored procedure.
    # Note: 'ds' is a common Airflow macro for execution_date in YYYY-MM-DD format.
    # We need YYYYMMDD, so we'll reformat it.
    stichtag_str = "{{ ds_nodash }}" # Example: '20230101' for execution date 2023-01-01

    execute_address_preparation = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_address_preparation_sp',
        project_id=BQ_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        procedure_id=BQ_SP_NAME,
        parameters=[
            {"name": "p_job_kennung", "parameterType": {"type": "STRING"}, "value": "K_AUSD_ADRESSEN"},
            {"name": "p_eintrags_nr", "parameterType": {"type": "INT64"}, "value": "1"}, # Example value
            {"name": "p_stichtag_str", "parameterType": {"type": "STRING"}, "value": stichtag_str},
            {"name": "p_wiederanlauf_wert", "parameterType": {"type": "STRING"}, "value": "0"} # Example value
        ],
        gcp_conn_id='google_cloud_default', # Ensure this connection exists
    )

    # Further tasks can be added here, e.g., for monitoring or dependent jobs.
    # For instance, if the SP returns a status or record count, you could capture it.

# Note on Parameters:
# - p_job_kennung, p_eintrags_nr, p_wiederanlauf_wert: These might need to be dynamic
#   Airflow Variables or XComs depending on how k_ausd_adressen.ksh passed them.
#   For this example, they are hardcoded or using simple defaults/macros.
# - p_stichtag_str: Uses Airflow's 'ds_nodash' macro, which gives the execution date
#   in YYYYMMDD format. Adjust if a different date is needed (e.g., yesterday's date).
# - BQ_PROJECT_ID and BQ_DATASET_ID should be configured as Airflow Variables
#   in your Airflow environment for proper execution.

# To deploy, ensure the BigQuery Stored Procedure is already deployed in your GCP project.