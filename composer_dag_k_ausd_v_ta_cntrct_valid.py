# Apache Airflow DAG for orchestrating the BigQuery contract validity job
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.models import Variable

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define Airflow Variables for BigQuery project, dataset, and job-specific parameters
# These should be created in the Airflow UI (Admin -> Variables)
# Example:
# Key: bq_project_id, Value: your-gcp-project-id
# Key: bq_dataset_id, Value: your_dataset_name
# Key: job_kennung_param, Value: MY_JOB_KENNUNG
# Key: eintrags_nr_param, Value: ENTRY_123

BIGQUERY_PROJECT_ID = Variable.get("bq_project_id", "your-gcp-project-id")
BIGQUERY_DATASET_ID = Variable.get("bq_dataset_id", "dataset")
JOB_KENNUNG_PARAM = Variable.get("job_kennung_param", "DEFAULT_JOB_KENNUNG")
EINTRAGS_NR_PARAM = Variable.get("eintrags_nr_param", "DEFAULT_ENTRY_NR")

with DAG(
    dag_id="k_ausd_v_ta_cntrct_valid_bigquery_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # Set your desired schedule here, e.g., "@daily", "0 0 * * *"
    tags=["bigquery", "etl", "isbert"],
    default_args=default_args,
    description="Orchestrates the BigQuery stored procedure for contract validity data.",
) as dag:
    call_bigquery_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_r_ausd_vertrag_sp",
        project_id=BIGQUERY_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET_ID,
        procedure="r_ausd_vertrag",
        gcp_conn_id="google_cloud_default",  # Assumes 'google_cloud_default' connection is configured
        parameters={
            "p_JobKennung": JOB_KENNUNG_PARAM,
            "p_EintragsNr": EINTRAGS_NR_PARAM,
        },
    )

    # Future tasks could include:
    # - Data quality checks
    # - Notifications (e.g., Slack, Email)
    # - Triggering downstream DAGs