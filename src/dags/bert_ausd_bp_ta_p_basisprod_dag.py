# Legacy Source: DW.BERT_AUSD_BP_TA_P_BASISPROD.xml (UC4), r_ausd_bp_ta_p_basisprod.ksh, k_ausd_bp_ta_p_basisprod.ksh
# Job: DW.BERT_AUSD_BP_TA_P_BASISPROD
#
# Apache Airflow DAG for preparing Basisprodukte data for BERT system in BigQuery.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "project_id"  # Replace with your GCP Project ID
DWH_DATASET = "bert_dwh_prod"
TARGET_TABLE = f"{PROJECT_ID}.{DWH_DATASET}.sof_ta_p_basisprod"
DWTK_MELDUNGEN_TABLE = f"{PROJECT_ID}.{DWH_DATASET}.dwtk_meldungen"

def _get_stichtag(**kwargs):
    """
    Determines the 'stichtag' (reference date) based on the logic from the
    original Oracle script's v_datum calculation.
    """
    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = hook.get_client(project_id=PROJECT_ID)

    query = f"""
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
    FROM
        `{DWTK_MELDUNGEN_TABLE}`
    WHERE
        job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    # Execute query and fetch result
    query_job = client.query(query)
    rows = query_job.result()
    stichtag = next(rows)[0] # Get the first column of the first row

    kwargs['ti'].xcom_push(key='stichtag', value=stichtag)
    print(f"Determined stichtag: {stichtag}")

def _log_status(**kwargs):
    """
    Placeholder for logging job status and errors.
    In a real implementation, this would integrate with Cloud Logging or
    a dedicated BigQuery logging table.
    """
    task_instance = kwargs['ti']
    job_status = "SUCCESS" # Assume success if this task is reached
    # Add more sophisticated logging, e.g.,
    # task_instance.xcom_pull(task_ids='execute_transformation_task', key='return_value')
    print(f"Job DW.BERT_AUSD_BP_TA_P_BASISPROD completed with status: {job_status}")
    # Example: Push to a BigQuery logging table
    # log_entry = {
    #     "job_name": "DW.BERT_AUSD_BP_TA_P_BASISPROD",
    #     "execution_date": kwargs['ds'],
    #     "status": job_status,
    #     "stichtag": task_instance.xcom_pull(key='stichtag')
    # }
    # bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    # bq_hook.insert_rows(table_id='your_logging_table', dataset_id='your_logging_dataset', rows=[log_entry])


with DAG(
    dag_id="bert_ausd_bp_ta_p_basisprod_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., "0 0 * * *" for daily
    catchup=False,
    tags=["bert", "basisprod", "dwh"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "project_id": PROJECT_ID,
    },
) as dag:
    parameter_setup_task = PythonOperator(
        task_id="parameter_setup_and_date_determination",
        python_callable=_get_stichtag,
    )

    truncate_target_table_task = BigQueryExecuteQueryOperator(
        task_id="truncate_target_table",
        sql=f"TRUNCATE TABLE `{TARGET_TABLE}`;",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )

    execute_transformation_task = BigQueryExecuteQueryOperator(
        task_id="execute_bigquery_transformation",
        sql="{% include 'sql/d_ausd_bp_ta_p_basisprod_bq.sql' %}",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )

    logging_task = PythonOperator(
        task_id="logging_and_status_update",
        python_callable=_log_status,
        trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
    )

    parameter_setup_task >> truncate_target_table_task >> execute_transformation_task >> logging_task