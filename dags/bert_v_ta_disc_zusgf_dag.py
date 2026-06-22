# Legacy Source: DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml, r_ausd_v_ta_disc_zusgf.ksh, k_ausd_v_ta_disc_zusgf.ksh
# Job: BERT_V_TA_DISC_ZUSGF
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# Import the transformation script
# Assuming src/python is in the PYTHONPATH or airflow's dags folder structure allows this
# If not, the PythonOperator might need to execute the script directly or load it differently.
# For simplicity, assuming it's available in the environment.
from transform_discount_data import transform_and_load_discount_data

# Define BigQuery project and dataset names - replace with actual values
# These could also be pulled from Airflow Variables
GCP_PROJECT_ID = "your-gcp-project-id"
BQ_DATASET_ID = "your_bigquery_dataset"
TARGET_TABLE_NAME = "sof_ta_disc_zusgf"
SOURCE_DISCOUNT_TABLE = "sof_ta_discount"
SOURCE_DATE_TABLE = "dwtk_meldungen" # Table for v_datum equivalent

def get_sysdate_equivalent(project_id: str, dataset_id: str, source_date_table: str, **kwargs):
    """
    Fetches the equivalent of v_datum from BigQuery.
    The legacy logic suggests 'v_datum' from 'dwtk_meldungen'.
    This task queries the max 'dat_gueltig_ab' for 'kenn_meldungsart' = 'DISCOUNT'.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
        MAX(dat_gueltig_ab)
    FROM
        `{project_id}.{dataset_id}.{source_date_table}`
    WHERE
        kenn_meldungsart = 'DISCOUNT' -- Example filter, adjust as per actual legacy logic
    """
    query_job = client.query(query)
    result = query_job.result()
    v_datum = None
    for row in result:
        v_datum = row[0] # Assuming only one column is returned
        break # Take the first (and only) row
    
    if v_datum:
        # Push v_datum to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='v_datum', value=str(v_datum))
        print(f"Fetched v_datum equivalent: {v_datum}")
    else:
        print("Could not determine v_datum equivalent. Proceeding without it or raising error.")
        # Depending on criticality, raise an error or set a default
        # For now, we allow it to proceed, assuming `transform_and_load_discount_data`
        # doesn't strictly depend on `v_datum` for its core logic as currently implemented.

with DAG(
    dag_id="bert_v_ta_disc_zusgf_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # Define your schedule, e.g., "@daily"
    catchup=False,
    tags=["bert", "discount", "bigquery"],
    description="Airflow DAG for BERT_V_TA_DISC_ZUSGF job, concatenating discount descriptions.",
) as dag:
    
    get_sysdate_equivalent_task = PythonOperator(
        task_id="get_sysdate_equivalent_task",
        python_callable=get_sysdate_equivalent,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "dataset_id": BQ_DATASET_ID,
            "source_date_table": SOURCE_DATE_TABLE,
        },
    )

    transform_and_load_discount_data_task = PythonOperator(
        task_id="transform_and_load_discount_data_task",
        python_callable=transform_and_load_discount_data,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "dataset_id": BQ_DATASET_ID,
            "target_table_name": TARGET_TABLE_NAME,
            "source_discount_table": SOURCE_DISCOUNT_TABLE,
            "source_date_table": SOURCE_DATE_TABLE, # Passed for completeness, not actively used in current transform logic
            # If v_datum was used for filtering in transform_and_load_discount_data,
            # you would retrieve it here:
            # "v_datum": "{{ task_instance.xcom_pull(task_ids='get_sysdate_equivalent_task', key='v_datum') }}"
        },
        # Ensure the transformation runs even if previous tasks fail (e.g., if v_datum is not critical)
        # Or remove this if strict dependency is required.
        trigger_rule=TriggerRule.ALL_SUCCESS, 
    )

    get_sysdate_equivalent_task >> transform_and_load_discount_data_task