# Migrated from DW.BERT_AUSD_BP_TA_ICCID_VERTRAG (legacy: UC4 job and KornShell scripts)

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator

# Import custom utilities
from utils.bert_utilities import (
    parse_stichtag_and_wiederanlaufwert,
    validate_date,
    log_message,
)

PROJECT_ID = "PROJECT_ID"  # Replace with your GCP Project ID
DATASET = "DATASET"  # Replace with your BigQuery Dataset ID
TARGET_TABLE = f"{PROJECT_ID}.{DATASET}.SOF_TA_ICCID_VERTRAG"

def _r_ausd_bp_ta_iccid_vertrag_logic(**kwargs):
    """
    Simulates the logic of r_ausd_bp_ta_iccid_vertrag.ksh for parameter parsing
    and date validation.
    """
    ti = kwargs["ti"]
    
    # Placeholder for getting Stichtag and Wiederanlaufwert.
    # In a real scenario, these might come from Airflow config, trigger args, or a more sophisticated parsing.
    # For now, we'll use a hardcoded default and pass them.
    # The original script uses `getopts` for -s (Stichtag) and -l (Wiederanlaufwert).
    
    # Example: default values or from Airflow DAG run configuration
    stichtag_raw = kwargs.get("stichtag", pendulum.now("UTC").format("YYYYMMDD"))
    wiederanlaufwert = kwargs.get("wiederanlaufwert", "0") # Default 0 as per ksh
    
    # Use utility function for date validation (simulated)
    if not validate_date(stichtag_raw):
        log_message("ERROR", f"Invalid Stichtag: {stichtag_raw}")
        raise ValueError(f"Invalid Stichtag: {stichtag_raw}")
        
    log_message("INFO", f"Processed Stichtag: {stichtag_raw}, Wiederanlaufwert: {wiederanlaufwert}")
    
    # Push parameters to XCom for the next task
    ti.xcom_push(key="stichtag", value=stichtag_raw)
    ti.xcom_push(key="wiederanlaufwert", value=wiederanlaufwert)


with DAG(
    dag_id="dag_dw_bert_ausd_bp_ta_iccid_vertrag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., "@daily"
    catchup=False,
    tags=["bert", "dw", "iccid", "bigquery"],
    description="Aggregates SIM card data by contract ID into BigQuery.",
) as dag:
    
    r_ausd_bp_ta_iccid_vertrag_task = PythonOperator(
        task_id="r_ausd_bp_ta_iccid_vertrag_task",
        python_callable=_r_ausd_bp_ta_iccid_vertrag_logic,
        # op_kwargs={"stichtag": "{{ ds_nodash }}"} # Example of passing dynamic date from Airflow
    )

    execute_bq_sql_task = BigQueryOperator(
        task_id="execute_bq_sql_task",
        sql="bigquery_sql/d_ausd_bp_ta_iccid_vertrag.sql",
        use_legacy_sql=False,
        destination_dataset_table=TARGET_TABLE,
        write_disposition="WRITE_TRUNCATE", # This overwrites the table as per TRUNCATE TABLE
        params={
            # No explicit params in this SQL, but if needed, they would go here
            # e.g., 'stichtag': "{{ ti.xcom_pull(task_ids='r_ausd_bp_ta_iccid_vertrag_task', key='stichtag') }}"
        },
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
    )

    r_ausd_bp_ta_iccid_vertrag_task >> execute_bq_sql_task