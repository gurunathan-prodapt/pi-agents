# This Airflow DAG replaces the legacy job DW.BERT_AUSD_BP_TA_BCP_ICCID,
# which was composed of UC4, KornShell, and Oracle SQL*Plus scripts.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.session import provide_session
from airflow.utils.xcom import XCom

# Define BigQuery project and dataset IDs as placeholders.
# Replace these with your actual project and dataset.
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_bigquery_dataset_id"

def _fetch_stichtag(**kwargs):
    """
    Fetches the 'Stichtag' (key date) from the DWTK_MELDUNGEN table,
    analogous to the original Oracle SQL logic.
    Pushes the result to XCom.
    """
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client()

    query = f"""
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', CAST(MAX(m.timecreated) AS DATE)), '19000101')
    FROM
        `{PROJECT_ID}.{DATASET_ID}.DWTK_MELDUNGEN` m
    WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    
    # Execute query and fetch result
    query_job = client.query(query)
    rows = list(query_job.result())
    
    # Extract the stichtag value
    stichtag_date = rows[0][0] if rows else '19000101' # Default if no rows or value

    kwargs['ti'].xcom_push(key='stichtag_date', value=stichtag_date)
    print(f"Fetched Stichtag: {stichtag_date}")


with DAG(
    dag_id="dw_bert_ausd_bp_ta_bcp_iccid",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., "@daily", "0 0 * * *", None for manual
    catchup=False,
    tags=["bert", "bigquery", "data_ingestion"],
    params={
        "stichtag": None, # Optional parameter to override fetched stichtag
    }
) as dag:
    # Task 1: Fetch the 'Stichtag' from DWTK_MELDUNGEN
    fetch_stichtag_task = PythonOperator(
        task_id="fetch_stichtag_task",
        python_callable=_fetch_stichtag,
        do_xcom_push=True,
    )

    # Task 2: Truncate the target table
    truncate_target_table_task = BigQueryOperator(
        task_id="truncate_target_table_task",
        sql=f"""
        TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.TA_BCP_ICCID`;
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # Task 3: Insert data into the target table
    insert_data_task = BigQueryOperator(
        task_id="insert_data_task",
        sql=f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.TA_BCP_ICCID`
        (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR)
        SELECT
            DISTINCT
            bp.cntrct_id,
            bp.bpr_id,
            bp.cntrct_id_ref,
            ic.tn_iccid,
            ic.tn_imsi_hlr
        FROM
            `{PROJECT_ID}.{DATASET_ID}.TA_BPR_BCP` bp
        INNER JOIN
            `{PROJECT_ID}.{DATASET_ID}.TA_ICCID_VERTRAG` ic
        ON
            bp.cntrct_id_ref = ic.cntrct_id;
        -- Note: The original Oracle INSERT statement did not use the 'v_datum' for filtering.
        -- If 'v_datum' (Stichtag) needs to be used for filtering, uncomment and modify the WHERE clause below.
        -- Example: WHERE some_date_column = '{{ ti.xcom_pull(task_ids="fetch_stichtag_task", key="stichtag_date") }}'
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        write_disposition="WRITE_APPEND", # INSERT INTO is implicitly append
        
        # This configuration is crucial for BigQueryOperator when using templated fields
        # If 'v_datum' was used, this would be uncommented:
        # params={'stichtag_date': '{{ ti.xcom_pull(task_ids="fetch_stichtag_task", key="stichtag_date") }}'}
    )

    # Define task dependencies
    fetch_stichtag_task >> truncate_target_table_task >> insert_data_task