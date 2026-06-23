"""
Airflow DAG for migrating the legacy `r_ausd_bp_ta_apn_vertrag` KornShell workflow to BigQuery.

This DAG runs daily at midnight UTC and performs a BigQuery transformation that:
1. Starts the workflow.
2. Executes the migrated SQL to determine the snapshot date from metadata.
3. Deletes any existing rows for that snapshot date from the target table.
4. Inserts the transformed APN and contract reference aggregates into the target table.

The DAG is configured with catchup disabled and a single active run to mirror the
synchronous behavior of the legacy UC4/Automic job.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# GCP / BigQuery configuration constants.
GCP_PROJECT_ID = "your_gcp_project"  # TODO: replace with your GCP project ID
BIGQUERY_DATASET = "your_bigquery_dataset"  # TODO: replace with your BigQuery dataset name
GCP_CONN_ID = "google_cloud_default"  # TODO: replace with your Airflow GCP connection ID

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="isbert_r_ausd_bp_ta_apn_vertrag_dag",
    default_args=default_args,
    description="Airflow DAG to migrate r_ausd_bp_ta_apn_vertrag.ksh to BigQuery",
    schedule="0 0 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["isbert", "bigquery", "etl"],
) as dag:
    # Task 1: Start marker for the DAG.
    start_task = DummyOperator(
        task_id="start_task",
    )

    # Task 2: Execute the BigQuery transformation that computes the snapshot date,
    # removes existing rows for that snapshot, and inserts the aggregated results.
    execute_bigquery_transformation = BigQueryExecuteQueryOperator(
        task_id="execute_bigquery_transformation",
        sql=f"""
            DECLARE v_snapshot_date DATE DEFAULT (
              SELECT COALESCE(MAX(DATE(timecreated)), DATE '1900-01-01')
              FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.isbert_dwtk_meldungen`
              WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
            );

            DELETE FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sof_ta_apn_vertrag`
            WHERE snapshot_date = v_snapshot_date;

            INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sof_ta_apn_vertrag`
              (cntrct_id, apn_list, contract_ref_list, snapshot_date)
            SELECT
              cntrct_id,
              SUBSTR(STRING_AGG(access_point_name, ', ' ORDER BY access_point_name), 1, 100) AS apn_list,
              SUBSTR(STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref), 1, 100) AS contract_ref_list,
              v_snapshot_date AS snapshot_date
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sof_ta_bpr_apn`
            GROUP BY cntrct_id
        """,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
    )

    start_task >> execute_bigquery_transformation