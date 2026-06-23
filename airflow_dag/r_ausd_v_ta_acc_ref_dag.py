#
# Airflow DAG for r_ausd_v_ta_acc_ref
# Orchestrates the execution of the BigQuery stored procedure,
# replacing legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
#
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "project"  # Replace with your GCP Project ID
DATASET_ID = "dataset"  # Replace with your BigQuery Dataset ID
JOB_NAME_KENNUNG = "BERT_V_TA_ACC_REF"

with DAG(
    dag_id="r_ausd_v_ta_acc_ref_wrapper_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule here, e.g., "@daily" or "0 5 * * *"
    catchup=False,
    tags=["bigquery", "data_reconciliation"],
    description="Orchestrates the BigQuery vertragsdatenabgleich_wrapper stored procedure for contract data reconciliation.",
) as dag:
    # Task to call the main BigQuery stored procedure
    call_vertragsdatenabgleich_wrapper = BigQueryInsertJobOperator(
        task_id="call_vertragsdatenabgleich_wrapper",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
                        p_s_parameter => 'system_param_value', -- Replace with actual value or Airflow variable
                        p_l_parameter => '{JOB_NAME_KENNUNG}_log_file', -- Replace with actual value or Airflow variable
                        p_h_flag => FALSE
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # You can add more tasks here, e.g., for data quality checks, notifications, etc.
    # call_vertragsdatenabgleich_wrapper