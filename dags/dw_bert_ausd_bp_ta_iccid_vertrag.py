# Airflow DAG for DW.BERT_AUSD_BP_TA_ICCID_VERTRAG
# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_BP_TA_ICCID_VERTRAG.xml
# Purpose: Orchestrates BigQuery stored procedure calls for ICCID data aggregation.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryStartStoredProcedureOperator

# Define your GCP project and dataset IDs
GCP_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_TARGET_DATASET = "target_dataset" # This dataset will contain the stored procedures

with DAG(
    dag_id="dw_bert_ausd_bp_ta_iccid_vertrag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set to your desired schedule (e.g., "@daily", "0 0 * * *")
    catchup=False,
    tags=["bert", "iccid", "bigquery"],
    description="Orchestrates BigQuery stored procedures for BERT ICCID data aggregation.",
) as dag:
    start_iccid_vertrag_sp = BigQueryStartStoredProcedureOperator(
        task_id="start_r_ausd_bp_ta_iccid_vertrag_sp",
        project_id=GCP_PROJECT_ID,
        dataset_id=BIGQUERY_TARGET_DATASET,
        procedure_id="r_ausd_bp_ta_iccid_vertrag_sp",
        parameters={
            "p_stichtag": "20231026",  # Example: Pass current date or dynamic value
            "p_wiederanlaufWert": 0,  # Example: Pass a restart value, 0 for full run
        },
        gcp_conn_id="google_cloud_default", # Ensure this connection exists in Airflow
    )