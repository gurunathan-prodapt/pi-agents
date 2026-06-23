# Airflow DAG for DW.BERT_AUSD_BP_TA_BCP_ICCID
# Replaces legacy UC4 job DW.BERT_AUSD_BP_TA_BCP_ICCID

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bcp_iccid",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # As per design: schedule=None, manual trigger or external scheduler
    catchup=False,
    tags=["bert", "dataproc", "pyspark"],
    owner="data_engineering", # Placeholder owner
    description="Migrated UC4 job DW.BERT_AUSD_BP_TA_BCP_ICCID - preparation of instantiated base products for ICCID",
) as dag:
    # Task to submit the PySpark job to Dataproc
    run_dw_bert_ausd_bp_ta_bcp_iccid = DataprocSubmitJobOperator(
        task_id="run_dw_bert_ausd_bp_ta_bcp_iccid",
        project_id="YOUR_GCP_PROJECT_ID",  # Placeholder
        region="YOUR_GCP_REGION",  # Placeholder, e.g., "us-central1"
        job={
            "placement": {
                "cluster_name": "YOUR_DATAPROC_CLUSTER_NAME"  # Placeholder
            },
            "pyspark_job": {
                "main_python_file_uri": "gs://YOUR_GCS_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_bcp_iccid.py", # Placeholder
                # Arguments for the PySpark script can be added here if needed
                # "args": ["--some_param", "some_value"],
            },
        },
        # Consider adding `gcp_conn_id` if a specific connection is needed.
    )

    # Define task dependencies
    # As per design: start >> run_dw_bert_ausd_bp_ta_bcp_iccid >> end
    # No explicit start/end operators needed for a single task DAG unless
    # more complex setup/teardown is required.