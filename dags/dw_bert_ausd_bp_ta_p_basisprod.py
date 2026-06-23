# Legacy Source: DW.BERT_AUSD_BP_TA_P_BASISPROD (UC4/Automic XML)
# Job: DW.BERT_AUSD_BP_TA_P_BASISPROD
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# GCP Configuration (PLACEHOLDERS)
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/r_ausd_bp_ta_p_basisprod.py"

default_args = {
    "owner": "airflow",
    "retries": 0, # Based on analysis, no explicit retries in UC4 XML
    "retry_delay": timedelta(minutes=5), # Placeholder
    "start_date": datetime(2023, 1, 1), # PLACEHOLDER_START_DATE
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_p_basisprod",
    schedule=None,  # No schedule derived from partial UC4 export
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["bert", "dataproc", "pyspark"],
) as dag:
    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_dw_bert_ausd_bp_ta_p_basisprod",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        job={
            "placement": {
                "cluster_name": DATAPROC_CLUSTER_NAME
            },
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_SCRIPT_URI,
                # Add arguments here if r_ausd_bp_ta_p_basisprod.py requires them
                # "args": ["--job-kennung", "AUSD_BP_TA_P_BASISPROD"]
            },
        },
        retries=0, # Matches default_args
        retry_delay=timedelta(minutes=5), # Matches default_args
    )

    # Dependencies
    # start >> run_dataproc_job >> end (implicitly handled by Airflow)