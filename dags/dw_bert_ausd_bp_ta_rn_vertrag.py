# This DAG replaces the legacy UC4 job DW.BERT_AUSD_BP_TA_RN_VERTRAG.
# It orchestrates the execution of a PySpark job on a Dataproc cluster.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# GCP configuration constants — replace these placeholder values with your environment-specific settings.
GCP_PROJECT_ID = "gcp-project-id"  # TODO: replace with your GCP project ID
DATAPROC_REGION = "us-central1"  # TODO: replace with your Dataproc region
DATAPROC_CLUSTER_NAME = "dataproc-cluster-name"  # TODO: replace with your cluster name
GCS_BUCKET_NAME = "your-gcs-bucket"  # TODO: replace with your GCS bucket name

default_args = {
    "owner": "data-platform",
    "retries": 0,
    "retry_delay": timedelta(seconds=0),
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_rn_vertrag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["uc4_migration", "dataproc", "pyspark"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Arguments for the PySpark job (derived from the original ksh script parameters)
    # The 'stichtag' (key date) and 'wiederanlaufWert' (restart value) are important.
    # These can be passed as arguments to the PySpark script.
    # For now, we'll pass placeholders; these should be dynamically determined
    # or configured based on business requirements.
    # The 'JobKennung' and 'DW_EintragsNr' were for legacy logging and can be managed by Airflow/Python logging.
    # The original script defaults p_wiederanlaufWert to 0 if not set.
    pyspark_job_arguments = [
        "--stichtag", "{{ ds_nodash }}",  # Example: use Airflow's data_interval_start as stichtag
        "--wiederanlaufWert", "0"  # Default to 0 as per ksh script logic if not overridden
    ]

    # Submit the migrated PySpark job to Dataproc.
    run_dw_bert_ausd_bp_ta_rn_vertrag = DataprocSubmitJobOperator(
        task_id="run_dw_bert_ausd_bp_ta_rn_vertrag",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job_id="{{ dag.dag_id }}_{{ run_id }}_run_dw_bert_ausd_bp_ta_rn_vertrag",
        job={
            "placement": {
                "cluster_name": DATAPROC_CLUSTER_NAME,
            },
            "pyspark_job": {
                "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py",
                "args": pyspark_job_arguments,
            },
        },
        retries=0,
        retry_delay=timedelta(seconds=0),
    )

    end = EmptyOperator(task_id="end")

    start >> run_dw_bert_ausd_bp_ta_rn_vertrag >> end