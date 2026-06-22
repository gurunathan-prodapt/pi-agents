# Replaces legacy source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_APT_EXPORT_MONATLICH_JP.xml
# Job: DW.DWH_APT_EXPORT_MONATLICH_JP

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# Default values for Dataproc cluster and GCS paths.
# These should be configured for your GCP project.
PROJECT_ID = "your-gcp-project-id"
REGION = "your-gcp-region"
CLUSTER_NAME = "your-dataproc-cluster-name"
GCS_PYSPARK_CODE_BUCKET = "gs://your-pyspark-code-bucket"
GCS_OUTPUT_BUCKET = "gs://your-gcs-export-bucket"

with DAG(
    dag_id="dw_dwh_apt_export_monatlich_jp",
    start_date=days_ago(1),
    # This DAG is primarily triggered by dw_dwh_run_apt_export_monatlich_jp_evt.py
    # so schedule_interval should be None for manual/external triggering.
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "export", "monthly", "uc4_jobplan"],
    max_active_runs=1, # Corresponds to SYNCREF Else=Wait on the original UC4 Job Plan.
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    }
) as dag:
    start = EmptyOperator(task_id="start")

    # Task for DW.DWH_EXIS_SD_APT_NNA_DATA
    # References nna_data_exporter.py for the PySpark application.
    export_nna_data = DataprocSubmitJobOperator(
        task_id="export_nna_data",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_PYSPARK_CODE_BUCKET}/nna_data_exporter.py",
                "args": [
                    # MONAT_ID will be passed from the triggering DAG's 'conf' if available,
                    # otherwise use the current execution date in YYYYMMDD format.
                    "--monat_id", "{{ dag_run.conf.get('monat_id') or ds_nodash }}",
                    "--output_gcs_bucket", GCS_OUTPUT_BUCKET,
                    # Add any other arguments required by nna_data_exporter.py
                ],
                # Add JARs for Oracle JDBC driver here if not pre-installed on cluster
                # e.g., "jar_file_uris": ["gs://your-gcs-bucket/ojdbc8.jar"],
            },
        },
    )

    # Task for DW.DWH_EXIS_SD_APT_NNA_VOIC
    # References nna_voice_exporter.py for the PySpark application.
    export_nna_voice = DataprocSubmitJobOperator(
        task_id="export_nna_voice",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_PYSPARK_CODE_BUCKET}/nna_voice_exporter.py",
                "args": [
                    "--monat_id", "{{ dag_run.conf.get('monat_id') or ds_nodash }}",
                    "--output_gcs_bucket", GCS_OUTPUT_BUCKET,
                    # Add any other arguments required by nna_voice_exporter.py
                ],
                # Add JARs for Oracle JDBC driver here if not pre-installed on cluster
                # e.g., "jar_file_uris": ["gs://your-gcs-bucket/ojdbc8.jar"],
            },
        },
    )

    end = EmptyOperator(task_id="end")

    start >> [export_nna_data, export_nna_voice] >> end