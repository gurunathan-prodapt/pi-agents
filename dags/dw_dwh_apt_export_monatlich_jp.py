```python
"""
Airflow DAG for DW.DWH_APT_EXPORT_MONATLICH_JP.

This DAG runs monthly and mirrors the UC4 job plan DW.DWH_APT_EXPORT_MONATLICH_JP.
It waits for the prerequisite DAGs corresponding to DW.BERT_STAMMDATEN_JP and
DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP, then submits two Dataproc PySpark jobs to
export master data into compressed CSV files in GCS:
- dw_dwh_exis_sd_apt_nna_data
- dw_dwh_exis_sd_apt_nna_voic

The DAG is configured to run with no catchup and a single active run at a time,
replicating the synchronous UC4 behavior.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State

# TODO: replace with your GCP project ID
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
# TODO: replace with your Dataproc region
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
# TODO: replace with your Dataproc cluster name
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
# TODO: replace with your GCS bucket name
GCS_BUCKET = "YOUR_BUCKET_NAME"

DAG_ID = "dw_dwh_apt_export_monatlich_jp"
SCHEDULE = "0 6 1 * *"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def on_failure_alarm(context):
    # TODO: implement your alerting logic here
    # (e.g. send email, post to Slack, trigger PagerDuty)
    pass


def check_no_active_run(**context):
    active_runs = DagRun.find(
        dag_id=context["dag"].dag_id,
        state=State.RUNNING,
    )
    active_runs = [r for r in active_runs if r.run_id != context["run_id"]]
    if active_runs:
        raise AirflowSkipException("Active run already in progress — skipping")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Monthly export of telephone system master data to compressed CSV files.",
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["dw", "export", "monthly", "dataproc"],
) as dag:
    # Guard task to skip this run if another active run is already in progress.
    skip_if_running = PythonOperator(
        task_id="skip_if_running",
        python_callable=check_no_active_run,
    )

    # Wait for prerequisite DAG corresponding to DW.BERT_STAMMDATEN_JP to succeed.
    wait_for_bert_stammdaten_jp = ExternalTaskSensor(
        task_id="wait_for_bert_stammdaten_jp",
        external_dag_id="dw_bert_stammdaten_jp",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 24 * 35,
        soft_fail=False,
    )

    # Wait for prerequisite DAG corresponding to DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP to succeed.
    wait_for_accessp_sigma_gprs_monatlich_jp = ExternalTaskSensor(
        task_id="wait_for_accessp_sigma_gprs_monatlich_jp",
        external_dag_id="dw_accessp_sigma_gprs_monatlich_jp",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 24 * 35,
        soft_fail=False,
    )

    # Submit the PySpark job that exports APT NNA data to compressed CSV in GCS.
    dw_dwh_exis_sd_apt_nna_data = DataprocSubmitJobOperator(
        task_id="dw_dwh_exis_sd_apt_nna_data",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        job_id="{{ dag.dag_id }}_{{ run_id }}_apt_nna_data",
        pyspark_job={
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark/dw_dwh_exis_sd_apt_nna_data.py",
            "args": [
                "--month_id",
                "{{ ds_nodash[:6] }}",
                "--output_path",
                f"gs://{GCS_BUCKET}/exports/apt_nna_data/",
            ],
        },
        on_failure_callback=on_failure_alarm,
    )

    # Submit the PySpark job that exports APT NNA voice data to compressed CSV in GCS.
    dw_dwh_exis_sd_apt_nna_voic = DataprocSubmitJobOperator(
        task_id="dw_dwh_exis_sd_apt_nna_voic",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        job_id="{{ dag.dag_id }}_{{ run_id }}_apt_nna_voic",
        pyspark_job={
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark/dw_dwh_exis_sd_apt_nna_voic.py",
            "args": [
                "--month_id",
                "{{ ds_nodash[:6] }}",
                "--output_path",
                f"gs://{GCS_BUCKET}/exports/apt_nna_voic/",
            ],
        },
        on_failure_callback=on_failure_alarm,
    )

    skip_if_running >> wait_for_bert_stammdaten_jp >> wait_for_accessp_sigma_gprs_monatlich_jp >> dw_dwh_exis_sd_apt_nna_data >> dw_dwh_exis_sd_apt_nna_voic
```