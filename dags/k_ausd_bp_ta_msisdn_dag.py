# File: dags/k_ausd_bp_ta_msisdn_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

PROJECT_ID = "gcp-project-placeholder"
DATASET_ID = "dw_isbert_dataset"
PROCEDURE_NAME = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_msisdn"

default_args = {
    "owner": "dw",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def run_bigquery_procedure(**context):
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    
    # Retrieve parameters dynamically from dag_run.conf, falling back to task-level params and defaults
    job_kennung = dag_run_conf.get("p_JobKennung") or context["params"].get("p_JobKennung") or "DEFAULT_JOB"
    eintrags_nr = dag_run_conf.get("p_EintragsNr") or context["params"].get("p_EintragsNr") or "1"
    stichtag = dag_run_conf.get("p_Stichtag") or context["params"].get("p_Stichtag") or datetime.now().strftime("%d%m%Y")
    wiederanlauf_wert = dag_run_conf.get("p_wiederanlaufWert") or context["params"].get("p_wiederanlaufWert") or "0"

    client = bigquery.Client(project=PROJECT_ID)

    sql = f"""
    CALL `{PROCEDURE_NAME}`(
      @p_JobKennung,
      @p_EintragsNr,
      @p_Stichtag,
      @p_wiederanlaufWert
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_JobKennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("p_EintragsNr", "STRING", eintrags_nr),
            bigquery.ScalarQueryParameter("p_Stichtag", "STRING", stichtag),
            bigquery.ScalarQueryParameter("p_wiederanlaufWert", "STRING", wiederanlauf_wert),
        ]
    )

    query_job = client.query(sql, job_config=job_config)
    query_job.result()

with DAG(
    dag_id="k_ausd_bp_ta_msisdn",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["migration", "bigquery"],
) as dag:

    execute_proc = PythonOperator(
        task_id="execute_r_ausd_bp_ta_msisdn",
        python_callable=run_bigquery_procedure,
        provide_context=True,
        params={
            "p_JobKennung": "",
            "p_EintragsNr": "",
            "p_Stichtag": "",
            "p_wiederanlaufWert": "0",
        },
    )