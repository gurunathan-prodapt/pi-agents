# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
# Target: Cloud Composer / Apache Airflow DAG

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models.param import Param

PROJECT_ID = "project"
DATASET_ID = "dataset"
PROCEDURE_NAME = "sp_d_ausd_bp_ta_cntrct_dist"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def validate_params(**context):
    params = context["params"]

    job_kennung = params.get("p_JobKennung")
    eintrags_nr = params.get("p_EintragsNr")
    stichtag = params.get("p_Stichtag")
    wiederanlauf_wert = params.get("p_wiederanlaufWert")

    if not job_kennung or not str(job_kennung).strip():
        raise AirflowFailException("Missing mandatory parameter: p_JobKennung")

    if not eintrags_nr or not str(eintrags_nr).strip():
        raise AirflowFailException("Missing mandatory parameter: p_EintragsNr")

    if not stichtag or not str(stichtag).strip():
        raise AirflowFailException("Missing mandatory parameter: p_Stichtag")

    if len(str(stichtag)) != 8 or not str(stichtag).isdigit():
        raise AirflowFailException("Invalid p_Stichtag format. Expected DDMMYYYY.")

    context["ti"].xcom_push(key="p_JobKennung", value=str(job_kennung).strip())
    context["ti"].xcom_push(key="p_EintragsNr", value=str(eintrags_nr).strip())
    context["ti"].xcom_push(key="p_Stichtag", value=str(stichtag).strip())
    context["ti"].xcom_push(key="p_wiederanlaufWert", value="" if wiederanlauf_wert is None else str(wiederanlauf_wert).strip())


with DAG(
    dag_id="dag_k_ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="Migration of k_ausd_bp_ta_cntrct_dist.ksh to Airflow + BigQuery",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "p_JobKennung": Param("", type="string"),
        "p_EintragsNr": Param("", type="string"),
        "p_Stichtag": Param("", type="string", description="DDMMYYYY"),
        "p_wiederanlaufWert": Param("", type="string", default=""),
    },
    tags=["migration", "isb", "bigquery"],
) as dag:

    validate_task = PythonOperator(
        task_id="validate_params",
        python_callable=validate_params,
    )

    call_stored_procedure = BigQueryInsertJobOperator(
        task_id="call_sp_d_ausd_bp_ta_cntrct_dist",
        configuration={
            "query": {
                "query": f"""
                CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_NAME}`(
                  @p_JobKennung,
                  @p_EintragsNr,
                  @p_Stichtag,
                  @p_wiederanlaufWert
                );
                """,
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "p_JobKennung",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ti.xcom_pull(task_ids='validate_params', key='p_JobKennung') }}"},
                    },
                    {
                        "name": "p_EintragsNr",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ti.xcom_pull(task_ids='validate_params', key='p_EintragsNr') }}"},
                    },
                    {
                        "name": "p_Stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ti.xcom_pull(task_ids='validate_params', key='p_Stichtag') }}"},
                    },
                    {
                        "name": "p_wiederanlaufWert",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ti.xcom_pull(task_ids='validate_params', key='p_wiederanlaufWert') }}"},
                    },
                ],
            }
        },
    )

    validate_task >> call_stored_procedure