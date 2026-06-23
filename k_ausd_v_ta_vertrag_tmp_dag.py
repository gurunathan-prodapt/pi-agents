# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
# Target: Airflow DAG for orchestration

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.models.param import Param

with DAG(
    dag_id="k_ausd_v_ta_vertrag_tmp_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["bigquery", "orchestration"],
    params={
        "p_job_kennung": Param(
            "DEFAULT_JOB", type="string", title="Job Kennung", description="Identifier for the job."
        ),
        "p_eintrags_nr": Param(
            "DEFAULT_ENTRY", type="string", title="Eintrags Nummer", description="Entry number for processing."
        ),
    },
) as dag:
    call_r_ausd_vertrag = BigQueryExecuteStoredProcedureOperator(
        task_id="call_r_ausd_vertrag",
        project_id="project",
        dataset_id="dataset",
        procedure_id="r_ausd_vertrag",
        gcp_conn_id="google_cloud_default",
        parameters={
            "p_JobKennung": "{{ dag_run.conf.get('p_job_kennung', params.p_job_kennung) }}",
            "p_EintragsNr": "{{ dag_run.conf.get('p_eintrags_nr', params.p_eintrags_nr) }}",
        },
    )