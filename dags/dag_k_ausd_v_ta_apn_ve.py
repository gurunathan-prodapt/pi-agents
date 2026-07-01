from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DEFAULT_ARGS = {
    "owner": "dw_isbert",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Environment-specific configuration mapped from the Design Document
ENV_CONFIG = {
    "dev": {
        "project_id": "gcp-proj-dw-dev",
        "dataset": "dw_isbert_dev",
        "notify_email": "dev-alerts@company.com",
    },
    "prod": {
        "project_id": "gcp-proj-dw-prod",
        "dataset": "dw_isbert_prod",
        "notify_email": "prod-alerts@company.com",
    },
}

with DAG(
    dag_id="dag_k_ausd_v_ta_apn_ve",
    default_args=DEFAULT_ARGS,
    description="Orchestrator for k_ausd_v_ta_apn_ve stored procedure wrapper",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["migration", "bigquery", "composer"],
    params={
        "env": Param("dev", enum=["dev", "prod"], description="Target execution environment"),
        "p_JobKennung": Param("", type="string", description="Job identifier parameter (-j)"),
        "p_EintragsNr": Param("", type="string", description="Entry number / record identifier (-f)"),
    },
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Call the BigQuery Stored Procedure with dynamic parameter mapping
    call_stored_procedure = BigQueryInsertJobOperator(
        task_id="call_stored_procedure",
        gcp_conn_id="bigquery_default",
        configuration={
            "query": {
                "query": """
                DECLARE env_name STRING DEFAULT '{{ params.env }}';
                DECLARE project_id STRING;
                DECLARE dataset STRING;
                DECLARE job_kennung STRING DEFAULT '{{ params.p_JobKennung }}';
                DECLARE eintrags_nr STRING DEFAULT '{{ params.p_EintragsNr }}';

                -- Parameter validation before initiating execution
                IF job_kennung IS NULL OR job_kennung = '' OR eintrags_nr IS NULL OR eintrags_nr = '' THEN
                  ERROR('Parameter validation failed: p_JobKennung and p_EintragsNr must not be empty.');
                END IF;

                -- Dynamically resolve active environment variables
                IF env_name = 'prod' THEN
                  SET project_id = 'gcp-proj-dw-prod';
                  SET dataset = 'dw_isbert_prod';
                ELSE
                  SET project_id = 'gcp-proj-dw-dev';
                  SET dataset = 'dw_isbert_dev';
                END IF;

                EXECUTE IMMEDIATE FORMAT(
                  'CALL `%s.%s.sp_k_ausd_v_ta_apn_ve`("%s", "%s")',
                  project_id, dataset, job_kennung, eintrags_nr
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    start >> call_stored_procedure >> end