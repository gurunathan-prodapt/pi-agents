# Migrated Airflow DAG for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
# This DAG orchestrates the data preparation process in Google Cloud BigQuery.

from __future__ import annotations

import pendulum
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.exceptions import AirflowFailException

# Import custom utilities
from utils.date_helpers import DWDate_Datum_Check, get_today_and_yesterday_dates
from utils.parameter_parser import parse_dag_parameters, pruefeParameterGesetzt
from utils.error_handling import DWMSG_MeldeFehler, setup_logger

logger = setup_logger(__name__)

# BigQuery project and dataset placeholders
# IMPORTANT: Replace with your actual BigQuery project and dataset IDs
BIGQUERY_PROJECT = "YOUR_BIGQUERY_PROJECT"
BIGQUERY_DATASET = "YOUR_BIGQUERY_DATASET"

# Path to the BigQuery SQL transformation file
BQ_SQL_TRANSFORM_PATH = "sql/d_ausd_bp_ta_bcp_msisdn_bq.sql"

# --- Python functions to replicate shell script logic ---

def _parse_and_validate_params(**kwargs):
    """
    Parses DAG parameters from dag_run.conf and performs validation.
    Replicates k_ausd_bp_ta_bcp_msisdn.ksh's parameter reading and validation.
    """
    dag_run_conf = kwargs["dag_run"].conf
    logger.info(f"DAG Run Configuration: {dag_run_conf}")

    try:
        params = parse_dag_parameters(dag_run_conf)

        p_JobKennung = params['p_JobKennung']
        p_EintragsNr = params['p_EintragsNr']
        p_Stichtag = params['p_Stichtag'] # Expected format 'DDMMYYYY'
        p_wiederanlaufWert = params['p_wiederanlaufWert']

        # Parameter validation (pruefeParameterGesetzt)
        pruefeParameterGesetzt("Jobkennung", p_JobKennung)
        pruefeParameterGesetzt("Stichtag", p_Stichtag)
        pruefeParameterGesetzt("EintragsNr", p_EintragsNr)

        # Date format validation (DWDate_Datum_Check)
        DWDate_Datum_Check(p_Stichtag, '%d%m%Y')
        logger.info(f"Parameter p_Stichtag ({p_Stichtag}) is valid.")

        # Push validated parameters to XCom for downstream tasks
        kwargs["ti"].xcom_push(key="p_JobKennung", value=p_JobKennung)
        kwargs["ti"].xcom_push(key="p_EintragsNr", value=p_EintragsNr)
        kwargs["ti"].xcom_push(key="p_Stichtag", value=p_Stichtag)
        kwargs["ti"].xcom_push(key="p_wiederanlaufWert", value=p_wiederanlaufWert)

    except ValueError as e:
        DWMSG_MeldeFehler(logger, 1, 'E', 193, str(e))
        raise AirflowFailException(f"Parameter validation failed: {e}")
    except Exception as e:
        DWMSG_MeldeFehler(logger, 1, 'E', 999, f"Unexpected error during parameter parsing: {e}")
        raise AirflowFailException(f"Unexpected error: {e}")

def _calculate_dates(**kwargs):
    """
    Calculates today's and yesterday's dates.
    Replicates gestern.ksh functionality.
    """
    p_stichtag_str = kwargs["ti"].xcom_pull(task_ids="parse_and_validate_parameters", key="p_Stichtag")
    
    # The original `gestern.ksh` seemed to just get system dates.
    # If `p_Stichtag` should be the reference for "today", then use it.
    # Based on the ksh, `gestern.ksh` is called without arguments after p_Stichtag is set,
    # and its output is assigned to p_datum_heute and p_datum_gestern.
    # Let's assume `gestern.ksh` returns system current date and previous date.
    p_datum_heute, p_datum_gestern = get_today_and_yesterday_dates()
    logger.info(f"Calculated p_datum_heute: {p_datum_heute}, p_datum_gestern: {p_datum_gestern}")

    kwargs["ti"].xcom_push(key="p_datum_heute", value=p_datum_heute)
    kwargs["ti"].xcom_push(key="p_datum_gestern", value=p_datum_gestern)


def _get_bq_sql_content(sql_file_path: str):
    """Reads the content of a SQL file."""
    try:
        with open(sql_file_path, "r") as f:
            sql_content = f.read()
        return sql_content
    except FileNotFoundError:
        DWMSG_MeldeFehler(logger, 1, 'E', 404, f"SQL file not found: {sql_file_path}")
        raise AirflowFailException(f"SQL file not found: {sql_file_path}")
    except Exception as e:
        DWMSG_MeldeFehler(logger, 1, 'E', 999, f"Error reading SQL file {sql_file_path}: {e}")
        raise AirflowFailException(f"Error reading SQL file: {e}")

with DAG(
    dag_id="k_ausd_bp_ta_bcp_msisdn_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # This DAG is likely triggered by an upstream process, not on a fixed schedule.
    catchup=False,
    tags=["bigquery", "data_preparation", "migration"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        # "retry_delay": pendulum.duration(minutes=5),
    },
    doc_md="""
    ### BigQuery Migration for k_ausd_bp_ta_bcp_msisdn.ksh

    This Airflow DAG replaces the legacy KornShell script
    `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`.
    It orchestrates the data preparation process for the `SOF_TA_BCP_MSISDN` table
    in BigQuery, translating the original Oracle SQL logic.

    **Expected DAG Parameters (via `dag_run.conf`):**
    *   `job_kennung` (string, required): Corresponds to `-j` in legacy script.
    *   `eintrags_nr` (string, required): Corresponds to `-f` in legacy script.
    *   `stichtag` (string, required, format 'DDMMYYYY'): Corresponds to `-s` in legacy script.
    *   `wiederanlauf_wert` (integer, optional, default 0): Corresponds to `-l` in legacy script.
    """,
) as dag:
    
    start_task = PythonOperator(
        task_id="start_job",
        python_callable=lambda: logger.info("Starting k_ausd_bp_ta_bcp_msisdn_dag..."),
    )

    parse_and_validate_parameters = PythonOperator(
        task_id="parse_and_validate_parameters",
        python_callable=_parse_and_validate_params,
    )

    calculate_dates = PythonOperator(
        task_id="calculate_dates",
        python_callable=_calculate_dates,
    )

    # Read SQL content from file
    read_sql_transform_content = PythonOperator(
        task_id="read_sql_transform_content",
        python_callable=_get_bq_sql_content,
        op_kwargs={
            "sql_file_path": BQ_SQL_TRANSFORM_PATH,
        },
    )

    # TRUNCATE TABLE (or DELETE FROM) followed by INSERT INTO is the direct equivalent
    # of the Oracle `TRUNCATE TABLE` then `INSERT INTO`.
    # BigQueryOperator can execute multiple statements.
    execute_bigquery_transformation = BigQueryOperator(
        task_id="execute_bigquery_transformation",
        sql=[
            f"TRUNCATE TABLE `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.SOF_TA_BCP_MSISDN`;",
            """
            INSERT INTO `{{ params.project_id }}.{{ params.dataset_id }}.SOF_TA_BCP_MSISDN` (
                CNTRCT_ID,
                BPR_ID,
                CNTRCT_ID_REF,
                TN_TEL_MSISDN
            )
            -- SQL content will be rendered from the pushed XCom value
            {{ ti.xcom_pull(task_ids='read_sql_transform_content') }}
            """,
        ],
        params={
            "project_id": BIGQUERY_PROJECT,
            "dataset_id": BIGQUERY_DATASET,
        },
        use_legacy_sql=False,
        bigquery_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
    )

    # Placeholder for record count and logging (original tmpFile logic)
    # In Airflow, you can query BigQuery for row counts and log them.
    log_record_count = PythonOperator(
        task_id="log_record_count",
        python_callable=lambda: logger.info("Finished BigQuery transformation. Query BigQuery for record count if needed."),
    )

    end_task = PythonOperator(
        task_id="end_job",
        python_callable=lambda: logger.info("k_ausd_bp_ta_bcp_msisdn_dag completed successfully."),
    )

    start_task >> parse_and_validate_parameters >> calculate_dates
    calculate_dates >> read_sql_transform_content >> execute_bigquery_transformation
    execute_bigquery_transformation >> log_record_count >> end_task