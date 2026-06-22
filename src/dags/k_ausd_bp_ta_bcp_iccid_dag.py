#
# Airflow DAG for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh
# This DAG orchestrates the BigQuery transformation logic.
#

from __future__ import annotations

import pendulum
import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# Initialize logging
log = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="k_ausd_bp_ta_bcp_iccid_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is intended to be triggered manually or externally
    catchup=False,
    tags=["isbert", "bcp", "iccid"],
    default_args=default_args,
    params={
        "p_JobKennung": "DEFAULT_JOB",
        "p_EintragsNr": "0",
        "p_Stichtag": pendulum.now("UTC").format("YYYYMMDD"),  # Default to today's date YYYYMMDD
        "p_wiederanlaufWert": "",
    },
) as dag:
    def _validate_and_prepare_parameters(**context):
        """
        Validates input parameters and prepares date variables.
        Replaces logic from gestern.ksh and h_alis_parameter.ksh.
        """
        dag_run_params = context["params"]
        
        p_job_kennung = dag_run_params.get("p_JobKennung")
        p_eintrags_nr = dag_run_params.get("p_EintragsNr")
        p_stichtag_str = dag_run_params.get("p_Stichtag")
        p_wiederanlauf_wert = dag_run_params.get("p_wiederanlaufWert")

        log.info(f"DAG Run Parameters: p_JobKennung={p_job_kennung}, p_EintragsNr={p_eintrags_nr}, p_Stichtag={p_stichtag_str}, p_wiederanlaufWert={p_wiederanlauf_wert}")

        # Basic date validation for p_Stichtag
        try:
            # Assuming p_Stichtag is always YYYYMMDD
            p_stichtag_dt = datetime.strptime(p_stichtag_str, "%Y%m%d").date()
        except ValueError:
            raise ValueError(f"Invalid p_Stichtag format. Expected YYYYMMDD, got {p_stichtag_str}")

        # Re-implement gestern.ksh logic: calculate today's and yesterday's dates
        today_dt = context["ds_date"] # DAG execution date as datetime.date
        yesterday_dt = today_dt - timedelta(days=1)

        p_datum_heute = today_dt.strftime("%Y%m%d")
        p_datum_gestern = yesterday_dt.strftime("%Y%m%d")
        
        log.info(f"Calculated Dates: p_datum_heute={p_datum_heute}, p_datum_gestern={p_datum_gestern}")

        # Push these to XCom if needed by downstream tasks for logging or other purposes
        context["ti"].xcom_push(key="p_datum_heute", value=p_datum_heute)
        context["ti"].xcom_push(key="p_datum_gestern", value=p_datum_gestern)
        # Note: The BigQuery SQL directly queries `isbert_schema.dwtk_meldungen` for `v_datum`,
        # so this DAG does not need to pass `v_datum` explicitly to the SQL.

    validate_and_prepare_params_task = PythonOperator(
        task_id="validate_and_prepare_parameters",
        python_callable=_validate_and_prepare_parameters,
    )

    execute_bigquery_transformation = BigQueryOperator(
        task_id="execute_bigquery_transformation",
        sql="src/sql/bigquery/d_ausd_bp_ta_bcp_iccid.bqsql",
        use_legacy_sql=False,
        # Specify the BigQuery project ID, if not implicitly set by the connection
        # project_id="your-gcp-project-id", 
        # For production, it's recommended to define a BigQuery connection in Airflow (e.g., bigquery_default)
        # bigquery_conn_id="google_cloud_default", 
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Define task dependencies
    validate_and_prepare_params_task >> execute_bigquery_transformation