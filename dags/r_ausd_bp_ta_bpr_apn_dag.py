# Airflow DAG for r_ausd_bp_ta_bpr_apn.ksh
# Replaces legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

from __future__ import annotations

import logging

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

log = logging.getLogger(__name__)

def _process_parameters(**kwargs):
    """
    Processes command-line-like parameters for Stichtag and Wiederanlaufwert.
    Defaults Stichtag to today's date and Wiederanlaufwert to 0 if not provided.
    Pushes processed parameters to XCom for downstream tasks.
    """
    dag_run = kwargs.get("dag_run")
    params = dag_run.conf if dag_run else {}

    # Stichtag (Reference Date) - 's' in original ksh
    # Expected format: YYYY-MM-DD
    p_stichtag = params.get("stichtag")
    if p_stichtag:
        try:
            # Validate if stichtag is a valid date
            pendulum.parse(p_stichtag)
        except Exception:
            log.error(f"Invalid stichtag format: {p_stichtag}. Expected YYYY-MM-DD. Using today's date.")
            p_stichtag = pendulum.today("UTC").format("YYYY-MM-DD")
    else:
        log.info("stichtag not provided. Defaulting to today's date.")
        p_stichtag = pendulum.today("UTC").format("YYYY-MM-DD")

    # Wiederanlaufwert (Restart Value) - 'l' in original ksh
    p_wiederanlaufwert = params.get("wiederanlaufwert", 0)
    try:
        p_wiederanlaufwert = int(p_wiederanlaufwert)
    except ValueError:
        log.warning(f"Invalid wiederanlaufwert: {p_wiederanlaufwert}. Defaulting to 0.")
        p_wiederanlaufwert = 0

    log.info(f"Resolved Stichtag: {p_stichtag}")
    log.info(f"Resolved Wiederanlaufwert: {p_wiederanlaufwert}")

    # Push parameters to XCom for use by subsequent tasks
    kwargs["ti"].xcom_push(key="p_stichtag", value=p_stichtag)
    kwargs["ti"].xcom_push(key="p_wiederanlaufwert", value=p_wiederanlaufwert)

def _invoke_core_processing(**kwargs):
    """
    Placeholder function to invoke the core processing logic for k_ausd_bp_ta_bpr_apn.
    This would typically be a BigQueryOperator, PythonOperator calling a script, etc.
    """
    ti = kwargs["ti"]
    p_stichtag = ti.xcom_pull(key="p_stichtag", task_ids="process_parameters_task")
    p_wiederanlaufwert = ti.xcom_pull(key="p_wiederanlaufwert", task_ids="process_parameters_task")

    log.info(f"Invoking core processing script (k_ausd_bp_ta_bpr_apn.ksh migration)...")
    log.info(f"  Parameters for core script:")
    log.info(f"    - Stichtag (s): {p_stichtag}")
    log.info(f"    - Wiederanlaufwert (l): {p_wiederanlaufwert}")
    log.info(f"    - JobKennung (j): (to be defined by Airflow context or configuration)")
    log.info(f"    - DW_EintragsNr (f): (to be defined by Airflow context or configuration)")

    # This is where the actual Airflow task for k_ausd_bp_ta_bpr_apn.ksh would go.
    # Example:
    # from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    #
    # bq_task = BigQueryExecuteQueryOperator(
    #     task_id='execute_core_bigquery_logic',
    #     sql=f"""
    #         CALL `your_gcp_project`.`your_dataset`.`k_ausd_bp_ta_bpr_apn_sp`(
    #             '{p_stichtag}',
    #             {p_wiederanlaufwert}
    #         );
    #     """,
    #     use_legacy_sql=False,
    #     gcp_conn_id='google_cloud_default'
    # )
    # bq_task.execute(context=kwargs) # Or just define it as a separate task in the DAG

    log.info("Core processing invocation complete (placeholder).")


with DAG(
    dag_id="r_ausd_bp_ta_bpr_apn_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["isbert", "bert", "provisioning"],
    params={
        "stichtag": Param(
            type="string",
            title="Stichtag (Reference Date)",
            description="Reference date in YYYY-MM-DD format. Defaults to current date if not provided.",
            pattern="^\\d{4}-\\d{2}-\\d{2}$|^$",
            default="" # Empty string to indicate optional input
        ),
        "wiederanlaufwert": Param(
            type="integer",
            title="Wiederanlaufwert (Restart Value)",
            description="Restart value. Defaults to 0 if not provided.",
            minimum=0,
            default=0
        ),
    },
) as dag:
    process_parameters_task = PythonOperator(
        task_id="process_parameters_task",
        python_callable=_process_parameters,
    )

    invoke_core_script_task = PythonOperator(
        task_id="invoke_core_script_task",
        python_callable=_invoke_core_processing,
    )

    process_parameters_task >> invoke_core_script_task