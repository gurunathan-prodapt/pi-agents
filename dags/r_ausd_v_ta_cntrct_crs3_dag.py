# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
Airflow DAG for orchestrating the contract data synchronization process.
This DAG replaces the legacy KornShell wrapper script `r_ausd_v_ta_cntrct_crs3.ksh`.

It handles parameter parsing, environment setup, error logging, and orchestrates
the execution of the core data processing logic (which will be migrated separately).
"""

from __future__ import annotations

import logging
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import utility modules
from utils.env_config import EnvConfig
from utils.logging_utils import (
    dwmsg_ermittle_nr, dwmsg_erzeuge_eintrag, dwmsg_setze_status_ok,
    dwmsg_setze_status_abbruch, dwmsg_melde_fehler, dwmsg_logdateiname,
    DWMSGError, FATAL, WARNING, ERROR, INFO
)
from utils.parameter_utils import (
    pruefe_parameter_gesetzt, konvertiere_kennzahl, konvertiere_system,
    pruefe_system_kennzahl, pruefe_zeitraum, pruefe_zahl_positiv,
    pruefe_zeit_parameter, konvertiere_zeitspanne, ParameterError
)
from utils.date_utils import (
    dwdate_vormonat, dwdate_datum_check, dwdate_datum_le,
    dwdate_gib_zeitraum, letzter_tag_des_monats, tage_im_monat,
    addiere_datum, DWDateError
)

# Configure DAG-level logging
# In Airflow, this logger will automatically be integrated with Cloud Logging.
airflow_logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define DAG
with DAG(
    dag_id="r_ausd_v_ta_cntrct_crs3_orchestration",
    start_date=days_ago(1),
    schedule_interval=None,  # This can be set to a cron schedule, e.g., "0 5 * * *"
    catchup=False,
    tags=["bert", "isrpt", "contract_sync"],
    default_args=default_args,
    params={
        "job_kennung": "DEFAULT_JOB",
        "dw_eintrags_nr": None, # Will be generated if not provided
        "program_name": "r_ausd_v_ta_cntrct_crs3_dag.py",
        "log_level": "INFO", # For this DAG specifically, can be overridden
        # Add other parameters that k_ausd_v_ta_cntrct_crs3.ksh might need
        "core_script_param_j": None,
        "core_script_param_f": None,
    },
) as dag:

    def initialize_job_entry(**kwargs):
        """
        Initializes the job entry, mimicking DWMSG_ErmittleNr and DWMSG_ErzeugeEintrag.
        Handles `trap` logic by catching exceptions.
        """
        ti = kwargs["ti"]
        dag_run_conf = kwargs["dag_run"].conf if kwargs["dag_run"] else {}
        
        # Override log level for this task, if provided via DAG run config
        current_log_level = dag_run_conf.get("log_level", dag.params["log_level"]).upper()
        airflow_logger.setLevel(getattr(logging, current_log_level, logging.INFO))
        
        job_kennung = dag_run_conf.get("job_kennung", dag.params["job_kennung"])
        dw_eintrags_nr = dag_run_conf.get("dw_eintrags_nr", None)

        try:
            pruefe_parameter_gesetzt("JobKennung", job_kennung)
            airflow_logger.info(f"JobKennung: {job_kennung}")
            ti.xcom_push(key="job_kennung", value=job_kennung)

            # Generate new entry number if not provided
            if not dw_eintrags_nr:
                dw_eintrags_nr = dwmsg_ermittle_nr()
            
            program_name = dag_run_conf.get("program_name", dag.params["program_name"])
            log_file_name = dwmsg_logdateiname(job_kennung, dw_eintrags_nr)

            dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, program_name, log_file_name)
            airflow_logger.info(f"Initialized job entry {dw_eintrags_nr} for {job_kennung}")

            ti.xcom_push(key="dw_eintrags_nr", value=dw_eintrags_nr)
            ti.xcom_push(key="log_file_name", value=log_file_name)

        except ParameterError as e:
            airflow_logger.error(f"Parameter error during initialization: {e}")
            dwmsg_melde_fehler(dw_eintrags_nr if dw_eintrags_nr else "UNKNOWN", FATAL, e.error_code, e.arg_info)
            raise # Re-raise to fail the task
        except DWMSGError as e:
            airflow_logger.critical(f"DWMSG error during initialization: {e}")
            raise # Re-raise to fail the task
        except Exception as e:
            airflow_logger.critical(f"Unexpected error during initialization: {e}")
            dwmsg_melde_fehler(dw_eintrags_nr if dw_eintrags_nr else "UNKNOWN", FATAL, 999, str(e))
            raise # Re-raise to fail the task

    def execute_core_data_processing(**kwargs):
        """
        Placeholder for the actual data synchronization logic from k_ausd_v_ta_cntrct_crs3.ksh.
        This function will call the migrated core logic.
        """
        ti = kwargs["ti"]
        job_kennung = ti.xcom_pull(key="job_kennung", task_ids="initialize_job_entry")
        dw_eintrags_nr = ti.xcom_pull(key="dw_eintrags_nr", task_ids="initialize_job_entry")
        log_file_name = ti.xcom_pull(key="log_file_name", task_ids="initialize_job_entry")

        dag_run_conf = kwargs["dag_run"].conf if kwargs["dag_run"] else {}
        core_script_param_j = dag_run_conf.get("core_script_param_j", dag.params["core_script_param_j"])
        core_script_param_f = dag_run_conf.get("core_script_param_f", dag.params["core_script_param_f"])

        airflow_logger.info(f"Executing core data processing for JobKennung: {job_kennung}, EntryNr: {dw_eintrags_nr}")
        airflow_logger.info(f"Core script parameters: -j {core_script_param_j} -f {core_script_param_f}")

        try:
            # --- START: Placeholder for actual core script migration ---
            # In a real migration, this would invoke BigQuery SQL, a Python script,
            # Dataform, or another specialized operator.
            # Example: BigQueryOperator to run a SQL file
            # from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
            #
            # core_processing_task = BigQueryExecuteQueryOperator(
            #     task_id="run_k_ausd_v_ta_cntrct_crs3_sql",
            #     sql="sql/k_ausd_v_ta_cntrct_crs3.sql", # Path to migrated SQL
            #     destination_dataset_table=f"{EnvConfig.BQ_DATASET}.ta_cntrct_crs3",
            #     write_disposition="WRITE_TRUNCATE", # Or WRITE_APPEND, WRITE_EMPTY, etc.
            #     params={"job_kennung": job_kennung, "dw_eintrags_nr": dw_eintrags_nr},
            #     use_legacy_sql=False,
            # )
            # core_processing_task.execute(context=kwargs)
            #
            # For now, simulate success:
            airflow_logger.info("Simulating successful core data processing...")
            # --- END: Placeholder ---

            ti.xcom_push(key="core_processing_status", value="SUCCESS")
            
        except Exception as e:
            airflow_logger.error(f"Core data processing failed: {e}")
            dwmsg_melde_fehler(dw_eintrags_nr, ERROR, 500, "Core script execution failed", str(e))
            ti.xcom_push(key="core_processing_status", value="FAILED")
            raise # Re-raise to fail the task

    def finalize_job_status(**kwargs):
        """
        Finalizes the job status, mimicking DWMSG_SetzeStatusOK or DWMSG_SetzeStatusAbbruch.
        """
        ti = kwargs["ti"]
        dw_eintrags_nr = ti.xcom_pull(key="dw_eintrags_nr", task_ids="initialize_job_entry")
        core_processing_status = ti.xcom_pull(key="core_processing_status", task_ids="execute_core_data_processing")

        if core_processing_status == "SUCCESS":
            dwmsg_setze_status_ok(dw_eintrags_nr)
            airflow_logger.info(f"Job {dw_eintrags_nr} successfully finalized.")
        else:
            dwmsg_setze_status_abbruch(dw_eintrags_nr)
            airflow_logger.error(f"Job {dw_eintrags_nr} finalized with ABORT status.")
            # In Airflow, if a task fails, downstream tasks might not run.
            # This 'finalize' task could be configured with trigger_rule='all_done'
            # to run even if upstream fails, but for status update,
            # it's better to manage the failure/success within the DAG structure.
            # For simplicity, if core processing status is FAILED, we assume
            # an exception would have already been raised in the previous task.
            # This task would only run on success if trigger_rule is 'all_success' (default).
            # If trigger_rule is 'all_done', this task handles both success/failure.
            pass # The exception for FAILED status should be handled by `on_failure_callback` or similar.

    initialize_task = PythonOperator(
        task_id="initialize_job_entry",
        python_callable=initialize_job_entry,
        provide_context=True,
    )

    core_processing_task = PythonOperator(
        task_id="execute_core_data_processing",
        python_callable=execute_core_data_processing,
        provide_context=True,
    )

    finalize_task = PythonOperator(
        task_id="finalize_job_status",
        python_callable=finalize_job_status,
        provide_context=True,
        trigger_rule="all_done", # Ensures this task runs even if core_processing_task fails
    )

    initialize_task >> core_processing_task >> finalize_task

    # Define an on_failure_callback for the DAG to report any unhandled failures
    def dag_failure_callback(context):
        dag_run = context.get("dag_run")
        task_instance = context.get("task_instance")
        dw_eintrags_nr = task_instance.xcom_pull(key="dw_eintrags_nr", task_ids="initialize_job_entry")
        if dw_eintrags_nr:
            dwmsg_melde_fehler(dw_eintrags_nr, FATAL, 9999, f"DAG failed: {dag_run.dag_id}", f"Task: {task_instance.task_id}")
        else:
            airflow_logger.critical(f"DAG {dag_run.dag_id} failed, no entry number available.")

    dag.on_failure_callback = dag_failure_callback