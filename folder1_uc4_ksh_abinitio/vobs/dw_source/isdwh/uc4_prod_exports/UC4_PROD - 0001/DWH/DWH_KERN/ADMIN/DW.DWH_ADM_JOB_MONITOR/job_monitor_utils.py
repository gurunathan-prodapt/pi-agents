"""
Shared operational monitoring utilities for DWH administration.
Translates UC4 variable mappings and database tracking into reusable Python functions.
"""

import os
import logging
from datetime import datetime
from typing import Any, Dict

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_gcp_project() -> str:
    """Resolves GCP Project ID from Environment or Airflow Variables."""
    return os.environ.get("GCP_PROJECT") or Variable.get("GCP_PROJECT")


def get_metadata_db_conn() -> str:
    """Resolves Metadata Audit Connection ID from Airflow Variables."""
    return Variable.get("METADATA_AUDIT_DB_CONN", default_var="metadata_audit_db")


def register_job_monitoring_start_logic(
    dag_id: str, 
    task_id: str, 
    run_id: str, 
    **kwargs: Any
) -> None:
    """
    Simulates the UC4 JOBI "DW.DWH_ADM_JOB_MONITOR_START" registration logic.
    Checks monitoring configuration and writes execution metadata to a database.
    """
    logging.info(f"Evaluating monitoring registration for DAG: {dag_id}, Task: {task_id}")

    # Retrieve monitoring rules from Airflow Variables (Simulating DW.DWH_MONITORED_JPS)
    monitored_jps = Variable.get("dwh_monitored_dags", deserialize_json=True, default_var={})
    
    # Determine flag (Check specifically for DAG, fall back to "ALL", default to "N")
    dag_monitoring_flag = monitored_jps.get(dag_id, monitored_jps.get("ALL", "N"))
    
    if dag_monitoring_flag == "J":
        logging.info(f"DAG {dag_id} is flagged for active monitoring. Registering execution state.")
        
        insert_sql = """
            INSERT INTO dwh_running_jobs (job_name, run_number, registration_timestamp, status)
            VALUES (%s, %s, NOW(), 'RUNNING')
            ON CONFLICT (job_name) 
            DO UPDATE SET run_number = EXCLUDED.run_number, registration_timestamp = NOW(), status = 'RUNNING';
        """
        
        try: 
            conn_id = get_metadata_db_conn()
            pg_hook = PostgresHook(postgres_conn_id=conn_id)
            pg_hook.run(insert_sql, parameters=(task_id, run_id))
            
            # Verbatim Output Rule Mapping: Added &ADMJOB with &ADMNRJOB
            logging.info(f"Added {task_id} with {run_id}")
        except Exception as e:
            logging.error(f"Failed to write metadata registry entry: {str(e)}")
            raise e
    else:
        logging.info(f"DAG {dag_id} is not registered for monitoring. Skipping audit registration.")
        raise AirflowSkipException("Audit registration not required for this DAG execution.")


def register_job_monitoring_end_logic(
    dag_id: str, 
    task_id: str, 
    dag_run_conf: Dict[str, Any], 
    **kwargs: Any
) -> None:
    """
    Simulates the UC4 JOBI "DW.DWH_ADM_JOB_MONITOR_END" post-execution logic.
    Logs metadata and updates the runtime variable registry database or central store.
    """
    # Extract DWH Job Identifier (Jobkennung) from context configuration parameters
    dwh_job_kennung = dag_run_conf.get('dwh_job_kennung', 'DEFAULT_KENNUNG')
    
    # Verbatim Output Rule Mapping: print Jobkennung &DWH_JOB_KENNUNG eingetragen für &JPMJOB
    logging.info(f"Jobkennung {dwh_job_kennung} eingetragen für {task_id}")
    
    # Port of PUT_VAR: Register to the dynamic central registry store
    try:
        registry_key = f"dw_dwh_adm_job_monitor_jobkennung_var_{task_id}"
        Variable.set(key=registry_key, value=dwh_job_kennung)
        logging.info(f"Successfully updated Airflow Variable {registry_key} with value {dwh_job_kennung}")
    except Exception as e:
        logging.error(f"Failed to write global metadata state registry variable: {str(e)}")
        raise e