# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
# and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
# Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

import logging
from datetime import datetime

# Initialize logger
log = logging.getLogger(__name__)

def validate_date_format(date_str, date_format="%Y%m%d"):
    """
    Validates if a given string matches the expected date format.
    Simulates DWDate_Datum_Check from k.ksh.
    """
    if date_str is None:
        return False
    try:
        datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False

def prepare_parameters_task(**kwargs):
    """
    Replicates parameter parsing, default value assignment, and validation
    from r_ausd_bp_ta_bcp_msisdn.ksh and k_ausd_bp_ta_bcp_msisdn.ksh.
    Parameters are expected from Airflow's DAG run configuration or default_args.
    """
    ti = kwargs['ti']
    dag_run_conf = kwargs.get('dag_run', {}).get('conf', {})

    # Default values as per legacy script logic (current date for Stichtag if not provided)
    default_stichtag = datetime.now().strftime("%Y%m%d")
    
    # Get parameters from DAG run configuration or provide defaults
    p_stichtag = dag_run_conf.get('stichtag', default_stichtag)
    p_wiederanlaufwert = dag_run_conf.get('wiederanlaufwert') # No default, can be None

    log.info(f"Received stichtag: {p_stichtag}")
    log.info(f"Received wiederanlaufwert: {p_wiederanlaufwert}")

    # Validate stichtag
    if not validate_date_format(p_stichtag):
        raise ValueError(f"Invalid date format for stichtag: {p_stichtag}. Expected YYYYMMDD.")
    
    # In legacy ksh, there were checks like p_JobKennung, p_EintragsNr.
    # For BigQuery SQL, we primarily need p_Stichtag and other parameters specific to the SQL.
    # The SQL itself handles `v_datum` derivation, so p_stichtag might not be directly
    # passed to the BigQuery SQL in this specific case, but kept for consistency
    # with legacy parameter handling if future changes require it.
    # Here, we will pass a 'job_execution_date' to BigQuery to align with Stichtag concept.

    processed_params = {
        'job_execution_date': p_stichtag,
        'wiederanlaufwert': p_wiederanlaufwert, # This might be used in more complex logic not visible here.
        'job_kennung': 'DW.BERT_AUSD_BP_TA_BCP_MSISDN' # Example of a static parameter
    }
    
    # Push processed parameters to XCom for subsequent tasks
    ti.xcom_push(key='processed_job_parameters', value=processed_params)
    log.info(f"Processed parameters pushed to XCom: {processed_params}")
    return processed_params

def log_job_status_task(**kwargs):
    """
    Replicates logging and status setting functionality from legacy ksh scripts.
    In Airflow, this typically means logging messages and potentially
    retrieving information (like row counts) from XComs.
    Simulates DWMSG_SetzeStatusOK etc.
    """
    ti = kwargs['ti']
    
    # Retrieve parameters or results from previous tasks via XCom
    processed_params = ti.xcom_pull(task_ids='prepare_parameters', key='processed_job_parameters')
    bq_query_results = ti.xcom_pull(task_ids='execute_bq_transformation', key='return_value')

    log.info("Job DW.BERT_AUSD_BP_TA_BCP_MSISDN completed successfully.")
    log.info(f"Execution Date (Stichtag): {processed_params.get('job_execution_date')}")
    log.info(f"BigQuery Query Results: {bq_query_results}") # This could contain job_id, bytes_processed etc.

    # If the BigQuery job returned row counts, we could log them here.
    # Example: if bq_query_results is a dict and has 'num_inserted_rows'
    if bq_query_results and isinstance(bq_query_results, dict) and 'num_inserted_rows' in bq_query_results:
        log.info(f"Number of rows inserted into sof.ta_bcp_msisdn: {bq_query_results['num_inserted_rows']}")
    else:
        log.warning("Could not retrieve specific row count from BigQuery task.")

    # In a real scenario, you might interact with a metadata table here
    # to mark job status, similar to how DWMSG_ErzeugeEintrag/DWMSG_SetzeStatusOK worked.
    log.info("Final job status: OK")