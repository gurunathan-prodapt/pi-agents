# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

import datetime
import logging
from airflow.exceptions import AirflowException
from google.cloud import bigquery

# Initialize logging
log = logging.getLogger(__name__)

def validate_parameters_func(**kwargs):
    """
    Placeholder function to simulate parameter validation.
    In the legacy script, this involved parsing -j, -f, -s, -l using getopts
    and validating with `pruefeParameterGesetzt`.
    This function will retrieve parameters from Airflow's `dag_run.conf`
    or default values and perform basic validation.
    Expected params:
        - j: Job name (e.g., 'MSISDN_PROCESSING')
        - f: Reference date (Stichtag) in YYYYMMDD format
        - s: Source system identifier (e.g., 'SAP')
        - l: Log level (e.g., 'INFO')
    """
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf:
        params = dag_run.conf
    else:
        # Provide default/example parameters for testing or manual trigger
        params = {
            'j': 'MSISDN_PROCESSING',
            'f': datetime.date.today().strftime('%Y%m%d'), # Default to today
            's': 'DEFAULT_SOURCE',
            'l': 'INFO'
        }
        log.info("No dag_run.conf found, using default parameters: %s", params)

    job_name = params.get('j')
    reference_date_str = params.get('f')
    source_system = params.get('s')
    log_level = params.get('l')

    if not all([job_name, reference_date_str, source_system, log_level]):
        raise AirflowException("Missing one or more required parameters (j, f, s, l).")

    try:
        # Validate reference date format (YYYYMMDD)
        datetime.datetime.strptime(reference_date_str, '%Y%m%d')
    except ValueError:
        raise AirflowException(f"Invalid reference date format for 'f': {reference_date_str}. Expected YYYYMMDD.")

    kwargs['ti'].xcom_push(key='job_name', value=job_name)
    kwargs['ti'].xcom_push(key='reference_date_str', value=reference_date_str)
    kwargs['ti'].xcom_push(key='source_system', value=source_system)
    kwargs['ti'].xcom_push(key='log_level', value=log_level)

    log.info(f"Parameters validated: job_name={job_name}, reference_date={reference_date_str}, source_system={source_system}, log_level={log_level}")

def derive_dates_func(**kwargs):
    """
    Placeholder function to replace `gestern.ksh` functionality.
    Calculates today's date and yesterday's date based on the reference_date.
    The reference_date is expected to be passed via XCom from the
    validate_parameters_func.
    """
    reference_date_str = kwargs['ti'].xcom_pull(task_ids='validate_parameters', key='reference_date_str')

    try:
        reference_date = datetime.datetime.strptime(reference_date_str, '%Y%m%d').date()
    except ValueError:
        raise AirflowException(f"Invalid reference date format pulled from XCom: {reference_date_str}. Expected YYYYMMDD.")

    today_date = reference_date
    yesterday_date = reference_date - datetime.timedelta(days=1)

    today_str = today_date.strftime('%Y%m%d')
    yesterday_str = yesterday_date.strftime('%Y%m%d')

    kwargs['ti'].xcom_push(key='today_date', value=today_str)
    kwargs['ti'].xcom_push(key='yesterday_date', value=yesterday_str)

    log.info(f"Derived dates: today_date={today_str}, yesterday_date={yesterday_str}")

def capture_record_count_func(**kwargs):
    """
    Placeholder function to capture record counts from BigQuery.
    In the legacy script, this involved reading a $tmpFile.
    This function will query a BigQuery table (e.g., project.dataset.PoolBasisprodukt)
    to get the number of records processed or inserted.
    Requires:
    - `gcp_project_id`: GCP Project ID (from Airflow variable or connection)
    - `target_dataset_id`: BigQuery Dataset ID
    - `target_table_id`: BigQuery Table ID (e.g., PoolBasisprodukt)
    - `reference_date_str`: To filter records if applicable.
    """
    gcp_project_id = kwargs['dag'].default_args.get('project_id', 'your-gcp-project-id') # Replace with actual project ID
    target_dataset_id = 'your_bigquery_dataset' # Replace with actual dataset ID
    target_table_id = 'PoolBasisprodukt' # Replace with actual table ID

    reference_date_str = kwargs['ti'].xcom_pull(task_ids='validate_parameters', key='reference_date_str')

    client = bigquery.Client(project=gcp_project_id)
    table_ref = f"{gcp_project_id}.{target_dataset_id}.{target_table_id}"

    # Example query: count records for the reference date
    # This query needs to be adapted based on the actual schema and filtering logic
    query = f"""
    SELECT
        COUNT(1)
    FROM
        `{table_ref}`
    WHERE
        DATE(your_date_column) = PARSE_DATE('%Y%m%d', '{reference_date_str}')
    """
    log.info(f"Executing BigQuery count query: {query}")

    try:
        query_job = client.query(query)
        results = query_job.result()
        record_count = next(results)[0]
        log.info(f"Captured record count: {record_count} from {table_ref} for date {reference_date_str}")
        kwargs['ti'].xcom_push(key='processed_record_count', value=record_count)
    except Exception as e:
        raise AirflowException(f"Failed to capture record count from BigQuery: {e}")

# Additional utility functions (placeholders) that would replace legacy ksh scripts:
# - f_alis_msgerr.ksh: For custom error messaging/logging. Airflow's native logging
#   and alerting are generally preferred, but specific logic can be added here.
def custom_error_handler(context):
    """
    Placeholder for custom error handling logic, equivalent to f_alis_msgerr.ksh.
    This could send notifications, log specific details, etc.
    Airflow's on_failure_callback can use this.
    """
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    log.error(f"Task {task_instance.task_id} failed with exception: {exception}")
    # Add custom notification logic here (e.g., Slack, PagerDuty, email)
    log.info("Custom error handling invoked.")

# - h_alis_date.ksh: For date format validation. Partially covered in validate_parameters_func.
#   Could add more complex date manipulations here if needed.
def is_valid_date_format(date_str, date_format='%Y%m%d'):
    """
    Checks if a string matches a given date format.
    """
    try:
        datetime.datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False

# - h_alis_parameter.ksh: For more complex parameter validation beyond basic checks.
#   Partially covered in validate_parameters_func.
def validate_complex_parameter(param_value, rules):
    """
    Placeholder for more intricate parameter validation based on predefined rules.
    """
    log.debug(f"Validating parameter '{param_value}' with rules: {rules}")
    # Example: Check if param_value is in a list of allowed values
    if 'allowed_values' in rules and param_value not in rules['allowed_values']:
        raise AirflowException(f"Parameter '{param_value}' not in allowed values: {rules['allowed_values']}")
    return True

# - h_alis_sqlplus.ksh: The execution part is replaced by BigQueryOperator.
#   Any non-execution related logic from this script (e.g., connection string
#   preparation, specific logging) would go here if not handled by Airflow connections.
#   For now, no direct Python equivalent needed as BigQueryOperator handles execution.