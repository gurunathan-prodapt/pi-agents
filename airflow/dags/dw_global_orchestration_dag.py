# Airflow DAG for dw_global orchestration
# Legacy source: vobs/dw_source/istools/seu/template/.dw_global
# Job: vobs/dw_source/istools/seu/template/.dw_global

# This Airflow DAG orchestrates the execution of the BigQuery `dw_global_init` stored procedure.
# It fetches configuration parameters, calls the stored procedure, captures its output,
# and makes the computed environment variables available for subsequent tasks.

# Prerequisites:
# - Apache Airflow installed and configured.
# - Google Cloud connection configured in Airflow (e.g., 'google_cloud_default').
# - Airflow Variables set for 'gcp_project_id', 'bq_dataset_name', and all
#   'p_dw_dir_*', 'p_oracle_home', 'p_existing_ld_library_path', 'p_existing_path',
#   'p_cognos_setup_exists' parameters.
# - Airflow provider `apache-airflow-providers-google` installed.
# - Python packages: `apache-airflow-providers-google`, `google-cloud-bigquery`.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import json

# Define your project and dataset. These should be set as Airflow Variables.
GCP_PROJECT_ID = Variable.get("gcp_project_id", default="your_project_id")
BQ_DATASET_NAME = Variable.get("bq_dataset_name", default="your_dataset_name")
BQ_STORED_PROC_NAME = f"{BQ_DATASET_NAME}.dw_global_init"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def _fetch_config_from_airflow_vars(**kwargs):
    """
    Fetches configuration parameters from Airflow Variables and pushes them to XCom.
    These parameters will be used as inputs for the BigQuery stored procedure.
    """
    config_params = {
        "p_dw_dir_root": Variable.get("dw_dir_root", "/default/dw/root"),
        "p_dw_dir_prot": Variable.get("dw_dir_prot", "/default/dw/prot"),
        "p_dw_dir_cubes": Variable.get("dw_dir_cubes", "/default/dw/cubes"),
        "p_dw_dir_imp_d1": Variable.get("dw_dir_imp_d1", "/default/dw/imp_d1"),
        "p_dw_dir_imp_xtra": Variable.get("dw_dir_imp_xtra", "/default/dw/imp_xtra"),
        "p_dw_dir_imp_ctel": Variable.get("dw_dir_imp_ctel", "/default/dw/imp_ctel"),
        "p_oracle_home": Variable.get("oracle_home", "/default/oracle/home"),
        "p_existing_ld_library_path": Variable.get("existing_ld_library_path", "/default/lib"),
        "p_existing_path": Variable.get("existing_path", "/default/bin"),
        "p_cognos_setup_exists": Variable.get("cognos_setup_exists", "false").lower() == "true",
    }
    kwargs['ti'].xcom_push(key='dw_global_sp_params', value=config_params)
    print("Fetched config parameters from Airflow Variables and pushed to XCom.")

def _process_dw_global_output(project_id, dataset_id, temp_results_table_id, **kwargs):
    """
    Reads the results from the temporary BigQuery table created by the stored procedure call.
    Makes the computed environment variables available for downstream tasks via XCom
    as a dictionary. Also checks for the Cognos setup note.
    """
    ti = kwargs['ti']
    hook = BigQueryHook(gcp_conn_id='google_cloud_default') # Assumes 'google_cloud_default' connection

    table_ref = f"{project_id}.{dataset_id}.{temp_results_table_id}"
    print(f"Attempting to read results from BigQuery table: {table_ref}")

    # Query the temporary table, converting the single result row to a JSON string
    # and then parsing it into a Python dictionary.
    query = f"SELECT TO_JSON(t) FROM `{table_ref}` AS t LIMIT 1"
    json_result = hook.get_records(query) # get_records returns a list of tuples

    if json_result:
        # json_result will be like [[json_string_of_row]]
        computed_vars_dict = json.loads(json_result[0][0])
        print(f"Computed environment variables (dictionary): {computed_vars_dict}")
        ti.xcom_push(key='dw_global_vars', value=computed_vars_dict)

        # Check for Cognos note from the stored procedure's output
        cognos_note = computed_vars_dict.get('cognos_note')
        if cognos_note:
            print(f"Cognos setup note: {cognos_note}. External orchestration required.")
            # Here, you could trigger a specific downstream task for Cognos setup,
            # send an alert, or update a status.
        else:
            print("No specific Cognos setup note detected.")
    else:
        raise ValueError(f"Could not retrieve JSON formatted results from temporary table {table_ref}.")


with DAG(
    dag_id='dw_global_orchestration',
    default_args=default_args,
    description='Orchestrates the BigQuery dw_global_init stored procedure to set environment variables.',
    schedule_interval=None, # Define your schedule, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'environment', 'dw'],
) as dag:
    
    # Task 1: Fetch configuration parameters from Airflow Variables
    fetch_config_task = PythonOperator(
        task_id='fetch_config_parameters',
        python_callable=_fetch_config_from_airflow_vars,
    )

    # Task 2: Call the BigQuery stored procedure and store its 'RETURNS TABLE' output
    # into a dynamically named temporary table.
    call_dw_global_init = BigQueryInsertJobOperator(
        task_id="call_dw_global_init",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                SELECT * FROM CALL `{GCP_PROJECT_ID}.{BQ_STORED_PROC_NAME}`(
                    p_dw_dir_root => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_dw_dir_root }}}}',
                    p_dw_dir_prot => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_dw_dir_prot }}}}',
                    p_dw_dir_cubes => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_dw_dir_cubes }}}}',
                    p_dw_dir_imp_d1 => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_dw_dir_imp_d1 }}}}',
                    p_dw_dir_imp_xtra => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_dw_dir_imp_xtra }}}}',
                    p_dw_dir_imp_ctel => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_dw_dir_imp_ctel }}}}',
                    p_oracle_home => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_oracle_home }}}}',
                    p_existing_ld_library_path => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_existing_ld_library_path }}}}',
                    p_existing_path => '{{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_existing_path }}}}',
                    p_cognos_setup_exists => {{{{ ti.xcom_pull(task_ids=\'fetch_config_parameters\', key=\'dw_global_sp_params\').p_cognos_setup_exists }}}}
                )
                """,
                "useLegacySql": False,
                # Store the result in a temporary table that will be automatically managed
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET_NAME,
                    "tableId": "temp_dw_global_init_results_{{ ds_nodash }}_{{ ts_nodash | replace(\':\', \'_\') }}"
                },
                "writeDisposition": "WRITE_TRUNCATE", # Overwrite if exists, good for temp table
            }
        },
    )

    # Task 3: Process the results from the temporary BigQuery table
    process_results_task = PythonOperator(
        task_id='process_dw_global_output',
        python_callable=_process_dw_global_output,
        op_kwargs={
            'project_id': GCP_PROJECT_ID,
            'dataset_id': BQ_DATASET_NAME,
            'temp_results_table_id': call_dw_global_init.destination_table['tableId'], # Pass the dynamic table ID to the Python task
        }
    )

    # Define the task dependencies
    fetch_config_task >> call_dw_global_init >> process_results_task