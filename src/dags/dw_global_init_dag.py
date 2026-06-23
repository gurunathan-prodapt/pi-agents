#
# Airflow DAG: dw_global_init_dag
# Legacy Source: vobs/dw_source/istools/seu/template/.dw_global
# Job: vobs/dw_source/istools/seu/template/.dw_global
#
# Description: This DAG orchestrates the migration of the .dw_global KornShell script.
#              It fetches configuration values, calls a BigQuery stored procedure
#              to validate and derive environment settings, and handles the conditional
#              Cognos PowerPlay setup logic.
#

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import logging
from datetime import datetime

# Set up logging for this DAG
log = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id='dw_global_init_dag',
    default_args=default_args,
    description='Orchestrates environment setup from .dw_global script to BigQuery and Python',
    schedule_interval=None, # This DAG is likely triggered manually or by another DAG
    catchup=False,
    tags=['dw', 'environment', 'bigquery', 'setup'],
) as dag:
    # Task 1: Get Configuration Values
    # This PythonOperator simulates fetching configuration from various sources.
    # In a production environment, replace 'os.getenv' with calls to Airflow Variables,
    # Google Secret Manager, or a dedicated BigQuery configuration table.
    def get_config_values(**kwargs):
        """
        Fetches or defines configuration values required for the environment setup.
        These values correspond to the legacy environment variables.
        """
        # --- IMPORTANT: Replace these placeholder values with your actual project and dataset IDs ---
        gcp_project_id = os.getenv('GCP_PROJECT_ID', 'your-gcp-project-id')
        bq_dataset_id = os.getenv('BQ_DATASET_ID', 'your_dataset_name')
        # -----------------------------------------------------------------------------------------

        config = {
            'project_id': gcp_project_id,
            'dataset_id': bq_dataset_id,
            # Replace these with actual values, potentially from Airflow Variables or Secret Manager
            'dw_dir_root': os.getenv('DW_DIR_ROOT', '/app/dw'),
            'dw_dir_prot': os.getenv('DW_DIR_PROT', '/app/dw/prot'),
            'dw_dir_cubes': os.getenv('DW_DIR_CUBES', '/app/dw/cubes'),
            'dw_dir_imp_d1': os.getenv('DW_DIR_IMP_D1', '/app/dw/imp/d1'),
            'dw_dir_imp_xtra': os.getenv('DW_DIR_IMP_XTRA', '/app/dw/imp/xtra'),
            'dw_dir_imp_ctel': os.getenv('DW_DIR_IMP_CTEL', '/app/dw/imp/ctel'),
            'oracle_home': os.getenv('ORACLE_HOME', '/usr/local/oracle/product/12.2.0/dbhome_1'),
            # The initial LD_LIBRARY_PATH and PATH that the legacy script would have appended to
            'initial_ld_library_path': os.getenv('LD_LIBRARY_PATH_INITIAL', ''),
            'initial_path': os.getenv('PATH_INITIAL', '/usr/local/bin:/usr/bin:/bin'),
        }
        log.info(f"Configuration values retrieved: {config}")
        kwargs['ti'].xcom_push(key='env_config', value=config)

    get_config_task = PythonOperator(
        task_id='get_configuration_values',
        python_callable=get_config_values,
        provide_context=True,
    )

    # Task 2: Call BigQuery Stored Procedure
    # Executes the BigQuery stored procedure to validate and derive environment variables.
    # The output of this procedure (a SELECT statement) can be captured by downstream tasks
    # if using operators that support result retrieval, or the values can be explicitly passed.
    # This operator uses Jinja templating to pass parameters from the 'get_config_values' task.
    call_dw_global_init_proc = BigQueryExecuteQueryOperator(
        task_id='call_dw_global_init_procedure',
        sql=f"""
            CALL `{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['dataset_id'] }}}}.dw_global_init`(
                p_dw_dir_root => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['dw_dir_root'] }}}}',
                p_dw_dir_prot => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['dw_dir_prot'] }}}}',
                p_dw_dir_cubes => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['dw_dir_cubes'] }}}}',
                p_dw_dir_imp_d1 => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['dw_dir_imp_d1'] }}}}',
                p_dw_dir_imp_xtra => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['dw_dir_imp_xtra'] }}}}',
                p_dw_dir_imp_ctel => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['dw_dir_imp_ctel'] }}}}',
                p_oracle_home => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['oracle_home'] }}}}',
                p_initial_ld_library_path => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['initial_ld_library_path'] }}}}',
                p_initial_path => '{{{{ ti.xcom_pull(task_ids='get_configuration_values', key='env_config')['initial_path'] }}}}'
            );
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Ensure this Airflow connection is properly configured
    )

    # Task 3: Handle Conditional Cognos PowerPlay Setup
    # This PythonOperator addresses the conditional sourcing of the Cognos setup script.
    # In a cloud environment, direct file system checks are not applicable.
    # This task should be adapted based on the actual Cognos migration strategy:
    # - If Cognos is still needed, replicate its environment setup (e.g., in a container, via API).
    # - If Cognos is retired, this task might be removed or log its irrelevance.
    def handle_cognos_setup_logic(**kwargs):
        """
        Simulates or performs the necessary actions for Cognos PowerPlay setup.
        """
        log.info("Starting Cognos PowerPlay setup handling.")
        cognos_script_path = "/appl/local/cognos/cognos5.2/pya52b17/setpya.sh"
        
        # --- IMPORTANT: Cloud-native implementation for Cognos setup check ---
        # Instead of os.path.exists, consider:
        # 1. Checking for a configuration flag in Airflow Variables.
        # 2. Querying a metadata table in BigQuery.
        # 3. Checking for a marker file in Google Cloud Storage (e.g., using google.cloud.storage client).
        # 4. Calling an external API that manages Cognos instances.
        
        # For demonstration, we'll use an environment variable to simulate the condition.
        # In a real scenario, replace this with actual cloud-native checks.
        cognos_setup_enabled = os.getenv('ENABLE_COGNOS_CLOUD_SETUP', 'False').lower() == 'true'

        if cognos_setup_enabled:
            log.info(f"Cognos setup enabled (simulated). Proceeding with cloud-native environment setup for Cognos.")
            # This is where the actual logic to set up Cognos-related environment
            # variables or configurations for downstream tasks would go.
            # E.g., push specific settings to XComs for subsequent tasks,
            # or trigger a Cloud Run job that initializes a Cognos container.
            
            # Example: Setting an XCom to signal Cognos environment is ready
            kwargs['ti'].xcom_push(key='cognos_env_ready', value=True)
            log.info("Cognos environment configuration complete for current context.")
        else:
            log.info(f"Cognos setup is not enabled or conditions not met for: {cognos_script_path}. Skipping.")
            kwargs['ti'].xcom_push(key='cognos_env_ready', value=False)

    handle_cognos_setup_task = PythonOperator(
        task_id='handle_cognos_setup',
        python_callable=handle_cognos_setup_logic,
        provide_context=True,
    )

    # Define the task dependencies
    get_config_task >> call_dw_global_init_proc >> handle_cognos_setup_task