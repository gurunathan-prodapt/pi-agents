# Python script for orchestrating BigQuery environment initialization (e.g., Airflow DAG)
# Legacy source: vobs/dw_source/istools/seu/template/.dw_init
# Job: vobs/dw_source/istools/seu/template/.dw_init

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models.variable import Variable

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# --- Airflow Variables (or similar configuration management) ---
# It is recommended to store sensitive or frequently changing parameters in Airflow Variables
# or a secrets manager like Google Secret Manager.
# Example:
# Variable.set("dw_home_path", "/gcp/env/root")
# Variable.set("dw_login_placeholder", "your_customer_login")
# Variable.set("dw_oracle_exists_816", "False") # Example: Set as True if applicable

# Placeholder for project and dataset. Replace with actual values.
BIGQUERY_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BIGQUERY_DATASET_ID = "dataset" # As used in the DDL and stored procedure

with DAG(
    dag_id='dw_environment_init',
    default_args=default_args,
    schedule_interval=None, # This DAG might be triggered by other DAGs or external events
    catchup=False,
    tags=['dw', 'environment', 'bigquery'],
    description='Initializes environment variables for Data Warehouse in BigQuery',
) as dag:
    # --- Task 1: Ensure BigQuery Configuration Tables exist (DDL) ---
    create_global_config_table = BigQueryExecuteQueryOperator(
        task_id='create_dw_global_config_table',
        sql="""
            CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dw_global_config`
            (
                config_key STRING NOT NULL OPTIONS(description="Configuration key from .dw_global"),
                config_value STRING NOT NULL OPTIONS(description="Configuration value from .dw_global"),
                description STRING OPTIONS(description="Optional description for the configuration item")
            )
            OPTIONS(
                description="Configuration values migrated from .dw_global"
            );
        """.format(project=BIGQUERY_PROJECT_ID, dataset=BIGQUERY_DATASET_ID),
        use_legacy_sql=False,
    )

    create_lokal_config_table = BigQueryExecuteQueryOperator(
        task_id='create_dw_lokal_config_table',
        sql="""
            CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dw_lokal_config`
            (
                config_key STRING NOT NULL OPTIONS(description="Configuration key from .dw_lokal"),
                config_value STRING NOT NULL OPTIONS(description="Configuration value from .dw_lokal"),
                description STRING OPTIONS(description="Optional description for the configuration item")
            )
            OPTIONS(
                description="Configuration values migrated from .dw_lokal"
            );
        """.format(project=BIGQUERY_PROJECT_ID, dataset=BIGQUERY_DATASET_ID),
        use_legacy_sql=False,
    )

    # --- Task 2: Call the BigQuery Stored Procedure for Environment Initialization ---
    # Parameters for the stored procedure. These would typically come from Airflow Variables,
    # XComs from previous tasks, or a configuration service.
    # For demonstration, using placeholder values and Airflow Variables for dynamic parts.
    _home_path = Variable.get("dw_home_path", default="/gcp/data_warehouse_root") # Example default
    _login_placeholder = Variable.get("dw_login_placeholder", default="default_login_user")
    _initial_oracle_home = Variable.get("dw_initial_oracle_home", default="") # Empty string means BQ proc will try to resolve
    _oracle_exists_816 = Variable.get("dw_oracle_exists_816", default="False").lower() == "true"
    _oracle_exists_734 = Variable.get("dw_oracle_exists_734", default="False").lower() == "true"
    _oracle_exists_733 = Variable.get("dw_oracle_exists_733", default="False").lower() == "true"
    _oracle_exists_732 = Variable.get("dw_oracle_exists_732", default="False").lower() == "true"
    _oracle_exists_723 = Variable.get("dw_oracle_exists_723", default="False").lower() == "true"

    call_init_procedure = BigQueryExecuteQueryOperator(
        task_id='call_init_dw_environment_procedure',
        sql=f"""
            CALL `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.init_dw_environment`(
                home_path => '{_home_path}',
                login_placeholder => '{_login_placeholder}',
                initial_oracle_home => '{_initial_oracle_home}',
                oracle_exists_816 => {_oracle_exists_816},
                oracle_exists_734 => {_oracle_exists_734},
                oracle_exists_733 => {_oracle_exists_733},
                oracle_exists_732 => {_oracle_exists_732},
                oracle_exists_723 => {_oracle_exists_723}
            );
        """,
        use_legacy_sql=False,
        # Set a destination table if you want to capture the output of the SELECT statement
        # within the procedure. E.g., for XCom push or further processing.
        # destination_dataset_table=f'{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dw_env_vars_current_run_{{ ds_nodash }}',
        # write_disposition='WRITE_TRUNCATE'
    )

    # Define task dependencies
    [create_global_config_table, create_lokal_config_table] >> call_init_procedure