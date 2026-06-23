# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
## 1. Purpose & Scope
This document outlines the migration plan for the KornShell script `h_alis_sqlplus.ksh`. The script serves as a utility wrapper for executing SQL*Plus scripts, providing helper routines for validating script existence and readability, and offering basic error reporting. Its primary function is to standardize the invocation of SQL*Plus commands within a larger legacy ecosystem.

## 2. Source Inventory
-   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    -   **Technology:** KornShell (ksh)
    -   **Complexity Tier:** Medium
    -   **Automation Bucket:** Semi-auto (B2)
    -   **Purpose:** Utility script for orchestrating SQL*Plus executions. It defines a function `starteSQLSkript` that takes an error entry number, the SQL script path, and additional parameters. It validates inputs, checks for file readability, logs invocation details, and executes `sqlplus` using the `DW_ORAUSER` environment variable. Error reporting is delegated to an external function `DWMSG_MeldeFehler`.
    -   **Description (from analysis):** "This KornShell script provides helper routines for executing SQL*Plus scripts, including validation for script existence and basic error handling."
    -   **Preliminary Role:** Active orchestration utility.

## 3. Target Architecture
The script's core functionality, which is orchestrating and executing other SQL scripts, is best suited for migration to Google Cloud Composer (Apache Airflow).
-   **Core Functionality:** The `starteSQLSkript` function will be re-implemented as a Python function within an Airflow DAG or a custom Airflow operator. This component will handle parameter validation, error checks, and the invocation of SQL.
-   **SQL*Plus Execution:** The calls to `sqlplus` will be replaced by native BigQuery SQL execution. This means any SQL scripts previously executed by `sqlplus` will need to be converted to BigQuery SQL dialect. The Airflow component will use Airflow's Google Cloud BigQuery operators (e.g., `BigQueryExecuteQueryOperator`) to execute these converted SQL queries.
-   **Parameterization:** Airflow's robust templating and parameter passing mechanisms will be leveraged for arguments such as the error entry number, the SQL script identifier (which might become a GCS path to a SQL file or an embedded SQL string), and any additional SQL script parameters.
-   **Error Handling:** The external `DWMSG_MeldeFehler` calls will be integrated with Airflow's standard logging (which feeds into Google Cloud Logging) and error handling. This allows for centralized monitoring and alerting through Google Cloud Monitoring.
-   **Environment Variables:** The `DW_ORAUSER` environment variable will be securely managed using Airflow Connections, ensuring that sensitive credentials are not exposed.

## 4. Data Flow & Lineage
The original script acts as an execution wrapper, dynamically invoking other SQL*Plus scripts. The migrated Airflow component will maintain this orchestration role.
-   **Invocation:** Other Airflow DAGs or external systems requiring SQL execution will trigger this new Airflow component, passing the necessary BigQuery SQL script reference and parameters.
-   **Execution Flow:**
    1.  The Airflow component receives an "entry number," a BigQuery SQL script reference (e.g., GCS path, BigQuery dataset.table.view reference, or inline SQL), and a list of parameters.
    2.  It performs validation checks similar to the original script (e.g., ensuring required parameters are present, verifying the existence/accessibility of the SQL script if it's a GCS path).
    3.  It constructs and executes the BigQuery SQL query using the `BigQueryExecuteQueryOperator`, passing along the collected parameters.
    4.  It logs execution details and handles any errors through Airflow's built-in mechanisms.
-   **Data Lineage:** The Airflow orchestrator itself does not process data. Instead, it facilitates the execution of BigQuery SQL queries that will perform data transformations. Data lineage will primarily be tracked within BigQuery for the actual SQL operations and within Cloud Composer for the orchestration events.

## 5. Transformation Logic
The core `starteSQLSkript` function will be re-implemented in Python for Airflow.

**Original KornShell (`starteSQLSkript`)**
```ksh
starteSQLSkript(){
    typeset p_Eintragsnr=$1
    typeset p_Skript=$2;
    typeset errcode

    shift 2
    
    if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    then
        DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"
        return 196
    fi

    if [ ! -r $p_Skript ]
    then
        DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript
        return 201
    fi

    echo "Rufe SQL*PLUS auf mit folgenden Einstellungen"
    echo "Sql*Plus-Skript : $p_Skript"
    echo "Skript-Parameter: $*"

    set +e
    sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    errcode=$?
    set -e
    return $errcode
}
```

**Target Python/Airflow Implementation (Conceptual `execute_bigquery_script` function/operator)**
```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

def execute_bigquery_script(entry_number: str, script_ref: str, sql_parameters: dict = None, **kwargs):
    """
    Executes a BigQuery SQL script or query, analogous to the legacy starteSQLSkript.
    
    :param entry_number: An identifier for logging/error reporting.
    :param script_ref: Reference to the SQL script (e.g., GCS path, or direct SQL string).
                       If it's a GCS path, the SQL content will be fetched from there.
    :param sql_parameters: A dictionary of parameters to pass to the BigQuery query.
    """
    sql_parameters = sql_parameters or {}

    # 1. Parameter Validation (equivalent to `if [ -z ... ]`)
    if not entry_number or not script_ref:
        logger.error(f"Error {entry_number} E 196: Missing 'entry_number' or 'script_ref' for BigQuery execution.")
        raise AirflowException(f"Missing required parameters (entry_number, script_ref) for task {kwargs['task_instance'].task_id}")

    sql_content = script_ref
    # If script_ref points to a GCS file, fetch its content
    if script_ref.startswith("gs://"):
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket_name = script_ref.split('/')[2]
            blob_name = '/'.join(script_ref.split('/')[3:])
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            if not blob.exists():
                logger.error(f"Error {entry_number} E 201: GCS SQL script '{script_ref}' not found or inaccessible.")
                raise AirflowException(f"GCS SQL script '{script_ref}' not found.")
            sql_content = blob.download_as_text()
            logger.info(f"Loaded SQL content from GCS: {script_ref}")
        except Exception as e:
            logger.error(f"Error {entry_number} E 201: Failed to read GCS SQL script '{script_ref}': {e}")
            raise AirflowException(f"Failed to read GCS SQL script '{script_ref}': {e}")
    
    logger.info(f"Initiating BigQuery SQL execution for script reference: {script_ref}")
    logger.info(f"SQL parameters: {sql_parameters}")

    # 2. SQL Execution (equivalent to `sqlplus ...`)
    try:
        BigQueryExecuteQueryOperator(
            task_id=f"execute_bq_script_{entry_number}", # Task ID should ideally be unique within a DAG run
            sql=sql_content,
            use_legacy_sql=False,
            params=sql_parameters,
            gcp_conn_id='google_cloud_default', # Assuming 'google_cloud_default' Airflow connection
            # Add other BigQuery specific parameters like destination_dataset_table, write_disposition etc.
            # based on the specific SQL script being executed.
        ).execute(context=kwargs) # Execute the operator
        logger.info(f"Successfully executed BigQuery SQL script via reference: {script_ref}")
    except Exception as e:
        logger.error(f"Error executing BigQuery SQL for script reference '{script_ref}': {e}")
        # Re-raise as AirflowException to ensure task failure
        raise AirflowException(f"BigQuery execution failed for '{script_ref}': {e}")

# Example Airflow DAG snippet showing how this would be used:
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
#
# with DAG(
#     dag_id='example_sql_executor',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=['utility'],
# ) as dag:
#     run_my_sql_task = PythonOperator(
#         task_id='run_daily_report_sql',
#         python_callable=execute_bigquery_script,
#         op_kwargs={
#             'entry_number': 'REPORT_001',
#             'script_ref': 'gs://my-gcs-bucket/sql/daily_report.sql', # or 'SELECT * FROM `project.dataset.table` WHERE date = "{{ ds }}"'
#             'sql_parameters': {'report_date': '{{ ds }}', 'region': 'US'}
#         },
#     )
```

## 6. External Dependencies
The original `h_alis_sqlplus.ksh` script has the following external dependencies and their proposed replacements:

-   **`sqlplus`:** This is an Oracle SQL command-line client.
    -   **Replacement:** Native BigQuery SQL execution, facilitated by Airflow's `BigQueryExecuteQueryOperator`. This implies that all SQL scripts previously executed by `sqlplus` must be converted from Oracle SQL dialect to BigQuery SQL dialect.
-   **`DW_ORAUSER`:** An environment variable likely holding Oracle database connection details (e.g., username).
    -   **Replacement:** This will be securely managed using an Airflow Connection (e.g., `google_cloud_default` or a custom BigQuery connection). Credentials will be stored securely within Airflow's backend, not as environment variables.
-   **`DWMSG_MeldeFehler`:** An external function used for standardized error reporting.
    -   **Replacement:** Airflow's native logging capabilities, which automatically integrate with Google Cloud Logging. Error details will be captured in structured logs, and alerts can be configured in Google Cloud Monitoring based on specific log patterns or error severities.

No other external systems (like SFTP, S3, other databases directly) were identified as being directly referenced *by this specific utility script*. Any such interactions would reside within the SQL scripts invoked by this utility and would be addressed in their respective migration plans.

## 7. Unresolved / Risks
-   **SQL Dialect Conversion (B2/B3):** The primary and most significant risk is the conversion of all Oracle SQL scripts that this utility invokes to BigQuery SQL. This requires detailed analysis of each SQL script, often involving semi-automatic tools or manual rewrite, classified as a B2 (semi-auto) to B3 (manual) migration bucket. The complexity analysis for `h_alis_sqlplus.ksh` itself does not cover the complexity of these downstream SQL scripts.
-   **Dynamic Script Resolution (B2):** If `p_Skript` (the SQL script reference) in the legacy system is generated dynamically through complex logic beyond simple variable substitution, mapping this dynamic behavior to a GCS path or inline SQL in Airflow might require custom development.
-   **`DWMSG_MeldeFehler` Semantic Fidelity (B2):** Replicating the exact functionality and integration points of `DWMSG_MeldeFehler` (e.g., specific error codes, internal alerting, or external system notifications) in Cloud Logging and Monitoring needs careful analysis to ensure functional parity.
-   **Invoked SQL Complexity (B4):** This migration focuses on the orchestration utility. The complexity and migration strategy for the actual SQL scripts being invoked (which could range from simple queries to complex ETL logic) are not covered and might require redesign (B4).
-   **Oracle Specific Features (B4):** If the invoked SQL scripts rely heavily on Oracle-specific features (PL/SQL, proprietary functions, specific data types), their conversion to BigQuery will present significant challenges and may necessitate architectural redesign.

## 8. Build Plan
The migration of `h_alis_sqlplus.ksh` will involve creating a reusable Python component within an Airflow environment.

1.  **Phase 1: Foundation Setup**
    *   **Task 1.1:** Configure Airflow BigQuery Connection.
        *   **Description:** Establish a secure Airflow Connection object for accessing the target BigQuery project.
        *   **Language:** YAML/Python (Airflow configuration)
    *   **Task 1.2:** Define Logging and Monitoring Standards.
        *   **Description:** Map legacy `DWMSG_MeldeFehler` error codes and logging patterns to Google Cloud Logging severity levels and define initial Cloud Monitoring alerts.
        *   **Language:** Google Cloud Logging/Monitoring configuration.

2.  **Phase 2: Component Development**
    *   **Task 2.1:** Develop `execute_bigquery_script` Python Function.
        *   **Description:** Implement the Python function `execute_bigquery_script` (as detailed in Section 5) that encapsulates the logic of `starteSQLSkript`. This includes parameter validation, fetching SQL content (if from GCS), and integrating with `BigQueryExecuteQueryOperator`.
        *   **Language:** Python
    *   **Task 2.2:** Unit Tests for `execute_bigquery_script`.
        *   **Description:** Write comprehensive unit tests for the `execute_bigquery_script` function to cover all validation, error paths, and BigQuery operator invocation.
        *   **Language:** Python

3.  **Phase 3: Integration and Testing**
    *   **Task 3.1:** Create Example Airflow DAG.
        *   **Description:** Develop a simple Airflow DAG that demonstrates the usage of `execute_bigquery_script` with a placeholder BigQuery SQL query (either inline or from GCS). This DAG will serve as a template for future integrations.
        *   **Language:** Python (Airflow DAG)
    *   **Task 3.2:** Integration Testing.
        *   **Description:** Deploy the example DAG to a development Cloud Composer environment. Verify successful execution, correct logging in Cloud Logging, and appropriate alerting in Cloud Monitoring. Test scenarios with valid and invalid inputs.
        *   **Language:** Python (Airflow) and BigQuery SQL (for test query).

4.  **Phase 4: Documentation and Handover**
    *   **Task 4.1:** Update Internal Documentation.
        *   **Description:** Document the new Airflow component, its parameters, usage patterns, and how it replaces the `h_alis_sqlplus.ksh` script. Provide guidelines for converting Oracle SQL scripts for use with this component.
        *   **Language:** Markdown/Confluence (internal documentation)