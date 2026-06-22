# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_v_ta_action_assoc.ksh`, serves as an orchestration wrapper for a data reconciliation process related to the `ta_action_assoc` table. Its primary business purpose is to ensure the correct environment setup, parameter handling, error logging, and execution of a core KornShell script, `k_ausd_v_ta_action_assoc.ksh`, which presumably performs the actual data reconciliation logic. The script itself does not contain business logic for data transformation but manages the execution flow and error handling of the reconciliation job.

## 2. Source Inventory
The job consists of a single primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh`
    *   **Technology:** KornShell (KSH)
    *   **Tier:** Simple (orchestration, no complex data transformations)
    *   **Automation Bucket:** Semi-Auto (Requires translation of shell scripting patterns to Airflow/Python, but logic is contained)
    *   **Purpose:** ETL Wrapper/Orchestration

## 3. Target Architecture
The migration target platform is Google Cloud BigQuery. Given the orchestration nature of the source script, the migrated solution will leverage Google Cloud Composer (Apache Airflow) for workflow management and scheduling. Any underlying data processing or reconciliation logic (expected to be in `k_ausd_v_ta_action_assoc.ksh`) will be translated to BigQuery SQL, Python with BigQuery client libraries, or potentially a PySpark job if complex transformations are involved.

*   **Orchestration:** Google Cloud Composer (Apache Airflow)
    *   An Airflow DAG will replace the KornShell wrapper.
    *   Tasks in the DAG will handle environment initialization, parameter passing, logging, error handling, and the invocation of the core data reconciliation logic.
*   **Data Processing:** Google BigQuery / Python
    *   The business logic currently within `k_ausd_v_ta_action_assoc.ksh` will be re-implemented.
    *   If `k_ausd_v_ta_action_assoc.ksh` contains SQL, it will be migrated to BigQuery SQL.
    *   If it involves file operations or complex procedural logic, it will be re-implemented in Python, utilizing BigQuery client libraries for data interaction.
*   **Logging & Monitoring:** Google Cloud Logging & Monitoring
    *   Standard Airflow logging will integrate with Cloud Logging.
    *   Cloud Monitoring will be used for alerts and operational visibility.

## 4. Data Flow & Lineage
The current script `r_ausd_v_ta_action_assoc.ksh` acts as an entry point.
1.  **Initialization**: The script initializes the environment by sourcing `$HOME/.dw_init` and error handling functions from `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`.
2.  **Parameter Parsing**: It parses command-line parameters (though no specific parameters `s:` or `l:` are used in the provided code, only `-h` for help).
3.  **Logging Setup**: Sets up job-specific logging, including a job identifier (`JobKennung="BERT_V_TA_ACTION_ASSOC"`) and a log file.
4.  **Core Logic Invocation**: The script's primary function is to invoke `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh` passing `JobKennung` and `DW_EintragsNr` as parameters. This indicates a direct dependency and execution flow.
5.  **Error Handling & Exit**: Traps are set for `INT` and `ERR` signals to ensure proper error logging and graceful exit. Upon successful completion, it logs a success message.

**Migrated Flow:**
*   An Airflow DAG will be triggered (e.g., on a schedule or external event).
*   A `BashOperator` or `PythonOperator` task will handle the initial environment setup and parameter extraction (if any are still relevant).
*   A `PythonOperator` task will manage logging (e.g., using Python's `logging` module integrated with Cloud Logging) and error handling for the overall DAG.
*   The core data reconciliation logic, migrated to BigQuery SQL or Python (e.g., a `BigQueryOperator` or another `PythonOperator`), will be executed as a downstream task.
*   The `k_ausd_v_ta_action_assoc.ksh` script (the core logic) will be analyzed separately to determine its data sources (reads) and data targets (writes). These will then map to BigQuery tables.

## 5. Transformation Logic
The current script `r_ausd_v_ta_action_assoc.ksh` itself contains no data transformation logic. Its transformation is purely operational:
*   **Legacy:** Environment variable sourcing (`. $HOME/.dw_init`, etc.), shell-based parameter parsing (`getopts`), shell-based logging calls (`DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`), and conditional execution (`if [ ! $ErrNr -eq 0 ]`).
*   **Target:**
    *   Environment sourcing will be replaced by Airflow's environment management (e.g., connection secrets, environment variables in Composer).
    *   Parameter parsing will be handled by Airflow DAG parameters or configuration.
    *   Logging and error handling will leverage Airflow's built-in capabilities and Python logging, integrating with Google Cloud Logging.
    *   The invocation of `k_ausd_v_ta_action_assoc.ksh` will be replaced by the execution of the migrated core reconciliation logic as a separate Airflow task.

**Example Migration Pattern for `r_ausd_v_ta_action_assoc.ksh` (Illustrative):**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator # if core logic is BQ SQL
from datetime import datetime
import logging

# Configure logging for the DAG
log = logging.getLogger(__name__)

def setup_environment_and_log_start(job_kennung, entry_nr):
    log.info(f"Job started: {job_kennung} with Entry Number: {entry_nr}")
    # Simulate environment setup or any initial checks here
    # In a real scenario, this might involve fetching secrets, validating config etc.
    return True

def handle_failure(context):
    task_instance = context.get('task_instance')
    log.error(f"Task {task_instance.task_id} failed with exception: {context.get('exception')}")
    # Additional error handling logic, e.g., send alerts, update status tables
    pass

def log_success(job_kennung, entry_nr):
    log.info(f"Job completed successfully: {job_kennung} with Entry Number: {entry_nr}")
    # Update status tables, etc.
    pass

with DAG(
    dag_id='ta_action_assoc_reconciliation_wrapper',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Or define a schedule e.g., '@daily'
    catchup=False,
    tags=['data_reconciliation', 'ta_action_assoc'],
    on_failure_callback=handle_failure,
) as dag:
    # Simulate DW_EintragsNr and JobKennung generation
    # In Airflow, these could be pulled from XComs, variables, or dynamic values
    job_kennung_val = "BERT_V_TA_ACTION_ASSOC"
    entry_nr_val = "{{ ti.xcom_pull(task_ids='generate_entry_number', key='return_value') }}"

    generate_entry_number_task = PythonOperator(
        task_id='generate_entry_number',
        python_callable=lambda: datetime.now().strftime('%Y%m%d%H%M%S'), # Simple example
    )

    setup_task = PythonOperator(
        task_id='setup_environment_and_log_start',
        python_callable=setup_environment_and_log_start,
        op_kwargs={'job_kennung': job_kennung_val, 'entry_nr': entry_nr_val},
    )

    # This task represents the migration of k_ausd_v_ta_action_assoc.ksh
    # Placeholder: this would be replaced by actual BQ SQL or Python logic
    execute_core_reconciliation_task = BashOperator( # Or PythonOperator/BigQueryOperator
        task_id='execute_core_reconciliation',
        bash_command=f"echo 'Executing core reconciliation for {job_kennung_val} with {entry_nr_val}' && sleep 10",
        # If it's a BigQuery SQL, it could be:
        # sql="CALL `your_project.your_dataset.k_ausd_v_ta_action_assoc_proc`('{{ params.job_kennung }}', '{{ params.entry_nr }}')",
        # use_legacy_sql=False,
        # params={'job_kennung': job_kennung_val, 'entry_nr': entry_nr_val},
    )

    log_success_task = PythonOperator(
        task_id='log_success',
        python_callable=log_success,
        op_kwargs={'job_kennung': job_kennung_val, 'entry_nr': entry_nr_val},
    )

    generate_entry_number_task >> setup_task >> execute_core_reconciliation_task >> log_success_task
```

## 6. External Dependencies
The current script explicitly sources several utility scripts, which can be considered internal dependencies rather than external systems like databases or SFTP.

*   `$HOME/.dw_init`: This is an environment initialization script.
    *   **Replacement:** Configuration management in Cloud Composer (e.g., Airflow Variables, environment variables, or loading configurations from Cloud Storage/Secret Manager).
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   **Replacement:** Airflow's built-in error handling (`on_failure_callback`), Python's `logging` module, and integration with Cloud Logging. Custom error notification logic can be implemented in Python operators.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   **Replacement:** Airflow DAG parameters, `op_kwargs` for Python operators, or Airflow Variables.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility.
    *   **Replacement:** Python's `datetime` module or Airflow's Jinja templating for dates.
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`: The core reconciliation script.
    *   **Replacement:** This script represents the core business logic. Its migration will be a separate but related effort, likely involving conversion to BigQuery SQL, Python, or PySpark, and encapsulated within an Airflow task.

No explicit external systems (Oracle, SFTP, S3, etc.) were identified for this wrapper script. Any such dependencies would reside within the `k_ausd_v_ta_action_assoc.ksh` and need to be addressed during its migration.

## 7. Unresolved / Risks
*   **Core Logic (`k_ausd_v_ta_action_assoc.ksh`)**: The most significant unresolved item is the actual data reconciliation logic contained within `k_ausd_v_ta_action_assoc.ksh`. This script was invoked by the wrapper but its content was not analyzed. A separate analysis and design will be required for this core script to understand its data sources, transformations, and targets, and then migrate it to BigQuery SQL, Python, or PySpark.
*   **Environment Variables**: The specific environment variables set by `$HOME/.dw_init` need to be cataloged and replicated or replaced with appropriate Airflow/GCP configurations.
*   **Error Code Mapping**: The custom error codes (`ErrNr=193`, `ErrNr=192`) and the `DWMSG_MeldeFehler` function imply a custom error reporting mechanism. This needs to be mapped to a standard GCP logging and alerting strategy.
*   **Parameter Usage**: While `getopts` is used, the script currently only checks for `-h`. If `s:` or `l:` parameters are intended for future use or are used by downstream scripts, their meaning must be determined and incorporated into the Airflow DAG's parameter handling.

## 8. Build Plan
1.  **Analyze `k_ausd_v_ta_action_assoc.ksh`**: Perform a detailed analysis of the core reconciliation script to understand its functionality, data sources, and targets.
2.  **Design Core Logic Migration**: Create a migration design for `k_ausd_v_ta_action_assoc.ksh` to BigQuery SQL, Python with BigQuery, or PySpark.
3.  **Create Airflow DAG**: Develop a new Airflow DAG (Python) for `ta_action_assoc_reconciliation_wrapper.py`.
    *   Implement tasks for environment setup, parameter handling, and robust logging/error handling, replacing the shell script utilities.
    *   Integrate the migrated core reconciliation logic as a distinct task within this DAG.
4.  **Configuration Management**: Define and implement required Airflow Variables and/or environment variables in Cloud Composer.
5.  **Logging and Alerting**: Configure Cloud Logging and Monitoring for the new DAG, setting up appropriate alerts for job failures.
6.  **Testing**: Develop and execute unit, integration, and end-to-end tests for the new Airflow DAG and its component tasks.
7.  **Deployment**: Deploy the Airflow DAG to a Cloud Composer environment.

**Files to Generate:**
*   `dags/ta_action_assoc_reconciliation_wrapper.py` (Python Airflow DAG)
*   `dags/k_ausd_v_ta_action_assoc_core_logic.sql` (or `.py` or `.py` for PySpark, depending on core logic migration)
*   Configuration files (e.g., Airflow Variables JSON/YAML)
*   Test scripts (Python `pytest` or similar)