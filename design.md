# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh

## 1. Purpose & Scope

This document details the migration design for the KornShell script `r_ausd_bp_ta_bpr_basis.ksh`.
The script serves as an orchestration wrapper for the initial provisioning of selected basic products (e.g., FAX, Data24) for BERT. Its primary function is to prepare parameters, establish an execution environment, handle logging and error management, and then invoke a core processing script, `k_ausd_bp_ta_bpr_basis.ksh`, with the resolved parameters.

The key functionalities include:
*   Parsing command-line arguments for a cutoff date (`Stichtag`) and a restart value (`Wiederanlaufwert`).
*   Defaulting parameters if not explicitly provided.
*   Sourcing environment variables and utility functions for error handling, parameter parsing, and date manipulation.
*   Initializing and managing a structured logging mechanism.
*   Executing the main data processing logic contained within `k_ausd_bp_ta_bpr_basis.ksh`.
*   Implementing robust error trapping and reporting.

The scope of this migration design is specifically for `r_ausd_bp_ta_bpr_basis.ksh` and its direct dependencies, aiming to transform its orchestration logic into a BigQuery-compatible target environment, leveraging Cloud Composer (Airflow) and Python. The migration of the actual business logic within `k_ausd_bp_ta_bpr_basis.ksh` is a separate, dependent task not covered in detail here.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh`
    *   **Technology:** KornShell (KSH)
    *   **Category:** Shell, Orchestration Script
    *   **Tool (Detected):** KornShell
    *   **Summary:** Orchestrates the initial provision of selected basic products for BERT by preparing parameters and executing a core processing script.
    *   **Complexity Tier:** Undetermined (data not available in `file_complexity` table). Inferred as `medium` due to parameter parsing, external script invocation, and custom error/logging framework.
    *   **Migration Flags:** Undetermined (data not available in `file_complexity` table). Inferred to include flags for "shell script to Python conversion", "logging framework migration", "parameter handling", and "external dependency management".
    *   **Automation Bucket:** Undetermined (data not available in `automation_rate` table). Inferred as `B2` (semi-automated) due to the need for custom Python implementation for legacy utility functions and orchestration within Airflow.
    *   **Estimated Effort:** Undetermined (data not available in `file_complexity` table).

## 3. Target Architecture

The migrated job will run on Google Cloud Platform, utilizing:
*   **Google Cloud Composer (Apache Airflow):** To schedule and orchestrate the Python-based workflow. The wrapper script will be translated into an Airflow DAG written in Python.
*   **Python:** The core logic of the shell orchestrator will be rewritten in Python, using standard libraries for argument parsing, date handling, and process invocation. Custom utility functions (like `DWMSG_*` and `DWDate_Gib_Zeitraum`) will be replaced with Python equivalents or dedicated modules.
*   **BigQuery:** While this wrapper script itself does not directly perform SQL operations, the core script it invokes (`k_ausd_bp_ta_bpr_basis.ksh`) is expected to perform data processing. This processing logic will ultimately be migrated to BigQuery SQL, potentially as BigQuery Scripting, Stored Procedures, or external tasks within the Airflow DAG.
*   **Cloud Logging:** For centralized log management, replacing the legacy file-based logging.

The overall architecture will involve an Airflow DAG, where individual tasks represent the logical steps of the original KornShell script, including parameter processing, logging setup, and the invocation of the core data processing task.

## 4. Data Flow & Lineage

The data flow for this orchestrator is primarily control flow and parameter passing.
*   **Inputs:**
    *   Command-line parameters: `-s` (Stichtag, DDMMYYYY), `-l` (Wiederanlaufwert).
    *   Environment variables/sourced files: `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`.
    *   System Date: Obtained via `DWDate_Gib_Zeitraum`.
    *   Implied Source Table (from comments and `file_analysis`): `DWH$TA_C_VERTRAG` (DWH Contract Cache).
*   **Processing:**
    1.  Initialization of environment variables and error handling mechanisms.
    2.  Parsing of input parameters using `getopts`.
    3.  Validation and defaulting of parameters (e.g., `p_wiederanlaufWert` to 0, `p_stichtag` to system date).
    4.  Generation of job-specific logging details (`LogDatei`, `DW_EintragsNr`).
    5.  Setting up `trap` handlers for error management.
    6.  Execution of the core processing script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh` with parameters (`-j`, `-s`, `-f`, `-l`).
*   **Outputs:**
    *   Log file output: `$LogDatei` (redirected stdout/stderr).
    *   Exit status: 0 for success, non-zero for errors.
    *   Implied Target Table (from comments and `file_analysis`): "FOS-Tabelle" (Forderungsscoring).

Explicit lineage edges were not found in the `lineage_edges` table for this script, but the above flow is derived from the script's content and `file_analysis`.

## 5. Transformation Logic

The KornShell script's logic will be transformed into Python.

### a. Parameter Parsing and Defaulting

*   **Legacy:** `getopts` for `-h`, `-s`, `-l`. Defaulting `p_wiederanlaufWert` to 0 and `p_stichtag` to `v_sysdate` if not provided.
*   **Target:** Use Python's `argparse` module for robust command-line argument parsing. Implement Python functions to handle the defaulting logic. Date handling will use Python's `datetime` module, replacing `DWDate_Gib_Zeitraum`.

### b. Environment Setup and Utility Sourcing

*   **Legacy:** Sourcing `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, etc. These provide environment variables and shared functions.
*   **Target:** Environment variables (`BERT_DIR_ROOT`, etc.) will be managed as Airflow variables or passed via configuration. The functionalities of the sourced utility scripts (error handling, logging, date utilities) will be reimplemented as Python modules or integrated into a common utility library accessible by Airflow tasks.

### c. Logging and Error Handling

*   **Legacy:** Custom `DWMSG_*` functions for logging job start, stichtag info, errors, and success status, redirected to `$LogDatei`. `set -e` and `trap` for error management.
*   **Target:** Use Python's `logging` module configured to output to Cloud Logging. Custom `DWMSG_*` functions will be replaced with standard Python logging calls or wrapped within a custom Python logging utility. Airflow's native error handling (`on_failure_callback`, `retries`) will replace shell `trap` mechanisms. The `DW_EintragsNr` and `JobKennung` will become Airflow task instance context or custom metadata.

### d. Core Script Invocation

*   **Legacy:** `${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`
*   **Target:** This will likely become a separate Airflow task.
    *   If `k_ausd_bp_ta_bpr_basis.ksh` is migrated to a BigQuery SQL script, the Python orchestrator will use a `BigQueryOperator` to execute it, passing parameters as `sql_params`.
    *   If `k_ausd_bp_ta_bpr_basis.ksh` is migrated to a Python script, the orchestrator will invoke it as a Python callable (`PythonOperator`) or an external Python script (`BashOperator` running `python script.py`).
    *   The parameters (`-j`, `-s`, `-f`, `-l`) will be passed as arguments to the downstream task.

### Pseudocode (Horizon Python / Airflow Task)

```python
import os
import sys
import argparse
from datetime import datetime

# Assuming a custom logging and utility module for migration context
from common.utils import (
    get_system_date_ddmmyyyy,
    get_next_job_entry_number,
    build_log_filename,
    create_log_entry,
    set_stichtag_info,
    log_error_and_exit,
    set_status_ok,
    append_to_log
)
# For Airflow specific operations
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG

# --- Constants / Metadata ---
PROG_NAME = "Bereitstellung Basisprodukte BERT"
PROG_VERSION = "V2.0.0"
JOB_KENNUNG = "ausd_bp_ta_bpr_basis"

# --- Usage Function (for help message) ---
def print_usage():
    """Prints the usage information for the script."""
    print(f"""
    Programm: {PROG_NAME}
    Version:  {PROG_VERSION}
    Aufruf:   python_orchestrator.py [OPTIONS]

    Parameter:
        -h, --help    shows this page
        -s STICHTAG   Cutoff date (Stichtag) in DDMMYYYY format
        -l WIEDERANLAUFWERT Restart value.
                      If set, only contracts with DWH_VERTRAG_ID > Wiederanlaufwert
                      are processed. Entries >= Wiederanlaufwert may be deleted prior.

    Beschreibung:
        This job generates a cutoff-date snapshot of the contract cache in the DWH
        and makes it available for Forderungsscoring. It handles cases where
        tables might need to be deleted if no active contract cache is pending pickup.
        Records are selected based on Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag.
        If Stichtag is not set, it defaults to MIN(current_system_date, max_load_date).
    """)

# --- Main Orchestration Logic ---
def run_orchestration(stichtag: str = None, wiederanlaufwert: int = None, **kwargs):
    """
    Main function to execute the orchestration logic.
    Assumes arguments are passed by Airflow or directly.
    """
    # Default restart value
    if wiederanlaufwert is None:
        wiederanlaufwert = 0

    # Get system date in DDMMYYYY
    v_sysdate = get_system_date_ddmmyyyy()

    # Default stichtag if not provided
    if stichtag is None:
        stichtag = v_sysdate

    # Validate required parameter (assuming get_system_date_ddmmyyyy always provides a value)
    if stichtag is None or not stichtag.strip():
        log_error_and_exit("Stichtag is required but could not be determined.", usage=True)

    # Initialize job logging / job number
    dw_eintragsnr = get_next_job_entry_number()
    log_datei = build_log_filename(JOB_KENNUNG, dw_eintragsnr)
    create_log_entry(dw_eintragsnr, JOB_KENNUNG, os.path.basename(__file__), log_datei)
    set_stichtag_info(dw_eintragsnr, v_sysdate, "DDMMYYYY")

    # In a real Airflow DAG, error handling is done by Airflow's retry/alert mechanisms
    # and not explicit trap handlers within the Python code itself.
    # The 'log_error_and_exit' would typically raise an exception to fail the Airflow task.

    # Print job metadata (to Cloud Logging via Airflow's logger)
    print(" ----------------- Job -----------------------")
    print(f" Job-Nr    : '{dw_eintragsnr}'")
    print(f" JobKennung: '{JOB_KENNUNG}'")
    print(f" Logdatei  : '{log_datei}'")
    print(f" Stichtag  : '{stichtag}'")
    print(" ---------------------------------------------")

    # Construct the command/parameters for the core script
    # This assumes k_ausd_bp_ta_bpr_basis.ksh is also migrated to a Python module
    # or a BigQuery task, and this orchestrator triggers it.
    bert_dir_root = os.getenv("BERT_DIR_ROOT", "/path/to/bert/root") # Get from Airflow Variable or Env
    core_script_command = (
        f"python {bert_dir_root}/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.py "
        f"-j {JOB_KENNUNG} -s {stichtag} -f {dw_eintragsnr} -l {wiederanlaufwert}"
    )
    # Alternatively, if k_ausd_bp_ta_bpr_basis is a BQ SQL script:
    # core_script_command = (
    #     f"bq query --use_legacy_sql=false "
    #     f"'CALL your_project.your_dataset.k_ausd_bp_ta_bpr_basis_sp("
    #     f"job_kennung=\"{JOB_KENNUNG}\", stichtag=\"{stichtag}\", "
    #     f"eintragsnr={dw_eintragsnr}, wiederanlaufwert={wiederanlaufwert})'"
    # )

    # Execute the core processing script (represented by a BashOperator for a shell/python script)
    # Or BigQueryOperator for BQSQL.
    # In Airflow, this would be a separate task. For direct Python, use subprocess.run.
    print(f"Invoking core script: {core_script_command}")
    # Example for Airflow:
    # task_run_core_script = BashOperator(
    #     task_id='run_core_processing',
    #     bash_command=core_script_command,
    #     dag=dag,
    # )
    # For a direct Python script, simulate execution
    # import subprocess
    # result = subprocess.run(core_script_command, shell=True, capture_output=True, text=True)
    # if result.returncode != 0:
    #     log_error_and_exit(f"Core script failed: {result.stderr}")


    # Success handling
    success_message = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
    print(success_message)
    append_to_log(log_datei, success_message) # Still log to legacy, or switch completely to Cloud Logging
    set_status_ok(dw_eintragsnr, log_datei) # Update status in legacy system if needed

    print("Orchestration completed successfully.")
    return 0

# --- Airflow DAG Definition Example ---
# with DAG(
#     dag_id='r_ausd_bp_ta_bpr_basis_dag',
#     start_date=days_ago(1),
#     schedule_interval=None, # Define schedule as per requirement
#     catchup=False,
#     tags=['bert', 'orchestration'],
#     default_args={
#         'owner': 'airflow',
#         'depends_on_past': False,
#         'email_on_failure': False,
#         'email_on_retry': False,
#         'retries': 1,
#         'retry_delay': timedelta(minutes=5),
#     },
# ) as dag:
#     parse_and_run_task = PythonOperator(
#         task_id='parse_and_run_orchestration',
#         python_callable=run_orchestration,
#         op_kwargs={
#             'stichtag': "{{ ds_nodash }}", # Example: pass execution date as stichtag
#             'wiederanlaufwert': None
#         },
#     )
#     # If k_ausd_bp_ta_bpr_basis.ksh is also a DAG or Python task, it would be called here.
#     # e.g., using TaskGroup or ExternalTaskSensor

```

## 6. External Dependencies

The original script has several external dependencies:

*   **Sourced KornShell Utility Scripts:**
    *   `. $HOME/.dw_init`: Initializes environment.
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities, including `DWDate_Gib_Zeitraum`.
    *   `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`: These are functions provided by the sourced scripts.

    **Replacement Strategy:** These will be replaced by Python modules or standard Python library functions. Environment initialization can be handled by Airflow environment variables or a Python configuration module. Custom logging and error handling functions will be rewritten in Python and integrated with Cloud Logging and Airflow's native error handling capabilities. Date utilities will use Python's `datetime` module.

*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh`: This is the main business logic script.

    **Replacement Strategy:** This script's migration is a critical dependency. It will need to be analyzed and migrated separately, likely to BigQuery SQL (via BigQuery Scripting/Stored Procedures) or a PySpark/Python script for data transformation. The migrated `r_ausd_bp_ta_bpr_basis.ksh` (as an Airflow DAG) will invoke this migrated component as a distinct Airflow task (e.g., `BigQueryOperator`, `DataprocSparkOperator`, or `PythonOperator`).

*   **Implied Database Access:**
    *   `DWH$TA_C_VERTRAG`: Implied source table for contract data.
    *   "FOS-Tabelle": Implied target table for output.

    **Replacement Strategy:** These will be migrated to BigQuery tables. The data processing in the core script (`k_ausd_bp_ta_bpr_basis.ksh`) will interact with these BigQuery tables.

## 7. Unresolved / Risks

*   **Core Logic in `k_ausd_bp_ta_bpr_basis.ksh`:** The most significant unresolved item is the actual data transformation and loading logic residing in `k_ausd_bp_ta_bpr_basis.ksh`. This script was not part of the `component_files` for this assembled job. Its content and migration strategy (e.g., to BigQuery SQL, PySpark) must be determined, and it will heavily influence the specific Airflow operator used in the orchestrator.
*   **Missing Complexity and Automation Rate Data:** The `file_complexity` and `automation_rate` tables did not return rows for this file. This leads to reliance on inferred complexity and automation buckets, which might require further manual review.
*   **Detailed Functionality of Sourced Utilities:** While the general purpose of sourced utility scripts is understood, the exact implementation details of functions like `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, and `DWMSG_*` are not fully known without their source code. A detailed analysis or recreation of their behavior in Python will be required.
*   **Data Volume and Performance:** The current design assumes a straight rewrite. Performance considerations for large data volumes handled by the invoked `k_ausd_bp_ta_bpr_basis.ksh` will need to be addressed in its specific migration design.
*   **Restart/Recovery Logic:** The restart value (`-l`) and its handling (deletion of entries `Wiederanlaufwert`) needs to be carefully reimplemented to ensure data consistency in BigQuery.

## 8. Build Plan

The build plan focuses on transforming `r_ausd_bp_ta_bpr_basis.ksh` into an Airflow DAG using Python.

1.  **Define Airflow DAG Structure (Python):**
    *   Create a Python file (e.g., `r_ausd_bp_ta_bpr_basis_dag.py`) to define the Airflow DAG.
    *   Set DAG parameters: `dag_id`, `start_date`, `schedule_interval`, `default_args`.

2.  **Implement Python Orchestration Logic:**
    *   **Argument Parsing:** Implement a Python function (`parse_arguments`) using `argparse` to replicate the `getopts` behavior for `-s` and `-l`.
    *   **Date Handling:** Develop a Python function `get_system_date_ddmmyyyy` to get the current date in DDMMYYYY format, replacing `DWDate_Gib_Zeitraum`.
    *   **Parameter Defaulting:** Implement logic to default `wiederanlaufwert` to 0 and `stichtag` to the system date if not provided.
    *   **Logging and Error Handling Utilities:** Create a `common.utils` Python module (or similar) containing Python equivalents for `DWMSG_*` functions for structured logging (to Cloud Logging) and error handling. This includes `get_next_job_entry_number`, `build_log_filename`, `create_log_entry`, `set_stichtag_info`, `log_error_and_exit`, `set_status_ok`, `append_to_log`.
    *   **Environment Variables:** Map `BERT_DIR_ROOT` and other necessary environment variables to Airflow Variables.

3.  **Define Airflow Tasks:**
    *   **Parameter Processing Task:** A `PythonOperator` to execute the Python orchestration logic (`run_orchestration` function outlined in Section 5) including argument parsing, defaulting, and logging setup.
    *   **Core Processing Task (Placeholder/Dependent):** This task will invoke the migrated `k_ausd_bp_ta_bpr_basis.ksh` logic.
        *   If `k_ausd_bp_ta_bpr_basis.ksh` migrates to BigQuery SQL, use a `BigQueryOperator` or `BigQueryExecuteQueryOperator`.
        *   If `k_ausd_bp_ta_bpr_basis.ksh` migrates to a Python/PySpark script, use a `PythonOperator` or `DataprocSparkOperator`.
        *   Ensure parameters are correctly passed from the orchestrator task to this core task.
    *   **Success/Finalization Task:** A `PythonOperator` or similar to log the successful completion and update any legacy status systems if required.

4.  **Dependencies and Ordering:**
    *   Establish task dependencies in the Airflow DAG (e.g., Parameter Processing -> Core Processing -> Finalization).

5.  **Testing:**
    *   Develop unit tests for the Python utility functions.
    *   Perform integration testing of the Airflow DAG on Cloud Composer, verifying parameter passing, logging, error handling, and successful invocation of the core processing task (even if it's a mock initially).

**Generated Files:**
*   `dags/r_ausd_bp_ta_bpr_basis_dag.py` (Python Airflow DAG)
*   `dags/common/utils.py` (Python module for shared utilities)
*   (Dependent) `dags/k_ausd_bp_ta_bpr_basis.py` (Python script) OR `sql/k_ausd_bp_ta_bpr_basis.sql` (BigQuery SQL script/SP)