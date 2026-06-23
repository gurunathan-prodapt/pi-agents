# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh

## 1. Purpose & Scope

This job, `r_ausd_v_ta_cntrct_valid.ksh`, is a KornShell wrapper script designed to orchestrate the processing and validation of contract data for the `ta_cntrct_valid` table. Its primary function is to set up the environment, handle command-line parameters, initialize custom logging and error handling mechanisms, and then invoke a core business logic script, `k_ausd_v_ta_cntrct_valid.ksh`, with specific parameters. It concludes by reporting the overall status of the operation. This job is part of a larger data warehousing process for contract data reconciliation.

## 2. Source Inventory

The job consists of one primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh`
*   **Technology:** KornShell Script
*   **Complexity Tier:** Not available (information not found in `file_complexity` table). Based on the analysis, it's an orchestration script.
*   **Automation Bucket:** `semi_auto`

## 3. Target Architecture

The target platform for this migration is Google Cloud's BigQuery. Given that `r_ausd_v_ta_cntrct_valid.ksh` is an orchestration shell script, its functionality will be migrated to a Python-based Airflow Directed Acyclic Graph (DAG).

*   **Orchestration:** The KornShell wrapper script will be replaced by an **Airflow DAG written in Python**. This DAG will define the sequence of tasks, including environment setup, parameter handling, execution of the core logic, and status reporting.
*   **Core Logic:** The logic encapsulated within `k_ausd_v_ta_cntrct_valid.ksh` (which is invoked by the current script) will need to be analyzed separately. It is anticipated that this core logic, if primarily SQL-based, will be converted to **BigQuery SQL**. If it involves complex procedural logic or data transformations, it might be migrated to **Python scripts utilizing the BigQuery client library** or potentially **PySpark jobs** running on Dataproc.
*   **Logging & Error Handling:** The custom `DWMSG_` functions in the original script will be replaced by **Airflow's native logging and error handling mechanisms**, providing centralized monitoring and alerts.
*   **Environment Management:** Environment initialization (`. $HOME/.dw_init`) will be managed either through **Airflow environment configurations** or by incorporating necessary environment variables directly into the Python DAG or its invoked tasks.

## 4. Data Flow & Lineage

The data flow for this job is primarily orchestrated by the `r_ausd_v_ta_cntrct_valid.ksh` script, which acts as a controlling wrapper.

1.  **Initialization:** The script starts by sourcing several utility KornShell scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) to set up the execution environment, error handling, parameter parsing, and date functions.
2.  **Parameter Processing:** It parses command-line arguments using `getopts`.
3.  **Logging Setup:** It initializes a custom logging framework using `DWMSG_` functions, determining a job entry number, log file name, and writing initial job information to the log.
4.  **Core Logic Invocation:** The script's central action is to invoke another KornShell script: `k_ausd_v_ta_cntrct_valid.ksh`. This invocation passes a job identifier (`-j $JobKennung`) and an entry number (`-f ${DW_EintragsNr}`). The standard output and error streams of this core script are redirected to the log file.
5.  **Status Reporting:** Upon completion of `k_ausd_v_ta_cntrct_valid.ksh`, the wrapper script reports a success message to both standard output and the log file, and updates the job status using `DWMSG_SetzeStatusOK`.
6.  **Exit:** The script exits with a status code (0 for success, non-zero for errors).

The actual data transformations and interactions with tables for `ta_cntrct_valid` are expected to occur within the `k_ausd_v_ta_cntrct_valid.ksh` script, which is currently an unanalyzed component.

## 5. Transformation Logic

The migration will transform the KornShell orchestration script into a Python Airflow DAG.

**Original KornShell Logic (`r_ausd_v_ta_cntrct_valid.ksh`):**

*   **Environment Sourcing:**
    *   `. $HOME/.dw_init`: Initializes shell environment variables.
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Integrates a custom error messaging framework.
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Provides custom parameter handling utilities.
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Offers custom date utility functions.
*   **Parameter Parsing:** Uses `getopts` to process command-line arguments like `-h` (help) and other generic parameters (`s:l:`).
*   **Error Handling:**
    *   `set -eu`: Ensures the script exits on errors or undefined variables.
    *   `trap` statements for `INT` (interrupt) and `ERR` (command error) signals, calling `DWMSG_Fehlerbehandlung` and custom exit logic.
    *   Checks `ErrNr` after `getopts` to handle parameter errors.
*   **Logging Initialization:**
    *   Sets `JobKennung` (BERT_V_TA_CNTRCT_VALID) and `v_sysdate`.
    *   Calls `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo` to set up logging and register the job.
*   **Core Script Execution:** Executes `"${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh"` passing `-j $JobKennung -f ${DW_EintragsNr}` and redirects `stdout`/`stderr` to `$LogDatei`.
*   **Success Reporting:** Calls `DWMSG_SetzeStatusOK` and prints a success message (`Die Abarbeitung wurde ohne erkennbare Fehler beendet`).

**Target Airflow DAG Logic (Python):**

*   **DAG Definition:** A Python file defining the Airflow DAG with appropriate scheduling, retry policies, and tags.
*   **Environment Management:**
    *   `$HOME/.dw_init`: Environment variables will be set as Airflow variables, connections, or within task definitions.
    *   Utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`): Their functionalities will be re-implemented as Python functions or custom Airflow operators, or replaced by native Airflow/Python features where applicable.
*   **Parameter Handling:** Airflow DAG parameters can be used to pass inputs to the DAG.
*   **Error Handling:** Airflow's built-in error handling (e.g., `on_failure_callback`, retries) will replace the shell's `set -eu` and `trap` mechanisms. Specific error conditions from downstream tasks will be propagated and handled by Airflow.
*   **Logging:** Airflow's robust logging system will replace the custom `DWMSG_` functions, providing centralized logs accessible via the Airflow UI.
*   **Core Logic Execution:**
    *   The `k_ausd_v_ta_cntrct_valid.ksh` script, once migrated, will be executed as an Airflow task.
    *   If `k_ausd_v_ta_cntrct_valid.ksh` is converted to BigQuery SQL, a `BigQueryOperator` (or a Python function wrapped in a `PythonOperator` that executes BigQuery SQL) will be used.
    *   If `k_ausd_v_ta_cntrct_valid.ksh` is converted to a Python script, a `PythonOperator` will execute it.
    *   Parameters (`JobKennung`, `DW_EintragsNr`) will be passed as Jinja templates or task arguments.
*   **Status Reporting:** Airflow automatically tracks task status. A final `PythonOperator` can be used to log custom success messages or trigger downstream notifications if needed.

## 6. External Dependencies

The initial analysis (`lineage_assembled_jobs.external_systems`) showed no direct external system dependencies for this specific job. However, the script has several internal dependencies on other shell scripts and conventions:

*   **Utility Scripts (Internal):**
    *   `$HOME/.dw_init`: This script likely initializes common environment variables, paths, and configurations. It needs to be translated into Airflow environment variables, Python configurations, or Airflow connections.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: A custom error messaging utility. Its functionality (error logging, standardized messages) needs to be re-implemented in Python using standard logging libraries and integrated into the Airflow DAG's error handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: A custom parameter handling utility. Its functions need to be replicated or replaced by Airflow's parameter passing mechanisms.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: A custom date utility. Its functions need to be replicated in Python or replaced by Python's `datetime` module.
*   **Core Logic Script:**
    *   `k_ausd_v_ta_cntrct_valid.ksh`: This is the primary business logic component invoked by the wrapper. Its specific content and external dependencies (e.g., databases, files) are currently unknown and will require separate, detailed analysis.

## 7. Unresolved / Risks

*   **Core Logic Analysis (High Risk):** The most significant unresolved item is the content and dependencies of `k_ausd_v_ta_cntrct_valid.ksh`. Without analyzing this script, the full data flow, transformation logic, and potential external system interactions remain unknown. This script is crucial for determining the actual BigQuery migration strategy (e.g., pure SQL, Python with BigQuery API, PySpark).
*   **Custom DWMSG_ Functions (Medium Risk):** The `DWMSG_` functions for logging and error handling are custom to the legacy environment. Their exact implementation details and required output formats (e.g., for downstream monitoring systems) need to be fully understood to ensure accurate replication in the Airflow/Python environment.
*   **Environment Initialization (`.dw_init`) (Medium Risk):** The contents of `$HOME/.dw_init` are unknown. This script might set critical environment variables or paths that are essential for the job's execution. These need to be identified and accurately configured in the new Airflow environment.
*   **Complexity Tier (Minor Risk):** The `file_complexity` table did not return any rows for the script, meaning its complexity tier is not explicitly known from automated analysis. While the `rev_eng_design` tool provided a good breakdown, a formal complexity assessment might be beneficial for resource planning.

## 8. Build Plan

1.  **Analyze `k_ausd_v_ta_cntrct_valid.ksh` (Priority 1):**
    *   Read the source code of `k_ausd_v_ta_cntrct_valid.ksh`.
    *   Perform static analysis to identify its type (SQL, another shell script, procedural code), database interactions (tables read/written), and any other external dependencies.
    *   Generate a migration design document for `k_ausd_v_ta_cntrct_valid.ksh`.
2.  **Design Core Logic Migration (Priority 2):**
    *   Based on the analysis of `k_ausd_v_ta_cntrct_valid.ksh`, design its specific migration path to BigQuery SQL, Python (BigQuery client), or PySpark.
3.  **Develop Airflow DAG (Python):**
    *   Create a new Python file for the Airflow DAG.
    *   Define the DAG structure, scheduling, and task dependencies.
    *   Implement tasks for environment setup, replacing the `.dw_init` functionality.
    *   Integrate the migrated core logic from `k_ausd_v_ta_cntrct_valid.ksh` as an appropriate Airflow operator (e.g., `BigQueryOperator`, `PythonOperator`).
    *   Implement Airflow-native logging and error handling to replace custom `DWMSG_` functions.
    *   Translate `getopts` parameter parsing into Airflow DAG parameters or task arguments.
4.  **Re-implement Utility Functions (Python):**
    *   Convert the functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` into reusable Python modules or Airflow helper functions.
5.  **Testing (Python):**
    *   Develop unit tests for individual Python components and functions.
    *   Develop integration tests for the entire Airflow DAG, ensuring correct execution flow, data processing, logging, and error handling.
    *   Perform data validation to ensure the migrated process produces identical or functionally equivalent results to the legacy job.