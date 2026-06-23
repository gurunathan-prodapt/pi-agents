# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh

## 1. Purpose & Scope

This KornShell script, `r_ausd_bp_ta_bpr_bcp.ksh`, serves as an orchestration wrapper for the initial provision of selected "Basisprodukte" (base products) for the BERT system. Its primary business purpose is to prepare and launch a core processing script (`k_ausd_bp_ta_bpr_bcp.ksh`) responsible for data extraction and staging related to these base products. The script handles parameter parsing for a key date (Stichtag) and a restart value, sets up logging, and implements error handling before executing the core logic. It ensures that the environment is correctly initialized and that the core process is invoked with the necessary context and parameters.

## 2. Source Inventory

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh`
    *   **Technology**: KornShell
    *   **Complexity Tier**: medium
    *   **Automation Bucket**: semi_auto
    *   **Summary**: This KornShell script orchestrates the initial provision of selected "Basisprodukte" (base products) for the BERT system. It parses parameters for a key date and a restart value, sets up logging and error handling, and then executes a core KornShell script to perform the actual data extraction and staging.

## 3. Target Architecture

The target platform for this migration is Google Cloud Platform, specifically BigQuery for data storage and Cloud Composer (managed Airflow) for orchestration.

*   **Orchestration**: The `r_ausd_bp_ta_bpr_bcp.ksh` script's orchestration logic will be migrated to an Airflow DAG in Cloud Composer. This DAG will define the sequence of tasks, parameter handling, and error management.
*   **Data Processing**: The core logic performed by the invoked `k_ausd_bp_ta_bpr_bcp.ksh` script (which is not part of this specific migration unit but is a critical dependency) is expected to be migrated to BigQuery SQL, PySpark on Dataproc, or another suitable Google Cloud data processing service, depending on its internal logic. For this orchestrator script, the BigQuery target applies to any data it might directly or indirectly interact with via the invoked script.
*   **Logging & Monitoring**: Re-implement custom logging functions (`DWMSG_*`) using Airflow's native logging capabilities, potentially integrating with Cloud Logging.
*   **Environment**: Environment setup (`. $HOME/.dw_init`) will be replaced by Airflow environment variables or configuration management within the DAG.

## 4. Data Flow & Lineage

The current script acts as a wrapper and orchestrator.
1.  **Input Parameters**: The script receives command-line parameters:
    *   `-s DDMMYYYY`: Stichtag (key date).
    *   `-l <value>`: Wiederanlaufwert (restart value).
2.  **Environment Setup**: It sources `$HOME/.dw_init` for environment initialization.
3.  **Utility Loading**: It sources several utility KornShell scripts for error handling (`f_alis_msgerr.ksh`), parameter parsing (`h_alis_parameter.ksh`), and date management (`h_alis_date.ksh`).
4.  **Parameter Processing**:
    *   Parses command-line arguments using `getopts`.
    *   Defaults `p_wiederanlaufWert` to 0 if not provided.
    *   Determines `v_sysdate` using `DWDate_Gib_Zeitraum`.
    *   Defaults `p_stichtag` to `v_sysdate` if not explicitly provided.
    *   Validates parameters using `pruefeParameterGesetzt`.
5.  **Logging & Error Handling Initialization**: Initializes job identification (`JobKennung`), retrieves a job number (`DW_EintragsNr`), sets up log file naming (`LogDatei`), creates an initial log entry, and records `Stichtag` information using `DWMSG_*` functions.
6.  **Core Script Invocation**: The orchestrator invokes the core KornShell script:
    *   `"${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_bcp.ksh"`
    *   Parameters passed to the core script include: `-j $JobKennung`, `-s $p_stichtag`, `-f ${DW_EintragsNr}`, `-l ${p_wiederanlaufWert}`.
    *   All output from the core script is redirected to `$LogDatei`.
7.  **Post-execution Status**: Upon successful completion of the core script, a success message is logged, and the job status is updated using `DWMSG_SetzeStatusOK`.
8.  **Error Handling**: `set -e` ensures the script exits on command failure. `trap` statements are set to call `DWMSG_Fehlerbehandlung` for various signals (INT, STOP, CONT, ERR).

## 5. Transformation Logic

The `r_ausd_bp_ta_bpr_bcp.ksh` script itself is an orchestration and control flow script rather than a data transformation script. Its logic focuses on:

*   **Parameter Management**:
    *   Accepts `-s` (Stichtag, `DDMMYYYY`) and `-l` (Wiederanlaufwert) as input.
    *   `p_wiederanlaufWert` defaults to `0` if not set.
    *   `p_stichtag` defaults to `v_sysdate` (current system date in `DDMMYYYY` format) if not set.
    *   Uses a custom function `pruefeParameterGesetzt` for parameter validation.
*   **Date Calculation**: Uses a custom function `DWDate_Gib_Zeitraum` to retrieve the system date.
*   **Orchestration**: The core function is to construct and execute the command for `k_ausd_bp_ta_bpr_bcp.ksh` with specific runtime parameters.
*   **Logging & Error Handling**: Leverages a custom message/error framework (`DWMSG_*` functions) for logging job status, errors, and success. This includes setting up `trap` handlers to ensure proper error reporting and cleanup.

The actual data transformation logic for "initial provision of selected Basisprodukte" is encapsulated within the `k_ausd_bp_ta_bpr_bcp.ksh` script, which is invoked by this wrapper.

## 6. External Dependencies

*   **Sourced Environment/Utility Scripts**:
    *   `$HOME/.dw_init`: Initializes the shell environment.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Provides custom error handling functions.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Provides custom parameter utility functions (e.g., `pruefeParameterGesetzt`).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Provides custom date utility functions (e.g., `DWDate_Gib_Zeitraum`).
    *   **Replacement Strategy**: These shell utilities and functions will need to be re-implemented in Python, conforming to Airflow's best practices for operators, hooks, and utilities. Common functions can be moved to a shared Python library.
*   **Core Logic Script**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_bcp.ksh`: This is the primary script invoked by the current job and contains the core business logic for data processing.
    *   **Replacement Strategy**: This script will require its own migration analysis. Depending on its content, it could be migrated to BigQuery SQL, PySpark, or another appropriate GCP service. The Airflow DAG for `r_ausd_bp_ta_bpr_bcp.ksh` will then invoke this migrated core logic as a separate Airflow task (e.g., `BigQueryOperator`, `DataprocSubmitJobOperator`).

## 7. Unresolved / Risks

*   **Custom Shell Framework Re-implementation**: A significant effort is required to re-implement the custom `DWMSG_*`, `DWDate_Gib_Zeitraum`, and `pruefeParameterGesetzt` functions in Python. This involves understanding their exact behavior and replicating it within an Airflow-compatible utility structure.
*   **`k_ausd_bp_ta_bpr_bcp.ksh` Migration**: The migration of the invoked core script (`k_ausd_bp_ta_bpr_bcp.ksh`) is a critical upstream dependency. Its complexity and migration path (e.g., to BigQuery SQL, PySpark) will directly influence the design and implementation of this orchestrator DAG. The current analysis does not delve into the internal workings of `k_ausd_bp_ta_bpr_bcp.ksh`.
*   **Environment Variables (`.dw_init`)**: The contents and purpose of `$HOME/.dw_init` need to be fully understood and translated into Airflow environment configurations or Python-based environment setup.
*   **Error Handling Fidelity**: The `set -e` and `trap` mechanisms in KornShell provide specific error handling behavior. Replicating this precisely within Airflow's task failure and retry mechanisms requires careful design.

## 8. Build Plan

The migration will involve creating a new Airflow DAG in Python that encapsulates the orchestration logic of `r_ausd_bp_ta_bpr_bcp.ksh`.

1.  **Analyze and Migrate Shared Utilities (Python)**:
    *   Deconstruct `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` to identify core functions (e.g., `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, `DWMSG_*`).
    *   Re-implement these functions as a shared Python library or Airflow utility functions/operators.
    *   Define Airflow connections and variables to replace environment settings from `$HOME/.dw_init`.
2.  **Analyze and Migrate Core Logic Script `k_ausd_bp_ta_bpr_bcp.ksh`**:
    *   Perform a detailed analysis of `k_ausd_bp_ta_bpr_bcp.ksh` to determine its migration path (e.g., to BigQuery SQL, PySpark, or another Python script).
    *   Develop the migrated code for `k_ausd_bp_ta_bpr_bcp.ksh` as a new BigQuery SQL script, PySpark job, or Python script.
3.  **Develop Airflow DAG for `r_ausd_bp_ta_bpr_bcp.ksh` (Python)**:
    *   Create a new Python file representing the Airflow DAG for this job.
    *   **Parameter Handling**: Implement parsing of `Stichtag` and `Wiederanlaufwert` using Airflow's `params` or external configuration.
    *   **Task 1: Environment Setup/Initialization**: A PythonOperator to initialize any necessary environment variables or configurations, potentially calling the migrated utility functions.
    *   **Task 2: Parameter Validation**: A PythonOperator to perform parameter validation using the migrated `pruefeParameterGesetzt` logic.
    *   **Task 3: Invoke Core Logic**: An appropriate Airflow operator to execute the *migrated* `k_ausd_bp_ta_bpr_bcp.ksh` logic (e.g., `BigQueryOperator`, `DataprocSubmitJobOperator`, `BashOperator` if migrated to a shell script).
    *   **Logging and Status**: Integrate the migrated `DWMSG_*` functions into PythonOperators or use Airflow's native logging. Use Airflow's task success/failure mechanisms to replace custom `DWMSG_SetzeStatusOK` and error traps.
    *   **Error Handling**: Leverage Airflow's retry mechanisms and error callbacks for robust error handling.

**Build Output Language**: Python (for Airflow DAG and utility functions), BigQuery SQL (for transformed data logic within `k_ausd_bp_ta_bpr_bcp.ksh` if applicable).