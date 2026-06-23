# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_vvl_dwh.ksh`. The script serves as an orchestration wrapper for a contract data reconciliation job targeting the `ta_vvl_dwh` table. Its primary functions include environment initialization, command-line parameter parsing, setup of logging and error handling, and the invocation of a core processing script, `k_ausd_v_ta_vvl_dwh.ksh`. The scope of this design focuses on migrating the wrapper's functionality to Google Cloud's BigQuery platform, leveraging BigQuery Stored Procedures and potentially Cloud Composer for external orchestration. The core business logic contained within `k_ausd_v_ta_vvl_dwh.ksh` is identified as a critical dependency requiring separate, detailed analysis.

## 2. Source Inventory
The primary source file for this job is `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh`.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** Orchestration, parameter handling, logging, error trapping, and invocation of the core processing script.
    *   **Content Summary:** This script defines program metadata, a usage function, sources common utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`), parses command-line arguments, initializes job-specific logging (`DWMSG_*` utilities), sets up `trap` handlers, and finally executes `k_ausd_v_ta_vvl_dwh.ksh` with derived parameters. It then logs the job's completion status.

## 3. Target Architecture
The target architecture will leverage BigQuery for data processing and storage, with external orchestration for the migrated wrapper logic.

*   **BigQuery Stored Procedure:** The orchestration logic of `r_ausd_v_ta_vvl_dwh.ksh` will be converted into a BigQuery Stored Procedure, e.g., `project.dataset.vertragsdatenabgleich`. This procedure will handle parameter validation, job metadata generation, and logging.
*   **BigQuery Logging/Control Tables:** Dedicated BigQuery tables will replace the file-based logging and job control mechanisms. Examples include:
    *   `project.dataset.job_control`: To store job entry numbers, status, script names, log names, and timestamps.
    *   `project.dataset.job_messages`: To store detailed job messages (INFO, ERROR).
    *   `project.dataset.job_error_log`: To store specific error details.
    *   `project.dataset.job_audit`: For final job audit records.
*   **External Orchestration (Cloud Composer/Workflows):** To manage the execution flow, especially the invocation of the translated core processing logic (from `k_ausd_v_ta_vvl_dwh.ksh`), a Cloud Composer DAG or Google Cloud Workflows will be used. This will handle scheduling and triggering the BigQuery Stored Procedures and any other necessary components.
*   **Core Logic:** The business logic from `k_ausd_v_ta_vvl_dwh.ksh` (once analyzed) will likely be migrated into one or more BigQuery Stored Procedures, SQL scripts, or potentially a PySpark job if complex transformations are involved, which would then be invoked by the `vertragsdatenabgleich` stored procedure.

## 4. Data Flow & Lineage
The migration will transform the shell script's sequential execution into a BigQuery-centric data flow:

1.  **Invocation:** The `project.dataset.vertragsdatenabgleich` BigQuery Stored Procedure is invoked, likely by an external orchestrator like Cloud Composer, passing necessary parameters (e.g., `-s`, `-l`).
2.  **Parameter Handling:** The stored procedure validates input parameters. Invalid parameters will lead to error logging and termination.
3.  **Job Initialization:**
    *   A unique job entry number (`DW_EintragsNr`) is allocated, possibly via a sequence or by querying the `job_control` table.
    *   Job metadata (JobKennung, `v_sysdate`, `LogDatei` identifier) is generated.
    *   An initial "RUNNING" entry is recorded in the `job_control` table.
4.  **Core Logic Execution:** The stored procedure then calls the migrated core processing logic. In the provided pseudocode, this is represented by `CALL project.dataset.k_ausd_v_ta_vvl_dwh(JobKennung, DW_EintragsNr);`.
5.  **Status and Logging:**
    *   Upon successful completion of the core logic, a success message is logged in `job_messages`, and the `job_control` table is updated with an "OK" status.
    *   Error handling is managed via `BEGIN...EXCEPTION...END` blocks within BigQuery. Any errors during core logic execution will trigger an update to "ERROR" status in `job_control` and an error message in `job_messages`.
6.  **Final Audit:** A final audit entry is inserted into the `job_audit` table.

**Note:** The lineage details for the core business logic of `k_ausd_v_ta_vvl_dwh.ksh` are not available from the current analysis and will need to be established upon its migration.

## 5. Transformation Logic
The transformation logic converts shell script constructs into BigQuery SQL equivalents:

*   **Shell Script Orchestration:** The overall control flow will be refactored into a BigQuery Stored Procedure.
*   **Parameter Parsing (`getopts`):** Command-line parameters will be translated into input parameters for the BigQuery Stored Procedure. Validation logic will use standard SQL conditional statements.
*   **Environment Sourcing (`. $HOME/.dw_init`):** Environment variables will be replaced by BigQuery procedure variables, configuration tables, or potentially external configuration management solutions.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These will be replaced by:
    *   Custom BigQuery functions or helper stored procedures for logging and date manipulation.
    *   Direct SQL functionality (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`).
*   **Logging (`DWMSG_*` utilities):** The `DWMSG_*` functions will be replaced by `INSERT` or `UPDATE` statements into the designated BigQuery logging/control tables.
*   **Error Handling (`set -eu`, `trap`):** BigQuery's `BEGIN...EXCEPTION...END` blocks will handle runtime errors. Specific error codes (`ErrNr`, `ErrArg`) will be mapped to BigQuery's `SIGNAL SQLSTATE` or custom error messages logged to the `job_error_log` table.
*   **Date Operations (`date +%d%m%Y`):** Will be replaced by BigQuery functions like `CURRENT_DATE()` and `FORMAT_DATE()`.
*   **Core Script Invocation (`${Name_Kernskript}`):** The invocation of `k_ausd_v_ta_vvl_dwh.ksh` will be translated into a `CALL` statement to a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_vvl_dwh`) once that script is migrated.

## 6. External Dependencies
The original script has several external dependencies:

*   **`$HOME/.dw_init`:** This environment initialization file will need to be analyzed to identify critical environment variables or configurations that must be translated into BigQuery stored procedure parameters, BigQuery configuration tables, or injected via the orchestration layer (e.g., Cloud Composer environment variables).
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`:** A utility for error messaging. This will be replaced by BigQuery `INSERT` statements into logging tables and potentially custom BigQuery error handling functions.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`:** A utility for parameter handling. Its relevant functionalities will be integrated into the BigQuery Stored Procedure's parameter validation logic.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`:** A utility for date handling. This will be replaced by native BigQuery date functions.
*   **`${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh`:** This is the *core processing script* invoked by the wrapper. This script contains the actual business logic for contract data reconciliation for `ta_vvl_dwh`. Its migration is critical for the overall job's functionality and will need its own dedicated analysis and migration design, likely into a BigQuery Stored Procedure or a data processing job (e.g., PySpark) orchestrated by Cloud Composer. The current design assumes this will be converted into a BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_vvl_dwh`.
*   **Filesystem Logging:** The original script writes logs to a file (`$LogDatei`). This will be replaced by inserts into BigQuery logging tables as described in Section 3.

No direct external database or API calls were identified in the `r_ausd_v_ta_vvl_dwh.ksh` wrapper script itself.

## 7. Unresolved / Risks
*   **Core Logic of `k_ausd_v_ta_vvl_dwh.ksh`:** This is the most significant unresolved item. The current design only covers the wrapper. The actual data reconciliation logic is in `k_ausd_v_ta_vvl_dwh.ksh`, which was not part of the assembled job's component files and thus not analyzed in detail during this initial phase. A complete migration requires a separate, in-depth analysis and design for this core script.
*   **`DWMSG_*` Utilities:** The exact implementations of `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` are unknown. Their functionalities need to be replicated faithfully in BigQuery stored procedure helper routines or directly within the logging table DML statements.
*   **`.dw_init` Content:** The specific environment settings configured by `.dw_init` are unknown. These settings might include critical paths, database connections, or other configuration values that need to be identified and incorporated into the BigQuery environment or passed as parameters.
*   **Parameter Usage:** The script declares `-s` and `-l` as accepted parameters via `getopts` but does not use them in the provided code. It is assumed these parameters are passed to the `k_ausd_v_ta_vvl_dwh.ksh` script or have implicit effects. Further investigation may be needed if they become relevant to the core logic.
*   **Error Handling Granularity:** The shell `trap` mechanism provides immediate and system-level error interception. BigQuery's `EXCEPTION` handling is within the SQL context. Any subtle differences in error propagation or recovery behavior need careful consideration during implementation and testing.

## 8. Build Plan
The build plan for migrating the `r_ausd_v_ta_vvl_dwh.ksh` wrapper script to BigQuery will proceed as follows:

1.  **Define BigQuery Schemas for Logging/Control Tables:**
    *   Create `project.dataset.job_control` table (e.g., `entry_nr INT64`, `job_kennung STRING`, `script_name STRING`, `log_name STRING`, `stichtag STRING`, `status STRING`, `created_at TIMESTAMP`, `finished_at TIMESTAMP`).
    *   Create `project.dataset.job_messages` table (e.g., `entry_nr INT64`, `job_kennung STRING`, `message_text STRING`, `message_type STRING`, `created_at TIMESTAMP`).
    *   Create `project.dataset.job_error_log` table (e.g., `job_kennung STRING`, `error_nr INT64`, `error_arg STRING`, `created_at TIMESTAMP`).
    *   Create `project.dataset.job_audit` table (e.g., `entry_nr INT64`, `job_kennung STRING`, `status STRING`, `log_name STRING`, `created_at TIMESTAMP`).

2.  **Develop BigQuery Stored Procedure for Wrapper Logic:**
    *   **Language:** BigQuery SQL
    *   **File:** `vertragsdatenabgleich.sql`
    *   **Content:** Implement the logic as provided in the BigQuery SQL pseudocode, including parameter handling, job number allocation, logging, and error handling. This procedure will include a `CALL` statement to the future BigQuery Stored Procedure for `k_ausd_v_ta_vvl_dwh.ksh`.

3.  **Migrate Core Logic (`k_ausd_v_ta_vvl_dwh.ksh`)**
    *   **Action:** Perform a dedicated analysis and design for `k_ausd_v_ta_vvl_dwh.ksh`.
    *   **Output:** BigQuery Stored Procedure (`k_ausd_v_ta_vvl_dwh.sql`) or other BigQuery data processing components.

4.  **Develop Orchestration Layer (Cloud Composer/Workflows):**
    *   **Language:** Python (for Airflow DAG) or YAML (for Workflows).
    *   **File:** `vertragsdatenabgleich_dag.py` or `vertragsdatenabgleich_workflow.yaml`
    *   **Content:** Define a DAG/Workflow that schedules and triggers the `project.dataset.vertragsdatenabgleich` BigQuery Stored Procedure, passing required parameters.

5.  **Testing and Validation:**
    *   Unit test the BigQuery Stored Procedure (`vertragsdatenabgleich.sql`) for parameter handling, logging, and error conditions.
    *   Integrate test with the migrated core logic (`k_ausd_v_ta_vvl_dwh` equivalent).
    *   End-to-end testing with the Cloud Composer DAG/Workflow.