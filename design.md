# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh

## 1. Purpose & Scope
This KornShell script (`r_ausd_v_ta_cntrct_crs.ksh`) serves as a wrapper or orchestration framework for the contract data reconciliation process, specifically for the `ta_cntrct_crs` table. Its primary responsibilities include parsing command-line parameters, initializing the runtime environment and error-handling framework, creating job and log metadata, invoking a core data processing script (`k_ausd_v_ta_cntrct_crs.ksh`), and recording the final success or failure status. The job was assembled from a single component.

## 2. Source Inventory
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh`
*   **Category**: shell
*   **Tool**: KornShell
*   **Complexity Tier**: Not available in `file_complexity` table.
*   **Migration Bucket**: semi_auto

## 3. Target Architecture
The migration target is Google BigQuery. The `r_ausd_v_ta_cntrct_crs.ksh` script, being an orchestration layer, will be primarily migrated to a BigQuery Stored Procedure.

*   **Orchestration**: A BigQuery Stored Procedure (`project.dataset.vertragsdatenabgleich_wrapper`) will encapsulate the shell script's logic for parameter validation, environment setup, job metadata management, and error handling.
*   **Logging**: Shell script logging to `$LogDatei` will be replaced with `INSERT` operations into a dedicated BigQuery logging table (e.g., `project.dataset.job_log` and `project.dataset.job_error_log`).
*   **Configuration**: Environment variables and sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be replaced by BigQuery procedure parameters, session variables, or configuration tables.
*   **Core Logic Invocation**: The invocation of the core script (`k_ausd_v_ta_cntrct_crs.ksh`) will be translated into a `CALL` to another BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_cntrct_crs`) that contains the migrated business logic.
*   **Error Handling**: Shell `trap` mechanisms will be mapped to BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for robust error management and status updates in the logging tables.
*   **External Orchestration (Optional)**: If the core script (`k_ausd_v_ta_cntrct_crs.ksh`) contains non-SQL logic that cannot be directly translated to BigQuery SQL, it may require isolation in Python and execution via external orchestration tools like Cloud Composer (Airflow), with results persisted back to BigQuery.

## 4. Data Flow & Lineage
The `r_ausd_v_ta_cntrct_crs.ksh` script acts as a control script.
1.  **Initialization**: It sources several utility scripts: `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, and `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`. These scripts provide environment variables, error handling functions, and date utilities.
2.  **Parameter Processing**: It parses command-line arguments using `getopts`.
3.  **Metadata Generation**: It determines a unique job entry number (`DW_EintragsNr`) and constructs a log file name (`LogDatei`).
4.  **Core Script Invocation**: The script's primary function is to invoke a core processing script, `k_ausd_v_ta_cntrct_crs.ksh`, passing `JobKennung` and `DW_EintragsNr` as parameters.
5.  **Logging and Status Update**: All output (stdout/stderr of itself and the invoked script) is redirected to `$LogDatei`. It uses utility functions (e.g., `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`) to manage job status and logging.

The `lineage_edges` query did not explicitly show `r_ausd_v_ta_cntrct_crs.ksh` invoking `k_ausd_v_ta_cntrct_crs.ksh`. However, the KornShell script content clearly indicates this internal invocation. The `lineage_edges` output did show numerous `INVOKES` relationships where UC4 XML files invoked other KornShell scripts, suggesting that the orchestrating role of `.ksh` scripts is recognized in the lineage.

## 5. Transformation Logic
The `r_ausd_v_ta_cntrct_crs.ksh` wrapper script itself does not contain any direct business transformation or aggregation logic. Its function is purely orchestrational and for metadata preparation. The actual data processing and transformation logic are delegated to the `k_ausd_v_ta_cntrct_crs.ksh` script, which this wrapper invokes.
The logic within `r_ausd_v_ta_cntrct_crs.ksh` primarily involves:
*   Program metadata initialization (ProgName, ProgVersion).
*   Parameter validation and error checking.
*   Initialization of job identifiers (`DW_EintragsNr`, `JobKennung`).
*   Log file naming (`LogDatei`).
*   Setting up system date (`v_sysdate`).
*   Error trapping and signal handling.

## 6. External Dependencies
The job itself (`6d73ee79`) does not have any explicitly listed external systems or unresolved targets from the `lineage_assembled_jobs` table.
However, based on the source code, the script depends on:
*   **Operating System**: KornShell environment (`#!/bin/ksh`).
*   **Sourced Utility Scripts**:
    *   `$HOME/.dw_init`: For environment variable setup.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: For error handling functions like `DWMSG_MeldeFehler`.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: For parameter handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: For date-related utilities.
*   **Core Processing Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh`. This is a critical dependency as it contains the actual business logic.
*   **System Commands**: `getopts`, `trap`, `date`, `tee`, `print`.

**Replacement in BigQuery:**
*   KornShell environment replaced by BigQuery SQL engine.
*   Sourced utility scripts' functionalities will be translated into BigQuery Stored Procedures or SQL UDFs, or replaced by BigQuery native features (e.g., logging to tables).
*   The core processing script will be migrated to a BigQuery Stored Procedure.
*   System commands will be mapped to their BigQuery SQL equivalents (e.g., `date` to `FORMAT_DATE(CURRENT_DATE())`, `tee` to `INSERT` into log tables).

## 7. Unresolved / Risks
*   **Complexity of Sourced Scripts**: The exact functionality of the sourced utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) is not fully detailed. Their conversion to BigQuery Stored Procedures or UDFs requires analysis of their content.
*   **Core Script Translation**: The most significant risk lies in the migration of `k_ausd_v_ta_cntrct_crs.ksh`. If it contains complex shell-based logic beyond simple SQL execution, its conversion to BigQuery SQL may be challenging, potentially requiring a Python-based solution in Cloud Composer or another orchestration tool.
*   **Shell-specific Behavior**: Direct translation of `trap` semantics and shell exit-code handling to BigQuery SQL is not fully equivalent and requires careful design using `EXCEPTION` blocks and status tables.
*   **Empty Parameter Handling**: The script declares `s:l:` for `getopts` but does not implement handling logic for `-s` or `-l`. This suggests potential dead code or incomplete functionality that needs clarification.

## 8. Build Plan
The migration of `r_ausd_v_ta_cntrct_crs.ksh` to BigQuery will involve the following ordered steps, with the primary language being BigQuery SQL (for stored procedures and DDL) and potentially Python (for Cloud Composer orchestration if needed for `k_ausd_v_ta_cntrct_crs.ksh`).

1.  **DDL for Logging and Control Tables (BigQuery SQL)**:
    *   Create `project.dataset.job_control` table to store job metadata, status, and log names.
    *   Create `project.dataset.job_log` table for detailed log messages.
    *   Create `project.dataset.job_error_log` table for error details.
    *   Create any necessary configuration tables to replace environment variables or script-defined paths.

2.  **Migrate Utility Functions (BigQuery SQL)**:
    *   Analyze `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`.
    *   Create BigQuery SQL Stored Procedures or UDFs that replicate the functionality of `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`.

3.  **Migrate Core Processing Script (BigQuery SQL / Python)**:
    *   **Crucially**, the content of `k_ausd_v_ta_cntrct_crs.ksh` must be analyzed and migrated. If it primarily contains SQL, convert it to a BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_cntrct_crs`). If it contains complex shell logic and data manipulations, consider migrating it to Python (e.g., using PySpark on Dataproc) and orchestrating its execution via Cloud Composer.

4.  **Create BigQuery Wrapper Stored Procedure (BigQuery SQL)**:
    *   Create a stored procedure `project.dataset.vertragsdatenabgleich_wrapper`.
    *   **Parameter Handling**: Define input parameters for the procedure to replace command-line arguments (e.g., `p_show_help BOOL`).
    *   **Variable Declarations**: Declare BigQuery SQL variables for `ProgName`, `ProgVersion`, `ErrNr`, `ErrArg`, `DW_EintragsNr`, `JobKennung`, `v_sysdate`, `LogDatei`, `Name_Kernskript`, and `v_status`.
    *   **Usage / Help**: Implement `IF p_show_help THEN ... LEAVE; END IF;` for help documentation.
    *   **Error Handling**: Translate `getopts` parameter validation into BigQuery SQL `IF` and `CASE` statements. If errors are found, insert into `job_error_log` and `SIGNAL SQLSTATE`.
    *   **Job Metadata**: Implement logic to get `DW_EintragsNr` and `LogDatei` using `SELECT MAX(job_id) + 1` from `job_control`.
    *   **Log Start**: `INSERT` into `job_control` and `job_log` to record job start.
    *   **Reference Date**: `UPDATE` `job_control` with `reference_date`.
    *   **Core Logic Invocation**: `CALL project.dataset.k_ausd_v_ta_cntrct_crs(JobKennung, DW_EintragsNr);`.
    *   **Success Handling**: If the core call succeeds, `INSERT` success message into `job_log` and `UPDATE` `job_control` status to 'OK'.
    *   **Exception Handling**: Implement `BEGIN...EXCEPTION WHEN ERROR THEN...END` block. Inside the `EXCEPTION` block, `INSERT` error details into `job_error_log`, `UPDATE` `job_control` status to 'ERROR', and `RAISE` an error.

5.  **Deployment and Scheduling (Cloud Composer / BigQuery Scheduled Queries)**:
    *   Deploy the BigQuery Stored Procedures.
    *   Set up a BigQuery Scheduled Query or a Cloud Composer (Airflow) DAG to execute `project.dataset.vertragsdatenabgleich_wrapper` on its required schedule.