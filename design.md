# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh

## 1. Purpose & Scope
The KornShell script `r_ausd_bp_ta_iccid_vertrag.ksh` acts as an orchestrator for an initial provisioning job of selected base products for BERT (an internal system). Its primary purpose is to parse command-line arguments, specifically a 'Stichtag' (key date) and a 'Wiederanlaufwert' (restart value). It then sets up logging and error handling, and subsequently executes a core data preparation script, `k_ausd_bp_ta_iccid_vertrag.ksh`, passing these parameters. The script is responsible for job control, ensuring proper execution, logging, and status tracking. The actual data processing and transformation logic resides within the invoked core script.

## 2. Source Inventory
The job is comprised of a single KornShell script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh`
*   **Technology**: KornShell (UNIX Shell Script)
*   **Complexity Tier**: medium
*   **Automation Bucket**: semi_auto

## 3. Target Architecture
The migration target is Google BigQuery. The orchestration logic of the KornShell script will be converted into a BigQuery Stored Procedure. The core processing logic, assumed to be in `k_ausd_bp_ta_iccid_vertrag.ksh`, will also be a separate BigQuery Stored Procedure.

*   **BigQuery Stored Procedures**:
    *   `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`: This procedure will replicate the orchestration logic of the original KornShell script.
    *   `project.dataset.k_ausd_bp_ta_iccid_vertrag`: This will be a placeholder for the migrated core logic, to be developed as a separate stored procedure.
*   **BigQuery Tables**:
    *   `project.dataset.job_control`: An audit/control table to store job execution metadata (job_entry_nr, job_name, status, timestamps).
    *   `project.dataset.job_run_log`: A logging table to store detailed execution logs (messages, log levels, timestamps).
    *   `project.dataset.job_error_log`: A table to record specific error details during validation or execution.
    *   `project.dataset.job_usage_log`: A table to log instances where usage instructions are displayed (e.g., due to invalid parameters).

## 4. Data Flow & Lineage
The original script's flow:
1.  **Environment Setup**: Sources `.dw_init` and utility scripts for error handling, parameter parsing, and date functions.
2.  **Parameter Parsing**: Uses `getopts` to read `-s <Stichtag>` (reference date in DDMMYYYY) and `-l <Wiederanlaufwert>` (restart value).
3.  **Defaulting**:
    *   `p_wiederanlaufWert` defaults to 0 if not provided.
    *   `p_stichtag` defaults to the current system date if not provided (though comments suggest `MIN(sysdate, maxladedatum)` as an alternative, the active code uses `sysdate`).
4.  **Validation**: Checks if `p_stichtag` is set.
5.  **Job Control & Logging**: Initializes job metadata, generates a unique job entry number (`DW_EintragsNr`), creates a log file, and logs job start information.
6.  **Error Trapping**: Sets up `trap` commands to catch `INT`, `STOP`, `CONT`, and `ERR` signals, directing error handling to `DWMSG_Fehlerbehandlung`.
7.  **Core Script Invocation**: Executes `k_ausd_bp_ta_iccid_vertrag.ksh` with parsed parameters: `${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert} >> $LogDatei 2>&1`.
8.  **Success Handling**: If the core script completes without error, logs a success message and updates the job status to OK.
9.  **Exit**: Exits with `0` on success or a non-zero error code on failure.

**Migrated BigQuery Data Flow**:
The BigQuery stored procedure `ausd_bp_ta_iccid_vertrag_wrapper` will:
1.  Accept `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) as input parameters.
2.  Derive the current system date using `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
3.  Apply defaulting logic for `v_wiederanlaufWert` (to 0 if NULL) and `v_stichtag` (to system date if NULL).
4.  Validate `v_stichtag` and raise an error if invalid, logging to `job_error_log` and `job_usage_log`.
5.  Generate a new `job_entry_nr` by incrementing the max existing entry from `job_control`.
6.  Record job start information in `job_control` and `job_run_log`.
7.  Execute the core logic by calling the `project.dataset.k_ausd_bp_ta_iccid_vertrag` stored procedure, passing all necessary parameters.
8.  Implement error handling using a `BEGIN...EXCEPTION WHEN ERROR THEN...END` block to catch and log exceptions, updating `job_control` and `job_run_log` with 'ERROR' status.
9.  On successful completion, update `job_control` status to 'OK' and log a success message to `job_run_log`.

## 5. Transformation Logic
The original KornShell script's control flow and parameter handling will be translated into BigQuery SQL scripting statements.

*   **Parameter Handling**: `getopts` arguments `-s` and `-l` will become `IN` parameters `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) in the BigQuery stored procedure.
*   **Defaulting Logic**: Shell `if [[ -z "$var" ]] then var=default fi` constructs will be mapped to `IFNULL(parameter, default_value)` or `IF` statements in BigQuery.
*   **Date Derivation**: `DWDate_Gib_Zeitraum` will be replaced by `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`. The commented-out `MIN(sysdate, maxladedatum)` would require querying source metadata if re-enabled.
*   **Validation**: The `pruefeParameterGesetzt` call and `if [ ! $ErrNr -eq 0 ]` block will be converted into `IF` statements with `RAISE` for immediate termination and logging to error tables.
*   **Job Control & Logging**: All `DWMSG_` functions and `print`/`tee` commands will be replaced by `INSERT` statements into the `job_control`, `job_run_log`, `job_error_log`, and `job_usage_log` tables.
*   **Error Trapping**: Shell `trap` commands will be substituted by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, providing robust error handling within the SQL context.
*   **Core Script Invocation**: The shell execution `${Name_Kernskript} ...` will be translated into a `CALL` statement to the corresponding BigQuery stored procedure `project.dataset.k_ausd_bp_ta_iccid_vertrag`.

## 6. External Dependencies
The original script has several external dependencies:

*   **Environment Initialization**: `. $HOME/.dw_init`
    *   **Migration**: Replaced by BigQuery environment configuration. This includes setting `project.dataset` in the stored procedure definitions, and potentially using BigQuery scripting variables or configuration tables for dynamic values.
*   **Helper Scripts**:
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling)
    *   **Migration**: The functionalities of these helper scripts are directly integrated into the BigQuery stored procedure's logic (e.g., `FORMAT_DATE`, `IFNULL`, `RAISE`, `INSERT` into log tables).
*   **Core Processing Script**: `k_ausd_bp_ta_iccid_vertrag.ksh`
    *   **Migration**: This is the most critical external dependency. It will be migrated into its own BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_iccid_vertrag`. This new procedure will be invoked by the `ausd_bp_ta_iccid_vertrag_wrapper`. The details of this core script's migration are out of scope for this document but are crucial for the overall job.
*   **File System (Logging)**: The script writes to log files (`$LogDatei`).
    *   **Migration**: Replaced by inserts into dedicated BigQuery logging tables (`job_run_log`, `job_error_log`).

There were no explicit external systems (like Oracle, SFTP, S3) identified as directly referenced by this orchestrator script. Any such dependencies would be inherited by the `k_ausd_bp_ta_iccid_vertrag.ksh` script and addressed in its specific migration design.

## 7. Unresolved / Risks
*   **Core Logic of `k_ausd_bp_ta_iccid_vertrag.ksh`**: The current design only covers the wrapper script. The actual business logic for data extraction, transformation, and loading is in `k_ausd_bp_ta_iccid_vertrag.ksh`. This script needs to be analyzed and migrated separately. Its complexity could range from simple SQL to complex data manipulations, potentially requiring BigQuery SQL, Python (for PySpark/Dataflow), or other BigQuery services.
*   **Exact Behavior of Helper Functions**: The current design assumes standard interpretations for functions like `DWMSG_ErmittleNr` or `DWMSG_Logdateiname`. If these functions have highly custom or complex logic, further analysis will be required to accurately replicate their behavior in BigQuery.
*   **Commented-out Logic**: The original script contains commented-out lines like `FOSHoleLadedatum "DWH\\$TA_C_VERTRAG" v_ladedatum` for determining `p_stichtag`. While currently inactive, if this logic is ever re-enabled or becomes relevant, it would require identifying the `DWH$TA_C_VERTRAG` table and its `maxladedatum` equivalent in BigQuery.
*   **System Exit Codes**: The shell script exits with specific error codes. In BigQuery, this is handled via `RAISE` and the job's overall success/failure status, but direct mapping of specific exit codes might need a custom error code logging mechanism.

## 8. Build Plan
The following BigQuery components will be built:

1.  **Create BigQuery Tables**:
    *   `project.dataset.job_control` (to track job execution status)
    *   `project.dataset.job_run_log` (for detailed logging)
    *   `project.dataset.job_error_log` (for error details)
    *   `project.dataset.job_usage_log` (for usage messages)
    *   (Details of columns for these tables would be part of a separate DDL script.)

2.  **Develop BigQuery Stored Procedure: `project.dataset.k_ausd_bp_ta_iccid_vertrag`**
    *   **Language**: BigQuery SQL (or other suitable BigQuery service if complex non-SQL logic is found).
    *   **Content**: This procedure will contain the actual data processing logic from the original `k_ausd_bp_ta_iccid_vertrag.ksh`. This involves selecting data based on `p_stichtag`, applying `p_wiederanlaufWert` as a filter, and writing results to a target FOS table.

3.  **Develop BigQuery Stored Procedure: `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`**
    *   **Language**: BigQuery SQL.
    *   **Content**: The pseudocode provided by the migration tool will be implemented directly in BigQuery SQL, including:
        *   Parameter definitions (`p_stichtag`, `p_wiederanlaufWert`).
        *   Variable declarations.
        *   Date formatting (`FORMAT_DATE`).
        *   Defaulting logic (`IFNULL`, `IF`).
        *   Validation logic (`IF`, `RAISE`).
        *   Job control logic (`INSERT` into `job_control`).
        *   Logging (`INSERT` into `job_run_log`, `job_error_log`, `job_usage_log`).
        *   Error handling (`BEGIN...EXCEPTION WHEN ERROR THEN...END`).
        *   Invocation of the core procedure (`CALL project.dataset.k_ausd_bp_ta_iccid_vertrag`).

4.  **Integration with Orchestration**: The `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper` BigQuery Stored Procedure will be integrated into the target orchestration system (e.g., Cloud Composer/Airflow DAG) to replace the original KornShell script execution. This would involve configuring the DAG to call the BigQuery stored procedure with appropriate parameters.