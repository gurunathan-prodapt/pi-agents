# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_inv_def.ksh`, which acts as a wrapper and orchestrator for a contract data reconciliation job. Its primary business purpose is to manage the data synchronization process for the `ta_inv_def` table.

The script's scope includes:
- Parsing command-line parameters (`-h`, `-s`, `-l`).
- Initializing the runtime environment by sourcing various utility scripts.
- Setting up comprehensive logging and error handling mechanisms.
- Invoking the core data synchronization logic contained within `k_ausd_v_ta_inv_def.ksh`.
- Reporting the job status upon completion.

The migration will target Google Cloud's BigQuery for data processing and potentially Cloud Composer (Airflow) or Cloud Workflows for orchestration.

## 2. Source Inventory

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh`
*   **Technology:** KornShell
*   **Summary:** This script is an orchestration wrapper responsible for environment setup, parameter handling, error trapping, and invoking a core data synchronization script for the `ta_inv_def` table.
*   **File Purpose:** Orchestrator/Wrapper
*   **Complexity Tier:** (Not available from `file_complexity` table, but the script is of medium complexity due to its wrapper nature, error handling, and external script invocations.)
*   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The migrated solution will primarily leverage BigQuery for data processing and possibly Cloud Composer (Airflow) or Cloud Workflows for job orchestration.

*   **Orchestration:** The wrapper logic will be migrated to a BigQuery stored procedure, invoked by an external scheduler like Cloud Composer. This stored procedure will manage parameters, logging, and call the transformed core logic.
*   **Core Logic:** The functionality of `k_ausd_v_ta_inv_def.ksh` (the core script) will be migrated into a separate BigQuery stored procedure or a series of SQL statements executed within the orchestration layer.
*   **Logging & Error Handling:** The existing `DWMSG_*` functions and file-based logging will be replaced by dedicated BigQuery audit/log tables. Error handling will utilize BigQuery's `BEGIN...EXCEPTION` blocks.
*   **Environment Variables:** Shell environment variables will be replaced by BigQuery stored procedure parameters or explicit configuration passed at runtime.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_inv_def.ksh` primarily orchestrates the execution of other scripts.

**Legacy Flow:**
1.  `r_ausd_v_ta_inv_def.ksh` starts.
2.  Sources environment configuration (`. $HOME/.dw_init`).
3.  Sources utility scripts for error handling and logging (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
4.  Parses command-line parameters.
5.  Sets up job-specific logging parameters (`JobKennung`, `DW_EintragsNr`, `LogDatei`).
6.  Installs `trap` handlers for `INT` and `ERR` signals.
7.  Prints job metadata to console and log file.
8.  **Invokes core script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh` with parameters (`-j $JobKennung -f ${DW_EintragsNr}`). The output is redirected to `$LogDatei`.
9.  Upon successful completion of the core script, a success message is printed and the job status is set to OK in the logging framework.
10. Traps are removed, and the script exits.

**Target BigQuery Flow:**
1.  A Cloud Composer DAG (or Cloud Workflow) is triggered by a scheduler.
2.  The DAG invokes a BigQuery stored procedure (e.g., `sp_r_ausd_v_ta_inv_def`).
3.  `sp_r_ausd_v_ta_inv_def` declares necessary variables, mimicking the shell script's initialization and parameter handling.
4.  It calls helper stored procedures or inserts into audit tables to log job start, parameters, and other metadata.
5.  **Invokes core logic:** `sp_r_ausd_v_ta_inv_def` calls another BigQuery stored procedure (e.g., `sp_k_ausd_v_ta_inv_def`) which contains the transformed logic of `k_ausd_v_ta_inv_def.ksh`. Parameters (`DW_EintragsNr`, `JobKennung`) are passed.
6.  Error handling uses `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks to capture and log errors to audit tables.
7.  Upon successful completion, `sp_r_ausd_v_ta_inv_def` updates the audit table with success status.

## 5. Transformation Logic
The transformation logic applies to the orchestration and utility aspects of `r_ausd_v_ta_inv_def.ksh`. The core data synchronization logic (within `k_ausd_v_ta_inv_def.ksh`) is assumed to be migrated separately into a BigQuery stored procedure or direct SQL statements.

*   **Parameter Parsing (`getopts`):** Replaced by BigQuery stored procedure input parameters. Validation will be done using `IF` statements and `RAISE` for errors.
*   **Environment Sourcing (`. $HOME/.dw_init`, etc.):** These shell library inclusions will be replaced by:
    *   BigQuery stored procedures for `DWMSG_*` functions (e.g., `DWMSG_MeldeFehler` becomes `CALL dwmsg.melde_fehler(...)`).
    *   Explicit parameter passing or configuration tables for environment-specific values like `BERT_DIR_ROOT`.
*   **Date Operations (`date +%d%m%Y`):** Will be converted to BigQuery SQL functions like `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Logging (`print`, `tee -a`, `>> $LogDatei`):** Replaced by `INSERT` statements into dedicated BigQuery logging/audit tables.
*   **Error Trapping (`trap INT ERR`):** Replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks to catch and handle execution errors.
*   **Conditional Logic (`if [ ! $ErrNr -eq 0 ]`, `case $param in`):** Directly translated to BigQuery SQL `IF` and `CASE` statements.
*   **Script Invocation (`${Name_Kernskript}`):** Replaced by a `CALL` statement to the migrated BigQuery stored procedure representing `k_ausd_v_ta_inv_def.ksh`.

**BigQuery SQL Pseudocode for `r_ausd_v_ta_inv_def.ksh` (orchestration wrapper):**

```sql
-- BigQuery Script: Vertragsdatenabgleich wrapper (sp_r_ausd_v_ta_inv_def)

CREATE OR REPLACE PROCEDURE `project_id`.`dataset_id`.sp_r_ausd_v_ta_inv_def(
  p_h STRING, -- Equivalent to -h flag
  p_s STRING, -- Equivalent to -s flag
  p_l STRING  -- Equivalent to -l flag
)
BEGIN

  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64;
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_INV_DEF';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE LogDatei STRING;

  -- Usage/help equivalent
  IF p_h IS NOT NULL THEN
    SELECT
      ProgName AS Programm,
      ProgVersion AS Version,
      'Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_inv_def.' AS Beschreibung;
    RETURN; -- Exit procedure
  END IF;

  -- Parameter validation (simplified; actual validation depends on p_s, p_l usage)
  -- If external orchestrator handles parameter issues, this might be minimal.
  -- For example, if p_s or p_l are mandatory and missing:
  -- IF p_s IS NULL THEN SET ErrNr = 193; SET ErrArg = 's'; END IF;

  IF ErrNr <> 0 THEN
    -- CALL to migrated DWMSG_MeldeFehler procedure
    CALL `project_id`.`dataset_id`.sp_dwmsg_meldefehler(DW_EintragsNr, 'E', ErrNr, ErrArg);
    RAISE USING MESSAGE = CONCAT('Parameterfehler: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Job metadata and logging setup
  CALL `project_id`.`dataset_id`.sp_dwmsg_ermittle_nr(DW_EintragsNr); -- Returns DW_EintragsNr
  CALL `project_id`.`dataset_id`.sp_dwmsg_logdateiname(LogDatei, JobKennung, DW_EintragsNr); -- Returns LogDatei
  CALL `project_id`.`dataset_id`.sp_dwmsg_erzeuge_eintrag(DW_EintragsNr, JobKennung, 'r_ausd_v_ta_inv_def.ksh', LogDatei);
  CALL `project_id`.`dataset_id`.sp_dwmsg_setze_stichtag_info(DW_EintragsNr, v_sysdate, 'DDMMYYYY');

  BEGIN
    -- Job banner equivalent (log to audit table)
    CALL `project_id`.`dataset_id`.sp_dwmsg_log_info('----------------- Job -----------------------');
    CALL `project_id`.`dataset_id`.sp_dwmsg_log_info(CONCAT(' Job-Nr    : \'', CAST(DW_EintragsNr AS STRING), '\''));
    CALL `project_id`.`dataset_id`.sp_dwmsg_log_info(CONCAT(' JobKennung: \'', JobKennung, '\''));
    CALL `project_id`.`dataset_id`.sp_dwmsg_log_info(CONCAT(' Logdatei  : \'', LogDatei, '\''));
    CALL `project_id`.`dataset_id`.sp_dwmsg_log_info('---------------------------------------------');

    -- Core script invocation equivalent
    -- Pass parameters (like JobKennung, DW_EintragsNr) to the core procedure
    CALL `project_id`.`dataset_id`.sp_k_ausd_v_ta_inv_def(JobKennung, DW_EintragsNr);

    -- Success handling
    CALL `project_id`.`dataset_id`.sp_dwmsg_log_info('Die Abarbeitung wurde ohne erkennbare Fehler beendet');
    CALL `project_id`.`dataset_id`.sp_dwmsg_setze_status_ok(DW_EintragsNr);

  EXCEPTION WHEN ERROR THEN
    CALL `project_id`.`dataset_id`.sp_dwmsg_fehlerbehandlung(DW_EintragsNr, @@error.message);
    CALL `project_id`.`dataset_id`.sp_dwmsg_log_error('AppError: Abbruch');
    RAISE; -- Re-raise the error to propagate
  END;

END;
```

## 6. External Dependencies
The script itself does not directly interact with external systems like Oracle, SFTP, or S3. Its dependencies are primarily other shell scripts and the underlying operating system environment.

*   **Sourced `dw_init` and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These provide environment variables and helper functions.
    *   **Replacement:** These will be replaced by BigQuery stored procedures (for functions like `DWMSG_*`) or configuration values/parameters managed within BigQuery or the orchestration layer.
*   **Core script (`k_ausd_v_ta_inv_def.ksh`):** This is the main dependency carrying the actual business logic.
    *   **Replacement:** This script will be migrated into a separate BigQuery stored procedure (e.g., `sp_k_ausd_v_ta_inv_def`) that handles the data synchronization for `ta_inv_def`. The wrapper procedure will call this core procedure.
*   **Operating System Commands (`date`, `print`, `tee`):**
    *   **Replacement:** These will be replaced by equivalent BigQuery SQL functions or logging mechanisms into BigQuery tables.

## 7. Unresolved / Risks
*   **Missing `file_complexity` data:** The complexity tier and migration flags were not available from the `file_complexity` table. This means the migration effort might be underestimated. A manual review of the `k_ausd_v_ta_inv_def.ksh` script (the core logic) is crucial to accurately assess the overall job complexity.
*   **Functionality of `k_ausd_v_ta_inv_def.ksh`:** The actual data synchronization logic within the core script is unknown from this analysis. Its complexity and dependencies (e.g., database interactions, external systems) are critical for a complete design and may introduce further risks or require redesign (B4).
*   **Shell Environment Variables:** The exact values and usage of environment variables set by `. $HOME/.dw_init` and other scripts (e.g., `BERT_DIR_ROOT`) need to be thoroughly understood and mapped to BigQuery parameters or configuration.
*   **Log Message Structure:** The `DWMSG_*` functions imply a specific logging framework. The exact structure and destination of these logs need to be replicated in BigQuery audit tables to maintain historical tracking and operational visibility.
*   **Error Handling Granularity:** While `BEGIN...EXCEPTION` handles errors, replicating the exact behavior of `trap INT ERR` for all edge cases (e.g., external process termination) might require additional orchestration logic in Cloud Composer/Workflows.

## 8. Build Plan
The build plan focuses on migrating the wrapper script and assumes the core `k_ausd_v_ta_inv_def.ksh` script will also be migrated to BigQuery.

1.  **Migrate Utility Functions to BigQuery Stored Procedures (BQSQL):**
    *   Create `sp_dwmsg_meldefehler`, `sp_dwmsg_ermittle_nr`, `sp_dwmsg_logdateiname`, `sp_dwmsg_erzeuge_eintrag`, `sp_dwmsg_setze_stichtag_info`, `sp_dwmsg_fehlerbehandlung`, `sp_dwmsg_setze_status_ok`, and `sp_dwmsg_log_info`. These procedures will interact with new BigQuery audit/log tables. (BQSQL)
2.  **Define BigQuery Audit/Log Tables (DDL):**
    *   Create tables (e.g., `job_audit_log`, `job_parameters`) to store job execution details, messages, and status. (BQSQL)
3.  **Migrate Core Logic (`k_ausd_v_ta_inv_def.ksh`) to a BigQuery Stored Procedure:**
    *   Design and implement `sp_k_ausd_v_ta_inv_def` to contain the actual data synchronization logic for `ta_inv_def`. This is a critical prerequisite. (BQSQL)
4.  **Create Wrapper Orchestration Stored Procedure (`sp_r_ausd_v_ta_inv_def`) (BQSQL):**
    *   Implement the BigQuery SQL pseudocode provided in Section 5. This procedure will call the utility procedures and `sp_k_ausd_v_ta_inv_def`.
5.  **Develop Cloud Composer DAG (Python):**
    *   Create an Airflow DAG that orchestrates the execution of `sp_r_ausd_v_ta_inv_def` in BigQuery. This DAG will handle scheduling, parameter passing, and potentially more advanced error/retry logic if needed beyond BigQuery's internal exception handling. (Python)
6.  **Configuration and Deployment (YAML/JSON/Terraform):**
    *   Define BigQuery dataset, project, and service account configurations.
    *   Set up Cloud Composer environment.
    *   Deploy DDL, stored procedures, and the Airflow DAG.