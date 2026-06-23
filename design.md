# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `r_ausd_bp_ta_rn_einzeln.ksh` to Google BigQuery. The original script acts as an orchestrator for the initial provisioning of selected base products for BERT (a business process/system). Its primary functions include parsing command-line parameters (like a cutoff date and a restart value), setting up a robust logging and error-handling framework, and then invoking a core downstream script (`k_ausd_bp_ta_rn_einzeln.ksh`) which is expected to contain the main business logic for data processing. The script ensures proper parameter validation and job status reporting.

## 2. Source Inventory
The job is composed of a single primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh`
    *   **Technology:** KornShell Script (`shell`)
    *   **Migration Bucket:** `semi_auto`
    *   **Complexity Tier:** Not explicitly available from `file_complexity` table. Based on the content, it's an orchestration script with moderate logic.
    *   **Purpose:** Job orchestration, parameter handling, logging setup, and invocation of a core processing script.
    *   **Key components and dependencies within the script:**
        *   Sourcing of environment initialization file: `$HOME/.dw_init`
        *   Sourcing of error handling utility: `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
        *   Sourcing of parameter parsing utility: `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
        *   Sourcing of date handling utility: `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
        *   Invocation of core processing script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh`

## 3. Target Architecture
The target architecture in BigQuery will involve:

*   **Main Orchestration:** The `r_ausd_bp_ta_rn_einzeln.ksh` script will be migrated into a BigQuery Stored Procedure (e.g., `project.dataset.ausd_bp_ta_rn_einzeln`). This stored procedure will encapsulate the parameter parsing, date logic, logging setup, and the invocation of the core business logic.
*   **Core Business Logic:** The downstream script `k_ausd_bp_ta_rn_einzeln.ksh` will also be migrated, ideally into another BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_bp_ta_rn_einzeln`) or a set of BigQuery SQL scripts/views, as it contains the actual data processing logic.
*   **Logging and Auditing:** File-based logging (`DWMSG_*` functions) will be replaced by dedicated BigQuery audit tables, such as `project.dataset.job_control`, `project.dataset.job_messages`, and `project.dataset.job_error_log`, to capture job metadata, status, and error information.
*   **Parameter Handling:** Command-line parameters will be mapped to input parameters of the BigQuery Stored Procedure.
*   **Environment Variables:** Sourced environment variables will be replaced by stored procedure variables or configuration parameters.
*   **External Orchestration (Optional):** If the overall job flow involves complex inter-dependencies or scheduling outside of BigQuery's native capabilities, an external orchestrator like Cloud Composer (Airflow), Workflows, or Dataform can be used to trigger the BigQuery Stored Procedures.

## 4. Data Flow & Lineage
The script `r_ausd_bp_ta_rn_einzeln.ksh` primarily orchestrates the execution flow.

**Legacy Flow:**
1.  **Initialization:** The script sources environment variables and utility functions for error handling, parameter parsing, and date manipulation.
2.  **Parameter Parsing:** It uses `getopts` to parse optional command-line arguments:
    *   `-s DDMMYYYY`: Specifies a cutoff date (`p_stichtag`).
    *   `-l value`: Specifies a restart value (`p_wiederanlaufWert`).
3.  **Defaulting:** If `-l` is not provided, `p_wiederanlaufWert` defaults to `0`. If `-s` is not provided, `p_stichtag` defaults to the current system date (`v_sysdate`).
4.  **Date Determination:** It calls `DWDate_Gib_Zeitraum` to get the current system date.
5.  **Validation:** It calls `pruefeParameterGesetzt` to ensure `Stichtag` is set. If validation fails, it logs an error and exits.
6.  **Logging Setup:** It initializes a job entry number (`DW_EintragsNr`) and a log file (`LogDatei`) using `DWMSG_*` functions.
7.  **Error Traps:** `trap` commands are set up to handle `INT`, `STOP`, `CONT`, and `ERR` signals, directing error handling to `DWMSG_Fehlerbehandlung`.
8.  **Job Execution:** It prints job summary information to standard output and the log file.
9.  **Core Logic Invocation:** It executes the core script `k_ausd_bp_ta_rn_einzeln.ksh` with the processed parameters (`-j JobKennung`, `-s p_stichtag`, `-f DW_EintragsNr`, `-l p_wiederanlaufWert`), redirecting its output to the log file.
10. **Completion:** Upon successful execution of the core script, it prints a success message and updates the job status using `DWMSG_SetzeStatusOK`.
11. **Exit:** Clears traps and exits with status 0.

**Target BigQuery Flow:**
1.  **Stored Procedure Invocation:** The BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` is called with `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) as parameters.
2.  **Variable Declaration and Defaulting:** Declares BigQuery variables and applies defaulting logic for `p_wiederanlaufWert` (to 0 if NULL) and `v_effective_stichtag` (to `CURRENT_DATE()` formatted as `DDMMYYYY` if `p_stichtag` is NULL or empty).
3.  **Parameter Validation:** Uses `IF` conditions and `RAISE` with an `INSERT` into `job_error_log` to simulate `pruefeParameterGesetzt` and error handling.
4.  **Job Control Entry:** Inserts a new record into `project.dataset.job_control` for tracking job execution, including `job_entry_nr`, `stichtag`, `restart_value`, and initial status.
5.  **Core Logic Execution:** Calls the BigQuery Stored Procedure for the core business logic, `project.dataset.k_ausd_bp_ta_rn_einzeln`, passing relevant parameters.
6.  **Status Update:** If the core logic executes successfully, updates the `job_control` table with a 'RUNNING' status to 'OK' and records a success message in `job_messages`.
7.  **Error Handling:** Uses a `BEGIN...EXCEPTION WHEN ERROR THEN...END` block to catch errors during the core logic execution, updating `job_control` with an 'ERROR' status and logging the error message in `job_messages`.

## 5. Transformation Logic
The transformation logic primarily involves translating shell scripting constructs and utility calls into BigQuery SQL and Stored Procedure logic.

**Parameter Handling:**
*   **Legacy (`getopts`):** Command-line arguments `-s` and `-l` are parsed.
*   **BigQuery:** These become `IN` parameters for the stored procedure (`p_stichtag STRING`, `p_wiederanlaufWert INT64`). Defaulting logic for `p_wiederanlaufWert` and `p_stichtag` is implemented using `IFNULL`.

**Date Handling:**
*   **Legacy (`DWDate_Gib_Zeitraum`):** Retrieves system date and formats it.
*   **BigQuery:** Replaced by `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`. Date comparison and handling will use BigQuery's native date functions.

**Error Handling and Logging:**
*   **Legacy (`f_alis_msgerr.ksh`, `DWMSG_*` functions, `trap`):** Custom shell functions for error messaging, log file creation, and trap-based error interception.
*   **BigQuery:** Replaced by `INSERT` statements into dedicated logging tables (`job_control`, `job_messages`, `job_error_log`). Error conditions will use `RAISE` and `EXCEPTION` blocks to manage flow and log errors. The `@@error.message` variable will capture error details.

**Orchestration:**
*   **Legacy (`${Name_Kernskript} ...`):** Direct invocation of the `k_ausd_bp_ta_rn_einzeln.ksh` shell script.
*   **BigQuery:** Replaced by a `CALL` statement to the corresponding BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_rn_einzeln`, passing the necessary parameters.

**Conditional Logic:**
*   **Legacy (`if`, `[[ ]]`):** Standard shell conditional expressions.
*   **BigQuery:** Translated to `IF...THEN...ELSE...END IF` and `ASSERT` statements within the stored procedure.

## 6. External Dependencies
The original script has several external dependencies that need to be addressed during migration:

*   **Environment Initialization:** `. $HOME/.dw_init`
    *   **Replacement:** Configuration values from this file should be explicitly passed as parameters to the BigQuery Stored Procedure or defined as constants/variables within the procedure.
*   **Utility Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error messaging)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing helpers)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date handling helpers)
    *   **Replacement:** The functionalities provided by these scripts will be re-implemented directly in BigQuery SQL using native functions, `IF` statements, and inserts into logging tables.
*   **Core Processing Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh`
    *   **Replacement:** This is a critical dependency that must be migrated to a BigQuery Stored Procedure or equivalent BigQuery SQL logic. Its input/output contracts need to be preserved or adapted for the BigQuery environment.
*   **Data Sources/Targets:** While not directly evident in `r_ausd_bp_ta_rn_einzeln.ksh` itself, the description in the `usage` function mentions "Stichtags-Abzug der Vertrags-Cache im DWH" and providing data to "Forderungsscoring." This implies the core script (`k_ausd_bp_ta_rn_einzeln.ksh`) reads from DWH tables (e.g., `DWH$TA_C_VERTRAG`) and writes to target tables for "Forderungsscoring" (FOS).
    *   **Replacement:** These legacy DWH tables will be replaced by BigQuery tables. The read/write operations will be performed via standard BigQuery SQL.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_bp_ta_rn_einzeln.ksh`) Migration:** The success of this migration heavily depends on the successful migration of the core script it invokes. The design for `k_ausd_bp_ta_rn_einzeln.ksh` is out of scope for this document but is a critical next step.
*   **Shell Traps:** Direct replication of shell `trap` functionality is not possible in BigQuery SQL. The `BEGIN...EXCEPTION` block offers a robust alternative for error handling.
*   **File-based Logging:** The shell's ability to append to log files (`>> $LogDatei`) will be replaced by structured inserts into BigQuery tables.
*   **Dynamic Path Resolution:** The use of `BERT_DIR_ROOT` for path resolution will be replaced by direct references to BigQuery dataset and table names, or BigQuery variables for configurable paths/datasets.
*   **System Date vs. Max Load Date:** The script's commented-out logic for `MIN(sysdate, maxladedatum)` suggests a potential historical intent that was not fully implemented. The migration will follow the currently implemented logic (defaulting to `sysdate` if `Stichtag` is not provided), but this divergence might warrant review.

## 8. Build Plan
1.  **Define BigQuery Audit Tables (DDL):**
    *   `project.dataset.job_control`: To store overall job execution status, parameters, and timestamps.
    *   `project.dataset.job_messages`: To log informational, warning, and success messages.
    *   `project.dataset.job_error_log`: To store detailed error information.
2.  **Migrate Utility Functionalities (BigQuery SQL):**
    *   Re-implement parameter validation logic.
    *   Translate date formatting and retrieval logic using BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()`.
3.  **Develop BigQuery Stored Procedure for `r_ausd_bp_ta_rn_einzeln.ksh`:**
    *   Create `CREATE OR REPLACE PROCEDURE project.dataset.ausd_bp_ta_rn_einzeln(...)` in BigQuery SQL, following the pseudocode provided by the CM MCP tool.
    *   Implement parameter handling, defaulting, and validation.
    *   Integrate inserts into the audit tables for logging.
    *   Include the `BEGIN...EXCEPTION` block for robust error handling.
    *   Add a `CALL` statement to invoke the `project.dataset.k_ausd_bp_ta_rn_einzeln` stored procedure (once it's migrated).
4.  **Migrate `k_ausd_bp_ta_rn_einzeln.ksh` (BigQuery SQL):**
    *   This step involves analyzing and migrating the core data processing logic of `k_ausd_bp_ta_rn_einzeln.ksh` into a separate BigQuery Stored Procedure or a series of SQL statements/views.
5.  **Testing:** Thoroughly test the migrated BigQuery Stored Procedure for functional equivalence, performance, and error handling.
6.  **Orchestration (if needed):** If external orchestration is required, develop a Cloud Composer DAG or Workflows definition to schedule and execute the BigQuery Stored Procedures.

**Example DDL for Audit Tables (conceptual):**

```sql
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_entry_nr INT64,
    job_name STRING,
    source_script STRING,
    log_name STRING,
    stichtag STRING,
    sysdate_ddmmyyyy STRING,
    restart_value INT64,
    status STRING,
    created_at TIMESTAMP,
    finished_at TIMESTAMP,
    success_message STRING,
    error_message STRING
);

CREATE TABLE IF NOT EXISTS `project.dataset.job_messages` (
    job_entry_nr INT64,
    job_name STRING,
    message_type STRING,
    message_text STRING,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_name STRING,
    error_number INT64,
    error_argument STRING,
    created_at TIMESTAMP,
    message STRING
);
```

**BigQuery Stored Procedure (as provided by tool):**

```sql
-- BigQuery Stored Procedure: orchestration wrapper for BERT base product provisioning

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_rn_einzeln`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_effective_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'AUSD_BP_TA_RN_EINZELN';
  DECLARE v_dwh_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Default restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- System date in DDMMYYYY format
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Effective cutoff date
  SET v_effective_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- Validate required parameter
  IF v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = '' THEN
    SET v_errnr = 193;
    SET v_errarg = 'Stichtag';
  END IF;

  IF v_errnr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (
      job_name,
      error_number,
      error_argument,
      created_at,
      message
    )
    VALUES
    (
      v_jobkennung,
      v_errnr,
      v_errarg,
      CURRENT_TIMESTAMP(),
      'Required parameter missing'
    );

    RAISE USING MESSAGE = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg);
  END IF;

  -- Create job entry number
  SET v_dwh_eintragsnr = (
    SELECT IFNULL(MAX(job_entry_nr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_name = v_jobkennung
  );

  -- Create log record / job entry
  SET v_logdatei = CONCAT('job_', v_jobkennung, '_', CAST(v_dwh_eintragsnr AS STRING), '.log');

  INSERT INTO `project.dataset.job_control`
  (
    job_entry_nr,
    job_name,
    source_script,
    log_name,
    stichtag,
    sysdate_ddmmyyyy,
    restart_value,
    status,
    created_at
  )
  VALUES
  (
    v_dwh_eintragsnr,
    v_jobkennung,
    'ausd_bp_ta_rn_einzeln',
    v_logdatei,
    v_effective_stichtag,
    v_sysdate,
    v_wiederanlaufWert,
    'RUNNING',
    CURRENT_TIMESTAMP()
  );

  BEGIN
    -- Downstream core logic replacement - Placeholder for migrated k_ausd_bp_ta_rn_einzeln.ksh
    CALL `project.dataset.k_ausd_bp_ta_rn_einzeln`(
      v_jobkennung,
      v_effective_stichtag,
      v_dwh_eintragsnr,
      v_wiederanlaufWert
    );

    SET v_status = 'OK';

    UPDATE `project.dataset.job_control`
    SET
      status = v_status,
      finished_at = CURRENT_TIMESTAMP(),
      success_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    WHERE job_entry_nr = v_dwh_eintragsnr
      AND job_name = v_jobkennung;

    INSERT INTO `project.dataset.job_messages`
    (
      job_entry_nr,
      job_name,
      message_type,
      message_text,
      created_at
    )
    VALUES
    (
      v_dwh_eintragsnr,
      v_jobkennung,
      'INFO',
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
      CURRENT_TIMESTAMP()
    );

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';

    UPDATE `project.dataset.job_control`
    SET
      status = v_status,
      finished_at = CURRENT_TIMESTAMP(),
      error_message = @@error.message
    WHERE job_entry_nr = v_dwh_eintragsnr
      AND job_name = v_jobkennung;

    INSERT INTO `project.dataset.job_messages`
    (
      job_entry_nr,
      job_name,
      message_type,
      message_text,
      created_at
    )
    VALUES
    (
      v_dwh_eintragsnr,
      v_jobkennung,
      'ERROR',
      @@error.message,
      CURRENT_TIMESTAMP()
    );

    RAISE;
  END;

END;
```