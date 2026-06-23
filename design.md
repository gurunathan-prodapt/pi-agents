# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bpr_apn.ksh`.
The script serves as an orchestration wrapper for the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system. Its primary responsibilities include parsing command-line arguments (reference date, restart value), setting up the execution environment by sourcing common utility scripts, handling logging and error management, and finally invoking a core processing script, `k_ausd_bp_ta_bpr_apn.ksh`, with the prepared parameters.

The job's purpose, as described in `lineage_assembled_jobs`, is "Job assembled from 1 component(s); stage dist: medium=1". The `file_analysis` summary states: "This KornShell script orchestrates the initial provision of selected base products (e.g., FAX, Data24) for BERT. It parses command-line arguments for a reference date and a restart value, sets up the environment, and then calls a core processing script."

## 2. Source Inventory
The job consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh`
    *   **Technology:** KornShell
    *   **Tool:** KornShell
    *   **Category:** shell
    *   **Purpose:** Orchestration, parameter handling, logging, error trapping, and invocation of a core business logic script.
    *   **Complexity Tier:** Not available (query returned "No rows.")
    *   **Migration Automation Bucket:** semi_auto

## 3. Target Architecture
The migration target platform is Google BigQuery. The KornShell script will be refactored into a BigQuery Stored Procedure, and its orchestration role might be managed by an external orchestrator like Cloud Composer (Airflow), Cloud Workflows, or BigQuery Scheduled Queries.

The key BigQuery components will include:
*   **Main Stored Procedure:** `project.dataset.ausd_bp_ta_bpr_apn` (equivalent to the `r_ausd_bp_ta_bpr_apn.ksh` script). This procedure will handle parameter validation, defaulting, and call the migrated "kernel" logic.
*   **Kernel Stored Procedure:** `project.dataset.k_ausd_bp_ta_bpr_apn` (migrated logic from `k_ausd_bp_ta_bpr_apn.ksh`). This is where the actual data processing (e.g., SQL statements for selecting, inserting, or updating data) will reside.
*   **Audit/Log Table:** `project.dataset.job_audit_log` for tracking job execution status, parameters, and error messages.
*   **Optional Configuration Table:** `project.dataset.job_config` for runtime parameters, if needed.
*   **Target Tables:** Tables where the `k_ausd_bp_ta_bpr_apn.ksh` logic will read from and write to (e.g., `DWH_VERTRAG_ID`).

## 4. Data Flow & Lineage
The original lineage indicates that this KornShell script (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh`) is invoked by an UC4 XML job definition:
*   `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_APN.xml` **INVOKES** `SCRIPT:R_AUSD_BP_TA_BPR_APN.KSH`

The KornShell script itself, based on its code, performs the following logical flow:
1.  Loads environment variables and utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2.  Parses input parameters (`-s` for Stichtag, `-l` for Wiederanlaufwert).
3.  Defaults the restart value to `0` if not provided.
4.  Determines the current system date (`v_sysdate`).
5.  Defaults `Stichtag` to `v_sysdate` if not explicitly provided.
6.  Validates required parameters.
7.  Initializes logging mechanisms (`DWMSG_*` functions).
8.  Sets up error traps.
9.  Invokes the core kernel script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` with parsed and defaulted parameters.
10. Records job success or failure.

In the BigQuery target, this translates to:
*   **UC4 Invocation:** The UC4 job will be migrated to an Airflow DAG or a BigQuery Scheduled Query that executes the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_apn`.
*   **Orchestration and Parameter Passing:** The main BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_apn` will receive parameters, validate them, apply defaults, and then call `project.dataset.k_ausd_bp_ta_bpr_apn` (the migrated kernel logic) directly within BigQuery.
*   **Data Flow within BigQuery:** The `k_ausd_bp_ta_bpr_apn` procedure will handle the actual `READS` and `WRITES` to BigQuery tables, processing data based on the `Stichtag` and `Wiederanlaufwert`.

## 5. Transformation Logic
The transformation of `r_ausd_bp_ta_bpr_apn.ksh` to BigQuery SQL involves re-implementing its shell scripting constructs and utility calls into BigQuery's SQL procedural language.

**Key Transformations:**

*   **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters (`IN p_stichtag STRING`, `IN p_wiederanlaufWert INT64`).
*   **Defaulting Parameters:** Shell `if [[ -z ... ]]` constructs are replaced by `IFNULL` functions (e.g., `SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);`).
*   **Date Handling (`DWDate_Gib_Zeitraum`):** Replaced by BigQuery date functions like `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Parameter Validation (`pruefeParameterGesetzt`):** Replaced by BigQuery `ASSERT` statements to ensure required parameters are present and in the correct format (e.g., `ASSERT v_stichtag IS NOT NULL AND v_stichtag != '' AS 'Stichtag must be set';`).
*   **Logging (`DWMSG_*` functions):** Replaced by `INSERT` statements into a BigQuery audit/log table (`project.dataset.job_audit_log`), recording job start, progress, and completion status.
*   **Error Trapping (`trap`):** Replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block for robust error handling, catching exceptions and logging them to the audit table before re-raising.
*   **External Script Invocation (`${Name_Kernskript}`):** The invocation of `k_ausd_bp_ta_bpr_apn.ksh` is replaced by a `CALL` statement to the corresponding BigQuery Stored Procedure (`CALL project.dataset.k_ausd_bp_ta_bpr_apn(...)`).

**Pseudocode for BigQuery Stored Procedure:**

```sql
-- BigQuery Script / Stored Procedure Pseudocode
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_apn`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_prog_name STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
  DECLARE v_prog_version STRING DEFAULT 'V2.0.0';
  DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_bpr_apn';
  DECLARE v_job_nr INT64;
  DECLARE v_log_datei STRING;
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_err_msg STRING DEFAULT NULL;

  -- Initialize defaults
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Derive system date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default stichtag if not provided
  SET v_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- Validate required parameter
  ASSERT v_stichtag IS NOT NULL AND v_stichtag != '' AS 'Stichtag must be set';

  -- Optional: validate date format DDMMYYYY
  ASSERT SAFE.PARSE_DATE('%d%m%Y', v_stichtag) IS NOT NULL AS 'Invalid Stichtag format';

  -- Create job number and log reference (placeholder logic for audit table)
  SET v_job_nr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `project.dataset.job_audit_log`
    WHERE job_kennung = v_job_kennung
  );

  SET v_log_datei = CONCAT('job_', v_job_kennung, '_', CAST(v_job_nr AS STRING), '.log');

  -- Insert audit start record
  INSERT INTO `project.dataset.job_audit_log` (
    job_nr,
    job_kennung,
    prog_name,
    prog_version,
    log_datei,
    stichtag,
    status,
    message,
    created_at
  )
  VALUES (
    v_job_nr,
    v_job_kennung,
    v_prog_name,
    v_prog_version,
    v_log_datei,
    v_stichtag,
    'STARTED',
    'Job started',
    CURRENT_TIMESTAMP()
  );

  BEGIN
    -- Core migrated logic placeholder:
    -- Equivalent to calling ${Name_Kernskript} with parameters
    CALL `project.dataset.k_ausd_bp_ta_bpr_apn`(
      v_job_kennung,
      v_stichtag,
      v_job_nr,
      v_wiederanlaufWert
    );

    -- Mark success
    INSERT INTO `project.dataset.job_audit_log` (
      job_nr,
      job_kennung,
      prog_name,
      prog_version,
      log_datei,
      stichtag,
      status,
      message,
      created_at
    )
    VALUES (
      v_job_nr,
      v_job_kennung,
      v_prog_name,
      v_prog_version,
      v_log_datei,
      v_stichtag,
      'OK',
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
      CURRENT_TIMESTAMP()
    );

  EXCEPTION WHEN ERROR THEN
    SET v_err_msg = @@error.message;

    INSERT INTO `project.dataset.job_audit_log` (
      job_nr,
      job_kennung,
      prog_name,
      prog_version,
      log_datei,
      stichtag,
      status,
      message,
      created_at
    )
    VALUES (
      v_job_nr,
      v_job_kennung,
      v_prog_name,
      v_prog_version,
      v_log_datei,
      v_stichtag,
      'ERROR',
      v_err_msg,
      CURRENT_TIMESTAMP()
    );

    RAISE USING MESSAGE = CONCAT('AppError: Abbruch - ', v_err_msg);
  END;
END;
```

## 6. External Dependencies
The `lineage_assembled_jobs` indicated no external systems, and `lineage_external_systems` query for this run was also empty. However, the script itself references several external components:

*   **Environment Sourcing:** `. $HOME/.dw_init`
    *   **Replacement:** Environment variables and configurations should be managed through BigQuery `OPTIONS` in Stored Procedures, configuration tables, or passed as parameters by the orchestrator (e.g., Airflow variables).
*   **Utility Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing/validation.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utilities.
    *   **Replacement:** These functionalities are re-implemented directly using BigQuery's built-in SQL functions (`ASSERT`, `IFNULL`, date functions like `FORMAT_DATE`, `CURRENT_DATE`), and explicit error handling (`EXCEPTION WHEN ERROR`).
*   **Core Processing Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh`
    *   **Replacement:** This script represents the main business logic and will be migrated as a separate BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_apn`). Its internal dependencies (e.g., database tables) will become BigQuery datasets and tables.
*   **Logging:** Output redirected to `$LogDatei`
    *   **Replacement:** Centralized logging to a dedicated BigQuery audit table (`project.dataset.job_audit_log`) for structured and queryable logs.
*   **UC4 Scheduler:** The script is invoked by an UC4 job.
    *   **Replacement:** The UC4 scheduler will be replaced by Google Cloud Composer (Apache Airflow), Cloud Workflows, or BigQuery Scheduled Queries.

## 7. Unresolved / Risks
*   **Missing Complexity Tier:** The `file_complexity` query for this file returned no rows. This means the detailed complexity analysis (e.g., `tier`, `migration_flags`) is unavailable, which might hide specific migration challenges not immediately apparent from the code.
*   **Kernel Script Logic:** The current design focuses on the wrapper script. The core business logic residing in `k_ausd_bp_ta_bpr_apn.ksh` is crucial and needs its own detailed migration design. This includes identifying all tables read/written by that script and complex SQL or shell logic within it.
*   **External Command Execution:** If the kernel script (`k_ausd_bp_ta_bpr_apn.ksh`) or any of the sourced utility scripts perform non-SQL operations (e.g., file system manipulation, calling external binaries not directly migratable to BigQuery SQL), these will require separate solutions (e.g., Cloud Functions, Dataflow, or custom Python operators in Airflow).
*   **Environment Variables:** While `. $HOME/.dw_init` is replaced, any critical configuration parameters set by this file must be explicitly identified and integrated into the BigQuery solution (e.g., as stored procedure default values, configuration table entries, or orchestrator variables).
*   **Date Format Assumptions:** The script assumes `DDMMYYYY` for `Stichtag`. This format should be consistently maintained or explicitly converted if the target BigQuery date formats differ.
*   **`FOSHoleLadedatum`:** The commented-out line `FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum` hints at a specific function for retrieving a load date from a table. If this function is part of `h_alis_fos_date.ksh` (also commented out), its logic needs to be migrated if it becomes active.

## 8. Build Plan
The build plan focuses on implementing the BigQuery Stored Procedure for the orchestration logic.

1.  **Define BigQuery Dataset:** Create a new BigQuery dataset (e.g., `project.dataset`) to house the migrated procedures and audit tables.
2.  **Create Audit Table:** Develop the DDL for the `project.dataset.job_audit_log` table to store job execution details (job_nr, job_kennung, status, messages, timestamps, etc.).
3.  **Develop `k_ausd_bp_ta_bpr_apn` (Kernel) Stored Procedure:**
    *   Analyze the content of `k_ausd_bp_ta_bpr_apn.ksh`.
    *   Migrate its core business logic (SQL queries, data manipulations) into a BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_bpr_apn`.
    *   Identify and define all input parameters and any tables read/written by this procedure.
    *   (This is a prerequisite for step 4, but its detailed design is out of scope for *this* document.)
4.  **Develop `ausd_bp_ta_bpr_apn` (Wrapper) Stored Procedure:**
    *   Write the BigQuery SQL for the `project.dataset.ausd_bp_ta_bpr_apn` stored procedure, implementing the logic detailed in Section 5.
    *   Ensure proper parameter handling, defaulting, validation (`ASSERT`), and error handling (`BEGIN...EXCEPTION`).
    *   Integrate `INSERT` statements to the `job_audit_log` table for status tracking.
    *   Include the `CALL` to `project.dataset.k_ausd_bp_ta_bpr_apn`.
5.  **Develop Orchestration (e.g., Airflow DAG):**
    *   Create an Airflow DAG (Python) or a BigQuery Scheduled Query to trigger the `project.dataset.ausd_bp_ta_bpr_apn` stored procedure.
    *   Configure parameter passing (e.g., `stichtag`, `wiederanlaufWert`) from the orchestrator to the BigQuery procedure.
6.  **Testing:** Thoroughly test the BigQuery Stored Procedure with various inputs, including valid, invalid, and missing parameters, and verify correct logging and error handling. Validate the invocation of the kernel procedure.

**Languages:**
*   DDL for `job_audit_log`: BigQuery SQL
*   Stored Procedures (`ausd_bp_ta_bpr_apn`, `k_ausd_bp_ta_bpr_apn`): BigQuery SQL (using procedural language features)
*   Orchestration: Python (for Cloud Composer/Airflow DAG) or BigQuery SQL (for Scheduled Queries).