# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

## 1. Purpose & Scope

This KornShell script, `r_ausd_bp_ta_apn_carmen.ksh`, serves as an orchestration and parameter-handling wrapper for the initial provisioning of selected basic products for BERT. Its primary function is to prepare parameters, determine the processing date (Stichtag), handle restart logic, and then invoke a core processing script (`k_ausd_bp_ta_apn_carmen.ksh`). It also integrates with a custom messaging and error logging framework.

The job orchestrates the creation of a daily snapshot of contract cache data from the Data Warehouse (DWH) for use by the Forderungsscoring (FOS) system. It manages the cutoff date (`Stichtag`) to ensure data consistency and supports a restart mechanism (`Wiederanlaufwert`) to process specific subsets of contracts.

The scope of this migration design is to translate the functionality of this KornShell wrapper script to Google Cloud Platform, specifically utilizing BigQuery stored procedures for logic and BigQuery tables for logging, while identifying the dependencies and migration approach for the invoked core script.

## 2. Source Inventory

| File Name                                                         | Technology | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                              |
| :---------------------------------------------------------------- | :--------- | :-------------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh` | KornShell  | Not available   | semi_auto         | This KornShell script orchestrates the initial provisioning of selected basic products for BERT by preparing parameters and calling a core processing script. It handles date determination and error logging. It reads parameters, sets a processing date (Stichtag), manages a restart value, and invokes a core script. |

## 3. Target Architecture

The target architecture will leverage BigQuery for the core logic, data storage, and logging, and potentially Cloud Composer (Airflow) for overall workflow orchestration, replacing the existing UC4 scheduler.

*   **BigQuery Stored Procedure:** The orchestration and parameter handling logic of `r_ausd_bp_ta_apn_carmen.ksh` will be migrated into a BigQuery Stored Procedure, e.g., `project.dataset.sp_ausd_bp_ta_apn_carmen`.
*   **BigQuery Tables for Logging:** The custom shell-based logging and error handling will be replaced by dedicated BigQuery audit tables:
    *   `project.dataset.job_audit_log`: To store job start/end times, parameters, status, and messages.
    *   `project.dataset.job_error_log`: To record detailed error information.
    *   `project.dataset.job_status`: To track the current status of ongoing jobs (if real-time status updates are required).
*   **BigQuery Stored Procedure (Invoked Core Logic):** The core script `k_ausd_bp_ta_apn_carmen.ksh` will also be migrated, ideally into another BigQuery Stored Procedure, e.g., `project.dataset.sp_k_ausd_bp_ta_apn_carmen`.
*   **Cloud Composer (Airflow):** The UC4 job that currently invokes `r_ausd_bp_ta_apn_carmen.ksh` will be replaced by an Airflow DAG. This DAG will be responsible for calling the `sp_ausd_bp_ta_apn_carmen` BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The current data flow is as follows:

1.  **UC4 Job:** A UC4 job definition (`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`) invokes the `r_ausd_bp_ta_apn_carmen.ksh` script.
2.  **`r_ausd_bp_ta_apn_carmen.ksh` (Wrapper):**
    *   Loads environment variables (`$HOME/.dw_init`).
    *   Sources common shell utilities for error handling (`f_alis_msgerr.ksh`), parameter parsing (`h_alis_parameter.ksh`), and date functions (`h_alis_date.ksh`).
    *   Parses command-line parameters for `Stichtag` (`-s`) and `Wiederanlaufwert` (`-l`).
    *   Determines the actual `Stichtag` (processing date), defaulting to the current system date if not provided.
    *   Initializes job logging parameters using `DWMSG_*` functions.
    *   Sets up shell `trap` commands for robust error handling.
    *   Invokes the core script `k_ausd_bp_ta_apn_carmen.ksh` with the determined parameters.
    *   Logs the job's completion status.
3.  **`k_ausd_bp_ta_apn_carmen.ksh` (Core Script):** This script (currently unanalyzed in detail) contains the actual business logic for data extraction, transformation, and loading related to contract cache provisioning.
4.  **Logging & Error Handling:** The `DWMSG_*` functions handle logging to a file and potentially sending notifications.

In the target BigQuery architecture:

1.  **Cloud Composer DAG:** An Airflow DAG replaces the UC4 job and triggers the BigQuery Stored Procedure.
2.  **`project.dataset.sp_ausd_bp_ta_apn_carmen` (BQ Stored Procedure):**
    *   Receives `Stichtag` and `Wiederanlaufwert` as input parameters.
    *   Derives the current system date using BigQuery date functions.
    *   Performs parameter validation and defaulting logic.
    *   Inserts audit records into `project.dataset.job_audit_log` at the start and end of execution.
    *   Catches exceptions and inserts error details into `project.dataset.job_error_log`.
    *   Calls `project.dataset.sp_k_ausd_bp_ta_apn_carmen` (the migrated core logic).
    *   Updates `project.dataset.job_status` as needed.
3.  **`project.dataset.sp_k_ausd_bp_ta_apn_carmen` (BQ Stored Procedure):** This procedure will contain the migrated business logic from the original `k_ausd_bp_ta_apn_carmen.ksh`, performing data operations on BigQuery tables.

## 5. Transformation Logic

The KornShell wrapper script's logic will be transformed into a BigQuery Stored Procedure using SQL scripting capabilities.

**Original Logic (KornShell) to Target (BigQuery Stored Procedure):**

*   **Environment Initialization (`. $HOME/.dw_init`):** Environment variables will be replaced by explicit parameters passed to the BigQuery Stored Procedure, or configurable values within the BigQuery project/dataset settings.
*   **Parameter Parsing (`getopts` for `-s` Stichtag, `-l` Wiederanlaufwert):** This will translate directly to `IN` parameters of the BigQuery Stored Procedure.
    *   `p_stichtag STRING` (format DDMMYYYY)
    *   `p_wiederanlaufWert INT64`
*   **Defaulting `Wiederanlaufwert`:** The `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0` logic will be implemented using `IFNULL(p_wiederanlaufWert, 0)` in BigQuery SQL.
*   **System Date Determination (`DWDate_Gib_Zeitraum`):** Will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', ...)` functions.
*   **Defaulting `Stichtag`:** The `if [[ -z "$p_stichtag" ]] then p_stichtag=$v_sysdate` logic will use `IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate)`.
*   **Parameter Validation (`pruefeParameterGesetzt`):** This will be incorporated into `IF` conditions within the stored procedure, raising a `SIGNAL SQLSTATE` (or equivalent error handling) if validation fails.
*   **Job Logging and Messaging (`DWMSG_*` functions):** These will be replaced by `INSERT` statements into `job_audit_log` and `job_error_log` BigQuery tables. This includes recording job start, end, status, and detailed error messages.
*   **Error Trapping (`trap`):** The `trap` mechanism will be handled by BigQuery's `BEGIN...EXCEPTION...END` blocks for error management and by the orchestration layer (Cloud Composer) for retries or upstream error handling.
*   **Invoking Core Script (`${Name_Kernskript} ...`):** This will be replaced by a `CALL` statement to the migrated BigQuery Stored Procedure for the core logic, i.e., `CALL project.dataset.sp_k_ausd_bp_ta_apn_carmen(...)`.

**BigQuery SQL Pseudocode (Wrapper Stored Procedure):**

```sql
-- BigQuery Stored Procedure: wrapper orchestration for "Bereitstellung Basisprodukte BERT"

CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_bp_ta_apn_carmen`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_apn_carmen';
  DECLARE v_job_nr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Initialize restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Current system date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default cutoff date if not provided
  SET v_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- Validate required parameter
  IF v_stichtag IS NULL OR TRIM(v_stichtag) = '' THEN
    SET v_errnr = 193;
    SET v_errarg = 'Stichtag';
  END IF;

  -- Error handling (simulated DWMSG_MeldeFehler)
  IF v_errnr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (
      job_kennung,
      error_nr,
      error_arg,
      log_ts,
      message
    )
    VALUES
    (
      v_jobkennung,
      v_errnr,
      v_errarg,
      CURRENT_TIMESTAMP(),
      'Required parameter missing or invalid'
    );

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg);
  END IF;

  -- Create job number and log file reference equivalent (simulated DWMSG_ErmittleNr, DWMSG_Logdateiname)
  SET v_job_nr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `project.dataset.job_audit_log`
  );

  SET v_logdatei = CONCAT('job_', v_jobkennung, '_', CAST(v_job_nr AS STRING), '.log');

  -- Insert job start audit record (simulated DWMSG_ErzeugeEintrag)
  INSERT INTO `project.dataset.job_audit_log`
  (
    job_nr,
    job_kennung,
    source_name,
    log_ref,
    stichtag,
    sysdate_ddmmyyyy,
    status,
    created_ts
  )
  VALUES
  (
    v_job_nr,
    v_jobkennung,
    'sp_ausd_bp_ta_apn_carmen',
    v_logdatei,
    v_stichtag,
    v_sysdate,
    'STARTED',
    CURRENT_TIMESTAMP()
  );

  -- Main logic with error handling (simulated trap)
  BEGIN
    -- Main orchestration placeholder:
    -- Replace shell invocation of k_ausd_bp_ta_apn_carmen.ksh with a stored procedure call
    CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(
      v_jobkennung,
      v_stichtag,
      v_job_nr,
      v_wiederanlaufWert
    );

    SET v_status = 'OK';

    -- Insert success audit record (simulated DWMSG_SetzeStatusOK)
    INSERT INTO `project.dataset.job_audit_log`
    (
      job_nr,
      job_kennung,
      source_name,
      log_ref,
      stichtag,
      sysdate_ddmmyyyy,
      status,
      created_ts,
      message
    )
    VALUES
    (
      v_job_nr,
      v_jobkennung,
      'sp_ausd_bp_ta_apn_carmen',
      v_logdatei,
      v_stichtag,
      v_sysdate,
      'SUCCESS',
      CURRENT_TIMESTAMP(),
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    );

    -- Update job status (if job_status table is used)
    UPDATE `project.dataset.job_status`
    SET
      status = 'OK',
      updated_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';

    -- Insert error audit record (simulated DWMSG_Fehlerbehandlung)
    INSERT INTO `project.dataset.job_error_log`
    (
      job_nr,
      job_kennung,
      error_nr,
      error_arg,
      log_ts,
      message
    )
    VALUES
    (
      v_job_nr,
      v_jobkennung,
      COALESCE(ERROR_CODE(), v_errnr), -- Use actual error code if available, otherwise script's error code
      COALESCE(ERROR_MESSAGE(), v_errarg), -- Use actual error message if available
      CURRENT_TIMESTAMP(),
      'AppError: Abbruch'
    );

    -- Update job status (if job_status table is used)
    UPDATE `project.dataset.job_status`
    SET
      status = 'ERROR',
      updated_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr;

    RAISE USING MESSAGE = CONCAT('AppError: Abbruch - ', COALESCE(ERROR_MESSAGE(), 'Unknown error'));
  END;

END;
```

## 6. External Dependencies

*   **UC4 Scheduler:** The current orchestration is driven by a UC4 job definition (`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`). This will be replaced by a Cloud Composer (Airflow) DAG.
*   **Filesystem-based configuration (`$HOME/.dw_init`):** Environment variables will be explicitly defined as parameters in the Airflow DAG or configured within the BigQuery Stored Procedure.
*   **Filesystem-based Utility Scripts:**
    *   `f_alis_msgerr.ksh` (Error messaging): To be replaced by BigQuery's error handling constructs (`EXCEPTION WHEN ERROR`) and logging to audit tables.
    *   `h_alis_parameter.ksh` (Parameter parsing): Functionality integrated directly into the BigQuery Stored Procedure's parameter handling.
    *   `h_alis_date.ksh` (Date handling): Functionality replaced by BigQuery's native date and time functions.
*   **Core Processing Script (`k_ausd_bp_ta_apn_carmen.ksh`):** This is a critical dependency. Its migration is paramount and will likely involve a separate design and implementation effort, resulting in a BigQuery Stored Procedure (`sp_k_ausd_bp_ta_apn_carmen`) or a series of BigQuery SQL statements.
*   **External Systems:** The `lineage_assembled_jobs` query showed no direct external system dependencies for this specific job, implying that any such interactions are handled within the core script (`k_ausd_bp_ta_apn_carmen.ksh`) or indirectly through DWH tables.

## 7. Unresolved / Risks

*   **Complexity Tier and Migration Flags:** The `file_complexity` data was not available, meaning a detailed, pre-assessed complexity and specific migration challenges for this script are unknown. This could introduce unforeseen complexities during implementation.
*   **Core Script Logic (`k_ausd_bp_ta_apn_carmen.ksh`):** The internal logic, data sources, and targets of the invoked core script are not part of this design. Its migration design and complexity are currently unresolved and represent the largest risk. It's assumed to contain the actual data processing logic, which could involve complex SQL, file operations, or other shell commands that require careful translation to BigQuery SQL, Python, or other GCP services.
*   **`DWMSG_*` Framework Replacement:** While audit tables are proposed for logging, the full scope of the `DWMSG_*` framework (e.g., specific alert mechanisms, integration with monitoring systems) needs to be mapped and implemented on GCP (e.g., Cloud Logging, Cloud Monitoring, Pub/Sub for alerts).
*   **"MAX(ladedatum)" logic in Stichtag determination:** The `usage` description mentions `MIN(sysdate,maxladedatum)` for synchronization, but the current script defaults to `sysdate` if `-s` is not provided. This discrepancy might indicate a hidden business rule or a potential for data integrity issues if `maxladedatum` is critical for correct `Stichtag` calculation. This requires clarification.
*   **`trap` functionality:** The direct translation of `trap` (for `INT`, `STOP`, `CONT`, `ERR`) to BigQuery `EXCEPTION` blocks handles errors, but nuances like graceful shutdowns or specific signal handling might need to be addressed at the orchestration level (Cloud Composer).

## 8. Build Plan

The migration will be executed in phases:

1.  **Foundation Setup (BigQuery):**
    *   Create `project.dataset.job_audit_log` (BigQuery Table) - DDL
    *   Create `project.dataset.job_error_log` (BigQuery Table) - DDL
    *   Create `project.dataset.job_status` (BigQuery Table) - DDL
    *   Define necessary BigQuery datasets (`project.dataset`).
2.  **Wrapper Script Migration (BigQuery Stored Procedure):**
    *   Develop `project.dataset.sp_ausd_bp_ta_apn_carmen` (BigQuery Stored Procedure) based on the transformation logic outlined in Section 5. (Language: BigQuery SQL)
3.  **Core Script Migration (Dependent Task):**
    *   **Prioritize a separate detailed analysis and design** for `k_ausd_bp_ta_apn_carmen.ksh`.
    *   Develop `project.dataset.sp_k_ausd_bp_ta_apn_carmen` (BigQuery Stored Procedure) or equivalent BigQuery SQL scripts/Python tasks based on the core script's functionality. (Language: BigQuery SQL / Python)
4.  **Orchestration Migration (Cloud Composer / Airflow):**
    *   Design and implement a Cloud Composer (Airflow) DAG to replace the UC4 scheduler for this job. (Language: Python)
    *   The DAG will be responsible for calling `project.dataset.sp_ausd_bp_ta_apn_carmen` with appropriate parameters.
5.  **Logging and Monitoring Integration:**
    *   Configure Cloud Logging to capture logs from BigQuery Stored Procedure executions.
    *   Set up Cloud Monitoring alerts based on BigQuery audit table entries or Cloud Logging.
6.  **Testing:**
    *   Unit tests for `sp_ausd_bp_ta_apn_carmen`.
    *   Integration tests with `sp_k_ausd_bp_ta_apn_carmen` (once available).
    *   End-to-end testing with the Cloud Composer DAG.