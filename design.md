# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This migration design document outlines the strategy for re-platforming the legacy KornShell script `r_ausd_bp_ta_bpr_beschr.ksh` from its current execution environment to Google Cloud's BigQuery.

The original KornShell script serves as an orchestration and wrapper script. Its primary purpose is to:
*   Orchestrate the initial provision of selected basic products for the BERT system.
*   Manage execution parameters such as the processing date (`Stichtag`) and a restart value (`Wiederanlaufwert`).
*   Handle logging and error reporting using a custom framework.
*   Invoke a core processing script (`k_ausd_bp_ta_bpr_beschr.ksh`) which is responsible for generating a snapshot of contract cache data from the Data Warehouse (DWH) and making it available for `Forderungsscoring`.
*   Report overall job status.

The scope of this migration focuses specifically on the `r_ausd_bp_ta_bpr_beschr.ksh` wrapper script. The core data transformation logic within `k_ausd_bp_ta_bpr_beschr.ksh` is identified as a separate, dependent migration effort.

## 2. Source Inventory
The job is primarily composed of one KornShell script.

**File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh`**
*   **Technology:** KornShell (shell)
*   **Tool:** KornShell
*   **Category:** Shell script (orchestration/wrapper)
*   **Complexity Tier:** Medium (Assumed, as `file_complexity` data was not available. Involves parameter handling, conditional logic, and external script invocation.)
*   **Migration Bucket:** B3 - Manual (Assumed, as `automation_rate` data was not available. Shell scripts often require significant manual effort to re-implement their logic and dependencies in a new platform.)
*   **Purpose:** Orchestrates the execution of a core data processing script, manages parameters, and handles logging/error reporting. It sets up the environment and calls `k_ausd_bp_ta_bpr_beschr.ksh` for actual data manipulation.

**Invoked Script: `k_ausd_bp_ta_bpr_beschr.ksh`**
*   This script is invoked by `r_ausd_bp_ta_bpr_beschr.ksh`. Its content and migration details are outside the immediate scope of this document but are recognized as a critical dependency. This script performs the actual data snapshot generation and provision to `Forderungsscoring`.

## 3. Target Architecture
The target architecture for `r_ausd_bp_ta_bpr_beschr.ksh` will leverage BigQuery's capabilities for stored procedures and a modern cloud-native orchestration framework.

*   **Core Logic:** The functionality of `r_ausd_bp_ta_bpr_beschr.ksh` will be re-implemented as a **BigQuery Stored Procedure**. This procedure will encapsulate the parameter handling, date determination, and job status management logic.
*   **Logging & Error Handling:** The custom logging and error handling framework will be replaced by dedicated **BigQuery Audit/Log Tables**. This allows for centralized, queryable logging of job status, errors, and execution details. Error reporting (e.g., email notification) would be handled by the orchestration layer or a separate Cloud Function/service.
*   **Orchestration:** The legacy UC4 scheduler will be replaced by **Cloud Composer (Apache Airflow)** or **Cloud Workflows**. This will be responsible for invoking the BigQuery Stored Procedure, managing its parameters, and monitoring its execution.
*   **Data Processing Core:** The invoked script `k_ausd_bp_ta_bpr_beschr.ksh` will need to be migrated to a separate **BigQuery Stored Procedure** or other appropriate BigQuery/PySpark solution. The `r_ausd_bp_ta_bpr_beschr.ksh` BigQuery Stored Procedure will then call this new core procedure.
*   **Helper Functions:** Common shell helper scripts (e.g., for date handling, parameter parsing) will be reimplemented as BigQuery UDFs (User-Defined Functions) or integrated directly into the stored procedures where appropriate.

**Example BigQuery Components:**
*   **Stored Procedures:**
    *   `project.dataset.ausd_bp_ta_bpr_beschr` (main wrapper SP)
    *   `project.dataset.k_ausd_bp_ta_bpr_beschr` (core processing SP - *to be migrated separately*)
*   **Tables:**
    *   `project.dataset.job_control` (for tracking job status, start/end times, parameters)
    *   `project.dataset.job_error_log` (for recording detailed error information)
*   **Orchestration:**
    *   Cloud Composer DAG (to schedule and execute `project.dataset.ausd_bp_ta_bpr_beschr`)

## 4. Data Flow & Lineage
The data flow and lineage for this job will be re-established within the Google Cloud ecosystem:

1.  **Orchestration (Cloud Composer/Workflows):** Replaces the legacy UC4 job (`DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml`) that currently invokes `r_ausd_bp_ta_bpr_beschr.ksh`. The orchestrator will trigger the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_beschr` at scheduled times, passing the necessary parameters (`p_stichtag`, `p_wiederanlaufWert`).
2.  **`project.dataset.ausd_bp_ta_bpr_beschr` (BigQuery Stored Procedure):**
    *   Receives parameters from the orchestrator.
    *   Derives `v_sysdate` (current date) and determines the effective `v_stichtag`.
    *   Initializes `v_wiederanlaufWert`.
    *   Performs parameter validation.
    *   Records a "STARTED" entry into the `project.dataset.job_control` table.
    *   Calls the core processing BigQuery Stored Procedure `project.dataset.k_ausd_bp_ta_bpr_beschr`, passing relevant execution parameters.
    *   On successful completion of the core procedure, updates the `project.dataset.job_control` table to "OK".
    *   On error, updates `project.dataset.job_control` to "ERROR" and inserts detailed error information into `project.dataset.job_error_log`.
3.  **`project.dataset.k_ausd_bp_ta_bpr_beschr` (BigQuery Stored Procedure - *Dependent Migration*):** This procedure will contain the migrated logic to:
    *   Read contract cache data from DWH source tables (likely external tables or pre-ingested BigQuery tables).
    *   Apply selection criteria (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`).
    *   Generate a snapshot of selected basic products.
    *   Provision this data to a target BigQuery table for `Forderungsscoring`. This might involve deleting/reloading existing data based on the `p_wiederanlaufWert` logic.

## 5. Transformation Logic
The original `r_ausd_bp_ta_bpr_beschr.ksh` script's transformation logic is primarily procedural and orchestration-focused rather than data-transformation focused. The migration aims to translate this procedural logic into a BigQuery Stored Procedure.

**Key Logic Points & BigQuery Equivalents:**

*   **Environment Initialization (`. $HOME/.dw_init`):** This is typically handled by environment variables in the Cloud Composer environment or by hardcoding/configuring BigQuery project/dataset names within the stored procedure or orchestrator.
*   **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters (`IN p_stichtag STRING, IN p_wiederanlaufWert INT64`).
*   **Defaulting Parameters:** Conditional logic (`IFNULL`, `CASE`) in BigQuery SQL will handle defaulting `p_wiederanlaufWert` to `0` and `p_stichtag` to the current system date if not provided.
*   **Date Determination (`DWDate_Gib_Zeitraum`):** Replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()` functions.
*   **Parameter Validation (`pruefeParameterGesetzt`):** Implemented using `IF` statements and conditional `SIGNAL SQLSTATE` for error handling within the stored procedure.
*   **Logging Framework (`DWMSG_* functions`):** Replaced by `INSERT` and `UPDATE` statements into dedicated `job_control` and `job_error_log` BigQuery tables. This provides a structured, queryable audit trail.
*   **Error Handling (`set -e`, `trap`):** Replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` block for robust error capture and status updates. Any unhandled exceptions will be propagated by `SIGNAL SQLSTATE`.
*   **Script Invocation (`${Name_Kernskript} ...`):** Replaced by a `CALL` statement to the migrated BigQuery Stored Procedure for the core logic (`CALL project.dataset.k_ausd_bp_ta_bpr_beschr(...)`).

**BigQuery SQL Pseudocode (as generated by MCP tool):**
```sql
-- BigQuery Stored Procedure: wrapper equivalent for the shell script
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE DW_EintragsNr INT64;
  DECLARE JobKennung STRING DEFAULT 'AUSD_BP_TA_BPR_BESCHR';
  DECLARE LogDatei STRING;
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.k_ausd_bp_ta_bpr_beschr';

  -- Default restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- System date equivalent in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default stichtag if not provided
  SET v_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

  -- Parameter validation equivalent
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Stichtag';
  END IF;

  IF ErrNr <> 0 THEN
    -- Replace with audit/error table insert
    INSERT INTO `project.dataset.job_error_log`
    (
      job_name,
      error_number,
      error_argument,
      created_at
    )
    VALUES
    (
      JobKennung,
      ErrNr,
      ErrArg,
      CURRENT_TIMESTAMP()
    );

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Parameter error: ', ErrArg, ', ErrNr=', CAST(ErrNr AS STRING));
  END IF;

  -- Job number generation equivalent
  SET DW_EintragsNr = (
    SELECT IFNULL(MAX(job_entry_nr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_name = JobKennung
  );

  -- Log file equivalent replaced by audit table reference
  SET LogDatei = CONCAT('job_', JobKennung, '_', CAST(DW_EintragsNr AS STRING));

  -- Create job start entry
  INSERT INTO `project.dataset.job_control`
  (
    job_entry_nr,
    job_name,
    script_name,
    log_reference,
    stichtag,
    status,
    created_at
  )
  VALUES
  (
    DW_EintragsNr,
    JobKennung,
    'ausd_bp_ta_bpr_beschr',
    LogDatei,
    v_stichtag,
    'STARTED',
    CURRENT_TIMESTAMP()
  );

  BEGIN
    -- Call core processing procedure
    CALL `project.dataset.k_ausd_bp_ta_bpr_beschr`(
      JobKennung,
      v_stichtag,
      DW_EintragsNr,
      v_wiederanlaufWert
    );

    -- Mark success
    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = DW_EintragsNr
      AND job_name = JobKennung;

  EXCEPTION WHEN ERROR THEN
    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = DW_EintragsNr
      AND job_name = JobKennung;

    INSERT INTO `project.dataset.job_error_log`
    (
      job_name,
      job_entry_nr,
      error_message,
      created_at
    )
    VALUES
    (
      JobKennung,
      DW_EintragsNr,
      @@error.message,
      CURRENT_TIMESTAMP()
    );

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = @@error.message;
  END;
END;
```

## 6. External Dependencies
The original script's external dependencies primarily consist of internal helper scripts and an orchestrator. There were no `external_systems` identified in the lineage analysis for this specific run.

*   **Legacy Orchestration (UC4):** The script `r_ausd_bp_ta_bpr_beschr.ksh` is invoked by a UC4 job (`DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml`).
    *   **Replacement:** Will be replaced by **Cloud Composer (Apache Airflow)** or **Cloud Workflows** to schedule and trigger the BigQuery Stored Procedure.
*   **Shared Helper Scripts:**
    *   `$HOME/.dw_init`: Environment initialization.
        *   **Replacement:** Not directly migrated. Environment variables or configuration management within Cloud Composer/Workflows and BigQuery.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
        *   **Replacement:** Replaced by BigQuery error handling (`EXCEPTION WHEN ERROR`) and logging to dedicated `job_error_log` table.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper.
        *   **Replacement:** Logic integrated directly into the BigQuery Stored Procedure's parameter handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helper.
        *   **Replacement:** Replaced by native BigQuery date functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`).
*   **Invoked Core Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`: The actual data processing logic.
        *   **Replacement:** This will be migrated to a separate BigQuery Stored Procedure, which the `ausd_bp_ta_bpr_beschr` procedure will then call. This is a critical dependent migration.

## 7. Unresolved / Risks
*   **Migration of Core Logic (`k_ausd_bp_ta_bpr_beschr.ksh`):** The primary data processing and transformation logic resides in the `k_ausd_bp_ta_bpr_beschr.ksh` script. This is a significant, separate migration effort and its complexity is currently unknown. The success of `r_ausd_bp_ta_bpr_beschr.ksh` migration is dependent on this core script's successful re-platforming.
*   **Missing Complexity/Automation Data:** The `file_complexity` and `automation_rate` data for `r_ausd_bp_ta_bpr_beschr.ksh` were unavailable, leading to assumptions about its complexity tier and migration bucket. This might indicate higher effort than initially assumed if hidden complexities exist.
*   **Dynamic Aspects of Shell Scripts:** While the MCP tool did a good job of translating the explicit logic, any highly dynamic aspects of the original shell script (e.g., constructing SQL queries at runtime with complex logic, extensive file system operations not apparent from the code) would need careful manual review and re-implementation.
*   **Business Logic in Helper Scripts:** If any of the sourced helper `.ksh` scripts contain critical business logic beyond simple utility functions (e.g., complex data lookups, environment-specific configurations that affect logic), these would need to be identified and appropriately migrated to BigQuery UDFs, configuration tables, or directly embedded logic.

## 8. Build Plan
The migration will follow a phased approach, focusing on foundational elements first.

1.  **Migrate Shared Helper Functions (Phase 1 - Foundational)**
    *   **Description:** Analyze helper scripts (`h_alis_date.ksh`, `h_alis_parameter.ksh`) and translate generic utilities (e.g., date formatting, simple parameter checks) into BigQuery UDFs or directly embed simple logic into target stored procedures. The error messaging logic (`f_alis_msgerr.ksh`) will be handled by the new logging tables.
    *   **Language:** BigQuery SQL
2.  **Create BigQuery Control Tables (Phase 2 - Foundational)**
    *   **Description:** Define and create the `job_control` and `job_error_log` tables in BigQuery for central job status and error tracking.
    *   **Language:** BigQuery DDL (SQL)
3.  **Migrate Core Processing Script (`k_ausd_bp_ta_bpr_beschr.ksh`) (Phase 3 - Critical Path)**
    *   **Description:** This is the most significant part of the data migration. Analyze `k_ausd_bp_ta_bpr_beschr.ksh` to identify data sources (DWH contract cache), transformation logic, and target tables (`Forderungsscoring`). Re-implement this as a BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_beschr`) or a PySpark job if complex transformations are involved. This step requires its own detailed design.
    *   **Language:** BigQuery SQL (for Stored Procedures) or Python (for PySpark/Dataflow).
4.  **Migrate Wrapper Script (`r_ausd_bp_ta_bpr_beschr.ksh`) (Phase 4 - Wrapper Implementation)**
    *   **Description:** Translate the logic of `r_ausd_bp_ta_bpr_beschr.ksh` into a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_bpr_beschr`), incorporating the BigQuery SQL pseudocode provided in Section 5. Ensure it correctly calls the migrated `k_ausd_bp_ta_bpr_beschr` procedure and updates the control tables.
    *   **Language:** BigQuery SQL
5.  **Develop Cloud Orchestration (Phase 5 - Orchestration)**
    *   **Description:** Create a Cloud Composer DAG or Cloud Workflow definition to schedule, parameterize, and invoke the `project.dataset.ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure. Configure error notifications and monitoring within the orchestration layer.
    *   **Language:** Python (for Airflow DAGs) or YAML (for Cloud Workflows).