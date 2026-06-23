# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This document outlines the migration plan for the KornShell script `k_ausd_bp_ta_bpr_beschr.ksh` to Google Cloud Platform, specifically targeting BigQuery.
The primary purpose of this script is to act as a control and orchestration component. It initializes the environment, parses and validates input parameters (job ID, entry number, reference date), performs date format checks, and then orchestrates the execution of an SQL script `d_ausd_bp_ta_bpr_beschr.sql` with the validated parameters. The job's purpose is described as "Job assembled from 1 component(s)".

## 2. Source Inventory

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`
*   **Technology:** KornShell
*   **Category:** Shell
*   **Tool:** KornShell
*   **Summary:** This ksh script acts as a control script for `r_ausd_bp_ta_bpr_beschr.ksh`. It initializes the environment, parses and validates input parameters (job ID, entry number, reference date), performs date format checks, and then orchestrates the execution of an SQL script `d_ausd_bp_ta_bpr_beschr.sql` with the validated parameters.
*   **Complexity Tier:** Not available from analysis.
*   **Migration Flags:** Not available from analysis.
*   **Automation Bucket:** semi_auto (B2)

## 3. Target Architecture
The target architecture will leverage BigQuery for data processing and storage. The KornShell script's orchestration logic will be converted into a BigQuery Stored Procedure, enabling parameter handling, date validation, and the invocation of the migrated SQL logic.
*   **Orchestration:** BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_beschr.ksh`.
*   **Data Processing:** BigQuery SQL for `d_ausd_bp_ta_bpr_beschr.sql` content, potentially within another BigQuery Stored Procedure.
*   **Data Storage:** BigQuery tables will replace legacy database tables (`DWTK_MELDUNGEN`, `PDS$TA_BPR`, `SOF$TA_BPR_BESCHR`).
*   **Logging/Auditing:** A dedicated BigQuery logging/auditing table (`project.dataset.job_run_log`, `project.dataset.job_table`) will replace temporary files and potential legacy job tables.

## 4. Data Flow & Lineage
The original job flow is as follows:
1.  **`r_ausd_bp_ta_bpr_beschr.ksh` (invoker script)** INVOKES
2.  **`k_ausd_bp_ta_bpr_beschr.ksh` (main script)**
    *   This script executes various helper scripts for environment setup, error handling, date checks, and parameter parsing.
    *   It then EXECUTES the SQL script `d_ausd_bp_ta_bpr_beschr.sql`.
3.  **`d_ausd_bp_ta_bpr_beschr.sql` (SQL script)**
    *   READS data from `TABLE:DWTK_MELDUNGEN`.
    *   READS data from `TABLE:PDS$TA_BPR`.
    *   WRITES data to `TABLE:SOF$TA_BPR_BESCHR`.
    *   USES `PACKAGE:DWPA_UTIL_SKRIPT`.

**Migrated Data Flow:**
1.  An external orchestrator (e.g., Cloud Composer, Cloud Workflows) will invoke the BigQuery Stored Procedure corresponding to `k_ausd_bp_ta_bpr_beschr.ksh`.
2.  The `k_ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure will:
    *   Receive parameters.
    *   Perform parameter and date validations using BigQuery SQL functions.
    *   Call another BigQuery Stored Procedure or execute direct SQL for the logic originally in `d_ausd_bp_ta_bpr_beschr.sql`.
3.  The `d_ausd_bp_ta_bpr_beschr` BigQuery component will:
    *   Read from migrated BigQuery tables (e.g., `dw_source.isrpt.DWTK_MELDUNGEN`, `dw_source.isrpt.PDS_TA_BPR`).
    *   Write to a target BigQuery table (e.g., `dw_target.isrpt.SOF_TA_BPR_BESCHR`).
    *   Any functionality from `PACKAGE:DWPA_UTIL_SKRIPT` will be reimplemented as BigQuery UDFs or functions within the stored procedures.
4.  Record counts and job status will be logged into BigQuery audit tables.

## 5. Transformation Logic

**`k_ausd_bp_ta_bpr_beschr.ksh` (Orchestration Logic):**
*   **Parameter Parsing (`getopts`):** Will be replaced by BigQuery Stored Procedure input parameters.
*   **Environment Sourcing (`. $HOME/.dw_init`):** Configuration will be managed via BigQuery procedure variables, project/dataset constants, or external orchestration environment variables.
*   **Helper Script Invocations (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These functionalities will be reimplemented using BigQuery SQL's native functions, `ASSERT` statements for error handling, `PARSE_DATE` for date checks, and explicit BigQuery Stored Procedure calls where complex logic from these helpers is required.
*   **Date Derivation (`gestern.ksh`):** Replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions.
*   **SQL Script Execution (`starteSQLSkript`):** The invocation of `d_ausd_bp_ta_bpr_beschr.sql` will be directly translated to calling another BigQuery Stored Procedure containing the SQL logic.
*   **Temporary File for Record Count (`tmpFile`):** Replaced by capturing the `ROW_COUNT()` from the SQL operations or returning the count as an `OUT` parameter from the called procedure. This count will then be inserted into a BigQuery audit table.
*   **Job Table Entry (`FOSJobErzeugeEintrag` - commented):** If this functionality is required, it will be implemented as an `INSERT` statement into a BigQuery audit table (`project.dataset.job_table`).

**`d_ausd_bp_ta_bpr_beschr.sql` (SQL Logic):**
*   The actual SQL statements within `d_ausd_bp_ta_bpr_beschr.sql` will be converted to BigQuery SQL.
*   `TABLE:DWTK_MELDUNGEN` and `TABLE:PDS$TA_BPR` will be migrated to BigQuery tables (e.g., `dw_source.isrpt.dwtk_meldungen`, `dw_source.isrpt.pds_ta_bpr`).
*   `TABLE:SOF$TA_BPR_BESCHR` will be migrated to a BigQuery target table (e.g., `dw_target.isrpt.sof_ta_bpr_beschr`).
*   `PACKAGE:DWPA_UTIL_SKRIPT` functions will be translated to BigQuery UDFs or equivalent native SQL constructs.

## 6. External Dependencies

*   **Legacy Oracle Database:** The source tables (`DWTK_MELDUNGEN`, `PDS$TA_BPR`, `SOF$TA_BPR_BESCHR`) currently reside in an Oracle database. These will be migrated to BigQuery tables. This involves a one-time historical data load and then continuous data ingestion for new and updated records, likely using services like Datastream, Striim, or custom data pipelines.
*   **File System Operations:** The original script relies on sourcing other KSH scripts and using temporary files for inter-process communication (e.g., `tmpFile`). In BigQuery, these dependencies will be replaced by:
    *   BigQuery Stored Procedure calls for script functionalities.
    *   BigQuery tables for temporary data storage or inter-procedure communication.
    *   Logging tables for capturing metrics like record counts.
*   **`SQLPLUS`:** The `starteSQLSkript` function likely uses `sqlplus` for executing the SQL script. This will be replaced by direct execution of BigQuery SQL or BigQuery Stored Procedures.
*   **System Commands (`cat`, `eval`, `print`, `sed`, `sort`, `join`):** The script uses standard Unix commands. Most of these (like `cat` for reading temporary files) will be absorbed into BigQuery's data handling. The commented-out `sed`, `sort`, `join` operations, if ever activated, would be migrated to BigQuery SQL transformations.

## 7. Unresolved / Risks

*   **`file_complexity` data:** The `file_complexity` information was not available for the source file. This might indicate a lack of detailed static analysis for this specific file, potentially hiding complex migration challenges not immediately apparent from the code. However, the `semi_auto` automation bucket suggests a degree of manual intervention is expected.
*   **`r_ausd_bp_ta_bpr_beschr.ksh` details:** While it's known to invoke the target script, its full functionality and whether it's part of the migration scope need to be explicitly defined. If `r_ausd_bp_ta_bpr_beschr.ksh` is a wrapper that manages a larger workflow, then the entire workflow's migration strategy should be considered.
*   **`bert_k_ausd_bp_ta_bpr_beschr.tmp`:** This temporary file is used to store the number of records. Its content and the method of populating it need to be understood fully to ensure an accurate BigQuery replacement. The design suggests capturing `ROW_COUNT()` or returning `OUT` parameters, which is a standard BigQuery practice.
*   **`FOSJobErzeugeEintrag` and `FOSJobDeaktivate`:** These are commented out but hint at a larger job management framework. The BigQuery migration should account for how job status and activation/deactivation are managed in the target environment (e.g., Cloud Composer, custom audit tables).
*   **Helper Script Reimplementation:** The exact logic within `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` needs to be fully understood to accurately translate them into BigQuery equivalent logic (UDFs, helper procedures, assertions).
*   **`gestern.ksh` and `starteSQLSkript`:** These custom shell functions/scripts need their exact behavior mapped to BigQuery built-in functions or custom BigQuery UDFs/procedures.
*   **SQL Script `d_ausd_bp_ta_bpr_beschr.sql`:** The actual transformation logic within this SQL script is crucial and needs separate detailed analysis for migration to BigQuery SQL.

## 8. Build Plan

1.  **Migrate Source Data:**
    *   Migrate historical data from Oracle tables `DWTK_MELDUNGEN`, `PDS$TA_BPR`, and `SOF$TA_BPR_BESCHR` to BigQuery tables (`dw_source.isrpt.dwtk_meldungen`, `dw_source.isrpt.pds_ta_bpr`, `dw_target.isrpt.sof_ta_bpr_beschr`). (Tool: Dataflow, Datastream, or custom ETL).
    *   Set up continuous ingestion for incremental updates to these BigQuery tables.
2.  **Translate SQL Script:**
    *   Analyze and convert the SQL logic from `d_ausd_bp_ta_bpr_beschr.sql` to BigQuery SQL.
    *   Encapsulate this logic within a BigQuery Stored Procedure, e.g., `project.dataset.d_ausd_bp_ta_bpr_beschr`.
    *   Implement any required BigQuery UDFs or functions corresponding to `PACKAGE:DWPA_UTIL_SKRIPT`.
3.  **Develop Orchestration Stored Procedure:**
    *   Create a BigQuery Stored Procedure `project.dataset.k_ausd_bp_ta_bpr_beschr` that handles:
        *   Input parameter parsing and validation (corresponding to `getopts`, `pruefeParameterGesetzt`, `DWDate_Datum_Check`).
        *   Date derivation (`gestern.ksh` equivalent).
        *   Calling `project.dataset.d_ausd_bp_ta_bpr_beschr`.
        *   Error handling (corresponding to `DWMSG_MeldeFehler`).
        *   Capturing and logging record counts.
4.  **Implement Logging/Auditing:**
    *   Create BigQuery tables for job logging, e.g., `project.dataset.job_run_log` and `project.dataset.job_table`, to replace the `tmpFile` and potential `FOSJobErzeugeEintrag` functionality.
5.  **Integrate with Workflow Orchestration:**
    *   If `r_ausd_bp_ta_bpr_beschr.ksh` is part of a larger workflow, develop a Cloud Composer DAG or Cloud Workflows definition to invoke the `project.dataset.k_ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure with the necessary parameters.
    *   Ensure proper scheduling and dependency management within the new orchestrator.

**Generated BigQuery Pseudocode provided by CM MCP (from `shellscript_to_bqsql_design` tool):**
```sql
-- BigQuery Stored Procedure / Script Pseudocode

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_Stichtag_date DATE;
  DECLARE v_restart STRING DEFAULT '0';
  DECLARE v_sql_script STRING DEFAULT 'project.dataset.d_ausd_bp_ta_bpr_beschr';
  DECLARE v_err STRING DEFAULT NULL;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err = 'Jobkennung fehlt';
    RAISE USING MESSAGE = v_err;
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET v_err = 'Stichtag fehlt';
    RAISE USING MESSAGE = v_err;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_err = 'EintragsNr fehlt';
    RAISE USING MESSAGE = v_err;
  END IF;

  -- Date validation for DDMMYYYY
  BEGIN
    SET v_Stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  EXCEPTION WHEN ERROR THEN
    SET v_err = CONCAT('Ungueltiges Datum: ', p_Stichtag);
    RAISE USING MESSAGE = v_err;
  END;

  -- Restart value defaulting
  IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN
    SET v_restart = '0';
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Derive today and yesterday
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Execute migrated SQL logic
  -- Option A: call another stored procedure containing the SQL script logic
  CALL `project.dataset.d_ausd_bp_ta_bpr_beschr`(
    p_EintragsNr,
    p_JobKennung,
    p_Stichtag,
    v_restart,
    v_TabName,
    v_datum_heute,
    v_datum_gestern
  );

  -- Capture record count from result table or logging table
  SET v_records = (
    SELECT COALESCE(MAX(record_count), 0)
    FROM `project.dataset.job_run_log`
    WHERE job_name = v_TabName
      AND entry_nr = p_EintragsNr
      AND stichtag = p_Stichtag
  );

  -- Optional job table entry replacement
  INSERT INTO `project.dataset.job_table`
  (
    tab_name,
    status_a,
    status_i,
    stichtag_from,
    stichtag_to,
    job_type,
    restart_flag,
    record_count,
    description
  )
  VALUES
  (
    v_TabName,
    'A',
    'I',
    v_Stichtag_date,
    v_Stichtag_date,
    'J',
    'N',
    v_records,
    'Initialbefuellung'
  );

END;
```