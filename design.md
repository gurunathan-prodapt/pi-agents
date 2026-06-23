# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

## 1. Purpose & Scope
This migration job, `k_ausd_bp_ta_bpr_instance.ksh`, is a KornShell script designed to orchestrate a data preparation process. Its primary purpose is to validate input parameters, perform date checks, and execute an underlying SQL script (`d_ausd_bp_ta_bpr_instance.sql`) for populating or updating a data pool, specifically referenced as 'PoolBasisprodukt'. The script handles environment setup, error logging, and captures record counts from the executed SQL. The migration aims to translate this orchestration and data processing logic to Google Cloud's BigQuery platform.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh`
    *   **Technology:** KornShell Script
    *   **Tier:** (Not explicitly available from `file_complexity`, but implied by its orchestration role as potentially 'medium')
    *   **Automation Bucket:** `semi_auto`
    *   **Description:** This script orchestrates the execution of an SQL script for data preparation, handling parameter parsing, validation, and environment setup. It manages input parameters `Jobkennung`, `EintragsNr`, `Stichtag`, and an optional `wiederanlaufWert`. It sources several utility scripts for common functions like error handling, date validation, and SQLPlus invocation. The core data manipulation is delegated to an external SQL file.

## 3. Target Architecture

The target architecture in BigQuery will involve:

*   **BigQuery Stored Procedure:** The main orchestration logic of `k_ausd_bp_ta_bpr_instance.ksh` will be migrated into a BigQuery stored procedure. This procedure will handle parameter validation, date checks, and the invocation of the core data processing logic.
    *   **Naming Convention:** `project.dataset.r_ausd_bp_ta_bpr_instance`
*   **Dependent BigQuery Stored Procedure/SQL Files:** The SQL script `d_ausd_bp_ta_bpr_instance.sql` will be converted into a separate BigQuery stored procedure or a series of SQL files executed within the main procedure.
    *   **Naming Convention:** `project.dataset.d_ausd_bp_ta_bpr_instance`
*   **Logging Tables:**
    *   `project.dataset.job_error_log`: To capture errors and messages, replacing the legacy `f_alis_msgerr.ksh` and direct `print` statements.
    *   `project.dataset.job_run_log`: To log job execution details, including record counts, replacing the temporary file `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_instance.tmp` and the commented-out `FOSJobErzeugeEintrag` logic.
*   **Target Data Tables:** The `PoolBasisprodukt` and any other tables manipulated by `d_ausd_bp_ta_bpr_instance.sql` will reside in BigQuery as standard tables (e.g., `project.dataset.target_table`).
*   **Orchestration (Optional):** If external scheduling or complex inter-job dependencies exist, Cloud Composer (Airflow) could be used to trigger the BigQuery stored procedure.

## 4. Data Flow & Lineage

The current data flow involves:

1.  **Environment Setup:** Sourcing `$HOME/.dw_init` and various utility scripts from `${BERT_DIR_ROOT}/allgemein/is/util/bin/`.
2.  **Parameter Input:** The script accepts `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` via command-line arguments.
3.  **Parameter Validation:** `h_alis_parameter.ksh` is used to check if required parameters are set.
4.  **Date Validation:** `h_alis_date.ksh` (specifically `DWDate_Datum_Check`) validates the format of `p_Stichtag`.
5.  **Date Derivation:** `gestern.ksh` is invoked to derive yesterday's and today's dates.
6.  **SQL Execution:** `starteSQLSkript` (from `h_alis_sqlplus.ksh`) is called to execute `d_ausd_bp_ta_bpr_instance.sql` with a set of parameters.
7.  **Record Count:** The `tmpFile` captures the record count, which is then read back into `v_records`.
8.  **Logging (Commented):** Legacy job logging (`FOSJobErzeugeEintrag`) is present but commented out.

**Migrated Data Flow:**

1.  **BigQuery Stored Procedure Call:** An external scheduler (e.g., Cloud Composer, Cloud Scheduler) or manual trigger invokes the `r_ausd_bp_ta_bpr_instance` stored procedure, passing in `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as arguments.
2.  **Parameter & Date Validation:** The stored procedure handles parameter existence and `DDMMYYYY` date format validation using BigQuery scripting logic. Errors are logged to `job_error_log`.
3.  **Date Derivation:** `CURRENT_DATE()` and `DATE_SUB()` BigQuery functions replace `gestern.ksh`.
4.  **SQL Logic Execution:** The `d_ausd_bp_ta_bpr_instance` stored procedure is called from the main orchestration procedure, executing the core business logic.
5.  **Record Count & Logging:** After the data processing, the stored procedure queries the target table to get the record count and inserts this, along with other run details, into `job_run_log`.

## 5. Transformation Logic

The KornShell script's logic will be transformed into BigQuery SQL scripting within stored procedures.

*   **Parameter Parsing and Validation:**
    *   **Legacy:** `getopts` and `pruefeParameterGesetzt` from `h_alis_parameter.ksh`.
    *   **Target:** BigQuery stored procedure parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) combined with `IF` conditions and `IS NULL`/`TRIM` checks. Errors will insert records into `project.dataset.job_error_log`.
*   **Date Validation:**
    *   **Legacy:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` from `h_alis_date.ksh`.
    *   **Target:** `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` will convert the input string to a DATE type. If conversion fails, it indicates an invalid format, and an error will be logged to `job_error_log`.
*   **Restart Value Initialization:**
    *   **Legacy:** `if [[ -z "$p_wiederanlaufWert" ]]; then p_wiederanlaufWert=0; fi`
    *   **Target:** BigQuery `IF` statement: `IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN SET v_restart_value = '0'; ELSE SET v_restart_value = p_wiederanlaufWert; END IF;`
*   **Date Derivation (Yesterday/Today):**
    *   **Legacy:** `set `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh``
    *   **Target:** BigQuery `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` will be used to derive `v_datum_heute` and `v_datum_gestern`.
*   **SQL Script Execution:**
    *   **Legacy:** `starteSQLSkript $p_EintragsNr $Name_SQLskript ...` which likely invokes SQLPlus. The SQL content is in `d_ausd_bp_ta_bpr_instance.sql`.
    *   **Target:** The content of `d_ausd_bp_ta_bpr_instance.sql` will be migrated into a separate BigQuery stored procedure (e.g., `project.dataset.d_ausd_bp_ta_bpr_instance`). The main orchestration procedure will then call this dependent procedure: `CALL `project.dataset.d_ausd_bp_ta_bpr_instance`(...);`.
*   **Record Count:**
    *   **Legacy:** `eval "v_records=`cat $tmpFile`"`
    *   **Target:** A `DECLARE` variable `v_records INT64;` will store the count, obtained by `SET v_records = (SELECT COUNT(*) FROM `project.dataset.target_table` WHERE ...);` after the data processing.
*   **Commented-out Post-processing (sed, sort, join):**
    *   These operations, if reactivated or found to be necessary, will be translated into BigQuery SQL. `sed`'s `s/\ //g` (remove spaces) can be `REPLACE(raw_line, ' ', '')`. `sort -u -k 1 -t ';'` (unique sort by first field, semicolon delimiter) can be implemented using `DISTINCT` and `ORDER BY` with appropriate string manipulation. `join` operations are directly transferable to `JOIN` clauses in BigQuery SQL, specifically `FULL OUTER JOIN` and `LEFT JOIN` as indicated in the MCP pseudocode. This suggests transforming file-based data into BigQuery tables for these operations.

**BQ SQL Pseudocode Snippet (main procedure):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value STRING DEFAULT '0';

  -- Parameter checks and error logging (simplified)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN SET v_err_nr = 1; SET v_err_arg = 'Jobkennung'; END IF;
  -- ... similar checks for Stichtag, EintragsNr ...
  IF v_err_nr <> 0 THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, error_code, error_arg, created_at) VALUES ('r_ausd_bp_ta_bpr_instance', v_err_nr, v_err_arg, CURRENT_TIMESTAMP());
    LEAVE;
  END IF;

  -- Date validation
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, error_code, error_arg, created_at) VALUES ('r_ausd_bp_ta_bpr_instance', 193, p_Stichtag, CURRENT_TIMESTAMP());
    LEAVE;
  END IF;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN SET v_restart_value = '0'; ELSE SET v_restart_value = p_wiederanlaufWert; END IF;

  -- Yesterday and today
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Execute migrated SQL logic (d_ausd_bp_ta_bpr_instance)
  CALL `project.dataset.d_ausd_bp_ta_bpr_instance`(
    p_EintragsNr, p_JobKennung, p_Stichtag, v_restart_value, v_datum_heute, v_datum_gestern
  );

  -- Record count replacement for temp file
  SET v_records = (SELECT COUNT(*) FROM `project.dataset.target_table` WHERE business_date = v_stichtag_date);

  -- Log successful run
  INSERT INTO `project.dataset.job_run_log` (job_name, job_id, entry_nr, business_date, record_count, status, created_at)
  VALUES ('r_ausd_bp_ta_bpr_instance', p_JobKennung, p_EintragsNr, v_stichtag_date, v_records, 'SUCCESS', CURRENT_TIMESTAMP());

END;
```

## 6. External Dependencies

The `lineage_assembled_jobs` record indicated no explicit external systems. However, the script itself reveals several dependencies that need to be addressed:

*   **Legacy Sourced Scripts (`.ksh` files):**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date validation utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQLPlus invocation wrapper.
    *   **Replacement:** The functionalities of these scripts will be re-implemented directly in BigQuery SQL scripting (for parameter validation, date checks), replaced by BigQuery's native error handling (`RAISE`, logging to tables), or become obsolete (e.g., SQLPlus wrapper). Environment variables will be replaced by BigQuery dataset/project references or procedure parameters.
*   **External Shell Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Script to get yesterday's and today's dates.
    *   **Replacement:** Replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions.
*   **External SQL Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql`: The core data manipulation logic.
    *   **Replacement:** This SQL script will be fully migrated to a BigQuery stored procedure (`project.dataset.d_ausd_bp_ta_bpr_instance`) or a series of SQL statements within the main stored procedure. Manual conversion of its SQL dialect may be required depending on complexity (Teradata, Oracle SQL, etc.).
*   **Temporary File:**
    *   `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_instance.tmp`: Used for storing and retrieving record counts.
    *   **Replacement:** Replaced by a BigQuery `DECLARE` variable within the stored procedure and by logging to `project.dataset.job_run_log`.

## 7. Unresolved / Risks

*   **Complexity of `d_ausd_bp_ta_bpr_instance.sql`:** The primary risk lies within the `d_ausd_bp_ta_bpr_instance.sql` file. Its content is unknown from the current analysis. If it contains complex procedural SQL (e.g., PL/SQL, T-SQL) or proprietary functions, its migration to BigQuery SQL may require significant manual effort or advanced conversion strategies, potentially elevating the overall job complexity beyond `semi_auto`. A separate analysis of this SQL file is recommended.
*   **Commented-out Logic Re-activation:** The commented-out `sed`, `sort`, and `join` pipeline suggests a historical data flow. If this logic needs to be reactivated as part of the migration, it will require careful translation into BigQuery SQL, assuming the input data is available in BigQuery tables or external sources that can be ingested.
*   **Job Control System:** The comments about `FOS-Jobverwaltung` (`h_alis_job.ksh`, `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) indicate integration with a legacy job control system. This needs to be understood and replaced with a modern scheduling and monitoring solution like Cloud Composer (Airflow) or Cloud Workflows, integrating with BigQuery logging tables for run status.
*   **Error Codes:** The script uses numeric error codes (e.g., 193, 192). These will be preserved in the BigQuery `job_error_log` for consistency but may need mapping to a more descriptive error enumeration if a centralized error management system is used.
*   **Missing `file_purpose` and `tier`:** The absence of `file_purpose` and `tier` in `file_analysis` and `file_complexity` might indicate incomplete static analysis for this file. This was partially compensated by the `summary` and MCP tool. A deeper understanding of the business criticality and technical complexity would benefit planning.

## 8. Build Plan

The migration will follow these steps:

1.  **Define BigQuery Datasets:**
    *   Create `project.dataset` for the migrated stored procedures and tables.
    *   Create `project.dataset.job_error_log` and `project.dataset.job_run_log` tables. (Language: DDL/BigQuery SQL)

2.  **Migrate `d_ausd_bp_ta_bpr_instance.sql`:**
    *   **Action:** Analyze the content of `d_ausd_bp_ta_bpr_instance.sql`.
    *   **Output:** Create `project.dataset.d_ausd_bp_ta_bpr_instance` BigQuery Stored Procedure. (Language: BigQuery SQL)

3.  **Migrate `k_ausd_bp_ta_bpr_instance.ksh` (Main Orchestration):**
    *   **Action:** Implement the parameter parsing, validation, date derivation, and invocation logic into a BigQuery stored procedure.
    *   **Output:** Create `project.dataset.r_ausd_bp_ta_bpr_instance` BigQuery Stored Procedure. (Language: BigQuery SQL)

4.  **Migrate Commented Post-processing (if needed):**
    *   **Action:** If the `sed`/`sort`/`join` logic is required, create corresponding BigQuery SQL statements or views to perform the transformations on relevant tables.
    *   **Output:** BigQuery SQL scripts/views. (Language: BigQuery SQL)

5.  **Implement Scheduling/Orchestration:**
    *   **Action:** Configure a Cloud Composer DAG or Cloud Scheduler job to invoke the `project.dataset.r_ausd_bp_ta_bpr_instance` stored procedure at the required frequency, passing in necessary parameters.
    *   **Output:** Python script for Cloud Composer DAG or Cloud Scheduler configuration. (Language: Python/YAML)

6.  **Testing:**
    *   **Action:** Develop and execute unit and integration tests for all BigQuery stored procedures and orchestration components.
    *   **Output:** SQL/Python test scripts. (Language: BigQuery SQL/Python)

