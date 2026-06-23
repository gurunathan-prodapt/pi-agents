# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_bp_ta_bcp_msisdn.ksh`, acts as an orchestrator for data preparation. Its primary function is to:
*   Set up the environment by sourcing various utility scripts.
*   Parse command-line parameters (Job ID, entry number, reference date, restart value).
*   Validate the provided parameters, including the format of the reference date.
*   Execute a core SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`) which contains the main data processing logic.
*   Capture the number of records processed by the SQL script from a temporary file.
*   Log the job's completion and record count (though the logging mechanism is commented out in the provided script).

The business purpose is to prepare data, likely related to product information (`PoolBasisprodukt`), for subsequent analysis or reporting, scoped by a given reference date and job identifier.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`
    *   **Technology:** KornShell
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Automatic (B2)
    *   **Summary:** This script controls data preparation, handling parameter parsing, date validation, error handling, and orchestrating the execution of an SQL script for data processing. It also includes commented-out interactions with a job management system and post-processing steps (sed, sort, join).

## 3. Target Architecture
The target architecture in BigQuery will involve:
*   **BigQuery Stored Procedure:** The core orchestration logic, parameter handling, and validation will be migrated into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_bp_ta_bcp_msisdn`). This procedure will accept input parameters and manage the execution flow.
*   **BigQuery SQL Script/Statements:** The business logic currently residing in `d_ausd_bp_ta_bcp_msisdn.sql` will be directly translated into BigQuery SQL statements within or called by the stored procedure.
*   **BigQuery Tables:**
    *   Target tables for the processed data (e.g., `project.dataset.target_table`).
    *   A job logging table (e.g., `project.dataset.job_log`) to replace the commented-out job management system interaction and temporary file record count.
*   **Orchestration (Optional):** If external scheduling or complex dependency management is required, Cloud Composer (Airflow DAG) or Google Cloud Workflows can be used to invoke the BigQuery Stored Procedure with appropriate parameters.

## 4. Data Flow & Lineage
The original script's data flow involves:
1.  **Parameter Input:** JobKennung, EintragsNr, Stichtag, wiederanlaufWert (command-line arguments).
2.  **Environment Setup:** Sourcing various utility `.ksh` scripts.
3.  **Validation:** Parameter existence and `Stichtag` date format check.
4.  **SQL Script Execution:** Invocation of `d_ausd_bp_ta_bcp_msisdn.sql` via `starteSQLSkript` with various parameters. This SQL script is responsible for the actual data transformation and loading.
5.  **Record Count Capture:** The number of processed records is read from `$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`.
6.  **Logging (Intended):** An entry is intended to be made into a job table via `FOSJobErzeugeEintrag`.

**Migrated Data Flow:**
1.  **BigQuery Stored Procedure Call:** The BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bcp_msisdn` will be invoked with parameters (equivalent to original command-line args).
2.  **Parameter Validation:** Implemented using `IF` conditions and `RAISE` statements within the stored procedure. Date format validation will use `SAFE.PARSE_DATE`.
3.  **Core Data Processing:** The logic from `d_ausd_bp_ta_bcp_msisdn.sql` will be executed directly as BigQuery SQL within the stored procedure or as a separate BigQuery script called by it. This will perform transformations and load data into `project.dataset.target_table`.
4.  **Record Count & Logging:** The number of records processed will be obtained using `COUNT(*)` from the target table or derived directly from the `INSERT` or `MERGE` statement. This count, along with job status and other metadata, will be logged into `project.dataset.job_log`.
5.  **Error Handling:** BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks will handle errors, logging them to `project.dataset.job_log`.

## 5. Transformation Logic
The transformation logic predominantly resides within the SQL script `d_ausd_bp_ta_bcp_msisdn.sql`, which this KornShell script orchestrates. The KornShell script itself handles:
*   **Parameter Handling:** `getopts` for command-line argument parsing. In BigQuery, this will be handled by stored procedure input parameters.
*   **Date Derivation:** Uses `gestern.ksh` to get yesterday's and today's dates. In BigQuery, this will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **Temporary File Management:** Creates and reads from `$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`. In BigQuery, this will be replaced by a BigQuery variable (`DECLARE v_records INT64;`) or by querying the target table directly.
*   **Error Handling:** Uses `f_alis_msgerr.ksh` for error reporting and exits with a status code. In BigQuery, this will be replaced by `RAISE` statements and error logging to a dedicated table.

The core SQL execution logic:
*   The script calls `starteSQLSkript` which is presumably a wrapper for `sqlplus` or similar to execute `d_ausd_bp_ta_bcp_msisdn.sql`. This will be replaced by direct BigQuery SQL execution.

**Example BigQuery SQL Pseudocode for `r_ausd_bp_ta_bcp_msisdn`:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bcp_msisdn`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_restart_value STRING DEFAULT '0';

  -- Parameter initialization and validation
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET v_restart_value = '0';
  ELSE
    SET v_restart_value = p_wiederanlaufWert;
  END IF;

  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_msg = 'Jobkennung fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_err_msg = 'EintragsNr fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_err_msg = 'Stichtag fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SET v_err_msg = 'Stichtag hat kein gueltiges Format DDMMYYYY';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- Core processing logic (migrated from d_ausd_bp_ta_bcp_msisdn.sql)
  BEGIN
    -- Placeholder for the actual INSERT/MERGE/UPDATE statements
    -- that perform the data processing based on p_EintragsNr, v_stichtag_date, etc.
    -- Example:
    -- INSERT INTO `project.dataset.target_table` (col1, col2, ...)
    -- SELECT some_col, other_col FROM `project.dataset.source_table`
    -- WHERE process_date = v_stichtag_date;

    SET v_records = (
      SELECT COUNT(*)
      FROM `project.dataset.target_table`
      WHERE DATE(process_date) = v_stichtag_date
    );

    -- Log job success
    INSERT INTO `project.dataset.job_log` (job_name, entry_nr, stichtag, restart_value, records_processed, status, created_at)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_restart_value, v_records, 'SUCCESS', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Log job failure
    INSERT INTO `project.dataset.job_log` (job_name, entry_nr, stichtag, restart_value, records_processed, status, created_at, error_message)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_restart_value, v_records, 'FAILED', CURRENT_TIMESTAMP(), @@error.message);
    RAISE; -- Re-raise the error to propagate it
  END;

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
  SELECT v_records AS records_processed;
END;
```

## 6. External Dependencies
The original script has several external dependencies, primarily other shell scripts and potentially a job management system.

*   **Environment Initialization (`. $HOME/.dw_init`):**
    *   **Replacement:** In BigQuery, environment variables are typically replaced by dataset and table naming conventions, stored procedure parameters, or configuration tables. Explicit `DW_DIR_UTL` and `BERT_DIR_ROOT` references will be replaced by direct BigQuery project/dataset/table paths.
*   **Error Handling (`f_alis_msgerr.ksh`):**
    *   **Replacement:** BigQuery's built-in error handling (`BEGIN...EXCEPTION`) and logging to a dedicated job log table.
*   **Date Utilities (`h_alis_date.ksh`, `gestern.ksh`):**
    *   **Replacement:** BigQuery's native date functions (`CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()`, `FORMAT_DATE()`).
*   **Parameter Utilities (`h_alis_parameter.ksh`):**
    *   **Replacement:** BigQuery Stored Procedure input parameters and conditional logic.
*   **SQLPlus Wrapper (`h_alis_sqlplus.ksh`):**
    *   **Replacement:** Direct execution of BigQuery SQL statements. The need for a separate SQLPlus client is eliminated.
*   **Job Management System (`h_alis_job.ksh`, `FOSJobDeaktivate`, `FOSJobErzeugeEintrag` - commented out):**
    *   **Replacement:** A BigQuery job logging table (`project.dataset.job_log`) to record job status, start/end times, and processed records. If full job orchestration features are required (e.g., job dependencies, retries), Cloud Composer or Workflows would be used.
*   **Temporary Files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`):**
    *   **Replacement:** BigQuery variables (`DECLARE`) to store intermediate values like record counts.
*   **Post-processing Utilities (`sed`, `sort`, `join` - commented out):**
    *   **Replacement:** If these steps become active and necessary, they will be translated into BigQuery SQL using functions like `REPLACE`, `ORDER BY`, `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1` for deduplication, and `JOIN` clauses.

## 7. Unresolved / Risks
*   **Core SQL Logic in `d_ausd_bp_ta_bcp_msisdn.sql`:** The actual data transformation logic is in a separate SQL file not analyzed here. This is the biggest unknown and requires separate migration and design. The success of this shell script's migration hinges on the successful migration of its dependent SQL script.
*   **Commented-out Code:** The script contains significant commented-out sections (e.g., `AL??` for job management, `sed/sort/join` for post-processing). It's crucial to confirm if these functionalities are still required. If so, they need to be designed and implemented in BigQuery.
*   **`starteSQLSkript` functionality:** The exact actions of the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) are not fully detailed. It's assumed to be a wrapper for executing the SQL script and potentially capturing its output (like record counts). Its precise behavior needs to be understood to ensure accurate migration.
*   **Error Number Mapping:** The error codes (e.g., `ErrNr=193`) would need to be mapped to appropriate BigQuery error handling or a custom error logging system.

## 8. Build Plan
1.  **Define BigQuery Environment:**
    *   Create BigQuery Dataset: `project.dataset`
    *   Create BigQuery Job Log Table: `project.dataset.job_log` (schema: `job_name STRING, entry_nr STRING, stichtag STRING, restart_value STRING, records_processed INT64, status STRING, created_at TIMESTAMP, error_message STRING`).
2.  **Migrate `d_ausd_bp_ta_bcp_msisdn.sql`:**
    *   Analyze `d_ausd_bp_ta_bcp_msisdn.sql` (requires a separate design phase for this SQL file).
    *   Translate `d_ausd_bp_ta_bcp_msisdn.sql` into BigQuery SQL, potentially as a series of `INSERT`, `MERGE`, or `CREATE TABLE AS SELECT` statements, possibly encapsulated within a separate BigQuery Stored Procedure.
    *   Identify source and target tables involved in `d_ausd_bp_ta_bcp_msisdn.sql` and ensure their DDL exists in BigQuery.
3.  **Create BigQuery Stored Procedure `r_ausd_bp_ta_bcp_msisdn`:**
    *   Implement the parameter parsing and validation logic in BigQuery SQL.
    *   Integrate the migrated BigQuery SQL logic from `d_ausd_bp_ta_bcp_msisdn.sql` (or call its dedicated stored procedure).
    *   Implement record counting and job logging to `project.dataset.job_log`.
    *   Implement BigQuery error handling (`BEGIN...EXCEPTION`).
4.  **Develop Orchestration (if needed):**
    *   If scheduled execution is required, create a Cloud Composer DAG or a Scheduled Query in BigQuery to invoke the `project.dataset.r_ausd_bp_ta_bcp_msisdn` stored procedure with runtime parameters.
5.  **Test:** Thoroughly test the BigQuery Stored Procedure with various parameter combinations, including valid and invalid dates, and ensure correct data processing and logging.