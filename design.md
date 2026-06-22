# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_disc_zusgf.ksh`. The script acts as a control and orchestration layer for a data processing job. Its primary purpose is to manage job execution, handle input parameters, and invoke an underlying SQL script, `d_ausd_v_ta_disc_zusgf.sql`, which updates the `ta_disc_zusgf` table. It also incorporates error handling, job status management (implied by "ignore active jobs" and "deactivate old active jobs"), and collects metrics (record count) from the executed SQL process. The scope of this migration is to re-implement this orchestration logic and the underlying SQL processing within the Google Cloud Platform, targeting BigQuery for data processing and storage.

## 2. Source Inventory
The job is composed of a single main KornShell script. Due to missing data in `file_complexity` and `automation_rate` tables, the tier and automation bucket are estimated.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`
    *   **Technology**: KornShell (orchestration)
    *   **Tier**: Medium (estimated, due to orchestration logic, parameter handling, and external script invocation)
    *   **Automation Bucket**: Semi-Auto (estimated, as the shell orchestration logic will require redesign into an Airflow DAG or BigQuery Stored Procedure, and the underlying SQL will need conversion)
    *   **Summary**: Control script managing job execution, handling parameters (`-j` for `JobKennung`, `-f` for `EintragsNr`), sourcing various utility scripts (e.g., error handling, date utilities, parameter parsing, SQL*Plus wrapper), and most importantly, invoking the SQL script `d_ausd_v_ta_disc_zusgf.sql` to update the `ta_disc_zusgf` table. It also reads a record count from a temporary file.

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform services:

*   **Orchestration**:
    *   **Cloud Composer (Apache Airflow)**: For scheduling and orchestrating the end-to-end workflow. The shell script's logic will be re-implemented as a Python-based Airflow DAG.
    *   **BigQuery Stored Procedure**: Alternatively, the orchestration logic can be encapsulated within a BigQuery Stored Procedure if it primarily involves BigQuery SQL operations and parameter passing. This would be a more direct translation of the script's control flow.
*   **Data Processing & Storage**:
    *   **BigQuery**: The `ta_disc_zusgf` table and any other tables involved in `d_ausd_v_ta_disc_zusgf.sql` will reside in BigQuery.
    *   **BigQuery SQL**: The SQL logic from `d_ausd_v_ta_disc_zusgf.sql` will be converted to BigQuery-compatible SQL, ideally encapsulated within a BigQuery Stored Procedure or executed directly by an Airflow `BigQueryOperator`.
*   **Logging & Monitoring**:
    *   **Cloud Logging**: For capturing execution logs, error messages, and operational insights.
    *   **BigQuery Logging/Audit Tables**: Dedicated tables within BigQuery for tracking job status, parameters, and record counts (replacing the shell's temporary file approach).

## 4. Data Flow & Lineage
The original script `k_ausd_v_ta_disc_zusgf.ksh` orchestrates the following:

1.  **Parameter Reception**: Accepts `JobKennung` and `EintragsNr` as command-line arguments.
2.  **Environment Initialization**: Sources various utility KornShell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  **Parameter Validation**: Validates the presence of required parameters. If validation fails, it logs an error and exits.
4.  **SQL Script Invocation**: Calls the `starteSQLSkript` function (presumably from `h_alis_sqlplus.ksh`) to execute the SQL script `d_ausd_v_ta_disc_zusgf.sql`. This SQL script is responsible for the core data manipulation, specifically updating `ta_disc_zusgf`.
5.  **Record Count Retrieval**: Reads a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_disc_zusgf_$$.tmp`), which is implicitly written by the executed SQL script or the `starteSQLSkript` function.

**Migrated Data Flow:**

1.  **Airflow DAG (or BigQuery Stored Procedure)**:
    *   **Task 1 (Parameter Handling)**: Receive `JobKennung` and `EintragsNr` (e.g., from Airflow DAG parameters or BigQuery procedure arguments). Validate these parameters.
    *   **Task 2 (Execute Core Logic)**: Invoke a BigQuery Stored Procedure, say `project.dataset.d_ausd_v_ta_disc_zusgf_sp`, passing the parameters. This stored procedure will contain the migrated SQL logic from `d_ausd_v_ta_disc_zusgf.sql`.
        *   **Inside `d_ausd_v_ta_disc_zusgf_sp`**: Perform DML operations to update the `ta_disc_zusgf` table. Return the number of processed records.
    *   **Task 3 (Logging & Metrics)**: Capture the returned record count from the BigQuery Stored Procedure and log it to a BigQuery logging table (e.g., `project.dataset.job_run_log`). Log any errors to Cloud Logging and a BigQuery error logging table (`project.dataset.job_error_log`).

**Lineage:**
Airflow DAG (or BigQuery Orchestration) -> BigQuery Stored Procedure `d_ausd_v_ta_disc_zusgf_sp` -> BigQuery Table `ta_disc_zusgf` (WRITE)
Also, logging to `project.dataset.job_error_log` and `project.dataset.job_run_log`.

## 5. Transformation Logic
The transformation logic is primarily contained within the referenced SQL script `d_ausd_v_ta_disc_zusgf.sql`, which the KornShell script orchestrates. The KornShell script itself handles:

*   **Parameter Parsing**: `getopts` for `j` and `f`.
*   **Parameter Validation**: Checks if required parameters are set.
*   **Error Handling**: Uses `f_alis_msgerr.ksh` for error messages and exits with specific error codes.
*   **SQL Execution**: Invokes an external SQL script via `h_alis_sqlplus.ksh`.
*   **Result Capture**: Reads record count from a temporary file.

**Migrated Transformation Logic:**

*   **Parameter Handling**: Replaced by BigQuery Stored Procedure input parameters or Airflow DAG parameters.
*   **Validation & Error Handling**: Re-implemented using BigQuery scripting (`IF`, `RAISE`, DML to logging tables) or Python in Airflow for error propagation and logging to Cloud Logging.
*   **SQL Execution**: The logic of `d_ausd_v_ta_disc_zusgf.sql` will be directly translated into BigQuery SQL and placed within a BigQuery Stored Procedure.
*   **Result Capture**: The temporary file mechanism will be replaced by the BigQuery Stored Procedure returning the record count as an `OUT` parameter, or by querying a logging table that the BigQuery SQL inserts into.

**Pseudocode (BigQuery Stored Procedure):**
```sql
-- BigQuery Stored Procedure Pseudocode for the orchestrator
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  OUT p_records_processed INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_disc_zusgf';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    -- Log error to a BigQuery table
    INSERT INTO `project.dataset.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg);
  END IF;

  -- Call the migrated SQL logic stored procedure
  CALL `project.dataset.d_ausd_v_ta_disc_zusgf_sp`(p_EintragsNr, p_JobKennung, v_TabName, p_records_processed);

  -- Log successful completion (optional, can be done in Airflow)
  INSERT INTO `project.dataset.job_run_log` (job_kennung, eintrags_nr, records_processed, created_ts)
  VALUES (p_JobKennung, p_EintragsNr, p_records_processed, CURRENT_TIMESTAMP());

END;
```

## 6. External Dependencies
The `lineage_assembled_jobs` analysis indicated no direct external system dependencies (`external_systems: []`). Similarly, `lineage_unresolved` also showed no entries.

The script primarily interacts with:
*   **Local File System**: For sourcing utility scripts and temporary files. These are internal to the legacy environment.
*   **Database**: Via the `d_ausd_v_ta_disc_zusgf.sql` script. The specific database system is not explicitly stated but is implicitly the source of the `ta_disc_zusgf` table.

**Migration Strategy for Dependencies:**

*   **Sourced Utility Scripts**: The functions provided by scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` will be absorbed into the BigQuery Stored Procedure's logic or the Airflow DAG's Python code. For example, parameter validation and error logging will be re-implemented directly.
*   **Temporary Files**: The use of `$DW_DIR_UTL/bert_k_ausd_v_ta_disc_zusgf_$$.tmp` to pass record counts will be replaced by BigQuery Stored Procedure `OUT` parameters or by writing to a dedicated BigQuery logging/metrics table.
*   **Database Interactions (via `d_ausd_v_ta_disc_zusgf.sql`)**: The SQL script's operations will be migrated to BigQuery SQL and executed within BigQuery. The target platform for the database is BigQuery, so this is an internal migration rather than an external system integration.

## 7. Unresolved / Risks
*   **Underlying SQL (`d_ausd_v_ta_disc_zusgf.sql`)**: The actual business logic is in this SQL file, which was not analyzed directly in this job run. Its complexity, dependencies, and migration path (e.g., to BigQuery Stored Procedure or view) are critical but currently unresolved. This is the biggest risk.
*   **Helper Script Logic**: The exact functions performed by the sourced KSH utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) are not fully detailed. Their functionalities will need to be thoroughly understood and re-implemented in BigQuery SQL scripting or Python for the Airflow DAG.
*   **Job Status Management**: The note about "active jobs being ignored" and "old active jobs being deactivated" suggests a job control mechanism not fully detailed by the script itself. This logic will need to be re-evaluated and implemented using BigQuery logging tables or Airflow's built-in task state management.
*   **Dynamic SQL**: If `d_ausd_v_ta_disc_zusgf.sql` contains dynamic SQL, its conversion will require careful handling using BigQuery's `EXECUTE IMMEDIATE`.

## 8. Build Plan
The migration will involve the following steps and generated artifacts:

1.  **Analyze `d_ausd_v_ta_disc_zusgf.sql`**: Perform a detailed analysis of the SQL script to understand its exact DML operations, source tables, and transformations.
    *   **Output Language**: BigQuery SQL
2.  **Design and Implement BigQuery Stored Procedure for SQL Logic**: Translate `d_ausd_v_ta_disc_zusgf.sql` into a BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_v_ta_disc_zusgf_sp`).
    *   **Output Language**: BigQuery SQL
3.  **Design and Implement BigQuery Control Stored Procedure**: Create a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`) that encapsulates the orchestration, parameter handling, validation, and error logging logic of `k_ausd_v_ta_disc_zusgf.ksh`. This procedure will call `project.dataset.d_ausd_v_ta_disc_zusgf_sp`.
    *   **Output Language**: BigQuery SQL
4.  **Create BigQuery Logging Tables**: Define DDL for `project.dataset.job_error_log` and `project.dataset.job_run_log` to capture job metadata and errors.
    *   **Output Language**: BigQuery DDL
5.  **Develop Airflow DAG (if chosen for orchestration)**: Create a Python Airflow DAG that schedules and executes the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure. This DAG will also handle parameter passing and monitor the procedure's execution.
    *   **Output Language**: Python
6.  **Data Migration**: Migrate the source `ta_disc_zusgf` table and any other dependent tables to BigQuery.
    *   **Output Language**: BigQuery DDL (for table schemas) and various data loading methods (e.g., Data Transfer Service, `bq load`, Cloud Storage).