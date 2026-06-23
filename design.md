# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_acc_ref.ksh`, serves as a control script for the ETL process related to the `ta_acc_ref` table. Its primary function is to orchestrate the execution of a SQL script, `d_ausd_v_ta_acc_ref.sql`, to process data for the `ta_acc_ref` table. The script handles parameter parsing, integrates utility functions for error handling and date operations, and manages the SQL*Plus execution environment. It is also responsible for basic job management, including ignoring active jobs, registering its own execution, and deactivating older active jobs.

## 2. Source Inventory
The job consists of a single primary source file:
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh`
    *   **Technology**: KornShell (ksh)
    *   **Complexity Tier**: `medium`
    *   **Automation Bucket**: `semi_auto`
    *   **Summary**: A control script that validates parameters, sets up the environment, and invokes an external SQL script (`d_ausd_v_ta_acc_ref.sql`) to manage and process data for the `ta_acc_ref` table. It also handles basic job logging/status updates.
    *   **Dependencies**: This script implicitly depends on the SQL script `d_ausd_v_ta_acc_ref.sql` for data processing and a table named `ta_acc_ref` (expected to be updated by the SQL script). It also sources several utility KornShell scripts for error handling, date functions, parameter parsing, and SQL*Plus execution.

## 3. Target Architecture
The migration targets Google Cloud's BigQuery platform. The current KornShell script and its invoked SQL logic will be transformed into the following BigQuery components:
*   **BigQuery Stored Procedure**: A main stored procedure (e.g., `project.dataset.r_ausd_vertrag_control`) will encapsulate the orchestration logic of the original KornShell script, including parameter validation, job status management, and the invocation of the migrated SQL processing logic.
*   **BigQuery Stored Procedure for SQL Logic**: The logic contained within `d_ausd_v_ta_acc_ref.sql` will be migrated into a separate BigQuery stored procedure (e.g., `project.dataset.d_ausd_v_ta_acc_ref`).
*   **Control Tables**:
    *   `project.dataset.job_table`: To manage job status, similar to the original script's job activation/deactivation logic. This table will track job identifiers (`job_kennung`), entry numbers (`eintrags_nr`), table names (`tab_name`), active flags (`active_flag`), and timestamps.
    *   `project.dataset.error_log`: For centralized error logging, replacing the shell script's `DWMSG_MeldeFehler` function and direct error prints.
    *   `project.dataset.job_run_log`: To record the outcome of each job run, including processed record counts, replacing the temporary file approach.
*   **Orchestration**: The execution of the BigQuery stored procedure will be managed by a BigQuery scheduled query or potentially integrated into a broader workflow orchestration tool like Cloud Composer (Airflow) if part of a larger ETL pipeline.

## 4. Data Flow & Lineage
The original data flow involves the `k_ausd_v_ta_acc_ref.ksh` script invoking `d_ausd_v_ta_acc_ref.sql`, which in turn processes data related to `ta_acc_ref`.

**Legacy Flow:**
1.  **Start**: `k_ausd_v_ta_acc_ref.ksh` is executed with parameters `p_JobKennung` and `p_EintragsNr`.
2.  **Initialization**: Sources environment files (`$HOME/.dw_init`) and utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  **Parameter Validation**: Validates `p_JobKennung` and `p_EintragsNr`. If invalid, logs an error and exits.
4.  **Job Status Management**: (Implicitly, via `starteSQLSkript` or the SQL script's logic) Deactivates older jobs related to `p_JobKennung` and marks the current job as active.
5.  **SQL Script Execution**: Executes `d_ausd_v_ta_acc_ref.sql` using `SQL*Plus` via the `starteSQLSkript` function. This SQL script is responsible for the actual data processing, typically reading from and writing to the `ta_acc_ref` table or related staging tables.
6.  **Record Count**: The executed SQL script (or `starteSQLSkript`) is expected to write a record count to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_acc_ref_$$.tmp`).
7.  **Finalization**: The ksh script reads this record count and prints a completion message.

**Target BigQuery Flow:**
1.  **Start**: The BigQuery stored procedure `project.dataset.r_ausd_vertrag_control` is called with `p_JobKennung` and `p_EintragsNr`.
2.  **Parameter Validation**: The procedure performs parameter validation using BigQuery's control flow statements. Errors are logged to `project.dataset.error_log`.
3.  **Job Status Management**: Updates the `project.dataset.job_table` to deactivate previous runs and record the current job's status.
4.  **Data Processing**: Calls the BigQuery stored procedure `project.dataset.d_ausd_v_ta_acc_ref` (migrated from `d_ausd_v_ta_acc_ref.sql`), passing necessary parameters. This procedure will contain the core ETL logic, reading from source tables and writing/merging into the target `ta_acc_ref` table in BigQuery.
5.  **Record Count**: The `project.dataset.d_ausd_v_ta_acc_ref` procedure returns the processed record count, which is captured by `r_ausd_vertrag_control` in a variable.
6.  **Logging**: The processed record count and job details are inserted into `project.dataset.job_run_log`.

## 5. Transformation Logic
The transformation will convert the KornShell orchestration and SQL*Plus invocation into BigQuery SQL stored procedure logic.

*   **Parameter Handling**:
    *   The `getopts` command and associated shell variables (`p_JobKennung`, `p_EintragsNr`) will be replaced by input parameters of the `r_ausd_vertrag_control` BigQuery stored procedure.
    *   Environment variables like `BERT_DIR_ROOT`, `DW_DIR_UTL`, etc., will be handled as procedure parameters or session variables if required for path construction, but their primary use will diminish as file-based dependencies are replaced.
*   **Error Handling**:
    *   The `if [ ! $ErrNr -eq 0 ]` blocks and `DWMSG_MeldeFehler` calls will be replaced with BigQuery `IF` statements, `ASSERT` statements for critical validation failures, and `INSERT` statements into a dedicated `project.dataset.error_log` table.
*   **Job Status Management**:
    *   The logic for deactivating older jobs and registering the current job will be implemented as `UPDATE` and `INSERT` statements against the `project.dataset.job_table`.
*   **SQL Script Execution**:
    *   The `starteSQLSkript` function and the invocation of `d_ausd_v_ta_acc_ref.sql` via `SQL*Plus` will be replaced by a `CALL` statement to the migrated BigQuery stored procedure `project.dataset.d_ausd_v_ta_acc_ref`.
    *   The content of `d_ausd_v_ta_acc_ref.sql` itself will need to be migrated to BigQuery SQL, potentially refactoring for BigQuery best practices (e.g., using `MERGE` statements for UPSERTs, clustering, partitioning).
*   **Temporary File Handling**:
    *   The use of `tmpFile` to store and `eval` to read `v_records` will be replaced by a `DECLARE`d variable (`v_records INT64`) within the stored procedure, which will receive the output of the `d_ausd_v_ta_acc_ref` procedure. This count will then be logged to `project.dataset.job_run_log`.
*   **Utility Scripts**:
    *   Sourced scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` will be replaced by native BigQuery features (e.g., `CURRENT_TIMESTAMP()`, `FORMAT_TIMESTAMP()` for dates, direct parameter validation, and BigQuery's SQL execution capabilities).

**BQ SQL Pseudocode for `r_ausd_vertrag_control` (simplified):**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_acc_ref';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;
  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  -- Error handling (simplified)
  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.error_log` (error_ts, error_source, error_nr, error_arg, message_text)
    VALUES (CURRENT_TIMESTAMP(), 'r_ausd_vertrag_control', ErrNr, ErrArg, 'Parameter validation failed');
    ASSERT FALSE AS 'Parameter validation failed';
  END IF;

  -- Deactivate older active jobs
  UPDATE `project.dataset.job_table`
  SET active_flag = FALSE, updated_at = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung AND active_flag = TRUE AND eintrags_nr <> p_EintragsNr;

  -- Register current job
  INSERT INTO `project.dataset.job_table` (job_kennung, eintrags_nr, tab_name, active_flag, created_at)
  VALUES (p_JobKennung, p_EintragsNr, v_TabName, TRUE, CURRENT_TIMESTAMP());

  -- Execute migrated SQL logic from d_ausd_v_ta_acc_ref.sql
  CALL `project.dataset.d_ausd_v_ta_acc_ref`(p_EintragsNr, p_JobKennung, v_TabName, v_records);

  -- Log processed record count
  INSERT INTO `project.dataset.job_run_log` (job_kennung, eintrags_nr, tab_name, records_processed, logged_at)
  VALUES (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());
END;
```

## 6. External Dependencies
The `lineage_assembled_jobs` record indicates no external systems are directly referenced by this job. The current implementation relies on `SQL*Plus` to interact with a traditional relational database (likely Oracle, given the context).
*   **Database Interaction (`SQL*Plus`)**: This is an implicit external dependency. In the target architecture, direct interaction with the legacy database via `SQL*Plus` will be eliminated. All data sources will be ingested into BigQuery, and all SQL operations will be performed natively within BigQuery.
*   **Utility Scripts**: The sourced KornShell utility scripts are internal to the legacy environment. Their functionalities will be either replicated using native BigQuery SQL features (e.g., date functions, parameter handling) or incorporated directly into the BigQuery stored procedure logic.

## 7. Unresolved / Risks
*   **Unresolved Targets**: The `lineage_assembled_jobs` record indicates `unresolved_targets` is empty, meaning all references detected were resolved.
*   **Core SQL Logic Migration**: The most significant unresolved task is the actual migration of the SQL code within `d_ausd_v_ta_acc_ref.sql`. This document provides a design for the *orchestration* script, but the internal SQL logic's transformation, data model mapping, and performance tuning for BigQuery are critical separate steps.
*   **Dynamic SQL / Complex SQL*Plus Features**: If `d_ausd_v_ta_acc_ref.sql` contains highly dynamic SQL generation, complex PL/SQL, or specific `SQL*Plus` features not easily translatable to standard SQL or BigQuery scripting, these will require careful redesign, potentially involving more advanced BigQuery scripting, UDFs, or even external processing (e.g., Dataflow/Spark). The current design assumes straightforward SQL conversion.
*   **Job Control Table (`job_table`)**: The design proposes a `job_table` for status management. The exact schema and the logic for `active_flag` and deactivation should be verified against the precise business requirements to ensure correct behavior in BigQuery.
*   **`BERT_DIR_ROOT`, `DW_DIR_UTL` Context**: The original script uses these environment variables to locate files. While file I/O is eliminated, if these variables implicitly define schemas or dataset locations in the legacy system, their mapping to BigQuery datasets or project IDs must be established.

## 8. Build Plan
The build plan involves creating the necessary BigQuery components.

1.  **DDL for Control Tables (BigQuery SQL)**:
    *   Create `project.dataset.job_table`
        ```sql
        CREATE TABLE `project.dataset.job_table` (
          job_kennung STRING NOT NULL,
          eintrags_nr STRING NOT NULL,
          tab_name STRING,
          active_flag BOOL,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        );
        ```
    *   Create `project.dataset.error_log`
        ```sql
        CREATE TABLE `project.dataset.error_log` (
          error_ts TIMESTAMP NOT NULL,
          error_source STRING,
          error_nr INT64,
          error_arg STRING,
          message_text STRING
        );
        ```
    *   Create `project.dataset.job_run_log`
        ```sql
        CREATE TABLE `project.dataset.job_run_log` (
          job_kennung STRING NOT NULL,
          eintrags_nr STRING NOT NULL,
          tab_name STRING,
          records_processed INT64,
          logged_at TIMESTAMP
        );
        ```
2.  **Migrate `d_ausd_v_ta_acc_ref.sql` to `project.dataset.d_ausd_v_ta_acc_ref` Stored Procedure (BigQuery SQL)**:
    *   This is a separate, significant migration task. The resulting stored procedure will encapsulate the data processing logic and return the processed record count.
    *   `CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_acc_ref`(IN p_EintragsNr STRING, IN p_JobKennung STRING, IN v_TabName STRING, OUT v_records INT64) ...`
3.  **Create `project.dataset.r_ausd_vertrag_control` Stored Procedure (BigQuery SQL)**:
    *   Implement the orchestration logic based on the "Transformation Logic" section.
    *   `CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(IN p_JobKennung STRING, IN p_EintragsNr STRING) ...`
4.  **BigQuery Scheduled Query / Orchestration Configuration**:
    *   Configure a scheduled query or a Cloud Composer DAG to execute the `project.dataset.r_ausd_vertrag_control` stored procedure at the appropriate time and with the necessary parameters.