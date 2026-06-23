# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh

## 1. Purpose & Scope
This job, originally `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`, serves as a control script for data processing. Its primary purpose is to orchestrate the execution of a SQL script responsible for managing `ta_action_assoc` data. It handles job parameter parsing, error logging, and includes logic for ignoring active jobs, invoking the SQL script, registering the job in a job table, and deactivating old active jobs. The script reads runtime parameters, validates them, executes a downstream SQL process, and subsequently reads a record count from a temporary file for job completion.

## 2. Source Inventory
The migration involves a single primary source file:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`
  - **Technology:** KornShell Script
  - **Summary:** Acts as a control script for data processing, handling job parameters, error logging, and orchestrating the execution of a SQL script to manage 'ta_action_assoc' data.
  - **Complexity Tier:** medium
  - **Automation Bucket:** semi_auto

## 3. Target Architecture
The migration target platform is Google Cloud BigQuery. The existing KornShell control script will be refactored into a BigQuery Stored Procedure, enabling native execution within the BigQuery environment.
- **Main Component:** A BigQuery Stored Procedure (`project.dataset.sp_ausd_v_ta_action_assoc`) will encapsulate the control flow, parameter handling, and orchestration logic previously found in the KornShell script.
- **Control Tables:** Dedicated BigQuery tables will replace the shell script's job tracking and error logging mechanisms:
    - `project.dataset.job_control`: To track job status, start/end timestamps, and parameters.
    - `project.dataset.job_result`: To store job execution results, such as record counts.
    - `project.dataset.error_log`: To log any errors encountered during execution.
- **SQL Logic Integration:** The SQL script (`d_ausd_v_ta_action_assoc.sql`) invoked by the original KornShell script will either be inlined into the BigQuery Stored Procedure using `EXECUTE IMMEDIATE` or migrated into a separate BigQuery Stored Procedure/SQL script, depending on its complexity and reusability.
- **Environment Variables:** Shell environment variables will be replaced by stored procedure parameters or BigQuery session variables.

## 4. Data Flow & Lineage
The original job's data flow is primarily an orchestration pattern:
1. **Parameter Input:** The `k_ausd_v_ta_action_assoc.ksh` script receives `JobKennung` and `EintragsNr` as command-line arguments.
2. **Configuration & Utility Sourcing:** It sources environment files (`.dw_init`) and other KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3. **Parameter Validation:** The script validates the input parameters.
4. **SQL Script Invocation:** It invokes an external SQL script, `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql`, via a `starteSQLSkript` function. This SQL script is expected to perform the core data manipulation on `ta_action_assoc`.
5. **Record Count Retrieval:** After the SQL script execution, the KornShell script reads a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_action_assoc_$$.tmp`).
6. **Output/Logging:** Status messages are printed to console, and presumably, the SQL script updates database tables.

In BigQuery, this flow will translate to:
1. **Stored Procedure Call:** The `sp_ausd_v_ta_action_assoc` stored procedure is called with `p_JobKennung` and `p_EintragsNr` as input parameters.
2. **Parameter Validation:** The stored procedure performs parameter validation using BigQuery's conditional logic (`IF` statements and `ASSERT`).
3. **Job Control Update:** The `job_control` table is updated to reflect the job's `STARTED` status.
4. **SQL Logic Execution:** The BigQuery equivalent of `d_ausd_v_ta_action_assoc.sql` is executed, performing transformations on BigQuery tables, including `ta_action_assoc`.
5. **Record Count Capture:** The record count will be obtained directly from the executed SQL or a `COUNT(*)` query on the target table and stored in a BigQuery variable (`v_records`).
6. **Job Result & Control Update:** The `job_result` table is populated with the record count, and the `job_control` table is updated to `FINISHED` status with the final record count.
7. **Error Logging:** Any errors during validation or execution are logged to the `error_log` table.

## 5. Transformation Logic
The transformation logic resides primarily within the downstream SQL script (`d_ausd_v_ta_action_assoc.sql`), which was not provided in the scope of this analysis. The `k_ausd_v_ta_action_assoc.ksh` script itself handles:
- **Parameter assignment:** `p_JobKennung`, `p_EintragsNr`
- **Fixed table name declaration:** `v_TabName='ta_action_assoc'`
- **Error handling and exit codes:** Based on parameter validation.
- **Orchestration of SQL execution.**
- **Retrieval of record count:** From a temporary file.

**BigQuery Pseudocode for Wrapper Logic:**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_action_assoc`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_action_assoc';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_sql STRING;
  DECLARE v_error STRING DEFAULT '';
  DECLARE v_error_code INT64 DEFAULT 0;

  -- Parameter validation (replaces shell getopts and pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_error_code = 193;
    SET v_error = 'Jobkennung';
  END IF;

  IF v_error_code = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_error_code = 193;
    SET v_error = 'EintragsNr';
  END IF;

  IF v_error_code != 0 THEN
    -- Equivalent to DWMSG_MeldeFehler / echo / exit
    INSERT INTO `project.dataset.error_log`
      (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
    VALUES
      (CURRENT_TIMESTAMP(), v_error_code, v_error, p_JobKennung, p_EintragsNr,
       CONCAT('FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error));

    RAISE USING MESSAGE = CONCAT('Bitte ueber Rahmenscript aufrufen | FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error);
  END IF;

  -- Optional job control initialization (replaces job table updates)
  INSERT INTO `project.dataset.job_control`
    (job_kennung, eintrags_nr, tab_name, status, created_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'STARTED', CURRENT_TIMESTAMP());

  -- Downstream SQL script equivalent (replaces starteSQLSkript)
  -- The actual transformation logic from d_ausd_v_ta_action_assoc.sql
  -- needs to be translated into BigQuery SQL and inlined here or called as a separate procedure.
  EXECUTE IMMEDIATE """
    -- Placeholder for the logic from d_ausd_v_ta_action_assoc.sql
    -- Example:
    -- UPDATE `project.dataset.ta_action_assoc`
    -- SET status = 'processed'
    -- WHERE entry_nr = @p_EintragsNr;
  """
  USING p_EintragsNr AS p_EintragsNr; -- Example parameter passing

  -- Record count equivalent to temp file read
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.ta_action_assoc`
    WHERE -- Add appropriate filtering conditions based on the SQL script's logic
      TRUE
  );

  -- Persist record count instead of temp file
  INSERT INTO `project.dataset.job_result`
    (job_kennung, eintrags_nr, tab_name, records, result_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

  -- Mark job complete
  UPDATE `project.dataset.job_control`
  SET status = 'FINISHED',
      finished_ts = CURRENT_TIMESTAMP(),
      record_count = v_records
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND tab_name = v_TabName;

END;
```

## 6. External Dependencies
The original script directly sources several other KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These are internal system dependencies within the legacy environment.
- **Environment Initialization:** `$HOME/.dw_init` will be replaced by explicit parameter passing or configuration tables in BigQuery.
- **Utility Scripts:** The logic within `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` will need to be re-implemented natively in BigQuery SQL (for date handling, parameter validation) or replaced by BigQuery's built-in functions and error handling mechanisms.
- **Oracle Database (Implied):** The `starteSQLSkript` function and the `.sql` script it invokes imply interaction with an Oracle database. This interaction will be replaced by direct BigQuery DML/DDL operations.

## 7. Unresolved / Risks
- **Missing Downstream SQL Content:** The most significant unresolved item is the actual content of `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql`. Without this, the precise transformation logic cannot be fully designed or migrated. A placeholder has been used in the BigQuery pseudocode. This SQL file must be analyzed and migrated.
- **Complex Utility Script Logic:** While basic functions are assumed, the full complexity of the sourced `.ksh` utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is unknown. Any intricate logic within them would require careful translation to BigQuery SQL or Python (if part of an external orchestration like Cloud Composer).
- **Temporary File Usage:** The `cat $tmpFile` mechanism for passing data (record counts) between processes will be replaced by BigQuery variables or direct inserts into control/result tables. This is a functional change but carries a low risk if properly implemented.
- **Job Activation/Deactivation Logic:** The summary mentions "ignoring active jobs" and "deactivating old active jobs." The specific logic for how jobs are tracked and their status managed will need to be explicitly defined within the BigQuery job control tables and corresponding stored procedure logic.

## 8. Build Plan
1. **Schema Definition:**
    - Create DDL for `project.dataset.job_control` table (columns: `job_kennung`, `eintrags_nr`, `tab_name`, `status`, `created_ts`, `finished_ts`, `record_count`, etc.).
    - Create DDL for `project.dataset.job_result` table (columns: `job_kennung`, `eintrags_nr`, `tab_name`, `records`, `result_ts`, etc.).
    - Create DDL for `project.dataset.error_log` table (columns: `error_ts`, `error_code`, `error_arg`, `job_kennung`, `eintrags_nr`, `message`, etc.).
    - Define the schema for `project.dataset.ta_action_assoc` and any other tables affected by `d_ausd_v_ta_action_assoc.sql`.
2. **Translate `d_ausd_v_ta_action_assoc.sql`:**
    - Analyze the content of the SQL script.
    - Translate all SQL statements (DML, DDL, DCL) to BigQuery SQL syntax.
    - If the SQL script contains procedural logic, identify whether it can be inlined or needs to be a separate BigQuery Stored Procedure.
3. **Develop BigQuery Stored Procedure:**
    - Create the BigQuery Stored Procedure `project.dataset.sp_ausd_v_ta_action_assoc` based on the provided pseudocode and the translated `d_ausd_v_ta_action_assoc.sql` logic.
    - Implement parameter validation.
    - Implement job control table updates.
    - Integrate the translated core SQL logic.
    - Implement record count capture and persistence.
    - Implement error logging.
4. **Testing:**
    - Unit tests for the BigQuery Stored Procedure and individual SQL components.
    - Integration tests to ensure the end-to-end data flow and job control work as expected.
    - Data validation tests to compare results with the legacy system.
5. **Deployment:**
    - Deployment scripts for the BigQuery DDL and Stored Procedure.
    - Orchestration (e.g., Cloud Composer DAG) to invoke the BigQuery Stored Procedure with required parameters.

**Language:** All components will be in BigQuery SQL.