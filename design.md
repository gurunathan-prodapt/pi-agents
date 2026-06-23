# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

## 1. Purpose & Scope
The KornShell script `k_ausd_v_ta_period.ksh` serves as a control script for a larger process related to `r_ausd_vertrag.ksh`. Its primary purpose is to orchestrate the execution of a SQL script, `d_ausd_v_ta_period.sql`, which likely performs data manipulation on the `ta_period` table.
The script handles:
- Parameter parsing (`p_JobKennung`, `p_EintragsNr`).
- Environment setup by sourcing common utility scripts.
- Error handling and logging.
- Activation/deactivation of jobs, potentially to ensure only one instance runs or to manage job states in a central job table.
- Execution of the core SQL logic encapsulated in `d_ausd_v_ta_period.sql`.
- Retrieval of record counts from the executed SQL process.

The scope of this migration is to re-implement this orchestration logic, including parameter handling, error management, and SQL execution, within the Google Cloud BigQuery ecosystem, leveraging BigQuery Stored Procedures and potentially Cloud Composer for overall workflow management if complex external dependencies arise.

## 2. Source Inventory
This job consists of a single KornShell script.

| File Path                                                       | Technology | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                        |
| :-------------------------------------------------------------- | :--------- | :----- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh` | KornShell  | medium | semi_auto         | This is a control script that handles parameter parsing, environment setup, error handling, and orchestrates the execution of a SQL script (`d_ausd_v_ta_period.sql`), likely performing data manipulation on the 'ta_period' table. |

## 3. Target Architecture
The migrated solution will primarily reside within BigQuery.
- **Orchestration Logic**: The shell script's control flow, parameter validation, and job management will be converted into a BigQuery Stored Procedure. This procedure will encapsulate the core orchestration and call the migrated SQL logic.
- **Data Transformation**: The underlying SQL script `d_ausd_v_ta_period.sql` (which is not part of this specific job but referenced) will be migrated to BigQuery Standard SQL, either as direct statements within the stored procedure or as a separate BigQuery Stored Procedure that is called by the main orchestration procedure.
- **Job Status Management**: A dedicated BigQuery table (e.g., `project.dataset.job_table`) will be used to manage job status, active flags, and record counts, replacing the temporary file and implicit job tracking in the original script.
- **Error Logging**: A BigQuery error logging table (e.g., `project.dataset.error_log`) will capture error messages, replacing the shell script's `DWMSG_MeldeFehler` calls.
- **Scheduling**: The BigQuery Stored Procedure can be scheduled directly using BigQuery Scheduled Queries or orchestrated via Cloud Composer (Apache Airflow) if it becomes part of a larger workflow with external dependencies or complex scheduling requirements.

## 4. Data Flow & Lineage
The original script `k_ausd_v_ta_period.ksh` exhibits the following conceptual data flow:

1.  **Input Parameters**: `p_JobKennung` and `p_EintragsNr` are provided as command-line arguments to the script.
2.  **Environment Setup**: The script sources `$HOME/.dw_init` and several utility KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These provide functions for error handling, date utilities, parameter parsing, and SQL execution.
3.  **Parameter Validation**: Inputs are validated using `pruefeParameterGesetzt`. If validation fails, an error message is generated via `DWMSG_MeldeFehler` and the script exits.
4.  **SQL Script Execution**: The script defines `Name_SQLskript` pointing to `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql`. It then executes this SQL script via the `starteSQLSkript` function, passing `p_EintragsNr` and `p_JobKennung`. This step likely updates a job table to mark the job as active or track its progress.
5.  **Record Count Retrieval**: After SQL execution, the script reads a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp`) to get the number of records processed and stores it in `v_records`.
6.  **Job Completion**: The script indicates completion via a console message. The updated record count and job status would typically be written back to a job control table by `starteSQLSkript` or similar post-processing.

**Migrated Data Flow in BigQuery:**

1.  **BigQuery Stored Procedure Execution**: A BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_vertrag_control(p_JobKennung STRING, p_EintragsNr STRING)`, is invoked.
2.  **Parameter Handling**: The procedure directly receives `p_JobKennung` and `p_EintragsNr` as parameters.
3.  **Validation & Error Logging**: Inside the stored procedure, `IF` statements perform parameter validation. If errors occur, `INSERT` statements write to an `error_log` table, and the procedure can signal an error or return.
4.  **Job Status Management**: `UPDATE` statements on `project.dataset.job_table` will mark old active jobs as inactive and register the current job as active.
5.  **Core SQL Logic Execution**: The content of `d_ausd_v_ta_period.sql` will be integrated directly into the stored procedure (via `EXECUTE IMMEDIATE` for dynamic SQL or as inline DML/DDL) or called as a separate BigQuery Stored Procedure. This SQL will perform the actual data transformations.
6.  **Record Count**: Instead of a temporary file, `SELECT COUNT(*)` will populate a `DECLARE`d variable `v_records` within the stored procedure.
7.  **Job Completion Update**: `UPDATE` statements on `project.dataset.job_table` will record the processed count and mark the job as completed/inactive.

## 5. Transformation Logic
The transformation logic within `k_ausd_v_ta_period.ksh` is primarily orchestrational rather than data-centric. The actual data transformation is delegated to `d_ausd_v_ta_period.sql`.

**Core transformations in `k_ausd_v_ta_period.ksh` to BigQuery:**

*   **Parameter Parsing (`getopts`)**: Replaced by direct stored procedure parameters.
*   **Environment Sourcing (`. $HOME/.dw_init`, etc.)**: Replaced by BigQuery project/dataset configuration or hardcoded/configurable variables within the stored procedure. External utility functions will either be re-implemented as UDFs/stored procedure helper routines or their functionality will be absorbed into the main procedure logic.
*   **Parameter Validation (`pruefeParameterGesetzt`)**: Replaced by BigQuery procedural `IF` statements and `IS NULL` / `TRIM('')` checks.
*   **Error Reporting (`DWMSG_MeldeFehler`)**: Replaced by `INSERT` statements into a BigQuery `error_log` table.
*   **SQL Execution Wrapper (`starteSQLSkript`)**: The functionality of this wrapper (executing `d_ausd_v_ta_period.sql`, managing active jobs, handling temporary files) will be absorbed into the BigQuery Stored Procedure. The SQL script's content will either be directly embedded or called.
*   **Temporary File Handling (`tmpFile`, `cat $tmpFile`, `eval`)**: Replaced by BigQuery `DECLARE` variables and `SELECT COUNT(*)` for record counting.
*   **Job Deactivation**: `UPDATE` statements on a BigQuery `job_table`.

**BigQuery SQL Pseudocode for `r_ausd_vertrag_control`:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_period';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    -- Log error
    INSERT INTO `project.dataset.error_log`(error_ts, error_code, error_arg, procedure_name)
    VALUES (CURRENT_TIMESTAMP(), ErrNr, ErrArg, 'r_ausd_vertrag_control');
    -- Optionally, raise an error or exit gracefully
    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    RETURN; -- Exit the procedure
  END IF;

  -- Deactivate old active jobs
  UPDATE `project.dataset.job_table`
  SET active_flag = FALSE, updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE;

  -- Register current job as active
  INSERT INTO `project.dataset.job_table`(job_kennung, eintrags_nr, tab_name, active_flag, created_ts)
  VALUES (p_JobKennung, p_EintragsNr, v_TabName, TRUE, CURRENT_TIMESTAMP());

  -- Core SQL logic migrated from d_ausd_v_ta_period.sql
  -- This section must be populated with the BigQuery SQL equivalent of d_ausd_v_ta_period.sql
  -- Example placeholder (replace with actual SQL):
  -- EXECUTE IMMEDIATE """
  --   INSERT INTO `project.dataset.target_data_table` (col1, col2, ...)
  --   SELECT source_col1, source_col2, ...
  --   FROM `project.dataset.source_data_table`
  --   WHERE some_condition = @p_EintragsNr;
  -- """ USING p_EintragsNr AS p_EintragsNr;

  -- After executing d_ausd_v_ta_period.sql logic, get record count
  -- Assuming 'project.dataset.result_table' is where the SQL script writes its output
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.result_table`
    WHERE eintrags_nr = p_EintragsNr -- or appropriate filtering
  );

  -- Update job table with record count and mark as completed
  UPDATE `project.dataset.job_table`
  SET record_count = v_records,
      active_flag = FALSE,
      completed_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr;

  SELECT v_records AS records_processed;
END;
```

## 6. External Dependencies
The current job `k_ausd_v_ta_period.ksh` has no explicit external system dependencies reported in `lineage_assembled_jobs`. However, based on the script content:
- **Oracle SQL*Plus/Database**: The script likely executes SQL via a `starteSQLSkript` wrapper, implying an underlying RDBMS, traditionally Oracle in many legacy DWH environments. This is inferred from typical usage of `.ksh` scripts with SQL scripts.
  - **Replacement**: The `d_ausd_v_ta_period.sql` content will be translated from its original SQL dialect (likely Oracle SQL) to BigQuery Standard SQL. Direct `EXECUTE IMMEDIATE` within BigQuery Stored Procedures will handle the execution.
- **Filesystem (for temporary files and sourced scripts)**: The script uses `$DW_DIR_UTL` for temporary files and `$BERT_DIR_ROOT` for sourcing utility scripts.
  - **Replacement**: Temporary file usage will be replaced by BigQuery `DECLARE` variables or internal BigQuery tables. Sourced utility script logic will be re-implemented as BigQuery UDFs or integrated directly into the stored procedure. Environment variables will be replaced by BigQuery procedure parameters or configuration managed in BigQuery or Cloud Storage.

## 7. Unresolved / Risks
- **`d_ausd_v_ta_period.sql` content**: The actual SQL logic within `d_ausd_v_ta_period.sql` is unknown. Its complexity, use of specific database features (e.g., PL/SQL, vendor-specific functions), and data sources/targets are critical for a complete migration. This is a significant unresolved item that needs to be analyzed separately.
- **Job Table Schema**: The schema for the `job_table` (used for `p_JobKennung`, `p_EintragsNr`, `active_flag`, `record_count`) and `error_log` table needs to be defined in BigQuery.
- **`h_alis_job.ksh` (commented out)**: The commented line `AL?? . ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_job.ksh` suggests there might be other job management logic that was either never used or temporarily disabled. This should be verified for completeness, though for this migration, it will be assumed it's not in scope given its commented status.
- **`DWMSG_MeldeFehler`**: The exact implementation of this error messaging utility is unknown. The BigQuery migration will replace it with `INSERT` into an error logging table. The details of the original logging (e.g., error codes, message formats) need to be preserved if they are consumed downstream.
- **`starteSQLSkript`**: This is a key abstraction. Its full functionality (e.g., how it handles connections, error propagation, temporary file creation, concurrent job handling) needs to be fully understood and replicated in the BigQuery Stored Procedure.

## 8. Build Plan
The build plan focuses on translating the KornShell orchestration into BigQuery Stored Procedures and DML/DDL.

1.  **Define BigQuery Schema**:
    *   Create `project.dataset.job_table` to store job execution metadata (job\_kennung, eintrags\_nr, tab\_name, active\_flag, created\_ts, updated\_ts, completed\_ts, record\_count).
    *   Create `project.dataset.error_log` table (error\_ts, error\_code, error\_arg, procedure\_name, message).
    *   Identify and create target tables for `d_ausd_v_ta_period.sql` if not already present.

2.  **Migrate Utility Script Logic to BigQuery**:
    *   Re-implement parameter validation logic from `h_alis_parameter.ksh` into the main stored procedure.
    *   Re-implement date utility logic from `h_alis_date.ksh` using BigQuery date/time functions.
    *   The `f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh` functionalities will be replaced by the BigQuery error logging table and direct BigQuery SQL execution.

3.  **Translate `k_ausd_v_ta_period.ksh` to BigQuery Stored Procedure**:
    *   Create `project.dataset.r_ausd_vertrag_control` as a BigQuery Stored Procedure.
    *   Implement parameter declarations for `p_JobKennung` and `p_EintragsNr`.
    *   Add variable declarations for `v_TabName`, `v_records`, `ErrNr`, `ErrArg`.
    *   Implement parameter validation using `IF` statements.
    *   Implement error logging by `INSERT`ing into `project.dataset.error_log`.
    *   Implement job deactivation (`UPDATE job_table SET active_flag = FALSE ...`).
    *   Implement new job registration (`INSERT INTO job_table ...`).

4.  **Migrate `d_ausd_v_ta_period.sql` to BigQuery Standard SQL**:
    *   This is a separate task but crucial for the `r_ausd_vertrag_control` procedure.
    *   Translate the SQL script into BigQuery Standard SQL, addressing any dialect differences (e.g., Oracle-specific functions, PL/SQL constructs).
    *   Embed this migrated SQL within the `r_ausd_vertrag_control` stored procedure using `EXECUTE IMMEDIATE` or as direct DML/DDL, or call it as a separate BigQuery stored procedure if it's substantial.

5.  **Implement Record Counting**:
    *   Replace temporary file reading with `SELECT COUNT(*)` into the `v_records` variable.

6.  **Final Job Status Update**:
    *   Implement `UPDATE job_table` to mark the job as completed and store the `v_records` count.

7.  **Testing**:
    *   Unit test the BigQuery Stored Procedure with various parameter combinations.
    *   Integration test with the migrated `d_ausd_v_ta_period.sql` logic.
    *   Test error handling and logging.

**Language**: BigQuery Standard SQL for Stored Procedures and DML/DDL.