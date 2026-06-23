# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

## 1. Purpose & Scope

This KornShell script, `k_ausd_v_ta_period.ksh`, serves as a control script (`Kontrollscript`) for a data preparation job related to the `ta_period` table. Its primary purpose is to orchestrate the execution of a SQL script (`d_ausd_v_ta_period.sql`), ensuring proper parameter handling, environment setup, and error management. It also manages job status, ignoring active jobs, and deactivating old active jobs. The job ensures that the SQL script performs data manipulation on the `ta_period` table, likely for data extraction or reporting.

The scope of this migration is to translate the control logic and SQL execution orchestration of this shell script into a BigQuery-native solution, ideally a BigQuery Stored Procedure, while maintaining its original functionality.

## 2. Source Inventory

This job comprises a single source file:

- **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh`
  - **Technology**: KornShell (scripting)
  - **Tier**: `medium`
  - **Automation Bucket**: `semi_auto`
  - **Summary**: A control script that handles parameter parsing, environment setup, error handling, and orchestrates the execution of an external SQL script (`d_ausd_v_ta_period.sql`) for data manipulation on the `ta_period` table.

## 3. Target Architecture

The target architecture will leverage BigQuery's capabilities, primarily BigQuery Stored Procedures for the orchestration logic.

- **BigQuery Stored Procedure**: The shell script's control flow, parameter handling, and job status management will be encapsulated within a BigQuery Stored Procedure. This procedure will accept input parameters and manage the execution of the core SQL logic.
- **BigQuery Tables**:
    - **`ta_period`**: The primary target table for data manipulation, as indicated by the original script.
    - **`job_table` (Proposed)**: A new or existing BigQuery table to manage job status, similar to the legacy job-control mechanism. This table will track job identifiers, entry numbers, script names, status (RUNNING, DONE, DEACTIVATED), and active flags.
- **BigQuery SQL Script**: The logic from the external `d_ausd_v_ta_period.sql` will be translated into a BigQuery-compatible SQL script, which will be executed or embedded within the BigQuery Stored Procedure.
- **External Orchestration (Optional)**: If complex external job scheduling or specific system interactions cannot be directly translated to BigQuery Stored Procedures, Google Cloud Composer (Apache Airflow) or Cloud Workflows could be used to trigger the BigQuery Stored Procedure. However, the initial design aims for a BigQuery-native solution.

## 4. Data Flow & Lineage

The original script's data flow is primarily orchestrating the execution of a SQL script that interacts with the `ta_period` table.

**Legacy Data Flow:**

1.  **Start**: `k_ausd_v_ta_period.ksh` execution.
2.  **Environment Setup**: Sources various helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  **Parameter Parsing**: `getopts` parses `j:` (JobKennung) and `f:` (EintragsNr).
4.  **Parameter Validation**: Calls `pruefeParameterGesetzt` (from `h_alis_parameter.ksh`) to validate `Jobkennung` and `EintragsNr`. If validation fails, `DWMSG_MeldeFehler` is called, and the script exits.
5.  **SQL Script Execution**: Calls `starteSQLSkript` (from `h_alis_sqlplus.ksh`) with parameters including `d_ausd_v_ta_period.sql`. This wrapper likely handles job status updates and database interaction.
6.  **Record Count**: The `starteSQLSkript` (or the underlying SQL execution) writes a record count to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp`).
7.  **Result Capture**: `eval "v_records=`cat $tmpFile`"` reads the record count from the temporary file into the `v_records` shell variable.
8.  **End**: Script completes.

**Target BigQuery Data Flow:**

1.  **Start**: BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control` is invoked with `p_JobKennung` and `p_EintragsNr` parameters (e.g., via Cloud Scheduler, Cloud Composer, or manual execution).
2.  **Parameter Validation**: Input parameters are validated within the stored procedure. Invalid parameters will raise an error.
3.  **Job Status Management**:
    -   Updates `project.dataset.job_table` to deactivate old active jobs for the given `p_JobKennung`.
    -   Inserts a new record into `project.dataset.job_table` with `status = 'RUNNING'` for the current execution.
4.  **Core SQL Logic Execution**: The translated BigQuery SQL from `d_ausd_v_ta_period.sql` is executed. This can be embedded directly or called as another BigQuery routine.
5.  **Record Count Capture**: The number of affected/processed records is captured directly within BigQuery (e.g., `SELECT COUNT(*)` on the target table after modification or using `ROW_COUNT()`).
6.  **Job Status Update**: `project.dataset.job_table` is updated with `record_count`, `status = 'DONE'`, and `active_flag = FALSE`.
7.  **End**: Stored Procedure completes, returning the record count if desired.

## 5. Transformation Logic

The migration primarily involves refactoring shell script constructs into BigQuery SQL scripting and procedural logic.

-   **Parameter Handling**:
    -   **Legacy**: `getopts` for command-line arguments.
    -   **Target**: BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING`, `IN p_EintragsNr STRING`).
-   **Environment Loading**:
    -   **Legacy**: Sourcing `.dw_init`, helper KSH scripts.
    -   **Target**: Replaced by BigQuery's inherent environment, explicit variable declarations within the stored procedure, or by hardcoding configuration values if they are static.
-   **Error Handling**:
    -   **Legacy**: `f_alis_msgerr.ksh` for error messaging, shell `exit` codes.
    -   **Target**: BigQuery's `RAISE` statement for error signaling, with descriptive messages.
-   **SQL Execution Orchestration**:
    -   **Legacy**: `starteSQLSkript` wrapper calling `d_ausd_v_ta_period.sql`.
    -   **Target**: The core logic of `d_ausd_v_ta_period.sql` will be directly translated into BigQuery SQL and embedded within the stored procedure or called as a separate BigQuery script/routine. The job-control aspects (ignoring active jobs, deactivating old ones) will be implemented as `UPDATE` statements on the `job_table`.
-   **Temporary File for Communication**:
    -   **Legacy**: Record count written to `$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp` and read via `cat` and `eval`.
    -   **Target**: Replaced by BigQuery scripting variables (`DECLARE v_records INT64;`) or direct `UPDATE` statements to the `job_table` for persistent storage of metrics.
-   **Table Naming**: `v_TabName='ta_period'` will be replaced by direct table references in BigQuery SQL, e.g., ``project.dataset.ta_period``.
-   **Shell Utilities**: Helper scripts like `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` will be replaced by native BigQuery functions, scripting constructs, or incorporated into the stored procedure's logic.

**BQ SQL Pseudocode (from MCP tool):**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_period';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_sql_script_name STRING DEFAULT 'd_ausd_v_ta_period.sql';

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Jobkennung';
  END IF;

  IF (p_EintragsNr IS NULL OR p_EintragsNr = '') AND v_err_nr = 0 THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'EintragsNr';
  END IF;

  IF v_err_nr != 0 THEN
    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' ', v_err_arg);
  END IF;

  -- Deactivate old active jobs for same job key if required
  UPDATE `project.dataset.job_table`
  SET active_flag = FALSE,
      status = 'DEACTIVATED',
      updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE;

  -- Insert current job execution record
  INSERT INTO `project.dataset.job_table` (
    job_kennung,
    eintrags_nr,
    script_name,
    tab_name,
    status,
    active_flag,
    created_ts,
    updated_ts
  )
  VALUES (
    p_JobKennung,
    p_EintragsNr,
    v_sql_script_name,
    v_TabName,
    'RUNNING',
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );

  -- Execute core SQL logic translated from d_ausd_v_ta_period.sql
  -- This section requires the actual content of d_ausd_v_ta_period.sql to be translated
  -- Example:
  -- INSERT INTO `project.dataset.ta_period` (...) SELECT ... FROM ...;
  -- Or: MERGE INTO `project.dataset.ta_period` T USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED ...;

  -- Example record count capture
  -- Replace with actual logic to count records affected by d_ausd_v_ta_period.sql
  SET v_records = (
    SELECT COUNT(*) FROM `project.dataset.ta_period` WHERE 1 = 1 -- apply relevant filters
  );

  -- Persist result count and update job status
  UPDATE `project.dataset.job_table`
  SET record_count = v_records,
      status = 'DONE',
      active_flag = FALSE,
      updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND script_name = v_sql_script_name
    AND active_flag = TRUE;

  SELECT v_records AS v_records;
END;
```

## 6. External Dependencies

There are no external systems explicitly identified in the `lineage_assembled_jobs` metadata (`external_systems: []`). However, the script itself implies some dependencies:

-   **Legacy**: Shell environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`).
    -   **Replacement**: BigQuery environment is self-contained. Environment variables will be replaced by explicit string literals, BigQuery scripting variables, or potentially managed through a BigQuery configuration table if dynamic.
-   **Legacy**: Helper KSH scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    -   **Replacement**: The functionality of these scripts will be integrated directly into the BigQuery Stored Procedure using BigQuery SQL scripting capabilities, built-in functions, or custom UDFs if needed.
-   **Legacy**: `d_ausd_v_ta_period.sql` (an external SQL file).
    -   **Replacement**: The content of this SQL file will be translated to BigQuery SQL and either embedded within the main stored procedure or called as a separate BigQuery routine.
-   **Legacy**: Temporary file system for communication (`$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp`).
    -   **Replacement**: Direct use of BigQuery scripting variables (`DECLARE`) or updates to a dedicated `job_table` to persist intermediate results or metadata.
-   **Legacy**: Implicit "job-table" or similar mechanism for tracking active jobs.
    -   **Replacement**: A dedicated BigQuery `job_table` will be created and managed by the stored procedure to replicate this functionality.

## 7. Unresolved / Risks

-   **Content of `d_ausd_v_ta_period.sql`**: The actual SQL logic within `d_ausd_v_ta_period.sql` is unknown. Its complexity, dialect (e.g., Oracle SQL, Teradata SQL), and specific transformations will significantly impact the migration effort. It is assumed to be translatable to BigQuery SQL. If it contains highly procedural or database-specific functions, further analysis and potentially more complex BigQuery UDFs or external Python logic might be required.
-   **`starteSQLSkript` Logic**: The exact implementation details of `starteSQLSkript` from `h_alis_sqlplus.ksh` are not fully known. The design assumes it primarily orchestrates SQL execution and job status updates. Any hidden side effects or complex logic within this wrapper might require additional BigQuery scripting or external orchestration.
-   **Error Framework `f_alis_msgerr.ksh`**: While the design proposes using BigQuery's `RAISE` statement, the full capabilities of the legacy error framework are unknown. If it involves complex logging or alerting mechanisms, these may need to be integrated with Google Cloud Logging and Monitoring services.
-   **Historization**: No explicit historization flags were found (`historization_candidate: 0`). If the `ta_period` table or related data requires historization, this needs to be explicitly designed (e.g., using BigQuery's partitioning/clustering, or creating snapshot tables).
-   **Unresolved Lineage Edges**: The `lineage_edges` query for this specific file returned no direct edges. While static analysis revealed dependencies, the absence of explicit graph edges suggests potential gaps in automated dependency detection for this job type. This implies a higher reliance on manual code review for complete dependency mapping, especially for downstream consumers of `ta_period`.

## 8. Build Plan

The build plan outlines the ordered steps to implement the migration.

1.  **Analyze `d_ausd_v_ta_period.sql`**:
    *   Read the source file for `d_ausd_v_ta_period.sql`.
    *   Analyze its SQL dialect and identify BigQuery-compatible equivalents for all statements, functions, and data types.
    *   **Tool**: `read_source_file`, `hql_sql_to_bqsql_design` (if SQL-like) or manual translation.
    *   **Output**: BigQuery SQL script for the core data manipulation logic.
2.  **Design and Create `job_table`**:
    *   Define the schema for a BigQuery `job_table` to track job executions, statuses, and metadata (e.g., `job_kennung`, `eintrags_nr`, `script_name`, `tab_name`, `status`, `active_flag`, `created_ts`, `updated_ts`, `record_count`).
    *   **Tool**: Manual design.
    *   **Output**: DDL for `project.dataset.job_table`.
3.  **Develop BigQuery Stored Procedure (`r_ausd_vertrag_control`)**:
    *   Implement the parameter validation logic.
    *   Implement the job status update logic for deactivating old jobs and inserting new job records into `project.dataset.job_table`.
    *   Integrate the translated BigQuery SQL from `d_ausd_v_ta_period.sql`.
    *   Implement the record count capture and final job status update.
    *   **Tool**: `shellscript_to_bqsql_design` (initial scaffold provided) followed by manual development and `bqsql_build`.
    *   **Output**: BigQuery Stored Procedure definition (`.sql` file).
4.  **Testing**:
    *   Unit test the `r_ausd_vertrag_control` stored procedure with various parameter inputs (valid, invalid, missing).
    *   Verify correct updates to the `job_table`.
    *   Verify the data manipulation on `ta_period` matches legacy behavior.
    *   **Tool**: `bqsql_test`.
    *   **Output**: Test cases and execution results.
5.  **Scheduling (if external orchestration is needed)**:
    *   If the job needs to be triggered by a scheduler, configure a Cloud Scheduler job to invoke the BigQuery Stored Procedure.
    *   For more complex workflows, create a Cloud Composer (Airflow) DAG to orchestrate the execution.
    *   **Tool**: `uc4_to_airflow_dag_design` (if legacy scheduler is UC4), manual Cloud Composer DAG development.
    *   **Output**: Cloud Scheduler configuration or Airflow DAG (`.py` file).