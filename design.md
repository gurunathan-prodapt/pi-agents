# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_geschaeftspartner.ksh`, serves as a control and orchestration layer for a data processing job. Its primary purpose is to manage the execution of an underlying SQL script, `d_ausd_geschaeftspartner.sql`, which performs the actual data transformation. The script handles parameter parsing, date validation, and is intended to manage job status in a job control table (though some functionality is commented out in the provided source). The overall business purpose is to prepare data related to "Geschaeftspartner" (business partners) for reporting or further processing.

**Key responsibilities:**
- Parsing command-line parameters (Job ID, Entry Number, Key Date, Restart Value).
- Validating the format of the key date.
- Sourcing common utility scripts for error handling, date functions, and parameter parsing.
- Orchestrating the execution of `d_ausd_geschaeftspartner.sql`.
- Capturing the number of processed records.
- (Intended) managing job status and logging in a central job table.
- (Intended) deactivating old active jobs.

## 2. Source Inventory
The job is comprised of a single KornShell script.

| File Path                                                                  | Technology  | Category | Tool       | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                         |
|:---------------------------------------------------------------------------|:------------|:---------|:-----------|:-------|:------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh` | KornShell   | shell    | KornShell  | medium | semi_auto         | This ksh script acts as a control script, parsing parameters, sourcing utility functions, and orchestrating the execution of an SQL script for data processing, including job status management. |

## 3. Target Architecture
The target platform is Google BigQuery. The orchestration logic embedded in the KornShell script will be migrated to a BigQuery Stored Procedure. The underlying SQL script (`d_ausd_geschaeftspartner.sql`, which is called by the ksh script) is assumed to be migrated to a separate BigQuery Stored Procedure or a set of BigQuery SQL statements.

**BigQuery Components:**
-   **Main Orchestration Stored Procedure:** `project.dataset.r_ausd_vertrag_control` (to replace `k_ausd_geschaeftspartner.ksh`)
    -   Will handle parameter validation, date calculations, and call the data processing procedure.
-   **Data Processing Stored Procedure:** `project.dataset.d_ausd_geschaeftspartner_proc` (to replace `d_ausd_geschaeftspartner.sql`)
    -   Will contain the core business logic for data transformation.
-   **Job Control Table:** `project.dataset.job_table`
    -   To replace the file-based/implied job tracking and to manage job status, start/end times, and record counts.
-   **Job Error Log Table:** `project.dataset.job_error_log`
    -   For logging errors and exceptions from the stored procedures.
-   **Job Run Log Table:** `project.dataset.job_run_log`
    -   For logging successful job runs and metrics.
-   **External Orchestration (Optional):** Cloud Composer (Airflow), Cloud Workflows, or BigQuery Scheduled Queries to trigger the main stored procedure.

## 4. Data Flow & Lineage
The original script's data flow is primarily orchestrating the execution of an SQL script that interacts with a database.

**Legacy Flow:**
1.  `k_ausd_geschaeftspartner.ksh` is invoked with parameters.
2.  Environment variables and utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) are sourced/executed.
3.  Parameters are parsed and validated.
4.  Date is validated.
5.  `starteSQLSkript` (an assumed wrapper for SQL*Plus) is called, which in turn executes `d_ausd_geschaeftspartner.sql` with several parameters.
6.  The record count from the SQL script's output is written to a temporary file, then read back into a shell variable.
7.  (Intended) Job control table is updated.

**Target BigQuery Flow:**
1.  An external orchestrator (e.g., Cloud Composer) or a BigQuery Scheduled Query calls the `project.dataset.r_ausd_vertrag_control` stored procedure with the required parameters.
2.  Inside `r_ausd_vertrag_control`:
    a.  Parameters are validated using BigQuery's `IF` conditions and `RAISE` statements.
    b.  Date format is validated using `SAFE.PARSE_DATE`.
    c.  `CURRENT_DATE()` and `DATE_SUB()` replace `gestern.ksh` for date calculations.
    d.  The `project.dataset.d_ausd_geschaeftspartner_proc` stored procedure is called, passing relevant parameters including variables for record counts.
    e.  Error handling is managed via `BEGIN...EXCEPTION` blocks, logging to `project.dataset.job_error_log`.
    f.  `project.dataset.job_run_log` and `project.dataset.job_table` are updated with job status and record counts using BigQuery DML.
    g.  A return message signifies completion.

## 5. Transformation Logic
The transformation logic itself is primarily contained within the `d_ausd_geschaeftspartner.sql` script, which is external to `k_ausd_geschaeftspartner.ksh`. The `k_ausd_geschaeftspartner.ksh` script focuses on orchestration.

**Orchestration Logic Migration:**

*   **Parameter Handling:**
    *   **Legacy:** `getopts` for command-line arguments `j`, `f`, `s`, `l`.
    *   **Target:** BigQuery Stored Procedure parameters: `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
*   **Environment Initialization:**
    *   **Legacy:** `. $HOME/.dw_init` and sourcing various `.ksh` utilities.
    *   **Target:** Not directly replicable. BigQuery Stored Procedures are self-contained. Any configurations (`BERT_DIR_ROOT`, `DW_DIR_UTL`) should be replaced by BigQuery dataset/table references, constants within the procedure, or passed as additional parameters. Utility functions for error handling, date checks, and parameter checks will be converted into BigQuery SQL logic (e.g., `IF`, `RAISE`, `SAFE.PARSE_DATE`).
*   **Parameter Validation:**
    *   **Legacy:** `pruefeParameterGesetzt` function calls; `if [ ! $ErrNr -eq 0 ]` for error checking.
    *   **Target:** `IF...RAISE USING MESSAGE` blocks within the `BEGIN...EXCEPTION` structure. Errors will be logged to `project.dataset.job_error_log`.
*   **Date Validation:**
    *   **Legacy:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'`.
    *   **Target:** `SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag); IF v_stichtag_date IS NULL THEN RAISE USING MESSAGE...`.
*   **SQL Script Execution:**
    *   **Legacy:** `starteSQLSkript $p_EintragsNr $Name_SQLskript ...`. This implies an SQL*Plus execution.
    *   **Target:** `CALL project.dataset.d_ausd_geschaeftspartner_proc(...)`. The actual SQL logic from `d_ausd_geschaeftspartner.sql` will be implemented inside `d_ausd_geschaeftspartner_proc`.
*   **Temporary File for Record Count:**
    *   **Legacy:** `tmpFile="..."`, `eval "v_records=\`cat $tmpFile\`"`.
    *   **Target:** The record count will be an `OUT` parameter of `d_ausd_geschaeftspartner_proc` or a variable within `r_ausd_vertrag_control`, stored directly in the `job_run_log` or `job_table`.
*   **Date Calculation (`gestern.ksh`):**
    *   **Legacy:** `set \`gestern.ksh\``
    *   **Target:** BigQuery date functions: `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **Job Table Management:**
    *   **Legacy:** Commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` indicate intended functionality.
    *   **Target:** Explicit `UPDATE` and `INSERT` DML statements against `project.dataset.job_table` and `project.dataset.job_run_log`.

**Pseudocode for BigQuery Stored Procedure (`r_ausd_vertrag_control`):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolVertrag';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_sql_script STRING DEFAULT 'project.dataset.d_ausd_geschaeftspartner_sql'; -- Reference to the child procedure
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart INT64 DEFAULT 0;

  -- Parameter Validation
  BEGIN
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN RAISE USING MESSAGE = 'Jobkennung fehlt'; END IF;
    IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN RAISE USING MESSAGE = 'Stichtag fehlt'; END IF;
    IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN RAISE USING MESSAGE = 'EintragsNr fehlt'; END IF;
  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, stichtag, error_message, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, p_Stichtag, @@error.message, CURRENT_TIMESTAMP());
    RAISE;
  END;

  -- Date Validation
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN RAISE USING MESSAGE = 'Datum hat nicht das Format DDMMYYYY'; END IF;

  -- Initialize restart value
  IF p_wiederanlaufWert IS NULL THEN SET v_restart = 0; ELSE SET v_restart = p_wiederanlaufWert; END IF;

  -- Deactivation of active jobs (if required, implement DML here)
  -- UPDATE `project.dataset.job_table` SET active_flag = 'N' WHERE table_name = v_TabName AND active_flag = 'A';

  -- Call the migrated SQL logic procedure
  CALL `project.dataset.d_ausd_geschaeftspartner_proc`(
    p_EintragsNr,
    p_JobKennung,
    v_stichtag_date,
    v_restart,
    v_datum_heute,
    v_datum_gestern,
    v_records -- OUT parameter
  );

  -- Log successful run
  INSERT INTO `project.dataset.job_run_log`
  (tab_name, job_kennung, eintrags_nr, stichtag, records_processed, status, created_ts)
  VALUES (v_TabName, p_JobKennung, p_EintragsNr, v_stichtag_date, v_records, 'SUCCESS', CURRENT_TIMESTAMP());

  -- Update job control table
  INSERT INTO `project.dataset.job_table`
  (tab_name, active_flag, process_flag, from_date, to_date, job_type, restart_flag, record_count, description)
  VALUES (v_TabName, 'A', 'I', v_stichtag_date, v_stichtag_date, 'J', 'N', v_records, 'Initialbefuellung');

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;
```
**Pseudocode for BigQuery Stored Procedure (`d_ausd_geschaeftspartner_proc` - placeholder for actual SQL script migration):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_geschaeftspartner_proc`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN p_Stichtag DATE,
  IN p_wiederanlaufWert INT64,
  IN p_datum_heute DATE,
  IN p_datum_gestern DATE,
  OUT p_records INT64
)
BEGIN
  DECLARE v_rows INT64 DEFAULT 0;

  -- Placeholder for the migrated logic from d_ausd_geschaeftspartner.sql
  -- This is where the actual SELECT, INSERT, UPDATE, DELETE statements
  -- from the original SQL script will be translated to BigQuery SQL.
  -- Example:
  -- INSERT INTO `project.dataset.target_table` (col1, col2, ...)
  -- SELECT src.col1, src.col2, ...
  -- FROM `project.dataset.source_table` src
  -- WHERE src.business_date = p_Stichtag;

  -- Example of how to get record count for demonstration:
  SET v_rows = (
    SELECT COUNT(*)
    FROM `project.dataset.some_target_table`
    WHERE processing_date = p_Stichtag
  );

  SET p_records = v_rows;
END;
```

## 6. External Dependencies
The original script's external dependencies are primarily other shell scripts and an assumed Oracle database (via `SQL*Plus` wrapper for `d_ausd_geschaeftspartner.sql`).

| Legacy External Dependency | How Replaced in BigQuery                                                               |
|:---------------------------|:---------------------------------------------------------------------------------------|
| Oracle Database (implied by SQL script execution) | BigQuery tables and procedures. Data should be ingested into BigQuery.       |
| `$HOME/.dw_init`           | Configuration values moved into BigQuery Stored Procedure constants or configuration tables. |
| `f_alis_msgerr.ksh`        | Replaced by BigQuery `RAISE USING MESSAGE` and logging to `project.dataset.job_error_log`. |
| `h_alis_date.ksh`          | Replaced by BigQuery native date functions (e.g., `SAFE.PARSE_DATE`, `CURRENT_DATE`, `DATE_SUB`). |
| `h_alis_parameter.ksh`     | Replaced by BigQuery Stored Procedure parameter declarations and `IF` conditions.       |
| `h_alis_sqlplus.ksh`       | Replaced by direct `CALL` to BigQuery Stored Procedures.                               |
| `gestern.ksh`              | Replaced by BigQuery date functions: `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`. |
| `d_ausd_geschaeftspartner.sql` | Migrated to a dedicated BigQuery Stored Procedure: `project.dataset.d_ausd_geschaeftspartner_proc`. |
| Temporary file (`$DW_DIR_UTL/bert_k_ausd_geschaeftspartner_$$.tmp`) | Replaced by BigQuery Stored Procedure variables (e.g., `v_records`) or direct DML updates to log tables. |
| Job Table (implied)        | Dedicated BigQuery `project.dataset.job_table` and `project.dataset.job_run_log` tables. |

No other external systems (like S3, SFTP) were detected.

## 7. Unresolved / Risks
- **Underlying SQL Logic Complexity:** The core data transformation logic resides in `d_ausd_geschaeftspartner.sql`. Its complexity (e.g., use of Oracle-specific SQL, complex PL/SQL, procedures, functions) will directly impact the effort required for its migration to `project.dataset.d_ausd_geschaeftspartner_proc`. This is currently an unknown factor.
- **Commented-out Code:** The script contains commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. It's crucial to confirm whether this functionality was intended to be active or if it's dead code. If active, the corresponding logic needs to be fully migrated and implemented in the BigQuery stored procedure.
- **Environment Variables/Paths:** The script relies heavily on shell environment variables like `BERT_DIR_ROOT` and `DW_DIR_UTL`. Their exact values and contents need to be fully understood to correctly map file paths to BigQuery dataset/table references or configure constants within the new environment.
- **Error Handling Details:** The `f_alis_msgerr.ksh` suggests a specific error handling framework. While a general BigQuery `EXCEPTION` block is provided, a detailed understanding of the legacy error codes (`ErrNr`) and messaging (`DWMSG_MeldeFehler`) is needed for a faithful migration of error reporting.
- **`starteSQLSkript` functionality:** The exact functionality of `starteSQLSkript` (e.g., connection details, error handling within the SQL execution, output parsing) needs to be analyzed to ensure the `CALL` to the BigQuery stored procedure replicates its behavior correctly.

## 8. Build Plan
The migration will involve creating BigQuery DDL/DML for tables and procedures.

1.  **Create BigQuery Log and Control Tables (BQ SQL DDL):**
    *   `project.dataset.job_error_log`
    *   `project.dataset.job_run_log`
    *   `project.dataset.job_table`

2.  **Migrate and Create `d_ausd_geschaeftspartner.sql` to `project.dataset.d_ausd_geschaeftspartner_proc` (BQ SQL DDL/DML):**
    *   This step requires detailed analysis of `d_ausd_geschaeftspartner.sql` to translate its logic into BigQuery SQL. This might involve creating intermediate views, tables, or complex SQL logic within the procedure.

3.  **Create Orchestration Stored Procedure `project.dataset.r_ausd_vertrag_control` (BQ SQL DDL):**
    *   Implement the parameter parsing, validation, date calculations, calls to `d_ausd_geschaeftspartner_proc`, and updates to the log/control tables as detailed in Section 5.

4.  **Develop External Orchestration (e.g., Cloud Composer DAG - Python):**
    *   Create an Airflow DAG or similar mechanism to schedule and trigger the `project.dataset.r_ausd_vertrag_control` stored procedure, passing necessary runtime parameters.

5.  **Data Ingestion (if `d_ausd_geschaeftspartner.sql` reads from non-BigQuery sources):**
    *   If `d_ausd_geschaeftspartner.sql` sources data from Oracle or other legacy systems, establish a data ingestion pipeline (e.g., Cloud Data Fusion, Dataflow, Striim) to bring that data into BigQuery.