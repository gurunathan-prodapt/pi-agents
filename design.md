# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_drop_temp_table.ksh

## 1. Purpose & Scope
This shell script, `k_drop_temp_table.ksh`, serves as a control script for the process of dropping temporary tables. Its primary functions include:
- Parsing command-line parameters (`Job ID`, `Entry Number`, `Stichtag`, `Restart Value`).
- Validating the provided parameters and date formats.
- Managing environmental variables and sourcing helper scripts for error handling, date operations, and SQL*Plus execution.
- Executing an underlying SQL script (`d_drop_temp_table.sql`) responsible for the actual dropping of temporary tables, passing relevant parameters.
- Handling job management, including deactivating old active jobs (though some related calls are commented out in the current version) and potentially creating new job entries.
- Logging progress and errors.

The scope of this migration is to re-platform this KornShell script and its associated SQL logic to Google Cloud's BigQuery platform, leveraging BigQuery Stored Procedures for the core logic and potentially Cloud Composer for orchestration of any remaining external dependencies or scheduling.

## 2. Source Inventory
The job consists of a single primary source file:

- **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_drop_temp_table.ksh`
    - **Technology:** KornShell (ksh) script
    - **Detected Tool:** KornShell
    - **Category:** Shell script
    - **Purpose:** Orchestration, Utility, Housekeeping
    - **Complexity Tier:** (Not explicitly available, but inferring from purpose: likely `medium` due to parameter parsing, external script calls, and SQL interaction)
    - **Automation Bucket:** (Not explicitly available, but based on shell script with SQL interaction to BQ, it's likely `B1 - Auto` or `B2 - Semi-Auto` depending on complexity of `d_drop_temp_table.sql` and `starteSQLSkript` function)
    - **Summary:** This ksh script acts as a control script for dropping temporary tables. It parses parameters, performs date validation, executes a SQL script to drop temporary tables, and handles job management.

The script relies on an external SQL file (`d_drop_temp_table.sql`) and interacts with a table named `PoolVertrag`. It also sources several utility KornShell scripts.

## 3. Target Architecture
The `k_drop_temp_table.ksh` script will be migrated to a BigQuery Stored Procedure. This stored procedure will encapsulate the parameter parsing, validation, date derivation, and the execution logic of the `d_drop_temp_table.sql`.

- **Main Component:** BigQuery Stored Procedure: `dataset.r_drop_temp_table_control`
- **SQL Logic:** The content of `d_drop_temp_table.sql` will be either integrated directly into the `r_drop_temp_table_control` stored procedure or translated into a separate BigQuery Stored Procedure (e.g., `dataset.d_drop_temp_table`) that is then called by `r_drop_temp_table_control`.
- **Logging:** A new BigQuery table, `dataset.job_error_log`, will be created to capture error messages, replacing the current file-based and console logging.
- **Job Control:** A BigQuery table, `dataset.job_table`, will be used for job status and entry management, replacing the legacy job-table interactions.
- **Orchestration:** While the core logic moves to BigQuery, if external scheduling or complex cross-system dependencies exist, Google Cloud Composer (Airflow) could be used to trigger the BigQuery Stored Procedure. For this specific job, a simple scheduled query in BigQuery or Cloud Scheduler could suffice.

## 4. Data Flow & Lineage
The original script's data flow and lineage involve:

1.  **Input Parameters:**
    - `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` are passed as command-line arguments.
2.  **Environmental Setup & Utilities:**
    - `$HOME/.dw_init` (environment initialization)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date validation)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus wrapper)
    - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (date derivation for today/yesterday/month)
3.  **Core Logic:**
    - The script parses and validates parameters.
    - `DWDate_Datum_Check` function validates `p_Stichtag`.
    - `gestern.ksh` is called to derive `p_datum_heute`, `p_datum_gestern`, `p_monat_heute`, `p_monat_gestern`.
    - The `starteSQLSkript` function is called, which internally executes `d_drop_temp_table.sql` using `sqlplus`. Parameters are passed to the SQL script.
    - The SQL script interacts with the `PoolVertrag` table (both reads and writes/drops).
    - A temporary file (`$DW_DIR_UTL/bert_k_drop_temp_table_$$.tmp`) is used to store the record count returned by the SQL script.
4.  **Output & Logging:**
    - Console output for status and errors.
    - Error messages via `DWMSG_MeldeFehler`.
    - Record count is read from the temporary file.
    - (Commented) `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` would interact with a job control table, likely using `PoolVertrag` as a parameter.

**Target BigQuery Data Flow:**

1.  **BigQuery Stored Procedure (`dataset.r_drop_temp_table_control`) receives:**
    - Input parameters: `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
2.  **Internal Logic:**
    - Parameter validation using `IF...THEN...END IF` blocks.
    - Date validation using `SAFE.PARSE_DATE`.
    - Date derivation using BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`, `FORMAT_DATE`).
    - Execution of the SQL logic (either inline or via `CALL dataset.d_drop_temp_table`).
    - The SQL logic (from `d_drop_temp_table.sql`) will perform `DROP TABLE` operations on temporary tables, potentially referencing or modifying the `PoolVertrag` table or similar staging tables.
3.  **Logging & Job Control:**
    - Error logging will be directed to `dataset.job_error_log` (INSERT statements).
    - Job status updates will be handled by INSERT/UPDATE statements into `dataset.job_table`.
    - Record counts will be managed via BigQuery variables or temporary tables instead of file I/O.

## 5. Transformation Logic
The core transformation involves moving from a shell-orchestrated SQL process to a native BigQuery Stored Procedure.

**Original Script Constructs vs. BigQuery SQL Equivalents:**

*   **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters (e.g., `p_JobKennung STRING`).
*   **Environment Variables (e.g., `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** Replaced by procedure parameters, BigQuery constants, or values from a configuration table.
*   **Sourcing Utility Scripts (e.g., `h_alis_date.ksh`, `f_alis_msgerr.ksh`):**
    *   Date validation logic (`DWDate_Datum_Check`) will be replaced by BigQuery's `SAFE.PARSE_DATE` and standard date functions.
    *   Error handling (`DWMSG_MeldeFehler`) will be replaced by `INSERT` statements into a dedicated `job_error_log` table and `SIGNAL SQLSTATE` for error propagation.
    *   Parameter validation (`pruefeParameterGesetzt`) will be replaced by `IF...THEN...END IF` blocks within the stored procedure.
    *   SQL*Plus execution (`h_alis_sqlplus.ksh`, `starteSQLSkript`) will be replaced by `EXECUTE IMMEDIATE` for dynamic SQL or `CALL` for another BigQuery Stored Procedure that contains the logic from `d_drop_temp_table.sql`.
*   **Date Derivation (`gestern.ksh`):** Replaced by BigQuery's `CURRENT_DATE()`, `DATE_SUB`, `FORMAT_DATE` functions.
*   **Temporary Files (`tmpFile` for record count):** Replaced by BigQuery `DECLARE` variables within the stored procedure or by inserting results into a staging/log table.
*   **Conditional Logic (`if [[ -z ... ]]`, `if [ ! $ErrNr -eq 0 ]`):** Replaced by BigQuery Scripting `IF...THEN...END IF` and `CASE` statements.
*   **Console Output (`print`, `echo`):** Replaced by `SELECT` statements for logging messages (which can be captured by an orchestrator) or `INSERT` into a logging table.
*   **Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** These commented-out calls, if intended for future use, will be implemented as `INSERT` or `UPDATE` statements into a BigQuery job control table (`dataset.job_table`).

**Pseudocode for BigQuery Stored Procedure (`dataset.r_drop_temp_table_control`):**

```sql
CREATE OR REPLACE PROCEDURE dataset.r_drop_temp_table_control(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert INT64
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'PoolVertrag';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_monat_heute STRING;
  DECLARE v_monat_gestern STRING;
  DECLARE v_restart INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Stichtag';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO dataset.job_error_log
    (job_name, error_nr, error_arg, created_at)
    VALUES
    ('r_drop_temp_table_control', ErrNr, ErrArg, CURRENT_TIMESTAMP());

    SELECT CONCAT('FEHLER: 0 E ', CAST(ErrNr AS STRING), ' ', ErrArg) AS message;
    RETURN; -- Exit procedure
  END IF;

  -- Date format validation
  IF SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL THEN
    INSERT INTO dataset.job_error_log
    (job_name, error_nr, error_arg, created_at)
    VALUES
    ('r_drop_temp_table_control', 193, p_Stichtag, CURRENT_TIMESTAMP());

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Invalid date format for Stichtag. Expected DDMMYYYY.';
  END IF;

  -- Derive dates
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  SET v_monat_heute = FORMAT_DATE('%m', v_datum_heute);
  SET v_monat_gestern = FORMAT_DATE('%m', v_datum_gestern);

  -- Initialize restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET v_restart = 0;
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Execute the SQL logic (from d_drop_temp_table.sql)
  -- This assumes d_drop_temp_table is also a BigQuery Stored Procedure
  -- and returns the record count.
  CALL dataset.d_drop_temp_table(
      p_EintragsNr,
      p_JobKennung,
      p_Stichtag,
      v_restart,
      v_datum_heute,
      v_datum_gestern,
      v_monat_heute,
      v_monat_gestern,
      v_records -- Output parameter for record count
  );

  -- Handle record count (v_records would be set by d_drop_temp_table)

  -- (Re-enable and implement if job management was active)
  -- INSERT INTO dataset.job_table (tab_name, status, mode, stichtag_from, stichtag_to, job_type, active_flag, records, description)
  -- VALUES
  -- (v_TabName, 'A', 'I', PARSE_DATE('%d%m%Y', p_Stichtag), PARSE_DATE('%d%m%Y', p_Stichtag), 'J', 'N', v_records, 'Initialbefuellung');

  SELECT '---------- ENDE Datenverarbeitung ----------' AS message;
END;
```

## 6. External Dependencies
The script has several external dependencies:

| Original External System/Component | Description                                                                   | BigQuery Replacement Strategy                                                                               |
| :--------------------------------- | :---------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------- |
| `$HOME/.dw_init`                   | Environment initialization script.                                            | Replaced by BigQuery environment (e.g., dataset/project config, or direct parameter passing).                 |
| `f_alis_msgerr.ksh`                | Error messaging utility.                                                      | Replaced by `INSERT` statements into `dataset.job_error_log` table and BigQuery `SIGNAL SQLSTATE`.           |
| `h_alis_date.ksh`                  | Date utility for validation.                                                  | Replaced by BigQuery standard date functions (`SAFE.PARSE_DATE`, `FORMAT_DATE`).                            |
| `h_alis_parameter.ksh`             | Parameter parsing utility.                                                    | Replaced by BigQuery Stored Procedure input parameters and explicit `IF` condition checks.                  |
| `h_alis_sqlplus.ksh`               | SQL*Plus execution wrapper.                                                   | Replaced by BigQuery `EXECUTE IMMEDIATE` for dynamic SQL or `CALL` to other BigQuery Stored Procedures.     |
| `gestern.ksh`                      | External script for deriving today's/yesterday's dates and months.            | Replaced by BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`, `FORMAT_DATE`).                          |
| `d_drop_temp_table.sql`            | The core SQL script for dropping temporary tables, invoked by ksh script.     | Migrated to a separate BigQuery Stored Procedure (`dataset.d_drop_temp_table`) called by the main procedure. |
| `PoolVertrag` table                | The primary table the SQL script operates on.                                 | Migrated to a BigQuery table (e.g., `project.dataset.PoolVertrag`).                                         |
| `sqlplus`                          | Oracle database client used for executing SQL.                                | Replaced by native BigQuery SQL execution.                                                                  |
| Temporary files (e.g., `$tmpFile`) | Used to store intermediate results like record counts.                        | Replaced by BigQuery variables or temporary tables within the stored procedure.                             |
| Job Control Table                  | (Implied by `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) for job status/logging.| Replaced by a BigQuery table (`dataset.job_table`) for tracking job entries.                                |

## 7. Unresolved / Risks
*   **Exact Logic of `d_drop_temp_table.sql`:** The actual content of `d_drop_temp_table.sql` was not available for detailed analysis. Its complexity and specific DDL operations will determine the final BigQuery SQL translation. It is assumed to be a series of `DROP TABLE` or `TRUNCATE TABLE` statements.
*   **`starteSQLSkript` function:** The precise implementation of `starteSQLSkript` was not provided, but it is assumed to be a wrapper around `sqlplus` execution. This assumption impacts the direct translation to `EXECUTE IMMEDIATE` or a `CALL` to a dedicated BQ procedure.
*   **Commented Job Management Calls:** The calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` are commented out. This implies they are currently inactive but might be intended for future use. The design provides for BigQuery table replacements if these functionalities are to be re-activated.
*   **Migration of Utility Scripts:** The general utility scripts (e.g., `h_alis_msgerr.ksh`) are replaced by BigQuery native constructs. If they contain complex, reusable logic beyond basic error reporting or date handling, those specific patterns might need further analysis for re-implementation as BigQuery UDFs or functions.
*   **Complexity Tier & Automation Rate:** The absence of entries in `file_complexity` and `automation_rate` for this specific file suggests a lack of prior automated analysis or categorization. The manual assessment implies a `medium` complexity, falling into `B1` or `B2` migration buckets.

## 8. Build Plan
1.  **Define BigQuery Dataset:** Ensure a target BigQuery dataset (e.g., `project.dataset`) exists for the migrated objects.
2.  **Create Logging Table DDL:**
    - Create `dataset.job_error_log` table to store error details.
    ```sql
    CREATE TABLE IF NOT EXISTS dataset.job_error_log (
        job_name STRING,
        error_nr INT64,
        error_arg STRING,
        created_at TIMESTAMP
    );
    ```
3.  **Create Job Control Table DDL (if reactivating commented logic):**
    - Create `dataset.job_table` for job management.
    ```sql
    CREATE TABLE IF NOT EXISTS dataset.job_table (
        tab_name STRING,
        status STRING,
        mode STRING,
        stichtag_from DATE,
        stichtag_to DATE,
        job_type STRING,
        active_flag STRING,
        records INT64,
        description STRING
    );
    ```
4.  **Migrate `d_drop_temp_table.sql` to BigQuery Stored Procedure:**
    - Translate the original SQL logic into a BigQuery Stored Procedure, e.g., `dataset.d_drop_temp_table`. This procedure should accept the necessary parameters and ideally return the number of dropped records.
    - **Language:** BigQuery SQL
5.  **Create `r_drop_temp_table_control` BigQuery Stored Procedure:**
    - Implement the `dataset.r_drop_temp_table_control` stored procedure using the provided pseudocode as a guideline.
    - This procedure will handle parameter validation, date derivations, calling `dataset.d_drop_temp_table`, and logging.
    - **Language:** BigQuery SQL
6.  **Develop Orchestration (Optional, if required):**
    - If scheduling or cross-system dependencies are needed, create a Cloud Composer (Airflow) DAG to call the `dataset.r_drop_temp_table_control` BigQuery Stored Procedure.
    - **Language:** Python (for Airflow DAG)
7.  **Testing:** Thoroughly test the BigQuery Stored Procedures with various parameter combinations, including edge cases and error conditions, to ensure functional parity with the original script.

This concludes the Migration Design Document.