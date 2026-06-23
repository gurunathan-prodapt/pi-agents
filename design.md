# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh

## 1. Purpose & Scope

This job, `k_ausd_bp_ta_bpr_opt_text.ksh`, is a KornShell control script designed to orchestrate the preparation and export of data related to 'PoolBasisprodukt'. Its primary function is to parse command-line parameters, perform date validation, execute a core Oracle SQL script (`d_ausd_bp_ta_bpr_opt_text.sql`), and record the number of processed records. The overall purpose of the assembled job is to process and manage "Basisprodukt" related text data within the `ISBERT` reporting schema.

The scope of this migration involves converting the KornShell script into a BigQuery-compatible orchestration mechanism (likely a stored procedure or an Airflow DAG) and translating the embedded Oracle SQL logic into BigQuery SQL.

## 2. Source Inventory

This job consists of two main components:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`**
    *   **Technology:** KornShell
    *   **Complexity Tier:** (Information not available - `file_complexity` returned no rows)
    *   **Automation Bucket:** `semi_auto`
    *   **Purpose:** Control script for parameter handling, date validation, and SQL script invocation.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_opt_text.sql`**
    *   **Technology:** Oracle SQL
    *   **Complexity Tier:** (Information not available - `file_complexity` returned no rows)
    *   **Automation Bucket:** `semi_auto`
    *   **Purpose:** Core data transformation logic, truncating and inserting data into a basis product options text table.

## 3. Target Architecture

The migrated solution will primarily reside within Google Cloud's BigQuery.

*   **Orchestration:** The KornShell script's logic will be converted into a BigQuery Stored Procedure, acting as the main entry point for the job. Alternatively, if complex external orchestrations are needed, a Cloud Composer (Airflow) DAG could be used.
*   **Data Transformation:** The Oracle SQL script's logic will be directly translated into BigQuery SQL statements, embedded within the main BigQuery Stored Procedure or as a separate BigQuery Script, called from the orchestrating procedure.
*   **Tables:**
    *   Source tables `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_optionen`, and `sof$ta_bpr_beschr` will be migrated to BigQuery tables (e.g., `isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, `isbert_dataset.sof_ta_bpr_beschr`).
    *   Target table `sof$ta_bpr_opt_text` will be migrated to a BigQuery table (e.g., `isbert_dataset.sof_ta_bpr_opt_text`).
    *   Control/logging tables (e.g., `project.dataset.job_error_log`, `project.dataset.job_run_control`) will be created in BigQuery to manage job state and error reporting, replacing temporary files and implicit job-table updates.

## 4. Data Flow & Lineage

The original data flow is:
1.  **`r_ausd_bp_ta_bpr_opt_text.ksh` (invoker script, external to this job)**: Invokes `k_ausd_bp_ta_bpr_opt_text.ksh`.
2.  **`k_ausd_bp_ta_bpr_opt_text.ksh` (KornShell)**:
    *   Parses command-line arguments (Job ID, Entry Number, Stichtag, Restart Value).
    *   Sources utility scripts for environment setup, error handling, date functions, parameter parsing, and SQL*Plus interaction.
    *   Calls `gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`.
    *   Executes `d_ausd_bp_ta_bpr_opt_text.sql` with collected parameters and environment variables.
    *   Reads a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_opt_text.tmp`) to get the record count.
    *   (Commented out) Updates a "Job-Tabelle" via `FOSJobErzeugeEintrag`.
3.  **`d_ausd_bp_ta_bpr_opt_text.sql` (Oracle SQL)**:
    *   Connects to Oracle, potentially using a DB Link `@pcrs1`.
    *   Dynamically determines a `v_datum` from `isbert_schema.dwtk_meldungen`.
    *   Truncates the `sof$ta_bpr_opt_text` table using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   Inserts into `sof$ta_bpr_opt_text` by joining `sof$ta_bpr_optionen` and `sof$ta_bpr_beschr` on `bpr_id`.
    *   Commits the changes.

The migrated data flow in BigQuery will be:
1.  **BigQuery Orchestration (Stored Procedure or Airflow DAG)**: Acts as the entry point, receiving parameters.
2.  **BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_opt_text`**:
    *   Validates input parameters (Job ID, Entry Number, Stichtag).
    *   Derives `v_datum_heute` and `v_datum_gestern` using BigQuery `CURRENT_DATE()` and `DATE_SUB()`.
    *   Executes the migrated SQL logic (formerly `d_ausd_bp_ta_bpr_opt_text.sql`) which includes:
        *   Retrieving `v_datum` from `isbert_dataset.dwtk_meldungen`.
        *   `TRUNCATE TABLE isbert_dataset.sof_ta_bpr_opt_text;`
        *   `INSERT INTO isbert_dataset.sof_ta_bpr_opt_text (...) SELECT ... FROM isbert_dataset.sof_ta_bpr_optionen JOIN isbert_dataset.sof_ta_bpr_beschr ...`
    *   Captures the number of inserted records directly (e.g., using `ROW_COUNT()`).
    *   Logs job run information and record counts into BigQuery control tables.

## 5. Transformation Logic

### `k_ausd_bp_ta_bpr_opt_text.ksh` (KornShell) Migration

*   **Parameter Handling:** The `getopts` logic will be replaced by BigQuery Stored Procedure parameters. Parameter validation (e.g., `pruefeParameterGesetzt`, `DWDate_Datum_Check`) will be implemented using procedural `IF` statements and `RAISE` for errors in BigQuery SQL scripting.
*   **Environment Sourcing:** The sourced `.ksh` utility scripts will be replaced by direct BigQuery functions, or if custom logic, will be reimplemented as BigQuery UDFs or part of the stored procedure itself.
    *   `f_alis_msgerr.ksh` (Error Handling): Replaced by `RAISE` and logging to a BigQuery error table.
    *   `h_alis_date.ksh` (Date Check): Replaced by `REGEXP_CONTAINS` and `PARSE_DATE` in BigQuery SQL.
    *   `h_alis_parameter.ksh` (Parameter Parsing): Replaced by Stored Procedure parameter declarations and `IF` checks.
    *   `h_alis_sqlplus.ksh` (SQL*Plus Routines): Replaced by the direct execution of BigQuery SQL.
*   **Date Derivation:** `gestern.ksh` will be replaced by BigQuery date functions such as `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Script Execution:** The `starteSQLSkript` call will be replaced by the direct execution of the migrated BigQuery SQL from the `d_ausd_bp_ta_bpr_opt_text.sql` logic within the same stored procedure.
*   **Record Counting:** The temporary file `tmpFile` will be eliminated. The record count (`v_records`) will be obtained directly from the `INSERT` statement's results or by a subsequent `SELECT COUNT(*)` on the target table in BigQuery, then stored in a BigQuery control table.
*   **Job Management:** The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls indicate an existing job management system. If active, these should be replaced with DML operations on BigQuery control/audit tables.

### `d_ausd_bp_ta_bpr_opt_text.sql` (Oracle SQL) Migration

*   **Variable Definitions:** Oracle `DEFINE` and `COLUMN ... NEW_VALUE` for `v_carmen` and `v_datum` will be converted to BigQuery `DECLARE` and `SET` statements.
    *   `v_datum` will be populated using `COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')` from `isbert_dataset.dwtk_meldungen`.
*   **SQL*Plus Directives:** `prompt`, `start`, `spool`, `WHENEVER SQLERROR`, `set timing on`, `exit success` are SQL*Plus client commands and will be removed or replaced by BigQuery scripting error handling (`BEGIN...EXCEPTION`) and logging mechanisms.
*   **Table Truncation:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_opt_text REUSE STORAGE')` will be replaced by a direct `TRUNCATE TABLE isbert_dataset.sof_ta_bpr_opt_text;` statement in BigQuery.
*   **Data Insertion:** The `INSERT INTO ... SELECT` statement will be directly translated:
    *   Oracle-specific hints like `/*+ full(bp) parallel(bp,4) full(bs) parallel(bs,4) */` will be removed as BigQuery's query optimizer handles parallelism automatically.
    *   The join condition `bp.bpr_id = bs.bpr_id` remains the same.
    *   `ORDER BY` clause (if uncommented) should be re-evaluated for necessity in BigQuery, as order is not guaranteed without `ORDER BY` in the final `SELECT` or for table insertion.

## 6. External Dependencies

*   **Oracle Database Link (`@pcrs1`):** The `DEFINE v_carmen = "@pcrs1"` indicates a database link usage. This will be replaced by direct access to the migrated source tables in BigQuery (e.g., `isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, `isbert_dataset.sof_ta_bpr_beschr`). If `pcrs1` refers to another Oracle database, data from that system will need to be ingested into BigQuery using appropriate methods (e.g., Cloud Data Fusion, Dataflow, or batch loading).
*   **KornShell Utility Scripts:**
    *   `$HOME/.dw_init`: Environment initialization will be handled by BigQuery's execution environment or explicit variable declarations within the stored procedure.
    *   `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`: These will be replaced by native BigQuery SQL scripting constructs, functions, and error handling.
*   **`gestern.ksh`:** This script for deriving yesterday's and today's dates will be replaced by BigQuery's native `CURRENT_DATE()` and `DATE_SUB()` functions.
*   **`isbert_schema.DWPA_UTIL_SKRIPT` (Oracle Package):** The call to `runstatement` for truncating a table will be replaced by a native BigQuery `TRUNCATE TABLE` statement. If other functionalities of this package are implicitly used, they will need to be identified and migrated to BigQuery equivalents (UDFs, stored procedures).
*   **Temporary File (`tmpFile`):** The use of a temporary file for record counts will be eliminated, replaced by BigQuery variables and control tables.
*   **Commented out `sed`, `sort`, `join` operations:** If these operations were to become active, they would be replaced by BigQuery SQL transformations (`REPLACE`, `SELECT DISTINCT`, `JOIN`) on BigQuery tables.

## 7. Unresolved / Risks

*   **Missing Complexity Information:** The `file_complexity` table returned no rows for both files. This means the migration effort estimation and potential flags for challenges are unknown, which could lead to underestimation of effort.
*   **`gestern.ksh` content:** The exact logic of `gestern.ksh` is not explicitly known, but assumed to be straightforward date calculation. If it involves complex calendar logic, this needs to be clarified and implemented correctly in BigQuery.
*   **Job-Tabelle (`FOSJobErzeugeEintrag`):** The logic for updating a "Job-Tabelle" is commented out. If this is required functionality, its schema and purpose need to be fully understood to create an equivalent in BigQuery.
*   **`trace.sql.cfg` and spooling:** The content of `trace.sql.cfg` and the spooling to `./tmp/trace_d_ausd_bpr_opt_text.trc` implies logging or debugging practices. An equivalent BigQuery logging mechanism (e.g., to Cloud Logging or a dedicated BigQuery log table) should be implemented.
*   **Assumptions on `BERT_DIR_ROOT`, `DW_DIR_UTL`, `HOME`:** These environment variables will need to be configured as parameters or constants within the BigQuery stored procedure or the orchestration layer.

## 8. Build Plan

1.  **Data Migration:**
    *   Migrate `isbert_schema.dwtk_meldungen` to `isbert_dataset.dwtk_meldungen` in BigQuery.
    *   Migrate `sof$ta_bpr_optionen` to `isbert_dataset.sof_ta_bpr_optionen` in BigQuery.
    *   Migrate `sof$ta_bpr_beschr` to `isbert_dataset.sof_ta_bpr_beschr` in BigQuery.
    *   Create the target table `isbert_dataset.sof_ta_bpr_opt_text` in BigQuery.
    *   Create BigQuery control/logging tables (e.g., `job_error_log`, `job_run_control`).

2.  **SQL Script Conversion (`d_ausd_bp_ta_bpr_opt_text.sql`):**
    *   Translate the Oracle SQL `INSERT ... SELECT` statement into BigQuery SQL, adapting data types and functions.
    *   Replace `TRUNCATE` logic with BigQuery's native `TRUNCATE TABLE` statement.
    *   Implement `v_datum` derivation using BigQuery date functions.
    *   This converted SQL will form the core logic within the BigQuery Stored Procedure.

3.  **KornShell Script Conversion (`k_ausd_bp_ta_bpr_opt_text.ksh`):**
    *   Create a BigQuery Stored Procedure (e.g., `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`) to encapsulate the orchestration logic.
    *   Define parameters for Job ID, Entry Number, Stichtag, and Restart Value.
    *   Implement parameter validation and error handling using BigQuery SQL scripting.
    *   Integrate the converted BigQuery SQL from step 2.
    *   Replace date derivation (`gestern.ksh`) with BigQuery functions.
    *   Implement record count capture and logging to control tables.

4.  **Orchestration (Optional, if external chaining is needed):**
    *   If `r_ausd_bp_ta_bpr_opt_text.ksh` represents a broader workflow, consider implementing an Airflow DAG in Cloud Composer to call the BigQuery Stored Procedure, passing required parameters.

5.  **Testing:**
    *   Unit test the BigQuery SQL transformation logic.
    *   Integration test the BigQuery Stored Procedure with sample data.
    *   Verify data accuracy and completeness after migration.

6.  **Documentation:**
    *   Document the BigQuery schema for new/migrated tables.
    *   Document the BigQuery Stored Procedure and its parameters.
    *   Update relevant runbooks and operational procedures.

**Target Language for Build:** BigQuery SQL (for stored procedure and core logic).