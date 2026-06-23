# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

## 1. Purpose & Scope

This job (`k_ausd_bp_ta_bpr_basis_his.ksh`) acts as a control script for the data processing of the core product table `PoolBasisprodukt`. Its primary function is to orchestrate the execution of a main SQL script, handling parameter validation, date checks, and environment setup. It is a KornShell script designed to manage the workflow, not to perform complex data transformations itself, though it includes commented-out sections for file post-processing. The script ensures that the necessary parameters are provided and correctly formatted before invoking the data processing logic encapsulated within an external SQL script.

## 2. Source Inventory

The migration job consists of a single primary source file, which is a KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh`
    *   **Technology:** KornShell
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Summary:** This is a control script that parses parameters, validates dates, sources utility scripts, and orchestrates the execution of a main SQL script to process data for the 'PoolBasisprodukt' table.
    *   **Complexity Tier:** medium
    *   **Migration Flags:** `[]` (None identified)
    *   **Automation Bucket:** semi_auto

## 3. Target Architecture

The target architecture in BigQuery will involve:

*   **BigQuery Stored Procedure:** The core orchestration logic of the KornShell script, including parameter parsing, validation, and invocation of the main data processing logic, will be migrated into a BigQuery Stored Procedure. This procedure will handle the flow control and error management.
*   **BigQuery SQL:** The logic from the invoked SQL script (`d_ausd_bp_ta_bpr_basis_his.sql`) will be translated into standard BigQuery SQL statements, likely residing directly within the stored procedure or as separate, callable SQL scripts if modularity is preferred.
*   **BigQuery Tables:**
    *   **Target Table:** The `PoolBasisprodukt` table will be a BigQuery table, where the processed data will be loaded.
    *   **Source Tables:** Any source tables referenced by `d_ausd_bp_ta_bpr_basis_his.sql` will be migrated to BigQuery.
    *   **Error Log Table:** A dedicated BigQuery table (`project.dataset.error_log`) will capture any errors encountered during the execution, replacing the shell script's `DWMSG_MeldeFehler` mechanism.
    *   **Job Audit Table:** A BigQuery table (`project.dataset.job_audit`) will record job execution details, replacing the commented `FOSJobErzeugeEintrag` functionality.
    *   **Staging Tables (Optional):** If the commented-out post-processing logic for file manipulation becomes active, staging tables might be used to handle intermediate data before final joining and export.
*   **BigQuery UDFs (Optional):** Custom User-Defined Functions could be implemented for reusable validation logic (e.g., date format checks), although these can often be integrated directly into the stored procedure.
*   **Cloud Storage (Optional):** If the commented post-processing logic which exports data to CSV files becomes active, Cloud Storage would be the target for these exported files.
*   **Cloud Composer / Workflows / Cloud Run (Optional):** For complex external orchestration or integration with other systems, these GCP services could serve as an alternative if the BigQuery stored procedure alone is insufficient.

## 4. Data Flow & Lineage

The current data flow starts with the `k_ausd_bp_ta_bpr_basis_his.ksh` script:

1.  **Parameter Input:** The script receives command-line parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
2.  **Environment Setup & Utilities:** It sources various utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  **Date Derivation:** It executes `gestern.ksh` to get today's and yesterday's dates.
4.  **Parameter Validation:** It validates the presence and format of the input parameters, particularly `p_Stichtag`.
5.  **SQL Script Execution:** The script then invokes a function `starteSQLSkript` which in turn executes `d_ausd_bp_ta_bpr_basis_his.sql`. This SQL script is responsible for the actual data processing and loading into the `PoolBasisprodukt` table.
6.  **Record Count & Logging:** After SQL execution, it reads a record count from a temporary file (`tmpFile`) and could, if uncommented, make an entry in a job-tracking table.
7.  **Post-Processing (Commented):** There's commented-out logic for manipulating temporary output files using `sed`, `sort`, and `join` to produce `cibasisprodukt.csv`.

**Target BigQuery Data Flow:**

1.  **BigQuery Stored Procedure Call:** The migration will begin with an external scheduler (e.g., Cloud Composer) or direct invocation of the `proc_ausd_bp_ta_bpr_basis_his` stored procedure in BigQuery, passing the required parameters.
2.  **Parameter & Date Handling:** The stored procedure will internally handle parameter validation, error logging (to `error_log` table), and date derivations using BigQuery SQL functions.
3.  **Data Processing (BigQuery SQL):** The core data processing logic from `d_ausd_bp_ta_bpr_basis_his.sql` will be implemented as SQL statements within the stored procedure. This will read from BigQuery source tables and write to the BigQuery `PoolBasisprodukt` table.
4.  **Record Count & Audit:** The stored procedure will calculate the number of processed records using `COUNT(*)` and insert an entry into the `job_audit` table.
5.  **Optional File Export:** If the legacy post-processing is required, separate BigQuery procedures could process staging tables and use `EXPORT DATA` to output CSV files to Cloud Storage.

## 5. Transformation Logic

The KornShell script itself is primarily an orchestration layer. The "transformation logic" resides mainly in the invoked `d_ausd_bp_ta_bpr_basis_his.sql` script, which is not directly available in this analysis. However, the wrapper script's logic needs to be transformed:

*   **Parameter Parsing (`getopts`):**
    *   **Legacy:** `while getopts ":h$ParamList" param`
    *   **Target:** BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING, IN p_EintragsNr STRING, IN p_Stichtag STRING, IN p_wiederanlaufWert INT64`). Default values and null checks handle missing parameters.
*   **Parameter Validation (`pruefeParameterGesetzt`):**
    *   **Legacy:** Shell functions checking if variables are set.
    *   **Target:** `IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN` and similar checks within the BigQuery Stored Procedure.
*   **Date Validation (`DWDate_Datum_Check`):**
    *   **Legacy:** Shell function for date format check.
    *   **Target:** `REGEXP_CONTAINS(p_Stichtag, r'^[0-9]{8}$` and `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` within BigQuery.
*   **Error Handling (`DWMSG_MeldeFehler`, `exit`):**
    *   **Legacy:** Echoing messages and exiting with specific error codes.
    *   **Target:** `INSERT INTO project.dataset.error_log ...` followed by `RAISE USING MESSAGE = v_err;` for structured error logging and termination in BigQuery.
*   **Date Derivation (`gestern.ksh`):**
    *   **Legacy:** External shell script.
    *   **Target:** BigQuery SQL functions `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Script Execution (`starteSQLSkript`):**
    *   **Legacy:** A shell function that executes the external SQL script `d_ausd_bp_ta_bpr_basis_his.sql`.
    *   **Target:** The content of `d_ausd_bp_ta_bpr_basis_his.sql` will be embedded directly as DML/DDL statements within the BigQuery Stored Procedure, or executed via `EXECUTE IMMEDIATE` if dynamic SQL is required.
*   **Record Count (`cat $tmpFile`):**
    *   **Legacy:** Reading from a temporary file.
    *   **Target:** `SELECT COUNT(*) FROM project.dataset.target_table WHERE ...` directly after the data load.
*   **Job Logging (`FOSJobErzeugeEintrag`):**
    *   **Legacy:** Commented-out shell function call.
    *   **Target:** `INSERT INTO project.dataset.job_audit ...` within the BigQuery Stored Procedure.
*   **File Post-processing (`sed`, `sort`, `join`):**
    *   **Legacy:** Commented-out shell commands for manipulating CSV files.
    *   **Target:** If activated, these would be translated into BigQuery SQL DML operations on staging tables, followed by `EXPORT DATA` to Cloud Storage for CSV output.

## 6. External Dependencies

The original KornShell script has several dependencies:

*   **Environment Files & Utility Scripts:**
    *   `$HOME/.dw_init` (environment initialization)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus helper)
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (date derivation for yesterday/today)
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_basis_his.sql` (main SQL logic)
*   **Temporary Files:**
    *   `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis_his.tmp` (for record count)
    *   Commented: `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`, `cibasis_24_96.tmp`, `cibasisprodukt.csv` (for post-processing)

**Replacement in BigQuery:**

*   **Environment & Utility Scripts:** These will be absorbed into the BigQuery Stored Procedure logic using native BigQuery functions and SQL constructs.
    *   Environment variables will be replaced by parameters or configuration tables.
    *   Error logging will use a dedicated BigQuery `error_log` table.
    *   Date utilities and parameter validation logic will be directly coded in SQL.
    *   The SQL*Plus helper becomes obsolete as the SQL logic will be directly executed within BigQuery.
*   **Main SQL Logic (`d_ausd_bp_ta_bpr_basis_his.sql`):** The content of this SQL script will be the primary data transformation logic within the BigQuery Stored Procedure.
*   **Temporary Files:**
    *   The record count will be obtained directly via `COUNT(*)` on the target table.
    *   If the commented post-processing for flat files is needed, BigQuery staging tables will replace these temporary files. Output to CSV would be handled by `EXPORT DATA` to Cloud Storage.

There are no identified external systems like Oracle, SFTP, or S3 that this specific job directly interacts with, based on the provided `external_systems` and `lineage_edges` analysis.

## 7. Unresolved / Risks

*   **Unresolved SQL Script Details:** The core data transformation logic resides in `d_ausd_bp_ta_bpr_basis_his.sql`, which was not available for detailed analysis. The success of the migration heavily depends on the complexity and content of this SQL script. It is assumed to be migratable to BigQuery SQL. If it contains complex procedural logic or non-standard SQL, it might require further decomposition or the use of BigQuery Scripting or external tools (e.g., Cloud Dataflow for more complex transformations).
*   **Commented-out Code:** The script contains significant commented-out sections, particularly for FOS Job management (`h_alis_job.ksh`, `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) and file-based post-processing (`sed`, `sort`, `join`). The migration design assumes these are currently inactive. If they need to be re-activated, they represent additional scope:
    *   FOS Job management would need to be replaced with a BigQuery audit/metadata table and potentially a scheduling mechanism (e.g., Cloud Composer).
    *   File post-processing would require BigQuery staging tables and `EXPORT DATA` to Cloud Storage.
*   **Parameter `p_wiederanlaufWert`:** The script initializes `p_wiederanlaufWert` if empty. The exact use of this parameter within `d_ausd_bp_ta_bpr_basis_his.sql` or other parts of the system is unknown. Its migration to BigQuery should consider its role in restartability or incremental loads.
*   **Complexity Tier:** The `medium` complexity tier for the shell script indicates that while not trivial, it should be manageable with a semi-automated approach using BigQuery stored procedures. However, the complexity of the *invoked SQL script* is the major unknown and could significantly impact the overall migration effort.

## 8. Build Plan

The build plan focuses on transforming the orchestration logic and integrating the underlying SQL.

1.  **Translate `d_ausd_bp_ta_bpr_basis_his.sql` to BigQuery SQL:**
    *   **Language:** BigQuery SQL
    *   **Output:** `d_ausd_bp_ta_bpr_basis_his.bq.sql` (or directly embedded into the stored procedure). This will be the core data processing logic.
    *   **Details:** Identify source tables, target tables, and all DML/DDL statements. Ensure compatibility with BigQuery syntax and best practices.

2.  **Create BigQuery Error Log Table:**
    *   **Language:** BigQuery DDL
    *   **Output:** `error_log.ddl.sql`
    *   **Details:** Define schema for error messages, job names, timestamps, etc.

3.  **Create BigQuery Job Audit Table:**
    *   **Language:** BigQuery DDL
    *   **Output:** `job_audit.ddl.sql`
    *   **Details:** Define schema for logging job execution details, including parameters, record counts, and status.

4.  **Develop BigQuery Stored Procedure (`proc_ausd_bp_ta_bpr_basis_his`):**
    *   **Language:** BigQuery SQL (Stored Procedure)
    *   **Output:** `k_ausd_bp_ta_bpr_basis_his.bq.sql` (containing the stored procedure definition).
    *   **Details:**
        *   Define input parameters matching the shell script's `getopts`.
        *   Implement parameter validation logic (presence, format) using BigQuery `IF` statements and `REGEXP_CONTAINS`, `SAFE.PARSE_DATE`.
        *   Integrate error logging by `INSERT`ing into the `error_log` table and using `RAISE` for fatal errors.
        *   Replace shell-based date derivation with `CURRENT_DATE()` and `DATE_SUB`.
        *   Embed or call the translated `d_ausd_bp_ta_bpr_basis_his.bq.sql` for the main data processing.
        *   Calculate and log record counts to the `job_audit` table.
        *   Handle the `p_wiederanlaufWert` parameter appropriately.

5.  **Develop Optional BigQuery Post-Processing Stored Procedure (if needed):**
    *   **Language:** BigQuery SQL (Stored Procedure)
    *   **Output:** `postprocessing_optional.bq.sql`
    *   **Details:** If the commented `sed`/`sort`/`join` logic is required, create a separate stored procedure to:
        *   Load data into temporary staging tables.
        *   Apply deduplication (`DISTINCT`).
        *   Perform `JOIN` operations.
        *   Use `EXPORT DATA` to generate CSV files to Cloud Storage if file output is needed.

6.  **Orchestration Integration:**
    *   **Language:** Python (for Cloud Composer DAG) or YAML (for Cloud Workflows)
    *   **Output:** `k_ausd_bp_ta_bpr_basis_his_dag.py` or `k_ausd_bp_ta_bpr_basis_his.workflow.yaml`
    *   **Details:** Create a scheduler to invoke the main BigQuery Stored Procedure, passing required parameters.

This comprehensive plan addresses the migration of the shell script's orchestration logic and prepares for the integration of the underlying SQL data processing into the BigQuery environment.