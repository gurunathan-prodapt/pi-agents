# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

## 1. Purpose & Scope
This migration design document details the conversion of the legacy KornShell script `k_ausd_bp_ta_p_basisprod.ksh` to Google BigQuery. The original script acts as a control wrapper for an SQL script, handling parameter parsing, environment setup, date validation, and execution of the main SQL logic. Its primary purpose is to orchestrate the processing of the `PoolBasisprodukt` dataset/job, capture record counts, and prepare for job logging.

The scope of this migration includes replicating the parameter handling, date validation, SQL execution, and record count capture functionalities within the BigQuery environment. Commented-out file processing logic will be considered for re-implementation using BigQuery-native capabilities.

## 2. Source Inventory
The job is comprised of a single KornShell script:
- **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh`
- **Technology:** KornShell
- **Complexity Tier:** medium
- **Automation Bucket:** semi_auto
- **Summary:** This ksh script serves as an orchestration layer, validating input parameters and dates before executing a core SQL script (`d_ausd_bp_ta_p_basisprod.sql`) responsible for data processing. It also includes commented-out sections for file-based data manipulation (sed, sort, join) that suggest potential post-processing steps.

## 3. Target Architecture
The migrated solution will primarily reside within Google BigQuery, leveraging its native features for orchestration, data transformation, and logging.

- **Main Orchestration:** A BigQuery Stored Procedure will replace the KornShell script. This stored procedure will handle parameter validation, date validation, and orchestrate the execution of the core data transformation logic.
- **Data Transformation:** The business logic originally in `d_ausd_bp_ta_p_basisprod.sql` will be converted into BigQuery SQL, potentially as a separate BigQuery script or integrated directly into the main stored procedure using `EXECUTE IMMEDIATE`.
- **Parameter Management:** Input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) will be passed as arguments to the BigQuery Stored Procedure.
- **Date Handling:** Date validation and derivation of `today` and `yesterday` will use BigQuery's `PARSE_DATE`, `CURRENT_DATE()`, and `DATE_SUB` functions.
- **File-based Operations:** The commented-out `sed`, `sort`, and `join` operations will be re-engineered. If required, source files will be staged in Google Cloud Storage and then ingested into BigQuery tables for SQL-based transformation (`SELECT DISTINCT`, `ORDER BY`, `JOIN`).
- **Logging and Auditing:** Record counts and job status will be captured and stored in dedicated BigQuery audit tables, replacing the temporary file (`tmpFile`) and legacy job management calls.

## 4. Data Flow & Lineage
The original script's logic flow dictates the data processing:
1. **Environment Setup & Parameter Parsing:** The script starts by sourcing initialization and helper scripts and then parses command-line parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
2. **Parameter and Date Validation:** Required parameters are checked for presence, and `p_Stichtag` is validated against `DDMMYYYY` format.
3. **Date Derivation:** `gestern.ksh` is called to derive `p_datum_heute` and `p_datum_gestern`.
4. **Core SQL Execution:** The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) executes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql` with various parameters including the key date and a temporary file for record count.
5. **Record Count Capture:** The number of records processed by the SQL script is written to a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_p_basisprod.tmp`) and then read back into `v_records`.
6. **Job Logging (Intended):** There are commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` for job management.
7. **Post-Processing (Commented):** Sections for `sed`, `sort`, and `join` operations on output files are commented out, indicating potential file-based transformations if activated.

**Target BigQuery Data Flow:**
- The BigQuery Stored Procedure will receive parameters directly.
- Parameter and date validation will be performed using BigQuery scripting constructs (`IF`, `REGEXP_CONTAINS`).
- Date derivations will use `CURRENT_DATE()` and `DATE_SUB`.
- The core SQL transformation (`d_ausd_bp_ta_p_basisprod.sql`) will be executed directly within or called by the stored procedure, writing to BigQuery tables.
- Record counts will be captured from `SELECT COUNT(*)` on target tables and inserted into a BigQuery audit table.
- File-based post-processing, if required, will involve BigQuery ingestion from Cloud Storage, followed by SQL transformations.

## 5. Transformation Logic
The transformation logic will be directly translated into BigQuery SQL and stored procedure scripting.

**Parameter Handling and Validation:**
- The shell script's `getopts` logic for parsing `j, f, s, l` will be replaced by `IN` parameters to the BigQuery Stored Procedure.
- `pruefeParameterGesetzt` checks will be converted to `IF` statements and `RAISE` errors in BigQuery scripting.
- `DWDate_Datum_Check` will be handled using `REGEXP_CONTAINS` and `PARSE_DATE` to ensure `DDMMYYYY` format and convert to `DATE` type.

**SQL Script Execution:**
- The `starteSQLSkript` function's role of executing `d_ausd_bp_ta_p_basisprod.sql` will be replaced by directly executing the translated BigQuery SQL code. This may involve `EXECUTE IMMEDIATE` if the SQL is dynamic, or direct DML/DDL statements if static.
- Parameters passed to `starteSQLSkript` (e.g., `p_EintragsNr`, `p_JobKennung`, `p_Stichtag`, `p_datum_heute`, `p_datum_gestern`) will be available as variables within the BigQuery Stored Procedure or passed directly to the sub-SQL script.

**Record Count and Logging:**
- The temporary file (`tmpFile`) for record count will be replaced by selecting `COUNT(*)` from the transformed BigQuery table and storing this value in a `DECLARE` variable, which can then be inserted into a job audit table.

**Commented-out File Processing (`sed`, `sort`, `join`):**
- If these operations become active requirements, they will be translated into BigQuery SQL.
  - `sed s/\\ //g`: `REGEXP_REPLACE(column, ' ', '')`
  - `sort -u -k 1 -t ';'`: `SELECT DISTINCT ... ORDER BY SPLIT(column, ';')[SAFE_OFFSET(0)]`
  - `join -j1 1 -j2 1 -o ...`: Standard SQL `JOIN` operations, likely `FULL OUTER JOIN` or `LEFT JOIN` depending on the exact logic, using `SPLIT` functions to extract join keys and output columns.

## 6. External Dependencies
The original script has several dependencies:
- **Environment Initialization:** `. $HOME/.dw_init` will be replaced by BigQuery project/dataset configurations, constants, or potentially a configuration table.
- **Utility Scripts:**
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling will be integrated into BigQuery scripting using `RAISE` and `EXCEPTION WHEN ERROR` blocks.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date validation functionality will be replaced by BigQuery's built-in date functions (`PARSE_DATE`, `REGEXP_CONTAINS`).
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing and validation logic will be integrated into the BigQuery Stored Procedure's `DECLARE` and `IF` statements.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: The SQL*Plus wrapper behavior will be replaced by direct BigQuery SQL execution.
- **Date Helper:** `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: This logic will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` in BigQuery SQL.
- **Core SQL Script:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql`: This is the most critical dependency, containing the core business logic. Its content must be fully translated into BigQuery SQL.
- **Temporary File:** `$DW_DIR_UTL/bert_k_ausd_bp_ta_p_basisprod.tmp`: Replaced by `DECLARE` variables and BigQuery audit tables.
- **Legacy Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** These commented-out calls imply an external job scheduling and monitoring system. In BigQuery, this would be handled by Cloud Composer (Apache Airflow), Google Cloud Workflows, or Dataform, which would orchestrate the BigQuery Stored Procedure and log job status to dedicated audit tables.

No other external systems (like Oracle, SFTP, S3) were explicitly identified in the `lineage_assembled_jobs` analysis for this job.

## 7. Unresolved / Risks
The job is categorized as `semi_auto` due to its complexity and the need for careful translation of shell scripting constructs to BigQuery.

**Functionality Gaps and Alternatives:**
- **Shell Environment Initialization (`. $HOME/.dw_init`):** This is not directly replicable in pure SQL. Alternatives include passing configuration values as stored procedure parameters, using BigQuery connection properties, or defining environment-specific constants within the BigQuery project/dataset.
- **Helper Scripts (Error Handling, Date, Parameter Checks):** While the logic is replicable, the direct sourcing of these scripts is not. Their functionalities will be embedded as BigQuery scripting logic within the main stored procedure or as separate BigQuery functions/procedures.
- **`gestern.ksh`:** Replaced by native BigQuery date functions.
- **Temporary File for Record Count:** Replaced by in-memory variables and persistent audit tables in BigQuery.
- **Commented-out File Processing (`sed`, `sort`, `join`):** Although commented out, if these operations are ever activated, they represent a significant translation effort from file-based processing to BigQuery table operations. This would involve staging data in Cloud Storage and then using BigQuery DML.
- **Legacy Job Management Calls:** The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls indicate reliance on a legacy job control system. This needs to be replaced by a modern orchestrator like Cloud Composer (Airflow) or Google Cloud Workflows, along with BigQuery audit tables for job status.
- **SQL*Plus Wrapper Behavior:** Any SQL*Plus specific features in the called SQL script (`d_ausd_bp_ta_p_basisprod.sql`) will need to be re-written to be compatible with BigQuery SQL.

**Risks:**
- **Translation of `d_ausd_bp_ta_p_basisprod.sql`:** The actual business logic is in this SQL file, which was not available for detailed analysis. A thorough review and translation of this SQL script to BigQuery SQL is critical and may reveal further complexities or dependencies.
- **Dynamic SQL:** If `d_ausd_bp_ta_p_basisprod.sql` contains dynamic SQL generated by the ksh script beyond what is apparent, this will require careful handling using BigQuery's `EXECUTE IMMEDIATE`.
- **Performance Tuning:** The `medium` complexity tier suggests that the BigQuery solution will require performance tuning to optimize query execution and resource utilization.

## 8. Build Plan
The migration will involve the following steps and generated artifacts:

1.  **BigQuery Stored Procedure (BQSQL):**
    *   Create a main BigQuery Stored Procedure (e.g., `project.dataset.sp_k_ausd_bp_ta_p_basisprod`) that encapsulates the parameter parsing, validation, date derivation, and orchestration logic of the original KornShell script.
    *   This procedure will accept `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as input parameters.
    *   It will include `DECLARE`, `SET`, `IF`, `BEGIN...END`, and `RAISE` statements for control flow and error handling.

2.  **Core Data Transformation Script (BQSQL):**
    *   Translate the content of `d_ausd_bp_ta_p_basisprod.sql` into a standalone BigQuery SQL script or integrate it as an `EXECUTE IMMEDIATE` block within the main stored procedure.
    *   This script will contain the DML operations (INSERT, UPDATE, MERGE) to populate the target tables.

3.  **BigQuery DDL for Target Tables (BQSQL):**
    *   Define the schema for all target tables that `d_ausd_bp_ta_p_basisprod.sql` writes to.
    *   Define schemas for any staging tables required for the file-based post-processing logic (if activated).

4.  **BigQuery DDL for Audit/Log Tables (BQSQL):**
    *   Create a `job_audit_table` to log job execution details, including parameters, start/end times, status, and the `v_records` count.

5.  **Orchestration Configuration (YAML/Python):**
    *   If using Cloud Composer (Airflow), create a Python DAG definition that calls the BigQuery Stored Procedure, handles scheduling, and monitors execution.
    *   If using Google Cloud Workflows, define a YAML workflow that orchestrates the BigQuery job.
    *   If using Dataform, define a Dataform SQLX file or stored procedure call.

6.  **Optional: Cloud Storage Staging (Configuration):**
    *   If the commented-out file processing logic (sed, sort, join) is enabled, configure Cloud Storage buckets and ingestion jobs for the `cibasis_data*.dat` files.

**Build Language:** Primarily BigQuery SQL for stored procedures and data transformations, with Python/YAML for orchestration layer if using Cloud Composer/Workflows.