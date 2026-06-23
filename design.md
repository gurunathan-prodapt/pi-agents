# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_bp_ta_msisdn.ksh`, serves as an orchestration and control script for a data preparation job. Its primary purpose is to:
- Parse and validate input parameters, including a job identifier, entry number, and key date (`Stichtag`).
- Initialize the environment by sourcing various helper scripts for error handling, date validation, parameter parsing, and SQL*Plus execution.
- Dynamically determine yesterday's and today's dates.
- Execute a core SQL script (`d_ausd_bp_ta_msisdn.sql`) with the collected parameters.
- Capture the number of records processed by the SQL script from a temporary file.
- (Intended) Log job execution details to a job table.
- There are commented-out sections indicating potential post-processing steps involving `sed`, `sort`, and `join` operations on temporary data files, which suggest further data manipulation.

The scope of this migration is to re-implement this orchestration logic and the underlying data transformations on the Google Cloud Platform, specifically utilizing BigQuery for data processing and potentially Cloud Composer for orchestration.

## 2. Source Inventory
The job consists of a single primary source file:
- **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh`**
  - **Technology**: KornShell Script
  - **Complexity Tier**: Medium
  - **Automation Bucket**: Semi-Auto (B2)
  - **Purpose**: ETL orchestration, parameter validation, SQL script execution.
  - **Migration Flags**: None identified by analysis.

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform services:
- **Orchestration**: Cloud Composer (Apache Airflow) will be used to manage the execution flow, parameter passing, and job logging.
- **Data Processing**: BigQuery will be the primary data warehouse and processing engine.
  - The core SQL logic from `d_ausd_bp_ta_msisdn.sql` will be refactored into a BigQuery Stored Procedure or a series of SQL queries executed within BigQuery.
  - Temporary file operations will be replaced by BigQuery temporary tables or direct table insertions.
- **Logging & Monitoring**: BigQuery tables will be used for audit logging, error logging, and tracking processed record counts. Cloud Logging and Cloud Monitoring will provide system-level observability.
- **Parameter Management**: Airflow DAG parameters will replace shell script `getopts` for job invocation.

### BigQuery Components:
- **Stored Procedure**: `project.dataset.r_ausd_bp_ta_msisdn` (replaces the main ksh script logic).
- **Stored Procedure**: `project.dataset.d_ausd_bp_ta_msisdn` (replaces the embedded SQL script).
- **Table**: `project.dataset.job_error_log` (for error logging).
- **Table**: `project.dataset.job_audit_log` (for job status and record counts).
- **Table(s)**: Target tables for the output of `d_ausd_bp_ta_msisdn.sql` (e.g., `project.dataset.PoolBasisprodukt` or similar).
- **Temporary Tables**: Used for intermediate results during transformations, replacing filesystem temporary files.

## 4. Data Flow & Lineage
The original script's data flow is primarily sequential orchestration:
1. **Initialization**: Sourcing various `.ksh` helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2. **Parameter Input**: Reads `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` via `getopts`.
3. **Date Derivation**: Executes `gestern.ksh` to get `p_datum_heute` and `p_datum_gestern`.
4. **Validation**: Validates required parameters and `p_Stichtag` format. Exits on error.
5. **SQL Execution**: Calls `starteSQLSkript` which is a wrapper around SQL*Plus-like tool to execute `d_ausd_bp_ta_msisdn.sql`. This SQL script is the main data transformation component.
6. **Record Count**: Reads the record count from `$DW_DIR_UTL/bert_k_ausd_bp_ta_msisdn.tmp`.
7. **Logging**: (Commented out) `FOSJobErzeugeEintrag` for job table logging.
8. **Post-Processing**: (Commented out) `sed`, `sort`, `join` operations on intermediate flat files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) to produce `cibasisprodukt.csv`.

### Migrated Data Flow (BigQuery & Airflow):
1. **Airflow DAG Trigger**: An Airflow DAG will be triggered, receiving job parameters as DAG run configurations.
2. **Parameter Validation**: The main BigQuery stored procedure `project.dataset.r_ausd_bp_ta_msisdn` will handle parameter parsing and validation using `IF` and `RAISE` statements.
3. **Date Derivation**: `CURRENT_DATE()` and `DATE_SUB()` BigQuery functions will replace `gestern.ksh`.
4. **Core Transformation**: The `project.dataset.d_ausd_bp_ta_msisdn` BigQuery stored procedure (or a set of BQ SQL queries) will be invoked. This SP will perform the main data transformations, reading from source tables and writing to target tables.
5. **Record Count & Logging**: The `project.dataset.r_ausd_bp_ta_msisdn` stored procedure will capture record counts from the target tables and insert them into `project.dataset.job_audit_log`. Errors will be logged to `project.dataset.job_error_log`.
6. **(Optional) Post-Processing**: If the commented `sed/sort/join` logic is required, it will be translated into a separate BigQuery stored procedure (`project.dataset.postprocess_cibasis`) that operates directly on BigQuery tables, performing `REPLACE`, `DISTINCT`, `JOIN`, and `SELECT` operations. This would be called as a separate task in the Airflow DAG.

## 5. Transformation Logic
### `k_ausd_bp_ta_msisdn.ksh` (Orchestration Script) to BigQuery Stored Procedure:
- **Parameter Handling**:
  - Shell `getopts` will be replaced by BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING, IN p_EintragsNr STRING, IN p_Stichtag STRING, IN p_wiederanlaufWert STRING`).
- **Date Derivation**:
  - `set \`gestern.ksh\`` will be replaced by BigQuery functions: `DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE(); DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`.
- **Parameter Validation**:
  - Shell `pruefeParameterGesetzt` and `if [ ! $ErrNr -eq 0 ]` logic will be replaced by BigQuery `IF` statements and `RAISE USING MESSAGE` for error handling. Error logging will go to `project.dataset.job_error_log`.
- **Date Format Check**:
  - `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will be replaced by `PARSE_DATE('%d%m%Y', p_Stichtag)` within the BigQuery Stored Procedure, with error handling for invalid formats.
- **SQL Script Execution**:
  - `starteSQLSkript ... d_ausd_bp_ta_msisdn.sql ...` will be replaced by a `CALL` to the `project.dataset.d_ausd_bp_ta_msisdn` BigQuery Stored Procedure or an `EXECUTE IMMEDIATE` statement if the SQL is dynamic.
- **Record Count**:
  - `eval "v_records=\`cat $tmpFile\`"` will be replaced by `SELECT COUNT(*) FROM target_output_table` and storing the result in a `DECLARE v_records INT64;` variable.
- **Job Logging**:
  - The commented `FOSJobErzeugeEintrag` will be re-implemented as an `INSERT` statement into the `project.dataset.job_audit_log` table.

### Commented Post-Processing Logic (`sed`, `sort`, `join`) to BigQuery Stored Procedure:
If these commented steps are later deemed necessary, they will be translated into BigQuery SQL within a dedicated stored procedure (`project.dataset.postprocess_cibasis`).
- `sed s/\\ //g`: `REPLACE(column_name, ' ', '')`
- `sort -u -k 1 -t ';'`: `SELECT DISTINCT ... ORDER BY column1`
- `join`: BigQuery `JOIN` operations (e.g., `FULL OUTER JOIN`, `LEFT JOIN`).

## 6. External Dependencies
- **`$HOME/.dw_init`**: Environment initialization. This will be replaced by Airflow environment variables or configuration managed within the Airflow DAG.
- **Helper Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**: Their functionalities will be absorbed into the BigQuery Stored Procedures using native BigQuery SQL constructs and error handling.
- **`gestern.ksh`**: Replaced by BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`).
- **SQL Script (`d_ausd_bp_ta_msisdn.sql`)**: This is a critical dependency that contains the core business logic. It needs to be migrated to a BigQuery Stored Procedure. The content of this SQL script was not available in the analysis, so its migration is a prerequisite.
- **Temporary Files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_msisdn.tmp`, `cibasis_data*.dat`)**: These will be replaced by BigQuery temporary tables or direct writes to persistent BigQuery tables.
- **External Systems**: No explicit external systems (like Oracle, SFTP, S3) were identified in the `lineage_assembled_jobs` analysis for this job. However, if `d_ausd_bp_ta_msisdn.sql` interacts with external databases, those would need separate migration strategies (e.g., Federated Queries, Data Transfer Service, Cloud SQL).

## 7. Unresolved / Risks
- **`d_ausd_bp_ta_msisdn.sql` content**: The content of this crucial SQL script was not provided. Its complexity and specific SQL dialect are unknown. This represents the primary unknown and risk for the migration, as its translation to BigQuery SQL will dictate a significant portion of the effort. It is assumed that this SQL is compatible with BigQuery or can be easily translated.
- **Commented-out code**: The `sed`, `sort`, `join` operations are commented out. A decision needs to be made whether this logic is obsolete or if it represents an unexecuted but desired transformation that should be migrated. If it is needed, it will add to the complexity.
- **Error Handling Details**: The specific error codes and messages from `f_alis_msgerr.ksh` would need to be mapped to an appropriate BigQuery error logging and reporting mechanism.
- **`DWMSG_MeldeFehler` and `FOSJobErzeugeEintrag`**: The exact implementation and integration points of these functions are not fully known without the source code of the helper scripts. The migration assumes a generic BigQuery audit and error logging table will suffice.
- **Locale/Encoding Issues**: The script contains German comments (`Lübbers`, `temporären`, `können`). Any data processing steps involving string manipulation should consider character encoding.

## 8. Build Plan
1. **Analyze `d_ausd_bp_ta_msisdn.sql`**: Obtain and analyze the content of `d_ausd_bp_ta_msisdn.sql`. (Manual Step)
2. **Design `d_ausd_bp_ta_msisdn` BigQuery Stored Procedure**: Translate `d_ausd_bp_ta_msisdn.sql` to a BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_msisdn.sql`).
   - **Language**: BigQuery SQL
3. **Generate `r_ausd_bp_ta_msisdn` BigQuery Stored Procedure**: Create the main orchestration stored procedure (`project.dataset.r_ausd_bp_ta_msisdn.sql`) based on the design.
   - **Language**: BigQuery SQL
4. **Define BigQuery Schemas**: Create DDL for `job_error_log`, `job_audit_log`, and any target tables that `d_ausd_bp_ta_msisdn.sql` writes to.
   - **Language**: BigQuery DDL
5. **(Optional) Generate `postprocess_cibasis` BigQuery Stored Procedure**: If required, implement the commented post-processing logic as a BigQuery Stored Procedure.
   - **Language**: BigQuery SQL
6. **Develop Airflow DAG**: Create a Python-based Airflow DAG to orchestrate the execution of the BigQuery stored procedures.
   - **Language**: Python
7. **Testing**: Develop unit and integration tests for BigQuery stored procedures and the Airflow DAG.
   - **Language**: SQL (for BQ procedures), Python (for DAG and end-to-end tests)