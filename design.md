# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope

This job is a KornShell control script (`k_ausd_bp_ta_bpr_apn.ksh`) responsible for orchestrating a data processing step. Its primary purpose is to handle parameter parsing and validation, specifically for job identification, entry number, and a key date (`Stichtag`). It then executes a core SQL script (`d_ausd_bp_ta_bpr_apn.sql`) with these validated parameters. The script also manages environment initialization, error handling, and records the number of processed records. Optionally, it contains commented-out logic for post-processing temporary output files (cleaning, sorting, joining, and CSV generation).

The scope of this migration is to translate the shell script's control flow, parameter handling, and SQL execution logic, along with its implicit data flow, into BigQuery compatible components. The commented-out file processing logic will also be migrated to demonstrate its equivalent BigQuery implementation.

## 2. Source Inventory

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh`
    *   **Technology**: KornShell Script
    *   **Category**: shell
    *   **Tool**: KornShell
    *   **Summary**: Control script that validates parameters, sources utility scripts, and orchestrates the execution of a SQL script.
    *   **Tier**: medium
    *   **Migration Flags**: None
    *   **Migration Bucket**: semi_auto
*   **Dependent Script**: `d_ausd_bp_ta_bpr_apn.sql` (executed by `k_ausd_bp_ta_bpr_apn.ksh`)
    *   This SQL script reads from `TABLE:DWTK_MELDUNGEN` and `TABLE:SOF$TA_BPR_INSTANCE`, and writes to `TABLE:SOF$TA_BPR_APN`. It also uses `PACKAGE:DWPA_UTIL_SKRIPT`.
*   **Invoking Script**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh` invokes the seed script.
*   **Sourced Utility Scripts**:
    *   `$HOME/.dw_init` (environment initialization)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date check)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helpers)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus routines)
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (derives yesterday's and today's dates)

## 3. Target Architecture

The target architecture will leverage Google Cloud's BigQuery for data processing and storage, with Cloud Composer (Apache Airflow) for orchestration.

*   **Main Processing Logic**: The core logic of `k_ausd_bp_ta_bpr_apn.ksh` will be migrated to a BigQuery Stored Procedure. This procedure will handle parameter validation, date checks, and orchestration of the main SQL logic.
*   **SQL Logic**: The `d_ausd_bp_ta_bpr_apn.sql` script's logic will be converted into a separate BigQuery Stored Procedure, which will be called by the main orchestration procedure.
*   **Error Handling**: Legacy error reporting (`DWMSG_MeldeFehler`) will be replaced by `INSERT` statements into a dedicated BigQuery error log table and `RAISE` statements for immediate process termination.
*   **Parameter Passing**: Shell script parameters (`-j`, `-f`, `-s`, `-l`) will become input parameters to the BigQuery Stored Procedure.
*   **Date Derivation**: Calls to `gestern.ksh` will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions.
*   **Temporary Files**: The temporary file used to store record counts will be replaced by BigQuery variables or by capturing the count directly from DML statement results or a subsequent `SELECT COUNT(*)` query on the target table.
*   **Commented File Post-processing**: The `sed`, `sort`, `join` operations will be translated into standard BigQuery SQL queries, potentially creating intermediate tables or views, ultimately leading to the final `cibasisprodukt.csv` equivalent table/export.
*   **Job Logging**: The intended job-table entry creation will be translated into an `INSERT` statement into a BigQuery audit/job control table.
*   **Orchestration**: A Cloud Composer (Airflow) DAG will be created to sequence the execution of the BigQuery Stored Procedures and any other necessary BigQuery operations.

## 4. Data Flow & Lineage

The current data flow is orchestrated by the KornShell script.
1.  **Environment Setup**: `k_ausd_bp_ta_bpr_apn.ksh` loads environment variables and utility functions.
2.  **Parameter Input**: The script receives `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as command-line arguments.
3.  **Validation**: `h_alis_parameter.ksh` and `h_alis_date.ksh` (sourced) are used to validate input parameters and the `p_Stichtag` format.
4.  **SQL Script Execution**: The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) executes `d_ausd_bp_ta_bpr_apn.sql`.
    *   `d_ausd_bp_ta_bpr_apn.sql` **reads** data from `TABLE:DWTK_MELDUNGEN` and `TABLE:SOF$TA_BPR_INSTANCE`.
    *   `d_ausd_bp_ta_bpr_apn.sql` **writes** data into `TABLE:SOF$TA_BPR_APN`.
    *   `d_ausd_bp_ta_bpr_apn.sql` utilizes `PACKAGE:DWPA_UTIL_SKRIPT`.
5.  **Record Count**: The script captures the number of records processed by the SQL script into a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`).
6.  **Job Logging**: An (inactive) step to create an entry in a job table using `FOSJobErzeugeEintrag` is present.
7.  **Post-processing (Commented)**: The script contains commented-out `sed`, `sort`, and `join` commands which would process intermediate files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) to produce a final `cibasisprodukt.csv`.

**Target BigQuery Data Flow**:
1.  **Airflow DAG**: An Airflow DAG will trigger the main BigQuery Stored Procedure.
2.  **Main BQ Stored Procedure**:
    *   Receives input parameters.
    *   Performs parameter and date validation using BQ SQL constructs.
    *   Calls the `d_ausd_bp_ta_bpr_apn` BigQuery Stored Procedure.
    *   Captures record counts directly from the `d_ausd_bp_ta_bpr_apn` procedure's output or a `SELECT COUNT(*)` on the target table.
    *   Inserts audit information into a BigQuery job log table.
    *   Handles errors by inserting into an error log table and raising exceptions.
3.  **`d_ausd_bp_ta_bpr_apn` BQ Stored Procedure**:
    *   Performs the data extraction, transformation, and loading (ETL) logic.
    *   Reads from BigQuery tables corresponding to `DWTK_MELDUNGEN` and `SOF$TA_BPR_INSTANCE`.
    *   Writes to a BigQuery table corresponding to `SOF$TA_BPR_APN`.
    *   `DWPA_UTIL_SKRIPT` functionality will be replatformed to equivalent BigQuery UDFs or native SQL.
4.  **BQ Post-processing (Optional)**: If the commented file processing becomes active, separate BigQuery SQL statements (or potentially another stored procedure) will perform the cleansing, sorting (DISTINCT), and joining operations on BigQuery tables, producing a final BigQuery table.

## 5. Transformation Logic

**Original KornShell (`k_ausd_bp_ta_bpr_apn.ksh`) to BigQuery Stored Procedure:**

*   **Parameter Parsing**: `getopts` logic will be replaced by explicitly defined `IN` parameters for the BigQuery Stored Procedure.
    *   `p_JobKennung` (STRING), `p_EintragsNr` (STRING), `p_Stichtag` (STRING), `p_wiederanlaufWert` (INT64).
*   **Environment Initialization**: `. $HOME/.dw_init` will be handled by setting appropriate BigQuery project and dataset context, or by passing environment-specific parameters to the stored procedure.
*   **Error Handling**: Sourced `f_alis_msgerr.ksh` and `pruefeParameterGesetzt` calls will be replaced by BigQuery `IF` conditions, `ASSERT` statements, `INSERT` into an `error_log` table, and `RAISE` for errors.
*   **Date Check**: `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will be replaced by `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and a check for `NULL` result, potentially with a `REGEXP_CONTAINS` check for initial format validation.
*   **Date Derivation**: `set `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` ` will be replaced by `DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE(); DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`.
*   **SQL Script Execution**: `starteSQLSkript ... d_ausd_bp_ta_bpr_apn.sql ...` will be replaced by a `CALL` statement to the migrated BigQuery stored procedure for `d_ausd_bp_ta_bpr_apn.sql`.
*   **Record Count**: `eval "v_records=`cat $tmpFile`"` will be replaced by capturing the `ROW_COUNT()` from DML or `SELECT COUNT(*)` on the target table.
*   **Job Table Entry**: The commented `FOSJobErzeugeEintrag` will become an `INSERT` statement into a BigQuery audit table.

**Commented File Processing Logic to BigQuery SQL:**

*   `sed s/\\ //g` : `REPLACE(column_name, ' ', '')`
*   `sort -u -k 1 -t ';'` : `SELECT DISTINCT column1, column2, ... FROM table ORDER BY column1`
*   `join -j1 1 -j2 1 -o 2.1,1.2,2.2 -a 2 -t ';'` : `FULL OUTER JOIN` or `LEFT JOIN` operations with `COALESCE` for key alignment.

**SQL Script (`d_ausd_bp_ta_bpr_apn.sql`) to BigQuery Stored Procedure:**

*   The existing SQL logic will be directly translated to BigQuery SQL syntax. This includes:
    *   `FROM isbert_schema.dwtk_meldungen` -> `FROM project.dataset.dwtk_meldungen`
    *   `FROM sof$ta_bpr_instance` -> `FROM project.dataset.sof_ta_bpr_instance` (assuming `SOF$` prefix implies a schema that maps to a dataset)
    *   `INTO sof$ta_bpr_apn` -> `INSERT INTO project.dataset.sof_ta_bpr_apn`
*   `PACKAGE:DWPA_UTIL_SKRIPT` calls will need to be analyzed for their functionality and re-implemented as BigQuery UDFs, stored procedures, or inline SQL as appropriate.

## 6. External Dependencies

*   **Database (Oracle)**: The current SQL script interacts with tables that are likely on an Oracle database (implied by `SQLPLUS` in the shell script and table names).
    *   **Replacement**: These source tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_INSTANCE`, `SOF$TA_BPR_APN`) will be migrated to BigQuery tables. Data ingestion from the source Oracle system to BigQuery will be handled by a separate data pipeline (e.g., Change Data Capture using Datastream, batch load via Dataflow, or Federation).
*   **Utility Scripts**:
    *   `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh` are shell-based helper scripts.
    *   **Replacement**: Their functionality will be absorbed directly into the BigQuery Stored Procedure using native BigQuery SQL functions, `ASSERT` statements, and a dedicated error logging table.
*   **Filesystem Operations**: Temporary files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`) and potential commented output files (`cibasis_data*.dat`, `cibasisprodukt.csv`) are filesystem-based.
    *   **Replacement**:
        *   Temporary record counts will be handled by BigQuery variables or direct `SELECT COUNT(*)` queries.
        *   Intermediate files for the commented post-processing will be replaced by temporary BigQuery tables or Common Table Expressions (CTEs).
        *   If `cibasisprodukt.csv` is truly an external file, it will be generated by `EXPORT DATA OPTIONS(uri='gs://bucket/path/cibasisprodukt.csv', format='CSV')` to Google Cloud Storage.

## 7. Unresolved / Risks

*   **`DWPA_UTIL_SKRIPT`**: The exact functionality of `PACKAGE:DWPA_UTIL_SKRIPT` used by `d_ausd_bp_ta_bpr_apn.sql` is unknown from the current analysis. This package needs to be thoroughly analyzed to ensure its correct translation to BigQuery UDFs or other BigQuery constructs. This is a potential `B3: Manual` or `B4: Redesign` item.
*   **Error Codes**: The legacy `ErrNr` (error number) system needs a mapping to BigQuery's error handling mechanisms or a custom error code system within BigQuery.
*   **Commented Code Activation**: The commented post-processing logic (`sed`, `sort`, `join`) is currently inactive. If this logic is to be activated in BigQuery, it represents additional development effort.
*   **`r_ausd_bp_ta_bpr_apn.ksh`**: The invoking script for `k_ausd_bp_ta_bpr_apn.ksh` (`r_ausd_bp_ta_bpr_apn.ksh`) implies a larger job structure. The migration of this parent job will need to account for the new BigQuery Stored Procedure.
*   **`p_wiederanlaufWert`**: The "restart/recovery value" needs careful consideration to ensure BigQuery stored procedure idempotency or proper state management if a restart mechanism is crucial.
*   **`v_TabName='PoolBasisprodukt'`**: The usage and significance of this table name need further investigation, especially in the context of `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` which are commented or external functions.

## 8. Build Plan

1.  **Migrate Source Data to BigQuery**:
    *   Create BigQuery tables for `DWTK_MELDUNGEN`, `SOF$TA_BPR_INSTANCE`, and `SOF$TA_BPR_APN`.
    *   Implement data ingestion pipelines to load data from the legacy Oracle sources into these BigQuery tables. (Language: Dataflow/Datastream/GCS Load jobs)
2.  **Migrate `DWPA_UTIL_SKRIPT`**:
    *   Analyze `DWPA_UTIL_SKRIPT` functionality.
    *   Develop BigQuery UDFs or stored procedures for its equivalent logic. (Language: BigQuery SQL)
3.  **Develop `d_ausd_bp_ta_bpr_apn` BigQuery Stored Procedure**:
    *   Translate the SQL logic from `d_ausd_bp_ta_bpr_apn.sql` into a BigQuery Stored Procedure, referencing the new BigQuery tables and migrated `DWPA_UTIL_SKRIPT` functionality. (Language: BigQuery SQL)
4.  **Develop `k_ausd_bp_ta_bpr_apn` BigQuery Stored Procedure**:
    *   Create the main BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_bp_ta_bpr_apn`) to replace the KornShell script.
    *   Implement parameter validation, date checks, and error handling.
    *   Include a `CALL` statement to the `d_ausd_bp_ta_bpr_apn` stored procedure.
    *   Implement logic to capture record counts and insert into the BigQuery job log table. (Language: BigQuery SQL)
5.  **Develop BigQuery Post-processing (Optional)**:
    *   If the commented shell post-processing (`sed`, `sort`, `join`) needs to be implemented, create separate BigQuery SQL scripts or stored procedures to perform these operations on BigQuery tables.
    *   If CSV export is required, add `EXPORT DATA` statements. (Language: BigQuery SQL)
6.  **Create BigQuery Audit and Error Log Tables**:
    *   Define DDL for `project.dataset.job_table` (audit log) and `project.dataset.error_log`. (Language: BigQuery SQL)
7.  **Develop Cloud Composer (Airflow) DAG**:
    *   Create an Airflow DAG to orchestrate the execution of the main BigQuery Stored Procedure and any subsequent post-processing steps.
    *   Configure task dependencies and parameter passing. (Language: Python)
8.  **Testing**: Develop comprehensive test cases for each BigQuery component and the end-to-end Airflow DAG.