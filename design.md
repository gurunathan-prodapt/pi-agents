# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh

## 1. Purpose & Scope

This document outlines the migration design for the legacy KornShell script `k_ausd_bp_ta_bpr_opt_text.ksh` to Google BigQuery. The script's primary purpose is to act as a control and orchestration wrapper for a core SQL script, `d_ausd_bp_ta_bpr_opt_text.sql`. It handles parameter validation, date checks, environment setup, and executes the SQL processing, ultimately recording a count of processed records. The migration aims to re-implement this orchestration and business logic natively within the BigQuery ecosystem, leveraging its capabilities for data processing and warehousing.

## 2. Source Inventory

*   **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`
*   **Type:** Shell Script (KornShell)
*   **Tool (Detected):** KornShell
*   **Summary:** Control script that validates parameters, checks dates, and orchestrates the execution of an SQL script (`d_ausd_bp_ta_bpr_opt_text.sql`) to process data, logging record counts.
*   **Purpose:** ETL Orchestration / Control Script
*   **Complexity Tier:** Not available from analysis tables. `lineage_assembled_jobs` indicated a 'medium' complexity for the overall job.
*   **Automation Bucket:** Not available from analysis tables.
*   **Key References:**
    *   **SQL Script:** `d_ausd_bp_ta_bpr_opt_text.sql` (critical, contains core business logic)
    *   **Database Table:** `PoolBasisprodukt` (critical, likely an Oracle table)
    *   **Utility Scripts:** `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`

## 3. Target Architecture

The migrated solution will reside entirely within Google Cloud Platform, primarily utilizing BigQuery for data processing and storage.

*   **Core Logic:** Re-implemented as a BigQuery Stored Procedure:
    *   `project.dataset.r_ausd_bp_ta_bpr_opt_text` (replaces `k_ausd_bp_ta_bpr_opt_text.ksh`)
*   **Data Storage:** BigQuery datasets and tables.
    *   Source data from `PoolBasisprodukt` (and other tables referenced by `d_ausd_bp_ta_bpr_opt_text.sql`) will be ingested into BigQuery tables (e.g., `source_dataset.PoolBasisprodukt`).
    *   Target data produced by the SQL logic will be written to appropriate BigQuery tables.
*   **Auditing and Logging:** Dedicated BigQuery tables for operational insights:
    *   `project.dataset.job_error_audit`: Stores details of job validation and execution errors.
    *   `project.dataset.job_run_audit`: Logs job execution details, including processed record counts.
*   **Orchestration:** A cloud-native scheduler, such as Apache Airflow running on Cloud Composer, will be used to invoke the BigQuery Stored Procedure, passing the necessary parameters.

## 4. Data Flow & Lineage

The original KornShell script orchestrates the execution of an SQL script that interacts with a database. The migrated data flow will mirror this, with BigQuery components replacing legacy elements.

**Legacy Flow:**
1.  **Start:** `k_ausd_bp_ta_bpr_opt_text.ksh` is invoked with parameters (Job ID, Entry Number, Key Date, Restart Value).
2.  **Environment Setup:** Sources `$HOME/.dw_init` and other utility KornShell scripts.
3.  **Parameter Processing:** Parses input parameters `-j`, `-f`, `-s`, `-l`.
4.  **Validation:**
    *   Checks for presence of required parameters (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`).
    *   Validates `p_Stichtag` format (DDMMYYYY) using `h_alis_date.ksh`.
5.  **Date Derivation:** Calls `gestern.ksh` to get `p_datum_heute` and `p_datum_gestern`.
6.  **SQL Execution:** Invokes `d_ausd_bp_ta_bpr_opt_text.sql` via `h_alis_sqlplus.ksh`.
    *   `d_ausd_bp_ta_bpr_opt_text.sql` reads from the `PoolBasisprodukt` table and performs data processing.
    *   The SQL script's output (implicitly, the number of records processed) is captured.
7.  **Record Count Capture:** The script `cat`s a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_opt_text.tmp`) to get the count of records.
8.  **Logging:** (Commented) `FOSJobErzeugeEintrag` for audit purposes.
9.  **End.**

**Migrated Flow (BigQuery):**
1.  **Start:** Cloud Composer (Airflow DAG) invokes `project.dataset.r_ausd_bp_ta_bpr_opt_text` stored procedure. Parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) are passed as arguments.
2.  **Parameter Handling:** Stored procedure declares variables for inputs.
3.  **Validation:**
    *   `IF` statements check for missing required parameters.
    *   `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` validates the date format.
    *   Errors trigger `SIGNAL SQLSTATE` and log to `project.dataset.job_error_audit`.
4.  **Date Derivation:** `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` replace the `gestern.ksh` call.
5.  **Core Business Logic:** The content of `d_ausd_bp_ta_bpr_opt_text.sql` is integrated directly into the stored procedure as BigQuery SQL. This logic reads from `source_dataset.PoolBasisprodukt` and other necessary BigQuery tables, performing transformations and writing results to target BigQuery tables.
6.  **Record Count Capture:** A `SELECT COUNT(*)` query within the stored procedure captures the number of processed records into a BigQuery variable.
7.  **Logging:** `INSERT` statements populate `project.dataset.job_run_audit` with job execution details and record counts.
8.  **End.**

## 5. Transformation Logic

The KornShell script primarily performs control flow and parameter handling. The actual data transformation logic resides in the invoked SQL script.

**Shell Script Logic Transformation:**

*   **Environment Sourcing (`. $HOME/.dw_init`, etc.):** Replaced by:
    *   Explicit parameter passing to the stored procedure.
    *   Configuration variables defined in the Airflow DAG or loaded from a BigQuery config table.
    *   BigQuery project/dataset references.
*   **Parameter Parsing (`getopts`):** Replaced by:
    *   BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING`, etc.).
*   **Parameter Validation (`pruefeParameterGesetzt` function, `if [ ! $ErrNr -eq 0 ]`):** Replaced by:
    *   `IF p_JobKennung IS NULL OR p_JobKennung = '' THEN ... END IF;` statements within the stored procedure.
    *   Error signaling using `SIGNAL SQLSTATE` and logging to `project.dataset.job_error_audit`.
*   **Date Validation (`DWDate_Datum_Check`):** Replaced by:
    *   `SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);`
    *   An `IF v_stichtag_date IS NULL` check for invalid formats.
*   **Date Derivation (`gestern.ksh`):** Replaced by:
    *   `DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();`
    *   `DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`
*   **SQL Script Execution (`starteSQLSkript $Name_SQLskript`):** The content of `d_ausd_bp_ta_bpr_opt_text.sql` will be directly embedded and adapted as BigQuery SQL within the `project.dataset.r_ausd_bp_ta_bpr_opt_text` stored procedure. This is the critical step that needs detailed analysis of the original SQL.
*   **Temporary File for Record Count (`tmpFile`, `eval "v_records=\`cat $tmpFile\`"`):** Replaced by:
    *   `SET v_records = (SELECT COUNT(*) FROM \`project.dataset.target_table\` WHERE ...);` where `target_table` is the output table of the core SQL logic.
*   **Job Bookkeeping (`FOSJobErzeugeEintrag`):** Replaced by:
    *   `INSERT INTO project.dataset.job_run_audit (...) VALUES (...);`

**Commented Legacy File Processing:**
The commented-out `sed`, `sort`, `join` operations, if required in a future state or if they represent dormant business logic, would be migrated as follows:
*   **File Ingestion:** External files (`cibasis_data24.dat`, etc.) loaded into BigQuery staging tables (e.g., `project.dataset.cibasis_data24_stage`).
*   **Blank Removal (`sed s/\\ //g`):** `REPLACE(raw_line, ' ', '')` SQL function.
*   **Sorting & Deduplication (`sort -u -k 1 -t ';' `):** `QUALIFY ROW_NUMBER() OVER (PARTITION BY key_col ORDER BY key_col) = 1` or `SELECT DISTINCT`.
*   **Joining (`join`):** Standard BigQuery `JOIN` operations (e.g., `LEFT JOIN`, `FULL OUTER JOIN`).

## 6. External Dependencies

The initial analysis indicated no explicit external systems via `lineage_assembled_jobs`. However, based on the script content and `file_analysis`, implicit dependencies exist.

*   **Oracle Database (Implicit):**
    *   **Legacy Role:** The `PoolBasisprodukt` table and the `h_alis_sqlplus.ksh` utility imply interaction with an Oracle database as the primary data source for the SQL script.
    *   **Replacement:** This Oracle database will be considered a source system. Data will be extracted from Oracle and ingested into BigQuery (e.g., via Cloud Data Fusion, Dataflow, or a direct connector). The `PoolBasisprodukt` table will be mirrored as `source_dataset.PoolBasisprodukt` in BigQuery.
*   **Filesystem / Environment Variables:**
    *   **Legacy Role:** `$HOME/.dw_init`, `${BERT_DIR_ROOT}`, `$DW_DIR_UTL` are used for environment setup and path resolution.
    *   **Replacement:** These will be replaced by structured configuration in the BigQuery environment. This includes BigQuery project IDs, dataset IDs, and potentially table names passed as parameters or defined in an Airflow DAG.
*   **Utility KornShell Scripts:**
    *   **Legacy Role:** `f_alis_msgerr.ksh` (error handling), `h_alis_date.ksh` (date validation), `h_alis_parameter.ksh` (parameter parsing), `h_alis_sqlplus.ksh` (SQL execution wrapper), `gestern.ksh` (date calculation).
    *   **Replacement:** The functionalities of these scripts will be re-implemented in BigQuery SQL directly within the stored procedure (e.g., `SAFE.PARSE_DATE`, `CURRENT_DATE`, `DATE_SUB`), or handled by the orchestration layer (e.g., Python code in an Airflow DAG for more complex logic if necessary). The `h_alis_sqlplus.ksh` wrapper is fully replaced by native BigQuery SQL execution.
*   **FOS Job Management (Commented):**
    *   **Legacy Role:** `FOSJobDeaktivate`, `FOSJobErzeugeEintrag` (if active) would interact with a legacy job scheduling/management system.
    *   **Replacement:** These functionalities will be replaced by native Cloud Composer/Airflow scheduling, monitoring, and logging features, or by inserts into BigQuery audit tables.

## 7. Unresolved / Risks

*   **Unknown Complexity & Automation Rate:** Due to no rows returned from `file_complexity` and `automation_rate` queries, the system-calculated values for migration effort and automation potential are unavailable. The manual review and tool-generated design indicate a straightforward shell-to-stored-procedure conversion.
*   **Contents of `d_ausd_bp_ta_bpr_opt_text.sql`:** The core business logic resides in this SQL file. A detailed analysis and migration plan for this SQL are crucial and are a prerequisite for fully implementing the BigQuery stored procedure. The current design assumes this SQL can be directly translated to BigQuery SQL.
*   **Oracle Schema Details:** The specific schema and column details for `PoolBasisprodukt` and other tables referenced in `d_ausd_bp_ta_bpr_opt_text.sql` are required for accurate BigQuery table creation and SQL translation.
*   **`p_wiederanlaufWert` Usage:** The script initializes `p_wiederanlaufWert` but its subsequent usage within the script or the invoked `d_ausd_bp_ta_bpr_opt_text.sql` is not apparent from the provided script. This needs clarification to ensure correct migration if it influences the SQL logic.
*   **Error Handling Fidelity:** The exact error codes and messaging from `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` need to be mapped to appropriate BigQuery error handling and audit logging.
*   **Legacy Flat-File Processing (Commented):** While currently commented out, if the `sed/sort/join` pipeline related to `cibasis_data*.dat` is ever re-enabled or becomes relevant for historical data, it represents additional migration work to translate these operations into BigQuery SQL.

## 8. Build Plan

The build plan focuses on creating the necessary BigQuery assets and the orchestration layer.

1.  **Analyze and Migrate `d_ausd_bp_ta_bpr_opt_text.sql` (Priority 1):**
    *   **Action:** Extract the content of `d_ausd_bp_ta_bpr_opt_text.sql`.
    *   **Tool/Language:** Manually or using an SQL migration tool to convert to BigQuery SQL.
    *   **Output:** BigQuery SQL code.
2.  **Define BigQuery Tables (Priority 2):**
    *   **Action:** Create target BigQuery tables for the output of `d_ausd_bp_ta_bpr_opt_text.sql`'s migrated logic.
    *   **Action:** Define BigQuery staging tables for source data ingestion, including `source_dataset.PoolBasisprodukt`.
    *   **Action:** Create `project.dataset.job_error_audit` and `project.dataset.job_run_audit` tables with appropriate schemas.
    *   **Tool/Language:** BigQuery DDL (Data Definition Language).
    *   **Output:** `.sql` DDL files.
3.  **Ingest Source Data (Priority 3):**
    *   **Action:** Establish data pipelines to load `PoolBasisprodukt` and any other required source tables from their legacy systems (e.g., Oracle) into the BigQuery staging tables.
    *   **Tool/Language:** Cloud Data Fusion, Dataflow, or custom ETL scripts (e.g., Python).
    *   **Output:** Populated BigQuery staging tables.
4.  **Create BigQuery Stored Procedure (Priority 4):**
    *   **Action:** Implement `project.dataset.r_ausd_bp_ta_bpr_opt_text` incorporating:
        *   The migrated SQL logic from `d_ausd_bp_ta_bpr_opt_text.sql`.
        *   Parameter validation and error handling using BigQuery SQL.
        *   Date derivation logic using `CURRENT_DATE()` and `DATE_SUB()`.
        *   Record count capture into a variable.
        *   Audit logging inserts into `job_error_audit` and `job_run_audit`.
    *   **Tool/Language:** BigQuery SQL.
    *   **Output:** `r_ausd_bp_ta_bpr_opt_text.sql` (CREATE PROCEDURE statement).
5.  **Develop Orchestration (Priority 5):**
    *   **Action:** Create an Airflow DAG in Cloud Composer to:
        *   Define parameters for the BigQuery stored procedure.
        *   Invoke the `project.dataset.r_ausd_bp_ta_bpr_opt_text` stored procedure using `BigQueryExecuteStoredProcedureOperator` or similar.
        *   Handle scheduling and monitoring.
    *   **Tool/Language:** Python (for Airflow DAG).
    *   **Output:** `k_ausd_bp_ta_bpr_opt_text_dag.py`.
6.  **Testing and Validation (Continuous):**
    *   **Action:** Develop unit, integration, and end-to-end tests for all migrated components.
    *   **Tool/Language:** Python (for testing BigQuery and Airflow), BigQuery SQL (for data validation).
    *   **Output:** Test scripts and results.