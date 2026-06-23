# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

## 1. Purpose & Scope
This document outlines the migration design for the ETL job identified by `run_id 5af228f1-3847-4cc6-9310-ed82ed19407c` and `seed_name vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh`.

The primary purpose of this job is to act as a control script for a data preparation process, specifically for `ta_inv_def`. It is responsible for handling parameter parsing, environment setup, and executing a core SQL script (`d_ausd_v_ta_inv_def.sql`) for data manipulation. The script also includes logic to ignore active jobs, deactivate old active jobs, and log the number of processed records.

## 2. Source Inventory
The job is comprised of two main files:

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh**
    *   **Technology:** KornShell Script
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** KornShell script that acts as a control script for a data preparation process, handling parameter parsing, environment setup, and executing a SQL script for data manipulation.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_inv_def.sql**
    *   **Technology:** Oracle SQL (embedded within the ksh script)
    *   **Summary:** Performs data manipulation on `SOF$TA_INV_DEF` and `VIA` tables, reading from `DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, and `CDS$TA_CARE_DESCRIPTION`.

## 3. Target Architecture
The migration target platform is Google BigQuery. The existing KornShell script orchestrating an SQL script will be re-architected into a combination of BigQuery Stored Procedures and potentially a Cloud Composer (Airflow) DAG for overall orchestration if it's part of a larger workflow.

*   **Orchestration Logic:** The logic from `k_ausd_v_ta_inv_def.ksh` (parameter handling, conditional execution, job management, logging) will be migrated to a BigQuery Stored Procedure. If this job is part of a broader scheduled workflow, an Airflow DAG in Cloud Composer would wrap this stored procedure call.
*   **Data Transformation Logic:** The SQL code from `d_ausd_v_ta_inv_def.sql` will be converted to BigQuery SQL and implemented as a separate BigQuery Stored Procedure.
*   **Data Storage:** All source tables (`DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, `CDS$TA_CARE_DESCRIPTION`) and target tables (`SOF$TA_INV_DEF`, `VIA`) will be created or exist in BigQuery. Data from external Oracle sources will be ingested into BigQuery using appropriate data pipelines (e.g., DataStream, custom ETL).
*   **Logging & Monitoring:** Error handling and logging (currently `DWMSG_MeldeFehler` and temporary files) will be replaced by BigQuery logging tables and BigQuery's native auditing capabilities. Job status tracking will use a dedicated BigQuery job control table.
*   **Configuration:** Environment variables and parameters (`$HOME/.dw_init`, `BERT_DIR_ROOT`, `DW_DIR_UTL`) will be replaced by BigQuery Stored Procedure parameters or a BigQuery configuration table.

## 4. Data Flow & Lineage
The current data flow is as follows:

1.  **Invocation:** The script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh` is invoked by an upstream script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh`.
2.  **Environment Setup:** `k_ausd_v_ta_inv_def.ksh` sources several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) for environment variables, error handling, date functions, parameter parsing, and SQL*Plus execution routines.
3.  **Parameter Handling:** The script parses command-line parameters `-j` (JobKennung) and `-f` (EintragsNr).
4.  **SQL Script Execution:** `k_ausd_v_ta_inv_def.ksh` calls a function `starteSQLSkript` (defined in `h_alis_sqlplus.ksh`) to execute the SQL script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_inv_def.sql`.
5.  **SQL Data Operations:**
    *   `d_ausd_v_ta_inv_def.sql` derives a `v_datum` from `isbert_schema.dwtk_meldungen`.
    *   It then truncates `sof$ta_inv_def` (likely an interim table).
    *   It performs an `INSERT INTO sof$ta_inv_def` by selecting data from `cds$ta_inv_definition`, `cds$ta_inv_cont_config`, and `cds$ta_care_description`. These `CDS$` tables are likely accessed via an Oracle DB link (`@pcrs1`).
    *   It performs a `MERGE` operation into the `VIA` table.
    *   It uses Oracle packages `DWPA_UTIL_SKRIPT` and `ICC`.
6.  **Record Count & Logging:** After SQL execution, `k_ausd_v_ta_inv_def.ksh` reads the number of processed records from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_inv_def_$$.tmp`) into the `v_records` variable.

## 5. Transformation Logic

### a. KornShell Script (`k_ausd_v_ta_inv_def.ksh`) Migration
The orchestrator script's logic will be translated into a BigQuery Stored Procedure, leveraging BigQuery scripting capabilities.

*   **Parameter Parsing:** Command-line arguments `p_JobKennung` and `p_EintragsNr` will become input parameters to the BigQuery Stored Procedure.
*   **Environment Sourcing (`.dw_init`, `BERT_DIR_ROOT`, `DW_DIR_UTL`):** These will be replaced by BigQuery Stored Procedure parameters, BigQuery configuration tables, or environment variables in a Cloud Composer DAG.
*   **Utility Script Calls:**
    *   `f_alis_msgerr.ksh` (error concept): Replaced by BigQuery's `RAISE` statement, BigQuery logging, and a dedicated BigQuery error log table.
    *   `h_alis_date.ksh` (date check): Replaced by BigQuery's built-in date functions (e.g., `CURRENT_DATE()`, `DATE_TRUNC()`).
    *   `h_alis_parameter.ksh` (parameter parsing): Replaced by explicit parameter checks within the stored procedure.
    *   `h_alis_sqlplus.ksh` (SQL routines, `starteSQLSkript`): The SQL execution logic will be directly integrated or called as another BigQuery Stored Procedure.
*   **Job Management:** The logic to "ignore active jobs" and "deactivate old active jobs" will be implemented using DML statements against a BigQuery job control table.
*   **Temporary File (`tmpFile` for `v_records`):** The record count will be captured directly from the SQL transformation output using `SELECT COUNT(*)`, stored in a BigQuery variable, and then logged.
*   **Error Handling:** The `set -e` equivalent will be handled by BigQuery's transactional behavior and explicit `IF` and `RAISE` statements for parameter validation.

### b. SQL Script (`d_ausd_v_ta_inv_def.sql`) Migration
The Oracle SQL script will be converted to BigQuery SQL and encapsulated within a BigQuery Stored Procedure.

*   **Variable Definitions (`DEFINE v_carmen`):** The Oracle DB link `v_carmen` will be handled by configuring external tables in BigQuery pointing to the Oracle source, or by establishing data ingestion pipelines to replicate `CDS$` tables into BigQuery.
*   **Dynamic `v_datum` Derivation:** The `SELECT NVL(TO_CHAR(MAX(m.timecreated),\'YYYYMMDD\'),\'19000101\') FROM isbert_schema.dwtk_meldungen` will be translated to BigQuery SQL using appropriate date functions.
*   **`TRUNCATE TABLE sof$ta_inv_def`:** This will be translated to `TRUNCATE TABLE `project.dataset.sof_ta_inv_def``.
*   **`INSERT INTO ... SELECT ...` statement:** This will be directly translated to BigQuery SQL. Oracle-specific hints (`/*+ full(id) ... parallel(id,4) */`) will be removed or replaced with BigQuery-specific query optimizations where applicable. The `NVL` functions will be converted to BigQuery's `IFNULL` or `COALESCE`. The join conditions and `WHERE` clause logic will be preserved.
*   **`MERGE` statement:** This will be converted to BigQuery's `MERGE` statement, adapting the syntax as needed.
*   **Oracle Packages (`DWPA_UTIL_SKRIPT`, `ICC`):** The functionalities of these packages will need to be re-implemented in BigQuery SQL or Python for any procedural logic they contain.
*   **Control Statements (`START`, `SPOOL`, `WHENEVER SQLERROR`, `COMMIT`):** These Oracle-specific commands will be removed. Transaction management is implicit in BigQuery SQL scripts and stored procedures, or explicit in Cloud Composer DAGs. `SPOOL` output will be handled by BigQuery's logging or by inserting results into a logging table.

## 6. External Dependencies
The job has the following external dependencies:

*   **Oracle Database:**
    *   **Source Tables:** `DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, `CDS$TA_CARE_DESCRIPTION`. These are critical input tables for the SQL script. The `CDS$` tables appear to be accessed via an Oracle DB Link `@pcrs1`.
    *   **Replacement:** Data from these Oracle tables will need to be ingested into BigQuery. Options include:
        *   **BigQuery External Tables:** Pointing directly to the Oracle database (if connectivity and performance allow, less common for production ETL).
        *   **Data Migration Service / Custom ETL:** Replicating these tables into BigQuery regularly using tools like Google Cloud DataStream, or custom ETL jobs (e.g., using Dataflow, Fivetran).
*   **Oracle Packages:** `DWPA_UTIL_SKRIPT`, `ICC`.
    *   **Replacement:** The specific functionalities provided by these Oracle packages will need to be identified and re-implemented in BigQuery SQL (for SQL logic) or Python (for procedural logic) within the BigQuery Stored Procedures or Airflow DAGs.
*   **Local File System/Shell Environment:** The shell script relies on `$HOME/.dw_init` and utility scripts located at `${BERT_DIR_ROOT}/allgemein/is/util/bin/`. It also uses a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_inv_def_$$.tmp`).
    *   **Replacement:** These environmental dependencies will be replaced by BigQuery Stored Procedure parameters, BigQuery configuration tables, or Cloud Composer environment variables/configurations. Temporary file usage will be replaced by BigQuery variables or temporary tables.

## 7. Unresolved / Risks
*   **Full Understanding of Sourced Utility Scripts:** The exact logic and side effects of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` are not fully detailed from the provided information. A thorough analysis of these scripts is required to ensure their functionalities are accurately replicated or replaced in the BigQuery environment.
*   **Oracle-specific SQL Features:** While the `INSERT...SELECT` and `MERGE` statements are generally portable, specific Oracle SQL functions, data types, or advanced features not immediately apparent in the provided snippet might require careful handling during conversion to BigQuery SQL.
*   **Performance Tuning:** Oracle-specific query hints (`parallel(id,4)`) will not apply in BigQuery. BigQuery queries will need to be optimized using native BigQuery techniques.
*   **Job Deactivation Logic:** The explicit logic for "ignoring active jobs" and "deactivating old active jobs" needs full clarification to ensure correct migration into BigQuery or Airflow job control.
*   **`r_ausd_v_ta_inv_def.ksh`:** The upstream invoker `r_ausd_v_ta_inv_def.ksh` must also be analyzed and migrated to ensure a complete end-to-end workflow in the target environment.

## 8. Build Plan
1.  **Analyze and Define BigQuery Schemas (Language: DDL/BigQuery SQL):**
    *   Create target tables: `sof_ta_inv_def`, `via`.
    *   Define schemas for source tables: `dwtk_meldungen`, `cds_ta_inv_definition`, `cds_ta_inv_cont_config`, `cds_ta_care_description`.
    *   Define schemas for logging and job control tables.
2.  **Establish Data Ingestion for Source Tables (Language: Google Cloud DataStream/Dataflow/Python):**
    *   Implement data pipelines to continuously ingest or replicate data from the Oracle source system's `DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, `CDS$TA_CARE_DESCRIPTION` into BigQuery.
3.  **Migrate SQL Transformation Logic (`d_ausd_v_ta_inv_def.sql`) (Language: BigQuery SQL):**
    *   Convert `d_ausd_v_ta_inv_def.sql` into a BigQuery Stored Procedure (e.g., `sp_d_ausd_v_ta_inv_def`).
    *   Address Oracle-specific syntax and package calls.
4.  **Migrate KornShell Orchestration Logic (`k_ausd_v_ta_inv_def.ksh`) (Language: BigQuery SQL / Python for Airflow):**
    *   Convert `k_ausd_v_ta_inv_def.ksh` into a BigQuery Stored Procedure (e.g., `sp_k_ausd_v_ta_inv_def`) that calls `sp_d_ausd_v_ta_inv_def` and handles parameters, logging, and job status updates.
    *   Alternatively, if part of a larger workflow, create a Cloud Composer (Airflow) DAG in Python to orchestrate the call to the BigQuery Stored Procedure.
5.  **Re-implement Utility Functions (Language: BigQuery SQL / Python):**
    *   Translate the core functionalities of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` into BigQuery SQL functions, BigQuery stored procedures, or Python helper functions used within Airflow.
6.  **Migrate Upstream Invoker (`r_ausd_v_ta_inv_def.ksh`) (Language: Python for Airflow / BigQuery SQL):**
    *   Analyze and migrate the `r_ausd_v_ta_inv_def.ksh` script to invoke the new BigQuery-based job.
7.  **Testing and Validation (Language: SQL / Python):**
    *   Develop test cases to validate the migrated BigQuery stored procedures and orchestration.