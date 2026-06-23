# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_bpr_beschr.ksh`, serves as a control script orchestrating the execution of a core SQL script (`d_ausd_bp_ta_bpr_beschr.sql`) for data preparation. Its primary functions include parsing command-line parameters, validating input, managing error handling, invoking the SQL processing logic, and recording job statistics. It is an integral part of the data warehousing process, specifically within the `isbert` domain for data Aufbereitung (preparation/enrichment). The job is assembled from a single component file and is categorized as having medium complexity.

## 2. Source Inventory

### File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`
*   **Technology:** KornShell Script
*   **Complexity Tier:** medium
*   **Automation Bucket:** semi_auto
*   **Purpose:** ETL Orchestration / Data Processing Control Script
*   **Description:** This script manages the execution flow, parameter handling, and error checking for the underlying SQL transformation. It sources several utility scripts and invokes an external SQL script. It also includes commented sections that suggest post-processing steps with standard Unix utilities (`sed`, `sort`, `join`) and job management (FOSJob).

**Direct Dependencies Identified from Content Analysis:**
*   `$HOME/.dw_init` (Environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error handling utility)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date utility)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing utility)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQLPlus helper utility, likely wraps `sqlplus` commands)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (Script to determine yesterday's and today's dates)
*   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_beschr.sql` (Core SQL transformation logic)

## 3. Target Architecture
The target architecture will leverage Google BigQuery's capabilities for data processing and orchestration. The KornShell script's logic will be migrated primarily into a BigQuery Stored Procedure, and the core SQL transformation from `d_ausd_bp_ta_bpr_beschr.sql` will also be translated into BigQuery-compatible SQL, likely as another stored procedure or a series of SQL statements.

*   **Orchestration Layer:** Cloud Composer (Apache Airflow) DAG to schedule and orchestrate the BigQuery Stored Procedure execution.
*   **Data Processing Layer:** BigQuery Stored Procedures for the main control logic and the core SQL transformations.
*   **Data Storage:** BigQuery tables for source data, intermediate results, and target data. Temporary files in the source system will be replaced by temporary BigQuery tables or CTEs.
*   **Logging & Monitoring:** BigQuery logging tables and Cloud Monitoring for job status, errors, and performance metrics.

**High-Level Components:**
*   **`dataset.r_ausd_bp_ta_bpr_beschr` (BigQuery Stored Procedure):** This will encapsulate the parameter parsing, validation, date checks, and the invocation of the core SQL transformation.
*   **`dataset.d_ausd_bp_ta_bpr_beschr_core` (BigQuery Stored Procedure/SQL):** This will contain the business logic from the original `d_ausd_bp_ta_bpr_beschr.sql`.
*   **`dataset.job_audit_table` (BigQuery Table):** To replace the job-tracking functionality (like `FOSJobErzeugeEintrag`).
*   **`dataset.target_result_table` (BigQuery Table):** The table where the `d_ausd_bp_ta_bpr_beschr.sql` performs its primary writes, used for record counting.

## 4. Data Flow & Lineage
The original data flow involves an external scheduler (likely UC4 based on other job names) invoking `k_ausd_bp_ta_bpr_beschr.ksh` with specific parameters.

**Current Flow (Legacy):**
1.  **External Scheduler** -> **`k_ausd_bp_ta_bpr_beschr.ksh`**
2.  `k_ausd_bp_ta_bpr_beschr.ksh` sources various utility shell scripts.
3.  `k_ausd_bp_ta_bpr_beschr.ksh` parses input parameters (`-j`, `-f`, `-s`, `-l`).
4.  `k_ausd_bp_ta_bpr_beschr.ksh` performs parameter validation and date format checks.
5.  `k_ausd_bp_ta_bpr_beschr.ksh` calls `gestern.ksh` to get current and previous dates.
6.  `k_ausd_bp_ta_bpr_beschr.ksh` invokes the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) which executes `d_ausd_bp_ta_bpr_beschr.sql` against an Oracle database, passing parameters and a temporary file path for record count.
7.  `d_ausd_bp_ta_bpr_beschr.sql` reads from source tables (implicit) and writes/updates target tables (implicit).
8.  `k_ausd_bp_ta_bpr_beschr.ksh` reads the record count from the temporary file.
9.  `k_ausd_bp_ta_bpr_beschr.ksh` (optionally) updates a job tracking system (`FOSJobErzeugeEintrag`).

**Target Flow (BigQuery):**
1.  **Cloud Composer DAG** (or Scheduled Query) -> **`dataset.r_ausd_bp_ta_bpr_beschr` (BQ Stored Procedure)**
2.  The BQ Stored Procedure receives input parameters (Job ID, Entry Number, Stichtag, Wiederanlaufwert).
3.  The BQ Stored Procedure performs parameter validation and date format checks using native BigQuery functions.
4.  The BQ Stored Procedure derives current and previous dates using `CURRENT_DATE()` and `DATE_SUB()`.
5.  The BQ Stored Procedure calls `dataset.d_ausd_bp_ta_bpr_beschr_core` (another BQ Stored Procedure/SQL script) with the necessary parameters.
6.  `dataset.d_ausd_bp_ta_bpr_beschr_core` reads from BigQuery source tables and writes/updates BigQuery target tables.
7.  The `dataset.r_ausd_bp_ta_bpr_beschr` procedure captures the record count directly from the target table (`SELECT COUNT(*) ...`).
8.  The `dataset.r_ausd_bp_ta_bpr_beschr` procedure inserts an entry into `dataset.job_audit_table` for job tracking.

## 5. Transformation Logic
The core transformation logic resides within `d_ausd_bp_ta_bpr_beschr.sql`, which is executed by `k_ausd_bp_ta_bpr_beschr.ksh`. The `k_ausd_bp_ta_bpr_beschr.ksh` script itself primarily handles orchestration rather than direct data transformation.

**Key Transformations in `k_ausd_bp_ta_bpr_beschr.ksh` (to be migrated to BigQuery Stored Procedure):**
*   **Parameter Handling:** Command-line parameters (`j`, `f`, `s`, `l`) are parsed and assigned to shell variables (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`). In BigQuery, these will be mapped to `IN` parameters of the stored procedure.
*   **Parameter Validation:** Checks ensure that `Jobkennung`, `Stichtag`, and `EintragsNr` are set. This will be converted to `IF ... THEN RAISE ... END IF;` statements in BigQuery SQL.
*   **Date Validation:** `p_Stichtag` is validated against `DDMMYYYY` format using `DWDate_Datum_Check`. In BigQuery, `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` will be used, and a `NULL` result indicates an invalid format, triggering an error.
*   **Date Derivation:** `gestern.ksh` computes today's and yesterday's dates. In BigQuery, `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` will be used.
*   **SQL Execution Orchestration:** The `starteSQLSkript` function executes `d_ausd_bp_ta_bpr_beschr.sql`. This will be replaced by a `CALL` to a dedicated BigQuery Stored Procedure containing the migrated SQL logic.
*   **Record Counting:** The script reads a record count from a temporary file. In BigQuery, this will be replaced by a `SELECT COUNT(*)` query on the target table.
*   **Job Logging:** The commented `FOSJobErzeugeEintrag` implies logging job status and record counts. This will be implemented as an `INSERT` statement into a BigQuery audit table.

## 6. External Dependencies
The source system has several implicit and explicit external dependencies:

*   **Oracle Database:** The `d_ausd_bp_ta_bpr_beschr.sql` script is executed against an Oracle database via SQL*Plus (orchestrated by `h_alis_sqlplus.ksh`).
    *   **Replacement:** All Oracle source tables involved in `d_ausd_bp_ta_bpr_beschr.sql` must be migrated or replicated to BigQuery. The SQL logic itself will be translated to BigQuery SQL.
*   **Utility KornShell Scripts:**
    *   `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`. These provide environment setup, error handling, date functions, parameter parsing, SQL execution wrappers, and date calculation.
    *   **Replacement:**
        *   Environment variables will be managed through BigQuery Stored Procedure parameters, Cloud Composer environment variables, or explicit configuration files.
        *   Error handling will use BigQuery's `RAISE` statement or inserts into logging tables.
        *   Date and parameter parsing/validation will use native BigQuery SQL functions.
        *   The `gestern.ksh` logic will be replaced by BigQuery's date functions.
        *   The `h_alis_sqlplus.ksh` wrapper will be obsolete as SQL will be executed natively in BigQuery.
*   **Temporary File System:** Used for storing the record count (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_beschr.tmp`).
    *   **Replacement:** Replaced by BigQuery variables, CTEs, or direct `COUNT(*)` queries on the target tables.
*   **FOS Job Management System:** Implied by `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` (though commented out).
    *   **Replacement:** Replaced by custom audit/control tables in BigQuery for job status tracking.
*   **Unix Utilities (`sed`, `sort`, `join`):** The commented-out post-processing logic utilizes these.
    *   **Replacement:** If this logic becomes active and required, it will be translated into BigQuery SQL transformations using functions like `REPLACE`, `ARRAY_AGG`, `STRING_AGG`, `QUALIFY ROW_NUMBER() OVER (...)`, and standard SQL `JOIN` operations.

## 7. Unresolved / Risks

*   **Absence of Lineage Edges:** The `lineage_edges` query did not return any direct dependencies for the seed script or its directly referenced files. This suggests a potential gap in the automated lineage analysis for this specific job or a type of dependency not captured (e.g., shell sourcing). The design relies heavily on static code analysis.
*   **Core SQL (`d_ausd_bp_ta_bpr_beschr.sql`) Details:** The content of the actual SQL script was not available. Its complexity, source/target tables, and specific transformation logic are critical for a complete migration. This will be the next major component to analyze and migrate.
*   **Commented-out Code:** The `sed`/`sort`/`join` data manipulation and the FOS job management calls are commented out. The migration assumes they are currently inactive. If they become active requirements, their migration will add complexity.
*   **Dynamic `BERT_DIR_ROOT`:** The `${BERT_DIR_ROOT}` variable is crucial for resolving paths. Its value needs to be explicitly defined in the BigQuery environment (e.g., as a parameter to the stored procedure or as an Airflow variable).
*   **Error Handling Granularity:** The original `f_alis_msgerr.ksh` might provide specific error codes or logging mechanisms that need to be carefully replicated or adapted to BigQuery's error handling and logging capabilities.

## 8. Build Plan

The migration will involve creating the following artifacts:

1.  **`d_ausd_bp_ta_bpr_beschr_core.sql` (BigQuery SQL Script):**
    *   **Language:** BigQuery SQL
    *   **Content:** Translated business logic from the original `d_ausd_bp_ta_bpr_beschr.sql`. This will likely involve `CREATE TABLE AS SELECT` or `INSERT INTO SELECT` statements.
    *   **Dependency:** This is the core transformation.

2.  **`r_ausd_bp_ta_bpr_beschr.sql` (BigQuery Stored Procedure DDL):**
    *   **Language:** BigQuery SQL (DDL for Stored Procedure)
    *   **Content:** Contains the orchestration logic derived from `k_ausd_bp_ta_bpr_beschr.ksh`, including parameter handling, validation, date derivation, calling `d_ausd_bp_ta_bpr_beschr_core`, record counting, and audit logging.
    *   **Dependency:** Depends on the migrated `d_ausd_bp_ta_bpr_beschr_core` and the `job_audit_table` schema.

3.  **`job_audit_table.sql` (BigQuery Table DDL):**
    *   **Language:** BigQuery SQL (DDL for Table)
    *   **Content:** `CREATE TABLE` statement for the audit table to track job execution, mimicking the functionality of `FOSJobErzeugeEintrag`.
    *   **Dependency:** Referenced by `r_ausd_bp_ta_bpr_beschr` stored procedure.

4.  **Cloud Composer DAG (Python Script):**
    *   **Language:** Python
    *   **Content:** An Airflow DAG definition that schedules and executes the `dataset.r_ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure, passing the necessary parameters.
    *   **Dependency:** Orchestrates the execution of `r_ausd_bp_ta_bpr_beschr`.

5.  **Target Table DDLs (BigQuery SQL Scripts):**
    *   **Language:** BigQuery SQL (DDL for Tables)
    *   **Content:** `CREATE TABLE` statements for all target tables written to by `d_ausd_bp_ta_bpr_beschr_core`.
    *   **Dependency:** Required before `d_ausd_bp_ta_bpr_beschr_core` can run.

6.  **Parameter/Configuration Mapping:**
    *   **Content:** Documenting how legacy parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) are mapped to BigQuery Stored Procedure parameters or Airflow variables.

This build plan assumes the migration of the SQL script `d_ausd_bp_ta_bpr_beschr.sql` is part of a broader effort or will be a subsequent task.