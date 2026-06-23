# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration design for the ETL job `k_ausd_bp_ta_rn_vertrag.ksh` to Google BigQuery. The original job is a KornShell script that acts as an orchestrator, handling parameter parsing, date validation, and executing a core SQL script (`d_ausd_bp_ta_rn_vertrag.sql`). The primary business purpose is to process data related to `PoolBasisprodukt` by reading from `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN`, and writing to `SOF$TA_RN_VERTRAG`. The scope of this migration is to convert the entire workflow, including the orchestration logic and the data transformation logic, into a BigQuery-native solution.

## 2. Source Inventory
The job consists of a single primary source file and its direct dependencies:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh`
    *   **Technology:** KornShell
    *   **Category:** Shell Script
    *   **Summary:** Control script for a data processing job, handling parameter validation, date validation, and orchestrating SQL script execution. It also manages a temporary file for record counts.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Dependencies:**
        *   Shell scripts: `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`, `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
        *   SQL script: `d_ausd_bp_ta_rn_vertrag.sql`
        *   Temporary file: `$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp`
        *   Tables: `PoolBasisprodukt` (declared `v_TabName`)

*   **File (Invoked):** `d_ausd_bp_ta_rn_vertrag.sql` (Inferred path: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql`)
    *   **Technology:** SQL
    *   **Summary:** Contains the core data manipulation logic.
    *   **Dependencies:**
        *   Reads tables: `DWTK_MELDUNGEN`, `SOF$TA_RN_EINZELN`
        *   Writes table: `SOF$TA_RN_VERTRAG`
        *   Uses package: `DWPA_UTIL_SKRIPT`

## 3. Target Architecture
The target architecture will leverage Google BigQuery's capabilities for both orchestration and data processing.

*   **Orchestration:** The shell script's logic will be converted into a BigQuery Stored Procedure. This procedure will handle parameter validation, date calculations, and execution of the core data transformation logic.
*   **Data Transformation:** The SQL logic from `d_ausd_bp_ta_rn_vertrag.sql` will be refactored and integrated into the BigQuery Stored Procedure or as separate, modular BigQuery SQL statements/views/procedures called from the main orchestration procedure.
*   **Logging & Error Handling:** Custom logging tables (`project.dataset.error_log`, `project.dataset.job_log`) will be created in BigQuery to replace shell-based logging and temporary files.
*   **Scheduling:** The BigQuery Stored Procedure can be scheduled using Cloud Composer (for complex workflows), Cloud Scheduler, or directly invoked via BigQuery Jobs API.
*   **Data Storage:** All source and target tables (`DWTK_MELDUNGEN`, `SOF$TA_RN_EINZELN`, `SOF$TA_RN_VERTRAG`, `PoolBasisprodukt`) will reside in BigQuery datasets. The schema for these tables will be defined using BigQuery DDL.

## 4. Data Flow & Lineage
The original data flow is:
1.  `k_ausd_bp_ta_rn_vertrag.ksh` (Shell Script) is executed with parameters (`-j`, `-f`, `-s`, `-l`).
2.  The shell script performs parameter validation and date checks using various utility scripts.
3.  It invokes the SQL script `d_ausd_bp_ta_rn_vertrag.sql` via a `starteSQLSkript` function (presumably via `sqlplus` given the sourced `h_alis_sqlplus.ksh`).
4.  The `d_ausd_bp_ta_rn_vertrag.sql` script:
    *   Reads data from `TABLE:DWTK_MELDUNGEN`.
    *   Reads data from `TABLE:SOF$TA_RN_EINZELN`.
    *   Uses `PACKAGE:DWPA_UTIL_SKRIPT`.
    *   Writes processed data to `TABLE:SOF$TA_RN_VERTRAG`.
5.  After SQL execution, the shell script reads a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp`) and logs the information (though commented out in the provided source).

**Target BigQuery Data Flow:**
1.  A BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_bp_ta_rn_vertrag`, is invoked with equivalent parameters.
2.  Inside the stored procedure:
    *   Parameter validation and date checks are performed using BigQuery SQL's procedural statements (e.g., `IF`, `ASSERT`, `SAFE.PARSE_DATE`).
    *   Date calculations (e.g., "today", "yesterday") use BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`).
    *   The core data transformation logic, derived from `d_ausd_bp_ta_rn_vertrag.sql`, is executed using BigQuery DML/DDL. This will involve `SELECT` statements from `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_RN_EINZELN` and `INSERT/UPDATE/MERGE` into `project.dataset.SOF_TA_RN_VERTRAG`.
    *   Record counts are obtained directly via `SELECT COUNT(*)` within the stored procedure.
    *   Logging of job status and errors is performed by inserting records into `project.dataset.job_log` and `project.dataset.error_log`.

## 5. Transformation Logic
The transformation logic will be split into two main parts in BigQuery:

**5.1. Orchestration Logic (from `k_ausd_bp_ta_rn_vertrag.ksh` -> BigQuery Stored Procedure)**

*   **Parameter Handling:** The `getopts` logic will be replaced by direct parameters to the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   **Validation:**
    *   `pruefeParameterGesetzt` will be translated to `IF p_param IS NULL OR p_param = '' THEN ...` blocks.
    *   `DWDate_Datum_Check` will be replaced by `REGEXP_CONTAINS(p_Stichtag, r'^[0-9]{8}$')` and `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` for date format and validity checks.
*   **Date Calculation:** The call to `gestern.ksh` will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Execution:** The `starteSQLSkript` call will be replaced by the direct execution of the migrated SQL logic within the BigQuery Stored Procedure.
*   **Record Counting:** The `eval "v_records=`cat $tmpFile`"` will be replaced by a `SELECT COUNT(*)` statement storing the result in a `DECLARE`d variable.
*   **Logging:** The commented-out `FOSJobErzeugeEintrag` will be implemented as an `INSERT` statement into a BigQuery job log table. Error logging will use `INSERT` into an error log table, potentially combined with `ASSERT` or `RAISE` statements.

**5.2. Data Transformation Logic (from `d_ausd_bp_ta_rn_vertrag.sql` -> BigQuery SQL)**

*   The SQL script `d_ausd_bp_ta_rn_vertrag.sql` (contents not provided, but inferred from lineage) needs to be analyzed and converted to BigQuery Standard SQL.
*   **Reads:** `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` will become `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_RN_EINZELN`.
*   **Writes:** `SOF$TA_RN_VERTRAG` will become `project.dataset.SOF_TA_RN_VERTRAG`.
*   **Package Usage:** The `DWPA_UTIL_SKRIPT` package functions must be identified and their logic reimplemented in BigQuery Standard SQL, potentially as user-defined functions (UDFs) or sub-procedures.
*   All Oracle-specific SQL syntax (if any) will be converted to BigQuery Standard SQL.

## 6. External Dependencies
The original job has the following external dependencies and their proposed BigQuery replacements:

*   **Oracle Database:** This is the underlying database for `DWTK_MELDUNGEN`, `SOF$TA_RN_EINZELN`, `SOF$TA_RN_VERTRAG`, and potentially `PoolBasisprodukt`.
    *   **Replacement:** All these tables will be migrated to BigQuery. Data ingestion from Oracle to BigQuery will be handled by a separate data loading process (e.g., using Cloud Data Fusion, Dataflow, or a custom solution to replicate data).
*   **Shell Environment (`$HOME/.dw_init`, `${BERT_DIR_ROOT}`):** The shell script relies on environment variables and sourcing other shell scripts.
    *   **Replacement:** Environment variables will be replaced by parameters to the BigQuery Stored Procedure, or configurable project/dataset names. Utility script logic will be re-implemented in BigQuery SQL or Python if necessary (e.g., for complex string manipulations not easily done in SQL).
*   **Temporary Files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp`):** Used for passing record counts.
    *   **Replacement:** BigQuery Stored Procedures can use `DECLARE` variables to store and pass intermediate values, eliminating the need for temporary files.
*   **`sqlplus` (implied by `h_alis_sqlplus.ksh`):** Used to execute the SQL script.
    *   **Replacement:** The SQL logic will be directly integrated into the BigQuery Stored Procedure, removing the need for an external SQL client.

## 7. Unresolved / Risks
*   **`d_ausd_bp_ta_rn_vertrag.sql` content:** The actual SQL code for `d_ausd_bp_ta_rn_vertrag.sql` is not available. A detailed analysis of this SQL script is required to precisely identify transformations, potential Oracle-specific functions, and the exact logic for creating/updating `SOF$TA_RN_VERTRAG`. This is a critical next step for the detailed design of the BigQuery SQL transformation.
*   **`DWPA_UTIL_SKRIPT` package:** The functions and procedures within this Oracle package need to be understood and re-implemented in BigQuery.
*   **Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** These commented-out lines indicate a job management system. While inactive in the provided code, if they were to become active or represent a broader pattern, a BigQuery-native job logging and monitoring solution (e.g., Cloud Logging, custom audit tables) would be required.
*   **Error Handling Complexity:** The current error handling uses `DWMSG_MeldeFehler` and shell exit codes. The BigQuery migration will need to ensure equivalent error reporting and handling mechanisms, possibly involving `RAISE` and `INSERT` into an error log table.
*   **Data Loading Strategy:** The mechanism for loading data from the legacy Oracle tables (`DWTK_MELDUNGEN`, `SOF$TA_RN_EINZELN`, `PoolBasisprodukt`) into BigQuery is outside the scope of this job's migration but must be addressed as a prerequisite.

## 8. Build Plan
The migration will be executed in the following steps:

1.  **Analyze `d_ausd_bp_ta_rn_vertrag.sql`:** Obtain the source code for the SQL script and perform a detailed analysis to understand its exact logic, table interactions, and any Oracle-specific features. Identify functions from `DWPA_UTIL_SKRIPT` used.
2.  **Schema Migration:**
    *   Define BigQuery DDL for the source tables (`DWTK_MELDUNGEN`, `SOF$TA_RN_EINZELN`, `PoolBasisprodukt`) and the target table (`SOF$TA_RN_VERTRAG`) based on the legacy Oracle schemas.
    *   Create BigQuery datasets and tables.
    *   Define DDL for logging tables (`project.dataset.job_log`, `project.dataset.error_log`).
3.  **Data Ingestion Pipeline:** Implement a pipeline to regularly load data from the legacy Oracle source tables into the new BigQuery tables. This is a prerequisite to running the migrated ETL job.
4.  **Develop BigQuery SQL Transformation Logic:**
    *   Translate the core SQL logic from `d_ausd_bp_ta_rn_vertrag.sql` into BigQuery Standard SQL, creating a modular BigQuery Stored Procedure or a set of SQL statements.
    *   Re-implement any `DWPA_UTIL_SKRIPT` functions as BigQuery UDFs or equivalent BigQuery SQL constructs.
5.  **Develop BigQuery Orchestration Stored Procedure:**
    *   Create a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_bp_ta_rn_vertrag`) to encapsulate the logic from `k_ausd_bp_ta_rn_vertrag.ksh`.
    *   Implement parameter handling, validation, date calculations, and error logging within this procedure.
    *   Integrate the BigQuery SQL transformation logic (from step 4) into this main procedure.
6.  **Unit Testing:** Develop and execute unit tests for the BigQuery Stored Procedure and the integrated SQL logic to ensure functional equivalence with the legacy system.
7.  **Integration Testing:** Test the entire BigQuery workflow, including parameter passing, data transformations, and logging, with representative data.
8.  **Deployment:** Deploy the BigQuery Stored Procedure, DDL, and any UDFs to the target BigQuery environment.
9.  **Scheduling:** Configure a scheduling mechanism (Cloud Composer, Cloud Scheduler) to trigger the BigQuery Stored Procedure as per the original job's schedule.
10. **Monitoring & Alerting:** Set up Cloud Monitoring and Alerting for the BigQuery job.