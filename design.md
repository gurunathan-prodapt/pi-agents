# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope
This document outlines the migration plan for the KornShell script `k_ausd_bp_ta_bpr_apn.ksh`. This script acts as a control and orchestration component within a data processing workflow. Its primary purpose is to:
- Parse and validate input parameters (job identifier, entry number, reference date, restart value).
- Validate the format of the reference date.
- Source several utility shell scripts for error handling, date functions, and SQLPlus execution.
- Dynamically determine "yesterday" and "today" dates.
- Orchestrate the execution of a core SQL script, `d_ausd_bp_ta_bpr_apn.sql`, passing various parameters to it.
- Capture the number of records processed (via a temporary file) and potentially update a job tracking table.
- Implement basic error handling and logging.

The scope of this migration is to re-platform this KornShell orchestration logic and its associated SQL execution to Google Cloud Platform, leveraging BigQuery for data processing and storage, and Cloud Composer (Airflow) for orchestration.

## 2. Source Inventory
The primary component of this job is a single KornShell script.

| File Path                                                         | Technology | Tier          | Automation Bucket |
| :---------------------------------------------------------------- | :--------- | :------------ | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` | KornShell  | Not available | `semi_auto`       |

**Summary of `k_ausd_bp_ta_bpr_apn.ksh`:**
This script is categorized as a `shell` script and its primary role is `pipeline_orchestrator`. It is an `active` component of the ETL process. The analysis indicates transformation patterns including `Orchestration` (re-platform to Cloud Composer) and `Dynamic_SQL_substitution` (re-factor using BigQuery DWH and Cloud Composer).

## 3. Target Architecture
The target architecture will leverage Google Cloud services for a scalable and maintainable solution:

- **Orchestration Layer:** Google Cloud Composer (Apache Airflow) will manage the overall workflow, scheduling, parameter passing, and dependency management.
- **Data Processing Layer:** Google BigQuery will be used for all data storage and transformation logic. The core SQL script and the shell's control logic will be refactored into BigQuery Stored Procedures.
- **Logging & Monitoring:** Cloud Logging and Monitoring will capture execution logs and metrics from both Cloud Composer and BigQuery. Error logging will also utilize dedicated BigQuery logging tables.
- **Configuration:** Project-level and dataset-level configurations, along with dynamic parameters, will be managed through Airflow DAG parameters or dedicated BigQuery configuration tables.

**Component Mapping:**
- `k_ausd_bp_ta_bpr_apn.ksh` (Orchestration logic) -> Airflow DAG (Python) invoking a BigQuery Stored Procedure.
- `d_ausd_bp_ta_bpr_apn.sql` (Core SQL logic) -> BigQuery Stored Procedure.
- Helper shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.) -> BigQuery UDFs or incorporated into BigQuery Stored Procedures / Airflow utilities.
- `PoolBasisprodukt` (Output Table) -> BigQuery Table.

## 4. Data Flow & Lineage
The original script's data flow involves parameter inputs, sourcing helper scripts, executing a SQL script, and producing an output count.

**Original Flow:**
1. **Inputs:** `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` (command-line parameters).
2. **Environment & Utilities:** Sources `.dw_init`, `f_alis_msgerr.ksh` (error handling), `h_alis_date.ksh` (date validation), `h_alis_parameter.ksh` (parameter parsing), `h_alis_sqlplus.ksh` (SQLPlus utilities).
3. **Date Derivation:** Executes `gestern.ksh` to get `p_datum_heute` and `p_datum_gestern`.
4. **Core Transformation:** Invokes `starteSQLSkript`, which executes `d_ausd_bp_ta_bpr_apn.sql`. This SQL script likely reads from source tables and writes to a target table (`PoolBasisprodukt`) or a temporary file.
5. **Record Count:** Reads `v_records` from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`).
6. **Optional Job Tracking:** (Commented out) Updates a job tracking table using `FOSJobErzeugeEintrag` with `v_TabName` (`PoolBasisprodukt`).

**Target BigQuery/Airflow Flow:**
1. **Airflow DAG Trigger:** An Airflow DAG is triggered, receiving parameters (job_id, entry_num, stichtag, restart_value) from its configuration or scheduler.
2. **Parameter Validation:** The Airflow DAG or a BigQuery Stored Procedure (called by the DAG) performs validation on the input parameters (e.g., presence, date format). Errors are logged to BigQuery logging tables and potentially raise Airflow exceptions.
3. **Date Calculation:** BigQuery's native date functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`) replace `gestern.ksh`.
4. **Core Transformation (BigQuery SP):** A BigQuery Stored Procedure, derived from `d_ausd_bp_ta_bpr_apn.sql`, is executed. This SP will handle the data transformations, reading from appropriate BigQuery source tables and writing to the target BigQuery table `PoolBasisprodukt`.
5. **Record Count:** The BigQuery Stored Procedure directly calculates the record count using `COUNT(*)` on the target table.
6. **Job Tracking:** A BigQuery Stored Procedure or an Airflow task inserts/updates a BigQuery job tracking table.
7. **Logging:** All steps log their status and any relevant metrics to Cloud Logging, and detailed job-specific information to a BigQuery audit/logging table.

## 5. Transformation Logic
The transformation logic will be primarily converted from KornShell and Oracle SQL to BigQuery SQL, wrapped in BigQuery Stored Procedures.

**Key Transformations:**
- **Parameter Handling:**
    - **Legacy:** `getopts` for command-line arguments.
    - **Target:** BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING`, etc.). Airflow can pass these parameters to the BigQuery SP.
- **Environment Setup:**
    - **Legacy:** Sourcing `.dw_init` and various helper `.ksh` scripts.
    - **Target:** Configuration values (e.g., project IDs, dataset names) will be passed as Airflow DAG parameters or retrieved from BigQuery configuration tables. Common utility functions from `.ksh` files will be re-implemented as BigQuery UDFs or inlined into the main procedures.
- **Date Validation & Calculation:**
    - **Legacy:** `DWDate_Datum_Check` and `gestern.ksh`.
    - **Target:** BigQuery's `REGEXP_CONTAINS`, `PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB()` functions for date validation and derivation.
- **Error Handling:**
    - **Legacy:** `ErrNr`, `ErrArg`, `DWMSG_MeldeFehler`, `exit $ErrNr`.
    - **Target:** BigQuery procedural `IF` statements, `SIGNAL SQLSTATE` for error raising, and dedicated `error_log` BigQuery tables for capturing error details. Airflow can catch and handle BigQuery exceptions.
- **Dynamic SQL Execution:**
    - **Legacy:** `starteSQLSkript` executing `d_ausd_bp_ta_bpr_apn.sql` with variables substituted.
    - **Target:** `d_ausd_bp_ta_bpr_apn.sql` will be converted to a standalone BigQuery Stored Procedure that accepts parameters. The main orchestration procedure (migrated `k_ausd_bp_ta_bpr_apn.ksh`) will `CALL` this new BigQuery SP.
- **Record Counting:**
    - **Legacy:** Reading from a temporary file `tmpFile` using `cat` and `eval`.
    - **Target:** Direct `SELECT COUNT(*)` from the target BigQuery table within the stored procedure, assigning the result to a `DECLARE`d variable.
- **Job Tracking:**
    - **Legacy:** (Commented out) `FOSJobErzeugeEintrag` into a job table.
    - **Target:** An `INSERT` statement into a BigQuery `job_tracking` table, if this functionality is required.

## 6. External Dependencies
The original script has dependencies on other shell scripts and an external SQL file. There were no external systems (like Oracle, SFTP) identified by the initial `lineage_assembled_jobs` query.

**Legacy Dependencies:**
- **Environment Initialization:** `.dw_init`
- **Utility Scripts:**
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error messaging)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date utilities)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing utilities)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQLPlus execution utilities)
    - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (Provides 'today' and 'yesterday' dates)
- **Core SQL Script:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_apn.sql`
- **Target Table:** `PoolBasisprodukt`

**Replacement in BigQuery/Airflow:**
- **Environment Initialization:** Replaced by Airflow's environment configuration and BigQuery project/dataset references.
- **Utility Scripts:**
    - Error messaging: Replaced by BigQuery error handling (`SIGNAL SQLSTATE`, `error_log` table) and Airflow logging.
    - Date utilities: Replaced by BigQuery's rich set of date and time functions.
    - Parameter parsing: Replaced by BigQuery Stored Procedure parameters and Airflow DAG parameters.
    - SQLPlus execution: Not applicable, as SQL will run natively in BigQuery.
    - Date derivation (`gestern.ksh`): Replaced by `CURRENT_DATE()` and `DATE_SUB()` in BigQuery.
- **Core SQL Script (`d_ausd_bp_ta_bpr_apn.sql`):** Migrated to a dedicated BigQuery Stored Procedure. This will involve converting any Oracle-specific SQL dialect to BigQuery Standard SQL.
- **Target Table (`PoolBasisprodukt`):** Migrated to a BigQuery table in the target dataset.

## 7. Unresolved / Risks
- **Tier Information:** The `file_complexity` table did not return rows for this job, so a specific complexity tier (simple, medium, complex, very_complex) and migration flags are not available from the automated analysis. The `semi_auto` bucket suggests some manual intervention or review will be needed.
- **Detailed SQL Conversion:** The core SQL script `d_ausd_bp_ta_bpr_apn.sql` was referenced but not analyzed in detail here. Its content needs to be thoroughly analyzed for Oracle-specific syntax, functions, or features that require direct conversion to BigQuery Standard SQL.
- **Commented-out Code:** The script contains commented-out sections (e.g., FOS job management, sed/sort/join operations). It needs to be confirmed whether these functionalities are truly deprecated or if they represent dormant logic that might need to be revived and migrated. This document assumes they are inactive as per the current code state.
- **Error Code Semantics:** If the original script relies on very specific `ErrNr` values for external systems, the BigQuery `SIGNAL SQLSTATE` and error logging might need custom mapping to preserve these semantics.
- **Parameter Validation Granularity:** The `pruefeParameterGesetzt` function in the original script might have specific validation rules that need to be fully replicated in the BigQuery stored procedure or Airflow tasks.

## 8. Build Plan
The migration will follow these steps, producing BigQuery and Airflow artifacts:

1.  **Define BigQuery Datasets:**
    *   Create a target BigQuery dataset (e.g., `prod_dw_isrpt`) to host the migrated tables and procedures.
    *   Create a `logging` dataset (e.g., `prod_dw_logs`) for error and job tracking tables.
2.  **Migrate Target Table:**
    *   Convert the schema for `PoolBasisprodukt` to BigQuery.
    *   Create the `PoolBasisprodukt` table in the target BigQuery dataset (e.g., `prod_dw_isrpt.PoolBasisprodukt`).
3.  **Migrate Logging and Job Tracking Tables:**
    *   Define schemas for `error_log` and `job_tracking` tables based on the error handling and optional job management in the original script.
    *   Create these tables in the `prod_dw_logs` BigQuery dataset.
4.  **Convert `d_ausd_bp_ta_bpr_apn.sql` to BigQuery Stored Procedure:**
    *   **Language:** BigQuery Standard SQL
    *   Refactor the SQL content into a `CREATE OR REPLACE PROCEDURE` statement.
    *   Replace any Oracle-specific syntax with BigQuery equivalents.
    *   Parameterize the procedure to accept inputs like `p_EintragsNr`, `p_JobKennung`, `p_Stichtag`, `v_restart`, `v_datum_heute`, `v_datum_gestern`.
    *   Deploy this procedure to BigQuery (e.g., `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp`).
5.  **Convert `k_ausd_bp_ta_bpr_apn.ksh` to BigQuery Stored Procedure:**
    *   **Language:** BigQuery Standard SQL
    *   Implement the parameter parsing, validation, date calculations, and error handling using BigQuery procedural language (`DECLARE`, `SET`, `IF`, `SIGNAL SQLSTATE`, `INSERT` into `error_log`).
    *   Replace calls to sourced helper scripts with native BigQuery logic.
    *   Replace the `starteSQLSkript` call with a `CALL` statement to the migrated `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp` procedure.
    *   Implement record counting using `SELECT COUNT(*)` on the target table.
    *   (If required) Implement the `job_tracking` table update.
    *   Deploy this procedure to BigQuery (e.g., `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp`).
6.  **Create Airflow DAG:**
    *   **Language:** Python
    *   Develop an Airflow DAG that orchestrates the execution.
    *   Define parameters for the DAG (job_id, entry_num, stichtag, restart_value).
    *   Use `BigQueryOperator` or `BigQueryExecuteStoredProcedureOperator` to call `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp`, passing the necessary parameters.
    *   Configure logging and alerting for the DAG.
7.  **Testing:** Implement unit and integration tests for the BigQuery Stored Procedures and the Airflow DAG.