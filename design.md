# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_bpr_apn.ksh` to Google Cloud Platform, targeting BigQuery for data processing and potentially Cloud Composer (Airflow) for orchestration.

The script serves as a control and orchestration component for a data processing step. Its primary functions include:
*   Parsing and validating command-line parameters such as job identifier (`Jobkennung`), entry number (`EintragsNr`), and a reference date (`Stichtag`).
*   Validating the format of the provided reference date.
*   Sourcing several utility shell scripts for error handling, date calculations, parameter parsing, and SQL execution.
*   Orchestrating the execution of a core SQL script, `d_ausd_bp_ta_bpr_apn.sql`, passing dynamically generated parameters.
*   Calculating and temporarily storing the count of processed records.
*   Logging job completion and record counts (currently commented out).

The scope of this migration includes replicating the parameter handling, validation logic, orchestration of the SQL process, record counting, and logging mechanisms within the BigQuery ecosystem.

## 2. Source Inventory

**File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh`
*   **Category:** `shell`
*   **Tool:** `KornShell`
*   **Purpose:** `pipeline_orchestrator` (primary), `env_bootstrapper`, `shared_utility`, `data_validator`
*   **Complexity Tier:** `medium`
*   **Migration Bucket:** `semi_auto`
*   **Summary:** This is a control script for a data processing step. It handles parameter parsing, validates input dates, sources several utility scripts, and orchestrates the execution of a SQL script (`d_ausd_bp_ta_bpr_apn.sql`) with dynamic parameters.

**Key Input Parameters:**
*   `p_JobKennung`: Job identifier.
*   `p_EintragsNr`: Entry number.
*   `p_Stichtag`: Reference date in `DDMMYYYY` format.
*   `p_wiederanlaufWert`: Restart value (optional).

**Generated/Derived Variables:**
*   `v_TabName`: Set to `PoolBasisprodukt`.
*   `tmpFile`: Temporary file path for record count.
*   `p_datum_heute`: Current date.
*   `p_datum_gestern`: Previous day's date.
*   `v_records`: Count of processed records.

## 3. Target Architecture

The existing KornShell script, acting as an orchestrator, will be migrated to a BigQuery Stored Procedure. The business logic residing within `d_ausd_bp_ta_bpr_apn.sql` will also be migrated to BigQuery SQL, potentially as a separate stored procedure or directly embedded within the orchestrating procedure via `EXECUTE IMMEDIATE`.

**BigQuery Components:**
*   **Orchestration Logic:** BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_bpr_apn`) will handle parameter parsing, validation, date calculations, and execution of the core business logic.
*   **Business Logic:** The logic from `d_ausd_bp_ta_bpr_apn.sql` will be translated into standard BigQuery SQL.
*   **Error Handling:** A dedicated error log table (`project.dataset.error_log`) will capture validation and execution errors.
*   **Job Logging:** A job log table (`project.dataset.job_log`) will replace the intended (but commented out) job logging mechanism.
*   **Data Storage:** All source and target data will reside in BigQuery tables. The `PoolBasisprodukt` table, referenced in the source script, will be a BigQuery table.
*   **Scheduler:** Google Cloud Composer (Airflow) or Cloud Scheduler can be used to invoke the BigQuery Stored Procedure.

**Conceptual BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_apn`):**
*   **Input Parameters:** Will directly map to the shell script's command-line arguments (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   **Variable Declarations:** BigQuery `DECLARE` statements for internal variables like `v_TabName`, `v_records`, `v_datum_heute`, `v_datum_gestern`.
*   **Conditional Logic:** `IF`, `ELSEIF`, `CASE` statements for parameter validation and error checks.
*   **Date Operations:** BigQuery `CURRENT_DATE()`, `DATE_SUB`, `SAFE.PARSE_DATE` for date calculations and validations.
*   **SQL Execution:** `EXECUTE IMMEDIATE` will be used to run the migrated SQL logic from `d_ausd_bp_ta_bpr_apn.sql`.
*   **Record Counting:** `SELECT COUNT(*)` will replace reading from a temporary file.
*   **Logging:** `INSERT` statements into `project.dataset.error_log` and `project.dataset.job_log`.

## 4. Data Flow & Lineage

The original script's data flow orchestrates the execution of a SQL script that likely processes data and updates the `PoolBasisprodukt` table.

**Conceptual Data Flow in GCP:**
1.  **Trigger:** An Airflow DAG (Cloud Composer) or Cloud Scheduler job initiates the `r_ausd_bp_ta_bpr_apn` BigQuery Stored Procedure, passing required parameters.
2.  **Parameter Handling & Validation:** The stored procedure parses and validates the input parameters. If validation fails, an entry is written to `project.dataset.error_log`, and the procedure raises an error.
3.  **Date Calculation:** The procedure determines `v_datum_heute` and `v_datum_gestern` using BigQuery date functions.
4.  **SQL Business Logic Execution:** The core SQL logic (migrated from `d_ausd_bp_ta_bpr_apn.sql`) is executed. This logic is expected to:
    *   Read data from various source tables (potentially including `PoolBasisprodukt` as an input if it processes historical data).
    *   Perform transformations and aggregations.
    *   Write results to `PoolBasisprodukt` or other target tables.
5.  **Record Counting:** After the SQL logic completes, the procedure counts the affected records (e.g., from `PoolBasisprodukt`) using a `SELECT COUNT(*)` statement.
6.  **Job Logging:** The procedure inserts a record into `project.dataset.job_log` detailing the execution, status, and record count.

**Lineage:**
*   **Input Nodes:**
    *   Parameters: `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`
    *   Date scripts: `gestern.ksh` (logic absorbed into BQ procedure)
    *   SQL script: `d_ausd_bp_ta_bpr_apn.sql` (logic absorbed into BQ procedure/function)
*   **Output Nodes:**
    *   `project.dataset.error_log` (on error)
    *   `project.dataset.job_log` (logging job execution)
    *   `PoolBasisprodukt` (target table updated by the embedded SQL logic)

## 5. Transformation Logic

The KornShell script itself primarily contains orchestration and validation logic. The actual data transformation logic is within `d_ausd_bp_ta_bpr_apn.sql`.

**Migration of Orchestration Logic to BigQuery Stored Procedure:**
*   **Parameter Parsing (`getopts`):** Replaced by named `IN` parameters in the BigQuery Stored Procedure definition.
*   **Variable Assignments:** Replaced by `DECLARE` statements and `SET` operations.
*   **Conditional Logic (`if`, `case`):** Translated to BigQuery SQL scripting `IF` / `ELSEIF` / `END IF` and `CASE` statements.
*   **Error Handling (`pruefeParameterGesetzt`, `DWMSG_MeldeFehler`):** Replaced by `IF` conditions, `INSERT` statements into an error log table, and `RAISE` statements.
*   **Date Validation (`DWDate_Datum_Check`):** Replaced by BigQuery `REGEXP_CONTAINS` for format validation and `SAFE.PARSE_DATE` for semantic validation.
*   **Dynamic SQL Execution (`starteSQLSkript`):** Replaced by `EXECUTE IMMEDIATE` for the migrated SQL content of `d_ausd_bp_ta_bpr_apn.sql`, allowing dynamic parameter substitution within the SQL string.
*   **Temporary File for Record Count (`cat $tmpFile`):** Replaced by a `SELECT COUNT(*)` query against the target table, storing the result in a `DECLARE`d variable.
*   **Job Logging (`FOSJobErzeugeEintrag`):** Replaced by an `INSERT` statement into `project.dataset.job_log`.

**Migration of `d_ausd_bp_ta_bpr_apn.sql`:**
The content of `d_ausd_bp_ta_bpr_apn.sql` must be converted from its current SQL dialect (likely Oracle SQL based on `h_alis_sqlplus.ksh` dependency) to BigQuery Standard SQL. This will involve:
*   Converting Oracle-specific functions (e.g., `NVL`, `DECODE`, date functions) to their BigQuery equivalents.
*   Adjusting data types as needed.
*   Refactoring cursor loops or procedural blocks to set-based BigQuery operations where possible, or to BigQuery scripting loops if necessary.

**Commented-out Post-processing (sed, sort, join):**
If these operations become active in the future, they would be translated to BigQuery SQL transformations using string functions (`REPLACE`), window functions (`QUALIFY`, `ROW_NUMBER`), and `FULL OUTER JOIN` for merging data, leveraging staging tables in BigQuery.

## 6. External Dependencies

The original script has several dependencies which need to be addressed in the migration:

*   **`$HOME/.dw_init`:** Environment initialization. This will be replaced by environment variables or configuration managed by Cloud Composer (if used for scheduling) or passed as parameters to the BigQuery Stored Procedure.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`:** Error message handling. Replaced by BigQuery `RAISE` statements and inserts into `project.dataset.error_log`.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`:** Date handling utilities. Replaced by BigQuery native date functions (`CURRENT_DATE()`, `DATE_SUB`, `SAFE.PARSE_DATE`, `FORMAT_DATE`).
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`:** Parameter parsing utilities. Replaced by BigQuery Stored Procedure parameters and conditional logic.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`:** SQL*Plus related utilities. Replaced by direct BigQuery SQL execution within the stored procedure.
*   **`${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`:** Script to get yesterday's and today's dates. Replaced by BigQuery native date functions.
*   **`${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_apn.sql`:** The core SQL script containing the business logic. This is the most critical dependency. Its content must be fully migrated to BigQuery Standard SQL and either embedded or called as a separate BigQuery Stored Procedure/Function.
*   **`PoolBasisprodukt` (TABLE):** This table is either an input source or an output target for the SQL script. It will be migrated to a BigQuery table.

No external systems (e.g., Oracle, SFTP, S3) were explicitly identified as direct dependencies in the `lineage_assembled_jobs` or `file_analysis` for this specific script. However, if the `d_ausd_bp_ta_bpr_apn.sql` or its data sources rely on external systems, those would need separate migration plans (e.g., data ingestion pipelines from Oracle to BigQuery, Cloud Storage for SFTP/S3 equivalents).

## 7. Unresolved / Risks

*   **Undetermined SQL Dialect:** The exact SQL dialect of `d_ausd_bp_ta_bpr_apn.sql` is not explicitly stated beyond being a dependency of a KornShell script that uses `sqlplus`. It is assumed to be Oracle SQL. Any proprietary functions or complex PL/SQL constructs in `d_ausd_bp_ta_bpr_apn.sql` might require significant refactoring during migration to BigQuery Standard SQL.
*   **Dynamic SQL Complexity:** The `starteSQLSkript` call suggests dynamic parameter substitution into `d_ausd_bp_ta_bpr_apn.sql`. The complexity of this dynamic substitution needs to be fully understood to ensure accurate migration to BigQuery's `EXECUTE IMMEDIATE`.
*   **Commented-Out Logic:** The `sed`, `sort`, `join` operations are commented out in the source script. It is a risk that these might become active in the future or represent dormant requirements. The migration design accounts for their BigQuery equivalents if they were to be reactivated.
*   **FOS Job Management:** References like `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` (commented out) indicate integration with an external job management system. This integration will need to be replaced with GCP-native job logging and scheduling mechanisms.
*   **Error Code Mapping:** The custom error codes (`ErrNr=193`, `ErrNr=192`) will need to be mapped to appropriate BigQuery error handling or a custom error logging system.

## 8. Build Plan

The migration will follow a structured approach to ensure a robust and functional BigQuery implementation.

1.  **Migrate Core SQL Logic (`d_ausd_bp_ta_bpr_apn.sql`):**
    *   **Extract:** Obtain the source code for `d_ausd_bp_ta_bpr_apn.sql`.
    *   **Convert:** Translate the SQL script from its native dialect (assumed Oracle SQL) to BigQuery Standard SQL. This may involve:
        *   Rewriting queries, DDL, DML.
        *   Replacing proprietary functions with BigQuery equivalents.
        *   Refactoring any procedural logic into BigQuery scripting or new stored procedures/functions.
    *   **Test:** Thoroughly test the migrated SQL logic in BigQuery using sample data.

2.  **Develop BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_apn`):**
    *   **Design:** Create the stored procedure with appropriate `IN` parameters matching the original shell script's arguments.
    *   **Implement Validation:** Implement parameter and date validation using BigQuery scripting (`IF`, `REGEXP_CONTAINS`, `SAFE.PARSE_DATE`).
    *   **Integrate SQL Logic:** Embed or call the migrated BigQuery SQL from step 1 using `EXECUTE IMMEDIATE`. Ensure dynamic parameter passing is correctly handled.
    *   **Implement Record Counting:** Add `SELECT COUNT(*)` logic to determine processed record count.
    *   **Develop Logging:** Implement `INSERT` statements for `project.dataset.error_log` and `project.dataset.job_log`.
    *   **Test:** Unit test the stored procedure with various valid and invalid inputs.

3.  **Define BigQuery Schemas:**
    *   Create DDL for `project.dataset.error_log` (e.g., `job_name STRING`, `error_nr INT64`, `error_arg STRING`, `created_at TIMESTAMP`).
    *   Create DDL for `project.dataset.job_log` (e.g., `tab_name STRING`, `status STRING`, `mode STRING`, `stichtag_from DATE`, `stichtag_to DATE`, `job_type STRING`, `restart_flag STRING`, `record_count INT64`, `description STRING`, `job_kennung STRING`, `eintragsnr STRING`, `created_at TIMESTAMP`).
    *   Ensure target table schemas (e.g., `PoolBasisprodukt`) are defined in BigQuery.

4.  **Orchestration (Cloud Composer/Airflow):**
    *   **Design:** Create an Airflow DAG to trigger the BigQuery Stored Procedure.
    *   **Implement:** Define the DAG structure, including BigQuery operators to call the stored procedure with required parameters.
    *   **Schedule:** Configure the DAG to run on the desired schedule, replacing any legacy scheduler.

5.  **Deployment and Monitoring:**
    *   Deploy BigQuery Stored Procedure and table DDL.
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Set up Cloud Monitoring and Logging for the BigQuery job and Airflow DAGs.

**Generated Artifacts:**
*   `project/dataset/r_ausd_bp_ta_bpr_apn.sql` (BigQuery Stored Procedure definition)
*   `project/dataset/ddl_error_log.sql` (DDL for error log table)
*   `project/dataset/ddl_job_log.sql` (DDL for job log table)
*   `project/dataset/ddl_poolbasisprodukt.sql` (DDL for target table, if not already existing)
*   `airflow/dags/k_ausd_bp_ta_bpr_apn_dag.py` (Cloud Composer DAG definition in Python)