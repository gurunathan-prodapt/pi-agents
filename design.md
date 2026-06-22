# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_bp_ref.ksh` to Google Cloud Platform, specifically targeting BigQuery. The original script acts as a control and orchestration wrapper for a core SQL script (`d_ausd_v_ta_bp_ref.sql`). Its primary responsibilities include:
*   Ignoring currently active jobs.
*   Invoking a specific SQL script for data processing.
*   Registering and updating job entries in a job tracking table.
*   Deactivating older active jobs.
The script handles parameter parsing and basic error handling before delegating the main data processing to the SQL component.

## 2. Source Inventory
The job is composed of a single main KornShell script that orchestrates the execution of a SQL script and relies on several utility shell scripts.

*   **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** ETL orchestrator/driver
    *   **Dependencies (sourced KSH scripts):**
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing/validation)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus wrapper, contains `starteSQLSkript` function)
    *   **Core Business Logic:** Delegates to SQL script `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_bp_ref.sql`

## 3. Target Architecture
The migration will leverage BigQuery's capabilities for scripting and data processing. The KornShell script's orchestration logic will be refactored into a BigQuery Stored Procedure, while the core SQL logic will also be migrated to a BigQuery Stored Procedure or script.

*   **Main Control Logic:** A BigQuery Stored Procedure, `project.dataset.r_ausd_vertrag_control`, will replace the `k_ausd_v_ta_bp_ref.ksh` script. This procedure will handle parameter validation, job status updates, and invocation of the business logic.
*   **Business Logic:** The SQL contained within `d_ausd_v_ta_bp_ref.sql` will be migrated into a separate BigQuery Stored Procedure, `project.dataset.d_ausd_v_ta_bp_ref`.
*   **Job Control & Logging:**
    *   A BigQuery table, `project.dataset.job_table`, will store job status and activity, replacing the shell script's implicit job management.
    *   An `error_log` table, `project.dataset.error_log`, will capture error messages and details.
    *   A `job_result` table, `project.dataset.job_result`, will store metrics like record counts, replacing the temporary file usage.
*   **Orchestration:** While not directly part of this migration, the migrated BigQuery Stored Procedure can be scheduled using BigQuery Scheduled Queries or orchestrated by Cloud Composer (Airflow) or Cloud Workflows for broader pipeline integration.

## 4. Data Flow & Lineage
The original script's flow involves receiving parameters, validating them, and then invoking an external SQL script via `sqlplus`. The SQL script performs the actual data manipulation.

**Original Flow:**
1.  `k_ausd_v_ta_bp_ref.ksh` receives parameters `j` (JobKennung) and `f` (EintragsNr).
2.  Sourced utility scripts (`h_alis_parameter.ksh`, `f_alis_msgerr.ksh`) are used for parameter validation and error handling.
3.  The script constructs the path to `d_ausd_v_ta_bp_ref.sql`.
4.  The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) is called, executing `d_ausd_v_ta_bp_ref.sql` via `sqlplus`. This implies interaction with an Oracle database.
5.  Job table updates (e.g., deactivating old jobs) occur, likely within the `starteSQLSkript` function or the `d_ausd_v_ta_bp_ref.sql` script.
6.  A temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_bp_ref_$$.tmp`) is used to store record counts from the SQL execution.
7.  The script reads the record count from the temporary file.

**Target BigQuery Flow:**
1.  The `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure is invoked with `p_JobKennung` and `p_EintragsNr` as input parameters.
2.  The procedure performs parameter validation directly using BigQuery SQL constructs.
3.  Error logging is handled by inserting rows into the `project.dataset.error_log` table.
4.  Job control logic (e.g., deactivating old jobs) is executed via `UPDATE` statements on `project.dataset.job_table`.
5.  The `project.dataset.d_ausd_v_ta_bp_ref` BigQuery Stored Procedure (containing the core business logic) is called, passing necessary parameters. This procedure will return the record count via an `OUT` parameter.
6.  The returned record count is stored in the `project.dataset.job_result` table.

## 5. Transformation Logic
The KornShell script's logic will be translated into a BigQuery Stored Procedure using standard SQL scripting capabilities.

*   **Environment Variables:** Shell environment variables (`$HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL`) will be replaced by:
    *   Input parameters to the Stored Procedure (for dynamic values like `JobKennung`, `EintragsNr`).
    *   Hardcoded string literals or configuration tables (for static directory paths or environment settings).
    *   BigQuery session variables if dynamic runtime configuration is needed.
*   **Parameter Parsing (`getopts`):** The `getopts` logic will be replaced by the direct use of BigQuery Stored Procedure input parameters. Validation checks (`pruefeParameterGesetzt`) will be translated to `IF` statements.
*   **Conditional Logic (`if`, `case`):** Translated directly to BigQuery's `IF...THEN...END IF` and `CASE` expressions.
*   **Error Handling (`f_alis_msgerr.ksh`):** Replaced by `INSERT` statements into the `project.dataset.error_log` table. Exiting with error codes will be replaced by `SIGNAL SQLSTATE` or `RAISE` in BigQuery, or by propagating error status through return codes/out parameters if part of a larger workflow.
*   **SQL Script Execution (`starteSQLSkript`, `sqlplus`):** The invocation of `d_ausd_v_ta_bp_ref.sql` will be replaced by a direct `CALL` to the migrated BigQuery Stored Procedure `project.dataset.d_ausd_v_ta_bp_ref`.
*   **Temporary File (`tmpFile`):** The use of a temporary file for record counts will be replaced by an `OUT` parameter from the `d_ausd_v_ta_bp_ref` procedure, with the result then persisted to the `project.dataset.job_result` table.
*   **Job Deactivation:** The logic to update job status will be implemented as an `UPDATE` statement on the `project.dataset.job_table`.

## 6. External Dependencies
The original script has an implicit dependency on an Oracle database (via `sqlplus` invocation) and relies on the local filesystem for temporary files and sourced utility scripts.

*   **Oracle Database:** The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) suggests interaction with an Oracle database. This dependency will be removed by migrating the SQL script (`d_ausd_v_ta_bp_ref.sql`) content directly into a BigQuery Stored Procedure. All tables referenced in `d_ausd_v_ta_bp_ref.sql` must also be migrated to BigQuery.
*   **Local Filesystem / Temporary Files:** The use of `$DW_DIR_UTL/bert_k_ausd_v_ta_bp_ref_$$.tmp` for storing record counts will be replaced by BigQuery tables (`project.dataset.job_result`) or Stored Procedure `OUT` parameters.
*   **Sourced Utility Scripts:** The functionality of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` will be absorbed and re-implemented using BigQuery SQL scripting capabilities directly within the main control procedure.

## 7. Unresolved / Risks
*   **Content of `d_ausd_v_ta_bp_ref.sql`:** The actual business transformation logic resides in this external SQL script. Its content is unknown at this stage and needs to be analyzed separately for BigQuery compatibility (e.g., Oracle-specific syntax, data types, functions). This is the primary unresolved item.
*   **Business Logic in Sourced Scripts:** While the tool assumes utility-like functions, if `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, or `h_alis_sqlplus.ksh` contain critical business logic or complex state management beyond simple utilities, their full scope must be understood and migrated.
*   **Job Table Structure:** The exact schema and usage of the original "job table" are unknown. The `project.dataset.job_table` and its `active_flag` need to reflect the legacy system's behavior accurately.
*   **`DWMSG_MeldeFehler` Implementation:** The `f_alis_msgerr.ksh` script's `DWMSG_MeldeFehler` function could involve complex logging or alerting mechanisms beyond simple table inserts. This needs to be clarified and implemented accordingly in BigQuery (e.g., Cloud Logging, Pub/Sub for alerts).
*   **`v_records` Evaluation:** The `eval "v_records=\`cat $tmpFile\`"` suggests dynamic command execution, which is not directly translatable. The BigQuery approach of using `OUT` parameters and direct `INSERT`s resolves this.

## 8. Build Plan
The migration involves creating BigQuery objects:

1.  **Create BigQuery Tables (DDL):**
    *   `project.dataset.job_table` (schema to be defined based on legacy job table)
    *   `project.dataset.error_log` (`error_ts DATETIME, error_nr INT64, error_arg STRING, procedure_name STRING`)
    *   `project.dataset.job_result` (`job_kennung STRING, eintrags_nr STRING, record_count INT64, created_ts DATETIME`)
    *   Any other tables required by `d_ausd_v_ta_bp_ref.sql`.
2.  **Develop `project.dataset.d_ausd_v_ta_bp_ref` Stored Procedure (BigQuery SQL):**
    *   Migrate the content of `d_ausd_v_ta_bp_ref.sql` into this procedure, ensuring BigQuery SQL compatibility.
    *   Add an `OUT` parameter to return the record count or relevant execution metrics.
3.  **Develop `project.dataset.r_ausd_vertrag_control` Stored Procedure (BigQuery SQL):**
    *   Implement parameter parsing and validation logic.
    *   Implement job deactivation logic for `job_table`.
    *   Integrate error logging into `error_log`.
    *   Call `project.dataset.d_ausd_v_ta_bp_ref`.
    *   Store the returned record count into `project.dataset.job_result`.
4.  **Test Procedures:** Unit and integration tests for both BigQuery Stored Procedures.
5.  **Deployment:** Deploy the BigQuery DDLs and Stored Procedures.
6.  **Orchestration Integration:** If required, set up BigQuery Scheduled Queries, Cloud Composer DAGs, or Cloud Workflows to execute the `r_ausd_vertrag_control` procedure.