# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_apn_ve.ksh`, is a KornShell control script designed to manage the execution of an SQL script (`d_ausd_v_ta_apn_ve.sql`). Its primary business purpose, as indicated by the source code comments, is to:
*   Control the execution of `r_ausd_vertrag.ksh` (implied by the control script description).
*   Ignore active jobs to prevent concurrent execution.
*   Invoke the external SQL script to perform data processing.
*   Update a job table with execution details.
*   Deactivate older active jobs.
The overall job is described as having a "medium" stage distribution in the `lineage_assembled_jobs` record.

## 2. Source Inventory
The job consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Unknown (file_complexity analysis was not available for this file). However, the detailed migration design provided suggests a 'medium' complexity for its translation given the number of shell constructs to be replaced.
    *   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The target architecture in BigQuery will involve:
*   **BigQuery Stored Procedures:** The shell script's logic, including parameter parsing, validation, job control, and SQL script invocation, will be migrated into one or more BigQuery Stored Procedures.
    *   `project.dataset.r_ausd_vertrag_control`: Main control procedure replacing the KornShell script.
    *   `project.dataset.starte_sql_skript`: A helper procedure to manage job activation/deactivation logic and call the actual data processing SQL.
    *   `project.dataset.d_ausd_v_ta_apn_ve`: A stored procedure or a series of SQL statements representing the migrated logic from the original `d_ausd_v_ta_apn_ve.sql` file.
*   **BigQuery Tables:**
    *   `project.dataset.error_log`: For centralized error logging, replacing shell error messages.
    *   `project.dataset.job_table`: To manage job states (active/deactive), replacing the shell script's internal job control mechanism.
    *   `project.dataset.job_run_audit`: To log execution details and record counts, replacing the temporary file and implicit job tracking.
    *   `project.dataset.target_table_for_ta_apn_ve`: The ultimate target table that the `d_ausd_v_ta_apn_ve.sql` script (and its BigQuery equivalent) populates or modifies.
*   **Configuration:** Deployment/configuration will be managed via stored procedure parameters, BigQuery dataset/project constants, or a dedicated configuration table.

## 4. Data Flow & Lineage
The original script `k_ausd_v_ta_apn_ve.ksh` acts as a wrapper or orchestrator.
1.  **Input Parameters:** `p_JobKennung` and `p_EintragsNr` are passed as command-line arguments.
2.  **Initialization:** The script sources environment files (`.dw_init`) and utility scripts (for error handling, date, parameter parsing, and SQLPlus interaction).
3.  **Parameter Validation:** The script validates the presence of required parameters. If validation fails, an error message is displayed, and the script exits.
4.  **SQL Script Invocation:** The script defines the path to an external SQL file (`d_ausd_v_ta_apn_ve.sql`). It then calls a shell function `starteSQLSkript` which is responsible for executing this SQL script, passing `p_EintragsNr` and `p_JobKennung`. This function also handles logic to ignore active jobs and potentially deactivate older ones.
5.  **Record Count:** After SQL execution, the script captures a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp`).
6.  **Completion:** A completion message is printed.

**BigQuery Data Flow:**
*   **Input:** The BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control` will accept `p_JobKennung` and `p_EintragsNr` as direct procedure parameters.
*   **Parameter Validation & Error Handling:** `IF` statements and `ASSERT` (or equivalent) will handle parameter validation. Errors will be logged to `project.dataset.error_log`.
*   **Job Control:** The `project.dataset.starte_sql_skript` procedure will query and update `project.dataset.job_table` to manage job activation/deactivation.
*   **SQL Logic Execution:** The core data processing logic, translated from `d_ausd_v_ta_apn_ve.sql`, will reside within `project.dataset.d_ausd_v_ta_apn_ve` (a stored procedure). It will likely read from source tables and write to `project.dataset.target_table_for_ta_apn_ve`.
*   **Record Count & Audit:** The record count will be obtained directly via `COUNT(*)` queries on the target table (or from the SQL logic's return value) and logged to `project.dataset.job_run_audit`.

No direct `lineage_edges` were found between this file and other objects in the `lineage_edges` table for this run. The dependency on `d_ausd_v_ta_apn_ve.sql` is identified via static analysis of the shell script.

## 5. Transformation Logic
The transformation logic involves converting shell script constructs and patterns to BigQuery SQL, primarily within Stored Procedures.

*   **Environment Variables:** Shell environment variables like `$HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL` will be replaced with BigQuery Stored Procedure parameters, configuration tables, or dataset/project constants.
*   **Parameter Parsing (`getopts`):** The `getopts` logic will be replaced by directly passing parameters to the BigQuery Stored Procedure.
*   **Conditional Logic (`if`, `case`):** Shell `if` and `case` statements will be translated to BigQuery SQL `IF` and `CASE` statements.
*   **Error Handling:** The custom `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` calls will be replaced by inserts into a BigQuery `error_log` table, potentially using `RAISE` or `SELECT ERROR()` for immediate termination.
*   **SQL Script Execution (`starteSQLSkript`):** The invocation of the external SQL script will be replaced by a `CALL` to a BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_apn_ve`) that encapsulates the migrated SQL logic. Dynamic SQL (`EXECUTE IMMEDIATE`) could be used if the SQL script name is variable, but a direct stored procedure call is preferred for stability.
*   **Temporary File Record Count:** The `eval "v_records=`cat $tmpFile`"` construct will be replaced by direct assignment from a `SELECT COUNT(*)` query or by capturing the return value of the SQL execution. This count will then be persisted to an audit table.
*   **Job Control Logic:** The logic for ignoring active jobs and deactivating older ones will be translated into `SELECT`, `UPDATE`, and `INSERT` statements against the `project.dataset.job_table`.
*   **Core SQL Logic:** The actual transformations and aggregations that currently reside in `d_ausd_v_ta_apn_ve.sql` will need to be translated from their original SQL dialect (likely Oracle SQL or similar, given the `sqlplus` utility hint) into BigQuery SQL. This is assumed to be the most significant part of the transformation work and is not detailed in the shell script analysis.

## 6. External Dependencies
The original script has the following external dependencies:

*   **Environment Initialization:** `. $HOME/.dw_init`
    *   **Replacement:** Configuration parameters for BigQuery procedures, or values retrieved from a BigQuery configuration table.
*   **Utility Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQLPlus interaction)
    *   **Replacement:** BigQuery SQL's built-in functions, `IF` blocks, and inserts into logging/error tables will replace these. The SQLPlus interaction will be replaced by direct BigQuery SQL execution within procedures.
*   **External SQL Script:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_apn_ve.sql`
    *   **Replacement:** This script's content will be fully migrated into a BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_apn_ve`).
*   **Temporary Files:** `$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp`
    *   **Replacement:** BigQuery variables or audit/logging tables.

No external systems (like Oracle, SFTP, S3) were explicitly identified in the `lineage_assembled_jobs` record or `lineage_external_systems`. However, the hint of `h_alis_sqlplus.ksh` suggests the original SQL script `d_ausd_v_ta_apn_ve.sql` likely interacts with an RDBMS (e.g., Oracle). The migration of that SQL content will need to consider specific database dialect conversion.

## 7. Unresolved / Risks
*   **Core SQL Logic in `d_ausd_v_ta_apn_ve.sql`:** The primary risk lies in the unknown complexity and content of the external SQL script `d_ausd_v_ta_apn_ve.sql`. If it contains complex Oracle-specific SQL, PL/SQL, or unsupported functions, these will require dedicated migration effort and potential redesign. This was not available for direct analysis in this step.
*   **Job Management Semantics:** The exact business rules for "ignoring active jobs" and "deactivating older active jobs" need to be thoroughly understood to ensure accurate replication in the BigQuery `job_table` logic.
*   **`file_complexity` data:** The absence of `file_complexity` data means the formal complexity tier and migration flags for this specific file are unknown. This might hide specific migration challenges not immediately obvious from the code.

## 8. Build Plan
The build plan focuses on translating the shell script's orchestration and control flow into BigQuery Stored Procedures and BigQuery tables for auditing and logging.

1.  **Define BigQuery Schema:**
    *   Create `project.dataset.error_log` table (for logging errors).
    *   Create `project.dataset.job_table` table (for job status management).
    *   Create `project.dataset.job_run_audit` table (for run metrics and record counts).
    *   Identify and define `project.dataset.target_table_for_ta_apn_ve` schema (based on the original `ta_apn_ve` table and the logic in `d_ausd_v_ta_apn_ve.sql`).
2.  **Migrate `d_ausd_v_ta_apn_ve.sql` to BigQuery Stored Procedure:**
    *   **File:** `d_ausd_v_ta_apn_ve.sql`
    *   **Target:** `project.dataset.d_ausd_v_ta_apn_ve` (BigQuery Stored Procedure)
    *   **Language:** BQSQL
    *   **Notes:** This is a placeholder; the actual content of this SQL file needs to be analyzed and translated.
3.  **Create BigQuery Stored Procedure for Job Control Wrapper:**
    *   **File:** `k_ausd_v_ta_apn_ve.ksh` (subset of logic)
    *   **Target:** `project.dataset.starte_sql_skript` (BigQuery Stored Procedure)
    *   **Language:** BQSQL
    *   **Description:** Implements the logic for deactivating older jobs, checking for active jobs, and calling the core SQL procedure.
4.  **Create Main BigQuery Stored Procedure:**
    *   **File:** `k_ausd_v_ta_apn_ve.ksh` (main logic)
    *   **Target:** `project.dataset.r_ausd_vertrag_control` (BigQuery Stored Procedure)
    *   **Language:** BQSQL
    *   **Description:** Handles parameter validation, calls `project.dataset.starte_sql_skript`, retrieves record counts, and logs audit information.
5.  **Orchestration (Optional but Recommended):**
    *   If the original script is part of a larger scheduled workflow, consider re-implementing the orchestration using Cloud Composer (Apache Airflow), Workflows, or Cloud Scheduler. This would involve creating a Python DAG (for Airflow) to call the BigQuery Stored Procedures.
    *   **Target:** Airflow DAG (Python)
    *   **Language:** Python
    *   **Description:** DAG to invoke `project.dataset.r_ausd_vertrag_control` and manage its scheduling.