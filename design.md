# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh

## 1. Purpose & Scope

The original script, `k_ausd_bp_ta_bpr_basis.ksh`, is a KornShell control script designed to orchestrate data preparation processes. Its primary responsibilities include parsing runtime parameters, setting up the execution environment, validating date formats, and executing a core SQL script named `d_ausd_bp_ta_bpr_basis.sql`. This script also manages temporary files for record counts and includes (commented out) functionality for job logging and data post-processing (sed, sort, join operations).

The scope of this migration is to translate the functionality of `k_ausd_bp_ta_bpr_basis.ksh` to BigQuery, specifically focusing on its role as an orchestrator for the embedded SQL logic and its parameter handling, error checking, and job control aspects. The core data transformation logic within `d_ausd_bp_ta_bpr_basis.sql` is considered a separate, but dependent, migration effort.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh`
    *   **Technology:** KornShell (shell script)
    *   **Description:** Control script for data preparation, handles parameter parsing, environment setup, date validation, and execution of `d_ausd_bp_ta_bpr_basis.sql`.
    *   **Complexity Tier:** Medium (estimated) - due to external script sourcing, parameter handling, and orchestrated SQL execution.
    *   **Automation Bucket:** Semi-Automated (estimated) - while much can be automated, the integration of sourced scripts and the core SQL logic requires careful manual review and design.

## 3. Target Architecture

The migrated job will primarily leverage BigQuery's capabilities for data processing and orchestration.

*   **Main Component:** A **BigQuery Stored Procedure** (`project.dataset.r_ausd_bp_ta_bpr_basis`) will replace the KornShell script. This stored procedure will encapsulate the parameter handling, validation, date calculations, and the execution of the core SQL logic.
*   **Data Processing:** The SQL logic from `d_ausd_bp_ta_bpr_basis.sql` will be migrated into either:
    *   Directly embedded BigQuery SQL statements within the main stored procedure.
    *   A separate, dedicated BigQuery Stored Procedure or a multi-statement BigQuery Script for the core data transformations.
*   **Logging & Auditing:**
    *   **`project.dataset.job_error_log`**: A BigQuery table to capture error messages and execution failures, replacing the `DWMSG_MeldeFehler` mechanism.
    *   **`project.dataset.job_table`**: A BigQuery table for logging job execution details, replacing the functionality of the commented-out `FOSJobErzeugeEintrag`.
*   **Orchestration (Optional, if external scheduling is needed):** A **Cloud Composer DAG** or **Cloud Workflows** could be used to schedule and invoke the BigQuery Stored Procedure, passing required parameters.

## 4. Data Flow & Lineage

**Current Data Flow (Legacy System):**

1.  `k_ausd_bp_ta_bpr_basis.ksh` starts.
2.  Sources various utility KornShell scripts (e.g., `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  Parses command-line parameters (`j`, `f`, `s`, `l`).
4.  Validates mandatory parameters.
5.  Validates the format of `p_Stichtag`.
6.  Executes `gestern.ksh` to derive `p_datum_heute` and `p_datum_gestern`.
7.  Calls `starteSQLSkript` which, in turn, executes `d_ausd_bp_ta_bpr_basis.sql` using tools like SQL*Plus, passing several derived parameters.
8.  Reads the record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis.tmp`).
9.  (Commented out) Updates a job table via `FOSJobErzeugeEintrag`.

**Target Data Flow (BigQuery):**

1.  The BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_basis` is invoked (manually or via Cloud Composer/Workflows) with input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
2.  **Parameter Validation**: `IF` conditions and `RAISE USING MESSAGE` or `INSERT` into `job_error_log` for missing parameters.
3.  **Date Validation**: `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and a `NULL` check to validate `p_Stichtag` format. Errors are logged to `job_error_log`.
4.  **Date Derivation**: `CURRENT_DATE()` and `DATE_SUB()` BigQuery functions are used to get today's and yesterday's dates.
5.  **Core SQL Execution**: The migrated logic of `d_ausd_bp_ta_bpr_basis.sql` is executed within the stored procedure, performing data transformations on BigQuery tables (sources and targets to be determined by `d_ausd_bp_ta_bpr_basis.sql`'s content).
6.  **Record Count**: A BigQuery variable (e.g., `v_records`) is populated using `SELECT COUNT(*)` from the target table after transformations.
7.  **Job Logging**: An `INSERT` statement populates `project.dataset.job_table` with job execution details and the record count.

## 5. Transformation Logic

The KornShell script's logic will be translated into a BigQuery Stored Procedure using BigQuery SQL scripting capabilities:

*   **Parameter Handling:**
    *   `getopts` in `k_ausd_bp_ta_bpr_basis.ksh` maps directly to **input parameters** of the BigQuery Stored Procedure (e.g., `p_JobKennung STRING`, `p_EintragsNr STRING`, `p_Stichtag STRING`, `p_wiederanlaufWert STRING`).
    *   Defaulting of `p_wiederanlaufWert` will use `IF p_wiederanlaufWert IS NULL THEN SET p_wiederanlaufWert = '0'; END IF;`.
*   **Environment Setup:**
    *   Sourcing `$HOME/.dw_init` and other utility scripts will be replaced. Global configurations will be managed via BigQuery dataset/table settings, procedure parameters, or configuration tables.
*   **Validation:**
    *   `pruefeParameterGesetzt` calls will be replaced by BigQuery `IF` statements checking for `NULL` or empty string values in the procedure's input parameters. Failure will result in `RAISE USING MESSAGE` or an `INSERT` into `job_error_log`.
    *   `DWDate_Datum_Check` will be replaced by `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`. A `NULL` result from `SAFE.PARSE_DATE` indicates an invalid format, triggering an error log entry and `RAISE`.
*   **Date Derivation:**
    *   The execution of `gestern.ksh` will be replaced by direct BigQuery date functions: `SET v_datum_heute = CURRENT_DATE(); SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);`.
*   **SQL Script Execution:**
    *   The `starteSQLSkript` call, which executes `d_ausd_bp_ta_bpr_basis.sql`, will be replaced by the direct execution of the migrated BigQuery SQL from `d_ausd_bp_ta_bpr_basis.sql`. This can be inline or by calling another BigQuery Stored Procedure.
*   **Temporary File Record Count:**
    *   Reading `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis.tmp` will be replaced by using a BigQuery `DECLARE` variable (`v_records INT64`) and populating it with a `SELECT COUNT(*)` query against the target table after the data transformations.
*   **Job Logging:**
    *   The commented-out `FOSJobErzeugeEintrag` will be implemented as an `INSERT` statement into the `project.dataset.job_table`, capturing the relevant metadata and `v_records`.
*   **Error Handling:**
    *   `DWMSG_MeldeFehler` will be replaced by `INSERT` statements into `project.dataset.job_error_log` within `IF` blocks or `EXCEPTION` handlers, followed by `RAISE USING MESSAGE` to halt execution.
*   **Commented-out Post-processing:** The `sed`, `sort`, `join` operations are currently inactive. If they need to be re-introduced, they would be migrated to BigQuery SQL transformations using `REPLACE`, `ORDER BY`, `GROUP BY`, `JOIN`, or `MERGE` statements on staging tables or data within Cloud Storage ingested to BigQuery.

## 6. External Dependencies

**Identified Dependencies from Source Script:**

*   **Sourced KornShell Utility Scripts:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    *   **Replacement:** These functionalities will be absorbed by BigQuery's native SQL scripting features, `ASSERT` statements, BigQuery UDFs (if complex logic warrants), or embedded into the main stored procedure. Environment variables will become procedure parameters or values retrieved from BigQuery configuration tables.
*   **External KornShell Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
    *   **Replacement:** Replaced by BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB(..., INTERVAL 1 DAY)`).
*   **Core SQL Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_basis.sql`
    *   **Replacement:** This script is critical and contains the main business logic. It must be migrated to native BigQuery SQL. This migrated SQL will be directly executed within the new BigQuery Stored Procedure or encapsulated in its own dedicated BigQuery Stored Procedure.
*   **Temporary File:**
    *   `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis.tmp`
    *   **Replacement:** Replaced by a BigQuery `DECLARE` variable to hold the record count. If larger intermediate results were to be stored, BigQuery temporary tables would be used.
*   **Database Connection (Implicit via `h_alis_sqlplus.ksh` and `starteSQLSkript`):**
    *   **Replacement:** BigQuery stored procedures execute directly within the BigQuery engine, eliminating the need for external database client wrappers like SQL*Plus.
*   **Other External Systems:** No other external systems (e.g., Oracle, SFTP, S3) were explicitly identified as direct dependencies for this specific KornShell script from the lineage data.

## 7. Unresolved / Risks

*   **Core SQL Logic Complexity (`d_ausd_bp_ta_bpr_basis.sql`):** The primary risk and unresolved item is the specific content and complexity of `d_ausd_bp_ta_bpr_basis.sql`. Its migration to BigQuery SQL is paramount and will dictate a significant portion of the overall effort and potential re-design. Without this content, the full end-to-end data flow cannot be precisely mapped.
*   **Commented-Out Functionality:** The `sed`, `sort`, `join` operations are commented out. A decision needs to be made if this functionality is still required. If so, it represents an additional scope for BigQuery data transformation.
*   **Missing Complexity/Automation Data:** The absence of `file_complexity` and `automation_rate` data means the effort and automation level are estimates. A manual assessment or a detailed analysis of `d_ausd_bp_ta_bpr_basis.sql` will be needed to refine these.
*   **Full `starteSQLSkript` Behavior:** The exact implementation details of `starteSQLSkript` are not fully known, especially how it handles SQL script execution and parameter binding. The migration will assume direct BigQuery SQL execution with parameters passed into the stored procedure.
*   **Job Framework (`FOSJobDeaktivate`, `h_alis_job.ksh`):** The commented-out references to a job framework (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`, `h_alis_job.ksh`) suggest a larger job control system. The migration path should consider if these job control features need to be replicated in BigQuery (e.g., in a dedicated job status table or via Cloud Composer's native tracking) or if the basic job logging proposed is sufficient.

## 8. Build Plan

1.  **Analyze and Migrate Core SQL Script (Language: BigQuery SQL):**
    *   Obtain the content of `d_ausd_bp_ta_bpr_basis.sql`.
    *   Analyze its tables, columns, joins, transformations, and DML operations.
    *   Design and implement `d_ausd_bp_ta_bpr_basis_bq.sql` (or a BigQuery Stored Procedure) in BigQuery SQL, optimizing for BigQuery performance. This is the critical path.
2.  **Create BigQuery Logging Tables (Language: BigQuery DDL):**
    *   Define and create `project.dataset.job_error_log` table (e.g., `job_name STRING, entry_nr STRING, stichtag STRING, error_message STRING, created_at TIMESTAMP`).
    *   Define and create `project.dataset.job_table` table (e.g., `job_name STRING, status_a STRING, status_i STRING, start_date DATE, end_date DATE, job_type STRING, restart_flag STRING, record_count INT64, description STRING, job_kennung STRING, eintrags_nr STRING, stichtag STRING, wiederanlaufwert STRING, created_at TIMESTAMP`).
3.  **Develop Main Orchestration Stored Procedure (Language: BigQuery SQL):**
    *   Create the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_basis` based on the pseudocode provided in the MCP analysis.
    *   Integrate the migrated `d_ausd_bp_ta_bpr_basis_bq.sql` logic.
    *   Implement parameter validation, date checks, date derivation, record counting, and job/error logging.
4.  **Unit Testing (Language: BigQuery SQL / Python):**
    *   Write unit tests for the `r_ausd_bp_ta_bpr_basis` stored procedure, covering various parameter combinations, valid/invalid dates, and error conditions.
    *   Test the core SQL logic from `d_ausd_bp_ta_bpr_basis_bq.sql` independently.
5.  **Integration Testing (Language: Python / Cloud Composer):**
    *   If using Cloud Composer, develop and test the Airflow DAG to invoke the BigQuery Stored Procedure, ensuring correct parameter passing and execution flow.
6.  **Deployment:**
    *   Deploy DDL for logging tables.
    *   Deploy the BigQuery Stored Procedure(s).
    *   Deploy Cloud Composer DAG (if applicable).