# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_bpr_opt_text.ksh`, is a control script designed to orchestrate a data processing workflow. Its primary purpose is to validate parameters, perform date checks, execute an SQL script (`d_ausd_bp_ta_bpr_opt_text.sql`) to process data, and log record counts. It prepares and runs a database extraction for a logical table/process named `PoolBasisprodukt`. The job is assembled from a single component and is categorized as having a 'medium' complexity stage distribution.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`
    *   **Technology:** KornShell script
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Control script that validates parameters, checks dates, and orchestrates the execution of an SQL script (d_ausd_bp_ta_bpr_opt_text.sql) to process data, logging record counts.
    *   **Content Summary:** The script loads environment variables, includes several utility KornShell scripts for error handling, date validation, parameter parsing, and SQL*Plus execution. It parses command-line arguments for job identification, entry number, cutoff date (Stichtag), and restart value. It validates these parameters and the date format. Finally, it executes an external SQL script, `d_ausd_bp_ta_bpr_opt_text.sql`, passing various parameters, and then logs the number of records processed. There are commented-out sections related to file processing (`sed`, `sort`, `join`) which are considered inactive legacy logic.

*   **Key Dependent Files / Components:**
    *   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_opt_text.sql`: The primary SQL script executed by the KornShell wrapper. This script performs the core data processing logic.
    *   `$HOME/.dw_init`: Environment initialization script.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date validation utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus execution utility.
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Date helper script (gets yesterday's date).

## 3. Target Architecture

The target architecture will leverage Google BigQuery for data processing and orchestration. The KornShell script's control flow and the embedded SQL logic will be migrated into a BigQuery Stored Procedure.

*   **BigQuery Stored Procedure:** A BigQuery Stored Procedure, likely named `project.dataset.r_ausd_bp_ta_bpr_opt_text`, will encapsulate the entire logic of the original KornShell script and its embedded SQL.
    *   It will accept parameters equivalent to the original command-line arguments (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    *   It will handle parameter validation and date format checks internally.
    *   The logic from `d_ausd_bp_ta_bpr_opt_text.sql` will be either inlined or called as a separate, nested BigQuery Stored Procedure.
    *   Error handling will be managed using BigQuery's `ASSERT` statements or `BEGIN...EXCEPTION` blocks.
    *   Logging will be redirected to a dedicated BigQuery logging table.
*   **BigQuery Logging Table:** A table named `project.dataset.job_log` (or similar) will store execution logs, status, error codes, and record counts, replacing the file-based temp output and job-table entry creation.
*   **Target Tables:** The tables read from and written to by `d_ausd_bp_ta_bpr_opt_text.sql` will be migrated to BigQuery. For instance, `SOF$TA_BPR_OPT_TEXT` will become `project.dataset.SOF_TA_BPR_OPT_TEXT`.
*   **Orchestration:** The invocation of this BigQuery Stored Procedure will be handled by a modern orchestrator like Cloud Composer (Airflow), replacing the `r_ausd_bp_ta_bpr_opt_text.ksh` wrapper script.

## 4. Data Flow & Lineage

The original job starts with `r_ausd_bp_ta_bpr_opt_text.ksh` invoking `k_ausd_bp_ta_bpr_opt_text.ksh`.

**Original Flow:**
1.  **`r_ausd_bp_ta_bpr_opt_text.ksh` (Invoker)**: Calls `k_ausd_bp_ta_bpr_opt_text.ksh`.
2.  **`k_ausd_bp_ta_bpr_opt_text.ksh` (Orchestrator)**:
    *   Loads environment and utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Parses and validates input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    *   Determines `p_datum_heute` and `p_datum_gestern` using `gestern.ksh`.
    *   Executes `d_ausd_bp_ta_bpr_opt_text.sql` via `h_alis_sqlplus.ksh`, passing parameters.
    *   Reads record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_opt_text.tmp`).
    *   (Commented out) Inserts an entry into a job table.
3.  **`d_ausd_bp_ta_bpr_opt_text.sql` (Data Processor)**:
    *   Reads data from `TABLE:DWTK_MELDUNGEN`.
    *   Reads data from `TABLE:SOF$TA_BPR_OPTIONEN`.
    *   Writes processed data to `TABLE:SOF$TA_BPR_OPT_TEXT`.
    *   Uses `PACKAGE:DWPA_UTIL_SKRIPT`.

**Migrated Flow (BigQuery Stored Procedure):**
1.  **Cloud Composer (Airflow) DAG:** Triggers the BigQuery Stored Procedure.
2.  **`project.dataset.r_ausd_bp_ta_bpr_opt_text` (BigQuery Stored Procedure):**
    *   Receives input parameters directly.
    *   Performs parameter and date validation using BQ SQL constructs (`IF`, `ASSERT`, `PARSE_DATE`, `REGEXP_CONTAINS`).
    *   Derives `v_datum_heute` and `v_datum_gestern` using BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`).
    *   Executes the core data processing logic (migrated from `d_ausd_bp_ta_bpr_opt_text.sql`) directly within the stored procedure or by calling a sub-procedure.
    *   Calculates record counts using `COUNT(*)` queries on BigQuery tables.
    *   Logs execution details, status, and record counts to `project.dataset.job_log`.

## 5. Transformation Logic

The transformation logic will involve converting the KornShell script's control flow and the Oracle SQL script's DML/DQL into BigQuery SQL, primarily within a Stored Procedure.

*   **Parameter Handling:**
    *   Original: `getopts` for command-line parameters.
    *   Target: Stored Procedure input parameters (`IN p_JobKennung STRING, IN p_EintragsNr STRING, IN p_Stichtag STRING, IN p_wiederanlaufWert STRING`).
*   **Parameter Validation:**
    *   Original: Shell `if` conditions, calls to `pruefeParameterGesetzt`, and error handling via `f_alis_msgerr.ksh`.
    *   Target: BigQuery `IF` statements and `ASSERT` clauses to check for `NULL` or empty strings. Error messages will be logged to `job_log` table, and `RAISE` will be used for termination.
*   **Date Validation:**
    *   Original: `DWDate_Datum_Check` and `REGEXP_CONTAINS` in shell.
    *   Target: `REGEXP_CONTAINS(p_Stichtag, r'^\\d{8}$')` and `PARSE_DATE('%d%m%Y', p_Stichtag)` for format validation and conversion.
*   **Date Derivation:**
    *   Original: `gestern.ksh` script to get yesterday's and today's dates.
    *   Target: BigQuery functions `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Execution:**
    *   Original: External `d_ausd_bp_ta_bpr_opt_text.sql` executed via `sqlplus`.
    *   Target: The logic within `d_ausd_bp_ta_bpr_opt_text.sql` will be directly incorporated into the BigQuery Stored Procedure using standard BigQuery DML/DQL. This will involve converting Oracle-specific syntax (if any) to BigQuery SQL.
*   **Record Counting:**
    *   Original: `cat $tmpFile` after SQL execution.
    *   Target: `SELECT COUNT(*) FROM `project.dataset.target_table` WHERE ...` within the Stored Procedure.
*   **Job Logging/Control:**
    *   Original: (Commented out) `FOSJobErzeugeEintrag` and `FOSJobDeaktivate` calls, implying a job control system.
    *   Target: `INSERT INTO project.dataset.job_log (...)` statements to track job status, parameters, and record counts.
*   **Utility Scripts:**
    *   `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`: These helper scripts will be rendered obsolete as their functionalities are absorbed into the BigQuery Stored Procedure logic using native BQ SQL features.

## 6. External Dependencies

The original environment relies on Oracle database objects and custom shell utilities.

*   **Oracle Database Tables:**
    *   `TABLE:DWTK_MELDUNGEN` (Read): Will be migrated to a BigQuery table, e.g., `project.dataset.DWTK_MELDUNGEN`.
    *   `TABLE:SOF$TA_BPR_OPTIONEN` (Read): Will be migrated to a BigQuery table, e.g., `project.dataset.SOF_TA_BPR_OPTIONEN`.
    *   `TABLE:SOF$TA_BPR_OPT_TEXT` (Write): Will be migrated to a BigQuery table, e.g., `project.dataset.SOF_TA_BPR_OPT_TEXT`.
    *   `TABLE:DUAL` (Read by `h_alis_date.ksh`): This is an Oracle-specific dummy table; its usage for date operations will be replaced by native BigQuery date functions.
*   **Oracle Database Procedures/Packages/Functions:**
    *   `PROCEDURE:SETZEZUSATZINFOS` (Called by `f_alis_msgerr.ksh`): The functionality of this procedure, if critical, will need to be re-implemented in BigQuery (e.g., as part of the logging mechanism or another BQ Stored Procedure). If it's purely for setting context for the original error system, it may be omitted.
    *   `PACKAGE:DWPA_UTIL_SKRIPT` (Used by `d_ausd_bp_ta_bpr_opt_text.sql`): Any functions or procedures within this package that are critical to `d_ausd_bp_ta_bpr_opt_text.sql`'s logic will need to be re-implemented as BigQuery User-Defined Functions (UDFs) or BigQuery Stored Procedures.
    *   `FUNCTION:DWMSG_ERMITTLENR` (Called by `f_alis_msgerr.ksh`, SENDS_MAIL): This function seems to be part of the error messaging system, potentially triggering emails. This functionality will be replaced by cloud-native alerting mechanisms (e.g., Cloud Monitoring alerts triggered by BigQuery job failures or specific log entries).
*   **Filesystem / Shell Utilities:**
    *   `$HOME/.dw_init`, `BERT_DIR_ROOT`, `DW_DIR_UTL`: These environment variables and directory structures will be replaced by BigQuery project/dataset structure, dataset default locations, or parameters within the BigQuery Stored Procedure.
    *   Temporary file `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_opt_text.tmp`: Replaced by `DECLARE` variables or temporary tables in BigQuery.

## 7. Unresolved / Risks

*   **Complexity of `d_ausd_bp_ta_bpr_opt_text.sql`:** The details of the SQL script are crucial. If it contains highly complex or proprietary Oracle-specific SQL, its migration to BigQuery might require significant effort, potentially impacting the `semi_auto` automation bucket. The `USES_PACKAGE:DWPA_UTIL_SKRIPT` also suggests potential complexity if this package contains extensive logic.
*   **Job Control System:** The commented-out FOS job management calls (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) indicate a legacy job control system. While direct replacement is planned with a BigQuery logging table and Cloud Composer, any remaining implicit dependencies or required integrations with the original job control system must be thoroughly investigated and addressed.
*   **Alerting/Messaging (`SENDS_MAIL`):** The `FUNCTION:DWMSG_ERMITTLENR` which `SENDS_MAIL` requires careful consideration. A direct replacement with Cloud Monitoring alerts or Cloud Functions for email notifications needs to be designed.
*   **Legacy Commented-Out Logic:** The `sed`, `sort`, `join` operations on `.dat` files are currently commented out. While treated as inactive, a review should confirm they are indeed no longer needed and will not be reactivated. If they were to become active, they would represent significant file-based processing that would need a different migration approach (e.g., Cloud Storage, Dataflow).

## 8. Build Plan

The migration will involve creating BigQuery DDL for tables and BigQuery SQL for the stored procedures.

1.  **Data Migration (ETL):** Migrate source Oracle tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_OPTIONEN`, `SOF$TA_BPR_OPT_TEXT`) to BigQuery. This is a prerequisite step.
2.  **DDL for BigQuery Logging Table:** Create `project.dataset.job_log` table definition.
    *   **Language:** BigQuery DDL
3.  **BigQuery Stored Procedure for `d_ausd_bp_ta_bpr_opt_text.sql` logic:**
    *   Translate the core DML/DQL from `d_ausd_bp_ta_bpr_opt_text.sql` into a BigQuery Stored Procedure, e.g., `project.dataset.p_bpr_opt_text_processing`.
    *   **Language:** BigQuery SQL
4.  **BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_opt_text.ksh` orchestration:**
    *   Create the main Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_opt_text` which incorporates parameter validation, date logic, calls the `p_bpr_opt_text_processing` (or inlines its logic), and performs logging.
    *   **Language:** BigQuery SQL
5.  **Cloud Composer (Airflow) DAG:**
    *   Develop an Airflow DAG to schedule and invoke the `project.dataset.r_ausd_bp_ta_bpr_opt_text` BigQuery Stored Procedure, passing the necessary runtime parameters.
    *   **Language:** Python
6.  **Alerting Configuration:**
    *   Set up Cloud Monitoring alerts for BigQuery job failures or specific log entries in `project.dataset.job_log` to replace the email functionality.
    *   **Language:** YAML/JSON (Cloud Monitoring configuration)