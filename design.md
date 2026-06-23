# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

## 1. Purpose & Scope
This migration job focuses on `k_ausd_v_ta_cntrct_templ.ksh`, a KornShell control script. Its primary purpose is to orchestrate a data preparation process that likely updates the `ta_cntrct_templ` table. The script handles environment setup, parses command-line parameters, includes error handling, and executes an external SQL script (`d_ausd_v_ta_cntrct_templ.sql`). It also reads the number of processed records from a temporary file and provides completion messages. The overall scope is to replicate this orchestration and data update process on the Google Cloud Platform, specifically utilizing BigQuery.

## 2. Source Inventory
The job is primarily composed of a single KornShell script, categorized as a `shell` script and identified as `KornShell` tool. Its complexity tier is `medium`, and it is assessed for `semi_auto` migration.

*   **Main Script:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`
    *   **Technology:** KornShell (shell)
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Control script for `r_ausd_vertrag.ksh`, responsible for environment setup, parameter parsing, error handling, and orchestrating the execution of an SQL script (`d_ausd_v_ta_cntrct_templ.sql`) which likely updates the `ta_cntrct_templ` table.

*   **Dependencies (identified from source code analysis):**
    *   Shell Environment: `$HOME/.dw_init`
    *   Helper Scripts (KornShell):
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error handling)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date utilities)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus related routines, implying Oracle interaction)
    *   External SQL Script: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql`
    *   Temporary Filesystem: `$DW_DIR_UTL` for temporary record count storage.

## 3. Target Architecture
The target architecture will leverage BigQuery for data processing and storage, with BigQuery Stored Procedures handling the orchestration logic currently residing in the KornShell script.

*   **Orchestration Logic:** The parameter parsing, validation, error handling, and sequential execution flow of `k_ausd_v_ta_cntrct_templ.ksh` will be migrated into a BigQuery Stored Procedure. This procedure will accept input parameters equivalent to the shell script's command-line arguments.
*   **Data Transformation:** The core SQL logic from `d_ausd_v_ta_cntrct_templ.sql` will be translated into BigQuery SQL (DML/DDL) and either inlined within the BigQuery Stored Procedure or called as a separate BigQuery script from within the procedure using `EXECUTE IMMEDIATE`.
*   **Table Storage:** The `ta_cntrct_templ` table (and any other tables involved in `d_ausd_v_ta_cntrct_templ.sql`) will be created and managed within BigQuery datasets.
*   **Job Monitoring/Auditing:** A dedicated BigQuery audit table (`project.dataset.job_audit` in the pseudocode) will replace any implicit job table updates from the legacy system. This table will track job status, start/end times, and record counts.
*   **Temporary Data:** File-based temporary storage (`tmpFile`) will be replaced by BigQuery variables (`DECLARE v_records INT64;`) or by directly querying the BigQuery tables for record counts.
*   **External Orchestration:** For scheduling and triggering the BigQuery Stored Procedure, Google Cloud Composer (Airflow DAG) or Cloud Workflows can be used to manage the end-to-end execution, including parameter passing.

## 4. Data Flow & Lineage
The original lineage analysis did not yield specific edge details, so the data flow is constructed from static analysis and the MCP output.

1.  **Input Parameters:** `p_JobKennung` and `p_EintragsNr` are provided to the orchestrating BigQuery Stored Procedure.
2.  **Orchestration (BQ Stored Procedure):**
    *   Initializes variables and potentially logs a job start entry into `project.dataset.job_audit`.
    *   Performs parameter validation.
    *   If validation passes, it executes the migrated BigQuery SQL derived from `d_ausd_v_ta_cntrct_templ.sql`. This SQL is expected to read from source tables (unknown) and write/update the `ta_cntrct_templ` table.
    *   Determines the number of processed records (e.g., via `SELECT COUNT(*)` on the target table or by capturing DML affected rows).
    *   Logs job completion and record count into `project.dataset.job_audit`.
3.  **Target Table:** `ta_cntrct_templ` in BigQuery is updated/populated.
4.  **Logging/Monitoring:** `project.dataset.job_audit` captures the execution metadata.
5.  **Output:** Completion status and record count are implicitly available via the stored procedure's execution status or by querying the audit table.

## 5. Transformation Logic
The transformation will involve converting the shell script's procedural logic and the external SQL script's DML/DDL into BigQuery SQL and stored procedure constructs.

*   **Parameter Handling:** The `getopts` logic for `j:` and `f:` will be replaced by `IN` parameters in the BigQuery Stored Procedure (`p_JobKennung STRING`, `p_EintragsNr STRING`).
*   **Environment Sourcing (`. $HOME/.dw_init`):** This will be replaced by explicitly declared variables within the stored procedure, using project/dataset variables, or by configuration passed by the orchestration layer (e.g., Airflow variables).
*   **Helper Script Inclusion (`. ksh` files):** The functionality of helper scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` will need to be re-implemented in BigQuery SQL procedural language or as separate BigQuery functions/procedures.
    *   `pruefeParameterGesetzt` will translate to `IF ... THEN RAISE ... END IF` blocks.
    *   `DWMSG_MeldeFehler` will translate to BigQuery error handling (`RAISE USING MESSAGE`) or logging inserts.
    *   `starteSQLSkript` will be replaced by `EXECUTE IMMEDIATE` for the migrated `d_ausd_v_ta_cntrct_templ.sql` content.
*   **Error Handling:** The `set +e`, `set -eu`, `if [ ! $ErrNr -eq 0 ]` blocks will be replaced with BigQuery's `EXCEPTION HANDLER` or `IF/THEN/ELSE` with `RAISE` statements.
*   **SQL Script Execution:** The content of `d_ausd_v_ta_cntrct_templ.sql` will be migrated from its current SQL dialect (likely Oracle-compatible given `h_alis_sqlplus.ksh`) to BigQuery SQL syntax. This is the core data transformation component.
*   **Temporary File Reading (`cat $tmpFile`):** This will be replaced by a direct variable assignment after a `SELECT COUNT(*)` or a similar aggregation within the BigQuery Stored Procedure. The `eval` command will not be necessary.
*   **Table Name Definition:** `v_TabName='ta_cntrct_templ'` will translate to a `DECLARE v_TabName STRING DEFAULT 'ta_cntrct_templ';` within the BigQuery Stored Procedure.

## 6. External Dependencies
The original script has no explicitly defined external systems in the lineage metadata. However, the code suggests implicit dependencies:

*   **Filesystem Paths:** Reliance on `$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL` for scripts and temporary files.
    *   **Replacement:** These paths will be replaced by BigQuery dataset and table references, or by runtime parameters/variables in the orchestration layer for configuration values. Temporary file usage will be replaced by BigQuery in-memory variables or audit tables.
*   **SQL*Plus/Oracle:** The presence of `h_alis_sqlplus.ksh` and the general context suggests interaction with an Oracle database.
    *   **Replacement:** All database interactions will be directly against BigQuery. The `d_ausd_v_ta_cntrct_templ.sql` content will be re-written for BigQuery SQL syntax.

## 7. Unresolved / Risks
*   **Content of `d_ausd_v_ta_cntrct_templ.sql`:** This is a critical dependency. The actual SQL logic within this file is unknown and its complexity (e.g., use of Oracle-specific functions, complex joins, DDL/DML mix) will dictate the effort required for BigQuery translation.
*   **Full Logic of Sourced Helper Scripts:** While their purpose is identified, the exact logic within `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` needs to be fully understood to ensure complete and accurate re-implementation in BigQuery.
*   **Job-Table Management:** The `purpose_note` and MCP output mention "Eintrag in die Job-Tabelle" and "Job-table related behavior". The specific table structure and update logic are implicit in the sourced scripts and need clarification for the BigQuery audit table design.
*   **Oracle-Specific SQL:** If `d_ausd_v_ta_cntrct_templ.sql` contains highly specialized Oracle SQL constructs, their conversion to BigQuery SQL might be complex.
*   **Idempotency:** Ensure the migrated BigQuery stored procedure and SQL are idempotent, especially considering the "aktive Jobs werden ignoriert" comment in the original script.

## 8. Build Plan
1.  **Define BigQuery Schema:**
    *   Create `ta_cntrct_templ` table (DDL in BigQuery SQL).
    *   Create `job_audit` table (DDL in BigQuery SQL) to log job executions.
2.  **Translate SQL Script:**
    *   Migrate `d_ausd_v_ta_cntrct_templ.sql` content to BigQuery SQL syntax. This will be the core DML/DDL that operates on `ta_cntrct_templ`.
3.  **Develop BigQuery Stored Procedure:**
    *   Create `k_ausd_v_ta_cntrct_templ_sp` (BigQuery SQL) that encapsulates:
        *   Parameter definitions (`p_JobKennung`, `p_EintragsNr`).
        *   Parameter validation logic.
        *   Integration of `job_audit` logging.
        *   `EXECUTE IMMEDIATE` call for the migrated `d_ausd_v_ta_cntrct_templ.sql` logic.
        *   Record counting logic (e.g., `SELECT COUNT(*)`).
        *   BigQuery SQL implementation for common helper functions (error reporting, date handling if needed).
4.  **Develop Orchestration (Optional but Recommended for Production):**
    *   Create a Cloud Composer (Airflow) DAG (Python) or Cloud Workflow definition (YAML) to:
        *   Trigger the `k_ausd_v_ta_cntrct_templ_sp` BigQuery Stored Procedure.
        *   Pass `p_JobKennung` and `p_EintragsNr` parameters.
        *   Monitor execution and handle retries.