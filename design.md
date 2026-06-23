# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh

## 1. Purpose & Scope

This job, `k_ausd_v_ta_barrier_zusgf.ksh`, is a KornShell control script acting as an orchestrator. Its primary purpose is to manage the execution of a related SQL script, `d_ausd_v_ta_barrier_zusgf.sql`, within a broader data processing workflow. The script handles job-specific parameters, checks for active job instances to avoid conflicts, calls the SQL script for data processing, records job execution in a job table, and deactivates older active jobs. It also captures the number of records processed by the SQL script.

The scope of this migration design is to translate the functionality of this KornShell orchestrator and its invoked SQL script to Google Cloud's BigQuery platform.

## 2. Source Inventory

The primary component of this job is a KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh`
    *   **Technology:** KornShell
    *   **Category:** Shell Script
    *   **Complexity Tier:** Medium (inferred from `lineage_assembled_jobs` stage distribution)
    *   **Automation Bucket:** Semi-Auto (B2)
    *   **Purpose:** Orchestration, parameter parsing, job control, SQL script invocation.

This shell script invokes an SQL script:
*   **File:** `d_ausd_v_ta_barrier_zusgf.sql`
    *   **Technology:** SQL (likely Oracle PL/SQL based on context)
    *   **Purpose:** Contains the core data transformation and manipulation logic. This script reads from and writes to several database tables.

## 3. Target Architecture

The target architecture will leverage BigQuery's capabilities for both orchestration and data processing.

*   **Orchestration:** The `k_ausd_v_ta_barrier_zusgf.ksh` shell script will be migrated to a **BigQuery Stored Procedure**. This procedure will handle parameter parsing, validation, job control logic, and the invocation of the data processing logic.
*   **Data Processing:** The SQL logic within `d_ausd_v_ta_barrier_zusgf.sql` will be translated into **BigQuery SQL**. This can either be inlined directly within the orchestrating BigQuery Stored Procedure or implemented as a separate BigQuery Stored Procedure, depending on its complexity and reusability.
*   **Tables:** All source database tables (`DWTK_MELDUNGEN`, `TABLE`, `SOF$TA_BARRIER`, `R_BAR`, `SOF$TA_BARRIER_ZUSGF`, `VIA`) will be migrated to **BigQuery Tables**. New control tables for job management (`job_control`, `job_error_log`) will be created in BigQuery.
*   **Scheduling:** Orchestration of the BigQuery Stored Procedure would typically be managed by a service like **Cloud Composer (Airflow)** or BigQuery's **Scheduled Queries**.

## 4. Data Flow & Lineage

The overall job flow is as follows:

1.  **`r_ausd_v_ta_barrier_zusgf.ksh` (Parent Script):** This external KornShell script (INVOKES) the `k_ausd_v_ta_barrier_zusgf.ksh` job. This indicates a higher-level orchestration outside the immediate scope of this migration but highlights the need for the migrated BigQuery stored procedure to accept parameters and potentially return status for integration.
2.  **`k_ausd_v_ta_barrier_zusgf.ksh` (Current Job - Orchestrator):**
    *   Receives parameters (`p_JobKennung`, `p_EintragsNr`).
    *   Loads environment and utility functions from various KSH scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Validates parameters.
    *   Manages job status (active/deactive) potentially by interacting with a job control mechanism.
    *   **EXECUTES_SQL** `d_ausd_v_ta_barrier_zusgf.sql`.
    *   Reads a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_barrier_zusgf_$$.tmp`) to get the record count.
3.  **`d_ausd_v_ta_barrier_zusgf.sql` (Data Processor):**
    *   **READS_TABLE:** `DWTK_MELDUNGEN`, `TABLE` (generic, needs clarification), `SOF$TA_BARRIER`.
    *   **WRITES_TABLE:** `R_BAR`, `SOF$TA_BARRIER_ZUSGF`, `VIA`.
    *   **DEFINES_PACKAGE:** `SOF$SP_TABLE_FUNCTIONS`.
    *   **USES_PACKAGE:** `DWPA_UTIL_SKRIPT`, `SP_TABLE_FUNCTIONS`, `PA_ANALYZE`.

## 5. Transformation Logic

The KornShell script itself primarily performs orchestration, error handling, and parameter management. The core data transformations reside within the `d_ausd_v_ta_barrier_zusgf.sql` script.

**KornShell Logic (migrated to BigQuery Stored Procedure):**

*   **Parameter Parsing:** The `getopts` logic for `j:` (p_JobKennung) and `f:` (p_EintragsNr) will be directly mapped to `IN` parameters of the BigQuery Stored Procedure.
*   **Parameter Validation:** `pruefeParameterGesetzt` calls will be replaced with `IF param IS NULL OR param = '' THEN` checks within the BigQuery Stored Procedure.
*   **Error Handling:** `DWMSG_MeldeFehler` and `exit $ErrNr` will be replaced with inserts into a BigQuery `job_error_log` table and `RAISE USING MESSAGE` statements.
*   **Job Control:** Logic to ignore active jobs, register job execution, and deactivate old jobs will be implemented using `INSERT` and `UPDATE` statements on a BigQuery `job_control` table.
*   **SQL Script Invocation:** The `starteSQLSkript` function call will be replaced by a direct `CALL` to the migrated BigQuery Stored Procedure corresponding to `d_ausd_v_ta_barrier_zusgf.sql` or by inlining the SQL logic using `EXECUTE IMMEDIATE`.
*   **Record Count:** The reading of `$tmpFile` will be replaced by querying the target table(s) (`SOF$TA_BARRIER_ZUSGF` or other relevant output tables) to `COUNT(*)` the affected records and updating the `job_control` table.

**SQL Script Logic (from `d_ausd_v_ta_barrier_zusgf.sql`):**

The actual transformation logic for generating `R_BAR`, `SOF$TA_BARRIER_ZUSGF`, and `VIA` from `DWTK_MELDUNGEN`, `TABLE`, and `SOF$TA_BARRIER` will need to be analyzed and converted from its original SQL dialect (likely Oracle PL/SQL) to BigQuery SQL. This will involve:
*   Translating DDL (CREATE TABLE, INSERT INTO) to BigQuery equivalents.
*   Converting Oracle-specific functions, data types, and syntax (e.g., `DECODE`, `NVL`, `ROWNUM`, `sysdate`) to their BigQuery counterparts.
*   Migrating `PACKAGE` definitions and usage (`SOF$SP_TABLE_FUNCTIONS`, `DWPA_UTIL_SKRIPT`, `PA_ANALYZE`) to BigQuery Stored Procedures or user-defined functions (UDFs) if they contain reusable logic.

## 6. External Dependencies

*   **Sourced Utility Scripts:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    *   **Replacement:** These shell-based environment and utility functions will be replaced by BigQuery Stored Procedure parameters, session variables, or dedicated utility procedures/UDFs within BigQuery. Error logging and parameter parsing will be re-implemented directly in BigQuery SQL.
*   **Temporary File System:** `$DW_DIR_UTL/bert_k_ausd_v_ta_barrier_zusgf_$$.tmp`
    *   **Replacement:** The temporary file for record counting will be replaced by writing the record count directly to a BigQuery control table or returning it as an `OUT` parameter from the data processing stored procedure.
*   **SQL*Plus Wrapper:** The `h_alis_sqlplus.ksh` and `starteSQLSkript` imply the use of `sqlplus`.
    *   **Replacement:** This will be fully absorbed by the BigQuery Stored Procedure environment. The direct execution of SQL within BigQuery's engine negates the need for an external SQL client wrapper.
*   **Database Packages:** `DWPA_UTIL_SKRIPT`, `SOF$SP_TABLE_FUNCTIONS`, `PA_ANALYZE`.
    *   **Replacement:** These packages, if they contain essential logic, will need to be analyzed and migrated to BigQuery Stored Procedures or UDFs. If they are simple wrappers for common SQL operations, their functionality might be directly integrated.

## 7. Unresolved / Risks

*   **Generic `TABLE` in `READS_TABLE`:** The `lineage_edges` indicates `d_ausd_v_ta_barrier_zusgf.sql` reads from `TABLE:TABLE`. This is overly generic and needs clarification. The actual table name(s) must be identified for accurate migration.
*   **Oracle-specific SQL:** The SQL script `d_ausd_v_ta_barrier_zusgf.sql` likely contains Oracle-specific syntax (PL/SQL, functions, data types). A detailed code analysis and manual conversion effort will be required to translate this to BigQuery SQL, addressing potential functional differences.
*   **Shell Environment Complexity:** While the MCP tool outlined many replacements, any complex environment variable dependencies or dynamic path constructions within the sourced utility scripts might require careful re-engineering in BigQuery or Cloud Composer.
*   **Business Logic in Utility Scripts:** If the sourced utility `.ksh` scripts contain critical business logic beyond simple utilities, those too would need to be migrated to BigQuery functions/procedures or Python (if using Cloud Composer for more complex orchestration).
*   **"Semi-Auto" (B2) Automation Bucket:** The semi-automatic designation indicates that some manual intervention or custom development will be necessary, aligning with the identified complexities of Oracle-to-BigQuery SQL conversion and shell environment replacement.

## 8. Build Plan

1.  **Define BigQuery Tables:**
    *   Create BigQuery tables for `DWTK_MELDUNGEN`, `SOF$TA_BARRIER`, `R_BAR`, `SOF$TA_BARRIER_ZUSGF`, `VIA`, and the actual table represented by `TABLE:TABLE`. (Language: BigQuery DDL)
    *   Create `job_control` table to track job execution status, parameters, and record counts. (Language: BigQuery DDL)
    *   Create `job_error_log` table for error reporting. (Language: BigQuery DDL)
2.  **Migrate SQL Logic:**
    *   Convert `d_ausd_v_ta_barrier_zusgf.sql` into a BigQuery Stored Procedure, e.g., `p_d_ausd_v_ta_barrier_zusgf`. This will involve translating Oracle-specific SQL/PLSQL to BigQuery SQL. (Language: BigQuery SQL)
    *   Migrate essential logic from any `USES_PACKAGE` or `DEFINES_PACKAGE` (e.g., `DWPA_UTIL_SKRIPT`, `SOF$SP_TABLE_FUNCTIONS`, `PA_ANALYZE`) into BigQuery Stored Procedures or UDFs as needed. (Language: BigQuery SQL)
3.  **Migrate Orchestration Logic:**
    *   Develop a BigQuery Stored Procedure, e.g., `p_k_ausd_v_ta_barrier_zusgf`, to replace `k_ausd_v_ta_barrier_zusgf.ksh`. This procedure will:
        *   Accept `p_JobKennung` and `p_EintragsNr` as input parameters.
        *   Implement parameter validation.
        *   Manage job status in `job_control` table.
        *   Call `p_d_ausd_v_ta_barrier_zusgf` for data processing.
        *   Update `job_control` with record counts and final status.
        *   Log errors to `job_error_log`.
        (Language: BigQuery SQL)
4.  **Integration with Parent Workflow (Optional, if `r_ausd_v_ta_barrier_zusgf.ksh` is also migrated):**
    *   If the invoking `r_ausd_v_ta_barrier_zusgf.ksh` is also migrated, ensure the new BigQuery stored procedure integrates correctly (e.g., through Cloud Composer DAG or another BigQuery stored procedure).
5.  **Scheduling:**
    *   Configure BigQuery Scheduled Queries or a Cloud Composer DAG to execute the `p_k_ausd_v_ta_barrier_zusgf` BigQuery Stored Procedure as per the original schedule. (Language: JSON/YAML for BigQuery Scheduled Queries, Python for Cloud Composer DAG)