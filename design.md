# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh

## 1. Purpose & Scope

This job is a control script (`k_ausd_v_ta_vertrag_tmp.ksh`) designed to orchestrate the execution of an Oracle SQL script (`d_ausd_v_ta_vertrag_tmp.sql`). Its primary business purpose is to prepare and populate a temporary contract table (`sof$ta_vertrag_tmp`).

The control script handles:
*   Ignoring currently active jobs to prevent conflicts.
*   Parsing and validating input parameters.
*   Invoking the Oracle SQL script to perform data processing.
*   Updating a job table (inferred) with execution status.
*   Deactivating older active jobs.
*   Capturing and reporting the number of records processed by the SQL script.

The Oracle SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) is responsible for selecting, transforming, and inserting contract-related data from various source tables into the temporary target table `sof$ta_vertrag_tmp`.

The scope of this migration is to re-platform this entire workflow, including both the shell orchestration and the embedded SQL logic, to Google BigQuery.

## 2. Source Inventory

The job consists of two primary components:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh`
    *   **Technology:** KornShell Script
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** B0 (retire) - suggesting it's a candidate for significant redesign or decommissioning of its shell script nature.
    *   **Purpose:** ETL Orchestration, parameter handling, error control.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vertrag_tmp.sql`
    *   **Technology:** Oracle SQL
    *   **Complexity Tier:** Inferred as part of the job's overall 'medium' complexity.
    *   **Automation Bucket:** Inferred as part of the job's overall 'retire' migration bucket.
    *   **Purpose:** Data Transformation and Load into a temporary table.

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud Platform services, primarily BigQuery.

*   **Orchestration Layer:** The logic from `k_ausd_v_ta_vertrag_tmp.ksh` will be migrated into a **BigQuery Stored Procedure**. This procedure will handle parameter validation, error management, and the execution flow. For more complex scheduling and external dependencies, a Cloud Composer (Airflow) DAG could wrap the BigQuery Stored Procedure execution, though a standalone BigQuery Stored Procedure should suffice for the observed logic.
*   **Data Transformation Layer:** The core SQL logic from `d_ausd_v_ta_vertrag_tmp.sql` will be directly translated into **BigQuery Standard SQL**. This will be embedded within or called by the BigQuery Stored Procedure, likely populating a persistent BigQuery table that serves the same purpose as the original temporary Oracle table.
*   **Data Storage:** All source tables and the target `sof$ta_vertrag_tmp` will reside as managed tables within **BigQuery datasets**.
*   **Logging and Monitoring:** Error handling and job status logging will be implemented using BigQuery tables (e.g., `error_log`, `job_table`, `job_run_log`) and standard BigQuery monitoring tools.

## 4. Data Flow & Lineage

The current data flow starts with the KornShell script, which then delegates to the Oracle SQL script.

**Source Flow:**

1.  **`k_ausd_v_ta_vertrag_tmp.ksh` (KornShell)**:
    *   **Inputs:** Command-line parameters (`-j <JobKennung>`, `-f <EintragsNr>`).
    *   **Internal Dependencies (Sourced Scripts):**
        *   `$HOME/.dw_init` (environment initialization)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus wrapper, contains `starteSQLSkript`)
    *   **Action:** Invokes the Oracle SQL script `d_ausd_v_ta_vertrag_tmp.sql` via `starteSQLSkript`.
    *   **Outputs:**
        *   Writes record count to a temporary file: `$DW_DIR_UTL/bert_k_ausd_v_ta_vertrag_tmp_$$.tmp`.
        *   Prints status messages to console.

2.  **`d_ausd_v_ta_vertrag_tmp.sql` (Oracle SQL)**:
    *   **Inputs (Reads from Oracle Tables/Views):**
        *   `isbert_schema.dwtk_meldungen` (for `v_datum`)
        *   `sof$ta_cntrct_crs3`
        *   `sof$ta_bp_ref`
        *   `sof$ta_inv_acc`
        *   `dwh$vi_s_rd_segment`
        *   `sof$ta_notice`
        *   `sof$ta_barrier_zusgf`
        *   `sof$ta_cntrct_templ`
        *   `sof$ta_cntrct_valid`
        *   `sof$ta_period`
        *   `sof$ta_vvl_upgrade`
        *   `sof$ta_apn_ve`
        *   `sof$ta_action_assoc`
        *   `sof$vi_c_bfc`
    *   **Outputs (Writes to Oracle Table):**
        *   `sof$ta_vertrag_tmp` (truncate and insert)
    *   **External Calls:** Uses Oracle Package `isbert_schema.DWPA_UTIL_SKRIPT` for DDL operations (TRUNCATE). Also relies on other Oracle packages/functions like `CV`, `VVL`, `AP`, `RD`, `AC`, `BF`.

**Target Flow (BigQuery):**

1.  **`r_ausd_vertrag_control` (BigQuery Stored Procedure)**:
    *   **Inputs:** Parameters `p_JobKennung`, `p_EintragsNr` (equivalent to shell script arguments).
    *   **Action:**
        *   Validates parameters.
        *   (Optional) Updates `job_table` (e.g., sets `active_flag` for job control).
        *   Truncates `target_dataset.ta_vertrag_tmp`.
        *   Executes the translated BigQuery SQL logic (from `d_ausd_v_ta_vertrag_tmp.sql`) to populate `target_dataset.ta_vertrag_tmp`.
        *   Captures `record_count` internally.
        *   Logs job execution details and record count to `job_run_log` table.
        *   Logs errors to `error_log` table.
    *   **Outputs:** `target_dataset.ta_vertrag_tmp` populated; audit entries in `error_log` and `job_run_log`.

## 5. Transformation Logic

**5.1. KornShell Script (`k_ausd_v_ta_vertrag_tmp.ksh`) Migration:**

*   **Parameter Handling:** `getopts` logic will be replaced by `IN` parameters of the BigQuery Stored Procedure.
*   **Environment Variables:** `$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL` will be replaced by BigQuery Stored Procedure constants, variables, or configuration parameters passed during invocation.
*   **Helper Scripts:** The functionalities of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` will be re-implemented:
    *   Error handling will use BigQuery's `RAISE` statement, `ASSERT`, and logging to `error_log` table.
    *   Date functions will use BigQuery's native date/time functions.
    *   Parameter validation will be explicit `IF` conditions within the stored procedure.
    *   `starteSQLSkript` wrapper and SQL*Plus invocation will be replaced by direct BigQuery SQL execution within the stored procedure.
*   **Temporary File (`tmpFile`):** The mechanism of writing to and reading from a temporary file to capture record counts will be replaced by direct variable assignment within the BigQuery Stored Procedure, e.g., `SET v_records = (SELECT COUNT(*) FROM target_dataset.ta_vertrag_tmp);`.
*   **Job Control Logic:** The "ignore active jobs" and "deactivate older active jobs" logic needs to be translated into `UPDATE` or `MERGE` statements against a `job_table` in BigQuery.

**5.2. Oracle SQL Script (`d_ausd_v_ta_vertrag_tmp.sql`) Migration:**

*   **Temporary Table:** `sof$ta_vertrag_tmp` will be created as a permanent (or temporary, if applicable) BigQuery table in the target dataset (`target_dataset.ta_vertrag_tmp`).
*   **Oracle-Specific Syntax & Features:**
    *   `DEFINE`, `COLUMN new_value`, `START`, `SPOOL`, `WHENEVER SQLERROR CONTINUE/EXIT FAILURE`: These Oracle SQL*Plus commands will be removed or replaced by BigQuery Stored Procedure control flow (e.g., error handling with `BEGIN...EXCEPTION...END`).
    *   `/*+ parallel(...) */` hints: These will be removed as BigQuery automatically handles parallelism.
    *   Oracle Functions: `NVL` will be converted to `COALESCE`. `TO_CHAR(date, 'YYYYMMDD')` will be `FORMAT_DATE('%Y%m%d', date_column)`. `MONTHS_BETWEEN` will need to be re-implemented using `DATE_DIFF` or a combination of `EXTRACT` and arithmetic operations to achieve equivalent logic.
    *   `DECODE`: Will be converted to `CASE` statements in BigQuery.
    *   Oracle Packages (`isbert_schema.DWPA_UTIL_SKRIPT`, `CV`, `VVL`, `AP`, `RD`, `AC`, `BF`):
        *   `TRUNCATE TABLE` via `DWPA_UTIL_SKRIPT` will be a direct BigQuery `TRUNCATE TABLE` statement.
        *   Other package calls (e.g., `CV.cntrct_validity_id()`) will need to be analyzed individually. If they are simple functions, they can become BigQuery UDFs. If they represent complex business logic or views, their underlying SQL will be translated and integrated directly into the main `SELECT` statement or as BigQuery views.
*   **Data Types:** Oracle data types will be mapped to appropriate BigQuery data types.
*   **Query Logic:** The `INSERT INTO ... SELECT ... UNION ALL` statement with its complex joins and `WHERE` clauses will be directly translated into BigQuery Standard SQL, ensuring correct table aliases and join conditions are maintained. The two `UNION ALL` branches with different `c.cntrct_ty` conditions will be preserved.

## 6. External Dependencies

*   **Oracle Database:**
    *   **Current State:** All source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, `sof$ta_inv_acc`, `dwh$vi_s_rd_segment`, `sof$ta_notice`, `sof$ta_barrier_zusgf`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_vvl_upgrade`, `sof$ta_apn_ve`, `sof$ta_action_assoc`, `sof$vi_c_bfc`) and the target `sof$ta_vertrag_tmp` table, as well as the Oracle packages, reside in a legacy Oracle database.
    *   **Replacement Strategy:** All these Oracle objects must be migrated to BigQuery. This involves:
        1.  **Data Migration:** Extracting data from the Oracle tables and loading it into corresponding BigQuery tables.
        2.  **Schema Conversion:** Creating BigQuery schemas for all referenced tables and views, mapping Oracle data types to BigQuery data types.
        3.  **Package Translation:** Converting Oracle packages and custom functions into BigQuery UDFs or BigQuery Stored Procedure logic as described in Section 5.2.
*   **Legacy Filesystem/Shell Utilities:**
    *   **Current State:** The KornShell script relies on a local filesystem for sourcing helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) and for temporary file I/O (`$DW_DIR_UTL/bert_k_ausd_v_ta_vertrag_tmp_$$.tmp`).
    *   **Replacement Strategy:** This dependency will be eliminated. The functionality of these helper scripts will be absorbed into the BigQuery Stored Procedure or Python orchestration logic. Temporary file I/O will be replaced by in-memory variables or direct query results within BigQuery.

## 7. Unresolved / Risks

*   **Discrepancy in Lineage:** The automated lineage analysis did not identify the `READS`/`WRITES` for the `d_ausd_v_ta_vertrag_tmp.sql` file at its correct path (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vertrag_tmp.sql`). This was identified through manual inspection of the `ksh` script. This highlights a potential gap in automated lineage detection for deeply embedded or dynamically referenced SQL files.
*   **Helper Script Logic:** The detailed logic within the sourced KornShell helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is not fully known. Their functionalities need to be thoroughly understood and accurately translated into BigQuery (or Python) equivalents.
*   **`migration_bucket`: retire (B0):** The `retire` bucket for the `ksh` script indicates a low automation rate and a potential need for significant redesign. This suggests that a direct lift-and-shift might not be the most efficient or desired outcome. A thorough review with business stakeholders is recommended to confirm if the existing logic is still entirely relevant or if simplification/refactoring is possible.
*   **Oracle-Specific SQL:** The transformation of Oracle-specific SQL features (e.g., `MONTHS_BETWEEN`, complex `DECODE` to `CASE`, parallel hints, and package calls) requires careful manual review and testing to ensure functional equivalence and optimal performance in BigQuery.
*   **`v_datum` Calculation:** The logic to derive `v_datum` from `isbert_schema.dwtk_meldungen` requires ensuring the `dwtk_meldungen` table (or its BigQuery equivalent) has the necessary data and structure, including the `timecreated` and `job_kennung` columns.
*   **Data Volume and Performance:** The current Oracle query uses parallel hints. While BigQuery handles parallelism automatically, large data volumes or complex joins might require specific partitioning, clustering, or optimization strategies in BigQuery to maintain or improve performance.

## 8. Build Plan

1.  **BigQuery Schema Definition (BigQuery DDL):**
    *   Create BigQuery datasets (e.g., `isbert_dataset`, `sof_dataset`, `dwh_dataset`, `target_dataset`).
    *   Define BigQuery tables for all source Oracle tables/views referenced in `d_ausd_v_ta_vertrag_tmp.sql` (e.g., `isbert_dataset.dwtk_meldungen`, `sof_dataset.ta_cntrct_crs3`, `dwh_dataset.vi_s_rd_segment`, etc.).
    *   Define the target BigQuery table `target_dataset.ta_vertrag_tmp` with appropriate schema and data types.
    *   Define BigQuery tables for logging (`target_dataset.error_log`, `target_dataset.job_table`, `target_dataset.job_run_log`).

2.  **Oracle Data Migration (ETL Tool / Data Transfer Service):**
    *   Execute a one-time and/or recurring data transfer process (e.g., using Datastream, Dataflow, or a custom ETL script) to migrate data from the source Oracle tables into their newly created BigQuery counterparts.

3.  **BigQuery SQL Transformation Logic (BigQuery SQL):**
    *   Translate the `INSERT INTO ... SELECT ... UNION ALL` statement from `d_ausd_v_ta_vertrag_tmp.sql` into BigQuery Standard SQL, converting `DECODE` to `CASE`, Oracle functions to BigQuery equivalents, and addressing any Oracle-specific syntax.
    *   Implement any necessary BigQuery UDFs for complex Oracle package functions if they cannot be inlined.

4.  **BigQuery Stored Procedure for Orchestration (BigQuery SQL Scripting):**
    *   Create a BigQuery Stored Procedure (e.g., `target_dataset.sp_k_ausd_v_ta_vertrag_tmp`) that encapsulates the control logic from `k_ausd_v_ta_vertrag_tmp.ksh`.
    *   Implement parameter handling, validation, and error logging using BigQuery SQL scripting features.
    *   Embed the translated BigQuery SQL transformation logic (from step 3) within this stored procedure.
    *   Implement the job control logic (active/deactivate jobs) using `UPDATE` or `MERGE` statements on `target_dataset.job_table`.
    *   Include logic to capture record counts and log them to `target_dataset.job_run_log`.

5.  **Testing (SQL / Python):**
    *   Develop unit tests for the BigQuery SQL transformation logic to ensure data accuracy and functional equivalence with the Oracle version.
    *   Develop integration tests for the BigQuery Stored Procedure, validating parameter handling, job control, and data loading.

6.  **Deployment & Scheduling (gcloud CLI / Cloud Composer):**
    *   Deploy the BigQuery schemas, tables, UDFs, and Stored Procedures.
    *   Configure a scheduling mechanism (e.g., Cloud Scheduler to directly invoke the BigQuery Stored Procedure, or a Cloud Composer DAG for more complex orchestration if needed) to run the `target_dataset.sp_k_ausd_v_ta_vertrag_tmp` procedure.