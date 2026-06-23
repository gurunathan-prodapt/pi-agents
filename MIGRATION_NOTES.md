# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh`, which previously managed parameter validation, date checks, and the execution of an underlying Oracle SQL script (`d_ausd_bp_ta_rn_vertrag.sql`), has been migrated.

The job has been migrated to Google Cloud Platform, specifically to **BigQuery**. The orchestration logic is now encapsulated within a BigQuery Stored Procedure, and the core data transformation logic (from `d_ausd_bp_ta_rn_vertrag.sql`) is intended to be translated into native BigQuery SQL.

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **`bq_sp_r_ausd_bp_ta_rn_vertrag.sql`**
    *   **Role:** This file contains the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_rn_vertrag`. It serves as the primary entry point for the migrated job. Its responsibilities include:
        *   Parsing and validating input parameters (`p_job_id`, `p_date_today_str`, `p_date_yesterday_str`, `p_mandant`).
        *   Performing date format and logical validation (e.g., `p_date_yesterday_str` being one day before `p_date_today_str`).
        *   Orchestrating the execution of the core data transformation logic (which would be derived from `d_ausd_bp_ta_rn_vertrag.sql`).
        *   Handling error conditions and logging job status and errors to dedicated BigQuery audit and error log tables (`project.dataset.job_audit_log`, `project.dataset.job_error_log`).
        *   Capturing and logging processed record counts.
*   **`bq_d_ausd_bp_ta_rn_vertrag.sql`**
    *   **Role:** This file is a **placeholder** for the core data transformation logic. It represents the migrated content of the original `d_ausd_bp_ta_rn_vertrag.sql` Oracle script, translated into BigQuery SQL. Its exact content is currently unknown and needs to be populated based on the original Oracle script. Once populated, this SQL would typically be embedded directly within the `bq_sp_r_ausd_bp_ta_rn_vertrag` stored procedure or called as a separate nested stored procedure.

## 3. Key design decisions

*   **Orchestration Layer Migration:** The KornShell script's role as an orchestrator (parameter handling, validation, execution flow) was directly translated into a BigQuery Stored Procedure. This leverages BigQuery's scripting capabilities for control flow and error handling, eliminating the need for an external shell environment.
*   **Parameter Handling:** Command-line arguments (`getopts`) from the KornShell script were mapped directly to typed input parameters of the BigQuery Stored Procedure, ensuring clear input contracts.
*   **Date Validation and Derivation:** Shell-based date utilities (`DWDate_Datum_Check`, `gestern.ksh`) were replaced with native BigQuery SQL functions like `SAFE.PARSE_DATE`, `CURRENT_DATE()`, and `DATE_SUB()`, providing robust and efficient date manipulation within the database.
*   **Logging and Error Handling:** The custom shell error handling (`DWMSG_MeldeFehler`) and job bookkeeping were replaced by inserts into standardized BigQuery audit (`job_audit_log`) and error (`job_error_log`) tables. This centralizes logging within the data warehouse environment.
*   **Elimination of Temporary Files:** The use of temporary files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn.tmp`) for capturing record counts was replaced by BigQuery scalar variables (`v_processed_records`) and direct logging, removing file system dependencies.
*   **Core SQL Integration:** The `d_ausd_bp_ta_rn_vertrag.sql` content is intended to be fully translated into BigQuery SQL. The design allows for this SQL to be either embedded directly within the main stored procedure or encapsulated in a separate, nested BigQuery stored procedure for modularity.
*   **Trade-off - Missing SQL Content:** A significant trade-off was made by generating a placeholder for `bq_d_ausd_bp_ta_rn_vertrag.sql` due to the unavailability of the original Oracle SQL content. This defers the most complex part of the migration (data transformation logic) to a later stage.

## 4. Manual steps before go-live

Before the migrated job can be deployed and run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset `project.dataset` exists. If not, create it.
2.  **BigQuery Table Creation:**
    *   Create the audit log table:
        ```sql
        CREATE TABLE project.dataset.job_audit_log (
            job_id STRING,
            log_timestamp TIMESTAMP,
            event_type STRING,
            details STRING
        );
        ```
    *   Create the error log table:
        ```sql
        CREATE TABLE project.dataset.job_error_log (
            job_id STRING,
            log_timestamp TIMESTAMP,
            error_message STRING
        );
        ```
    *   **Crucially, identify and create all target tables** that `d_ausd_bp_ta_rn_vertrag.sql` writes to. This requires analyzing the original Oracle SQL script. Ensure schemas, data types, and partitioning/clustering are optimized for BigQuery.
3.  **IAM/Permissions:**
    *   The service account or user executing the BigQuery stored procedure must have appropriate IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` to create/update tables and insert into log tables.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `BigQuery Data Viewer` on any source tables accessed by the `d_ausd_bp_ta_rn_vertrag.sql` logic.
4.  **Connection Strings / Secrets:**
    *   No direct connection strings are needed for BigQuery stored procedures. However, if the `d_ausd_bp_ta_rn_vertrag.sql` logic involved external data sources or required specific credentials, these would need to be configured (e.g., BigQuery connections, external tables, or secure parameter management in the orchestrator).
5.  **Scheduling:**
    *   The original job was invoked by `r_ausd_bp_ta_rn_vertrag.ksh`. The new BigQuery stored procedure will need to be scheduled by an orchestration tool (e.g., Airflow, Cloud Composer, Cloud Workflows).
    *   Create an Airflow DAG or similar workflow that calls `project.dataset.r_ausd_bp_ta_rn_vertrag` and passes the required parameters (`p_job_id`, `p_date_today_str`, `p_date_yesterday_str`, `p_mandant`).
6.  **Populate `bq_d_ausd_bp_ta_rn_vertrag.sql`:**
    *   **This is a critical step.** The placeholder content in `bq_d_ausd_bp_ta_rn_vertrag.sql` must be replaced with the actual BigQuery-translated SQL logic from the original `d_ausd_bp_ta_rn_vertrag.sql` file. This may involve creating a separate nested stored procedure or embedding the SQL directly.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up and represent potential risks or incomplete aspects of the migration:

*   **Missing `d_ausd_bp_ta_rn_vertrag.sql` Content (B4 Item):** The most significant gap is the absence of the actual SQL logic from `d_ausd_bp_ta_rn_vertrag.sql`. The `bq_d_ausd_bp_ta_rn_vertrag.sql` file is a placeholder. This content must be obtained, analyzed, and translated into BigQuery SQL. This will likely be the most complex part of the migration.
*   **Commented-Out Logic:** The original KornShell script contained commented-out sections for `sed`, `sort`, `join` operations, and `FOSJobDeaktivate`/`FOSJobErzeugeEintrag` calls. It needs to be confirmed if these functionalities are truly obsolete or if they represent dormant logic that might become active. If active, they would require migration to BigQuery SQL or integration with the orchestration layer.
*   **Full Utility Script Equivalence:** The functionality of custom utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) was assumed to be covered by BigQuery's native features or the new logging mechanism. A detailed review of these scripts is needed to ensure no critical business logic or configuration was missed.
*   **Environment Variables (`.dw_init`, `BERT_DIR_ROOT`):** The design document notes that these will be replaced by BigQuery configuration tables or parameters. A concrete plan for how these environment variables are used and how their values will be provided to the BigQuery stored procedure is required.
*   **`v_processed_records` Placeholder:** The `v_processed_records` variable in the generated stored procedure currently has a dummy value (`-1`). Once `bq_d_ausd_bp_ta_rn_vertrag.sql` is implemented, this variable must be correctly populated with the actual count of records processed by the core SQL logic.
*   **`r_ausd_bp_ta_rn_vertrag.ksh` Orchestration:** The design document mentions that `k_ausd_bp_ta_rn_vertrag.ksh` is invoked by `r_ausd_bp_ta_rn_vertrag.ksh`. The migration of `r_ausd_bp_ta_rn_vertrag.ksh` and its interaction with the new BigQuery stored procedure needs to be designed and implemented, likely as part of an Airflow DAG.

## 6. Validation

Validation of the migrated job involves ensuring functional equivalence and correct operation in the BigQuery environment.

1.  **Deploy the Stored Procedure:**
    *   Execute the `bq_sp_r_ausd_bp_ta_rn_vertrag.sql` script to create the stored procedure in BigQuery.
2.  **Populate `bq_d_ausd_bp_ta_rn_vertrag.sql`:**
    *   **Before full validation, the placeholder `bq_d_ausd_bp_ta_rn_vertrag.sql` must be replaced with the actual BigQuery SQL.**
3.  **Run the Stored Procedure:**
    *   Execute the stored procedure with various test parameters, including valid, invalid, and edge-case dates/mandants.
    *   Example execution (after `bq_d_ausd_bp_ta_rn_vertrag.sql` is implemented):
        ```sql
        CALL project.dataset.r_ausd_bp_ta_rn_vertrag(
            'k_ausd_bp_ta_rn_vertrag_test',
            '20231027',
            '20231026',
            '100'
        );
        ```
4.  **Verification Steps:**
    *   **Parameter Validation:**
        *   Call with missing parameters (e.g., `NULL` for `p_date_today_str`). Expect an error logged in `job_error_log` and a `SIGNAL` (error raised).
        *   Call with invalid date formats (e.g., `2023-10-27`). Expect an error logged in `job_error_log` and a `SIGNAL`.
        *   Call with logically incorrect dates (e.g., `p_date_yesterday_str` not being the day before `p_date_today_str`). Expect an error logged in `job_error_log` and a `SIGNAL`.
    *   **Data Transformation:**
        *   After a successful run with valid parameters, query the target tables that `d_ausd_bp_ta_rn_vertrag.sql` (now `bq_d_ausd_bp_ta_rn_vertrag.sql`) is supposed to populate.
        *   Compare the data in these BigQuery target tables with the expected output from the original Oracle job for the same input parameters. This requires a baseline from the legacy system.
    *   **Logging:**
        *   Query `project.dataset.job_audit_log` to confirm successful job completion entries, including the correct `job_id`, `log_timestamp`, `event_type='SUCCESS'`, and `details` (especially the processed record count).
        *   Query `project.dataset.job_error_log` to confirm that any expected errors (e.g., from invalid parameters) are logged correctly.
    *   **Performance:** Monitor BigQuery job execution time and slot consumption to ensure it meets performance requirements.

**"Passing" Criteria:**

*   The BigQuery stored procedure executes successfully for valid inputs without raising unhandled errors.
*   All parameter and date validations function as expected, raising errors and logging them for invalid inputs.
*   The data transformed and loaded into the BigQuery target tables is functionally identical to the output of the original Oracle job for the same input data and parameters.
*   The `job_audit_log` accurately reflects job start, success, and processed record counts.
*   The `job_error_log` accurately captures any errors encountered during execution.
*   The `v_processed_records` variable correctly reflects the number of records processed by the core SQL logic.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be followed:

1.  **Orchestrator Reversion:**
    *   Immediately revert the scheduling mechanism (e.g., Airflow DAG) to call the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh` instead of the BigQuery stored procedure.
2.  **BigQuery Stored Procedure Deletion:**
    *   Delete the migrated BigQuery stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.r_ausd_bp_ta_rn_vertrag;
        ```
3.  **Data Reversion (if necessary):**
    *   If the BigQuery job made incorrect data modifications to target tables, revert these changes. This typically requires:
        *   Restoring the target tables from a snapshot or backup taken just before the BigQuery job ran.
        *   Running compensating transactions to undo the incorrect changes.
        *   **Note:** A robust data rollback strategy should be in place for all critical data transformations.
4.  **Monitoring:**
    *   Monitor the legacy system to ensure it resumes normal operation.
    *   Monitor BigQuery for any residual impact or unexpected behavior.

This rollback procedure assumes that the original KornShell script and its dependencies remain operational and accessible.