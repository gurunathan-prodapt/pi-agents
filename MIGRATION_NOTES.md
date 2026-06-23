# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_barrier_zusgf.ksh` KornShell orchestrator script and its associated `d_ausd_v_ta_barrier_zusgf.sql` SQL data processing script.

The original system involved a KornShell script managing job execution, parameter handling, and invoking an Oracle SQL script for data transformation. The migration target is Google Cloud's BigQuery platform.

The KornShell orchestration logic has been translated into a BigQuery Stored Procedure (`p_k_ausd_v_ta_barrier_zusgf`), while the core data transformation logic from the Oracle SQL script has been converted into another BigQuery Stored Procedure (`p_d_ausd_v_ta_barrier_zusgf`). All relevant source and target tables have been defined as BigQuery tables, and new control tables (`job_control`, `job_error_log`) have been introduced for BigQuery-native job management and error logging.

This migration aims to leverage BigQuery's scalability and managed services, reducing reliance on on-premise shell scripting and Oracle database specifics.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL files:

*   **`build/ddl/DWTK_MELDUNGEN.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `DWTK_MELDUNGEN` table. This table serves as a source for the data processing logic, mirroring its legacy counterpart.
*   **`build/ddl/SOF_TA_BARRIER.sql`**
    *   **Role:** BigQuery DDL script to create the `SOF_TA_BARRIER` table. This table is another critical source for the data processing, reflecting its structure from the legacy system.
*   **`build/ddl/SOF_TA_BARRIER_ZUSGF.sql`**
    *   **Role:** BigQuery DDL script to create the `SOF_TA_BARRIER_ZUSGF` table. This is a primary target table where the transformed data is written by the migrated data processing logic.
*   **`build/ddl/R_BAR.sql`**
    *   **Role:** BigQuery DDL script to create a placeholder `R_BAR` table. While identified as a target in the design, the generated data processing procedure (`p_d_ausd_v_ta_barrier_zusgf`) does not currently write to this table. This DDL is included for completeness based on the source inventory but highlights a potential gap (see Section 5).
*   **`build/ddl/VIA.sql`**
    *   **Role:** BigQuery DDL script to create a placeholder `VIA` table. Similar to `R_BAR`, this table was identified as a target, but the generated data processing procedure does not write to it. This DDL is included for completeness but also points to a potential gap (see Section 5).
*   **`build/ddl/job_control.sql`**
    *   **Role:** BigQuery DDL script to create the `job_control` table. This new table is central to the migrated orchestration, tracking the status, start/end times, record counts, and error messages for each job execution.
*   **`build/ddl/job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This new table captures detailed error and warning messages generated during job execution, replacing the shell script's error logging mechanisms.
*   **`build/stored_procedures/p_d_ausd_v_ta_barrier_zusgf.sql`**
    *   **Role:** BigQuery Stored Procedure containing the translated data transformation logic from the original `d_ausd_v_ta_barrier_zusgf.sql`. It reads from `SOF_TA_BARRIER` and writes to `SOF_TA_BARRIER_ZUSGF`, performing aggregations and conditional logic.
*   **`build/stored_procedures/p_k_ausd_v_ta_barrier_zusgf.sql`**
    *   **Role:** BigQuery Stored Procedure replacing the `k_ausd_v_ta_barrier_zusgf.ksh` KornShell script. This procedure handles parameter validation, job concurrency checks, job status management in `job_control`, invocation of `p_d_ausd_v_ta_barrier_zusgf`, and error logging to `job_error_log`.

## 3. Key Design Decisions

The following key design decisions guided the migration:

*   **Orchestration to BigQuery Stored Procedure:** The KornShell orchestrator (`k_ausd_v_ta_barrier_zusgf.ksh`) was migrated to a BigQuery Stored Procedure (`p_k_ausd_v_ta_barrier_zusgf`). This decision eliminates external shell script dependencies, simplifies deployment, and allows for native BigQuery scheduling and monitoring. It centralizes the job's control flow within the BigQuery environment.
*   **Data Processing to BigQuery Stored Procedure:** The core SQL logic from `d_ausd_v_ta_barrier_zusgf.sql` was translated into a separate BigQuery Stored Procedure (`p_d_ausd_v_ta_barrier_zusgf`). This modular approach promotes reusability, improves readability, and allows for independent testing of the data transformation logic.
*   **BigQuery-Native Job Control:** The custom job control mechanisms (e.g., checking for active instances, logging execution) previously handled by shell scripts and potentially a legacy job table, were replaced by dedicated BigQuery tables (`job_control` and `job_error_log`) and integrated directly into the orchestrating stored procedure. This provides a centralized, BigQuery-native way to manage and monitor job executions.
*   **Parameter Handling:** Shell script `getopts` for parameter parsing was replaced by `IN` parameters in the BigQuery Stored Procedure. This simplifies parameter passing and validation within the BigQuery environment.
*   **Error Handling and Logging:** The shell script's error handling (`DWMSG_MeldeFehler`, `exit`) was replaced by BigQuery's `RAISE` statements and inserts into the `job_error_log` table. This provides structured error logging within BigQuery, making it easier to query and analyze job failures.
*   **Record Count Mechanism:** The original method of writing record counts to a temporary file was replaced by directly querying the target table (`SOF_TA_BARRIER_ZUSGF`) within the BigQuery Stored Procedure and updating the `job_control` table. This streamlines the process and removes file system dependencies.
*   **Oracle SQL to BigQuery SQL Translation:** Oracle-specific functions and syntax (e.g., `DECODE`, `REPLACE`, date formatting) were carefully translated to their BigQuery SQL equivalents (`CASE WHEN`, `REPLACE`, `FORMAT_DATE`, `STRING_AGG`). This required a detailed analysis of the original SQL to ensure functional parity.

**Notable Trade-offs:**
*   **Loss of Shell Flexibility:** Migrating from KornShell means losing the flexibility of shell commands for file system operations, external tool invocation, or complex environment manipulations. These functionalities must now be re-implemented using BigQuery SQL, UDFs, or potentially by integrating with Cloud Composer for more complex orchestration needs.
*   **BigQuery SQL Dialect Specificity:** The migration introduces a dependency on BigQuery's SQL dialect, which differs from Oracle's PL/SQL. Future maintenance will require BigQuery SQL expertise.
*   **Manual Translation Effort:** The "Semi-Auto (B2)" automation bucket indicates that the Oracle-to-BigQuery SQL translation required significant manual review and adjustment, especially for complex logic and package usage.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Google Cloud Project and Dataset Setup:**
    *   Ensure a Google Cloud Project (`your_project_id`) is established.
    *   Create the target BigQuery Dataset (`your_dataset_id`) within the project. This dataset will house all migrated tables and stored procedures.
    *   **Command Example:** `bq mk --dataset your_project_id:your_dataset_id`

2.  **BigQuery Table Creation:**
    *   Execute all DDL scripts located in `build/ddl/` to create the necessary tables in your target dataset.
    *   This includes: `DWTK_MELDUNGEN`, `SOF_TA_BARRIER`, `SOF_TA_BARRIER_ZUSGF`, `R_BAR`, `VIA`, `job_control`, and `job_error_log`.
    *   **Command Example (for each DDL):** `bq query --use_legacy_sql=false < build/ddl/DWTK_MELDUNGEN.sql`

3.  **Initial Data Loading:**
    *   Load historical or initial data into the source tables: `DWTK_MELDUNGEN` and `SOF_TA_BARRIER`. This typically involves exporting data from the legacy Oracle system and importing it into BigQuery using tools like `bq load`, Cloud Storage, or Data Transfer Service.
    *   **Note:** The `TABLE:TABLE` reference in the source inventory is generic and requires manual identification of the actual table(s) it refers to in the legacy system. These tables must also be created and loaded into BigQuery.

4.  **BigQuery Stored Procedure Deployment:**
    *   Deploy the generated stored procedures to your target BigQuery dataset.
    *   Execute `build/stored_procedures/p_d_ausd_v_ta_barrier_zusgf.sql`.
    *   Execute `build/stored_procedures/p_k_ausd_v_ta_barrier_zusgf.sql`.
    *   **Command Example (for each SP):** `bq query --use_legacy_sql=false < build/stored_procedures/p_d_ausd_v_ta_barrier_zusgf.sql`

5.  **IAM and Permissions:**
    *   Ensure the Google Cloud service account or user identity that will execute these BigQuery stored procedures has the necessary IAM roles. At a minimum, it will require:
        *   `BigQuery Data Editor` on `your_project_id.your_dataset_id` to create/update tables and run procedures.
        *   `BigQuery Job User` on `your_project_id` to run BigQuery jobs.
    *   If using Cloud Composer/Airflow, ensure the Composer service account has these permissions.

6.  **Scheduling Configuration:**
    *   Configure a scheduling mechanism to invoke the `p_k_ausd_v_ta_barrier_zusgf` stored procedure.
    *   **Option A: BigQuery Scheduled Queries:** Create a scheduled query that calls `CALL your_project_id.your_dataset_id.p_k_ausd_v_ta_barrier_zusgf('your_job_kennung', 'your_eintrags_nr');` with the appropriate frequency.
    *   **Option B: Cloud Composer (Airflow):** Develop a Python DAG that uses the `BigQueryOperator` to call the stored procedure. This is recommended for more complex workflows or dependencies.
    *   Ensure the `p_JobKennung` and `p_EintragsNr` parameters are correctly passed during scheduling.

7.  **Legacy Package Migration (if applicable):**
    *   Analyze the Oracle packages (`DWPA_UTIL_SKRIPT`, `SOF$SP_TABLE_FUNCTIONS`, `PA_ANALYZE`) mentioned in the source inventory. If they contain critical business logic not yet translated, migrate them to BigQuery Stored Procedures or User-Defined Functions (UDFs) and deploy them. The current generated code does not include their migration.

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up or represent unresolved aspects of the migration:

*   **Generic `TABLE` Reference:** The `lineage_edges` indicated `d_ausd_v_ta_barrier_zusgf.sql` reads from `TABLE:TABLE`. This is an overly generic reference. The actual table name(s) must be identified in the legacy system and their DDLs and data migrated to BigQuery. The current DDLs do not include a specific table for this.
*   **`R_BAR` and `VIA` Tables - Write Operations:** The `MIGRATION DESIGN DOCUMENT` states that `d_ausd_v_ta_barrier_zusgf.sql` *WRITES_TABLE* to `R_BAR` and `VIA`. However, the generated BigQuery Stored Procedure `p_d_ausd_v_ta_barrier_zusgf` only writes to `SOF_TA_BARRIER_ZUSGF`. The DDLs for `R_BAR` and `VIA` are placeholders. This is a significant functional gap that requires investigation into the original SQL to determine if these writes are conditional, part of a different logical flow, or simply missed during translation. If these writes are essential, the `p_d_ausd_v_ta_barrier_zusgf` procedure must be updated to include them.
*   **Oracle Packages (`DWPA_UTIL_SKRIPT`, `SOF$SP_TABLE_FUNCTIONS`, `PA_ANALYZE`):** The design document noted these packages as dependencies. While the generated code handles the direct SQL translation, it does not include the migration of these packages themselves. If these packages contain reusable functions or procedures critical to the business logic, they must be analyzed and migrated to BigQuery Stored Procedures or UDFs.
*   **Oracle-Specific SQL Nuances:** While common Oracle functions have been translated, a comprehensive, line-by-line audit of the original `d_ausd_v_ta_barrier_zusgf.sql` is recommended to ensure all subtle Oracle-specific behaviors, data type conversions, and edge cases are correctly replicated in BigQuery SQL.
*   **Parent Script Integration (`r_ausd_v_ta_barrier_zusgf.ksh`):** The original job is invoked by `r_ausd_v_ta_barrier_zusgf.ksh`. The migration of this parent script is outside the current scope. A plan for how this parent script (or its migrated equivalent) will invoke the new BigQuery Stored Procedure needs to be established. This might involve using `gcloud bq` commands, a Cloud Composer DAG, or another orchestration layer.
*   **"Semi-Auto (B2)" Implications:** The job's classification as "Semi-Auto (B2)" implies that the generated code serves as a strong starting point but requires manual review, potential adjustments, and thorough testing to ensure full functional equivalence and robustness in the BigQuery environment.

## 6. Validation

Validation of the migrated job involves several stages to ensure functional correctness, data integrity, and performance.

### How to Run Tests:

1.  **Unit Testing `p_d_ausd_v_ta_barrier_zusgf`:**
    *   Load a small, representative dataset into `your_project_id.your_dataset_id.DWTK_MELDUNGEN` and `your_project_id.your_dataset_id.SOF_TA_BARRIER`.
    *   Execute the stored procedure directly: `CALL your_project_id.your_dataset_id.p_d_ausd_v_ta_barrier_zusgf();`
    *   Query the target table `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF` to inspect the output.

2.  **Unit Testing `p_k_ausd_v_ta_barrier_zusgf`:**
    *   **Valid Execution:** Call the orchestrator with valid parameters: `CALL your_project_id.your_dataset_id.p_k_ausd_v_ta_barrier_zusgf('TEST_JOB', '123');`
    *   **Parameter Validation:** Test with `NULL` or empty `p_job_kennung` and `p_eintrags_nr` to ensure error logging and procedure termination.
    *   **Concurrency Check:** Run the procedure twice in quick succession with the same `job_kennung` and `eintrags_nr` to verify the "already active" logic.
    *   **Error Handling:** Introduce a deliberate error in `p_d_ausd_v_ta_barrier_zusgf` (e.g., invalid column name) and then call `p_k_ausd_v_ta_barrier_zusgf` to verify error logging in `job_error_log` and status update in `job_control`.

3.  **End-to-End Integration Testing:**
    *   Load a full, production-like dataset into the source tables (`DWTK_MELDUNGEN`, `SOF_TA_BARRIER`).
    *   Execute the orchestrating stored procedure via the chosen scheduling mechanism (e.g., BigQuery Scheduled Query or Cloud Composer DAG).
    *   Monitor the job's execution in BigQuery's UI or via `bq jobs list`.

4.  **Data Comparison:**
    *   After a successful run, extract the data from the target table `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF`.
    *   Run the original legacy job and extract its output from `SOF$TA_BARRIER_ZUSGF`.
    *   Perform a row-by-row comparison of the two datasets to identify any discrepancies. Tools like `diff`, custom SQL queries, or data validation frameworks can be used.

### What "Passing" Means:

A successful migration and validation implies the following:

*   **Functional Equivalence:** The data generated in `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF` by the BigQuery job is identical to the data generated by the legacy job in `SOF$TA_BARRIER_ZUSGF` for the same input data. This is the primary success criterion.
*   **Orchestration Logic:**
    *   The `job_control` table correctly reflects the job's lifecycle: `RUNNING` at start, `SUCCESS` or `FAILED` at completion, with accurate `start_time`, `end_time`, and `record_count`.
    *   The concurrency check correctly prevents duplicate runs for active jobs and logs a `WARNING` in `job_error_log`.
    *   Parameter validation correctly identifies missing/empty parameters and logs an `ERROR`.
*   **Error Handling:** Any errors during data processing are caught, logged in `job_error_log` with relevant details, and the `job_control` entry is marked as `FAILED`.
*   **Performance:** The BigQuery job completes within acceptable timeframes, ideally matching or improving upon the legacy job's performance.
*   **Resource Utilization:** BigQuery slot consumption and byte processing are within expected limits.

## 7. Rollback Procedure

In the event of critical issues identified during validation or after go-live, the following rollback procedure can be initiated to revert to the legacy system:

1.  **Immediate Action: Stop New Job Execution:**
    *   **If using BigQuery Scheduled Queries:** Disable or delete the scheduled query that invokes `p_k_ausd_v_ta_barrier_zusgf`.
    *   **If using Cloud Composer (Airflow):** Unpause or delete the DAG responsible for running the BigQuery job.
    *   Ensure no new instances of the migrated job are started.

2.  **Data Reversion (if necessary):**
    *   If the migrated job has written incorrect or incomplete data to `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF`, the following options exist:
        *   **Option A (Preferred):** Re-run the original legacy `k_ausd_v_ta_barrier_zusgf.ksh` job to regenerate the `SOF$TA_BARRIER_ZUSGF` table with correct data. This assumes the legacy system is still operational and has the correct source data.
        *   **Option B:** Restore `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF` from a BigQuery table snapshot or backup taken prior to the problematic run.
        *   **Option C:** If the data is critical and cannot be easily regenerated/restored, manually correct the data in `SOF_TA_BARRIER_ZUSGF` (least preferred, prone to errors).

3.  **Re-enable Legacy System:**
    *   Re-enable the original scheduling mechanism for `k_ausd_v_ta_barrier_zusgf.ksh` (e.g., cron job, enterprise scheduler).
    *   Verify that the legacy job runs successfully and produces the expected output.

4.  **Cleanup (Optional, Post-Rollback):**
    *   Once the legacy system is confirmed to be fully operational, the BigQuery stored procedures (`p_k_ausd_v_ta_barrier_zusgf`, `p_d_ausd_v_ta_barrier_zusgf`) can be dropped from the BigQuery dataset.
    *   The `job_control` and `job_error_log` tables can be retained for audit purposes or dropped if no longer needed.
    *   The migrated data tables (`DWTK_MELDUNGEN`, `SOF_TA_BARRIER`, `SOF_TA_BARRIER_ZUSGF`, etc.) can be retained for analysis or dropped if they contain only erroneous data.

**Note:** A well-defined rollback strategy relies heavily on the ability to quickly revert data to a known good state and reactivate the legacy process. Regular backups and clear documentation of the legacy environment are crucial.