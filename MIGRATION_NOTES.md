# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell job `k_ausd_v_ta_inv_acc.ksh` and its associated Oracle SQL logic (`d_ausd_v_ta_inv_acc.sql`).

The original job, responsible for orchestrating data processing for the `ta_inv_acc` table, including parameter validation, common utility function calls, SQL execution, and job status management, has been re-platformed.

**Target Platform:** Google Cloud's BigQuery environment.
**Approach:** The orchestration logic previously handled by the KornShell script has been translated into a BigQuery Stored Procedure. The core data transformation logic from the Oracle SQL script has been re-written in BigQuery Standard SQL, embedded within or called by this stored procedure. Job status management has been adapted to utilize BigQuery tables.

## 2. Generated Artifacts

The migration process resulted in the creation of the following BigQuery-native artifacts:

*   **`sp_k_ausd_v_ta_inv_acc` (BigQuery Stored Procedure):**
    *   **Role:** Replaces the `k_ausd_v_ta_inv_acc.ksh` KornShell script. This stored procedure handles parameter parsing (`p_JobKennung`, `p_EintragsNr`), orchestrates the data processing, manages job status (activating, deactivating, ignoring active jobs), and logs execution details. It encapsulates the overall flow and error handling.
*   **`d_ausd_v_ta_inv_acc_bq.sql` (BigQuery SQL Script/Logic):**
    *   **Role:** Contains the translated data transformation logic from the original `d_ausd_v_ta_inv_acc.sql` Oracle script. This BigQuery Standard SQL code is executed by the `sp_k_ausd_v_ta_inv_acc` stored procedure to perform the core data processing on the `ta_inv_acc` table (or its BigQuery equivalent).
*   **`job_status_log` (BigQuery Table):**
    *   **Role:** A new BigQuery table designed to store job execution status, replacing the functionality of the original Oracle job table. This table tracks job activation, deactivation, and other relevant metadata for the `sp_k_ausd_v_ta_inv_acc` procedure.

## 3. Key Design Decisions

*   **Orchestration Re-platforming (KornShell to BigQuery Stored Procedure):**
    *   **Why:** Leveraging BigQuery Stored Procedures provides native integration with the Google Cloud ecosystem, eliminates external shell script dependencies, and allows for more efficient execution of data-centric workflows directly within BigQuery. This centralizes the logic and benefits from BigQuery's scalability.
    *   **Trade-offs:** Required a complete re-implementation of shell-specific logic (e.g., file operations, complex string parsing, external utility calls) using BigQuery's SQL scripting capabilities, which can be less flexible for non-data operations.
*   **Data Transformation Language (Oracle SQL to BigQuery Standard SQL):**
    *   **Why:** Migrating the core data processing to BigQuery Standard SQL allows the job to fully utilize BigQuery's columnar storage, distributed query engine, and optimized performance for large datasets. It avoids the overhead of external SQL*Plus calls.
    *   **Trade-offs:** Required careful translation of Oracle-specific SQL constructs, functions, and PL/SQL logic to their BigQuery Standard SQL equivalents. Performance characteristics might differ, necessitating query optimization for BigQuery.
*   **Job Status Management (Oracle Table to BigQuery Table):**
    *   **Why:** Consolidating job status tracking within BigQuery provides a unified data platform for both operational data and metadata. This simplifies monitoring and auditing within the Google Cloud environment.
    *   **Trade-offs:** Required designing a new BigQuery schema for the `job_status_log` table and re-implementing the status update logic within the BigQuery Stored Procedure.
*   **Parameter Handling:**
    *   **Why:** Direct parameter passing to the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`) offers type safety and a standardized interface, replacing the shell script's command-line argument parsing.
*   **Error Handling and Logging:**
    *   **Why:** Utilizes BigQuery's native error handling mechanisms (e.g., `RAISE` in stored procedures) and integrates with Cloud Logging for centralized error reporting and monitoring, replacing custom shell error utilities.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `isrpt_bert_aufbereitung`) exists in the project. If not, create it.
2.  **Target Table Verification:**
    *   Verify that the target table `ta_inv_acc` (or its BigQuery equivalent) exists within the designated BigQuery dataset and is accessible. Ensure its schema is compatible with the migrated SQL logic.
3.  **`job_status_log` Table Creation:**
    *   Execute the DDL to create the `job_status_log` BigQuery table in the appropriate dataset. This table is crucial for the stored procedure's job status management.
4.  **IAM Permissions Configuration:**
    *   Grant the necessary BigQuery Data Editor and BigQuery Job User roles to the service account or user identity that will be executing the `sp_k_ausd_v_ta_inv_acc` stored procedure. This includes permissions to read from source tables, write to target tables, and update the `job_status_log` table.
5.  **Scheduling Setup:**
    *   Configure a scheduler (e.g., Cloud Composer/Airflow, Cloud Scheduler, Dataform) to invoke the `sp_k_ausd_v_ta_inv_acc` BigQuery Stored Procedure at the required frequency and with the correct parameters. This replaces the original KornShell script's scheduling mechanism.

## 5. Known Gaps & Unresolved References

The following items were identified as requiring further attention or represent a change in approach from the original system:

*   **Shell Utility Functionality Replacement:**
    *   The original KornShell script sourced several utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While `h_alis_parameter.ksh` and `h_alis_sqlplus.ksh` functionality is directly replaced by BigQuery SP parameters and native execution, the specific implementations of `f_alis_msgerr.ksh` (error messaging) and `h_alis_date.ksh` (date checks) need to be fully verified for functional equivalence within the BigQuery Stored Procedure.
    *   **Follow-up:** Ensure all error conditions and date validations from the original script are accurately replicated and tested in the BigQuery Stored Procedure.
*   **Temporary File for Record Count:**
    *   The original script captured the number of processed records in a temporary file. This mechanism is not directly replicated in BigQuery.
    *   **Follow-up:** The record count is now returned by the stored procedure or can be logged to Cloud Logging or a dedicated metrics table. Confirm the chosen method meets reporting requirements.
*   **B4 Items (Redesign Considerations):**
    *   The migration itself represents a significant re-platforming and redesign of the orchestration layer. While the core data transformation logic aims for functional equivalence, the overall job flow and error handling have been adapted to BigQuery's paradigm.
    *   **Follow-up:** A thorough review of the new BigQuery Stored Procedure's logic is recommended to ensure it aligns with best practices for BigQuery and GCP, potentially identifying areas for further optimization or simplification that were not feasible in the original KornShell environment.

## 6. Validation

To ensure the successful migration and correct functioning of the new BigQuery job, the following validation steps should be performed:

1.  **Unit Testing of SQL Logic:**
    *   Execute the `d_ausd_v_ta_inv_acc_bq.sql` (or embedded SQL) transformation logic with various test datasets, including edge cases and invalid inputs, to confirm correct data manipulation.
2.  **Integration Testing of Stored Procedure:**
    *   Invoke the `sp_k_ausd_v_ta_inv_acc` stored procedure with different combinations of `p_JobKennung` and `p_EintragsNr` parameters.
    *   Test scenarios for active jobs (should be ignored), new jobs (should activate and process), and deactivation of old jobs.
3.  **Data Validation:**
    *   **Record Counts:** Compare the number of records processed and affected in the target `ta_inv_acc` table (or its BigQuery equivalent) by the new BigQuery job against the counts from a successful run of the original Oracle job using the same input data.
    *   **Data Integrity:** Perform aggregate checks (e.g., SUM, AVG, COUNT DISTINCT) on key columns of the target table. Compare these aggregates between the BigQuery output and the Oracle output for a representative dataset.
    *   **Sample Data Comparison:** Select a sample of records and verify their transformation results manually between the source and target systems.
4.  **Job Status Logging Verification:**
    *   After each test run, query the `job_status_log` BigQuery table to ensure that job activation, deactivation, and completion statuses are accurately recorded.
5.  **Performance Testing:**
    *   Monitor the execution time and BigQuery slot consumption of the `sp_k_ausd_v_ta_inv_acc` procedure. Compare against the original job's execution time to ensure performance is acceptable or improved.

**"Passing" Criteria:**

*   The `sp_k_ausd_v_ta_inv_acc` stored procedure executes successfully without any BigQuery errors.
*   All data transformations defined in the original `d_ausd_v_ta_inv_acc.sql` are correctly applied in BigQuery, resulting in functionally equivalent output data.
*   Record counts and data integrity checks (e.g., checksums, aggregate sums) on the target table match the expected results from the original system.
*   The `job_status_log` table accurately reflects the lifecycle and status of the job execution.
*   The performance of the BigQuery job is within acceptable limits, ideally matching or exceeding the original job's performance.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Job Suspension:**
    *   Immediately disable or pause the scheduler (e.g., Cloud Composer DAG, Cloud Scheduler job) responsible for invoking the `sp_k_ausd_v_ta_inv_acc` BigQuery Stored Procedure.
2.  **Re-enable Original Job:**
    *   Re-enable the original scheduling mechanism for the `k_ausd_v_ta_inv_acc.ksh` KornShell script to resume operations on the legacy platform.
3.  **Data Recovery/Reconciliation (if necessary):**
    *   Assess the impact of the BigQuery job's execution on the target `ta_inv_acc` table.
    *   If the BigQuery job performed destructive updates or introduced incorrect data, initiate a data recovery process. This may involve restoring the `ta_inv_acc` table from a snapshot or backup taken prior to the BigQuery job's execution, or running a data correction script.
    *   If the job is idempotent (e.g., truncates and reloads), simply re-running the original job might be sufficient to correct the state.
4.  **Investigation and Remediation:**
    *   Analyze BigQuery job logs, Cloud Logging, and the `job_status_log` table to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery Stored Procedure or its dependent SQL.
5.  **Cleanup (Optional):**
    *   If the rollback is deemed permanent, consider deleting the `sp_k_ausd_v_ta_inv_acc` stored procedure and the `job_status_log` table from BigQuery.