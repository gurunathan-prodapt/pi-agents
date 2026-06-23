# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh`. The script, primarily responsible for job orchestration, parameter handling, logging, and invoking a "kernel script" (`k_ausd_v_ta_barrier_zusgf.ksh`), has been migrated to a BigQuery Stored Procedure.

**Source:**
*   **Job Name:** `r_ausd_v_ta_barrier_zusgf.ksh`
*   **Technology:** KornShell (ksh)
*   **Role:** Wrapper/orchestration script for `ta_barrier_zusgf` data reconciliation.

**Target Platform:**
*   **Platform:** Google Cloud Platform (GCP)
*   **Service:** BigQuery
*   **Component:** BigQuery Stored Procedure (`project.dataset.sp_r_ausd_v_ta_barrier_zusgf`)

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`sp_r_ausd_v_ta_barrier_zusgf.sql`**
    *   **Role:** This is the primary generated artifact, a BigQuery Stored Procedure that replaces the original KornShell wrapper script. It handles input parameters, initializes logging, manages error trapping, and orchestrates the call to the core business logic (which will be migrated to `sp_k_ausd_v_ta_barrier_zusgf`).
    *   **Location:** `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`

*   **`project.dataset.job_log` (BigQuery Table DDL)**
    *   **Role:** This DDL defines a new BigQuery table used for centralized job logging. It replaces the file-based logging and custom `DWMSG_*` functions from the original KornShell environment, providing structured, queryable audit and operational monitoring data.
    *   **Location:** `project.dataset.job_log`

*   **`sp_k_ausd_v_ta_barrier_zusgf` (BigQuery Stored Procedure - Placeholder)**
    *   **Role:** This represents the future migration of the `k_ausd_v_ta_barrier_zusgf.ksh` kernel script. While not fully migrated as part of *this* wrapper migration, a placeholder procedure is assumed to exist for the wrapper to successfully call. It will eventually contain the core data reconciliation logic.
    *   **Location:** `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Migration of Wrapper Script to BigQuery Stored Procedure:**
    *   **Why:** Encapsulates the orchestration logic directly within BigQuery, leveraging its native capabilities for procedural SQL, parameter handling, and error management. This aligns with the target architecture's preference for BigQuery-native solutions where possible.
    *   **Trade-offs:** Requires re-implementation of shell-specific constructs (e.g., `getopts`, `trap`, `DWMSG_*` functions) using BigQuery SQL equivalents. Loses direct filesystem access for logging, which is mitigated by a dedicated logging table.

*   **Centralized BigQuery Logging Table (`project.dataset.job_log`):**
    *   **Why:** Replaces the disparate file-based logging and custom `DWMSG_*` framework. Provides structured, queryable logs for all migrated jobs, enabling easier monitoring, auditing, and troubleshooting across the data platform.
    *   **Trade-offs:** Requires a new DDL and a standardized logging approach across all migrated jobs.

*   **BigQuery `BEGIN...EXCEPTION` for Error Handling:**
    *   **Why:** Replaces the KornShell `trap` command for robust error handling. This is the standard and idiomatic way to manage exceptions in BigQuery Stored Procedures, ensuring proper logging of failures and graceful termination.
    *   **Trade-offs:** Requires careful mapping of shell error codes/messages to BigQuery's error handling mechanisms.

*   **Stored Procedure Parameters for Input:**
    *   **Why:** Replaces the `getopts` command-line parsing. BigQuery Stored Procedure parameters provide a type-safe and explicit way to pass inputs, improving readability and maintainability.
    *   **Trade-offs:** Requires callers to adapt to the new parameter signature.

*   **Separate Stored Procedure for Kernel Logic (`sp_k_ausd_v_ta_barrier_zusgf`):**
    *   **Why:** Maintains the modularity of the original design where the wrapper (`r_ausd_v_ta_barrier_zusgf.ksh`) invoked a kernel script (`k_ausd_v_ta_barrier_zusgf.ksh`). This allows for independent migration and testing of the core business logic, promoting clear separation of concerns.
    *   **Trade-offs:** The overall job functionality remains dependent on the successful and complete migration of the kernel script.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```

2.  **BigQuery Logging Table Creation:**
    *   Execute the DDL for the `project.dataset.job_log` table to create it. This table is crucial for logging job status and messages.
        ```sql
        CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
          job_name STRING,
          job_number INT64,
          log_level STRING,
          error_code INT64,
          error_arg STRING,
          message STRING,
          status STRING,
          stichtag STRING,
          stichtag_format STRING,
          log_file_name STRING, -- Retained for compatibility/future use, though not directly used by BQ SP
          script_name STRING,
          created_at TIMESTAMP,
          finished_at TIMESTAMP
        );
        ```

3.  **Placeholder Kernel Stored Procedure Creation:**
    *   Create a placeholder for `sp_k_ausd_v_ta_barrier_zusgf`. This is necessary for `sp_r_ausd_v_ta_barrier_zusgf` to execute successfully, even if the kernel's full logic isn't migrated yet.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`(
            IN p_job_kennung STRING,
            IN p_entry_number INT64
        )
        BEGIN
            -- Placeholder for the actual kernel script logic
            SELECT FORMAT('Placeholder for sp_k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, EntryNumber: %d', p_job_kennung, p_entry_number);
            -- Optionally, simulate success or failure for testing the wrapper's error handling
            -- RAISE 'Simulated error from kernel script';
        END;
        ```

4.  **IAM/Permissions:**
    *   The service account or user executing `sp_r_ausd_v_ta_barrier_zusgf` must have the following BigQuery permissions:
        *   `bigquery.dataEditor` on `project.dataset` (to insert/update `job_log`).
        *   `bigquery.routines.call` on `project.dataset.sp_k_ausd_v_ta_barrier_zusgf` (to call the kernel procedure).
        *   `bigquery.routines.create` and `bigquery.routines.update` (to deploy/update the stored procedures themselves).

5.  **Scheduling:**
    *   If using Cloud Composer (Airflow), deploy the corresponding DAG (`r_ausd_v_ta_barrier_zusgf_dag.py` if created) to schedule the execution of `sp_r_ausd_v_ta_barrier_zusgf`.
    *   If direct BigQuery execution, ensure the calling mechanism (e.g., Cloud Functions, Cloud Run, manual execution) is configured.

## 5. Known Gaps & Unresolved References

*   **Kernel Script Migration (`k_ausd_v_ta_barrier_zusgf.ksh`):** This is the most significant unresolved item. The `sp_r_ausd_v_ta_barrier_zusgf` procedure is a wrapper; its full functionality relies on the complete and correct migration of `k_ausd_v_ta_barrier_zusgf.ksh` to `sp_k_ausd_v_ta_barrier_zusgf`. A separate, detailed migration effort is required for the kernel script.
*   **Completeness of Sourced Utilities:** The full functionality and implications of the original KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) need to be thoroughly assessed during the kernel script's migration. While the wrapper's direct use of these has been replaced, their impact on the kernel script's logic might be substantial.
*   **Runtime Environment Variables:** The original script relied on `HOME` and `BERT_DIR_ROOT`. In the BigQuery environment, these are not directly applicable. Any configuration or paths derived from these variables in the kernel script will need to be managed via BigQuery procedure parameters, configuration tables, or environment variables within an orchestration layer (e.g., Airflow).
*   **`p_test_mode` Parameter:** The generated `sp_r_ausd_v_ta_barrier_zusgf` procedure accepts `p_test_mode` as an input parameter, but its logic does not currently utilize this flag. If the original `ksh` script had specific test-mode behaviors, this functionality is currently a gap and would need to be implemented in the BigQuery procedure.
*   **`semi_auto` Classification:** The original job's classification as `semi_auto` suggests potential complexities or manual steps that might not be fully captured by an automated migration. Ongoing vigilance during testing and initial production runs is advised.

## 6. Validation

To validate the successful migration of `r_ausd_v_ta_barrier_zusgf.ksh`:

1.  **Execution:**
    *   Execute the BigQuery Stored Procedure `project.dataset.sp_r_ausd_v_ta_barrier_zusgf` with various parameter combinations.
    *   **Example Call (Success Scenario):**
        ```sql
        CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
            p_job_kennung => 'TEST_JOB_WRAPPER',
            p_entry_number => 12345,
            p_debug_mode => TRUE,
            p_test_mode => FALSE
        );
        ```
    *   **Example Call (Error Scenario - requires `sp_k_ausd_v_ta_barrier_zusgf` to raise an error):**
        *   Modify the placeholder `sp_k_ausd_v_ta_barrier_zusgf` to `RAISE 'Simulated error from kernel script';`
        ```sql
        CALL `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
            p_job_kennung => 'TEST_JOB_ERROR',
            p_entry_number => 67890,
            p_debug_mode => TRUE,
            p_test_mode => FALSE
        );
        ```

2.  **Passing Criteria:**
    *   **Successful Execution:** The procedure completes without unhandled errors.
    *   **Logging Verification:**
        *   Query the `project.dataset.job_log` table to confirm entries for the executed job.
        *   Verify that `status` transitions correctly from `RUNNING` to `SUCCESS` (for successful runs) or `FAILED` (for error runs).
        *   Check `message`, `created_at`, `finished_at`, `job_name`, `job_number`, and `script_name` fields are populated correctly.
        *   For error scenarios, ensure `log_level` is `ERROR`, and `error_code` and `error_arg` contain relevant error details.
    *   **Kernel Call Verification:** Confirm that the `sp_k_ausd_v_ta_barrier_zusgf` placeholder procedure was successfully invoked (e.g., by checking its output if it prints messages, or by observing its simulated behavior).
    *   **Debug Mode:** When `p_debug_mode` is `TRUE`, verify that debug messages (e.g., "Job started.", "Job-Nr", "JobKennung") are printed to the BigQuery console output.

## 7. Rollback Procedure

In case of issues during or after go-live, follow these steps to roll back the migration:

1.  **Disable New Execution:** Immediately stop any scheduled executions (e.g., Airflow DAGs) or direct calls to `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`.

2.  **Delete BigQuery Stored Procedure:**
    *   Remove the migrated stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`;
        ```
    *   (Optional) If the placeholder `sp_k_ausd_v_ta_barrier_zusgf` was created solely for this migration, it can also be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`;
        ```

3.  **Revert Logging Table (if necessary):**
    *   If the `project.dataset.job_log` table was newly created and is not used by other migrated jobs, it can be dropped. If it's a shared logging table, no action is needed here.
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        ```

4.  **Re-enable Original Job:**
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh` script and its dependencies are fully functional and accessible.
    *   Re-point any upstream orchestrators or calling mechanisms to execute the original KornShell script.

5.  **Verify Original Job Functionality:**
    *   Run the original `ksh` script and verify that it executes correctly and produces expected outputs and logs.