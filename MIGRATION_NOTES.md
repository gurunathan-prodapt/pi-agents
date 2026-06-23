# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh` has been migrated to Google BigQuery. This script, originally responsible for orchestrating a contract data reconciliation job (specifically for the `ta_discount` table), including environment initialization, parameter validation, logging, and invoking a core processing script, has been re-implemented as a BigQuery Stored Procedure.

The target platform for this migration is Google BigQuery, leveraging its native stored procedure capabilities for orchestration and dedicated BigQuery tables for job control and error logging.

## 2. Generated Artifacts

The migration produced the following BigQuery SQL artifacts:

*   **`project/dataset/job_control.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_control` table. This table serves as the central repository for tracking the execution status, metadata, and timestamps of all migrated jobs, replacing the original script's file-based job control and log file functions.
*   **`project/dataset/job_error_log.sql`**
    *   **Role:** Defines the DDL for the `job_error_log` table. This table captures detailed error information, including job identifiers, entry numbers, error messages, and timestamps, providing a structured replacement for the original script's error logging mechanisms.
*   **`project/dataset/sp_k_ausd_v_ta_discount.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the core reconciliation logic. This procedure represents the migrated `k_ausd_v_ta_discount.ksh` script, which contains the actual business logic. Its detailed implementation is outside the scope of this specific migration and will be addressed in a separate design.
*   **`project/dataset/sp_vertragsdatenabgleich_ta_discount.sql`**
    *   **Role:** The main BigQuery Stored Procedure that replaces the `r_ausd_v_ta_discount.ksh` wrapper script. It handles parameter input, validates mandatory arguments, manages job entry numbers, logs job status to `job_control`, invokes `sp_k_ausd_v_ta_discount`, and manages error handling by logging to `job_error_log`.
*   **`project/dataset/sp_log_error` (within `sp_vertragsdatenabgleich_ta_discount.sql`)**
    *   **Role:** A helper stored procedure designed to centralize error logging into the `job_error_log` table. It partially replaces the functionality of the original `f_alis_msgerr.ksh` utility script.

## 3. Key Design Decisions

*   **Migration to BigQuery Stored Procedures:** The orchestration logic of the KornShell script was translated into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich_ta_discount`). This decision leverages BigQuery's native capabilities for data processing and orchestration, keeping the logic close to the data and reducing cross-platform dependencies.
*   **Centralized Audit Tables:** Instead of file-based logging and job control, dedicated BigQuery tables (`job_control` and `job_error_log`) were introduced. This provides a structured, queryable, and scalable solution for monitoring job executions and errors, aligning with modern data warehousing practices.
*   **Placeholder for Core Logic:** The core data reconciliation logic, originally in `k_ausd_v_ta_discount.ksh`, was represented by a placeholder stored procedure (`sp_k_ausd_v_ta_discount`). This modular approach allowed for the migration of the wrapper script independently, deferring the more complex data transformation logic to a subsequent phase.
*   **Direct Parameter Handling:** Command-line argument parsing (`getopts`) was replaced by direct input parameters to the BigQuery Stored Procedure. This simplifies the interface and integrates seamlessly with BigQuery's execution model.
*   **BigQuery Native Error Handling:** Shell `trap` mechanisms were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, providing robust error capture and logging within the SQL environment. Transactions are used to ensure atomicity of job status updates.
*   **Replacement of Utility Scripts:** Common functions from sourced utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were reimplemented as BigQuery SQL functions or integrated directly into the stored procedure logic, interacting with the new audit tables.

**Notable Trade-offs:**
*   **Loss of Real-time File Tailing:** The shift from file-based logs to database tables means that real-time log tailing (e.g., using `tail -f`) is no longer directly possible. Monitoring will rely on querying the `job_control` and `job_error_log` tables.
*   **Shell-Specific Features:** Direct translation of OS-level signals (like `trap INT`) is not possible in BigQuery SQL. Error handling is focused on SQL execution errors. More complex external orchestration (e.g., Cloud Composer) might be needed for handling external system signals or process management.
*   **Environment Sourcing:** The implicit environment setup from `.dw_init` is replaced by explicit parameter passing, configuration tables, or BigQuery session variables, requiring careful identification and re-configuration of all necessary environment settings.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists in your Google Cloud project. If not, create it:
    ```bash
    bq mk --dataset project:dataset
    ```
2.  **IAM Permissions:** Grant appropriate Identity and Access Management (IAM) roles to the service account or user that will execute the BigQuery Stored Procedure. This typically includes:
    *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on the `project.dataset` for creating/updating tables and procedures, and inserting/updating data.
    *   `BigQuery Job User` for running BigQuery jobs.
3.  **Deploy Audit Tables:** Execute the DDL scripts for the `job_control` and `job_error_log` tables in the target BigQuery dataset:
    ```bash
    bq query --use_legacy_sql=false < project/dataset/job_control.sql
    bq query --use_legacy_sql=false < project/dataset/job_error_log.sql
    ```
4.  **Deploy Stored Procedures:** Deploy the generated stored procedures to the target BigQuery dataset:
    ```bash
    bq query --use_legacy_sql=false < project/dataset/sp_k_ausd_v_ta_discount.sql
    bq query --use_legacy_sql=false < project/dataset/sp_vertragsdatenabgleich_ta_discount.sql
    ```
5.  **External Orchestration Configuration:** If an external orchestrator (e.g., Cloud Composer/Airflow, Workflows, Cloud Scheduler) is used to trigger the job, update its configuration to call the new BigQuery Stored Procedure:
    *   Replace calls to `r_ausd_v_ta_discount.ksh` with `CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(...)`.
    *   Ensure the orchestrator has the necessary BigQuery connection details and IAM permissions.
6.  **Secrets Management (if applicable):** If the original `k_ausd_v_ta_discount.ksh` or any of its dependencies used secrets, ensure these are securely managed (e.g., in Google Secret Manager) and accessible by the BigQuery environment or the orchestrator. (No explicit secrets identified for this wrapper script itself).

## 5. Known Gaps & Unresolved References

*   **Core Logic Migration (B4 Item):** The most significant gap is that the actual data reconciliation logic within `k_ausd_v_ta_discount.ksh` has not been migrated. `project.dataset.sp_k_ausd_v_ta_discount` is currently a placeholder. Its detailed design and migration are a critical follow-up item (categorized as a B4 item, requiring further design and development).
*   **Missing Metadata:** The original `file_complexity` and `automation_rate` metadata for `r_ausd_v_ta_discount.ksh` were unavailable, which could impact the overall assessment of migration effort and future maintenance.
*   **Environment Sourcing (`.dw_init`):** The full impact of replacing the sourced `.dw_init` file is not entirely clear. Any environment variables or configurations set by this file that are critical for `sp_k_ausd_v_ta_discount` will need to be explicitly managed (e.g., as BigQuery procedure parameters, configuration tables, or within the core logic's implementation).
*   **Dynamic Script Invocation:** If `k_ausd_v_ta_discount.ksh` was ever dynamically invoked or its parameters were highly complex/variable, the current static `CALL` statement might need adjustment. This will be clarified during the core logic migration.
*   **Comprehensive Error Handling:** While BigQuery's `EXCEPTION WHEN ERROR` covers SQL errors, the original shell script might have handled specific OS-level errors or signals. These are not directly translated and might require external orchestration for full parity.
*   **`f_alis_msgerr.ksh` Full Parity:** The `sp_log_error` helper procedure is a simplified version of `f_alis_msgerr.ksh`. If the original utility had more complex logging levels, message formatting, or notification mechanisms, these would need to be enhanced in the BigQuery implementation.

## 6. Validation

To validate the successful migration of the `r_ausd_v_ta_discount.ksh` wrapper script:

**How to Run Tests:**

1.  **Direct BigQuery Call:** Execute the stored procedure directly from the BigQuery console or via the `bq query` command-line tool.
    ```sql
    -- Test with valid parameters (replace with actual values)
    CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
      p_stichtag => '20231026',
      p_logfile_suffix => 'TEST_RUN',
      p_job_identifier => 'TA_DISCOUNT_JOB'
    );

    -- Test help option
    CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(p_help => TRUE);

    -- Test missing mandatory parameter (should raise an error)
    CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(
      p_stichtag => '20231026',
      p_job_identifier => NULL -- or ''
    );
    ```
2.  **Simulate Core Logic Failure:** Temporarily modify `project.dataset.sp_k_ausd_v_ta_discount` to `RAISE BQ_EXCEPTION 'Simulated error from core logic for testing purposes.';` to test the error handling of the wrapper. Then call `sp_vertragsdatenabgleich_ta_discount` again.
3.  **Check Audit Tables:** After each test run, query `project.dataset.job_control` and `project.dataset.job_error_log` to inspect the recorded job status and error details.

**What "Passing" Means:**

*   **Successful Execution:**
    *   The `job_control` table contains a new entry for the job with `job_kennung` matching the input, `script_name` as 'r_ausd_v_ta_discount', and `status = 'OK'`.
    *   `created_ts` and `finished_ts` are populated.
    *   The `sp_k_ausd_v_ta_discount` placeholder is successfully called (no error raised from it).
*   **Help Option:**
    *   Calling with `p_help => TRUE` should print the usage message and `RETURN` without inserting into `job_control` or `job_error_log`.
*   **Error Handling (e.g., Missing Parameter):**
    *   The `job_control` table should contain an entry with `status = 'ERROR'`.
    *   The `job_error_log` table should contain a corresponding entry with `error_message` detailing the specific error (e.g., "Job identifier (-j) is a mandatory parameter.").
    *   The stored procedure call should terminate with an error, which can be caught by an external orchestrator.
*   **Core Logic Failure Simulation:**
    *   The `job_control` table should show `status = 'ERROR'`.
    *   The `job_error_log` table should contain an entry reflecting the simulated error message from `sp_k_ausd_v_ta_discount`.

## 7. Rollback Procedure

In case of critical issues or failure during go-live, the following rollback procedure can be executed:

1.  **Revert Orchestration:** Update any external orchestrators (e.g., Cloud Composer DAGs, Cloud Scheduler jobs) to revert to calling the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh` script. Ensure the original script and its environment are still available and functional.
2.  **Monitor Original Job:** Verify that the original job is running successfully after the rollback.
3.  **Optional: Remove BigQuery Artifacts:** If the migrated BigQuery objects are not intended for further use or testing, they can be dropped:
    ```bash
    DROP PROCEDURE IF EXISTS project.dataset.sp_vertragsdatenabgleich_ta_discount;
    DROP PROCEDURE IF EXISTS project.dataset.sp_k_ausd_v_ta_discount;
    DROP PROCEDURE IF EXISTS project.dataset.sp_log_error; -- If it was created as a separate procedure
    DROP TABLE IF EXISTS project.dataset.job_control;
    DROP TABLE IF EXISTS project.dataset.job_error_log;
    ```
    *Note: Only drop if these tables/procedures are not shared or used by other processes.*