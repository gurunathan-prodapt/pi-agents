# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_bp_ref.ksh`, an orchestration/wrapper script for a contract data reconciliation job. The script's primary function is to prepare the runtime environment, validate parameters, initialize logging, and invoke a core processing script.

The migration targets Google BigQuery. The original KornShell wrapper script has been re-engineered into a BigQuery Stored Procedure named `vertragsdatenabgleich_wrapper`. File-based logging has been replaced by a dedicated BigQuery audit table, `job_audit_log`. The core processing logic, originally in `k_ausd_v_ta_bp_ref.ksh`, is assumed to be migrated into a separate BigQuery Stored Procedure, which the wrapper will then call.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/ddl/job_audit_log.sql`**
    *   **Role:** This DDL (Data Definition Language) script creates the `job_audit_log` table in BigQuery. This table serves as the central repository for all job execution logs, status updates, and error messages, replacing the file-based logging of the original KornShell script.
*   **`sql/stored_procedures/vertragsdatenabgleich_wrapper.sql`**
    *   **Role:** This SQL script defines the BigQuery Stored Procedure `vertragsdatenabgleich_wrapper`. It encapsulates the orchestration logic of the original `r_ausd_v_ta_bp_ref.ksh` script, including parameter handling, environment setup (via `DECLARE` statements), logging to `job_audit_log`, and invoking the core processing logic (via a `CALL` to `k_ausd_v_ta_bp_ref`). It also includes robust error handling.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Wrapper Logic:** The KornShell wrapper script's orchestration nature (parameter parsing, logging, invoking another script) maps well to BigQuery's SQL scripting capabilities. Using a Stored Procedure (`vertragsdatenabgleich_wrapper`) keeps the logic within the BigQuery ecosystem, leveraging its native features for control flow and error handling, and simplifying deployment and scheduling compared to external shell scripts.
*   **Dedicated Audit Log Table:** Replacing file-based logging with a BigQuery table (`job_audit_log`) centralizes logging, enables easier querying and analysis of job execution history, and integrates seamlessly with BigQuery's data warehousing capabilities. This provides a structured, queryable audit trail.
*   **Parameter-based Input:** The original `getopts` command-line parameter parsing is translated directly into BigQuery Stored Procedure input parameters (`p_s`, `p_l`, `p_help`). This is the idiomatic way to pass arguments to BigQuery procedures.
*   **BigQuery Native Error Handling:** The `set -eu` and `trap` mechanisms of KornShell are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides robust, structured error management within the SQL context, allowing for detailed error logging to the `job_audit_log` table.
*   **Placeholder for Core Logic:** The design explicitly assumes the core processing script (`k_ausd_v_ta_bp_ref.ksh`) will also be migrated to a BigQuery Stored Procedure. This allows the wrapper to maintain its orchestration role by simply `CALL`ing the new BigQuery core procedure, ensuring a clean separation of concerns.
*   **UUID for Job Instance ID:** `GENERATE_UUID()` is used to create a unique `job_instance_id` for each execution, replacing the original `DW_EintragsNr` concept. This ensures uniqueness and traceability for each job run in the audit log.

## 4. Manual Steps Before Go-Live

Before the migrated job can be executed in BigQuery, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure a Google Cloud Project is set up.
    *   Create a BigQuery Dataset (e.g., `your_dataset_id`) where the `job_audit_log` table and the `vertragsdatenabgleich_wrapper` stored procedure will reside. Replace `your_project_id` and `your_dataset_id` placeholders in the generated SQL with actual values.
2.  **IAM Permissions:**
    *   The service account or user executing the stored procedure must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on the target dataset to create/update tables and stored procedures, and to insert/update data into `job_audit_log`.
        *   `BigQuery Job User` to run jobs (including stored procedures).
        *   Permissions to call the `k_ausd_v_ta_bp_ref` stored procedure (once it's migrated and deployed).
3.  **Deploy `job_audit_log` Table:**
    *   Execute the DDL script `sql/ddl/job_audit_log.sql` in your BigQuery environment to create the audit log table.
4.  **Deploy `vertragsdatenabgleich_wrapper` Stored Procedure:**
    *   Execute the DDL script `sql/stored_procedures/vertragsdatenabgleich_wrapper.sql` in your BigQuery environment to create the stored procedure.
5.  **Deploy Core Logic Stored Procedure (`k_ausd_v_ta_bp_ref`):**
    *   **Crucial Dependency:** The `vertragsdatenabgleich_wrapper` procedure calls `your_project_id.your_dataset_id.k_ausd_v_ta_bp_ref`. This core procedure must be migrated and deployed to BigQuery *before* the wrapper can function correctly. Its specific parameters (`v_JobKennung`, `v_job_instance_id`) are assumed based on the wrapper's invocation.
6.  **Scheduling:**
    *   The execution of `vertragsdatenabgleich_wrapper` will need to be scheduled using a BigQuery-compatible orchestrator. Options include:
        *   **Cloud Composer (Apache Airflow):** Recommended for complex workflows, providing robust scheduling, monitoring, and dependency management.
        *   **Cloud Scheduler + Cloud Functions:** For simpler, time-based triggers that can invoke the BigQuery stored procedure.
        *   **BigQuery Scheduled Queries:** While primarily for queries, they can be adapted to call stored procedures.

## 5. Known Gaps & Unresolved References

*   **Core Script (`k_ausd_v_ta_bp_ref.ksh`) Logic:** The migration of the wrapper is dependent on the successful migration of the core script `k_ausd_v_ta_bp_ref.ksh` into a BigQuery Stored Procedure. The current design includes a placeholder `CALL` statement. The specific parameters and return values of the core procedure need to be finalized during its migration.
*   **Unsupported Shell Features in Core Script:** This migration focused on the wrapper. If the core script or any of its sourced utilities contain complex shell-specific operations (e.g., advanced filesystem manipulation, external system calls not directly supported by BigQuery SQL), these will require further analysis and potentially alternative solutions (e.g., Cloud Functions, Cloud Run, Dataflow using Python) to encapsulate and execute those parts.
*   **Environmental Variables (`BERT_DIR_ROOT`):** The original script relied on environment variables like `BERT_DIR_ROOT`. In the BigQuery environment, these are replaced by explicit parameters or hardcoded values within the stored procedure. If `BERT_DIR_ROOT` was used to dynamically locate other scripts or resources, this dynamic resolution needs to be re-evaluated and potentially replaced with explicit BigQuery object references or configuration lookups.
*   **`p_s` and `p_l` Parameter Usage:** The specific meaning and validation rules for the `p_s` and `p_l` parameters are determined by the core script (`k_ausd_v_ta_bp_ref`). The wrapper currently passes them through without specific validation beyond their presence. This might need refinement once the core script's requirements are fully understood.

## 6. Validation

To validate the migrated `vertragsdatenabgleich_wrapper` stored procedure:

1.  **Prerequisites:** Ensure the `job_audit_log` table and the `vertragsdatenabgleich_wrapper` stored procedure are deployed. For a full end-to-end test, a placeholder `k_ausd_v_ta_bp_ref` stored procedure (even a simple one that just logs its invocation) should also be deployed.

2.  **Test Cases:**

    *   **Help Message Display:**
        ```sql
        CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(p_s => NULL, p_l => NULL, p_help => TRUE);
        ```
        *   **Passing:** A usage message should be returned in the query results, and an entry with `message_type = 'USAGE'` should be present in `job_audit_log`.

    *   **Successful Execution (with placeholder core script):**
        ```sql
        CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(p_s => 'test_s_value', p_l => 'test_l_value', p_help => FALSE);
        ```
        *   **Passing:**
            *   The query should complete without error.
            *   The `job_audit_log` table should contain multiple entries for the `job_instance_id` generated during this run:
                *   An initial `status = 'STARTED'` entry.
                *   `message_type = 'INFO'` entries for job details and start.
                *   An `INFO` entry indicating the core script completed successfully.
                *   The final entry for that `job_instance_id` should have `status = 'SUCCESS'` and `end_timestamp` populated.
            *   If the placeholder `k_ausd_v_ta_bp_ref` logs its invocation, verify that log entry as well.

    *   **Error Handling (simulating core script failure):**
        *   Modify the placeholder `k_ausd_v_ta_bp_ref` to intentionally raise an error (e.g., `RAISE 'Simulated error from core script';`).
        ```sql
        CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(p_s => 'error_s', p_l => 'error_l', p_help => FALSE);
        ```
        *   **Passing:**
            *   The `CALL` statement should result in an error being raised to the caller.
            *   The `job_audit_log` table should contain:
                *   An initial `status = 'STARTED'` entry.
                *   An `message_type = 'ERROR'` entry detailing the simulated error, with `error_code` and `error_argument` populated.
                *   The final entry for that `job_instance_id` should have `status = 'FAILED'` and `end_timestamp` populated.

3.  **"Passing" Criteria:**
    *   The stored procedure executes without unexpected BigQuery errors.
    *   The `job_audit_log` table accurately reflects the start, end, status (SUCCESS/FAILED), and detailed messages for each execution.
    *   Parameters are correctly passed to and from the wrapper and the (placeholder) core procedure.
    *   The usage message is displayed correctly when `p_help` is TRUE.

## 7. Rollback Procedure

In case of issues or a decision to revert the migration, follow these steps:

1.  **Stop Scheduling:** Immediately halt any scheduled executions of the `vertragsdatenabgleich_wrapper` BigQuery Stored Procedure (e.g., disable Cloud Composer DAGs, Cloud Scheduler jobs).
2.  **Re-enable Original Job:** Re-enable the original `r_ausd_v_ta_bp_ref.ksh` KornShell script in its legacy environment.
3.  **Delete BigQuery Stored Procedure:**
    ```sql
    DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`;
    ```
4.  **Delete `job_audit_log` Table (Optional but Recommended):**
    *   If the `job_audit_log` table was created solely for this migration and is not used by other processes, it can be deleted.
    *   **Caution:** If other migrated jobs also use this table, do not delete it.
    ```sql
    DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_audit_log`;
    ```
5.  **Delete Core Logic Stored Procedure (if applicable):**
    *   If the `k_ausd_v_ta_bp_ref` BigQuery Stored Procedure was also deployed as part of this migration, it should be dropped as well.
    ```sql
    DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.k_ausd_v_ta_bp_ref`;
    ```
6.  **Verify Original Job Functionality:** Confirm that the original KornShell script is running as expected in the legacy environment.