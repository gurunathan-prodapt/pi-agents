# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `r_ausd_bp_ta_iccid_vertrag.ksh` from a UNIX-based environment to Google BigQuery. The original script, responsible for parsing parameters, setting up logging, and invoking a core data processing script, has been re-engineered into BigQuery Stored Procedures and supporting logging/control tables.

**Original Asset:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh` (KornShell script)
**Target Platform:** Google BigQuery

## 2. Generated Artifacts

The migration produced the following BigQuery SQL artifacts:

*   **`project/dataset/job_control.sql`**
    *   **Role:** DDL script to create the `job_control` table. This table serves as the central repository for tracking the execution metadata and status of migrated jobs, including `job_entry_nr`, `job_name`, `stichtag`, `wiederanlaufwert`, `start_timestamp`, `end_timestamp`, and `status`. It replaces the job control aspects previously handled by the KornShell script's internal variables and status updates.

*   **`project/dataset/job_run_log.sql`**
    *   **Role:** DDL script to create the `job_run_log` table. This table stores detailed, timestamped log messages (`log_level`, `message`) for each job execution, linked via `job_entry_nr`. It replaces the file-based logging (`$LogDatei`) of the original KornShell script.

*   **`project/dataset/job_error_log.sql`**
    *   **Role:** DDL script to create the `job_error_log` table. This table is dedicated to recording specific error details (`error_message`, `stack_trace`, relevant parameters) when a job encounters an issue. It provides structured error reporting, replacing ad-hoc error messages in log files.

*   **`project/dataset/job_usage_log.sql`**
    *   **Role:** DDL script to create the `job_usage_log` table. This table captures instances where invalid parameters are provided, logging usage instructions or validation errors. It replaces the shell script's mechanism for displaying usage messages and exiting.

*   **`project/dataset/k_ausd_bp_ta_iccid_vertrag.sql`**
    *   **Role:** BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_iccid_vertrag`). This is a **placeholder** for the core data processing logic that was originally contained within `k_ausd_bp_ta_iccid_vertrag.ksh`. It accepts `job_entry_nr`, `p_stichtag`, and `p_wiederanlaufWert` as parameters. Its implementation is crucial for the complete functionality of the job but is outside the scope of this specific migration. Currently, it logs a message indicating its execution.

*   **`project/dataset/ausd_bp_ta_iccid_vertrag_wrapper.sql`**
    *   **Role:** BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`). This procedure is the direct migration of the `r_ausd_bp_ta_iccid_vertrag.ksh` orchestration logic. It handles parameter parsing, defaulting, validation, job control, logging, and error handling. It orchestrates the execution by calling the `project.dataset.k_ausd_bp_ta_iccid_vertrag` stored procedure.

## 3. Key Design Decisions

*   **Orchestration to BigQuery Stored Procedure:** The entire control flow, parameter handling, and job management logic of the KornShell script were translated into a BigQuery Stored Procedure (`ausd_bp_ta_iccid_vertrag_wrapper`). This centralizes the job's execution within BigQuery, leveraging its native scripting capabilities for control flow, variables, and error handling.
*   **Table-based Logging and Job Control:** Instead of file-based logging and ad-hoc status tracking, dedicated BigQuery tables (`job_control`, `job_run_log`, `job_error_log`, `job_usage_log`) were introduced. This provides structured, queryable, and centralized auditing and monitoring capabilities for job executions, aligning with modern data warehousing practices.
*   **Separation of Orchestration and Core Logic:** The design maintains a clear separation between the orchestration (wrapper) and the core data processing logic. The `ausd_bp_ta_iccid_vertrag_wrapper` handles job setup and invocation, while `k_ausd_bp_ta_iccid_vertrag` is designated for the actual data manipulation. This modularity improves maintainability and allows for independent development and testing of the core logic.
*   **BigQuery Native Error Handling:** The KornShell `trap` commands for error handling were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides robust, structured error management within the SQL context, allowing for specific error logging and graceful termination.
*   **Parameter Translation:** KornShell `getopts` and shell variable handling were directly mapped to BigQuery Stored Procedure `IN` parameters and `DECLARE`d variables, with `IFNULL` and `IF` statements replicating defaulting logic. Date functions were replaced by BigQuery's `FORMAT_DATE` and `PARSE_DATE`.
*   **Trade-offs:**
    *   **Core Logic as Placeholder:** The most significant trade-off is that the core data processing logic (`k_ausd_bp_ta_iccid_vertrag`) is currently a placeholder. This means the full end-to-end functionality is not yet migrated, requiring a subsequent, more complex migration effort.
    *   **Increased BigQuery Dependency:** The solution is now entirely dependent on BigQuery for execution and logging, which might require specific BigQuery expertise for troubleshooting compared to general shell scripting.
    *   **Loss of Direct File System Access:** The ability to write arbitrary files (e.g., temporary files, specific reports) is lost, as BigQuery Stored Procedures operate within the BigQuery environment. Any such requirements from the core script would need to be re-evaluated for BigQuery-native solutions (e.g., GCS, BigQuery tables).

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
    ```bash
    bq mk --dataset project:dataset
    ```
2.  **Deploy DDL for Logging and Control Tables:** Execute the DDL scripts to create the `job_control`, `job_run_log`, `job_error_log`, and `job_usage_log` tables in the target BigQuery dataset.
    ```bash
    bq query --use_legacy_sql=false < project/dataset/job_control.sql
    bq query --use_legacy_sql=false < project/dataset/job_run_log.sql
    bq query --use_legacy_sql=false < project/dataset/job_error_log.sql
    bq query --use_legacy_sql=false < project/dataset/job_usage_log.sql
    ```
3.  **Deploy Stored Procedures:** Execute the DDL scripts to create the `k_ausd_bp_ta_iccid_vertrag` (placeholder) and `ausd_bp_ta_iccid_vertrag_wrapper` stored procedures.
    ```bash
    bq query --use_legacy_sql=false < project/dataset/k_ausd_bp_ta_iccid_vertrag.sql
    bq query --use_legacy_sql=false < project/dataset/ausd_bp_ta_iccid_vertrag_wrapper.sql
    ```
4.  **IAM Permissions:**
    *   The service account or user executing the `ausd_bp_ta_iccid_vertrag_wrapper` stored procedure must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` to insert/update records in `job_control`, `job_run_log`, `job_error_log`, `job_usage_log`.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
        *   `BigQuery Data Viewer` on any source tables that `k_ausd_bp_ta_iccid_vertrag` might read from (once implemented).
        *   `BigQuery Data Editor` on any target tables that `k_ausd_bp_ta_iccid_vertrag` might write to (once implemented).
5.  **Scheduling Integration:**
    *   Update the existing scheduler (e.g., cron, Airflow/Cloud Composer) to disable the original KornShell script.
    *   Configure a new scheduler entry (e.g., Cloud Composer DAG, Cloud Scheduler job calling a Cloud Function/Run) to invoke the `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper` BigQuery Stored Procedure.
    *   The invocation command will look like:
        ```sql
        CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => 'DDMMYYYY', p_wiederanlaufWert => 0);
        ```
        or with default parameters:
        ```sql
        CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(NULL, NULL);
        ```
6.  **Secrets Management:** No explicit secrets were identified in the wrapper script. However, if the core script (`k_ausd_bp_ta_iccid_vertrag`) requires database credentials or other sensitive information, ensure these are managed securely (e.g., using Google Secret Manager) and passed appropriately to the core procedure (if needed) or configured within the BigQuery environment.

## 5. Known Gaps & Unresolved References

*   **Core Logic of `k_ausd_bp_ta_iccid_vertrag.ksh` (B4 Item):** The most critical gap is the unimplemented core data processing logic. The `project.dataset.k_ausd_bp_ta_iccid_vertrag` stored procedure is a placeholder. A separate, detailed analysis and migration effort is required to translate the actual business logic from the original `k_ausd_bp_ta_iccid_vertrag.ksh` into BigQuery SQL or another suitable BigQuery service (e.g., Dataflow, Dataproc). This is a **Blocker (B4)** for full functionality.
*   **Exact Behavior of Helper Functions:** While standard interpretations were used for functions like `DWMSG_ErmittleNr` or `DWMSG_Logdateiname`, if these helper functions from the original KornShell environment contained highly custom or complex logic beyond basic logging/ID generation, further analysis might be required to ensure exact replication in BigQuery.
*   **Commented-out Logic (`FOSHoleLadedatum`):** The original script contained commented-out logic for deriving `p_stichtag` based on `maxladedatum` from `DWH$TA_C_VERTRAG`. If this logic is ever re-enabled or deemed necessary, it would require identifying the BigQuery equivalent of `DWH$TA_C_VERTRAG` and its `maxladedatum` column, and integrating that into the `ausd_bp_ta_iccid_vertrag_wrapper`'s date defaulting logic.
*   **System Exit Codes:** The original shell script used specific exit codes. In BigQuery, errors are handled via `RAISE` and the job's overall success/failure status. If specific numeric exit codes are consumed by downstream systems, a custom error code logging mechanism might be needed in `job_error_log` or `job_control`.

## 6. Validation

To validate the migrated `ausd_bp_ta_iccid_vertrag_wrapper` stored procedure, execute it with various parameter combinations and verify the state of the BigQuery logging and control tables.

**How to Run Tests:**

1.  **Valid Parameters:**
    ```sql
    -- Test 1: Explicit Stichtag and Wiederanlaufwert
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => '01012023', p_wiederanlaufWert => 100);

    -- Test 2: Default Wiederanlaufwert (NULL)
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => '15062023', p_wiederanlaufWert => NULL);

    -- Test 3: Default Stichtag (NULL)
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => NULL, p_wiederanlaufWert => 50);

    -- Test 4: Both parameters default (NULL)
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(NULL, NULL);
    ```
2.  **Invalid Parameters:**
    ```sql
    -- Test 5: Invalid Stichtag format
    CALL project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(p_stichtag => '2023-01-01', p_wiederanlaufWert => 10);

    -- Test 6: Non-numeric Wiederanlaufwert (if BigQuery allowed this, but it's typed as INT64, so this would be a client-side error)
    -- This scenario would typically be caught by the client calling the SP, not the SP itself.
    ```

**What "Passing" Means:**

For each test case, query the `job_control`, `job_run_log`, `job_error_log`, and `job_usage_log` tables to verify the following:

*   **`job_control` Table:**
    *   A new entry with an incremented `job_entry_nr` is created for each execution.
    *   `job_name`, `stichtag`, `wiederanlaufwert`, `start_timestamp` are correctly populated.
    *   For successful runs (Tests 1-4): `status` is 'OK' and `end_timestamp` is populated.
    *   For failed runs (Test 5): `status` is 'ERROR' and `end_timestamp` is populated.
*   **`job_run_log` Table:**
    *   Contains a sequence of log messages for the corresponding `job_entry_nr`, including job start, parameter logging, core procedure invocation, and job completion/failure messages.
    *   Log levels (`INFO`, `ERROR`) are correctly assigned.
*   **`job_error_log` Table:**
    *   For failed runs (Test 5): A new entry is created with `error_timestamp`, `job_entry_nr`, `error_message` (e.g., "Validation Error: Invalid Stichtag format."), and `stack_trace`.
    *   For successful runs (Tests 1-4): No new entries are created.
*   **`job_usage_log` Table:**
    *   For failed runs due to invalid parameters (Test 5): A new entry is created with `usage_timestamp`, `job_entry_nr`, `message` (e.g., "Usage: CALL...Invalid Stichtag..."), and the `provided_stichtag`/`provided_wiederanlaufwert`.
    *   For successful runs (Tests 1-4): No new entries are created.
*   **Core Procedure Invocation:** Verify that the `project.dataset.k_ausd_bp_ta_iccid_vertrag` procedure is called with the correct `job_entry_nr`, `p_stichtag`, and `p_wiederanlaufWert` parameters (as indicated by the `job_run_log` entry for core procedure invocation).
*   **Error Propagation:** Ensure that when an error occurs (e.g., invalid `p_stichtag`), the `ausd_bp_ta_iccid_vertrag_wrapper` procedure terminates with an error, and the calling environment (e.g., `bq query` command) reports a failure.

## 7. Rollback Procedure

In case of issues with the migrated BigQuery job, the following steps outline the rollback procedure to revert to the original KornShell script:

1.  **Disable BigQuery Orchestration:**
    *   Immediately disable or remove the new scheduler entry (e.g., Cloud Composer DAG, Cloud Scheduler job) that invokes `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`. This prevents any further execution of the migrated job.
2.  **Re-enable Original Scheduler:**
    *   Re-enable the original scheduler entry (e.g., cron job, Airflow DAG) that was responsible for executing `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh`.
3.  **Verify Original Job Functionality:**
    *   Monitor the execution of the re-enabled original KornShell script to ensure it runs successfully and produces the expected output. Check its log files and any downstream systems it affects.
4.  **Optional: Clean Up BigQuery Artifacts (if necessary):**
    *   If the rollback is permanent or if the BigQuery artifacts are causing issues, consider dropping the created stored procedures and tables. **Exercise caution as this will delete historical job control and log data.**
    ```bash
    bq rm -f -r project.dataset.ausd_bp_ta_iccid_vertrag_wrapper
    bq rm -f -r project.dataset.k_ausd_bp_ta_iccid_vertrag
    bq rm -f -r project.dataset.job_control
    bq rm -f -r project.dataset.job_run_log
    bq rm -f -r project.dataset.job_error_log
    bq rm -f -r project.dataset.job_usage_log
    ```
    *   It is generally recommended to retain the logging tables for post-mortem analysis unless storage is a critical concern.

This rollback procedure ensures a swift return to the previous stable state while allowing for investigation into the issues encountered with the migrated solution.