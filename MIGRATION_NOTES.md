# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `k_ausd_v_ta_period.ksh`, located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh`, has been migrated.

**Original Purpose:** This script served as an orchestration layer, handling parameter parsing, environment setup, error logging, job activation/deactivation, and the execution of a core SQL script (`d_ausd_v_ta_period.sql`) which performs data manipulation on the `ta_period` table. It also managed record counts and job status.

**Target Platform:** The script's functionality has been re-implemented within the Google Cloud BigQuery ecosystem. The orchestration logic, parameter handling, error management, and job status tracking are now encapsulated in a BigQuery Stored Procedure. The underlying SQL logic from `d_ausd_v_ta_period.sql` is expected to be translated into BigQuery Standard SQL and integrated into this procedure.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL files:

*   **`sql/ddl/job_table.sql`**
    *   **Role:** This DDL script creates the `job_table` in BigQuery. This table is central to managing the execution status, active flags, and record counts for various jobs, replacing the temporary file-based and implicit job tracking mechanisms of the original KornShell script. It provides a persistent and queryable record of job executions.
*   **`sql/ddl/error_log.sql`**
    *   **Role:** This DDL script creates the `error_log` table in BigQuery. This table serves as a centralized repository for capturing error messages and details during the execution of the migrated stored procedure, replacing the `DWMSG_MeldeFehler` calls and shell-based logging of the original script.
*   **`sql/procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** This BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control`) is the core migrated artifact. It encapsulates the entire orchestration logic of the original `k_ausd_v_ta_period.ksh` script, including:
        *   Receiving and validating input parameters (`p_JobKennung`, `p_EintragsNr`).
        *   Managing job activation/deactivation and status updates in the `job_table`.
        *   Executing the core data manipulation logic (which will be derived from `d_ausd_v_ta_period.sql`).
        *   Capturing record counts.
        *   Handling and logging errors to the `error_log` table.

## 3. Key Design Decisions

The migration strategy involved several key design decisions to leverage BigQuery's capabilities and address the limitations of the original KornShell script:

*   **Orchestration Logic: KornShell to BigQuery Stored Procedure:**
    *   **Why:** Migrating the control flow from a shell script to a BigQuery Stored Procedure centralizes the logic within the data platform. This eliminates external dependencies on shell environments, simplifies deployment, and allows for direct, native execution of SQL statements without external client wrappers (like SQL*Plus). It also provides better integration with BigQuery's scheduling and monitoring tools.
    *   **Trade-offs:** Requires re-implementing shell-specific constructs (e.g., parameter parsing, file I/O, environment sourcing) using BigQuery's procedural SQL capabilities.
*   **Job Status Management: Filesystem/Implicit to Dedicated BigQuery Table (`job_table`):**
    *   **Why:** The original script used temporary files and implicit mechanisms for job tracking. A dedicated `job_table` in BigQuery provides a durable, queryable, and consistent way to manage job states, active flags, and record counts. This allows for easier monitoring, auditing, and debugging of job executions.
    *   **Trade-offs:** Requires defining and maintaining a new BigQuery table schema.
*   **Error Handling: `DWMSG_MeldeFehler` to BigQuery `error_log` Table:**
    *   **Why:** Replacing shell-based error messaging with structured logging into a BigQuery `error_log` table provides a centralized, queryable, and standardized way to capture error details. This facilitates easier analysis, alerting, and integration with other monitoring tools.
    *   **Trade-offs:** Requires defining and maintaining a new BigQuery table schema.
*   **Parameter Handling: Shell Arguments to Stored Procedure Parameters:**
    *   **Why:** Direct parameters in a BigQuery Stored Procedure offer type safety, clear input definitions, and eliminate the need for manual parsing logic (like `getopts` in shell).
*   **Temporary File Handling: Eliminated:**
    *   **Why:** The use of temporary files for record counting (`$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp`) is no longer necessary. BigQuery's procedural language allows for direct variable assignment (`DECLARE`, `SET`) and `COUNT(*)` operations within the same procedure, leveraging its in-memory processing capabilities.
*   **SQL Execution: `starteSQLSkript` Wrapper to Direct BigQuery SQL:**
    *   **Why:** The `starteSQLSkript` wrapper, likely used for executing SQL*Plus, is replaced by direct execution of BigQuery Standard SQL within the stored procedure (either inline or via `EXECUTE IMMEDIATE`). This removes the dependency on external database clients and optimizes performance by keeping execution within BigQuery.

## 4. Manual Steps Before Go-Live

Before the migrated solution can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` as referenced in the generated code) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
2.  **Deploy DDL for Job and Error Tables:**
    *   Execute the DDL scripts to create the `job_table` and `error_log` tables in your target BigQuery dataset:
        ```bash
        bq query --use_legacy_sql=false < sql/ddl/job_table.sql
        bq query --use_legacy_sql=false < sql/ddl/error_log.sql
        ```
3.  **Deploy BigQuery Stored Procedure:**
    *   Execute the SQL script to create the `r_ausd_vertrag_control` stored procedure:
        ```bash
        bq query --use_legacy_sql=false < sql/procedures/r_ausd_vertrag_control.sql
        ```
4.  **Migrate `d_ausd_v_ta_period.sql` Content:**
    *   **Crucial Step:** The placeholder for the core SQL logic within `sql/procedures/r_ausd_vertrag_control.sql` (marked `-- Core SQL logic migrated from d_ausd_v_ta_period.sql`) **must be replaced** with the actual BigQuery Standard SQL translation of `d_ausd_v_ta_period.sql`. This may involve:
        *   Translating Oracle-specific SQL constructs to BigQuery Standard SQL.
        *   Identifying and updating table/column names to match BigQuery schemas.
        *   Ensuring the record counting logic (`SET v_records = ...`) accurately reflects the number of rows processed by the migrated SQL.
    *   After updating the procedure, redeploy it using the `bq query` command as above.
5.  **IAM Permissions:**
    *   The Google Cloud service account or user identity that will execute the BigQuery Stored Procedure needs the following IAM roles/permissions:
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) on the `project.dataset` to `INSERT`, `UPDATE`, `SELECT` from `job_table`, `error_log`, and any target tables modified by the `d_ausd_v_ta_period.sql` logic.
        *   `BigQuery Job User` (roles/bigquery.jobUser) to run BigQuery jobs.
        *   `BigQuery Metadata Viewer` (roles/bigquery.metadataViewer) to view dataset and table metadata.
6.  **Scheduling:**
    *   If the procedure needs to run on a schedule, configure a BigQuery Scheduled Query to call `CALL `project.dataset.r_ausd_vertrag_control`('YOUR_JOBKENNUNG', 'YOUR_EINTRAGSNR');`.
    *   Alternatively, if it's part of a larger workflow, integrate it into a Cloud Composer (Apache Airflow) DAG using the `BigQueryOperator`.

## 5. Known Gaps & Unresolved References

*   **`d_ausd_v_ta_period.sql` Content (B4 Item):** The most significant gap is the actual SQL logic from `d_ausd_v_ta_period.sql`. The generated stored procedure includes a placeholder for this. This SQL needs to be thoroughly analyzed, translated to BigQuery Standard SQL, and integrated into the `r_ausd_vertrag_control` procedure. This is a critical prerequisite for the solution to be functional.
*   **`DWMSG_MeldeFehler` Details:** While the error logging mechanism has been replaced by the `error_log` table, the exact error codes and message formats used by the original `DWMSG_MeldeFehler` utility are not fully known. If downstream systems depend on specific error codes or message patterns, these might need to be mapped or replicated in the `error_log` table.
*   **`starteSQLSkript` Full Functionality:** The `starteSQLSkript` wrapper in the original script might have contained complex logic beyond simple SQL execution (e.g., specific connection handling, pre/post-execution hooks, advanced error propagation). While the migration aims to cover the core aspects, any subtle or undocumented behaviors of this wrapper might need further investigation if issues arise.
*   **`h_alis_job.ksh`:** The commented-out reference to `h_alis_job.ksh` in the original script suggests potential job management logic that was either unused or disabled. For this migration, it was assumed to be out of scope due to its commented status. If future requirements emerge, this might need re-evaluation.
*   **`project.dataset.result_table` for Record Count:** The example record counting logic in the stored procedure uses a placeholder `project.dataset.result_table`. This needs to be updated to reflect the actual target table(s) where the migrated `d_ausd_v_ta_period.sql` logic writes its output, and the `WHERE` clause for counting should be adjusted accordingly.

## 6. Validation

Validation ensures the migrated solution functions correctly and produces the expected results.

**How to Run Tests:**

1.  **Unit Test Procedure with Parameters:**
    *   Execute the `r_ausd_vertrag_control` stored procedure directly in BigQuery with various valid and invalid parameter combinations.
    *   **Valid Call Example:**
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', 'ENTRY_123');
        ```
    *   **Invalid Call Example (missing parameter):**
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`('JOB_B', NULL);
        ```
2.  **Monitor `job_table`:**
    *   After each execution, query the `project.dataset.job_table` to verify that job statuses, active flags, creation/completion timestamps, and record counts are updated correctly.
3.  **Monitor `error_log`:**
    *   After executions with invalid parameters or simulated failures, query the `project.dataset.error_log` to ensure error messages are captured with correct codes, arguments, and timestamps.
4.  **Data Validation:**
    *   After successful execution (once `d_ausd_v_ta_period.sql` is fully migrated and integrated), query the target data tables in BigQuery to confirm that the data transformations performed by the procedure are accurate and match the expected output of the original `d_ausd_v_ta_period.sql` script. This may involve comparing row counts, specific column values, or checksums.
5.  **Concurrency Testing:**
    *   If the job is expected to run concurrently, test multiple simultaneous calls to ensure the `job_table`'s active flag logic correctly handles concurrent requests and prevents unintended parallel processing of the same job.

**What "Passing" Means:**

*   The `r_ausd_vertrag_control` stored procedure completes without unhandled BigQuery errors for valid inputs.
*   For successful runs, the `project.dataset.job_table` shows:
    *   `active_flag` set to `FALSE` upon completion.
    *   `created_ts`, `updated_ts`, and `completed_ts` are populated correctly.
    *   `record_count` accurately reflects the number of records processed by the integrated `d_ausd_v_ta_period.sql` logic.
*   For invalid inputs (e.g., missing parameters), the procedure logs an appropriate error to `project.dataset.error_log` and exits gracefully without processing data.
*   The data in the target BigQuery tables, after the procedure's execution, is identical to or functionally equivalent to the data produced by the original `k_ausd_v_ta_period.ksh` script and its underlying `d_ausd_v_ta_period.sql`.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after deploying the migrated solution, follow these steps to revert to the original system:

1.  **Stop New Executions:**
    *   If using BigQuery Scheduled Queries, disable or delete the scheduled query that invokes `r_ausd_vertrag_control`.
    *   If using Cloud Composer/Airflow, pause or disable the DAG that includes the `r_ausd_vertrag_control` task.
2.  **Revert to Original Script:**
    *   Ensure the original `k_ausd_v_ta_period.ksh` script and its dependencies are fully operational and can be executed.
    *   Resume execution of the original KornShell script through its established scheduling mechanism.
3.  **Isolate BigQuery Artifacts (Optional but Recommended):**
    *   To prevent accidental re-execution or interference, consider renaming or dropping the BigQuery Stored Procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_vertrag_control`;
        ```
    *   If the `job_table` and `error_log` are *only* used by this migrated job, they can also be dropped (after backing up any necessary historical data):
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_table`;
        DROP TABLE IF EXISTS `project.dataset.error_log`;
        ```
    *   **Caution:** Do not drop these tables if other migrated processes depend on them.
4.  **Analyze and Redesign:**
    *   Investigate the root cause of the failure in the BigQuery migration.
    *   Address the identified issues, potentially involving a redesign of the BigQuery Stored Procedure or the `d_ausd_v_ta_period.sql` translation.
    *   Once issues are resolved, repeat the "Manual Steps Before Go-Live" and "Validation" procedures.