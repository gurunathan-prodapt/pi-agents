```markdown
# MIGRATION_NOTES: k_ausd_v_ta_bp_ref.ksh

## 1. Summary

This document outlines the migration of the `k_ausd_v_ta_bp_ref.ksh` job, originally a KornShell script orchestrating an Oracle SQL*Plus script (`d_ausd_v_ta_bp_ref.sql`), to Google Cloud Platform. The job's primary function is to extract, filter, and load data related to `ta_bp_ref` (Business Partner Reference) entities, manage job states, and log execution details.

The migration targets Google BigQuery for data storage and transformation, with the orchestration logic refactored into a BigQuery Stored Procedure. This consolidates the shell scripting and SQL logic into a single, cloud-native component, leveraging BigQuery's capabilities for data processing and logging.

## 2. Generated Artifacts

The migration process generated the following key artifacts:

*   **`sql/stored_procedures/k_ausd_v_ta_bp_ref_sp.sql`**:
    *   **Role**: This BigQuery Stored Procedure replaces the original `k_ausd_v_ta_bp_ref.ksh` shell script and `d_ausd_v_ta_bp_ref.sql` SQL*Plus script. It encapsulates the entire job logic, including:
        *   Parameter parsing and validation (`p_JobKennung`, `p_EintragsNr`).
        *   Job state management (checking for active jobs, deactivating old ones, updating `job_control` table).
        *   Determination of the processing date (`v_datum`) from `dwtk_meldungen`.
        *   Truncation of the target table (`sof_ta_bp_ref`).
        *   The core `INSERT...SELECT` data transformation from `cds_ta_bp_ref` to `sof_ta_bp_ref`.
        *   Record counting and logging of job status, processed records, and errors to `job_control` and `job_error_log` tables.
    *   **Deployment**: This SQL file defines the BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_bp_ref_sp` which must be deployed to the target BigQuery dataset.

In addition to the stored procedure, the migration implicitly requires the creation of several BigQuery tables:

*   **`project.dataset.dwtk_meldungen`**: BigQuery table mirroring the Oracle `isbert_schema.dwtk_meldungen` table.
*   **`project.dataset.cds_ta_bp_ref`**: BigQuery table mirroring the Oracle `cds$ta_bp_ref` table.
*   **`project.dataset.sof_ta_bp_ref`**: BigQuery table mirroring the Oracle `sof$ta_bp_ref` table, serving as the target for the data transformation.
*   **`project.dataset.job_control`**: A new BigQuery table to track the status, start/end times, processed records, and other metadata for each job run, replacing the shell script's internal state management.
*   **`project.dataset.job_error_log`**: A new BigQuery table to log errors encountered during job execution, replacing the shell script's error reporting mechanisms.

## 3. Key Design Decisions

*   **Consolidation into BigQuery Stored Procedure**:
    *   **Why**: The original job involved a KornShell script orchestrating an Oracle SQL*Plus script. Migrating both components into a single BigQuery Stored Procedure (`k_ausd_v_ta_bp_ref_sp`) simplifies the architecture, reduces inter-process communication overhead, and leverages BigQuery's native scripting capabilities for control flow, error handling, and data manipulation. This aligns with a cloud-native approach, minimizing reliance on external shell environments.
    *   **Trade-offs**: This approach requires re-implementing shell-specific logic (e.g., parameter parsing, environment variable handling, temporary file usage, external utility script calls) using BigQuery SQL scripting features. This can sometimes lead to more verbose SQL for control flow compared to shell scripts, but offers better integration with BigQuery's data processing engine.

*   **Replacement of Oracle DB-Link and `DWPA_UTIL_SKRIPT`**:
    *   **Why**: The original `d_ausd_v_ta_bp_ref.sql` script relied on an Oracle DB-Link (`@pcrs1`) and an Oracle stored procedure (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE`). In BigQuery, these external dependencies are eliminated. Source tables (`cds_ta_bp_ref`, `dwtk_meldungen`) are directly referenced as BigQuery tables, and `TRUNCATE TABLE` is performed using native BigQuery DDL.
    *   **Trade-offs**: Requires prior migration of the source Oracle tables to BigQuery. This is a prerequisite for the job's functionality.

*   **Dedicated BigQuery Logging Tables (`job_control`, `job_error_log`)**:
    *   **Why**: The original KSH script managed job state, error reporting, and record counts through a combination of internal variables, temporary files, and custom logging functions. Migrating this to dedicated BigQuery tables provides a centralized, queryable, and persistent audit trail for job executions. This enhances observability and simplifies debugging compared to parsing shell script logs.
    *   **Trade-offs**: Introduces new BigQuery tables that need to be managed (DDL, permissions, potential partitioning/clustering for performance).

*   **Handling of "Active Jobs" Logic**:
    *   **Why**: The original KSH script included logic to ignore new runs if an active job with the same `JobKennung` was already running, and to deactivate old "RUNNING" jobs. This critical business logic is preserved and implemented within the BigQuery Stored Procedure using the `job_control` table. This ensures consistent behavior regarding job concurrency and state management.
    *   **Trade-offs**: Requires careful implementation of `UPDATE` and `SELECT COUNT(*)` statements on the `job_control` table to ensure atomicity and correctness, especially under high concurrency (though BigQuery's transaction model for DML helps here).

*   **BigQuery Data Type and Function Adaptation**:
    *   **Why**: Oracle-specific functions (e.g., `NVL`, `TO_CHAR`, `TO_DATE`) and data types are translated to their BigQuery equivalents (e.g., `COALESCE`, `FORMAT_DATE`, `PARSE_DATE`). This ensures data integrity and correct functionality within the BigQuery environment.
    *   **Trade-offs**: Requires a thorough understanding of BigQuery's SQL dialect and potential differences in behavior or precision for certain functions.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` as used in the generated code) exists. If not, create it.

2.  **BigQuery Table DDL Deployment**:
    *   **Data Tables**: Create the DDL for and deploy the following BigQuery tables, ensuring their schemas accurately reflect their Oracle counterparts (column names, data types, nullability, partitioning/clustering strategy):
        *   `project.dataset.dwtk_meldungen`
        *   `project.dataset.cds_ta_bp_ref`
        *   `project.dataset.sof_ta_bp_ref`
    *   **Logging/Control Tables**: Deploy the DDL for the new logging and control tables:
        *   `project.dataset.job_control` (e.g., `job_id STRING, entry_number INT64, start_timestamp TIMESTAMP, end_timestamp TIMESTAMP, status STRING, records_processed INT64, processing_date DATE, script_name STRING`)
        *   `project.dataset.job_error_log` (e.g., `job_id STRING, entry_number INT64, error_timestamp TIMESTAMP, error_message STRING, script_name STRING, log_level STRING`)

3.  **Initial Data Migration**:
    *   Perform a one-time (or set up continuous) data migration from the source Oracle tables to their BigQuery equivalents:
        *   `isbert_schema.dwtk_meldungen` -> `project.dataset.dwtk_meldungen`
        *   `cds$ta_bp_ref` -> `project.dataset.cds_ta_bp_ref`
    *   Ensure data consistency and completeness during this migration.

4.  **BigQuery Stored Procedure Deployment**:
    *   Execute the `sql/stored_procedures/k_ausd_v_ta_bp_ref_sp.sql` script in BigQuery to create the `project.dataset.k_ausd_v_ta_bp_ref_sp` stored procedure.

5.  **IAM and Permissions Configuration**:
    *   Configure appropriate IAM roles and permissions for the service account or user that will invoke the BigQuery Stored Procedure. This entity must have:
        *   `bigquery.jobs.create` permission to run jobs.
        *   `bigquery.tables.getData` on `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_bp_ref`.
        *   `bigquery.tables.updateData` and `bigquery.tables.truncate` on `project.dataset.sof_ta_bp_ref`.
        *   `bigquery.tables.insertData` and `bigquery.tables.updateData` on `project.dataset.job_control` and `project.dataset.job_error_log`.

6.  **Scheduling Configuration**:
    *   If the original job was invoked by `r_ausd_v_ta_bp_ref.ksh` or another scheduler, update or create a new scheduler (e.g., a Cloud Composer DAG, Cloud Workflow, or Cloud Scheduler job) to invoke the BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_bp_ref_sp` with the required `p_JobKennung` and `p_EintragsNr` parameters.

## 5. Known Gaps & Unresolved References

*   **Complete Schema Details**: The exact, comprehensive schemas (all column names, precise data types, constraints) for `cds$ta_bp_ref`, `sof$ta_bp_ref`, and `isbert_schema.dwtk_meldungen` were inferred. A detailed schema mapping and validation are required to ensure the BigQuery DDLs are 100% accurate and prevent data type mismatches or truncation issues.
*   **Utility Script Logic (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.)**: The full, intricate logic within the original KSH utility scripts (e.g., `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`) was not fully detailed. While core functionalities are translated, subtle behaviors, especially complex error handling or date calculations, might require further investigation and precise translation into BigQuery scripting.
*   **Performance Tuning**: The initial BigQuery SQL translation is functional. For large datasets, performance tuning (e.g., optimizing `WHERE` clauses, applying appropriate partitioning and clustering keys to BigQuery tables) may be necessary post-migration.
*   **`p_EintragsNr` Usage**: The original KSH script passes `p_EintragsNr` to `starteSQLSkript` twice, and its exact role within the SQL script was not fully clear from the design document. In the migrated SP, it's primarily used for logging/tracking. If it had a more active role in the Oracle SQL, this needs re-verification.
*   **`is_production` field**: The `WHERE br.is_production = 1` clause assumes the `is_production` column exists and is of a compatible type (e.g., `BOOLEAN` or `INT64`) in BigQuery. This needs to be confirmed during schema mapping.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prerequisites**: Ensure all manual steps (DDL deployment, data migration, SP deployment, IAM) are completed.
2.  **Test Data**: Prepare a representative set of test data in `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_bp_ref` that covers various scenarios (e.g., different `insert_at`/`modified_at`/`valid_from`/`valid_to` combinations, edge cases for `v_datum`).
3.  **Execution**:
    *   Manually invoke the BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.k_ausd_v_ta_bp_ref_sp`('TEST_JOB', 12345);
        ```
    *   Alternatively, trigger the new Cloud Composer DAG or Cloud Workflow if already configured.
4.  **Verification of "Passing"**:
    *   **Stored Procedure Completion**: The BigQuery Stored Procedure execution should complete without raising an unhandled error.
    *   **`job_control` Table**:
        *   Query `project.dataset.job_control` for the `p_JobKennung` and `p_EintragsNr` used in the test.
        *   Verify that `status` is 'SUCCESS'.
        *   Verify `start_timestamp`, `end_timestamp`, `records_processed`, and `processing_date` are correctly populated.
    *   **`job_error_log` Table**:
        *   Query `project.dataset.job_error_log` for the `p_JobKennung` and `p_EintragsNr`.
        *   Ensure no 'ERROR' level entries are present for the successful run.
    *   **Target Data (`sof_ta_bp_ref`)**:
        *   Query `project.dataset.sof_ta_bp_ref`.
        *   Verify that the data inserted matches the expected output based on the source data and the transformation logic.
        *   Confirm that the `COUNT(*)` from `sof_ta_bp_ref` matches the `records_processed` value in `job_control`.
    *   **Comparison with Legacy System**:
        *   Run the original `k_ausd_v_ta_bp_ref.ksh` job with the *same* source data (or a snapshot of it).
        *   Compare the final contents of the Oracle `sof$ta_bp_ref` table with the BigQuery `project.dataset.sof_ta_bp_ref` table. The data should be identical.
        *   Compare the record counts reported by both systems.

## 7. Rollback Procedure

In case of critical issues or failures after go-live, the following rollback procedure can be executed to revert to the legacy system:

1.  **Disable New Scheduler**:
    *   Immediately disable or pause the Cloud Composer DAG, Cloud Workflow, or any other scheduler that invokes the BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_bp_ref_sp`.

2.  **Re-enable Legacy Scheduler**:
    *   Re-enable the original scheduler (e.g., `r_ausd_v_ta_bp_ref.ksh` or its cron job) that was responsible for invoking the `k_ausd_v_ta_bp_ref.ksh` script.

3.  **Data Restoration (if necessary)**:
    *   The BigQuery Stored Procedure performs a `TRUNCATE TABLE` on `project.dataset.sof_ta_bp_ref` before inserting data. If the new system has run and potentially corrupted or incorrectly populated `sof_ta_bp_ref`, and if the legacy system relies on this table for subsequent processes, you might need to:
        *   Restore `project.dataset.sof_ta_bp_ref` to its state just before the problematic BigQuery job run (if backups or snapshots are available).
        *   Alternatively, if the legacy system's `sof$ta_bp_ref` is the authoritative source, ensure it remains unaffected by the BigQuery migration.
    *   **Note**: The `TRUNCATE` operation in BigQuery is generally not immediately reversible without a table snapshot or backup. Ensure a robust backup strategy for `sof_ta_bp_ref` if data integrity is paramount during rollback.

4.  **Monitor Legacy System**:
    *   Verify that the legacy `k_ausd_v_ta_bp_ref.ksh` job is running as expected and producing correct output in the Oracle environment.

5.  **Post-Rollback Analysis**:
    *   Investigate the root cause of the failure in the migrated BigQuery job. This may involve reviewing BigQuery job logs, `job_error_log` table entries, and the stored procedure code.
    *   Plan for remediation and re-testing before attempting another go-live.

**Important**: This rollback procedure assumes that the original Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_bp_ref`) were not modified or truncated by the BigQuery migration and remain available for the legacy system. If the migration involved destructive changes to these source tables, a more complex data restoration plan would be required.
```