# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh`. This script, originally responsible for orchestrating the initial provisioning and snapshot extraction of the contract cache for the Forderungsscoring (FOS) system, has been re-implemented.

The migration targets Google Cloud's BigQuery platform. The original shell-based orchestration and core data processing logic have been translated into BigQuery Stored Procedures and BigQuery tables, leveraging native BigQuery capabilities for parameter handling, logging, error management, and data manipulation.

## 2. Generated Artifacts

The migration process has generated the following BigQuery SQL DDL and Stored Procedure definitions:

*   **`ddl/job_log.sql`**:
    *   **Role**: Defines the `project_id.dataset_id.job_log` table. This table serves as a centralized logging mechanism, replacing the filesystem-based log files of the original KornShell script. It captures detailed execution messages, levels (INFO, ERROR), and timestamps for all job runs.
*   **`ddl/job_control.sql`**:
    *   **Role**: Defines the `project_id.dataset_id.job_control` table. This table tracks the overall status and metadata of each job execution, including job number, identifier, reference date (`Stichtag`), restart value (`Wiederanlaufwert`), and final status (STARTED, SUCCESS, FAILED). It replaces the implicit job control logic of the original script.
*   **`ddl/fos_vertrags_cache.sql`**:
    *   **Role**: Defines the `project_id.dataset_id.fos_vertrags_cache` table. This is the target table where the processed contract cache data for the FOS system will be stored. Its schema reflects the expected output of the original `k_ausd_geschaeftspartner.ksh` script.
*   **`ddl/dwh_vertrag_cache_source.sql`**:
    *   **Role**: Defines the `project_id.dataset_id.dwh_vertrag_cache_source` table. This DDL provides a conceptual schema for the source data from which the contract cache is populated. It represents the DWH source tables that the original `k_ausd_geschaeftspartner.ksh` would have queried. This table is assumed to be pre-existing and populated.
*   **`sp/sp_ausd_geschaeftspartner.sql`**:
    *   **Role**: Defines the BigQuery Stored Procedure `project_id.dataset_id.sp_ausd_geschaeftspartner`. This procedure encapsulates the core business logic for data extraction, transformation, and loading, originally found in `k_ausd_geschaeftspartner.ksh`. It handles the restart logic (deleting and re-inserting records based on `dwh_vertrag_id`) and applies filtering criteria based on validity dates.
*   **`sp/sp_initial_befuellung_vertrags_cache_fos.sql`**:
    *   **Role**: Defines the BigQuery Stored Procedure `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`. This is the main wrapper procedure, replacing `r_ausd_geschaeftspartner.ksh`. It manages parameter parsing, defaulting (`Stichtag`, `Wiederanlaufwert`), validation, logging to `job_log`, updating `job_control`, and orchestrating the call to `sp_ausd_geschaeftspartner`. It also includes robust error handling.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **BigQuery Stored Procedures for Logic Encapsulation**: The entire KornShell logic, including both orchestration and core business processing, was translated into BigQuery Stored Procedures. This choice leverages BigQuery's native SQL capabilities, eliminates the need for external compute environments for the core logic, and allows for direct interaction with BigQuery tables.
*   **Separation of Wrapper and Core Logic**: The original architecture's separation into a wrapper (`r_ausd_geschaeftspartner.ksh`) and a core business logic script (`k_ausd_geschaeftspartner.ksh`) was maintained. This promotes modularity, reusability, and easier debugging. `sp_initial_befuellung_vertrags_cache_fos` acts as the orchestrator, calling `sp_ausd_geschaeftspartner` for the actual data manipulation.
*   **Centralized Logging and Job Control Tables**: Instead of disparate filesystem logs and implicit job state management, dedicated BigQuery tables (`job_log` and `job_control`) were introduced. This provides structured, queryable, and centralized visibility into job execution, status, and errors, significantly improving monitoring and auditing capabilities.
*   **Native BigQuery Parameter Handling**: Shell script parameter parsing (`getopts`) and defaulting logic were replaced by `IN` parameters in the BigQuery Stored Procedures, combined with `COALESCE` and `IF` statements for defaulting and validation. This integrates parameter management directly into the SQL environment.
*   **SQL-Native Date and Time Functions**: All date-related operations (e.g., `CURRENT_DATE()`, `PARSE_DATE`) were migrated to BigQuery's built-in functions, ensuring consistent and reliable date manipulation within the SQL context.
*   **Structured Error Handling**: The shell script's `trap` and `if [ ! $ErrNr -eq 0 ]` error handling was replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` blocks. This provides a more robust and structured way to catch, log, and manage errors, ensuring job status is correctly updated in `job_control`.
*   **Direct Translation of Restart Logic**: The restart mechanism, involving `DWH_VERTRAG_ID` thresholds, was directly translated into a `DELETE` statement followed by an `INSERT` statement in `sp_ausd_geschaeftspartner`. This ensures functional equivalence while leveraging BigQuery's DML capabilities.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project_id.dataset_id`) exists. If not, create it.
2.  **Apply DDLs**:
    *   Execute the DDL scripts (`ddl/job_log.sql`, `ddl/job_control.sql`, `ddl/fos_vertrags_cache.sql`, `ddl/dwh_vertrag_cache_source.sql`) in the specified `project_id.dataset_id`.
    *   **Note**: `dwh_vertrag_cache_source` is a placeholder. The actual source tables for `k_ausd_geschaeftspartner.ksh` must be identified and either mapped to existing BigQuery tables or migrated/replicated into BigQuery under the `project_id.dataset_id` with the appropriate schema.
3.  **Deploy Stored Procedures**:
    *   Execute the Stored Procedure DDLs (`sp/sp_ausd_geschaeftspartner.sql`, `sp/sp_initial_befuellung_vertrags_cache_fos.sql`) in the `project_id.dataset_id`.
4.  **IAM/Permissions Configuration**:
    *   The service account or user executing the main stored procedure (`sp_initial_befuellung_vertrags_cache_fos`) must have the following BigQuery roles:
        *   `BigQuery Data Editor` on `project_id.dataset_id` (for `INSERT`, `UPDATE`, `DELETE` on `job_log`, `job_control`, `fos_vertrags_cache`).
        *   `BigQuery Data Viewer` on `project_id.dataset_id` (for `SELECT` on `job_control`, `dwh_vertrag_cache_source`).
        *   `BigQuery Job User` (to run BigQuery jobs/stored procedures).
5.  **Orchestration Setup**:
    *   Configure a Google Cloud orchestration service (e.g., Cloud Composer/Airflow, Workflows, Dataform) to schedule and execute the `sp_initial_befuellung_vertrags_cache_fos` stored procedure.
    *   The orchestrator should be configured to pass the `p_stichtag_str` and `p_wiederanlaufWert_input` parameters as needed.
    *   Ensure the orchestrator's service account has the necessary BigQuery permissions as listed above.
6.  **Initial Data Population (if applicable)**:
    *   Ensure that the `dwh_vertrag_cache_source` table (or its equivalent source tables) is populated with the necessary data for the job to run successfully.

## 5. Known Gaps & Unresolved References

The following items have been identified as known gaps or require further investigation/resolution:

*   **Detailed Logic of `k_ausd_geschaeftspartner.ksh`**: The provided migration design and generated code for `sp_ausd_geschaeftspartner` assume a direct translation of the core business logic. However, the exact SQL for data extraction and transformation within the original `k_ausd_geschaeftspartner.ksh` was not fully detailed in the analysis of `r_ausd_geschaeftspartner.ksh`. A thorough review of `k_ausd_geschaeftspartner.ksh` is required to confirm that all business rules, joins, filters, and transformations are accurately represented in `sp_ausd_geschaeftspartner`. This is a critical B4 item for redesign/refinement.
*   **Environment Initialization (`. $HOME/.dw_init`)**: The original `.dw_init` script might set complex environment variables or execute commands with side effects beyond simple parameter assignment. While basic environment variables are handled by BigQuery parameters or configuration, any deeper implications (e.g., specific database connections, complex OS-level interactions, or external tool invocations) need to be thoroughly investigated and replicated using BigQuery features, configuration tables, or potentially external Cloud Functions/Python scripts if complex OS-level interactions are truly necessary.
*   **KornShell Specifics**: While the migration covers the main functional aspects, subtle nuances of KornShell behavior (e.g., specific string manipulations, advanced `trap` scenarios not directly related to error logging, or external command executions not fully captured in the design) might require careful review during implementation and testing.
*   **Performance Characteristics**: The performance of the BigQuery implementation might differ significantly from the legacy KornShell script. `sp_ausd_geschaeftspartner` will require careful query optimization, especially for large datasets, to ensure it meets performance SLAs.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, perform the following steps:

1.  **Execute the Main Stored Procedure**:
    *   Manually call `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos` from the BigQuery console or via a `bq query` command.
    *   **Test Case 1 (Default Parameters)**: `CALL project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos(NULL, NULL);` (should use `CURRENT_DATE()` for `Stichtag` and `0` for `Wiederanlaufwert`).
    *   **Test Case 2 (Specific Stichtag)**: `CALL project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos('31122023', NULL);`
    *   **Test Case 3 (Specific Stichtag and Wiederanlaufwert)**: `CALL project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos('31122023', 1000);`
    *   **Test Case 4 (Invalid Stichtag)**: `CALL project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos('20231231', NULL);` (should fail due to format).
2.  **Monitor `job_log` Table**:
    *   Query `SELECT * FROM project_id.dataset_id.job_log WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC;`
    *   **Passing Criteria**: Verify that log messages are recorded correctly, including INFO messages for start/end of procedures and any ERROR messages for failed runs.
3.  **Monitor `job_control` Table**:
    *   Query `SELECT * FROM project_id.dataset_id.job_control WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC;`
    *   **Passing Criteria**:
        *   For successful runs, the `status` column should be 'SUCCESS' and `finished_at` should be populated.
        *   For failed runs, the `status` column should be 'FAILED' and `finished_at` should be populated.
        *   `stichtag` and `resume_value` should reflect the input parameters.
4.  **Verify `fos_vertrags_cache` Data**:
    *   After a successful run, query `SELECT * FROM project_id.dataset_id.fos_vertrags_cache ORDER BY dwh_vertrag_id;`
    *   **Passing Criteria**:
        *   The data in `fos_vertrags_cache` should accurately reflect the expected output based on the `p_stichtag` and `p_wiederanlaufWert` used.
        *   Compare a sample of the output data with the output generated by the legacy system for the same input parameters. This is the most critical validation step.
        *   Specifically, verify the restart logic: if `p_wiederanlaufWert > 0`, records with `dwh_vertrag_id < p_wiederanlaufWert` should remain, and records with `dwh_vertrag_id >= p_wiederanlaufWert` should be refreshed.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions**:
    *   Immediately disable or pause the orchestration (e.g., Cloud Composer DAG, Workflow) that triggers `sp_initial_befuellung_vertrags_cache_fos`. This prevents further execution of the migrated job.
2.  **Revert BigQuery Objects (Optional, if DDL changes are problematic)**:
    *   If the issue is related to the DDL of the tables or stored procedures themselves, you may need to revert them.
    *   **Tables**: If the table schemas were altered in a non-backward-compatible way, you might need to drop and recreate them to their previous state (if a backup DDL exists). For `fos_vertrags_cache`, if data integrity is paramount, consider restoring from a BigQuery snapshot or a point-in-time recovery if enabled.
    *   **Stored Procedures**: Revert `sp_initial_befuellung_vertrags_cache_fos` and `sp_ausd_geschaeftspartner` to their previous working versions by deploying the older SQL definitions.
3.  **Re-enable Legacy Job**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh` script in its legacy environment.
4.  **Data Rollback (If Necessary)**:
    *   If the migrated job has written incorrect data to `fos_vertrags_cache` and this data needs to be reverted, perform a data rollback.
    *   **Option A (BigQuery Time Travel)**: If the incorrect data was written recently, use BigQuery's time travel feature to query the table as of a point in time before the erroneous run and overwrite the current table with the correct state.
    *   **Option B (Backup/Snapshot Restore)**: If a backup or snapshot of `fos_vertrags_cache` was taken before the migration or go-live, restore the table from that backup.
    *   **Option C (Manual Correction)**: For smaller, isolated issues, manual `DELETE` and `INSERT` statements might be used to correct the data.
    *   **Note**: The impact of incorrect data on downstream systems must be assessed immediately.
5.  **Post-Rollback Verification**:
    *   Verify that the legacy job is running correctly and producing expected output.
    *   Confirm that `fos_vertrags_cache` (if not rolled back) or the legacy target data is in a consistent state.
6.  **Root Cause Analysis**:
    *   Investigate the root cause of the issue that necessitated the rollback using the `job_log` and `job_control` tables, and BigQuery job history. Address the identified issues before attempting re-migration or re-deployment.