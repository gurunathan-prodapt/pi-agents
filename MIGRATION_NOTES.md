# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh`.

The original job, a KornShell script, served as an orchestration layer for an SQL script (`d_ausd_v_ta_vvl_upgrade.sql`). Its primary functions included parameter validation, job status management (ignoring active jobs, registering new jobs, deactivating old jobs), and executing the core data processing SQL.

The job has been migrated to Google Cloud Platform, specifically re-implemented as a BigQuery Stored Procedure. The core data processing logic from `d_ausd_v_ta_vvl_upgrade.sql` has been inlined and converted to BigQuery SQL within this procedure. Job status tracking and logging functionalities have been replaced with dedicated BigQuery tables.

## 2. Generated Artifacts

The migration process generated the following BigQuery-native artifacts:

*   **`ddl/job_status_table.sql`**
    *   **Role**: Defines the BigQuery table `job_status_table`. This table is used to track the lifecycle and status of job executions, replacing the functionality of the legacy "Job-Tabelle" mentioned in the original design. It records `job_id`, `entry_nr`, `status` (e.g., 'ACTIVE', 'COMPLETED', 'FAILED'), `start_time`, `end_time`, `records_processed`, and `error_message`.
*   **`ddl/job_log_table.sql`**
    *   **Role**: Defines the BigQuery table `job_log`. This table serves as a centralized logging mechanism for the migrated job, capturing `log_time`, `job_id`, `entry_nr`, `log_level` (e.g., 'INFO', 'WARN', 'ERROR'), `message`, and `error_details`. It replaces filesystem-based logging and `DWMSG_MeldeFehler` calls.
*   **`stored_procedures/k_ausd_v_ta_vvl_upgrade_sp.sql`**
    *   **Role**: This is the core migrated component, a BigQuery Stored Procedure named `k_ausd_v_ta_vvl_upgrade_sp`. It encapsulates the entire logic of the original `k_ausd_v_ta_vvl_upgrade.ksh` script, including:
        *   Parameter parsing and validation.
        *   Checking for and ignoring already active jobs.
        *   Registering job status in `job_status_table`.
        *   Executing the data processing logic originally found in `d_ausd_v_ta_vvl_upgrade.sql`.
        *   Capturing the number of records processed.
        *   Deactivating older active jobs.
        *   Updating the final job status and logging messages to `job_log`.

## 3. Key Design Decisions

*   **Consolidation into a Single BigQuery Stored Procedure**: The KornShell orchestration and the invoked SQL script (`d_ausd_v_ta_vvl_upgrade.sql`) were combined into a single BigQuery Stored Procedure. This simplifies deployment, execution, and monitoring by leveraging BigQuery's native procedural capabilities, eliminating the need for external shell environments or separate SQL client invocations.
*   **BigQuery-Native Job Status and Logging**: Instead of replicating legacy job tables or filesystem-based logging, dedicated BigQuery tables (`job_status_table` and `job_log`) were created. This provides a scalable, queryable, and integrated solution for tracking job execution and logging within the BigQuery ecosystem.
*   **Direct SQL Script Translation**: The SQL logic from `d_ausd_v_ta_vvl_upgrade.sql` was directly translated into BigQuery SQL and inlined within the stored procedure. This avoids the overhead of calling external scripts and ensures optimal performance within BigQuery.
*   **`@@row_count` for Processed Records**: The BigQuery `@@row_count` system variable is used to efficiently capture the number of rows affected by the main DML statement, directly replacing the legacy method of reading a record count from a temporary file.
*   **BigQuery `RAISE` for Error Handling**: Critical errors, such as parameter validation failures or unexpected SQL execution issues, are handled using BigQuery's `RAISE` statement. This provides a clear signal of failure that can be caught by external orchestrators (e.g., Cloud Composer) and logged appropriately.
*   **Replication of "Ignore Active Jobs" Logic**: The original script's behavior of ignoring a run if the job is already active (for the same `job_id` and `entry_nr`) is replicated using a check against the `job_status_table`. This ensures idempotency and prevents concurrent processing for the same job instance.
*   **Placeholder for Project/Dataset IDs**: The generated code uses `your_project_id.your_dataset_id` as placeholders. This allows for flexible deployment across different environments (e.g., dev, test, prod) by simply replacing these values during deployment.

## 4. Manual Steps Before Go-Live

Before the migrated job can be executed in a production environment, the following manual steps must be performed:

1.  **BigQuery Dataset Creation**: Ensure that the target BigQuery dataset (e.g., `your_dataset_id`) exists within `your_project_id`. If not, create it.
2.  **Table Creation**:
    *   Execute `ddl/job_status_table.sql` to create the `job_status_table`.
    *   Execute `ddl/job_log_table.sql` to create the `job_log` table.
    *   **Source Data Tables**: Ensure the following source tables, referenced in the stored procedure, exist in BigQuery and contain the necessary data. Their schemas must match the expectations of the `INSERT` statement:
        *   `your_project_id.your_dataset_id.dwtk_meldungen`
        *   `your_project_id.your_dataset_id.sof_ta_vvl_dwh`
        *   `your_project_id.your_dataset_id.dwh_ta_l_bindefr_aendgr_carm`
    *   **Target Data Table**: Ensure the target table `your_project_id.your_dataset_id.sof_ta_vvl_upgrade` exists with the expected schema (`vertrags_id`, `upgradegrund`, `upgradedatum`).
3.  **Stored Procedure Deployment**:
    *   **Replace Placeholders**: Before deployment, replace all occurrences of `your_project_id.your_dataset_id` in `stored_procedures/k_ausd_v_ta_vvl_upgrade_sp.sql` with the actual BigQuery project ID and dataset ID.
    *   Execute the modified `stored_procedures/k_ausd_v_ta_vvl_upgrade_sp.sql` to create the stored procedure in BigQuery.
4.  **IAM Permissions**:
    *   The service account or user executing the BigQuery Stored Procedure must have the following IAM roles/permissions:
        *   `BigQuery Data Editor` on `your_project_id.your_dataset_id` (to create/update/delete data in `job_status_table`, `job_log`, and `sof_ta_vvl_upgrade`).
        *   `BigQuery Data Viewer` on `your_project_id.your_dataset_id` (to read from source tables like `dwtk_meldungen`, `sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
5.  **Scheduling**:
    *   **BigQuery Scheduled Query**: If using BigQuery's native scheduling, configure a scheduled query to call `CALL `your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp`('your_job_kennung', 'your_eintrags_nr');` with the appropriate frequency and parameters.
    *   **Cloud Composer (Airflow)**: If using Cloud Composer for more complex orchestration, deploy a DAG that invokes the BigQuery Stored Procedure using the `BigQueryExecuteStoredProcedureOperator` or a similar mechanism.

## 5. Known Gaps & Unresolved References

*   **`file_complexity` Data**: The original `file_complexity` data for the source script was missing. While the migration has proceeded, this indicates a potential gap in the automated analysis of the original system.
*   **`dwtk_meldungen` Table**: The stored procedure relies on the `dwtk_meldungen` table to determine the `Stichtag`. The migration and population of this table with relevant data are critical for the correct functioning of the job. Its schema and data content must accurately reflect the legacy system's `dwtk_meldungen` equivalent.
*   **Source Table Data Integrity**: The accuracy of the migrated job heavily depends on the correct and complete migration of data into `sof_ta_vvl_dwh` and `dwh_ta_l_bindefr_aendgr_carm`. Any discrepancies in these source tables will directly impact the output of `sof_ta_vvl_upgrade`.
*   **Placeholder Replacement**: The generated code contains `your_project_id.your_dataset_id` placeholders. These *must* be replaced with actual project and dataset IDs before deployment. Failure to do so will result in execution errors.
*   **Legacy `r_ausd_vertrag.ksh` Context**: The original `k_ausd_v_ta_vvl_upgrade.ksh` is described as a control script for `r_ausd_vertrag.ksh`. While `k_ausd_v_ta_vvl_upgrade.ksh` itself is migrated, the broader context of `r_ausd_vertrag.ksh` and its other controlled scripts is not covered by this specific migration. Any dependencies or orchestration from `r_ausd_vertrag.ksh` would need separate migration efforts.

## 6. Validation

To validate the successful migration and functionality of the BigQuery Stored Procedure:

1.  **Setup Test Environment**:
    *   Create a dedicated test BigQuery dataset.
    *   Deploy `job_status_table.sql`, `job_log_table.sql`, and `k_ausd_v_ta_vvl_upgrade_sp.sql` to this test dataset, ensuring placeholders are correctly replaced.
    *   Populate the test `dwtk_meldungen`, `sof_ta_vvl_dwh`, and `dwh_ta_l_bindefr_aendgr_carm` tables with representative test data, including edge cases (e.g., no matching data, multiple updates for a `vertrags_id`).
    *   Ensure the target `sof_ta_vvl_upgrade` table exists with the correct schema.

2.  **Test Cases**:
    *   **Successful Execution**:
        *   Execute the stored procedure with valid `p_job_kennung` and `p_eintrags_nr` (e.g., `CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_1', 'ENTRY_001');`).
        *   **Passing Criteria**:
            *   The procedure completes without error.
            *   `job_status_table` contains an entry for `TEST_JOB_1`/`ENTRY_001` with `status = 'COMPLETED'`, `end_time` populated, and `records_processed` matching the actual number of rows inserted into `sof_ta_vvl_upgrade`.
            *   `job_log` contains `INFO` messages indicating successful steps and completion.
            *   `sof_ta_vvl_upgrade` contains the expected processed data based on the test inputs.
    *   **Parameter Validation Failure**:
        *   Execute the stored procedure with missing or empty `p_job_kennung` (e.g., `CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('', 'ENTRY_002');`).
        *   Execute with missing or empty `p_eintrags_nr` (e.g., `CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_2', '');`).
        *   **Passing Criteria**:
            *   The procedure raises an error (e.g., "Parameter validation failed: Jobkennung is missing.").
            *   `job_log` contains an `ERROR` message detailing the validation failure.
            *   No entry is made in `job_status_table` for this run, or if an entry is made (depending on exact error handling flow), its status is 'FAILED'.
    *   **Job Already Active**:
        *   First, manually insert an 'ACTIVE' entry into `job_status_table` for a specific `job_id`/`entry_nr` (e.g., `('TEST_JOB_3', 'ENTRY_003', 'ACTIVE', CURRENT_TIMESTAMP(), 0, NULL)`).
        *   Then, execute the stored procedure with the same `job_id`/`entry_nr` (e.g., `CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_3', 'ENTRY_003');`).
        *   **Passing Criteria**:
            *   The procedure returns immediately without processing data or raising an error.
            *   `job_log` contains a `WARN` message indicating that the job was already active and ignored.
            *   The `job_status_table` entry for `TEST_JOB_3`/`ENTRY_003` remains 'ACTIVE' (or unchanged by this specific run).
    *   **Deactivation of Older Jobs**:
        *   Insert multiple 'ACTIVE' entries into `job_status_table` for the same `p_job_kennung` but different `p_eintrags_nr` (e.g., `('TEST_JOB_4', 'ENTRY_OLD_1', 'ACTIVE', ...)` and `('TEST_JOB_4', 'ENTRY_OLD_2', 'ACTIVE', ...)`).
        *   Execute the stored procedure with `p_job_kennung = 'TEST_JOB_4'` and a new `p_eintrags_nr` (e.g., `CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_4', 'ENTRY_NEW');`).
        *   **Passing Criteria**:
            *   The procedure completes successfully.
            *   The `job_status_table` entries for `ENTRY_OLD_1` and `ENTRY_OLD_2` are updated to `status = 'INACTIVE'` with `end_time` populated.
            *   The `job_status_table` entry for `ENTRY_NEW` is `status = 'COMPLETED'`.
            *   `job_log` contains an `INFO` message about deactivating older jobs.
    *   **Data Processing Verification**:
        *   After successful runs, query `sof_ta_vvl_upgrade` and compare its contents with the expected output based on the test data and the original `d_ausd_v_ta_vvl_upgrade.sql` logic.

## 7. Rollback Procedure

In the event of critical issues or a decision to revert the migration, follow these steps:

1.  **Stop New Executions**:
    *   If using a BigQuery Scheduled Query, disable or delete the scheduled query that invokes `k_ausd_v_ta_vvl_upgrade_sp`.
    *   If using Cloud Composer, undeploy or disable the DAG that triggers `k_ausd_v_ta_vvl_upgrade_sp`.
    *   Ensure no manual executions of the stored procedure are initiated.

2.  **Delete BigQuery Artifacts**:
    *   **Delete Stored Procedure**:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp`;
        ```
    *   **Delete Tables (Optional, but recommended for clean rollback)**:
        *   If `sof_ta_vvl_upgrade` was newly created as part of this migration, consider dropping it. If it's a shared table, ensure its data is backed up or handled appropriately.
        ```sql
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_status_table`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_log`;
        -- DROP TABLE IF EXISTS `your_project_id.your_dataset_id.sof_ta_vvl_upgrade`; -- Only if newly created for migration
        ```
    *   **Note**: If the `job_status_table` or `job_log` contain valuable historical data, consider archiving them instead of outright deleting.

3.  **Re-enable Legacy Job**:
    *   Re-deploy or re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh` script and its associated dependencies (e.g., `d_ausd_v_ta_vvl_upgrade.sql`, utility scripts).
    *   Ensure the legacy scheduling mechanism for the KornShell script is reactivated.

4.  **Verify Legacy System Functionality**:
    *   Confirm that the original job is running as expected and producing correct outputs in the legacy environment.