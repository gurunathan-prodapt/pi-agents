# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh` and its associated core SQL logic (`d_ausd_v_ta_cntrct_valid.sql`). The original script served as a control and orchestration mechanism for data processing related to `ta_cntrct_valid`, handling parameter parsing, job state management, and the execution of an SQL script.

The migration target is Google BigQuery. The KornShell control logic has been re-engineered into a BigQuery Stored Procedure, `my_project.my_dataset.r_ausd_vertrag_control`, while the core data transformation logic from `d_ausd_v_ta_cntrct_valid.sql` has been migrated into a separate BigQuery Stored Procedure, `my_project.my_dataset.d_ausd_v_ta_cntrct_valid`. Job state management and logging are now handled by dedicated BigQuery tables, `my_project.my_dataset.job_table` and `my_project.my_dataset.job_log`.

## 2. Generated artifacts

The migration produced the following BigQuery artifacts:

*   **`ddl/my_project.my_dataset.job_table.sql`**
    *   **Role**: Defines the schema for the `job_table` in BigQuery. This table is used to track the overall status, start/end times, record counts, and error messages for each execution of the `r_ausd_vertrag_control` stored procedure. It replaces the implicit job tracking and activation/deactivation logic of the original KornShell script.

*   **`ddl/my_project.my_dataset.job_log.sql`**
    *   **Role**: Defines the schema for the `job_log` table in BigQuery. This table stores detailed log messages, including informational, warning, and error messages, for each job execution. It provides a centralized and auditable log of job activities, replacing the shell script's console output and basic error reporting.

*   **`stored_procedures/my_project.my_dataset.d_ausd_v_ta_cntrct_valid.sql`**
    *   **Role**: This BigQuery Stored Procedure encapsulates the core data transformation logic originally found in `d_ausd_v_ta_cntrct_valid.sql`. It is responsible for determining a date from `dwtk_meldungen`, truncating the `sof_ta_cntrct_valid` table, and inserting processed data from `cds_ta_cntrct_validity` into `sof_ta_cntrct_valid`. It returns the count of processed records.

*   **`stored_procedures/my_project.my_dataset.r_ausd_vertrag_control.sql`**
    *   **Role**: This BigQuery Stored Procedure is the direct migration of the `k_ausd_v_ta_cntrct_valid.ksh` control script. It handles:
        *   Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`).
        *   Checking for and ignoring already active job instances.
        *   Deactivating older active job instances.
        *   Registering the current job execution in `job_table`.
        *   Orchestrating the execution of the `d_ausd_v_ta_cntrct_valid` stored procedure.
        *   Capturing the number of processed records.
        *   Implementing robust error handling using `EXCEPTION WHEN ERROR` blocks.
        *   Updating the `job_table` with final status, record counts, and error details.
        *   Logging all significant events to the `job_log` table.

## 3. Key design decisions

The migration strategy involved several key design decisions to translate the KornShell script's functionality into a BigQuery-native environment:

*   **Orchestration to BigQuery Stored Procedure**: The primary control script (`k_ausd_v_ta_cntrct_valid.ksh`) was migrated to a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This decision allows the entire workflow to run natively within BigQuery, leveraging its scalability and eliminating external shell dependencies. It provides a structured, version-controlled, and directly executable unit within the data warehouse.
*   **Core Logic Separation**: The data transformation logic from `d_ausd_v_ta_cntrct_valid.sql` was encapsulated in its own BigQuery Stored Procedure (`d_ausd_v_ta_cntrct_valid`). This promotes modularity, reusability, and a clear separation of concerns between orchestration/control and core data processing.
*   **Centralized Job State Management**: The implicit job tracking and activation/deactivation logic of the KornShell script was replaced with explicit `INSERT` and `UPDATE` operations on dedicated BigQuery tables (`job_table`, `job_log`). This provides a centralized, auditable, and scalable mechanism for monitoring job status and history.
*   **Native Parameter Handling**: KornShell's `getopts` and parameter validation were replaced by BigQuery Stored Procedure input parameters and `ASSERT` statements or `IF` conditions. This ensures type safety and integrates parameter validation directly into the BigQuery environment.
*   **Robust Error Handling**: The shell script's `ErrNr` and basic `if` conditions for error handling were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` blocks and `RAISE` statements. This provides structured, robust error management and integrates seamlessly with the `job_log` table for detailed error reporting.
*   **Elimination of Temporary Files**: The use of filesystem-based temporary files for record counting was replaced by BigQuery `DECLARE` and `SET` statements for variables within the stored procedure. This removes external filesystem dependencies and aligns with cloud-native practices.
*   **Absorption of Utility Script Functionality**: The functionalities of various sourced KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) were re-implemented directly within the main BigQuery stored procedure using native BigQuery functions and scripting constructs. This simplifies deployment and removes external script dependencies.

**Notable Trade-offs:**
*   **Re-implementation Effort**: Functionalities from the original shell utility scripts had to be re-implemented in BigQuery SQL, which can sometimes be more verbose for control flow than shell scripting.
*   **Loss of Direct Filesystem Access**: While beneficial for cloud-native operations, this means any logic relying on direct filesystem manipulation (beyond temporary files, which were addressed) would require a different approach (e.g., Cloud Storage).
*   **Assumptions on `starteSQLSkript`**: The migration assumes the `starteSQLSkript` function primarily executed SQL and handled basic error codes. Any more complex logic (e.g., advanced connection pooling, specific retry mechanisms) not explicitly detailed in the design document might not have been fully replicated.

## 4. Manual steps before go-live

Before the migrated BigQuery stored procedures can be used in a production environment, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`my_project.my_dataset`) exists. If not, create it:
    ```sql
    CREATE SCHEMA IF NOT EXISTS `my_project.my_dataset`;
    ```
2.  **Deploy DDLs**: Execute the DDL scripts to create the `job_table` and `job_log` tables:
    ```bash
    bq query --use_legacy_sql=false < ddl/my_project.my_dataset.job_table.sql
    bq query --use_legacy_sql=false < ddl/my_project.my_dataset.job_log.sql
    ```
3.  **Deploy Stored Procedures**: Execute the scripts to create the `d_ausd_v_ta_cntrct_valid` and `r_ausd_vertrag_control` stored procedures:
    ```bash
    bq query --use_legacy_sql=false < stored_procedures/my_project.my_dataset.d_ausd_v_ta_cntrct_valid.sql
    bq query --use_legacy_sql=false < stored_procedures/my_project.my_dataset.r_ausd_vertrag_control.sql
    ```
4.  **IAM Permissions**:
    *   The Google Cloud service account or user identity that will execute these stored procedures must have appropriate BigQuery permissions.
    *   Minimum required roles typically include:
        *   `BigQuery Data Editor` on `my_project.my_dataset` (for `INSERT`, `UPDATE`, `TRUNCATE` on `job_table`, `job_log`, `sof_ta_cntrct_valid`).
        *   `BigQuery Data Viewer` on `my_project.my_dataset` (for `SELECT` on `dwtk_meldungen`, `cds_ta_cntrct_validity`).
        *   `BigQuery Job User` on `my_project` (to run BigQuery jobs, including stored procedures).
5.  **Source and Target Table Existence**:
    *   Ensure the source tables (`my_project.my_dataset.dwtk_meldungen`, `my_project.my_dataset.cds_ta_cntrct_validity`) and the target table (`my_project.my_dataset.sof_ta_cntrct_valid`) exist in BigQuery and have the expected schemas and data types.
    *   If these tables do not exist, their DDLs must be created and executed.
6.  **Scheduling (if applicable)**: If the job is to be scheduled, set up a Google Cloud Scheduler job, Cloud Workflows definition, or Cloud Composer DAG to invoke the `my_project.my_dataset.r_ausd_vertrag_control` stored procedure with the required parameters.

## 5. Known gaps & unresolved references

While the migration aims for full functional parity, the following items were identified as potential gaps or areas requiring further review:

*   **Inferred `d_ausd_v_ta_cntrct_valid.sql` Logic**: The original content of `d_ausd_v_ta_cntrct_valid.sql` was not directly provided in the design document. The migration was based on an inferred understanding of its purpose and a sample SQL snippet. Specifically, the mapping of `cv.insert_at` to `bfc_age` in the `INSERT` statement was an inference. A thorough review against the original SQL script is recommended.
*   **Original `DW_DIR_UTL` and `BERT_DIR_ROOT` Variables**: The exact values and full usage context of these environment variables from the original KornShell environment were not fully resolved. While temporary file handling was addressed, if these variables influenced other aspects (e.g., dynamic dataset/table naming), those aspects would need explicit configuration in the BigQuery environment.
*   **`starteSQLSkript` Function Details**: The precise implementation of `starteSQLSkript` within `h_alis_sqlplus.ksh` was unknown. The migration assumes it primarily handled SQL execution and basic error code propagation. Any advanced features like specific connection pooling, complex retry logic, or custom logging within `starteSQLSkript` are not explicitly replicated in the BigQuery stored procedure.
*   **Source and Target Table Schemas**: The DDLs for `dwtk_meldungen`, `cds_ta_cntrct_validity`, and `sof_ta_cntrct_valid` were not provided. The generated BigQuery SQL assumes compatible schemas and data types for these tables. Any discrepancies could lead to runtime errors.
*   **`TRUNCATE TABLE` Behavior**: The original script used `DWPA_UTIL_SKRIPT` for truncation. The migration uses BigQuery's `TRUNCATE TABLE`. This assumes that `sof_ta_cntrct_valid` is not partitioned or clustered in a way that would make a simple `TRUNCATE` problematic, or that `DWPA_UTIL_SKRIPT` did not perform additional complex operations beyond a standard truncate.

## 6. Validation

To validate the successful migration and functionality of the BigQuery stored procedures, follow these steps:

1.  **Prepare Test Data**:
    *   Populate `my_project.my_dataset.dwtk_meldungen` with sample data, including entries with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` values.
    *   Populate `my_project.my_dataset.cds_ta_cntrct_validity` with sample data that would be expected to be processed by the `d_ausd_v_ta_cntrct_valid` logic, including `insert_at` and `modified_at` values.
    *   Ensure `my_project.my_dataset.sof_ta_cntrct_valid` is either empty or contains known test data that can be truncated.

2.  **Execute the Control Procedure**:
    *   **Successful Run**: Call the main control stored procedure with valid parameters:
        ```sql
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG_1', 'TEST_ENTRY_NR_1');
        ```
    *   **"Ignore Active Job" Scenario**: Immediately call the procedure again with the *same* parameters:
        ```sql
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG_1', 'TEST_ENTRY_NR_1');
        ```
    *   **Parameter Validation Failure**: Call the procedure with missing or empty parameters:
        ```sql
        CALL `my_project.my_dataset.r_ausd_vertrag_control`(NULL, 'TEST_ENTRY_NR_2');
        -- or
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG_2', '');
        ```
    *   **Simulated Core Logic Failure**: To test error handling, you might temporarily modify `d_ausd_v_ta_cntrct_valid` to `RAISE` an error or reference a non-existent table, then call `r_ausd_vertrag_control`.

3.  **Verify Results**:

    *   **`my_project.my_dataset.job_table`**:
        *   **Passing**:
            *   For successful runs, verify an entry exists with `job_id`, `entry_number`, `start_time`, `end_time`, `status = 'COMPLETED'`, `record_count` matching the expected number of rows inserted into `sof_ta_cntrct_valid`, and `is_active = FALSE`.
            *   For the "ignore active job" scenario, verify that the *first* run is `COMPLETED` (or `RUNNING` if still in progress) and the *second* run is not recorded as `RUNNING` but potentially as `IGNORED` or simply not present as a new active job. The `job_log` should contain a `WARNING` message.
            *   For parameter validation failures, verify an entry with `status = 'FAILED'` and an `error_message` indicating the validation failure.
            *   For simulated core logic failures, verify an entry with `status = 'FAILED'` and an `error_message` reflecting the internal error.

    *   **`my_project.my_dataset.job_log`**:
        *   **Passing**:
            *   Verify `INFO` messages for job start and completion.
            *   Verify `WARNING` messages for ignored active jobs.
            *   Verify `ERROR` messages for parameter validation failures and core logic failures, with detailed messages.
            *   Ensure `job_id` and `entry_number` are correctly logged for all messages.

    *   **`my_project.my_dataset.sof_ta_cntrct_valid`**:
        *   **Passing**: After a successful run, query this table to confirm that the data has been truncated and then correctly inserted, matching the expected output based on the logic in `d_ausd_v_ta_cntrct_valid` and the test data in source tables. The `COUNT(*)` should match the `record_count` in `job_table`.

## 7. Rollback procedure

In the event of critical issues detected after deployment, the following rollback procedure can be executed to revert to the previous state:

1.  **Stop New Invocations**:
    *   Immediately disable or delete any scheduling mechanisms (e.g., Cloud Scheduler jobs, Cloud Workflows, Cloud Composer DAGs) that invoke the `my_project.my_dataset.r_ausd_vertrag_control` BigQuery stored procedure.
    *   Communicate to any dependent systems or users to cease manual invocations.

2.  **Revert BigQuery Artifacts**:
    *   **Drop Stored Procedures**: Delete the newly deployed BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.d_ausd_v_ta_cntrct_valid`;
        ```
    *   **Optional: Drop Control Tables**: If `job_table` and `job_log` are exclusively used by this migrated job and no other processes depend on them, they can be dropped. Otherwise, they should be retained.
        ```sql
        DROP TABLE IF EXISTS `my_project.my_dataset.job_table`;
        DROP TABLE IF EXISTS `my_project.my_dataset.job_log`;
        ```

3.  **Restore Original Environment**:
    *   Re-enable the original KornShell script (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh`) and its associated scheduling or triggering mechanisms.

4.  **Data Rollback (Critical)**:
    *   The `d_ausd_v_ta_cntrct_valid` procedure truncates and re-inserts data into `my_project.my_dataset.sof_ta_cntrct_valid`. If the BigQuery job ran successfully and introduced incorrect data, a data rollback is necessary.
    *   **BigQuery Time Travel**: Utilize BigQuery's time travel feature to restore `my_project.my_dataset.sof_ta_cntrct_valid` to a state before the BigQuery job execution.
        ```sql
        CREATE OR REPLACE TABLE `my_project.my_dataset.sof_ta_cntrct_valid` AS
        SELECT * FROM `my_project.my_dataset.sof_ta_cntrct_valid` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE);
        ```
        (Replace `X` with an appropriate number of minutes to go back before the erroneous execution).
    *   **Backup Restoration**: If time travel is not feasible or sufficient, restore `sof_ta_cntrct_valid` from the most recent valid backup.

5.  **Verify Rollback**:
    *   Confirm that the BigQuery stored procedures are no longer present or callable.
    *   Verify that the original KornShell script can execute successfully.
    *   Check `my_project.my_dataset.sof_ta_cntrct_valid` to ensure data integrity has been restored to the pre-migration state.