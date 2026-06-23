# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `r_ausd_v_ta_cntrct_crs2.ksh`, located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`, has been migrated to Google BigQuery.

The original script served as an orchestration wrapper for a contract data reconciliation process. Its primary functions included environment setup, parameter parsing, job and error logging (using a custom `DWMSG` framework), and the invocation of a core processing script (`k_ausd_v_ta_cntrct_crs2.ksh`). The wrapper itself did not contain direct business logic for data transformation.

The migration involved transforming this shell script into a BigQuery Stored Procedure, `sp_vertragsdatenabgleich`, which replicates the orchestration, parameter handling, and logging functions within the BigQuery environment. The core business logic, originally in `k_ausd_v_ta_cntrct_crs2.ksh`, is now expected to reside in a separate BigQuery Stored Procedure, `sp_k_ausd_v_ta_cntrct_crs2`, which is called by the wrapper.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_execution_log.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `job_execution_log` table in BigQuery. This table replaces the file-based logging and `DWMSG` framework's job entry tracking, recording the start, end, and status of each job execution.
*   **`sql/ddl/job_error_log.sql`**
    *   **Role**: Defines the DDL for the `job_error_log` table in BigQuery. This table replaces the error logging functionality of the original script, capturing detailed error information for failed job executions.
*   **`sql/ddl/config_job_control.sql`**
    *   **Role**: Defines the DDL for the `config_job_control` table in BigQuery. This table centralizes job metadata, including the mapping between original job identifiers (`job_kennung`) and their corresponding BigQuery stored procedures for both the wrapper and the core logic. It replaces implicit configurations and dynamic script invocation logic.
*   **`sql/stored_procedures/sp_k_ausd_v_ta_cntrct_crs2.sql`**
    *   **Role**: This is a placeholder for the BigQuery Stored Procedure that will contain the migrated core business logic of the original `k_ausd_v_ta_cntrct_crs2.ksh` script. It defines the procedure signature and serves as the target for invocation by `sp_vertragsdatenabgleich`. **Note: This procedure requires full implementation of the core business logic.**
*   **`sql/stored_procedures/sp_vertragsdatenabgleich.sql`**
    *   **Role**: This is the main BigQuery Stored Procedure that replaces the `r_ausd_v_ta_cntrct_crs2.ksh` wrapper script. It handles parameter parsing, validation, logging to `job_execution_log` and `job_error_log`, and orchestrates the call to the core logic procedure (`sp_k_ausd_v_ta_cntrct_crs2`). It includes robust error handling.

## 3. Key design decisions

*   **Wrapper-to-Stored Procedure Migration**: The KornShell wrapper script, which primarily handled orchestration and logging, was migrated directly into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This decision keeps the orchestration logic close to the data, leveraging BigQuery's native capabilities for parameter handling, control flow, and error management.
*   **Centralized BigQuery Logging**: The custom `DWMSG` framework and file-based logging were replaced by dedicated BigQuery tables (`job_execution_log` and `job_error_log`). This provides a centralized, queryable, and scalable logging solution within the BigQuery ecosystem, simplifying auditing and monitoring.
*   **Configuration Management via BigQuery Table**: Environment variables and implicit configurations from the original shell environment (e.g., `BERT_DIR_ROOT`, `Name_Kernskript`) are now managed through a `config_job_control` BigQuery table. This allows for dynamic retrieval of job-specific metadata and the mapping to the correct core stored procedure, enhancing maintainability and flexibility.
*   **Parameter Handling Translation**: The `getopts` mechanism for command-line argument parsing was translated into explicit input parameters for the BigQuery Stored Procedure (`p_job_kennung`, `p_s`, `p_l`, `p_h`). Validation logic is implemented using BigQuery's `IF` statements.
*   **Robust Error Handling**: Shell-specific `trap` commands for signal handling were replaced by BigQuery's `EXCEPTION WHEN OTHERS` block. This provides a structured and consistent way to catch and log errors, ensuring that job failures are properly recorded in `job_error_log` and propagated.
*   **Delegation of Core Logic**: The design maintains the original separation of concerns by having the wrapper procedure (`sp_vertragsdatenabgleich`) call a separate BigQuery Stored Procedure (`sp_k_ausd_v_ta_cntrct_crs2`) for the core business logic. This modular approach simplifies development, testing, and maintenance of both components.
*   **Dynamic Procedure Invocation (Constraint Handling)**: BigQuery Stored Procedures do not directly support dynamic `CALL` statements using a variable for the procedure name. The current design assumes a direct, hardcoded call to `sp_k_ausd_v_ta_cntrct_crs2` based on the `job_kennung` retrieved from `config_job_control`. If more dynamic core script selection were required, a `CASE` statement or a more complex metadata-driven approach would be necessary.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure that the target BigQuery dataset (`YOUR_PROJECT_ID.YOUR_DATASET_ID`) exists. If not, create it.
    ```sql
    CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID`;
    ```
2.  **Deploy DDL for Logging and Configuration Tables**:
    *   Execute the DDL from `sql/ddl/job_execution_log.sql` to create the `job_execution_log` table.
    *   Execute the DDL from `sql/ddl/job_error_log.sql` to create the `job_error_log` table.
    *   Execute the DDL from `sql/ddl/config_job_control.sql` to create the `config_job_control` table.
3.  **Populate `config_job_control` Table**: Insert the necessary configuration for the `TA_CNTRCT_CRS2` job into the `config_job_control` table.
    ```sql
    INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.config_job_control` (job_kennung, program_name, kernel_script_name, description)
    VALUES ('TA_CNTRCT_CRS2', 'r_ausd_v_ta_cntrct_crs2', 'sp_k_ausd_v_ta_cntrct_crs2', 'Contract reconciliation wrapper job');
    ```
4.  **Implement and Deploy Core Logic Stored Procedure**: The `sql/stored_procedures/sp_k_ausd_v_ta_cntrct_crs2.sql` file is currently a placeholder. The actual business logic from the original `k_ausd_v_ta_cntrct_crs2.ksh` script must be fully migrated and implemented within this BigQuery Stored Procedure. Once implemented, deploy it to BigQuery.
5.  **Deploy Wrapper Stored Procedure**: Execute the DDL from `sql/stored_procedures/sp_vertragsdatenabgleich.sql` to create the main wrapper stored procedure.
6.  **IAM Permissions**:
    *   Ensure the service account or user that will execute `sp_vertragsdatenabgleich` has the following BigQuery roles:
        *   `BigQuery Data Editor` on `YOUR_PROJECT_ID.YOUR_DATASET_ID` (for inserting/updating log tables).
        *   `BigQuery Data Viewer` on `YOUR_PROJECT_ID.YOUR_DATASET_ID` (for reading `config_job_control`).
        *   `BigQuery Job User` (for running stored procedures).
7.  **Scheduling**: Configure an orchestration tool (e.g., Cloud Composer/Airflow, Cloud Workflows, or a scheduled query in BigQuery) to call `sp_vertragsdatenabgleich` with the required parameters (`p_job_kennung`, `p_s`, `p_l`).

## 5. Known gaps & unresolved references

*   **Core Logic Implementation**: The most significant gap is that `sp_k_ausd_v_ta_cntrct_crs2.sql` is a placeholder. The complete business logic from `k_ausd_v_ta_cntrct_crs2.ksh` must be analyzed, designed, and implemented as a BigQuery Stored Procedure. This is a critical prerequisite (B4 item).
*   **Parameter `-s` and `-l` Semantics**: The original KornShell script defined `-s` and `-l` in `ParamList` but did not explicitly process them in the `case` statement. The migrated `sp_vertragsdatenabgleich` assumes these are mandatory and passes them to `sp_k_ausd_v_ta_cntrct_crs2`. The exact meaning and usage of these parameters need to be clarified during the core logic migration to ensure correct implementation.
*   **Dynamic Procedure Invocation**: BigQuery's SQL does not support calling a stored procedure whose name is stored in a variable. The current implementation of `sp_vertragsdatenabgleich` makes a direct `CALL` to `sp_k_ausd_v_ta_cntrct_crs2`. If the `config_job_control` table was intended to allow for different core scripts to be called based on `job_kennung`, this would require a more complex `CASE` statement or a metadata-driven execution pattern in the orchestration layer.
*   **Shell-Specific Features Parity**: While `trap` and `tee` functionalities have been replaced with BigQuery's `EXCEPTION` blocks and `INSERT` into log tables, the exact behavioral parity (e.g., how `tee` handles buffering, or specific signal handling) should be validated if critical to the original script's operation.
*   **`DWMSG` Framework Completeness**: The `DWMSG` framework was replaced by direct inserts into `job_execution_log` and `job_error_log`. Any advanced features of `DWMSG` (e.g., specific error code mappings, complex retry logic, or external notifications) that were not explicitly identified and migrated will be missing.

## 6. Validation

Validation of the migrated job involves both unit testing of the individual components and integration testing of the complete flow.

### How to run the tests:

1.  **Unit Test `sp_vertragsdatenabgleich` (Wrapper)**:
    *   **Help Message**:
        ```sql
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(p_h => TRUE);
        ```
        *Expected*: Usage instructions should be returned.
    *   **Missing `p_job_kennung`**:
        ```sql
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(p_job_kennung => NULL, p_s => 'test_s', p_l => 'test_l');
        ```
        *Expected*: Procedure should raise an exception, and `job_error_log` should contain an entry for `UNKNOWN_JOB` with error code 1.
    *   **Unknown `p_job_kennung`**:
        ```sql
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(p_job_kennung => 'NON_EXISTENT_JOB', p_s => 'test_s', p_l => 'test_l');
        ```
        *Expected*: Procedure should raise an exception, and `job_error_log` should contain an entry for `NON_EXISTENT_JOB` with error code 2.
    *   **Missing `p_s` or `p_l`**:
        ```sql
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(p_job_kennung => 'TA_CNTRCT_CRS2', p_s => NULL, p_l => 'test_l');
        -- or
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(p_job_kennung => 'TA_CNTRCT_CRS2', p_s => 'test_s', p_l => NULL);
        ```
        *Expected*: Procedure should raise an exception, and `job_error_log` should contain an entry for `TA_CNTRCT_CRS2` with error code 3 or 4 respectively.
    *   **Successful Execution (with placeholder core logic)**:
        ```sql
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(p_job_kennung => 'TA_CNTRCT_CRS2', p_s => '2023-01-01', p_l => 'daily');
        ```
        *Expected*: Procedure completes successfully. `job_execution_log` should have a 'STARTED' and then an 'OK' entry for the `TA_CNTRCT_CRS2` job. The placeholder `sp_k_ausd_v_ta_cntrct_crs2` should print its message.
    *   **Simulated Core Logic Failure**: Modify `sp_k_ausd_v_ta_cntrct_crs2` to `RAISE SCRIPT_EXCEPTION('Simulated core logic error');` and then call `sp_vertragsdatenabgleich` as above.
        *Expected*: `sp_vertragsdatenabgleich` should catch the exception, log a 'FAILED' status in `job_execution_log`, and create an entry in `job_error_log`.

2.  **Integration Test (End-to-End)**:
    *   Once `sp_k_ausd_v_ta_cntrct_crs2` is fully implemented with the actual business logic, execute `sp_vertragsdatenabgleich` with production-like parameters.
    *   Compare the data output, transformations, and side effects (e.g., target table updates) with the results from the original `r_ausd_v_ta_cntrct_crs2.ksh` script. This may involve running both jobs in parallel on identical datasets and comparing the outcomes.

### What "passing" means:

*   The `sp_vertragsdatenabgleich` stored procedure completes execution without unhandled exceptions.
*   For successful runs, the `job_execution_log` table contains an entry with `status = 'OK'` and accurate `start_timestamp` and `end_timestamp`.
*   For failed runs (including simulated failures), the `job_execution_log` table contains an entry with `status = 'FAILED'`, and the `job_error_log` table contains a corresponding detailed error entry.
*   All input parameters (`p_job_kennung`, `p_s`, `p_l`) are correctly parsed, validated, and passed to the core logic procedure.
*   The `sp_k_ausd_v_ta_cntrct_crs2` (core logic) procedure is invoked correctly with the expected parameters.
*   (For integration tests) The data transformations and final state of target tables produced by the migrated BigQuery job are identical to those produced by the original KornShell script.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after deployment, the following rollback procedure can be followed to revert to the original KornShell script:

1.  **Deactivate New BigQuery Job**:
    *   If scheduled via Cloud Composer/Airflow, pause or delete the DAG.
    *   If scheduled via Cloud Workflows or BigQuery Scheduled Queries, disable or delete the schedule.
    *   Update the `is_active` flag in `config_job_control` for `TA_CNTRCT_CRS2` to `FALSE` to prevent accidental manual execution.
        ```sql
        UPDATE `YOUR_PROJECT_ID.YOUR_DATASET_ID.config_job_control`
        SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = 'TA_CNTRCT_CRS2';
        ```
2.  **Re-enable Original KornShell Script**:
    *   Re-enable the original `r_ausd_v_ta_cntrct_crs2.ksh` script in its legacy scheduler.
3.  **Data Rollback (if necessary)**:
    *   If the migrated `sp_k_ausd_v_ta_cntrct_crs2` (core logic) made any data modifications to production tables, a data rollback strategy must be executed. This could involve:
        *   Restoring affected tables from a point-in-time backup.
        *   Executing compensating transactions or scripts to revert changes.
        *   Using BigQuery's time travel feature to query data as of a previous timestamp and potentially restore it.
    *   **Note**: The wrapper script itself does not modify data, but its core logic does. This step is crucial and depends on the specific implementation of `sp_k_ausd_v_ta_cntrct_crs2`.
4.  **Optional: Clean Up BigQuery Objects**:
    *   If the rollback is permanent, the newly created BigQuery objects can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`;
        DROP PROCEDURE IF EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_k_ausd_v_ta_cntrct_crs2`;
        DROP TABLE IF EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_execution_log`;
        DROP TABLE IF EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_error_log`;
        -- Consider if config_job_control should be dropped or just the entry removed
        -- DELETE FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.config_job_control` WHERE job_kennung = 'TA_CNTRCT_CRS2';
        ```
    *   It is generally recommended to retain log tables for historical auditing unless storage is a critical concern.