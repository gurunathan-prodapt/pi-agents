# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh`. This script, originally responsible for orchestrating the provisioning of selected base products for the BERT system, including environment setup, parameter parsing, date determination, and logging, has been refactored.

The migration target platform is Google BigQuery. The original shell script's orchestration and control flow logic have been translated into a BigQuery Stored Procedure, which manages input parameters, logging, and invokes a separate BigQuery Stored Procedure for the core business logic. Logging has been centralized into a dedicated BigQuery table.

## 2. Generated artifacts

The migration process generated the following BigQuery-native artifacts:

*   **`project.dataset.job_log.sql`**
    *   **Role**: This DDL script creates the `job_log` table in BigQuery. This table serves as the centralized logging mechanism for the migrated job, replacing the file-based logging of the original KornShell script. It captures job execution details, status updates, and error messages.
*   **`project.dataset.ausd_bp_ta_bpr_beschr_core.sql`**
    *   **Role**: This DDL script creates a BigQuery Stored Procedure named `ausd_bp_ta_bpr_beschr_core`. This procedure is a *placeholder* for the core business logic originally contained within `k_ausd_bp_ta_bpr_beschr.ksh`. It is designed to be invoked by the wrapper procedure and will contain the actual data processing, transformation, and loading logic once fully implemented.
*   **`project.dataset.ausd_bp_ta_bpr_beschr_wrapper.sql`**
    *   **Role**: This DDL script creates the primary BigQuery Stored Procedure, `ausd_bp_ta_bpr_beschr_wrapper`. This procedure directly replaces the `r_ausd_bp_ta_bpr_beschr.ksh` KornShell script. Its responsibilities include:
        *   Accepting input parameters (`p_stichtag`, `p_wiederanlaufWert`).
        *   Initializing job-specific variables and determining effective dates.
        *   Performing parameter validation.
        *   Logging job start, progress, and parameters to `project.dataset.job_log`.
        *   Invoking the `project.dataset.ausd_bp_ta_bpr_beschr_core` procedure to execute the main business logic.
        *   Handling and logging errors using BigQuery's `EXCEPTION WHEN ERROR` block.
        *   Logging job completion status.

## 3. Key design decisions

The following key design decisions guided the migration to BigQuery:

*   **BigQuery Stored Procedures for Orchestration and Logic**:
    *   **Why**: BigQuery Stored Procedures offer a native, SQL-centric way to encapsulate complex logic, manage control flow, and execute data manipulation statements directly within BigQuery. This aligns with the goal of a BigQuery-native solution, leveraging BigQuery's performance and scalability.
    *   **Trade-offs**: While powerful for SQL-based logic, BigQuery Stored Procedures are less suited for complex filesystem operations or interactions with external non-GCP services. For such scenarios, Cloud Functions or Cloud Composer would be more appropriate, but were deemed unnecessary for this wrapper script.
*   **Separation of Wrapper and Core Logic**:
    *   **Why**: The original KornShell script clearly separated orchestration (`r_ausd_bp_ta_bpr_beschr.ksh`) from core business logic (`k_ausd_bp_ta_bpr_beschr.ksh`). This separation was maintained by creating two distinct BigQuery Stored Procedures (`_wrapper` and `_core`). This promotes modularity, reusability, and allows for independent development and testing of the core business logic.
    *   **Trade-offs**: Requires two separate BigQuery objects and a clear interface (parameters) between them.
*   **Centralized BigQuery `job_log` Table for Logging**:
    *   **Why**: Replaces disparate file-based logging with a structured, queryable, and centralized log table in BigQuery. This simplifies auditing, monitoring, and troubleshooting, allowing for easy analysis of job execution history and errors using standard SQL.
    *   **Trade-offs**: Requires DDL creation and `INSERT` statements for every log event, which adds minor overhead compared to simple file appends. However, the benefits of structured logging outweigh this.
*   **BigQuery SQL for Parameter Handling and Validation**:
    *   **Why**: Leverages BigQuery's `IN` parameters for procedures, `DECLARE`d variables, and built-in functions (`IFNULL`, `TRIM`, `FORMAT_DATE`, `CURRENT_DATE()`) for robust parameter parsing, defaulting, and validation. This eliminates the need for external shell utilities.
    *   **Trade-offs**: Translating shell-specific parameter parsing logic (e.g., `getopts`) into SQL control flow can sometimes be verbose, but it ensures all logic resides within the BigQuery environment.
*   **BigQuery `BEGIN...EXCEPTION WHEN ERROR THEN...END` for Error Handling**:
    *   **Why**: Provides a structured and robust mechanism to catch and handle errors during procedure execution, similar to `trap` commands in shell scripts. This ensures that job failures are properly logged and the procedure exits gracefully (or re-raises the error) with appropriate messages.
    *   **Trade-offs**: Requires careful placement of `BEGIN...END` blocks to capture specific error scopes.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
2.  **IAM Permissions Configuration**:
    *   Grant appropriate IAM roles to the service account or user that will deploy and execute these procedures.
        *   `BigQuery Data Editor` (or `BigQuery Admin`) for creating tables and stored procedures.
        *   `BigQuery Data Viewer` for reading from `job_log` (for monitoring).
        *   `BigQuery Job User` for executing stored procedures.
        *   Permissions to read/write to any source/target tables that `ausd_bp_ta_bpr_beschr_core` will interact with.
3.  **Deploy `project.dataset.job_log` Table**:
    *   Execute the DDL from `project.dataset.job_log.sql` to create the logging table.
    ```bash
    bq query --use_legacy_sql=false < project.dataset.job_log.sql
    ```
4.  **Implement and Deploy `project.dataset.ausd_bp_ta_bpr_beschr_core`**:
    *   **Crucial Step**: The provided `ausd_bp_ta_bpr_beschr_core.sql` is a placeholder. The full business logic from `k_ausd_bp_ta_bpr_beschr.ksh` must be analyzed, translated into BigQuery SQL, and implemented within this stored procedure.
    *   Once implemented, deploy the procedure:
        ```bash
        bq query --use_legacy_sql=false < project.dataset.ausd_bp_ta_bpr_beschr_core.sql
        ```
5.  **Deploy `project.dataset.ausd_bp_ta_bpr_beschr_wrapper` Procedure**:
    *   Execute the DDL from `project.dataset.ausd_bp_ta_bpr_beschr_wrapper.sql` to create the wrapper procedure.
    ```bash
    bq query --use_legacy_sql=false < project.dataset.ausd_bp_ta_bpr_beschr_wrapper.sql
    ```
6.  **Scheduling Configuration**:
    *   Determine the appropriate scheduling mechanism (e.g., Cloud Composer DAG, Cloud Workflows, BigQuery Scheduled Query) and configure it to invoke `CALL project.dataset.ausd_bp_ta_bpr_beschr_wrapper(p_stichtag => '...', p_wiederanlaufWert => ...);` with the required parameters.

## 5. Known gaps & unresolved references

*   **Core Logic Implementation (B4 Item)**: The most significant gap is the complete implementation of the `project.dataset.ausd_bp_ta_bpr_beschr_core` stored procedure. The current version is a placeholder. The detailed analysis and translation of `k_ausd_bp_ta_bpr_beschr.ksh` into BigQuery SQL is a prerequisite for full functionality.
*   **Dynamic OS Interactions**: While the wrapper script itself had minimal direct OS interactions beyond sourcing scripts, if the `k_ausd_bp_ta_bpr_beschr.ksh` kernel script contains calls to external binaries, complex filesystem operations, or non-SQL specific utilities, these elements cannot be directly translated to BigQuery SQL. They would require re-design using other GCP services (e.g., Cloud Functions, Cloud Composer) and integration with the BigQuery workflow. This is flagged as a potential risk in the design document.
*   **Performance Tuning**: Once the core logic is implemented and integrated, thorough performance testing and tuning will be required, especially for large datasets and complex operations within `ausd_bp_ta_bpr_beschr_core`.
*   **Data Integrity and Idempotency**: The `Wiederanlaufwert` parameter implies specific restart logic and idempotency requirements. The exact implementation of this logic within `ausd_bp_ta_bpr_beschr_core` (e.g., using `MERGE` statements, conditional deletes/inserts) needs careful design and validation to ensure data integrity during restarts.

## 6. Validation

Validation of the migrated job involves testing the wrapper's parameter handling, logging, error handling, and its ability to correctly invoke the core procedure.

**How to run the tests:**

1.  **Manual Execution via BigQuery Console/CLI**:
    *   **Test Case 1: Successful Execution (with parameters)**
        ```sql
        CALL `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`(p_stichtag => '01012023', p_wiederanlaufWert => 0);
        ```
    *   **Test Case 2: Successful Execution (without optional parameters, relying on defaults)**
        ```sql
        CALL `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`(p_stichtag => NULL, p_wiederanlaufWert => NULL);
        -- Or simply:
        CALL `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`(NULL, NULL);
        ```
    *   **Test Case 3: Parameter Validation Error (e.g., empty Stichtag after default)**
        ```sql
        -- This should trigger the 'Stichtag parameter missing' error if the default calculation also results in NULL/empty
        CALL `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`(p_stichtag => '', p_wiederanlaufWert => 0);
        ```
    *   **Test Case 4: Simulated Core Procedure Error (once `_core` is implemented to simulate errors)**
        *   Modify `ausd_bp_ta_bpr_beschr_core` temporarily to `SELECT ERROR('Simulated error');` under certain conditions (e.g., `IF p_wiederanlaufWert = 999 THEN SELECT ERROR('Simulated error'); END IF;`).
        *   Then call the wrapper with the condition:
            ```sql
            CALL `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`(p_stichtag => '01012023', p_wiederanlaufWert => 999);
            ```
2.  **Query the `job_log` table**: After each execution, query the `project.dataset.job_log` table to verify log entries.
    ```sql
    SELECT * FROM `project.dataset.job_log` WHERE job_name = 'ausd_bp_ta_bpr_beschr' ORDER BY created_at DESC LIMIT 10;
    ```

**What "passing" means:**

*   **Successful Execution**:
    *   The `CALL` statement completes without an explicit BigQuery error message (unless testing error conditions).
    *   The `job_log` table contains a sequence of `INFO` messages for the specific `job_number`, including "Job started", parameter details, "Core procedure called...", and finally "Job completed successfully".
    *   (Once `_core` is implemented) The data processed by `ausd_bp_ta_bpr_beschr_core` is correct and matches expected output.
*   **Parameter Validation Error**:
    *   The `CALL` statement returns an error message indicating a missing or invalid parameter (e.g., "Required parameter Stichtag is missing").
    *   The `job_log` table contains an `ERROR` message corresponding to the parameter validation failure, and no "Job completed successfully" message.
*   **Core Procedure Error**:
    *   The `CALL` statement returns an error message originating from the `EXCEPTION WHEN ERROR` block in the wrapper, indicating "AppError: Abbruch" and potentially details from the core procedure's error.
    *   The `job_log` table contains an `ERROR` message from the wrapper's error handler, indicating "AppError: Abbruch" and the specific error details from the core procedure. No "Job completed successfully" message should be present.

## 7. Rollback procedure

In case of issues or if the migrated job needs to be reverted, follow these steps to roll back to the original KornShell script execution:

1.  **Stop New BigQuery Job Schedules**:
    *   Immediately disable or delete any scheduled executions (e.g., Cloud Composer DAGs, Cloud Workflows, BigQuery Scheduled Queries) that invoke `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`.
2.  **Verify Original Script Functionality**:
    *   Ensure the original KornShell script (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh`) and its dependencies are still present and functional in the legacy environment.
    *   If any changes were made to the original environment (e.g., data sources redirected), revert those changes.
3.  **Re-enable Original Scheduling**:
    *   Re-enable the original scheduling mechanism for `r_ausd_bp_ta_bpr_beschr.ksh` in the legacy environment.
4.  **Delete BigQuery Stored Procedures**:
    *   Delete the migrated BigQuery stored procedures.
    ```bash
    bq rm -f -r project.dataset.ausd_bp_ta_bpr_beschr_wrapper
    bq rm -f -r project.dataset.ausd_bp_ta_bpr_beschr_core
    ```
5.  **Retain or Delete `job_log` Table**:
    *   The `project.dataset.job_log` table can be retained for historical auditing of the migration attempt, or deleted if no longer needed.
    ```bash
    # To delete:
    bq rm -f project.dataset.job_log
    ```
6.  **Data Rollback (if necessary)**:
    *   If the partially migrated `ausd_bp_ta_bpr_beschr_core` procedure made any data modifications that need to be undone, execute appropriate BigQuery DML statements to revert the data to its state before the migration attempt. This step is highly dependent on the specific actions of the `_core` procedure.