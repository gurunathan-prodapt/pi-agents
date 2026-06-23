# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh`. This script, originally responsible for orchestrating the initial provisioning of base products for the BERT system, including parameter handling, logging, job tracking, and invoking a core provisioning script, has been migrated to a BigQuery-native solution.

The target platform is Google Cloud BigQuery, where the wrapper's functionality is now encapsulated within a BigQuery Stored Procedure, `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`. Job control and logging mechanisms have been replaced with dedicated BigQuery tables (`project.dataset.job_control` and `project.dataset.job_log`).

## 2. Generated artifacts

The migration process generated the following BigQuery components:

*   **`SQL/ddl/job_control.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_control` table in BigQuery. This table serves as the central repository for tracking the status, metadata, and lifecycle of job executions, replacing the legacy internal job tracking system.
*   **`SQL/ddl/job_log.sql`**
    *   **Role:** Defines the DDL for the `job_log` table in BigQuery. This table stores detailed log messages, informational output, and error details generated during job execution, replacing the file-based logging of the original KornShell script.
*   **`SQL/sprocs/ausd_bp_ta_bpr_optionen_wrapper.sql`**
    *   **Role:** This BigQuery Stored Procedure, named `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`, is the direct replacement for the original `r_ausd_bp_ta_bpr_optionen.ksh` KornShell script. Its responsibilities include:
        *   Parsing and validating input parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`).
        *   Applying default values for parameters if not provided.
        *   Generating unique job identifiers and tracking numbers.
        *   Managing job status and logging messages to the `job_control` and `job_log` tables.
        *   Implementing robust error handling using BigQuery's `BEGIN...EXCEPTION` blocks.
        *   Invoking the core business logic, which is expected to be migrated to a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_bp_ta_bpr_optionen`).

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration**: The decision to use a BigQuery Stored Procedure (`ausd_bp_ta_bpr_optionen_wrapper`) for the wrapper logic was made to leverage BigQuery's native capabilities for job control, parameter handling, and direct invocation of other BigQuery components. This approach aligns with a BigQuery-native solution, minimizing the need for external orchestration tools for this specific wrapper's logic.
*   **Dedicated BigQuery Tables for Job Control and Logging**: Instead of replicating file-based logging and an internal job tracking system, dedicated BigQuery tables (`job_control` and `job_log`) were created. This centralizes job metadata and logs within BigQuery, making them easily queryable, auditable, and integrated with the BigQuery ecosystem.
*   **Structured Error Handling with `BEGIN...EXCEPTION`**: The KornShell `trap` mechanism for error handling was replaced with BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides a more robust, structured, and SQL-native way to catch and manage errors, ensuring proper logging and status updates in case of failures.
*   **Direct `CALL` for Core Logic**: The design assumes that the core business logic, originally in `k_ausd_bp_ta_bpr_optionen.ksh`, will also be migrated to a BigQuery Stored Procedure. This allows the wrapper to directly `CALL` the core logic within the BigQuery environment, maintaining a seamless, in-database execution flow.
*   **Parameter Handling via Stored Procedure Arguments**: The KornShell `getopts` mechanism for command-line arguments was replaced by direct `IN` parameters for the BigQuery Stored Procedure. This is the standard and most efficient way to pass parameters in BigQuery.
*   **Trade-off: `MAX(job_nr) + 1` for `DW_EintragsNr`**: For simplicity in the initial migration, the `DW_EintragsNr` (job entry number) is generated using `SELECT IFNULL(MAX(job_nr), 0) + 1 FROM job_control`. This approach is known to be susceptible to race conditions in high-concurrency environments. While acceptable for initial deployment, it is flagged for future robustness improvements (e.g., using BigQuery sequences or more atomic update mechanisms).

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be performed:

1.  **BigQuery Dataset Creation**: Ensure that the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **Table Creation**: Execute the DDL scripts to create the necessary job control and logging tables:
    *   Run `SQL/ddl/job_control.sql`
    *   Run `SQL/ddl/job_log.sql`
3.  **Stored Procedure Deployment**: Deploy the `ausd_bp_ta_bpr_optionen_wrapper` stored procedure by executing `SQL/sprocs/ausd_bp_ta_bpr_optionen_wrapper.sql` in BigQuery.
4.  **Core Logic Deployment**: Ensure that the corresponding BigQuery Stored Procedure for the core business logic (`project.dataset.k_ausd_bp_ta_bpr_optionen`) is also deployed and accessible.
5.  **IAM Permissions**: Grant the appropriate Identity and Access Management (IAM) roles to the service account or user that will be executing the `ausd_bp_ta_bpr_optionen_wrapper` stored procedure. This typically includes:
    *   `BigQuery Data Editor` or `BigQuery Data Owner` on the `project.dataset` to allow `INSERT` into `job_control` and `job_log`, `UPDATE` on `job_control`, and `CALL` other stored procedures.
    *   `BigQuery Job User` to run BigQuery jobs.
6.  **Scheduling Configuration**: Set up a scheduling mechanism to invoke the `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` stored procedure. This could be:
    *   A BigQuery Scheduled Query.
    *   A Cloud Composer (Airflow) DAG.
    *   A Cloud Workflows definition.
    *   A Cloud Function triggered by a schedule.
    *   Ensure the scheduler passes the required `p_stichtag_in` and `p_wiederanlaufWert_in` parameters correctly.
7.  **Configuration of `BERT_DIR_ROOT` (if applicable)**: If the `BERT_DIR_ROOT` value is still relevant for the core logic or other parts of the system, ensure its equivalent (e.g., a configuration parameter or constant) is correctly set within the BigQuery environment or passed to the core procedure.

## 5. Known gaps & unresolved references

The following items have been identified as known gaps or require further follow-up:

*   **Core Script Logic (`k_ausd_bp_ta_bpr_optionen.ksh`)**: The content and migration strategy for the core provisioning script (`k_ausd_bp_ta_bpr_optionen.ksh`) are outside the scope of this wrapper migration. This design *assumes* it will be migrated to a callable BigQuery component (`project.dataset.k_ausd_bp_ta_bpr_optionen`). Its specific dependencies, data flow, and potential external system integrations need to be assessed and migrated separately.
*   **`DW_EintragsNr` Generation Concurrency**: The current `MAX(job_nr) + 1` approach for generating `DW_EintragsNr` is prone to race conditions in highly concurrent environments. For production systems with frequent or parallel executions, a more robust mechanism (e.g., BigQuery sequences, atomic `INSERT` with `SELECT MAX(...)`) should be implemented to guarantee unique and sequential job numbers.
*   **Detailed Log File Content Replication**: While the `job_log` table captures essential messages, ensuring an exact, byte-for-byte replication of all specific log messages and their formats from the original KornShell script's file output might require further detailed mapping and potentially additional `INSERT` statements within the stored procedure.
*   **`BERT_DIR_ROOT` Definition**: The `BERT_DIR_ROOT` environment variable was crucial for locating other scripts in the legacy environment. Its equivalent definition and how it will be mapped or passed as a configuration parameter within the BigQuery context needs to be finalized.
*   **`semi_auto` Migration Bucket Implications**: The original job's classification as `semi_auto` indicates that manual intervention and refinement were necessary beyond direct automated translation. This primarily stemmed from the need to re-implement custom shell functions (e.g., for logging, date handling, parameter parsing) and environment sourcing using BigQuery-native constructs.

## 6. Validation

Validation of the migrated `ausd_bp_ta_bpr_optionen_wrapper` stored procedure involves verifying its functionality under various scenarios.

**How to run the tests:**

1.  **Prerequisites**: Ensure all DDLs are executed, the wrapper stored procedure is deployed, and a mock or actual `project.dataset.k_ausd_bp_ta_bpr_optionen` stored procedure exists (even if it just returns successfully or raises a controlled error for testing purposes).
2.  **Execute with all parameters**:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`('25122023', 1);
    ```
3.  **Execute with default `p_stichtag_in`**:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(NULL, 1);
    ```
4.  **Execute with default `p_wiederanlaufWert_in`**:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`('01012024', NULL);
    ```
5.  **Execute with both defaults**:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(NULL, NULL);
    ```
6.  **Simulate core logic failure**: If possible, configure the mock `k_ausd_bp_ta_bpr_optionen` to `SIGNAL SQLSTATE '45000'` to test error handling.
    ```sql
    -- Example of a mock k_ausd_bp_ta_bpr_optionen that fails
    CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_optionen`(
        p_job_kennung STRING, p_stichtag STRING, p_dw_eintrags_nr INT64, p_wiederanlaufWert INT64
    )
    BEGIN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core logic failure.';
    END;
    -- Then call the wrapper
    CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`('01012024', 0);
    ```

**What "passing" means:**

*   **Successful Execution**:
    *   The `CALL` statement completes without raising an error.
    *   A new record is present in `project.dataset.job_control` with:
        *   `status = 'OK'`
        *   `finished_at` populated.
        *   `stichtag_info` and other fields reflecting the input parameters or their defaults.
    *   Corresponding `INFO` level messages are present in `project.dataset.job_log` for job start and successful completion, linked by `job_nr` and `job_kennung`.
    *   The `CALL` to `project.dataset.k_ausd_bp_ta_bpr_optionen` (or its mock) was made with the correct parameters derived from the wrapper's inputs.
*   **Failed Execution (Core Logic Failure)**:
    *   The `CALL` statement for the wrapper procedure terminates with an error (e.g., `SQLSTATE '45000'`).
    *   A new record is present in `project.dataset.job_control` with:
        *   `status = 'FAILED'`
        *   `finished_at` populated.
    *   Corresponding `ERROR` level messages are present in `project.dataset.job_log`, detailing the failure, linked by `job_nr` and `job_kennung`.
    *   The transaction initiated by the wrapper is rolled back, ensuring no partial updates from the wrapper itself (though the core procedure's transaction behavior needs separate validation).
*   **Parameter Defaulting**: When `NULL` is passed for `p_stichtag_in` or `p_wiederanlaufWert_in`, the `job_control` and `job_log` entries should reflect the correctly applied default values (current date for `stichtag`, `0` for `wiederanlaufWert`).

## 7. Rollback procedure

In the event that the migrated BigQuery solution needs to be reverted to the original KornShell script, follow these steps:

1.  **Disable New Components**:
    *   **Stop Scheduling**: Immediately disable or delete any BigQuery Scheduled Queries, Cloud Composer DAGs, Cloud Workflows, or other mechanisms that invoke the `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` stored procedure.
    *   **Prevent Execution**: (Optional but recommended) To prevent accidental execution, you can either:
        *   Rename the `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` stored procedure.
        *   Revoke `bigquery.routines.execute` permissions from the service accounts or users that would normally run it.
2.  **Re-enable Original Script**:
    *   Ensure the original KornShell script, `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh`, is available, executable, and correctly configured in its legacy environment.
    *   Re-establish its original scheduling mechanism (e.g., cron job, legacy scheduler).
3.  **Data Consistency Considerations**:
    *   The `job_control` and `job_log` tables in BigQuery will retain records of executions that occurred during the migration period. These records can be useful for auditing but will no longer be updated by the rolled-back system.
    *   Any data processed or provisioned by the *core* logic (whether the legacy `k_ausd_bp_ta_bpr_optionen.ksh` or its BigQuery counterpart if it was also deployed) during the migration period will remain. The rollback procedure primarily affects the orchestration layer.
    *   If the core logic was also migrated and needs to be rolled back, its specific rollback procedure must be followed separately.