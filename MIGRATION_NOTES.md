# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh` has been migrated to Google Cloud Platform, specifically to BigQuery.

This script, originally an orchestration wrapper, is responsible for:
*   Parsing input parameters (`Stichtag` and `Wiederanlaufwert`).
*   Setting up the execution environment.
*   Handling logging and error reporting.
*   Invoking a core kernel script (`k_ausd_bp_ta_bpr_bcp.ksh`) to perform the actual data processing for provisioning base products for BERT.

The migration involved transforming the KornShell logic into a BigQuery Stored Procedure (`p_ausd_bp_ta_bpr_bcp`) and replacing the file-based logging with a dedicated BigQuery logging table (`job_log`). The core business logic of `k_ausd_bp_ta_bpr_bcp.ksh` is assumed to be migrated to a separate BigQuery Stored Procedure (`p_k_ausd_bp_ta_bpr_bcp`) which will be called by this wrapper.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_log.sql`**
    *   **Role**: This DDL script creates the `job_log` table in BigQuery. This table serves as a centralized, queryable repository for all job execution metadata, status updates, informational messages, and error details, replacing the original file-based logging mechanism.
*   **`sql/procedures/p_ausd_bp_ta_bpr_bcp.sql`**
    *   **Role**: This DDL script creates the BigQuery Stored Procedure `p_ausd_bp_ta_bpr_bcp`. This procedure encapsulates the entire orchestration logic of the original `r_ausd_bp_ta_bpr_bcp.ksh` script, including:
        *   Parameter parsing and validation (`in_stichtag`, `in_wiederanlaufWert`).
        *   Defaulting logic for missing parameters.
        *   Comprehensive error handling using BigQuery's `EXCEPTION WHEN ERROR` blocks.
        *   Logging of job lifecycle events (start, info, success, failure) to the `job_log` table.
        *   Invoking the core business logic procedure (`p_k_ausd_bp_ta_bpr_bcp`).

## 3. Key Design Decisions

*   **Migration to BigQuery Stored Procedure**:
    *   **Why**: Aligning with the target BigQuery data warehouse architecture, a BigQuery Stored Procedure provides a native, scalable, and performant way to execute the orchestration logic directly within the data platform. This allows for seamless integration with other BigQuery components, such as the future `p_k_ausd_bp_ta_bpr_bcp` procedure.
    *   **Trade-offs**: Required re-implementation of shell-specific constructs (e.g., `getopts`, `trap`, environment variable sourcing) using BigQuery SQL scripting capabilities. Direct OS-level interactions are no longer possible.
*   **Centralized BigQuery `job_log` Table for Logging**:
    *   **Why**: Replaces disparate file-based logs with a structured, queryable, and highly available logging solution within BigQuery. This significantly improves monitoring, debugging, and auditing capabilities by centralizing all job execution information.
    *   **Trade-offs**: Requires explicit `INSERT` statements into the `job_log` table for every log event, rather than simple `echo` or `tee` commands.
*   **In-Procedure Parameter Handling and Defaulting**:
    *   **Why**: The logic for parsing, validating, and defaulting `Stichtag` and `Wiederanlaufwert` is directly embedded within the `p_ausd_bp_ta_bpr_bcp` procedure. This makes the procedure self-contained and ensures consistent parameter handling regardless of how it's invoked.
    *   **Trade-offs**: Requires careful translation of shell-based date formatting and integer parsing/validation to BigQuery SQL functions (`FORMAT_DATE`, `SAFE.PARSE_DATE`, `CAST`).
*   **BigQuery `BEGIN...EXCEPTION...END` for Error Handling**:
    *   **Why**: Replaces the KornShell `trap` mechanism with BigQuery's native, structured error handling. This ensures that any runtime errors are caught, logged appropriately to the `job_log` table with detailed messages, and re-raised to propagate failure status to the caller.
    *   **Trade-offs**: Different error handling paradigm; requires explicit blocks and `RAISE` statements.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` as referenced in the generated code) exists. If not, create it in your GCP project.
2.  **IAM Permissions Configuration**:
    *   The service account or user identity that will execute the BigQuery Stored Procedure (`p_ausd_bp_ta_bpr_bcp`) must have the necessary IAM roles and permissions:
        *   `BigQuery Data Editor` or `BigQuery Data Owner` on the `project.dataset` to create/update the `job_log` table and execute routines.
        *   `BigQuery Routine User` on `project.dataset` to call the `p_k_ausd_bp_ta_bpr_bcp` procedure.
        *   `BigQuery Job User` to run BigQuery jobs.
3.  **Deployment of `p_k_ausd_bp_ta_bpr_bcp`**:
    *   The core kernel procedure (`p_k_ausd_bp_ta_bpr_bcp`) which contains the actual business logic *must* be migrated and deployed to `project.dataset` before `p_ausd_bp_ta_bpr_bcp` can be successfully executed. This is a critical prerequisite.
4.  **Scheduling Configuration**:
    *   The original job was likely scheduled via UC4. A new scheduling mechanism needs to be configured in GCP to invoke the `p_ausd_bp_ta_bpr_bcp` BigQuery Stored Procedure. Recommended options include:
        *   **Cloud Composer (Apache Airflow)**: Create an Airflow DAG that uses the `BigQueryOperator` to call the stored procedure, passing parameters as needed. This is ideal for complex workflows or existing Airflow environments.
        *   **Cloud Workflows + Cloud Scheduler**: Use Cloud Scheduler to trigger a Cloud Workflow that executes the BigQuery procedure. This offers a serverless orchestration option.
        *   **BigQuery Scheduled Queries**: While primarily for SQL queries, it can be adapted to call stored procedures if no complex external orchestration is required.
    *   Ensure the scheduler passes the `in_stichtag`, `in_wiederanlaufWert`, `in_job_source`, and `in_run_mode` parameters correctly.
5.  **Connection Strings/Secrets**:
    *   This wrapper procedure itself does not directly handle external connection strings or secrets. However, if the invoked `p_k_ausd_bp_ta_bpr_bcp` procedure requires access to external systems or sensitive data, ensure those connections and secrets are securely managed (e.g., via Secret Manager) and configured for the `p_k_ausd_bp_ta_bpr_bcp` procedure.

## 5. Known Gaps & Unresolved References

*   **Core Kernel Script (`k_ausd_bp_ta_bpr_bcp.ksh`) Migration (B4 Item)**: The most significant gap is that the actual business logic residing in `k_ausd_bp_ta_bpr_bcp.ksh` has not been migrated as part of this scope. The `p_ausd_bp_ta_bpr_bcp` procedure *assumes* the existence and successful migration of `p_k_ausd_bp_ta_bpr_bcp`. Without this, the wrapper will execute but the core data processing will not occur. This is a critical follow-up item.
*   **Full Framework Functionality**: While basic parameter parsing and logging are implemented, the full scope of the original KornShell helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, including functions like `DWMSG_*`, `DWDate_*`, `pruefeParameterGesetzt`) needs further review. Some functionalities might be fully replaced by BigQuery native features, while others might require dedicated BigQuery UDFs or procedures if their complexity warrants it beyond the current inline implementation.
*   **Dynamic `job_kennung`**: The `v_job_kennung` variable in the BigQuery procedure is currently a static placeholder (`'DW.BERT.BP_TA_BPR_BCP'`). If the original KornShell script derived this value dynamically, that logic needs to be identified and replicated in the BigQuery procedure.
*   **Environment Context (`$HOME/.dw_init`)**: The original script sourced `$HOME/.dw_init` for environment variables. Any critical variables from this file that are not explicitly handled as procedure parameters or BigQuery session variables might be missing. A thorough analysis of `.dw_init` is required to ensure all necessary environment settings are replicated.
*   **UC4 Integration Redesign**: The original UC4 job invocation needs to be fully redesigned to trigger the new BigQuery procedure, likely through Cloud Composer or Cloud Workflows. This redesign should cover parameter mapping and error reporting back to the enterprise scheduler if required.

## 6. Validation

Validation ensures the migrated BigQuery Stored Procedure functions equivalently to the original KornShell script.

### Deployment Verification

1.  Confirm the `project.dataset.job_log` table exists in BigQuery.
2.  Confirm the `project.dataset.p_ausd_bp_ta_bpr_bcp` stored procedure exists.
3.  **Crucially**: Confirm the `project.dataset.p_k_ausd_bp_ta_bpr_bcp` stored procedure (even if it's a mock/stub for initial testing) exists and is callable.

### Functional Testing

Execute the `p_ausd_bp_ta_bpr_bcp` procedure with various parameter combinations and verify the outcomes in the `job_log` table and the behavior of the invoked kernel procedure.

**Passing Criteria**: For all successful test cases, the `job_log` table should contain entries for `RUNNING`, `INFO` (for parameter processing and kernel call), and `SUCCESS` for the specific `job_run_id`. For failed cases, a `FAILED` entry with an appropriate error message should be present.

1.  **Test Case 1: Successful Execution (All parameters provided)**
    ```sql
    CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', '1', 'TEST_SOURCE', 'DEV');
    ```
    *   **Expected**: Procedure completes successfully. `job_log` shows `Stichtag` as '01012023' and `Wiederanlaufwert` as 1. The `p_k_ausd_bp_ta_bpr_bcp` procedure is called.
2.  **Test Case 2: Successful Execution (Default Stichtag)**
    ```sql
    CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`(NULL, '0', 'TEST_SOURCE', 'DEV');
    -- Also test with empty string: CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('', '0', 'TEST_SOURCE', 'DEV');
    ```
    *   **Expected**: Procedure completes successfully. `job_log` shows `Stichtag` defaulted to the current system date (e.g., `DDMMYYYY` format).
3.  **Test Case 3: Successful Execution (Default Wiederanlaufwert)**
    ```sql
    CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', NULL, 'TEST_SOURCE', 'DEV');
    -- Also test with empty string: CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', '', 'TEST_SOURCE', 'DEV');
    ```
    *   **Expected**: Procedure completes successfully. `job_log` shows `Wiederanlaufwert` defaulted to `0`.
4.  **Test Case 4: Failed Execution (Invalid Stichtag format)**
    ```sql
    CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('2023-01-01', '0', 'TEST_SOURCE', 'DEV');
    ```
    *   **Expected**: Procedure fails with an error message indicating an invalid `Stichtag` format. `job_log` contains a `FAILED` entry with the error details.
5.  **Test Case 5: Failed Execution (Invalid Wiederanlaufwert format)**
    ```sql
    CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', 'abc', 'TEST_SOURCE', 'DEV');
    ```
    *   **Expected**: Procedure fails with an error message related to casting 'abc' to an integer. `job_log` contains a `FAILED` entry with the error details.
6.  **Test Case 6: Failed Execution (Invoked kernel procedure fails)**
    *   **Prerequisite**: Temporarily modify `p_k_ausd_bp_ta_bpr_bcp` to intentionally raise an error.
    ```sql
    CALL `project.dataset.p_ausd_bp_ta_bpr_bcp`('01012023', '0', 'TEST_SOURCE', 'DEV');
    ```
    *   **Expected**: The `p_ausd_bp_ta_bpr_bcp` procedure fails, and `job_log` contains a `FAILED` entry reflecting the error propagated from `p_k_ausd_bp_ta_bpr_bcp`.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after deployment, the following rollback procedure can be initiated:

1.  **Immediate Rollback (Post-Deployment Issues)**:
    *   If issues are detected immediately after deploying the BigQuery procedures (e.g., compilation errors, incorrect logging, or immediate functional failures):
        *   **Disable New Scheduling**: Immediately pause or delete the new GCP-based scheduler (e.g., Cloud Composer DAG, Cloud Scheduler job) that invokes `p_ausd_bp_ta_bpr_bcp`.
        *   **Re-enable Original Scheduling**: Re-point the original UC4 job back to the `r_ausd_bp_ta_bpr_bcp.ksh` script.
        *   **Revert BigQuery Procedures**:
            *   If a previous version of `p_ausd_bp_ta_bpr_bcp` existed, revert to that version.
            *   Otherwise, drop the `p_ausd_bp_ta_bpr_bcp` procedure: `DROP PROCEDURE IF EXISTS \`project.dataset.p_ausd_bp_ta_bpr_bcp\`;`
            *   The `job_log` table is additive and can typically remain, providing a history of attempts.
2.  **Long-Term Rollback (Post-Successful Runs / Data Issues)**:
    *   If issues are discovered after some successful runs, or if the `p_k_ausd_bp_ta_bpr_bcp` migration (which this wrapper calls) is found to be problematic:
        *   **Disable New Scheduling**: Pause or delete the new GCP-based scheduler.
        *   **Re-enable Original Scheduling**: Re-enable the original UC4 job pointing to `r_ausd_bp_ta_bpr_bcp.ksh`.
        *   **Analyze `job_log`**: Use the `job_log` table to analyze the failure patterns and diagnose the root cause.
        *   **Data Rollback (if applicable)**: This wrapper script primarily orchestrates and logs. Any actual data modifications are performed by the invoked `p_k_ausd_bp_ta_bpr_bcp` procedure. If data corruption or incorrect processing occurred, a separate data rollback strategy for the tables affected by `p_k_ausd_bp_ta_bpr_bcp` would be necessary (e.g., restoring from backups, point-in-time recovery, or running corrective scripts).
        *   **Re-plan/Re-migrate**: Address the identified issues, potentially redesigning parts of the migration, and then re-deploy the corrected BigQuery components.