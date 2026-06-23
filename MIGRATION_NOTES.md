# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `r_ausd_bp_ta_rn_vertrag.ksh`, which serves as an orchestration wrapper for creating a snapshot of the contract cache (`Vertrags-Cache`) for credit scoring within the Data Warehouse. The original script handles parameter parsing, date determination, logging, and invokes a core "kernel" script (`k_ausd_bp_ta_rn_vertrag.ksh`) for data processing.

The job has been migrated from a KornShell-based environment to **Google Cloud Platform (GCP)**, specifically leveraging **BigQuery SQL stored procedures** for both the wrapper and kernel logic, and a BigQuery table for centralized logging.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_audit.sql`**
    *   **Role**: Defines the schema for the `job_audit` BigQuery table. This table replaces the file-based logging of the original KornShell script, centralizing job execution metadata, status, and error information within BigQuery. It captures details like job entry number, status, error codes, timestamps, and parameters used.
*   **`sql/procedures/k_ausd_bp_ta_rn_vertrag.sql`**
    *   **Role**: This BigQuery SQL stored procedure encapsulates the core data manipulation logic previously found in the inferred `k_ausd_bp_ta_rn_vertrag.ksh` kernel script. It is responsible for performing a conditional `DELETE` and `INSERT` operation on the `project.dataset.fos_vertrag` table based on a restart value (`p_wiederanlaufWert`) and specific date conditions, using `project.dataset.ta_vertrag_cache` as its source.
*   **`sql/procedures/ausd_bp_ta_rn_vertrag_wrapper.sql`**
    *   **Role**: This BigQuery SQL stored procedure acts as the main orchestration wrapper, replacing the original `r_ausd_bp_ta_rn_vertrag.ksh` KornShell script. It handles input parameter validation, defaults for `p_stichtag` and `p_wiederanlaufWert`, generates a unique job entry number, logs job start/success/failure to the `job_audit` table, and orchestrates the call to the `k_ausd_bp_ta_rn_vertrag` stored procedure.

## 3. Key design decisions

*   **BigQuery Stored Procedures for all logic**: Both the orchestration wrapper and the core data processing logic were translated into BigQuery SQL stored procedures. This decision leverages BigQuery's native capabilities for procedural logic, parameter handling, and DML operations, keeping the entire job execution within the BigQuery ecosystem for performance and scalability.
*   **Centralized BigQuery Logging**: File-based logging was replaced by a dedicated `job_audit` BigQuery table. This provides a structured, queryable, and centralized repository for all job execution logs, simplifying monitoring, debugging, and auditing compared to distributed log files.
*   **Direct Translation of Shell Constructs**: Shell script functionalities like parameter parsing (`getopts`), date handling (`FORMAT_DATE`, `CURRENT_DATE()`), and error handling (`EXCEPTION WHEN ERROR`) were directly translated into their BigQuery SQL equivalents. This maintains functional parity with the original script's behavior.
*   **"Delete then Insert" for Restart Logic**: The restart mechanism, involving a conditional delete followed by an insert, was directly replicated in BigQuery SQL. While effective, this approach was noted as a potential area for future optimization (e.g., using `MERGE` statements for better idempotency and performance, especially on very large datasets).
*   **Schema-on-Write for Audit Table**: The `job_audit` table was designed with a fixed schema to ensure consistency and ease of querying for job metadata.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Data Migration**:
    *   Ensure that the source Oracle DWH tables, specifically `DWH$TA_C_VERTRAG` and any other related tables required by the kernel logic, have been fully migrated to BigQuery. These should be available as `project.dataset.ta_vertrag_cache` and `project.dataset.fos_vertrag` (the target table). The schemas of these BigQuery tables must accurately reflect their Oracle counterparts.
2.  **BigQuery Dataset Creation**:
    *   Verify that the target BigQuery dataset (`project.dataset`) exists. If not, create it.
3.  **BigQuery Table Creation**:
    *   Execute the DDL script `sql/ddl/job_audit.sql` to create the `job_audit` table in the target BigQuery dataset.
4.  **BigQuery Stored Procedure Deployment**:
    *   Execute the DDL scripts `sql/procedures/k_ausd_bp_ta_rn_vertrag.sql` and `sql/procedures/ausd_bp_ta_rn_vertrag_wrapper.sql` to create or replace the stored procedures in the target BigQuery dataset.
5.  **IAM Permissions**:
    *   Grant appropriate IAM roles to the service account or user that will execute the BigQuery stored procedures. This includes, but is not limited to:
        *   `BigQuery Data Editor` on `project.dataset` for `INSERT`, `DELETE`, and `SELECT` operations on `job_audit`, `ta_vertrag_cache`, and `fos_vertrag`.
        *   `BigQuery Job User` to run BigQuery jobs.
6.  **Orchestration Setup**:
    *   If external orchestration (e.g., Cloud Composer/Airflow, Cloud Workflows) is used to trigger `project.dataset.ausd_bp_ta_rn_vertrag_wrapper`, configure the respective DAG or workflow. This includes defining the schedule and passing any required parameters (`p_stichtag`, `p_wiederanlaufWert`).
7.  **Connection Strings/Secrets**:
    *   For BigQuery stored procedures, direct connection strings are not typically required as they execute within BigQuery. However, if the orchestration layer requires authentication (e.g., service account key files), ensure these are securely managed and configured.

## 5. Known gaps & unresolved references

*   **Full `k_ausd_bp_ta_rn_vertrag.ksh` Analysis**: The migration of the kernel script (`k_ausd_bp_ta_rn_vertrag.ksh`) was based on inferred logic from the MCP. A detailed, line-by-line analysis of the actual kernel script's content is crucial to ensure all business logic, complex SQL, joins, or transformations are accurately replicated in `project.dataset.k_ausd_bp_ta_rn_vertrag`. Any uncaptured logic could lead to data discrepancies.
*   **Error Message Translation**: The original KornShell script used specific error codes (e.g., `ErrNr=192`, `ErrNr=193`) and messages. While the BigQuery stored procedures log generic error messages, a more precise mapping or replication of these specific error codes and their associated messages might be required for consistent operational behavior and downstream error handling systems.
*   **Idempotency and Performance of Restart Logic**: The current "delete then insert" pattern for restart functionality might not be the most performant or robust for very large tables. Investigating the use of BigQuery's `MERGE` statement could offer a more efficient and truly idempotent solution, reducing potential for race conditions or performance bottlenecks during restarts. This is a B4 item for potential redesign.
*   **Missing `file_complexity` Details**: The original `file_complexity` details were not available, leading to reliance on inferred complexity. This means there might be hidden complexities or edge cases in the original script that were not fully captured during the migration design. Further manual review of the original script is recommended.
*   **Parameter Validation Completeness**: The current BigQuery wrapper only validates `p_wiederanlaufWert` for negativity. If the original script had more extensive parameter validation (e.g., `p_stichtag` format validation, range checks), these should be added to the BigQuery wrapper.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, perform the following steps:

1.  **Unit Testing of Stored Procedures**:
    *   **`k_ausd_bp_ta_rn_vertrag`**:
        *   Call the procedure directly with various `p_stichtag` and `p_wiederanlaufWert` values.
        *   Test with `p_wiederanlaufWert = 0` (full run).
        *   Test with `p_wiederanlaufWert > 0` (restart scenario).
        *   Test with `p_stichtag` values that should result in no inserts, some inserts, and all inserts based on `gueltig_von`, `gueltig_bis`, and `ladedatum` conditions.
        *   **Passing Criteria**: The `project.dataset.fos_vertrag` table should contain the expected data, accurately reflecting the source `project.dataset.ta_vertrag_cache` based on the provided parameters and date logic.
    *   **`ausd_bp_ta_rn_vertrag_wrapper`**:
        *   Call the procedure with no parameters (should use defaults).
        *   Call with valid `p_stichtag` and `p_wiederanlaufWert`.
        *   Call with `p_wiederanlaufWert < 0` (should trigger the validation error).
        *   **Passing Criteria**:
            *   Successful calls should result in `status = 'OK'` entries in `job_audit` and the `k_ausd_bp_ta_rn_vertrag` procedure being called successfully.
            *   Invalid parameter calls should result in `status = 'ERROR'` entries in `job_audit` with the correct `error_nr` and `message`, and the procedure should terminate with a `SIGNAL SQLSTATE '45000'` error.
            *   The `job_audit` table should accurately record `STARTED` and `OK`/`ERROR` entries for each execution, including all relevant parameters.

2.  **Integration Testing (End-to-End)**:
    *   If an orchestration layer (e.g., Cloud Composer) is used, trigger the job through the orchestrator.
    *   **Passing Criteria**:
        *   The orchestrator should successfully trigger the BigQuery stored procedure.
        *   The BigQuery job should complete successfully, updating `project.dataset.fos_vertrag` as expected.
        *   All logging in `project.dataset.job_audit` should be correct and complete.
        *   Compare the final state of `project.dataset.fos_vertrag` with the expected output from the legacy system for a given `Stichtag` and `Wiederanlaufwert`. This is the most critical validation step.

3.  **Performance Testing**:
    *   Run the job with production-like data volumes to ensure it meets performance SLAs.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Stop New Executions**: Immediately halt any scheduled or manual executions of the `project.dataset.ausd_bp_ta_rn_vertrag_wrapper` BigQuery stored procedure from the orchestration layer (e.g., pause the Airflow DAG, disable the Cloud Workflow).
2.  **Revert to Legacy Script**: Re-enable and restart the original KornShell script `r_ausd_bp_ta_rn_vertrag.ksh` in the legacy environment. Ensure it is configured to run with the correct parameters and schedule.
3.  **Data Restoration (if necessary)**:
    *   If the `project.dataset.fos_vertrag` table was corrupted or incorrectly modified by the migrated job, restore it to its state prior to the migration. This can be done using BigQuery's time travel feature (`FOR SYSTEM_TIME AS OF`) or by restoring from a backup if available.
    *   `TRUNCATE TABLE project.dataset.fos_vertrag;` followed by a re-run of the legacy job or a data load from a known good state might be necessary.
4.  **Delete BigQuery Resources**:
    *   Drop the BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_rn_vertrag_wrapper`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_rn_vertrag`;
        ```
    *   (Optional) If the `job_audit` table is not used by other processes, it can also be dropped:
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_audit`;
        ```
    *   (Optional) If the `project.dataset` was created solely for this migration and is not used by other resources, it can be deleted.
5.  **Post-Rollback Verification**:
    *   Confirm that the legacy job is running successfully and producing the correct output.
    *   Verify that no BigQuery resources related to the migration are inadvertently running or consuming resources.

This rollback procedure ensures a quick return to the stable legacy state while allowing for investigation and remediation of the migration issues.