# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh`. This script, originally an orchestration wrapper for contract data synchronization into the `ta_inv_def` table, has been re-platformed from a KornShell environment to Google BigQuery. The core orchestration logic is now implemented as a BigQuery Stored Procedure, leveraging BigQuery tables for centralized logging and error handling. The actual data transformation logic, originally in `k_ausd_v_ta_inv_def.ksh`, is a placeholder for a separate, future migration into another BigQuery Stored Procedure.

## 2. Generated artifacts

The migration has produced the following BigQuery SQL artifacts:

*   **`project/dataset/job_audit.sql`**
    *   **Role**: This script defines and creates the `job_audit` BigQuery table. This table serves as the central repository for logging the execution status, start/end times, and metadata of all migrated BigQuery jobs. It replaces the custom file-based logging and `DWMSG_*` functions from the original KornShell environment.
*   **`project/dataset/job_error_log.sql`**
    *   **Role**: This script defines and creates the `job_error_log` BigQuery table. This table is dedicated to capturing detailed error messages, stack traces, and other diagnostic information whenever a BigQuery job fails. It replaces the error handling and messaging (`f_alis_msgerr.ksh`) of the original KornShell script.
*   **`project/dataset/sp_vertragsdatenabgleich.sql`**
    *   **Role**: This script defines and creates the `sp_vertragsdatenabgleich` BigQuery Stored Procedure. This procedure is the direct migration of the `r_ausd_v_ta_inv_def.ksh` KornShell wrapper. It handles parameter parsing, environment setup (via `DECLARE` statements), logging to `job_audit` and `job_error_log`, and orchestrates the call to the core data synchronization logic (represented by the placeholder `sp_k_ausd_v_ta_inv_def`).

**Note on `sp_k_ausd_v_ta_inv_def`**: While referenced in `sp_vertragsdatenabgleich`, the actual SQL for `project.dataset.sp_k_ausd_v_ta_inv_def` (which would contain the core data synchronization logic from `k_ausd_v_ta_inv_def.ksh`) is *not* part of these generated artifacts and must be migrated separately.

## 3. Key design decisions

*   **Re-platforming Orchestration to BigQuery Stored Procedures**: The KornShell wrapper (`r_ausd_v_ta_inv_def.ksh`) was migrated directly into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This decision centralizes the ETL workflow within BigQuery, leveraging its native capabilities for execution, error handling, and logging, thereby reducing reliance on external shell environments and improving maintainability within the GCP ecosystem.
*   **Centralized BigQuery Logging**: The custom shell-based logging (`DWMSG_*` functions, `tee -a $LogDatei`) and error messaging (`f_alis_msgerr.ksh`) were replaced by dedicated BigQuery tables (`job_audit` and `job_error_log`). This provides a structured, queryable, and centralized logging solution, making it easier to monitor job executions, analyze failures, and integrate with GCP's operational tools.
*   **Native BigQuery Error Handling**: The shell script's `trap` mechanism for error handling was replaced by BigQuery's `EXCEPTION WHEN ERROR THEN` blocks. This provides robust, structured error capture directly within the SQL procedure, allowing for detailed error logging and graceful failure management.
*   **Stored Procedure Parameterization**: Command-line parameter parsing (`getopts`) from the original script was translated into explicit input parameters for the BigQuery Stored Procedure. This aligns with BigQuery's standard procedure interface and improves clarity and type safety.
*   **Modular Core Logic Invocation**: The invocation of the core script (`k_ausd_v_ta_inv_def.ksh`) was designed as a `CALL` to a separate BigQuery Stored Procedure (`sp_k_ausd_v_ta_inv_def`). This maintains modularity, allowing the complex core business logic to be migrated and developed independently while `sp_vertragsdatenabgleich` focuses solely on orchestration.
*   **BigQuery Native Functions for Utilities**: Shell utilities for date handling (`h_alis_date.ksh`) and environment setup (`. $HOME/.dw_init`) were replaced by BigQuery's built-in SQL functions (e.g., `CURRENT_TIMESTAMP()`, `CURRENT_DATE()`) and `DECLARE` statements or configuration tables. This simplifies the code, removes external script dependencies, and leverages optimized BigQuery functionalities.

**Notable Trade-offs:**
*   **Dependency on Core Logic Migration**: The `sp_vertragsdatenabgleich` procedure is dependent on the successful and complete migration of `k_ausd_v_ta_inv_def.ksh` into `sp_k_ausd_v_ta_inv_def`. Until this core logic is migrated, the wrapper procedure cannot perform its full intended function.
*   **Loss of Direct File-based Logs**: The original `tee -a $LogDatei` created physical log files. While BigQuery tables offer superior queryability, any external systems or processes that directly consumed these specific log files will require an alternative mechanism (e.g., exporting `job_audit` data to Cloud Storage).
*   **BigQuery Specificity**: The migrated solution is highly optimized for and dependent on Google BigQuery, reducing its portability to other data warehousing platforms.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`
        OPTIONS(
            location="YOUR_GCP_REGION",
            description="Dataset for migrated ISBERT ETL jobs."
        );
        ```
2.  **Deploy Audit Tables**:
    *   Execute the `project/dataset/job_audit.sql` script to create the `job_audit` table.
    *   Execute the `project/dataset/job_error_log.sql` script to create the `job_error_log` table.
3.  **Deploy Orchestration Stored Procedure**:
    *   Execute the `project/dataset/sp_vertragsdatenabgleich.sql` script to create the `sp_vertragsdatenabgleich` stored procedure.
4.  **Migrate and Deploy Core Logic (`sp_k_ausd_v_ta_inv_def`)**:
    *   **Crucially**, the core business logic from `k_ausd_v_ta_inv_def.ksh` must be fully analyzed, designed, and migrated into a BigQuery Stored Procedure named `project.dataset.sp_k_ausd_v_ta_inv_def`. This procedure must be deployed *before* `sp_vertragsdatenabgleich` can be successfully executed.
5.  **IAM/Permissions Configuration**:
    *   The service account or user identity that will execute `sp_vertragsdatenabgleich` (e.g., via Cloud Composer, Cloud Scheduler, or direct invocation) must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` (to insert/update `job_audit` and `job_error_log`).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   `BigQuery Data Viewer` on any source tables read by `sp_k_ausd_v_ta_inv_def`.
        *   `BigQuery Data Editor` on any target tables written to by `sp_k_ausd_v_ta_inv_def` (e.g., `ta_inv_def`).
        *   `BigQuery Routine Caller` on `project.dataset.sp_vertragsdatenabgleich` and `project.dataset.sp_k_ausd_v_ta_inv_def`.
6.  **Scheduling**:
    *   Set up a scheduling mechanism (e.g., Cloud Composer DAG, Cloud Scheduler job, Dataform pipeline) to invoke `project.dataset.sp_vertragsdatenabgleich` at the required frequency and with the necessary parameters.
7.  **Configuration Management (if applicable for core logic)**:
    *   If `sp_k_ausd_v_ta_inv_def` requires external connection strings, secrets, or complex configurations (e.g., for external APIs or non-BigQuery data sources), these must be securely managed (e.g., using Secret Manager) and made accessible to the execution environment.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up and represent known gaps or areas requiring further design/implementation:

*   **Core Business Logic Migration (`k_ausd_v_ta_inv_def.ksh`)**: This is the most significant unresolved item (B4 item). The actual data synchronization logic for `ta_inv_def` is contained within `k_ausd_v_ta_inv_def.ksh`. This script needs a dedicated analysis, design, and migration effort into `project.dataset.sp_k_ausd_v_ta_inv_def` (or equivalent BigQuery SQL/Python components). Until this is complete, `sp_vertragsdatenabgleich` will call a non-existent or incomplete procedure.
*   **Full Analysis of Sourced Scripts**: The original script sourced `. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`. While general replacements have been designed, a thorough review of the *exact content* of these scripts is needed to ensure no critical logic, environment variables, or configurations have been missed during the migration to BigQuery `DECLARE` statements or configuration tables.
*   **Specifics of `-s` and `-l` Parameters**: The original script accepted `-s` and `-l` parameters. While placeholders `p_source_param` and `p_log_param` exist in `sp_vertragsdatenabgleich`, their precise meaning and how they should influence the core data synchronization logic in `sp_k_ausd_v_ta_inv_def` need to be fully understood and implemented.
*   **External Log Consumers**: If any downstream systems or monitoring tools relied on the specific format or location of the log files generated by `tee -a $LogDatei` in the original environment, a new mechanism to export or stream data from `job_audit` and `job_error_log` (e.g., to Cloud Storage, Pub/Sub, or a SIEM) might be required.
*   **Error Code/Argument Mapping**: The `job_error_log` table includes `error_code` and `error_argument` fields. If the original `f_alis_msgerr.ksh` provided specific numeric codes or arguments, these should be mapped and populated in the BigQuery error log for consistency.

## 6. Validation

Validation of the migrated solution involves unit testing the individual components and integration testing the end-to-end workflow.

**How to run tests:**

1.  **Unit Tests (BigQuery SQL)**:
    *   **Table Creation**: Execute the `CREATE TABLE` statements for `job_audit` and `job_error_log` in a test BigQuery project/dataset. Verify tables are created with correct schema.
    *   **Stored Procedure `sp_vertragsdatenabgleich`**:
        *   **Help Message**: Call `CALL project.dataset.sp_vertragsdatenabgleich(p_help => TRUE);` and verify the output matches the expected help text.
        *   **Successful Execution (Mock Core Logic)**: Create a temporary mock `project.dataset.sp_k_ausd_v_ta_inv_def` that simply returns successfully (e.g., `CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_inv_def(job_key STRING, entry_number STRING, p_source_param STRING, p_log_param STRING) BEGIN SELECT 'Core logic executed successfully (mock)'; END;`). Then, call `CALL project.dataset.sp_vertragsdatenabgleich();`.
        *   **Error Handling (Mock Core Logic)**: Modify the mock `sp_k_ausd_v_ta_inv_def` to raise an error (e.g., `RAISE 'Mock error from core logic';`). Then, call `CALL project.dataset.sp_vertragsdatenabgleich();`.
        *   **Parameter Passing**: Call `sp_vertragsdatenabgleich` with various `p_source_param` and `p_log_param` values and verify they are correctly logged in the `parameters` JSON column of `job_audit`.
2.  **Integration Tests (End-to-End)**:
    *   Once `project.dataset.sp_k_ausd_v_ta_inv_def` (the migrated core logic) is fully implemented and deployed, execute `project.dataset.sp_vertragsdatenabgleich` with realistic production-like parameters.
    *   Compare the state of the target `ta_inv_def` table (or other affected tables) after the BigQuery job runs with the state after the original `r_ausd_v_ta_inv_def.ksh` script runs against the same source data. This may involve data validation queries or checksums.
    *   If the original script had specific output files or reports, ensure the BigQuery solution can produce equivalent results.

**What "passing" means:**

*   **`sp_vertragsdatenabgleich` Execution**: The stored procedure completes without unhandled BigQuery errors.
*   **Audit Logging**:
    *   For successful runs, the `project.dataset.job_audit` table contains a new entry with `status = 'SUCCESS'`, accurate `start_time`, `end_time`, and `parameters`.
    *   For failed runs, the `project.dataset.job_audit` table contains an entry with `status = 'FAILED'`, and the `project.dataset.job_error_log` table contains a corresponding detailed error entry for the `job_id` with `severity = 'ERROR'`, `error_message`, and `stack_trace`.
*   **Functional Equivalence**: The data synchronization performed by the combined `sp_vertragsdatenabgleich` and `sp_k_ausd_v_ta_inv_def` results in the exact same data state in the target tables (e.g., `ta_inv_def`) as the original `r_ausd_v_ta_inv_def.ksh` and `k_ausd_v_ta_inv_def.ksh` scripts, given identical input data.
*   **Performance**: The migrated solution meets or exceeds the performance requirements of the original script.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Halt**:
    *   Immediately stop any scheduled executions (e.g., Cloud Composer DAGs, Cloud Scheduler jobs) that invoke `project.dataset.sp_vertragsdatenabgleich`.
2.  **Re-enable Original Job**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh` script and its associated scheduling mechanism.
    *   Verify that the original job is running correctly and producing expected results.
3.  **Data Rollback (if necessary)**:
    *   If the migrated `sp_k_ausd_v_ta_inv_def` (core logic) made irreversible or incorrect data modifications to `ta_inv_def` or other tables, execute the pre-defined data rollback strategy for the core logic. This might involve:
        *   Restoring tables from a point-in-time backup.
        *   Using snapshot tables if they were part of the migration design.
        *   Executing specific "undo" SQL scripts.
        *   **Note**: A robust data rollback plan for `sp_k_ausd_v_ta_inv_def` is critical and must be part of its migration design.
4.  **BigQuery Artifact Deletion (Optional)**:
    *   If the BigQuery artifacts are not being used by other processes and are deemed unstable, they can be deleted:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_vertragsdatenabgleich`;
        -- Only drop sp_k_ausd_v_ta_inv_def if it was part of this migration and is also unstable
        DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_v_ta_inv_def`;
        DROP TABLE IF EXISTS `project.dataset.job_audit`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        ```
    *   Consider retaining `job_audit` and `job_error_log` for post-mortem analysis, even if the procedures are dropped.
5.  **Post-Rollback Analysis**:
    *   Analyze the root cause of the rollback using the `job_audit` and `job_error_log` tables, as well as any other available logs or monitoring data.
    *   Address the identified issues before attempting another migration or re-deployment.