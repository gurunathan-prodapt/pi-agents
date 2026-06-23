# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh` has been migrated to Google BigQuery.

This script's original purpose was to orchestrate the preparation and provision of selected basic products (e.g., FAX, Data24) for BERT. It achieved this by creating a cutoff-date extraction of contract cache data from the Data Warehouse (DWH) and delivering this data to the Forderungsscoring (FOS) system. The script handled parameter parsing, date determination, logging, and delegated core business logic to an external kernel script (`k_ausd_bp_ta_bpr_basis.ksh`).

The functionality of this KornShell wrapper script has been re-implemented as a BigQuery Stored Procedure named `project.dataset.Bereitstellung_Basisprodukte_BERT`. Logging and error handling, previously file-based and using custom shell functions, are now managed through dedicated BigQuery audit and error log tables. Orchestration of this BigQuery Stored Procedure will be handled by a modern cloud-native orchestrator (e.g., Cloud Composer), replacing the existing shell script's execution context. The core business logic of the kernel script (`k_ausd_bp_ta_bpr_basis.ksh`) is identified as a separate, dependent migration task, with a placeholder BigQuery Stored Procedure created for it.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`ddl/job_audit_log.sql`**
    *   **Role**: BigQuery DDL (Data Definition Language) script to create the `job_audit_log` table. This table centralizes job metadata, start/end times, and overall status for each execution, replacing the shell script's `DWMSG_` functions for high-level logging.
*   **`ddl/job_error_log.sql`**
    *   **Role**: BigQuery DDL script to create the `job_error_log` table. This table records detailed error information, including error numbers and arguments, replacing the shell script's error logging to files.
*   **`ddl/job_run_log.sql`**
    *   **Role**: BigQuery DDL script to create the `job_run_log` table. This table stores detailed information for each job run, including a unique job ID, log file name (for reference), and status updates throughout execution.
*   **`stored_procedures/k_ausd_bp_ta_bpr_basis.sql`**
    *   **Role**: A skeletal BigQuery Stored Procedure named `project.dataset.k_ausd_bp_ta_bpr_basis`. This serves as a placeholder for the core business logic originally contained in `k_ausd_bp_ta_bpr_basis.ksh`. Its full implementation is a separate, significant migration effort.
*   **`stored_procedures/Bereitstellung_Basisprodukte_BERT.sql`**
    *   **Role**: The primary BigQuery Stored Procedure, `project.dataset.Bereitstellung_Basisprodukte_BERT`, which replaces the original `r_ausd_bp_ta_bpr_basis.ksh` wrapper script. It handles parameter validation, defaulting logic, logging to the audit tables, and invokes the `k_ausd_bp_ta_bpr_basis` stored procedure.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Wrapper Logic**: The KornShell wrapper script's functionality (parameter handling, logging, orchestration) was re-implemented as a BigQuery Stored Procedure. This leverages BigQuery's native capabilities for procedural logic and direct interaction with BigQuery tables, aligning with a cloud-native data platform strategy.
*   **Dedicated BigQuery Audit Tables for Logging**: File-based logging and custom `DWMSG_` functions were replaced by structured, centralized logging within BigQuery using `job_audit_log`, `job_error_log`, and `job_run_log` tables. This significantly improves observability, simplifies error analysis, and integrates seamlessly with BigQuery's ecosystem.
*   **Cloud-Native Orchestration**: The existing shell script's execution context is replaced by a cloud-native orchestrator (e.g., Cloud Composer/Airflow). This provides robust scheduling, monitoring, dependency management, and error handling capabilities inherent to cloud platforms, moving away from custom shell-based scheduling.
*   **Separation of Wrapper and Kernel Logic**: The migration explicitly separates the wrapper's role (parameter handling, logging, calling core logic) into `Bereitstellung_Basisprodukte_BERT` from the core business logic of `k_ausd_bp_ta_bpr_basis.ksh`, which is represented by a separate placeholder procedure. This modular approach simplifies the migration of each component and allows for focused development.
*   **Parameter Defaulting and Validation within Stored Procedure**: Logic for defaulting `p_stichtag` and `p_wiederanlaufWert` and performing parameter validation is directly embedded within the BigQuery Stored Procedure. This ensures data integrity and consistent parameter handling at the point of execution, replacing shell-based `getopts` and validation.
*   **BigQuery `EXCEPTION` Handling**: Shell `trap` commands for signal handling are replaced by BigQuery's native `EXCEPTION WHEN ERROR` blocks and `SIGNAL SQLSTATE` statements. This provides robust error management within the BigQuery environment and allows orchestrators to react to specific BigQuery errors.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (e.g., `project.dataset`) exists. If not, create it.
2.  **IAM Permissions Configuration**:
    *   Grant the necessary BigQuery Data Editor or BigQuery Admin roles to the service account that will be used to deploy the DDLs and stored procedures.
    *   Grant BigQuery Data Editor/User roles to the service account that will execute the `Bereitstellung_Basisprodukte_BERT` stored procedure (e.g., the Cloud Composer service account). This includes permissions to `CREATE TABLE`, `CREATE PROCEDURE`, `INSERT`, `UPDATE`, `SELECT` on the relevant tables and dataset.
3.  **Deploy DDLs**:
    *   Execute the DDL scripts to create the logging tables:
        *   `ddl/job_audit_log.sql`
        *   `ddl/job_error_log.sql`
        *   `ddl/job_run_log.sql`
4.  **Deploy Stored Procedures**:
    *   Execute the stored procedure scripts:
        *   `stored_procedures/k_ausd_bp_ta_bpr_basis.sql` (Note: This is a placeholder and needs full implementation as a separate task).
        *   `stored_procedures/Bereitstellung_Basisprodukte_BERT.sql`
5.  **Orchestrator Setup (e.g., Cloud Composer/Airflow)**:
    *   Configure the Cloud Composer environment (if not already set up).
    *   Develop and deploy the Cloud Composer DAG (or Cloud Workflow) that will schedule and execute the `project.dataset.Bereitstellung_Basisprodukte_BERT` BigQuery Stored Procedure.
    *   Configure any required connection strings or service account impersonation for the orchestrator to interact with BigQuery.
    *   Define the scheduling parameters (e.g., cron schedule) for the orchestrator.
6.  **Data Migration (for Kernel Script)**:
    *   Ensure that all source DWH tables and data referenced by the *kernel script* (`k_ausd_bp_ta_bpr_basis.ksh`) are migrated to BigQuery and are accessible to the `k_ausd_bp_ta_bpr_basis` stored procedure. This is crucial for the full end-to-end functionality once the kernel is migrated.

## 5. Known Gaps & Unresolved References

*   **Core Kernel Script Migration (Major Gap)**: The most significant unresolved item is the actual business logic contained within `k_ausd_bp_ta_bpr_basis.ksh`. The generated `stored_procedures/k_ausd_bp_ta_bpr_basis.sql` is a placeholder. Its full migration, involving data extraction, transformation, and loading (ETL) logic into BigQuery SQL, is a separate and substantial effort.
*   **FOS Data Delivery Mechanism**: The original script "delivers data to FOS". The specific mechanism for this data delivery in the BigQuery target environment needs to be defined and implemented. This could involve BigQuery exports to Cloud Storage, direct data sharing, or a push mechanism via Cloud Functions/Pub/Sub, depending on the FOS system's capabilities and integration requirements.
*   **Helper Script Functionality**: While the design states that functionalities from sourced helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be absorbed, specific complex or reusable utility functions might warrant dedicated BigQuery User-Defined Functions (UDFs) or separate helper procedures for better modularity and reusability.
*   **`FOSHoleLadedatum` Function**: The commented-out `FOSHoleLadedatum` function in the original script suggests a potential external function to retrieve the maximum load date. If this logic was ever active or is required, its BigQuery equivalent (e.g., querying `MAX(ladedatum)` from the relevant DWH table) needs to be implemented, likely within the kernel script's migration.
*   **Environment Variables/Configuration**: The original KornShell script relied on shell environment variables (e.g., `BERT_DIR_ROOT`). These need to be fully replaced by BigQuery project/dataset settings, orchestrator variables, or hardcoded values within the stored procedures where appropriate.

## 6. Validation

To validate the migration of the `r_ausd_bp_ta_bpr_basis.ksh` wrapper script, follow these steps:

1.  **Deployment Verification**:
    *   Confirm that all DDLs and stored procedures have been successfully deployed to the target BigQuery dataset.
    *   Verify the existence of the `job_audit_log`, `job_error_log`, and `job_run_log` tables in BigQuery.

2.  **Functional Validation (Wrapper Logic)**:
    *   **Successful Execution**:
        *   Execute the main stored procedure with valid parameters:
            ```sql
            CALL `project.dataset.Bereitstellung_Basisprodukte_BERT`('01012023', 0);
            ```
        *   **Passing Criteria**:
            *   The call completes without any BigQuery errors.
            *   Query `project.dataset.job_audit_log`: Expect an entry with `status = 'STARTED'` and a subsequent entry with `status = 'SUCCESS'` (or 'OK') for the `JobKennung = 'ausd_bp_ta_bpr_basis'`.
            *   Query `project.dataset.job_run_log`: Expect an entry with `status = 'RUNNING'` and a subsequent update to `status = 'OK'` for the corresponding `job_id`.
            *   Query `project.dataset.job_error_log`: Expect no new entries.
            *   (Optional) If the `k_ausd_bp_ta_bpr_basis` placeholder procedure is modified to log its invocation, verify that it was called.
    *   **Parameter Defaulting**:
        *   Execute the main stored procedure without providing input parameters:
            ```sql
            CALL `project.dataset.Bereitstellung_Basisprodukte_BERT`(NULL, NULL);
            ```
        *   **Passing Criteria**:
            *   The call completes successfully.
            *   Query `project.dataset.job_audit_log` and `project.dataset.job_run_log`: Verify that `stichtag` defaults to the current system date (`FORMAT_DATE('%d%m%Y', CURRENT_DATE())`) and `wiederanlaufwert` defaults to `0`.
    *   **Parameter Validation Error**:
        *   Execute the main stored procedure with an invalid `p_stichtag` (e.g., empty string):
            ```sql
            CALL `project.dataset.Bereitstellung_Basisprodukte_BERT`('', 0);
            ```
        *   **Passing Criteria**:
            *   The procedure should terminate with a `SQLSTATE '45000'` error message indicating "Parameter validation failed".
            *   Query `project.dataset.job_error_log`: Expect an entry with `error_nr = 193` and `error_arg = 'Stichtag'`.
            *   Query `project.dataset.job_run_log` and `project.dataset.job_audit_log`: Expect the corresponding run to have a `status = 'FAILED'`.

3.  **Orchestration Validation (e.g., Cloud Composer)**:
    *   Deploy the Cloud Composer DAG (or Cloud Workflow) that triggers `project.dataset.Bereitstellung_Basisprodukte_BERT`.
    *   Manually trigger the DAG/workflow and monitor its execution in the Airflow UI or Cloud Workflows console.
    *   **Passing Criteria**: The DAG/workflow run should succeed, indicating that the BigQuery Stored Procedure was invoked correctly and completed without orchestrator-level errors. Review the orchestrator logs for confirmation of BigQuery job execution.

4.  **End-to-End Validation (Post-Kernel Migration)**:
    *   Once the `k_ausd_bp_ta_bpr_basis` stored procedure is fully migrated and implemented, comprehensive end-to-end tests will be required to verify the correctness of data extraction, transformation, and the final delivery to FOS.

## 7. Rollback Procedure

In case of issues or a need to revert the migration, follow these steps:

1.  **Orchestrator Rollback**:
    *   **Deactivate New Orchestration**: Deactivate or delete the new Cloud Composer DAG or Cloud Workflow that triggers `project.dataset.Bereitstellung_Basisprodukte_BERT`.
    *   **Re-enable Original Scheduling**: Re-enable the original scheduling mechanism (e.g., cron job, scheduler) for the legacy `r_ausd_bp_ta_bpr_basis.ksh` script.

2.  **BigQuery Stored Procedure Rollback**:
    *   **Drop New Procedures**: Execute the following DDL commands in BigQuery to remove the migrated stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.Bereitstellung_Basisprodukte_BERT`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_bpr_basis`;
        ```
    *   **Revert any other BigQuery object changes**: If any other BigQuery objects (e.g., views, UDFs) were created or modified as part of this migration, revert them to their previous state using appropriate `DROP` or `ALTER` statements.

3.  **BigQuery Logging Table Rollback (Optional)**:
    *   The logging tables (`job_audit_log`, `job_error_log`, `job_run_log`) are additive and generally do not interfere with other processes. They can be retained for historical audit.
    *   If their removal is strictly necessary, execute the following DDL commands:
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_audit_log`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        DROP TABLE IF EXISTS `project.dataset.job_run_log`;
        ```
    *   **Data Impact Consideration**: For this wrapper script, the direct data impact is minimal. However, if the kernel script (once migrated) had written or altered data, a more complex data rollback strategy might be required, potentially involving data restoration from backups or point-in-time recovery.

4.  **Source System Re-activation**:
    *   Ensure that the original `r_ausd_bp_ta_bpr_basis.ksh` script and all its dependencies (sourced helper scripts, kernel script, DWH connectivity) are fully operational and can be re-activated without issues.