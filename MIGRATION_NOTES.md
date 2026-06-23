# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_cntrct_valid.ksh`. This script, acting as an orchestration wrapper for a contract data reconciliation process, was migrated from its original KornShell environment to Google BigQuery. The core orchestration logic, parameter handling, logging, and error management have been re-engineered into BigQuery stored procedures and associated audit tables, with overall scheduling managed by Google Cloud Composer.

## 2. Generated Artifacts

The migration of `r_ausd_v_ta_cntrct_valid.ksh` has resulted in the following generated artifacts:

*   **`project.dataset.sp_r_ausd_v_ta_cntrct_valid` (BigQuery Stored Procedure):** This is the primary artifact, replicating the original KornShell script's orchestration logic, including environment setup, parameter parsing, and the invocation of the core processing logic. It accepts parameters mirroring the original command-line arguments.
*   **`project.dataset.sp_dwmsg_meldefehler` (BigQuery Stored Procedure):** Replicates the `DWMSG_MeldeFehler` function for standardized error reporting.
*   **`project.dataset.sp_dwmsg_ermittlenr` (BigQuery Stored Procedure):** Replicates the `DWMSG_ErmittleNr` function for generating unique job entry numbers.
*   **`project.dataset.sp_dwmsg_logdateiname` (BigQuery Stored Procedure):** Replicates the `DWMSG_Logdateiname` function for constructing log file names (or equivalent log identifiers in BigQuery).
*   **`project.dataset.sp_dwmsg_erzeugeeintrag` (BigQuery Stored Procedure):** Replicates the `DWMSG_ErzeugeEintrag` function for logging job start and initial status.
*   **`project.dataset.sp_dwmsg_setzestichtaginfo` (BigQuery Stored Procedure):** Replicates the `DWMSG_SetzeStichtagInfo` function for recording specific date information.
*   **`project.dataset.sp_dwmsg_fehlerbehandlung` (BigQuery Stored Procedure):** Replicates the `DWMSG_Fehlerbehandlung` function for centralized error handling.
*   **`project.dataset.sp_dwmsg_setzestatusok` (BigQuery Stored Procedure):** Replicates the `DWMSG_SetzeStatusOK` function for marking job completion as successful.
*   **`project.dataset.audit_log_table` (BigQuery Table):** A new BigQuery table designed to store detailed job execution logs, status updates, and error messages, replacing the file-based logging of the original script.
*   **`project.dataset.config_table` (BigQuery Table, optional):** A configuration table to manage environment-specific parameters or variables previously sourced from `.dw_init`.
*   **`r_ausd_v_ta_cntrct_valid_dag.py` (Cloud Composer DAG):** A Python-based Apache Airflow DAG responsible for scheduling and orchestrating the execution of the `sp_r_ausd_v_ta_cntrct_valid` BigQuery stored procedure.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration:** The decision to translate the KornShell orchestration logic into a BigQuery stored procedure (`sp_r_ausd_v_ta_cntrct_valid`) was made to leverage BigQuery's native capabilities for control flow, parameter handling, and direct invocation of other BigQuery components. This provides a serverless, scalable, and integrated solution within the target data platform.
*   **BigQuery Stored Procedures and Audit Tables for Logging/Error Handling:** The custom `DWMSG_*` framework was replaced by dedicated BigQuery stored procedures interacting with a centralized `audit_log_table`. This standardizes logging, enables easier querying and monitoring of job statuses, and integrates error handling using BigQuery's `BEGIN ... EXCEPTION ... END` blocks, which functionally replaces shell `trap` commands.
*   **Cloud Composer for Scheduling:** Google Cloud Composer (Apache Airflow) was chosen for scheduling to provide robust workflow orchestration, dependency management, monitoring, and alerting capabilities, which are superior to traditional cron-based scheduling.
*   **Parameter Management:** Command-line parameters are now passed as explicit arguments to the BigQuery stored procedure, and environment variables are managed either through stored procedure parameters, BigQuery session variables, or dedicated configuration tables. This ensures explicit and auditable configuration.
*   **Trade-offs:**
    *   **Shell Trap Equivalence:** Direct replication of shell `INT` and `ERR` traps is not possible. The `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks in BigQuery provide equivalent error handling but require careful mapping of original error conditions and recovery logic.
    *   **Custom Framework Porting:** The `DWMSG_*` functions required bespoke BigQuery stored procedure implementations, which involved a manual translation effort rather than an automated one.
    *   **Core Script Dependency:** The migration of the wrapper script is dependent on the separate, and potentially complex, migration of the core processing script (`k_ausd_v_ta_cntrct_valid.ksh`). This introduces a critical dependency and makes the overall migration "semi-automatic" (B2).

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **IAM Permissions:**
    *   Grant the service account used by Cloud Composer (or the user executing the stored procedure) the necessary BigQuery Data Editor (`roles/bigquery.dataEditor`) or BigQuery User (`roles/bigquery.user`) roles on the target dataset to create/execute stored procedures and write to audit tables.
    *   Ensure the service account has permissions to create and manage BigQuery jobs.
3.  **Configuration Table Population (if applicable):** If a `config_table` is used for environment variables, populate it with the necessary key-value pairs.
4.  **Core Script Migration Completion:** The core processing script `k_ausd_v_ta_cntrct_valid.ksh` *must* be migrated to its BigQuery equivalent (e.g., `sp_k_ausd_v_ta_cntrct_valid`) and deployed. The `sp_r_ausd_v_ta_cntrct_valid` stored procedure will call this component.
5.  **Cloud Composer DAG Deployment:**
    *   Upload the `r_ausd_v_ta_cntrct_valid_dag.py` file to the Cloud Composer environment's DAGs folder.
    *   Verify the DAG appears in the Airflow UI and is unpaused.
6.  **Secrets Management (if applicable):** If any sensitive parameters were identified during the analysis (not explicitly mentioned for this orchestrator but good practice), ensure they are securely stored in Google Secret Manager and accessed appropriately by the Cloud Composer DAG or BigQuery stored procedures.

## 5. Known Gaps & Unresolved References

*   **Core Script (`k_ausd_v_ta_cntrct_valid.ksh`) Migration:** This remains the primary unresolved item. The complexity and specific implementation details of this core script are unknown, and its migration will be a separate, significant effort. The current wrapper migration assumes its successful future migration into a callable BigQuery component.
*   **Exact `trap` Behavior:** While `BEGIN ... EXCEPTION` blocks provide robust error handling, the nuanced behavior of shell `trap` commands for signals like `INT` (e.g., user interruption) might not be perfectly replicated. This should be tested thoroughly.
*   **Environment Variable Mapping:** The `.dw_init` script's full scope of environment variable settings needs to be thoroughly reviewed to ensure all critical variables are correctly mapped to BigQuery parameters or configuration table entries.
*   **`DWMSG_*` Function Parity:** While the `DWMSG_*` functions have been translated, their exact logging format and any implicit side effects (e.g., specific file system interactions beyond logging) need to be verified for full functional parity.

## 6. Validation

Validation of the migrated job involves verifying both the individual components and the end-to-end workflow.

**How to Run Tests:**

1.  **Individual Stored Procedure Execution:**
    *   Execute `sp_r_ausd_v_ta_cntrct_valid` directly in BigQuery, providing test parameters (e.g., `CALL project.dataset.sp_r_ausd_v_ta_cntrct_valid('TEST_JOB', 123);`).
    *   Execute the individual `sp_dwmsg_*` procedures to ensure they write correctly to the `audit_log_table`.
2.  **Cloud Composer DAG Trigger:**
    *   Manually trigger the `r_ausd_v_ta_cntrct_valid_dag.py` from the Airflow UI.
    *   Trigger the DAG with various parameter combinations, including valid and invalid ones, to test error handling.

**What "Passing" Means:**

*   **Successful Execution:** The `sp_r_ausd_v_ta_cntrct_valid` stored procedure completes without BigQuery errors.
*   **Correct Logging:** Entries are correctly written to the `project.dataset.audit_log_table` at each expected stage (job start, status updates, completion).
*   **Status Updates:** The `DWMSG_SetzeStatusOK` (or equivalent) procedure correctly updates the job status to 'OK' upon successful completion.
*   **Core Script Invocation:** The stored procedure successfully attempts to invoke the (migrated) core script (`sp_k_ausd_v_ta_cntrct_valid`). The return status of this invocation is correctly captured and handled.
*   **Error Handling:**
    *   When invalid parameters are passed, the stored procedure correctly logs an error and exits gracefully (or marks the job as failed).
    *   If the invoked core script (or a simulated error within it) raises an exception, the `BEGIN ... EXCEPTION` block in `sp_r_ausd_v_ta_cntrct_valid` correctly catches it, logs the error via `sp_dwmsg_meldefehler`, and marks the job as failed.
*   **Cloud Composer DAG Status:** The Cloud Composer DAG run completes successfully (green status in Airflow UI) for successful scenarios, and correctly reports failures (red status) for error scenarios.
*   **Resource Utilization:** BigQuery job statistics show reasonable resource consumption (slots, bytes processed).

## 7. Rollback Procedure

In case of issues after go-live, the following rollback procedure can be executed:

1.  **Disable New Scheduling:**
    *   In Cloud Composer, pause or delete the `r_ausd_v_ta_cntrct_valid_dag.py` to prevent further execution of the migrated job.
    *   Re-enable the original scheduling mechanism (e.g., cron job) for `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh`.
2.  **Disable BigQuery Stored Procedures:**
    *   To prevent accidental execution, consider revoking `EXECUTE` permissions from the service account on `project.dataset.sp_r_ausd_v_ta_cntrct_valid` and related `sp_dwmsg_*` procedures. Alternatively, rename the stored procedures to a temporary name (e.g., `_OLD_sp_r_ausd_v_ta_cntrct_valid`).
3.  **Data Impact:**
    *   Since this script is an orchestrator and does not directly modify business data, its rollback primarily involves stopping its execution.
    *   If the *invoked* core script (`k_ausd_v_ta_cntrct_valid.ksh`) performs data writes, its rollback procedure would need to be executed separately, potentially involving data restoration from backups or reverse operations, depending on its specific logic.
4.  **Monitoring:** Monitor the original system to ensure it resumes normal operation and the migrated system for any lingering activity.