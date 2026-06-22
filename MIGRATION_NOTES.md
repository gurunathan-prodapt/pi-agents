# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestrator script `r_ausd_bp_ta_rn_da_vda_tk.ksh`. This script is responsible for orchestrating the initial provision of selected basic product data for the BERT system, including parameter handling, date validation, restart logic, and error handling, before delegating core data processing to an external "kernel script."

The job has been migrated to **Google Cloud BigQuery**. The orchestrator logic is now implemented as a BigQuery Stored Procedure, while logging and auditing are handled by dedicated BigQuery tables. The core data processing logic, originally in the "kernel script," has been provisionally embedded as BigQuery SQL within the main stored procedure, pending a detailed analysis of the original kernel script's content.

## 2. Generated artifacts

The following artifacts have been generated as part of this migration:

*   **`project.dataset.job_audit_ddl.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_audit` BigQuery table. This table is used to store comprehensive audit logs for each execution of the migrated job, including status, parameters, and error messages.
*   **`project.dataset.job_log_ddl.sql`**
    *   **Role:** Defines the DDL for the `job_log` BigQuery table. This table captures detailed logging messages (INFO, ERROR) generated during the execution of the migrated job, replacing the legacy file-based logging.
*   **`sp_bereitstellung_basisprodukte_bert.sql`**
    *   **Role:** This BigQuery Stored Procedure replaces the original `r_ausd_bp_ta_rn_da_vda_tk.ksh` KornShell script. It handles input parameter parsing (`p_stichtag`, `p_wiederanlaufWert`), validation, default value assignment, logging to `job_audit` and `job_log`, flow control, and integrates the core data processing logic (equivalent to the legacy "kernel script") as an `INSERT...SELECT` statement.
*   **`sp_process_contract_cache_data.sql`**
    *   **Role:** This file serves as a placeholder for a BigQuery Stored Procedure that would encapsulate the detailed business logic from the legacy `k_ausd_bp_ta_rn_da_vda_tk.ksh` kernel script. Currently, its content is pending analysis, and the core logic is provisionally embedded in `sp_bereitstellung_basisprodukte_bert.sql`. If the kernel script's logic proves complex, this procedure would be developed and called by `sp_bereitstellung_basisprodukte_bert`.

## 3. Key design decisions

*   **Orchestrator to BigQuery Stored Procedure**: The KornShell orchestrator was migrated to a BigQuery Stored Procedure (`sp_bereitstellung_basisprodukte_bert`). This decision leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, providing a cloud-native, scalable, and maintainable solution that integrates seamlessly with BigQuery data.
*   **Structured Logging to BigQuery Tables**: The legacy file-based logging (`DWMSG_...` functions) was replaced by structured logging to dedicated BigQuery tables (`job_audit`, `job_log`). This improves auditability, enables easier querying and analysis of job execution history, and facilitates integration with Cloud Monitoring for alerts.
*   **Direct Embedding of Kernel Logic (Provisional)**: For the initial migration of the orchestrator, the inferred core data processing logic from `k_ausd_bp_ta_rn_da_vda_tk.ksh` was directly embedded as an `INSERT...SELECT` statement within `sp_bereitstellung_basisprodukte_bert.sql`.
    *   **Trade-off**: This simplifies the initial migration of the orchestrator wrapper but introduces a "Known Gap" as the full complexity of the kernel script is yet to be analyzed. If the kernel logic is extensive, it will be refactored into a separate stored procedure (`sp_process_contract_cache_data.sql`) for better modularity and maintainability.
*   **BigQuery SQL for Parameter Handling and Validation**: Shell-based parameter parsing (`getopts`) and validation were translated into BigQuery SQL procedural statements (`IF...THEN RAISE`). This ensures robust input handling and error reporting directly within the BigQuery environment.
*   **BigQuery `DELETE` and `INSERT...WHERE` for Restart Logic**: The legacy restart logic, involving conditional deletion and filtering based on `Wiederanlaufwert`, was directly translated into BigQuery `DELETE` and `INSERT...WHERE` statements. This maintains functional parity while utilizing BigQuery's efficient data manipulation capabilities.
*   **BigQuery `BEGIN...EXCEPTION` for Error Handling**: The shell's `trap` mechanism for error handling was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides a robust and structured way to capture, log, and manage errors during stored procedure execution.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps and configurations are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery datasets (e.g., `my_project.isbert_data_processing` or `project.dataset` as used in the generated code) are created in your Google Cloud project.
2.  **IAM/Permissions Configuration**:
    *   Grant the service account or user that will execute the BigQuery Stored Procedure the necessary IAM roles:
        *   `BigQuery Data Editor` on the dataset containing `job_audit`, `job_log`, and `target_fos_table`.
        *   `BigQuery Data Viewer` on the dataset containing `source_contract_cache`.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `BigQuery Routine User` or `BigQuery Data Editor` to create/update stored procedures.
3.  **Source Data Ingestion**:
    *   Set up a data ingestion pipeline (e.g., Datastream for Oracle to BigQuery, Dataflow, or periodic batch loads) to populate the `project.dataset.source_contract_cache` table with the contract cache data from the legacy DWH. This table must be available and up-to-date before the job runs.
4.  **Target Table Schema Definition**:
    *   Define the complete schema for `project.dataset.target_fos_table` based on the requirements of the `Forderungsscoring` system and the expected output of the kernel script's transformations. The generated SQL uses placeholder columns; these must be replaced with actual column names and types.
5.  **Scheduling Configuration**:
    *   Configure a Google Cloud orchestration service (e.g., Cloud Composer/Airflow, Cloud Workflows, or Cloud Scheduler) to invoke the `sp_bereitstellung_basisprodukte_bert` stored procedure at the required frequency.
    *   Ensure the scheduler passes the `p_stichtag` and `p_wiederanlaufWert` parameters correctly.
6.  **Secrets Management (if applicable)**:
    *   While not explicitly identified in the provided design, if any sensitive configurations (e.g., API keys for external systems, though none are directly used by this orchestrator) were to be introduced, they should be managed using Google Secret Manager.

## 5. Known gaps & unresolved references

*   **Kernel Script Logic (B4 Redesign Risk)**: The most significant gap is the detailed business logic within `k_ausd_bp_ta_rn_da_vda_tk.ksh`. This script contains the actual data extraction, transformation, and loading (ETL) logic, which is critical for the job's functionality. Without its full content, the migration of this core logic to BigQuery SQL/Stored Procedures cannot be fully designed. The current `sp_bereitstellung_basisprodukte_bert.sql` includes a provisional `INSERT...SELECT` based on inferred logic. A separate, detailed analysis of `k_ausd_bp_ta_rn_da_vda_tk.ksh` is required to complete the migration, which may necessitate a more complex `sp_process_contract_cache_data.sql` and constitutes a **B4 redesign risk**.
*   **`BERT_DIR_ROOT` Resolution**: The exact definition and how the `BERT_DIR_ROOT` environment variable is set in the legacy environment are not explicitly known. This variable is crucial for resolving paths to internal utility scripts. Confirmation is needed to ensure all internal script paths are correctly identified.
*   **Legacy DWH Schema Details**: While the source is identified as a DWH, specific table and column schemas for "contract cache data" and the target `Forderungsscoring` table are not fully available. The generated SQL uses placeholder table and column names. Full DDL and data dictionaries for all relevant DWH tables are required to finalize column mappings and ensure data integrity.
*   **Performance Tuning**: Initial BigQuery SQL for complex transformations may require performance tuning. This is a common post-migration activity and should be planned for.

## 6. Validation

To ensure the successful migration and correct functionality of the BigQuery job, follow these validation steps:

1.  **How to Run Tests**:
    *   **Unit Testing**: Execute the `sp_bereitstellung_basisprodukte_bert` stored procedure directly in BigQuery. Test with various input parameters:
        *   Valid `p_stichtag` (e.g., `20012023`) and `p_wiederanlaufWert` (e.g., `0`, `100`).
        *   `NULL` or empty `p_stichtag` to verify default to `CURRENT_DATE()`.
        *   `NULL` `p_wiederanlaufWert` to verify default to `0`.
        *   Invalid `p_stichtag` formats (e.g., `20230120`, `ABC`) to verify error handling.
    *   **Integration Testing**:
        *   Populate `project.dataset.source_contract_cache` with a representative set of test data that mimics production scenarios, including data that should be filtered out by date and `DWH_VERTRAG_ID`.
        *   Run the stored procedure with these test data sets.
    *   **Orchestration Testing**:
        *   Deploy the job via the chosen Google Cloud scheduler (e.g., Cloud Composer DAG).
        *   Trigger the scheduled job to verify end-to-end execution, including parameter passing from the scheduler.
2.  **What "Passing" Means**:
    *   **Audit Log Verification**: For successful runs, the `project.dataset.job_audit` table must contain an entry with `status = 'OK'` and an appropriate `message`. For failed runs, it should show `status = 'ERROR'` with a descriptive error message.
    *   **Detailed Log Verification**: The `project.dataset.job_log` table should contain expected informational messages for successful execution and detailed 'ERROR' level entries for failures, aiding in debugging.
    *   **Data Integrity**: The `project.dataset.target_fos_table` must contain the correct, transformed data. This should be verified by comparing its contents with the expected output of the legacy job for the same input parameters and source data.
    *   **Restart Logic Validation**: Verify that when `p_wiederanlaufWert > 0`, the `DELETE` operation correctly removes existing records from `target_fos_table` and the subsequent `INSERT` only processes records with `DWH_VERTRAG_ID > p_wiederanlaufWert`.
    *   **Error Handling**: Ensure that invalid inputs or runtime errors within the stored procedure correctly trigger the `EXCEPTION` block, log the error, and raise an appropriate message.
    *   **Performance**: The job should complete within acceptable performance thresholds, comparable to or better than the legacy system. Monitor BigQuery query execution times.
    *   **Cloud Logging**: No unexpected errors or warnings should appear in Cloud Logging for the BigQuery job or the orchestrator component.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable New Job Schedule**: Immediately disable or delete the Google Cloud scheduler (e.g., Cloud Composer DAG, Cloud Scheduler job) that invokes `sp_bereitstellung_basisprodukte_bert`. This prevents further execution of the migrated job.
2.  **Revert BigQuery Code**:
    *   Delete the BigQuery Stored Procedures (`sp_bereitstellung_basisprodukte_bert`, `sp_process_contract_cache_data`) from the BigQuery dataset.
    *   If the `job_audit` and `job_log` tables were created solely for this migration and contain no other critical data, they can be dropped. Otherwise, they should be retained for historical auditing.
3.  **Data Rollback (if necessary)**:
    *   **For `target_fos_table`**:
        *   If the `sp_bereitstellung_basisprodukte_bert` job only performs `INSERT` operations, clearing the `target_fos_table` might be sufficient.
        *   If the job performs `DELETE` or `UPDATE` operations (especially due to the restart logic), a backup of `target_fos_table` should have been taken prior to go-live. Restore the `target_fos_table` from the most recent valid backup.
        *   Alternatively, if the `target_fos_table` is fully derived from `source_contract_cache`, it might be possible to simply truncate and re-populate it using the legacy job.
4.  **Reactivate Legacy Job**: Re-enable and restart the original `r_ausd_bp_ta_rn_da_vda_tk.ksh` script in the legacy environment to resume normal operations.
5.  **Post-Rollback Analysis**: Conduct a thorough root cause analysis of the issues that necessitated the rollback to inform future migration attempts.