# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_aurd_rechstan.ksh` KornShell script. This script, originally responsible for orchestrating the generation of contract-related data snapshots for a credit scoring system, has been migrated from a traditional Unix/Linux environment to Google Cloud Platform (GCP).

The migration targets a **BigQuery Stored Procedure** for the orchestration and parameter management logic, leveraging BigQuery's native capabilities for SQL-based processing, logging, and error handling. The original UC4 scheduler will be replaced by a **Cloud Composer (Airflow) DAG** or **Google Cloud Workflow** for triggering. The core data transformation logic, originally in `k_aurd_rechstan.ksh`, is identified as a separate, subsequent migration effort.

## 2. Generated Artifacts

The migration process has generated the following BigQuery-specific artifacts:

*   **`target/bigquery/ddl/logging_tables.sql`**
    *   **Role**: This DDL script defines the BigQuery tables (`job_control`, `job_run_log`, `job_error_log`) used for centralized job logging, status tracking, and error reporting. These tables replace the file-based logging and custom messaging framework (`DWMSG_`) of the original KornShell script.
*   **`target/bigquery/stored_procedures/sp_erzeugung_abzug_rechnungsdaten.sql`**
    *   **Role**: This BigQuery Stored Procedure encapsulates the orchestration logic of the original `r_aurd_rechstan.ksh` script. It handles parameter parsing (`p_stichtag`, `p_wiederanlaufWert`), validation, default value assignment, and integrates with the new BigQuery logging tables. It also includes the error handling mechanism. This procedure is designed to invoke the migrated core data processing logic (from `k_aurd_rechstan.ksh`) once that is available.

## 3. Key Design Decisions

Several key design decisions were made during this migration:

*   **BigQuery Stored Procedure for Orchestration**: The core orchestration logic of `r_aurd_rechstan.ksh` was translated into a BigQuery Stored Procedure (`sp_erzeugung_abzug_rechnungsdaten`). This decision leverages BigQuery's serverless nature, native SQL capabilities for control flow, parameter handling, and error management, aligning with a modern cloud data warehousing approach. It eliminates the need for a separate compute instance to run shell scripts.
*   **Centralized BigQuery Logging Tables**: Instead of relying on file-based logs and custom shell messaging frameworks, a structured logging approach using dedicated BigQuery tables (`job_control`, `job_run_log`, `job_error_log`) was adopted. This provides enhanced observability, queryability of job history, and easier integration with GCP's monitoring and alerting services.
*   **Replacement of Shell Helper Scripts with BigQuery SQL**: The functionalities provided by various shell helper scripts (e.g., `h_alis_date.ksh` for date handling, `f_alis_msgerr.ksh` for error reporting) are replaced by native BigQuery SQL functions (`FORMAT_DATE`, `PARSE_DATE`) and BigQuery's `EXCEPTION WHEN ERROR` blocks. This reduces external dependencies, simplifies the execution environment, and consolidates logic within BigQuery.
*   **Decoupling Core Business Logic**: The data extraction and transformation logic residing in `k_aurd_rechstan.ksh` was explicitly identified as a separate migration effort. This phased approach allows for the migration of the wrapper/orchestration layer independently, acknowledging that the core logic might be more complex and could potentially require different BigQuery constructs (e.g., separate stored procedures, views, or even external processing tools like Dataproc if the logic is highly procedural or involves non-SQL operations).
*   **Trade-offs**: The primary trade-off of decoupling the core logic is that the full end-to-end functionality of the original job is not immediately available after the migration of `r_aurd_rechstan.ksh` alone. It introduces a dependency on the subsequent, accurate migration and integration of `k_aurd_rechstan.ksh`'s logic to achieve complete functional parity.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (e.g., `project.dataset`) exists. If not, create it using the BigQuery console or `bq mk` command. This dataset will host the logging tables and the stored procedure.
2.  **IAM Permissions Configuration**:
    *   **Service Account for Orchestration**: Create or identify a service account for the Cloud Composer DAG or Google Cloud Workflow that will trigger the BigQuery Stored Procedure.
    *   **BigQuery Permissions**: Grant this service account the necessary BigQuery roles:
        *   `BigQuery Data Editor` (or more granular permissions) on the `project.dataset` to create/update logging tables and execute the stored procedure.
        *   `BigQuery Data Viewer` on source tables and `BigQuery Data Editor` on target tables (once the `k_aurd_rechstan.ksh` logic is migrated and integrated).
3.  **Core Logic Migration and Deployment**:
    *   **Migrate `k_aurd_rechstan.ksh`**: The actual data extraction and transformation logic from `k_aurd_rechstan.ksh` must be migrated to BigQuery SQL. This will likely result in one or more BigQuery SQL scripts, views, or another stored procedure (e.g., `sp_k_aurd_rechstan`).
    *   **Deploy Core Logic**: Deploy the migrated core logic to BigQuery.
    *   **Integrate Core Logic**: Update the placeholder section within `sp_erzeugung_abzug_rechnungsdaten.sql` to `CALL` or execute the newly migrated core logic.
4.  **Orchestration Setup**:
    *   **Develop Cloud Composer DAG / Cloud Workflow**: Create and deploy the Cloud Composer (Airflow) DAG or Google Cloud Workflow definition. This orchestrator will be responsible for invoking `sp_erzeugung_abzug_rechnungsdaten`, passing the required `p_stichtag` and `p_wiederanlaufWert` parameters.
    *   **Scheduling**: Configure the DAG/Workflow's schedule to match the original UC4 job's execution frequency.
5.  **Connection Strings / Secrets**:
    *   While BigQuery stored procedures do not use traditional connection strings, ensure that the `project.dataset` references in the generated code are correctly configured for your GCP environment.
    *   If the migrated core logic (from `k_aurd_rechstan.ksh`) requires access to external systems or sensitive credentials, these should be managed securely using GCP Secret Manager and accessed appropriately by the BigQuery procedure or the orchestrator.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or require further follow-up:

*   **Core Business Logic (`k_aurd_rechstan.ksh`)**: This is the most significant unresolved item. The actual data extraction, transformation, and loading logic is *not* part of this migration and requires a separate, dedicated effort. The `sp_erzeugung_abzug_rechnungsdaten` procedure currently contains a placeholder for this logic.
*   **Schema Finalization**: The BigQuery SQL pseudocode in the design document and generated code uses placeholder table and dataset names (e.g., ``project.dataset.job_control``, ``project.dataset.source_contract_cache``, ``project.dataset.target_fos_table``). These need to be finalized with the actual target BigQuery project, dataset, and table names.
*   **Completeness of Helper Script Replication**: While general replacements are proposed for shell helper scripts (e.g., date functions, error handling), a detailed analysis of the exact functionalities of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` is required to ensure all aspects are accurately and completely replicated in BigQuery SQL.
*   **"Wiederanlaufwert" Logic Validation**: The `Wiederanlaufwert` (restart value) logic, which implies deleting records based on `DWH_VERTRAG_ID`, needs careful validation during the core logic migration to ensure it behaves idempotently and correctly in BigQuery, preventing data loss or duplication.
*   **BigQuery vs. Python for Core Logic**: Depending on the complexity and specific requirements of `k_aurd_rechstan.ksh`, a decision might be needed on whether to implement its logic purely in BigQuery SQL or if a more programmatic approach (e.g., Python with PySpark on Dataproc or Serverless Spark) is required, especially if it involves complex procedural logic or external file interactions.

## 6. Validation

Validation ensures that the migrated orchestration logic functions as expected.

### How to Run Tests

1.  **Deployment**: Ensure the `logging_tables.sql` DDL and `sp_erzeugung_abzug_rechnungsdaten.sql` stored procedure are successfully deployed to your BigQuery environment.
2.  **Manual Execution (BigQuery Console/CLI)**:
    *   Execute the stored procedure directly from the BigQuery console or using the `bq query` command-line tool. Test various parameter combinations:
        *   **Successful Run**: `CALL `project.dataset.sp_erzeugung_abzug_rechnungsdaten`('01012023', 100);` (replace with valid date/value)
        *   **Default `p_wiederanlaufWert`**: `CALL `project.dataset.sp_erzeugung_abzug_rechnungsdaten`('01012023', NULL);`
        *   **Default `p_stichtag`**: `CALL `project.dataset.sp_erzeugung_abzug_rechnungsdaten`(NULL, 0);`
        *   **Missing `p_stichtag` (Error Case)**: `CALL `project.dataset.sp_erzeugung_abzug_rechnungsdaten`('', 0);`
3.  **Orchestrator Execution (Once Core Logic is Integrated)**:
    *   Once the `k_aurd_rechstan.ksh` core logic is migrated and integrated into `sp_erzeugung_abzug_rechnungsdaten`, deploy and trigger the Cloud Composer DAG or Google Cloud Workflow. This will test the end-to-end flow, including parameter passing from the orchestrator.

### What "Passing" Means

*   **Orchestration Logic Validation**:
    *   **Successful Runs**:
        *   Query the `project.dataset.job_control` table: Verify that an entry for `job_kennung = 'BERT_RKOPF_STAN'` exists with `status = 'OK'`, and `start_ts` and `end_ts` are populated.
        *   Query the `project.dataset.job_run_log` table: Confirm the presence of the success message (`'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`) associated with the job's `eintragsnr`.
        *   Verify that `v_effective_stichtag` and `v_wiederanlaufWert` are correctly parsed, defaulted, and logged in the `job_control` table.
    *   **Error Scenarios**:
        *   For expected error cases (e.g., missing `p_stichtag`): Query `project.dataset.job_control` to confirm `status = 'ERROR'` and `error_message` contains the expected error.
        *   Query `project.dataset.job_error_log` to verify the corresponding error entry, including `error_code` and `error_message`.
*   **Core Business Logic Validation (Once Migrated and Integrated)**:
    *   **Data Accuracy**: Compare the data generated in the target BigQuery tables by the migrated job against the output of the original `k_aurd_rechstan.ksh` script for identical input parameters. This includes record counts, specific column values, and key aggregates.
    *   **Idempotency**: Test the `Wiederanlaufwert` logic by running the job multiple times with the same parameters to ensure it produces consistent results without data duplication or loss.
    *   **Performance**: Monitor the execution time of the BigQuery stored procedure to ensure it meets performance requirements.

## 7. Rollback Procedure

In the event of critical issues detected after go-live, the following rollback procedure should be followed:

1.  **Halt New Migrated Runs**: Immediately pause or disable the Cloud Composer DAG or Google Cloud Workflow that triggers the `sp_erzeugung_abzug_rechnungsdaten` BigQuery Stored Procedure. This prevents any further execution of the migrated job.
2.  **Reactivate Original Scheduler**: Reactivate the original UC4 job (`DW.BERT_RECHNUNGSDATEN.xml`) to resume execution of the `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh` KornShell script. This ensures business continuity using the proven legacy system.
3.  **Assess Data Impact**:
    *   Review the `project.dataset.job_control` and `project.dataset.job_error_log` tables in BigQuery to understand the extent and nature of any failures or incorrect data generation.
    *   If the core logic was integrated and produced erroneous data in target BigQuery tables, a data rollback or correction strategy might be necessary. This could involve restoring tables from backups, deleting incorrect data, or running corrective SQL statements, depending on the specific impact and the idempotency of the core logic.
4.  **Root Cause Analysis**: Conduct a thorough investigation using BigQuery logs, Cloud Logging for Composer/Workflows, and any other available diagnostics to identify the root cause of the failure.
5.  **Rectify and Re-deploy**: Once the issue is resolved (e.g., bug fix in the BigQuery stored procedure, correction in the orchestrator, or completion of the `k_aurd_rechstan.ksh` migration), re-deploy the corrected artifacts and re-attempt the migration process from the validation phase.