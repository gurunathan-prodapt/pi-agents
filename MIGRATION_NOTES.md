# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh`. This script is responsible for orchestrating the initial provision of selected basic products ("Basisprodukte") for the BERT system, preparing a snapshot of contract cache data from a Data Warehouse (DWH) for a Forderungsscoring (scoring for receivables) system.

The migration targets Google Cloud Platform (GCP), leveraging:
*   **Google BigQuery**: For data processing, storage of source/target data, and implementation of the orchestration logic via Stored Procedures.
*   **Cloud Composer (Apache Airflow)**: For scheduling and managing the execution of the BigQuery Stored Procedures.
*   **BigQuery Tables**: For centralized audit and job run logging, replacing file-based logging.

The core business logic, currently residing in `k_ausd_bp_ta_msisdn.ksh`, is acknowledged as a separate, subsequent migration effort, with a placeholder BigQuery Stored Procedure created for now.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`job_audit_log_ddl.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the `job_audit_log` table. This table will store detailed audit information, including job status, errors, and messages, replacing the legacy file-based logging.
*   **`job_run_info_ddl.sql`**
    *   **Role**: BigQuery DDL script to create the `job_run_info` table. This table will track high-level job execution details such as job number, cutoff date (`stichtag`), and system date (`sysdate`).
*   **`k_ausd_bp_ta_msisdn_sp.sql`**
    *   **Role**: BigQuery Stored Procedure (placeholder) for the core business logic. This procedure is designed to encapsulate the data transformation and loading logic originally found in `k_ausd_bp_ta_msisdn.ksh`. It currently contains only a logging statement and requires further development.
*   **`ausd_bp_ta_msisdn_wrapper_sp.sql`**
    *   **Role**: BigQuery Stored Procedure that implements the orchestration logic of the original `r_ausd_bp_ta_msisdn.ksh` script. This includes parameter parsing, date determination, error handling, logging to the new audit tables, and invoking the `k_ausd_bp_ta_msisdn` core logic procedure.
*   **`ausd_bp_ta_msisdn_dag.py`**
    *   **Role**: An Apache Airflow DAG (Directed Acyclic Graph) script for Cloud Composer. This DAG is responsible for scheduling and executing the `ausd_bp_ta_msisdn_wrapper` BigQuery Stored Procedure, passing necessary parameters.

## 3. Key Design Decisions

The following key design decisions were made during this migration:

*   **Orchestration Logic to BigQuery Stored Procedure**: The KornShell orchestration logic (parameter handling, date determination, error trapping, logging, and calling the kernel script) was directly translated into a BigQuery Stored Procedure (`ausd_bp_ta_msisdn_wrapper_sp`).
    *   **Rationale**: This approach centralizes the job's control flow within BigQuery, leveraging its native capabilities for SQL-based logic, error handling, and integration with GCP's monitoring and logging services. It simplifies the overall architecture by keeping the core execution within the data platform.
    *   **Trade-offs**: Requires re-implementation of shell-specific utilities and error handling mechanisms using BigQuery SQL constructs, which can be less flexible than shell scripting for certain tasks.
*   **Separation of Orchestration and Core Business Logic**: The original script's pattern of an `r_` (runner/orchestrator) script calling a `k_` (kernel/core logic) script was preserved by creating two distinct BigQuery Stored Procedures: `ausd_bp_ta_msisdn_wrapper` for orchestration and `k_ausd_bp_ta_msisdn` for the core business logic.
    *   **Rationale**: This maintains a clear separation of concerns, allowing the complex data transformation logic to be developed and migrated independently. It also makes the orchestration wrapper cleaner and more reusable.
    *   **Trade-offs**: Introduces an additional dependency between the two stored procedures and necessitates a separate, detailed analysis and migration effort for the `k_ausd_bp_ta_msisdn` script.
*   **Centralized Audit and Logging in BigQuery**: File-based logging and status tracking from the legacy system were replaced with dedicated BigQuery tables (`job_audit_log`, `job_run_info`).
    *   **Rationale**: Provides a scalable, queryable, and centralized repository for job execution metadata. This significantly improves monitoring, troubleshooting, and historical analysis capabilities compared to distributed log files. It also integrates seamlessly with BigQuery's ecosystem.
    *   **Trade-offs**: Requires explicit `INSERT` statements for logging within the stored procedures, which is more verbose than simple shell `echo` commands.
*   **Cloud Composer for Scheduling and Execution**: Apache Airflow, managed via Cloud Composer, was chosen as the primary orchestration tool for scheduling and triggering the BigQuery Stored Procedures.
    *   **Rationale**: Cloud Composer offers a robust, scalable, and feature-rich platform for managing complex data pipelines. It provides advanced scheduling, dependency management, monitoring, and integration with other GCP services, making it suitable for enterprise-grade data workflows.
    *   **Trade-offs**: Cloud Composer has a higher operational overhead and learning curve compared to simpler scheduling options like BigQuery Scheduled Queries or Google Workflows. However, its benefits for complex environments outweigh these for this use case.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be performed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
2.  **BigQuery Table Creation**:
    *   Execute the DDL scripts to create the audit and run info tables:
        ```bash
        bq query --use_legacy_sql=false < job_audit_log_ddl.sql
        bq query --use_legacy_sql=false < job_run_info_ddl.sql
        ```
    *   **Source Tables**: Ensure all source DWH tables (e.g., `DWH$TA_C_VERTRAG`) are ingested and available in BigQuery (e.g., as `project.dataset.source_table`). The specific DDL for these tables must be created based on the source system's schema.
    *   **Target Table**: Create the DDL for the `FOS-Tabelle` (e.g., `project.dataset.target_table`) that will receive the processed data. This DDL will be finalized during the `k_ausd_bp_ta_msisdn` kernel script migration.
3.  **BigQuery Stored Procedure Deployment**:
    *   Deploy the generated BigQuery Stored Procedures:
        ```bash
        bq query --use_legacy_sql=false < k_ausd_bp_ta_msisdn_sp.sql
        bq query --use_legacy_sql=false < ausd_bp_ta_msisdn_wrapper_sp.sql
        ```
4.  **IAM Permissions Configuration**:
    *   **BigQuery Service Account**: Create or identify a service account that has the necessary permissions to:
        *   Execute BigQuery Stored Procedures (`bigquery.routines.call`).
        *   Read from source tables (`bigquery.tables.getData`).
        *   Write to target tables (`bigquery.tables.updateData`, `bigquery.tables.create`).
        *   Insert into `job_audit_log` and `job_run_info` tables (`bigquery.tables.insertData`).
        *   Recommended roles: `BigQuery Data Editor`, `BigQuery Job User`.
    *   **Cloud Composer Service Account**: The service account associated with the Cloud Composer environment must have permissions to:
        *   Trigger BigQuery jobs (`bigquery.jobs.create`).
        *   Access the BigQuery dataset and routines.
        *   Recommended roles: `Composer Worker`, `BigQuery Job User`.
5.  **Cloud Composer DAG Deployment**:
    *   Upload the `ausd_bp_ta_msisdn_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Configure the DAG parameters (`GCP_PROJECT_ID`, `BQ_DATASET_ID`) within the DAG file or via Airflow variables.
    *   Set the desired `schedule` for the DAG (e.g., `@daily`, `0 3 * * *`).
6.  **Data Ingestion Pipelines**:
    *   Ensure that robust and reliable data ingestion pipelines are in place to bring the `DWH$TA_C_VERTRAG` and any other required source data from the legacy DWH into BigQuery. This might involve tools like Dataflow, Cloud Data Fusion, or Storage Transfer Service.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or require further follow-up:

*   **Core Business Logic (`k_ausd_bp_ta_msisdn.ksh`) Migration**: The most significant gap is the complete migration of the actual data transformation and loading logic from `k_ausd_bp_ta_msisdn.ksh`. The `k_ausd_bp_ta_msisdn_sp.sql` is currently a placeholder and needs dedicated analysis and implementation. This includes:
    *   Identifying the exact source tables and their schemas.
    *   Understanding the full transformation logic (e.g., filtering, joins, aggregations, data type conversions).
    *   Defining the precise schema for the target `FOS-Tabelle`.
    *   Implementing the restart logic (`p_wiederanlaufWert`) within the kernel SP (e.g., conditional `DELETE` and `WHERE` clauses).
*   **`DWH$TA_C_VERTRAG` Schema Details**: The exact schema, data types, and primary keys of the `DWH$TA_C_VERTRAG` table (and any other implied source tables) are not fully known from the provided context. This information is crucial for defining the corresponding BigQuery source tables.
*   **Helper Script Functionality**: The full functionality of the sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs to be thoroughly understood. While some aspects (parameter parsing, date handling) are covered, any remaining specific functions (e.g., complex error message formatting, date calculations) might need to be translated into BigQuery UDFs or integrated into the stored procedures.
*   **Downstream System Integration**: The method by which the "Forderungsscoring" system consumes the data from the `FOS-Tabelle` needs to be re-evaluated and implemented for BigQuery. This could involve BigQuery views, exports to Cloud Storage, or direct API access.
*   **`file_complexity` Assessment**: The original tier assessment was manual due to missing `file_complexity` data. A more precise assessment might be possible once the complexity of the `k_ausd_bp_ta_msisdn.ksh` logic is fully understood.

## 6. Validation

Validation of the migrated job involves several stages:

### Unit Tests (BigQuery Stored Procedures)

1.  **`ausd_bp_ta_msisdn_wrapper_sp`**:
    *   **Test Cases**:
        *   Call with valid `p_stichtag_string` (e.g., '01012023') and `p_wiederanlaufWert` (e.g., 100).
        *   Call with `p_stichtag_string` as `NULL` or empty string (should default to `CURRENT_DATE()`).
        *   Call with `p_wiederanlaufWert` as `NULL` (should default to 0).
        *   Call with an invalid `p_stichtag_string` format (e.g., '2023-01-01') to trigger parameter validation error.
    *   **Expected Outcome ("Passing")**:
        *   Successful calls should result in `status = 'OK'` in `job_audit_log` and an `INFO` message from the kernel procedure.
        *   Error calls should result in `status = 'ERROR'` in `job_audit_log` and `SIGNAL SQLSTATE '45000'` being raised.
        *   The `k_ausd_bp_ta_msisdn` procedure should be called with the correct, derived parameters.
        *   Entries in `job_audit_log` and `job_run_info` should accurately reflect the execution details (job_kennung, job_nr, stichtag, sysdate, status, messages).

2.  **`k_ausd_bp_ta_msisdn_sp` (Placeholder)**:
    *   **Test Cases**: Call directly with various valid parameters.
    *   **Expected Outcome ("Passing")**: Should log an `INFO` message indicating execution without error. (Full validation will occur once the kernel logic is implemented).

### Integration Tests (Cloud Composer DAG)

1.  **DAG Execution**:
    *   **Test Cases**: Trigger the `ausd_bp_ta_msisdn_orchestration` DAG manually in Cloud Composer.
    *   **Expected Outcome ("Passing")**:
        *   The DAG should complete successfully without Airflow task failures.
        *   The `BigQueryExecuteStoredProcedureOperator` task should show successful execution in Airflow logs.
        *   BigQuery job history should show the `ausd_bp_ta_msisdn_wrapper` stored procedure being executed.
        *   The `job_audit_log` table in BigQuery should contain entries reflecting the DAG's execution, with a final `status = 'OK'`.

### Data Validation (Post-Kernel Implementation)

1.  **Data Comparison**:
    *   **Test Cases**: Run the migrated job for a specific `stichtag` and `wiederanlaufWert` using a representative dataset.
    *   **Expected Outcome ("Passing")**:
        *   The data generated in `project.dataset.target_table` should be identical (or functionally equivalent, considering data type changes) to the output produced by the legacy `r_ausd_bp_ta_msisdn.ksh` job for the same input parameters and source data.
        *   Row counts, aggregate values, and specific record details should match.
        *   Data quality checks (e.g., null values, data ranges) should pass.
2.  **Performance**:
    *   **Test Cases**: Execute the job under typical load conditions.
    *   **Expected Outcome ("Passing")**: Execution time and BigQuery slot consumption should be within acceptable limits, ideally matching or improving upon the legacy system's performance.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Disable New Job**: Immediately pause or delete the `ausd_bp_ta_msisdn_orchestration` DAG in Cloud Composer to prevent further execution of the migrated job.
    *   **Re-enable Legacy Job**: Re-enable the original `r_ausd_bp_ta_msisdn.ksh` script in its legacy scheduling system to ensure business continuity.
2.  **Data Rollback (if necessary)**:
    *   If the migrated job has written erroneous or incomplete data to `project.dataset.target_table`, use BigQuery's time travel capabilities to revert the table to a state before the problematic execution.
        ```sql
        -- Example: Restore table to a state 1 hour ago
        CREATE OR REPLACE TABLE `project.dataset.target_table` AS
        SELECT * FROM `project.dataset.target_table` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```
    *   Alternatively, if backups were taken, restore the `target_table` from the most recent valid backup.
    *   Clear any partial or erroneous data from the `job_audit_log` and `job_run_info` tables related to the failed migration attempt, if desired for cleanliness.
3.  **Code Rollback**:
    *   **Delete BigQuery Stored Procedures**: Remove the deployed BigQuery stored procedures:
        ```bash
        bq rm -f -r project:dataset.ausd_bp_ta_msisdn_wrapper
        bq rm -f -r project:dataset.k_ausd_bp_ta_msisdn
        ```
    *   **Delete BigQuery Tables**: If the `job_audit_log` and `job_run_info` tables were created solely for this migration, they can be removed:
        ```bash
        bq rm -f project:dataset.job_audit_log
        bq rm -f project:dataset.job_run_info
        ```
    *   **Remove Airflow DAG**: Delete the `ausd_bp_ta_msisdn_dag.py` file from the Cloud Composer DAGs folder.
4.  **Verification**:
    *   Confirm that the legacy `r_ausd_bp_ta_msisdn.ksh` job is running successfully and producing the expected output in the legacy environment.
    *   Monitor the legacy system closely for any anomalies.

This rollback procedure ensures a swift return to the stable legacy state while allowing for post-mortem analysis of the migration failure.