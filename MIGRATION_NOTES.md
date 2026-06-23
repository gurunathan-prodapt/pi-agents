# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_bp_ta_iccid_einzeln.ksh` from its legacy environment to Google Cloud Platform (GCP). The original script orchestrated the extraction of contract cache data from a Data Warehouse (DWH) for credit scoring (FOS).

The migration targets the following GCP services:
*   **BigQuery:** For data storage (migrated DWH tables, FOS target tables, and audit logs) and data processing (BigQuery Stored Procedures replacing KornShell logic).
*   **Cloud Composer (Apache Airflow):** For job orchestration, scheduling, and parameter management.

The core logic, previously split between a wrapper (`r_ausd_bp_ta_iccid_einzeln.ksh`) and a kernel script (`k_ausd_bp_ta_iccid_einzeln.ksh`), has been re-implemented as two distinct BigQuery Stored Procedures.

## 2. Generated Artifacts

The migration process generated the following files, which constitute the new solution on GCP:

*   **`sql/ddl/job_audit_log.sql`**
    *   **Role:** Defines the schema for the `job_audit_log` BigQuery table. This table serves as a centralized repository for tracking the execution status, parameters, and messages of all migrated jobs, replacing the legacy filesystem-based logging.

*   **`sql/sprocs/k_ausd_bp_ta_iccid_einzeln.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core data extraction and transformation logic previously found in the `k_ausd_bp_ta_iccid_einzeln.ksh` kernel script. It queries the `dwh_contract_cache` table, applies filtering based on `p_stichtag` and `p_wiederanlaufWert`, and inserts the processed data into the `fos_contract_data` target table. It also logs its execution status to `job_audit_log`.

*   **`sql/sprocs/ausd_bp_ta_iccid_einzeln_wrapper.sql`**
    *   **Role:** This BigQuery Stored Procedure acts as the main entry point, replacing the `r_ausd_bp_ta_iccid_einzeln.ksh` wrapper script. It handles parameter parsing, validation, default value assignment (e.g., `p_stichtag` to current date), and orchestrates the call to the `k_ausd_bp_ta_iccid_einzeln` core logic procedure. It manages the overall job status and logs events to `job_audit_log`.

*   **`sql/sprocs/log_job_event.sql`**
    *   **Role:** A helper BigQuery Stored Procedure designed for internal use by other procedures to log informational messages or specific events to the `job_audit_log` table.

*   **`dags/r_ausd_bp_ta_iccid_einzeln_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) for Cloud Composer. This Python script defines the orchestration workflow, scheduling, and parameter interface for the migrated job. It triggers the `ausd_bp_ta_iccid_einzeln_wrapper` BigQuery Stored Procedure, passing parameters received from Airflow's UI or scheduler.

*   **`sql/ddl/dwh_contract_cache.sql`**
    *   **Role:** A placeholder DDL for the `dwh_contract_cache` BigQuery table. This table is intended to house the migrated contract cache data from the legacy DWH, serving as the source for the `k_ausd_bp_ta_iccid_einzeln` procedure. Its schema needs to be finalized based on the actual DWH source.

*   **`sql/ddl/fos_contract_data.sql`**
    *   **Role:** A placeholder DDL for the `fos_contract_data` BigQuery table. This table serves as the target for the processed data, making it available for the Forderungsscoring (FOS) system. Its schema needs to be finalized based on the specific requirements of the FOS system.

## 3. Key Design Decisions

*   **KornShell to BigQuery Stored Procedures:** The core business logic, previously embedded in KornShell scripts, was migrated to BigQuery Stored Procedures. This decision leverages BigQuery's native SQL capabilities for data processing, offering improved scalability, performance, and maintainability compared to shell scripting. It also reduces operational overhead by utilizing a fully managed service.
*   **Maintaining Wrapper/Kernel Separation:** The logical separation between the wrapper (`r_ausd_bp_ta_iccid_einzeln.ksh`) and kernel (`k_ausd_bp_ta_iccid_einzeln.ksh`) scripts was preserved by creating two distinct BigQuery Stored Procedures (`ausd_bp_ta_iccid_einzeln_wrapper` and `k_ausd_bp_ta_iccid_einzeln`). This promotes modularity, reusability, and clearer separation of concerns.
*   **Cloud Composer for Orchestration:** Cloud Composer (managed Apache Airflow) was chosen for scheduling and orchestrating the job. This provides a robust, scalable, and feature-rich platform for workflow management, including dependency handling, retries, and integration with other GCP services, replacing manual cron jobs or custom schedulers.
*   **BigQuery as Central Data Platform:** BigQuery is used for all persistent data storage, including source DWH data, target FOS data, and job audit logs. This consolidates data management, simplifies data access, and benefits from BigQuery's performance and cost-effectiveness for analytical workloads.
*   **Centralized Audit Logging:** File-based logging was replaced by a structured `job_audit_log` table in BigQuery. This enables easier querying of job history, status, and parameters, and integrates seamlessly with Cloud Logging and Cloud Monitoring for enhanced observability.
*   **Parameter Handling:** Script parameters (`Stichtag`, `Wiederanlaufwert`) are now passed as parameters to the BigQuery Stored Procedures and managed via Airflow DAG parameters. This provides a more structured and type-safe way to handle inputs compared to shell arguments.
*   **Native BigQuery Functions for Utilities:** Common shell utilities (e.g., date manipulation, error handling) are replaced by BigQuery's rich set of SQL functions and `EXCEPTION` blocks, eliminating the need to translate complex shell logic into SQL.
*   **Idempotency Strategy (Trade-off):** The `k_ausd_bp_ta_iccid_einzeln` procedure currently uses a simple `INSERT` into the target table. A commented-out `TRUNCATE TABLE` suggests a full reload strategy. For production, a `MERGE` statement or a more sophisticated incremental load approach might be required to ensure idempotency and handle partial failures gracefully. This is a trade-off between simplicity and robustness, with the current implementation leaning towards simplicity for initial migration.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **GCP Project and Dataset Setup:**
    *   Ensure the target GCP project (`project`) is active and billing is enabled.
    *   Create the BigQuery dataset (`dataset`) where all tables and stored procedures will reside.

2.  **IAM Permissions:**
    *   **Cloud Composer Service Account:** Grant the Composer service account (typically `service-<project-number>@cloudcomposer.gserviceaccount.com`) the necessary BigQuery roles:
        *   `BigQuery Data Editor` (for creating/updating tables and inserting/updating data in `job_audit_log`, `dwh_contract_cache`, `fos_contract_data`).
        *   `BigQuery Job User` (for running queries and stored procedures).
    *   **User/Deployment Service Account:** Ensure the user or service account deploying the BigQuery DDLs and stored procedures has `BigQuery Data Editor` and `BigQuery Job User` roles.

3.  **Source Data Migration (`dwh_contract_cache`):**
    *   **Schema Finalization:** Review and finalize the schema for `project.dataset.dwh_contract_cache` based on the actual legacy DWH table structure. The provided DDL is a placeholder.
    *   **Data Ingestion:** Migrate historical and ongoing data from the legacy DWH into the `project.dataset.dwh_contract_cache` BigQuery table. This can be done using various methods such as BigQuery Data Transfer Service, Cloud Storage transfers, or custom data pipelines (e.g., Dataflow).

4.  **Target Data Schema Definition (`fos_contract_data`):**
    *   **Schema Finalization:** Define the complete and accurate schema for `project.dataset.fos_contract_data` based on the exact requirements of the Forderungsscoring (FOS) system. The provided DDL is a placeholder.

5.  **BigQuery Object Deployment:**
    *   Execute the DDL scripts in the following order to create the necessary tables and stored procedures:
        1.  `sql/ddl/job_audit_log.sql`
        2.  `sql/ddl/dwh_contract_cache.sql` (after schema finalization)
        3.  `sql/ddl/fos_contract_data.sql` (after schema finalization)
        4.  `sql/sprocs/log_job_event.sql`
        5.  `sql/sprocs/k_ausd_bp_ta_iccid_einzeln.sql`
        6.  `sql/sprocs/ausd_bp_ta_iccid_einzeln_wrapper.sql`

6.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Upload the `dags/r_ausd_bp_ta_iccid_einzeln_dag.py` file to the DAGs folder of your Composer environment.

7.  **Scheduling Configuration:**
    *   Once the DAG is uploaded, configure its schedule in the Airflow UI (e.g., `@daily`, `0 0 * * *`).
    *   Set default parameter values or ensure they are passed correctly during manual triggers.

## 5. Known Gaps & Unresolved References

*   **Detailed Kernel Script Logic:** The most significant gap is the complete and detailed transformation logic within the original `k_ausd_bp_ta_iccid_einzeln.ksh`. The provided BigQuery stored procedure (`k_ausd_bp_ta_iccid_einzeln.sql`) is a functional translation based on the wrapper's description but assumes a basic `SELECT` and `INSERT`. A thorough review and reverse-engineering of the original kernel script's SQL queries, joins, and any complex data manipulations are required to ensure full functional parity.
*   **DWH Source Table Schema (`dwh_contract_cache`):** The DDL for `dwh_contract_cache` is a placeholder. The actual schema (column names, data types, primary keys, partitioning/clustering strategy) must be accurately derived from the legacy DWH system.
*   **FOS Target Table Schema (`fos_contract_data`):** The DDL for `fos_contract_data` is also a placeholder. The exact schema required by the downstream Forderungsscoring (FOS) system needs to be defined and implemented. This includes all necessary columns, data types, and potential unique constraints.
*   **Data Volume and Frequency:** The expected data volume and execution frequency of the job are not specified. This information is crucial for optimizing BigQuery table partitioning, clustering, and for fine-tuning the Cloud Composer DAG's schedule and resource allocation.
*   **Idempotency Strategy for Target Table:** The current `k_ausd_bp_ta_iccid_einzeln` procedure performs a simple `INSERT`. Depending on the job's nature (e.g., daily full refresh, incremental update), this might need to be replaced with a `MERGE` statement or a `TRUNCATE` followed by `INSERT` to ensure data consistency and idempotency upon re-runs.
*   **Error Handling Granularity:** While basic `EXCEPTION` handling is implemented, specific business-level error conditions or custom error codes from the original KornShell script might need to be mapped and handled explicitly within the BigQuery stored procedures.
*   **Performance Tuning:** Initial migration focuses on functional parity. Post-migration, performance monitoring and tuning (e.g., optimizing BigQuery queries, adjusting partitioning/clustering, reviewing Airflow resource allocation) will be necessary to meet SLAs.

## 6. Validation

Validation of the migrated job involves unit testing individual components and integration testing the end-to-end workflow.

### How to Run Tests:

1.  **BigQuery Stored Procedure Unit Tests:**
    *   **`k_ausd_bp_ta_iccid_einzeln`:**
        *   Manually call the stored procedure in BigQuery SQL interface with various `p_stichtag` and `p_wiederanlaufWert` values.
        *   Test with valid dates, dates that should return no data, and dates that should return specific data.
        *   Test with `p_wiederanlaufWert` being `NULL` and with specific integer values.
        *   Ensure `dwh_contract_cache` contains representative test data.
    *   **`ausd_bp_ta_iccid_einzeln_wrapper`:**
        *   Manually call the wrapper procedure with various `p_stichtag_raw` (valid DDMMYYYY, invalid format, NULL/empty) and `p_wiederanlaufWert_raw` (valid integer, invalid format, NULL/empty) inputs.
        *   Verify parameter parsing, default assignments, and error handling for invalid inputs.
    *   **`log_job_event`:**
        *   Verify that calls to this helper procedure correctly insert records into `job_audit_log`.

2.  **Cloud Composer Integration Tests:**
    *   **Manual Trigger:** Trigger the `r_ausd_bp_ta_iccid_einzeln` DAG manually from the Airflow UI.
    *   **Parameter Input:** Provide different combinations of `stichtag_raw` and `wiederanlaufwert_raw` via the Airflow UI's "Trigger DAG with config" option.
    *   **Scheduled Run:** Allow the DAG to run according to its defined schedule (if any) to test automated execution.

### What "Passing" Means:

*   **Orchestration Success:** The `r_ausd_bp_ta_iccid_einzeln` DAG completes successfully in Cloud Composer (green status in Airflow UI).
*   **BigQuery Job Status:**
    *   For successful runs, the `job_audit_log` table should contain entries for both `ausd_bp_ta_iccid_einzeln_wrapper` and `k_ausd_bp_ta_iccid_einzeln` with `status = 'SUCCEEDED'` and appropriate `start_time`, `end_time`, and `message` fields.
    *   For expected error scenarios (e.g., invalid date format), the `job_audit_log` should show `status = 'FAILED'` with a descriptive `message`.
*   **Data Validation:**
    *   **Row Count:** The number of rows inserted into `project.dataset.fos_contract_data` should match the expected count from the legacy system for the same input parameters.
    *   **Data Content:** A sample of the extracted data in `fos_contract_data` should be compared against the output of the legacy system to ensure data accuracy, column mapping, and transformation logic parity.
    *   **Schema Conformity:** The data in `fos_contract_data` should conform to its defined schema (data types, nullability).
*   **Logging and Monitoring:**
    *   No unexpected errors or warnings should appear in Cloud Logging for the BigQuery jobs or the Cloud Composer tasks.
    *   Cloud Monitoring dashboards (if configured) should show normal resource utilization and no alerts.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Pause/Disable New Job:**
    *   Immediately pause or delete the `r_ausd_bp_ta_iccid_einzeln` DAG in the Cloud Composer Airflow UI to prevent further execution of the migrated job.

2.  **Revert BigQuery Stored Procedures (if necessary):**
    *   If previous versions of the stored procedures (`k_ausd_bp_ta_iccid_einzeln`, `ausd_bp_ta_iccid_einzeln_wrapper`, `log_job_event`) exist and are stable, revert to those versions.
    *   Alternatively, drop the newly deployed procedures if they are causing issues and there's no immediate need to revert to an older BigQuery version.

3.  **Data Rollback (if necessary):**
    *   **Target Table (`fos_contract_data`):**
        *   If the `k_ausd_bp_ta_iccid_einzeln` procedure was configured for a full reload (e.g., `TRUNCATE` then `INSERT`), simply truncate the `project.dataset.fos_contract_data` table to remove any data generated by the failed run.
        *   If the procedure uses `MERGE` or incremental inserts, identify and delete/update the records inserted by the problematic run using the `load_timestamp` or `stichtag` and `run_id` from the `job_audit_log`.
        *   If data corruption occurred, restore the `fos_contract_data` table from a previous BigQuery snapshot or backup.
    *   **Audit Log (`job_audit_log`):** While generally not rolled back, entries for failed runs can be marked or filtered out if needed for reporting.

4.  **Re-enable Legacy System:**
    *   Re-enable the execution of the original `r_ausd_bp_ta_iccid_einzeln.ksh` script in its legacy environment. Ensure all necessary dependencies and configurations are in place for the legacy job to run successfully.

5.  **Investigation and Remediation:**
    *   Analyze the `job_audit_log` entries, Cloud Logging, and Airflow task logs to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery stored procedures, DAG, or underlying data.
    *   Once the fix is implemented and thoroughly tested in a non-production environment, the migration process can be re-attempted.