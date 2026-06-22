# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `r_ausd_bp_ta_iccid_einzeln.ksh` KornShell script. This script, originally an orchestration wrapper for an ETL job, managed the initial provisioning of basic products for the BERT system by extracting contract cache data from the Data Warehouse (DWH) and preparing it for credit scoring (FOS).

The migration targets Google Cloud Platform (GCP), specifically:
*   **BigQuery:** For hosting the core logic as Stored Procedures and for all data storage (source DWH tables, target FOS table, and logging/status tables).
*   **Cloud Composer (Apache Airflow):** For orchestrating the execution of the BigQuery Stored Procedures, replacing the shell-based scheduling.

The primary goal was to convert the shell script's control flow, parameter handling, and logging mechanisms into a BigQuery-native solution, while identifying the need for a separate migration of the underlying "kernel" data processing logic.

## 2. Generated Artifacts

The migration produced the following BigQuery SQL artifacts:

*   **`your_gcp_project.your_bq_dataset.job_log.sql`**
    *   **Role:** This DDL script defines the `job_log` table. This table serves as a centralized repository for all execution logs, including job start/end, parameter values, informational messages, warnings, and detailed error messages. It replaces the file-based logging of the original ksh script.
*   **`your_gcp_project.your_bq_dataset.job_status.sql`**
    *   **Role:** This DDL script defines the `job_status` table. This table tracks the latest status (RUNNING, SUCCEEDED, FAILED) of the migrated job, along with the timestamp of the last update and any associated error messages. It provides a quick overview of the job's health.
*   **`your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_kernel.sql`**
    *   **Role:** This is a placeholder BigQuery Stored Procedure. It represents the future migration of the `k_ausd_bp_ta_iccid_einzeln.ksh` kernel script, which contains the actual data extraction, transformation, and loading logic. The wrapper procedure (`sp_ausd_bp_ta_iccid_einzeln_wrapper`) will call this kernel procedure. Its detailed implementation is a follow-up task.
*   **`your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper.sql`**
    *   **Role:** This BigQuery Stored Procedure is the direct migration of the `r_ausd_bp_ta_iccid_einzeln.ksh` wrapper script. It handles:
        *   Accepting input parameters (`p_stichtag`, `p_wiederanlaufWert`).
        *   Defaulting parameter values (e.g., `p_wiederanlaufWert` to '0', `p_stichtag` to system date).
        *   Parameter validation.
        *   Centralized logging to `job_log` and status updates to `job_status`.
        *   Orchestrating the execution of the kernel logic by calling `sp_ausd_bp_ta_iccid_einzeln_kernel`.
        *   Implementing robust error handling using BigQuery's `EXCEPTION WHEN ERROR` blocks.

## 3. Key Design Decisions

The migration strategy focused on leveraging BigQuery's capabilities for data processing and GCP's orchestration services for job scheduling and monitoring.

*   **Migration to BigQuery Stored Procedures:** The shell script's control flow, parameter parsing, and environmental setup were translated into a BigQuery Stored Procedure (`sp_ausd_bp_ta_iccid_einzeln_wrapper`). This decision centralizes the job's logic within the data platform, allowing for better integration with BigQuery's data processing capabilities, improved maintainability, and easier debugging compared to distributed shell scripts.
*   **Cloud Composer for Orchestration:** Cloud Composer (Apache Airflow) was chosen to replace the legacy shell-based scheduling. Airflow provides robust scheduling, dependency management, monitoring, and alerting features, which are superior to simple cron jobs or shell script chains. This allows for a more resilient and observable data pipeline.
*   **Centralized BigQuery Logging and Status:** Instead of disparate log files, dedicated BigQuery tables (`job_log`, `job_status`) were created. This enables centralized, queryable logging and real-time job status tracking, significantly improving operational visibility and troubleshooting.
*   **Modular Design for Kernel Logic:** The core data transformation logic, originally in `k_ausd_bp_ta_iccid_einzeln.ksh`, was explicitly separated and represented as a placeholder BigQuery Stored Procedure (`sp_ausd_bp_ta_iccid_einzeln_kernel`). This modular approach allows for the wrapper to be migrated independently, while acknowledging that the kernel requires its own detailed design and implementation, potentially involving complex SQL or other BigQuery features.
*   **BigQuery-Native Error Handling:** The shell script's `trap` mechanism was replaced with BigQuery's `EXCEPTION WHEN ERROR` blocks and `RAISE` statements. This provides structured error handling within the stored procedure, ensuring that failures are caught, logged, and propagated to the orchestrator.

**Notable Trade-offs:**
*   **Increased BigQuery SQL Complexity:** Implementing control flow, variable management, and error handling in BigQuery SQL can be more verbose and less intuitive than in a shell script for simple orchestration tasks.
*   **Dependency on Kernel Migration:** The full functionality of the job cannot be validated until the `sp_ausd_bp_ta_iccid_einzeln_kernel` is implemented, which is a significant follow-up task.
*   **Initial Setup Overhead:** Setting up Cloud Composer and BigQuery infrastructure requires more initial effort than deploying a simple shell script.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and Dataset Setup:**
    *   Ensure the target GCP project (`your_gcp_project`) exists.
    *   Create the BigQuery dataset (`your_bq_dataset`) where all tables and procedures will reside.
2.  **IAM Permissions:**
    *   Grant the service account that will execute the BigQuery procedures (e.g., the Cloud Composer service account) the necessary BigQuery roles:
        *   `BigQuery Data Editor` (for `job_log`, `job_status`, and the target FOS table).
        *   `BigQuery Job User` (to run queries and procedures).
        *   `BigQuery Data Viewer` (for source DWH tables).
3.  **Deploy DDL for Logging and Status Tables:**
    *   Execute `your_gcp_project.your_bq_dataset.job_log.sql` to create the `job_log` table.
    *   Execute `your_gcp_project.your_bq_dataset.job_status.sql` to create the `job_status` table.
4.  **Deploy BigQuery Stored Procedures:**
    *   Execute `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_kernel.sql` to create the placeholder kernel procedure.
    *   Execute `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper.sql` to create the wrapper procedure.
5.  **Source Data Ingestion:**
    *   Ensure that the legacy DWH source tables (e.g., `DWH_TA_C_VERTRAG`) are ingested and available in BigQuery within `your_bq_dataset` (or a designated source dataset) with the correct schema and data.
6.  **Target FOS Table Creation:**
    *   Create the target `FOS_Tabelle` in BigQuery (`your_gcp_project.your_bq_dataset.FOS_Tabelle`) with the appropriate schema to receive the processed data.
7.  **Cloud Composer (Airflow) Setup:**
    *   Provision a Cloud Composer environment if one does not already exist.
    *   Create and deploy an Airflow DAG that:
        *   Calls the `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper` procedure using a `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator`.
        *   Passes the `p_stichtag` and `p_wiederanlaufWert` parameters as required.
        *   Configures appropriate scheduling, retries, and alerting.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent areas requiring further design and implementation:

*   **Kernel Script (`k_ausd_bp_ta_iccid_einzeln.ksh`) Migration:** This is the most significant unresolved item. The `sp_ausd_bp_ta_iccid_einzeln_kernel` procedure is currently a placeholder. A dedicated analysis and design effort is required to translate the actual data extraction, transformation, and loading logic from the original shell script into a robust BigQuery-native solution. This includes understanding the exact SQL queries, filtering conditions (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`), and the restart logic (`DWH_VERTRAG_ID > Wiederanlaufwert`).
*   **Data Model Clarity:** The precise schemas for the source DWH tables (e.g., `DWH_TA_C_VERTRAG`) and the target `FOS_Tabelle` are not fully defined in this document. Detailed data dictionaries are needed to ensure accurate data mapping and type conversions during the kernel migration.
*   **Restart Logic Implementation:** While the wrapper handles the `p_wiederanlaufWert` parameter, the exact mechanism for how the kernel script uses this value (e.g., for deleting existing records before inserting, or for filtering source data) needs to be fully understood and implemented within the `sp_ausd_bp_ta_iccid_einzeln_kernel`.
*   **Missing Complexity/Automation Data:** The absence of `file_complexity` and `automation_rate` for the original script means the effort assessment for the kernel migration remains an estimate.
*   **`f_alis_msgerr.ksh` and `h_alis_parameter.ksh` Replacements:** While the wrapper procedure handles basic parameter parsing and logging, any more complex functionalities from these original helper scripts (e.g., specific error codes, advanced parameter validation) would need to be explicitly replicated in BigQuery SQL or custom UDFs if required by the kernel.

## 6. Validation

Validation of the migrated wrapper involves both unit testing and integration testing.

**How to Run Tests:**

1.  **Unit Test (Wrapper Procedure):**
    *   Open the BigQuery UI or use the `bq` command-line tool.
    *   Call the `sp_ausd_bp_ta_iccid_einzeln_wrapper` procedure directly with various parameter combinations:
        *   `CALL your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper(NULL, NULL);` (Test defaults)
        *   `CALL your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper('01012023', NULL);` (Test `stichtag` with default `wiederanlaufWert`)
        *   `CALL your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper('01012023', '12345');` (Test both parameters)
        *   `CALL your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper('', '');` (Test empty strings)
        *   `CALL your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper(NULL, '12345');` (Test `wiederanlaufWert` with default `stichtag`)
    *   Observe the output and check the `job_log` and `job_status` tables.
2.  **Integration Test (via Cloud Composer):**
    *   Deploy the Airflow DAG that calls the wrapper procedure.
    *   Trigger the DAG manually or wait for its scheduled run.
    *   Monitor the DAG run in the Airflow UI for successful completion.
    *   Verify the logs in Cloud Logging (for Airflow task logs) and BigQuery `job_log` table.
    *   (Once kernel is implemented) Verify the data in the target `FOS_Tabelle`.

**What "Passing" Means:**

*   **Wrapper Procedure Execution:**
    *   The `sp_ausd_bp_ta_iccid_einzeln_wrapper` procedure completes without raising an unhandled error.
    *   The `job_log` table contains entries reflecting the job's start, parameter determination, validation, kernel call, and successful completion (or a detailed error message if a failure was intentionally triggered for testing error handling).
    *   The `job_status` table shows the job `r_ausd_bp_ta_iccid_einzeln.ksh` with a `SUCCEEDED` status and the correct `last_stichtag` and `last_wiederanlaufwert`.
    *   Parameters passed to the `sp_ausd_bp_ta_iccid_einzeln_kernel` (as seen in `job_log` entries from the kernel) correctly reflect the final determined values.
*   **Data Validation (Post-Kernel Implementation):**
    *   The data loaded into the `FOS_Tabelle` is accurate and complete, matching the expected output based on the original `k_ausd_bp_ta_iccid_einzeln.ksh` logic when run with identical input data and parameters. This will require a detailed comparison with the legacy system's output.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated to revert to the legacy system:

1.  **Stop New System Execution:**
    *   Immediately pause or delete the Cloud Composer DAG responsible for scheduling `sp_ausd_bp_ta_iccid_einzeln_wrapper`. This prevents any further execution of the migrated job.
2.  **Re-enable Legacy System:**
    *   Re-enable the original scheduler (e.g., cron job) for the `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh` script.
3.  **Data Rollback (if necessary):**
    *   **Assess Impact:** Determine if the migrated job has written any data to the target `FOS_Tabelle` that needs to be reverted or cleaned up. This is highly dependent on the `sp_ausd_bp_ta_iccid_einzeln_kernel`'s implementation.
    *   **Option A (No Destructive Writes):** If the kernel only inserts new data or performs idempotent updates, no specific data rollback might be needed, beyond ensuring the legacy system can overwrite/reprocess the data.
    *   **Option B (Destructive Writes/Deletes):** If the kernel performs deletes or complex updates that cannot be easily undone by the legacy system, a data restoration from a backup of the `FOS_Tabelle` (taken just before go-live) might be required. Alternatively, specific `DELETE` or `UPDATE` statements could be executed on the `FOS_Tabelle` to revert changes made by the new system.
4.  **Monitor Legacy System:**
    *   Verify that the legacy `r_ausd_bp_ta_iccid_einzeln.ksh` script is running as expected and producing correct output.

**Note:** A robust data rollback strategy for the `FOS_Tabelle` can only be fully defined once the `sp_ausd_bp_ta_iccid_einzeln_kernel`'s exact data manipulation logic is known. It is recommended to have a pre-go-live backup of all affected target tables.