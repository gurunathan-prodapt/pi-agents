# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh` to Google Cloud Platform (GCP).

The original script, responsible for provisioning selected base products (e.g., FAX, Data24) for the BERT system by creating a cutoff-date snapshot of the contract cache for "Forderungsscoring," has been migrated.

The target platform leverages:
*   **Google BigQuery** for data storage and processing, utilizing Stored Procedures to encapsulate the business logic.
*   **Google Cloud Composer (Apache Airflow)** for orchestration, scheduling, and monitoring of the BigQuery processes.

The migration involved transforming the shell script's orchestration, parameter handling, and logging into BigQuery Stored Procedures and a Cloud Composer DAG. The core data transformation logic, originally in `k_ausd_bp_ta_apn_vertrag.ksh`, is represented by a placeholder BigQuery Stored Procedure, requiring further detailed implementation.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`sql/ddl/job_registry.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the `job_registry` table. This table serves as a central repository for tracking the execution metadata of all migrated jobs, including start/end times, status, and parameters. It replaces the custom job control and status tracking of the legacy environment.
*   **`sql/ddl/job_log.sql`**
    *   **Role**: BigQuery DDL script to create the `job_log` table. This table stores detailed log entries (INFO, WARNING, ERROR) generated during job execution, linked to specific job runs in `job_registry`. It replaces the custom logging framework (`f_alis_msgerr.ksh`) used in the legacy script.
*   **`sql/procedures/ausd_bp_ta_apn_vertrag_wrapper.sql`**
    *   **Role**: BigQuery Stored Procedure that replaces the `r_ausd_bp_ta_apn_vertrag.ksh` wrapper script. It handles input parameter parsing, defaulting logic for `p_stichtag` and `p_wiederanlaufWert`, job logging (to `job_registry` and `job_log`), and robust error handling. This procedure orchestrates the call to the core processing logic.
*   **`sql/procedures/k_ausd_bp_ta_apn_vertrag.sql`**
    *   **Role**: BigQuery Stored Procedure acting as a placeholder for the core data transformation logic. This procedure is intended to replace `k_ausd_bp_ta_apn_vertrag.ksh`. It will contain the actual BigQuery SQL for data extraction, filtering, transformation, and loading into the target `fos_contract_cache` table. **Note: The detailed implementation of this procedure is a known gap and requires further development.**
*   **`sql/ddl/fos_contract_cache.sql`**
    *   **Role**: BigQuery DDL script to create the `fos_contract_cache` table. This table is the target for the processed contract data, serving the "Forderungsscoring" application. It replaces the "FOS-Tabelle" mentioned in the legacy documentation.
*   **`dags/ausd_bp_ta_apn_vertrag_dag.py`**
    *   **Role**: Python script defining an Apache Airflow DAG for Cloud Composer. This DAG is responsible for scheduling and triggering the `ausd_bp_ta_apn_vertrag_wrapper` BigQuery Stored Procedure. It provides robust orchestration capabilities, including scheduling, parameter passing, and monitoring.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Business Logic**: The core and wrapper logic were migrated to BigQuery Stored Procedures. This decision was made to leverage BigQuery's native SQL capabilities, scalability, and performance for data processing, aligning with a cloud-native data warehousing approach. It allows for direct execution within the data platform, minimizing data movement.
*   **Cloud Composer (Apache Airflow) for Orchestration**: Cloud Composer was chosen to replace the legacy scheduler and shell-based orchestration. Airflow provides a robust, scalable, and feature-rich platform for defining, scheduling, and monitoring complex data workflows. Its Python-based DAGs offer flexibility for parameterization, error handling, and integration with other GCP services.
*   **Separation of Wrapper and Core Logic**: The original `r_ausd_bp_ta_apn_vertrag.ksh` acted as a wrapper for `k_ausd_bp_ta_apn_vertrag.ksh`. This structure was preserved by creating two distinct BigQuery Stored Procedures (`ausd_bp_ta_apn_vertrag_wrapper` and `k_ausd_bp_ta_apn_vertrag`). This maintains modularity, separates concerns (orchestration/logging vs. data transformation), and simplifies debugging.
*   **Dedicated BigQuery Logging Tables (`job_registry`, `job_log`)**: Instead of replicating the custom shell-based logging framework, dedicated BigQuery tables were designed for job metadata and detailed logging. This provides structured, queryable, and auditable logs directly within BigQuery, making it easier to monitor job health and troubleshoot issues.
*   **Parameter Handling and Defaulting in Wrapper SP**: The logic for parsing command-line arguments and applying default values (e.g., for `Stichtag` and `Wiederanlaufwert`) was directly translated into the `ausd_bp_ta_apn_vertrag_wrapper` BigQuery Stored Procedure. This ensures consistent parameter handling and validation within the BigQuery environment.

**Notable Trade-offs:**

*   **`semi_auto` Migration Bucket**: The original script was classified as `semi_auto`, indicating that a direct, fully automated conversion was not feasible. This is reflected in the `k_ausd_bp_ta_apn_vertrag.sql` being a placeholder, requiring manual analysis and implementation of the complex data transformation logic. This trade-off prioritizes a structured migration framework over immediate full automation of complex legacy SQL.
*   **Loss of Direct Shell Scripting Capabilities**: Moving to BigQuery Stored Procedures means losing the flexibility of shell scripting for file system operations or external command execution. This is generally a positive trade-off for data processing, but any such dependencies in the core script will need alternative GCP solutions (e.g., Cloud Functions, Dataflow).

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure a GCP project (`your_gcp_project`) is created and configured.
    *   Create the target BigQuery dataset (`your_bq_dataset`) within this project.
    *   **Action**: `gcloud projects create your_gcp_project` (if not exists), `bq mk --dataset your_gcp_project:your_bq_dataset`.
2.  **IAM Permissions**:
    *   **Service Account for Cloud Composer**: The Cloud Composer environment's service account (or a dedicated service account for the DAG) must have the following roles:
        *   `BigQuery Data Editor` (to create/update tables and run jobs).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   `BigQuery Data Viewer` (to read from source tables).
        *   `Composer Worker` (for general Composer operations).
    *   **User Permissions**: Any users manually testing or deploying the BigQuery components will need appropriate BigQuery roles.
    *   **Action**: Grant necessary IAM roles to the relevant service accounts and users.
3.  **BigQuery DDL and Stored Procedure Deployment**:
    *   Execute the DDL scripts to create the logging tables:
        *   `bq query --use_legacy_sql=false < sql/ddl/job_registry.sql`
        *   `bq query --use_legacy_sql=false < sql/ddl/job_log.sql`
        *   `bq query --use_legacy_sql=false < sql/ddl/fos_contract_cache.sql`
    *   Deploy the BigQuery Stored Procedures:
        *   `bq query --use_legacy_sql=false < sql/procedures/ausd_bp_ta_apn_vertrag_wrapper.sql`
        *   `bq query --use_legacy_sql=false < sql/procedures/k_ausd_bp_ta_apn_vertrag.sql` (after implementation)
    *   **Action**: Replace `your_gcp_project` and `your_bq_dataset` placeholders in the SQL files before execution.
4.  **Airflow Connection Configuration**:
    *   Ensure the `google_cloud_default` Airflow connection is correctly configured in your Cloud Composer environment. This connection is used by the `BigQueryStartStoredProcedureOperator`.
    *   **Action**: Verify or create the `google_cloud_default` connection in Airflow UI (Admin -> Connections).
5.  **Source Data Ingestion**:
    *   The source DWH tables (e.g., `dwh_contract_cache`) from which `k_ausd_bp_ta_apn_vertrag` reads must be ingested into BigQuery. This could involve batch loading, streaming, or using data transfer services.
    *   **Action**: Define and implement the data ingestion pipelines for all required source tables into `your_gcp_project.your_bq_dataset`.
6.  **Core Logic Implementation**:
    *   The `sql/procedures/k_ausd_bp_ta_apn_vertrag.sql` placeholder must be fully implemented with the actual data extraction, transformation, and loading logic derived from the original `k_ausd_bp_ta_apn_vertrag.ksh` script.
    *   **Action**: Develop and deploy the complete `k_ausd_bp_ta_apn_vertrag` stored procedure.
7.  **Cloud Composer DAG Deployment**:
    *   Upload the `dags/ausd_bp_ta_apn_vertrag_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Action**: Copy the DAG file to the GCS bucket associated with your Composer environment's DAGs folder.
8.  **Scheduling**:
    *   Once deployed, the DAG will appear in the Airflow UI. Enable the DAG and verify its `schedule_interval` (e.g., `@daily`) is appropriate for production.
    *   **Action**: Enable the DAG in Airflow UI. Adjust `schedule_interval` in the DAG file if needed.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up and represent areas requiring further analysis or development:

*   **Core Script (`k_ausd_bp_ta_apn_vertrag.ksh`) Complexity**: The most significant gap is the detailed implementation of the `k_ausd_bp_ta_apn_vertrag` BigQuery Stored Procedure. The original `k_ausd_bp_ta_apn_vertrag.ksh` script's content is currently unanalyzed in detail. Its complexity (e.g., intricate SQL, external calls, file operations) will dictate the final BigQuery SQL implementation or potentially require a hybrid approach with other GCP services.
    *   **Action**: Dedicated analysis of `k_ausd_bp_ta_apn_vertrag.ksh` is required to fully implement `sql/procedures/k_ausd_bp_ta_apn_vertrag.sql`.
*   **Source Data Definition**: The specific source tables and their schemas within the legacy "DWH" that feed the contract cache are not explicitly defined in the wrapper script.
    *   **Action**: Identify all source tables and their schemas from the original `k_ausd_bp_ta_apn_vertrag.ksh` and ensure they are correctly ingested into BigQuery.
*   **Target "FOS-Tabelle" Schema**: While a placeholder `fos_contract_cache.sql` is provided, the exact schema and data requirements of the "FOS-Tabelle" for "Forderungsscoring" need to be confirmed with the downstream consumers.
    *   **Action**: Collaborate with the "Forderungsscoring" team to finalize the `fos_contract_cache` table schema and ensure data compatibility.
*   **Legacy Environment Variables/Custom Utilities**: The original script sourced `$HOME/.dw_init` and custom utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). While logging and date functions are replaced, any other environment-specific configurations or complex logic from these utilities need to be identified and replicated in the GCP environment (e.g., via Airflow variables, BigQuery UDFs, or explicit parameters).
    *   **Action**: Review `$HOME/.dw_init` and other sourced scripts for critical configurations or logic not yet addressed.
*   **`p_wiederanlaufWert` Usage**: The `p_wiederanlaufWert` parameter's exact usage in the core logic (e.g., for restartability or incremental loads) needs to be fully understood and implemented correctly in `k_ausd_bp_ta_apn_vertrag`.
    *   **Action**: Confirm the precise role of `p_wiederanlaufWert` in the original core script and ensure its correct translation.

## 6. Validation

Validation involves ensuring the migrated job runs successfully, produces correct output, and integrates seamlessly with the target environment.

**How to Run Tests:**

1.  **Manual BigQuery Stored Procedure Execution (Unit Testing)**:
    *   Once `ausd_bp_ta_apn_vertrag_wrapper.sql` and `k_ausd_bp_ta_apn_vertrag.sql` (even with placeholder logic) are deployed, you can manually execute the wrapper SP from the BigQuery console or `bq` CLI:
        ```bash
        bq query --run_as_user --use_legacy_sql=false \
        "CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('28022023', '0');"
        ```
        Test with various `p_stichtag` and `p_wiederanlaufWert` values, including `NULL` or empty strings to verify defaulting logic.
2.  **Cloud Composer DAG Execution (Integration Testing)**:
    *   After deploying `dags/ausd_bp_ta_apn_vertrag_dag.py` to Cloud Composer, trigger the DAG manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for success or failure.
    *   Check Airflow logs for task-specific output and errors.

**What "Passing" Means:**

A successful migration validation implies the following:

1.  **DAG Success**: The `ausd_bp_ta_apn_vertrag_wrapper_dag` completes successfully in the Airflow UI without any task failures.
2.  **Job Registry Status**: The `your_gcp_project.your_bq_dataset.job_registry` table contains an entry for the executed job with `status = 'SUCCESS'` and `end_timestamp` populated.
3.  **Job Log Entries**: The `your_gcp_project.your_bq_dataset.job_log` table contains expected `INFO` messages for job start, parameter processing, and successful completion, with no `ERROR` or `WARNING` entries.
4.  **Data Correctness**:
    *   The `your_gcp_project.your_bq_dataset.fos_contract_cache` table is populated with data.
    *   The data in `fos_contract_cache` matches the expected output from the legacy system for the same `Stichtag` and `Wiederanlaufwert`. This requires a data comparison exercise between the legacy and migrated outputs.
    *   Data filtering (e.g., `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`) and other transformations are correctly applied.
5.  **Parameter Handling**: The `p_stichtag` and `p_wiederanlaufWert` are correctly interpreted and applied by the BigQuery Stored Procedures, including the defaulting logic.
6.  **Error Handling**: Intentional error scenarios (e.g., invalid `Stichtag` format, missing source data) should be tested to ensure errors are caught, logged correctly in `job_log`, and the `job_registry` status is `FAILED`.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Runs**:
    *   **Cloud Composer**: Immediately pause or disable the `ausd_bp_ta_apn_vertrag_wrapper_dag` in the Airflow UI to prevent any further execution of the migrated job.
    *   **Legacy Scheduler**: Ensure the original `r_ausd_bp_ta_apn_vertrag.ksh` job is re-enabled in the legacy scheduling system.
2.  **Revert Data (if necessary)**:
    *   If the `fos_contract_cache` table was overwritten or modified in a way that impacts downstream systems, and a clean state is required, consider:
        *   Restoring the `fos_contract_cache` table from a previous backup (if backups are configured).
        *   Running a compensating job to revert or correct the data.
        *   **Note**: This step is highly dependent on the impact of the issue and the nature of the data modification (e.g., `DELETE` + `INSERT` vs. `MERGE`).
3.  **Verify Legacy System**:
    *   Confirm that the original `r_ausd_bp_ta_apn_vertrag.ksh` job is running as expected and producing correct output in the legacy environment.
4.  **Investigate and Plan Re-deployment**:
    *   Analyze the root cause of the failure in the migrated system.
    *   Address the issues (e.g., fix bugs in BigQuery SPs, update DAG, correct data ingestion).
    *   Plan for re-testing and a subsequent re-deployment.

**Important Considerations for Rollback:**

*   **Data Consistency**: If the migrated job partially processed data, ensure that rolling back does not leave the target system in an inconsistent state.
*   **Downstream Impact**: Communicate immediately with downstream consumers of the `fos_contract_cache` table about any data inconsistencies or delays due to the rollback.
*   **Monitoring**: Maintain vigilant monitoring of both legacy and migrated systems during and after a rollback.