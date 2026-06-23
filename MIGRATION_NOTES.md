# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2`, a "Next Generation Exporter" framework, has been migrated from its legacy on-premise environment to Google Cloud Platform (GCP).

The original script orchestrated data extraction from an Oracle database, applied transformations using various Unix utilities (`nawk`, `sed`, `perl`), and distributed files via `scp`, `sftp`, and `mailx`. It featured complex logic for configuration-driven execution, partitioning, parallel processing, and incremental loading.

The migrated solution leverages a modern, cloud-native architecture on GCP:
*   **Orchestration:** Cloud Composer (Apache Airflow) for workflow management.
*   **Data Storage & Processing:** BigQuery for data warehousing and SQL transformations.
*   **Complex Data Transformation:** Dataflow (Apache Beam) for non-SQL text processing.
*   **File Staging & Archiving:** Google Cloud Storage (GCS) for landing zones and file management.
*   **External File Distribution:** Cloud Storage Transfer Service and custom Cloud Run services for SFTP/SCP and email.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring for operational visibility.

This migration involved a complete re-engineering of the script's logic into Python-based Airflow DAGs, BigQuery SQL (managed by Dataform), and Dataflow pipelines, eliminating the reliance on KornShell, `eval`, and local filesystem operations.

## 2. Generated Artifacts

The migration effort produced the following artifacts:

*   **`exporter_config.sql`**
    *   **Role:** BigQuery DDL for the `exporter_config` table. This table stores the migrated job configurations (e.g., `OUTPUT_SQL`, `DISTRIBUTION`, `META` nodes) that were originally defined in configuration files like `k_exis_v2_defaults.cfg`.
*   **`exporter_log.sql`**
    *   **Role:** BigQuery DDL for the `exporter_log` table. This table centralizes detailed log entries for each job run, replacing the legacy `$LogDatei` and custom `DWMSG_` functions.
*   **`exporter_status.sql`**
    *   **Role:** BigQuery DDL for the `exporter_status` table. This table tracks the overall status and last run details for exporter jobs, replacing the functionality of `dwh$ta_k_meldungen` for monitoring.
*   **`exporter_distribution_queue.sql`**
    *   **Role:** BigQuery DDL for the `exporter_distribution_queue` table. This table acts as a queue for external file distribution tasks, decoupling the Airflow DAG from direct interaction with external SFTP/SCP servers and email systems.
*   **`handletimestamps_bq.sql`**
    *   **Role:** BigQuery SQL User-Defined Function (UDF). This UDF re-implements the date calculation and formatting logic found in the original `handletimestamps` KornShell function.
*   **`fillattribs_bq.sql`**
    *   **Role:** BigQuery SQL User-Defined Function (UDF). This UDF provides basic attribute filling/placeholder resolution, similar to the original `fillattribs` function, for dynamic SQL generation within BigQuery. For more complex templating, Airflow's Python logic or Dataform's capabilities are used.
*   **`get_file_partitions_bq.sql`**
    *   **Role:** BigQuery SQL Stored Procedure. This procedure calculates file partitions based on date ranges and partition units, replacing the `getsubintervalls` function for determining file-based partitioning.
*   **`get_sql_splits_bq.sql`**
    *   **Role:** BigQuery SQL Stored Procedure. This procedure calculates SQL split boundaries for parallel query execution, replacing the `getsqlsplits` function for determining SQL-based partitioning.
*   **`r_exis_v2_dag.py`**
    *   **Role:** Apache Airflow DAG definition in Python. This is the core orchestration artifact, replacing the entire `r_exis_v2` KornShell script. It defines the sequence of tasks, handles parameter resolution, triggers BigQuery/Dataform jobs, manages GCS operations, and initiates external distribution.
*   **`r_exis_v2_sql_model_output.sql`**
    *   **Role:** Dataform SQL model. This model contains the re-engineered `OUTPUT_SQL` logic, which performs the main data extraction and initial transformations in BigQuery. It replaces the `sqlplus` execution of `OUTPUT_SQL` from the legacy script.
*   **`r_exis_v2_sql_model_pre.sql`**
    *   **Role:** Dataform SQL model. This model contains the re-engineered `PRE_SQL` logic, executed before the main data extraction for setup or checks.
*   **`r_exis_v2_sql_model_post.sql`**
    *   **Role:** Dataform SQL model. This model contains the re-engineered `POST_SQL` logic, executed after data extraction and processing for cleanup, status updates, or final checks.
*   **`r_exis_v2_dataflow_pipeline.py`**
    *   **Role:** Apache Beam pipeline in Python. This pipeline is deployed as a Dataflow job to handle complex, multi-stage text processing and reformatting that was previously done by `nawk`, `sed`, and `perl` pipes in the KornShell script.
*   **`external_distributor_cloud_run.py`**
    *   **Role:** Python application for a Cloud Run service. This service handles external file distribution (SFTP/SCP) and email notifications, replacing the direct `scp`, `sftp`, and `mailx` commands. It is triggered by the Airflow DAG.
*   **`sftp_config.yaml`**
    *   **Role:** Example configuration for SFTP details. This file is illustrative and represents the structure of SFTP configuration that would be stored securely in GCP Secret Manager and referenced by the Cloud Run service.

## 3. Key Design Decisions

The migration strategy focused on re-engineering the legacy KornShell script into a cloud-native, maintainable, and scalable solution on GCP.

*   **Orchestration with Cloud Composer (Airflow):**
    *   **Decision:** Replaced the entire KornShell script's control flow, scheduling, and dynamic execution logic with Python-based Airflow DAGs.
    *   **Rationale:** Airflow provides robust scheduling, dependency management, error handling, and native integration with GCP services, offering a significant upgrade in operational capabilities and maintainability compared to a complex shell script.
*   **Data Storage and SQL Processing with BigQuery & Dataform:**
    *   **Decision:** Migrated all Oracle data sources and SQL-based transformations (`PRE_SQL`, `OUTPUT_SQL`, `POST_SQL`) to BigQuery. Dataform manages the BigQuery SQL assets.
    *   **Rationale:** BigQuery offers petabyte-scale analytics, serverless execution, and cost-effectiveness. Dataform provides version control, testing, and dependency management for SQL, addressing the challenges of dynamic SQL generation and maintenance in the legacy script.
*   **Complex Text Processing with Dataflow:**
    *   **Decision:** Replaced intricate `nawk`, `sed`, and `perl` piping with Apache Beam pipelines executed on Dataflow.
    *   **Rationale:** Dataflow provides a managed, scalable, and robust service for complex data transformations, which are difficult to replicate efficiently or maintainably in BigQuery SQL or simple Python scripts. This ensures that non-SQL transformations are handled by the appropriate tool.
*   **File Staging and Archiving with Google Cloud Storage (GCS):**
    *   **Decision:** Utilized GCS as the primary landing zone for exported files and for temporary data staging.
    *   **Rationale:** GCS offers highly durable, scalable, and cost-effective object storage with built-in lifecycle management, replacing the reliance on local filesystems and manual `compress`/`rm` commands.
*   **External Distribution via Cloud Run & Cloud Storage Transfer Service:**
    *   **Decision:** Decoupled external file distribution (`scp`, `sftp`, `mailx`) from the core DAG by using Cloud Run services (for custom logic) and Cloud Storage Transfer Service (for scheduled transfers).
    *   **Rationale:** This approach enhances security (no direct credentials in Airflow), scalability, and observability. Cloud Run provides a serverless environment for custom code, while Cloud Storage Transfer Service is optimized for large-scale transfers.
*   **Configuration Management in BigQuery:**
    *   **Decision:** Migrated job configurations from flat files to structured BigQuery tables (`exporter_config`).
    *   **Rationale:** Centralizing configuration in BigQuery allows for easier management, versioning, and dynamic access by Airflow DAGs, eliminating the need for custom shell parsing logic and `eval`.
*   **Elimination of `eval` and Dynamic Shell Scripting:**
    *   **Decision:** Re-engineered all logic that relied on `eval` and complex shell piping into structured Python code within Airflow, BigQuery SQL, or Dataflow.
    *   **Rationale:** This significantly improves security, maintainability, and debuggability. Dynamic shell execution is prone to injection vulnerabilities and is notoriously difficult to test and scale.
*   **Standardized Logging and Monitoring:**
    *   **Decision:** Integrated logging with Cloud Logging and monitoring with Cloud Monitoring, replacing custom `DWMSG_` routines.
    *   **Rationale:** Leveraging GCP's native observability tools provides a unified, scalable, and feature-rich platform for understanding job execution, performance, and errors.

## 4. Manual Steps Before Go-Live

Before the migrated `r_exis_v2` job can go live, several manual steps are required:

1.  **GCP Project Setup:**
    *   Ensure a GCP Project is created and configured.
    *   Enable necessary APIs: BigQuery, Cloud Composer, Dataflow, Cloud Storage, Cloud Run, Secret Manager, Dataform.

2.  **BigQuery Dataset & Table Creation:**
    *   Create the BigQuery dataset (e.g., `your_bigquery_dataset`) where the control tables and output data will reside.
    *   Execute the DDLs for the control tables:
        *   `exporter_config.sql`
        *   `exporter_log.sql`
        *   `exporter_status.sql`
        *   `exporter_distribution_queue.sql`
    *   Execute the DDLs for BigQuery UDFs and Stored Procedures:
        *   `handletimestamps_bq.sql`
        *   `fillattribs_bq.sql`
        *   `get_file_partitions_bq.sql`
        *   `get_sql_splits_bq.sql`

3.  **Configuration Migration:**
    *   **Manually extract** the configuration details from the legacy `r_exis_v2` script and its associated config files (e.g., `k_exis_v2_defaults.cfg`).
    *   **Load this configuration data** into the `exporter_config` BigQuery table. This is a critical step as the Airflow DAG relies on this table for job parameters. Ensure all `OUTPUT_SQL`, `PRE_SQL`, `POST_SQL`, `DISTRIBUTION`, `META` nodes are correctly structured as JSON.

4.  **Oracle Source Data Migration:**
    *   **Perform initial bulk migration** of historical data from Oracle tables (e.g., `DWH$TA_K_MELDUNGEN`, and any other source tables used by `OUTPUT_SQL`) to BigQuery.
    *   **Set up ongoing incremental data transfer** from Oracle to BigQuery using appropriate GCP services (e.g., BigQuery Data Transfer Service, Datastream, or custom Dataflow pipelines) to ensure the BigQuery source tables are up-to-date.

5.  **GCS Bucket Setup:**
    *   Create the designated GCS buckets:
        *   `your-gcs-landing-bucket` (for exported files and Dataflow output).
        *   `your-gcs-temp-bucket` (for Dataflow temporary files and templates).
    *   Configure appropriate lifecycle policies for archiving and retention on these buckets.

6.  **IAM & Permissions:**
    *   Create dedicated GCP Service Accounts for:
        *   Cloud Composer (Airflow Worker, Scheduler, Webserver).
        *   BigQuery (Data Editor, Job User).
        *   Dataflow (Worker, Developer).
        *   Cloud Storage (Object Admin).
        *   Cloud Run (Invoker, Secret Manager Secret Accessor).
        *   Secret Manager (Secret Manager Secret Accessor).
    *   Grant these Service Accounts the minimum necessary permissions (Principle of Least Privilege) to interact with BigQuery, GCS, Dataform, Dataflow, Cloud Run, and Secret Manager.
    *   Ensure the Airflow Service Account has permissions to read from `exporter_config` and write to `exporter_log`, `exporter_status`, and `exporter_distribution_queue`.

7.  **Secret Management:**
    *   Store sensitive credentials (e.g., SFTP passwords, email API keys) in GCP Secret Manager.
    *   Update the `external_distributor_cloud_run.py` and `sftp_config.yaml` (if used as a template) to reference the correct secret names.

8.  **Dataform Setup:**
    *   Create a Dataform project, repository, and workspace.
    *   Upload the Dataform SQL models (`r_exis_v2_sql_model_output.sql`, `r_exis_v2_sql_model_pre.sql`, `r_exis_v2_sql_model_post.sql`) to the Dataform repository.
    *   Configure Dataform compilation and execution settings.

9.  **Cloud Run Deployment:**
    *   Deploy the `external_distributor_cloud_run.py` application as a Cloud Run service.
    *   Configure environment variables for the Cloud Run service (e.g., `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, `SENDER_EMAIL`, `SMTP_SERVER`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD_SECRET_NAME`).
    *   Ensure the Cloud Run service has the necessary IAM permissions to access GCS (to download files) and Secret Manager (to retrieve credentials).
    *   Update the `SFTP_CLOUD_RUN_SERVICE_URL` and `EMAIL_CLOUD_RUN_SERVICE_URL` variables in `r_exis_v2_dag.py` with the deployed Cloud Run service URLs.

10. **Dataflow Template Deployment:**
    *   If `r_exis_v2_dataflow_pipeline.py` is required, package it as a Dataflow template.
    *   Upload the Dataflow template to the `your-gcs-temp-bucket` (e.g., `gs://your-gcs-temp-bucket/dataflow_templates/r_exis_v2_dataflow_pipeline`).

11. **Airflow DAG Deployment:**
    *   Upload the `r_exis_v2_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Ensure all placeholder variables (e.g., `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, `GCS_LANDING_BUCKET`, `GCS_TEMP_BUCKET`, `GCP_REGION`, Dataform repository/workspace IDs) are updated with actual values.

## 5. Known Gaps & Unresolved References

This migration is a re-engineering effort, and certain aspects require careful consideration and potential follow-up:

*   **Complex Shell Logic and Dynamic `eval`:** The original script's heavy reliance on `eval` and intricate shell piping has been re-engineered into structured Python and BigQuery components. While the core functionality is replicated, the exact behavior and edge cases of highly dynamic shell command construction need thorough validation. Any remaining implicit behaviors or side effects of `eval` might require further analysis.
*   **Custom `ksh_dwh` Utilities:** The functionalities of sourced KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parser.ksh`) have been re-implemented in Python (for Airflow) or BigQuery (for data utilities). It's crucial to ensure that all nuances of these helper functions, especially error reporting, date handling, and parameter parsing, are fully captured and replicated in the new environment.
*   **Performance Characteristics of Parallelism:** The legacy script's custom parallel processing (`merger`, `filepartition`, `sqlpartition` with `mkfifo`) had specific performance characteristics. The new Airflow/BigQuery/Dataflow architecture uses different parallelism mechanisms. While generally more scalable, the performance profile might differ, requiring careful benchmarking and optimization to match or improve upon the original.
*   **Configuration Migration Complexity:** The migration of the deeply intertwined configuration from legacy files to the `exporter_config` BigQuery table is a manual and critical step. Any misinterpretation or incomplete transfer of configuration elements could lead to incorrect job execution. A robust validation process for the migrated configuration is essential.
*   **Legacy `DWMSG_*` Routines:** The custom DWH messaging and status update routines have been mapped to Cloud Logging, Cloud Monitoring, and BigQuery audit tables (`exporter_log`, `exporter_status`). While providing better observability, ensuring that all critical alerts and operational insights previously provided by `DWMSG_` are replicated and easily accessible is a follow-up item.
*   **Oracle `dbms_application_info`:** The Oracle-specific call to `dbms_application_info.set_module` was likely for auditing or tracking within the Oracle database. Its equivalent functionality in GCP (e.g., BigQuery audit logs, custom metadata in `exporter_log`) needs to be confirmed to meet any compliance or operational requirements.
*   **Oracle-Specific SQL Functions/Metadata:** If the original `OUTPUT_SQL`, `PRE_SQL`, or `POST_SQL` contained highly specific Oracle functions or relied on Oracle-specific metadata views (e.g., `DBA_TAB_PARTITIONS`), these have been translated to BigQuery equivalents or re-engineered. Thorough testing is needed to ensure functional parity.
*   **Incremental Merge Logic:** The legacy script's incremental merge logic using shell commands (`nawk`, `grep`) has been re-implemented using BigQuery `MERGE` statements. The exact behavior, especially concerning handling of duplicates, updates, and deletions, needs to be rigorously tested against the original.

## 6. Validation

Validation ensures the migrated job functions correctly, produces accurate results, and meets performance and operational requirements.

### How to Run Tests:

1.  **Unit Tests:**
    *   **Python Functions:** Run unit tests for all Python functions within the Airflow DAG (`r_exis_v2_dag.py`), Dataflow pipeline (`r_exis_v2_dataflow_pipeline.py`), and Cloud Run service (`external_distributor_cloud_run.py`).
    *   **BigQuery UDFs/SPs:** Execute test cases for `handletimestamps_bq`, `fillattribs_bq`, `get_file_partitions_bq`, and `get_sql_splits_bq` directly in BigQuery.
2.  **Dataform Validation:**
    *   Compile and run Dataform assertions for `r_exis_v2_sql_model_output.sql`, `r_exis_v2_sql_model_pre.sql`, and `r_exis_v2_sql_model_post.sql` within the Dataform environment to ensure SQL correctness and data quality.
3.  **Integration Tests (Dev/Staging Composer):**
    *   **Trigger the `r_exis_v2_dag`** manually in a development or staging Cloud Composer environment.
    *   **Verify task execution:** Monitor Airflow UI for successful task completion, XCom pass-through, and correct branching/looping.
    *   **BigQuery Interactions:** Check `exporter_config`, `exporter_log`, `exporter_status`, and `exporter_distribution_queue` tables for correct entries and updates.
    *   **GCS Operations:** Verify that output files are correctly generated in `your-gcs-landing-bucket` with the expected naming conventions and compression. Check temporary files in `your-gcs-temp-bucket`.
    *   **Dataflow Execution:** If the Dataflow task is enabled, verify that the Dataflow job starts, processes data, and completes successfully.
    *   **Cloud Run Invocation:** Confirm that the Cloud Run services for SFTP/SCP and email are correctly invoked and respond as expected.
4.  **End-to-End Data Validation (UAT):**
    *   **Run the migrated job** with a representative dataset.
    *   **Compare output files:** Perform a byte-by-byte or record-by-record comparison of the exported files from the new GCP system against those produced by the legacy `r_exis_v2` script for the same input data and configuration.
    *   **Data Accuracy:** Verify that all data transformations, filtering, and aggregations are functionally identical.
    *   **Metadata:** Check that metadata files (`DESTINATION_RDY`) or their BigQuery equivalents contain the correct information.
    *   **External Distribution:** Confirm that files are successfully delivered to external SFTP/SCP endpoints and email notifications are sent as expected.
5.  **Performance Testing:**
    *   Run the migrated job with production-scale data volumes and compare execution times against the legacy system. Identify and optimize any performance bottlenecks.
6.  **Observability Testing:**
    *   Verify that all relevant logs appear in Cloud Logging and that metrics are visible in Cloud Monitoring.
    *   Test error scenarios (e.g., invalid configuration, source data issues, network failures) to ensure proper error logging, alerting, and status updates in `exporter_log` and `exporter_status`.

### What "Passing" Means:

A successful migration is defined by the following criteria:

*   **Functional Equivalence:** The migrated job produces identical output data (content, format, naming) to the legacy `r_exis_v2` script for the same input and configuration.
*   **Data Accuracy:** All data transformations, filtering, and aggregations are correctly applied, resulting in accurate data.
*   **Performance:** The migrated job meets or exceeds the performance characteristics (execution time, resource utilization) of the legacy system.
*   **Reliability:** The job completes successfully without unexpected failures, handles anticipated errors gracefully, and recovers from transient issues.
*   **Observability:** All job executions are fully logged in Cloud Logging, key metrics are available in Cloud Monitoring, and job status is accurately reflected in BigQuery `exporter_status` and `exporter_log` tables.
*   **Security:** The solution adheres to GCP security best practices, utilizing IAM, Secret Manager, and least-privilege principles.
*   **Maintainability:** The code is well-structured, documented, and easily understandable by the development team.

## 7. Rollback Procedure

In the event of critical issues post-go-live that cannot be immediately resolved, the following rollback procedure can be initiated to revert to the legacy `r_exis_v2` system:

1.  **Stop New GCP Job:**
    *   Immediately **pause or delete the `r_exis_v2_dag`** in Cloud Composer to prevent any further execution of the migrated job.
    *   If any Dataflow jobs are running, cancel them.
    *   If any Cloud Run services were triggered and are still processing, monitor their completion or manually intervene if necessary (e.g., for long-running SFTP transfers).

2.  **Re-enable Legacy Job:**
    *   **Re-enable and re-schedule the original `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2` KornShell script** in its legacy environment.
    *   Verify that the legacy job starts and runs as expected.

3.  **Data Consistency Check (Output Files):**
    *   **Assess the state of output files** generated by the new GCP system.
    *   If the new system generated files that were already distributed to external systems, coordinate with recipients to either:
        *   Inform them of a potential re-delivery from the legacy system.
        *   If the data is identical, confirm they can continue using the GCP-generated files.
    *   If the new system generated files that were *not* yet distributed, consider deleting them from GCS to avoid confusion or accidental distribution later.
    *   The `exporter_distribution_queue` in BigQuery can be used to identify pending or failed distributions from the new system.

4.  **Configuration Reversion:**
    *   Ensure that any external systems or downstream consumers that were configured to receive data from the new GCP system are reverted to consume from the legacy system.

5.  **Monitor Legacy System:**
    *   Closely monitor the re-enabled legacy `r_exis_v2` script to ensure it is functioning correctly and producing the expected output.

6.  **Post-Rollback Analysis:**
    *   Once the legacy system is stable, conduct a thorough root cause analysis of the issues that necessitated the rollback.
    *   Address the identified issues in the GCP migration, re-test, and plan for a subsequent re-deployment.