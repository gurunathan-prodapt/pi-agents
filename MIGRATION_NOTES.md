# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2`. This script served as a unified exporter framework within the Data Warehouse (DWH) environment, handling data export for general requirements and cube-specific needs. Its functionality included configuration parsing, timestamp resolution, initial/incremental runs, SQL execution against Oracle, partitioned/parallel file creation, ready-file generation, post-processing, and distribution via SCP, SFTP, email, compression, move, copy, and delete operations.

The migration re-implements this functionality on **Google Cloud Platform (GCP)**, leveraging cloud-native services for enhanced scalability, reliability, and maintainability. The target platform utilizes:
*   **BigQuery**: For core data processing, storage, and orchestration via Stored Procedures.
*   **Cloud Storage**: For file staging, storage, and management.
*   **Cloud Functions**: For handling external interactions (SFTP, email) and specific file operations.
*   **Cloud Composer/Airflow**: For job scheduling and orchestration.

## 2. Generated Artifacts

The migration produced the following artifacts:

### BigQuery DDL (Data Definition Language)
*   **`bigquery/ddl/dwh_exporter_dataset.sql`**:
    *   **Role**: Defines and creates the `dwh_exporter` BigQuery dataset, which will house all tables and stored procedures related to this migration.
*   **`bigquery/ddl/dwh_exporter_config_kv.sql`**:
    *   **Role**: Creates the `config_kv` BigQuery table. This table stores job-specific and default configuration parameters, replacing the legacy external configuration files (`p_ConfigFile`, `p_DefaultFile`).
*   **`bigquery/ddl/dwh_exporter_job_history.sql`**:
    *   **Role**: Creates the `job_history` BigQuery table. This table tracks overall job execution metadata, status, and parameters, serving as the equivalent of the legacy `DWH$TA_K_MELDUNGEN` table.
*   **`bigquery/ddl/dwh_exporter_export_audit.sql`**:
    *   **Role**: Creates the `export_audit` BigQuery table. This table stores detailed audit information for individual steps within an export job, providing granular logging and traceability.
*   **`bigquery/ddl/dwh_exporter_export_distribution.sql`**:
    *   **Role**: Creates the `export_distribution` BigQuery table. This table stores rules and instructions for how exported files should be distributed (e.g., via SFTP, email, GCS operations).
*   **`bigquery/ddl/dwh_exporter_export_readyfiles.sql`**:
    *   **Role**: Creates the `export_readyfiles` BigQuery table. This table manages metadata for generated output files and their corresponding "ready" files, tracking their status and location.

### BigQuery Stored Procedures and UDFs (User-Defined Functions)
*   **`bigquery/stored_procedures/dwh_exporter_resolve_timestamp.sql`**:
    *   **Role**: A BigQuery UDF designed to resolve timestamp strings into `TIMESTAMP` data types, mimicking the date arithmetic and resolution logic found in `h_alis_date.ksh`.
*   **`bigquery/stored_procedures/dwh_exporter_get_config_value.sql`**:
    *   **Role**: A BigQuery Stored Procedure to retrieve a specific configuration value from the `dwh_exporter.config_kv` table based on job name and key.
*   **`bigquery/stored_procedures/dwh_exporter_log_audit.sql`**:
    *   **Role**: A BigQuery Stored Procedure to insert audit messages into the `dwh_exporter.export_audit` table, centralizing logging for job steps.
*   **`bigquery/stored_procedures/dwh_exporter_r_exis_v2.sql`**:
    *   **Role**: The main BigQuery Stored Procedure that orchestrates the entire export process. It encapsulates the core logic of the original `r_exis_v2` script, handling configuration loading, timestamp resolution, execution planning, and calling sub-procedures for data export and distribution.

### Python Cloud Functions
*   **`python/cloud_functions/parse_config_files.py`**:
    *   **Role**: A Cloud Function designed to parse legacy configuration files (e.g., `.cfg`, `.conf`) uploaded to Cloud Storage. It extracts key-value pairs and loads them into the `dwh_exporter.config_kv` BigQuery table.
*   **`python/cloud_functions/distribute_file_sftp.py`**:
    *   **Role**: A Cloud Function triggered by new files in a Cloud Storage bucket. It queries `export_distribution` for SFTP rules and transfers the specified file to an external SFTP server, replacing the legacy `sftp` and `scp` commands.
*   **`python/cloud_functions/distribute_file_email.py`**:
    *   **Role**: A Cloud Function triggered by new files in a Cloud Storage bucket. It queries `export_distribution` for email rules and sends the file as an email attachment, replacing the legacy `mailx` and `uuencode` functionality.
*   **`python/cloud_functions/gcs_file_operations.py`**:
    *   **Role**: A Cloud Function designed to perform various Cloud Storage file operations (move, copy, delete, compress/decompress). It can be triggered by GCS events or explicit invocation, replacing shell commands like `mv`, `cp`, `rm`, `compress`.

## 3. Key Design Decisions

The migration strategy involved several key design decisions to transition from a KornShell-based, Oracle-centric environment to a cloud-native GCP architecture:

*   **BigQuery Stored Procedures for Core Orchestration**:
    *   **Why**: The complex procedural logic of the KornShell script, including parameter handling, conditional branching, and looping, was re-implemented using BigQuery Stored Procedures. This leverages BigQuery's native capabilities for scalable data processing, transactional integrity for job state management, and keeps the core logic close to the data.
    *   **Trade-offs**: Direct translation of highly dynamic shell features (e.g., `eval`, `mkfifo`, complex string manipulation with `nawk`/`sed`) is challenging. These required re-architecting using BigQuery SQL functions, temporary tables, or external services like Cloud Pub/Sub for inter-process communication.
*   **Cloud Storage as the Central File System**:
    *   **Why**: All local filesystem operations (for config files, temporary files, and final output) were replaced with Cloud Storage. This provides a highly scalable, durable, and cost-effective object storage solution that integrates seamlessly with other GCP services and enables event-driven architectures.
    *   **Trade-offs**: Requires a complete re-design of file paths and operations. Direct shell commands like `mv`, `cp`, `rm` are replaced by Cloud Storage API calls, often orchestrated by Cloud Functions or Cloud Workflows.
*   **Cloud Functions for External Interactions and File Operations**:
    *   **Why**: External dependencies such as `scp`, `sftp`, `mailx`, and file compression/manipulation utilities were replaced by serverless Cloud Functions. This approach is cost-effective for intermittent tasks, provides a managed execution environment, and simplifies integration with external APIs.
    *   **Trade-offs**: Introduces potential latency for file transfers to external systems. Requires careful management of credentials (e.g., SFTP private keys, SMTP passwords) using Secret Manager and secure network configurations. Each function needs separate deployment and monitoring.
*   **BigQuery Tables for Configuration and Job State Management**:
    *   **Why**: Legacy flat configuration files and Oracle-based job tracking tables (`DWH$TA_K_MELDUNGEN`) were migrated to dedicated BigQuery tables (`config_kv`, `job_history`, `export_audit`, `export_distribution`, `export_readyfiles`). This centralizes configuration, makes it queryable, enables dynamic updates without code changes, and leverages BigQuery's scalability for metadata management.
    *   **Trade-offs**: Requires an initial data migration effort to populate these tables and introduces a BigQuery dependency for configuration retrieval.
*   **BigQuery Data Transfer Service / Federated Queries for Oracle Data Ingestion**:
    *   **Why**: The primary data source, Oracle, is integrated with BigQuery using standard GCP methods. BigQuery Data Transfer Service is used for scheduled, batch ingestion of Oracle tables, while Federated Queries can provide ad-hoc, near real-time access if required. This facilitates the transition from `sqlplus` calls to native BigQuery SQL.
    *   **Trade-offs**: Data Transfer Service is batch-oriented, which might require adjustments for real-time needs. Federated Queries can have performance implications for very large datasets or complex joins. All Oracle SQL queries must be translated to BigQuery SQL dialect.

## 4. Manual Steps Before Go-Live

Before the migrated `r_exis_v2` solution can go live, several manual setup and configuration steps are required:

1.  **BigQuery Dataset Creation**:
    *   Execute `bigquery/ddl/dwh_exporter_dataset.sql` to create the `dwh_exporter` dataset in your target GCP project and region.
    *   Execute all other DDL scripts (`dwh_exporter_config_kv.sql`, `dwh_exporter_job_history.sql`, etc.) to create the necessary tables within the `dwh_exporter` dataset.
    *   Deploy all BigQuery Stored Procedures and UDFs (`dwh_exporter_resolve_timestamp.sql`, `dwh_exporter_get_config_value.sql`, `dwh_exporter_log_audit.sql`, `dwh_exporter_r_exis_v2.sql`) to the `dwh_exporter` dataset.

2.  **IAM & Permissions**:
    *   **BigQuery Service Account**: Create a dedicated service account for BigQuery operations. Grant it `BigQuery Data Editor` on `dwh_exporter` dataset, `BigQuery Job User`, and `BigQuery Data Viewer` on source data tables (if federated queries are used).
    *   **Cloud Functions Service Accounts**: Create separate service accounts for each Cloud Function (e.g., `sftp-distributor-sa`, `email-distributor-sa`, `gcs-ops-sa`). Grant them:
        *   `Cloud Functions Invoker` (for internal triggers).
        *   `Cloud Storage Object Admin` on relevant buckets.
        *   `BigQuery Data Editor` on `dwh_exporter` tables (`export_distribution`, `export_readyfiles`).
        *   `Secret Manager Secret Accessor` for accessing credentials.
        *   `Cloud Logging Log Writer`.
        *   Network access roles if connecting to external systems (e.g., `roles/compute.networkUser` for VPC access).
    *   **Cloud Composer/Airflow Service Account**: Ensure the Airflow service account has permissions to invoke BigQuery Stored Procedures (`BigQuery Job User`) and monitor BigQuery jobs.

3.  **Connection Strings & Secrets Management**:
    *   **SFTP Credentials**: Store SFTP host, username, and the private key (or its path) securely in Google Secret Manager. The `distribute_file_sftp.py` Cloud Function will retrieve these.
    *   **SMTP Credentials**: Store SMTP server, port, username, and password securely in Google Secret Manager. The `distribute_file_email.py` Cloud Function will retrieve these.
    *   **Oracle Connection Details**: Configure BigQuery Data Transfer Service jobs with the necessary Oracle connection details (hostname, port, service name/SID, username, password). If using federated queries, ensure appropriate external connection objects are created in BigQuery.

4.  **Configuration Data Ingestion**:
    *   Upload all legacy configuration files (`p_ConfigFile`, `p_DefaultFile`, and any other job-specific config files) to a designated Cloud Storage bucket (e.g., `gs://your-project-config-bucket/legacy_configs/`).
    *   Trigger the `parse_config_files.py` Cloud Function (either manually or via a GCS event trigger) to parse these files and populate the `dwh_exporter.config_kv` BigQuery table.
    *   Manually populate the `dwh_exporter.export_distribution` table with the necessary rules for file distribution (SFTP, Email, GCS operations) for each job. This defines where and how exported files should be sent.

5.  **Cloud Storage Buckets**:
    *   Create dedicated Cloud Storage buckets for:
        *   **Source Data**: If raw Oracle data is staged before BigQuery ingestion.
        *   **Exported Files**: Where BigQuery writes its output (e.g., `gs://your-project-exports/`).
        *   **Archive**: For long-term storage of exported files after distribution.
        *   **Temporary Files**: For Cloud Functions to use during processing (e.g., `/tmp` directory in Cloud Functions).

6.  **Scheduling**:
    *   Develop and deploy Cloud Composer/Airflow DAGs to orchestrate the execution of the main `dwh_exporter.r_exis_v2` BigQuery Stored Procedure. The DAG should pass necessary job parameters (e.g., `p_job_name`, `p_run_id`, `p_parameters`).
    *   Configure GCS event triggers for the Cloud Functions (`distribute_file_sftp.py`, `distribute_file_email.py`, `gcs_file_operations.py`) to activate when new files are created in the designated export bucket.

7.  **Oracle Data Transfer**:
    *   Set up and configure BigQuery Data Transfer Service jobs to regularly ingest the required source Oracle tables into BigQuery. Ensure the target BigQuery tables are correctly defined and partitioned.
    *   Translate all Oracle SQL queries used in the legacy script's `OUTPUT_SQL`, `PRE_SQL`, `POST_SQL` nodes to BigQuery SQL dialect.

8.  **Network Configuration**:
    *   If Cloud Functions need to connect to private networks or external systems (e.g., on-prem SFTP servers, private SMTP relays), configure VPC Service Controls, Serverless VPC Access, or appropriate firewall rules.

## 5. Known Gaps & Unresolved References

The migration design document highlighted several areas of complexity and potential risks that require ongoing attention or further refinement:

*   **Complexity of Shell Script Logic Translation**:
    *   The original script's extensive use of `nawk`, `sed`, `grep`, `cut`, `expr`, `mkfifo`, and `eval` for dynamic string manipulation, data processing, and inter-process communication is highly complex. While BigQuery SQL functions and Python in Cloud Functions cover most string/data processing, the exact replication of `mkfifo`-based pipe logic and `eval` for dynamic command execution requires careful re-engineering, potentially using Cloud Pub/Sub or Cloud Workflows for complex inter-step communication.
*   **Dynamic SQL Generation**:
    *   The legacy script dynamically constructs SQL queries based on configuration. In BigQuery, this needs to be re-engineered using parameterized queries within stored procedures or careful string manipulation for dynamic query construction, strictly adhering to BigQuery's SQL dialect and security best practices.
*   **Parallel Execution Orchestration**:
    *   The shell-based `merger` function and `PARALLELFILE`/`PARALLELSQL` processing logic need a cloud-native parallel execution model. While BigQuery offers inherent parallelism, complex inter-process communication patterns (like `mkfifo` and `controlledpipeelem`) will require different solutions, such as Cloud Pub/Sub for message passing, Cloud Storage object notifications, or Cloud Workflows for explicit task orchestration.
*   **External System Integration Details**:
    *   While Cloud Functions replace `SCP`, `SFTP`, and `Mail`, the exact parameters, authentication mechanisms, and network configurations for each external system need thorough analysis. The `EXT:DATABASE` reference in the original script is vague and requires further investigation to identify the specific Oracle database and its connection requirements.
*   **Configuration Management Robustness**:
    *   The full translation of `parser_getnode`, `parser_filattrib`, `parser_filfile` logic for reading and substituting configuration attributes needs to be robustly implemented within BigQuery Stored Procedures or helper UDFs. This includes handling nested configurations and complex attribute resolution.
*   **Error Handling and Traps**:
    *   The shell `trap` mechanism and custom error messaging (`DWMSG_*`) need to be fully adapted to BigQuery's `EXCEPTION` blocks for error handling within stored procedures and integrated with Cloud Logging and Cloud Monitoring for alerts and incident management.
*   **Environment Variables (`DW_DIR_ROOT`, `HOME`)**:
    *   These environment-specific paths in the legacy script must be replaced by Cloud Storage paths or passed as parameters/environment variables managed by the orchestration layer (e.g., Cloud Composer).
*   **Oracle Metadata Tables**:
    *   The script's reliance on Oracle metadata tables (`dwh$ta_k_meldungen`, `all_tables`, `all_synonyms`, `dba_tab_partitions`) needs to be addressed. `dwh$ta_k_meldungen` is replaced by `dwh_exporter.job_history`. Other metadata queries will either need to be translated to BigQuery's `INFORMATION_SCHEMA` or require specific BigQuery tables to store relevant Oracle metadata if direct access is not feasible or performant.
*   **`dwh_exporter.resolve_timestamp` UDF**:
    *   The current UDF provides a simplified timestamp resolution. Full replication of the legacy shell script's complex date arithmetic (e.g., `DATE_ADD`, `DATE_SUB` with various units and formats) might require more sophisticated parsing logic within the UDF or dynamic SQL execution within stored procedures.

## 6. Validation

Validation of the migrated `r_exis_v2` solution involves a multi-faceted approach to ensure functional correctness, data integrity, and performance.

### How to Run Tests:

1.  **Unit Tests**:
    *   **BigQuery Stored Procedures/UDFs**: Execute each BigQuery Stored Procedure and UDF individually using the BigQuery console or `bq query` command-line tool. Provide various input parameters (including edge cases) and verify the output, return values, and updates to `job_history` and `export_audit` tables.
    *   **Cloud Functions**: Deploy each Cloud Function (`parse_config_files.py`, `distribute_file_sftp.py`, `distribute_file_email.py`, `gcs_file_operations.py`). Trigger them manually using sample Cloud Storage events or Pub/Sub messages. Monitor Cloud Logging for execution details and verify the expected actions (e.g., file transfer, email sent, BigQuery table updates).

2.  **Integration Tests (End-to-End)**:
    *   **Orchestration**: Trigger the main `dwh_exporter.r_exis_v2` BigQuery Stored Procedure via a Cloud Composer/Airflow DAG. This simulates the production workflow.
    *   **Data Flow**:
        *   Ensure source Oracle data is correctly ingested into BigQuery (if applicable).
        *   Verify that the BigQuery Stored Procedure processes data, generates output, and writes files to the designated Cloud Storage export bucket.
        *   Confirm that Cloud Functions are triggered by the new files in Cloud Storage and successfully perform distribution (SFTP, Email) and GCS operations (move, archive).
    *   **State Tracking**: Monitor the `dwh_exporter.job_history`, `dwh_exporter.export_audit`, and `dwh_exporter.export_readyfiles` tables for accurate and complete status updates throughout the job lifecycle.

3.  **Data Validation**:
    *   **Comparison with Legacy Output**: For a representative set of export jobs, run both the legacy KornShell script and the new GCP solution in parallel.
        *   **Row Counts**: Compare the number of records in the exported files.
        *   **Checksums/Hashes**: Compute checksums (e.g., MD5) for the exported files to ensure byte-for-byte identical content (if transformations are identical).
        *   **Sample Data Comparison**: Perform detailed comparisons of sample records to identify any discrepancies in data values, formatting, or encoding.
        *   **Schema Validation**: Verify that the schema of the exported files matches the expected schema.

4.  **Performance Testing**:
    *   Run the new GCP solution with production-like data volumes and concurrency.
    *   Compare execution times and resource consumption (BigQuery slots, Cloud Function invocations, Cloud Storage operations) against the legacy script's performance metrics.
    *   Identify any performance bottlenecks and optimize BigQuery queries or Cloud Function logic.

### What "Passing" Means:

A successful migration validation implies the following criteria are met:

*   **Functional Equivalence**: The new GCP solution produces the same output files with identical content and formatting as the legacy `r_exis_v2` script for all tested scenarios.
*   **Data Integrity**: All data integrity checks (row counts, checksums, sample data comparisons) pass, confirming no data loss or corruption during migration.
*   **Successful Execution**:
    *   The main `dwh_exporter.r_exis_v2` BigQuery Stored Procedure completes with `status = 'SUCCESS'` in `dwh_exporter.job_history`.
    *   All sub-steps logged in `dwh_exporter.export_audit` show `status = 'COMPLETED'` or `status = 'SUCCESS'`.
    *   All Cloud Functions execute without errors and perform their intended actions (e.g., SFTP transfers complete, emails are sent, GCS files are moved/copied/deleted).
    *   `dwh_exporter.export_readyfiles` accurately reflects the status and location of all generated and distributed files.
*   **Performance**: The new solution meets or exceeds the performance of the legacy script, or operates within acceptable service level objectives (SLOs) for execution time and resource utilization.
*   **Observability**: Logging (Cloud Logging) and monitoring (Cloud Monitoring) are correctly configured, providing clear insights into job status, errors, and performance metrics.

## 7. Rollback Procedure

In the event of critical failures or unacceptable performance after go-live, the following rollback procedure should be initiated:

1.  **Immediate Halt of New System**:
    *   **Stop Orchestration**: Immediately pause or disable the Cloud Composer/Airflow DAG responsible for triggering the `dwh_exporter.r_exis_v2` BigQuery Stored Procedure. This prevents any further execution of the new system.
    *   **Disable Cloud Functions**: Temporarily disable or un-deploy the Cloud Functions (`distribute_file_sftp.py`, `distribute_file_email.py`, `gcs_file_operations.py`) to prevent them from processing any residual or erroneous files.

2.  **Re-enable Legacy System**:
    *   **Restore Scheduling**: Re-enable the original scheduling mechanism (e.g., cron, Autosys, UC4) for the legacy KornShell script `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2`.
    *   **Verify Legacy Operation**: Confirm that the legacy script is running as expected and producing correct output to its original destinations.

3.  **Data Consistency and Cleanup**:
    *   **Downstream Coordination**: If the new GCP solution successfully wrote data to external systems (via SFTP, email, etc.) before the rollback, coordinate immediately with downstream consumers to:
        *   Identify and quarantine any potentially incorrect or duplicate data.
        *   Determine if a re-processing of the affected period using the legacy system is required.
        *   Communicate the rollback and its implications.
    *   **Cloud Storage Review**: Inspect the Cloud Storage buckets used by the new system (e.g., `gs://your-project-exports/`) for any partially created, incorrect, or duplicate files. Delete or move these to an "error" or "quarantine" bucket to prevent interference with future runs or re-deployments.
    *   **BigQuery State**: The `dwh_exporter.job_history`, `dwh_exporter.export_audit`, and `dwh_exporter.export_readyfiles` tables can be retained for post-mortem analysis of the failure. No immediate rollback of these tables is typically required unless their schema changes conflict with a future re-deployment.
    *   **Oracle Data Transfer**: If BigQuery Data Transfer Service jobs were configured to ingest data from Oracle, consider pausing them if the data is not immediately needed by other BigQuery processes, to reduce costs.

4.  **Root Cause Analysis**:
    *   Once the legacy system is fully operational and data flow is restored, conduct a thorough root cause analysis of the failure in the GCP environment. Utilize Cloud Logging, Cloud Monitoring, and BigQuery audit tables to pinpoint the exact issue.
    *   Develop a remediation plan, including any necessary code changes, configuration adjustments, or architectural refinements.

5.  **Re-deployment (Post-Remediation)**:
    *   After implementing the remediation, follow the "Manual Steps Before Go-Live" and "Validation" procedures again to ensure the issue is resolved and the system is stable before attempting another go-live.