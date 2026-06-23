# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the Oracle batch ETL job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh`.

**What was migrated:**
The original job is a KornShell-orchestrated Oracle SQL*Plus script (`d_ausd_v_ta_apn_ve.sql`) that extracts data from several Oracle source tables (`dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`), applies date-based filtering and join logic, and performs a full truncate-and-reload into the Oracle target table `sof$ta_apn_ve`.

**To which target platform:**
The job has been migrated to Google Cloud Platform (GCP), utilizing:
*   **Cloud Composer (Apache Airflow)** for orchestration.
*   **BigQuery** for data storage (staging and target) and transformation logic.
*   **Cloud Logging** for centralized logging.
*   **Cloud Monitoring** for alerting and operational oversight.
*   **Secret Manager** for secure credential management.

## 2. Generated Artifacts

The migration process results in the following key artifacts:

*   **`dags/r_ausd_v_ta_apn_ve_dag.py`**: An Apache Airflow DAG Python script responsible for orchestrating the entire workflow, including source data ingestion triggers (if applicable), watermark derivation, and BigQuery transformation execution.
*   **`sql/d_ausd_v_ta_apn_ve_bq.sql`**: A BigQuery Standard SQL script containing the rewritten transformation logic. This script performs the `TRUNCATE TABLE` and `INSERT INTO ... SELECT ...` operations on the BigQuery target table `project.dataset.sof_ta_apn_ve`.
*   **`ddl/sof_ta_apn_ve.sql`**: (Optional, but recommended) A BigQuery DDL script to define the schema for the target table `project.dataset.sof_ta_apn_ve`.
*   **Source Ingestion Configuration**: Configuration files or scripts for the chosen data ingestion method (e.g., Datastream stream configuration, Data Fusion pipeline definition, custom JDBC ingestion script) to bring Oracle source data into BigQuery staging tables.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Cloud Composer for Orchestration**: Replaced the original KornShell scripts (`r_ausd_v_ta_apn_ve.ksh`, `k_ausd_v_ta_apn_ve.ksh`) with an Airflow DAG. This provides robust scheduling, dependency management, retry mechanisms, and integrated monitoring capabilities inherent to Airflow, surpassing the capabilities of shell scripting.
*   **BigQuery for Transformation**: The core transformation logic, originally in Oracle SQL*Plus (`d_ausd_v_ta_apn_ve.sql`), was rewritten into BigQuery Standard SQL. This leverages BigQuery's serverless, scalable, and high-performance analytics engine, optimizing for large-scale data processing.
*   **BigQuery Staging Tables**: To decouple the transformation from the Oracle source system, all required source tables are first ingested into BigQuery staging tables. This allows the BigQuery transformation to operate entirely within BigQuery, simplifying the SQL and improving performance.
*   **Full Table Truncate/Reload Strategy**: The original job's behavior of truncating and fully reloading the target table (`sof$ta_apn_ve`) was maintained in BigQuery. This simplifies the migration by preserving the existing data refresh pattern, assuming BigQuery's performance for this operation is acceptable for the data volume.
*   **Secret Manager for Credentials**: Oracle database connection details and other sensitive information, previously managed via environment variables or configuration files, are now securely stored and accessed via Google Cloud Secret Manager. This enhances security and centralizes credential management.
*   **Cloud Logging and Monitoring Integration**: Replaced custom shell-based logging and error trapping with native Cloud Logging for structured logs and Cloud Monitoring for alerts. This provides a unified observability platform for the entire GCP environment.
*   **Direct BigQuery DDL for `TRUNCATE`**: The `DWPA_UTIL_SKRIPT.runstatement` call for `TRUNCATE TABLE` was replaced with a direct `TRUNCATE TABLE` statement in BigQuery SQL, simplifying the transformation logic and removing an external dependency.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and Folder Setup**: Ensure the dedicated GCP project and appropriate folder structure are established.
2.  **BigQuery Dataset Creation**: Create the necessary BigQuery datasets (e.g., `project.raw`, `project.staging`, `project.data_warehouse`) to host the source, staging, and target tables.
3.  **Cloud Composer Environment Provisioning**: Deploy and configure a Cloud Composer environment (Airflow version compatible with the DAG).
4.  **Secret Manager Configuration**:
    *   Create secrets in Google Cloud Secret Manager for Oracle source database credentials (username, password, connection string/JDBC URL).
    *   Ensure the Composer service account has `Secret Manager Secret Accessor` role for these secrets.
5.  **IAM Roles and Permissions**:
    *   Grant the Cloud Composer service account the necessary BigQuery roles (e.g., `BigQuery Data Editor` for target tables, `BigQuery Data Viewer` for staging tables) and any other required permissions (e.g., `Secret Manager Secret Accessor`).
    *   Ensure the service account used for source ingestion has appropriate permissions to read from Oracle and write to BigQuery.
6.  **Source Data Ingestion Setup**:
    *   Implement and configure the chosen data ingestion method (e.g., Datastream, Data Fusion, custom JDBC-based tool) to continuously or periodically replicate the Oracle source tables (`dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`) into their respective BigQuery staging tables.
    *   Validate that data is flowing correctly and schemas are mapped appropriately.
7.  **Target Table DDL Execution**: Execute the DDL script (`ddl/sof_ta_apn_ve.sql`) to create the target BigQuery table `project.dataset.sof_ta_apn_ve` with the correct schema.
8.  **Airflow Connections**: Configure any necessary Airflow connections (e.g., `google_cloud_default` or custom BigQuery connections) within the Composer environment.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps, risks, or areas requiring further investigation:

*   **Oracle DB-link `v_carmen` Behavior**: The exact functionality and performance characteristics of the `v_carmen` DB-link in the original Oracle environment need to be thoroughly understood. This is crucial to ensure that the chosen BigQuery data ingestion method accurately replicates its behavior, especially concerning data freshness and consistency.
*   **`DWPA_UTIL_SKRIPT` Full Functionality**: While `runstatement` for `TRUNCATE TABLE` was addressed, the full scope of the `isbert_schema.DWPA_UTIL_SKRIPT` Oracle package is not fully known. If it contains other complex business logic beyond simple DDL execution, additional reverse engineering and migration effort will be required.
*   **Oracle Date Semantics**: Differences in date/timestamp handling, time zones, and implicit conversions between Oracle and BigQuery can lead to subtle data discrepancies. Careful validation is needed to ensure identical date logic.
*   **Data Type Mismatches**: Potential for data type incompatibilities during the ingestion of Oracle data into BigQuery staging tables. This requires thorough schema mapping and validation.
*   **Hidden Dependencies in KornShell**: The original KornShell scripts might have undocumented dependencies on other shell utilities, environment variables, or internal scripts not fully captured in the design document. These could manifest as runtime errors in the Airflow environment.
*   **Logging Semantics Replication**: Exact replication of `DWMSG_` logging messages and their downstream consumption (if any) may be challenging. The focus is on functional equivalence with Cloud Logging.
*   **Downstream Impacts of Refresh Timing**: Changes in the job's refresh timing or data availability due to migration might affect other systems that rely on the `sof$ta_apn_ve` table. Communication and coordination with downstream consumers are essential.

## 6. Validation

Validation of the migrated job involves several stages to ensure data integrity, performance, and operational stability:

1.  **Unit Testing (BigQuery SQL)**:
    *   Run the `d_ausd_v_ta_apn_ve_bq.sql` script independently against representative sample data in BigQuery staging tables.
    *   Verify the correctness of the transformation logic, join conditions, and date filters.
    *   **Passing Criteria**: The output data matches expected results based on the sample input.

2.  **Integration Testing (Airflow DAG)**:
    *   Deploy the `r_ausd_v_ta_apn_ve_dag.py` to the Cloud Composer environment.
    *   Trigger the DAG manually and observe its execution flow, task dependencies, and error handling.
    *   Ensure all tasks complete successfully and logs are generated in Cloud Logging.
    *   **Passing Criteria**: The DAG runs without errors, all tasks succeed, and the BigQuery job is triggered and completes.

3.  **Data Validation and Reconciliation**:
    *   Perform parallel runs: Execute the legacy Oracle job and the new GCP job concurrently for a defined period (e.g., several days to a week).
    *   Compare the output of the `sof$ta_apn_ve` table in Oracle with `project.dataset.sof_ta_apn_ve` in BigQuery.
    *   **Key Checks**:
        *   **Row Counts**: Total row count in both target tables should match for the same execution date.
        *   **Checksums/Hashes**: Compute checksums or hashes of key columns or entire rows for a representative sample to detect subtle data differences.
        *   **Sample Data Comparison**: Manually compare a random sample of records from both target tables.
        *   **Data Type and Format**: Verify that data types and formats are consistent.
    *   **Passing Criteria**: All data reconciliation checks show identical results between the Oracle and BigQuery target tables.

4.  **Performance Testing**:
    *   Monitor the execution time of the BigQuery transformation job.
    *   Compare it against the historical runtime of the Oracle job.
    *   **Passing Criteria**: The BigQuery job completes within the defined SLA, ideally matching or improving upon the legacy job's performance.

5.  **Error Handling and Alerting Testing**:
    *   Introduce controlled errors (e.g., invalid SQL, missing data, permission issues) to specific tasks within the Airflow DAG.
    *   Verify that Airflow correctly catches the errors, retries (if configured), and triggers the expected alerts via Cloud Monitoring.
    *   **Passing Criteria**: Errors are caught, appropriate alerts are sent, and the DAG's failure state is correctly reflected.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Reversion to Legacy System**:
    *   If issues are detected with the migrated job (e.g., data quality issues, performance degradation, critical failures), immediately disable or pause the Airflow DAG in Cloud Composer.
    *   Ensure the original Oracle batch job (`r_ausd_v_ta_apn_ve.ksh`) is re-enabled and scheduled to run as per its original schedule. This will ensure the `sof$ta_apn_ve` table in Oracle continues to be populated and downstream systems are not impacted.

2.  **Data State Management**:
    *   The BigQuery target table `project.dataset.sof_ta_apn_ve` can be left as is, truncated, or ignored. Its state will not affect the legacy system's operation.
    *   Downstream consumers relying on the BigQuery table should be temporarily switched back to consuming from the Oracle `sof$ta_apn_ve` table, if they were already migrated.

3.  **Root Cause Analysis**:
    *   Thoroughly investigate the root cause of the failure in the GCP environment using Cloud Logging, Cloud Monitoring, and Airflow logs.
    *   Identify and resolve the underlying problem (e.g., code bug, configuration error, infrastructure issue).

4.  **Re-deployment and Re-testing**:
    *   Once the issue is resolved, re-deploy the corrected Airflow DAG and BigQuery SQL scripts.
    *   Perform a targeted re-test of the specific functionality that failed.

5.  **Re-attempt Cutover**:
    *   After successful re-testing, re-initiate the cutover process, potentially with another period of parallel runs for increased confidence.

**Note**: Maintaining the ability to run both the legacy and migrated systems in parallel for a defined period post-cutover is highly recommended to facilitate a quick and seamless rollback if necessary.