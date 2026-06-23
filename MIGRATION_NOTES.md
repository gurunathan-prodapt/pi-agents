# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job identified by `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh`. The original job, written in KornShell and Oracle SQL*Plus, extracts and prepares APN (Access Point Name) contract data for the BERT system, populating the `sof$ta_apn_carmen` table based on a reference date.

The job has been re-platformed to **Google Cloud Platform (GCP)**, utilizing **Google BigQuery** for data warehousing and transformations, and **Apache Airflow on Cloud Composer** for orchestration.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`ddl/isbert_schema.dwtk_meldungen.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `dwtk_meldungen` table. This table serves as a control table, previously residing in Oracle, used to determine the `v_datum` (control date) for the main transformation.
*   **`ddl/sof_ta_apn_carmen.sql`**
    *   **Role:** BigQuery DDL script to create the target table `sof$ta_apn_carmen`. This table will store the processed APN contract data in BigQuery.
*   **`ddl/pds_ta_pdp_context_assoc.sql`**
    *   **Role:** BigQuery DDL script to create the `pds$ta_pdp_context_assoc` table. This is one of the primary source tables, previously in Oracle, containing PDP context association information.
*   **`ddl/pds_ta_pdp_context.sql`**
    *   **Role:** BigQuery DDL script to create the `pds$ta_pdp_context` table. This is another primary source table, previously in Oracle, containing PDP context details.
*   **`ddl/pds_ta_access_point.sql`**
    *   **Role:** BigQuery DDL script to create the `pds$ta_access_point` table. This is the third primary source table, previously in Oracle, containing access point information.
*   **`sql/d_ausd_bp_ta_apn_carmen.sql`**
    *   **Role:** BigQuery Standard SQL script containing the core data transformation logic. This script replaces the original Oracle SQL*Plus script (`d_ausd_bp_ta_apn_carmen.sql`), performing data extraction, filtering, joining, and insertion into the `sof$ta_apn_carmen` BigQuery table. It also includes the logic for determining `v_datum` and truncating the target table.
*   **`dags/r_ausd_bp_ta_apn_carmen_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG orchestrates the entire job, replacing the original KornShell scripts (`r_ausd_bp_ta_apn_carmen.ksh`, `k_ausd_bp_ta_apn_carmen.ksh`, and associated utilities). It includes tasks for determining the reference date and executing the BigQuery SQL transformation.

## 3. Key Design Decisions

*   **Target Platform Selection (GCP):** Google Cloud Platform was chosen for its scalable, managed services. BigQuery provides a high-performance, cost-effective data warehouse, while Cloud Composer (managed Airflow) offers robust orchestration capabilities.
*   **Orchestration Re-platforming (Airflow):** The original KornShell scripts, including the main entry point and intermediate orchestration, have been replaced by a Python-based Airflow DAG. This decision leverages Airflow's native scheduling, dependency management, logging, monitoring, and error handling, which are more advanced and maintainable than custom shell scripting.
*   **Transformation Language (BigQuery Standard SQL):** The core data transformation logic, originally in Oracle SQL*Plus, has been converted to BigQuery Standard SQL. This ensures native execution within BigQuery, optimizing performance and reducing the need for external processing engines.
*   **External Data Source Handling (`@pcrs1`):** The Oracle database link `@pcrs1` for source tables (`pds$*`) is a critical external dependency. The primary design decision is to **replicate these Oracle tables into BigQuery** (e.g., via Cloud Datastream or a custom ETL process). This approach minimizes latency, improves reliability, and reduces direct dependency on the source Oracle system during job execution. Federated queries were considered but deemed less optimal for performance and reliability in this context.
*   **Utility Script Replacement:** All custom KornShell utility scripts (e.g., `gestern.ksh` for date calculation, `h_alis_sqlplus.ksh` for SQL execution, custom error/parameter handling) have been replaced. Date calculations are handled by Python's `pendulum` library within the Airflow DAG, and BigQuery's native date functions. Airflow's built-in operators and logging mechanisms replace custom shell wrappers and error handling.
*   **Target Table Preparation (BigQuery DDL):** The Oracle stored procedure call (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) used for truncating the target table has been replaced by a direct `TRUNCATE TABLE` statement within the BigQuery SQL script, which is the standard and most efficient way to clear a BigQuery table.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the environment and deploy the migrated job:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP project is available and configured.
    *   Create or identify the target BigQuery dataset (e.g., `your_dataset`) where the tables will reside. **Update `GCP_PROJECT_ID` and `BIGQUERY_DATASET` placeholders in the generated code accordingly.**
2.  **BigQuery Schema Creation:**
    *   Execute all DDL scripts (`ddl/*.sql`) in the target BigQuery dataset to create the necessary tables:
        *   `ddl/isbert_schema.dwtk_meldungen.sql`
        *   `ddl/sof_ta_apn_carmen.sql`
        *   `ddl/pds_ta_pdp_context_assoc.sql`
        *   `ddl/pds_ta_pdp_context.sql`
        *   `ddl/pds_ta_access_point.sql`
3.  **Source Data Ingestion/Replication:**
    *   Implement and configure a data replication solution (e.g., Google Cloud Datastream, Data Transfer Service, or a custom ETL pipeline) to continuously ingest data from the source Oracle tables (`pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`, and `isbert_schema.dwtk_meldungen`) into their respective BigQuery tables. This is crucial for the job to have up-to-date source data.
4.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Verify that the Airflow environment has the necessary Python packages installed (e.g., `apache-airflow-providers-google`).
5.  **Airflow DAG Deployment:**
    *   Upload the `dags/r_ausd_bp_ta_apn_carmen_dag.py` file to the DAGs folder of your Cloud Composer environment (typically a GCS bucket).
6.  **IAM Permissions:**
    *   Ensure the service account associated with your Cloud Composer environment has the necessary IAM roles for BigQuery:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on the target dataset (`your_dataset`) to create, truncate, and insert data.
        *   `BigQuery Data Viewer` on the source tables (if different datasets).
        *   `BigQuery Job User` to run BigQuery jobs.
7.  **Airflow Connections:**
    *   Verify the `google_cloud_default` Airflow connection is correctly configured to point to your GCP project.
8.  **Scheduling:**
    *   Review and adjust the `schedule` parameter in `r_ausd_bp_ta_apn_carmen_dag.py` to match the desired execution frequency (currently `timedelta(days=1)` for daily).
9.  **Secrets Management:**
    *   If any direct Oracle connectivity is chosen (e.g., for federated queries, though replication is preferred), ensure Oracle connection details are securely stored in Airflow Connections or Google Secret Manager.

## 5. Known Gaps & Unresolved References

*   **`pcrs1` External System Connectivity:** The exact nature and secure connectivity method for the `@pcrs1` Oracle instance from GCP needs thorough validation. While replication is the preferred strategy, the implementation details (e.g., network peering, VPN, Datastream configuration) are critical and require dedicated effort. The `v_carmen` variable in the BigQuery SQL is a placeholder and does not directly connect to an external system; its original purpose was to denote the database link.
*   **Custom Utility Re-implementation:** The custom KornShell utilities (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) have been replaced by standard Python/Airflow functionalities. While common functions like date calculations are straightforward, any highly specific or complex logic within these original utilities needs careful review to ensure equivalent behavior and robustness in their Python/Airflow counterparts.
*   **Missing Metadata for Utility Scripts:** The original utility scripts were not fully assessed by automated tools, implying a higher manual effort for their migration and validation.
*   **Data Types and Schema Mapping:** While initial DDLs are provided, a detailed, column-by-column review of data types, precision, scale, and nullability between the Oracle source tables and their BigQuery counterparts is recommended to prevent data loss or unexpected behavior.
*   **Performance Tuning:** The `WHENEVER SQLERROR` and `set timing on` directives from the original Oracle SQL*Plus script are replaced by Airflow's error handling and BigQuery's query statistics. Post-migration, the BigQuery job's performance (duration, slot usage, bytes processed) should be monitored and tuned using BigQuery's optimization features (e.g., partitioning, clustering, query rewrite).

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

*   **Unit Testing (BigQuery SQL):**
    *   **How to run:** Execute the `sql/d_ausd_bp_ta_apn_carmen.sql` script directly in the BigQuery console or via the `bq` command-line tool, using a small, representative set of test data in the source tables (`dwtk_meldungen`, `pds$*`).
    *   **Passing means:** The script executes without syntax errors, and the `sof$ta_apn_carmen` table is populated with the expected `CNTRCT_ID` and `ACCESS_POINT_NAME` values, matching the output of the original Oracle SQL for the same input data.
*   **Integration Testing (Airflow DAG):**
    *   **How to run:** Trigger the `r_ausd_bp_ta_apn_carmen_dag` manually in the Airflow UI (Cloud Composer).
    *   **Passing means:**
        *   The DAG runs successfully to completion without task failures.
        *   All tasks (e.g., `get_stichtag`, `execute_bigquery_sql`) complete with a "success" status.
        *   Airflow logs show correct parameter passing and BigQuery job execution details.
        *   The `sof$ta_apn_carmen` table in BigQuery is updated as expected.
*   **Data Validation:**
    *   **How to run:**
        1.  Execute the original Oracle job for a specific `Stichtag`.
        2.  Execute the migrated Airflow DAG for the *same* `Stichtag` (or equivalent date logic).
        3.  Compare the contents of the `sof$ta_apn_carmen` table in Oracle with the `sof$ta_apn_carmen` table in BigQuery.
    *   **Passing means:** The data in the BigQuery target table is identical to the data in the Oracle target table for the same processing date. This includes count of rows, specific column values, and data types.
*   **Performance Benchmarking:**
    *   **How to run:** Monitor the execution time and resource consumption (e.g., BigQuery slots, bytes processed) of the migrated job over several runs. Compare these metrics with the historical performance of the original Oracle job.
    *   **Passing means:** The BigQuery job completes within acceptable timeframes and resource limits, ideally showing performance improvements or cost efficiencies.
*   **Logging and Alerting:**
    *   **How to run:** Simulate failure scenarios (e.g., invalid data, missing source tables) and observe Airflow's logging and alerting mechanisms.
    *   **Passing means:** Airflow correctly logs errors, triggers configured alerts (if any), and provides sufficient information for troubleshooting.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed to revert to the legacy system:

1.  **Disable New Airflow DAG:**
    *   In the Airflow UI (Cloud Composer), toggle off the `r_ausd_bp_ta_apn_carmen_dag` to prevent further executions.
2.  **Re-enable Legacy Job:**
    *   Re-enable the original KornShell job (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh`) in its original scheduler (e.g., UC4).
3.  **Data Rollback (if necessary):**
    *   If the `sof$ta_apn_carmen` table in BigQuery is used by downstream systems and its data needs to be reverted to a pre-migration state, a backup of the table should have been taken before the migration. Restore the BigQuery `sof$ta_apn_carmen` table from this backup.
    *   Alternatively, if the BigQuery table is only a copy and the Oracle target table is the system of record, no data rollback is needed for BigQuery.
4.  **Monitor Legacy Job:**
    *   Verify that the re-enabled legacy job runs successfully and populates the Oracle `sof$ta_apn_carmen` table as expected.

This procedure ensures a quick return to the stable, legacy environment while issues with the migrated job are investigated and resolved.