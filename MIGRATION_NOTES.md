# MIGRATION_NOTES.md

## 1. Summary

The ETL job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL`, responsible for mirroring Carmen contract templates, has been migrated from its legacy environment to Google Cloud Platform (GCP).

**Legacy Environment:**
*   **Orchestration:** UC4/Automic job scheduler and KornShell control scripts (`r_ausd_v_ta_cntrct_templ.ksh`, `k_ausd_v_ta_cntrct_templ.ksh`).
*   **Transformation Logic:** Oracle SQL*Plus script (`d_ausd_v_ta_cntrct_templ.sql`) executing against an Oracle database.
*   **Source Systems:** Carmen database (via Oracle DB link) for `cds$ta_cntrct_template` and `cds$ta_care_description`, and an internal Oracle table `isbert_schema.dwtk_meldungen` for metadata.
*   **Target System:** Oracle table `sof$ta_cntrct_templ`.

**Target Platform (GCP):**
*   **Orchestration:** Apache Airflow (managed by Cloud Composer).
*   **Data Storage & Transformation:** Google BigQuery (Standard SQL).
*   **Data Staging:** BigQuery staging tables.
*   **Configuration & Secrets:** `config.yaml` and Google Secret Manager (for sensitive credentials).
*   **Monitoring & Logging:** Cloud Logging and Cloud Monitoring.

The migration preserves the existing business logic, execution order, data lineage, and output datasets, leveraging cloud-native capabilities for improved scalability, maintainability, and auditability.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dags/dw_bert_ausd_v_ta_cntrct_templ_dag.py`**
    *   **Role:** The main Airflow DAG definition file. It orchestrates the entire ETL process, defining tasks for configuration loading, processing date determination, data extraction from Carmen to BigQuery staging, BigQuery transformation, and updating control tables.
*   **`python/data_ingestion.py`**
    *   **Role:** A Python module containing placeholder functions (`extract_cds_ta_cntrct_template_to_bq`, `extract_cds_ta_care_description_to_bq`) for extracting data from the Carmen Oracle database and loading it into BigQuery staging tables. This module will require further implementation for robust Oracle connectivity and data transfer.
*   **`python/utils.py`**
    *   **Role:** A Python module providing utility functions that replace functionality from the legacy KornShell scripts. This includes loading YAML configurations, logging errors, determining the processing date (`v_datum`) from BigQuery, and a placeholder for parameter parsing and BigQuery SQL execution.
*   **`sql/d_ausd_v_ta_cntrct_templ_transformed.sql`**
    *   **Role:** The core BigQuery Standard SQL script. It translates the original Oracle SQL*Plus logic to BigQuery, performing the join, filtering, and insertion into the final curated table (`curated.final_fact_table`). It uses Airflow's Jinja templating for dynamic parameters like `gcp_project_id` and `v_datum`.
*   **`config/config.yaml`**
    *   **Role:** A centralized configuration file for the Airflow DAG and associated Python scripts. It defines GCP project IDs, BigQuery dataset names, Airflow connection IDs, target table names, and Airflow-specific settings like schedule and default arguments.
*   **`ddl/control_etl_job_run.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `control.etl_job_run` table, used for tracking job execution status and metadata.
*   **`ddl/control_etl_watermark.sql`**
    *   **Role:** BigQuery DDL script to create the `control.etl_watermark` table, used for managing incremental load watermarks.
*   **`ddl/metadata_dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL script to create the `metadata.dwtk_meldungen` table, which is the BigQuery equivalent of the legacy Oracle `isbert_schema.dwtk_meldungen` table.
*   **`ddl/staging_cds_ta_cntrct_template_stg.sql`**
    *   **Role:** BigQuery DDL script to create the `staging.cds_ta_cntrct_template_stg` table, serving as a staging area for data extracted from the Carmen `cds$ta_cntrct_template` Oracle table.
*   **`ddl/staging_cds_ta_care_description_stg.sql`**
    *   **Role:** BigQuery DDL script to create the `staging.cds_ta_care_description_stg` table, serving as a staging area for data extracted from the Carmen `cds$ta_care_description` Oracle table.
*   **`ddl/curated_final_fact_table.sql`**
    *   **Role:** BigQuery DDL script to create the `curated.final_fact_table` table, which is the final target table in BigQuery, replacing the legacy Oracle `sof$ta_cntrct_templ`.
*   **`ddl/audit_etl_validation_results.sql`**
    *   **Role:** BigQuery DDL script to create the `audit.etl_validation_results` table, intended for storing data quality validation outcomes.

## 3. Key Design Decisions

*   **Cloud-Native Orchestration with Apache Airflow (Cloud Composer)**
    *   **Why:** Replaces the legacy UC4 and KornShell scripts with a modern, scalable, and Python-based workflow management system. Cloud Composer provides a fully managed Airflow environment, reducing operational overhead and integrating seamlessly with other GCP services. This allows for better code maintainability, version control, and observability.
    *   **Trade-offs:** Requires re-implementation of KornShell logic in Python and adaptation to Airflow's task-based paradigm. There is an initial learning curve for teams unfamiliar with Airflow.
*   **BigQuery for Data Storage and Transformation**
    *   **Why:** Replaces the Oracle database for data warehousing. BigQuery offers petabyte-scale analytics, high performance, and cost-effectiveness, eliminating the need for Oracle licensing and maintenance. Its Standard SQL dialect is powerful and widely adopted, facilitating the translation of Oracle SQL logic.
    *   **Trade-offs:** Requires conversion of Oracle SQL*Plus specific syntax and functions to BigQuery Standard SQL. Schema adjustments may be necessary due to differences in data types and indexing strategies.
*   **Direct Data Ingestion from Oracle to BigQuery Staging via Airflow Python Operators**
    *   **Why:** The `python/data_ingestion.py` module, invoked by Airflow's `PythonOperator`, is designed to connect directly to the Carmen Oracle DB and load data into BigQuery staging tables. This approach keeps the data ingestion logic within the Airflow DAG, allowing for centralized orchestration and error handling.
    *   **Trade-offs:** The current implementation is a placeholder. A robust solution for large-scale data transfer from Oracle to BigQuery requires careful consideration of network connectivity (e.g., Cloud VPN/Interconnect), Oracle client libraries (e.g., `cx_Oracle`), and efficient data streaming/batching techniques (e.g., using Apache Beam/Dataflow for very large datasets, or `pandas` with `to_gbq` for smaller ones).
*   **Centralized Configuration with `config.yaml`**
    *   **Why:** All environment-specific parameters (GCP project IDs, dataset names, connection IDs, schedules) are externalized into a single YAML file. This promotes reusability, simplifies environment-specific deployments, and makes configuration changes easier without modifying code.
*   **Dedicated Control and Audit Tables in BigQuery**
    *   **Why:** New BigQuery tables (`control.etl_job_run`, `control.etl_watermark`, `audit.etl_validation_results`) are introduced to standardize the tracking of job execution, incremental load watermarks, and data quality checks. This leverages BigQuery's capabilities for structured logging and auditing.
    *   **Trade-offs:** These are new tables that need to be managed and integrated into the DAG's workflow. The `audit.etl_validation_results` table is currently created but not populated by the DAG, indicating a future enhancement.
*   **Dynamic Processing Date (`v_datum`) Retrieval**
    *   **Why:** The `v_datum` (processing date) is now dynamically retrieved from the migrated `metadata.dwtk_meldungen` table in BigQuery using a PythonOperator. This ensures consistency and allows the date to be passed as a templated parameter to subsequent BigQuery SQL tasks.
    *   **Trade-offs:** Requires the `metadata.dwtk_meldungen` table to be populated with relevant historical data.

## 4. Manual Steps Before Go-Live

Before deploying the migrated job to a production environment, the following manual steps must be completed:

1.  **GCP Project Setup:**
    *   Ensure the target GCP project (`your-gcp-project-id` in `config.yaml`) is correctly configured and has billing enabled.
    *   Replace `"your-gcp-project-id"` in `config.yaml` and all DDL files with the actual GCP Project ID.

2.  **BigQuery Dataset Creation:**
    *   Manually create the following BigQuery datasets in your GCP project if they do not already exist:
        *   `control`
        *   `metadata`
        *   `staging`
        *   `curated`
        *   `audit`

3.  **BigQuery Table Creation (DDL Execution):**
    *   Execute all SQL DDL scripts located in the `ddl/` directory against their respective BigQuery datasets. This can be done via the BigQuery UI, `bq` command-line tool, or a CI/CD pipeline.
    *   **Crucially, populate the `metadata.dwtk_meldungen` table** with historical data from the legacy Oracle `isbert_schema.dwtk_meldungen` table. This is essential for the `get_processing_date_task` to function correctly.

4.  **IAM Permissions:**
    *   The Service Account associated with your Cloud Composer environment (Airflow) must have the following minimum IAM roles:
        *   `BigQuery Data Editor`: For writing to `control`, `metadata`, `staging`, `curated`, and `audit` datasets.
        *   `BigQuery Data Viewer`: For reading from `metadata` dataset.
        *   `Secret Manager Secret Accessor`: If Oracle database credentials are stored in Google Secret Manager.
        *   Appropriate network permissions (e.g., `Compute Network User`, `Cloud SQL Client` if using Cloud SQL Proxy, or VPC Service Controls) to establish secure connectivity to the Carmen Oracle database.

5.  **Connection Strings & Secrets:**
    *   **Airflow Oracle Connection:** Create an Airflow Connection named `oracle_carmen_db` (as specified in `config.yaml`). This connection should contain the necessary details to connect to the Carmen Oracle database (e.g., Host, Port, Service Name/SID, Username, Password). It is highly recommended to store the Oracle password in Google Secret Manager and reference it in the Airflow connection.
    *   **GCP Project ID:** Verify that the `gcp_project_id` in `config.yaml` matches your actual GCP project ID.

6.  **Scheduling:**
    *   The DAG is configured with a cron schedule of `0 1 * * *` (daily at 1 AM UTC) in `config.yaml`. Confirm this schedule is appropriate for production and adjust if necessary.
    *   Ensure the Airflow DAG `dw_bert_ausd_v_ta_cntrct_templ_dag` is deployed to your Cloud Composer environment and is unpaused in the Airflow UI.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps, unresolved references, or areas requiring further attention (including B4 items from the design document):

*   **Critical: Oracle Data Ingestion Implementation (`python/data_ingestion.py`)**
    *   The `extract_and_load_carmen_data` function in `python/data_ingestion.py` is currently a placeholder. A robust and performant solution for extracting data from the Carmen Oracle database (`cds$ta_cntrct_template`, `cds$ta_care_description`) and loading it into BigQuery staging tables is **essential**. This requires:
        *   Detailed investigation into Carmen DB authentication, data volume, and network latency.
        *   Selection and implementation of an appropriate ingestion tool (e.g., `apache-airflow-providers-oracle` with `cx_Oracle`, Cloud Data Fusion, Dataflow, or a custom Python script optimized for streaming large datasets).
        *   Ensuring secure and private network connectivity between GCP and the Carmen DB (e.g., Cloud VPN, Cloud Interconnect, Private Service Connect).
*   **KornShell Utilities Full Re-implementation:**
    *   While `python/utils.py` provides basic replacements for `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh`, a thorough review is needed to ensure all edge cases and specific functionalities (e.g., complex date calculations in `h_alis_date.ksh`, specific error reporting mechanisms in `f_alis_msgerr.ksh`) are accurately replicated in Python.
*   **SQL*Plus Specific Features:**
    *   The `d_ausd_v_ta_cntrct_templ.sql` script used SQL*Plus specific commands (`DEFINE`, `COLUMN new_value`, `START`, `SPOOL`, `WHENEVER SQLERROR`). These are implicitly handled by Airflow's logging and error handling or are no longer relevant in the BigQuery context. If any specific output formatting or custom error handling logic from these commands was critical, it needs to be re-evaluated and potentially re-implemented within Airflow tasks or BigQuery SQL.
*   **Data Volume and Performance Assessment:**
    *   The current performance of the Oracle SQL query and the volume of data processed are unknown. Post-migration, the performance of the BigQuery transformation and data ingestion needs to be assessed with actual production data volumes to ensure it meets performance requirements and cost efficiency.
*   **Comprehensive Error Handling and Alerting:**
    *   The `log_error_message` function in `python/utils.py` is a starting point. Full integration with Cloud Logging and Cloud Monitoring for centralized log collection, custom metrics, and automated alerting (e.g., email, PagerDuty) on job failures or data quality issues needs to be configured and tested.
*   **Missing Automated Lineage Details:**
    *   As noted in the design document, automated lineage analysis was incomplete, requiring manual inference of execution and data flow. This increases the risk of overlooking subtle dependencies and requires diligent manual verification during testing.
*   **Data Quality Checks and `audit.etl_validation_results` Population:**
    *   The `audit.etl_validation_results` table has been created, but the current Airflow DAG does not include tasks to perform data quality checks or populate this table. This is a future enhancement to ensure data integrity and auditability.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run Tests:**

1.  **Unit Tests (Python):**
    *   Develop and execute unit tests for `python/utils.py` and `python/data_ingestion.py` to verify individual function logic (e.g., config loading, date parsing, error logging).
2.  **Local Airflow Testing:**
    *   Use `airflow dags test dw_bert_ausd_v_ta_cntrct_templ_dag YYYY-MM-DD` (e.g., `2023-01-01`) to test the DAG structure, task dependencies, and basic execution flow locally without actual data processing.
3.  **Development Cloud Composer Deployment:**
    *   Deploy the DAG and associated files (`dags/`, `python/`, `sql/`, `config/`) to a dedicated development or staging Cloud Composer environment.
4.  **Manual Trigger:**
    *   Trigger the `dw_bert_ausd_v_ta_cntrct_templ_dag` manually via the Airflow UI.
5.  **Scheduled Run Observation:**
    *   Observe the DAG's execution during its scheduled run time (`0 1 * * *` by default).
6.  **Data Reconciliation:**
    *   **Critical Step:** Perform a comprehensive data reconciliation between the output of the migrated job (`project.curated.final_fact_table` in BigQuery) and the legacy job (`sof$ta_cntrct_templ` in Oracle). This should include:
        *   **Row Count Comparison:** Verify that the number of rows in the BigQuery target table matches the Oracle target table for the same processing date.
        *   **Column-level Value Comparison:** Sample data from key columns (e.g., `CNTRCT_TEMPLATE_ID`, `CDS_DESCRIPTION_ID`, `CDS_DESCRIPTION`) and compare values between BigQuery and Oracle.
        *   **Data Type Verification:** Ensure data types are correctly mapped and no unexpected conversions occurred.
        *   **Edge Case Testing:** Test with specific `v_datum` values that might trigger different filtering conditions (e.g., dates where `modified_at` or `valid_to` are NULL).
7.  **Log Review:**
    *   Monitor Airflow task logs and Cloud Logging for any errors, warnings, or unexpected behavior during execution.

**What "Passing" Means:**

*   The Airflow DAG `dw_bert_ausd_v_ta_cntrct_templ_dag` completes successfully without any failed tasks.
*   The `curated.final_fact_table` in BigQuery is populated with data.
*   The data in `curated.final_fact_table` is an exact match (or within acceptable tolerances, if defined) to the data in the legacy `sof$ta_cntrct_templ` table for the same processing period, as verified by data reconciliation.
*   The `control.etl_job_run` table is updated with a 'SUCCESS' status for the job run.
*   The `control.etl_watermark` table is updated with the correct `last_watermark_value` corresponding to the `v_datum` used in the run.
*   No critical errors or unexpected warnings are observed in Airflow or Cloud Logging.
*   The job's execution time and resource consumption are within acceptable performance and cost thresholds.

## 7. Rollback Procedure

In the event of critical issues or data corruption after go-live, the following rollback procedure should be followed:

1.  **Immediate Rollback (Stop GCP Job, Re-enable Legacy):**
    *   **Pause the Airflow DAG:** Immediately pause the `dw_bert_ausd_v_ta_cntrct_templ_dag` in the Airflow UI to prevent further execution.
    *   **Re-enable Legacy Job:** Re-enable the original UC4 job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` in the legacy environment to resume normal business operations using the old pipeline.

2.  **Data Rollback (if necessary):**
    *   **BigQuery Target Table:** The `CREATE OR REPLACE TABLE` statement used in `sql/d_ausd_v_ta_cntrct_templ_transformed.sql` means that each successful run overwrites the entire `curated.final_fact_table`. If data corruption is detected, the table can be:
        *   **Truncated:** `TRUNCATE TABLE \`your-gcp-project-id.curated.final_fact_table\`` to clear corrupted data.
        *   **Restored from Snapshot:** If BigQuery table snapshots or time travel are enabled, the table can be restored to a state before the problematic run.
    *   **Control Tables:** If `control.etl_job_run` or `control.etl_watermark` were updated incorrectly, manual updates or deletion of the affected records may be required.

3.  **Code Rollback:**
    *   **Version Control:** Revert the Airflow DAG (`dags/dw_bert_ausd_v_ta_cntrct_templ_dag.py`) and any associated Python or SQL files (`python/`, `sql/`, `config/`) to a previous, known-good version in your Git repository.
    *   **Redeploy:** Redeploy the reverted code to the Cloud Composer environment.
    *   **Debugging:** Analyze the logs and metrics from the failed runs to identify the root cause of the issue before attempting to re-deploy the corrected version.