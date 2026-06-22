# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL workflow previously managed by `r_ausd_v_ta_cntrct_templ.ksh` and its dependent scripts (`k_ausd_v_ta_cntrct_templ.ksh`, `d_ausd_v_ta_cntrct_templ.sql`). The original workflow was responsible for reconciling contract data and populating the `SOF$TA_CNTRCT_TEMPL` table within a legacy Oracle environment.

The migration re-implements this entire workflow on Google Cloud Platform (GCP). The target platform utilizes:
*   **BigQuery** for data storage and all SQL-based transformations.
*   **Cloud Composer (Apache Airflow)** for workflow orchestration, replacing the KornShell scripts.

The core functionality remains the same: extracting a processing date, truncating a target table, and inserting transformed contract template data based on joined source tables with specific date and status filters.

## 2. Generated artifacts

The migration process has generated the following key artifacts:

1.  **`d_ausd_v_ta_cntrct_templ_bq.sql`**
    *   **Role:** This file contains the core data transformation logic, translated from Oracle SQL*Plus to BigQuery Standard SQL. It includes the logic for determining the processing date (`v_datum`), truncating the target table (`sof_ta_cntrct_templ`), and inserting the transformed data from `cds_ta_cntrct_template` and `cds_ta_care_description`. While presented as a single file, its components are designed to be executed as distinct steps within the Airflow DAG.

2.  **`r_ausd_v_ta_cntrct_templ_dag.py`**
    *   **Role:** This is an Apache Airflow DAG (Directed Acyclic Graph) written in Python. It replaces the orchestration logic of the original `r_ausd_v_ta_cntrct_templ.ksh` and `k_ausd_v_ta_cntrct_templ.ksh` scripts. The DAG defines three sequential tasks:
        *   `extract_v_datum`: Determines the processing date (`v_datum`) from `dwtk_meldungen` and passes it to subsequent tasks via XCom.
        *   `truncate_target_table`: Executes a `TRUNCATE TABLE` command on the BigQuery target table `sof_ta_cntrct_templ`.
        *   `insert_transformed_data`: Executes the main BigQuery `INSERT INTO ... SELECT ...` statement, utilizing the `v_datum` retrieved from XCom.
    *   This DAG manages task dependencies, error handling, and parameter passing within the GCP environment.

## 3. Key design decisions

### Orchestration: KornShell to Cloud Composer (Airflow)
*   **Decision:** Migrate from custom KornShell scripts (`r_*.ksh`, `k_*.ksh`) to Apache Airflow on Cloud Composer.
*   **Rationale:** Airflow provides robust, scalable, and cloud-native orchestration capabilities. It offers centralized monitoring, logging, scheduling, dependency management, and built-in retry mechanisms, significantly improving reliability and maintainability compared to disparate shell scripts. It also integrates seamlessly with other GCP services.
*   **Trade-offs:** Introduces a new technology stack (Python, Airflow concepts) and requires managing an Airflow environment.

### Data Storage & Transformation: Oracle SQL*Plus to BigQuery SQL
*   **Decision:** Migrate data storage and transformation logic from Oracle Database and SQL*Plus to BigQuery.
*   **Rationale:** BigQuery offers a highly scalable, fully managed, and cost-effective data warehouse solution. Its columnar storage and distributed query engine provide superior performance for analytical workloads. Standard SQL compatibility simplifies the migration of existing SQL logic.
*   **Trade-offs:** Requires setting up data ingestion pipelines from Oracle to BigQuery for source tables. BigQuery's SQL dialect has minor differences from Oracle SQL, necessitating careful translation.

### Parameter Passing: Shell Variables to Airflow XCom
*   **Decision:** Replace shell-based parameter passing (e.g., `&v_datum` in SQL*Plus) with Airflow's XCom (Cross-Communication) mechanism.
*   **Rationale:** XCom provides a native and robust way for tasks within a DAG to exchange small amounts of data, such as the `v_datum` processing date. This ensures data consistency and proper dependency flow between tasks.

### Truncate and Insert Pattern
*   **Decision:** Maintain the `TRUNCATE TABLE` followed by `INSERT INTO` pattern for populating the target table.
*   **Rationale:** This pattern ensures that the target table is always a fresh representation of the source data based on the current processing date, aligning with the original job's behavior. BigQuery's `TRUNCATE TABLE` is an efficient DDL operation.

### Error Handling and Logging
*   **Decision:** Leverage Airflow's native error handling, retry policies, and logging mechanisms.
*   **Rationale:** This replaces custom KornShell error handling (`f_alis_msgerr.ksh`) and provides a standardized, centralized approach to monitoring job health and debugging issues. Airflow integrates with Cloud Logging and Cloud Monitoring.

## 4. Manual steps before go-live

Before the migrated workflow can be deployed and run in production, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` as per `BIGQUERY_DATASET_ID` in the DAG) exists.
    *   Create the target table `project.dataset.sof_ta_cntrct_templ` with the appropriate schema (`cntrct_template_id`, `cds_description_id`, `cds_description`).
    *   Ensure source tables (`project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_cntrct_template`, `project.dataset.cds_ta_care_description`) exist and have compatible schemas.

2.  **Data Ingestion Pipelines:**
    *   Establish robust data ingestion pipelines (e.g., using Datastream, Fivetran, or custom batch jobs) to continuously replicate or transfer data from the legacy Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, `cds$ta_care_description`) into their respective BigQuery counterparts.
    *   Verify that the ingested data is fresh and complete according to business requirements.

3.  **IAM Permissions:**
    *   Grant the Cloud Composer service account (or the service account used by the Airflow worker) the necessary IAM roles for BigQuery:
        *   `BigQuery Data Editor` (or equivalent) on the target dataset (`project.dataset`) to allow `TRUNCATE TABLE` and `INSERT` operations.
        *   `BigQuery Data Viewer` (or equivalent) on the source datasets to allow `SELECT` operations.
        *   `BigQuery Job User` to run BigQuery jobs.

4.  **Airflow Connections:**
    *   Ensure the `google_cloud_default` Airflow connection is properly configured in your Cloud Composer environment. This connection is used by the `BigQueryExecuteQueryOperator` to authenticate with GCP services.

5.  **Airflow DAG Configuration:**
    *   Update the `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID` variables in `r_ausd_v_ta_cntrct_templ_dag.py` with the actual GCP project ID and BigQuery dataset ID.
    *   Set the desired `schedule` for the DAG (e.g., `@daily`, `0 0 * * *`) in `r_ausd_v_ta_cntrct_templ_dag.py` to match the original job's frequency.

6.  **Secrets Management (if applicable):**
    *   If any sensitive parameters or credentials were part of the original KSH scripts (beyond what's handled by Airflow connections), ensure they are securely managed in GCP Secret Manager and accessed appropriately by the DAG. (Not explicitly identified in this migration, but good practice).

## 5. Known gaps & unresolved references

The following items are flagged for follow-up or represent known limitations/assumptions:

*   **Missing `automation_rate` and `file_complexity` data:** The original design document inferred these values for `r_ausd_v_ta_cntrct_templ.ksh`. While not critical for the technical migration, this indicates a potential gap in the initial assessment data.
*   **`starteSQLSkript` function details:** The exact implementation of the legacy `starteSQLSkript` function (which likely wrapped `h_alis_sqlplus.ksh`) was not fully detailed. It's assumed to be a standard SQL*Plus execution wrapper. The migration directly uses `BigQueryExecuteQueryOperator`. Any custom logic embedded in `starteSQLSkript` (e.g., specific job status updates, unique error handling, temporary file management for record counts) would need to be re-implemented within the Airflow DAG if it's critical to the business process.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** This Oracle procedure for truncating the table is directly replaced by a BigQuery `TRUNCATE TABLE` statement. This assumes that `DWPA_UTIL_SKRIPT.runstatement` did not perform any additional, non-standard operations beyond a simple truncate.
*   **Data Latency:** The migration relies on the successful and timely ingestion of Oracle source data into BigQuery. The required data freshness and latency for these ingestion pipelines must be carefully considered and validated to ensure the migrated job meets its SLAs.
*   **`TODO` comments in generated code:** The generated `d_ausd_v_ta_cntrct_templ_bq.sql` and `r_ausd_v_ta_cntrct_templ_dag.py` files contain `TODO` comments for replacing placeholder `project.dataset` values with actual GCP project and dataset IDs. These must be addressed before deployment.

## 6. Validation

Validation ensures that the migrated workflow functions correctly, produces accurate results, and meets performance expectations.

### How to run tests:

1.  **BigQuery SQL Validation (Unit Test):**
    *   Execute the SQL queries from `d_ausd_v_ta_cntrct_templ_bq.sql` (or the individual query components) directly in the BigQuery console or via the `bq` command-line tool.
    *   Use a representative sample of source data in BigQuery that mirrors the Oracle source.
    *   Manually set the `v_datum` variable for testing purposes.

2.  **Airflow DAG Validation (Integration Test):**
    *   Upload `r_ausd_v_ta_cntrct_templ_dag.py` to the Cloud Composer DAGs folder.
    *   Trigger the DAG manually from the Airflow UI.
    *   Monitor the task logs in Airflow and Cloud Logging for any errors or warnings.
    *   Verify XCom values (e.g., `v_datum`) are correctly passed between tasks.

3.  **Data Validation (End-to-End Test):**
    *   **Pre-migration Baseline:** Before migrating, capture the state of `SOF$TA_CNTRCT_TEMPL` in Oracle after a successful run of the legacy job. Record row counts, checksums, and a sample of key data points.
    *   **Post-migration Comparison:** After a successful run of the Airflow DAG, query `project.dataset.sof_ta_cntrct_templ` in BigQuery.
    *   **Comparison Tools:** Use data comparison tools or custom SQL queries to compare:
        *   **Row Counts:** Ensure the number of rows in the BigQuery target table matches the Oracle target table for the same processing date.
        *   **Data Integrity:** Sample specific `CNTRCT_TEMPLATE_ID` values and verify that `CDS_DESCRIPTION_ID` and `CDS_DESCRIPTION` match exactly between Oracle and BigQuery.
        *   **Schema:** Confirm that the BigQuery table schema matches the Oracle table's structure and data types.
        *   **Edge Cases:** Test with `v_datum` values that trigger edge cases (e.g., `1900-01-01`, dates with no `modified_at` records, etc.).

### What "passing" means:

*   **Functional Equivalence:** The BigQuery target table `project.dataset.sof_ta_cntrct_templ` contains exactly the same data (row count, content, and schema) as the Oracle `SOF$TA_CNTRCT_TEMPL` table for the same processing date.
*   **Successful DAG Execution:** The `r_ausd_v_ta_cntrct_templ_dag` completes successfully without any task failures or retries in the Airflow UI.
*   **Performance:** The Airflow DAG and its underlying BigQuery jobs complete within acceptable timeframes, meeting or exceeding the performance of the legacy Oracle job.
*   **Logging and Alerting:** All relevant logs are captured in Cloud Logging, and any configured alerts (e.g., on task failure) are triggered correctly.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action: Disable New Workflow**
    *   Access the Cloud Composer Airflow UI.
    *   Locate the `r_ausd_v_ta_cntrct_templ_dag`.
    *   Toggle the DAG to "Off" to prevent any further runs of the migrated workflow.

2.  **Revert to Legacy Workflow**
    *   Re-enable the original `r_ausd_v_ta_cntrct_templ.ksh` job in the legacy Oracle environment.
    *   Verify that the legacy job can run successfully and populate the `SOF$TA_CNTRCT_TEMPL` table as expected.

3.  **Data Recovery (if necessary)**
    *   If the `project.dataset.sof_ta_cntrct_templ` table in BigQuery was corrupted or populated with incorrect data by the migrated job, it can be:
        *   **Truncated:** `TRUNCATE TABLE project.dataset.sof_ta_cntrct_templ;`
        *   **Restored (if backups exist):** If BigQuery table snapshots or backups are in place, restore the table to a known good state.
        *   **Repopulated (manual):** If the data is critical and cannot be easily restored, a manual process might be required to re-ingest or re-transform the correct data into BigQuery, potentially using the legacy Oracle table as the source of truth.

4.  **Investigation and Remediation**
    *   Analyze the logs from the failed Airflow DAG runs in Cloud Logging.
    *   Identify the root cause of the issue (e.g., BigQuery SQL error, IAM permissions, data ingestion problem).
    *   Develop and test a fix in a non-production environment.

5.  **Re-deployment (after fix)**
    *   Once the fix is validated, update the Airflow DAG and/or BigQuery SQL.
    *   Re-enable the `r_ausd_v_ta_cntrct_templ_dag` in Cloud Composer.
    *   Monitor closely after re-enabling.