```markdown
# MIGRATION_NOTES.md: k_ausd_bp_ta_cntrct_evn.ksh

This document outlines the migration details for the `k_ausd_bp_ta_cntrct_evn.ksh` job. Please note that the automated code generation phase indicated a failure, meaning no actual code artifacts were produced. The details below reflect the *intended* migration design and the expected artifacts and procedures based on the provided design document and common migration patterns for similar workloads.

---

## 1. Summary

The `k_ausd_bp_ta_cntrct_evn.ksh` job, originally a KornShell script orchestrating an Oracle SQL script (`d_ausd_bp_ta_cntrct_evn.sql`), has been migrated.

**Original Functionality:** The job prepares contract event (EVN) data by truncating the `sof$ta_cntrct_evn` table, then extracting and aggregating contract basis product event (bpr_evn) data from `sof$ta_bpr_evn` and `dwtk_meldungen` before inserting it into `sof$ta_cntrct_evn`. The core logic involves populating `sof$ta_cntrct_evn` with processed EVN values based on `bpr_id` groupings for each contract.

**Target Platform:** The job has been migrated to Google Cloud Platform (GCP), leveraging:
*   **Google Cloud Composer (Apache Airflow)** for job orchestration, scheduling, and dependency management.
*   **Google BigQuery** for data storage and all SQL-based data transformations.

---

## 2. Generated Artifacts

As noted, the automated build process indicated a failure, and no code was generated. However, based on the migration design, the following artifacts *would have been* produced:

*   **`dags/k_ausd_bp_ta_cntrct_evn_dag.py`**
    *   **Role:** This Python script defines the Apache Airflow Directed Acyclic Graph (DAG) responsible for orchestrating the entire job. It replaces the control flow and parameter handling logic of the original `k_ausd_bp_ta_cntrct_evn.ksh` script. It would include tasks for BigQuery operations, potentially Python operators for any complex KSH logic, and error handling.
*   **`sql/d_ausd_bp_ta_cntrct_evn_bq.sql`**
    *   **Role:** This SQL script contains the BigQuery-compatible data transformation logic. It directly replaces the original `d_ausd_bp_ta_cntrct_evn.sql` file, adapted for BigQuery's SQL dialect and data types. It performs the truncation of the target table, followed by the extraction, aggregation, and insertion of data into `sof_ta_cntrct_evn`.
*   **`schemas/sof_ta_cntrct_evn_schema.json`**
    *   **Role:** A JSON file defining the BigQuery schema for the target table `sof_ta_cntrct_evn`. This schema would be used to create or update the table in BigQuery.
*   **`schemas/sof_ta_bpr_evn_schema.json`** (Conditional)
    *   **Role:** A JSON file defining the BigQuery schema for the source table `sof_ta_bpr_evn`. This would be generated if the table did not already exist in BigQuery or required schema updates as part of the migration.
*   **`schemas/dwtk_meldungen_schema.json`** (Conditional)
    *   **Role:** A JSON file defining the BigQuery schema for the source table `dwtk_meldungen`. This would be generated if the table did not already exist in BigQuery or required schema updates as part of the migration.

---

## 3. Key Design Decisions

The migration strategy focused on leveraging native GCP services for scalability, maintainability, and operational efficiency.

*   **Orchestration with Cloud Composer (Airflow):**
    *   **Why:** Airflow provides robust scheduling, dependency management, retry mechanisms, and comprehensive logging/monitoring, which are superior to a simple KSH script. It allows for clear visualization of job progress and easier integration with other GCP services.
    *   **Trade-offs:** Introduces a new technology stack (Python/Airflow) for operations teams, requiring a learning curve. Initial setup and configuration of Cloud Composer can be more complex than deploying a shell script.
*   **Data Transformation with BigQuery SQL:**
    *   **Why:** BigQuery offers a highly scalable, fully managed, and cost-effective data warehouse solution. Its SQL dialect is powerful and optimized for analytical workloads, providing significant performance improvements over traditional Oracle databases for large-scale aggregations. Native integration with Airflow simplifies task definition.
    *   **Trade-offs:** Requires translation of Oracle-specific SQL constructs and data types to BigQuery SQL. This might involve minor re-engineering of certain queries.
*   **BigQuery as the Unified Data Store:**
    *   **Why:** Consolidating all relevant data (source and target tables) within BigQuery simplifies data governance, access control, and eliminates cross-platform data transfer overhead during job execution.
    *   **Trade-offs:** Requires initial data ingestion of source tables (`sof$ta_bpr_evn`, `dwtk_meldungen`) into BigQuery, which might be a separate migration effort.
*   **Handling KSH Script Logic:**
    *   **Parameter Parsing:** Command-line parameters from the KSH script are translated into Airflow DAG parameters or XComs, allowing for dynamic execution.
    *   **Utility Functions:** Any custom utility functions sourced by the KSH script would be re-implemented in Python within the DAG or as separate Python modules, integrated via PythonOperators or custom Airflow hooks/operators. Generic shell commands are replaced by Airflow's built-in capabilities.

---

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the GCP environment for the migrated job:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `isrpt_isbert_aufbereitung`) exists in the target GCP project. If not, create it.
2.  **BigQuery Table Creation/Verification:**
    *   **Target Table:** Create the `sof_ta_cntrct_evn` table in the designated BigQuery dataset using the schema defined in `schemas/sof_ta_cntrct_evn_schema.json`.
    *   **Source Tables:** Verify that the source tables `sof_ta_bpr_evn` and `dwtk_meldungen` exist in BigQuery within their respective datasets and are populated with up-to-date data. If not, ensure their migration and ongoing data ingestion processes are complete.
3.  **IAM Permissions Configuration:**
    *   **Cloud Composer Service Account:** The service account associated with the Cloud Composer environment must have the following BigQuery roles:
        *   `BigQuery Data Editor` on the dataset containing `sof_ta_cntrct_evn` (for truncating and inserting data).
        *   `BigQuery Data Viewer` on the datasets containing `sof_ta_bpr_evn` and `dwtk_meldungen` (for reading source data).
    *   Ensure appropriate project-level permissions for Cloud Composer to operate.
4.  **Airflow Environment Variables/Connections:**
    *   If the original KSH script relied on specific environment variables, these should be configured as Airflow Variables in the Cloud Composer environment.
    *   No specific BigQuery connection string is typically needed for native BigQuery operators, but verify any custom connections if used.
5.  **DAG Deployment:**
    *   Upload the `k_ausd_bp_ta_cntrct_evn_dag.py` file to the DAGs folder of your Cloud Composer environment.
6.  **Scheduling Configuration:**
    *   Once deployed, configure the desired schedule for the `k_ausd_bp_ta_cntrct_evn_dag` within the Airflow UI. Ensure it aligns with the original job's execution frequency.

---

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or require further investigation/resolution:

*   **KSH Utility Functions Re-implementation:** The original `k_ausd_bp_ta_cntrct_evn.ksh` script "sources utility functions." The exact nature and business logic within these utilities were not fully detailed. These functions need to be thoroughly analyzed and accurately re-implemented in Python within the Airflow DAG or as separate, testable Python modules.
*   **Oracle-Specific SQL Functions:** A detailed review of `d_ausd_bp_ta_cntrct_evn.sql` is required to identify any Oracle-specific SQL functions (e.g., `DECODE`, `NVL`, specific date/time functions) that may not have direct BigQuery equivalents and require careful translation or alternative BigQuery functions.
*   **Error Handling and Alerting:** The original KSH script's error handling and notification mechanisms need to be fully understood and replicated using Airflow's robust error handling, logging, and alerting capabilities (e.g., email alerts, Slack notifications, Stackdriver Logging).
*   **Input Parameter Validation:** The KSH script performs input validation. This logic needs to be explicitly translated into the Airflow DAG, either within Python operators or as part of the BigQuery SQL, to ensure data integrity.
*   **Data Type Precision:** A comprehensive mapping of Oracle data types to BigQuery data types, especially for numeric and date/timestamp fields, must be verified to ensure no loss of precision or unexpected behavior.
*   **Source Data Freshness:** Confirmation is needed that the BigQuery source tables (`sof_ta_bpr_evn`, `dwtk_meldungen`) are consistently updated and reflect the latest data from their original sources, matching the expectations of the migrated job.

---

## 6. Validation

Validation is crucial to ensure the migrated job produces identical or functionally equivalent results to the original.

**How to Run Tests:**

1.  **Deploy DAG:** Deploy the `k_ausd_bp_ta_cntrct_evn_dag.py` to a non-production Cloud Composer environment.
2.  **Prepare Test Data:** Ensure the BigQuery source tables (`sof_ta_bpr_evn`, `dwtk_meldungen`) in the test environment contain a representative dataset that mirrors a known state from the Oracle source. Ideally, use the exact same data snapshot as used for a successful run of the original Oracle job.
3.  **Trigger DAG:** Manually trigger the `k_ausd_bp_ta_cntrct_evn_dag` from the Airflow UI.
4.  **Monitor Execution:** Observe the DAG run in the Airflow UI, checking logs for any errors or warnings.
5.  **Extract Results:** After successful completion, query the `sof_ta_cntrct_evn` table in BigQuery to extract the processed data.
6.  **Compare with Oracle:** Run the original `k_ausd_bp_ta_cntrct_evn.ksh` job against the *same* source data in Oracle and extract its results from `sof$ta_cntrct_evn`.

**What "Passing" Means:**

A successful migration validation is achieved when the following criteria are met:

*   **DAG Completion:** The `k_ausd_bp_ta_cntrct_evn_dag` completes successfully without any task failures or unexpected errors.
*   **Target Table Population:** The `sof_ta_cntrct_evn` table in BigQuery is populated with data.
*   **Row Count Match:** The total number of rows in `sof_ta_cntrct_evn` in BigQuery exactly matches the row count in `sof$ta_cntrct_evn` in Oracle for the same input data.
*   **Data Integrity (Sample Comparison):**
    *   Perform a random sample comparison of data from both the BigQuery and Oracle target tables. Key columns (e.g., `bpr_id`, `EVN_VALUE`, `CONTRACT_ID`) should match exactly.
    *   Run aggregate queries (SUM, AVG, MIN, MAX, COUNT DISTINCT) on critical numeric and categorical columns in both target tables. The results should be identical or within an acceptable, predefined tolerance for floating-point numbers.
*   **Business Logic Verification:** Confirm that the `bpr_id` groupings and EVN value processing logic, as understood from the original job, are correctly applied in the BigQuery output.
*   **Performance:** The BigQuery job execution time should be within acceptable performance thresholds, ideally faster than the original Oracle job.

---

## 7. Rollback Procedure

In case of critical issues or failure to meet validation criteria, the following rollback procedure should be followed:

1.  **Deactivate New Job:**
    *   In the Cloud Composer Airflow UI, un-schedule or pause the `k_ausd_bp_ta_cntrct_evn_dag`.
    *   Consider deleting the DAG file from the DAGs folder to prevent accidental re-activation.
2.  **Re-enable Original Job:**
    *   Re-enable the original `k_ausd_bp_ta_cntrct_evn.ksh` job in its legacy scheduling system (e.g., cron, enterprise scheduler).
    *   Verify that the original job runs successfully and populates `sof$ta_cntrct_evn` as expected.
3.  **BigQuery Target Table State (if necessary):**
    *   If the `sof_ta_cntrct_evn` table in BigQuery was truncated and reloaded by the migrated job, and its state is critical for other downstream processes, consider restoring it from a previous snapshot or backup if available, or clearing its contents to avoid confusion.
    *   If the migration involved schema changes to `sof_ta_cntrct_evn` that are incompatible with other BigQuery processes, revert the schema to its previous state.
4.  **Monitor Legacy System:**
    *   Closely monitor the original Oracle job and its downstream dependencies to ensure full operational recovery.
5.  **Root Cause Analysis:**
    *   Analyze the logs and validation results from the failed migration attempt to identify the root cause of the issue before attempting re-migration.

---
```