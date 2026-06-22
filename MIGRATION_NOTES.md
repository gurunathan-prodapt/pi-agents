# MIGRATION_NOTES.md for DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Summary

The legacy ETL job `DW.BERT_AUSD_BP_TA_BCP_ICCID` has been migrated from its on-premise environment to Google Cloud Platform (GCP).

**Original System:**
*   **Orchestration:** UC4/Automic
*   **ETL Logic:** KornShell scripts (`r_ausd_bp_ta_bcp_iccid.ksh`, `k_ausd_bp_ta_bcp_iccid.ksh`) and Oracle SQL*Plus script (`d_ausd_bp_ta_bcp_iccid.sql`)
*   **Data Storage:** Oracle Database (tables like `DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, `SOF$TA_ICCID_VERTRAG`, `SOF$TA_BCP_ICCID`)

**Target Platform:**
*   **Orchestration:** Google Cloud Composer (Apache Airflow)
*   **ETL Logic:** Python (for orchestration logic) and BigQuery Standard SQL (for data transformation)
*   **Data Storage:** Google BigQuery (tables like `dwtk_meldungen`, `sof_ta_bpr_bcp`, `sof_ta_iccid_vertrag`, `sof_ta_bcp_iccid`)

The job's primary purpose remains to prepare selected 'Basisprodukte' (basic products) for BERT's demand scoring system by populating the `sof_ta_bcp_iccid` table with enriched data.

## 2. Generated Artifacts

The following files have been generated as part of this migration:

*   **`d_ausd_bp_ta_bcp_iccid.bqsql`**
    *   **Role:** Contains the core data transformation logic, translated from the original Oracle SQL*Plus script (`d_ausd_bp_ta_bcp_iccid.sql`) into BigQuery Standard SQL. This script performs the truncation of the target table and the subsequent `INSERT INTO ... SELECT DISTINCT` operation.
*   **`bert_ausd_bp_ta_bcp_iccid_tasks.py`**
    *   **Role:** A Python module containing functions that encapsulate the orchestration logic previously handled by the KornShell scripts (`r_ausd_bp_ta_bcp_iccid.ksh` and `k_ausd_bp_ta_bcp_iccid.ksh`). This includes parameter parsing, date calculations, and dynamic construction/execution of the BigQuery SQL.
*   **`dw_bert_ausd_bp_ta_bcp_iccid_dag.py`**
    *   **Role:** The Apache Airflow DAG definition file. This Python script defines the workflow, scheduling, and tasks (using `PythonOperator` and/or `BigQueryOperator`) to orchestrate the execution of the `bert_ausd_bp_ta_bcp_iccid_tasks.py` logic and the `d_ausd_bp_ta_bcp_iccid.bqsql` script within Cloud Composer.

## 3. Key Design Decisions

*   **Cloud-Native Re-platforming:** The entire workflow was re-platformed to GCP using managed services (Cloud Composer, BigQuery) to leverage scalability, performance, and reduced operational overhead.
*   **Airflow for Orchestration:** Cloud Composer (Apache Airflow) was chosen to replace UC4 due to its robust scheduling capabilities, Python-based extensibility, and native integration with GCP services.
*   **BigQuery for Data Processing:** BigQuery was selected as the target data warehouse to replace Oracle, offering serverless analytics, high performance for large datasets, and cost-effectiveness.
*   **KornShell to Python Refactoring:** The complex orchestration logic within the KornShell scripts was refactored into Python functions. This allows for seamless integration with Airflow's `PythonOperator` and provides better maintainability and testability.
*   **Oracle SQL to BigQuery Standard SQL Translation:** The core SQL transformation logic was translated to BigQuery Standard SQL. Oracle-specific hints (`/*+ full(bp) parallel(bp,4) ... */`) were removed as they are not applicable in BigQuery. Implicit comma joins were converted to explicit `JOIN` clauses for clarity and best practice.
*   **Dynamic SQL Generation:** Given the dynamic parameter handling in the original KornShell scripts, the Python tasks are designed to dynamically construct and execute the BigQuery SQL, allowing for flexible parameter injection (e.g., snapshot date).
*   **Data Synchronization Strategy:** A strategy for initial historical data load and continuous data synchronization (e.g., using Datastream or custom CDC) from the source Oracle system to BigQuery was adopted to ensure data freshness until the Oracle source can be deprecated.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Create the necessary BigQuery datasets (e.g., `isbert_schema`, `sof_ta`) in the target GCP project.
2.  **BigQuery Table Schema Creation:**
    *   Define and create the target BigQuery tables: `dwtk_meldungen`, `sof_ta_bpr_bcp`, `sof_ta_iccid_vertrag`, and `sof_ta_bcp_iccid`.
    *   **Crucial:** Ensure the schemas accurately reflect the source Oracle table definitions, paying close attention to data types and nullability.
3.  **Initial Data Load (Historical):**
    *   Perform a one-time historical data load from the source Oracle tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, `SOF$TA_ICCID_VERTRAG`, `SOF$TA_BCP_ICCID`) into their respective BigQuery counterparts.
4.  **Continuous Data Synchronization:**
    *   Set up and configure a continuous data synchronization mechanism (e.g., Google Cloud Datastream, custom Change Data Capture solution) to keep the BigQuery source tables (`dwtk_meldungen`, `sof_ta_bpr_bcp`, `sof_ta_iccid_vertrag`) updated with changes from the Oracle source until the Oracle system is fully deprecated.
5.  **IAM Permissions Configuration:**
    *   Ensure the Cloud Composer service account has the necessary IAM roles and permissions, including:
        *   `BigQuery Data Editor` or `BigQuery User` for executing queries and writing to tables.
        *   `Storage Object Admin` or `Storage Object Viewer` for accessing DAGs and logs in Cloud Storage.
6.  **Airflow Connection Setup (if applicable):**
    *   If not using the default service account for BigQuery, configure a BigQuery connection within Airflow.
7.  **Airflow Environment Variables/Secrets:**
    *   If any sensitive parameters or environment-specific configurations (e.g., project IDs, dataset names) are not hardcoded, ensure they are configured as Airflow Variables or stored securely in Google Secret Manager and accessed by the DAG.
8.  **Python Dependencies:**
    *   Verify that any specific Python libraries required by `bert_ausd_bp_ta_bcp_iccid_tasks.py` are installed in the Cloud Composer environment.
9.  **DAG Deployment:**
    *   Upload `dw_bert_ausd_bp_ta_bcp_iccid_dag.py` and `bert_ausd_bp_ta_bcp_iccid_tasks.py` (and `d_ausd_bp_ta_bcp_iccid.bqsql` if stored separately) to the Cloud Storage bucket associated with the Cloud Composer environment's `dags` folder.
10. **Scheduling Configuration:**
    *   Configure the desired schedule for the `dw_bert_ausd_bp_ta_bcp_iccid` DAG in the Airflow UI, mirroring the original UC4 schedule.

## 5. Known Gaps & Unresolved References

The following items were flagged during the migration design and require further attention or follow-up:

*   **Missing Complexity and Automation Rate Data:** The absence of `file_complexity` and `automation_rate` data for source files means the initial assessment of migration effort might be incomplete. Further manual analysis may be required if unexpected complexities arise.
*   **KornShell Script Nuances:**
    *   The exact behavior of the `starteSQLSkript` function (implicitly called by ksh) was assumed to be basic SQL execution. Any complex error handling or logging within this function needs to be explicitly replicated in the Python tasks.
    *   The full scope of error handling and parameter validation within the original KornShell scripts needs thorough testing to ensure identical behavior in the Python refactoring.
*   **`gestern.ksh` Script Logic:** The logic for deriving `p_datum_heute` and `p_datum_gestern` from `gestern.ksh` has been translated to Python date calculations. This translation needs explicit validation to ensure correctness for all edge cases (e.g., month/year boundaries).
*   **UC4 External Dependencies:** The purpose and content of UC4 objects like `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` were not fully known. It is assumed they are UC4-specific and replaced by Airflow/GCP standard practices. If they contained critical logic, this needs to be re-evaluated.
*   **Oracle `DWPA_UTIL_SKRIPT.runstatement`:** The specific implementation of this Oracle stored procedure for table truncation is unknown. The migration assumes a direct `TRUNCATE TABLE` equivalent in BigQuery. Verification of exact behavior (e.g., logging, error handling) is needed.
*   **Legacy Data Types:** While a best-effort translation was made, a definitive mapping of Oracle data types to BigQuery data types requires access to the precise Oracle source schema definitions to ensure accurate BigQuery table creation and prevent data loss or type mismatches.

## 6. Validation

To ensure the successful migration and correct operation of the `DW.BERT_AUSD_BP_TA_BCP_ICCID` job on GCP, follow these validation steps:

**How to Run Tests:**

1.  **Unit Testing:**
    *   Execute unit tests for the Python functions within `bert_ausd_bp_ta_bcp_iccid_tasks.py` to verify parameter parsing, date calculations, and SQL generation logic.
    *   Execute unit tests for the `d_ausd_bp_ta_bcp_iccid.bqsql` script by running it against a test BigQuery environment with sample data.
2.  **Integration Testing:**
    *   Manually trigger the `dw_bert_ausd_bp_ta_bcp_iccid` DAG in the Cloud Composer Airflow UI in a development/staging environment.
    *   Monitor the DAG run in the Airflow UI for task success/failure and review task logs in Cloud Logging.
3.  **End-to-End Data Validation:**
    *   Run the migrated job in a staging environment with a representative dataset.
    *   Compare the data in the target BigQuery table (`sof_ta_bcp_iccid`) with the output of the original Oracle job for the same input data and execution date.

**What "Passing" Means:**

*   **DAG Execution:** The `dw_bert_ausd_bp_ta_bcp_iccid` DAG completes successfully without any failed tasks or unexpected errors in the Airflow UI.
*   **Logs:** Cloud Logging for the Airflow tasks shows no critical errors, unexpected warnings, or stack traces. Informational logs should reflect the expected execution flow.
*   **Data Accuracy:**
    *   The `sof_ta_bcp_iccid` table in BigQuery is populated.
    *   The row count in the BigQuery `sof_ta_bcp_iccid` table matches the row count from the original Oracle `SOF$TA_BCP_ICCID` table for the same execution.
    *   A sample of records (e.g., using `SELECT * FROM ... LIMIT 100`) from the BigQuery table matches the corresponding records from the Oracle table.
    *   Key aggregate metrics (e.g., `COUNT(*)`, `COUNT(DISTINCT CNTRCT_ID)`, `SUM(some_numeric_column)`) from the BigQuery table match those from the Oracle table.
*   **Performance:** The execution time of the Airflow DAG and the BigQuery query is within acceptable limits, ideally matching or improving upon the original job's performance.
*   **Idempotency:** Rerunning the DAG for the same logical date produces the same correct output in the target table.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Disable New Job:** Immediately pause or delete the `dw_bert_ausd_bp_ta_bcp_iccid` DAG in the Cloud Composer Airflow UI to prevent further execution.
2.  **Revert to Legacy Job:** Reactivate and schedule the original `DW.BERT_AUSD_BP_TA_BCP_ICCID` job in UC4.
3.  **Data Integrity Check (Oracle):** Verify the state of the `SOF$TA_BCP_ICCID` table in Oracle. If the new job caused any data corruption or incorrect updates before being rolled back, restore the table from the most recent valid backup.
4.  **BigQuery Table Review:** Review the `sof_ta_bcp_iccid` table in BigQuery. If the data is incorrect, it can be truncated or dropped, and then re-populated with correct data if needed (e.g., from a snapshot or by re-running the legacy job and then re-syncing).
5.  **Troubleshooting & Analysis:** Investigate the root cause of the failure using Airflow logs, BigQuery job history, and Cloud Logging.
6.  **Re-deployment (if fixed):** Once the issue is identified and resolved, the corrected DAG and associated files can be re-deployed to Cloud Composer, and the validation steps repeated.