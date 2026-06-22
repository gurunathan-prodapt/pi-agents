# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Summary

This document details the migration of the `DW.BERT_AUSD_BP_TA_BCP_ICCID` job.

**Original Job:**
The original job was responsible for preparing instantiated basic products for BERT's demand scoring system. It consisted of:
*   **Orchestration:** UC4 job (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) invoking KornShell scripts.
*   **KornShell Scripts:** `r_ausd_bp_ta_bcp_iccid.ksh` (main orchestrator) and `k_ausd_bp_ta_bcp_iccid.ksh` (SQL execution wrapper), handling parameters, logging, and error handling.
*   **Core Logic:** An Oracle SQL*Plus script (`d_ausd_bp_ta_bcp_iccid.sql`) that populated or refreshed the `sof$ta_bcp_iccid` table by joining `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag`, and deriving a date parameter (`Stichtag`) from `isbert_schema.dwtk_meldungen`.

**Target Platform:**
The job has been re-implemented as a BigQuery-native solution on Google Cloud Platform (GCP).
*   **Orchestration:** Apache Airflow (managed by Cloud Composer).
*   **Data Warehouse:** Google BigQuery for all source and target tables.
*   **Language:** Python for orchestration logic and BigQuery Standard SQL for data transformations.

## 2. Generated Artifacts

The migration produced the following file:

*   **`dags/dw_bert_ausd_bp_ta_bcp_iccid.py`**
    *   **Role:** This is the main Apache Airflow Directed Acyclic Graph (DAG) file. It defines the entire workflow, including tasks for fetching parameters, truncating the target table, and inserting transformed data into BigQuery. It replaces the functionality of the original UC4 job and KornShell orchestration scripts.

## 3. Key Design Decisions

*   **Apache Airflow for Orchestration:** Airflow was chosen to replace UC4 and KornShell scripts due to its native cloud integration, Python-based extensibility, robust scheduling, monitoring, and error handling capabilities. This aligns with the target architecture of Cloud Composer for workflow management on GCP.
*   **BigQuery for Data Transformation:** BigQuery was selected as the primary data warehouse, replacing Oracle. Its serverless, scalable, and cost-effective nature makes it ideal for large-scale data warehousing and transformations. BigQuery Standard SQL was used to re-implement the Oracle SQL*Plus logic, ensuring compatibility and leveraging BigQuery's performance features.
*   **Python for Parameter Handling and Utilities:** The parameter parsing, environment setup, and helper script logic previously handled by KornShell have been re-implemented in Python within the Airflow DAG. Python is the native language for Airflow, allowing for seamless integration, leveraging its rich ecosystem for date/time operations, logging, and BigQuery API interactions.
*   **Direct BigQuery Operators:** The `BigQueryOperator` was utilized for executing SQL statements directly within BigQuery. This approach simplifies the DAG structure, reduces external dependencies, and efficiently leverages BigQuery's capabilities.
*   **XCom for Inter-Task Communication:** Airflow's Cross-Communication (XCom) mechanism is used to pass the dynamically fetched `stichtag_date` from the Python task to subsequent tasks, maintaining the original job's parameter flow.
*   **`TRUNCATE TABLE` then `INSERT INTO` Pattern:** The BigQuery DML operations mirror the original Oracle `TRUNCATE` and `INSERT` pattern for refreshing the target table, ensuring the same data refresh strategy.
*   **Removal of Oracle-Specific Constructs:** Oracle-specific syntax such as `/*+ ... */` (hints) and explicit `COMMIT;` statements were removed as they are not applicable or necessary in BigQuery, which handles transactions atomically for DML operations.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset_id` in `your-gcp-project-id`) exists. If not, create it.
2.  **BigQuery Table Creation & Data Migration:**
    *   **Target Table:** Create the schema for `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` in BigQuery. The DDL should be derived from the original Oracle `sof$ta_bcp_iccid` table.
    *   **Source Tables:** Ensure the following source tables have been migrated from Oracle to BigQuery and are populated with data:
        *   `PROJECT_ID.DATASET_ID.TA_BPR_BCP`
        *   `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG`
        *   `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN`
3.  **IAM/Permissions Configuration:**
    *   The Google Cloud Service Account associated with your Cloud Composer environment (Airflow worker) must be granted the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `PROJECT_ID.DATASET_ID` to allow `TRUNCATE` and `INSERT` operations on `TA_BCP_ICCID`.
        *   `BigQuery Data Viewer` on `PROJECT_ID.DATASET_ID` to allow `SELECT` operations from `TA_BPR_BCP`, `TA_ICCID_VERTRAG`, and `DWTK_MELDUNGEN`.
4.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` connection in your Airflow environment is correctly configured to authenticate with GCP.
5.  **DAG Deployment:**
    *   Upload the `dags/dw_bert_ausd_bp_ta_bcp_iccid.py` file to the DAGs folder in your Cloud Composer environment's Cloud Storage bucket.
6.  **Scheduling Configuration:**
    *   Update the `schedule` parameter in the DAG definition within `dw_bert_ausd_bp_ta_bcp_iccid.py` from `None` to the desired schedule (e.g., `"@daily"`, `"0 0 * * *"`) to match the original UC4 job's execution frequency.

## 5. Known Gaps & Unresolved References

*   **`v_datum` Usage in `INSERT` Statement:** The original Oracle SQL script determined a `v_datum` value but did not explicitly use it in the `INSERT` statement provided in the design document. The migrated DAG fetches this value, but the `insert_data_task` currently does not use it for filtering. **If the original intent was to filter the `INSERT` based on `v_datum`, the BigQuery SQL in `insert_data_task` needs to be updated with a `WHERE` clause using the XCom-pulled `stichtag_date`.** This requires clarification from the business owner.
*   **"Retire" Migration Bucket for `d_ausd_bp_ta_bcp_iccid.sql`:** The design document flagged the core Oracle SQL script for "retire." This migration assumes that the *logic* of this script is still required and has been re-implemented in BigQuery SQL. **If "retire" implies the business logic itself is obsolete, then the job might be decommissioned rather than migrated. Clarification from the business owner is crucial.**
*   **KornShell Helper Script Functionality:** While the general capabilities of KornShell helper scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`) are covered by Python's standard library and Airflow's logging, any highly specific custom error reporting or utility functions need to be explicitly mapped to Airflow's alerting mechanisms or custom Python functions. The current DAG relies on standard Airflow logging.
*   **Original UC4 External Dependencies:** Any external dependencies or upstream/downstream jobs defined in the original UC4 XML (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) that are not explicitly part of this job's internal logic need to be identified and configured in Airflow (e.g., using `ExternalTaskSensor` or by integrating into a larger DAG structure). The current DAG is designed as a standalone unit.
*   **`Wiederanlaufwert` (Restart Value) Parameter:** The original KornShell script handled a `Wiederanlaufwert` parameter for restart logic. The current Airflow DAG does not explicitly implement a custom restart mechanism beyond Airflow's native retry capabilities. If a specific "restart from a certain point" logic is required, it needs to be designed and implemented (e.g., by storing state or using a custom parameter).

## 6. Validation

To ensure the successful migration and correct functioning of the `dw_bert_ausd_bp_ta_bcp_iccid` DAG, follow these validation steps:

**How to Run Tests:**

1.  **Local Airflow Environment (Development/Testing):**
    *   Ensure your BigQuery tables (source and target) are set up and populated with representative test data.
    *   Use the Airflow CLI to simulate a DAG run: `airflow dags test dw_bert_ausd_bp_ta_bcp_iccid <execution_date>` (e.g., `2023-01-01`).
    *   For individual task testing: `airflow tasks test dw_bert_ausd_bp_ta_bcp_iccid fetch_stichtag_task <execution_date>`.
2.  **Cloud Composer Environment (UAT/Production):**
    *   Deploy the `dags/dw_bert_ausd_bp_ta_bcp_iccid.py` file to your Cloud Composer environment.
    *   Trigger the DAG manually from the Airflow UI.
    *   Monitor the DAG run status and task logs directly within the Airflow UI.

**What "Passing" Means:**

*   **DAG Run Status:** The entire DAG run must complete successfully, with all tasks (e.g., `fetch_stichtag_task`, `truncate_target_table_task`, `insert_data_task`) marked as "success" (green) in the Airflow UI.
*   **Logs:** Review the task logs for each operator. There should be no errors, unexpected warnings, or stack traces.
*   **Data Validation:**
    *   **Target Table State:** Verify that the `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` table in BigQuery was first truncated and then populated with new data.
    *   **Row Count:** Compare the row count of `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` after the DAG run with the expected row count from the original Oracle `sof$ta_bcp_iccid` table (for a comparable `Stichtag` or dataset).
    *   **Data Integrity:** Perform spot checks on a representative sample of data in `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`. Verify that `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, and `TN_IMSI_HLR` values are correctly derived from the joined source tables (`TA_BPR_BCP`, `TA_ICCID_VERTRAG`) according to the specified logic.
    *   **`Stichtag` Value:** Confirm that the `stichtag_date` value pushed to XCom by `fetch_stichtag_task` matches the expected value derived from `DWTK_MELDUNGEN`.
*   **Performance:** The DAG should complete within an acceptable timeframe, ideally comparable to or better than the original Oracle job's execution time.

## 7. Rollback Procedure

In the event of issues with the migrated job, follow these rollback steps:

**A. Immediate Action (If issues detected post-deployment but pre-go-live):**

1.  **Disable DAG:** In the Airflow UI, immediately disable the `dw_bert_ausd_bp_ta_bcp_iccid` DAG to prevent any further scheduled or manual runs.
2.  **Remove DAG File:** Delete the `dags/dw_bert_ausd_bp_ta_bcp_iccid.py` file from the Cloud Composer DAGs folder in Cloud Storage.

**B. If Issues Detected Post-Go-Live (After the migrated job has run in production):**

1.  **Disable Migrated DAG:** In the Airflow UI, immediately disable the `dw_bert_ausd_bp_ta_bcp_iccid` DAG to halt any further execution.
2.  **Re-enable Original Job:** Re-enable the original UC4 job `DW.BERT_AUSD_BP_TA_BCP_ICCID` in the legacy environment to resume normal business operations using the proven system.
3.  **Data Restoration (if necessary):**
    *   If the `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` table in BigQuery was corrupted or incorrectly populated by the migrated DAG, it may need to be restored. This could involve:
        *   Restoring the table from a BigQuery snapshot or a previous backup.
        *   Manually correcting the data if the corruption is limited.
        *   Re-running the original Oracle job to populate the BigQuery table if a direct data transfer mechanism is in place.
    *   If the original Oracle `sof$ta_bcp_iccid` table was also affected (unlikely if the migration was a cut-over), restore it from its last good backup.
4.  **Root Cause Analysis & Fix:**
    *   Thoroughly investigate the cause of the failure in the migrated DAG.
    *   Implement the necessary code fixes in `dw_bert_ausd_bp_ta_bcp_iccid.py`.
    *   Perform comprehensive testing in a development/UAT environment.
5.  **Re-migration:** Once the fix is validated and approved, follow the "Manual Steps Before Go-Live" and "Validation" procedures again to redeploy the corrected DAG.