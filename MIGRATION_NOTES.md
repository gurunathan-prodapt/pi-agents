# MIGRATION NOTES: `ausd_bp_ta_cntrct_dist`

This document provides the comprehensive migration notes for transitioning the legacy Oracle/UC4 job `ausd_bp_ta_cntrct_dist` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary
The legacy job `ausd_bp_ta_cntrct_dist` prepares and loads distinct contract IDs from the basis product table (`sof$ta_bpr_basis`) into the target table (`sof$ta_cntrct_dist`). 

*   **Source Platform:** Oracle Database, UC4 Scheduler, Unix Shell Scripts (`ksh`), SQL*Plus.
*   **Target Platform:** Google Cloud Platform (GCP).
    *   **Orchestration:** Cloud Composer (Apache Airflow 2.x).
    *   **Data Warehouse:** BigQuery (Standard SQL).
*   **Migration Scope:** The multi-layered legacy orchestration (UC4 XML $\rightarrow$ wrapper shell $\rightarrow$ kernel shell $\rightarrow$ PL/SQL) has been consolidated into a single, native Airflow DAG that executes optimized BigQuery SQL statements.

---

## 2. Generated Artifacts

The migration process has generated the following target-state artifacts:

### 1. `dags/ausd_bp_ta_cntrct_dist.py`
*   **Role:** Apache Airflow DAG definition file.
*   **Purpose:** Orchestrates the execution of the data transformation. It defines the task execution chain, handles environment-specific variables, and triggers the BigQuery execution using the `BigQueryInsertJobOperator`.

### 2. `gcs/sql/d_ausd_bp_ta_cntrct_dist.sql`
*   **Role:** BigQuery Standard SQL script.
*   **Purpose:** Contains the core data transformation logic. It truncates the target table and populates it with distinct, non-null contract IDs from the source table.

---

## 3. Key Design Decisions

### A. Consolidation of Orchestration Layers
*   **Decision:** The legacy 4-layer execution chain (UC4 $\rightarrow$ `r_*.ksh` $\rightarrow$ `k_*.ksh` $\rightarrow$ `d_*.sql`) was collapsed into a single Airflow DAG.
*   **Reasoning:** Reduces operational complexity, eliminates shell-script maintenance overhead, and leverages native Cloud Composer logging, retries, and alerting.

### B. Sanitization of Special Characters in Identifiers
*   **Decision:** Oracle table names containing the `$` symbol (e.g., `sof$ta_bpr_basis`, `sof$ta_cntrct_dist`) have been renamed to use underscores (e.g., `sof_ta_bpr_basis`, `sof_ta_cntrct_dist`) in BigQuery.
*   **Reasoning:** BigQuery does not support special characters like `$` in standard table identifiers. Standardizing on underscores prevents syntax compilation errors and aligns with GCP naming conventions.

### C. Removal of Obsolete Oracle Features
*   **Decision:** 
    *   The Oracle parallel hint `/*+ parallel(rp,4) */` was removed.
    *   The custom PL/SQL utility package call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(...)` was replaced with a native BigQuery `TRUNCATE TABLE` statement.
*   **Reasoning:** BigQuery automatically manages parallel execution dynamically based on slot availability and query complexity. Custom database utility wrappers are obsolete in a serverless data warehouse environment.

### D. Exclusion of Inactive Legacy Code
*   **Decision:** A large block of commented-out shell logic in `k_ausd_bp_ta_cntrct_dist.ksh` (referencing sorting and merging of files like `cibasis_data24.dat`, `cibasis_data96.dat`, and `cibasis_fax.dat`) was completely excluded from the migration.
*   **Reasoning:** This logic is inactive in the production legacy environment. Migrating it would introduce unnecessary technical debt and maintainability issues.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated pipeline in production, the following setup steps must be completed:

### A. Schema & Dataset Creation
Ensure that the target BigQuery dataset exists and that the source and target tables are defined with compatible schemas:
1.  **Source Table:** `sof_ta_bpr_basis` must exist and contain the column `cntrct_id`.
2.  **Target Table:** `sof_ta_cntrct_dist` must be created.
    ```sql
    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_cntrct_dist` (
        cntrct_id STRING -- Or matching data type of the source column
    );
    ```

### B. IAM & Permissions
The Cloud Composer Service Account must be granted the following IAM roles:
*   `roles/bigquery.dataEditor` on the target dataset.
*   `roles/bigquery.jobUser` on the GCP project running the BigQuery jobs.
*   `roles/storage.objectViewer` on the GCS bucket hosting the SQL scripts (if loaded dynamically from GCS).

### C. Airflow Variables Configuration
The following Airflow variables must be configured in the Cloud Composer environment:

| Variable Name | Expected Value Example | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `my-gcp-prod-project` | The target GCP Project ID. |
| `bq_dataset` | `isbert_dataset` | The target BigQuery dataset name. |
| `bq_location` | `europe-west3` | The geographic location of the BigQuery dataset. |
| `gcp_conn_id` | `google_cloud_default` | The Airflow connection ID used for GCP services. |

### D. Scheduling & Upstream Dependencies
*   The DAG is currently configured with `schedule_interval=None` (manual/ad-hoc).
*   **Action Required:** Align the DAG schedule or trigger mechanism with the upstream job that populates `sof_ta_bpr_basis`. If using Airflow-native scheduling, configure a `Dataset` dependency or an `ExternalTaskSensor`.

---

## 5. Known Gaps & Unresolved References

### A. Downstream System Compatibility
*   **Gap:** Downstream legacy systems or reporting tools may still expect the table name to contain the `$` character (`sof$ta_cntrct_dist`).
*   **Mitigation:** If legacy naming compatibility is strictly required for external tools, create a BigQuery view or an authorized view named `sof$ta_cntrct_dist` (using backticks to escape the special character) pointing to `sof_ta_cntrct_dist`.

### B. Redesign (B4) Items
*   The commented-out file-processing logic for `PoolBasisprodukt` has been permanently discarded. If business requirements change and this file-ingestion logic must be restored, it must be designed as a separate, isolated Cloud Storage-to-BigQuery ingestion pipeline.

---

## 6. Validation

To validate the migration, execute the following test plan:

### A. Execution Test
1.  Upload `ausd_bp_ta_cntrct_dist.py` to the Composer DAGs folder.
2.  Trigger the DAG manually via the Airflow UI or the gcloud CLI:
    ```bash
    gcloud composer environments run <env-name> \
        --location <location> \
        dags trigger -- ausd_bp_ta_cntrct_dist
    ```
3.  Verify that all tasks (`start` $\rightarrow$ `load_contract_distinct` $\rightarrow$ `end`) complete with a `success` status.

### B. Data Reconciliation (Passing Criteria)
Run the following validation queries in BigQuery to verify data integrity:

1.  **Uniqueness Check:** Ensure no duplicate contract IDs exist in the target table.
    ```sql
    SELECT cntrct_id, COUNT(*) 
    FROM `your_project.your_dataset.sof_ta_cntrct_dist` 
    GROUP BY cntrct_id 
    HAVING COUNT(*) > 1;
    -- Expected Result: 0 rows returned.
    ```

2.  **Null Check:** Ensure no null values were loaded.
    ```sql
    SELECT COUNT(*) 
    FROM `your_project.your_dataset.sof_ta_cntrct_dist` 
    WHERE cntrct_id IS NULL;
    -- Expected Result: 0.
    ```

3.  **Row Count Reconciliation:** Compare the target row count with the distinct source row count.
    ```sql
    SELECT 
      (SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_cntrct_dist`) AS target_count,
      (SELECT COUNT(DISTINCT cntrct_id) FROM `your_project.your_dataset.sof_ta_bpr_basis` WHERE cntrct_id IS NOT NULL) AS source_distinct_count;
    -- Expected Result: target_count must equal source_distinct_count.
    ```

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment:

1.  **Pause the Airflow DAG:**
    Immediately pause the DAG in the Airflow UI or via the CLI to prevent further automated executions:
    ```bash
    gcloud composer environments run <env-name> \
        --location <location> \
        dags pause -- ausd_bp_ta_cntrct_dist
    ```
2.  **Re-enable Legacy Execution:**
    If the legacy environment is still active, re-enable the UC4 job `DW.BERT_AUSD_BP_TA_CNTRCT_DIST`.
3.  **Data Restoration (Optional):**
    If the target table `sof_ta_cntrct_dist` was corrupted and needs to be restored to its pre-migration state, use BigQuery Time Travel to restore the table to a point-in-time before the DAG execution:
    ```sql
    CREATE OR REPLACE TABLE `your_project.your_dataset.sof_ta_cntrct_dist` AS
    SELECT * FROM `your_project.your_dataset.sof_ta_cntrct_dist`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```