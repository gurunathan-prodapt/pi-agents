# Migration Notes: `ausd_bp_ta_bpr_optionen`

This document provides the technical migration notes for transitioning the legacy job `ausd_bp_ta_bpr_optionen` to Google Cloud Platform (GCP).

---

## 1. Summary
The legacy job `ausd_bp_ta_bpr_optionen` has been migrated from an on-premises environment to Google Cloud Platform (GCP). 

*   **Source Platform**: UC4 / Automic scheduler executing KornShell (`.ksh`) wrapper scripts that run Oracle SQL*Plus database operations.
*   **Target Platform**: Google Cloud Platform (GCP).
    *   **Orchestration**: Cloud Composer (Apache Airflow 2.x).
    *   **Data Warehouse**: Google BigQuery (Standard SQL).
*   **Functional Scope**: This job is a core data-staging step within the BERT (Stammdaten / Basisprodukt) processing domain. It truncates the target table `sof_ta_bpr_optionen` and reloads it with contract tariff options extracted from the base product instance table `sof_ta_bpr_instance`.

---

## 2. Generated Artifacts
The migration process generated the following files, which must be deployed to the Cloud Composer environment:

1.  **`dags/dw_bert_ausd_bp_ta_bpr_optionen.py`**
    *   **Role**: Apache Airflow DAG file. It orchestrates the execution flow, manages environment configurations via Airflow Variables, and triggers the BigQuery SQL execution.
2.  **`dags/sql/d_ausd_bp_ta_bpr_optionen.sql`**
    *   **Role**: BigQuery Standard SQL script. It contains the operational logic to declare audit variables, truncate the target table, and perform the bulk insert from the source table.

---

## 3. Key Design Decisions

*   **Direct BigQuery Execution (SQL-First)**: While the initial UC4 analysis suggested a generic Dataproc/PySpark template, the core logic is entirely database-centric (TRUNCATE + INSERT). To minimize execution overhead, complexity, and operational costs, the logic was refactored to run directly on BigQuery using the native `BigQueryExecuteQueryOperator`.
*   **Table Name Normalization**: Legacy Oracle table names containing the special character `$` (e.g., `sof$ta_bpr_optionen` and `sof$ta_bpr_instance`) have been normalized to use underscores (e.g., `sof_ta_bpr_optionen` and `sof_ta_bpr_instance`) to comply with BigQuery naming standards and prevent syntax issues.
*   **Preservation of Audit Logic**: The legacy SQL script calculated an audit variable `v_datum` from the `dwtk_meldungen` table. Although this variable is not actively used in the subsequent `INSERT` statement, this logic was preserved in the BigQuery SQL script (`DECLARE` and `SET` statements) to maintain functional parity and lineage tracking.
*   **Removal of Database Hints and Commits**: Oracle-specific optimizer hints (e.g., `/*+ full(bp) parallel(bp,4) */`) and explicit `COMMIT` statements were removed. BigQuery automatically optimizes query execution plans and guarantees atomic statement execution.

---

## 4. Manual Steps Before Go-Live

Before enabling and running the migrated pipeline in production, the following setup steps must be completed:

### Schema & Dataset Creation
Ensure that the target BigQuery dataset and tables exist in the target GCP project. If they do not exist, create them using the following schemas:

```sql
-- Target Table Schema
CREATE TABLE IF NOT EXISTS `your_project.isbert_schema.sof_ta_bpr_optionen` (
  cntrct_id STRING, -- Replace with actual legacy data types (e.g., INT64 or STRING)
  bpr_id STRING
);
```

### IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles on the target BigQuery dataset:
*   `roles/bigquery.jobUser` (to run BigQuery jobs)
*   `roles/bigquery.dataEditor` (to truncate and insert data into the target table)

### Airflow Variables & Connections
The following Airflow variables must be configured in the Cloud Composer environment (via Airflow UI -> Admin -> Variables):

| Variable Name | Expected Value (Example) | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `prod-gcp-project-123` | The target GCP Project ID. |
| `bq_dataset` | `isbert_schema` | The BigQuery dataset containing the BERT tables. |
| `bq_location` | `EU` | The geographic location of the BigQuery dataset. |
| `gcp_conn_id` | `google_cloud_default` | The Airflow connection ID used to connect to GCP. |

### Scheduling & Upstream Triggers
Because the UC4 export did not contain a parent Job Plan (`JOBP`), the migrated DAG is configured with `schedule_interval=None` (manual/triggered execution). 
*   If this job must run as part of a larger sequence, configure a `TriggerDagRunOperator` in the upstream DAG (e.g., the DAG responsible for populating `sof_ta_bpr_instance`).

---

## 5. Known Gaps & Unresolved References

*   **Unused Audit Variable (`v_datum`)**: The variable `v_datum` is successfully declared and populated from `dwtk_meldungen`, but it is never used in the data loading step. This is a legacy design carryover. 
    *   *Redesign (B4) Recommendation*: In a future sprint, evaluate if the `dwtk_meldungen` query can be completely removed to save execution costs, or if it should be replaced by standard Airflow metadata logging.
*   **Data Type Alignment**: The SQL script assumes that the schemas of `sof_ta_bpr_instance` and `sof_ta_bpr_optionen` are already aligned in BigQuery. Any mismatch in data types (e.g., `INT64` vs `STRING` for `cntrct_id`) will cause a runtime query failure.

---

## 6. Validation

To validate the migration, perform the following testing steps:

### How to Run the Tests
1.  **Dry Run (Syntax Check)**:
    Run the Airflow DAG compiler check locally or in a development Composer environment:
    ```bash
    python dags/dw_bert_ausd_bp_ta_bpr_optionen.py
    ```
2.  **Manual DAG Execution**:
    *   Log in to the Airflow UI.
    *   Locate the DAG `dw_bert_ausd_bp_ta_bpr_optionen`.
    *   Unpause the DAG and click **Trigger DAG**.

### What "Passing" Means
The migration is considered successful and validated if:
*   The Airflow DAG run completes with a `success` status.
*   The BigQuery job logs confirm that `TRUNCATE` and `INSERT` statements executed without errors.
*   **Data Parity Check**: The row count of the target table `sof_ta_bpr_optionen` matches the row count of the source table `sof_ta_bpr_instance` exactly:
    ```sql
    SELECT COUNT(*) FROM `your_project.isbert_schema.sof_ta_bpr_instance`;
    SELECT COUNT(*) FROM `your_project.isbert_schema.sof_ta_bpr_optionen`;
    -- Both counts must be identical.
    ```

---

## 7. Rollback Procedure

If a critical failure occurs during or immediately after go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG**:
    Immediately pause the DAG in the Airflow UI or via the gcloud CLI to prevent further automated executions:
    ```bash
    gcloud composer environments run <env_name> \
        --location <location> \
        dags pause -- dw_bert_ausd_bp_ta_bpr_optionen
    ```
2.  **Restore Target Table State**:
    If the target table was corrupted or incorrectly truncated, restore it to its pre-migration state using BigQuery Time Travel:
    ```sql
    -- Step 1: Drop the corrupted table
    DROP TABLE `your_project.isbert_schema.sof_ta_bpr_optionen`;

    -- Step 2: Restore from a known good timestamp (e.g., 1 hour ago)
    CREATE TABLE `your_project.isbert_schema.sof_ta_bpr_optionen`
    AS SELECT * FROM `your_project.isbert_schema.sof_ta_bpr_optionen`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
3.  **Re-enable Legacy Execution**:
    If a hybrid execution phase is active, re-enable the legacy UC4 job `DW.BERT_AUSD_BP_TA_BPR_OPTIONEN` to resume on-premises processing.