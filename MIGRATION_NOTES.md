# MIGRATION_NOTES.md — DW.BERT_AUSD_BP_TA_MSISDN_HIS

This document provides comprehensive migration notes for transitioning the historical tracking of MSISDNs associated with basic product tariff agreements from the legacy Oracle and UC4 environment to Google Cloud.

---

## 1. Summary

The legacy data pipeline `DW.BERT_AUSD_BP_TA_MSISDN_HIS` has been migrated from an on-premises Oracle database and Automic UC4 orchestration environment to a modern, cloud-native architecture on Google Cloud Platform (GCP).

*   **Source Platform**: Oracle Database, Automic UC4 Scheduler, KornShell (KSH) wrapper scripts (`r_ausd_bp_ta_msisdn_his.ksh`, `k_ausd_bp_ta_msisdn_his.ksh`), and PL/SQL (`d_ausd_bp_ta_msisdn_his.sql`).
*   **Target Platform**: Google Cloud Platform (GCP)
    *   **Orchestration**: Cloud Composer (Apache Airflow 2.x DAG)
    *   **Data Warehouse & Compute**: BigQuery (Standard SQL Scripting)
    *   **Configuration Management**: JSON-based environment configuration
    *   **Optional Transformation Layer**: Dataform (SQLX) for declarative modeling

---

## 2. Generated Artifacts

The migration process has consolidated five legacy files into three clean, maintainable target assets:

| Target Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `definitions/d_ausd_bp_ta_msisdn_his.sqlx` | Dataform / BigQuery SQL | Core transformation logic. Declares variables, calculates the dynamic watermark date (`v_datum`), truncates the target table, and inserts active production MSISDNs. |
| `dags/dw_bert_ausd_bp_ta_msisdn_his.py` | Python (Airflow DAG) | Orchestrates the pipeline execution. Replaces UC4 scheduling and KSH wrapper scripts. Uses `BigQueryInsertJobOperator` to execute the SQL logic. |
| `config/env_config.json` | JSON | Externalizes environment variables, table names, and scheduling parameters to ensure environment parity (Dev/Test/Prod). |

---

## 3. Key Design Decisions

### 3.1 Consolidation of Shell Wrappers into Airflow
The legacy KornShell scripts (`r_*.ksh` and `k_*.ksh`) were primarily responsible for environment initialization (`.dw_init`), parameter parsing, logging, and error handling. 
*   **Decision**: These wrappers were completely retired. Their orchestration, parameter injection, and error-handling responsibilities have been absorbed directly by the Airflow DAG and its native logging/alerting mechanisms. This reduces operational complexity and eliminates the overhead of maintaining virtual machines or containers just to run shell wrappers.

### 3.2 Explicit Type Casting during Concatenation
In the legacy Oracle PL/SQL script, the MSISDN was constructed using string concatenation: `cn1.cc||cn1.ndc||cn1.sn`.
*   **Decision**: In BigQuery, concatenating non-string types or handling potential `NULL` values can lead to unexpected results or query failures. The migrated SQL explicitly casts each component to `STRING` inside a `CONCAT()` function: `CONCAT(CAST(cn1.cc AS STRING), CAST(cn1.ndc AS STRING), CAST(cn1.sn AS STRING))`.

### 3.3 Removal of Optimizer Hints
The legacy Oracle script contained performance hints: `/*+ full(cn1) parallel(cn1,4) */`.
*   **Decision**: These hints were stripped out. BigQuery is a serverless, columnar data warehouse that automatically manages query execution plans, scaling, and parallelism. Manual execution hints are obsolete and unsupported in BigQuery Standard SQL.

### 3.4 Dynamic Watermark Calculation
The pipeline relies on a dynamic date watermark (`v_datum`) derived from `dwtk_meldungen`.
*   **Decision**: This logic is kept inside the BigQuery SQL script using `DECLARE` and `SET` statements. This ensures that the watermark is calculated atomically within the same database session as the transaction, minimizing latency and avoiding round-trips between Airflow and BigQuery.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment to the production environment, the following manual setup steps must be completed:

### 4.1 Schema & Dataset Creation
Ensure that the target BigQuery datasets exist in the target project (`gcp-prod-dwh-project`) and region (e.g., `EU`):
1.  **Source Dataset**: `isbert_schema_prod`
2.  **Target Dataset**: `sof_dataset`
3.  **Target Table**: Create the target table `sof$ta_msisdn_his` if it does not already exist, matching the schema of the legacy table:
    ```sql
    CREATE TABLE IF NOT EXISTS `gcp-prod-dwh-project.sof_dataset.sof$ta_msisdn_his` (
      BPRI_COM_ID STRING, -- Match source type (e.g., INT64 or STRING)
      MSISDN STRING,
      CALLNUMBER_ROLE_ID INT64,
      VALID_TO DATE
    );
    ```

### 4.2 IAM & Permissions
The service account running the Cloud Composer worker nodes (or the specific Airflow connection `google_cloud_default`) must be granted the following IAM roles:
*   `roles/bigquery.jobUser` on the GCP project.
*   `roles/bigquery.dataViewer` on the source dataset `isbert_schema_prod`.
*   `roles/bigquery.dataEditor` on the target dataset `sof_dataset`.

### 4.3 Connection Strings & Secrets
*   Verify that the Airflow connection `google_cloud_default` is configured correctly in Cloud Composer.
*   If executing in a non-production environment, update the Airflow Environment Variable `BQ_LOCATION` (e.g., `US` or `EU`) and `GCP_CONN_ID` if a custom connection is used.

### 4.4 Upstream Data Replication (Critical)
The legacy script queried `pds$ta_callnumber@pcrs1` via an Oracle DB Link. 
*   **Action**: Confirm that the replication pipeline (e.g., Datastream, Fivetran, or an equivalent ELT process) is actively syncing the upstream table `pds$ta_callnumber` from the source system into `gcp-prod-dwh-project.isbert_schema_prod.pds$ta_callnumber` before enabling this DAG.

### 4.5 Scheduling & Airflow Variables
1.  Upload `dw_bert_ausd_bp_ta_msisdn_his.py` to the Composer DAGs folder (`gs://<composer-bucket>/dags/`).
2.  Upload `env_config.json` to the configuration directory or import its values into the Airflow Variable store if configuration management is centralized.
3.  Keep the DAG turned **OFF** (paused) in the Airflow UI until the validation phase is complete.

---

## 5. Known Gaps & Unresolved References

*   **Database Link (`@pcrs1`)**: The legacy DB link reference has been hardcoded to point to the local BigQuery dataset `isbert_schema_prod`. This assumes that the table `pds$ta_callnumber` is fully replicated and up-to-date. Any latency in the replication pipeline will directly affect the accuracy of the historical MSISDN capture.
*   **Redesign (B4) Items**: 
    *   *Table Partitioning/Clustering*: The target table `sof$ta_msisdn_his` is currently truncated and fully reloaded. If this table grows significantly over time, consider redesigning it to use ingestion-time partitioning or clustering on `BPRI_COM_ID` to optimize downstream query costs.
    *   *Dataform Integration*: While a `.sqlx` file has been prepared, the Airflow DAG currently executes the raw SQL string. Transitioning the execution entirely to Dataform (via the `DataformCreateCompilationResultOperator` and `DataformWriteActionOperator`) is recommended for long-term schema and dependency management.

---

## 6. Validation

To validate the migration and verify that the target table is populated correctly:

### 6.1 Dry-Run Validation
Run a dry-run of the SQL script in the BigQuery Console to verify syntax and estimate bytes scanned:
```sql
-- Run in BigQuery Console to validate syntax
DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING;
SET v_datum = '20260421'; -- Mocked date
```

### 6.2 DAG Execution Test
1.  In the Airflow UI, unpause the DAG `dw_bert_ausd_bp_ta_msisdn_his`.
2.  Trigger a manual run of the DAG.
3.  Monitor the task `execute_msisdn_his_logic` and ensure it completes successfully.
4.  Check the Airflow task logs to verify that the query executed without errors.

### 6.3 Data Reconciliation (Passing Criteria)
Run the following reconciliation queries on both the legacy Oracle database and BigQuery to verify data integrity:

*   **Row Count Validation**:
    ```sql
    -- BigQuery
    SELECT COUNT(*) FROM `gcp-prod-dwh-project.sof_dataset.sof$ta_msisdn_his`;
    
    -- Oracle
    SELECT COUNT(*) FROM sof$ta_msisdn_his;
    ```
    *The row counts must match exactly (assuming identical source watermark states).*

*   **Null Value Check**:
    ```sql
    SELECT COUNT(*) FROM `gcp-prod-dwh-project.sof_dataset.sof$ta_msisdn_his` WHERE MSISDN IS NULL;
    ```
    *This count must be `0`.*

---

## 7. Rollback Procedure

If critical issues are discovered in production post-go-live, execute the following steps to roll back to the legacy system:

1.  **Pause the Airflow DAG**: Go to the Airflow UI and pause the DAG `dw_bert_ausd_bp_ta_msisdn_his` to prevent further scheduled executions.
2.  **Re-enable Legacy Scheduler**: Reactivate the UC4 job `DW.BERT_AUSD_BP_TA_MSISDN_HIS` in the Automic scheduler.
3.  **Verify Legacy Execution**: Manually trigger the legacy UC4 job and verify that it successfully connects to the Oracle database, processes the data, and updates the legacy target table.
4.  **Document the Incident**: Log the failure details, including error messages from Airflow/BigQuery, and assign them to the data engineering team for remediation before attempting another go-live.