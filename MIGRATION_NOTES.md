# Migration Notes: DW.BERT_AUSD_BP_TA_MSISDN_HIS

This document provides comprehensive migration notes for transitioning the legacy Automic (UC4) job `DW.BERT_AUSD_BP_TA_MSISDN_HIS` and its associated KornShell wrapper scripts to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy batch job `DW.BERT_AUSD_BP_TA_MSISDN_HIS` has been migrated from an on-premises Oracle and Automic (UC4) environment to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**.

*   **Source Platform:** Automic UC4 Orchestrator, KornShell Wrapper scripts (`r_...ksh` / `k_...ksh`), and Oracle PL/SQL / DML executing over an Oracle Database Link (`@pcrs1` / `@carmen`).
*   **Target Platform:** **Google Cloud Composer (Airflow)** for orchestration and **Google BigQuery** for data warehousing and transformation.
*   **Migration Pattern:** The legacy shell control scripts and Oracle SQL logic have been consolidated into a BigQuery-native SQL workflow. The database link dependency has been eliminated by querying native BigQuery tables populated by upstream ingestion pipelines.

---

## 2. Generated Artifacts

The migration process generated the following key artifacts:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `dataform/definitions/sof_ta_msisdn_his.sqlx` | **Dataform SQLX Script** | Defines the BigQuery transformation logic, including watermark resolution, target table truncation, and incremental-like history population. |
| `dags/dw_bert_ausd_bp_ta_msisdn_his_dag.py` | **Cloud Composer DAG** | Airflow DAG that schedules and executes the BigQuery multi-statement query using the `BigQueryInsertJobOperator`. |

---

## 3. Key Design Decisions

### 3.1 BigQuery-Native Execution
*   **Decision:** Consolidate KornShell wrapper logic (date calculation, parameter parsing) directly into BigQuery SQL using scripting variables (`DECLARE`, `SET`).
*   **Reasoning:** Eliminates the need for intermediate compute resources (such as GCE VMs or Kubernetes Pods) to run shell scripts. Running logic directly inside BigQuery reduces operational overhead, simplifies the architecture, and lowers execution costs.

### 3.2 Elimination of Database Links
*   **Decision:** Replace the Oracle DB Link (`@pcrs1`) with a direct reference to the BigQuery table `pds.ta_callnumber`.
*   **Reasoning:** DB links are highly coupled and perform poorly over WANs. In GCP, upstream ingestion pipelines are expected to sync the source data into the `pds` dataset prior to this job's execution, allowing for high-performance, localized BigQuery joins.

### 3.3 Handling of Oracle Optimizer Hints
*   **Decision:** The Oracle optimizer hint `/*+ full(cn1) parallel(cn1,4) */` was completely omitted.
*   **Reasoning:** BigQuery is a serverless, columnar data warehouse that automatically manages query execution plans and scales compute resources dynamically. Manual parallelization hints are obsolete.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this job in production, the following manual setup steps must be completed:

### 4.1 Schema & Dataset Creation
Ensure the following BigQuery datasets and tables exist in your target GCP project:
1.  **Datasets:**
    *   `isbert_schema`
    *   `pds`
    *   `sof`
2.  **Tables:**
    *   `isbert_schema.dwtk_meldungen` (Must contain historical watermark records with `job_kennung = 'BERT_DROP_TEMP_TABLE'`).
    *   `pds.ta_callnumber` (Must be populated by the upstream ingestion pipeline).
    *   `sof.ta_msisdn_his` (Target table; must match the schema of the legacy `sof$ta_msisdn_his` table).

### 4.2 IAM & Permissions
The Cloud Composer service account must be granted the following IAM roles in the target GCP project:
*   `roles/bigquery.jobUser` (To run BigQuery jobs).
*   `roles/bigquery.dataEditor` on the `sof` dataset (To truncate and insert into the target table).
*   `roles/bigquery.dataViewer` on the `isbert_schema` and `pds` datasets (To read source and watermark tables).

### 4.3 Airflow Variables
Define the following Airflow Variable in your Cloud Composer environment:
*   `gcp_project_id`: The ID of the GCP project where your BigQuery datasets reside.

### 4.4 Scheduling & Dependencies
*   Deactivate the legacy Automic (UC4) job `DW.BERT_AUSD_BP_TA_MSISDN_HIS` to prevent concurrent writes.
*   Ensure that the upstream ingestion pipeline for `pds.ta_callnumber` is scheduled to complete *before* this Airflow DAG starts.

---

## 5. Known Gaps & Unresolved References

*   **Upstream Ingestion Dependency:** This migration assumes that an independent ingestion process continuously replicates `pds$ta_callnumber` from the source Oracle database (`@pcrs1`) to BigQuery (`pds.ta_callnumber`). If this ingestion pipeline is not yet operational, this job will process stale data.
*   **Watermark Synchronization:** The job relies on `isbert_schema.dwtk_meldungen` containing a valid timestamp for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. Ensure that the process updating this watermark table has also been migrated to GCP and runs successfully.

---

## 6. Validation

To validate the migration, execute the following testing steps:

### 6.1 Dry-Run Validation
Run a dry-run of the SQL query in the BigQuery console to verify syntax and estimate bytes scanned:
```sql
DECLARE v_job_name STRING DEFAULT 'BERT_DROP_TEMP_TABLE';
DECLARE v_default_date STRING DEFAULT '19000101';
DECLARE v_datum STRING;
DECLARE v_process_date DATE;

SET v_datum = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), v_default_date)
  FROM `your_project_id.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = v_job_name
);
-- Verify that v_datum resolves to the expected historical date.
SELECT v_datum;
```

### 6.2 DAG Execution Test
1.  Upload `dw_bert_ausd_bp_ta_msisdn_his_dag.py` to the Airflow DAGs folder.
2.  Trigger the DAG manually from the Airflow UI.
3.  Verify that the task `execute_msisdn_history_update` completes with a `SUCCESS` status.

### 6.3 Data Reconciliation (Passing Criteria)
Compare the output of the BigQuery target table with the legacy Oracle table for a given watermark date:
*   **Row Count Check:** Run `SELECT COUNT(*) FROM sof.ta_msisdn_his` in both environments. The counts must match exactly.
*   **Data Integrity Check:** Run a checksum or sample comparison on key columns (`bpri_com_id`, `msisdn`, `valid_to`) to ensure concatenation and date logic match the legacy output.

---

## 7. Rollback Procedure

If critical issues are discovered post-go-live, follow these steps to roll back to the legacy environment:

1.  **Pause the Airflow DAG:**
    *   Go to the Cloud Composer Airflow UI.
    *   Locate `dw_bert_ausd_bp_ta_msisdn_his` and toggle the switch to **Off** (Paused).
2.  **Re-enable Legacy Scheduling:**
    *   Log into the Automic (UC4) interface.
    *   Re-activate the job definition `DW.BERT_AUSD_BP_TA_MSISDN_HIS`.
3.  **Verify Legacy Execution:**
    *   Manually trigger the legacy UC4 job for the missed processing window.
    *   Verify that the Oracle table `sof$ta_msisdn_his` is successfully populated.