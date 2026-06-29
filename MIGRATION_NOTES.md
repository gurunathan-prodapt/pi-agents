# MIGRATION NOTES: `ausd_bp_ta_bpr_beschr`

This document provides the technical migration notes for the job `ausd_bp_ta_bpr_beschr`, detailing the transition from an on-premises Oracle and UC4/Automic environment to Google Cloud Platform (BigQuery and Cloud Composer).

---

## 1. Summary

The job `ausd_bp_ta_bpr_beschr` prepares instantiated base products (e.g., FAX, Data24) and loads their valid descriptions into the master data table `sof$ta_bpr_beschr` for the BERT system (Forderungsscoring / Debt Scoring).

*   **Source Platform:** UC4 / Automic, KornShell (KSH), Oracle SQL (utilizing DB Links `@pcrs1` to remote databases).
*   **Target Platform:** Google Cloud Platform (GCP).
    *   **Orchestration:** Cloud Composer (Apache Airflow).
    *   **Data Warehouse:** BigQuery (Standard SQL).
*   **Migration Tier:** Medium.

---

## 2. Generated Artifacts

The migration process has consolidated the legacy orchestration XML and shell wrappers into two primary cloud-native artifacts:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `sql/d_ausd_bp_ta_bpr_beschr.sql` | BigQuery SQL Script | Contains the target DML/DDL logic. It truncates the target table, performs metadata lookups, and executes an optimized `INNER JOIN` to load active base product descriptions. |
| `dags/ausd_bp_ta_bpr_beschr.py` | Apache Airflow DAG | Replaces the UC4 scheduler and the KSH wrappers (`r_*.ksh`, `k_*.ksh`). It handles scheduling, environment variable configuration, and triggers the BigQuery execution. |

---

## 3. Key Design Decisions

### 3.1 Naming Conventions & Special Characters
*   **Decision:** Oracle table names containing dollar signs (e.g., `sof$ta_bpr_beschr`, `pds$ta_bpr`) were refactored to use underscores (e.g., `sof_ta_bpr_beschr`, `pds_ta_bpr`).
*   **Reasoning:** BigQuery does not support special characters like `$` in table identifiers.

### 3.2 Elimination of Database Links (`@pcrs1`)
*   **Decision:** The Oracle DB Link `@pcrs1` was removed. The query now references local BigQuery staging tables (`pds_ta_bpr` and `pds_ta_care_description`).
*   **Reasoning:** Direct database links are not supported in BigQuery. The source tables are assumed to be pre-replicated into BigQuery via an upstream replication pipeline (e.g., GCP Datastream or Cloud Data Fusion).

### 3.3 SQL Refactoring (Explicit Joins)
*   **Decision:** Legacy Oracle comma-separated joins were refactored into explicit ANSI-compliant `INNER JOIN` syntax.
*   **Reasoning:** Improves query readability, maintainability, and allows the BigQuery query planner to optimize execution paths more effectively.

### 3.4 Preservation of Legacy Audit Lookups
*   **Decision:** The query on `dwtk_meldungen` to capture the `v_datum` variable was preserved using BigQuery scripting (`DECLARE` and `SET`).
*   **Reasoning:** Although the dynamic table-dropping logic associated with this variable was deprecated in Oracle version `10.2.1`, preserving the metadata lookup ensures 100% functional equivalence and prevents breaking downstream audit tasks that scan execution logs.

### 3.5 Idempotency via Truncate-Load
*   **Decision:** The target table `sof_ta_bpr_beschr` is cleared using a `TRUNCATE TABLE` statement before inserting new records.
*   **Reasoning:** This guarantees that the job is fully idempotent (safe to restart at any point in time without duplicating data).

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target environment before activating the Airflow DAG:

### 4.1 Schema and Table Creation
Ensure that the target and staging tables exist in your BigQuery dataset (`isbert_schema`):
*   `sof_ta_bpr_beschr` (Target)
*   `pds_ta_bpr` (Source/Staging)
*   `pds_ta_care_description` (Source/Staging)
*   `dwtk_meldungen` (Audit/Metadata)

### 4.2 IAM & Permissions
The service account running the Cloud Composer workers must have the following IAM roles:
*   `roles/bigquery.dataEditor` on the target dataset (`isbert_schema`).
*   `roles/bigquery.jobUser` on the GCP project.

### 4.3 Airflow Connections & Variables
Configure the following parameters in the Airflow Web UI:
1.  **Connections:**
    *   Ensure `google_cloud_default` is configured with the correct GCP service account credentials or system-assigned managed identity.
2.  **Variables:**
    *   `gcp_project_id`: The target GCP project ID (e.g., `gcp-bert-prod`).
    *   `bq_dataset_isbert`: The target BigQuery dataset name (e.g., `isbert_schema`).

### 4.4 Scheduling
*   The DAG is configured to run daily at **04:00 AM UTC** (`0 4 * * *`). Verify that this window does not conflict with upstream replication pipelines syncing `pds_ta_bpr` and `pds_ta_care_description`.

---

## 5. Known Gaps & Unresolved References

*   **Unused Parameter (`-l` / `Wiederanlaufwert`):** The legacy KornShell controller accepted a restart parameter (`-l Wiederanlaufwert`) to filter records where `DWH_VERTRAG_ID > Wiederanlaufwert`. However, this parameter was not utilized in the original Oracle SQL logic. The Airflow DAG declares it as a placeholder for future compatibility, but it is not passed to the BigQuery SQL script.
*   **Replication Dependency:** This job assumes that the replication of `pds_ta_bpr` and `pds_ta_care_description` from the Oracle PCRS database to BigQuery is completed *before* 04:00 AM. If replication delays occur, this job will process stale data. 
    *   *Follow-up (Redesign B4 Item):* It is highly recommended to replace the time-based schedule (`schedule_interval`) with an event-driven trigger or an Airflow Dataset/Sensor that waits for the replication tasks to complete.

---

## 6. Validation

To verify the correctness of the migrated job, perform the following validation steps:

### 6.1 Dry Run Validation
Run a dry run of the SQL script in the BigQuery console to verify syntax and estimate bytes processed:
```sql
-- Run this in the BigQuery SQL Editor to validate syntax
DECLARE v_datum STRING;
SET v_datum = (SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated)), '19000101') FROM `gcp-bert-prod.isbert_schema.dwtk_meldungen` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE');
```

### 6.2 Data Consistency Audit (A/B Testing)
Compare the output of the legacy Oracle execution with the BigQuery execution. Run the following query in BigQuery after both jobs have run on the same source snapshot:

```sql
-- This query should return 0 rows if the datasets are identical
(
  SELECT BPR_ID, PDS_DESCRIPTION FROM `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`
  EXCEPT DISTINCT
  SELECT BPR_ID, PDS_DESCRIPTION FROM `your_legacy_oracle_export_table`
)
UNION ALL
(
  SELECT BPR_ID, PDS_DESCRIPTION FROM `your_legacy_oracle_export_table`
  EXCEPT DISTINCT
  SELECT BPR_ID, PDS_DESCRIPTION FROM `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`
);
```

### 6.3 Airflow DAG Validation
1.  Upload `ausd_bp_ta_bpr_beschr.py` to the Composer DAGs folder.
2.  Verify that the DAG parses successfully without import errors in the Airflow UI.
3.  Trigger the DAG manually and verify that the task `execute_bpr_transform` completes successfully.

---

## 7. Rollback Procedure

If issues are detected in production after go-live, follow these steps to roll back to the legacy system:

1.  **Pause the Airflow DAG:**
    *   Go to the Airflow UI and toggle the switch for `ausd_bp_ta_bpr_beschr` to **Off**.
2.  **Re-enable Legacy Orchestration:**
    *   In UC4 / Automic, reactivate the job definition `DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml`.
3.  **Verify Oracle Target:**
    *   Ensure the legacy Oracle job runs successfully and repopulates the Oracle table `sof$ta_bpr_beschr`.
4.  **Data Cleanup (Optional):**
    *   If downstream GCP processes rely on the BigQuery table, you can manually truncate the BigQuery target table to prevent downstream systems from reading partially migrated or corrupted data:
        ```sql
        TRUNCATE TABLE `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`;
        ```