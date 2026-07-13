# Migration Notes

## 1. Summary
This document details the migration of the UC4 Job Plan (`JOBP`) workflow **`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP`** to **Google Cloud Composer (Apache Airflow)**. 

The legacy workload coordinates, serializes, and synchronizes the daily ingestion, consolidation, and export of KEK and master data (Stamm) within the DWH IKDB ecosystem. The target architecture replaces the legacy UC4 orchestration engine and KornShell wrappers with an Airflow DAG structure utilizing Google Cloud Storage (GCS) and BigQuery for data transformation, storage, and export.

---

## 2. Generated Artifacts
The migration process has generated the following target files:

| Target File Path | Language | Role |
| :--- | :--- | :--- |
| `dags/dw_dwh_ikdb_stamm_kek_taeglich_jp.py` | Python | **Parent Orchestrator DAG**: Coordinates the sequential execution and parallel downstream branches of the sub-workflows. |
| `dags/dw_dwh_ikdb_export_stamm_taeglich_jp.py` | Python | **Downstream Orchestrator DAG**: Triggers the BigQuery Master Data (Stamm) export tasks. |
| `scripts/r_exp_ikdb.py` | Python | **Reusable TaskGroup Utility**: Replaces the legacy `r_exp_ikdb.ksh` script. Manages execution tracking, BigQuery transformations, GCS exports, and metadata logging. |
| `sql/d_ikdb_exp_stamm.sql` | SQL | **BigQuery SQL Query**: Structural placeholder for the missing legacy extraction query. |

---

## 3. Key Design Decisions

### Decoupled Orchestration (TriggerDagRunOperator)
The parent Job Plan (`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP`) acts purely as an orchestrator. To preserve this separation of concerns, child Job Plans are mapped to individual `TriggerDagRunOperator` tasks with `wait_for_completion=True` and `deferrable=True`. This prevents worker slot starvation while waiting for downstream DAGs to complete.

### Concurrency Control (`max_active_runs=1`)
The legacy workflow utilizes a Sync Object (`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP_SYNC`) with the rule `Else="Wait"`. This is natively mapped to the Airflow DAG configuration using `max_active_runs=1`, ensuring that concurrent execution runs of the same DAG are queued rather than executed simultaneously.

### Metadata-Driven Execution Guard
The legacy KornShell script (`r_exp_ikdb.ksh`) checks the `DWTK_MELDUNGEN` table to prevent duplicate runs. This logic is modernized using a `BigQueryValueCheckOperator` that queries a centralized BigQuery tracking table (`metadata_dataset.dwtk_meldungen`) before executing any transformations.

### Encapsulation via TaskGroups
The export logic is encapsulated within a reusable Python function (`execute_ikdb_export`) returning an Airflow `TaskGroup`. This ensures clean DAG visualization and modularity, making it easy to apply the same pattern to other export pipelines (e.g., KEK or Pseudo exports).

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Dataset Creation
Ensure the following BigQuery datasets exist in your target GCP project:
* `metadata_dataset` (For execution tracking)
* `temporary_staging_dataset` (For intermediate export tables)
* `analytical_dataset` (Source tables containing master data)

Create the tracking table in `metadata_dataset`:
```sql
CREATE TABLE IF NOT EXISTS `metadata_dataset.dwtk_meldungen` (
  job_name STRING,
  execution_date DATE,
  status STRING,
  updated_timestamp TIMESTAMP
);
```

### 2. IAM & Permissions
The Cloud Composer Service Account must have the following IAM roles:
* `roles/bigquery.admin` (or fine-grained `DataEditor` and `JobUser` roles on the target datasets)
* `roles/storage.objectAdmin` on the export bucket `gs://dwh-export-ikdb-work`

### 3. Airflow Connections
Configure the following connections in the Airflow UI (**Admin -> Connections**):
* `google_cloud_default`: Google Cloud connection with appropriate project credentials.
* `sftp_default` (or custom connection ID): Required for downstream SFTP transfer DAGs (`*sftp_jp`).

### 4. Scheduling & Upstream Triggers
Because the source Job Plan does not define an internal scheduler, the parent DAG `dw_dwh_ikdb_stamm_kek_taeglich_jp` is configured with `schedule_interval=None`. You must:
* Manually trigger this DAG, or
* Configure an upstream Airflow DAG / Cloud Pub/Sub sensor to trigger it upon daily data arrival.

---

## 5. Known Gaps & Unresolved References

The following components were flagged as **NOT FOUND** during the migration assessment and must be resolved before production deployment:

1. **SQL Extraction Logic (`d_ikdb_exp_stamm.sql`)**:
   * *Status*: Stubbed with a schema placeholder.
   * *Action*: Replace the query in `sql/d_ikdb_exp_stamm.sql` with the actual business logic from the legacy Oracle database.
2. **Downstream Sub-DAGs**:
   * The following DAGs triggered by the parent orchestrator must be migrated and deployed to the environment to prevent execution failures:
     * `dw_dwh_ikdb_info_import_taeglich_jp`
     * `dw_dwh_ikdb_stamm_nachlieferung_export_jp`
     * `dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp`
     * `dw_dwh_ikdb_pseudo_nachlieferung_export_jp`
     * `dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp`
     * `dw_dwh_ikdb_kek_export_taeglich_jp`
     * `dw_dwh_ikdb_kek_nachlieferung_export_jp`
     * `dw_dwh_ikdb_kek_konsolidierung_taeglich_jp`
     * `dw_dwh_ikdb_kek_out_tmd_sftp_jp`
     * `dw_dwh_ikdb_stamm_out_tmd_sftp_jp`
     * `dw_dwh_ikdb_pseudo_out_tmd_sftp_jp`

---

## 6. Validation

### How to Run Tests
1. **DAG Parse Test**: Verify that Airflow can load the DAGs without syntax or import errors:
   ```bash
   python3 dags/dw_dwh_ikdb_stamm_kek_taeglich_jp.py
   python3 dags/dw_dwh_ikdb_export_stamm_taeglich_jp.py
   ```
2. **Unit Testing TaskGroups**: Mock the BigQuery operators to verify that the task dependency chain inside `execute_ikdb_export` behaves as expected.
3. **Dry Run**: Trigger the export sub-DAG `dw_dwh_ikdb_export_stamm_taeglich_jp` manually from the Airflow UI with a test execution date.

### Definition of "Passing"
* The `check_prior_run_registration` task successfully queries the metadata table.
* The transformation query executes and writes to the temporary staging table.
* A CSV file is successfully generated in the target GCS bucket (`gs://dwh-export-ikdb-work/`).
* A new row is appended to `metadata_dataset.dwtk_meldungen` with `status = 'SUCCESS'`.

---

## 7. Rollback Procedure

In the event of a critical failure post-deployment, execute the following steps:

1. **Pause Airflow DAGs**:
   Pause the parent orchestrator and child export DAGs via the Airflow CLI or UI:
   ```bash
   airflow dags pause dw_dwh_ikdb_stamm_kek_taeglich_jp
   airflow dags pause dw_dwh_ikdb_export_stamm_taeglich_jp
   ```
2. **Clean Up Stale Metadata**:
   If a run failed mid-execution and registered an incorrect status, remove the entry from the tracking table:
   ```sql
   DELETE FROM `metadata_dataset.dwtk_meldungen` 
   WHERE job_name = 'EXIS_IKDB_STAMM_R' AND execution_date = '<TARGET_DATE>';
   ```
3. **Revert to Legacy Scheduler**:
   Re-enable the legacy UC4 Job Plan (`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP`) in the UC4 console to resume legacy operations.