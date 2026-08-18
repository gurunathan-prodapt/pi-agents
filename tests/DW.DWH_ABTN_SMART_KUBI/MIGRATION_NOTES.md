# Migration Notes: DW.DWH_ABTN_SMART_KUBI

## 1. Summary
This document details the migration of the legacy UC4 UNIX job `DW.DWH_ABTN_SMART_KUBI` and its associated shell and SQL scripts to Google Cloud Platform (GCP). 

The legacy pipeline was responsible for calculating a dynamic reporting month parameter (`MONATSID`) based on the execution date, truncating a temporary target table, and executing an Oracle PL/SQL block to aggregate and load contract and tariff data.

### Migration Target
* **Orchestration**: Google Cloud Composer (Apache Airflow 2.x)
* **Data Warehouse / Execution Engine**: Google BigQuery
* **Language Layer**: Python 3 (replacing KornShell scripts) and BigQuery Standard SQL Scripting (replacing Oracle PL/SQL)

---

## 2. Generated Artifacts
The migration process has translated the legacy shell scripts, SQL files, and UC4 XML definitions into the following clean, modular Python and SQL artifacts:

| File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dw_dwh_abtn_smart_kubi.py` | Python | **Airflow DAG**: Orchestrates the pipeline. Dynamically calculates the `MONATSID` parameter and triggers the execution wrapper. |
| `kubi/dw_init.py` | Python | **Environment Config**: Replaces `.dw_init`. Maps legacy directory variables to Google Cloud Storage (GCS) buckets and local paths. |
| `kubi/d_abtn_x_smart_kubi.sql` | SQL (BigQuery) | **BigQuery Script**: Replaces the Oracle PL/SQL block. Contains the core ETL logic, including table truncation, standard ANSI joins, and partition-pruned aggregation. |
| `kubi/f_alis_msgerr.py` | Python | **Logging Utility**: Replaces `f_alis_msgerr.ksh`. Manages execution registration, status tracking, and error logging in BigQuery audit tables. |
| `kubi/h_alis_sqlplus.py` | Python | **Execution Helper**: Replaces `h_alis_sqlplus.ksh`. Reads SQL scripts, binds runtime parameters, and executes queries on BigQuery. |
| `kubi/r_sqlscript.py` | Python | **Execution Wrapper**: Replaces `r_sqlscript`. Parses command-line arguments, resolves relative paths, and manages execution traps. |

---

## 3. Key Design Decisions

### Decision 1: Native Python Modules over Shell Subprocesses
* **Approach**: Legacy KornShell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`, `r_sqlscript`) were completely rewritten as native Python 3 modules.
* **Reasoning**: Executing shell scripts via Airflow `BashOperator` introduces brittle subprocess management, environment mismatch risks, and poor error propagation. Native Python modules allow direct integration with the Google Cloud Client Libraries, robust try-except blocks, and clean unit testing.

### Decision 2: BigQuery Scripting Block for PL/SQL Conversion
* **Approach**: The anonymous Oracle PL/SQL block was converted into a BigQuery Standard SQL Scripting block (`DECLARE`, `SET`, `BEGIN...EXCEPTION...END`).
* **Reasoning**: This preserves the procedural logic (variable declarations, dynamic date calculations, and try-catch error handling) natively within BigQuery, minimizing the need to manage transactional logic inside the Python application layer.

### Decision 3: Standard ANSI Joins over Oracle `(+)` Syntax
* **Approach**: All legacy Oracle-specific outer join operators `(+)` were refactored into standard ANSI `LEFT OUTER JOIN` statements.
* **Reasoning**: BigQuery does not support the legacy `(+)` syntax. Explicit ANSI joins improve query readability, maintainability, and execution planning.

### Decision 4: Cost Optimization via Partition Pruning
* **Approach**: Added an explicit partition filter (`_PARTITIONDATE = PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`) to the source table `dwh$ta_f_d1_twvv_tn` query.
* **Reasoning**: BigQuery charges based on the volume of data scanned. Since the source table is highly partitioned, applying an explicit partition filter based on the calculated `MONATSID` prevents full-table scans and significantly reduces query costs.

### Decision 5: Dynamic Parameter Binding
* **Approach**: The Airflow DAG calculates `MONATSID` at runtime and passes it along with the unique execution ID (`EintragsNr`) as query parameters (`@monats_id`, `@eintragsnr`) using BigQuery's `QueryJobConfig`.
* **Reasoning**: This prevents SQL injection risks, avoids hardcoding, and ensures that the SQL script executes against the correct monthly partition.

---

## 4. Manual Steps Before Go-Live

### 4.1. BigQuery Dataset & Schema Creation
Ensure that the target BigQuery dataset exists and contains the required tables. If they do not exist, create them with schemas matching the legacy definitions:
1. **Target Table**: `dwh$ta_t_smart_kubi`
2. **Source Tables/Views**:
   * `dwh$ta_f_d1_twvv_tn` (Must be partitioned by date)
   * `dwh$vi_l_map_fa_tarif`
   * `bl_d_tarif`
   * `dwh$ta_c_vertrag`
3. **Audit/Logging Tables** (Required by `f_alis_msgerr.py`):
   * `BERT_MELDUNG` (Columns: `entry_id`, `job_kennung`, `programm_name`, `log_datei`, `status`, `start_timestamp`, `end_timestamp`, `stichtag`, `timing_info`)
   * `BERT_MELDUNG_ERRORS` (Columns: `entry_id`, `type`, `error_no`, `detail_1`, `detail_2`, `log_timestamp`)

### 4.2. IAM & Permissions
The Service Account running the Cloud Composer Airflow workers must be granted the following IAM roles in the target GCP project:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the GCP project.
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the GCS bucket housing the SQL scripts.

### 4.3. Airflow Variables Configuration
Set the following Airflow Variables in the Composer environment (via Airflow UI -> Admin -> Variables or gcloud CLI):

```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "GCP_REGION": "europe-west3",
  "BQ_DATASET": "your_bigquery_dataset_name",
  "GCS_BUCKET": "your-composer-environment-bucket"
}
```

### 4.4. Scheduling
The DAG is currently configured with `schedule=None` (on-demand/externally triggered) to match the legacy UC4 behavior. If this job needs to run on a time-based schedule, update the `schedule` parameter in `dags/dw_dwh_abtn_smart_kubi.py` to a standard cron expression (e.g., `schedule="0 2 15 * *"` to run on the 15th of every month at 02:00 AM).

---

## 5. Known Gaps & Unresolved References

### 5.1. Empty String vs. NULL Semantics
* **Description**: Oracle natively treats empty strings (`''`) as `NULL`. BigQuery distinguishes between them.
* **Mitigation**: The migrated SQL script uses explicit checks: `CASE WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN ...`. 
* **Follow-up**: Validate during testing that source data containing empty strings behaves identically to the legacy system.

### 5.2. Partitioning Column Verification
* **Description**: The partition filter in `d_abtn_x_smart_kubi.sql` assumes that `dwh$ta_f_d1_twvv_tn` is partitioned on a date column and that the partition format aligns with `_PARTITIONDATE`.
* **Follow-up**: Verify the physical partitioning column of `dwh$ta_f_d1_twvv_tn` in BigQuery. If it is partitioned on a custom `TIMESTAMP` or `DATE` column (e.g., `gueltigkeitszeitpunkt`), update the partition filter in the SQL file accordingly.

### 5.3. Legacy Oracle Home References
* **Description**: `dw_init.py` retains fallback checks for legacy Oracle directories to prevent key errors in downstream scripts.
* **Follow-up**: Once all dependent pipelines are fully migrated and verified to be Oracle-free, these legacy paths and variables can be safely deprecated and removed.

---

## 6. Validation

### 6.1. Unit Testing Python Modules
Run unit tests on the migrated Python utility modules to verify path resolution, parameter parsing, and logging:

```bash
# Navigate to the migration directory
cd local/home/gurunathan_t/kubi/

# Run pytest (ensure google-cloud-bigquery and pytest are installed)
pytest test_f_alis_msgerr.py test_r_sqlscript.py
```

### 6.2. BigQuery SQL Dry-Run
Validate the syntax and partition scanning of the migrated SQL script using the BigQuery CLI:

```bash
bq query --use_legacy_sql=false --dry_run \
  --parameter=monats_id:INT64:202310 \
  --parameter=eintragsnr:INT64:12345 \
  < d_abtn_x_smart_kubi.sql
```
*A successful dry-run confirms syntax validity and provides an estimate of bytes scanned.*

### 6.3. End-to-End Integration Test
1. Upload the DAG file `dw_dwh_abtn_smart_kubi.py` to the Composer DAGs folder.
2. Upload the utility scripts and SQL file to the designated GCS bucket paths.
3. Trigger the DAG manually from the Airflow UI.
4. **Criteria for "Passing"**:
   * The Airflow DAG run completes with a `SUCCESS` status.
   * The target table `dwh$ta_t_smart_kubi` is truncated and populated with aggregated rows.
   * A new row is inserted into `BERT_MELDUNG` with `status = 'OK'`.
   * No error records are written to `BERT_MELDUNG_ERRORS`.
   * Airflow task logs output the exact German literal: `Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet`.

---

## 7. Rollback Procedure

In the event of an execution failure or data anomaly post-deployment, follow these steps to roll back to the legacy environment:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the switch for `dw_dwh_abtn_smart_kubi` to **Off** (Paused).
2. **Restore Target Table (Optional)**:
   If the target table `dwh$ta_t_smart_kubi` was corrupted by a failed run, restore it to its pre-migration state using BigQuery's table snapshot restore feature:
   ```sql
   CREATE OR REPLACE TABLE `your_project.your_dataset.dwh$ta_t_smart_kubi`
   AS SELECT * FROM `your_project.your_dataset.dwh$ta_t_smart_kubi`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Re-enable Legacy UC4 Job**:
   Log in to the UC4/Automic interface, locate the job `DW.DWH_ABTN_SMART_KUBI`, and set the active flag back to `1` (Active).
4. **Investigate Logs**:
   Review the Airflow task logs and the `BERT_MELDUNG_ERRORS` table in BigQuery to identify the root cause of the failure.