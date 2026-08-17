# Migration Notes: DW.DWH_ABTN_SMART_KUBI

This document provides comprehensive migration notes for transitioning the UC4 job **`DW.DWH_ABTN_SMART_KUBI`** and its associated shell wrappers, environment configurations, and database scripts to Apache Airflow (Google Cloud Composer) and Google Cloud BigQuery.

---

## 1. Summary

The legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` was an active UNIX job (`JOBS_UNIX`) running on host `dwhdwh1p` under the login `DW.UNIX.ISTNS`. Its primary purpose was to execute an Oracle PL/SQL script (`d_abtn_x_smart_kubi.sql`) to aggregate access data and populate a temporary reporting table (`DWH$TA_T_SMART_KUBI`). 

This job, along with its entire supporting shell utility framework (`.dw_init`, `r_sqlscript`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh`), has been migrated to **Google Cloud Platform (GCP)**. 
* **Orchestration**: Apache Airflow / Google Cloud Composer.
* **Database / Execution Engine**: Google Cloud BigQuery.
* **Storage**: Google Cloud Storage (GCS) for scripts, logs, and environment configurations.

---

## 2. Generated Artifacts

The migration process has generated the following modular artifacts, preserving the exact directory structure and functional relationships of the legacy environment:

| Generated File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dw_dwh_abtn_smart_kubi_dag.py` | Python (Airflow) | The master Airflow DAG. It calculates the reporting month (`MONATSID`) dynamically using a Python Jinja macro and triggers the execution wrapper. |
| `local/home/gurunathan_t/kubi/dw_init.py` | Python | Replaces `.dw_init`. Initializes environment variables, maps legacy directory paths to GCS bucket paths, and configures global constants. |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | BigQuery SQL | Replaces the Oracle PL/SQL script. Uses BigQuery standard SQL scripting, transaction blocks, and optimized date-range filters for partition pruning. |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Python | Replaces `f_alis_msgerr.ksh`. Provides operational logging, error trapping, and status tracking (OK/Aborted) in the metadata database. |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Python | Replaces `h_alis_sqlplus.ksh`. Uses the native Google Cloud BigQuery client to execute SQL scripts, handling positional parameter substitution (e.g., `&1`, `&2`). |
| `local/home/gurunathan_t/kubi/r_sqlscript.py` | Python | Replaces `r_sqlscript`. Serves as the main execution wrapper, parsing command-line arguments, resolving relative paths, and handling error trapping. |

---

## 3. Key Design Decisions

### 3.1. BigQuery Scripting for Core Database Logic
Instead of wrapping the entire SQL transformation inside a Python script, the PL/SQL block was converted into a native **BigQuery SQL Scripting** block. This preserves the transactional integrity (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`, `EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION`) and procedural flow control directly within the data warehouse, maximizing execution performance.

### 3.2. Dynamic Date Calculation at the Orchestration Layer
The legacy KSH script calculated the reporting month (`MONATSID`) dynamically based on the execution day (if day < 15, use the previous month; otherwise, use the current month). This logic has been moved to a Python macro within the Airflow DAG:
```python
def get_monatsid(logical_date):
    if logical_date.day < 15:
        first_of_this_month = logical_date.replace(day=1)
        prev_month_date = first_of_this_month - timedelta(days=1)
        return prev_month_date.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")
```
This ensures that the date logic is evaluated at orchestration runtime and passed cleanly as a parameter to the downstream SQL script.

### 3.3. ANSI Joins and Partition Pruning Optimization
* **Outer Joins**: Oracle-specific outer join syntax `(+)` was refactored to standard ANSI `LEFT OUTER JOIN` statements.
* **Partition Pruning**: The legacy query referenced a static partition explicitly: `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, this was refactored to use standard date-range filters on the partition column `gueltigkeitszeitpunkt`:
  ```sql
  WHERE fact.gueltigkeitszeitpunkt >= l_monats_start_date
    AND fact.gueltigkeitszeitpunkt < l_monats_end_date
  ```
  This allows BigQuery's query optimizer to perform automatic partition pruning, reducing slot usage and query costs.

### 3.4. Modular Python Architecture
To resolve disjointed implementations, the migrated Python scripts (`r_sqlscript.py`, `h_alis_sqlplus.py`, `f_alis_msgerr.py`) natively import one another. `h_alis_sqlplus.py` utilizes the native `google-cloud-bigquery` client library to execute queries, establishing a consistent, unified execution pipeline.

### 3.5. Verbatim German Language Logging
To comply with strict output/print literal rules, all original German logging statements, warnings, and error messages have been retained character-for-character (e.g., `"Rufe SQL*PLUS auf mit folgenden Einstellungen"`, `"!FEHLER gemeldet!"`, `"Fehler in .dw_init:"`).

### 3.6. Correction of Legacy Path Validation Bug
The legacy KSH script `r_sqlscript` contained a logical bug where it raised error `198` if the target SQL file *did* exist:
```bash
if [ -f "$l_DBskript" ] # Legacy bug: triggered error if file existed
```
This has been corrected in `r_sqlscript.py` to verify that the file does *not* exist before raising the error:
```python
if not os.path.exists(l_DBskript):
    # Raises error 198 appropriately
```

---

## 4. Manual Steps Before Go-Live

Before deploying the migrated pipeline to production, the following manual setup steps must be completed:

### 4.1. BigQuery Schema and Dataset Creation
1. Create the target BigQuery dataset (e.g., `dwh_dataset`) in your designated processing region (e.g., `EU` or `US`).
2. Create the target and source tables with schemas matching the legacy Oracle definitions:
   * `dwh_dataset.ta_t_smart_kubi` (Target table)
   * `dwh_dataset.ta_f_d1_twvv_tn` (Source fact table — **Must be partitioned by `gueltigkeitszeitpunkt`**)
   * `dwh_dataset.bl_d_tarif` (Source dimension table)
   * `dwh_dataset.vi_l_map_fa_tarif` (Source mapping view)
   * `dwh_dataset.ta_c_vertrag` (Source contract table)

### 4.2. Metadata / Audit Logging Setup
If the operational metadata tracking system (`BERT_MELDUNG` package) is still hosted in Oracle, ensure that the database connection string is configured. If the metadata tracking is being migrated to BigQuery, the `f_alis_msgerr.py` module must be updated to target equivalent BigQuery logging tables.

### 4.3. IAM and Permissions
Ensure the Google Cloud Composer / Airflow worker service account has the following IAM roles:
* **`BigQuery Data Editor`** on the target datasets.
* **`BigQuery Job User`** on the GCP project.
* **`Storage Object Viewer`** (and `Storage Object Creator` if writing logs to GCS) on the GCS bucket hosting the scripts.

### 4.4. Airflow Variables and Environment Variables
Configure the following Airflow Variables in the Cloud Composer environment:
* `GCP_PROJECT`: The GCP Project ID.
* `GCS_BUCKET`: The GCS bucket name where the migrated Python and SQL scripts are stored.
* `BQ_LOCATION`: The BigQuery processing region (e.g., `EU` or `US`).
* `DW_ORAUSER`: (If applicable) The connection string for the Oracle metadata database.

### 4.5. Scheduling and Integration
The DAG is currently configured with `schedule=None` (standalone/manual). If this job needs to be triggered as part of a larger monthly schedule (e.g., legacy parent workflow `DW.DWH_SMART_KUBI_ZUGANG_MONATLICH_JP`), integrate it using an `ExternalTaskSensor` or a `TriggerDagRunOperator` in the parent DAG.

---

## 5. Known Gaps & Unresolved References

### 5.1. Metadata DB Co-existence (Oracle vs. BigQuery)
The migrated `f_alis_msgerr.py` module currently uses `oracledb` to call the `BERT_MELDUNG` PL/SQL package in Oracle. If the metadata tracking database is migrated to BigQuery, `f_alis_msgerr.py` must undergo a secondary refactoring pass to replace the `oracledb` calls with native BigQuery `INSERT` statements.

### 5.2. Retired Legacy Includes
The legacy includes `DW.HOLE_PFAD` and `DW.LESE_LOG` were flagged during human review as unnecessary for the cloud environment and have been retired. No Python stubs or imports are required for these.

---

## 6. Validation

To validate the migration, perform the following test procedure:

### 6.1. Test Execution
1. Upload the migrated Python modules (`dw_init.py`, `f_alis_msgerr.py`, `h_alis_sqlplus.py`, `r_sqlscript.py`) and the SQL script (`d_abtn_x_smart_kubi.sql`) to the designated GCS bucket paths.
2. Place the Airflow DAG `dw_dwh_abtn_smart_kubi_dag.py` into the Composer `dags/` folder.
3. Trigger the DAG manually in the Airflow UI with a specific logical date (e.g., `2015-09-18`).

### 6.2. Definition of "Passing"
The test is considered successful ("passing") if:
1. The Airflow DAG run completes with a `SUCCESS` status.
2. The Airflow task logs display the verbatim German output:
   ```
   Berichtsmonat: 201508
   ```
   *(Since the execution day is the 18th, which is >= 15, the macro correctly targets the previous month, August 2015).*
3. The BigQuery table `dwh_dataset.ta_t_smart_kubi` is successfully truncated and populated with aggregated records.
4. The execution log file generated in GCS contains the verbatim string:
   ```
   X rows inserted in DWH$TA_T_SMART_KUBI
   ```
   *(where X is the number of rows inserted).*

---

## 7. Rollback Procedure

In the event of an unforeseen failure or data inconsistency during go-live, execute the following rollback steps:

1. **Pause the Airflow DAG**: In the Google Cloud Composer UI, locate the DAG `dw_dwh_abtn_smart_kubi` and toggle it to **Paused**.
2. **Clean Target Tables**: If necessary, clear any partially written or corrupted data in the BigQuery target table:
   ```sql
   TRUNCATE TABLE dwh_dataset.ta_t_smart_kubi;
   ```
3. **Re-enable Legacy UC4 Job**: In the UC4 client, locate the job `DW.DWH_ABTN_SMART_KUBI` and set its status to **Active** (`active=1`).
4. **Verify Legacy Database**: Ensure the legacy Oracle database and its associated tables (`DWH$TA_T_SMART_KUBI`) are intact and accessible.
5. **Resume Legacy Scheduling**: Re-integrate the job into the legacy scheduler to resume normal operations.