# MIGRATION NOTES: DW.DWH_ABTN_SMART_KUBI

## 1. Summary
The legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` has been migrated from an on-premises Automic/UC4 scheduler and Oracle database environment to **Google Cloud Platform (GCP)**. 

The workload has been split into two primary layers:
*   **Orchestration**: Managed by **Cloud Composer (Apache Airflow)**, which handles scheduling, dynamic parameter calculation, and task execution.
*   **Data Warehousing & Processing**: Managed by **Google BigQuery**, which executes the core data transformation and aggregation logic.

The legacy job was responsible for calculating a reporting month (`MONATSID`) based on the execution date, truncating a temporary target table (`DWH$TA_T_SMART_KUBI`), and performing a high-volume aggregation insert from a partitioned contract transaction table and related dimensions.

---

## 2. Generated Artifacts
The migration process generated the following files, each playing a specific role in the target architecture:

| Generated File | Path / Location | Role / Description |
| :--- | :--- | :--- |
| **Airflow DAG** | `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py` | Orchestrates the workflow. It dynamically calculates the `MONATSID` parameter using Airflow/Jinja macros, retrieves the SQL query, and triggers the BigQuery execution job. |
| **BigQuery SQL Script** | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | The migrated core business logic. Converted from Oracle PL/SQL to BigQuery Standard SQL Scripting. It handles transactional control, table truncation, and the main aggregation query. |
| **Logging Utility** | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Python translation of the legacy `f_alis_msgerr.ksh` library. Provides status logging, error trapping, and tracking sequence generation via BigQuery stored procedures. |
| **SQL*Plus Helper** | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Python translation of `h_alis_sqlplus.ksh`. Validates script existence/readability and executes SQL scripts (retains legacy subprocess execution compatibility if needed). |
| **SQL Script Runner** | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Python translation of `r_sqlscript`. Resolves SQL script paths, performs parameter substitutions, and executes SQL statements on BigQuery using the Google Cloud Client Library. |

---

## 3. Key Design Decisions

### PL/SQL to BigQuery Scripting Block
The legacy Oracle PL/SQL anonymous block was converted into a native **BigQuery Standard SQL Scripting block** (`DECLARE... BEGIN... EXCEPTION... END`). This approach:
*   Preserves transactional control (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`, `ROLLBACK TRANSACTION`) natively within BigQuery.
*   Keeps the error-handling and rollback logic close to the data layer, minimizing network round-trips between Airflow and BigQuery.
*   Maps Oracle's `SQL%ROWCOUNT` directly to BigQuery's native `@@row_count` system variable.

### ANSI Join Refactoring
The legacy Oracle query heavily utilized implicit outer join syntax (`(+)`). These have been refactored into standard ANSI `LEFT OUTER JOIN` statements. This ensures compatibility with BigQuery and improves query plan optimization and readability.

### Dynamic Parameter Calculation via Airflow
The legacy date logic determined the reporting month (`MONATSID`) based on the execution day:
*   If the execution day is before the 15th, use the previous month (`YYYYMM`).
*   Otherwise, use the current month (`YYYYMM`).

This logic is now handled dynamically within the Airflow DAG using timezone-aware Python/Jinja expressions. This ensures that backfills and manual runs compute the correct date context relative to their logical execution date rather than the actual system time.

### Retirement of `.dw_init`
The legacy environment initialization script `.dw_init` has been **retired**. Local directory paths and Oracle-specific environment variables are obsolete in a serverless BigQuery environment. Global configurations (such as GCP project IDs and dataset names) are now managed natively via Airflow Variables and Connections.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following database, IAM, and orchestration configurations must be completed:

### 1. BigQuery Schema & Dataset Creation
Ensure the target dataset and tables exist in BigQuery.
*   **Target Table**: Create `dwh$ta_t_smart_kubi` with a schema matching the legacy table structure.
*   **Source Tables/Views**: Ensure the following tables/views are populated and accessible:
    *   `dwh$vi_l_map_fa_tarif`
    *   `bl_d_tarif`
    *   `dwh$ta_f_d1_twvv_tn` (Ensure this table is partitioned on `gueltigkeitszeitpunkt` to enable partition pruning).
    *   `dwh$ta_c_vertrag`

### 2. Deploy Logging Stored Procedures
The SQL script and Python utilities call custom metadata logging procedures. You must deploy the BigQuery equivalents of these legacy Oracle procedures within your target dataset:
*   `BERT_MELDUNG_SetzeStatusOk`
*   `BERT_MELDUNG_SetzeStatusAbbruch`
*   `BERT_MELDUNG_Erzeuge_Eintrag`
*   `BERT_MELDUNG_Fehler`
*   `BERT_MELDUNG_SetzeZusatzInfos`
*   `generate_tracking_nr` (Function returning a unique `INT64` sequence).

### 3. IAM & Permissions
The Service Account running the Cloud Composer/Airflow workers must be granted the following IAM roles:
*   **BigQuery Job User** (`roles/bigquery.jobUser`): To run query jobs.
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`): On the target dataset to truncate and insert data.
*   **Storage Object Viewer** (`roles/storage.objectViewer`): On the GCS bucket if SQL scripts are stored in Cloud Storage.

### 4. Airflow Variables & Connections
Configure the following Airflow Variables in the Airflow UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-prod` | The target GCP Project ID. |
| `GCP_REGION` | `europe-west3` | The GCP region for BigQuery execution. |
| `GCS_BUCKET` | `my-composer-bucket` | GCS bucket used for staging or logging. |
| `BQ_DATASET` | `dwh_prod_dataset` | The destination BigQuery dataset. |
| `SQL_FILE_PATH` | `gs://my-composer-bucket/dags/sql/d_abtn_x_smart_kubi.sql` | Path to the migrated SQL script (GCS or local path). |
| `GCP_CONN_ID` | `google_cloud_default` | The Airflow Connection ID for GCP authentication. |

### 5. Scheduling Configuration
The DAG is currently configured with `schedule=None` (externally triggered) to match the legacy standalone extraction. If this job needs to run on a monthly schedule, update the DAG definition:
```python
schedule="0 0 15 * *" # Example: Runs at midnight on the 15th of every month
```

---

## 5. Known Gaps & Unresolved References

### 1. Metadata Logging Stored Procedures
The core SQL script (`d_abtn_x_smart_kubi.sql`) and the logging utility (`f_alis_msgerr.py`) contain calls to `BERT_MELDUNG` stored procedures. These procedures are **unresolved references** within this specific bundle. They must be migrated and compiled in BigQuery prior to executing this job, or the calls must be mocked/disabled if a centralized logging database is not being used.

### 2. Partitioning Semantics
The legacy Oracle query targeted a specific partition: `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, explicit partition targeting in DML is replaced by standard filters in the `WHERE` clause:
```sql
WHERE FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE)) = CAST(l_monats_id AS STRING)
```
If the physical BigQuery table `dwh$ta_f_d1_twvv_tn` is not partitioned on `gueltigkeitszeitpunkt`, BigQuery will perform a full table scan, resulting in high query costs and degraded performance.

### 3. Inverse File Check in `r_sqlscript`
The legacy `r_sqlscript` contained a logical anomaly where it set an error code if the SQL script *did* exist. This has been corrected in `r_sqlscript.py` to fail if the file is *missing*. Verify if this correction aligns with your operational expectations.

---

## 6. Validation

To validate the migration, perform the following testing phases:

### Phase 1: Dry-Run Validation
Execute a dry run of the BigQuery SQL script using the BigQuery Console or CLI to verify syntax and schema validity without incurring query costs:
```sql
-- Enable dry-run in BigQuery console settings or run via bq CLI:
bq query --dry_run --use_legacy_sql=false < d_abtn_x_smart_kubi.sql
```

### Phase 2: Airflow DAG Manual Trigger
1. Navigate to the Airflow UI.
2. Unpause the `dw_dwh_abtn_smart_kubi` DAG.
3. Trigger the DAG manually with a specific logical date configuration to test both date calculation branches:
    *   **Test Case A (Before the 15th)**: Trigger with logical date `2023-10-10`.
        *   *Expected Output*: `Berichtsmonat: 202309` (Previous month).
    *   **Test Case B (On/After the 15th)**: Trigger with logical date `2023-10-20`.
        *   *Expected Output*: `Berichtsmonat: 202310` (Current month).

### Phase 3: Data Verification ("Passing" Criteria)
The migration is considered successful and "passing" when:
1. The Airflow DAG execution completes with a `SUCCESS` status.
2. The Airflow task logs display the exact literal text:
    ```
    Berichtsmonat:  YYYYMM
    ```
3. The BigQuery execution log output displays:
    ```
    X rows inserted in DWH$TA_T_SMART_KUBI
    ```
4. The target table `dwh$ta_t_smart_kubi` contains the aggregated records for the corresponding `monats_id`, and the counts match the legacy Oracle execution results for the same period.

---

## 7. Rollback Procedure

If critical failures are encountered post-go-live, execute the following rollback steps:

### Step 1: Pause the Airflow DAG
Immediately pause the DAG in the Airflow UI to prevent any scheduled or automated executions:
```bash
airflow dags pause dw_dwh_abtn_smart_kubi
```

### Step 2: Restore the Target Table
If a failed run truncated or corrupted the target table `dwh$ta_t_smart_kubi`, restore it to its pre-execution state using BigQuery's time-travel feature (up to 7 days):
```sql
-- Restore the table to its state 1 hour ago
CREATE OR REPLACE TABLE `your_project.your_dataset.dwh$ta_t_smart_kubi`
AS SELECT * FROM `your_project.your_dataset.dwh$ta_t_smart_kubi`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

### Step 3: Re-enable the Legacy Environment
1. Re-enable the legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` in the Automic/UC4 scheduler.
2. Verify that the legacy Oracle database and execution host (`dwhdwh1p`) are active and accepting connections.
3. Execute a manual run of the legacy UC4 job for the target reporting month to ensure data consistency.