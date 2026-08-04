# Migration Notes: DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS

This document details the migration of the weekly customer master-data address reconciliation job from UC4 (Automic) and an Oracle/Unix environment to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary

The legacy job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` was a standalone UC4 Unix job that executed a KornShell script (`r_abgl_kunde_woech.ksh`) to perform a weekly address reconciliation of customer master data. It compared customer records in the `DWH_KERN` schema against a reference dataset in the `STAMMDATEN` schema.

This job has been migrated to **GCP**, utilizing **Cloud Composer (Airflow)** for orchestration and **BigQuery** as the data warehouse engine. The shell script's control flow has been refactored into a native Python script, and the Oracle SQL query has been converted to BigQuery Standard SQL.

---

## 2. Generated Artifacts

The migration process generated the following files:

| Artifact Path | Language / Format | Role |
| :--- | :--- | :--- |
| `dags/dw_dwh_kunde_abgl_woechentlich_js.py` | Python (Airflow DAG) | Orchestrates the workflow, calculates the logical execution date, logs progress, and triggers the Python execution wrapper. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.py` | Python 3 | Replaces the legacy KornShell wrapper (`r_abgl_kunde_woech.ksh`). Handles argument parsing, default date calculation, BigQuery client execution, log parsing, and warning outputs. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql` | BigQuery SQL | Contains the SQL query to identify address discrepancies between the customer table and the reference table. *(Note: Generated as a stub in the initial code block; the production-ready SQL is provided in Section 5).* |

---

## 3. Key Design Decisions

### Python over Bash for Script Wrapper
The legacy KornShell script (`r_abgl_kunde_woech.ksh`) contained operational logic (date arithmetic, log parsing, and conditional warning logic based on query output). To align with GCP cloud-native patterns and avoid maintaining legacy shell scripts inside containerized Airflow workers, the script was fully refactored into Python (`r_abgl_kunde_woech.py`).

### Idempotency and Logical Date Alignment
The legacy job calculated the execution date using the system clock (`SYS_DATE("YYYYMMDD")`). To ensure idempotency and support historical backfills, the migrated Airflow DAG maps the execution date to Airflow's logical date macro `{{ ds_nodash }}`.

### Verbatim Log Retention
To prevent breaking downstream log-monitoring, alerting, or auditing tools that parse execution logs, all German log outputs and print statements have been preserved exactly as-is (e.g., `"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen"` and `"Anzahl gefundener Abweichungen: {l_Abweichungen}"`).

### Parameterized BigQuery Queries
The Oracle SQL*Plus substitution variable (`&1` / `&p_Stichtag`) was replaced with a native BigQuery query parameter (`@p_Stichtag`). This prevents SQL injection risks and leverages BigQuery's query plan caching.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the migrated workflow, the following setup steps must be completed:

### Schema and Dataset Creation
Ensure that the target BigQuery datasets and tables exist in your GCP project:
*   Dataset: `DWH_KERN` (Table: `T_KUNDE`)
*   Dataset: `STAMMDATEN` (Table: `T_KUNDE_REFERENZ`)

### IAM & Permissions
The Service Account running the Cloud Composer workers must have the following IAM roles:
*   `roles/bigquery.jobUser` (to run query jobs)
*   `roles/bigquery.dataViewer` on the `DWH_KERN` and `STAMMDATEN` datasets (or `roles/bigquery.dataEditor` if writing results to a table)

### Airflow Variables
Configure the following Airflow Variables in the Cloud Composer environment:

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-prod` | The target GCP Project ID. |
| `GCS_BUCKET` | `us-east1-composer-bucket` | The GCS bucket associated with your Composer environment. |
| `dw_dwh_kunde_abgl_woechentlich_js_script_path` | `/home/airflow/gcs/dags/bin/r_abgl_kunde_woech.py` | The absolute path to the Python wrapper script on the Airflow worker. |

### Scheduling and Triggers
The DAG is currently configured with `schedule=None` (matching the standalone, externally triggered nature of the legacy UC4 job). 
*   If this job should run weekly, update the DAG's `schedule` parameter to a cron expression (e.g., `schedule="0 2 * * 1"` for every Monday at 02:00 AM).
*   If it is triggered by an upstream workflow, configure a `TriggerDagRunOperator` in the parent DAG (`DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`).

---

## 5. Known Gaps & Unresolved References

### SQL File Stub Replacement
The generated code block for `d_abgl_kunde_woech.sql` was output as a placeholder stub. **You must replace the stub file with the following fully translated BigQuery Standard SQL query** before deployment:

```sql
-- DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql
SELECT
  'ABWEICHUNG' AS MARKER,
  k.KUNDE,
  k.NACHNAME,
  k.VORNAME,
  k.PLZ,
  k.ORT,
  k.STRASSE,
  r.PLZ       AS REF_PLZ,
  r.ORT       AS REF_ORT,
  r.STRASSE   AS REF_STRASSE
FROM 
  `DWH_KERN.T_KUNDE` k
JOIN 
  `STAMMDATEN.T_KUNDE_REFERENZ` r
ON 
  r.KUNDE = k.KUNDE
WHERE 
  k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', @p_Stichtag)
  AND (
    COALESCE(k.PLZ, 'x')     != COALESCE(r.PLZ, 'x')
    OR COALESCE(k.ORT, 'x')     != COALESCE(r.ORT, 'x')
    OR COALESCE(k.STRASSE, 'x') != COALESCE(r.STRASSE, 'x')
  )
ORDER BY 
  k.KUNDE;
```

### Date Type Compatibility
In the query above, `k.AKTUALISIERT_AM` is compared to the parsed date parameter. 
*   If `k.AKTUALISIERT_AM` is stored as a `TIMESTAMP` or `DATETIME` in BigQuery, you must cast the parsed date to match:
    ```sql
    k.AKTUALISIERT_AM <= CAST(PARSE_DATE('%Y%m%d', @p_Stichtag) AS TIMESTAMP)
    ```

### Local Ephemeral Storage
The Python script `r_abgl_kunde_woech.py` writes its execution log to a local directory (`DW_DIR_LOG`). In Cloud Composer, local worker storage is ephemeral and will be lost when the worker pod restarts. 
*   *Recommendation:* Modify the script's logging destination to write directly to standard output (which is automatically captured by Cloud Logging) or redirect the log file to a persistent Google Cloud Storage (GCS) bucket path.

---

## 6. Validation

To validate the migration, perform the following steps:

1.  **Upload Artifacts**: Place the DAG file in the `dags/` folder and the Python/SQL scripts in the designated bucket paths.
2.  **Trigger Manually**: In the Airflow UI, locate `dw_dwh_kunde_abgl_woechentlich_js` and trigger it manually using "Trigger DAG w/ config" to specify a logical date (e.g., `{"ds_nodash": "20240101"}`).
3.  **Verify Logs**:
    *   Confirm the `log_start` task prints: `Kundenadressabgleich fuer Lauf 20240101 angestossen`.
    *   Confirm the `execute_r_abgl_kunde_woech` task executes successfully.
    *   Check the task logs for the discrepancy count output: `Anzahl gefundener Abweichungen: <count>`.
4.  **Verify Warning Behavior**: If the test date has known discrepancies, verify that the task logs output a warning message starting with `[W]` to standard error, but the task itself still completes with a `SUCCESS` status (matching legacy behavior).

---

## 7. Rollback Procedure

If critical issues are discovered in production, revert to the legacy system using the following steps:

1.  **Pause the Airflow DAG**: Toggle the DAG switch to **Off** in the Airflow UI to prevent any automated or manual executions.
2.  **Re-enable UC4 Job**: In the Automic (UC4) UI, locate the job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` (or its parent JobPlan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`) and set its status back to **Active**.
3.  **Verify Legacy Environment**: Ensure that the target Unix host (`DWHDWH1P`) and the Oracle database (`DWHP1`) are online and accepting connections.