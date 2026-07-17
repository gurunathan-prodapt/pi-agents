# Migration Notes: DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS

This document provides comprehensive migration notes for the weekly customer address reconciliation job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`, transitioning from a legacy UC4, KornShell (KSH), and Oracle environment to Google Cloud Composer (Airflow) and BigQuery.

---

## 1. Summary

The `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` job has been migrated from its legacy on-premise environment to a modern, cloud-native architecture on Google Cloud Platform (GCP).

* **Source Platform:** UC4 (Scheduler) + KornShell (Wrapper Script) + Oracle Database (SQL Transformation)
* **Target Platform:** Google Cloud Composer (Apache Airflow Orchestration) + Google BigQuery (Serverless Data Warehouse)
* **Migration Pattern:** `UC4+KSH+SQL_MEDIUM` (Cloud Composer + BigQuery Python API)

### Key Improvements:
* **Serverless Execution:** Replaced legacy Oracle database operations with high-performance, scalable BigQuery SQL.
* **Native Python Orchestration:** Eliminated SSH/Bash execution wrappers on remote VMs by migrating shell logic (`r_abgl_kunde_woech.ksh`) into native Python operators running directly within Cloud Composer.
* **Fidelity of Logging:** Retained all original German operational logging and discrepancy-checking logic (`grep -c "^ABWEICHUNG"`) by translating them into native Python structures querying BigQuery result sets.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy components, maintaining absolute folder structure integrity where applicable:

| Source File Path | Target File Path | Role / Description |
| :--- | :--- | :--- |
| `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.xml` | `dags/dw_dwh_kunde_abgl_woechentlich_js.py` | **Airflow DAG:** Orchestrates the weekly workflow, manages scheduling, and resolves runtime variables. |
| `bin/r_abgl_kunde_woech.ksh` | `dags/bin/r_abgl_kunde_woech.py` | **Python Execution Wrapper:** Replaces the KornShell script. Handles date calculations, executes BigQuery queries, counts discrepancies, and outputs legacy logs. |
| `sql/d_abgl_kunde_woech.sql` | `dags/sql/d_abgl_kunde_woech.sql` | **BigQuery SQL:** The translated address reconciliation query, optimized for BigQuery syntax (e.g., `IFNULL`, `PARSE_DATE`). |

---

## 3. Key Design Decisions

### Native Python Wrapper vs. BashOperator
Instead of utilizing a `BashOperator` to execute a shell script on a remote worker, the wrapper logic (`r_abgl_kunde_woech.ksh`) was completely rewritten in Python (`r_abgl_kunde_woech.py`). This eliminates VM management overhead, improves security, and allows native integration with the Google Cloud BigQuery Client SDK.

### In-Memory Discrepancy Counting
The legacy shell script wrote query outputs to a flat file and parsed it using `grep -c "^ABWEICHUNG"`. The migrated Python wrapper executes the query, processes the result rows in-memory (or queries the staging table directly), counts the rows marked with `ABWEICHUNG`, and logs the exact count. This avoids disk I/O and simplifies the execution pipeline.

### Preservation of Legacy Log Messages
To ensure operational continuity and compatibility with legacy log parsers, all console outputs have been preserved verbatim in German:
* `"Kundenadressabgleich fuer Lauf {stichtag} angestossen"`
* `"Starte Adressabgleich Kundenstammdaten fuer Stichtag {stichtag}"`
* `"Anzahl gefundener Abweichungen: {count}"`
* `"Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet"`

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following setup steps must be completed:

### 1. Schema & Dataset Creation
Ensure that the target BigQuery datasets exist in your project:
* `DWH_KERN` (containing the `T_KUNDE` table)
* `STAMMDATEN` (containing the `T_KUNDE_REFERENZ` table)

### 2. IAM & Permissions
The Cloud Composer Service Account must be granted the following IAM roles:
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the source datasets.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level to execute queries.

### 3. Airflow Variables
Configure the following Airflow Variables in the Composer Environment (via Airflow UI -> Admin -> Variables):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-production-project` | Target Google Cloud Project ID |
| `BQ_DATASET_DWH` | `DWH_KERN` | Dataset name for core customer tables |
| `BQ_DATASET_STAMM` | `STAMMDATEN` | Dataset name for reference tables |

### 4. Scheduling & Catchup
The DAG is configured with `catchup=False` and a weekly schedule (`0 2 * * 0` - Sundays at 02:00 AM). If historical runs are required, temporarily set `catchup=True` or trigger manual backfills via the Airflow CLI.

---

## 5. Known Gaps & Unresolved References

* **Upstream Dependency Alignment:** The legacy job was triggered by the job plan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`. In the cloud environment, this dependency must be handled either by scheduling this DAG to run after the ingestion DAG completes, or by implementing an `ExternalTaskSensor` to monitor the upstream ingestion task.
* **Date Formats:** The SQL query assumes that `k.AKTUALISIERT_AM` is stored as a standard BigQuery `DATE` or `TIMESTAMP`. If the source system loads this column as a string, explicit casting or schema modifications will be required.

---

## 6. Validation

To validate the migration, execute the following test steps:

### 1. Local Python Validation
Run the Python wrapper locally or in a development environment to verify connection and query execution:
```bash
python dags/bin/r_abgl_kunde_woech.py --stichtag 20260101
```

### 2. Airflow DAG Dry-Run
Trigger a manual run of the DAG `dw_dwh_kunde_abgl_woechentlich_js` from the Airflow UI.

### 3. Verification of "Passing" Criteria
The run is considered successful if:
* The task execution status is `SUCCESS`.
* The task logs display the verbatim German startup and completion messages.
* The log outputs the correct count of discrepancies, matching a manual validation query run directly in the BigQuery console:
```sql
SELECT COUNT(1) 
FROM `DWH_KERN.T_KUNDE` k
INNER JOIN `STAMMDATEN.T_KUNDE_REFERENZ` r ON r.KUNDE = k.KUNDE
WHERE k.AKTUALISIERT_AM <= '2026-01-01'
  AND (k.PLZ != r.PLZ OR k.ORT != r.ORT OR k.STRASSE != r.STRASSE);
```

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:** Navigate to the Airflow UI and toggle the switch for `dw_dwh_kunde_abgl_woechentlich_js` to **Off**.
2. **Re-enable Legacy Scheduling:** Reactivate the active flag for `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` in the UC4 scheduler.
3. **Verify Legacy Execution:** Confirm that the next scheduled run executes successfully on the legacy on-premise infrastructure.
4. **Post-Mortem:** Inspect the Cloud Composer task logs and BigQuery Job History to diagnose the root cause of the failure.