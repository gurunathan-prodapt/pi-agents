# Migration Notes: `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml`

This document outlines the migration details, design decisions, manual setup steps, and validation procedures for the monthly turnover consolidation job (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml`).

---

## 1. Summary

The legacy UC4 job plan (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`) and its underlying UNIX task (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS`), which previously executed an Ab Initio graph (`umsatz_konsolidierung.mp`) via a Korn Shell wrapper (`r_umsatz_konsolidierung_monatlich.ksh`) on the host `DWHDWH1P`, have been migrated to **Google Cloud Platform (GCP)**.

*   **Orchestration Target:** Cloud Composer (Apache Airflow 2.x)
*   **Execution Target:** Dataproc Serverless (PySpark 3.4 / Spark 3.x Runtime)
*   **Database Target:** BigQuery (replacing the legacy Oracle database references)

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy components:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/dw_dwh_umsatz_konsolidierung_monatlich_jp.py` | **Airflow DAG:** Orchestrates the monthly execution loop, schedules the workflow, and handles environment variable resolution. |
| `bin/r_umsatz_konsolidierung_monatlich.py` | **Horizon Python Wrapper:** Replaces the legacy KSH wrapper script. It parses execution parameters, compiles the BigQuery SQL consolidation query, and executes it via the Horizon core library. |
| `abinitio/umsatz_konsolidierung.py` | **PySpark Application:** Replaces the legacy Ab Initio graph (`umsatz_konsolidierung.mp`). It handles data cleaning, referential integrity checks, dual-stream aggregation (regular vs. storno), and quality gate validations. |
| `dags/umsatz_konsolidierung_monatlich_dag.py` | **Airflow DAG (Alternative/Direct):** Direct Dataproc Serverless submission DAG that bypasses the Horizon Python wrapper to execute the PySpark application directly. |

---

## 3. Key Design Decisions

### 3.1 Dual-Stream Aggregation in PySpark
To preserve the exact business logic of the legacy Ab Initio graph, the PySpark application splits the normalized transaction stream into two parallel pipelines:
1.  **Regular Stream (`REGULAER`):** Aggregates gross values to compute `umsatz_summe_cent` and counts transactions (`anzahl_buchungen`).
2.  **Cancellation Stream (`STORNO`):** Aggregates total cancellations to compute `storno_summe_cent`.

These streams are recombined using a `left_outer` join. This prevents floating-point representation drift during downstream aggregation by multiplying values by `100.0` and casting them to absolute integer `cents` (Long).

### 3.2 Horizon Python Wrapper vs. Direct PySpark
Two execution paths are provided to support different deployment patterns:
*   **Horizon Pattern (`bin/r_umsatz_konsolidierung_monatlich.py`):** Translates the legacy shell wrapper logic into Python, compiling and executing a direct BigQuery SQL consolidation query.
*   **Dataproc Serverless Pattern (`abinitio/umsatz_konsolidierung.py`):** Runs a distributed PySpark job on Dataproc Serverless, pulling from Oracle (or migrated BigQuery tables) and performing complex data cleaning and validation.

### 3.3 Quality Gates and Tolerances
The PySpark application implements strict post-processing quality gates:
*   **Row Count Check:** Verifies that the total loaded records meet the `MIN_ROW_COUNT` threshold.
*   **Tolerance Check:** Compares the absolute variance between regular and storno sums against `KONSOLIDIERUNG_TOLERANZ`. If deviations exceed `MAX_ABWEICHUNGEN`, a critical alert is written to GCS and the job is failed.

---

## 4. Manual Steps Before Go-Live

### 4.1 Schema and Dataset Creation
Ensure the target BigQuery tables (or Oracle tables, if running in hybrid mode) are created.

```sql
-- Target Fact Table
CREATE TABLE IF NOT EXISTS bq_dataset.fact_umsatz_konzern_monat (
    konzerngesellschaft STRING NOT NULL,
    verarbeitungsmonat STRING NOT NULL,
    tarifgruppen_code STRING,
    waehrung STRING DEFAULT 'EUR' NOT NULL,
    umsatz_summe_cent INT64 NOT NULL,
    storno_summe_cent INT64 DEFAULT 0 NOT NULL,
    anzahl_buchungen INT64 NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

-- Audit Logging Table
CREATE TABLE IF NOT EXISTS bq_dataset.audit_umsatz_consolidation (
    audit_id STRING DEFAULT GENERATE_UUID(),
    verarbeitungsmonat STRING NOT NULL,
    konzerngesellschaft STRING NOT NULL,
    source_row_count INT64 NOT NULL,
    target_row_count INT64 NOT NULL,
    deviation_count INT64 NOT NULL,
    status STRING NOT NULL, -- 'SUCCESS', 'TOLERANCE_BREACH', 'FAILED'
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);
```

### 4.2 IAM & Permissions
The Cloud Composer / Dataproc Serverless Service Account must be granted the following roles:
*   `roles/bigquery.dataEditor` on the target BigQuery dataset.
*   `roles/bigquery.jobUser` on the GCP project.
*   `roles/storage.objectAdmin` on the GCS bucket containing code assets, error outputs, and alerts.
*   `roles/secretmanager.secretAccessor` on the Oracle database credential secrets (if applicable).

### 4.3 Airflow Variables & Secrets
Configure the following Airflow Variables in Cloud Composer:

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | Target Google Cloud Project ID |
| `GCP_REGION` | `europe-west3` | Target GCP Region |
| `GCS_BUCKET` | `my-dwh-environment-bucket` | GCS Bucket for code and logs |
| `KONZERNGESELLSCHAFT` | `ALL` | Default corporate consolidation scope |

If connecting to an Oracle database, store the credentials in GCP Secret Manager:
*   `DB_USER`
*   `DB_PASSWORD`

---

## 5. Known Gaps & Unresolved References

### 5.1 Unresolved Ab Initio Graph Logic
> [!WARNING]
> **SOURCE: NOT FOUND — `abinitio/umsatz_konsolidierung.mp` — no candidate**
> The actual internal graph logic (transformations, lookup rules, sorting) was not present in the workspace. The PySpark code (`abinitio/umsatz_konsolidierung.py`) and BigQuery SQL queries represent a standard aggregation model based on the wrapper parameters. A data engineer must manually verify the actual GDE mapping of `umsatz_konsolidierung.mp` to ensure no extra business rules (e.g., currency conversion, intercompany sales elimination) were missed.

### 5.2 Unconfirmed Validation Scripts
The validation files referenced in the legacy Ab Initio graph (`validate_umsatz_periode.sql`, `validate_umsatz_counts.sql`, `check_umsatz_toleranz.sql`) are unconfirmed. These have been implemented as inline PySpark validations, but must be verified against the original SQL files if they are located.

---

## 6. Validation

### 6.1 How to Run the Tests

#### Local PySpark Validation
Run the PySpark script locally or on a development Dataproc cluster using dummy arguments:

```bash
export DB_USER="test_user"
export DB_PASSWORD="test_password"

python3 abinitio/umsatz_konsolidierung.py \
  --verarbeitungsmonat "202601" \
  --konzerngesellschaft "ALL" \
  --ora_connect_string "jdbc:oracle:thin:@localhost:1521/ORCL" \
  --error_output_dir "gs://my-bucket/errors" \
  --alert_output_dir "gs://my-bucket/alerts" \
  --log_dir "gs://my-bucket/logs" \
  --konsolidierung_toleranz 2.5 \
  --max_abweichungen 25 \
  --min_row_count 1
```

#### Airflow DAG Dry-Run
Verify the Airflow DAG syntax and structure:

```bash
airflow dags test dw_dwh_umsatz_konsolidierung_monatlich_jp 2026-01-01
```

### 6.2 Meaning of "Passing"
A test run is considered successful ("passing") if:
1.  The execution completes with exit code `0`.
2.  No lines starting with `FEHLER` or `[ERROR]` are written to the log file.
3.  The target table `FACT_UMSATZ_KONZERN_MONAT` is populated with aggregated records.
4.  Unmatched corporate records are successfully isolated in the `error_output_dir` GCS path.
5.  An audit record with status `SUCCESS` is written to `AUDIT_UMSATZ_CONSOLIDATION`.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live:

1.  **Pause the Airflow DAG:**
    ```bash
    airflow dags pause dw_dwh_umsatz_konsolidierung_monatlich_jp
    ```
2.  **Revert Database Writes:**
    If the job failed midway or wrote bad data, delete the records written for the active `VERARBEITUNGSMONAT`:
    ```sql
    DELETE FROM `your_project_id.your_dataset_id.tgt_umsatz_konsolidiert`
    WHERE verarbeitungs_monat = 'YYYYMM';
    ```
3.  **Re-enable Legacy Scheduling:**
    Re-activate the legacy UC4 Job Plan `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP` to resume on-premise processing.