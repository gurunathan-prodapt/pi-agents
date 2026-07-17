# Migration Notes: `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml`

This document provides comprehensive migration notes for transitioning the monthly sales consolidation pipeline from the legacy UC4, KornShell, and Ab Initio environment to Google Cloud Platform (GCP).

---

## 1. Summary

The monthly sales consolidation workflow (**`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`**) has been migrated from a legacy on-premise environment to a modern cloud-native architecture on **Google Cloud Platform (GCP)**. 

*   **Source Platform:** UC4 (Automic) Scheduler, KornShell (`.ksh`) wrappers, and Ab Initio (`.mp`) data processing graphs.
*   **Target Platform:** Google Cloud Composer (Apache Airflow) for orchestration, with data processing executing on **Google Cloud BigQuery** (via native SQL) and **Google Cloud Dataproc Serverless** (via PySpark).
*   **Business Purpose:** Performs monthly consolidation of sales data (`UMSATZ`) across all corporate group companies, standardizing currencies, processing cancellations (stornos), and outputting consolidated metrics for financial reporting.

---

## 2. Generated Artifacts

The migration process has generated the following production-ready files, structured to preserve folder integrity and logical isolation:

| Generated File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dw/dw_dwh_umsatz_konsolidierung_monatlich_jp.py` | Python (Airflow) | **Cloud Composer DAG:** Orchestrates the monthly execution run, handles dynamic date calculations, and triggers processing tasks. |
| `abinitio/umsatz_konsolidierung.py` | Python (PySpark) | **Dataproc Serverless Job:** High-fidelity PySpark implementation of the core Ab Initio graph logic, performing data normalization, GCS error-routing, and BigQuery persistence. |
| `bin/r_umsatz_konsolidierung_monatlich.py` | Python 3 | **Wrapper Script:** Migrated KornShell wrapper that compiles and executes the decoupled SQL transformation using the Horizon core library. |
| `abinitio/umsatz_konsolidierung.sql` | BigQuery SQL | **Decoupled SQL Asset:** Core consolidation query extracted from the legacy graph, parameterized for execution via the Python wrapper. |

---

## 3. Key Design Decisions

### Decoupled Compute Paradigms (Hybrid Approach)
To accommodate different target patterns within the corporate group, two high-fidelity target paths have been generated:
1.  **Dataproc Serverless (PySpark):** Best suited for complex, programmatic ETL pipelines requiring explicit row-by-row normalization, left-outer joins with dimension tables, and routing of unmatched records directly to Google Cloud Storage (GCS) as delimited files.
2.  **BigQuery Native SQL (BQSQL):** Best suited for high-performance, cost-effective ELT processing directly inside the data warehouse, leveraging BigQuery's compute engine for aggregations.

### State-Preserving Jinja Templating
To prevent state skew and ensure that historical backfills run deterministically, the legacy system-time extraction (`SYS_DATE("YYYYMM")`) is mapped to Airflow's native Jinja macro:
`{{ execution_date.strftime('%Y%m') }}`.

### Strict Log and Output Preservation (Verbatim Rule)
All legacy German-language print statements, log messages, and error indicators have been preserved character-for-character within the target scripts (e.g., `"Umsatzkonsolidierung fuer Monat..., Konzerngesellschaft... angestossen"`). This ensures downstream diagnostic tools and operations teams experience zero friction.

### Folder Integrity and Namespace Isolation
Target files are organized into directories (`bin/`, `abinitio/`, `dags/dw/`) that mirror the legacy repository structure. This prevents namespace collisions across different migration project groups.

---

## 4. Manual Steps Before Go-Live

Before activating the migrated pipeline in a production environment, the following manual setup steps must be completed:

### Schema & Dataset Creation
1.  Ensure the target BigQuery dataset (configured via `BQ_DATASET`, e.g., `dwh_kern`) exists in the target GCP project.
2.  Create or migrate the following tables in BigQuery with schemas compatible with the legacy Oracle definitions:
    *   `STG_UMSATZ_TRANSAKTIONEN`
    *   `DIM_KONZERNGESELLSCHAFT`
    *   `STG_TARIFGRUPPEN_MAPPING`
    *   `FACT_UMSATZ_KONZERN_MONAT` (or `fact_umsatz_konsolidiert`)

### IAM & Permissions
1.  Create or identify the Google Cloud Service Account used to run the Dataproc Serverless jobs and Cloud Composer tasks (mapping to legacy login `DW.UNIX.ISTNS`).
2.  Grant the Service Account the following IAM roles:
    *   `roles/dataproc.editor` (to submit Dataproc Serverless jobs)
    *   `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` (to read/write BigQuery tables)
    *   `roles/storage.objectAdmin` (to read/write scripts and error logs in GCS)

### Airflow Variables & Environment Variables
Configure the following Airflow Variables in the Cloud Composer environment:

| Airflow Variable Key | Expected Value Example | Classification |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-1234` | Global Infrastructure |
| `GCP_REGION` | `europe-west3` | Global Infrastructure |
| `DATAPROC_CLUSTER` | `ephemeral-spark-cluster` | Global Infrastructure |
| `GCS_BUCKET` | `prod-dwh-workspace-bucket` | Global Infrastructure |
| `BQ_DATASET` | `dwh_kern` | Global Infrastructure |

### Scheduling & Triggering
Because the legacy UC4 Job Plan did not contain an explicit `EVNT_TIME` or schedule file, the Airflow DAG is deployed with `schedule=None`. 
*   **Action Required:** If this job must run on a fixed monthly schedule, update the `schedule` parameter in `dw_dwh_umsatz_konsolidierung_monatlich_jp.py` (e.g., `schedule="0 2 1 * *"` to run at 02:00 AM on the 1st of every month) or integrate it with your external enterprise orchestrator.

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) Items & Missing Source Components
1.  **Validation Scripts (`sqlplus` dependencies):**
    *   *Gap:* The legacy pipeline references external validation steps (`validate_umsatz_periode.sql`, `validate_umsatz_counts.sql`, and `check_umsatz_toleranz.sql`) originally executed via Oracle `sqlplus`.
    *   *Redesign Action:* These validation scripts must be rewritten as native BigQuery SQL assertions or executed using Airflow's `BigQueryCheckOperator` within the DAG before and after the main processing task.
2.  **Ab Initio Graph Binary (`umsatz_konsolidierung.mp`):**
    *   *Gap:* The physical Ab Initio graph binary was not present in the source workspace.
    *   *Mitigation:* The PySpark script (`abinitio/umsatz_konsolidierung.py`) and BigQuery SQL asset (`abinitio/umsatz_konsolidierung.sql`) were reverse-engineered from wrapper scripts and metadata. Field mappings and data types must be verified against production schemas by a data engineer.

---

## 6. Validation

To validate the migrated pipeline, perform the following test execution steps:

### How to Run the Tests
1.  **Upload Assets:** Upload the PySpark script and SQL template to your GCS workspace bucket:
    ```bash
    gsutil cp abinitio/umsatz_konsolidierung.py gs://{GCS_BUCKET}/pyspark_scripts/
    gsutil cp abinitio/umsatz_konsolidierung.sql gs://{GCS_BUCKET}/abinitio/
    ```
2.  **Deploy DAG:** Copy `dw_dwh_umsatz_konsolidierung_monatlich_jp.py` to your Composer DAGs folder.
3.  **Trigger Test Run:** Trigger the DAG manually via the Airflow UI or CLI, passing a test execution date and configuration:
    ```bash
    gcloud composer environments run {COMPOSER_ENV} \
        --location {REGION} dags trigger \
        -- dw_dwh_umsatz_konsolidierung_monatlich_jp \
        --conf '{"monat":"202601", "konzern":"ALL"}'
    ```

### Definition of "Passing"
The migration test is successful if:
*   The Airflow DAG completes with a `SUCCESS` status.
*   The Dataproc Serverless task logs show the verbatim start message:
    `Umsatzkonsolidierung fuer Monat 202601, Konzerngesellschaft ALL angestossen`.
*   Unmatched records (if any) are successfully written to GCS at:
    `gs://{GCS_BUCKET}/errors/umsatz/umsatz_unmatched_ALL_202601.dat`.
*   Consolidated records are appended to the BigQuery table `FACT_UMSATZ_KONZERN_MONAT` and match legacy run totals exactly.
*   The audit log JSON is successfully written to GCS with the correct record count.

---

## 7. Rollback Procedure

In the event of an operational failure or data discrepancy post-go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG:**
    Immediately pause the migrated DAG to prevent subsequent scheduled or automated runs:
    ```bash
    gcloud composer environments run {COMPOSER_ENV} \
        --location {REGION} dags pause -- dw_dwh_umsatz_konsolidierung_monatlich_jp
    ```
2.  **Purge Corrupted Data:**
    If the failed run wrote partial or corrupted data to the target BigQuery table, run a transaction purge query for the affected processing month:
    ```sql
    DELETE FROM `your_project_id.dwh_kern.FACT_UMSATZ_KONZERN_MONAT`
    WHERE verarbeitungsmonat = '202601';
    ```
3.  **Re-enable Legacy Execution:**
    Re-activate the legacy UC4 Job Plan (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`) in the Automic UI to resume on-premise processing.