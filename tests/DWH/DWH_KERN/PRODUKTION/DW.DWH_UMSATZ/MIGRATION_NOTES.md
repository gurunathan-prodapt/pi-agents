# Migration Notes

## 1. Summary
This document details the migration of the monthly sales revenue consolidation job (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`) from a legacy on-premises environment to Google Cloud Platform (GCP).

*   **Source Platform:** UC4/Automic scheduler orchestrating a KornShell wrapper script (`r_umsatz_konsolidierung_monatlich.ksh`) which executes an Ab Initio graph (`umsatz_konsolidierung.mp`).
*   **Target Platform:** Google Cloud Platform (GCP) utilizing **Cloud Composer (Airflow 2.x)** for orchestration and **Dataproc Serverless (PySpark)** for scalable, distributed data processing.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy components:

| File Path | Role |
| :--- | :--- |
| `dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/dw_dwh_umsatz_konsolidierung_monatlich_jp.py` | **Airflow DAG**: Replaces the UC4 Job Plan (`_JP.xml`) and Unix Job (`_JS.xml`). Orchestrates the execution of the Python wrapper. |
| `dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/bin/r_umsatz_konsolidierung_monatlich.py` | **Python Wrapper**: Replaces the legacy KornShell wrapper (`.ksh`). Parses parameters, sets up environment variables, and triggers the PySpark application. |
| `dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/abinitio/umsatz_konsolidierung.py` | **PySpark Application**: Replaces the Ab Initio graph (`.mp`). Implements modular data normalization, dimension enrichment joins, data splitting, and aggregation. |
| `dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/abinitio/dwh_umsatz_konsolidierung_monatlich.py` | **Advanced Airflow DAG**: An alternative, production-ready DAG featuring upstream period validation sensors, direct Dataproc Serverless batch submission, row-count checks, and statistical tolerance validation. |
| `dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/abinitio/umsatz_konsolidierung.sql` | **BigQuery SQL Template**: Provides a native BigQuery ELT alternative to the PySpark pipeline for direct database-level transformations. |

---

## 3. Key Design Decisions

### 3.1 Dataproc Serverless (PySpark) vs. BigQuery SQL
*   **Decision:** Both a PySpark pipeline (`umsatz_konsolidierung.py`) and a BigQuery SQL template (`umsatz_konsolidierung.sql`) were generated.
*   **Why:** PySpark was chosen as the primary target to maintain structural alignment with the complex, multi-stage join and split logic typical of Ab Initio graphs. However, for simpler consolidation runs, the BigQuery SQL template offers a lower-overhead, cost-effective ELT alternative.

### 3.2 Error Handling and Log Preservation
*   **Decision:** Original German log messages and error outputs from the legacy shell script are preserved verbatim in the Python wrapper.
*   **Why:** This ensures operational continuity. Monitoring tools, log parsers, and support teams accustomed to the legacy system can continue to track job states using the same string patterns (e.g., `"Fehlerzeilen im Konsolidierungs-Protokoll gefunden"`).

### 3.3 Data Splitting for Quality Assurance
*   **Decision:** The PySpark application splits processed records into a "matched" dataset (written to BigQuery) and an "unmatched" dataset (written to GCS as a CSV report).
*   **Why:** This replicates the robust error-handling patterns of Ab Initio, preventing invalid or unmapped records from polluting the core DWH tables while ensuring they are preserved for audit and remediation.

---

## 4. Manual Steps Before Go-Live

### 4.1 Schema & Dataset Creation
Ensure the target BigQuery datasets and tables exist. If they do not, create them:
```sql
CREATE SCHEMA IF NOT EXISTS `your_project.DWH_STAGING`;
CREATE SCHEMA IF NOT EXISTS `your_project.DWH_CORE`;

-- Create target consolidated table if not using auto-schema creation
CREATE TABLE IF NOT EXISTS `your_project.DWH_CORE.FACT_UMSATZ_KONS_MONAT` (
    verarbeitungsmonat STRING,
    konzerngesellschaft STRING,
    tarifgruppen_code STRING,
    waehrung STRING,
    umsatz_cents INT64,
    storno_cents INT64,
    anzahl_buchungen INT64
);
```

### 4.2 IAM & Permissions
The Cloud Composer / Dataproc Service Account must have the following roles:
*   `roles/dataproc.editor` (to create and run Dataproc Serverless batches)
*   `roles/bigquery.dataEditor` (on staging and core datasets)
*   `roles/bigquery.jobUser` (to run BigQuery queries)
*   `roles/storage.objectAdmin` (on the GCS bucket used for code and error logs)

### 4.3 Airflow Variables & Connections
Define the following Airflow Variables in the Composer environment:
*   `GCP_PROJECT`: Your target GCP Project ID.
*   `GCS_BUCKET`: The GCS bucket where scripts and error logs are stored (e.g., `my-dwh-bucket`).
*   `BQ_DATASET_STG`: Staging dataset name (default: `DWH_STAGING`).
*   `BQ_DATASET_DWH`: Core dataset name (default: `DWH_CORE`).

### 4.4 Code Deployment
Upload the generated scripts to your Cloud Storage bucket:
```bash
gsutil cp dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/abinitio/umsatz_konsolidierung.py gs://[GCS_BUCKET]/src/umsatz_konsolidierung.py
gsutil cp dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/bin/r_umsatz_konsolidierung_monatlich.py gs://[GCS_BUCKET]/src/r_umsatz_konsolidierung_monatlich.py
```
Place the DAG files into the Composer `/dags` folder.

---

## 5. Known Gaps & Unresolved References

1.  **Ab Initio Graph Logic Verification (Redesign B4 Item):**
    *   *Gap:* The original `.mp` graph file was missing from the migration bundle.
    *   *Remediation:* The PySpark script (`umsatz_konsolidierung.py`) implements a standard normalization, join, and aggregation pattern. A developer must verify if there are specific, complex business rules or custom transformations in the legacy Ab Initio graph that need to be manually ported into the `UmsatzTransformer` class.
2.  **Staging Table Schemas:**
    *   *Gap:* The exact schemas for `STG_UMSATZ_TRANSAKTIONEN`, `DIM_KONZERNGESELLSCHAFT`, and `STG_TARIFGRUPPEN_MAPPING` were inferred.
    *   *Remediation:* Verify that the column names used in `UmsatzTransformer.normalise_umsatz` and `UmsatzTransformer.enrich_transactions` match your actual BigQuery table schemas.

---

## 6. Validation & Testing

### 6.1 Running the DAG
1.  Navigate to the Airflow UI.
2.  Locate the DAG `dwh_umsatz_konsolidierung_monatlich`.
3.  Trigger the DAG manually with a custom configuration JSON to test a specific month and company:
    ```json
    {
      "verarbeitungsmonat": "202601",
      "konzerngesellschaft": "COMPANY_DE"
    }
    ```

### 6.2 Validation Criteria ("Passing" State)
*   **Task `validate_period_sensor`:** Must pass successfully, confirming the target period is marked as `ACTIVE` in `DIM_PROCESS_PERIODS`.
*   **Task `submit_pyspark_job`:** Must complete with exit code `0`.
*   **Task `validate_row_counts`:** Must confirm that at least 1 row was written to the target table for the processed period.
*   **Task `check_konsolidierung_toleranz`:** Must evaluate to `True`, confirming that the consolidated revenue does not deviate from the previous month by more than the defined threshold (2.5% and 25 absolute units).
*   **Error Logs:** Check `gs://[GCS_BUCKET]/errors/umsatz/` for any unmatched records. A successful run with high data quality should result in an empty or near-empty unmatched CSV.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during the migration run:

1.  **Pause the DAG:** Immediately pause the `dwh_umsatz_konsolidierung_monatlich` DAG in the Airflow UI to prevent scheduled runs.
2.  **Purge Corrupted Data:** Run a delete query in BigQuery to remove records written by the failed migration run:
    ```sql
    DELETE FROM `your_project.DWH_CORE.FACT_UMSATZ_KONS_MONAT`
    WHERE verarbeitungsmonat = 'TARGET_YYYYMM'
      AND konzerngesellschaft = 'TARGET_COMPANY';
    ```
3.  **Revert to Legacy Scheduler:** If a fallback to the legacy environment is required, re-enable the UC4 Job Plan `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP` and run the legacy job for the target period.