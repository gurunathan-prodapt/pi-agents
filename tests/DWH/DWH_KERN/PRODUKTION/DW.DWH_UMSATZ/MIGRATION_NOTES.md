# Migration Notes: `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS`

This document provides comprehensive technical notes for migrating the monthly revenue consolidation job from the legacy on-premises environment (UC4, KornShell, and Ab Initio) to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and Dataproc Serverless (PySpark).

---

## 1. Summary

The monthly revenue consolidation workflow (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS`) has been migrated from a legacy on-premises architecture to a modern, cloud-native data pipeline on Google Cloud Platform.

### Migration Scope
*   **Source Platform:** Automic/UC4 Scheduler, KornShell (`.ksh`) wrapper scripts, and Ab Initio (`.mp`) data transformation graphs.
*   **Target Platform:** Google Cloud Composer (Apache Airflow 2.x) and Dataproc Serverless (PySpark 3.x) executing against Google Cloud Storage (GCS) and BigQuery.
*   **Functional Objective:** Reconcile, normalize, and aggregate monthly revenue transaction data across all corporate group companies (`KONZERNGESELLSCHAFT = 'ALL'`) for downstream financial reporting.

---

## 2. Generated Artifacts

The migration process generated the following implementation-ready files, preserving the original directory structure where applicable:

### 1. Production Orchestration DAG
*   **Target Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_js.py`
*   **Role:** Orchestrates the monthly execution schedule (`0 3 1 * *`). It handles dynamic parameter resolution, executes pre-validation checks on BigQuery, triggers the Dataproc Serverless PySpark batch, and runs post-execution row-count audits.

### 2. Production Execution Utility Wrapper
*   **Target Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.py`
*   **Role:** A Python-based command-line utility that replaces the legacy KornShell wrapper (`r_umsatz_konsolidierung_monatlich.ksh`). It preserves folder integrity and provides a standardized CLI interface for manual execution and testing.

### 3. PySpark Business Logic Application
*   **Target Path:** `pyspark/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py`
*   **Role:** Replaces the legacy Ab Initio graph (`umsatz_konsolidierung.mp`). It performs data ingestion from BigQuery, normalizes transaction amounts to cent integers, executes left-outer joins for master data enrichment, splits regular bookings from stornos, performs aggregation rollups, and appends the consolidated facts to the target BigQuery table.

---

## 3. Key Design Decisions

### Dataproc Serverless (Batch API) vs. Managed Clusters
*   **Decision:** Dataproc Serverless was chosen over a persistent or ephemeral managed Dataproc cluster.
*   **Reasoning:** Since this job runs strictly once a month, maintaining a running cluster is highly cost-inefficient. Dataproc Serverless eliminates cluster management overhead, dynamically scales resources, and charges only for the exact duration of the batch execution.

### Cent-Integer Normalization
*   **Decision:** All monetary amounts (`umsatz_betrag`) are multiplied by `100.0` and cast to `IntegerType` (`umsatz_betrag_cent`) during the normalization phase.
*   **Reasoning:** Floating-point arithmetic introduces precision errors during large-scale aggregations. Converting currency to cent integers guarantees absolute mathematical precision during sum and rollup operations, aligning with financial accounting standards.

### Dead-Letter Queue (DLQ) for Unmatched Dimensions
*   **Decision:** Transactions that fail the left-outer join with `DIM_KONZERNGESELLSCHAFT` are written to a dedicated GCS error path (`gs://{GCS_BUCKET}/opt/dwh/errors/umsatz`) rather than failing the entire pipeline.
*   **Reasoning:** This prevents a single invalid master-data record from blocking the monthly financial close, while ensuring that data quality anomalies are fully preserved and visible for manual remediation.

### Folder Integrity Preservation
*   **Decision:** The target repository mirrors the exact directory structure of the legacy environment.
*   **Reasoning:** Preserving the folder structure ensures that existing deployment pipelines, code ownership boundaries, and documentation remain coherent and easy to navigate for legacy developers.

---

## 4. Manual Steps Before Go-Live

The following administrative and infrastructure setup steps must be completed before enabling the DAG in production:

### 1. Schema and Dataset Creation
Ensure the target BigQuery datasets and tables are created in the appropriate region (e.g., `europe-west3`):
*   **Dataset:** `DWH_TARGET` (or the value configured in the `BQ_DATASET` Airflow variable).
*   **Tables:**
    *   `STG_UMSATZ_TRANSAKTIONEN`
    *   `DIM_KONZERNGESELLSCHAFT`
    *   `STG_TARIFGRUPPEN_MAPPING`
    *   `DIM_PERIODE`
    *   `FACT_UMSATZ_KONZERN_MONAT` (Target table)

### 2. IAM and Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
*   `roles/dataproc.editor` (To submit Dataproc Serverless batches)
*   `roles/bigquery.dataEditor` (On the target BigQuery datasets)
*   `roles/storage.objectAdmin` (On the GCS bucket hosting scripts and error logs)
*   `roles/composer.worker` (Standard Composer execution permissions)

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer UI (`Admin -> Variables`):

| Variable Name | Example Production Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-1234` | The target GCP Project ID. |
| `GCP_REGION` | `europe-west3` | The GCP region for Dataproc and Composer. |
| `GCS_BUCKET` | `prod-dwh-artifacts-bucket` | The GCS bucket hosting PySpark scripts and logs. |
| `BQ_DATASET` | `DWH_TARGET` | The target BigQuery dataset name. |
| `DATAPROC_SUBNET` | `projects/prod-dwh-gcp-1234/regions/europe-west3/subnetworks/dwh-private-subnet` | Private VPC subnetwork URI for Dataproc Serverless. |

### 4. Artifact Deployment
Upload the PySpark application to the mirrored path in your GCS bucket:
```bash
gsutil cp pyspark/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py \
  gs://<YOUR_GCS_BUCKET>/pyspark/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py
```

---

## 5. Known Gaps & Unresolved References

### 1. Missing Source Code for Ab Initio Graph (`umsatz_konsolidierung.mp`)
*   **Risk:** The original `.mp` file was not available in the source code context.
*   **Mitigation:** The PySpark application (`umsatz_konsolidierung.py`) was reverse-engineered and reconstructed based on the functional specifications, database schemas, and legacy wrapper logic.
*   **Action Required:** A data engineer must perform a side-by-side logic audit of the PySpark code against the legacy Ab Initio graph in a development environment to verify that all edge cases (such as specific storno flags or custom mapping rules) are perfectly matched.

### 2. Parent JobPlan Orchestration (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`)
*   **Risk:** The parent JobPlan container DAG is not yet migrated.
*   **Mitigation:** The child DAG is currently configured to run standalone on a monthly schedule (`0 3 1 * *`).
*   **Action Required:** Once the parent `_JP` DAG is migrated, this child DAG should either be converted to a sub-DAG, triggered via a `TriggerDagRunOperator` from the parent, or wired using an `ExternalTaskSensor`.

---

## 6. Validation

To validate the migration, execute the pipeline in a non-production environment using the following steps:

### 1. Local/CLI Validation of the Wrapper
Run the Python wrapper script locally to verify parameter parsing and legacy log output:
```bash
python3 dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.py \
  -m 202601 \
  -k ALL
```
*   **Expected Output:**
    `2026-01-01 00:00:00,000 - INFO - Umsatzkonsolidierung fuer Monat 202601, Konzerngesellschaft ALL angestossen`

### 2. Airflow DAG Dry-Run
Trigger the DAG manually from the Airflow UI with the following configuration JSON:
```json
{
  "VERARBEITUNGSMONAT": "202601",
  "KONZERNGESELLSCHAFT": "ALL"
}
```

### 3. Definition of "Passing"
The validation run is considered successful if and only if:
1.  The `validate_period` task successfully finds the target month registered in `DIM_PERIODE`.
2.  The `umsatz_konsolidierung` Dataproc Serverless task completes with an `ExitCode: 0`.
3.  The `validate_row_counts` task confirms that at least one record has been appended to `FACT_UMSATZ_KONZERN_MONAT` for the target period.
4.  The Airflow task logs display the verbatim legacy success marker:
    `Monatliche Umsatzkonsolidierung ohne erkennbare Fehler beendet`

---

## 7. Rollback Procedure

If the migrated pipeline fails in production or causes data corruption, execute the following rollback steps:

### 1. Pause the Airflow DAG
Immediately pause the DAG in the Cloud Composer UI to prevent subsequent scheduled runs:
```bash
gcloud composer environments run <ENVIRONMENT_NAME> \
  --location <LOCATION> \
  dags pause -- dw_dwh_umsatz_konsolidierung_monatlich_js
```

### 2. Purge Corrupted Data
If the PySpark job partially wrote corrupted data to the target BigQuery table, run the following query to remove the affected partition:
```sql
DELETE FROM `DWH_TARGET.FACT_UMSATZ_KONZERN_MONAT`
WHERE VERARBEITUNGSMONAT = '202601'  -- Replace with the failed processing month
  AND KONZERNGESELLSCHAFT = 'ALL';
```

### 3. Reactivate Legacy Execution
Re-enable the legacy UC4 job schedule (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS`) in the Automic/UC4 UI to resume processing on the legacy on-premises infrastructure.