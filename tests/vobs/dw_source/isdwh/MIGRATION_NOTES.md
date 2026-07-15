# Migration Notes: DW.DWH_ABPZ_KKM_AIL_AGENT

This document provides comprehensive migration notes for transitioning the legacy `DW.DWH_ABPZ_KKM_AIL_AGENT` workflow from Automic/UC4 and KornShell/Ab Initio to Apache Cloud Composer (Airflow) and Google Cloud Dataproc Serverless (PySpark) on BigQuery.

---

## 1. Summary

The legacy workflow `DW.DWH_ABPZ_KKM_AIL_AGENT` was responsible for building a flat-file lookup for the data warehouse view `DWH$VI_S_SDM_AGENT_ADS`. It triggered an Ab Initio process to write agent ADS lookup data as part of the daily KKM import processing pipeline.

In the target architecture, this process has been migrated to:
* **Orchestration**: Apache Airflow (Cloud Composer) running a daily DAG (`dw_dwh_abpz_kkm_ail_agent`).
* **Compute**: Dataproc Serverless (PySpark) executing a batch job (`agent_ads_lookup.py`).
* **Storage & Source**: Pulls source data from a BigQuery view (`DW.DWH_VI_S_SDM_AGENT_ADS`), formats it, and stores the resulting flat-file lookup (`AgentADSLookup.txt`) in Google Cloud Storage (GCS).

---

## 2. Generated Artifacts

The migration process generated the following files:

| File Path | Target Platform Role | Description |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/dw_dwh_abpz_kkm_ail_agent.py` | Cloud Composer (Airflow) | The DAG orchestration script. It handles the execution schedule, defines task dependencies, resolves global variables, and triggers the Dataproc Serverless PySpark batch. |
| `vobs/dw_source/isdwh/agent_ads_lookup.py` | Dataproc Serverless (PySpark) | The ETL application script. It reads from the BigQuery view, applies date filters, formats the data as a pipe-delimited CSV, and writes a single coalesced file to GCS. |

---

## 3. Key Design Decisions

### Dataproc Serverless (PySpark) over Standard Dataproc
* **Decision**: Use Dataproc Serverless instead of maintaining a persistent or ephemeral Hadoop cluster.
* **Rationale**: Eliminates cluster management overhead, scales automatically, and charges only for the exact duration of the batch execution. This aligns with modern cloud-native operational standards.

### Single Partition Coalesce (`.coalesce(1)`)
* **Decision**: Force the final Spark DataFrame to write using a single partition.
* **Rationale**: The downstream legacy systems expect a single flat file (`AgentADSLookup.txt`). While coalescing to a single partition can limit parallel write performance, lookup datasets are typically small enough that this trade-off is acceptable to maintain compatibility with downstream consumers.

### Preservation of Legacy Log Output
* **Decision**: Retain original German log messages (e.g., *"Parameter für den ab initio Prozess"*, *"Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler..."*) within the PySpark script and Airflow failure callbacks.
* **Rationale**: Ensures operational continuity. Support teams monitoring logs can use existing runbooks and search patterns to verify execution success or diagnose failures.

---

## 4. Manual Steps Before Go-Live

Before activating the migrated workflow in production, the following setup steps must be completed:

### A. Schema & Dataset Verification
1. Ensure that the BigQuery view `DW.DWH_VI_S_SDM_AGENT_ADS` exists in the target GCP project and is populated with data.
2. Verify that the view contains a timestamp or date column named `LAST_UPDATE` used for filtering.

### B. IAM & Permissions
Ensure that the Cloud Composer / Dataproc Serverless service account has the following IAM roles:
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the source dataset/view.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the target GCS bucket.
* **Dataproc Worker** (`roles/dataproc.worker`) to execute serverless batches.

### C. Airflow Variables Configuration
Define the following Airflow Variables in the Cloud Composer environment:
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `GCP_REGION`: The GCP region where Dataproc Serverless will run (e.g., `europe-west3`).
* `GCS_BUCKET`: The operational GCS bucket name (without the `gs://` prefix).

### D. GCS Directory Structure & Artifact Deployment
1. Upload the PySpark script to GCS:
   ```bash
   gsutil cp vobs/dw_source/isdwh/agent_ads_lookup.py gs://<GCS_BUCKET>/pyspark/agent_ads_lookup.py
   ```
2. Ensure the target directory for lookups exists: `gs://<GCS_BUCKET>/lookups/`.

### E. Scheduling & Downstream Coordination
* The DAG is scheduled to run daily at `05:00 UTC` (`0 5 * * *`).
* Ensure downstream jobs in the daily import loop are configured with a GCS sensor (e.g., `GCSObjectExistenceSensor`) pointing to `gs://<GCS_BUCKET>/lookups/AgentADSLookup.txt` to prevent them from running before this lookup is generated.

---

## 5. Known Gaps & Unresolved References

### 1. Monitoring Integration (`showlog.ksh` / `DW.DWH_ADM_JOB_MONITOR_START`)
* **Legacy Status**: The legacy environment used custom shell utilities (`showlog.ksh`, `r_alis_objekt`) and database procedures to register job execution states.
* **Current State**: Replaced by mock Python functions (`start_monitoring` and `end_monitoring`) that write to Airflow task logs.
* **Follow-up**: If centralized database logging is strictly required for enterprise auditing, these mock tasks must be updated to call an API or write to a centralized metadata table in BigQuery.

### 2. Network Configuration (`subnetwork_uri`)
* **Current State**: The Dataproc batch configuration uses `"subnetwork_uri": "default"`.
* **Follow-up**: In production environments with strict VPC controls, update `"default"` to the specific VPC subnetwork configured for Private Google Access.

---

## 6. Validation

### How to Run the Tests

#### 1. Local/Dev Airflow DAG Validation
To verify that the DAG parses without syntax or import errors:
```bash
python3 vobs/dw_source/isdwh/dw_dwh_abpz_kkm_ail_agent.py
```

#### 2. Manual Dataproc Batch Test Run
You can test the PySpark execution directly using the gcloud CLI:
```bash
gcloud dataproc batches submit pyspark gs://<GCS_BUCKET>/pyspark/agent_ads_lookup.py \
    --project=<GCP_PROJECT> \
    --region=<GCP_REGION> \
    --dependency-jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    -- \
    --output_bucket <GCS_BUCKET> \
    --output_file AgentADSLookup.txt \
    --backlook_days 84 \
    --project_prefix BHB_CCM_PROC \
    --first_day 2023-03-19 \
    --last_day_plus_1 2023-06-12 \
    --gcp_project <GCP_PROJECT>
```

### What "Passing" Means
* **Exit Code**: The Dataproc batch job completes with exit code `0`.
* **Logs**: The execution logs contain the message:
  `Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.`
* **Output Verification**: A single pipe-delimited file is successfully created at:
  `gs://<GCS_BUCKET>/lookups/AgentADSLookup.txt`
* **Data Integrity**: The output file contains a header row, uses `|` as a delimiter, and contains records filtered within the specified date range.

---

## 7. Rollback Procedure

If a critical failure occurs post-deployment, execute the following steps to roll back:

1. **Pause the Airflow DAG**:
   ```bash
   gcloud composer environments run <COMPOSER_ENV_NAME> \
       --location <GCP_REGION> \
       dags pause -- dw_dwh_abpz_kkm_ail_agent
   ```
2. **Re-enable Legacy Scheduling**:
   Re-activate the legacy UC4 job `DW.DWH_ABPZ_KKM_AIL_AGENT` in the Automic controller.
3. **Verify Legacy Source**:
   Ensure that the legacy Ab Initio environment and database connections remain intact and have not been decommissioned.
4. **Clean Up GCS (Optional)**:
   If a partial or corrupted lookup file was written to GCS, remove it to prevent downstream jobs from consuming bad data:
   ```bash
   gsutil rm gs://<GCS_BUCKET>/lookups/AgentADSLookup.txt
   ```