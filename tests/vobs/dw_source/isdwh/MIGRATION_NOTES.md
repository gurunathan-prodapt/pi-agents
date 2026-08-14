# Migration Notes: DW.CCM_WRITE_CONTRACTMAPLOOKUP

This document details the migration of the UC4 Unix job `DW.CCM_WRITE_CONTRACTMAPLOOKUP` and its associated Ab Initio graph `BHB_CCM_PROC_WriteContractMapLookup.mp` to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy job `DW.CCM_WRITE_CONTRACTMAPLOOKUP` was responsible for extracting contract map attributes from an Oracle database, sorting them, writing them to a delimited flat file, and updating execution metadata via an Oracle stored procedure. 

This workload has been migrated from an on-premise **UC4 / Unix / Ab Initio** environment to **GCP**, utilizing **Cloud Composer (Apache Airflow)** for orchestration, **Dataproc Serverless (PySpark)** for data processing, **Google BigQuery** as the data warehouse, and **Google Cloud Storage (GCS)** for file storage.

---

## 2. Generated Artifacts

The migration process generated the following files:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.py` | **Airflow DAG** | Orchestrates the execution of the PySpark job on Dataproc Serverless. Replaces the legacy UC4 job definition. |
| `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py` | **PySpark Script** | Implements the core ETL logic: reads from BigQuery, sorts, deduplicates, writes a delimited file to GCS, and calls the BigQuery stored procedure. Replaces the Ab Initio graph. |
| `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh` | **Retired** | This KornShell wrapper script has been retired to eliminate redundant orchestration layers. |

---

## 3. Key Design Decisions

### Retirement of the KornShell Wrapper
The legacy wrapper script `BHB_CCM_PROC_WriteContractMapLookup.ksh` was used to set environment variables and trigger the Ab Initio graph. In the target architecture, environment variables are managed via Airflow/GCP, and the PySpark script is executed directly by the Airflow DAG. Retiring the `.ksh` script avoids redundant logic and simplifies the execution chain.

### Single-File GCS Output Pattern
PySpark writes output files in parallel, resulting in directory structures containing multiple `part-*.csv` files. To match the legacy requirement of generating a single, cleanly named flat file (`ContractMapLookup.txt`), the PySpark script:
1. Coalesces the DataFrame to a single partition (`coalesce(1)`).
2. Writes the output to a temporary GCS directory.
3. Uses the Google Cloud Storage client library to locate the generated part file, copy it to the final destination (`gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt`), and clean up the temporary directory.

### Delimiter Configuration
The legacy Ab Initio graph used the `\001` (SOH) character as a field delimiter and `\n` as a line terminator. This formatting has been preserved in the PySpark CSV writer options (`delimiter="\u0001"`) to ensure downstream compatibility.

### BigQuery Stored Procedure Integration
The legacy graph executed an Oracle stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` to update load tracking tables. This has been replaced by a native BigQuery stored procedure call (`CALL {GCP_PROJECT}.{BQ_DATASET}.SetzeLadedatumAbInitio(...)`) executed via the BigQuery Python client within the PySpark job.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated workflow, the following setup steps must be completed:

### 1. BigQuery Schema & Dataset Creation
Ensure the target BigQuery dataset and table exist:
* **Dataset**: `{BQ_DATASET}` (e.g., `ccm_proc`)
* **Table**: `dwh_ta_l_map_vt_carm_dwh`
* **Stored Procedure**: `SetzeLadedatumAbInitio` must be migrated from Oracle PL/SQL to BigQuery SQL and compiled within the target dataset.

### 2. IAM & Permissions
The Service Account running the Cloud Composer environment and Dataproc Serverless workloads must have the following roles:
* `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target BigQuery dataset.
* `roles/storage.objectAdmin` on the target GCS bucket.
* `roles/dataproc.worker` and `roles/dataproc.editor` for Dataproc Serverless execution.

### 3. Airflow Variables
The following Airflow variables must be configured in the Cloud Composer environment:

```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "GCP_REGION": "your-gcp-region",
  "DATAPROC_REGION": "your-dataproc-region",
  "DATAPROC_CLUSTER": "your-dataproc-cluster-name",
  "GCS_BUCKET": "your-gcs-bucket-name",
  "BQ_DATASET": "your_bigquery_dataset"
}
```

### 4. PySpark Script Deployment
Upload the generated PySpark script to GCS:
* **Source**: `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py`
* **Destination**: `gs://{GCS_BUCKET}/pyspark_scripts/bhb_ccm_proc_writecontractmaplookup.py`

### 5. Scheduling
The Airflow DAG is configured with `schedule=None` because the legacy job was externally triggered. Once the parent workflow `DW.CCM_PROC_JP` is migrated, this DAG should be integrated as a sub-task group or triggered via a `TriggerDagRunOperator`.

---

## 5. Known Gaps & Unresolved References

* **Downstream Consumer (`DW.CCM_PROC_JP`)**: The parent/downstream job plan `DW.CCM_PROC_JP` has not yet been migrated. The final cross-DAG triggering mechanism or task-group integration cannot be completed until that migration pass occurs.
* **Stored Procedure Verification**: The translation of `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` to BigQuery must be manually verified to ensure it correctly updates the metadata tables.

---

## 6. Validation

To validate the migration, perform the following tests:

### Local/Interactive PySpark Test
Submit the PySpark job manually using the gcloud CLI to verify data processing:

```bash
gcloud dataproc batches submit pyspark \
    gs://{GCS_BUCKET}/pyspark_scripts/bhb_ccm_proc_writecontractmaplookup.py \
    --project={GCP_PROJECT} \
    --region={DATAPROC_REGION} \
    --env-vars GCP_PROJECT={GCP_PROJECT},GCS_BUCKET={GCS_BUCKET},BQ_DATASET={BQ_DATASET} \
    -- --first_day 20050217 --last_day_plus_1 20050218
```

### Airflow DAG Test
Trigger the Airflow DAG manually from the Airflow UI or via the CLI:

```bash
airflow dags trigger dw_ccm_write_contractmaplookup
```

### Success Criteria
The validation is considered **passing** if:
1. The Dataproc Serverless batch job completes with an `EXIT_CODE 0`.
2. A single file is created at `gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt`.
3. The file contains the extracted attributes, delimited by `\001`, sorted by `vertrags_id` in ascending order, with no duplicate `vertrags_id` keys.
4. The BigQuery stored procedure `SetzeLadedatumAbInitio` executes successfully and updates the load tracking metadata.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, execute the following rollback steps:

1. **Pause the Airflow DAG**:
   ```bash
   airflow dags pause dw_ccm_write_contractmaplookup
   ```
2. **Re-enable the Legacy UC4 Job**:
   * Log into the UC4/Automic interface.
   * Locate the job `DW.CCM_WRITE_CONTRACTMAPLOOKUP`.
   * Set the active flag back to `1` (Active).
3. **Verify Legacy Environment**:
   * Ensure the on-premise Oracle database and the local filesystem paths for `$CCM_PROC_ContractMapLookupFilename` are accessible and active.
   * Verify that the wrapper script `BHB_CCM_PROC_WriteContractMapLookup.ksh` is intact on the host `|DWHDWH2P|HOST`.