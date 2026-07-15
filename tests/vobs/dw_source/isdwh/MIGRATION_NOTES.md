# MIGRATION_NOTES.md — DW.DWH_ABPZ_KKM_AIL_AGENT

This document provides the comprehensive migration notes for transitioning the legacy UC4 job `DW.DWH_ABPZ_KKM_AIL_AGENT` to Google Cloud Platform (GCP).

---

## 1. Summary

The daily ETL extraction and transformation job **`DW.DWH_ABPZ_KKM_AIL_AGENT`** has been migrated from a legacy UC4 and Ab Initio environment to a modern, serverless cloud architecture on Google Cloud Platform (GCP).

* **Source Platform:** UC4 Orchestrator, Unix Shell Wrappers (`r_alis_objekt`), and Ab Initio GDE (`BHB_CCM_PROC_WriteAgentADSLookup` graph).
* **Target Platform:** Google Cloud Composer (Apache Airflow) and Dataproc Serverless (PySpark).
* **Core Business Function:** Reconstructs and compiles a flat-file lookup key-store for the structural Data Warehouse (DWH) database view `DWH$VI_S_SDM_AGENT_ADS` (originally mapped to Oracle).

---

## 2. Generated Artifacts

The migration process generated two primary code artifacts to replace the legacy UC4 XML and Ab Initio wrapper configurations:

### 1. Airflow DAG File
* **Path:** `vobs/dw_source/isdwh/dw_dwh_abpz_kkm_ail_agent.py`
* **Role:** Orchestrates the daily execution. It resolves environment variables, fetches dynamic lookback parameters from the Airflow Variable Store, submits the PySpark job to Dataproc, and handles execution failures via an alarm callback.

### 2. PySpark Execution Script
* **Path:** `vobs/dw_source/isdwh/agent_ads_lookup.py`
* **Role:** Executes on Dataproc Serverless. It parses command-line arguments passed by the DAG, initializes the Spark session, replicates legacy logging structures, and isolates the transformation logic within a dedicated execution block.

---

## 3. Key Design Decisions

### Dataproc Serverless (PySpark) over Persistent Clusters
* **Decision:** Use `DataprocSubmitJobOperator` targeting a serverless execution model or dynamic cluster.
* **Reasoning:** Eliminates the overhead of maintaining 24/7 idle VM clusters for a single daily job with an Estimated Run Time (ERT) of approximately 114 seconds.

### Parameterized Lookback Dates
* **Decision:** Map the legacy UC4 variable `GET_VAR('DW.VARIABLEN_DWK_KKM','KKM_Rueckblick_Ladedatum')` to Airflow's JSON-backed Variable Store (`dw_variablen_dwk_kkm.kkm_rueckblick_ladedatum`).
* **Reasoning:** Preserves dynamic scheduling windows without hardcoding date offsets inside the DAG or PySpark code.

### Transformation Isolation Pattern
* **Decision:** Implement a strict `NotImplementedError` (exiting with code `10`) inside the PySpark transformation block.
* **Reasoning:** Because the physical Ab Initio `.mp` file was missing from the legacy codebase, this design isolates the transformation stage. This prevents the hallucination of schemas while providing a fully functional, deployable execution wrapper.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target environment before unpausing the DAG:

### 1. Schema & Dataset Creation
* Ensure the target BigQuery dataset containing the migrated view/table equivalent of `DWH$VI_S_SDM_AGENT_ADS` is created and populated.

### 2. IAM & Permissions
* The Cloud Composer environment's service account must have the following roles:
  * `roles/dataproc.editor` (to submit jobs)
  * `roles/bigquery.dataViewer` (to read source views)
  * `roles/storage.objectAdmin` (to write output lookup files to GCS)

### 3. Airflow Variables Configuration
Create the following variables in the Airflow UI (**Admin -> Variables**):

* **`GCP_PROJECT`**: `your-gcp-project-id`
* **`GCP_REGION`**: `europe-west3` (or your target region)
* **`DATAPROC_CLUSTER_NAME`**: `your-dataproc-cluster`
* **`GCS_BUCKET`**: `your-environment-gcs-bucket`
* **`dw_variablen_dwk_kkm`** (JSON format):
  ```json
  {
    "kkm_rueckblick_ladedatum": "2023-06-11"
  }
  ```

### 4. GCS Code Deployment
Upload the PySpark script to your environment's bucket:
```bash
gsutil cp vobs/dw_source/isdwh/agent_ads_lookup.py gs://<YOUR_BUCKET_NAME>/pyspark_scripts/agent_ads_lookup.py
```

---

## 5. Known Gaps & Unresolved References

### 1. Missing Ab Initio Graph Logic (Critical Redesign Item)
* **Gap:** The physical transformation logic (`BHB_CCM_PROC_WriteAgentADSLookup.mp`) was not present in the legacy source files.
* **Action Required:** A data engineer must extract the source-to-target mapping from the Ab Initio GDE and implement it inside the `execute_transformation` function in `agent_ads_lookup.py`.

### 2. Legacy Session Tracking Retirement
* **Gap:** Legacy scripts tracked execution states in Oracle tables (`dwh$ta_k_meldungen`) and managed session locks.
* **Action Taken:** These have been retired. Airflow's metadata database natively handles task auditing, execution states, and concurrency limits (`max_active_runs=1`).

---

## 6. Validation

### How to Run the Tests
1. **DAG Syntax Check:**
   Run a local compilation check on the DAG file:
   ```bash
   python3 vobs/dw_source/isdwh/dw_dwh_abpz_kkm_ail_agent.py
   ```
2. **Local PySpark Dry-Run:**
   Execute the PySpark script locally or in a development container to verify argument parsing:
   ```bash
   python3 vobs/dw_source/isdwh/agent_ads_lookup.py \
     --job_kennung "ABPZ_KKM_AIL_AGENT" \
     --rueckblick_ladedatum "2023-06-11" \
     --output_file "AgentADSLookup.txt" \
     --config "BHB_CCM_PROC_WriteAgentADSLookup.cfg"
   ```

### What "Passing" Means
* **Before Graph Implementation:** The local PySpark execution must exit with **Exit Code 10** and print the `[FATAL_GAP]` message. This confirms that the wrapper, argument parser, and safety guards are operating correctly.
* **After Graph Implementation:** The PySpark job must exit with **Exit Code 0**, and the output lookup file must be successfully written to the designated Cloud Storage bucket.

---

## 7. Rollback Procedure

If a deployment rollback is required:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dw_dwh_abpz_kkm_ail_agent` to **Off**.
2. **Stop Active Dataproc Jobs:**
   If a job is currently running, terminate it via the GCP Console (Dataproc -> Jobs) or using the gcloud CLI:
   ```bash
   gcloud dataproc jobs kill <job_id> --region=<region>
   ```
3. **Revert Code Artifacts:**
   Roll back the Git repository to the previous stable release tag and redeploy the DAG folder to Cloud Composer.