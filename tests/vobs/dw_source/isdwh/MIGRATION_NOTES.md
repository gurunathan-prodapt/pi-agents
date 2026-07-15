# Migration Notes: DW.DWH_ABPZ_KKM_AIL_AGENT

## 1. Summary
The `DW.DWH_ABPZ_KKM_AIL_AGENT` workflow has been migrated from a legacy UC4 UNIX-based job executing an Ab Initio graph to a modern, cloud-native orchestration and processing model on Google Cloud Platform (GCP).

* **Source Platform:** UC4 (Orchestration) & Ab Initio GDE (Data Processing) running on a legacy UNIX environment.
* **Target Platform:** Google Cloud Composer (Apache Airflow) for orchestration and Google Cloud Dataproc Serverless (PySpark) for data processing.
* **Functional Purpose:** Builds a tab-delimited flat-file lookup (`AgentADSLookup.txt`) supporting the `DWH$VI_S_SDM_AGENT_ADS` database view. This lookup is updated daily as part of the core data warehouse (`DWH_KERN`) daily load sequence.

---

## 2. Generated Artifacts
The migration process generated the following files, which replace the legacy UC4 XML exports, wrapper scripts, and Ab Initio configurations:

### 1. Orchestration DAG
* **File Path:** `dags/dw_dwh_abpz_kkm_ail_agent.py`
* **Role:** Airflow DAG that orchestrates the execution lifecycle. It handles pre-execution synchronization, resolves global and job-specific parameters, and triggers the Dataproc Serverless PySpark task.

### 2. Processing Script
* **File Path:** `pyspark_scripts/abpz_kkm_ail_agent.py`
* **Role:** PySpark application that replaces the Ab Initio GDE graph (`ABPZ_KKM_AIL_AGENT`) and its wrapper scripts (`r_alis_objekt`, `h_alis_date.ksh`, etc.). It reads configurations from Cloud Storage, queries BigQuery, performs date calculations, and exports the tab-delimited flat file.

---

## 3. Key Design Decisions

### Dataproc Serverless (PySpark) vs. Standard Dataproc
* **Decision:** Selected Dataproc Serverless to run the PySpark workload.
* **Reasoning:** Eliminates the operational overhead of managing, scaling, and configuring persistent VM-based clusters for a daily batch job that runs for a short duration. This reduces idle compute costs.

### Consolidation of Legacy Shell Utilities
* **Decision:** Retired legacy shell utilities (`h_alis_date.ksh`, `h_alis_parameter.ksh`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`) and folded their logic directly into Python/PySpark.
* **Reasoning:** 
  * Legacy date calculations using Oracle SQL*Plus are replaced by Python's native `datetime` module.
  * Custom Oracle error logging is replaced by native Google Cloud Logging and Airflow task logs.
  * Path resolution (`DW.HOLE_PFAD`) is replaced by Cloud Storage URI templates (`gs://{bucket}/...`).

### Parameterization and Configuration Management
* **Decision:** Extracted hardcoded environment configurations and mapped them to Airflow Variables (`Variable.get()`) and dynamic task arguments.
* **Reasoning:** Decouples environment-specific parameters (Project IDs, Bucket Names, Cluster Regions) from the code, enabling seamless promotion across Development, Test, and Production environments without code modifications.

### Single-File Output Coalescing
* **Decision:** Used `.coalesce(1)` in the PySpark export step before writing to GCS.
* **Reasoning:** Spark writes data in parallel partitions by default. Because downstream legacy systems expect a single flat file (`AgentADSLookup.txt`), coalescing forces Spark to merge partitions into a single output file, preserving downstream compatibility.

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Dataset Creation
Ensure the target BigQuery dataset and source view exist in the target GCP project:
* Verify that the BigQuery view representing `DWH$VI_S_SDM_AGENT_ADS` is deployed and accessible.
* Create the target lookup dataset directory in GCS if it does not exist:
  ```bash
  gcloud storage buckets create gs://<YOUR_BUCKET_NAME> --location=<YOUR_REGION>
  ```

### 2. IAM & Permissions
The Cloud Composer Service Account and the Dataproc VM Service Account must have the following IAM roles:
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the source dataset/view.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the processing project.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the GCS bucket containing scripts, configurations, and outputs.
* **Dataproc Worker** (`roles/dataproc.worker`) to execute Serverless Spark workloads.

### 3. Airflow Variables Configuration
Define the following Airflow Variables in the Cloud Composer environment (via Airflow UI -> Admin -> Variables or gcloud CLI):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-project` | Target GCP Project ID |
| `DATAPROC_REGION` | `europe-west3` | GCP Region for Dataproc execution |
| `DATAPROC_CLUSTER` | `dataproc-ephemeral-cluster` | Target cluster name/identifier |
| `GCS_BUCKET` | `prod-dwh-assets-bucket` | GCS Bucket for scripts and configs |

### 4. Upload Assets to Cloud Storage
Upload the PySpark script and the legacy configuration file to their respective GCS paths:
```bash
# Upload PySpark execution script
gcloud storage cp pyspark_scripts/abpz_kkm_ail_agent.py gs://<YOUR_BUCKET_NAME>/pyspark_scripts/abpz_kkm_ail_agent.py

# Upload configuration file
gcloud storage cp vobs/dw_source/isdwh/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg gs://<YOUR_BUCKET_NAME>/config/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg
```

### 5. Scheduling & Triggering
* The DAG is configured with `schedule_interval=None` to match its legacy behavior as a dependency-driven task.
* Integrate this DAG into the master daily load DAG using an `TriggerDagRunOperator` or external orchestration tool.

---

## 5. Known Gaps & Unresolved References

### 1. BigQuery View Resolution
* **Gap:** The legacy code references the Oracle view `DWH$VI_S_SDM_AGENT_ADS`. 
* **Resolution Required:** Ensure that the BigQuery equivalent view (mapped as `dw_sdm_agent_ads` in `JOB_CONFIG`) is fully deployed and contains identical column definitions (`AGENT_ID`, `AGENT_NAME`, `AGENT_STATUS`, `LAST_UPDATE_TIMESTAMP`).

### 2. Configuration File Parsing
* **Gap:** The PySpark script contains a basic parser for `BHB_CCM_PROC_WriteAgentADSLookup.cfg`. If this configuration file contains complex Ab Initio conditional parameters or shell-interpolated variables, the parser may require enhancement.
* **Resolution Required:** Review the `.cfg` file contents to ensure all parameters are static key-value pairs.

### 3. Non-Blocking Legacy Scripts
* **Gap:** The legacy reference to `showlog.ksh` was identified as a non-blocking candidate with no physical file path in the source export.
* **Resolution Required:** This has been retired. Standard Cloud Logging outputs replace this invocation.

---

## 6. Validation

### How to Run the Tests
1. **Dry Run (Airflow Compiler Check):**
   Run a DAG structural integrity test to ensure the Airflow scheduler can parse the DAG without syntax errors:
   ```bash
   python dags/dw_dwh_abpz_kkm_ail_agent.py
   ```
2. **Manual DAG Trigger:**
   Trigger the DAG manually from the Cloud Composer Airflow UI or via the gcloud CLI:
   ```bash
   gcloud composer environments run <COMPOSER_ENV_NAME> \
       --location <REGION> \
       dags trigger -- dw_dwh_abpz_kkm_ail_agent
   ```
3. **Dataproc Job Verification:**
   Monitor the Dataproc job execution in the GCP Console under **Dataproc -> Jobs**.

### What "Passing" Means
* The Airflow DAG transitions to a `SUCCESS` state.
* The Dataproc job logs output the legacy-compliant termination headers:
  ```text
  ==========================================================================
  Starting Job Step Validation Context for identifier: ABPZ_KKM_AIL_AGENT
  ==========================================================================
  ...
  --------------------------------------------------------------------------
  Rueckgabewert: '0'
  Der Status fuer den Pruefjob wurde erfolgreich auf BEENDET gesetzt.
  Execution Completed Successfully at GCP UTC timestamp: <TIMESTAMP>
  ==========================================================================
  ```
* A single tab-delimited file is successfully written to `gs://<YOUR_BUCKET_NAME>/lookups/AgentADSLookup.txt` and contains valid data extracted from the BigQuery view.

---

## 7. Rollback Procedure

In the event of a critical failure or data mismatch post-deployment, execute the following rollback steps:

1. **Pause the Airflow DAG:**
   Immediately pause the migrated DAG to prevent subsequent scheduled or triggered runs:
   ```bash
   gcloud composer environments run <COMPOSER_ENV_NAME> \
       --location <REGION> \
       dags pause -- dw_dwh_abpz_kkm_ail_agent
   ```
2. **Re-enable Legacy UC4 Job:**
   Re-activate the legacy `DW.DWH_ABPZ_KKM_AIL_AGENT` job in the UC4 scheduler interface.
3. **Redirect Downstream Consumers:**
   If downstream processes were modified to read the lookup file from GCS (`gs://...`), revert their configuration paths to point back to the legacy UNIX mount directory where the original `AgentADSLookup.txt` is generated.
4. **Investigate Logs:**
   Analyze the execution logs in Google Cloud Logging under the resource type `Cloud Dataproc Job` using the specific Job ID generated during the failed run.