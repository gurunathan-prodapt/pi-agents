# MIGRATION_NOTES.md

**Job ID:** `DW.DWH_ABPZ_KKM_AIL_AGENT`  
**Target Architecture:** Cloud Composer (Apache Airflow) + Dataproc Serverless (PySpark)

---

## 1. Summary

This document details the migration of the legacy UC4/Automic orchestration job `DW.DWH_ABPZ_KKM_AIL_AGENT` to Google Cloud Platform (GCP). 

* **Legacy Platform:** UC4/Automic wrapping a Unix shell execution block (`JOBS_UNIX`) on host `|DWHDWH1P|`. It executed an Ab Initio graph (`ABPZ_KKM_AIL_AGENT`) via the wrapper script `r_alis_objekt` to extract agent lookup data and build a flat-file lookup (`AgentADSLookup.txt`) for the downstream database view `DWH$VI_S_SDM_AGENT_ADS`.
* **Target Platform:** Cloud Composer (Apache Airflow) orchestrating a Dataproc Serverless PySpark job.
* **Migration Scope:** The legacy shell scripts, Ab Initio graph logic, and UC4 scheduling configurations have been refactored into a native Airflow DAG and a PySpark extraction script.

---

## 2. Generated Artifacts

The migration process generated the following files, which must be deployed to their respective locations in the target environment:

### 1. Airflow DAG File
* **Path:** `vobs/dw_source/isdwh/dw_dwh_abpz_kkm_ail_agent_dag.py`
* **Role:** Orchestrates the execution flow. It defines the DAG `dw_dwh_abpz_kkm_ail_agent_dag`, manages execution parameters, handles task sequencing, and implements failure/success logging callbacks that mimic legacy outputs.

### 2. PySpark Executable Script
* **Path:** `vobs/dw_source/isdwh/agent_ads_lookup.py`
* **Role:** Replaces the legacy Ab Initio graph logic. It initializes a Spark session, queries the source view (`DWH_VI_S_SDM_AGENT_ADS`), applies date filters, formats the output as a semicolon-delimited flat file, and writes the result to Google Cloud Storage (GCS).

---

## 3. Key Design Decisions

### Dataproc Serverless (PySpark) over Lift-and-Shift
* **Decision:** Refactor the Ab Initio graph logic into a native PySpark script (`agent_ads_lookup.py`) executed via `DataprocSubmitJobOperator`.
* **Reasoning:** Eliminates licensing costs and infrastructure overhead associated with running legacy Ab Initio runtimes on GCP VMs. PySpark provides native scalability, integrates seamlessly with GCS, and allows direct querying of modern cloud data warehouses (e.g., BigQuery or Hive metastores).

### Semicolon-Delimited Flat-File Generation
* **Decision:** The PySpark script uses `F.concat_ws(";", ...)` and coalesces the DataFrame to a single partition (`coalesce(1)`) before writing as text.
* **Reasoning:** This guarantees strict compatibility with downstream legacy systems expecting the exact Ab Initio flat-file format (`AgentADSLookup.txt`) without requiring immediate downstream modifications.

### Legacy Log Retention & Callbacks
* **Decision:** Implemented custom Python callbacks (`on_task_failure_callback` and `on_dag_success_callback`) within the DAG.
* **Reasoning:** Replaces the legacy `showlog.ksh` utility. It ensures that exact legacy log patterns (e.g., `"Jobkennung ABPZ_KKM_AIL_AGENT eingetragen für..."` and `"Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet."`) are preserved in Cloud Logging for operational continuity and automated log-scraping tools.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following manual setup steps must be completed:

### 1. Schema & Dataset Verification
* Ensure that the source view/table `DWH_VI_S_SDM_AGENT_ADS` is fully migrated and accessible within the Spark SQL catalog (e.g., Dataproc Metastore or Hive Metastore).
* Verify that the columns `AGENT_ID`, `AGENT_NAME`, `AGENT_STATUS`, `ADS_DOMAIN`, `ADS_USER_ID`, `EMAIL`, `UPDATE_TIMESTAMP`, and `LAST_MODIFIED_DATE` exist and match the expected types.

### 2. IAM & Permissions
* The Cloud Composer worker service account must have the following IAM roles:
  * `roles/dataproc.editor` (to submit Dataproc Serverless jobs)
  * `roles/storage.objectAdmin` on the target GCS bucket (`GCS_BUCKET`)

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-project` | Target GCP Project ID |
| `DATAPROC_REGION` | `europe-west3` | GCP Region for Dataproc execution |
| `DATAPROC_CLUSTER` | `dataproc-ephemeral-cluster` | Target Dataproc cluster name |
| `GCS_BUCKET` | `prod-dwh-data-bucket` | GCS bucket for scripts and output lookups |
| `dwh_home` | `/opt/dwh` | Root path for DWH configurations |
| `kkm_rueckblick_ladedatum` | `2026-01-01` | Lookback date parameter for delta loads |

### 4. Artifact Deployment
* Upload the PySpark script to GCS:  
  `gs://<GCS_BUCKET>/pyspark_scripts/agent_ads_lookup.py`
* Upload the legacy configuration file (if referenced by downstream logic) to GCS:  
  `gs://<GCS_BUCKET>/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg`
* Place the DAG file in the Composer DAGs folder:  
  `gs://<COMPOSER_DAG_BUCKET>/dags/dw_dwh_abpz_kkm_ail_agent_dag.py`

---

## 5. Known Gaps & Unresolved References

### 1. Missing Ab Initio Config Context (B4 Redesign Item)
* **Gap:** The original Ab Initio configuration file `BHB_CCM_PROC_WriteAgentADSLookup.cfg` and its internal graph logic were not fully provided.
* **Resolution/Mitigation:** The PySpark script implements a standard SQL extraction query based on the view `DWH_VI_S_SDM_AGENT_ADS`. **Action Required:** Data engineers must verify if there are additional complex transformation rules or filtering logic inside the legacy Ab Initio graph that need to be manually ported into the `process_lookup_extraction` function in `agent_ads_lookup.py`.

### 2. Upstream Dependency Sequencing
* **Gap:** The parent Job Plan (`JOBP`) and schedule definitions were missing from the source XML.
* **Resolution/Mitigation:** The DAG is currently configured with a placeholder daily schedule (`0 3 * * *`). **Action Required:** Once the parent UC4 orchestration workflows are migrated, update the DAG schedule or implement `ExternalTaskSensor` tasks to align with upstream prerequisites.

---

## 6. Validation

To validate the migration, execute the following test steps:

### 1. Local/Dev Execution
Trigger the DAG manually from the Airflow UI:
```bash
gcloud composer environments run <COMPOSER_ENV_NAME> \
    --location <REGION> \
    dags trigger -- dw_dwh_abpz_kkm_ail_agent_dag
```

### 2. Verification of "Passing" Status
The run is successful if:
1. The task `dw_dwh_abpz_kkm_ail_agent` completes with a `SUCCESS` status.
2. The Airflow task logs contain the legacy success string:  
   `Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.`
3. A single, non-empty, semicolon-delimited text file is generated in GCS at:  
   `gs://<GCS_BUCKET>/lookups/agent/AgentADSLookup.txt`
4. The generated file schema matches the legacy format:  
   `AGENT_ID;AGENT_NAME;AGENT_STATUS;ADS_DOMAIN;ADS_USER_ID;EMAIL;UPDATE_TIMESTAMP`

---

## 7. Rollback Procedure

In the event of an operational failure or data mismatch on the target platform, perform the following steps to roll back:

1. **Pause the Airflow DAG:**  
   Disable the DAG `dw_dwh_abpz_kkm_ail_agent_dag` in the Airflow UI or via the CLI:
   ```bash
   gcloud composer environments run <COMPOSER_ENV_NAME> \
       --location <REGION> \
       dags pause -- dw_dwh_abpz_kkm_ail_agent_dag
   ```
2. **Re-enable Legacy UC4 Job:**  
   Re-activate the `DW.DWH_ABPZ_KKM_AIL_AGENT` job in the UC4/Automic console.
3. **Verify Legacy Execution:**  
   Trigger the legacy UC4 job manually and verify that `AgentADSLookup.txt` is successfully written to the legacy shared file path and that downstream views are updated correctly.