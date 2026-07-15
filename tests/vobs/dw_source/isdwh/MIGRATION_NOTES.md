# MIGRATION_NOTES.md — DW.DWH_ABPZ_KKM_AIL_AGENT

This document provides comprehensive migration notes for transitioning the legacy job `DW.DWH_ABPZ_KKM_AIL_AGENT` to Google Cloud Platform (GCP).

---

## 1. SUMMARY

The legacy job `DW.DWH_ABPZ_KKM_AIL_AGENT` has been migrated from an on-premise Automic/UC4 and Ab Initio environment to a modern, cloud-native architecture on Google Cloud Platform (GCP).

*   **Source Legacy Job:** `DW.DWH_ABPZ_KKM_AIL_AGENT` (UC4 Job calling KornShell wrapper `r_alis_objekt` executing Ab Initio Graph `BHB_CCM_PROC_WriteAgentADSLookup`).
*   **Target Platform:** Google Cloud Platform (GCP).
*   **Target Orchestration:** Cloud Composer (Airflow 2.x DAG).
*   **Target Execution Engine:** Dataproc Serverless (PySpark 3.x) and BigQuery.
*   **Business Purpose:** Extracts active agent records from the core data warehouse view, filters them based on a dynamic lookback window (84 days), and generates a tab-separated lookup file (`AgentADSLookup.txt`) used by downstream daily KKM import processes.

---

## 2. GENERATED ARTIFACTS

The migration process generated the following target files:

1.  **`dw_dwh_abpz_kkm_ail_agent.py` (Airflow DAG)**
    *   *Role:* Replaces the legacy UC4 Job, Jobplans, and KornShell wrapper scripts. It orchestrates the entire workflow, including the start-monitoring registration, the Ab Initio status polling barrier, the PySpark execution trigger, and the final end-monitoring registration.
2.  **`tmp5bupf309_write_agent_lookup.py` (PySpark Application)**
    *   *Role:* Replaces the Ab Initio Graph (`BHB_CCM_PROC_WriteAgentADSLookup` / `tmp5bupf309`). It runs on Dataproc Serverless, queries the BigQuery view `DWH$VI_S_SDM_AGENT_ADS`, filters records based on the lookback parameter, and writes the final tab-separated flat file to Google Cloud Storage (GCS).

---

## 3. KEY DESIGN DECISIONS

### Native Airflow Orchestration vs. Shell Script Wrappers
*   **Decision:** The legacy KornShell wrapper (`r_alis_objekt`) was completely retired. 
*   **Reasoning:** Rather than running shell scripts inside Airflow via `BashOperator`, we chose to implement native Airflow operators (`PythonOperator`, `BigQueryValueCheckOperator`, and `DataprocCreateBatchOperator`). This reduces container overhead, improves logging visibility, and utilizes native Airflow retry and monitoring capabilities.

### Dataproc Serverless (PySpark) for Ab Initio Graph Replacement
*   **Decision:** Migrated the Ab Initio graph logic to a PySpark application running on Dataproc Serverless.
*   **Reasoning:** Dataproc Serverless eliminates the need to provision, scale, and manage a persistent Spark cluster. It provides a highly scalable execution environment for processing large datasets while keeping operational costs low.

### Single-File Coalesced Output
*   **Decision:** The PySpark job uses `.coalesce(1)` before writing the output to GCS.
*   **Reasoning:** The downstream legacy systems expect a single flat file (`AgentADSLookup.txt`) rather than partitioned part-files. While coalescing to a single partition can limit write performance on massive datasets, the agent lookup dataset is small enough that a single-file output is highly performant and preserves downstream compatibility.

---

## 4. MANUAL STEPS BEFORE GO-LIVE

Before deploying and enabling the migrated workflow, the following manual setup steps must be completed:

### A. Schema & Dataset Creation
Ensure the target BigQuery dataset and monitoring tables exist in the target GCP project:
1.  **Variables Table:** Create the table `DW_ADM_AB_INITIO_VAR` in your target BigQuery dataset.
    ```sql
    CREATE TABLE `your_project.your_dataset.DW_ADM_AB_INITIO_VAR` (
        app_name STRING,
        status_val STRING
    );
    
    -- Seed the initial status barrier
    INSERT INTO `your_project.your_dataset.DW_ADM_AB_INITIO_VAR` (app_name, status_val)
    VALUES ('STATUS_DWH', 'go');
    ```
2.  **Source View:** Ensure the view or table `DWH$VI_S_SDM_AGENT_ADS` is migrated and populated in BigQuery.

### B. IAM & Permissions
The Cloud Composer Service Account must have the following IAM roles:
*   `roles/dataproc.editor` (To submit Dataproc Serverless batches)
*   `roles/bigquery.dataViewer` (To read from the source BigQuery views)
*   `roles/bigquery.jobUser` (To run BigQuery status checks)
*   `roles/storage.objectAdmin` (To read/write code and output files in GCS)

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | The target GCP Project ID |
| `DATAPROC_REGION` | `europe-west3` | The GCP region for Dataproc Serverless execution |
| `GCS_BUCKET` | `my-dwh-migration-bucket` | The GCS bucket for code and lookup outputs |
| `BQ_DATASET` | `dwh_core_dataset` | The BigQuery dataset containing source views and variables |

### D. Code Deployment
Upload the PySpark execution script to GCS:
*   **Source:** `tmp5bupf309_write_agent_lookup.py`
*   **Destination:** `gs://[GCS_BUCKET]/code/tmp5bupf309_write_agent_lookup.py`

---

## 5. KNOWN GAPS & UNRESOLVED REFERENCES

The following items were flagged during migration for manual follow-up:

1.  **Source View Schema Verification (`DWH$VI_S_SDM_AGENT_ADS`)**
    *   *Gap:* The physical schema of the legacy view was not available in the source code export.
    *   *Mitigation:* The PySpark script uses a fallback schema mapping (`agent_id`, `agent_name`, `agent_status`, `region_code`). A data engineer must verify that these column names match the newly migrated BigQuery view `DWH$VI_S_SDM_AGENT_ADS`.
2.  **Legacy Log Utility (`showlog.ksh`)**
    *   *Gap:* The legacy wrapper called `showlog` to parse execution logs.
    *   *Mitigation:* This utility has been retired. Cloud Composer natively routes all task logs to Cloud Logging (Stackdriver), which serves as the single source of truth for execution monitoring.
3.  **Upstream Environment Configuration (`dw_files`)**
    *   *Gap:* The job historically sourced global variables from `dw_files`.
    *   *Mitigation:* These variables have been mapped to Airflow Variables. Ensure that PR `#648` is fully merged and deployed to the target environment before running this DAG.

---

## 6. VALIDATION

To validate the migrated pipeline, perform the following steps:

### Running the Tests
1.  **Airflow DAG Dry-Run:**
    Run a task-level test in the Cloud Composer environment to verify DAG parsing and task rendering:
    ```bash
    airflow dags test dw_dwh_abpz_kkm_ail_agent 2023-06-01
    ```
2.  **Manual DAG Trigger:**
    Trigger the DAG manually from the Airflow Web UI.

### Definition of "Passing"
The migration is considered successful and ready for production when:
*   The `poll_ab_initio_status_barrier` task successfully reads `'go'` from BigQuery and proceeds.
*   The `execute_pyspark_write_agent_ads_lookup` task successfully launches a Dataproc Serverless batch.
*   A tab-separated file named `AgentADSLookup.txt` is successfully written to `gs://[GCS_BUCKET]/lookups/`.
*   The output file contains a header row and matches the expected schema structure.
*   The `dw_dwh_adm_job_monitor_end` task completes, printing the legacy success confirmation messages to the task logs.

---

## 7. ROLLBACK PROCEDURE

In the event of a critical failure during the go-live window, execute the following rollback steps:

1.  **Pause the Airflow DAG:**
    Immediately pause the DAG in the Airflow UI or via the CLI:
    ```bash
    airflow dags pause dw_dwh_abpz_kkm_ail_agent
    ```
2.  **Re-enable Legacy UC4 Job:**
    Re-activate the legacy UC4 job `DW.DWH_ABPZ_KKM_AIL_AGENT` in the Automic/UC4 scheduler.
3.  **Verify Legacy Execution:**
    Monitor the legacy execution logs to ensure the on-premise Ab Initio graph runs successfully and generates the lookup file in the legacy filesystem path.