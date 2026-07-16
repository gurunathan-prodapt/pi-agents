# Migration Notes: DW.DWH_ABPZ_KKM_AIL_AGENT

## 1. Summary
The legacy UC4 workflow module `DW.DWH_ABPZ_KKM_AIL_AGENT` has been migrated to Google Cloud Platform (GCP). 

* **Source System:** UC4 (Automic) UNIX Job executing Ab Initio graphs, shell helper utilities (`r_alis_objekt`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`), and Oracle database procedures.
* **Target Platform:** Google Cloud Composer (Apache Airflow) and Google Cloud Dataproc (Serverless PySpark).
* **Core Functionality:** Compiles a flat-file lookup dataset (`AgentADSLookup.txt`) used downstream by the `DWH$VI_S_SDM_AGENT_ADS` database view. It filters and processes agent administrative and lookup information based on a dynamic historical lookback window (`&RUECKBLICK`).

---

## 2. Generated Artifacts
The migration process generated three primary files to replace the legacy UC4 and Ab Initio components:

1. **Airflow Orchestration DAG**
   * **File Path:** `dags/dw_dwh_abpz_kkm_ail_agent.py`
   * **Role:** Orchestrates the execution sequence. It replaces the UC4 XML job definition, handles start/stop logging, resolves dynamic variables, and submits the PySpark job to Dataproc.
2. **PySpark ETL Script**
   * **File Path:** `pyspark_scripts/abpz_kkm_ail_agent.py`
   * **Role:** Replaces the Ab Initio graph processing logic. It reads raw agent data from BigQuery, applies filtering and lookup rules, and writes the output file to Cloud Storage.
3. **JSON Configuration File**
   * **File Path:** `configs/BHB_CCM_PROC_WriteAgentADSLookup.json`
   * **Role:** Replaces the legacy Ab Initio configuration file (`BHB_CCM_PROC_WriteAgentADSLookup.cfg`). It externalizes dataset names, source tables, and business filtering rules.

---

## 3. Key Design Decisions

### Dataproc Serverless PySpark over GCSFuse/Shell Scripts
* **Decision:** Instead of lifting and shifting the legacy shell scripts and Ab Initio binaries into a Compute Engine VM, the processing was redesigned into a native PySpark pipeline running on Dataproc.
* **Trade-off:** Requires maintaining a PySpark script instead of a black-box Ab Initio graph, but drastically reduces licensing costs, improves scalability, and aligns with modern cloud-native data engineering standards.

### Externalized JSON Configuration
* **Decision:** The legacy `.cfg` file was converted into a structured `.json` configuration file stored in Google Cloud Storage (GCS).
* **Trade-off:** The PySpark script must perform an extra read operation to parse the JSON configuration at runtime, but this decouples business rules (such as active status codes and restricted agent types) from the execution code.

### Verbatim Logging Preservation
* **Decision:** Legacy German log outputs from UC4 includes (`DW.LESE_LOG`, `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`, etc.) and shell wrappers (`r_alis_objekt`) are preserved character-for-character inside Airflow Python operators.
* **Trade-off:** Adds minor boilerplate code to the Airflow DAG, but ensures absolute compliance with legacy operational monitoring patterns and audit trails.

---

## 4. Manual Steps Before Go-Live

### Schema & Dataset Creation
Ensure the source BigQuery dataset and table exist in the target project:
* **Dataset:** `dwh_kern_bi` (or the override value specified in your Airflow variables)
* **Table:** `agent_raw_data`

### IAM & Permissions
The Cloud Composer Service Account must have the following IAM roles:
* **Dataproc Editor** (`roles/dataproc.editor`) or **Dataproc Worker** (`roles/dataproc.worker`)
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the source dataset.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the target GCS bucket.

### Airflow Variables
The following Airflow variables must be configured in the Cloud Composer environment:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-prod` | Target GCP Project ID |
| `DATAPROC_REGION` | `europe-west3` | Target execution region |
| `DATAPROC_CLUSTER` | `my-dataproc-cluster` | Target Dataproc cluster name |
| `GCS_BUCKET` | `my-dwh-migration-bucket` | GCS bucket for scripts and outputs |
| `KKM_Rueckblick_Ladedatum` | `84` | Historical lookback window (days) |

### GCS Directory Structure
Upload the generated artifacts to your GCS bucket matching the following structure:
```text
gs://<GCS_BUCKET>/
├── configs/
│   └── BHB_CCM_PROC_WriteAgentADSLookup.json
└── pyspark_scripts/
    └── abpz_kkm_ail_agent.py
```

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) Items & Legacy Database Procedures
* **Oracle Package Dependencies:** The legacy shell utility library `f_alis_msgerr.ksh` relied on Oracle procedures `DWPA_MELDUNG.SetzeZusatzInfos`, `DWPA_MELDUNG.Fehler`, and `DWH$VS_MELDUNG.LogAusgabe_Debug`. 
* **Resolution Status:** These database procedures have **not** been migrated to BigQuery. Instead, standard Airflow task failure callbacks (`on_failure_callback`) and native Cloud Logging have been implemented. Any downstream monitoring system relying on the Oracle `MELDUNG` tables must be updated to consume Cloud Logging events.
* **Unresolved Utility (`showlog.ksh`):** Flagged during codebase scans as an unconfirmed dependency. It has been verified as a non-blocking legacy utility wrapper. No action is required as native Cloud Composer task logs supersede it.

---

## 6. Validation

### How to Run the Tests
1. **Dry Run:** Trigger the DAG manually from the Airflow UI with an empty configuration or custom run parameters.
2. **Local PySpark Validation:** Run the PySpark script locally or on a development Dataproc cluster using mock data:
   ```bash
   python abpz_kkm_ail_agent.py \
     --config "configs/BHB_CCM_PROC_WriteAgentADSLookup.json" \
     --output "gs://<TEST_BUCKET>/test_output/AgentADSLookup.txt" \
     --rueckblick 84 \
     --gcs_bucket "<TEST_BUCKET>" \
     --project_id "<TEST_PROJECT>"
   ```

### What "Passing" Means
* **Task Execution:** All four DAG tasks (`dw_dwh_adm_job_monitor_start` -> `log_pre_execution` -> `dw_dwh_abpz_kkm_ail_agent` -> `log_post_execution`) complete with a `success` state.
* **Log Verification:** Airflow task logs display the verbatim German legacy strings, including:
  * `Jobkennung ABPZ_KKM_AIL_AGENT eingetragen für...`
  * `Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.`
* **Output Artifact:** A pipe-delimited file named `AgentADSLookup.txt` is successfully written to `gs://<GCS_BUCKET>/lookups/` containing the filtered agent records.

---

## 7. Rollback Procedure
In the event of a deployment failure or data corruption:

1. **Pause the DAG:** Immediately pause the `dw_dwh_abpz_kkm_ail_agent` DAG in the Airflow UI to prevent further scheduled or manual executions.
2. **Revert GCS Artifacts:** Restore the previous versions of `abpz_kkm_ail_agent.py` and `BHB_CCM_PROC_WriteAgentADSLookup.json` from your Git repository history to GCS.
3. **Clean Target Directory:** Delete any corrupted lookup files from the target GCS path:
   ```bash
   gcloud storage rm gs://<GCS_BUCKET>/lookups/AgentADSLookup.txt
   ```
4. **Legacy Fallback (If Dual-Running):** If the legacy UC4 environment is still active, re-enable the legacy UC4 job `DW.DWH_ABPZ_KKM_AIL_AGENT` to resume daily processing on the legacy infrastructure.