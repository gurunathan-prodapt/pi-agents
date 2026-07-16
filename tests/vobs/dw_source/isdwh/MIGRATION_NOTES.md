# MIGRATION NOTES
**Job:** `DW.DWH_ABPZ_KKM_AIL_AGENT`  
**Source Platform:** Automic/UC4, KornShell (KSH), Ab Initio  
**Target Platform:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow), Dataproc Serverless (PySpark), BigQuery  

---

## 1. Summary

The legacy daily KKM Agent lookup import workflow (`DW.DWH_ABPZ_KKM_AIL_AGENT`) has been migrated from an on-premises Automic/UC4 and KornShell orchestration framework running Ab Initio graphs to a cloud-native architecture on Google Cloud Platform.

### 1.1 Scope of Migration
* **Orchestration:** Legacy UC4 XML job definitions, wrapper scripts (`r_alis_objekt`), and status checking includes (`DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`, `DW.LESE_LOG`, etc.) are consolidated into a single, modular **Google Cloud Composer (Apache Airflow) DAG**.
* **Transformation Logic:** The Ab Initio lookup construction logic defined by `BHB_CCM_PROC_WriteAgentADSLookup.cfg` is migrated to a **Dataproc Serverless PySpark pipeline**.
* **Date & Utility Calculations:** Legacy date arithmetic and string manipulation utilities (`h_alis_date.ksh`) are converted into native **BigQuery SQL Stored Procedures and JavaScript/SQL User-Defined Functions (UDFs)**.

### 1.2 Target Architecture Overview
```
       +-------------------------------------------------------+
       |             Cloud Composer (Airflow DAG)              |
       |  - Manages execution sequence and task dependencies   |
       |  - Handles failure alerts and state logging           |
       +-------------------------------------------------------+
                                   |
         +-------------------------+-------------------------+
         |                                                   |
         v                                                   v
+------------------------------------+             +----------------------------------+
|      Dataproc Serverless Task      |             |       BigQuery Engine (SQL)      |
|  - Runs PySpark pipeline           |             |  - Executes stored procedures    |
|  - Queries BQ source view          |             |  - Evaluates date libraries      |
|  - Generates AgentADSLookup.txt    |             |  - Updates metadata run states   |
+------------------------------------+             +----------------------------------+
```

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy components. Each file must be deployed to its respective directory in the target repository.

### 2.1 Orchestration & Pipelines
* **`dags/dw_dwh_abpz_kkm_ail_agent.py`**  
  *Role:* The primary Airflow DAG. It orchestrates the execution sequence, evaluates conditional branching, triggers the Dataproc PySpark job, and handles logging and failure callbacks.
* **`pyspark/bhb_ccm_proc_write_agent_ads_lookup.py`**  
  *Role:* The core PySpark transformation script. It reads the source view `DWH$VI_S_SDM_AGENT_ADS` from BigQuery, deduplicates records to find the latest active directory state per agent, formats the output columns into a pipe-delimited structure, and writes the flat-file lookup (`AgentADSLookup.txt`) to Google Cloud Storage (GCS).

### 2.2 Database & Library Assets
* **`gcp/dwh/sql/h_alis_date_library.sql`**  
  *Role:* Contains the BigQuery SQL Stored Procedures and JavaScript UDFs that replace the legacy `h_alis_date.ksh` utility. It provides functions such as `DWDate_Vormonat`, `DWDate_Datum_Check`, and `udf_adjust_zeitbereich` to handle complex date algebra.
* **`dags/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.json`**  
  *Role:* JSON configuration file containing metadata parameters for the lookup pipeline, replacing the legacy `.cfg` file.

---

## 3. Key Design Decisions

### 3.1 Cloud Composer + Dataproc Serverless (PySpark)
* **Decision:** Replaced the legacy `UC4 + KSH + AbInitio` pattern with Cloud Composer and Dataproc Serverless.
* **Reasoning:** Dataproc Serverless eliminates the overhead of managing and scaling physical VM clusters. PySpark provides native, high-performance connectors to BigQuery and GCS, allowing the pipeline to scale dynamically based on data volume.

### 3.2 BigQuery JavaScript UDFs for Date Algebra
* **Decision:** Migrated the complex date arithmetic parser (`SubtrahiereZeitbereich`, `AddiereZeitbereich`) to a JavaScript-based User-Defined Function (`udf_adjust_zeitbereich`) embedded directly inside BigQuery.
* **Reasoning:** The legacy script relied on POSIX `dc` (arbitrary-precision calculator) and `sed` stack operations to parse custom interval strings (e.g., `-1m3t`, `1y3d45i`). Re-implementing this parser in JavaScript inside BigQuery allows high-performance, database-native date evaluations without spinning up external compute resources.

### 3.3 Literal Log Preservation
* **Decision:** Retained legacy German log outputs and success/failure string assertions (e.g., `"Rueckgabewert: '1' (Fehlerfall)"`, `"!FEHLER gemeldet!"`) within the Python and SQL code.
* **Reasoning:** This ensures that downstream log-scraping, monitoring, and auditing tools scanning Google Cloud Logging (Stackdriver) can detect pipeline states without requiring modifications to their regex patterns.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target GCP environment prior to scheduling the migrated workflow.

### 4.1 Schema & Dataset Creation
Ensure the target BigQuery dataset exists and deploy the date library procedures:
1. Create the BigQuery dataset (if not already present):
   ```bash
   bq mk --dataset --location=europe-west3 your_project_id:dwh_kkm
   ```
2. Execute the SQL script `gcp/dwh/sql/h_alis_date_library.sql` in the BigQuery console to register the UDFs and stored procedures.
3. Ensure the metadata tracking table is initialized:
   ```sql
   CREATE TABLE IF NOT EXISTS `your_project_id.dwh_kkm.metadata_run_state` (
     run_id STRING NOT NULL,
     pipeline_name STRING NOT NULL,
     business_date DATE NOT NULL,
     run_mode STRING NOT NULL,
     status STRING NOT NULL,
     records_processed INT64,
     run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
   )
   PARTITION BY DATE(run_timestamp)
   CLUSTER BY pipeline_name, status;
   ```

### 4.2 IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
* **Dataproc Editor** (`roles/dataproc.editor`) and **Dataproc Worker** (`roles/dataproc.worker`) to submit and run Serverless PySpark batches.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery Job User** (`roles/bigquery.jobUser`) to query the source view and write metadata states.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the GCS bucket hosting the PySpark scripts and output lookups.

### 4.3 Airflow Variables & Connections
Configure the following Airflow Variables in the Cloud Composer UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT_ID` | `prod-dwh-platform-123` | The target GCP Project ID |
| `GCP_REGION` | `europe-west3` | The GCP region for Dataproc and Composer |
| `GCS_BUCKET` | `prod-dwh-composer-bucket` | The GCS bucket containing code and staging assets |
| `BQ_DATASET` | `dwh_kkm` | The target BigQuery dataset |

---

## 5. Known Gaps & Unresolved References

### 5.1 Legacy `showlog.ksh` Utility
* **Gap:** The legacy pipeline invoked a custom binary utility (`$HOME/tools/showlog`) during task failures to format and route alerts. This utility is not present in the cloud environment.
* **Resolution:** The legacy utility has been retired. Task failures are now handled natively by the Airflow DAG's `on_failure_callback` (`parse_failure_log`), which writes structured error logs to Stackdriver. 
* **Follow-up:** If real-time notifications (e.g., Slack, PagerDuty, or Email) are required, an Airflow notification provider must be configured within the `on_failure_callback` function.

### 5.2 Source View Availability
* **Gap:** The PySpark pipeline queries the view `DWH$VI_S_SDM_AGENT_ADS`. 
* **Resolution:** Ensure that the DDL defining this view has been migrated and deployed to the target BigQuery dataset before executing the DAG.

---

## 6. Validation

To validate the migration, execute the pipeline in a test environment and verify the outputs against the legacy execution results.

### 6.1 Running the Validation Test
1. Upload the PySpark script to GCS:
   ```bash
   gsutil cp pyspark/bhb_ccm_proc_write_agent_ads_lookup.py gs://[YOUR_GCS_BUCKET]/pyspark/
   ```
2. Trigger the Airflow DAG manually from the Composer UI or via the gcloud CLI:
   ```bash
   gcloud composer environments run [COMPOSER_ENV_NAME] \
       --location [REGION] \
       dags trigger -- dw_dwh_abpz_kkm_ail_agent
   ```

### 6.2 Definition of "Passing"
The validation run is successful if:
* The Airflow DAG execution completes with a `SUCCESS` status.
* The PySpark task successfully reads from `DWH$VI_S_SDM_AGENT_ADS` and writes a single, pipe-delimited file named `AgentADSLookup.txt` to `gs://[YOUR_GCS_BUCKET]/exports/`.
* The output file contains unique records (one per `AgentId`) representing the latest state based on `LastModifiedTimestamp`.
* A run state record is successfully appended to the `metadata_run_state` table in BigQuery with `status = 'COMPLETED'`.

---

## 7. Rollback Procedure

In the event of an execution failure or data corruption during deployment, follow these steps to revert the environment to its previous state.

### 7.1 Step 1: Pause the Airflow DAG
Disable the migrated workflow in Cloud Composer to prevent further automated runs:
```bash
gcloud composer environments run [COMPOSER_ENV_NAME] \
    --location [REGION] \
    dags pause -- dw_dwh_abpz_kkm_ail_agent
```

### 7.2 Step 2: Clean Up Target Assets
Delete any corrupted lookup files generated in GCS:
```bash
gsutil rm gs://[YOUR_GCS_BUCKET]/exports/AgentADSLookup.txt
```

### 7.3 Step 3: Revert Metadata State
If necessary, remove the run log entry from the BigQuery tracking table:
```sql
DELETE FROM `your_project_id.dwh_kkm.metadata_run_state`
WHERE pipeline_name = 'r_alis_objekt'
  AND business_date = CURRENT_DATE();
```