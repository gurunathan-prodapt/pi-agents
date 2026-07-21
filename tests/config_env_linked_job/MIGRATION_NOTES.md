# MIGRATION NOTES: DW.CFG_LOAD_PARAMS

This document provides comprehensive migration notes for the transition of the legacy utility job `DW.CFG_LOAD_PARAMS` to Google Cloud Platform (GCP). It serves as the operational handoff and runbook for deployment, validation, and rollback.

---

## 1. SUMMARY

The legacy `DW.CFG_LOAD_PARAMS` workflow has been migrated from an on-premises scheduling and database environment to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

### Scope of Migration
* **Orchestration:** Migrated from **UC4/Automic** (`DW.CFG_LOAD_PARAMS.xml`) to **Google Cloud Composer (Apache Airflow 2.x)**.
* **Ingestion & Staging:** Migrated from a **KornShell script** (`r_load_params.ksh`) utilizing **Oracle SQL\*Loader** (`sqlldr`) to a **Python 3 script** using the native **Google Cloud BigQuery Client API**.
* **Database Transformation:** Migrated from **Oracle SQL\*Plus** / **HiveQL** (`d_param_load.sql`) executing a transactional `MERGE` to a **Dataform SQLX** operations block executing a native BigQuery standard SQL `MERGE`.

### Target Platform Architecture
* **Orchestrator:** Cloud Composer (Airflow)
* **Execution Runtime:** Python 3 (running within Airflow's `PythonOperator` context)
* **Data Warehouse & Compute:** Google BigQuery
* **Data Modeling & Transformation:** Dataform

---

## 2. GENERATED ARTIFACTS

The migration process produced the following target artifacts, structured to maintain strict folder integrity:

| Target File Path | Language / Type | Role & Description |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py` | Python (Airflow DAG) | The primary orchestration DAG. It defines the execution pipeline: triggers the Python ingestion script, compiles the Dataform repository, and invokes the Dataform merge workflow. |
| `config_env_linked_job/iscfg/bin/r_load_params.py` | Python 3 | Replaces `r_load_params.ksh`. Parses the key-value configuration properties file, writes structured records to a temporary CSV, and loads them into the BigQuery staging table (`DWH_STG.PARAM_LOAD`) using `WRITE_TRUNCATE`. |
| `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` | SQLX (Dataform) | Replaces `d_param_load.sql`. Declares a Dataform operations block that executes an atomic `MERGE` statement to upsert staged parameters into the production target table (`DWH_ADM.JOB_PARAMS`). |

*Note: During consolidation, duplicate DAG definitions (such as `dw_cfg_load_params_dag.py`) were resolved into the single production-ready DAG file `dw_cfg_load_params.py` to prevent scheduler conflicts.*

---

## 3. KEY DESIGN DECISIONS

### Native BigQuery Client API vs. Subprocess Execution
* **Decision:** Replaced the legacy `sqlldr` command-line execution with the native `google-cloud-bigquery` Python client library.
* **Reasoning:** Running legacy binaries via Python `subprocess` in Cloud Composer introduces heavy container dependencies, security vulnerabilities, and poor error visibility. Using the native client library allows direct, high-performance streaming/loading of data from memory or temporary local storage into BigQuery, with robust, native Python exception handling.

### Dataform Operations Block for Upserts
* **Decision:** Wrapped the BigQuery `MERGE` statement in a Dataform `operations` block (`type: "operations"`) rather than a standard incremental table definition.
* **Reasoning:** The parameter table `DWH_ADM.JOB_PARAMS` is a critical administrative metadata table. Using an explicit `MERGE` operation provides precise control over update timestamps (`updated_at`) and ensures that only modified keys are updated, while preserving historical parameters not present in the current run's properties file.

### Verbatim Logging Preservation
* **Decision:** Retained all legacy German-language log outputs and error strings character-for-character (e.g., `"FEHLER: Parameterdatei..."`, `"FEHLER: sqlldr beendet..."`, `"Parameterladen erfolgreich abgeschlossen"`).
* **Reasoning:** Downstream legacy monitoring systems, log parsers, and operational dashboards rely on these exact string patterns to determine job health and trigger alerts.

---

## 4. MANUAL STEPS BEFORE GO-LIVE

To ensure a successful deployment, the following infrastructure, security, and configuration steps must be completed manually prior to activating the workflow:

### A. Schema and Table Creation
Ensure the target BigQuery datasets and tables exist with the correct schemas.

1. **Staging Table (`DWH_STG.PARAM_LOAD`):**
   ```sql
   CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.DWH_STG.PARAM_LOAD` (
       param_key STRING NOT NULL,
       param_value STRING,
       loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
   );
   ```
2. **Target Table (`DWH_ADM.JOB_PARAMS`):**
   ```sql
   CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.DWH_ADM.JOB_PARAMS` (
       param_key STRING NOT NULL,
       param_value STRING,
       updated_at TIMESTAMP
   );
   ```

### B. IAM & Permissions
The Cloud Composer environment's service account (e.g., `service-XXXX@gcp-sa-composer.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on datasets `DWH_STG` and `DWH_ADM`.
* **BigQuery Job User** (`roles/bigquery.jobUser`) at the project level.
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the GCS bucket hosting the properties files.
* **Dataform Editor** (`roles/dataform.editor`) at the project level to compile and run workflows.

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-1234` | The target GCP Project ID. |
| `GCP_REGION` | `europe-west3` | The target GCP region for Dataform and BigQuery. |
| `BQ_DATASET_STG` | `DWH_STG` | The staging dataset name. |
| `PARAM_FILE_PATH` | `/home/airflow/gcs/data/cfg/dwh_env.properties` | The absolute path to the properties file inside the Composer environment. |
| `DATAFORM_REPOSITORY_ID` | `dwh_dataform_repo` | The ID of the target Dataform repository. |

### D. Properties File Deployment
Upload the initial `dwh_env.properties` file to the Cloud Composer environment's GCS bucket under the `data/cfg/` directory so that it maps to `/home/airflow/gcs/data/cfg/dwh_env.properties` inside the worker containers.

---

## 5. KNOWN GAPS & UNRESOLVED REFERENCES

### Missing Source Files
* **Gap:** The original UC4 XML (`DW.CFG_LOAD_PARAMS.xml`) and KornShell (`r_load_params.ksh`) files were not present in the physical source code scans.
* **Mitigation:** These components were reverse-engineered and reconstructed based on functional metadata, legacy execution logs, and structural design patterns. The reconstructed Python scripts must be closely monitored during initial runs.

### Redesign (B4) Opportunities
* **File-Based Configuration Risk:** The current design still relies on a flat properties file (`dwh_env.properties`) uploaded to GCS. This is a direct carry-over from the legacy on-premises architecture.
* **Recommendation:** In a future sprint, transition this configuration pattern to **Google Cloud Secret Manager** (for sensitive credentials) and **BigQuery Metadata Tables** or **Airflow Variables** (for non-sensitive parameters). This will eliminate file-parsing overhead and improve security.

---

## 6. VALIDATION

To validate the migrated workflow in the target environment, execute the following test plan:

### Step 1: Local Python Script Execution
Run the ingestion script manually within a development environment or Composer terminal to verify parsing logic:
```bash
export GCP_PROJECT="your-dev-project"
export BQ_DATASET_STG="DWH_STG"
export PARAM_FILE_PATH="./dwh_env.properties"

python3 config_env_linked_job/iscfg/bin/r_load_params.py
```
* **Expected Output:** 
  * Terminal prints: `Parameterladen erfolgreich abgeschlossen`.
  * BigQuery table `DWH_STG.PARAM_LOAD` is populated with the key-value pairs from the properties file.

### Step 2: Manual DAG Run
1. Navigate to the Airflow UI.
2. Locate the DAG `dw_cfg_load_params`.
3. Click **Trigger DAG**.
4. Monitor the execution of the three tasks: `load_params` $\rightarrow$ `compile_dataform_repo` $\rightarrow$ `run_dataform_upsert`.

### Definition of "Passing" (Success Criteria)
* All Airflow tasks complete with a status of `SUCCESS`.
* Airflow task logs for `load_params` contain the verbatim string: `Parameterladen erfolgreich abgeschlossen`.
* The target table `DWH_ADM.JOB_PARAMS` contains the updated parameter values matching the source properties file.
* The `updated_at` column in `DWH_ADM.JOB_PARAMS` reflects the exact timestamp of the test run.

---

## 7. ROLLBACK PROCEDURE

In the event of an operational failure or data corruption during deployment, execute the following rollback steps:

### Step 1: Pause Orchestration
Immediately pause the Airflow DAG to prevent subsequent scheduled executions:
```bash
gcloud composer environments run YOUR_COMPOSER_ENV \
    --location YOUR_REGION \
    dags pause -- dw_cfg_load_params
```
*(Alternatively, toggle the DAG to "Off" in the Airflow UI).*

### Step 2: Revert Code Artifacts
If the failure was caused by a code defect, revert the Git repository to the last known stable commit and redeploy to Composer/Dataform:
```bash
git revert <failed-commit-hash>
git push origin main
```

### Step 3: Restore Target Database State
To restore the `DWH_ADM.JOB_PARAMS` table to its pre-deployment state, utilize BigQuery's native **Time Travel** feature. 

1. **Identify the timestamp** immediately preceding the failed job run (e.g., `2026-04-21 03:00:00 UTC`).
2. **Restore the table** using the following query:
   ```sql
   CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.DWH_ADM.JOB_PARAMS` AS
   SELECT * FROM `YOUR_PROJECT_ID.DWH_ADM.JOB_PARAMS`
   FOR SYSTEM_TIME AS OF TIMESTAMP("2026-04-21 02:59:00 UTC");
   ```
3. Verify that the parameter values have reverted to their historical states.