# Migration Notes: DW.CFG_LOAD_PARAMS

This document details the migration of the legacy parameter loading job `DW.CFG_LOAD_PARAMS` from its on-premises UC4/Oracle environment to Google Cloud Platform (GCP) using Cloud Composer (Airflow), BigQuery, and Dataform.

---

## 1. Summary

The `DW.CFG_LOAD_PARAMS` workflow has been migrated from a legacy UC4-orchestrated KornShell (`.ksh`) and Oracle environment to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

### Legacy vs. Target Architecture
* **Orchestration:** Migrated from **UC4 Automic** (`DW.CFG_LOAD_PARAMS.xml`) to **Cloud Composer (Apache Airflow)**.
* **Ingestion & Staging:** Migrated from **Oracle SQL\*Loader** (`sqlldr` via `r_load_params.ksh`) to a **Python 3 script** using the native Google Cloud SDKs to read from **Google Cloud Storage (GCS)** and load into **BigQuery**.
* **Data Transformation:** Migrated from **Oracle SQL\*Plus** (`sqlplus` executing `d_param_load.sql`) to **BigQuery SQL** (orchestrated either via native Airflow operators or **Dataform SQLX**).
* **Retired Components:** The helper scripts `.DW_INIT` (environment initialization), `DW.BERT_LESE_LOG` (execution logging), and `DW.HOLE_PFAD` (path resolution) have been **retired**. Their functionalities are handled natively by Cloud Composer's environment variables, task logging, and GCS path resolution.

---

## 2. Generated Artifacts

The migration process generated the following files, preserving the original directory structure to maintain folder integrity:

| Generated File Path | Language | Role |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py` | Python (Airflow) | **Orchestrator DAG (Dataform Option):** Compiles and runs the Dataform model after executing the staging script. |
| `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py` | Python (Airflow) | **Orchestrator DAG (Direct BigQuery Option):** Executes the staging script and runs the MERGE query directly via the `BigQueryInsertJobOperator`. |
| `config_env_linked_job/iscfg/bin/r_load_params.py` | Python 3 | **Parameter Ingestion Script:** Replaces `r_load_params.ksh`. Reads `dwh_env.properties` from GCS, parses key-value pairs, and loads them into `DWH_STG.PARAM_LOAD`. |
| `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` | SQLX (Dataform) | **Dataform Model:** Performs an incremental SCD Type 1 `MERGE` from the staging table to the target table `DWH_ADM.JOB_PARAMS`. |
| `config_env_linked_job/iscfg/cfg/d_param_load.sql` | BigQuery SQL | **Standard SQL Script:** Parameterized BigQuery SQL script used by the direct Airflow DAG option to perform the `MERGE` operation. |

---

## 3. Key Design Decisions

### Native Python over Bash/KornShell
Instead of running a wrapper shell script inside an Airflow `BashOperator` to call database clients, the KornShell script (`r_load_params.ksh`) was refactored into a native Python script (`r_load_params.py`). This allows direct integration with the Google Cloud Client Libraries (`google-cloud-storage` and `google-cloud-bigquery`), improving error handling, logging, and execution security.

### SQL*Loader Replacement
Oracle SQL\*Loader (`sqlldr`) and its control file (`param_load.ctl`) were retired. The Python ingestion script parses the `dwh_env.properties` file line-by-line, converts the properties into JSON records, and performs a native BigQuery load job using the `WRITE_TRUNCATE` write disposition. This replicates the legacy "fresh write" staging behavior.

### Dual Orchestration Options
To accommodate different enterprise deployment standards, two DAG options were generated:
1. **Dataform-based DAG (`dw_cfg_load_params.py`):** Best suited for environments where SQL transformations are managed declaratively within a Dataform repository.
2. **Direct BigQuery DAG (`dw_cfg_load_params_dag.py`):** Best suited for lightweight deployments where transformations are executed directly via Airflow BigQuery operators.

### Verbatim Preservation of German Log Messages
To ensure compatibility with legacy log-monitoring tools and operational runbooks, all log and error messages in the Python script have been preserved character-for-character in German (e.g., `"FEHLER: Parameterdatei existiert nicht oder ist leer."`).

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated workflow, the following manual setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure the target BigQuery datasets and tables exist in your GCP project:
* **Staging Table:** `DWH_STG.PARAM_LOAD`
  ```sql
  CREATE OR REPLACE TABLE `DWH_STG.PARAM_LOAD` (
    param_key STRING,
    param_value STRING,
    loaded_at TIMESTAMP
  );
  ```
* **Target Table:** `DWH_ADM.JOB_PARAMS`
  ```sql
  CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` (
    param_key STRING,
    param_value STRING,
    updated_at TIMESTAMP
  );
  ```

### 2. IAM & Permissions
The service account running the Cloud Composer workers must have the following IAM roles:
* `roles/bigquery.admin` (or specific read/write permissions on the `DWH_STG` and `DWH_ADM` datasets).
* `roles/storage.objectViewer` on the GCS bucket containing the configuration properties.
* `roles/dataform.editor` (if using the Dataform orchestration DAG).

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `GCP_REGION`: The target GCP region (e.g., `europe-west3`).
* `GCS_CONFIG_BUCKET` / `GCS_BUCKET`: The GCS bucket name where configuration files are stored.
* `DATAFORM_REPOSITORY`: The name of the Dataform repository (required for the Dataform DAG).

### 4. Properties File Placement
Upload the legacy configuration properties file to GCS:
* **Destination Path:** `gs://<GCS_CONFIG_BUCKET>/cfg/dwh_env.properties`

---

## 5. Known Gaps & Unresolved References

### Missing Source SQL (`d_param_load.sql`)
The original Oracle SQL merge script `d_param_load.sql` was not provided in the source codebase. The BigQuery SQL and Dataform SQLX models were reverse-engineered based on the described behavior of performing an SCD Type 1 `MERGE` from `DWH_STG.PARAM_LOAD` to `DWH_ADM.JOB_PARAMS`. 
* **Action Required:** A database administrator or developer must verify that the target table schema and merge keys match the actual production requirements.

### Properties File Parsing Limits
The Python parser in `r_load_params.py` assumes a standard `key=value` format. If the production `dwh_env.properties` file contains complex structures (such as multi-line values or escaped characters), the parsing logic may need to be updated to use a robust parser like `configparser` or `jproperties`.

---

## 6. Validation

To validate the migrated workflow, follow these steps:

### 1. Local/Development Dry Run
Run the Python ingestion script locally by setting the required environment variables:
```bash
export GCP_PROJECT="your-dev-project"
export GCS_CONFIG_BUCKET="your-dev-bucket"
python3 config_env_linked_job/iscfg/bin/r_load_params.py
```
*Verify that the script successfully reads the properties file from GCS and loads it into `DWH_STG.PARAM_LOAD`.*

### 2. DAG Execution Test
1. Upload the chosen DAG file and the Python script to the Composer DAGs bucket.
2. Trigger the DAG manually from the Airflow UI.
3. Verify that both tasks (`parse_and_stage_parameters` and `merge_parameters`) complete with a `success` status.

### 3. Definition of "Passing"
The migration is considered successful when:
* The staging table `DWH_STG.PARAM_LOAD` is truncated and populated with the exact key-value pairs from `dwh_env.properties`.
* The target table `DWH_ADM.JOB_PARAMS` is updated with new or modified keys.
* The `updated_at` timestamp in the target table matches the execution time of the run.
* The Airflow task logs display the expected German log messages without errors.

---

## 7. Rollback Procedure

In the event of a failure or unexpected behavior post-deployment, execute the following rollback steps:

### 1. Database Rollback
Because the `MERGE` operation performs an in-place overwrite (SCD Type 1), you must restore the target table to its pre-execution state. Use BigQuery's **Time Travel** feature to restore the table:
```sql
CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` AS
SELECT * FROM `DWH_ADM.JOB_PARAMS`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```
*(Adjust the interval to target a timestamp immediately prior to the failed DAG run).*

### 2. Orchestration Rollback
1. Pause the migrated Airflow DAG in the Airflow UI.
2. If necessary, remove the DAG file from the Composer `dags/` bucket.
3. Re-enable the legacy UC4 job scheduler to resume on-premises execution.