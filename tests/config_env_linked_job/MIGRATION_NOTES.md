# Migration Notes: DW.CFG_LOAD_PARAMS

This document details the migration of the legacy UC4 job `DW.CFG_LOAD_PARAMS` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow), Google Cloud Storage (GCS), BigQuery, and Dataform.

---

## 1. Summary

The legacy `DW.CFG_LOAD_PARAMS` job was a standalone Unix job (`JOBS_UNIX`) in UC4 responsible for loading Data Warehouse (DWH) parameter files into an Oracle staging environment. 

### Legacy Architecture
* **Orchestration**: UC4 Job wrapper setting environment variables (e.g., `&DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'`).
* **Execution**: A Korn shell script (`r_load_params.ksh`) that:
  1. Parsed database connection details from a local properties file (`dwh_env.properties`).
  2. Executed Oracle SQL\*Loader (`sqlldr`) to load the properties file into the staging table `DWH_STG.PARAM_LOAD`.
  3. Executed SQL\*Plus (`sqlplus`) to run `d_param_load.sql`, merging the staged parameters into the master parameter table `DWH_ADM.JOB_PARAMS`.

### Target Architecture
* **Orchestration**: Apache Airflow DAG (`dw_cfg_load_params`) running on Cloud Composer.
* **Ingestion**: A Python script (`r_load_params.py`) replacing the shell script and SQL\*Loader. It reads the properties file from GCS (with a local fallback) and loads it directly into BigQuery staging (`DWH_STG.PARAM_LOAD`) using the native BigQuery client library.
* **Transformation**: A Dataform SQLX operations file (`d_param_load.sqlx`) replacing SQL\*Plus. It executes a native BigQuery `MERGE` statement to upsert parameters into `DWH_ADM.JOB_PARAMS`.

---

## 2. Generated Artifacts

The migration process generated three core artifacts, each mapping to a specific layer of the target architecture:

| Artifact Path | Language / Type | Role |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py` | Python (Airflow DAG) | Orchestrates the end-to-end workflow: triggers the Python loader script, compiles the Dataform repository, and invokes the Dataform merge execution. |
| `config_env_linked_job/iscfg/bin/r_load_params.py` | Python 3 | Replaces `r_load_params.ksh` and `sqlldr`. Parses `dwh_env.properties` and loads the key-value pairs into the BigQuery staging table. |
| `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` | SQLX (Dataform) | Replaces `d_param_load.sql` and `sqlplus`. Executes the BigQuery `MERGE` statement to perform an SCD Type 1 upsert from staging to target. |

---

## 3. Key Design Decisions

### Retirement of SQL\*Loader (`sqlldr`)
* **Decision**: Replaced `sqlldr` with a native Python script using the `google-cloud-bigquery` client library (`load_table_from_json` with `WRITE_TRUNCATE`).
* **Rationale**: Avoids the overhead of installing and maintaining Oracle client binaries and SQL\*Loader utilities on the Cloud Composer worker nodes. Python-native loading is highly reliable, handles schema validation, and integrates seamlessly with IAM.

### Retirement of SQL\*Plus (`sqlplus`)
* **Decision**: Replaced the SQL\*Plus execution of `d_param_load.sql` with a Dataform workflow invocation (`DataformCreateWorkflowInvocationOperator`).
* **Rationale**: Aligns with modern cloud data warehousing practices. Dataform provides built-in dependency management, SQL compilation validation, execution logging, and environment isolation.

### Properties File Storage and Fallback
* **Decision**: The Python loader script is designed to look for `dwh_env.properties` in GCS first (`gs://{GCS_BUCKET}/...`), falling back to the local file system if GCS is unavailable or during local development.
* **Rationale**: Ensures cloud-native execution while preserving local testing capabilities for developers.

### Concurrency and Race Condition Prevention
* **Decision**: Set `max_active_runs=1` on the Airflow DAG.
* **Rationale**: Because parameter loading overwrites staging tables (`WRITE_TRUNCATE`), concurrent runs of this DAG would cause race conditions and data corruption.

---

## 4. Manual Steps Before Go-Live

To deploy and run this workflow successfully, the following manual setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure the following BigQuery datasets and tables exist in your target GCP project:
* **Dataset**: `DWH_STG`
  * **Table**: `PARAM_LOAD`
    * `param_key` (STRING, REQUIRED)
    * `param_value` (STRING, NULLABLE)
    * `loaded_at` (TIMESTAMP, REQUIRED)
* **Dataset**: `DWH_ADM`
  * **Table**: `JOB_PARAMS`
    * `param_key` (STRING, REQUIRED)
    * `param_value` (STRING, NULLABLE)
    * `updated_at` (TIMESTAMP, REQUIRED)

### 2. IAM and Permissions
* **Composer Worker Service Account**: Must have the following roles:
  * `roles/bigquery.dataEditor` on `DWH_STG` and `DWH_ADM` datasets.
  * `roles/bigquery.jobUser` on the project.
  * `roles/storage.objectViewer` on the GCS bucket containing the properties file.
  * `roles/dataform.editor` to compile and trigger Dataform workflows.

### 3. Airflow Variables
Configure the following Airflow Variables in the Cloud Composer environment:
* `GCP_PROJECT`: Your target Google Cloud Project ID.
* `GCS_BUCKET`: The GCS bucket name where configuration files are stored.
* `BQ_DATASET`: The target BigQuery dataset (e.g., `DWH_ADM`).
* `GCP_REGION`: The GCP region for Dataform execution (e.g., `us-central1`).

### 4. Configuration File Deployment
Upload the legacy `dwh_env.properties` file to GCS:
* **Target Path**: `gs://<GCS_BUCKET>/config_env_linked_job/iscfg/cfg/dwh_env.properties`

### 5. Dataform Repository Setup
* Ensure a Dataform repository named `dwh_dataform_repository` is created in your GCP project and region.
* Commit and push `d_param_load.sqlx` to the `main` branch of this repository.

### 6. Scheduling
* The DAG is currently configured with `schedule=None` (externally triggered), matching the legacy UC4 configuration. If this job needs to run on a time-based schedule, update the `schedule` parameter in `dw_cfg_load_params.py`.

---

## 5. Known Gaps & Unresolved References

### Legacy Includes Retired
The legacy UC4 includes were retired during migration:
* `. $HOME/.dw_init` and `:inc DW.HOLE_PFAD`: Environment initialization is now handled natively by Airflow environment variables and GCS pathing.
* `:inc DW.BERT_LESE_LOG`: Log parsing is retired; Cloud Composer natively captures and centralizes all task logs in Cloud Logging.

### Missing `param_load.ctl`
* **Gap**: The legacy SQL\*Loader control file (`param_load.ctl`) was not provided in the source bundle.
* **Impact**: The Python parser assumes a standard `key=value` structure in `dwh_env.properties`. If the properties file contains complex formatting, multi-line values, or custom delimiters that were handled by the control file, the Python parser may fail or load incorrect data.
* **Action Required**: Verify the structure of `dwh_env.properties` against the parsing logic in `r_load_params.py` before production deployment.

### Hardcoded Datasets in SQLX
* **Gap**: The Dataform file `d_param_load.sqlx` hardcodes the datasets `DWH_ADM` and `DWH_STG`.
* **Impact**: This limits environment portability (e.g., running Dev, Test, and Prod in the same GCP project with prefixed datasets).
* **Action Required**: If multi-environment isolation is required within a single project, refactor the SQLX file to use Dataform's native `${ref()}` or declare them as sources/declarations.

---

## 6. Validation

To validate the migrated workflow, perform the following steps:

### Step 1: Local Python Validation
Run the Python loader script locally using mock environment variables to verify parsing and BigQuery ingestion:
```bash
export GCP_PROJECT="your-gcp-project"
export GCS_BUCKET="your-gcs-bucket"
export BQ_DATASET_STG="DWH_STG"
export DWH_HOME="./local_test_dir" # Ensure local_test_dir/cfg/dwh_env.properties exists

python3 config_env_linked_job/iscfg/bin/r_load_params.py
```
* **Passing Criteria**: The script exits with code `0`, and the `DWH_STG.PARAM_LOAD` table in BigQuery is populated with the properties.

### Step 2: End-to-End DAG Validation
1. Trigger the `dw_cfg_load_params` DAG manually from the Airflow UI.
2. Monitor the execution of the three tasks:
   * `run_load_params`
   * `create_compilation`
   * `invoke_dataform`

### Step 3: Data Verification
Query the target BigQuery table to verify the merge operation succeeded:
```sql
SELECT * FROM `DWH_ADM.JOB_PARAMS` WHERE param_key = 'db.host';
```
* **Passing Criteria**: 
  1. All tasks in the Airflow DAG show a status of `Success`.
  2. The `updated_at` timestamp in `DWH_ADM.JOB_PARAMS` matches the execution time of the DAG.
  3. No duplicate keys exist in `DWH_ADM.JOB_PARAMS`.

---

## 7. Rollback Procedure

If the migration fails in production, execute the following rollback steps:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the switch for `dw_cfg_load_params` to **Off** to prevent any scheduled or accidental manual triggers.
2. **Re-enable Legacy UC4 Job**:
   Set the active flag back to `1` on the legacy `DW.CFG_LOAD_PARAMS` job in the UC4 console.
3. **Revert Database State (Optional)**:
   If the failed migration corrupted the target parameter table, restore the table to its pre-migration state using BigQuery's time travel feature:
   ```sql
   CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` AS
   SELECT * FROM `DWH_ADM.JOB_PARAMS`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```