# MIGRATION_NOTES.md: DW.CFG_LOAD_PARAMS

This document provides comprehensive migration notes and operational guidelines for transitioning the legacy `DW.CFG_LOAD_PARAMS` workflow to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy `DW.CFG_LOAD_PARAMS` workflow has been migrated from an on-premises environment orchestrated by **UC4 (Automic)** and driven by **KornShell (KSH)**, **Oracle SQL\*Loader**, and **SQL\*Plus** to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**.

### Migration Mapping
* **Orchestration:** UC4 Scheduler XML (`DW.CFG_LOAD_PARAMS.xml`) $\rightarrow$ **Cloud Composer (Apache Airflow 2.x)**
* **Ingestion & Processing:** KornShell Script (`r_load_params.ksh`) & SQL\*Loader (`sqlldr`) $\rightarrow$ **Python 3 Operator** utilizing native Google Cloud Storage (GCS) and BigQuery Client Libraries.
* **Database Engine:** Oracle Database (`DWH_STG` and `DWH_ADM` schemas) $\rightarrow$ **Google Cloud BigQuery** (`DWH_STG` and `DWH_ADM` datasets).
* **Data Transformation & Merge:** SQL\*Plus Script (`d_param_load.sql`) $\rightarrow$ **Google Cloud Dataform** (`d_param_load.sqlx`).

---

## 2. Generated Artifacts

The migration process generated three core artifacts, each serving a distinct role in the target architecture:

### 1. Python Ingestion Script
* **Path:** `config_env_linked_job/iscfg/bin/r_load_params.py`
* **Role:** Replaces the legacy KornShell script and SQL\*Loader utility. It reads the raw configuration properties file from GCS, parses the key-value pairs, cleanses the staging table, and streams the parsed records into the BigQuery staging table (`DWH_STG.PARAM_LOAD`) using a `WRITE_TRUNCATE` disposition.

### 2. Dataform SQLX Operations Model
* **Path:** `dataform/definitions/config_env_linked_job/iscfg/cfg/d_param_load.sqlx`
* **Role:** Replaces the legacy Oracle SQL\*Plus merge script (`d_param_load.sql`). It defines a Dataform operations block that executes an atomic BigQuery `MERGE` statement to upsert staged parameters from `DWH_STG.PARAM_LOAD` into the production parameter registry `DWH_ADM.JOB_PARAMS`.

### 3. Consolidated Airflow DAG
* **Path:** `dags/config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py`
* **Role:** Replaces the UC4 XML job definition. It orchestrates the end-to-end execution flow:
  1. Triggers the Python ingestion script to stage parameters.
  2. Compiles the Dataform repository.
  3. Invokes the Dataform workflow execution for the `cfg_load_params` tag to merge the parameters.

---

## 3. Key Design Decisions

### Native Python Ingestion over Subprocess Emulation
* **Decision:** Replaced Oracle SQL\*Loader (`sqlldr`) with a native Python script using the official Google Cloud SDKs (`google-cloud-storage` and `google-cloud-bigquery`).
* **Reasoning:** Emulating SQL\*Loader via subprocesses would require maintaining legacy Oracle Client binaries and configuration files (like `.ctl` and `tnsnames.ora`) inside the Cloud Composer worker environment. Native Python ingestion simplifies the container footprint, improves error handling, and integrates with GCP IAM.

### Dataform for ELT Orchestration
* **Decision:** Migrated the Oracle `MERGE` SQL script to a Dataform SQLX operations block rather than executing raw SQL strings directly inside an Airflow operator.
* **Reasoning:** Dataform provides compilation-time validation, dependency tracking, and environment isolation. This ensures that schema changes or syntax errors are caught before execution, aligning with modern analytics engineering practices.

### Truncate-and-Load Staging Strategy
* **Decision:** Maintained the legacy behavior of truncating the staging table (`PARAM_LOAD`) before loading new parameters.
* **Reasoning:** This prevents stale or deprecated configuration parameters from persisting in the staging layer and corrupting downstream runs.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed in the target GCP environment:

### A. BigQuery Dataset & Table Creation
If they do not already exist, create the target datasets and tables in BigQuery:

```sql
-- Create Datasets
CREATE SCHEMA IF NOT EXISTS `DWH_STG`;
CREATE SCHEMA IF NOT EXISTS `DWH_ADM`;

-- Create Staging Table
CREATE TABLE IF NOT EXISTS `DWH_STG.PARAM_LOAD` (
    param_key STRING NOT NULL,
    param_value STRING,
    loaded_at TIMESTAMP NOT NULL
);

-- Create Target Table
CREATE TABLE IF NOT EXISTS `DWH_ADM.JOB_PARAMS` (
    param_key STRING NOT NULL,
    param_value STRING,
    updated_at TIMESTAMP NOT NULL
);
```

### B. IAM & Permissions
Ensure that the Cloud Composer Service Account (typically `service-XXX@gcp-sa-composer.iam.gserviceaccount.com` or a custom user-managed service account) has the following IAM roles:
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the GCS bucket containing the configuration files.
* **BigQuery Admin** (`roles/bigquery.admin`) or a combination of **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery Job User** (`roles/bigquery.jobUser`) on the `DWH_STG` and `DWH_ADM` datasets.
* **Dataform Editor** (`roles/dataform.editor`) to compile and execute Dataform workflows.

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment (via the Airflow UI under **Admin -> Variables** or the `gcloud` CLI):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | The target Google Cloud Project ID. |
| `GCP_REGION` | `europe-west3` | The GCP region where Composer and Dataform reside. |
| `GCS_BUCKET` | `my-dwh-config-bucket` | The GCS bucket where configuration properties are uploaded. |
| `DATAFORM_REPOSITORY_ID` | `dwh-dataform-repo` | The ID of the Dataform repository. |

### D. Initial File Placement
Upload the initial configuration properties file to the designated GCS path:
* **Target Path:** `gs://{GCS_BUCKET}/config/d_param_load.properties`
* **Format:** Standard key-value properties format (e.g., `db.host=10.0.0.1`).

---

## 5. Known Gaps & Unresolved References

### Retired Legacy Components (No-Source Components)
The following legacy UC4 includes were analyzed and determined to be **not needed** in the target GCP environment:
* `DW.HOLE_PFAD`: Used for legacy UNIX path resolution. This is replaced by native Python `os.path` and GCS URI structures.
* `DW.BERT_LESE_LOG`: Used for legacy log parsing and error alerting. This is replaced by native **Cloud Logging** and Airflow's built-in task failure callbacks/alerts.

### Unresolved References & Follow-ups
* **Dynamic File Naming:** The current migration assumes a static configuration file name (`config/d_param_load.properties`). If the upstream system delivers files with dynamic timestamps (e.g., `d_param_load_20260421.properties`), the Python script will need to be updated with a GCS prefix search/wildcard matching mechanism.
* **Dataform Repository Linkage:** The Airflow DAG assumes that the Dataform repository is already initialized, connected to a Git provider, and contains the `d_param_load.sqlx` file on the `main` branch.

---

## 6. Validation

To validate the migrated workflow, perform the following steps:

### Execution Test
1. Navigate to the Cloud Composer Airflow UI.
2. Locate the DAG `dw_cfg_load_params_dag` and trigger it manually.
3. Monitor the execution of the three tasks:
   * `load_staging_params`
   * `create_compilation`
   * `execute_dataform_merge`

### Verification of Success
The migration is considered successful ("passing") when:
1. All three Airflow tasks complete with a `success` status.
2. Airflow task logs for `load_staging_params` show the correct number of parsed parameters matching the source file.
3. The BigQuery table `DWH_STG.PARAM_LOAD` contains the newly loaded records with the current timestamp.
4. The BigQuery table `DWH_ADM.JOB_PARAMS` successfully reflects the upserted values, verifying that the Dataform `MERGE` statement executed without syntax or permission errors.

---

## 7. Rollback Procedure

If critical failures occur post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:**
   Disable the DAG in the Airflow UI or via CLI to prevent further automated executions:
   ```bash
   gcloud composer environments run <env-name> \
       --location <region> \
       dags disable -- dw_cfg_load_params_dag
   ```
2. **Restore Target Table State (Optional):**
   If the target table `DWH_ADM.JOB_PARAMS` was corrupted by a bad run, restore it to its pre-execution state using BigQuery time-travel:
   ```sql
   -- Restore table to state 1 hour ago
   CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` AS
   SELECT * FROM `DWH_ADM.JOB_PARAMS`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Re-enable Legacy Execution:**
   Re-activate the legacy UC4 job `DW.CFG_LOAD_PARAMS` in the Automic/UC4 scheduler to resume on-premises processing.