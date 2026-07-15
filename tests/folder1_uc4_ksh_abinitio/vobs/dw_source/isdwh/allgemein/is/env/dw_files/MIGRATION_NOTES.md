# MIGRATION_NOTES.md

**Job:** Shared Files — `folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Target Platform:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow), Google Cloud Secret Manager, Google Cloud Storage (GCS), Dataproc Serverless (PySpark), and BigQuery.

---

## 1. Summary

This migration addresses the legacy environment-level configuration scripts (`.dw_global`, `.dw_ai`, `.dw_db`, and `.dw_init`) that historically provided the operational backbone for UC4, KornShell (KSH), and Ab Initio pipelines interacting with an Oracle-based Data Warehouse. 

Because the original legacy configuration files were physically missing from disk, a clean-room reconstruction was performed. The brittle, OS-level shell-sourcing routines have been refactored into a modernized, unified, cloud-native environment engine on GCP. 

* **Source Pattern:** UC4 + KornShell (KSH) + Ab Initio (Oracle-based DWH Framework)
* **Target Architecture:** Google Cloud Composer (Apache Airflow Orchestration) + Dataproc Serverless (PySpark Execution Engine) + BigQuery (Data Warehousing)
* **Migration Strategy:** Refactored into a centralized Python utility module (`gcp_environment_loader.py`) integrated with Google Cloud Secret Manager, GCS, and Airflow variables.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy configuration scripts:

### 1. `gcp_environment_loader.py`
* **Target Path:** `dags/plugins/gcp_environment_loader.py`
* **Language:** Python (Airflow / GCP SDK)
* **Role:** Core utility class that replaces the functional capabilities of `.dw_global`, `.dw_ai`, `.dw_db`, and `.dw_init`. It handles dynamic GCS path resolution, secure database credential retrieval from Secret Manager, Dataproc scaling profile configuration, and execution lock management.

### 2. `create_metadata_tables.sql`
* **Target Path:** `gcs/scripts/sql/create_metadata_tables.sql`
* **Language:** BigQuery SQL DDL
* **Role:** Instantiates the control and metadata tables (`env_variable_mapping`, `pipeline_execution_audit`, and `database_schema_mapping`) in BigQuery. These tables track environment configurations, run states, and database target schemas, replacing legacy flat-file tracking.

### 3. `gcp_environment_setup_dag.py`
* **Target Path:** `dags/gcp_environment_setup_dag.py`
* **Language:** Python (Airflow DAG)
* **Role:** A verification and orchestration DAG that demonstrates how to consume the `GCPEnvironmentLoader` plugin. It asserts global variable status, verifies secret access, and executes step-by-step initialization and finalization locks.

---

## 3. Key Design Decisions

### Centralized Python Class vs. Shell Sourcing
* **Decision:** Replace OS-level shell sourcing (`source .dw_global`) with an object-oriented Python utility (`GCPEnvironmentLoader`).
* **Reasoning:** Shell scripts are brittle, difficult to test, and lack native integrations with cloud-managed services. A Python class allows seamless integration with Airflow, Google Cloud SDKs, and structured logging.

### Secret Manager for Database Profiles
* **Decision:** Store database connection profiles in Google Cloud Secret Manager as JSON payloads instead of plain-text files (`.dw_db`).
* **Reasoning:** This aligns with enterprise security standards, ensuring credentials are encrypted at rest and in transit, and accessed only via IAM-authorized service accounts.

### Serverless Spark Scaling vs. Fixed Parallelism (MFS)
* **Decision:** Map Ab Initio Multifile System (MFS) parallel layouts (`MFS_DEPTH`) to dynamic Dataproc Serverless scaling profiles.
* **Reasoning:** Physical disk partitioning is obsolete in a cloud-native architecture. Using GCS folder partitioning combined with dynamic Spark executor allocation provides superior performance and cost optimization.

### Airflow Variables for Process Locks
* **Decision:** Emulate `.dw_init` file-based execution locks using Airflow Variables (`lock_[job_name]_[env]`).
* **Reasoning:** Prevents concurrency conflicts across distributed workers without relying on local Unix filesystems or shared NFS mounts.

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Metadata Table Creation
Execute the generated DDL script in BigQuery to establish the control metadata schema:
```bash
bq query --use_legacy_sql=false < gcs/scripts/sql/create_metadata_tables.sql
```

### 2. IAM & Permissions
Ensure the Cloud Composer worker service account has been granted the following roles:
* **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`)
* **Storage Object Admin** (`roles/storage.objectAdmin`) on environment-specific buckets
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery Job User** (`roles/bigquery.jobUser`)

### 3. Secret Manager Configuration
Create a secret in Google Cloud Secret Manager for each target schema following this naming convention: `db-conn-[schema_name]-[env]`.
* **Example Secret ID:** `db-conn-dw_core-dev`
* **Payload Format (JSON):**
  ```json
  {
    "host": "10.x.x.x",
    "port": 1521,
    "database_name": "ORADWH",
    "username": "migrated_user",
    "password": "secure_password"
  }
  ```

### 4. Airflow Variables Setup
Configure the following Airflow Variables in the Cloud Composer environment:
* `gcp_project_id`: The target GCP project ID (e.g., `gcp-prod-dwh`).
* `environment_phase`: The current environment tier (`dev`, `qa`, or `prod`).
* `mfs_depth_dev` / `mfs_depth_qa` / `mfs_depth_prod`: Parallelism depth (e.g., `8`).

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) / Unresolved References
The following legacy helper scripts and configuration files were referenced in the legacy environment but were physically missing from disk and have no direct cloud candidate:
* **`SETPYA.SH`**: Historically invoked by `.dw_global` to set python/system paths.
* **`.DW_LOKAL`**: Historically read by `.dw_init` for localized machine settings.

### Mitigation Strategy
* The `GCPEnvironmentLoader` class has been designed to bypass these files by resolving all paths and localized settings directly through Airflow Variables and GCS bucket mappings. 
* If custom local behaviors are discovered during downstream job migrations, they must be appended as properties within the `GCPEnvironmentLoader` class or added as environment-scoped Airflow variables.

---

## 6. Validation

### How to Run the Tests
1. Deploy `gcp_environment_loader.py` to the Airflow `plugins/` directory.
2. Deploy `gcp_environment_setup_dag.py` to the Airflow `dags/` directory.
3. Trigger the `gcp_environment_setup_verification` DAG manually from the Airflow UI.

### What "Passing" Means
* **Task `verify_environment_variables` completes with `SUCCESS`**:
  * Confirms successful connection to Secret Manager.
  * Confirms successful resolution of logical paths (`AI_SERIAL`, `AI_TEMP`) to GCS URIs.
  * Confirms successful retrieval of execution parallelism profiles.
  * Confirms that the initialization handshake successfully sets the execution lock variable to `TRUE`.
* **Task `cleanup_environment_lock` completes with `SUCCESS`**:
  * Confirms that the finalization release successfully resets the execution lock variable to `FALSE`.
* **Logs Verification**:
  * Inspect the task logs in Cloud Logging. Ensure that the output matches the exact legacy logging formats (e.g., `[INIT RUN] Executing initialization handshake...`).

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live:

1. **Pause Orchestration:** Pause the `gcp_environment_setup_verification` DAG (and any downstream consumer DAGs) in the Airflow UI.
2. **Release Active Locks:** If a process aborted mid-execution and left a lock active, manually delete or set the corresponding Airflow Variable `lock_[job_name]_[env]` to `FALSE` via the Airflow UI (**Admin -> Variables**).
3. **Remove Artifacts:** Delete the generated files from the Cloud Composer environment:
   ```bash
   gcloud storage rm gs://[composer-bucket]/dags/gcp_environment_setup_dag.py
   gcloud storage rm gs://[composer-bucket]/plugins/gcp_environment_loader.py
   ```
4. **Revert Metadata (Optional):** If metadata tables need to be purged:
   ```sql
   DROP TABLE IF EXISTS `control_metadata.env_variable_mapping`;
   DROP TABLE IF EXISTS `control_metadata.pipeline_execution_audit`;
   DROP TABLE IF EXISTS `control_metadata.database_schema_mapping`;
   ```