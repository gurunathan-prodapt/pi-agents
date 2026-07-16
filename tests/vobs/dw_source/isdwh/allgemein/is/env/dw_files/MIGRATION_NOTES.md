# Migration Notes: Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files

## 1. Summary

This migration transitions the legacy environment configuration scripts and database connection profiles located in `vobs/dw_source/isdwh/allgemein/is/env/dw_files` to a secure, cloud-native architecture on **Google Cloud Platform (GCP)**. 

### Migrated Components
* **`.dw_ai`**: Legacy Ab Initio environment setup variables.
* **`.dw_db`**: Legacy Oracle database connection profiles and credentials.
* **`.dw_global`**: Global directory structures and National Language Support (NLS) settings.
* **`.dw_init`**: Core environment initialization orchestrator.

### Target Platform
* **Orchestration**: Google Cloud Composer (Apache Airflow)
* **Data Warehouse**: Google BigQuery
* **Secrets Management**: Google Secret Manager
* **Storage**: Google Cloud Storage (GCS)
* **Execution Runtime**: Python 3 (Horizon Python framework compatible)

---

## 2. Generated Artifacts

The migration process has generated four key Python modules, preserving the directory structure of the legacy repository:

| Generated File Path | Role / Description |
| :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_ai.py` | Migrates legacy Ab Initio environment variables (`AB_HOME`, `AB_AIR_ROOT`, etc.) into a Python configuration block. Provides helper functions to inject variables into `os.environ` or export them for Airflow tasks. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_db.py` | Replaces the legacy plain-text/obfuscated `.dw_db` file. Dynamically retrieves credentials from Google Secret Manager, maps Oracle NLS settings to BigQuery-compatible formats, and logs execution metrics to BigQuery audit tables. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_global_config.py` | Replaces `.dw_global`. Maps legacy local directory paths to GCS bucket paths, validates that required environment variables are set, and preserves legacy German error reporting formats. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init.py` | The main orchestrator script. Replaces the legacy `.dw_init` shell script. It coordinates the execution of `dw_global_config.py` and `dw_db.py`, resolves path placeholders (such as `$HOME`), and configures the runtime environment. |

---

## 3. Key Design Decisions

### Decoupled Secrets Management
* **Decision**: Completely remove hardcoded or obfuscated database credentials from environment files.
* **Reasoning**: Storing credentials in plain-text or local obfuscated files violates modern cloud security standards. Utilizing **Google Secret Manager** ensures centralized access control, encryption at rest and in transit, and full audit logging.

### Transition-Phase Compatibility
* **Decision**: Retain legacy environment variables (e.g., `AB_HOME`, `ORACLE_SID`) in Python dictionaries and inject them into `os.environ` at runtime.
* **Reasoning**: Downstream processes (such as `r_ai_start` and `DW.DWH_ABPZ_KKM_AIL_AGENT`) are not yet migrated. Preserving these variables ensures that legacy wrapper scripts can still function during the transition phase.

### Local Directory to GCS Mapping
* **Decision**: Map legacy local directory paths (e.g., `DW_DIR_ROOT`, `DW_DIR_IMP_D1`) to logical Google Cloud Storage (GCS) paths using a configurable base bucket.
* **Reasoning**: Cloud Composer workers are ephemeral and serverless. Persistent file storage must be transitioned to GCS to ensure scalability and durability.

### Preservation of Verbatim German Log Outputs
* **Decision**: Retain exact German print statements (e.g., `"Fehler in .dw_global: Umgebungsvariable {varname} ist nicht gesetzt !"`) in the validation logic.
* **Reasoning**: Downstream log parsers or operational monitoring tools may rely on these exact string patterns to trigger alerts.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed:

### 1. BigQuery Schema Creation
Execute the following DDL statements in your target BigQuery project to create the metadata and audit tables:

```sql
-- Create Metadata Dataset
CREATE SCHEMA IF NOT EXISTS `your_project_id.migration_metadata`
OPTIONS(location="europe-west3");

-- Create Environment Configurations Table
CREATE OR REPLACE TABLE `your_project_id.migration_metadata.dw_environment_configurations` (
  config_id STRING NOT NULL OPTIONS(description="Unique configuration UUID"),
  legacy_sid STRING OPTIONS(description="Legacy Oracle System Identifier mapping"),
  gcp_project_id STRING NOT NULL OPTIONS(description="Target GCP Project Identifier"),
  bq_dataset_id STRING NOT NULL OPTIONS(description="Target BigQuery Dataset Name"),
  nls_lang_mapped STRING NOT NULL DEFAULT 'UTF-8' OPTIONS(description="Character encoding"),
  secret_manager_path STRING NOT NULL OPTIONS(description="Full resource identifier path of Secret Manager secret"),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create Audit Log Table
CREATE OR REPLACE TABLE `your_project_id.migration_metadata.gcp_migration_audit_log` (
  log_id STRING NOT NULL OPTIONS(description="Unique ID of execution run"),
  job_name STRING NOT NULL OPTIONS(description="Name of the migrated ETL job or run"),
  execution_status STRING NOT NULL OPTIONS(description="Status of the task (SUCCESS, FAILED, RUNNING)"),
  records_processed INT64 OPTIONS(description="Total records updated or written during run"),
  error_message STRING OPTIONS(description="Captured exceptions if runtime failed"),
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  finished_at TIMESTAMP
);
```

### 2. Google Secret Manager Setup
1. Create a secret named `dw-database-access-credentials` in Google Secret Manager.
2. Populate the secret payload with the following JSON structure:
   ```json
   {
     "ORACLE_SID": "DWPROD",
     "DB_USER_DWH": "sa-dwh-meyreis",
     "NLS_LANG": "GERMAN_GERMANY.WE8ISO8859P1",
     "NLS_DATE_FORMAT": "YYYY-MM-DD"
   }
   ```

### 3. IAM Permissions
Grant the Cloud Composer Service Account (e.g., `service-account@your-project.iam.gserviceaccount.com`) the following IAM roles:
* **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) on the `dw-database-access-credentials` secret.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the `migration_metadata` dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the target GCP project.

### 4. Airflow Variables Configuration
Configure the following Airflow Variables in your Cloud Composer environment:
* `GCS_BUCKET`: `your-gcs-landing-bucket-name`
* `GCP_PROJECT_ID`: `your-gcp-project-id`
* `BQ_DATASET_ID`: `migration_metadata`

---

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up and must be addressed before final decommissioning of the legacy environment:

### 1. Unresolved Local Configuration (`.dw_lokal`)
* **Gap**: The legacy `.dw_init` script references a local configuration file named `.dw_lokal` which was not present in the scanned codebase.
* **Remediation**: A placeholder check has been implemented in `.dw_init.py`. You must manually recreate any local overrides in a `.dw_lokal.json` file in the execution home directory or define them as Airflow Variables.

### 2. Missing Legacy Scripts (`setpya.sh` & `dwh_init`)
* **Gap**: References to `/appl/local/cognos/pya60207/setpya.sh` (Cognos PowerPlay setup) and the parent initializer `dwh_init` are missing from the scanned context.
* **Remediation**: If Cognos integrations are still active, the `setpya.sh` script must be mounted into the Airflow worker containers, or its variables must be ported to `dw_global_config.py`.

### 3. Downstream Wiring (B4 Redesign Items)
* **Gap**: Downstream consumers (`DW.DWH_ABPZ_KKM_AIL_AGENT`, `r_ai_start`, and `vobs/dw_source/isdwh/abinitio/bin/r_ai_start`) are not yet migrated.
* **Remediation**: Once these downstream processes are migrated to Cloud Composer, their DAGs must be configured to import and execute `validate_and_get_dw_env()` from `dw_global_config.py` during task initialization.

---

## 6. Validation

To validate the migrated environment configurations, run the scripts in a staging environment.

### Execution Commands

#### 1. Validate Environment Initialization
Run the core environment initializer to verify directory mappings and Oracle home resolution:
```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export BQ_DATASET_ID="migration_metadata"
export SECRET_NAME="dw-database-access-credentials"

python3 vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init.py
```

#### 2. Validate Secret Manager & BigQuery Integration
Run the database configuration engine to verify Secret Manager retrieval and BigQuery logging:
```bash
python3 vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_db.py
```

### What "Passing" Looks Like
* **Console Output**: The execution completes with exit code `0`.
* **Logs**: You should see structured log entries confirming successful secret retrieval:
  ```text
  [INFO] dw_db_migration_engine: Accessing Secret Manager payload: projects/your-project-id/secrets/dw-database-access-credentials/versions/latest
  [INFO] dw_db_migration_engine: Successfully persisted environment settings mapping to BigQuery.
  ```
* **BigQuery Verification**: Query the audit and configuration tables to verify that a new record has been written:
  ```sql
  SELECT * FROM `your_project_id.migration_metadata.gcp_migration_audit_log` 
  WHERE job_name = 'dw_db_migration_initialization' 
  ORDER BY started_at DESC LIMIT 1;
  ```
  *The `execution_status` column must read `SUCCESS`.*

---

## 7. Rollback Procedure

In the event of an operational failure during deployment, perform the following steps to roll back to the legacy environment:

1. **Disable Cloud Composer DAGs**: Pause any newly deployed Airflow DAGs that reference the migrated `dw_files` Python modules.
2. **Restore Legacy Sourcing**: Revert downstream wrapper scripts to source the legacy shell scripts:
   ```bash
   . /home/gurunathan_t/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init
   ```
3. **Audit Logs**: Check the `gcp_migration_audit_log` table in BigQuery to identify the exact timestamp and error message associated with the failure.