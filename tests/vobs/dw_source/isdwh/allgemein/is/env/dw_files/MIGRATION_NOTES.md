# Migration Notes

**Job Name**: Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Target Platform**: Google Cloud Platform (BigQuery, Cloud Storage, Cloud Composer / Airflow, Secret Manager)

---

## 1. Summary

This migration transitions the legacy Ab Initio and Oracle environment configuration scripts (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) into a unified, cloud-native Python configuration framework. 

The legacy shell-based environment variables, local Unix filesystem paths, and hardcoded database credentials have been migrated to **Google Cloud Platform (GCP)**. The target architecture leverages **Cloud Storage (GCS)** for file paths, **Google Cloud Secret Manager** for secure credential storage, and **Cloud Composer (Airflow)** as the orchestration and execution environment.

---

## 2. Generated Artifacts

The migration consolidates the legacy shell scripts into a modular Python package deployed within the Airflow DAGs folder structure (`dags/submodules/`):

| Generated File | Role / Description | Legacy Source File(s) |
| :--- | :--- | :--- |
| `dw_config_helper.py` | **Reusable Utility Class**: Centralizes GCP project parameter resolution, dynamic GCS path mapping, environment validation, and secure Secret Manager access. | *New architectural component* |
| `dw_global.py` | **Global Configuration Engine**: Validates environment variables, configures backwards-compatible Oracle NLS session settings, and exports global metadata. | `.dw_global`, `.dw_ai` |
| `dw_init.py` | **Core Orchestration & Mapping Hub**: Replaces legacy Unix local directory paths with logical GCS bucket URI mappings and loads database credentials. | `.dw_init`, `.dw_db` |
| `dw_ai.json` | **Structured Metadata Config**: JSON representation of legacy Ab Initio EME parameters, uploaded to GCS for historical reference. | `.dw_ai` |

---

## 3. Key Design Decisions

* **Transition from Local Filesystem to GCS**: Legacy Unix paths (e.g., `/home/horizon/daten/d1`) are dynamically mapped to logical Cloud Storage URIs (e.g., `gs://[GCS_BUCKET]/dwh/imports/d1`). This allows downstream PySpark and BigQuery jobs to use the same environment variable names while reading directly from object storage.
* **Decoupling Credentials via Secret Manager**: Hardcoded database passwords and legacy Ab Initio `m_password` obfuscated strings are retired. The migrated code dynamically fetches credentials at runtime from Google Cloud Secret Manager using the application's Service Account.
* **Modular Python Package over Shell Sourcing**: Sourcing shell scripts does not translate natively to distributed Cloud Composer workers. Converting these configurations into Python modules (`dw_init.py`, `dw_global.py`) allows Airflow tasks to import and execute initialization routines in-memory.
* **Graceful Oracle Fallbacks**: While the target platform is BigQuery, legacy Oracle environment variables (such as `NLS_LANG` and `ORACLE_HOME` detection) are preserved to support hybrid execution phases where some downstream Oracle tasks are not yet retired.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed in the target GCP environment:

### Schema & Dataset Creation
1. Ensure the target BigQuery dataset (defined by the `BQ_DATASET` variable) is created in your project.

### IAM & Permissions
1. The Cloud Composer / Airflow worker Service Account must be granted the following IAM roles:
   * **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) on the database credential secret.
   * **Storage Object Viewer** (`roles/storage.objectViewer`) and **Storage Object Creator** (`roles/storage.objectCreator`) on the GCS bucket.

### Secrets Creation
1. Create a secret in Google Cloud Secret Manager named `DB_PASSWD_DWH`.
2. Store the plaintext Oracle database password as the latest version of this secret.

### Airflow Variables & Environment Variables
Configure the following variables in Cloud Composer (either as Airflow Variables or Environment Variables):
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `GCS_BUCKET`: The target Cloud Storage bucket name (e.g., `my-dwh-data-bucket`).
* `BQ_DATASET`: The target BigQuery dataset name.

---

## 5. Known Gaps & Unresolved References

The following items were flagged during migration and require manual resolution or coordination with downstream migration waves:

* **Missing Upstream Dependencies**:
  * `.dw_lokal` (Legacy local environment script) was not found in the source code repository. Local overrides must be defined directly in Airflow variables if needed.
  * `setpya.sh` (Legacy Cognos script) was not migrated. Cognos-specific environment sourcing is currently bypassed.
* **Unmigrated Downstream Consumers**:
  * Downstream jobs such as `DW.DWH_ABPZ_KKM_AIL_AGENT` and `r_ai_start` have not yet been migrated. End-to-end integration testing must be scheduled once these components are deployed to GCP.
* **Redesign (B4) Items**:
  * **Oracle Client Dependency**: The `ORACLE_HOME` resolution logic in `dw_init.py` searches for local directories. Once all downstream database tasks are migrated to BigQuery, this logic and the `dw_global.py` NLS settings should be completely deprecated.

---

## 6. Validation

To validate the migration of the environment configuration files, execute the following tests:

### Local/Unit Test Execution
Run the initialization script directly in a Python environment where the required environment variables are set:

```bash
export GCP_PROJECT="your-gcp-project-id"
export GCS_BUCKET="your-gcs-bucket-name"
export BQ_DATASET="your_bq_dataset"

python3 dw_init.py
```

### What "Passing" Means
1. **Zero Exceptions**: The script executes to completion without throwing `KeyError` or `ImportError`.
2. **Correct Path Mapping**: Verify that the environment variables are correctly populated with GCS URIs. For example, printing `os.environ.get("DW_DIR_IMP_D1")` should output `gs://your-gcs-bucket-name/dwh/imports/d1`.
3. **Secret Retrieval**: The log output should confirm whether the secret was successfully retrieved from Secret Manager or if it fell back to the placeholder (in non-GCP local environments).

---

## 7. Rollback Procedure

If issues are encountered during deployment, follow these steps to roll back:

1. **Remove Airflow Submodules**: Delete the migrated files (`dw_config_helper.py`, `dw_global.py`, `dw_init.py`) from the `dags/submodules/` directory in the Cloud Storage DAGs bucket.
2. **Revert Airflow Variables**: Delete or revert any newly added Airflow Variables (`GCS_BUCKET`, `BQ_DATASET`) if they conflict with legacy configurations.
3. **Restore Legacy Execution**: If running in a hybrid environment, redirect downstream shell scripts to source the legacy `.dw_init` and `.dw_global` files from the on-premises network share or legacy VM mount.