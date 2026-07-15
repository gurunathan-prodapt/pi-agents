# Migration Notes: Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files

## 1. Summary
This migration covers the legacy environment initialization and configuration profiles (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) located under the source root `/home/gurunathan_t/folder1_uc4_ksh_abinitio`. 

In the legacy architecture, these files were sourced sequentially by UNIX shell wrappers to configure Ab Initio environments, Oracle database connections, localized formats, directory trees, and system paths. 

These profiles have been migrated to **Google Cloud Platform (GCP)**, targeting **BigQuery** and **Cloud Composer (Apache Airflow)**. The sequential shell-sourcing pattern has been refactored into a unified, importable Python environment configuration module (`dwh_env_config.py`) and integrated with Airflow Variables and Google Cloud Secret Manager.

---

## 2. Generated Artifacts
The legacy shell profiles have been consolidated into a single, highly maintainable Python module:

| Target File Path | Target Language | Role / Purpose |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dwh_env_config.py` | Python 3 | Consolidated environment configuration manager. Replicates the logic of `.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`. Exposes structured profiles as Python dictionaries and binds variables to `os.environ` for downstream compatibility. |

---

## 3. Key Design Decisions

### Consolidation of Shell Profiles
Instead of maintaining four separate shell scripts (`.dw_ai`, `.dw_db`, `.dw_global`, `.dw_init`) that rely on sequential execution and shell-sourcing side effects, we consolidated the logic into a single Python class `DWHConfigManager`. This reduces disk I/O, simplifies debugging, and allows Airflow DAGs or PySpark jobs to import settings natively.

### Cloud Storage (GCS) Path Mapping
Legacy local directory paths (`$HOME/daten/*`) have been mapped to Google Cloud Storage (GCS) URI prefixes (`gs://{GCS_BUCKET}/dwh/daten/*`). This preserves the logical directory structure while transitioning the physical storage layer to serverless cloud storage.

### Redundant Variable Preservation
Traditional Oracle-specific session encodings (`NLS_LANG`, `NLS_DATE_FORMAT`) and Ab Initio system variables (`AB_HOME`, `AI_ENV_SAND_ROOT`) are obsolete in a native BigQuery environment. However, to maintain functional parity and prevent runtime failures in downstream components that may still reference them, these variables are preserved and exported to the execution context (`os.environ`).

### Decoupling of Secrets
Hardcoded passwords and legacy `m_password` encrypted strings have been removed from the codebase. The Python module is designed to dynamically fetch credentials from Google Cloud Secret Manager when running in a cloud environment.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variable Creation
Ensure the following Airflow Variable is defined in your Cloud Composer environment:
* **Key:** `GCS_BUCKET`
* **Value:** `your-gcp-dwh-bucket-name` (do not include the `gs://` prefix)

### 2. IAM & Permissions
The Cloud Composer service account (or the runtime service account for Dataproc/Cloud Run) must be granted the following IAM roles:
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the GCS bucket configured above.
* **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) on the database credential secret.

### 3. Secret Manager Setup
1. Create a secret in Google Cloud Secret Manager named `dwh-oracle-password`.
2. Store the plaintext database password as the secret payload.
3. Note the resource name (e.g., `projects/{PROJECT_ID}/secrets/dwh-oracle-password/versions/latest`).

### 4. Downstream Scheduling & Wiring
Because downstream consumers such as `DW.DWH_ABPZ_KKM_AIL_AGENT` and `r_ai_start` are not yet migrated, you must manually verify that any temporary bridge wrappers source `dwh_env_config.py` via a Python execution step before triggering legacy components.

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) Items & Missing Files
* **`.dw_lokal` (Missing Source File):** The legacy `.dw_init` script dynamically sources `$HOME/.dw_lokal` at runtime. This file was not present in the source codebase. It represents local developer overrides. If local overrides are required in GCP, they must be injected via Composer environment variables.
* **`setpya.sh` (Missing Source File):** The legacy `.dw_global` script attempts to source `/appl/local/cognos/pya60207/setpya.sh`. This Cognos configuration profile is missing and obsolete. The migrated Python module logs a bypass warning and skips execution.
* **Oracle Connectivity:** The variable `DB_TNS_NAME_DWH` (`@eDWH3.devlab.de.tmo`) points to an on-premise Oracle instance. If downstream jobs require federated queries from BigQuery to this Oracle instance, a Cloud SQL Auth Proxy or VPC Network Peering must be established.

---

## 6. Validation

### How to Run the Tests
You can validate the migrated configuration module locally or within a container by executing it directly with Python 3:

```bash
# Set up mock environment variables
export GCS_BUCKET="my-test-bucket"
export ORACLE_SID="TEST_SID"

# Run the configuration manager
python3 vobs/dw_source/isdwh/allgemein/is/env/dw_files/dwh_env_config.py
```

### What "Passing" Means
A successful validation run must meet the following criteria:
1. The script exits with status code `0`.
2. The console output displays:
   ```text
   === Executing Horizon Environment Profiling Initialization ===
   [INFO] Initializing directory maps and infrastructure targets (.dw_init)...
   [INFO] Initializing Ab Initio legacy environment configuration (.dw_ai)...
   [INFO] Initializing database connection configuration (.dw_db)...
   [INFO] Validating global environment dependencies (.dw_global)...
   ```
3. The exact legacy German error messages are printed to `stderr` if critical variables (like `ORACLE_HOME` or directory paths) are missing, matching legacy behavior:
   ```text
   Fehler in .dw_global:
      Umgebungsvariable ORACLE_HOME ist nicht gesetzt !
   ```
4. The output confirms that variables are correctly mapped to GCS prefixes (e.g., `DW_DIR_PROT` resolves to `gs://my-test-bucket/dwh/daten/logfiles`).

---

## 7. Rollback Procedure

In the event of an operational failure during deployment:

1. **Revert Airflow Variables:** If Airflow variables were modified, restore them to their previous values via the Airflow UI (**Admin -> Variables**).
2. **Revert Shell Wrappers:** If downstream shell wrappers were modified to call `dwh_env_config.py`, revert those wrappers to their previous versions in Git to resume sourcing the legacy `.dw_init` and `.dw_global` files.
3. **Verify Local Environment:** Ensure that the legacy environment variables are still exported correctly in the fallback environment by running:
   ```bash
   source $HOME/.dw_init
   env | grep DW_DIR_
   ```