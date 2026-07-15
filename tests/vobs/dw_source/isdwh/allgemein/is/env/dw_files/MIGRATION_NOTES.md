# Migration Notes

**Job Name:** Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Target Platform:** Google Cloud Platform (GCP) — Google BigQuery & Cloud Composer (Airflow)  
**Migration Pattern:** `UC4_ONLY` (Centralized Environment Configuration & Variable Injection)

---

## 1. Summary

This migration covers the transition of legacy KornShell (`ksh_dwh`) environment initialization and configuration files to a modern, cloud-native Python structure. 

### 1.1 What Was Migrated
The legacy configuration suite establishes structural path definitions, Ab Initio ETL metadata framework parameters, database connection strings, and localization settings. These files have been consolidated and refactored into structured Python modules utilizing the Horizon Python runtime framework.

### 1.2 Target Platform
*   **Orchestration:** Google Cloud Composer (Airflow)
*   **Execution Runtime:** Horizon Python Runtime Environment
*   **Data Warehouse:** Google BigQuery (for downstream consumers)
*   **Secrets Management:** GCP Secret Manager / Airflow Connections

---

## 2. Generated Artifacts

The following Python modules have been generated to replace the legacy shell configuration scripts:

| Target File Relative Path | Language | Source File | Role / Purpose |
| :--- | :--- | :--- | :--- |
| `dwh_env_config/dw_ai.py` | Python (Horizon) | `.dw_ai` | Sets Ab Initio framework metadata, sandbox roots, and execution paths. |
| `dwh_env_config/dw_db.py` | Python (Horizon) | `.dw_db` | Handles Oracle database credentials and character encoding configurations. |
| `dwh_env_config/dw_global.py` | Python (Horizon) | `.dw_global` | Performs environment validations, sets Oracle NLS parameters, and handles Cognos configurations. |
| `dwh_env_config/dw_init.py` | Python (Horizon) | `.dw_init` | Primary environment initialization entry point; maps legacy directory structures to environment variables. |

---

## 3. Key Design Decisions

### 3.1 Transition from Shell Sourcing to Python Module Execution
*   **Decision:** Instead of sourcing shell scripts sequentially (which modifies the parent shell process), the configuration has been modularized into Python scripts that manipulate `os.environ` and configuration dictionaries.
*   **Trade-off:** Python subprocesses cannot natively modify the environment of their parent process. Downstream Python tasks must import these modules or run within the same execution context (e.g., a unified Airflow task or custom operator) to inherit these environment variables.

### 3.2 Decoupling of Legacy Physical Paths
*   **Decision:** Legacy physical paths (e.g., `/appl/local/abinitio/...` and `/appl/local/oracle/...`) are retained as string literals within `os.environ` to preserve compatibility with downstream legacy scripts that may still reference them.
*   **Trade-off:** These paths will not resolve to active local directories on serverless GCP execution environments. Downstream processes must be updated to map these variables to Google Cloud Storage (GCS) buckets (e.g., `gs://<bucket-name>/daten/...`).

### 3.3 Dynamic Shell Sourcing Helper
*   **Decision:** A helper function `source_shell_script` was implemented in `dw_global.py` using `subprocess.Popen` to execute legacy scripts (like `setpya.sh`) and capture their exported variables back into Python's `os.environ`.
*   **Trade-off:** This introduces a dependency on a local shell interpreter (`/bin/ksh`) during the transition phase. Once all configurations are fully migrated to GCP, this helper should be deprecated in favor of direct Python configuration dictionaries.

---

## 4. Manual Steps Before Go-Live

### 4.1 Schema & Dataset Creation
*   Ensure that the target BigQuery datasets referenced by downstream consumers are created and configured with the correct region and KMS keys.

### 4.2 IAM & Permissions
*   The Cloud Composer Service Account must have the following roles:
    *   `roles/secretmanager.secretAccessor` (to retrieve database credentials).
    *   `roles/storage.objectViewer` / `roles/storage.objectAdmin` on the GCS buckets replacing the `$HOME/daten` directories.

### 4.3 Connection Strings & Secrets
1.  **Database Credentials:** Do not hardcode the decrypted password from `.dw_db` in production.
2.  Create a secret in **GCP Secret Manager** named `oracle-dwh-credentials` containing:
    ```json
    {
      "username": "meyreis",
      "password": "<decrypted_password>",
      "tns_name": "eDWH3.devlab.de.tmo"
    }
    ```
3.  Update `dw_db.py` to fetch these values dynamically using the Secret Manager API or configure them as an **Airflow Connection** (`oracle_dwh_default`).

### 4.4 Scheduling & Variable Injection
*   Import the configuration variables into the Airflow Metadata Database as Airflow Variables or inject them via a common `default_args` dictionary in your DAGs.
*   Configure the environment variables in the Cloud Composer Environment Configuration so they are globally available to all workers.

---

## 5. Known Gaps & Unresolved References

### 5.1 Missing Source Files (B4 Redesign Items)
*   **`.dw_lokal`:** Sourced by `.dw_init` but was not found in the source filesystem scan. A warning stub is implemented in `dw_init.py`. If local overrides are required, a `dw_lokal.py` file must be created manually.
*   **`setpya.sh`:** Sourced by `.dw_global` but was missing from the filesystem scan. A warning stub is configured in `dw_global.py`.

### 5.2 Downstream Dependency Wiring
The following downstream consumers of these shared files are **not yet migrated**:
*   `DW.DWH_ABPZ_KKM_AIL_AGENT`
*   `r_ai_start`
*   `vobs/dw_source/isdwh/abinitio/bin/r_ai_start`

> [!WARNING]  
> Cross-DAG execution dependencies and sensors cannot be finalized until these downstream jobs are migrated to Cloud Composer.

---

## 6. Validation

### 6.1 How to Run the Tests
To validate the migrated configuration scripts locally or in a development environment, execute the following commands:

```bash
# Set the Horizon library path environment variable
export DIR_LIB_PY="/path/to/horizon/framework/lib"

# Run the initialization scripts sequentially
python3 dwh_env_config/dw_init.py
python3 dwh_env_config/dw_global.py
python3 dwh_env_config/dw_ai.py
python3 dwh_env_config/dw_db.py
```

### 6.2 What "Passing" Means
*   The scripts must execute with exit code `0`.
*   The console output must display:
    ```text
    [INFO] Sourcing configurations from: ...
    [WARNING] Configuration file .dw_lokal not found. (Expected until resolved)
    Environment and database connection variables initialized successfully.
    Horizon global variables initialized successfully.
    [SUCCESS] Environment initialization completed.
    ```
*   All critical environment variables (e.g., `DW_DIR_ROOT`, `NLS_LANG`, `DB_USER_DWH`) must be successfully populated in the runtime process environment.

---

## 7. Rollback Procedure

In the event of a deployment failure or validation error in the target environment:

1.  **Revert Airflow Variables:** Restore the previous Airflow Variable configurations in the Composer UI.
2.  **Revert Secret Manager:** If database credentials were changed, roll back the secret version in GCP Secret Manager to the previous active version.
3.  **DAG Disable:** Pause the newly deployed initialization DAGs in the Airflow UI.
4.  **Legacy Fallback:** Route downstream execution back to the on-premises UC4 engine running the legacy `.dw_init` and `.dw_global` KornShell scripts.