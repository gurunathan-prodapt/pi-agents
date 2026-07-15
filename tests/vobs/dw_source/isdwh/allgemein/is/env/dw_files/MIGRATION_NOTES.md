# Migration Notes: Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`

These migration notes document the transition of the environment configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) from legacy KornShell profiles to a cloud-native Python configuration model within Google Cloud Platform (GCP).

---

## 1. Summary

The legacy environment configuration files historically acted as local POSIX profiles sourced during the initiation of UC4 job steps in the Information Services Data Warehouse (`isdwh`). These files have been migrated from their physical on-premises directories to a centralized, modular Python configuration package designed to run within Google Cloud Composer (Airflow) and the Horizon Python execution framework.

*   **Source Platform:** On-premises Unix/Linux filesystem (KornShell profiles sourced via UC4).
*   **Target Platform:** Google Cloud Platform (GCP) — Google Cloud Composer (Airflow) / GCS / Secret Manager.
*   **Migration Strategy:** Functional translation of shell scripts into structured Python modules, replacing local POSIX paths with Google Cloud Storage (GCS) URI mappings, and replacing hardcoded credentials with Google Cloud Secret Manager (GSM) lookups.

---

## 2. Generated Artifacts

The following Python modules have been generated to replace the legacy shell scripts. They are structured to reside in the Airflow DAGs directory under `dags/config/`:

| Generated File | Role | Legacy Equivalent |
| :--- | :--- | :--- |
| `dags/config/helpers.py` | Shared utility routines for parsing legacy POSIX files and fetching secrets from GCP Secret Manager. | *New Utility* |
| `dags/config/dw_ai.py` | Registers Ab Initio engine paths, sandbox directories, and framework execution parameters. | `.dw_ai` |
| `dags/config/dw_db.py` | Manages database connection parameters, NLS session localization, and secure credential retrieval. | `.dw_db` |
| `dags/config/dw_global.py` | Performs validation checks on required environment variables and sets up global session locales. | `.dw_global` |
| `dags/config/dw_init.py` | Master bootstrapper that maps legacy physical directories to GCS paths and orchestrates the configuration sequence. | `.dw_init` |

---

## 3. Key Design Decisions

### 3.1 Transition from POSIX Sourcing to Python Execution Context
*   **Decision:** Instead of executing shell scripts that export variables to a parent shell process, the environment is managed via Python's `os.environ` dictionary.
*   **Reasoning:** Cloud-native orchestration (Airflow) executes tasks within Python operators or isolated containers. Managing variables within `os.environ` ensures that downstream Python tasks in the same execution context can natively access these parameters.

### 3.2 GCS Path Mapping
*   **Decision:** Legacy local directories (e.g., `$HOME/daten/...`) are mapped to structured prefixes within a single Google Cloud Storage bucket (e.g., `gs://[BUCKET_NAME]/daten/...`).
*   **Reasoning:** This maintains logical compatibility with downstream legacy scripts that expect structured subdirectories while leveraging scalable, cloud-native object storage.

### 3.3 Decoupling Credentials from Code
*   **Decision:** The legacy `.dw_db` file contained hardcoded database passwords (or local `m_password` references). The migrated `dw_db.py` utilizes a helper function to dynamically fetch credentials from Google Cloud Secret Manager (GSM).
*   **Reasoning:** This aligns with enterprise security standards, preventing sensitive credentials from being committed to version control.

### 3.4 Graceful Fallbacks for Hybrid Execution
*   **Decision:** The configuration scripts include fallback mechanisms (e.g., checking local filesystem paths for `ORACLE_HOME` or reading local `.dw_lokal` files if present).
*   **Reasoning:** This supports hybrid migration phases where some components may still run on-premises or in transitional VMs before full containerization.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following infrastructure and configuration steps must be completed:

### 4.1 GCS Bucket Setup
1.  Identify or create the target GCS bucket to replace the legacy `$HOME/daten` storage.
2.  Ensure the bucket contains the standard directory structure (e.g., `/daten/logfiles`, `/daten/cubes`, `/daten/d1`, etc.).

### 4.2 IAM & Permissions
1.  The service account running the Cloud Composer workers (or Kubernetes pods) must be granted the following IAM roles:
    *   `roles/storage.objectViewer` (or `roles/storage.objectAdmin` if writing logs/exports) on the target GCS bucket.
    *   `roles/secretmanager.secretAccessor` on the database password secret in GCP Secret Manager.

### 4.3 Secret Manager Configuration
1.  Create a secret in GCP Secret Manager named `DB_PASSWD_DWH`.
2.  Add the decrypted database password as the latest version of this secret.

### 4.4 Airflow Environment Variables
Configure the following Airflow Variables (or environment variables in the Composer environment):
*   `GCP_PROJECT`: The target GCP Project ID.
*   `GCS_BUCKET`: The target GCS bucket URI (e.g., `gs://my-dwh-bucket`).
*   `DIR_LIB_PY`: The path to the Horizon Python framework library (if applicable).

---

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up and may require architectural redesign (B4) or manual intervention:

### 5.1 Missing Legacy Dependencies
*   **`SETPYA.SH` (Cognos Powerplay Setup Script):** This script was referenced in the legacy `.dw_global` but was not present in the source codebase. The migrated `dw_global.py` logs a warning and skips execution if the file is missing. If Cognos integration is still required, this initialization must be redesigned.
*   **`.dw_lokal` (Local Machine Overrides):** This file is sourced dynamically if present in the user's home directory. In a containerized or serverless environment, local overrides should be replaced by Airflow connection parameters or environment-specific variables.

### 5.2 Downstream Job Integration
The following downstream jobs are not yet migrated and must be updated to import and execute `dw_init.bootstrap_environment()` once they transition to Python/Airflow:
*   `DW.DWH_ABPZ_KKM_AIL_AGENT`
*   `r_ai_start`
*   `vobs/dw_source/isdwh/abinitio/bin/r_ai_start`

---

## 6. Validation

To validate the migrated configuration modules, execute the following tests in the target environment:

### 6.1 Unit Testing the Bootstrapper
Run the master bootstrapper directly from a terminal or test container:
```bash
export GCP_PROJECT="your-gcp-project-id"
export GCS_BUCKET="gs://your-test-bucket"
python3 dags/config/dw_init.py
```

#### Passing Criteria:
*   The script exits with code `0`.
*   Console output displays:
    ```text
    [INFO] Database context configuration variables loaded.
    [SUCCESS] Ab Initio Engine settings updated successfully.
    Environment setup completed successfully.
    ```
*   Verify that the environment variables are correctly registered in the active Python process by printing them:
    ```python
    import os
    print(os.environ.get('DW_DIR_PROT'))  # Should output: gs://your-test-bucket/daten/logfiles
    print(os.environ.get('DB_USER_DWH'))  # Should output: meyreis
    ```

### 6.2 Secret Manager Integration Test
Temporarily rename or remove the fallback password in `dw_db.py` and verify that the script successfully fetches the secret from GSM.

#### Passing Criteria:
*   No permission errors are thrown.
*   `os.environ['DB_PASSWD_DWH']` contains the actual secret value retrieved from GCP Secret Manager instead of the placeholder `<password encrypted with m_password>`.

---

## 7. Rollback Procedure

In the event of a deployment failure or unexpected behavior in downstream jobs, execute the following rollback steps:

1.  **Revert Code Changes:** Roll back the deployment of the `dags/config/` directory to the previous stable commit in version control.
2.  **Restore Legacy Sourcing:** If running in a hybrid environment, ensure that legacy shell scripts continue to source the original physical files (`.dw_init`, `.dw_global`, etc.) on the local filesystem.
3.  **Verify Environment Variables:** Ensure that the legacy environment variables (such as `DW_DIR_ROOT` and `ORACLE_HOME`) point back to the local POSIX paths instead of GCS bucket URIs.