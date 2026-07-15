# Migration Notes

**Job / Seed:** Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Source Path:** `/home/gurunathan_t/folder1_uc4_ksh_abinitio`  
**Target Platform:** Google Cloud BigQuery / Cloud Composer (Airflow)  
**Migration Pattern:** `UC4_ONLY` (Pure environment, orchestration initialization, and path-mapping configuration)

---

## 1. Summary

This migration covers the transition of the legacy on-premise environment initialization scripts (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) to a cloud-native configuration model on Google Cloud Platform (GCP). 

Historically, these files were sourced dynamically by interactive shells, Ab Initio EME runtimes, and KornShell wrapper scripts to establish POSIX file system boundaries, Oracle database connections, locale settings, and Ab Initio framework parameters. 

In the target architecture:
*   **Orchestration & Environment Initialization:** Migrated from static shell sourcing to dynamic configuration files (`.json`) and a Python-based validation engine integrated into **Cloud Composer (Airflow)**.
*   **Data Layer & Storage:** POSIX file paths are mapped to unified **Google Cloud Storage (GCS)** URIs.
*   **Database Connectivity:** Legacy Oracle parameters and encrypted passwords are retired in favor of native **GCP Service Accounts**, **Secret Manager**, and **Airflow Connections** pointing to **Google BigQuery**.

---

## 2. Generated Artifacts

The following target files have been generated to replace the legacy shell scripts:

| Source File | Target File Path | Target Language | Role / Description |
| :--- | :--- | :--- | :--- |
| `.dw_ai` | `/dags/config/env_ai_config.json` | JSON | Holds environment variables for Ab Initio compatibility (e.g., sandbox roots, project codes). |
| `.dw_db` | *Retired as a file* | IAM / Secret Manager | Connection parameters and credentials are migrated securely to GCP Secret Manager and Airflow Connections. |
| `.dw_global` | `/dags/utils/env_validator.py` | Python | Python validation engine that replaces the legacy validation logic, ensuring runtime parameters are populated. |
| `.dw_init` | `/dags/config/gcs_paths.json` | JSON | Dictionary mapping legacy POSIX directories directly to GCS path URIs using `{GCS_BUCKET}` placeholders. |

---

## 3. Key Design Decisions

### Decoupling Shell Sourcing from Runtimes
*   **Decision:** Replace KornShell sourcing (`. .dw_init`) with structured JSON configuration files and a Python validation class (`ConfigurationEngine`).
*   **Reasoning:** Sourcing shell scripts is highly error-prone, platform-dependent, and difficult to test. Structured JSON files can be parsed natively by Python, Airflow, and other cloud services, enabling dynamic runtime injection.

### POSIX to GCS Path Mapping
*   **Decision:** Standardize all `$HOME/daten/...` paths to GCS URIs (`gs://{GCS_BUCKET}/daten/...`) using a template placeholder.
*   **Reasoning:** This allows the same configuration file to be utilized across multiple environments (Development, QA, Production) simply by changing the `GCS_BUCKET` variable in Cloud Composer.

### Retirement of Oracle-Specific Parameters
*   **Decision:** Retire `ORACLE_HOME`, `ORACLE_SID`, `DB_TNS_NAME_DWH`, and `DW_DIR_UTL_FILE`.
*   **Reasoning:** BigQuery is serverless and does not require local client installations, TNS aliases, or local directory paths for file-based database utilities (`UTL_FILE`).

### Security & Password Storage
*   **Decision:** Completely remove the encrypted password string from the codebase.
*   **Reasoning:** Storing passwords in code (even if encrypted with Ab Initio's `m_password`) violates modern cloud security standards. Credentials are now managed via GCP IAM and Secret Manager.

---

## 4. Manual Steps Before Go-Live

### 1. GCS Bucket Creation
Ensure the target GCS bucket is created in your project:
```bash
gsutil mb -c standard -l europe-west3 gs://your-dwh-bucket-name
```

### 2. Airflow Environment Variables
Configure the following Global Variables in your Cloud Composer environment:
*   `GCS_BUCKET`: `gs://your-dwh-bucket-name` (e.g., `gs://dwh-isdwh-prod`)
*   `GCP_PROJECT`: Your GCP Project ID
*   `BQ_LOCATION`: `EU` or `US`

### 3. IAM & Permissions
Grant the Cloud Composer Service Account the following IAM roles:
*   `roles/storage.objectAdmin` on the target GCS bucket.
*   `roles/bigquery.admin` on the target BigQuery datasets.
*   `roles/secretmanager.secretAccessor` if accessing external credentials.

### 4. Deploy Configuration Files
Upload the generated configuration files to your Cloud Composer DAGs folder:
*   Upload `env_ai_config.json` and `gcs_paths.json` to `gs://<composer-bucket>/dags/config/`
*   Upload `env_validator.py` to `gs://<composer-bucket>/dags/utils/`

---

## 5. Known Gaps & Unresolved References

### 1. Missing `.dw_lokal` Sourcing
*   **Gap:** The legacy `.dw_init` script conditionally sources `$HOME/.dw_lokal` if it exists. This file was not provided in the source seed.
*   **Mitigation:** If host-local parameters are required in the future, they must be added to the Airflow Environment Variables or appended to `env_ai_config.json`.

### 2. Retired Cognos Integration (`setpya.sh`)
*   **Gap:** The legacy `.dw_global` script sources `/appl/local/cognos/pya60207/setpya.sh` for Cognos PowerPlay.
*   **Mitigation:** This integration is marked as **out of scope** for the BigQuery target runtime. Downstream BI reporting must be migrated to Looker or another modern BI tool.

### 3. Downstream Job Wiring
*   **Gap:** Downstream consumers such as `DW.DWH_ABPZ_KKM_AIL_AGENT` and `r_ai_start` are not yet migrated.
*   **Mitigation:** When migrating these downstream components, ensure they are refactored to read configurations from `gcs_paths.json` or Airflow variables instead of attempting to source `.dw_init`.

---

## 6. Validation

A self-contained validation routine is built directly into `env_validator.py`. This can be executed locally or in a CI/CD pipeline to verify configuration integrity.

### Running the Validation Locally

1. Set up a local test environment:
   ```bash
   export GCS_BUCKET="gs://dwh-isdwh-local-dev"
   ```

2. Ensure your local directory structure matches the expected layout:
   ```text
   ├── dags/
   │   ├── config/
   │   │   ├── env_ai_config.json
   │   │   └── gcs_paths.json
   │   └── utils/
   │       └── env_validator.py
   ```

3. Execute the validation script:
   ```bash
   python3 dags/utils/env_validator.py
   ```

### What "Passing" Means
A successful validation run will output:
*   `Successfully resolved 45 runtime path configurations.`
*   `Successfully loaded Ab Initio compatibility runtime parameters.`
*   `Configuration Validation successfully completed. All environment layers set.`
*   `Environment Validation Status Passed: True`

If any mandatory keys (such as `DW_DIR_ROOT` or `DW_DIR_PROT`) are missing from `gcs_paths.json`, the script will log errors to `stderr` and return `Validation Status Passed: False`.

---

## 7. Rollback Procedure

Because this migration involves configuration and orchestration initialization rather than database state changes, the rollback procedure is straightforward:

1. **Revert Airflow DAGs:** Revert any migrated downstream Airflow DAGs to their previous versions or disable them.
2. **On-Premise Execution:** Keep the legacy on-premise cron/UC4 schedules active or re-enable them. The legacy `.dw_init`, `.dw_global`, `.dw_db`, and `.dw_ai` files remain intact on the on-premise filesystem and can be sourced by legacy jobs immediately.
3. **Configuration Cleanup:** If a complete cleanup is required on GCP, delete the configuration files from the Cloud Composer bucket:
   ```bash
   gsutil rm gs://<composer-bucket>/dags/config/env_ai_config.json
   gsutil rm gs://<composer-bucket>/dags/config/gcs_paths.json
   gsutil rm gs://<composer-bucket>/dags/utils/env_validator.py
   ```