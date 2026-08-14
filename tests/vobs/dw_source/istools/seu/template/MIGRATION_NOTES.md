# Migration Notes: Shared Files — vobs/dw_source/istools/seu/template

## 1. Summary
This migration covers the transition of the legacy KornShell (KSH) environment initialization and global configuration scripts (`.dw_global` and `.dw_init`) to a modern, Python 3-based environment. 

* **Source Artifacts:** 
  * `vobs/dw_source/istools/seu/template/.dw_global` (KSH environment configuration)
  * `vobs/dw_source/istools/seu/template/.dw_init` (KSH environment initialization)
* **Target Platform:** Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow) running Python 3.
* **Purpose:** These scripts establish the global runtime environment, validate required directory structures, configure Oracle database client paths, set localization (NLS) parameters, and apply process-level file creation masks (`umask`).

---

## 2. Generated Artifacts
The migration process generated two Python modules to replace the legacy shell scripts:

### 1. `dw_global.py`
* **Path:** `vobs/dw_source/istools/seu/template/dw_global.py`
* **Role:** 
  * Validates that critical environment variables (such as `DW_DIR_ROOT`, `DW_DIR_PROT`, and `ORACLE_HOME`) are defined.
  * Dynamically configures system search paths (`PATH` and `LD_LIBRARY_PATH`) for Oracle client libraries.
  * Exports Oracle NLS localization variables (`NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`) to ensure consistent date and character parsing.
  * Contains a placeholder for conditional Cognos PowerPlay environment sourcing.

### 2. `dw_init.py`
* **Path:** `vobs/dw_source/istools/seu/template/dw_init.py`
* **Role:**
  * Establishes and exports root, logging, and interface-specific directory paths (e.g., `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`).
  * Dynamically discovers and validates the `ORACLE_HOME` directory on the host filesystem if it is not already set.
  * Applies the process-level file-creation mask (`umask 022`) to restrict default write permissions on generated files.
  * Serves as the primary entry point for environment initialization.

---

## 3. Key Design Decisions

### Python-Native Environment Management
To align with a cloud-native architecture (such as Google Cloud Composer), the legacy shell scripts were converted to Python. This allows environment validation and path setup to be programmatically managed within Airflow DAGs or containerized execution tasks.

### Strict Fail-Fast Error Handling
* **Legacy Behavior:** The legacy KSH scripts printed `"Breche ab .."` (German for "aborting") upon validation failure but did not execute an `exit` command or raise an error code, allowing downstream scripts to run in an uninitialized or invalid state.
* **Python Behavior:** The migrated Python scripts raise an explicit `EnvironmentError` or call `sys.exit(1)` when validation fails. This ensures that downstream Airflow tasks fail immediately, preventing silent failures and data corruption.

### Correction of Legacy Typo
In the legacy `.dw_init` script, the variable `DW_DIR_IMP_MP_ZM` was assigned a path, but the script mistakenly re-exported `DW_DIR_IMP_MP_TS` instead of exporting the ZM path. The migrated `dw_init.py` corrects this typo by properly assigning and exporting `DW_DIR_IMP_MP_ZM` to prevent downstream directory resolution failures.

### In-Process Environment Limitations
Sourcing external shell scripts (such as Cognos's `setpya.sh`) directly inside a Python process does not natively modify Python's own `os.environ` dictionary. The Python implementation includes placeholders and warnings for these external scripts, indicating that these variables should be managed via Airflow Variables or a centralized JSON/YAML configuration file.

---

## 4. Manual Steps Before Go-Live

### 1. Environment Variable Configuration
The directory paths validated by these scripts must be mapped to their Google Cloud Storage (GCS) equivalents. Configure the following variables in your Cloud Composer environment or container runtime:
* `DW_DIR_ROOT` (e.g., `gs://<your-gcs-bucket>/dags/aktuell`)
* `DW_DIR_PROT` (e.g., `gs://<your-gcs-bucket>/daten/logfiles`)
* `DW_DIR_CUBES` (e.g., `gs://<your-gcs-bucket>/daten/cubes`)
* `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL` (and all other interface paths defined in `dw_init.py`)

### 2. IAM & Permissions
Ensure that the service account executing the Cloud Composer workers has:
* `roles/storage.objectAdmin` on the GCS bucket(s) representing the migrated directory structures.
* Appropriate permissions to write logs to Google Cloud Logging (replacing local file-based logging in `DW_DIR_PROT` where applicable).

### 3. Connection Strings & Secrets
If downstream jobs still require connectivity to legacy Oracle databases:
* Install the Oracle Instant Client in the Cloud Composer worker/Kubernetes pod image.
* Set `ORACLE_HOME` in the environment to point to the container's native driver path (e.g., `/opt/oracle/instantclient`).
* Store database credentials securely in GCP Secret Manager and reference them via Airflow Connections.

### 4. Scheduling & Integration
These scripts are utility modules and **must not** be scheduled as standalone DAGs. Instead:
* Import `dw_init` and `dw_global` at the beginning of downstream Python operators or DAG definitions to initialize the environment.
* Alternatively, pre-load these environment variables directly into the Cloud Composer environment configuration to bypass runtime initialization.

---

## 5. Known Gaps & Unresolved References

### 1. Missing `.dw_lokal` Profile
* **Gap:** The legacy `.dw_init` script sources `.dw_lokal` (`. $HOME/.dw_lokal`), which was not supplied in the source codebase.
* **Action Required:** A system administrator must locate the legacy `.dw_lokal` file on the on-premise system, identify any environment overrides it contains, and manually add them to the Cloud Composer environment variables.

### 2. Missing Cognos Configuration (`setpya.sh`)
* **Gap:** The legacy `.dw_global` script conditionally sources `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` for Cognos PowerPlay. This file was not supplied.
* **Action Required:** 
  * If Cognos has been retired and replaced by GCP-native BI tools (e.g., Looker), this dependency can be safely ignored.
  * If Cognos connectivity is still required, the environment variables set by `setpya.sh` must be manually extracted and configured in the target environment.

### 3. Hardcoded Remote Host
* **Gap:** The variable `DW_HOST_CUSTOMER` is hardcoded to `dxcst3.bn.detemobil.de`.
* **Action Required:** Verify if this SFTP transfer host is still active. If active, migrate the transfer logic to use Airflow's `SFTPToGCSOperator` with credentials stored securely in Airflow Connections.

---

## 6. Validation

To validate the migrated scripts, execute them in a test environment with Python 3:

### Test Case 1: Missing Environment Variables (Expected Failure)
1. Unset the required environment variables:
   ```bash
   unset DW_DIR_ROOT DW_DIR_PROT ORACLE_HOME
   ```
2. Run the global configuration script:
   ```bash
   python3 dw_global.py
   ```
3. **Expected Result:** The script must print the exact legacy German error messages to standard output and raise an `EnvironmentError`:
   ```text
   Fehler in .dw_global:
      Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !
      Umgebungsvariable DW_DIR_PROT ist nicht gesetzt !
      Umgebungsvariable ORACLE_HOME ist nicht gesetzt !
   Breche ab ..
   Traceback (most recent call last):
   ...
   EnvironmentError: Missing required environment variables: DW_DIR_ROOT, DW_DIR_PROT, ORACLE_HOME
   ```

### Test Case 2: Successful Initialization
1. Set all required environment variables to mock paths:
   ```bash
   export HOME="/tmp"
   export ORACLE_HOME="/tmp"
   export DW_DIR_ROOT="/tmp/aktuell"
   export DW_DIR_PROT="/tmp/daten/logfiles"
   export DW_DIR_CUBES="/tmp/daten/cubes"
   export DW_DIR_IMP_D1="/tmp/daten/d1"
   export DW_DIR_IMP_XTRA="/tmp/daten/xtra"
   export DW_DIR_IMP_CTEL="/tmp/daten/ctel"
   ```
2. Run the initialization and global scripts:
   ```bash
   python3 dw_init.py
   python3 dw_global.py
   ```
3. **Expected Result:** Both scripts must exit with code `0` without printing any error messages. Verify that the process `umask` has been successfully set to `022`.

---

## 7. Rollback Procedure

In the event of an issue during deployment, perform the following steps to roll back:

1. **Revert Downstream DAGs:** Revert any migrated Airflow DAGs or Python tasks to point back to the legacy shell-based execution wrappers.
2. **Restore Legacy Environment:** Ensure that the legacy on-premise VM or server hosting the original `.dw_init` and `.dw_global` scripts is active and that its local environment variables are intact.
3. **Verify Legacy Paths:** Confirm that the legacy directory paths (e.g., `/vobs/dw_source`, `/appl/local/oracle/...`) and permissions are restored to their pre-migration state.