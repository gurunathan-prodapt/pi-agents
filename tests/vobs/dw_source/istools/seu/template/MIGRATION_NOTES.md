# Migration Notes: Shared Files — `vobs/dw_source/istools/seu/template`

This document details the migration of the legacy environment initialization and configuration scripts from the on-premise data warehouse environment to Google Cloud Platform (GCP).

---

## 1. Summary

The environment initialization scripts `.dw_global` and `.dw_init` have been migrated from legacy Korn Shell (KSH) to Python 3. 

*   **Source Components:** 
    *   `vobs/dw_source/istools/seu/template/.dw_global` (KSH environment setup script)
    *   `vobs/dw_source/istools/seu/template/.dw_init` (KSH environment initialization script)
*   **Target Platform:** Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow).
*   **Migration Strategy:** The scripts have been converted into native Python modules (`dw_global.py` and `dw_init.py`). They will be deployed to a shared utility directory (e.g., `plugins/` or `dags/utils/`) within Cloud Composer, allowing downstream Python-based ETL tasks to import and execute them to establish runtime environments.

---

## 2. Generated Artifacts

The migration process generated the following Python modules:

1.  **`vobs/dw_source/istools/seu/template/dw_global.py`**
    *   **Role:** Validates the presence of required global environment variables. It configures system search paths (`PATH`, `LD_LIBRARY_PATH`) and sets Oracle database session localization parameters (`NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`). It also contains conditional logic to handle legacy Cognos BI environment sourcing.
2.  **`vobs/dw_source/istools/seu/template/dw_init.py`**
    *   **Role:** Bootstraps the "Information Services" environment. It defines standardized directory paths for logs, OLAP cubes, and data feeds relative to the user's home directory. It dynamically searches the filesystem to locate a valid `ORACLE_HOME`, executes `dw_global.py` to set global variables, and enforces standard file creation permissions (`umask 022`).

---

## 3. Key Design Decisions

*   **Native Python Conversion:** Converting KSH scripts to Python modules eliminates the need for legacy shell wrappers in Cloud Composer. This allows for cleaner error handling, native logging, and seamless integration with Airflow DAGs.
*   **Correction of Legacy Copy-Paste Bug:** In the legacy `.dw_init` script, a copy-paste error existed where `DW_DIR_IMP_MP_ZM` was assigned a path, but `DW_DIR_IMP_MP_TS` was exported twice instead:
    ```bash
    DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm; export DW_DIR_IMP_MP_TS
    ```
    This left `DW_DIR_IMP_MP_ZM` unexported. The migrated `dw_init.py` corrects this by properly exporting `DW_DIR_IMP_MP_ZM` to the environment.
*   **Parameterization of Placeholders:** The legacy script contained a hardcoded placeholder `<login>` for the `DW_DIR_CUSTOMER` variable. In `dw_init.py`, this has been parameterized to read from the environment (`os.environ.get("DW_DIR_CUSTOMER", "")`) to prevent hardcoded values and support secure runtime injection.
*   **Preservation of Legacy Oracle/Cognos Logic:** Although the target architecture utilizes Google BigQuery (which does not require Oracle client paths or NLS session settings), these variables have been preserved in the Python code to maintain backward compatibility for any hybrid-phase jobs still querying legacy databases.

---

## 4. Manual Steps Before Go-Live

Before deploying these scripts to production, the following configuration steps must be completed:

1.  **Environment Variable Configuration:**
    Ensure the following environment variables are configured in the Cloud Composer environment (or the target execution container):
    *   `HOME`: Must point to the execution user's home directory or a persistent mount point.
    *   `DW_DIR_CUSTOMER`: Set this to the appropriate customer login identifier (replacing the legacy `<login>` placeholder).
2.  **Storage Bucket Mapping:**
    The legacy directory paths (e.g., `$HOME/daten/logfiles`, `$HOME/daten/d1`) must be mapped to Google Cloud Storage (GCS) buckets or local directory mounts (e.g., using `gcsfuse` on GKE/Cloud Run) if downstream jobs expect a local filesystem interface.
3.  **IAM & Permissions:**
    The service account executing the Cloud Composer DAGs must have:
    *   `roles/storage.objectAdmin` on the GCS buckets representing the data and log directories.
    *   Access to Secret Manager if database credentials or customer logins are retrieved dynamically.
4.  **Deployment Location:**
    Copy `dw_global.py` and `dw_init.py` to the Airflow `plugins/` or `dags/utils/` directory so they can be imported by other Python scripts. **Do not schedule these files as standalone DAGs.**

---

## 5. Known Gaps & Unresolved References

*   **Missing `.dw_lokal` File:** The legacy `.dw_init` script attempted to source `.dw_lokal` (`. $HOME/.dw_lokal`). This file was not supplied in the source codebase. If local environment overrides are required, a stub or configuration parser must be created.
*   **Missing Cognos Setup Script (`setpya.sh`):** The legacy `.dw_global` script conditionally sourced `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`. This file was not supplied. If Cognos BI integration is still active, its environment variables must be manually extracted and configured in the target environment.
*   **Unmigrated Downstream Jobs:** There are 15 downstream jobs that depend on these initialization scripts:
    *   `DW.BERT_ABLAUFSTEUERUNG`
    *   `DW.BERT_AUSD_BP_TA_MSISDN`
    *   `DW.BERT_AUSD_BP_TA_P_BASISPROD`
    *   `DW.BERT_AUSD_V_TA_PERIOD`
    *   `DW.BERT_AUSD_V_TA_P_VERTRAG`
    *   `DW.BERT_AUSD_V_TA_VERTRAG_TMP`
    *   `DW.BERT_DROP_TEMP_TABLE`
    *   `DW.BERT_P_ADRESSEN`
    *   `DW.BERT_P_AUSTAUSCH`
    *   `DW.BERT_P_GESCHAEFTSP`
    *   `DW.BERT_P_RECH_EMPF`
    *   `DW.BERT_RECHNUNGSDATEN`
    *   `DW.CRS_VERFUEGBAR_JA_NEIN_PF_JOB_FUER_BERT`
    *   `DW.DWH_EXIS_SD_APT_BESTANDS`
    *   `DW.DWH_EXIS_SD_APT_RABATT`
    
    Full integration testing cannot be completed until these downstream jobs are migrated and wired to import `dw_init.py`.
*   **Redesign (B4) Recommendation:** Once the migration to BigQuery is fully complete and legacy Oracle/Cognos systems are retired, a refactoring phase should be scheduled to prune all Oracle client path checks, NLS session variables, and Cognos sourcing logic from these scripts.

---

## 6. Validation

To validate the migrated scripts, execute the following tests in the target environment:

### Unit Test Execution
Create a test script (e.g., `test_env.py`) to verify that the environment variables are correctly initialized and that validation errors are raised when required variables are missing.

```python
# test_env.py
import os
import pytest
import dw_init

def test_environment_initialization(monkeypatch):
    # Set up mock environment
    monkeypatch.setenv("HOME", "/tmp")
    monkeypatch.setenv("ORACLE_HOME", "/tmp/oracle")
    
    # Run initialization
    dw_init.init_env()
    
    # Assert variables are set correctly
    assert os.environ["DW_DIR_ROOT"] == "/tmp/aktuell"
    assert os.environ["DW_DIR_PROT"] == "/tmp/daten/logfiles"
    assert os.environ["DW_DIR_IMP_MP_ZM"] == "/tmp/daten/mp/zm"  # Verifying the bug fix
    assert os.environ["NLS_LANG"] == "GERMAN_GERMANY.WE8ISO8859P1"

def test_validation_failure(monkeypatch):
    # Clear required variables to trigger failure
    monkeypatch.delenv("ORACLE_HOME", raising=False)
    
    with pytest.raises(SystemExit):
        dw_init.init_env()
```

### What "Passing" Means
*   Running `python3 dw_init.py` with all required directories and `ORACLE_HOME` present completes with exit code `0`.
*   All 20+ environment variables defined in `dw_init.py` are successfully populated in `os.environ`.
*   The process `umask` is successfully set to `022` (octal).
*   Missing required variables result in a clean exit with code `1` and descriptive German error messages printed to `stderr` (matching legacy behavior).

---

## 7. Rollback Procedure

In the event of an issue during deployment, follow these steps to roll back to the legacy state:

1.  **Revert Airflow DAGs:** Revert any modified Airflow DAGs to use the legacy `BashOperator` pointing to the original KSH scripts.
2.  **Restore Legacy Environment:** Ensure the legacy on-premise execution nodes and NFS mount paths are active and accessible.
3.  **Point Storage Back to On-Premise:** If directory paths were redirected to GCS, revert the environment variables (`DW_DIR_ROOT`, `DW_DIR_PROT`, etc.) to point back to the legacy local filesystem paths.
4.  **Verify Permissions:** Ensure the legacy `umask 022` is active on the rollback environment to prevent file permission issues.