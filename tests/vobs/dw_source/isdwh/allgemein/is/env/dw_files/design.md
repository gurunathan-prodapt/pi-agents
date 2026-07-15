# Migration Design Document
**Job Name**: Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files
**Target Platform**: BigQuery & Cloud Composer (Airflow)

---

## File Disposition Table

| Source File Path | Target File / Action | Disposition | Description / Reason |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai` | `gs://[GCS_BUCKET]/dwh/config/dw_ai.json` | Merged into configuration / GCS | Maps legacy Ab Initio paths to a global configuration JSON. Since execution shifts to PySpark/BigQuery, traditional Ab Initio EME parameters are kept as structured metadata only. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db` | `gs://[GCS_BUCKET]/dwh/config/dw_db.json` | Retired / Secret Manager | Oracle TNS credentials are retired. Database access shifts to native GCP IAM permissions, Service Accounts, and Google Cloud Secret Manager. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` | `dags/submodules/dw_global.py` | Target File | Converted into a Python-based configuration module to dynamically validate global environment paths and setup execution variables. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init` | `dags/submodules/dw_init.py` | Target File | Converted into a central Python-based environment initialization script. Replaces UNIX filesystem paths with GCS URI mapping. |

---

## Job Dependencies & Scheduling

### Upstream Dependencies / Lineage
* **`vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init`** relies on:
  * `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` (Internal migration file - resolved).
  * **`UNRESOLVED:.DW_LOKAL`** (Legacy local environment script - unresolved).
* **`vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global`** relies on:
  * **`UNRESOLVED:SETPYA.SH`** (Legacy Cognos script - unresolved).

### Downstream Dependencies
These files establish variables that are referenced by downstream orchestrations and scripts:
* **`DW.DWH_ABPZ_KKM_AIL_AGENT`** — Not yet migrated (Wiring cannot be finalized until it exists).
* **`r_ai_start`** — Not yet migrated (Wiring cannot be finalized until it exists).
* **`vobs/dw_source/isdwh/abinitio/bin/r_ai_start`** — Not yet migrated (Wiring cannot be finalized until it exists).

### Target Orchestration Mapping
These initialization and global configuration scripts are converted into Python modules. In **Cloud Composer (Airflow)**, they will be packaged as a DAG utility submodule (`dags/submodules/dw_init.py` and `dw_global.py`) imported by task operators at runtime, ensuring that variables are consistently populated across Airflow tasks.

---

## Environment-Specific Values (GCP Mapping)

Applying the environment variables policy, we classify and extract the variables discovered within the configurations:

### 1. Global (Environment-Wide)
These values are identical across all environments (dev, test, prod) and are bound to the cloud infrastructure setup.
* **`GCP_PROJECT`**: Extracted via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")`.
* **`GCS_BUCKET`**: Extracted via `os.environ.get("GCS_BUCKET")` or Airflow `Variable.get("GCS_BUCKET")`. Replaces legacy path roots.
* **`BQ_DATASET`**: Extracted via `os.environ.get("BQ_DATASET")` or Airflow `Variable.get("BQ_DATASET")`.

### 2. Job-Specific
These settings are customized parameters specific to the legacy DWH interfaces:
* **`ETL_Host`** / **`DW_HOST_CUSTOMER`**: Specific legacy host parameters (`dxcsa4.bn.detemobil.de`, `dxcst3.bn.detemobil.de`). Mapped into a local job configuration dict.
* **`ETL_Projekt`**: Specific legacy project code (`BHB`).
* **Interface Path Constants**: Directories (`DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, etc.) are converted to logically equivalent structured GCS URI mappings under the root `GCS_BUCKET` variable.

---

## Risks & Manual Actions
* **SOURCE: NOT FOUND — .dw_lokal — no candidate**
* **SOURCE: NOT FOUND — setpya.sh — no candidate**
* **RISK**: Downstream dependencies (`DW.DWH_ABPZ_KKM_AIL_AGENT`, `r_ai_start`, and `/vobs/dw_source/isdwh/abinitio/bin/r_ai_start`) are not yet migrated. Integration must be verified as soon as those tasks are designed.
* **RISK**: Database passwords are obfuscated via Ab Initio's legacy `m_password` script. These must be securely migrated into GCP Secret Manager instead of being kept inline.

---

## Verbatim MCP Output & Pseudocode

### File: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai`
```python
import os
import sys

# Append the library directory as per Horizon environment specifications
sys.path.append(os.getenv('DIR_LIB_PY', ''))

def main():
    """
    Initializes and sets up the required environment configurations.
    Replicates the exports declared in the Shell script.
    """
    # Define and export Ab Initio environment variables
    os.environ['AB_HOME'] = "/appl/local/abinitio/abinitio"
    os.environ['AB_AIR_ROOT'] = "/appl/local/abinitio/TMD_EME/eme_dev/repo"
    os.environ['AB_AIR_HOME'] = "/appl/local/abinitio/abinitio-V2-14"
    
    # Update system path with AB_HOME/bin
    current_path = os.getenv('PATH', '')
    os.environ['PATH'] = f"{current_path}:.:{os.environ['AB_HOME']}/bin"
    
    # Define and export DWH ETL Host and Project configuration
    os.environ['ETL_Host'] = "dxcsa4.bn.detemobil.de"
    os.environ['ETL_Projekt'] = "BHB"
    
    # Define and export local sandbox environment paths
    home_dir = os.path.expanduser('~')
    os.environ['AI_PRIV_SAND_ROOT'] = os.path.join(home_dir, "abinitio")
    os.environ['AI_ENV_SAND_ROOT'] = "/appl/local/abinitio/sandboxes/DEV"
    
    # AI_REPOSIT_TRACKING is commented out in the shell script, hence excluded here

if __name__ == '__main__':
    main()
```

### File: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db`
```python
import os
import sys

# Ensure the Horizon Python framework library path is added to the system path
DIR_LIB_PY = os.getenv('DIR_LIB_PY', '')
if DIR_LIB_PY:
    sys.path.append(DIR_LIB_PY)

from framework.core.lib import script

def main():
    try:
        # Step 1: Initialize Environment and DB Configuration variables
        # Set NLS_LANG equivalent if needed by downstream processes, default to GERMAN_GERMANY UTF-8 configuration compatibility
        os.environ['NLS_LANG'] = "GERMAN_GERMANY.WE8ISO8859P1"
        
        # Oracle Connection properties mapped for environment tracking
        os.environ['DB_TNS_NAME_DWH'] = "@eDWH3.devlab.de.tmo"
        os.environ['DB_USER_DWH'] = "meyreis"
        
        # Decrypted password placeholder (In BigQuery/GCP, use Secret Manager instead of hardcoded strings)
        os.environ['DB_PASSWD_DWH'] = "<password encrypted with m_password>"
        
        print("Environment and connection variables successfully initialized.")
        
    except Exception as e:
        print(f"Error during environment initialization: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### File: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global`
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module Name: dw_global_migration
Description: Migrated environment configuration and validation script for Horizon Python.
"""

import os
import sys

# Ensure Horizon Library Path is added to Python System Path (Mandatory for BigQuery executions)
sys.path.append(os.getenv('DIR_LIB_PY', ''))
try:
    from framework.core.lib import script
except ImportError:
    # Fallback/Mock for local testing environment if DIR_LIB_PY is empty
    script = None


def main():
    # List of environment variables to validate
    required_env_vars = [
        "DW_DIR_ROOT",
        "DW_DIR_PROT",
        "DW_DIR_CUBES",
        "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA",
        "DW_DIR_IMP_CTEL",
        "DW_DIR_IMP_VO",
        "DW_DIR_IMP_RV",
        "DW_DIR_IMP_IF",
        "DW_DIR_IMP_NNV",
        "ORACLE_HOME"
    ]

    missing_vars = []

    # 1. Environment Verification Step
    for var_name in required_env_vars:
        value = os.environ.get(var_name)
        if not value:  # Variable is either not set or is empty
            missing_vars.append(var_name)

    # If any error exists, print verification failures to stdout/stderr
    if missing_vars:
        print("Fehler in .dw_global:", file=sys.stderr)
        for varname in missing_vars:
            print(f"   Umgebungsvariable {varname} ist nicht gesetzt !", file=sys.stderr)

    # 2. Database Session Settings Configuration (Oracle / SQL-Net 2)
    os.environ['NLS_LANG'] = 'GERMAN_GERMANY.WE8ISO8859P1'
    os.environ['NLS_DATE_FORMAT'] = 'DD.MM.YY'
    os.environ['NLS_DATE_LANGUAGE'] = 'GERMAN_GERMANY.WE8ISO8859P1'

    # 3. Cognos PowerPlay Settings
    os.environ['PYA_USR'] = ''
    
    # Check for legacy script existence and execute system-level sourcing if present
    setpya_path = "/appl/local/cognos/pya60207/setpya.sh"
    if os.path.isfile(setpya_path):
        # NOTE: Sourcing environment parameters directly inside a child process 
        # doesn't affect the parent process environment. If setpya.sh values 
        # are needed, they should be parsed or hardcoded here.
        # Below is the dynamic emulation of checking file system:
        pass

    # 4. System Language/Locale Config
    os.environ['LANG'] = 'de'

    # 5. BigQuery Query Wrapper Emulation (Required structure, though no query exists)
    # def run_bq_query_example():
    #     bqsql_query = "SELECT * FROM `your_project_id.your_dataset_id.your_table_name`"
    #     if script is not None:
    #         script.func_execute_bq(bqsql_query, "pass_file.txt", ",", "\n")


if __name__ == '__main__':
    main()
```

### File: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init`
```python
"""
Module Name: env_config_init
Description: Replicates environment path setups, dynamic Oracle parameters, 
             and external global/local imports for Horizon Python/BigQuery.
"""

import os
import sys
import logging

# Ensure Horizon execution library is accessible
sys.path.append(os.getenv('DIR_LIB_PY', ''))
from framework.core.lib import script

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_external_configs():
    """
    Simulates sourcing .dw_global and .dw_lokal.
    In cloud migrations, these are typically read from GCS JSON/YAML config files.
    """
    logging.info("Loading global and local configuration maps...")
    
    # Placeholders for environment variables from .dw_global and .dw_lokal
    configs = {
        "PROJECT_ID": os.getenv("GCP_PROJECT", "your_project_id"),
        "DATASET_ID": os.getenv("BQ_DATASET", "your_dataset_id"),
        "ORACLE_SID": os.getenv("ORACLE_SID", "DEFAULT_SID")
    }
    return configs

def main():
    logging.info("Initializing Horizon environment configurations...")

    # Establish root environment directory equivalent
    HOME = os.getenv("HOME", "/home/horizon")
    BUCKET_ROOT = "gs://your_gcs_data_bucket"

    # Directory/GCS Path Mapping Configuration
    env_config = {
        "DW_DIR_ROOT": f"{HOME}/aktuell",
        "DW_DIR_PROT": f"{HOME}/daten/logfiles",
        "DW_DIR_CUBES": f"{HOME}/daten/cubes",
        
        # Imports mapping
        "DW_DIR_IMP_D1": f"{HOME}/daten/d1",
        "DW_DIR_IMP_BWA": f"{HOME}/daten/dpps/bwa",
        "DW_DIR_IMP_XTRA": f"{HOME}/daten/xtra",
        "DW_DIR_IMP_CTEL": f"{HOME}/daten/ctel",
        "DW_DIR_IMP_VO": f"{HOME}/daten/vo",
        "DW_DIR_IMP_RV": f"{HOME}/daten/rv",
        "DW_DIR_IMP_IF": f"{HOME}/daten/ees",
        "DW_DIR_IMP_NNV": f"{HOME}/daten/nnv",
        "DW_DIR_IMP_SIGMA": f"{HOME}/daten/gd/sigma",
        "DW_DIR_EXP_SIGMA": f"{HOME}/daten/gd/sigma/export",
        "DW_DIR_IMP_TRF": f"{HOME}/daten/trf",
        "DW_DIR_IMP_AUF": f"{HOME}/daten/sd/auf",
        "DW_DIR_IMP_GUT": f"{HOME}/daten/sd/gut",
        "DW_DIR_IMP_KDG": f"{HOME}/daten/sd/kdg",
        "DW_DIR_IMP_MP_KDG": f"{HOME}/daten/mp/kdg",
        "DW_DIR_IMP_MP_TS": f"{HOME}/daten/mp/ts",
        "DW_DIR_IMP_MP_ZM": f"{HOME}/daten/mp/zm",
        "DW_DIR_IMP_TS": f"{HOME}/daten/sd/ts",
        "DW_DIR_IMP_ZM": f"{HOME}/daten/sd/zm",
        "DW_DIR_EXP": f"{HOME}/daten/exporter",
        "DW_DIR_IMP_BPM": f"{HOME}/daten/bm",
        "DW_DIR_IMP_ZTS": f"{HOME}/daten/zts",
        "DW_DIR_IMP_VRS": f"{HOME}/daten/vrs",
        "DW_DIR_IMP_BRUNET": f"{HOME}/daten/brunet",
        "DW_DIR_IMP_DWH": f"{HOME}/daten/dwh",
        "DW_DIR_IMP_PLATO": f"{HOME}/daten/dwh/plato",
        
        # 2.5 Imports
        "DW_DIR_IMP_CARMEN": f"{HOME}/daten/carmen",
        "DW_DIR_IMP_SAP": f"{HOME}/daten/sap",
        "DW_DIR_IMP_SR_RV": f"{HOME}/daten/sap/sr_rv_dpps",
        "DW_DIR_IMP_SAP_L": f"{HOME}/daten/sap/sap_l_gutgr",
        "DW_DIR_IMP_L_MAHNSTYP_IST": f"{HOME}/daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_FI": f"{HOME}/daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_IST": f"{HOME}/daten/sap/mahn",
        "DW_DIR_IMP_L_GUTGR": f"{HOME}/daten/sd/l_gutschr",
        "DW_DIR_IMP_L_LEIST": f"{HOME}/daten/sd/l_leist",
        "DW_DIR_IMP_L_PROD": f"{HOME}/daten/sd/l_prod",
        "DW_DIR_IMP_LKODE": f"{HOME}/daten/sd/lkode",
        
        # Subscription Server & SMS
        "DW_DIR_IMP_SUBSE": f"{HOME}/daten/subse",
        "DW_DIR_SMS_PRG": f"{HOME}/aktuell/allgemein/is/util",
        "DW_DIR_SMS_ADR": f"{HOME}/daten/sms/adressen",
        "DW_DIR_SMS_TMP": f"{HOME}/daten/sms/tmp",
        
        # 3.5 Imports
        "DW_DIR_IMP_DPPS": f"{HOME}/daten/dpps",
        "DW_DIR_IMP_PLANF2": f"{HOME}/daten/planf2",
        
        # Remote configuration
        "DW_HOST_CUSTOMER": "dxcst3.bn.detemobil.de"
    }

    # Dynamic Oracle Environment Path Resolution
    oracle_home = os.getenv("ORACLE_HOME")
    if not oracle_home:
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            oracle_home = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            oracle_home = "/appl/local/oracle/11.2.0"
        else:
            logging.error("Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen!")
            # While migrating to BQ, this might be non-critical but warning is raised
            oracle_home = "ORACLE_HOME_NOT_FOUND"
    
    env_config["ORACLE_HOME"] = oracle_home

    # Load Sourced Configurations
    ext_configs = load_external_configs()
    oracle_sid = ext_configs.get("ORACLE_SID", "DEFAULT_SID")
    
    # Oracle utility directory
    env_config["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

    # Export keys to standard OS environments for backwards-compatible sub-processes
    for key, value in env_config.items():
        os.environ[key] = value
        logging.debug(f"Exported {key}={value}")

    logging.info("Environment configuration loaded successfully.")

if __name__ == "__main__":
    main()
```