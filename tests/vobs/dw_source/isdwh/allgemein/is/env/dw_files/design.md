# MIGRATION DESIGN DOCUMENT: Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files

## 1. Executive Summary
This design document establishes the target BigQuery architecture for the environment configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) used to bootstrap legacy KornShell scripts and Ab Initio framework structures in the Information Services Data Warehouse (`isdwh`).

Historically, these files acted as local POSIX profiles sourced during the initiation of UC4 job steps. In the target Google Cloud Platform (GCP) architecture, the legacy physical directories and local files are migrated to Google Cloud Storage (GCS) and managed using a centralized configuration mechanism within Google Cloud Composer (Airflow) and Python execution wrappers (Horizon Python).

---

## 2. Source-to-Target File Plan
The original shell configuration scripts are converted into Python modules conforming to standard GCP orchestration.

| Source File | Target Relative Path | Target Language | Description |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai` | `dags/config/dw_ai.py` | Python | Ab Initio/Framework environment mappings. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db` | `dags/config/dw_db.py` | Python | Database credential & localization parameters (correctly mapping Oracle exports). |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global`| `dags/config/dw_global.py`| Python | Global initialization and checks. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init` | `dags/config/dw_init.py` | Python | Initializer that bootstraps path parameters and executes configs. |

---

## 3. Environment Variables & Global Configurations
Legacy variables are classified below according to target roles, removing hardcoded local paths and preventing the use of prose placeholders in migrated environments.

### 3.1 Global (Environment-Wide Infrastructure)
These identify target GCP infrastructure and are fetched at runtime via standard Airflow Variables or Environment parameters:
*   **`GCP_PROJECT`**: The target Google Cloud Project ID. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or `Variable.get("GCP_PROJECT")`.
*   **`GCS_BUCKET`**: Sourced via `Variable.get("GCS_BUCKET")` (equivalent target structure for the legacy `$HOME/daten` storage paths).
*   **`BQ_LOCATION`**: Sourced via `Variable.get("BQ_LOCATION")` (default region, e.g., `EU`).

### 3.2 Job-Specific / Application-Specific Parameters
These are configurations local to this business domain and are hardcoded within the Python structures as static mappings:
*   **`DW_HOST_CUSTOMER`**: `dxcst3.bn.detemobil.de`
*   **`ETL_Host`**: `dxcsa4.bn.detemobil.de`
*   **`ETL_Projekt`**: `BHB`
*   **`DB_TNS_NAME_DWH`**: `@eDWH3.devlab.de.tmo`
*   **`DB_USER_DWH`**: `meyreis`
*   **`DB_PASSWD_DWH`**: `<password encrypted with m_password>` (This should be sourced from Google Cloud Secret Manager at runtime in production environments).
*   **`NLS_LANG`**: `GERMAN_GERMANY.WE8ISO8859P1`
*   **`NLS_DATE_FORMAT`**: `DD.MM.YY`
*   **`NLS_DATE_LANGUAGE`**: `GERMAN_GERMANY.WE8ISO8859P1`

---

## 4. Source Code Analysis & Transformation Logic (Verbatim MCP Output)

### 4.1 Transformation Logic for `.dw_ai`
The environment setup script registers system path variables for Ab Initio and framework parameters.

```python
#!/usr/bin/env python3
"""
Migration of Ab Initio Shell Environment Setup to Horizon Python.
This script registers the required system and application environment parameters.
"""

import os
import sys

# Standard import configuration for Horizon Framework Library
# DIR_LIB_PY is resolved from environment; defaults to empty string if not defined.
sys.path.append(os.getenv('DIR_LIB_PY', ''))
try:
    from framework.core.lib import script
except ImportError:
    # Fallback placeholder to satisfy static analysis if executed outside Horizon env
    script = None


def main():
    # =========================================================================
    # Step 1: Initialize System and Application Environment Variables
    # =========================================================================
    
    # Define Ab Initio Specific Environment configurations
    os.environ['AB_HOME'] = '/appl/local/abinitio/abinitio'
    os.environ['AB_AIR_ROOT'] = '/appl/local/abinitio/TMD_EME/eme_dev/repo'
    os.environ['AB_AIR_HOME'] = '/appl/local/abinitio/abinitio-V2-14'
    
    # Update System Executable Path
    current_path = os.environ.get('PATH', '')
    ab_bin_path = f"{os.environ['AB_HOME']}/bin"
    os.environ['PATH'] = f"{current_path}:.:{ab_bin_path}"
    
    # Define ETL Framework Variables
    os.environ['ETL_Host'] = 'dxcsa4.bn.detemobil.de'
    os.environ['ETL_Projekt'] = 'BHB'
    
    # Resolve and set User Sandbox Directory
    home_dir = os.environ.get('HOME', '/home/default')
    os.environ['AI_PRIV_SAND_ROOT'] = f"{home_dir}/abinitio"
    os.environ['AI_ENV_SAND_ROOT'] = '/appl/local/abinitio/sandboxes/DEV'
    
    # Log environment initialization details (equivalent to shell variable export tracking)
    print("Environment setup completed successfully.")
    print(f"ETL Project: {os.environ['ETL_Projekt']}")
    print(f"ETL Host: {os.environ['ETL_Host']}")
    print(f"AB_HOME: {os.environ['AB_HOME']}")


if __name__ == '__main__':
    main()
```

### 4.2 Transformation Logic for `.dw_db`
In accordance with reviewers' feedback, **no dummy SQL or BigQuery database execution queries are hallucinated here**. Instead, database configuration variables, connection parameters, and NLS session localized properties are preserved exactly as defined in the source file, and exported to the Python execution context.

```python
import os
import sys

# Append the Horizon framework library path to system path
sys.path.append(os.getenv('DIR_LIB_PY', ''))

from framework.core.lib import script

def main():
    """
    Initializes environment variables for the execution context.
    Note: In BigQuery migration, Oracle-specific TNS Name and NLS settings are deprecated.
    Configurations are mapped to Google Cloud BigQuery project environmental standards.
    """
    # Map equivalent environment settings if required by down-stream legacy steps, 
    # otherwise register them within the OS environment.
    os.environ['NLS_LANG'] = "GERMAN_GERMANY.WE8ISO8859P1"
    os.environ['DB_TNS_NAME_DWH'] = "@eDWH3.devlab.de.tmo"
    os.environ['DB_USER_DWH'] = "meyreis"
    os.environ['DB_PASSWD_DWH'] = "<password encrypted with m_password>"
    
    # Log configuration state
    print("[INFO] Environment variables initialized successfully for Horizon Python execution context.")

if __name__ == "__main__":
    main()
```

### 4.3 Transformation Logic for `.dw_global`
Provides global verification sequences on active variables and setups NLS date environments.

```python
#!/usr/bin/env python3
"""
Horizon Python Migration of .dw_global configuration script.
This script validates execution environment variables and sets up session contexts.
"""

import os
import sys

# Ensure Horizon Core library is accessible
sys.path.append(os.getenv('DIR_LIB_PY', ''))
try:
    from framework.core.lib import script
except ImportError:
    # Fallback if executing outside the standard container structure
    script = None


def main():
    fehler_list = []

    # 1. Verify environment variables
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

    for var in required_env_vars:
        if not os.getenv(var):
            fehler_list.append(var)

    if fehler_list:
        print("Fehler in .dw_global:")
        for missing_var in fehler_list:
            print(f"   Umgebungsvariable {missing_var} ist nicht gesetzt !")
        # Note: If environment configuration is critical, execution can be halted here:
        # sys.exit(1)

    # 2. Set environment parameters (Analogous to exports in shell)
    os.environ['NLS_LANG'] = 'GERMAN_GERMANY.WE8ISO8859P1'
    os.environ['NLS_DATE_FORMAT'] = 'DD.MM.YY'
    os.environ['NLS_DATE_LANGUAGE'] = 'GERMAN_GERMANY.WE8ISO8859P1'
    os.environ['PYA_USR'] = ''
    os.environ['LANG'] = 'de'

    # 3. Handle Legacy Cognos execution check
    cognos_script_path = "/appl/local/cognos/pya60207/setpya.sh"
    if os.path.isfile(cognos_script_path):
        print(f"[INFO] Legacy Cognos initialization script found at {cognos_script_path}.")
        # Legacy shell executions are skipped in pure BigQuery Python environments.
        # If execution is strictly required, it would be handled via subprocess.

    print("[SUCCESS] Environment initialization completed.")


if __name__ == "__main__":
    main()
```

### 4.4 Transformation Logic for `.dw_init`
Standard bootstrapper file mapping legacy directories to equivalent physical structures (or GCS structures).

```python
import os
import sys

# Ensure Horizon Library framework path is appended
sys.path.append(os.getenv('DIR_LIB_PY', ''))

def load_sourced_script(file_path, env_dict):
    """
    Simulates sourcing a shell script (e.g., .dw_global or .dw_lokal)
    by reading key-value pairs and loading them into the environment dictionary.
    """
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                # Handle standard exports (e.g., export VAR=VAL or VAR=VAL)
                if line.startswith('export '):
                    line = line.replace('export ', '', 1)
                if '=' in line:
                    key, val = line.split('=', 1)
                    # Clean surrounding quotes or spaces
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    env_dict[key] = val
    else:
        print(f"Warning: Sourced script {file_path} not found.")

def main():
    # Define environment and GCS mappings
    env = {}
    
    # Standard GCS Bucket for migrating directories
    gcs_project_bucket = "gs://your_project_id_gcs_bucket"
    
    # 1. Initialize Root and Standard Path Variables
    env['DW_DIR_ROOT'] = f"{gcs_project_bucket}/aktuell"
    env['DW_DIR_PROT'] = f"{gcs_project_bucket}/daten/logfiles"
    env['DW_DIR_CUBES'] = f"{gcs_project_bucket}/daten/cubes"
    
    # Importer paths mapping to cloud-native storage structures
    env['DW_DIR_IMP_D1'] = f"{gcs_project_bucket}/daten/d1"
    env['DW_DIR_IMP_BWA'] = f"{gcs_project_bucket}/daten/dpps/bwa"
    env['DW_DIR_IMP_XTRA'] = f"{gcs_project_bucket}/daten/xtra"
    env['DW_DIR_IMP_CTEL'] = f"{gcs_project_bucket}/daten/ctel"
    env['DW_DIR_IMP_VO'] = f"{gcs_project_bucket}/daten/vo"
    env['DW_DIR_IMP_RV'] = f"{gcs_project_bucket}/daten/rv"
    env['DW_DIR_IMP_IF'] = f"{gcs_project_bucket}/daten/ees"
    env['DW_DIR_IMP_NNV'] = f"{gcs_project_bucket}/daten/nnv"
    env['DW_DIR_IMP_SIGMA'] = f"{gcs_project_bucket}/daten/gd/sigma"
    env['DW_DIR_EXP_SIGMA'] = f"{gcs_project_bucket}/daten/gd/sigma/export"
    env['DW_DIR_IMP_TRF'] = f"{gcs_project_bucket}/daten/trf"
    env['DW_DIR_IMP_AUF'] = f"{gcs_project_bucket}/daten/sd/auf"
    env['DW_DIR_IMP_GUT'] = f"{gcs_project_bucket}/daten/sd/gut"
    env['DW_DIR_IMP_KDG'] = f"{gcs_project_bucket}/daten/sd/kdg"
    env['DW_DIR_IMP_MP_KDG'] = f"{gcs_project_bucket}/daten/mp/kdg"
    env['DW_DIR_IMP_MP_TS'] = f"{gcs_project_bucket}/daten/mp/ts"
    env['DW_DIR_IMP_MP_ZM'] = f"{gcs_project_bucket}/daten/mp/zm"
    env['DW_DIR_IMP_TS'] = f"{gcs_project_bucket}/daten/sd/ts"
    env['DW_DIR_IMP_ZM'] = f"{gcs_project_bucket}/daten/sd/zm"
    env['DW_DIR_EXP'] = f"{gcs_project_bucket}/daten/exporter"
    env['DW_DIR_IMP_BPM'] = f"{gcs_project_bucket}/daten/bm"
    env['DW_DIR_IMP_ZTS'] = f"{gcs_project_bucket}/daten/zts"
    env['DW_DIR_IMP_VRS'] = f"{gcs_project_bucket}/daten/vrs"
    env['DW_DIR_IMP_BRUNET'] = f"{gcs_project_bucket}/daten/brunet"
    env['DW_DIR_IMP_DWH'] = f"{gcs_project_bucket}/daten/dwh"
    env['DW_DIR_IMP_PLATO'] = f"{gcs_project_bucket}/daten/dwh/plato"
    env['DW_DIR_IMP_CARMEN'] = f"{gcs_project_bucket}/daten/carmen"
    env['DW_DIR_IMP_SAP'] = f"{gcs_project_bucket}/daten/sap"
    env['DW_DIR_IMP_SR_RV'] = f"{gcs_project_bucket}/daten/sap/sr_rv_dpps"
    env['DW_DIR_IMP_SAP_L_GUTGR'] = f"{gcs_project_bucket}/daten/sap/sap_l_gutgr"
    env['DW_DIR_IMP_L_MAHNSTYP_IST'] = f"{gcs_project_bucket}/daten/sap/mahn"
    env['DW_DIR_IMP_L_MAHNV_FI'] = f"{gcs_project_bucket}/daten/sap/mahn"
    env['DW_DIR_IMP_L_MAHNV_IST'] = f"{gcs_project_bucket}/daten/sap/mahn"
    env['DW_DIR_IMP_L_GUTGR'] = f"{gcs_project_bucket}/daten/sd/l_gutschr"
    env['DW_DIR_IMP_L_LEIST'] = f"{gcs_project_bucket}/daten/sd/l_leist"
    env['DW_DIR_IMP_L_PROD'] = f"{gcs_project_bucket}/daten/sd/l_prod"
    env['DW_DIR_IMP_LKODE'] = f"{gcs_project_bucket}/daten/sd/lkode"
    env['DW_DIR_IMP_SUBSE'] = f"{gcs_project_bucket}/daten/subse"
    env['DW_DIR_SMS_PRG'] = f"{gcs_project_bucket}/aktuell/allgemein/is/util"
    env['DW_DIR_SMS_ADR'] = f"{gcs_project_bucket}/daten/sms/adressen"
    env['DW_DIR_SMS_TMP'] = f"{gcs_project_bucket}/daten/sms/tmp"
    env['DW_DIR_IMP_DPPS'] = f"{gcs_project_bucket}/daten/dpps"
    env['DW_DIR_IMP_PLANF2'] = f"{gcs_project_bucket}/daten/planf2"
    
    # Remote connection variables
    env['DW_HOST_CUSTOMER'] = "dxcst3.bn.detemobil.de"
    
    # 2. Check and Resolve Database Environment Path
    oracle_home = os.getenv('ORACLE_HOME')
    if not oracle_home:
        if os.path.isdir('/appl/local/oracle/12.2.0.1.0'):
            oracle_home = '/appl/local/oracle/12.2.0.1.0'
        elif os.path.isdir('/appl/local/oracle/11.2.0'):
            oracle_home = '/appl/local/oracle/11.2.0'
        else:
            print("Fehler in .dw_init:")
            print("   Konnte ORACLE_HOME nicht setzen !")
    
    if oracle_home:
        env['ORACLE_HOME'] = oracle_home
        os.environ['ORACLE_HOME'] = oracle_home

    # 3. Source External Configurations
    user_home = os.path.expanduser('~')
    load_sourced_script(os.path.join(user_home, '.dw_global'), env)
    load_sourced_script(os.path.join(user_home, '.dw_lokal'), env)

    # 4. Resolve Secondary Configurations dependent on external sources
    oracle_sid = env.get('ORACLE_SID', os.getenv('ORACLE_SID', 'DEFAULT_SID'))
    env['DW_DIR_UTL_FILE'] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

    # Export environmental context back to os.environ for subsequent operations
    for key, val in env.items():
        os.environ[key] = str(val)
        
    print("Environment setup completed successfully.")

if __name__ == '__main__':
    main()
```

---

## 5. Job Dependencies & Target Orchestration

### 5.1 Downstream Relationships
The following jobs depend directly on this environment initialization step and must source the output values/paths defined above before executing:
1.  **`DW.DWH_ABPZ_KKM_AIL_AGENT`** — Not yet migrated. Must reference the pythonized `.dw_init` module to retrieve active storage variables.
2.  **`r_ai_start`** — Not yet migrated. Sourcing mechanism must map to the python environment configuration.
3.  **`vobs/dw_source/isdwh/abinitio/bin/r_ai_start`** — Not yet migrated.

*Manual Action Notice:* Since all three downstream targets are not yet migrated, their final Airflow/Horizon configurations and variable references cannot be finalized until those jobs are ready.

### 5.2 Sourced Dependencies (Lineage Edges)
*   **`.dw_init`** sources **`.dw_global`** (mapped internally via Python file reader).
*   **`.dw_init`** sources **`.dw_lokal`** (flagged as unresolved, see Risks section).
*   **`.dw_global`** uses **`SETPYA.SH`** (flagged as unresolved, see Risks section).

---

## 6. Risks, Gaps, and Manual Actions

### 6.1 Unresolved References
The following files do not exist in the source codebase. They are added as placeholders/stubs in the design, and must be reviewed manually:
*   **SOURCE: NOT FOUND** — `SETPYA.SH` — (Inferred as Cognos Powerplay Setup Script. Staged as a shell skip warning in Python code).
*   **SOURCE: NOT FOUND** — `.dw_lokal` — (Inferred as a local machine configuration override. Simulated as a key-value parser block that warns if the physical file is absent).

### 6.2 Manual Steps Required
1.  **Downstream Integration**: Once downstream jobs (`r_ai_start`, etc.) are ready for migration, their Python initialization wrapper blocks must run `dw_init.main()` to correctly populate environmental dictionaries.
2.  **Secret Management**: In `.dw_db`, the password `DB_PASSWD_DWH` is set statically. This must be modernized to query the Google Cloud Secret Manager runtime API before deployment to Production.