# DATA MIGRATION DESIGN DOCUMENT
**Target Platform:** Google BigQuery / Cloud Composer (Airflow)  
**Job Name:** Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Migration Bucket:** Environment Orchestration & Variable Injection Configuration (UC4_ONLY Pattern)  

---

## 1. Executive Summary & Prescribed Migration Pattern

### 1.1 Deployed Architecture
*   **Legacy Framework:** Legacy KornShell (`ksh_dwh`) configuration setup running on-premises. This suite establishes structural path definitions, Ab Initio ETL metadata framework parameters, and database connection strings.
*   **Target Cloud Architecture:** Google Cloud Composer (Airflow) / BigQuery Environment. 
*   **Migration Pattern:** `UC4_ONLY` (High Confidence). Since these files are configuration files that establish shared variables across downstream KSH, SQL, and Ab Initio graph wrapper execution blocks, they are consolidated into **one centralized Composer Variable Injection workflow** and structured Python modules utilizing the Horizon Python runtime framework.

---

## 2. Shared Environment & Variable Classification

These scripts use various names to store metadata. The target platform maps these properties based on their role:

### 2.1 GLOBAL (Environment-Wide Variables)
These variables are common across all runs in this GCP environment. They map directly to infra endpoints or system properties:

*   **`GCP_PROJECT`**: The target BigQuery Project ID (injected at runtime via Composer).
*   **`GCS_BUCKET`**: Replaces the local directory root `/vobs/` and `$HOME/daten/` file systems with GCS bucket landing areas.
*   **`DB_TNS_NAME_DWH`** / **`DB_USER_DWH`** / **`DB_PASSWD_DWH`**: Extracted out of the raw script text and securely mapped to **GCP Secret Manager** or configured in **Airflow Connections** as `oracle_dwh_conn`. 
*   **`LANG` / `NLS_LANG`**: Character encoding configurations are handled at the Airflow operator environment level.

### 2.2 JOB-SPECIFIC VARIABLES
These are specific variables mapped into Python dictionaries/Airflow params for this script configuration:
*   `ETL_Host` = `"dxcsa4.bn.detemobil.de"`
*   `ETL_Projekt` = `"BHB"`
*   `DW_HOST_CUSTOMER` = `"dxcst3.bn.detemobil.de"`

---

## 3. Orchestration & Job Dependencies

### 3.1 Job Dependencies (Airflow Cross-DAG Sensor Wiring)
As specified in the pre-collected context metadata:
*   **Upstream:** None discovered (This represents an initialization script).
*   **Downstream (Consumers of these shared files):**
    *   `DW.DWH_ABPZ_KKM_AIL_AGENT` — *not yet migrated*
    *   `r_ai_start` — *not yet migrated*
    *   `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` — *not yet migrated*

Under the target Cloud Composer architecture, these environment configurations must be imported, executed, or sensed prior to executing the downstream DAGs listed above.

> **Risks & Manual Actions:**
> *   **DEPENDENCY NOT FOUND:** The downstream job `DW.DWH_ABPZ_KKM_AIL_AGENT` is not yet migrated. Downstream wiring cannot be finalized until it exists.
> *   **DEPENDENCY NOT FOUND:** The downstream script/job `r_ai_start` is not yet migrated. Downstream wiring cannot be finalized until it exists.
> *   **DEPENDENCY NOT FOUND:** The downstream script/job `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` is not yet migrated. Downstream wiring cannot be finalized until it exists.

---

## 4. Risks, Gaps & Manual Actions

*   **SOURCE: NOT FOUND — .DW_LOKAL — no candidate**
    *   *Action:* The file `.dw_init` attempts to source `$HOME/.dw_lokal`. No source file was found in the scan for `.dw_lokal`. A stub is provided in the Python execution logic.
*   **SOURCE: NOT FOUND — SETPYA.SH — no candidate**
    *   *Action:* The file `.dw_global` attempts to source `/appl/local/cognos/pya60207/setpya.sh`. This file is unresolved on the filesystem. A stub is configured in the Python translation.
*   **On-Premises Local Paths:** Local UNIX home directories (`$HOME/daten/...`) must be mapped to Cloud Storage buckets (e.g., `gs://<your-gcs-bucket>/daten/...`) for cloud processing.

---

## 5. Target File Plan

| Target File Relative Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `dwh_env_config/dw_ai.py` | Python (Horizon) | `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai` | Sets Ab Initio framework metadata. |
| `dwh_env_config/dw_db.py` | Python (Horizon) | `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db` | Handles Oracle credentials configuration. |
| `dwh_env_config/dw_global.py` | Python (Horizon) | `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` | Global environment validations. |
| `dwh_env_config/dw_init.py` | Python (Horizon) | `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init` | Primary environment initialization entrypoint. |

---

## 6. Detailed Migration Content (Verbatim MCP Output)

The following sections contain the verbatim generated design documents and matching Python transformation logic for each script:

### 6.1 FILE: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai`

=== Result for vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai ===
Based on the analysis of the provided Shell script, here is the Design Document and Low-Level Pseudocode for migrating the logic to the Horizon Python environment running in BigQuery.

---

### Step 1: Understand the Shell Script Code

#### 1.1 Identify Sources
No data sources (files, databases, or tables) are declared or referenced in this script.

#### 1.2 Processing Flow
The script acts purely as an environment configuration/initialization script. It defines and exports environment variables for an Ab Initio ETL framework (such as path configurations, Ab Initio home directories, host info, and sandbox root paths). There are no processing loops, transformations, or data flow operations.

#### 1.3 Identify Targets
No target tables, directories, or files are specified in the script.

#### 1.4 External Dependencies
The script utilizes standard Korn Shell (`#!/bin/ksh_dwh`) environment export commands. It references Ab Initio system paths `/appl/local/abinitio/...` which are typical for legacy on-premise setups.

#### 1.5 Parameters and Variables
The following environment variables are declared and exported:
*   `AB_HOME` = `/appl/local/abinitio/abinitio`
*   `AB_AIR_ROOT` = `/appl/local/abinitio/TMD_EME/eme_dev/repo`
*   `AB_AIR_HOME` = `/appl/local/abinitio/abinitio-V2-14`
*   `PATH` = `${PATH}.:${AB_HOME}/bin`
*   `ETL_Host` = `dxcsa4.bn.detemobil.de`
*   `ETL_Projekt` = `BHB`
*   `AI_PRIV_SAND_ROOT` = `$HOME/abinitio`
*   `AI_ENV_SAND_ROOT` = `/appl/local/abinitio/sandboxes/DEV`
*   `AI_REPOSIT_TRACKING` = `FALSE` (commented out in the source script)

#### 1.6 Identify Undeclared Variables
*   `PATH`: Standard system path variable.
*   `HOME`: Standard Unix user home directory variable.
Since these are standard system environment variables inherited from the OS shell environment, they are considered standard system properties and do not require user-defined origin inputs.

#### 1.7 Identify Hive Queries
There are **no Hive queries** present in this Shell Script.

---

### Step 2: Decompose the Logic

#### 2.1 Breakdown of Steps
*   **Step 2.1.1: Environment Initialization:** Setting up configuration variables that represent paths, hosts, and project names.
*   **Step 2.1.2: Path Append:** Appending the execution paths.

#### 2.2 Equivalent in Horizon Python
*   Environment variables in Python are handled via the native `os.environ` map.
*   Path modifications can be implemented using `sys.path` or `os.environ["PATH"]`.
*   Since Horizon Python executes within a GCP/BigQuery-native cloud environment, on-premise Ab Initio paths (`/appl/local/abinitio/...`) and legacy sandbox root structures are no longer functionally operational. However, to match the exact behavior of variable assignments, they are mapped to standard Python dictionary configurations or environment variables.

---

### Step 3: Map Shell Commands to Horizon Python

*   **File Handling Commands**: None used.
*   **Sources and Targets**: No sources/targets to map.
*   **Hive to BQSQL**: No queries to convert.
*   **BQ Execution Pattern**: Though no queries are executed in this configuration script, we will structure the Python script using the requested Horizon import structure (`from framework.core.lib import script`) and execute the required setup logic securely.

---

### Design Document

#### Shell Script Analysis & Target Mapping
The provided shell script initializes structural path metadata for an Ab Initio environment. In a modern BigQuery-native Python (Horizon) framework, these physical directory paths (`/appl/...`) are obsolete. However, to maintain exact architectural compatibility, the variables are saved as environmental properties using Python's `os` module.

#### Assumptions and Additional Notes
1.  **Environment Relevance**: Variables pointing to physical local disks (such as `/appl/local/abinitio`) will not resolve to active local directories on serverless Google Cloud/BigQuery execution environments. They are retained purely as environment variable values for metadata alignment.
2.  **No SQL Execution**: Because there are no database queries or data loads in the source shell script, the `script.func_execute_bq` wrapper is imported but not executed.

---

### Low-Level Horizon Python Pseudocode

```python
#!/usr/bin/env python3
"""
Migration of Ab Initio Shell Environment Setup to Horizon Python.
Designed for BigQuery-native Horizon platform execution.
"""

import os
import sys

# Append Horizon library path to access framework resources
sys.path.append(os.getenv('DIR_LIB_PY', ''))
from framework.core.lib import script

def main():
    # ---------------------------------------------------------
    # Step 1: Initialize System Paths and Environment Metadata
    # ---------------------------------------------------------
    
    # AI-Environment Setup
    os.environ['AB_HOME'] = "/appl/local/abinitio/abinitio"
    os.environ['AB_AIR_ROOT'] = "/appl/local/abinitio/TMD_EME/eme_dev/repo"
    os.environ['AB_AIR_HOME'] = "/appl/local/abinitio/abinitio-V2-14"
    
    # Update execution PATH variable
    current_path = os.environ.get('PATH', '')
    ab_bin_path = f"{os.environ['AB_HOME']}/bin"
    os.environ['PATH'] = f"{current_path}.:{ab_bin_path}"
    
    # DWH-AI-Framework Configuration
    os.environ['ETL_Host'] = "dxcsa4.bn.detemobil.de"
    os.environ['ETL_Projekt'] = "BHB"
    
    # Establish Sandbox Roots
    home_dir = os.environ.get('HOME', '/home/default')
    os.environ['AI_PRIV_SAND_ROOT'] = f"{home_dir}/abinitio"
    os.environ['AI_ENV_SAND_ROOT'] = "/appl/local/abinitio/sandboxes/DEV"
    
    # AI_REPOSIT_TRACKING is commented out in the source script;
    # Decoupled matching representation:
    # os.environ['AI_REPOSIT_TRACKING'] = "FALSE"
    
    print("[INFO] Environment variables initialized successfully.")

if __name__ == "__main__":
    main()
```

---

### 6.2 FILE: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db`

=== Result for vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db ===
Based on your instructions, here is the comprehensive Shell-to-Horizon Python Migration Design Document and Low-Level Pseudocode.

---

# DESIGN DOCUMENT & MIGRATION SPECIFICATION

## 1. Shell Script Analysis

### 1.1 Sources and Targets
*   **Sources**: The shell script does not query or extract data from any sources.
*   **Targets**: No database tables or file targets are written to or modified.

### 1.2 Processing Flow
1.  **Environment Configuration**: The shell script sets the Oracle database character encoding environment variable `NLS_LANG` to `GERMAN_GERMANY.WE8ISO8859P1`.
2.  **Database Connection Variables**: It sets connection parameters (`DB_TNS_NAME_DWH`, `DB_USER_DWH`, and `DB_PASSWD_DWH`) to connect to an external Oracle database engine (`eDWH3.devlab.de.tmo`) using the user credential `meyreis` and an encrypted password.
3.  **Execution**: The script only initializes these environment variables and exits. There are no operational loops, conditional branches, or transformations executed.

### 1.3 External Dependencies
*   The script depends on the `ksh_dwh` KornShell execution environment.
*   It assumes an Oracle client environment that utilizes the `NLS_LANG` variable and is integrated with the `m_password` decryption utility.

### 1.4 Parameters and Variables
*   `NLS_LANG`: `GERMAN_GERMANY.WE8ISO8859P1` (Character set/locale)
*   `DB_TNS_NAME_DWH`: `@eDWH3.devlab.de.tmo` (Oracle connection string)
*   `DB_USER_DWH`: `meyreis` (Database username)
*   `DB_PASSWD_DWH`: `<password encrypted with m_password>` (Database password)

### 1.5 Undeclared Variables
*   **No undeclared variables were detected.** All variables utilized inside this script are defined directly within its body.

### 1.6 Hive Queries
*   **No Hive queries are present** in the provided shell script.

---

## 2. Decomposed Logic & Translation Strategy

In a Cloud/BigQuery Horizon Python environment, setting local shell variables for a legacy Oracle connection is obsolete unless those credentials need to be migrated to Secret Manager or utilized in an external federation query. 

We will establish a Pythonic configuration structure matching the exact functional properties of the source shell script. Additionally, we will import and structure the script to run seamlessly inside the Horizon environment.

### 2.1 Mapping Shell Commands to Horizon Python
*   **Environment Export (`export NLS_LANG=...`)**: Mapped to Python's `os.environ` configuration.
*   **Database Credentials**: Restructured into a configuration dictionary.

---

## 3. Assumptions and Additional Notes

1.  **Target Execution Environment**: The script is converted assuming it will execute in a BigQuery-native Python runtime utilizing Horizon framework standards.
2.  **Credential Security**: In a production environment, hardcoded encrypted passwords should be resolved using a Secret Manager vault. For the purpose of strict behavioral replication, these are mapped to python variables.
3.  **BigQuery Execution Standard**: The library-loading standard `sys.path.append(os.getenv('DIR_LIB_PY', ''))` is strictly implemented.

---

## 4. Python Pseudocode

Below is the complete, non-truncated Horizon Python code replicating the configuration setup of the shell script.

```python
#!/usr/bin/env python3
"""
Migration of Shell Configuration Script to Horizon Python.
Target Environment: BigQuery / Horizon Python Environment.
"""

import os
import sys

# Ensure Horizon Library Path is registered
DIR_LIB_PY = os.getenv('DIR_LIB_PY', '')
if DIR_LIB_PY:
    sys.path.append(DIR_LIB_PY)
else:
    # Fallback to local execution default if DIR_LIB_PY is empty
    sys.path.append('/home/horizon/lib/python')

from framework.core.lib import script

def main():
    try:
        # Step 1: Set Environment Locale/Encoding Parameters
        os.environ['NLS_LANG'] = 'GERMAN_GERMANY.WE8ISO8859P1'
        
        # Step 2: Initialize Database Connection Variables
        db_config = {
            'DB_TNS_NAME_DWH': '@eDWH3.devlab.de.tmo',
            'DB_USER_DWH': 'meyreis',
            'DB_PASSWD_DWH': '<password encrypted with m_password>'
        }
        
        # Step 3: Export variables to environment for downstream execution compatibility
        for key, value in db_config.items():
            os.environ[key] = value
            
        print("Environment and database connection variables initialized successfully.")
        
        # Note: If subsequent steps require executing BQSQL queries, they would use:
        # bqsql_query = "SELECT * FROM `your_project_id.your_dataset_id.your_table_name`"
        # script.func_execute_bq(bqsql_query, pass_file_name, col_delimiter, row_delimiter)

    except Exception as e:
        print(f"Error during environment initialization: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
```

---

### 6.3 FILE: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global`

=== Result for vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global ===
An analysis of the shell script and the corresponding design document along with low-level pseudocode for migrating it to the Horizon Python environment (optimized for Google BigQuery) is detailed below.

---

### Shell Script Analysis

#### 1.1 Identify Sources
*   **Environment Variables:** The script reads several pre-existing environment variables: `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `DW_DIR_IMP_VO`, `DW_DIR_IMP_RV`, `DW_DIR_IMP_IF`, `DW_DIR_IMP_NNV`, and `ORACLE_HOME`.
*   **External Configurations:** It checks for the existence of and sources a configuration shell script: `/appl/local/cognos/pya60207/setpya.sh`.

#### 1.2 Processing Flow
1.  **Validation Phase:** The script initializes an empty error string (`fehler`). It systematically checks if each critical path/environment variable is defined. If any of these are unset or empty, they are appended to the `fehler` string.
2.  **Error Reporting:** If `fehler` is not empty, it prints an error header and lists every missing environment variable. Note that the script *does not* call `exit 1` upon encountering missing variables; it merely prints warnings and continues execution.
3.  **Environment Setup:**
    *   Sets and exports Oracle NLS parameters (`NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`).
    *   Initializes `PYA_USR` as empty.
    *   Conditionally executes (sources) `/appl/local/cognos/pya60207/setpya.sh` if the file exists.
    *   Sets and exports `LANG=de`.
4.  **Commented Block Handling:** The updates to `$PATH` and `$SQLPATH` are commented out in the shell script and will be excluded from execution in the migrated Python version.

#### 1.3 Identify Targets
*   **System Environment (`os.environ`):** The final outputs of this script are exported environment variables. In Python, these will be injected directly into `os.environ` so that downstream processes run with the correct environment configuration.

#### 1.4 External Dependencies
*   `/appl/local/cognos/pya60207/setpya.sh` (External Shell Script). Note that sourcing a shell script to set environment variables directly inside Python requires running a subprocess to extract variables, or mapping its configuration to BigQuery/GCS parameters.

#### 1.5 Parameters and Variables
*   `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `DW_DIR_IMP_VO`, `DW_DIR_IMP_RV`, `DW_DIR_IMP_IF`, `DW_DIR_IMP_NNV`, `ORACLE_HOME`.

#### 1.6 Identify Undeclared Variables
*   There are **no undeclared variables** inside the script. All validated parameters are expected to be present in the execution environment prior to running this script (standard for initialization wrappers).

#### 1.7 Identify Hive Queries
*   **None.** There are no Hive or SQL queries executed within this script.

---

### Assumptions and Additional Notes

1.  **BigQuery Execution Context:** Since the migration target is BigQuery via Horizon Python, the target system will be a Linux-based Python runtime (e.g., Vertex AI Pipelines, Cloud Composer, or Dataproc Serverless).
2.  **Sourcing Shell Scripts in Python:** Sourcing `setpya.sh` directly within Python is not natively possible because Python cannot directly run a shell file to modify its own process environment. To handle this:
    *   The pseudocode checks for the file's existence.
    *   If the file exists, it runs a subprocess helper to execute the script and capture the resulting environment variables, importing them back into Python's `os.environ`.
3.  **Error Handling Behavior:** Following the exact logic of the provided script, validation errors are logged to standard output but do not halt execution.

---

### Low-Level Pseudocode

```python
"""
Migration of .dw_global Initialization Script to Horizon Python.
Purpose: Validate runtime paths, set up localizations, and source Cognos/Oracle configurations.
"""

import os
import sys
import subprocess

# Ensure Horizon Core library is accessible
sys.path.append(os.getenv('DIR_LIB_PY', ''))
# Note: Since no BigQuery queries are executed in this environment setup script,
# framework.core.lib.script.func_execute_bq is not invoked here, but the framework path is registered.


def validate_environment():
    """
    Validates required paths and environment parameters.
    Replicates the shell error-checking routine.
    """
    required_variables = [
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
    
    missing_variables = []
    
    for var in required_variables:
        if not os.getenv(var):
            missing_variables.append(var)
            
    if missing_variables:
        print("Fehler in .dw_global:")
        for varname in missing_variables:
            print(f"   Umgebungsvariable {varname} ist nicht gesetzt !")


def source_shell_script(script_path):
    """
    Helper function to execute an external shell script, capture the exported 
    variables, and apply them directly to Python's os.environ.
    """
    if os.path.exists(script_path) and os.path.isfile(script_path):
        try:
            # Run the script and output the updated environment variables
            command = f"exec /bin/ksh -c '. {script_path} && env'"
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, text=True)
            stdout, _ = proc.communicate()
            
            if proc.returncode == 0:
                for line in stdout.splitlines():
                    # Parse exported key=value pairs
                    if '=' in line:
                        key, _, value = line.partition('=')
                        os.environ[key] = value
            else:
                print(f"Warning: Execution of {script_path} failed with return code {proc.returncode}")
        except Exception as e:
            print(f"Warning: Could not source {script_path}. Error: {str(e)}")


def set_global_parameters():
    """
    Applies Oracle NLS, Cognos settings, and other localization variables.
    """
    # SQL-Net 2 Connections Configuration
    os.environ["NLS_LANG"] = "GERMAN_GERMANY.WE8ISO8859P1"
    os.environ["NLS_DATE_FORMAT"] = "DD.MM.YY"
    os.environ["NLS_DATE_LANGUAGE"] = "GERMAN_GERMANY.WE8ISO8859P1"

    # Cognos PowerPlay configuration
    os.environ["PYA_USR"] = ""
    
    # Conditional sourcing of the Cognos configuration script
    # SOURCE: NOT FOUND — SETPYA.SH — No candidate found in scan.
    cognos_setpya_path = "/appl/local/cognos/pya60207/setpya.sh"
    source_shell_script(cognos_setpya_path)
    
    # Final localization overrides
    os.environ["LANG"] = "de"


def main():
    # 1. Run validation of critical paths
    validate_environment()
    
    # 2. Set Oracle/Cognos/Lang parameters
    set_global_parameters()
    
    # 3. Print success acknowledgement
    print("Horizon global variables initialized successfully.")


if __name__ == "__main__":
    main()
```

---

### 6.4 FILE: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init`

=== Result for vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init ===
### Design Document: Shell Script Migration to Horizon Python

This document outlines the migration design of an environment initialization KornShell (`ksh`) script into Horizon Python, designed to be executed within a BigQuery environment.

---

### 1. Shell Script Analysis

#### 1.1 Sources and Targets
*   **Sources**: 
    *   No physical data files or database tables are read directly in this script.
    *   The script reads environment variables like `$HOME` and `$ORACLE_HOME`.
    *   It sources two external shell scripts: `$HOME/.dw_global` and `$HOME/.dw_lokal`.
*   **Targets**:
    *   No persistent database tables, files, or message queues are written to.
    *   The primary output of this script is setting environment variables (`export`) to define directory paths, database connection parameters, and paths to utility scripts.

#### 1.2 Processing Flow
1.  **Define Environment Variables**: The script sets up a series of path variables related to roots, logs, cubes, import zones, and exports (under the `$HOME` path structure).
2.  **Define Host Information**: Sets host details for remote systems (e.g., `DW_HOST_CUSTOMER`).
3.  **Validate and Set `ORACLE_HOME`**:
    *   Checks if `$ORACLE_HOME` is empty.
    *   If empty, it checks for directories `/appl/local/oracle/12.2.0.1.0` or `/appl/local/oracle/11.2.0` sequentially and exports the matching directory.
    *   If neither directory is present, it prints an error message.
4.  **Execute Sub-Scripts**: It sources `.dw_global` and `.dw_lokal` configurations.
5.  **Define Oracle-Specific Paths**: Dynamically sets `DW_DIR_UTL_FILE` using the active `$ORACLE_SID`.

#### 1.3 External Dependencies
*   The script depends on the availability of local filesystem paths (under `/appl/local/` and `$HOME`).
*   It requires the presence of `$HOME/.dw_global` and `$HOME/.dw_lokal`.

#### 1.4 Parameters and Variables
All key environment variables declared in the script are listed below with their default mapping structures:

| Variable Name | Description / Path |
| :--- | :--- |
| `DW_DIR_ROOT` | `$HOME/aktuell` |
| `DW_DIR_PROT` | `$HOME/daten/logfiles` |
| `DW_DIR_CUBES` | `$HOME/daten/cubes` |
| `DW_DIR_IMP_*` | Various imports under `$HOME/daten/` |
| `DW_DIR_SMS_*` | SMS directories |
| `DW_HOST_CUSTOMER` | `dxcst3.bn.detemobil.de` |
| `ORACLE_HOME` | `/appl/local/oracle/12.2.0.1.0` or `/appl/local/oracle/11.2.0` |
| `DW_DIR_UTL_FILE` | `/appl/local/oracle/admin/$ORACLE_SID/utl_file` |

#### 1.5 Undeclared or Context-Dependent Variables
The script contains the following variables whose origins depend on the environment context:
1.  `HOME`: The system user's home directory.
2.  `ORACLE_SID`: Database System Identifier, used to build `DW_DIR_UTL_FILE`.

*To maintain consistency, we will resolve these using Python's standard `os` module or default parameters, representing any missing cloud/filesystem mappings to Cloud Storage (GCS).*

#### 1.6 Hive/BigQuery Queries
There are no Hive or SQL queries present in this initialization script.

---

### 2. Decomposition and Migration Strategy

#### 2.1 Mapping Strategy to Horizon Python and Google Cloud
*   **Environment Handling**: Environment variable setups are translated to Python `os.environ` updates.
*   **Pathing Modernization**: Where directories are referenced, they will default to Python paths or mapped to Google Cloud Storage (GCS) prefixes (e.g., `gs://<bucket>/...`) if they are target directories for modern workloads.
*   **Sourced Scripts**: Sourcing external shell scripts (`.dw_global`, `.dw_lokal`) is represented in the python logic via a helper placeholder function. This function mimics loading configurations dynamically, ensuring modular structure consistency.
*   **No Hive Queries**: No `func_execute_bq` executions are required for this configuration file, but the importing framework structure is prepared to ensure Horizon standards are met.

---

### 3. Low-Level Python Pseudocode

```python
import os
import sys
from pathlib import Path

# Required framework environment setup
sys.path.append(os.getenv('DIR_LIB_PY', ''))
# Note: From framework.core.lib import script is omitted here since no BigQuery execution (func_execute_bq) is needed.

def source_config_file(file_path: str):
    """
    Simulates sourcing a shell configuration file.
    In a real Python migration, this reads key-value pairs or executes a Python configuration equivalent.
    """
    path = Path(file_path).expanduser()
    if path.exists():
        print(f"[INFO] Sourcing configurations from: {path}")
        # Parse or load environment settings if necessary
    else:
        print(f"[WARNING] Configuration file {path} not found.")

def main():
    # 1. Resolve HOME directory
    home_dir = os.getenv('HOME', str(Path.home()))
    
    # 2. Define and Export Information Service Directory Paths
    os.environ['DW_DIR_ROOT'] = os.path.join(home_dir, 'aktuell')
    os.environ['DW_DIR_PROT'] = os.path.join(home_dir, 'daten/logfiles')
    os.environ['DW_DIR_CUBES'] = os.path.join(home_dir, 'daten/cubes')

    # Import and Export Directories
    os.environ['DW_DIR_IMP_D1'] = os.path.join(home_dir, 'daten/d1')
    os.environ['DW_DIR_IMP_BWA'] = os.path.join(home_dir, 'daten/dpps/bwa')
    os.environ['DW_DIR_IMP_XTRA'] = os.path.join(home_dir, 'daten/xtra')
    os.environ['DW_DIR_IMP_CTEL'] = os.path.join(home_dir, 'daten/ctel')
    os.environ['DW_DIR_IMP_VO'] = os.path.join(home_dir, 'daten/vo')
    os.environ['DW_DIR_IMP_RV'] = os.path.join(home_dir, 'daten/rv')
    os.environ['DW_DIR_IMP_IF'] = os.path.join(home_dir, 'daten/ees')
    os.environ['DW_DIR_IMP_NNV'] = os.path.join(home_dir, 'daten/nnv')
    os.environ['DW_DIR_IMP_SIGMA'] = os.path.join(home_dir, 'daten/gd/sigma')
    os.environ['DW_DIR_EXP_SIGMA'] = os.path.join(home_dir, 'daten/gd/sigma/export')
    os.environ['DW_DIR_IMP_TRF'] = os.path.join(home_dir, 'daten/trf')
    os.environ['DW_DIR_IMP_AUF'] = os.path.join(home_dir, 'daten/sd/auf')
    os.environ['DW_DIR_IMP_GUT'] = os.path.join(home_dir, 'daten/sd/gut')
    os.environ['DW_DIR_IMP_KDG'] = os.path.join(home_dir, 'daten/sd/kdg')
    os.environ['DW_DIR_IMP_MP_KDG'] = os.path.join(home_dir, 'daten/mp/kdg')
    os.environ['DW_DIR_IMP_MP_TS'] = os.path.join(home_dir, 'daten/mp/ts')
    os.environ['DW_DIR_IMP_MP_ZM'] = os.path.join(home_dir, 'daten/mp/zm')
    os.environ['DW_DIR_IMP_TS'] = os.path.join(home_dir, 'daten/sd/ts')
    os.environ['DW_DIR_IMP_ZM'] = os.path.join(home_dir, 'daten/sd/zm')
    os.environ['DW_DIR_EXP'] = os.path.join(home_dir, 'daten/exporter')
    os.environ['DW_DIR_IMP_BPM'] = os.path.join(home_dir, 'daten/bm')
    os.environ['DW_DIR_IMP_ZTS'] = os.path.join(home_dir, 'daten/zts')
    os.environ['DW_DIR_IMP_VRS'] = os.path.join(home_dir, 'daten/vrs')
    os.environ['DW_DIR_IMP_BRUNET'] = os.path.join(home_dir, 'daten/brunet')
    os.environ['DW_DIR_IMP_DWH'] = os.path.join(home_dir, 'daten/dwh')
    os.environ['DW_DIR_IMP_PLATO'] = os.path.join(home_dir, 'daten/dwh/plato')
    
    # DWH 2.5 imports
    os.environ['DW_DIR_IMP_CARMEN'] = os.path.join(home_dir, 'daten/carmen')
    os.environ['DW_DIR_IMP_SAP'] = os.path.join(home_dir, 'daten/sap')
    os.environ['DW_DIR_IMP_SR_RV'] = os.path.join(home_dir, 'daten/sap/sr_rv_dpps')
    os.environ['DW_DIR_IMP_SAP_L'] = os.path.join(home_dir, 'daten/sap/sap_l_gutgr')
    os.environ['DW_DIR_IMP_L_MAHNSTYP_IST'] = os.path.join(home_dir, 'daten/sap/mahn')
    os.environ['DW_DIR_IMP_L_MAHNV_FI'] = os.path.join(home_dir, 'daten/sap/mahn')
    os.environ['DW_DIR_IMP_L_MAHNV_IST'] = os.path.join(home_dir, 'daten/sap/mahn')
    os.environ['DW_DIR_IMP_L_GUTGR'] = os.path.join(home_dir, 'daten/sd/l_gutschr')
    os.environ['DW_DIR_IMP_L_LEIST'] = os.path.join(home_dir, 'daten/sd/l_leist')
    os.environ['DW_DIR_IMP_L_PROD'] = os.path.join(home_dir, 'daten/sd/l_prod')
    os.environ['DW_DIR_IMP_LKODE'] = os.path.join(home_dir, 'daten/sd/lkode')

    # DWH 7.5 Subscription Server
    os.environ['DW_DIR_IMP_SUBSE'] = os.path.join(home_dir, 'daten/subse')

    # DWH 2.5 SMS Utility Paths
    os.environ['DW_DIR_SMS_PRG'] = os.path.join(home_dir, 'aktuell/allgemein/is/util')
    os.environ['DW_DIR_SMS_ADR'] = os.path.join(home_dir, 'daten/sms/adressen')
    os.environ['DW_DIR_SMS_TMP'] = os.path.join(home_dir, 'daten/sms/tmp')

    # DWH 3.5 Planning
    os.environ['DW_DIR_IMP_DPPS'] = os.path.join(home_dir, 'daten/dpps')
    os.environ['DW_DIR_IMP_PLANF2'] = os.path.join(home_dir, 'daten/planf2')

    # Remote Network Hosts
    os.environ['DW_HOST_CUSTOMER'] = 'dxcst3.bn.detemobil.de'

    # 3. Handle ORACLE_HOME setting and path validation
    oracle_home = os.getenv('ORACLE_HOME', '')
    if not oracle_home:
        path_12_2 = '/appl/local/oracle/12.2.0.1.0'
        path_11_2 = '/appl/local/oracle/11.2.0'
        
        if os.path.isdir(path_12_2):
            oracle_home = path_12_2
        elif os.path.isdir(path_11_2):
            oracle_home = path_11_2
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
            
        if oracle_home:
            os.environ['ORACLE_HOME'] = oracle_home

    # 4. Source Global and Local parameters
    source_config_file(os.path.join(home_dir, '.dw_global'))
    
    # SOURCE: NOT FOUND — .DW_LOKAL — no candidate
    source_config_file(os.path.join(home_dir, '.dw_lokal'))

    # 5. Dynamic setting of UTL_FILE directory using active ORACLE_SID
    oracle_sid = os.getenv('ORACLE_SID', 'DEFAULT_SID')
    os.environ['DW_DIR_UTL_FILE'] = f'/appl/local/oracle/admin/{oracle_sid}/utl_file'
    
    print("[SUCCESS] Environment initialization completed.")

if __name__ == '__main__':
    main()
```