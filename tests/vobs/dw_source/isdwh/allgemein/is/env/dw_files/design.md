# Migration Design Document

**Seed Name:** Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Seed Type:** `shared_files`  
**Source Root:** `/home/gurunathan_t/folder1_uc4_ksh_abinitio`  
**Target Platform:** Google Cloud BigQuery / Cloud Composer (Airflow)  
**Prescribed Migration Pattern:** `UC4_ONLY` (High Confidence) — Pure environment and orchestration initialization config; no data layer migration involved.

---

## 1. System-Wide Environment and Configuration Strategy (Target Platform)

In the legacy on-premise framework, the four configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) were sourced dynamically by various interactive shells, Ab Initio EME runtimes, and KornShell wrapper scripts to establish file system boundaries, Oracle TNS aliases, locale settings, and Ab Initio framework parameters. 

In the migrated **Google Cloud BigQuery and Cloud Composer** architecture, these environment configurations must be handled as follows:
*   **Infrastructure Configuration (GCP Projects, Buckets, Dataproc regional endpoints):** Managed via **Global Environment Variables** in the target environments.
*   **BigQuery Datasets, Tables, and Stored Procedures:** Handled via Airflow `params` or fully-qualified SQL references, with project IDs injected dynamically at run-time via calling tasks.
*   **File Path Mapping (POSIX Paths to GCS Buckets):** Standardized using unified cloud storage URIs. All `$HOME/daten/...` paths map directly to directories within a dedicated Google Cloud Storage bucket (`GCS_BUCKET`).
*   **Database Integration (Oracle to BigQuery):** Legacy Oracle parameters (`NLS_LANG`, `DB_TNS_NAME_DWH`, `ORACLE_HOME`) are retired. BigQuery connectivity is handled natively using Airflow's BigQuery operators with Service Accounts.

---

## 2. Target File Plan

| Source File | Target File Path (Cloud Composer / Airflow Config) | Target Language | Implementation / Role |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai` | `/dags/config/env_ai_config.json` | JSON | Holds environment variables for the Ab Initio environment compatibility. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db` | Airflow Connection / Secret Manager | IAM Role / Connection | Retires standard files; connection and credentials migrate securely into **Secret Manager** & **Airflow Connections**. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` | `/dags/utils/env_validator.py` | Python | Python validator used to ensure runtime Composer parameters are populated. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init` | `/dags/config/gcs_paths.json` | JSON | Dictionary mapping all legacy directories directly to GCS path URIs. |

---

## 3. Environment Variable Classifications

### GLOBAL Variables (Environment-Wide Infrastructure)
These values are resolved dynamically in Cloud Composer at runtime using `from airflow.models import Variable`.

| Legacy Reference | Target Key | Resolution Mechanism / Default |
| :--- | :--- | :--- |
| `ETL_Host` | `GCP_PROJECT` | `Variable.get("GCP_PROJECT")` |
| `DW_HOST_CUSTOMER` | `GCS_BUCKET` | `Variable.get("GCS_BUCKET")` (e.g. `gs://dwh-isdwh-prod`) |
| `ORACLE_HOME` | `BQ_LOCATION` | `Variable.get("BQ_LOCATION", default_var="EU")` |

### JOB-SPECIFIC Variables (Configured on Target Orchestrators)
These are injected into specific Airflow DAG contexts via DAG `params`.

| Legacy Reference | Target Variable Name | Mapping value in Target Config |
| :--- | :--- | :--- |
| `ETL_Projekt` | `project_code` | `"BHB"` |
| `DB_USER_DWH` | `bq_dataset` | `"meyreis"` (Migrated sandbox dataset) |
| `DW_DIR_ROOT` | `root_gcs_prefix` | `gs://{GCS_BUCKET}/aktuell/` |
| `DW_DIR_PROT` | `log_gcs_prefix` | `gs://{GCS_BUCKET}/daten/logfiles/` |
| `DW_DIR_CUBES` | `cubes_gcs_prefix` | `gs://{GCS_BUCKET}/daten/cubes/` |

---

## 4. Dependencies & Execution Context

### Upstream / Downstream Job Mapping
*   **Downstream Consumers:** 
    *   `DW.DWH_ABPZ_KKM_AIL_AGENT` — *not yet migrated*
    *   `r_ai_start` — *not yet migrated*
    *   `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` — *not yet migrated*
*   **Orchestration Wiring:** Since downstream components are not yet migrated, this initialization configuration should be published as a unified JSON config inside a GCS Metadata bucket. Downstream Airflow DAGs will access this configuration via an S3/GCS sensor or by directly parsing `/dags/config/gcs_paths.json`.

### Lineage Edges (Unresolved Components)
*   The legacy `.dw_global` script sources `setpya.sh` for Cognos PowerPlay integrations. This integration is marked as out of scope for the BigQuery target runtime.
*   The legacy `.dw_init` script conditionally sources `.dw_lokal`. 

---

## 5. Reverse-Engineered Design Specs (Verbatim MCP Output)

### === Result for `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai` ===

```text
# Bash Design Document

This document provides a technical design specification of the provided KornShell (`ksh_dwh`) configuration script.

---

## 1. Objective
The objective of this script is to initialize and export environment variables required for the Ab Initio AI-Environment and the DWH-AI-Framework (Data Warehouse Ab Initio Framework) in a development/sandbox context. 

---

## 2. Inputs and Outputs

### Inputs
*   **System Environment Variables**: The existing system `PATH` and `HOME` variables are used as inputs to construct new paths.

### Outputs
*   **Environment Variables**: The script exports several environment configuration variables to the shell execution environment:
    *   `AB_HOME`
    *   `AB_AIR_ROOT`
    *   `PATH`
    *   `ETL_Host`
    *   `ETL_Projekt`
    *   `AI_PRIV_SAND_ROOT`
    *   `AI_ENV_SAND_ROOT`

### Types of Input/Output
*   **Files**: None.
*   **Tables**: None.
*   **API Calls**: None.
*   **Shell Environment**: Standard in-memory Unix/Linux environment variables.

---

## 3. Dependencies
*   **Shell Interpreter**: `/bin/ksh_dwh` (KornShell variant).
*   **System Directories**: 
    *   `/appl/local/abinitio/abinitio` (Ab Initio home directory)
    *   `/appl/local/abinitio/TMD_EME/eme_dev/repo` (Ab Initio EME repository root)
    *   `$HOME/abinitio` (User-specific private sandbox directory)
    *   `/appl/local/abinitio/sandboxes/DEV` (Development sandbox root directory)

---

## 4. Error Scenarios
*   *Not implemented.* (The script executes sequential variable assignments with no conditional logic, validation checks, or error handling routines).

---

## 5. Abstract Syntax Tree (AST)

```
Program
├── Shebang (#!/bin/ksh_dwh)
├── Comment ("# AI-Environment")
├── Assignment & Export (AB_HOME="/appl/local/abinitio/abinitio")
├── Assignment & Export (AB_AIR_ROOT="/appl/local/abinitio/TMD_EME/eme_dev/repo")
├── Assignment (AB_AIR_HOME="/appl/local/abinitio/abinitio-V2-14")
├── Assignment & Export (PATH="${PATH}.:${AB_HOME}/bin")
├── Comment ("# DWH-AI-Framework")
├── Assignment & Export (ETL_Host="dxcsa4.bn.detemobil.de")
├── Assignment & Export (ETL_Projekt="BHB")
├── Assignment & Export (AI_PRIV_SAND_ROOT="$HOME/abinitio")
├── Assignment & Export (AI_ENV_SAND_ROOT="/appl/local/abinitio/sandboxes/DEV")
├── Comment ("# Repository Tracking deaktivieren")
└── Comment ("#AI_REPOSIT_TRACKING=FALSE; export AI_REPOSIT_TRACKING")
```

---

## 6. Pseudo Code

```text
SET AB_HOME TO "/appl/local/abinitio/abinitio"
EXPORT AB_HOME

SET AB_AIR_ROOT TO "/appl/local/abinitio/TMD_EME/eme_dev/repo"
EXPORT AB_AIR_ROOT

SET AB_AIR_HOME TO "/appl/local/abinitio/abinitio-V2-14"

SET PATH TO CURRENT_PATH + ".:" + AB_HOME + "/bin"
EXPORT PATH

SET ETL_Host TO "dxcsa4.bn.detemobil.de"
EXPORT ETL_Host

SET ETL_Projekt TO "BHB"
EXPORT ETL_Projekt

SET AI_PRIV_SAND_ROOT TO USER_HOME_DIRECTORY + "/abinitio"
EXPORT AI_PRIV_SAND_ROOT

SET AI_ENV_SAND_ROOT TO "/appl/local/abinitio/sandboxes/DEV"
EXPORT AI_ENV_SAND_ROOT
```

---

## 7. Security Implementations
*   *Not implemented.* (No credentials, authorizations, encryption, or security mechanisms are implemented in the code).

---

## 8. Monitoring Implementations
*   *Not implemented.* (No logging, auditing, tracing, or monitoring mechanisms are implemented in the code).
```

### === Result for `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db` ===

```text
# Bash Design Document

This design document is generated from the provided KornShell (`ksh_dwh`) script snippet. 

---

## 1. Objective
The objective of this script is to set up the database environment variables, specifically the locale/language settings and the database connection credentials (TNS name, username, and encrypted password) for an Oracle Database connection targeting the `eDWH2`/`eDWH3` environment.

---

## 2. Inputs and Outputs

### Inputs
*   **Hardcoded Configuration Values:**
    *   `NLS_LANG` value: `"GERMAN_GERMANY.WE8ISO8859P1"`
    *   Database TNS Name: `"@eDWH3.devlab.de.tmo"`
    *   Database User: `"meyreis"`
    *   Database Password: Enclosed as an encrypted string `"<password encrypted with m_password>"`

### Outputs
*   **Environment Variables (exported to the shell session):**
    *   `NLS_LANG`
    *   `DB_TNS_NAME_DWH`
    *   `DB_USER_DWH`
    *   `DB_PASSWD_DWH`

---

## 3. Dependencies
*   **Interpreter:** `/bin/ksh_dwh` (KornShell variant)
*   **Encryption Utility:** `m_password` (referenced in comments as the tool used to encrypt the password value stored in `DB_PASSWD_DWH`).

---

## 4. Error Scenarios
*   Not implemented.

---

## 5. Abstract Syntax Tree (AST)

Below is the conceptual Abstract Syntax Tree representing the sequential execution flow of variable assignments and exports in the script.

```
Program (Interpreter: /bin/ksh_dwh)
 └── Sequence
      ├── Command: Variable Assignment & Export
      │    ├── Name: NLS_LANG
      │    └── Value: "GERMAN_GERMANY.WE8ISO8859P1"
      ├── Command: Variable Assignment & Export
      │    ├── Name: DB_TNS_NAME_DWH
      │    └── Value: "@eDWH3.devlab.de.tmo"
      ├── Command: Variable Assignment & Export
      │    ├── Name: DB_USER_DWH
      │    └── Value: "meyreis"
      └── Command: Variable Assignment & Export
           ├── Name: DB_PASSWD_DWH
           └── Value: "<password encrypted with m_password>"
```

---

## 6. Pseudo Code

```text
START
    EXPORT NLS_LANG = "GERMAN_GERMANY.WE8ISO8859P1"
    EXPORT DB_TNS_NAME_DWH = "@eDWH3.devlab.de.tmo"
    EXPORT DB_USER_DWH = "meyreis"
    EXPORT DB_PASSWD_DWH = "<password encrypted with m_password>"
END
```

---

## 7. Security Implementations

### Credentials & Encryption
*   **Password Encryption:** The database password assigned to `DB_PASSWD_DWH` is not stored in plaintext; it is encrypted using the `m_password` utility.

---

## 8. Monitoring Implementations
*   Not implemented.
```

### === Result for `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` ===

```text
# Technical Design Document: `.dw_global`

## 1. Objective
The `.dw_global` script is an initialization file designed to run under the Korn Shell (`/bin/ksh`). Its primary purpose is to validate the existence of mandatory environment variables and establish global configuration parameters (such as database locale settings, system paths, and third-party application configurations) required for a Data Warehouse (DWH) execution environment. This script is intended to be sourced exclusively by `dwh_init`.

---

## 2. Inputs and Outputs

### Inputs
*   **Environment Variables**: The script reads and checks for the existence of the following system environment variables:
    *   `DW_DIR_ROOT`
    *   `DW_DIR_PROT`
    *   `DW_DIR_CUBES`
    *   `DW_DIR_IMP_D1`
    *   `DW_DIR_IMP_XTRA`
    *   `DW_DIR_IMP_CTEL`
    *   `DW_DIR_IMP_VO`
    *   `DW_DIR_IMP_RV`
    *   `DW_DIR_IMP_IF`
    *   `DW_DIR_IMP_NNV`
    *   `ORACLE_HOME`
*   **External Configuration Files**: Sourced conditionally if present on the filesystem:
    *   `/appl/local/cognos/pya60207/setpya.sh`

### Outputs
*   **Standard Output (`stdout`)**: Prints error messages listing any missing environment variables.
*   **Environment Variables Set/Exported**:
    *   `NLS_LANG` (Set to `GERMAN_GERMANY.WE8ISO8859P1`)
    *   `NLS_DATE_FORMAT` (Set to `DD.MM.YY`)
    *   `NLS_DATE_LANGUAGE` (Set to `GERMAN_GERMANY.WE8ISO8859P1`)
    *   `PYA_USR` (Set to an empty string `""`)
    *   `LANG` (Set to `de`)
    *   Other environment variables modifications made by sourcing external configuration file `/appl/local/cognos/pya60207/setpya.sh`.

---

## 3. Dependencies
*   **Shell Interpreter**: `/bin/ksh` (Korn Shell).
*   **External File System Dependencies**: 
    *   `/appl/local/cognos/pya60207/setpya.sh` (Sourced dynamically if the file exists).

---

## 4. Error Scenarios
*   **Missing Environment Variables**: 
    *   *Detection*: The script tests whether each of the 11 mandatory environment variables (`DW_DIR_ROOT`, `ORACLE_HOME`, etc.) is empty or unset.
    *   *Handling*: Accumulates missing variable names in a local variable `fehler`. If any are missing, it prints a list of the unset variables to standard output. (Note: The script does not terminate execution; it only prints the errors).

---

## 5. Abstract Syntax Tree

```
Program
├── Step 1: Initialize Error Collector
│   └── fehler=""
│
├── Step 2: Environmental Validations (Conditional Checks)
│   ├── If [ -z "$DW_DIR_ROOT" ]        => True: fehler="$fehler DW_DIR_ROOT "
│   ├── If [ -z "$DW_DIR_PROT" ]        => True: fehler="$fehler DW_DIR_PROT "
│   ├── If [ -z "$DW_DIR_CUBES" ]       => True: fehler="$fehler DW_DIR_CUBES "
│   ├── If [ -z "$DW_DIR_IMP_D1" ]      => True: fehler="$fehler DW_DIR_IMP_D1 "
│   ├── If [ -z "$DW_DIR_IMP_XTRA" ]    => True: fehler="$fehler DW_DIR_IMP_XTRA "
│   ├── If [ -z "$DW_DIR_IMP_CTEL" ]    => True: fehler="$fehler DW_DIR_IMP_CTEL "
│   ├── If [ -z "$DW_DIR_IMP_VO" ]      => True: fehler="$fehler DW_DIR_IMP_VO "
│   ├── If [ -z "$DW_DIR_IMP_RV" ]      => True: fehler="$fehler DW_DIR_IMP_RV "
│   ├── If [ -z "$DW_DIR_IMP_IF" ]      => True: fehler="$fehler DW_DIR_IMP_IF "
│   ├── If [ -z "$DW_DIR_IMP_NNV" ]     => True: fehler="$fehler DW_DIR_IMP_NNV "
│   └── If [ -z "$ORACLE_HOME" ]        => True: fehler="$fehler ORACLE_HOME"
│
├── Step 3: Error Reporting
│   └── If [ ! -z "$fehler" ] (Is True if there are errors)
│       ├── Action: Echo "Fehler in .dw_global:"
│       └── For each varname in $fehler
│           └── Action: Echo "   Umgebungsvariable $varname ist nicht gesetzt !"
│
└── Step 4: Configuration Settings & Execution
    ├── Set/Export NLS_LANG="GERMAN_GERMANY.WE8ISO8859P1"
    ├── Set/Export NLS_DATE_FORMAT="DD.MM.YY"
    ├── Set/Export NLS_DATE_LANGUAGE="GERMAN_GERMANY.WE8ISO8859P1"
    ├── Set/Export PYA_USR=""
    ├── Test file existence: /appl/local/cognos/pya60207/setpya.sh
    │   └── If exists => Source file: . /appl/local/cognos/pya60207/setpya.sh
    └── Set/Export LANG="de"
```

---

## 6. Pseudo Code

```pseudo
Initialize fehler as empty string ""

# Check mandatory environment variables
IF DW_DIR_ROOT is empty THEN
    Append " DW_DIR_ROOT " to fehler
END IF
IF DW_DIR_PROT is empty THEN
    Append " DW_DIR_PROT " to fehler
END IF
IF DW_DIR_CUBES is empty THEN
    Append " DW_DIR_CUBES " to fehler
END IF
IF DW_DIR_IMP_D1 is empty THEN
    Append " DW_DIR_IMP_D1 " to fehler
END IF
IF DW_DIR_IMP_XTRA is empty THEN
    Append " DW_DIR_IMP_XTRA " to fehler
END IF
IF DW_DIR_IMP_CTEL is empty THEN
    Append " DW_DIR_IMP_CTEL " to fehler
END IF
IF DW_DIR_IMP_VO is empty THEN
    Append " DW_DIR_IMP_VO " to fehler
END IF
IF DW_DIR_IMP_RV is empty THEN
    Append " DW_DIR_IMP_RV " to fehler
END IF
IF DW_DIR_IMP_IF is empty THEN
    Append " DW_DIR_IMP_IF " to fehler
END IF
IF DW_DIR_IMP_NNV is empty THEN
    Append " DW_DIR_IMP_NNV " to fehler
END IF
IF ORACLE_HOME is empty THEN
    Append " ORACLE_HOME" to fehler
END IF

# Report errors if any variables are missing
IF fehler is not empty THEN
    PRINT "Fehler in .dw_global:"
    FOR EACH varname IN fehler DO
        PRINT "   Umgebungsvariable " + varname + " ist nicht gesetzt !"
    END FOR
END IF

# Set Database National Language Support (NLS) parameters
NLS_LANG = "GERMAN_GERMANY.WE8ISO8859P1"
Export NLS_LANG

NLS_DATE_FORMAT = "DD.MM.YY"
Export NLS_DATE_FORMAT

NLS_DATE_LANGUAGE = "GERMAN_GERMANY.WE8ISO8859P1"
Export NLS_DATE_LANGUAGE

# Set Cognos parameters
PYA_USR = ""
Export PYA_USR

# Conditionally source Cognos setup script if it exists
IF file "/appl/local/cognos/pya60207/setpya.sh" exists THEN
    Source "/appl/local/cognos/pya60207/setpya.sh"
END IF

LANG = "de"
Export LANG
```

---

## 7. Security Implementations
Not implemented.

---

## 8. Monitoring Implementations
*   **Console Logging**: Writes standard text warning/error logs directly to stdout when validated environment variables are missing (e.g., `Umgebungsvariable [VAR] ist nicht gesetzt !`).
```

### === Result for `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init` ===

```text
# Technical Design Document: Information Service Environment Setup (`.dw_init`)

## 1. Objective
The primary objective of this KornShell (`ksh`) script is to initialize and export global environment variables required for the operation of the "Information Service" (Data Warehouse system). It configures directory pathways for various import/export interfaces, sets system integration directories, determines the correct `ORACLE_HOME` path based on the host environment, and loads additional global and local configuration profiles.

---

## 2. Inputs and Outputs

### Inputs
*   **Environment Variables**:
    *   `HOME`: Used as the base path to resolve user directories (e.g., `$HOME/daten/...`).
    *   `ORACLE_HOME` (Optional): If pre-defined, the script skips automatic Oracle home detection.
    *   `ORACLE_SID`: Used to construct the database utility file pathway.
*   **External Files / Configurations**:
    *   `$HOME/.dw_global`: A shell script containing global system parameters (sourced during execution).
    *   `$HOME/.dw_lokal`: A shell script containing host-local parameters (sourced during execution).
*   **Filesystem Directory Presence Checks**:
    *   `/appl/local/oracle/12.2.0.1.0`
    *   `/appl/local/oracle/11.2.0`

### Outputs
*   **Exported Environment Variables**:
    *   `DW_DIR_ROOT`: Root execution directory for scripts.
    *   `DW_DIR_PROT`: Directory path for logging and protocol outputs.
    *   `DW_DIR_CUBES`: Target directory for OLAP cubes.
    *   `DW_DIR_IMP_*` (Multiple variables): Import path mappings for various modules (e.g., SAP, D1, PLATO, CARMEN, DPPS, SMS).
    *   `DW_DIR_EXP_*` (Multiple variables): Export path mappings.
    *   `DW_DIR_SMS_*` (Multiple variables): Paths for utility programs, addresses, and temporary directories related to SMS.
    *   `DW_HOST_CUSTOMER`: Remote customer host address (`dxcst3.bn.detemobil.de`).
    *   `ORACLE_HOME`: Evaluated directory path for the active Oracle database system.
    *   `DW_DIR_UTL_FILE`: Directory path allocated for Oracle's `UTL_FILE` processing.

---

## 3. Dependencies
*   **Shell Interpreter**: `/bin/ksh` (KornShell).
*   **Oracle Database System**: Relies on specific installation paths (`/appl/local/oracle/*`) for standard operation.
*   **Sub-configuration Scripts**: 
    *   `$HOME/.dw_global`
    *   `$HOME/.dw_lokal`

---

## 4. Error Scenarios
*   **Oracle Home Determination Failure**:
    If `ORACLE_HOME` is not pre-set and neither target directories `/appl/local/oracle/12.2.0.1.0` nor `/appl/local/oracle/11.2.0` exist on the filesystem, the script outputs an error message to standard output (`stdout`):
    ```text
    Fehler in .dw_init:
       Konnte ORACLE_HOME nicht setzen !
    ```
    *Note: Even if this evaluation fails, the script continues execution without halting (non-zero termination is not implemented).*

---

## 5. Abstract Syntax Tree (AST)

```
[Program]
   │
   ├── [Block: Path Configuration Variables]
   │     ├── Assign & Export: DW_DIR_ROOT = $HOME/aktuell
   │     ├── Assign & Export: DW_DIR_PROT = $HOME/daten/logfiles
   │     ├── Assign & Export: DW_DIR_CUBES = $HOME/daten/cubes
   │     ├── Assign & Export: [Various DW_DIR_IMP_* paths...]
   │     ├── Assign & Export: [Various DW_DIR_EXP_* paths...]
   │     ├── Assign & Export: [Various DW_DIR_SMS_* paths...]
   │     └── Assign & Export: DW_HOST_CUSTOMER = "dxcst3.bn.detemobil.de"
   │
   ├── [Conditional: If ORACLE_HOME is Empty]
   │     ├── True Branch: [Conditional: Check Directory Path Existences]
   │     │     ├── If -d "/appl/local/oracle/12.2.0.1.0"
   │     │     │     └── Assign: ORACLE_HOME = "/appl/local/oracle/12.2.0.1.0"
   │     │     ├── Elif -d "/appl/local/oracle/11.2.0"
   │     │     │     └── Assign: ORACLE_HOME = "/appl/local/oracle/11.2.0"
   │     │     └── Else (Directories missing)
   │     │           └── Write to stdout: "Fehler in .dw_init..."
   │     └── Action: Export ORACLE_HOME
   │
   ├── [Action: Source Configurations]
   │     ├── Source: $HOME/.dw_global
   │     └── Source: $HOME/.dw_lokal
   │
   └── [Block: Database Utility Paths]
         └── Assign & Export: DW_DIR_UTL_FILE = "/appl/local/oracle/admin/$ORACLE_SID/utl_file"
```

---

## 6. Pseudo Code

```pascal
// Define primary paths
export DW_DIR_ROOT := HOME + "/aktuell"
export DW_DIR_PROT := HOME + "/daten/logfiles"
export DW_DIR_CUBES := HOME + "/daten/cubes"

// Define Import & Export Mappings
export DW_DIR_IMP_D1 := HOME + "/daten/d1"
export DW_DIR_IMP_BWA := HOME + "/daten/dpps/bwa"
export DW_DIR_IMP_XTRA := HOME + "/daten/xtra"
export DW_DIR_IMP_CTEL := HOME + "/daten/ctel"
export DW_DIR_IMP_VO := HOME + "/daten/vo"
export DW_DIR_IMP_RV := HOME + "/daten/rv"
export DW_DIR_IMP_IF := HOME + "/daten/ees"
export DW_DIR_IMP_NNV := HOME + "/daten/nnv"
export DW_DIR_IMP_SIGMA := HOME + "/daten/gd/sigma"
export DW_DIR_EXP_SIGMA := HOME + "/daten/gd/sigma/export"
export DW_DIR_IMP_TRF := HOME + "/daten/trf"
export DW_DIR_IMP_AUF := HOME + "/daten/sd/auf"
export DW_DIR_IMP_GUT := HOME + "/daten/sd/gut"
export DW_DIR_IMP_KDG := HOME + "/daten/sd/kdg"
export DW_DIR_IMP_MP_KDG := HOME + "/daten/mp/kdg"
export DW_DIR_IMP_MP_TS := HOME + "/daten/mp/ts"
export DW_DIR_IMP_MP_ZM := HOME + "/daten/mp/zm"
export DW_DIR_IMP_TS := HOME + "/daten/sd/ts"
export DW_DIR_IMP_ZM := HOME + "/daten/sd/zm"
export DW_DIR_EXP := HOME + "/daten/exporter"
export DW_DIR_IMP_BPM := HOME + "/daten/bm"
export DW_DIR_IMP_ZTS := HOME + "/daten/zts"
export DW_DIR_IMP_VRS := HOME + "/daten/vrs"
export DW_DIR_IMP_BRUNET := HOME + "/daten/brunet"
export DW_DIR_IMP_DWH := HOME + "/daten/dwh"
export DW_DIR_IMP_PLATO := HOME + "/daten/dwh/plato"
export DW_DIR_IMP_CARMEN := HOME + "/daten/carmen"
export DW_DIR_IMP_SAP := HOME + "/daten/sap"
export DW_DIR_IMP_SR_RV := HOME + "/daten/sap/sr_rv_dpps"
export DW_DIR_IMP_SAP_L := HOME + "/daten/sap/sap_l_gutgr"
export DW_DIR_IMP_L_MAHNSTYP_IST := HOME + "/daten/sap/mahn"
export DW_DIR_IMP_L_MAHNV_FI := HOME + "/daten/sap/mahn"
export DW_DIR_IMP_L_MAHNV_IST := HOME + "/daten/sap/mahn"
export DW_DIR_IMP_L_GUTGR := HOME + "/daten/sd/l_gutschr"
export DW_DIR_IMP_L_LEIST := HOME + "/daten/sd/l_leist"
export DW_DIR_IMP_L_PROD := HOME + "/daten/sd/l_prod"
export DW_DIR_IMP_LKODE := HOME + "/daten/sd/lkode"
export DW_DIR_IMP_SUBSE := HOME + "/daten/subse"

// SMS Directory Configurations
export DW_DIR_SMS_PRG := HOME + "/aktuell/allgemein/is/util"
export DW_DIR_SMS_ADR := HOME + "/daten/sms/adressen"
export DW_DIR_SMS_TMP := HOME + "/daten/sms/tmp"

// DPPS & Planning Configuration
export DW_DIR_IMP_DPPS := HOME + "/daten/dpps"
export DW_DIR_IMP_PLANF2 := HOME + "/daten/planf2"

// Target Remote Host configuration
export DW_HOST_CUSTOMER := "dxcst3.bn.detemobil.de"

// Resolve Oracle Environment
IF IS_EMPTY(ORACLE_HOME) THEN
    IF DIRECTORY_EXISTS("/appl/local/oracle/12.2.0.1.0") THEN
        ORACLE_HOME := "/appl/local/oracle/12.2.0.1.0"
    ELSE IF DIRECTORY_EXISTS("/appl/local/oracle/11.2.0") THEN
        ORACLE_HOME := "/appl/local/oracle/11.2.0"
    ELSE
        PRINT "Fehler in .dw_init:"
        PRINT "   Konnte ORACLE_HOME nicht setzen !"
    END IF
    export ORACLE_HOME
END IF

// Source External Profiles
source HOME + "/.dw_global"
source HOME + "/.dw_lokal"

// Configure Admin Utility Directories
export DW_DIR_UTL_FILE := "/appl/local/oracle/admin/" + ORACLE_SID + "/utl_file"
```

---

## 7. Security Implementations
*   **Security Controls**: Not implemented.
*   **Authentication/Authorization**: Not implemented.
*   **Comments Note**: An execution mask command (`#umask 007`) is commented out in the code and is thus inactive.

---

## 8. Monitoring Implementations
*   **Logs**: Standard output (`stdout`) message notifications are used for reporting missing directory pathways when evaluating `ORACLE_HOME`. No dedicated logging framework, file tracing, or monitoring APIs are implemented.
```

---

## 6. Risks, Manual Actions, & Out-Of-Scope Elements

### Risks & Manual Actions
*   **SOURCE: NOT FOUND** — `.dw_lokal` — *No candidate*. The legacy `.dw_init` script conditionally sources `$HOME/.dw_lokal`. Downstream migration steps must ensure that any host-local scripts are registered under GCS config parameters if used.
*   **SOURCE: NOT FOUND** — `setpya.sh` — *No candidate*. Sourced in `.dw_global` for legacy Cognos deployments. Since BI workloads are decoupled on BigQuery, this file should be flagged as retired.
*   **WIRING ACTION REQUIRED:** Downstream targets `DW.DWH_ABPZ_KKM_AIL_AGENT`, `r_ai_start`, and `/bin/r_ai_start` must have their variables pointed to the new Airflow runtime variables rather than referencing local shell scripts.

### Out-of-Scope Elements
*   **Cognos PowerPlay Configurations:** Legacy configurations mapping to `/appl/local/cognos` are retired.
*   **Oracle Directory Paths (`DW_DIR_UTL_FILE`):** Path definitions for `utl_file` processing are completely retired as BigQuery handles export operations natively via GCS.