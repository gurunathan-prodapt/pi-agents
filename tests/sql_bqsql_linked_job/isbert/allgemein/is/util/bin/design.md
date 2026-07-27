=== FILE: sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh ===
# Zweck:
#    Hilfsroutinen fuer die Benutzung von SQL*Plus
#
# Erzeugt von : TJ 
# Erzeugt am  : 18.02.98
# Aenderung :
# Versions-Anmerkungen:
# $Log$
#   1.1.3; 07.09.98; TJ
#     - Ausgabe der Befehlszeile fuer Logdatei

#
# Oeffentliche Funktionen:
# -----------------------
# starteSQLSkript EntryNr Skript Parameter
#

#Generelle Annahmen: 
#   1.  Fehlerbehandlung ist aktiv

ModulName="alis_sqlplus"
ModulVersion="V1.1.3";


#####################################
# Funktion:
#    starteSQLSkript - Startet ein SQL-Pluskript
#Parameter:
#   I   Fehlereintragsnummer
#   I   Name der Skriptes, welches zu starten ist
#   I   beliebige Anzahl von Parametern fuer das SQLSkript
#Annahmen: (PreConditions)
#   1. Fehlerbehandlung ist aktiv
#Beschreibung:
#   Falls ein SQLSkript nicht als Datei vorhanden ist, meldet
#   ein Aufruf durch SQLPlus kein Fehler.
#   Daher wird vor dem SQLPlus-Aufruf ueberpruft, ob die Datei
#   lesbar ist. Ist dies nicht der Fall, so erfolgt eine
#   Fehlermeldung.
starteSQLSkript(){

    typeset p_Eintragsnr=$1
    typeset p_Skript=$2;
    typeset errcode

    shift 2
    
    if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    then
        DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"
        return 196
    fi

    if [ ! -r $p_Skript ]
    then
        DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript
        return 201
    fi

    echo "Rufe SQL*PLUS auf mit folgenden Einstellungen"
    echo "Sql*Plus-Skript : $p_Skript"
    echo "Skript-Parameter: $*"

    set +e

    sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null

    errcode=$?

    set -e

    return $errcode
}


# DESIGN DOCUMENT: Legacy KornShell to Python 3 Migration

## 1. SCRIPT OVERVIEW
- **Purpose**: This script (`h_alis_sqlplus.ksh`) is a reusable shell utility library providing helper functions for executing Oracle SQL*Plus scripts within a Data Warehouse (DWH) pipeline. Its primary function, `starteSQLSkript`, checks if a target SQL script is readable, logs the invocation details, and executes the script using `sqlplus` with the Oracle connection string/credentials stored in `DW_ORAUSER`.
- **Triggers**: It is sourced and invoked as a library module by other ksh scripts/jobs in the production pipeline.
- **Reads**: 
  - The external SQL script specified as an argument.
  - The environment variable `DW_ORAUSER` (Oracle connection string/credentials).
- **Writes**: 
  - Execution metadata and parameter details to stdout.
  - Error messages via an external utility `DWMSG_MeldeFehler` if input validations fail.

---

## 2. INVOCATION CONTEXT
- **Caller**: Sourced by other parent KornShell scripts or UC4 jobs.
- **UC4 Native Includes**:
  - None referenced in this extraction.
- **Environment Files Sourced**:
  - None explicitly sourced inside this library file, but it relies on a pre-initialized environment containing `DW_ORAUSER` and the `DWMSG_MeldeFehler` executable.

---

## 3. PARAMETERS / INPUTS
The main entry point is the shell function `starteSQLSkript`, which accepts positional arguments:

| Name | Source | Used? | Python Surface | Description |
|---|---|---|---|---|
| `$1` (`p_Eintragsnr`) | Function positional argument | Yes | `p_eintragsnr: str` | Error log entry identifier. Passed to `DWMSG_MeldeFehler`. |
| `$2` (`p_Skript`) | Function positional argument | Yes | `p_skript: str` | Path to the SQL*Plus script to execute. Checked for readability. |
| `$3` onwards (`$*`) | Function positional arguments (after shifting 2) | Yes | `*args: str` | Variadic parameters to pass down to the target SQL script. |
| `DW_ORAUSER` | Environment variable | Yes | `os.environ.get("DW_ORAUSER")` | Oracle DB connection identifier/credentials. |

### Cross-Referenced Parameters
No GDE/Ab Initio environment parameter block is present in this extraction. Only `DW_ORAUSER` is used to establish the DB connection.

---

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The script invokes two external commands:

1. **`DWMSG_MeldeFehler`**
   - **Exact Command Lines**: 
     - `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"`
     - `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript`
   - **Purpose**: Registers/broadcasts standard application-level errors.
   - **Python Translation**: 
     - # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion. Must remain an external call via `subprocess.run` unless an equivalent Python logging/alerting library is available.
     - **Resolvable Launcher**: No.

2. **`sqlplus`**
   - **Exact Command Line**: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
   - **Purpose**: Runs the actual Oracle SQL script. Redirects stdin from `/dev/null` to prevent interactive hangs in background batch processes.
   - **Python Translation**: 
     - Since this is a generic utility function designed to execute arbitrary, unsupplied SQL files, it does not qualify as a "Resolvable Launcher" for a single known SQL block. To maintain 100% compatibility with SQL*Plus syntax features (such as nested `@` calls, `set` commands, formatting variables), this must be invoked via `subprocess.run(["sqlplus", ...], check=False)` to capture and return the exit code exactly as the original shell script does.

---

## 5. EMBEDDED SQL
- No embedded SQL is present in this utility library. It executes external `.sql` scripts dynamically via the target path provided in `p_Skript`.

---

## 6. CONTROL FLOW
The script defines a module and a single function. In Python, this should be structured as a module containing a function `starte_sql_skript`:

1. **Module Level Initialization**:
   - Set script variables: `modul_name = "alis_sqlplus"` and `modul_version = "V1.1.3"`.
   - # REVIEW: Typo in the original shell script where `ModulName="alis_sqlplus"` is defined, but `${Modul_Name}` (with underscore) is referenced in the error message call. Standardize on `modul_name` in Python.
2. **Function Definition**: `def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:`
3. **Validation 1 (Check Missing Parameters)**:
   - Check if `p_eintragsnr` or `p_skript` is empty.
   - If empty, run `DWMSG_MeldeFehler` with error code `196` and return `196`.
4. **Validation 2 (Check File Readability)**:
   - Verify if `p_skript` file exists and is readable.
   - If not readable, run `DWMSG_MeldeFehler` with error code `201` and return `201`.
5. **Log Invocation Details**:
   - Print details of the script path and its arguments to standard output.
6. **SQL*Plus Execution**:
   - Execute `sqlplus` passing `DW_ORAUSER`, the script path prefixed with `@`, and any additional parameters.
   - Ensure stdin is redirected (using `stdin=subprocess.DEVNULL`).
   - Capture the exit code of `sqlplus` (`errcode`).
7. **Return Code**:
   - Return the captured exit code to the caller.

---

## 7. ERROR HANDLING & EXIT CODES
- **Error Detection**: 
  - String validation checks for empty arguments.
  - File readability check using standard shell operators (`[ ! -r $p_Skript ]`).
  - Disabling immediate exit (`set +e`) before running `sqlplus` to capture the exit status `$?` safely, and then re-enabling it (`set -e`).
- **Error Action**:
  - Emits errors via `DWMSG_MeldeFehler` and returns non-zero codes (`196`, `201`, or the exact `sqlplus` exit status).
- **Python Mapping**:
  - Validate parameters directly using standard Python checks (`if not p_eintragsnr or not p_skript`).
  - Verify file readability using `os.access(p_skript, os.R_OK)`.
  - Execute `subprocess.run` with `check=False` to mimic the `set +e` behavior, capture `returncode`, and return it.

---

## 8. OUTPUTS / SIDE EFFECTS
- **Console Output**: Prints SQL*Plus target settings and parameters to stdout.
- **Database Modification**: Executes arbitrary DDL/DML specified in the dynamically executed SQL files.
- **Standard Error Logging**: Initiates error messages via `DWMSG_MeldeFehler` on validation failures.

---

## 9. BUSINESS SUMMARY
- **Standardized Execution**: Centralizes all SQL*Plus executions across the system to maintain consistent parameter handling, logging, and error tracking.
- **Proactive Validation**: Prevents hanging db connections or silent failures by verifying that the target SQL script is accessible and readable *before* attempting a connection.
- **Batch Processing Safety**: Explicitly overrides interactive SQL*Plus inputs with `/dev/null` redirection to ensure batch runs never hang on prompts.
- **Uniform Error Reporting**: Integrates with the company’s enterprise message-logging system (`DWMSG_MeldeFehler`) using uniform error IDs (`196` for configuration/logic issues, `201` for missing files).

---

# PYTHON PSEUDOCODE OUTLINE

```python
import os
import sys
import shutil
import subprocess

# Step 1: Initialize module-level metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


# Step 2: Define helper function to call external error reporter
def _dwmsg_melde_fehler(p_eintragsnr: str, msg_type: str, err_code: int, msg_text: str) -> None:
    """
    Helper to run the external error logger DWMSG_MeldeFehler.
    # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction;
    confirm logging, error propagation, and credential handling before finalizing the conversion.
    """
    cmd = ["DWMSG_MeldeFehler", p_eintragsnr, msg_type, str(err_code), msg_text]
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        print(f"shutil.which warning: DWMSG_MeldeFehler not found in PATH.", file=sys.stderr)
        print(f"Logged Error {err_code} ({msg_type}) for entry {p_eintragsnr}: {msg_text}", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)


# Step 3: Define starte_sql_skript function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
    """
    Python equivalent of 'starteSQLSkript' function.
    Validates script path, logs details, and runs SQL*Plus.
    """
    
    # Step 4: Validate inputs
    if not p_eintragsnr or not p_skript:
        # Note: Resolves the original typo where Modul_Name was used instead of ModulName
        error_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        _dwmsg_melde_fehler(p_eintragsnr, "E", 196, error_msg)
        return 196
        
    # Step 5: Check file existence and readability
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        _dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 6: Log execution metadata
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Fetch Oracle connection credentials from environment
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 7: Execute SQL*Plus with arguments (equivalent to set +e / sqlplus / set -e)
    # Stdin is redirected to subprocess.DEVNULL to prevent hangs (equivalent to </dev/null)
    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
    
    try:
        # check=False is used here to allow capturing and returning the error code natively
        result = subprocess.run(sqlplus_cmd, stdin=subprocess.DEVNULL, check=False)
        errcode = result.returncode
    except FileNotFoundError:
        print("Error: sqlplus executable not found in PATH.", file=sys.stderr)
        return 127  # Standard command-not-found exit code

    # Step 8: Return exit status
    return errcode
```

### Job dependencies
- **Upstream**: none discovered.
- **Downstream**:
  - `DW.BERT_AUSD_V_TA_PERIOD` — not yet migrated. On Cloud Composer, this dependency will be implemented as a cross-DAG trigger or an Airflow sensor once the downstream job is migrated.
  - `abinitio_rpos_carmen_linked_job/isdwh/abinitio/bin/r_ai_start` — not yet migrated. On Cloud Composer, this downstream relationship will be wired via an Airflow task dependency once migrated.
  - *Risk Note*: Since both downstream consumers are currently unmigrated, the scheduling linkage cannot be finalized and must be tracked in Risks.

### Execution order
- none discovered (this is a shared library/utility script containing a helper function; it has no independent task sequence or execution order).

### Scheduling
- none discovered (as a shared utility module, this code is not independently scheduled and is instead imported and called dynamically by other orchestrated tasks).

### Schedule & variables
- none discovered (no legacy schedules or variables must be directly preserved on BigQuery/Cloud Composer for this library script).

### Lineage
- Upstream Producers: none discovered.
- Downstream Consumers: none discovered (beyond the downstream jobs listed under Job Dependencies).

### External system replacements
- **Oracle `sqlplus` to BigQuery Client**:
  - The legacy script executes Oracle SQL*Plus files using `sqlplus`.
  - In the migrated target platform, these SQL scripts are migrated to BigQuery SQL (BQSQL). The Python equivalent will replace `sqlplus` invocation with BigQuery client library queries (`google-cloud-bigquery`) or Cloud Composer’s standard `BigQueryInsertJobOperator`.
  - Standard input redirection (`</dev/null`) is obsolete under Python-driven API executions.

### Cross-file dependencies
- Other KornShell jobs source this file to utilize the `starteSQLSkript` logic. In the target environment, this will be imported as a shared Python module located at `sql_bqsql_linked_job.isbert.allgemein.is.util.bin.h_alis_sqlplus`.

### Target file plan
- **Target File Path**: `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.py`
- **Source File Path**: `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- **Language**: Python 3
- **Purpose**: Defines the shared function `starte_sql_skript` which validates SQL file readability, outputs standard metadata, and executes BigQuery queries using the client API.
- **Pseudocode Outline**:
  ```python
  import os
  import sys
  import logging
  from google.cloud import bigquery

  MODUL_NAME = "alis_sqlplus"
  MODUL_VERSION = "V1.1.3"

  def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
      # Step 1: Input Validation
      if not p_eintragsnr or not p_skript:
          logging.error(f"Error 196: {MODUL_NAME} {MODUL_VERSION} starteSQLSkript")
          return 196

      # Step 2: Check File Readability
      if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
          logging.error(f"Error 201: {p_skript} is not readable")
          return 201

      # Step 3: Log metadata (Preserving original German output literals verbatim)
      print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
      print(f"Sql*Plus-Skript : {p_skript}")
      print(f"Skript-Parameter: {' '.join(args)}")

      # Step 4: SQL Execution (BigQuery replacement)
      try:
          client = bigquery.Client()
          with open(p_skript, 'r', encoding='utf-8') as f:
              query_text = f.read()
          
          # Run query on BigQuery
          query_job = client.query(query_text)
          query_job.result()  # Wait for execution to complete
          errcode = 0
      except Exception as e:
          logging.error(f"BigQuery execution failed: {e}")
          errcode = 1

      return errcode
  ```

### Environment-specific values
- **`GCP_PROJECT`** (GLOBAL): Environment-wide GCP project ID. In Python, sourced at runtime via `os.environ.get("GCP_PROJECT")` or Cloud Composer Airflow variables `Variable.get("GCP_PROJECT")`.
- **`BQ_LOCATION`** (GLOBAL): Standard BigQuery processing location (e.g., `EU` or `US`). Sourced via `Variable.get("BQ_LOCATION")`.
- **`DW_ORAUSER`** (GLOBAL): Legacy Oracle credentials variable. If legacy Oracle DB connectivity must be temporarily retained instead of migrating straight to BigQuery, this will resolve to an Airflow Connection ID (`oracle_default`) rather than storing hardcoded credentials.

### Risks and manual steps
- **Downstream Migration Dependency**: Downstream jobs `DW.BERT_AUSD_V_TA_PERIOD` and `abinitio_rpos_carmen_linked_job/isdwh/abinitio/bin/r_ai_start` are not yet migrated. Scheduling links cannot be established or verified.
- **Unresolved Component `DWMSG_MeldeFehler`**:
  SOURCE: NOT FOUND — DWMSG_MeldeFehler — no candidate
  *Manual Action*: This is a custom enterprise logging executable. It must be manually replaced by standard Python logging or integrated into a custom GCP logging operator.
- **SQL Compatibility (Class B4 Redesign)**: The legacy script is designed to execute Oracle SQL*Plus scripts. If these scripts use proprietary SQL*Plus directives (e.g., `SET PAGESIZE`, `COLUMN FORMAT`) or PL/SQL blocks, they must be manually refactored to BigQuery SQL (BQSQL) before they can be executed by this Python function.

---

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.py` | Migrated to a Python module at the mirrored directory path (Folder Integrity Rule) to maintain validation checks and convert query execution to BigQuery. |