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


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script defines a custom utility function with file-readability validation, positional parameter shifting, and SQL*Plus external command execution, which must be converted to Python.

EVIDENCE
- Business logic found: KSH custom logic in `starteSQLSkript` verifies positional parameters, validates target SQL script file readability, and triggers external SQL*Plus execution with argument passing.
- AWK: none
- SQL-expressible: no, contains file-system state operations and dynamic SQL*Plus process management.
- Non-SQL side effects: performs local file-readability checks (`[ ! -r $p_Skript ]`), redirects process stdin from `/dev/null`, and interfaces with external error-messaging utility `DWMSG_MeldeFehler`.
- Against this verdict: none, as this is a structural shell helper utility that executes arbitrary dynamically-passed script paths rather than a specific static database transformation.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a KornShell utility module (`h_alis_sqlplus.ksh`) that provides a centralized helper function `starteSQLSkript` for launching Oracle SQL*Plus scripts. It performs validation on parameters and target file readability, executes the SQL*Plus CLI tool non-interactively, and handles exit codes. It acts as an orchestration and error-handling layer rather than running static SQL transformations.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced or executed by parent job scripts within UC4/Automic processes. The exact calling JOBS_UNIX object is not specified in the current extraction.
   - Any UC4 native includes: None referenced in this extraction.
   - Environment files sourced: None. It expects parent environments to define execution parameters such as `DW_ORAUSER` and utility commands like `DWMSG_MeldeFehler`.

3. PARAMETERS / INPUTS
   - Name: `$p_Eintragsnr` ($1)
     - Source: Positional argument when invoking `starteSQLSkript`
     - Used: Yes, validated to be non-empty and forwarded to `DWMSG_MeldeFehler` on validation failures.
     - Surfaced in Python: Function parameter `p_eintragsnr` (string).
   - Name: `$p_Skript` ($2)
     - Source: Positional argument when invoking `starteSQLSkript`
     - Used: Yes, checked for readability and passed to `sqlplus`.
     - Surfaced in Python: Function parameter `p_skript` (string).
   - Name: `$*` (remaining trailing arguments after shifting by 2)
     - Source: Forwarded from caller.
     - Used: Yes, appended as positional arguments to the SQL*Plus CLI command.
     - Surfaced in Python: Variadic argument list `*args`.
   - Name: `DW_ORAUSER`
     - Source: Environment variable.
     - Used: Yes, used as the credentials/connect identifier for `sqlplus`.
     - Surfaced in Python: `os.environ.get("DW_ORAUSER")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - Exact command line: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
   - Purpose: Executes the target SQL script (`$p_Skript`) non-interactively with the Oracle SQL*Plus engine using the credentials specified in `DW_ORAUSER` and passing through any trailing runtime parameters.
   - Python counterpart: Must remain an external process invocation via `subprocess.run` because this function dynamically loads and executes arbitrary files determined at runtime.
   - Resolvable Launcher: No. The target script file path is dynamic and its SQL content is not supplied, so this cannot be resolved to a static Python DB-client call.

5. EMBEDDED SQL
   - No static SQL exists in this utility script. It executes dynamic external SQL files on demand.

6. CONTROL FLOW
   1. Environment setup / Variable declaration:
      - Sets module metadata: `ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`.
   2. Function definition:
      - Defines `starteSQLSkript` receiving arguments.
   3. Parameter validation:
      - Shifts positional parameters by 2 to isolate SQL script arguments.
      - Checks if `$p_Eintragsnr` or `$p_Skript` is empty. If so, calls `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"` and returns exit code 196.
   4. File readability check:
      - Verifies that `$p_Skript` is readable. If not, calls `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript` and returns exit code 201.
   5. Output/logging:
      - Outputs script details and arguments to stdout.
   6. SQL*Plus execution:
      - Disables exit-on-error (`set +e`).
      - Invokes `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`.
      - Captures exit status `$?` into `errcode`.
      - Restores exit-on-error (`set -e`).
      - Returns `errcode` to the calling parent shell.

7. ERROR HANDLING & EXIT CODES
   - How script detects failure: Empty string parameter checks, file existence and readability (`[ ! -r $p_Skript ]`), and the exit code of the spawned `sqlplus` command.
   - Action on failure: Logs the event using the external helper `DWMSG_MeldeFehler` and returns:
     - `196`: Invalid/missing parameters.
     - `201`: Target SQL script file is unreadable.
     - `errcode`: Propagated from SQL*Plus execution status.
   - Success exit code: `0` (or whatever `sqlplus` returns for a successful execution).
   - Python mapping:
     - Use standard conditional guards and raise exceptions or return specific integer codes.
     - Execute `sqlplus` using `subprocess.run(..., check=False)` to capture its `returncode` without raising an unhandled exception, matching the original `set +e` logic.

8. OUTPUTS / SIDE EFFECTS
   - Writes logging info to stdout.
   - Database operations executed inside the downstream SQL files (dependent on the target SQL scripts).

9. BUSINESS SUMMARY
   - Provides a standard, reusable utility wrapper for executing SQL*Plus scripts safely.
   - Protects against runtime crashes by checking that target SQL script files physically exist and are readable before executing them.
   - Integrates with a centralized corporate message and logging framework (`DWMSG_MeldeFehler`) to record failures.
   - Prevents SQL*Plus from hanging in interactive terminal states by redirecting input from an empty source (`/dev/null`).

=== PSEUDOCODE STYLE ===

```python
import os
import sys
import subprocess
import pathlib

# Step 1: Initialize module-level variables
# # REVIEW: The original shell script defines variables 'ModulName' and 'ModulVersion'
# but calls them in DWMSG_MeldeFehler as 'Modul_Name' and 'Modul_Version' (with underscores).
# This is a likely bug in the legacy KSH code. Correcting this to use matching Python names.
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# # REVIEW-STRUCT: external function DWMSG_MeldeFehler not supplied — behavior/implementation unknown.
# Define a placeholder function mapping to the external DWMSG_MeldeFehler interface.
def dwmsg_melde_fehler(p_eintragsnr: str, severity: str, error_id: int, message_or_arg: str):
    # Implement logging pipeline call or subprocess wrapper for DWMSG_MeldeFehler in target system.
    pass

# Step 2: Define the main starte_sql_skript utility function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    
    # Step 3: Validate input parameters
    # Equivalent to: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript")
        return 196

    # Step 4: Check if SQL script exists and is readable
    # Equivalent to: if [ ! -r $p_Skript ]
    script_path = pathlib.Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 5: Log execution details
    # Equivalent to: echo "Rufe SQL*PLUS auf mit folgenden Einstellungen" ...
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Invoke SQL*Plus external client
    # # REVIEW: target database platform assumed as Oracle via sqlplus client; connection details (DW_ORAUSER) are fetched from environment.
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    
    # Construct command equivalent to: sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    try:
        # Note: stdin=subprocess.DEVNULL mimics </dev/null
        # check=False mimics the 'set +e' and 'set -e' environment state around sqlplus
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            text=True,
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"CRITICAL: Failed to spawn sqlplus process: {e}", file=sys.stderr)
        errcode = 1

    # Step 7: Return SQL*Plus exit status
    return errcode
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.py` | Migrates the KornShell utility to a Python module, translating positional parameter shifting, file readability validations, and dynamic process execution into native Python logic. |

---

# Additional Context (Not Visible to MCP)

### Job Dependencies
* **Downstream Jobs**:
  * `DW.BERT_AUSD_V_TA_PERIOD` (not yet migrated)
  * `r_ai_start` (not yet migrated)
* **Wiring on BigQuery / Cloud Composer**:
  * Since both downstream dependencies are not yet migrated, their downstream execution linkages cannot be finalized. Once migrated to Airflow DAGs, these linkages must be wired using Airflow `TriggerDagRunOperator` or dataset-based scheduling (`Dataset` URI triggers). 

### Lineage
* **Downstream Consumers (Cross-Job Hand-offs)**:
  * Hand-off to `DW.BERT_AUSD_V_TA_PERIOD` (job: `DW.BERT_AUSD_V_TA_PERIOD`)
  * Hand-off to `r_ai_start` (job: `r_ai_start`)

### External System Replacements
* **Oracle SQL*Plus (`sqlplus`) Replacement**:
  * The legacy utility triggers dynamic SQL*Plus executions via the CLI. On the BigQuery target platform, executing dynamic SQL*Plus commands is obsolete.
  * In the target environment, the dynamic SQL scripts must be translated into BigQuery SQL / Stored Procedures. The Python wrapper should be updated to utilize the `google.cloud.bigquery` client library to run these queries (e.g., using `bigquery.Client().query()`) instead of triggering raw OS subprocesses.

### Cross-File Dependencies
* This script is a shared utility module (`h_alis_sqlplus.ksh`). In the legacy shell environment, parent KSH scripts source this file to call `starteSQLSkript`. 
* On the target Python platform, any migrated Python operator or scripts that require this functionality must import the function from this target file:
  ```python
  from sql_bqsql_linked_job.isbert.allgemein.is.util.bin.h_alis_sqlplus import starte_sql_skript
  ```

### Target File Plan
* **Target Path**: `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.py`
  * **Language**: Python
  * **Source**: `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh`
  * **Note**: No code is rewritten or duplicated here; the automatic attachment contains the authoritative implementation.

### Environment-Specific Values

1. **`DW_ORAUSER`**
   * **Classification**: GLOBAL (environment-wide infrastructure credential).
   * **Target Mechanism**: Under a native BigQuery execution model, credentials are managed via GCP Service Accounts (IAM) and Application Default Credentials (ADC). If external transactional database connections are still required (e.g., Cloud SQL), connection configurations must be retrieved securely from Google Cloud Secrets Manager or Airflow Connection definitions rather than environment variables.
2. **`GCP_PROJECT`**
   * **Classification**: GLOBAL (environment-wide).
   * **Target Mechanism**: Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow's environment configuration.
3. **`BQ_DATASET`**
   * **Classification**: GLOBAL (environment-wide).
   * **Target Mechanism**: Sourced at runtime via `os.environ.get("BQ_DATASET")` or Airflow's environment configuration.

---

# Risks & Manual Steps

* **UNRESOLVED COMPONENT**:
  * `SOURCE: NOT FOUND — DWMSG_MeldeFehler — no candidate`
  * **Risk**: The external error logging utility `DWMSG_MeldeFehler` is invoked to log error messages (Error codes 196 and 201), but its source code and implementation are not supplied. 
  * **Mitigation**: A placeholder function `dwmsg_melde_fehler` is introduced. Downstream developers must map this placeholder to the target platform's logging framework, such as Google Cloud Logging or standard Python `logging`.
* **UNMIGRATED DOWNSTREAMS**:
  * `DOWNSTREAM: NOT MIGRATED — DW.BERT_AUSD_V_TA_PERIOD`
  * `DOWNSTREAM: NOT MIGRATED — r_ai_start`
  * **Risk**: Orchestration hooks cannot be tested or completely configured until these downstream systems are fully migrated.
* **SQL*Plus Features & Syntax Non-Compatibility**:
  * **Risk**: The scripts triggered by this utility may contain Oracle-specific PL/SQL syntax or SQL*Plus command-line instructions (e.g., `SET SERVEROUTPUT ON`, `WHENEVER SQLERROR EXIT`). These will fail on BigQuery.
  * **Mitigation**: The dynamic SQL scripts themselves must be systematically translated to BigQuery SQL, and the python helper refactored to execute them via the BigQuery Client API.