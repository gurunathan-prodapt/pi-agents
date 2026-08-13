=== FILE: vobs/dw_source/istools/seu/template/.dw_global ===
#! /bin/ksh
#                               -*- Mode: Sh -*- 
# .dw_global --- Alle GLOBALEN Variablen setzen
# Autor               : Thomas Bregulla
# Erzeugt am          : Thu Feb 19 12:51:23 1998
# Letzte Aenderung von: Karen Bisseling
# Letzte Aenderung am : Mon Oct  5 16:58:58 1998
# Status              : Unbekannt, bitte Vorsicht!
# $Id$
# $Locker$
# Versions-Anmerkungen
# $Log$
# 

#! /bin/ksh
#######################
#
# Beschreibung:
#     Globale Parameter werden abhaengig von den Einstiegspunkten gesetzt
#
# Datei wird ausschliesslich von dwh_init aufgerufen


#######################
# Pruefung der Umgebung
#
fehler=""

if [ -z "$DW_DIR_ROOT" ]
then
    fehler="$fehler DW_DIR_ROOT "
fi
if [ -z "$DW_DIR_PROT" ]
then
    fehler="$fehler DW_DIR_PROT "
fi
if [ -z "$DW_DIR_CUBES" ]
then
    fehler="$fehler DW_DIR_CUBES "
fi
if [ -z "$DW_DIR_IMP_D1" ]
then
    fehler="$fehler DW_DIR_IMP_D1 "
fi
if [ -z "$DW_DIR_IMP_XTRA" ]
then
    fehler="$fehler DW_DIR_IMP_XTRA"
fi
if [ -z "$DW_DIR_IMP_CTEL" ]
then
    fehler="$fehler DW_DIR_IMP_CTEL"
fi
if [ -z "$ORACLE_HOME" ]
then
    fehler="$fehler ORACLE_HOME"
fi

if [ ! -z "$fehler" ]
then
    echo "Fehler in .dw_global:"
    for varname in $fehler
    do
        echo "   Umgebungsvariable $varname ist nicht gesetzt !"
    done

    echo "Breche ab .."
fi

###################################
# Setzen von abhaengigen Parametern
#

# Pfade
LD_LIBRARY_PATH=${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}; export LD_LIBRARY_PATH
PATH="$PATH:$ORACLE_HOME/bin:"; export PATH

#SQL-Pfade setzen

# SQL-Net 2 Connections
NLS_LANG=GERMAN_GERMANY.WE8ISO8859P1; export NLS_LANG
NLS_DATE_FORMAT=DD-MON-YY; export NLS_DATE_FORMAT
NLS_DATE_LANGUAGE=AMERICAN; export NLS_DATE_LANGUAGE


# Cognos PowerPlay...
if [ -f  /appl/local/cognos/cognos5.2/pya52b17/setpya.sh ]
then
	. /appl/local/cognos/cognos5.2/pya52b17/setpya.sh
fi

#################################################
# PATH
#################################################
# find $HOME/projekt/aktuell/dw_source -name bin -print
#export PATH=$DW_DIR_ROOT/allgemein/is/util/bin:$DW_DIR_ROOT/allgemein/sd/util/bin:$DW_DIR_ROOT/allgemein/tn/util/bin:$DW_DIR_ROOT/aufbereitung/is/bin:$DW_DIR_ROOT/aufbereitung/sd/bin:$DW_DIR_ROOT/aufbereitung/tn/bin:$DW_DIR_ROOT/import/is/bin:$DW_DIR_ROOT/import/sd/bin:$DW_DIR_ROOT/import/tn/bin:$DW_DIR_ROOT/pruef/is/bin:$DW_DIR_ROOT/pruef/sd/bin:$DW_DIR_ROOT/pruef/tn/bin:$DW_DIR_ROOT/../istools/bin:$DW_DIR_ROOT/verdichtung/is/bin:$DW_DIR_ROOT/verdichtung/sd/bin:$DW_DIR_ROOT/verdichtung/tn/bin:$DW_DIR_ROOT/olap/is/bin:$PATH

################################################
# SQLPATH
################################################
# find $HOME/projekt/aktuell/dw_source -name sql -print
#export SQLPATH=$DW_DIR_ROOT/allgemein/is/util/sql:$DW_DIR_ROOT/allgemein/sd/util/sql:$DW_DIR_ROOT/allgemein/tn/util/sql:$DW_DIR_ROOT/aufbereitung/is/sql:$DW_DIR_ROOT/aufbereitung/sd/sql:$DW_DIR_ROOT/aufbereitung/tn/sql:$DW_DIR_ROOT/import/is/sql:$DW_DIR_ROOT/import/sd/sql:$DW_DIR_ROOT/import/tn/sql:$DW_DIR_ROOT/pruef/is/sql:$DW_DIR_ROOT/pruef/sd/sql:$DW_DIR_ROOT/pruef/tn/sql:$DW_DIR_ROOT/../istools/sql:$DW_DIR_ROOT/verdichtung/is/sql:$DW_DIR_ROOT/verdichtung/sd/sql:$DW_DIR_ROOT/verdichtung/tn/sql:$SQLPATH


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script performs environment parameter validation, path manipulations, and conditional sourcing of external configuration files.

EVIDENCE
- Business logic found: KSH custom logic validates that seven required directory and database path environment variables are set, performs string manipulation to assemble error lists, sets localization/database configurations (NLS_LANG, etc.), and conditionally sources a Cognos setup script if present.
- AWK: none
- SQL-expressible: no, this script manages OS-level environment variables, paths, and script sourcing.
- Non-SQL side effects: Modifies environment variables (PATH, LD_LIBRARY_PATH, NLS settings) and checks file existence on the host filesystem.
- Against this verdict: One could argue that global environment setup scripts do not require conversion to Python since cloud orchestration handles environment settings, but because of the custom validation logic and conditional sourcing, mapping it to Python ensures robust setup execution.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The `.dw_global` script is an environment initialization and configuration script. It is designed to be sourced (exclusively by `dwh_init`) to validate that key environment variables are set, configure runtime search paths (PATH and LD_LIBRARY_PATH), define Oracle database session localization parameters (NLS_LANG, NLS_DATE_FORMAT, NLS_DATE_LANGUAGE), and conditionally execute a Cognos PowerPlay configuration script if present.

### 2. INVOCATION CONTEXT
- **Sourced by**: `dwh_init` (exclusively, as stated in the script comments: `"Datei wird ausschliesslich von dwh_init aufgerufen"`).
- **Sourced via**: `. .dw_global` or `source .dw_global`
- **UC4 Includes**: None referenced in this extraction.
- **Environment files sourced**:
  - `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` (sourced conditionally if it exists).
  - # REVIEW-STRUCT: environment file [/appl/local/cognos/cognos5.2/pya52b17/setpya.sh] not supplied — variables it sets are unknown; do not guess their names or values

### 3. PARAMETERS / INPUTS
The script does not accept command-line parameters ($1, $2, etc.). Instead, it validates and interacts with the following OS environment variables:

| Variable Name | Source | Used in Script? | Surfaced in Python |
|---|---|---|---|
| `DW_DIR_ROOT` | OS Environment | Yes (Validated) | `os.environ.get("DW_DIR_ROOT")` |
| `DW_DIR_PROT` | OS Environment | Yes (Validated) | `os.environ.get("DW_DIR_PROT")` |
| `DW_DIR_CUBES` | OS Environment | Yes (Validated) | `os.environ.get("DW_DIR_CUBES")` |
| `DW_DIR_IMP_D1` | OS Environment | Yes (Validated) | `os.environ.get("DW_DIR_IMP_D1")` |
| `DW_DIR_IMP_XTRA` | OS Environment | Yes (Validated) | `os.environ.get("DW_DIR_IMP_XTRA")` |
| `DW_DIR_IMP_CTEL` | OS Environment | Yes (Validated) | `os.environ.get("DW_DIR_IMP_CTEL")` |
| `ORACLE_HOME` | OS Environment | Yes (Validated & Read) | `os.environ.get("ORACLE_HOME")` |
| `LD_LIBRARY_PATH` | OS Environment | Yes (Modified) | `os.environ.get("LD_LIBRARY_PATH")` |
| `PATH` | OS Environment | Yes (Modified) | `os.environ.get("PATH")` |

*Note: Commented-out lines in the script suggest past usage of `DW_DIR_ROOT` to construct PATH and SQLPATH variables, but these are currently disabled.*

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **Sourcing command**: `. /appl/local/cognos/cognos5.2/pya52b17/setpya.sh`
  - **Purpose**: Sourced to initialize Cognos PowerPlay environment variables.
  - **Mapping**: Since this is a sourced shell script, its environment modifications cannot be directly inherited by a Python subprocess unless executed in the same shell session. It should remain as a subprocess invocation or be translated into native environment modifications if the variables it exports are discovered.
  - # REVIEW-STRUCT: environment file [/appl/local/cognos/cognos5.2/pya52b17/setpya.sh] not supplied — variables it sets are unknown; do not guess their names or values

### 5. EMBEDDED SQL
None.

### 6. CONTROL FLOW
1. **Initialize Error Tracker**: Set `fehler=""` to track missing variables.
2. **Validate Required Environment Variables**:
   - Check if `DW_DIR_ROOT` is unset/empty; if so, append `" DW_DIR_ROOT "` to `fehler`.
   - Check if `DW_DIR_PROT` is unset/empty; if so, append `" DW_DIR_PROT "` to `fehler`.
   - Check if `DW_DIR_CUBES` is unset/empty; if so, append `" DW_DIR_CUBES "` to `fehler`.
   - Check if `DW_DIR_IMP_D1` is unset/empty; if so, append `" DW_DIR_IMP_D1 "` to `fehler`.
   - Check if `DW_DIR_IMP_XTRA` is unset/empty; if so, append `" DW_DIR_IMP_XTRA"` to `fehler`.
   - Check if `DW_DIR_IMP_CTEL` is unset/empty; if so, append `" DW_DIR_IMP_CTEL"` to `fehler`.
   - Check if `ORACLE_HOME` is unset/empty; if so, append `" ORACLE_HOME"` to `fehler`.
3. **Handle Errors**: If `fehler` is not empty:
   - Print `"Fehler in .dw_global:"`.
   - Loop through each variable name in `fehler` and print `"   Umgebungsvariable $varname ist nicht gesetzt !"`.
   - Print `"Breche ab .."`.
4. **Append Paths**:
   - Prepend `${ORACLE_HOME}/lib` to `LD_LIBRARY_PATH`.
   - Append `$ORACLE_HOME/bin:` to `PATH`.
5. **Set Database Session Environment Variables**:
   - Export `NLS_LANG=GERMAN_GERMANY.WE8ISO8859P1`
   - Export `NLS_DATE_FORMAT=DD-MON-YY`
   - Export `NLS_DATE_LANGUAGE=AMERICAN`
6. **Sourcing Cognos Script**:
   - Check if `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` exists as a regular file.
   - If it exists, source it (`. /appl/local/cognos/cognos5.2/pya52b17/setpya.sh`).

### 7. ERROR HANDLING & EXIT CODES
- **Failure Detection**: The script checks if any critical environment variables are empty. If so, it prints error messages to stdout.
- **Exit/Abort Behavior**: It prints `"Breche ab .."` (breaking off/aborting). However, since it is a sourced shell script (`.dw_global`), it does not call `exit` to avoid terminating the user's active login session/parent terminal. 
- **Python Mapping**: In Python, environment validation failures should raise an explicit `EnvironmentError` or `ValueError` to prevent execution of downstream ETL stages.

### 8. OUTPUTS / SIDE EFFECTS
- Modifies the environment variables of the running process (`os.environ`).

### 9. BUSINESS SUMMARY
- Validates the presence of essential data warehouse directory paths and Oracle database paths before script executions.
- Sets dynamic environment configurations, path resolutions, and localization/globalization parameters for Oracle database interactions.
- Ensures Cognos reporting software integrations are initialized if the relevant configuration script is physically present on the application server.

### PSEUDOCODE OUTLINE

```python
import os
import sys
import pathlib

# Step 1: Initialize error tracker
fehler = []

# Step 2: Validate required environment variables
required_vars = [
    "DW_DIR_ROOT",
    "DW_DIR_PROT",
    "DW_DIR_CUBES",
    "DW_DIR_IMP_D1",
    "DW_DIR_IMP_XTRA",
    "DW_DIR_IMP_CTEL",
    "ORACLE_HOME"
]

for varname in required_vars:
    if not os.environ.get(varname):
        fehler.append(varname)

# Step 3: Handle validation errors
if fehler:
    print("Fehler in .dw_global:", file=sys.stderr)
    for varname in fehler:
        print(f"   Umgebungsvariable {varname} ist nicht gesetzt !", file=sys.stderr)
    print("Breche ab ..", file=sys.stderr)
    raise EnvironmentError(f"Required environment variables are not set: {', '.join(fehler)}")

# Step 4: Prepend/Append dependent paths
oracle_home = os.environ["ORACLE_HOME"]

# Update LD_LIBRARY_PATH
ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
os.environ["LD_LIBRARY_PATH"] = f"{oracle_home}/lib:{ld_library_path}"

# Update PATH
path = os.environ.get("PATH", "")
os.environ["PATH"] = f"{path}:{oracle_home}/bin:"

# Step 5: Export database session locale variables
os.environ["NLS_LANG"] = "GERMAN_GERMANY.WE8ISO8859P1"
os.environ["NLS_DATE_FORMAT"] = "DD-MON-YY"
os.environ["NLS_DATE_LANGUAGE"] = "AMERICAN"

# Step 6: Conditionally source Cognos setup script
cognos_script_path = pathlib.Path("/appl/local/cognos/cognos5.2/pya52b17/setpya.sh")
if cognos_script_path.is_file():
    # REVIEW-STRUCT: environment file [/appl/local/cognos/cognos5.2/pya52b17/setpya.sh] not supplied — variables it sets are unknown; do not guess their names or values
    # In a real execution, we would source the script in a shell subprocess and extract its exports,
    # or migrate those variables to a static python config once they are known.
    pass
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/istools/seu/template/.dw_global` | `vobs/dw_source/istools/seu/template/dw_global.py` | Converted from a KSH environment setup shell script to a Python module to validate and manage global runtime environment variables in the Cloud Composer environment. |

### Job Dependencies
- **Downstream Jobs** (all marked "not yet migrated"):
  - `DW.BERT_ABLAUFSTEUERUNG`
  - `DW.BERT_AUSD_BP_TA_MSISDN`
  - `DW.BERT_AUSD_BP_TA_P_BASISPROD`
  - `DW.BERT_AUSD_V_TA_PERIOD`
  - `DW.BERT_AUSD_V_TA_P_VERTRAG`
  - `DW.BERT_AUSD_V_TA_VERTRAG_TMP`
  - `DW.BERT_DROP_TEMP_TABLE`
  - `DW.BERT_P_ADRESSEN`
  - `DW.BERT_P_AUSTAUSCH`
  - `DW.BERT_P_GESCHAEFTSP`
  - `DW.BERT_P_RECH_EMPF`
  - `DW.BERT_RECHNUNGSDATEN`
  - `DW.CRS_VERFUEGBAR_JA_NEIN_PF_JOB_FUER_BERT`
  - `DW.DWH_EXIS_SD_APT_BESTANDS`
  - `DW.DWH_EXIS_SD_APT_RABATT`

  These downstream jobs consume this job's variables and environment configurations. Since they are not yet migrated, the final integration wiring cannot be completed. Once migrated, they will import the converted `dw_global.py` or use its environment mappings within their own Cloud Composer DAG tasks.

### Scheduling
- This job is not directly triggered by any scheduler; instead, it is an include/shared module sourced by other runtime jobs. The migrated Python module should be deployed to a shared DAGs or utility folder (such as the `plugins/` or a shared `utils/` folder in Cloud Composer) where it can be imported as a callable/importable Python module by other jobs. Do not schedule this module independently.

### Lineage
- **Upstream Files**: None.
- **Downstream Consumers**: Invoked/sourced by `dwh_init` (external shell setup script).
- **External Sourced Components**: Sources `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` if it is present on the filesystem.

### External System Replacements
- **Oracle Client & NLS Environment**: Legacy Oracle environmental variables (`ORACLE_HOME`, `LD_LIBRARY_PATH`) and locale session variables (`NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`) are retired. BigQuery handles its own character encoding (UTF-8) and date/time formatting natively.
- **Cognos BI Integration**: Sourcing `setpya.sh` initializes Cognos. If Cognos reporting is retired or replaced (e.g. by Looker) on Google Cloud, this integration logic is retired. If Cognos is retained, these configurations must be translated into Airflow variables or Cloud Composer environment configurations.

### Cross-File Dependencies
- This script is tightly coupled to `dwh_init`, which must import or execute `dw_global.py` to correctly initialize variables before executing ETL steps.

### Target File Plan
- `vobs/dw_source/istools/seu/template/dw_global.py` (Source: `vobs/dw_source/istools/seu/template/.dw_global`): A Python utility module that validates the existence of global environment variables and updates the current process environment. In case of validation failure, it prints the original German logging statements verbatim:
  - `"Fehler in .dw_global:"`
  - `"   Umgebungsvariable {varname} ist nicht gesetzt !"`
  - `"Breche ab .."`

### Environment-Specific Values
The following are classified as **GLOBAL** environment-wide variables:
- `DW_DIR_ROOT`: GLOBAL. Sourced in Python via `os.environ.get("DW_DIR_ROOT")`.
- `DW_DIR_PROT`: GLOBAL. Sourced in Python via `os.environ.get("DW_DIR_PROT")`.
- `DW_DIR_CUBES`: GLOBAL. Sourced in Python via `os.environ.get("DW_DIR_CUBES")`.
- `DW_DIR_IMP_D1`: GLOBAL. Sourced in Python via `os.environ.get("DW_DIR_IMP_D1")`.
- `DW_DIR_IMP_XTRA`: GLOBAL. Sourced in Python via `os.environ.get("DW_DIR_IMP_XTRA")`.
- `DW_DIR_IMP_CTEL`: GLOBAL. Sourced in Python via `os.environ.get("DW_DIR_IMP_CTEL")`.
- `ORACLE_HOME`: GLOBAL (legacy). Sourced in Python via `os.environ.get("ORACLE_HOME")`.
- `LD_LIBRARY_PATH`: GLOBAL (legacy). Sourced in Python via `os.environ.get("LD_LIBRARY_PATH")`.
- `PATH`: GLOBAL. Sourced in Python via `os.environ.get("PATH")`.
- `NLS_LANG`: GLOBAL (legacy). Hardcoded default `"GERMAN_GERMANY.WE8ISO8859P1"`.
- `NLS_DATE_FORMAT`: GLOBAL (legacy). Hardcoded default `"DD-MON-YY"`.
- `NLS_DATE_LANGUAGE`: GLOBAL (legacy). Hardcoded default `"AMERICAN"`.

*There are no job-specific variables in this configuration file.*

### Risks and Manual Steps
- SOURCE: NOT FOUND — SETPYA.SH — no candidate
- **Downstream Migration Gaps**: Fifteen downstream consuming jobs are not yet migrated, which prevents full integration testing of this environment utility.
- **Cognos Integration Sourcing**: Sourcing `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` represents an unresolved dependency. A human developer must investigate what variables this script initializes and verify whether Cognos remains active in the target GCP environment.
- **Oracle Variable Cleanup**: If the target platform only writes and reads from BigQuery and has no legacy Oracle connectivity requirements, the Oracle client paths and NLS variable exports should be safely pruned.

---

=== FILE: vobs/dw_source/istools/seu/template/.dw_init ===
#! /bin/ksh
#                               -*- Mode: Sh -*- 
# .dw_init --- Initialisierung fuer Information Services
# Autor               : Thomas Bregulla
# Erzeugt am          : Thu Feb 19 12:53:26 1998
# Letzte Aenderung von: Karen Bisseling
# Letzte Aenderung am : Tue Aug 25 10:26:25 1998
# Status              : Unbekannt, bitte Vorsicht!
# $Id$
# $Locker$
# Versions-Anmerkungen
# $Log$
# 


############################################################
#
# Diese Datei setzt alle notwendigen Umgebungsvariablen fuer
# den Betrieb des Information Services
# 


#########################################
# Einstiegspunke fuer Information Service
#
# DW_DIR_ROOT  : Ausgangsverzeichnis fuer alle Skripte 
#                (z.B. /vobs/dw_source) 
# DW_DIR_PROT  : Protokollverzeichnis in denen Logs, etc. abgelegt werden 
#                (z.B. /vobs/dw_source/daten/protokoll)
# DW_DIR_CUBES : Datenverzeichnis in denen die Wuerfel abgelegt werden 
#                (z.B. /vobs/dw_source/daten/cubes)
# DW_DIR_IMP_<XX>  : Datenverzeichnis der einzelnen Importerkennungen
#                (z.B. ~isctelim/daten)
#

#DW_DIR_ROOT=$HOME; export DW_DIR_ROOT
DW_DIR_ROOT=$HOME/aktuell; export DW_DIR_ROOT

#
DW_DIR_PROT=$HOME/daten/logfiles; export DW_DIR_PROT
DW_DIR_CUBES=$HOME/daten/cubes; export DW_DIR_CUBES
DW_DIR_IMP_D1=$HOME/daten/d1; export DW_DIR_IMP_D1
DW_DIR_IMP_XTRA=$HOME/daten/xtra; export DW_DIR_IMP_XTRA
DW_DIR_IMP_CTEL=$HOME/daten/ctel; export DW_DIR_IMP_CTEL
DW_DIR_IMP_VO=$HOME/daten/vo; export DW_DIR_IMP_VO
DW_DIR_IMP_RV=$HOME/daten/rv; export DW_DIR_IMP_RV
DW_DIR_IMP_TRF=$HOME/daten/trf; export DW_DIR_IMP_TRF
DW_DIR_IMP_TS=$HOME/daten/sd/ts; export DW_DIR_IMP_TS
DW_DIR_IMP_ZM=$HOME/daten/sd/zm; export DW_DIR_IMP_ZM
DW_DIR_IMP_AUF=$HOME/daten/sd/auf; export DW_DIR_IMP_AUF
DW_DIR_IMP_GUT=$HOME/daten/sd/gut; export DW_DIR_IMP_GUT
DW_DIR_IMP_KDG=$HOME/daten/sd/kdg; export DW_DIR_IMP_KDG
DW_DIR_IMP_MP_TS=$HOME/daten/mp/ts; export DW_DIR_IMP_MP_TS
DW_DIR_IMP_MP_KDG=$HOME/daten/mp/kdg; export DW_DIR_IMP_MP_KDG
DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm; export DW_DIR_IMP_MP_TS
DW_DIR_IMP_IF=$HOME/daten/if; export DW_DIR_IMP_IF
DW_DIR_IMP_NNV=$HOME/daten/nnv; export DW_DIR_IMP_NNV
DW_DIR_IMP_CARMEN=$HOME/daten/carmen; export DW_DIR_IMP_CARMEN
#
GEN_HOME=$DW_DIR_ROOT/generator; export GEN_HOME
#
########################################
# Pfade in Remote Systemen
DW_DIR_CUSTOMER=<login>; export DW_DIR_CUSTOMER

########################################
# Remote Hosts
DW_HOST_CUSTOMER=dxcst3.bn.detemobil.de; export DW_HOST_CUSTOMER


##########################
# Einstellung der Umgebung
#
# ORAHOME: entsprechend ORACLE-Dokumentation
#
if [ -z "$ORACLE_HOME" ]
then
if [ -d /appl/local/oracle/oracle.8.1.6 ]
then
ORACLE_HOME=/appl/local/oracle/8.1.6
elif [ -d /appl/local/oracle/7.3.4 ]
then
ORACLE_HOME=/appl/local/oracle/7.3.4
elif [ -d /appl/local/oracle/oracle.7.3.3 ]
then
ORACLE_HOME=/appl/local/oracle/oracle.7.3.3
elif [ -d /appl/local/oracle/7.3.2 ]
then
ORACLE_HOME=/appl/local/oracle/7.3.2
elif [ -d /appl/local/oracle/7.2.3 ]
then
ORACLE_HOME=/appl/local/oracle/7.2.3
else
echo "Fehler in .dw_init:"
echo "   Konnte ORACLE_HOME nicht setzen !"
echo "Breche ab .."
exit
fi
export ORACLE_HOME
fi

########################
# Aufruf weitere Skripte
#
# dw_global: Einstellung der globalen Parameter
# dw_lokal:  Einstellung der lokalen Parameter
#
. $HOME/.dw_global
. $HOME/.dw_lokal

##################################################
#
 #    #  #    #    ##     ####   #    #
 #    #  ##  ##   #  #   #       #   #
 #    #  # ## #  #    #   ####   ####
 #    #  #    #  ######       #  #  #
 #    #  #    #  #    #  #    #  #   #
  ####   #    #  #    #   ####   #    #

umask 022





=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script performs complex environment configuration including conditional filesystem directory checks to dynamically detect and assign ORACLE_HOME, sets process umask, and sources external configuration files.

EVIDENCE
- Business logic found: KSH custom logic (`.dw_init`) initializes critical paths, verifies filesystem directories to set `ORACLE_HOME` dynamically, sets process umask, and sources downstream scripts.
- AWK: none
- SQL-expressible: no (the script is strictly concerned with operating system and shell environment initialization)
- Non-SQL side effects: checks directory existence on the local filesystem, sets process umask, modifies environment variables, and sources external profile scripts.
- Against this verdict: NO_CONVERSION_REQUIRED could be argued if environment setup is entirely offloaded to modern container or orchestrator-level environment variables (e.g., Airflow/Kubernetes env vars), but the directory-existence check logic and fallback conditions must be represented or handled.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`.dw_init`) is an initialization profile used to bootstrap environment variables for the "Information Services" data warehouse system. It sets standardized directories for root paths, log files, multi-dimensional OLAP cubes, and specific data feeds. Additionally, it dynamically searches the filesystem to locate a valid Oracle home directory (`ORACLE_HOME`), enforces standard file creation permissions (umask), and sources global and local setup files.

2. INVOCATION CONTEXT
   - Who calls this script: It is typically sourced (using `. .dw_init` or `. /path/to/.dw_init`) by other data processing scripts, tools, or UC4 jobs before executing main tasks. No explicit UC4 JOBS_UNIX caller is supplied.
   - UC4 native includes: None referenced in this extraction.
   - Environment files sourced:
     * `. $HOME/.dw_global` — # REVIEW-STRUCT: environment file .dw_global not supplied — variables it sets are unknown; do not guess their names or values
     * `. $HOME/.dw_lokal` — # REVIEW-STRUCT: environment file .dw_lokal not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `HOME` (Environment Variable): Sourced from the shell environment; used as the root for resolving directory locations and external configuration files.
   - `ORACLE_HOME` (Environment Variable): Checked for an existing value. If empty, the script attempts to discover it conditionally.
   - Declarations of internal directory and host variables:
     * `DW_DIR_ROOT` (Defined as `$HOME/aktuell`)
     * `DW_DIR_PROT` (Defined as `$HOME/daten/logfiles`)
     * `DW_DIR_CUBES` (Defined as `$HOME/daten/cubes`)
     * `DW_DIR_IMP_D1` (Defined as `$HOME/daten/d1`)
     * `DW_DIR_IMP_XTRA` (Defined as `$HOME/daten/xtra`)
     * `DW_DIR_IMP_CTEL` (Defined as `$HOME/daten/ctel`)
     * `DW_DIR_IMP_VO` (Defined as `$HOME/daten/vo`)
     * `DW_DIR_IMP_RV` (Defined as `$HOME/daten/rv`)
     * `DW_DIR_IMP_TRF` (Defined as `$HOME/daten/trf`)
     * `DW_DIR_IMP_TS` (Defined as `$HOME/daten/sd/ts`)
     * `DW_DIR_IMP_ZM` (Defined as `$HOME/daten/sd/zm`)
     * `DW_DIR_IMP_AUF` (Defined as `$HOME/daten/sd/auf`)
     * `DW_DIR_IMP_GUT` (Defined as `$HOME/daten/sd/gut`)
     * `DW_DIR_IMP_KDG` (Defined as `$HOME/daten/sd/kdg`)
     * `DW_DIR_IMP_MP_TS` (Defined as `$HOME/daten/mp/ts`)
     * `DW_DIR_IMP_MP_KDG` (Defined as `$HOME/daten/mp/kdg`)
     * `DW_DIR_IMP_MP_ZM` (Defined as `$HOME/daten/mp/zm`) — # REVIEW: Legacy script contains a copy-paste error where it assigns to `DW_DIR_IMP_MP_ZM` but exports `DW_DIR_IMP_MP_TS` a second time: `DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm; export DW_DIR_IMP_MP_TS`. Verify whether `DW_DIR_IMP_MP_ZM` should be properly exported instead.
     * `DW_DIR_IMP_IF` (Defined as `$HOME/daten/if`)
     * `DW_DIR_IMP_NNV` (Defined as `$HOME/daten/nnv`)
     * `DW_DIR_IMP_CARMEN` (Defined as `$HOME/daten/carmen`)
     * `GEN_HOME` (Defined as `$DW_DIR_ROOT/generator`)
     * `DW_DIR_CUSTOMER` (Defined as `<login>`)
     * `DW_HOST_CUSTOMER` (Defined as `dxcst3.bn.detemobil.de`)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   No external client binary commands (such as `sqlplus` or `sftp`) are directly invoked in this script. Only shell built-in utilities (directory tests `-d` and `umask`) are executed.

5. EMBEDDED SQL
   None.

6. CONTROL FLOW
   1. **Path Initialization**: Assigns global directory paths for the application, logs, cubing systems, and operational data imports relative to `$HOME`.
   2. **ORACLE_HOME Presence Check**: Evaluates if `$ORACLE_HOME` is already set. If it is already populated, skips detection.
   3. **ORACLE_HOME Conditional Detection**: If not set, checks the existence of specific directories sequentially:
      - `/appl/local/oracle/oracle.8.1.6` (resolves assignment to `/appl/local/oracle/8.1.6`)
      - `/appl/local/oracle/7.3.4`
      - `/appl/local/oracle/oracle.7.3.3`
      - `/appl/local/oracle/7.3.2`
      - `/appl/local/oracle/7.2.3`
   4. **Error Handling**: If none of the directory candidates exist, displays an error message to standard output and terminates execution.
   5. **Environment Sourcing**: Sources `.dw_global` and `.dw_lokal` to inherit external variable scopes.
   6. **Standardize Permissions**: Sets the process-level file-creation mask (`umask`) to `022`.

7. ERROR HANDLING & EXIT CODES
   - **Detection**: Standard directory checks `-d` inside an `if/elif/else` control structure.
   - **Behavior**: On failure to find any Oracle directory, prints an error message and calls `exit` (without an explicit exit code, which in shell exits with the last status, though it is logically treated as an error).
   - **Python Mapping**: Raise a `FileNotFoundError` or print to stderr and call `sys.exit(1)`.

8. OUTPUTS / SIDE EFFECTS
   - Side effects: Populates and updates the active process's environment variables (`os.environ`).
   - Modifies process permissions using `umask`.

9. BUSINESS SUMMARY
   - Establishes the environmental backbone of the legacy Detemobil data warehouse system.
   - Defines physical log, file, and work directory structures for incoming interface files (such as billing, customer, and call details).
   - Validates that a database driver client directory (`ORACLE_HOME`) is present before allowing applications to run, preventing silent downstream query failures.
   - Configures global and local system environment defaults to enforce parameter alignment across execution nodes.
   - Restricts file creation permissions (`umask 022`) to guarantee that created data/log files are readable but only writable by the owner.

=== PSEUDOCODE ===

```python
# Step 1: Initialize environment and directory paths
import os
import sys

home = os.environ.get("HOME", "")

os.environ["DW_DIR_ROOT"] = os.path.join(home, "aktuell")
os.environ["DW_DIR_PROT"] = os.path.join(home, "daten/logfiles")
os.environ["DW_DIR_CUBES"] = os.path.join(home, "daten/cubes")
os.environ["DW_DIR_IMP_D1"] = os.path.join(home, "daten/d1")
os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home, "daten/xtra")
os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home, "daten/ctel")
os.environ["DW_DIR_IMP_VO"] = os.path.join(home, "daten/vo")
os.environ["DW_DIR_IMP_RV"] = os.path.join(home, "daten/rv")
os.environ["DW_DIR_IMP_TRF"] = os.path.join(home, "daten/trf")
os.environ["DW_DIR_IMP_TS"] = os.path.join(home, "daten/sd/ts")
os.environ["DW_DIR_IMP_ZM"] = os.path.join(home, "daten/sd/zm")
os.environ["DW_DIR_IMP_AUF"] = os.path.join(home, "daten/sd/auf")
os.environ["DW_DIR_IMP_GUT"] = os.path.join(home, "daten/sd/gut")
os.environ["DW_DIR_IMP_KDG"] = os.path.join(home, "daten/sd/kdg")
os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home, "daten/mp/ts")
os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home, "daten/mp/kdg")

# REVIEW: Legacy script contains copy-paste bug assigning to DW_DIR_IMP_MP_ZM but exporting DW_DIR_IMP_MP_TS.
# Replicating original logic by populating DW_DIR_IMP_MP_ZM.
os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home, "daten/mp/zm")

os.environ["DW_DIR_IMP_IF"] = os.path.join(home, "daten/if")
os.environ["DW_DIR_IMP_NNV"] = os.path.join(home, "daten/nnv")
os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home, "daten/carmen")

os.environ["GEN_HOME"] = os.path.join(os.environ["DW_DIR_ROOT"], "generator")
os.environ["DW_DIR_CUSTOMER"] = "<login>"
os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

# Step 2: Dynamically resolve ORACLE_HOME if not already configured
if not os.environ.get("ORACLE_HOME"):
    if os.path.isdir("/appl/local/oracle/oracle.8.1.6"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/8.1.6"
    elif os.path.isdir("/appl/local/oracle/7.3.4"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/7.3.4"
    elif os.path.isdir("/appl/local/oracle/oracle.7.3.3"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/oracle.7.3.3"
    elif os.path.isdir("/appl/local/oracle/7.3.2"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/7.3.2"
    elif os.path.isdir("/appl/local/oracle/7.2.3"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/7.2.3"
    else:
        print("Fehler in .dw_init:", file=sys.stderr)
        print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
        print("Breche ab ..", file=sys.stderr)
        sys.exit(1)

# Step 3: Source global and local configuration profiles
# REVIEW-STRUCT: environment file .dw_global not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file .dw_lokal not supplied — variables it sets are unknown; do not guess their names or values
# In modern architectures, dynamic shell sourcing is resolved by reading a JSON/YAML configuration file into environment variables.

# Step 4: Apply permissions umask (022 octal is 18 decimal)
os.umask(0o022)
```

### Job Dependencies
- **Downstream Jobs (not yet migrated):**
  - `DW.BERT_ABLAUFSTEUERUNG`
  - `DW.BERT_AUSD_BP_TA_MSISDN`
  - `DW.BERT_AUSD_BP_TA_P_BASISPROD`
  - `DW.BERT_AUSD_V_TA_PERIOD`
  - `DW.BERT_AUSD_V_TA_P_VERTRAG`
  - `DW.BERT_AUSD_V_TA_VERTRAG_TMP`
  - `DW.BERT_DROP_TEMP_TABLE`
  - `DW.BERT_P_ADRESSEN`
  - `DW.BERT_P_AUSTAUSCH`
  - `DW.BERT_P_GESCHAEFTSP`
  - `DW.BERT_P_RECH_EMPF`
  - `DW.BERT_RECHNUNGSDATEN`
  - `DW.CRS_VERFUEGBAR_JA_NEIN_PF_JOB_FUER_BERT`
  - `DW.DWH_EXIS_SD_APT_BESTANDS`
  - `DW.DWH_EXIS_SD_APT_RABATT`
- **Target Wiring:**
  In the target Cloud Composer (Airflow) architecture, these downstream units will import or call `dw_init.py` at startup to resolve environmental directories and configurations. Because these downstream jobs are not yet migrated, the orchestration bindings must be finalized as those specific migration passes are completed.

### Scheduling
- **Linkage:** This initialization module is not directly scheduled. It runs dynamically as an import/include inside parent execution contexts.
- **Target Platform Map:** In Cloud Composer, this script will be deployed as a shared utility package/module (e.g., within the Airflow `plugins/` or `dags/utils/` directory) rather than having its own standalone DAG or execution schedule.

### Schedule & Variables
- **Sourcing:** No variables are passed directly from a scheduler. Instead, variables are inherited from the executing shell environment and external profiles.
- **Target Platform Map:** Environment variables will be dynamically populated using Python's native `os.environ` combined with Airflow Variables (`Variable.get`) or runtime task environment configurations.

### Lineage
- **Upstream Configuration Files:**
  - `vobs/dw_source/istools/seu/template/.dw_global` (Local config file dependency)
  - `.DW_LOKAL` (Unresolved configuration dependency)
- **Downstream Consumers:** The 15 dependent jobs listed under the Downstream Jobs section.

### External System Replacements
- **Legacy Filesystem Paths:** The `$HOME/daten/*` structures (used for logfiles, cubes, and importer feeds) map conceptually to a shared cloud storage directory or local mount point on the executing compute environment (e.g., containerized execution on Cloud Run/GKE with access to GCS via `gcsfuse` or Cloud Storage APIs).
- **Oracle Client Connectivity (`ORACLE_HOME`):** Under BigQuery, Oracle-specific path checks are obsolete for native queries. For any hybrid-phase tasks querying legacy databases, this path will be supplied within a containerized environment (using pre-installed Instant Clients) instead of relying on local OS path checks.
- **Remote Host Integration:** The remote host `dxcst3.bn.detemobil.de` and login will be superseded by secure Airflow SSH/SFTP Connection definitions rather than hardcoded environment variables.

### Cross-File Dependencies
- This script relies directly on `.dw_global` and `.dw_lokal` being loaded or executed beforehand to establish global and environment-specific parameters.

### Target File Plan
- **Target File:** `vobs/dw_source/istools/seu/template/dw_init.py`
  - **Language:** Python
  - **Source File:** `vobs/dw_source/istools/seu/template/.dw_init`

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
- **`ORACLE_HOME`**
  - *Description:* Local path to Oracle drivers used to establish DB connections.
  - *Target Resolution:* `os.environ.get("ORACLE_HOME")` (passed into container environments) or managed as part of the execution platform's infrastructure.
- **`HOME`**
  - *Description:* User home directory.
  - *Target Resolution:* `os.environ.get("HOME")`
- **`GCS_BUCKET`** (Conceptual mapping for `$HOME/daten` root)
  - *Description:* Target Cloud Storage bucket representing the root of the data directory.
  - *Target Resolution:* `Variable.get("GCS_BUCKET")`

#### 2. JOB-SPECIFIC
- **`DW_DIR_ROOT`**: Sourced as `$HOME/aktuell` -> managed as an internal script/module constant or task parameter.
- **`DW_DIR_PROT`**: Sourced as `$HOME/daten/logfiles` -> internal constant or job config.
- **`DW_DIR_CUBES`**: Sourced as `$HOME/daten/cubes` -> internal constant or job config.
- **`DW_DIR_IMP_*`** (all individual importer subdirectories): Managed as local Python constants relative to the configured storage root.
- **`GEN_HOME`**: Sourced as `$DW_DIR_ROOT/generator` -> internal job constant.
- **`DW_DIR_CUSTOMER`**: Hardcoded as `<login>` (Placeholder requires manual resolution).
- **`DW_HOST_CUSTOMER`**: Hardcoded as `dxcst3.bn.detemobil.de`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/istools/seu/template/.dw_init` | `vobs/dw_source/istools/seu/template/dw_init.py` | Converted to a Python initialization module to dynamically configure and export environment paths for executing jobs. |

---

### Risks and Manual Steps
1. **SOURCE: NOT FOUND — .DW_LOKAL — no candidate**
   - *Risk:* The local configurations sourced in `.dw_lokal` could not be located in the source codebase. 
   - *Action:* A placeholder/empty stub or config parser must be defined for `.dw_lokal` configuration keys until the physical file is provided by the legacy team.
2. **Downstream Pipeline Migration Lag:**
   - *Risk:* The 15 downstream jobs (`DW.BERT_*`, `DW.CRS_*`, `DW.DWH_EXIS_*`) are currently unmigrated.
   - *Action:* The environment setup cannot be fully integrated into execution tasks until these dependent modules undergo migration.
3. **Legacy Script Copy-Paste Bug:**
   - *Risk:* The legacy KSH script contains an assignment error: `DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm; export DW_DIR_IMP_MP_TS`. This causes the export of `DW_DIR_IMP_MP_TS` twice and leaves `DW_DIR_IMP_MP_ZM` unexported in the environment.
   - *Action:* The Python conversion corrects this by assigning and exporting `DW_DIR_IMP_MP_ZM` correctly. This change must be validated against downstream consumer assumptions.
4. **Placeholder Values in Source:**
   - *Risk:* The variable `DW_DIR_CUSTOMER` is assigned a literal placeholder `<login>`.
   - *Action:* This must be replaced with a secure credential-retrieval mechanism or parameterized environment config before deploying to target environments.