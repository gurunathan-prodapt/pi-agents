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
REASON: The script performs environment variable validation, conditional shell script sourcing, and string-based path manipulations using nested if/for loops that must be converted to Python logic.

EVIDENCE
- Business logic found: None (performs system-level environment configuration, variable validation, and path building rather than processing business data).
- AWK: none
- SQL-expressible: no (system/environment variables and shell path variables cannot be managed inside BigQuery SQL).
- Non-SQL side effects: Modifies environment variables (PATH, LD_LIBRARY_PATH, NLS_*), prints error and status logs to stdout, and conditionally sources a Cognos configuration shell script.
- Against this verdict: In a cloud-native architecture (e.g., Airflow or Cloud Composer), these global path configurations would be managed via environment variables in a Docker container or DAG definition rather than executed as a Python script. However, because the script contains complex logic (validation checks, error printing loops, and path modifications), it cannot be skipped as a pure wrapper.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `.dw_global` acts as a global configuration and environment validation routine for a Data Warehouse ingestion and processing pipeline. Sourced primarily by an initialization shell script (`dwh_init`), it verifies that all critical root, protocol, and staging directories are defined in the environment. It then dynamically appends Oracle database binary and library paths to the system paths, configures Oracle localization/date formats, and conditionally configures Cognos PowerPlay dependencies.

### 2. INVOCATION CONTEXT
- **Sourced by**: Sourced dynamically by the initialization shell script `dwh_init`. There is no direct UC4 scheduling for this specific script as it is a utility file sourced by other scripts.
- **UC4 Native Includes**: None.
- **Sourced Environment Files**: 
  - `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` (Sourced conditionally if the file exists on the filesystem).
    - `# REVIEW-STRUCT: environment file /appl/local/cognos/cognos5.2/pya52b17/setpya.sh not supplied — variables it sets are unknown`

### 3. PARAMETERS / INPUTS
The script checks for the presence of the following pre-defined environment variables:
- `DW_DIR_ROOT`: Root directory for the DW system. (Used in validation)
- `DW_DIR_PROT`: Protocol/logging directory path. (Used in validation)
- `DW_DIR_CUBES`: Target directory for OLAP Cognos cubes. (Used in validation)
- `DW_DIR_IMP_D1`: Import directory for D1 source data. (Used in validation)
- `DW_DIR_IMP_XTRA`: Import directory for extra/miscellaneous sources. (Used in validation)
- `DW_DIR_IMP_CTEL`: Import directory for CTEL sources. (Used in validation)
- `ORACLE_HOME`: Home path for the Oracle Client. (Used in validation and to dynamically update system search paths)

These parameters are retrieved in Python via `os.environ`.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The script does not invoke compiled binary programs, but conditionally evaluates and sources:
- `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`
  - **Exact Command**: `. /appl/local/cognos/cognos5.2/pya52b17/setpya.sh`
  - **Purpose**: Sourced to initialize environment variables required by Cognos PowerPlay.
  - **Python translation**: In Python, sourcing a shell script to modify the parent process's state is not natively possible. The recommended approach is to explicitly manage these variables in an orchestrator (such as Airflow) or load them from a JSON/YAML configuration file.
  - **Launcher status**: Not a database launcher; treated as an external configuration component.

### 5. EMBEDDED SQL
No SQL scripts or statements are present in this environment file.

### 6. CONTROL FLOW
1. Initialize an empty validation tracking variable `fehler`.
2. Check if `$DW_DIR_ROOT` is empty; if so, append `DW_DIR_ROOT` to `fehler`.
3. Check if `$DW_DIR_PROT` is empty; if so, append `DW_DIR_PROT` to `fehler`.
4. Check if `$DW_DIR_CUBES` is empty; if so, append `DW_DIR_CUBES` to `fehler`.
5. Check if `$DW_DIR_IMP_D1` is empty; if so, append `DW_DIR_IMP_D1` to `fehler`.
6. Check if `$DW_DIR_IMP_XTRA` is empty; if so, append `DW_DIR_IMP_XTRA` to `fehler`.
7. Check if `$DW_DIR_IMP_CTEL` is empty; if so, append `DW_DIR_IMP_CTEL` to `fehler`.
8. Check if `$ORACLE_HOME` is empty; if so, append `ORACLE_HOME` to `fehler`.
9. If `fehler` is not empty:
   - Print "Fehler in .dw_global:" to stdout.
   - Iterate through each space-delimited variable name in `fehler` and print a warning that it is unset.
   - Print "Breche ab .." to stdout. (Note: The script does not actually issue an exit command here).
10. Prepend `${ORACLE_HOME}/lib` to `LD_LIBRARY_PATH` and export it.
11. Append `$ORACLE_HOME/bin` to `PATH` and export it.
12. Export localization and database session variables:
    - `NLS_LANG=GERMAN_GERMANY.WE8ISO8859P1`
    - `NLS_DATE_FORMAT=DD-MON-YY`
    - `NLS_DATE_LANGUAGE=AMERICAN`
13. If `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` exists, source it.

### 7. ERROR HANDLING & EXIT CODES
- **Failure Detection**: The script checks if mandatory variables are empty. 
- **Error Behavior**: If validation fails, error descriptions are written to stdout.
- `# REVIEW: The script prints "Breche ab .." (aborting) upon missing environment parameters, but does not execute exit 1 or exit 2. It continues execution despite the failure. In the Python version, we should raise an exception (e.g. ValueError or EnvironmentError) to strictly prevent downstream execution with an invalid context.`
- **Success Code**: If no validation fails, it completes with exit code 0.

### 8. OUTPUTS / SIDE EFFECTS
- **Environment variables updated**: `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`.
- **System log**: Validation failure warnings printed to standard output.

### 9. BUSINESS SUMMARY
- Validates that mandatory Data Warehouse path structures (root, logs, and import directories) are present.
- Configures Oracle database client environment paths and localization profiles to ensure correct parsing of German/American date and character formats.
- Ensures local Cognos environment variables are loaded if PowerPlay tools are installed.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys

# Step 1: Initialize list of validation errors
fehler = []

# Step 2: Validate DW_DIR_ROOT
if not os.environ.get("DW_DIR_ROOT"):
    fehler.append("DW_DIR_ROOT")

# Step 3: Validate DW_DIR_PROT
if not os.environ.get("DW_DIR_PROT"):
    fehler.append("DW_DIR_PROT")

# Step 4: Validate DW_DIR_CUBES
if not os.environ.get("DW_DIR_CUBES"):
    fehler.append("DW_DIR_CUBES")

# Step 5: Validate DW_DIR_IMP_D1
if not os.environ.get("DW_DIR_IMP_D1"):
    fehler.append("DW_DIR_IMP_D1")

# Step 6: Validate DW_DIR_IMP_XTRA
if not os.environ.get("DW_DIR_IMP_XTRA"):
    fehler.append("DW_DIR_IMP_XTRA")

# Step 7: Validate DW_DIR_IMP_CTEL
if not os.environ.get("DW_DIR_IMP_CTEL"):
    fehler.append("DW_DIR_IMP_CTEL")

# Step 8: Validate ORACLE_HOME
if not os.environ.get("ORACLE_HOME"):
    fehler.append("ORACLE_HOME")

# Step 9: Report validation errors
if fehler:
    print("Fehler in .dw_global:")
    for varname in fehler:
        print(f"   Umgebungsvariable {varname} ist nicht gesetzt !")
    print("Breche ab ..")
    # # REVIEW: Replicating the legacy behavior will not exit.
    # # However, it is highly recommended to raise an exception here:
    # raise EnvironmentError(f"Missing required environment variables: {', '.join(fehler)}")

# Step 10: Prepend Oracle library path to LD_LIBRARY_PATH
oracle_home = os.environ.get("ORACLE_HOME", "")
current_ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
if oracle_home:
    if current_ld_library_path:
        os.environ["LD_LIBRARY_PATH"] = f"{oracle_home}/lib:{current_ld_library_path}"
    else:
        os.environ["LD_LIBRARY_PATH"] = f"{oracle_home}/lib"

# Step 11: Append Oracle binary path to PATH
current_path = os.environ.get("PATH", "")
if oracle_home:
    os.environ["PATH"] = f"{current_path}:{oracle_home}/bin:"

# Step 12: Export Database localization and session NLS environment variables
os.environ["NLS_LANG"] = "GERMAN_GERMANY.WE8ISO8859P1"
os.environ["NLS_DATE_FORMAT"] = "DD-MON-YY"
os.environ["NLS_DATE_LANGUAGE"] = "AMERICAN"

# Step 13: Conditionally execute Cognos environment script
# # REVIEW-STRUCT: environment file /appl/local/cognos/cognos5.2/pya52b17/setpya.sh not supplied — variables it sets are unknown
cognos_script = "/appl/local/cognos/cognos5.2/pya52b17/setpya.sh"
if os.path.isfile(cognos_script):
    # Running a shell script from Python will not affect Python's own environment dictionary.
    # Recommended to parse cognos variables in a central Python config module rather than executing a shell script.
    pass
```

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/istools/seu/template/.dw_global` | `vobs/dw_source/istools/seu/template/dw_global.py` | Utility Python module to validate required GCS environment variables and provide global path configuration for the Cloud Composer/Python ecosystem. |

### Job dependencies
- **Downstream Jobs (not yet migrated)**:
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
  - `DW.CCM_PROC_JP`
  - `DW.CCM_WRITE_CONTRACTMAPLOOKUP`
  - `DW.CRS_VERFUEGBAR_JA_NEIN_PF_JOB_FUER_BERT`
  - `DW.DWH_EXIS_SD_APT_BESTANDS`
  - `DW.DWH_EXIS_SD_APT_RABATT`
  - `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`
  - `DW.DWH_IPTN_IAR_BGF_GUTSCHR`
  - `DW.DWH_VVTN_IAR_BGF_GUTSCHR`
  
  *Wiring on BigQuery / Cloud Composer:* These downstream processes depend on the global environment context initialized by this configuration script. Since this script is a shared utility module rather than a standalone scheduled job, it is not wired via cross-DAG dependencies or sensors. Instead, the downstream jobs (once migrated to Airflow DAGs / Python operators) will import or call this configuration module (`dw_global.py`) directly as part of their standard execution flow, or reference these variables via global Airflow Variables.

### Scheduling
- **Trigger Type**: Inherited / Inline Execution.
- **Scheduler Mapping**: This utility module is not scheduled independently. It executes inline inside downstream scheduled pipelines. In the target environment (Cloud Composer), it should be imported and executed as a common utility function/operator at the beginning of downstream DAGs, or its validated environment parameters should be pre-loaded into the Composer Environment Variables.

### Schedule & variables
- **Schedule**: None. This module does not run on a standalone schedule.
- **Variables**:
  - `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`: These directories must be supplied to the runtime environment. On Google Cloud, they map to Google Cloud Storage (GCS) buckets and subdirectories. In Cloud Composer, they must be set as Airflow Variables (e.g., `Variable.get("GCS_BUCKET")`) or environment variables in the Composer environment, which the `dw_global.py` script will dynamically retrieve and validate.
  - `ORACLE_HOME`: Pre-existing library path variable. If downstream modules still connect to legacy Oracle systems, this variable should be configured as a Composer Environment Variable.

### Lineage
- **Upstream Producers**: None.
- **Downstream Consumers**: 
  - `setpya.sh` (Sourced dynamically via `. /appl/local/cognos/cognos5.2/pya52b17/setpya.sh`). This is identified as an external dependency/configuration script, but its exact source content is unresolved.
  - Sourced by `dwh_init` (an external initialization script).

### External system replacements
- **Oracle Client Settings (`ORACLE_HOME`, `LD_LIBRARY_PATH`, `PATH`)**: In a pure BigQuery target environment, these configurations are obsolete unless a hybrid state is maintained where Python operators connect to Oracle databases via SQLAlchemy / `oracledb`. In the cloud target, standard database connections will be managed using Airflow's BigQuery or generic DB connections instead of local shell environment paths.
- **Cognos Integration (`setpya.sh`)**: The conditional execution of `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` for Cognos PowerPlay is obsolete or must be redesigned. In GCP, BI and reporting capabilities are replaced by Looker or BigQuery-native BI tools. The environment sourcing of `setpya.sh` should be retired, or if required for legacy connectivity, replaced with static Composer configuration values.

### Cross-file dependencies
- This module is designed to be sourced/imported by every other script in the ingestion pipeline (e.g., via `dwh_init`). In the target architecture, `dw_global.py` will serve as a common Python utility module that other Python scripts/DAGs import to initialize/validate their run environments.

### Target file plan
- **Target File**: `vobs/dw_source/istools/seu/template/dw_global.py`
  - **Language**: Python
  - **Source File**: `vobs/dw_source/istools/seu/template/.dw_global`
  - **Purpose**: Validates critical environment variables, prepends configuration paths, and exports database localization constants (`NLS_*`). This script preserves the validation check logic and error printing.

### Environment-specific values
Classified based on target environment roles:
- **GLOBAL (Environment-wide)**:
  - `GCS_BUCKET`: Represents the root bucket path replacing `DW_DIR_ROOT`. Sourced in Python via `Variable.get("GCS_BUCKET")` or `os.environ.get("GCS_BUCKET")`.
  - `GCS_PROT_PATH`: Maps to `DW_DIR_PROT` (protocol/log directory). Sourced via environment/Airflow variable.
  - `GCS_CUBES_PATH`: Maps to `DW_DIR_CUBES` (OLAP cube directory). Sourced via environment/Airflow variable.
  - `GCS_IMP_D1_PATH`: Maps to `DW_DIR_IMP_D1`. Sourced via environment/Airflow variable.
  - `GCS_IMP_XTRA_PATH`: Maps to `DW_DIR_IMP_XTRA`. Sourced via environment/Airflow variable.
  - `GCS_IMP_CTEL_PATH`: Maps to `DW_DIR_IMP_CTEL`. Sourced via environment/Airflow variable.
  - `ORACLE_HOME`: Represents the path to the legacy Oracle Client libraries if hybrid access is required. Sourced via `os.environ.get("ORACLE_HOME")`.
  - `GCP_PROJECT`: Sourced via `os.environ.get("GCP_PROJECT")` or Airflow Variable.
  - `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`: Database/session language profiles. Configured as global session context parameters or environment configurations.

- **JOB-SPECIFIC**:
  - *None.* (This file serves purely as a global configuration setter, meaning all variables handled are global in scope).

### Risks and manual steps
- **SOURCE: NOT FOUND — setpya.sh — /appl/local/cognos/cognos5.2/pya52b17/setpya.sh**
  - *Risk*: The Cognos configuration script `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` was not supplied. Any environment variables or path configurations it establishes are unknown.
  - *Mitigation*: A human operator must verify if Cognos is still in use. If Cognos has been retired or replaced by GCP BI tools (e.g., Looker), the logic attempting to source `setpya.sh` can be safely removed.
- **Error Handling Redesign**:
  - *Risk*: The legacy script prints "Breche ab .." (German for "aborting") upon detecting missing required environment variables, but it does not call `exit 1` or raise an error. Instead, it allows downstream execution to proceed in an invalid state.
  - *Mitigation*: In the target Python implementation (`dw_global.py`), missing required variables must raise an explicit `EnvironmentError` or `ValueError` to halt DAG execution safely.
- **Literal Print Rule Preservation**:
  - *Risk*: Translation tools may attempt to translate logging or error outputs.
  - *Mitigation*: The exact German output texts (`"Fehler in .dw_global:"`, `"   Umgebungsvariable {varname} ist nicht gesetzt !"` and `"Breche ab .."`) must be preserved character-for-character in the target Python print/logging statements.

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
REASON: The script contains conditional logic and directory validation to determine and set the ORACLE_HOME environment variable, which requires Python translation.

EVIDENCE
- Business logic found: None (only environment setup and directory checking), but contains conditional logic (if-elif-else) for ORACLE_HOME directory discovery and validation in KSH custom logic.
- AWK: none
- SQL-expressible: no, the script's entire purpose is to set process environment variables, validate directory paths on the local filesystem, and modify file-creation masks.
- Non-SQL side effects: modifies process environment variables (`os.environ`), checks directory existence on the local host, sets the process file-creation mask (`umask`), and sources external environment files.
- Against this verdict: NO_CONVERSION_REQUIRED could be argued since this is a utility initialization profile (`.dw_init`) rather than an executable batch job, but the presence of directory validation blocks and error exits means it must be fully translated to Python if the surrounding architecture is migrating to a Python-native environment.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`.dw_init`) is an environment initialization profile for the Information Services / Data Warehouse system. It is designed to be sourced by other KornShell scripts to establish a consistent set of environment variables, export directory paths for log files, data cubes, and various import interfaces, and dynamically locate the `ORACLE_HOME` installation directory on the host filesystem. It concludes by sourcing global and local override profiles and setting the process-level `umask`.

2. INVOCATION CONTEXT
   - Who calls this script: It is sourced by execution scripts or UC4 JOBS_UNIX wrappers (using `. .dw_init`) to initialize environment variables before running processing workloads. No specific UC4 wrapper or job was provided.
   - UC4 native includes: None referenced in this extraction.
   - Environment files sourced:
     * `. $HOME/.dw_global` — # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
     * `. $HOME/.dw_lokal` — # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `HOME` (environment variable): Read from the OS environment to establish root paths and locate configuration profiles. Surfaced in Python via `os.environ.get("HOME")` or `os.path.expanduser("~")`.
   - `ORACLE_HOME` (environment variable): Checked at runtime. If empty or unset, the script executes a sequence of directory checks to find a valid path on the host. Surfaced via `os.environ.get("ORACLE_HOME")`.
   - KSH DECLARED ENVIRONMENT PARAMETERS:
     * `DB_USER_DWH`, `DB_TNS_NAME_DWH` style connection variables: None declared in this script, though it configures `ORACLE_HOME` which suggests a downstream dependency on Oracle Database clients.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - None. (The script uses shell-builtin directory checks `[ -d ... ]` and sources other files via the dot `.` operator).

5. EMBEDDED SQL
   - None.

6. CONTROL FLOW
   - Step 1: Initialize the core root directory variable `DW_DIR_ROOT` to `$HOME/aktuell` and export it.
   - Step 2: Establish and export logging and analytical cube directories (`DW_DIR_PROT`, `DW_DIR_CUBES`).
   - Step 3: Establish and export nineteen importer-specific interface directory paths (`DW_DIR_IMP_...`).
     * *Note on source-code anomaly:* The assignment `DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm` is followed by `export DW_DIR_IMP_MP_TS`. This is an apparent copy-paste typo in the legacy shell script, resulting in `DW_DIR_IMP_MP_ZM` remaining unexported to child processes.
   - Step 4: Establish and export the metadata generator directory path `GEN_HOME`.
   - Step 5: Establish and export remote customer transfer settings (`DW_DIR_CUSTOMER`, `DW_HOST_CUSTOMER`).
   - Step 6: Validate and resolve `ORACLE_HOME` if it is not already defined in the process environment:
     * Test directory paths sequentially: `/appl/local/oracle/oracle.8.1.6`, `/appl/local/oracle/7.3.4`, `/appl/local/oracle/oracle.7.3.3`, `/appl/local/oracle/7.3.2`, `/appl/local/oracle/7.2.3`.
     * Assign the corresponding valid path to `ORACLE_HOME` and export it.
     * If no valid directory is found, print an error message to stdout and terminate execution immediately.
   - Step 7: Source external environment scripts `.dw_global` and `.dw_lokal`.
   - Step 8: Apply process-level file-creation mask (`umask 022`).

7. ERROR HANDLING & EXIT CODES
   - How does the script detect failure: It tests the existence of five candidate directories for `ORACLE_HOME` using `[ -d <path> ]`.
   - What does it do on failure: Prints a multi-line error message to stdout and executes a bare `exit`.
   - Success exit code convention: Normal script completion yields an implicit exit code `0`.
   - Python Mapping: Map filesystem validation failures to an explicit `FileNotFoundError` or terminate via `sys.exit(1)` with messages written to `sys.stderr`.

8. OUTPUTS / SIDE EFFECTS
   - Modifies the process environment variable dictionary (`os.environ`) which propagates to any child subprocesses spawned by the script.
   - Restricts file creation permissions for any subsequently created files via `os.umask(0o022)`.

9. BUSINESS SUMMARY
   - Coordinates file path definitions across the Data Warehouse import sub-systems (such as D1, VO, RV, MP, and CARMEN).
   - Dynamically adapts to historical database client upgrades (from Oracle 7.2.3 up to Oracle 8.1.6) without manual script modification.
   - Enforces uniform logging structure by establishing a centralized protokoll directory (`daten/logfiles`).
   - Restricts default write-permissions on output files to owner-write, group-read, and world-read (`umask 022`) to maintain compliance and system security.

=== PSEUDOCODE STYLE ===

```python
import os
import sys

# Step 1: Define and export DW_DIR_ROOT
home_dir = os.environ.get("HOME", "")
if not home_dir:
    home_dir = os.path.expanduser("~")

dw_dir_root = os.path.join(home_dir, "aktuell")
os.environ["DW_DIR_ROOT"] = dw_dir_root

# Step 2: Define and export log and cube directories
os.environ["DW_DIR_PROT"] = os.path.join(home_dir, "daten/logfiles")
os.environ["DW_DIR_CUBES"] = os.path.join(home_dir, "daten/cubes")

# Step 3: Define and export importer interface directories
os.environ["DW_DIR_IMP_D1"] = os.path.join(home_dir, "daten/d1")
os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home_dir, "daten/xtra")
os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home_dir, "daten/ctel")
os.environ["DW_DIR_IMP_VO"] = os.path.join(home_dir, "daten/vo")
os.environ["DW_DIR_IMP_RV"] = os.path.join(home_dir, "daten/rv")
os.environ["DW_DIR_IMP_TRF"] = os.path.join(home_dir, "daten/trf")
os.environ["DW_DIR_IMP_TS"] = os.path.join(home_dir, "daten/sd/ts")
os.environ["DW_DIR_IMP_ZM"] = os.path.join(home_dir, "daten/sd/zm")
os.environ["DW_DIR_IMP_AUF"] = os.path.join(home_dir, "daten/sd/auf")
os.environ["DW_DIR_IMP_GUT"] = os.path.join(home_dir, "daten/sd/gut")
os.environ["DW_DIR_IMP_KDG"] = os.path.join(home_dir, "daten/sd/kdg")
os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home_dir, "daten/mp/ts")
os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home_dir, "daten/mp/kdg")

# REVIEW: Original script contains a typo: DW_DIR_IMP_MP_ZM is assigned but DW_DIR_IMP_MP_TS is exported again instead.
# To preserve the exact behavior where DW_DIR_IMP_MP_ZM was not exported, we can omit exporting it,
# but for modern integration we export it to avoid downstream file-access failures.
os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home_dir, "daten/mp/zm")

os.environ["DW_DIR_IMP_IF"] = os.path.join(home_dir, "daten/if")
os.environ["DW_DIR_IMP_NNV"] = os.path.join(home_dir, "daten/nnv")
os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home_dir, "daten/carmen")

# Step 4: Define and export generator home
os.environ["GEN_HOME"] = os.path.join(dw_dir_root, "generator")

# Step 5: Define and export customer remote settings
os.environ["DW_DIR_CUSTOMER"] = "<login>"
os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

# Step 6: Validate and discover ORACLE_HOME if empty or unset
if not os.environ.get("ORACLE_HOME"):
    discovered_oracle_home = None
    if os.path.isdir("/appl/local/oracle/oracle.8.1.6"):
        discovered_oracle_home = "/appl/local/oracle/8.1.6"
    elif os.path.isdir("/appl/local/oracle/7.3.4"):
        discovered_oracle_home = "/appl/local/oracle/7.3.4"
    elif os.path.isdir("/appl/local/oracle/oracle.7.3.3"):
        discovered_oracle_home = "/appl/local/oracle/oracle.7.3.3"
    elif os.path.isdir("/appl/local/oracle/7.3.2"):
        discovered_oracle_home = "/appl/local/oracle/7.3.2"
    elif os.path.isdir("/appl/local/oracle/7.2.3"):
        discovered_oracle_home = "/appl/local/oracle/7.2.3"
    else:
        # Print error details to stderr and terminate process
        print("Fehler in .dw_init:", file=sys.stderr)
        print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
        print("Breche ab ..", file=sys.stderr)
        sys.exit(1)
    
    os.environ["ORACLE_HOME"] = discovered_oracle_home

# Step 7: Sourcing external environment profiles
# Because .dw_global and .dw_lokal are sourced in-process, their variable modifications
# cannot be directly simulated unless those files are parsed, converted to python, or executed in shell.
# REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

# Step 8: Apply umask 022 (read/write/execute for owner, read/execute for group/others)
os.umask(0o022)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/istools/seu/template/.dw_init` | `vobs/dw_source/istools/seu/template/dw_init.py` | Converts the legacy shell-based environment initializer into a Python module. It exports target-equivalent environment variables to the operating system environment (`os.environ`) and handles runtime environment setup (such as file permissions via process `umask`) for Cloud Composer or containerized workers. |

---

### 1. Job Dependencies
Downstream consumer jobs that inherit configurations initialized by this script include:
* `DW.BERT_ABLAUFSTEUERUNG` — *not yet migrated*
* `DW.BERT_AUSD_BP_TA_MSISDN` — *not yet migrated*
* `DW.BERT_AUSD_BP_TA_P_BASISPROD` — *not yet migrated*
* `DW.BERT_AUSD_V_TA_PERIOD` — *not yet migrated*
* `DW.BERT_AUSD_V_TA_P_VERTRAG` — *not yet migrated*
* `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — *not yet migrated*
* `DW.BERT_DROP_TEMP_TABLE` — *not yet migrated*
* `DW.BERT_P_ADRESSEN` — *not yet migrated*
* `DW.BERT_P_AUSTAUSCH` — *not yet migrated*
* `DW.BERT_P_GESCHAEFTSP` — *not yet migrated*
* `DW.BERT_P_RECH_EMPF` — *not yet migrated*
* `DW.BERT_RECHNUNGSDATEN` — *not yet migrated*
* `DW.CCM_PROC_JP` — *not yet migrated*
* `DW.CCM_WRITE_CONTRACTMAPLOOKUP` — *not yet migrated*
* `DW.CRS_VERFUEGBAR_JA_NEIN_PF_JOB_FUER_BERT` — *not yet migrated*
* `DW.DWH_EXIS_SD_APT_BESTANDS` — *not yet migrated*
* `DW.DWH_EXIS_SD_APT_RABATT` — *not yet migrated*
* `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` — *not yet migrated*
* `DW.DWH_IPTN_IAR_BGF_GUTSCHR` — *not yet migrated*
* `DW.DWH_VVTN_IAR_BGF_GUTSCHR` — *not yet migrated*

**Wiring Strategy on GCP**: Because the downstream orchestration jobs are not yet migrated, the target Python module (`dw_init.py`) must be made available as a shared utility library. When these downstream DAGs or Python tasks are migrated, they will import `dw_init.py` at runtime or leverage the Composer environment variables initialized by it.

---

### 2. Scheduling
* **Trigger Event**: This script is an initialization profile and is not directly triggered by any scheduling engines.
* **Target Scheduling Construct**: This module will remain a callable/importable dependency and should **not** have its own standalone schedule or DAG. It will be imported inside other execution tasks or designated as an initialization/startup task inside downstream Cloud Composer DAGs.

---

### 3. Schedule & Variables
* **Schedule**: Inherited. This script runs dynamically inside scheduled child workloads.
* **Scheduler-Set Variables**: The script itself is not fed parameters by a scheduler. It reads the local shell environment parameter `HOME` to resolve the root path structure. In Cloud Composer, `HOME` maps to the local workspace of the running Airflow worker (e.g., `/home/airflow`).

---

### 4. Lineage
* **Upstream Producers (Sourced Configurations)**:
  * `vobs/dw_source/istools/seu/template/.dw_global` — *not in scope for this design pass*
  * `.DW_LOKAL` — *unresolved configuration profile*
* **Downstream Consumers**:
  * Cross-job hand-offs to multiple downstream shell execution tasks (listed in Job Dependencies).

---

### 5. External System Replacements
* **Oracle Database Environment**:
  * The legacy script checks local filesystem paths to resolve `ORACLE_HOME` (from Oracle 7.2.3 to 8.1.6). In the target BigQuery architecture, direct Oracle database paths are obsolete. If downstream components still require connectivity to legacy databases, the Oracle Instant Client must be pre-installed in the Google Cloud Composer / Kubernetes worker image, and `ORACLE_HOME` should point to the container's native driver path (e.g., `/opt/oracle/instantclient`).
* **Remote Customer Host**:
  * The variables `DW_HOST_CUSTOMER` and `DW_DIR_CUSTOMER` point to an external transfer server (`dxcst3.bn.detemobil.de`). In GCP, transfer operations to and from this host should be managed using Google Cloud Composer's `SFTPToGCSOperator` or secure secret-backed transfer wrappers.

---

### 6. Cross-File Dependencies
* **Global & Local Profile Sourcing**:
  * Sourcing `.dw_global` and `.dw_lokal` represents an essential cross-file dependency. To avoid configuration gaps, these files must be fully translated to Python or merged with the global configuration environment of the target Airflow execution workspace.

---

### 7. Target File Plan
* **Target File Path**: `vobs/dw_source/istools/seu/template/dw_init.py`
  * **Language**: Python
  * **Source File**: `vobs/dw_source/istools/seu/template/.dw_init`
  * **Purpose**: Serves as the environment initialization module for Python runtimes. It maps directory structures to GCS equivalents, handles local `umask` modifications, and provides environment verification checks.

---

### 8. Environment-Specific Values

#### GLOBAL (Environment-Wide)
These constants are environment-specific and represent the target infrastructure. They should be loaded dynamically at runtime via environment variables in the Python container or through Airflow configuration:
* `DW_DIR_ROOT`: Maps to the global dags or execution directory root, dynamically computed or loaded via `os.environ.get("DW_DIR_ROOT", f"{GCS_BUCKET}/dags/aktuell")`.
* `DW_DIR_PROT`: Directory for logs. Maps to `gs://{GCS_BUCKET}/daten/logfiles` or Google Cloud Logging.
* `DW_DIR_CUBES`: Storage for analytical cubes. Maps to `gs://{GCS_BUCKET}/daten/cubes`.
* `DW_DIR_IMP_<XX>`: Ingestion interfaces. Maps to distinct Cloud Storage paths under `gs://{GCS_BUCKET}/daten/{interface}` (e.g. `gs://{GCS_BUCKET}/daten/d1`).
* `GEN_HOME`: Generator subdirectory. Maps to `gs://{GCS_BUCKET}/dags/aktuell/generator`.
* `ORACLE_HOME`: If required for database connectivity, loaded from container settings via `os.environ.get("ORACLE_HOME")`.

#### JOB-SPECIFIC
These parameters are specific to this task's external targets and do not belong in global settings. They are populated with concrete values:
* `DW_DIR_CUSTOMER`: Job-specific SFTP login/directory. Fetched via Airflow variable: `Variable.get("dw_dir_customer", default_var="<login>")`.
* `DW_HOST_CUSTOMER`: Remote transfer host. Fetched via Airflow variable: `Variable.get("dw_host_customer", default_var="dxcst3.bn.detemobil.de")`.

---

### 9. Risks and Manual Steps

* **SOURCE: NOT FOUND — .DW_LOKAL — no candidate**
  * The local environment script `.dw_lokal` was sourced by `.dw_init` (`. $HOME/.dw_lokal`) but is missing from the codebase. Any localized parameters that were established inside this file must be manually identified and appended to the target configuration.
* **SOURCE: NOT IN SCOPE — .dw_global — vobs/dw_source/istools/seu/template/.dw_global**
  * The script sources `.dw_global` which is not listed in the current `SOURCE FILES` block and is therefore not converted in this design pass. These global configurations must be manually aligned once `.dw_global` is processed.
* **Correction of Legacy Typo**:
  * In the legacy script, `DW_DIR_IMP_MP_ZM` is assigned but the script mistakenly re-exports `DW_DIR_IMP_MP_TS` instead of exporting the ZM path. The target Python module corrects this mistake and exports `DW_DIR_IMP_MP_ZM` properly to prevent downstream folder access failures.
* **Downstream Integration**:
  * Since downstream jobs are currently unmigrated, they must be adapted during their respective migration phases to import or invoke `dw_init.py` instead of sourcing `.dw_init` in shell.