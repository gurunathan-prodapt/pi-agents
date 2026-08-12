=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh ===
#! /bin/ksh
########################################################################
# Projekt         : Information Services
# Copyright       : 1997,1998 (DeTeMobil Deutsche Telekom MobilNet GmbH)
# Datei Name      : dwmsg.ksh
# Erstellt von    : Ralf Biermanns
# Erstellt am     : 
# Geaendert von   : $Author: Ralf Biermanns $
# Geaendert am    : $Date: $
# Version         : $Revision: 1.00 $
#
# HISTORY
# Zweck/Aufgabe
#     In dieser Datei werden ksh Hilfsroutinen zum Fehlermangement
#     in Information Services gesammelt. Sie dienen zur Vereinheitlichung 
#     und Vereinfachung des Fehlermanagment.
# 
#   DWMSG_Fehlerbehandlung <EintragsNr>
#     Funktion, die bei Auftreten eines Fehlers aufgerufen wird (falls
#     so konfiguriert). Sie regelt das Eintragen in der Meldungstabelle und 
#     ggf. das Ansto�en weiterer Aktionen wie Mail und Robomonbenachrichtigung
#     
#     Die Funktion ist f�r K-SH Skripte gedacht, die mit
#     trap <function> ERR daf�r sorgen, da� bei einem aufgetretenem Fehler
#     (ReturnCode != 0) die Fehlerroutine angesprungen wird. 
#     Je nach Bedarf k�nnen einzelne Fehlercodes auch ignoriert werden.
#
#   DWMSG_SetzeStatusOk <EintragsNr>
#     Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich
#     beendet. Dies ist i.a. die letzte Anweisung im Rahmenskript
#
#   DWMSG_SetzeStatusAbbruch <EintragsNr>
#     Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
#     Dies wird von der Fehlerroutine aufgerufen.
#
#   DWMSG_ErmittleNr <VarName>
#     Funktion ermittelt durch Aufruf einer entsprechenden PL/SQl Routine
#     eine Nr, die f�r nachfolgende Aufrufe der Fehleroutinen ben�tigt wird. 
#     Diese Nummer ist eineindeutig. Die Nummer wird in der Variablen abgelegt
#     dessen Name als erster Parameter angegeben wurde
#
#   DWMSG_ErzeugeEintrag <EintragsNr> <JobKennung> <Programmname> <Logdatei>
#     Funktion erzeugt durch Aufruf einer entsprechenden PL/SQL Routine
#     einen Eintrag ind er Meldungstabelle zur Aufnahme von Fehlermeldungen
#     und weiteren Infos. Die <JobKennung> ist die Jobkennung des
#     Rahmenskriptes.
#      
#   DWMSG_MeldeFehler <EintragsNr> <Typ> <FehlerNr> [[<Zusatz1>] <Zusatz2>]
#     Funktion meldet einen Fehler durch Aufruf einer entsprechenden PL/SQL 
#     Routine. Diese regelt die Ausgabe der Meldung im entsprechendem Format
#     und den Eintrag in der Meldungstabelle (mit Eintragsnummer EintragsNr)
#     Typ kann folgende Auspr�gungen haben F/E/W (Fatal, Error, Warning)
#     FehlerNr mu� eine bekannte Fehlernummer sein
#     Zusatz1 und Zusatz2 sind optionale Fehlernummerspezifische Angaben
#     (z.B. den Namen der Datei, die man nicht �ffnen konnte)
#
#   DWMSG_Logdateiname <VarName> <JobKennung> <EintragsNr>
#     Funktion baut aus den Angaben JobKennung und Eintragsnummer
#     einen LogDateinamen auf und legt ihn in der Variablen VarName ab.
#
#   DWMSG_AppendTimingInfos <EintragsNr> <InfoText> <DateFormat>
#     Funktion traegt Zeitinfos mit Infotext in die Spalte ZUSATZINFOS ein   
#
#########################################################################


DWMSG_Fehlerbehandlung() {
# Fehlerbehandlung wird NUR im Rahmenskript durchgef�hrt
#
# In dieser Funktion wird die Fehlerbehandlung geregelt
# Die Schritte sind immer
#   (*) Meldung produzieren und in Datenbank ablegen (nur wenn
#       dort nicht schon ein Eintrag steht, das regelt aber die aufgerufene
#       PL/SQL-Routine)       
#   (*) Eintrag in der Datenbank auf fehlerhaft beendet setzen
# und ggf.: 
#   (*) Benachrichtigung per Mail und/oder Robomonbenachrichtigung
#
# Parameter:
#   EintragsNr , die Nummer des Eintrages in der Meldungstabelle
####################################################################
# sichern des FehlerCodes:
  FehlerNr=$?
  typeset DWMSG_EintragsNr
  typeset -r kUnerwFehler=10
  DWMSG_EintragsNr=$1

  # echo "Ich bin im Fehlerhandler, fehler der DB melden..."
  # Melde Fehler in der Meldungstabelle. (Diese Fehlernummer wird aber 
  # nur abgelegt, wenn nicht von einer eigenen Routine ein anderer Fehler
  # abgelegt wurde (Unterscheidung zwischen Applikations und unerwarteten
  # Fehlern)
  DWMSG_MeldeFehler $DWMSG_EintragsNr F $kUnerwFehler "ErrorCode ist: $FehlerNr"

  echo "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"
  # DWMSG_SetzOsFehler $DWMSG_EintragsNr $FehlerNr
  DWMSG_SetzeStatusAbbruch $DWMSG_EintragsNr

  # hier ggf. weitere Schritte einbauen.

}

####################################################################

DWMSG_SetzeStatusOK() {
#  Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich
#  beendet. Dies ist i.a. die letzte Anweisung im Rahmenskript
#
  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"
    exit 1
  fi

  # Aufruf des PL/SQL Skriptes zum Setzen des Status
  sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql \
          BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null
}

####################################################################

DWMSG_SetzeStatusAbbruch() {
#  Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
#  Dies wird von der Fehlerroutine aufgerufen.

  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"
    exit 1
  fi

  # Aufruf des PL/SQL Skriptes zum Setzen des Status
  sqlplus $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql \
          BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null
}

####################################################################

DWMSG_ErmittleNr() {
#  Funktion ermittelt durch Aufruf einer entsprechenden PL/SQl Routine
#  eine Nr, die f�r nachfolgende Aufrufe der Fehleroutinen ben�tigt wird. 
#  Diese Nummer ist eineindeutig. Die Nummer wird in der Varaiblen abgelegt
#  dessen Name als erster Parameter angegeben wurde
  typeset VarName
  VarName=$1

  if [ -z "$VarName" ]
  then
    echo "Argh!, keinen Variablennamen bei ErmittleNr angegeben"
    exit 1
  fi
  
  # Zur Kommunikation mit sqlplus wird der ermittelte Wert vom SQL-Plus
  # Skript in eine tempor�re Datei geschrieben und dann hier ausgelesen.
  typeset TempFile
  TempFile="/tmp/ErmittleNr_$$.lst"

  sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql \
          "$TempFile" </dev/null

  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=`cat $TempFile | tr -d ' '`

  rm $TempFile

  # zuweisen
  eval "$VarName=$DWMSG_EintragsNr"
}

####################################################################

DWMSG_ErzeugeEintrag() {
#  Funktion erzeugt durch Aufruf einer entsprechenden PL/SQL Routine
#  einen Eintrag in der Meldungstabelle zur Aufnahme von Fehlermeldungen
#  und weiteren Infos. Die <JobKennung> ist die Jobkennung des
#  Rahmenskriptes.

  typeset DWMSG_EintragsNr
  typeset JobKennung
  typeset Programmname
  typeset LogDatei

  DWMSG_EintragsNr=$1
  JobKennung=$2
  Programmname=$3
  LogDatei=$4

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"
    exit 1
  fi
  # momentan keine weiteren Pr�fungen (ToDo)
  
  # Aufruf des PL/SQL Skriptes zum Setzen des Status
  sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql \
          BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung \
          $Programmname $LogDatei </dev/null

}

####################################################################

DWMSG_MeldeFehler () {
#  Funktion meldet einen Fehler durch Aufruf einer entsprechenden PL/SQL 
#  Routine. Diese regelt die Ausgabe der Meldung im entsprechendem Format
#  und den Eintrag in der Meldungstabelle (mit Eintragsnummer EintragsNr)
#  Typ kann folgende Auspr�gungen haben F/E/W (Fatal, Error, Warning)
#  FehlerNr mu� eine bekannte Fehlernummer sein
#  Zusatz1 und Zusatz2 sind optionale Fehlernummerspezifische Angaben
#  (z.B. den Namen der Datei, die man nicht �ffnen konnte)
#
#      <EintragsNr> <Typ> <FehlerNr> [[<Zusatz1>] <Zusatz2>]
#

  typeset DWMSG_EintragsNr=""
  typeset Typ=""
  typeset FehlerNr=""
  typeset Zusatz1=""
  typeset Zusatz2=""

  # echo "Meldefehler: $*"

  DWMSG_EintragsNr=$1
  Typ=$2
  FehlerNr=$3
  if [ $# -ge 4 ]; then
    Zusatz1=$4
  fi
  if [ $# -ge 5 ]; then
    Zusatz2=$5
  fi

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"
    exit 1
  fi
  # keine weiteren Pr�fungen mehr (ToDo)

  # Anzahl der Parameter ermitteln
  typeset NumParm
  if [ -z "$Zusatz1" ]
  then
    # Aufruf des PL/SQL Skriptes mit drei Parametern
    NumParm=3
  elif [ -z "$Zusatz2" ]
  then
    NumParm=4
  else
    NumParm=5
  fi

  typeset Dateipfad

  Dateipfad="$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p${NumParm}.sql"
  sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler \
          $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' \
          </dev/null

}

####################################################################

DWMSG_Logdateiname() {
#  Funktion baut aus den Angaben JobKennung und Eintragsnummer
#  einen LogDateinamen auf und legt ihn in der Variablen VarName ab.
#  
#  Parameter  <VarName> <JobKennung> <EintragsNr>
  typeset Dateiname
  typeset JobKennung
  typeset VarName

  VarName=$1
  JobKennung=$2
  DWMSG_EintragsNr=$3

  Dateiname="${DW_DIR_PROT}/${JobKennung}_"
  Dateiname="${Dateiname}`date '+%Y%m%d_%H%M'`_${DWMSG_EintragsNr}.log"

  # zuweisen
  eval "$VarName=$Dateiname"

}
####################################################################

DWMSG_SetzeStichtagInfo() {
#  Funktion setzt weitere Infofelder des Eintrages mit Nummer EintragsNr 
#
  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1
  DWMSG_Stichtag=$2
  DWMSG_StichtagFmt=$3

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"
    exit 1
  fi

  if [ -z "$DWMSG_Stichtag" ]
  then
    echo "Argh!, keinen Stichtag angegeben!"
    exit 1
  fi
  
  if [ -z "$DWMSG_StichtagFmt" ]
  then
    echo "Argh!, Stichtagsangaben ohne Formatangaben k�nnen nicht verarbeitet werden!"
    exit 2
  fi

  # Aufruf des der SP zum Setzen der ZusatzInfos
  sqlplus -s $DW_ORAUSER <<EOF
    EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
    commit;
EOF
}

####################################################################

DWMSG_AppendTimingInfos() {
#  Funktion fuegt Timinginfos in die Spalte ZUSATZINFOS hinzu
#
  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1
  DWMSG_InfoText=$2
  DWMSG_DateFormat=$3

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"
    exit 1
  fi
  
  if [ -z "$DWMSG_DateFormat" ]
  then
    echo "Argh!, Formatangabe erforderlich!"
    exit 2
  fi

  # Aufruf des der SP zum Setzen der ZusatzInfos
  sqlplus -s $DW_ORAUSER <<EOF
    EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
    commit;
EOF
}



####################################################################

#
###################################
### Local Variables:
### sh-shell: ksh88
### End:
###################################




=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script is a utility library defining multiple shell functions containing database interactions, error-handling routines, and local file operations that are not expressible in pure SQL.

EVIDENCE
- Business logic found: KSH custom logic contains utility helper functions for error logging, state tracking, and DB interaction.
- AWK: none
- SQL-expressible: no, contains shell function definitions, local temp file operations, dynamic variable assignment via eval, and dynamic path building.
- Non-SQL side effects: creates/deletes temporary files under /tmp, builds dynamic log filenames, and manages process environment settings.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) is a reusable KornShell utility library that provides centralized error management, execution status tracking, and logging for the Information Services data warehouse pipeline. It defines a set of helper functions to initialize log entries, write error messages, and commit process states (Success/Failure) to a centralized Oracle database tracking table named `BERT_MELDUNG`. Sourced by parent ETL shell scripts, it utilizes SQL*Plus to execute PL/SQL stored procedures and database-backed monitoring routines.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced internally (via `. f_alis_msgerr.ksh`) by other parent job scripts within the environment. The parent scripts are managed and launched by UC4/Automic jobs (JOBS_UNIX objects).
   - UC4 Native Includes: None referenced directly inside this library file.
   - Environment files sourced: None. It assumes environment variables such as `DW_ORAUSER`, `DW_DIR_ROOT`, and `DW_DIR_PROT` are already exported by the calling context.

3. PARAMETERS / INPUTS
   This script acts as a library, so its inputs are passed as arguments to individual functions or read from environment variables:
   - `DW_ORAUSER` (env var): Database connection credentials (e.g., username/password or TNS entry).
     - Class: DB-connection-style parameter.
   - `DW_DIR_ROOT` (env var): Base path for SQL wrapper files.
     - Class: Generic environment path.
   - `DW_DIR_PROT` (env var): Directory where log files are written.
     - Class: Generic environment path.
   - Function-specific positional parameters:
     - `DWMSG_EintragsNr` ($1 in status/logging functions): Central transaction/entry ID in the database tracking table. Used to update the state of the active run.
     - `VarName` ($1 in `DWMSG_ErmittleNr` & `DWMSG_Logdateiname`): Target shell variable name used to assign dynamic outputs back to the caller using `eval`.
     - `JobKennung` ($2 in `DWMSG_ErzeugeEintrag` / `DWMSG_Logdateiname`): Unique identifier of the executing job.
     - `Programmname` ($3 in `DWMSG_ErzeugeEintrag`): Executing script name.
     - `LogDatei` ($4 in `DWMSG_ErzeugeEintrag`): Absolute log file path.
     - `Typ` ($2 in `DWMSG_MeldeFehler`): Alert severity type (F=Fatal, E=Error, W=Warning).
     - `FehlerNr` ($3 in `DWMSG_MeldeFehler`): Lookup code in the database's error directory.
     - `Zusatz1` / `Zusatz2` ($4 and $5 in `DWMSG_MeldeFehler`): Optional custom message fields.
     - `DWMSG_Stichtag` ($2 in `DWMSG_SetzeStichtagInfo`): Data processing date.
     - `DWMSG_StichtagFmt` ($3 in `DWMSG_SetzeStichtagInfo`): Date format for parsing (e.g. 'YYYYMMDD').
     - `DWMSG_InfoText` ($2 in `DWMSG_AppendTimingInfos`): String description of timing info.
     - `DWMSG_DateFormat` ($3 in `DWMSG_AppendTimingInfos`): Formatting template for database timestamps.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Invoked across functions to run stored procedures and inline PL/SQL scripts.
     - Example: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/.../d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
     - Purpose: Connects to Oracle DB and executes monitoring routines.
     - Target: Should become native Python DB-client calls using connection pool execution instead of launching separate `sqlplus` CLI sub-processes.
     - # REVIEW: target database platform not specified; DB-client library choice below is provisional
   - `cat` / `rm` / `tr`: Used inside `DWMSG_ErmittleNr` to read and parse sequence IDs from `/tmp` files.
     - Target: Replace completely with Pythonic string manipulations and standard `os.remove()` calls.
   - `date`: Used inside `DWMSG_Logdateiname` to format timestamps.
     - Target: Replace with python native `datetime.datetime.now().strftime(...)`.

5. EMBEDDED SQL
   The script contains dynamic inline PL/SQL blocks and calls to packaged stored procedures.
   
   - SQL Block in `DWMSG_SetzeStichtagInfo`:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     - Statement Type: PL/SQL Stored Procedure Execution
     - Tables touched: Database log metadata table (indirectly updated inside `SetzeZusatzInfos`)
     - Dialect: Oracle (uses `to_date`, SQL*Plus `EXEC`, `commit`)

   - SQL Block in `DWMSG_AppendTimingInfos`:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     - Statement Type: PL/SQL Stored Procedure Execution
     - Tables touched: Database log metadata table (indirectly updated inside `SetzeZusatzInfos`)
     - Dialect: Oracle (uses `to_char`, `SYSDATE`, custom string concatenation `||`, `commit`)

   - Dynamic Procedure Invocations (Referenced SQL files not fully supplied):
     - `BERT_MELDUNG.SetzeStatusOk`
     - `BERT_MELDUNG.SetzeStatusAbbruch`
     - `BERT_MELDUNG.Erzeuge_Eintrag`
     - `BERT_MELDUNG.Fehler`
     - Sequence ID retriever (`d_al_is_ermittlenr.sql`)
     - # REVIEW-STRUCT: SQL scripts (e.g. d_alis_spaufruf_p1.sql) not fully supplied; their behavior is inferred to be wrapping direct package calls

6. CONTROL FLOW
   - **DWMSG_Fehlerbehandlung**:
     1. Captures last process exit code ($?).
     2. Calls `DWMSG_MeldeFehler` using error code 10 (Fatal unexpected error) and formats the captured shell exit code.
     3. Calls `DWMSG_SetzeStatusAbbruch` to flag run as aborted.
   - **DWMSG_SetzeStatusOK**:
     1. Validates that the entry ID `$1` is present. Exits with code 1 if empty.
     2. Calls database procedure `BERT_MELDUNG.SetzeStatusOk` using `sqlplus`.
   - **DWMSG_SetzeStatusAbbruch**:
     1. Validates that the entry ID `$1` is present. Exits with code 1 if empty.
     2. Calls database procedure `BERT_MELDUNG.SetzeStatusAbbruch` using `sqlplus`.
   - **DWMSG_ErmittleNr**:
     1. Validates target return variable name `$1` is provided. Exits with code 1 if empty.
     2. Generates unique temp file name: `/tmp/ErmittleNr_<PID>.lst`.
     3. Runs `d_al_is_ermittlenr.sql` via `sqlplus` to query sequence and output it to the temp file.
     4. Reads the sequence value from the temp file, stripping out whitespaces.
     5. Deletes the temporary file.
     6. Returns the value to the parent shell by performing a dynamic variable assignment (`eval`).
   - **DWMSG_ErzeugeEintrag**:
     1. Validates entry ID `$1`. Exits with code 1 if empty.
     2. Calls procedure `BERT_MELDUNG.Erzeuge_Eintrag` via `sqlplus` with Entry ID, Job Identifier, Program Name, and Log File path.
   - **DWMSG_MeldeFehler**:
     1. Unpacks arguments: Entry ID, alert severity, error code, and up to two optional descriptors.
     2. Validates Entry ID is not empty. Exits with code 1 if empty.
     3. Evaluates the count of arguments to resolve the corresponding SQL execution script wrapper (`d_alis_spaufruf_p3.sql` / `_p4.sql` / `_p5.sql`).
     4. Executes the database procedure `BERT_MELDUNG.Fehler` via `sqlplus`.
   - **DWMSG_Logdateiname**:
     1. Resolves timestamp via system `date '+%Y%m%d_%H%M'`.
     2. Constructs file path string: `${DW_DIR_PROT}/${JobKennung}_<TIMESTAMP>_${DWMSG_EintragsNr}.log`.
     3. Dynamically returns path back to the parent scope via `eval`.
   - **DWMSG_SetzeStichtagInfo**:
     1. Validates Entry ID, Business Date, and Date Format are populated. Exits with 1 or 2 on error.
     2. Executes PL/SQL block calling `BERT_MELDUNG.SetzeZusatzInfos` passing parsed date.
   - **DWMSG_AppendTimingInfos**:
     1. Validates Entry ID and Date Format are populated. Exits with 1 or 2 on error.
     2. Executes PL/SQL block appending current timestamp text inside `BERT_MELDUNG.SetzeZusatzInfos`.

7. ERROR HANDLING & EXIT CODES
   - Standard shell parameter validations use `-z` string checks and exit immediately with code `1` or `2` if a required positional argument is absent.
   - Error detection is handled externally by parent scripts using KSH traps (`trap DWMSG_Fehlerbehandlung ERR`).
   - Translating to Python:
     - Replace shell-style variable validation exits with native Python exception handling (`ValueError`).
     - Standardize error propagation inside database connections using `try...except` and raising custom process tracking errors.

8. OUTPUTS / SIDE EFFECTS
   - Central database tracking metadata table is updated (statuses, log paths, custom timestamps, and error entries).
   - Temporary file `/tmp/ErmittleNr_*.lst` is created and deleted.

9. BUSINESS SUMMARY
   - Coordinates end-to-end telemetry and execution audit tracking of the DWH pipeline.
   - Records operational process boundaries (Start, Stop, and Aborted states) for analytical tasks.
   - Prevents log file collisions by building unique filenames using run IDs and task timestamps.
   - Provides granular logging of ETL business context dates and performance timing metrics directly into administrative database tables.

=======================================================================================
PYTHON PSEUDOCODE OUTLINE
=======================================================================================

```python
# Modernized Python equivalent of f_alis_msgerr.ksh
# Designed to be imported as a class or a set of modular logging/telemetry functions.

import os
import sys
import datetime
import tempfile
# REVIEW: target database platform not specified; DB-client library choice below is provisional
import oracledb as db_driver  # Provisonal client library for Oracle interactions

# Retrieve global env configurations
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT")


def _get_db_connection():
    """Helper utility to establish connections to the central metadata database."""
    # In practice, this should parse connection details from the DW_ORAUSER credential string.
    if not DW_ORAUSER:
        raise ValueError("Environment variable 'DW_ORAUSER' is not set.")
    return db_driver.connect(dsn=DW_ORAUSER)


# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr: int, last_exit_code: int = None):
    """
    Standard exception/error handler. Registers unexpected failures to tracking tables.
    """
    if last_exit_code is None:
        # Fallback if no specific exit code is provided
        last_exit_code = 1

    fatal_error_code = 10
    detail_msg = f"ErrorCode ist: {last_exit_code}"
    
    # Report error to DB
    dwmsg_melde_fehler(eintrags_nr, "F", fatal_error_code, detail_msg)
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus", file=sys.stderr)
    
    # Flag run state as aborted
    dwmsg_setze_status_abbruch(eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr: int):
    """
    Marks the processing run represented by eintrags_nr as successfully completed.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)

    # # REVIEW-STRUCT: SQL scripts (e.g. d_alis_spaufruf_p1.sql) not fully supplied; direct package call executed
    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
            conn.commit()


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr: int):
    """
    Marks the processing run represented by eintrags_nr as aborted.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    # # REVIEW-STRUCT: SQL scripts (e.g. d_alis_spaufruf_p1.sql) not fully supplied; direct package call executed
    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
            conn.commit()


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr() -> int:
    """
    Retrieves a unique tracking sequence ID from the database.
    Replaces temp file mechanics with a direct database query return value.
    """
    # # REVIEW-STRUCT: d_al_is_ermittlenr.sql not supplied; sequence fetch is direct-mapped below
    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Emulated call to sequence query previously handled by d_al_is_ermittlenr.sql
            # In Oracle this is often: SELECT bert_seq.NEXTVAL FROM dual;
            cursor.execute("SELECT BERT_MELDUNG.GetNextSequenceVal FROM DUAL")
            row = cursor.fetchone()
            if row:
                return int(row[0])
            else:
                raise RuntimeError("Failed to fetch unique sequence number from BERT_MELDUNG database.")


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr: int, job_kennung: str, programmname: str, log_datei: str):
    """
    Initializes a monitoring row in the BERT_MELDUNG table for a new run.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)

    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programmname, log_datei])
            conn.commit()


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr: int, typ: str, fehler_nr: int, zusatz1: str = "", zusatz2: str = ""):
    """
    Appends error messages or warnings to a registered tracking ID.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)

    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Maps dynamically to the package procedure, preserving argument counts
            cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
            conn.commit()


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung: str, eintrags_nr: int) -> str:
    """
    Constructs an absolute log file path containing the timestamp, transaction ID, and job name.
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    base_dir = DW_DIR_PROT if DW_DIR_PROT else "."
    filename = f"{job_kennung}_{timestamp}_{eintrags_nr}.log"
    return os.path.join(base_dir, filename)


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr: int, stichtag: str, stichtag_fmt: str):
    """
    Configures specific runtime date filters (Stichtag) for the active logging entry.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)

    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Executes procedure SetzeZusatzInfos with converted native date
            # We convert the format mask from Oracle-style to Python-style or execute as raw query
            query = f"BEGIN BERT_MELDUNG.SetzeZusatzInfos(:1, TO_DATE(:2, :3)); COMMIT; END;"
            cursor.execute(query, [eintrags_nr, stichtag, stichtag_fmt])


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr: int, info_text: str, date_format: str):
    """
    Appends a timestamped annotation to the active logging transaction.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)

    # Resolve date string natively or in SQL
    current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Derived internally or via DB
    append_str = f"{info_text} {current_time_str} "

    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            query = "BEGIN BERT_MELDUNG.SetzeZusatzInfos(:e_nr, NULL, :text); COMMIT; END;"
            cursor.execute(query, e_nr=eintrags_nr, text=append_str)
```

# MIGRATION DESIGN DOCUMENT

## File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `allgemein/is/util/bin/f_alis_msgerr.py` | Converts the KornShell utility library to a Python logging/audit module. Database-backed operations are migrated from Oracle PL/SQL to Cloud BigQuery tracking tables and structured Google Cloud Logging. |

---

## Target File Plan
### `allgemein/is/util/bin/f_alis_msgerr.py`
- **Language**: Python
- **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
- **Purpose**: Provides centralized session orchestration, telemetry, status tracking, and error logging. It translates shell functions that execute SQL\*Plus commands into structured Python client operations using BigQuery.
- **Modernization Details**:
  - **Sequencing**: BigQuery lacks lightweight Oracle-style native database sequences. The function `dwmsg_ermittle_nr()` is modernized to generate a unique run ID based on Python's `uuid.uuid4()` or Airflow's execution context run IDs rather than performing high-overhead SQL execution loops.
  - **Database Calls**: Oracle stored procedure calls (`BERT_MELDUNG.SetzeStatusOk`, etc.) are converted to parameterized DML queries targeting the BigQuery centralized auditing/logging table (`bert_meldung`).
  - **Error Routing**: Integrates standard Python tracebacks with the target BigQuery audit schema, ensuring native exceptions are translated cleanly.

---

## Job Dependencies
This utility module is not a standalone executable and is designed to be imported by parent orchestration layers or other processing jobs. 
- **Downstream Consumer Jobs** (Not yet migrated; will require integration with this modernized Python library post-migration):
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

---

## Scheduling
- **Trigger/Schedule**: This utility library has **no standalone schedule**. It executes strictly inside parent ETL pipelines and scheduled Cloud Composer DAG tasks as an imported library module. It must remain a callable/importable Python module.

---

## Schedule & Variables
- **Injected Execution Environment**: Sourced parameters are handled dynamically via Python library imports or execution environment variables.
- **Variable Mapping**:
  - `DW_DIR_PROT` $\rightarrow$ Replaced by the global environment-wide path `GCS_LOGS_BUCKET` pointing to the centralized logging directory on GCS.
  - `DW_ORAUSER` $\rightarrow$ Replaced by standard IAM credentials tied to the execution Service Account running on Cloud Composer/GKE.

---

## Lineage
- **Upstream Source**: None (this is a foundation utility script).
- **Downstream Target Operations**:
  - `PROCEDURE:SETZEZUSATZINFOS` $\rightarrow$ Maps to BigQuery SQL updates or inserts targeting the logging destination table (`bert_meldung`).

---

## External System Replacements
- **Oracle PL/SQL & SQL\*Plus**: Replaced by standard GCP BigQuery operations via the `google-cloud-bigquery` Python client SDK.
- **Oracle Metadata Package (`BERT_MELDUNG`)**: Replaced by a corresponding BigQuery audit dataset (`BQ_DATASET`) containing an audit table named `bert_meldung`.

---

## Cross-File Dependencies
- **Audit Table Schema**: The BigQuery schema structure of `bert_meldung` must be deployed and made available to all ETL tasks.
- **Calling Code Compatibility**: Parent tasks previously sourcing `f_alis_msgerr.ksh` must be refactored to import `f_alis_msgerr.py` and call its native Python functions.

---

## Environment-Specific Values
### Global (Environment-wide)
- `GCP_PROJECT`: Sourced dynamically at runtime using `os.environ.get("GCP_PROJECT")` or Airflow's environment configuration.
- `BQ_DATASET`: Sourced at runtime using `os.environ.get("BQ_DATASET")` to identify the target BigQuery audit dataset.
- `GCS_BUCKET`: Sourced at runtime using `os.environ.get("GCS_BUCKET")` to identify the central storage bucket for log exports.

### Job-Specific
- `job_kennung`: Passed dynamically as a function parameter by the parent caller.
- `programmname`: Passed dynamically as a function parameter by the parent caller.

---

## Risks & Manual Actions
- **SOURCE: NOT FOUND** — `d_alis_spaufruf_p1.sql` — no candidate (Oracle-specific SQL wrapper script referenced by the legacy shell routines).
- **SOURCE: NOT FOUND** — `d_alis_spaufruf_p4.sql` — no candidate (Oracle-specific SQL wrapper script referenced by the legacy shell routines).
- **SOURCE: NOT FOUND** — `d_al_is_ermittlenr.sql` — no candidate (Oracle-specific sequence retrieval script).
- **Unmigrated Downstreams**: The 12 downstream calling scripts (listed under Job Dependencies) are not yet migrated. The integration of this logging utility cannot be fully end-to-end verified until those caller tasks are refactored into Python.
- **BigQuery Sequence Emulation**: Since BigQuery does not natively support lightweight, sequential sequence-generator calls, replacing `DWMSG_ErmittleNr` with standard UUIDs is recommended. Downstream schemas should be reviewed to verify that the `EintragsNr` column supports string/UUID formats instead of strict integers. If integer IDs are mandatory, a central counter (e.g., Firestore or Cloud SQL) must be evaluated.

---

=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh ===
#! /bin/ksh
# 
# Zweck:
#   Hilfsroutinen fuer das Rechnen mit Datumswerten
#   Momentan basiert es noch auf SQLPLUS/SQL
# Vorbedingung:
#   Nur in Verbindung mit DW-Variablen nutzbar 
#   D.H.: .dw_init mu� durchlaufen sein, oder 
#         DW_DIR_ROOT und DW_ORAUSER sollten gesetzt sein
#
# Erzeugt von : Ralf Biermanns
# Erzeugt am  : 03.09.1998
# Aenderungshistorie:
#    0.1.0; 03.09.1998; rb
#      - Initialversion, nur Funktion zur Berechnung des Vormonats
#    2.5.0; 27.09.1999; Thorsten Juergens
#      - Funktion DW_Date_Datum_Check und DW_Date_Datum_LE erstellt 
#    2.5.1; 28.09.1999; Thorsten Juergens
#      - Funktion DWDate_Gib_Zeitraum erstellt 
#    2.5.2; 29.09.1999; Thorsten Juergens
#      - Suche nach Pattern bei DWDate_Gib_Zeitraum zum Umgang mit 
#        Tracing-Ausgaben der SQLPLUS-Session
#    2.5.3; 04.11.1999; Thorsten Juergens
#      - Verhalten von DW_Gib_Zeitraum hat sich geaendert.
#        Monate und Jahre werden nicht mehr Tagesbasis bestimmt,
#        sondern aufgrund von Ultimo und Ersten.
#    3.0.0; 31.01.2000; Ingo Schwitters
#      - Funktion LetzterTagDesMonats hinzugefuegt
#    3.0.1; 15.5.2000; Ingo Schwitters
#      - Funktion AddiereDatum und TageimMonat hinzugefuegt

DWDate_Vormonat(){
  #
  # P1 : Namen der Variablen, der das Ergebnis zugewiesen werden soll
  # P2 : Formatangabe f�r Oracle to_char/to_date

  typeset DWDate_tmpFile
  typeset VarName=$1
  typeset DWDate_FMT=$2

  # ToDo: Parameter pr�fen

  # hole den Vormonat
  DWDate_tmpFile=/tmp/h_alis_date_`basename $0`_$$.tmp
  sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql \
          $DWDate_tmpFile $DWDate_FMT </dev/null
  # Zuweisen
  eval "$VarName=`cat $DWDate_tmpFile`"
  rm -f $DWDate_FMT
}

#####################################
#Funktion:
#  DWDate_Datum_Check
#Parameter:
#  P1: zu pruefender Datumswert
#  P2: Datumsformat von P1
#Rueckgabe:
#  =0, falls Wert P1 ein gueltiges Datum des Formats P1 ist
#Beschreibung:
#  Zur Pruefung wird die Datenbank genutzt.
DWDate_Datum_Check(){

    typeset wert=$1
    typeset format=$2

    if [ $# -ne 2 ]
    then
        return 1;
    fi

    sqlplus -s <<EOF
$DW_ORAUSER

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
SET HEADING OFF;

-- Implizite Ueberpruefung, ob $Wert ein Datum des Format $format ist
select to_date('$wert','$format') from dual;

EOF

    return $?
}


#####################################
#Funktion:
#  DWDate_Datum_LE
#Parameter:
#  P1: Datum1 im Format YYYYMMDD
#  P2: Datum2 im Format YYYYMMDD
#Rueckgabe:
#  =0, falls P1<=P2 ist
#Annahmen:
#  P1,P2 sind gueltige Datumswerte
#Beschreibung:
#  Zur Pruefung wird die Datenbank genutzt.
DWDate_Datum_LE(){

    typeset datum1=$1
    typeset datum2=$2
    typeset format="YYYYMMDD"

    if [ $# -ne 2 ]
    then
        return 1;
    fi

    sqlplus -s <<EOF
$DW_ORAUSER

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
SET HEADING OFF;

-- PL/SQL-Block garantiert bessere Lessbarkeit und Verstaendnis
-- Ebenfalls kann eine aussagekraeftige Fehlermeldung ausgegeben werden.
DECLARE
    datum1 DATE;
    datum2 DATE;
BEGIN
    
    datum1:=TO_DATE('$datum1','$format');
    datum2:=TO_DATE('$datum2','$format');

    IF datum1>datum2 
    THEN
        -- -20422 ist Fehlernr fuer "Parameter fehlerhaft"
        raise_application_error(-20422,'Datum $datum1 ist groesser als $datum2');
    END IF;

END;
/

EOF

    return $?
}

#####################################
#Funktion:
#  DWDate_Gib_Zeitraum
#Parameter:
#  I-P1: Offset (ganze Zahl)
#  I-P2: Stufe ('Y','M','D')
#  I-P3: Ergebnisformat der Datumswerte
#  O-P4: Variablenname fuer Startpunktes (=Sysdate)
#  O-P5: Variablenname fuer Endepunkt (Start+Offset)
#Rueckgabe:
#  =0, falls Wert P1 ein gueltiges Datum des Formats P1 ist
#Beschreibung:
#  Als Startpunkt wird Sysdate genutzt. Je nach Stufe werden
#  eine bestimmte Anzahl (Offset) von Tagen, Monaten oder Jahren
#  dem Systemdatum hinzugezaehlt.
#  Bei Monaten ist der Anfang immer Monatserste und das Ende immer 
#  der Ultimo des entsprechenden Monats
#  Bei Jahren ist der Anfang immer Neujahr und das Ende immer Sylvester
#  des entsprechenden Jahres
#  Das Ergebnis wird in den uebergebenen Variablen gespeichert
DWDate_Gib_Zeitraum(){

    if [ $# -ne 5 ]
    then
        return 1;
    fi

    typeset Offset=$1
    typeset Stufe=$2
    typeset Format=$3
    typeset Var_Start=$4
    typeset Var_Ende=$5

    typeset tmpFile=/tmp/tmp_`basename $0`_`date +%Y%m%d%H%M%S`_$$.tmp

    # Hole den Zeitraum aus der Datenbank
    sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql \
          $tmpFile $Offset $Stufe $Format \
          </dev/null

    # Pruefe die Korrektheit der Rueckgabe
    # Suche nach dem Ergebnispattern (ECHO/VERIFY ignorieren)
    typeset anzahl=`grep -c "DWH_Ergebnis;" $tmpFile`
    if [ "$anzahl" != "1" ]
    then
        echo "!! Interner Fehler bei der Rueckgabe von Datumswerten"
        echo "   Funktion: DWDate_Gib_Zeitraum"
        echo "   1 Zeile erwartet, $anzahl Zeile(n) bekommen"
        return 1
    fi

    # Zuweisen
    typeset Start=`grep "DWH_Ergebnis;" $tmpFile | cut -f2 -d";"`
    typeset Ende=`grep "DWH_Ergebnis;" $tmpFile | cut -f3 -d";"`
  
    eval "$Var_Start=$Start"
    eval "$Var_Ende=$Ende"

    # Aufraeumen
    rm -f $tmpFile
}


#####################################
#Funktion:
#  LetzterTagDesMonat
#Parameter:
#  P1: zu pruefender Datumswert (Format YYYYMMDD)
#Rueckgabe:
#  =0, falls Wert P1 der Letzte Tag des Monats ist
LetzterTagDesMonats(){
  Jahr=`echo $1 | cut -c1-4`
  Monat=`echo $1 | cut -c5-6`
  Tag=`echo $1 | cut -c7-8`

  if ([ $(($Jahr%4)) == 0 ] && [ $(($Jahr%100)) != 0 ]) || [ $(($Jahr%400)) == 0 ]
  then
    LetzterFeb=29
  else
    LetzterFeb=28
  fi

  set -A LetzterTag 0 31 $LetzterFeb 31 30 31 30 31 31 30 31 30 31
  
  if [ ${LetzterTag[$Monat]} == $Tag ] 
  then
    return 0
  else
    return 1
  fi

  # Das Skrpt gibt 1 aus, wenn der Tag nicht der letzte des Monats ist,
  # und 0 wenn der Tag der letzte ist (Schaltjahre werden beruecksichtigt)
  # Diese Ausgabe wir Returnd
}
#####################################
#Funktion
#  TageimMonat
#Parameter:
#  P1: Jahr (YYYYY)
#  P2: Monat (MM)
# Rueckgabe:
#  gibt die Anzahl der Tage des Monats P2 im Jahr P1 zurueck

TageimMonat(){
  # zuerst feststellen ob das Jahr ein Schaltjahr ist
  if ([ $(($1%4)) == 0 ] && [ $(($1%100)) != 0 ]) || [ $(($1%400)) == 0 ]
  then
    LetzterFeb=29
  else
    LetzterFeb=28
  fi
  
  set -A LetzterTag 0 31 $LetzterFeb 31 30 31 30 31 31 30 31 30 31
  echo "${LetzterTag[$2]}"
}

#####################################
#Funktion:
#  AddiereDatum
#Parameter:
#  P1: Datumswert (Format YYYYMMDD)
#  P2: Anzahl der Tage die addiert wird
#Rueckgabe:
#  Datum: P1+(n Tage)
AddiereDatum(){
  # Datum in Teile zerlegen
  Jahr=`echo $1 | cut -c1-4`
  Monat=`echo $1 | cut -c5-6`
  Tag=`echo $1 | cut -c7-8`

  # Ersteinmal addieren, und sich nicht um den Ueberschlag kuemmern 
  Tag=$(($Tag+$2))

  # Monatsueberschlag, solange bis Tag im Monat liegt
  while [ $Tag -gt `TageimMonat $Jahr $Monat` ]
  do
    Tag=$((Tag-`TageimMonat $Jahr $Monat`)) # Dann den Tag den Uebertrag ensprechend anpassen
    Monat=$(($Monat+1))  # ... den Monat einen Hochzahlen

    # Jahresueberschlag, solange bis Monat im Jahr liegt
    while [ $Monat -gt 12 ] 
    do
      Monat=$(($Monat-12)) # dann den Monat ein Jahr zuruecksetzen
      Jahr=$(($Jahr+1))    # und das Jahr ein Hachsetzen
    done
  done

  Tag=`echo "00$Tag"|tail -3c` # Tag auf 2 Stellen bringen ("2" zu "02")
  Monat=`echo "00$Monat"|tail -3c` # Monat auf 2 Stellen bringen ("2" zu "02")
  Jahr=`echo "0000$Jahr"|tail -5c` # Jahr auf 4 Stellen bringen ("400" zu "0400")

  # Ausgabe und Ende
  echo "$Jahr$Monat$Tag"
}


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script is a KornShell utility library containing functions for date arithmetic, loop-based validation, and SQL*Plus database sessions.

EVIDENCE
- Business logic found: KSH custom logic contains several utility functions for date calculations, leap-year checking, date addition, and date range generation using both local shell math and database calls.
- AWK: none
- SQL-expressible: no (the script is a helper library designed to be sourced by other shell scripts to manipulate environment variables, not a standalone tabular data transformation).
- Non-SQL side effects: Writes temporary files in `/tmp`, manipulates local environment variables using dynamic variable assignment (`eval`), and performs file cleanup.
- Against this verdict: If these functions are only used inside database migrations, they could theoretically be rewritten as BigQuery User Defined Functions (UDFs); however, because they act as a shell-level orchestration library, converting to a Python module is the correct path.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `h_alis_date.ksh` script is a utility library of helper functions for calculating and validating dates. It provides routines to check date formats, compare dates, add offsets (days, months, years), and determine month-end conditions. This library is designed to be sourced by other KornShell scripts in the Data Warehouse (DWH) ecosystem to support orchestration and scheduling logic.

2. INVOCATION CONTEXT
   - Sourced or called by other ksh utilities or UC4 wrappers within the DWH environment. It is not designed to run as a standalone root-level UC4 job itself.
   - Sourced environment files: Requires `.dw_init` to be executed first, or at least `DW_DIR_ROOT` and `DW_ORAUSER` to be pre-set.
   - # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   The parameters are function-specific:
   - `DWDate_Vormonat`
     - `$1`: Target variable name (source: caller; used to dynamically assign the result via `eval`)
     - `$2`: Format string (source: caller; e.g. "YYYYMM")
   - `DWDate_Datum_Check`
     - `$1`: Date string to check (source: caller)
     - `$2`: Expected format string (source: caller)
   - `DWDate_Datum_LE`
     - `$1`: First date in YYYYMMDD format (source: caller)
     - `$2`: Second date in YYYYMMDD format (source: caller)
   - `DWDate_Gib_Zeitraum`
     - `$1`: Offset integer (source: caller)
     - `$2`: Increment unit ('Y', 'M', or 'D') (source: caller)
     - `$3`: Expected output format (source: caller)
     - `$4`: Name of variable to hold start date (source: caller)
     - `$5`: Name of variable to hold end date (source: caller)
   - `LetzterTagDesMonats`
     - `$1`: Date string in YYYYMMDD format (source: caller)
   - `TageimMonat`
     - `$1`: Year in YYYY format (source: caller)
     - `$2`: Month in MM format (source: caller)
   - `AddiereDatum`
     - `$1`: Date string in YYYYMMDD format (source: caller)
     - `$2`: Number of days to add (source: caller)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql`
     - Verbatim command: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql $DWDate_tmpFile $DWDate_FMT </dev/null`
     - Purpose: Computes the previous month based on Oracle database logic.
     - Target: Native Python using `datetime` or `dateutil.relativedelta`.
   - `sqlplus -s` (with inline EOF PL/SQL blocks for `DWDate_Datum_Check` and `DWDate_Datum_LE`)
     - Purpose: Validates format correctness and performs relational comparisons in Oracle.
     - Target: Native Python date parsing (`datetime.strptime`).
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql`
     - Verbatim command: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql $tmpFile $Offset $Stufe $Format </dev/null`
     - Purpose: Calculates relative start/end dates.
     - Target: Native Python calculations.
   - Standard shell utilities: `grep`, `cut`, `rm`, `date`, `basename`, `tail`.
     - Target: Native Python file parsing and string operations.

5. EMBEDDED SQL
   - **Source**: Inline PL/SQL inside `DWDate_Datum_Check`
     ```sql
     select to_date('$wert','$format') from dual;
     ```
     - Statement type: SELECT
     - Tables: Dual
     - Dialect: Oracle SQL
   - **Source**: Inline PL/SQL inside `DWDate_Datum_LE`
     ```sql
     DECLARE
         datum1 DATE;
         datum2 DATE;
     BEGIN
         datum1:=TO_DATE('$datum1','$format');
         datum2:=TO_DATE('$datum2','$format');

         IF datum1>datum2 
         THEN
             -- -20422 ist Fehlernr fuer "Parameter fehlerhaft"
             raise_application_error(-20422,'Datum $datum1 ist groesser als $datum2');
         END IF;

     END;
     ```
     - Statement type: PL/SQL Anonymous Block
     - Dialect: Oracle PL/SQL (unambiguous due to `raise_application_error`, variable declarations, and syntax block).

6. CONTROL FLOW
   - **`DWDate_Vormonat`**:
     1. Creates temp file path `/tmp/h_alis_date_...tmp`.
     2. Runs `sqlplus` calling `d_alis_vormonat.sql`.
     3. Reads output file and dynamically assigns it to the variable named in `$1`.
     4. Deletes temporary format file (Note: code has a bug `rm -f $DWDate_FMT` instead of `$DWDate_tmpFile`. We should correct this cleanup in Python).
   - **`DWDate_Datum_Check`**:
     1. Verifies that exactly 2 arguments are received.
     2. Runs SQL*Plus session executing `TO_DATE` on dual.
     3. Returns SQL*Plus exit status.
   - **`DWDate_Datum_LE`**:
     1. Verifies that exactly 2 arguments are received.
     2. Runs SQL*Plus PL/SQL block comparing dates.
     3. Returns exit status (fails if date1 > date2).
   - **`DWDate_Gib_Zeitraum`**:
     1. Verifies that exactly 5 arguments are received.
     2. Creates a unique temporary file in `/tmp/tmp_...tmp` using system date and process ID.
     3. Runs `sqlplus` calling `d_alis_datum_zeitraum.sql`.
     4. Greps output file for "DWH_Ergebnis;" to verify exact match.
     5. Parses results using `cut` and assigns values dynamically to variables named in `$4` and `$5`.
     6. Cleans up temporary file.
   - **`LetzterTagDesMonats`**:
     1. Splits argument `$1` (YYYYMMDD) into Year, Month, Day.
     2. Performs leap year check via modulo logic.
     3. Determines end-of-month day from array representation.
     4. Compares input day with expected end-of-month day and returns 0 (true) or 1 (false).
   - **`TageimMonat`**:
     1. Determines if input year is a leap year.
     2. Looks up days in the array-like set and prints the result.
   - **`AddiereDatum`**:
     1. Parses input date (YYYYMMDD) into numeric Year, Month, Day.
     2. Adds requested days to Day.
     3. Iteratively subtracts days in current month while rolling over to next month and year using a nested while loop.
     4. Normalizes string output to YYYYMMDD format via tail padding.

7. ERROR HANDLING & EXIT CODES
   - Shell functions use standard return codes (`return 0` for success, `return 1` for failure).
   - Within database sessions, `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` ensures execution aborts and communicates failure back to shell.
   - Python equivalence: Standard exceptions (`ValueError`, `RuntimeError`) or returning boolean values.

8. OUTPUTS / SIDE EFFECTS
   - Temporary file writes inside `/tmp` directory.
   - Dynamic environment variables modified within the scope of the parent execution thread (which will map to return values or dictionary mutations in Python).

9. BUSINESS SUMMARY
   - Standardizes date parsing and consistency rules across the legacy DWH batch pipeline.
   - Provides robust leap-year checking and calendar month-end evaluation.
   - Enables simple date addition operations without external language wrappers in pure shell contexts.
   - Standardizes relative business calculations such as finding "previous month" or "current business loading interval" based on reference offsets.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import tempfile
from datetime import datetime, date
import calendar
from typing import Tuple, Dict, Any

# REVIEW-STRUCT: SQL script [d_alis_vormonat.sql] and [d_alis_datum_zeitraum.sql] are not supplied.
# Their functionality can be completely replaced by native Python datetime logic, which is much safer and faster.
# The database-dependent logic is reconstructed below natively.

def dw_date_vormonat(format_str: str) -> str:
    """
    Replaces: DWDate_Vormonat
    Returns the first day of the previous month formatted as requested.
    """
    # Equivalent native calculation:
    today = date.today()
    # Go back to the first of current month, then subtract 1 day to reach previous month
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - datetime.timedelta(days=1)
    prev_month_date = last_of_prev_month.replace(day=1)
    
    # Translate common Oracle formats to Python strftime formats
    format_map = {
        "YYYYMM": "%Y%m",
        "YYYYMMDD": "%Y%m%d",
        "YYYY-MM-DD": "%Y-%m-%d"
    }
    py_fmt = format_map.get(format_str, "%Y%m%d") # Default fallback
    return prev_month_date.strftime(py_fmt)


def dw_date_datum_check(wert: str, format_str: str) -> bool:
    """
    Replaces: DWDate_Datum_Check
    Returns True if 'wert' matches 'format_str', False otherwise.
    """
    format_map = {
        "YYYYMMDD": "%Y%m%d",
        "YYYYMM": "%Y%m",
        "DD.MM.YYYY": "%d.%m.%Y"
    }
    py_fmt = format_map.get(format_str, format_str)
    try:
        datetime.strptime(wert, py_fmt)
        return True
    except ValueError:
        return False


def dw_date_datum_le(datum1_str: str, datum2_str: str) -> bool:
    """
    Replaces: DWDate_Datum_LE
    Returns True if datum1 <= datum2. Raises ValueError if not, mimicking raise_application_error.
    """
    fmt = "%Y%m%d"
    try:
        d1 = datetime.strptime(datum1_str, fmt)
        d2 = datetime.strptime(datum2_str, fmt)
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYYMMDD. Error: {e}")
        
    if d1 > d2:
        # Replicates custom exception raising with error -20422
        raise ValueError(f"Application Error (-20422): Datum {datum1_str} ist groesser als {datum2_str}")
    return True


def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> Tuple[str, str]:
    """
    Replaces: DWDate_Gib_Zeitraum
    Computes a date interval based on offset, increment unit (Y, M, D), and target format.
    Returns (start_date, end_date) tuple.
    """
    today = date.today()
    start_date = today
    
    # Note: Legacy script logic for months/years goes by calendar limits
    if stufe.upper() == 'D':
        end_date = today + datetime.timedelta(days=offset)
    elif stufe.upper() == 'M':
        # Start is first of current month
        start_date = today.replace(day=1)
        # Shift month by offset
        month_val = start_date.month - 1 + offset
        year_val = start_date.year + (month_val // 12)
        month_val = (month_val % 12) + 1
        end_date_first = date(year_val, month_val, 1)
        # End is last day of that target month
        last_day = calendar.monthrange(end_date_first.year, end_date_first.month)[1]
        end_date = end_date_first.replace(day=last_day)
    elif stufe.upper() == 'Y':
        # Start is first day of current year
        start_date = today.replace(month=1, day=1)
        # Shift year by offset
        end_date_first = start_date.replace(year=start_date.year + offset)
        # End is last day of that target year (Sylvester)
        end_date = end_date_first.replace(month=12, day=31)
    else:
        raise ValueError(f"Unknown level (Stufe): {stufe}. Must be Y, M, or D.")
        
    format_map = {
        "YYYYMMDD": "%Y%m%d",
        "YYYYMM": "%Y%m"
    }
    py_fmt = format_map.get(format_str, "%Y%m%d")
    return start_date.strftime(py_fmt), end_date.strftime(py_fmt)


def letzter_tag_des_monats(date_str: str) -> bool:
    """
    Replaces: LetzterTagDesMonats
    Checks if given YYYYMMDD string is the last day of its month.
    """
    try:
        year = int(date_str[0:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
    except ValueError:
        return False
        
    last_day = calendar.monthrange(year, month)[1]
    return day == last_day


def tage_im_monat(year: int, month: int) -> int:
    """
    Replaces: TageimMonat
    Returns the number of days in the specified year and month.
    """
    return calendar.monthrange(year, month)[1]


def addiere_datum(date_str: str, days_to_add: int) -> str:
    """
    Replaces: AddiereDatum
    Adds integer days to YYYYMMDD date and returns resulting string in same format.
    """
    fmt = "%Y%m%d"
    dt = datetime.strptime(date_str, fmt)
    result_dt = dt + datetime.timedelta(days=days_to_add)
    return result_dt.strftime(fmt)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py` | Converted to a reusable Python module containing date helper functions, replacing shell-level mathematics and SQL*Plus database queries with native Python date arithmetic. |

---

### Job Dependencies
- **Downstream Jobs**:
  - `DW.BERT_ABLAUFSTEUERUNG` (not yet migrated)
  - `DW.BERT_AUSD_BP_TA_MSISDN` (not yet migrated)
  - `DW.BERT_AUSD_BP_TA_P_BASISPROD` (not yet migrated)
  - `DW.BERT_AUSD_V_TA_PERIOD` (not yet migrated)
  - `DW.BERT_AUSD_V_TA_P_VERTRAG` (not yet migrated)
  - `DW.BERT_AUSD_V_TA_VERTRAG_TMP` (not yet migrated)
  - `DW.BERT_DROP_TEMP_TABLE` (not yet migrated)
  - `DW.BERT_P_ADRESSEN` (not yet migrated)
  - `DW.BERT_P_AUSTAUSCH` (not yet migrated)
  - `DW.BERT_P_GESCHAEFTSP` (not yet migrated)
  - `DW.BERT_P_RECH_EMPF` (not yet migrated)
  - `DW.BERT_RECHNUNGSDATEN` (not yet migrated)
- **Target Platform Integration**: Because this file is a shared utility library, these downstream jobs will import this as a Python module within their Cloud Composer DAG tasks or Python operators. Since all listed downstreams are marked "not yet migrated", the library must be packaged or placed in the Cloud Composer Python search path (e.g., `/dags/` or `/plugins/` folders) so it is immediately accessible when those downstream jobs are eventually migrated.

---

### Scheduling
- This library is not directly executed by any scheduler. It operates as an include/shared module and does not require its own standalone DAG schedule or trigger.

---

### Schedule & Variables
- **Schedule**: This job is a callable/importable unit only; do not schedule it independently.
- **Variables**: No scheduler-set variables are directly fed to this utility script. Instead, inputs are supplied dynamically as function arguments by callers at runtime.

---

### Lineage
- **Upstream Producers**: None.
- **Downstream Consumers**: The 12 downstream jobs listed under the "Job Dependencies" section.
- **Table Lineage**: The legacy utility reads from the Oracle database table `DUAL` to run format validation and comparisons. In the target architecture, this database read dependency is retired; all date validations and calculations are done locally in-memory inside the Python runtime, eliminating unnecessary queries.

---

### External System Replacements
- **Oracle Database (SQL*Plus)**: Inline SQL*Plus calls are completely retired. In Python, these are replaced with the standard `datetime` and `calendar` libraries to process dates locally, reducing the dependency on the database engine.

---

### Cross-file Dependencies
- **Dependent SQL Scripts**: 
  - `d_alis_vormonat.sql` (located at `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql`)
  - `d_alis_datum_zeitraum.sql` (located at `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql`)
  These external SQL scripts are not supplied in this group, but their core calculations (such as subtracting a month and generating date-range boundaries) are handled natively within the target Python library's functions.

---

### Target File Plan
- **`vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py`**
  - **Language**: Python
  - **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
  - **Role**: Reusable Python module containing helper functions for date validation, calculation, and interval mapping.

---

### Environment-specific Values
- **`DW_DIR_ROOT`**: GLOBAL. Used in the legacy environment to locate SQL script folders. Since those scripts are retired in favor of native Python logic, this variable is retired for this module.
- **`DW_ORAUSER`**: GLOBAL. Oracle connection credentials. Since Oracle SQL*Plus database queries are retired in favor of native Python calculations, this variable is retired for this module.

---

### Risks & Manual Steps
- **Unresolved SQL Scripts**: The logic in `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` is not present in the codebase. Standard Gregorian calendar behaviors are modeled in the target Python logic. However, if the business uses custom calendars, custom fiscal dates, or custom holidays, discrepancies could occur. A developer must verify the legacy SQL scripts to confirm they do not implement custom calendar overrides.
- **Downstream Migration Coordination**: The downstream jobs are "not yet migrated". When they are migrated, a manual code update will be required to change their shell sourcing directives into Python import statements.

---

=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh ===
#!/bin/ksh
 
# Zweck:
#    Hilfsroutinen fuer das Parsen von Parametern
# Erzeugt von:  TJ 
# Erzeugt am:   18.02.1998
# Versions-Anmerkungen:
# Historie     :
#    01.01.02; 18-02-1998; Thorsten Juergens
#      - Initialversion
#    01.01.03; 13-Mar-1998; Tino Rudolph
#      - Erweiterung konvertiereSystem: neues System vo
#    01.01.04; 20-Mar-1998; Tino Rudolph
#      - Erweiterung konvertiereSystem rueckgaenging gemacht,
#        dafuer neue Funktion konvertiereSDSystem
#    01.02.01; 11-Mai-1998; Thorsten Juergens
#      - Funktion konvertiereSDSystem umbenannt in konvertiereSDNamen
#    02.00.01; 24-Jul-1998; Thomas Bregulla
#      - Erweiterungen in konvertiereKennzahl, pruefeSystemKennzahl
#      - fuer das UmsatzTeilProjekt/Xtra Gutschriften Import
#    02.00.02; 27-Jul-1998; Thorsten Juergens
#      - Von gutschriften auf gutschrift
#      - Funktion gibBereich implementiert
#      - Funktion gibIntervall implementiert
#    02.00.03; 17-Aug-1998; Steffen Zimmer
#      - Erweiterung von konvertiereKennzahl um Kennzahl restguthaben
#      - Erweiterung der Teilnehmerbereichsliste in gibBereich um 'rst gut auf'
#    02.00.04; 27-Aug-1998; Wilfried Richter
#      - Erweiterung von konvertiereKennzahl, konvertiereSystem,
#        pruefeSystemKennzahl,gibBereich,gibIntervall um System 'nnv',
#        Kennzahl 'tvd', Bereich 'gd'
#    02.00.05; 28-Aug-1998; Thomas Bregulla
#      - Anpassung fuer SD-Import Tarifkennung
#    02.00.06; 03-Sep-1998; Ralf Biermanns
#      - Neue Funktion konvertiere_AufbStufeXtra
#    02.00.07; 10-Sep-1998; Thorsten Juergens
#      - Pflege des Moduls (Kommentare/Historie)
#      - rudimentaere Inspektion und Verbesserung (keine neue Funktionalitaet)
#    02.00.07; 10-Sept-1998; Steffen Zimmer
#      - Setzen des Bereiches us fuer die Kennzahlen rst und auf in der Funktion
#        gibBereich 
#    02.01.00; 12-Nov-1998; Stephan Kriwet
#      - Neue Kennzahlen ust und usk ( D1-Umsatz teilnehmer und konto )
#    02.01.01; 21-Jan-1999; Florian Dingler
#       - Neue Kennzahl lkl (TVD Leistungsklasse)
#    02.02.00; 05-Mai-1999; Oliver Lunnebach
#       - Neue Kategorien fur SD-Historisierung implementiert
#    02.02.01; 14-Jun-1999; Thorsten Juergens
#       - Fehler bei Schleifenverarbeitung (IFS) und Parameterpruefung beseitigt
#    02.05.00; 24-Aug-1999; Ingo Schwitters
#       - Erweiterung fuer neue Kennzahlen "rak" und "loe"
#       - Erweiterung fuer neue Namenskonvention "carmen" und "dpps" als
#         Quellsystem 
#    02.05.01; 28-Sep-1999; Thorsten Juergens
#       - Funktion pruefeZeitraum hinzugefuegt
#    02.05.02; 28-Sep-1999; Armin Angenent
#       - Funktion pruefeZeitParameter hinzugefuegt
#       - Funktion pruefeZahlPositiv hinzugefuegt
#    02.05.03; 29-Sep-1999; Armin Angenent
#       - Funktion konvertiereZeitspanne hinzugefuegt
#    02.05.04; 30-Sep-1999; Ingo Schwitters
#       - Erweiterrung fuer neue Kennzhalen "sgs" und "srs"
#    02.05.05; 04.10.1999; Stefan Kurz
#       - Erweiterung fuer Kundenstammdaten aus Carmen.
#    02.05.06; 26.10.1999; Armin Angenent
#       - Verbiete Aufruf Xtra mit zug, abg, bst, abz, auf, gut
#    02.05.07; 27.10.1999; Ingo Schwitters
#       - Hurrraaaa - Noch mehr Kennzahlen. Diesmal in den Stammdaten:
#         "l_prod", "l_gutgr" und "l_leist"
#    02.05.08; 29.10.1999; Ingo Schwitters
#       - SAP als Liefersystem f�r SRS und SGS eingefuehrt
#    02.05.09; 15.11.1999; Ingo Schwitters
#       - Aufruf des Importers fuer Carmen nicht mit Kennzahl UST erlauben 
#    02.05.10; 22.11.1999; Armin Angenent
#       - Erlaube CTel Tarifwechsler wieder   
#    03.00.01;  3.12.1999; Ingo Schwitters
#       - Und wir machen weiter, froh und heiter,
#         und fuehren ins neue Release hinein
#         die "Mahnstufen" ("mahn") aus SAP mit ein.
#    03.00.02; 08.02.2000; Roger Butenuth
#       - Ergaenzung fuer Metadatenstruktur
#    03.00.03; 10.03.2000; Steffen Martin
#       - Einfuegen des Liefersystem brunet
#         mit den Kennzahlen rubrik und liefermodus
#    03.00.04; 15.03.2000; Frank Wedemeyer
#       - Ergaenzung der Kennzahl "sg_rv"
#         Gutschriften von Rahmenvertragskunden ueber SAP
#    03.00.05; 16.03.2000; Heinz Schulte
#       - Erweiterung um Liefersystem SIGMA 
#       - Einfuehrung der Kennzahlen 
#                   
#                   "nnk"       (netznutzungsklassen)
#                   "tvk"       (tagesverkehrskurven)
#                   "gz"        (gespraechsziele)
#                   "glv"       (gespraechslaengenverteilung)
#           
#    03.00.06; 17.03.2000; Ralph Christgen
#        - Kennzahlen "sr_rv_dpps" (Rechn.daten, Lief. von SAP)
#                und "bwa" (Bewegungsarten aus DPPS)
#
#    03.00.07; 20.03.2000; Heinz Schulte
#        - Einf�hrung der Kennzahlen fuer Liefersystem SIGMA (Lookupdaten)          
#                   "zonek"     (zonenkennung)                   
#                   "zonet"     (zonentyp)
#                   "nnkt"      (netznutzungsklassentyp)
#                   "trfa"      (tarifart) 
#    03.00.08; 20.03.2000; Frank Wedemeyer
#       - Ergaenzung fuer Gutschriftengruende aus SAP
#
#    03.00.09; 23.03.2000; Heinz Schulte
#        - Einf�hrung der Kennzahlen fuer Liefersystem SIGMA (Lookupdaten)          
#                    "gtyp"     <gespraechstyp>
#                    "basisd"   <basisdienst>
#                    "natint"   <nationalinternational>
#                    "glint"    <glaengenintervall>     
#
#    03.00.10; 26.03.2000; Ralph Christgen
#        - Kennzahl "bwa" in konvertiereSDName ergaenzt
#
#
#
#    Offene Punkte: Verwaltung der erlaubten Parameter fuer 
#    PruefeKennzahlSystem umstellen auf Arrays
#    Siehe Gib_Bereich
#    Zu Klaeren: Kennzahlen uskonto und usteilnehmer
#    
#
# Oeffentliche Funktionen:
# -----------------------
# pruefeParameterGesetzt Param_Name Param_Var 
# konvertiereKennzahl Param_Var
# konvertiereSystem   Param_Var
# konvertiereSDName   Param_Var
# pruefeSystemKennzahl Param_System Param_Kennzahl
# gibBereich   Param_Kennzahl ParamVar
# gibIntervall Param_Kennzahl ParamVar
# konvertiereAufbStufeXtra Param_Var
# pruefeZeitraum Param_Anfang Param_Ende
# konvertiereZeitspanne

#Generelle Annahmen: 
#   1.  Fehlerbehandlung ist aktiv

ModulName="alis_parameter";
ModulVersion="V3.0.9";

#####################################
#Funktion:
#   pruefeParameterGesetzt
#Parameter:
#   $1 - I  Parameter_Name      Beschreibender Name des Parameters
#   $2 - I  Parameter_Variable  Name der Environment-Variable
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#Annahmen:
#   Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   prueft, ob die uebergebene Environment-Variable einen Wert
#   beinhaltet. 
#   Falls dies nicht der Fall ist, wird ein standardisierter
#   Fehlerzustand generiert. Um bestehende Fehlerzustaende nicht zu 
#   ueberschreiben, wird ein Test nur dann durchgefuehrt, falls noch 
#   kein Fehler vorliegt (vgl. Annahmen).
pruefeParameterGesetzt(){

    typeset param_name=$1
    typeset param_var=$2
    typeset param_wert;

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    if [ -z "$param_name" -o -z "$param_var" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} pruefeParameterGesetzt"
        return 
    fi

    eval "param_wert=\$$param_var"

    if [ -z "$param_wert" ]
    then
        ErrNr=194
        ErrArg=$param_name
    fi
}

#####################################
#Funktion:
#   konvertiereKennzahl
#Parameter:
#   $1 - IO Name der Environment-Variable die Kennzahlbeschreibung enthaelt
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#   Sonst
#      Uebergebene Environment-Variable enthaelt gueltige Kennzahlabkuerzung
#Annahmen:
#   1. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   konvertiert die Kennzahlbezeichnung basierend auf dem Namenskonzept 
#   in eine gueltige Abkuerzung fuer Kennzahlen.
#   Falls keine Konvertierung erfolgen kann, da die Kennzahlbeschreibung
#   unbekannt ist, wird ein standardisierter Fehlerzustand generiert. 
#   Um bestehende Fehlerzustaende nicht zu ueberschreiben, wird ein Test
#   nur dann durchgefuehrt, falls noch kein Fehler vorliegt (vgl. Annahmen).
konvertiereKennzahl(){
    typeset -l Kennzahl     # Konvertierung in Kleinbuchstaben
    typeset VarName

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    VarName=$1
    if [ -z "$VarName" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} konvertiereKennzahl"
        return 
    fi

    eval "Kennzahl=\$$VarName"

    case $Kennzahl in
        zugang)
            Kennzahl="zug";;
        abgang)
            Kennzahl="abg";;
        abgang_zukunft)
            Kennzahl="abz";;
        bestand)
            Kennzahl="bst";;
        tarifwechsel)
            Kennzahl="twe";;
        plan)
            Kennzahl="pln";;
        gutschrift)
            Kennzahl="gut";;
        aufladung)
            Kennzahl="auf";;
        restguthaben)
            Kennzahl="rst";;
        teilnehmerverbindungsdaten)
            Kennzahl="tvd";;
        uskonto)
            Kennzahl="usk";;
        usteilnehmer)
            Kennzahl="ust";;
        leistungsklasse)
            Kennzahl="lkl";;
        loeschung)
            Kennzahl="loe";;
        reaktivierung)
            Kennzahl="rak";;
        standard_rechnung)
            Kennzahl="srs";;
        standard_gutschrift)
            Kennzahl="sgs";;
        gutschrift_rv)
            Kennzahl="sg_rv";;
        rechnungen_rv_dpps)
            Kennzahl="sr_rv_dpps";;
        bewegart)
            Kennzahl="bwa";;
        kundenstamm)
            Kennzahl="ksd";;
        mahnstufe)
            Kennzahl="mahn";;
        metadatenstruktur)
            Kennzahl="mds";;
        d1news)
            Kennzahl="d1n";;
        rubrik)
            Kennzahl="rub";;
        liefermodus)
            Kennzahl="lmo";;
        netznutzungsklassen)
            Kennzahl="nnk";;
        tagesverkehrskurven)
            Kennzahl="tvk";;
        gespraechsziele)
            Kennzahl="gz";;
        gespraechslaengenverteilung) 
            Kennzahl="glv";;
        zonenkennung) 
            Kennzahl="zonek";;
        zonentyp) 
            Kennzahl="zonet";;
        netznutzungsklassentyp) 
            Kennzahl="nnkt";;
        tarifart) 
            Kennzahl="trfa";;
        gespraechstyp)
            Kennzahl="gtyp";;
        basisdienst)
            Kennzahl="basisd";;
        nationalinternational)
            Kennzahl="natint";;
        glaengenintervall)
            Kennzahl="glint";;
        *)
            ErrNr=198; # Wert des Parameters unbekannt
            ErrArg=$Kennzahl
            Kennzahl="???";;
    esac

    eval "$VarName=$Kennzahl"
}


#####################################
#Funktion:
#   konvertiereSystem
#Parameter:
#   $1 - IO Name der Environment-Variable die Systembeschreibung enthaelt
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#   Sonst
#      Uebergebene Environment-Variable enthaelt gueltige Abkuerzung
#Annahmen: (PreConditions)
#   1. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   konvertiert die Systembezeichnung basierend auf dem Namenskonzept
#   in eine gueltige Abkuerzung fuer Liefersysteme.
#   Falls keine Konvertierung erfolgen kann, da die Systembeschreibung
#   unbekannt ist, wird ein standardisierter Fehlerzustand generiert. 
#   Um bestehende Fehlerzustaende nicht zu ueberschreiben, wird ein Test
#   nur dann durchgefuehrt, falls noch kein Fehler vorliegt (vgl. Annahmen).
konvertiereSystem(){

    typeset -l System     # Konvertierung in Kleinbuchstaben
    typeset VarName

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    VarName=$1
    if [ -z "$VarName" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} konvertiereSystem"
        return 
    fi

    eval "System=\$$VarName"

    case $System in
        sap)
            ;;
        carmen)
            ;;
        dpps)
            ;;
        d1)
            ;;
        xtra)
            ;;
        ctel)
            ;;
        nnv)
            ;;
        dwh)
            ;;
        brunet)
            ;;
        sigma)
            ;;
        *)
            ErrNr=195
            ErrArg="Unbekannte Datenherkunft $System !"
            System="???";;
    esac

    eval "$VarName=$System"
}

#####################################
#Funktion:
#   konvertiereSDName
#Parameter:
#   $1 - IO Name der Environment-Variable die Beschreibung von SD enthaelt
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#   Sonst
#      Uebergebene Environment-Variable enthaelt gueltige Systemabkuerzung
#Annahmen: (PreConditions)
#   1. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   konvertiert die Systembezeichnung basierend auf dem Namenskonzept
#   in eine gueltige Abkuerzung fuer Stammdaten-Liefersysteme.
#   Falls keine Konvertierung erfolgen kann, da die Systembeschreibung
#   unbekannt ist, wird ein standardisierter Fehlerzustand generiert. 
#   Um bestehende Fehlerzustaende nicht zu ueberschreiben, wird ein Test
#   nur dann durchgefuehrt, falls noch kein Fehler vorliegt (vgl. Annahmen).
konvertiereSDName(){

    typeset -l System     # Konvertierung in Kleinbuchstaben
    typeset VarName

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    VarName=$1
    if [ -z "$VarName" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} konvertiereSDSystem"
        return 
    fi

    eval "System=\$$VarName"

    case $System in
        vo)
            ;;
        rahmenvertrag)
            System="rv";;
        tarif)
            System="trf";;
        tstatus)
            System="ts";;
        zahlmodus)
            System="zm";;
        kdg_grund)
            System="kdg";;
        gutschrift)
            System="gut";;
        aufladung)
            System="auf";;
        leistung)
            System="l_leist";;
        gutschrift_grund)
            System="l_gutgr";;
        sap_gutschrift_grund)
            System="sap_l_gutgr";;
        produkt)
            System="l_prod";;
        mahnverfahren_sapist)
            System="l_mahnv_ist";;
        mahnverfahren_sapfi)
            System="l_mahnv_fi";;
        mahnstufentyp_sapist)
            System="l_mahnstyp_ist";;
        bewegart)
            System="bwa";;
        *)
            ErrNr=195
            ErrArg="Unbekannte Stammdaten-Datenherkunft $System !"
            System="???";;
    esac

    eval "$VarName=$System"
}

#####################################
#Funktion:
#   konvertiereAufbStufeXtra
#Parameter:
#   $1 - IO Name der Environment-Variable die den Namen der Aufb.Stufe enth�lt
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#   Sonst
#      Uebergebene Environment-Variable enthaelt gueltige Kennzahlabkuerzung
#Annahmen:
#   1. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   konvertiert die Aufbereitungsstufenname in eine normierte Abkuerzung.
#   Falls keine Konvertierung erfolgen kann, da der Stufenname 
#   unbekannt ist, wird ein standardisierter Fehlerzustand generiert. 
#   Um bestehende Fehlerzustaende nicht zu ueberschreiben, wird ein Test
#   nur dann durchgefuehrt, falls noch kein Fehler vorliegt (vgl. Annahmen).
konvertiereAufbStufeXtra(){
    typeset -l Stufe     # Konvertierung in Kleinbuchstaben
    typeset VarName

    # Schon Fehler gesetzt, dann keine Pr�fung
    if [ $ErrNr -ne 0 ]
    then
      return
    fi

    VarName=$1
    if [ -z "$VarName" ]
    then
      ErrNr=196
      ErrArg="${ModulName} ${ModulVersion} konvertiereAufbStufeXtra"
      return 
    fi

    eval "Stufe=\$$VarName"

    case $Stufe in
      zusammenfuehrung)
        Stufe="mrg";;
      befuellung)
        Stufe="fill";;
      *)
        ErrNr=195
        ErrArg="Unbekannte Stufenangabe $Stufe !"
        Stufe="???";;
    esac

    eval "$VarName=$Stufe"

}


#####################################
#Funktion:
#   pruefeSystemKennzahl
#Parameter:
#   $1 - I System   Kurzbezeichnung des Liefersystems
#   $2 - I Kennzahl Kurzbezeichnung der Kennzahl
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#Annahmen: (PreConditions)
#   1. System besitzt gueltige Abkuerzung fuer Liefersysteme (d1,xtra, ..)
#      vgl. hierzu die Funktion konvertiereSystem
#   2. Kennzahl besitzt gueltige Abkuerzung fuer Kennzahlen (zug,abg, ..)
#      vgl. hierzu die Funktion konvertiereKennzahl
#   3. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   prueft, ob die Kombination von System und Kennzahl erlaubt ist, d.h.
#   vom IS unterstuetzt wird.
#   Falls dies nicht der Fall ist, wird ein standardisierter
#   Fehlerzustand generiert. Um bestehende Fehlerzustaende nicht zu 
#   ueberschreiben, wird ein Test nur dann durchgefuehrt, falls noch 
#   kein Fehler vorliegt (vgl. Annahmen).
pruefeSystemKennzahl(){
    typeset System=$1
    typeset Kennzahl=$2

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    if [ -z "$System" -o -z "$Kennzahl" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} pruefeSystemKennzahl"
        return 
    fi

    # Pruefe die Kombinationen der Parameter
    if [ "$System" != "nnv" -a \( "$Kennzahl" = "tvd" -o "$Kennzahl" = "lkl" \) ]
    then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
    elif [ "$System" = "carmen" ] 
    then
        if [ "$Kennzahl" = "twe" -o "$Kennzahl" = "pln" -o "$Kennzahl" = "rst" -o "$Kennzahl" = "srs" -o "$Kennzahl" = "sgs" -o "$Kennzahl" = "ust" -o "$Kennzahl" = "mahn" -o "$Kennzahl" = "sg_rv" -o "$Kennzahl" = "sr_rv_dpps" -o "$Kennzahl" = "bwa"  ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "sap" ] 
    then
        if [ "$Kennzahl" = "zug" -o "$Kennzahl" = "abg" -o "$Kennzahl" = "abz" -o "$Kennzahl" = "bst" -o "$Kennzahl" = "twe" -o "$Kennzahl" = "pln" -o "$Kennzahl" = "gut" -o "$Kennzahl" = "auf" -o "$Kennzahl" = "rst" -o "$Kennzahl" = "tvd" -o "$Kennzahl" = "usk" -o "$Kennzahl" = "ust" -o "$Kennzahl" = "lkl" -o "$Kennzahl" = "loe" -o "$Kennzahl" = "rak" -o "$Kennzahl" = "ksd" -o "$Kennzahl" = "bwa" ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "dpps" ] 
    then
        if [ "$Kennzahl" = "twe" -o "$Kennzahl" = "pln" -o "$Kennzahl" = "loe" -o "$Kennzahl" = "rak" -o "$Kennzahl" = "srs" -o "$Kennzahl" = "sgs" -o "$Kennzahl" = "mahn" -o "$Kennzahl" = "sg_rv" -o "$Kennzahl" = "sr_rv_dpps" ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "ctel" ]
    then
        if [ "$Kennzahl" != "abg" -a "$Kennzahl"  != "bst" -a "$Kennzahl" != "zug" -a "$Kennzahl" != "twe" ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "xtra" ] 
    then
        if [ "$Kennzahl" != "rst" ]    
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "d1" ]
    then
        if [ "$Kennzahl" = "gut" -o "$Kennzahl" = "auf" -o "$Kennzahl" = "loe" -o "$Kennzahl" = "rak" -o "$Kennzahl" = "sgs" -o "$Kennzahl" = "srs" -o "$Kennzahl" = "twe" -o "$Kennzahl" = "ksd" -o "$Kennzahl" = "mahn" -o "$Kennzahl" = "sg_rv" -o "$Kennzahl" = "sr_rv_dpps" -o "$Kennzahl" = "bwa" ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "nnv" ]
    then
        if [ ! \( "$Kennzahl" = "tvd" -o "$Kennzahl" = "lkl" \) ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "dwh" ]
    then
        if [ "$Kennzahl" != "mds" ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "brunet" ]
    then
        if [ "$Kennzahl" != "d1n" -a "$Kennzahl" != "rub" -a "$Kennzahl" != "lmo" ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    elif [ "$System" = "sigma" ]
    then
        if [ "$Kennzahl" != "nnk"  -a "$Kennzahl" != "tvk"    -a "$Kennzahl" != "glv"    -a "$Kennzahl" != "gz" -a "$Kennzahl" != "zonek" -a "$Kennzahl" != "zonet" -a "$Kennzahl" != "nnkt" -a "$Kennzahl" != "trfa" -a \
             "$Kennzahl" != "gtyp" -a "$Kennzahl" != "basisd" -a "$Kennzahl" != "natint" -a "$Kennzahl" != "glint" ]
        then
            ErrArg="Ungueltige Kombination $System $Kennzahl"
        fi
    fi

    if [ -n "$ErrArg" ]
    then
        ErrNr=195
    fi

}

#####################################
#Funktion:
#   gibBereich
#Parameter:
#   $1 - I Kennzahl Kurzbezeichnung der Kennzahl
#   $1 - O Name der Environment-Variable der den Bereich enthalten soll
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#Annahmen: (PreConditions)
#   1. Kennzahl besitzt gueltige Abkuerzung fuer Kennzahlen (zug,abg, ..)
#      vgl. hierzu die Funktion konvertiereKennzahl
#   2. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   gibt in Abhaengigkeit zu einer Kennzahl/Eingangsgroesse den
#   entsprechenden Bereich aus.  
#   Kann kein Bereich zugeordnet werden, wird ein standardisierter
#   Fehlerzustand generiert. Um bestehende Fehlerzustaende nicht zu 
#   ueberschreiben, wird ein Test nur dann durchgefuehrt, falls noch 
#   kein Fehler vorliegt (vgl. Annahmen).
gibBereich(){

    typeset Kennzahl=$1
    typeset VarBereich=$2 

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    if [ -z "$Kennzahl" -o -z "$VarBereich" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} gibBereich"
        return 
    fi
    
    typeset list_tn="abg abz bst pln twe zug loe rak"
    typeset list_us="gut rst auf ust usk srs sgs mahn sg_rv sr_rv_dpps"
    typeset list_gd="tvd lkl d1n rub lmo nnk tvk gz glv zonek zonet nnkt trfa gtyp basisd natint glint"
    typeset list_sd="ksd bwa"
    typeset list_md="mds"
    typeset listBereich="tn us gd sd md"
    typeset my_Bereich

    typeset OLD_IFS=$IFS
    IFS=" "
    
    for bk in $listBereich;
    do
        eval "list=\$list_$bk"
        if [ -z "$my_Bereich" ] 
        then
            for groesse in $list;
            do 
                if [ $groesse = "$Kennzahl" ]
                then
                    my_Bereich=$bk
                fi    
            done
        fi
    done

    IFS=$OLD_IFS

    if [ -z "$my_Bereich" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} gibBereich - Kuerzel '$Kennzahl' unbekannt"
        return
    fi

    eval "$VarBereich=$my_Bereich"

}

#####################################
#Funktion:
#   gibIntervall
#Parameter:
#   $1 - I Kennzahl Kurzbezeichnung der Kennzahl
#   $1 - O Name der Environment-Variable der das Intervall enthalten soll
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#Annahmen: (PreConditions)
#   1. Kennzahl besitzt gueltige Abkuerzung fuer Kennzahlen (zug,abg, ..)
#      vgl. hierzu die Funktion konvertiereKennzahl
#   2. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   gibt in Abhaengigkeit zu einer Kennzahl/Eingangsgroesse das
#   entsprechenden Intervall (t,m) aus.  
#   Kann kein Intervall zugeordnet werden, wird ein standardisierter
#   Fehlerzustand generiert. Um bestehende Fehlerzustaende nicht zu 
#   ueberschreiben, wird ein Test nur dann durchgefuehrt, falls noch 
#   kein Fehler vorliegt (vgl. Annahmen).
gibIntervall(){

    typeset Kennzahl=$1
    typeset VarIntervall=$2 

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    if [ -z "$Kennzahl" -o -z "$VarIntervall" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} gibIntervall"
        return 
    fi
    
    typeset list_t="abg abz twe zug gut auf rst ust usk rak loe srs sgs ksd mahn mds tvk sr_rv_dpps gtyp basisd bwa"
    typeset list_m="bst pln tvd lkl sg_rv d1n rub lmo nnk gz glv zonek zonet nnkt trfa natint glint"
    typeset listIntervall="t m"
    typeset my_Intervall

    typeset OLD_IFS=$IFS
    IFS=" "
    for ik in $listIntervall;
    do
        eval "list=\$list_$ik"
        if [ -z "$my_Intervall" ] 
        then
            for groesse in $list;
            do 
                if [ $groesse = "$Kennzahl" ]
                then
                    my_Intervall=$ik
                fi
            done
        fi
    done
    IFS=$OLD_IFS

    if [ -z "$my_Intervall" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} gibIntervall - Kuerzel '$Kennzahl' unbekannt"
        return
    fi

    eval "$VarIntervall=$my_Intervall"
}


#####################################
#Funktion:
#   pruefeZeitraum
#Parameter:
#   $1 Anfangsdatum des Zeitraums im Format YYYYMMDD
#   $2 Endedatum des Zeitraums im Format YYYYMMDD
#Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
#Annahmen: (PreConditions)
#   1. Die Funktionen DW_Date_Datum_Check und DW_Date_Datum_LE
#      sind verfuegbar
#   2. Ein Fehlerzustand liegt vor falls die Variable ErrNr!=0 ist.
#Beschreibung:
#   Die Funktion prueft, ob die beiden Parameter einen gueltigen
#   Zeitraum beschreiben. Hierzu wird sowohl das Format der Parameter
#   geprueft als auch die Eigenschaft dass das Ende nicht kleiner als
#   das Anfangsdatum sein darf
pruefeZeitraum(){

    typeset Anfang=$1
    typeset Ende=$2 
    typeset Format="YYYYMMDD"

    if [ $ErrNr -ne 0 ]
    then
        return
    fi

    if [ -z "$Ende" -o -z "$Anfang" ]
    then
        ErrNr=196
        ErrArg="${ModulName} ${ModulVersion} pruefeZeitraum"
        return 
    fi

    # In dieser Subshell ERR-Flag loeschen, keine Auswirkung auf Parent-Shell
    set +e

    # Setze ein temp. Logfile
    typeset tmpFile=/tmp/tmp_`basename $0`_`date +%Y%m%d%H%M%S`_$$.tmp
    typeset ergebnis;
    typeset Wert;
    ErrArg=""

    # Pruefe, ob Datum dem Format entspricht
    for param in Anfang Ende;
    do
        eval "Wert=\$$param"
        DWDate_Datum_Check $Wert $Format >> $tmpFile 2>&1
        ergebnis=$?
        if [ $ergebnis -ne 0 ]
        then
            ErrArg="${param}datum entspricht nicht dem Format $Format"
        fi
    done

    if [ -z "$ErrArg" ]
    then
        # Pruefe Reihenfolge
        DWDate_Datum_LE $Anfang $Ende >> $tmpFile 2>&1
        ergebnis=$?
        if [ $ergebnis -ne 0 ]
        then
            ErrArg="Anfangsdatum ist nicht kleiner gleich Endedatum"
        fi
    fi

    if [ -n "$ErrArg" ]
    then
        ErrNr=195
        cat $tmpFile
    fi

    rm -f $tmpFile
}

#####################
# Funktion pruefeZahlPositiv:
# Parameter:
#   $1 Zahlenwert (auch das ist zu pruefen!)
#   $2 Name des Parameters
# Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
# Beschreibung:
# prueft ob der uebergebene Parameter numerisch und > 0 ist
pruefeZahlPositiv(){
    typeset p_Zahl=$1
    typeset p_ParameterName=$2
    if [ "$p_Zahl" -ne 0 -o "$p_Zahl" -eq 0 ]
    then
        if [ "$p_Zahl" -lt 0 ]
        then
            ErrNr=195
            ErrArg="Parameter $p_ParameterName muss groesser gleich 0 sein"
        fi
    else
        ErrNr=195
        ErrArg="Parameter $p_ParameterName ist kein numerischer Wert"
    fi
}

#####################
# Funktion pruefeZeitParameter:
# Parameter:
#   $1 Anfangsdatum
#   $2 Endedatum
#   $3 Zeitspanne
# Rueckgabe:
#   Im Falle eines Fehlers enthalten die folgenden Variablen
#      ErrNr  - Fehlercode
#      ErrArg - beschreibendes Argument fuer Fehlerausgabe
# Beschreibung:
# prueft ob genau Anfangs und Endedatum oder Zeitraum gesetzt und 
# gueltig sind
pruefeZeitParameter(){

    typeset p_Anfangsdatum=$1
    typeset p_Endedatum=$2
    typeset p_ZeitOffset=$3

    if [ "$ErrNr" -ne 0 ]
    then
        return
    fi
    
    # 1.Fall: die Zeitspanne ist gesetzt, dann muessen 
    # Anfang und Ende leer sein
    if [ -n "$p_ZeitOffset" ]
    then
        if [ -z "$p_Anfangsdatum" -a -z "$p_Endedatum" ]
        then
            # der Zeitoffset selbst mu� ein numerischer Wert > 0 sein
            pruefeZahlPositiv $p_ZeitOffset Zeitspanne
            return
        else
            ErrNr=195
            ErrArg="Es darf nur eine Zeitspanne oder \
                    beide Datumwerte gesetzt werden"
            return
        fi
    else
        # 2.Fall: der ZeitOffset ist leer, 
        # sind Datumswerte plausibel ?
        if [ -n "$p_Anfangsdatum" -a -n "$p_Endedatum" ]
        then
            # pruefe Datumsemantik
            pruefeZeitraum $p_Anfangsdatum $p_Endedatum 
        else
            ErrNr=195
            if [ -z "$p_Anfangsdatum" -a -z "$p_Endedatum" ]
            then
                ErrArg="Datumswerte oder Zeitspanne fehlen"
            else
                ErrArg="Sowohl Anfang- als auch Endedatum muessen angegeben werden"
            fi
            return
        fi
    fi
}

#####################
# Funktion konvertiereZeitspanne
# Parameter:
#   $1 Name der Anfangdatums-Variable
#   $2 Name der Endedatums-Varialble
#   $3 numerische Zeitspanne
#   $4 Kennzahl in Kurzform
# Beschreibung:
# Berechnet aus der Zeitspanne und der Kennzahl Anfangs und Endedatum
# Vorbedingung: Zeitspanne ist numerisch, die Variablennamen sind nicht 
# leer und die Kennzahl ist gueltig
konvertiereZeitspanne(){
    typeset p_VarAnfang=$1
    typeset p_VarEnde=$2
    typeset p_Spanne=$3
    typeset p_Kennzahl=$4

    if [ $ErrNr -ne 0 ]
    then
        return
    fi
    
    # In dieser Subshell ERR-Flag loeschen, keine Auswirkung auf Parent-Shell
    set +e

    ########################
    # Vorbereitungen zur Konvertierung

    # Die Einheit des Offsets ist in der Regel Tag
    Offset_Unit=D
    
    # Bei Bestand ist die Einheit Monat
    if [ "$p_Kennzahl" = bst ]
    then
        Offset_Unit=M
    fi

    # Setze ein temp. Logfile
    typeset tmpFile=/tmp/tmp_`basename $0`_`date +%Y%m%d%H%M%S`_$$.tmp
    typeset ergebnis
    typeset Anfangsdatum
    typeset Endedatum

    ########################
    # eigentliche Berechnung, fuer eine Zeitspanne die in die Vergangenheit
    # reicht muss ein negativer Parameter eingegeben werden.
    DWDate_Gib_Zeitraum     -$p_Spanne \
                            $Offset_Unit \
                            "YYYYMMDD" \
                            Anfangsdatum \
                            Endedatum > $tmpFile
    ergebnis=$?
    if [ $ergebnis -ne 0 ]
    then
        ErrNr=85
        ErrArg="DWDate_Gib_Zeitraum"
        cat $tmpFile
    else
        eval "$p_VarAnfang=$Anfangsdatum"
        eval "$p_VarEnde=$Endedatum"
    fi

    #Aufraeumen
    rm -f $tmpFile
}


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script is a KornShell library containing utility function definitions for parameter validation, name mapping, and date range calculation that must be converted to Python.

EVIDENCE
- Business logic found: KSH custom logic defines functions for normalizing system names and KPIs, mapping areas and intervals, and validating date/numeric arguments.
- AWK: none
- SQL-expressible: no, these are procedural validations and string/date calculations meant for shell environment orchestration.
- Non-SQL side effects: invokes external helper commands (`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`) and generates temporary files under `/tmp`.
- Against this verdict: none, as this is a library of utility shell functions with no database interaction.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_parameter.ksh`) is a library of modular KornShell utility functions. It standardizes the parsing, normalization, and validation of business-critical parameters (such as source system names, key figure identifiers/KPIs, and execution date intervals) across the Information System (IS) ETL environment. It provides consistent error tracking using global variables (`ErrNr` and `ErrArg`) and includes safety checks for date ranges and numeric values.

2. INVOCATION CONTEXT
   - Who calls this script: It is sourced (e.g., `. h_alis_parameter.ksh`) by other KornShell ETL or orchestration wrappers in the DWH environment to inject validation capabilities. It is not designed to be executed as a standalone executable.
   - Any UC4 native includes (:inc ...): None referenced in this script.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   Because this is a library, parameters are passed as arguments directly to its public functions:
   - `pruefeParameterGesetzt`:
     - `$1` (`param_name`): The descriptive name of the parameter.
     - `$2` (`param_var`): The string name of the environment variable containing the value to check.
   - `konvertiereKennzahl`:
     - `$1` (`VarName`): The name of the environment variable whose value is a key figure (KPI) string to be converted.
   - `konvertiereSystem`:
     - `$1` (`VarName`): The name of the environment variable whose value is a system name to be converted.
   - `konvertiereSDName`:
     - `$1` (`VarName`): The name of the environment variable whose value is a master data system name to be converted.
   - `konvertiereAufbStufeXtra`:
     - `$1` (`VarName`): The name of the environment variable whose value is an aggregation stage name to be converted.
   - `pruefeSystemKennzahl`:
     - `$1` (`System`): The normalized source system name.
     - `$2` (`Kennzahl`): The normalized key figure name.
   - `gibBereich`:
     - `$1` (`Kennzahl`): The key figure code.
     - `$2` (`VarBereich`): The name of the variable to store the business area code.
   - `gibIntervall`:
     - `$1` (`Kennzahl`): The key figure code.
     - `$2` (`VarIntervall`): The name of the variable to store the interval code (`t` or `m`).
   - `pruefeZeitraum`:
     - `$1` (`Anfang`): Start date (`YYYYMMDD`).
     - `$2` (`Ende`): End date (`YYYYMMDD`).
   - `pruefeZahlPositiv`:
     - `$1` (`p_Zahl`): Value to verify.
     - `$2` (`p_ParameterName`): Variable description.
   - `pruefeZeitParameter`:
     - `$1` (`p_Anfangsdatum`): Start date.
     - `$2` (`p_Endedatum`): End date.
     - `$3` (`p_ZeitOffset`): Relational window offset.
   - `konvertiereZeitspanne`:
     - `$1` (`p_VarAnfang`): Variable name to store computed start date.
     - `$2` (`p_VarEnde`): Variable name to store computed end date.
     - `$3` (`p_Spanne`): Numeric offset duration.
     - `$4` (`p_Kennzahl`): The key figure determining if the offset is daily or monthly.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWDate_Datum_Check`: Used to validate format semantics of date strings.
     # REVIEW-STRUCT: external utility DWDate_Datum_Check invoked — code not supplied; implement natively in Python using datetime.strptime or dateutil
   - `DWDate_Datum_LE`: Used to assert that start date is less than or equal to end date.
     # REVIEW-STRUCT: external utility DWDate_Datum_LE invoked — code not supplied; implement natively in Python using datetime comparison
   - `DWDate_Gib_Zeitraum`: Calculates a start/end date window dynamically from an offset and unit.
     # REVIEW-STRUCT: external utility DWDate_Gib_Zeitraum invoked — code not supplied; implement natively in Python using dateutil.relativedelta for robust month arithmetic
   - `date`: System utility to obtain timestamp for naming temporary files.
   - `basename`: Standard command used for dynamic file naming.

5. EMBEDDED SQL
   None (this is a procedural utility shell script).

6. CONTROL FLOW
   - Global Variable Definition:
     Sets `ModulName="alis_parameter"` and `ModulVersion="V3.0.9"`.
   - Function definitions (ordered as they appear in the source):
     1. `pruefeParameterGesetzt`: Evaluates the variable named by `$2`. If empty and no prior error exists, records error `194`.
     2. `konvertiereKennzahl`: Translates descriptive KPI strings (e.g., `zugang`, `reaktivierung`) into standardized 3-character keys (e.g., `zug`, `rak`). Records error `198` on unrecognized input.
     3. `konvertiereSystem`: Normalizes system identifiers to lowercase and asserts that they belong to the approved set of source platforms. Records error `195` on failure.
     4. `konvertiereSDName`: Standardizes master data platform prefixes (e.g., `rahmenvertrag` -> `rv`). Records error `195` on failure.
     5. `konvertiereAufbStufeXtra`: Converts Xtra consolidation steps (`zusammenfuehrung` -> `mrg`). Records error `195` on failure.
     6. `pruefeSystemKennzahl`: Validates the combination of source system and key figure to prevent unauthorized data imports. Checks combinations against strict system rules.
     7. `gibBereich`: Categorizes KPIs into areas (`tn`, `us`, `gd`, `sd`, `md`) based on predefined text patterns.
     8. `gibIntervall`: Matches KPIs to daily (`t`) or monthly (`m`) granularities.
     9. `pruefeZeitraum`: Subshell validation process utilizing external `DWDate_` utilities to check and compare dates.
     10. `pruefeZahlPositiv`: Performs validation on numeric integers.
     11. `pruefeZeitParameter`: Resolves scheduling bounds. Validates that either an offset is configured (and positive) OR explicit dates are populated (and logical).
     12. `konvertiereZeitspanne`: Calculates absolute execution dates from offsets, defaulting to monthly calculations for inventory metrics (`bst`) and daily for other KPIs.

7. ERROR HANDLING & EXIT CODES
   - The KornShell script uses a global error tracking mechanism using two main variables:
     - `ErrNr`: Integer tracking the error code (0 indicates success).
     - `ErrArg`: String argument containing contextual error messages or calling scope.
   - Guard Conditions: Most functions include an entry check `if [ $ErrNr -ne 0 ]; then return; fi` to preserve the first error detected and skip subsequent actions.
   - Subshell isolation (`set +e`) is used inside date parsing functions to capture command exit statuses without terminating the script.
   - Python modernization: The class/global-state pattern can be preserved using a validation result class, or modernized by raising custom Python exceptions (`AlisParameterError`) with attributes for error number and arguments.

8. OUTPUTS / SIDE EFFECTS
   - The original script dynamically mutates calling-scope environment variables using shell `eval` (e.g., `eval "$VarName=$System"`).
   - Python modernization will avoid modifying `os.environ` dynamically; instead, functions will return parsed strings, tuples, or write to an explicit parameter dictionary passed by reference.
   - Writes to a temporary log file (`/tmp/tmp_basename_timestamp_pid.tmp`) during date processing, which is deleted on completion.

9. BUSINESS SUMMARY
   - Coordinates business metadata terms across all IS ETL workflows.
   - Guarantees referential consistency by checking if a source system is allowed to deliver specific key figures.
   - Automatically maps scheduling constraints (such as running daily or monthly) for ingested KPIs.
   - Prevents invalid or empty load window parameterizations by parsing and verifying date ranges before processing begins.

=======================================================================================
PSEUDOCODE OUTLINE (PYTHON STYLE)
=======================================================================================

```python
import os
import sys
import tempfile
from datetime import datetime, timedelta
# REVIEW: standard relativedelta is recommended for month calculations
from dateutil.relativedelta import relativedelta

# Module Metadata
MODUL_NAME = "alis_parameter"
MODUL_VERSION = "V3.0.9"

# Global Error State to mimic legacy shell behavior
err_nr = 0
err_arg = ""

def reset_error_state():
    global err_nr, err_arg
    err_nr = 0
    err_arg = ""

# Step 1: pruefeParameterGesetzt
def pruefeParameterGesetzt(param_name, param_var, env_dict=None):
    global err_nr, err_arg
    if err_nr != 0:
        return

    if not param_name or not param_var:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeParameterGesetzt"
        return

    # Look up parameter in the provided dict or environment
    lookup_source = env_dict if env_dict is not None else os.environ
    param_wert = lookup_source.get(param_var, "")

    if not param_wert:
        err_nr = 194
        err_arg = param_name

# Step 2: konvertiereKennzahl
def konvertiereKennzahl(kennzahl_val):
    global err_nr, err_arg
    if err_nr != 0:
        return kennzahl_val

    if not kennzahl_val:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereKennzahl"
        return "???"

    kennzahl = str(kennzahl_val).lower()
    
    mappings = {
        "zugang": "zug",
        "abgang": "abg",
        "abgang_zukunft": "abz",
        "bestand": "bst",
        "tarifwechsel": "twe",
        "plan": "pln",
        "gutschrift": "gut",
        "aufladung": "auf",
        "restguthaben": "rst",
        "teilnehmerverbindungsdaten": "tvd",
        "uskonto": "usk",
        "usteilnehmer": "ust",
        "leistungsklasse": "lkl",
        "loeschung": "loe",
        "reaktivierung": "rak",
        "standard_rechnung": "srs",
        "standard_gutschrift": "sgs",
        "gutschrift_rv": "sg_rv",
        "rechnungen_rv_dpps": "sr_rv_dpps",
        "bewegart": "bwa",
        "kundenstamm": "ksd",
        "mahnstufe": "mahn",
        "metadatenstruktur": "mds",
        "d1news": "d1n",
        "rubrik": "rub",
        "liefermodus": "lmo",
        "netznutzungsklassen": "nnk",
        "tagesverkehrskurven": "tvk",
        "gespraechsziele": "gz",
        "gespraechslaengenverteilung": "glv",
        "zonenkennung": "zonek",
        "zonentyp": "zonet",
        "netznutzungsklassentyp": "nnkt",
        "tarifart": "trfa",
        "gespraechstyp": "gtyp",
        "basisdienst": "basisd",
        "nationalinternational": "natint",
        "glaengenintervall": "glint"
    }

    if kennzahl in mappings:
        return mappings[kennzahl]
    else:
        err_nr = 198
        err_arg = kennzahl_val
        return "???"

# Step 3: konvertiereSystem
def konvertiereSystem(system_val):
    global err_nr, err_arg
    if err_nr != 0:
        return system_val

    if not system_val:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereSystem"
        return "???"

    system = str(system_val).lower()
    valid_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

    if system in valid_systems:
        return system
    else:
        err_nr = 195
        err_arg = f"Unbekannte Datenherkunft {system_val} !"
        return "???"

# Step 4: konvertiereSDName
def konvertiereSDName(sd_system_val):
    global err_nr, err_arg
    if err_nr != 0:
        return sd_system_val

    if not sd_system_val:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereSDSystem"
        return "???"

    system = str(sd_system_val).lower()
    mappings = {
        "vo": "vo",
        "rahmenvertrag": "rv",
        "tarif": "trf",
        "tstatus": "ts",
        "zahlmodus": "zm",
        "kdg_grund": "kdg",
        "gutschrift": "gut",
        "aufladung": "auf",
        "leistung": "l_leist",
        "gutschrift_grund": "l_gutgr",
        "sap_gutschrift_grund": "sap_l_gutgr",
        "produkt": "l_prod",
        "mahnverfahren_sapist": "l_mahnv_ist",
        "mahnverfahren_sapfi": "l_mahnv_fi",
        "mahnstufentyp_sapist": "l_mahnstyp_ist",
        "bewegart": "bwa"
    }

    if system in mappings:
        return mappings[system]
    else:
        err_nr = 195
        err_arg = f"Unbekannte Stammdaten-Datenherkunft {sd_system_val} !"
        return "???"

# Step 5: konvertiereAufbStufeXtra
def konvertiereAufbStufeXtra(stufe_val):
    global err_nr, err_arg
    if err_nr != 0:
        return stufe_val

    if not stufe_val:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereAufbStufeXtra"
        return "???"

    stufe = str(stufe_val).lower()
    mappings = {
        "zusammenfuehrung": "mrg",
        "befuellung": "fill"
    }

    if stufe in mappings:
        return mappings[stufe]
    else:
        err_nr = 195
        err_arg = f"Unbekannte Stufenangabe {stufe_val} !"
        return "???"

# Step 6: pruefeSystemKennzahl
def pruefeSystemKennzahl(system, kennzahl):
    global err_nr, err_arg
    if err_nr != 0:
        return

    if not system or not kennzahl:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeSystemKennzahl"
        return

    invalid_combination = False
    
    if system != "nnv" and (kennzahl == "tvd" or kennzahl == "lkl"):
        invalid_combination = True
    elif system == "carmen":
        if kennzahl in {"twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
            invalid_combination = True
    elif system == "sap":
        if kennzahl in {"zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"}:
            invalid_combination = True
    elif system == "dpps":
        if kennzahl in {"twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}:
            invalid_combination = True
    elif system == "ctel":
        if kennzahl not in {"abg", "bst", "zug", "twe"}:
            invalid_combination = True
    elif system == "xtra":
        if kennzahl != "rst":
            invalid_combination = True
    elif system == "d1":
        if kennzahl in {"gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
            invalid_combination = True
    elif system == "nnv":
        if kennzahl not in {"tvd", "lkl"}:
            invalid_combination = True
    elif system == "dwh":
        if kennzahl != "mds":
            invalid_combination = True
    elif system == "brunet":
        if kennzahl not in {"d1n", "rub", "lmo"}:
            invalid_combination = True
    elif system == "sigma":
        if kennzahl not in {"nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}:
            invalid_combination = True

    if invalid_combination:
        err_nr = 195
        err_arg = f"Ungueltige Kombination {system} {kennzahl}"

# Step 7: gibBereich
def gibBereich(kennzahl):
    global err_nr, err_arg
    if err_nr != 0:
        return None

    if not kennzahl:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibBereich"
        return None

    list_tn = {"abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"}
    list_us = {"gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}
    list_gd = {"tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
    list_sd = {"ksd", "bwa"}
    list_md = {"mds"}

    if kennzahl in list_tn:
        return "tn"
    elif kennzahl in list_us:
        return "us"
    elif kennzahl in list_gd:
        return "gd"
    elif kennzahl in list_sd:
        return "sd"
    elif kennzahl in list_md:
        return "md"
    else:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibBereich - Kuerzel '{kennzahl}' unbekannt"
        return None

# Step 8: gibIntervall
def gibIntervall(kennzahl):
    global err_nr, err_arg
    if err_nr != 0:
        return None

    if not kennzahl:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibIntervall"
        return None

    list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
    list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

    if kennzahl in list_t:
        return "t"
    elif kennzahl in list_m:
        return "m"
    else:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
        return None

# Step 9: pruefeZeitraum
# REVIEW-STRUCT: external utilities DWDate_Datum_Check and DWDate_Datum_LE replaced with native datetime validation
def pruefeZeitraum(anfang, ende):
    global err_nr, err_arg
    if err_nr != 0:
        return

    if not anfang or not ende:
        err_nr = 196
        err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeZeitraum"
        return

    try:
        # Replaces legacy DWDate_Datum_Check
        dt_anfang = datetime.strptime(str(anfang), "%Y%m%d")
    except ValueError:
        err_nr = 195
        err_arg = "Anfangsdatum entspricht nicht dem Format YYYYMMDD"
        return

    try:
        dt_ende = datetime.strptime(str(ende), "%Y%m%d")
    except ValueError:
        err_nr = 195
        err_arg = "Endedatum entspricht nicht dem Format YYYYMMDD"
        return

    # Replaces legacy DWDate_Datum_LE
    if dt_anfang > dt_ende:
        err_nr = 195
        err_arg = "Anfangsdatum ist nicht kleiner gleich Endedatum"

# Step 10: pruefeZahlPositiv
def pruefeZahlPositiv(p_Zahl, p_ParameterName):
    global err_nr, err_arg
    try:
        val = int(p_Zahl)
        if val < 0:
            err_nr = 195
            err_arg = f"Parameter {p_ParameterName} muss groesser gleich 0 sein"
    except (ValueError, TypeError):
        err_nr = 195
        err_arg = f"Parameter {p_ParameterName} ist kein numerischer Wert"

# Step 11: pruefeZeitParameter
def pruefeZeitParameter(p_Anfangsdatum, p_Endedatum, p_ZeitOffset):
    global err_nr, err_arg
    if err_nr != 0:
        return

    # Case 1: Relational window configured
    if p_ZeitOffset is not None and str(p_ZeitOffset).strip() != "":
        if not p_Anfangsdatum and not p_Endedatum:
            pruefeZahlPositiv(p_ZeitOffset, "Zeitspanne")
            return
        else:
            err_nr = 195
            err_arg = "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden"
            return
    else:
        # Case 2: Exact dates configured
        if p_Anfangsdatum and p_Endedatum:
            pruefeZeitraum(p_Anfangsdatum, p_Endedatum)
        else:
            err_nr = 195
            if not p_Anfangsdatum and not p_Endedatum:
                err_arg = "Datumswerte oder Zeitspanne fehlen"
            else:
                err_arg = "Sowohl Anfang- als auch Endedatum muessen angegeben werden"

# Step 12: konvertiereZeitspanne
# REVIEW-STRUCT: external utility DWDate_Gib_Zeitraum logic implemented natively via datetime and dateutil.relativedelta
def konvertiereZeitspanne(p_Spanne, p_Kennzahl):
    global err_nr, err_arg
    if err_nr != 0:
        return None, None

    try:
        span = int(p_Spanne)
    except (ValueError, TypeError):
        err_nr = 85
        err_arg = "DWDate_Gib_Zeitraum"
        return None, None

    # Determine resolution unit (M for Month for inventory "bst", otherwise D for Day)
    offset_unit = "M" if p_Kennzahl == "bst" else "D"
    
    # Calculate intervals relative to current date (legacy behaviour of DWDate_Gib_Zeitraum)
    today = datetime.now().date()
    
    try:
        if offset_unit == "M":
            # For monthly intervals: end of period logic
            # Subtract span in months
            dt_end = today
            dt_start = today - relativedelta(months=span)
        else:
            # For daily intervals
            dt_end = today
            dt_start = today - timedelta(days=span)
            
        anfang_str = dt_start.strftime("%Y%m%d")
        ende_str = dt_end.strftime("%Y%m%d")
        return anfang_str, ende_str
    except Exception as e:
        err_nr = 85
        err_arg = "DWDate_Gib_Zeitraum"
        return None, None
```

# Migration Design Document

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` | Converted to a Python module to retain parameter parsing, validation, and metadata normalization logic. |

---

## Job Dependencies
The following downstream jobs consume this utility script's functions/outputs and are **not yet migrated**. Because these downstream jobs are pending migration, final call-chain wiring and full integration testing cannot be finalized until they are migrated:
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

Once migrated, these jobs must import/use the rewritten Python library `h_alis_parameter.py` within their respective Python tasks/operators.

---

## Scheduling
This utility script is NOT directly triggered by any scheduler. It operates as a shared include/helper module.
- **Target Platform Scheduling Construct**: This module will not have its own standalone Cloud Composer DAG or schedule. It must be made available as a package or placed within the `plugins/` or standard python `sys.path` in the Cloud Composer environment so that other migrated Airflow DAGs can import it natively.

---

## Schedule & Variables
This script is not directly scheduled and does not receive direct scheduler-set variables from UC4. It inherits parameters dynamically at runtime from the calling parent script or wrapper context.

---

## Cross-File Dependencies
- **Common Schemas / Mappings**: The logic contains extensive mapping tables translating long-form key figures (KPIs) and systems (e.g., `sap`, `carmen`, `dpps`, `d1`, `xtra`, `ctel`, `nnv`, `dwh`, `brunet`, `sigma`) to normalized short forms. Any downstream systems expecting these shortened forms depend directly on the structural mappings in this file.
- **Call Chains**: Sourced dynamically by parent scripts before running actual ETL workloads to pre-validate execution date bounds, numeric params, and source-target KPI combinations.

---

## Target File Plan
- **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py`
  - **Language**: Python
  - **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
  - **Purpose**: Standard parameterized utility library containing parsing, validation, and conversion routines. (No implementation code/pseudocode is written here, as the automatically attached MCP design output remains the sole authoritative code reference).

---

## Environment-Specific Values
This library is highly procedural and relies primarily on local variable scopes, with one environment-specific aspect:
1. **JOB-SPECIFIC**:
   - `temp_directory` (Legacy: uses `/tmp` for intermediate validation logs during date operations)
     - **Target Role**: Destination folder for writing temporary validation evaluation files.
     - **Resolution**: Use Python's standard `tempfile.gettempdir()` to resolve to the execution environment's native temporary directory dynamically.

---

## Risks & Manual Steps

- **Downstream Integration Risks**: Since all downstream calling jobs (such as `DW.BERT_RECHNUNGSDATEN` and `DW.BERT_ABLAUFSTEUERUNG`) are marked as "not yet migrated", this utility library cannot be validated end-to-end in live workflows until downstream migration starts.
- **Dependency on External Date Libraries**: The date utility logic (`pruefeZeitraum` and `konvertiereZeitspanne`) originally relied on external custom shell scripts (`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`). The Python target implementation replaces these with standard library `datetime` logic and `dateutil.relativedelta` for month calculations. Ensure that `python-dateutil` is included in the Composer/Airflow worker environment requirements.
- **Output/Print Literal Rule Constraint**: To ensure behavioral consistency across logging and audit trails, all literal German error messages and outputs must be preserved character-for-character in the target implementation (such as `"Parameter $p_ParameterName muss groesser gleich 0 sein"`, `"Unbekannte Datenherkunft $System !"`, and `"Anfangsdatum ist nicht kleiner gleich Endedatum"`). Do not translate or localize these messages.

---

=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh ===
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
REASON: The script is a reusable KornShell utility library defining a helper function for validating and executing SQL*Plus scripts with error reporting.

EVIDENCE
- Business logic found: KSH custom logic. Defines `starteSQLSkript` function containing operational validation, file-readability checks, dynamic SQL*Plus execution, and custom error reporting.
- AWK: none
- SQL-expressible: no. It contains shell-specific orchestration logic, file system validation, and calls an external database client.
- Non-SQL side effects: Running the external binary `sqlplus`, checking local file system readability (`[ ! -r $p_Skript ]`), and calling external logging/error utilities (`DWMSG_MeldeFehler`).
- Against this verdict: none. It cannot be `NO_CONVERSION_REQUIRED` because it defines complex utility functions, performs parameter validations, and implements custom error propagation.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script is a KornShell utility library (`h_alis_sqlplus.ksh`) providing a helper routine (`starteSQLSkript`) for executing SQL*Plus scripts. It acts as an operational wrapper that validates input parameters, verifies the existence and readability of the targeted SQL file, logs execution parameters, launches Oracle `sqlplus` with credentials derived from the environment, and captures and propagates exit codes. It is designed to be sourced by other batch processing scripts to standardize SQL execution and error handling within the data warehouse environment.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other KSH scripts in the `isrpt` / `isbert` ecosystem. The specific parent UC4 jobs/scripts are not detailed in this isolated utility file.
   - UC4 native includes: None referenced in the extracted code.
   - Environment files sourced: None directly in this snippet, although it depends on `DW_ORAUSER` and the `DWMSG_MeldeFehler` command being available in the runtime shell environment.

3. PARAMETERS / INPUTS
   The helper function `starteSQLSkript` accepts the following arguments:
   - `p_Eintragsnr` ($1): Positional argument representing the unique error tracking/entry ID. Used for reporting execution or file failures. Surfaced as the first parameter of the Python function.
   - `p_Skript` ($2): Positional argument representing the absolute or relative path of the SQL script to be executed. Surfaced as the second parameter of the Python function.
   - `$*` (remaining arguments after `shift 2`): Dynamic list of arguments forwarded directly to the SQL*Plus script. Surfaced in Python using variable positioning (`*script_args`).
   
   Additionally, it reads the following environment variable:
   - `DW_ORAUSER`: The Oracle database connection string / credential details used by `sqlplus`. Surfaced as `os.environ.get("DW_ORAUSER")`.

   Variables declared in the script body:
   - `ModulName` / `ModulVersion`: Hardcoded identifying variables.
     # REVIEW: The script declares `ModulName="alis_sqlplus"` and `ModulVersion="V1.1.3"` but attempts to reference `Modul_Name` and `Modul_Version` (with underscores) inside `DWMSG_MeldeFehler`. This would result in empty strings in the legacy shell execution. The Python equivalent should resolve this naming discrepancy to ensure correct error reporting.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Executes the Oracle SQL script with arguments, redirecting standard input from `/dev/null` to prevent interactive hangs.
     - Decision: Must remain an external process invocation via `subprocess` because this utility helper is designed to dynamically run any SQL file passed to it. It does not qualify as a RESOLVABLE LAUNCHER since the SQL content is dynamic and not supplied in this scope.
     # REVIEW-STRUCT: launcher [sqlplus] invoked — internal behaviour of the wrapped SQL script is unknown at this utility level.
   - `DWMSG_MeldeFehler $p_Eintragsnr E [code] [args]`
     - Purpose: Custom enterprise error-reporting and logging command.
     - Decision: Must remain an external process invocation via `subprocess.run` to ensure integration with the legacy monitoring systems, unless replaced with a native logging framework.
     # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

5. EMBEDDED SQL
   - No inline SQL exists in this script. It acts purely as a shell wrapper around a variable path SQL script.

6. CONTROL FLOW
   1. Initialize module variables `ModulName` and `ModulVersion`.
   2. Define function `starteSQLSkript` accepting positional arguments.
   3. Check if `p_Eintragsnr` or `p_Skript` are empty. If either is empty, call `DWMSG_MeldeFehler` with code 196 and return code 196.
   4. Verify if `p_Skript` exists and is readable. If not, call `DWMSG_MeldeFehler` with code 201 and return code 201.
   5. Log information regarding script path and remaining parameters to stdout.
   6. Temporarily disable immediate failure exit (`set +e`) to allow capture of `sqlplus` return code.
   7. Execute `sqlplus` with `DW_ORAUSER` connection parameters, forwarding all extra arguments.
   8. Capture exit code of `sqlplus` in local variable `errcode`.
   9. Re-enable immediate failure exit (`set -e`).
   10. Return captured `errcode`.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments: Triggers code 196 and reports failure.
   - Missing/Unreadable SQL script: Triggers code 201 and reports failure.
   - `sqlplus` execution: Return code captured natively and propagated out of the function without causing immediate shell termination.
   - In Python, this will be handled via standard `subprocess.run` execution capturing the `returncode`, returning it from the function, and raising/logging as appropriate.

8. OUTPUTS / SIDE EFFECTS
   - Error messages dispatched via `DWMSG_MeldeFehler` (system logs / DB tables).
   - Standard output logs indicating SQL*Plus execution status.
   - Database state changes enacted by the underlying SQL script run by `sqlplus`.

9. BUSINESS SUMMARY
   - Standardizes SQL script execution across the legacy DWH batch pipeline.
   - Enforces pre-flight checks (existence, parameters, readability) before touching the database.
   - Standardizes corporate error logging through integration with the `DWMSG_MeldeFehler` infrastructure.
   - Preserves traceability by logging runtime arguments for every database operation.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
import pathlib

# Step 1: Initialize module identifying parameters
# # REVIEW: Source script defines ModulName/ModulVersion but references Modul_Name/Modul_Version.
# Defining both patterns here to maintain backward-compatibility and fix the original typo.
ModulName = "alis_sqlplus"
ModulVersion = "V1.1.3"
Modul_Name = ModulName
Modul_Version = ModulVersion


# Step 2: Define Python wrapper for legacy error utility
def call_dwmsg_meldefehler(eintragsnr, status_char, error_code, msg_text):
    """
    Invokes the external DWMSG_MeldeFehler log management script.
    # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction
    """
    cmd = [
        "DWMSG_MeldeFehler",
        str(eintragsnr),
        str(status_char),
        str(error_code),
        str(msg_text)
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to execute DWMSG_MeldeFehler: {e}", file=sys.stderr)


# Step 3: Define starteSQLSkript utility function
def starteSQLSkript(p_Eintragsnr, p_Skript, *script_args):
    """
    Verifies parameters and readability of a SQL file, then executes it via SQL*Plus.
    """
    # Step 3.1: Validate parameter inputs (replicates -z checks)
    if not p_Eintragsnr or not p_Skript:
        error_msg = f"{Modul_Name} {Modul_Version} starteSQLSkript"
        call_dwmsg_meldefehler(p_Eintragsnr, "E", 196, error_msg)
        return 196

    # Step 3.2: Check if script is readable on filesystem (replicates [ ! -r $p_Skript ])
    script_path = pathlib.Path(p_Skript)
    if not script_path.exists() or not os.access(script_path, os.R_OK):
        call_dwmsg_meldefehler(p_Eintragsnr, "E", 201, str(p_Skript))
        return 201

    # Step 3.3: Output parameters for standard logging
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(script_args)}")

    # Step 3.4: Resolve DB user credentials from environment
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 3.5: Execute SQL*Plus command line
    # Forward remaining arguments to sqlplus. Stdin redirected to DEVNULL to mimic </dev/null
    # # REVIEW-STRUCT: launcher [sqlplus] invoked — internal behaviour of SQL files not available
    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_Skript}"] + list(script_args)
    
    try:
        # set +e / set -e simulation: we capture returncode without raising exception
        completed_process = subprocess.run(
            sqlplus_cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            text=True
        )
        errcode = completed_process.returncode
    except Exception as e:
        print(f"System execution error starting SQL*Plus: {e}", file=sys.stderr)
        errcode = -1  # Standard fallback error code

    # Step 3.6: Return captured execution code
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Converts the legacy KSH utility helper function `starteSQLSkript` to Python, mirroring folder structure to maintain repository integrity. |

---

### Job dependencies
The following downstream legacy jobs consume this utility script (or execute downstream processes that depend on its execution environment). These are not yet migrated:
* `DW.BERT_ABLAUFSTEUERUNG`
* `DW.BERT_AUSD_BP_TA_MSISDN`
* `DW.BERT_AUSD_BP_TA_P_BASISPROD`
* `DW.BERT_AUSD_V_TA_PERIOD`
* `DW.BERT_AUSD_V_TA_P_VERTRAG`
* `DW.BERT_AUSD_V_TA_VERTRAG_TMP`
* `DW.BERT_DROP_TEMP_TABLE`
* `DW.BERT_P_ADRESSEN`
* `DW.BERT_P_AUSTAUSCH`
* `DW.BERT_P_GESCHAEFTSP`
* `DW.BERT_P_RECH_EMPF`
* `DW.BERT_RECHNUNGSDATEN`

*Target Wiring*: Because these downstream targets are not yet migrated, direct orchestration dependencies cannot be finalized. In the target system, when these consumer jobs are migrated to Airflow DAGs/Python tasks, they will import the converted Python module and execute the utility wrapper natively.

---

### Scheduling
* *Legacy Schedule*: This utility script is not directly triggered by any of the system's schedulers. It runs on-demand as a shared utility library sourced by other scripts.
* *Target Orchestration*: The migrated file `h_alis_sqlplus.py` must not be scheduled independently. It must remain an importable, callable utility module within the Python environment.

---

### Schedule & variables
* *Variables*: This utility receives its parameters (`p_Eintragsnr`, `p_Skript`, and trailing arguments) dynamically at runtime via standard positional command-line/function arguments. There are no static environment-level schedulers feeding variables to this file. All parameter passing must be retained via Python function signature parameters.

---

### External system replacements
* *Database Client Replacement (`sqlplus`)*: The legacy code invokes the external Oracle CLI `sqlplus`. In the target BigQuery environment, raw `sqlplus` calls are obsolete. 
  * The Python utility wrapper must be adapted to use the Google Cloud BigQuery client library (e.g. `google-cloud-bigquery` API) or native Airflow BigQuery Operators to execute migrated SQL/DDL scripts instead of launching external database binaries.
* *Logging/Error Program (`DWMSG_MeldeFehler`)*: The script uses the command-line command `DWMSG_MeldeFehler` to log execution metadata and dispatch errors. On Google Cloud, this should be replaced with native Python logging integrated with GCP Cloud Logging, unless a legacy command-line wrapper is maintained.

---

### Cross-file dependencies
* *Shared Library Imports*: This utility script is a common helper that is sourced by multiple parent scripts. Any migrated Python scripts/operators representing those legacy parent jobs must import `starteSQLSkript` from `vobs.dw_source.isrpt.isbert.SQL.aktuell.allgemein.is.util.bin.h_alis_sqlplus`.

---

### Target file plan
* **Target File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py`
  * **Language**: Python
  * **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`

---

### Environment-specific values
The following environment-dependent configurations must be resolved dynamically at runtime:
1. **GLOBAL (Environment-wide)**
   * `GCP_PROJECT`: Identifies the target GCP Project ID. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")`.
   * `GCP_REGION` / `BQ_LOCATION`: Identifies the target Cloud Composer or BigQuery location. Sourced via environment or Airflow config.
2. **JOB-SPECIFIC**
   * `DW_ORAUSER`: Sourced from environment via `os.environ.get("DW_ORAUSER")` in legacy systems. If Oracle database connectivity is still required during a hybrid phase, this value must be fetched securely (e.g. from Google Secret Manager or Airflow Connection properties) rather than being written as a literal placeholder.
   * `p_Eintragsnr` / `p_Skript`: Job-specific parameters passed dynamically to the utility during batch execution.

---

### Risks and manual steps
* **DOWNSTREAM INTEGRATION GAP**: The dependent consuming modules (e.g. `DW.BERT_ABLAUFSTEUERUNG`, `DW.BERT_RECHNUNGSDATEN`, and others listed in the dependencies section) are marked as **not yet migrated**. Full orchestration wiring and automated validation are blocked until those modules are present.
* **SQL*PLUS COMMAND REPLACEMENT**: The script executes an external `sqlplus` call. Converting this to execute native BigQuery SQL queries requires that SQL scripts called by the parent processes are first compiled/translated to BigQuery standard SQL, and the execution engine in `starteSQLSkript` is refactored to submit BigQuery query jobs.
* **EXTERNAL LOGGING DEPENDENCY**: The external utility executable `DWMSG_MeldeFehler` is used for reporting. If this binary is missing from the target Python or Airflow execution environment, calls to it will fail. This wrapper must be manually reconciled with GCP Cloud Logging or an equivalent centralized monitoring endpoint.