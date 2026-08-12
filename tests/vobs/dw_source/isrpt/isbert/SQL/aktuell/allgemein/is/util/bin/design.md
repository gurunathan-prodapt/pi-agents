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
REASON: This is a KornShell utility library defining several error handling and logging functions that invoke PL/SQL stored procedures via SQL*Plus and manage local temporary files.

EVIDENCE
- Business logic found: KSH custom logic. The script implements a centralized logging and error-management framework ("Information Services") consisting of nine functions to handle unexpected failures, log events, create execution records, and update execution state in an Oracle DB.
- AWK: none
- SQL-expressible: no. While it executes PL/SQL database procedures, it contains heavy shell orchestration features (such as trapping signals, creating/manipulating temporary files, dynamic log naming, and shell parameter validations) that cannot be expressed purely in BigQuery SQL.
- Non-SQL side effects: Creating, reading, and removing temporary files (`/tmp/ErmittleNr_$$.lst`), formatting log file names, writing to standard error, and environment variable manipulation.
- Against this verdict: none. This is a library script designed to be sourced by other shells, requiring functional modernization in Python to serve as an importable module.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) acts as a utility library for error management and execution status tracking within the "Information Services" system. It provides shell functions that standardize log-file naming, sequence/ID generation, error reporting, and status tracking (e.g. success or failure/abort) by executing Oracle PL/SQL stored procedures. Other scripts source this library to invoke these handlers dynamically during batch processing.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other KSH batch jobs using `. f_alis_msgerr.ksh`. No direct UC4 invocation context exists for this library itself, but its sourced functions run under various UC4 jobs.
   - UC4 includes: None referenced directly in this file.
   - Environment files sourced: None. It expects global variables like `DW_ORAUSER`, `DW_DIR_ROOT`, and `DW_DIR_PROT` to be set by the calling shell context.

3. PARAMETERS / INPUTS
   - `DW_ORAUSER` (env var): Database connection string. Actually used in SQL*Plus invocations. Surface in Python as an environment variable or via a centralized database config.
   - `DW_DIR_ROOT` (env var): Root directory of the source scripts. Actually used to construct SQL script execution paths. Surface as `os.environ.get("DW_DIR_ROOT")`.
   - `DW_DIR_PROT` (env var): Destination directory for protocol logs. Actually used in `DWMSG_Logdateiname`. Surface as `os.environ.get("DW_DIR_PROT")`.

   Functions Parameters (mapped to Python arguments):
   - `DWMSG_Fehlerbehandlung`:
     - `$1` (`DWMSG_EintragsNr`): Log entry ID. Mapped to function argument `eintrags_nr`.
   - `DWMSG_SetzeStatusOK`:
     - `$1` (`DWMSG_EintragsNr`): Log entry ID. Mapped to function argument `eintrags_nr`.
   - `DWMSG_SetzeStatusAbbruch`:
     - `$1` (`DWMSG_EintragsNr`): Log entry ID. Mapped to function argument `eintrags_nr`.
   - `DWMSG_ErmittleNr`:
     - `$1` (`VarName`): Variable name to write result to via `eval`. Mapped to a Python function return value.
   - `DWMSG_ErzeugeEintrag`:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`JobKennung`), `$3` (`Programmname`), `$4` (`LogDatei`): Log entry metadata. Mapped to function arguments.
   - `DWMSG_MeldeFehler`:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`Typ`), `$3` (`FehlerNr`), `$4` (`Zusatz1`), `$5` (`Zusatz2`): Error severity and message info. Mapped to function arguments (with `$4` and `$5` optional).
   - `DWMSG_Logdateiname`:
     - `$1` (`VarName`), `$2` (`JobKennung`), `$3` (`DWMSG_EintragsNr`): File metadata. Mapped to return a string path.
   - `DWMSG_SetzeStichtagInfo`:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`DWMSG_Stichtag`), `$3` (`DWMSG_StichtagFmt`): Dates and metadata. Mapped to function arguments.
   - `DWMSG_AppendTimingInfos`:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`DWMSG_InfoText`), `$3` (`DWMSG_DateFormat`): Timing metadata. Mapped to function arguments.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus` invocations (RESOLVABLE LAUNCHER):
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
     - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
     - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
     - `sqlplus -s $DW_ORAUSER` (via inline HERE-docs inside `DWMSG_SetzeStichtagInfo` and `DWMSG_AppendTimingInfos`).
     - **Purpose:** Invoke PL/SQL procedures from the `BERT_MELDUNG` package.
     - **Modernization Recommendation:** Since SQL dialect features (Oracle PL/SQL blocks, dynamic connection string `DW_ORAUSER`) are identified, these qualify as a **RESOLVABLE LAUNCHER** pattern. Rather than invoking external `sqlplus` processes, convert them to native Python DB Client calls (e.g., using `oracledb` or your target's corresponding driver/client library) directly invoking the PL/SQL database procedures.
     - # REVIEW-STRUCT: SQL wrapper scripts (e.g., d_alis_spaufruf_p1.sql, d_al_is_ermittlenr.sql) are not supplied in this extraction. The proposed conversion uses Python native cursor calls directly executing BERT_MELDUNG package functions to eliminate dependencies on these wrapper files.

5. EMBEDDED SQL
   - **Source:** Inline HERE-doc inside `DWMSG_SetzeStichtagInfo`:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     - Statement type: PL/SQL Block.
     - Dialect: Oracle SQL*Plus.
     - Table touched: Underlying logging tables updated via the `BERT_MELDUNG` package.

   - **Source:** Inline HERE-doc inside `DWMSG_AppendTimingInfos`:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     - Statement type: PL/SQL Block.
     - Dialect: Oracle SQL*Plus.
     - Table touched: Underlying logging tables updated via the `BERT_MELDUNG` package.

   - # REVIEW: Target database platform is assumed to be Oracle because of the extensive use of PL/SQL packages (`BERT_MELDUNG`), `to_date`, `to_char`, and `SYSDATE`. If migrating to BigQuery, these package operations must be completely refactored into standard BigQuery DML writes on an equivalent log table.

6. CONTROL FLOW
   Each shell library function maps to a dedicated logical routine:
   - **Routine 1 (`DWMSG_Fehlerbehandlung`)**: Captures status code of last failing execution, triggers error registration via DB call (`BERT_MELDUNG.Fehler`), and flags run status to abort (`BERT_MELDUNG.SetzeStatusAbbruch`).
   - **Routine 2 (`DWMSG_SetzeStatusOK`)**: Validates parameter, executes `BERT_MELDUNG.SetzeStatusOk` on the database.
   - **Routine 3 (`DWMSG_SetzeStatusAbbruch`)**: Validates parameter, executes `BERT_MELDUNG.SetzeStatusAbbruch` on the database.
   - **Routine 4 (`DWMSG_ErmittleNr`)**: Employs a temporary file to write database sequence output and read it back. In Python, replace this by executing sequence queries directly on the DB cursor and fetching the result into memory, bypassing the file system.
   - **Routine 5 (`DWMSG_ErzeugeEintrag`)**: Validates run metadata, executes `BERT_MELDUNG.Erzeuge_Eintrag` on the database.
   - **Routine 6 (`DWMSG_MeldeFehler`)**: Resolves argument count (3 to 5 parameters) and routes to `BERT_MELDUNG.Fehler` call.
   - **Routine 7 (`DWMSG_Logdateiname`)**: Formats system timestamp (`%Y%m%d_%H%M`) and constructs the target log path in `DW_DIR_PROT`.
   - **Routine 8 (`DWMSG_SetzeStichtagInfo`)**: Validates parameters and commits date/timing information through `BERT_MELDUNG.SetzeZusatzInfos`.
   - **Routine 9 (`DWMSG_AppendTimingInfos`)**: Validates parameter inputs and appends custom timeline progress descriptions into the DB.

7. ERROR HANDLING & EXIT CODES
   - If a function is called with empty key arguments (e.g. `DWMSG_EintragsNr` is empty), it issues an error to stdout/stderr and halts execution with `exit 1` or `exit 2`.
   - In Python, this validation should be translated to raising a standard `ValueError` or custom exception rather than forcing a script termination (since these functions will be imported and used as a module).
   - Database operations should handle connection and driver exceptions gracefully (e.g., throwing a wrapped custom logging exception).

8. OUTPUTS / SIDE EFFECTS
   - Logs: Status and runtime changes recorded directly in the DB log tables.
   - Temp Files: Creation/removal of `/tmp/ErmittleNr_*.lst` files. This side effect is removed in Python.
   - Output files: Path name generator for logs.

9. BUSINESS SUMMARY
   - Standardizes batch program registration and execution logging across the environment.
   - Captures application/command failures and maps them to clean logging records in the target database.
   - Allows fine-grained timing checks and business date association with specific workflow processes.
   - Coordinates with downstream monitoring by providing status endpoints (Success / Aborted) for batch runs.

=======================================================================================
PSEUDOCODE OUTLINE (PYTHON)
=======================================================================================

```python
# Step 1: Import standard modules and declare environment parameter bindings
import os
import sys
import datetime
import tempfile

# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")

# Dummy DB connection framework (replace with actual oracledb/SQLAlchemy connection setup)
def get_db_cursor():
    # Placeholder for database client instantiation using DW_ORAUSER
    # return conn.cursor()
    pass

# Step 2: Define DWMSG_Fehlerbehandlung (unexpected error handler)
def dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code=1):
    # Capture last error code from subprocess / preceding steps
    fehler_nr = last_exit_code
    unerw_fehler = 10
    
    # Log the unexpected shell exception details
    dwmsg_melde_fehler(eintrags_nr, "F", unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus", file=sys.stderr)
    dwmsg_setze_status_abbruch(eintrags_nr)

# Step 3: Define DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        raise ValueError("Missing log entry ID")
    
    # Execute PL/SQL package procedure natively on database
    # REVIEW-STRUCT: SQL wrapper script d_alis_spaufruf_p1.sql not supplied. Converting to native call.
    cursor = get_db_cursor()
    cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
    cursor.connection.commit()

# Step 4: Define DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        raise ValueError("Missing log entry ID")
        
    # Execute PL/SQL package procedure natively on database
    # REVIEW-STRUCT: SQL wrapper script d_alis_spaufruf_p1.sql not supplied. Converting to native call.
    cursor = get_db_cursor()
    cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
    cursor.connection.commit()

# Step 5: Define DWMSG_ErmittleNr (replaces temp-file based sequence generator)
def dwmsg_ermittle_nr():
    # Instead of writing output of d_al_is_ermittlenr.sql to local /tmp file, 
    # query DB sequence directly and capture in variable
    # REVIEW-STRUCT: d_al_is_ermittlenr.sql not supplied. Assuming sequence/run id query behavior.
    cursor = get_db_cursor()
    cursor.execute("SELECT BERT_MELDUNG_SEQ.nextval FROM dual")  # Hypothetical sequence retrieval
    row = cursor.fetchone()
    if row:
        eintrags_nr = str(row[0]).strip()
        return eintrags_nr
    else:
        raise RuntimeError("Could not retrieve entry sequence number from Database")

# Step 6: Define DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programm_name, log_datei):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        raise ValueError("Missing log entry ID")
        
    # Execute PL/SQL registration procedure natively
    # REVIEW-STRUCT: SQL wrapper script d_alis_spaufruf_p4.sql not supplied. Converting to native call.
    cursor = get_db_cursor()
    cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programm_name, log_datei])
    cursor.connection.commit()

# Step 7: Define DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        raise ValueError("Missing log entry ID")
        
    # Execute PL/SQL Fehler procedure natively
    cursor = get_db_cursor()
    cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
    cursor.connection.commit()

# Step 8: Define DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dw_dir_prot = DW_DIR_PROT if DW_DIR_PROT else "/tmp"
    
    dateiname = f"{dw_dir_prot}/{job_kennung}_{date_str}_{eintrags_nr}.log"
    return dateiname

# Step 9: Define DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        raise ValueError("Missing log entry ID")
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        raise ValueError("Missing Stichtag parameter")
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        raise ValueError("Missing Date Format string")
        
    cursor = get_db_cursor()
    # Call stored procedure using native date conversion
    query = """
    BEGIN
        BERT_MELDUNG.SetzeZusatzInfos(:1, TO_DATE(:2, :3));
    END;
    """
    cursor.execute(query, [eintrags_nr, stichtag, stichtag_fmt])
    cursor.connection.commit()

# Step 10: Define DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        raise ValueError("Missing log entry ID")
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        raise ValueError("Missing Date Format string")
        
    cursor = get_db_cursor()
    # Call stored procedure using DB-native dates
    query = """
    BEGIN
        BERT_MELDUNG.SetzeZusatzInfos(:1, NULL, :2 || ' ' || TO_CHAR(SYSDATE, :3) || ' ');
    END;
    """
    cursor.execute(query, [eintrags_nr, info_text, date_format])
    cursor.connection.commit()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | Migrated from KornShell to Python to serve as an importable shared logging and error handling module in Cloud Composer. |

---

### Job Dependencies
* **Downstream Jobs (not yet migrated)**:
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

* **Wiring on BigQuery / Cloud Composer**: 
  Because these downstream consumer jobs are not yet migrated, their direct import and function-call structures cannot be finalized. Once migrated, they will import `f_alis_msgerr` as a Python module within their respective Airflow DAG execution environments to handle standard process registration, status reporting, and unexpected error logging.

---

### Scheduling
* **Trigger and Frequency**: This utility script is a passive, shared helper library. It is not directly triggered by any scheduler and has no standalone execution schedule. It will remain a callable, importable Python module invoked dynamically inside other batch workloads' Composer/Airflow DAG tasks.

---

### Schedule & Variables — Must Be Retained
* **Scheduling Equivalence**: This job remains unscheduled and will be deployed as an importable helper library.
* **Environment Variables**:
  * `DW_ORAUSER`: Contains the database credentials. Will be mapped to a standard database connection in the environment or a secure Airflow Connection/Secret Manager reference.
  * `DW_DIR_ROOT`: Sourced globally to identify root execution directories.
  * `DW_DIR_PROT`: Sourced globally to identify target log and protocol output directories.

---

### Lineage
* **Upstream Source**: None (invoked as an internal utility).
* **Downstream Consumers**: Invokes the `BERT_MELDUNG` package procedures in the database context. Specifically, calls procedure `SETZEZUSATZINFOS` (confidence = 0.75).

---

### External System Replacements
* **Oracle SQL*Plus Client**: The legacy system uses `sqlplus` commands to execute the PL/SQL database procedures inside the `BERT_MELDUNG` package.
* **BigQuery / Target Platform Alternative**: Because the target platform is BigQuery, standard Oracle PL/SQL package execution is not natively supported.
  * **Option A (Interim / Hybrid Migration)**: Maintain audit metadata on an Oracle instance, migrating `sqlplus` invocations to a native Python Oracle database client (such as `oracledb` or `sqlalchemy`) that connects using `DW_ORAUSER`.
  * **Option B (BigQuery Native)**: Refactor the logging actions to write structured audit rows directly to a BigQuery dataset table (e.g., `audit_logs.job_status`) using the Python Google Cloud BigQuery client library (`google.cloud.bigquery`).

---

### Cross-File Dependencies
* **SQL Wrapper Scripts**: The legacy utility script depends on several standalone helper SQL scripts:
  * `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql` (invokes procedures with 1 parameter)
  * `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql` (invokes procedures with 4 parameters)
  * `@$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql` (extracts run-IDs / sequence numbers)
  * Inferred script names like `d_alis_spaufruf_p3.sql` and `d_alis_spaufruf_p5.sql` based on parameter counts.
* **Modernization Replacement**: In the target Python code, these external wrapper files are retired. All database statements, procedure executions, or sequence queries are issued directly via the connection client object, consolidating the logic and eliminating cross-file dependencies.

---

### Target File Plan
* **Target Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py`
* **Language**: `python`
* **Source Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
* **Purpose**: Houses the migrated Python helper functions corresponding to the original KornShell logging functions (such as `dwmsg_setze_status_ok`, `dwmsg_melde_fehler`, and `dwmsg_fehlerbehandlung`) to be imported by downstream PythonOperators.

---

### Environment-Specific Values

#### Global (Environment-Wide)
* **`GCP_PROJECT`**: The target Google Cloud Project ID. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Cloud Composer configurations.
* **`BQ_DATASET`**: The target auditing/logging BigQuery dataset. Sourced via Airflow Variables.
* **`DW_DIR_ROOT`**: Sourced via `os.environ.get("DW_DIR_ROOT")` to resolve script directories.
* **`DW_DIR_PROT`**: Sourced via `os.environ.get("DW_DIR_PROT")` or mapped to a standard Google Cloud Storage bucket path (`GCS_BUCKET`) for job logs.
* **`DW_ORAUSER`**: Legacy database connection identifier. Sourced at runtime via `os.environ.get("DW_ORAUSER")` or a secure database connection pool secret.

#### Job-Specific
* **`BERT_MELDUNG`**: Mapped to the specific audit table/package on the target platform. (e.g. `{GCP_PROJECT}.{BQ_DATASET}.audit_log` inside SQL statements).

---

### Risks & Manual Steps

* **Downstream Integration Pipeline**:
  * **SOURCE: NOT FOUND** — Upstream/Downstream Jobs: The 12 downstream consumers (such as `DW.BERT_ABLAUFSTEUERUNG`, `DW.BERT_AUSD_BP_TA_MSISDN`, etc.) are not yet migrated. The integration wiring and module imports cannot be fully verified or finalized until those components exist on the target system.
* **PL/SQL Logging Emulation**: The legacy procedures (`BERT_MELDUNG.Fehler`, `BERT_MELDUNG.SetzeStatusOk`, etc.) write to dynamic database schemas and log tables. If migrating fully to BigQuery, these database-side routines must be redesigned as BQ DML operations. If they are to remain on an interim relational database, the client connection credentials and driver libraries must be set up and configured in the Cloud Composer environment.
* **Local File System Dependencies**: The legacy sequence generator `DWMSG_ErmittleNr` uses `/tmp/ErmittleNr_$$.lst` on local disk to capture database output. The Python script replaces this by resolving the query directly in memory, mitigating local disk access risks in serverless or containerized runtimes.
* **German Logging Literals**: German log and error output statements (e.g., `"Argh!, keine EintragsNummer bei Aufruf..."`) are retained exactly as-is to preserve operational continuity for existing diagnostic parsers and support teams. Ensure these output formats are strictly preserved downstream.

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
REASON: This is a date calculation utility library containing multiple KornShell function definitions with nested logic, string slicing, mathematical operations, and inline Oracle SQL*Plus and PL/SQL queries.

EVIDENCE
- Business logic found: KSH custom logic. The script defines a library of reusable date processing functions, including calendar math, range generation, date padding, and format checking.
- AWK: none
- SQL-expressible: No. While some functions invoke Oracle via SQL*Plus to perform validations and date ranges, the core script is structured as a modular Shell library defining functions, local variables, mathematical assignments, and loops that cannot be expressed as a static BigQuery transformation.
- Non-SQL side effects: Writes temporary files under `/tmp`, relies on standard output parsing via grep/cut, and modifies script-level variables via dynamic `eval` assignments.
- Against this verdict: If all dependent scripts were rewritten to use BigQuery's native date functions, this utility library would not need conversion. However, because it is a referenced shell library file, a Python equivalent is required to support the migration of the surrounding orchestration scripts.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
This script (`h_alis_date.ksh`) is a reusable KornShell date utility library. It provides helper functions for performing date arithmetic, checking date validity, validating date order, and determining date ranges. These routines are used by other Data Warehouse scripts to establish date boundaries and execute partition validation against an Oracle database.

2. INVOCATION CONTEXT
- **Caller**: This library is sourced (`. h_alis_date.ksh`) by other Data Warehouse scripts. It is not executed directly as a standalone UC4 job.
- **UC4 Includes**: None referenced in the script itself.
- **Environment Files Sourced**: 
  - The script's header indicates that `.dw_init` must be sourced before executing these utilities, or `DW_DIR_ROOT` and `DW_ORAUSER` must be set in the shell environment.
  - # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
The script defines multiple functions with localized positional parameters:
- **`DWDate_Vormonat`**:
  - `VarName` (positional `$1`): Name of the calling script's variable to receive the result.
  - `DWDate_FMT` (positional `$2`): Oracle format string.
- **`DWDate_Datum_Check`**:
  - `wert` (positional `$1`): Date value string to be verified.
  - `format` (positional `$2`): Format of the date value string.
- **`DWDate_Datum_LE`**:
  - `datum1` (positional `$1`): First date string (expected format `YYYYMMDD`).
  - `datum2` (positional `$2`): Second date string (expected format `YYYYMMDD`).
- **`DWDate_Gib_Zeitraum`**:
  - `Offset` (positional `$1`): Integer offset value.
  - `Stufe` (positional `$2`): Step unit (`'Y'` for Year, `'M'` for Month, `'D'` for Day).
  - `Format` (positional `$3`): Output date format.
  - `Var_Start` (positional `$4`): Name of the calling script's variable to store the calculated start date.
  - `Var_Ende` (positional `$5`): Name of the calling script's variable to store the calculated end date.
- **`LetzterTagDesMonats`**:
  - Positional `$1`: Date string in `YYYYMMDD` format.
- **`TageimMonat`**:
  - Positional `$1`: Year (`YYYY`).
  - Positional `$2`: Month (`MM`).
- **`AddiereDatum`**:
  - Positional `$1`: Base date string in `YYYYMMDD` format.
  - Positional `$2`: Days to add (integer).

Global Environment Variables used:
- `DW_ORAUSER`: Oracle connection credential string.
- `DW_DIR_ROOT`: Root directory path for SQL scripts.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **`sqlplus`**:
  - Verbatim invocation in `DWDate_Vormonat`:
    `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql $DWDate_tmpFile $DWDate_FMT </dev/null`
  - Verbatim inline invocation in `DWDate_Datum_Check`:
    `sqlplus -s <<EOF ... EOF`
  - Verbatim inline PL/SQL invocation in `DWDate_Datum_LE`:
    `sqlplus -s <<EOF ... EOF`
  - Verbatim invocation in `DWDate_Gib_Zeitraum`:
    `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql $tmpFile $Offset $Stufe $Format </dev/null`
  - *Recommendation*: Since these calls utilize standard database validations, they should be implemented via a native Python database client (such as `oracledb`) using credentials loaded from the environment.
  - # REVIEW-STRUCT: connection parameters inferred from Oracle SQL*Plus usage — confirm these exact env var names (e.g. DW_ORAUSER) are set in this job's actual runtime environment before deploying

5. EMBEDDED SQL
- **Inline SQL in `DWDate_Datum_Check`**:
  ```sql
  select to_date('$wert','$format') from dual;
  ```
  - Statement Type: SELECT
  - Tables Touched: `dual`
  - Dialect: Oracle SQL*Plus (indicated by explicit `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` and `SET HEADING OFF`)

- **Inline PL/SQL in `DWDate_Datum_LE`**:
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
  /
  ```
  - Statement Type: PL/SQL Block
  - Tables Touched: None
  - Dialect: Oracle PL/SQL

- **External Script calls**:
  - `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql`
    - # REVIEW-STRUCT: SQL file [d_alis_vormonat.sql] body not supplied — behaviour unknown
  - `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql`
    - # REVIEW-STRUCT: SQL file [d_alis_datum_zeitraum.sql] body not supplied — behaviour unknown

6. CONTROL FLOW
The script consists of modular helper functions:
1. **`DWDate_Vormonat`**:
   - Step 1.1: Generate a temporary file path `/tmp/h_alis_date_basename_$0_$$.tmp`.
   - Step 1.2: Execute `d_alis_vormonat.sql` via SQL*Plus to calculate the previous month, passing the temp file and format string as arguments.
   - Step 1.3: Read the temp file using `cat` and dynamically assign the value to the variable name specified in parameter `$1` using shell `eval`.
   - Step 1.4: Execute cleanup. (Note: The original script contains a bug where it calls `rm -f $DWDate_FMT` instead of the temp file variable `$DWDate_tmpFile`. In Python, this will be corrected to clean up the temporary database extraction file.)
2. **`DWDate_Datum_Check`**:
   - Step 2.1: Verify argument count is exactly 2. Return 1 if incorrect.
   - Step 2.2: Launch SQL*Plus to check if string `$wert` fits `$format` using `TO_DATE()`.
   - Step 2.3: Return the exit status of SQL*Plus (`$?`).
3. **`DWDate_Datum_LE`**:
   - Step 3.1: Verify argument count is exactly 2. Return 1 if incorrect.
   - Step 3.2: Launch SQL*Plus running PL/SQL block comparing two parsed dates. Raise application error `-20422` if `datum1` > `datum2`.
   - Step 3.3: Return the exit status of SQL*Plus (`$?`).
4. **`DWDate_Gib_Zeitraum`**:
   - Step 4.1: Verify argument count is exactly 5. Return 1 if incorrect.
   - Step 4.2: Build a dynamic temporary file path `/tmp/tmp_basename_$0_date_%Y%m%d%H%M%S_$.tmp`.
   - Step 4.3: Execute `d_alis_datum_zeitraum.sql` via SQL*Plus passing arguments.
   - Step 4.4: Count occurrences of string `"DWH_Ergebnis;"` in the temp file. If count is not exactly 1, print an error and return 1.
   - Step 4.5: Parse result values using `grep` and `cut` on semi-colon delimiter. Set output variable names passed in parameters `$4` and `$5` using `eval`.
   - Step 4.6: Delete the temporary file.
5. **`LetzterTagDesMonats`**:
   - Step 5.1: Slice date `$1` into Year, Month, and Day variables.
   - Step 5.2: Verify if Year is a leap year using standard modulo conditions.
   - Step 5.3: Look up last day of Month in a static array.
   - Step 5.4: Compare the day of input to the array-retrieved last day. Return 0 if identical, 1 if not.
6. **`TageimMonat`**:
   - Step 6.1: Check if year parameter `$1` is a leap year.
   - Step 6.2: Return month length value from static array based on month parameter `$2`.
7. **`AddiereDatum`**:
   - Step 7.1: Slice date `$1` into Year, Month, Day variables.
   - Step 7.2: Add parameter `$2` directly to Day variable.
   - Step 7.3: Loop to handle positive day values that exceed `TageimMonat`. Subtract month length from day, increment month, and carry over to year if month > 12.
   - Step 7.4: Pad values to fixed widths (Day 2 digits, Month 2 digits, Year 4 digits) using string tails.
   - Step 7.5: Echo the reconstructed date string.

7. ERROR HANDLING & EXIT CODES
- Shell validation errors (such as incorrect argument count) return exit status `1`.
- Database exceptions are caught via `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` and returned as the SQL*Plus failure status to the shell.
- In Python, these will raise native exceptions (`ValueError`, `subprocess.CalledProcessError`) or return boolean flags.

8. OUTPUTS / SIDE EFFECTS
- Temp files in `/tmp` containing query results (cleaned up after evaluation).
- Dynamic variable modifications inside the sourcing shell script context using `eval`.

9. BUSINESS SUMMARY
- Reusable library for processing data warehouse date calculations.
- Verifies business partition boundaries and checks that process date sequences are valid.
- Manages standard calendar logic, leap years, month lengths, and date math offsets.

=======================================================================================
PSEUDOCODE OUTLINE (PYTHON MODULE STYLE)
=======================================================================================

```python
import os
import sys
import datetime
import tempfile
import subprocess
import calendar

# Global parameters assumed from shell initialization
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")

# Step 1: DWDate_Vormonat equivalent
def dw_date_vormonat(format_str: str) -> str:
    """
    Calculates previous month using external Oracle SQL file.
    Replaces shell-based temp files and eval dynamic assignments.
    """
    # Step 1.1: Build temporary file path
    temp_file = tempfile.NamedTemporaryFile(mode='w+', prefix='h_alis_date_', suffix='.tmp', delete=False)
    temp_file_name = temp_file.name
    temp_file.close()

    try:
        # Step 1.2: Execute external Oracle script via subprocess
        # # REVIEW-STRUCT: SQL file d_alis_vormonat.sql body not supplied — behaviour unknown
        sql_script_path = os.path.join(DW_DIR_ROOT, "allgemein/is/util/sql/d_alis_vormonat.sql")
        cmd = ["sqlplus", "-s", DW_ORAUSER, f"@{sql_script_path}", temp_file_name, format_str]
        subprocess.run(cmd, stdin=subprocess.DEVNULL, check=True)

        # Step 1.3: Read results
        with open(temp_file_name, 'r') as f:
            result = f.read().strip()
        return result
    finally:
        # Step 1.4: Cleanup temporary file
        if os.path.exists(temp_file_name):
            os.remove(temp_file_name)


# Step 2: DWDate_Datum_Check equivalent
def dw_date_datum_check(wert: str, format_str: str) -> bool:
    """
    Validates a date string using database verification.
    """
    # Step 2.1: Parameter count validation
    if not wert or not format_str:
        raise ValueError("DWDate_Datum_Check requires exactly 2 parameters")

    # Step 2.2: Execute inline validation via Oracle SQL*Plus
    sql_command = f"""
    {DW_ORAUSER}
    WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
    SET HEADING OFF;
    select to_date('{wert}','{format_str}') from dual;
    """
    try:
        subprocess.run(["sqlplus", "-s"], input=sql_command, text=True, capture_output=True, check=True)
        return True
    except subprocess.CalledProcessError:
        # Step 2.3: Return False if database check returns failure code
        return False


# Step 3: DWDate_Datum_LE equivalent
def dw_date_datum_le(datum1: str, datum2: str) -> bool:
    """
    Verifies if datum1 <= datum2 using database evaluation.
    """
    # Step 3.1: Parameter count validation
    if not datum1 or not datum2:
        raise ValueError("DWDate_Datum_LE requires exactly 2 parameters")

    format_str = "YYYYMMDD"
    # Step 3.2: Execute inline PL/SQL comparison block
    plsql_command = f"""
    {DW_ORAUSER}
    WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
    SET HEADING OFF;
    DECLARE
        datum1 DATE;
        datum2 DATE;
    BEGIN
        datum1:=TO_DATE('{datum1}','{format_str}');
        datum2:=TO_DATE('{datum2}','{format_str}');
        IF datum1>datum2 
        THEN
            raise_application_error(-20422,'Datum {datum1} ist groesser als {datum2}');
        END IF;
    END;
    /
    """
    try:
        subprocess.run(["sqlplus", "-s"], input=plsql_command, text=True, capture_output=True, check=True)
        return True
    except subprocess.CalledProcessError:
        # Step 3.3: Return False if date verification fails
        return False


# Step 4: DWDate_Gib_Zeitraum equivalent
def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    """
    Generates a start and end range by executing an Oracle SQL utility.
    """
    # Step 4.1: Parameter validation
    if offset is None or not stufe or not format_str:
        raise ValueError("DWDate_Gib_Zeitraum requires offset, stufe, and format parameters")

    # Step 4.2: Build temporary file name with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    temp_file_name = f"/tmp/tmp_h_alis_date_{timestamp}_{os.getpid()}.tmp"

    try:
        # Step 4.3: Execute query external script via SQL*Plus
        # # REVIEW-STRUCT: SQL file d_alis_datum_zeitraum.sql body not supplied — behaviour unknown
        sql_script_path = os.path.join(DW_DIR_ROOT, "allgemein/is/util/sql/d_alis_datum_zeitraum.sql")
        cmd = ["sqlplus", "-s", DW_ORAUSER, f"@{sql_script_path}", temp_file_name, str(offset), stufe, format_str]
        subprocess.run(cmd, stdin=subprocess.DEVNULL, check=True)

        # Step 4.4: Parse file results
        if not os.path.exists(temp_file_name):
            raise FileNotFoundError(f"Result file {temp_file_name} not generated")

        matching_lines = []
        with open(temp_file_name, 'r') as f:
            for line in f:
                if "DWH_Ergebnis;" in line:
                    matching_lines.append(line.strip())

        # Verify pattern count
        if len(matching_lines) != 1:
            print(f"!! Interner Fehler bei der Rueckgabe von Datumswerten\nFunktion: DWDate_Gib_Zeitraum\n1 Zeile erwartet, {len(matching_lines)} Zeile(n) bekommen", file=sys.stderr)
            raise RuntimeError("DWDate_Gib_Zeitraum return verification failed")

        # Step 4.5: Extract values
        parts = matching_lines[0].split(";")
        start_date = parts[1]
        end_date = parts[2]

        return start_date, end_date

    finally:
        # Step 4.6: Cleanup temporary file
        if os.path.exists(temp_file_name):
            os.remove(temp_file_name)


# Step 5: LetzterTagDesMonats equivalent
def letzter_tag_des_monats(date_str: str) -> bool:
    """
    Returns True if date_str (YYYYMMDD) represents the last day of the month.
    """
    # Step 5.1: Parse strings
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])

    # Step 5.2 & 5.3: Utilize Python standard library calendar module (leap year handled natively)
    _, last_day = calendar.monthrange(year, month)

    # Step 5.4: Verify if input day matches the last day
    return day == last_day


# Step 6: TageimMonat equivalent
def tage_im_monat(year: int, month: int) -> int:
    """
    Returns the count of days for the specified month and year.
    """
    # Step 6.1 & 6.2: Fetch using native calendar module
    _, days = calendar.monthrange(year, month)
    return days


# Step 7: AddiereDatum equivalent
def addiere_datum(date_str: str, days_to_add: int) -> str:
    """
    Translates custom date loop addition logic using native Python datetime.
    """
    # Step 7.1 & 7.2: Parse input and apply offset
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    # Step 7.3: Execute date addition using standard timedelta logic
    new_dt = dt + datetime.timedelta(days=days_to_add)
    # Step 7.4 & 7.5: Return formatted string
    return new_dt.strftime("%Y%m%d")
```

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py` | Migrating the KornShell utility library to a native Python module. This module provides date validations, math, and range checks that can be imported directly by other migrated Python scripts or Airflow operators. |

### Job Dependencies
The following downstream consumer jobs depend on this date utility library. None of them have been migrated yet. Orchestration wiring cannot be finalized until these downstream jobs are migrated to the target platform:
* `DW.BERT_ABLAUFSTEUERUNG` (not yet migrated)
* `DW.BERT_AUSD_BP_TA_MSISDN` (not yet migrated)
* `DW.BERT_AUSD_BP_TA_P_BASISPROD` (not yet migrated)
* `DW.BERT_AUSD_V_TA_PERIOD` (not yet migrated)
* `DW.BERT_AUSD_V_TA_P_VERTRAG` (not yet migrated)
* `DW.BERT_AUSD_V_TA_VERTRAG_TMP` (not yet migrated)
* `DW.BERT_DROP_TEMP_TABLE` (not yet migrated)
* `DW.BERT_P_ADRESSEN` (not yet migrated)
* `DW.BERT_P_AUSTAUSCH` (not yet migrated)
* `DW.BERT_P_GESCHAEFTSP` (not yet migrated)
* `DW.BERT_P_RECH_EMPF` (not yet migrated)
* `DW.BERT_RECHNUNGSDATEN` (not yet migrated)

### Schedule & Variables
* **Schedule**: This job is not directly triggered by any scheduler. It operates as an included/shared library module sourced by other scripts. In the target BigQuery / Cloud Composer architecture, it must remain a callable/importable Python module (`h_alis_date.py`) and should not have its own standalone schedule.
* **Variables**:
  * `DW_ORAUSER`: Legacy Oracle connection parameter. Replaced in target via global credentials (BigQuery service account or execution environment parameters).
  * `DW_DIR_ROOT`: Legacy code base path root directory. In the Python target, this is handled dynamically using Python's standard pathing/imports or relative project directories.

### Lineage
* **Upstream Producers**: The utility script accesses the Oracle dummy table `TABLE:DUAL` to run standard date checking expressions. On BigQuery, this dependency is eliminated by using native Python libraries (`datetime`, `calendar`) which compute this logic in-memory without database roundtrips.

### External System Replacements
* **Oracle SQL*Plus to Python / BigQuery**: Legacy date arithmetic routines and format validations executed via SQL*Plus or PL/SQL are replaced with Python's standard `datetime` and `calendar` modules. If dynamic database queries are needed for specific tables, they will be executed using the native Google Cloud BigQuery client library.

### Cross-File Dependencies
* This helper library is widely used across different SQL and shell scripts. Migrating it to a standardized Python module (`h_alis_date.py`) ensures that dependent Python tasks can invoke its functions directly via standard python imports (`import h_alis_date`).

### Target File Plan
* **`vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py`**:
  * **Language**: Python
  * **Source**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
  * **Purpose**: Implements helper functions (`dw_date_vormonat`, `dw_date_datum_check`, `dw_date_datum_le`, `dw_date_gib_zeitraum`, `letzter_tag_des_monats`, `tage_im_monat`, `addiere_datum`) using pure Python logic, removing unnecessary Oracle database hits where possible.

### Environment-Specific Values
1. **GLOBAL (Environment-Wide)**:
   * `GCP_PROJECT`: Global GCP project identifier. Sourced at runtime using `os.environ.get("GCP_PROJECT")` if BigQuery interactions are required.
   * `BQ_DATASET`: Environment-wide dataset namespace for running queries, retrieved via `os.environ.get("BQ_DATASET")`.
   * `DW_ORAUSER`: Legacy connection string. Mapped to BigQuery client authentication credentials configured globally in Composer.
   * `DW_DIR_ROOT`: Legacy codebase root folder. Solved in Cloud Composer by resolving imports relative to the Python path or DAGs directory.

2. **JOB-SPECIFIC**:
   * None. No specific runtime parameters or hardcoded environment constants are needed; all variables are supplied dynamically via function parameters.

### Risks and Manual Steps
* **SOURCE: NOT FOUND** — `d_alis_vormonat.sql` — no candidate. (Referenced in legacy function `DWDate_Vormonat` to extract the previous month. Its source code was not in the payload and must be manually migrated to Python/BQSQL).
* **SOURCE: NOT FOUND** — `d_alis_datum_zeitraum.sql` — no candidate. (Referenced in legacy function `DWDate_Gib_Zeitraum` to determine date intervals. The SQL file is missing and must be manually evaluated and implemented).
* **Downstream Integration**: Since all 12 downstream consumer jobs are unmigrated, their direct orchestration integration with this utility cannot be validated. They must be updated during their respective migration phases.

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
REASON: The script is a library of helper functions for parameter validation, string mapping, and date calculations that must be converted to Python utility functions.

EVIDENCE
- Business logic found: KSH custom logic contains parameter validation, key-figure and source-system normalization mappings, and date-handling operations.
- AWK: none
- SQL-expressible: no, this script contains programmatic string mappings, validations, and custom date range calculations that operate on environment state, not tabular database data.
- Non-SQL side effects: none (operates entirely on environment state variable modification).
- Against this verdict: none, as this is a utility/library script designed to be sourced by other processes to manage execution state.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script, `h_alis_parameter.ksh`, is a KornShell library containing utility functions used to parse, validate, and convert job parameters in the Information System (IS) environment. It normalizes key figures (Kennzahlen), master data types, and source systems (Liefersysteme), enforces domain compatibility rules, verifies date formats/chronology, and calculates dates based on relative offsets. It is designed to be sourced by other operational processing scripts rather than being run as a standalone executable.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other loader or transformation KornShell scripts (e.g., via `. h_alis_parameter.ksh`) to provide validation functions. It has no direct UC4 wrapper of its own but runs within the context of whatever UC4 job launches its consumer scripts.
   - UC4 native includes: None referenced in the extracted source.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   This script does not accept standalone command-line arguments but defines functions that accept positional arguments to modify or read environment states dynamically.
   - `pruefeParameterGesetzt`:
     - `$1`: Descriptive parameter name (for error messages).
     - `$2`: Name of the environment variable to check.
   - `konvertiereKennzahl`:
     - `$1`: Name of the environment variable holding the key figure name (modified in-place).
   - `konvertiereSystem`:
     - `$1`: Name of the environment variable holding the system name (modified in-place).
   - `konvertiereSDName`:
     - `$1`: Name of the environment variable holding the master data system name (modified in-place).
   - `konvertiereAufbStufeXtra`:
     - `$1`: Name of the environment variable holding the stage name (modified in-place).
   - `pruefeSystemKennzahl`:
     - `$1`: Normalised system name.
     - `$2`: Normalised key figure name.
   - `gibBereich`:
     - `$1`: Normalised key figure name.
     - `$2`: Name of the environment variable to set with the mapped functional area (Bereich).
   - `gibIntervall`:
     - `$1`: Normalised key figure name.
     - `$2`: Name of the environment variable to set with the mapped reporting frequency interval (Intervall).
   - `pruefeZeitraum`:
     - `$1`: Start date string (expected format `YYYYMMDD`).
     - `$2`: End date string (expected format `YYYYMMDD`).
   - `pruefeZahlPositiv`:
     - `$1`: Value to check.
     - `$2`: Descriptive parameter name.
   - `pruefeZeitParameter`:
     - `$1`: Start date value.
     - `$2`: End date value.
     - `$3`: Time offset span.
   - `konvertiereZeitspanne`:
     - `$1`: Name of env variable to set with calculated start date.
     - `$2`: Name of env variable to set with calculated end date.
     - `$3`: Numeric offset span.
     - `$4`: Normalised key figure name.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWDate_Datum_Check`:
     - Exact command line: `DWDate_Datum_Check $Wert $Format`
     - Purpose: Verifies if a date value matches the specified format (`YYYYMMDD`).
     - Standard handling: Convert to native Python `datetime.datetime.strptime()`.
     - # REVIEW-STRUCT: command [DWDate_Datum_Check] logic is not supplied in this extraction — behavior is mapped to standard Python datetime validation.
   - `DWDate_Datum_LE`:
     - Exact command line: `DWDate_Datum_LE $Anfang $Ende`
     - Purpose: Asserts that the start date is less than or equal to the end date.
     - Standard handling: Convert to native Python date comparisons (`start <= end`).
     - # REVIEW-STRUCT: command [DWDate_Datum_LE] logic is not supplied in this extraction — behavior is mapped to standard Python date comparisons.
   - `DWDate_Gib_Zeitraum`:
     - Exact command line: `DWDate_Gib_Zeitraum -$p_Spanne $Offset_Unit "YYYYMMDD" Anfangsdatum Endedatum`
     - Purpose: Calculates a relative historical reporting window.
     - Standard handling: Implement using Python's `datetime` math and `relativedelta`.
     - # REVIEW-STRUCT: command [DWDate_Gib_Zeitraum] logic is not supplied in this extraction — behavior is mapped to standard Python date arithmetic.
   - `date`:
     - Exact command line: `date +%Y%m%d%H%M%S`
     - Purpose: Standard UNIX date command used for unique temporary log filename creation.
     - Standard handling: Python `datetime.datetime.now().strftime("%Y%m%d%H%M%S")`.
   - `basename`:
     - Exact command line: `basename $0`
     - Purpose: Extracts the base filename of the executing script.
     - Standard handling: Python `os.path.basename(sys.argv[0])`.

5. EMBEDDED SQL
   - None.

6. CONTROL FLOW
   Upon sourcing, the library performs sequential initialization:
   1. Set global module metadata variables: `ModulName="alis_parameter"` and `ModulVersion="V3.0.9"`.
   2. Define `pruefeParameterGesetzt`: Checks if a variable is populated; if not, sets error variables.
   3. Define `konvertiereKennzahl`: Normative translation dictionary matching verbose German key figure terms to short abbreviations (or setting `ErrNr=198`).
   4. Define `konvertiereSystem`: Normalizes source systems to lowercase and matches against allowed systems list (or setting `ErrNr=195`).
   5. Define `konvertiereSDName`: Normalizes master data categories/systems (or setting `ErrNr=195`).
   6. Define `konvertiereAufbStufeXtra`: Normalizes stage codes for Xtra process runs.
   7. Define `pruefeSystemKennzahl`: Validates permitted combinations of source system and key figure (e.g. DPPS cannot be run with `twe` / `pln` / `loe`, etc.). If invalid, sets `ErrNr=195`.
   8. Define `gibBereich`: Groups key figures into functional business domain areas (`tn`, `us`, `gd`, `sd`, `md`).
   9. Define `gibIntervall`: Determines whether key figures report on a daily (`t`) or monthly (`m`) boundary.
   10. Define `pruefeZeitraum`: Checks date formats and asserts chronological accuracy.
   11. Define `pruefeZahlPositiv`: Validates if a string is numeric and >= 0.
   12. Define `pruefeZeitParameter`: Ensures mutually exclusive parameters (either offset or start/end date pair is supplied, but not both).
   13. Define `konvertiereZeitspanne`: Calculates calendar boundaries using `DWDate_Gib_Zeitraum` based on key-figure unit (Days vs. Months).

7. ERROR HANDLING & EXIT CODES
   - KornShell mechanism: Uses global variables `ErrNr` (integer error number) and `ErrArg` (detailed context) to manage script state.
   - Cascade Prevention: Functions check `if [ $ErrNr -ne 0 ]` immediately on entry and bypass operations if an error condition has already occurred, keeping the initial error state clean.
   - Subshell isolation: `set +e` is used inside subshells during external tool validation calls to prevent instant script termination on minor check failures.
   - Python mapping: State can be managed cleanly inside a state class, or natively utilizing Python exceptions. To perfectly preserve the legacy "bypass if error already set" workflow, a state-propagation class pattern is ideal.

8. OUTPUTS / SIDE EFFECTS
   - State Modification: Modifies caller variables in-place via dynamic environment variable mutations (using `eval`). To mimic this in Python, functions should operate on a mutable dictionary representing the running job context.
   - Temporary Files: Short-lived validation logging files created under `/tmp` are removed via `rm -f`. These are avoided completely in Python by capturing execution checks directly in memory.

9. BUSINESS SUMMARY
   - Reusable Logic Framework: Serves as the central validator library for DWH batch runs, ensuring parameter format sanity.
   - Code Standardisation: Normalizes domain-specific terms (e.g., converting "abgang_zukunft" to "abz") to preserve file name and database convention rules.
   - Integrity Enforcement: Prevents running incompatible pipeline jobs (e.g., verifying that a specific billing movement key figure is not imported from an administrative billing master data source system).
   - Time-Horizon Alignment: Automatically shifts daily and monthly rolling targets based on key-figure tracking boundaries.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import datetime
from typing import Dict, Any, Optional

# Step 1: Initialize Module Constants
MODUL_NAME = "alis_parameter"
MODUL_VERSION = "V3.0.9"

# Class to manage global validation state preserving legacy error propagation mechanics
class ValidationState:
    def __init__(self):
        self.err_nr: int = 0
        self.err_arg: str = ""

def get_env_var(env: Dict[str, Any], var_name: str) -> Optional[Any]:
    return env.get(var_name)

def set_env_var(env: Dict[str, Any], var_name: str, value: Any) -> None:
    env[var_name] = value

# Step 2: Define pruefeParameterGesetzt
def pruefeParameterGesetzt(state: ValidationState, env: Dict[str, Any], param_name: str, param_var: str) -> None:
    if state.err_nr != 0:
        return

    if not param_name or not param_var:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeParameterGesetzt"
        return

    param_wert = get_env_var(env, param_var)
    if param_wert is None or param_wert == "":
        state.err_nr = 194
        state.err_arg = param_name

# Step 3: Define konvertiereKennzahl
def konvertiereKennzahl(state: ValidationState, env: Dict[str, Any], var_name: str) -> None:
    if state.err_nr != 0:
        return

    if not var_name:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereKennzahl"
        return

    kennzahl_val = get_env_var(env, var_name)
    if kennzahl_val is None:
        kennzahl_val = ""

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
        result = mappings[kennzahl]
    else:
        state.err_nr = 198
        state.err_arg = kennzahl_val
        result = "???"

    set_env_var(env, var_name, result)

# Step 4: Define konvertiereSystem
def konvertiereSystem(state: ValidationState, env: Dict[str, Any], var_name: str) -> None:
    if state.err_nr != 0:
        return

    if not var_name:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereSystem"
        return

    system_val = get_env_var(env, var_name)
    if system_val is None:
        system_val = ""

    system = str(system_val).lower()
    allowed_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

    if system in allowed_systems:
        result = system
    else:
        state.err_nr = 195
        state.err_arg = f"Unbekannte Datenherkunft {system_val} !"
        result = "???"

    set_env_var(env, var_name, result)

# Step 5: Define konvertiereSDName
def konvertiereSDName(state: ValidationState, env: Dict[str, Any], var_name: str) -> None:
    if state.err_nr != 0:
        return

    if not var_name:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereSDSystem"
        return

    system_val = get_env_var(env, var_name)
    if system_val is None:
        system_val = ""

    system = str(system_val).lower()

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
        result = mappings[system]
    else:
        state.err_nr = 195
        state.err_arg = f"Unbekannte Stammdaten-Datenherkunft {system_val} !"
        result = "???"

    set_env_var(env, var_name, result)

# Step 6: Define konvertiereAufbStufeXtra
def konvertiereAufbStufeXtra(state: ValidationState, env: Dict[str, Any], var_name: str) -> None:
    if state.err_nr != 0:
        return

    if not var_name:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereAufbStufeXtra"
        return

    stufe_val = get_env_var(env, var_name)
    if stufe_val is None:
        stufe_val = ""

    stufe = str(stufe_val).lower()

    if stufe == "zusammenfuehrung":
        result = "mrg"
    elif stufe == "befuellung":
        result = "fill"
    else:
        state.err_nr = 195
        state.err_arg = f"Unbekannte Stufenangabe {stufe_val} !"
        result = "???"

    set_env_var(env, var_name, result)

# Step 7: Define pruefeSystemKennzahl
def pruefeSystemKennzahl(state: ValidationState, system: str, kennzahl: str) -> None:
    if state.err_nr != 0:
        return

    if not system or not kennzahl:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeSystemKennzahl"
        return

    err_arg_temp = ""

    if system != "nnv" and (kennzahl == "tvd" or kennzahl == "lkl"):
        err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "carmen":
        if kennzahl in {"twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sap":
        if kennzahl in {"zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dpps":
        if kennzahl in {"twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "ctel":
        if kennzahl not in {"abg", "bst", "zug", "twe"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "xtra":
        if kennzahl != "rst":
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "d1":
        if kennzahl in {"gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "nnv":
        if kennzahl not in {"tvd", "lkl"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dwh":
        if kennzahl != "mds":
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "brunet":
        if kennzahl not in {"d1n", "rub", "lmo"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sigma":
        allowed_sigma = {"nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
        if kennzahl not in allowed_sigma:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    if err_arg_temp:
        state.err_nr = 195
        state.err_arg = err_arg_temp

# Step 8: Define gibBereich
def gibBereich(state: ValidationState, env: Dict[str, Any], kennzahl: str, var_bereich: str) -> None:
    if state.err_nr != 0:
        return

    if not kennzahl or not var_bereich:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibBereich"
        return

    list_tn = {"abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"}
    list_us = {"gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}
    list_gd = {"tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
    list_sd = {"ksd", "bwa"}
    list_md = {"mds"}

    my_bereich = None
    if kennzahl in list_tn:
        my_bereich = "tn"
    elif kennzahl in list_us:
        my_bereich = "us"
    elif kennzahl in list_gd:
        my_bereich = "gd"
    elif kennzahl in list_sd:
        my_bereich = "sd"
    elif kennzahl in list_md:
        my_bereich = "md"

    if my_bereich is None:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibBereich - Kuerzel '{kennzahl}' unbekannt"
        return

    set_env_var(env, var_bereich, my_bereich)

# Step 9: Define gibIntervall
def gibIntervall(state: ValidationState, env: Dict[str, Any], kennzahl: str, var_intervall: str) -> None:
    if state.err_nr != 0:
        return

    if not kennzahl or not var_intervall:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibIntervall"
        return

    list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
    list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

    my_intervall = None
    if kennzahl in list_t:
        my_intervall = "t"
    elif kennzahl in list_m:
        my_intervall = "m"

    if my_intervall is None:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
        return

    set_env_var(env, var_intervall, my_intervall)

# Step 10: Define pruefeZeitraum
def pruefeZeitraum(state: ValidationState, anfang: str, ende: str) -> None:
    if state.err_nr != 0:
        return

    if not anfang or not ende:
        state.err_nr = 196
        state.err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeZeitraum"
        return

    err_arg_temp = ""

    # # REVIEW-STRUCT: command [DWDate_Datum_Check] logic not supplied — mapped to native datetime verification
    try:
        dt_anfang = datetime.datetime.strptime(anfang, "%Y%m%d")
    except ValueError:
        err_arg_temp = "Anfangsdatum entspricht nicht dem Format YYYYMMDD"

    try:
        dt_ende = datetime.datetime.strptime(ende, "%Y%m%d")
    except ValueError:
        if not err_arg_temp:
            err_arg_temp = "Endedatum entspricht nicht dem Format YYYYMMDD"

    if not err_arg_temp:
        # # REVIEW-STRUCT: command [DWDate_Datum_LE] logic not supplied — mapped to native date verification
        if dt_anfang > dt_ende:
            err_arg_temp = "Anfangsdatum ist nicht kleiner gleich Endedatum"

    if err_arg_temp:
        state.err_nr = 195
        state.err_arg = err_arg_temp

# Step 11: Define pruefeZahlPositiv
def pruefeZahlPositiv(state: ValidationState, p_zahl: Any, p_parameter_name: str) -> None:
    try:
        val = int(p_zahl)
        is_numeric = True
    except ValueError:
        try:
            val = float(p_zahl)
            is_numeric = True
        except ValueError:
            is_numeric = False

    if is_numeric:
        if val < 0:
            state.err_nr = 195
            state.err_arg = f"Parameter {p_parameter_name} muss groesser gleich 0 sein"
    else:
        state.err_nr = 195
        state.err_arg = f"Parameter {p_parameter_name} ist kein numerischer Wert"

# Step 12: Define pruefeZeitParameter
def pruefeZeitParameter(state: ValidationState, p_anfangsdatum: str, p_endedatum: str, p_zeit_offset: str) -> None:
    if state.err_nr != 0:
        return

    if p_zeit_offset and p_zeit_offset != "":
        if (not p_anfangsdatum or p_anfangsdatum == "") and (not p_endedatum or p_endedatum == ""):
            pruefeZahlPositiv(state, p_zeit_offset, "Zeitspanne")
            return
        else:
            state.err_nr = 195
            state.err_arg = "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden"
            return
    else:
        if p_anfangsdatum and p_anfangsdatum != "" and p_endedatum and p_endedatum != "":
            pruefeZeitraum(state, p_anfangsdatum, p_endedatum)
        else:
            state.err_nr = 195
            if (not p_anfangsdatum or p_anfangsdatum == "") and (not p_endedatum or p_endedatum == ""):
                state.err_arg = "Datumswerte oder Zeitspanne fehlen"
            else:
                state.err_arg = "Sowohl Anfang- als auch Endedatum muessen angegeben werden"
            return

# Step 13: Define konvertiereZeitspanne
def konvertiereZeitspanne(state: ValidationState, env: Dict[str, Any], p_var_anfang: str, p_var_ende: str, p_spanne: Any, p_kennzahl: str) -> None:
    if state.err_nr != 0:
        return

    offset_unit = "D"
    if p_kennzahl == "bst":
        offset_unit = "M"

    # # REVIEW-STRUCT: command [DWDate_Gib_Zeitraum] logic not supplied — mapped to relative datetime offsets
    # Base running date defaults to today unless specified in context dictionary.
    run_date_str = env.get("RUN_DATE", datetime.date.today().strftime("%Y%m%d"))
    try:
        run_date = datetime.datetime.strptime(run_date_str, "%Y%m%d").date()
    except ValueError:
        state.err_nr = 85
        state.err_arg = "DWDate_Gib_Zeitraum"
        return

    try:
        span_int = int(p_spanne)
    except ValueError:
        state.err_nr = 85
        state.err_arg = "DWDate_Gib_Zeitraum"
        return

    if offset_unit == "D":
        ende_date = run_date
        anfang_date = run_date - datetime.timedelta(days=span_int)
    else:
        # Perform month delta manipulation logic natively
        ende_date = run_date
        year_shift = span_int // 12
        month_shift = span_int % 12
        new_month = run_date.month - month_shift
        new_year = run_date.year - year_shift
        if new_month <= 0:
            new_month += 12
            new_year -= 1
        try:
            anfang_date = datetime.date(new_year, new_month, run_date.day)
        except ValueError:
            # Realign for end-of-month boundaries (e.g. Feb 30th -> Feb 28th)
            if new_month == 12:
                next_month_date = datetime.date(new_year + 1, 1, 1)
            else:
                next_month_date = datetime.date(new_year, new_month + 1, 1)
            anfang_date = next_month_date - datetime.timedelta(days=1)

    set_env_var(env, p_var_anfang, anfang_date.strftime("%Y%m%d"))
    set_env_var(env, p_var_ende, ende_date.strftime("%Y%m%d"))
```

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` | KornShell parameters utility library converted to a native Python module containing reusable helper functions for mapping, validation, and date calculations. |

---

### Job dependencies
The script `h_alis_parameter.ksh` serves as a shared library module that is sourced by downstream tasks to set execution states, map key variables, and validate date arguments. The following downstream jobs consume its outputs:
* **DW.BERT_ABLAUFSTEUERUNG** *(not yet migrated)* — Will import `h_alis_parameter.py` to validate step execution inputs.
* **DW.BERT_AUSD_BP_TA_MSISDN** *(not yet migrated)* — Will import `h_alis_parameter.py` to resolve and parse input parameters.
* **DW.BERT_AUSD_BP_TA_P_BASISPROD** *(not yet migrated)* — Will import `h_alis_parameter.py` to process temporal arguments.
* **DW.BERT_AUSD_V_TA_PERIOD** *(not yet migrated)* — Will import `h_alis_parameter.py` to handle date ranges.
* **DW.BERT_AUSD_V_TA_P_VERTRAG** *(not yet migrated)* — Will import `h_alis_parameter.py` to validate source parameters.
* **DW.BERT_AUSD_V_TA_VERTRAG_TMP** *(not yet migrated)* — Will import `h_alis_parameter.py` to evaluate run-time context configurations.
* **DW.BERT_DROP_TEMP_TABLE** *(not yet migrated)* — Will import `h_alis_parameter.py` to determine schema contexts.
* **DW.BERT_P_ADRESSEN** *(not yet migrated)* — Will import `h_alis_parameter.py` to check standard arguments.
* **DW.BERT_P_AUSTAUSCH** *(not yet migrated)* — Will import `h_alis_parameter.py` to validate system codes.
* **DW.BERT_P_GESCHAEFTSP** *(not yet migrated)* — Will import `h_alis_parameter.py` to align run boundaries.
* **DW.BERT_P_RECH_EMPF** *(not yet migrated)* — Will import `h_alis_parameter.py` to check financial key-figure arguments.
* **DW.BERT_RECHNUNGSDATEN** *(not yet migrated)* — Will import `h_alis_parameter.py` to perform date sequence checks.

---

### Scheduling
* **Standalone Schedule:** This job is NOT directly triggered by any scheduler. It executes as an internal library loaded dynamically by other scheduled jobs. On Cloud Composer / BigQuery, it must remain an importable, non-executable Python helper module packaged or placed in the shared Python path (e.g. within the `/dags` directory or Python environment of Composer).

---

### Schedule & variables
* **Schedule Linkage:** Inherits the schedules and environment states of importing jobs.
* **Inherited/Run-time Variables:**
  * `RUN_DATE`: Used as the base execution date for relative rolling calculation logic (replacing legacy system datetime utilities). Sourced globally from the calling Airflow DAG's context using `{{ ds }}`.

---

### External system replacements
* **Legacy Shell Date Helpers:** The external validation commands `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` must be replaced with native Python standard libraries (`datetime` and `dateutil.relativedelta`) inside the migrated Python utility module to calculate days and months relative offsets.

---

### Cross-file dependencies
* **Translation Mappings:** Serves as the authoritative validator module mapping legacy German key-figure names (e.g., `"restguthaben"`, `"abgang_zukunft"`) to standardized database abbreviations (`"rst"`, `"abz"`).
* **System Constraints:** Implements the matrix logic (`pruefeSystemKennzahl`) which ensures invalid configurations are detected and blocked before triggering transformation workloads.

---

### Target file plan
* **`vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py`**
  * **Language:** Python 3.x
  * **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
  * **Purpose:** Implements equivalent modular helper functions (`pruefeParameterGesetzt`, `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra`, `pruefeSystemKennzahl`, `gibBereich`, `gibIntervall`, `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, `konvertiereZeitspanne`) utilizing Python types, exceptions, and dictionaries.

---

### Environment-specific values
* **`ModulName`**: JOB-SPECIFIC — `"alis_parameter"` (constant inside Python library module).
* **`ModulVersion`**: JOB-SPECIFIC — `"V3.0.9"` (constant inside Python library module).
* **`RUN_DATE`**: GLOBAL — Airflow DAG runtime context parameters (`{{ ds }}`) passed dynamically from the importing operator.

---

### Risks and manual steps
* **WIRING: NOT FINALIZED — DW.BERT_ABLAUFSTEUERUNG — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_AUSD_BP_TA_MSISDN — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_AUSD_BP_TA_P_BASISPROD — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_AUSD_V_TA_PERIOD — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_AUSD_V_TA_P_VERTRAG — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_AUSD_V_TA_VERTRAG_TMP — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_DROP_TEMP_TABLE — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_P_ADRESSEN — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_P_AUSTAUSCH — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_P_GESCHAEFTSP — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_P_RECH_EMPF — not yet migrated**
* **WIRING: NOT FINALIZED — DW.BERT_RECHNUNGSDATEN — not yet migrated**
* **DEPENDENCY: UTILITIES NOT FOUND — DWDate_Datum_Check, DWDate_Datum_LE, DWDate_Gib_Zeitraum — no source files available**
  * *Manual Risk:* The behavior of these date routines is modeled using native Python datetime libraries based on standard validation semantics. A developer must verify that the underlying calculations match the legacy binaries exactly.

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
REASON: The script defines a reusable KornShell utility function with parameter validation, file-readability checks, and error logging to wrap Oracle SQL*Plus subprocess invocations.

EVIDENCE
- Business logic found: KSH custom logic. The script defines a helper function `starteSQLSkript` to validate parameters, check file readability, log execution details, and launch `sqlplus`.
- AWK: none
- SQL-expressible: no (the script is a shell utility function designed to invoke external command-line programs, which cannot be expressed inside a SQL script).
- Non-SQL side effects: Verifies filesystem file readability, invokes the external `sqlplus` executable, and calls a custom external error-messaging utility `DWMSG_MeldeFehler`.
- Against this verdict: none (it is a pure shell utility wrapper library).

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `h_alis_sqlplus.ksh` script is a utility library providing helper functions for executing Oracle SQL*Plus scripts within a Data Warehouse environment. Specifically, it defines the function `starteSQLSkript` which ensures robust parameters are provided and the target SQL file exists and is readable. It then safely executes SQL*Plus with the appropriate database credentials and passes through any extra script parameters, tracking and propagating execution status.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other Data Warehouse orchestration scripts (`. h_alis_sqlplus.ksh`) when they need to run SQL files via SQL*Plus.
   - UC4 Native Includes: None.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   - `p_Eintragsnr` ($1 inside `starteSQLSkript`): Error entry number used when reporting issues via the custom `DWMSG_MeldeFehler` script. Surfaces in Python as the first parameter of the function.
   - `p_Skript` ($2 inside `starteSQLSkript`): Path to the SQL file to be executed. Surfaces in Python as the second parameter of the function.
   - SQL arguments (`$*` after `shift 2`): Positional arguments forwarded directly to the SQL*Plus script. Surfaces in Python as variable positional arguments (`*args`).
   - `DW_ORAUSER` (environment variable): Contains the database connection string and credentials used by SQL*Plus. Surfaced in Python via `os.environ.get("DW_ORAUSER")`.
   - Cross-referenced environment parameters from companion metadata (if applicable): None.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Invokes Oracle SQL*Plus to execute the specified SQL script using `/dev/null` to prevent SQL*Plus from hanging on interactive user input.
     - Execution model: Must remain an external process invocation via `subprocess.run` as it relies on a specific local installation of SQL*Plus and its CLI formatting behavior.
     - Resolvable Launcher check: Not a resolvable launcher here because the utility's purpose is to launch arbitrary file paths passed in dynamically, rather than wrapping a single static SQL file.
   - `DWMSG_MeldeFehler`
     - Purpose: External custom logging/monitoring executable called to log errors in the event of missing parameters or unreadable files.
     - Execution model: External process invocation via `subprocess.run` or Python wrapper.
     - # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

5. EMBEDDED SQL
   - No embedded SQL is defined directly in this helper script. The SQL text resides in external files passed via the `p_Skript` argument.

6. CONTROL FLOW
   1. Set global shell variables `ModulName="alis_sqlplus"` and `ModulVersion="V1.1.3"`.
   2. Define the function `starteSQLSkript()`.
   3. Check if required parameters (`p_Eintragsnr` and `p_Skript`) are provided. If not, log a code `196` error using `DWMSG_MeldeFehler` and return `196`.
   4. Check if the file `$p_Skript` is readable on the filesystem. If not, log a code `201` error using `DWMSG_MeldeFehler` and return `201`.
   5. Output informative execution messages showing the script path and parameters.
   6. Disable shell exit-on-error (`set +e`) to handle SQL*Plus failure manually.
   7. Execute `sqlplus` feeding `/dev/null` to standard input.
   8. Capture the execution exit code (`$?`) into the `errcode` variable.
   9. Restore shell exit-on-error behavior (`set -e`).
   10. Return the captured `errcode`.

7. ERROR HANDLING & EXIT CODES
   - Missing required inputs: Logs error code `196` via `DWMSG_MeldeFehler` and returns `196`.
   - Unreadable SQL script file: Logs error code `201` via `DWMSG_MeldeFehler` and returns `201`.
   - SQL*Plus execution error: Standard error trapping is disabled (`set +e`) around `sqlplus` to prevent immediate script termination. The exit code of `sqlplus` is captured and returned to the caller.
   - Python mapping: The Python function will return the integer error/exit codes to match the signature of the legacy shell function. Standard system/validation exceptions can alternatively be handled.

8. OUTPUTS / SIDE EFFECTS
   - Writes informational logs to standard output.
   - DB alterations: Side-effects in the target database will depend entirely on the external SQL script executed.

9. BUSINESS SUMMARY
   - Standardizes the launch process of SQL scripts in the Data Warehouse.
   - Performs proactive file system validations to identify missing or unreadable script assets prior to database connection attempts, minimizing log noise.
   - Ensures any failures inside SQL*Plus are safely captured and returned as exit codes to the orchestrator rather than causing silent script aborts or hangs.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
from pathlib import Path

# Step 1: Initialize global module variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# REVIEW-STRUCT: environment file / function DWMSG_MeldeFehler is not supplied. 
# Implemented as a placeholder that executes the external CLI program.
def dwmsg_melde_fehler(eintragsnr: str, msg_type: str, code: int, details: str) -> None:
    try:
        subprocess.run(
            ["DWMSG_MeldeFehler", str(eintragsnr), msg_type, str(code), details],
            check=True
        )
    except FileNotFoundError:
        print(
            f"[WARNING] DWMSG_MeldeFehler missing. Logged: {eintragsnr} {msg_type} {code} {details}", 
            file=sys.stderr
        )

# Step 2: Define helper function to start SQL*Plus scripts safely
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
    """
    Validates parameter inputs and file availability, then executes a SQL script via SQL*Plus.
    Returns the integer exit code of the execution.
    """
    
    # Step 3: Validate input arguments
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(
            p_eintragsnr, 
            "E", 
            196, 
            f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        )
        return 196

    # Step 4: Validate script file readability
    skript_path = Path(p_skript)
    if not skript_path.exists() or not os.access(skript_path, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, str(p_skript))
        return 201

    # Step 5: Log execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Identify Oracle Connection String
    # # REVIEW: target database platform is assumed to be Oracle because of sqlplus CLI invocation.
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        print("[WARNING] Environment variable DW_ORAUSER is not set.", file=sys.stderr)
        # We proceed to let sqlplus raise credentials errors if applicable

    # Step 7: Prepare and run SQL*Plus CLI command
    # Equivalent to sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    cmd = ["sqlplus", dw_orauser if dw_orauser else "", f"@{p_skript}"] + list(args)

    try:
        # Step 8: Execute subprocess with set +e equivalent (check=False)
        # Passing subprocess.DEVNULL is equivalent to shell's </dev/null redirection
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Fehler bei der Ausfuehrung von SQL*Plus: {e}", file=sys.stderr)
        errcode = -1

    # Step 9: Return exit status code
    return errcode
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Converted to a Python module containing `starte_sql_skript` to perform validation, file checks, and launch process logic. |

# Job Dependencies
The following downstream jobs depend on this utility job to run SQL*Plus scripts:
* `DW.BERT_ABLAUFSTEUERUNG` — not yet migrated
* `DW.BERT_AUSD_BP_TA_MSISDN` — not yet migrated
* `DW.BERT_AUSD_BP_TA_P_BASISPROD` — not yet migrated
* `DW.BERT_AUSD_V_TA_PERIOD` — not yet migrated
* `DW.BERT_AUSD_V_TA_P_VERTRAG` — not yet migrated
* `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — not yet migrated
* `DW.BERT_DROP_TEMP_TABLE` — not yet migrated
* `DW.BERT_P_ADRESSEN` — not yet migrated
* `DW.BERT_P_AUSTAUSCH` — not yet migrated
* `DW.BERT_P_GESCHAEFTSP` — not yet migrated
* `DW.BERT_P_RECH_EMPF` — not yet migrated
* `DW.BERT_RECHNUNGSDATEN` — not yet migrated

Because these upstream/downstream components are not yet migrated, the orchestration wiring cannot be finalized until they exist. Downstream Python DAGs/operators must eventually import this pythonic utility or directly invoke BigQuery SQL execution.

# Scheduling
This utility job is not directly triggered by any of the environment's schedulers. It is executed dynamically as an included or shared module inside scheduled downstream jobs. The migrated Python module must remain a callable/importable unit without its own standalone schedule.

# External System Replacements
* **Oracle SQL\*Plus:** Relies on the local `sqlplus` CLI. For the target BigQuery platform, execution of actual SQL queries should be replaced using the native BigQuery Python Client (`google.cloud.bigquery`) or Cloud Composer BigQuery operators (e.g., `BigQueryInsertJobOperator`), while retaining validation checks.
* **Oracle Credentials (`DW_ORAUSER`):** Replaced with target GCP IAM permissions and Cloud Composer connection setups, eliminating the need to pass literal credentials via environment variables.

# Cross-File Dependencies
* **Dynamic Sourcing:** Sourced by multiple downstream shell scripts via `. h_alis_sqlplus.ksh`. In Python, this dependency will be handled via standard module import statements (`from vobs.dw_source.isrpt.isbert.SQL.aktuell.allgemein.is.util.bin.h_alis_sqlplus import starte_sql_skript`).
* **SQL Scripts Location:** Downstream calling scripts will pass paths to target SQL files. Ensure folders containing the SQL assets are properly mounted, uploaded, or resolved in the target environment (e.g., inside the Composer DAGs/plugins folder or a GCS bucket).

# Target File Plan
* **Target File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py`
  * **Language:** Python
  * **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
  * **Purpose:** Provides a validated helper function `starte_sql_skript` to mirror the legacy shell function's interface and handle parameter validation, file-readability verification, and process orchestration.

# Environment-Specific Values
* `DW_ORAUSER` (GLOBAL): Legacy database username/credentials. In the BigQuery target, this maps to environment-wide metadata such as `GCP_PROJECT`, `BQ_DATASET`, and `BQ_LOCATION` which are loaded via `os.environ.get("GCP_PROJECT")` or Cloud Composer's configuration store `Variable.get("GCP_PROJECT")`.

# Risks & Manual Steps
* SOURCE: NOT FOUND — `DWMSG_MeldeFehler` — no candidate. The external error logging command `DWMSG_MeldeFehler` is used in the validation steps of `starteSQLSkript` but is not defined in this codebase. A placeholder or a native logging wrapper must be implemented manually by the build agent.
* **SQL Dialect Transition:** While this utility manages script orchestration, the actual SQL scripts passed as parameters contain legacy Oracle dialect syntax and must be separately migrated to BigQuery Standard SQL.
* **Module Import Rewrites:** All downstream KSH files migrated to Python must be refactored to import `h_alis_sqlplus.py` rather than attempting to source the legacy KornShell library.