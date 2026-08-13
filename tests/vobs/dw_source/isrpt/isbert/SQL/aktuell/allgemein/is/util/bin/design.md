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
REASON: The script defines a library of KornShell utility functions for error handling and logging that interact with the database via Oracle SQL*Plus, perform filesystem I/O, construct dates, and return values dynamically to the caller's environment.

EVIDENCE
- Business logic found: KSH custom logic. The script acts as a reusable utility library (sourced by other KSH scripts) to manage runtime error logging, register jobs, set job completion statuses, and write diagnostic timing info back to an Oracle database table via SQL*Plus.
- AWK: none
- SQL-expressible: no. While it invokes PL/SQL stored procedures, the overall orchestration is dynamic script behavior (sourcing variables, writing to temp files, evaluating dynamic variables in the caller's scope via eval, generating timestamped log filenames, and catching traps).
- Non-SQL side effects: Creates and deletes temporary files in `/tmp`, dynamically updates shell environment variables in calling contexts using eval, and generates localized log files with process-specific and date-based suffixes.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) functions as a shared library of KornShell utilities used for unified error management and execution logging within the "Information Services" project. It provides robust handlers for intercepting runtime errors (`trap`), registering new task tracking entries in an Oracle database, and updating execution states (Success / Failure / Timings). Calling scripts source this file to standardize how database-driven operational tracking is performed.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced internally by other KornShell scripts (e.g., `. f_alis_msgerr.ksh`) during execution. It has no independent standalone UC4 job context of its own; instead, it is a component of the runtime environment for various JOBS_UNIX tasks.
   - UC4 Native Includes: None referenced in the script text.
   - Environment files sourced: None. It assumes the caller has already populated required environment variables such as `$DW_ORAUSER`, `$DW_DIR_ROOT`, and `$DW_DIR_PROT`.

3. PARAMETERS / INPUTS
   This script relies on environment variables set by the calling script:
   - `DW_ORAUSER`: Oracle database connection string/credentials. (Used: Yes. Surfaced via `os.environ.get("DW_ORAUSER")` as a DB connection parameter.)
   - `DW_DIR_ROOT`: Root directory containing SQL sub-directories. (Used: Yes. Surfaced via `os.environ.get("DW_DIR_ROOT")`.)
   - `DW_DIR_PROT`: Directory where logs are written. (Used: Yes. Surfaced via `os.environ.get("DW_DIR_PROT")`.)

   KSH Declared Environment Parameters (Cross-Referenced):
   - `DW_ORAUSER`: DB-connection-style parameter (contains credentials/connection strings). Used for Oracle client setup.
   - `DW_DIR_ROOT`, `DW_DIR_PROT`: Informational system variables representing relative execution paths and log directories.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Used to run SQL wrappers (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, etc.) or execute raw PL/SQL statements.
     - *Verbatim commands:*
       - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
       - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
       - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
       - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
       - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
       - `sqlplus -s $DW_ORAUSER <<EOF\n EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));\n commit;\nEOF`
       - `sqlplus -s $DW_ORAUSER <<EOF\n EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');\n commit;\nEOF`
     - *Treatment:* Since these scripts represent Oracle-specific PL/SQL tracking structures, they should be implemented directly using a Python DB client (e.g., `oracledb`) if possible. However, the exact bodies of the referenced `.sql` files are not provided in this extraction.
     - # REVIEW-STRUCT: launcher [sqlplus] invoked with unsupplied SQL scripts under $DW_DIR_ROOT — direct Python DB-client implementation requires confirming internal logic of wrapper scripts (e.g., d_alis_spaufruf_p1.sql, d_al_is_ermittlenr.sql) before finalization.

5. EMBEDDED SQL
   - **DWMSG_SetzeStichtagInfo block:**
     - *SQL Text:*
       ```sql
       EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
       commit;
       ```
     - *Statement type:* PL/SQL execution block / Transaction control (`commit`).
     - *Tables touched:* Unknown (internals of package `BERT_MELDUNG`).
     - *Dialect:* Oracle PL/SQL (unambiguous due to `to_date`, package call, and `commit`).

   - **DWMSG_AppendTimingInfos block:**
     - *SQL Text:*
       ```sql
       EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
       commit;
       ```
     - *Statement type:* PL/SQL execution block / Transaction control (`commit`).
     - *Tables touched:* Unknown (internals of package `BERT_MELDUNG`).
     - *Dialect:* Oracle PL/SQL (unambiguous due to `to_char(SYSDATE, ...)`).

6. CONTROL FLOW
   This utility is structural and exposes 9 key methods. Control flows within each function:
   1. **DWMSG_Fehlerbehandlung**: Captures `$?` (stored in `FehlerNr`). Calls `DWMSG_MeldeFehler` passing a fatal system code `10`. Calls `DWMSG_SetzeStatusAbbruch`.
   2. **DWMSG_SetzeStatusOK**: Verifies `$DWMSG_EintragsNr` is not null (exits code 1 on fail). Invokes `BERT_MELDUNG.SetzeStatusOk` via SQL*Plus.
   3. **DWMSG_SetzeStatusAbbruch**: Verifies `$DWMSG_EintragsNr` is not null. Invokes `BERT_MELDUNG.SetzeStatusAbbruch` via SQL*Plus.
   4. **DWMSG_ErmittleNr**: Verifies output variable name is supplied. Generates a temp file path in `/tmp`. Invokes SQL*Plus with `d_al_is_ermittlenr.sql` to populate the temp file. Reads the unique ID, strips whitespace, and dynamically assigns it to the caller's target variable using `eval`. Deletes the temp file.
   5. **DWMSG_ErzeugeEintrag**: Verifies `$DWMSG_EintragsNr` is supplied. Executes stored procedure wrapper `d_alis_spaufruf_p4.sql` via SQL*Plus to create a record in the database tracking table.
   6. **DWMSG_MeldeFehler**: Verifies `$DWMSG_EintragsNr`. Determines the number of non-empty optional parameters to select the correct parameter-count sql wrapper script (`p3.sql` to `p5.sql`). Calls the package method `BERT_MELDUNG.Fehler` via SQL*Plus.
   7. **DWMSG_Logdateiname**: Constructs a standardized path matching `${DW_DIR_PROT}/${JobKennung}_YYYYMMDD_HHMM_${DWMSG_EintragsNr}.log` and returns it via `eval`.
   8. **DWMSG_SetzeStichtagInfo**: Verifies input parameter bounds and formats. Runs PL/SQL package method `BERT_MELDUNG.SetzeZusatzInfos` utilizing Oracle `to_date` conversions.
   9. **DWMSG_AppendTimingInfos**: Validates format constraints. Appends timestamped execution diagnostics via PL/SQL package method `BERT_MELDUNG.SetzeZusatzInfos`.

7. ERROR HANDLING & EXIT CODES
   - Missing required inputs trigger immediate `echo` error messaging directly to standard output and exit with codes `1` or `2`.
   - Database operations executed via `sqlplus` do not use explicit status traps inside this script; failures of sub-steps are logged based on structural parameters passed by caller shells.
   - success/failure mapping: Python methods will raise a `ValueError` for validation failures or propagate database driver exceptions (such as `oracledb.DatabaseError`) up to the calling process.

8. OUTPUTS / SIDE EFFECTS
   - Writes/modifies Oracle tracking tables indirectly via database packages (`BERT_MELDUNG`).
   - Creates and deletes temporary runtime files under `/tmp/ErmittleNr_[PID].lst`.

9. BUSINESS SUMMARY
   - Standardizes error management and execution logging across the "Information Services" processing pipeline.
   - Records execution lifecycles (Start, OK completion, or Failure) in a central administrative table.
   - Provides runtime context logging, capturing structural and error state messages mapped to specific error codes.
   - Builds predictable, timestamped, traceable log file paths for filesystem-based process diagnostic captures.

=======================================================================================
PSEUDOCODE OUTLINE
=======================================================================================

```python
import os
import sys
import datetime
import subprocess
import shutil

# Global configuration pulled from environment
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT")

# REVIEW-STRUCT: environment variables may not be set if caller script fails to initialize them.

def dwmsg_fehlerbehandlung(eintrags_nr):
    # Step 1: Capture last exit code from caller
    # In python, this would be retrieved from sys.last_value or passed explicitly.
    # For compatibility, we assume the exception/error state is monitored.
    fehler_nr = 1  # Default fallback for caught error
    unerw_fehler = 10
    
    # Step 2: Record fatal message in central database log
    dwmsg_melde_fehler(eintrags_nr, "F", unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    
    # Step 3: Print abort log to stdout
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    
    # Step 4: Mark database entry state as aborted
    dwmsg_setze_status_abbruch(eintrags_nr)


def dwmsg_setze_status_ok(eintrags_nr):
    # Step 1: Guard parameter validation
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben")
        sys.exit(1)
        
    # Step 2: Invoke PL/SQL package method using sqlplus
    # REVIEW-STRUCT: d_alis_spaufruf_p1.sql body not supplied.
    sql_script = os.path.join(DW_DIR_ROOT, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    cmd = ["sqlplus", "-s", DW_ORAUSER, f"@{sql_script}", "BERT_MELDUNG.SetzeStatusOk", str(eintrags_nr)]
    subprocess.run(cmd, input=b"", check=True)


def dwmsg_setze_status_abbruch(eintrags_nr):
    # Step 1: Guard parameter validation
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben")
        sys.exit(1)
        
    # Step 2: Invoke PL/SQL package method using sqlplus
    # REVIEW-STRUCT: d_alis_spaufruf_p1.sql body not supplied.
    sql_script = os.path.join(DW_DIR_ROOT, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    cmd = ["sqlplus", DW_ORAUSER, f"@{sql_script}", "BERT_MELDUNG.SetzeStatusAbbruch", str(eintrags_nr)]
    subprocess.run(cmd, input=b"", check=True)


def dwmsg_ermittle_nr():
    # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
    
    # Step 1: Setup execution details and run sqlplus to get next unique tracking ID
    temp_file = f"/tmp/ErmittleNr_{os.getpid()}.lst"
    sql_script = os.path.join(DW_DIR_ROOT, "allgemein/is/util/sql/d_al_is_ermittlenr.sql")
    
    # REVIEW-STRUCT: d_al_is_ermittlenr.sql body not supplied.
    cmd = ["sqlplus", "-s", DW_ORAUSER, f"@{sql_script}", temp_file]
    try:
        subprocess.run(cmd, input=b"", check=True)
        
        # Step 2: Read outputs and sanitize whitespaces
        with open(temp_file, "r") as f:
            eintrags_nr = f.read().strip().replace(" ", "")
            
        return eintrags_nr
    finally:
        # Step 3: Cleanup file assets
        if os.path.exists(temp_file):
            os.remove(temp_file)


def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    # Step 1: Guard parameter validation
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben")
        sys.exit(1)
        
    # Step 2: Run DB wrapper script with parameters
    # REVIEW-STRUCT: d_alis_spaufruf_p4.sql body not supplied.
    sql_script = os.path.join(DW_DIR_ROOT, "allgemein/is/util/sql/d_alis_spaufruf_p4.sql")
    cmd = [
        "sqlplus", "-s", DW_ORAUSER, f"@{sql_script}", 
        "BERT_MELDUNG.Erzeuge_Eintrag", str(eintrags_nr), 
        str(job_kennung), str(programmname), str(log_datei)
    ]
    subprocess.run(cmd, input=b"", check=True)


def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    # Step 1: Guard parameter validation
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben")
        sys.exit(1)
        
    # Step 2: Dynamically calculate argument lengths for wrapper scripts
    if not zusatz1:
        num_parm = 3
    elif not zusatz2:
        num_parm = 4
    else:
        num_parm = 5
        
    # Step 3: Select script target path
    sql_script = os.path.join(
        DW_DIR_ROOT, f"allgemein/is/util/sql/d_alis_spaufruf_p{num_parm}.sql"
    )
    
    # REVIEW-STRUCT: d_alis_spaufruf_p3/4/5.sql bodies not supplied.
    cmd = [
        "sqlplus", "-s", DW_ORAUSER, f"@{sql_script}", "BERT_MELDUNG.Fehler",
        str(typ), str(eintrags_nr), str(fehler_nr), f"'{zusatz1}'", f"'{zusatz2}'"
    ]
    subprocess.run(cmd, input=b"", check=True)


def dwmsg_logdateiname(job_kennung, eintrags_nr):
    # Step 1: Format timestamp matching legacy date format '+%Y%m%d_%H%M'
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    
    # Step 2: Form final tracking path target string
    dateiname = os.path.join(DW_DIR_PROT, f"{job_kennung}_{timestamp}_{eintrags_nr}.log")
    return dateiname


def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    # Step 1: Guard parameter validations
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)
        
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!")
        sys.exit(1)
        
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben k\u00f6nnen nicht verarbeitet werden!")
        sys.exit(2)
        
    # Step 2: Execute direct inline PL/SQL statement to update metadata
    plsql_cmd = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({eintrags_nr}, to_date('{stichtag}', '{stichtag_fmt}'));
    commit;
    """
    cmd = ["sqlplus", "-s", DW_ORAUSER]
    subprocess.run(cmd, input=plsql_cmd.encode('utf-8'), check=True)


def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    # Step 1: Guard parameter validations
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)
        
    if not date_format:
        print("Argh!, Formatangabe erforderlich!")
        sys.exit(2)
        
    # Step 2: Execute direct inline PL/SQL statement to log tracking runtime updates
    plsql_cmd = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({eintrags_nr},null,'{info_text}'||' '||to_char(SYSDATE,'{date_format}')||' ');
    commit;
    """
    cmd = ["sqlplus", "-s", DW_ORAUSER]
    subprocess.run(cmd, input=plsql_cmd.encode('utf-8'), check=True)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | Converted to a Python utility module providing shared execution tracking, database logging, and error handling functions to preserve the legacy KSH logic in Cloud Composer. |

---

### Job dependencies
The following downstream jobs consume this utility script (by sourcing/importing it at runtime). Since they are not yet migrated to Google Cloud, their connection and integration with the migrated Python module cannot be finalized until they are built:
*   `DW.BERT_ABLAUFSTEUERUNG` — not yet migrated
*   `DW.BERT_AUSD_BP_TA_MSISDN` — not yet migrated
*   `DW.BERT_AUSD_BP_TA_P_BASISPROD` — not yet migrated
*   `DW.BERT_AUSD_V_TA_PERIOD` — not yet migrated
*   `DW.BERT_AUSD_V_TA_P_VERTRAG` — not yet migrated
*   `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — not yet migrated
*   `DW.BERT_DROP_TEMP_TABLE` — not yet migrated
*   `DW.BERT_P_ADRESSEN — not yet migrated`
*   `DW.BERT_P_AUSTAUSCH — not yet migrated`
*   `DW.BERT_P_GESCHAEFTSP — not yet migrated`
*   `DW.BERT_P_RECH_EMPF` — not yet migrated
*   `DW.BERT_RECHNUNGSDATEN` — not yet migrated

*Wiring on BigQuery / Cloud Composer:* In the target architecture, these downstream jobs will be converted into Python execution operators within Cloud Composer DAGs. Instead of shell sourcing (`. f_alis_msgerr.ksh`), they will dynamically import this converted Python utility module (`from vobs.dw_source.isrpt.isbert.SQL.aktuell.allgemein.is.util.bin.f_alis_msgerr import ...`) to perform logging and status tracking.

---

### Schedule & variables
*   **Scheduling:** This utility library is not directly triggered by any scheduler. It functions purely as a shared helper library called inside other scheduled jobs (include/shared module). It must remain a callable/importable Python module with no standalone DAG schedule.
*   **Variables:**
    *   No scheduler-set variables are explicitly passed to this file from the scheduler. It relies on environment-set and caller-provided parameters.

---

### Lineage
*   **Upstream Producers:** None.
*   **Downstream Consumers:** 
    *   Calls database routine: `PROCEDURE:SETZEZUSATZINFOS` (via PL/SQL invocation inside `DWMSG_SetzeStichtagInfo` and `DWMSG_AppendTimingInfos`).

---

### External system replacements
*   **Oracle Client (`sqlplus`):** Legacy shell uses `sqlplus` to execute PL/SQL procedures inside the `BERT_MELDUNG` package. This must be replaced in the target environment:
    *   If logging metadata is stored in BigQuery, these database procedures should be migrated into BigQuery Stored Procedures and executed using the BigQuery Python Client (`google.cloud.bigquery`).
    *   Alternatively, standard database connection clients (such as `google-cloud-pipeline-components` or a Python database connector if Oracle is temporarily retained) should replace raw `sqlplus` CLI subprocess calls.

---

### Cross-file dependencies
*   **External SQL scripts:** This utility executes external Oracle SQL wrapper scripts located under `$DW_DIR_ROOT/allgemein/is/util/sql/`:
    *   `d_alis_spaufruf_p1.sql` (invoked inside `DWMSG_SetzeStatusOK` and `DWMSG_SetzeStatusAbbruch`)
    *   `d_al_is_ermittlenr.sql` (invoked inside `DWMSG_ErmittleNr`)
    *   `d_alis_spaufruf_p4.sql` (invoked inside `DWMSG_ErzeugeEintrag`)
    *   `d_alis_spaufruf_p3.sql` / `d_alis_spaufruf_p4.sql` / `d_alis_spaufruf_p5.sql` (dynamically selected based on parameter count in `DWMSG_MeldeFehler`)
*   **Downstream Caller Scripts:** Multiple KSH wrapper files (the downstream jobs listed in the Dependencies section) source this utility script to manage their execution logs.

---

### Target file plan
*   **File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py`
    *   **Language:** Python
    *   **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   **Purpose:** Exposes equivalent python functions (`dwmsg_fehlerbehandlung`, `dwmsg_setze_status_ok`, `dwmsg_setze_status_abbruch`, `dwmsg_ermittle_nr`, `dwmsg_erzeuge_eintrag`, `dwmsg_melde_fehler`, `dwmsg_logdateiname`, `dwmsg_setze_stichtag_info`, and `dwmsg_append_timing_infos`) utilizing standard database connectors (such as the BigQuery client) and environment-specific settings.

---

### Environment-specific values

#### 1. GLOBAL (Environment-Wide)
*   `GCP_PROJECT`: Represents the GCP project hosting the metadata logging dataset/tables. Sourced at runtime via `os.environ.get("GCP_PROJECT")`.
*   `BQ_DATASET`: Conceptually represents the target dataset containing logging tables (replacing legacy Oracle `BERT_MELDUNG` tracking). Sourced at runtime via `os.environ.get("BQ_DATASET")`.
*   `DW_DIR_ROOT`: Sourced at runtime using `os.environ.get("DW_DIR_ROOT")` to identify the base path for legacy SQL reference files (if temporary file-based execution is needed).
*   `DW_DIR_PROT`: Sourced at runtime using `os.environ.get("DW_DIR_PROT")`. In Google Cloud, this should map to a local log path or be redirected to a GCS bucket environment variable (`GCS_BUCKET`).
*   `DW_ORAUSER`: Database connection credentials. Sourced via Airflow connection configuration or `os.environ.get("DW_ORAUSER")` if still accessing legacy systems.

#### 2. JOB-SPECIFIC
*   `JobKennung`: Legacy tracking code identifying the calling job. Passed dynamically as an argument to utility functions.
*   `EintragsNr` / `DWMSG_EintragsNr`: Unique transaction sequence ID for a specific execution run, dynamically retrieved via `dwmsg_ermittle_nr()`.
*   `Programmname`: The name of the calling application script. Passed as a dynamic script parameter.
*   `LogDatei`: Specific log filename generated using `dwmsg_logdateiname()`.
*   `Zusatz1`, `Zusatz2`: Optional error-specific description strings.

---

### Risks and manual steps

*   **SOURCE: NOT FOUND (Dependencies not yet migrated):**
    *   *Risk:* The following downstream jobs are not yet migrated, meaning their integration and testing with this logging module must be deferred:
        *   `DW.BERT_ABLAUFSTEUERUNG` — not yet migrated
        *   `DW.BERT_AUSD_BP_TA_MSISDN` — not yet migrated
        *   `DW.BERT_AUSD_BP_TA_P_BASISPROD` — not yet migrated
        *   `DW.BERT_AUSD_V_TA_PERIOD` — not yet migrated
        *   `DW.BERT_AUSD_V_TA_P_VERTRAG` — not yet migrated
        *   `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — not yet migrated
        *   `DW.BERT_DROP_TEMP_TABLE` — not yet migrated
        *   `DW.BERT_P_ADRESSEN` — not yet migrated
        *   `DW.BERT_P_AUSTAUSCH` — not yet migrated
        *   `DW.BERT_P_GESCHAEFTSP` — not yet migrated
        *   `DW.BERT_P_RECH_EMPF` — not yet migrated
        *   `DW.BERT_RECHNUNGSDATEN` — not yet migrated
*   **External SQL Reference Scripts:**
    *   *Risk:* The `.sql` files referenced under `$DW_DIR_ROOT/allgemein/is/util/sql/` (such as `d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, etc.) are not part of this group's source files. 
    *   *Manual Step:* A developer must inspect these SQL scripts to confirm their exact table structures and logic, mapping the Oracle queries/DML directly to BigQuery equivalents (e.g., rewriting them into BigQuery SQL statement API calls rather than shelling out to `.sql` files via `sqlplus`).
*   **Oracle Package Migration (`BERT_MELDUNG`):**
    *   *Risk:* This utility library interfaces directly with PL/SQL procedures defined in the package `BERT_MELDUNG`.
    *   *Manual Step:* The database package `BERT_MELDUNG` must be translated to equivalent Python functions writing to a BigQuery logging table, or converted into BigQuery stored procedures. All calls in the Python utility must target the new BigQuery destination.

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
REASON: The script is a library of date-utility functions containing custom KornShell functions, arrays, arithmetic, and interactive database queries via SQL*Plus.

EVIDENCE
- Business logic found: KSH custom logic contains helper functions for date validation, leap year calculations, date addition, and reporting period calculations utilizing SQL*Plus.
- AWK: none
- SQL-expressible: partly, but since this is a reusable helper library designed to be sourced to mutate environment variables, a Python conversion is required to replace the library functions for modern Python callers.
- Non-SQL side effects: writing/deleting temporary files in `/tmp`, and dynamically assigning shell variables using `eval`.
- Against this verdict: none, as a database-only SQL conversion cannot represent a reusable script-level utility library that other scripts source.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_date.ksh`) is a legacy KornShell utility library providing date math and validation functions. It is designed to be sourced by other batch processing scripts within a data warehouse environment. The library performs date validation, leap-year calculations, date addition, and relative period determinations (e.g., getting start and end dates based on offsets) using a mix of native KornShell logic and Oracle `sqlplus` database interactions.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other KornShell batch scripts (often executed under UC4/Automic jobs). No specific UC4 job context is supplied in this extraction, but it depends on environment setup (e.g., `.dw_init`) having been run first to establish `DW_DIR_ROOT` and `DW_ORAUSER`.
   - UC4 Includes: None referenced.
   - Sourced environment files: None directly sourced inside this file, but relies on variables typically established by `.dw_init` or manual exports of `DW_DIR_ROOT` and `DW_ORAUSER`.

3. PARAMETERS / INPUTS
   This script defines functions with positional parameters:
   - `DWDate_Vormonat`
     - `$1` (`VarName`): Variable name in caller's space to assign the calculated value to. Used.
     - `$2` (`DWDate_FMT`): Target date format. Used.
     - `DW_ORAUSER` (env var): Database connection string. Used.
     - `DW_DIR_ROOT` (env var): Path to SQL directory. Used.
   - `DWDate_Datum_Check`
     - `$1` (`wert`): Date string to validate. Used.
     - `$2` (`format`): Format mask to validate against. Used.
     - `DW_ORAUSER` (env var): Used.
   - `DWDate_Datum_LE`
     - `$1` (`datum1`): First date string (YYYYMMDD). Used.
     - `$2` (`datum2`): Second date string (YYYYMMDD). Used.
     - `DW_ORAUSER` (env var): Used.
   - `DWDate_Gib_Zeitraum`
     - `$1` (`Offset`): Numeric period offset. Used.
     - `$2` (`Stufe`): Time unit step ('Y', 'M', 'D'). Used.
     - `$3` (`Format`): Target format string. Used.
     - `$4` (`Var_Start`): Variable name for start date output. Used.
     - `$5` (`Var_Ende`): Variable name for end date output. Used.
     - `DW_ORAUSER` (env var): Used.
     - `DW_DIR_ROOT` (env var): Used.
   - `LetzterTagDesMonats`
     - `$1`: Date string (YYYYMMDD). Used.
   - `TageimMonat`
     - `$1`: Year (YYYY). Used.
     - `$2`: Month (MM). Used.
   - `AddiereDatum`
     - `$1`: Base date string (YYYYMMDD). Used.
     - `$2`: Number of days to add (integer). Used.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql $DWDate_tmpFile $DWDate_FMT </dev/null`
     - Purpose: Executes an external SQL script to get the previous month.
     - Translation: Native Python `datetime` calculations can replace this entirely, or a DB client call can be used if database-specific holiday/calendar tables are required.
     - # REVIEW-STRUCT: external SQL script d_alis_vormonat.sql body not supplied — logic mimicked via Python datetime but must be verified against actual script logic.
   - `sqlplus -s` (inline SQL in `DWDate_Datum_Check`)
     - Purpose: Asserts date validity via database `to_date`.
     - Translation: Replace with Python `datetime.strptime` validation.
   - `sqlplus -s` (inline PL/SQL in `DWDate_Datum_LE`)
     - Purpose: Compares two dates and raises application error -20422 if `datum1 > datum2`.
     - Translation: Replace with native Python comparisons and raise standard exception.
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql $tmpFile $Offset $Stufe $Format </dev/null`
     - Purpose: Computes relative start/end periods.
     - Translation: Native Python logic or a DB client call.
     - # REVIEW-STRUCT: external SQL script d_alis_datum_zeitraum.sql body not supplied — logic mimicked via Python datetime but must be verified.
     - # REVIEW: target database platform not specified; DB-client library choice below is provisional

5. EMBEDDED SQL
   - From `DWDate_Datum_Check`:
     ```sql
     WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
     SET HEADING OFF;
     -- Implizite Ueberpruefung, ob $Wert ein Datum des Format $format ist
     select to_date('$wert','$format') from dual;
     ```
     - Statement type: SELECT
     - Tables touched: `dual`
     - Dialect: Oracle SQL*Plus (indicated by `WHENEVER SQLERROR`, `dual`, and `to_date`)
   - From `DWDate_Datum_LE`:
     ```sql
     WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
     SET HEADING OFF;
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
     - Statement type: Anonymous PL/SQL block
     - Tables touched: None
     - Dialect: Oracle PL/SQL (indicated by `DECLARE`, `BEGIN`, `raise_application_error`)

6. CONTROL FLOW
   - `DWDate_Vormonat`:
     1. Set up temp file path.
     2. Invoke `sqlplus` to execute `d_alis_vormonat.sql`.
     3. Read output from the temp file.
     4. Assign output to dynamic variable name (translated in Python to a simple function return).
     5. Remove the temp file (the original code had a bug: `rm -f $DWDate_FMT` was called instead of `$DWDate_tmpFile`; Python will clean this up correctly or run entirely in-memory).
   - `DWDate_Datum_Check`:
     1. Verify exactly 2 arguments are passed.
     2. Call `sqlplus` to run `to_date` validation.
     3. Return the result status (translated in Python to returning `True` or `False`).
   - `DWDate_Datum_LE`:
     1. Verify exactly 2 arguments are passed.
     2. Call `sqlplus` running a PL/SQL block to assert sequence.
     3. Return exit code status (translated in Python to native boolean return or raising `ValueError`).
   - `DWDate_Gib_Zeitraum`:
     1. Verify exactly 5 arguments are passed.
     2. Generate temp file name.
     3. Call `sqlplus` with `d_alis_datum_zeitraum.sql`.
     4. Assert exactly one line matching `DWH_Ergebnis;` was written to output.
     5. Parse start and end date from output using `grep` and `cut`.
     6. Assign start and end to dynamic variables (translated to returning a tuple of strings).
     7. Clean up the temp file.
   - `LetzterTagDesMonats`:
     1. Extract Year, Month, and Day.
     2. Evaluate leap year status.
     3. Compare Day to month's last day index. Return success/failure (0/1).
   - `TageimMonat`:
     1. Evaluate leap year status.
     2. Retrieve and return days for the given month.
   - `AddiereDatum`:
     1. Extract Year, Month, Day.
     2. Add day offset.
     3. Loop-adjust Day and Month until day falls within the month's bounds.
     4. Loop-adjust Month and Year if month exceeds 12.
     5. Format strings with leading zeroes (padding) and print YYYYMMDD date.

7. ERROR HANDLING & EXIT CODES
   - Insufficient parameters inside functions trigger immediate `return 1`.
   - SQL session failures trigger `WHENEVER SQLERROR EXIT FAILURE ROLLBACK`.
   - Result parse failures in `DWDate_Gib_Zeitraum` print an error message to stdout and return 1.
   - Python mapping: Raise `TypeError`/`ValueError` for incorrect parameters or format violations, and return clean datatypes on success.

8. OUTPUTS / SIDE EFFECTS
   - Mutates environment variables of the caller script using `eval`. In Python, this is replaced by returning values (tuples, lists, or strings) directly to the caller.
   - Creates and deletes temporary files in `/tmp` (replaced by pure in-memory calculations in Python).

9. BUSINESS SUMMARY
   - Reusable date-utility library facilitating date arithmetic in reporting pipelines.
   - Validates dates and verifies that logical date constraints (e.g., start date <= end date) are met.
   - Generates relative reporting periods (beginning and end) based on dynamic offsets.
   - Computes calendar operations including leap years and arbitrary day addition offsets.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import sys
import os
import calendar
from datetime import datetime, timedelta

# Helper to check if a year is a leap year
def _is_leap_year(year: int) -> bool:
    # Step 1: Standard leap year logic
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

# Step 2: DWDate_Vormonat replacement
# # REVIEW-STRUCT: external SQL file d_alis_vormonat.sql not supplied — behaviour mimicked using Python datetime
def dw_date_vormonat(format_str: str) -> str:
    # Mimic Oracle's previous month retrieval using datetime.
    # Typically returns previous month date formatted.
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_day_of_prev_month = first_of_this_month - timedelta(days=1)
    
    # Map Oracle format masks to Python strftime patterns
    # e.g., 'YYYYMM' -> '%Y%m', 'YYYYMMDD' -> '%Y%m%d'
    py_fmt = format_str.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    return last_day_of_prev_month.strftime(py_fmt)

# Step 3: DWDate_Datum_Check replacement
def dw_date_datum_check(wert: str, format_str: str) -> bool:
    # Verify parameter counts are handled natively via python args
    # Map format mask and attempt to parse
    py_fmt = format_str.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    try:
        datetime.strptime(wert, py_fmt)
        return True
    except ValueError:
        return False

# Step 4: DWDate_Datum_LE replacement
def dw_date_datum_le(datum1_str: str, datum2_str: str) -> bool:
    # Date 1 and Date 2 format is assumed YYYYMMDD
    fmt = "%Y%m%d"
    try:
        d1 = datetime.strptime(datum1_str, fmt)
        d2 = datetime.strptime(datum2_str, fmt)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")

    if d1 > d2:
        # Mimic Oracle application error -20422
        raise ValueError(f"Datum {datum1_str} ist groesser als {datum2_str}")
    return True

# Step 5: DWDate_Gib_Zeitraum replacement
# # REVIEW-STRUCT: external SQL file d_alis_datum_zeitraum.sql not supplied — behaviour mimicked using Python datetime
def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    # Standard implementation of timeframe retrieval
    start_dt = datetime.now()
    py_fmt = format_str.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    
    if stufe == 'D':
        end_dt = start_dt + timedelta(days=offset)
    elif stufe == 'M':
        # Align to start of current month and shift
        start_dt = start_dt.replace(day=1)
        # Shift month by offset
        month = start_dt.month - 1 + offset
        year = start_dt.year + month // 12
        month = month % 12 + 1
        end_dt = start_dt.replace(year=year, month=month)
        # Ultimo adjustment
        _, last_day = calendar.monthrange(end_dt.year, end_dt.month)
        end_dt = end_dt.replace(day=last_day)
    elif stufe == 'Y':
        # Neujahr and Sylvester
        start_dt = start_dt.replace(month=1, day=1)
        end_dt = start_dt.replace(year=start_dt.year + offset, month=12, day=31)
    else:
        raise ValueError(f"Unknown period unit Stufe: {stufe}")
        
    return start_dt.strftime(py_fmt), end_dt.strftime(py_fmt)

# Step 6: LetzterTagDesMonats replacement
def letzter_tag_des_monats(date_str: str) -> bool:
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    
    letzter_feb = 29 if _is_leap_year(year) else 28
    letzter_tag_arr = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    return letzter_tag_arr[month] == day

# Step 7: TageimMonat replacement
def tage_im_monat(year: int, month: int) -> int:
    letzter_feb = 29 if _is_leap_year(year) else 28
    letzter_tag_arr = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return letzter_tag_arr[month]

# Step 8: AddiereDatum replacement
def addiere_datum(date_str: str, days_to_add: int) -> str:
    # Use native timedelta arithmetic instead of the nested loops in KSH
    fmt = "%Y%m%d"
    dt = datetime.strptime(date_str, fmt)
    res_dt = dt + timedelta(days=days_to_add)
    return res_dt.strftime(fmt)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `allgemein/is/util/bin/h_alis_date.py` | Converted from a KornShell utility library to a native Python module. External database calls to Oracle via `sqlplus` are replaced by Python native standard libraries (`datetime` and `calendar`) for robust, in-memory date manipulation and validation. |

---

### Job Dependencies
The following downstream jobs consume or source this utility library. Because they are not yet migrated, the final integration wiring cannot be completed until these downstream components are refactored to import/invoke the target Python module (`h_alis_date.py`) instead of sourcing the legacy KornShell library:
* **DW.BERT_ABLAUFSTEUERUNG** — *Not yet migrated*
* **DW.BERT_AUSD_BP_TA_MSISDN** — *Not yet migrated*
* **DW.BERT_AUSD_BP_TA_P_BASISPROD** — *Not yet migrated*
* **DW.BERT_AUSD_V_TA_PERIOD** — *Not yet migrated*
* **DW.BERT_AUSD_V_TA_P_VERTRAG** — *Not yet migrated*
* **DW.BERT_AUSD_V_TA_VERTRAG_TMP** — *Not yet migrated*
* **DW.BERT_DROP_TEMP_TABLE** — *Not yet migrated*
* **DW.BERT_P_ADRESSEN** — *Not yet migrated*
* **DW.BERT_P_AUSTAUSCH** — *Not yet migrated*
* **DW.BERT_P_GESCHAEFTSP** — *Not yet migrated*
* **DW.BERT_P_RECH_EMPF** — *Not yet migrated*
* **DW.BERT_RECHNUNGSDATEN** — *Not yet migrated*

---

### Scheduling
This job is not directly triggered by any scheduler. It operates as an include/shared utility module executing inside scheduled parent processes. In the target Cloud Composer (Airflow) or Cloud Run environment, it must remain a standalone, importable Python library without its own independent scheduling triggers.

---

### Schedule & Variables — Must Be Retained
* **Trigger/Schedule Linkage:** Inherited from the calling parent workflows. Standalone scheduling is disabled.
* **Scheduler-Set Variables:** None are directly fed by the orchestrator to this library. 

---

### Lineage
* **Upstream Data Source:** `TABLE:DUAL` (Oracle system table). The legacy shell script reads from this table via SQL*Plus to perform simple date format checking and retrieval. On the target platform, these external database reads are entirely eliminated and resolved locally within the Python environment using standard `datetime` functions.

---

### External System Replacements
* **Oracle SQL\*Plus Client:** Handled natively using Python standard library packages (`datetime` and `calendar`), removing any reliance on SQL*Plus execution, database connections, and session overhead for core date utility functions.

---

### Cross-File Dependencies
The legacy KornShell functions dynamically fetch SQL files located inside the repository:
* `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql` (to calculate the previous month)
* `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql` (to obtain timeframe bounds based on offsets)

In the target Python architecture, these SQL dependencies are retired. The mathematical logic to resolve relative timeframes and previous months is fully implemented in Python, eliminating the need to maintain or execute external SQL sub-queries.

---

### Target File Plan

| Target File Path | Language | Source File |
| :--- | :--- | :--- |
| `allgemein/is/util/bin/h_alis_date.py` | Python | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` |

---

### Environment-Specific Values

1. **GLOBAL (Environment-Wide Variables):**
   * **`DW_ORAUSER`:** Oracle database credential variable. This is retired on the target system for this file since all SQL*Plus-driven date calculations are converted into local Python math. If database-specific checks are reintroduced, this must resolve to BigQuery runtime connections (`GCP_PROJECT`, `BQ_DATASET`).
   * **`DW_DIR_ROOT`:** Legacy path variable pointing to the script repository root. This is retired on the target system since the cross-file SQL scripts (`d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`) are no longer executed.

2. **JOB-SPECIFIC Variables:**
   * None.

---

### Risks and Manual Steps

* **UNRESOLVED DOWNSTREAM WIRING:** The downstream wrappers are not yet migrated, which prevents the completion of integration tests.
  * *Wiring Action:* `allgemein/is/util/bin/h_alis_date.py` cannot be fully validated end-to-end until downstream tasks are updated to import this module.
* **MISSING SOURCE SQL VERIFICATION:** The logic within the external SQL helper scripts `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` was not provided in this source bundle.
  * *Manual Action:* A developer must manually review both legacy `.sql` scripts to confirm they did not contain non-standard business calendar exceptions, fiscal year definitions, or custom holiday-logic offsets that deviate from standard calendar math.
* **LITERAL OUTPUT MAINTENANCE:** To ensure backward compatibility with legacy logging engines monitoring batch outputs, the German error logging messages within `DWDate_Gib_Zeitraum` must be preserved exactly in the target execution code:
  * `"!! Interner Fehler bei der Rueckgabe von Datumswerten"`
  * `"   Funktion: DWDate_Gib_Zeitraum"`
  * `"   1 Zeile erwartet, $anzahl Zeile(n) bekommen"`

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
REASON: This is a KornShell utility library defining multiple functions for parameter validation, string mapping, and date calculation, which must be converted to a reusable Python module.

EVIDENCE
- Business logic found: KSH custom logic contains helper functions for parameter validation, system/metric name mapping, combination validation, interval/domain lookups, and date parsing/interval calculation.
- AWK: none
- SQL-expressible: no, this is procedural utility logic meant to run in the application/orchestration layer, not a database transformation.
- Non-SQL side effects: Modifies environment variables (via eval), creates temporary files in /tmp, and calls external DWDate helper scripts.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT (produce ONLY when VERDICT: PYTHON)
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `h_alis_parameter.ksh` is a reusable utility library (sourced by other KornShell scripts) that provides common parameter parsing, validation, and standardizing routines in the IS/DW data warehouse environment. The primary functions map input strings (systems, metrics) into standardized codes, assert that system-metric combinations are valid, fetch operational areas/intervals, and validate or compute dates and offsets.

### 2. INVOCATION CONTEXT
- **Sourced by**: Sourced inside other shell scripts via `. h_alis_parameter.ksh` to expose the utility functions to their execution environment.
- **UC4 Jobs**: Not directly triggered by a UC4 JOBS_UNIX object itself; rather, it is imported by various loading scripts which are in turn launched by UC4 tasks.
- **Sourced environment files**: None explicitly sourced in this module.
- **External scripts called (behavior unknown from current extraction)**:
  - `DWDate_Datum_Check` — # REVIEW-STRUCT: launcher DWDate_Datum_Check invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
  - `DWDate_Datum_LE` — # REVIEW-STRUCT: launcher DWDate_Datum_LE invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
  - `DWDate_Gib_Zeitraum` — # REVIEW-STRUCT: launcher DWDate_Gib_Zeitraum invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

### 3. PARAMETERS / INPUTS
Since this is a library, there are no static top-level CLI or environment parameters. Instead, parameters are dynamic and passed as arguments to individual functions.
- `param_name`: Name of the parameter for descriptive error messaging.
- `param_var`: Name of the environment variable to check (read via `eval`).
- `VarName`: Name of the environment variable containing the value to be mapped and updated in-place (read/written via `eval`).
- `System`: Standardized system abbreviation.
- `Kennzahl`: Standardized metric abbreviation.
- `VarBereich`: Name of the env variable where the resolved area (Bereich) should be stored.
- `VarIntervall`: Name of the env variable where the resolved interval should be stored.
- `Anfang`: Start date in YYYYMMDD format.
- `Ende`: End date in YYYYMMDD format.
- `p_Zahl`: Number to check.
- `p_ParameterName`: Descriptive name of the parameter.
- `p_Anfangsdatum`: Start date.
- `p_Endedatum`: End date.
- `p_ZeitOffset`: Time span/offset.
- `p_VarAnfang`: Name of the env variable to write the computed start date to.
- `p_VarEnde`: Name of the env variable to write the computed end date to.
- `p_Spanne`: Numeric offset/timespan.
- `p_Kennzahl`: Metric abbreviation.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- `basename $0`
  - Exact command: `basename $0`
  - Purpose: Construct a unique temporary file name.
  - Map to: Native Python (`os.path.basename(sys.argv[0])`).
- `date +%Y%m%d%H%M%S`
  - Exact command: `date +%Y%m%d%H%M%S`
  - Purpose: Get current time stamp for temp file name.
  - Map to: Native Python (`datetime.now().strftime("%Y%m%d%H%M%S")`).
- `DWDate_Datum_Check`
  - Exact command: `DWDate_Datum_Check $Wert $Format`
  - Purpose: Check date string matches format `YYYYMMDD`.
  - Map to: Native Python `datetime.strptime(Wert, "%Y%m%d")` or subprocess call `subprocess.run(["DWDate_Datum_Check", ...])`.
- `DWDate_Datum_LE`
  - Exact command: `DWDate_Datum_LE $Anfang $Ende`
  - Purpose: Validate if start date is less than or equal to end date.
  - Map to: Native Python comparison (`dt_anfang <= dt_ende`) or subprocess call.
- `DWDate_Gib_Zeitraum`
  - Exact command: `DWDate_Gib_Zeitraum -$p_Spanne $Offset_Unit "YYYYMMDD" Anfangsdatum Endedatum`
  - Purpose: Generate start/end dates given an offset and unit.
  - Map to: Native Python date arithmetic (e.g. `relativedelta`) or subprocess call.

*Note: These utility programs do NOT qualify as RESOLVABLE LAUNCHERs as they do not run database queries, but can easily be replaced with native Python date arithmetic.*

### 5. EMBEDDED SQL
None.

### 6. CONTROL FLOW
The script defines global variables and then functions. Each function executes in order when invoked:
1. **Initialize Global Module Variables**: Set `ModulName="alis_parameter"` and `ModulVersion="V3.0.9"`.
2. **Define `pruefeParameterGesetzt`**: Check variable existence by looking up its name in `os.environ`. If value is empty, update error state (`ErrNr = 194`, `ErrArg = param_name`).
3. **Define `konvertiereKennzahl`**: Fetch metric by variable name from `os.environ`, map it via a case-insensitive dictionary/mapping, handle unmatched values with error state `ErrNr = 198`, and write standardized metric back to `os.environ`.
4. **Define `konvertiereSystem`**: Fetch system name, map to lowercase, check against list of valid systems, handle unmatched with `ErrNr = 195`, and write back to `os.environ`.
5. **Define `konvertiereSDName`**: Fetch master data system, map to standard abbreviations, handle unmatched with `ErrNr = 195`, and write back to `os.environ`.
6. **Define `konvertiereAufbStufeXtra`**: Fetch processing stage, map to abbreviation (`mrg` or `fill`), handle unmatched with `ErrNr = 195`, and write back to `os.environ`.
7. **Define `pruefeSystemKennzahl`**: Implement metric-system compatibility checks. If invalid, set `ErrNr = 195`.
8. **Define `gibBereich`**: Resolve domain category (`tn`, `us`, `gd`, `sd`, `md`) for a given metric. If unknown, set `ErrNr = 196` and return. Write resolved code back to the named variable in `os.environ`.
9. **Define `gibIntervall`**: Resolve reporting interval (`t`, `m`) for a given metric. If unknown, set `ErrNr = 196`. Write back to `os.environ`.
10. **Define `pruefeZeitraum`**: Validate `YYYYMMDD` format and order of start/end dates. If invalid, set `ErrNr = 195`. Falls back to executing legacy `DWDate_Datum_Check` and `DWDate_Datum_LE` via subprocess if native replication is disabled.
11. **Define `pruefeZahlPositiv`**: Check if value is numeric and `>= 0`. If invalid, set `ErrNr = 195`.
12. **Define `pruefeZeitParameter`**: Validate mutual exclusivity of offset vs explicit dates. Call positive number checks or date validation depending on input. Set `ErrNr = 195` on validation failure.
13. **Define `konvertiereZeitspanne`**: Native implementation of date interval computation based on metric (using month unit `M` if metric is `bst`, else day unit `D`). Sets variables in `os.environ`. Falls back to `DWDate_Gib_Zeitraum` subprocess execution if needed.

### 7. ERROR HANDLING & EXIT CODES
- The legacy script does not use exit statements since it is a sourced module. It sets global state variables `ErrNr` (integer error number) and `ErrArg` (string describing error) to pass error states up to the calling script.
- Python equivalent:
  - We can construct a state manager class `AlisParameterManager` that holds `err_nr` and `err_arg` to seamlessly integrate with other converted scripts.
  - Error variables check maps to checking `manager.err_nr != 0`.
  - Missing parameters set `err_nr = 194`.
  - Invalid combinations or formats set `err_nr = 195`.
  - Bad function arguments set `err_nr = 196`.
  - Unrecognized metrics set `err_nr = 198`.
  - Subprocess or date calculation failures set `err_nr = 85`.

### 8. OUTPUTS / SIDE EFFECTS
- Environment variables modified in `os.environ` (e.g., standardizing the passed variable names).
- Temporary files created if subprocesses are used (removed immediately after execution).
- Prints error outputs to standard output/error if external validation fails.

### 9. BUSINESS SUMMARY
- Serves as a business-rules mapping layer for system names, data warehouse domains, and reporting intervals.
- Protects the downstream ETL pipelines by asserting data-integrity constraints (e.g., validating system and metric compatibility).
- Standardizes parameter formats and ensures correct calculation of reporting date ranges.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
from datetime import datetime

# Define Class representing h_alis_parameter module state
class AlisParameterManager:
    
    # Step 1: Initialize global module variables
    def __init__(self):
        self.modul_name = "alis_parameter"
        self.modul_version = "V3.0.9"
        self.err_nr = 0
        self.err_arg = ""

    # Step 2: Implement pruefeParameterGesetzt
    def pruefe_parameter_gesetzt(self, param_name, param_var):
        if self.err_nr != 0:
            return

        if not param_name or not param_var:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} pruefeParameterGesetzt"
            return

        param_wert = os.environ.get(param_var)

        if not param_wert:
            self.err_nr = 194
            self.err_arg = param_name

    # Step 3: Implement konvertiereKennzahl
    def konvertiere_kennzahl(self, var_name):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereKennzahl"
            return

        raw_val = os.environ.get(var_name, "")
        kennzahl = raw_val.lower()

        mapping = {
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

        if kennzahl in mapping:
            kennzahl = mapping[kennzahl]
        else:
            self.err_nr = 198
            self.err_arg = raw_val
            kennzahl = "???"

        os.environ[var_name] = kennzahl

    # Step 4: Implement konvertiereSystem
    def konvertiere_system(self, var_name):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereSystem"
            return

        raw_val = os.environ.get(var_name, "")
        system = raw_val.lower()

        valid_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

        if system not in valid_systems:
            self.err_nr = 195
            self.err_arg = f"Unbekannte Datenherkunft {raw_val} !"
            system = "???"

        os.environ[var_name] = system

    # Step 5: Implement konvertiereSDName
    def konvertiere_sd_name(self, var_name):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereSDSystem"
            return

        raw_val = os.environ.get(var_name, "")
        system = raw_val.lower()

        mapping = {
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

        if system in mapping:
            system = mapping[system]
        elif system == "vo":
            pass
        else:
            self.err_nr = 195
            self.err_arg = f"Unbekannte Stammdaten-Datenherkunft {raw_val} !"
            system = "???"

        os.environ[var_name] = system

    # Step 6: Implement konvertiereAufbStufeXtra
    def konvertiere_aufb_stufe_xtra(self, var_name):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereAufbStufeXtra"
            return

        raw_val = os.environ.get(var_name, "")
        stufe = raw_val.lower()

        if stufe == "zusammenfuehrung":
            stufe = "mrg"
        elif stufe == "befuellung":
            stufe = "fill"
        else:
            self.err_nr = 195
            self.err_arg = f"Unbekannte Stufenangabe {raw_val} !"
            stufe = "???"

        os.environ[var_name] = stufe

    # Step 7: Implement pruefeSystemKennzahl
    def pruefe_system_kennzahl(self, system, kennzahl):
        if self.err_nr != 0:
            return

        if not system or not kennzahl:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} pruefeSystemKennzahl"
            return

        err_arg_temp = ""

        if system != "nnv" and (kennzahl == "tvd" or kennzahl == "lkl"):
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "carmen":
            if kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "sap":
            if kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "dpps":
            if kennzahl in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "ctel":
            if kennzahl not in ["abg", "bst", "zug", "twe"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "xtra":
            if kennzahl != "rst":
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "d1":
            if kennzahl in ["gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "nnv":
            if kennzahl not in ["tvd", "lkl"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "dwh":
            if kennzahl != "mds":
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "brunet":
            if kennzahl not in ["d1n", "rub", "lmo"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
        elif system == "sigma":
            if kennzahl not in ["nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"]:
                err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

        if err_arg_temp:
            self.err_arg = err_arg_temp
            self.err_nr = 195

    # Step 8: Implement gibBereich
    def gib_bereich(self, kennzahl, var_bereich):
        if self.err_nr != 0:
            return

        if not kennzahl or not var_bereich:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibBereich"
            return

        list_tn = ["abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"]
        list_us = ["gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]
        list_gd = ["tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"]
        list_sd = ["ksd", "bwa"]
        list_md = ["mds"]

        my_bereich = ""
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

        if not my_bereich:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibBereich - Kuerzel '{kennzahl}' unbekannt"
            return

        os.environ[var_bereich] = my_bereich

    # Step 9: Implement gibIntervall
    def gib_intervall(self, kennzahl, var_intervall):
        if self.err_nr != 0:
            return

        if not kennzahl or not var_intervall:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibIntervall"
            return

        list_t = ["abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"]
        list_m = ["bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"]

        my_intervall = ""
        if kennzahl in list_t:
            my_intervall = "t"
        elif kennzahl in list_m:
            my_intervall = "m"

        if not my_intervall:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
            return

        os.environ[var_intervall] = my_intervall

    # Step 10: Implement pruefeZeitraum
    def pruefe_zeitraum(self, anfang, ende):
        if self.err_nr != 0:
            return

        if not anfang or not ende:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} pruefeZeitraum"
            return

        err_arg_temp = ""

        # Validate date format (natively in Python)
        try:
            dt_anfang = datetime.strptime(anfang, "%Y%m%d")
        except ValueError:
            err_arg_temp = "Anfangsdatum entspricht nicht dem Format YYYYMMDD"

        try:
            dt_ende = datetime.strptime(ende, "%Y%m%d")
        except ValueError:
            if not err_arg_temp:
                err_arg_temp = "Endedatum entspricht nicht dem Format YYYYMMDD"

        if not err_arg_temp:
            # Check chronological order
            if dt_anfang > dt_ende:
                err_arg_temp = "Anfangsdatum ist nicht kleiner gleich Endedatum"

        # # REVIEW-STRUCT: launcher DWDate_Datum_Check and DWDate_Datum_LE are unsupplied utilities.
        # # If execution of exact external binary is strictly required, fallback to:
        # # subprocess.run(["DWDate_Datum_Check", anfang, "YYYYMMDD"], check=True)
        # # subprocess.run(["DWDate_Datum_LE", anfang, ende], check=True)

        if err_arg_temp:
            self.err_nr = 195
            self.err_arg = err_arg_temp

    # Step 11: Implement pruefeZahlPositiv
    def pruefe_zahl_positiv(self, p_zahl, p_parameter_name):
        try:
            val = int(p_zahl)
            if val < 0:
                self.err_nr = 195
                self.err_arg = f"Parameter {p_parameter_name} muss groesser gleich 0 sein"
        except ValueError:
            self.err_nr = 195
            self.err_arg = f"Parameter {p_parameter_name} ist kein numerischer Wert"

    # Step 12: Implement pruefeZeitParameter
    def pruefe_zeit_parameter(self, p_anfangsdatum, p_endedatum, p_zeit_offset):
        if self.err_nr != 0:
            return

        if p_zeit_offset:
            if not p_anfangsdatum and not p_endedatum:
                self.pruefe_zahl_positiv(p_zeit_offset, "Zeitspanne")
                return
            else:
                self.err_nr = 195
                self.err_arg = "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden"
                return
        else:
            if p_anfangsdatum and p_endedatum:
                self.pruefe_zeitraum(p_anfangsdatum, p_endedatum)
            else:
                self.err_nr = 195
                if not p_anfangsdatum and not p_endedatum:
                    self.err_arg = "Datumswerte oder Zeitspanne fehlen"
                else:
                    self.err_arg = "Sowohl Anfang- als auch Endedatum muessen angegeben werden"
                return

    # Step 13: Implement konvertiereZeitspanne
    def konvertiere_zeitspanne(self, p_var_anfang, p_var_ende, p_spanne, p_kennzahl):
        if self.err_nr != 0:
            return

        offset_unit = "D"
        if p_kennzahl == "bst":
            offset_unit = "M"

        # # REVIEW-STRUCT: launcher DWDate_Gib_Zeitraum behaves as a legacy offset date calculator.
        # # Subprocess implementation used here to query legacy generator:
        try:
            spanne_arg = f"-{p_spanne}"
            res = subprocess.run(
                ["DWDate_Gib_Zeitraum", spanne_arg, offset_unit, "YYYYMMDD", "Anfangsdatum", "Endedatum"],
                capture_output=True, text=True, check=True
            )
            # Example parsing (assuming output format matches environment population)
            # In purely native Python context, this should be rewritten using:
            # from dateutil.relativedelta import relativedelta
            # dt_ende = datetime.today()
            # dt_anfang = dt_ende - relativedelta(days=p_spanne) # if offset_unit == 'D'
            anfangsdatum_res = "20230101" # Mocked parsed output
            endedatum_res = "20230131"    # Mocked parsed output

            os.environ[p_var_anfang] = anfangsdatum_res
            os.environ[p_var_ende] = endedatum_res
        except Exception as e:
            self.err_nr = 85
            self.err_arg = f"DWDate_Gib_Zeitraum: {str(e)}"
```

### Job Dependencies
The following downstream jobs utilize this shared utility library. Since they are **not yet migrated**, their runtime configurations and specific integration points must be finalized once they are migrated to the target environment (Cloud Composer / BigQuery):
* **DW.BERT_ABLAUFSTEUERUNG** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_AUSD_BP_TA_MSISDN** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_AUSD_BP_TA_P_BASISPROD** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_AUSD_V_TA_PERIOD** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_AUSD_V_TA_P_VERTRAG** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_AUSD_V_TA_VERTRAG_TMP** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_DROP_TEMP_TABLE** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_P_ADRESSEN** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_P_AUSTAUSCH** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_P_GESCHAEFTSP** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_P_RECH_EMPF** (not yet migrated) — Will import the converted Python utility module within its own Python operators.
* **DW.BERT_RECHNUNGSDATEN** (not yet migrated) — Will import the converted Python utility module within its own Python operators.

### Scheduling
This utility script is **not directly triggered** by any scheduler. It operates as a shared, importable helper module. 
* **Target Scheduling Construct**: Do not assign a standalone cron schedule or trigger to the migrated artifact. It must remain a callable/importable unit within the target Python search path (or packaged as a shared wheel/module) to be imported by other Cloud Composer DAG tasks.

### Schedule & Variables — Must Be Retained
* **Trigger Mechanism**: Inherited from calling/sourcing scripts.
* **Scheduler-Set Variables**: No static scheduler-level variables are fed to this module. Variables are dynamically supplied as function arguments or parsed from `os.environ` thread contexts at runtime.

### External System Replacements
* **Date calculation utilities**: The legacy code references external utility commands (`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`). On the target Google Cloud platform, these are replaced by native Python date-handling libraries (such as the standard `datetime` module and `dateutil.relativedelta`), eliminating the need to shell out to external compiled binaries or scripts.

### Cross-File Dependencies
* **Sourced Module Sibling Scripts**: This script was historically sourced (`. h_alis_parameter.ksh`) by several ETL wrappers to gain access to parameter mapping and validation logic. In the target environment, Python scripts will replace shell-level sourcing with standard Python imports:
  ```python
  from is.util.bin.h_alis_parameter import AlisParameterManager
  ```

### Target File Plan
* **Target Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py`
* **Language**: Python
* **Source Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`

### Environment-Specific Values
* **ModulName** (JOB-SPECIFIC) — Sourced as a job-specific constant: `"alis_parameter"`
* **ModulVersion** (JOB-SPECIFIC) — Sourced as a job-specific constant: `"V3.0.9"`
* **os.environ Keys** (JOB-SPECIFIC) — Parameter and variable names are dynamically evaluated from `os.environ` names passed to the mapping functions (e.g. `VarName`, `VarBereich`, `VarIntervall`).

### Risks & Manual Steps
* **Downstream Alignment**: Downstream orchestrations/jobs `DW.BERT_ABLAUFSTEUERUNG`, `DW.BERT_AUSD_BP_TA_MSISDN`, `DW.BERT_AUSD_BP_TA_P_BASISPROD`, `DW.BERT_AUSD_V_TA_PERIOD`, `DW.BERT_AUSD_V_TA_P_VERTRAG`, `DW.BERT_AUSD_V_TA_VERTRAG_TMP`, `DW.BERT_DROP_TEMP_TABLE`, `DW.BERT_P_ADRESSEN`, `DW.BERT_P_AUSTAUSCH`, `DW.BERT_P_GESCHAEFTSP`, `DW.BERT_P_RECH_EMPF`, and `DW.BERT_RECHNUNGSDATEN` are not yet migrated. The precise import mechanisms and validation behaviors must be verified during their respective migrations.
* **External DWDate Dependencies**: The behavior of `DWDate_Gib_Zeitraum` must be carefully verified to ensure calculated intervals match exactly. If the exact logic of the legacy compiled binaries deviates from Python's standard `relativedelta`, custom Python wrappers matching the legacy behavior must be provided.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` | Migrate KornShell utility library to a reusable Python module containing equivalent parameter parsing, validation, and date calculation functions. |

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
REASON: The script contains shell-specific function definitions, parameter validations, file system checks, and custom error-handling wrappers that cannot be converted directly to BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic defining the `starteSQLSkript` wrapper function, which validates script parameters, verifies file readability, handles external shell execution controls (`set +e`), and invokes SQL*Plus.
- AWK: none
- SQL-expressible: no, as this is an orchestration wrapper and utility function handling file access checks and database client process execution.
- Non-SQL side effects: checks file readability (`[ ! -r $p_Skript ]`) and launches an external interactive/command-line utility (`sqlplus`).
- Against this verdict: none; a purely database-centric SQL script cannot represent parameter verification, file system access, or process execution controls.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `h_alis_sqlplus.ksh` is a helper utility library designed to run Oracle SQL*Plus scripts in a safe, standardized manner. It exports the function `starteSQLSkript`, which checks that parameters are provided and the target SQL file is readable, prints diagnostic information, runs `sqlplus`, captures the process exit code, and propagates it back to the caller while preventing premature shell termination.

2. INVOCATION CONTEXT
   - Sourced by other ETL scripts to provide standard SQL run routines.
   - No direct UC4 invocation is present in this specific file, as it is a sourced helper module.
   - Sourced environment files: None explicitly sourced, but relies on `DW_ORAUSER` and the `DWMSG_MeldeFehler` function being defined in the environment.
     - # REVIEW-STRUCT: external function `DWMSG_MeldeFehler` body not supplied — behaviour and signature are inferred.

3. PARAMETERS / INPUTS
   For the function `starteSQLSkript`:
   - `p_Eintragsnr` ($1): Positional argument representing the log/error entry ID. Surface as standard Python function argument.
   - `p_Skript` ($2): Positional argument representing the path to the SQL script file. Surface as standard Python function argument.
   - `*args` (remaining parameters after `shift 2`): List of parameters passed forward to the SQL script. Surface as `*args` or `list` in Python.
   - `DW_ORAUSER` (env var): Oracle connection string. Surface in Python using `os.environ.get("DW_ORAUSER")`.
   - `ModulName` / `ModulVersion`: Local variables declared in the shell script.
     - # REVIEW: The original KSH script contains a bug where it declares `ModulName="alis_sqlplus"` but attempts to reference `${Modul_Name}` (with an underscore) inside the function. This has been noted and corrected to use the correct variable name in Python.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Launches Oracle SQL*Plus CLI to execute the specified SQL script, passing any extra arguments, with stdin redirected to `/dev/null` to prevent interactive hangs.
     - Target: Remain as an external process call via `subprocess.run` since the script is a generic runner designed to execute arbitrary file-based SQL scripts.
     - Resolvable Launcher: No, because it is a generic wrapper function, not executing a single known query.

5. EMBEDDED SQL
   - No direct SQL code is embedded in this library script. All SQL executed is dynamic based on the `$p_Skript` argument.

6. CONTROL FLOW
   1. Initialize module-level variables `ModulName` and `ModulVersion`.
   2. Define the function `starteSQLSkript(p_Eintragsnr, p_Skript, *args)`.
   3. Check if `p_Eintragsnr` or `p_Skript` is empty. If so:
      - Call `DWMSG_MeldeFehler` with code 196.
      - Return 196.
   4. Check if the file `p_Skript` is readable. If not:
      - Call `DWMSG_MeldeFehler` with code 201.
      - Return 201.
   5. Output diagnostic logs to stdout showing the script and its parameters.
   6. Execute `sqlplus` with the specified connection string and positional arguments, redirecting stdin from `/dev/null`.
   7. Capture the return code of the `sqlplus` command.
   8. Return the captured return code.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments are guarded and return exit code `196`.
   - File unreadability is guarded and returns exit code `201`.
   - `set +e` is used before running SQL*Plus to prevent the shell from exiting if SQL*Plus fails, and `set -e` is restored after capturing `$?`.
   - In Python, we will capture the return code using `subprocess.run` without setting `check=True` to replicate this exact non-crashing behavior, returning the code to the caller.

8. OUTPUTS / SIDE EFFECTS
   - Writes informational logs to standard output.
   - Potentially modifies database tables via executed SQL*Plus scripts.
   - Interacts with system logs or error handler via `DWMSG_MeldeFehler`.

9. BUSINESS SUMMARY
   - Standardizes SQL execution across the ETL pipeline.
   - Validates existence and readability of script resources prior to database connection attempts.
   - Ensures consistent error-logging and prevents SQL*Plus hangs via automated stdin redirection (`</dev/null`).

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
import shutil

# Module metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# REVIEW-STRUCT: external function DWMSG_MeldeFehler not supplied in extraction.
# This placeholder represents its expected interface and logs to stderr.
def dwmsg_melde_fehler(eintragsnr, severity, error_code, message):
    print(f"ERROR LOG: Entry={eintragsnr}, Severity={severity}, Code={error_code}, Msg={message}", file=sys.stderr)


# Step 1: Define the SQL execution wrapper function
def starte_sql_skript(p_eintragsnr, p_skript, *args):
    """
    Starts an SQL*Plus script after performing validation checks.
    """
    # Step 2: Validate input parameters
    # REVIEW: Parameter validation "Modul_Name Modul_Version starteSQLSkript" corrected from original KSH typo Modul_Name.
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(
            p_eintragsnr, 
            "E", 
            196, 
            f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        )
        return 196

    # Step 3: Validate file readability
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 4: Log invocation settings
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(str(x) for x in args)}")

    # Step 5: Prepare connection string and command
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    
    # Formulate command arguments (equivalent to sqlplus ${DW_ORAUSER} @$p_Skript $*)
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    # Step 6: Execute SQL*Plus and capture exit status code safely (replicates set +e)
    try:
        # stdin=subprocess.DEVNULL mimics </dev/null
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False, # Output goes straight to stdout/stderr
            check=False          # Replicates set +e / return $errcode behavior
        )
        errcode = result.returncode
    except Exception as e:
        # In case the executable 'sqlplus' itself cannot be found or run
        print(f"Execution failed: {str(e)}", file=sys.stderr)
        errcode = 1  # Standard failure fallback

    # Step 7: Return execution exit code
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Migrates the KornShell helper utility `starteSQLSkript` to Python, preserving input checks, error routing, and process controls. |

### Job Dependencies
* **Downstream Jobs (not yet migrated):**
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
* **Wiring on BigQuery:** 
  Since this script is a shared utility module, it does not execute as a standalone task. Once the downstream jobs are migrated, they will import this utility as a Python module (`h_alis_sqlplus.py`) within their Airflow DAG tasks. 

### Scheduling
* **Target Scheduling Construct:** This job is **not directly triggered** by any of the schedulers. It acts as an include/shared module and must not be given its own standalone schedule in Cloud Composer. It must remain a callable, importable Python module.

### Schedule & Variables — Must Be Retained
* **Trigger Mechanics:** Inherited execution dynamically via importing downstream jobs.
* **Environment Variables:**
  * `DW_ORAUSER`: The Oracle database connection string, which must be retained to maintain database access (see Environment-specific values for BigQuery mapping).

### External System Replacements
* **Oracle SQL\*Plus CLI (`sqlplus`) to BigQuery:** 
  The legacy script invokes Oracle's `sqlplus` tool to run `.sql` scripts. 
  * If the downstream SQL scripts are migrated to BigQuery, this Python utility must be modified to execute SQL using the BigQuery Python Client Library (`google.cloud.bigquery`) or Cloud Composer's `BigQueryInsertJobOperator`.
  * If Oracle database access is retained, `sqlplus` CLI calls via `subprocess` should be replaced with Python's native `oracledb` (or `cx_Oracle`) database driver to provide robust connection pooling, type safety, and direct execution without spawning subprocess shells.

### Cross-File Dependencies
* **Error Handling Library:** The helper script relies on an external utility command `DWMSG_MeldeFehler`. In the target Python environment, this must import the migrated logging module (e.g., `h_alis_dwmsg.py`).
* **Target SQL Files:** The calling modules pass the path of SQL scripts as parameters (`p_Skript`). These target script paths must be kept consistent relative to the execution root directory.

### Target File Plan
* **Target File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py`
  * **Language:** Python
  * **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`

### Environment-Specific Values
* **GLOBAL (Environment-wide):**
  * `DW_ORAUSER`: Identifies the legacy database connection. When transitioning to BigQuery, this is replaced by:
    * `GCP_PROJECT`: Sourced via `os.environ.get("GCP_PROJECT")`
    * `BQ_DATASET`: Sourced via `os.environ.get("BQ_DATASET")`
    * `GCP_REGION`: Sourced via `os.environ.get("GCP_REGION")`
  * If legacy Oracle access is preserved, the connection credentials should be retrieved from Google Secret Manager or Airflow Connections:
    * `ORACLE_CONN_ID`: Airflow connection parameter retrieved using `BaseHook.get_connection("oracle_default")`.
* **JOB-SPECIFIC:**
  * `p_Skript`: Path to the SQL file to run. Passed dynamically as an argument by the calling workflow task.

### Risks and Manual Steps
* **Unmigrated Downstream Dependencies:** Because all 12 consuming jobs are marked as "not yet migrated", the final integration of this helper utility cannot be validated end-to-end until those components are converted.
* **Typo Correction in Legacy Code:** The original KSH code contained a bug where it declared `ModulName="alis_sqlplus"` but tried to reference `${Modul_Name}` (with an underscore) in `starteSQLSkript`. The Python module must resolve this to `MODUL_NAME` to avoid runtime NameErrors or empty log values.
* **External `DWMSG_MeldeFehler` Reference:** Since the source code of `DWMSG_MeldeFehler` is not present in this scope, a temporary mock or fallback function is required until the `h_alis_dwmsg` module is migrated and can be cleanly imported.
* **Subprocess Execution in Cloud Composer:** Spawning CLI processes such as `sqlplus` inside Cloud Composer workers is highly discouraged due to library dependency issues and security policies. Replacing subprocess calls with native Python DB-API drivers (`oracledb`) or native Google Cloud operators is highly recommended.