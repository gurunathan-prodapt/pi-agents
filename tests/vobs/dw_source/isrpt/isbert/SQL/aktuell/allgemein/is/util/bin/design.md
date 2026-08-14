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
REASON: The script is a reusable utility library defining multiple shell functions with parameter validations, local file operations, and Oracle SQL*Plus invocations.

EVIDENCE
- Business logic found: KSH custom logic defines a suite of 9 error-handling, status-tracking, and message-logging functions that interact with an Oracle database and manage local process states.
- AWK: none
- SQL-expressible: No. It is a control-flow and process management utility library that performs parameter checks, dynamic log filename creation, and executes external SQL scripts.
- Non-SQL side effects: Writes and deletes temporary files in `/tmp`, builds log-file paths using system dates, and manages shell-level status tracking and error propagation.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) is a reusable KornShell utility library designed to standardize error handling and execution tracking across the Information Services (IS) data warehouse platform. It provides a set of helper functions that interface with Oracle database PL/SQL procedures (specifically within the `BERT_MELDUNG` package) to initialize logs, update process statuses, record execution errors, and track timing metrics. It is sourced by other operational KornShell scripts rather than being executed independently.

2. INVOCATION CONTEXT
   - **Caller**: Sourced dynamically (via `. f_alis_msgerr.ksh`) by operational ETL or orchestration scripts. No direct UC4 invocation exists for this library itself, but scripts sourcing it are triggered by UC4 Jobs (e.g., inside JOBS_UNIX tasks).
   - **UC4 Includes**: None referenced in this script.
   - **Sourced Environment Files**: None within this script. It expects the caller to have defined system environment variables like `DW_ORAUSER`, `DW_DIR_ROOT`, and `DW_DIR_PROT`.

3. PARAMETERS / INPUTS
   This script defines functions that accept positional arguments. Key environment variables used globally inside the functions include:
   - `DW_ORAUSER`: Database connection credentials used for `sqlplus` connection.
   - `DW_DIR_ROOT`: Root installation directory used to locate SQL scripts (e.g., `$DW_DIR_ROOT/allgemein/is/util/sql/...`).
   - `DW_DIR_PROT`: Target directory where log files are written.

   *Function-level parameters:*
   - `DWMSG_EintragsNr` (Positional `$1` in most functions): Unique numeric tracking identifier for a job run in the message table.
   - `VarName` (Positional `$1` in `DWMSG_ErmittleNr` and `DWMSG_Logdateiname`): Name of the environment variable where results are dynamically stored using `eval`.
   - `JobKennung` (Positional `$2` in `DWMSG_ErzeugeEintrag` and `DWMSG_Logdateiname`): Job identifier string.
   - `Programmname` (Positional `$3` in `DWMSG_ErzeugeEintrag`): Name of the script/program running.
   - `LogDatei` (Positional `$4` in `DWMSG_ErzeugeEintrag`): File path of the log output.
   - `Typ` (Positional `$2` in `DWMSG_MeldeFehler`): Alert classification (`F`=Fatal, `E`=Error, `W`=Warning).
   - `FehlerNr` (Positional `$3` in `DWMSG_MeldeFehler`): Integer code mapped to a registered system error.
   - `Zusatz1`, `Zusatz2` (Positional `$4`, `$5` in `DWMSG_MeldeFehler`): Optional text strings for error context (e.g., filename).
   - `DWMSG_Stichtag` (Positional `$2` in `DWMSG_SetzeStichtagInfo`): Date value string.
   - `DWMSG_StichtagFmt` (Positional `$3` in `DWMSG_SetzeStichtagInfo`): Oracle-compatible date format string.
   - `DWMSG_InfoText` (Positional `$2` in `DWMSG_AppendTimingInfos`): Progress or performance text.
   - `DWMSG_DateFormat` (Positional `$3` in `DWMSG_AppendTimingInfos`): Format for timestamp logging.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - **`sqlplus`**: Invoked across multiple functions to execute external PL/SQL scripts or anonymous blocks.
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null` (Database status update)
     - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null` (Database status update)
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null` (Fetches sequential tracking ID)
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null` (Initializes logging record)
     - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null` (Registers custom error details)
     - `sqlplus -s $DW_ORAUSER` with inline heredocs executing:
       `EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt')); commit;`
       `EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' '); commit;`
   - **`cat`, `tr`, `rm`**: Used to process and clean up temporary sequence ID files.
   - **`date`**: Generates timestamps for log filenames.

   *Resolvable Launcher evaluation*: These are direct calls to `sqlplus` invoking custom PL/SQL wrapper scripts (`d_alis_spaufruf_p*.sql`). They are best converted into native Python database calls using an Oracle client library (like `oracledb` / `cx_Oracle`) executing the PL/SQL blocks directly, rather than launching `sqlplus` sub-processes. This requires that Oracle-specific environment variables are present.

5. EMBEDDED SQL
   - **Source**: Inline heredocs within `DWMSG_SetzeStichtagInfo` and `DWMSG_AppendTimingInfos`.
   - **SQL Text**:
     - `EXEC BERT_MELDUNG.SetzeZusatzInfos(:DWMSG_EintragsNr, to_date(:DWMSG_Stichtag, :DWMSG_StichtagFmt)); commit;`
     - `EXEC BERT_MELDUNG.SetzeZusatzInfos(:DWMSG_EintragsNr, null, :DWMSG_InfoText || ' ' || to_char(SYSDATE, :DWMSG_DateFormat) || ' '); commit;`
   - **Statement Type**: PL/SQL stored procedure executions (`BERT_MELDUNG` package).
   - **Tables Touched**: Implicitly modifies tracking/logging tables handled internally by the package.
   - **Dialect**: Unambiguously Oracle SQL / PL/SQL (utilizes `EXEC`, `to_date`, `to_char`, `SYSDATE`, `commit`, and `||` string concatenation).

6. CONTROL FLOW
   Each function executes sequentially when called by parent scripts:
   1. **`DWMSG_Fehlerbehandlung`**: Triggered via shell traps when an unexpected error occurs (`returncode != 0`). Stores the error code, calls `DWMSG_MeldeFehler` with code `10` (unexpected error), and updates the status to Aborted via `DWMSG_SetzeStatusAbbruch`.
   2. **`DWMSG_SetzeStatusOK`**: Validates `DWMSG_EintragsNr`. Executes `BERT_MELDUNG.SetzeStatusOk` using `d_alis_spaufruf_p1.sql`. Exits with code `1` if the parameter is empty.
   3. **`DWMSG_SetzeStatusAbbruch`**: Validates `DWMSG_EintragsNr`. Executes `BERT_MELDUNG.SetzeStatusAbbruch` using `d_alis_spaufruf_p1.sql`. Exits with code `1` if empty.
   4. **`DWMSG_ErmittleNr`**: Validates dynamic variable storage target name. Creates a local temp file `/tmp/ErmittleNr_$$.lst`. Runs `d_al_is_ermittlenr.sql` to populate the file, reads/strips spaces, deletes the temp file, and dynamically updates the caller's target variable via `eval`.
   5. **`DWMSG_ErzeugeEintrag`**: Validates `DWMSG_EintragsNr`. Executes `BERT_MELDUNG.Erzeuge_Eintrag` using `d_alis_spaufruf_p4.sql` with the tracking ID, job ID, program name, and log file path.
   6. **`DWMSG_MeldeFehler`**: Validates `DWMSG_EintragsNr`. Evaluates optional parameters `Zusatz1` and `Zusatz2` to choose the correct SQL wrapper script dynamically (`d_alis_spaufruf_p3.sql` to `p5.sql`). Executes `BERT_MELDUNG.Fehler`.
   7. **`DWMSG_Logdateiname`**: Builds a log filename pattern: `${DW_DIR_PROT}/${JobKennung}_$(date '+%Y%m%d_%H%M')_${DWMSG_EintragsNr}.log` and dynamically assigns it to the variable passed as the first parameter.
   8. **`DWMSG_SetzeStichtagInfo`**: Validates parameter bounds (EintragsNr, Stichtag, Format). Executes the `BERT_MELDUNG.SetzeZusatzInfos` procedure utilizing `to_date` conversions.
   9. **`DWMSG_AppendTimingInfos`**: Validates parameters. Appends timed text tracking output to the Oracle package session using `to_char(SYSDATE, DateFormat)`.

7. ERROR HANDLING & EXIT CODES
   - Functions perform explicit checks on mandatory parameters: `if [ -z "$PARAMETER" ]`. On failure, they write a terminal warning message and exit the process using exit code `1` (or `2` for date-format missing parameters).
   - Operational failures from individual `sqlplus` commands are not explicitly captured at each call step, but `DWMSG_Fehlerbehandlung` acts as a catch-all routine for parent script errors when bound via `trap DWMSG_Fehlerbehandlung ERR`.
   - In Python, these validation errors must raise standard exceptions (`ValueError` / `KeyError`), and SQL execution errors should raise DB API driver-specific exceptions (`oracledb.DatabaseError`).

8. OUTPUTS / SIDE EFFECTS
   - **Database Updates**: Executed via PL/SQL package calls (`BERT_MELDUNG`).
   - **Local filesystem**: Creates and deletes `/tmp/ErmittleNr_[PID].lst` during unique tracking ID acquisition.
   - **Console logs**: Standard output (`stdout`) and error logging messages indicating failure states.

9. BUSINESS SUMMARY
   - Standardizes the creation, progression, and error logging of batch database operations.
   - Inserts run metadata (job ID, program name, and trace file locations) inside an Oracle administrative schema.
   - Updates execution statuses dynamically to provide centralized monitoring visibility.
   - Enriches operational logs with timeline and date partition details (`Stichtag`).
   - Standardizes recovery management by writing explicit status information when failures occur.

=======================================================================================
PSEUDOCODE OUTLINE (PYTHON STYLE)
=======================================================================================

```python
# Sourced utility library containing error management functions.
# Consists of a class containing native Python DB client integrations.

import os
import sys
import datetime
import subprocess

# REVIEW: target database platform confirmed as Oracle. DB client choice 'oracledb' is used below.
import oracledb

class AlisMsgErr:
    def __init__(self):
        # Validate that necessary environment variables are set
        self.dw_orauser = os.environ.get("DW_ORAUSER")
        self.dw_dir_root = os.environ.get("DW_DIR_ROOT")
        self.dw_dir_prot = os.environ.get("DW_DIR_PROT")
        
        # In a real environment, the DB connection pool/connection would be managed here.
        self.db_connection = None 

    def _get_connection(self):
        # Helper to initialize DB connection using credentials in DW_ORAUSER
        if not self.db_connection:
            # REVIEW: Confirm connection string parsing format from DW_ORAUSER
            self.db_connection = oracledb.connect(self.dw_orauser)
        return self.db_connection

    # Step 1: DWMSG_Fehlerbehandlung
    def dwmsg_fehlerbehandlung(self, dwmsg_eintrags_nr, last_exit_code=1):
        # Captures system error status, logs unexpected error status 10, and aborts tracking status
        k_unerw_fehler = 10
        print(f"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus. ErrorCode: {last_exit_code}")
        
        self.dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {last_exit_code}")
        self.dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)

    # Step 2: DWMSG_SetzeStatusOK
    def dwmsg_setze_status_ok(self, dwmsg_eintrags_nr):
        if not dwmsg_eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
            sys.exit(1)
            
        # Execute stored procedure call BERT_MELDUNG.SetzeStatusOk
        # Legacy: sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [dwmsg_eintrags_nr])
            conn.commit()

    # Step 3: DWMSG_SetzeStatusAbbruch
    def dwmsg_setze_status_abbruch(self, dwmsg_eintrags_nr):
        if not dwmsg_eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
            sys.exit(1)
            
        # Execute stored procedure call BERT_MELDUNG.SetzeStatusAbbruch
        # Legacy: sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [dwmsg_eintrags_nr])
            conn.commit()

    # Step 4: DWMSG_ErmittleNr
    def dwmsg_ermittle_nr(self):
        # Legacy wrote to a temp file using d_al_is_ermittlenr.sql. Direct execute is preferred.
        # Temp file logic legacy simulation included for safety:
        # temp_file = f"/tmp/ErmittleNr_{os.getpid()}.lst"
        
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Native PL/SQL approach returning sequence/unique tracking ID
            # Assuming d_al_is_ermittlenr.sql fetches BERT_MELDUNG sequence or procedure.
            # REVIEW: Confirm if d_al_is_ermittlenr.sql runs a select on sequence or a function call
            out_val = cursor.var(oracledb.NUMBER)
            cursor.execute("BEGIN :out_val := BERT_MELDUNG.GetNextNr; END;", out_val=out_val)
            eintrags_nr = int(out_val.getvalue())
            
        return eintrags_nr

    # Step 5: DWMSG_ErzeugeEintrag
    def dwmsg_erzeuge_eintrag(self, dwmsg_eintrags_nr, job_kennung, programm_name, log_datei):
        if not dwmsg_eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
            sys.exit(1)
            
        # Legacy: sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag ...
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [dwmsg_eintrags_nr, job_kennung, programm_name, log_datei])
            conn.commit()

    # Step 6: DWMSG_MeldeFehler
    def dwmsg_melde_fehler(self, dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
        if not dwmsg_eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
            sys.exit(1)
            
        # Dynamic argument lists mapping parameters to BERT_MELDUNG.Fehler procedure
        # Legacy used dynamically resolved sql script depending on whether arguments are set.
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Pass args explicitly, keeping empty string logic
            cursor.callproc("BERT_MELDUNG.Fehler", [typ, dwmsg_eintrags_nr, fehler_nr, zusatz1, zusatz2])
            conn.commit()

    # Step 7: DWMSG_Logdateiname
    def dwmsg_logdateiname(self, job_kennung, dwmsg_eintrags_nr):
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        log_filename = f"{self.dw_dir_prot}/{job_kennung}_{current_time}_{dwmsg_eintrags_nr}.log"
        return log_filename

    # Step 8: DWMSG_SetzeStichtagInfo
    def dwmsg_setze_stichtag_info(self, dwmsg_eintrags_nr, stichtag, stichtag_fmt):
        if not dwmsg_eintrags_nr:
            print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
            sys.exit(1)
        if not stichtag:
            print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
            sys.exit(1)
        if not stichtag_fmt:
            print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
            sys.exit(2)
            
        # Execute procedure SetzeZusatzInfos
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Convert Oracle format string placeholders dynamically if needed.
            # Python equivalent runs the exact raw SQL to retain formatting parsing in Oracle
            sql_stmt = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, to_date(:stichtag, :stichtag_fmt));
            END;
            """
            cursor.execute(sql_stmt, eintrags_nr=dwmsg_eintrags_nr, stichtag=stichtag, stichtag_fmt=stichtag_fmt)
            conn.commit()

    # Step 9: DWMSG_AppendTimingInfos
    def dwmsg_append_timing_infos(self, dwmsg_eintrags_nr, info_text, date_format):
        if not dwmsg_eintrags_nr:
            print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
            sys.exit(1)
        if not date_format:
            print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
            sys.exit(2)
            
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Inline legacy Oracle logic: to_char(SYSDATE, :date_format)
            sql_stmt = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, null, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
            END;
            """
            cursor.execute(sql_stmt, eintrags_nr=dwmsg_eintrags_nr, info_text=info_text, date_format=date_format)
            conn.commit()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | KornShell utility script converted to a reusable Python helper library containing logging, status-tracking, and runtime error-handling functions. |

---

### Job Dependencies
The following downstream consumer jobs depend on sourcing this logging and status-tracking utility library:
* **DW.BERT_ABLAUFSTEUERUNG** — *not yet migrated*
* **DW.BERT_AUSD_BP_TA_MSISDN** — *not yet migrated*
* **DW.BERT_AUSD_BP_TA_P_BASISPROD** — *not yet migrated*
* **DW.BERT_AUSD_V_TA_PERIOD** — *not yet migrated*
* **DW.BERT_AUSD_V_TA_P_VERTRAG** — *not yet migrated*
* **DW.BERT_AUSD_V_TA_VERTRAG_TMP** — *not yet migrated*
* **DW.BERT_DROP_TEMP_TABLE** — *not yet migrated*
* **DW.BERT_P_ADRESSEN** — *not yet migrated*
* **DW.BERT_P_AUSTAUSCH** — *not yet migrated*
* **DW.BERT_P_GESCHAEFTSP** — — *not yet migrated*
* **DW.BERT_P_RECH_EMPF** — *not yet migrated*
* **DW.BERT_RECHNUNGSDATEN** — *not yet migrated*
* **DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP** — *not yet migrated*
* **DW.DWH_VVTN_IAR_BGF_GUTSCHR** — *not yet migrated*

**Wiring on Target Platform**:
Because this script functions as a library rather than a standalone scheduled job, these downstream jobs will import this Python module (`f_alis_msgerr.py`) directly. When they are migrated to Cloud Composer, their corresponding Python DAGs/operators will call the logging and status-handling methods defined in this library. 

---

### Scheduling
* **Target Scheduling Construct**: This utility library is not directly scheduled by any primary orchestrator. It executes dynamically as an imported shared module inside scheduled ETL pipelines. It must remain a callable/importable unit without its own standalone schedule.

---

### Schedule & Variables
* **Runtime Parameter Mapping**: Since this library runs in-memory as part of the calling Python processes, arguments such as the tracking identifier (`DWMSG_EintragsNr`) are passed directly as function arguments. Other global parameters (like log directories) are resolved via Cloud Composer's environment-wide variables.

---

### Lineage
* **Upstream/Internal Hand-offs**:
  * `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` **--[CALLS_PROCEDURE]-->** `PROCEDURE:SETZEZUSATZINFOS` (Confidence: 0.75). This lineage represents the invocation of administrative metadata updates inside the database.

---

### External System Replacements
* **Oracle Database (`BERT_MELDUNG` package)**: BigQuery has no direct equivalent for Oracle PL/SQL packages. These operations must be mapped to BigQuery audit/logging tables (via the BigQuery Python Client executing standardized DML statements) or redirected entirely to Google Cloud Logging API sinks.
* **Temporary Local Files (`/tmp/ErmittleNr_$$.lst`)**: Replaced entirely by in-memory Python variable assignments (avoiding local I/O).
* **Logging Directories (`DW_DIR_PROT`)**: Replaced by standard Google Cloud Storage (`GCS_BUCKET` paths) or Airflow's native logging configuration.

---

### Cross-File Dependencies
This script is structured around helper SQL files located in `allgemein/is/util/sql/` to execute database routines via `sqlplus`:
* `d_alis_spaufruf_p1.sql` (invokes `BERT_MELDUNG.SetzeStatusOk` / `SetzeStatusAbbruch`)
* `d_al_is_ermittlenr.sql` (determines sequence tracking number)
* `d_alis_spaufruf_p4.sql` (invokes `BERT_MELDUNG.Erzeuge_Eintrag`)
* `d_alis_spaufruf_p3.sql` / `d_alis_spaufruf_p5.sql` (dynamically chosen based on parameter counts to call `BERT_MELDUNG.Fehler`)

In the target Python design, these intermediate physical SQL files are bypassed entirely. Python's database client can execute equivalent parameterized queries or stored procedures directly.

---

### Target File Plan
* **`vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py`**
  * **Language**: Python
  * **Source**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
  * **Purpose**: Houses the Python-native equivalents of the 9 core logging and tracking functions.

---

### Environment-Specific Values
* **`DW_DIR_PROT` (GLOBAL)**: Normalizes to `GCS_BUCKET` (specifically representing the designated logging bucket subdirectory path, e.g., `gs://[GCS_BUCKET]/logs`).
* **`DW_DIR_ROOT` (GLOBAL)**: Normalizes to the system deployment root path within Cloud Composer workspace.
* **`DW_ORAUSER` (GLOBAL)**: Normalizes to `GCP_PROJECT` with Service Account authentication context (standard BigQuery IAM replaces explicit database logins).
* **`DWMSG_EintragsNr` (JOB-SPECIFIC)**: Mapped to runtime tracking parameters, typically generated using Cloud Composer Airflow `run_id` or sequential process numbers.

---

### Risks and Manual Steps
* **PL/SQL Stored Procedure Conversion**: The extensive dependencies on Oracle's `BERT_MELDUNG` stored procedures (`SetzeStatusOk`, `SetzeStatusAbbruch`, `Fehler`, `Erzeuge_Eintrag`, `SetzeZusatzInfos`) require a database-side migration design. These must be manually re-implemented as BigQuery stored procedures or translated into direct SQL table operations.
* **Downstream Sourcing Gaps**: All 14 downstream jobs referencing this utility library are marked as "not yet migrated." The connection points must be verified and completed iteratively as those downstream jobs undergo migration.
* **External SQL Wrapper Dependencies**: The logic inside auxiliary wrapper SQL files (e.g., `d_al_is_ermittlenr.sql`) is unconfirmed within this scope. A human operator must verify that the sequential ID generation logic maps correctly to BigQuery.
* **OUTPUT/PRINT LITERAL RULE compliance**: To avoid breaking downstream automation patterns or operational expectations, the original log/warning statements in German must not be translated. The build phase must carry over the following literal messages character-for-character:
  * `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`
  * `"Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"`
  * `"Argh!, keinen Variablennamen bei ErmittleNr angegeben"`
  * `"Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`
  * `"Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"`
  * `"Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"`
  * `"Argh!, keinen Stichtag angegeben!"`
  * `"Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!"`
  * `"Argh!, Formatangabe erforderlich!"`
  * `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`

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
REASON: The script is a date utility library containing multiple function definitions, shell arithmetic, file manipulation, and Oracle SQL*Plus invocations.

EVIDENCE
- Business logic found: KSH custom logic contains helper functions for date checking, date comparison, period generation, and date arithmetic (including leap year checks, manual date addition loops, and month/year overflows).
- AWK: none
- SQL-expressible: No, because it defines environment-modifying shell utility functions, creates temp files, performs shell-based math, and is meant to be sourced as a helper library.
- Non-SQL side effects: Writes/deletes temporary files (`/tmp/h_alis_date_...`, `/tmp/tmp_...`), uses `eval` to set variables in the caller's environment, and exits with specific shell return codes.
- Against this verdict: A small part of the date logic could be written in BigQuery SQL functions, but the overall wrapper structure and variable-returning mechanism require Python to maintain utility-library usability.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `h_alis_date.ksh` is a date utility library (helper script) designed to provide common date calculation and validation routines for the ALIS data warehouse system. It defines several functions to calculate previous months, check date validity via Oracle, compare dates, add offsets to dates, find the last day of a month, and perform date arithmetic. It reads system dates and configuration parameters, and writes output to temporary files and shell variables.

### 2. INVOCATION CONTEXT
- **Sourced By**: This helper library is typically sourced (`. h_alis_date.ksh`) or executed by other DWH scripts or UC4 jobs.
- **UC4 Native Includes**: None referenced in the provided extraction.
- **Environment Sourced**: Comment mentions `.dw_init` must be run prior to using this script (not supplied).
  - `# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values`

### 3. PARAMETERS / INPUTS
The script does not have global arguments. Instead, its individual functions accept parameters:

#### `DWDate_Vormonat`
- **Name**: `VarName` (`$1`), `DWDate_FMT` (`$2`)
- **Source**: Passed as arguments to the function call.
- **Used**: Yes.
- **Python Mapping**: Standard positional arguments to a Python function.

#### `DWDate_Datum_Check`
- **Name**: `wert` (`$1`), `format` (`$2`)
- **Source**: Passed as arguments to the function call.
- **Used**: Yes.
- **Python Mapping**: Standard positional arguments to a Python function.

#### `DWDate_Datum_LE`
- **Name**: `datum1` (`$1`), `datum2` (`$2`)
- **Source**: Passed as arguments to the function call.
- **Used**: Yes.
- **Python Mapping**: Standard positional arguments to a Python function.

#### `DWDate_Gib_Zeitraum`
- **Name**: `Offset` (`$1`), `Stufe` (`$2`), `Format` (`$3`), `Var_Start` (`$4`), `Var_Ende` (`$5`)
- **Source**: Passed as arguments to the function call.
- **Used**: Yes.
- **Python Mapping**: Standard positional arguments to a Python function.

#### `LetzterTagDesMonats`
- **Name**: `datum` (`$1`)
- **Source**: Passed as argument.
- **Used**: Yes.
- **Python Mapping**: Standard positional argument.

#### `TageimMonat`
- **Name**: `Jahr` (`$1`), `Monat` (`$2`)
- **Source**: Passed as arguments.
- **Used**: Yes.
- **Python Mapping**: Standard positional arguments.

#### `AddiereDatum`
- **Name**: `datum` (`$1`), `tage` (`$2`)
- **Source**: Passed as arguments.
- **Used**: Yes.
- **Python Mapping**: Standard positional arguments.

#### Global Environment Variables Referenced:
- `DW_ORAUSER`: DB-connection-style parameter (Oracle database user).
- `DW_DIR_ROOT`: Root path of the data warehouse scripts.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **Command 1**: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql $DWDate_tmpFile $DWDate_FMT </dev/null`
  - **Purpose**: Computes the previous month based on format `$DWDate_FMT` and writes it to `$DWDate_tmpFile`.
  - **Mapping**: External SQL execution. In modern Python, this can be implemented using native `datetime` calculations (e.g., subtracting a month via `relativedelta` or basic timedelta math) to avoid database roundtrips entirely. However, to preserve the exact interface if database state is required, we can execute the SQL file using a Python DB driver (e.g. `oracledb`).
  - **Status**: Non-resolvable launcher.
    - `# REVIEW-STRUCT: SQL file [d_alis_vormonat.sql] not supplied — behavior of calculation must be confirmed; native Python equivalent recommended instead`

- **Command 2**: `sqlplus -s` (with inline PL/SQL and SQL checks)
  - **Purpose**: Validates date format or checks if `datum1 <= datum2`.
  - **Mapping**: Recommended to replace with native Python `datetime.strptime()` parsing, which naturally throws exceptions if format or constraints are violated.

- **Command 3**: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql $tmpFile $Offset $Stufe $Format </dev/null`
  - **Purpose**: Retrieves a specific start and end period using a database-driven script.
  - **Mapping**: Recommended to replace with native Python date arithmetic based on offset unit ('Y', 'M', 'D') using standard Python tools (e.g., `dateutil.relativedelta`).
  - **Status**: Non-resolvable launcher.
    - `# REVIEW-STRUCT: SQL file [d_alis_datum_zeitraum.sql] not supplied — behavior must be confirmed; native Python equivalent recommended instead`

### 5. EMBEDDED SQL
- **Source**: Inline Heredoc within function `DWDate_Datum_Check`
  ```sql
  select to_date('$wert','$format') from dual;
  ```
  - **Statement Type**: SELECT
  - **Tables Touched**: `dual`
  - **Dialect**: Oracle SQL*Plus (indicated by `to_date`, `dual`).

- **Source**: Inline PL/SQL Heredoc within function `DWDate_Datum_LE`
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
  - **Statement Type**: PL/SQL block
  - **Tables Touched**: none
  - **Dialect**: Oracle PL/SQL (indicated by `DECLARE`, `BEGIN`, `raise_application_error`).
  - **Target platform check**: `# REVIEW: target database platform not specified; DB-client library choice below is provisional`
  - **PL/SQL check**: Oracle anonymous PL/SQL blocks can be executed natively via a Python Oracle driver (e.g., `python-oracledb`).

### 6. CONTROL FLOW
1. **Script Sourcing / Definition**: Defines the shell functions in the current environment context.
2. **`DWDate_Vormonat`**:
   - Creates a temporary file path `/tmp/h_alis_date_...`
   - Executes SQL*Plus with `d_alis_vormonat.sql`.
   - Reads the generated date from the temp file.
   - Evaluates to assign it to the dynamically specified variable name.
   - Cleans up the temporary file. *Note: Original script has a bug: `rm -f $DWDate_FMT` instead of `rm -f $DWDate_tmpFile`.*
3. **`DWDate_Datum_Check`**:
   - Asserts exactly 2 positional arguments.
   - Executes SQL*Plus selecting `to_date($wert, $format)` from dual.
   - Propagates status code ($? = 0 on success, >0 on failure).
4. **`DWDate_Datum_LE`**:
   - Asserts exactly 2 positional arguments.
   - Executes PL/SQL block checking `datum1 <= datum2`.
   - Raises application error `-20422` if assertion fails.
   - Propagates status code.
5. **`DWDate_Gib_Zeitraum`**:
   - Asserts exactly 5 positional arguments.
   - Generates unique temporary file name.
   - Executes SQL*Plus with `d_alis_datum_zeitraum.sql`.
   - Checks if results containing `DWH_Ergebnis;` occur exactly once. If not, returns `1`.
   - Extracts start and end dates via `grep` and `cut`.
   - Dynamically assigns output variables.
   - Cleans up temporary file.
6. **`LetzterTagDesMonats`**:
   - Splits input string (YYYYMMDD) into Year, Month, Tag.
   - Performs shell-level modulo arithmetic to determine leap year.
   - Validates if the given day is the last day of that month.
   - Returns 0 if true, 1 if false.
7. **`TageimMonat`**:
   - Identifies if year is leap.
   - Outputs the total number of days for the given month.
8. **`AddiereDatum`**:
   - Splits input string (YYYYMMDD).
   - Adds the offset of days directly to the Day.
   - Runs nested `while` loops to adjust month overflows (based on `TageimMonat`) and year overflows (if month > 12).
   - Reformats variables with padding.
   - Prints string representation of computed date.

### 7. ERROR HANDLING & EXIT CODES
- Parameter counts are checked directly using `if [ $# -ne X ]` and return `1` on failure.
- Database commands utilize `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` to bubble errors.
- Custom exceptions are raised in database code via `raise_application_error`.
- Error outputs are written to stdout or stderr.
- **Python Mapping**:
  - Convert parameter validation to native Python assertions (`ValueError`).
  - Convert file IO operations to pythonic context managers (`with open()`) with cleanup in `finally` blocks.
  - Map database execution exceptions to `oracledb.DatabaseError` blocks.
  - Map native Python date parsing errors to standard pythonic exceptions.

### 8. OUTPUTS / SIDE EFFECTS
- Returns outputs to calling scripts by setting environment variables using `eval`.
- Modifies filesystem by creating and removing temporary files in `/tmp/`.
- Prints warnings and computed dates to standard output.

### 9. BUSINESS SUMMARY
- Provides standard date validation routines for the ALIS data warehouse platform.
- Performs leap-year aware calculations, ensuring correct date ranges and month lengths.
- Centralizes period generation and previous-month retrieval logic.
- Eliminates manual date arithmetic complications in calling shell scripts.

---

### PSEUDOCODE OUTLINE (PYTHON)

```python
import os
import sys
import tempfile
import datetime
from dateutil.relativedelta import relativedelta  # Used for standard interval arithmetic

# Helper to load DB connection configuration
# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
DB_USER = os.environ.get("DW_ORAUSER")
DIR_ROOT = os.environ.get("DW_DIR_ROOT")


def get_previous_month(fmt: str) -> str:
    """
    Python equivalent of DWDate_Vormonat.
    Calculates previous month from current date natively to avoid SQLPlus dependency.
    """
    # Step 1: Compute previous month
    prev_month = datetime.date.today() - relativedelta(months=1)
    
    # Step 2: Format outcome to match specified Oracle format style
    # Format mapping for common formats
    py_fmt = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    return prev_month.strftime(py_fmt)


def check_date_validity(wert: str, fmt: str) -> bool:
    """
    Python equivalent of DWDate_Datum_Check.
    Validates if 'wert' is a valid date according to format 'fmt'.
    """
    # Step 1: Parameter check
    if not wert or not fmt:
        raise ValueError("Exactly two parameters required: date value and format.")

    # Step 2: Translate Oracle date formats to Python formats
    py_fmt = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    
    # Step 3: Parse date natively
    try:
        datetime.datetime.strptime(wert, py_fmt)
        return True
    except ValueError:
        return False


def assert_date_less_or_equal(datum1_str: str, datum2_str: str) -> bool:
    """
    Python equivalent of DWDate_Datum_LE.
    Ensures datum1 is <= datum2 (both strings in YYYYMMDD format).
    """
    # Step 1: Parameter validation
    if not datum1_str or not datum2_str:
        raise ValueError("Exactly two parameters required.")

    # Step 2: Native comparison
    try:
        datum1 = datetime.datetime.strptime(datum1_str, "%Y%m%d")
        datum2 = datetime.datetime.strptime(datum2_str, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYYMMDD: {e}")

    # Step 3: Assertion comparison
    if datum1 > datum2:
        # Mimic Oracle raise_application_error (-20422)
        raise ValueError(f"Application Error -20422: Datum {datum1_str} ist groesser als {datum2_str}")
    return True


def get_date_period(offset: int, unit: str, fmt: str) -> tuple:
    """
    Python equivalent of DWDate_Gib_Zeitraum.
    Calculates dynamic start and end periods using Python's relativedelta.
    """
    # Step 1: Parameter validation
    if offset is None or not unit or not fmt:
        raise ValueError("All parameters (offset, unit, format) must be specified.")

    start_date = datetime.date.today()
    unit = unit.upper()

    # Step 2: Calculate date range natively
    if unit == 'D':
        end_date = start_date + relativedelta(days=offset)
    elif unit == 'M':
        # Align start to first of current month, end to end of target month
        start_date = start_date.replace(day=1)
        target_month = start_date + relativedelta(months=offset)
        end_date = target_month + relativedelta(day=31)  # day=31 aligns to last day of month
    elif unit == 'Y':
        # Align start to first day of year, end to last day of target year
        start_date = start_date.replace(month=1, day=1)
        target_year = start_date + relativedelta(years=offset)
        end_date = target_year.replace(month=12, day=31)
    else:
        raise ValueError(f"Invalid offset unit: {unit}. Must be 'Y', 'M', or 'D'.")

    # Step 3: Format output
    py_fmt = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    return start_date.strftime(py_fmt), end_date.strftime(py_fmt)


def is_last_day_of_month(datum_str: str) -> bool:
    """
    Python equivalent of LetzterTagDesMonats.
    """
    # Step 1: Parse input
    try:
        dt = datetime.datetime.strptime(datum_str, "%Y%m%d").date()
    except ValueError:
        return False
        
    # Step 2: Compare next day's month
    next_day = dt + datetime.timedelta(days=1)
    return next_day.month != dt.month


def get_days_in_month(year: int, month: int) -> int:
    """
    Python equivalent of TageimMonat.
    """
    # Step 1: Handle year/month boundary checks using standard calendar calculation
    import calendar
    return calendar.monthrange(year, month)[1]


def add_days_to_date(datum_str: str, days_to_add: int) -> str:
    """
    Python equivalent of AddiereDatum.
    """
    # Step 1: Direct date parsing, arithmetic and output
    dt = datetime.datetime.strptime(datum_str, "%Y%m%d").date()
    result_date = dt + datetime.timedelta(days=days_to_add)
    return result_date.strftime("%Y%m%d")
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py` | Reimplemented as a Python utility module containing equivalent date arithmetic and validation functions to be imported by Composer DAGs and Python Operators. |

---

### Job Dependencies

The following downstream jobs consume this utility script as an included/shared module:
*   `DW.BERT_ABLAUFSTEUERUNG`
*   `DW.BERT_AUSD_BP_TA_MSISDN`
*   `DW.BERT_AUSD_BP_TA_P_BASISPROD`
*   `DW.BERT_AUSD_V_TA_PERIOD`
*   `DW.BERT_AUSD_V_TA_P_VERTRAG`
*   `DW.BERT_AUSD_V_TA_VERTRAG_TMP`
*   `DW.BERT_DROP_TEMP_TABLE`
*   `DW.BERT_P_ADRESSEN`
*   `DW.BERT_P_AUSTAUSCH`
*   `DW.BERT_P_GESCHAEFTSP`
*   `DW.BERT_P_RECH_EMPF`
*   `DW.BERT_RECHNUNGSDATEN`
*   `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`
*   `DW.DWH_VVTN_IAR_BGF_GUTSCHR`

**Target Platform Wiring:**
Since all downstream jobs are currently **not yet migrated**, their runtime dependency wiring cannot be finalized. Once migrated, they will import the converted Python utility module (`h_alis_date.py`) directly into their Airflow DAG tasks or Python Operators to compute required execution date parameters.

---

### Schedule & Variables

*   **Schedule:** This utility library is a shared helper module and is not directly triggered by any scheduler. It must remain a callable/importable unit on GCP and should not receive its own standalone Airflow schedule.
*   **Variables:** The script relies on legacy environment-sourced variables (`DW_ORAUSER`, `DW_DIR_ROOT`) to locate SQL dependencies and authenticate to Oracle. In the target environment, these variables will be mapped to modern, secure configurations.

---

### Lineage

*   **Legacy Read:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` $\rightarrow$ reads table `DUAL`.
*   **GCP Target:** BigQuery does not require a dummy `DUAL` table to compute expressions or run standalone `SELECT` calculations (e.g. `SELECT CURRENT_DATE()`). In Python, native calculations are executed locally via standard library functions, completely removing any lineage to `DUAL`.

---

### External System Replacements

*   **Oracle SQL\*Plus:** Remapped to native Python runtime date logic. The legacy execution of SQL queries and PL/SQL blocks via SQL\*Plus is replaced by standard Python datetime functions (`datetime`, `calendar`, `dateutil`), eliminating unnecessary database round-trips and dependency on database-side date functions.

---

### Cross-File Dependencies

*   **`d_alis_vormonat.sql`**: Sourced inside `DWDate_Vormonat` (located at `$DW_DIR_ROOT/allgemein/is/util/sql/`). This file is not in the source file list. The logic is replaced by Python’s native date subtraction arithmetic.
*   **`d_alis_datum_zeitraum.sql`**: Sourced inside `DWDate_Gib_Zeitraum` (located at `$DW_DIR_ROOT/allgemein/is/util/sql/`). This file is not in the source file list. The logic is replaced by standard date interval offsets implemented in Python.

---

### Target File Plan

*   **Target File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py`
    *   **Language:** Python
    *   **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
    *   **Purpose:** Provides a centralized, importable utility package for date calculations, date checks, period generation, and date comparisons across migrated workflows in Cloud Composer.

---

### Environment-Specific Values

1.  **`DW_ORAUSER`** (Oracle Database User)
    *   **Role Classification:** GLOBAL (environment-wide). Identifies legacy infrastructure.
    *   **Target Resolution:** Under BigQuery / Cloud Composer, database connection settings and project credentials are saved within Cloud Secret Manager or Airflow Connections. Resolved dynamically at runtime:
        ```python
        GCP_PROJECT = os.environ.get("GCP_PROJECT")
        ```
2.  **`DW_DIR_ROOT`** (Source Root Directory)
    *   **Role Classification:** GLOBAL (environment-wide). Used to locate other warehouse modules.
    *   **Target Resolution:** In Composer, all shared modules reside in the Python search path (e.g., standard Python packaging, `/home/airflow/gcs/dags/` or a custom plugins folder). Resolved using Airflow configuration or standard Python relative imports:
        ```python
        import os
        from airflow.models import Variable
        DAGS_FOLDER = Variable.get("DAGS_FOLDER", default_var="/home/airflow/gcs/dags")
        ```

---

### Risks and Manual Steps

1.  **Unmigrated Downstream Consumers:** All 14 downstream jobs referencing this utility module are currently "not yet migrated". Downstream integration and end-to-end orchestration tests cannot be finalized until those jobs are ready.
2.  **External Unresolved SQL Dependencies:** `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` are executed via SQL\*Plus but are not part of this job's file scope. While native Python date logic mathematically achieves the same results, a manual validation of these SQL contents is recommended to confirm they don't contain atypical custom logic (such as holiday calendars or non-standard fiscal definitions).
3.  **German Literal Messages:** Per the Output/Print Literal Rule, several error strings and console outputs must be maintained exactly in German, character-for-character, when throwing exceptions or writing logger calls in the target Python module:
    *   `"!! Interner Fehler bei der Rueckgabe von Datumswerten"`
    *   `"   Funktion: DWDate_Gib_Zeitraum"`
    *   `"   1 Zeile erwartet, $anzahl Zeile(n) bekommen"`
    *   `'Datum $datum1 ist groesser als $datum2'` (with dynamic variables interpolated using Python string formatting).
4.  **Missing `.dw_init` Environment Context:** Sourcing of the environment initialization script `.dw_init` is referenced in the legacy comments but not supplied in this scope. Any further variables set by that initialization script must be reviewed once downstream jobs are available.

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
REASON: The script is a library of KornShell helper functions for parameter validation, code translation, and date calculations that cannot be represented as pure SQL.

EVIDENCE
- Business logic found: KSH custom logic contains 12 utility functions validating ETL parameters, converting long names to standardized codes, and calculating execution dates.
- AWK: none
- SQL-expressible: no, the logic consists of shell control flow, string manipulation, system variable verification, and external date utility calls.
- Non-SQL side effects: Writes and cleans up temporary files under `/tmp`, and modifies environment variable states.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_parameter.ksh`) is a legacy KornShell utility module containing helper functions used across the Information System (IS) reporting platform. Its primary purpose is to parse, validate, and normalize parameters (such as source systems, metrics, and date ranges) passed to ETL processes. It acts as a safety layer that enforces metadata conventions, maps long business names to standardized shortcodes, and performs date-range validations.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced (via `. h_alis_parameter.ksh`) by various main ETL shell scripts within the `isrpt` environment. It is not directly invoked by UC4 as a standalone job, but serves as a dependency.
   - UC4 native includes: None.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   The script does not accept command-line arguments directly. Instead, it defines functions that operate on environment variables or variables passed as references (via string names, which are evaluated dynamically using `eval`).
   
   Global Control Variables:
   - `ErrNr` (numeric error state; used as a guard clause across all functions)
   - `ErrArg` (string context for errors)
   - `ModulName` (hardcoded as `"alis_parameter"`)
   - `ModulVersion` (hardcoded as `"V3.0.9"`)

   Function Parameters (defined inside functions):
   - `param_name` / `param_var` (used in `pruefeParameterGesetzt` to dynamically inspect env variables)
   - `VarName` / `Kennzahl` / `System` / `Stufe` (dynamic variables updated in-place via `eval`)
   - `Anfang` / `Ende` (date bounds formatted as YYYYMMDD)
   - `p_Zahl` / `p_ParameterName` (numeric checks)
   - `p_Anfangsdatum` / `p_Endedatum` / `p_ZeitOffset` (range configuration parameters)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWDate_Datum_Check`: Utility to check if a string matches a specified date format.
     - # REVIEW-STRUCT: launcher [DWDate_Datum_Check] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `DWDate_Datum_LE`: Utility to check if one date is less than or equal to another.
     - # REVIEW-STRUCT: launcher [DWDate_Datum_LE] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `DWDate_Gib_Zeitraum`: Utility to calculate a date range based on an offset and unit.
     - # REVIEW-STRUCT: launcher [DWDate_Gib_Zeitraum] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `date`: System utility to obtain the current timestamp (used to construct temporary filenames).
   - `basename`: Standard path parser.

5. EMBEDDED SQL
   - None.

6. CONTROL FLOW
   Since this is a library file, the top-level execution only defines module-level variables. The core logic consists of individual functions:
   
   1. **Module Initialization**: Sets `ModulName` and `ModulVersion`.
   2. **`pruefeParameterGesetzt`**:
      - Guards against existing errors (`ErrNr != 0`).
      - Validates that parameters are passed.
      - Uses `eval` to check if the variable named by `$param_var` is empty, setting `ErrNr=194` if it is.
   3. **`konvertiereKennzahl`**:
      - Maps German business descriptions to 3-letter codes (e.g., "zugang" -> "zug", "abgang" -> "abg").
      - Updates the variable dynamically in-place via `eval`. Sets `ErrNr=198` for unknown mappings.
   4. **`konvertiereSystem`**:
      - Normalizes source systems (e.g., "sap", "carmen", "dpps") to lowercase.
      - Sets `ErrNr=195` for unknown systems.
   5. **`konvertiereSDName`**:
      - Maps Master Data (Stammdaten) source names to codes (e.g., "tarif" -> "trf").
      - Sets `ErrNr=195` for unknown names.
   6. **`konvertiereAufbStufeXtra`**:
      - Normalizes stage names for Xtra (e.g., "zusammenfuehrung" -> "mrg").
   7. **`pruefeSystemKennzahl`**:
      - Enforces strict business validation rules regarding which combinations of systems and metrics are allowed (e.g., "carmen" is invalid with "twe", "pln", "rst").
      - Sets `ErrNr=195` if an invalid combination is encountered.
   8. **`gibBereich`**:
      - Categorizes a metric code into a business area (`tn`, `us`, `gd`, `sd`, `md`) by searching static lists.
   9. **`gibIntervall`**:
      - Categorizes a metric into a time granularity (`t` for daily, `m` for monthly).
   10. **`pruefeZeitraum`**:
       - Temporarily disables script termination (`set +e`).
       - Validates format of start and end dates (YYYYMMDD) and verifies `Anfang <= Ende` using external `DWDate` binaries, writing outputs to `/tmp`.
   11. **`pruefeZahlPositiv`**:
       - Asserts that a value is numeric and `>= 0`.
   12. **`pruefeZeitParameter`**:
       - Validates that either a relative time span OR explicit start/end dates are provided, but not both.
   13. **`konvertiereZeitspanne`**:
       - Resolves a relative offset (e.g., past N days or months) into concrete `Anfangsdatum` and `Endedatum` using `DWDate_Gib_Zeitraum`.

7. ERROR HANDLING & EXIT CODES
   - State-Based Tracking: The functions do not use `exit` directly. Instead, they write to global state variables: `ErrNr` (integer) and `ErrArg` (string).
   - Cascade Guard: Every function begins with `if [ $ErrNr -ne 0 ]; then return; fi` to prevent executing subsequent steps if a prior failure exists.
   - Conversion to Python: The state-based approach can be modeled cleanly using a `ValidationContext` class or standard Python Exception classes (e.g., raising custom exceptions that are caught by the calling orchestration script).

8. OUTPUTS / SIDE EFFECTS
   - Files written: Temporary files in `/tmp` containing output from external `DWDate` commands, cleaned up before the function exits.
   - State side effects: Modifies environment/global state variables.

9. BUSINESS SUMMARY
   - Normalization: Translates verbose, human-readable German metrics and systems into standard system keys.
   - Consistency Engine: Prevents data corruption by verifying that source systems are only paired with valid metric subsets (e.g., preventing SAP from loading subscriber-level raw metrics).
   - Date Guarding: Standardizes how historical dates and relative sliding-window ranges are computed and validated across the entire IS platform.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
import os
import sys
import datetime
import subprocess
import shutil

# Global Module Variables
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"

# Global Error Context state (replicating the legacy global state contract)
ErrNr = 0
ErrArg = ""

def reset_errors():
    global ErrNr, ErrArg
    ErrNr = 0
    ErrArg = ""

# Step 1: helper to simulate the global guard condition
def _has_error():
    global ErrNr
    return ErrNr != 0

# Step 2: pruefeParameterGesetzt
def pruefeParameterGesetzt(param_name: str, param_var: str):
    global ErrNr, ErrArg
    if _has_error():
        return

    if not param_name or not param_var:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeParameterGesetzt"
        return

    # In Python we check os.environ or a passed configuration map. Replicating environment check.
    param_wert = os.environ.get(param_var, "")

    if not param_wert:
        ErrNr = 194
        ErrArg = param_name

# Step 3: konvertiereKennzahl
def konvertiereKennzahl(kennzahl_val: str) -> str:
    global ErrNr, ErrArg
    if _has_error():
        return kennzahl_val

    if not kennzahl_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereKennzahl"
        return "???"

    kennzahl = kennzahl_val.lower()
    
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
        return mapping[kennzahl]
    else:
        ErrNr = 198
        ErrArg = kennzahl
        return "???"

# Step 4: konvertiereSystem
def konvertiereSystem(system_val: str) -> str:
    global ErrNr, ErrArg
    if _has_error():
        return system_val

    if not system_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSystem"
        return "???"

    system = system_val.lower()
    allowed_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

    if system in allowed_systems:
        return system
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Datenherkunft {system} !"
        return "???"

# Step 5: konvertiereSDName
def konvertiereSDName(system_val: str) -> str:
    global ErrNr, ErrArg
    if _has_error():
        return system_val

    if not system_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSDSystem"
        return "???"

    system = system_val.lower()
    
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
        return mapping[system]
    elif system == "vo":  # Handled in direct check in legacy
        return system
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stammdaten-Datenherkunft {system} !"
        return "???"

# Step 6: konvertiereAufbStufeXtra
def konvertiereAufbStufeXtra(stufe_val: str) -> str:
    global ErrNr, ErrArg
    if _has_error():
        return stufe_val

    if not stufe_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra"
        return "???"

    stufe = stufe_val.lower()
    if stufe == "zusammenfuehrung":
        return "mrg"
    elif stufe == "befuellung":
        return "fill"
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stufenangabe {stufe} !"
        return "???"

# Step 7: pruefeSystemKennzahl
def pruefeSystemKennzahl(system: str, kennzahl: str):
    global ErrNr, ErrArg
    if _has_error():
        return

    if not system or not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeSystemKennzahl"
        return

    # Check combinations
    invalid_combination = False
    
    if system != "nnv" and kennzahl in ["tvd", "lkl"]:
        invalid_combination = True
    elif system == "carmen":
        if kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            invalid_combination = True
    elif system == "sap":
        if kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            invalid_combination = True
    elif system == "dpps":
        if kennzahl in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
            invalid_combination = True
    elif system == "ctel":
        if kennzahl not in ["abg", "bst", "zug", "twe"]:
            invalid_combination = True
    elif system == "xtra":
        if kennzahl != "rst":
            invalid_combination = True
    elif system == "d1":
        if kennzahl in ["gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            invalid_combination = True
    elif system == "nnv":
        if kennzahl not in ["tvd", "lkl"]:
            invalid_combination = True
    elif system == "dwh":
        if kennzahl != "mds":
            invalid_combination = True
    elif system == "brunet":
        if kennzahl not in ["d1n", "rub", "lmo"]:
            invalid_combination = True
    elif system == "sigma":
        allowed_sigma = ["nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"]
        if kennzahl not in allowed_sigma:
            invalid_combination = True

    if invalid_combination:
        ErrArg = f"Ungueltige Kombination {system} {kennzahl}"
        ErrNr = 195

# Step 8: gibBereich
def gibBereich(kennzahl: str) -> str:
    global ErrNr, ErrArg
    if _has_error():
        return ""

    if not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich"
        return ""

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
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich - Kuerzel '{kennzahl}' unbekannt"
        return ""

# Step 9: gibIntervall
def gibIntervall(kennzahl: str) -> str:
    global ErrNr, ErrArg
    if _has_error():
        return ""

    if not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall"
        return ""

    list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
    list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

    if kennzahl in list_t:
        return "t"
    elif kennzahl in list_m:
        return "m"
    else:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
        return ""

# Step 10: pruefeZeitraum
def pruefeZeitraum(anfang: str, ende: str):
    global ErrNr, ErrArg
    if _has_error():
        return

    if not anfang or not ende:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeZeitraum"
        return

    format_mask = "YYYYMMDD"
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    tmp_file = f"/tmp/tmp_h_alis_parameter.py_{timestamp}_{os.getpid()}.tmp"

    err_arg_local = ""

    # REVIEW-STRUCT: DWDate_Datum_Check launcher is used; check path validity
    # Simulating the validation check calls via subprocess
    try:
        with open(tmp_file, "a+") as f:
            for label, val in [("Anfang", anfang), ("Ende", ende)]:
                res = subprocess.run(["DWDate_Datum_Check", val, format_mask], stdout=f, stderr=subprocess.STDOUT)
                if res.returncode != 0:
                    err_arg_local = f"{label}datum entspricht nicht dem Format {format_mask}"

            if not err_arg_local:
                # REVIEW-STRUCT: DWDate_Datum_LE launcher is used; check path validity
                res_le = subprocess.run(["DWDate_Datum_LE", anfang, ende], stdout=f, stderr=subprocess.STDOUT)
                if res_le.returncode != 0:
                    err_arg_local = "Anfangsdatum ist nicht kleiner gleich Endedatum"
                    
        if err_arg_local:
            ErrNr = 195
            ErrArg = err_arg_local
            with open(tmp_file, "r") as f:
                print(f.read(), file=sys.stderr)
    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

# Step 11: pruefeZahlPositiv
def pruefeZahlPositiv(p_Zahl, p_ParameterName: str):
    global ErrNr, ErrArg
    try:
        val = int(p_Zahl)
        if val < 0:
            ErrNr = 195
            ErrArg = f"Parameter {p_ParameterName} muss groesser gleich 0 sein"
    except ValueError:
        ErrNr = 195
        ErrArg = f"Parameter {p_ParameterName} ist kein numerischer Wert"

# Step 12: pruefeZeitParameter
def pruefeZeitParameter(p_Anfangsdatum: str, p_Endedatum: str, p_ZeitOffset: str):
    global ErrNr, ErrArg
    if _has_error():
        return

    # Case 1: Zeitspanne is filled, then start and end date must be empty
    if p_ZeitOffset:
        if not p_Anfangsdatum and not p_Endedatum:
            pruefeZahlPositiv(p_ZeitOffset, "Zeitspanne")
            return
        else:
            ErrNr = 195
            ErrArg = "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden"
            return
    else:
        # Case 2: ZeitOffset is empty, date values must be filled and valid
        if p_Anfangsdatum and p_Endedatum:
            pruefeZeitraum(p_Anfangsdatum, p_Endedatum)
        else:
            ErrNr = 195
            if not p_Anfangsdatum and not p_Endedatum:
                ErrArg = "Datumswerte oder Zeitspanne fehlen"
            else:
                ErrArg = "Sowohl Anfang- als auch Endedatum muessen angegeben werden"
            return

# Step 13: konvertiereZeitspanne
def konvertiereZeitspanne(p_Spanne: str, p_Kennzahl: str) -> tuple:
    global ErrNr, ErrArg
    if _has_error():
        return ("", "")

    offset_unit = "D"
    if p_Kennzahl == "bst":
        offset_unit = "M"

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    tmp_file = f"/tmp/tmp_h_alis_parameter.py_{timestamp}_{os.getpid()}.tmp"
    
    anfangsdatum = ""
    endedatum = ""

    try:
        # Spanne is negative to compute past window
        negative_spanne = f"-{p_Spanne}"
        # REVIEW-STRUCT: DWDate_Gib_Zeitraum launcher is used; check path validity
        with open(tmp_file, "w") as f:
            res = subprocess.run(
                ["DWDate_Gib_Zeitraum", negative_spanne, offset_unit, "YYYYMMDD", "Anfangsdatum", "Endedatum"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            
        if res.returncode != 0:
            ErrNr = 85
            ErrArg = "DWDate_Gib_Zeitraum"
            print(res.stderr, file=sys.stderr)
        else:
            # Parse calculated values from stdout or mock target mechanism.
            # Assuming outputs are written to stdout or parsed back.
            # (In legacy shell, DWDate_Gib_Zeitraum bound variables in parent shell via eval/reference)
            lines = res.stdout.strip().split("\n")
            # Expected parsed results logic mapping:
            # # REVIEW: confirm how DWDate_Gib_Zeitraum exposes output parameters to caller.
            # If standard outputs prints: Anfangsdatum=YYYYMMDD\nEndedatum=YYYYMMDD
            for line in lines:
                if "Anfangsdatum=" in line:
                    anfangsdatum = line.split("=")[1].strip()
                elif "Endedatum=" in line:
                    endedatum = line.split("=")[1].strip()
    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
            
    return (anfangsdatum, endedatum)
```

# Migration Design Document

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` | KornShell utility library converted to a Python module to provide reusable parameter validation, code mapping, and date validation logic. |

---

## Job Dependencies

The following downstream jobs utilize this shared parameter library. Since they are not yet migrated, their import mechanisms and calling conventions cannot be finalized until they are converted to the target environment:

- **Downstream Consumers (not yet migrated)**:
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
  - `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`
  - `DW.DWH_VVTN_IAR_BGF_GUTSCHR`

**Wiring on Target Platform**:
- This utility script will be packaged as a shared Python module.
- Downstream jobs (migrated as Cloud Composer Python operators / DAG files) will import functions from `h_alis_parameter.py` (e.g. `import h_alis_parameter` or packaging it in a shared dependencies library path).

---

## Scheduling

This utility script is a library of helper functions and is **not** directly triggered by any scheduler. 
- It must not be given its own standalone schedule.
- It will remain a callable/importable unit, executed as an in-memory import inside the Python execution context of downstream jobs.

---

## Schedule & Variables

- **Schedule**: None (callable/importable shared library).
- **Variables**: The library does not consume scheduler-defined global environment variables directly. It dynamically processes the variables or parameters passed into its functions. Global state variables such as `ErrNr` (error number) and `ErrArg` (error details) will be managed locally within a `ValidationContext` class or as return signatures in the converted Python implementation.

---

## External System Replacements

The script invokes legacy utility binaries for date checks and calculations:
- `DWDate_Datum_Check`
- `DWDate_Datum_LE`
- `DWDate_Gib_Zeitraum`

**Replacement Strategy**:
- These utility invocations should be replaced with native Python standard libraries (e.g., standard `datetime` module methods) to avoid spawning subprocesses and relying on local shell executions.
- In the event that these binaries contain complex proprietary fiscal calendar definitions or banking holiday maps, they must be packaged and made accessible within the Cloud Composer worker environment.

---

## Cross-File Dependencies

- This utility script is heavily sourced via `. h_alis_parameter.ksh` by downstream scripts. 
- To maintain folder structure integrity and ease import paths, the target Python module will be placed in the mirrored path `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` and included in Python's `sys.path` or installed as part of a common workspace wheel package.

---

## Target File Plan

- **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py`
  - **Language**: Python
  - **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
  - **Purpose**: Defines parameter validation functions (`pruefeParameterGesetzt`, `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `pruefeSystemKennzahl`, `gibBereich`, `gibIntervall`, `konvertiereAufbStufeXtra`, `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, `konvertiereZeitspanne`).

---

## Environment-Specific Values

There are no environment-specific infrastructure variables (such as database connections, project IDs, or bucket names) in this logic-only validation library. All variables parsed are runtime-based.

---

## Risks & Manual Actions

- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_ABLAUFSTEUERUNG` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_AUSD_BP_TA_MSISDN` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_AUSD_BP_TA_P_BASISPROD` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_AUSD_V_TA_PERIOD` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_AUSD_V_TA_P_VERTRAG` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_DROP_TEMP_TABLE` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_P_ADRESSEN` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_P_AUSTAUSCH` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_P_GESCHAEFTSP` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_P_RECH_EMPF` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.BERT_RECHNUNGSDATEN` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` — wiring cannot be finalized until it exists.
- **SOURCE: NOT FOUND — Downstream Dependencies Not Migrated** — `DW.DWH_VVTN_IAR_BGF_GUTSCHR` — wiring cannot be finalized until it exists.
- **EXTERNAL UTILITY RISKS**: The script relies on execution of legacy binaries `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum`. Utilizing subprocess execution introduces performance penalties. It is highly recommended to perform a manual review to replace these with standard Python datetime functions. If they depend on external state/files, those files must be packaged into the Composer execution target.

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
REASON: The script defines a procedural shell utility function with conditional validations, error log integrations, and dynamic external command invocation, which must be modeled as a Python module.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `h_alis_sqlplus.ksh` is a reusable utility module (`alis_sqlplus`) designed to safely execute Oracle SQL*Plus scripts. It defines a single helper function, `starteSQLSkript`, which performs safety checks (parameter validation and file readability checks) before invoking the Oracle SQL*Plus CLI. This utility ensures that the execution of database scripts is consistently logged and that failures are safely detected and propagated to the calling shell environment.

2. INVOCATION CONTEXT
   - **Caller**: This script acts as a library and is sourced (via `. h_alis_sqlplus.ksh`) by other KornShell batch scripts or UC4 jobs within the data warehouse environment.
   - **UC4 Includes**: None referenced directly in this script.
   - **Environment Files Sourced**: None sourced within this script. It assumes that the calling environment has already defined required variables (such as `DW_ORAUSER`) and utility functions (such as `DWMSG_MeldeFehler`).
     # REVIEW-STRUCT: external function DWMSG_MeldeFehler not supplied — behavior and signature inferred from usage.

3. PARAMETERS / INPUTS
   The parameters described below are local to the `starteSQLSkript` function:
   - `p_Eintragsnr` (Function Argument $1): An error tracking/entry ID used for registering failures. Surfaced as a positional function parameter in Python.
   - `p_Skript` (Function Argument $2): The file path of the SQL script to be executed. Checked for readability. Surfaced as a positional function parameter in Python.
   - `*args` (Function Arguments $3 onwards, captured via `shift 2` and `$*`): Arbitrary parameters passed down to the target SQL script. Surfaced in Python as a variable argument list (`*args`).
   - `DW_ORAUSER` (Environment Variable): Holds the connection credentials/string for Oracle SQL*Plus. Surfaced in Python via `os.environ.get("DW_ORAUSER")`.
   - `KSH DECLARED ENVIRONMENT PARAMETERS`: None declared.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - **Command Line**: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - **Purpose**: Executes the specified SQL script via Oracle's SQL*Plus client using the credentials in `DW_ORAUSER`. Input redirection from `/dev/null` is used to prevent the CLI from hanging on interactive prompts.
     - **Python Strategy**: Since the target SQL script is dynamic and determined at runtime, this remains a dynamic subprocess invocation. However, if the calling scripts are migrated to Python, it is highly recommended to replace SQL*Plus CLI calls with a native Oracle driver connection (such as `oracledb`).
     - **Resolvable Launcher**: No. The target script path is dynamic.
     - # REVIEW-STRUCT: launcher [sqlplus] invoked dynamically; verify if target SQL scripts can be run directly via native Python DB API (e.g. `oracledb`) instead of spawning a subprocess.

5. EMBEDDED SQL
   - No inline or static SQL statements are contained in this utility script. All SQL operations are dynamic and reside within the external script file specified by `$p_Skript`.

6. CONTROL FLOW
   1. Initialize module-level constants `ModulName` ("alis_sqlplus") and `ModulVersion` ("V1.1.3").
   2. Define the function `starteSQLSkript`.
   3. **Inside `starteSQLSkript`**:
      - Extract and shift positional arguments.
      - Validate that both `p_Eintragsnr` and `p_Skript` are provided. If either is empty, call `DWMSG_MeldeFehler` with code `196` and return `196`.
      - Check if the target script file `p_Skript` is readable. If not, call `DWMSG_MeldeFehler` with code `201` and return `201`.
      - Log execution settings (script path and parameters) to stdout.
      - Disable shell fail-on-error behavior (`set +e`).
      - Invoke `sqlplus` with the credential string, script path, and additional arguments. Standard input is redirected from `/dev/null`.
      - Capture the SQL*Plus return code (`$?`).
      - Restore shell fail-on-error behavior (`set -e`).
      - Return the captured exit code.

7. ERROR HANDLING & EXIT CODES
   - **Missing Arguments**: Triggers error `196` via `DWMSG_MeldeFehler` and returns `196`.
   - **Unreadable Script File**: Triggers error `201` via `DWMSG_MeldeFehler` and returns `201`.
   - **SQL*Plus Execution Failures**: Captured via `errcode=$?` under `set +e` to prevent the wrapper script itself from crashing, allowing graceful propagation of the exit code back to the caller.
   - **Python Mapping**: Map to explicit return values or raise standard/custom Python exceptions. `subprocess.run(..., check=False)` will be used to replicate the `set +e` behavior, capturing and returning the `returncode`.

8. OUTPUTS / SIDE EFFECTS
   - Standard output and standard error from the SQL*Plus CLI execution are printed to the console (propagated to stdout/stderr).
   - Side effects may occur in the database depending on the contents of the SQL script executed.
   - External error logging side effects via `DWMSG_MeldeFehler`.

9. BUSINESS SUMMARY
   - Standardizes Oracle SQL*Plus script execution across the data warehouse batch environment.
   - Prevents execution attempts of missing or unreadable files, avoiding useless process initialization and ensuring clean logging.
   - Enforces uniform error reporting for missing arguments and missing execution targets.
   - Facilitates clean integration with orchestration agents (like UC4) by capturing and returning the exact database script exit codes.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess

# Module constants
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# REVIEW-STRUCT: external function DWMSG_MeldeFehler body not supplied — placeholder implemented
def dwmsg_melde_fehler(eintrags_nr: str, severity: str, error_code: int, details: str) -> None:
    """
    Placeholder for the external DWMSG_MeldeFehler error logging system.
    """
    print(f"ERROR LOG: [{severity}] Code {error_code} - {details} (Entry ID: {eintrags_nr})", file=sys.stderr)


def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
    """
    Validates and executes an external SQL*Plus script.
    """
    # Step 1: Validate required parameters
    if not p_eintragsnr or not p_skript:
        details = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, details)
        return 196

    # Step 2: Validate file readability
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 3: Log execution metadata
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 4: Retrieve environment connection details
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    
    # Step 5: Execute SQL*Plus (replicates set +e safety)
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
    
    try:
        # stdin=subprocess.DEVNULL mimics </dev/null
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            text=True,
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Exception raised during sqlplus execution: {e}", file=sys.stderr)
        errcode = -1

    # Step 6: Return the execution exit status
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Migrates the shell utility module for SQL script validation and execution into a Python utility module to support calling tasks on Cloud Composer. |

---

### Downstream Job Dependencies
The following downstream jobs consume this shared utility (directly or indirectly via sourcing/inclusion). Because this is a library module and is not independently scheduled, it has no standalone DAG schedule. Instead, it must be deployed in the Cloud Composer environment to be importable by these downstream tasks once they are migrated:
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
* `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` — *not yet migrated*
* `DW.DWH_VVTN_IAR_BGF_GUTSCHR` — *not yet migrated*

**Wiring on BigQuery/Cloud Composer**:
Downstream tasks, once migrated to Cloud Composer, will import `starte_sql_skript` from `h_alis_sqlplus.py` as a Python module or call it as a task-level helper, replacing the legacy `. h_alis_sqlplus.ksh` sourcing pattern. Since all upstreams/downstreams are marked "not yet migrated", the final end-to-end DAG task flow cannot be fully wired until those DAGs are generated.

---

### Scheduling
This job is **NOT** directly triggered by any of the run's schedulers. It acts as an include/shared module within other batch processes. Do not give the migrated Python module its own standalone Airflow DAG or trigger. Instead, distribute it inside the Cloud Composer environment's `PYTHONPATH` (such as the `/dags` or `/plugins` folder, mirroring the legacy folder structure) to ensure it is discoverable by downstream job DAGs.

---

### External System Replacements
* **Oracle SQL\*Plus to BigQuery Native Execution**: The legacy KornShell utility wraps Oracle's `sqlplus` command-line tool. On the BigQuery target platform, database interactions should be handled natively via the Google Cloud BigQuery client library (`google.cloud.bigquery`) or Airflow's native operator (`BigQueryInsertJobOperator`) rather than spawning shell subprocesses executing SQL client commands.
* **Credentials Management (`DW_ORAUSER`)**: Database access credentials should be replaced by native Google Cloud Service Account IAM permissions or Cloud Composer Airflow Connections.

---

### Target File Plan
* **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py`
  * **Language**: Python
  * **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`

---

### Environment-Specific Values
* **`DW_ORAUSER`**: **GLOBAL**. In the legacy environment, this represents the database connection credentials. For BigQuery/Cloud Composer, this environment-wide configuration must map to standard Airflow connections or be dynamically fetched using Airflow's variables (`Variable.get("GCP_PROJECT")` / standard GCP connection mechanisms) instead of literal shell credentials.

---

### Risks & Manual Steps
* **External Logging Dependency (`DWMSG_MeldeFehler`)**: The utility invokes an external logging routine `DWMSG_MeldeFehler` which is defined in a separate shared logging library not part of this specific job scope. To prevent runtime errors, a common Python logging utility or custom wrapper routing to Google Cloud Logging must be deployed as a dependency on Cloud Composer.
* **Subprocess Execution Risk**: If direct database lift-and-shift via Oracle client commands is still required for certain interim tasks, executing `subprocess.run(["sqlplus", ...])` will require installing the Oracle Instant Client and configuring Oracle connection descriptors inside the Cloud Composer worker containers. Translating these tasks to native BigQuery Python operators is highly recommended to eliminate this dependency.
* **Literal Output Preservation**: In accordance with the Output/Print Literal Rule, all standard output strings from the original script (e.g., `"Rufe SQL*PLUS auf mit folgenden Einstellungen"`, `"Sql*Plus-Skript : "`, `"Skript-Parameter: "`) must be printed exactly as they are in the German source code without translation or modification.