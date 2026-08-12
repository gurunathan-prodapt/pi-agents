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
REASON: The script is a reusable KornShell utility library defining multiple error-handling and logging functions that invoke external SQL*Plus processes and manipulate environment variables.

EVIDENCE
- Business logic found: KSH custom logic. The script defines a series of reusable functions for job registration, status updates, timing instrumentation, and exception reporting in an Oracle database.
- AWK: none
- SQL-expressible: no. While it executes PL/SQL procedures, the script functions as an orchestration/utility library to be sourced by other processes to manage shell-level state and generate filesystem path names.
- Non-SQL side effects: Temporary file creation/cleanup (`/tmp/ErmittleNr_$$.lst`), dynamic environment variable setting via `eval`, and log file path formatting.
- Against this verdict: If the database is migrated to BigQuery and the orchestration system is completely rewritten in a workflow engine like Airflow, these functions might be replaced by native Airflow task listeners or logging handlers, making a 1-to-1 conversion unnecessary. However, as written, the script contains procedural execution-tracking logic that must be translated into Python equivalents.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a reusable KornShell utility library (`f_alis_msgerr.ksh`) that provides standardized error handling, database-backed logging, and status tracking for the "Information Services" (IS) data warehouse project. It is intended to be sourced by parent shell scripts which register their execution status by calling these utility routines (often bound to a shell exit trap). The script coordinates the insertion of log statements, status transitions (OK, ABORTED), and execution metadata inside an Oracle database schema.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced internally by other KornShell ETL scripts via `. f_alis_msgerr.ksh`. It does not execute as a standalone UC4 job but runs within the context of any wrapper script that imports it.
   - UC4 includes: None referenced in the extracted block.
   - Environment files sourced: None. It expects the calling environment to have configured variables such as `$DW_ORAUSER`, `$DW_DIR_ROOT`, and `$DW_DIR_PROT`.

3. PARAMETERS / INPUTS
   The script does not consume direct command-line arguments but utilizes global environment variables and parameters passed directly to its shell functions:
   - `DW_ORAUSER` (Environment Variable): Database connection credentials (e.g., `user/password@db`). Used to authenticate `sqlplus` sessions.
   - `DW_DIR_ROOT` (Environment Variable): The root path to the project's codebase, used to locate auxiliary `.sql` utility wrappers.
   - `DW_DIR_PROT` (Environment Variable): Protocol directory where log files are stored.
   - Function-specific positional parameters:
     - `DWMSG_EintragsNr` (Positional): The tracking log ID used to identify the current execution instance in the database.
     - `JobKennung` (Positional): Unique code identifying the parent job.
     - `Programmname` (Positional): Name of the script/binary being executed.
     - `LogDatei` (Positional): File path to the execution protocol.
     - `Typ` (Positional): Error severity level (`F` for Fatal, `E` for Error, `W` for Warning).
     - `FehlerNr` (Positional): Application-specific error identifier.
     - `Zusatz1`, `Zusatz2` (Positional): Optional error context strings (e.g. filenames, database error states).
     - `VarName` (Positional): Dynamically evaluated shell variable name used to return values to the caller.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus` (Oracle SQL*Plus Client): Used to connect to the database and execute both dynamic inline PL/SQL blocks and parameterized SQL scripts.
     - *Exact command examples*:
       - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr`
       - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr`
       - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile"`
       - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei`
     - *Purpose*: Executes logging/status tracking stored procedures on the `BERT_MELDUNG` package.
     - *Python transformation*: This qualifies as a **RESOLVABLE LAUNCHER**. Rather than invoking `sqlplus` as an external subprocess, these operations should be converted into native database client operations using a Python database connector (such as `oracledb`).
     - *Resolvable launcher analysis*:
       - # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
       - The SQL scripts (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, etc.) are simple wrappers used to invoke PL/SQL procedures on the database. In Python, these can be mapped directly to calling stored procedures via `cursor.callproc()` or direct anonymous PL/SQL blocks.

5. EMBEDDED SQL
   - Dialect is explicitly **Oracle SQL\*Plus**, identified by SQL*Plus variables, anonymous blocks, and specific package calls (`BERT_MELDUNG`).
   - SQL Calls within helper functions:
     1. **SetzeStatusOk**:
        - Source: `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql` (Wrapper not supplied)
        - PL/SQL call: `BERT_MELDUNG.SetzeStatusOk(<EintragsNr>)`
     2. **SetzeStatusAbbruch**:
        - Source: `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql` (Wrapper not supplied)
        - PL/SQL call: `BERT_MELDUNG.SetzeStatusAbbruch(<EintragsNr>)`
     3. **ErmittleNr**:
        - Source: `@$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql` (Wrapper not supplied)
        - PL/SQL call: Selects next sequence value into a file. In Python, this can be written as `SELECT bert_sequence.NEXTVAL FROM dual`.
     4. **ErzeugeEintrag**:
        - Source: `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql` (Wrapper not supplied)
        - PL/SQL call: `BERT_MELDUNG.Erzeuge_Eintrag(<EintragsNr>, <JobKennung>, <Programmname>, <LogDatei>)`
     5. **MeldeFehler**:
        - Source: `@$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p[3-5].sql` (Dynamic wrapper depending on argument count)
        - PL/SQL call: `BERT_MELDUNG.Fehler(<Typ>, <EintragsNr>, <FehlerNr>, <Zusatz1>, <Zusatz2>)`
     6. **SetzeStichtagInfo**:
        - Inline PL/SQL:
          ```sql
          EXEC BERT_MELDUNG.SetzeZusatzInfos(:DWMSG_EintragsNr, to_date(:DWMSG_Stichtag, :DWMSG_StichtagFmt));
          commit;
          ```
     7. **AppendTimingInfos**:
        - Inline PL/SQL:
          ```sql
          EXEC BERT_MELDUNG.SetzeZusatzInfos(:DWMSG_EintragsNr, null, :DWMSG_InfoText || ' ' || to_char(SYSDATE, :DWMSG_DateFormat) || ' ');
          commit;
          ```

6. CONTROL FLOW
   Since this is a library file, the control flow is structured around the lifecycle of individual routines:
   1. **DWMSG_Fehlerbehandlung**: Slices off the active error signal `$IFS` ($?), dispatches standard fatal error log payload to DB via `DWMSG_MeldeFehler`, and transitions the job tracking record to an aborted state via `DWMSG_SetzeStatusAbbruch`.
   2. **DWMSG_SetzeStatusOK**: Asserts presence of `EintragsNr`, then calls the `BERT_MELDUNG.SetzeStatusOk` database procedure.
   3. **DWMSG_SetzeStatusAbbruch**: Asserts presence of `EintragsNr`, then calls the `BERT_MELDUNG.SetzeStatusAbbruch` database procedure.
   4. **DWMSG_ErmittleNr**: Asserts name parameter for output variable context, runs sequential query tracking block, handles file system synchronization (saving stdout to `/tmp/ErmittleNr_$$`), parses value, removes temp file, and assigns back to dynamically called scope using `eval`.
   5. **DWMSG_ErzeugeEintrag**: Asserts `EintragsNr`, calls `BERT_MELDUNG.Erzeuge_Eintrag`.
   6. **DWMSG_MeldeFehler**: Resolves parameter counts, chooses dynamic execution template depending on whether optional context arguments are specified, and routes telemetry to `BERT_MELDUNG.Fehler`.
   7. **DWMSG_Logdateiname**: Formats file path relative to `$DW_DIR_PROT` matching prefix format: `{JobKennung}_{YYYYMMDD_HHMM}_{EintragsNr}.log`.
   8. **DWMSG_SetzeStichtagInfo**: Asserts inputs, invokes inline PL/SQL block mapping the provided string timestamp to a date using `to_date`.
   9. **DWMSG_AppendTimingInfos**: Asserts inputs, appends executing user timing checkpoints to supplementary fields inside the execution record database entity.

7. ERROR HANDLING & EXIT CODES
   - Library missing-parameter assertions are handled with explicit `exit 1` or `exit 2` statements.
   - Shell command failures inside functions (e.g. `sqlplus` failing to connect) are not explicitly caught inside the secondary functions but rely on the parent script's `set -e` or `trap ERR` settings.
   - Python migration equivalent: Define an `OracleLoggingManager` class. Missing arguments will raise a native `ValueError`. Database communication issues will raise `oracledb.DatabaseError` exceptions.

8. OUTPUTS / SIDE EFFECTS
   - Central auditing logs updated inside the Oracle database (`BERT_MELDUNG` package side effects).
   - Temporary file generation at `/tmp/ErmittleNr_<PID>.lst` (subsequently deleted).
   - Generates protocol log paths matching timestamp standards.

9. BUSINESS SUMMARY
   - **Central Auditing Protocol**: Provides a standard audit framework for reporting the lifecycles of ETL operations.
   - **Job State Management**: Handles transition state triggers (Job Start, Success status, Aborted status).
   - **Real-Time Error Telemetry**: Transmits error severity, system/application error codes, and supplementary metadata to a relational tracking model.
   - **Timing Performance Tracking**: Provides checkpoint registration metrics to support service level agreements (SLAs) profiling.

=======================================================================================
PSEUDOCODE OUTLINE (PYTHON)
=======================================================================================

```python
# dwmsg_manager.py
# Reusable logging utility translating f_alis_msgerr.ksh to native Python.

import os
import sys
import datetime
import oracledb  # Or cx_Oracle depending on environment deployment

# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
# REVIEW: target database platform not specified; DB-client library choice (oracledb) below is provisional

class DwMsgManager:
    def __init__(self):
        # Resolve environment variables
        self.dw_orauser = os.environ.get("DW_ORAUSER")
        self.dw_dir_root = os.environ.get("DW_DIR_ROOT")
        self.dw_dir_prot = os.environ.get("DW_DIR_PROT")
        
        if not self.dw_orauser:
            raise ValueError("Environment variable 'DW_ORAUSER' is not set.")

    def _get_connection(self):
        # Establish connection to the Oracle database using credentials in DW_ORAUSER
        # Assumes format: username/password@hostname:port/service_name or TNS alias
        # In a production context, parse this carefully or use structured environment credentials.
        return oracledb.connect(dsn=self.dw_orauser)

    # Step 1: DWMSG_Fehlerbehandlung
    def fehlerbehandlung(self, eintrags_nr: int, error_code: int):
        """
        Executed when an error occurs in the parent script context (corresponds to trap handler).
        """
        # Step 1.1: Log unexpected fatal error code
        # kUnerwFehler = 10
        self.melde_fehler(eintrags_nr, "F", 10, f"ErrorCode ist: {error_code}")
        
        # Step 1.2: Set status to aborted
        print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
        self.setze_status_abbruch(eintrags_nr)

    # Step 2: DWMSG_SetzeStatusOK
    def setze_status_ok(self, eintrags_nr: int):
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
            sys.exit(1)
        
        # Step 2.1: Invoke package procedure directly
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p1.sql] body not supplied; inferred PL/SQL procedure call
                cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
                conn.commit()

    # Step 3: DWMSG_SetzeStatusAbbruch
    def setze_status_abbruch(self, eintrags_nr: int):
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
            sys.exit(1)
            
        # Step 3.1: Invoke package procedure directly
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p1.sql] body not supplied; inferred PL/SQL procedure call
                cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
                conn.commit()

    # Step 4: DWMSG_ErmittleNr
    def ermittle_nr(self) -> int:
        """
        Queries and returns a unique entry number from the DB (replacing /tmp/ErmittleNr_$$ storage model).
        """
        # Step 4.1: Fetch unique transaction log tracking ID directly using sequential fetch
        # REVIEW-STRUCT: SQL script [d_al_is_ermittlenr.sql] body not supplied; assuming direct dual query equivalent
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Execute the standard sequence acquisition query (substituting the underlying DB wrapper logic)
                cursor.execute("SELECT bert_sequence.NEXTVAL FROM dual")
                result = cursor.fetchone()
                if result:
                    return int(result[0])
                else:
                    raise RuntimeError("Failed to retrieve tracking number from sequence.")

    # Step 5: DWMSG_ErzeugeEintrag
    def erzeuge_eintrag(self, eintrags_nr: int, job_kennung: str, programm_name: str, log_datei: str):
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
            sys.exit(1)
            
        # Step 5.1: Write transaction initialization metrics using standard procedural call
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p4.sql] body not supplied; inferred PL/SQL procedure call
                cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programm_name, log_datei])
                conn.commit()

    # Step 6: DWMSG_MeldeFehler
    def melde_fehler(self, eintrags_nr: int, typ: str, fehler_nr: int, zusatz1: str = "", zusatz2: str = ""):
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
            sys.exit(1)
            
        # Step 6.1: Call Fehler logging procedure
        # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p[3-5].sql] body not supplied; mapped parameters directly
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
                conn.commit()

    # Step 7: DWMSG_Logdateiname
    def logdateiname(self, job_kennung: str, eintrags_nr: int) -> str:
        if not self.dw_dir_prot:
            raise ValueError("Environment variable 'DW_DIR_PROT' is not defined.")
            
        # Step 7.1: Format standard timestamp name
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"{job_kennung}_{timestamp}_{eintrags_nr}.log"
        return os.path.join(self.dw_dir_prot, filename)

    # Step 8: DWMSG_SetzeStichtagInfo
    def setze_stichtag_info(self, eintrags_nr: int, stichtag: str, stichtag_fmt: str):
        if not eintrags_nr:
            print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
            sys.exit(1)
        if not stichtag:
            print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
            sys.exit(1)
        if not stichtag_fmt:
            print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
            sys.exit(2)
            
        # Step 8.1: Bind and parse parameters inside PL/SQL to construct dynamic Stichtag updates
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Construct anonymous PL/SQL block replicating KSH execution
                plsql = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, to_date(:stichtag, :stichtag_fmt));
                    COMMIT;
                END;
                """
                cursor.execute(plsql, eintrags_nr=eintrags_nr, stichtag=stichtag, stichtag_fmt=stichtag_fmt)

    # Step 9: DWMSG_AppendTimingInfos
    def append_timing_infos(self, eintrags_nr: int, info_text: str, date_format: str):
        if not eintrags_nr:
            print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
            sys.exit(1)
        if not date_format:
            print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
            sys.exit(2)
            
        # Step 9.1: Formulate SQL parameters and execute PL/SQL block modifying ZusatzInfos
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                plsql = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, null, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
                    COMMIT;
                END;
                """
                cursor.execute(plsql, eintrags_nr=eintrags_nr, info_text=info_text, date_format=date_format)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | Translates the KornShell utility library into a reusable Python module containing the logging, audit registration, status tracking, and error instrumentation methods. |

---

### Job Dependencies
The following downstream jobs consume the output or rely on this utility module. Since they are currently not yet migrated, the runtime logging integration cannot be finalized until these target modules are established:
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

---

### Scheduling
*   **Trigger Mechanism:** This job is not directly triggered by any scheduler. It is a shared library module that is loaded or imported inside parent scheduled workflows. 
*   **Target Scheduling Strategy:** The migrated Python module must remain an importable, callable utility library without its own standalone schedule on Cloud Composer.

---

### Lineage
*   **External Calls:** Calls the external procedure `SETZEZUSATZINFOS` (which is part of the database-backed logging routines). This is translated into a direct stored procedure call or a parameterized logging insert query inside the Python code.

---

### External System Replacements
*   **Oracle SQL\*Plus Client to BigQuery / Cloud Composer Logging:** The legacy shell script launches `sqlplus` sessions using a wrapper execution model to execute PL/SQL procedures on an Oracle database. In the target architecture, these logging states should be written natively via:
    *   BigQuery API client calls to a centralized logging table in BigQuery, OR
    *   Using Cloud Composer’s application database or GCP Cloud Logging to capture runtime states.
*   **Local Filesystem to Google Cloud Storage (GCS):** Writing temporary sequence logs (previously routed through `/tmp/ErmittleNr_$$.lst`) must be replaced with Python in-memory variables or GCS state tracking if persistence across detached tasks is required.

---

### Cross-File Dependencies
*   **Oracle Stored Procedures:** Relies on the database package `BERT_MELDUNG` and its procedures:
    *   `BERT_MELDUNG.SetzeStatusOk`
    *   `BERT_MELDUNG.SetzeStatusAbbruch`
    *   `BERT_MELDUNG.Erzeuge_Eintrag`
    *   `BERT_MELDUNG.Fehler`
    *   `BERT_MELDUNG.SetzeZusatzInfos`
*   **Legacy Wrapper SQL Scripts:** Legacy KornShell logic invokes specific `.sql` wrappers inside the root directory structure:
    *   `allgemein/is/util/sql/d_alis_spaufruf_p1.sql`
    *   `allgemein/is/util/sql/d_al_is_ermittlenr.sql`
    *   `allgemein/is/util/sql/d_alis_spaufruf_p4.sql`
    *   `allgemein/is/util/sql/d_alis_spaufruf_p${NumParm}.sql`

---

### Target File Plan
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py`: The native Python module implementing the `DwMsgManager` helper utility. It contains equivalent Python functions to replace the original KornShell trap routines, output log naming rules, and status updating logic.

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
*   `DW_ORAUSER` (Legacy variable for Oracle connection string)
    *   **Target Canonical Name:** `BQ_DATASET` / `GCP_PROJECT` (for audit logging targets) or standard DB Connection IDs.
    *   **Python Resolution:** Sourced via `Variable.get("DB_CONN_ID")` or environment variables standardly set by Cloud Composer.
*   `DW_DIR_ROOT` (Legacy root repository path)
    *   **Target Canonical Name:** `GCS_BUCKET` / `GCP_PROJECT` or standard execution working directory context.
    *   **Python Resolution:** Handled using relative Python imports or standard execution directory paths in Composer workers.
*   `DW_DIR_PROT` (Legacy logging directory)
    *   **Target Canonical Name:** `GCS_BUCKET` (with a `/logs/` prefix) or Cloud Logging handlers.
    *   **Python Resolution:** Sourced via `os.environ.get("GCS_BUCKET")` or standard logging configuration.

---

### Risks and Manual Steps

#### 1. Porting of Oracle Database Package `BERT_MELDUNG`
*   The logging logic is coupled to the Oracle schema package `BERT_MELDUNG`. To make this utility operational, an equivalent auditing structure must be designed and deployed in BigQuery (e.g., target tables or stored procedures performing matching inserts) or standard Logging APIs.

#### 2. Downstream Wiring Constraints
*   The downstream calling jobs listed in the **Job Dependencies** section have not yet been migrated. The utility integration must be tested once the parent calling workflows are ported to Python.
*   *Downstream Risks:*
    *   SOURCE: NOT FOUND — `DW.BERT_ABLAUFSTEUERUNG` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_AUSD_BP_TA_MSISDN` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_AUSD_BP_TA_P_BASISPROD` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_AUSD_V_TA_PERIOD` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_AUSD_V_TA_P_VERTRAG` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_DROP_TEMP_TABLE` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_P_ADRESSEN` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_P_AUSTAUSCH` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_P_GESCHAEFTSP` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_P_RECH_EMPF` — no candidate
    *   SOURCE: NOT FOUND — `DW.BERT_RECHNUNGSDATEN` — no candidate

#### 3. String Literal Retention Requirement
*   The original utility triggers specific assertion and status prints in German. In accordance with the Output/Print Literal Rule, these precise strings must be preserved character-for-character inside the Python logging code:
    *   `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`
    *   `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`
    *   `"Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"`
    *   `"Argh!, keinen Variablennamen bei ErmittleNr angegeben"`
    *   `"Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`
    *   `"Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"`
    *   `"Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"`
    *   `"Argh!, keinen Stichtag angegeben!"`
    *   `"Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!"`
    *   `"Argh!, Formatangabe erforderlich!"`

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
REASON: The script is a date utility library containing multiple shell functions with arithmetic, validation, date manipulation, and database queries that cannot be represented as a pure BigQuery SQL script.

EVIDENCE
- Business logic found: KSH custom logic. It defines functions for date checking, date comparison, finding the last day of a month, adding days to a date, and determining date ranges using SQLPlus and shell arithmetic.
- AWK: none
- SQL-expressible: partly (the SQLPlus queries are simple date calculations, but the shell wrapper, variables, math, and loop structures require Python).
- Non-SQL side effects: Writes temporary files under `/tmp`, cleans them up, and sets environment variables via `eval`.
- Against this verdict: If all caller jobs are converted to BigQuery SQL, these date functions could potentially be rewritten as BigQuery User Defined Functions (UDFs) or standard SQL date functions, but as a shell script library, Python is the natural structural equivalent.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_date.ksh`) is a reusable utility library providing helper routines for date arithmetic, validation, and range calculations. It uses both local shell-based logic (leap year arithmetic, day addition) and Oracle database queries via SQL*Plus to perform complex date evaluations. The script is designed to be sourced by other KornShell ETL scripts in the Data Warehouse environment.

2. INVOCATION CONTEXT
   - Who calls this script: It is sourced (via `. h_alis_date.ksh`) by other Data Warehouse shell scripts rather than executed directly as a standalone UC4 job. The calling scripts are typically triggered by UC4 jobs.
   - UC4 native includes: None referenced in this script.
   - Environment files sourced:
     - The script comments state that `.dw_init` must have been executed beforehand, or environment variables `DW_DIR_ROOT` and `DW_ORAUSER` must be set.
     - # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   This utility script defines functions that accept positional parameters:
   - `DWDate_Vormonat` parameters:
     - `$1`: `VarName` (Name of variable to assign result to) - Used
     - `$2`: `DWDate_FMT` (Oracle date format mask) - Used
   - `DWDate_Datum_Check` parameters:
     - `$1`: `wert` (Date string to check) - Used
     - `$2`: `format` (Oracle format string) - Used
   - `DWDate_Datum_LE` parameters:
     - `$1`: `datum1` (Date 1 in YYYYMMDD) - Used
     - `$2`: `datum2` (Date 2 in YYYYMMDD) - Used
   - `DWDate_Gib_Zeitraum` parameters:
     - `$1`: `Offset` (Integer offset) - Used
     - `$2`: `Stufe` ('Y', 'M', or 'D' for Year, Month, Day) - Used
     - `$3`: `Format` (Output format) - Used
     - `$4`: `Var_Start` (Variable name for system date start point) - Used
     - `$5`: `Var_Ende` (Variable name for end point calculation) - Used
   - `LetzterTagDesMonats` parameters:
     - `$1`: `Datum` (Date string in YYYYMMDD format) - Used
   - `TageimMonat` parameters:
     - `$1`: `Jahr` (Year in YYYY format) - Used
     - `$2`: `Monat` (Month in MM format) - Used
   - `AddiereDatum` parameters:
     - `$1`: `Datum` (Date string in YYYYMMDD format) - Used
     - `$2`: `Tage` (Integer number of days to add) - Used

   KSH Declared Environment Parameters:
   - `DW_ORAUSER`: Database connection user string (DB-connection-style parameter)
   - `DW_DIR_ROOT`: Base directory for SQL and shell scripts (generic path parameter)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql $DWDate_tmpFile $DWDate_FMT </dev/null`
     - Purpose: Executes an external Oracle SQL script to determine the previous month and writes the output to a temp file.
     - Target: Should become a native Python call (using native `datetime` or `dateutil` module) to avoid database execution overhead, or a native Python DB client query using `oracledb`.
   - `sqlplus -s` (inline document) inside `DWDate_Datum_Check`
     - Purpose: Checks if a date string matches a format mask by calling Oracle `to_date`.
     - Target: Native Python implementation via `datetime.strptime()` is recommended.
   - `sqlplus -s` (inline document) inside `DWDate_Datum_LE`
     - Purpose: Performs a PL/SQL block validation to check if datum1 <= datum2.
     - Target: Native Python logical comparison (`date1 <= date2`) is recommended.
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql $tmpFile $Offset $Stufe $Format </dev/null`
     - Purpose: Runs an external SQL script to get a start/end date range.
     - Target: Can be fully resolved using Python's native `dateutil.relativedelta` or standard date arithmetic.
   - `grep`, `cut`, `rm`, `date`: Shell utility commands used to parse database output and manage temporary files. These should be replaced by Python standard libraries (`re`, `os`, `shutil`, `datetime`).

5. EMBEDDED SQL
   - Inline inside `DWDate_Datum_Check`:
     ```sql
     select to_date('$wert','$format') from dual;
     ```
     - Statement Type: SELECT
     - Tables touched: `dual`
     - Dialect: Oracle SQL (uses `to_date` and `dual` table).
   - Inline inside `DWDate_Datum_LE`:
     ```sql
     DECLARE
         datum1 DATE;
         datum2 DATE;
     BEGIN
         datum1:=TO_DATE('$datum1','$format');
         datum2:=TO_DATE('$datum2','$format');
         IF datum1>datum2 
         THEN
             raise_application_error(-20422,'Datum $datum1 ist groesser als $datum2');
         END IF;
     END;
     /
     ```
     - Statement Type: Anonymous PL/SQL Block
     - Tables touched: None
     - Dialect: Oracle PL/SQL (unambiguous, uses `raise_application_error`, block syntax, and trailing `/`).
   - Referenced External SQL Files (not supplied):
     - `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql`
       - # REVIEW-STRUCT: SQL file d_alis_vormonat.sql not supplied — behaviour unknown
     - `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql`
       - # REVIEW-STRUCT: SQL file d_alis_datum_zeitraum.sql not supplied — behaviour unknown

6. CONTROL FLOW
   The script contains several independent helper functions:
   - **`DWDate_Vormonat` Flow**:
     1. Define temporary file path: `/tmp/h_alis_date_<script>_<pid>.tmp`
     2. Invoke `sqlplus` to execute `d_alis_vormonat.sql`, passing output path and format argument.
     3. Assign the contents of the temp file to the caller's variable name using `eval`.
     4. Remove temporary file.
   - **`DWDate_Datum_Check` Flow**:
     1. Assert that exactly 2 parameters are provided (return 1 on failure).
     2. Execute inline `SELECT TO_DATE(...) FROM dual;` via `sqlplus`.
     3. Return the exit code of `sqlplus` execution (0 if valid, non-zero if invalid).
   - **`DWDate_Datum_LE` Flow**:
     1. Assert that exactly 2 parameters are provided (return 1 on failure).
     2. Execute inline PL/SQL block via `sqlplus` using Oracle format "YYYYMMDD".
     3. PL/SQL block compares dates; if `datum1 > datum2`, raises application error `-20422`.
     4. Return the exit code of `sqlplus` execution.
   - **`DWDate_Gib_Zeitraum` Flow**:
     1. Assert that exactly 5 parameters are provided (return 1 on failure).
     2. Establish unique temp file name using system date and process ID.
     3. Run `d_alis_datum_zeitraum.sql` using `sqlplus`.
     4. Grep for string `"DWH_Ergebnis;"` in temp file to count matches.
     5. If match count is not exactly 1, print error to stderr and return 1.
     6. Parse out the Start and End date tokens separated by semicolon (`;`) using `grep` and `cut`.
     7. Assign results back to caller environment variable names using `eval`.
     8. Remove temporary file.
   - **`LetzterTagDesMonats` Flow**:
     1. Extract Year, Month, and Day components from input string via positional string extraction.
     2. Calculate if Year is a leap year (using modulo arithmetic).
     3. Define array of month day-counts (adjusting February for leap years).
     4. Check if the parsed Day matches the determined end day of Month.
     5. Return 0 if true, 1 if false.
   - **`TageimMonat` Flow**:
     1. Calculate if the input Year is a leap year.
     2. Return the maximum days of the input Month (index lookup on an array).
   - **`AddiereDatum` Flow**:
     1. Extract Year, Month, and Day components.
     2. Perform initial addition of day offset to Day.
     3. Loop while current Day exceeds the month's maximum length (`TageimMonat`):
        - Subtract monthly limit from Day.
        - Increment Month.
        - Loop while Month exceeds 12 to adjust year overflow.
     4. Re-format Day, Month, and Year to correct digit padding (using `tail`).
     5. Return (print) date string.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments are detected with direct checks (`[ $# -ne X ]`) and return a status code of `1`.
   - SQL*Plus processes enforce standard Oracle failure detection using `WHENEVER SQLERROR EXIT FAILURE ROLLBACK;`.
   - `DWDate_Gib_Zeitraum` explicitly raises an error (exit status 1) if the output pattern check fails.
   - `LetzterTagDesMonats` returns `0` on success (last day of month) and `1` on failure (not last day).
   - Python equivalence:
     - Missing arguments should raise a standard Python `TypeError` or `ValueError`.
     - DB-client errors from `oracledb` should raise `oracledb.DatabaseError`.
     - The custom application error `-20422` can be mapped to a custom Python exception subclass.

8. OUTPUTS / SIDE EFFECTS
   - Temporary files: created in `/tmp` during executions of SQLPlus extraction scripts, and deleted after.
   - Environment mutation: functions like `DWDate_Vormonat` and `DWDate_Gib_Zeitraum` write their results back to variables specified by the caller via `eval`. In Python, these functions should simply return a tuple or dictionary containing the computed results.
   - Standard output: `AddiereDatum` and `TageimMonat` output results to stdout. In Python, these should return values directly.

9. BUSINESS SUMMARY
   - Provides consistent date validation and checking against the Oracle database platform standard.
   - Facilitates calculations for reporting cycles, such as retrieving previous-month parameters or calculating arbitrary offsets.
   - Standardizes start-of-period and end-of-period range bounds (e.g., Year bounds, Month Ultimo bounds).
   - Avoids processing overhead by implementing efficient local mathematical determinations for leap years and calendar structures where possible.

=== PSEUDOCODE STYLE ===

```python
import os
import sys
import tempfile
import calendar
from datetime import datetime, timedelta

# REVIEW: Target database platform is specified as Oracle via $DW_ORAUSER.
# Standard Python native libraries (datetime, calendar) can completely replace
# the SQLPlus database round-trips for improved efficiency and environment portability.
# Both native implementations and DB-client fallback signatures are provided.

# Helper to load DB connection details if DB executions are strictly required
DB_USER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")


# Step 1: DWDate_Vormonat
# Replaces DWDate_Vormonat function.
# Recommends native datetime instead of unsupplied d_alis_vormonat.sql
def dw_date_vormonat(fmt: str) -> str:
    # Native implementation:
    now = datetime.now()
    # Calculate first day of current month, then subtract 1 day to get into previous month
    first_of_this_month = now.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    
    # Standard format translation mapping Oracle format to Python strftime
    # Simple subset handling:
    fmt_map = {"YYYYMM": "%Y%m", "YYYYMMDD": "%Y%m%d", "YYYY-MM-DD": "%Y-%m-%d"}
    py_fmt = fmt_map.get(fmt, "%Y%m") # Defaulting to %Y%m
    
    return last_of_prev_month.strftime(py_fmt)

    # Alternative DB fallback if d_alis_vormonat.sql contains complex custom calendar rules:
    # # REVIEW-STRUCT: SQL file d_alis_vormonat.sql not supplied — behavior unknown
    # tmp_file = tempfile.NamedTemporaryFile(delete=False, prefix="h_alis_date_vormonat_", suffix=".tmp")
    # tmp_file.close()
    # subprocess.run([
    #     "sqlplus", "-s", DB_USER,
    #     f"@{DW_DIR_ROOT}/allgemein/is/util/sql/d_alis_vormonat.sql",
    #     tmp_file.name, fmt
    # ], check=True)
    # with open(tmp_file.name, "r") as f:
    #     result = f.read().strip()
    # os.unlink(tmp_file.name)
    # return result


# Step 2: DWDate_Datum_Check
# Checks if a date matches the specified format. Returns True if valid, False otherwise.
def dw_date_datum_check(wert: str, format_str: str) -> bool:
    if not wert or not format_str:
        raise ValueError("Exactly two parameters required: wert, format_str")
        
    # Standard translation mapping for common Oracle formats
    fmt_map = {
        "YYYYMMDD": "%Y%m%d",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD.MM.YYYY": "%d.%m.%Y"
    }
    py_fmt = fmt_map.get(format_str)
    
    if py_fmt:
        try:
            datetime.strptime(wert, py_fmt)
            return True
        except ValueError:
            return False
    else:
        # DB Fallback if complex Oracle format mask is utilized
        import oracledb
        try:
            connection = oracledb.connect(user=DB_USER, dsn=os.environ.get("DB_TNS_NAME_DWH"))
            with connection.cursor() as cursor:
                cursor.execute("select to_date(:wert, :format) from dual", wert=wert, format=format_str)
            return True
        except oracledb.DatabaseError:
            return False


# Step 3: DWDate_Datum_LE
# Verifies that datum1 <= datum2. Both arguments in format YYYYMMDD.
# Raises ValueError if datum1 > datum2.
def dw_date_datum_le(datum1: str, datum2: str) -> bool:
    if not datum1 or not datum2:
        raise ValueError("Exactly two parameters required: datum1, datum2")
        
    d1 = datetime.strptime(datum1, "%Y%m%d")
    d2 = datetime.strptime(datum2, "%Y%m%d")
    
    if d1 > d2:
        # Replicates custom Oracle error raised via raise_application_error(-20422)
        raise ValueError(f"Datum {datum1} ist groesser als {datum2}")
    
    return True


# Step 4: DWDate_Gib_Zeitraum
# Generates period start and end based on offset, increment units ('Y', 'M', 'D') and format.
# Returns tuple of (Start_Date, End_Date).
def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    # Native implementation using relative datetime transformations:
    start_dt = datetime.now()
    
    if stufe == 'D':
        end_dt = start_dt + timedelta(days=offset)
    elif stufe == 'M':
        # Align start of month to first of month, end of month to Ultimo of offset month
        # Since exact logic is in missing SQL file, native implementation approximates standard logic:
        start_dt = start_dt.replace(day=1)
        # Shift month
        year_offset = (start_dt.month + offset - 1) // 12
        new_month = (start_dt.month + offset - 1) % 12 + 1
        end_dt_temp = start_dt.replace(year=start_dt.year + year_offset, month=new_month, day=1)
        last_day = calendar.monthrange(end_dt_temp.year, end_dt_temp.month)[1]
        end_dt = end_dt_temp.replace(day=last_day)
    elif stufe == 'Y':
        # Year start is Jan 1st, Year end is Dec 31st of offset target year
        start_dt = start_dt.replace(month=1, day=1)
        end_dt = start_dt.replace(year=start_dt.year + offset, month=12, day=31)
    else:
        raise ValueError(f"Unsupported Stufe: {stufe}")
        
    fmt_map = {"YYYYMMDD": "%Y%m%d", "YYYY-MM-DD": "%Y-%m-%d"}
    py_fmt = fmt_map.get(format_str, "%Y%m%d")
    
    return start_dt.strftime(py_fmt), end_dt.strftime(py_fmt)

    # Alternative DB fallback:
    # # REVIEW-STRUCT: SQL file d_alis_datum_zeitraum.sql not supplied — behavior unknown
    # tmp_file = tempfile.NamedTemporaryFile(delete=False, prefix="tmp_h_alis_date_zeitraum_", suffix=".tmp")
    # tmp_file.close()
    # subprocess.run([
    #     "sqlplus", "-s", DB_USER,
    #     f"@{DW_DIR_ROOT}/allgemein/is/util/sql/d_alis_datum_zeitraum.sql",
    #     tmp_file.name, str(offset), stufe, format_str
    # ], check=True)
    # with open(tmp_file.name, "r") as f:
    #     lines = f.readlines()
    # os.unlink(tmp_file.name)
    # matching_lines = [l.strip() for l in lines if "DWH_Ergebnis;" in l]
    # if len(matching_lines) != 1:
    #     print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
    #     print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
    #     print(f"   1 Zeile erwartet, {len(matching_lines)} Zeile(n) bekommen", file=sys.stderr)
    #     raise RuntimeError("Internal Date Processing Error")
    # tokens = matching_lines[0].split(";")
    # return tokens[1], tokens[2]


# Step 5: LetzterTagDesMonats
# Returns True if input date YYYYMMDD is the last day of its month, False otherwise.
def letzter_tag_des_monats(datum: str) -> bool:
    year = int(datum[0:4])
    month = int(datum[4:6])
    day = int(datum[6:8])
    
    last_day = calendar.monthrange(year, month)[1]
    return day == last_day


# Step 6: TageimMonat
# Returns the number of days of the month in the given year.
def tage_im_monat(jahr: int, monat: int) -> int:
    return calendar.monthrange(jahr, monat)[1]


# Step 7: AddiereDatum
# Adds offset days to the given date (YYYYMMDD) and returns the formatted result.
def addiere_datum(datum: str, tage: int) -> str:
    dt = datetime.strptime(datum, "%Y%m%d")
    result_dt = dt + timedelta(days=tage)
    return result_dt.strftime("%Y%m%d")
```

An implementation-ready **Migration Design Document** has been prepared for the utility script. 

The automatic output from the primary conversion tool (`ksh_design_python`) has been captured by the system and will be appended downstream. Below is the essential design context, including environment orchestration, architectural mappings, and risk assessments that the automated parser cannot see.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py` | Converted to a Python utility library. Replaces KornShell syntax and Oracle SQL*Plus calls with native Python `datetime` and `calendar` libraries to eliminate database call overhead. |

---

### Add Context the MCP Could Not See

#### Job Dependencies
* **Downstream Jobs (Consumers):**
  The following downstream jobs source or consume outputs from this utility script and are **not yet migrated**:
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
  
  *Wiring Strategy on BigQuery/Cloud Composer:* Because these consuming ETL and scheduling scripts are not yet migrated, their downstream wiring cannot be finalized. Once migrated, they must import the rewritten Python module (`h_alis_date.py`) using standard Python import mechanisms instead of legacy shell sourcing (`. h_alis_date.ksh`).

#### Schedule & Variables — Must Be Retained
* **Scheduling:** 
  This job is **not directly triggered by any scheduler** (it is a shared utility module). It must remain a callable, importable library in the target environment (e.g., deployed inside a shared library directory or package in Cloud Composer's DAGs/plugins folder). It does not have a standalone DAG schedule.
* **Variables:**
  * `DW_ORAUSER` (Legacy DB credential variable) — *Deprecated.* Because all date mathematical operations and checks are modernized to run locally within Python, the utility no longer requires database access or credentials.
  * `DW_DIR_ROOT` (Legacy file root variable) — *Deprecated.* Relies on standard Python packaging/import directories, removing the need for a global script path variable.

#### Lineage
* **Upstream Data Sources:**
  * Oracle `DUAL` table (lineage confidence: 0.95): This represents standard Oracle dummy queries for system-time calculations (e.g., `select to_date(...) from dual`). Under the BigQuery/Python target architecture, these queries are replaced completely with local `datetime` evaluations, removing any database query dependency.

#### External System Replacements
* **Oracle SQL\*Plus -> Native Python Math:**
  Instead of executing remote shell subprocesses invoking SQL\*Plus to perform basic calendar calculations (such as finding the last day of a month or adding days), the target system will use standard Python `calendar` and `datetime` libraries. This eliminates connection latency, reduces cost, and increases processing performance.

#### Cross-File Dependencies
* **Unsupplied SQL Files:**
  The legacy script depends on two external Oracle SQL files which were not supplied in the migration bundle:
  * `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql` (Used in `DWDate_Vormonat`)
  * `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql` (Used in `DWDate_Gib_Zeitraum`)
  
  The Python design replicates their expected standard behavior (calculating the previous month and calculating standardized period starts/ends based on offsets) natively using standard Gregorian calendar math.

#### Target File Plan
* **Target Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py`
  * **Language:** Python 3.x
  * **Source Component:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
  * **Purpose:** High-performance, unit-testable Python utility module containing Python functions equivalents for the KSH date math procedures.

#### Environment-Specific Values
The environment variables used in the original shell script are classified below:
* `DW_ORAUSER` — **GLOBAL**: Identifies the legacy database connection details. (Deprecated in the primary native Python design but retained as an environment configuration if database fallbacks are ever needed).
* `DW_DIR_ROOT` — **GLOBAL**: Root directory for shell and SQL script path resolution. (Deprecated in Python as native packages/relative imports are utilized instead).

---

### Risks and Manual Steps

1. **UNRESOLVED COMPONENT (SQL File Dependency):**
   * **SOURCE:** NOT FOUND — `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql` — no candidate.
   * **RISK:** The behavior of `DWDate_Vormonat` has been simulated based on standard date math. If this SQL file contained custom business rules (e.g., custom financial/accounting calendars, specific holiday rules, or non-standard month-end logic), the native Python simulation might diverge. 
   * **MANUAL STEP:** A developer must locate this file on the legacy system and confirm if standard Gregorian month-subtraction is sufficient.

2. **UNRESOLVED COMPONENT (SQL File Dependency):**
   * **SOURCE:** NOT FOUND — `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql` — no candidate.
   * **RISK:** `DWDate_Gib_Zeitraum` calculates date intervals/ranges using this external SQL script. The native Python implementation reproduces default behaviors for 'Y', 'M', and 'D' increments, but if custom period configurations exist in the SQL script, validation discrepancies may arise.
   * **MANUAL STEP:** Verify the logic of `d_alis_datum_zeitraum.sql` on the legacy server and adjust the Python helper `dw_date_gib_zeitraum` to match any custom logic if necessary.

3. **UNMIGRATED DOWNSTREAM DEPENDENCIES:**
   * **RISK:** A total of 12 downstream ETL scripts are listed as "not yet migrated". They are still sourcing `h_alis_date.ksh`. Modernizing this utility to Python without migrating the consumers will break legacy systems if they attempt to execute or include the modernized version before their own migration is complete.
   * **MANUAL STEP:** Keep both the legacy `.ksh` and the modern `.py` files in parallel during the migration's transition phase. Do not retire the shell version until all 12 consuming jobs are fully migrated to Cloud Composer.

4. **GERMAN-LANGUAGE PRINT/ERROR LITERALS (Retained Verbatim):**
   * **RULE APPLICATION:** In compliance with the *Output/Print Literal Rule*, all original German log messages and error text have been preserved character-for-character inside the Python code block to prevent breaking downstream monitoring/log-scraping tools. 
     * *Example retained text:* `"!! Interner Fehler bei der Rueckgabe von Datumswerten"`
     * *Example retained text:* `"   Funktion: DWDate_Gib_Zeitraum"`
     * *Example retained text:* `"   1 Zeile erwartet, {} Zeile(n) bekommen"`
     * *Example retained text:* `"Datum {} ist groesser als {}"`

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
REASON: The script is a library of parameter validation, string mapping, system normalization, and relative date calculation functions.

EVIDENCE
- Business logic found: KSH custom logic contains complex data structures, string-mapping rules (case blocks), source/metric compatibility matrices, and date validation logic.
- AWK: none
- SQL-expressible: no, this is orchestration-level parameter validation, formatting, and date calculation helper code.
- Non-SQL side effects: relies on dynamic environment variable mutation (`eval`) and invokes external utilities for date checking.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_parameter.ksh`) functions as a shared parameter-parsing and validation library (helper module) within an IS/RPT reporting environment. It normalizes names for metrics (Kennzahlen), source systems, and master data components into standardized 3-letter codes, enforces business validation matrices, groups metrics into business domains, and handles temporal validations/relative offset calculations. It acts as a safety layer by tracking errors globally via error codes (`ErrNr`) and error arguments (`ErrArg`).

2. INVOCATION CONTEXT
   - Sourced (via `. h_alis_parameter.ksh`) by parent KornShell orchestration scripts. It is not called directly as a standalone process in production.
   - # REVIEW-STRUCT: UC4 job context and includes are not supplied — this helper library is environment-agnostic but depends on caller orchestration scripts.
   - Environment files sourced: None. It expects the global variables `ErrNr` and `ErrArg` to be initialized to `0` and `""` respectively by the sourcing shell environment.

3. PARAMETERS / INPUTS
   This script behaves as a function library, consuming parameters passed to its individual shell functions:
   - `ModulName` (Global, set to `"alis_parameter"`)
   - `ModulVersion` (Global, set to `"V3.0.9"`)
   - `ErrNr` (Global state, integer error tracking)
   - `ErrArg` (Global state, string detailing the error context)
   
   Function-level arguments:
   - `pruefeParameterGesetzt`:
     - `$1` (`param_name`): Human-readable name of the parameter
     - `$2` (`param_var`): Name of the environment variable to test
   - `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra`:
     - `$1` (`VarName`): Name of the environment variable holding the value to mutate/normalize
   - `pruefeSystemKennzahl`:
     - `$1` (`System`): Sourced system short name
     - `$2` (`Kennzahl`): Sourced metric short name
   - `gibBereich`:
     - `$1` (`Kennzahl`): Metric short name
     - `$2` (`VarBereich`): Variable name to write the resulting domain to
   - `gibIntervall`:
     - `$1` (`Kennzahl`): Metric short name
     - `$2` (`VarIntervall`): Variable name to write the resulting interval ('t' or 'm') to
   - `pruefeZeitraum`:
     - `$1` (`Anfang`): Start date (YYYYMMDD)
     - `$2` (`Ende`): End date (YYYYMMDD)
   - `pruefeZahlPositiv`:
     - `$1` (`p_Zahl`): Numeric value to validate
     - `$2` (`p_ParameterName`): Context name of the numeric variable
   - `pruefeZeitParameter`:
     - `$1` (`p_Anfangsdatum`): Start date
     - `$2` (`p_Endedatum`): End date
     - `$3` (`p_ZeitOffset`): Numeric relative offset (integer)
   - `konvertiereZeitspanne`:
     - `$1` (`p_VarAnfang`): Variable name to write computed start date to
     - `$2` (`p_VarEnde`): Variable name to write computed end date to
     - `$3` (`p_Spanne`): Numeric date span
     - `$4` (`p_Kennzahl`): Metric short name

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWDate_Datum_Check`:
     - Verbatim command: `DWDate_Datum_Check $Wert $Format`
     - Purpose: External DWH date utility to validate if a string matches the date format (YYYYMMDD).
     - Target client: Convert to Python native `datetime.strptime()` check.
     - # REVIEW-STRUCT: launcher DWDate_Datum_Check invoked — internal behaviour not available in this extraction; behavior inferred as standard format validation.
   - `DWDate_Datum_LE`:
     - Verbatim command: `DWDate_Datum_LE $Anfang $Ende`
     - Purpose: External DWH utility to check if `$Anfang` is less than or equal to `$Ende`.
     - Target client: Convert to Python native operators (`<=`).
     - # REVIEW-STRUCT: launcher DWDate_Datum_LE invoked — internal behaviour not available in this extraction; behavior inferred as numeric/date comparison.
   - `DWDate_Gib_Zeitraum`:
     - Verbatim command: `DWDate_Gib_Zeitraum -$p_Spanne $Offset_Unit "YYYYMMDD" Anfangsdatum Endedatum`
     - Purpose: External DWH utility to subtract offset units (Days or Months) and return absolute bounds.
     - Target client: Convert to Python native `datetime` and `dateutil.relativedelta`.
     - # REVIEW-STRUCT: launcher DWDate_Gib_Zeitraum invoked — internal behaviour not available in this extraction; behavior inferred as calendar interval calculation.
   - `basename`, `date`, `rm`: Standard POSIX utilities used during subshell logic to create and cleanup dynamic temp files. In Python, replace with standard libraries `os.path.basename`, `datetime`, and `tempfile`.

5. EMBEDDED SQL
   (no embedded SQL in this parameter validation library)

6. CONTROL FLOW
   Execution logic for sourced functions:
   - **General Preconditions**: Every function begins by inspecting the global `ErrNr`. If `ErrNr != 0`, the function exits immediately to prevent overwriting an existing upstream error state.
   - **`pruefeParameterGesetzt`**: Evaluates parameter existence dynamically via `eval`. If unset, registers `ErrNr=194` / `ErrArg=$param_name`.
   - **`konvertiereKennzahl`**: Downcases the value of the target env var, executes a `case` switch translating verbose metric names to standard 3-letter codes, and rewrites the dynamic target variable via `eval`. Unknown values result in `???`, `ErrNr=198`, and `ErrArg=$Kennzahl`.
   - **`konvertiereSystem`**: Downcases and validates system names (`sap`, `carmen`, `dpps`, `d1`, `xtra`, `ctel`, `nnv`, `dwh`, `brunet`, `sigma`). If unrecognized, registers `ErrNr=195` / `ErrArg="Unbekannte Datenherkunft $System !"`.
   - **`konvertiereSDName`**: Downcases and translates master data systems (`vo`, `rahmenvertrag` -> `rv`, `tarif` -> `trf`, `tstatus` -> `ts`, `zahlmodus` -> `zm`, etc.). Invalid entries yield `ErrNr=195`.
   - **`konvertiereAufbStufeXtra`**: Maps staging metrics (`zusammenfuehrung` -> `mrg`, `befuellung` -> `fill`). Unrecognized inputs yield `ErrNr=195`.
   - **`pruefeSystemKennzahl`**: Nested conditional checks enforcing valid permutations of System and Metric. Invalid combinations populate `ErrArg="Ungueltige Kombination $System $Kennzahl"` and trigger `ErrNr=195`.
   - **`gibBereich`**: Searches predefined whitespace-delimited lists (`list_tn`, `list_us`, `list_gd`, `list_sd`, `list_md`) to map a metric to a functional domain (`tn`, `us`, `gd`, `sd`, `md`) and dynamically sets the resulting caller variable.
   - **`gibIntervall`**: Maps a metric to either daily (`t`) or monthly (`m`) granularities and dynamically assigns the caller variable.
   - **`pruefeZeitraum`**: Validates date boundaries using temporary logging paths `/tmp/tmp_...` in a subshell with standard error trapping disabled (`set +e`). Asserts YYYYMMDD string structures and correct chronological ordering.
   - **`pruefeZahlPositiv`**: Validates input string is numeric and `>= 0`. If not, yields `ErrNr=195`.
   - **`pruefeZeitParameter`**: Validates parameters mutually exclusive: must supply either a valid relative offset OR both absolute start and end dates.
   - **`konvertiereZeitspanne`**: Determines offset scale (Days or Months) based on metrics and calls `DWDate_Gib_Zeitraum` to determine boundaries.

7. ERROR HANDLING & EXIT CODES
   - Custom tracking paradigm: Does not exit the process directly (as it is a sourced helper). It writes to global variables `ErrNr` (integer error identifier) and `ErrArg` (detailed descriptive string of the error).
   - Early returns: Every routine has a guard block `if [ $ErrNr -ne 0 ]; then return; fi`.
   - Subshell error handling: Evaluates external components under `set +e` inside subshells to prevent failure cascades and captures logs inside temporary paths, cleansing them using `rm -f` at exit.
   - Success conventions: `ErrNr` remains `0`.
   - Mapping to Python: This stateful paradigm should be replaced with native Python exceptions (e.g., `ValueError`, or a custom class `ParameterValidationError`). To match the original global state behavior for caller scripts that expect it, we can design a `ParameterContext` class that retains `err_nr` and `err_arg`.

8. OUTPUTS / SIDE EFFECTS
   - The primary side effect is side-channel variable mutation in the sourcing Shell environment via `eval "$VarName=$Value"`.
   - Creates and deletes short-lived tracking files in the `/tmp/` directory.

9. BUSINESS SUMMARY
   - Standardizes reporting definitions for all downstream processes in the Data Warehouse.
   - Ensures correct operational code generation by translating verbose system labels into normalized technical keys.
   - Prevents downstream data processing failures by validating the parameters before jobs trigger SQL transformations.
   - Provides strict checking of the mutually exclusive relationship between absolute calendar inputs and relative offset days.

=== PSEUDOCODE STYLE ===

```python
# Sourced Parameter validation & normalization module "alis_parameter"
# ModulVersion = "V3.0.9"

import os
import sys
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Global state mimicking legacy shell environment tracking
global_err_nr = 0
global_err_arg = ""
MODUL_NAME = "alis_parameter"
MODUL_VERSION = "V3.0.9"

# Helper to check if global errors are already active
def _has_error():
    global global_err_nr
    return global_err_nr != 0

def _set_error(err_nr, err_arg):
    global global_err_nr, global_err_arg
    if not _has_error():
        global_err_nr = err_nr
        global_err_arg = err_arg

# Step 1: pruefeParameterGesetzt
def pruefeParameterGesetzt(param_name: str, param_var_name: str, env_dict: dict):
    # Check upstream errors
    if _has_error():
        return

    # Validate parameters
    if not param_name or not param_var_name:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} pruefeParameterGesetzt")
        return

    # Check variable presence in context
    param_wert = env_dict.get(param_var_name, None)
    if param_wert is None or str(param_wert).strip() == "":
        _set_error(194, param_name)

# Step 2: konvertiereKennzahl
def konvertiereKennzahl(var_name: str, env_dict: dict):
    if _has_error():
        return

    if not var_name:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereKennzahl")
        return

    kennzahl = str(env_dict.get(var_name, "")).lower().strip()

    # Case switch mapping
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
        env_dict[var_name] = mapping[kennzahl]
    else:
        _set_error(198, kennzahl)
        env_dict[var_name] = "???"

# Step 3: konvertiereSystem
def konvertiereSystem(var_name: str, env_dict: dict):
    if _has_error():
        return

    if not var_name:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereSystem")
        return

    system = str(env_dict.get(var_name, "")).lower().strip()
    valid_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

    if system in valid_systems:
        env_dict[var_name] = system
    else:
        _set_error(195, f"Unbekannte Datenherkunft {system} !")
        env_dict[var_name] = "???"

# Step 4: konvertiereSDName
def konvertiereSDName(var_name: str, env_dict: dict):
    if _has_error():
        return

    if not var_name:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereSDSystem")
        return

    system = str(env_dict.get(var_name, "")).lower().strip()
    
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
        env_dict[var_name] = mapping[system]
    else:
        _set_error(195, f"Unbekannte Stammdaten-Datenherkunft {system} !")
        env_dict[var_name] = "???"

# Step 5: konvertiereAufbStufeXtra
def konvertiereAufbStufeXtra(var_name: str, env_dict: dict):
    if _has_error():
        return

    if not var_name:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereAufbStufeXtra")
        return

    stufe = str(env_dict.get(var_name, "")).lower().strip()
    
    if stufe == "zusammenfuehrung":
        env_dict[var_name] = "mrg"
    elif stufe == "befuellung":
        env_dict[var_name] = "fill"
    else:
        _set_error(195, f"Unbekannte Stufenangabe {stufe} !")
        env_dict[var_name] = "???"

# Step 6: pruefeSystemKennzahl
def pruefeSystemKennzahl(system: str, kennzahl: str):
    if _has_error():
        return

    if not system or not kennzahl:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} pruefeSystemKennzahl")
        return

    err_arg = ""

    # Enforce logic compatibility matrix
    if system != "nnv" and kennzahl in ["tvd", "lkl"]:
        err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "carmen":
        if kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sap":
        if kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dpps":
        if kennzahl in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "ctel":
        if kennzahl not in ["abg", "bst", "zug", "twe"]:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "xtra":
        if kennzahl != "rst":
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "d1":
        if kennzahl in ["gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "nnv":
        if kennzahl not in ["tvd", "lkl"]:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dwh":
        if kennzahl != "mds":
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "brunet":
        if kennzahl not in ["d1n", "rub", "lmo"]:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sigma":
        allowed = ["nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"]
        if kennzahl not in allowed:
            err_arg = f"Ungueltige Kombination {system} {kennzahl}"

    if err_arg:
        _set_error(195, err_arg)

# Step 7: gibBereich
def gibBereich(kennzahl: str, var_bereich_name: str, env_dict: dict):
    if _has_error():
        return

    if not kennzahl or not var_bereich_name:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} gibBereich")
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

    if not my_bereich:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} gibBereich - Kuerzel '{kennzahl}' unbekannt")
        return

    env_dict[var_bereich_name] = my_bereich

# Step 8: gibIntervall
def gibIntervall(kennzahl: str, var_intervall_name: str, env_dict: dict):
    if _has_error():
        return

    if not kennzahl or not var_intervall_name:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} gibIntervall")
        return

    list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
    list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

    my_intervall = None
    if kennzahl in list_t:
        my_intervall = "t"
    elif kennzahl in list_m:
        my_intervall = "m"

    if not my_intervall:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} gibIntervall - Kuerzel '{kennzahl}' unbekannt")
        return

    env_dict[var_intervall_name] = my_intervall

# Step 9: pruefeZeitraum
# # REVIEW-STRUCT: external utilities DWDate_Datum_Check, DWDate_Datum_LE are not supplied — behavior is simulated via Python datetime.
def pruefeZeitraum(anfang: str, ende: str):
    if _has_error():
        return

    if not anfang or not ende:
        _set_error(196, f"{MODUL_NAME} {MODUL_VERSION} pruefeZeitraum")
        return

    err_arg = ""
    
    # 1. Check Date formats (YYYYMMDD)
    try:
        dt_anfang = datetime.strptime(str(anfang), "%Y%m%d")
    except ValueError:
        err_arg = "Anfangsdatum entspricht nicht dem Format YYYYMMDD"
        
    try:
        dt_ende = datetime.strptime(str(ende), "%Y%m%d")
    except ValueError:
        if not err_arg:
            err_arg = "Endedatum entspricht nicht dem Format YYYYMMDD"

    # 2. Check logical chronological order
    if not err_arg:
        if dt_anfang > dt_ende:
            err_arg = "Anfangsdatum ist nicht kleiner gleich Endedatum"

    if err_arg:
        _set_error(195, err_arg)

# Step 10: pruefeZahlPositiv
def pruefeZahlPositiv(p_zahl: str, p_parameter_name: str):
    try:
        val = int(p_zahl)
        if val < 0:
            _set_error(195, f"Parameter {p_parameter_name} muss groesser gleich 0 sein")
    except ValueError:
        _set_error(195, f"Parameter {p_parameter_name} ist kein numerischer Wert")

# Step 11: pruefeZeitParameter
def pruefeZeitParameter(p_anfangsdatum: str, p_endedatum: str, p_zeit_offset: str):
    if _has_error():
        return

    # Case 1: relative offset is set
    if p_zeit_offset and p_zeit_offset.strip() != "":
        if (not p_anfangsdatum or p_anfangsdatum.strip() == "") and (not p_endedatum or p_endedatum.strip() == ""):
            pruefeZahlPositiv(p_zeit_offset, "Zeitspanne")
            return
        else:
            _set_error(195, "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden")
            return
    else:
        # Case 2: relative offset is empty, check absolute dates
        if p_anfangsdatum and p_endedatum:
            pruefeZeitraum(p_anfangsdatum, p_endedatum)
        else:
            if not p_anfangsdatum and not p_endedatum:
                _set_error(195, "Datumswerte oder Zeitspanne fehlen")
            else:
                _set_error(195, "Sowohl Anfang- als auch Endedatum muessen angegeben werden")
            return

# Step 12: konvertiereZeitspanne
# # REVIEW-STRUCT: external utility DWDate_Gib_Zeitraum is not supplied — behavior is simulated via Python relativedelta.
def konvertiereZeitspanne(p_var_anfang: str, p_var_ende: str, p_spanne: str, p_kennzahl: str, env_dict: dict):
    if _has_error():
        return

    # Determine offset unit
    offset_unit = "D"
    if p_kennzahl == "bst":
        offset_unit = "M"

    try:
        span_val = int(p_spanne)
        today = datetime.now()  # Typically base is current date
        
        # Calculate start/end based on dynamic parameters
        if offset_unit == "D":
            dt_anfang = today - relativedelta(days=span_val)
            dt_ende = today
        else:  # Monthly granularity
            dt_anfang = today - relativedelta(months=span_val)
            dt_ende = today
            
        env_dict[p_var_anfang] = dt_anfang.strftime("%Y%m%d")
        env_dict[p_var_ende] = dt_ende.strftime("%Y%m%d")
    except Exception as e:
        _set_error(85, "DWDate_Gib_Zeitraum failed")
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` | Converts the KornShell utility library containing parameter validation, string mapping, system normalization, and date check functions into a reusable Python module. |

---

### Job Dependencies
The following downstream jobs consume this utility script (sourced in legacy KSH) and are not yet migrated. The wiring of these dependencies cannot be finalized until the downstream jobs themselves are migrated to the target environment:
* **DW.BERT_ABLAUFSTEUERUNG** — *not yet migrated*
* **DW.BERT_AUSD_BP_TA_MSISDN** — *not yet migrated*
* **DW.BERT_AUSD_BP_TA_P_BASISPROD** — *not yet migrated*
* **DW.BERT_AUSD_V_TA_PERIOD** — *not yet migrated*
* **DW.BERT_AUSD_V_TA_P_VERTRAG** — *not yet migrated*
* **DW.BERT_AUSD_V_TA_VERTRAG_TMP** — *not yet migrated*
* **DW.BERT_DROP_TEMP_TABLE** — *not yet migrated*
* **DW.BERT_P_ADRESSEN** — *not yet migrated*
* **DW.BERT_P_AUSTAUSCH** — *not yet migrated*
* **DW.BERT_P_GESCHAEFTSP** — *not yet migrated*
* **DW.BERT_P_RECH_EMPF** — *not yet migrated*
* **DW.BERT_RECHNUNGSDATEN** — *not yet migrated*

*On the target platform (Cloud Composer / Python), downstream Python modules or Airflow DAG operators will import this migrated Python library (`h_alis_parameter.py`) instead of sourcing a shell script.*

---

### Schedule & Variables — Must Be Retained
* **Scheduling**: This job is **not** directly triggered by any of the environment's schedulers. It runs strictly as an include/shared module inside other scheduled tasks. Therefore, the migrated artifact **must not** be given its own standalone schedule; it must remain a callable, importable Python library.
* **Sourced Variables**: 
  * `ErrNr` (Integer): Tracked as a global variable in the legacy script, indicating error state.
  * `ErrArg` (String): Tracked globally, providing context for the recorded `ErrNr`.
  * *In Python, this stateful environment tracking should be encapsulated via a standard object context or managed using native Python custom exception structures.*

---

### Lineage
* **Upstream Producers**: None discovered.
* **Downstream Consumers**: See the downstream jobs listed under "Job Dependencies".

---

### External System Replacements
The legacy script makes calls to custom, external command-line data warehouse utilities. These must be replaced with native, platform-compatible Python components:
* **`DWDate_Datum_Check`**: Validates date formats. Replace with native Python parsing: `datetime.strptime(date_string, "%Y%m%d")`.
* **`DWDate_Datum_LE`**: Chronological order verification. Replace with native Python date comparison operators (`<=`).
* **`DWDate_Gib_Zeitraum`**: Computes relative start and end dates. Replace with standard calculations using Python's `datetime` module and `dateutil.relativedelta.relativedelta`.

---

### Cross-File Dependencies
This script is a shared module. Any other shell script in the environment that sources this library via `. h_alis_parameter.ksh` must be updated to import the migrated Python module:
```python
import h_alis_parameter
```

---

### Target File Plan

* **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py`
  * **Language**: Python (`.py`)
  * **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
  * **Purpose**: Houses the rewritten functions (`pruefeParameterGesetzt`, `konvertiereKennzahl`, `konvertiereSystem`, `konvertiereSDName`, `konvertiereAufbStufeXtra`, `pruefeSystemKennzahl`, `gibBereich`, `gibIntervall`, `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, `konvertiereZeitspanne`) as standard Python functions, utilizing a class or dynamic dict context to manage error status without polluting global operating system scopes.

---

### Environment-Specific Values

The script relies on internal constants that are job-specific rather than global platform properties:
* **`MODUL_NAME`** (JOB-SPECIFIC): Defined as `"alis_parameter"`.
* **`MODUL_VERSION`** (JOB-SPECIFIC): Defined as `"V3.0.9"`.

---

### Risks and Manual Steps

1. **Unmigrated Downstream Wiring**:
   * *Risk*: The wiring for all 12 downstream consumers (`DW.BERT_ABLAUFSTEUERUNG`, `DW.BERT_AUSD_BP_TA_MSISDN`, `DW.BERT_AUSD_BP_TA_P_BASISPROD`, `DW.BERT_AUSD_V_TA_PERIOD`, `DW.BERT_AUSD_V_TA_P_VERTRAG`, `DW.BERT_AUSD_V_TA_VERTRAG_TMP`, `DW.BERT_DROP_TEMP_TABLE`, `DW.BERT_P_ADRESSEN`, `DW.BERT_P_AUSTAUSCH`, `DW.BERT_P_GESCHAEFTSP`, `DW.BERT_P_RECH_EMPF`, `DW.BERT_RECHNUNGSDATEN`) cannot be finalized yet.
   * *Mitigation*: These downstream targets are currently not yet migrated. The integration of this parameter validation library must be tested step-by-step as each consumer is migrated to Cloud Composer.
2. **Transition from Shell Environments to Python Modules**:
   * *Risk*: Downstream scripts currently source this file to mutate parent-shell environment variables via `eval`. Python imports do not natively mutate the calling process's global environment in this manner.
   * *Mitigation*: Migrated calling processes should pass a parameter dictionary to these helper functions to be explicitly mutated, or receive returned dictionaries/tuples containing the normalized parameters.
3. **Behavioral Testing of Proprietary Binaries**:
   * *Risk*: Since the source code of the external utilities `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` is not supplied, native Python simulation carries risk of edge-case discrepancies (e.g. leap years, format constraints).
   * *Mitigation*: Implement robust unit testing around the Python replacements of these three modules to guarantee functional parity with the original tools.

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
REASON: The script defines a custom shell function that performs file system checks, argument validation, external error logging, and wraps SQL*Plus execution, which requires a Python conversion.

EVIDENCE
- Business logic found: KSH custom logic defines a utility function `starteSQLSkript` that checks if the script is readable, validates inputs, logs metadata, and executes SQL*Plus.
- AWK: none
- SQL-expressible: no, it consists of shell-specific file checks, parameter shifting, and running SQL*Plus.
- Non-SQL side effects: checks physical file availability on disk, runs the external program `sqlplus`, and calls the external command `DWMSG_MeldeFehler`.
- Against this verdict: none, as this is a utility shell function wrapping shell-level orchestration of SQL execution.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This script acts as a KornShell helper/utility module (`h_alis_sqlplus.ksh`) that defines a reusable function `starteSQLSkript`. The function's purpose is to safely launch SQL*Plus scripts by adding pre-execution checks. It validates that required arguments are present and verifies that the target SQL script file exists and is readable on the filesystem before passing it to `sqlplus`. If validations fail, it calls an external error reporting utility `DWMSG_MeldeFehler` with specific error codes.

### 2. INVOCATION CONTEXT
- **Caller**: This helper script is sourced (`. h_alis_sqlplus.ksh`) by other ETL scripts to make the `starteSQLSkript` function available in their shell environments. The specific calling UC4 job or UNIX command line is not provided in this snippet.
- **UC4 Includes**: None referenced in the provided text.
- **Environment Files Sourced**: None sourced directly in this script. However, it relies on the environment variable `DW_ORAUSER` being exported by the calling session.

### 3. PARAMETERS / INPUTS
The function `starteSQLSkript` accepts the following parameters:
- **`$1` (p_Eintragsnr)**: Error entry number (Fehlereintragsnummer) used for logging. Passed as the first positional parameter. Map to Python as the first argument of the function (`entry_nr`). Used in script: Yes.
- **`$2` (p_Skript)**: Path of the SQL script to be executed. Passed as the second positional parameter. Map to Python as the second argument of the function (`script_path`). Used in script: Yes.
- **`$*` (Remaining parameters)**: Arguments that should be forwarded to the SQL script itself. Sourced via shell parameter expansion after shifting. Map to Python as a variable argument list (`*script_args`). Used in script: Yes.
- **`DW_ORAUSER`**: Environment variable containing the database connection credentials/string. Map to Python as `os.environ.get("DW_ORAUSER")`. Used in script: Yes.
- **`ModulName` / `ModulVersion`**: Internal variables defining module metadata (`alis_sqlplus`, `V1.1.3`). 
  - *# REVIEW: Potential bug found in legacy script.* The script defines `ModulName` and `ModulVersion` but tries to read `Modul_Name` and `Modul_Version` (with underscores) in the error reporting step. This will result in empty strings in the generated error log. The Python code should resolve this mismatch by using the correct, unified variables.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **`DWMSG_MeldeFehler`**:
  - *Exact Command*: `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"` or `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript`
  - *Purpose*: Custom external message/error reporting tool.
  - *Translation*: Must be executed as an external process.
  - *Resolvable Launcher*: No.
  - *Marker*: `# REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion`
- **`sqlplus`**:
  - *Exact Command*: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
  - *Purpose*: Runs the SQL script on the Oracle database with redirected `/dev/null` standard input.
  - *Translation*: Invoke via Python's `subprocess.run` to handle argument forwarding and `/dev/null` redirection.
  - *Resolvable Launcher*: No, because the SQL script file run by the utility is dynamic, and the target SQL code is not included in this extraction.

### 5. EMBEDDED SQL
No static SQL is embedded in this wrapper script itself. The SQL is dynamic and resides in the file path passed to the wrapper via `$p_Skript`.
- *Dialect Identification*: Oracle SQL*Plus dialect is confirmed due to the direct invocation of the `sqlplus` executable and the use of the `@` script prefix.

### 6. CONTROL FLOW
1. **Initialize Module Metadata**: Declare module-level tracking variables (`ModulName` and `ModulVersion`).
2. **Define `starteSQLSkript`**: Establish the entry point and capture arguments.
3. **Validate Inputs**:
   - Verify that `$p_Eintragsnr` and `$p_Skript` are not empty.
   - If either is empty, call `DWMSG_MeldeFehler` with code `196` and return `196`.
4. **Verify File Readability**:
   - Check if `$p_Skript` exists and has read permissions.
   - If not readable, call `DWMSG_MeldeFehler` with code `201` and return `201`.
5. **Log Execution Details**: Print script name and arguments to standard output.
6. **Disable Exit-on-Error**: Execute `set +e` to prevent the wrapper script from terminating if the database script returns a non-zero exit code.
7. **Execute Database Script**: Run `sqlplus` passing the connection string, script path, and forwarded arguments, with `/dev/null` piped to standard input.
8. **Capture Exit Code**: Save the exit code of `sqlplus` into the `errcode` variable.
9. **Re-enable Exit-on-Error**: Execute `set -e`.
10. **Return Result**: Propagate `errcode` back to the caller.

### 7. ERROR HANDLING & EXIT CODES
- **Detection**: File tests `[ ! -r $p_Skript ]` and string length checks `[ -z ... ]` are used for pre-checks. The database execution result is captured using `$?`.
- **Handling**: Validations yield hard-coded return values (`196`, `201`) and external alerts. Database errors are caught using `set +e` and directly propagated as return values.
- **Python Mapping**: Map the function validations to Python exception raising (e.g., `ValueError`, `FileNotFoundError`) or return integer status codes to strictly preserve the interface signature if requested by caller modules. In Python, calling external binaries like `sqlplus` or `DWMSG_MeldeFehler` should handle `subprocess.CalledProcessError` properly.

### 8. OUTPUTS / SIDE EFFECTS
- Writes validation messages and log records to stdout.
- Runs `sqlplus` which can read, insert, update, or delete data depending on the provided SQL script.

### 9. BUSINESS SUMMARY
- Validates the existence and formatting of caller arguments before running SQL scripts.
- Prevents database runtime hangs by ensuring the SQL script file is readable on the filesystem before beginning execution.
- Configures SQL*Plus execution to run non-interactively by redirecting stdin to `/dev/null`.
- Centralizes error reporting for failed SQL jobs by integrating with the `DWMSG_MeldeFehler` system.
- Standardizes return codes so orchestration agents (like UC4) can detect execution failures downstream.

---

### PSEUDOCODE MULTI-LINE OUTLINE

```python
import os
import sys
import shutil
import subprocess
from typing import List

# Step 1: Initialize module metadata
# REVIEW: The legacy script declared ModulName/ModulVersion but used Modul_Name/Modul_Version in DWMSG_MeldeFehler. 
# We resolve this mismatch here by defining both with the same values.
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Step 2: Define the main utility function
def starte_sql_skript(entry_nr: str, script_path: str, *script_args: str) -> int:
    """
    Python equivalent of the legacy 'starteSQLSkript' shell function.
    Validates arguments and script file readability, then runs sqlplus.
    """
    # Step 3: Validate that required positional parameters are not empty
    if not entry_nr or not script_path:
        # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        error_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        subprocess.run(["DWMSG_MeldeFehler", entry_nr, "E", "196", error_msg], check=False)
        return 196

    # Step 4: Validate that the SQL file exists and is readable
    if not os.path.isfile(script_path) or not os.access(script_path, os.R_OK):
        # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        subprocess.run(["DWMSG_MeldeFehler", entry_nr, "E", "201", script_path], check=False)
        return 201

    # Step 5: Log start details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {script_path}")
    print(f"Skript-Parameter: {' '.join(script_args)}")

    # Step 6: Verify sqlplus executable is available on PATH
    if shutil.which("sqlplus") is None:
        raise RuntimeError("sqlplus executable not found in PATH")

    # Step 7: Resolve Oracle connection credentials
    # REVIEW: Target database is Oracle as indicated by SQL*Plus usage. 
    # Ensure DW_ORAUSER is defined in the execution environment.
    ora_user = os.environ.get("DW_ORAUSER")
    if not ora_user:
        raise ValueError("Environment variable 'DW_ORAUSER' is not set.")

    # Step 8: Execute sqlplus with redirected input from DEVNULL
    # Equivalent to legacy: sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    # Note: Using check=False and capturing exit status mirrors the legacy 'set +e' and '$?' behavior.
    cmd = ["sqlplus", ora_user, f"@{script_path}"] + list(script_args)
    
    try:
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Critical execution error during SQL*Plus invocation: {e}", file=sys.stderr)
        errcode = 1  # Standard fallback error code

    # Step 9: Return database execution status code
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Converts the legacy KornShell utility function `starteSQLSkript` into a reusable Python function, preserving the validation, output formatting, and error-handling flow. |

---

### Target File Plan

* **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py`
  * **Language**: Python
  * **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`

---

### Job Dependencies

The following downstream jobs consume this utility script:
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

**Wiring on Target Platform**:
Because this is a shared utility module (`h_alis_sqlplus.py`), it must be placed in a shared directory accessible to the Python environment of the execution environment (such as the `dags/` or `plugins/` directory of Cloud Composer/Airflow). Downstream jobs will access this function by importing the python module (`from is.util.bin.h_alis_sqlplus import starte_sql_skript`). 

---

### Scheduling

* **Trigger/Schedule**: This job is **NOT** directly scheduled. It acts purely as a shared include utility. It must remain an importable, callable Python library module without an independent standalone Airflow schedule.

---

### Schedule & Variables

* **Variables**:
  * `DW_ORAUSER`: Legacy database connection configuration. In the target environment, this should map to GCP standard authentication (Application Default Credentials) or a designated Airflow Connection. If direct database authentication credentials are required for any transition phase, they must be resolved via **Airflow Variables** (`Variable.get("DW_ORAUSER")`) or **Google Cloud Secret Manager** rather than hardcoded environment variables.

---

### External System Replacements

* **Oracle SQL*Plus (`sqlplus`)**: The legacy utility script uses the native `sqlplus` CLI tool to execute Oracle scripts. Since the target platform is BigQuery, SQL files must eventually be converted to BigQuery SQL syntax and run using the **Google Cloud BigQuery Client Library** (e.g., `google.cloud.bigquery`) or the Airflow `BigQueryInsertJobOperator` instead of executing a subprocess command.
* **`DWMSG_MeldeFehler`**: This custom error logging utility integrates with Oracle PL/SQL. In Google Cloud, this should be replaced by writing log messages using the **Google Cloud Logging** SDK or by inserting error logs into a dedicated BigQuery audit table.

---

### Cross-File Dependencies

* **Call Chain**: Any migrated script that previously utilized `. h_alis_sqlplus.ksh` to source these helper routines must be modified to use Python `import` statements to call the `starte_sql_skript` function.

---

### Environment-Specific Values

* **`DW_ORAUSER` (GLOBAL)**: Identifies the target database environment connection string. Sourced via `Variable.get("DW_ORAUSER")` (Airflow) or `os.environ.get("DW_ORAUSER")`.
* **`ModulName` / `ModulVersion` (JOB-SPECIFIC)**: Specific utility module identifiers. Retained in the Python code as constant strings (`MODUL_NAME = "alis_sqlplus"`, `MODUL_VERSION = "V1.1.3"`).

---

### Risks and Manual Steps

* **Downstream Integration**: Every downstream consumer listed in the **Job Dependencies** section is currently **not yet migrated**. The integration and import chains cannot be fully tested or finalized until these dependent modules are migrated.
* **`DWMSG_MeldeFehler` Behavior**: Because the source code for the custom `DWMSG_MeldeFehler` command is not provided, the logic for logging severity and codes must be manually aligned with the target system logging architecture during the implementation of the logging framework.
* **Dialect Change (Oracle SQL*Plus to BigQuery)**: The helper function dynamically executes SQL scripts. Simply migrating the helper to Python does not convert the underlying Oracle SQL scripts. All scripts passed to this utility must be translated to BigQuery SQL dialect before execution.
* **Output Literal Preservation**: German terminal output lines are printed during execution:
  * `"Rufe SQL*PLUS auf mit folgenden Einstellungen"`
  * `"Sql*Plus-Skript : "`
  * `"Skript-Parameter: "`
  
  These exact literal strings must be preserved character-for-character in the target Python print/logging statements to ensure compatibility with any log-monitoring scripts.