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
REASON: The script is a library of KornShell functions containing orchestration logic, error logging, parameter validation, and Oracle DB calls via sqlplus that cannot be fully expressed in BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic defines multiple utility functions for error handling, database-backed logging via Oracle PL/SQL, and log filename construction.
- AWK: none
- SQL-expressible: no, contains extensive shell-based control flow, file manipulation, dynamic variable assignment, and date calculations.
- Non-SQL side effects: writing/deleting temp files, determining log paths based on process ID, and exiting shell processes.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This script (`f_alis_msgerr.ksh`) serves as a central logging, status-tracking, and error-handling library for the "Information Services" (IS) system. It is designed to be sourced by other KornShell scripts to standardise how execution states, timing info, and execution errors are captured and written to the database. It interfaces with an Oracle database to maintain a run register in the table/package `BERT_MELDUNG`.

### 2. INVOCATION CONTEXT
- **Sourced by**: Various parent KornShell ETL/reporting scripts within the system.
- **UC4 Job invocation**: This script is a helper library and is not run directly by UC4; however, the parent scripts sourcing it are run under UC4 (e.g. standard UNIX jobs).
- **Environment files sourced**: None directly sourced within this file.
- **Implicit environment dependencies**: Assumes environment variables `$DW_ORAUSER` (Oracle connection details), `$DW_DIR_ROOT` (application root path), and `$DW_DIR_PROT` (protocol/log directory) are already defined by the caller.

### 3. PARAMETERS / INPUTS
This script defines functions that accept parameters. Below is the mapping of all parameters used inside the functions:

| Function Name | Parameter | Source | Used? | Python equivalent / Surface path |
|---|---|---|---|---|
| `DWMSG_Fehlerbehandlung` | `$1` (DWMSG_EintragsNr) | Positional argument | Yes | Function argument `eintrags_nr` |
| `DWMSG_SetzeStatusOK` | `$1` (DWMSG_EintragsNr) | Positional argument | Yes | Function argument `eintrags_nr` |
| `DWMSG_SetzeStatusAbbruch` | `$1` (DWMSG_EintragsNr) | Positional argument | Yes | Function argument `eintrags_nr` |
| `DWMSG_ErmittleNr` | `$1` (VarName) | Positional argument | Yes | Function return value (instead of dynamic `eval` assignment) |
| `DWMSG_ErzeugeEintrag` | `$1` (DWMSG_EintragsNr) <br> `$2` (JobKennung) <br> `$3` (Programmname) <br> `$4` (LogDatei) | Positional arguments | Yes | Function arguments `eintrags_nr`, `job_kennung`, `programm_name`, `log_datei` |
| `DWMSG_MeldeFehler` | `$1` (DWMSG_EintragsNr) <br> `$2` (Typ) <br> `$3` (FehlerNr) <br> `$4` (Zusatz1) <br> `$5` (Zusatz2) | Positional arguments | Yes | Function arguments with optional arguments mapped to keyword arguments |
| `DWMSG_Logdateiname` | `$1` (VarName) <br> `$2` (JobKennung) <br> `$3` (DWMSG_EintragsNr) | Positional arguments | Yes | Return value or constructed path string |
| `DWMSG_SetzeStichtagInfo` | `$1` (DWMSG_EintragsNr) <br> `$2` (DWMSG_Stichtag) <br> `$3` (DWMSG_StichtagFmt) | Positional arguments | Yes | Function arguments `eintrags_nr`, `stichtag`, `stichtag_fmt` |
| `DWMSG_AppendTimingInfos` | `$1` (DWMSG_EintragsNr) <br> `$2` (DWMSG_InfoText) <br> `$3` (DWMSG_DateFormat) | Positional arguments | Yes | Function arguments `eintrags_nr`, `info_text`, `date_format` |

**Environment variables used:**
- `DW_ORAUSER`: DB-connection-style parameter (contains credentials/connection-string for Oracle).
- `DW_DIR_ROOT`: Root folder path. Informational only.
- `DW_DIR_PROT`: Target directory path for logs.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The script invokes `sqlplus` to execute Oracle PL/SQL commands.
- **Command:** `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
  - **Purpose:** Calls PL/SQL stored procedure `BERT_MELDUNG.SetzeStatusOk` to set job status to successful.
  - **Python Handling:** Direct DB-client call via an Oracle database driver (e.g. `oracledb`).
- **Command:** `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
  - **Purpose:** Calls PL/SQL stored procedure `BERT_MELDUNG.SetzeStatusAbbruch` to set job status to aborted.
  - **Python Handling:** Direct DB-client call.
- **Command:** `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
  - **Purpose:** Generates a unique tracking sequence ID and dumps it into a temporary file.
  - **Python Handling:** Replaced by querying the DB directly (e.g., executing a query and retrieving the returned value), avoiding temporary files completely.
- **Command:** `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
  - **Purpose:** Registers an execution record in the database tracking table.
  - **Python Handling:** Direct DB-client call.
- **Command:** `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
  - **Purpose:** Logs an error entry with optional parameter strings.
  - **Python Handling:** Direct DB-client call.
- **Command:** Inline heredoc database invocations via `sqlplus -s $DW_ORAUSER <<EOF ... EOF`
  - **Purpose:** Calls `BERT_MELDUNG.SetzeZusatzInfos` with custom parameters and performs SQL commits.
  - **Python Handling:** Executed via DB driver transaction blocks.

# REVIEW-STRUCT: SQL helper scripts (such as d_alis_spaufruf_p1.sql, d_al_is_ermittlenr.sql, etc.) are external files whose bodies are not supplied in this extraction. Their behavior is inferred from parameter structure and the name of the procedures they call.

### 5. EMBEDDED SQL
The script calls external SQL files with PL/SQL targets and has inline SQL blocks.
- **Source:** Inline block in `DWMSG_SetzeStichtagInfo`
  ```sql
  EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
  commit;
  ```
- **Source:** Inline block in `DWMSG_AppendTimingInfos`
  ```sql
  EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
  commit;
  ```
- **Dialect Identification:** These statements contain Oracle-specific PL/SQL commands (`EXEC`, `commit;`), functions (`to_date`, `to_char`, `SYSDATE`), and packages (`BERT_MELDUNG`).
- **Target tables touched:** `BERT_MELDUNG` (or matching underlying tracking table).
- **Python implementation:** Execute via `cursor.execute(...)` with binding parameters instead of string substitution.

# REVIEW: Target database platform is assumed to be Oracle based on SQL*Plus. If the database target platform is migrated to BigQuery, these PL/SQL packages (`BERT_MELDUNG`) must be translated into BigQuery procedures or a tracking table log handler.

### 6. CONTROL FLOW
The script consists of 9 function definitions. Below is the step-by-step logic of each function:

1. **`DWMSG_Fehlerbehandlung` (Error Handler)**
   - Captures shell exit status `$FehlerNr = $?`.
   - Calls `DWMSG_MeldeFehler` with the error code and error category "F" (Fatal).
   - Prints "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus" to stdout.
   - Calls `DWMSG_SetzeStatusAbbruch`.

2. **`DWMSG_SetzeStatusOK`**
   - Validates `$1` (`DWMSG_EintragsNr`). If empty, prints error to stdout and exits with status 1.
   - Invokes `sqlplus` to call Oracle procedure `BERT_MELDUNG.SetzeStatusOk`.

3. **`DWMSG_SetzeStatusAbbruch`**
   - Validates `$1` (`DWMSG_EintragsNr`). If empty, prints error and exits with status 1.
   - Invokes `sqlplus` to call Oracle procedure `BERT_MELDUNG.SetzeStatusAbbruch`.

4. **`DWMSG_ErmittleNr`**
   - Validates `$1` (`VarName`). If empty, prints error and exits with status 1.
   - Creates a temporary file path `/tmp/ErmittleNr_<PID>.lst`.
   - Runs `d_al_is_ermittlenr.sql` via `sqlplus` to generate a unique sequence number and write it to the file.
   - Reads the file, trims whitespaces using `tr -d ' '`, and removes the temporary file.
   - Assigns the sequence number back to the variable named in `VarName` using dynamic shell expansion (`eval`).

5. **`DWMSG_ErzeugeEintrag`**
   - Validates `$1` (`DWMSG_EintragsNr`). If empty, prints error and exits with status 1.
   - Calls `sqlplus` with `d_alis_spaufruf_p4.sql` to execute `BERT_MELDUNG.Erzeuge_Eintrag` with variables `EintragsNr`, `JobKennung`, `Programmname`, and `LogDatei`.

6. **`DWMSG_MeldeFehler`**
   - Validates `$1` (`DWMSG_EintragsNr`). If empty, prints error and exits with status 1.
   - Evaluates the number of arguments to select the correct SQL parameter wrapper (`d_alis_spaufruf_p3.sql` for 3 arguments, `p4` for 4, and `p5` for 5 arguments).
   - Calls `sqlplus` with the dynamic SQL wrapper path to execute `BERT_MELDUNG.Fehler` with the passed arguments.

7. **`DWMSG_Logdateiname`**
   - Combines environment variable `$DW_DIR_PROT`, `JobKennung`, current system time in format `YYYYMMDD_HHMM`, and `EintragsNr` to build a log file path.
   - Dynamically assigns this path to the variable named in `$1` using `eval`.

8. **`DWMSG_SetzeStichtagInfo`**
   - Validates that `$1` (EintragsNr), `$2` (Stichtag), and `$3` (StichtagFmt) are not empty. Exits with status 1 or 2 if validation fails.
   - Connects to database and executes `BERT_MELDUNG.SetzeZusatzInfos` using a date-formatted string.

9. **`DWMSG_AppendTimingInfos`**
   - Validates `$1` (EintragsNr) and `$3` (DateFormat). Exits with status 1 or 2 if validation fails.
   - Connects to database and appends text and current time formatted by `SYSDATE` to `SetzeZusatzInfos`.

### 7. ERROR HANDLING & EXIT CODES
- **Shell Validations:** Missing key positional arguments trigger validation messages (e.g., "Argh!, keine EintragsNummer...") printed to stdout, followed by exit codes `1` or `2`.
- **Database call failure:** The KornShell script does not perform explicit exit checks after each `sqlplus` execution, rely on the implicit exception handler.
- **Python translation:** Missing arguments inside functions will throw standard `ValueError` exceptions. Database operations will be enclosed in `try-except` blocks to catch and raise specific database client exceptions.

### 8. OUTPUTS / SIDE EFFECTS
- Updates database table records via the `BERT_MELDUNG` PL/SQL package (modifying status, logging errors, appending timing stats).
- Generated log files are identified by paths calculated in `DWMSG_Logdateiname`.
- Deletes temporary list files in `/tmp` (e.g. `/tmp/ErmittleNr_*.lst`).

### 9. BUSINESS SUMMARY
- **Tracking Registry:** Provides unique run registry IDs for any starting process to maintain cross-job execution history.
- **Job Status Monitoring:** Standardises state transitions (In Progress -> OK / Aborted) for tracking ETL progress.
- **Error Capturing:** Captures unexpected terminal failures and exit status, reporting them directly to database tables for operator notifications.
- **System Metrics & Timing:** Enables parent workflows to report processing dates (Stichtag) and performance execution run times.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import datetime
import shutil

# REVIEW: Assuming an Oracle database platform target. Replace with 'google.cloud.bigquery' if migrating to GCP.
# If migrating to BigQuery, PL/SQL packages must be replaced with equivalent SQL statements/procedures.
import oracledb

# Helper to get database connection
def get_db_connection():
    ora_user = os.environ.get("DW_ORAUSER")
    if not ora_user:
        raise ValueError("Environment variable 'DW_ORAUSER' is not set.")
    # In a production script, connection details would be extracted from the connection string or secure vault
    conn = oracledb.connect(dsn=ora_user)
    return conn

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, exit_code):
    """
    Handles shell trap errors. Logs error to DB and sets state to aborted.
    """
    k_unerw_fehler = 10
    
    # Report error to logging table
    dwmsg_melde_fehler(
        eintrags_nr, 
        typ="F", 
        fehler_nr=k_unerw_fehler, 
        zusatz1=f"ErrorCode ist: {exit_code}"
    )
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    """
    Sets status in logging table to OK.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Replaces call to d_alis_spaufruf_p1.sql with direct procedure call
            cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
            conn.commit()
    finally:
        conn.close()

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    """
    Sets status in logging table to aborted.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Replaces call to d_alis_spaufruf_p1.sql with direct procedure call
            cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
            conn.commit()
    finally:
        conn.close()

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr():
    """
    Retrieves and returns a unique log entry number.
    Replaces /tmp file writing and eval dynamic variables with standard Python return.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # REVIEW-STRUCT: Replaces d_al_is_ermittlenr.sql script logic.
            # Assumed sequence query or function execution logic.
            result_var = cursor.var(oracledb.NUMBER)
            # Example representation of sequence retrieval
            cursor.execute("SELECT seq_bert_meldung.NEXTVAL FROM dual")
            eintrags_nr = str(cursor.fetchone()[0]).strip()
            return eintrags_nr
    finally:
        conn.close()

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programm_name, log_datei):
    """
    Creates a new registration row in the logging table.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Replaces d_alis_spaufruf_p4.sql
            cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programm_name, log_datei])
            conn.commit()
    finally:
        conn.close()

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    Logs an error in the system.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Map dynamic parameters equivalent to d_alis_spaufruf_p3/p4/p5 logic
            cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
            conn.commit()
    finally:
        conn.close()

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    """
    Builds and returns a standardized log path.
    """
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "")
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = os.path.join(dw_dir_prot, f"{job_kennung}_{now_str}_{eintrags_nr}.log")
    return dateiname

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    """
    Sets supplementary runtime business date in database tracking table.
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
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Parse date in Python according to format
            # Converting Oracle formats to Python format strings if necessary
            # For direct SQL, we can let Oracle execute the parsed representation
            cursor.execute(
                f"BEGIN BERT_MELDUNG.SetzeZusatzInfos(:1, to_date(:2, :3)); COMMIT; END;", 
                [eintrags_nr, stichtag, stichtag_fmt]
            )
    finally:
        conn.close()

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    """
    Appends process execution metrics and current timing info to the DB tracking registry.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Appending custom formatted timestamp string to tracking registry
            # Representing Oracle to_char(SYSDATE, date_format) logic
            cursor.execute(
                f"BEGIN BERT_MELDUNG.SetzeZusatzInfos(:1, null, :2 || ' ' || to_char(SYSDATE, :3) || ' '); COMMIT; END;",
                [eintrags_nr, info_text, date_format]
            )
    finally:
        conn.close()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | KornShell utility script rewritten into Python to provide standardized logging, execution-state registry, and exception-handling functions for the BigQuery environment. |

---

### Job Dependencies
The following downstream jobs consume or source this logging and error-handling utility library. Because this is a core shared module, its downstream consumers must import the rewritten Python library once they are migrated:
*   **`DW.BERT_ABLAUFSTEUERUNG`** — *Not yet migrated*
*   **`DW.BERT_AUSD_BP_TA_MSISDN`** — *Not yet migrated*
*   **`DW.BERT_AUSD_BP_TA_P_BASISPROD`** — *Not yet migrated*
*   **`DW.BERT_AUSD_V_TA_PERIOD`** — *Not yet migrated*
*   **`DW.BERT_AUSD_V_TA_P_VERTRAG`** — *Not yet migrated*
*   **`DW.BERT_AUSD_V_TA_VERTRAG_TMP`** — *Not yet migrated*
*   **`DW.BERT_DROP_TEMP_TABLE`** — *Not yet migrated*
*   **`DW.BERT_P_ADRESSEN`** — *Not yet migrated*
*   **`DW.BERT_P_AUSTAUSCH`** — *Not yet migrated*
*   **`DW.BERT_P_GESCHAEFTSP`** — *Not yet migrated*
*   **`DW.BERT_P_RECH_EMPF`** — *Not yet migrated*
*   **`DW.BERT_RECHNUNGSDATEN`** — *Not yet migrated*

*Note: Integration wiring cannot be finalized until these downstream targets exist on Google Cloud.*

---

### Scheduling
*   **Trigger Event / Scheduler linkage:** This job is not directly triggered by any of the environment's standalone schedulers. It runs strictly inside other scheduled workflows (as an included/shared library module).
*   **Target Scheduling Strategy:** It must not be assigned a standalone DAG or schedule in Cloud Composer. Instead, it must be deployed as an importable helper module (`f_alis_msgerr.py`) within the shared execution path of the Cloud Composer environment, allowing other orchestrated Python operators/tasks to import and invoke its functions.

---

### Schedule & Variables — Must Be Retained
*   **Schedule Equivalence:** No standalone cron/event schedule is required, mirroring the legacy "include/shared module" behavior.
*   **Retained Variables:**
    *   No dynamic scheduler-set variables are directly fed to this utility; rather, it receives runtime values passed down by the caller scripts (e.g. `JobKennung`, `EintragsNr`).

---

### Lineage
*   **Downstream Consumers (Cross-Job Hand-off):**
    *   Sourced by parent wrapper scripts of the downstream ETL jobs (e.g., `DW.BERT_RECHNUNGSDATEN`, `DW.BERT_ABLAUFSTEUERUNG`) to maintain process execution histories, register execution tracking IDs, and alert on system aborts.
*   **External Oracle Interface:**
    *   Calls database procedures on the schema package `BERT_MELDUNG` (specifically `SetzeStatusOk`, `SetzeStatusAbbruch`, `Erzeuge_Eintrag`, `Fehler`, and `SetzeZusatzInfos`).

---

### External System Replacements
*   **Oracle PL/SQL Logging (`BERT_MELDUNG` package) ➔ BigQuery Audit Logging:**
    *   The legacy script calls out to local `.sql` templates (e.g., `d_alis_spaufruf_p1.sql`) to invoke PL/SQL procedures on `BERT_MELDUNG` inside Oracle. 
    *   On the target BigQuery platform, these logging operations must write directly to a BigQuery auditing/monitoring table (e.g., `audit_logs.bert_meldung`) using standard BigQuery DML operations (or BQ stored procedures mimicking the legacy PL/SQL package endpoints) via the BigQuery Python Client.
*   **`sqlplus` CLI ➔ BigQuery Python API Client:**
    *   Legacy SQL*Plus calls are replaced by queries executed through the Google Cloud Client Library (`google.cloud.bigquery`).

---

### Cross-File Dependencies
*   **Legacy SQL Wrapper Templates:**
    *   `d_alis_spaufruf_p1.sql` (retired / functionality moved to inline client code)
    *   `d_alis_spaufruf_p4.sql` (retired / functionality moved to inline client code)
    *   `d_alis_spaufruf_p3.sql` (retired / functionality moved to inline client code)
    *   `d_alis_spaufruf_p5.sql` (retired / functionality moved to inline client code)
    *   `d_al_is_ermittlenr.sql` (retired / replaced by direct sequence query execution)
*   These external helper templates are no longer required as standalone SQL files; their logic of compiling and wrapping PL/SQL procedures must be implemented as direct BigQuery API calls inside `f_alis_msgerr.py`.

---

### Target File Plan

| Target File Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | Python | `f_alis_msgerr.ksh` | Contains python functions to register execution runs, log status changes, record fatal errors, and capture execution timings in the target BigQuery audit log database. |

---

### Environment-Specific Values

The legacy shell script relies on a set of environment-specific paths and databases. These are mapped to global GCP variables below:

#### 1. GLOBAL (Environment-Wide Infrastructure)
*   **`GCP_PROJECT`**: Identifies the GCP project hosting the Composer and BigQuery environments. Sourced at runtime via:
    ```python
    import os
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    ```
*   **`BQ_DATASET`**: Identifies the target dataset containing the audit/logging tables (e.g., `audit_logs`). Sourced at runtime via:
    ```python
    import os
    BQ_DATASET = os.environ.get("BQ_DATASET")
    ```
*   **`GCS_LOG_BUCKET`** (replaces legacy `$DW_DIR_PROT`): Google Cloud Storage bucket path utilized to store runtime system log files. Sourced at runtime via:
    ```python
    import os
    GCS_LOG_BUCKET = os.environ.get("GCS_LOG_BUCKET")
    ```
*   **`GCS_ROOT_BUCKET`** (replaces legacy `$DW_DIR_ROOT`): Root GCS location for resources. Sourced at runtime via:
    ```python
    import os
    GCS_ROOT_BUCKET = os.environ.get("GCS_ROOT_BUCKET")
    ```

#### 2. JOB-SPECIFIC (Module/Method Parameters)
*   **`eintrags_nr`**: The specific log entry tracking number representing a process execution, passed dynamically as a method parameter.
*   **`job_kennung`**: The job ID string of the parent script, passed dynamically as a method parameter.
*   **`programm_name`**: The script or binary name execution identifier, passed dynamically as a method parameter.
*   **`log_datei`**: Calculated path to the log output, constructed dynamically inside `dwmsg_logdateiname` using Python's `datetime` module.

---

### Risks and Manual Steps

1.  **Integration of Downstream Jobs (Not Yet Migrated):**
    *   *Risk:* The 12 downstream consumers (e.g., `DW.BERT_RECHNUNGSDATEN`) are not yet migrated to the target GCP environment.
    *   *Action:* The integration wiring of the utility functions cannot be verified end-to-end until these downstream jobs are ported. Maintain stubs and mock test execution scripts to validate logging behavior independently.
2.  **Replacement of PL/SQL Package `BERT_MELDUNG`:**
    *   *Risk:* The script relies on an external Oracle database layer package `BERT_MELDUNG` to insert/update the status of execution logs.
    *   *Action:* A target BigQuery dataset (`audit_logs`) and audit table must be created with identical schemas to track runtime processes. BigQuery stored procedures must be created to mimic:
        *   `BERT_MELDUNG.SetzeStatusOk`
        *   `BERT_MELDUNG.SetzeStatusAbbruch`
        *   `BERT_MELDUNG.Erzeuge_Eintrag`
        *   `BERT_MELDUNG.Fehler`
        *   `BERT_MELDUNG.SetzeZusatzInfos`
3.  **Unique Run ID Generation (`DWMSG_ErmittleNr`):**
    *   *Risk:* The legacy system uses `d_al_is_ermittlenr.sql` to generate sequence numbers. BigQuery does not natively support standard auto-incrementing relational sequences.
    *   *Action:* This sequence lookup must be replaced in GCP. Options include querying an external metadata database, generating a UUID inside Python, or querying a BigQuery table using transactional updates.
4.  **Preservation of Output/Print Literals:**
    *   *Risk:* Under the Output/Print Literal Rule, any text emitted by stdout or stderr must be preserved verbatim.
    *   *Action:* In the Python implementation, the print statements must keep their exact legacy German messaging:
        *   `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`
        *   `"Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"`
        *   `"Argh!, keinen Variablennamen bei ErmittleNr angegeben"`
        *   `"Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`
        *   `"Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"`
        *   `"Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"`
        *   `"Argh!, keinen Stichtag angegeben!"`
        *   `"Argh!, Stichtagsangaben ohne Formatangaben k\u00f6nnen nicht verarbeitet werden!"` (preserving character representation of `können`)
        *   `"Argh!, Formatangabe erforderlich!"`
        *   `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`
        *   `"ErrorCode ist: "`

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
REASON: The script is a date utility library containing multiple shell functions with local variables, loops, arithmetic, and SQL*Plus database calls, which is best represented as a reusable Python utility module.

EVIDENCE
- Business logic found: KSH custom logic contains several utility functions for date validation, date comparison, leap year calculations, month-end determination, and date arithmetic.
- AWK: none
- SQL-expressible: No, because it is a procedural shell library with shell-level function declarations, control flow (loops, branches, local arrays), and dynamic environment variable assignments using `eval`.
- Non-SQL side effects: Writes temporary files under `/tmp`, throws custom shell/PL-SQL application errors, and modifies caller environment variables via `eval`.
- Against this verdict: A small subset of functions perform SQL queries via SQL*Plus to execute date checks on Oracle database instances, but the overall asset is fundamentally an orchestration/utility library, not a tabular data transformation script.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `h_alis_date.ksh` script is a utility library containing helper functions for date calculations, validation, and manipulation. It is designed to be sourced (imported) by other shell scripts in the Data Warehouse (DWH) environment. The utility performs various tasks such as finding the previous month, validating date formats, comparing dates, determining leap years, getting date ranges based on offsets, and performing date arithmetic, using a mix of local shell logic and inline/external Oracle SQL*Plus calls.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other DW shell scripts via `. h_alis_date.ksh` (or similar sourcing mechanisms). It is not directly run as a top-level UC4 job but acts as an underlying shared library.
   - UC4 native includes: None referenced in the script text.
   - Environment files sourced: Mentions in header comments that `.dw_init` must be run/sourced beforehand, or the environment variables `DW_DIR_ROOT` and `DW_ORAUSER` must be set.
     # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   Since this is a library, parameters are passed directly as arguments to the individual functions:
   - `DWDate_Vormonat`:
     - `$1` (VarName): Name of the environment variable in the calling script that will receive the output. (Used; surfaced as a function return value in Python).
     - `$2` (DWDate_FMT): Oracle date format mask (e.g., `YYYYMM`). (Used).
   - `DWDate_Datum_Check`:
     - `$1` (wert): Date string to validate. (Used).
     - `$2` (format): Date format to check against. (Used).
   - `DWDate_Datum_LE`:
     - `$1` (datum1): First date in `YYYYMMDD` format. (Used).
     - `$2` (datum2): Second date in `YYYYMMDD` format. (Used).
   - `DWDate_Gib_Zeitraum`:
     - `$1` (Offset): Integer offset value. (Used).
     - `$2` (Stufe): Date step unit (`Y`, `M`, or `D`). (Used).
     - `$3` (Format): Desired output date format. (Used).
     - `$4` (Var_Start): Target variable name for the start date. (Used; returned in Python).
     - `$5` (Var_Ende): Target variable name for the end date. (Used; returned in Python).
   - `LetzterTagDesMonats`:
     - `$1`: Date string in `YYYYMMDD` format. (Used).
   - `TageimMonat`:
     - `$1`: Year (YYYY). (Used).
     - `$2`: Month (MM). (Used).
   - `AddiereDatum`:
     - `$1`: Date string in `YYYYMMDD` format. (Used).
     - `$2`: Number of days to add (integer). (Used).

   KSH Declared Environment Parameters:
   - `DW_ORAUSER` (DB-connection-style parameter indicating Oracle database credentials/connection string)
   - `DW_DIR_ROOT` (Generic path parameter pointing to the root directory of the DW repository)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql $DWDate_tmpFile $DWDate_FMT </dev/null`
     - Purpose: Executes an external Oracle SQL script to determine the previous month and output it to a temporary file.
     - Target: Should become native Python date arithmetic (using `datetime` and `dateutil.relativedelta`), removing the database dependency entirely.
     - Launcher: Not a resolvable launcher for direct SQL conversion since the SQL script body is external.
     - # REVIEW-STRUCT: external SQL file d_alis_vormonat.sql not supplied — behaviour unknown

   - `sqlplus -s` (with inline EOF blocks in `DWDate_Datum_Check` and `DWDate_Datum_LE`)
     - Purpose: Connects to Oracle to parse/validate date patterns or perform date comparison logic.
     - Target: Replace with native Python parsing (`datetime.strptime`).
     - Dialect: Oracle SQL / PL-SQL.

   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql ...`
     - Purpose: Gets a date range start and end based on current date, offset, and step unit.
     - Target: Replace with native Python date/time arithmetic.
     - # REVIEW-STRUCT: external SQL file d_alis_datum_zeitraum.sql not supplied — behaviour unknown

5. EMBEDDED SQL
   - **SQL 1** (within `DWDate_Datum_Check`):
     - Source: Inline in shell function
     - SQL Text:
       ```sql
       WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
       SET HEADING OFF;

       -- Implizite Ueberpruefung, ob $Wert ein Datum des Format $format ist
       select to_date('$wert','$format') from dual;
       ```
     - Statement Type: SELECT
     - Table touched: `dual`
     - Dialect: Oracle (unambiguous via `to_date(..., ...)` and `from dual`).

   - **SQL 2** (within `DWDate_Datum_LE`):
     - Source: Inline PL/SQL block in shell function
     - SQL Text:
       ```sql
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
       ```
     - Statement Type: PL/SQL Anonymous Block
     - Table touched: None
     - Dialect: Oracle PL/SQL (unambiguous via `DECLARE/BEGIN/END` and `raise_application_error`).

6. CONTROL FLOW
   Each function is self-contained. The logical steps of execution inside the functions are:
   1. **DWDate_Vormonat**:
      - Define temp file path `/tmp/h_alis_date_..._pid.tmp`.
      - Invoke SQL*Plus to execute `d_alis_vormonat.sql`, outputting results to the temp file.
      - Read the temp file content and assign it to the dynamically named variable via `eval`.
      - Clean up the temp file (Note: code has a bug `rm -f $DWDate_FMT` instead of `$DWDate_tmpFile`, which should be corrected to cleanup the temp file).
   2. **DWDate_Datum_Check**:
      - Validate that exactly 2 arguments are provided; return 1 on failure.
      - Execute inline Oracle SQL to parse the date string using the provided format.
      - Propagate the exit code (`$?`) of SQL*Plus.
   3. **DWDate_Datum_LE**:
      - Validate that exactly 2 arguments are provided; return 1 on failure.
      - Execute inline Oracle PL/SQL block. If the first date is greater than the second date, raise an Oracle application error (`-20422`).
      - Propagate the exit code (`$?`) of SQL*Plus.
   4. **DWDate_Gib_Zeitraum**:
      - Validate that exactly 5 arguments are provided; return 1 on failure.
      - Generate a timestamped temp file `/tmp/tmp_..._pid.tmp`.
      - Invoke SQL*Plus executing `d_alis_datum_zeitraum.sql` with offset, step, and format arguments.
      - Check if the output file contains exactly one line with pattern `DWH_Ergebnis;`. If not, print an error and return 1.
      - Parse the start and end dates from the semicolon-delimited output line.
      - Dynamically assign start and end dates to caller-specified variable names via `eval`.
      - Clean up the temp file.
   5. **LetzterTagDesMonats**:
      - Substring the `YYYYMMDD` input into Year, Month, Day.
      - Compute leap year status to set February days (29 if leap year, 28 otherwise).
      - Maintain array of month lengths.
      - Verify if the input day matches the expected last day of that month. Return 0 if yes, 1 if no.
   6. **TageimMonat**:
      - Determine leap year status of given year.
      - Output the total days in the specified month.
   7. **AddiereDatum**:
      - Substring `YYYYMMDD` input into Year, Month, Day.
      - Perform raw addition of offset days to the day component.
      - Loop with `while` to progressively subtract the days in the current month and increment the month value until the day falls within the valid range of the current month.
      - Loop nested `while` to handle month roll-over exceeding 12 (increment year, subtract 12 from month).
      - Pad Year (4 digits), Month (2 digits), and Day (2 digits) to construct the final `YYYYMMDD` output string.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments in shell helper functions return `1`.
   - SQL*Plus runtime errors are handled via `WHENEVER SQLERROR EXIT FAILURE ROLLBACK`, propagating a non-zero exit status back to the shell caller.
   - Pattern match errors in `DWDate_Gib_Zeitraum` print an internal error to stdout and return `1`.
   - In Python, these will map directly to native exception handling (`ValueError`, standard assertions) or clean boolean/return value contracts.

8. OUTPUTS / SIDE EFFECTS
   - Standard output messages for error scenarios.
   - Dynamic environment variables created/modified in the parent shell environment (via `eval`). In Python, this is cleanly handled by returning structured dictionaries, tuples, or class objects from the utility functions.
   - Temporary file generation under `/tmp` (not needed in a native Python refactoring).

9. BUSINESS SUMMARY
   - Provides standardized date manipulation utility logic across DW processes.
   - Validates that date parameters conform to specific formats before processing.
   - Asserts chronological execution constraints (e.g., ensuring start date is less than or equal to end date).
   - Performs date offset calculations (generating standard reporting intervals like month-start, month-end, year-start, year-end).
   - Implements native leap-year-safe date addition and month-end validation logic.

=======================================================================================
PYTHON PSEUDOCODE OUTLINE
=======================================================================================

```python
# date_utils.py
# Modern Python translation of the h_alis_date.ksh library.
# This module replaces all database-dependent date operations with standard Python datetime logic.

import os
import sys
import datetime
from typing import Tuple, Optional

# REVIEW-STRUCT: environment file .dw_init variables are not resolved.
# We assume standard environment/module configurations are loaded externally.

# Helper to check if a year is a leap year
def _is_leap_year(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

# Replaces TageimMonat
def tage_im_monat(year: int, month: int) -> int:
    # Step 1: Initialize month lengths (index 0 is a dummy value)
    feb_days = 29 if _is_leap_year(year) else 28
    month_lengths = [0, 31, feb_days, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return month_lengths[month]

# Replaces LetzterTagDesMonats
def letzter_tag_des_monats(date_str: str) -> bool:
    # Step 1: Parse the date_str input (format YYYYMMDD)
    try:
        year = int(date_str[0:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
    except (ValueError, IndexError):
        raise ValueError(f"Invalid date string format: {date_str}. Expected YYYYMMDD.")
    
    # Step 2: Compare input day with the last day of that month
    return day == tage_im_monat(year, month)

# Replaces AddiereDatum
def addiere_datum(date_str: str, days_to_add: int) -> str:
    # Step 1: Parse input components
    try:
        year = int(date_str[0:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
    except (ValueError, IndexError):
        raise ValueError(f"Invalid date string format: {date_str}. Expected YYYYMMDD.")
        
    # Step 2: Perform addition natively using datetime for robust calendar safety
    # The original script implemented a manual shift loop, which we modernize here.
    current_date = datetime.date(year, month, day)
    new_date = current_date + datetime.timedelta(days=days_to_add)
    
    # Step 3: Format and return as YYYYMMDD
    return new_date.strftime("%Y%m%d")

# Replaces DWDate_Datum_Check
def dw_date_datum_check(wert: str, format_mask: str) -> bool:
    # Step 1: Map Oracle date formats to Python strptime format codes
    # # REVIEW: target database validation simulated via Python strptime. Ensure format mapping is complete.
    format_mapping = {
        "YYYYMMDD": "%Y%m%d",
        "YYYYMM": "%Y%m",
        "DD.MM.YYYY": "%d.%m.%Y",
        # Add further mappings as discovered in the source environment
    }
    py_format = format_mapping.get(format_mask, format_mask)
    
    # Step 2: Attempt parsing to validate date authenticity
    try:
        datetime.datetime.strptime(wert, py_format)
        return True
    except ValueError:
        return False

# Replaces DWDate_Datum_LE
def dw_date_datum_le(datum1_str: str, datum2_str: str) -> bool:
    # Step 1: Parse date inputs (explicitly YYYYMMDD)
    try:
        d1 = datetime.datetime.strptime(datum1_str, "%Y%m%d")
        d2 = datetime.datetime.strptime(datum2_str, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"Failed to parse dates in YYYYMMDD format: {e}")
        
    # Step 2: Evaluate condition. If datum1 > datum2, raise exception equivalent to PL/SQL error -20422
    if d1 > d2:
        raise ValueError(f"Application Error -20422: Datum {datum1_str} ist groesser als {datum2_str}")
        
    return True

# Replaces DWDate_Vormonat
# # REVIEW-STRUCT: external SQL file d_alis_vormonat.sql logic replaced with native Python logic
def dw_date_vormonat(format_mask: str) -> str:
    # Step 1: Compute previous month relative to current system date
    today = datetime.date.today()
    # Subtract to get prior month
    first_of_this_month = today.replace(day=1)
    last_of_prior_month = first_of_this_month - datetime.timedelta(days=1)
    
    # Step 2: Convert Oracle format mask to Python style
    format_mapping = {
        "YYYYMM": "%Y%m",
        "YYYYMMDD": "%Y%m%d",
    }
    py_format = format_mapping.get(format_mask, "%Y%m")
    
    return last_of_prior_month.strftime(py_format)

# Replaces DWDate_Gib_Zeitraum
# # REVIEW-STRUCT: external SQL file d_alis_datum_zeitraum.sql logic replaced with native Python logic
def dw_date_gib_zeitraum(offset: int, stufe: str, format_mask: str) -> Tuple[str, str]:
    # Step 1: Establish base today's date
    start_date = datetime.date.today()
    
    # Step 2: Perform interval calculations based on 'stufe' (Y, M, D)
    stufe = stufe.upper()
    if stufe == 'D':
        end_date = start_date + datetime.timedelta(days=offset)
    elif stufe == 'M':
        # Align to start of current month and shift
        # For months, the logic establishes first day and last day boundary
        first_of_start = start_date.replace(day=1)
        
        # Approximate offset logic (reproducing business rules of d_alis_datum_zeitraum.sql)
        # Shift month count by offset
        total_months = first_of_start.month - 1 + offset
        new_year = first_of_start.year + (total_months // 12)
        new_month = (total_months % 12) + 1
        
        first_of_end = datetime.date(new_year, new_month, 1)
        # Find last day of target month (Ultimo)
        last_day_of_month = tage_im_monat(new_year, new_month)
        end_date = datetime.date(new_year, new_month, last_day_of_month)
        start_date = first_of_start
    elif stufe == 'Y':
        # Align to Neujahr (Jan 1) and Sylvester (Dec 31)
        start_date = datetime.date(start_date.year, 1, 1)
        target_year = start_date.year + offset
        end_date = datetime.date(target_year, 12, 31)
    else:
        raise ValueError(f"Unsupported Stufe value: {stufe}. Must be Y, M, or D.")
        
    # Step 3: Format output strings
    format_mapping = {
        "YYYYMMDD": "%Y%m%d",
        "YYYYMM": "%Y%m"
    }
    py_format = format_mapping.get(format_mask, "%Y%m%d")
    
    return start_date.strftime(py_format), end_date.strftime(py_format)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py` | Convert KSH date manipulation utility library functions to native Python functions. Eliminates dependency on external Oracle SQL*Plus calls and temporary files. |

---

### Job Dependencies
*   **Upstream Jobs:** No direct upstream jobs are specified in the scheduling or dependency context.
*   **Downstream Jobs:** 
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

*Wiring on BigQuery / Cloud Composer:* These downstream jobs cannot be fully wired or tested until they are migrated. Once migrated, they will import the converted Python module `h_alis_date.py` directly to utilize its date validation, check, and manipulation methods.

---

### Scheduling
*   This job is **NOT** directly triggered by any of the environment's primary schedulers.
*   It acts strictly as an include/shared module (utility library).
*   **Target Rule:** Do not give this migrated artifact its own standalone schedule or Airflow DAG wrapper; it must remain a pure callable/importable Python module.

---

### Schedule & Variables
*   **Trigger Event:** None. Inherited/imported context only.
*   **Variables:** No scheduler-set variables are directly provided to this script. It relies on standard system parameters.

---

### Lineage
*   **Upstream Data Sources:**
    *   `TABLE:DUAL` (Oracle system table used via SQL*Plus for execution of date expressions like `to_date` and `sysdate`).
*   **Downstream Consumers:**
    *   Multiple downstream processes (listed under Job Dependencies) that source this library to parse dates, check leap years, compare dates, and compute start/end period bounds.

---

### External System Replacements
*   **SQL*Plus and Oracle DB Access (`DUAL`):** Completely replaced by Python's native `datetime`, `calendar`, and `dateutil` packages. Standardizing operations such as checking if a date is less than or equal to another, validating date formats, and performing date additions directly in memory. This eliminates database round-trips and `/tmp` filesystem overhead.

---

### Cross-File Dependencies
*   **Downstream Callers:** Sibling migration tasks converting the downstream shell wrappers (e.g., KSH wrappers for the listed `DW.BERT_*` jobs) must replace their sourcing syntax `. h_alis_date.ksh` with Python imports: `import h_alis_date` or `from h_alis_date import ...`.

---

### Target File Plan

*   **File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py`
    *   **Language:** Python
    *   **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
    *   **Purpose:** Reusable date manipulation library. It encapsulates functions: `dw_date_vormonat`, `dw_date_datum_check`, `dw_date_datum_le`, `dw_date_gib_zeitraum`, `letzter_tag_des_monats`, `tage_im_monat`, and `addiere_datum`. All legacy terminal prints and error exceptions (including specific application error text) must be strictly maintained in their native German text to protect downstream logging parsing.

---

### Environment-Specific Values

1.  **`DW_ORAUSER` [GLOBAL]**
    *   *Role:* Oracle database connection credentials.
    *   *Classification:* GLOBAL.
    *   *Handling:* Retired/Obsolete for this module since database queries are fully replaced by native Python `datetime` calculations.
2.  **`DW_DIR_ROOT` [GLOBAL]**
    *   *Role:* Standard root directory of the DWH source tree.
    *   *Classification:* GLOBAL.
    *   *Handling:* Retired/Obsolete. Relocated Python files will resolve module layouts natively using Python module search paths (`sys.path` or package-level absolute/relative imports).

---

### Risks & Manual Steps

*   **Risk: External SQL Dependencies (`d_alis_vormonat.sql` / `d_alis_datum_zeitraum.sql`)**
    *   *Description:* The functions `DWDate_Vormonat` and `DWDate_Gib_Zeitraum` called external Oracle SQL files. While the expected logical behavior (calculating last-month and range offsets) is fully converted to standard Gregorian calendar logic in Python, any proprietary enterprise holiday offsets, fiscal calendars, or bespoke date behaviors embedded in those external SQL files are unconfirmed.
    *   *Manual Action:* Verify that the target environment does not use custom accounting calendars or corporate holidays inside `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`.
*   **Risk: Downstream Integration Blockers**
    *   *Description:* All 12 downstream dependencies are not yet migrated.
    *   *Manual Action:* Wire and test integration once the downstream orchestration layers (e.g. Composer DAGs) are generated.
*   **Downstream Dependency Tracking:**
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_ABLAUFSTEUERUNG` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_AUSD_BP_TA_MSISDN` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_AUSD_BP_TA_P_BASISPROD` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_AUSD_V_TA_PERIOD` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_AUSD_V_TA_P_VERTRAG` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_AUSD_V_TA_VERTRAG_TMP` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_DROP_TEMP_TABLE` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_P_ADRESSEN` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_P_AUSTAUSCH` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_P_GESCHAEFTSP` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_P_RECH_EMPF` is not yet migrated.
    *   *Wiring Blocked:* Downstream consumer `DW.BERT_RECHNUNGSDATEN` is not yet migrated.

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
REASON: The script is a helper utility library containing complex parameter validation, conversion, and date arithmetic functions that manipulate environment variables via dynamic eval, requiring a Python conversion.

EVIDENCE
- Business logic found: KSH custom logic. The script defines a set of utility functions for parsing, validating, and converting application parameters, handling system-to-code mapping, checking date ranges, and calculating offsets.
- AWK: none
- SQL-expressible: no, this is orchestration-level parameter parsing and validation logic that modifies environment state, not database data transformation.
- Non-SQL side effects: Dynamically modifies script environment variables via 'eval', writes temporary log files, and invokes external DWDate utility functions.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `h_alis_parameter.ksh` is a helper/utility module (denoted by the `h_` prefix) containing reusable functions for parameter handling in the IS (Information System) processing framework. It converts descriptive system and key-figure (Kennzahl) names to standardized short codes, validates inputs, and performs date interval calculations. It is sourced by other scripts in the pipeline rather than being run as a standalone executable.

### 2. INVOCATION CONTEXT
- **Sourced by:** Other shell scripts in the IS/allgemein/is/util pipeline (such as import or processing scripts). No specific UC4 job context is provided in the extraction, but it's part of the general utility library.
- **UC4 Native Includes:** None referenced.
- **Environment Files Sourced:** None. It defines variables `ModulName="alis_parameter"` and `ModulVersion="V3.0.9"`.

### 3. PARAMETERS / INPUTS
Since this is a library, parameters are passed to individual functions:
- **`pruefeParameterGesetzt`**:
  - `$1` (param_name): Description of the parameter.
  - `$2` (param_var): Name of the environment variable containing the parameter value.
- **`konvertiereKennzahl`**:
  - `$1` (VarName): Name of the environment variable containing the key figure to be converted (in-out parameter).
- **`konvertiereSystem`**:
  - `$1` (VarName): Name of the environment variable containing the system name to be converted (in-out parameter).
- **`konvertiereSDName`**:
  - `$1` (VarName): Name of the environment variable containing the master data system name to be converted (in-out parameter).
- **`konvertiereAufbStufeXtra`**:
  - `$1` (VarName): Name of the environment variable containing the stage name to be converted (in-out parameter).
- **`pruefeSystemKennzahl`**:
  - `$1` (System): Short name of the source system.
  - `$2` (Kennzahl): Short name of the key figure.
- **`gibBereich`**:
  - `$1` (Kennzahl): Short name of the key figure.
  - `$2` (VarBereich): Name of the environment variable to store the resulting domain (Bereich) (output parameter).
- **`gibIntervall`**:
  - `$1` (Kennzahl): Short name of the key figure.
  - `$2` (VarIntervall): Name of the environment variable to store the resulting interval (t/m) (output parameter).
- **`pruefeZeitraum`**:
  - `$1` (Anfang): Start date (YYYYMMDD).
  - `$2` (Ende): End date (YYYYMMDD).
- **`pruefeZahlPositiv`**:
  - `$1` (p_Zahl): Number to check.
  - `$2` (p_ParameterName): Parameter name for logging.
- **`pruefeZeitParameter`**:
  - `$1` (p_Anfangsdatum): Start date.
  - `$2` (p_Endedatum): End date.
  - `$3` (p_ZeitOffset): Time span offset.
- **`konvertiereZeitspanne`**:
  - `$1` (p_VarAnfang): Name of the environment variable to store the calculated start date.
  - `$2` (p_VarEnde): Name of the environment variable to store the calculated end date.
  - `$3` (p_Spanne): Numerical time span.
  - `$4` (p_Kennzahl): Key figure code.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **`basename $0`**, **`date`** (standard utilities used to generate temp files).
- **`DWDate_Datum_Check` / `DWDate_Datum_LE` / `DWDate_Gib_Zeitraum`**:
  - These are external date utility commands assumed to be part of the framework's date module.
  - # REVIEW-STRUCT: external date utility commands [DW_Date_Datum_Check, DWDate_Datum_Check, DWDate_Datum_LE, DWDate_Gib_Zeitraum] invoked — their exact internal implementation details are not supplied. However, their behavior is standard date validation/calculation and should be implemented natively in Python.

### 5. EMBEDDED SQL
- None observed.

### 6. CONTROL FLOW
The script initializes global metadata variables and defines 12 distinct functions:
1. **Initialize Global Module Metadata**: Sets `ModulName="alis_parameter"` and `ModulVersion="V3.0.9"`.
2. **`pruefeParameterGesetzt`**: Checks if the named variable is non-empty. If empty, sets error code `194`.
3. **`konvertiereKennzahl`**: Translates descriptive key-figure names to 3-letter codes using a large mapping (`case` statement).
4. **`konvertiereSystem`**: Converts descriptive source systems (e.g. `sap`, `carmen`, `sigma`) to lowercase.
5. **`konvertiereSDName`**: Converts descriptive master-data source systems (e.g. `rahmenvertrag` -> `rv`) to standardized codes.
6. **`konvertiereAufbStufeXtra`**: Converts Xtra stage names to normalized codes (`zusammenfuehrung` -> `mrg`, `befuellung` -> `fill`).
7. **`pruefeSystemKennzahl`**: Validates compatibility between a given system and a key figure.
8. **`gibBereich`**: Categorizes a key figure into a functional domain (`tn`, `us`, `gd`, `sd`, `md`).
9. **`gibIntervall`**: Categorizes a key figure into a processing frequency (daily `t` or monthly `m`).
10. **`pruefeZeitraum`**: Validates start/end date format (YYYYMMDD) and logical ordering.
11. **`pruefeZahlPositiv`**: Assures that a parameter value is a non-negative integer.
12. **`pruefeZeitParameter`**: Validates mutual exclusivity between a date range (start/end) and a relative time-span offset.
13. **`konvertiereZeitspanne`**: Calculates actual start and end dates from a relative numeric offset.

### 7. ERROR HANDLING & EXIT CODES
- **KornShell implementation**: Uses global state variables `ErrNr` and `ErrArg` to track error codes. Every function begins with a guard clause checking `if [ $ErrNr -ne 0 ]; then return; fi` to simulate exception propagation.
- **Python target strategy**: Use standard Python exception handling (`ValueError` or a custom `ParameterError` exception) or structured return types rather than global state variables.

### 8. OUTPUTS / SIDE EFFECTS
- The original script uses `eval "$VarName=$Value"` to dynamically set variables in the caller's environment.
- In Python, functions should natively **return** values (as strings, tuples, or objects) to the caller instead of dynamically mutating variable tables.

### 9. BUSINESS SUMMARY
- Centralizes parameter parsing and validation across the IS (Information System) reporting ecosystem.
- Converts user-friendly or legacy descriptive names for systems and key performance indicators into strict technical short codes.
- Enforces business rules regarding valid system/metric combinations.
- Provides consistent date interval and validation calculations across standard files and runs.

=======================================================================================
PYTHON IMPLEMENTATION (PSEUDOCODE)
=======================================================================================

```python
import os
import sys
import tempfile
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# Module Metadata
MODUL_NAME = "alis_parameter"
MODUL_VERSION = "V3.0.9"

class ParameterError(ValueError):
    """Custom exception simulating ErrNr and ErrArg mapping."""
    def __init__(self, err_nr, err_arg):
        super().__init__(f"Error {err_nr}: {err_arg}")
        self.err_nr = err_nr
        self.err_arg = err_arg

# Step 1: Check if parameter value is set
def pruefe_parameter_gesetzt(param_name, param_value):
    if not param_name or not param_value:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} pruefeParameterGesetzt")
    
    if not str(param_value).strip():
        raise ParameterError(194, param_name)

# Step 2: Convert Key Figure (Kennzahl) to short code
KENNZAH_MAP = {
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
    "glaengenintervall": "glint",
}

def konvertiere_kennzahl(kennzahl_desc):
    if not kennzahl_desc:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereKennzahl")
    
    lower_desc = kennzahl_desc.lower()
    if lower_desc in KENNZAH_MAP:
        return KENNZAH_MAP[lower_desc]
    else:
        raise ParameterError(198, kennzahl_desc)

# Step 3: Convert Source System name
VALID_SYSTEMS = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

def konvertiere_system(system_desc):
    if not system_desc:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereSystem")
    
    lower_sys = system_desc.lower()
    if lower_sys in VALID_SYSTEMS:
        return lower_sys
    else:
        raise ParameterError(195, f"Unbekannte Datenherkunft {system_desc} !")

# Step 4: Convert Master Data System Name (Stammdaten)
SD_SYSTEM_MAP = {
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

def konvertiere_sd_name(sd_desc):
    if not sd_desc:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereSDSystem")
    
    lower_sd = sd_desc.lower()
    if lower_sd in SD_SYSTEM_MAP:
        return SD_SYSTEM_MAP[lower_sd]
    else:
        raise ParameterError(195, f"Unbekannte Stammdaten-Datenherkunft {sd_desc} !")

# Step 5: Convert Xtra Staging Name
STAGE_MAP = {
    "zusammenfuehrung": "mrg",
    "befuellung": "fill"
}

def konvertiere_aufb_stufe_xtra(stage_desc):
    if not stage_desc:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} konvertiereAufbStufeXtra")
    
    lower_stage = stage_desc.lower()
    if lower_stage in STAGE_MAP:
        return STAGE_MAP[lower_stage]
    else:
        raise ParameterError(195, f"Unbekannte Stufenangabe {stage_desc} !")

# Step 6: Validate System and Key Figure compatibility
def pruefe_system_kennzahl(system, kennzahl):
    if not system or not kennzahl:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} pruefeSystemKennzahl")

    invalid = False
    
    if system != "nnv" and kennzahl in ("tvd", "lkl"):
        invalid = True
    elif system == "carmen":
        if kennzahl in ("twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"):
            invalid = True
    elif system == "sap":
        if kennzahl in ("zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"):
            invalid = True
    elif system == "dpps":
        if kennzahl in ("twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"):
            invalid = True
    elif system == "ctel":
        if kennzahl not in ("abg", "bst", "zug", "twe"):
            invalid = True
    elif system == "xtra":
        if kennzahl != "rst":
            invalid = True
    elif system == "d1":
        if kennzahl in ("gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"):
            invalid = True
    elif system == "nnv":
        if kennzahl not in ("tvd", "lkl"):
            invalid = True
    elif system == "dwh":
        if kennzahl != "mds":
            invalid = True
    elif system == "brunet":
        if kennzahl not in ("d1n", "rub", "lmo"):
            invalid = True
    elif system == "sigma":
        sigma_valid = {
            "nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa",
            "gtyp", "basisd", "natint", "glint"
        }
        if kennzahl not in sigma_valid:
            invalid = True

    if invalid:
        raise ParameterError(195, f"Ungueltige Kombination {system} {kennzahl}")

# Step 7: Get Domain Category (Bereich)
DOMAINS = {
    "tn": ["abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"],
    "us": ["gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"],
    "gd": ["tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"],
    "sd": ["ksd", "bwa"],
    "md": ["mds"]
}

def gib_bereich(kennzahl):
    if not kennzahl:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} gibBereich")
    
    for domain, k_list in DOMAINS.items():
        if kennzahl in k_list:
            return domain
            
    raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} gibBereich - Kuerzel '{kennzahl}' unbekannt")

# Step 8: Get Processing Interval (t=Daily, m=Monthly)
INTERVALS = {
    "t": ["abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"],
    "m": ["bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"]
}

def gib_intervall(kennzahl):
    if not kennzahl:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} gibIntervall")
        
    for interval, k_list in INTERVALS.items():
        if kennzahl in k_list:
            return interval
            
    raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} gibIntervall - Kuerzel '{kennzahl}' unbekannt")

# Step 9: Validate Date Range (Anfang / Ende)
def pruefe_zeitraum(anfang, ende, date_format="%Y%m%d"):
    if not anfang or not ende:
        raise ParameterError(196, f"{MODUL_NAME} {MODUL_VERSION} pruefeZeitraum")
        
    try:
        dt_anfang = datetime.strptime(anfang, date_format)
    except ValueError:
        raise ParameterError(195, f"Anfangdatum entspricht nicht dem Format YYYYMMDD")
        
    try:
        dt_ende = datetime.strptime(ende, date_format)
    except ValueError:
        raise ParameterError(195, f"Endedatum entspricht nicht dem Format YYYYMMDD")
        
    if dt_anfang > dt_ende:
        raise ParameterError(195, "Anfangsdatum ist nicht kleiner gleich Endedatum")

# Step 10: Validate Numeric Value is positive
def pruefe_zahl_positiv(value, param_name):
    try:
        numeric_val = int(value)
    except ValueError:
        raise ParameterError(195, f"Parameter {param_name} ist kein numerischer Wert")
        
    if numeric_val < 0:
        raise ParameterError(195, f"Parameter {param_name} muss groesser gleich 0 sein")

# Step 11: Validate Date Parameters (Mutual exclusivity of explicit dates vs relative offset)
def pruefe_zeit_parameter(anfang, ende, zeit_offset):
    # Case 1: relative offset is set, explicit dates must be empty
    if zeit_offset:
        if not anfang and not ende:
            pruefe_zahl_positiv(zeit_offset, "Zeitspanne")
            return
        else:
            raise ParameterError(195, "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden")
    
    # Case 2: relative offset is missing, both explicit dates must be set and valid
    else:
        if anfang and ende:
            pruefe_zeitraum(anfang, ende)
        else:
            if not anfang and not ende:
                raise ParameterError(195, "Datumswerte oder Zeitspanne fehlen")
            else:
                raise ParameterError(195, "Sowohl Anfang- als auch Endedatum muessen angegeben werden")

# Step 12: Calculate relative date range
# # REVIEW-STRUCT: DWDate_Gib_Zeitraum internal calculation semantics — confirm if calculation is relative to today or another business date.
def konvertiere_zeitspanne(spanne, kennzahl, reference_date=None):
    if reference_date is None:
        reference_date = date.today()
        
    offset_unit = "D"
    if kennzahl == "bst":
        offset_unit = "M"
        
    # Standard implementation using python's relativedelta:
    try:
        spanne_val = int(spanne)
        if offset_unit == "D":
            # Assuming interval ends today and starts spanne days ago
            anfangs_datum = reference_date - relativedelta(days=spanne_val)
        else:
            # Monthly offset
            anfangs_datum = reference_date - relativedelta(months=spanne_val)
            
        ended_datum = reference_date
        return anfangs_datum.strftime("%Y%m%d"), ended_datum.strftime("%Y%m%d")
    except Exception as e:
        raise ParameterError(85, f"DWDate_Gib_Zeitraum error: {str(e)}")
```

# Migration Design Document

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `allgemein/is/util/bin/h_alis_parameter.py` | Migrated to a Python library utility module containing standardized parameter parsing, validation, and mapping functions to be imported by downstream Python ETL/orchestration scripts. |

***

## ADD CONTEXT THE MCP COULD NOT SEE

### Job Dependencies
The legacy script is a shared utility module. While it has no upstream triggers, it is sourced by several downstream ETL processing runs. These downstream workflows must be configured to import the migrated Python library (`allgemein/is/util/bin/h_alis_parameter.py`) instead of executing or sourcing the legacy shell script.

**Downstream Consumers (not yet migrated):**
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

---

### Scheduling
* **Schedule Rule:** This job is NOT directly triggered by any scheduler. It operates as an include/shared library. 
* **Target Scheduling Construct:** Do NOT create a standalone Airflow DAG or standalone Cloud Composer schedule for this artifact. It must remain a callable/importable Python module (`h_alis_parameter.py`) stored in a shared path accessible to other Python operators in Cloud Composer.

---

### External System Replacements
* **Legacy DWDate Helpers:** The shell script invokes external commands `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` for date checking and calculation. In the target BigQuery/Composer environment, these must be replaced by standard Python native date modules (`datetime`, `dateutil.relativedelta`) or a migrated shared library of the DWDate utility.

---

### Cross-File Dependencies
* **Function Imports:** Downstream Python tasks (migrated from legacy `.ksh` wrappers) will import functions from this module, specifically:
  * `pruefe_parameter_gesetzt`
  * `konvertiere_kennzahl`
  * `konvertiere_system`
  * `konvertiere_sd_name`
  * `pruefe_system_kennzahl`
  * `gib_bereich`
  * `gib_intervall`
  * `pruefe_zeit_parameter`
  * `konvertiere_zeitspanne`

---

### Target File Plan
* **Target Path:** `allgemein/is/util/bin/h_alis_parameter.py`
  * **Language:** Python 3.x
  * **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`

---

### Environment-Specific Values
The module defines general metadata constants. No global infrastructure connection properties are defined in this utility:
* `ModulName` — **JOB-SPECIFIC**; value `"alis_parameter"` is set inline.
* `ModulVersion` — **JOB-SPECIFIC**; value `"V3.0.9"` is set inline.

---

### Risks & Manual Actions
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_ABLAUFSTEUERUNG** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_AUSD_BP_TA_MSISDN** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_AUSD_BP_TA_P_BASISPROD** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_AUSD_V_TA_PERIOD** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_AUSD_V_TA_P_VERTRAG** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_AUSD_V_TA_VERTRAG_TMP** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_DROP_TEMP_TABLE** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_P_ADRESSEN** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_P_AUSTAUSCH** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_P_GESCHAEFTSP** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_P_RECH_EMPF** — the wiring cannot be finalized until it exists.
* **DOWNSTREAM: NOT YET MIGRATED — DW.BERT_RECHNUNGSDATEN** — the wiring cannot be finalized until it exists.
* **External Date Libraries:** Ensure that the custom `DWDate_*` functions are either mapped cleanly to Python native `datetime` libraries or compiled into a standard shared python module and imported dynamically. Verify that the time-span calculations match the exact semantic expectations of the caller scripts.

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
REASON: The script defines a reusable shell helper function that validates parameters, checks file readability, and executes SQL*Plus, requiring Python's orchestration and subprocess management capabilities.

EVIDENCE
- Business logic found: KSH custom logic inside the `starteSQLSkript` function, which performs parameter validation, verifies file readability, and manages SQL*Plus subprocess execution.
- AWK: none
- SQL-expressible: no, it contains shell-specific control flow, filesystem checks (`-r`), and external execution of SQL*Plus, which cannot be represented as standard SQL.
- Non-SQL side effects: checks local filesystem readability (`[ ! -r $p_Skript ]`), redirects process input from `/dev/null`, and returns subprocess status codes.
- Against this verdict: none; this is a pure orchestration utility library.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a reusable KornShell utility library (`h_alis_sqlplus.ksh`) that provides a standardized function (`starteSQLSkript`) to execute SQL*Plus scripts. It performs pre-execution validation checks, verifying that necessary arguments are supplied and that the target SQL script is readable before attempting a database connection. The utility serves to encapsulate error logging and exit code propagation for database maintenance and extraction workflows.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other KSH scripts in the batch environment to gain access to the `starteSQLSkript` function. It is not invoked directly as a standalone UC4 job.
   - UC4 native includes (:inc ...): none referenced in the provided source.
   - Environment files sourced: none sourced directly in this snippet, but it expects `DW_ORAUSER` and potentially logging functions to be exported in the calling environment.

3. PARAMETERS / INPUTS
   - `p_Eintragsnr` ($1): Positional argument, sourced from the calling script. Used as a reference ID for error logging. Map to a Python function argument.
   - `p_Skript` ($2): Positional argument, sourced from the calling script. Defines the path of the SQL script to be executed. Map to a Python function argument.
   - `$*` (remaining parameters): Positional arguments, sourced from the calling script after `shift 2`. Represents runtime arguments passed to the SQL script. Map to a Python variable-length argument list (`*script_args`).
   - `DW_ORAUSER`: Environment variable containing the Oracle connection string (e.g., `user/password@db`). Map to `os.environ.get("DW_ORAUSER")`.
   - KSH DECLARED ENVIRONMENT PARAMETERS: none present in the source.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Launches Oracle SQL*Plus to execute the specified SQL script with the provided parameters, using standard input redirected from `/dev/null` to prevent interactive terminal hangs.
     - Target: External process invocation via `subprocess.run(...)`.
     - Resolvable Launcher: No, because this function is a generic utility runner designed to execute arbitrary SQL scripts passed dynamically at runtime, rather than a single hardcoded SQL file.

5. EMBEDDED SQL
   - There are no inline SQL statements in this file (only a generic call to `sqlplus` to execute a dynamic external script).

6. CONTROL FLOW
   1. Initialize module metadata: `ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`.
   2. Function entry for `starteSQLSkript` receiving `p_Eintragsnr`, `p_Skript`, and trailing arguments.
   3. Check if `p_Eintragsnr` or `p_Skript` is empty. If so, log error 196 via `DWMSG_MeldeFehler` and return `196`.
   4. Check if the file path `p_Skript` is readable. If not, log error 201 via `DWMSG_MeldeFehler` and return `201`.
   5. Log informational execution messages showing the script path and parameters to stdout.
   6. Disable immediate exit-on-error behavior (`set +e`).
   7. Execute `sqlplus` with credentials, the script path, and additional parameters, redirecting input from `/dev/null`.
   8. Capture the execution exit status (`errcode=$?`).
   9. Re-enable exit-on-error behavior (`set -e`).
   10. Return the captured `errcode` to the caller.

7. ERROR HANDLING & EXIT CODES
   - How does the script detect failure: Uses `[ -z ... ]` and `[ ! -r ... ]` tests for input validation, and captures `$?` immediately after running the `sqlplus` command.
   - What does it do on failure: Calls the external logger `DWMSG_MeldeFehler` and returns code `196` or `201` for validation errors, or propagates the exit code returned by `sqlplus`.
   - Success exit code convention: Returns `0` if validation checks pass and `sqlplus` executes successfully.
   - Map to Python: Return codes `196` and `201` are returned as integers from the Python function. Subprocess execution errors are handled by capturing and returning `result.returncode`.
   - # REVIEW-STRUCT: DWMSG_MeldeFehler function body not supplied — behaviour simulated via a logging placeholder.

8. OUTPUTS / SIDE EFFECTS
   - Writes informational logs and execution parameters to standard output.
   - Invokes SQL*Plus which interacts with the Oracle database (reading/modifying tables) and writes output to stdout/stderr.

9. BUSINESS SUMMARY
   - Enforces pre-execution validation checks (presence of parameters and script file readability) before launching SQL*Plus to prevent silent or misleading failures.
   - Provides consistent logging and error reporting interface.
   - Safely encapsulates SQL*Plus invocation by redirecting input from `/dev/null` to prevent interactive hangs in automated batch environments.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
import pathlib

# Step 1: Define module metadata
# REVIEW: The original ksh script defines ModulName but references Modul_Name in the error call.
# Using MODUL_NAME as a consolidated constant.
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# REVIEW-STRUCT: DWMSG_MeldeFehler function body not supplied — behavior simulated via placeholder
def DWMSG_MeldeFehler(eintragsnr, severity, code, message_args):
    """
    Placeholder simulating the external DWMSG_MeldeFehler logging function.
    """
    print(f"DWMSG ERROR: [{severity}] {code} - {message_args} (Entry: {eintragsnr})", file=sys.stderr)


# Step 2: Define starteSQLSkript function
def starteSQLSkript(p_Eintragsnr, p_Skript, *script_args):
    """
    Validates and executes a SQL*Plus script.
    """
    # Step 3: Parameter validation (empty check)
    # ksh: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_Eintragsnr or not p_Skript:
        # Construct error message string
        err_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        DWMSG_MeldeFehler(p_Eintragsnr, "E", 196, err_msg)
        return 196

    # Step 4: File readability check
    # ksh: if [ ! -r $p_Skript ]
    script_path = pathlib.Path(p_Skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        DWMSG_MeldeFehler(p_Eintragsnr, "E", 201, str(p_Skript))
        return 201

    # Step 5: Informational logging
    # ksh: echo "Rufe SQL*PLUS auf mit folgenden Einstellungen" ...
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    args_str = " ".join(script_args)
    print(f"Skript-Parameter: {args_str}")

    # Step 6: Disable exit-on-error (implicit in Python by running subprocess with check=False)
    # Step 7: Execute sqlplus via subprocess
    # ksh: sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    
    # REVIEW: Target database platform is assumed to be Oracle based on the sqlplus client.
    # Python cx_Oracle / oracledb client is recommended if transitioning to native DB API, 
    # but since this is a general-purpose runner utility, we preserve the sqlplus CLI call.
    cmd = ["sqlplus", dw_orauser, f"@{p_Skript}"] + list(script_args)

    try:
        # Redirect stdin from DEVNULL to match </dev/null
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            text=True,
            check=False  # Replaces 'set +e' and captures exit code
        )
        # Step 8: Capture the exit status
        errcode = result.returncode
    except Exception as e:
        print(f"Failed to execute sqlplus subprocess: {e}", file=sys.stderr)
        errcode = -1

    # Step 9: Re-enable exit-on-error (implicit as we return control back to caller)
    # Step 10: Return captured exit status
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Converted to a Python utility module maintaining parameter validation, file readability checks, and executing SQL scripts on the target database. |

---

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
- **Target Wiring**: Because this is a shared utility function, downstream Python scripts and Cloud Composer DAG steps will import this Python module (`h_alis_sqlplus.py`) and call the equivalent function rather than sourcing KSH. For the downstream jobs marked "not yet migrated", the wiring cannot be finalized until they are migrated.

---

### Schedule & Variables
- **Schedule**: This job is NOT directly triggered by any of the run's schedulers. It executes inside other scheduled jobs (as a shared utility module). The migrated Python module should remain a callable/importable library unit without its own standalone Cloud Composer DAG or schedule.
- **Variables**:
  - `DW_ORAUSER`: Originally held the Oracle connection details. In the BigQuery target environment, this maps to standard GCP connection environment variables (`GCP_PROJECT`, `BQ_LOCATION`) or Airflow connections.

---

### External System Replacements
- **Oracle SQL\*Plus (`sqlplus`)**: This utility executes SQL\*Plus scripts in Oracle. For the BigQuery target platform, this execution should be replaced with either:
  - BigQuery client library queries (`google.cloud.bigquery.Client().query()`) executing parameterized SQL scripts.
  - Airflow `BigQueryInsertJobOperator` if the callers run inside Composer DAGs.
- **Parameter Sourcing**: Positional script parameters ($*) are mapped to BigQuery query parameters or execution parameters in standard BigQuery SQL.

---

### Cross-File Dependencies
- This file is a shared utility module. Other converted KSH shell scripts in the environment source this script to execute SQL statements. In Python, these callers will import `starteSQLSkript` from the converted `h_alis_sqlplus` module.

---

### Target File Plan

| Target File Path | Language | Source File Path |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Python | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` |

---

### Environment-Specific Values

1. **GLOBAL (Environment-wide)**:
   - `GCP_PROJECT`: Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")`. Represents the target BigQuery billing project.
   - `BQ_LOCATION`: Sourced at runtime via `os.environ.get("BQ_LOCATION")` or Airflow `Variable.get("BQ_LOCATION")`. Represents the target BigQuery dataset location.
2. **JOB-SPECIFIC**:
   - `p_Eintragsnr`: Logging reference ID, passed as a function argument at runtime.
   - `p_Skript`: The SQL script file path, passed as a function argument at runtime.
   - `script_args` (`*` / `$*`): Additional arguments for the SQL script, passed as variable arguments at runtime.

---

### Risks and Manual Steps

- **SOURCE: NOT FOUND** — `DWMSG_MeldeFehler` — no candidate. The external error logging function called in this script has no defined source code in the provided bundle. A mock or wrapper mapping to the target logging framework must be implemented.
- **SQL\*Plus Subprocess Transition**: The legacy script invokes standard `sqlplus` CLI. Since the target is BigQuery, running Oracle SQL\*Plus directly is incompatible. Developers must update the internal execution logic in `h_alis_sqlplus.py` to use BigQuery client queries (`google.cloud.bigquery`), or maintain a transient Oracle connection helper if some scripts remain on Oracle during a phased migration.
- **Downstream Migration Dependencies**: Twelve downstream calling jobs are "not yet migrated". Fully testing and verifying this shared library is blocked until at least one of these calling jobs is migrated.
- **Output/Print Literal Preservation**: In the translated logic, the original German literal statements in print/log outputs must be preserved character-for-character:
  - `"Rufe SQL*PLUS auf mit folgenden Einstellungen"`
  - `"Sql*Plus-Skript : "`
  - `"Skript-Parameter: "`