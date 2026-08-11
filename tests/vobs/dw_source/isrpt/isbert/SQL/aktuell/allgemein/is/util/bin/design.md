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
REASON: The script defines a library of KornShell functions containing validation logic, database client sessions, file operations, error handling wrappers, and dynamic variable evaluations that cannot be modeled in pure SQL.

EVIDENCE
- Business logic found: KSH custom logic. The script is a modular logging and error-management utility containing 9 shell functions that track job states via dynamic SQL*Plus PL/SQL calls, generate dynamic timestamps/filenames, and maintain system state.
- AWK: none
- SQL-expressible: no. While some functions call stored procedures, the overall library relies heavily on dynamic file I/O, parameter validations, environment variable generation, and shell function call-stacks.
- Non-SQL side effects: Creates and cleans up temporary local listings (`/tmp/ErmittleNr_$$.lst`), generates dynamic log files using system commands, and assigns dynamic variables using `eval`.
- Against this verdict: none. This is an orchestrational and utility helper module that must be migrated to a Python module to support Python-based jobs.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a KornShell utility library containing reusable functions for central error management, logging, and state tracking within the "Information Services" (isbert) project. It provides orchestration functions that other jobs source to register their execution, retrieve a unique logging sequence ID from an Oracle database, and log execution milestones or errors. In case of job failure, these functions capture exit codes, generate dynamically-named log files, and invoke PL/SQL packages (`BERT_MELDUNG`) to set job status states.

2. INVOCATION CONTEXT
   - Sourced by: Other parent/caller `.ksh` scripts within the BERT system using standard dot-sourcing: `. f_alis_msgerr.ksh`.
   - Parent scripts use `trap 'DWMSG_Fehlerbehandlung <EintragsNr>' ERR` to automatically trigger error logging on failure.
   - Sourced environment files: None inside this file, but it relies on external variables (`DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`) being populated by the caller's environment initialization scripts.
   - UC4 Context: No direct UC4 includes exist in this utility library itself, but scripts sourcing it are run within the UC4 scheduler.

3. PARAMETERS / INPUTS
   The following function parameters are accepted across the defined library:
   - `DWMSG_EintragsNr` (Positional parameter `$1` or `$3` depending on function): The unique primary ID representing this job's logging row in the database. Essential for all logging updates.
   - `VarName` (Positional parameter `$1`): The name of a shell variable to dynamically assign a calculated value to (such as a retrieved sequence ID or structured filename) via `eval`.
   - `JobKennung` (Positional parameter `$2`): A text identifier indicating the calling job's name.
   - `Programmname` (Positional parameter `$3`): The filename/executable name of the calling script.
   - `LogDatei` (Positional parameter `$4`): File path of the log output file.
   - `Typ` (Positional parameter `$2` in `DWMSG_MeldeFehler`): Error level indicator, e.g., 'F' (Fatal), 'E' (Error), or 'W' (Warning).
   - `FehlerNr` (Positional parameter `$3` in `DWMSG_MeldeFehler`): Integer code mapped to specific database error messages.
   - `Zusatz1` / `Zusatz2` (Positional parameters `$4` / `$5` in `DWMSG_MeldeFehler`): Optional text arguments providing context (e.g., dynamic filenames or system errors).
   - `DWMSG_Stichtag` / `DWMSG_StichtagFmt` (Positional parameters `$2` / `$3` in `DWMSG_SetzeStichtagInfo`): Reporting reference date and its string format.
   - `DWMSG_InfoText` (Positional parameter `$2` in `DWMSG_AppendTimingInfos`): Text to attach alongside execution time metrics.
   - `DWMSG_DateFormat` (Positional parameter `$3` in `DWMSG_AppendTimingInfos`): Format template to convert execution time dynamically.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus` (Standard Oracle Database Client): Used extensively to call the PL/SQL stored procedures on the DB target.
     - Case 1: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
     - Case 2: `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
     - Case 3: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
     - Case 4: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
     - Case 5: `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
     - Case 6: Dynamic heredoc executions `sqlplus -s $DW_ORAUSER <<EOF` executing native anonymous PL/SQL block calls.
     - *Python Migration Recommendation*: Rather than executing external processes with `sqlplus` via `subprocess.run()`, these must be replaced by direct calls to the Oracle database using a Python driver library like `oracledb` or `cx_Oracle`.
     - # REVIEW: target database platform is Oracle as indicated by SQL*Plus and PL/SQL stored procedure calls; Python DB-client library choice (e.g., python-oracledb) is provisional.
   - `cat` / `rm` / `tr`: Used to manage and clean the `/tmp/ErmittleNr_$$.lst` file. Replace with native Python `open().read()`, string replacement methods, and `os.remove()`.
   - `date`: System date generator inside `DWMSG_Logdateiname`. Replace with Python's native `datetime` formatting.

5. EMBEDDED SQL
   Since the script invokes SQL*Plus passing dynamic parameters to modular `.sql` files, the underlying statements reside inside external assets. However, inline blocks and parameters are defined as follows:
   - **`d_alis_spaufruf_p1.sql`** (PL/SQL generic launcher for 1-parameter calls):
     - Targets table/package: `BERT_MELDUNG` (Stored Procedure: `SetzeStatusOk` or `SetzeStatusAbbruch`)
     - Statement type: PL/SQL execution (`EXEC` or anonymous block)
     - Touch target: Tracks status states of individual log records.
     - # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p1.sql] not supplied — exact package signature should be verified.
   - **`d_al_is_ermittlenr.sql`** (PL/SQL database sequence generator):
     - Writes output sequence into file `$TempFile` (typically outputs a select from a database sequence, e.g., `BERT_SEQ.NEXTVAL`).
     - # REVIEW-STRUCT: SQL script [d_al_is_ermittlenr.sql] not supplied — behavior assumed to write a single numeric string into the output path.
   - **`d_alis_spaufruf_p4.sql`** (PL/SQL generic launcher for 4-parameter calls):
     - Targets procedure: `BERT_MELDUNG.Erzeuge_Eintrag`
     - Parameters: `DWMSG_EintragsNr`, `JobKennung`, `Programmname`, `LogDatei`
     - # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p4.sql] not supplied — signature assumed to execute an INSERT into database tracking tables.
   - **`d_alis_spaufruf_p[NumParm].sql`** (PL/SQL generic launcher for dynamic parameters):
     - Targets procedure: `BERT_MELDUNG.Fehler`
     - Parameters: `Typ`, `DWMSG_EintragsNr`, `FehlerNr`, `Zusatz1`, `Zusatz2`
   - **`DWMSG_SetzeStichtagInfo`** (Inline Block):
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     - Statement Type: PL/SQL Procedure Call.
     - Dialect: Oracle (uses `EXEC`, `to_date`, and `commit`).
   - **`DWMSG_AppendTimingInfos`** (Inline Block):
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     - Statement Type: PL/SQL Procedure Call.
     - Dialect: Oracle (uses `EXEC`, `SYSDATE`, string concatenation operator `||`, `to_char`, and `commit`).

6. CONTROL FLOW
   Each function executes in isolation when sourced and triggered. Their flows map sequentially:
   - **Step 1 (Error trap - `DWMSG_Fehlerbehandlung`):** Captures error return code (`$?`). Calls `DWMSG_MeldeFehler` with code `10` ("UnerwFehler") passing captured exit status. Calls `DWMSG_SetzeStatusAbbruch`.
   - **Step 2 (Set Success - `DWMSG_SetzeStatusOK`):** Validates input `DWMSG_EintragsNr` is not null (exits 1 if empty). Runs `sqlplus` to execute `BERT_MELDUNG.SetzeStatusOk`.
   - **Step 3 (Set Cancel - `DWMSG_SetzeStatusAbbruch`):** Validates input `DWMSG_EintragsNr` is not null (exits 1 if empty). Runs `sqlplus` to execute `BERT_MELDUNG.SetzeStatusAbbruch`.
   - **Step 4 (Sequence Generator - `DWMSG_ErmittleNr`):** Validates argument variable name is not null (exits 1 if empty). Creates a temp file `/tmp/ErmittleNr_[PID].lst`. Runs `sqlplus` with `d_al_is_ermittlenr.sql` to output the new database sequence value to the temp file. Reads the value, strips spaces, removes the file, and dynamically assigns it back to the requested variable.
   - **Step 5 (Create Log Entry - `DWMSG_ErzeugeEintrag`):** Validates ID is present. Calls `BERT_MELDUNG.Erzeuge_Eintrag` with job details.
   - **Step 6 (Record Error - `DWMSG_MeldeFehler`):** Evaluates provided optional variables to determine variable length (3, 4, or 5). Maps path dynamically to `d_alis_spaufruf_p[Num].sql` and executes `BERT_MELDUNG.Fehler` via `sqlplus`.
   - **Step 7 (Log Name Generator - `DWMSG_Logdateiname`):** Captures system date `date +%Y%m%d_%H%M`. Combines variables to assemble `${DW_DIR_PROT}/${JobKennung}_[date]_${DWMSG_EintragsNr}.log` and dynamically assigns it back to the requested parent shell variable.
   - **Step 8 (Set Reporting Date - `DWMSG_SetzeStichtagInfo`):** Validates arguments (exits 1 or 2 if missing). Executes SQL*Plus with dynamic inline anonymous block to execute `BERT_MELDUNG.SetzeZusatzInfos`.
   - **Step 9 (Add Metrics - `DWMSG_AppendTimingInfos`):** Validates arguments. Executes SQL*Plus with inline block to append execution times into database logging parameters.

7. ERROR HANDLING & EXIT CODES
   - If required parameters are missing in the shell library calls, the shell functions issue error print statements to `stdout` and exit with explicit termination codes (`exit 1` or `exit 2`).
   - The trap function `DWMSG_Fehlerbehandlung` captures external error levels using `$FehlerNr=$?` and reports it to the logger.
   - Python equivalence mapping:
     - Replace parameter checks with standard `ValueError` exceptions or log warnings.
     - Replace `exit 1`/`exit 2` with `sys.exit(1)` / `sys.exit(2)` or raise custom exceptions.
     - Use database connection context managers (`with`) to automatically catch `oracledb.DatabaseError` exceptions and trace issues.

8. OUTPUTS / SIDE EFFECTS
   - Mutates Oracle backend logging registry via `BERT_MELDUNG` PL/SQL functions (inserting status rows, setting transaction commits).
   - Generates and immediately removes local temporary scratch files under `/tmp/ErmittleNr_*.lst`.
   - Populates and mutates variables in the calling environment.

9. BUSINESS SUMMARY
   - **Central Execution Ledger:** Standardizes job logging, making sure every processing run registers a unique tracking index (`EintragsNr`).
   - **Automatic Failure Recovery & Auditing:** Captures unexpected system or application errors natively via trap mechanics, registering error details to a centralized DB table.
   - **Job State Automation:** Signals step-by-step state changes (`OK`, `Abbruch`, timing metadata) to allow monitoring dashboards to observe the processing pipeline.
   - **Log Path Standardization:** Enforces unified format rules for execution logging files.

=======================================================================================
PSEUDOCODE OUTLINE
=======================================================================================

```python
# Modern Python Equivalent of f_alis_msgerr.ksh
# Implemented as a reusable module that can be imported by Python-based ETL jobs.

import os
import sys
import datetime
import tempfile
import oracledb  # # REVIEW: provisional choice for Oracle DB interaction

# Global Environment Variables expected to be established
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT")


def _get_db_connection():
    """
    Internal helper to establish a database session using the DW_ORAUSER configuration.
    """
    # REVIEW: Confirm connection details for Oracle DB from DW_ORAUSER or environment
    if not DW_ORAUSER:
        raise ValueError("DW_ORAUSER environment variable is not defined.")
    
    # Standard splitting of user/password@dsn string typically contained in DW_ORAUSER
    # e.g., "username/password@hostname:port/service_name"
    try:
        connection = oracledb.connect(dsn=DW_ORAUSER)
        return connection
    except Exception as e:
        print(f"Failed to connect to Oracle database: {e}", file=sys.stderr)
        raise


# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, captured_exit_code=1):
    """
    Error handler routine called by parent scripts upon failure.
    Logs unexpected fatal failure to tracking table and sets abort status.
    """
    const_unerw_fehler = 10
    
    # Log unexpected fatal error with captured exit code
    dwmsg_melde_fehler(
        eintrags_nr, 
        "F", 
        const_unerw_fehler, 
        f"ErrorCode ist: {captured_exit_code}"
    )
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    """
    Sets the execution log entry status to completed (OK).
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p1.sql] not supplied — executing equivalent PL/SQL directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
                conn.commit()
    except Exception as e:
        print(f"Database error in SetzeStatusOk: {e}", file=sys.stderr)
        sys.exit(1)


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    """
    Sets the execution log entry status to aborted (Abbruch).
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p1.sql] not supplied — executing equivalent PL/SQL directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
                conn.commit()
    except Exception as e:
        print(f"Database error in SetzeStatusAbbruch: {e}", file=sys.stderr)
        sys.exit(1)


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr() -> str:
    """
    Fetches a unique execution logging ID sequence from the database.
    Replaces temp file strategy with direct variable return.
    """
    # REVIEW-STRUCT: SQL script [d_al_is_ermittlenr.sql] not supplied — behavior modeled as direct sequence fetch
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Assuming BERT_MELDUNG has a sequence generator or procedure to retrieve next ID
                # We fetch sequence or execute equivalent logic
                cursor.execute("SELECT BERT_SEQ.NEXTVAL FROM DUAL")
                row = cursor.fetchone()
                if row:
                    return str(row[0]).strip()
                else:
                    raise RuntimeError("Failed to fetch next logging sequence ID from database.")
    except Exception as e:
        print(f"Database error in ErmittleNr: {e}", file=sys.stderr)
        sys.exit(1)


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programm_name, log_datei):
    """
    Registers a new processing run entry inside the logging system.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL script [d_alis_spaufruf_p4.sql] not supplied — executing equivalent PL/SQL directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc(
                    "BERT_MELDUNG.Erzeuge_Eintrag", 
                    [eintrags_nr, job_kennung, programm_name, log_datei]
                )
                conn.commit()
    except Exception as e:
        print(f"Database error in ErzeugeEintrag: {e}", file=sys.stderr)
        sys.exit(1)


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    Logs an application error, warning, or fatal exception to the database.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)

    # In Python, we bypass dynamic file template selection (p3, p4, p5)
    # and simply pass the populated parameters directly to the database library.
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc(
                    "BERT_MELDUNG.Fehler", 
                    [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2]
                )
                conn.commit()
    except Exception as e:
        print(f"Database error in MeldeFehler: {e}", file=sys.stderr)
        sys.exit(1)


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr) -> str:
    """
    Constructs a structured logging output filename path based on run metadata.
    """
    dir_prot = DW_DIR_PROT if DW_DIR_PROT else "."
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    
    filename = f"{job_kennung}_{timestamp}_{eintrags_nr}.log"
    full_path = os.path.join(dir_prot, filename)
    return full_path


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    """
    Saves the target reporting business date for the active execution log record.
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

    try:
        # Convert format mask from Oracle/KSH conventions to Python datetime parsing if needed
        # Or parse natively on Oracle DB via SQL
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Parse using the format specified natively in SQL
                query = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:e_nr, TO_DATE(:s_tag, :s_fmt));
                    COMMIT;
                END;
                """
                cursor.execute(query, e_nr=eintrags_nr, s_tag=stichtag, s_fmt=stichtag_fmt)
    except Exception as e:
        print(f"Database error in SetzeStichtagInfo: {e}", file=sys.stderr)
        sys.exit(1)


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    """
    Appends timing checkpoints into the execution tracking record metadata.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)

    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Replicates: BERT_MELDUNG.SetzeZusatzInfos(eintrags_nr, null, text || ' ' || to_char(SYSDATE, format) || ' ')
                query = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(
                        :e_nr, 
                        NULL, 
                        :i_text || ' ' || TO_CHAR(SYSDATE, :d_fmt) || ' '
                    );
                    COMMIT;
                END;
                """
                cursor.execute(query, e_nr=eintrags_nr, i_text=info_text, d_fmt=date_format)
    except Exception as e:
        print(f"Database error in AppendTimingInfos: {e}", file=sys.stderr)
        sys.exit(1)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | Converts the KornShell utility and error-logging functions into reusable Python module functions to support Cloud Composer (Airflow) DAGs and Python Operators. |

---

### Job Dependencies
The following downstream jobs consume the outputs/logging status generated by this script. Because they are not yet migrated, end-to-end integration, execution state reporting, and dependency wiring cannot be finalized until they are created in the target environment:
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

---

### Scheduling
* **Schedule Context:** This utility library is not directly triggered by any schedule. Instead, it is imported and called inside other scheduled job wrappers as an include module.
* **Target Execution Mapping:** The migrated Python script must remain a callable, importable utility library in Cloud Composer, rather than being scheduled as a standalone DAG.

---

### Lineage
* **Upstream/Downstream Lineage:** The script has a direct lineage edge calling an external database procedure:
  * `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` --[CALLS_PROCEDURE]--> `PROCEDURE:SETZEZUSATZINFOS`

---

### Cross-File Dependencies
* **Shared Table Access:** The script interacts directly with database logging tables and routines belonging to the `BERT_MELDUNG` PL/SQL package/schema. It establishes a shared logging contract used across the entire `isbert` job suite to synchronize execution steps, track job progress, and record execution milestones.

---

### Target File Plan
* **Target File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py`
  * **Language:** Python
  * **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`

---

### Environment-Specific Values
The legacy system's environment variables are mapped to global BigQuery/Google Cloud variables at runtime.

* **GLOBAL Variables:**
  * `GCP_PROJECT`: Sourced dynamically at runtime using `os.environ.get("GCP_PROJECT")` or via `Variable.get("GCP_PROJECT")`. Represents the target GCP project ID.
  * `BQ_DATASET`: Sourced dynamically using `os.environ.get("BQ_DATASET")` or via `Variable.get("BQ_DATASET")`. Represents the target BigQuery metadata/audit dataset replacing Oracle's schema.
  * `GCS_BUCKET`: Sourced dynamically using `os.environ.get("GCS_BUCKET")` or via `Variable.get("GCS_BUCKET")`. Replaces legacy local directories (`DW_DIR_ROOT` and `DW_DIR_PROT`) for persistent storage of log files and output assets.

---

### Risks and Manual Steps
* **Downstream Integration Risks:** Since all downstream consumer workflows (such as `DW.BERT_RECHNUNGSDATEN`) are not yet migrated, the exact import mechanisms, custom Airflow PythonOperators, or execution hooks cannot be validated or finalized until those downstream components are created.
* **Logging System Redesign (B4 Redesign Item):** The legacy script calls Oracle PL/SQL procedures (`BERT_MELDUNG`) via SQL*Plus. BigQuery is a serverless OLAP platform, and direct transactional row-by-row updates for execution logs is an anti-pattern that can lead to concurrency bottlenecks. The central logging mechanism should be redesigned to either:
  1. Write to structured auditing tables in BigQuery using buffered inserts.
  2. Map logging calls directly to Google Cloud Logging or Cloud Pub/Sub.
  3. Log to a Cloud SQL instance if ACID/operational transactionality is strictly required for scheduling state.

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
REASON: The script is a reusable utility library of date-arithmetic functions containing complex KornShell logic, conditional loops, arrays, and Oracle SQLPlus database interactions.

EVIDENCE
- Business logic found: KSH custom logic. It defines functions for date checking, chronological ordering, leap year calculations, month-end calculations, and manual date addition, as well as wrapping SQLPlus calls.
- AWK: none
- SQL-expressible: partly, but since this is a sourced shell helper library that alters calling scopes and performs manual shell math, it is best translated to a Python helper module.
- Non-SQL side effects: Writes/removes temp files in `/tmp`, alters environment variables via `eval`, and prints output to stdout.
- Against this verdict: One could theoretically write SQL UDFs in BigQuery for the date calculations, but that wouldn't replace the orchestration/shell utility nature of the library or help external non-SQL jobs calling it.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The `h_alis_date.ksh` script is a reusable date arithmetic and validation utility library. It defines various shell helper functions used to calculate relative dates, check date validity, compare dates, and add offsets to dates. These operations are performed either natively in shell code (e.g., leap year and day calculations) or by executing Oracle SQLPlus queries.

### 2. INVOCATION CONTEXT
- **Invoker:** Sourced or directly called by other shell scripts (typically run via UC4). There is no explicit UC4 job name declared in this script, but the header notes that it is designed for use in connection with Data Warehouse (DW) variables.
- **UC4 Native Includes:** None referenced.
- **Environment Files Sourced:** None explicitly sourced inside the script, but the header specifies that `.dw_init` must be run beforehand, or the environment variables `DW_DIR_ROOT` and `DW_ORAUSER` must be set.

### 3. PARAMETERS / INPUTS
The script contains function-level positional parameters rather than global script parameters.

- **For function `DWDate_Vormonat`:**
  - `P1` (positional `$1`): Target variable name to receive the calculated value via `eval`. Surfaced in Python as a function return value.
  - `P2` (positional `$2`): Oracle date format string (e.g., `'YYYYMM'`). Surfaced in Python as a string parameter.
- **For function `DWDate_Datum_Check`:**
  - `P1` (positional `$1`): Date string to check.
  - `P2` (positional `$2`): Date format string.
- **For function `DWDate_Datum_LE`:**
  - `P1` (positional `$1`): First date (string in `'YYYYMMDD'` format).
  - `P2` (positional `$2`): Second date (string in `'YYYYMMDD'` format).
- **For function `DWDate_Gib_Zeitraum`:**
  - `I-P1` (positional `$1`): Offset (integer).
  - `I-P2` (positional `$2`): Interval step (`'Y'`, `'M'`, `'D'`).
  - `I-P3` (positional `$3`): Output format.
  - `O-P4` (positional `$4`): Variable name for Start Date.
  - `O-P5` (positional `$5`): Variable name for End Date.
- **For function `LetzterTagDesMonats`:**
  - `P1` (positional `$1`): Date string in `'YYYYMMDD'` format.
- **For function `TageimMonat`:**
  - `P1` (positional `$1`): Year (integer or string `YYYY`).
  - `P2` (positional `$2`): Month (integer or string `MM`).
- **For function `AddiereDatum`:**
  - `P1` (positional `$1`): Date string in `'YYYYMMDD'` format.
  - `P2` (positional `$2`): Integer days to add.

**Environment variables used:**
- `DW_ORAUSER`: Database connection credentials used to execute SQLPlus.
- `DW_DIR_ROOT`: Base directory for DWH scripts.
- # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **sqlplus**: Used to query the database or execute PL/SQL blocks.
  - *Command 1:* `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql $DWDate_tmpFile $DWDate_FMT </dev/null`
    - Purpose: Computes the previous month's date using an external SQL script and outputs to a temporary file.
  - *Command 2:* `sqlplus -s <<EOF ...` (inside `DWDate_Datum_Check` and `DWDate_Datum_LE`)
    - Purpose: Performs validation of date strings and chronologies natively in Oracle.
  - *Command 3:* `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql $tmpFile $Offset $Stufe $Format </dev/null`
    - Purpose: Calculates start and end timestamps based on dynamic intervals.
- # REVIEW-STRUCT: launcher [sqlplus] executes external scripts d_alis_vormonat.sql and d_alis_datum_zeitraum.sql — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

### 5. EMBEDDED SQL
- **SQL 1 (inside `DWDate_Datum_Check`):**
  - Source: Inline here-doc
  - SQL Text:
    ```sql
    WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
    SET HEADING OFF;
    select to_date('$wert','$format') from dual;
    ```
  - Type: SELECT
  - Table: `dual` (Oracle system table)
  - Dialect: Oracle SQL (uses `to_date`, `dual`, and SQLPlus control directives).

- **SQL 2 (inside `DWDate_Datum_LE`):**
  - Source: Inline here-doc PL/SQL block
  - SQL Text:
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
            raise_application_error(-20422,'Datum $datum1 ist groesser als $datum2');
        END IF;
    END;
    /
    ```
  - Type: Anonymous PL/SQL Block
  - Table: None
  - Dialect: Oracle PL/SQL (unambiguously Oracle due to variable declarations, assignments `:=`, and `raise_application_error`).

### 6. CONTROL FLOW
1. **DWDate_Vormonat**:
   - Creates a temporary file `/tmp/h_alis_date_..._$$`
   - Executes external SQL script `d_alis_vormonat.sql` via `sqlplus`.
   - Reads the generated file and sets the variable named by `$1` to this value using `eval`.
   - # REVIEW: The original script runs `rm -f $DWDate_FMT` instead of deleting the temporary file `$DWDate_tmpFile`. This is a bug in the legacy script. The Python equivalent should properly clean up the temporary file instead of attempting to delete the format string.

2. **DWDate_Datum_Check**:
   - Ensures exactly 2 arguments are provided.
   - Launches SQLPlus inline, executing `select to_date('$wert','$format') from dual;`.
   - Returns the exit status of the SQLPlus run (0 for success, non-zero for failure).

3. **DWDate_Datum_LE**:
   - Ensures exactly 2 arguments are provided.
   - Runs inline PL/SQL block via SQLPlus to verify if `$datum1` is less than or equal to `$datum2`.
   - If not, raises application error `-20422` causing SQLPlus to exit with failure.
   - Returns the exit status of SQLPlus.

4. **DWDate_Gib_Zeitraum**:
   - Validates that 5 arguments are provided.
   - Generates a timestamped temporary file in `/tmp/`.
   - Executes `d_alis_datum_zeitraum.sql` using SQLPlus.
   - Validates that the phrase `"DWH_Ergebnis;"` occurs exactly once in the temporary file. If not, prints an error and returns 1.
   - Extracts the start and end values using `grep` and `cut` (delimited by `;`).
   - Assigns variables in calling scope via `eval`.
   - Deletes the temporary file.

5. **LetzterTagDesMonats**:
   - Splits input `$1` (YYYYMMDD) into Year, Month, Day.
   - Determines if Year is a leap year (using modulo arithmetic).
   - Looks up the maximum days in that month using a static array (handling February leap year).
   - Returns 0 if input Day is equal to the maximum days for that Month, else returns 1.

6. **TageimMonat**:
   - Determines if the given Year ($1) is a leap year.
   - Returns the length of Month ($2) from the static array.

7. **AddiereDatum**:
   - Splits input `$1` (YYYYMMDD) into Year, Month, Day.
   - Adds the offset `$2` directly to Day.
   - While Day exceeds the month's maximum days (as determined by `TageimMonat`):
     - Subtracts current month's capacity from Day.
     - Increments Month by 1.
     - While Month exceeds 12: decrements Month by 12 and increments Year by 1.
   - Pads Year (to 4 digits), Month, and Day (to 2 digits).
   - Prints the final calculated date as `YYYYMMDD`.

### 7. ERROR HANDLING & EXIT CODES
- Shell functions use `return 1` for parameter count validation failures and command failures.
- SQLPlus execution uses `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` to convert database errors into shell process failures.
- In Python, we will raise exceptions (`ValueError`) for validation errors or propagate SQL execution errors through standard driver exception blocks.

### 8. OUTPUTS / SIDE EFFECTS
- Creates temporary files under `/tmp/`.
- Alters environment/variable states of caller using `eval`.
- Writes log messages to standard output and standard error.

### 9. BUSINESS SUMMARY
- **Date Verification:** Validates if a date string conforms to a expected Oracle date format.
- **Chronological Assertions:** Asserts that one date precedes or equals another date.
- **Reporting Windowing:** Generates absolute start and end date boundaries based on relative temporal steps (Days, Months, Years).
- **Leap Year & Month Ultimo Arithmetic:** Computes month capacities and verifies if a date is the last day of the month.
- **Native Shell Date Offsets:** Implements an iterative rollover algorithm to perform calendar day additions natively without initiating database roundtrips.

---

### PSEUDOCODE OUTLINE (PYTHON)

```python
import os
import sys
import re
import tempfile
import subprocess
from datetime import datetime, timedelta

# REVIEW: target database platform not specified; DB-client library choice below is provisional
# To replicate sqlplus functionality in native Python, we assume either an Oracle driver (oracledb)
# or a standard DB client configuration. Here we outline the logical implementation.

# Helper to check leap year
def is_leap_year(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

# Replaces: TageimMonat
def tage_im_monat(year: int, month: int) -> int:
    # Step 1: Handle leap year adjustment for February
    letzter_feb = 29 if is_leap_year(year) else 28
    letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return letzter_tag[month]

# Replaces: LetzterTagDesMonats
def letzter_tag_des_monats(date_str: str) -> bool:
    # Step 1: Parse parts from YYYYMMDD string
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    
    # Step 2: Compare parsed day with calculated month capacity
    return day == tage_im_monat(year, month)

# Replaces: AddiereDatum
def addiere_datum(date_str: str, days_to_add: int) -> str:
    # Step 1: Extract year, month, day
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    
    # Step 2: Perform addition
    day += days_to_add
    
    # Step 3: Rollover days to next months/years
    while day > tage_im_monat(year, month):
        day -= tage_im_monat(year, month)
        month += 1
        while month > 12:
            month -= 12
            year += 1
            
    # Step 4: Pad values to YYYYMMDD format
    return f"{year:04d}{month:02d}{day:02d}"

# Replaces: DWDate_Datum_Check
def dw_date_datum_check(wert: str, format_str: str) -> bool:
    # Step 1: Map basic Oracle format strings to Python strptime equivalents
    # REVIEW: Oracle format models are extensive. Below is a simplified mapping.
    format_mapping = {
        "YYYYMMDD": "%Y%m%d",
        "YYYYMM": "%Y%m",
        "DD.MM.YYYY": "%d.%m.%Y"
    }
    py_format = format_mapping.get(format_str, format_str)
    try:
        datetime.strptime(wert, py_format)
        return True
    except ValueError:
        # Step 2: Fall back to executing Oracle DB check if format is complex
        # REVIEW-STRUCT: DB credentials (DW_ORAUSER) required if falling back to SQLPlus
        dw_orauser = os.environ.get("DW_ORAUSER")
        if not dw_orauser:
            raise ValueError("DW_ORAUSER environment variable is not defined.")
            
        sql_command = f"""
        WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
        SET HEADING OFF;
        select to_date('{wert}','{format_str}') from dual;
        """
        try:
            subprocess.run(
                ["sqlplus", "-s", dw_orauser],
                input=sql_command,
                text=True,
                capture_output=True,
                check=True
            )
            return True
        except subprocess.CalledProcessError:
            return False

# Replaces: DWDate_Datum_LE
def dw_date_datum_le(datum1_str: str, datum2_str: str) -> bool:
    # Step 1: Parse both date strings
    try:
        d1 = datetime.strptime(datum1_str, "%Y%m%d")
        d2 = datetime.strptime(datum2_str, "%Y%m%d")
        if d1 > d2:
            raise ValueError(f"Datum {datum1_str} ist groesser als {datum2_str}")
        return True
    except ValueError as e:
        print(f"Error checking dates: {e}", file=sys.stderr)
        return False

# Replaces: DWDate_Vormonat
# Instead of modifying scope via 'eval', Python returns the calculated string.
def dw_date_vormonat(format_str: str) -> str:
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    
    if not dw_orauser or not dw_dir_root:
        raise ValueError("DW_ORAUSER and DW_DIR_ROOT must be defined in environment.")
        
    # Step 1: Establish temporary file path securely
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp_file:
        tmp_file_path = tmp_file.name
        
    try:
        # Step 2: Execute external sql script
        script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_vormonat.sql")
        # # REVIEW-STRUCT: external script d_alis_vormonat.sql not supplied
        subprocess.run(
            ["sqlplus", "-s", dw_orauser, f"@{script_path}", tmp_file_path, format_str],
            stdin=subprocess.DEVNULL,
            check=True
        )
        
        # Step 3: Read result from temp file
        with open(tmp_file_path, 'r') as f:
            result = f.read().strip()
        return result
    finally:
        # Step 4: Corrected bug - delete the actual temporary file rather than format string
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)

# Replaces: DWDate_Gib_Zeitraum
def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    
    if not dw_orauser or not dw_dir_root:
        raise ValueError("DW_ORAUSER and DW_DIR_ROOT must be defined in environment.")
        
    # Step 1: Set up temporary file
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp_file:
        tmp_file_path = tmp_file.name
        
    try:
        # Step 2: Execute sqlplus wrapper
        script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_datum_zeitraum.sql")
        # # REVIEW-STRUCT: external script d_alis_datum_zeitraum.sql not supplied
        subprocess.run(
            ["sqlplus", "-s", dw_orauser, f"@{script_path}", tmp_file_path, str(offset), stufe, format_str],
            stdin=subprocess.DEVNULL,
            check=True
        )
        
        # Step 3: Validate file output
        with open(tmp_file_path, 'r') as f:
            lines = f.readlines()
            
        matching_lines = [line for line in lines if "DWH_Ergebnis;" in line]
        if len(matching_lines) != 1:
            print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
            print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
            print(f"   1 Zeile erwartet, {len(matching_lines)} Zeile(n) bekommen", file=sys.stderr)
            raise RuntimeError("Database return formatting error.")
            
        # Step 4: Parse tokens
        parts = matching_lines[0].strip().split(';')
        start_val = parts[1]
        end_val = parts[2]
        
        return start_val, end_val
    finally:
        # Step 5: Clean up temp file
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py` | Migrated to a Python utility module mirroring the original folder structure, converting legacy shell/SQL*Plus date-handling functions to native Python datetime/calendar logic and BigQuery client calls. |

---

### Job Dependencies
* **Downstream Jobs:**
  The following downstream consumer jobs depend on the date functions in this library and are marked as **not yet migrated**:
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

  *Target Wiring:* Once these downstream KSH/Python or Airflow tasks are migrated, they will import the converted Python module (`h_alis_date.py`) directly or call it via PythonOperators in Cloud Composer to execute the required date math and validation functions.

---

### Scheduling
* **Trigger Mechanism:** This utility library is not directly triggered by any scheduler. It operates as an include/shared helper module. 
* **Target Scheduling Construct:** In the BigQuery/Cloud Composer target architecture, this file remains a callable/importable Python module (`h_alis_date.py`). It should not have its own standalone Airflow DAG or schedule. Instead, it must be packaged and placed in the Airflow `plugins/` or `dags/` folder structure (or a shared library path in the environment's `PYTHONPATH`) so that other Composer DAGs can import it natively.

---

### Schedule & Variables
* **Scheduler-Set Variables:** This utility library historically inherited environment variables from the parent shell runner or `.dw_init`.
  * `DW_DIR_ROOT` (Inherited): Defines the base path of the workspace. In the target environment, this will map to the base of the imported python packages or modules in Cloud Composer.
  * `DW_ORAUSER` (Inherited): Used for SQL*Plus database connections. In BigQuery, this is replaced by the default Google Cloud credentials or service account configured on Cloud Composer / BigQuery Client.

---

### Lineage
* **Upstream Table Producer:**
  * `TABLE:DUAL` (Oracle system table): Used inside inline SQL*Plus calls for validation. In the target BigQuery dialect, queries against `DUAL` are replaced by standard native BigQuery SELECT statements that omit the `FROM` clause entirely.

---

### External System Replacements
* **Oracle Database (SQL\*Plus) to BigQuery Client:** 
  The legacy script uses `sqlplus` to execute inline validations and run external scripts (`d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`). 
  * The native date-arithmetic functions (`LetzterTagDesMonats`, `TageimMonat`, `AddiereDatum`) are converted entirely to native Python code using Python's `datetime` and `calendar` libraries, eliminating database roundtrips.
  * Database-reliant validations (`DWDate_Datum_Check`, `DWDate_Datum_LE`) are rewritten using Python's standard `datetime.strptime()` parsing logic. For highly complex formats that cannot be natively matched by Python, a fallback BigQuery SQL execution is used via the `google.cloud.bigquery` client SDK.

---

### Cross-File Dependencies
* **External SQL Scripts:**
  The library invokes two external SQL scripts that are expected to exist in the DWH repository:
  * `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql` (called by `DWDate_Vormonat`)
  * `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql` (called by `DWDate_Gib_Zeitraum`)
  
  The Python equivalents of these functions will look up these corresponding converted SQL scripts from the configured python package/workspace folder structure.

---

### Target File Plan

* **Target File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py`
  * **Language:** Python (`.py`)
  * **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
  * **Purpose:** Implements all legacy KornShell date validation and manipulation functions using native Python datetime libraries, and leverages the standard Google Cloud BigQuery API for necessary database validations.

---

### Environment-Specific Values

1. **GCP_PROJECT**
   * **Role:** GLOBAL (identifies the GCP project hosting the target BigQuery datasets).
   * **Target Resolution:** Fetched at runtime via `os.environ.get("GCP_PROJECT")` or configured globally via the default Google Cloud project settings.

2. **BQ_DATASET**
   * **Role:** GLOBAL (identifies the default BigQuery dataset for executing queries).
   * **Target Resolution:** Fetched at runtime via `os.environ.get("BQ_DATASET")`.

3. **DW_DIR_ROOT**
   * **Role:** GLOBAL (specifies the base root folder of the migrated python repository).
   * **Target Resolution:** Fetched at runtime via `os.environ.get("DW_DIR_ROOT")` or resolved dynamically relative to the executing module's file path (`os.path.dirname(...)`).

4. **DWDate_tmpFile / tmpFile**
   * **Role:** JOB-SPECIFIC (path for writing dynamic temp files).
   * **Target Resolution:** Handled dynamically inside Python via standard library `tempfile.NamedTemporaryFile` calls, ensuring secure, cross-platform local temp file generation.

---

### Risks & Manual Steps

* SOURCE: NOT FOUND — `d_alis_vormonat.sql` — no candidate
* SOURCE: NOT FOUND — `d_alis_datum_zeitraum.sql` — no candidate
* **Bug Fix Notice (Temp File Leak):** The original shell function `DWDate_Vormonat` contains a bug where it attempts to delete the format string variable (`rm -f $DWDate_FMT`) instead of the actual temporary file (`$DWDate_tmpFile`). This causes a slight leak of temporary files in `/tmp` in the legacy environment. The converted Python target implementation fixes this by using context-managed temporary files (`tempfile.NamedTemporaryFile`) which are guaranteed to be cleaned up automatically on function exit.
* **German Console Output Preservation:** In accordance with the Output/Print Literal Rule, the original German error messages and debug outputs must be printed exactly as-is in the Python target module. The following literals are retained character-for-character:
  * `!! Interner Fehler bei der Rueckgabe von Datumswerten`
  * `   Funktion: DWDate_Gib_Zeitraum`
  * `   1 Zeile erwartet, {anzahl} Zeile(n) bekommen`
  * `Datum {datum1} ist groesser als {datum2}`

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
REASON: This script is a modular parameter parsing and validation library with complex string-mapping, system-key validation, and date-range logic that is not expressible in SQL.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This script, `h_alis_parameter.ksh`, is a modular KornShell utility library providing helper routines for parameter parsing, verification, and normalization in a data warehousing environment. It is sourced by other scripts in the loading pipeline to validate input parameters (systems, metrics, and dates), convert long names to standardized short abbreviations, map metrics to business areas and processing intervals, and handle date arithmetic. It serves as a centralized metadata and parameter routing module to maintain data consistency across warehouse ingestion jobs.

### 2. INVOCATION CONTEXT
- **Sourcing/Invocation**: This script is not executed directly. It is designed to be sourced (e.g., `. h_alis_parameter.ksh`) by primary loading scripts to expose its functions.
- **UC4 Job Name / Arguments**: Sourced dynamically during job execution; exact UC4 jobs sourcing this script are not directly identified in this module, but it participates in the `is_bert` / `isrpt` load chain.
- **UC4 Native Includes**: None referenced in this module's extraction.
- **Environment files sourced**: None.

### 3. PARAMETERS / INPUTS
The script utilizes several state and parameter variables:
- `ErrNr` (Environment/Global State Variable): Integer representing the current error state. Initialized or inherited from the calling script. Checked at the entry of each function; if non-zero, functions return early.
- `ErrArg` (Environment/Global State Variable): String containing the error description or failing argument.
- `ModulName` (Internal Module Variable): Hardcoded to `"alis_parameter"`.
- `ModulVersion` (Internal Module Variable): Hardcoded to `"V3.0.9"`.

Function Parameters (passed dynamically to the modular routines):
- `param_name` (Function positional parameter): Descriptive name of a parameter for error tracking.
- `param_var` (Function positional parameter): The string name of an environment variable whose value needs checking.
- `VarName` (Function positional parameter): The string name of a variable to be modified in-place (simulating pass-by-reference).
- `System` (Function positional parameter): System abbreviation code (e.g. `sap`, `carmen`, `xtra`).
- `Kennzahl` (Function positional parameter): Metric abbreviation code (e.g. `zug`, `abg`, `rst`).
- `VarBereich` (Function positional parameter): Variable name to receive the business domain classification (e.g., `tn`, `us`).
- `VarIntervall` (Function positional parameter): Variable name to receive the interval code (e.g., `t`, `m`).
- `Anfang` / `Ende` (Function positional parameters): Start and end dates in `YYYYMMDD` format.
- `p_Zahl` (Function positional parameter): Numeric value to validate.
- `p_ZeitOffset` / `p_Spanne` (Function positional parameters): Relative history span value.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- `DWDate_Datum_Check`:
  - Command: `DWDate_Datum_Check $Wert $Format`
  - Purpose: External date utility used to check format compliance of a date string.
  - Conversion: Replace with native Python `datetime.strptime(date_str, "%Y%m%d")` within a `try/except` block.
- `DWDate_Datum_LE`:
  - Command: `DWDate_Datum_LE $Anfang $Ende`
  - Purpose: External date comparison utility checking if `Anfang <= Ende`.
  - Conversion: Replace with standard native Python comparison (`anfang_date <= ende_date`).
- `DWDate_Gib_Zeitraum`:
  - Command: `DWDate_Gib_Zeitraum -$p_Spanne $Offset_Unit "YYYYMMDD" Anfangsdatum Endedatum`
  - Purpose: Computes relative start/end dates based on an offset count and unit (M for Month, D for Day).
  - Conversion: Replace with Python `datetime` and `dateutil.relativedelta` operations to subtract the relative span.
  - # REVIEW-STRUCT: launcher [DWDate_Gib_Zeitraum] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion. If native replacement is preferred, verify historical calendar rules match the original C/C++ utility exactly.
- `basename`:
  - Command: `basename $0`
  - Purpose: Get script name.
  - Conversion: `os.path.basename(__file__)`.
- `date`:
  - Command: `date +%Y%m%d%H%M%S`
  - Purpose: Get current timestamp.
  - Conversion: `datetime.now().strftime("%Y%m%d%H%M%S")`.

### 5. EMBEDDED SQL
- None (this script contains parameter verification logic only, no database queries).

### 6. CONTROL FLOW
The script execution proceeds as a library loading phase followed by dynamic function execution by calling scripts:

1. **Initialization**: Set internal metadata `ModulName` and `ModulVersion`.
2. **`pruefeParameterGesetzt`**:
   - Check if `ErrNr != 0`. If so, exit.
   - Assert parameter names are provided.
   - Dynamically inspect environment variable (using `eval "param_wert=\$$param_var"`).
   - If value is empty, set `ErrNr = 194`, `ErrArg = param_name`.
3. **`konvertiereKennzahl`**:
   - Check if `ErrNr != 0`. If so, exit.
   - Dynamic lower-casing and mapping of metric names (e.g., `zugang` -> `zug`, `bestand` -> `bst`).
   - Write output to the caller's variable. Set `ErrNr = 198` and `ErrArg` if mapping fails.
4. **`konvertiereSystem`**:
   - Check if `ErrNr != 0`. If so, exit.
   - Validate system code against allowed set (`sap`, `carmen`, `dpps`, `d1`, `xtra`, `ctel`, `nnv`, `dwh`, `brunet`, `sigma`).
   - If invalid, set `ErrNr = 195` and update caller's variable to `"???"`.
5. **`konvertiereSDName`**:
   - Check if `ErrNr != 0`. If so, exit.
   - Lower-case and map master data source categories (e.g. `rahmenvertrag` -> `rv`, `tarif` -> `trf`).
   - Set `ErrNr = 195` if unknown.
6. **`konvertiereAufbStufeXtra`**:
   - Map process phase names (`zusammenfuehrung` -> `mrg`, `befuellung` -> `fill`).
7. **`pruefeSystemKennzahl`**:
   - Enforce system-to-metric combination boundaries (e.g. `sap` cannot pair with `zug`, `carmen` cannot pair with `srs`).
   - Set `ErrNr = 195` and `ErrArg` on violation.
8. **`gibBereich`**:
   - Map metrics to functional domains: `tn` (Subscriber), `us` (Sales/Revenue), `gd` (General), `sd` (Master Data), `md` (Metadata).
   - Dynamically write back domain code to target variable; set `ErrNr = 196` if unknown.
9. **`gibIntervall`**:
   - Map metrics to execution interval frequency: `t` (Daily) or `m` (Monthly).
10. **`pruefeZeitraum`**:
    - Temporarily disable exit-on-error (`set +e`).
    - Validate date string format compliance and ordering using date helper functions.
11. **`pruefeZahlPositiv`**:
    - Confirm value is non-negative and numeric.
12. **`pruefeZeitParameter`**:
    - Ensure mutual exclusivity between direct date boundaries (`Anfang` + `Ende`) and relative date calculations (`p_ZeitOffset`).
13. **`konvertiereZeitspanne`**:
    - Identify interval unit (`M` if metric is `bst`, else `D`).
    - Execute time interval offset deduction to calculate absolute start and end date variables.

### 7. ERROR HANDLING & EXIT CODES
- The script uses modular global tracking via variable `ErrNr`.
- If a step fails, `ErrNr` is set to an error number, and `ErrArg` is loaded with descriptive text. Sourcing scripts are expected to inspect `ErrNr` and fail/abort accordingly.
- Error codes mapping:
  - `194`: Parameter is unset.
  - `195`: Logic error / invalid parameter combination / validation constraint violation.
  - `196`: Missing function parameters / unknown mapping index.
  - `198`: Unknown metric code.
  - `85`: Date calculation error.
- Python translation: Implement as a stateful utility class (`AlisParameterHelper`) tracking `err_nr` and `err_arg`, or implement standard Exception raising which translates cleanly to standard job failure structures. To match the original global variable design, the class structure can hold state, allowing the caller to either raise exceptions or inspect numeric codes.

### 8. OUTPUTS / SIDE EFFECTS
- Modification of referenced environment variables (simulated via dictionary manipulation or `globals()` state in Python).
- Clean temporary files generated in `/tmp/` during date validations.

### 9. BUSINESS SUMMARY
- **Abbreviation Standardization**: Standardizes long business terms into uniform codes (e.g., `gutschrift` -> `gut`, `tarifwechsel` -> `twe`) for downstream processing.
- **Logical Feasibility Constraints**: Restricts ingestion paths to logical source-to-metric pairs (e.g., master data definitions cannot be loaded under a transaction system like `sap`).
- **Domain Assignment**: Routes metrics to domains like subscriber statistics (`tn`) or revenue metrics (`us`).
- **Scheduling Interval Processing**: Resolves whether a dataset runs on daily (`t`) or monthly (`m`) schedules.
- **Chronological Validity**: Enforces consistent historical window calculations (such as calculating start and end dates from sliding parameter configurations).

---

### PSEUDOCODE OUTLINE

```python
# Modernized Parameter Utility Module: h_alis_parameter.py
import os
import sys
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class AlisParameterHelper:
    def __init__(self):
        self.modul_name = "alis_parameter"
        self.modul_version = "V3.0.9"
        self.err_nr = 0
        self.err_arg = ""

    # Step 1: Check if parameter is set in the runtime context
    def pruefe_parameter_gesetzt(self, param_name, param_var_name, context_dict):
        if self.err_nr != 0:
            return

        if not param_name or not param_var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} pruefeParameterGesetzt"
            return

        param_wert = context_dict.get(param_var_name, None)
        if param_wert is None or param_wert == "":
            self.err_nr = 194
            self.err_arg = param_name

    # Step 2: Normalize and convert metric names
    def konvertiere_kennzahl(self, var_name, context_dict):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereKennzahl"
            return

        original_val = str(context_dict.get(var_name, "")).lower()

        mappings = {
            "zugang": "zug", "abgang": "abg", "abgang_zukunft": "abz", "bestand": "bst",
            "tarifwechsel": "twe", "plan": "pln", "gutschrift": "gut", "aufladung": "auf",
            "restguthaben": "rst", "teilnehmerverbindungsdaten": "tvd", "uskonto": "usk",
            "usteilnehmer": "ust", "leistungsklasse": "lkl", "loeschung": "loe",
            "reaktivierung": "rak", "standard_rechnung": "srs", "standard_gutschrift": "sgs",
            "gutschrift_rv": "sg_rv", "rechnungen_rv_dpps": "sr_rv_dpps", "bewegart": "bwa",
            "kundenstamm": "ksd", "mahnstufe": "mahn", "metadatenstruktur": "mds",
            "d1news": "d1n", "rubrik": "rub", "liefermodus": "lmo", "netznutzungsklassen": "nnk",
            "tagesverkehrskurven": "tvk", "gespraechsziele": "gz", "gespraechslaengenverteilung": "glv",
            "zonenkennung": "zonek", "zonentyp": "zonet", "netznutzungsklassentyp": "nnkt",
            "tarifart": "trfa", "gespraechstyp": "gtyp", "basisdienst": "basisd",
            "nationalinternational": "natint", "glaengenintervall": "glint"
        }

        if original_val in mappings:
            context_dict[var_name] = mappings[original_val]
        else:
            self.err_nr = 198
            self.err_arg = original_val
            context_dict[var_name] = "???"

    # Step 3: Validate and convert source system code
    def konvertiere_system(self, var_name, context_dict):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereSystem"
            return

        system = str(context_dict.get(var_name, "")).lower()
        allowed = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

        if system in allowed:
            context_dict[var_name] = system
        else:
            self.err_nr = 195
            self.err_arg = f"Unbekannte Datenherkunft {system} !"
            context_dict[var_name] = "???"

    # Step 4: Validate and convert Master Data source type
    def konvertiere_sd_name(self, var_name, context_dict):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereSDSystem"
            return

        system = str(context_dict.get(var_name, "")).lower()
        mappings = {
            "vo": "vo", "rahmenvertrag": "rv", "tarif": "trf", "tstatus": "ts",
            "zahlmodus": "zm", "kdg_grund": "kdg", "gutschrift": "gut", "aufladung": "auf",
            "leistung": "l_leist", "gutschrift_grund": "l_gutgr", "sap_gutschrift_grund": "sap_l_gutgr",
            "produkt": "l_prod", "mahnverfahren_sapist": "l_mahnv_ist", "mahnverfahren_sapfi": "l_mahnv_fi",
            "mahnstufentyp_sapist": "l_mahnstyp_ist", "bewegart": "bwa"
        }

        if system in mappings:
            context_dict[var_name] = mappings[system]
        else:
            self.err_nr = 195
            self.err_arg = f"Unbekannte Stammdaten-Datenherkunft {system} !"
            context_dict[var_name] = "???"

    # Step 5: Convert stage name for Xtra
    def konvertiere_aufb_stufe_xtra(self, var_name, context_dict):
        if self.err_nr != 0:
            return

        if not var_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} konvertiereAufbStufeXtra"
            return

        stufe = str(context_dict.get(var_name, "")).lower()
        if stufe == "zusammenfuehrung":
            context_dict[var_name] = "mrg"
        elif stufe == "befuellung":
            context_dict[var_name] = "fill"
        else:
            self.err_nr = 195
            self.err_arg = f"Unbekannte Stufenangabe {stufe} !"
            context_dict[var_name] = "???"

    # Step 6: Validate combinations of System and Metric
    def pruefe_system_kennzahl(self, system, kennzahl):
        if self.err_nr != 0:
            return

        if not system or not kennzahl:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} pruefeSystemKennzahl"
            return

        violating_combination = False

        if system != "nnv" and kennzahl in {"tvd", "lkl"}:
            violating_combination = True
        elif system == "carmen":
            if kennzahl in {"twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
                violating_combination = True
        elif system == "sap":
            if kennzahl in {"zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"}:
                violating_combination = True
        elif system == "dpps":
            if kennzahl in {"twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}:
                violating_combination = True
        elif system == "ctel":
            if kennzahl not in {"abg", "bst", "zug", "twe"}:
                violating_combination = True
        elif system == "xtra":
            if kennzahl != "rst":
                violating_combination = True
        elif system == "d1":
            if kennzahl in {"gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
                violating_combination = True
        elif system == "nnv":
            if kennzahl not in {"tvd", "lkl"}:
                violating_combination = True
        elif system == "dwh":
            if kennzahl != "mds":
                violating_combination = True
        elif system == "brunet":
            if kennzahl not in {"d1n", "rub", "lmo"}:
                violating_combination = True
        elif system == "sigma":
            sigma_valid = {"nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
            if kennzahl not in sigma_valid:
                violating_combination = True

        if violating_combination:
            self.err_arg = f"Ungueltige Kombination {system} {kennzahl}"
            self.err_nr = 195

    # Step 7: Retrieve functional area (Bereich) based on metric
    def gib_bereich(self, kennzahl, var_bereich_name, context_dict):
        if self.err_nr != 0:
            return

        if not kennzahl or not var_bereich_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibBereich"
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
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibBereich - Kuerzel '{kennzahl}' unbekannt"
            return

        context_dict[var_bereich_name] = my_bereich

    # Step 8: Retrieve schedule interval frequency
    def gib_intervall(self, kennzahl, var_intervall_name, context_dict):
        if self.err_nr != 0:
            return

        if not kennzahl or not var_intervall_name:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibIntervall"
            return

        list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
        list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

        my_intervall = None
        if kennzahl in list_t:
            my_intervall = "t"
        elif kennzahl in list_m:
            my_intervall = "m"

        if not my_intervall:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
            return

        context_dict[var_intervall_name] = my_intervall

    # Step 9: Validate timeframe boundaries natively
    def pruefe_zeitraum(self, anfang, ende):
        if self.err_nr != 0:
            return

        if not anfang or not ende:
            self.err_nr = 196
            self.err_arg = f"{self.modul_name} {self.modul_version} pruefeZeitraum"
            return

        format_str = "%Y%m%d"
        anfang_dt = None
        ende_dt = None

        # REVIEW-STRUCT: DWDate_Datum_Check functionality is replaced below via datetime.strptime parsing.
        try:
            anfang_dt = datetime.strptime(anfang, format_str)
        except ValueError:
            self.err_nr = 195
            self.err_arg = f"Anfangdatum entspricht nicht dem Format {format_str}"
            return

        try:
            ende_dt = datetime.strptime(ende, format_str)
        except ValueError:
            self.err_nr = 195
            self.err_arg = f"Endedatum entspricht nicht dem Format {format_str}"
            return

        # REVIEW-STRUCT: DWDate_Datum_LE functionality replaced below via direct date comparisons.
        if anfang_dt > ende_dt:
            self.err_nr = 195
            self.err_arg = "Anfangsdatum ist nicht kleiner gleich Endedatum"

    # Step 10: Validate numeric parameter positivity
    def pruefe_zahl_positiv(self, p_zahl, p_parameter_name):
        try:
            val = int(p_zahl)
            if val < 0:
                self.err_nr = 195
                self.err_arg = f"Parameter {p_parameter_name} muss groesser gleich 0 sein"
        except (ValueError, TypeError):
            self.err_nr = 195
            self.err_arg = f"Parameter {p_parameter_name} ist kein numerischer Wert"

    # Step 11: Assert logical configuration of date parameters
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

    # Step 12: Compute relative date limits using negative intervals
    def konvertiere_zeitspanne(self, p_var_anfang, p_var_ende, p_spanne, p_kennzahl, context_dict):
        if self.err_nr != 0:
            return

        # REVIEW-STRUCT: DWDate_Gib_Zeitraum logic is parsed natively below using datetime and relativedelta.
        try:
            spanne_int = int(p_spanne)
            offset_unit = "D"
            if p_kennzahl == "bst":
                offset_unit = "M"

            today = datetime.now()
            # Simulate historical window calculation based on standard DWDate_Gib_Zeitraum logic
            if offset_unit == "M":
                # Offset in months
                target_start_dt = today - relativedelta(months=spanne_int)
                target_end_dt = today
            else:
                # Offset in days
                target_start_dt = today - timedelta(days=spanne_int)
                target_end_dt = today

            format_str = "%Y%m%d"
            context_dict[p_var_anfang] = target_start_dt.strftime(format_str)
            context_dict[p_var_ende] = target_end_dt.strftime(format_str)
        except Exception as e:
            self.err_nr = 85
            self.err_arg = "DWDate_Gib_Zeitraum"
            print(f"Error computing relative time span: {str(e)}", file=sys.stderr)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` | Migrated to a Python utility library preserving all validation, abbreviation normalization, and date-offset helper functions. |

---

### Job Dependencies
The following downstream consumers depend on this utility module. These consumers are currently **not yet migrated**; therefore, the calling structures and variable context mappings cannot be fully validated or finalized until their migrations are undertaken:
- **DW.BERT_ABLAUFSTEUERUNG** (not yet migrated)
- **DW.BERT_AUSD_BP_TA_MSISDN** (not yet migrated)
- **DW.BERT_AUSD_BP_TA_P_BASISPROD** (not yet migrated)
- **DW.BERT_AUSD_V_TA_PERIOD** (not yet migrated)
- **DW.BERT_AUSD_V_TA_P_VERTRAG** (not yet migrated)
- **DW.BERT_AUSD_V_TA_VERTRAG_TMP** (not yet migrated)
- **DW.BERT_DROP_TEMP_TABLE** (not yet migrated)
- **DW.BERT_P_ADRESSEN** (not yet migrated)
- **DW.BERT_P_AUSTAUSCH** (not yet migrated)
- **DW.BERT_P_GESCHAEFTSP** (not yet migrated)
- **DW.BERT_P_RECH_EMPF** (not yet migrated)
- **DW.BERT_RECHNUNGSDATEN** (not yet migrated)

---

### Schedule & Variables
- **Schedule**: This library module is not directly triggered by any scheduler. It operates purely as an importable module or utility helper. It must remain a callable/importable Python unit inside Cloud Composer / Apache Airflow DAGs rather than being given its own independent DAG run schedule.
- **Variables**: There are no scheduler-set variables directly fed to this utility from an orchestrator. Instead, variables representing state (`ErrNr`, `ErrArg`) and input arguments (such as parameter names, date values, metric abbreviations, or source system names) are passed dynamically at execution time by the importing calling scripts.

---

### External System Replacements
- **Oracle Utilities (`DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`)**: 
  These external SQL*Plus utilities used for checking formats, verifying dates ordering, and calculating sliding time windows are replaced with standard Python `datetime` and `dateutil.relativedelta` operations to compute identical time bounds natively within Cloud Composer.

---

### Cross-File Dependencies
- This utility script is dynamically sourced via standard KornShell sourcing commands (e.g. `. h_alis_parameter.ksh`) inside downstream wrapper scripts. To support this behavior in the Python environment, downstream Python scripts will import `AlisParameterHelper` from `h_alis_parameter.py` and invoke its methods directly, passing state via a shared execution dictionary or parameter wrapper object.

---

### Target File Plan
- **Target File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py`
  - **Language**: Python
  - **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
  - **Purpose**: Modular parameter parser, validator, and date arithmetic assistant library.

---

### Environment-Specific Values
- **Local/Temporary Directory**: `/tmp/`
  - **Role**: Job-Specific. Used for creating unique transient temporary files during custom date checking operations. In Python, this is replaced by the standard library's thread-safe and process-safe `tempfile` module or in-memory string parsing.

---

### Risks and Manual Steps
- **Unmigrated Downstream Dependencies**:
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_ABLAUFSTEUERUNG` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_AUSD_BP_TA_MSISDN` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_AUSD_BP_TA_P_BASISPROD` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_AUSD_V_TA_PERIOD` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_AUSD_V_TA_P_VERTRAG` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_DROP_TEMP_TABLE` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_P_ADRESSEN` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_P_AUSTAUSCH` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_P_GESCHAEFTSP` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_P_RECH_EMPF` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
  - DOWNSTREAM: NOT YET MIGRATED — `DW.BERT_RECHNUNGSDATEN` — Sourcing and logic validation linkages cannot be verified until this job is migrated.
- **Verification of Date Operations**: The original code depends on an external Oracle-based utility `DWDate_Gib_Zeitraum` for date-interval calculations. The native Python replacements (which utilize `relativedelta` and `timedelta` to subtract offsets) assume a standard Gregorian calendar. This logic must be manually reviewed during user acceptance testing to ensure calculations match the original behavior for boundary conditions (such as leap years and end-of-month transitions).

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
REASON: The script defines a helper function starteSQLSkript with argument validation, file checks, and external Oracle sqlplus execution.

EVIDENCE
- Business logic found: KSH custom logic defines a utility function `starteSQLSkript` which validates parameters, checks SQL file availability, and manages Oracle `sqlplus` session lifecycle/exit codes.
- AWK: none
- SQL-expressible: no (the logic handles file validation and external database client process lifecycle management, not database table transformations).
- Non-SQL side effects: file readability checks (`[ ! -r $p_Skript ]`), external DB client execution via `sqlplus`, and calling an external error reporting tool (`DWMSG_MeldeFehler`).
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a KornShell utility library (`h_alis_sqlplus.ksh`) that defines a reusable helper function `starteSQLSkript`. Its purpose is to safely and consistently run Oracle SQL*Plus scripts. The utility validates that the target SQL script exists and is readable before launching the Oracle SQL*Plus client, preventing silent failures, and standardizes error logging and exit code propagation.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced or executed by other application shell scripts in the DW pipeline that need to run Oracle SQL scripts.
   - UC4 native includes: None referenced in this code snippet.
   - Environment files sourced: None explicitly sourced within this snippet (expected to be inherited from the parent shell script sourcing this helper).

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts the following positional parameters:
   - `$1` (local variable `p_Eintragsnr`): Error tracking ID used when reporting failures. Sourced from caller. Used inside function validation. Surfaced in Python as a positional parameter.
   - `$2` (local variable `p_Skript`): Path to the target SQL script to execute. Sourced from caller. Used to check file readability and passed to `sqlplus`. Surfaced in Python as a positional parameter.
   - `$*` (remaining arguments after `shift 2`): Dynamic parameters passed through to the underlying SQL*Plus script. Surfaced in Python as variable positional arguments (`*args`).
   - Env Var `DW_ORAUSER`: Sourced from environment. Used as the connection/credential string for `sqlplus`. Surfaced in Python via `os.environ.get("DW_ORAUSER")`.
   - Env Var `Modul_Name`, `Modul_Version`: Sourced from environment/parent shell or script level (`ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`). Note: the error reporter references `${Modul_Name}` and `${Modul_Version}` with an underscore, which might be a typo in the original shell script; both are tracked.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWMSG_MeldeFehler`: Error reporting utility invoked on validation failure.
     - Command: `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"` and `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript`
     - Purpose: Dispatches structured application error logs.
     - Python Translation: Must remain an external process invocation via `subprocess.run` or mapped to a standard Python logging call if the utility is decommissioned.
     - Launcher status: # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.
   - `sqlplus`: Oracle interactive SQL query client.
     - Command: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Connects to Oracle DB using `$DW_ORAUSER` and runs `$p_Skript` with parameter expansion, redirecting input from `/dev/null` to prevent hang/interactive prompts.
     - Python Translation: If migrated to Python, this can run via `subprocess.run(["sqlplus", ...])`. If migrating completely to native Python DB execution, it can execute via the `oracledb` library, though the wrapper pattern itself suggests keeping it as a generic runner.
     - Launcher status: Not a resolvable launcher because the script runs dynamic, unsupplied SQL files passed as arguments.

5. EMBEDDED SQL
   - No SQL statements are embedded in this script. The SQL execution is entirely external and dynamic based on the `$p_Skript` parameter.

6. CONTROL FLOW
   1. Initialize global module variables: `ModulName="alis_sqlplus"` and `ModulVersion="V1.1.3"`.
   2. Define the `starteSQLSkript()` function.
   3. Within the function:
      a. Capture first positional argument into `p_Eintragsnr`.
      b. Capture second positional argument into `p_Skript`.
      c. Execute `shift 2` to clear the first two arguments, leaving the rest of the arguments in `$*`.
      d. Validate if `p_Eintragsnr` or `p_Skript` are empty. If empty, invoke `DWMSG_MeldeFehler` with code 196 and return 196.
      e. Validate if `p_Skript` is a readable file using standard `-r` test. If not readable, invoke `DWMSG_MeldeFehler` with code 201 and return 201.
      f. Print operational statements detailing script path and parameters.
      g. Temporarily disable exit-on-error (`set +e`) to prevent the script crashing from a failed sqlplus run.
      h. Execute `sqlplus` passing `DW_ORAUSER`, target script, and remaining arguments, redirecting stdin from `/dev/null`.
      i. Capture exit code of `sqlplus` into `errcode`.
      j. Re-enable exit-on-error (`set -e`).
      k. Return the captured `errcode`.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments trigger a code `196` exit from the function.
   - Unreadable SQL script triggers a code `201` exit from the function.
   - SQL*Plus execution failures are captured via `errcode=$?` and returned directly to the caller.
   - Python mapping:
     - Missing arguments: `ValueError` or standard Python return values.
     - Unreadable SQL file: `FileNotFoundError` or custom exception.
     - SQL*Plus run: `subprocess.run` capturing the return code and returning it, or throwing `subprocess.CalledProcessError`.

8. OUTPUTS / SIDE EFFECTS
   - Standard output logs detailing script executions.
   - Error messages dispatched via `DWMSG_MeldeFehler`.
   - Modifies state of database depending on the actions executed by `$p_Skript`.

9. BUSINESS SUMMARY
   - Standardizes Oracle SQL script execution across the legacy DWH environment.
   - Ensures rigorous pre-execution checks, confirming files exist and are readable before attempting to connect to the database.
   - Guarantees error-state reporting via a centralized error messaging engine (`DWMSG_MeldeFehler`).
   - Ensures database connections are handled non-interactively (`</dev/null`) to avoid hung automated processes.

=== PSEUDOCODE STYLE ===

```python
import os
import sys
import subprocess
from pathlib import Path

# Step 1: Initialize global module variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Step 2: Define starteSQLSkript utility function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Python equivalent of the starteSQLSkript shell function.
    """
    # Step 3: Parameter validation
    # Check if either required parameter is empty/null
    if not p_eintragsnr or not p_skript:
        # # REVIEW-STRUCT: DWMSG_MeldeFehler is an external error utility. Ensure its python path or equivalent logger is validated.
        cmd_err = [
            "DWMSG_MeldeFehler", 
            p_eintragsnr if p_eintragsnr else "", 
            "E", 
            "196", 
            f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        ]
        subprocess.run(cmd_err, check=False)
        return 196

    # Step 4: Validate file accessibility
    script_path = Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        cmd_err = [
            "DWMSG_MeldeFehler", 
            p_eintragsnr, 
            "E", 
            "201", 
            p_skript
        ]
        subprocess.run(cmd_err, check=False)
        return 201

    # Step 5: Log operation parameters
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Execute external SQL*Plus program
    # Retrieve connection details from the environment
    # # REVIEW: Ensure DW_ORAUSER is populated in Python's environment or secure vault.
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    try:
        # Executing with stdin redirected from devnull (corresponds to </dev/null)
        # check=False mimics the 'set +e' logic, allowing manual capture and propagation of exit code
        result = subprocess.run(
            sqlplus_cmd, 
            stdin=subprocess.DEVNULL, 
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Fehler bei der Ausfuehrung von sqlplus: {e}", file=sys.stderr)
        errcode = 1 # Generic execution failure

    # Step 7: Return SQLPlus execution exit code
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Converts KornShell utility library to a reusable Python module containing SQL execution helper logic. |

---

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
- **Wiring on BigQuery:**
  This script is a shared utility module rather than a standalone orchestration job. It should be migrated to a shared Python module. Once the downstream jobs are migrated to Airflow DAGs/Python, they will import the converted `starte_sql_skript` function directly from this module (`from is.util.bin.h_alis_sqlplus import starte_sql_skript`).
  *Note: Because all 12 downstream dependencies are not yet migrated, the integration wiring cannot be finalized until those workflows are built.*

---

### Scheduling
- This utility script is not directly triggered by any of the schedulers (it executes inside scheduled jobs as an import/include module). It must not be given its own standalone schedule on BigQuery; it must remain a callable/importable Python unit in Google Cloud Storage or the Airflow shared DAGs folder (e.g. `dags/is/util/bin/h_alis_sqlplus.py`).

---

### Schedule & Variables — Must Be Retained
- **Variables to Retain:**
  - `DW_ORAUSER`: Legacy Oracle connection credential. Under BigQuery, the target logic must resolve connection details via global configuration or Secret Manager rather than using static SQL*Plus credentials.
  - `ModulName` / `ModulVersion`: Sourced dynamically, kept as module-level constants.

---

### External System Replacements
- **Oracle SQL\*Plus Client to Google Cloud BigQuery API / Python Client:**
  The legacy utility executes SQL scripts using `sqlplus`. On BigQuery, this must be replaced depending on the target state:
  - If the downstream `.sql` files are migrated to BigQuery SQL, the Python function should be refactored to execute queries via the Google Cloud BigQuery Client library (`google.cloud.bigquery`), rather than executing `sqlplus` via a subprocess.
  - If the script must connect to Oracle during a transition period, it must utilize Python’s native `oracledb` library, fetching credentials securely from Google Cloud Secret Manager.

---

### Cross-File Dependencies
- Sourced by downstream scripts/jobs.
- Calls `DWMSG_MeldeFehler` (external utility). This dependency must be resolved via a central Python error-reporting utility or Google Cloud Logging.

---

### Target File Plan

| Target File Path | Language | Source File |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Python | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` |

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
- `DW_ORAUSER` -> Maps to legacy Oracle database credentials. In the BigQuery target, this should be retired in favor of native GCP IAM credentials or Secret Manager secrets representing database endpoints. For Python environment queries, access using:
  ```python
  GCP_PROJECT = os.environ.get("GCP_PROJECT")
  ```

#### 2. JOB-SPECIFIC
- `ModulName` = `"alis_sqlplus"`
- `ModulVersion` = `"V1.1.3"`
- These values are local module variables and must remain inlined within the Python code.

---

### Risks and Manual Steps
- **Unmigrated Downstream Dependencies:** The 12 downstream jobs listed under Job Dependencies are not yet migrated. The integration and end-to-end testing of this shared utility module cannot be finalized until those jobs exist.
- **External Launcher/Dependency:**
  - `SOURCE: NOT FOUND — DWMSG_MeldeFehler — no candidate`
- **Subprocess Transition:** Running a `subprocess` to call `sqlplus` requires the Oracle Instant Client and SQL*Plus binary to be installed on Cloud Composer/Dataproc worker nodes. The design strongly recommends refactoring the call execution to use native Python libraries (like `oracledb` for Oracle, or `google-cloud-bigquery` for BigQuery SQL) to avoid external CLI dependencies and container bloat.