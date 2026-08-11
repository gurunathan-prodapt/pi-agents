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
REASON: The script defines a set of utility functions for error handling and logging that contain dynamic shell logic, variable assignment, file handling, and SQL*Plus execution, requiring a Python implementation.

EVIDENCE
- Business logic found: KSH custom logic. The script defines nine utility functions for error management, state logging, timing metrics collection, and Oracle PL/SQL procedure execution.
- AWK: none
- SQL-expressible: partly. While the underlying operations are database-driven (calling `BERT_MELDUNG` packaged procedures), the script itself is a shell-based framework managing environment state, error trap handlers, temp files, and dynamic variable assignments.
- Non-SQL side effects: Creation and deletion of temporary files, date formatting, dynamic shell variable evaluation (`eval`), and standard error reporting.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This script, `f_alis_msgerr.ksh`, is a central utility library providing error management and execution logging functions for the "Information Services" system. It does not run as a standalone executable; rather, it is sourced by other KornShell scripts to establish unified logging, record success/failure statuses in an Oracle database, register run execution entries, format log file names, and handle uncaught shell errors. 

### 2. INVOCATION CONTEXT
- **Caller**: This library is sourced (`. f_alis_msgerr.ksh`) by other master and worker ksh scripts in the environment.
- **UC4 Job / JOBS_UNIX**: Not supplied in the extraction.
  - # REVIEW-STRUCT: UC4 invocation context and JOBS_UNIX caller object not supplied — behavior assumed to be standard sourcing.
- **UC4 Native Includes**: Not supplied in the extraction.
- **Environment Files Sourced**: This script relies on several environment variables being pre-set by sourcing wrappers (such as `.ccr_init` or equivalent), specifically:
  - `DW_ORAUSER`: The Oracle database connection string.
  - `DW_DIR_ROOT`: Root directory for system scripts.
  - `DW_DIR_PROT`: Root directory for log and protocol files.

### 3. PARAMETERS / INPUTS
The script defines multiple functions, each accepting specific arguments.

| Function | Argument | Source | Description | Python Surface |
|---|---|---|---|---|
| **DWMSG_Fehlerbehandlung** | `$1` (DWMSG_EintragsNr) | Caller function | Unique ID of the log entry. | Method argument |
| **DWMSG_SetzeStatusOK** | `$1` (DWMSG_EintragsNr) | Caller function | Unique ID of the log entry. | Method argument |
| **DWMSG_SetzeStatusAbbruch** | `$1` (DWMSG_EintragsNr) | Caller function | Unique ID of the log entry. | Method argument |
| **DWMSG_ErmittleNr** | `$1` (VarName) | Caller function | Shell variable name to receive the generated ID. | Returns value (int) |
| **DWMSG_ErzeugeEintrag** | `$1` (DWMSG_EintragsNr)<br>`$2` (JobKennung)<br>`$3` (Programmname)<br>`$4` (LogDatei) | Caller function | Log ID, Job ID, Program name, and log file path. | Method arguments |
| **DWMSG_MeldeFehler** | `$1` (DWMSG_EintragsNr)<br>`$2` (Typ)<br>`$3` (FehlerNr)<br>`$4` (Zusatz1, opt)<br>`$5` (Zusatz2, opt) | Caller function | Log ID, error severity (F/E/W), error number, and optional context strings. | Method arguments with defaults |
| **DWMSG_Logdateiname** | `$1` (VarName)<br>`$2` (JobKennung)<br>`$3` (DWMSG_EintragsNr) | Caller function | Variable name, Job ID, and Log ID to format file name. | Returns formatted string |
| **DWMSG_SetzeStichtagInfo** | `$1` (DWMSG_EintragsNr)<br>`$2` (DWMSG_Stichtag)<br>`$3` (DWMSG_StichtagFmt) | Caller function | Log ID, target business date, and date format string. | Method arguments |
| **DWMSG_AppendTimingInfos** | `$1` (DWMSG_EintragsNr)<br>`$2` (DWMSG_InfoText)<br>`$3` (DWMSG_DateFormat) | Caller function | Log ID, description text, and date formatting string. | Method arguments |

**Ksh Declared Environment Parameters Reference**:
- `DW_ORAUSER`: Used to connect to the database. This suggests an Oracle database target platform.
- `DW_DIR_ROOT`: Points to the base path for SQL script templates.
- `DW_DIR_PROT`: Points to the base path for log/protocol generation.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The script interacts with SQL*Plus to execute database actions.

- **Command Line**: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
  - **Purpose**: Mark a batch run as successful in the database tracking table.
  - **Conversion**: Becomes a native Python DB-client call (`cursor.callproc('BERT_MELDUNG.SetzeStatusOk', [eintrags_nr])`).

- **Command Line**: `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
  - **Purpose**: Mark a batch run as aborted in the database tracking table.
  - **Conversion**: Becomes a native Python DB-client call (`cursor.callproc('BERT_MELDUNG.SetzeStatusAbbruch', [eintrags_nr])`).

- **Command Line**: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
  - **Purpose**: Generates a new sequence number using a database generator, writing it to a temporary file, which is then read back into KSH via `cat`.
  - **Conversion**: Becomes a native Python DB-client call. In Python, we can directly retrieve the sequence value from a query (`SELECT sequence.NEXTVAL ...` or an equivalent PL/SQL function return) without creating temporary files on disk.

- **Command Line**: `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
  - **Purpose**: Registers a new tracking entry.
  - **Conversion**: Becomes a native Python DB-client call (`cursor.callproc('BERT_MELDUNG.Erzeuge_Eintrag', [...])`).

- **Command Line**: `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
  - **Purpose**: Registers an error record.
  - **Conversion**: Becomes a native Python DB-client call (`cursor.callproc('BERT_MELDUNG.Fehler', [...])`).

- **Command Line (Inline SQL)**: 
  ```sql
  sqlplus -s $DW_ORAUSER <<EOF
    EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
    commit;
  EOF
  ```
  - **Purpose**: Updates the record with business date details.
  - **Conversion**: Becomes a native Python DB-client call.

- **Command Line (Inline SQL)**:
  ```sql
  sqlplus -s $DW_ORAUSER <<EOF
    EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
    commit;
  EOF
  ```
  - **Purpose**: Appends timing/metric annotations to the database run log.
  - **Conversion**: Becomes a native Python DB-client call.

### 5. EMBEDDED SQL
The script calls external `.sql` files with arguments and runs inline PL/SQL blocks.

- **Source Files**: 
  - `d_alis_spaufruf_p1.sql` (Takes 1 SP parameter + SP Name)
  - `d_alis_spaufruf_p4.sql` (Takes 4 SP parameters + SP Name)
  - `d_alis_spaufruf_p3.sql` / `p4.sql` / `p5.sql` (Takes dynamic parameter counts)
  - `d_al_is_ermittlenr.sql` (Generates sequence value and writes to output file)
- **SQL Dialect**: Highly Oracle-specific (`sqlplus`, PL/SQL packages `BERT_MELDUNG`, database sequence generators, `to_date`, `to_char(SYSDATE, ...)`).
  - # REVIEW-STRUCT: SQL file bodies (`d_alis_spaufruf_p*.sql`, `d_al_is_ermittlenr.sql`) are not supplied in this extraction — behaviour has been inferred from their positional arguments and stored procedure calls.

### 6. CONTROL FLOW
The script contains function definitions only. When imported/sourced, no execution occurs until functions are called:

1. **`DWMSG_Fehlerbehandlung`**:
   - Captures previous shell exit code (`FehlerNr=$?`).
   - Invokes `DWMSG_MeldeFehler` with fatal code `10` and the captured status.
   - Invokes `DWMSG_SetzeStatusAbbruch` to flag the batch run as failed.
2. **`DWMSG_SetzeStatusOK`**:
   - Asserts entry ID is provided (exits 1 if null).
   - Runs `BERT_MELDUNG.SetzeStatusOk` via SQL*Plus.
3. **`DWMSG_SetzeStatusAbbruch`**:
   - Asserts entry ID is provided (exits 1 if null).
   - Runs `BERT_MELDUNG.SetzeStatusAbbruch` via SQL*Plus.
4. **`DWMSG_ErmittleNr`**:
   - Asserts receiving variable name is provided (exits 1 if null).
   - Generates unique temp filename `/tmp/ErmittleNr_<PID>.lst`.
   - Runs `d_al_is_ermittlenr.sql` via SQL*Plus to populate the temp file.
   - Reads sequence ID from the temp file and strips spaces.
   - Deletes the temp file.
   - Uses `eval` to assign the retrieved ID to the requested variable.
5. **`DWMSG_ErzeugeEintrag`**:
   - Asserts entry ID is provided.
   - Invokes `BERT_MELDUNG.Erzeuge_Eintrag` via SQL*Plus.
6. **`DWMSG_MeldeFehler`**:
   - Asserts entry ID is provided.
   - Evaluates optional parameters `$4` and `$5`.
   - Determines the number of parameters to load the correct script wrapper (`d_alis_spaufruf_p3.sql`, `_p4.sql`, or `_p5.sql`).
   - Invokes `BERT_MELDUNG.Fehler` via SQL*Plus.
7. **`DWMSG_Logdateiname`**:
   - Resolves target variable name, job identifier, and entry ID.
   - Computes a standard path under `$DW_DIR_PROT` formatted as: `{DW_DIR_PROT}/{JobKennung}_{YYYYMMDD_HHMM}_{DWMSG_EintragsNr}.log`.
   - Uses `eval` to assign the value back to the caller's variable.
8. **`DWMSG_SetzeStichtagInfo`**:
   - Asserts all three arguments are provided (exits 1 or 2 on failures).
   - Calls `BERT_MELDUNG.SetzeZusatzInfos` passing the converted business date.
9. **`DWMSG_AppendTimingInfos`**:
   - Asserts all three arguments are provided.
   - Calls `BERT_MELDUNG.SetzeZusatzInfos` appending a timestamped timing metric string.

### 7. ERROR HANDLING & EXIT CODES
- The KSH script exits with code `1` or `2` if mandatory validation arguments are missing when its functions are called.
- In `DWMSG_Fehlerbehandlung`, it catches whatever error status was present (`$?`) when a trap fired, using that to log the error to the database.
- **Python Mapping**:
  - Missing parameters will raise standard Python exceptions (`ValueError`).
  - Database access issues will raise database driver-specific exceptions (e.g., `oracledb.DatabaseError`), which should be caught and logged.

### 8. OUTPUTS / SIDE EFFECTS
- **Database Entries**: Creates, modifies, and commits rows in log tracking tables via `BERT_MELDUNG` PL/SQL package procedures.
- **Log Files**: Standardized log paths are calculated (though physical file creation is handled by the calling program).
- **Temp Files**: Writes to and cleans up `/tmp/ErmittleNr_*.lst`. (Eliminated in Python).

### 9. BUSINESS SUMMARY
- **Unified Run Registry**: Generates tracking identifiers to correlate steps of a batch execution.
- **Standardized Exception Trapping**: Intercepts shell failures, logs detailed application errors into the database, and flags runs as aborted.
- **Metric Collection**: Captures runtime benchmarks and performance timing milestones.
- **Data Partition Association**: Flags run tracking entries with their associated business date context.

---

### RESOLVABLE LAUNCHER PATTERN ANALYSIS
This script qualifies as a **Resolvable Launcher Pattern** target:
- The wrappers called (`d_alis_spaufruf_p*.sql`) are thin SQL wrappers executing single stored procedure calls (`BERT_MELDUNG.SetzeStatusOk`, etc.).
- The environment configuration uses `DW_ORAUSER` which indicates Oracle.
- **Recommendation**: Instead of calling `subprocess.run(["sqlplus", ...])`, implement these functions as native database client interactions using a modern Python DB driver (e.g., `oracledb`). 
- # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names (DW_ORAUSER) are set in this job's actual runtime environment before deploying.

---

### PSEUDOCODE / TARGET PYTHON IMPLEMENTATION

```python
# dw_msg_err.py
"""
Modern Python equivalent for f_alis_msgerr.ksh.
Provides structured logging and error management interfacing with Oracle Database.
"""

import os
import sys
import tempfile
from datetime import datetime
import oracledb  # Modern successor to cx_Oracle

# # REVIEW-STRUCT: environment file variables are assumed to be loaded into os.environ.
# Ensure DW_ORAUSER, DW_DIR_ROOT, and DW_DIR_PROT are available.

class DWMsgManager:
    def __init__(self, dsn=None):
        self.dsn = dsn or os.environ.get("DW_ORAUSER")
        self.dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
        self.dir_root = os.environ.get("DW_DIR_ROOT")
        
        if not self.dsn:
            # # REVIEW: target database platform assumed to be Oracle based on sqlplus and PL/SQL usages.
            print("WARNING: DW_ORAUSER environment variable not found.", file=sys.stderr)

    def _get_connection(self):
        """Helper to establish a connection to Oracle."""
        # Parsing DW_ORAUSER (typically 'user/password@host:port/service' or TNS name)
        # For security and modern practices, credentials should ideally come from a secret manager.
        return oracledb.connect(dsn=self.dsn)

    # Step 1: DWMSG_Fehlerbehandlung
    def fehlerbehandlung(self, eintrags_nr: int, error_code: int = 1):
        """
        Handles uncaught errors trapped during execution.
        Fires a database failure log and sets execution status to Aborted.
        """
        k_unerw_fehler = 10
        print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus", file=sys.stderr)
        
        # Report error
        self.melde_fehler(
            eintrags_nr=eintrags_nr,
            typ="F",
            fehler_nr=k_unerw_fehler,
            zusatz1=f"ErrorCode ist: {error_code}"
        )
        # Set status to Aborted
        self.setze_status_abbruch(eintrags_nr)

    # Step 2: DWMSG_SetzeStatusOK
    def setze_status_ok(self, eintrags_nr: int):
        """Sets the tracking entry status to successful."""
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
            sys.exit(1)
            
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Executes BERT_MELDUNG.SetzeStatusOk(eintrags_nr)
                cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
                conn.commit()

    # Step 3: DWMSG_SetzeStatusAbbruch
    def setze_status_abbruch(self, eintrags_nr: int):
        """Sets the tracking entry status to aborted."""
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
            sys.exit(1)
            
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Executes BERT_MELDUNG.SetzeStatusAbbruch(eintrags_nr)
                cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
                conn.commit()

    # Step 4: DWMSG_ErmittleNr
    def ermittle_nr(self) -> int:
        """
        Retrieves a new unique log run identifier from the Oracle database.
        Replaces the legacy process of executing sqlplus with temporary files.
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Assuming d_al_is_ermittlenr.sql calls a sequence generator.
                # In Python, we query the sequence directly instead of writing to a local text file.
                # Adjust sequence name if confirmed in the database schema.
                cursor.execute("SELECT SEQ_BERT_MELDUNG_ID.NEXTVAL FROM DUAL")
                row = cursor.fetchone()
                if row:
                    return int(row[0])
                else:
                    raise RuntimeError("Failed to retrieve new ID sequence from Oracle.")

    # Step 5: DWMSG_ErzeugeEintrag
    def erzeuge_eintrag(self, eintrags_nr: int, job_kennung: str, programm_name: str, log_datei: str):
        """Creates a primary record log entry in BERT_MELDUNG table."""
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
            sys.exit(1)
            
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Executes BERT_MELDUNG.Erzeuge_Eintrag
                cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programm_name, log_datei])
                conn.commit()

    # Step 6: DWMSG_MeldeFehler
    def melde_fehler(self, eintrags_nr: int, typ: str, fehler_nr: int, zusatz1: str = "", zusatz2: str = ""):
        """Logs an error to BERT_MELDUNG."""
        if not eintrags_nr:
            print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
            sys.exit(1)
            
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Executes BERT_MELDUNG.Fehler(typ, eintrags_nr, fehler_nr, zusatz1, zusatz2)
                cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
                conn.commit()

    # Step 7: DWMSG_Logdateiname
    def logdateiname(self, job_kennung: str, eintrags_nr: int) -> str:
        """Returns the standardized run log filename path."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"{job_kennung}_{timestamp}_{eintrags_nr}.log"
        return os.path.join(self.dir_prot, filename)

    # Step 8: DWMSG_SetzeStichtagInfo
    def setze_stichtag_info(self, eintrags_nr: int, stichtag: str, stichtag_fmt: str):
        """Binds a business process date (Stichtag) to the logging tracking entry."""
        if not eintrags_nr:
            print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
            sys.exit(1)
        if not stichtag:
            print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
            sys.exit(1)
        if not stichtag_fmt:
            print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
            sys.exit(2)

        # Convert Java/Oracle date format conventions to Python if needed. 
        # Alternatively, let the Oracle database handle conversion via PL/SQL inside execution block.
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Execute PL/SQL block natively
                sql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, to_date(:stichtag, :stichtag_fmt));
                END;
                """
                cursor.execute(sql_block, {
                    'eintrags_nr': eintrags_nr,
                    'stichtag': stichtag,
                    'stichtag_fmt': stichtag_fmt
                })
                conn.commit()

    # Step 9: DWMSG_AppendTimingInfos
    def append_timing_infos(self, eintrags_nr: int, info_text: str, date_format: str):
        """Appends metrics/timing notes with date details."""
        if not eintrags_nr:
            print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
            sys.exit(1)
        if not date_format:
            print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
            sys.exit(2)

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Execute PL/SQL timing append natively
                sql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, null, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
                END;
                """
                cursor.execute(sql_block, {
                    'eintrags_nr': eintrags_nr,
                    'info_text': info_text,
                    'date_format': date_format
                })
                conn.commit()
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py` | Migrates KornShell utility functions to a reusable Python module (`f_alis_msgerr.py`) to manage standardized runtime logging, error mapping, and timing metrics on Google Cloud Platform. |

# Job Dependencies
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
- **Wiring on Target Platform**: These downstream dependencies are not yet migrated to the target GCP environment. In their eventual Python/Composer forms, they must import the migrated logging utility module (`f_alis_msgerr.py`) to perform execution registration, error trapping, and performance metrics tracking.

# Scheduling
- **Triggering/Execution**: This job is not directly triggered by any scheduler. It operates as a shared, importable utility library and must remain an importable Python library (`f_alis_msgerr.py`) rather than being scheduled as a standalone execution DAG.

# Schedule & Variables — Must Be Retained
- **Equivalent Schedule/Trigger**: Sourced as a shared helper library inside scheduled downstream jobs; it does not require a standalone Airflow DAG schedule.
- **Variables**: There are no scheduler-set variables mapped directly to this library.

# Lineage
- **Upstream Producers**: None.
- **Downstream Consumers**:
  - `DW.BERT_ABLAUFSTEUERUNG` (job: `DW.BERT_ABLAUFSTEUERUNG`)
  - `DW.BERT_AUSD_BP_TA_MSISDN` (job: `DW.BERT_AUSD_BP_TA_MSISDN`)
  - `DW.BERT_AUSD_BP_TA_P_BASISPROD` (job: `DW.BERT_AUSD_BP_TA_P_BASISPROD`)
  - `DW.BERT_AUSD_V_TA_PERIOD` (job: `DW.BERT_AUSD_V_TA_PERIOD`)
  - `DW.BERT_AUSD_V_TA_P_VERTRAG` (job: `DW.BERT_AUSD_V_TA_P_VERTRAG`)
  - `DW.BERT_AUSD_V_TA_VERTRAG_TMP` (job: `DW.BERT_AUSD_V_TA_VERTRAG_TMP`)
  - `DW.BERT_DROP_TEMP_TABLE` (job: `DW.BERT_DROP_TEMP_TABLE`)
  - `DW.BERT_P_ADRESSEN` (job: `DW.BERT_P_ADRESSEN`)
  - `DW.BERT_P_AUSTAUSCH` (job: `DW.BERT_P_AUSTAUSCH`)
  - `DW.BERT_P_GESCHAEFTSP` (job: `DW.BERT_P_GESCHAEFTSP`)
  - `DW.BERT_P_RECH_EMPF` (job: `DW.BERT_P_RECH_EMPF`)
  - `DW.BERT_RECHNUNGSDATEN` (job: `DW.BERT_RECHNUNGSDATEN`)
- **Database Logic Interactions**: 
  - Calls DB procedure: `BERT_MELDUNG.SetzeZusatzInfos` (represented via the `SETZEZUSATZINFOS` lineage edge) to record dynamic run-time metrics and business partition dates.

# External System Replacements
- **Legacy Interface**: KornShell executes Oracle PL/SQL package procedures inside database schema `BERT_MELDUNG` using interactive `sqlplus` CLI sessions.
- **Target Replacement**: 
  - If the application tracking database remains on Oracle during a phased migration, python connections should utilize the native `oracledb` python driver (thin mode) executing packaged PL/SQL calls directly, bypassing shell-level subprocesses.
  - If the tracking database is migrated to Google Cloud, these operations should be redirected to target logging tables in BigQuery or a dedicated tracking relational instance (such as Cloud SQL PostgreSQL), or natively integrated with Cloud Logging via GCP's standard `google-cloud-logging` Python library.

# Cross-File Dependencies
- **Sourced Wrapper Scripts**: The legacy utility dynamically invokes the following external SQL files containing PL/SQL command templates:
  - `d_alis_spaufruf_p1.sql` (single-parameter procedure wrappers)
  - `d_alis_spaufruf_p3.sql` / `_p4.sql` / `_p5.sql` (multi-parameter dynamic procedures)
  - `d_al_is_ermittlenr.sql` (Oracle sequence number getter)
- **Target Resolution**: These external text wrapper files are retired. They are replaced by native SQL execution blocks run via database cursor adapters inside `f_alis_msgerr.py`.

# Target File Plan
- **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py`
- **Target Language**: Python
- **Source File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`

# Environment-Specific Values

### 1. GLOBAL (Environment-Wide)
- **`DW_ORAUSER`**
  - *Classification*: GLOBAL
  - *Target Mechanism*: DB Connection String/Credential mapping. Sourced dynamically from Airflow Connections or Cloud Secret Manager. It must not be written as a hardcoded literal.
- **`DW_DIR_PROT`**
  - *Classification*: GLOBAL
  - *Target Mechanism*: Standardized log deposit storage bucket. Maps to the canonical environment-wide GCS bucket configuration:
    ```python
    GCS_BUCKET = os.environ.get("GCS_BUCKET")
    ```
- **`DW_DIR_ROOT`**
  - *Classification*: GLOBAL
  - *Target Mechanism*: Reference path of system code files. In Cloud Composer, maps to `/home/airflow/gcs/dags/` or is sourced dynamically:
    ```python
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    ```

### 2. JOB-SPECIFIC
- **`BERT_MELDUNG` (Logging Target API/Schema)**
  - *Classification*: JOB-SPECIFIC
  - *Target Mechanism*: Keeps its semantic name as the target DB package namespace. Concrete parameters are passed as inline values in database connection contexts.

# Risks and Manual Steps
- **Database Sequence Replacement**: The original code queries an Oracle-based sequence inside `d_al_is_ermittlenr.sql` to get a unique run ID. If moving fully to BigQuery, BigQuery does not natively support sequential generator objects (`NEXTVAL`). The unique identifier generation must be re-architected (e.g., using a UUID generator in Python or utilizing Cloud SQL PostgreSQL sequence capabilities).
- **Not-Yet-Migrated Downstreams**: All listed downstream callers are currently marked "not yet migrated". Their integration with `f_alis_msgerr.py` cannot be fully validated or finalized until their migrations are actively executed.
- **Trap Interceptor Matching**: In legacy KSH, scripts captured uncaught errors globally using shell-level `trap 'DWMSG_Fehlerbehandlung ...' ERR`. In python-based jobs, this pattern does not exist natively; callers must use structured `try...except` blocks or Airflow DAG `on_failure_callback` hooks to invoke the migrated `fehlerbehandlung` execution route.
- **Missing External SQL Implementations**: The files `d_alis_spaufruf_p*.sql` and `d_al_is_ermittlenr.sql` are not included in the pre-collected context. Implementers must manually verify the exact structure of these legacy SQL wrappers to ensure parameter counts and data types correspond precisely.

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
REASON: This is a shared date utility library script defining multiple functions that perform date validations, calendar math (such as leap years and day additions), and SQL-based date range calculations, which must be converted into a reusable Python module.

EVIDENCE
- Business logic found: KSH custom logic. The script defines modular helper functions for validating dates, calculating month boundaries, and adding days using custom leap-year algorithms and database lookups.
- AWK: none
- SQL-expressible: Partly. While individual queries inside functions are SQL-compatible, the script itself is a stateful library containing procedural control flow (loops, variables, local arithmetic, shell arrays) and cannot be executed as a single BigQuery SQL transformation.
- Non-SQL side effects: Writes and cleans up temporary files in `/tmp/`, uses `eval` to assign return values to dynamically named variables in the caller's shell environment, and manages subprocess return codes.
- Against this verdict: One could theoretically implement each date utility function as a BigQuery SQL User-Defined Function (UDF) or Stored Procedure. However, because this script is sourced as a shell library to assist orchestrations, a Python utility module is required to maintain compatibility with Python-based orchestrations.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_date.ksh`) is a reusable utility library providing helper functions for date arithmetic, formatting, and validation. It is designed to be sourced (`. h_alis_date.ksh`) by other ETL and orchestration shell scripts. The logic is split between direct local shell arithmetic (e.g., calculating leap years and adding offsets) and external database executions via SQL*Plus to perform more complex Oracle-specific date operations.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by downstream data warehouse ETL shell scripts. It is not run directly as a standalone UC4 job, but is used as a runtime dependency.
   - UC4 native includes: None referenced in this script.
   - Environment files sourced: Requires the execution environment to have already executed `.dw_init` (not supplied) or defined `DW_DIR_ROOT` and `DW_ORAUSER`.
     # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values.

3. PARAMETERS / INPUTS
   The functions in this library utilize positional parameters passed during function invocation:
   - `DWDate_Vormonat`:
     - `$1` (`VarName`): Target variable name to hold the calculated previous month value.
     - `$2` (`DWDate_FMT`): Oracle format string for date parsing/formatting.
   - `DWDate_Datum_Check`:
     - `$1` (`wert`): The date string to validate.
     - `$2` (`format`): The expected date format.
   - `DWDate_Datum_LE`:
     - `$1` (`datum1`): First date string (assumed `YYYYMMDD`).
     - `$2` (`datum2`): Second date string (assumed `YYYYMMDD`).
   - `DWDate_Gib_Zeitraum`:
     - `$1` (`Offset`): Numeric offset (integer).
     - `$2` (`Stufe`): Time unit step ('Y' for Year, 'M' for Month, 'D' for Day).
     - `$3` (`Format`): Output format of the returned dates.
     - `$4` (`Var_Start`): Target variable name to store the start date (System Date).
     - `$5` (`Var_Ende`): Target variable name to store the calculated end date.
   - `LetzterTagDesMonats`:
     - `$1` (unnamed): Input date string (format `YYYYMMDD`).
   - `TageimMonat`:
     - `$1` (unnamed): Input year (format `YYYY`).
     - `$2` (unnamed): Input month (format `MM`).
   - `AddiereDatum`:
     - `$1` (unnamed): Base date string (format `YYYYMMDD`).
     - `$2` (unnamed): Number of days to add (positive integer).

   *KSH Declared Environment Parameters:*
   - `DW_ORAUSER`: Database connection user string.
   - `DW_DIR_ROOT`: Root directory path for data warehouse scripts.
   These are typical database connection/configuration variables.
   # REVIEW: target database platform not specified; DB-client library choice below is provisional.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Used to query the database for date math.
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_vormonat.sql ...`
       - Purpose: Runs an external SQL script to find the previous month.
       - Target: Python native database connection (oracledb, psycopg2, or google-cloud-bigquery).
     - `sqlplus -s <<EOF ... (inline TO_DATE check)`
       - Purpose: Validates if a string matches a database-supported date format.
       - Target: Convert to Python's native `datetime.strptime`.
     - `sqlplus -s <<EOF ... (inline PL/SQL block)`
       - Purpose: Asserts that date1 is less than or equal to date2, raising an exception if not.
       - Target: Convert to native Python comparative logic (`date1 <= date2`).
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_datum_zeitraum.sql ...`
       - Purpose: Calculates start and end dates based on offset and interval.
       - Target: Convert to native Python `datetime` and `dateutil.relativedelta` operations.

5. EMBEDDED SQL
   - **Source**: `DWDate_Datum_Check` inline script
     ```sql
     select to_date('$wert','$format') from dual;
     ```
     - Statement Type: `SELECT`
     - Tables touched: `dual`
     - Dialect: Oracle SQL (uses `to_date` and `dual` table).
   - **Source**: `DWDate_Datum_LE` inline script
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
     ```
     - Statement Type: Anonymous PL/SQL Block.
     - Tables touched: None.
     - Dialect: Oracle PL/SQL (unambiguously Oracle-specific constructs: `DECLARE`, `raise_application_error`, assignment operators `:=`).

6. CONTROL FLOW
   1. **Initialization**: Shell functions are defined and stored in memory when this script is sourced. No immediate execution occurs.
   2. **`DWDate_Vormonat`**:
      - Creates a temporary file in `/tmp/`.
      - Executes `sqlplus` calling `d_alis_vormonat.sql`.
      - Reads output from temp file and assigns it to the dynamically specified variable name.
      - Cleans up the temp file.
   3. **`DWDate_Datum_Check`**:
      - Validates that exactly 2 arguments are provided.
      - Connects via `sqlplus` and executes `select to_date(...)`.
      - Propagates the exit code (`$?`) of the SQL session (0 for success, non-zero for format failure).
   4. **`DWDate_Datum_LE`**:
      - Validates that exactly 2 arguments are provided.
      - Connects via `sqlplus` and runs a PL/SQL assertion block.
      - Propagates the exit code (`$?`).
   5. **`DWDate_Gib_Zeitraum`**:
      - Validates that exactly 5 arguments are provided.
      - Generates a timestamped temporary file in `/tmp/`.
      - Runs `sqlplus` calling `d_alis_datum_zeitraum.sql` passing the temp file path and criteria.
      - Greps the temp file for `"DWH_Ergebnis;"` to verify a single output line was generated.
      - Parses the start and end dates from the semicolon-delimited output line.
      - Assigns values to the dynamic variable names using `eval`.
      - Cleans up the temp file.
   6. **`LetzterTagDesMonats`**:
      - Parses year, month, and day substrings.
      - Determines if the year is a leap year using modulo arithmetic.
      - Assigns days-per-month array (`LetzterTag`).
      - Compares input day with the max day of the month; returns 0 if matches (is last day), else 1.
   7. **`TageimMonat`**:
      - Computes leap year logic for a given year.
      - Returns (prints) the number of days for the specified month.
   8. **`AddiereDatum`**:
      - Deconstructs input date `YYYYMMDD` into year, month, day components.
      - Natively adds days to the day component.
      - Executes a `while` loop to adjust days and increment months (calling `TageimMonat`).
      - Executes a nested `while` loop to adjust months and increment years.
      - Pads day, month, and year with leading zeros using `tail`.
      - Outputs the resulting `YYYYMMDD` string.

7. ERROR HANDLING & EXIT CODES
   - Missing function arguments return standard shell status `1`.
   - SQL*Plus uses `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` to convert database errors into process failure exit codes.
   - In Python, all local calculations should use Python exceptions (`ValueError`, `TypeError`), while database queries should raise driver-specific exceptions (`oracledb.DatabaseError` or `google.cloud.exceptions.GoogleCloudError`).
   - Dynamic variable assignments (`eval`) in Python should be replaced by returning tuple/dictionary values from functions.

8. OUTPUTS / SIDE EFFECTS
   - Temporary file creations in `/tmp/` during standard execution (fully cleaned up on successful execution).
   - Standard output outputting calculation results (e.g., `AddiereDatum` outputs `YYYYMMDD`).

9. BUSINESS SUMMARY
   - Standardizes date arithmetic across all corporate ETL pipelines.
   - Provides leap-year-safe calendar logic for reporting periods (determining month-end boundaries).
   - Validates business-date parameters before running long-running database transformations.
   - Calculates relative historical reporting windows (e.g., previous month or variable date offsets).

=======================================================================================
PSEUDOCODE MULTI-FUNCTION MODULE
=======================================================================================

```python
# h_alis_date.py
# Shared date library replacing h_alis_date.ksh

import os
import sys
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# # REVIEW: target database platform not specified; DB-client library choice below is provisional
# To maintain compatibility with existing sqlplus logic, a database helper may be required.
# If migrating fully to a modern environment (e.g., BigQuery or Python-native orchestrations),
# these database calls can be executed natively in Python without launching external SQL files.

def dw_date_vormonat(format_str: str) -> str:
    """
    Replaces DWDate_Vormonat.
    Calculates previous month date formatted according to format_str.
    Uses native Python datetime calculation instead of launching sqlplus.
    """
    # Step 1: Calculate previous month based on current time
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    previous_month_date = first_of_this_month - timedelta(days=1)
    
    # Step 2: Map Oracle format string to Python strftime format
    # # REVIEW: Format mappings should be validated against all expected incoming formats
    py_format = format_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
    return previous_month_date.strftime(py_format)


def dw_date_datum_check(wert: str, format_str: str) -> bool:
    """
    Replaces DWDate_Datum_Check.
    Returns True if 'wert' is a valid date matching 'format_str', else False.
    """
    # Step 1: Map format string
    py_format = format_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
    
    # Step 2: Try parsing date using Python native library
    try:
        datetime.strptime(wert, py_format)
        return True
    except ValueError:
        return False


def dw_date_datum_le(datum1_str: str, datum2_str: str) -> bool:
    """
    Replaces DWDate_Datum_LE.
    Asserts if datum1 <= datum2 (both in YYYYMMDD format).
    Raises ValueError if datum1 > datum2.
    """
    # Step 1: Parse input strings
    try:
        d1 = datetime.strptime(datum1_str, "%Y%m%d")
        d2 = datetime.strptime(datum2_str, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format (expected YYYYMMDD): {e}")

    # Step 2: Perform comparative logic
    if d1 > d2:
        raise ValueError(f"Datum {datum1_str} ist groesser als {datum2_str}")
    
    return True


def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    """
    Replaces DWDate_Gib_Zeitraum.
    Computes a start (now) and end date range based on offset and unit step.
    Returns (start_date_str, end_date_str).
    """
    # Step 1: Establish current system date
    start_dt = datetime.now()
    py_format = format_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')

    # Step 2: Compute end date based on Step 'stufe' (Y, M, D)
    stufe = stufe.upper()
    if stufe == 'D':
        end_dt = start_dt + timedelta(days=offset)
    elif stufe == 'M':
        # Align behavior with original logic (Month is based on Ultimo and firsts)
        # End date is offset by 'offset' months, adjusted to end of that month
        target_dt = start_dt + relativedelta(months=offset)
        # Standardize: Start of range is 1st of current month, End is last day of target month
        start_dt = start_dt.replace(day=1)
        _, last_day = calendar.monthrange(target_dt.year, target_dt.month)
        end_dt = target_dt.replace(day=last_day)
    elif stufe == 'Y':
        # Standardize: Start of range is New Year of current year, End is New Year's Eve of target year
        target_dt = start_dt + relativedelta(years=offset)
        start_dt = start_dt.replace(month=1, day=1)
        end_dt = target_dt.replace(month=12, day=31)
    else:
        raise ValueError(f"Unsupported stufe: {stufe}. Must be 'Y', 'M', or 'D'.")

    return start_dt.strftime(py_format), end_dt.strftime(py_format)


def letzter_tag_des_monats(date_str: str) -> bool:
    """
    Replaces LetzterTagDesMonats.
    Returns True if date_str (YYYYMMDD) is the last day of its month.
    """
    # Step 1: Parse date
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        return False
        
    # Step 2: Determine last day of the month using calendar module
    _, last_day = calendar.monthrange(dt.year, dt.month)
    return dt.day == last_day


def tage_im_monat(year: int, month: int) -> int:
    """
    Replaces TageimMonat.
    Returns total days in the specified year and month.
    """
    # Step 1: Use calendar.monthrange which natively accounts for leap years
    _, days = calendar.monthrange(year, month)
    return days


def addiere_datum(date_str: str, days_to_add: int) -> str:
    """
    Replaces AddiereDatum.
    Adds integer days to date string YYYYMMDD and returns resulting YYYYMMDD string.
    """
    # Step 1: Parse base date
    base_dt = datetime.strptime(date_str, "%Y%m%d")
    
    # Step 2: Add days natively (handles rollover cleanly)
    result_dt = base_dt + timedelta(days=days_to_add)
    
    # Step 3: Return formatted date
    return result_dt.strftime("%Y%m%d")
```

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py` | Migrated to a Python utility library containing equivalent date-math, validation, and parsing functions. |

---

### Job Dependencies
The following downstream consumer jobs depend on the date calculations provided by this utility library. Because they are not yet migrated, their integration cannot be fully validated. They should be configured on Google Cloud / Cloud Composer to import `h_alis_date.py` or execute its equivalent functions inside Python tasks.
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

---

### Scheduling
- This component is a shared utility library and does not execute on a standalone schedule. It is imported and executed as a runtime module inside downstream tasks. It must remain a callable, shared module within the Airflow DAG container environment.

---

### Schedule & Variables
- **Schedule**: None (runs on demand/by import inside downstream DAGs).
- **Scheduler-set / Sourced Variables**:
  - `DW_DIR_ROOT`: Legacy root directory path. Under the target Python module structure, this should be retired in favor of native Python relative imports or standard path configurations within Cloud Composer (`/home/airflow/gcs/dags/...`).
  - `DW_ORAUSER`: Connection identifier used for legacy Oracle calls. This environment variable is retired for this module as all SQL queries are refactored into native, high-performance local Python code.

---

### Lineage
- **Upstream Producers**: None.
- **Downstream Consumers (via shell source `/ .` execution)**:
  - `DW.BERT_ABLAUFSTEUERUNG` (job)
  - `DW.BERT_AUSD_BP_TA_MSISDN` (job)
  - `DW.BERT_AUSD_BP_TA_P_BASISPROD` (job)
  - `DW.BERT_AUSD_V_TA_PERIOD` (job)
  - `DW.BERT_AUSD_V_TA_P_VERTRAG` (job)
  - `DW.BERT_AUSD_V_TA_VERTRAG_TMP` (job)
  - `DW.BERT_DROP_TEMP_TABLE` (job)
  - `DW.BERT_P_ADRESSEN` (job)
  - `DW.BERT_P_AUSTAUSCH` (job)
  - `DW.BERT_P_GESCHAEFTSP` (job)
  - `DW.BERT_P_RECH_EMPF` (job)
  - `DW.BERT_RECHNUNGSDATEN` (job)
- **External Legacy Tables**:
  - `DUAL` (Oracle system table, legacy source only) - replaced completely by local Python calculations.

---

### External System Replacements
- **Oracle SQL\*Plus**: All database round-trips used for date validations (`TO_DATE` checks), date comparative checks (`datum1 > datum2`), and date-range calculations are completely replaced with local Python standard libraries (`datetime`, `calendar`, and `dateutil.relativedelta`). This avoids unnecessary queries against BigQuery.

---

### Cross-File Dependencies
- **Legacy SQL Dependencies**: The script previously executed two companion Oracle SQL scripts:
  - `d_alis_vormonat.sql` (to calculate the previous month)
  - `d_alis_datum_zeitraum.sql` (to compute date intervals)
- **Target Replacement**: These SQL scripts are retired. Their business logic is converted into pure-Python calculations inside `h_alis_date.py`.
- **Calling Scripts**: Downstream KSH scripts sourcing `h_alis_date.ksh` must be refactored during their own migration passes to use Python native import statements (`import h_alis_date`).

---

### Target File Plan
- **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.py`
- **Target Language**: Python 3
- **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
- **Purpose**: Shared library containing modular Python functions for date calculations, validations, and leaps.

---

### Environment-Specific Values
- **GLOBAL (Environment-wide)**:
  - None required. By performing all calendar calculations locally, the module requires no direct GCP project, location, or cluster parameters.
- **JOB-SPECIFIC**:
  - `DW_DIR_ROOT` (Legacy script location variable) - Classified as job-specific. This variable is retired. The Python environment must handle import resolution natively through the Python path.
  - `DW_ORAUSER` (Legacy DB connection identifier) - Classified as job-specific. Retired as database execution is no longer required for date validation and math.

---

### Risks and Manual Steps
- **Unmigrated Downstream Jobs**: Since all downstream calling scripts (such as `DW.BERT_RECHNUNGSDATEN`) are "not yet migrated", the physical integration, directory search paths, and function call interfaces cannot be fully verified in end-to-end integration tests until those callers are migrated.
- **Dynamic Assignment (`eval`) Refactoring**: The legacy KSH code used `eval "$VarName=..."` to dynamically inject computed dates into the caller script's environment. In the migrated Python environment, callers must explicitly capture the return values (e.g., `start, end = dw_date_gib_zeitraum(...)`), requiring manual verification of calling code patterns during downstream migrations.
- **Output/Print Literal Preservation**: In accordance with the Output Literal rule, the literal German warning text must be retained character-for-character within the Python exception messages and console prints:
  - `!! Interner Fehler bei der Rueckgabe von Datumswerten`
  - `   Funktion: DWDate_Gib_Zeitraum`
  - `   1 Zeile erwartet, {anzahl} Zeile(n) bekommen`
  - `Datum {datum1} ist groesser als {datum2}`

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
REASON: The script is a KornShell module/library containing complex function definitions, business logic parameters, case statements, loops, and external utility integrations that are not expressible in SQL.

EVIDENCE
- Business logic found: KSH custom logic contains parameter parsing, verification, string mappings, domain routing, and date validation logic across multiple defined functions.
- AWK: none
- SQL-expressible: no, this is purely procedural orchestration-level metadata/parameter handling.
- Non-SQL side effects: none (handles only shell environment states and variable assignments).
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_parameter.ksh`) acts as a shared library of utility functions for parsing, validating, and converting parameters within the IS (Information System) ETL context. It provides robust error handling, domain-specific mapping for KPIs (Kennzahlen) and system identifiers, compatibility checks between systems and KPIs, and date arithmetic parsing. Rather than running as a standalone job, it is sourced by other scripts in the data processing flow to enforce strict parameter interfaces.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by downstream KornShell ETL scripts (via `. h_alis_parameter.ksh`). There is no standalone UC4 JOBS_UNIX execution wrapper for this script itself.
   - UC4 includes: None.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   This script defines reusable functions that accept arguments. Below are the inputs to each public function defined:
   - `pruefeParameterGesetzt`:
     - `$1`: Parameter descriptive name (string)
     - `$2`: Target environment variable name (string, whose value is checked dynamically)
   - `konvertiereKennzahl`:
     - `$1`: Name of environment variable holding the KPI descriptor (modified in-place)
   - `konvertiereSystem`:
     - `$1`: Name of environment variable holding the source system descriptor (modified in-place)
   - `konvertiereSDName`:
     - `$1`: Name of environment variable holding the master data (Stammdaten) descriptor (modified in-place)
   - `konvertiereAufbStufeXtra`:
     - `$1`: Name of environment variable holding the processing phase name (modified in-place)
   - `pruefeSystemKennzahl`:
     - `$1`: System short-name (string)
     - `$2`: KPI (Kennzahl) short-name (string)
   - `gibBereich`:
     - `$1`: KPI short-name (string)
     - `$2`: Name of environment variable to receive the target business area (modified in-place)
   - `gibIntervall`:
     - `$1`: KPI short-name (string)
     - `$2`: Name of environment variable to receive the target interval code (modified in-place)
   - `pruefeZeitraum`:
     - `$1`: Start date in `YYYYMMDD` format
     - `$2`: End date in `YYYYMMDD` format
   - `pruefeZahlPositiv`:
     - `$1`: Numeric value
     - `$2`: Parameter name for error logs
   - `pruefeZeitParameter`:
     - `$1`: Start date (optional)
     - `$2`: End date (optional)
     - `$3`: Time offset (optional)
   - `konvertiereZeitspanne`:
     - `$1`: Name of environment variable to receive the calculated start date
     - `$2`: Name of environment variable to receive the calculated end date
     - `$3`: Numeric offset span
     - `$4`: KPI short-name

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWDate_Datum_Check`: Sourced/external date verification tool. Checks if a date complies with a specific format.
     # REVIEW-STRUCT: external function DWDate_Datum_Check not supplied in this extraction — behaviour simulated via standard datetime parsing in Python.
   - `DWDate_Datum_LE`: Sourced/external date comparison tool. Checks if Start Date <= End Date.
     # REVIEW-STRUCT: external function DWDate_Datum_LE not supplied in this extraction — behaviour simulated via native Python comparison operators.
   - `DWDate_Gib_Zeitraum`: Sourced/external date offset calculation tool. Computes start and end dates based on a relative time offset and offset unit.
     # REVIEW-STRUCT: external function DWDate_Gib_Zeitraum not supplied in this extraction — behaviour simulated using Python's datetime and dateutil.relativedelta modules.
   - `basename`, `date`, `rm`, `cat`: Standard UNIX system commands used during temp logging. Handled natively in Python via standard libraries (`os`, `sys`, `shutil`).

5. EMBEDDED SQL
   (No embedded SQL found in this utility script)

6. CONTROL FLOW
   Since this is a library, control flow executes on a per-function invocation basis. All functions share global state variables `ErrNr` (Error Number) and `ErrArg` (Error Argument) to cascade and manage failures.
   - `pruefeParameterGesetzt`: Checks if variable named in `$2` is empty. If so, sets `ErrNr=194` and `ErrArg=$1`.
   - `konvertiereKennzahl`: Converts KPI name to lowercase and evaluates against a large `case` statement, mapping names like `zugang` to `zug`, `abgang` to `abg`, etc. Invalid names set `ErrNr=198` and `ErrArg=$Kennzahl`.
   - `konvertiereSystem`: Standardizes system string (e.g. `sap`, `carmen`, `dpps`, `d1`, `xtra`, `ctel`, `nnv`, `dwh`, `brunet`, `sigma`). Unsupported values set `ErrNr=195`.
   - `konvertiereSDName`: Standardizes master data name (e.g., `rahmenvertrag` -> `rv`, `tarif` -> `trf`, etc.). Unsupported values set `ErrNr=195`.
   - `konvertiereAufbStufeXtra`: Standardizes step name (`zusammenfuehrung` -> `mrg`, `befuellung` -> `fill`). Unsupported values set `ErrNr=195`.
   - `pruefeSystemKennzahl`: Multi-branch validation checking combinations. For example, system `sap` is restricted from KPIs `zug`, `abg`, `abz`, `bst`, `twe`, `pln`, `gut`, `auf`, `rst`, `tvd`, `usk`, `ust`, `lkl`, `loe`, `rak`, `ksd`, `bwa`. Invalid mappings trigger `ErrNr=195`.
   - `gibBereich`: Maps a given KPI (e.g., `abg`, `gut`, `tvd`) to its business domain (e.g., `tn` (Teilnehmer), `us` (Umsatz), `gd` (Gesprächsdaten), `sd` (Stammdaten), `md` (Metadaten)) by traversing pre-defined string lists.
   - `gibIntervall`: Maps a KPI to its processing interval (e.g., `t` for daily (täglich), `m` for monthly (monatlich)).
   - `pruefeZeitraum`: Checks if start and end dates are in `YYYYMMDD` format and that start <= end. It handles execution in a subshell with a temporary file to capture errors and safely unsets `set -e` temporarily (`set +e`).
   - `pruefeZahlPositiv`: Verifies that a parameter value is a valid integer and >= 0.
   - `pruefeZeitParameter`: Verifies mutual exclusivity/completeness: either offset is defined (and positive) or both start and end dates are defined.
   - `konvertiereZeitspanne`: Calculates absolute start and end dates from a relative offset based on the KPI domain (uses months `M` for KPI `bst`, else days `D`).

7. ERROR HANDLING & EXIT CODES
   - The script simulates a globally-scoped, cascading error-flag pattern:
     - `ErrNr`: Integer, initialized outside the script (0 represents success).
     - `ErrArg`: String, description of the error payload or argument.
   - Almost all functions begin with:
     ```ksh
     if [ $ErrNr -ne 0 ]; then return; fi
     ```
     This prevents subsequent parameter-processing functions from overwriting an already established error code.
   - Standard Error Codes:
     - `194`: Parameter not set / empty.
     - `195`: Invalid combination or value configuration.
     - `196`: Missing function arguments or unmappable metrics.
     - `198`: Unknown KPI.
     - `85`: Date calculation utility failure.
   - Python mapping: The best practice is to design a class or state container `AlisParameterContext` that manages this state, or standard functions raising custom exceptions (e.g., `AlisParameterException`) containing `err_nr` and `err_arg` attributes.

8. OUTPUTS / SIDE EFFECTS
   - Mutates environment variables dynamically using `eval` (e.g., modifying the caller's variable in-place).
   - Temporary file generation in `/tmp` named `/tmp/tmp_<basename>_<timestamp>_<pid>.tmp` which is deleted immediately after date verification tasks finish.

9. BUSINESS SUMMARY
   - Standardizes reporting KPIs, ensuring consistent three-letter shorthand codes (e.g., `zug` for logins/accesses, `bst` for inventory/stock) are propagated downstream.
   - Enforces business boundaries by ensuring metrics are linked to their valid upstream source systems (e.g., preventing system `sap` from producing subscriber status modifications like `zug`).
   - Formats and groups indicators into functional business domains (`tn` for subscriber, `us` for sales, `gd` for call data).
   - Regulates date interval alignments, separating high-frequency daily evaluations from monthly consolidations.
   - Guarantees data sanity for downstream processing by validating input boundaries (checking ranges, non-negative bounds, and date chronology).

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
import os
import sys
import datetime
import tempfile
import re

# Module-level tracking variables to replicate global KSH variables
MODUL_NAME = "alis_parameter"
MODUL_VERSION = "V3.0.9"

class AlisParameterContext:
    def __init__(self):
        self.err_nr = 0
        self.err_arg = ""

    def reset(self):
        self.err_nr = 0
        self.err_arg = ""

    def has_error(self):
        return self.err_nr != 0

ctx = AlisParameterContext()


# Helper function to mimic eval "param_wert=\$$param_var"
def get_env_var(var_name: str) -> str:
    return os.environ.get(var_name, "")


# Helper function to mimic eval "$VarName=$Value"
def set_env_var(var_name: str, value: str):
    os.environ[var_name] = str(value)


# Step 1: pruefeParameterGesetzt
def pruefe_parameter_gesetzt(param_name: str, param_var: str):
    if ctx.has_error():
        return

    if not param_name or not param_var:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeParameterGesetzt"
        return

    param_wert = get_env_var(param_var)

    if not param_wert:
        ctx.err_nr = 194
        ctx.err_arg = param_name


# Step 2: konvertiereKennzahl
def konvertiere_kennzahl(var_name: str):
    if ctx.has_error():
        return

    if not var_name:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereKennzahl"
        return

    kennzahl = get_env_var(var_name).lower()

    # Case statement translation
    if kennzahl == "zugang":
        kennzahl = "zug"
    elif kennzahl == "abgang":
        kennzahl = "abg"
    elif kennzahl == "abgang_zukunft":
        kennzahl = "abz"
    elif kennzahl == "bestand":
        kennzahl = "bst"
    elif kennzahl == "tarifwechsel":
        kennzahl = "twe"
    elif kennzahl == "plan":
        kennzahl = "pln"
    elif kennzahl == "gutschrift":
        kennzahl = "gut"
    elif kennzahl == "aufladung":
        kennzahl = "auf"
    elif kennzahl == "restguthaben":
        kennzahl = "rst"
    elif kennzahl == "teilnehmerverbindungsdaten":
        kennzahl = "tvd"
    elif kennzahl == "uskonto":
        kennzahl = "usk"
    elif kennzahl == "usteilnehmer":
        kennzahl = "ust"
    elif kennzahl == "leistungsklasse":
        kennzahl = "lkl"
    elif kennzahl == "loeschung":
        kennzahl = "loe"
    elif kennzahl == "reaktivierung":
        kennzahl = "rak"
    elif kennzahl == "standard_rechnung":
        kennzahl = "srs"
    elif kennzahl == "standard_gutschrift":
        kennzahl = "sgs"
    elif kennzahl == "gutschrift_rv":
        kennzahl = "sg_rv"
    elif kennzahl == "rechnungen_rv_dpps":
        kennzahl = "sr_rv_dpps"
    elif kennzahl == "bewegart":
        kennzahl = "bwa"
    elif kennzahl == "kundenstamm":
        kennzahl = "ksd"
    elif kennzahl == "mahnstufe":
        kennzahl = "mahn"
    elif kennzahl == "metadatenstruktur":
        kennzahl = "mds"
    elif kennzahl == "d1news":
        kennzahl = "d1n"
    elif kennzahl == "rubrik":
        kennzahl = "rub"
    elif kennzahl == "liefermodus":
        kennzahl = "lmo"
    elif kennzahl == "netznutzungsklassen":
        kennzahl = "nnk"
    elif kennzahl == "tagesverkehrskurven":
        kennzahl = "tvk"
    elif kennzahl == "gespraechsziele":
        kennzahl = "gz"
    elif kennzahl == "gespraechslaengenverteilung":
        kennzahl = "glv"
    elif kennzahl == "zonenkennung":
        kennzahl = "zonek"
    elif kennzahl == "zonentyp":
        kennzahl = "zonet"
    elif kennzahl == "netznutzungsklassentyp":
        kennzahl = "nnkt"
    elif kennzahl == "tarifart":
        kennzahl = "trfa"
    elif kennzahl == "gespraechstyp":
        kennzahl = "gtyp"
    elif kennzahl == "basisdienst":
        kennzahl = "basisd"
    elif kennzahl == "nationalinternational":
        kennzahl = "natint"
    elif kennzahl == "glaengenintervall":
        kennzahl = "glint"
    else:
        ctx.err_nr = 198
        ctx.err_arg = kennzahl
        kennzahl = "???"

    set_env_var(var_name, kennzahl)


# Step 3: konvertiereSystem
def konvertiere_system(var_name: str):
    if ctx.has_error():
        return

    if not var_name:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereSystem"
        return

    system = get_env_var(var_name).lower()

    allowed_systems = {
        "sap", "carmen", "dpps", "d1", "xtra", 
        "ctel", "nnv", "dwh", "brunet", "sigma"
    }

    if system in allowed_systems:
        pass
    else:
        ctx.err_nr = 195
        ctx.err_arg = f"Unbekannte Datenherkunft {system} !"
        system = "???"

    set_env_var(var_name, system)


# Step 4: konvertiereSDName
def konvertiere_sd_name(var_name: str):
    if ctx.has_error():
        return

    if not var_name:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereSDSystem"
        return

    system = get_env_var(var_name).lower()

    if system == "vo":
        pass
    elif system == "rahmenvertrag":
        system = "rv"
    elif system == "tarif":
        system = "trf"
    elif system == "tstatus":
        system = "ts"
    elif system == "zahlmodus":
        system = "zm"
    elif system == "kdg_grund":
        system = "kdg"
    elif system == "gutschrift":
        system = "gut"
    elif system == "aufladung":
        system = "auf"
    elif system == "leistung":
        system = "l_leist"
    elif system == "gutschrift_grund":
        system = "l_gutgr"
    elif system == "sap_gutschrift_grund":
        system = "sap_l_gutgr"
    elif system == "produkt":
        system = "l_prod"
    elif system == "mahnverfahren_sapist":
        system = "l_mahnv_ist"
    elif system == "mahnverfahren_sapfi":
        system = "l_mahnv_fi"
    elif system == "mahnstufentyp_sapist":
        system = "l_mahnstyp_ist"
    elif system == "bewegart":
        system = "bwa"
    else:
        ctx.err_nr = 195
        ctx.err_arg = f"Unbekannte Stammdaten-Datenherkunft {system} !"
        system = "???"

    set_env_var(var_name, system)


# Step 5: konvertiereAufbStufeXtra
def konvertiere_aufbstufe_xtra(var_name: str):
    if ctx.has_error():
        return

    if not var_name:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} konvertiereAufbStufeXtra"
        return

    stufe = get_env_var(var_name).lower()

    if stufe == "zusammenfuehrung":
        stufe = "mrg"
    elif stufe == "befuellung":
        stufe = "fill"
    else:
        ctx.err_nr = 195
        ctx.err_arg = f"Unbekannte Stufenangabe {stufe} !"
        stufe = "???"

    set_env_var(var_name, stufe)


# Step 6: pruefeSystemKennzahl
def pruefe_system_kennzahl(system: str, kennzahl: str):
    if ctx.has_error():
        return

    if not system or not kennzahl:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeSystemKennzahl"
        return

    err_arg_temp = ""

    if system != "nnv" and (kennzahl == "tvd" or kennzahl == "lkl"):
        err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    elif system == "carmen":
        invalid_kpis = {
            "twe", "pln", "rst", "srs", "sgs", 
            "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"
        }
        if kennzahl in invalid_kpis:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    elif system == "sap":
        invalid_kpis = {
            "zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", 
            "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"
        }
        if kennzahl in invalid_kpis:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    elif system == "dpps":
        invalid_kpis = {
            "twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"
        }
        if kennzahl in invalid_kpis:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    elif system == "ctel":
        allowed_kpis = {"abg", "bst", "zug", "twe"}
        if kennzahl not in allowed_kpis:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    elif system == "xtra":
        if kennzahl != "rst":
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    elif system == "d1":
        invalid_kpis = {
            "gut", "auf", "loe", "rak", "sgs", "srs", "twe", 
            "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"
        }
        if kennzahl in invalid_kpis:
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
        allowed_kpis = {
            "nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa",
            "gtyp", "basisd", "natint", "glint"
        }
        if kennzahl not in allowed_kpis:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    if err_arg_temp:
        ctx.err_nr = 195
        ctx.err_arg = err_arg_temp


# Step 7: gibBereich
def gib_bereich(kennzahl: str, var_bereich: str):
    if ctx.has_error():
        return

    if not kennzahl or not var_bereich:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibBereich"
        return

    # Categorised metrics mapping
    list_tn = {"abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"}
    list_us = {"gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}
    list_gd = {
        "tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", 
        "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"
    }
    list_sd = {"ksd", "bwa"}
    list_md = {"mds"}

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
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibBereich - Kuerzel '{kennzahl}' unbekannt"
        return

    set_env_var(var_bereich, my_bereich)


# Step 8: gibIntervall
def gib_intervall(kennzahl: str, var_intervall: str):
    if ctx.has_error():
        return

    if not kennzahl or not var_intervall:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibIntervall"
        return

    list_t = {
        "abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", 
        "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"
    }
    list_m = {
        "bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", 
        "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"
    }

    my_intervall = ""
    if kennzahl in list_t:
        my_intervall = "t"
    elif kennzahl in list_m:
        my_intervall = "m"

    if not my_intervall:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
        return

    set_env_var(var_intervall, my_intervall)


# Step 9: pruefeZeitraum
# REVIEW-STRUCT: external functions DWDate_Datum_Check and DWDate_Datum_LE are simulated via native standard datetime parsing in Python.
def pruefe_zeitraum(anfang: str, ende: str):
    if ctx.has_error():
        return

    if not anfang or not ende:
        ctx.err_nr = 196
        ctx.err_arg = f"{MODUL_NAME} {MODUL_VERSION} pruefeZeitraum"
        return

    err_arg_temp = ""
    format_mask = "%Y%m%d"

    # Simulate DWDate_Datum_Check
    anfang_dt = None
    ende_dt = None

    try:
        anfang_dt = datetime.datetime.strptime(anfang, format_mask)
    except ValueError:
        err_arg_temp = f"Anfangdatum entspricht nicht dem Format YYYYMMDD"

    try:
        ende_dt = datetime.datetime.strptime(ende, format_mask)
    except ValueError:
        if err_arg_temp:
            err_arg_temp += " / Endedatum entspricht nicht dem Format YYYYMMDD"
        else:
            err_arg_temp = "Endedatum entspricht nicht dem Format YYYYMMDD"

    if not err_arg_temp:
        # Simulate DWDate_Datum_LE (Anfang <= Ende)
        if anfang_dt > ende_dt:
            err_arg_temp = "Anfangsdatum ist nicht kleiner gleich Endedatum"

    if err_arg_temp:
        ctx.err_nr = 195
        ctx.err_arg = err_arg_temp


# Step 10: pruefeZahlPositiv
def pruefe_zahl_positiv(zahl_str: str, parameter_name: str):
    try:
        val = int(zahl_str)
        if val < 0:
            ctx.err_nr = 195
            ctx.err_arg = f"Parameter {parameter_name} muss groesser gleich 0 sein"
    except ValueError:
        ctx.err_nr = 195
        ctx.err_arg = f"Parameter {parameter_name} ist kein numerischer Wert"


# Step 11: pruefeZeitParameter
def pruefe_zeit_parameter(anfangsdatum: str, endedatum: str, zeit_offset: str):
    if ctx.has_error():
        return

    if zeit_offset:
        if not anfangsdatum and not endedatum:
            pruefe_zahl_positiv(zeit_offset, "Zeitspanne")
            return
        else:
            ctx.err_nr = 195
            ctx.err_arg = "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden"
            return
    else:
        if anfangsdatum and endedatum:
            pruefe_zeitraum(anfangsdatum, endedatum)
        else:
            ctx.err_nr = 195
            if not anfangsdatum and not endedatum:
                ctx.err_arg = "Datumswerte oder Zeitspanne fehlen"
            else:
                ctx.err_arg = "Sowohl Anfang- als auch Endedatum muessen angegeben werden"
            return


# Step 12: konvertiereZeitspanne
# REVIEW-STRUCT: external function DWDate_Gib_Zeitraum is simulated via timedelta / rel_delta logic based on unit (Days vs Months).
def konvertiere_zeitspanne(var_anfang: str, var_ende: str, spanne_str: str, kennzahl: str):
    if ctx.has_error():
        return

    # In KSH this unit is computed dynamically
    offset_unit = "D"  # Default Days
    if kennzahl == "bst":
        offset_unit = "M"  # Monthly

    try:
        spanne = int(spanne_str)
    except ValueError:
        ctx.err_nr = 85
        ctx.err_arg = "konvertiereZeitspanne - Spanne ist kein Integer"
        return

    today = datetime.date.today()
    # A negative span calculates into the past. KSH uses -(-spanne) internally or subtracts.
    # Logic in shell: DWDate_Gib_Zeitraum -$p_Spanne $Offset_Unit ...
    # Hence, offset is computed with negative value: e.g. -spanne.
    try:
        if offset_unit == "D":
            calculated_date = today - datetime.timedelta(days=spanne)
            anfangsdatum = calculated_date.strftime("%Y%m%d")
            endedatum = today.strftime("%Y%m%d")
        elif offset_unit == "M":
            # Rough simulation of relative months subtraction
            # Note: For strict monthly intervals, dateutil.relativedelta should be deployed.
            # Using basic approximation here to remain dependency-free in pseudocode.
            year = today.year - (spanne // 12)
            month = today.month - (spanne % 12)
            if month <= 0:
                month += 12
                year -= 1
            calculated_date = datetime.date(year, month, today.day)
            anfangsdatum = calculated_date.strftime("%Y%m%d")
            endedatum = today.strftime("%Y%m%d")

        set_env_var(var_anfang, anfangsdatum)
        set_env_var(var_ende, endedatum)

    except Exception as e:
        ctx.err_nr = 85
        ctx.err_arg = "DWDate_Gib_Zeitraum"
        print(f"Error calculating dates: {e}", file=sys.stderr)
```

### 1. File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py` | Migrated to a Python utility module containing equivalent functions to be imported and used by downstream Python DAG tasks. |

---

### 2. Job Dependencies

This utility library does not run as a standalone orchestrated job. Instead, it is a shared dependency that must be made available for import by downstream processes.

* **Downstream Consumers (Not Yet Migrated):**
  The following downstream consumer jobs depend on the parameter parsing, verification, and validation logic defined in this utility:
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

* **Wiring on Target:**
  Since the downstream consumers are not yet migrated, this Python library (`h_alis_parameter.py`) must be placed in a shared dependencies directory (such as a Google Cloud Storage bucket path `/dags/dependencies/` or packaged and installed in the Cloud Composer environment) so that when those downstream jobs are migrated to Cloud Composer, they can import this utility module natively (`import h_alis_parameter`).

---

### 3. Scheduling

* **Schedule Mapping:** This job has no standalone schedule because it is a shared utility library. It is executed purely via inclusion or import within other scheduled tasks. It must remain a callable, importable module in the Cloud Composer environment and should not have its own standalone DAG.

---

### 4. Schedule & Variables — Must Be Retained

* **Variables Handling:**
  The legacy script operates by reading and mutating environment variables dynamically in-place (via `eval` and `typeset` manipulations). 
  * In the migrated Python module, instead of mutating global shell environment variables directly, functions should return their parsed and validated outputs or accept a context dictionary to modify.
  * For Airflow-driven workflows calling downstream jobs, parameters should be passed dynamically using Airflow `params` or task outputs/XComs rather than relying on mutating process-level OS environment variables.

---

### 5. Cross-File Dependencies

* This library relies on external date processing logic referred to in the source as:
  * `DWDate_Datum_Check`
  * `DWDate_Datum_LE`
  * `DWDate_Gib_Zeitraum`
* These are defined in sibling date utilities within the legacy shared codebase. In the target environment, these helper functions must either be resolved by importing a corresponding migrated date helper module (e.g. `is_date_util.py`) or simulated using standard Python libraries (`datetime` and `dateutil`).

---

### 6. Target File Plan

* **Target File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.py`
  * **Language:** Python
  * **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
  * **Purpose:** Provides a pythonic module-level helper structure replicating the parameter validation, code mapping, and interval grouping logic.

---

### 7. Environment-Specific Values

No environment-wide target infrastructure constants (such as GCP projects, buckets, or datasets) are defined or referenced within this utility script. All variables processed are job-specific parameters (such as `Anfangsdatum`, `Endedatum`, `Kennzahl`, or `System`) which are passed dynamically during runtime execution.

---

### 8. Risks & Manual Actions

* **Downstream Migration Dependencies:**
  * Downstream consumer `DW.BERT_ABLAUFSTEUERUNG` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_AUSD_BP_TA_MSISDN` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_AUSD_BP_TA_P_BASISPROD` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_AUSD_V_TA_PERIOD` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_AUSD_V_TA_P_VERTRAG` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_AUSD_V_TA_VERTRAG_TMP` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_DROP_TEMP_TABLE` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_P_ADRESSEN` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_P_AUSTAUSCH` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_P_GESCHAEFTSP` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_P_RECH_EMPF` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
  * Downstream consumer `DW.BERT_RECHNUNGSDATEN` is not yet migrated. Wiring and end-to-end testing cannot be finalized until it exists.
* **External Date Helper Functions:** The script references `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` which are external to this file. A manual step is required to ensure these helpers are correctly resolved either via a unified Python date-utility import or via local implementation using Python’s standard `datetime` module.

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
REASON: The script defines a reusable helper function that performs input validation, file-readability checks, and executes SQL*Plus with dynamic arguments, which must be converted into a Python utility module.

EVIDENCE
- Business logic found: KSH custom logic defines a reusable utility function `starteSQLSkript` that validates parameters, checks SQL script readability, and invokes SQL*Plus.
- AWK: none
- SQL-expressible: no (the script acts as a dynamic orchestration wrapper for arbitrary, parameterized SQL*Plus scripts and contains file-system checks)
- Non-SQL side effects: Invokes external process (sqlplus), checks filesystem readability, and calls external error logging function (`DWMSG_MeldeFehler`).
- Against this verdict: none (it defines functions, does argument checks, and error handling, making it a clear candidate for Python utility conversion)

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a KornShell helper/library module (`h_alis_sqlplus.ksh`) that provides a reusable function, `starteSQLSkript`. The function is designed to safely execute Oracle SQL*Plus scripts by first validating that the required parameters are supplied and that the target SQL script is readable on the filesystem. It wraps the execution in error-handling logic and logs the invocation details before executing the script with any trailing arguments.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced (using `. h_alis_sqlplus.ksh`) by other legacy shell scripts to make the `starteSQLSkript` function available in their execution scope.
   - UC4 Native Includes: None referenced in this extraction.
   - Environment files sourced: None explicitly sourced inside this script, but it depends on external functions and environment variables being pre-initialized (such as `DW_ORAUSER` and `DWMSG_MeldeFehler`).

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts positional parameters:
   - `p_Eintragsnr` ($1): Positional argument. Represents the error entry identifier used during error reporting. Map to a Python function parameter.
   - `p_Skript` ($2): Positional argument. The filesystem path of the SQL*Plus script to be executed. Map to a Python function parameter.
   - `*` (remaining arguments, captured via `shift 2` and `$*`): Dynamic parameters passed through to the SQL*Plus script. Map to Python `*args` or a list.
   - `DW_ORAUSER` (env var): The database connection string/credentials used to authenticate SQL*Plus. Map to `os.environ.get("DW_ORAUSER")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Launches Oracle SQL*Plus, connects using the credentials in `DW_ORAUSER`, runs the SQL script specified by `p_Skript`, and passes any remaining arguments (`$*`). Input is redirected from `/dev/null` to prevent interactive hangs.
     - Target: Must remain an external process invocation via `subprocess.run` because the target script path (`p_Skript`) is dynamic and determined at runtime.
     - Resolvable Launcher: No. The script path is dynamic, so the SQL content is not available for static extraction or direct DB-client conversion here.

5. EMBEDDED SQL
   None. The script executes dynamic external SQL files passed as parameters.

6. CONTROL FLOW
   1. Initialize global variables `ModulName="alis_sqlplus"` and `ModulVersion="V1.1.3"`.
   2. Define the `starteSQLSkript` function (to be translated as a Python function).
   3. Validate that both `p_Eintragsnr` and `p_Skript` are not empty. If either is missing, call the error logger `DWMSG_MeldeFehler` with code `196` and return `196`.
   4. Validate that `p_Skript` is a readable file. If not, call `DWMSG_MeldeFehler` with code `201` and return `201`.
   5. Log the script path and its dynamic arguments to stdout.
   6. Temporarily disable strict shell exit-on-error (`set +e`).
   7. Execute `sqlplus` with the specified credentials, script, and arguments, redirecting stdin from `/dev/null`.
   8. Capture the exit code of `sqlplus`.
   9. Restore strict exit-on-error (`set -e`).
   10. Return the captured exit code to the caller.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments trigger a call to `DWMSG_MeldeFehler $p_Eintragsnr E 196 ...` and return exit code `196`.
   - An unreadable SQL script triggers a call to `DWMSG_MeldeFehler $p_Eintragsnr E 201 ...` and returns exit code `201`.
   - `set +e` is used to prevent the script from exiting immediately if SQL*Plus fails. The exit code (`$?`) is captured in `errcode` and propagated as the function return value.
   - Map to Python: Raise custom exceptions or return the exit code integer. Use `try...except` and `subprocess.run(..., check=False)` to capture and return the return code.

8. OUTPUTS / SIDE EFFECTS
   - Standard output messages detailing script settings.
   - Potential database updates, logs, or files created by the executed SQL*Plus script.
   - Invocation of `DWMSG_MeldeFehler` (external logging system).

9. BUSINESS SUMMARY
   - Provides a standard, hardened wrapper for launching SQL*Plus scripts within the data warehouse environment.
   - Ensures all SQL script executions verify parameter presence and file readability prior to connecting to the database.
   - Standardizes error logging via a centralized messaging function (`DWMSG_MeldeFehler`) when preconditions fail.
   - Correctly propagates SQL*Plus return codes back to the parent process to maintain execution chain integrity.

=== PSEUDOCODE STYLE ===

```python
import os
import sys
import subprocess
import pathlib

# Step 1: Initialize module metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# REVIEW-STRUCT: external utility function DWMSG_MeldeFehler not supplied; behavior must be stubbed or imported from the corresponding migrated error-handling module.
def dwmsg_melde_fehler(eintragsnr, severity, code, message):
    """
    Stub for the external error-reporting utility DWMSG_MeldeFehler.
    """
    print(f"ERROR LOG: [{eintragsnr}] {severity} {code}: {message}", file=sys.stderr)


# Step 2: Define starte_sql_skript function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Starts an SQL*Plus script with parameters and error handling.
    """
    # Step 3: Validate that required parameters are not empty
    if not p_eintragsnr or not p_skript:
        error_context = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", "196", error_context)
        return 196

    # Step 4: Validate that the script file is readable
    script_path = pathlib.Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", "201", str(p_skript))
        return 201

    # Step 5: Log execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Retrieve DB credentials from environment
    # REVIEW: target database platform assumed to be Oracle based on sqlplus usage; confirm connection string format in DW_ORAUSER env var.
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 7: Execute SQL*Plus command (equivalent to set +e / execute / capture exit code / set -e)
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
    
    try:
        # Redirect stdin from devnull equivalent to </dev/null
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            check=False  # Do not raise exception automatically to mimic 'set +e' and return errcode
        )
        errcode = result.returncode
    except Exception as e:
        # Handle case where sqlplus executable itself is missing or cannot be started
        print(f"Execution failed: {str(e)}", file=sys.stderr)
        errcode = -1

    # Step 8: Return exit code
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py` | Converts the legacy KornShell SQL\*Plus runner library function (`starteSQLSkript`) into a Python utility module while retaining folder structure. |

### Job Dependencies
- **Downstream Consumers (not yet migrated):**
  The following 12 downstream jobs depend on this utility module. Because this library module is sourced or called by them, their migration must be coordinated, and the exact module integration cannot be finalized until they are migrated:
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

### Scheduling
- **Schedule Policy:**
  This utility script has no standalone execution schedule. It acts as an include/shared module sourced by other scripts. In the target Cloud Composer environment, it must remain an importable/callable Python utility module (e.g., placed within a shared `plugins/` or `dags/utils/` directory) rather than having its own DAG or execution schedule.

### Schedule & Variables
- **Schedule & Variables Retention:**
  No direct scheduling triggers are defined for this script. Any variables or context inherited from the calling environments must be passed dynamically as Python function parameters (`p_eintragsnr`, `p_skript`, and extra dynamic arguments) to match the legacy invocation contract.

### Lineage
- **Lineage Edges:**
  No direct automated lineage edges were discovered for this utility script. Its usage is transient and determined by which scripts dynamically source it at runtime.

### External System Replacements
- **Oracle SQL\*Plus to BigQuery:**
  - **Legacy:** The script launches `sqlplus` with the database credentials stored in the environment variable `DW_ORAUSER` to execute local or dynamic SQL scripts (`@$p_Skript`).
  - **BigQuery Migration:** In the target BigQuery platform, Oracle SQL\*Plus is replaced. Dynamic script execution should be performed using the BigQuery Python Client (`google.cloud.bigquery`) or BigQuery Operators in Cloud Composer. The actual SQL scripts being executed must also be migrated from Oracle PL/SQL dialect to BigQuery Standard SQL, and their invocations rewritten to run against BigQuery.

### Cross-File Dependencies
- **Message Utility Dependency:**
  The script depends on the external function `DWMSG_MeldeFehler` (likely defined in a sibling messaging shell utility) to log errors under specific codes (such as `196` and `201`).
- **Downstream Script Sourcing:**
  Any migrated Python script that replaces a legacy KornShell script sourcing `h_alis_sqlplus.ksh` must import the migrated `starte_sql_skript` function from this Python module.

### Target File Plan
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.py`
  - **Language:** Python
  - **Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
  - **Description:** A Python module containing the translated `starte_sql_skript` function, using standard Python filesystem checks (`pathlib`) and process execution utilities (`subprocess`), with stubs/imports for the shared error logging library.

### Environment-Specific Values
- **GLOBAL:**
  - `DW_ORAUSER` (represents the legacy Oracle connection credentials).
    - *GCP Target Mechanism:* In Python, retrieve via `os.environ.get("DW_ORAUSER")` or map to a shared GCP Connection/Secret. For a complete BigQuery migration, this should transition to a BigQuery client configuration utilizing standard service account IAM permissions (`GCP_PROJECT`, etc.).
- **JOB-SPECIFIC:**
  - `ModulName` ("alis_sqlplus") and `ModulVersion` ("V1.1.3")
    - *GCP Target Mechanism:* Stored as module-level Python constants (`MODUL_NAME = "alis_sqlplus"`, `MODUL_VERSION = "V1.1.3"`).

### Risks & Manual Steps
- **Unresolved Dependencies:**
  - SOURCE: NOT FOUND — DWMSG_MeldeFehler — no candidate
- **Database Client Transition / B4 Redesign:**
  The `starte_sql_skript` function relies on executing `sqlplus` as an external subprocess. When migrating fully to BigQuery, launching a subprocess to run SQL\*Plus scripts is deprecated. A B4 redesign is required: downstream jobs should be refactored to execute migrated BigQuery SQL queries using the BigQuery Python client API rather than relying on standard shell-style SQL execution subprocesses.
- **Downstream Integrations:**
  Since the 12 downstream consumer jobs are not yet migrated, their direct wiring and imports of this module cannot be verified or finalized.