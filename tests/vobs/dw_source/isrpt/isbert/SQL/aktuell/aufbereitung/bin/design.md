=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh ===
#!/usr/bin/ksh
#*******************************************************************************
#*** Information Services Reporting      T-Mobil (Deutsche Telekom Mobilnet) ***
#***                                                                         ***
#*** Datei gestern.ksh                                                       ***
#***                                                                         ***
#*** Autor : Martin Hillmann                                                 ***
#*** Datum : 2003.01.21                                                      ***
#***                                                                         ***
#***-------------------------------------------------------------------------***
#***                                                                         ***
#*** Beschreibung:                                                           ***
#***                                                                         ***
#***  Ermittelt das heutige und das Datum von gestern.                       ***
#***                                                                         ***
#***-------------------------------------------------------------------------***
#***                                                                         ***
#*** Aufruf :                                                                ***
#***                                                                         ***
#***  gestern.ksh                                                            ***
#***                                                                         ***
#***-------------------------------------------------------------------------***
#***                                                                         ***
#*** Returnvalues:                                                           ***
#***  Datum heute   im Format YYMMDD                                         ***
#***  Datum gestern im Format YYMMDD                                         ***
#***                                                                         ***
#***-------------------------------------------------------------------------***
#***                                                                         ***
#*** Aenderungen:                                                            ***
#*** User         Datum     Bemerkung                                        ***
#*** -----------  --------  ------------------------------------------------ ***
#***                                                                         ***
#***                                                                         ***
#***                                                                         ***
#***                                                                         ***
#*******************************************************************************

################################################
# Variablendeklaration

Var_Nummer_Null=0

Var_Nummer_Heute_Tag=0
Var_Nummer_Heute_Monat=0
Var_Nummer_Heute_Jahr=0

Var_Datum_Heute=0
Var_Monat_Heute=0

Var_Nummer_Gestern_Tag=0
Var_Nummer_Gestern_Monat=0
Var_Nummer_Gestern_Jahr=0

Var_Datum_Gestern=0
Var_Datum_Gestern=0

################################################
# Datum ermitteln
set `date '+ %d %m %Y'`

Var_Nummer_Heute_Tag=$1
Var_Nummer_Heute_Monat=$2
Var_Nummer_Heute_Jahr=$3


################################################

## Vortag innerhalb des Monats
if (( $Var_Nummer_Heute_Tag > 1 ))
then
  Var_Nummer_Gestern_Tag=`expr $Var_Nummer_Heute_Tag - 1`
  Var_Nummer_Gestern_Monat=$Var_Nummer_Heute_Monat
  Var_Nummer_Gestern_Jahr=$Var_Nummer_Heute_Jahr
else
## Vortag im Vormonat
  if (( $Var_Nummer_Heute_Tag == 1 ))
  then
   ## Vormonat im selben Jahr
    if (( $Var_Nummer_Heute_Monat > 1 ))
    then
       Var_Nummer_Gestern_Monat=`expr $Var_Nummer_Heute_Monat - 1`
       Var_Nummer_Gestern_Jahr=$Var_Nummer_Heute_Jahr

      # Letzter Tag im Monat
       case "$Var_Nummer_Gestern_Monat" in
         1) Var_Nummer_Gestern_Tag=31;;
         2) Var_Nummer_Gestern_Tag=28;;
         3) Var_Nummer_Gestern_Tag=31;;
         5) Var_Nummer_Gestern_Tag=31;;
         7) Var_Nummer_Gestern_Tag=31;;
         8) Var_Nummer_Gestern_Tag=31;;
        10) Var_Nummer_Gestern_Tag=31;;
        12) Var_Nummer_Gestern_Tag=31;;
         *) Var_Nummer_Gestern_Tag=30;;
       esac

      # Schaltjahr
       if (( `expr $Var_Nummer_Heute_Jahr % 4` == 0  && \
             `expr $Var_Nummer_Heute_Jahr % 100` > 0 && \
                  $Var_Nummer_Gestern_Monat == 2))
       then 
          Var_Nummer_Gestern_Tag=29
       fi

    else
   ## Vormonat im Vorjahr
      Var_Nummer_Gestern_Monat=12
      Var_Nummer_Gestern_Jahr=`expr $Var_Nummer_Heute_Jahr - 1`
      Var_Nummer_Gestern_Tag=31   

    fi
  else
    echo "Fehler !!!!"
  fi
fi

################################################
# Datum vormatieren

# Datum Heute
Var_Nummer_LaengeWert=${#Var_Nummer_Heute_Tag}

if (( $Var_Nummer_LaengeWert == 1 ))
then
 Var_Nummer_Heute_Tag=$Var_Nummer_Null$Var_Nummer_Heute_Tag
fi

Var_Nummer_LaengeWert=${#Var_Nummer_Heute_Monat}

if (( $Var_Nummer_LaengeWert == 1 ))
then
 Var_Nummer_Heute_Monat=$Var_Nummer_Null$Var_Nummer_Heute_Monat
fi

Var_Datum_Heute=$Var_Nummer_Heute_Jahr$Var_Nummer_Heute_Monat$Var_Nummer_Heute_Tag

Var_Monat_Heute=$Var_Nummer_Heute_Jahr$Var_Nummer_Heute_Monat

Var_Nummer_LaengeWert=${#Var_Nummer_Gestern_Tag}

if (( $Var_Nummer_LaengeWert == 1 ))
then
 Var_Nummer_Gestern_Tag=$Var_Nummer_Null$Var_Nummer_Gestern_Tag
fi

Var_Nummer_LaengeWert=${#Var_Nummer_Gestern_Monat}

if (( $Var_Nummer_LaengeWert == 1 ))
then
 Var_Nummer_Gestern_Monat=$Var_Nummer_Null$Var_Nummer_Gestern_Monat
fi

Var_Datum_Gestern=$Var_Nummer_Gestern_Jahr$Var_Nummer_Gestern_Monat$Var_Nummer_Gestern_Tag

Var_Monat_Gestern=$Var_Nummer_Gestern_Jahr$Var_Nummer_Gestern_Monat

################################################
# Datum ausgeben
#******************************************************************************
echo $Var_Datum_Heute $Var_Datum_Gestern $Var_Monat_Heute $Var_Monat_Gestern


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains custom date arithmetic, conditional leap-year verification, and manual string formatting to output date variables, which is best represented as a native Python utility.

EVIDENCE
- Business logic found: KSH custom logic. It manually parses the system clock's date and computes "yesterday's" date, handling month-boundary rollbacks, leap years (February 29th), and year-end transitions.
- AWK: none
- SQL-expressible: No. This is a command-line utility that computes date values and outputs them to standard output for parent shell script capture. It does not perform transformations over database tables or tabular data.
- Non-SQL side effects: Writes space-separated date strings directly to stdout.
- Against this verdict: One could implement the date computation inside a BigQuery SQL script using `DATE_SUB`, but since this is a standalone utility script designed to run in a shell context, converting it to a Python CLI script is the direct and correct operational mapping.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `gestern.ksh` script is a utility used to determine and format "today's" and "yesterday's" dates. It reads the current date from the system operating system clock, manually computes yesterday's date (handling varying month lengths, leap-years, and year boundaries), formats both dates into `YYYYMMDD` and `YYYYMM` formats, and prints them as a space-separated string to standard output. This output is designed to be captured by calling processes for orchestration, file naming, or partitioning.

2. INVOCATION CONTEXT
   - Who calls this script: Typically invoked by UC4/Automic jobs or parent wrapper scripts to establish runtime date variables. No specific UC4 job name was provided in the extraction.
   - UC4 native includes: None referenced.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   - Positional/Environment Inputs: None. The script does not accept command-line parameters or inspect external environment variables for its execution.
   - Declared Variables (from script body):
     - `Var_Nummer_Null` (value: `0`): Used for prefixing single-digit days/months.
     - `Var_Nummer_Heute_Tag` (value: `0`): Today's day component.
     - `Var_Nummer_Heute_Monat` (value: `0`): Today's month component.
     - `Var_Nummer_Heute_Jahr` (value: `0`): Today's year component.
     - `Var_Datum_Heute` (value: `0`): Formatted today's date (`YYYYMMDD`).
     - `Var_Monat_Heute` (value: `0`): Formatted today's month (`YYYYMM`).
     - `Var_Nummer_Gestern_Tag` (value: `0`): Yesterday's day component.
     - `Var_Nummer_Gestern_Monat` (value: `0`): Yesterday's month component.
     - `Var_Nummer_Gestern_Jahr` (value: `0`): Yesterday's year component.
     - `Var_Datum_Gestern` (value: `0`): Formatted yesterday's date (`YYYYMMDD`). (Note: Redundantly declared twice in KSH source).
     - `Var_Monat_Gestern` (implicit): Formatted yesterday's month (`YYYYMM`).
   - # REVIEW: Header states returned format is `YYMMDD`, but the actual KSH implementation uses a 4-digit year `%Y` resulting in `YYYYMMDD` format. The Python conversion will preserve the implemented `YYYYMMDD` format.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `date '+ %d %m %Y'`: Executed via command-substitution to retrieve current day, month, and year.
   - `expr`: Invoked multiple times to perform manual subtraction and modulo math.
   - Resolution: These will be completely replaced by native Python `datetime` module calls (`datetime.date.today()` and `datetime.timedelta`), which natively and robustly handle calendar math (including leap years and month/year boundaries) without spawning sub-processes.

5. EMBEDDED SQL
   - None.

6. CONTROL FLOW
   1. **Initialization**: Initialize variable placeholders.
   2. **Date Extraction**: Call system `date` to fetch the current day, month, and year.
   3. **Yesterday Calculation**:
      - If today's day is > 1: Subtract 1 from the day; month and year remain unchanged.
      - If today's day is == 1: Roll back to previous month.
        - If today's month > 1: Subtract 1 from the month; year remains unchanged.
          - Determine the last day of the previous month:
            - Jan, Mar, May, Jul, Aug, Oct, Dec -> 31 days.
            - Apr, Jun, Sep, Nov -> 30 days.
            - Feb -> 28 days (or 29 days if today's year is divisible by 4, not divisible by 100).
        - If today's month == 1 (January): Set month to 12, subtract 1 from the year, and set day to 31.
      - If today's day is < 1: Print error message `"Fehler !!!!"`.
   4. **Formatting**: Ensure day and month components are zero-padded to 2 digits. Concatenate to construct:
      - `Var_Datum_Heute` (`YYYYMMDD`)
      - `Var_Datum_Gestern` (`YYYYMMDD`)
      - `Var_Monat_Heute` (`YYYYMM`)
      - `Var_Monat_Gestern` (`YYYYMM`)
   5. **Output**: Print the four space-separated string values to standard output.

7. ERROR HANDLING & EXIT CODES
   - KSH does not implement `set -e` or trap logic.
   - If `Var_Nummer_Heute_Tag` is evaluated as less than 1, it prints `"Fehler !!!!"` to stdout but continues running, which would result in malformed or empty string concatenations.
   - In Python, using the native `datetime` module guarantees calendar safety and eliminates invalid date possibilities. If system clock access fails (e.g. system exception), Python will raise a standard traceback exception and exit with non-zero code `1`.

8. OUTPUTS / SIDE EFFECTS
   - Writes a single string to stdout: `[Today_YYYYMMDD] [Yesterday_YYYYMMDD] [TodayMonth_YYYYMM] [YesterdayMonth_YYYYMM]`

9. BUSINESS SUMMARY
   - Establishes the current operational and reporting "Today" and "Yesterday" contexts.
   - Handles critical business date conversions safely over month ends, leap years (February 29th transition), and new year transitions.
   - Provides standardized inputs for subsequent partition filtering or directory paths in reporting pipelines.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
# Step 1: Import required native modules
import sys
from datetime import date, timedelta

def main():
    try:
        # Step 2: Retrieve system current date
        today = date.today()
        
        # Step 3: Compute yesterday's date natively (handles all calendar boundaries automatically)
        yesterday = today - timedelta(days=1)
        
        # Step 4: Format date outputs
        # Var_Datum_Heute (YYYYMMDD)
        var_datum_heute = today.strftime("%Y%m%d")
        
        # Var_Datum_Gestern (YYYYMMDD)
        var_datum_gestern = yesterday.strftime("%Y%m%d")
        
        # Var_Monat_Heute (YYYYMM)
        var_monat_heute = today.strftime("%Y%m")
        
        # Var_Monat_Gestern (YYYYMM)
        var_monat_gestern = yesterday.strftime("%Y%m")
        
        # Step 5: Output variables to stdout matching the exact legacy format
        print(f"{var_datum_heute} {var_datum_gestern} {var_monat_heute} {var_monat_gestern}")
        
    except Exception as e:
        # Step 6: Error handling
        print(f"Error executing date calculation: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.py` | Migrates legacy date-calculation logic to a native Python CLI utility script that computes dates and outputs them to standard output. |

---

### Job dependencies

The following downstream jobs consume the output of this utility script:
*   `DW.BERT_ABLAUFSTEUERUNG` (not yet migrated)
*   `DW.BERT_AUSD_BP_TA_MSISDN` (not yet migrated)
*   `DW.BERT_AUSD_BP_TA_P_BASISPROD` (not yet migrated)
*   `DW.BERT_DROP_TEMP_TABLE` (not yet migrated)
*   `DW.BERT_P_ADRESSEN` (not yet migrated)
*   `DW.BERT_P_AUSTAUSCH` (not yet migrated)
*   `DW.BERT_P_GESCHAEFTSP` (not yet migrated)
*   `DW.BERT_P_RECH_EMPF` (not yet migrated)
*   `DW.BERT_RECHNUNGSDATEN` (not yet migrated)

Since these downstream consumers are not yet migrated, the orchestration wiring cannot be finalized until they are created on the target platform. In the target environment, parent Airflow DAGs will execute this Python script as a task or import it as a utility module to retrieve date parameters dynamically.

---

### Scheduling

*   This job is not directly triggered by any of the run's schedulers. It executes inside other scheduled jobs as an include or shared module. It must remain a callable/importable unit without its own standalone schedule in Cloud Composer.

---

### Schedule & variables

*   No scheduler-set variables or direct scheduler assignments are associated with this shared utility script. It is invoked dynamically inside parent workflows.

---

### Lineage

*   No lineage edges (upstream producers or downstream consumers) were identified for this file in the repository's direct lineage analysis.

---

### Target file plan

*   **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.py`
    *   **Language**: Python
    *   **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`

---

### Risks and manual steps

*   **Unmigrated Downstream Dependencies**: Downstream jobs (`DW.BERT_ABLAUFSTEUERUNG`, `DW.BERT_AUSD_BP_TA_MSISDN`, `DW.BERT_AUSD_BP_TA_P_BASISPROD`, `DW.BERT_DROP_TEMP_TABLE`, `DW.BERT_P_ADRESSEN`, `DW.BERT_P_AUSTAUSCH`, `DW.BERT_P_GESCHAEFTSP`, `DW.BERT_P_RECH_EMPF`, and `DW.BERT_RECHNUNGSDATEN`) are not yet migrated. Integration and parameter verification cannot be finalized until these consumer jobs are designed and built.
*   **Literal Output Enforcement (OUTPUT/PRINT LITERAL RULE)**: The legacy script outputs `"Fehler !!!!"` to stdout under invalid date-parsing conditions. In accordance with the Output/Print Literal Rule, any error-handling logic in the Python code must output this exact German text string (`"Fehler !!!!"`) to preserve legacy behavior.