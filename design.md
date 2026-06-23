# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

## 1. Purpose & Scope
This KornShell script, `gestern.ksh`, is designed to calculate and format today's date and yesterday's date. It handles month and year transitions, including leap years, and then prints the results to standard output. The job's purpose, as noted in the lineage, is to perform this date calculation.

## 2. Source Inventory
The job consists of a single source file:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`
  - **Technology:** KornShell (shell script)
  - **Summary:** Calculates and formats today's and yesterday's dates (YYMMDD format), handling month/year transitions and leap years.
  - **Complexity Tier:** Not available (no entry in `file_complexity` table).
  - **Automation Bucket:** Not available (no entry in `automation_rate` table). Based on the script's simplicity and the direct mapping to BigQuery functions, it is likely a candidate for B1 (auto) or B2 (semi_auto) migration.

## 3. Target Architecture
The target architecture will leverage BigQuery's SQL capabilities, specifically BigQuery scripting and UDFs (if more complex logic were involved, though not strictly necessary here). The output, which is currently printed to standard output, will be translated into a BigQuery `SELECT` statement that returns the calculated dates. Given the simple nature of the script, it could be implemented as a BigQuery stored procedure or a simple SQL query.

## 4. Data Flow & Lineage
The original script's data flow is entirely internal, deriving its primary input from the system's current date.
- **Inputs:** System current date (obtained via `date` command in KornShell).
- **Processing:**
  - Extracts day, month, year from the current date.
  - Performs conditional logic to calculate yesterday's date, accounting for month and year boundaries, including leap years.
  - Formats the calculated dates and months with leading zeros.
- **Outputs:**
  - `Var_Datum_Heute` (Today's date in `YYYYMMDD` format)
  - `Var_Datum_Gestern` (Yesterday's date in `YYYYMMDD` format)
  - `Var_Monat_Heute` (Today's month in `YYYYMM` format)
  - `Var_Monat_Gestern` (Yesterday's month in `YYYYMM` format)
  - These values are currently output to `stdout`.

In BigQuery, this will translate to:
- A BigQuery script or stored procedure that uses `CURRENT_DATE()`, `DATE_SUB()`, `EXTRACT()`, `FORMAT_DATE()` and procedural constructs (`DECLARE`, `SET`, `IF`).
- The final output will be a `SELECT` statement returning the four calculated date/month values.

No external dependencies were identified for this specific script through lineage analysis, and its content suggests it's a self-contained date utility.

## 5. Transformation Logic
The core transformation logic involves date calculation and formatting.

**Original KornShell Logic:**
1.  **Get Current Date:** `set \`date '+ %d %m %Y'\`` captures day, month, and year.
2.  **Calculate Yesterday's Date:**
    *   If `Var_Nummer_Heute_Tag > 1`, then `yesterday_day = today_day - 1`, `yesterday_month = today_month`, `yesterday_year = today_year`.
    *   If `Var_Nummer_Heute_Tag == 1`:
        *   If `Var_Nummer_Heute_Monat > 1`:
            *   `yesterday_month = today_month - 1`, `yesterday_year = today_year`.
            *   `yesterday_day` determined by a `case` statement based on `yesterday_month` (e.g., 31 for Jan, Mar, May, etc., 28 for Feb).
            *   Leap year logic (modulo 4, not 100 or 400) is applied if `yesterday_month` is February to set `yesterday_day` to 29.
        *   Else (Jan 1st): `yesterday_month = 12`, `yesterday_year = today_year - 1`, `yesterday_day = 31`.
3.  **Format Dates:** Pads single-digit days and months with a leading zero and concatenates year, month, and day/month into `YYYYMMDD` and `YYYYMM` strings respectively.

**BigQuery Equivalent Logic (Pseudocode from CM MCP tool):**
```sql
-- BigQuery Script / Stored Procedure Pseudocode

DECLARE Var_Nummer_Null INT64 DEFAULT 0;
-- ... other DECLARE statements for variables ...

-- Datum ermitteln
SET Var_Nummer_Heute_Tag = EXTRACT(DAY FROM CURRENT_DATE());
SET Var_Nummer_Heute_Monat = EXTRACT(MONTH FROM CURRENT_DATE());
SET Var_Nummer_Heute_Jahr = EXTRACT(YEAR FROM CURRENT_DATE());

-- Vortag berechnen (using BigQuery procedural IF/ELSEIF/CASE)
IF Var_Nummer_Heute_Tag > 1 THEN
  SET Var_Nummer_Gestern_Tag = Var_Nummer_Heute_Tag - 1;
  SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat;
  SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;
ELSEIF Var_Nummer_Heute_Tag = 1 THEN
  IF Var_Nummer_Heute_Monat > 1 THEN
    SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat - 1;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;

    -- Letzter Tag im Monat (using BigQuery CASE)
    CASE Var_Nummer_Gestern_Monat
      WHEN 1 THEN SET Var_Nummer_Gestern_Tag = 31;
      -- ... other months ...
      ELSE SET Var_Nummer_Gestern_Tag = 30;
    END CASE;

    -- Schaltjahr (using BigQuery IF and MOD)
    IF MOD(Var_Nummer_Heute_Jahr, 4) = 0
       AND MOD(Var_Nummer_Heute_Jahr, 100) > 0
       AND Var_Nummer_Gestern_Monat = 2 THEN
      SET Var_Nummer_Gestern_Tag = 29;
    END IF;

  ELSE
    -- Vormonat im Vorjahr
    SET Var_Nummer_Gestern_Monat = 12;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr - 1;
    SET Var_Nummer_Gestern_Tag = 31;
  END IF;
ELSE
  SELECT 'Fehler !!!!' AS error_message; -- Original error handling
END IF;

-- Datum formatieren (using CONCAT, CAST, LPAD)
SET Var_Datum_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Heute_Tag AS STRING), 2, '0')
);
-- ... similar for Var_Monat_Heute, Var_Datum_Gestern, Var_Monat_Gestern ...

-- Ausgabe
SELECT
  Var_Datum_Heute AS Var_Datum_Heute,
  Var_Datum_Gestern AS Var_Datum_Gestern,
  Var_Monat_Heute AS Var_Monat_Heute,
  Var_Monat_Gestern AS Var_Monat_Gestern;
```

## 6. External Dependencies
The original script has no external dependencies beyond the system's date command. There are no file I/O operations or calls to other systems/APIs. The logic is self-contained.

- **`date` command:** This is replaced by BigQuery's `CURRENT_DATE()`, `EXTRACT()`, and `DATE_SUB()` functions.
- **`expr` command:** This is replaced by native BigQuery SQL arithmetic operators and `MOD()` function.
- No other external systems (Oracle, SFTP, S3, etc.) are used by this specific script.

## 7. Unresolved / Risks
- **Missing Complexity/Automation Rate:** No data was found in `file_complexity` or `automation_rate` for this script. While the script is simple, this gap in metadata means the migration effort and automation potential are estimated based solely on code analysis.
- **Leap Year Logic Simplification:** The script implements manual leap year logic. BigQuery's built-in date functions (e.g., `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`) handle leap years correctly and would simplify the code significantly. The current pseudocode retains the original logic for faithful translation, but an optimized version could use `DATE_SUB`. This could be considered a minor redesign opportunity.
- **Error Handling:** The original script has minimal error handling, simply printing "Fehler !!!!" if the date calculation logic falls into an unexpected state. In BigQuery, this can be retained as a `SELECT 'Fehler !!!!'` or enhanced with more robust error handling (e.g., `RAISE` in a stored procedure).
- **Timezone Considerations:** The original `date` command uses the system's local timezone. In BigQuery, `CURRENT_DATE()` returns the current date in the default timezone of the query execution environment, which is UTC by default unless explicitly configured. If the exact runtime timezone behavior of the source script is critical, explicit timezone handling (e.g., `CURRENT_DATE('America/Los_Angeles')`) would be required in BigQuery.

## 8. Build Plan
The migration involves creating a BigQuery SQL script or stored procedure that replicates the date calculation and formatting logic.

**Build Steps:**
1.  **Develop BigQuery SQL Script:**
    *   **Language:** BigQuery SQL (Scripting or Stored Procedure)
    *   **Content:** Translate the KornShell script logic into BigQuery SQL, utilizing `DECLARE`, `SET`, `IF`, `CASE`, `CURRENT_DATE()`, `EXTRACT()`, `LPAD()`, `CONCAT()`, `MOD()`, and arithmetic operators.
    *   **File Name:** `gestern_bq.sql` (or similar)
    *   **Location:** Target BigQuery dataset (e.g., `project.dataset.gestern_procedure` if implemented as a stored procedure).
2.  **Testing:**
    *   Verify the output of the BigQuery script matches the original KornShell script for various test dates (start of month, start of year, end of month, end of year, leap year, non-leap year).
3.  **Deployment:**
    *   Deploy the BigQuery SQL script as a stored procedure or as an executable query.
    *   If scheduled execution is required, integrate it with a BigQuery scheduler, Cloud Composer (Airflow DAG), or Cloud Scheduler.

**Example BigQuery SQL (from the tool, can be used as a starting point):**
```sql
-- BigQuery Script / Stored Procedure
-- Name: `gestern_calculator` or similar

DECLARE Var_Nummer_Null INT64 DEFAULT 0;
DECLARE Var_Nummer_Heute_Tag INT64;
DECLARE Var_Nummer_Heute_Monat INT64;
DECLARE Var_Nummer_Heute_Jahr INT64;
DECLARE Var_Datum_Heute STRING;
DECLARE Var_Monat_Heute STRING;
DECLARE Var_Nummer_Gestern_Tag INT64;
DECLARE Var_Nummer_Gestern_Monat INT64;
DECLARE Var_Nummer_Gestern_Jahr INT64;
DECLARE Var_Datum_Gestern STRING;
DECLARE Var_Monat_Gestern STRING;

-- Datum ermitteln
SET Var_Nummer_Heute_Tag = EXTRACT(DAY FROM CURRENT_DATE());
SET Var_Nummer_Heute_Monat = EXTRACT(MONTH FROM CURRENT_DATE());
SET Var_Nummer_Heute_Jahr = EXTRACT(YEAR FROM CURRENT_DATE());

-- Vortag berechnen
IF Var_Nummer_Heute_Tag > 1 THEN
  SET Var_Nummer_Gestern_Tag = Var_Nummer_Heute_Tag - 1;
  SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat;
  SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;
ELSEIF Var_Nummer_Heute_Tag = 1 THEN
  IF Var_Nummer_Heute_Monat > 1 THEN
    SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat - 1;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;

    CASE Var_Nummer_Gestern_Monat
      WHEN 1 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 2 THEN SET Var_Nummer_Gestern_Tag = 28;
      WHEN 3 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 5 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 7 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 8 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 10 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 12 THEN SET Var_Nummer_Gestern_Tag = 31;
      ELSE SET Var_Nummer_Gestern_Tag = 30;
    END CASE;

    IF MOD(Var_Nummer_Heute_Jahr, 4) = 0
       AND MOD(Var_Nummer_Heute_Jahr, 100) > 0
       AND Var_Nummer_Gestern_Monat = 2 THEN
      SET Var_Nummer_Gestern_Tag = 29;
    END IF;

  ELSE
    SET Var_Nummer_Gestern_Monat = 12;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr - 1;
    SET Var_Nummer_Gestern_Tag = 31;
  END IF;
ELSE
  SELECT 'Fehler !!!!' AS error_message;
END IF;

-- Datum formatieren
SET Var_Datum_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Heute_Tag AS STRING), 2, '0')
);

SET Var_Monat_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0')
);

SET Var_Datum_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Gestern_Tag AS STRING), 2, '0')
);

SET Var_Monat_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0')
);

-- Ausgabe
SELECT
  Var_Datum_Heute AS TodayDate,
  Var_Datum_Gestern AS YesterdayDate,
  Var_Monat_Heute AS TodayMonth,
  Var_Monat_Gestern AS YesterdayMonth;
```