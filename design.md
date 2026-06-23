# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

## 1. Purpose & Scope

The legacy job, `gestern.ksh`, is a KornShell script designed to calculate and format today's date and yesterday's date. Its primary function is to output four date-related values to standard output: today's date in `YYYYMMDD` format, yesterday's date in `YYYYMMDD` format, today's month in `YYYYMM` format, and yesterday's month in `YYYYMM` format. The script handles month and year transitions and includes custom logic for determining leap years.

The business purpose of this script is to provide current and prior day/month date strings, likely for use in downstream processes that require date-stamped file names, partition values, or reporting periods.

The scope of this migration design is to replace the functionality of this KornShell script with an equivalent solution within the Google Cloud BigQuery ecosystem.

## 2. Source Inventory

The job consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`
    *   **Technology:** KornShell
    *   **Summary:** Calculates and formats today's date and yesterday's date, including handling month and year transitions and leap years, then prints them to standard output.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** retire

## 3. Target Architecture

The target platform for this migration is Google BigQuery. The functionality of the `gestern.ksh` script will be re-implemented directly in BigQuery SQL. Given the script's simple utility nature, it is not anticipated to require a separate BigQuery table or a complex data pipeline. Instead, the date calculation logic will be available as a BigQuery SQL script (or potentially a stored procedure) that can be called by other BigQuery jobs, or its logic can be embedded directly into consuming queries or views.

The BigQuery implementation will leverage native BigQuery SQL date and time functions for robustness, accuracy, and performance, rather than replicating the manual calculation logic from the shell script.

## 4. Data Flow & Lineage

**Current Data Flow (Legacy):**
The `gestern.ksh` script has no external inputs (e.g., files or database tables). It determines the current date and time from the operating system's `date` command at runtime. After performing calculations for yesterday's date, it prints four formatted date strings to standard output. There are no explicit dependency edges (INVOKES, READS, WRITES, DEPENDS_ON) recorded in the lineage for this script, suggesting it operates as a standalone utility.

**Target Data Flow (BigQuery):**
In BigQuery, the date calculations will also be self-contained. The equivalent BigQuery SQL script will retrieve the current date using `CURRENT_DATE()`. It will then use BigQuery's native date arithmetic functions to determine yesterday's date and format all required date strings. The output will be a `SELECT` statement returning the four date values, which can then be consumed by any subsequent BigQuery query, view, or external process. This approach avoids any manual dependencies on external system commands or error-prone custom date logic.

## 5. Transformation Logic

The core transformation logic involves calculating today's date, yesterday's date, and the corresponding year-month values, and formatting them as `YYYYMMDD` and `YYYYMM` strings.

**Legacy Logic (KornShell - `gestern.ksh`):**
1.  **Get Current Date:** Uses `date '+ %d %m %Y'` to get the current day, month, and year.
2.  **Calculate Yesterday's Date:**
    *   If today's day is greater than 1, yesterday is simply (today's day - 1) in the same month and year.
    *   If today's day is 1:
        *   If today's month is greater than 1, yesterday is in the previous month of the same year. The script then uses a `case` statement to determine the last day of the previous month. It includes custom `if` conditions to adjust for February in (partially) handled leap years (divisible by 4 and not by 100, but missing the divisible by 400 rule).
        *   If today's month is 1 (January 1st), yesterday is December 31st of the previous year.
    *   An error message "Fehler !!!!" is printed if `Var_Nummer_Heute_Tag` is not 1 and not > 1, though this path is unlikely to be hit under normal circumstances.
3.  **Format Dates:** Pads single-digit day and month values with a leading zero and concatenates year, month, and day into `YYYYMMDD` strings, and year and month into `YYYYMM` strings.
4.  **Output:** Prints the four calculated date strings to standard output.

**Target Logic (BigQuery SQL - Recommended Native Approach):**
The manual and error-prone date calculations (especially the custom leap year logic) in the legacy script will be replaced by BigQuery's robust native date functions.

1.  **Get Current Date:** `CURRENT_DATE()` will provide today's date.
2.  **Calculate Yesterday's Date:** `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` will accurately calculate yesterday's date, correctly handling month and year transitions and leap years without custom logic.
3.  **Extract Components:** `EXTRACT(DAY FROM ...)` and `EXTRACT(MONTH FROM ...)` will be used if individual day/month components are needed (e.g., for debugging or intermediate steps).
4.  **Format Dates:** `FORMAT_DATE('%Y%m%d', date_value)` and `FORMAT_DATE('%Y%m', date_value)` will be used to format the dates into the desired `YYYYMMDD` and `YYYYMM` strings. `LPAD` can be used for explicit zero-padding if needed for intermediate string manipulations.

**Example BigQuery SQL (Recommended):**

```sql
DECLARE Var_Datum_Heute STRING;
DECLARE Var_Monat_Heute STRING;
DECLARE Var_Datum_Gestern STRING;
DECLARE Var_Monat_Gestern STRING;

DECLARE current_date_value DATE DEFAULT CURRENT_DATE();
DECLARE yesterday_date_value DATE DEFAULT DATE_SUB(current_date_value, INTERVAL 1 DAY);

SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', current_date_value);
SET Var_Monat_Heute = FORMAT_DATE('%Y%m', current_date_value);

SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday_date_value);
SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday_date_value);

SELECT
  Var_Datum_Heute AS Var_Datum_Heute,
  Var_Datum_Gestern AS Var_Datum_Gestern,
  Var_Monat_Heute AS Var_Monat_Heute,
  Var_Monat_Gestern AS Var_Monat_Gestern;
```

**Target Logic (BigQuery SQL - Shell-Equivalent Branch Structure - If exact parity is strictly required):**
This approach, while functional, is less recommended due to the verbosity and potential for introducing errors by manually replicating logic that native functions handle better.

```sql
DECLARE Var_Nummer_Null STRING DEFAULT '0';

DECLARE Var_Nummer_Heute_Tag INT64;
DECLARE Var_Nummer_Heute_Monat INT64;
DECLARE Var_Nummer_Heute_Jahr INT64;

DECLARE Var_Nummer_Gestern_Tag INT64;
DECLARE Var_Nummer_Gestern_Monat INT64;
DECLARE Var_Nummer_Gestern_Jahr INT64;

DECLARE Var_Datum_Heute STRING;
DECLARE Var_Monat_Heute STRING;
DECLARE Var_Datum_Gestern STRING;
DECLARE Var_Monat_Gestern STRING;

DECLARE current_date_value DATE DEFAULT CURRENT_DATE();

SET Var_Nummer_Heute_Tag = EXTRACT(DAY FROM current_date_value);
SET Var_Nummer_Heute_Monat = EXTRACT(MONTH FROM current_date_value);
SET Var_Nummer_Heute_Jahr = EXTRACT(YEAR FROM current_date_value);

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

    -- Incomplete leap year logic from source
    IF MOD(Var_Nummer_Heute_Jahr, 4) = 0
       AND MOD(Var_Nummer_Heute_Jahr, 100) > 0
       AND Var_Nummer_Gestern_Monat = 2 THEN
      SET Var_Nummer_Gestern_Tag = 29;
    END IF;

  ELSE -- Vormonat im Vorjahr (Yesterday in previous year)
    SET Var_Nummer_Gestern_Monat = 12;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr - 1;
    SET Var_Nummer_Gestern_Tag = 31;
  END IF;
ELSE
  SELECT 'Fehler !!!!' AS error_message;
END IF;

-- Pad today day/month and concatenate
SET Var_Datum_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Heute_Tag AS STRING), 2, '0')
);
SET Var_Monat_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0')
);

-- Pad yesterday day/month and concatenate
SET Var_Datum_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Gestern_Tag AS STRING), 2, '0')
);
SET Var_Monat_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0')
);

SELECT
  Var_Datum_Heute,
  Var_Datum_Gestern,
  Var_Monat_Heute,
  Var_Monat_Gestern;
```

## 6. External Dependencies

The `gestern.ksh` script has no external system dependencies (e.g., Oracle, SFTP, S3). Its only "external" interaction is with the operating system's `date` command to get the current timestamp and `expr` for arithmetic operations.

In the BigQuery target environment, these dependencies will be replaced by:
*   **System Date:** `CURRENT_DATE()` function in BigQuery SQL.
*   **Arithmetic Operations:** Native SQL arithmetic operators and date functions.

## 7. Unresolved / Risks

*   **Migration Bucket: Retire (B0):** The `automation_rate` analysis indicates that this job is a candidate for retirement. This is a strong signal that the script's functionality is likely redundant or can be easily absorbed by existing or new cloud-native capabilities. Given its simple purpose, using direct BigQuery SQL functions is a more efficient and maintainable solution than a direct "lift and shift" or complex re-implementation.
*   **Incomplete Leap Year Logic:** The original `gestern.ksh` script's leap year calculation is incomplete by Gregorian standards (it misses the rule for years divisible by 400). Migrating to BigQuery's native `DATE_SUB` function will automatically correct this and provide accurate date arithmetic.
*   **No File I/O / External APIs:** The absence of file I/O or external API calls simplifies the migration significantly, reducing potential integration risks.
*   **Unresolved Targets:** There are no unresolved targets identified for this job.

The primary recommendation is to retire the original script and integrate its required functionality directly into BigQuery SQL processes that consume these date values, using the native BigQuery date functions.

## 8. Build Plan

The build plan focuses on implementing the recommended native BigQuery SQL approach:

1.  **Create BigQuery SQL Script (`gestern_bq.sql`):**
    *   Develop a BigQuery SQL script that uses `CURRENT_DATE()`, `DATE_SUB(..., INTERVAL 1 DAY)`, and `FORMAT_DATE()` to calculate and output the required date strings.
    *   **Language:** BQSQL
    *   **Content:** The "Recommended Native Approach" BigQuery SQL example provided in Section 5.

2.  **Integration:**
    *   Identify consuming BigQuery jobs, views, or external tools that require these date values.
    *   Integrate the logic from `gestern_bq.sql` directly into these consumers, or establish a mechanism for them to execute `gestern_bq.sql` (e.g., as part of an orchestrated pipeline using Cloud Composer/Airflow or a scheduled query).

This approach ensures that the functionality is retained, modernized, and made more robust within the BigQuery environment, while retiring the legacy KornShell script.