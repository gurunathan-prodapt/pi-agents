# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

## 1. Purpose & Scope
This document outlines the migration design for the `gestern.ksh` KornShell script to Google BigQuery. The original script's primary purpose is to calculate and format today's date and yesterday's date, including handling month and year transitions and leap years, then printing these formatted dates to standard output. This script appears to be a utility for date calculation, likely invoked by other processes that require yesterday's date. The job was assembled from a single component and its stage distribution is medium.

## 2. Source Inventory
The migration involves a single source file:

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`
*   **Technology:** KornShell Script
*   **Purpose:** Utility for date calculation and formatting.
*   **Complexity Tier:** Medium
*   **Migration Flags:** None
*   **Automation Bucket:** Retire (B0)
*   **Summary:** This KornShell script calculates and formats today's date and yesterday's date, including handling month and year transitions and leap years, then prints them to standard output.

## 3. Target Architecture
The functionality of the `gestern.ksh` script will be migrated to Google BigQuery. Given its utility nature and the "Retire" migration bucket, the direct output of this script (formatted dates) can be generated using native BigQuery SQL functions.

The target architecture will consist of:
*   **BigQuery SQL Script/Routine:** A BigQuery SQL script or a stored procedure that encapsulates the date calculation and formatting logic. This script will leverage BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB()`, `FORMAT_DATE()`, `EXTRACT()`) to achieve the same output as the original KornShell script.
*   **Output:** The result (today's date, yesterday's date, current month, previous month, all in YYYYMMDD/YYYYMM format) will be available as the output of the BigQuery SQL query/routine. This can be directly consumed by downstream processes that previously relied on the shell script's standard output.

## 4. Data Flow & Lineage
The original `gestern.ksh` script acts as a standalone utility that generates date information. No direct input sources or output targets (tables/files) were identified in the `lineage_edges` for this specific `run_id`. The script primarily reads the system date (via the `date` command) and outputs formatted dates to standard output.

In the BigQuery target architecture, the data flow will be as follows:
1.  **System Date:** BigQuery's `CURRENT_DATE()` function will serve as the equivalent of the `date` command in the KornShell script, providing the current date.
2.  **Transformation:** BigQuery's date arithmetic and formatting functions will process the current date to derive yesterday's date and format both dates and months into the required `YYYYMMDD` and `YYYYMM` formats.
3.  **Output:** The final formatted dates will be presented as the result of a `SELECT` statement within a BigQuery script or stored procedure.

## 5. Transformation Logic
The core logic of `gestern.ksh` is to:
1.  Obtain the current day, month, and year.
2.  Calculate yesterday's day, month, and year, carefully handling month transitions (e.g., end of month, end of year) and leap years.
3.  Format the calculated dates and months to a `YYYYMMDD` or `YYYYMM` string, ensuring leading zeros for single-digit days/months.
4.  Print the four resulting strings.

The BigQuery transformation logic will directly map to this by using BigQuery's rich set of date and string manipulation functions:

**Original KornShell (key parts):**
*   `set \`date '+ %d %m %Y'\``: Get current date components.
*   `expr $Var_Nummer_Heute_Tag - 1`: Subtract one day.
*   `if (( $Var_Nummer_Heute_Tag > 1 ))`: Conditional logic for day subtraction.
*   `case "$Var_Nummer_Gestern_Monat"`: Logic to determine last day of previous month.
*   `if (( \`expr $Var_Nummer_Heute_Jahr % 4\` == 0 && ... ))`: Leap year detection.
*   `Var_Nummer_Heute_Tag=$Var_Nummer_Null$Var_Nummer_Heute_Tag`: Add leading zero for formatting.
*   `Var_Datum_Heute=$Var_Nummer_Heute_Jahr$Var_Nummer_Heute_Monat$Var_Nummer_Heute_Tag`: Concatenate date parts.

**Target BigQuery SQL (from MCP output):**
```sql
-- BigQuery Script: gestern equivalent

DECLARE Var_Nummer_Null STRING DEFAULT '0'; -- Not strictly needed in BQ, but for conceptual mapping

DECLARE today_date DATE DEFAULT CURRENT_DATE();
DECLARE yesterday_date DATE DEFAULT DATE_SUB(today_date, INTERVAL 1 DAY);

-- Extract components (for internal logic, though FORMAT_DATE handles it directly for output)
-- SET Var_Nummer_Heute_Tag = EXTRACT(DAY FROM today_date);
-- SET Var_Nummer_Heute_Monat = EXTRACT(MONTH FROM today_date);
-- SET Var_Nummer_Heute_Jahr = EXTRACT(YEAR FROM today_date);

-- SET Var_Nummer_Gestern_Tag = EXTRACT(DAY FROM yesterday_date);
-- SET Var_Nummer_Gestern_Monat = EXTRACT(MONTH FROM yesterday_date);
-- SET Var_Nummer_Gestern_Jahr = EXTRACT(YEAR FROM yesterday_date);

-- Format today
DECLARE Var_Datum_Heute STRING DEFAULT FORMAT_DATE('%Y%m%d', today_date);
DECLARE Var_Monat_Heute STRING DEFAULT FORMAT_DATE('%Y%m', today_date);

-- Format yesterday
DECLARE Var_Datum_Gestern STRING DEFAULT FORMAT_DATE('%Y%m%d', yesterday_date);
DECLARE Var_Monat_Gestern STRING DEFAULT FORMAT_DATE('%Y%m', yesterday_date);

-- Output equivalent to shell echo
SELECT
  Var_Datum_Heute AS Var_Datum_Heute,
  Var_Datum_Gestern AS Var_Datum_Gestern,
  Var_Monat_Heute AS Var_Monat_Heute,
  Var_Monat_Gestern AS Var_Monat_Gestern;
```
BigQuery's `DATE_SUB` and `FORMAT_DATE` functions inherently handle month/year transitions and leap years correctly, simplifying the logic significantly compared to the original shell script's manual calculations. The pseudocode provided by the MCP tool correctly identifies this.

## 6. External Dependencies
The original `gestern.ksh` script has minimal external dependencies, relying primarily on:
*   **OS `date` command:** To get the current system date.
*   **Korn Shell (`ksh`):** For script execution and built-in features.
*   **`expr` command:** For arithmetic operations.

In the BigQuery environment, these dependencies will be replaced:
*   **`CURRENT_DATE()`:** BigQuery's native function will replace the OS `date` command for obtaining the current date.
*   **BigQuery SQL Engine:** The entire logic will execute within the BigQuery SQL engine, eliminating the need for `ksh` and `expr`.

There are no external databases, APIs, or files referenced by this script.

## 7. Unresolved / Risks
*   **Unused Output:** The "Retire" migration bucket suggests that this script's output might no longer be strictly needed, or its functionality can be superseded by native features of scheduling tools (like Airflow's date macros). If its output is consumed by other legacy systems that are also being migrated, ensuring compatibility with the BigQuery SQL output format is crucial.
*   **Timezone:** The original `date` command implicitly uses the system's timezone. In BigQuery, `CURRENT_DATE()` operates on UTC by default unless a specific timezone is provided. If timezone sensitivity is critical, the BigQuery script should use `CURRENT_DATE('America/Los_Angeles')` or the appropriate timezone.
*   **Error Handling:** The original script has minimal error handling ("Fehler !!!!"). The BigQuery equivalent, relying on robust built-in functions, has less need for explicit date calculation error handling, but any surrounding logic consuming its output would need appropriate error handling in BigQuery.

## 8. Build Plan
The build plan for migrating `gestern.ksh` involves creating a BigQuery SQL script or stored procedure that replicates its date calculation and formatting functionality.

1.  **Create BigQuery SQL Script:**
    *   **File Name:** `bq_gestern.sql` (or similar, reflecting its origin).
    *   **Language:** BigQuery Standard SQL.
    *   **Content:**
        ```sql
        -- This script replicates the functionality of the legacy gestern.ksh script
        -- to calculate and format today's and yesterday's dates.

        -- Declare variables to hold the formatted date strings
        DECLARE Var_Datum_Heute STRING;
        DECLARE Var_Monat_Heute STRING;
        DECLARE Var_Datum_Gestern STRING;
        DECLARE Var_Monat_Gestern STRING;

        -- Get the current date (in UTC or specified timezone)
        DECLARE today_date DATE DEFAULT CURRENT_DATE(); -- Use CURRENT_DATE('Your/Timezone') if needed
        -- Calculate yesterday's date
        DECLARE yesterday_date DATE DEFAULT DATE_SUB(today_date, INTERVAL 1 DAY);

        -- Format today's date into YYYYMMDD and YYYYMM
        SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', today_date);
        SET Var_Monat_Heute = FORMAT_DATE('%Y%m', today_date);

        -- Format yesterday's date into YYYYMMDD and YYYYMM
        SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday_date);
        SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday_date);

        -- Output the results in the same order as the original shell script
        SELECT
          Var_Datum_Heute AS today_date_yyyymmdd,
          Var_Datum_Gestern AS yesterday_date_yyyymmdd,
          Var_Monat_Heute AS today_month_yyyymm,
          Var_Monat_Gestern AS yesterday_month_yyyymm;
        ```
2.  **Deployment:** Deploy this `bq_gestern.sql` script into the BigQuery environment. It can be run as a standard query or incorporated into a scheduled query or a BigQuery stored procedure if the output needs to be consistently produced and potentially stored or used by other BigQuery components.
3.  **Integration:** Identify any downstream processes that consume the output of the original `gestern.ksh` and update them to either:
    *   Call the new BigQuery SQL script directly.
    *   Use native date functions within their own BigQuery SQL.
    *   Utilize date functions available in the new orchestration tool (e.g., Airflow's JINJA templating for dates).
    Given the "Retire" bucket, the ideal approach is to eliminate the need for this specific script by using native date capabilities in consuming systems.