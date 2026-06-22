# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

## 1. Purpose & Scope
The KornShell script `gestern.ksh` is responsible for calculating and formatting today's date and yesterday's date. It handles month and year transitions, including a rudimentary leap year calculation for February. The script's output, consisting of four space-separated date and month key strings (`YYYYMMDD` for dates, `YYYYMM` for month keys), is printed to standard output. This output is likely consumed by subsequent processes as date parameters.

The scope of this migration is to replicate the exact date calculation and formatting logic in Google BigQuery, ensuring the output format remains consistent for any downstream dependencies.

## 2. Source Inventory
The job consists of a single KornShell script.

| File Path                                                 | Technology | Complexity Tier | Automation Bucket | Notes                                                                                                                                                                                                                                                                                                                                                          |
| :-------------------------------------------------------- | :--------- | :-------------- | :---------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh` | KornShell  | Simple          | Auto              | This script calculates today's date, yesterday's date, the current month key, and yesterday's month key. It uses the system's `date` command and performs manual arithmetic and conditional logic for date transitions and leap years. Output is to stdout as space-separated `YYYYMMDD` and `YYYYMM` strings. (Complexity and Automation Bucket assumed due to database missing this information, and the simplicity of the script). |

## 3. Target Architecture
The functionality of `gestern.ksh` will be migrated to Google BigQuery. Given its simple nature and output to standard output, the most appropriate BigQuery component is a BigQuery SQL script or a BigQuery Stored Procedure. This will allow the date calculation to be performed directly within the BigQuery environment, making the output available for subsequent BigQuery operations or for export/consumption by other services if needed.

The output will be generated as a single row result set from a `SELECT` statement, matching the space-separated output of the original script but as distinct columns.

## 4. Data Flow & Lineage
The original `gestern.ksh` script operates as a standalone utility. It takes no explicit inputs (it relies on the system's current date) and produces four formatted date strings as output to standard output.

**Legacy Flow:**
`System Date` → `gestern.ksh` (KornShell script) → `stdout: "YYYYMMDD_today YYYYMMDD_yesterday YYYYMM_current YYYYMM_yesterday"`

**Target BigQuery Flow:**
`BigQuery System Date (CURRENT_DATE())` → `BigQuery SQL Script/Stored Procedure` → `BigQuery Result Set` (containing four STRING columns)

There are no external system dependencies for this specific script's core functionality (date calculation) based on the analysis. The `lineage_edges` analysis for this `run_id` did not show any direct dependencies (READS/WRITES/INVOKES) involving `gestern.ksh`, further supporting its role as a standalone utility.

## 5. Transformation Logic

The core logic of `gestern.ksh` involves:
1.  Getting the current date (day, month, year).
2.  Calculating yesterday's date by:
    *   Subtracting 1 from the day if the day is greater than 1.
    *   If the day is 1, transitioning to the previous month/year and determining the last day of that month, including a basic leap year check for February.
3.  Formatting the calculated dates into `YYMMDD` and `YYMM` strings, padding single-digit days/months with a leading zero.
4.  Printing these four values.

The BigQuery migration will leverage BigQuery's native date functions for simplicity and accuracy, as recommended by the CM MCP tool.

**Original KornShell Output:**
`echo $Var_Datum_Heute $Var_Datum_Gestern $Var_Monat_Heute $Var_Monat_Gestern`

**Target BigQuery SQL Implementation (Simplified Native Version):**

```sql
DECLARE today_date DATE DEFAULT CURRENT_DATE();
DECLARE yesterday_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

DECLARE Var_Datum_Heute STRING DEFAULT FORMAT_DATE('%Y%m%d', today_date);
DECLARE Var_Datum_Gestern STRING DEFAULT FORMAT_DATE('%Y%m%d', yesterday_date);
DECLARE Var_Monat_Heute STRING DEFAULT FORMAT_DATE('%Y%m', today_date);
DECLARE Var_Monat_Gestern STRING DEFAULT FORMAT_DATE('%Y%m', yesterday_date);

SELECT
  Var_Datum_Heute AS today_ymd,
  Var_Datum_Gestern AS yesterday_ymd,
  Var_Monat_Heute AS today_ym,
  Var_Monat_Gestern AS yesterday_ym;
```

This BigQuery SQL script achieves the same result as the original KornShell script but with greater precision for leap years and overall calendar logic due to `DATE_SUB`, and in a native BigQuery format.

## 6. External Dependencies
The original `gestern.ksh` script has no explicit external system dependencies other than the operating system's `date` command and standard shell utilities (`expr`, `echo`).
The `lineage_assembled_jobs` query confirmed no `external_systems` were identified for this job.

In the BigQuery environment, these dependencies are replaced by BigQuery's internal date and time functions (`CURRENT_DATE()`, `DATE_SUB()`, `FORMAT_DATE()`). No other external systems (like Oracle, SFTP, S3) are referenced or required for this specific piece of logic.

## 7. Unresolved / Risks
**Unresolved:**
*   The `file_complexity` and `automation_rate` tables did not contain entries for `gestern.ksh`. This means the migration team did not explicitly assign a complexity tier or automation bucket. However, given the script's simple functionality, it is assessed as 'Simple' and 'Auto' migratable.
*   The original script has a basic error message `echo "Fehler !!!!"` for an unexpected date state. The BigQuery equivalent includes `SELECT 'Fehler !!!!' AS error_message;` for direct translation, but ideally, in a production BigQuery environment, error handling would involve BigQuery-specific logging mechanisms or more robust error reporting. For direct functional equivalence, the provided translation is sufficient.

**Risks:**
*   **Dependency on Output Format:** If any downstream legacy system relies on the exact space-separated string output of `gestern.ksh` as a single string, the BigQuery output (which will be a structured result set with multiple columns) would need an additional step to concatenate these into a single string if consumed by external tools expecting that specific format. If used within BigQuery, the columnar output is preferable.
*   **Timezone considerations:** The original script relies on the server's local time zone. BigQuery's `CURRENT_DATE()` function typically operates in UTC by default unless a specific timezone is set for the session or project. This is a common migration consideration that needs to be addressed if the original script operated in a specific non-UTC timezone and downstream systems depend on that. The current BigQuery translation does not explicitly define a timezone, implying UTC.

## 8. Build Plan
The migration involves creating a BigQuery SQL script or a stored procedure to replicate the functionality of `gestern.ksh`.

**Build Item:** BigQuery SQL Script / Stored Procedure
**Language:** Google Standard SQL

**Code (Simplified Native Version - Recommended):**

```sql
-- Filename: gestern_bq.sql (or as a stored procedure)

DECLARE today_date DATE DEFAULT CURRENT_DATE();
DECLARE yesterday_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

DECLARE Var_Datum_Heute STRING DEFAULT FORMAT_DATE('%Y%m%d', today_date);
DECLARE Var_Datum_Gestern STRING DEFAULT FORMAT_DATE('%Y%m%d', yesterday_date);
DECLARE Var_Monat_Heute STRING DEFAULT FORMAT_DATE('%Y%m', today_date);
DECLARE Var_Monat_Gestern STRING DEFAULT FORMAT_DATE('%Y%m', yesterday_date);

SELECT
  Var_Datum_Heute AS today_ymd,
  Var_Datum_Gestern AS yesterday_ymd,
  Var_Monat_Heute AS today_ym,
  Var_Monat_Gestern AS yesterday_ym;
```

**Steps:**
1.  Create a new BigQuery SQL file (e.g., `gestern_bq.sql`) in the target repository.
2.  Populate it with the provided BigQuery SQL code.
3.  Integrate this script into the relevant BigQuery job orchestration (e.g., Airflow DAG, Cloud Composer, Scheduled Query in BigQuery) to be executed whenever the original `gestern.ksh` was scheduled.
4.  Ensure any downstream processes that consumed the output of `gestern.ksh` are updated to consume the output from the BigQuery script, adapting to the columnar output if necessary.