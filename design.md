# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

This document outlines the migration design for migrating the date calculation utility script `gestern.ksh` from a legacy KornShell environment to Google Cloud Platform (GCP) with BigQuery as the target platform.

---

## 1. Purpose & Scope

### Legacy Business Function
The script `gestern.ksh` is a helper utility used within the Information Services Reporting environment of T-Mobil (Deutsche Telekom Mobilnet). Its purpose is to calculate current and previous dates and months relative to the execution runtime. 

Specifically, it computes:
*   **Today's Date** in `YYYYMMDD` format (`Var_Datum_Heute`)
*   **Yesterday's Date** in `YYYYMMDD` format (`Var_Datum_Gestern`)
*   **Current Month** in `YYYYMM` format (`Var_Monat_Heute`)
*   **Previous Month** (relative to yesterday) in `YYYYMM` format (`Var_Monat_Gestern`)

### Target Scope
The script prints these values on a single space-separated line to standard output (`stdout`), allowing calling shell scripts or job schedulers (such as UC4) to parse them and pass them as execution parameters to downstream SQL statements or data-processing scripts. 

In the target architecture, these date calculations are either:
1.  **Fully integrated into BigQuery SQL statements** using native date functions.
2.  **Determined at the orchestration layer (Apache Airflow / Cloud Composer)** and injected as dynamic parameters.
3.  **Encapsulated as a BigQuery Stored Procedure** for backwards compatibility with any remaining legacy-style scripting.

---

## 2. Source Inventory

| File Path | Technology | Complexity Tier | Automation Bucket | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh` | KornShell (KSH) | Medium | Semi-auto (Rate: 0.65) | Calculates reporting date and month parameters. |

---

## 3. Target Architecture

The manual date and leap-year calculations in the legacy Shell script are highly redundant on modern cloud platforms. The target architecture replaces manual arithmetic with native, database-level or workflow-level date calculations.

There are two primary targets for these calculations:

```
               +---------------------------------------------------+
               |               Legacy Shell Script                 |
               |                  gestern.ksh                      |
               +------------------------+--------------------------+
                                        |
                                        v
                 +----------------------+----------------------+
                 |                                             |
                 v                                             v
  +------------------------------+              +------------------------------+
  |    Option A: Orchestration   |              |   Option B: BigQuery Engine  |
  |     (Airflow / Composer)     |              |     (Stored Proc / View)     |
  +------------------------------+              +------------------------------+
  |  Use Jinja templates,        |              |  Use native SQL functions    |
  |  Airflow macros, or Python   |              |  (CURRENT_DATE, DATE_SUB)    |
  |  to compute and pass dates.  |              |  for dynamic data filtering. |
  +------------------------------+              +------------------------------+
```

### Option A: Cloud Composer / Apache Airflow Integration (Recommended)
When migrating the orchestration from UC4 / Shell to Airflow, date calculations should be handled dynamically via Airflow macros or Python execution context variables (such as `ds` and `yesterday_ds`). This eliminates execution-time overhead and simplifies scripts.

### Option B: BigQuery Stored Procedure / Table Function
To maintain a drop-in database replacement that returns identical records, we will implement a lightweight BigQuery table function or stored procedure that returns these values dynamically.

---

## 4. Data Flow & Lineage

The legacy data flow is local, execution-time-bound, and has no database tables as inputs.

### Legacy Execution Flow
1.  **System Clock Trigger**: Invokes system command `date '+ %d %m %Y'` to retrieve current day, month, and year.
2.  **Date Arithmetic Logic**:
    *   If today's day is $> 1$, decrement day by $1$.
    *   If today's day is $1$, transition to the previous month, computing the last day of that month (handling February leap-year logic manual checks).
    *   If month is January (1), transition to December (12) of the previous year.
3.  **Padding & String Building**: Ensures all day and month strings are zero-padded (e.g., `09` instead of `9`).
4.  **Stdout Output**: Emits the variables: `[Var_Datum_Heute] [Var_Datum_Gestern] [Var_Monat_Heute] [Var_Monat_Gestern]`.

---

## 5. Transformation Logic

### Bug in Legacy Implementation
The legacy shell script has a subtle bug in its manual leap-year logic:
```bash
if (( `expr $Var_Nummer_Heute_Jahr % 4` == 0  && \
      `expr $Var_Nummer_Heute_Jahr % 100` > 0 && \
           $Var_Nummer_Gestern_Monat == 2))
```
This logic assumes that leap years are strictly multiples of 4 that are not divisible by 100. However, centurial years divisible by 400 (e.g., the year 2000) are indeed leap years. Under the original shell script, the year 2000 would fail this check and assign February 28 days instead of 29. 

**Solution**: By moving to native BigQuery date operations, all calendar calculations (including leap years and timezone offsets) are handled automatically and correctly.

### Mapping Legacy Constructs to BigQuery

| Legacy Construct | BigQuery Standard SQL Equivalent | Notes |
| :--- | :--- | :--- |
| `date '+ %d %m %Y'` | `CURRENT_DATE('Europe/Berlin')` | Must specify European timezone to avoid UTC boundary shifts. |
| `expr $Var_Nummer_Heute_Tag - 1` | `DATE_SUB(..., INTERVAL 1 DAY)` | Native date subtraction. |
| Zero padding (`LPAD`) | `FORMAT_DATE('%Y%m%d', ...)` | Safe formatting handles padding implicitly. |
| `case "$Var_Nummer_Gestern_Monat"` | Handled natively by calendar engine | Completely bypassed. |

### Target Code Compilation

#### Implementation 1: Simplified Declarative BigQuery SQL (Recommended)
This clean query produces the identical output string format as the legacy script:

```sql
SELECT
  FORMAT_DATE('%Y%m%d', CURRENT_DATE('Europe/Berlin')) AS Var_Datum_Heute,
  FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Europe/Berlin'), INTERVAL 1 DAY)) AS Var_Datum_Gestern,
  FORMAT_DATE('%Y%m', CURRENT_DATE('Europe/Berlin')) AS Var_Monat_Heute,
  FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE('Europe/Berlin'), INTERVAL 1 DAY)) AS Var_Monat_Gestern;
```

#### Implementation 2: BigQuery Backwards-Compatible Stored Procedure
For stored procedures that need to return or output these parameters dynamically into SQL variables:

```sql
CREATE OR REPLACE PROCEDURE `isbert_aufbereitung.get_reporting_dates`(
  OUT Var_Datum_Heute STRING,
  OUT Var_Datum_Gestern STRING,
  OUT Var_Monat_Heute STRING,
  OUT Var_Monat_Gestern STRING
)
BEGIN
  DECLARE today DATE;
  DECLARE yesterday DATE;
  
  -- Use local reporting timezone
  SET today = CURRENT_DATE('Europe/Berlin');
  SET yesterday = DATE_SUB(today, INTERVAL 1 DAY);
  
  SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', today);
  SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday);
  SET Var_Monat_Heute = FORMAT_DATE('%Y%m', today);
  SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday);
END;
```

---

## 6. External Dependencies

1.  **System Timezone (Critical)**:
    The original script executed on local Unix servers in Germany, meaning it implicitly used Central European Time (CET/CEST). In GCP, the system time defaults to Coordinated Universal Time (UTC). 
    *   **Risk**: If timezone-agnostic `CURRENT_DATE()` is called in BigQuery, the date will shift to the next day at 11:00 PM or 12:00 AM Central European Time, causing data-loading discrepancies.
    *   **Mitigation**: All target code (both in BigQuery and Airflow) must explicitly use the `'Europe/Berlin'` timezone.
2.  **Calling Process Parsing**:
    Any wrappers that parse the stdout output of `gestern.ksh` must be updated to either execute the BigQuery query directly or capture the parameters via Cloud Composer variables.

---

## 7. Unresolved / Risks

*   **Upstream Wrapper Identification**: The pre-collected context does not list the calling parent scripts that execute `gestern.ksh`. If these scripts are migrated to Airflow, their dependency on parsing `gestern.ksh`'s stdout must be converted to utilize native Airflow variables.
*   **Execution Time Sync**: If a script is run exactly at midnight, minor timing differences between systems could result in different date assignments. Calling tasks should compute this once per batch execution and share the value.

---

## 8. Build Plan

The migration of this utility script follows a three-step build plan:

1.  **Orchestrator Setup (Airflow)**:
    If this script serves Airflow DAG tasks, utilize Airflow's built-in macro parameters.
    *   `{{ ds_nodash }}` (corresponds to `Var_Datum_Heute`)
    *   `{{ yesterday_ds_nodash }}` (corresponds to `Var_Datum_Gestern`)
    *   `{{ logical_date.strftime('%Y%m') }}` (corresponds to `Var_Monat_Heute`)
2.  **Database View Creation**:
    Deploy the lightweight helper view inside BigQuery for SQL-only workflows:
    ```sql
    CREATE OR REPLACE VIEW `isbert_aufbereitung.v_reporting_dates` AS 
    SELECT
      FORMAT_DATE('%Y%m%d', CURRENT_DATE('Europe/Berlin')) AS Var_Datum_Heute,
      FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Europe/Berlin'), INTERVAL 1 DAY)) AS Var_Datum_Gestern,
      FORMAT_DATE('%Y%m', CURRENT_DATE('Europe/Berlin')) AS Var_Monat_Heute,
      FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE('Europe/Berlin'), INTERVAL 1 DAY)) AS Var_Monat_Gestern;
    ```
3.  **Deprecation**:
    Once calling structures have been updated to utilize either the Airflow parameter macros or the `isbert_aufbereitung.v_reporting_dates` view, deprecate and decommission `gestern.ksh`.