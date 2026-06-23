# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh

## 1. Purpose & Scope
The KornShell script `h_alis_date.ksh` serves as a utility library for date calculations within a data warehousing environment. Its primary purpose is to provide reusable functions for common date operations, including calculating previous months, validating and comparing dates, determining periods (e.g., month, year), identifying month-end dates, calculating days in a month, and adding days to a given date. These functions were historically implemented using a mix of shell scripting logic and interactions with an Oracle database via `sqlplus`. The business purpose is to standardize and centralize date-related computations used by other ETL jobs.

The scope of this migration is to re-implement the functionality of `h_alis_date.ksh` natively in Google BigQuery, specifically leveraging BigQuery SQL and stored procedures, to eliminate dependencies on KornShell, Oracle, and temporary file-based inter-process communication.

## 2. Source Inventory
| File Name                                                 | Technology | Tier        | Automation Bucket | Summary                                                                                                                                                                                                                                           |
| :-------------------------------------------------------- | :--------- | :---------- | :---------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | KornShell  | medium (inferred) | semi_auto         | KornShell script providing utility functions for date calculations, often interacting with an Oracle database via sqlplus. It defines functions to check, compare, and calculate dates, including month-end and date addition. |

## 3. Target Architecture
The functionality of `h_alis_date.ksh` will be migrated to BigQuery as a set of BigQuery SQL stored procedures. Each KornShell function will correspond to a BigQuery stored procedure. These procedures will reside in a dedicated dataset within BigQuery, e.g., `your_project.date_utilities`.

The target architecture will consist of:
*   **BigQuery Dataset:** `your_project.date_utilities` to house the stored procedures.
*   **BigQuery Stored Procedures:**
    *   `DWDate_Vormonat`
    *   `DWDate_Datum_Check`
    *   `DWDate_Datum_LE`
    *   `DWDate_Gib_Zeitraum`
    *   `LetzterTagDesMonat`
    *   `TageimMonat`
    *   `AddiereDatum`
*   **BigQuery Native Functions:** Leveraging BigQuery's rich set of date and time functions (`DATE_SUB`, `DATE_TRUNC`, `LAST_DAY`, `DATE_ADD`, `PARSE_DATE`, `FORMAT_DATE`, `SAFE.PARSE_DATE`, `DATE_DIFF`).

## 4. Data Flow & Lineage
The original script's data flow involves:
1.  **Input Parameters:** Each KornShell function takes parameters passed as command-line arguments.
2.  **Internal Logic:** Date parsing, calculations, and conditional logic within the shell script.
3.  **Oracle Interaction:** Via `sqlplus` calls, executing inline SQL (e.g., `TO_DATE` on `dual`) or external SQL scripts (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`).
4.  **Temporary File I/O:** `sqlplus` output is written to temporary files, which are then parsed by `grep` and `cut` to extract results.
5.  **Variable Assignment:** Results are assigned to shell variables using `eval`.
6.  **Output:** Function return codes, echoed values, or updated shell variables.

In BigQuery, this data flow will be transformed:
1.  **Input Parameters:** BigQuery stored procedures will accept typed input parameters (e.g., `INT64`, `STRING`, `BOOL`).
2.  **Internal Logic:** All date calculation logic will be implemented directly using BigQuery SQL functions.
3.  **Oracle Interaction Elimination:** Direct database calls will be replaced by native BigQuery date functions. The logic within `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` will need to be re-implemented as BigQuery SQL within the respective stored procedures or as helper functions.
4.  **Temporary File Elimination:** The need for temporary files and shell parsing (`grep`, `cut`) will be removed, as BigQuery procedures handle result sets and variable assignments internally.
5.  **Variable Assignment:** Procedure `OUT` parameters or BigQuery scripting variables will be used for returning results.
6.  **Output:** Procedure results will be available through output parameters or direct return values.

## 5. Transformation Logic
Each KornShell function will be migrated to a corresponding BigQuery SQL stored procedure.

| Original KornShell Function | BigQuery Stored Procedure Name | BigQuery Logic Overview                                                                                                                                                                                                                                                                                                                                                                                                                             |
| :-------------------------- | :----------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `DWDate_Vormonat()`         | `DWDate_Vormonat`              | Calculates the first day of the previous month and formats it. Replaces the `sqlplus` call to `d_alis_vormonat.sql` with `DATE_TRUNC`, `DATE_SUB`, and `FORMAT_DATE`.                                                                                                                                                                                                                                                                  |
| `DWDate_Datum_Check()`      | `DWDate_Datum_Check`           | Validates if a string is a valid date according to a given format. Replaces the `sqlplus` `TO_DATE` check with `SAFE.PARSE_DATE` and checks for `NULL`.                                                                                                                                                                                                                                                                                |
| `DWDate_Datum_LE()`         | `DWDate_Datum_LE`              | Compares two dates (`P1 <= P2`). Replaces the `sqlplus` PL/SQL block with `PARSE_DATE` and a direct comparison of `DATE` types. Error handling will be via `RAISE USING MESSAGE` if `p_datum1 > p_datum2` is considered an exceptional case, or simply returning a boolean.                                                                                                                                                                 |
| `DWDate_Gib_Zeitraum()`     | `DWDate_Gib_Zeitraum`          | Calculates a date period (start and end) based on an offset and granularity (`Y`, `M`, `D`). Replaces the `sqlplus` call to `d_alis_datum_zeitraum.sql` and subsequent `grep`/`cut` parsing with BigQuery date functions like `CURRENT_DATE()`, `DATE_TRUNC`, `DATE_ADD`, `LAST_DAY`. Includes conditional logic for granularity.                                                                                        |
| `LetzterTagDesMonat()`      | `LetzterTagDesMonat`           | Checks if a given date is the last day of its month. Replaces manual date parsing and leap year logic with `PARSE_DATE` and `LAST_DAY`.                                                                                                                                                                                                                                                                                              |
| `TageimMonat()`             | `TageimMonat`                  | Calculates the number of days in a specific month of a year. Replaces manual leap year logic and array lookup with `DATE` construction, `LAST_DAY`, and `DATE_DIFF`.                                                                                                                                                                                                                                                                |
| `AddiereDatum()`            | `AddiereDatum`                 | Adds a specified number of days to a given date. Replaces manual arithmetic and overflow handling with `PARSE_DATE` and `DATE_ADD`.                                                                                                                                                                                                                                                                                                     |

**BigQuery Pseudocode (from `shellscript_to_bqsql_design` tool output):**

```sql
-- Procedure for DWDate_Vormonat
CREATE OR REPLACE PROCEDURE DWDate_Vormonat(
  IN p_format STRING,
  OUT p_result STRING
)
BEGIN
  SET p_result = FORMAT_DATE(
    p_format,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
  );
END;

-- Procedure for DWDate_Datum_Check
CREATE OR REPLACE PROCEDURE DWDate_Datum_Check(
  IN p_wert STRING,
  IN p_format STRING,
  OUT p_is_valid BOOL
)
BEGIN
  DECLARE v_date DATE DEFAULT NULL;
  SET v_date = SAFE.PARSE_DATE(p_format, p_wert);
  IF v_date IS NULL THEN
    SET p_is_valid = FALSE;
  ELSE
    SET p_is_valid = TRUE;
  END IF;
END;

-- Procedure for DWDate_Datum_LE
CREATE OR REPLACE PROCEDURE DWDate_Datum_LE(
  IN p_datum1 STRING,
  IN p_datum2 STRING,
  OUT p_is_le BOOL
)
BEGIN
  DECLARE v_datum1 DATE;
  DECLARE v_datum2 DATE;
  SET v_datum1 = PARSE_DATE('%Y%m%d', p_datum1);
  SET v_datum2 = PARSE_DATE('%Y%m%d', p_datum2);
  IF v_datum1 > v_datum2 THEN
    SET p_is_le = FALSE;
  ELSE
    SET p_is_le = TRUE;
  END IF;
END;

-- Procedure for DWDate_Gib_Zeitraum
CREATE OR REPLACE PROCEDURE DWDate_Gib_Zeitraum(
  IN p_offset INT64,
  IN p_stufe STRING,
  IN p_format STRING,
  OUT p_start STRING,
  OUT p_ende STRING
)
BEGIN
  DECLARE v_today DATE DEFAULT CURRENT_DATE();
  DECLARE v_start DATE;
  DECLARE v_ende DATE;

  IF p_stufe = 'D' THEN
    SET v_start = v_today;
    SET v_ende = DATE_ADD(v_today, INTERVAL p_offset DAY);
  ELSEIF p_stufe = 'M' THEN
    SET v_start = DATE_TRUNC(v_today, MONTH);
    SET v_ende = LAST_DAY(DATE_ADD(v_start, INTERVAL p_offset MONTH));
  ELSEIF p_stufe = 'Y' THEN
    SET v_start = DATE_TRUNC(v_today, YEAR);
    SET v_ende = LAST_DAY(DATE_ADD(v_start, INTERVAL p_offset YEAR));
  ELSE
    RAISE USING MESSAGE = 'Invalid p_stufe';
  END IF;

  SET p_start = FORMAT_DATE(p_format, v_start);
  SET p_ende = FORMAT_DATE(p_format, v_ende);
END;

-- Procedure for LetzterTagDesMonat
CREATE OR REPLACE PROCEDURE LetzterTagDesMonat(
  IN p_datum STRING,
  OUT p_is_last_day BOOL
)
BEGIN
  DECLARE v_date DATE DEFAULT PARSE_DATE('%Y%m%d', p_datum);
  IF v_date = LAST_DAY(v_date) THEN
    SET p_is_last_day = TRUE;
  ELSE
    SET p_is_last_day = FALSE;
  END IF;
END;

-- Procedure for TageimMonat
CREATE OR REPLACE PROCEDURE TageimMonat(
  IN p_jahr INT64,
  IN p_monat INT64,
  OUT p_tage INT64
)
BEGIN
  DECLARE v_first DATE DEFAULT DATE(p_jahr, p_monat, 1);
  DECLARE v_last DATE DEFAULT LAST_DAY(v_first);
  SET p_tage = DATE_DIFF(DATE_ADD(v_last, INTERVAL 1 DAY), v_first, DAY);
END;

-- Procedure for AddiereDatum
CREATE OR REPLACE PROCEDURE AddiereDatum(
  IN p_datum STRING,
  IN p_tage INT64,
  OUT p_result STRING
)
BEGIN
  DECLARE v_date DATE DEFAULT PARSE_DATE('%Y%m%d', p_datum);
  SET p_result = FORMAT_DATE('%Y%m%d', DATE_ADD(v_date, INTERVAL p_tage DAY));
END;
```

## 6. External Dependencies
The original script has the following external dependencies:

*   **Oracle Database & `sqlplus`:** The script heavily relies on `sqlplus` to execute Oracle SQL and PL/SQL for date validation, comparison, and calculation of previous month/date periods. This dependency will be completely removed. All Oracle-specific date logic will be re-implemented using BigQuery native date functions. The external SQL files (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`) will have their logic embedded directly into the corresponding BigQuery stored procedures.
*   **Shell Utilities (`grep`, `cut`, `basename`, `date`, `tail`, `rm`):** These are used for parsing `sqlplus` output from temporary files and for general shell scripting tasks. These dependencies will be eliminated. BigQuery stored procedures handle variable assignments and logic internally, removing the need for external parsing or temporary files.
*   **Environment Variables (`DW_ORAUSER`, `DW_DIR_ROOT`):** These variables configure the Oracle user and the directory for external SQL files. These will be replaced by direct references within the BigQuery stored procedures (e.g., project/dataset names for any future table references, or hardcoded values if the paths become irrelevant). For instance, the `DW_ORAUSER` is no longer needed, and the logic from files under `DW_DIR_ROOT` will be integrated.
*   **Temporary Files:** The script uses temporary files (`/tmp/h_alis_date_...tmp`) for inter-process communication with `sqlplus`. These will be eliminated entirely in the BigQuery solution.

## 7. Unresolved / Risks
*   **Unresolved Lineage:** The `lineage_edges` query returned no results, meaning the tool could not automatically detect execution order or data flow dependencies based on the `sqlplus` calls. The design proceeds based on manual code inspection. Any other scripts calling `h_alis_date.ksh` will need their invocation mechanisms updated from shell commands to BigQuery procedure calls.
*   **Oracle-specific Date Behavior:** While BigQuery date functions are robust, subtle differences in how Oracle handles specific date edge cases (e.g., timezone handling, specific `TO_DATE` format behaviors not perfectly aligned with `PARSE_DATE`) might exist. Thorough testing is required to ensure functional equivalence.
*   **Dynamic Variable Names (`eval`):** The original script uses `eval` to assign values to dynamically named variables. In BigQuery, this will be handled by using `OUT` parameters in stored procedures, which requires the calling context to be aware of the specific output parameter names. If a truly dynamic variable assignment is needed (e.g., the name of the output variable is a runtime parameter), BigQuery scripting variables or returning a temporary table could be considered, but `OUT` parameters are the most direct replacement.
*   **Error Handling Fidelity:** The original script's error handling involves `return` codes and `raise_application_error` in Oracle. BigQuery procedures will use `RAISE USING MESSAGE` for errors and boolean `OUT` parameters for success/failure indicators where `return` codes were used. The exact error messages and their behavior might need to be fine-tuned for BigQuery's error model.

## 8. Build Plan
1.  **Create BigQuery Dataset:** Create the `your_project.date_utilities` dataset in BigQuery.
2.  **Translate External SQL:** Review the content of `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` (if available) and translate their logic into BigQuery SQL. This logic will be directly embedded into the `DWDate_Vormonat` and `DWDate_Gib_Zeitraum` stored procedures, respectively.
3.  **Implement BigQuery Stored Procedures:**
    *   Create `DWDate_Vormonat` stored procedure using the provided BigQuery pseudocode.
    *   Create `DWDate_Datum_Check` stored procedure using the provided BigQuery pseudocode.
    *   Create `DWDate_Datum_LE` stored procedure using the provided BigQuery pseudocode.
    *   Create `DWDate_Gib_Zeitraum` stored procedure using the provided BigQuery pseudocode.
    *   Create `LetzterTagDesMonat` stored procedure using the provided BigQuery pseudocode.
    *   Create `TageimMonat` stored procedure using the provided BigQuery pseudocode.
    *   Create `AddiereDatum` stored procedure using the provided BigQuery pseudocode.
4.  **Unit Testing:** Develop comprehensive unit tests for each BigQuery stored procedure to ensure functional equivalence with the original KornShell functions across various date inputs and edge cases (e.g., leap years, month boundaries).
5.  **Integration Testing:** Update any calling scripts or jobs that currently invoke `h_alis_date.ksh` to call the new BigQuery stored procedures. This will involve changing shell script calls to BigQuery API calls or integrating with an orchestration tool (e.g., Airflow) that can execute BigQuery procedures.
6.  **Deployment:** Deploy the BigQuery stored procedures to the target BigQuery environment.
7.  **Documentation Update:** Update internal documentation to reflect the migration of date utility functions to BigQuery.