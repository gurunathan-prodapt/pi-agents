# MIGRATION_NOTES.md

## 1. Summary

This migration involved the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`. This script served as a utility library providing various date calculation and validation functions by interacting with an Oracle database via `sqlplus`.

The script's functionalities have been migrated to **Google Cloud BigQuery**. Each core function from the original KornShell script has been re-implemented as a BigQuery SQL User-Defined Function (UDF) or, where appropriate, a placeholder UDF for more complex logic that requires further analysis. The target architecture leverages BigQuery's native date/time functions, eliminating the need for `sqlplus` calls and external Oracle database dependencies.

## 2. Generated Artifacts

The migration produced the following BigQuery SQL files:

*   **`bigquery/ddl/dw_utils.sql`**
    *   **Role:** Defines the BigQuery dataset `dw_utils`. This dataset serves as the container for all migrated date utility UDFs and potentially other shared utility routines. It ensures a logical grouping and separation of these functions within BigQuery.

*   **`bigquery/udf/dw_utils.is_last_day_of_month.sql`**
    *   **Role:** Implements the logic of the legacy `LetzterTagDesMonat()` function. This UDF checks if a given date string (in `YYYYMMDD` format) represents the last day of its month. It returns `TRUE` or `FALSE`, or `NULL` for invalid input dates.

*   **`bigquery/udf/dw_utils.days_in_month.sql`**
    *   **Role:** Implements the logic of the legacy `TageimMonat()` function. This UDF calculates the number of days in a specified month and year. It returns an `INT64` representing the number of days, or `NULL` for invalid year/month combinations.

*   **`bigquery/udf/dw_utils.add_days_to_date.sql`**
    *   **Role:** Implements the logic of the legacy `AddiereDatum()` function. This UDF adds a specified number of days to an input date string (in `YYYYMMDD` format) and returns the resulting date as a string in the same format. It returns `NULL` for invalid input dates.

*   **`bigquery/udf/dw_utils.is_valid_date.sql`**
    *   **Role:** Implements the logic of the legacy `DWDate_Datum_Check()` function. This UDF validates if a given date string conforms to a specified format. It returns `TRUE` for valid dates, `FALSE` for invalid dates, and `NULL` if the input date string or format is `NULL`.

*   **`bigquery/udf/dw_utils.is_date_le.sql`**
    *   **Role:** Implements the logic of the legacy `DWDate_Datum_LE()` function. This UDF compares two date strings (in `YYYYMMDD` format) and returns `TRUE` if the first date is less than or equal to the second date, `FALSE` otherwise. It returns `NULL` if any input date string is invalid.

*   **`bigquery/udf/dw_utils.get_previous_month.sql`**
    *   **Role:** Provides a BigQuery UDF for the legacy `DWDate_Vormonat()` function. This UDF calculates a date from the previous month relative to an input date string. The exact logic of the original `d_alis_vormonat.sql` was not available, so this UDF provides a common interpretation. It returns the date string in a specified output format, or `NULL` for invalid input.

*   **`bigquery/udf/dw_utils.get_date_range.sql`**
    *   **Role:** Provides a BigQuery UDF for the legacy `DWDate_Gib_Zeitraum()` function. This UDF calculates a date range (start and end dates) based on a current date, an offset, and a unit (Days, Months, Years). The exact logic of the original `d_alis_datum_zeitraum.sql` was not available, so this UDF implements common interpretations. It returns a `STRUCT` containing the start and end date strings, or `NULL` for invalid inputs or unsupported units.

## 3. Key Design Decisions

*   **Migration to BigQuery UDFs/Stored Procedures:** The primary decision was to re-implement the KornShell functions as BigQuery SQL UDFs. This approach directly leverages BigQuery's powerful and optimized native date/time functions, eliminating the overhead and complexity of shell scripting, `sqlplus` invocations, and output parsing.
*   **Elimination of Oracle Dependencies:** All direct dependencies on Oracle `sqlplus` and the `DUAL` table have been removed. BigQuery's SQL capabilities allow for direct evaluation of expressions and date manipulations without needing a separate database connection.
*   **Handling of External SQL Files:** The original script invoked external SQL files (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`). Since their content was not available, placeholder UDFs (`dw_utils.get_previous_month`, `dw_utils.get_date_range`) were created based on common interpretations of their likely functionality. This is a **notable trade-off** as the exact behavior might differ if the original SQL scripts contained highly specific or complex logic. Further analysis of these files is required for full fidelity.
*   **Standardized Date Formats:** Input date strings are primarily expected in `YYYYMMDD` format, and output strings are also formatted similarly or as specified by an `output_format` parameter. This standardizes interaction with the UDFs.
*   **Robust Error Handling within UDFs:** Instead of shell exit codes or Oracle `raise_application_error`, the BigQuery UDFs utilize `SAFE.PARSE_DATE` and `SAFE.MAKE_DATE`. This ensures that invalid date inputs gracefully return `NULL`, preventing runtime errors and allowing calling queries to handle invalid data explicitly.
*   **Performance Improvement:** Moving from shell-based `sqlplus` calls (which involve process spawning and network latency) to native BigQuery UDFs significantly improves performance for date operations, as they execute directly within the BigQuery engine.

## 4. Manual Steps Before Go-Live

1.  **Analyze Missing SQL Content (Critical B4 Item):**
    *   **Action:** Locate and thoroughly analyze the content of `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`.
    *   **Purpose:** Ensure the placeholder UDFs (`dw_utils.get_previous_month`, `dw_utils.get_date_range`) accurately reflect the original, potentially complex, logic. If the original logic is more intricate, these UDFs will need to be refined or re-designed.

2.  **BigQuery Dataset Creation:**
    *   **Action:** Execute `bigquery/ddl/dw_utils.sql` to create the `dw_utils` dataset in your target BigQuery project.
    *   **Command Example:** `bq mk --dataset --description "Dataset for migrated date utility functions from h_alis_date.ksh" your_project_id:dw_utils`

3.  **Deploy BigQuery UDFs:**
    *   **Action:** Execute each UDF SQL file (`bigquery/udf/*.sql`) against the `dw_utils` dataset.
    *   **Command Example (for each UDF):** `bq query --use_legacy_sql=false --file=bigquery/udf/dw_utils.is_last_day_of_month.sql`

4.  **IAM/Permissions:**
    *   **Action:** Grant appropriate BigQuery IAM roles to the service accounts or users that will be creating, managing, or executing these UDFs.
    *   **Required Roles:**
        *   `BigQuery Data Editor` (or `BigQuery Admin`) for creating/replacing UDFs.
        *   `BigQuery Data Viewer` (or `BigQuery User`) for executing UDFs.
    *   **Purpose:** Ensure proper access control for deployment and usage.

5.  **Identify and Refactor Calling Scripts:**
    *   **Action:** Identify all legacy scripts or applications that `source` or directly call functions from `h_alis_date.ksh`.
    *   **Purpose:** These calling scripts will need to be updated to invoke the new BigQuery UDFs. This might involve:
        *   Rewriting shell scripts to use `bq query` commands.
        *   Developing Python wrappers (e.g., using `google-cloud-bigquery` client library) to call the UDFs, potentially within an Airflow DAG or Cloud Functions.
        *   Integrating direct BigQuery UDF calls into existing BigQuery SQL queries or stored procedures.

6.  **Scheduling:**
    *   **Action:** If the original `h_alis_date.ksh` was part of a scheduled job, update the scheduler (e.g., cron, Oozie) to trigger the new BigQuery-based process (e.g., an Airflow DAG, Cloud Composer workflow, or a Cloud Scheduler job that invokes a BigQuery query).
    *   **Purpose:** Maintain the original execution schedule and dependencies.

## 5. Known Gaps & Unresolved References

*   **Missing External SQL Content (B4 Item):** The most significant gap is the unknown content of `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql`. The current UDFs (`dw_utils.get_previous_month`, `dw_utils.get_date_range`) are based on common interpretations. A full analysis and potential redesign of these UDFs are required once the original SQL content is available.
*   **Usage Patterns of Calling Scripts:** While the utility functions themselves are migrated, the exact context and parameters with which other scripts call `h_alis_date.ksh` are not fully known. This could impact the design of any required orchestration layer or the specific parameters needed for the new BigQuery UDFs.
*   **Error Handling Translation:** The original script used shell return codes and Oracle `raise_application_error`. While BigQuery UDFs return `NULL` for invalid inputs, the exact translation of specific error conditions (e.g., `raise_application_error` in `DWDate_Datum_LE`) to a BigQuery-native error mechanism or an orchestration layer's exception handling needs to be confirmed.
*   **Environment Variables:** The legacy script relied on `DW_ORAUSER` and `DW_DIR_ROOT`. These are no longer relevant in the BigQuery context. Any configuration they provided (e.g., Oracle connection details, file paths) must be re-evaluated and integrated into the new BigQuery environment or calling applications as appropriate.

## 6. Validation

Validation should involve both unit testing of individual UDFs and integration testing with any refactored calling processes.

### How to Run Tests:

1.  **Unit Tests for Each UDF:**
    *   Execute each BigQuery UDF with a variety of test cases directly in the BigQuery console or via the `bq query` command.
    *   **Test Cases:**
        *   **Valid Inputs:** Test with typical, expected date strings and parameters.
        *   **Edge Cases:** Test with dates at month/year boundaries, leap years, first/last day of month, zero/negative offsets (where applicable).
        *   **Invalid Inputs:** Test with malformed date strings, incorrect formats, `NULL` inputs, or out-of-range values (e.g., `month=13`).
        *   **Comparison with Legacy:** For each test case, compare the output of the BigQuery UDF with the output of the original `h_alis_date.ksh` function using the same inputs.

    *   **Example Test Query for `dw_utils.is_last_day_of_month`:**
        ```sql
        SELECT
          dw_utils.is_last_day_of_month('20230131') AS test_jan_31,
          dw_utils.is_last_day_of_month('20230228') AS test_feb_28,
          dw_utils.is_last_day_of_month('20240229') AS test_leap_feb_29,
          dw_utils.is_last_day_of_month('20230315') AS test_mid_month,
          dw_utils.is_last_day_of_month('INVALID_DATE') AS test_invalid;
        ```

2.  **Integration Tests (if calling scripts are refactored):**
    *   If any scripts that previously sourced `h_alis_date.ksh` have been refactored (e.g., into Python scripts or Airflow DAGs), execute these new processes.
    *   **Purpose:** Verify that the end-to-end data flow and logic remain consistent, and that the new BigQuery UDFs are correctly invoked and their results are properly consumed.

### What "Passing" Means:

*   **Functional Equivalence:** The output of each BigQuery UDF for a given input must precisely match the output of its corresponding legacy KornShell function.
*   **Error Handling:**
    *   For invalid inputs, UDFs should return `NULL` as designed, rather than throwing an error.
    *   Any specific error conditions from the legacy script (e.g., `raise_application_error`) should be handled appropriately in the BigQuery UDF or the calling orchestration layer.
*   **Performance:** While not a strict "pass/fail" criterion, the BigQuery UDFs are expected to perform significantly faster than the legacy `sqlplus` calls. Monitor and compare execution times.
*   **No Unintended Side Effects:** The UDFs should only perform the intended date calculations and not interact with other BigQuery resources or cause unexpected behavior.

## 7. Rollback Procedure

In the event of issues during or after go-live, the following steps outline the rollback procedure:

1.  **Deactivate New BigQuery Processes:**
    *   Immediately stop or disable any new BigQuery-based processes (e.g., Airflow DAGs, Cloud Functions, scheduled BigQuery queries) that were introduced as part of this migration.

2.  **Revert Calling Scripts:**
    *   Revert any changes made to calling scripts or applications that were updated to use the new BigQuery UDFs. Restore them to their state prior to the migration, where they sourced or called `h_alis_date.ksh`.

3.  **Re-enable Legacy `h_alis_date.ksh`:**
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` script and its dependencies (including Oracle database connectivity) are fully functional and re-enabled in the legacy environment.

4.  **Remove BigQuery UDFs:**
    *   Execute `DROP FUNCTION` statements for each migrated UDF in the `dw_utils` dataset.
    *   **Command Example (for each UDF):** `bq query --use_legacy_sql=false "DROP FUNCTION IF EXISTS dw_utils.is_last_day_of_month;"`

5.  **Remove BigQuery Dataset (Optional, if no other UDFs/tables are present):**
    *   If the `dw_utils` dataset was created solely for this migration and contains no other critical resources, it can be deleted.
    *   **Command Example:** `bq rm -r -f your_project_id:dw_utils` (Use `-r` for recursive deletion, `-f` for force).

6.  **Verify Legacy Functionality:**
    *   Thoroughly test the original `h_alis_date.ksh` and its dependent processes to ensure full restoration of functionality.