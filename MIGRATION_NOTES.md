# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`, which calculates and formats today's and yesterday's dates, has been migrated to Google BigQuery. The original script's logic, including handling of month/year transitions and leap years, has been translated into a BigQuery SQL script. The output, previously printed to standard output, is now returned as a `SELECT` statement result set.

## 2. Generated artifacts

*   **`gestern_bq.sql`**: This BigQuery SQL script contains the translated logic from the original `gestern.ksh` KornShell script. It uses BigQuery scripting features (`DECLARE`, `SET`, `IF`, `CASE`) and functions (`CURRENT_DATE()`, `EXTRACT()`, `LPAD()`, `CONCAT()`, `MOD()`, `CAST()`) to calculate and format today's and yesterday's dates and months. When executed, it returns a single row with four columns: `TodayDate`, `YesterdayDate`, `TodayMonth`, and `YesterdayMonth`.

## 3. Key design decisions

*   **Direct Translation to BigQuery SQL Scripting:** The procedural nature of the original KornShell script, with its variable assignments, conditional logic (`if/else`), and `case` statements, was directly translated into BigQuery SQL scripting. This approach ensures high fidelity to the original logic and minimizes potential behavioral changes.
    *   **Trade-off:** While BigQuery offers more concise date functions like `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` which inherently handle month/year transitions and leap years, the migration tool opted for a direct translation of the manual calculation logic. This preserves the exact original calculation steps but results in a more verbose and less idiomatic BigQuery solution. A future optimization (B4 item) could refactor this to use native BigQuery date arithmetic.
*   **Output as `SELECT` Statement:** The original script printed its results to standard output. In BigQuery, this behavior is replicated by a final `SELECT` statement that returns the calculated date and month variables as a result set. This allows for easy consumption by downstream processes or for direct viewing.
*   **Timezone Handling:** `CURRENT_DATE()` in BigQuery returns the current date in the default timezone of the query execution environment, which is typically UTC unless explicitly configured otherwise at the project or session level. The original `date` command in KornShell would use the system's local timezone. This migration assumes that the default BigQuery timezone (UTC) is acceptable. If the original script relied on a specific local timezone, explicit timezone handling (e.g., `CURRENT_DATE('America/New_York')`) would be required in the BigQuery script.

## 4. Manual steps before go-live

1.  **BigQuery Project and Dataset Setup:** Ensure a target BigQuery project and dataset exist where the `gestern_bq.sql` script can be deployed. No specific tables are created or required by this script, but it needs an execution environment.
2.  **IAM Permissions:** The service account or user executing the BigQuery script must have the necessary IAM permissions:
    *   `bigquery.jobs.create` to run queries.
    *   If deployed as a stored procedure: `bigquery.routines.create` in the target dataset.
    *   If scheduled: Permissions for the scheduling service (e.g., Cloud Scheduler, Cloud Composer service account) to invoke BigQuery jobs.
3.  **Deployment (if Stored Procedure):** If the script is to be deployed as a stored procedure, execute the `CREATE OR REPLACE PROCEDURE` statement (wrapping the provided SQL) in the target BigQuery dataset.
    ```sql
    CREATE OR REPLACE PROCEDURE `your_project.your_dataset.gestern_calculator`()
    BEGIN
      -- Content of gestern_bq.sql goes here
      DECLARE Var_Nummer_Null INT64 DEFAULT 0;
      -- ... rest of the script ...
    END;
    ```
4.  **Scheduling:** If the original `gestern.ksh` script was part of a scheduled job, the migrated BigQuery script must also be scheduled. This can be done using:
    *   **BigQuery Scheduled Queries:** For simple, recurring executions.
    *   **Cloud Composer (Apache Airflow):** For more complex orchestration and dependency management.
    *   **Cloud Scheduler:** To trigger a Cloud Function or other service that executes the BigQuery script.
    *   Configure the schedule to match the original job's frequency and timing.

## 5. Known gaps & unresolved references

*   **Leap Year Logic Simplification (B4 Item):** The manual leap year calculation and day-of-month logic (e.g., `CASE Var_Nummer_Gestern_Monat WHEN 2 THEN SET Var_Nummer_Gestern_Tag = 28;`) was directly translated. This is functional but less robust and more verbose than using BigQuery's native date functions. A B4 item is to refactor the calculation of `Var_Nummer_Gestern_Tag`, `Var_Nummer_Gestern_Monat`, and `Var_Nummer_Gestern_Jahr` to use `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` and then `EXTRACT` from the result. This would simplify the code and leverage BigQuery's built-in date intelligence.
*   **Error Handling:** The original script's minimal error handling (`SELECT 'Fehler !!!!' AS error_message;`) has been directly translated. For a production BigQuery stored procedure, it would be beneficial to replace this with more robust error handling, such as `RAISE` statements or logging mechanisms, to provide clearer indications of issues.
*   **Timezone Alignment:** As noted in "Key design decisions," the default UTC behavior of `CURRENT_DATE()` in BigQuery might differ from the original KornShell script's system timezone. If the exact local timezone of the source system is critical for date calculations, this needs to be explicitly addressed in the BigQuery script (e.g., `CURRENT_DATE('Europe/Berlin')`). This is currently an unresolved reference.
*   **Missing Metadata:** The design document noted the absence of complexity and automation rate metadata for the original script. While the script's simplicity allowed for a straightforward migration, this gap in information could impact future migration planning for similar assets.

## 6. Validation

To validate the migrated BigQuery script, execute `gestern_bq.sql` in a BigQuery environment and compare its output against the output of the original `gestern.ksh` script.

**How to run the tests:**

1.  **Execute `gestern_bq.sql`:**
    *   Open the BigQuery console.
    *   Navigate to the project and dataset where the script is deployed (or simply open a new query tab).
    *   Paste the content of `gestern_bq.sql` into the query editor and run it.
2.  **Execute `gestern.ksh`:**
    *   Log in to the environment where the original `gestern.ksh` script resides.
    *   Execute the script: `./gestern.ksh`
    *   Capture the standard output.

**What "passing" means:**

The BigQuery script passes validation if the values returned by its final `SELECT` statement (`TodayDate`, `YesterdayDate`, `TodayMonth`, `YesterdayMonth`) exactly match the corresponding values printed by the original `gestern.ksh` script for the same execution date.

**Recommended Test Cases:**

It is crucial to test the script with various dates to ensure correct handling of edge cases:

*   **Standard Day:** Execute on a typical day (e.g., 2023-10-15).
*   **First Day of Month:** Execute on the 1st of any month (e.g., 2023-10-01).
*   **First Day of Year:** Execute on January 1st (e.g., 2023-01-01).
*   **Day After Leap Day:** Execute on March 1st of a leap year (e.g., 2024-03-01).
*   **Day After Non-Leap Feb:** Execute on March 1st of a non-leap year (e.g., 2023-03-01).
*   **Leap Year February 29th:** Execute on February 29th of a leap year (e.g., 2024-02-29).
*   **Non-Leap Year February 28th:** Execute on February 28th of a non-leap year (e.g., 2023-02-28).
*   **End of Month (30-day month):** Execute on the 1st of a 31-day month following a 30-day month (e.g., 2023-07-01, checking for June 30th).
*   **End of Month (31-day month):** Execute on the 1st of a 30-day month following a 31-day month (e.g., 2023-05-01, checking for April 30th).

## 7. Rollback procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Stop New BigQuery Job/Schedule:** Immediately disable or delete any BigQuery Scheduled Queries, Cloud Composer DAGs, or Cloud Scheduler jobs that are executing the `gestern_bq.sql` script or calling the `gestern_calculator` stored procedure.
2.  **Re-enable Original Script:** Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh` script and its associated scheduling mechanism in the source environment.
3.  **Delete BigQuery Artifacts:**
    *   If deployed as a stored procedure, drop the `gestern_calculator` procedure from the BigQuery dataset:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project.your_dataset.gestern_calculator`;
        ```
    *   Remove the `gestern_bq.sql` file from the BigQuery project's source control or deployment location.
4.  **Monitor Original Job:** Verify that the original `gestern.ksh` script is running as expected and producing correct output.