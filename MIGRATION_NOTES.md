```markdown
# MIGRATION_NOTES: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

## 1. Summary

The legacy KornShell script `gestern.ksh`, responsible for calculating and formatting today's and yesterday's dates and months (in `YYYYMMDD` and `YYYYMM` formats), has been migrated. Its functionality has been re-implemented in **Google BigQuery SQL**, leveraging native BigQuery date and time functions. The original script's manual date calculation logic, including its incomplete leap year handling, has been replaced with BigQuery's robust and accurate built-in capabilities.

## 2. Generated Artifacts

*   **File:** `gestern_bq.sql`
    *   **Role:** This BigQuery SQL script provides the equivalent functionality of the original `gestern.ksh`. It uses `CURRENT_DATE()`, `DATE_SUB()`, and `FORMAT_DATE()` to calculate and format today's date, yesterday's date, today's month, and yesterday's month. The script outputs these four string values as a single row result set, which can be consumed by other BigQuery processes or external applications.

## 3. Key Design Decisions

*   **Native BigQuery SQL Implementation:** The core decision was to re-implement the date calculation logic directly in BigQuery SQL using its native date and time functions (`CURRENT_DATE()`, `DATE_SUB()`, `FORMAT_DATE()`).
    *   **Why this approach:**
        *   **Robustness and Accuracy:** BigQuery's native functions correctly handle complex date arithmetic, including month/year transitions and leap years, without requiring custom, potentially error-prone logic. This resolves the incomplete leap year logic present in the original KornShell script.
        *   **Performance:** Native functions are optimized for performance within the BigQuery environment.
        *   **Maintainability:** Reduces the amount of custom code, making the solution easier to understand, debug, and maintain.
        *   **Cloud-Native Alignment:** Leverages the capabilities of the target platform, integrating seamlessly into the BigQuery ecosystem.
        *   **"Retire" Automation Bucket:** The original script was flagged for "retire" (B0), indicating its functionality could be absorbed by cloud-native capabilities. This migration aligns with that recommendation by providing a modern, integrated solution.
*   **No Direct 1:1 Translation of Manual Logic:** Instead of attempting to translate the KornShell script's `if/else` and `case` statements for date calculation, the migration opted for the more idiomatic and reliable BigQuery date functions. This is a deliberate choice to improve correctness and simplify the solution.
*   **Output as SELECT Statement:** The BigQuery script returns the four date values as a `SELECT` statement, making them easily consumable by other BigQuery queries, views, or external tools that can execute BigQuery SQL.

## 4. Manual Steps Before Go-Live

The following steps are required to prepare for the go-live of the migrated functionality:

1.  **IAM/Permissions:**
    *   Ensure that the service account or user identity that will execute `gestern_bq.sql` has the necessary BigQuery permissions, specifically `bigquery.jobs.create` in the target BigQuery project.
2.  **Integration with Consumers:**
    *   Identify all downstream processes, scripts, or applications that currently rely on the output of the original `gestern.ksh` script.
    *   Update these consumers to execute `gestern_bq.sql` (e.g., via a BigQuery client library, a scheduled query, or a Cloud Composer/Airflow DAG) and consume its output. Alternatively, the logic from `gestern_bq.sql` can be directly embedded into consuming BigQuery queries or views if appropriate.
3.  **Scheduling (if applicable):**
    *   If the original `gestern.ksh` was part of a scheduled job, establish a new scheduling mechanism for `gestern_bq.sql` or its consuming processes within the Google Cloud environment (e.g., Cloud Scheduler, Cloud Composer, BigQuery Scheduled Queries).
4.  **No Schema/Dataset/Connection String/Secrets Creation:** This migration does not involve creating new BigQuery datasets or tables, nor does it require specific connection strings or secrets, as it is a self-contained utility query.

## 5. Known Gaps & Unresolved References

*   **Original Script's "Retire" Status (B0):** The original `gestern.ksh` was categorized under the "retire" automation bucket. While this migration provides a modern equivalent, it's crucial to confirm that the functionality provided by `gestern_bq.sql` is still actively required by downstream processes. If no consumers are identified or if the need for these specific date strings has diminished, the BigQuery script might also be considered for deprecation.
*   **Consumer Identification:** A critical follow-up is the comprehensive identification and update of all consuming systems that previously relied on `gestern.ksh`. Any unaddressed consumers will represent a gap in the migration.
*   **No Unresolved Targets:** As per the migration design, there are no specific unresolved targets or external dependencies identified for this job.
*   **No Redesign (B4) Items:** No specific redesign items were flagged for this migration beyond the general modernization to BigQuery SQL.

## 6. Validation

To validate the successful migration and functionality of `gestern_bq.sql`:

1.  **Execution:**
    *   Execute the `gestern_bq.sql` script in the target BigQuery project.
    *   Example: `bq query --use_legacy_sql=false --file=./gestern_bq.sql`
2.  **Output Verification:**
    *   **Expected Output:** The script should return a single row with four columns: `Var_Datum_Heute`, `Var_Datum_Gestern`, `Var_Monat_Heute`, `Var_Monat_Gestern`.
    *   **Comparison:**
        *   Run the original `gestern.ksh` script on the same day as the BigQuery script.
        *   Compare the output of `gestern_bq.sql` with the output of `gestern.ksh`.
        *   **Passing Criteria:** The output values for all four date strings must match exactly.
    *   **Edge Cases:**
        *   Test on January 1st to verify correct year and month transitions for yesterday's date (e.g., `20240101` should yield `20231231`).
        *   Test on March 1st in a leap year (e.g., 2024) to ensure February 29th is correctly handled for yesterday's date.
        *   Test on March 1st in a non-leap year (e.g., 2023) to ensure February 28th is correctly handled for yesterday's date.

## 7. Rollback Procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Revert Consumer Changes:**
    *   Immediately revert any changes made to downstream jobs, scripts, or applications that were updated to consume the output of `gestern_bq.sql`. Point them back to their original source of date values (which might be the re-enabled `gestern.ksh` or another mechanism).
2.  **Decommission BigQuery Script:**
    *   If `gestern_bq.sql` was deployed as a standalone scheduled query or part of a larger BigQuery script, disable or delete it.
3.  **Re-enable Legacy Script:**
    *   If the original `gestern.ksh` script was disabled or removed from its scheduling, re-enable it and restore its original invocation mechanism.
4.  **Monitor:**
    *   Monitor the affected systems to ensure they are functioning correctly with the reverted configuration.
```