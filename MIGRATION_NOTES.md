# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`, responsible for calculating and formatting today's date, yesterday's date, and their respective month keys, has been migrated. The functionality has been re-implemented in Google BigQuery using Standard SQL. The target platform is Google Cloud Platform, specifically BigQuery, to leverage its native date functions and integrate seamlessly with other BigQuery-based data processes.

## 2. Generated artifacts

The migration produced the following artifact:

*   **`gestern_bq.sql`**: This BigQuery SQL script replicates the date calculation and formatting logic of the original `gestern.ksh` script. It uses BigQuery's native date functions (`CURRENT_DATE()`, `DATE_SUB()`, `FORMAT_DATE()`) to determine today's date, yesterday's date, the current month key, and yesterday's month key, outputting them as a single-row result set with four distinct string columns.

## 3. Key design decisions

*   **Target Platform Choice (BigQuery SQL Script/Stored Procedure)**: Given the `gestern.ksh` script's standalone nature and its output to standard output, a BigQuery SQL script (or a stored procedure if more complex orchestration is needed) was chosen as the most direct and appropriate replacement. This allows the date calculation to occur natively within the BigQuery environment, making the output readily available for subsequent BigQuery operations.
*   **Leveraging Native BigQuery Date Functions**: Instead of directly translating the KornShell script's manual date arithmetic and conditional logic (especially for month transitions and rudimentary leap year checks), the migration leverages BigQuery's robust and accurate built-in date functions (`CURRENT_DATE()`, `DATE_SUB()`, `FORMAT_DATE()`). This simplifies the code, improves accuracy (e.g., for leap years), and aligns with BigQuery best practices.
*   **Output Format Transformation**: The original script outputs a single space-separated string to `stdout`. The BigQuery implementation produces a structured, single-row result set with four distinct string columns (`today_ymd`, `yesterday_ymd`, `today_ym`, `yesterday_ym`). This columnar output is more idiomatic for BigQuery and provides clearer data separation for downstream consumers within the BigQuery ecosystem.
*   **Trade-offs**:
    *   **Output Consumption**: The change from a single space-separated string output to a structured columnar result set means any external downstream systems directly consuming the `stdout` of `gestern.ksh` will require adaptation. If such systems exist and cannot easily consume BigQuery's structured output, an additional step to concatenate the BigQuery columns into a single string might be necessary.
    *   **Timezone Handling**: The original script relied on the server's local timezone. BigQuery's `CURRENT_DATE()` typically operates in UTC by default. This difference could lead to discrepancies if the original script operated in a specific non-UTC timezone and downstream processes are sensitive to this. The current BigQuery implementation assumes UTC or the project's default timezone.

## 4. Manual steps before go-live

Before the migrated `gestern_bq.sql` can go live, the following manual steps are required:

1.  **BigQuery Project and Dataset**: Ensure a target Google Cloud Project and a BigQuery dataset are designated for hosting and executing this script. While the script itself doesn't create tables, it will run within the context of a project.
2.  **IAM/Permissions**:
    *   The service account or user executing the BigQuery script must have appropriate IAM roles, including `BigQuery Job User` (roles/bigquery.jobUser) to run queries and `BigQuery Data Viewer` (roles/bigquery.dataViewer) if it were to read from tables (not applicable here, but good practice).
    *   Ensure the entity responsible for scheduling (e.g., Cloud Composer service account) has these permissions.
3.  **Scheduling**: The original `gestern.ksh` script was likely scheduled via a cron job or similar mechanism. This scheduling needs to be replicated in the Google Cloud environment.
    *   **Option A (Recommended for orchestration)**: Integrate `gestern_bq.sql` into an existing or new Cloud Composer (Apache Airflow) DAG. The DAG would execute the BigQuery script.
    *   **Option B (For simple, recurring execution)**: Configure a BigQuery Scheduled Query to run `gestern_bq.sql` at the desired frequency.
    *   **Option C (For external triggers)**: If triggered by an external system, use a Cloud Function or Cloud Run service to execute the BigQuery script via the BigQuery API.
4.  **Downstream System Updates**: Identify and update any downstream processes or applications that consumed the output of the original `gestern.ksh`. These systems must be adapted to consume the structured columnar output from BigQuery, or an intermediate step must be introduced to transform the BigQuery output back into the expected single string format if strictly necessary.

## 5. Known gaps & unresolved references

*   **Error Handling**: The original script had a basic `echo "Fehler !!!!"` for an unexpected date state. The current BigQuery translation does not include explicit BigQuery-native error handling (e.g., `RAISE ERROR`). While the `DATE_SUB` function is robust, for production-grade BigQuery scripts, more sophisticated error logging (e.g., to Cloud Logging) or error reporting mechanisms might be desired (B4 item).
*   **Original Script Metadata**: The `file_complexity` and `automation_rate` for `gestern.ksh` were not explicitly available in the source inventory. The assessment of 'Simple' and 'Auto' migratable was based on functional analysis.
*   **Timezone Sensitivity**: As noted in "Key design decisions," the potential difference in timezone handling between the original server's local time and BigQuery's default UTC could be a gap if downstream systems are highly sensitive to the exact time of day the date is calculated. This requires verification with business users.

## 6. Validation

To validate the migrated `gestern_bq.sql` script:

1.  **Execution**:
    *   Execute the `gestern_bq.sql` script directly in the BigQuery console or via the `bq query` command-line tool.
    *   If integrated into a scheduler (e.g., Airflow), trigger the DAG manually.
2.  **Output Verification**:
    *   **Functional Equivalence**: Compare the output of `gestern_bq.sql` with the output of the original `gestern.ksh` script for the same execution date.
        *   Example `gestern.ksh` output (executed on 2023-10-27): `20231027 20231026 202310 202310`
        *   Expected `gestern_bq.sql` output (executed on 2023-10-27):
            ```
            +------------+---------------+----------+-------------+
            | today_ymd  | yesterday_ymd | today_ym | yesterday_ym|
            +------------+---------------+----------+-------------+
            | 20231027   | 20231026      | 202310   | 202310      |
            +------------+---------------+----------+-------------+
            ```
    *   **Edge Cases**:
        *   **First day of the month**: Execute on the 1st of a month (e.g., 2023-11-01). Verify `yesterday_ymd` correctly reflects the last day of the previous month (e.g., `20231031`) and `yesterday_ym` reflects the previous month key (e.g., `202310`).
        *   **First day of the year**: Execute on January 1st (e.g., 2024-01-01). Verify `yesterday_ymd` is December 31st of the previous year (e.g., `20231231`) and `yesterday_ym` is December of the previous year (e.g., `202312`).
        *   **Leap Year (March 1st)**: Execute on March 1st of a leap year (e.g., 2024-03-01). Verify `yesterday_ymd` is February 29th (e.g., `20240229`).
3.  **"Passing" Criteria**:
    *   The `gestern_bq.sql` script executes successfully without any BigQuery errors.
    *   The output values for `today_ymd`, `yesterday_ymd`, `today_ym`, and `yesterday_ym` precisely match the expected values derived from the execution date, including all edge cases.
    *   The output format (column names and data types) is consistent with the generated artifact.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Halt New Process**: Immediately stop any BigQuery Scheduled Queries, Cloud Composer DAGs, or other orchestration mechanisms that are executing `gestern_bq.sql`.
2.  **Re-enable Legacy Process**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh` script in its legacy environment (e.g., re-activate its cron job).
3.  **Revert Downstream Consumers**: If any downstream systems were updated to consume the BigQuery output, revert them to consume the output from the original `gestern.ksh` script.
4.  **Monitor**: Monitor both the legacy process and any dependent systems to ensure normal operation is restored.
5.  **Analysis**: Investigate the root cause of the rollback to address the issues before attempting re-migration.