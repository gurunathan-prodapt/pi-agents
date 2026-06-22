# MIGRATION_NOTES.md

## 1. Summary

This migration job involved the conversion of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` and its associated Oracle SQL scripts (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`). The original script provided a suite of utility functions for date calculations and validations, interacting with an Oracle database via `sqlplus` for some operations and handling others natively within the shell.

The migration target platform is **Google BigQuery**. All functionalities have been refactored into equivalent BigQuery SQL stored procedures and user-defined functions (UDFs), eliminating dependencies on Oracle `sqlplus`, temporary files, and shell-based parsing.

## 2. Generated artifacts

The migration produced a single BigQuery SQL script that defines the necessary database objects:

*   **`bq_date_utils.sql`**:
    *   **Role**: This script contains the Data Definition Language (DDL) for creating a BigQuery dataset, user-defined functions (UDFs), and stored procedures. It serves as the deployment artifact for the migrated date utility logic.
    *   **Contents**:
        *   **Dataset**: `dataset` (placeholder, to be configured)
        *   **User-Defined Functions (UDFs)**:
            *   `dataset.LetzterTagDesMonat(datum STRING)`: Returns `BOOL` indicating if a date is the last day of its month.
            *   `dataset.TageimMonat(jahr INT64, monat INT64)`: Returns `INT64` representing the number of days in a given month/year.
            *   `dataset.AddiereDatum(datum STRING, tage INT64)`: Returns `STRING` (YYYYMMDD) after adding days to a given date.
        *   **Stored Procedures**:
            *   `dataset.DWDate_Vormonat(IN v_dateformat STRING, OUT v_result STRING)`: Calculates the last day of the previous month.
            *   `dataset.DWDate_Datum_Check(IN wert STRING, IN format STRING)`: Validates a date string against a format, raising an error on failure.
            *   `dataset.DWDate_Datum_LE(IN datum1 STRING, IN datum2 STRING)`: Compares two dates, raising an error if `datum1 > datum2`.
            *   `dataset.DWDate_Gib_Zeitraum(IN offset_value INT64, IN stufe STRING, IN format STRING, OUT start_date STRING, OUT end_date STRING)`: Calculates a date range based on an offset and step (Day, Month, Year).

## 3. Key design decisions

*   **Target Platform Choice**: Google BigQuery was chosen to leverage its native SQL capabilities for date/time operations, scalability, and integration within the Google Cloud ecosystem.
*   **Elimination of External Dependencies**: The core design decision was to remove all dependencies on external systems (Oracle database via `sqlplus`) and shell-specific mechanisms (temporary files, `grep`, `cut`, `eval`). This simplifies the architecture, improves performance, and reduces operational overhead.
*   **BigQuery UDFs vs. Stored Procedures**:
    *   **UDFs** were chosen for functions that perform direct, scalar computations and return a single value (`LetzterTagDesMonat`, `TageimMonat`, `AddiereDatum`). This aligns with their functional nature.
    *   **Stored Procedures** were chosen for functions involving control flow (IF/ELSE), variable assignments, and explicit error raising (`DWDate_Vormonat`, `DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`). This allows for more complex logic and explicit output parameters.
*   **Native BigQuery Date Functions**: All date arithmetic and formatting are performed using BigQuery's built-in `DATE_ADD`, `DATE_SUB`, `LAST_DAY`, `DATE_TRUNC`, `PARSE_DATE`, and `FORMAT_DATE` functions, ensuring optimal performance and correctness within the BigQuery environment.
*   **Error Handling**: Oracle's `sqlplus` exit codes and PL/SQL exceptions are translated into BigQuery's native error-raising mechanisms (`RAISE USING MESSAGE`) for equivalent behavior.

**Notable Trade-offs**:

*   **Orchestration Shift**: The original KornShell script could be directly executed or called from other shell scripts. The migrated BigQuery procedures/UDFs require a BigQuery client or an orchestration service (e.g., Cloud Composer, Workflows) to invoke them. This shifts the orchestration responsibility from the shell environment to a cloud-native scheduler.
*   **Semantic Equivalence**: While efforts were made to match the logic, subtle differences in date arithmetic behavior between Oracle and BigQuery (e.g., `ADD_MONTHS` edge cases) might exist. Thorough testing is crucial to confirm semantic equivalence.

## 4. Manual steps before go-live

Before the migrated components can be used in a production environment, the following manual steps are required:

1.  **BigQuery Dataset Creation/Configuration**:
    *   The generated `bq_date_utils.sql` script uses `CREATE SCHEMA IF NOT EXISTS dataset;`. The placeholder `dataset` must be replaced with the actual target BigQuery dataset name (e.g., `your_project_id.your_dataset_name`).
    *   Ensure the dataset exists or is created with the correct project ID and location.

2.  **IAM Permissions**:
    *   The service account or user identity that will deploy these BigQuery objects must have sufficient IAM permissions (e.g., `roles/bigquery.dataEditor` or more granular roles like `bigquery.routines.create`, `bigquery.routines.update`, `bigquery.routines.delete`, `bigquery.datasets.create`) on the target BigQuery project and dataset.
    *   The identity that will *execute* these procedures/UDFs must have `bigquery.routines.call` permission.

3.  **Deployment**:
    *   Execute the `bq_date_utils.sql` script against the target BigQuery project and dataset using the `bq` command-line tool, BigQuery UI, or a client library (e.g., Python).
        ```bash
        bq query --use_legacy_sql=false --file=bq_date_utils.sql
        ```
        (Ensure `dataset` placeholder is replaced with the actual dataset name in the file before execution).

4.  **Orchestration Integration**:
    *   If the original `h_alis_date.ksh` script was part of a scheduled job or a larger workflow, the new BigQuery procedures must be integrated into a new orchestration system (e.g., Cloud Composer/Airflow DAGs, Cloud Scheduler, Workflows). This involves creating new tasks to call the BigQuery procedures.

5.  **Environment Variable Deprecation**:
    *   The legacy environment variables like `DW_ORAUSER` and `DW_DIR_ROOT` are no longer relevant for the migrated BigQuery components and should be removed from any calling contexts.

## 5. Known gaps & unresolved references

The following items were identified as potential gaps or areas requiring further attention:

*   **Missing Complexity/Automation Metrics**: The initial analysis lacked complexity and automation tier metrics for the source files. This means the migration effort might have been underestimated, and a deeper manual review of the original script's intricacies might be beneficial if unexpected issues arise.
*   **Dynamic Variable Names (`eval`)**: The original script used `eval` for dynamic variable assignment, particularly in `DWDate_Vormonat` and `DWDate_Gib_Zeitraum`. The BigQuery migration uses explicit `OUT` parameters. While this covers the known use cases, if the original script had highly dynamic output variable naming conventions, this might require a more complex BigQuery solution (e.g., returning a `STRUCT` or `JSON` string) or a redesign of the calling application to handle fixed output parameter names. This is flagged as a **B4** item if the current `OUT` parameter approach proves insufficient for specific dynamic scenarios.
*   **Oracle-specific Date Semantics**: Although BigQuery's date functions are robust, subtle differences in how Oracle and BigQuery handle edge cases (e.g., `ADD_MONTHS` behavior on month-ends, `LAST_DAY` with specific dates) could lead to discrepancies. Thorough testing with a wide range of dates, especially boundary conditions, is essential to confirm exact semantic equivalence.
*   **Environment Variable Replacement**: The original `DW_ORAUSER` and `DW_DIR_ROOT` environment variables are implicitly replaced by the BigQuery project/dataset context. Any logic that relied on these variables for configuration or paths will need to be explicitly configured within the BigQuery environment or the calling orchestration.

## 6. Validation

Validation ensures that the migrated BigQuery components function correctly and produce results consistent with the legacy system.

**How to run the tests**:

1.  **Unit Testing**:
    *   For each BigQuery UDF and Stored Procedure, execute it directly in the BigQuery console or via the `bq` command-line tool with a comprehensive set of input parameters.
    *   **Example for UDF `AddiereDatum`**:
        ```sql
        SELECT dataset.AddiereDatum('20230101', 30);
        SELECT dataset.AddiereDatum('20240228', 1); -- Leap year
        SELECT dataset.AddiereDatum('20231231', 1); -- Year rollover
        ```
    *   **Example for Stored Procedure `DWDate_Vormonat`**:
        ```sql
        DECLARE result STRING;
        CALL dataset.DWDate_Vormonat('%Y%m%d', result);
        SELECT result;
        ```
    *   **Example for Stored Procedure `DWDate_Datum_Check` (testing error handling)**:
        ```sql
        -- This should succeed
        CALL dataset.DWDate_Datum_Check('20230101', '%Y%m%d');
        -- This should raise an error
        CALL dataset.DWDate_Datum_Check('20231301', '%Y%m%d');
        ```
2.  **Comparative Testing**:
    *   Prepare a set of diverse input values for each function.
    *   Execute the original `h_alis_date.ksh` script with these inputs and capture its output.
    *   Execute the corresponding BigQuery UDFs/Stored Procedures with the same inputs and capture their outputs.
    *   Compare the outputs meticulously.
3.  **Edge Case Testing**:
    *   Specifically test dates around month ends, year ends, leap years, and invalid date formats to ensure BigQuery's behavior matches the expected legacy behavior.
    *   Test `DWDate_Gib_Zeitraum` with various `offset_value` and `stufe` combinations, including negative offsets.

**What "passing" means**:

*   All BigQuery UDFs and Stored Procedures execute without BigQuery runtime errors (unless an error is explicitly expected, as in `DWDate_Datum_Check` for invalid dates or `DWDate_Datum_LE` for `datum1 > datum2`).
*   The output values from the BigQuery components precisely match the output values obtained from the original `h_alis_date.ksh` script for all tested inputs.
*   Error conditions (e.g., invalid date formats, date comparison failures) are handled consistently and raise appropriate errors as designed.
*   Performance of the BigQuery routines is acceptable for the intended use cases.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after deployment, the following steps outline the rollback procedure to revert to the legacy system:

1.  **Decommission BigQuery Objects**:
    *   Delete the BigQuery UDFs and Stored Procedures created during the migration. This can be done individually or by dropping the entire dataset (if it was created solely for these objects).
        ```bash
        # Example to drop a procedure
        bq rm -r -f --routine dataset.DWDate_Vormonat
        # Example to drop a function
        bq rm -r -f --routine dataset.LetzterTagDesMonat
        # To drop the entire dataset (USE WITH CAUTION, ensure no other critical data resides here)
        bq rm -r -f --dataset your_project_id:dataset
        ```
    *   Alternatively, if `CREATE OR REPLACE` was used, simply not deploying the new version effectively rolls back to the previous state (if there was one).

2.  **Revert Orchestration**:
    *   Reconfigure any calling applications, scheduling systems (e.g., Cloud Composer DAGs, Cloud Scheduler jobs), or workflows to invoke the original `h_alis_date.ksh` script and its dependencies.
    *   Ensure any environment variables (`DW_ORAUSER`, `DW_DIR_ROOT`) required by the legacy script are re-established in the execution environment.

3.  **Verify Legacy System**:
    *   Perform a smoke test on the original `h_alis_date.ksh` script to ensure it is fully operational and integrated correctly with its Oracle dependencies.
    *   Confirm that data pipelines or applications relying on the script are functioning as expected.