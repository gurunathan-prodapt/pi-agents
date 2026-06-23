The migration of `r_aurd_rechstan.ksh` to Google BigQuery involves re-architecting a KornShell-based orchestration and SQL execution into BigQuery stored procedures, complemented by BigQuery tables for logging and configuration. The following test cases are designed to ensure the migrated solution is behaviourally equivalent to the legacy system, covering output parity, transformation correctness, external system replacements (internal framework calls in this case), and data quality assertions.

---

## Migration Validation Tests for `r_aurd_rechstan.ksh`

### 1. Output Parity — Same Inputs Produce Same Outputs

#### Test Case 1.1: Basic Full Run with Explicit Stichtag

*   **Purpose:** Verify that a standard execution of the migrated job, with an explicitly provided `Stichtag` and no restart value, produces data in the target table identical to the legacy job. This validates the core `INSERT...SELECT` logic and basic parameter handling.
*   **Setup:**
    1.  **Legacy:**
        *   Ensure the legacy DWH source tables (corresponding to `project.dataset.source_table`) are populated with a known, consistent dataset.
        *   Ensure the legacy FOS target table is empty or in a known baseline state.
    2.  **Migrated:**
        *   Populate `project.dataset.source_table` in BigQuery with data identical to the legacy DWH source tables.
        *   Ensure `project.dataset.target_table` is empty.
        *   Ensure `project.dataset.job_log` and `project.dataset.job_status` are empty or in a known baseline state.
        *   Define a specific `Stichtag` for the test (e.g., '01012023').
*   **Action:**
    1.  **Legacy:** Execute the legacy script:
        ```bash
        r_aurd_rechstan.ksh -s 01012023
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('01012023', 0);
        ```
*   **Pass/Fail Criterion:**
    *   The row count in the legacy FOS target table must be identical to the row count in `project.dataset.target_table`.
    *   A deep comparison of all columns and rows between the legacy FOS target table (captured as a snapshot) and `project.dataset.target_table` must show no differences.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming 'legacy_fos_table_snapshot' is a BigQuery table containing a snapshot of the legacy FOS table
        -- after the legacy job run, with identical schema to `project.dataset.target_table`.
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.target_table`) = (SELECT COUNT(*) FROM `project.dataset.legacy_fos_table_snapshot`)
                THEN 'PASS: Row counts match'
                ELSE 'FAIL: Row counts differ'
            END AS row_count_check,
            CASE
                WHEN (
                    SELECT COUNT(*) FROM (
                        SELECT * FROM `project.dataset.target_table`
                        EXCEPT DISTINCT
                        SELECT * FROM `project.dataset.legacy_fos_table_snapshot`
                    )
                ) = 0
                AND
                (
                    SELECT COUNT(*) FROM (
                        SELECT * FROM `project.dataset.legacy_fos_table_snapshot`
                        EXCEPT DISTINCT
                        SELECT * FROM `project.dataset.target_table`
                    )
                ) = 0
                THEN 'PASS: Data content matches'
                ELSE 'FAIL: Data content differs'
            END AS data_content_check;
        ```

#### Test Case 1.2: Stichtag Fallback Logic

*   **Purpose:** Verify that when `Stichtag` is not provided, the migrated job correctly applies the fallback logic (`LEAST(CURRENT_DATE(), MAX(ladedatum))`) as specified in the design, and produces the correct output.
*   **Setup:**
    1.  **Legacy:**
        *   Ensure legacy DWH source tables are in a known state, including `ladedatum` values.
        *   Ensure legacy FOS target table is empty.
        *   Set the system date for the legacy environment to a specific date (e.g., '2023-01-15') to control `CURRENT_DATE()`.
    2.  **Migrated:**
        *   Populate `project.dataset.source_table` with identical data, including `ladedatum` values.
        *   Ensure `project.dataset.target_table` is empty.
        *   Ensure `project.dataset.job_log` and `project.dataset.job_status` are empty.
        *   Ensure the BigQuery environment's `CURRENT_DATE()` is effectively '2023-01-15' (e.g., by running the test on that date or mocking).
*   **Action:**
    1.  **Legacy:** Execute the legacy script without the `-s` parameter:
        ```bash
        r_aurd_rechstan.ksh
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure, passing `NULL` for `p_input_stichtag`:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`(NULL, 0);
        ```
*   **Pass/Fail Criterion:**
    *   The `Stichtag` determined and logged by the legacy script must match the `stichtag` logged in `project.dataset.job_log` for the `PARAM_FALLBACK` entry.
    *   The final data in the legacy FOS target table must be identical to the data in `project.dataset.target_table`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Verify logged Stichtag
        SELECT
            CASE
                WHEN stichtag = DATE('2023-01-15') -- Assuming MAX(ladedatum) is also 2023-01-15 or earlier
                THEN 'PASS: Fallback Stichtag matches expected date'
                ELSE 'FAIL: Fallback Stichtag does not match'
            END AS fallback_stichtag_check
        FROM `project.dataset.job_log`
        WHERE status = 'PARAM_FALLBACK'
        ORDER BY log_timestamp DESC
        LIMIT 1;

        -- Then, perform the same data content comparison as in Test Case 1.1.
        ```

#### Test Case 1.3: Restart Logic - Partial Run and Resume

*   **Purpose:** Verify that the restart functionality (`p_wiederanlaufWert`) correctly deletes existing records and then inserts new/remaining records based on `dwh_vertrag_id`, resulting in identical final data.
*   **Setup:**
    1.  **Legacy & Migrated (Initial Load):**
        *   Populate source tables with a diverse set of `dwh_vertrag_id` values (e.g., 100, 200, 300, 400, 500) and data that would be selected by the `Stichtag` filter.
        *   Run a full initial load (e.g., `r_aurd_rechstan.ksh -s 01012023` and `CALL ...('01012023', 0)`) to populate the target tables.
    2.  **Legacy & Migrated (Restart Scenario):**
        *   Introduce new data into the source tables with `dwh_vertrag_id` values greater than a chosen restart point (e.g., `dwh_vertrag_id > 300`).
        *   Choose a `p_wiederanlaufWert` (e.g., 300).
*   **Action:**
    1.  **Legacy:** Execute the legacy script with a restart value:
        ```bash
        r_aurd_rechstan.ksh -s 01012023 -l 300
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure with the restart value:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('01012023', 300);
        ```
*   **Pass/Fail Criterion:**
    *   The final data in the legacy FOS target table must be identical to the data in `project.dataset.target_table`.
    *   Specifically, records with `dwh_vertrag_id < p_wiederanlaufWert` should remain untouched, and records with `dwh_vertrag_id >= p_wiederanlaufWert` should reflect the re-processed state (deleted and re-inserted if criteria met).
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Perform the same data content comparison as in Test Case 1.1.
        ```

### 2. Transformation Correctness

#### Test Case 2.1: Date Filters and Edge Cases

*   **Purpose:** Verify the precise application of the date filters (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`) and handling of boundary conditions.
*   **Setup:**
    1.  **Legacy & Migrated:**
        *   Populate source tables with specific test data covering various date scenarios relative to a chosen `Stichtag` (e.g., '2023-01-15'):
            *   `gueltig_von` exactly `Stichtag` (should be included if other conditions met).
            *   `gueltig_bis` exactly `Stichtag` (should be excluded).
            *   `gueltig_bis` exactly `Stichtag + 1 day` (should be included).
            *   `ladedatum` exactly `Stichtag - 1 day` (should be included).
            *   `ladedatum` exactly `Stichtag` (should be excluded).
            *   Records where `gueltig_von` is after `Stichtag`.
            *   Records where `gueltig_bis` is before `Stichtag`.
        *   Ensure target tables are empty.
        *   Use a fixed `Stichtag` (e.g., '15012023').
*   **Action:**
    1.  **Legacy:** Execute the legacy script:
        ```bash
        r_aurd_rechstan.ksh -s 15012023
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('15012023', 0);
        ```
*   **Pass/Fail Criterion:**
    *   The final data in the legacy FOS target table must be identical to the data in `project.dataset.target_table`.
    *   Specifically, verify that records matching the filter criteria are included and those not matching are excluded, according to the precise date boundaries.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Perform the same data content comparison as in Test Case 1.1.
        -- Additionally, for specific edge cases, you might query the target table directly:
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.target_table` WHERE gueltig_von = '2023-01-15' AND gueltig_bis = '2023-01-16' AND ladedatum = '2023-01-14') = 1 -- Example expected count
                THEN 'PASS: Specific edge case record included'
                ELSE 'FAIL: Specific edge case record not included as expected'
            END AS edge_case_check;
        ```

#### Test Case 2.2: NULL Handling in Date Columns

*   **Purpose:** Verify how the migration handles `NULL` values in `gueltig_von`, `gueltig_bis`, and `ladedatum` columns, especially concerning the `WHERE` clause filters.
*   **Setup:**
    1.  **Legacy & Migrated:**
        *   Populate source tables with records containing `NULL` values in `gueltig_von`, `gueltig_bis`, and `ladedatum` columns, alongside valid data.
        *   Ensure target tables are empty.
        *   Use a fixed `Stichtag` (e.g., '01012023').
*   **Action:**
    1.  **Legacy:** Execute the legacy script:
        ```bash
        r_aurd_rechstan.ksh -s 01012023
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('01012023', 0);
        ```
*   **Pass/Fail Criterion:**
    *   The final data in the legacy FOS target table must be identical to the data in `project.dataset.target_table`.
    *   Specifically, records with `NULL`s in date columns should be excluded from the target table, as `NULL` comparisons typically evaluate to `UNKNOWN` and thus `FALSE` in `WHERE` clauses.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Perform the same data content comparison as in Test Case 1.1.
        -- Additionally, verify no records with NULL dates were inserted:
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.target_table` WHERE gueltig_von IS NULL OR gueltig_bis IS NULL OR ladedatum IS NULL) = 0
                THEN 'PASS: No records with NULL dates inserted'
                ELSE 'FAIL: Records with NULL dates found in target table'
            END AS null_date_check;
        ```

### 3. External-System Replacements (Internal Framework Calls)

The design document explicitly states "No external systems (like Oracle, SFTP, S3) were explicitly identified". Therefore, this section focuses on the correct replacement of the legacy KornShell framework calls (logging, parameter handling, date helpers) with BigQuery equivalents.

#### Test Case 3.1: Logging and Status Updates

*   **Purpose:** Verify that all significant events (job start, parameter parsing, core processing start/end, completion, errors) are correctly logged to `project.dataset.job_log` and `project.dataset.job_status` is updated appropriately throughout the job lifecycle.
*   **Setup:**
    1.  **Legacy & Migrated:**
        *   Populate source tables with minimal data.
        *   Ensure target tables are empty.
        *   Ensure `project.dataset.job_log` and `project.dataset.job_status` are empty.
        *   Use a fixed `Stichtag` (e.g., '01012023').
*   **Action:**
    1.  **Legacy:** Execute the legacy script:
        ```bash
        r_aurd_rechstan.ksh -s 01012023
        ```
        Capture the console output and inspect the generated log file.
    2.  **Migrated:** Execute the main BigQuery stored procedure:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('01012023', 0);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:** The log file should contain entries for job start, Stichtag info, core script invocation, and successful completion. The script should exit with status 0.
    *   **Migrated:**
        *   `project.dataset.job_log` should contain entries for:
            *   Job `STARTED`
            *   `k_aurd_rechstan` `RUNNING_CORE`
            *   `k_aurd_rechstan` `CORE_COMPLETED`
            *   Job `COMPLETED`
        *   The `stichtag` and `wiederanlauf_wert` in the log entries should match the input parameters.
        *   `project.dataset.job_status` should show `overall_status = 'OK'` and `last_run_timestamp`, `last_stichtag`, `last_wiederanlauf_wert` updated correctly.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Check log entries
        SELECT log_level, message, status, stichtag, wiederanlauf_wert
        FROM `project.dataset.job_log`
        WHERE job_id = 'AURD_RECHSTAN'
        ORDER BY log_timestamp;
        -- Expected: At least one entry for each status: STARTED, RUNNING_CORE, CORE_COMPLETED, COMPLETED.
        -- Verify stichtag and wiederanlauf_wert match '2023-01-01' and 0 respectively.

        -- Check final job status
        SELECT overall_status, last_stichtag, last_wiederanlauf_wert
        FROM `project.dataset.job_status`
        WHERE job_id = 'AURD_RECHSTAN';
        -- Expected: overall_status = 'OK', last_stichtag = DATE('2023-01-01'), last_wiederanlauf_wert = 0.
        ```

#### Test Case 3.2: Error Handling - Invalid Stichtag Format

*   **Purpose:** Verify that the migrated job correctly handles invalid `Stichtag` input format, logs the error, and terminates gracefully (or raises an error as per BigQuery SP design). This replaces the `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` logic.
*   **Setup:**
    1.  **Legacy & Migrated:**
        *   Ensure source tables are in a known state.
        *   Ensure target tables are empty.
        *   Ensure logging tables are empty.
*   **Action:**
    1.  **Legacy:** Execute the legacy script with an invalid `Stichtag` format:
        ```bash
        r_aurd_rechstan.ksh -s 2023-01-01
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure with an invalid `Stichtag` format:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('2023-01-01', 0);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:** The script should output an error message (e.g., "Invalid Stichtag format") and exit with a non-zero status. The log file should contain an error entry. The FOS target table should remain empty.
    *   **Migrated:** The `CALL` statement should raise an error and terminate. `project.dataset.job_log` should contain an `ERROR` level entry with a message indicating invalid `Stichtag` format and `status = 'PARAM_ERROR'`. `project.dataset.job_status` should show `overall_status = 'FAILED'`. The `project.dataset.target_table` should remain empty.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE log_level = 'ERROR' AND message LIKE '%Invalid Stichtag format%' AND status = 'PARAM_ERROR') > 0
                THEN 'PASS: Invalid Stichtag error logged'
                ELSE 'FAIL: Invalid Stichtag error not logged as expected'
            END AS error_log_check,
            CASE
                WHEN (SELECT overall_status FROM `project.dataset.job_status` WHERE job_id = 'AURD_RECHSTAN') = 'FAILED'
                THEN 'PASS: Job status updated to FAILED'
                ELSE 'FAIL: Job status not FAILED'
            END AS job_status_check,
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.target_table`) = 0
                THEN 'PASS: Target table remains empty'
                ELSE 'FAIL: Target table contains data after error'
            END AS target_table_empty_check;
        ```

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Empty Source Table Handling

*   **Purpose:** Verify that the job handles an empty source table gracefully, resulting in an empty target table and correct logging.
*   **Setup:**
    1.  **Legacy & Migrated:**
        *   Ensure source tables are completely empty.
        *   Ensure target tables are empty.
        *   Ensure logging tables are empty.
        *   Use a fixed `Stichtag` (e.g., '01012023').
*   **Action:**
    1.  **Legacy:** Execute the legacy script:
        ```bash
        r_aurd_rechstan.ksh -s 01012023
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('01012023', 0);
        ```
*   **Pass/Fail Criterion:**
    *   **Legacy:** The job should complete successfully, and the FOS target table should remain empty.
    *   **Migrated:** The job should complete successfully. `project.dataset.target_table` should remain empty. `project.dataset.job_log` should show successful completion. `project.dataset.job_status` should show `overall_status = 'OK'`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.target_table`) = 0
                THEN 'PASS: Target table is empty'
                ELSE 'FAIL: Target table contains data unexpectedly'
            END AS target_table_count_check,
            CASE
                WHEN (SELECT overall_status FROM `project.dataset.job_status` WHERE job_id = 'AURD_RECHSTAN') = 'OK'
                THEN 'PASS: Job status is OK'
                ELSE 'FAIL: Job status is not OK'
            END AS job_status_check,
            CASE
                WHEN (SELECT COUNT(*) FROM `project.dataset.job_log` WHERE status = 'COMPLETED') > 0
                THEN 'PASS: Job completed log entry exists'
                ELSE 'FAIL: Job completed log entry missing'
            END AS log_entry_check;
        ```

#### Test Case 4.2: Schema and Data Type Integrity

*   **Purpose:** Verify that the schema and data types of the `project.dataset.target_table` match the expected schema and data types of the legacy FOS target table, and that data is inserted without type conversion errors or loss of precision.
*   **Setup:**
    1.  **Legacy & Migrated:**
        *   Populate source tables with a representative set of data, including edge cases for data types (e.g., max length strings, min/max integer values, specific date formats).
        *   Ensure target tables are empty.
        *   Use a fixed `Stichtag` (e.g., '01012023').
*   **Action:**
    1.  **Legacy:** Execute the legacy script:
        ```bash
        r_aurd_rechstan.ksh -s 01012023
        ```
    2.  **Migrated:** Execute the main BigQuery stored procedure:
        ```sql
        CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('01012023', 0);
        ```
*   **Pass/Fail Criterion:**
    *   The schema (column names, data types, nullability) of `project.dataset.target_table` must match the schema of the legacy FOS target table.
    *   No data type conversion errors should occur during the BigQuery job execution (this would typically manifest as a job failure or log errors).
    *   The data inserted into `project.dataset.target_table` should retain its original precision and format as seen in the legacy FOS target table (verified by deep data comparison in Test Case 1.1).
    *   **SQL Assertion (BigQuery - for schema comparison):**
        ```sql
        -- This is a programmatic assertion using BigQuery's INFORMATION_SCHEMA.
        -- You would compare the output of this query for `project.dataset.target_table`
        -- against the schema definition of the legacy FOS table.
        SELECT column_name, data_type, is_nullable
        FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'target_table'
        ORDER BY ordinal_position;

        -- Example Python/pytest assertion (conceptual):
        # def test_schema_parity(bigquery_client, legacy_schema_definition):
        #     bq_schema = bigquery_client.get_table_schema('project.dataset.target_table')
        #     assert bq_schema == legacy_schema_definition, "Target table schema does not match legacy"
        ```