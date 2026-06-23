As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `k_ausd_v_ta_notice.ksh` migration to Google Cloud Platform. These tests aim to ensure the migrated BigQuery Stored Procedure and Airflow DAG are functionally equivalent to the legacy KornShell script, covering output parity, transformation correctness, external system replacements, and data quality assertions.

The tests assume the following BigQuery project and dataset structure, as indicated in the migration design:
*   **Project ID:** `your_project_id` (replace with actual GCP project ID)
*   **Dataset ID:** `isbert_reporting` (replace with actual BigQuery dataset ID)

All BigQuery SQL assertions should be run against these specific project and dataset IDs.

---

## Migration Validation Tests for `k_ausd_v_ta_notice.ksh`

### Test Case 1: Output Parity - End-to-End Data Comparison

*   **Purpose:** To verify that the migrated BigQuery job produces an identical final dataset in `ta_notice` as the legacy KornShell script, given the same initial source data. This is the most critical behavioral equivalence test.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Prepare a representative set of test data in the legacy Oracle `cds$ta_notice` table. This data should cover various scenarios for `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`, and NULL values.
        *   Execute the legacy `k_ausd_v_ta_notice.ksh` script with specific `p_JobKennung` and `p_EintragsNr` (e.g., `k_ausd_v_ta_notice.ksh -j LEGACY_TEST -f 20231026`).
        *   Extract the resulting data from the legacy `ta_notice` table into a canonical format (e.g., CSV, JSON) or a temporary comparison table in BigQuery.
    2.  **BigQuery Environment:**
        *   Ensure `your_project_id.isbert_reporting.cds_ta_notice` is populated with the *exact same* test data as the legacy Oracle `cds$ta_notice` table.
        *   Clear the `your_project_id.isbert_reporting.ta_notice`, `your_project_id.isbert_reporting.job_table`, and `your_project_id.isbert_reporting.error_log` tables.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with the *same* `job_kennung` (`LEGACY_TEST`) and `eintrags_nr` (`20231026`) parameters used in the legacy run.
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` completes successfully.
    2.  The `your_project_id.isbert_reporting.job_table` contains an entry for `job_kennung = 'LEGACY_TEST'` and `eintrags_nr = '20231026'` with `status = 'COMPLETED'`.
    3.  The `record_count` in the `job_table` entry matches the number of records processed by the legacy job.
    4.  A deep comparison of the data in `your_project_id.isbert_reporting.ta_notice` with the extracted legacy `ta_notice` data shows exact equivalence (same number of rows, same values for each column).

    ```python
    # Example pytest assertion for data parity (assuming data is loaded into pandas DataFrames)
    import pandas as pd
    from google.cloud import bigquery

    def test_output_parity(legacy_data_df: pd.DataFrame, bq_project_id: str, bq_dataset_id: str):
        client = bigquery.Client(project=bq_project_id)
        query = f"SELECT cntrct_id, valid_from, valid_to, entry_date_of_notice FROM `{bq_project_id}.{bq_dataset_id}.ta_notice` ORDER BY cntrct_id"
        bq_data_df = client.query(query).to_dataframe()

        # Ensure column types are consistent for comparison
        bq_data_df['valid_from'] = bq_data_df['valid_from'].dt.date
        bq_data_df['valid_to'] = bq_data_df['valid_to'].dt.date
        bq_data_df['entry_date_of_notice'] = bq_data_df['entry_date_of_notice'].dt.date

        legacy_data_df['valid_from'] = pd.to_datetime(legacy_data_df['valid_from']).dt.date
        legacy_data_df['valid_to'] = pd.to_datetime(legacy_data_df['valid_to']).dt.date
        legacy_data_df['entry_date_of_notice'] = pd.to_datetime(legacy_data_df['entry_date_of_notice']).dt.date

        # Sort both DataFrames for reliable comparison
        legacy_data_df = legacy_data_df.sort_values(by='cntrct_id').reset_index(drop=True)
        bq_data_df = bq_data_df.sort_values(by='cntrct_id').reset_index(drop=True)

        pd.testing.assert_frame_equal(legacy_data_df, bq_data_df, check_dtype=True)

        # Assert record count in job_table
        job_query = f"""
        SELECT record_count, status
        FROM `{bq_project_id}.{bq_dataset_id}.job_table`
        WHERE job_kennung = 'LEGACY_TEST' AND eintrags_nr = '20231026'
        """
        job_result = client.query(job_query).to_dataframe()
        assert not job_result.empty, "Job entry not found in job_table"
        assert job_result['status'].iloc[0] == 'COMPLETED', "Job status is not COMPLETED"
        assert job_result['record_count'].iloc[0] == len(legacy_data_df), "Record count mismatch"
    ```

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and exiting gracefully.
*   **Setup:**
    *   Clear `your_project_id.isbert_reporting.job_table` and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with `job_kennung` set to an empty string (`""`) and a valid `eintrags_nr` (e.g., `'20231026'`).
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` fails.
    2.  `your_project_id.isbert_reporting.error_log` contains exactly one entry with:
        *   `error_number = 1`
        *   `error_argument = 'Jobkennung'`
        *   `procedure_name = 'r_ausd_v_ta_notice'`
        *   `error_message` containing "Missing required input parameter."
    3.  No new entries are created in `your_project_id.isbert_reporting.job_table`.

    ```sql
    -- SQL Assertion for error_log
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.error_log`
    WHERE error_number = 1
      AND error_argument = 'Jobkennung'
      AND procedure_name = 'r_ausd_v_ta_notice'
      AND error_message LIKE '%Missing required input parameter%';
    -- Expected: 1

    -- SQL Assertion for job_table (should be empty or no new entries)
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.job_table`
    WHERE job_kennung = '' AND eintrags_nr = '20231026';
    -- Expected: 0
    ```

### Test Case 3: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly identifies and handles a missing `p_EintragsNr` parameter, logging an error and exiting gracefully.
*   **Setup:**
    *   Clear `your_project_id.isbert_reporting.job_table` and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with a valid `job_kennung` (e.g., `'TEST_JOB_MISSING_EINTRAG'`) and `eintrags_nr` set to an empty string (`""`).
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` fails.
    2.  `your_project_id.isbert_reporting.error_log` contains exactly one entry with:
        *   `error_number = 1`
        *   `error_argument = 'EintragsNr'`
        *   `procedure_name = 'r_ausd_v_ta_notice'`
        *   `error_message` containing "Missing required input parameter."
    3.  No new entries are created in `your_project_id.isbert_reporting.job_table`.

    ```sql
    -- SQL Assertion for error_log
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.error_log`
    WHERE error_number = 1
      AND error_argument = 'EintragsNr'
      AND procedure_name = 'r_ausd_v_ta_notice'
      AND error_message LIKE '%Missing required input parameter%';
    -- Expected: 1

    -- SQL Assertion for job_table (should be empty or no new entries)
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.job_table`
    WHERE job_kennung = 'TEST_JOB_MISSING_EINTRAG' AND eintrags_nr = '';
    -- Expected: 0
    ```

### Test Case 4: Parameter Validation - Invalid `p_EintragsNr` Format

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly handles an `p_EintragsNr` that is not in the expected `YYYYMMDD` format, logging a specific error and exiting.
*   **Setup:**
    *   Clear `your_project_id.isbert_reporting.job_table` and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with a valid `job_kennung` (e.g., `'TEST_JOB_INVALID_DATE'`) and an invalid `eintrags_nr` (e.g., `'2023-10-26'` or `'ABCDEFGH'`).
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` fails.
    2.  `your_project_id.isbert_reporting.error_log` contains exactly one entry with:
        *   `error_number = 2` (custom error for invalid date format)
        *   `error_argument = 'p_EintragsNr (invalid date format)'`
        *   `procedure_name = 'r_ausd_v_ta_notice'`
        *   `error_message` containing "Invalid date format for p_EintragsNr. Expected YYYYMMDD."
    3.  No new entries are created in `your_project_id.isbert_reporting.job_table`.

    ```sql
    -- SQL Assertion for error_log
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.error_log`
    WHERE error_number = 2
      AND error_argument = 'p_EintragsNr (invalid date format)'
      AND procedure_name = 'r_ausd_v_ta_notice'
      AND error_message LIKE '%Invalid date format for p_EintragsNr. Expected YYYYMMDD%';
    -- Expected: 1

    -- SQL Assertion for job_table (should be empty or no new entries)
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.job_table`
    WHERE job_kennung = 'TEST_JOB_INVALID_DATE';
    -- Expected: 0
    ```

### Test Case 5: Transformation Correctness - Date Filtering and NULL Handling

*   **Purpose:** To verify the complex `WHERE` clause logic, including `insert_at`, `modified_at`, `valid_to` conditions, `is_production` filter, and correct handling of `NULL` values.
*   **Setup:**
    *   Populate `your_project_id.isbert_reporting.cds_ta_notice` with the following test data (using `p_EintragsNr = '20231026'` for the run):

        | cntrct_id | valid_from (TIMESTAMP) | valid_to (TIMESTAMP) | entry_date_of_notice (TIMESTAMP) | insert_at (TIMESTAMP) | modified_at (TIMESTAMP) | is_production | Expected in `ta_notice` |
        | :-------- | :--------------------- | :------------------- | :------------------------------- | :-------------------- | :---------------------- | :------------ | :---------------------- |
        | C001      | 2023-01-01 00:00:00    | 2024-01-01 00:00:00  | 2023-01-01 00:00:00              | 2023-10-25 10:00:00   | NULL                    | 1             | YES                     |
        | C002      | 2023-01-01 00:00:00    | 2024-01-01 00:00:00  | 2023-01-01 00:00:00              | 2023-10-26 00:00:00   | NULL                    | 1             | YES                     |
        | C003      | 2023-01-01 00:00:00    | 2024-01-01 00:00:00  | 2023-01-01 00:00:00              | 2023-10-27 10:00:00   | NULL                    | 1             | NO (`insert_at > v_datum_date`) |
        | C004      | 2023-01-01 00:00:00    | 2024-01-01 00:00:00  | 2023-01-01 00:00:00              | 2023-10-25 10:00:00   | 2023-10-27 10:00:00     | 1             | YES (`modified_at > v_datum_date`) |
        | C005      | 2023-01-01 00:00:00    | 2024-01-01 00:00:00  | 2023-01-01 00:00:00              | 2023-10-25 10:00:00   | 2023-10-26 00:00:00     | 1             | NO (`modified_at <= v_datum_date`) |
        | C006      | 2023-01-01 00:00:00    | NULL                 | 2023-01-01 00:00:00              | 2023-10-25 10:00:00   | NULL                    | 1             | YES (`valid_to IS NULL`) |
        | C007      | 2023-01-01 00:00:00    | 2023-10-25 00:00:00  | 2023-01-01 00:00:00              | 2023-10-25 10:00:00   | NULL                    | 1             | NO (`valid_to <= v_datum_date`) |
        | C008      | 2023-01-01 00:00:00    | 2024-01-01 00:00:00  | 2023-01-01 00:00:00              | 2023-10-25 10:00:00   | NULL                    | 0             | NO (`is_production = 0`) |
        | C009      | 2023-01-01 00:00:00    | 2024-01-01 00:00:00  | 2023-01-01 00:00:00              | 2023-10-25 10:00:00   | 2023-10-26 12:00:00     | 0             | NO (`is_production = 0`) |
    *   Clear `your_project_id.isbert_reporting.ta_notice`, `your_project_id.isbert_reporting.job_table`, and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with `job_kennung = 'DATE_FILTER_TEST'` and `eintrags_nr = '20231026'`.
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` completes successfully.
    2.  The `your_project_id.isbert_reporting.job_table` entry for this run has `status = 'COMPLETED'` and `record_count = 4`.
    3.  `your_project_id.isbert_reporting.ta_notice` contains exactly 4 records, specifically those with `cntrct_id` C001, C002, C004, and C006. No other records should be present.

    ```sql
    -- SQL Assertion for record count in ta_notice
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.ta_notice`;
    -- Expected: 4

    -- SQL Assertion for specific cntrct_ids in ta_notice
    SELECT ARRAY_AGG(cntrct_id ORDER BY cntrct_id) FROM `your_project_id.isbert_reporting.ta_notice`;
    -- Expected: ['C001', 'C002', 'C004', 'C006']

    -- SQL Assertion for job_table record_count
    SELECT record_count FROM `your_project_id.isbert_reporting.job_table`
    WHERE job_kennung = 'DATE_FILTER_TEST' AND eintrags_nr = '20231026';
    -- Expected: 4
    ```

### Test Case 6: Transformation Correctness - Type Casting

*   **Purpose:** To verify that `TIMESTAMP` columns from `cds_ta_notice` are correctly cast to `DATE` in `ta_notice`, truncating any time components.
*   **Setup:**
    *   Populate `your_project_id.isbert_reporting.cds_ta_notice` with a record where `valid_from`, `valid_to`, and `entry_date_of_notice` have non-midnight time components:

        | cntrct_id | valid_from (TIMESTAMP) | valid_to (TIMESTAMP) | entry_date_of_notice (TIMESTAMP) | insert_at (TIMESTAMP) | modified_at (TIMESTAMP) | is_production |
        | :-------- | :--------------------- | :------------------- | :------------------------------- | :-------------------- | :---------------------- | :------------ |
        | C010      | 2023-01-15 14:30:00    | 2024-02-20 08:00:00  | 2023-03-10 23:59:59              | 2023-10-25 10:00:00   | NULL                    | 1             |
    *   Clear `your_project_id.isbert_reporting.ta_notice`, `your_project_id.isbert_reporting.job_table`, and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with `job_kennung = 'TYPE_CAST_TEST'` and `eintrags_nr = '20231026'`.
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` completes successfully.
    2.  `your_project_id.isbert_reporting.ta_notice` contains one record for `cntrct_id = 'C010'`.
    3.  For this record, the `valid_from`, `valid_to`, and `entry_date_of_notice` columns are of `DATE` type and have the following values:
        *   `valid_from = '2023-01-15'`
        *   `valid_to = '2024-02-20'`
        *   `entry_date_of_notice = '2023-03-10'`

    ```sql
    -- SQL Assertion for type casting
    SELECT
        cntrct_id,
        valid_from,
        valid_to,
        entry_date_of_notice
    FROM `your_project_id.isbert_reporting.ta_notice`
    WHERE cntrct_id = 'C010';
    -- Expected:
    -- cntrct_id: 'C010'
    -- valid_from: DATE '2023-01-15'
    -- valid_to: DATE '2024-02-20'
    -- entry_date_of_notice: DATE '2023-03-10'
    ```

### Test Case 7: Job State Management - Successful Run

*   **Purpose:** To verify that the `job_table` is correctly updated with `status = 'COMPLETED'` and the accurate `record_count` upon successful execution.
*   **Setup:**
    *   Populate `your_project_id.isbert_reporting.cds_ta_notice` with at least one record that will be processed (e.g., `cntrct_id = 'C001'` from Test Case 5 setup).
    *   Clear `your_project_id.isbert_reporting.job_table` and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with `job_kennung = 'SUCCESS_TEST'` and `eintrags_nr = '20231026'`.
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` completes successfully.
    2.  `your_project_id.isbert_reporting.job_table` contains exactly one entry for `job_kennung = 'SUCCESS_TEST'` and `eintrags_nr = '20231026'`.
    3.  This entry has:
        *   `status = 'COMPLETED'`
        *   `tab_name = 'ta_notice'`
        *   `record_count` matching the actual number of rows inserted into `your_project_id.isbert_reporting.ta_notice`.
        *   `created_at` and `updated_at` are recent timestamps.
        *   `error_message` is `NULL`.
    4.  `your_project_id.isbert_reporting.error_log` contains no new entries.

    ```sql
    -- SQL Assertion for job_table status and record_count
    SELECT status, record_count, error_message
    FROM `your_project_id.isbert_reporting.job_table`
    WHERE job_kennung = 'SUCCESS_TEST' AND eintrags_nr = '20231026';
    -- Expected: status = 'COMPLETED', record_count > 0, error_message IS NULL

    -- SQL Assertion for error_log (should be empty)
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.error_log`
    WHERE procedure_name = 'r_ausd_v_ta_notice' AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);
    -- Expected: 0
    ```

### Test Case 8: Job State Management - Failed Run (Simulated Internal Error)

*   **Purpose:** To verify that the `job_table` is correctly updated with `status = 'FAILED'` and an `error_message`, and that a detailed error is logged in `error_log` when an internal error occurs within the stored procedure.
*   **Setup:**
    1.  **Temporary Stored Procedure Modification:** Temporarily modify the `your_project_id.isbert_reporting.r_ausd_v_ta_notice` stored procedure to introduce an intentional error within its main `BEGIN...EXCEPTION` block, for example, by adding `SELECT 1/0;` or `RAISE 'Simulated internal processing error';` right before the `INSERT` statement.
    2.  Populate `your_project_id.isbert_reporting.cds_ta_notice` with some data.
    3.  Clear `your_project_id.isbert_reporting.job_table` and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with `job_kennung = 'FAIL_TEST'` and `eintrags_nr = '20231026'`.
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` fails.
    2.  `your_project_id.isbert_reporting.job_table` contains exactly one entry for `job_kennung = 'FAIL_TEST'` and `eintrags_nr = '20231026'`.
    3.  This entry has:
        *   `status = 'FAILED'`
        *   `updated_at` is a recent timestamp.
        *   `error_message` is populated with details of the simulated error (e.g., "Division by zero" or "Simulated internal processing error").
    4.  `your_project_id.isbert_reporting.error_log` contains exactly one entry with:
        *   `procedure_name = 'r_ausd_v_ta_notice'`
        *   `error_message` matching the simulated error.
*   **Cleanup:** Revert the temporary modification to the `your_project_id.isbert_reporting.r_ausd_v_ta_notice` stored procedure immediately after this test.

    ```sql
    -- SQL Assertion for job_table status and error_message
    SELECT status, error_message
    FROM `your_project_id.isbert_reporting.job_table`
    WHERE job_kennung = 'FAIL_TEST' AND eintrags_nr = '20231026';
    -- Expected: status = 'FAILED', error_message LIKE '%Simulated internal processing error%' (or 'Division by zero')

    -- SQL Assertion for error_log entry
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.error_log`
    WHERE procedure_name = 'r_ausd_v_ta_notice'
      AND error_message LIKE '%Simulated internal processing error%' -- or 'Division by zero'
      AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);
    -- Expected: 1
    ```

### Test Case 9: Data Quality - Empty Source Table

*   **Purpose:** To verify that the job handles an empty source table (`cds_ta_notice`) gracefully, resulting in an empty target table and a `record_count` of 0 in the `job_table`.
*   **Setup:**
    *   Ensure `your_project_id.isbert_reporting.cds_ta_notice` is completely empty.
    *   Clear `your_project_id.isbert_reporting.ta_notice`, `your_project_id.isbert_reporting.job_table`, and `your_project_id.isbert_reporting.error_log`.
*   **Action:**
    *   Trigger the Airflow DAG `k_ausd_v_ta_notice_dag` with `job_kennung = 'EMPTY_SOURCE_TEST'` and `eintrags_nr = '20231026'`.
*   **Pass/Fail Criterion:**
    1.  The Airflow task `call_r_ausd_v_ta_notice_sp` completes successfully.
    2.  `your_project_id.isbert_reporting.ta_notice` is empty (contains 0 rows).
    3.  `your_project_id.isbert_reporting.job_table` contains one entry for `job_kennung = 'EMPTY_SOURCE_TEST'` and `eintrags_nr = '20231026'` with:
        *   `status = 'COMPLETED'`
        *   `record_count = 0`
        *   `error_message` is `NULL`.
    4.  `your_project_id.isbert_reporting.error_log` contains no new entries.

    ```sql
    -- SQL Assertion for ta_notice
    SELECT COUNT(*) FROM `your_project_id.isbert_reporting.ta_notice`;
    -- Expected: 0

    -- SQL Assertion for job_table
    SELECT status, record_count, error_message
    FROM `your_project_id.isbert_reporting.job_table`
    WHERE job_kennung = 'EMPTY_SOURCE_TEST' AND eintrags_nr = '20231026';
    -- Expected: status = 'COMPLETED', record_count = 0, error_message IS NULL
    ```

### Test Case 10: Schema Assertions

*   **Purpose:** To verify that the schemas of the target BigQuery tables (`ta_notice`, `job_table`, `error_log`, `cds_ta_notice`) match the defined DDL and expected types.
*   **Setup:**
    *   Ensure the DDL scripts (`sql/ddl/isbert_reporting_tables.sql`) have been executed in the target BigQuery environment.
*   **Action:**
    *   Query BigQuery's `INFORMATION_SCHEMA` for the table schemas.
*   **Pass/Fail Criterion:**
    1.  **`your_project_id.isbert_reporting.ta_notice`:**
        *   `cntrct_id` is `STRING`
        *   `valid_from` is `DATE`
        *   `valid_to` is `DATE`
        *   `entry_date_of_notice` is `DATE`
    2.  **`your_project_id.isbert_reporting.job_table`:**
        *   `job_kennung` is `STRING` and `NOT NULL`
        *   `eintrags_nr` is `STRING` and `NOT NULL`
        *   `tab_name` is `STRING`
        *   `status` is `STRING` and `NOT NULL`
        *   `record_count` is `INT64`
        *   `created_at` is `TIMESTAMP` and `NOT NULL`
        *   `updated_at` is `TIMESTAMP`
        *   `error_message` is `STRING`
    3.  **`your_project_id.isbert_reporting.error_log`:**
        *   `error_number` is `INT64`
        *   `error_argument` is `STRING`
        *   `procedure_name` is `STRING` and `NOT NULL`
        *   `created_at` is `TIMESTAMP` and `NOT NULL`
        *   `error_message` is `STRING`
    4.  **`your_project_id.isbert_reporting.cds_ta_notice`:**
        *   `cntrct_id` is `STRING`
        *   `valid_from` is `TIMESTAMP`
        *   `valid_to` is `TIMESTAMP`
        *   `entry_date_of_notice` is `TIMESTAMP`
        *   `insert_at` is `TIMESTAMP`
        *   `modified_at` is `TIMESTAMP`
        *   `is_production` is `INT64`

    ```sql
    -- SQL Assertion for ta_notice schema
    SELECT column_name, data_type, is_nullable
    FROM `your_project_id.isbert_reporting.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ta_notice'
    ORDER BY ordinal_position;
    /* Expected Output:
    column_name         data_type   is_nullable
    cntrct_id           STRING      YES
    valid_from          DATE        YES
    valid_to            DATE        YES
    entry_date_of_notice DATE       YES
    */

    -- SQL Assertion for job_table schema
    SELECT column_name, data_type, is_nullable
    FROM `your_project_id.isbert_reporting.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_table'
    ORDER BY ordinal_position;
    /* Expected Output:
    column_name         data_type   is_nullable
    job_kennung         STRING      NO
    eintrags_nr         STRING      NO
    tab_name            STRING      YES
    status              STRING      NO
    record_count        INT64       YES
    created_at          TIMESTAMP   NO
    updated_at          TIMESTAMP   YES
    error_message       STRING      YES
    */

    -- Similar queries for error_log and cds_ta_notice
    ```