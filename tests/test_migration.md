The migration of `r_ausd_v_ta_notice.ksh` to BigQuery involves transforming shell scripts into BigQuery Stored Procedures and Oracle SQL*Plus into BigQuery SQL. The following test cases are designed to validate this migration, ensuring behavioral equivalence and correctness across various aspects.

---

## Test Case 1: End-to-End Output Parity - Happy Path

*   **Purpose**: Verify that the migrated BigQuery job produces the exact same final data in `my_project.my_dataset.sof_ta_notice` as the legacy KornShell job produces in `sof$ta_notice` when run with typical, valid input data. This is the primary test for output parity and overall behavioral equivalence.
*   **Setup**:
    1.  **Legacy Environment**:
        *   Populate the Oracle source table `cds$ta_notice@pcrs1` with a representative dataset (e.g., 100-1000 rows) including various `insert_at`, `modified_at`, `valid_to` dates, and `is_production` flags (both 0 and 1).
        *   Populate the Oracle metadata table `isbert_schema.dwtk_meldungen` with a `job_kennung = 'BERT_DROP_TEMP_TABLE'` entry, setting `timecreated` to a date that will filter some records from `cds$ta_notice`.
        *   Ensure the target Oracle table `sof$ta_notice` is empty.
    2.  **BigQuery Environment**:
        *   Create and populate `my_project.my_dataset.cds_ta_notice` with *identical* data as `cds$ta_notice@pcrs1`.
        *   Create and populate `my_project.my_dataset.dwtk_meldungen` with *identical* data as `isbert_schema.dwtk_meldungen`.
        *   Ensure `my_project.my_dataset.sof_ta_notice`, `my_project.my_dataset.job_log`, `my_project.my_dataset.job_control`, and `my_project.my_dataset.error_log` are empty.
*   **Action**:
    1.  Execute the legacy job: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh`
    2.  Execute the migrated BigQuery job: `CALL my_project.my_dataset.sp_r_ausd_v_ta_notice('TEST_JOB_HAPPY_PATH');`
*   **Pass/Fail Criterion**:
    *   The row count in legacy `sof$ta_notice` must be identical to the row count in BigQuery `my_project.my_dataset.sof_ta_notice`.
    *   A full data comparison (e.g., checksum, hash, or row-by-row comparison) of `sof$ta_notice` (legacy) and `my_project.my_dataset.sof_ta_notice` (BigQuery) must show no differences.
    *   The `status` in `my_project.my_dataset.job_control` for `TEST_JOB_HAPPY_PATH` must be 'SUCCESS'.
    *   The `record_count` in `my_project.my_dataset.job_control` must match the actual number of rows inserted.

```sql
-- BigQuery comparison query (assuming legacy_oracle_replica is a BigQuery external table or replica)
-- Step 1: Compare row counts
SELECT
    (SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_notice`) AS bq_row_count,
    (SELECT COUNT(*) FROM `legacy_oracle_replica.sof_ta_notice`) AS legacy_row_count;
-- Expected: bq_row_count = legacy_row_count

-- Step 2: Perform a detailed row-by-row comparison
SELECT 'Mismatch found in BigQuery but not in Legacy' AS mismatch_type, t1.*
FROM `my_project.my_dataset.sof_ta_notice` t1
EXCEPT DISTINCT
SELECT 'Mismatch found in BigQuery but not in Legacy' AS mismatch_type, t2.*
FROM `legacy_oracle_replica.sof_ta_notice` t2;

SELECT 'Mismatch found in Legacy but not in BigQuery' AS mismatch_type, t1.*
FROM `legacy_oracle_replica.sof_ta_notice` t1
EXCEPT DISTINCT
SELECT 'Mismatch found in Legacy but not in BigQuery' AS mismatch_type, t2.*
FROM `my_project.my_dataset.sof_ta_notice` t2;
-- Expected result for both queries: 0 rows (no mismatches)

-- Step 3: Check job control status and record count
SELECT status, record_count
FROM `my_project.my_dataset.job_control`
WHERE job_kennung = 'TEST_JOB_HAPPY_PATH'
ORDER BY start_timestamp DESC
LIMIT 1;
-- Expected result: status = 'SUCCESS', record_count = [expected_count_from_step_1]
```

---

## Test Case 2: Transformation Correctness - Date Filtering and NULL Handling

*   **Purpose**: Validate the `WHERE` clause logic in `sp_d_ausd_v_ta_notice_sql`, specifically how `insert_at`, `modified_at`, `valid_to`, and `is_production` filters interact, including NULL values, to ensure transformation correctness.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Populate `my_project.my_dataset.cds_ta_notice` with a diverse set of records, including:
            *   Rows where `insert_at <= v_datum`, `modified_at IS NULL`, `valid_to IS NULL`, `is_production = 1` (should be included).
            *   Rows where `insert_at <= v_datum`, `modified_at > v_datum`, `valid_to IS NULL`, `is_production = 1` (should be included).
            *   Rows where `insert_at <= v_datum`, `modified_at IS NULL`, `valid_to > v_datum`, `is_production = 1` (should be included).
            *   Rows where `insert_at <= v_datum`, `modified_at <= v_datum`, `valid_to IS NULL`, `is_production = 1` (should be *excluded* due to `modified_at`).
            *   Rows where `insert_at <= v_datum`, `modified_at IS NULL`, `valid_to <= v_datum`, `is_production = 1` (should be *excluded* due to `valid_to`).
            *   Rows where `is_production = 0` (should be *excluded*).
            *   Rows with `insert_at > v_datum` (should be *excluded*).
        *   Populate `my_project.my_dataset.dwtk_meldungen` to set `v_datum` to a specific date (e.g., '2023-01-15').
        *   Ensure `my_project.my_dataset.sof_ta_notice` is empty.
*   **Action**:
    1.  Execute the core data processing procedure directly: `CALL my_project.my_dataset.sp_d_ausd_v_ta_notice_sql('TEST_JOB_FILTER', 12345, @records_inserted);`
*   **Pass/Fail Criterion**:
    *   The number of records inserted into `my_project.my_dataset.sof_ta_notice` must exactly match the expected count based on the filtering logic and the setup data.
    *   Verify the content of `my_project.my_dataset.sof_ta_notice` to ensure only the expected rows are present and their values are correct.

```sql
-- Example setup for cds_ta_notice (v_datum will be '2023-01-15')
TRUNCATE TABLE `my_project.my_dataset.cds_ta_notice`;
TRUNCATE TABLE `my_project.my_dataset.dwtk_meldungen`;
TRUNCATE TABLE `my_project.my_dataset.sof_ta_notice`;

INSERT INTO `my_project.my_dataset.cds_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES
('C001', '2022-01-01', NULL, '2022-01-01', '2023-01-10', NULL, 1),             -- Included
('C002', '2022-01-01', NULL, '2022-01-01', '2023-01-10', '2023-01-20', 1),     -- Included
('C003', '2022-01-01', '2023-01-20', '2022-01-01', '2023-01-10', NULL, 1),     -- Included
('C004', '2022-01-01', NULL, '2022-01-01', '2023-01-10', '2023-01-10', 1),     -- Excluded (modified_at <= v_datum)
('C005', '2022-01-01', '2023-01-10', '2022-01-01', '2023-01-10', NULL, 1),     -- Excluded (valid_to <= v_datum)
('C006', '2022-01-01', NULL, '2022-01-01', '2023-01-10', NULL, 0),             -- Excluded (is_production = 0)
('C007', '2022-01-01', NULL, '2022-01-01', '2023-01-20', NULL, 1);             -- Excluded (insert_at > v_datum)

INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00');

-- Execute the procedure
CALL `my_project.my_dataset.sp_d_ausd_v_ta_notice_sql`('TEST_JOB_FILTER', 12345, @records_inserted);

-- After running the procedure, check the results:
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_notice`;
-- Expected result: 3

SELECT cntrct_id FROM `my_project.my_dataset.sof_ta_notice` ORDER BY cntrct_id;
-- Expected result: C001, C002, C003
```

---

## Test Case 3: External System Replacement - `dwtk_meldungen` `v_datum` Calculation

*   **Purpose**: Verify that the `v_datum` calculation from `dwtk_meldungen` in `sp_d_ausd_v_ta_notice_sql` correctly replicates the Oracle `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` logic, covering the external system replacement aspect.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   `my_project.my_dataset.cds_ta_notice` can be empty or contain dummy data, as the focus is on `v_datum` calculation.
        *   `my_project.my_dataset.sof_ta_notice` should be empty.
        *   Ensure logging tables are clear.
*   **Action**:
    1.  For each scenario (multiple entries, no entries, single entry for `BERT_DROP_TEMP_TABLE`), populate `my_project.my_dataset.dwtk_meldungen` accordingly.
    2.  Execute the core data processing procedure: `CALL my_project.my_dataset.sp_d_ausd_v_ta_notice_sql('TEST_JOB_VDATUM_SCENARIO_X', [unique_entry_nr], @records_inserted);`
    3.  Inspect the `job_log` table for the `v_datum` value logged by `sp_d_ausd_v_ta_notice_sql`.
*   **Pass/Fail Criterion**:
    *   The `v_datum` logged in `my_project.my_dataset.job_log` (e.g., `CONCAT('Determined v_datum: ', FORMAT_DATE('%Y-%m-%d', v_datum))`) must match the expected `MAX(timecreated)` (or '1900-01-01' if no entries) for each scenario.

```sql
-- Scenario 1: Multiple entries, find max
TRUNCATE TABLE `my_project.my_dataset.dwtk_meldungen`;
INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', '2023-01-01 10:00:00'),
('BERT_DROP_TEMP_TABLE', '2023-01-15 12:30:00'),
('OTHER_JOB', '2023-01-20 08:00:00'),
('BERT_DROP_TEMP_TABLE', '2023-01-10 15:00:00');
CALL `my_project.my_dataset.sp_d_ausd_v_ta_notice_sql`('TEST_JOB_VDATUM_MULTI', 12346, @records_inserted);
SELECT log_message FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_VDATUM_MULTI' AND log_message LIKE 'Determined v_datum:%' ORDER BY log_timestamp DESC LIMIT 1;
-- Expected: 'Determined v_datum: 2023-01-15'

-- Scenario 2: No entries for job_kennung 'BERT_DROP_TEMP_TABLE'
TRUNCATE TABLE `my_project.my_dataset.dwtk_meldungen`;
INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('OTHER_JOB', '2023-01-01 10:00:00');
CALL `my_project.my_dataset.sp_d_ausd_v_ta_notice_sql`('TEST_JOB_VDATUM_NONE', 12347, @records_inserted);
SELECT log_message FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_VDATUM_NONE' AND log_message LIKE 'Determined v_datum:%' ORDER BY log_timestamp DESC LIMIT 1;
-- Expected: 'Determined v_datum: 1900-01-01'

-- Scenario 3: Single entry for job_kennung 'BERT_DROP_TEMP_TABLE'
TRUNCATE TABLE `my_project.my_dataset.dwtk_meldungen`;
INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', '2024-03-01 09:00:00');
CALL `my_project.my_dataset.sp_d_ausd_v_ta_notice_sql`('TEST_JOB_VDATUM_SINGLE', 12348, @records_inserted);
SELECT log_message FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_VDATUM_SINGLE' AND log_message LIKE 'Determined v_datum:%' ORDER BY log_timestamp DESC LIMIT 1;
-- Expected: 'Determined v_datum: 2024-03-01'
```

---

## Test Case 4: Error Handling and Logging

*   **Purpose**: Verify that the BigQuery job correctly handles errors, logs them to the appropriate tables (`job_log`, `error_log`, `job_control`), and sets the job status to 'FAILED', ensuring robust error handling.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Ensure `my_project.my_dataset.job_log`, `job_control`, `error_log` are empty.
        *   Create `my_project.my_dataset.cds_ta_notice` and `my_project.my_dataset.dwtk_meldungen` with valid data.
        *   Intentionally modify the schema of `my_project.my_dataset.sof_ta_notice` to cause a data type mismatch during insertion (e.g., change `cntrct_id` from `STRING` to `INT64`).
*   **Action**:
    1.  Execute the top-level migrated job: `CALL my_project.my_dataset.sp_r_ausd_v_ta_notice('TEST_JOB_ERROR');`
*   **Pass/Fail Criterion**:
    *   The `status` in `my_project.my_dataset.job_control` for `TEST_JOB_ERROR` must be 'FAILED'.
    *   An entry must exist in `my_project.my_dataset.error_log` for `TEST_JOB_ERROR` with a relevant error message and stack trace.
    *   `my_project.my_dataset.job_log` must contain messages indicating the job started, encountered an error, and failed.
    *   The `message` field in `job_control` should contain the error message.

```sql
-- Setup to induce an error (e.g., type mismatch for cntrct_id)
DROP TABLE IF EXISTS `my_project.my_dataset.sof_ta_notice`;
CREATE TABLE `my_project.my_dataset.sof_ta_notice` (
    cntrct_id INT64, -- INTENTIONALLY WRONG TYPE (should be STRING)
    valid_from DATE,
    valid_to DATE,
    entry_date_of_notice DATE
);

TRUNCATE TABLE `my_project.my_dataset.cds_ta_notice`;
TRUNCATE TABLE `my_project.my_dataset.dwtk_meldungen`;
TRUNCATE TABLE `my_project.my_dataset.job_control`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.error_log`;

-- Populate source data that would normally pass
INSERT INTO `my_project.my_dataset.cds_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES
('C001', '2022-01-01', NULL, '2022-01-01', '2023-01-10', NULL, 1);
INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00');

-- Execute the job
CALL `my_project.my_dataset.sp_r_ausd_v_ta_notice`('TEST_JOB_ERROR');

-- Check results
SELECT status, message FROM `my_project.my_dataset.job_control` WHERE job_kennung = 'TEST_JOB_ERROR' ORDER BY start_timestamp DESC LIMIT 1;
-- Expected: status = 'FAILED', message contains error details like "Cannot write STRING value 'C001' to INT64 field cntrct_id"

SELECT error_message, script_name FROM `my_project.my_dataset.error_log` WHERE job_kennung = 'TEST_JOB_ERROR' ORDER BY error_timestamp DESC LIMIT 1;
-- Expected: error_message indicating type mismatch, script_name = 'sp_d_ausd_v_ta_notice_sql'

SELECT log_message, log_level FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_ERROR' ORDER BY log_timestamp;
-- Expected: Sequence of INFO messages, then ERROR messages related to the failure.
```

---

## Test Case 5: Empty Source Table Handling

*   **Purpose**: Verify the job behaves correctly when the source table (`my_project.my_dataset.cds_ta_notice`) is empty. It should truncate the target and insert zero rows, completing successfully. This covers data quality and row-count assertions.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Ensure `my_project.my_dataset.cds_ta_notice` is empty.
        *   Populate `my_project.my_dataset.dwtk_meldungen` with a valid `timecreated` entry.
        *   `my_project.my_dataset.sof_ta_notice` can contain existing data (to test truncation).
        *   Ensure logging tables are clear.
*   **Action**:
    1.  Execute the top-level migrated job: `CALL my_project.my_dataset.sp_r_ausd_v_ta_notice('TEST_JOB_EMPTY_SOURCE');`
*   **Pass/Fail Criterion**:
    *   `my_project.my_dataset.sof_ta_notice` must be empty after the job runs.
    *   The `status` in `my_project.my_dataset.job_control` for `TEST_JOB_EMPTY_SOURCE` must be 'SUCCESS'.
    *   The `record_count` in `my_project.my_dataset.job_control` must be 0.
    *   `my_project.my_dataset.job_log` should show messages indicating truncation and 0 records inserted.

```sql
-- Setup
TRUNCATE TABLE `my_project.my_dataset.cds_ta_notice`; -- Ensure empty source
TRUNCATE TABLE `my_project.my_dataset.dwtk_meldungen`;
TRUNCATE TABLE `my_project.my_dataset.sof_ta_notice`;
TRUNCATE TABLE `my_project.my_dataset.job_control`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;

INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00');
-- Optionally, pre-populate sof_ta_notice to confirm truncation
INSERT INTO `my_project.my_dataset.sof_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice) VALUES
('OLD_C001', '2021-01-01', NULL, '2021-01-01');

-- Execute
CALL `my_project.my_dataset.sp_r_ausd_v_ta_notice`('TEST_JOB_EMPTY_SOURCE');

-- Check results
SELECT COUNT(*) FROM `my_project.my_dataset.sof_ta_notice`;
-- Expected: 0

SELECT status, record_count FROM `my_project.my_dataset.job_control` WHERE job_kennung = 'TEST_JOB_EMPTY_SOURCE' ORDER BY start_timestamp DESC LIMIT 1;
-- Expected: status = 'SUCCESS', record_count = 0

SELECT log_message FROM `my_project.my_dataset.job_log` WHERE job_kennung = 'TEST_JOB_EMPTY_SOURCE' AND log_message LIKE '%Inserted 0 records%';
-- Expected: One row with message 'Inserted 0 records into `my_project.my_dataset.sof_ta_notice`.'
```

---

## Test Case 6: Schema and Data Type Integrity

*   **Purpose**: Assert that the target table `my_project.my_dataset.sof_ta_notice` has the correct schema and that data types are preserved or correctly transformed during the migration, covering data quality and schema assertions.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Ensure `my_project.my_dataset.cds_ta_notice` and `my_project.my_dataset.dwtk_meldungen` are populated with data covering all possible data types and edge cases (e.g., max length strings, min/max dates, NULLs).
        *   `my_project.my_dataset.sof_ta_notice` should be empty.
*   **Action**:
    1.  Execute the top-level migrated job: `CALL my_project.my_dataset.sp_r_ausd_v_ta_notice('TEST_JOB_SCHEMA_CHECK');`
    2.  After the job completes, use BigQuery's information schema or client libraries to inspect the schema of `my_project.my_dataset.sof_ta_notice`.
*   **Pass/Fail Criterion**:
    *   The schema of `my_project.my_dataset.sof_ta_notice` must match the expected schema:
        *   `cntrct_id` STRING
        *   `valid_from` DATE
        *   `valid_to` DATE
        *   `entry_date_of_notice` DATE
    *   After insertion, query `my_project.my_dataset.sof_ta_notice` and verify that data types are correct and values are not truncated or malformed (e.g., dates are valid, strings are not cut off).

```python
# pytest example for schema assertion (requires google-cloud-bigquery library)
import pytest
from google.cloud import bigquery
import datetime

def test_sof_ta_notice_schema_and_data_types():
    client = bigquery.Client()
    table_id = "my_project.my_dataset.sof_ta_notice"

    # 1. Assert schema structure
    table = client.get_table(table_id)
    actual_schema = {field.name: field.field_type for field in table.schema}
    expected_schema = {
        "cntrct_id": "STRING",
        "valid_from": "DATE",
        "valid_to": "DATE",
        "entry_date_of_notice": "DATE",
    }
    assert actual_schema == expected_schema, f"Schema mismatch for {table_id}"

    # 2. Assert data types and values after job execution (assuming job was run and data inserted)
    # Setup for job execution would be done before this test function, e.g., in a fixture
    # For this example, we'll assume some data is present.
    query = """
    SELECT
        cntrct_id,
        valid_from,
        valid_to,
        entry_date_of_notice
    FROM `my_project.my_dataset.sof_ta_notice`
    WHERE cntrct_id = 'C001' -- Assuming 'C001' was an inserted record
    LIMIT 1
    """
    rows = client.query(query).result()
    for row in rows:
        assert isinstance(row.cntrct_id, str), "cntrct_id is not a string"
        assert isinstance(row.valid_from, datetime.date), "valid_from is not a date"
        # valid_to can be None if NULL, or datetime.date
        assert row.valid_to is None or isinstance(row.valid_to, datetime.date), "valid_to is not a date or None"
        assert isinstance(row.entry_date_of_notice, datetime.date), "entry_date_of_notice is not a date"
        # Add more specific value checks if needed, e.g., length of strings, date ranges.
        assert len(row.cntrct_id) > 0, "cntrct_id is empty"
```

---

## Test Case 7: Parameter Handling and Default Behavior

*   **Purpose**: Verify that the `sp_r_ausd_v_ta_notice` procedure correctly handles its input parameters, including the optional `p_optional_job_kennung`, and that default behavior is as expected.
*   **Setup**:
    1.  **BigQuery Environment**:
        *   Ensure logging tables (`job_log`, `job_control`) are clear.
        *   `my_project.my_dataset.cds_ta_notice` and `my_project.my_dataset.dwtk_meldungen` should contain minimal valid data to allow the job to run successfully.
*   **Action**:
    1.  Call `sp_r_ausd_v_ta_notice` without `p_optional_job_kennung` (passing `NULL` or omitting it if allowed by BigQuery): `CALL my_project.my_dataset.sp_r_ausd_v_ta_notice(NULL);`
    2.  Call `sp_r_ausd_v_ta_notice` with a specific `p_optional_job_kennung`: `CALL my_project.my_dataset.sp_r_ausd_v_ta_notice('CUSTOM_JOB_ID');`
*   **Pass/Fail Criterion**:
    *   For the `NULL` call, the `job_kennung` in `job_control` and `job_log` must be 'R_AUSD_V_TA_NOTICE' (the default).
    *   For the `CUSTOM_JOB_ID` call, the `job_kennung` in `job_control` and `job_log` must be 'CUSTOM_JOB_ID'.
    *   Both calls should complete successfully (status 'SUCCESS' in `job_control`).

```sql
-- Test 1: No optional job_kennung provided (should use default 'R_AUSD_V_TA_NOTICE')
TRUNCATE TABLE `my_project.my_dataset.job_control`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
-- Ensure source data exists for successful run
INSERT INTO `my_project.my_dataset.cds_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES ('D1', '2023-01-01', NULL, '2023-01-01', '2023-01-10', NULL, 1);
INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00');

CALL `my_project.my_dataset.sp_r_ausd_v_ta_notice`(NULL);
SELECT job_kennung, status FROM `my_project.my_dataset.job_control` ORDER BY start_timestamp DESC LIMIT 1;
-- Expected: job_kennung = 'R_AUSD_V_TA_NOTICE', status = 'SUCCESS'

-- Test 2: Custom job_kennung provided
TRUNCATE TABLE `my_project.my_dataset.job_control`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
-- Ensure source data exists for successful run
INSERT INTO `my_project.my_dataset.cds_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES ('D2', '2023-01-01', NULL, '2023-01-01', '2023-01-10', NULL, 1);
INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00');

CALL `my_project.my_dataset.sp_r_ausd_v_ta_notice`('CUSTOM_JOB_ID');
SELECT job_kennung, status FROM `my_project.my_dataset.job_control` ORDER BY start_timestamp DESC LIMIT 1;
-- Expected: job_kennung = 'CUSTOM_JOB_ID', status = 'SUCCESS'
```