This document outlines a comprehensive set of migration validation tests for the `k_ausd_v_ta_bp_ref.ksh` job, which has been migrated from a KornShell script orchestrating Oracle SQL*Plus to a BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_bp_ref_sp`). These tests are designed to ensure behavioral equivalence, data integrity, and correct functionality across all specified migration aspects.

**Assumptions for Testing:**
*   A BigQuery project and dataset (`project.dataset`) are configured.
*   All necessary BigQuery tables (`dwtk_meldungen`, `cds_ta_bp_ref`, `sof_ta_bp_ref`, `job_control`, `job_error_log`) exist with appropriate schemas, mirroring their Oracle counterparts.
*   The `k_ausd_v_ta_bp_ref_sp` BigQuery Stored Procedure is deployed.
*   A test harness (e.g., Python with `google-cloud-bigquery` and `subprocess` for legacy script execution) is available to:
    *   Execute the legacy KornShell script and capture its outputs/side effects (e.g., Oracle DB state).
    *   Execute the BigQuery Stored Procedure.
    *   Insert and query data from BigQuery tables.
    *   Query data from the legacy Oracle database (for comparison).
*   For simplicity, `project.dataset` will be used as a placeholder for the actual BigQuery project and dataset names.
*   The `test_utils.py` snippets are conceptual and would require actual implementation for Oracle interaction and robust BigQuery data manipulation.

---

## Test Case 1.1: Standard Data Transformation and Output Parity

*   **Purpose:** Verify that for a typical set of input data, the migrated BigQuery Stored Procedure produces the exact same output data in `project.dataset.sof_ta_bp_ref` as the legacy KornShell script would produce in `sof$ta_bp_ref` in Oracle. This covers the core `INSERT...SELECT` logic and filters.
*   **Setup:**
    1.  Ensure `project.dataset.dwtk_meldungen` contains a `timecreated` entry (e.g., `2023-01-15 10:00:00 UTC`).
    2.  Populate `project.dataset.cds_ta_bp_ref` with a diverse set of test data, including rows that should be selected and rows that should be filtered out based on the `WHERE` clause conditions (dates, `is_production`, `bp_ref_ty`).
        *   Example `cds_ta_bp_ref` data:
            ```
            (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty)
            ('C1', 'B1', '2023-01-10', NULL, '2023-01-01', NULL, 1, 4)  -- Should be selected
            ('C2', 'B2', '2023-01-10', '2023-01-16', '2023-01-01', NULL, 1, 4) -- Should be selected
            ('C3', 'B3', '2023-01-10', '2023-01-14', '2023-01-01', NULL, 1, 4) -- Should be filtered (modified_at <= v_datum)
            ('C4', 'B4', '2023-01-10', NULL, '2023-01-01', '2023-01-14', 1, 4) -- Should be filtered (valid_to <= v_datum)
            ('C5', 'B5', '2023-01-10', NULL, '2023-01-16', NULL, 1, 4) -- Should be filtered (valid_from > v_datum)
            ('C6', 'B6', '2023-01-10', NULL, '2023-01-01', NULL, 0, 4) -- Should be filtered (is_production != 1)
            ('C7', 'B7', '2023-01-10', NULL, '2023-01-01', NULL, 1, 5) -- Should be filtered (bp_ref_ty != 4)
            ```
    3.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.job_control`, and `project.dataset.job_error_log`.
    4.  Ensure the corresponding Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`) are in an identical state.
*   **Action:**
    1.  Execute the legacy KornShell script: `run_legacy_ksh(job_kennung='TEST_JOB_1', eintrags_nr=101)`
    2.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_1', eintrags_nr=101)`
*   **Pass/Fail Criterion:**
    *   The data in `project.dataset.sof_ta_bp_ref` must be identical to the data in Oracle's `sof$ta_bp_ref`.
    *   The row count in `project.dataset.sof_ta_bp_ref` must match the row count in Oracle's `sof$ta_bp_ref`.
    *   The `job_control` table in BigQuery should show a `SUCCESS` status for `TEST_JOB_1` with `records_processed` matching the row count.

```python
# Example pytest for Test Case 1.1
import pytest
from your_test_utils import (
    run_legacy_ksh, run_bq_sp, get_bq_table_data, get_oracle_table_data,
    clear_bq_table, insert_bq_data, get_job_control_entry
)

PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_dataset_id"

@pytest.fixture(autouse=True)
def setup_teardown_tables():
    # Clear BQ tables before each test
    clear_bq_table("dwtk_meldungen")
    clear_bq_table("cds_ta_bp_ref")
    clear_bq_table("sof_ta_bp_ref")
    clear_bq_table("job_control")
    clear_bq_table("job_error_log")

    # Populate dwtk_meldungen
    insert_bq_data("dwtk_meldungen", ["timecreated"], [("2023-01-15 10:00:00 UTC",)])

    # Populate cds_ta_bp_ref
    cds_data = [
        ('C1', 'B1', '2023-01-10', None, '2023-01-01', None, 1, 4),
        ('C2', 'B2', '2023-01-10', '2023-01-16', '2023-01-01', None, 1, 4),
        ('C3', 'B3', '2023-01-10', '2023-01-14', '2023-01-01', None, 1, 4), # Filtered
        ('C4', 'B4', '2023-01-10', None, '2023-01-01', '2023-01-14', 1, 4), # Filtered
        ('C5', 'B5', '2023-01-10', None, '2023-01-16', None, 1, 4), # Filtered
        ('C6', 'B6', '2023-01-10', None, '2023-01-01', None, 0, 4), # Filtered
        ('C7', 'B7', '2023-01-10', None, '2023-01-01', None, 1, 5), # Filtered
    ]
    cds_cols = ["cntrct_cp2_id", "bp_id", "insert_at", "modified_at", "valid_from", "valid_to", "is_production", "bp_ref_ty"]
    insert_bq_data("cds_ta_bp_ref", cds_cols, cds_data)

    # Assume Oracle setup is mirrored here (conceptual)
    # setup_oracle_tables_with_data(...)
    yield
    # Teardown (optional, if not handled by fixture scope)
    # clear_bq_table(...)

def test_standard_data_transformation_parity():
    job_kennung = 'TEST_JOB_1'
    eintrags_nr = 101

    # 1. Run Legacy KSH (conceptual)
    # legacy_return_code, legacy_stdout, legacy_stderr = run_legacy_ksh(job_kennung, eintrags_nr)
    # assert legacy_return_code == 0, f"Legacy KSH failed: {legacy_stderr}"
    # oracle_sof_data = get_oracle_table_data("sof$ta_bp_ref") # Get data from Oracle

    # For testing the BQ SP in isolation, we'll define expected data directly
    expected_sof_data = [
        ('C1', 'B1'),
        ('C2', 'B2'),
    ]
    # Sort for consistent comparison
    expected_sof_data.sort()

    # 2. Run BigQuery Stored Procedure
    bq_return_code, bq_stdout, bq_stderr = run_bq_sp(job_kennung, eintrags_nr)
    assert bq_return_code == 0, f"BigQuery SP failed: {bq_stderr}"

    # 3. Verify Output Parity
    bq_sof_data = get_bq_table_data("sof_ta_bp_ref")
    bq_sof_data.sort() # Sort for consistent comparison

    assert bq_sof_data == expected_sof_data, "Data in sof_ta_bp_ref does not match expected output."
    # assert bq_sof_data == oracle_sof_data, "Data in sof_ta_bp_ref does not match Oracle output."

    # 4. Verify Job Control Entry
    job_control_entry = get_job_control_entry(job_kennung, eintrags_nr)
    assert job_control_entry is not None, "Job control entry not found."
    assert job_control_entry.status == 'SUCCESS', f"Job status was {job_control_entry.status}, expected SUCCESS."
    assert job_control_entry.records_processed == len(expected_sof_data), \
        f"Records processed mismatch: Expected {len(expected_sof_data)}, Got {job_control_entry.records_processed}."
    assert job_control_entry.processing_date == '2023-01-15', "Processing date mismatch."
```

---

## Test Case 1.2: Empty Source Data (`cds_ta_bp_ref`)

*   **Purpose:** Verify the job handles an empty source table gracefully, resulting in an empty target table and correct record count.
*   **Setup:**
    1.  Ensure `project.dataset.dwtk_meldungen` contains a `timecreated` entry.
    2.  Ensure `project.dataset.cds_ta_bp_ref` is empty.
    3.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.job_control`, and `project.dataset.job_error_log`.
    4.  Ensure corresponding Oracle tables are in an identical state.
*   **Action:**
    1.  Execute the legacy KornShell script: `run_legacy_ksh(job_kennung='TEST_JOB_2', eintrags_nr=102)`
    2.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_2', eintrags_nr=102)`
*   **Pass/Fail Criterion:**
    *   `project.dataset.sof_ta_bp_ref` must be empty (0 rows).
    *   The `job_control` table in BigQuery should show a `SUCCESS` status for `TEST_JOB_2` with `records_processed = 0`.

---

## Test Case 1.3: No `dwtk_meldungen` Entries (Date Determination Default)

*   **Purpose:** Verify that if `dwtk_meldungen` is empty, `v_datum` defaults to `CURRENT_DATE()` as per the `COALESCE` logic, and the transformation proceeds correctly with this date.
*   **Setup:**
    1.  Ensure `project.dataset.dwtk_meldungen` is empty.
    2.  Populate `project.dataset.cds_ta_bp_ref` with data that would be selected if `v_datum` is `CURRENT_DATE()`.
        *   Example: `insert_at` and `valid_from` values well in the past, `modified_at` and `valid_to` values in the future or NULL.
    3.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.job_control`, and `project.dataset.job_error_log`.
    4.  Ensure corresponding Oracle tables are in an identical state.
*   **Action:**
    1.  Execute the legacy KornShell script: `run_legacy_ksh(job_kennung='TEST_JOB_3', eintrags_nr=103)`
    2.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_3', eintrags_nr=103)`
*   **Pass/Fail Criterion:**
    *   The data in `project.dataset.sof_ta_bp_ref` must be identical to the data in Oracle's `sof$ta_bp_ref` (which should reflect `CURRENT_DATE()` as `v_datum`).
    *   The `job_control` table in BigQuery should show a `SUCCESS` status for `TEST_JOB_3` with `processing_date` matching `CURRENT_DATE()`.

---

## Test Case 2.1: `v_datum` Calculation Correctness

*   **Purpose:** Explicitly verify the `v_datum` calculation logic, including `MAX(timecreated)` and `COALESCE(..., CURRENT_DATE())`.
*   **Setup:**
    1.  **Scenario A (Max `timecreated`):** Populate `project.dataset.dwtk_meldungen` with multiple `timecreated` entries, ensuring one is clearly the maximum (e.g., `2023-01-01`, `2023-01-10`, `2023-01-15`).
    2.  **Scenario B (Empty `dwtk_meldungen`):** Ensure `project.dataset.dwtk_meldungen` is empty.
    3.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.job_control`, and `project.dataset.job_error_log`.
*   **Action:**
    1.  **Scenario A:** Execute `run_bq_sp(job_kennung='TEST_JOB_4A', eintrags_nr=104)`.
    2.  **Scenario B:** Execute `run_bq_sp(job_kennung='TEST_JOB_4B', eintrags_nr=105)`.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The `processing_date` in `job_control` for `TEST_JOB_4A` must be `2023-01-15` (or the `YYYY-MM-DD` representation of the maximum `timecreated`).
    *   **Scenario B:** The `processing_date` in `job_control` for `TEST_JOB_4B` must be `CURRENT_DATE()` (the date when the SP was executed).

```sql
-- SQL Assertion for Scenario A (after SP execution)
SELECT processing_date FROM `project.dataset.job_control`
WHERE job_id = 'TEST_JOB_4A' AND entry_number = 104;
-- Expected result: '2023-01-15'

-- SQL Assertion for Scenario B (after SP execution)
SELECT processing_date FROM `project.dataset.job_control`
WHERE job_id = 'TEST_JOB_4B' AND entry_number = 105;
-- Expected result: CURRENT_DATE() (e.g., '2024-07-30')
```

---

## Test Case 2.2: `TRUNCATE` Behavior

*   **Purpose:** Verify that the `TRUNCATE TABLE` operation correctly clears the target table before new data is inserted.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bp_ref` with some dummy data (e.g., one row).
    2.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_bp_ref` such that the SP *will* insert at least one row.
    3.  Clear `project.dataset.job_control` and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_5', eintrags_nr=106)`
*   **Pass/Fail Criterion:**
    *   The final row count in `project.dataset.sof_ta_bp_ref` must be equal to the number of rows inserted by the `INSERT...SELECT` statement, not the sum of initial dummy data and new data. This implicitly proves the truncate happened.
    *   The `job_control` entry for `TEST_JOB_5` should reflect the correct number of newly inserted records.

---

## Test Case 2.3: `INSERT...SELECT` Filter Logic (Edge Cases)

*   **Purpose:** Thoroughly test the `WHERE` clause conditions, especially around date boundaries and NULL values.
*   **Setup:**
    1.  Set `project.dataset.dwtk_meldungen` to yield a specific `v_datum` (e.g., `2023-01-15`).
    2.  Populate `project.dataset.cds_ta_bp_ref` with rows specifically designed to test each part of the `WHERE` clause:
        *   `insert_at`: exactly `v_datum`, one day before, one day after.
        *   `modified_at`: `NULL`, exactly `v_datum`, one day before, one day after.
        *   `valid_from`: exactly `v_datum`, one day before, one day after.
        *   `valid_to`: `NULL`, exactly `v_datum`, one day before, one day after.
        *   `is_production`: `0`, `1`.
        *   `bp_ref_ty`: `4`, `5`.
    3.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.job_control`, and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_6', eintrags_nr=107)`
*   **Pass/Fail Criterion:**
    *   Query `project.dataset.sof_ta_bp_ref` and verify that only rows satisfying *all* `WHERE` conditions (with `v_datum = '20230115'`) are present.
    *   Specifically, verify:
        *   `insert_at <= '2023-01-15'`
        *   `modified_at IS NULL` OR `modified_at > '2023-01-15'`
        *   `valid_from <= '2023-01-15'`
        *   `valid_to IS NULL` OR `valid_to > '2023-01-15'`
        *   `is_production = 1`
        *   `bp_ref_ty = 4`

```sql
-- Example assertion for a specific row after SP execution
-- Assuming v_datum was '20230115'
SELECT COUNT(*) FROM `project.dataset.sof_ta_bp_ref`
WHERE
    cntrct_cp2_id = 'C_TEST_1' AND bp_id = 'B_TEST_1'
    AND insert_at = '2023-01-15'
    AND modified_at IS NULL
    AND valid_from = '2023-01-15'
    AND valid_to IS NULL
    AND is_production = 1
    AND bp_ref_ty = 4;
-- Expected result: 1 (if this row was designed to pass all filters)

SELECT COUNT(*) FROM `project.dataset.sof_ta_bp_ref`
WHERE
    cntrct_cp2_id = 'C_TEST_FAIL_1' AND bp_id = 'B_TEST_FAIL_1'
    AND modified_at = '2023-01-14'; -- This row should have been filtered out
-- Expected result: 0
```

---

## Test Case 3.1: External System Replacement - Oracle Reads

*   **Purpose:** Confirm that the BigQuery Stored Procedure exclusively uses BigQuery tables (`project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_bp_ref`) as sources and does not attempt to connect to or read from any legacy Oracle databases (e.g., via DB-Links).
*   **Setup:**
    1.  Ensure the BigQuery tables (`dwtk_meldungen`, `cds_ta_bp_ref`) are populated with test data.
    2.  (Optional but recommended for strict isolation) Temporarily disable or block network access from the BigQuery environment to the legacy Oracle database.
    3.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.job_control`, and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_7', eintrags_nr=108)`
*   **Pass/Fail Criterion:**
    *   The BigQuery Stored Procedure must complete successfully.
    *   The `project.dataset.sof_ta_bp_ref` table must be populated correctly based *only* on the data in the BigQuery source tables.
    *   No errors related to Oracle connectivity or DB-Link failures should be observed in BigQuery logs (Cloud Logging).

---

## Test Case 3.2: External System Replacement - Oracle `TRUNCATE` Procedure

*   **Purpose:** Verify that the `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call for `TRUNCATE TABLE` is correctly replaced by a native BigQuery `TRUNCATE TABLE` statement.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bp_ref` with dummy data.
    2.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_bp_ref` with data that would result in new inserts.
    3.  Clear `project.dataset.job_control` and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_8', eintrags_nr=109)`
*   **Pass/Fail Criterion:**
    *   The `project.dataset.sof_ta_bp_ref` table must contain only the data inserted by the current run, confirming the prior truncation.
    *   No errors related to missing Oracle procedures or `DWPA_UTIL_SKRIPT` should appear in BigQuery logs.

---

## Test Case 4.1: Job Control Table - Success Scenario

*   **Purpose:** Verify that the `job_control` table is correctly updated upon successful completion of the BigQuery Stored Procedure.
*   **Setup:**
    1.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_bp_ref` to ensure a successful run with some records.
    2.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.job_control`, and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_9', eintrags_nr=110)`
*   **Pass/Fail Criterion:**
    *   A single entry must exist in `project.dataset.job_control` for `job_id = 'TEST_JOB_9'` and `entry_number = 110`.
    *   This entry must have `status = 'SUCCESS'`.
    *   `start_timestamp` and `end_timestamp` must be populated and `end_timestamp` must be after `start_timestamp`.
    *   `records_processed` must accurately reflect the number of rows inserted into `project.dataset.sof_ta_bp_ref`.
    *   `processing_date` must match the `v_datum` derived during the run.

```sql
-- SQL Assertion (after SP execution)
SELECT status, records_processed, processing_date, start_timestamp IS NOT NULL AS start_set, end_timestamp IS NOT NULL AS end_set
FROM `project.dataset.job_control`
WHERE job_id = 'TEST_JOB_9' AND entry_number = 110;
-- Expected result: ('SUCCESS', <actual_record_count>, <actual_processing_date>, TRUE, TRUE)
```

---

## Test Case 4.2: Job Control Table - Failure Scenario

*   **Purpose:** Verify that the `job_control` and `job_error_log` tables are correctly updated when the BigQuery Stored Procedure encounters an error.
*   **Setup:**
    1.  Configure `project.dataset.cds_ta_bp_ref` or `project.dataset.sof_ta_bp_ref` to cause an error during the `INSERT...SELECT` (e.g., by making `sof_ta_bp_ref` schema incompatible, or by attempting to insert invalid data if BigQuery's strict typing is enabled).
        *   Example: Alter `sof_ta_bp_ref` to have a `NOT NULL` column that `cds_ta_bp_ref` provides `NULL` for, or make a column too small.
    2.  Clear `project.dataset.job_control` and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_10', eintrags_nr=111)`
*   **Pass/Fail Criterion:**
    *   The `run_bq_sp` call must return a non-zero error code.
    *   A single entry must exist in `project.dataset.job_control` for `job_id = 'TEST_JOB_10'` and `entry_number = 111`.
    *   This entry must have `status = 'FAILED'`.
    *   `records_processed` should be `0` (as the transaction would likely be rolled back or no successful inserts occurred).
    *   An entry must exist in `project.dataset.job_error_log` for `job_id = 'TEST_JOB_10'` and `entry_number = 111`, with `log_level = 'ERROR'` and a descriptive `error_message`.

```sql
-- SQL Assertion (after SP execution)
SELECT status, records_processed FROM `project.dataset.job_control`
WHERE job_id = 'TEST_JOB_10' AND entry_number = 111;
-- Expected result: ('FAILED', 0)

SELECT error_message, log_level FROM `project.dataset.job_error_log`
WHERE job_id = 'TEST_JOB_10' AND entry_number = 111;
-- Expected result: (<error_message_string>, 'ERROR')
```

---

## Test Case 4.3: Parameter Validation

*   **Purpose:** Verify that the stored procedure correctly validates input parameters (`p_JobKennung`, `p_EintragsNr`) and logs errors for invalid inputs.
*   **Setup:**
    1.  Clear `project.dataset.job_control` and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute with `p_JobKennung = NULL`: `run_bq_sp(job_kennung=None, eintrags_nr=112)`
    2.  Execute with `p_JobKennung = ''`: `run_bq_sp(job_kennung='', eintrags_nr=113)`
    3.  Execute with `p_EintragsNr = NULL`: `run_bq_sp(job_kennung='TEST_JOB_11C', eintrags_nr=None)`
*   **Pass/Fail Criterion:**
    *   Each execution must result in an error (non-zero return code from `run_bq_sp`).
    *   For each case, an entry must be present in `project.dataset.job_error_log` with `log_level = 'ERROR'` and an `error_message` indicating the specific parameter validation failure.
    *   No entries should be created in `project.dataset.job_control` for these invalid parameter calls (as validation happens before job control entry).

```sql
-- SQL Assertion for p_JobKennung = NULL
SELECT error_message FROM `project.dataset.job_error_log`
WHERE entry_number = 112 AND log_level = 'ERROR';
-- Expected result: 'Parameter p_JobKennung cannot be NULL or empty.'

-- SQL Assertion for p_JobKennung = ''
SELECT error_message FROM `project.dataset.job_error_log`
WHERE entry_number = 113 AND log_level = 'ERROR';
-- Expected result: 'Parameter p_JobKennung cannot be NULL or empty.'

-- SQL Assertion for p_EintragsNr = NULL
SELECT error_message FROM `project.dataset.job_error_log`
WHERE job_id = 'TEST_JOB_11C' AND log_level = 'ERROR';
-- Expected result: 'Parameter p_EintragsNr cannot be NULL.'
```

---

## Test Case 4.4: Active Job Handling - Ignore Current Run

*   **Purpose:** Verify that if an active job with the same `p_JobKennung` is already running, the current execution is gracefully ignored, and an informational message is logged.
*   **Setup:**
    1.  Insert an entry into `project.dataset.job_control` with `job_id = 'TEST_JOB_12'`, `status = 'RUNNING'`, and a recent `start_timestamp`.
    2.  Clear `project.dataset.sof_ta_bp_ref` and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_12', eintrags_nr=114)`
*   **Pass/Fail Criterion:**
    *   The `run_bq_sp` call must complete without error (return code 0).
    *   `project.dataset.sof_ta_bp_ref` must remain empty (no data inserted by the ignored run).
    *   No new `job_control` entry should be created for `entry_number = 114`.
    *   An entry must exist in `project.dataset.job_error_log` for `job_id = 'TEST_JOB_12'` and `entry_number = 114`, with `log_level = 'INFO'` and `error_message = 'Active job found for JobKennung. Current run ignored.'`.

```sql
-- SQL Assertion (after SP execution)
SELECT COUNT(*) FROM `project.dataset.sof_ta_bp_ref`;
-- Expected result: 0

SELECT COUNT(*) FROM `project.dataset.job_control`
WHERE job_id = 'TEST_JOB_12' AND entry_number = 114;
-- Expected result: 0 (no new entry for this run)

SELECT error_message, log_level FROM `project.dataset.job_error_log`
WHERE job_id = 'TEST_JOB_12' AND entry_number = 114;
-- Expected result: ('Active job found for JobKennung. Current run ignored.', 'INFO')
```

---

## Test Case 4.5: Active Job Handling - Deactivate Old Jobs

*   **Purpose:** Verify that old `RUNNING` jobs (older than 24 hours) are correctly deactivated (`status = 'FAILED'`) before a new job starts.
*   **Setup:**
    1.  Insert an entry into `project.dataset.job_control` with `job_id = 'TEST_JOB_13'`, `status = 'RUNNING'`, and `start_timestamp` set to more than 24 hours in the past (e.g., `CURRENT_TIMESTAMP() - INTERVAL 25 HOUR`).
    2.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_bp_ref` to ensure the new run will be successful.
    3.  Clear `project.dataset.sof_ta_bp_ref` and `project.dataset.job_error_log`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `run_bq_sp(job_kennung='TEST_JOB_13', eintrags_nr=115)`
*   **Pass/Fail Criterion:**
    *   The `run_bq_sp` call must complete successfully.
    *   The old `job_control` entry for `job_id = 'TEST_JOB_13'` (the one inserted in setup) must have its `status` updated to `FAILED` and `end_timestamp` populated.
    *   A new `job_control` entry for `job_id = 'TEST_JOB_13'` and `entry_number = 115` must exist with `status = 'SUCCESS'`.
    *   `project.dataset.sof_ta_bp_ref` must be populated correctly by the new run.

```sql
-- SQL Assertion (after SP execution)
SELECT status, end_timestamp IS NOT NULL AS end_set FROM `project.dataset.job_control`
WHERE job_id = 'TEST_JOB_13' AND status = 'FAILED';
-- Expected result: ('FAILED', TRUE) (for the old entry)

SELECT status FROM `project.dataset.job_control`
WHERE job_id = 'TEST_JOB_13' AND entry_number = 115;
-- Expected result: ('SUCCESS') (for the new entry)
```