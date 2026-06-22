The migration of `k_ausd_bp_ta_cntrct_dist.ksh` to BigQuery stored procedures requires thorough validation to ensure behavioral equivalence. The following test cases cover output parity, transformation correctness, external system replacements, and data quality assertions, focusing on the orchestration logic of the migrated `sp_k_ausd_bp_ta_cntrct_dist` procedure.

**Note on Test Setup:**
For some tests, temporary modifications to the BigQuery stored procedures (`sp_k_ausd_bp_ta_cntrct_dist` and `sp_d_ausd_bp_ta_cntrct_dist`) are suggested to make internal state and parameter passing observable. These modifications should be reverted after testing. The provided `pytest` examples assume these temporary modifications are in place.

---

## Test Case 1: Successful Execution with Valid Parameters

**Purpose:**
Verify that the migrated orchestration stored procedure `sp_k_ausd_bp_ta_cntrct_dist` executes successfully when provided with all valid parameters, correctly calls the core SQL procedure (`sp_d_ausd_bp_ta_cntrct_dist`), and accurately logs the job status and record count in the `job_tracking_table`.

**Setup:**
1.  Ensure `my_gcp_project.my_bq_dataset.job_tracking_table` and `my_gcp_project.my_bq_dataset.target_result_table` are empty or truncated before execution.
2.  The placeholder `sp_d_ausd_bp_ta_cntrct_dist` should be configured to insert a predictable number of rows (e.g., 2 rows) into `target_result_table` for the given `p_Stichtag`.

**Action:**
Execute the `sp_k_ausd_bp_ta_cntrct_dist` stored procedure with a set of valid input parameters:
*   `p_JobKennung = 'TEST_JOB_001'`
*   `p_EintragsNr = 'ENTRY_001'`
*   `p_Stichtag_raw = '01012023'` (DDMMYYYY format)
*   `p_wiederanlaufWert = '0'`

**Pass/Fail Criterion:**
1.  The `CALL` statement for `sp_k_ausd_bp_ta_cntrct_dist` completes without raising any BigQuery errors.
2.  A single new record exists in `my_gcp_project.my_bq_dataset.job_tracking_table` with the following attributes:
    *   `job_id` = `'TEST_JOB_001'`
    *   `entry_number` = `'ENTRY_001'`
    *   `key_date` = `DATE('2023-01-01')`
    *   `status` = `'SUCCESS'`
    *   `record_count` = `2` (matching the expected output from `sp_d_ausd_bp_ta_cntrct_dist`).
    *   `start_timestamp` and `end_timestamp` are populated.
    *   `error_message` is `NULL`.
3.  The `my_gcp_project.my_bq_dataset.target_result_table` contains exactly 2 rows where `stichtag = DATE('2023-01-01')`.

```python
# pytest / Python client assertion
from google.cloud import bigquery
import pytest
from datetime import date

project_id = "my_gcp_project"
dataset_id = "my_bq_dataset"
client = bigquery.Client(project=project_id)

@pytest.fixture(autouse=True)
def setup_teardown_tables():
    # Truncate tables before each test for isolation
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_tracking_table`").result()
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.target_result_table`").result()
    yield

def test_successful_execution():
    job_kennung = 'TEST_JOB_001'
    eintrags_nr = 'ENTRY_001'
    stichtag_raw = '01012023'
    stichtag_parsed = date(2023, 1, 1)
    wiederanlauf_wert = '0'

    # Action: Call the main stored procedure
    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag_raw}',
        '{wiederanlauf_wert}'
    );
    """
    client.query(call_query).result()

    # Pass/Fail Criterion 1: Check job_tracking_table
    tracking_query = f"""
    SELECT job_id, entry_number, key_date, record_count, status, error_message
    FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE job_id = '{job_kennung}' AND entry_number = '{eintrags_nr}' AND key_date = '{stichtag_parsed.isoformat()}'
    """
    tracking_results = list(client.query(tracking_query).result())
    assert len(tracking_results) == 1, "Expected one tracking record."
    tracking_record = tracking_results[0]
    assert tracking_record['job_id'] == job_kennung
    assert tracking_record['entry_number'] == eintrags_nr
    assert tracking_record['key_date'] == stichtag_parsed
    assert tracking_record['status'] == 'SUCCESS'
    assert tracking_record['error_message'] is None

    # Pass/Fail Criterion 2: Check target_result_table count
    target_count_query = f"""
    SELECT COUNT(*) FROM `{project_id}.{dataset_id}.target_result_table`
    WHERE stichtag = '{stichtag_parsed.isoformat()}'
    """
    target_count_result = list(client.query(target_count_query).result())[0][0]
    assert target_count_result == 2, "Expected 2 records in target_result_table."
    assert tracking_record['record_count'] == target_count_result, "Record count in tracking table mismatch."
```

---

## Test Case 2: Missing `Jobkennung` Parameter

**Purpose:**
Verify that the migrated procedure correctly handles a missing or empty `Jobkennung` parameter, raises an error, and logs the job status as 'FAILED' with an appropriate error message. This tests the parameter validation logic.

**Setup:**
1.  Ensure `my_gcp_project.my_bq_dataset.job_tracking_table` is empty.

**Action:**
Execute the `sp_k_ausd_bp_ta_cntrct_dist` stored procedure with `p_JobKennung` as an empty string:
*   `p_JobKennung = ''`
*   `p_EintragsNr = 'ENTRY_002'`
*   `p_Stichtag_raw = '02012023'`
*   `p_wiederanlaufWert = '0'`

**Pass/Fail Criterion:**
1.  The `CALL` statement for `sp_k_ausd_bp_ta_cntrct_dist` completes without a Python exception (as the procedure's `EXCEPTION` block handles it internally).
2.  A single new record exists in `my_gcp_project.my_bq_dataset.job_tracking_table` with:
    *   `job_id` = `''` (the empty string passed)
    *   `entry_number` = `'ENTRY_002'`
    *   `key_date` = `DATE('2023-01-02')` (Stichtag parsing should succeed before Jobkennung check)
    *   `status` = `'FAILED'`
    *   `record_count` = `NULL`
    *   `error_message` contains the substring "Jobkennung parameter is missing or empty."

```python
# pytest / Python client assertion
def test_missing_jobkennung_parameter():
    job_kennung = ''
    eintrags_nr = 'ENTRY_002'
    stichtag_raw = '02012023'
    stichtag_parsed = date(2023, 1, 2)
    wiederanlauf_wert = '0'

    # Action: Call the main stored procedure with missing Jobkennung
    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag_raw}',
        '{wiederanlauf_wert}'
    );
    """
    client.query(call_query).result() # Should complete without Python exception

    # Pass/Fail Criterion: Check job_tracking_table for FAILED status
    tracking_query = f"""
    SELECT job_id, entry_number, key_date, record_count, status, error_message
    FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE entry_number = '{eintrags_nr}'
    ORDER BY start_timestamp DESC LIMIT 1
    """
    tracking_results = list(client.query(tracking_query).result())
    assert len(tracking_results) == 1, "Expected one tracking record."
    tracking_record = tracking_results[0]
    assert tracking_record['job_id'] == job_kennung
    assert tracking_record['entry_number'] == eintrags_nr
    assert tracking_record['key_date'] == stichtag_parsed
    assert tracking_record['status'] == 'FAILED'
    assert tracking_record['record_count'] is None
    assert "Jobkennung parameter is missing or empty." in tracking_record['error_message']
```

---

## Test Case 3: Invalid `Stichtag` Format

**Purpose:**
Verify that the migrated procedure correctly handles an invalid `Stichtag` format (not `DDMMYYYY`), raises an error, and logs the job status as 'FAILED' with an appropriate error message. This tests the date validation logic.

**Setup:**
1.  Ensure `my_gcp_project.my_bq_dataset.job_tracking_table` is empty.

**Action:**
Execute the `sp_k_ausd_bp_ta_cntrct_dist` stored procedure with an invalid `Stichtag_raw`:
*   `p_JobKennung = 'TEST_JOB_003'`
*   `p_EintragsNr = 'ENTRY_003'`
*   `p_Stichtag_raw = '2023-01-03'` (invalid format, expected DDMMYYYY)
*   `p_wiederanlaufWert = '0'`

**Pass/Fail Criterion:**
1.  The `CALL` statement for `sp_k_ausd_bp_ta_cntrct_dist` completes without a Python exception.
2.  A single new record exists in `my_gcp_project.my_bq_dataset.job_tracking_table` with:
    *   `job_id` = `'TEST_JOB_003'`
    *   `entry_number` = `'ENTRY_003'`
    *   `key_date` = `NULL` (as parsing failed)
    *   `status` = `'FAILED'`
    *   `record_count` = `NULL`
    *   `error_message` contains the substring "Invalid Stichtag format".

```python
# pytest / Python client assertion
def test_invalid_stichtag_format():
    job_kennung = 'TEST_JOB_003'
    eintrags_nr = 'ENTRY_003'
    stichtag_raw = '2023-01-03' # Invalid format
    wiederanlauf_wert = '0'

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag_raw}',
        '{wiederanlauf_wert}'
    );
    """
    client.query(call_query).result() # Should complete without Python exception

    tracking_query = f"""
    SELECT job_id, entry_number, key_date, record_count, status, error_message
    FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE job_id = '{job_kennung}' AND entry_number = '{eintrags_nr}'
    ORDER BY start_timestamp DESC LIMIT 1
    """
    tracking_results = list(client.query(tracking_query).result())
    assert len(tracking_results) == 1, "Expected one tracking record."
    tracking_record = tracking_results[0]
    assert tracking_record['job_id'] == job_kennung
    assert tracking_record['entry_number'] == eintrags_nr
    assert tracking_record['key_date'] is None # Key date should be NULL because parsing failed
    assert tracking_record['status'] == 'FAILED'
    assert tracking_record['record_count'] is None
    assert "Invalid Stichtag format" in tracking_record['error_message']
```

---

## Test Case 4: Date Derivation Correctness

**Purpose:**
Verify that `v_datum_heute` and `v_datum_gestern` are correctly derived within `sp_k_ausd_bp_ta_cntrct_dist` using BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions, replicating the functionality of `gestern.ksh`.

**Setup:**
1.  Ensure `my_gcp_project.my_bq_dataset.job_tracking_table` is empty.
2.  **Temporary Modification:** Modify `sp_k_ausd_bp_ta_cntrct_dist` to include `OUT` parameters for `v_datum_heute` and `v_datum_gestern` to allow direct verification of their derived values.
    ```sql
    CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.sp_k_ausd_bp_ta_cntrct_dist`(
        IN p_JobKennung STRING,
        IN p_EintragsNr STRING,
        IN p_Stichtag_raw STRING,
        IN p_wiederanlaufWert STRING,
        OUT out_datum_heute DATE,    -- Temporary OUT parameter for testing
        OUT out_datum_gestern DATE   -- Temporary OUT parameter for testing
    )
    BEGIN
        -- ... existing declarations ...
        SET v_datum_heute = CURRENT_DATE();
        SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET out_datum_heute = v_datum_heute;     -- Assign for testing
        SET out_datum_gestern = v_datum_gestern; -- Assign for testing
        -- ... rest of the procedure ...
    END;
    ```

**Action:**
Execute the modified `sp_k_ausd_bp_ta_cntrct_dist` with valid parameters and capture the `OUT` parameters:
*   `p_JobKennung = 'TEST_JOB_004'`
*   `p_EintragsNr = 'ENTRY_004'`
*   `p_Stichtag_raw = '04012023'`
*   `p_wiederanlaufWert = '0'`

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully.
2.  The captured `out_datum_heute` matches the actual `CURRENT_DATE()` at the time of execution.
3.  The captured `out_datum_gestern` matches `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` at the time of execution.
4.  A record exists in `job_tracking_table` for this execution with `status = 'SUCCESS'`.

```python
# pytest / Python client assertion
from datetime import date, timedelta

def test_date_derivation_correctness():
    job_kennung = 'TEST_JOB_004'
    eintrags_nr = 'ENTRY_004'
    stichtag_raw = '04012023'
    wiederanlauf_wert = '0'

    # Action: Call the main stored procedure with OUT parameters
    call_query = f"""
    DECLARE v_heute DATE;
    DECLARE v_gestern DATE;
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag_raw}',
        '{wiederanlauf_wert}',
        v_heute,
        v_gestern
    );
    SELECT v_heute, v_gestern;
    """
    job = client.query(call_query)
    results = list(job.result())
    assert len(results) == 1, "Expected one result row for derived dates."
    derived_dates = results[0]

    expected_today = date.today()
    expected_yesterday = date.today() - timedelta(days=1)

    assert derived_dates['v_heute'] == expected_today, "Derived 'today' date mismatch."
    assert derived_dates['v_gestern'] == expected_yesterday, "Derived 'yesterday' date mismatch."

    # Also check job_tracking_table for successful execution
    tracking_query = f"""
    SELECT status FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE job_id = '{job_kennung}' AND entry_number = '{eintrags_nr}'
    ORDER BY start_timestamp DESC LIMIT 1
    """
    tracking_results = list(client.query(tracking_query).result())
    assert len(tracking_results) == 1, "Expected one tracking record for status."
    assert tracking_results[0]['status'] == 'SUCCESS', "Job status should be SUCCESS."
```

---

## Test Case 5: `p_wiederanlaufWert` Handling

**Purpose:**
Verify that `p_wiederanlaufWert` is correctly handled: its explicit value is passed to `sp_d_ausd_bp_ta_cntrct_dist`, and if not provided (NULL or empty string), it defaults to `'0'` as per the original KornShell script's logic.

**Setup:**
1.  Ensure `my_gcp_project.my_bq_dataset.job_tracking_table` is empty.
2.  **Temporary Modification 1:** Add the default value logic for `p_wiederanlaufWert` to `sp_k_ausd_bp_ta_cntrct_dist`:
    ```sql
    -- Inside sp_k_ausd_bp_ta_cntrct_dist, after parameter declarations:
    IF p_wiederanlaufWert IS NULL OR LENGTH(TRIM(p_wiederanlaufWert)) = 0 THEN
      SET p_wiederanlaufWert = '0';
    END IF;
    ```
3.  **Temporary Modification 2:** Modify `sp_d_ausd_bp_ta_cntrct_dist` to include an `OUT` parameter to return the `p_wiederanlaufWert` it received.
    ```sql
    CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.sp_d_ausd_bp_ta_cntrct_dist`(
        IN p_JobKennung STRING,
        IN p_EintragsNr STRING,
        IN p_Stichtag DATE,
        IN p_wiederanlaufWert_in STRING, -- Renamed to avoid conflict with OUT
        OUT p_wiederanlaufWert_out STRING -- Temporary OUT parameter for testing
    )
    BEGIN
        SET p_wiederanlaufWert_out = p_wiederanlaufWert_in; -- Assign for testing
        -- ... rest of the procedure ...
    END;
    ```
4.  **Temporary Modification 3:** Update the `CALL` to `sp_d_ausd_bp_ta_cntrct_dist` within `sp_k_ausd_bp_ta_cntrct_dist` to capture this new `OUT` parameter.
    ```sql
    -- Inside sp_k_ausd_bp_ta_cntrct_dist, where sp_d_ausd_bp_ta_cntrct_dist is called:
    DECLARE v_wiederanlaufWert_from_core STRING; -- To capture OUT param
    CALL `my_gcp_project.my_bq_dataset.sp_d_ausd_bp_ta_cntrct_dist`(
        p_JobKennung,
        p_EintragsNr,
        v_Stichtag,
        p_wiederanlaufWert, -- Pass IN parameter
        v_wiederanlaufWert_from_core -- Capture OUT parameter
    );
    -- For testing, you might need to add v_wiederanlaufWert_from_core as an OUT param to sp_k_ausd_bp_ta_cntrct_dist as well.
    -- Or, for simplicity, assume the Python test can directly call sp_d_ausd_bp_ta_cntrct_dist to verify.
    -- Let's add it as an OUT param to sp_k_ausd_bp_ta_cntrct_dist for direct testing.
    ```
    **Revised Temporary Modification for `sp_k_ausd_bp_ta_cntrct_dist` (adding another OUT param):**
    ```sql
    CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.sp_k_ausd_bp_ta_cntrct_dist`(
        IN p_JobKennung STRING,
        IN p_EintragsNr STRING,
        IN p_Stichtag_raw STRING,
        IN p_wiederanlaufWert_in STRING, -- Renamed to avoid conflict with internal variable
        OUT out_datum_heute DATE,
        OUT out_datum_gestern DATE,
        OUT out_wiederanlaufWert_passed STRING -- Temporary OUT parameter for testing
    )
    BEGIN
        DECLARE p_wiederanlaufWert STRING DEFAULT p_wiederanlaufWert_in; -- Internal variable
        -- ...
        IF p_wiederanlaufWert IS NULL OR LENGTH(TRIM(p_wiederanlaufWert)) = 0 THEN
          SET p_wiederanlaufWert = '0';
        END IF;
        -- ...
        DECLARE v_wiederanlaufWert_from_core STRING;
        CALL `my_gcp_project.my_bq_dataset.sp_d_ausd_bp_ta_cntrct_dist`(
            p_JobKennung,
            p_EintragsNr,
            v_Stichtag,
            p_wiederanlaufWert,
            v_wiederanlaufWert_from_core
        );
        SET out_wiederanlaufWert_passed = v_wiederanlaufWert_from_core; -- Assign for testing
        -- ...
    END;
    ```

**Action:**
1.  Execute the modified `sp_k_ausd_bp_ta_cntrct_dist` with an explicit `p_wiederanlaufWert = '123'`.
2.  Execute the modified `sp_k_ausd_bp_ta_cntrct_dist` with `p_wiederanlaufWert = NULL` or `''`.

**Pass/Fail Criterion:**
1.  Both `CALL` statements complete successfully.
2.  For the first call, the captured `out_wiederanlaufWert_passed` from `sp_k_ausd_bp_ta_cntrct_dist` is `'123'`.
3.  For the second call, the captured `out_wiederanlaufWert_passed` is `'0'`.
4.  Both calls result in `status = 'SUCCESS'` in `job_tracking_table`.

```python
# pytest / Python client assertion
def test_wiederanlaufwert_handling():
    # Test 1: Explicit value
    job_kennung_1 = 'TEST_JOB_005_1'
    eintrags_nr_1 = 'ENTRY_005_1'
    stichtag_raw_1 = '05012023'
    wiederanlauf_wert_1 = '123'

    call_query_1 = f"""
    DECLARE v_heute DATE;
    DECLARE v_gestern DATE;
    DECLARE v_wiederanlauf_out STRING;
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung_1}',
        '{eintrags_nr_1}',
        '{stichtag_raw_1}',
        '{wiederanlauf_wert_1}',
        v_heute, v_gestern,
        v_wiederanlauf_out
    );
    SELECT v_wiederanlauf_out;
    """
    job_1 = client.query(call_query_1)
    results_1 = list(job_1.result())
    assert len(results_1) == 1, "Expected one result for explicit wiederanlaufWert."
    assert results_1[0]['v_wiederanlauf_out'] == wiederanlauf_wert_1, "Explicit wiederanlaufWert mismatch."

    # Test 2: Default value (NULL/empty string)
    job_kennung_2 = 'TEST_JOB_005_2'
    eintrags_nr_2 = 'ENTRY_005_2'
    stichtag_raw_2 = '05022023'
    wiederanlauf_wert_2 = '' # Or None

    call_query_2 = f"""
    DECLARE v_heute DATE;
    DECLARE v_gestern DATE;
    DECLARE v_wiederanlauf_out STRING;
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung_2}',
        '{eintrags_nr_2}',
        '{stichtag_raw_2}',
        '{wiederanlauf_wert_2}',
        v_heute, v_gestern,
        v_wiederanlauf_out
    );
    SELECT v_wiederanlauf_out;
    """
    job_2 = client.query(call_query_2)
    results_2 = list(job_2.result())
    assert len(results_2) == 1, "Expected one result for default wiederanlaufWert."
    assert results_2[0]['v_wiederanlauf_out'] == '0', "Default wiederanlaufWert mismatch."

    # Verify job_tracking_table status for both
    tracking_query = f"""
    SELECT job_id, status FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE job_id IN ('{job_kennung_1}', '{job_kennung_2}')
    """
    tracking_results = {r['job_id']: r['status'] for r in client.query(tracking_query).result()}
    assert tracking_results[job_kennung_1] == 'SUCCESS', f"Job {job_kennung_1} status should be SUCCESS."
    assert tracking_results[job_kennung_2] == 'SUCCESS', f"Job {job_kennung_2} status should be SUCCESS."
```

---

## Test Case 6: Core SQL Procedure Invocation and Record Count

**Purpose:**
Verify that `sp_k_ausd_bp_ta_cntrct_dist` correctly invokes `sp_d_ausd_bp_ta_cntrct_dist` with the right parameters and accurately captures the record count from `target_result_table` after `sp_d_ausd_bp_ta_cntrct_dist` completes. This directly replaces the temporary file reading (`eval "v_records=\`cat $tmpFile\`"`) from the legacy script.

**Setup:**
1.  Ensure `my_gcp_project.my_bq_dataset.job_tracking_table` and `my_gcp_project.my_bq_dataset.target_result_table` are empty.
2.  The placeholder `sp_d_ausd_bp_ta_cntrct_dist` should insert a *predictable* number of rows (e.g., 2 rows) into `target_result_table` for the given `p_Stichtag`.

**Action:**
Execute `sp_k_ausd_bp_ta_cntrct_dist` with valid parameters:
*   `p_JobKennung = 'TEST_JOB_006'`
*   `p_EintragsNr = 'ENTRY_006'`
*   `p_Stichtag_raw = '06012023'`
*   `p_wiederanlaufWert = '0'`

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully.
2.  A record exists in `job_tracking_table` with:
    *   `job_id` = `'TEST_JOB_006'`
    *   `status` = `'SUCCESS'`
    *   `record_count` = `2` (matching the expected output from `sp_d_ausd_bp_ta_cntrct_dist`).
3.  `my_gcp_project.my_bq_dataset.target_result_table` contains exactly 2 rows where `stichtag = DATE('2023-01-06')`.

```python
# pytest / Python client assertion
from datetime import date

def test_core_sql_invocation_and_record_count():
    job_kennung = 'TEST_JOB_006'
    eintrags_nr = 'ENTRY_006'
    stichtag_raw = '06012023'
    stichtag_parsed = date(2023, 1, 6)
    wiederanlauf_wert = '0'

    # Action: Call the main stored procedure
    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag_raw}',
        '{wiederanlauf_wert}'
    );
    """
    client.query(call_query).result()

    # Pass/Fail Criterion 1: Check job_tracking_table record_count
    tracking_query = f"""
    SELECT record_count, status
    FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE job_id = '{job_kennung}' AND key_date = '{stichtag_parsed.isoformat()}'
    """
    tracking_results = list(client.query(tracking_query).result())
    assert len(tracking_results) == 1, "Expected one tracking record."
    tracking_record = tracking_results[0]
    assert tracking_record['status'] == 'SUCCESS'
    assert tracking_record['record_count'] == 2, "Record count in tracking table mismatch."

    # Pass/Fail Criterion 2: Verify actual count in target_result_table
    target_count_query = f"""
    SELECT COUNT(*) FROM `{project_id}.{dataset_id}.target_result_table`
    WHERE stichtag = '{stichtag_parsed.isoformat()}'
    """
    actual_target_count = list(client.query(target_count_query).result())[0][0]
    assert actual_target_count == 2, "Actual target table count mismatch."
```

---

## Test Case 7: Error Handling in Core SQL Procedure

**Purpose:**
Verify that if `sp_d_ausd_bp_ta_cntrct_dist` (the core SQL logic) encounters an error during its execution, `sp_k_ausd_bp_ta_cntrct_dist` (the orchestration procedure) correctly catches this error, logs the error message, and sets the job status to 'FAILED' in the `job_tracking_table`.

**Setup:**
1.  Ensure `my_gcp_project.my_bq_dataset.job_tracking_table` is empty.
2.  **Temporary Modification:** Modify `sp_d_ausd_bp_ta_cntrct_dist` to `RAISE` an error under a specific condition (e.g., if `p_JobKennung` is `'FAIL_JOB'`).
    ```sql
    CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.sp_d_ausd_bp_ta_cntrct_dist`(
        IN p_JobKennung STRING,
        IN p_EintragsNr STRING,
        IN p_Stichtag DATE,
        IN p_wiederanlaufWert STRING
    )
    BEGIN
        IF p_JobKennung = 'FAIL_JOB' THEN
            RAISE USING MESSAGE 'Simulated error in core SQL procedure.';
        END IF;
        -- ... rest of the procedure ...
    END;
    ```

**Action:**
Execute `sp_k_ausd_bp_ta_cntrct_dist` with `p_JobKennung = 'FAIL_JOB'` to trigger the simulated error in the core procedure:
*   `p_JobKennung = 'FAIL_JOB'`
*   `p_EintragsNr = 'ENTRY_007'`
*   `p_Stichtag_raw = '07012023'`
*   `p_wiederanlaufWert = '0'`

**Pass/Fail Criterion:**
1.  The `CALL` statement for `sp_k_ausd_bp_ta_cntrct_dist` completes without raising a Python exception (it should catch the internal error and log it).
2.  A single new record exists in `my_gcp_project.my_bq_dataset.job_tracking_table` with:
    *   `job_id` = `'FAIL_JOB'`
    *   `entry_number` = `'ENTRY_007'`
    *   `key_date` = `DATE('2023-01-07')`
    *   `status` = `'FAILED'`
    *   `record_count` = `NULL`
    *   `error_message` contains the substring "Simulated error in core SQL procedure."

```python
# pytest / Python client assertion
from datetime import date

def test_error_handling_in_core_sql_procedure():
    job_kennung = 'FAIL_JOB' # Trigger error in sp_d_ausd_bp_ta_cntrct_dist
    eintrags_nr = 'ENTRY_007'
    stichtag_raw = '07012023'
    stichtag_parsed = date(2023, 1, 7)
    wiederanlauf_wert = '0'

    # Action: Call the main stored procedure. It should NOT raise an error at this level,
    # but log it internally due to the EXCEPTION block.
    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag_raw}',
        '{wiederanlauf_wert}'
    );
    """
    client.query(call_query).result() # Should complete without Python exception

    # Pass/Fail Criterion: Check job_tracking_table for FAILED status and error message
    tracking_query = f"""
    SELECT job_id, entry_number, key_date, record_count, status, error_message
    FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE job_id = '{job_kennung}' AND entry_number = '{eintrags_nr}'
    ORDER BY start_timestamp DESC LIMIT 1
    """
    tracking_results = list(client.query(tracking_query).result())
    assert len(tracking_results) == 1, "Expected one tracking record for failed job."
    tracking_record = tracking_results[0]
    assert tracking_record['job_id'] == job_kennung
    assert tracking_record['entry_number'] == eintrags_nr
    assert tracking_record['key_date'] == stichtag_parsed
    assert tracking_record['status'] == 'FAILED'
    assert tracking_record['record_count'] is None
    assert "Simulated error in core SQL procedure." in tracking_record['error_message']
```

---

## Test Case 8: Commented-Out Post-Processing (Negative Test)

**Purpose:**
Verify that the commented-out `sed`, `sort`, `join` logic from the original KornShell script, which performed file-based post-processing, is *not* executed or replicated in the migrated BigQuery stored procedure. This confirms that functionality explicitly marked as commented-out in the legacy source and not included in the migration design is indeed absent.

**Setup:**
1.  No specific setup is required beyond the standard BigQuery environment. Ensure no BigQuery tables or Cloud Storage objects exist that would correspond to the output of the commented-out legacy logic (e.g., `cibasis_data24.dat`, `cibasisprodukt.csv`).

**Action:**
Execute `sp_k_ausd_bp_ta_cntrct_dist` with valid parameters.

**Pass/Fail Criterion:**
1.  No BigQuery tables with names resembling the intermediate or final files from the commented-out logic (e.g., `cibasis_data24_sed`, `cibasis_24_96_tmp`, `cibasisprodukt_csv`) are created or modified in `my_gcp_project.my_bq_dataset`.
2.  No new files corresponding to the `cibasis_*.dat` or `cibasis_*.csv` patterns are created in any Cloud Storage buckets associated with the project by the execution of `sp_k_ausd_bp_ta_cntrct_dist`.
3.  The `job_tracking_table` shows a `SUCCESS` status for this execution.

```python
# pytest / Python client assertion (conceptual, as it verifies absence of side effects)
from google.cloud import bigquery, storage

def test_commented_out_post_processing_not_executed():
    job_kennung = 'TEST_JOB_008'
    eintrags_nr = 'ENTRY_008'
    stichtag_raw = '08012023'
    wiederanlauf_wert = '0'

    # Action: Call the main stored procedure
    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_k_ausd_bp_ta_cntrct_dist`(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag_raw}',
        '{wiederanlauf_wert}'
    );
    """
    client.query(call_query).result()

    # Pass/Fail Criterion 1: Assert that specific BigQuery tables are NOT created/modified
    tables_to_check = [
        f"{project_id}.{dataset_id}.cibasis_data24_sed",
        f"{project_id}.{dataset_id}.cibasis_data96_sed",
        f"{project_id}.{dataset_id}.cibasis_fax_sed",
        f"{project_id}.{dataset_id}.cibasis_24_96_tmp",
        f"{project_id}.{dataset_id}.cibasisprodukt_csv"
    ]
    for table_ref_str in tables_to_check:
        try:
            client.get_table(table_ref_str)
            pytest.fail(f"Table {table_ref_str} was unexpectedly created or exists.")
        except Exception as e:
            assert "Not found" in str(e) # Expected error if table does not exist

    # Pass/Fail Criterion 2: Assert that specific Cloud Storage files are NOT created/modified
    # This requires knowledge of the target GCS bucket where such files *might* be written.
    # For demonstration, assume a 'test-output-bucket' and specific prefixes.
    # storage_client = storage.Client(project=project_id)
    # bucket_name = "test-output-bucket" # Replace with actual bucket if applicable
    # try:
    #     bucket = storage_client.get_bucket(bucket_name)
    #     blobs = list(bucket.list_blobs(prefix="cibasis_"))
    #     assert len(blobs) == 0, f"Unexpected files found in GCS bucket {bucket_name}: {[b.name for b in blobs]}"
    # except Exception as e:
    #     # If the bucket doesn't exist, that's also a pass for this negative test.
    #     # If it exists but no files, that's a pass.
    #     pass

    # Pass/Fail Criterion 3: Verify job_tracking_table status
    tracking_query = f"""
    SELECT status FROM `{project_id}.{dataset_id}.job_tracking_table`
    WHERE job_id = '{job_kennung}' AND entry_number = '{eintrags_nr}'
    ORDER BY start_timestamp DESC LIMIT 1
    """
    tracking_results = list(client.query(tracking_query).result())
    assert len(tracking_results) == 1, "Expected one tracking record for status."
    assert tracking_results[0]['status'] == 'SUCCESS', "Job status should be SUCCESS."
```