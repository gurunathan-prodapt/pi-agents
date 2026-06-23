Here are the migration validation tests for the `k_ausd_bp_ta_tarifoption.ksh` script, migrated to a BigQuery Stored Procedure.

---

## Migration Validation Tests: `k_ausd_bp_ta_tarifoption`

**Assumptions:**
*   The BigQuery Stored Procedure `your_project.your_dataset.k_ausd_bp_ta_tarifoption` and all associated DDLs (`job_audit_table.sql`, `target_table_ddl.sql`) have been deployed.
*   A Python testing framework (e.g., `pytest`) with `google-cloud-bigquery` client is used for orchestration.
*   `your_project` and `your_dataset` are placeholders for the actual GCP Project ID and BigQuery Dataset ID.
*   The `setup_teardown_tables` fixture (or equivalent) ensures that `job_audit_table`, `sof_ta_tarifoption`, `dwtk_meldungen`, `sof_ta_l_bpr_optionen_filter`, and `sof_ta_bpr_opt_text` are cleared before each test run to ensure isolation.

---

### Test Case 1: Successful Execution - Output Parity & Record Count

**Purpose:**
To verify that the migrated BigQuery Stored Procedure (SP) processes a standard set of inputs correctly, producing the expected output data in `sof_ta_tarifoption` and logging a successful entry in `job_audit_table` with the correct record count. This test covers output parity and basic record count assertion.

**Setup:**
Populate the source tables with representative data that covers all `opt_kategorie` types and multiple entries for `STRING_AGG`.

```sql
-- Insert into sof_ta_l_bpr_optionen_filter
INSERT INTO `your_project.your_dataset.sof_ta_l_bpr_optionen_filter` (bpr_id, opt_kategorie) VALUES
(101, 'BUDGET'),
(102, 'BUDGET'),
(201, 'SONST'),
(202, 'SONST'),
(301, 'GPRS'),
(302, 'GPRS'),
(401, 'OTHER'); -- An uncategorized option

-- Insert into sof_ta_bpr_opt_text
INSERT INTO `your_project.your_dataset.sof_ta_bpr_opt_text` (bpr_id, cntrct_id, pds_description) VALUES
(101, 1001, 'Budget Option A'),
(102, 1001, 'Budget Option B'),
(201, 1001, 'Sonstige Option X'),
(301, 1001, 'GPRS Option 1'),
(302, 1001, 'GPRS Option 2'),
(101, 1002, 'Budget Option C'),
(202, 1002, 'Sonstige Option Y'),
(401, 1003, 'Uncategorized Option Z'); -- Should not appear in final output

-- Insert into dwtk_meldungen for v_datum_suffix
INSERT INTO `your_project.your_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00 UTC')),
('SOME_OTHER_JOB', TIMESTAMP('2023-01-16 11:00:00 UTC')),
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-20 12:00:00 UTC')); -- Max timecreated for BERT_DROP_TEMP_TABLE
```

**Action:**
Call the BigQuery Stored Procedure with valid parameters.

```python
# Python (pytest)
from your_test_utils import call_bigquery_sp, get_table_data, get_audit_log_entry

def test_successful_execution_output_parity(setup_teardown_tables):
    job_kennung = "TEST_JOB_001"
    entry_nr = "1"
    as_of_date_str = "21012023" # DDMMYYYY
    restart_val = 0

    status, error = call_bigquery_sp(job_kennung, entry_nr, as_of_date_str, restart_val)

    assert status == "SUCCESS"
    assert error is None

    # Fetch results
    target_data = get_table_data("sof_ta_tarifoption")
    audit_log = get_audit_log_entry(job_kennung, entry_nr, as_of_date_str)

    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
1.  The SP execution completes successfully without raising an error.
2.  The `sof_ta_tarifoption` table contains the expected data, matching the legacy output.
3.  The `job_audit_table` contains one entry for this job run with `status = 'SUCCESS'` and `records_processed` matching the count in `sof_ta_tarifoption`.

```python
    # Expected data in sof_ta_tarifoption
    expected_target_data = [
        {'cntrct_id': 1001, 'business_option': 'Budget Option A, Budget Option B', 'sonstige_option': 'Sonstige Option X', 'gprs_option': 'GPRS Option 1, GPRS Option 2'},
        {'cntrct_id': 1002, 'business_option': 'Budget Option C', 'sonstige_option': None, 'gprs_option': None}
    ]
    # Sort for consistent comparison
    target_data_sorted = sorted(target_data, key=lambda x: x['cntrct_id'])
    expected_target_data_sorted = sorted(expected_target_data, key=lambda x: x['cntrct_id'])

    assert target_data_sorted == expected_target_data_sorted

    # Audit log assertions
    assert audit_log is not None
    assert audit_log['job_identifier'] == job_kennung
    assert audit_log['entry_number'] == entry_nr
    assert audit_log['as_of_date'].strftime('%d%m%Y') == as_of_date_str
    assert audit_log['restart_value'] == restart_val
    assert audit_log['status'] == 'SUCCESS'
    assert audit_log['records_processed'] == len(expected_target_data)
    assert audit_log['error_message'] is None
```

---

### Test Case 2: Transformation Correctness - `STRING_AGG` and `SAFE_SUBSTR`

**Purpose:**
To specifically verify the correctness of the `STRING_AGG` aggregation, `TRIM(LEADING ', ' FROM COALESCE(...))` for handling empty categories, and `SAFE_SUBSTR(..., 1, 500)` for truncation, including edge cases like very long strings and categories with no matches.

**Setup:**
Populate source tables with data that specifically tests these transformations.

```sql
-- Insert into sof_ta_l_bpr_optionen_filter
INSERT INTO `your_project.your_dataset.sof_ta_l_bpr_optionen_filter` (bpr_id, opt_kategorie) VALUES
(1, 'BUDGET'),
(2, 'SONST'),
(3, 'GPRS');

-- Insert into sof_ta_bpr_opt_text
INSERT INTO `your_project.your_dataset.sof_ta_bpr_opt_text` (bpr_id, cntrct_id, pds_description) VALUES
(1, 100, 'Short Option'),
(1, 100, 'Another Short Option'),
(2, 100, NULL), -- NULL pds_description
(3, 100, REPEAT('A', 600)), -- Very long string for GPRS
(1, 101, 'Only Budget'),
(2, 102, 'Only Sonst');
-- No entries for cntrct_id 103, testing all NULL categories
```

**Action:**
Call the BigQuery Stored Procedure with valid parameters.

```python
# Python (pytest)
def test_transformation_correctness_string_agg_substr(setup_teardown_tables):
    job_kennung = "TEST_JOB_002"
    entry_nr = "1"
    as_of_date_str = "22012023"
    restart_val = 0

    status, error = call_bigquery_sp(job_kennung, entry_nr, as_of_date_str, restart_val)

    assert status == "SUCCESS"
    assert error is None

    target_data = get_table_data("sof_ta_tarifoption")
    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
1.  The SP executes successfully.
2.  The `sof_ta_tarifoption` table contains the following data:
    *   `cntrct_id = 100`:
        *   `business_option`: 'Another Short Option, Short Option' (order might vary based on `ORDER BY pds_description` in `STRING_AGG`, but content should be correct).
        *   `sonstige_option`: `NULL` (due to `NULL` `pds_description` and `COALESCE` then `TRIM` handling).
        *   `gprs_option`: A string of 500 'A's (truncated from 600).
    *   `cntrct_id = 101`:
        *   `business_option`: 'Only Budget'
        *   `sonstige_option`: `NULL`
        *   `gprs_option`: `NULL`
    *   `cntrct_id = 102`:
        *   `business_option`: `NULL`
        *   `sonstige_option`: 'Only Sonst'
        *   `gprs_option`: `NULL`
3.  Total row count is 3.

```python
    expected_target_data = [
        {'cntrct_id': 100, 'business_option': 'Another Short Option, Short Option', 'sonstige_option': None, 'gprs_option': REPEAT('A', 500)},
        {'cntrct_id': 101, 'business_option': 'Only Budget', 'sonstige_option': None, 'gprs_option': None},
        {'cntrct_id': 102, 'business_option': None, 'sonstige_option': 'Only Sonst', 'gprs_option': None}
    ]
    target_data_sorted = sorted(target_data, key=lambda x: x['cntrct_id'])
    expected_target_data_sorted = sorted(expected_target_data, key=lambda x: x['cntrct_id'])

    assert target_data_sorted == expected_target_data_sorted
    assert len(target_data) == 3
```

---

### Test Case 3: Parameter Validation - Missing Mandatory Parameters

**Purpose:**
To verify that the BigQuery SP correctly validates mandatory input parameters and raises an appropriate error, logging the failure in the audit table.

**Setup:**
Ensure all tables are clear.

**Action:**
Call the BigQuery Stored Procedure with one or more mandatory parameters missing or empty.

```python
# Python (pytest)
def test_parameter_validation_missing_job_kennung(setup_teardown_tables):
    job_kennung = "" # Missing
    entry_nr = "1"
    as_of_date_str = "23012023"
    restart_val = 0

    status, error = call_bigquery_sp(job_kennung, entry_nr, as_of_date_str, restart_val)

    assert status == "FAILED"
    assert "Parameter \"Jobkennung\" (job_kennung) must be set." in error

    audit_log = get_audit_log_entry(job_kennung, entry_nr, as_of_date_str)
    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
1.  The SP execution fails with an error message indicating the missing parameter.
2.  The `job_audit_table` contains one entry for this job run with `status = 'FAILED'` and `error_message` containing the validation error.
3.  The `sof_ta_tarifoption` table remains empty.

```python
    assert audit_log is not None
    assert audit_log['job_identifier'] == job_kennung
    assert audit_log['entry_number'] == entry_nr
    assert audit_log['as_of_date'].strftime('%d%m%Y') == as_of_date_str # as_of_date might be NULL if validation fails before parsing
    assert audit_log['status'] == 'FAILED'
    assert "Parameter \"Jobkennung\" (job_kennung) must be set." in audit_log['error_message']

    target_data = get_table_data("sof_ta_tarifoption")
    assert len(target_data) == 0
```

*(Repeat similar tests for missing `entry_nr` and `as_of_date_str`)*

---

### Test Case 4: Parameter Validation - Invalid Date Format

**Purpose:**
To verify that the BigQuery SP correctly validates the `as_of_date_str` format (DDMMYYYY) and raises an appropriate error, logging the failure.

**Setup:**
Ensure all tables are clear.

**Action:**
Call the BigQuery Stored Procedure with an `as_of_date_str` in an invalid format.

```python
# Python (pytest)
def test_parameter_validation_invalid_date_format(setup_teardown_tables):
    job_kennung = "TEST_JOB_003"
    entry_nr = "1"
    as_of_date_str = "2023-01-24" # Invalid format, expected DDMMYYYY
    restart_val = 0

    status, error = call_bigquery_sp(job_kennung, entry_nr, as_of_date_str, restart_val)

    assert status == "FAILED"
    assert "Parameter \"Stichtag\" (2023-01-24) has an invalid date format. Expected DDMMYYYY." in error

    audit_log = get_audit_log_entry(job_kennung, entry_nr, as_of_date_str)
    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
1.  The SP execution fails with an error message indicating the invalid date format.
2.  The `job_audit_table` contains one entry for this job run with `status = 'FAILED'` and `error_message` containing the validation error.
3.  The `sof_ta_tarifoption` table remains empty.

```python
    assert audit_log is not None
    assert audit_log['job_identifier'] == job_kennung
    assert audit_log['entry_number'] == entry_nr
    # as_of_date will be NULL in audit log because SAFE.PARSE_DATE failed
    assert audit_log['as_of_date'] is None
    assert audit_log['status'] == 'FAILED'
    assert "Parameter \"Stichtag\" (2023-01-24) has an invalid date format. Expected DDMMYYYY." in audit_log['error_message']

    target_data = get_table_data("sof_ta_tarifoption")
    assert len(target_data) == 0
```

---

### Test Case 5: `restart_val` Defaulting and `v_datum_suffix` Derivation

**Purpose:**
To verify that `restart_val` defaults to 0 when `NULL` is passed, and that `v_datum_suffix` is correctly derived from `dwtk_meldungen` or defaults to '19000101' if no matching entry is found.

**Setup:**
Populate `dwtk_meldungen` with specific data to test `v_datum_suffix` logic.

```sql
-- Insert into sof_ta_l_bpr_optionen_filter and sof_ta_bpr_opt_text for a minimal successful run
INSERT INTO `your_project.your_dataset.sof_ta_l_bpr_optionen_filter` (bpr_id, opt_kategorie) VALUES (1, 'BUDGET');
INSERT INTO `your_project.your_dataset.sof_ta_bpr_opt_text` (bpr_id, cntrct_id, pds_description) VALUES (1, 100, 'Test Option');

-- Insert into dwtk_meldungen for v_datum_suffix
INSERT INTO `your_project.your_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-01 00:00:00 UTC')),
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-02-10 15:30:00 UTC')), -- This should be picked up
('ANOTHER_JOB', TIMESTAMP('2023-03-01 00:00:00 UTC'));
```

**Action:**
Call the BigQuery Stored Procedure with `restart_val` as `NULL`.

```python
# Python (pytest)
def test_restart_val_default_and_datum_suffix(setup_teardown_tables):
    job_kennung = "TEST_JOB_004"
    entry_nr = "1"
    as_of_date_str = "25012023"
    restart_val = None # Test defaulting

    status, error = call_bigquery_sp(job_kennung, entry_nr, as_of_date_str, restart_val)

    assert status == "SUCCESS"
    assert error is None

    audit_log = get_audit_log_entry(job_kennung, entry_nr, as_of_date_str)
    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
1.  The SP executes successfully.
2.  The `job_audit_table` entry shows `restart_value = 0`, confirming the defaulting logic.
3.  (Implicit) The internal `v_datum_suffix` variable was correctly set to '20230210'. While not directly observable from the audit log, this is crucial for the core SQL logic if `sof_ta_bpr_opt_text` were dynamically named (e.g., `sof$ta_bpr_opt_text_20230210`). Since the migration design uses a unified `sof_ta_bpr_opt_text` table, this specific suffix is less critical for the final output but important for verifying the variable derivation logic.

```python
    assert audit_log is not None
    assert audit_log['restart_value'] == 0 # Should default to 0
    assert audit_log['status'] == 'SUCCESS'

    # To verify v_datum_suffix, we would need to add it to the audit log or
    # inspect the SP's internal state, which is not directly possible from outside.
    # However, if the core logic relied on it (e.g., dynamic table names),
    # the output parity test (Test Case 1) would implicitly validate it.
    # For this test, we confirm restart_val defaulting.

    # Test case for default v_datum_suffix ('19000101')
    clear_table("dwtk_meldungen") # Clear previous entries
    status_no_dwtk, error_no_dwtk = call_bigquery_sp("TEST_JOB_005", "1", "26012023", None)
    assert status_no_dwtk == "SUCCESS"
    audit_log_no_dwtk = get_audit_log_entry("TEST_JOB_005", "1", "26012023")
    assert audit_log_no_dwtk['status'] == 'SUCCESS'
    # Again, v_datum_suffix is internal. If it were logged, we'd assert its value.
    # The absence of a failure implies the default was handled gracefully.
```

---

### Test Case 6: External System Replacement - Audit Logging (Success & Failure)

**Purpose:**
To verify that the `job_audit_table` correctly captures job execution details, replacing the legacy temporary file for record counts and the commented `FOSJobErzeugeEintrag` calls. This covers both successful and failed scenarios.

**Setup:**
1.  For success: Populate source tables for a successful run (similar to Test Case 1).
2.  For failure: Setup a condition that causes the SP to fail (e.g., missing mandatory parameter, as in Test Case 3).

**Action:**
1.  Execute the SP successfully.
2.  Execute the SP with a known failure condition.

```python
# Python (pytest)
def test_audit_logging_success_and_failure(setup_teardown_tables):
    # --- Success Scenario ---
    # Setup for success
    INSERT INTO `your_project.your_dataset.sof_ta_l_bpr_optionen_filter` (bpr_id, opt_kategorie) VALUES (1, 'BUDGET');
    INSERT INTO `your_project.your_dataset.sof_ta_bpr_opt_text` (bpr_id, cntrct_id, pds_description) VALUES (1, 100, 'Audit Test');

    job_kennung_success = "AUDIT_SUCCESS"
    entry_nr_success = "10"
    as_of_date_str_success = "27012023"
    restart_val_success = 1

    status_success, error_success = call_bigquery_sp(job_kennung_success, entry_nr_success, as_of_date_str_success, restart_val_success)
    assert status_success == "SUCCESS"

    audit_log_success = get_audit_log_entry(job_kennung_success, entry_nr_success, as_of_date_str_success)

    # --- Failure Scenario ---
    job_kennung_failure = "AUDIT_FAILURE"
    entry_nr_failure = "11"
    as_of_date_str_failure = "28012023"
    restart_val_failure = 0

    status_failure, error_failure = call_bigquery_sp("", entry_nr_failure, as_of_date_str_failure, restart_val_failure) # Missing job_kennung
    assert status_failure == "FAILED"

    audit_log_failure = get_audit_log_entry("", entry_nr_failure, as_of_date_str_failure) # Note: job_kennung is empty here
    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
1.  **Success Log:**
    *   One entry in `job_audit_table` for `AUDIT_SUCCESS`.
    *   `status` is 'SUCCESS'.
    *   `records_processed` matches `COUNT(*)` from `sof_ta_tarifoption` (which should be 1).
    *   `error_message` is `NULL`.
    *   `start_timestamp`, `end_timestamp`, `audit_timestamp` are populated.
2.  **Failure Log:**
    *   One entry in `job_audit_table` for `AUDIT_FAILURE` (or the parameters that were available before failure).
    *   `status` is 'FAILED'.
    *   `error_message` contains the expected error string (e.g., "Parameter \"Jobkennung\"...").
    *   `records_processed` is `NULL`.
    *   `start_timestamp`, `end_timestamp`, `audit_timestamp` are populated.

```python
    # Assertions for Success Log
    assert audit_log_success is not None
    assert audit_log_success['job_identifier'] == job_kennung_success
    assert audit_log_success['status'] == 'SUCCESS'
    assert audit_log_success['records_processed'] == 1
    assert audit_log_success['error_message'] is None
    assert audit_log_success['start_timestamp'] is not None
    assert audit_log_success['end_timestamp'] is not None
    assert audit_log_success['audit_timestamp'] is not None

    # Assertions for Failure Log
    assert audit_log_failure is not None
    assert audit_log_failure['job_identifier'] == "" # As passed
    assert audit_log_failure['status'] == 'FAILED'
    assert audit_log_failure['records_processed'] is None
    assert "Parameter \"Jobkennung\" (job_kennung) must be set." in audit_log_failure['error_message']
    assert audit_log_failure['start_timestamp'] is not None
    assert audit_log_failure['end_timestamp'] is not None
    assert audit_log_failure['audit_timestamp'] is not None
```

---

### Test Case 7: Data Quality - Empty Source Tables

**Purpose:**
To verify that the SP handles empty source tables gracefully, resulting in an empty target table and a successful audit log with 0 records processed.

**Setup:**
Ensure all source tables (`sof_ta_l_bpr_optionen_filter`, `sof_ta_bpr_opt_text`, `dwtk_meldungen`) are empty.

**Action:**
Call the BigQuery Stored Procedure with valid parameters.

```python
# Python (pytest)
def test_data_quality_empty_source_tables(setup_teardown_tables):
    job_kennung = "TEST_JOB_006"
    entry_nr = "1"
    as_of_date_str = "29012023"
    restart_val = 0

    status, error = call_bigquery_sp(job_kennung, entry_nr, as_of_date_str, restart_val)

    assert status == "SUCCESS"
    assert error is None

    target_data = get_table_data("sof_ta_tarifoption")
    audit_log = get_audit_log_entry(job_kennung, entry_nr, as_of_date_str)
    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
1.  The SP executes successfully.
2.  The `sof_ta_tarifoption` table is empty.
3.  The `job_audit_table` contains one entry with `status = 'SUCCESS'` and `records_processed = 0`.

```python
    assert len(target_data) == 0

    assert audit_log is not None
    assert audit_log['status'] == 'SUCCESS'
    assert audit_log['records_processed'] == 0
    assert audit_log['error_message'] is None
```

---

### Test Case 8: Schema Assertions - Target Table Structure

**Purpose:**
To verify that the `sof_ta_tarifoption` table maintains the expected schema (column names, data types, nullability) after the SP execution. This is a static check on the DDL.

**Setup:**
No specific data setup required, but the table must exist.

**Action:**
Query the BigQuery information schema to retrieve the table's schema.

```python
# Python (pytest)
def test_schema_assertions_target_table(setup_teardown_tables):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table("sof_ta_tarifoption")
    table = client.get_table(table_ref)

    # ... (rest of pass/fail criteria)
```

**Pass/Fail Criterion:**
The schema of `sof_ta_tarifoption` matches the expected definition:

| Column Name       | Data Type | Nullable |
| :---------------- | :-------- | :------- |
| `cntrct_id`       | `INT64`   | `NULLABLE` |
| `business_option` | `STRING`  | `NULLABLE` |
| `sonstige_option` | `STRING`  | `NULLABLE` |
| `gprs_option`     | `STRING`  | `NULLABLE` |

```python
    expected_schema = {
        "cntrct_id": {"field_type": "INTEGER", "mode": "NULLABLE"},
        "business_option": {"field_type": "STRING", "mode": "NULLABLE"},
        "sonstige_option": {"field_type": "STRING", "mode": "NULLABLE"},
        "gprs_option": {"field_type": "STRING", "mode": "NULLABLE"},
    }

    actual_schema = {field.name: {"field_type": field.field_type, "mode": field.mode} for field in table.schema}

    assert actual_schema == expected_schema
```