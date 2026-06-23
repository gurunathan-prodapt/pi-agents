As a senior data-migration QA engineer, I've analyzed the provided migration design and the generated BigQuery code for `k_ausd_v_ta_period.ksh`. The migration aims to translate a KornShell orchestration script into a BigQuery Stored Procedure, including parameter handling, job status management, and core SQL logic.

Below are the migration validation tests, structured to cover output parity, transformation correctness, external system replacements, and data quality/schema assertions. Each test case includes its purpose, setup, action, and a concrete pass/fail criterion, along with runnable `pytest` code examples.

---

### Test Setup & Helper Functions

The following `pytest` fixtures and helper functions are assumed for the runnable test code. They provide a clean environment for each test and simplify interactions with BigQuery.

```python
import pytest
from google.cloud import bigquery
import datetime
import time
import os

# --- Configuration ---
# Replace with your actual GCP project ID and dataset ID
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")

# Fully qualified table and procedure names
JOB_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.job_table`"
TA_PERIOD_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.ta_period`"
DWTK_MELDUNGEN_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen`"
CDS_TA_PERIOD_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.cds_ta_period`"
CDS_TA_TIME_MEAS_CV_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.cds_ta_time_meas_cv`"
CDS_TA_DESCRIPTION_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.cds_ta_description`"
STORED_PROCEDURE = f"`{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def setup_teardown_tables(bq_client):
    """
    Fixture to ensure all relevant tables are truncated before and after each test.
    This provides test isolation.
    """
    _clear_all_test_tables(bq_client)
    yield  # Run the test
    _clear_all_test_tables(bq_client)

def _clear_all_test_tables(bq_client):
    """Helper to clear data from all relevant tables."""
    bq_client.query(f"TRUNCATE TABLE {JOB_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {TA_PERIOD_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {DWTK_MELDUNGEN_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {CDS_TA_PERIOD_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {CDS_TA_TIME_MEAS_CV_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {CDS_TA_DESCRIPTION_TABLE}").result()

def _insert_dwtk_meldungen(bq_client, job_kennung, timecreated):
    """Inserts a record into the dwtk_meldungen table."""
    query = f"""
    INSERT INTO {DWTK_MELDUNGEN_TABLE} (job_kennung, timecreated)
    VALUES ('{job_kennung}', TIMESTAMP('{timecreated}'))
    """
    bq_client.query(query).result()

def _insert_cds_ta_period(bq_client, period_id, number_time_measurement, time_meas_cv, insert_at, modified_at=None):
    """Inserts a record into the cds_ta_period table."""
    modified_at_str = f"TIMESTAMP('{modified_at}')" if modified_at else "NULL"
    query = f"""
    INSERT INTO {CDS_TA_PERIOD_TABLE} (period_id, number_time_measurement, time_meas_cv, insert_at, modified_at)
    VALUES ({period_id}, {number_time_measurement}, '{time_meas_cv}', TIMESTAMP('{insert_at}'), {modified_at_str})
    """
    bq_client.query(query).result()

def _insert_cds_ta_time_meas_cv(bq_client, time_meas_cv, description_id):
    """Inserts a record into the cds_ta_time_meas_cv table."""
    query = f"""
    INSERT INTO {CDS_TA_TIME_MEAS_CV_TABLE} (time_meas_cv, description_id)
    VALUES ('{time_meas_cv}', {description_id})
    """
    bq_client.query(query).result()

def _insert_cds_ta_description(bq_client, description_id, description):
    """Inserts a record into the cds_ta_description table."""
    query = f"""
    INSERT INTO {CDS_TA_DESCRIPTION_TABLE} (description_id, description)
    VALUES ({description_id}, '{description}')
    """
    bq_client.query(query).result()

def _call_stored_procedure(bq_client, job_kennung, eintrags_nr):
    """Executes the BigQuery Stored Procedure."""
    query = f"CALL {STORED_PROCEDURE}('{job_kennung}', '{eintrags_nr}')"
    return bq_client.query(query).result()

def _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status=None):
    """Fetches a specific job entry from the job_table."""
    status_filter = f"AND status = '{status}'" if status else ""
    query = f"""
    SELECT * FROM {JOB_TABLE}
    WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
    {status_filter}
    ORDER BY created_ts DESC
    LIMIT 1
    """
    rows = list(bq_client.query(query).result())
    return rows[0] if rows else None

def _get_ta_period_data(bq_client):
    """Fetches all data from the ta_period table."""
    query = f"SELECT * FROM {TA_PERIOD_TABLE} ORDER BY period_id"
    return list(bq_client.query(query).result())

def _get_ta_period_row_count(bq_client):
    """Fetches the row count from the ta_period table."""
    query = f"SELECT COUNT(*) FROM {TA_PERIOD_TABLE}"
    return list(bq_client.query(query).result())[0][0]

```

---

### Test Case 1: Parameter Validation - Missing JobKennung

**Purpose**: Verify that the stored procedure correctly handles a missing or empty `p_JobKennung` parameter, raising the expected error. This corresponds to the legacy script's `pruefeParameterGesetzt Jobkennung p_JobKennung` check.

**Setup**:
*   Ensure all target tables (`job_table`, `ta_period`) are empty.
*   No specific data needed in source tables for this validation test.

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with an empty `p_JobKennung` and a valid `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The stored procedure call should fail with an error message containing "FEHLER: 0 E 193 Jobkennung".
*   No records should be inserted into `job_table` or `ta_period`.

**Runnable Test Code (pytest)**:
```python
def test_missing_jobkennung_parameter(bq_client):
    job_kennung = ""
    eintrags_nr = "001"
    
    with pytest.raises(Exception) as excinfo:
        _call_stored_procedure(bq_client, job_kennung, eintrags_nr)
    
    assert "FEHLER: 0 E 193 Jobkennung" in str(excinfo.value)
    assert _get_job_table_entry(bq_client, job_kennung, eintrags_nr) is None
    assert _get_ta_period_row_count(bq_client) == 0
```

---

### Test Case 2: Parameter Validation - Missing EintragsNr

**Purpose**: Verify that the stored procedure correctly handles a missing or empty `p_EintragsNr` parameter, raising the expected error. This corresponds to the legacy script's `pruefeParameterGesetzt EintragsNr p_EintragsNr` check.

**Setup**:
*   Ensure all target tables (`job_table`, `ta_period`) are empty.
*   No specific data needed in source tables for this validation test.

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with a valid `p_JobKennung` and an empty `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The stored procedure call should fail with an error message containing "FEHLER: 0 E 193 EintragsNr".
*   No records should be inserted into `job_table` or `ta_period`.

**Runnable Test Code (pytest)**:
```python
def test_missing_eintragsnr_parameter(bq_client):
    job_kennung = "TEST_JOB_01"
    eintrags_nr = ""
    
    with pytest.raises(Exception) as excinfo:
        _call_stored_procedure(bq_client, job_kennung, eintrags_nr)
    
    assert "FEHLER: 0 E 193 EintragsNr" in str(excinfo.value)
    assert _get_job_table_entry(bq_client, job_kennung, eintrags_nr) is None
    assert _get_ta_period_row_count(bq_client) == 0
```

---

### Test Case 3: Job Status Management - Initial Run Success

**Purpose**: Verify that a successful execution of the stored procedure correctly updates the `job_table` with `RUNNING` and then `DONE` statuses, and captures the correct record count. This covers the basic job orchestration and record count capture.

**Setup**:
*   Ensure `job_table` and `ta_period` are empty.
*   Populate source tables (`dwtk_meldungen`, `cds_ta_period`, `cds_ta_time_meas_cv`, `cds_ta_description`) with data that will result in a known number of records being inserted into `ta_period`.

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

**Pass/Fail Criterion**:
*   A record for the job should exist in `job_table` with `job_kennung = 'TEST_JOB_02'`, `eintrags_nr = '002'`, `status = 'DONE'`, `active_flag = FALSE`.
*   The `record_count` in this `job_table` entry should match the number of rows inserted into `ta_period`.
*   The `ta_period` table should contain the expected number of rows.

**Runnable Test Code (pytest)**:
```python
def test_successful_job_execution(bq_client):
    job_kennung = "TEST_JOB_02"
    eintrags_nr = "002"
    
    # Setup source data
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-01-15 10:00:00 UTC")
    
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_A", 101)
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_B", 102)
    _insert_cds_ta_description(bq_client, 101, "Description A")
    _insert_cds_ta_description(bq_client, 102, "Description B")

    # Data that should be inserted (insert_at <= 2023-01-15, modified_at IS NULL or > 2023-01-15)
    _insert_cds_ta_period(bq_client, 1, 10, "TM_CV_A", "2023-01-10 00:00:00 UTC", None) # Match
    _insert_cds_ta_period(bq_client, 2, 20, "TM_CV_B", "2023-01-15 23:59:59 UTC", "2023-01-16 00:00:00 UTC") # Match
    _insert_cds_ta_period(bq_client, 3, 30, "TM_CV_A", "2023-01-15 00:00:00 UTC", None) # Match
    
    # Data that should NOT be inserted (insert_at > 2023-01-15)
    _insert_cds_ta_period(bq_client, 4, 40, "TM_CV_B", "2023-01-16 00:00:00 UTC", None) # No Match (insert_at too late)
    
    # Data that should NOT be inserted (modified_at <= 2023-01-15)
    _insert_cds_ta_period(bq_client, 5, 50, "TM_CV_A", "2023-01-10 00:00:00 UTC", "2023-01-10 00:00:00 UTC") # No Match (modified_at too early)

    expected_records = 3 # Based on setup data

    # Action
    _call_stored_procedure(bq_client, job_kennung, eintrags_nr)

    # Assertions
    job_entry = _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status='DONE')
    assert job_entry is not None
    assert job_entry.status == 'DONE'
    assert job_entry.active_flag is False
    assert job_entry.record_count == expected_records
    assert job_entry.script_name == 'd_ausd_v_ta_period.sql'
    assert job_entry.tab_name == 'ta_period'

    ta_period_data = _get_ta_period_data(bq_client)
    assert len(ta_period_data) == expected_records
    
    # Verify content of inserted data (example for one row)
    inserted_row_1 = next((row for row in ta_period_data if row.period_id == 1), None)
    assert inserted_row_1 is not None
    assert inserted_row_1.number_time_measurement == 10
    assert inserted_row_1.time_meas_cv == "TM_CV_A"
    assert inserted_row_1.einheit == "Description A"
    assert inserted_row_1.bfc_age == datetime.datetime(2023, 1, 10, 0, 0, tzinfo=datetime.timezone.utc) # insert_at from source
```

---

### Test Case 4: Job Status Management - Deactivate Old Active Jobs

**Purpose**: Verify that the stored procedure correctly deactivates any previously active jobs for the same `JobKennung` before starting a new one. This directly tests the `UPDATE ... SET active_flag = FALSE, status = 'DEACTIVATED' WHERE job_kennung = p_JobKennung AND active_flag = TRUE;` logic.

**Setup**:
*   Ensure `job_table` and `ta_period` are empty.
*   Insert an "active" job record into `job_table` for `TEST_JOB_03` with `eintrags_nr = '001'`, `status = 'RUNNING'`, `active_flag = TRUE`.
*   Populate source tables for a successful run (similar to Test Case 3).

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with `p_JobKennung = 'TEST_JOB_03'` and a *new* `p_EintragsNr = '002'`.

**Pass/Fail Criterion**:
*   The old job record (`job_kennung = 'TEST_JOB_03'`, `eintrags_nr = '001'`) should have `status = 'DEACTIVATED'` and `active_flag = FALSE`.
*   A new job record (`job_kennung = 'TEST_JOB_03'`, `eintrags_nr = '002'`) should exist with `status = 'DONE'` and `active_flag = FALSE`.
*   The `record_count` for the new job should be correct.

**Runnable Test Code (pytest)**:
```python
def test_deactivate_old_active_jobs(bq_client):
    job_kennung = "TEST_JOB_03"
    old_eintrags_nr = "001"
    new_eintrags_nr = "002"

    # Setup: Insert an old active job
    bq_client.query(f"""
    INSERT INTO {JOB_TABLE} (job_kennung, eintrags_nr, script_name, tab_name, status, active_flag, created_ts, updated_ts)
    VALUES ('{job_kennung}', '{old_eintrags_nr}', 'd_ausd_v_ta_period.sql', 'ta_period', 'RUNNING', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """).result()

    # Setup source data for new run
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-01-15 10:00:00 UTC")
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_C", 103)
    _insert_cds_ta_description(bq_client, 103, "Description C")
    _insert_cds_ta_period(bq_client, 6, 60, "TM_CV_C", "2023-01-10 00:00:00 UTC", None)
    expected_records = 1

    # Action: Run the new job
    _call_stored_procedure(bq_client, job_kennung, new_eintrags_nr)

    # Assertions
    old_job_entry = _get_job_table_entry(bq_client, job_kennung, old_eintrags_nr, status='DEACTIVATED')
    assert old_job_entry is not None
    assert old_job_entry.status == 'DEACTIVATED'
    assert old_job_entry.active_flag is False

    new_job_entry = _get_job_table_entry(bq_client, job_kennung, new_eintrags_nr, status='DONE')
    assert new_job_entry is not None
    assert new_job_entry.status == 'DONE'
    assert new_job_entry.active_flag is False
    assert new_job_entry.record_count == expected_records
    assert _get_ta_period_row_count(bq_client) == expected_records
```

---

### Test Case 5: Job Status Management - Job Failure

**Purpose**: Verify that if an error occurs during the core SQL execution, the `job_table` is updated with `status = 'FAILED'` and `active_flag = FALSE`, and the error message is captured. This tests the `EXCEPTION WHEN ERROR THEN ...` block.

**Setup**:
*   Ensure `job_table` and `ta_period` are empty.
*   Populate source tables in a way that will cause a BigQuery runtime error during the `INSERT` statement. For instance, by attempting to insert a `NULL` value into a `NOT NULL` column (`ta_period.einheit` is assumed to be `NOT NULL` for this test).

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The stored procedure call should fail with an error.
*   A record for the job should exist in `job_table` with `job_kennung = 'TEST_JOB_04'`, `eintrags_nr = '001'`, `status = 'FAILED'`, `active_flag = FALSE`.
*   The `error_message` field in `job_table` should contain details about the error (e.g., "NULL value in column 'einheit' violates NOT NULL constraint").
*   `ta_period` should be empty (due to `TRUNCATE` followed by a failed `INSERT`).

**Runnable Test Code (pytest)**:
```python
def test_job_failure_updates_status(bq_client):
    job_kennung = "TEST_JOB_04"
    eintrags_nr = "001"

    # Setup source data to cause a NULL into NOT NULL error (assuming ta_period.einheit is NOT NULL)
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-01-15 10:00:00 UTC")
    
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_E", 999) # description_id 999
    # DO NOT insert into cds_ta_description for description_id 999.
    # This will cause the JOIN to cds_ta_description to produce NULL for d.description.
    
    _insert_cds_ta_period(bq_client, 8, 80, "TM_CV_E", "2023-01-10 00:00:00 UTC", None) # This row will attempt to insert NULL into einheit

    # Action: Call the SP, expecting it to fail
    with pytest.raises(Exception) as excinfo:
        _call_stored_procedure(bq_client, job_kennung, eintrags_nr)
    
    # Assertions
    # The error message should indicate a NOT NULL constraint violation or similar.
    assert "NULL value in column 'einheit' violates NOT NULL constraint" in str(excinfo.value) or \
           "Cannot insert NULL into a NOT NULL column" in str(excinfo.value) # BigQuery error message might vary

    # Check job_table status
    job_entry = _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status='FAILED')
    assert job_entry is not None
    assert job_entry.status == 'FAILED'
    assert job_entry.active_flag is False
    assert job_entry.error_message is not None
    assert "NULL" in job_entry.error_message # Check for some error content related to NULL

    # ta_period should be empty because TRUNCATE happened, but INSERT failed.
    assert _get_ta_period_row_count(bq_client) == 0
```

---

### Test Case 6: Transformation Correctness - `v_datum` Calculation (Normal Case)

**Purpose**: Verify that `v_datum` is correctly derived from `dwtk_meldungen` when data exists. This tests the `MAX(m.timecreated)` and `IFNULL(FORMAT_DATE(...), '19000101')` logic.

**Setup**:
*   Ensure `job_table` and `ta_period` are empty.
*   Insert multiple entries into `dwtk_meldungen` for `BERT_DROP_TEMP_TABLE` with varying `timecreated` values.
*   Populate other source tables (`cds_ta_period`, etc.) such that some rows match the expected `v_datum` filter and some do not, to verify the filter.

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The `ta_period` table should contain only those records from `cds_ta_period` where `p.insert_at <= MAX(dwtk_meldungen.timecreated)` (converted to date) and `(p.modified_at IS NULL OR p.modified_at > MAX(dwtk_meldungen.timecreated))`.
*   The `record_count` in `job_table` should reflect this filtered count.

**Runnable Test Code (pytest)**:
```python
def test_v_datum_calculation_normal(bq_client):
    job_kennung = "TEST_JOB_05"
    eintrags_nr = "001"

    # Setup dwtk_meldungen to determine v_datum
    _insert_dwtk_meldungen(bq_client, "OTHER_JOB", "2022-12-01 00:00:00 UTC") # Irrelevant job
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-01-01 10:00:00 UTC")
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-01-20 15:30:00 UTC") # This should be MAX
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-01-10 05:00:00 UTC")
    
    expected_v_datum_date = datetime.date(2023, 1, 20) # From MAX timecreated

    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_F", 105)
    _insert_cds_ta_description(bq_client, 105, "Description F")

    # Data matching v_datum filter
    _insert_cds_ta_period(bq_client, 9, 90, "TM_CV_F", "2023-01-19 00:00:00 UTC", None) # Match
    _insert_cds_ta_period(bq_client, 10, 100, "TM_CV_F", "2023-01-20 23:59:59 UTC", "2023-01-21 00:00:00 UTC") # Match
    _insert_cds_ta_period(bq_client, 11, 110, "TM_CV_F", "2023-01-20 00:00:00 UTC", None) # Match

    # Data NOT matching v_datum filter
    _insert_cds_ta_period(bq_client, 12, 120, "TM_CV_F", "2023-01-21 00:00:00 UTC", None) # insert_at > v_datum
    _insert_cds_ta_period(bq_client, 13, 130, "TM_CV_F", "2023-01-10 00:00:00 UTC", "2023-01-10 00:00:00 UTC") # modified_at <= v_datum

    expected_records = 3

    # Action
    _call_stored_procedure(bq_client, job_kennung, eintrags_nr)

    # Assertions
    job_entry = _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status='DONE')
    assert job_entry.record_count == expected_records
    assert _get_ta_period_row_count(bq_client) == expected_records
    
    ta_period_data = _get_ta_period_data(bq_client)
    # Verify specific rows are present and others are not
    assert any(row.period_id == 9 for row in ta_period_data)
    assert any(row.period_id == 10 for row in ta_period_data)
    assert any(row.period_id == 11 for row in ta_period_data)
    assert not any(row.period_id == 12 for row in ta_period_data)
    assert not any(row.period_id == 13 for row in ta_period_data)
```

---

### Test Case 7: Transformation Correctness - `v_datum` Calculation (Empty `dwtk_meldungen`)

**Purpose**: Verify that `v_datum` defaults to '19000101' when no relevant entries exist in `dwtk_meldungen`. This tests the `IFNULL(..., '19000101')` part.

**Setup**:
*   Ensure `job_table` and `ta_period` are empty.
*   Ensure `dwtk_meldungen` is empty or contains no entries for `BERT_DROP_TEMP_TABLE`.
*   Populate other source tables (`cds_ta_period`, etc.) such that some rows match the `19000101` `v_datum` filter and some do not.

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The `ta_period` table should contain only those records from `cds_ta_period` where `p.insert_at <= '1900-01-01'` and `(p.modified_at IS NULL OR p.modified_at > '1900-01-01')`.
*   The `record_count` in `job_table` should reflect this filtered count.

**Runnable Test Code (pytest)**:
```python
def test_v_datum_calculation_empty_dwtk_meldungen(bq_client):
    job_kennung = "TEST_JOB_06"
    eintrags_nr = "001"

    # Setup: dwtk_meldungen is empty (or no BERT_DROP_TEMP_TABLE entries)
    # _clear_all_test_tables fixture ensures this.
    
    expected_v_datum_date = datetime.date(1900, 1, 1) # Default value

    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_G", 106)
    _insert_cds_ta_description(bq_client, 106, "Description G")

    # Data matching v_datum filter (insert_at <= 1900-01-01)
    _insert_cds_ta_period(bq_client, 14, 140, "TM_CV_G", "1900-01-01 00:00:00 UTC", None) # Match
    
    # Data NOT matching v_datum filter
    _insert_cds_ta_period(bq_client, 15, 150, "TM_CV_G", "1900-01-02 00:00:00 UTC", None) # insert_at > v_datum
    _insert_cds_ta_period(bq_client, 16, 160, "TM_CV_G", "1899-12-31 00:00:00 UTC", "1899-12-31 00:00:00 UTC") # modified_at <= v_datum

    expected_records = 1

    # Action
    _call_stored_procedure(bq_client, job_kennung, eintrags_nr)

    # Assertions
    job_entry = _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status='DONE')
    assert job_entry.record_count == expected_records
    assert _get_ta_period_row_count(bq_client) == expected_records
    
    ta_period_data = _get_ta_period_data(bq_client)
    assert any(row.period_id == 14 for row in ta_period_data)
    assert not any(row.period_id == 15 for row in ta_period_data)
    assert not any(row.period_id == 16 for row in ta_period_data)
```

---

### Test Case 8: Transformation Correctness - `TRUNCATE` Behavior

**Purpose**: Verify that the `TRUNCATE TABLE ta_period` statement correctly clears the target table before new data is inserted.

**Setup**:
*   Ensure `job_table` is empty.
*   Pre-populate `ta_period` with some dummy data.
*   Populate source tables (`dwtk_meldungen`, `cds_ta_period`, etc.) for a successful run that will insert new data.

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The `ta_period` table should *only* contain the data inserted by the current run, and none of the pre-existing dummy data.
*   The `record_count` in `job_table` should match the count of newly inserted rows, not including the truncated data.

**Runnable Test Code (pytest)**:
```python
def test_truncate_behavior(bq_client):
    job_kennung = "TEST_JOB_07"
    eintrags_nr = "001"

    # Setup: Pre-populate ta_period with dummy data
    bq_client.query(f"""
    INSERT INTO {TA_PERIOD_TABLE} (period_id, number_time_measurement, time_meas_cv, einheit, bfc_age)
    VALUES (999, 1, 'DUMMY', 'Dummy Unit', CURRENT_TIMESTAMP())
    """).result()
    assert _get_ta_period_row_count(bq_client) == 1

    # Setup source data for new run
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-02-01 10:00:00 UTC")
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_H", 107)
    _insert_cds_ta_description(bq_client, 107, "Description H")
    _insert_cds_ta_period(bq_client, 17, 170, "TM_CV_H", "2023-01-25 00:00:00 UTC", None)
    expected_new_records = 1

    # Action
    _call_stored_procedure(bq_client, job_kennung, eintrags_nr)

    # Assertions
    job_entry = _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status='DONE')
    assert job_entry.record_count == expected_new_records
    
    ta_period_data = _get_ta_period_data(bq_client)
    assert len(ta_period_data) == expected_new_records
    assert any(row.period_id == 17 for row in ta_period_data)
    assert not any(row.period_id == 999 for row in ta_period_data) # Old dummy data should be gone
```

---

### Test Case 9: Transformation Correctness - Join and Filter Logic

**Purpose**: Verify the correctness of the `JOIN` conditions and `WHERE` clause filters, including `NULL` handling for `modified_at`. This is a detailed check of the core `INSERT ... SELECT` statement.

**Setup**:
*   Ensure `job_table` and `ta_period` are empty.
*   Populate `dwtk_meldungen` to set `v_datum` (e.g., '2023-01-15').
*   Populate `cds_ta_period`, `cds_ta_time_meas_cv`, `cds_ta_description` with a diverse set of data, including:
    *   Rows that should match all join and filter conditions.
    *   Rows that fail `p.insert_at <= v_datum`.
    *   Rows that fail `p.modified_at IS NULL OR p.modified_at > v_datum` (i.e., `modified_at` is present and `<= v_datum`).
    *   Rows with `modified_at IS NULL` that should match.
    *   Rows that fail join conditions (e.g., `time_meas_cv` or `DESCRIPTION_ID` not found).

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The `ta_period` table should contain *exactly* the rows that satisfy all `JOIN` and `WHERE` conditions.
*   The `record_count` in `job_table` should match this exact count.
*   Verify the transformed column values (`einheit`, `bfc_age`) are correct.

**Runnable Test Code (pytest)**:
```python
def test_join_and_filter_logic(bq_client):
    job_kennung = "TEST_JOB_08"
    eintrags_nr = "001"

    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-01-15 12:00:00 UTC")
    expected_v_datum_date = datetime.date(2023, 1, 15)

    # Setup lookup tables
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_X", 201)
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_Y", 202)
    _insert_cds_ta_description(bq_client, 201, "Unit X")
    _insert_cds_ta_description(bq_client, 202, "Unit Y")

    # Test data for cds_ta_period
    # 1. Should be inserted: insert_at <= v_datum, modified_at IS NULL
    _insert_cds_ta_period(bq_client, 101, 10, "TM_CV_X", "2023-01-10 00:00:00 UTC", None)
    # 2. Should be inserted: insert_at <= v_datum, modified_at > v_datum
    _insert_cds_ta_period(bq_client, 102, 20, "TM_CV_Y", "2023-01-15 00:00:00 UTC", "2023-01-16 00:00:00 UTC")
    # 3. Should be inserted: insert_at = v_datum, modified_at IS NULL
    _insert_cds_ta_period(bq_client, 103, 30, "TM_CV_X", "2023-01-15 23:59:59 UTC", None)
    # 4. Should NOT be inserted: insert_at > v_datum
    _insert_cds_ta_period(bq_client, 104, 40, "TM_CV_Y", "2023-01-16 00:00:00 UTC", None)
    # 5. Should NOT be inserted: modified_at <= v_datum
    _insert_cds_ta_period(bq_client, 105, 50, "TM_CV_X", "2023-01-10 00:00:00 UTC", "2023-01-14 00:00:00 UTC")
    # 6. Should NOT be inserted: Join to cds_ta_time_meas_cv fails
    _insert_cds_ta_period(bq_client, 106, 60, "TM_CV_Z", "2023-01-10 00:00:00 UTC", None) # TM_CV_Z does not exist
    # 7. Should NOT be inserted: Join to cds_ta_description fails (via tm.DESCRIPTION_ID)
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_W", 999) # description_id 999 does not exist
    _insert_cds_ta_period(bq_client, 107, 70, "TM_CV_W", "2023-01-10 00:00:00 UTC", None)

    expected_inserted_period_ids = {101, 102, 103}
    expected_records = len(expected_inserted_period_ids)

    # Action
    _call_stored_procedure(bq_client, job_kennung, eintrags_nr)

    # Assertions
    job_entry = _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status='DONE')
    assert job_entry.record_count == expected_records
    assert _get_ta_period_row_count(bq_client) == expected_records

    ta_period_data = _get_ta_period_data(bq_client)
    actual_period_ids = {row.period_id for row in ta_period_data}
    assert actual_period_ids == expected_inserted_period_ids

    # Verify specific column transformations
    row_101 = next(row for row in ta_period_data if row.period_id == 101)
    assert row_101.einheit == "Unit X"
    assert row_101.bfc_age == datetime.datetime(2023, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    
    row_102 = next(row for row in ta_period_data if row.period_id == 102)
    assert row_102.einheit == "Unit Y"
    assert row_102.bfc_age == datetime.datetime(2023, 1, 15, 0, 0, tzinfo=datetime.timezone.utc)
```

---

### Test Case 10: Data Quality - Schema and Data Types

**Purpose**: Verify that the final `ta_period` table schema matches expectations and that data types are correctly handled during insertion, without unexpected `NULL`s or truncation.

**Setup**:
*   Ensure `job_table` and `ta_period` are empty.
*   Populate source tables with data that covers various data types and potential edge cases (e.g., maximum length strings, boundary dates).
*   Ensure `dwtk_meldungen` is set for a successful run.

**Action**:
*   Call the BigQuery Stored Procedure `r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

**Pass/Fail Criterion**:
*   The schema of `ta_period` should match the expected DDL (e.g., `period_id` as `INT64`, `einheit` as `STRING`, `bfc_age` as `TIMESTAMP`).
*   All inserted rows should have non-NULL values for `period_id`, `number_time_measurement`, `time_meas_cv`, `einheit`, `bfc_age` (assuming these are `NOT NULL` in the target schema).
*   String values should not be truncated.
*   Date/Timestamp values should be correctly converted and stored.

**Runnable Test Code (pytest)**:
```python
def test_data_quality_schema_types(bq_client):
    job_kennung = "TEST_JOB_09"
    eintrags_nr = "001"

    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-03-01 10:00:00 UTC")
    
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_LONG_DESC", 301)
    _insert_cds_ta_description(bq_client, 301, "This is a very long description that should not be truncated and should fit into the target string column.")

    _insert_cds_ta_period(bq_client, 201, 1234567890, "TM_CV_LONG_DESC", "2023-02-15 14:30:00 UTC", None)
    
    expected_records = 1

    # Action
    _call_stored_procedure(bq_client, job_kennung, eintrags_nr)

    # Assertions
    job_entry = _get_job_table_entry(bq_client, job_kennung, eintrags_nr, status='DONE')
    assert job_entry.record_count == expected_records

    ta_period_data = _get_ta_period_data(bq_client)
    assert len(ta_period_data) == expected_records
    
    inserted_row = ta_period_data[0]
    
    # Check data types and values
    assert isinstance(inserted_row.period_id, int)
    assert inserted_row.period_id == 201
    
    assert isinstance(inserted_row.number_time_measurement, int)
    assert inserted_row.number_time_measurement == 1234567890
    
    assert isinstance(inserted_row.time_meas_cv, str)
    assert inserted_row.time_meas_cv == "TM_CV_LONG_DESC"
    
    assert isinstance(inserted_row.einheit, str)
    assert inserted_row.einheit == "This is a very long description that should not be truncated and should fit into the target string column."
    
    assert isinstance(inserted_row.bfc_age, datetime.datetime)
    assert inserted_row.bfc_age == datetime.datetime(2023, 2, 15, 14, 30, 0, tzinfo=datetime.timezone.utc)

    # Assert no unexpected NULLs (assuming target columns are NOT NULL)
    assert inserted_row.period_id is not None
    assert inserted_row.number_time_measurement is not None
    assert inserted_row.time_meas_cv is not None
    assert inserted_row.einheit is not None
    assert inserted_row.bfc_age is not None

    # Schema check (can be done programmatically or manually)
    table = bq_client.get_table(TA_PERIOD_TABLE.replace('`', ''))
    schema_fields = {field.name: field.field_type for field in table.schema}
    assert schema_fields['period_id'] == 'INT64'
    assert schema_fields['number_time_measurement'] == 'INT64'
    assert schema_fields['time_meas_cv'] == 'STRING'
    assert schema_fields['einheit'] == 'STRING'
    assert schema_fields['bfc_age'] == 'TIMESTAMP'
```

---

### Test Case 11: Output Parity - End-to-End Comparison (Conceptual)

**Purpose**: To ensure that given identical inputs, the migrated BigQuery job produces the exact same final state in `ta_period` and the same record count as the legacy KornShell job. This is the ultimate validation of behavioral equivalence.

**Setup**:
*   **Legacy Environment**:
    *   Set up a legacy environment with the original `k_ausd_v_ta_period.ksh` script and its dependencies.
    *   Populate the Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds_ta_period`, `cds_ta_time_meas_cv`, `cds_ta_description`) with a representative dataset.
    *   Ensure the legacy `job_table` (or equivalent) and `sof$ta_period` are clean.
*   **BigQuery Environment**:
    *   Populate the BigQuery source tables (`project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_period`, etc.) with *identical* data to the Oracle source tables.
    *   Ensure `project.dataset.job_table` and `project.dataset.ta_period` are clean.

**Action**:
1.  **Run Legacy Job**: Execute `k_ausd_v_ta_period.ksh -j LEGACY_JOB -f 001` in the legacy environment. Capture its output (record count) and the final state of `sof$ta_period`.
2.  **Run Migrated Job**: Execute `CALL project.dataset.r_ausd_vertrag_control('MIGRATED_JOB', '001')` in BigQuery.

**Pass/Fail Criterion**:
1.  **Record Count**: The `record_count` captured by the legacy job (from `$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp`) must be identical to the `record_count` stored in `project.dataset.job_table` for the migrated job.
2.  **Data Content**: The data in the legacy `sof$ta_period` table must be *identical* to the data in the BigQuery `project.dataset.ta_period` table. This includes row counts, column values, and data types (after BigQuery's native type mapping). A row-by-row comparison or hash comparison of sorted data is ideal.

**Runnable Test Code (Conceptual / Pytest with external comparison)**:
```python
# This test case is more conceptual as it involves interacting with a legacy system.
# The actual implementation would depend on tools available for legacy data extraction
# (e.g., SQL*Plus for Oracle, custom scripts for file parsing).

def test_output_parity_end_to_end(bq_client):
    # --- Setup Legacy Environment (Conceptual Steps) ---
    # 1. Populate Oracle source tables with a known, representative dataset.
    #    This would typically involve a data dump/load from a golden source.
    # 2. Clear legacy target table (sof$ta_period) and job tracking.
    # 3. Execute the legacy script:
    #    subprocess.run(["/path/to/k_ausd_v_ta_period.ksh", "-j", "LEGACY_JOB", "-f", "001"])
    # 4. Extract results from legacy:
    #    legacy_output_records = read_legacy_tmp_file("/path/to/bert_k_ausd_v_ta_period_*.tmp")
    #    legacy_ta_period_data = fetch_data_from_oracle("sof$ta_period") # e.g., via cx_Oracle or JDBC

    # --- Setup BigQuery Environment (Identical Data) ---
    # For this example, we'll use a simplified setup similar to previous tests,
    # assuming the BQ source tables are populated with data IDENTICAL to Oracle.
    _insert_dwtk_meldungen(bq_client, "BERT_DROP_TEMP_TABLE", "2023-04-01 10:00:00 UTC")
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_PARITY_A", 401)
    _insert_cds_ta_time_meas_cv(bq_client, "TM_CV_PARITY_B", 402)
    _insert_cds_ta_description(bq_client, 401, "Parity Unit A")
    _insert_cds_ta_description(bq_client, 402, "Parity Unit B")
    _insert_cds_ta_period(bq_client, 301, 100, "TM_CV_PARITY_A", "2023-03-20 00:00:00 UTC", None)
    _insert_cds_ta_period(bq_client, 302, 200, "TM_CV_PARITY_B", "2023-03-25 00:00:00 UTC", "2023-03-30 00:00:00 UTC")
    _insert_cds_ta_period(bq_client, 303, 300, "TM_CV_PARITY_A", "2023-04-05 00:00:00 UTC", None) # Should NOT be inserted
    
    expected_records_bq = 2 # Based on BQ setup, this is what legacy should also produce

    # --- Simulate/Assume Legacy Output ---
    # In a real test, these would be actual values extracted from the legacy system.
    legacy_output_records = expected_records_bq 
    legacy_ta_period_data_expected = [
        {'period_id': 301, 'number_time_measurement': 100, 'time_meas_cv': 'TM_CV_PARITY_A', 'einheit': 'Parity Unit A', 'bfc_age': datetime.datetime(2023, 3, 20, 0, 0, tzinfo=datetime.timezone.utc)},
        {'period_id': 302, 'number_time_measurement': 200, 'time_meas_cv': 'TM_CV_PARITY_B', 'einheit': 'Parity Unit B', 'bfc_age': datetime.datetime(2023, 3, 25, 0, 0, tzinfo=datetime.timezone.utc)},
    ]

    # --- Action: Execute Migrated Job ---
    job_kennung_bq = "MIGRATED_JOB_PARITY"
    eintrags_nr_bq = "001"
    _call_stored_procedure(bq_client, job_kennung_bq, eintrags_nr_bq)

    # --- Pass/Fail Criterion ---
    # 1. Compare record counts
    job_entry_bq = _get_job_table_entry(bq_client, job_kennung_bq, eintrags_nr_bq, status='DONE')
    assert job_entry_bq.record_count == legacy_output_records, \
        f"Record count mismatch: BQ={job_entry_bq.record_count}, Legacy={legacy_output_records}"

    # 2. Compare data content (row by row, after sorting)
    bq_ta_period_data = _get_ta_period_data(bq_client)
    
    # Convert BQ rows to dicts for easier comparison
    bq_ta_period_dicts = [dict(row.items()) for row in bq_ta_period_data]
    
    # Sort both lists for consistent comparison
    sorted_bq_data = sorted(bq_ta_period_dicts, key=lambda x: x['period_id'])
    sorted_legacy_data = sorted(legacy_ta_period_data_expected, key=lambda x: x['period_id'])

    assert sorted_bq_data == sorted_legacy_data, \
        f"Data content mismatch: BQ={sorted_bq_data}, Legacy={sorted_legacy_data}"
```