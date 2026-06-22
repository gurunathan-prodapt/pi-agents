The following tests are designed to validate the migration of `k_drop_temp_table.ksh` to the BigQuery Stored Procedure `dataset.r_drop_temp_table_control`. The tests cover output parity, transformation correctness, external system replacements, and data quality assertions.

For these tests, we assume a BigQuery client is available (e.g., via a `pytest` fixture) and that the target dataset is `my_project.my_dataset`. A mock `dataset.d_drop_temp_table` procedure is used to simulate the behavior of the actual table dropping logic, as its content was not provided.

---

## Test Setup and Mock Procedures

Before running the tests, ensure the BigQuery dataset and tables (`job_error_log`, `job_table`, `d_drop_temp_table_log`) are created. The `d_drop_temp_table_log` table is used by our mock `d_drop_temp_table` to record its invocations.

```python
import pytest
from google.cloud import bigquery
from datetime import date, timedelta, datetime, timezone

# Replace with your actual project and dataset ID
DATASET_ID = "my_project.my_dataset"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    return bigquery.Client()

@pytest.fixture(autouse=True)
def setup_and_teardown(bq_client):
    """
    Ensures necessary tables exist and are cleared before each test.
    Deploys a mock d_drop_temp_table procedure.
    """
    # Ensure schema and tables exist
    bq_client.query(f"""
        CREATE SCHEMA IF NOT EXISTS {DATASET_ID} OPTIONS(default_table_expiration_days=1);
        CREATE TABLE IF NOT EXISTS {DATASET_ID}.job_error_log (
            job_name STRING,
            error_nr INT64,
            error_arg STRING,
            created_at TIMESTAMP
        ) OPTIONS(expiration_days=1);
        CREATE TABLE IF NOT EXISTS {DATASET_ID}.job_table (
            tab_name STRING,
            status STRING,
            mode STRING,
            stichtag_from DATE,
            stichtag_to DATE,
            job_type STRING,
            active_flag STRING,
            records INT64,
            description STRING
        ) OPTIONS(expiration_days=1);
        CREATE TABLE IF NOT EXISTS {DATASET_ID}.d_drop_temp_table_log (
            eintrags_nr STRING,
            job_kennung STRING,
            stichtag STRING,
            restart INT64,
            datum_heute DATE,
            datum_gestern DATE,
            monat_heute STRING,
            monat_gestern STRING,
            call_time TIMESTAMP
        ) OPTIONS(expiration_days=1);
    """).result()

    # Clear tables before each test run
    bq_client.query(f"TRUNCATE TABLE {DATASET_ID}.job_error_log;").result()
    bq_client.query(f"TRUNCATE TABLE {DATASET_ID}.job_table;").result()
    bq_client.query(f"TRUNCATE TABLE {DATASET_ID}.d_drop_temp_table_log;").result()

    # Deploy the mock d_drop_temp_table for testing the control procedure's interaction
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE {DATASET_ID}.d_drop_temp_table(
            p_EintragsNr STRING,
            p_JobKennung STRING,
            p_Stichtag STRING,
            p_restart INT64,
            p_datum_heute DATE,
            p_datum_gestern DATE,
            p_monat_heute STRING,
            p_monat_gestern STRING,
            INOUT p_records INT64
        )
        BEGIN
            -- Log parameters for verification
            INSERT INTO {DATASET_ID}.d_drop_temp_table_log (
                eintrags_nr, job_kennung, stichtag, restart,
                datum_heute, datum_gestern, monat_heute, monat_gestern,
                call_time
            )
            VALUES (
                p_EintragsNr, p_JobKennung, p_Stichtag, p_restart,
                p_datum_heute, p_datum_gestern, p_monat_heute, p_monat_gestern,
                CURRENT_TIMESTAMP()
            );
            -- Simulate 5 tables dropped/records processed
            SET p_records = 5;
        END;
    """).result()

    yield

    # Teardown: (Optional) Clean up temporary tables if not using expiration_days
    # For this setup, tables will expire automatically.
```

---

## Test Cases

### 1. Test Case: Missing `p_JobKennung` Parameter

*   **Purpose:** Verify that the procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and returning an appropriate message.
*   **Setup:** Ensure `job_error_log` is empty.
*   **Action:** Call `dataset.r_drop_temp_table_control` with `p_JobKennung` as `NULL` or empty string, but valid `p_EintragsNr` and `p_Stichtag`.
*   **Pass/Fail Criterion:**
    *   The procedure returns a result set containing the message `FEHLER: 0 E 1 Jobkennung`.
    *   A row is inserted into `dataset.job_error_log` with `job_name = 'r_drop_temp_table_control'`, `error_nr = 1`, and `error_arg = 'Jobkennung'`.
    *   The `d_drop_temp_table` mock procedure is *not* called.

```python
def test_missing_jobkennung(bq_client):
    # Action
    query_job = bq_client.query(f"""
        CALL {DATASET_ID}.r_drop_temp_table_control(
            p_JobKennung => NULL,
            p_EintragsNr => '123',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
    """)
    results = list(query_job.result())

    # Assert console output
    assert len(results) == 1
    assert results[0].message == 'FEHLER: 0 E 1 Jobkennung'

    # Assert error log entry
    error_log_query = bq_client.query(f"SELECT job_name, error_nr, error_arg FROM {DATASET_ID}.job_error_log;").result()
    error_entries = list(error_log_query)
    assert len(error_entries) == 1
    assert error_entries[0].job_name == 'r_drop_temp_table_control'
    assert error_entries[0].error_nr == 1
    assert error_entries[0].error_arg == 'Jobkennung'

    # Assert d_drop_temp_table was NOT called
    d_drop_log_query = bq_client.query(f"SELECT COUNT(*) FROM {DATASET_ID}.d_drop_temp_table_log;").result()
    assert list(d_drop_log_query)[0][0] == 0
```

### 2. Test Case: Missing `p_Stichtag` Parameter

*   **Purpose:** Verify that the procedure correctly identifies and handles a missing `p_Stichtag` parameter, logging an error and returning an appropriate message.
*   **Setup:** Ensure `job_error_log` is empty.
*   **Action:** Call `dataset.r_drop_temp_table_control` with `p_Stichtag` as `NULL` or empty string, but valid `p_JobKennung` and `p_EintragsNr`.
*   **Pass/Fail Criterion:**
    *   The procedure returns a result set containing the message `FEHLER: 0 E 1 Stichtag`.
    *   A row is inserted into `dataset.job_error_log` with `job_name = 'r_drop_temp_table_control'`, `error_nr = 1`, and `error_arg = 'Stichtag'`.
    *   The `d_drop_temp_table` mock procedure is *not* called.

```python
def test_missing_stichtag(bq_client):
    # Action
    query_job = bq_client.query(f"""
        CALL {DATASET_ID}.r_drop_temp_table_control(
            p_JobKennung => 'JOB1',
            p_EintragsNr => '123',
            p_Stichtag => NULL,
            p_wiederanlaufWert => 0
        );
    """)
    results = list(query_job.result())

    # Assert console output
    assert len(results) == 1
    assert results[0].message == 'FEHLER: 0 E 1 Stichtag'

    # Assert error log entry
    error_log_query = bq_client.query(f"SELECT job_name, error_nr, error_arg FROM {DATASET_ID}.job_error_log;").result()
    error_entries = list(error_log_query)
    assert len(error_entries) == 1
    assert error_entries[0].job_name == 'r_drop_temp_table_control'
    assert error_entries[0].error_nr == 1
    assert error_entries[0].error_arg == 'Stichtag'

    # Assert d_drop_temp_table was NOT called
    d_drop_log_query = bq_client.query(f"SELECT COUNT(*) FROM {DATASET_ID}.d_drop_temp_table_log;").result()
    assert list(d_drop_log_query)[0][0] == 0
```

### 3. Test Case: Missing `p_EintragsNr` Parameter

*   **Purpose:** Verify that the procedure correctly identifies and handles a missing `p_EintragsNr` parameter, logging an error and returning an appropriate message.
*   **Setup:** Ensure `job_error_log` is empty.
*   **Action:** Call `dataset.r_drop_temp_table_control` with `p_EintragsNr` as `NULL` or empty string, but valid `p_JobKennung` and `p_Stichtag`.
*   **Pass/Fail Criterion:**
    *   The procedure returns a result set containing the message `FEHLER: 0 E 1 EintragsNr`.
    *   A row is inserted into `dataset.job_error_log` with `job_name = 'r_drop_temp_table_control'`, `error_nr = 1`, and `error_arg = 'EintragsNr'`.
    *   The `d_drop_temp_table` mock procedure is *not* called.

```python
def test_missing_eintragsnr(bq_client):
    # Action
    query_job = bq_client.query(f"""
        CALL {DATASET_ID}.r_drop_temp_table_control(
            p_JobKennung => 'JOB1',
            p_EintragsNr => NULL,
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
    """)
    results = list(query_job.result())

    # Assert console output
    assert len(results) == 1
    assert results[0].message == 'FEHLER: 0 E 1 EintragsNr'

    # Assert error log entry
    error_log_query = bq_client.query(f"SELECT job_name, error_nr, error_arg FROM {DATASET_ID}.job_error_log;").result()
    error_entries = list(error_log_query)
    assert len(error_entries) == 1
    assert error_entries[0].job_name == 'r_drop_temp_table_control'
    assert error_entries[0].error_nr == 1
    assert error_entries[0].error_arg == 'EintragsNr'

    # Assert d_drop_temp_table was NOT called
    d_drop_log_query = bq_client.query(f"SELECT COUNT(*) FROM {DATASET_ID}.d_drop_temp_table_log;").result()
    assert list(d_drop_log_query)[0][0] == 0
```

### 4. Test Case: Invalid `p_Stichtag` Date Format

*   **Purpose:** Verify that the procedure correctly identifies and handles an invalid `p_Stichtag` format (e.g., not DDMMYYYY), logging an error and raising a `SIGNAL SQLSTATE`.
*   **Setup:** Ensure `job_error_log` is empty.
*   **Action:** Call `dataset.r_drop_temp_table_control` with an invalid `p_Stichtag` (e.g., '2023-01-01').
*   **Pass/Fail Criterion:**
    *   The procedure execution raises an exception (due to `SIGNAL SQLSTATE`) with `MESSAGE_TEXT = 'Invalid date format for Stichtag. Expected DDMMYYYY.'`.
    *   A row is inserted into `dataset.job_error_log` with `job_name = 'r_drop_temp_table_control'`, `error_nr = 193`, and `error_arg` matching the invalid `p_Stichtag`.
    *   The `d_drop_temp_table` mock procedure is *not* called.

```python
def test_invalid_stichtag_format(bq_client):
    invalid_stichtag = '2023-01-01' # Expected DDMMYYYY

    # Action
    with pytest.raises(Exception) as excinfo:
        bq_client.query(f"""
            CALL {DATASET_ID}.r_drop_temp_table_control(
                p_JobKennung => 'JOB1',
                p_EintragsNr => '123',
                p_Stichtag => '{invalid_stichtag}',
                p_wiederanlaufWert => 0
            );
        """).result()

    # Assert error message from SIGNAL SQLSTATE
    assert "Invalid date format for Stichtag. Expected DDMMYYYY." in str(excinfo.value)

    # Assert error log entry
    error_log_query = bq_client.query(f"SELECT job_name, error_nr, error_arg FROM {DATASET_ID}.job_error_log;").result()
    error_entries = list(error_log_query)
    assert len(error_entries) == 1
    assert error_entries[0].job_name == 'r_drop_temp_table_control'
    assert error_entries[0].error_nr == 193
    assert error_entries[0].error_arg == invalid_stichtag

    # Assert d_drop_temp_table was NOT called
    d_drop_log_query = bq_client.query(f"SELECT COUNT(*) FROM {DATASET_ID}.d_drop_temp_table_log;").result()
    assert list(d_drop_log_query)[0][0] == 0
```

### 5. Test Case: Happy Path - Valid Parameters and `p_wiederanlaufWert` provided

*   **Purpose:** Verify the procedure executes successfully with all valid parameters, correctly derives dates, calls the `d_drop_temp_table` mock, and outputs the final message.
*   **Setup:** Ensure `job_error_log` and `d_drop_temp_table_log` are empty.
*   **Action:** Call `dataset.r_drop_temp_table_control` with valid `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert`.
*   **Pass/Fail Criterion:**
    *   The procedure returns two result sets: one with the `p_records` message and one with the final "ENDE Datenverarbeitung" message.
    *   `job_error_log` remains empty.
    *   The `d_drop_temp_table` mock is called exactly once with the correct derived date parameters and `p_wiederanlaufWert`.
    *   The `p_records` value returned by the mock (5 in this case) is reflected in the output message.

```python
def test_happy_path_with_restart_value(bq_client):
    job_kennung = 'JOB_DROP_TEMP'
    eintrags_nr = 'ENTRY_001'
    stichtag = '15032023'
    wiederanlauf_wert = 1

    # Expected derived dates
    today = date.today()
    yesterday = today - timedelta(days=1)
    expected_monat_heute = today.strftime('%m')
    expected_monat_gestern = yesterday.strftime('%m')

    # Action
    query_job = bq_client.query(f"""
        CALL {DATASET_ID}.r_drop_temp_table_control(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """)
    results = list(query_job.result())

    # Assert console output
    assert len(results) == 2
    assert results[0].message == 'Records processed by d_drop_temp_table: 5'
    assert results[1].message == '---------- ENDE Datenverarbeitung ----------'

    # Assert error log is empty
    error_log_query = bq_client.query(f"SELECT COUNT(*) FROM {DATASET_ID}.job_error_log;").result()
    assert list(error_log_query)[0][0] == 0

    # Assert d_drop_temp_table was called with correct parameters
    d_drop_log_query = bq_client.query(f"""
        SELECT eintrags_nr, job_kennung, stichtag, restart,
               datum_heute, datum_gestern, monat_heute, monat_gestern
        FROM {DATASET_ID}.d_drop_temp_table_log;
    """).result()
    d_drop_entries = list(d_drop_log_query)
    assert len(d_drop_entries) == 1
    assert d_drop_entries[0].eintrags_nr == eintrags_nr
    assert d_drop_entries[0].job_kennung == job_kennung
    assert d_drop_entries[0].stichtag == stichtag
    assert d_drop_entries[0].restart == wiederanlauf_wert
    assert d_drop_entries[0].datum_heute == today
    assert d_drop_entries[0].datum_gestern == yesterday
    assert d_drop_entries[0].monat_heute == expected_monat_heute
    assert d_drop_entries[0].monat_gestern == expected_monat_gestern
```

### 6. Test Case: Happy Path - `p_wiederanlaufWert` is NULL

*   **Purpose:** Verify the procedure correctly initializes `v_restart` to `0` when `p_wiederanlaufWert` is `NULL`.
*   **Setup:** Ensure `job_error_log` and `d_drop_temp_table_log` are empty.
*   **Action:** Call `dataset.r_drop_temp_table_control` with valid parameters, but `p_wiederanlaufWert` as `NULL`.
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully and returns the expected messages.
    *   `job_error_log` remains empty.
    *   The `d_drop_temp_table` mock is called exactly once, and the `restart` parameter passed to it is `0`.

```python
def test_happy_path_null_restart_value(bq_client):
    job_kennung = 'JOB_DROP_TEMP_NULL'
    eintrags_nr = 'ENTRY_002'
    stichtag = '20032023'

    # Expected derived dates
    today = date.today()
    yesterday = today - timedelta(days=1)
    expected_monat_heute = today.strftime('%m')
    expected_monat_gestern = yesterday.strftime('%m')

    # Action
    query_job = bq_client.query(f"""
        CALL {DATASET_ID}.r_drop_temp_table_control(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag}',
            p_wiederanlaufWert => NULL
        );
    """)
    results = list(query_job.result())

    # Assert console output
    assert len(results) == 2
    assert results[0].message == 'Records processed by d_drop_temp_table: 5'
    assert results[1].message == '---------- ENDE Datenverarbeitung ----------'

    # Assert error log is empty
    error_log_query = bq_client.query(f"SELECT COUNT(*) FROM {DATASET_ID}.job_error_log;").result()
    assert list(error_log_query)[0][0] == 0

    # Assert d_drop_temp_table was called with restart=0
    d_drop_log_query = bq_client.query(f"""
        SELECT restart
        FROM {DATASET_ID}.d_drop_temp_table_log
        WHERE job_kennung = '{job_kennung}';
    """).result()
    d_drop_entries = list(d_drop_log_query)
    assert len(d_drop_entries) == 1
    assert d_drop_entries[0].restart == 0
```

### 7. Test Case: `job_table` Interaction (if uncommented)

*   **Purpose:** Verify that if the job management logic (`INSERT` into `dataset.job_table`) is uncommented and activated, it correctly inserts a record.
*   **Setup:** Ensure `job_table` is empty. This test requires uncommenting the `INSERT` statement in `r_drop_temp_table_control.sql`.
*   **Action:** Call `dataset.r_drop_temp_table_control` with valid parameters.
*   **Pass/Fail Criterion:**
    *   A row is inserted into `dataset.job_table` with the expected values for `tab_name`, `status`, `mode`, `stichtag_from`, `stichtag_to`, `job_type`, `active_flag`, `records`, and `description`.

```python
# NOTE: This test assumes the INSERT INTO dataset.job_table section in
# r_drop_temp_table_control.sql has been UNCOMMENTED for testing.
# If it remains commented, this test will fail as no data will be inserted.

def test_job_table_insertion_if_active(bq_client):
    job_kennung = 'JOB_TABLE_TEST'
    eintrags_nr = 'ENTRY_003'
    stichtag = '25032023' # DDMMYYYY format
    wiederanlauf_wert = 0
    expected_records = 5 # From the mock d_drop_temp_table

    # Action
    bq_client.query(f"""
        CALL {DATASET_ID}.r_drop_temp_table_control(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """).result()

    # Assert job_table entry
    job_table_query = bq_client.query(f"""
        SELECT tab_name, status, mode, stichtag_from, stichtag_to,
               job_type, active_flag, records, description
        FROM {DATASET_ID}.job_table;
    """).result()
    job_entries = list(job_table_query)

    # This assertion will fail if the INSERT statement is still commented out.
    assert len(job_entries) == 1
    assert job_entries[0].tab_name == 'PoolVertrag'
    assert job_entries[0].status == 'A'
    assert job_entries[0].mode == 'I'
    assert job_entries[0].stichtag_from == date(2023, 3, 25)
    assert job_entries[0].stichtag_to == date(2023, 3, 25)
    assert job_entries[0].job_type == 'J'
    assert job_entries[0].active_flag == 'N'
    assert job_entries[0].records == expected_records
    assert job_entries[0].description == 'Initialbefuellung'
```

---

### 8. Test Case: `job_error_log` Schema and Data Quality

*   **Purpose:** Verify the schema of `dataset.job_error_log` and ensure `created_at` is correctly populated.
*   **Setup:** Trigger an error condition (e.g., missing `p_JobKennung`).
*   **Action:** Call `dataset.r_drop_temp_table_control` with `p_JobKennung` as `NULL`.
*   **Pass/Fail Criterion:**
    *   The `job_error_log` table exists and has the expected columns (`job_name`, `error_nr`, `error_arg`, `created_at`) with correct data types.
    *   The `created_at` timestamp for the inserted error record is recent (within a few seconds of the test execution).

```python
def test_job_error_log_schema_and_timestamp(bq_client):
    # Action (trigger an error)
    bq_client.query(f"""
        CALL {DATASET_ID}.r_drop_temp_table_control(
            p_JobKennung => NULL,
            p_EintragsNr => '123',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
    """).result()

    # Assert schema (implicitly checked by query, but can be explicit)
    # For explicit schema check, you'd query INFORMATION_SCHEMA.COLUMNS
    # Example:
    # schema_query = bq_client.query(f"""
    #     SELECT column_name, data_type FROM {DATASET_ID}.INFORMATION_SCHEMA.COLUMNS
    #     WHERE table_name = 'job_error_log' ORDER BY ordinal_position;
    # """).result()
    # columns = [(row.column_name, row.data_type) for row in schema_query]
    # assert columns == [
    #     ('job_name', 'STRING'), ('error_nr', 'INT64'),
    #     ('error_arg', 'STRING'), ('created_at', 'TIMESTAMP')
    # ]

    # Assert created_at timestamp
    error_log_query = bq_client.query(f"SELECT created_at FROM {DATASET_ID}.job_error_log;").result()
    error_entries = list(error_log_query)
    assert len(error_entries) == 1
    created_at = error_entries[0].created_at.replace(tzinfo=timezone.utc) # Ensure timezone awareness

    now = datetime.now(timezone.utc)
    # Check if created_at is within a reasonable time window (e.g., last 30 seconds)
    assert now - created_at < timedelta(seconds=30)
    assert now - created_at >= timedelta(seconds=0) # Should not be in the future
```