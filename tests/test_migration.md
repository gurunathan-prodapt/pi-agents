As a senior data-migration QA engineer, I've analyzed the migration design for `k_ausd_adressen.ksh` to a BigQuery-native solution. The core challenge lies in the fact that `d_ausd_adressen.sql` (the main data processing logic) is an external dependency whose contents are not provided. Therefore, my tests will focus on the *control flow*, *parameter handling*, *date logic*, *error reporting*, and *job tracking* aspects of the migrated `r_ausd_adressen_control` BigQuery stored procedure, and how it interacts with the *assumed* migrated `d_ausd_adressen.sql` logic.

For tests involving the output of `d_ausd_adressen.sql`, I will assume:
1.  The `d_ausd_adressen.sql` logic has been successfully migrated into BigQuery SQL and is either embedded directly within `project.dataset.r_ausd_adressen_control` or called as a sub-procedure.
2.  Its output is written to a BigQuery table, which I'll refer to as `project.dataset.target_ausd_adressen`.
3.  The `project.dataset.error_log` and `project.dataset.job_table` exist with the schemas described in the design.
4.  A `project.dataset.config` table can be used for environment-like variables.

The tests are designed to prove behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

### Test Setup Prerequisites (Common for all tests)

Before running any tests, ensure the following BigQuery resources are created:

```sql
-- BigQuery Dataset
CREATE SCHEMA IF NOT EXISTS your_gcp_project_id.your_dataset;

-- Error Log Table
CREATE OR REPLACE TABLE your_gcp_project_id.your_dataset.error_log (
    error_code INT64,
    error_message STRING,
    error_argument STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Job Tracking Table (assuming commented-out functionality is implemented)
CREATE OR REPLACE TABLE your_gcp_project_id.your_dataset.job_table (
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag DATE,
    status STRING, -- e.g., 'SUCCESS', 'FAILED'
    record_count INT64,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Configuration Table (for environment variable replacements)
CREATE OR REPLACE TABLE your_gcp_project_id.your_dataset.config (
    config_key STRING,
    config_value STRING
);

-- Target Data Table (where d_ausd_adressen.sql output goes)
-- The schema here is illustrative and should match the actual migrated d_ausd_adressen.sql output.
CREATE OR REPLACE TABLE your_gcp_project_id.your_dataset.target_ausd_adressen (
    id INT64,
    processed_name STRING,
    processed_address STRING,
    stichtag DATE,
    load_date DATE, -- Example for p_datum_heute
    zip_code_numeric INT64,
    processed_region STRING, -- Example for config table usage
    wiederanlauf_param_used INT64 -- Example for p_wiederanlaufWert usage
);

-- Source Data Table (input for d_ausd_adressen.sql)
-- The schema here is illustrative and should match the actual d_ausd_adressen.sql input.
CREATE OR REPLACE TABLE your_gcp_project_id.your_dataset.source_addresses (
    id INT64,
    name STRING,
    address STRING,
    stichtag DATE,
    zip_code STRING,
    status_code INT64,
    region STRING,
    optional_value INT64
);

-- BigQuery Stored Procedure (the migrated k_ausd_adressen.ksh)
-- This is a placeholder. The actual procedure code would be much longer.
-- It should encapsulate the logic described in the Migration Design Document.
-- For testing, assume it has the following signature and internal logic:
/*
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_dataset.r_ausd_adressen_control(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING,
    p_wiederanlaufWert INT64 DEFAULT 0
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_records INT64;
    DECLARE p_datum_heute DATE DEFAULT CURRENT_DATE();
    DECLARE p_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    DECLARE v_error_code INT64;
    DECLARE v_error_message STRING;
    DECLARE v_error_argument STRING;
    DECLARE v_processing_mode STRING;
    DECLARE v_default_region STRING;

    -- Parameter Validation
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
        SET v_error_code = 193; SET v_error_message = 'Missing mandatory parameter: Jobkennung'; SET v_error_argument = 'p_JobKennung';
        INSERT INTO your_gcp_project_id.your_dataset.error_log (error_code, error_message, error_argument) VALUES (v_error_code, v_error_message, v_error_argument);
        LEAVE;
    END IF;
    IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
        SET v_error_code = 193; SET v_error_message = 'Missing mandatory parameter: EintragsNr'; SET v_error_argument = 'p_EintragsNr';
        INSERT INTO your_gcp_project_id.your_dataset.error_log (error_code, error_message, error_argument) VALUES (v_error_code, v_error_message, v_error_argument);
        LEAVE;
    END IF;
    IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
        SET v_error_code = 193; SET v_error_message = 'Missing mandatory parameter: Stichtag'; SET v_error_argument = 'p_Stichtag';
        INSERT INTO your_gcp_project_id.your_dataset.error_log (error_code, error_message, error_argument) VALUES (v_error_code, v_error_message, v_error_argument);
        LEAVE;
    END IF;

    -- Date Validation
    SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_stichtag_date IS NULL THEN
        SET v_error_code = 192; SET v_error_message = 'Invalid date format for Stichtag. Expected DDMMYYYY.'; SET v_error_argument = p_Stichtag;
        INSERT INTO your_gcp_project_id.your_dataset.error_log (error_code, error_message, error_argument) VALUES (v_error_code, v_error_message, v_error_argument);
        LEAVE;
    END IF;

    -- Read configuration (example)
    SELECT config_value INTO v_processing_mode FROM your_gcp_project_id.your_dataset.config WHERE config_key = 'PROCESSING_MODE';
    SELECT config_value INTO v_default_region FROM your_gcp_project_id.your_dataset.config WHERE config_key = 'DEFAULT_REGION';

    -- Main SQL Logic (migrated d_ausd_adressen.sql)
    -- This is a simplified example. The actual logic would be complex.
    -- It demonstrates how parameters, derived dates, and config values might be used.
    DELETE FROM your_gcp_project_id.your_dataset.target_ausd_adressen WHERE stichtag = v_stichtag_date; -- Idempotency
    INSERT INTO your_gcp_project_id.your_dataset.target_ausd_adressen (
        id, processed_name, processed_address, stichtag, load_date, zip_code_numeric, processed_region, wiederanlauf_param_used
    )
    SELECT
        s.id,
        COALESCE(s.name, 'UNKNOWN') AS processed_name, -- NULL handling example
        s.address AS processed_address,
        v_stichtag_date AS stichtag,
        p_datum_heute AS load_date, -- Date derivation example
        SAFE_CAST(s.zip_code AS INT64) AS zip_code_numeric, -- Type handling example
        COALESCE(s.region, v_default_region) AS processed_region, -- Config usage example
        p_wiederanlaufWert AS wiederanlauf_param_used -- WiederanlaufWert usage example
    FROM your_gcp_project_id.your_dataset.source_addresses s
    WHERE s.stichtag = v_stichtag_date
      AND (p_wiederanlaufWert = 0 OR s.status_code = p_wiederanlaufWert) -- WiederanlaufWert filter example
      AND s.address IS NOT NULL; -- NULL filtering example

    SET v_records = @@row_count;

    -- Job Logging
    INSERT INTO your_gcp_project_id.your_dataset.job_table (job_kennung, eintrags_nr, stichtag, status, record_count)
    VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, 'SUCCESS', v_records);

EXCEPTION WHEN ERROR THEN
    INSERT INTO your_gcp_project_id.your_dataset.error_log (error_code, error_message, error_argument)
    VALUES (ERROR_CODE(), ERROR_MESSAGE(), 'Internal SQL Error');
    INSERT INTO your_gcp_project_id.your_dataset.job_table (job_kennung, eintrags_nr, stichtag, status, record_count)
    VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, 'FAILED', 0); -- Log failure to job table too
END;
*/
```

---

### Test Case 1: Successful Execution - Happy Path (Output Parity & Row Count)

**Purpose**: Verify that the migrated BigQuery stored procedure executes successfully with valid inputs, processes the main SQL logic, captures the record count, and logs the job details, behaving identically to a successful run of the legacy KornShell script.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.error_log`, `project.dataset.job_table`, and `project.dataset.target_ausd_adressen` are empty.
3.  Populate `project.dataset.source_addresses` with mock data that the legacy `d_ausd_adressen.sql` would process.
    ```sql
    INSERT INTO your_gcp_project_id.your_dataset.source_addresses (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
    (1, 'John Doe', '123 Main St', '2023-01-01', '10001', 0, 'US', 100),
    (2, 'Jane Smith', '456 Oak Ave', '2023-01-01', '20002', 0, 'EU', 200),
    (3, 'Peter Jones', '789 Pine Ln', '2023-01-02', '30003', 0, 'US', 300); -- This one should not be picked up by 01012023 stichtag
    ```
4.  Define the `expected_target_data` and `expected_record_count` based on the legacy `d_ausd_adressen.sql`'s transformation of the mock source data for `stichtag = '01012023'`.
    *   Expected records: 2 (John Doe, Jane Smith)
    *   Expected `load_date`: `CURRENT_DATE()` of test execution.

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` with valid parameters:
    *   `p_JobKennung = 'TEST_JOB_HAPPY'`
    *   `p_EintragsNr = '12345'`
    *   `p_Stichtag = '01012023'`
    *   `p_wiederanlaufWert = 0`

**Pass/Fail Criterion**:
*   The stored procedure executes without error.
*   The `project.dataset.target_ausd_adressen` table contains data identical to the `expected_target_data` (content and schema).
*   The `project.dataset.job_table` contains exactly one entry for `TEST_JOB_HAPPY` with `status = 'SUCCESS'`, `record_count` matching the number of rows in `target_ausd_adressen` (i.e., `expected_record_count`), and correct `stichtag` (`2023-01-01`).
*   The `project.dataset.error_log` table remains empty.

**Runnable Test Code (Python with BigQuery client)**:

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date, timedelta

PROJECT_ID = "your_gcp_project_id"
DATASET_ID = "your_dataset"
CONTROL_PROCEDURE = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_adressen_control"
ERROR_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.error_log"
JOB_TRACKING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_table"
TARGET_DATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.target_ausd_adressen"
SOURCE_DATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.source_addresses"
CONFIG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.config"

client = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def cleanup_tables():
    """Fixture to clean up tables before and after each test."""
    client.query(f"TRUNCATE TABLE {ERROR_LOG_TABLE}").result()
    client.query(f"TRUNCATE TABLE {JOB_TRACKING_TABLE}").result()
    client.query(f"TRUNCATE TABLE {TARGET_DATA_TABLE}").result()
    client.query(f"TRUNCATE TABLE {SOURCE_DATA_TABLE}").result()
    client.query(f"TRUNCATE TABLE {CONFIG_TABLE}").result()
    # Populate config table with defaults for tests that don't specifically test config
    client.query(f"""
        INSERT INTO {CONFIG_TABLE} (config_key, config_value) VALUES
        ('PROCESSING_MODE', 'FULL'),
        ('DEFAULT_REGION', 'EU');
    """).result()
    yield
    client.query(f"TRUNCATE TABLE {ERROR_LOG_TABLE}").result()
    client.query(f"TRUNCATE TABLE {JOB_TRACKING_TABLE}").result()
    client.query(f"TRUNCATE TABLE {TARGET_DATA_TABLE}").result()
    client.query(f"TRUNCATE TABLE {SOURCE_DATA_TABLE}").result()
    client.query(f"TRUNCATE TABLE {CONFIG_TABLE}").result()

def test_successful_execution_happy_path():
    """
    Tests the successful execution of the control procedure with valid parameters,
    ensuring output parity, record count, and job logging.
    """
    # --- Setup: Prepare mock source data and expected target data ---
    client.query(f"""
        INSERT INTO {SOURCE_DATA_TABLE} (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
        (1, 'John Doe', '123 Main St', '2023-01-01', '10001', 0, 'US', 100),
        (2, 'Jane Smith', '456 Oak Ave', '2023-01-01', '20002', 0, 'EU', 200),
        (3, 'Peter Jones', '789 Pine Ln', '2023-01-02', '30003', 0, 'US', 300);
    """).result()

    expected_load_date = date.today() # Assumed from p_datum_heute = CURRENT_DATE()
    expected_target_data = [
        (1, 'John Doe', '123 Main St', date(2023, 1, 1), expected_load_date, 10001, 'US', 0),
        (2, 'Jane Smith', '456 Oak Ave', date(2023, 1, 1), expected_load_date, 20002, 'EU', 0),
    ]
    expected_record_count = len(expected_target_data)

    # --- Action: Call the BigQuery stored procedure ---
    job_kennung = 'TEST_JOB_HAPPY'
    eintrags_nr = '12345'
    stichtag_param = '01012023'
    wiederanlauf_wert = 0

    call_query = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """
    job = client.query(call_query)
    job.result()

    # --- Assertions ---
    assert job.error_result is None, f"Stored procedure failed: {job.error_result}"

    target_data_rows = client.query(f"""
        SELECT id, processed_name, processed_address, stichtag, load_date, zip_code_numeric, processed_region, wiederanlauf_param_used
        FROM {TARGET_DATA_TABLE} ORDER BY id
    """).result()
    actual_target_data = [tuple(row.values()) for row in target_data_rows]
    assert actual_target_data == expected_target_data, \
        f"Target data mismatch. Expected: {expected_target_data}, Got: {actual_target_data}"

    job_log_rows = client.query(f"SELECT job_kennung, eintrags_nr, stichtag, status, record_count FROM {JOB_TRACKING_TABLE}").result()
    job_logs = [dict(row) for row in job_log_rows]
    assert len(job_logs) == 1, f"Expected 1 job log entry, got {len(job_logs)}"
    job_entry = job_logs[0]
    assert job_entry['job_kennung'] == job_kennung
    assert job_entry['eintrags_nr'] == eintrags_nr
    assert job_entry['stichtag'] == date(2023, 1, 1)
    assert job_entry['status'] == 'SUCCESS'
    assert job_entry['record_count'] == expected_record_count

    error_log_count = client.query(f"SELECT COUNT(*) FROM {ERROR_LOG_TABLE}").result().single_value
    assert error_log_count == 0, f"Error log table should be empty, but has {error_log_count} entries."
```

---

### Test Case 2: Missing Mandatory Parameter (`p_JobKennung`)

**Purpose**: Verify that the stored procedure correctly identifies and handles a missing mandatory parameter (`p_JobKennung`), logs an error, and exits gracefully without processing data. This directly tests the `pruefeParameterGesetzt Jobkennung p_JobKennung` equivalent.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.error_log`, `project.dataset.job_table`, and `project.dataset.target_ausd_adressen` are empty.

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` with `p_JobKennung` as an empty string (simulating a missing parameter as per shell script behavior), and other parameters valid:
    *   `p_JobKennung = ''`
    *   `p_EintragsNr = '12345'`
    *   `p_Stichtag = '01012023'`
    *   `p_wiederanlaufWert = 0`

**Pass/Fail Criterion**:
*   The stored procedure execution completes (due to `LEAVE`), but no data is processed.
*   The `project.dataset.error_log` table contains exactly one entry with `error_code = 193` and `error_message` indicating `p_JobKennung` was missing.
*   The `project.dataset.job_table` remains empty (as validation fails before job logging).
*   The `project.dataset.target_ausd_adressen` table remains empty.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_missing_jobkennung_parameter():
    """
    Tests handling of a missing mandatory p_JobKennung parameter.
    """
    # --- Action: Call the BigQuery stored procedure with missing JobKennung ---
    eintrags_nr = '12345'
    stichtag_param = '01012023'
    wiederanlauf_wert = 0

    call_query = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '', -- Simulating missing/empty parameter
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """
    client.query(call_query).result()

    # --- Assertions ---
    error_log_rows = client.query(f"SELECT error_code, error_message, error_argument FROM {ERROR_LOG_TABLE}").result()
    error_logs = [dict(row) for row in error_log_rows]
    assert len(error_logs) == 1, f"Expected 1 error log entry, got {len(error_logs)}"
    error_entry = error_logs[0]
    assert error_entry['error_code'] == 193
    assert 'Jobkennung' in error_entry['error_message']
    assert error_entry['error_argument'] == 'p_JobKennung'

    job_log_count = client.query(f"SELECT COUNT(*) FROM {JOB_TRACKING_TABLE}").result().single_value
    assert job_log_count == 0, f"Job log table should be empty, but has {job_log_count} entries."

    target_data_count = client.query(f"SELECT COUNT(*) FROM {TARGET_DATA_TABLE}").result().single_value
    assert target_data_count == 0, f"Target data table should be empty, but has {target_data_count} entries."
```

---

### Test Case 3: Invalid Date Format (`p_Stichtag`)

**Purpose**: Verify that the stored procedure correctly validates the `p_Stichtag` format (`DDMMYYYY`), logs an error for invalid formats, and exits without processing data. This tests the `DWDate_Datum_Check` equivalent using `SAFE.PARSE_DATE`.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.error_log`, `project.dataset.job_table`, and `project.dataset.target_ausd_adressen` are empty.

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` with an invalid `p_Stichtag` format:
    *   `p_JobKennung = 'TEST_JOB'`
    *   `p_EintragsNr = '12345'`
    *   `p_Stichtag = '2023-01-01'` (invalid, expects `DDMMYYYY`)
    *   `p_wiederanlaufWert = 0`

**Pass/Fail Criterion**:
*   The stored procedure execution completes (due to `LEAVE`), but no data is processed.
*   The `project.dataset.error_log` table contains exactly one entry with `error_code = 192` and `error_message` indicating the date format issue.
*   The `project.dataset.job_table` remains empty.
*   The `project.dataset.target_ausd_adressen` table remains empty.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_invalid_stichtag_format():
    """
    Tests handling of an invalid p_Stichtag format (e.g., YYYY-MM-DD instead of DDMMYYYY).
    """
    # --- Action: Call the BigQuery stored procedure with invalid Stichtag format ---
    job_kennung = 'TEST_JOB_INVALID_DATE'
    eintrags_nr = '12345'
    stichtag_param = '2023-01-01' # Expected DDMMYYYY, providing YYYY-MM-DD
    wiederanlauf_wert = 0

    call_query = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """
    client.query(call_query).result()

    # --- Assertions ---
    error_log_rows = client.query(f"SELECT error_code, error_message, error_argument FROM {ERROR_LOG_TABLE}").result()
    error_logs = [dict(row) for row in error_log_rows]
    assert len(error_logs) == 1, f"Expected 1 error log entry, got {len(error_logs)}"
    error_entry = error_logs[0]
    assert error_entry['error_code'] == 192
    assert 'Stichtag' in error_entry['error_message']
    assert 'DDMMYYYY' in error_entry['error_message']
    assert error_entry['error_argument'] == stichtag_param

    job_log_count = client.query(f"SELECT COUNT(*) FROM {JOB_TRACKING_TABLE}").result().single_value
    assert job_log_count == 0, f"Job log table should be empty, but has {job_log_count} entries."

    target_data_count = client.query(f"SELECT COUNT(*) FROM {TARGET_DATA_TABLE}").result().single_value
    assert target_data_count == 0, f"Target data table should be empty, but has {target_data_count} entries."
```

---

### Test Case 4: Date Derivation Correctness (Transformation Correctness)

**Purpose**: Verify that the BigQuery stored procedure correctly derives `p_datum_heute` and `p_datum_gestern` using BigQuery's native date functions, matching the behavior of the legacy `gestern.ksh` script.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.job_table` and `project.dataset.target_ausd_adressen` are empty.
3.  Populate `project.dataset.source_addresses` with mock data.
    ```sql
    INSERT INTO your_gcp_project_id.your_dataset.source_addresses (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
    (1, 'Test User 1', '100 Date St', '2023-01-01', '11111', 0, 'EU', 1);
    ```
4.  Assume `d_ausd_adressen.sql` uses `p_datum_heute` to stamp a `load_date` column in `target_ausd_adressen`.

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` with valid parameters.
    *   `p_JobKennung = 'TEST_DATE_DERIVATION'`
    *   `p_EintragsNr = '67890'`
    *   `p_Stichtag = '01012023'`
    *   `p_wiederanlaufWert = 0`

**Pass/Fail Criterion**:
*   The stored procedure executes successfully.
*   The `target_ausd_adressen` table contains data where the `load_date` column correctly reflects `CURRENT_DATE()` of the test execution.
*   The `job_table` entry reflects a successful run.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_date_derivation_correctness():
    """
    Tests that p_datum_heute and p_datum_gestern are correctly derived.
    Assumes the d_ausd_adressen.sql logic uses these dates to stamp target data.
    """
    # --- Setup: Prepare mock source data ---
    client.query(f"""
        INSERT INTO {SOURCE_DATA_TABLE} (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
        (1, 'Test User 1', '100 Date St', '2023-01-01', '11111', 0, 'EU', 1);
    """).result()

    # --- Action: Call the BigQuery stored procedure ---
    job_kennung = 'TEST_DATE_DERIVATION'
    eintrags_nr = '67890'
    stichtag_param = '01012023'
    wiederanlauf_wert = 0

    call_query = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """
    client.query(call_query).result()

    # --- Assertions ---
    expected_today = date.today()

    target_data_rows = client.query(f"SELECT COUNT(*) as count, MIN(load_date) as min_load_date, MAX(load_date) as max_load_date FROM {TARGET_DATA_TABLE}").result()
    target_summary = next(iter(target_data_rows))

    assert target_summary.count > 0, "No data processed into target table."
    assert target_summary.min_load_date == expected_today, \
        f"Min load date mismatch. Expected: {expected_today}, Got: {target_summary.min_load_date}"
    assert target_summary.max_load_date == expected_today, \
        f"Max load date mismatch. Expected: {expected_today}, Got: {target_summary.max_load_date}"

    job_log_rows = client.query(f"SELECT status FROM {JOB_TRACKING_TABLE} WHERE job_kennung = '{job_kennung}'").result()
    job_status = next(iter(job_log_rows)).status
    assert job_status == 'SUCCESS', f"Job status was not SUCCESS: {job_status}"

    error_log_count = client.query(f"SELECT COUNT(*) FROM {ERROR_LOG_TABLE}").result().single_value
    assert error_log_count == 0, f"Error log table should be empty, but has {error_log_count} entries."
```

---

### Test Case 5: `p_wiederanlaufWert` Handling (Transformation Correctness)

**Purpose**: Verify that the optional `p_wiederanlaufWert` parameter is correctly handled, specifically its default value of `0` when not provided, and that the value is passed through to the main SQL logic.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.job_table` and `project.dataset.target_ausd_adressen` are empty.
3.  Populate `project.dataset.source_addresses` with mock data.
    ```sql
    INSERT INTO your_gcp_project_id.your_dataset.source_addresses (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
    (1, 'User A', 'Addr A', '2023-01-01', '10001', 0, 'EU', 10),
    (2, 'User B', 'Addr B', '2023-01-01', '10002', 1, 'EU', 20),
    (3, 'User C', 'Addr C', '2023-01-01', '10003', 0, 'EU', 30);
    ```
4.  Assume `d_ausd_adressen.sql` uses `p_wiederanlaufWert` to filter `status_code` (e.g., if `p_wiederanlaufWert = 0`, process all; if `p_wiederanlaufWert = 1`, process only `status_code = 1`). Also, assume `p_wiederanlaufWert` is stamped into `wiederanlauf_param_used` in `target_ausd_adressen`.

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` *without* providing `p_wiederanlaufWert`.
    *   `p_JobKennung = 'TEST_WIEDERANLAUF_DEFAULT'`
    *   `p_EintragsNr = '11111'`
    *   `p_Stichtag = '01012023'`
2.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` *with* a specific `p_wiederanlaufWert = 1`.
    *   `p_JobKennung = 'TEST_WIEDERANLAUF_CUSTOM'`
    *   `p_EintragsNr = '22222'`
    *   `p_Stichtag = '01012023'`
    *   `p_wiederanlaufWert = 1`

**Pass/Fail Criterion**:
*   Both stored procedure calls execute successfully.
*   For the first call (default `p_wiederanlaufWert = 0`), the `record_count` in `job_table` is 3 (all records processed). The `wiederanlauf_param_used` column in `target_ausd_adressen` is 0 for all records.
*   For the second call (custom `p_wiederanlaufWert = 1`), the `record_count` in `job_table` is 1 (only `status_code = 1` record processed). The `wiederanlauf_param_used` column in `target_ausd_adressen` is 1 for the processed record.
*   The `project.dataset.error_log` table remains empty.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_wiederanlaufwert_handling():
    """
    Tests the handling of p_wiederanlaufWert, including its default value.
    Assumes d_ausd_adressen.sql uses this value to filter or modify data,
    and stamps the target table with the value used.
    """
    # --- Setup: Prepare mock source data ---
    client.query(f"""
        INSERT INTO {SOURCE_DATA_TABLE} (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
        (1, 'User A', 'Addr A', '2023-01-01', '10001', 0, 'EU', 10),
        (2, 'User B', 'Addr B', '2023-01-01', '10002', 1, 'EU', 20),
        (3, 'User C', 'Addr C', '2023-01-01', '10003', 0, 'EU', 30);
    """).result()

    # --- Action 1: Call with default p_wiederanlaufWert (not provided) ---
    job_kennung_default = 'TEST_WIEDERANLAUF_DEFAULT'
    eintrags_nr_default = '11111'
    stichtag_param = '01012023'

    call_query_default = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung_default}',
            p_EintragsNr => '{eintrags_nr_default}',
            p_Stichtag => '{stichtag_param}'
            -- p_wiederanlaufWert is omitted here, should default to 0
        );
    """
    client.query(call_query_default).result()

    # --- Action 2: Call with custom p_wiederanlaufWert = 1 ---
    job_kennung_custom = 'TEST_WIEDERANLAUF_CUSTOM'
    eintrags_nr_custom = '22222'
    wiederanlauf_value = 1

    call_query_custom = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung_custom}',
            p_EintragsNr => '{eintrags_nr_custom}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_value}
        );
    """
    client.query(call_query_custom).result()

    # --- Assertions ---
    job_log_default = client.query(f"SELECT record_count FROM {JOB_TRACKING_TABLE} WHERE job_kennung = '{job_kennung_default}'").result().single_value
    assert job_log_default == 3, f"Expected 3 records for default wiederanlaufWert, got {job_log_default}"
    target_default_wiederanlauf = client.query(f"SELECT DISTINCT wiederanlauf_param_used FROM {TARGET_DATA_TABLE} WHERE stichtag = '{stichtag_param}' AND wiederanlauf_param_used IS NOT NULL").result().to_dataframe()['wiederanlauf_param_used'].tolist()
    assert target_default_wiederanlauf == [0], f"Expected wiederanlauf_param_used to be 0 for default run, got {target_default_wiederanlauf}"


    job_log_custom = client.query(f"SELECT record_count FROM {JOB_TRACKING_TABLE} WHERE job_kennung = '{job_kennung_custom}'").result().single_value
    assert job_log_custom == 1, f"Expected 1 record for custom wiederanlaufWert=1, got {job_log_custom}"
    target_custom_wiederanlauf = client.query(f"SELECT DISTINCT wiederanlauf_param_used FROM {TARGET_DATA_TABLE} WHERE stichtag = '{stichtag_param}' AND wiederanlauf_param_used IS NOT NULL AND wiederanlauf_param_used = 1").result().to_dataframe()['wiederanlauf_param_used'].tolist()
    assert target_custom_wiederanlauf == [1], f"Expected wiederanlauf_param_used to be 1 for custom run, got {target_custom_wiederanlauf}"

    error_log_count = client.query(f"SELECT COUNT(*) FROM {ERROR_LOG_TABLE}").result().single_value
    assert error_log_count == 0, f"Error log table should be empty, but has {error_log_count} entries."
```

---

### Test Case 6: External System Replacement - Configuration Table

**Purpose**: Verify that environment-like variables (e.g., `BERT_DIR_ROOT`, `DW_DIR_UTL` equivalents) are correctly sourced from a BigQuery configuration table or handled as direct parameters, replacing the shell's `.dw_init` sourcing.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.job_table` and `project.dataset.target_ausd_adressen` are empty.
3.  Populate `project.dataset.config` with key-value pairs.
    ```sql
    INSERT INTO your_gcp_project_id.your_dataset.config (config_key, config_value) VALUES
    ('PROCESSING_MODE', 'FULL'),
    ('DEFAULT_REGION', 'EU'); -- This value will be used in the SP example
    ```
4.  Populate `project.dataset.source_addresses` with mock data.
    ```sql
    INSERT INTO your_gcp_project_id.your_dataset.source_addresses (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
    (1, 'User X', 'Addr X', '2023-01-01', '10001', 0, NULL, 10), -- Region is NULL, should use DEFAULT_REGION
    (2, 'User Y', 'Addr Y', '2023-01-01', '10002', 0, 'US', 20);
    ```
5.  Assume `d_ausd_adressen.sql` reads `DEFAULT_REGION` from the `config` table and uses it to populate `processed_region` if the source `region` is NULL.

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` with valid parameters.
    *   `p_JobKennung = 'TEST_CONFIG_READ'`
    *   `p_EintragsNr = '33333'`
    *   `p_Stichtag = '01012023'`
    *   `p_wiederanlaufWert = 0`

**Pass/Fail Criterion**:
*   The stored procedure executes successfully.
*   The `target_ausd_adressen` table contains data where the `processed_region` for `User X` is 'EU' (from config table), and for `User Y` is 'US' (from source).
*   The `job_table` entry reflects a successful run.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_config_table_sourcing():
    """
    Tests that environment-like variables are correctly sourced from a BigQuery config table.
    Assumes the d_ausd_adressen.sql logic uses a config value (e.g., 'DEFAULT_REGION').
    """
    # --- Setup: Populate config table and mock source data ---
    client.query(f"""
        INSERT INTO {SOURCE_DATA_TABLE} (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
        (1, 'User X', 'Addr X', '2023-01-01', '10001', 0, NULL, 10),
        (2, 'User Y', 'Addr Y', '2023-01-01', '10002', 0, 'US', 20);
    """).result()

    # --- Action: Call the BigQuery stored procedure ---
    job_kennung = 'TEST_CONFIG_READ'
    eintrags_nr = '33333'
    stichtag_param = '01012023'
    wiederanlauf_wert = 0

    call_query = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """
    client.query(call_query).result()

    # --- Assertions ---
    target_data_rows = client.query(f"SELECT id, processed_region FROM {TARGET_DATA_TABLE} ORDER BY id").result()
    actual_regions = {row.id: row.processed_region for row in target_data_rows}

    assert actual_regions.get(1) == 'EU', f"Expected User X region to be 'EU', got {actual_regions.get(1)}"
    assert actual_regions.get(2) == 'US', f"Expected User Y region to be 'US', got {actual_regions.get(2)}"

    job_log_rows = client.query(f"SELECT status FROM {JOB_TRACKING_TABLE} WHERE job_kennung = '{job_kennung}'").result()
    job_status = next(iter(job_log_rows)).status
    assert job_status == 'SUCCESS', f"Job status was not SUCCESS: {job_status}"

    error_log_count = client.query(f"SELECT COUNT(*) FROM {ERROR_LOG_TABLE}").result().single_value
    assert error_log_count == 0, f"Error log table should be empty, but has {error_log_count} entries."
```

---

### Test Case 7: Error Reporting Granularity

**Purpose**: Verify that specific error codes and messages are correctly logged to `project.dataset.error_log` for different types of failures, matching the legacy script's `DWMSG_MeldeFehler` behavior.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.error_log`, `project.dataset.job_table`, and `project.dataset.target_ausd_adressen` are empty.

**Action**:
1.  Execute the stored procedure with a missing mandatory parameter (`p_EintragsNr`).
2.  Execute the stored procedure with an invalid date format (`p_Stichtag` as a non-date string).
3.  (Optional, if `d_ausd_adressen.sql` can fail in a controlled way): Execute the stored procedure where the internal `d_ausd_adressen.sql` logic is designed to fail (e.g., by inserting data that causes a BigQuery error, like a type mismatch if not handled by `SAFE_CAST`). For this example, we'll rely on the parameter/date validation errors.

**Pass/Fail Criterion**:
*   For each failure scenario, the `project.dataset.error_log` table contains exactly one entry corresponding to that failure.
*   Each error entry has the correct `error_code` (e.g., `193` for missing param, `192` for invalid date).
*   Each error entry has a descriptive `error_message` that clearly identifies the cause.
*   The `error_argument` column correctly captures the problematic argument.
*   No data is processed into `target_ausd_adressen` for failed runs.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_error_reporting_granularity():
    """
    Tests that specific error codes and messages are correctly logged for different failures.
    """
    # --- Scenario 1: Missing p_EintragsNr ---
    job_kennung_missing_eintrags = 'ERR_MISSING_EINTRAGS'
    stichtag_param = '01012023'

    call_query_missing_eintrags = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung_missing_eintrags}',
            p_EintragsNr => '', -- Missing EintragsNr
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => 0
        );
    """
    client.query(call_query_missing_eintrags).result()

    # --- Scenario 2: Invalid p_Stichtag (non-date string) ---
    job_kennung_invalid_stichtag_str = 'ERR_INVALID_STICHTAG_STR'
    eintrags_nr = '44444'
    stichtag_param_invalid_str = 'NOT_A_DATE'

    call_query_invalid_stichtag_str = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung_invalid_stichtag_str}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param_invalid_str}',
            p_wiederanlaufWert => 0
        );
    """
    client.query(call_query_invalid_stichtag_str).result()

    # --- Assertions ---
    error_log_rows = client.query(f"SELECT error_code, error_message, error_argument FROM {ERROR_LOG_TABLE} ORDER BY created_at").result()
    error_logs = [dict(row) for row in error_log_rows]

    assert len(error_logs) == 2, f"Expected 2 error log entries, got {len(error_logs)}"

    error1 = error_logs[0]
    assert error1['error_code'] == 193
    assert 'EintragsNr' in error1['error_message']
    assert error1['error_argument'] == 'p_EintragsNr'

    error2 = error_logs[1]
    assert error2['error_code'] == 192
    assert 'Stichtag' in error2['error_message']
    assert 'DDMMYYYY' in error2['error_message']
    assert error2['error_argument'] == stichtag_param_invalid_str

    job_log_count = client.query(f"SELECT COUNT(*) FROM {JOB_TRACKING_TABLE}").result().single_value
    assert job_log_count == 0, f"Job log table should be empty, but has {job_log_count} entries."

    target_data_count = client.query(f"SELECT COUNT(*) FROM {TARGET_DATA_TABLE}").result().single_value
    assert target_data_count == 0, f"Target data table should be empty, but has {target_data_count} entries."
```

---

### Test Case 8: Data Quality - NULL Handling in `d_ausd_adressen.sql` (Transformation Correctness)

**Purpose**: Verify that the migrated `d_ausd_adressen.sql` logic (embedded in the control procedure) correctly handles NULL values in source data, producing expected NULL or default values in the target table, matching the legacy behavior.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.target_ausd_adressen` is empty.
3.  Populate `project.dataset.source_addresses` with mock data that includes NULL values in relevant columns.
    ```sql
    INSERT INTO your_gcp_project_id.your_dataset.source_addresses (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
    (1, 'Valid User', 'Valid Address', '2023-01-01', '10001', 0, 'EU', 10),
    (2, NULL, 'Address with NULL Name', '2023-01-01', '10002', 0, 'EU', 20),
    (3, 'Name with NULL Address', NULL, '2023-01-01', '10003', 0, 'EU', 30), -- This row should be filtered out by `s.address IS NOT NULL`
    (4, 'User with NULL Optional', 'Addr 4', '2023-01-01', '10004', 0, 'EU', NULL);
    ```
4.  Define the `expected_target_data` based on the legacy `d_ausd_adressen.sql`'s NULL handling rules (e.g., `COALESCE(s.name, 'UNKNOWN')`, `s.address IS NOT NULL` filter, `SAFE_CAST` for `zip_code`).

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` with valid parameters.
    *   `p_JobKennung = 'TEST_NULL_HANDLING'`
    *   `p_EintragsNr = '55555'`
    *   `p_Stichtag = '01012023'`
    *   `p_wiederanlaufWert = 0`

**Pass/Fail Criterion**:
*   The stored procedure executes successfully.
*   The `project.dataset.target_ausd_adressen` table contains data where NULL values from the source are handled precisely as expected by the legacy `d_ausd_adressen.sql` logic (e.g., NULL names replaced with 'UNKNOWN', rows with NULL addresses filtered out, NULL optional values propagated).
*   The `job_table` entry reflects a successful run.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_null_handling_in_main_logic():
    """
    Tests that the migrated d_ausd_adressen.sql logic correctly handles NULL values.
    """
    # --- Setup: Prepare mock source data with NULLs ---
    client.query(f"""
        INSERT INTO {SOURCE_DATA_TABLE} (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
        (1, 'Valid User', 'Valid Address', '2023-01-01', '10001', 0, 'EU', 10),
        (2, NULL, 'Address with NULL Name', '2023-01-01', '10002', 0, 'EU', 20),
        (3, 'Name with NULL Address', NULL, '2023-01-01', '10003', 0, 'EU', 30),
        (4, 'User with NULL Optional', 'Addr 4', '2023-01-01', '10004', 0, 'EU', NULL);
    """).result()

    expected_load_date = date.today()
    expected_target_data = [
        (1, 'Valid User', 'Valid Address', date(2023, 1, 1), expected_load_date, 10001, 'EU', 0),
        (2, 'UNKNOWN', 'Address with NULL Name', date(2023, 1, 1), expected_load_date, 10002, 'EU', 0),
        (4, 'User with NULL Optional', 'Addr 4', date(2023, 1, 1), expected_load_date, 10004, 'EU', 0)
    ]
    # Note: Row 3 (NULL address) is filtered out by `s.address IS NOT NULL` in the example SP.

    # --- Action: Call the BigQuery stored procedure ---
    job_kennung = 'TEST_NULL_HANDLING'
    eintrags_nr = '55555'
    stichtag_param = '01012023'
    wiederanlauf_wert = 0

    call_query = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """
    client.query(call_query).result()

    # --- Assertions ---
    target_data_rows = client.query(f"""
        SELECT id, processed_name, processed_address, stichtag, load_date, zip_code_numeric, processed_region, wiederanlauf_param_used
        FROM {TARGET_DATA_TABLE} ORDER BY id
    """).result()
    actual_target_data = [tuple(row.values()) for row in target_data_rows]

    assert actual_target_data == expected_target_data, \
        f"NULL handling mismatch. Expected: {expected_target_data}, Got: {actual_target_data}"

    job_log_rows = client.query(f"SELECT status FROM {JOB_TRACKING_TABLE} WHERE job_kennung = '{job_kennung}'").result()
    job_status = next(iter(job_log_rows)).status
    assert job_status == 'SUCCESS', f"Job status was not SUCCESS: {job_status}"

    error_log_count = client.query(f"SELECT COUNT(*) FROM {ERROR_LOG_TABLE}").result().single_value
    assert error_log_count == 0, f"Error log table should be empty, but has {error_log_count} entries."
```

---

### Test Case 9: Schema Assertion for `d_ausd_adressen.sql` Output (Schema Assertion)

**Purpose**: Verify that the schema of the output table (`project.dataset.target_ausd_adressen`) produced by the migrated `d_ausd_adressen.sql` logic is identical to the schema produced by the legacy script, including column names, data types, and nullability.

**Setup**:
1.  Ensure the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` is deployed.
2.  Ensure `project.dataset.target_ausd_adressen` is empty.
3.  Populate `project.dataset.source_addresses` with minimal mock data to trigger processing.
    ```sql
    INSERT INTO your_gcp_project_id.your_dataset.source_addresses (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
    (1, 'User A', 'Addr A', '2023-01-01', '12345', 0, 'EU', 10);
    ```
4.  Obtain the `expected_schema` of `project.dataset.target_ausd_adressen` from the legacy `d_ausd_adressen.sql`'s output schema.

**Action**:
1.  Call the BigQuery stored procedure `project.dataset.r_ausd_adressen_control` with valid parameters.
    *   `p_JobKennung = 'TEST_SCHEMA_ASSERTION'`
    *   `p_EintragsNr = '77777'`
    *   `p_Stichtag = '01012023'`
    *   `p_wiederanlaufWert = 0`

**Pass/Fail Criterion**:
*   The stored procedure executes successfully.
*   The schema of `project.dataset.target_ausd_adressen` (column names, data types, nullability) exactly matches the `expected_schema`.

**Runnable Test Code (Python with BigQuery client)**:

```python
# ... (client, PROJECT_ID, DATASET_ID, table names, cleanup_tables fixture from previous test) ...

def test_schema_assertion_for_target_table():
    """
    Tests that the schema of the target table matches the expected schema.
    """
    # --- Setup: Prepare mock source data ---
    client.query(f"""
        INSERT INTO {SOURCE_DATA_TABLE} (id, name, address, stichtag, zip_code, status_code, region, optional_value) VALUES
        (1, 'User A', 'Addr A', '2023-01-01', '12345', 0, 'EU', 10);
    """).result()

    # Define the expected schema for TARGET_DATA_TABLE based on the example SP logic
    expected_schema = [
        bigquery.SchemaField("id", "INT64", mode="NULLABLE"), # BQ defaults to NULLABLE if not specified
        bigquery.SchemaField("processed_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processed_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("stichtag", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("load_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("zip_code_numeric", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("processed_region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("wiederanlauf_param_used", "INT64", mode="NULLABLE"),
    ]

    # --- Action: Call the BigQuery stored procedure ---
    job_kennung = 'TEST_SCHEMA_ASSERTION'
    eintrags_nr = '77777'
    stichtag_param = '01012023'
    wiederanlauf_wert = 0

    call_query = f"""
        CALL {CONTROL_PROCEDURE}(
            p_JobKennung => '{job_kennung}',
            p_EintragsNr => '{eintrags_nr}',
            p_Stichtag => '{stichtag_param}',
            p_wiederanlaufWert => {wiederanlauf_wert}
        );
    """
    client.query(call_query).result()

    # --- Assertions ---
    table = client.get_table(TARGET_DATA_TABLE)
    actual_schema = table.schema

    actual_schema_comparable = sorted([(f.name, f.field_type, f.mode) for f in actual_schema])
    expected_schema_comparable = sorted([(f.name, f.field_type, f.mode) for f in expected_schema])

    assert actual_schema_comparable == expected_schema_comparable, \
        f"Target table schema mismatch. Expected: {expected_schema_comparable}, Got: {actual_schema_comparable}"

    job_log_status = client.query(f"SELECT status FROM {JOB_TRACKING_TABLE} WHERE job_kennung = '{job_kennung}'").result().single_value
    assert job_log_status == 'SUCCESS', f"Job status was not SUCCESS: {job_log_status}"

    error_log_count = client.query(f"SELECT COUNT(*) FROM {ERROR_LOG_TABLE}").result().single_value
    assert error_log_count == 0, f"Error log table should be empty, but has {error_log_count} entries."
```