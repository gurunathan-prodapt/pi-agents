As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `f_alis_msgerr.ksh`. The migration shifts core logging and error handling functionalities from KornShell/Oracle PL/SQL to BigQuery Stored Procedures, orchestrated by a Python layer.

The following test cases are designed to validate the behavioral equivalence and correctness of the migrated solution, covering output parity, transformation logic, external system replacements, and data quality.

---

## Test Environment Setup

Before running the tests, ensure the following:

1.  **BigQuery Project and Dataset:** A GCP project and a BigQuery dataset are provisioned.
2.  **BigQuery Schema:** The `bert_meldung` and `bert_meldung_fehler` tables are created in the target BigQuery dataset using `sql/ddl/bert_meldung_schema.sql`.
3.  **BigQuery Stored Procedures:** All BigQuery Stored Procedures (`dwmsg_ermittle_nr`, `dwmsg_erzeuge_eintrag`, `dwmsg_setze_status_ok`, `dwmsg_setze_status_abbruch`, `dwmsg_melde_fehler`, `dwmsg_logdateiname`, `dwmsg_setze_stichtag_info`, `dwmsg_append_timing_infos`, `dwmsg_fehlerbehandlung`) are deployed to the target BigQuery dataset.
4.  **Python Environment:** A Python environment with `google-cloud-bigquery` installed.
5.  **Configuration:** The `PROJECT_ID` and `DATASET_ID` placeholders in the Python orchestration example and test code are replaced with actual values.

For `pytest` based tests, a `conftest.py` or similar setup would be used to initialize the `BigQueryErrorLogger` and handle cleanup.

```python
# conftest.py (example)
import pytest
from google.cloud import bigquery
from python.orchestration_example import BigQueryErrorLogger # Assuming path

PROJECT_ID = "your-gcp-project-id" # Replace with your project ID
DATASET_ID = "your_dataset_id"    # Replace with your dataset ID

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def bq_logger():
    return BigQueryErrorLogger(PROJECT_ID, DATASET_ID)

@pytest.fixture(autouse=True)
def cleanup_tables(bq_client):
    """Cleans up tables before each test."""
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.bert_meldung`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler`").result()
    yield
```

---

## Test Cases

### 1. Test `dwmsg_ermittle_nr` (Generate Unique Entry Number)

*   **Purpose:** Verify that the `dwmsg_ermittle_nr` BigQuery Stored Procedure correctly generates a unique identifier, replacing the legacy Oracle sequence and temporary file mechanism.
*   **Setup:** Ensure the `dwmsg_ermittle_nr` procedure is deployed.
*   **Action:** Call the `dwmsg_ermittle_nr` procedure multiple times and capture the output.
*   **Pass/Fail Criterion:**
    *   The procedure returns a non-empty string.
    *   Each call returns a unique string.
    *   The returned string is a valid UUID format (or similar unique identifier as per `GENERATE_UUID()`).

```python
# pytest test_ermittle_nr.py
import re
import pytest

def test_dwmsg_ermittle_nr_generates_unique_id(bq_logger):
    """Verifies that dwmsg_ermittle_nr generates unique, non-empty IDs."""
    ids = set()
    for _ in range(5): # Generate multiple IDs to check uniqueness
        entry_nr = bq_logger.dwmsg_ermittle_nr()
        assert entry_nr is not None and entry_nr != ""
        assert entry_nr not in ids
        ids.add(entry_nr)
        # Basic UUID format check (e.g., xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx)
        assert re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', entry_nr)

def test_dwmsg_ermittle_nr_returns_string_type(bq_logger):
    """Verifies the return type is string."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    assert isinstance(entry_nr, str)
```

### 2. Test `dwmsg_erzeuge_eintrag` (Create New Log Entry)

*   **Purpose:** Verify that `dwmsg_erzeuge_eintrag` correctly inserts a new record into the `bert_meldung` table with all provided and default values. This replaces the Oracle `INSERT` logic.
*   **Setup:** Ensure `bert_meldung` table is empty.
*   **Action:** Call `dwmsg_erzeuge_eintrag` with valid parameters. Query `bert_meldung` to check the inserted data.
*   **Pass/Fail Criterion:**
    *   One row is inserted into `bert_meldung`.
    *   All input parameters (`eintrags_nr`, `job_kennung`, `programm_name`, `log_datei`, `typ`) match the inserted values.
    *   `status` is 'IN_PROGRESS'.
    *   `creation_timestamp` and `last_update_timestamp` are populated and close to the current time.
    *   Calling with `NULL` or empty `p_eintrags_nr` raises an error (`SIGNAL SQLSTATE '45000'`).

```python
# pytest test_erzeuge_eintrag.py
import pytest
import datetime

def test_dwmsg_erzeuge_eintrag_success(bq_logger, bq_client):
    """Verifies successful insertion of a new entry."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    job_kennung = "TEST_JOB_001"
    programm_name = "test_script.py"
    log_datei = "/tmp/test_log.log"
    typ = "INFO"

    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, job_kennung, programm_name, log_datei, typ)

    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())

    assert len(rows) == 1
    row = rows[0]
    assert row.eintrags_nr == entry_nr
    assert row.job_kennung == job_kennung
    assert row.programm_name == programm_name
    assert row.log_datei == log_datei
    assert row.typ == typ
    assert row.status == "IN_PROGRESS"
    assert isinstance(row.creation_timestamp, datetime.datetime)
    assert isinstance(row.last_update_timestamp, datetime.datetime)
    assert (datetime.datetime.now(datetime.timezone.utc) - row.creation_timestamp).total_seconds() < 5

def test_dwmsg_erzeuge_eintrag_null_eintrags_nr_fails(bq_logger):
    """Verifies error handling for NULL eintrags_nr."""
    with pytest.raises(Exception, match="p_eintrags_nr cannot be empty or NULL"):
        bq_logger.dwmsg_erzeuge_eintrag(None, "JOB", "PROG", "LOG", "INFO")

def test_dwmsg_erzeuge_eintrag_empty_eintrags_nr_fails(bq_logger):
    """Verifies error handling for empty eintrags_nr."""
    with pytest.raises(Exception, match="p_eintrags_nr cannot be empty or NULL"):
        bq_logger.dwmsg_erzeuge_eintrag("", "JOB", "PROG", "LOG", "INFO")
```

### 3. Test `dwmsg_setze_status_ok` (Set Status to OK)

*   **Purpose:** Verify that `dwmsg_setze_status_ok` correctly updates the `status` and `last_update_timestamp` for a given entry. This replaces the Oracle `UPDATE` logic.
*   **Setup:** Insert a test entry into `bert_meldung` with an initial status (e.g., 'IN_PROGRESS').
*   **Action:** Call `dwmsg_setze_status_ok` with the `eintrags_nr` of the test entry. Query `bert_meldung` to verify the update.
*   **Pass/Fail Criterion:**
    *   The `status` field for the specified `eintrags_nr` is updated to 'OK'.
    *   The `last_update_timestamp` is updated and is more recent than the `creation_timestamp`.
    *   Calling with `NULL` or empty `p_eintrags_nr` raises an error.
    *   No other rows are affected.

```python
# pytest test_setze_status_ok.py
import pytest
import datetime

def test_dwmsg_setze_status_ok_success(bq_logger, bq_client):
    """Verifies successful status update to OK."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_OK", "PROG_OK", "LOG_OK", "INFO")

    initial_row = list(bq_client.query(f"SELECT creation_timestamp, last_update_timestamp FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'").result())[0]
    initial_lud = initial_row.last_update_timestamp

    bq_logger.dwmsg_setze_status_ok(entry_nr)

    query = f"SELECT status, last_update_timestamp FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())

    assert len(rows) == 1
    assert rows[0].status == "OK"
    assert rows[0].last_update_timestamp > initial_lud # last_update_timestamp should be updated

def test_dwmsg_setze_status_ok_null_eintrags_nr_fails(bq_logger):
    """Verifies error handling for NULL eintrags_nr."""
    with pytest.raises(Exception, match="p_eintrags_nr cannot be empty or NULL"):
        bq_logger.dwmsg_setze_status_ok(None)
```

### 4. Test `dwmsg_setze_status_abbruch` (Set Status to ABORTED)

*   **Purpose:** Verify that `dwmsg_setze_status_abbruch` correctly updates the `status` and `last_update_timestamp` for a given entry. This replaces the Oracle `UPDATE` logic.
*   **Setup:** Insert a test entry into `bert_meldung` with an initial status.
*   **Action:** Call `dwmsg_setze_status_abbruch` with the `eintrags_nr` of the test entry. Query `bert_meldung` to verify the update.
*   **Pass/Fail Criterion:**
    *   The `status` field for the specified `eintrags_nr` is updated to 'ABORTED'.
    *   The `last_update_timestamp` is updated and is more recent than the `creation_timestamp`.
    *   Calling with `NULL` or empty `p_eintrags_nr` raises an error.
    *   No other rows are affected.

```python
# pytest test_setze_status_abbruch.py
import pytest
import datetime

def test_dwmsg_setze_status_abbruch_success(bq_logger, bq_client):
    """Verifies successful status update to ABORTED."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_ABORT", "PROG_ABORT", "LOG_ABORT", "INFO")

    initial_row = list(bq_client.query(f"SELECT creation_timestamp, last_update_timestamp FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'").result())[0]
    initial_lud = initial_row.last_update_timestamp

    bq_logger.dwmsg_setze_status_abbruch(entry_nr)

    query = f"SELECT status, last_update_timestamp FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())

    assert len(rows) == 1
    assert rows[0].status == "ABORTED"
    assert rows[0].last_update_timestamp > initial_lud # last_update_timestamp should be updated

def test_dwmsg_setze_status_abbruch_null_eintrags_nr_fails(bq_logger):
    """Verifies error handling for NULL eintrags_nr."""
    with pytest.raises(Exception, match="p_eintrags_nr cannot be empty or NULL"):
        bq_logger.dwmsg_setze_status_abbruch(None)
```

### 5. Test `dwmsg_melde_fehler` (Record Error Details)

*   **Purpose:** Verify that `dwmsg_melde_fehler` correctly inserts a new record into `bert_meldung_fehler` and updates the status of the corresponding `bert_meldung` entry. This replaces the Oracle error logging logic.
*   **Setup:** Insert a test entry into `bert_meldung`.
*   **Action:** Call `dwmsg_melde_fehler` with various combinations of parameters (including `NULL` for optional ones). Query `bert_meldung_fehler` and `bert_meldung` to verify.
*   **Pass/Fail Criterion:**
    *   One row is inserted into `bert_meldung_fehler`.
    *   All input parameters (`eintrags_nr`, `fehler_nr`, `fehler_text`, `zusatz_info_1`, `zusatz_info_2`, `variable_name`) match the inserted values.
    *   `error_id` is a unique UUID.
    *   `error_timestamp` is populated.
    *   The `bert_meldung` entry's `status` is updated to 'ERROR' (if not already 'OK' or 'ABORTED').
    *   The `bert_meldung` entry's `last_update_timestamp` is updated.
    *   Calling with `NULL` or empty `p_eintrags_nr` raises an error.
    *   Correct handling of `NULL` values for optional parameters (`zusatz_info_1`, `zusatz_info_2`, `variable_name`).

```python
# pytest test_melde_fehler.py
import pytest
import datetime
import re

def test_dwmsg_melde_fehler_full_params_success(bq_logger, bq_client):
    """Verifies successful error logging with all parameters."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_ERR", "PROG_ERR", "LOG_ERR", "INFO")

    fehler_nr = "1001"
    fehler_text = "File not found"
    zusatz_info_1 = "filename.txt"
    zusatz_info_2 = "path/to/file"
    variable_name = "FILE_PATH"

    bq_logger.dwmsg_melde_fehler(entry_nr, fehler_nr, fehler_text, zusatz_info_1, zusatz_info_2, variable_name)

    # Verify bert_meldung_fehler entry
    error_query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler` WHERE eintrags_nr = '{entry_nr}'"
    error_rows = list(bq_client.query(error_query).result())
    assert len(error_rows) == 1
    error_row = error_rows[0]
    assert re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', error_row.error_id)
    assert error_row.eintrags_nr == entry_nr
    assert error_row.fehler_nr == fehler_nr
    assert error_row.fehler_text == fehler_text
    assert error_row.zusatz_info_1 == zusatz_info_1
    assert error_row.zusatz_info_2 == zusatz_info_2
    assert error_row.variable_name == variable_name
    assert isinstance(error_row.error_timestamp, datetime.datetime)

    # Verify bert_meldung status update
    meldung_query = f"SELECT status, last_update_timestamp FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    meldung_rows = list(bq_client.query(meldung_query).result())
    assert len(meldung_rows) == 1
    assert meldung_rows[0].status == "ERROR"

def test_dwmsg_melde_fehler_null_optional_params(bq_logger, bq_client):
    """Verifies error logging with NULL optional parameters."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_ERR_NULL", "PROG_ERR_NULL", "LOG_ERR_NULL", "INFO")

    fehler_nr = "1002"
    fehler_text = "Generic error"
    # All optional parameters are None
    bq_logger.dwmsg_melde_fehler(entry_nr, fehler_nr, fehler_text, None, None, None)

    error_query = f"SELECT zusatz_info_1, zusatz_info_2, variable_name FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler` WHERE eintrags_nr = '{entry_nr}'"
    error_rows = list(bq_client.query(error_query).result())
    assert len(error_rows) == 1
    error_row = error_rows[0]
    assert error_row.zusatz_info_1 is None
    assert error_row.zusatz_info_2 is None
    assert error_row.variable_name is None

def test_dwmsg_melde_fehler_null_eintrags_nr_fails(bq_logger):
    """Verifies error handling for NULL eintrags_nr."""
    with pytest.raises(Exception, match="p_eintrags_nr cannot be empty or NULL"):
        bq_logger.dwmsg_melde_fehler(None, "1000", "Test Error")
```

### 6. Test `dwmsg_logdateiname` (Construct Log Filename)

*   **Purpose:** Verify that `dwmsg_logdateiname` correctly constructs a log filename string based on the provided parameters and current timestamp. This replaces the shell's `date` command and string concatenation.
*   **Setup:** Ensure the `dwmsg_logdateiname` procedure is deployed.
*   **Action:** Call `dwmsg_logdateiname` with sample `job_kennung` and `programm_name`.
*   **Pass/Fail Criterion:**
    *   The returned filename matches the expected format: `JOB_KENNUNG_PROGRAMM_NAME_YYYYMMDD_HHMMSS.log`.
    *   The timestamp part is accurate to the current time (within a few seconds).
    *   The output is a string.

```python
# pytest test_logdateiname.py
import pytest
import datetime
import re

def test_dwmsg_logdateiname_format_and_content(bq_logger):
    """Verifies the format and content of the generated log filename."""
    job_kennung = "MY_JOB"
    programm_name = "my_program.sh"
    
    log_filename = bq_logger.dwmsg_logdateiname(job_kennung, programm_name)

    assert isinstance(log_filename, str)
    assert log_filename.startswith(f"{job_kennung}_{programm_name}_")
    assert log_filename.endswith(".log")

    # Extract timestamp part and validate
    timestamp_str = log_filename.replace(f"{job_kennung}_{programm_name}_", "").replace(".log", "")
    
    # Check format YYYYMMDD_HHMMSS
    assert re.match(r'^\d{8}_\d{6}$', timestamp_str)

    # Convert to datetime object for comparison (assuming 'Europe/Berlin' timezone for BigQuery)
    # Note: Python's datetime.now() is UTC by default, BigQuery's CURRENT_TIMESTAMP() is UTC,
    # but FORMAT_TIMESTAMP uses 'Europe/Berlin'. Need to adjust for comparison.
    # For simplicity, we'll check the date part and approximate time.
    generated_dt = datetime.datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
    
    # Get current time in Europe/Berlin timezone
    import pytz
    berlin_tz = pytz.timezone('Europe/Berlin')
    current_dt_berlin = datetime.datetime.now(pytz.utc).astimezone(berlin_tz)
    
    # Compare date parts
    assert generated_dt.date() == current_dt_berlin.date()
    # Compare time, allowing for a small delta due to execution time
    time_delta = abs((generated_dt - current_dt_berlin.replace(tzinfo=None)).total_seconds())
    assert time_delta < 5 # Allow up to 5 seconds difference
```

### 7. Test `dwmsg_setze_stichtag_info` (Set Key Date Information)

*   **Purpose:** Verify that `dwmsg_setze_stichtag_info` correctly updates the `zusatz_infos` field in `bert_meldung`, handling date formatting and attempting JSON serialization. This replaces the Oracle `to_date`/`to_char` and string concatenation.
*   **Setup:** Insert a test entry into `bert_meldung`.
*   **Action:** Call `dwmsg_setze_stichtag_info` with various `stichtag_value` and `stichtag_format` combinations, and `info_text`. Test initial insertion and updating existing `zusatz_infos` (both JSON and non-JSON).
*   **Pass/Fail Criterion:**
    *   The `zusatz_infos` field for the specified `eintrags_nr` is updated.
    *   If `zusatz_infos` was initially `NULL` or empty, it becomes a JSON string containing `stichtag` and `stichtag_info_text`.
    *   If `zusatz_infos` was valid JSON, the `stichtag` and `stichtag_info_text` keys are updated/added within the JSON.
    *   If `zusatz_infos` was not valid JSON, the new info is appended as a concatenated string.
    *   The date formatting (`PARSE_TIMESTAMP`, `FORMAT_TIMESTAMP`) is correct.
    *   The `last_update_timestamp` is updated.
    *   Calling with `NULL` or empty `p_eintrags_nr` raises an error.

```python
# pytest test_setze_stichtag_info.py
import pytest
import json
import datetime

def test_dwmsg_setze_stichtag_info_initial_json(bq_logger, bq_client):
    """Verifies initial insertion of stichtag info as JSON."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_STI", "PROG_STI", "LOG_STI", "INFO")

    stichtag_value = "2023-10-26"
    stichtag_format = "%Y-%m-%d"
    info_text = "End of month processing"

    bq_logger.dwmsg_setze_stichtag_info(entry_nr, stichtag_value, stichtag_format, info_text)

    query = f"SELECT zusatz_infos FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())
    assert len(rows) == 1
    zusatz_infos = json.loads(rows[0].zusatz_infos)
    assert zusatz_infos["stichtag"] == stichtag_value
    assert zusatz_infos["stichtag_info_text"] == info_text

def test_dwmsg_setze_stichtag_info_update_existing_json(bq_logger, bq_client):
    """Verifies updating existing JSON zusatz_infos."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_STI_UPD", "PROG_STI_UPD", "LOG_STI_UPD", "INFO")

    # Initial info
    bq_client.query(f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.bert_meldung`
        SET zusatz_infos = '{{"existing_key": "existing_value"}}'
        WHERE eintrags_nr = '{entry_nr}'
    """).result()

    stichtag_value = "2023-11-15"
    stichtag_format = "%Y-%m-%d"
    info_text = "Mid-month run"

    bq_logger.dwmsg_setze_stichtag_info(entry_nr, stichtag_value, stichtag_format, info_text)

    query = f"SELECT zusatz_infos FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())
    assert len(rows) == 1
    zusatz_infos = json.loads(rows[0].zusatz_infos)
    assert zusatz_infos["existing_key"] == "existing_value"
    assert zusatz_infos["stichtag"] == stichtag_value
    assert zusatz_infos["stichtag_info_text"] == info_text

def test_dwmsg_setze_stichtag_info_non_json_fallback(bq_logger, bq_client):
    """Verifies fallback to string concatenation if zusatz_infos is not JSON."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_STI_FALL", "PROG_STI_FALL", "LOG_STI_FALL", "INFO")

    # Set non-JSON initial info
    bq_client.query(f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.bert_meldung`
        SET zusatz_infos = 'Some plain text info'
        WHERE eintrags_nr = '{entry_nr}'
    """).result()

    stichtag_value = "2023-12-01"
    stichtag_format = "%Y-%m-%d"
    info_text = "Start of month"

    bq_logger.dwmsg_setze_stichtag_info(entry_nr, stichtag_value, stichtag_format, info_text)

    query = f"SELECT zusatz_infos FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())
    assert len(rows) == 1
    expected_string = f"Some plain text info | Stichtag: {stichtag_value} Info: {info_text}"
    assert rows[0].zusatz_infos == expected_string

def test_dwmsg_setze_stichtag_info_null_eintrags_nr_fails(bq_logger):
    """Verifies error handling for NULL eintrags_nr."""
    with pytest.raises(Exception, match="p_eintrags_nr cannot be empty or NULL"):
        bq_logger.dwmsg_setze_stichtag_info(None, "2023-01-01", "%Y-%m-%d", "Info")
```

### 8. Test `dwmsg_append_timing_infos` (Append Timing Information)

*   **Purpose:** Verify that `dwmsg_append_timing_infos` correctly appends timing information to the `zusatz_infos` field, attempting JSON serialization. This replaces the Oracle string concatenation.
*   **Setup:** Insert a test entry into `bert_meldung`.
*   **Action:** Call `dwmsg_append_timing_infos` multiple times with different keys/values. Test initial insertion and appending to existing `zusatz_infos` (both JSON and non-JSON).
*   **Pass/Fail Criterion:**
    *   The `zusatz_infos` field for the specified `eintrags_nr` is updated.
    *   If `zusatz_infos` was initially `NULL` or empty, it becomes a JSON string with the new timing info.
    *   If `zusatz_infos` was valid JSON, the new timing info is added/updated within the JSON.
    *   If `zusatz_infos` was not valid JSON, the new info is appended as a concatenated string.
    *   The `last_update_timestamp` is updated.
    *   Calling with `NULL` or empty `p_eintrags_nr` raises an error.

```python
# pytest test_append_timing_infos.py
import pytest
import json
import datetime

def test_dwmsg_append_timing_infos_initial_json(bq_logger, bq_client):
    """Verifies initial insertion of timing info as JSON."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_TIM", "PROG_TIM", "LOG_TIM", "INFO")

    bq_logger.dwmsg_append_timing_infos(entry_nr, "step1_duration_ms", "1234")

    query = f"SELECT zusatz_infos FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())
    assert len(rows) == 1
    zusatz_infos = json.loads(rows[0].zusatz_infos)
    assert zusatz_infos["step1_duration_ms"] == "1234"

def test_dwmsg_append_timing_infos_multiple_json_updates(bq_logger, bq_client):
    """Verifies multiple updates to JSON zusatz_infos."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_TIM_MULTI", "PROG_TIM_MULTI", "LOG_TIM_MULTI", "INFO")

    bq_logger.dwmsg_append_timing_infos(entry_nr, "step1_duration_ms", "100")
    bq_logger.dwmsg_append_timing_infos(entry_nr, "step2_duration_ms", "250")
    bq_logger.dwmsg_append_timing_infos(entry_nr, "step1_duration_ms", "110") # Update existing key

    query = f"SELECT zusatz_infos FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())
    assert len(rows) == 1
    zusatz_infos = json.loads(rows[0].zusatz_infos)
    assert zusatz_infos["step1_duration_ms"] == "110"
    assert zusatz_infos["step2_duration_ms"] == "250"

def test_dwmsg_append_timing_infos_non_json_fallback(bq_logger, bq_client):
    """Verifies fallback to string concatenation if zusatz_infos is not JSON."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, "JOB_TIM_FALL", "PROG_TIM_FALL", "LOG_TIM_FALL", "INFO")

    bq_client.query(f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.bert_meldung`
        SET zusatz_infos = 'Initial non-JSON info'
        WHERE eintrags_nr = '{entry_nr}'
    """).result()

    bq_logger.dwmsg_append_timing_infos(entry_nr, "load_time_s", "60")
    bq_logger.dwmsg_append_timing_infos(entry_nr, "validation_status", "PASS")

    query = f"SELECT zusatz_infos FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    rows = list(bq_client.query(query).result())
    assert len(rows) == 1
    expected_string = "Initial non-JSON info | load_time_s: 60 | validation_status: PASS"
    assert rows[0].zusatz_infos == expected_string

def test_dwmsg_append_timing_infos_null_eintrags_nr_fails(bq_logger):
    """Verifies error handling for NULL eintrags_nr."""
    with pytest.raises(Exception, match="p_eintrags_nr cannot be empty or NULL"):
        bq_logger.dwmsg_append_timing_infos(None, "key", "value")
```

### 9. Test `dwmsg_fehlerbehandlung` (Generic Error Handler)

*   **Purpose:** Verify that the `dwmsg_fehlerbehandlung` procedure correctly orchestrates error logging and status updates, mimicking the legacy `trap ERR` behavior.
*   **Setup:** Insert a test entry into `bert_meldung`.
*   **Action:** Call `dwmsg_fehlerbehandlung` with various error details.
*   **Pass/Fail Criterion:**
    *   A new row is inserted into `bert_meldung_fehler` with the provided error details, and `zusatz_info_1` and `zusatz_info_2` are correctly populated with `job_kennung` and `programm_name`.
    *   The corresponding `bert_meldung` entry's `status` is updated to 'ABORTED'.
    *   The procedure signals an SQLSTATE error, which should be caught by the orchestration layer.
    *   If `p_eintrags_nr` is `NULL` or empty, it should log a message (or raise an error if the design intended a hard stop) and return without database operations. (The current code prints a message and returns).

```python
# pytest test_fehlerbehandlung.py
import pytest
import datetime
import re

def test_dwmsg_fehlerbehandlung_success(bq_logger, bq_client):
    """Verifies dwmsg_fehlerbehandlung logs error and sets status to ABORTED."""
    entry_nr = bq_logger.dwmsg_ermittle_nr()
    job_kennung = "FATAL_JOB"
    programm_name = "fatal_script.py"
    bq_logger.dwmsg_erzeuge_eintrag(entry_nr, job_kennung, programm_name, "LOG_FATAL", "INFO")

    error_code = "SHELL_EXIT_1"
    error_message = "Command failed with exit code 1"

    with pytest.raises(Exception, match=f"Process aborted due to error: {error_message} \\(Code: {error_code}\\)"):
        bq_logger.dwmsg_fehlerbehandlung(entry_nr, job_kennung, programm_name, error_code, error_message)

    # Verify bert_meldung_fehler entry
    error_query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler` WHERE eintrags_nr = '{entry_nr}'"
    error_rows = list(bq_client.query(error_query).result())
    assert len(error_rows) == 1
    error_row = error_rows[0]
    assert error_row.fehler_nr == error_code
    assert error_row.fehler_text == error_message
    assert error_row.zusatz_info_1 == job_kennung # As per SP logic
    assert error_row.zusatz_info_2 == programm_name # As per SP logic
    assert error_row.variable_name is None

    # Verify bert_meldung status update
    meldung_query = f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    meldung_rows = list(bq_client.query(meldung_query).result())
    assert len(meldung_rows) == 1
    assert meldung_rows[0].status == "ABORTED"

def test_dwmsg_fehlerbehandlung_null_eintrags_nr_no_db_ops(bq_logger, bq_client, caplog):
    """Verifies dwmsg_fehlerbehandlung handles NULL eintrags_nr without DB operations."""
    initial_meldung_count = bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung`").result().next()[0]
    initial_fehler_count = bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler`").result().next()[0]

    job_kennung = "NO_ENTRY_JOB"
    programm_name = "no_entry_script.py"
    error_code = "NO_ENTRY_ERR"
    error_message = "Error before entry created"

    # The Python orchestration example's dwmsg_fehlerbehandlung wrapper will catch the SIGNAL SQLSTATE
    # from the BigQuery SP. If the SP returns early due to NULL p_eintrags_nr, it won't SIGNAL.
    # So, we need to test the BigQuery SP directly or adjust the Python wrapper for this specific case.
    # For now, let's assume the Python wrapper will call the BQ SP, and the BQ SP will handle it.

    # Direct BigQuery call to simulate the scenario where p_eintrags_nr is NULL
    # The BQ SP itself has a SELECT FORMAT(...) and RETURN if p_eintrags_nr is NULL.
    # This means it won't SIGNAL SQLSTATE.
    # The Python wrapper's try-except block for dwmsg_fehlerbehandlung will not catch an exception
    # if the BQ SP returns early.
    # So, the test should verify no DB changes and a log message.

    # The Python wrapper will raise an exception if the BQ SP signals.
    # If the BQ SP returns early, the Python wrapper will not raise an exception.
    # The current Python wrapper does not explicitly check for the "Error: EintragsNr is missing" message.
    # We'll test the BQ SP's behavior directly via the client.

    # Simulate calling the BQ SP with NULL p_eintrags_nr
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.dwmsg_fehlerbehandlung`(
        NULL,
        '{job_kennung}',
        '{programm_name}',
        '{error_code}',
        '{error_message}'
    );
    """
    bq_client.query(query).result() # Should not raise an exception

    final_meldung_count = bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung`").result().next()[0]
    final_fehler_count = bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler`").result().next()[0]

    assert final_meldung_count == initial_meldung_count
    assert final_fehler_count == initial_fehler_count
    # Verifying the SELECT FORMAT() output would require inspecting BigQuery job results,
    # which is more complex for a simple test. The primary check is no DB modification.
```

### 10. End-to-End Orchestration Test (using `python/orchestration_example.py`)

*   **Purpose:** Verify the overall flow of the migrated system, including the Python orchestration layer interacting with multiple BigQuery Stored Procedures for a successful job run and an error scenario. This covers the full replacement of the KornShell script's lifecycle management.
*   **Setup:** Ensure all BigQuery components are deployed and the Python script is configured.
*   **Action:**
    1.  Run the `if __name__ == "__main__":` block of `python/orchestration_example.py` for a successful path.
    2.  Modify the example to force an error (e.g., by passing `None` to a required parameter in a subsequent call, or simulating an external failure) and run it again to test the error path.
*   **Pass/Fail Criterion:**
    *   **Successful Path:**
        *   A new entry is created in `bert_meldung` with `status = 'OK'`.
        *   `zusatz_infos` contains both `stichtag` and `processing_duration_ms` (or similar timing info) in JSON format.
        *   No entries in `bert_meldung_fehler`.
    *   **Error Path:**
        *   A new entry is created in `bert_meldung` with `status = 'ABORTED'`.
        *   An entry is created in `bert_meldung_fehler` linked to the `bert_meldung` entry, containing the error details.
        *   The Python script logs the error and handles the `SIGNAL SQLSTATE` gracefully (or re-raises as intended).

```python
# pytest test_e2e_orchestration.py
import pytest
import json
import datetime
from unittest.mock import patch
import time

# Assuming BigQueryErrorLogger is imported and PROJECT_ID, DATASET_ID are configured

def test_e2e_orchestration_success_path(bq_logger, bq_client):
    """Tests the full successful orchestration flow."""
    job_id = "E2E_SUCCESS_JOB"
    program_name = "e2e_success_script.py"

    # Simulate the main block of orchestration_example.py
    entry_nr = None
    try:
        entry_nr = bq_logger.dwmsg_ermittle_nr()
        log_file = bq_logger.dwmsg_logdateiname(job_id, program_name)
        bq_logger.dwmsg_erzeuge_eintrag(entry_nr, job_id, program_name, log_file, "INFO")

        current_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        bq_logger.dwmsg_setze_stichtag_info(entry_nr, current_date_str, '%Y-%m-%d', 'Processing for current date.')

        start_time = time.time()
        time.sleep(0.1) # Simulate work
        end_time = time.time()
        duration_ms = int((end_time - start_time) * 1000)
        bq_logger.dwmsg_append_timing_infos(entry_nr, 'processing_duration_ms', str(duration_ms))

        bq_logger.dwmsg_setze_status_ok(entry_nr)

    except Exception as e:
        pytest.fail(f"Orchestration failed unexpectedly: {e}")

    # Assertions for successful path
    meldung_query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    meldung_rows = list(bq_client.query(meldung_query).result())
    assert len(meldung_rows) == 1
    meldung_row = meldung_rows[0]
    assert meldung_row.status == "OK"
    assert meldung_row.job_kennung == job_id
    assert meldung_row.programm_name == program_name
    assert meldung_row.log_datei is not None # Check format in dwmsg_logdateiname test
    assert meldung_row.typ == "INFO"

    zusatz_infos = json.loads(meldung_row.zusatz_infos)
    assert zusatz_infos["stichtag"] == current_date_str
    assert zusatz_infos["stichtag_info_text"] == 'Processing for current date.'
    assert "processing_duration_ms" in zusatz_infos
    assert int(zusatz_infos["processing_duration_ms"]) >= 0

    fehler_query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler` WHERE eintrags_nr = '{entry_nr}'"
    fehler_count = bq_client.query(fehler_query).result().next()[0]
    assert fehler_count == 0

def test_e2e_orchestration_error_path(bq_logger, bq_client):
    """Tests the full error orchestration flow."""
    job_id = "E2E_ERROR_JOB"
    program_name = "e2e_error_script.py"

    entry_nr = None
    try:
        entry_nr = bq_logger.dwmsg_ermittle_nr()
        log_file = bq_logger.dwmsg_logdateiname(job_id, program_name)
        bq_logger.dwmsg_erzeuge_eintrag(entry_nr, job_id, program_name, log_file, "INFO")

        # Simulate an error by calling a procedure with invalid input
        # This should trigger the exception block in the orchestration example
        bq_logger.dwmsg_setze_status_ok(None) # This will raise an exception

        pytest.fail("Expected an exception but none was raised.")

    except Exception as e:
        # The orchestration example's error handling logic
        if entry_nr:
            try:
                bq_logger.dwmsg_fehlerbehandlung(entry_nr, job_id, program_name, "PYTHON_TEST_ERROR", str(e))
            except Exception as fe:
                # dwmsg_fehlerbehandlung itself signals an error, so this is expected
                pass
        else:
            pytest.fail("Error occurred but entry_nr was not available for logging.")

    # Assertions for error path
    meldung_query = f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung` WHERE eintrags_nr = '{entry_nr}'"
    meldung_rows = list(bq_client.query(meldung_query).result())
    assert len(meldung_rows) == 1
    assert meldung_rows[0].status == "ABORTED"

    fehler_query = f"SELECT fehler_nr, fehler_text, zusatz_info_1, zusatz_info_2 FROM `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler` WHERE eintrags_nr = '{entry_nr}'"
    fehler_rows = list(bq_client.query(fehler_query).result())
    assert len(fehler_rows) == 1
    fehler_row = fehler_rows[0]
    assert fehler_row.fehler_nr == "PYTHON_TEST_ERROR"
    assert "p_eintrags_nr cannot be empty or NULL" in fehler_row.fehler_text # From the dwmsg_setze_status_ok error
    assert fehler_row.zusatz_info_1 == job_id
    assert fehler_row.zusatz_info_2 == program_name
```

---

### 11. Schema and Data Quality Assertions

*   **Purpose:** Verify that the BigQuery tables (`bert_meldung`, `bert_meldung_fehler`) adhere to the defined schema and data types, and that basic data quality constraints (e.g., NOT NULL) are enforced.
*   **Setup:** Ensure tables are created.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` and attempt to insert invalid data.
*   **Pass/Fail Criterion:**
    *   Table schemas match the DDL (column names, types, nullability).
    *   `eintrags_nr` in `bert_meldung` is `NOT NULL`.
    *   `eintrags_nr` in `bert_meldung_fehler` is `NOT NULL`.
    *   Partitioning and clustering are correctly applied.

```python
# pytest test_schema_data_quality.py
import pytest
from google.cloud import bigquery

def test_bert_meldung_schema(bq_client):
    """Verifies the schema of the bert_meldung table."""
    table_ref = bq_client.dataset(DATASET_ID, project=PROJECT_ID).table("bert_meldung")
    table = bq_client.get_table(table_ref)

    expected_schema = {
        "eintrags_nr": {"field_type": "STRING", "mode": "REQUIRED"},
        "job_kennung": {"field_type": "STRING", "mode": "NULLABLE"},
        "programm_name": {"field_type": "STRING", "mode": "NULLABLE"},
        "log_datei": {"field_type": "STRING", "mode": "NULLABLE"},
        "typ": {"field_type": "STRING", "mode": "NULLABLE"},
        "status": {"field_type": "STRING", "mode": "NULLABLE"},
        "zusatz_infos": {"field_type": "STRING", "mode": "NULLABLE"},
        "creation_timestamp": {"field_type": "TIMESTAMP", "mode": "NULLABLE"},
        "last_update_timestamp": {"field_type": "TIMESTAMP", "mode": "NULLABLE"},
    }

    actual_schema = {field.name: {"field_type": field.field_type, "mode": field.mode} for field in table.schema}

    for col_name, expected_props in expected_schema.items():
        assert col_name in actual_schema, f"Column {col_name} missing from bert_meldung"
        assert actual_schema[col_name]["field_type"] == expected_props["field_type"], \
            f"Type mismatch for {col_name}: Expected {expected_props['field_type']}, got {actual_schema[col_name]['field_type']}"
        assert actual_schema[col_name]["mode"] == expected_props["mode"], \
            f"Mode mismatch for {col_name}: Expected {expected_props['mode']}, got {actual_schema[col_name]['mode']}"

    assert table.time_partitioning.type_ == "DAY"
    assert table.time_partitioning.field == "creation_timestamp"
    assert table.clustering_fields == ["eintrags_nr"]

def test_bert_meldung_fehler_schema(bq_client):
    """Verifies the schema of the bert_meldung_fehler table."""
    table_ref = bq_client.dataset(DATASET_ID, project=PROJECT_ID).table("bert_meldung_fehler")
    table = bq_client.get_table(table_ref)

    expected_schema = {
        "error_id": {"field_type": "STRING", "mode": "REQUIRED"},
        "eintrags_nr": {"field_type": "STRING", "mode": "REQUIRED"},
        "fehler_nr": {"field_type": "STRING", "mode": "NULLABLE"},
        "fehler_text": {"field_type": "STRING", "mode": "NULLABLE"},
        "zusatz_info_1": {"field_type": "STRING", "mode": "NULLABLE"},
        "zusatz_info_2": {"field_type": "STRING", "mode": "NULLABLE"},
        "variable_name": {"field_type": "STRING", "mode": "NULLABLE"},
        "error_timestamp": {"field_type": "TIMESTAMP", "mode": "NULLABLE"},
    }

    actual_schema = {field.name: {"field_type": field.field_type, "mode": field.mode} for field in table.schema}

    for col_name, expected_props in expected_schema.items():
        assert col_name in actual_schema, f"Column {col_name} missing from bert_meldung_fehler"
        assert actual_schema[col_name]["field_type"] == expected_props["field_type"], \
            f"Type mismatch for {col_name}: Expected {expected_props['field_type']}, got {actual_schema[col_name]['field_type']}"
        assert actual_schema[col_name]["mode"] == expected_props["mode"], \
            f"Mode mismatch for {col_name}: Expected {expected_props['mode']}, got {actual_schema[col_name]['mode']}"

    assert table.time_partitioning.type_ == "DAY"
    assert table.time_partitioning.field == "error_timestamp"
    assert table.clustering_fields == ["eintrags_nr"]

def test_bert_meldung_eintrags_nr_not_null_enforced(bq_client):
    """Verifies NOT NULL constraint for bert_meldung.eintrags_nr."""
    with pytest.raises(Exception, match="Cannot insert NULL into a NOT NULL column"):
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.bert_meldung` (eintrags_nr, job_kennung)
            VALUES (NULL, 'INVALID_JOB')
        """).result()

def test_bert_meldung_fehler_eintrags_nr_not_null_enforced(bq_client):
    """Verifies NOT NULL constraint for bert_meldung_fehler.eintrags_nr."""
    with pytest.raises(Exception, match="Cannot insert NULL into a NOT NULL column"):
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.bert_meldung_fehler` (error_id, eintrags_nr, fehler_nr)
            VALUES ('some_uuid', NULL, '123')
        """).result()
```