As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `f_alis_msgerr.ksh` to BigQuery Stored Procedures. These tests aim to ensure behavioral equivalence, data integrity, and correct transformation logic.

The tests are structured to cover the four key areas: output parity, transformation correctness, external-system replacements, and data quality/schema assertions. Each test case includes a purpose, setup, action, and a concrete pass/fail criterion, with runnable Python/SQL code examples.

**Assumptions for Test Execution:**
*   A BigQuery project and dataset (`your_project_id.your_dataset_name`) are configured.
*   The `message_table` and all BigQuery Stored Procedures (from the `GENERATED MIGRATION CODE` section) have been successfully deployed to this dataset.
*   Python with the `google-cloud-bigquery` library is available for running the test code.
*   `pytest` is used as the testing framework.
*   `CURRENT_TIMESTAMP()` in BigQuery will be close enough for comparison within a few seconds.

---

## Common Test Setup & Helper Functions

Before running the tests, ensure your BigQuery environment is set up.

```python
# conftest.py or a common test_utils.py
import pytest
from google.cloud import bigquery
import time
import uuid
from datetime import datetime, timedelta, timezone

# --- Configuration ---
PROJECT_ID = "your_project_id"  # Replace with your GCP Project ID
DATASET_ID = "your_dataset_name"  # Replace with your BigQuery Dataset ID
TABLE_ID = "message_table"

FULL_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def clean_message_table(bq_client):
    """Cleans the message_table before each test."""
    bq_client.query(f"TRUNCATE TABLE `{FULL_TABLE_PATH}`").result()
    print(f"\nCleaned table: {FULL_TABLE_PATH}")
    yield
    # Optional: Clean up again after tests if needed, but 'autouse' fixture runs before each test.

def call_bq_procedure(bq_client, procedure_name, *args, output_param_name=None):
    """
    Calls a BigQuery stored procedure and returns results.
    If output_param_name is provided, it expects an OUT parameter and returns its value.
    """
    arg_strings = []
    for arg in args:
        if isinstance(arg, str):
            arg_strings.append(f"'{arg}'")
        elif arg is None:
            arg_strings.append("NULL")
        else:
            arg_strings.append(str(arg))

    if output_param_name:
        # For procedures with OUT parameters, we need a DECLARE and CALL statement
        query = f"""
        DECLARE {output_param_name} STRING;
        CALL `{PROJECT_ID}.{DATASET_ID}.{procedure_name}`({', '.join(arg_strings + [output_param_name])});
        SELECT {output_param_name};
        """
        job = bq_client.query(query)
        result = job.result()
        for row in result:
            return row[0] # Assuming a single OUT parameter
        return None
    else:
        query = f"CALL `{PROJECT_ID}.{DATASET_ID}.{procedure_name}`({', '.join(arg_strings)});"
        bq_client.query(query).result()
        return None

def get_table_row_count(bq_client):
    """Returns the number of rows in the message_table."""
    query = f"SELECT COUNT(*) FROM `{FULL_TABLE_PATH}`"
    job = bq_client.query(query)
    for row in job.result():
        return row[0]
    return 0

def get_message_entry(bq_client, entry_nr):
    """Retrieves a single entry from the message_table by entry_nr."""
    query = f"SELECT * FROM `{FULL_TABLE_PATH}` WHERE entry_nr = '{entry_nr}'"
    job = bq_client.query(query)
    rows = list(job.result())
    if rows:
        return rows[0]
    return None

def assert_timestamp_within_tolerance(actual_ts, expected_ts_ref, tolerance_seconds=5):
    """Asserts that a timestamp is within a given tolerance of a reference timestamp."""
    if actual_ts is None:
        raise AssertionError("Actual timestamp is NULL")
    if expected_ts_ref is None:
        raise AssertionError("Expected timestamp reference is NULL")

    # Ensure both are timezone-aware for proper comparison
    if actual_ts.tzinfo is None:
        actual_ts = actual_ts.replace(tzinfo=timezone.utc)
    if expected_ts_ref.tzinfo is None:
        expected_ts_ref = expected_ts_ref.replace(tzinfo=timezone.utc)

    diff = abs((actual_ts - expected_ts_ref).total_seconds())
    assert diff <= tolerance_seconds, \
        f"Timestamp {actual_ts} not within {tolerance_seconds}s of {expected_ts_ref} (diff: {diff}s)"

```

---

## Test Cases

### Test Case 1: Schema and Initial State Assertion

*   **Purpose:** Verify that the `message_table` exists with the correct schema and is initially empty. This covers data-quality/schema assertions.
*   **Setup:** The `bq_message_table_ddl.sql` script must have been executed. The `clean_message_table` fixture ensures the table is empty before the test.
*   **Action:** Query the BigQuery information schema for the table's columns and count rows.
*   **Pass/Fail Criterion:**
    *   The table `your_project_id.your_dataset_name.message_table` exists.
    *   It contains exactly 11 columns with the specified names and types.
    *   The table is empty (row count is 0).

```python
# test_f_alis_msgerr.py
def test_message_table_schema_and_initial_state(bq_client):
    """
    Verifies the schema of the message_table and its initial empty state.
    """
    # 1. Check table existence and schema
    query_schema = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{TABLE_ID}'
    ORDER BY ordinal_position
    """
    job = bq_client.query(query_schema)
    schema_rows = list(job.result())

    expected_schema = [
        ("entry_nr", "STRING"),
        ("job_kennung", "STRING"),
        ("programmname", "STRING"),
        ("logdatei", "STRING"),
        ("status", "STRING"),
        ("fehler_typ", "STRING"),
        ("fehler_nr", "STRING"),
        ("zusatz1", "STRING"),
        ("zusatz2", "STRING"),
        ("zusatzinfos", "STRING"),
        ("created_ts", "TIMESTAMP"),
        ("updated_ts", "TIMESTAMP"),
    ]

    assert len(schema_rows) == len(expected_schema), "Incorrect number of columns"
    for i, (col_name, col_type) in enumerate(expected_schema):
        assert schema_rows[i].column_name == col_name
        assert schema_rows[i].data_type == col_type

    # 2. Check initial row count (ensured by fixture)
    row_count = get_table_row_count(bq_client)
    assert row_count == 0, "Message table is not empty initially"

```

### Test Case 2: `DWMSG_ErmittleNr` - Unique ID Generation

*   **Purpose:** Verify that `DWMSG_ErmittleNr` correctly generates and returns a unique identifier, replicating the legacy script's behavior of providing a unique number. This covers output parity and transformation correctness.
*   **Setup:** None, other than the `clean_message_table` fixture.
*   **Action:** Call the `DWMSG_ErmittleNr` procedure multiple times.
*   **Pass/Fail Criterion:**
    *   Each call returns a non-empty string.
    *   The returned strings are unique across multiple calls.
    *   The format resembles a UUID (as `GENERATE_UUID()` is used).

```python
# test_f_alis_msgerr.py
def test_dwmsg_ermittlenr_generates_unique_id(bq_client):
    """
    Verifies that DWMSG_ErmittleNr generates unique identifiers.
    """
    generated_ids = set()
    num_calls = 5

    for _ in range(num_calls):
        entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
        assert entry_nr is not None and entry_nr != "", "DWMSG_ErmittleNr returned an empty or NULL ID"
        assert entry_nr not in generated_ids, f"DWMSG_ErmittleNr generated a duplicate ID: {entry_nr}"
        generated_ids.add(entry_nr)
        # Basic UUID format check (e.g., length, presence of hyphens)
        assert len(entry_nr) == 36 and entry_nr.count('-') == 4, f"ID {entry_nr} does not look like a UUID"

```

### Test Case 3: `DWMSG_ErzeugeEintrag` - New Entry Creation

*   **Purpose:** Verify that `DWMSG_ErzeugeEintrag` correctly inserts a new row into the `message_table` with the provided details and sets default values for status and timestamps. This covers output parity, transformation correctness, and external-system replacement.
*   **Setup:** Generate a unique `entry_nr` using `DWMSG_ErmittleNr`.
*   **Action:** Call `DWMSG_ErzeugeEintrag` with valid parameters. Then, query the `message_table` to retrieve the newly created entry.
*   **Pass/Fail Criterion:**
    *   The row count in `message_table` increases by one.
    *   The retrieved entry matches the input `entry_nr`, `job_kennung`, `programmname`, `logdatei`.
    *   `status` is 'NEW'.
    *   `created_ts` and `updated_ts` are set to a recent timestamp (within a small tolerance).
    *   Calling with NULL/empty `entry_nr` raises an error.

```python
# test_f_alis_msgerr.py
def test_dwmsg_erzeugeeintrag_creates_new_entry(bq_client):
    """
    Verifies that DWMSG_ErzeugeEintrag creates a new entry in the message_table.
    """
    initial_row_count = get_table_row_count(bq_client)
    
    entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
    job_kennung = "TEST_JOB_001"
    programmname = "test_program.ksh"
    logdatei = "/tmp/test_job_001.log"
    
    start_time = datetime.now(timezone.utc)
    call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", entry_nr, job_kennung, programmname, logdatei)
    end_time = datetime.now(timezone.utc)

    # 1. Row count assertion
    assert get_table_row_count(bq_client) == initial_row_count + 1, "Row count did not increase by 1"

    # 2. Data assertion
    entry = get_message_entry(bq_client, entry_nr)
    assert entry is not None, f"Entry with entry_nr {entry_nr} not found"
    assert entry.job_kennung == job_kennung
    assert entry.programmname == programmname
    assert entry.logdatei == logdatei
    assert entry.status == "NEW"
    assert entry.fehler_typ is None
    assert entry.fehler_nr is None
    assert entry.zusatz1 is None
    assert entry.zusatz2 is None
    assert entry.zusatzinfos is None
    assert_timestamp_within_tolerance(entry.created_ts, start_time)
    assert_timestamp_within_tolerance(entry.updated_ts, start_time)

    # 3. Error handling for NULL entry_nr
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"):
        call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", None, job_kennung, programmname, logdatei)
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"):
        call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", "", job_kennung, programmname, logdatei)

```

### Test Case 4: `DWMSG_SetzeStatusOK` - Status Update

*   **Purpose:** Verify that `DWMSG_SetzeStatusOK` correctly updates the `status` of an existing entry to 'OK' and updates `updated_ts`. This covers output parity, transformation correctness, and external-system replacement.
*   **Setup:** Create an initial entry in the `message_table` with a status other than 'OK'.
*   **Action:** Call `DWMSG_SetzeStatusOK` with the `entry_nr` of the created entry. Then, query the `message_table`.
*   **Pass/Fail Criterion:**
    *   The `status` of the specified entry changes to 'OK'.
    *   The `updated_ts` of the entry is updated to a recent timestamp.
    *   `created_ts` remains unchanged.
    *   Calling with a non-existent `entry_nr` affects 0 rows.
    *   Calling with NULL/empty `entry_nr` raises an error.

```python
# test_f_alis_msgerr.py
def test_dwmsg_setzestatusok_updates_status(bq_client):
    """
    Verifies that DWMSG_SetzeStatusOK updates the status to 'OK'.
    """
    entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
    call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", entry_nr, "JOB_OK", "prog_ok.ksh", "/log/ok.log")
    
    # Ensure initial status is not OK
    initial_entry = get_message_entry(bq_client, entry_nr)
    assert initial_entry.status == "NEW"
    initial_created_ts = initial_entry.created_ts
    initial_updated_ts = initial_entry.updated_ts

    # Introduce a small delay to ensure updated_ts changes
    time.sleep(1) 
    start_time = datetime.now(timezone.utc)
    call_bq_procedure(bq_client, "DWMSG_SetzeStatusOK", entry_nr)
    end_time = datetime.now(timezone.utc)

    updated_entry = get_message_entry(bq_client, entry_nr)
    assert updated_entry.status == "OK"
    assert updated_entry.created_ts == initial_created_ts # created_ts should not change
    assert_timestamp_within_tolerance(updated_entry.updated_ts, start_time)
    assert updated_entry.updated_ts > initial_updated_ts # updated_ts must be newer

    # Test with non-existent entry_nr (should not raise error, but affect 0 rows)
    non_existent_entry_nr = str(uuid.uuid4())
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.DWMSG_SetzeStatusOK`('{non_existent_entry_nr}');").result()
    assert get_message_entry(bq_client, non_existent_entry_nr) is None # No new entry created

    # Test error handling for NULL/empty entry_nr
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStatusOK", None)
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStatusOK", "")

```

### Test Case 5: `DWMSG_SetzeStatusAbbruch` - Status Update

*   **Purpose:** Verify that `DWMSG_SetzeStatusAbbruch` correctly updates the `status` of an existing entry to 'ABBRUCH' and updates `updated_ts`. This covers output parity, transformation correctness, and external-system replacement.
*   **Setup:** Create an initial entry in the `message_table` with a status other than 'ABBRUCH'.
*   **Action:** Call `DWMSG_SetzeStatusAbbruch` with the `entry_nr` of the created entry. Then, query the `message_table`.
*   **Pass/Fail Criterion:**
    *   The `status` of the specified entry changes to 'ABBRUCH'.
    *   The `updated_ts` of the entry is updated to a recent timestamp.
    *   `created_ts` remains unchanged.
    *   Calling with a non-existent `entry_nr` affects 0 rows.
    *   Calling with NULL/empty `entry_nr` raises an error.

```python
# test_f_alis_msgerr.py
def test_dwmsg_setzestatusabbruch_updates_status(bq_client):
    """
    Verifies that DWMSG_SetzeStatusAbbruch updates the status to 'ABBRUCH'.
    """
    entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
    call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", entry_nr, "JOB_ABORT", "prog_abort.ksh", "/log/abort.log")
    
    initial_entry = get_message_entry(bq_client, entry_nr)
    assert initial_entry.status == "NEW"
    initial_created_ts = initial_entry.created_ts
    initial_updated_ts = initial_entry.updated_ts

    time.sleep(1)
    start_time = datetime.now(timezone.utc)
    call_bq_procedure(bq_client, "DWMSG_SetzeStatusAbbruch", entry_nr)
    end_time = datetime.now(timezone.utc)

    updated_entry = get_message_entry(bq_client, entry_nr)
    assert updated_entry.status == "ABBRUCH"
    assert updated_entry.created_ts == initial_created_ts
    assert_timestamp_within_tolerance(updated_entry.updated_ts, start_time)
    assert updated_entry.updated_ts > initial_updated_ts

    # Test with non-existent entry_nr
    non_existent_entry_nr = str(uuid.uuid4())
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.DWMSG_SetzeStatusAbbruch`('{non_existent_entry_nr}');").result()
    assert get_message_entry(bq_client, non_existent_entry_nr) is None

    # Test error handling for NULL/empty entry_nr
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStatusAbbruch", None)
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStatusAbbruch", "")

```

### Test Case 6: `DWMSG_MeldeFehler` - Error Details Update

*   **Purpose:** Verify that `DWMSG_MeldeFehler` correctly updates the error-related columns (`fehler_typ`, `fehler_nr`, `zusatz1`, `zusatz2`) and `updated_ts` for an existing entry, handling optional parameters. This covers output parity, transformation correctness, and external-system replacement.
*   **Setup:** Create an initial entry in the `message_table`.
*   **Action:** Call `DWMSG_MeldeFehler` with various combinations of `zusatz1` and `zusatz2` (present, absent, NULL). Query the `message_table` after each call.
*   **Pass/Fail Criterion:**
    *   The specified error columns are updated correctly.
    *   `updated_ts` is updated.
    *   `created_ts` and other fields remain unchanged.
    *   Calling with NULL/empty `entry_nr` raises an error.

```python
# test_f_alis_msgerr.py
def test_dwmsg_meldefehler_updates_error_details(bq_client):
    """
    Verifies that DWMSG_MeldeFehler updates error-related columns.
    """
    entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
    call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", entry_nr, "JOB_ERR", "prog_err.ksh", "/log/err.log")
    
    initial_entry = get_message_entry(bq_client, entry_nr)
    initial_created_ts = initial_entry.created_ts
    initial_updated_ts = initial_entry.updated_ts

    # Test Case 1: All parameters provided
    time.sleep(1)
    start_time_1 = datetime.now(timezone.utc)
    call_bq_procedure(bq_client, "DWMSG_MeldeFehler", entry_nr, "F", "1001", "File not found", "path/to/file.txt")
    updated_entry_1 = get_message_entry(bq_client, entry_nr)
    assert updated_entry_1.fehler_typ == "F"
    assert updated_entry_1.fehler_nr == "1001"
    assert updated_entry_1.zusatz1 == "File not found"
    assert updated_entry_1.zusatz2 == "path/to/file.txt"
    assert_timestamp_within_tolerance(updated_entry_1.updated_ts, start_time_1)
    assert updated_entry_1.created_ts == initial_created_ts

    # Test Case 2: Only zusatz1 provided (zusatz2 is NULL)
    time.sleep(1)
    start_time_2 = datetime.now(timezone.utc)
    call_bq_procedure(bq_client, "DWMSG_MeldeFehler", entry_nr, "E", "2002", "DB connection failed", None)
    updated_entry_2 = get_message_entry(bq_client, entry_nr)
    assert updated_entry_2.fehler_typ == "E"
    assert updated_entry_2.fehler_nr == "2002"
    assert updated_entry_2.zusatz1 == "DB connection failed"
    assert updated_entry_2.zusatz2 is None
    assert_timestamp_within_tolerance(updated_entry_2.updated_ts, start_time_2)
    assert updated_entry_2.created_ts == initial_created_ts

    # Test Case 3: No zusatz1 or zusatz2
    time.sleep(1)
    start_time_3 = datetime.now(timezone.utc)
    call_bq_procedure(bq_client, "DWMSG_MeldeFehler", entry_nr, "W", "3003", None, None)
    updated_entry_3 = get_message_entry(bq_client, entry_nr)
    assert updated_entry_3.fehler_typ == "W"
    assert updated_entry_3.fehler_nr == "3003"
    assert updated_entry_3.zusatz1 is None
    assert updated_entry_3.zusatz2 is None
    assert_timestamp_within_tolerance(updated_entry_3.updated_ts, start_time_3)
    assert updated_entry_3.created_ts == initial_created_ts

    # Test error handling for NULL/empty entry_nr
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"):
        call_bq_procedure(bq_client, "DWMSG_MeldeFehler", None, "F", "1", "Msg", "Detail")
    with pytest.raises(Exception, match="Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"):
        call_bq_procedure(bq_client, "DWMSG_MeldeFehler", "", "F", "1", "Msg", "Detail")

```

### Test Case 7: `DWMSG_Fehlerbehandlung` - Combined Error Handling

*   **Purpose:** Verify that `DWMSG_Fehlerbehandlung` correctly orchestrates calls to `DWMSG_MeldeFehler` and `DWMSG_SetzeStatusAbbruch` as per its design. This covers transformation correctness and external-system replacement.
*   **Setup:** Create an initial entry in the `message_table`.
*   **Action:** Call `DWMSG_Fehlerbehandlung` with the `entry_nr`. Query the `message_table`.
*   **Pass/Fail Criterion:**
    *   The `status` of the entry is 'ABBRUCH'.
    *   `fehler_typ` is 'F'.
    *   `fehler_nr` is '10' (as per the migrated code's `kUnerwFehler`).
    *   `zusatz1` contains "ErrorCode ist: 1" (as per the migrated code's placeholder `fehler_nr`).
    *   `updated_ts` is updated.

```python
# test_f_alis_msgerr.py
def test_dwmsg_fehlerbehandlung_orchestrates_error_flow(bq_client):
    """
    Verifies that DWMSG_Fehlerbehandlung calls MeldeFehler and SetzeStatusAbbruch.
    """
    entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
    call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", entry_nr, "JOB_TRAP", "prog_trap.ksh", "/log/trap.log")
    
    initial_entry = get_message_entry(bq_client, entry_nr)
    assert initial_entry.status == "NEW"
    assert initial_entry.fehler_typ is None

    time.sleep(1)
    start_time = datetime.now(timezone.utc)
    call_bq_procedure(bq_client, "DWMSG_Fehlerbehandlung", entry_nr)
    end_time = datetime.now(timezone.utc)

    updated_entry = get_message_entry(bq_client, entry_nr)
    assert updated_entry.status == "ABBRUCH"
    assert updated_entry.fehler_typ == "F"
    assert updated_entry.fehler_nr == "10" # kUnerwFehler
    assert updated_entry.zusatz1 == "ErrorCode ist: 1" # Placeholder fehler_nr
    assert updated_entry.zusatz2 is None
    assert_timestamp_within_tolerance(updated_entry.updated_ts, start_time)

```

### Test Case 8: `DWMSG_Logdateiname` - Log File Name Generation

*   **Purpose:** Verify that `DWMSG_Logdateiname` constructs the log file name string correctly, including the base path, job identifier, formatted timestamp, and entry number. This covers output parity and transformation correctness.
*   **Setup:** None.
*   **Action:** Call `DWMSG_Logdateiname` with sample `job_kennung` and `entry_nr`.
*   **Pass/Fail Criterion:**
    *   The returned string matches the expected format: `/protocol/<JOB_KENNUNG>_YYYYMMDD_HHMM_<ENTRY_NR>.log`.
    *   The timestamp part is correctly formatted and reflects the approximate current time.

```python
# test_f_alis_msgerr.py
def test_dwmsg_logdateiname_generates_correct_format(bq_client):
    """
    Verifies that DWMSG_Logdateiname generates log file names in the correct format.
    """
    job_kennung = "MY_BATCH_JOB"
    entry_nr = "12345-ABC"
    
    start_time = datetime.now(timezone.utc)
    log_filename = call_bq_procedure(bq_client, "DWMSG_Logdateiname", job_kennung, entry_nr, output_param_name="log_name_out")
    end_time = datetime.now(timezone.utc)

    assert log_filename is not None and log_filename != "", "Log filename was not generated"

    # Expected format: /protocol/JOB_KENNUNG_YYYYMMDD_HHMM_ENTRY_NR.log
    # Extract timestamp part from the generated filename
    parts = log_filename.split('_')
    assert len(parts) >= 3, "Log filename format is incorrect"
    
    # The timestamp part is the second-to-last part before the .log extension
    # Example: /protocol/MY_BATCH_JOB_20231027_1030_12345-ABC.log
    # parts[0] = /protocol/MY_BATCH_JOB
    # parts[1] = 20231027
    # parts[2] = 1030
    # parts[3] = 12345-ABC.log
    
    # Re-evaluate the split based on the CONCAT logic:
    # CONCAT('/protocol/', job_kennung, '_', FORMAT_TIMESTAMP('%Y%m%d_%H%M', CURRENT_TIMESTAMP()), '_', entry_nr, '.log')
    # This means the timestamp part is directly after job_kennung and before entry_nr.
    
    expected_prefix = f"/protocol/{job_kennung}_"
    expected_suffix = f"_{entry_nr}.log"
    
    assert log_filename.startswith(expected_prefix)
    assert log_filename.endswith(expected_suffix)

    # Extract the timestamp string from the middle
    timestamp_str_start = len(expected_prefix)
    timestamp_str_end = log_filename.rfind(expected_suffix)
    timestamp_part = log_filename[timestamp_str_start:timestamp_str_end]

    assert len(timestamp_part) == 13, f"Timestamp part length is incorrect: {timestamp_part}" # YYYYMMDD_HHMM (8+1+4=13)
    
    # Try to parse the timestamp part
    try:
        generated_ts = datetime.strptime(timestamp_part, '%Y%m%d_%H%M').replace(tzinfo=timezone.utc)
    except ValueError:
        pytest.fail(f"Generated timestamp part '{timestamp_part}' is not in expected format YYYYMMDD_HHMM")

    # Check if the generated timestamp is within a reasonable range of the test execution time
    assert_timestamp_within_tolerance(generated_ts, start_time, tolerance_seconds=60) # Allow more tolerance for date formatting

```

### Test Case 9: `DWMSG_SetzeStichtagInfo` - Date Info Update

*   **Purpose:** Verify that `DWMSG_SetzeStichtagInfo` correctly parses and stores a date string in `zusatzinfos`, replicating the `TO_DATE` functionality. This covers transformation correctness (date parsing) and external-system replacement.
*   **Setup:** Create an initial entry.
*   **Action:** Call `DWMSG_SetzeStichtagInfo` with various date strings and format masks. Query the `message_table`.
*   **Pass/Fail Criterion:**
    *   `zusatzinfos` is updated with the correctly parsed and casted timestamp string.
    *   `updated_ts` is updated.
    *   Calling with NULL/empty `entry_nr`, `stichtag`, or `stichtag_fmt` raises an error.
    *   Calling with an invalid `stichtag` for the given `stichtag_fmt` raises an error.

```python
# test_f_alis_msgerr.py
def test_dwmsg_setzestichtaginfo_updates_date_info(bq_client):
    """
    Verifies that DWMSG_SetzeStichtagInfo correctly parses and stores date information.
    """
    entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
    call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", entry_nr, "JOB_DATE", "prog_date.ksh", "/log/date.log")
    
    initial_entry = get_message_entry(bq_client, entry_nr)
    initial_updated_ts = initial_entry.updated_ts

    # Test Case 1: Standard date format
    time.sleep(1)
    start_time_1 = datetime.now(timezone.utc)
    stichtag_1 = "2023-10-27 14:30:00"
    stichtag_fmt_1 = "%Y-%m-%d %H:%M:%S"
    call_bq_procedure(bq_client, "DWMSG_SetzeStichtagInfo", entry_nr, stichtag_1, stichtag_fmt_1)
    updated_entry_1 = get_message_entry(bq_client, entry_nr)
    # BigQuery's CAST(PARSE_TIMESTAMP(...) AS STRING) will typically output in ISO format
    expected_zusatzinfos_1 = "2023-10-27 14:30:00 UTC" # BigQuery adds UTC by default
    assert updated_entry_1.zusatzinfos == expected_zusatzinfos_1
    assert_timestamp_within_tolerance(updated_entry_1.updated_ts, start_time_1)

    # Test Case 2: Different date format
    time.sleep(1)
    start_time_2 = datetime.now(timezone.utc)
    stichtag_2 = "27.10.2023 14:30"
    stichtag_fmt_2 = "%d.%m.%Y %H:%M"
    call_bq_procedure(bq_client, "DWMSG_SetzeStichtagInfo", entry_nr, stichtag_2, stichtag_fmt_2)
    updated_entry_2 = get_message_entry(bq_client, entry_nr)
    expected_zusatzinfos_2 = "2023-10-27 14:30:00 UTC"
    assert updated_entry_2.zusatzinfos == expected_zusatzinfos_2
    assert_timestamp_within_tolerance(updated_entry_2.updated_ts, start_time_2)

    # Test error handling for NULL/empty parameters
    with pytest.raises(Exception, match="Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStichtagInfo", None, "2023-01-01", "%Y-%m-%d")
    with pytest.raises(Exception, match="Argh!, keinen Stichtag angegeben!"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStichtagInfo", entry_nr, None, "%Y-%m-%d")
    with pytest.raises(Exception, match="Argh!, Stichtagsangaben ohne Formatangaben knnen nicht verarbeitet werden!"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStichtagInfo", entry_nr, "2023-01-01", None)

    # Test invalid date format (should raise BigQuery error)
    with pytest.raises(Exception, match="Failed to parse input string"):
        call_bq_procedure(bq_client, "DWMSG_SetzeStichtagInfo", entry_nr, "2023-99-99", "%Y-%m-%d")

```

### Test Case 10: `DWMSG_AppendTimingInfos` - Append Information

*   **Purpose:** Verify that `DWMSG_AppendTimingInfos` correctly appends new information, including a formatted timestamp, to the `zusatzinfos` column. This covers transformation correctness (string concatenation, timestamp formatting) and external-system replacement.
*   **Setup:** Create an initial entry, potentially with some existing `zusatzinfos`.
*   **Action:** Call `DWMSG_AppendTimingInfos` multiple times with different `info_text` and `date_format`. Query the `message_table`.
*   **Pass/Fail Criterion:**
    *   `zusatzinfos` is correctly appended with the new text and formatted timestamp.
    *   `updated_ts` is updated after each append.
    *   Calling with NULL/empty `entry_nr` or `date_format` raises an error.

```python
# test_f_alis_msgerr.py
def test_dwmsg_appendtiminginfos_appends_info(bq_client):
    """
    Verifies that DWMSG_AppendTimingInfos appends information to zusatzinfos.
    """
    entry_nr = call_bq_procedure(bq_client, "DWMSG_ErmittleNr", output_param_name="entry_nr_out")
    call_bq_procedure(bq_client, "DWMSG_ErzeugeEintrag", entry_nr, "JOB_APPEND", "prog_append.ksh", "/log/append.log")
    
    # Set initial zusatzinfos
    call_bq_procedure(bq_client, "DWMSG_SetzeStichtagInfo", entry_nr, "2023-01-01", "%Y-%m-%d")
    initial_entry = get_message_entry(bq_client, entry_nr)
    assert initial_entry.zusatzinfos == "2023-01-01 00:00:00 UTC"
    initial_updated_ts = initial_entry.updated_ts

    # Test Case 1: Append first timing info
    time.sleep(1)
    start_time_1 = datetime.now(timezone.utc)
    info_text_1 = "Start Time:"
    date_format_1 = "%Y-%m-%d %H:%M:%S"
    call_bq_procedure(bq_client, "DWMSG_AppendTimingInfos", entry_nr, info_text_1, date_format_1)
    updated_entry_1 = get_message_entry(bq_client, entry_nr)
    
    expected_ts_str_1 = start_time_1.strftime("%Y-%m-%d %H:%M:%S")
    expected_zusatzinfos_1 = f"2023-01-01 00:00:00 UTC{info_text_1} {expected_ts_str_1} "
    assert updated_entry_1.zusatzinfos == expected_zusatzinfos_1
    assert_timestamp_within_tolerance(updated_entry_1.updated_ts, start_time_1)
    assert updated_entry_1.updated_ts > initial_updated_ts

    # Test Case 2: Append second timing info with different format
    time.sleep(1)
    start_time_2 = datetime.now(timezone.utc)
    info_text_2 = "End Time:"
    date_format_2 = "%H:%M:%S"
    call_bq_procedure(bq_client, "DWMSG_AppendTimingInfos", entry_nr, info_text_2, date_format_2)
    updated_entry_2 = get_message_entry(bq_client, entry_nr)

    expected_ts_str_2 = start_time_2.strftime("%H:%M:%S")
    expected_zusatzinfos_2 = f"{expected_zusatzinfos_1}{info_text_2} {expected_ts_str_2} "
    assert updated_entry_2.zusatzinfos == expected_zusatzinfos_2
    assert_timestamp_within_tolerance(updated_entry_2.updated_ts, start_time_2)
    assert updated_entry_2.updated_ts > updated_entry_1.updated_ts

    # Test error handling for NULL/empty parameters
    with pytest.raises(Exception, match="Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"):
        call_bq_procedure(bq_client, "DWMSG_AppendTimingInfos", None, "Info", "%Y-%m-%d")
    with pytest.raises(Exception, match="Argh!, Formatangabe erforderlich!"):
        call_bq_procedure(bq_client, "DWMSG_AppendTimingInfos", entry_nr, "Info", None)
    with pytest.raises(Exception, match="Argh!, Formatangabe erforderlich!"):
        call_bq_procedure(bq_client, "DWMSG_AppendTimingInfos", entry_nr, "Info", "")

```

---

These tests provide a robust framework for validating the migration of `f_alis_msgerr.ksh`. They cover the core functionality of each translated function, ensuring that the BigQuery Stored Procedures behave as expected and maintain data integrity in the `message_table`. The use of `pytest` and BigQuery client interactions allows for automated, repeatable validation.