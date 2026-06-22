As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_bp_ta_rn_da_vda_tk.ksh` to a BigQuery Stored Procedure. The primary challenge identified is the "unresolved risk" regarding the detailed business logic within the `k_ausd_bp_ta_rn_da_vda_tk.ksh` kernel script. The provided BigQuery stored procedure (`sp_bereitstellung_basisprodukte_bert.sql`) implements an *inferred* version of this kernel logic.

Therefore, the following tests are designed to validate:
1.  The orchestrator's behavior (parameter handling, logging, error handling, restart logic) as implemented in `sp_bereitstellung_basisprodukte_bert.sql`.
2.  The *inferred* data transformation logic (date filtering, `DWH_VERTRAG_ID` filtering) as embedded in the BigQuery stored procedure.

**Important Disclaimer:** These tests validate the *currently implemented* BigQuery stored procedure based on the *inferred* kernel script logic. Once the full, detailed logic of `k_ausd_bp_ta_rn_da_vda_tk.ksh` is analyzed and implemented, additional, more granular tests specifically targeting complex transformations, aggregations, and column-level logic will be required.

---

## Migration Validation Tests for `sp_bereitstellung_basisprodukte_bert`

**Assumptions for Test Environment:**
*   A BigQuery project and dataset (`project.dataset`) are configured.
*   The DDLs for `job_audit`, `job_log`, `source_contract_cache`, and `target_fos_table` have been executed.
*   The `sp_bereitstellung_basisprodukte_bert` stored procedure has been deployed.
*   For `source_contract_cache` and `target_fos_table`, a minimal schema is assumed for testing purposes, including:
    *   `source_contract_cache`: `DWH_VERTRAG_ID INT64`, `GUELTIG_VON DATE`, `GUELTIG_BIS DATE`, `LADEDATUM DATE`, `SOME_OTHER_COL STRING`
    *   `target_fos_table`: `DWH_VERTRAG_ID INT64`, `SOME_OTHER_COL STRING`, `LAST_UPDATE_DATE DATE`, `LADEDATUM DATE`
*   The `pytest` framework with `google-cloud-bigquery` client is used for test execution and assertions. Helper functions are provided for common operations.

```python
# test_bert_migration.py (Illustrative Pytest structure)
import pytest
from google.cloud import bigquery
import os
from datetime import date, timedelta

# --- Configuration and Helper Functions (to be placed in a common test_utils.py or similar) ---
PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "your_dataset")
CLIENT = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def setup_teardown_tables():
    """Fixture to clear tables before each test."""
    CLIENT.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.source_contract_cache`").result()
    CLIENT.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.target_fos_table`").result()
    CLIENT.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_audit`").result()
    CLIENT.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
    yield # Test runs here
    # Optional: Add cleanup after test if needed, though autouse=True and truncate before is usually sufficient.

def call_stored_procedure(stichtag: str = None, wiederanlaufWert: int = None):
    """Calls the BigQuery stored procedure."""
    stichtag_param = f"'{stichtag}'" if stichtag is not None else "NULL"
    wiederanlauf_param = str(wiederanlaufWert) if wiederanlaufWert is not None else "NULL"
    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.sp_bereitstellung_basisprodukte_bert`({stichtag_param}, {wiederanlauf_param});
    """
    try:
        CLIENT.query(query).result()
        return True, None
    except Exception as e:
        return False, str(e)

def get_table_count(table_name: str) -> int:
    """Returns the row count of a specified table."""
    query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
    rows = CLIENT.query(query).result()
    return [row[0] for row in rows][0]

def get_table_data(table_name: str, columns: list = None) -> list[dict]:
    """Fetches all data from a table."""
    cols_str = ", ".join(columns) if columns else "*"
    query = f"SELECT {cols_str} FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` ORDER BY DWH_VERTRAG_ID"
    rows = CLIENT.query(query).result()
    return [dict(row) for row in rows]

def insert_source_data(data: list[dict]):
    """Inserts data into source_contract_cache."""
    rows_to_insert = [
        (row['DWH_VERTRAG_ID'], row['GUELTIG_VON'], row['GUELTIG_BIS'], row['LADEDATUM'], row['SOME_OTHER_COL'])
        for row in data
    ]
    errors = CLIENT.insert_rows(
        table=f"{PROJECT_ID}.{DATASET_ID}.source_contract_cache",
        rows=rows_to_insert,
        selected_fields=[
            bigquery.SchemaField("DWH_VERTRAG_ID", "INT64"),
            bigquery.SchemaField("GUELTIG_VON", "DATE"),
            bigquery.SchemaField("GUELTIG_BIS", "DATE"),
            bigquery.SchemaField("LADEDATUM", "DATE"),
            bigquery.SchemaField("SOME_OTHER_COL", "STRING"),
        ]
    )
    assert not errors, f"Errors inserting source data: {errors}"

def insert_target_data(data: list[dict]):
    """Inserts data into target_fos_table."""
    rows_to_insert = [
        (row['DWH_VERTRAG_ID'], row['SOME_OTHER_COL'], row['LAST_UPDATE_DATE'], row['LADEDATUM'])
        for row in data
    ]
    errors = CLIENT.insert_rows(
        table=f"{PROJECT_ID}.{DATASET_ID}.target_fos_table",
        rows=rows_to_insert,
        selected_fields=[
            bigquery.SchemaField("DWH_VERTRAG_ID", "INT64"),
            bigquery.SchemaField("SOME_OTHER_COL", "STRING"),
            bigquery.SchemaField("LAST_UPDATE_DATE", "DATE"),
            bigquery.SchemaField("LADEDATUM", "DATE"),
        ]
    )
    assert not errors, f"Errors inserting target data: {errors}"

def assert_audit_entry(status: str, message_substring: str = None, expected_count: int = 1):
    """Asserts the presence and content of an audit entry."""
    query = f"""
        SELECT status, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit`
        WHERE status = '{status}'
        ORDER BY created_at DESC
    """
    rows = list(CLIENT.query(query).result())
    assert len(rows) >= expected_count, f"Expected at least {expected_count} audit entries with status '{status}', got {len(rows)}"
    if message_substring:
        found = False
        for row in rows:
            if message_substring in row['message']:
                found = True
                break
        assert found, f"No audit entry found with status '{status}' containing message '{message_substring}'"

def assert_log_entry(log_level: str, message_substring: str = None, expected_count: int = 1):
    """Asserts the presence and content of a log entry."""
    query = f"""
        SELECT log_level, message FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
        WHERE log_level = '{log_level}'
        ORDER BY created_at DESC
    """
    rows = list(CLIENT.query(query).result())
    assert len(rows) >= expected_count, f"Expected at least {expected_count} log entries with level '{log_level}', got {len(rows)}"
    if message_substring:
        found = False
        for row in rows:
            if message_substring in row['message']:
                found = True
                break
        assert found, f"No log entry found with level '{log_level}' containing message '{message_substring}'"

# --- End of Helper Functions ---
```

---

### Test Case 1: Default Stichtag and No Restart Value

*   **Purpose:** Verify that when no `Stichtag` or `Wiederanlaufwert` is provided, the procedure defaults `Stichtag` to `CURRENT_DATE()` and performs a full load (no restart logic applied).
*   **Setup:**
    *   `source_contract_cache` contains data with various `GUELTIG_VON`, `GUELTIG_BIS`, `LADEDATUM` values, some matching `CURRENT_DATE()` filters, some not.
    *   `target_fos_table` is empty.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert(NULL, NULL)`.
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully without error.
    2.  `job_audit` contains one `STARTED` entry and one `OK` entry.
    3.  `target_fos_table` contains all records from `source_contract_cache` that satisfy the date filters (`GUELTIG_VON <= CURRENT_DATE() < GUELTIG_BIS` AND `LADEDATUM < CURRENT_DATE()`).
    4.  The count of records in `target_fos_table` matches the expected count based on the `CURRENT_DATE()` filters.

```python
def test_default_stichtag_no_restart():
    today = date.today()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    source_data = [
        # Valid record for today
        {'DWH_VERTRAG_ID': 1, 'GUELTIG_VON': yesterday, 'GUELTIG_BIS': tomorrow, 'LADEDATUM': yesterday, 'SOME_OTHER_COL': 'A'},
        # Invalid: LADEDATUM not < Stichtag
        {'DWH_VERTRAG_ID': 2, 'GUELTIG_VON': yesterday, 'GUELTIG_BIS': tomorrow, 'LADEDATUM': today, 'SOME_OTHER_COL': 'B'},
        # Invalid: Stichtag not < GUELTIG_BIS
        {'DWH_VERTRAG_ID': 3, 'GUELTIG_VON': yesterday, 'GUELTIG_BIS': today, 'LADEDATUM': yesterday, 'SOME_OTHER_COL': 'C'},
        # Invalid: Stichtag not >= GUELTIG_VON
        {'DWH_VERTRAG_ID': 4, 'GUELTIG_VON': tomorrow, 'GUELTIG_BIS': tomorrow + timedelta(days=1), 'LADEDATUM': yesterday, 'SOME_OTHER_COL': 'D'},
        # Another valid record
        {'DWH_VERTRAG_ID': 5, 'GUELTIG_VON': today - timedelta(days=10), 'GUELTIG_BIS': today + timedelta(days=10), 'LADEDATUM': today - timedelta(days=5), 'SOME_OTHER_COL': 'E'},
    ]
    insert_source_data(source_data)

    success, error_msg = call_stored_procedure(stichtag=None, wiederanlaufWert=None)
    assert success, f"Procedure failed: {error_msg}"

    assert_audit_entry('STARTED')
    assert_audit_entry('OK')
    
    # Expected records: DWH_VERTRAG_ID 1 and 5
    expected_target_ids = {1, 5}
    actual_target_data = get_table_data('target_fos_table', ['DWH_VERTRAG_ID'])
    actual_target_ids = {row['DWH_VERTRAG_ID'] for row in actual_target_data}

    assert actual_target_ids == expected_target_ids
    assert get_table_count('target_fos_table') == len(expected_target_ids)
```

---

### Test Case 2: Specific Stichtag and No Restart Value

*   **Purpose:** Verify that a provided `Stichtag` is correctly used for filtering and a full load is performed when no restart value.
*   **Setup:**
    *   `source_contract_cache` contains data with various `GUELTIG_VON`, `GUELTIG_BIS`, `LADEDATUM` values, some matching a specific `Stichtag` (e.g., '01012023'), some not.
    *   `target_fos_table` is empty.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully without error.
    2.  `job_audit` contains one `STARTED` entry and one `OK` entry, with `stichtag` column showing '01012023'.
    3.  `target_fos_table` contains all records from `source_contract_cache` that satisfy the date filters (`GUELTIG_VON <= '2023-01-01' < GUELTIG_BIS` AND `LADEDATUM < '2023-01-01'`).
    4.  The count of records in `target_fos_table` matches the expected count based on the '01012023' filters.

```python
def test_specific_stichtag_no_restart():
    stichtag_str = '01012023'
    stichtag_date = date(2023, 1, 1)

    source_data = [
        # Valid record for 2023-01-01
        {'DWH_VERTRAG_ID': 10, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'X'},
        # Invalid: LADEDATUM not < Stichtag
        {'DWH_VERTRAG_ID': 11, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2023, 1, 1), 'SOME_OTHER_COL': 'Y'},
        # Invalid: Stichtag not < GUELTIG_BIS
        {'DWH_VERTRAG_ID': 12, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 1), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'Z'},
        # Valid record for 2023-01-01
        {'DWH_VERTRAG_ID': 13, 'GUELTIG_VON': date(2022, 1, 1), 'GUELTIG_BIS': date(2024, 1, 1), 'LADEDATUM': date(2022, 12, 15), 'SOME_OTHER_COL': 'W'},
    ]
    insert_source_data(source_data)

    success, error_msg = call_stored_procedure(stichtag=stichtag_str, wiederanlaufWert=None)
    assert success, f"Procedure failed: {error_msg}"

    assert_audit_entry('STARTED')
    assert_audit_entry('OK')
    
    # Expected records: DWH_VERTRAG_ID 10 and 13
    expected_target_ids = {10, 13}
    actual_target_data = get_table_data('target_fos_table', ['DWH_VERTRAG_ID'])
    actual_target_ids = {row['DWH_VERTRAG_ID'] for row in actual_target_data}

    assert actual_target_ids == expected_target_ids
    assert get_table_count('target_fos_table') == len(expected_target_ids)
```

---

### Test Case 3: Specific Stichtag and Restart Value

*   **Purpose:** Verify the restart logic: deletion of existing records with `DWH_VERTRAG_ID >= Wiederanlaufwert` and subsequent insertion of new records with `DWH_VERTRAG_ID > Wiederanlaufwert`, respecting date filters.
*   **Setup:**
    *   `source_contract_cache` contains data, including some `DWH_VERTRAG_ID`s above and below the restart value, and some matching the date filters.
    *   `target_fos_table` is pre-populated with data, some of which should be deleted by the restart logic.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('01012023', 100)`.
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully without error.
    2.  `job_audit` contains one `STARTED` entry and one `OK` entry, with `stichtag` '01012023' and `restart_value` 100.
    3.  `job_log` contains an `INFO` message indicating restart cleanup was performed.
    4.  `target_fos_table` reflects:
        *   Deletion of all records where `DWH_VERTRAG_ID >= 100` that existed *before* the run.
        *   Insertion of records from `source_contract_cache` where `DWH_VERTRAG_ID > 100` and satisfy the date filters.
        *   Records with `DWH_VERTRAG_ID < 100` that were in `target_fos_table` initially and were not affected by the deletion remain.

```python
def test_specific_stichtag_with_restart_value():
    stichtag_str = '01012023'
    stichtag_date = date(2023, 1, 1)
    restart_value = 100

    source_data = [
        # Valid, DWH_VERTRAG_ID > restart_value
        {'DWH_VERTRAG_ID': 101, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S1'},
        {'DWH_VERTRAG_ID': 102, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S2'},
        # Invalid, DWH_VERTRAG_ID <= restart_value (should not be inserted)
        {'DWH_VERTRAG_ID': 99, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S3'},
        # Valid, but DWH_VERTRAG_ID = restart_value (should not be inserted due to > filter)
        {'DWH_VERTRAG_ID': 100, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S4'},
        # Valid, but date filter fails
        {'DWH_VERTRAG_ID': 103, 'GUELTIG_VON': date(2023, 1, 2), 'GUELTIG_BIS': date(2023, 1, 3), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S5'},
    ]
    insert_source_data(source_data)

    initial_target_data = [
        # Should remain (DWH_VERTRAG_ID < restart_value)
        {'DWH_VERTRAG_ID': 50, 'SOME_OTHER_COL': 'T1', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)},
        {'DWH_VERTRAG_ID': 99, 'SOME_OTHER_COL': 'T2', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)},
        # Should be deleted (DWH_VERTRAG_ID >= restart_value)
        {'DWH_VERTRAG_ID': 100, 'SOME_OTHER_COL': 'T3', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)},
        {'DWH_VERTRAG_ID': 150, 'SOME_OTHER_COL': 'T4', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)},
    ]
    insert_target_data(initial_target_data)

    success, error_msg = call_stored_procedure(stichtag=stichtag_str, wiederanlaufWert=restart_value)
    assert success, f"Procedure failed: {error_msg}"

    assert_audit_entry('STARTED')
    assert_audit_entry('OK')
    assert_log_entry('INFO', f'Restart cleanup performed for DWH_VERTRAG_ID >= {restart_value}')

    # Expected final target IDs:
    # 50, 99 (from initial_target_data, not deleted)
    # 101, 102 (from source_data, inserted because > restart_value and date filters match)
    expected_final_target_ids = {50, 99, 101, 102}
    actual_target_data = get_table_data('target_fos_table', ['DWH_VERTRAG_ID'])
    actual_target_ids = {row['DWH_VERTRAG_ID'] for row in actual_target_data}

    assert actual_target_ids == expected_final_target_ids
    assert get_table_count('target_fos_table') == len(expected_final_target_ids)
```

---

### Test Case 4: Invalid Stichtag Format

*   **Purpose:** Verify that the procedure correctly validates the `Stichtag` parameter format and raises an error for invalid input.
*   **Setup:**
    *   `job_audit` and `job_log` are empty.
    *   `target_fos_table` contains some initial data (to ensure it's not affected).
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('2023-01-01', NULL)` (invalid format).
*   **Pass/Fail Criterion:**
    1.  The procedure raises an error and does not complete successfully.
    2.  `job_audit` contains one `STARTED` entry and one `ERROR` entry.
    3.  `job_log` contains an `ERROR` entry with a message indicating invalid `Stichtag` format.
    4.  `target_fos_table` remains unchanged.

```python
def test_invalid_stichtag_format():
    initial_target_count = 5
    insert_target_data([{'DWH_VERTRAG_ID': i, 'SOME_OTHER_COL': 'Initial', 'LAST_UPDATE_DATE': date(2020,1,1), 'LADEDATUM': date(2020,1,1)} for i in range(initial_target_count)])

    success, error_msg = call_stored_procedure(stichtag='2023-01-01', wiederanlaufWert=None)
    assert not success, "Procedure should have failed due to invalid Stichtag format"
    assert "Invalid Stichtag parameter. Must be in DDMMYYYY format." in error_msg

    assert_audit_entry('STARTED')
    assert_audit_entry('ERROR', message_substring='Invalid Stichtag parameter')
    assert_log_entry('ERROR', message_substring='Invalid Stichtag parameter')

    assert get_table_count('target_fos_table') == initial_target_count, "Target table should not be modified on validation error"
```

---

### Test Case 5: Empty Source Table

*   **Purpose:** Verify graceful handling when the `source_contract_cache` table is empty.
*   **Setup:**
    *   `source_contract_cache` is empty.
    *   `target_fos_table` is empty.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully without error.
    2.  `job_audit` contains one `STARTED` entry and one `OK` entry.
    3.  `target_fos_table` remains empty.

```python
def test_empty_source_table():
    # source_contract_cache is empty by fixture setup
    
    success, error_msg = call_stored_procedure(stichtag='01012023', wiederanlaufWert=None)
    assert success, f"Procedure failed: {error_msg}"

    assert_audit_entry('STARTED')
    assert_audit_entry('OK')
    assert get_table_count('target_fos_table') == 0, "Target table should remain empty if source is empty"
```

---

### Test Case 6: No Records Match Filters

*   **Purpose:** Verify graceful handling when `source_contract_cache` has data, but none of it matches the specified date filters.
*   **Setup:**
    *   `source_contract_cache` contains data, but all records have `GUELTIG_VON`, `GUELTIG_BIS`, or `LADEDATUM` values that prevent them from being selected for `Stichtag = '01012023'`.
    *   `target_fos_table` is empty.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully without error.
    2.  `job_audit` contains one `STARTED` entry and one `OK` entry.
    3.  `target_fos_table` remains empty.

```python
def test_no_records_match_filters():
    stichtag_str = '01012023'
    stichtag_date = date(2023, 1, 1)

    source_data = [
        # Invalid: LADEDATUM not < Stichtag
        {'DWH_VERTRAG_ID': 20, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2023, 1, 1), 'SOME_OTHER_COL': 'F1'},
        # Invalid: Stichtag not < GUELTIG_BIS
        {'DWH_VERTRAG_ID': 21, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 1), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'F2'},
        # Invalid: Stichtag not >= GUELTIG_VON
        {'DWH_VERTRAG_ID': 22, 'GUELTIG_VON': date(2023, 1, 2), 'GUELTIG_BIS': date(2023, 1, 3), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'F3'},
    ]
    insert_source_data(source_data)

    success, error_msg = call_stored_procedure(stichtag=stichtag_str, wiederanlaufWert=None)
    assert success, f"Procedure failed: {error_msg}"

    assert_audit_entry('STARTED')
    assert_audit_entry('OK')
    assert get_table_count('target_fos_table') == 0, "Target table should remain empty if no records match filters"
```

---

### Test Case 7: Data Quality - No Duplicates in Target After Restart

*   **Purpose:** Ensure that the `target_fos_table` does not contain duplicate `DWH_VERTRAG_ID`s after a run, especially when restart logic is involved. This verifies the `DELETE` and `INSERT` operations work cohesively.
*   **Setup:**
    *   `source_contract_cache` contains unique `DWH_VERTRAG_ID`s, some of which will be selected.
    *   `target_fos_table` is pre-populated with data that includes `DWH_VERTRAG_ID`s that will be deleted by the restart logic, and some that will remain.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('01012023', 100)`.
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully without error.
    2.  After execution, `target_fos_table` contains no duplicate `DWH_VERTRAG_ID`s.
    3.  The count of distinct `DWH_VERTRAG_ID`s equals the total count of records in `target_fos_table`.

```python
def test_no_duplicates_in_target_after_restart():
    stichtag_str = '01012023'
    stichtag_date = date(2023, 1, 1)
    restart_value = 100

    source_data = [
        {'DWH_VERTRAG_ID': 101, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S1'},
        {'DWH_VERTRAG_ID': 102, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S2'},
        {'DWH_VERTRAG_ID': 103, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'S3'},
    ]
    insert_source_data(source_data)

    initial_target_data = [
        {'DWH_VERTRAG_ID': 50, 'SOME_OTHER_COL': 'T1', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)},
        {'DWH_VERTRAG_ID': 99, 'SOME_OTHER_COL': 'T2', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)},
        {'DWH_VERTRAG_ID': 101, 'SOME_OTHER_COL': 'T3_old', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)}, # Should be deleted and replaced
        {'DWH_VERTRAG_ID': 150, 'SOME_OTHER_COL': 'T4', 'LAST_UPDATE_DATE': date(2022, 1, 1), 'LADEDATUM': date(2022, 1, 1)}, # Should be deleted
    ]
    insert_target_data(initial_target_data)

    success, error_msg = call_stored_procedure(stichtag=stichtag_str, wiederanlaufWert=restart_value)
    assert success, f"Procedure failed: {error_msg}"

    # Verify no duplicates
    query_duplicates = f"""
        SELECT DWH_VERTRAG_ID, COUNT(1)
        FROM `{PROJECT_ID}.{DATASET_ID}.target_fos_table`
        GROUP BY DWH_VERTRAG_ID
        HAVING COUNT(1) > 1
    """
    duplicate_rows = list(CLIENT.query(query_duplicates).result())
    assert len(duplicate_rows) == 0, f"Found duplicate DWH_VERTRAG_ID in target_fos_table: {duplicate_rows}"

    # Verify total count matches distinct count
    total_count = get_table_count('target_fos_table')
    distinct_count_query = f"SELECT COUNT(DISTINCT DWH_VERTRAG_ID) FROM `{PROJECT_ID}.{DATASET_ID}.target_fos_table`"
    distinct_count = list(CLIENT.query(distinct_count_query).result())[0][0]
    assert total_count == distinct_count, "Total count does not match distinct count in target_fos_table"
```

---

### Test Case 8: Logging and Audit Trail - Full Success Path

*   **Purpose:** Verify that all expected audit and log entries are created correctly during a successful execution.
*   **Setup:**
    *   `source_contract_cache` contains some valid data.
    *   `target_fos_table` is empty.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('01012023', 50)`. (Using a restart value to ensure the INFO log for cleanup is generated).
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully.
    2.  `job_audit` contains exactly one `STARTED` entry and one `OK` entry for the current job run.
    3.  The `STARTED` entry in `job_audit` has the correct `stichtag` and `restart_value`.
    4.  `job_log` contains an `INFO` entry related to the restart cleanup.

```python
def test_logging_audit_full_success_path():
    stichtag_str = '01012023'
    restart_value = 50

    source_data = [
        {'DWH_VERTRAG_ID': 51, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'L1'},
    ]
    insert_source_data(source_data)
    insert_target_data([{'DWH_VERTRAG_ID': 60, 'SOME_OTHER_COL': 'Old', 'LAST_UPDATE_DATE': date(2022,1,1), 'LADEDATUM': date(2022,1,1)}])

    success, error_msg = call_stored_procedure(stichtag=stichtag_str, wiederanlaufWert=restart_value)
    assert success, f"Procedure failed: {error_msg}"

    # Check audit entries
    audit_entries = get_table_data('job_audit', ['status', 'stichtag', 'restart_value', 'message'])
    assert len(audit_entries) == 2, "Expected exactly two audit entries (STARTED, OK)"

    started_entry = next((e for e in audit_entries if e['status'] == 'STARTED'), None)
    ok_entry = next((e for e in audit_entries if e['status'] == 'OK'), None)

    assert started_entry is not None, "STARTED entry not found in job_audit"
    assert ok_entry is not None, "OK entry not found in job_audit"

    assert started_entry['stichtag'] == stichtag_str
    assert started_entry['restart_value'] == restart_value
    assert 'Job started successfully.' in started_entry['message']

    assert ok_entry['stichtag'] == stichtag_str
    assert ok_entry['restart_value'] == restart_value
    assert 'Job completed successfully.' in ok_entry['message']

    # Check log entries
    log_entries = get_table_data('job_log', ['log_level', 'message'])
    assert len(log_entries) >= 1, "Expected at least one log entry (for restart cleanup)"
    assert_log_entry('INFO', f'Restart cleanup performed for DWH_VERTRAG_ID >= {restart_value}')
```

---

### Test Case 9: Error Handling - Forced Kernel Logic Error

*   **Purpose:** Verify that if an error occurs within the core data processing logic (simulating a kernel script error), the procedure correctly logs the error to `job_audit` and `job_log` and raises the exception.
*   **Setup:**
    *   Temporarily modify `sp_bereitstellung_basisprodukte_bert.sql` to force an error within the `BEGIN...EXCEPTION` block (e.g., attempt to divide by zero, or reference a non-existent column in the `INSERT...SELECT` statement).
    *   `source_contract_cache` contains some valid data.
    *   `target_fos_table` is empty.
    *   `job_audit` and `job_log` are empty.
*   **Action:** Call `sp_bereitstellung_basisprodukte_bert('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The procedure raises an error and does not complete successfully.
    2.  `job_audit` contains one `STARTED` entry and one `ERROR` entry.
    3.  `job_log` contains an `ERROR` entry with the specific error message from the forced error.
    4.  `target_fos_table` remains unchanged or is in a consistent state (e.g., no partial inserts if the error occurred during `INSERT`).

```python
# NOTE: This test requires a temporary modification to the stored procedure
# to simulate an internal error. This would typically be done in a dedicated
# test environment or with a mocked version of the SP.

# Example temporary modification in sp_bereitstellung_basisprodukte_bert.sql:
# Inside the BEGIN...EXCEPTION block, before the INSERT:
# SELECT 1/0; -- This will cause a division by zero error

def test_error_handling_forced_kernel_logic_error():
    stichtag_str = '01012023'
    source_data = [
        {'DWH_VERTRAG_ID': 1, 'GUELTIG_VON': date(2022, 12, 1), 'GUELTIG_BIS': date(2023, 1, 2), 'LADEDATUM': date(2022, 12, 31), 'SOME_OTHER_COL': 'E1'},
    ]
    insert_source_data(source_data)

    # Assuming the SP is modified to cause an error, e.g., SELECT 1/0;
    success, error_msg = call_stored_procedure(stichtag=stichtag_str, wiederanlaufWert=None)
    assert not success, "Procedure should have failed due to forced error"
    assert "Division by zero" in error_msg # Or other specific error message

    assert_audit_entry('STARTED')
    assert_audit_entry('ERROR', message_substring='Job failed: Division by zero') # Adjust message based on actual error
    assert_log_entry('ERROR', message_substring='Job execution failed: Division by zero') # Adjust message based on actual error

    assert get_table_count('target_fos_table') == 0, "Target table should remain empty or in a consistent state after an internal error"
```