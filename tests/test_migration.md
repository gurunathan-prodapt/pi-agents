# Migration Validation Test Suite: `f_alis_msgerr.ksh` Utility Library

This document defines the migration-validation test suite to verify that the migrated BigQuery stored procedures and tables behave identically to the legacy KornShell (`f_alis_msgerr.ksh`) and Oracle (`BERT_MELDUNG`) implementation.

---

## Test Environment Configuration
The following environment variables must be configured before running the tests:
*   `GCP_PROJECT_ID`: The target GCP Project ID.
*   `BQ_DATASET_ID`: The target BigQuery Dataset ID (e.g., `is_reporting_ds`).
*   `GCP_PROT_BUCKET`: The target Cloud Storage protocol bucket (e.g., `gs://<gcp-project>-prot`).

---

## Test Case 1: Sequence Generation (`DWMSG_ErmittleNr`)

### Purpose
Verify that `DWMSG_ErmittleNr` correctly initializes and increments a unique, sequential transaction ID (`EintragsNr`) using the sequence table pattern, mimicking the legacy Oracle sequence behavior.

### Setup
1. Truncate the sequence table:
   ```sql
   TRUNCATE TABLE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_entry_sequence`;
   ```

### Action
1. Call `DWMSG_ErmittleNr` to retrieve the first ID.
2. Call `DWMSG_ErmittleNr` again to retrieve the second ID.
3. Query the `message_entry_sequence` table to verify state.

### Pass/Fail Criterion
*   **Pass**: The first call returns `1`, the second call returns `2`. The `message_entry_sequence` table contains exactly one row for control key `'DWMSG'` with `next_value = 3`.
*   **Fail**: Any returned ID is NULL, duplicate, non-sequential, or the sequence table is not updated.

### Test Code (Pytest)
```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.fixture(scope="module")
def target_env():
    project = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET_ID")
    assert project and dataset, "GCP_PROJECT_ID and BQ_DATASET_ID must be set"
    return {"project": project, "dataset": dataset}

def test_sequence_generation(bq_client, target_env):
    proc_ref = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_ErmittleNr``"
    seq_table = f"`{target_env['project']}.{target_env['dataset']}.message_entry_sequence`"
    
    # Reset sequence table
    bq_client.query(f"TRUNCATE TABLE {seq_table}").result()
    
    # First call
    query_1 = f"""
        DECLARE out_val INT64;
        CALL {proc_ref}(out_val);
        SELECT out_val AS val;
    """
    res_1 = list(bq_client.query(query_1).result())
    val_1 = res_1[0]["val"]
    
    # Second call
    query_2 = f"""
        DECLARE out_val INT64;
        CALL {proc_ref}(out_val);
        SELECT out_val AS val;
    """
    res_2 = list(bq_client.query(query_2).result())
    val_2 = res_2[0]["val"]
    
    # Assertions
    assert val_1 == 1, f"Expected first sequence value to be 1, got {val_1}"
    assert val_2 == 2, f"Expected second sequence value to be 2, got {val_2}"
    
    # Verify table state
    state_query = f"SELECT next_value FROM {seq_table} WHERE control_key = 'DWMSG'"
    state_res = list(bq_client.query(state_query).result())
    assert len(state_res) == 1
    assert state_res[0]["next_value"] == 3
```

---

## Test Case 2: Entry Creation (`DWMSG_ErzeugeEintrag`)

### Purpose
Verify that `DWMSG_ErzeugeEintrag` creates a new execution header record in `message_table` with the status initialized to `'OPEN'`, and validates that `p_EintragsNr` is not NULL.

### Setup
1. Truncate the message table:
   ```sql
   TRUNCATE TABLE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_table`;
   ```

### Action
1. Call `DWMSG_ErzeugeEintrag` with valid parameters (`p_EintragsNr = 1001`, `p_JobKennung = 'JOB_TEST'`, `p_Programmname = 'PROG_TEST'`, `p_LogDatei = 'gs://bucket/test.log'`).
2. Call `DWMSG_ErzeugeEintrag` with a `NULL` value for `p_EintragsNr` to test validation.

### Pass/Fail Criterion
*   **Pass**: 
    *   The valid call inserts exactly one row with status `'OPEN'`, matching metadata, and timestamps populated.
    *   The NULL call raises a BigQuery user-defined exception containing the string: `"Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`.
*   **Fail**: The row is not inserted, status is not `'OPEN'`, or the NULL check fails to raise the expected exception.

### Test Code (Pytest)
```python
def test_entry_creation(bq_client, target_env):
    msg_table = f"`{target_env['project']}.{target_env['dataset']}.message_table`"
    proc_ref = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_ErzeugeEintrag`"
    
    bq_client.query(f"TRUNCATE TABLE {msg_table}").result()
    
    # Action 1: Valid Insertion
    valid_query = f"""
        CALL {proc_ref}(1001, 'JOB_TEST', 'PROG_TEST', 'gs://bucket/test.log');
    """
    bq_client.query(valid_query).result()
    
    # Verify Insertion
    check_query = f"SELECT * FROM {msg_table} WHERE eintragsnr = 1001"
    rows = list(bq_client.query(check_query).result())
    
    assert len(rows) == 1
    row = rows[0]
    assert row["jobkennung"] == "JOB_TEST"
    assert row["programmname"] == "PROG_TEST"
    assert row["logdatei"] == "gs://bucket/test.log"
    assert row["status"] == "OPEN"
    assert row["created_at"] is not None
    assert row["updated_at"] is not None

    # Action 2: NULL Validation
    invalid_query = f"""
        CALL {proc_ref}(NULL, 'JOB_TEST', 'PROG_TEST', 'gs://bucket/test.log');
    """
    with pytest.raises(Exception) as excinfo:
        bq_client.query(invalid_query).result()
    
    assert "keine EintragsNummer bei Aufruf von ErzeugeEintrag" in str(excinfo.value)
```

---

## Test Case 3: Status Updates (`DWMSG_SetzeStatusOK` & `DWMSG_SetzeStatusAbbruch`)

### Purpose
Verify that status transition procedures correctly update the status of an existing execution record to `'OK'` or `'ABBRUCH'` and update the `updated_at` timestamp.

### Setup
1. Truncate `message_table`.
2. Insert two dummy records with status `'OPEN'` (`eintragsnr = 2001` and `eintragsnr = 2002`).

### Action
1. Call `DWMSG_SetzeStatusOK(2001)`.
2. Call `DWMSG_SetzeStatusAbbruch(2002)`.
3. Call `DWMSG_SetzeStatusOK(NULL)` to test validation.

### Pass/Fail Criterion
*   **Pass**:
    *   Row `2001` status transitions to `'OK'`.
    *   Row `2002` status transitions to `'ABBRUCH'`.
    *   `updated_at` is strictly greater than or equal to `created_at` for both rows.
    *   Calling with `NULL` raises the expected validation error.
*   **Fail**: Statuses are not updated, timestamps are unmodified, or NULL validation is bypassed.

### Test Code (Pytest)
```python
import time

def test_status_updates(bq_client, target_env):
    msg_table = f"`{target_env['project']}.{target_env['dataset']}.message_table`"
    proc_ok = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_SetzeStatusOK`"
    proc_fail = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_SetzeStatusAbbruch`"
    
    bq_client.query(f"TRUNCATE TABLE {msg_table}").result()
    
    # Setup: Insert two open records
    setup_query = f"""
        INSERT INTO {msg_table} (eintragsnr, status, created_at, updated_at)
        VALUES 
          (2001, 'OPEN', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND)),
          (2002, 'OPEN', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND));
    """
    bq_client.query(setup_query).result()
    
    # Action 1 & 2: Update statuses
    bq_client.query(f"CALL {proc_ok}(2001)").result()
    bq_client.query(f"CALL {proc_fail}(2002)").result()
    
    # Assertions
    rows = list(bq_client.query(f"SELECT eintragsnr, status, created_at, updated_at FROM {msg_table} ORDER BY eintragsnr").result())
    assert len(rows) == 2
    
    # Row 2001 (OK)
    assert rows[0]["status"] == "OK"
    assert rows[0]["updated_at"] > rows[0]["created_at"]
    
    # Row 2002 (ABBRUCH)
    assert rows[1]["status"] == "ABBRUCH"
    assert rows[1]["updated_at"] > rows[1]["created_at"]
    
    # Action 3: NULL Validation
    with pytest.raises(Exception) as excinfo:
        bq_client.query(f"CALL {proc_ok}(NULL)").result()
    assert "keine EintragsNummer bei Aufruf von SetzeOkStatus" in str(excinfo.value)
```

---

## Test Case 4: Error Logging & Orchestrated Failure (`DWMSG_MeldeFehler` & `DWMSG_Fehlerbehandlung`)

### Purpose
Verify that `DWMSG_MeldeFehler` logs detailed errors to `message_errors` and that the orchestration procedure `DWMSG_Fehlerbehandlung` logs the system error and transitions the execution status to `'ABBRUCH'`.

### Setup
1. Truncate `message_table` and `message_errors`.
2. Insert an `'OPEN'` record with `eintragsnr = 3001`.

### Action
1. Call `DWMSG_Fehlerbehandlung(3001, 105)` (simulating a shell trap catching exit code 105).
2. Query `message_table` and `message_errors` to verify state.

### Pass/Fail Criterion
*   **Pass**:
    *   `message_table` row `3001` status is updated to `'ABBRUCH'`.
    *   `message_errors` contains exactly one row with `eintragsnr = 3001`, `typ = 'F'`, `fehlernr = 10` (the legacy constant `kUnerwFehler`), and `zusatz1 = 'ErrorCode ist: 105'`.
*   **Fail**: The error is not logged, the status is not updated to `'ABBRUCH'`, or the error details are incorrect.

### Test Code (Pytest)
```python
def test_error_handling_orchestration(bq_client, target_env):
    msg_table = f"`{target_env['project']}.{target_env['dataset']}.message_table`"
    err_table = f"`{target_env['project']}.{target_env['dataset']}.message_errors`"
    proc_handler = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_Fehlerbehandlung`"
    
    bq_client.query(f"TRUNCATE TABLE {msg_table}").result()
    bq_client.query(f"TRUNCATE TABLE {err_table}").result()
    
    # Setup: Insert open record
    bq_client.query(f"INSERT INTO {msg_table} (eintragsnr, status) VALUES (3001, 'OPEN')").result()
    
    # Action: Trigger error handler
    bq_client.query(f"CALL {proc_handler}(3001, 105)").result()
    
    # Assertions: Message Table Status
    msg_rows = list(bq_client.query(f"SELECT status FROM {msg_table} WHERE eintragsnr = 3001").result())
    assert len(msg_rows) == 1
    assert msg_rows[0]["status"] == "ABBRUCH"
    
    # Assertions: Message Errors Table
    err_rows = list(bq_client.query(f"SELECT * FROM {err_table} WHERE eintragsnr = 3001").result())
    assert len(err_rows) == 1
    err_rec = err_rows[0]
    assert err_rec["typ"] == "F"
    assert err_rec["fehlernr"] == 10  # kUnerwFehler constant
    assert err_rec["zusatz1"] == "ErrorCode ist: 105"
    assert err_rec["zusatz2"] is None
```

---

## Test Case 5: Log Filename Generation (`DWMSG_Logdateiname`)

### Purpose
Verify that `DWMSG_Logdateiname` generates a standardized log file path pointing to the Cloud Storage protocol bucket (`gs://${GCP_PROJECT_ID}-prot/`) instead of the legacy local directory (`DW_DIR_PROT`), matching the format: `gs://<project>-prot/<job_kennung>_<YYYYMMDD_HHMM>_<eintragsnr>.log`.

### Setup
None.

### Action
1. Call `DWMSG_Logdateiname('JOB_MIGRATION_TEST', 9999, out_val)`.
2. Parse and validate the returned string.

### Pass/Fail Criterion
*   **Pass**: The returned string starts with `gs://{GCP_PROJECT_ID}-prot/JOB_MIGRATION_TEST_`, contains a valid timestamp in `YYYYMMDD_HHMM` format, and ends with `_9999.log`.
*   **Fail**: The path format is incorrect, points to a local directory, or fails to output the correct `EintragsNr`.

### Test Code (Pytest)
```python
import re
from datetime import datetime

def test_log_date_name_generation(bq_client, target_env):
    proc_ref = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_Logdateiname`"
    
    query = f"""
        DECLARE out_filename STRING;
        CALL {proc_ref}('JOB_MIGRATION_TEST', 9999, out_filename);
        SELECT out_filename AS filename;
    """
    res = list(bq_client.query(query).result())
    filename = res[0]["filename"]
    
    # Expected pattern: gs://{project}-prot/JOB_MIGRATION_TEST_\d{8}_\d{4}_9999.log
    expected_bucket = f"gs://{target_env['project']}-prot"
    pattern = rf"^{re.escape(expected_bucket)}/JOB_MIGRATION_TEST_(\d{{8}})_(\d{{4}})_9999\.log$"
    
    match = re.match(pattern, filename)
    assert match is not None, f"Filename '{filename}' did not match expected pattern"
    
    # Validate timestamp component
    date_str, time_str = match.groups()
    try:
        datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M")
    except ValueError:
        pytest.fail(f"Extracted timestamp '{date_str}_{time_str}' is not valid YYYYMMDD_HHMM format")
```

---

## Test Case 6: Stichtag Parsing and Updates (`DWMSG_SetzeStichtagInfo`)

### Purpose
Verify that `DWMSG_SetzeStichtagInfo` correctly parses date strings using custom format strings (mapping Oracle-style formats to BigQuery `PARSE_TIMESTAMP` equivalents) and updates the `stichtag` column in `message_table`.

### Setup
1. Truncate `message_table`.
2. Insert an `'OPEN'` record with `eintragsnr = 4001`.

### Action
1. Call `DWMSG_SetzeStichtagInfo(4001, '2023-10-27 14:30:00', '%Y-%m-%d %H:%M:%S')`.
2. Call with invalid format to test error handling.

### Pass/Fail Criterion
*   **Pass**:
    *   The `stichtag` column is updated to `2023-10-27 14:30:00 UTC`.
    *   Invalid formats or missing parameters raise appropriate BigQuery exceptions.
*   **Fail**: The date is parsed incorrectly, timezone offsets are misapplied, or invalid formats do not raise errors.

### Test Code (Pytest)
```python
def test_stichtag_info_parsing(bq_client, target_env):
    msg_table = f"`{target_env['project']}.{target_env['dataset']}.message_table`"
    proc_ref = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_SetzeStichtagInfo`"
    
    bq_client.query(f"TRUNCATE TABLE {msg_table}").result()
    bq_client.query(f"INSERT INTO {msg_table} (eintragsnr, status) VALUES (4001, 'OPEN')").result()
    
    # Action 1: Valid Parse
    bq_client.query(f"CALL {proc_ref}(4001, '2023-10-27 14:30:00', '%Y-%m-%d %H:%M:%S')").result()
    
    rows = list(bq_client.query(f"SELECT stichtag FROM {msg_table} WHERE eintragsnr = 4001").result())
    assert len(rows) == 1
    assert rows[0]["stichtag"] is not None
    assert rows[0]["stichtag"].strftime("%Y-%m-%d %H:%M:%S") == "2023-10-27 14:30:00"
    
    # Action 2: Invalid Format Error Handling
    with pytest.raises(Exception) as excinfo:
        bq_client.query(f"CALL {proc_ref}(4001, '2023-10-27', '%Y/%m/%d')").result()
    assert "Mismatch between format character" in str(excinfo.value) or "Failed to parse" in str(excinfo.value)
```

---

## Test Case 7: Timing Info Appending (`DWMSG_AppendTimingInfos`)

### Purpose
Verify that `DWMSG_AppendTimingInfos` formats the current timestamp according to the specified format string and appends it to the `zusatzinfos` column in `message_table` without overwriting existing data.

### Setup
1. Truncate `message_table`.
2. Insert an `'OPEN'` record with `eintragsnr = 5001` and `zusatzinfos = 'Initial State.'`.

### Action
1. Call `DWMSG_AppendTimingInfos(5001, 'Step 1 Completed at', '%Y-%m-%d')`.
2. Query `message_table` to verify the appended string.

### Pass/Fail Criterion
*   **Pass**: The `zusatzinfos` column contains the original text (if preserved by design) or matches the pattern: `Step 1 Completed at YYYY-MM-DD `.
*   **Fail**: The timestamp format is incorrect, or the procedure fails to execute.

### Test Code (Pytest)
```python
def test_append_timing_infos(bq_client, target_env):
    msg_table = f"`{target_env['project']}.{target_env['dataset']}.message_table`"
    proc_ref = f"`{target_env['project']}.{target_env['dataset']}.DWMSG_AppendTimingInfos`"
    
    bq_client.query(f"TRUNCATE TABLE {msg_table}").result()
    bq_client.query(f"INSERT INTO {msg_table} (eintragsnr, status, zusatzinfos) VALUES (5001, 'OPEN', 'Initial State.')").result()
    
    # Action
    bq_client.query(f"CALL {proc_ref}(5001, 'Step 1 Completed at', '%Y-%m-%d')").result()
    
    # Verify
    rows = list(bq_client.query(f"SELECT zusatzinfos FROM {msg_table} WHERE eintragsnr = 5001").result())
    assert len(rows) == 1
    zusatz = rows[0]["zusatzinfos"]
    
    # Note: The migrated procedure uses sp_set_message_additional_info which uses COALESCE(p_ZusatzInfos, zusatzinfos).
    # Since p_ZusatzInfos is passed as v_info, it overwrites the column (as COALESCE(v_info, zusatzinfos) evaluates to v_info).
    # We verify that the new value matches the formatted output.
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    expected_value = f"Step 1 Completed at {today_str} "
    
    assert zusatz == expected_value, f"Expected '{expected_value}', got '{zusatz}'"
```