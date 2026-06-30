# Migration Validation Test Suite: `f_alis_msgerr.ksh`

This document defines the migration-validation tests to prove behavioral equivalence between the legacy KornShell/Oracle implementation of the `f_alis_msgerr.ksh` utility and the migrated Google BigQuery stored procedures and Python orchestration helpers.

---

## Test Case 1: Sequence Generation & Concurrency (`DWMSG_ErmittleNr`)

### Purpose
Verify that the migrated stored procedure `DWMSG_ErmittleNr` correctly increments and retrieves a unique, monotonically increasing sequence number from `message_sequence_table` within a transaction block, replacing the legacy Oracle sequence and temporary file-based retrieval (`/tmp/ErmittleNr_$$`).

### Setup
1. Ensure the `message_sequence_table` is initialized with a known starting value.
2. Clear any existing sequence values or set the sequence to a baseline.

```sql
-- SQL Setup
UPDATE `${gcp_project_id}.${audit_dataset}.message_sequence_table`
SET current_value = 100,
    updated_at = CURRENT_TIMESTAMP()
WHERE sequence_name = 'DWMSG';
```

### Action
Execute the `DWMSG_ErmittleNr` procedure sequentially multiple times, and concurrently using a multi-threaded Python script to simulate parallel ETL job starts.

#### Python Test Code (`pytest` + Google Cloud BigQuery Client)
```python
import pytest
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery

PROJECT_ID = "your-gcp-project"
DATASET = "your_audit_dataset"

def call_ermittle_nr(client):
    query = f"""
        DECLARE out_val INT64;
        CALL `{PROJECT_ID}.{DATASET}.DWMSG_ErmittleNr`(out_val);
        SELECT out_val;
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0]

def test_sequential_sequence_generation():
    client = bigquery.Client()
    
    # Reset sequence to 100
    client.query(f"UPDATE `{PROJECT_ID}.{DATASET}.message_sequence_table` SET current_value = 100 WHERE sequence_name = 'DWMSG'").result()
    
    val1 = call_ermittle_nr(client)
    val2 = call_ermittle_nr(client)
    val3 = call_ermittle_nr(client)
    
    assert val1 == 101
    assert val2 == 102
    assert val3 == 103

def test_concurrent_sequence_generation():
    client = bigquery.Client()
    client.query(f"UPDATE `{PROJECT_ID}.{DATASET}.message_sequence_table` SET current_value = 1000 WHERE sequence_name = 'DWMSG'").result()
    
    num_threads = 5
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Execute concurrent calls
        futures = [executor.submit(call_ermittle_nr, client) for _ in range(num_threads)]
        results = [f.result() for f in futures]
        
    # Assert all returned values are unique and within the expected range
    assert len(set(results)) == num_threads
    assert min(results) >= 1001
    assert max(results) <= 1000 + num_threads
```

### Pass/Fail Criterion
* **Pass**: Sequential calls return strictly incremented integers (e.g., 101, 102, 103). Concurrent calls return unique integers with no duplicates or transaction deadlocks.
* **Fail**: Any duplicate sequence numbers are generated, or the transaction block fails to commit due to unhandled concurrency conflicts.

---

## Test Case 2: Entry Creation & Status Transitions (`DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`, `DWMSG_SetzeStatusAbbruch`)

### Purpose
Verify the lifecycle of a message entry from creation (`CREATED`) to completion (`OK` or `ABBRUCH`). This replaces the legacy Oracle `BERT_MELDUNG.Erzeuge_Eintrag`, `SetzeStatusOk`, and `SetzeStatusAbbruch` procedures.

### Setup
Truncate or clean the target `message_table` for the test partition.

```sql
-- SQL Setup
DELETE FROM `${gcp_project_id}.${audit_dataset}.message_table` WHERE entry_nr IN (99901, 99902);
```

### Action
1. Call `DWMSG_ErzeugeEintrag` with `entry_nr = 99901`.
2. Call `DWMSG_SetzeStatusOK` for `entry_nr = 99901`.
3. Call `DWMSG_ErzeugeEintrag` with `entry_nr = 99902`.
4. Call `DWMSG_SetzeStatusAbbruch` for `entry_nr = 99902`.
5. Attempt to call `DWMSG_ErzeugeEintrag` with a `NULL` entry number to test input validation.

```sql
-- SQL Action Execution
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_ErzeugeEintrag`(99901, 'JOB_TEST_OK', 'prog_test_ok', 'gs://bucket/logs/test_ok.log');
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_SetzeStatusOK`(99901);

CALL `${gcp_project_id}.${audit_dataset}.DWMSG_ErzeugeEintrag`(99902, 'JOB_TEST_ERR', 'prog_test_err', 'gs://bucket/logs/test_err.log');
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_SetzeStatusAbbruch`(99902);
```

### Pass/Fail Criterion
* **Pass**: 
  * Entry `99901` has status `OK` and `updated_at >= created_at`.
  * Entry `99902` has status `ABBRUCH` and `updated_at >= created_at`.
  * Calling the procedures with `NULL` as the entry number raises an exception with the message: `"Argh!, keine EintragsNummer bei Aufruf von..."`.
* **Fail**: Statuses are not updated, timestamps are unmodified, or `NULL` inputs do not raise the expected exceptions.

#### Verification Query
```sql
-- Assertions Query
SELECT 
  entry_nr, 
  status, 
  job_kennung, 
  programmname, 
  log_datei,
  (updated_at >= created_at) AS timestamp_valid
FROM `${gcp_project_id}.${audit_dataset}.message_table`
WHERE entry_nr IN (99901, 99902)
ORDER BY entry_nr;

-- Expected Output:
-- Row 1: 99901 | OK      | JOB_TEST_OK  | prog_test_ok  | gs://bucket/logs/test_ok.log  | true
-- Row 2: 99902 | ABBRUCH | JOB_TEST_ERR | prog_test_err | gs://bucket/logs/test_err.log | true
```

---

## Test Case 3: Error Reporting with Dynamic Parameters (`DWMSG_MeldeFehler`)

### Purpose
Verify that `DWMSG_MeldeFehler` correctly handles dynamic parameter counts (3, 4, or 5 parameters) and maps them to the target table, replacing the legacy dynamic SQL wrapper scripts (`d_alis_spaufruf_p3.sql`, `d_alis_spaufruf_p4.sql`, `d_alis_spaufruf_p5.sql`).

### Setup
Clear the `message_error_table` for the test partition.

```sql
-- SQL Setup
DELETE FROM `${gcp_project_id}.${audit_dataset}.message_error_table` WHERE entry_nr = 88801;
```

### Action
Execute `DWMSG_MeldeFehler` three times representing the three legacy parameter variations:
1. **3 Parameters**: Only `EintragsNr`, `Typ`, and `FehlerNr` (Zusatz1 and Zusatz2 are NULL).
2. **4 Parameters**: `Zusatz1` is populated, `Zusatz2` is NULL.
3. **5 Parameters**: Both `Zusatz1` and `Zusatz2` are populated.

```sql
-- SQL Action Execution
-- 3 Parameters equivalent
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_MeldeFehler`(88801, 'W', 101, NULL, NULL);

-- 4 Parameters equivalent
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_MeldeFehler`(88801, 'E', 102, 'MissingFile.txt', NULL);

-- 5 Parameters equivalent
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_MeldeFehler`(88801, 'F', 103, 'DatabaseConnection', 'Timeout after 30s');
```

### Pass/Fail Criterion
* **Pass**: 
  * Three distinct rows are inserted into `message_error_table` for `entry_nr = 88801`.
  * `error_id` is sequentially incremented (using the `MAX(error_id) + 1` logic).
  * Optional fields `zusatz1` and `zusatz2` are correctly mapped or set to `NULL`.
* **Fail**: Rows are missing, `error_id` values collide, or optional parameters are incorrectly shifted or omitted.

#### Verification Query
```sql
-- Assertions Query
SELECT 
  error_id,
  typ,
  fehler_nr,
  zusatz1,
  zusatz2
FROM `${gcp_project_id}.${audit_dataset}.message_error_table`
WHERE entry_nr = 88801
ORDER BY error_id ASC;

-- Expected Output:
-- Row 1: <ID_1> | W | 101 | NULL                 | NULL
-- Row 2: <ID_2> | E | 102 | MissingFile.txt      | NULL
-- Row 3: <ID_3> | F | 103 | DatabaseConnection   | Timeout after 30s
```

---

## Test Case 4: Log Filename Generation (`DWMSG_Logdateiname`)

### Purpose
Verify that `DWMSG_Logdateiname` constructs the log file path correctly using the GCS bucket path, job identifier, current timestamp, and entry number, matching the legacy shell pattern: `${DW_DIR_PROT}/${JobKennung}_\`date '+%Y%m%d_%H%M'\`_${DWMSG_EintragsNr}.log`.

### Setup
Define the expected GCS bucket path variable (e.g., `gs://prod-dw-protocol-logs`).

### Action
Call `DWMSG_Logdateiname` and capture the output variable.

```sql
-- SQL Action Execution
DECLARE out_filename STRING;
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_Logdateiname`('JOB_MIGRATION_TEST', 77701, out_filename);
SELECT out_filename;
```

### Pass/Fail Criterion
* **Pass**: The returned filename matches the regex pattern:
  `^gs:\/\/[a-zA-Z0-9\-_.]+\/JOB_MIGRATION_TEST_[0-9]{8}_[0-9]{4}_77701\.log$`
* **Fail**: The output path does not start with the correct GCS bucket prefix, is missing the timestamp, or does not include the entry number.

#### Python Verification Code
```python
import re
import pytest
from google.cloud import bigquery

def test_log_filename_format():
    client = bigquery.Client()
    query = """
        DECLARE out_filename STRING;
        CALL `your-gcp-project.your_audit_dataset.DWMSG_Logdateiname`('JOB_MIGRATION_TEST', 77701, out_filename);
        SELECT out_filename;
    """
    result = list(client.query(query).result())[0][0]
    
    # Pattern matches: gs://<bucket_name>/JOB_MIGRATION_TEST_YYYYMMDD_HHMM_77701.log
    pattern = r"^gs:\/\/[a-zA-Z0-9\-_.]+\/JOB_MIGRATION_TEST_\d{8}_\d{4}_77701\.log$"
    assert re.match(pattern, result) is not None, f"Filename '{result}' did not match expected format."
```

---

## Test Case 5: Additional Info & Timing Updates (`DWMSG_SetzeStichtagInfo`, `DWMSG_AppendTimingInfos`)

### Purpose
Verify date parsing, format handling, and string concatenation for timing logs. This replaces legacy calls to `BERT_MELDUNG.SetzeZusatzInfos` with date conversions and string appends.

### Setup
Create a base message entry to update.

```sql
-- SQL Setup
DELETE FROM `${gcp_project_id}.${audit_dataset}.message_table` WHERE entry_nr = 66601;
DELETE FROM `${gcp_project_id}.${audit_dataset}.message_timing_table` WHERE entry_nr = 66601;

CALL `${gcp_project_id}.${audit_dataset}.DWMSG_ErzeugeEintrag`(66601, 'JOB_TIMING_TEST', 'prog_timing', 'gs://bucket/logs/timing.log');
```

### Action
1. Call `DWMSG_SetzeStichtagInfo` with a standard date string and format.
2. Call `DWMSG_AppendTimingInfos` twice to append sequential execution milestones.

```sql
-- SQL Action Execution
-- Set Stichtag (using BigQuery PARSE_DATE format equivalent to legacy Oracle format)
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_SetzeStichtagInfo`(66601, '2026-03-30', '%Y-%m-%d');

-- Append Timing Milestones
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_AppendTimingInfos`(66601, 'START_STEP_1', '%Y-%m-%d %H:%M:%S');
CALL `${gcp_project_id}.${audit_dataset}.DWMSG_AppendTimingInfos`(66601, 'END_STEP_1', '%Y-%m-%d %H:%M:%S');
```

### Pass/Fail Criterion
* **Pass**:
  * `zusatzinfos_date` in `message_table` is updated to `2026-03-30`.
  * `zusatzinfos_text` in `message_table` contains both timing strings concatenated sequentially.
  * Two rows are inserted into `message_timing_table` with incrementing `timing_id` values.
* **Fail**: Date parsing fails, timing strings overwrite instead of appending, or the `message_timing_table` does not capture the audit trail.

#### Verification Query
```sql
-- Assertions Query
SELECT 
  zusatzinfos_date,
  zusatzinfos_text
FROM `${gcp_project_id}.${audit_dataset}.message_table`
WHERE entry_nr = 66601;

-- Expected Output:
-- zusatzinfos_date: 2026-03-30
-- zusatzinfos_text: Contains "START_STEP_1 <TIMESTAMP> END_STEP_1 <TIMESTAMP>"

SELECT 
  timing_id,
  info_text,
  timing_text
FROM `${gcp_project_id}.${audit_dataset}.message_timing_table`
WHERE entry_nr = 66601
ORDER BY timing_id;

-- Expected Output:
-- Row 1: 1 | START_STEP_1 | START_STEP_1 YYYY-MM-DD HH:MM:SS
-- Row 2: 2 | END_STEP_1   | END_STEP_1 YYYY-MM-DD HH:MM:SS
```

---

## Test Case 6: Error Handler Trap (`DWMSG_Fehlerbehandlung`)

### Purpose
Verify that the error handler captures the error state, logs a fatal error (`F`, code 10) with the error code, and marks the entry status as `ABBRUCH`. This replaces the legacy shell `trap 'DWMSG_Fehlerbehandlung $EintragsNr' ERR` mechanism.

### Setup
Create a message entry to simulate a failing job.

```sql
-- SQL Setup
DELETE FROM `${gcp_project_id}.${audit_dataset}.message_table` WHERE entry_nr = 55501;
DELETE FROM `${gcp_project_id}.${audit_dataset}.message_error_table` WHERE entry_nr = 55501;

CALL `${gcp_project_id}.${audit_dataset}.DWMSG_ErzeugeEintrag`(55501, 'JOB_FAILING', 'prog_fail', 'gs://bucket/logs/fail.log');
```

### Action
Simulate a failure block and invoke `DWMSG_Fehlerbehandlung`.

```sql
-- SQL Action Execution
BEGIN
  -- Force a runtime error (e.g., division by zero) to trigger exception block
  DECLARE division_by_zero FLOAT64;
  SET division_by_zero = 1 / 0;
EXCEPTION WHEN ERROR THEN
  -- Call the migrated error handler
  CALL `${gcp_project_id}.${audit_dataset}.DWMSG_Fehlerbehandlung`(55501);
END;
```

### Pass/Fail Criterion
* **Pass**:
  * A row is added to `message_error_table` with `entry_nr = 55501`, `typ = 'F'`, and `fehler_nr = 10`.
  * `zusatz1` contains the text `'ErrorCode ist: <error_code>'` (where `<error_code>` is the BigQuery error code for division by zero, e.g., `division by zero` or standard system error code).
  * The entry status in `message_table` is updated to `ABBRUCH`.
* **Fail**: The error is swallowed, the error table is not updated, or the status remains `CREATED`.

#### Verification Query
```sql
-- Assertions Query
SELECT status FROM `${gcp_project_id}.${audit_dataset}.message_table` WHERE entry_nr = 55501;
-- Expected: ABBRUCH

SELECT typ, fehler_nr, zusatz1 
FROM `${gcp_project_id}.${audit_dataset}.message_error_table` 
WHERE entry_nr = 55501;
-- Expected: F | 10 | ErrorCode ist: <error_code>
```

---

## Test Case 7: Python Orchestration Utility Parity (`dwmsg.py`)

### Purpose
Verify that the Python helper functions in `dwmsg.py` generate syntactically correct BigQuery SQL statements matching the expected procedure signatures.

### Setup
Initialize the `DwmsgConfig` with mock environment parameters.

### Action
Execute unit tests against the Python helper functions to assert the generated SQL strings.

#### Python Test Code (`pytest`)
```python
import pytest
from gcp.orchestration.utils.dwmsg import (
    DwmsgConfig,
    call_set_status_ok,
    call_set_status_abbruch,
    call_erzeuge_eintrag,
    call_melde_fehler,
    call_setze_stichtag_info,
    call_append_timing_infos
)

@pytest.fixture
def config():
    return DwmsgConfig(
        project_id="gcp-dw-test",
        audit_dataset="dw_audit_logging",
        protocol_gcs_bucket="gs://test-protocol-logs"
    )

def test_call_set_status_ok(config):
    sql = call_set_status_ok(config, 12345)
    assert sql == "CALL `gcp-dw-test.dw_audit_logging.DWMSG_SetzeStatusOK`(12345);"

def test_call_set_status_abbruch(config):
    sql = call_set_status_abbruch(config, 12345)
    assert sql == "CALL `gcp-dw-test.dw_audit_logging.DWMSG_SetzeStatusAbbruch`(12345);"

def test_call_erzeuge_eintrag(config):
    sql = call_erzeuge_eintrag(config, 12345, "JOB_A", "PROG_B", "gs://bucket/log.txt")
    assert sql == "CALL `gcp-dw-test.dw_audit_logging.DWMSG_ErzeugeEintrag`(12345, 'JOB_A', 'PROG_B', 'gs://bucket/log.txt');"

def test_call_melde_fehler(config):
    sql = call_melde_fehler(config, 12345, "F", 99, "ErrDetail1", "ErrDetail2")
    assert sql == "CALL `gcp-dw-test.dw_audit_logging.DWMSG_MeldeFehler`(12345, 'F', 99, 'ErrDetail1', 'ErrDetail2');"

def test_call_melde_fehler_nulls(config):
    sql = call_melde_fehler(config, 12345, "W", 50)
    assert sql == "CALL `gcp-dw-test.dw_audit_logging.DWMSG_MeldeFehler`(12345, 'W', 50, NULL, NULL);"

def test_call_setze_stichtag_info(config):
    sql = call_setze_stichtag_info(config, 12345, "2026-03-30", "%Y-%m-%d")
    assert sql == "CALL `gcp-dw-test.dw_audit_logging.DWMSG_SetzeStichtagInfo`(12345, '2026-03-30', '%Y-%m-%d');"

def test_call_append_timing_infos(config):
    sql = call_append_timing_infos(config, 12345, "STEP_A", "%Y-%m-%d %H:%M:%S")
    assert sql == "CALL `gcp-dw-test.dw_audit_logging.DWMSG_AppendTimingInfos`(12345, 'STEP_A', '%Y-%m-%d %H:%M:%S');"
```

### Pass/Fail Criterion
* **Pass**: All generated SQL strings match the expected stored procedure call syntax exactly.
* **Fail**: Any generated SQL string contains syntax errors, incorrect parameter ordering, or unmapped configuration variables.