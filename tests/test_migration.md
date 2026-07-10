# Migration Validation Test Suite: `TEST.NEW_HOUSEKEEPING_JOB`

This document contains the comprehensive migration-validation test suite for the migrated logging and housekeeping utility job `TEST.NEW_HOUSEKEEPING_JOB` (originally `h_alis_meldungen.ksh`). 

The test suite is designed to prove behavioral equivalence between the legacy Oracle/KornShell implementation and the target BigQuery Stored Procedures.

---

## Test Suite Overview & Prerequisites

### Target Schema Setup
Before executing any tests, the target BigQuery tables must be initialized in the test environment. Run the following DDL to establish the baseline schema:

```sql
CREATE SCHEMA IF NOT EXISTS `project.audit_dataset`;

-- Sequence control table for ID generation
CREATE TABLE IF NOT EXISTS `project.audit_dataset.message_id_control` (
  entry_nr INT64 OPTIONS(description="Auto-incrementing sequence proxy"),
  created_at TIMESTAMP
)
AS SELECT 1000001 AS entry_nr, CURRENT_TIMESTAMP() AS created_at;

-- Main metadata table
CREATE TABLE IF NOT EXISTS `project.audit_dataset.message_table` (
  entry_nr INT64,
  job_kennung STRING,
  programmname STRING,
  log_datei STRING,
  parameter_string STRING,
  stichtag DATETIME,
  anzahl INT64,
  dateiname STRING,
  zusatzinfos STRING,
  status STRING,
  error_type INT64,
  error_text STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- Log messages detail table
CREATE TABLE IF NOT EXISTS `project.audit_dataset.message_log` (
  entry_nr INT64,
  severity STRING,
  error_type INT64,
  log_text STRING,
  created_at TIMESTAMP
);
```

---

## Section 1: Output Parity Tests

### Test Case 1.1: Unique ID Generation Parity (`CCRMSG_ErmittleNr`)
* **Purpose**: Verify that `CCRMSG_ErmittleNr` generates sequential, unique tracking IDs equivalent to the legacy Oracle sequence generator `CCR$VS_MELDUNG.Erzeuge_EintragNr()`.
* **Setup**: 
  1. Truncate `project.audit_dataset.message_id_control`.
  2. Insert a seed record with a known ID (e.g., `5000`).
* **Action**: Call the stored procedure twice and capture the output parameters.
* **Pass/Fail Criterion**: 
  * **Pass**: The first call returns `5001`, and the second call returns `5002`.
  * **Fail**: Any returned ID is null, non-sequential, or fails to increment.

```sql
-- Test Execution Script
DECLARE first_id INT64;
DECLARE second_id INT64;

TRUNCATE TABLE `project.audit_dataset.message_id_control`;
INSERT INTO `project.audit_dataset.message_id_control` (entry_nr, created_at) VALUES (5000, CURRENT_TIMESTAMP());

-- Action
CALL `project.audit_dataset.CCRMSG_ErmittleNr`(first_id);
CALL `project.audit_dataset.CCRMSG_ErmittleNr`(second_id);

-- Assertion
ASSERT first_id = 5001 MESSAGE 'First ID generation failed. Expected 5001, got ' || CAST(first_id AS STRING);
ASSERT second_id = 5002 MESSAGE 'Second ID generation failed. Expected 5002, got ' || CAST(second_id AS STRING);
```

---

### Test Case 1.2: Log File Name Construction Parity (`CCRMSG_Logdateiname`)
* **Purpose**: Verify that the generated log file path matches the legacy shell string construction pattern: `${CCR_DIR_PROT}/${v_JobKennung}_YYYYMMDD_HHMM_${v_EintragsNr}.log`.
* **Setup**: Define input variables matching legacy environment variables: `v_ProtDir = 'gs://project-bucket/logs'`, `v_JobKennung = 'JOB_TEST_01'`, and `v_EintragsNr = 99999`.
* **Action**: Call `CCRMSG_Logdateiname` and capture the output string.
* **Pass/Fail Criterion**: 
  * **Pass**: The output string matches the regex pattern `^gs://project-bucket/logs/JOB_TEST_01_\d{8}_\d{4}_99999\.log$`.
  * **Fail**: The string structure, directory separator, or timestamp format deviates from the legacy pattern.

```python
# Pytest Validation Code
import re
import pytest
from google.cloud import bigquery

def test_logdateiname_parity():
    client = bigquery.Client()
    query = """
        DECLARE out_filename STRING;
        CALL `project.audit_dataset.CCRMSG_Logdateiname`('gs://project-bucket/logs', 'JOB_TEST_01', 99999, out_filename);
        SELECT out_filename;
    """
    query_job = client.query(query)
    results = list(query_job.result())
    generated_path = results[0][0]
    
    # Regex matching: gs://project-bucket/logs/JOB_TEST_01_YYYYMMDD_HHMM_99999.log
    pattern = r"^gs://project-bucket/logs/JOB_TEST_01_\d{8}_\d{4}_99999\.log$"
    assert re.match(pattern, generated_path) is not None, f"Path mismatch: {generated_path}"
```

---

## Section 2: Transformation & Metadata Correctness

### Test Case 2.1: Initial Entry Registration (`CCRMSG_ErzeugeEintrag`)
* **Purpose**: Verify that calling `CCRMSG_ErzeugeEintrag` correctly writes a record to `message_table` with status `'INITIAL'` and maps all metadata fields correctly.
* **Setup**: Clear `project.audit_dataset.message_table`.
* **Action**: Call `CCRMSG_ErzeugeEintrag` with a set of test parameters.
* **Pass/Fail Criterion**: 
  * **Pass**: A single row is inserted with matching values, status is `'INITIAL'`, and `created_at` / `updated_at` timestamps are populated.
  * **Fail**: Row count is not 1, fields are mismatched, or timestamps are null.

```sql
-- Test Execution Script
TRUNCATE TABLE `project.audit_dataset.message_table`;

-- Action
CALL `project.audit_dataset.CCRMSG_ErzeugeEintrag`(
  12345, 
  'JOB_VAL_01', 
  '/bin/test_script.sh', 
  'gs://bucket/test.log', 
  '--param1=val1 --param2=val2'
);

-- Assertions
ASSERT (SELECT COUNT(1) FROM `project.audit_dataset.message_table`) = 1 
  MESSAGE 'Expected exactly 1 row in message_table';

ASSERT (
  SELECT AS STRUCT 
    job_kennung, programmname, log_datei, parameter_string, status
  FROM `project.audit_dataset.message_table` 
  WHERE entry_nr = 12345
) = ('JOB_VAL_01', '/bin/test_script.sh', 'gs://bucket/test.log', '--param1=val1 --param2=val2', 'INITIAL')
  MESSAGE 'Metadata field values do not match input parameters';
```

---

### Test Case 2.2: Dynamic Date Format Handling (`CCRMSG_SetzeStichtag`)
* **Purpose**: Verify that `CCRMSG_SetzeStichtag` correctly parses dynamic date formats and updates the `stichtag` field in `message_table`.
* **Setup**: Insert a baseline record with `entry_nr = 12345` into `message_table`.
* **Action**: Call `CCRMSG_SetzeStichtag` with format `'%d.%m.%Y'` and value `'31.12.2024'`.
* **Pass/Fail Criterion**: 
  * **Pass**: The `stichtag` field is updated to `2024-12-31T00:00:00`.
  * **Fail**: Parsing fails, or the date is written incorrectly.

```sql
-- Test Execution Script
-- Action
CALL `project.audit_dataset.CCRMSG_SetzeStichtag`(12345, '31.12.2024', '%d.%m.%Y');

-- Assertion
ASSERT (
  SELECT stichtag 
  FROM `project.audit_dataset.message_table` 
  WHERE entry_nr = 12345
) = DATETIME(2024, 12, 31, 0, 0, 0)
  MESSAGE 'Stichtag parsing or update failed';
```

---

### Test Case 2.3: Metadata Updates (`SetzeAnzahl`, `SetzeDateiname`, `SetzeZusatzinfos`)
* **Purpose**: Verify that optional metadata updates correctly modify their respective fields in `message_table` without altering other fields.
* **Setup**: Ensure baseline record `12345` exists in `message_table`.
* **Action**: Call `CCRMSG_SetzeAnzahl`, `CCRMSG_SetzeDateiname`, and `CCRMSG_SetzeZusatzinfos` sequentially.
* **Pass/Fail Criterion**: 
  * **Pass**: Fields `anzahl`, `dateiname`, and `zusatzinfos` are updated to the specified values.
  * **Fail**: Any of the updates fail, or they overwrite unrelated fields.

```sql
-- Test Execution Script
-- Action
CALL `project.audit_dataset.CCRMSG_SetzeAnzahl`(12345, 850300);
CALL `project.audit_dataset.CCRMSG_SetzeDateiname`(12345, 'export_data.csv');
CALL `project.audit_dataset.CCRMSG_SetzeZusatzinfos`(12345, 'Execution partition=202401');

-- Assertion
SELECT 
  ASSERT(anzahl = 850300, 'Anzahl update failed'),
  ASSERT(dateiname = 'export_data.csv', 'Dateiname update failed'),
  ASSERT(zusatzinfos = 'Execution partition=202401', 'Zusatzinfos update failed')
FROM `project.audit_dataset.message_table`
WHERE entry_nr = 12345;
```

---

## Section 3: Error Handling & Null Validation

### Test Case 3.1: Null Parameter Assertions
* **Purpose**: Verify that all procedures enforce parameter presence and raise explicit errors when required parameters are passed as `NULL` (equivalent to legacy shell `exit 2` behavior).
* **Setup**: None.
* **Action**: Call procedures with `NULL` values for mandatory fields.
* **Pass/Fail Criterion**: 
  * **Pass**: Every call raises an exception containing the expected error message.
  * **Fail**: A procedure executes successfully or raises a generic database error instead of the custom assertion message.

```python
# Pytest Validation Code
def test_null_parameter_assertions():
    client = bigquery.Client()
    
    # Test CCRMSG_ErzeugeEintrag with NULL Entry Number
    with pytest.raises(Exception) as excinfo:
        client.query("CALL `project.audit_dataset.CCRMSG_ErzeugeEintrag`(NULL, 'JOB', 'Prog', 'log', 'param')").result()
    assert "Keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben" in str(excinfo.value)

    # Test CCRMSG_SetzeStichtag with NULL Stichtag
    with pytest.raises(Exception) as excinfo:
        client.query("CALL `project.audit_dataset.CCRMSG_SetzeStichtag`(12345, NULL, '%Y-%m-%d')").result()
    assert "Stichtag nicht gesetzt" in str(excinfo.value)

    # Test CCRMSG_LogDebug with NULL Text
    with pytest.raises(Exception) as excinfo:
        client.query("CALL `project.audit_dataset.CCRMSG_LogDebug`(12345, NULL)").result()
    assert "Text nicht gesetzt" in str(excinfo.value)
```

---

### Test Case 3.2: Error State Logging & Status Transition (`CCRMSG_Fehler`)
* **Purpose**: Verify that calling `CCRMSG_Fehler` writes an `'ERROR'` record to `message_log` and transitions the job status in `message_table` to `'FAILED'`.
* **Setup**: Ensure baseline record `12345` exists in `message_table` with status `'INITIAL'`.
* **Action**: Call `CCRMSG_Fehler` with error type `3` and message `'Database connection timeout'`.
* **Pass/Fail Criterion**: 
  * **Pass**: 
    1. `message_table` status is updated to `'FAILED'`.
    2. `message_table` error fields are populated.
    3. An entry is written to `message_log` with severity `'ERROR'`.
  * **Fail**: Status remains unchanged, or log entry is missing.

```sql
-- Test Execution Script
TRUNCATE TABLE `project.audit_dataset.message_log`;

-- Action
CALL `project.audit_dataset.CCRMSG_Fehler`(12345, 3, 'Database connection timeout');

-- Assertions
ASSERT (
  SELECT AS STRUCT status, error_type, error_text 
  FROM `project.audit_dataset.message_table` 
  WHERE entry_nr = 12345
) = ('FAILED', 3, 'Database connection timeout')
  MESSAGE 'Job status transition to FAILED failed';

ASSERT (
  SELECT COUNT(1) 
  FROM `project.audit_dataset.message_log` 
  WHERE entry_nr = 12345 AND severity = 'ERROR' AND error_type = 3 AND log_text = 'Database connection timeout'
) = 1
  MESSAGE 'Error log entry not written correctly';
```

---

### Test Case 3.3: Success State Transition (`CCRMSG_Fertig`)
* **Purpose**: Verify that calling `CCRMSG_Fertig` transitions the job status in `message_table` to `'SUCCESS'`.
* **Setup**: Reset baseline record `12345` in `message_table` to status `'INITIAL'`.
* **Action**: Call `CCRMSG_Fertig` for entry `12345`.
* **Pass/Fail Criterion**: 
  * **Pass**: `message_table` status is updated to `'SUCCESS'`.
  * **Fail**: Status is not updated, or update fails.

```sql
-- Test Execution Script
UPDATE `project.audit_dataset.message_table` 
SET status = 'INITIAL' 
WHERE entry_nr = 12345;

-- Action
CALL `project.audit_dataset.CCRMSG_Fertig`(12345);

-- Assertion
ASSERT (
  SELECT status 
  FROM `project.audit_dataset.message_table` 
  WHERE entry_nr = 12345
) = 'SUCCESS'
  MESSAGE 'Job status transition to SUCCESS failed';
```

---

## Section 4: Concurrency & Stress Testing

### Test Case 4.1: High-Concurrency ID Generation
* **Purpose**: Verify that the sequence control table and `CCRMSG_ErmittleNr` procedure can handle concurrent requests without generating duplicate IDs.
* **Setup**: Truncate `project.audit_dataset.message_id_control` and seed with `10000`.
* **Action**: Execute 50 concurrent calls to `CCRMSG_ErmittleNr` using a multi-threaded Python script.
* **Pass/Fail Criterion**: 
  * **Pass**: 50 unique, sequential IDs are generated between `10001` and `10050`.
  * **Fail**: Any duplicate IDs are generated, or database locking errors occur.

```python
# Pytest Concurrency Validation Code
import concurrent.futures
from google.cloud import bigquery

def call_get_id():
    client = bigquery.Client()
    query = """
        DECLARE out_id INT64;
        CALL `project.audit_dataset.CCRMSG_ErmittleNr`(out_id);
        SELECT out_id;
    """
    query_job = client.query(query)
    results = list(query_job.result())
    return results[0][0]

def test_concurrent_id_generation():
    client = bigquery.Client()
    # Reset seed
    client.query("TRUNCATE TABLE `project.audit_dataset.message_id_control`").result()
    client.query("INSERT INTO `project.audit_dataset.message_id_control` (entry_nr, created_at) VALUES (10000, CURRENT_TIMESTAMP())").result()

    # Execute 50 concurrent requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(call_get_id) for _ in range(50)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    # Assertions
    assert len(results) == 50, "Expected 50 generated IDs"
    assert len(set(results)) == 50, f"Duplicate IDs detected: {results}"
    assert min(results) == 10001, f"Expected minimum ID 10001, got {min(results)}"
    assert max(results) == 10050, f"Expected maximum ID 10050, got {max(results)}"
```