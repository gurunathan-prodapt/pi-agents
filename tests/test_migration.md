# Migration Validation Test Suite: `k_exis_parallel.ksh` to `procedures/k_exis_parallel.sql`

This document contains the migration-validation test suite designed to verify that the migrated BigQuery Stored Procedure `dwh_operations.k_exis_parallel` is behaviorally equivalent to the legacy KornShell script `k_exis_parallel.ksh`.

---

## Test Suite Overview & Architecture

The validation strategy focuses on proving parity across four core areas:
1. **Output Parity:** Verifying that the data exported to Cloud Storage (GCS) matches the legacy local file exports exactly (row counts, schema, and content).
2. **Transformation Correctness:** Validating the mathematical partitioning logic (`MOD(ABS(FARM_FINGERPRINT(...)))`) to ensure it covers 100% of the source dataset without overlaps or omissions.
3. **External-System Replacements:** Confirming that the BigQuery `EXPORT DATA` statements and logging tables replace the legacy Oracle SQL*Plus and local disk assembly (`cat`) operations correctly.
4. **Error Isolation & Resilience:** Ensuring that failures in individual threads are captured, logged, and propagated to the orchestration layer without leaving orphaned resources.

### Test Environment Setup
The tests use a mock source table `dwh_operations.source_table` defined as follows:

```sql
CREATE OR REPLACE TABLE `dwh_operations.source_table` (
  routing_id INT64,
  payload STRING,
  processing_timestamp TIMESTAMP
);
```

---

## Section 1: Output Parity & Data Completeness

### Purpose
Verify that running the migrated BigQuery Stored Procedure with $N$ threads produces the exact same total set of records as the legacy script, with no duplicate or missing rows.

### Setup
1. Populate the source table with 10,000 rows containing sequential `routing_id` values from `1` to `10000`.
2. Set the processing window to cover all records.
3. Define a target GCS bucket path: `gs://ccr-export-data-bucket/test_output/parity_run`.

```sql
-- Populate test data
INSERT INTO `dwh_operations.source_table` (routing_id, payload, processing_timestamp)
SELECT 
  val, 
  CONCAT('payload_data_', CAST(val AS STRING)), 
  TIMESTAMP('2023-10-27 12:00:00')
FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS val;
```

### Action
Execute the migrated Stored Procedure with 4 threads:

```sql
CALL `dwh_operations.k_exis_parallel`(
  'E-10001', 
  'JOB_PARITY_TEST', 
  'dwh_operations.source_table', 
  'routing_id', 
  'gs://ccr-export-data-bucket/test_output/parity_run', 
  '20231027110000', 
  '20231027130000', 
  1, 
  4
);
```

### Pass/Fail Criterion
* **Pass:** 
  * The total row count of all exported CSV files combined in `gs://ccr-export-data-bucket/test_output/parity_run_*` is exactly 10,000 (excluding header rows).
  * A query on the exported GCS files (via a BigQuery external table) shows zero duplicate `routing_id` values and zero missing values in the range `[1, 10000]`.
* **Fail:** The combined row count is not 10,000, or duplicate/missing keys are detected.

---

## Section 2: Mathematical Partitioning & Thread Distribution

### Purpose
Prove that the dynamic SQL routing logic (`MOD(ABS(FARM_FINGERPRINT(CAST(routing_col AS STRING))), total_threads)`) partitions the source dataset deterministically and mutually exclusively across all threads.

### Setup
Use the same 10,000-row dataset in `dwh_operations.source_table`.

### Action
Run a validation query that simulates the Stored Procedure's internal routing logic for 4 threads and checks for overlap or unassigned rows.

```sql
WITH partitioned_data AS (
  SELECT 
    routing_id,
    MOD(ABS(FARM_FINGERPRINT(CAST(routing_id AS STRING))), 4) AS assigned_thread
  FROM 
    `dwh_operations.source_table`
  WHERE 
    processing_timestamp >= TIMESTAMP('2023-10-27 11:00:00')
    AND processing_timestamp <= TIMESTAMP('2023-10-27 13:00:00')
)
SELECT
  assigned_thread,
  COUNT(1) AS row_count,
  COUNT(DISTINCT routing_id) AS unique_keys
FROM 
  partitioned_data
GROUP BY 
  assigned_thread;
```

### Pass/Fail Criterion
* **Pass:**
  * The sum of `row_count` across all returned groups (threads `0, 1, 2, 3`) is exactly 10,000.
  * For every group, `row_count` is equal to `unique_keys` (proving no duplicate routing within a thread).
  * No thread group has 0 rows (proving even distribution).
* **Fail:** The sum of rows does not equal 10,000, or any key is routed to multiple threads.

---

## Section 3: Logging & Metadata Audit Trail

### Purpose
Verify that the logging mechanism (`dwh_operations.write_ccr_log`) correctly records execution states, thread initializations, and completion statuses in the persistent `dwh_operations.ccr_logs` table.

### Setup
Clear the log table for the test run identifier.

```sql
DELETE FROM `dwh_operations.ccr_logs` WHERE job_kennung = 'JOB_LOG_TEST';
```

### Action
Execute the Stored Procedure with `p_Debug = 1`:

```sql
CALL `dwh_operations.k_exis_parallel`(
  'E-20002', 
  'JOB_LOG_TEST', 
  'dwh_operations.source_table', 
  'routing_id', 
  'gs://ccr-export-data-bucket/test_output/log_run', 
  '20231027110000', 
  '20231027130000', 
  1, 
  3
);
```

### Pass/Fail Criterion
* **Pass:** The following assertions on `dwh_operations.ccr_logs` return `TRUE`:
  1. At least one log entry exists with `log_level = 'DEBUG'` containing `'Initiating execution loop'`.
  2. Exactly 3 log entries exist containing `'Spawning execution thread block'`.
  3. A final log entry exists with `log_level = 'INFO'` containing `'Execution finished successfully'`.
* **Fail:** Any of the expected log entries are missing, or timestamps are out of sequence.

```sql
-- Verification Query
SELECT 
  COUNTIF(message LIKE 'Initiating execution loop%') = 1 AS has_start_log,
  COUNTIF(message LIKE 'Spawning execution thread block%') = 3 AS has_all_thread_logs,
  COUNTIF(message LIKE 'Execution finished successfully%') = 1 AS has_end_log
FROM 
  `dwh_operations.ccr_logs`
WHERE 
  job_kennung = 'JOB_LOG_TEST';
```

---

## Section 4: Error Isolation & Transactional Resilience

### Purpose
Verify that if one parallel thread fails (e.g., due to an invalid routing column or schema mismatch), the Stored Procedure:
1. Captures the error in the temporary tracking table.
2. Continues executing the remaining threads to prevent partial-run hangs.
3. Logs the specific failure to `dwh_operations.ccr_logs`.
4. Propagates a database exception back to the orchestrator at the end of the run.

### Setup
Create a scenario where a thread will fail by passing an invalid routing column name (`non_existent_column`).

### Action
Execute the Stored Procedure within a test block to catch the expected final exception:

```sql
DECLARE execution_failed BOOL DEFAULT FALSE;

BEGIN
  CALL `dwh_operations.k_exis_parallel`(
    'E-30003', 
    'JOB_FAIL_TEST', 
    'dwh_operations.source_table', 
    'non_existent_column', -- This will trigger an EXECUTE IMMEDIATE failure
    'gs://ccr-export-data-bucket/test_output/fail_run', 
    '20231027110000', 
    '20231027130000', 
    1, 
    2
  );
EXCEPTION WHEN ERROR THEN
  SET execution_failed = TRUE;
END;

-- Assert that the procedure raised an error
SELECT execution_failed AS test_passed;
```

### Pass/Fail Criterion
* **Pass:**
  * `execution_failed` is evaluated as `TRUE`.
  * The `dwh_operations.ccr_logs` table contains log entries with `log_level = 'ERROR'` detailing the failure of the threads.
  * The temporary table `temp_fehler_log` is successfully dropped and does not leak into the session.
* **Fail:** The procedure exits with a success code (0) despite the internal thread failure, or fails to log the error details.

```sql
-- Verification Query for Error Logs
SELECT 
  COUNT(1) >= 1 AS error_logged,
  ANY_VALUE(message) LIKE '%encountered failure%' AS correct_error_msg
FROM 
  `dwh_operations.ccr_logs`
WHERE 
  job_kennung = 'JOB_FAIL_TEST' 
  AND log_level = 'ERROR';
```

---

## Section 5: Automated Integration Test (Pytest)

The following Python test script automates the validation of the migrated Stored Procedure against a Google Cloud BigQuery environment.

```python
import pytest
from google.cloud import bigquery
from google.cloud import storage
import re

PROJECT_ID = "your-gcp-project"
DATASET_ID = "dwh_operations"
BUCKET_NAME = "ccr-export-data-bucket"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def gcs_client():
    return storage.Client(project=PROJECT_ID)

def test_k_exis_parallel_success(bq_client, gcs_client):
    # 1. Clean up previous GCS exports
    bucket = gcs_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="test_output/pytest_run")
    for blob in blobs:
        blob.delete()

    # 2. Execute the Stored Procedure
    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.k_exis_parallel`(
          'E-99999', 
          'PYTEST_JOB', 
          '{PROJECT_ID}.{DATASET_ID}.source_table', 
          'routing_id', 
          'gs://{BUCKET_NAME}/test_output/pytest_run', 
          '20231027110000', 
          '20231027130000', 
          1, 
          2
        );
    """
    query_job = bq_client.query(query)
    query_job.result()  # Wait for execution to complete

    # 3. Assert GCS files were created
    blobs = list(bucket.list_blobs(prefix="test_output/pytest_run"))
    assert len(blobs) > 0, "No export files found in GCS."

    # 4. Verify GCS file naming convention matches thread structure
    thread_0_pattern = re.compile(r"test_output/pytest_run_thread_0_.*\.csv")
    thread_1_pattern = re.compile(r"test_output/pytest_run_thread_1_.*\.csv")
    
    has_thread_0 = any(thread_0_pattern.match(b.name) for b in blobs)
    has_thread_1 = any(thread_1_pattern.match(b.name) for b in blobs)
    
    assert has_thread_0 and has_thread_1, "Thread-specific segmented files are missing."
```