# Migration Validation Test Suite: `d_ipis_loader.ksh`

This document contains the migration-validation test suite designed to verify that the migrated BigQuery stored procedure `d_ipis_loader` behaves identically to the legacy Oracle SQL\*Loader wrapper script `d_ipis_loader.ksh`.

---

## Test Suite Overview & Prerequisites

To execute these tests, the following environment variables and resources must be configured:
* **GCP Project ID**: `your_project` (or target environment project)
* **BigQuery Dataset**: `your_dataset`
* **GCS Test Bucket**: `gs://your-test-bucket`
* **Python Environment**: `pytest` with `google-cloud-bigquery` and `google-cloud-storage` installed.

### Target Tables Setup
Before running the test suite, ensure the target tables exist in your test dataset:

```sql
CREATE OR REPLACE TABLE `your_project.your_dataset.t_customer` (
  customer_id INT64,
  customer_name STRING,
  country STRING,
  updated_at TIMESTAMP
);

CREATE OR REPLACE TABLE `your_project.your_dataset.t_orders` (
  order_id INT64,
  customer_id INT64,
  amount NUMERIC,
  order_date DATE
);
```

---

## Section 1: Parameter Validation Tests

### Test Case 1.1: Missing Control File Parameter (`-c` equivalent)
* **Purpose**: Verify that calling the stored procedure with an empty or `NULL` control file name returns `p_err_nr = 1` and logs an error with level `'E'`, matching the legacy parameter validation logic.
* **Setup**: Ensure the audit tables `dw_execution_log` and `dw_error_log` are truncated.
* **Action**: Execute the stored procedure with `p_control_file_name = NULL` or `''`.
* **Pass/Fail Criterion**: 
  * `p_err_nr` must return `1`.
  * A record must be written to `dw_error_log` with `level = 'E'`, `error_code = 1`, and a message containing `"Validation failed: CONTROLFILE"`.
  * No load job should be executed.

#### Test Code (Python / pytest)
```python
import pytest
from google.cloud import bigquery

def test_missing_control_file():
    client = bigquery.Client()
    
    # Truncate error log
    client.query("TRUNCATE TABLE `your_project.your_dataset.dw_error_log`").result()
    
    # Call procedure
    query = """
        DECLARE out_err INT64;
        CALL `your_project.your_dataset.d_ipis_loader`(NULL, 'gs://your-test-bucket/data.csv', 'DE', out_err);
        SELECT out_err;
    """
    query_job = client.query(query)
    results = list(query_job.result())
    out_err = results[0][0]
    
    # Assert output parameter
    assert out_err == 1, f"Expected error code 1, got {out_err}"
    
    # Assert error log entry
    log_query = "SELECT level, error_code, message FROM `your_project.your_dataset.dw_error_log`"
    log_rows = list(client.query(log_query).result())
    
    assert len(log_rows) == 1, "Expected exactly 1 error log entry"
    assert log_rows[0]['level'] == 'E'
    assert log_rows[0]['error_code'] == 1
    assert "CONTROLFILE" in log_rows[0]['message']
```

---

### Test Case 1.2: Missing Data File Parameter (`-d` equivalent)
* **Purpose**: Verify that calling the stored procedure with an empty or `NULL` data file URI returns `p_err_nr = 1` and logs an error with level `'E'`.
* **Setup**: Ensure `dw_error_log` is truncated.
* **Action**: Execute the stored procedure with `p_data_file_uri = NULL` or `''`.
* **Pass/Fail Criterion**:
  * `p_err_nr` must return `1`.
  * A record must be written to `dw_error_log` with `level = 'E'`, `error_code = 1`, and a message containing `"Validation failed: DATAFILE URI"`.

#### Test Code (SQL Assertion)
```sql
-- Setup
TRUNCATE TABLE `your_project.your_dataset.dw_error_log`;

-- Action
DECLARE out_err INT64;
CALL `your_project.your_dataset.d_ipis_loader`('customer_import.ctl', NULL, 'DE', out_err);

-- Assertions
ASSERT out_err = 1 AS 'Error: Expected return code 1 for missing data file';

ASSERT (
  SELECT COUNT(1) 
  FROM `your_project.your_dataset.dw_error_log` 
  WHERE level = 'E' 
    AND error_code = 1 
    AND message LIKE '%DATAFILE URI%'
) = 1 AS 'Error: Missing or incorrect error log entry for missing data file';
```

---

### Test Case 1.3: Unmapped Control File Layout
* **Purpose**: Verify that if a control file name is passed that does not match any known target table mapping pattern, the procedure returns `p_err_nr = 2` and logs a fatal error (`'F'`).
* **Setup**: Ensure `dw_error_log` is truncated.
* **Action**: Execute the stored procedure with `p_control_file_name = 'unknown_layout.ctl'`.
* **Pass/Fail Criterion**:
  * `p_err_nr` must return `2`.
  * A record must be written to `dw_error_log` with `level = 'F'`, `error_code = 2`, and a message containing `"Unknown control file layout"`.

#### Test Code (SQL Assertion)
```sql
-- Setup
TRUNCATE TABLE `your_project.your_dataset.dw_error_log`;

-- Action
DECLARE out_err INT64;
CALL `your_project.your_dataset.d_ipis_loader`('unknown_layout.ctl', 'gs://your-test-bucket/data.csv', 'DE', out_err);

-- Assertions
ASSERT out_err = 2 AS 'Error: Expected return code 2 for unmapped control file';

ASSERT (
  SELECT COUNT(1) 
  FROM `your_project.your_dataset.dw_error_log` 
  WHERE level = 'F' 
    AND error_code = 2 
    AND message LIKE '%Unknown control file layout%'
) = 1 AS 'Error: Missing or incorrect error log entry for unmapped control file';
```

---

## Section 2: Ingestion & Transformation Correctness

### Test Case 2.1: Successful CSV Data Load (Output Parity)
* **Purpose**: Verify that a valid CSV file stored on GCS is successfully loaded into the mapped target table, matching the schema and row counts.
* **Setup**: 
  1. Upload a valid CSV file to `gs://your-test-bucket/test_customer.csv` containing:
     ```csv
     customer_id;customer_name;country;updated_at
     101;ACME Corp;US;2023-10-27 10:00:00
     102;Globex Corp;CA;2023-10-27 11:00:00
     ```
  2. Truncate target table `t_customer` and execution log tables.
* **Action**: Call the stored procedure with `p_control_file_name = 'customer_load.ctl'` and `p_data_file_uri = 'gs://your-test-bucket/test_customer.csv'`.
* **Pass/Fail Criterion**:
  * `p_err_nr` must return `0`.
  * Target table `t_customer` must contain exactly 2 rows with correct values.
  * `dw_execution_log` must contain a success entry.

#### Test Code (Python / pytest)
```python
import pytest
from google.cloud import bigquery
from google.cloud import storage

def test_successful_csv_load():
    bq_client = bigquery.Client()
    gcs_client = storage.Client()
    
    # 1. Upload test file to GCS
    bucket = gcs_client.bucket("your-test-bucket")
    blob = bucket.blob("test_customer.csv")
    csv_data = "customer_id;customer_name;country;updated_at\n101;ACME Corp;US;2023-10-27 10:00:00\n102;Globex Corp;CA;2023-10-27 11:00:00\n"
    blob.upload_from_string(csv_data, content_type="text/csv")
    
    # 2. Clean up target and log tables
    bq_client.query("TRUNCATE TABLE `your_project.your_dataset.t_customer`").result()
    bq_client.query("TRUNCATE TABLE `your_project.your_dataset.dw_execution_log`").result()
    
    # 3. Execute Procedure
    query = """
        DECLARE out_err INT64;
        CALL `your_project.your_dataset.d_ipis_loader`('customer_load.ctl', 'gs://your-test-bucket/test_customer.csv', NULL, out_err);
        SELECT out_err;
    """
    out_err = list(bq_client.query(query).result())[0][0]
    
    # 4. Assertions
    assert out_err == 0, f"Expected return code 0, got {out_err}"
    
    # Verify row count and data integrity
    data_query = "SELECT * FROM `your_project.your_dataset.t_customer` ORDER BY customer_id"
    rows = list(bq_client.query(data_query).result())
    
    assert len(rows) == 2, f"Expected 2 rows loaded, found {len(rows)}"
    assert rows[0]['customer_id'] == 101
    assert rows[0]['customer_name'] == "ACME Corp"
    assert rows[0]['country'] == "US"
    
    # Verify execution log
    log_query = "SELECT message FROM `your_project.your_dataset.dw_execution_log` WHERE message LIKE '%SUCCESS%'"
    logs = list(bq_client.query(log_query).result())
    assert len(logs) == 1, "Success log entry not found"
```

---

## Section 3: Error Handling & Transactional Integrity

### Test Case 3.1: Bad Record Handling (Equivalent to `.bad` / `.dis` File Generation)
* **Purpose**: Verify that if the input file contains malformed rows (e.g., type mismatch), the transaction rolls back, no rows are loaded, and the procedure returns `p_err_nr = 200` (replicating the legacy SQL\*Loader bad/discard file detection).
* **Setup**:
  1. Upload a CSV file to `gs://your-test-bucket/bad_customer.csv` containing a string in the integer `customer_id` column:
     ```csv
     customer_id;customer_name;country;updated_at
     NOT_AN_INT;ACME Corp;US;2023-10-27 10:00:00
     ```
  2. Truncate target table `t_customer` and error log tables.
* **Action**: Call the stored procedure with `p_control_file_name = 'customer_load.ctl'` and `p_data_file_uri = 'gs://your-test-bucket/bad_customer.csv'`.
* **Pass/Fail Criterion**:
  * `p_err_nr` must return `200`.
  * Target table `t_customer` must remain completely empty (transaction rollback).
  * `dw_error_log` must contain a record with `level = 'F'`, `error_code = 200`, and the detailed BigQuery system error message.

#### Test Code (Python / pytest)
```python
def test_bad_record_rollback_and_logging():
    bq_client = bigquery.Client()
    gcs_client = storage.Client()
    
    # 1. Upload malformed CSV to GCS
    bucket = gcs_client.bucket("your-test-bucket")
    blob = bucket.blob("bad_customer.csv")
    csv_data = "customer_id;customer_name;country;updated_at\nNOT_AN_INT;ACME Corp;US;2023-10-27 10:00:00\n"
    blob.upload_from_string(csv_data, content_type="text/csv")
    
    # 2. Clean up tables
    bq_client.query("TRUNCATE TABLE `your_project.your_dataset.t_customer`").result()
    bq_client.query("TRUNCATE TABLE `your_project.your_dataset.dw_error_log`").result()
    
    # 3. Execute Procedure
    query = """
        DECLARE out_err INT64;
        CALL `your_project.your_dataset.d_ipis_loader`('customer_load.ctl', 'gs://your-test-bucket/bad_customer.csv', NULL, out_err);
        SELECT out_err;
    """
    out_err = list(bq_client.query(query).result())[0][0]
    
    # 4. Assertions
    assert out_err == 200, f"Expected return code 200, got {out_err}"
    
    # Verify target table is empty (Rollback verification)
    count_query = "SELECT COUNT(1) as cnt FROM `your_project.your_dataset.t_customer`"
    count = list(bq_client.query(count_query).result())[0]['cnt']
    assert count == 0, f"Transaction failed to rollback! Target table contains {count} rows."
    
    # Verify error log entry
    log_query = "SELECT level, error_code, message, error_statement FROM `your_project.your_dataset.dw_error_log`"
    log_rows = list(bq_client.query(log_query).result())
    
    assert len(log_rows) == 1, "Expected exactly 1 error log entry"
    assert log_rows[0]['level'] == 'F'
    assert log_rows[0]['error_code'] == 200
    assert "Could not parse" in log_rows[0]['message'] or "Error Details" in log_rows[0]['message']
    assert "LOAD DATA OVERWRITE" in log_rows[0]['error_statement']
```

---

### Test Case 3.2: Missing Input File Handling
* **Purpose**: Verify that if the specified GCS URI does not exist, the procedure fails gracefully, rolls back, returns `p_err_nr = 200`, and logs the error.
* **Setup**: Ensure the target GCS URI `gs://your-test-bucket/non_existent_file.csv` does not exist. Truncate target and log tables.
* **Action**: Call the stored procedure with `p_data_file_uri = 'gs://your-test-bucket/non_existent_file.csv'`.
* **Pass/Fail Criterion**:
  * `p_err_nr` must return `200`.
  * `dw_error_log` must contain a record with `level = 'F'`, `error_code = 200`, and a message indicating that the file or URI was not found.

#### Test Code (SQL Assertion)
```sql
-- Setup
TRUNCATE TABLE `your_project.your_dataset.t_orders`;
TRUNCATE TABLE `your_project.your_dataset.dw_error_log`;

-- Action
DECLARE out_err INT64;
CALL `your_project.your_dataset.d_ipis_loader`('orders_load.ctl', 'gs://your-test-bucket/non_existent_file.csv', NULL, out_err);

-- Assertions
ASSERT out_err = 200 AS 'Error: Expected return code 200 for missing GCS file';

ASSERT (
  SELECT COUNT(1) 
  FROM `your_project.your_dataset.dw_error_log` 
  WHERE level = 'F' 
    AND error_code = 200 
    AND (message LIKE '%Not found%' OR message LIKE '%Error Details%')
) = 1 AS 'Error: Missing or incorrect error log entry for missing GCS file';
```