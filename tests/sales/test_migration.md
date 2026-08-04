# Migration Validation Test Suite: SALES.PRODUCT_AND_SALES_EXTRACT

This test suite contains migration-validation tests to prove that the migrated Apache Airflow, Python, and Google Cloud BigQuery components for the `SALES.PRODUCT_AND_SALES_EXTRACT` job are behaviorally equivalent to the legacy UC4 and KornShell (KSH) implementation.

---

## Test Case 1: GCS Sentinel File Polling & Timeout (External System Replacement)

### Purpose
Verify that the GCS file-polling mechanism in `k_product_and_sales_extract.py` correctly replaces the legacy local filesystem check (`[ -f SOURCE_MARKER ]`). It must successfully detect the sentinel file when present, wait/retry when temporarily absent, and fail with exit code `1` if the file does not appear within the maximum retry limit.

### Setup
1. A mock or test GCS bucket (`test-sales-bucket`).
2. Environment variables set:
   * `RUN_DATE="2026-01-05"`
   * `GCS_BUCKET="test-sales-bucket"`
   * `MAX_WAIT_CHECKS="3"`
   * `WAIT_INTERVAL_SECONDS="1"`
3. A test runner environment with `google-cloud-storage` installed.

### Action
Execute the polling logic under three distinct scenarios:
1. **Immediate Success**: The sentinel file `inbound/pos_feed_2026-01-05.done` exists in the bucket prior to execution.
2. **Delayed Success**: The sentinel file is missing initially but is uploaded to GCS during the polling loop (e.g., on check 2).
3. **Timeout Failure**: The sentinel file does not exist at all during the execution.

### Pass/Fail Criterion
* **Pass**: 
  * Scenario 1 exits immediately with success.
  * Scenario 2 waits, detects the file on check 2, and proceeds.
  * Scenario 3 retries exactly 3 times, logs the abort error, and exits with code `1`.
* **Fail**: Any scenario exits with an incorrect code, fails to retry, or fails to detect the GCS object.

### Test Code (Pytest)

```python
import os
import time
import pytest
from unittest.mock import MagicMock, patch
import sys

# Import the main module under test
sys.path.append(os.path.abspath("./sales"))
import k_product_and_sales_extract

@pytest.fixture
def setup_env(monkeypatch):
    monkeypatch.setenv("RUN_DATE", "2026-01-05")
    monkeypatch.setenv("GCS_BUCKET", "test-sales-bucket")
    monkeypatch.setenv("MAX_WAIT_CHECKS", "3")
    monkeypatch.setenv("WAIT_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("RETAIL_HOME", "/tmp/sales")

@patch("google.cloud.storage.Client")
def test_gcs_polling_immediate_success(mock_storage_client, setup_env):
    # Mock GCS blob.exists() to return True immediately
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_bucket.blob.return_value = mock_blob
    mock_storage_client.return_value.bucket.return_value = mock_bucket

    exists = k_product_and_sales_extract.gcs_file_exists("test-sales-bucket", "inbound/pos_feed_2026-01-05.done")
    assert exists is True

@patch("google.cloud.storage.Client")
@patch("time.sleep", return_value=None)  # Fast-forward sleeps
def test_gcs_polling_timeout_failure(mock_sleep, mock_storage_client, setup_env, capsys):
    # Mock GCS blob.exists() to always return False
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket.blob.return_value = mock_blob
    mock_storage_client.return_value.bucket.return_value = mock_bucket

    with pytest.raises(SystemExit) as excinfo:
        k_product_and_sales_extract.main()
    
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "ERROR: source feed marker never appeared" in captured.out
    assert mock_sleep.call_count == 2  # Retried 3 times (sleeps between check 1-2, 2-3)
```

---

## Test Case 2: Daily Sales Extract Idempotency & Transformation (Output Parity)

### Purpose
Verify that `d_daily_sales_extract.sql` correctly performs an idempotent load. It must purge existing records in `STG_DAILY_SALES` for the target date, join `SRC_POS_TRANSACTIONS` with `DIM_STORE` to resolve the `REGION_CODE`, and insert the consolidated records.

### Setup
1. A BigQuery emulator or test dataset (`test_analytics_dataset`).
2. Populate `DIM_STORE` with reference stores:
   * `STORE_ID = 100`, `REGION_CODE = 'US-EAST'`
   * `STORE_ID = 200`, `REGION_CODE = 'US-WEST'`
3. Populate `SRC_POS_TRANSACTIONS` with source transactions for `2026-01-05`:
   * `SALE_ID = 1`, `SALE_DATE = '2026-01-05'`, `STORE_ID = 100`, `SALE_AMOUNT = 150.00`
   * `SALE_ID = 2`, `SALE_DATE = '2026-01-05'`, `STORE_ID = 200`, `SALE_AMOUNT = 250.50`
4. Populate `STG_DAILY_SALES` with a pre-existing stale record for `2026-01-05` to test the purge:
   * `SALE_ID = 999`, `SALE_DATE = '2026-01-05'`, `STORE_ID = 100`, `REGION_CODE = 'OLD'`, `SALE_AMOUNT = 99.99`

### Action
Execute `d_daily_sales_extract.sql` using the BigQuery client, passing `@input_date = '2026-01-05'`.

### Pass/Fail Criterion
* **Pass**:
  * The stale record (`SALE_ID = 999`) is deleted.
  * Exactly 2 new records are inserted into `STG_DAILY_SALES` for `2026-01-05`.
  * `REGION_CODE` is correctly resolved to `'US-EAST'` and `'US-WEST'` respectively.
  * Financial precision of `SALE_AMOUNT` is preserved exactly (no floating-point rounding errors).
* **Fail**: Stale records remain, region codes are null or incorrect, or transaction counts do not match.

### Test Code (Pytest + BigQuery Client)

```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_daily_sales_extract_transformation(bq_client):
    project = os.environ.get("GCP_PROJECT")
    dataset = os.environ.get("BQ_DATASET")
    target_date = "2026-01-05"

    # 1. Clean up and seed test tables
    bq_client.query(f"DELETE FROM `{project}.{dataset}.STG_DAILY_SALES` WHERE TRUE").result()
    bq_client.query(f"DELETE FROM `{project}.{dataset}.SRC_POS_TRANSACTIONS` WHERE TRUE").result()
    bq_client.query(f"DELETE FROM `{project}.{dataset}.DIM_STORE` WHERE TRUE").result()

    # Seed DIM_STORE
    bq_client.query(f"""
        INSERT INTO `{project}.{dataset}.DIM_STORE` (STORE_ID, REGION_CODE) VALUES
        (100, 'US-EAST'),
        (200, 'US-WEST')
    """).result()

    # Seed SRC_POS_TRANSACTIONS
    bq_client.query(f"""
        INSERT INTO `{project}.{dataset}.SRC_POS_TRANSACTIONS` (SALE_ID, SALE_DATE, PRODUCT_ID, CUSTOMER_ID, STORE_ID, SALE_AMOUNT) VALUES
        (1, '2026-01-05', 10, 500, 100, 150.00),
        (2, '2026-01-05', 11, 501, 200, 250.50)
    """).result()

    # Seed stale record in target to verify DELETE step
    bq_client.query(f"""
        INSERT INTO `{project}.{dataset}.STG_DAILY_SALES` (SALE_ID, SALE_DATE, PRODUCT_ID, CUSTOMER_ID, STORE_ID, REGION_CODE, SALE_AMOUNT) VALUES
        (999, '2026-01-05', 99, 999, 100, 'OLD', 99.99)
    """).result()

    # 2. Read and execute the migrated SQL script
    with open("sales/d_daily_sales_extract.sql", "r") as f:
        sql_script = f.read()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("input_date", "STRING", target_date),
            bigquery.ScalarQueryParameter("gcp_project", "STRING", project),
            bigquery.ScalarQueryParameter("bq_dataset", "STRING", dataset)
        ]
    )
    
    query_job = bq_client.query(sql_script, job_config=job_config)
    query_job.result()  # Wait for execution

    # 3. Assertions
    results_query = f"""
        SELECT SALE_ID, REGION_CODE, SALE_AMOUNT 
        FROM `{project}.{dataset}.STG_DAILY_SALES` 
        ORDER BY SALE_ID
    """
    rows = list(bq_client.query(results_query).result())

    assert len(rows) == 2, "Stale record was not purged or inserts failed."
    
    # Record 1 assertions
    assert rows[0]["SALE_ID"] == 1
    assert rows[0]["REGION_CODE"] == "US-EAST"
    assert float(rows[0]["SALE_AMOUNT"]) == 150.00

    # Record 2 assertions
    assert rows[1]["SALE_ID"] == 2
    assert rows[1]["REGION_CODE"] == "US-WEST"
    assert float(rows[1]["SALE_AMOUNT"]) == 250.50
```

---

## Test Case 3: Product Master SCD Type 2 Dimension Load (Transformation Correctness)

### Purpose
Verify that `d_product_master_load.sql` correctly implements Slowly Changing Dimension (SCD) Type 2 logic. It must:
1. Insert entirely new products as active (`IS_CURRENT = 1`, `VALID_FROM = current_datetime_val`, `VALID_TO IS NULL`).
2. Expire changed products (`IS_CURRENT = 0`, `VALID_TO = current_datetime_val`).
3. Insert a new active version of changed products (`IS_CURRENT = 1`, `VALID_FROM = current_datetime_val`).
4. Leave unchanged products completely untouched.
5. Enforce temporal consistency (the expired record's `VALID_TO` must match the new active record's `VALID_FROM` exactly).

### Setup
1. A BigQuery test dataset containing `DIM_PRODUCT` and `STG_PRODUCT_MASTER`.
2. Populate `DIM_PRODUCT` with baseline records:
   * Product 10 (Unchanged): `PRODUCT_NAME = 'Widget A'`, `CATEGORY = 'Tools'`, `UNIT_PRICE = 10.00`, `IS_CURRENT = 1`, `VALID_FROM = '2025-01-01 00:00:00'`
   * Product 20 (To be changed): `PRODUCT_NAME = 'Widget B'`, `CATEGORY = 'Tools'`, `UNIT_PRICE = 20.00`, `IS_CURRENT = 1`, `VALID_FROM = '2025-01-01 00:00:00'`
3. Populate `STG_PRODUCT_MASTER` with the daily feed:
   * Product 10 (Unchanged): `PRODUCT_NAME = 'Widget A'`, `CATEGORY = 'Tools'`, `UNIT_PRICE = 10.00`
   * Product 20 (Changed price): `PRODUCT_NAME = 'Widget B'`, `CATEGORY = 'Tools'`, `UNIT_PRICE = 25.00`
   * Product 30 (New product): `PRODUCT_NAME = 'Widget C'`, `CATEGORY = 'Home'`, `UNIT_PRICE = 5.99`

### Action
Execute `d_product_master_load.sql` inside a BigQuery transaction.

### Pass/Fail Criterion
* **Pass**:
  * Product 10 remains unchanged (exactly 1 active row, `VALID_FROM` remains `'2025-01-01 00:00:00'`).
  * Product 20 has exactly 2 rows:
    * The old row is expired (`IS_CURRENT = 0`, `VALID_TO` is set to the execution timestamp).
    * The new row is active (`IS_CURRENT = 1`, `UNIT_PRICE = 25.00`, `VALID_FROM` matches the old row's `VALID_TO` exactly).
  * Product 30 is inserted as active (`IS_CURRENT = 1`, `VALID_FROM` is set to the execution timestamp, `VALID_TO IS NULL`).
* **Fail**: Any mismatch in `IS_CURRENT` flags, mismatched timestamps between expired and new active records, or failure to insert new records.

### Test Code (Pytest + BigQuery Client)

```python
import os
import pytest
from google.cloud import bigquery

def test_scd_type_2_product_master(bq_client):
    project = os.environ.get("GCP_PROJECT")
    dataset = os.environ.get("BQ_DATASET")

    # 1. Clean up and seed test tables
    bq_client.query(f"DELETE FROM `{project}.{dataset}.DIM_PRODUCT` WHERE TRUE").result()
    bq_client.query(f"DELETE FROM `{project}.{dataset}.STG_PRODUCT_MASTER` WHERE TRUE").result()

    # Seed DIM_PRODUCT (Target)
    bq_client.query(f"""
        INSERT INTO `{project}.{dataset}.DIM_PRODUCT` 
        (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, IS_CURRENT, VALID_FROM, VALID_TO) VALUES
        (10, 'Widget A', 'Tools', 10.00, 1, DATETIME('2025-01-01 00:00:00'), NULL),
        (20, 'Widget B', 'Tools', 20.00, 1, DATETIME('2025-01-01 00:00:00'), NULL)
    """).result()

    # Seed STG_PRODUCT_MASTER (Source Feed)
    bq_client.query(f"""
        INSERT INTO `{project}.{dataset}.STG_PRODUCT_MASTER` 
        (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE) VALUES
        (10, 'Widget A', 'Tools', 10.00),  -- Unchanged
        (20, 'Widget B', 'Tools', 25.00),  -- Price Changed
        (30, 'Widget C', 'Home', 5.99)     -- New Product
    """).result()

    # 2. Execute the migrated SCD2 SQL script
    with open("sales/d_product_master_load.sql", "r") as f:
        sql_script = f.read()

    # Replace schema placeholder with test dataset
    sql_script_resolved = sql_script.replace("ANALYTICS_SCHEMA", f"{project}.{dataset}")

    query_job = bq_client.query(sql_script_resolved)
    query_job.result()

    # 3. Assertions
    # Verify Product 10 (Unchanged)
    p10_rows = list(bq_client.query(f"""
        SELECT * FROM `{project}.{dataset}.DIM_PRODUCT` WHERE PRODUCT_ID = 10
    """).result())
    assert len(p10_rows) == 1
    assert p10_rows[0]["IS_CURRENT"] == 1
    assert float(p10_rows[0]["UNIT_PRICE"]) == 10.00
    assert p10_rows[0]["VALID_TO"] is None

    # Verify Product 20 (SCD2 Expired + New Active)
    p20_rows = list(bq_client.query(f"""
        SELECT * FROM `{project}.{dataset}.DIM_PRODUCT` WHERE PRODUCT_ID = 20 ORDER BY IS_CURRENT
    """).result())
    assert len(p20_rows) == 2
    
    # Old expired record
    old_p20 = p20_rows[0]
    assert old_p20["IS_CURRENT"] == 0
    assert float(old_p20["UNIT_PRICE"]) == 20.00
    assert old_p20["VALID_TO"] is not None

    # New active record
    new_p20 = p20_rows[1]
    assert new_p20["IS_CURRENT"] == 1
    assert float(new_p20["UNIT_PRICE"]) == 25.00
    assert new_p20["VALID_TO"] is None

    # Temporal Consistency Check
    assert old_p20["VALID_TO"] == new_p20["VALID_FROM"], "Temporal gap detected between expired and active record!"

    # Verify Product 30 (New Record)
    p30_rows = list(bq_client.query(f"""
        SELECT * FROM `{project}.{dataset}.DIM_PRODUCT` WHERE PRODUCT_ID = 30
    """).result())
    assert len(p30_rows) == 1
    assert p30_rows[0]["IS_CURRENT"] == 1
    assert float(p30_rows[0]["UNIT_PRICE"]) == 5.99
    assert p30_rows[0]["VALID_TO"] is None
```

---

## Test Case 4: End-to-End Execution & Logging Wrapper (Orchestration & Error Handling)

### Purpose
Verify that the Python wrapper `r_product_and_sales_extract.py` correctly mimics the legacy KornShell wrapper `r_product_and_sales_extract.ksh`. It must validate the presence of the `RUN_DATE` environment variable, create the log directory, write execution logs to both standard output and a timestamped log file, and propagate exit codes from the child process.

### Setup
1. A clean temporary directory to act as `RETAIL_HOME`.
2. Mock the execution of the child script `k_product_and_sales_extract.py` to return success (`0`) and failure (`2`).

### Action
1. Run `r_product_and_sales_extract.py` without `RUN_DATE` set.
2. Run `r_product_and_sales_extract.py` with `RUN_DATE` set and a mock successful child script.
3. Run `r_product_and_sales_extract.py` with a mock failing child script.

### Pass/Fail Criterion
* **Pass**:
  * Action 1 exits with code `1` and logs the missing variable error.
  * Action 2 creates a log file in `${RETAIL_HOME}/logs/product_and_sales_extract_*.log` containing the start and success messages, and exits with code `0`.
  * Action 3 captures the non-zero exit code from the child script, logs the failure, and exits with the exact same non-zero code.
* **Fail**: Log files are not created, exit codes are swallowed, or standard output does not match the legacy format.

### Test Code (Pytest)

```python
import os
import sys
import shutil
import pytest
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath("./sales"))
import r_product_and_sales_extract

@pytest.fixture
def temp_retail_home(tmp_path):
    retail_home = tmp_path / "retail_home"
    retail_home.mkdir()
    return str(retail_home)

def test_wrapper_missing_run_date(temp_retail_home, monkeypatch):
    monkeypatch.setenv("RETAIL_HOME", temp_retail_home)
    monkeypatch.delenv("RUN_DATE", raising=False)

    with pytest.raises(SystemExit) as excinfo:
        r_product_and_sales_extract.main()
    assert excinfo.value.code == 1

@patch("subprocess.run")
def test_wrapper_success_flow(mock_run, temp_retail_home, monkeypatch):
    monkeypatch.setenv("RETAIL_HOME", temp_retail_home)
    monkeypatch.setenv("RUN_DATE", "2026-01-05")

    # Mock subprocess.run to simulate successful execution of k_product_and_sales_extract.py
    mock_response = MagicMock()
    mock_response.returncode = 0
    mock_run.return_value = mock_response

    with pytest.raises(SystemExit) as excinfo:
        r_product_and_sales_extract.main()
    
    assert excinfo.value.code == 0

    # Verify log directory and file creation
    log_dir = os.path.join(temp_retail_home, "logs")
    assert os.path.exists(log_dir)
    log_files = os.listdir(log_dir)
    assert len(log_files) == 1
    assert log_files[0].startswith("product_and_sales_extract_")

    # Verify log contents
    with open(os.path.join(log_dir, log_files[0]), "r") as f:
        log_content = f.read()
    assert "Starting product-and-sales extract for run date 2026-01-05" in log_content
    assert "Product-and-sales extract completed successfully for 2026-01-05" in log_content

@patch("subprocess.run")
def test_wrapper_child_failure_propagation(mock_run, temp_retail_home, monkeypatch):
    monkeypatch.setenv("RETAIL_HOME", temp_retail_home)
    monkeypatch.setenv("RUN_DATE", "2026-01-05")

    # Mock subprocess.run to simulate a database failure (exit code 2)
    import subprocess
    mock_run.side_effect = subprocess.CalledProcessError(returncode=2, cmd="mock_cmd")

    with pytest.raises(SystemExit) as excinfo:
        r_product_and_sales_extract.main()
    
    assert excinfo.value.code == 2

    log_dir = os.path.join(temp_retail_home, "logs")
    log_files = os.listdir(log_dir)
    with open(os.path.join(log_dir, log_files[0]), "r") as f:
        log_content = f.read()
    assert "ERROR: k_product_and_sales_extract.ksh failed with exit code 2" in log_content
```

---

## Test Case 5: Schema and Data Quality Assertions (Data Quality)

### Purpose
Ensure that the migrated target tables on BigQuery strictly adhere to the required schema definitions, precision limits, and relational integrity constraints. This prevents silent data truncation, precision loss on financial fields, or orphaned records.

### Setup
The target tables `STG_DAILY_SALES` and `DIM_PRODUCT` are deployed in the BigQuery environment.

### Action
Execute structural metadata queries and data-quality validation checks.

### Pass/Fail Criterion
* **Pass**:
  * `SALE_AMOUNT` in `STG_DAILY_SALES` and `UNIT_PRICE` in `DIM_PRODUCT` are defined as `NUMERIC` or `BIGNUMERIC` (not `FLOAT64`).
  * `IS_CURRENT` is defined as `INT64`.
  * No active records (`IS_CURRENT = 1`) in `DIM_PRODUCT` have a non-null `VALID_TO` date.
  * No inactive records (`IS_CURRENT = 0`) in `DIM_PRODUCT` have a null `VALID_TO` date.
  * All records in `STG_DAILY_SALES` have a valid `REGION_CODE` (no nulls resulting from failed store joins).
* **Fail**: Any schema mismatch or data integrity violation.

### Test Code (SQL Assertions)

```sql
-- Assertion 1: Verify Column Data Types in Target Tables
-- Expected: Financial fields must be NUMERIC/BIGNUMERIC to prevent rounding errors.
SELECT
  table_name,
  column_name,
  data_type
FROM
  `ANALYTICS_SCHEMA.INFORMATION_SCHEMA.COLUMNS`
WHERE
  (table_name = 'STG_DAILY_SALES' AND column_name = 'SALE_AMOUNT')
  OR (table_name = 'DIM_PRODUCT' AND column_name = 'UNIT_PRICE')
  OR (table_name = 'DIM_PRODUCT' AND column_name = 'IS_CURRENT');

-- Assertion 2: Enforce SCD Type 2 Logical Integrity Constraints
-- This query must return 0 rows. Any returned row indicates a corrupted SCD2 state.
SELECT
  PRODUCT_ID,
  IS_CURRENT,
  VALID_FROM,
  VALID_TO,
  'Active record has VALID_TO set' AS failure_reason
FROM
  `ANALYTICS_SCHEMA.DIM_PRODUCT`
WHERE
  IS_CURRENT = 1 AND VALID_TO IS NOT NULL

UNION ALL

SELECT
  PRODUCT_ID,
  IS_CURRENT,
  VALID_FROM,
  VALID_TO,
  'Inactive record has NULL VALID_TO' AS failure_reason
FROM
  `ANALYTICS_SCHEMA.DIM_PRODUCT`
WHERE
  IS_CURRENT = 0 AND VALID_TO IS NULL;

-- Assertion 3: Verify No Orphaned Store Transactions (Join Completeness)
-- This query must return 0 rows. Any returned row indicates a transaction loaded without a valid region mapping.
SELECT
  SALE_ID,
  STORE_ID,
  REGION_CODE
FROM
  `ANALYTICS_SCHEMA.STG_DAILY_SALES`
WHERE
  REGION_CODE IS NULL;
```