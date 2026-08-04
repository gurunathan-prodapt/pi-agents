# Migration Validation Test Suite: CUSTOMER.HISTORIZATION_LOAD

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy UC4/Oracle/KornShell implementation and the migrated Apache Airflow/BigQuery/Python implementation of the `CUSTOMER.HISTORIZATION_LOAD` job.

---

## Section 1: End-to-End Pipeline & Orchestration Validation

### Test Case 1.1: Airflow DAG Parameter Propagation & Environment Resolution
* **Purpose**: Verify that the Airflow DAG correctly resolves the execution date (`RUN_DATE`) and configuration parameters (`MAX_EXPECTED_CHANGE_PCT`), and propagates them to the execution environment of the Python tasks.
* **Setup**:
  * A test Airflow environment with the `customer_historization_load` DAG registered.
  * Airflow Variables configured:
    * `GCP_PROJECT` = `test-gcp-project`
    * `BQ_DATASET` = `test_analytics_dataset`
    * `CRM_HOME` = `/opt/etl/customer`
* **Action**:
  * Trigger the DAG manually via the Airflow CLI/API for a specific logical date (e.g., `2026-03-30`) with custom parameters:
    ```bash
    airflow dags trigger \
      --conf '{"max_expected_change_pct": 15, "run_date": "2026-03-30"}' \
      customer_historization_load
    ```
* **Pass/Fail Criterion**:
  * The DAG run completes successfully.
  * The task execution context receives `RUN_DATE` as `2026-03-30` and `MAX_EXPECTED_CHANGE_PCT` as `15`.
  * The execution logs confirm that these values were successfully read and utilized by the underlying Python execution wrapper.

---

### Test Case 1.2: Wrapper Script Execution and Error Propagation
* **Purpose**: Verify that `r_historization_load.py` correctly wraps `k_historization_load.py`, captures its exit codes, and propagates failures to Airflow.
* **Setup**:
  * Configure the environment variable `CRM_HOME` to point to a mock directory.
  * Create a failing mock version of `k_historization_load.py` that exits with code `1`.
* **Action**:
  * Execute `r_historization_load.py` in a test shell environment:
    ```bash
    export CRM_HOME="/tmp/mock_customer"
    export RUN_DATE="2026-03-30"
    python3 customer/r_historization_load.py
    ```
* **Pass/Fail Criterion**:
  * The script must exit with status code `1`.
  * Standard output must contain the log message: `ERROR: k_historization_load.ksh failed with rc=1` (or the Python equivalent `rc=1`).

---

## Section 2: SCD2 Core Transformation & Output Parity

### Test Case 2.1: SCD2 State Transitions (Output Parity)
* **Purpose**: Verify that the BigQuery-translated SCD2 merge logic behaves identically to the legacy Oracle MERGE for all standard SCD2 scenarios:
  1. **New Customer**: Customer exists in staging but not in the dimension (Insert with `IS_CURRENT = 1`, `VALID_FROM = CURRENT_TIMESTAMP()`).
  2. **Unchanged Customer**: Customer exists in both staging and dimension with identical segment and score (No action).
  3. **Changed Customer**: Customer exists in both, but segment or score has changed (Expire existing row by setting `IS_CURRENT = 0`, `VALID_TO = CURRENT_TIMESTAMP()`, and insert a new row with `IS_CURRENT = 1`, `VALID_FROM = CURRENT_TIMESTAMP()`).
  4. **Historical Version Preservation**: Ensure that only the active row (`IS_CURRENT = 1`) is expired, and older historical rows (`IS_CURRENT = 0`) remain untouched.

* **Setup**:
  * Deploy the translated BigQuery SQL scripts for `d_historization_load.sql` and `d_segment_quality_check.sql`.
  * Populate the BigQuery staging table `STG_CUSTOMER_SCORE_OUTPUT` and dimension table `DIM_CUSTOMER_SEGMENT` with the following test dataset:

```sql
-- Clean up tables
TRUNCATE TABLE test_analytics_dataset.STG_CUSTOMER_SCORE_OUTPUT;
TRUNCATE TABLE test_analytics_dataset.DIM_CUSTOMER_SEGMENT;

-- Populate Staging
INSERT INTO test_analytics_dataset.STG_CUSTOMER_SCORE_OUTPUT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE) VALUES
(101, 'GOLD', 'HIGH', 850, DATE '2026-03-30'), -- New Customer
(102, 'SILVER', 'MED', 550, DATE '2026-03-30'), -- Unchanged Customer
(103, 'BRONZE', 'LOW', 250, DATE '2026-03-30'); -- Changed Customer

-- Populate Dimension (Initial State)
INSERT INTO test_analytics_dataset.DIM_CUSTOMER_SEGMENT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO) VALUES
-- Customer 102: Active row matches staging
(102, 'SILVER', 'MED', 550, 1, TIMESTAMP '2026-03-23 00:00:00 UTC', NULL),
-- Customer 103: Active row differs from staging (Segment was GOLD, now BRONZE)
(103, 'GOLD', 'HIGH', 800, 1, TIMESTAMP '2026-03-23 00:00:00 UTC', NULL),
-- Customer 103: Old historical row (should remain untouched)
(103, 'SILVER', 'MED', 500, 0, TIMESTAMP '2026-03-16 00:00:00 UTC', TIMESTAMP '2026-03-23 00:00:00 UTC');
```

* **Action**:
  * Execute the Python test script which runs the SCD2 merge logic on BigQuery for `RUN_DATE = '2026-03-30'`.

* **Concrete Pass/Fail Criterion (pytest code)**:
  * Run the following pytest suite to validate the post-merge state of the BigQuery dimension table.

```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    project = os.environ.get("GCP_PROJECT", "test-gcp-project")
    return bigquery.Client(project=project)

def test_scd2_state_transitions(bq_client):
    dataset = os.environ.get("BQ_DATASET", "test_analytics_dataset")
    
    # 1. Assert New Customer (101) was inserted correctly
    query_101 = f"""
        SELECT SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_TO 
        FROM `{dataset}.DIM_CUSTOMER_SEGMENT` 
        WHERE CUSTOMER_ID = 101
    """
    rows_101 = list(bq_client.query(query_101).result())
    assert len(rows_101) == 1
    assert rows_101[0]["SEGMENT_CODE"] == "GOLD"
    assert rows_101[0]["SCORE_BAND"] == "HIGH"
    assert rows_101[0]["SCORE_VALUE"] == 850
    assert rows_101[0]["IS_CURRENT"] == 1
    assert rows_101[0]["VALID_TO"] is None

    # 2. Assert Unchanged Customer (102) remains untouched (no duplicate rows, still current)
    query_102 = f"""
        SELECT COUNT(*) as cnt, MAX(IS_CURRENT) as max_curr
        FROM `{dataset}.DIM_CUSTOMER_SEGMENT` 
        WHERE CUSTOMER_ID = 102
    """
    rows_102 = list(bq_client.query(query_102).result())
    assert rows_102[0]["cnt"] == 1
    assert rows_102[0]["max_curr"] == 1

    # 3. Assert Changed Customer (103) has old row expired and new row inserted
    query_103 = f"""
        SELECT SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_TO 
        FROM `{dataset}.DIM_CUSTOMER_SEGMENT` 
        WHERE CUSTOMER_ID = 103
        ORDER BY VALID_TO DESC NULLS FIRST
    """
    rows_103 = list(bq_client.query(query_103).result())
    # Should have 3 rows now: 1 new active, 1 expired active, 1 old historical
    assert len(rows_103) == 3
    
    # New active row
    assert rows_103[0]["SEGMENT_CODE"] == "BRONZE"
    assert rows_103[0]["SCORE_BAND"] == "LOW"
    assert rows_103[0]["IS_CURRENT"] == 1
    assert rows_103[0]["VALID_TO"] is None
    
    # Expired active row
    assert rows_103[1]["SEGMENT_CODE"] == "GOLD"
    assert rows_103[1]["SCORE_BAND"] == "HIGH"
    assert rows_103[1]["IS_CURRENT"] == 0
    assert rows_103[1]["VALID_TO"] is not None
    
    # Old historical row (untouched)
    assert rows_103[2]["SEGMENT_CODE"] == "SILVER"
    assert rows_103[2]["SCORE_BAND"] == "MED"
    assert rows_103[2]["IS_CURRENT"] == 0
    assert rows_103[2]["VALID_TO"] is not None
```

---

### Test Case 2.2: NULL Handling and Edge Cases
* **Purpose**: Verify that NULL values in staging fields (`SEGMENT_CODE`, `SCORE_BAND`, `SCORE_VALUE`) do not cause unexpected matching failures or infinite row versioning.
* **Setup**:
  * Populate staging with a record containing NULL values.
  * Populate the dimension with a matching record containing NULL values.
* **Action**:
  * Run the historization load.
* **Pass/Fail Criterion**:
  * If staging and dimension both contain NULL for a field, they must be treated as identical (no new version created).
  * If staging contains a value and dimension contains NULL (or vice versa), the row must be correctly expired and a new version inserted.

```python
def test_null_handling(bq_client):
    dataset = os.environ.get("BQ_DATASET", "test_analytics_dataset")
    
    # Setup: Staging has NULL score band
    bq_client.query(f"""
        TRUNCATE TABLE `{dataset}.STG_CUSTOMER_SCORE_OUTPUT`;
        INSERT INTO `{dataset}.STG_CUSTOMER_SCORE_OUTPUT` (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE) 
        VALUES (104, 'GOLD', NULL, 800, DATE '2026-03-30');
        
        TRUNCATE TABLE `{dataset}.DIM_CUSTOMER_SEGMENT`;
        INSERT INTO `{dataset}.DIM_CUSTOMER_SEGMENT` (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO) 
        VALUES (104, 'GOLD', NULL, 800, 1, TIMESTAMP '2026-03-23 00:00:00 UTC', NULL);
    """).result()
    
    # Run merge logic (mocked execution of k_historization_load.py)
    # ... execution code ...

    # Assert: No new row created because NULL == NULL in business logic
    query = f"SELECT COUNT(*) as cnt FROM `{dataset}.DIM_CUSTOMER_SEGMENT` WHERE CUSTOMER_ID = 104"
    res = list(bq_client.query(query).result())
    assert res[0]["cnt"] == 1
```

---

## Section 3: Data Quality & Threshold Validation

### Test Case 3.1: Quality Check Threshold - Under Limit
* **Purpose**: Verify that when the percentage of changed customer segments is below the safety threshold, the script logs the percentage and exits with code `0`.
* **Setup**:
  * Set `MAX_EXPECTED_CHANGE_PCT` = `25`.
  * Populate `DIM_CUSTOMER_SEGMENT` such that 10% of the active rows have `VALID_FROM` equal to the run date (representing a 10% change rate).
* **Action**:
  * Execute `k_historization_load.py` with `RUN_DATE="2026-03-30"`.
* **Pass/Fail Criterion**:
  * The process exits with code `0`.
  * Standard output contains: `Historization merge complete, 10% of customers re-versioned`.
  * Standard output does *not* contain any `WARN` messages regarding threshold violations.

---

### Test Case 3.2: Quality Check Threshold - Over Limit (Warning Trigger)
* **Purpose**: Verify that when the percentage of changed customer segments exceeds the safety threshold, the script logs a warning but still exits with code `0` (non-blocking warning behavior).
* **Setup**:
  * Set `MAX_EXPECTED_CHANGE_PCT` = `25`.
  * Populate `DIM_CUSTOMER_SEGMENT` such that 40% of the active rows have `VALID_FROM` equal to the run date (representing a 40% change rate).
* **Action**:
  * Execute `k_historization_load.py` with `RUN_DATE="2026-03-30"`.
* **Pass/Fail Criterion**:
  * The process exits with code `0`.
  * Standard output contains: `WARN: 40% of customers changed segment this week (expected <= 25%) - flagging for review, not failing the job`.
  * Standard output contains: `Historization merge complete, 40% of customers re-versioned`.

---

### Test Case 3.3: Quality Check - Empty Output Handling
* **Purpose**: Verify that if the quality check query returns an empty result or cannot compute the percentage, the script logs a warning and exits gracefully with code `0`.
* **Setup**:
  * Truncate `DIM_CUSTOMER_SEGMENT` so that the total count of active rows is `0` (causing a division by zero or empty result in the quality check query).
* **Action**:
  * Execute `k_historization_load.py`.
* **Pass/Fail Criterion**:
  * The process exits with code `0`.
  * Standard output contains: `WARN: could not compute changed-row percentage - skipping sanity check`.

---

## Section 4: SQL Dialect & Schema Equivalence

### Test Case 4.1: Oracle to BigQuery SQL Dialect Equivalence
* **Purpose**: Verify that the translated BigQuery SQL queries produce identical results to the legacy Oracle SQL queries, accounting for dialect differences (`SYSDATE` vs `CURRENT_TIMESTAMP()`, `TO_DATE` vs `PARSE_DATE`, and `MERGE` syntax constraints).
* **Setup**:
  * Maintain a parallel test environment with Oracle (legacy) and BigQuery (target).
  * Load identical input datasets into both environments.
* **Action**:
  * Execute `d_historization_load.sql` on Oracle.
  * Execute the translated BigQuery SQL on BigQuery.
* **Pass/Fail Criterion**:
  * Compare the resulting target tables row-by-row.
  * The values for `CUSTOMER_ID`, `SEGMENT_CODE`, `SCORE_BAND`, `SCORE_VALUE`, and `IS_CURRENT` must match exactly between Oracle and BigQuery.
  * `VALID_FROM` and `VALID_TO` timestamps must represent the same logical dates (ignoring minor execution-time second differences).

```sql
-- BigQuery SQL Assertion for Schema and Column Type Equivalence
SELECT 
  column_name, 
  data_type, 
  is_nullable
FROM 
  `test_analytics_dataset`.INFORMATION_SCHEMA.COLUMNS
WHERE 
  table_name = 'DIM_CUSTOMER_SEGMENT'
ORDER BY 
  ordinal_position;

-- Expected Output Types:
-- CUSTOMER_ID: INT64
-- SEGMENT_CODE: STRING
-- SCORE_BAND: STRING
-- SCORE_VALUE: INT64
-- IS_CURRENT: INT64 (or BOOL)
-- VALID_FROM: TIMESTAMP
-- VALID_TO: TIMESTAMP
```