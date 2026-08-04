# Migration Validation Test Suite: CUSTOMER.HISTORIZATION_LOAD

This document defines the migration-validation test suite for the `CUSTOMER.HISTORIZATION_LOAD` pipeline. The tests verify that the migrated Apache Airflow DAG, BigQuery SQL scripts, and Python orchestration scripts are behaviourally equivalent to the legacy UC4, Oracle SQL*Plus, and KornShell implementation.

All BigQuery tests are designed to run against a designated test dataset (e.g., `TEST_ANALYTICS_SCHEMA`) to isolate validation from production data.

---

## Test Suite Overview

```
                                 ┌─────────────────────────────────┐
                                 │  1. Schema & Type Verification  │
                                 └────────────────┬────────────────┘
                                                  │
                                                  ▼
                                 ┌─────────────────────────────────┐
                                 │  2. SCD2 Core Logic Validation  │
                                 │     (New, Unchanged, Changed)   │
                                 └────────────────┬────────────────┘
                                                  │
                                                  ▼
                                 ┌─────────────────────────────────┐
                                 │  3. Transaction & Rollback      │
                                 └────────────────┬────────────────┘
                                                  │
                                                  ▼
                                 ┌─────────────────────────────────┐
                                 │  4. Quality Check & Thresholds  │
                                 └────────────────┬────────────────┘
                                                  │
                                                  ▼
                                 ┌─────────────────────────────────┐
                                 │  5. Airflow E2E Integration     │
                                 └─────────────────────────────────┘
```

---

## Section 1: Schema & Data Type Assertions

### Test Case 1.1: Target and Staging Schema Structure Validation
#### Purpose
Verify that the migrated BigQuery tables (`DIM_CUSTOMER_SEGMENT` and `STG_CUSTOMER_SCORE_OUTPUT`) match the target schema specifications, ensuring correct data types, nullability, and precision (especially for `TIMESTAMP` and `NUMERIC` fields).

#### Setup
Ensure the target tables have been deployed to the test dataset `TEST_ANALYTICS_SCHEMA`.

#### Action
Execute the following BigQuery metadata query:

```sql
SELECT 
  table_name, 
  column_name, 
  data_type, 
  is_nullable
FROM 
  `TEST_ANALYTICS_SCHEMA.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name IN ('DIM_CUSTOMER_SEGMENT', 'STG_CUSTOMER_SCORE_OUTPUT')
ORDER BY 
  table_name, ordinal_position;
```

#### Pass/Fail Criterion
The query must return the exact schema structure defined below. Any mismatch in data types (e.g., `DATE` instead of `TIMESTAMP` for `VALID_FROM`/`VALID_TO`) constitutes a **FAIL**.

| Table Name | Column Name | Expected Data Type | Is Nullable |
| :--- | :--- | :--- | :--- |
| `DIM_CUSTOMER_SEGMENT` | `CUSTOMER_ID` | `STRING` (or `INT64`) | `NO` |
| `DIM_CUSTOMER_SEGMENT` | `SEGMENT_CODE` | `STRING` | `YES` |
| `DIM_CUSTOMER_SEGMENT` | `SCORE_BAND` | `STRING` | `YES` |
| `DIM_CUSTOMER_SEGMENT` | `SCORE_VALUE` | `NUMERIC` | `YES` |
| `DIM_CUSTOMER_SEGMENT` | `IS_CURRENT` | `INT64` | `NO` |
| `DIM_CUSTOMER_SEGMENT` | `VALID_FROM` | `TIMESTAMP` | `NO` |
| `DIM_CUSTOMER_SEGMENT` | `VALID_TO` | `TIMESTAMP` | `YES` |
| `STG_CUSTOMER_SCORE_OUTPUT` | `CUSTOMER_ID` | `STRING` (or `INT64`) | `NO` |
| `STG_CUSTOMER_SCORE_OUTPUT` | `SEGMENT_CODE` | `STRING` | `YES` |
| `STG_CUSTOMER_SCORE_OUTPUT` | `SCORE_BAND` | `STRING` | `YES` |
| `STG_CUSTOMER_SCORE_OUTPUT` | `SCORE_VALUE` | `NUMERIC` | `YES` |
| `STG_CUSTOMER_SCORE_OUTPUT` | `RUN_DATE` | `DATE` | `NO` |

---

## Section 2: Output Parity & SCD2 Logic

### Test Case 2.1: SCD2 - New Customer Insertion
#### Purpose
Verify that a customer present in the staging table but completely absent from the target dimension is inserted as a new active record (`IS_CURRENT = 1`) with `VALID_FROM` set to the execution timestamp and `VALID_TO` set to `NULL`.

#### Setup
1. Clear the test tables.
2. Populate the staging table with one new customer.

```sql
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`;
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`;

INSERT INTO `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` 
  (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES 
  ('CUST_NEW_01', 'BRONZE', 'BAND_C', 450.50, DATE '2023-10-15');
```

#### Action
Execute the migrated BigQuery script `d_historization_load.sql` with `@run_date_param = '2023-10-15'`.

#### Pass/Fail Criterion
Query the target table:
```sql
SELECT * FROM `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` WHERE CUSTOMER_ID = 'CUST_NEW_01';
```
* **Pass**: Exactly 1 row is returned where `IS_CURRENT = 1`, `SEGMENT_CODE = 'BRONZE'`, `SCORE_BAND = 'BAND_C'`, `SCORE_VALUE = 450.50`, `VALID_FROM` is populated with the current execution timestamp, and `VALID_TO` is `NULL`.
* **Fail**: No rows are returned, multiple rows are returned, or metadata fields (`IS_CURRENT`, `VALID_FROM`, `VALID_TO`) are incorrectly populated.

---

### Test Case 2.2: SCD2 - Unchanged Customer (No-Op)
#### Purpose
Verify that if a customer's staging data matches their active target record exactly, the pipeline performs no updates or insertions (no-op).

#### Setup
1. Clear the test tables.
2. Populate the target table with an active record.
3. Populate the staging table with identical attributes for the current run date.

```sql
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`;
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`;

INSERT INTO `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` 
  (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES 
  ('CUST_UNCH_01', 'GOLD', 'BAND_A', 950.00, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL);

INSERT INTO `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` 
  (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES 
  ('CUST_UNCH_01', 'GOLD', 'BAND_A', 950.00, DATE '2023-10-15');
```

#### Action
Execute the migrated BigQuery script `d_historization_load.sql` with `@run_date_param = '2023-10-15'`.

#### Pass/Fail Criterion
Query the target table:
```sql
SELECT * FROM `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` WHERE CUSTOMER_ID = 'CUST_UNCH_01';
```
* **Pass**: Exactly 1 row exists. It remains unmodified: `IS_CURRENT = 1`, `VALID_FROM = '2023-10-01 12:00:00 UTC'`, and `VALID_TO` is `NULL`.
* **Fail**: Any row is updated, a new row is inserted, or the existing row's timestamps are modified.

---

### Test Case 2.3: SCD2 - Changed Customer (Re-versioning)
#### Purpose
Verify that if an existing customer's segment or score band changes:
1. The active target record is expired (`IS_CURRENT = 0`, `VALID_TO` set to the transaction timestamp).
2. A new active record is inserted (`IS_CURRENT = 1`, `VALID_FROM` set to the *exact same* transaction timestamp, `VALID_TO` set to `NULL`).

#### Setup
1. Clear the test tables.
2. Populate the target table with an active record.
3. Populate the staging table with a modified `SEGMENT_CODE` for the current run date.

```sql
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`;
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`;

INSERT INTO `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` 
  (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES 
  ('CUST_CHG_01', 'SILVER', 'BAND_B', 600.00, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL);

INSERT INTO `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` 
  (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES 
  ('CUST_CHG_01', 'PLATINUM', 'BAND_B', 600.00, DATE '2023-10-15');
```

#### Action
Execute the migrated BigQuery script `d_historization_load.sql` with `@run_date_param = '2023-10-15'`.

#### Pass/Fail Criterion
Query the target table:
```sql
SELECT * FROM `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` WHERE CUSTOMER_ID = 'CUST_CHG_01' ORDER BY VALID_FROM ASC;
```
* **Pass**: Exactly 2 rows are returned:
  * **Row 1 (Expired)**: `SEGMENT_CODE = 'SILVER'`, `IS_CURRENT = 0`, `VALID_FROM = '2023-10-01 12:00:00 UTC'`, and `VALID_TO` is populated with the transaction timestamp ($T$).
  * **Row 2 (New Active)**: `SEGMENT_CODE = 'PLATINUM'`, `IS_CURRENT = 1`, `VALID_FROM` is populated with the *exact same* transaction timestamp ($T$), and `VALID_TO` is `NULL`.
  * **Timestamp Alignment**: `Row1.VALID_TO` must equal `Row2.VALID_FROM` down to the microsecond.
* **Fail**: The old record is not expired, the new record is not inserted, or the timestamps do not align exactly.

---

## Section 3: Transformation Correctness & Edge Cases

### Test Case 3.1: Transaction Atomicity and Rollback
#### Purpose
Verify that the multi-statement BigQuery scripting block executes within an atomic transaction. If a failure occurs during the second step (the `INSERT` of new active records), all changes made in the first step (the `MERGE` expiration) must be rolled back.

#### Setup
1. Clear the test tables.
2. Populate the target table with an active record.
3. Populate the staging table with a modified record.
4. To force a failure in Step 2, we will temporarily alter the target table schema to make a column non-nullable, then insert a `NULL` value in Step 2, or we can run a modified test script that contains a deliberate runtime error (e.g., division by zero) in the second statement. 

Let's use a test-specific script execution that mimics `d_historization_load.sql` but includes a division-by-zero error in the second statement:

```sql
-- TEST SCRIPT: d_historization_load_fail_test.sql
BEGIN
  DECLARE v_run_date DATE DEFAULT DATE '2023-10-15';
  DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  BEGIN TRANSACTION;

  -- Step 1: Expire old record (This should succeed temporarily)
  MERGE INTO `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` AS tgt
  USING (
      SELECT 'CUST_ROLLBACK_01' AS CUSTOMER_ID, 'PLATINUM' AS SEGMENT_CODE, 'BAND_B' AS SCORE_BAND, 600.00 AS SCORE_VALUE
  ) AS src
  ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = 1)
  WHEN MATCHED THEN
    UPDATE SET tgt.IS_CURRENT = 0, tgt.VALID_TO = v_current_timestamp;

  -- Step 2: Force a runtime error (Division by Zero)
  INSERT INTO `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
      (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
  SELECT 
      src.CUSTOMER_ID, src.SEGMENT_CODE, src.SCORE_BAND, 
      (src.SCORE_VALUE / 0), -- FORCED ERROR
      1, v_current_timestamp
  FROM `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` AS src;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  -- Do not raise here so the test runner can verify the rollback state gracefully
END;
```

Initialize the data:
```sql
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`;
INSERT INTO `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` 
  (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES 
  ('CUST_ROLLBACK_01', 'SILVER', 'BAND_B', 600.00, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL);
```

#### Action
Execute the test script containing the forced error.

#### Pass/Fail Criterion
Query the target table:
```sql
SELECT * FROM `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` WHERE CUSTOMER_ID = 'CUST_ROLLBACK_01';
```
* **Pass**: Exactly 1 row is returned, and it remains completely unchanged (`IS_CURRENT = 1`, `VALID_TO` is `NULL`). This proves the transaction rolled back successfully.
* **Fail**: The row is left in an expired state (`IS_CURRENT = 0`), or any new records are partially committed.

---

## Section 4: External-System Replacements & Orchestration

### Test Case 4.1: Quality Check - Threshold Warning Logic
#### Purpose
Verify that `k_historization_load.py` correctly calculates the percentage of changed customer segments and logs a warning (but does not fail the job) when the change percentage exceeds `MAX_EXPECTED_CHANGE_PCT` (25%).

#### Setup
1. Populate the target table with 10 active records.
2. Populate the staging table such that 3 out of 10 records (30%) are updated/re-versioned on the run date.
3. Set environment variables:
   * `RUN_DATE = '2023-10-15'`
   * `MAX_EXPECTED_CHANGE_PCT = '25'`
   * `GCP_PROJECT = 'your-test-gcp-project'`
   * `BQ_DATASET = 'TEST_ANALYTICS_SCHEMA'`

```sql
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`;
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`;

-- Insert 10 active records
INSERT INTO `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES
  ('C01', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C02', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C03', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C04', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C05', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C06', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C07', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C08', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C09', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL),
  ('C10', 'BRONZE', 'B1', 100, 1, TIMESTAMP '2023-10-01 12:00:00 UTC', NULL);

-- Staging has 3 changed records (C01, C02, C03) and 7 unchanged records
INSERT INTO `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES
  ('C01', 'GOLD', 'B1', 100, DATE '2023-10-15'), -- Changed
  ('C02', 'GOLD', 'B1', 100, DATE '2023-10-15'), -- Changed
  ('C03', 'GOLD', 'B1', 100, DATE '2023-10-15'), -- Changed
  ('C04', 'BRONZE', 'B1', 100, DATE '2023-10-15'),
  ('C05', 'BRONZE', 'B1', 100, DATE '2023-10-15'),
  ('C06', 'BRONZE', 'B1', 100, DATE '2023-10-15'),
  ('C07', 'BRONZE', 'B1', 100, DATE '2023-10-15'),
  ('C08', 'BRONZE', 'B1', 100, DATE '2023-10-15'),
  ('C09', 'BRONZE', 'B1', 100, DATE '2023-10-15'),
  ('C10', 'BRONZE', 'B1', 100, DATE '2023-10-15');
```

#### Action
Execute the Python script `k_historization_load.py` and capture `stdout` and the exit code.

```bash
export RUN_DATE="2023-10-15"
export MAX_EXPECTED_CHANGE_PCT="25"
export CRM_HOME="./" # Path containing customer/d_historization_load.sql
python3 customer/k_historization_load.py
```

#### Pass/Fail Criterion
* **Pass**: 
  * The script exits with code `0`.
  * The console output contains the exact warning message:
    `"WARN: 30% of customers changed segment this week (expected <= 25%) - flagging for review, not failing the job"`
  * The console output contains the completion message:
    `"Historization merge complete, 30% of customers re-versioned"`
* **Fail**: The script exits with a non-zero code, or the warning message is missing from the logs.

---

### Test Case 4.2: Quality Check - Zero Division Protection
#### Purpose
Verify that if the target table is empty (e.g., during initial load), the quality check query does not fail with a division-by-zero error, and the Python script handles the empty state gracefully.

#### Setup
1. Clear the target table.
2. Populate the staging table with initial records.
3. Set environment variables:
   * `RUN_DATE = '2023-10-15'`
   * `MAX_EXPECTED_CHANGE_PCT = '25'`

```sql
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`;
TRUNCATE TABLE `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`;

INSERT INTO `TEST_ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES ('C01', 'GOLD', 'B1', 100, DATE '2023-10-15');
```

#### Action
1. Execute the SQL quality check query directly to verify `NULL` handling.
2. Execute `k_historization_load.py` and capture logs.

#### Pass/Fail Criterion
* **Pass**:
  * The SQL query returns `NULL` (due to `NULLIF(..., 0)`).
  * The Python script logs `"WARN: could not compute changed-row percentage - skipping sanity check"` and exits with code `0`.
* **Fail**: The SQL query fails with a division-by-zero error, or the Python script exits with a non-zero code.

---

## Section 5: Airflow End-to-End Integration

### Test Case 5.1: Airflow DAG Parameter Propagation and Execution
#### Purpose
Verify that the Airflow DAG `CUSTOMER_HISTORIZATION_LOAD_dag` successfully triggers, resolves the `RUN_DATE` parameter dynamically from the Airflow context (`{{ ds }}`), and executes the underlying Python wrapper script.

#### Setup
A Python-based test suite using `pytest` and Airflow's `DagBag` to validate the DAG structure and execute a mock run.

```python
# test_customer_historization_dag.py
import pytest
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.utils import timezone

def test_dag_loaded():
    dagbag = DagBag(dag_folder="dags/customer", include_examples=False)
    dag = dagbag.get_dag(dag_id="CUSTOMER_HISTORIZATION_LOAD_dag")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

def test_dag_parameter_propagation(mocker):
    dagbag = DagBag(dag_folder="dags/customer", include_examples=False)
    dag = dagbag.get_dag(dag_id="CUSTOMER_HISTORIZATION_LOAD_dag")
    
    # Mock the BashOperator execution to prevent actual system calls
    mock_subprocess = mocker.patch("airflow.operators.bash.subprocess.Popen")
    mock_subprocess.return_value.communicate.return_value = (b"Success", b"")
    mock_subprocess.return_value.returncode = 0

    # Create a manual DAG run with a specific logical date
    execution_date = timezone.datetime(2023, 10, 15)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
    )

    ti = dag_run.get_task_instance(task_id="run_historization_load_wrapper")
    ti.task = dag.get_task(task_id="run_historization_load_wrapper")
    
    # Render templates to verify parameter resolution
    ti.render_templates()
    
    # Assert that RUN_DATE resolves to the logical date string (YYYY-MM-DD)
    assert ti.task.env["RUN_DATE"] == "2023-10-15"
```

#### Action
Run the pytest suite:
```bash
pytest test_customer_historization_dag.py
```

#### Pass/Fail Criterion
* **Pass**: All tests pass, confirming that the DAG is syntactically correct, contains the expected tasks, and correctly maps the Airflow logical date (`{{ ds }}`) to the `RUN_DATE` environment variable.
* **Fail**: The DAG fails to load, tasks are missing, or the `RUN_DATE` template resolves incorrectly.