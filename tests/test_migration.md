Here is a comprehensive migration-validation test suite designed to verify that the migrated GCP/BigQuery/Airflow implementation of `BERT_P_ADRESSEN` is behaviorally equivalent to the legacy UC4/UNIX/Oracle implementation.

---

# Test Suite: BERT_P_ADRESSEN Migration Validation

## Test Case 1: Cleansing and Standardization (Transformation Correctness)
### Purpose
Verify that the stored procedure `dw_bert.sp_prep_adressen` correctly standardizes raw address fields (trimming whitespace, converting to uppercase, and handling NULL values) during the staging phase.

### Setup
1. Clear the staging table `dw_bert_staging.stg_addresses` and the target table `dw_bert.t_adressen`.
2. Clear the metadata log table `dw_bert.metadata_job_runs` to ensure the delta logic processes all records.
3. Insert test records with mixed casing, leading/trailing whitespaces, and NULL values into the staging table.

```sql
-- Clean up
TRUNCATE TABLE `dw_bert_staging.stg_addresses`;
TRUNCATE TABLE `dw_bert.t_adressen`;
TRUNCATE TABLE `dw_bert.metadata_job_runs`;

-- Insert dirty test data
INSERT INTO `dw_bert_staging.stg_addresses` (address_id, street_name, postal_code, city, country_code, last_modified_timestamp)
VALUES 
  ('ADR-001', '  Hauptstrasse 12a  ', ' de-12345 ', ' Berlin ', ' de ', TIMESTAMP('2026-04-22 10:00:00+00')),
  ('ADR-002', 'CHAMP-DE-MARS', NULL, 'Paris', 'FR', TIMESTAMP('2026-04-22 10:00:00+00')),
  ('ADR-003', NULL, '8001', 'Zürich', NULL, TIMESTAMP('2026-04-22 10:00:00+00'));
```

### Action
Execute the BigQuery stored procedure:
```sql
CALL `dw_bert.sp_prep_adressen`();
```

### Pass/Fail Criterion
**Pass:** 
* `ADR-001` is standardized to: street `HAUPTSTRASSE 12A`, postal code `DE-12345`, city `BERLIN`, country `DE`.
* `ADR-002` and `ADR-003` are processed successfully without throwing `NULL` pointer exceptions.
* The target table contains exactly 3 records, all marked as `is_current = TRUE`.

#### Verification Query
```sql
SELECT 
  address_id, 
  street_name, 
  postal_code, 
  city, 
  country_code,
  is_current
FROM `dw_bert.t_adressen`
ORDER BY address_id;
```
*Expected Output:*
| address_id | street_name | postal_code | city | country_code | is_current |
| :--- | :--- | :--- | :--- | :--- | :--- |
| ADR-001 | HAUPTSTRASSE 12A | DE-12345 | BERLIN | DE | true |
| ADR-002 | CHAMP-DE-MARS | NULL | PARIS | FR | true |
| ADR-003 | NULL | 8001 | ZÜRICH | NULL | true |

---

## Test Case 2: SCD Type 2 - Insert, Update, and Invalidation Logic
### Purpose
Verify that the stored procedure correctly manages Slowly Changing Dimensions (SCD Type 2):
1. Inserts new addresses as active (`is_current = TRUE`, `valid_to = '9999-12-31'`).
2. Deactivates existing active records when a change is detected (`is_current = FALSE`, `valid_to = CURRENT_TIMESTAMP`).
3. Inserts the updated record as the new active version.
4. Ignores records that have not changed.

### Setup
1. Seed the target table `dw_bert.t_adressen` with an active address.
2. Insert three records into the staging table:
   * `ADR-100`: Modified street name (triggers SCD2 update).
   * `ADR-200`: Unchanged record (should be ignored).
   * `ADR-300`: Brand new address (triggers SCD2 insert).

```sql
-- Clean up
TRUNCATE TABLE `dw_bert_staging.stg_addresses`;
TRUNCATE TABLE `dw_bert.t_adressen`;
TRUNCATE TABLE `dw_bert.metadata_job_runs`;

-- Seed target with existing active records
INSERT INTO `dw_bert.t_adressen` (
  address_id, street_name, postal_code, city, country_code, 
  valid_from, valid_to, is_current, source_last_modified, load_ts, batch_id
) VALUES 
('ADR-100', 'OLD STREET 1', '11111', 'MUNICH', 'DE', TIMESTAMP('2026-04-20 00:00:00+00'), TIMESTAMP('9999-12-31 23:59:59+00'), TRUE, TIMESTAMP('2026-04-20 00:00:00+00'), TIMESTAMP('2026-04-20 00:00:00+00'), 'BATCH-000'),
('ADR-200', 'SAME STREET 2', '22222', 'HAMBURG', 'DE', TIMESTAMP('2026-04-20 00:00:00+00'), TIMESTAMP('9999-12-31 23:59:59+00'), TRUE, TIMESTAMP('2026-04-20 00:00:00+00'), TIMESTAMP('2026-04-20 00:00:00+00'), 'BATCH-000');

-- Insert delta records into staging
INSERT INTO `dw_bert_staging.stg_addresses` (address_id, street_name, postal_code, city, country_code, last_modified_timestamp)
VALUES 
  ('ADR-100', 'NEW STREET 1', '11111', 'MUNICH', 'DE', TIMESTAMP('2026-04-22 12:00:00+00')), -- Modified
  ('ADR-200', 'SAME STREET 2', '22222', 'HAMBURG', 'DE', TIMESTAMP('2026-04-22 12:00:00+00')), -- Unchanged
  ('ADR-300', 'NEW ROAD 3', '33333', 'COLOGNE', 'DE', TIMESTAMP('2026-04-22 12:00:00+00'));   -- New
```

### Action
Execute the BigQuery stored procedure:
```sql
CALL `dw_bert.sp_prep_adressen`();
```

### Pass/Fail Criterion
**Pass:**
* `ADR-100` has two rows: the old row is deactivated (`is_current = FALSE`, `valid_to < 9999-12-31`), and the new row is active (`is_current = TRUE`, `street_name = 'NEW STREET 1'`).
* `ADR-200` remains unchanged (only one active row exists, no new row inserted because values matched).
* `ADR-300` is inserted as a new active row.

#### Verification Query
```sql
SELECT 
  address_id, 
  street_name, 
  is_current, 
  valid_from, 
  valid_to
FROM `dw_bert.t_adressen`
ORDER BY address_id, valid_from;
```
*Expected Output:*
| address_id | street_name | is_current | valid_from | valid_to |
| :--- | :--- | :--- | :--- | :--- |
| ADR-100 | OLD STREET 1 | false | 2026-04-20 00:00:00 UTC | *[Execution Timestamp]* |
| ADR-100 | NEW STREET 1 | true | *[Execution Timestamp]* | 9999-12-31 23:59:59 UTC |
| ADR-200 | SAME STREET 2 | true | 2026-04-20 00:00:00 UTC | 9999-12-31 23:59:59 UTC |
| ADR-300 | NEW ROAD 3 | true | *[Execution Timestamp]* | 9999-12-31 23:59:59 UTC |

---

## Test Case 3: Delta Load Isolation (Incremental Processing)
### Purpose
Verify that the stored procedure isolates delta changes by only processing staging records where `last_modified_timestamp` is strictly greater than the `end_time` of the last successful execution of `BERT_P_ADRESSEN`.

### Setup
1. Insert a dummy successful run into `dw_bert.metadata_job_runs` with an execution end time of `2026-04-22 12:00:00 UTC`.
2. Insert two records into the staging table:
   * `ADR-OLD`: Modified *before* the last run (`2026-04-22 11:00:00 UTC`).
   * `ADR-NEW`: Modified *after* the last run (`2026-04-22 13:00:00 UTC`).

```sql
-- Clean up
TRUNCATE TABLE `dw_bert_staging.stg_addresses`;
TRUNCATE TABLE `dw_bert.t_adressen`;
TRUNCATE TABLE `dw_bert.metadata_job_runs`;

-- Seed last successful run
INSERT INTO `dw_bert.metadata_job_runs` (job_name, start_time, end_time, status, rows_affected, message, batch_id)
VALUES ('BERT_P_ADRESSEN', TIMESTAMP('2026-04-22 11:30:00+00'), TIMESTAMP('2026-04-22 12:00:00+00'), 'SUCCESS', 5, 'Prior run', 'BATCH-PREV');

-- Insert staging records
INSERT INTO `dw_bert_staging.stg_addresses` (address_id, street_name, postal_code, city, country_code, last_modified_timestamp)
VALUES 
  ('ADR-OLD', 'OLD WAY', '55555', 'BONN', 'DE', TIMESTAMP('2026-04-22 11:00:00+00')),
  ('ADR-NEW', 'NEW WAY', '66666', 'KIEL', 'DE', TIMESTAMP('2026-04-22 13:00:00+00'));
```

### Action
Execute the BigQuery stored procedure:
```sql
CALL `dw_bert.sp_prep_adressen`();
```

### Pass/Fail Criterion
**Pass:**
* Only `ADR-NEW` is loaded into `dw_bert.t_adressen`.
* `ADR-OLD` is completely ignored because its modification timestamp is older than the last successful run.
* The new entry in `dw_bert.metadata_job_runs` records exactly `rows_affected = 1`.

#### Verification Query
```sql
SELECT address_id, street_name FROM `dw_bert.t_adressen`;
```
*Expected Output:*
| address_id | street_name |
| :--- | :--- |
| ADR-NEW | NEW WAY |

---

## Test Case 4: Airflow Concurrency Guard (UC4 Sync Object Simulation)
### Purpose
Verify that the custom Python guard task `guard_active_run` correctly simulates the UC4 lock `DW.BERT_ADRESS_SYNC` with `Else=Skip` behavior. If another instance of the DAG is already running, the task must raise an `AirflowSkipException` to prevent concurrent execution.

### Setup
Use a Python unit test with `pytest` and mock the Airflow `DagRun` model to simulate an active concurrent execution.

### Test Code
```python
# test_dw_bert_p_adressen_dag.py
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowSkipException
from dags.dw_bert_p_adressen import guard_active_run

@patch('airflow.models.DagRun.find')
def test_guard_active_run_skips_when_active_run_exists(mock_find):
    """
    Test that guard_active_run raises AirflowSkipException if another 
    instance of the same DAG is currently in the RUNNING state.
    """
    # Mock current context
    context = {"run_id": "manual__2026-04-22T12:00:00+00:00"}
    
    # Mock active runs: one is the current run, one is a competing active run
    mock_current_run = MagicMock()
    mock_current_run.run_id = "manual__2026-04-22T12:00:00+00:00"
    
    mock_competing_run = MagicMock()
    mock_competing_run.run_id = "scheduled__2026-04-22T11:00:00+00:00"
    
    mock_find.return_value = [mock_current_run, mock_competing_run]
    
    # Assert that the skip exception is raised
    with pytest.raises(AirflowSkipException) as exc_info:
        guard_active_run(dag_id="dw_bert_p_adressen", **context)
        
    assert "Sync Object constraint failed" in str(exc_info.value)

@patch('airflow.models.DagRun.find')
def test_guard_active_run_proceeds_when_no_other_active_run(mock_find):
    """
    Test that guard_active_run completes without exception if no other
    instance of the DAG is running.
    """
    context = {"run_id": "manual__2026-04-22T12:00:00+00:00"}
    
    mock_current_run = MagicMock()
    mock_current_run.run_id = "manual__2026-04-22T12:00:00+00:00"
    
    mock_find.return_value = [mock_current_run]
    
    # Should run without raising any exception
    try:
        guard_active_run(dag_id="dw_bert_p_adressen", **context)
    except AirflowSkipException:
        pytest.fail("AirflowSkipException raised unexpectedly!")
```

### Action
Run the test suite using `pytest`:
```bash
pytest test_dw_bert_p_adressen_dag.py
```

### Pass/Fail Criterion
**Pass:** Both tests pass successfully, proving that the DAG skips execution when a concurrent run is detected and proceeds normally when it is the sole active run.

---

## Test Case 5: Error Handling and Audit Logging
### Purpose
Verify that if an unexpected error occurs during the execution of `dw_bert.sp_prep_adressen()`, the transaction is rolled back, the error is logged to `dw_bert.metadata_job_runs` with status `FAILED`, and the exception is propagated to fail the Airflow task.

### Setup
1. Force an error inside the stored procedure execution by temporarily renaming the staging table `dw_bert_staging.stg_addresses` to simulate a missing table dependency.
2. Clear the metadata log table.

```sql
-- Rename staging table to trigger a "Table not found" error
ALTER TABLE `dw_bert_staging.stg_addresses` RENAME TO `dw_bert_staging.stg_addresses_temp_hidden`;
TRUNCATE TABLE `dw_bert.metadata_job_runs`;
```

### Action
Execute the BigQuery stored procedure and catch the expected exception:
```sql
BEGIN
  CALL `dw_bert.sp_prep_adressen`();
EXCEPTION WHEN ERROR THEN
  SELECT 'Exception successfully caught and propagated' AS status;
END;
```

### Pass/Fail Criterion
**Pass:**
* The stored procedure fails and propagates the error back to the caller (ensuring Airflow marks the task as failed).
* A row is written to `dw_bert.metadata_job_runs` with `status = 'FAILED'`.
* The `message` column in the metadata table contains the BigQuery system error message (e.g., "Not found: Table...").

#### Verification Query
```sql
SELECT 
  job_name, 
  status, 
  message 
FROM `dw_bert.metadata_job_runs`
WHERE job_name = 'BERT_P_ADRESSEN';
```
*Expected Output:*
| job_name | status | message |
| :--- | :--- | :--- |
| BERT_P_ADRESSEN | FAILED | Not found: Table [project_id]:dw_bert_staging.stg_addresses was not found... |

#### Cleanup
```sql
-- Restore staging table
ALTER TABLE `dw_bert_staging.stg_addresses_temp_hidden` RENAME TO `dw_bert_staging.stg_addresses`;
```

---

## Test Case 6: Schema and Partitioning Assertions
### Purpose
Verify that the target table `dw_bert.t_adressen` is correctly partitioned by `valid_from` and clustered by `address_id` to ensure optimal query performance and cost control in BigQuery.

### Setup
No data setup is required. This test queries BigQuery's `INFORMATION_SCHEMA` metadata.

### Action
Execute the metadata validation query:
```sql
SELECT 
  table_name,
  ddl
FROM `dw_bert.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 't_adressen';
```

### Pass/Fail Criterion
**Pass:** The DDL returned by the metadata query contains the correct partitioning and clustering clauses:
* `PARTITION BY DATE(valid_from)`
* `CLUSTER BY address_id`

#### Verification Query
```sql
SELECT 
  field_path, 
  data_type, 
  is_nullable
FROM `dw_bert.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 't_adressen'
  AND field_path IN ('address_id', 'valid_from', 'valid_to', 'is_current');
```
*Expected Output:*
| field_path | data_type | is_nullable |
| :--- | :--- | :--- |
| address_id | STRING | NO |
| valid_from | TIMESTAMP | NO |
| valid_to | TIMESTAMP | NO |
| is_current | BOOL | NO |