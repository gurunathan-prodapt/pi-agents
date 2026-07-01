Here is a comprehensive suite of migration-validation tests for the migrated `k_ausd_bp_ta_cntrct_dist.ksh` pipeline. 

These tests are designed to validate the Airflow DAG orchestration, the BigQuery stored procedures, parameter validation, error logging, and data idempotency.

---

## Test Case 1: Parameter Validation and Error Logging (Robustness)

### Purpose
Verify that both the Airflow DAG validation task and the BigQuery stored procedure `sp_k_ausd_bp_ta_cntrct_dist` reject invalid inputs, fail gracefully, and correctly log the errors to the `audit_log.job_error_log` table.

### Setup
Ensure the audit log table is empty or state is known before running the test:
```sql
TRUNCATE TABLE `gcp-project-id.audit_log.job_error_log`;
```

### Action
Execute the stored procedure with various invalid parameters:
1. **Scenario A**: Missing `EintragsNr` (NULL).
2. **Scenario B**: Invalid date format for `Stichtag` (`"20231231"` instead of `"31122023"`).
3. **Scenario C**: Non-existent calendar date (`"31062023"` - June 31st).

```sql
-- Scenario A: Missing EintragsNr
DECLARE ex_missing_entry ERROR_TYPE;
BEGIN
  CALL `gcp-project-id.isbert_schema.sp_k_ausd_bp_ta_cntrct_dist`(
    'PoolBasisprodukt', NULL, '31122023', 0, '01012024', '31122023'
  );
EXCEPTION WHEN ERROR THEN
  -- Expected failure
  SELECT 'Scenario A failed as expected' AS status;
END;

-- Scenario B: Invalid date format
BEGIN
  CALL `gcp-project-id.isbert_schema.sp_k_ausd_bp_ta_cntrct_dist`(
    'PoolBasisprodukt', 'RUN_001', '20231231', 0, '01012024', '31122023'
  );
EXCEPTION WHEN ERROR THEN
  SELECT 'Scenario B failed as expected' AS status;
END;

-- Scenario C: Non-existent calendar date
BEGIN
  CALL `gcp-project-id.isbert_schema.sp_k_ausd_bp_ta_cntrct_dist`(
    'PoolBasisprodukt', 'RUN_001', '31062023', 0, '01012024', '31122023'
  );
EXCEPTION WHEN ERROR THEN
  SELECT 'Scenario C failed as expected' AS status;
END;
```

### Pass/Fail Criterion
Query the `job_error_log` table to assert that all three failures were logged with correct descriptive messages.

```sql
-- ASSERTION QUERY
SELECT 
  (SELECT COUNT(1) FROM `gcp-project-id.audit_log.job_error_log` WHERE error_message = 'EintragsNr fehlt') = 1 AS scenario_a_logged,
  (SELECT COUNT(1) FROM `gcp-project-id.audit_log.job_error_log` WHERE error_message LIKE '%Ungueltiges Stichtag Datum%20231231%') = 1 AS scenario_b_logged,
  (SELECT COUNT(1) FROM `gcp-project-id.audit_log.job_error_log` WHERE error_message LIKE '%Ungueltiges Stichtag Datum%31062023%') = 1 AS scenario_c_logged;
```
**Pass**: All three assertions return `TRUE`.  
**Fail**: Any assertion returns `FALSE`, or the stored procedure executions do not raise an error.

---

## Test Case 2: Idempotency and Data Purging (Transformation Correctness)

### Purpose
Verify that the transformation procedure `sp_d_ausd_bp_ta_cntrct_dist` is fully idempotent. Running the job multiple times for the same `Stichtag` must clear out previous runs' data for that date and replace it, preventing duplicate records.

### Setup
1. Clear target table.
2. Insert mock "stale" records for a specific `Stichtag` (`2023-10-15`).

```sql
TRUNCATE TABLE `gcp-project-id.isbert_schema.PoolBasisprodukt`;

-- Insert stale run data
INSERT INTO `gcp-project-id.isbert_schema.PoolBasisprodukt` (
  job_kennung, eintrags_nr, stichtag, heute_date, gestern_date, restart_val, created_at
)
VALUES 
  ('PoolBasisprodukt', 'OLD_RUN_01', '2023-10-15', '2023-10-16', '2023-10-14', 0, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)),
  ('PoolBasisprodukt', 'OLD_RUN_02', '2023-10-15', '2023-10-16', '2023-10-14', 0, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));
```

### Action
Execute the stored procedure for the same `Stichtag` (`15102023`) with a new entry number (`NEW_RUN_99`).

```sql
CALL `gcp-project-id.isbert_schema.sp_k_ausd_bp_ta_cntrct_dist`(
  'PoolBasisprodukt', 
  'NEW_RUN_99', 
  '15102023', 
  0, 
  '16102023', 
  '14102023'
);
```

### Pass/Fail Criterion
Verify that the stale records for `2023-10-15` were purged and only the single new record exists.

```sql
-- ASSERTION QUERY
SELECT 
  COUNT(1) = 1 AS single_record_remains,
  MAX(eintrags_nr) = 'NEW_RUN_99' AS correct_run_retained,
  COUNT(DISTINCT eintrags_nr) = 1 AS no_duplicates
FROM `gcp-project-id.isbert_schema.PoolBasisprodukt`
WHERE stichtag = '2023-10-15';
```
**Pass**: All columns in the assertion query return `TRUE`.  
**Fail**: Stale records remain, or the new record is missing.

---

## Test Case 3: End-to-End Happy Path and Metadata Logging

### Purpose
Verify that a successful execution of the stored procedure correctly populates the target table `PoolBasisprodukt` and logs the execution metadata (including record counts) to `audit_log.job_run_log`.

### Setup
Clear target and run log tables:
```sql
TRUNCATE TABLE `gcp-project-id.isbert_schema.PoolBasisprodukt`;
TRUNCATE TABLE `gcp-project-id.audit_log.job_run_log`;
```

### Action
Call the stored procedure with valid parameters:
```sql
CALL `gcp-project-id.isbert_schema.sp_k_ausd_bp_ta_cntrct_dist`(
  'PoolBasisprodukt', 
  'RUN_SUCCESS_01', 
  '25122023', 
  0, 
  '26122023', 
  '24122023'
);
```

### Pass/Fail Criterion
Verify that:
1. One record is written to `PoolBasisprodukt`.
2. One record is written to `job_run_log` with `records_written = 1`.
3. The dates are correctly parsed and stored.

```sql
-- ASSERTION QUERY
SELECT 
  -- Target Table Assertions
  (SELECT COUNT(1) FROM `gcp-project-id.isbert_schema.PoolBasisprodukt` WHERE stichtag = '2023-12-25') = 1 AS target_written,
  
  -- Run Log Assertions
  (SELECT COUNT(1) FROM `gcp-project-id.audit_log.job_run_log` WHERE entry_nr = 'RUN_SUCCESS_01') = 1 AS log_written,
  (SELECT records_written FROM `gcp-project-id.audit_log.job_run_log` WHERE entry_nr = 'RUN_SUCCESS_01') = 1 AS correct_count_logged,
  (SELECT stichtag FROM `gcp-project-id.audit_log.job_run_log` WHERE entry_nr = 'RUN_SUCCESS_01') = '2023-12-25' AS correct_log_date;
```
**Pass**: All assertions return `TRUE`.  
**Fail**: Target table or run log is missing records, or the logged record count does not match actual records written.

---

## Test Case 4: Airflow DAG Python Validation Logic (Unit Test)

### Purpose
Verify that the Python-based parameter validation and date derivation logic inside the Airflow DAG (`k_ausd_bp_ta_cntrct_dist.py`) behaves identically to the legacy shell script's parameter checks.

### Setup
This test is executed in a Python environment using `pytest` and `unittest.mock`. It mocks the BigQueryHook to prevent actual database writes during unit testing.

### Action
Run the following `pytest` suite:

```python
# test_k_ausd_bp_ta_cntrct_dist_dag.py

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException

# Import the validation function from the DAG file
# (Assuming DAG is in the python path or dags folder)
from dags.k_ausd_bp_ta_cntrct_dist import validate_and_prepare_parameters

class DummyTaskInstance:
    def __init__(self):
        self.xcoms = {}
    def xcom_push(self, key, value):
        self.xcoms[key] = value

@pytest.fixture
def mock_bq_hook():
    with patch('dags.k_ausd_bp_ta_cntrct_dist.BigQueryHook') as mock:
        yield mock

def test_validation_success(mock_bq_hook):
    """Test validation with valid parameters."""
    ti = DummyTaskInstance()
    dag_run_conf = {
        'job_kennung': 'TestJob',
        'eintrags_nr': 'E_123',
        'stichtag': '31122023',
        'wiederanlauf_wert': 1
    }
    
    dag_run_mock = MagicMock()
    dag_run_mock.conf = dag_run_conf
    
    validate_and_prepare_parameters(ti=ti, dag_run=dag_run_mock)
    
    # Assert XComs are pushed correctly
    assert ti.xcoms['job_kennung'] == 'TestJob'
    assert ti.xcoms['eintrags_nr'] == 'E_123'
    assert ti.xcoms['stichtag'] == '31122023'
    assert ti.xcoms['wiederanlauf_wert'] == 1
    
    # Assert dynamic dates are generated in correct format
    today_str = datetime.now().strftime("%d%m%Y")
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%d%m%Y")
    assert ti.xcoms['p_datum_heute'] == today_str
    assert ti.xcoms['p_datum_gestern'] == yesterday_str

def test_validation_missing_params(mock_bq_hook):
    """Test validation fails when required parameters are missing."""
    ti = DummyTaskInstance()
    dag_run_conf = {
        'job_kennung': 'TestJob',
        # 'eintrags_nr' is missing
        'stichtag': '31122023'
    }
    
    dag_run_mock = MagicMock()
    dag_run_mock.conf = dag_run_conf
    
    with pytest.raises(AirflowFailException) as exc_info:
        validate_and_prepare_parameters(ti=ti, dag_run=dag_run_mock)
        
    assert "EintragsNr fehlt" in str(exc_info.value)

def test_validation_invalid_date_format(mock_bq_hook):
    """Test validation fails when date format is invalid."""
    ti = DummyTaskInstance()
    dag_run_conf = {
        'job_kennung': 'TestJob',
        'eintrags_nr': 'E_123',
        'stichtag': '2023-12-31'  # Wrong format (YYYY-MM-DD)
    }
    
    dag_run_mock = MagicMock()
    dag_run_mock.conf = dag_run_conf
    
    with pytest.raises(AirflowFailException) as exc_info:
        validate_and_prepare_parameters(ti=ti, dag_run=dag_run_mock)
        
    assert "Format DDMMYYYY erforderlich" in str(exc_info.value)

def test_validation_invalid_calendar_date(mock_bq_hook):
    """Test validation fails when date is not a valid calendar day."""
    ti = DummyTaskInstance()
    dag_run_conf = {
        'job_kennung': 'TestJob',
        'eintrags_nr': 'E_123',
        'stichtag': '29022023'  # 2023 is not a leap year
    }
    
    dag_run_mock = MagicMock()
    dag_run_mock.conf = dag_run_conf
    
    with pytest.raises(AirflowFailException) as exc_info:
        validate_and_prepare_parameters(ti=ti, dag_run=dag_run_mock)
        
    assert "kein gueltiger Kalendertag" in str(exc_info.value)
```

### Pass/Fail Criterion
**Pass**: All `pytest` assertions pass successfully.  
**Fail**: Any test fails, indicating a gap in parameter validation logic between the legacy shell script and the migrated Python DAG.