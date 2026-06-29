This document provides a comprehensive migration-validation test suite for the BigQuery stored procedure `sp_k_ausd_bp_ta_bpr_apn` and its associated orchestration components, which replace the legacy KornShell script `k_ausd_bp_ta_bpr_apn.ksh`.

---

## Test Suite Overview

The validation strategy is divided into four main areas:
1. **Negative Testing (Input Validation & Error Handling)**: Verifies that parameter checks and date-format validations behave identically to the legacy shell script, raising the correct error codes (`192`, `193`) and logging them to `job_error_log`.
2. **Positive Testing (Functional & Behavioral Equivalence)**: Verifies that valid inputs execute successfully, default parameters are handled correctly, and data is written to `PoolBasisprodukt` and `job_audit_log`.
3. **Orchestration & Integration Testing**: Verifies that the Airflow DAG correctly formats execution dates and triggers the stored procedure with the expected parameters.
4. **Data Quality & Schema Assertions**: Verifies that the target tables conform to the expected schema and constraints.

---

## Test Category 1: Negative Testing (Input Validation & Error Handling)

### Test Case 1.1: Missing Parameter Validation (Error Code 193)
* **Purpose**: Verify that calling the stored procedure with missing mandatory parameters (`p_JobKennung`, `p_Stichtag`, or `p_EintragsNr`) raises an exception with error code `193` and logs the failure to `job_error_log`, matching the legacy `h_alis_parameter.ksh` behavior.
* **Setup**:
  ```sql
  TRUNCATE TABLE `project.dataset.job_error_log`;
  ```
* **Action**: Execute the stored procedure with `p_JobKennung` set to `NULL`.
  ```sql
  DECLARE error_thrown BOOLEAN DEFAULT FALSE;
  BEGIN
    CALL `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
      NULL,          -- p_JobKennung (Missing)
      'E_001',       -- p_EintragsNr
      '31122023',    -- p_Stichtag
      '0'            -- p_wiederanlaufWert
    );
  EXCEPTION WHEN ERROR THEN
    SET error_thrown = TRUE;
  END;
  ```
* **Pass/Fail Criterion**:
  * **Pass**: The procedure raises an exception containing the string `'FEHLER: 0 E 193 - Jobkennung is missing'`. A query on `job_error_log` returns exactly 1 row with `error_code = 193` and `error_arg = 'Jobkennung is missing'`.
  * **Fail**: The procedure executes without throwing an error, or the error is not logged in `job_error_log`.

#### Automated Pytest Assertion
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

def test_missing_parameter_raises_193(bq_client):
    # Clear error log
    bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`").result()
    
    # Execute procedure with missing JobKennung
    query = """
    CALL `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
      NULL, 'E_001', '31122023', '0'
    )
    """
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(query).result()
        
    assert "FEHLER: 0 E 193 - Jobkennung is missing" in str(excinfo.value)
    
    # Verify log entry
    log_query = "SELECT * FROM `project.dataset.job_error_log` WHERE error_code = 193"
    results = list(bq_client.query(log_query).result())
    assert len(results) == 1
    assert results[0]["error_arg"] == "Jobkennung is missing"
    assert results[0]["tab_name"] == "PoolBasisprodukt"
```

---

### Test Case 1.2: Invalid Date Format Validation (Error Code 192)
* **Purpose**: Verify that passing a date parameter not in `DDMMYYYY` format (e.g., ISO format `YYYY-MM-DD` or alphanumeric strings) is caught by `SAFE.PARSE_DATE`, raises an exception with error code `192`, and logs the failure to `job_error_log`. This replaces the legacy `DWDate_Datum_Check` utility.
* **Setup**:
  ```sql
  TRUNCATE TABLE `project.dataset.job_error_log`;
  ```
* **Action**: Call the stored procedure with an invalid date format `'2023-12-31'`.
  ```sql
  CALL `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
    'JOB_TEST_01',
    'E_001',
    '2023-12-31',  -- Invalid format (Expected DDMMYYYY)
    '0'
  );
  ```
* **Pass/Fail Criterion**:
  * **Pass**: The procedure fails with message `'FEHLER: 0 E 192 - Invalid date format for Stichtag: 2023-12-31'`. A row is inserted into `job_error_log` with `error_code = 192`.
  * **Fail**: The procedure parses the date incorrectly without failing, or fails with a generic system exception instead of the custom business exception.

---

## Test Category 2: Positive Testing (Functional & Behavioral Equivalence)

### Test Case 2.1: Standard Execution & Audit Logging
* **Purpose**: Verify that a successful execution with valid parameters runs the child transformation procedure `sp_d_ausd_bp_ta_bpr_apn`, populates the target table `PoolBasisprodukt`, and writes a success record to `job_audit_log` with the correct record count.
* **Setup**:
  ```sql
  TRUNCATE TABLE `project.dataset.PoolBasisprodukt`;
  TRUNCATE TABLE `project.dataset.job_audit_log`;
  ```
* **Action**: Call the stored procedure with valid parameters.
  ```sql
  CALL `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
    'JOB_VALID_01',
    'E_999',
    '15082023',  -- 15th Aug 2023
    '1'
  );
  ```
* **Pass/Fail Criterion**:
  * **Pass**: 
    1. `PoolBasisprodukt` contains exactly 1 row with `stichtag = '2023-08-15'`, `job_kennung = 'JOB_VALID_01'`, `eintrags_nr = 'E_999'`, and `status = 'PROCESSED_RESTART_1'`.
    2. `job_audit_log` contains exactly 1 row with `records_loaded = 1`, `status = 'SUCCESS'`, and `stichtag = '15082023'`.
  * **Fail**: No records are loaded, or the record count in `job_audit_log` does not match the actual number of rows inserted into `PoolBasisprodukt`.

#### Automated Pytest Assertion
```python
def test_successful_execution_and_audit(bq_client):
    # Clear target and audit tables
    bq_client.query("TRUNCATE TABLE `project.dataset.PoolBasisprodukt`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_audit_log`").result()
    
    # Execute procedure
    query = """
    CALL `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
      'JOB_VALID_01', 'E_999', '15082023', '1'
    )
    """
    bq_client.query(query).result()
    
    # Assert Target Table Population
    target_query = "SELECT * FROM `project.dataset.PoolBasisprodukt` WHERE stichtag = '2023-08-15'"
    target_rows = list(bq_client.query(target_query).result())
    assert len(target_rows) == 1
    assert target_rows[0]["job_kennung"] == "JOB_VALID_01"
    assert target_rows[0]["eintrags_nr"] == "E_999"
    assert target_rows[0]["status"] == "PROCESSED_RESTART_1"
    
    # Assert Audit Log Entry
    audit_query = "SELECT * FROM `project.dataset.job_audit_log` WHERE stichtag = '15082023'"
    audit_rows = list(bq_client.query(audit_query).result())
    assert len(audit_rows) == 1
    assert audit_rows[0]["records_loaded"] == 1
    assert audit_rows[0]["status"] == "SUCCESS"
    assert audit_rows[0]["job_kennung"] == "JOB_VALID_01"
```

---

### Test Case 2.2: Restart Value Defaulting
* **Purpose**: Verify that if `p_wiederanlaufWert` is passed as `NULL` or an empty string `''`, the procedure defaults it to `'0'` before passing it to the child transformation procedure.
* **Setup**:
  ```sql
  TRUNCATE TABLE `project.dataset.PoolBasisprodukt`;
  ```
* **Action**: Execute the stored procedure with `p_wiederanlaufWert` as `NULL`.
  ```sql
  CALL `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
    'JOB_RESTART_TEST',
    'E_002',
    '16082023',
    NULL
  );
  ```
* **Pass/Fail Criterion**:
  * **Pass**: The row inserted into `PoolBasisprodukt` has `status = 'PROCESSED_RESTART_0'`, proving that the `NULL` value was successfully defaulted to `'0'`.
  * **Fail**: The procedure fails with a null pointer exception, or the status column contains `'PROCESSED_RESTART_NULL'`.

---

## Test Category 3: Orchestration & Integration Testing

### Test Case 3.1: Airflow DAG Parameter Formatting & Dry Run
* **Purpose**: Verify that the Airflow DAG `dw_k_ausd_bp_ta_bpr_apn` correctly formats the execution date `ds` (which is in `YYYY-MM-DD` format) into the required `DDMMYYYY` format using Airflow's `ds_format` macro, and successfully compiles the SQL statement.
* **Setup**: Install `apache-airflow` and the Google Cloud provider package in the test environment.
* **Action**: Programmatically render the Airflow task templates for a specific execution date.
* **Pass/Fail Criterion**:
  * **Pass**: The rendered SQL query string matches the expected pattern: `CALL \`project.dataset.sp_k_ausd_bp_ta_bpr_apn\`('DEFAULT_JOB', 'DEFAULT_ENTRY', '25102023', '0')` for an execution date of `2023-10-25`.
  * **Fail**: The date is rendered in the wrong format (e.g., `'2023-10-25'`), or template rendering throws a syntax error.

#### Automated Pytest Assertion
```python
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
import pendulum

def test_dag_template_rendering():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("dw_k_ausd_bp_ta_bpr_apn")
    assert dag is not None
    
    task = dag.get_task("run_sp_k_ausd_bp_ta_bpr_apn")
    
    # Create a mock DagRun
    execution_date = pendulum.datetime(2023, 10, 25, tz="UTC")
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id="test_run",
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        conf={}
    )
    
    # Render templates
    from airflow.templates import SandboxedEnvironment
    env = SandboxedEnvironment()
    context = dag_run.get_template_context()
    
    rendered_query = env.from_string(task.configuration["query"]["query"]).render(**context)
    
    # Assertions
    assert "sp_k_ausd_bp_ta_bpr_apn" in rendered_query
    # Verify that YYYY-MM-DD (2023-10-25) is converted to DDMMYYYY (25102023)
    assert "'25102023'" in rendered_query
```

---

## Test Category 4: Data Quality & Schema Assertions

### Test Case 4.1: Target and Log Schema Validation
* **Purpose**: Ensure that the target tables (`PoolBasisprodukt`, `job_error_log`, `job_audit_log`) are created with the correct column data types and descriptions as defined in the DDL specifications.
* **Setup**: Ensure DDL scripts have been executed in the target BigQuery dataset.
* **Action**: Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view.
* **Pass/Fail Criterion**:
  * **Pass**: All columns match the specified types exactly:
    * `job_error_log.error_code` is `INT64`.
    * `job_audit_log.records_loaded` is `INT64`.
    * `PoolBasisprodukt.stichtag` is `DATE`.
  * **Fail**: Any column is missing, or has an incorrect data type (e.g., `stichtag` defined as `STRING` instead of `DATE`).

#### SQL Assertion Script
```sql
-- Assert Schema for job_error_log
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'job_error_log'
  AND column_name IN ('tab_name', 'error_code', 'error_arg', 'created_at')
ORDER BY
  column_name;

-- Expected Output Verification:
-- error_code -> INT64
-- created_at -> TIMESTAMP
-- error_arg  -> STRING
-- tab_name   -> STRING
```