# Migration Validation Test Suite: `k_ausd_bp_ta_rn_einzeln.ksh`

This document defines the migration-validation test suite to verify the behavioral equivalence of the migrated Apache Airflow DAG and Google Cloud BigQuery stored procedures against the legacy KornShell orchestrator `k_ausd_bp_ta_rn_einzeln.ksh`.

---

## Test Suite Overview

The validation strategy is divided into five key areas:
1. **Parameter Validation & Error Handling**: Verifying that missing or malformed inputs trigger the exact expected assertions.
2. **Date Logic & External Script Replacement**: Proving that the BigQuery-native date calculations match the legacy `gestern.ksh` and `h_alis_date.ksh` utilities.
3. **Core Transformation & Row-Count Capture**: Ensuring the business logic correctly interacts with the target table `PoolBasisprodukt`.
4. **Audit Logging & State Preservation**: Confirming that the BigQuery `job_log` table replaces the legacy local temp files (`.tmp`) and FOS job tables with 100% fidelity.
5. **Orchestration & DAG Integration**: Validating that the Airflow DAG correctly parses, sanitizes, and passes parameters to BigQuery.

---

## Test Case 1: Parameter Validation — Missing Required Inputs

### Purpose
Verify that the stored procedure rejects execution and raises an explicit error if any of the mandatory parameters (`Jobkennung`, `EintragsNr`, `Stichtag`) are missing or empty, matching the legacy `pruefeParameterGesetzt` behavior.

### Setup
Ensure the target dataset `prod-isbert-data.isbert_aufbereitung` and the stored procedures are deployed.

### Action
Execute the outer safe wrapper procedure with various combinations of missing parameters.

### Pass/Fail Criterion
Each call must fail with a query execution error containing the specific validation message (e.g., `"Jobkennung fehlt"`, `"EintragsNr fehlt"`, or `"Stichtag fehlt"`). No log entry should be written with a status of `SUCCESS`.

### Test Code (BigQuery SQL Assertions)

```sql
-- Test Case 1a: Missing Jobkennung
BEGIN
  DECLARE error_msg STRING DEFAULT '';
  BEGIN
    CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`(
      NULL, 'E9876', '31122023', '0'
    );
  EXCEPTION WHEN ERROR THEN
    SET error_msg = @@error.message;
  END;
  
  ASSERT error_msg LIKE '%Jobkennung fehlt%' 
    AS CONCAT('Expected "Jobkennung fehlt" error, but got: ', error_msg);
END;

-- Test Case 1b: Empty EintragsNr
BEGIN
  DECLARE error_msg STRING DEFAULT '';
  BEGIN
    CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`(
      'J12345', ' ', '31122023', '0'
    );
  EXCEPTION WHEN ERROR THEN
    SET error_msg = @@error.message;
  END;
  
  ASSERT error_msg LIKE '%EintragsNr fehlt%' 
    AS CONCAT('Expected "EintragsNr fehlt" error, but got: ', error_msg);
END;
```

---

## Test Case 2: Parameter Validation — Invalid Date Format

### Purpose
Verify that the date validation logic rejects any `Stichtag` value that does not strictly conform to the `DDMMYYYY` format, replacing the legacy `DWDate_Datum_Check` utility.

### Setup
Deploy the helper procedure `sp_validate_ddmmyyyy`.

### Action
Call the validation procedure with invalid date formats (e.g., ISO format, text, invalid days/months).

### Pass/Fail Criterion
The procedure must raise an assertion error containing `"Stichtag hat nicht das Format DDMMYYYY"` for all invalid inputs, and successfully parse valid inputs without throwing an error.

### Test Code (BigQuery SQL Assertions)

```sql
-- Test Case 2a: Invalid Date Format (ISO Format)
BEGIN
  DECLARE error_msg STRING DEFAULT '';
  DECLARE out_date DATE;
  BEGIN
    CALL `prod-isbert-data.isbert_aufbereitung.sp_validate_ddmmyyyy`('Stichtag', '2023-12-31', out_date);
  EXCEPTION WHEN ERROR THEN
    SET error_msg = @@error.message;
  END;
  
  ASSERT error_msg LIKE '%Stichtag hat nicht das Format DDMMYYYY%'
    AS CONCAT('Expected date format error, but got: ', error_msg);
END;

-- Test Case 2b: Invalid Calendar Date (February 30th)
BEGIN
  DECLARE error_msg STRING DEFAULT '';
  DECLARE out_date DATE;
  BEGIN
    CALL `prod-isbert-data.isbert_aufbereitung.sp_validate_ddmmyyyy`('Stichtag', '30022023', out_date);
  EXCEPTION WHEN ERROR THEN
    SET error_msg = @@error.message;
  END;
  
  ASSERT error_msg LIKE '%Stichtag hat nicht das Format DDMMYYYY%'
    AS CONCAT('Expected date format error for invalid calendar date, but got: ', error_msg);
END;

-- Test Case 2c: Valid Date Format
BEGIN
  DECLARE out_date DATE;
  CALL `prod-isbert-data.isbert_aufbereitung.sp_validate_ddmmyyyy`('Stichtag', '31122023', out_date);
  ASSERT out_date = DATE('2023-12-31') AS 'Valid date failed to parse correctly';
END;
```

---

## Test Case 3: Restart Value Initialization

### Purpose
Verify that the restart value (`wiederanlaufWert`) is correctly initialized to `'0'` if passed as `NULL` or empty, and preserves its original value if provided.

### Setup
Deploy the helper procedure `sp_init_restart_value`.

### Action
Call the procedure with `NULL`, empty string, and a valid numeric string.

### Pass/Fail Criterion
The output variable must return `'0'` for empty/null inputs, and the exact input string for valid inputs.

### Test Code (BigQuery SQL Assertions)

```sql
-- Test Case 3: Restart Value Defaults
BEGIN
  DECLARE out_val_1 STRING;
  DECLARE out_val_2 STRING;
  DECLARE out_val_3 STRING;

  CALL `prod-isbert-data.isbert_aufbereitung.sp_init_restart_value`(NULL, out_val_1);
  CALL `prod-isbert-data.isbert_aufbereitung.sp_init_restart_value`('  ', out_val_2);
  CALL `prod-isbert-data.isbert_aufbereitung.sp_init_restart_value`('12', out_val_3);

  ASSERT out_val_1 = '0' AS CONCAT('Expected "0" for NULL, got: ', out_val_1);
  ASSERT out_val_2 = '0' AS CONCAT('Expected "0" for empty string, got: ', out_val_2);
  ASSERT out_val_3 = '12' AS CONCAT('Expected "12" for input "12", got: ', out_val_3);
END;
```

---

## Test Case 4: Business Date Calculation (Replacing `gestern.ksh`)

### Purpose
Verify that the business date calculation procedure correctly determines "today" and "yesterday" relative to the current execution timestamp, replacing the legacy `gestern.ksh` script.

### Setup
Deploy `sp_get_business_dates`.

### Action
Call the procedure and compare the output parameters against native BigQuery date functions.

### Pass/Fail Criterion
`o_datum_heute` must equal `CURRENT_DATE()`, and `o_datum_gestern` must equal `CURRENT_DATE() - 1`.

### Test Code (BigQuery SQL Assertions)

```sql
-- Test Case 4: Business Date Parity
BEGIN
  DECLARE v_heute DATE;
  DECLARE v_gestern DATE;

  CALL `prod-isbert-data.isbert_aufbereitung.sp_get_business_dates`(v_heute, v_gestern);

  ASSERT v_heute = CURRENT_DATE() 
    AS CONCAT('Heute mismatch. Expected: ', CURRENT_DATE(), ' Got: ', v_heute);
  ASSERT v_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
    AS CONCAT('Gestern mismatch. Expected: ', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), ' Got: ', v_gestern);
END;
```

---

## Test Case 5: End-to-End Execution & Logging (Success Path)

### Purpose
Verify that a successful execution of the main wrapper procedure:
1. Executes the core business transformation logic.
2. Captures the correct record count from the target table `PoolBasisprodukt`.
3. Writes a `SUCCESS` log entry to `job_log` (replacing the legacy `.tmp` file and FOS job table).

### Setup
1. Create a mock `PoolBasisprodukt` table and populate it with a known number of rows.
2. Truncate the `job_log` table.

```sql
-- Setup DDL & DML
CREATE OR REPLACE TABLE `prod-isbert-data.isbert_aufbereitung.PoolBasisprodukt` AS
SELECT 1 AS id, 'Product A' AS name UNION ALL
SELECT 2 AS id, 'Product B' AS name;

TRUNCATE TABLE `prod-isbert-data.isbert_aufbereitung.job_log`;
```

### Action
Call the safe outer wrapper procedure with valid parameters.

### Pass/Fail Criterion
1. The procedure must complete without errors.
2. A row must be inserted into `job_log` with:
   - `status = 'SUCCESS'`
   - `records = 2` (matching the row count of `PoolBasisprodukt`)
   - `job_kennung = 'J_TEST_01'`
   - `eintrags_nr = 'E_TEST_01'`
   - `stichtag = '15102023'`
   - `error_message IS NULL`

### Test Code (BigQuery SQL Assertions)

```sql
-- Test Case 5: Success Path Execution
BEGIN
  -- 1. Execute wrapper
  CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`(
    'J_TEST_01', 'E_TEST_01', '15102023', '0'
  );

  -- 2. Assert log entry correctness
  ASSERT (
    SELECT COUNT(1) 
    FROM `prod-isbert-data.isbert_aufbereitung.job_log`
    WHERE job_kennung = 'J_TEST_01'
      AND eintrags_nr = 'E_TEST_01'
      AND stichtag = '15102023'
      AND records = 2
      AND status = 'SUCCESS'
      AND error_message IS NULL
  ) = 1 AS 'Success log entry missing or incorrect';
END;
```

---

## Test Case 6: End-to-End Execution & Logging (Failure Path)

### Purpose
Verify that when a downstream step fails (e.g., due to validation errors):
1. The error is caught by the exception handler.
2. A `FAILED` log entry is written to `job_log` containing the exact error message.
3. The exception is raised back to the orchestrator (Airflow) to ensure task failure.

### Setup
Truncate the `job_log` table.

```sql
TRUNCATE TABLE `prod-isbert-data.isbert_aufbereitung.job_log`;
```

### Action
Call the safe outer wrapper with an invalid date format to trigger a validation failure, catching the raised exception at the test runner level.

### Pass/Fail Criterion
1. The procedure call must throw a runtime exception.
2. A row must be written to `job_log` with:
   - `status = 'FAILED'`
   - `error_message` containing `'Stichtag hat nicht das Format DDMMYYYY'`
   - `records = 0`

### Test Code (BigQuery SQL Assertions)

```sql
-- Test Case 6: Failure Path Execution and Logging
BEGIN
  DECLARE error_caught BOOLEAN DEFAULT FALSE;
  
  BEGIN
    CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`(
      'J_FAIL_01', 'E_FAIL_01', 'INVALID_DATE', '0'
    );
  EXCEPTION WHEN ERROR THEN
    SET error_caught = TRUE;
  END;

  -- Assert that the procedure propagated the error to the caller
  ASSERT error_caught = TRUE AS 'Procedure did not raise the exception to the caller';

  -- Assert that the failure was logged in the audit table
  ASSERT (
    SELECT COUNT(1) 
    FROM `prod-isbert-data.isbert_aufbereitung.job_log`
    WHERE job_kennung = 'J_FAIL_01'
      AND eintrags_nr = 'E_FAIL_01'
      AND stichtag = 'INVALID_DATE'
      AND status = 'FAILED'
      AND error_message LIKE '%Stichtag hat nicht das Format DDMMYYYY%'
  ) = 1 AS 'Failure log entry missing or incorrect';
END;
```

---

## Test Case 7: Airflow DAG Parameter Mapping & Validation

### Purpose
Verify that the Airflow DAG `k_ausd_bp_ta_rn_einzeln_dag` correctly parses runtime parameters and maps them to the BigQuery stored procedure call as named parameters.

### Setup
Install `pytest` and the required Apache Airflow libraries in the test environment. Ensure the DAG file `dags/k_ausd_bp_ta_rn_einzeln_dag.py` is in the Python path.

### Action
Run a unit test using `pytest` to parse the DAG, extract the `execute_stored_procedure` task, and validate its configuration and parameter mapping.

### Pass/Fail Criterion
1. The DAG must load without syntax or import errors.
2. The task `execute_stored_procedure` must be an instance of `BigQueryInsertJobOperator`.
3. The SQL query template must target the correct stored procedure.
4. The query parameters must map to the DAG runtime parameters.

### Test Code (Pytest Integration Test)

```python
# test_k_ausd_bp_ta_rn_einzeln_dag.py
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loads_with_no_errors(dagbag):
    """Verify that the DAG is parsed without import errors."""
    dag_id = "k_ausd_bp_ta_rn_einzeln_dag"
    dag = dagbag.get_dag(dag_id)
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

def test_dag_parameters_and_operator_config(dagbag):
    """Verify that the BigQuery operator is configured with the correct parameters."""
    dag = dagbag.get_dag("k_ausd_bp_ta_rn_einzeln_dag")
    task = dag.get_task("execute_stored_procedure")
    
    # Verify Operator Type
    assert task.__class__.__name__ == "BigQueryInsertJobOperator"
    
    # Verify Query Configuration
    query_config = task.configuration["query"]
    expected_procedure_call = "CALL `prod-isbert-data.isbert_aufbereitung.sp_k_ausd_bp_ta_rn_einzeln_safe`("
    
    assert expected_procedure_call in query_config["query"]
    assert query_config["useLegacySql"] is False
    assert query_config["parameterMode"] == "NAMED"
    
    # Verify Parameter Mapping
    params = {p["name"]: p["parameterValue"]["value"] for p in query_config["queryParameters"]}
    assert params["job_kennung"] == "{{ params.job_kennung }}"
    assert params["eintrags_nr"] == "{{ params.eintrags_nr }}"
    assert params["stichtag"] == "{{ params.stichtag }}"
    assert params["wiederanlauf_wert"] == "{{ params.wiederanlauf_wert }}"
```