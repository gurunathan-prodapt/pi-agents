Here is a comprehensive suite of migration-validation tests designed to prove that the migrated BigQuery stored procedures, metadata tables, and Airflow DAG behave identically to the legacy KornShell script (`k_ausd_bp_ta_bcp_iccid.ksh`).

---

# Migration Validation Test Suite: `k_ausd_bp_ta_bcp_iccid`

## Test Suite Overview
These tests validate that the Google Cloud Platform (GCP) target components preserve the exact orchestration, parameter validation, error handling, and metadata logging behaviors of the legacy on-premises KornShell script.

---

## Section 1: Parameter Validation & Error Handling

### Test Case 1.1: Missing Required Parameters (Error Code 193)
#### Purpose
Verify that the stored procedure raises an error and logs a failure to `isbert_metadata.job_error_log` when any of the required parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`) are missing or empty, matching the legacy `pruefeParameterGesetzt` behavior.

#### Setup
Ensure the metadata tables are initialized and empty for the test run.
```sql
TRUNCATE TABLE `isbert_metadata.job_error_log`;
TRUNCATE TABLE `isbert_metadata.job_tracking`;
```

#### Action
Execute the stored procedure with a missing `p_job_kennung` parameter:
```sql
DECLARE v_error_thrown BOOLEAN DEFAULT FALSE;

BEGIN
  CALL `isbert_schema.k_ausd_bp_ta_bcp_iccid`(
    NULL,                -- p_job_kennung (Missing)
    '12345',             -- p_eintrags_nr
    '31122024',          -- p_stichtag
    '0'                  -- p_wiederanlauf_wert
  );
EXCEPTION WHEN ERROR THEN
  IF @@error.message LIKE '%FEHLER: 193%' THEN
    SET v_error_thrown = TRUE;
  END IF;
END;

-- Assert that the error was caught
SELECT v_error_thrown AS error_successfully_raised;
```

#### Pass/Fail Criterion
*   **Pass**: The procedure raises an exception containing the string `FEHLER: 193 Jobkennung fehlt`, and a row is written to `isbert_metadata.job_error_log` matching the assertions below.
*   **Fail**: The procedure executes successfully, raises a different error code, or fails to write to the error log.

#### SQL Assertions
```sql
-- Assert error log entry exists with correct metadata
SELECT 
  COUNT(1) = 1 AS has_one_error_log,
  MAX(error_code) = 193 AS has_correct_error_code,
  MAX(error_message) = 'Jobkennung fehlt' AS has_correct_error_message
FROM `isbert_metadata.job_error_log`
WHERE job_name = 'k_ausd_bp_ta_bcp_iccid';
```

---

### Test Case 1.2: Invalid Date Format (Error Code 192)
#### Purpose
Verify that the stored procedure rejects dates that do not conform to the strict `DDMMYYYY` format, matching the legacy `DWDate_Datum_Check` utility.

#### Setup
```sql
TRUNCATE TABLE `isbert_metadata.job_error_log`;
```

#### Action
Execute the stored procedure with an ISO-formatted date (`YYYY-MM-DD`) instead of `DDMMYYYY`:
```sql
DECLARE v_error_thrown BOOLEAN DEFAULT FALSE;

BEGIN
  CALL `isbert_schema.k_ausd_bp_ta_bcp_iccid`(
    'JOB_TEST_01',
    '12345',
    '2024-12-31',        -- Invalid format (should be 31122024)
    '0'
  );
EXCEPTION WHEN ERROR THEN
  IF @@error.message LIKE '%FEHLER: 192%' THEN
    SET v_error_thrown = TRUE;
  END IF;
END;

SELECT v_error_thrown AS error_successfully_raised;
```

#### Pass/Fail Criterion
*   **Pass**: The procedure raises an exception containing `FEHLER: 192 Ungueltiges Datum: 2024-12-31`, and a row is written to `isbert_metadata.job_error_log` with error code `192`.
*   **Fail**: The procedure accepts the date, parses it incorrectly, or fails to log the error.

#### SQL Assertions
```sql
SELECT 
  COUNT(1) = 1 AS has_one_error_log,
  MAX(error_code) = 192 AS has_correct_error_code,
  MAX(error_message) = 'Ungueltiges Datum: 2024-12-31' AS has_correct_error_message
FROM `isbert_metadata.job_error_log`
WHERE job_name = 'k_ausd_bp_ta_bcp_iccid';
```

---

## Section 2: Happy Path Execution & Metadata Logging

### Test Case 2.1: Successful Execution and State Tracking
#### Purpose
Verify that a successful run of the stored procedure registers a start state (`R` for Running), executes the core logic, updates the state to success (`S`), and accurately records the processed row count. This replaces the legacy `FOSJobErzeugeEintrag` and the temporary file (`$tmpFile`) record-count capture.

#### Setup
Create a mock source table and populate it with sample data to verify row-count capture.
```sql
CREATE OR REPLACE TABLE `isbert_schema.source_table` AS
SELECT DATE '2024-12-31' AS business_date, 'ICCID_001' AS iccid, 'ACTIVE' AS status UNION ALL
SELECT DATE '2024-12-31' AS business_date, 'ICCID_002' AS iccid, 'ACTIVE' AS status UNION ALL
SELECT DATE '2024-12-31' AS business_date, 'ICCID_003' AS iccid, 'INACTIVE' AS status;

-- Create target table structure
CREATE OR REPLACE TABLE `isbert_schema.PoolBasisprodukt` (
  business_date DATE,
  iccid STRING,
  status STRING
);

TRUNCATE TABLE `isbert_metadata.job_tracking`;
```

#### Action
Execute the stored procedure with valid parameters:
```sql
CALL `isbert_schema.k_ausd_bp_ta_bcp_iccid`(
  'JOB_TEST_OK',
  '99999',
  '31122024',
  '0'
);
```

#### Pass/Fail Criterion
*   **Pass**: 
    1. The target table `isbert_schema.PoolBasisprodukt` is populated with exactly 3 rows.
    2. The tracking table `isbert_metadata.job_tracking` contains a single record with status `S`.
    3. The `record_count` in the tracking table is exactly `3`.
    4. The description is set to `'Initialbefuellung'`.
*   **Fail**: The procedure fails, target table is empty, or tracking metrics do not match the actual rows processed.

#### SQL Assertions
```sql
-- Assert Target Table Population
SELECT COUNT(1) = 3 AS target_has_correct_row_count FROM `isbert_schema.PoolBasisprodukt`;

-- Assert Tracking Table Metrics
SELECT 
  COUNT(1) = 1 AS has_one_tracking_entry,
  MAX(run_status) = 'S' AS status_is_success,
  MAX(record_count) = 3 AS tracking_has_correct_row_count,
  MAX(description) = 'Initialbefuellung' AS description_is_correct,
  MAX(business_date) = DATE '2024-12-31' AS business_date_is_correct
FROM `isbert_metadata.job_tracking`
WHERE job_name = 'k_ausd_bp_ta_bcp_iccid' AND entry_no = '99999';
```

---

## Section 3: Core Transformation & Exception Handling

### Test Case 3.1: Core Execution Failure and Rollback Logging
#### Purpose
Verify that if the core SQL execution block fails (e.g., due to a missing table, schema mismatch, or database error), the exception is caught, logged as a failure (`F`) in `job_tracking`, logged in `job_error_log`, and then re-raised to alert the orchestrator.

#### Setup
Force a failure by dropping the source table before execution.
```sql
DROP TABLE IF EXISTS `isbert_schema.source_table`;
TRUNCATE TABLE `isbert_metadata.job_tracking`;
TRUNCATE TABLE `isbert_metadata.job_error_log`;
```

#### Action
Execute the stored procedure and catch the expected failure:
```sql
DECLARE v_error_propagated BOOLEAN DEFAULT FALSE;

BEGIN
  CALL `isbert_schema.k_ausd_bp_ta_bcp_iccid`(
    'JOB_TEST_FAIL',
    '88888',
    '31122024',
    '0'
  );
EXCEPTION WHEN ERROR THEN
  SET v_error_propagated = TRUE;
END;

SELECT v_error_propagated AS error_was_propagated_to_caller;
```

#### Pass/Fail Criterion
*   **Pass**: The procedure propagates the error to the caller, creates a tracking entry with status `F`, and writes the detailed database error message to `isbert_metadata.job_error_log`.
*   **Fail**: The procedure swallows the error, reports success, or fails to log the error details.

#### SQL Assertions
```sql
-- Assert Job Tracking shows Failure
SELECT 
  COUNT(1) = 1 AS has_tracking_entry,
  MAX(run_status) = 'F' AS status_is_failed,
  MAX(description) LIKE '%Not found: Table%' OR MAX(description) LIKE '%source_table%' AS tracking_has_error_details
FROM `isbert_metadata.job_tracking`
WHERE job_name = 'k_ausd_bp_ta_bcp_iccid' AND entry_no = '88888';

-- Assert Error Log contains the system error message
SELECT 
  COUNT(1) = 1 AS has_error_log_entry,
  MAX(error_code) = 1 AS error_code_is_generic_failure,
  MAX(error_message) LIKE '%Not found: Table%' OR MAX(error_message) LIKE '%source_table%' AS error_log_has_system_message
FROM `isbert_metadata.job_error_log`
WHERE job_name = 'k_ausd_bp_ta_bcp_iccid' AND entry_no = '88888';
```

---

## Section 4: Orchestration & Airflow Integration

### Test Case 4.1: Airflow DAG Parameter Rendering and Execution
#### Purpose
Verify that the Airflow DAG (`dag_k_ausd_bp_ta_bcp_iccid.py`) correctly parses execution dates, formats them to the legacy `DDMMYYYY` string format, and passes them as named parameters to the BigQuery stored procedure.

#### Setup
Install `pytest` and `apache-airflow` in the test environment. Place the DAG file in the test DAGs folder.

#### Action
Run the following Python unit test to validate DAG compilation and parameter rendering:

```python
# test_dag_k_ausd_bp_ta_bcp_iccid.py
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from datetime import datetime

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="gcp_migration/airflow/dags", include_examples=False)

def test_dag_loaded(dagbag):
    """Verify that the DAG loads without import errors."""
    dag = dagbag.get_dag(dag_id="dag_k_ausd_bp_ta_bcp_iccid")
    assert dagbag.import_errors == {}
    assert dag is not None

def test_dag_parameter_rendering(dagbag):
    """Verify that the execution date is correctly formatted to DDMMYYYY."""
    dag = dagbag.get_dag(dag_id="dag_k_ausd_bp_ta_bcp_iccid")
    task = dag.get_task("run_k_ausd_bp_ta_bcp_iccid")
    
    # Create a mock execution run for 2024-12-31
    execution_date = datetime(2024, 12, 31, 12, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_id="test_run_1",
        run_type=DagRunType.MANUAL,
        execution_date=execution_date,
        state=DagRunState.RUNNING
    )
    
    # Render templates
    context = dag_run.get_template_context()
    context['run_id'] = "test_run_1"
    
    # Manually resolve the Jinja template for p_stichtag
    rendered_query = task.render_template(
        task.configuration["query"]["query"], 
        context
    )
    
    # Extract the rendered parameters from the query configuration
    params = task.configuration["query"]["queryParameters"]
    
    # Resolve the stichtag parameter template
    stichtag_param = next(p for p in params if p["name"] == "p_stichtag")
    rendered_stichtag = task.render_template(stichtag_param["parameterValue"]["value"], context)
    
    # Assertions
    assert rendered_stichtag == "31122024", f"Expected '31122024', got '{rendered_stichtag}'"
```

#### Pass/Fail Criterion
*   **Pass**: 
    1. The DAG loads with zero import errors.
    2. The Jinja template `{{ (logical_date or execution_date).strftime('%d%m%Y') }}` correctly renders `2024-12-31` as `31122024`.
*   **Fail**: The DAG fails to load, or the date format renders incorrectly (e.g., as `2024-12-31` or `31-12-2024`).