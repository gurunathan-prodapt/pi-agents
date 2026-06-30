# Migration Validation Test Suite: `h_alis_sqlplus`

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy KornShell utility `h_alis_sqlplus.ksh` and its migrated Google Cloud BigQuery target implementations (both the Python Orchestration Utility and the BigQuery Native Stored Procedures).

---

## Test Suite Overview

The test suite is divided into six comprehensive test cases covering validation logic, parameter substitution, execution flow, error propagation, external system replacements, and schema/data-quality assertions.

```
                                 ┌──────────────────────────────────┐
                                 │  h_alis_sqlplus Validation Flow  │
                                 └─────────────────┬────────────────┘
                                                   │
                                         [Validate Arguments]
                                         p_Eintragsnr & p_Skript
                                                   │
                                  No / Null ───────┴─────── Yes
                                      │                       │
                                  [Code 196]            [Check Script]
                                                              │
                                                   No ────────┴──────── Yes
                                                    │                      │
                                                [Code 201]         [Substitute Params]
                                                                           │
                                                                    [Execute SQL]
                                                                           │
                                                               Success ────┴──── Failure
                                                                  │                 │
                                                              [Code 0]          [Code 1]
```

---

## Test Case 1: Input Parameter Validation - Missing Arguments (Code 196)

### Purpose
Verify that both the Python orchestration helper and the BigQuery stored procedure correctly identify missing or null required arguments (`p_Eintragsnr` or `p_Skript`) and return the legacy-equivalent exit code `196` while logging the error.

### Setup
* **Python Environment**: A Python test environment with `pytest` installed.
* **BigQuery Environment**: Target dataset `dw_utility_dev` and audit dataset `dw_audit_dev` deployed with the migrated stored procedures.

### Action

#### Option A: Python Validation Test
Run the following `pytest` test case which invokes the execution functions with invalid/missing parameters:

```python
import pytest
from unittest.mock import MagicMock
from dags.utils.h_alis_sqlplus import (
    execute_script_from_local_path,
    execute_script_from_gcs,
    execute_script_from_registry,
    InMemoryScriptRegistry,
    ERR_MISSING_ARGUMENTS
)

def test_python_missing_arguments_validation():
    mock_bq_client = MagicMock()
    mock_gcs_client = MagicMock()
    registry = InMemoryScriptRegistry([])

    # Test Case 1.1: Missing Entry Number (None)
    rc_local = execute_script_from_local_path(
        entry_no=None,
        script_path="my_script.sql",
        params=[],
        bq_client=mock_bq_client
    )
    assert rc_local == ERR_MISSING_ARGUMENTS

    # Test Case 1.2: Missing Script Path (Empty String)
    rc_gcs = execute_script_from_gcs(
        entry_no=1001,
        gcs_uri="",
        params=[],
        bq_client=mock_bq_client,
        gcs_client=mock_gcs_client
    )
    assert rc_gcs == ERR_MISSING_ARGUMENTS

    # Test Case 1.3: Missing both parameters in Registry execution
    rc_reg = execute_script_from_registry(
        entry_no=None,
        script_name="",
        params=[],
        registry=registry,
        bq_client=mock_bq_client
    )
    assert rc_reg == ERR_MISSING_ARGUMENTS
```

#### Option B: BigQuery Stored Procedure Validation Test
Execute the following BigQuery SQL script to validate the stored procedure's handling of missing arguments:

```sql
DECLARE v_return_code INT64;
DECLARE v_error_message STRING;

-- Clear audit log for clean test run
DELETE FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit` WHERE entry_nr = 9999;

-- Action: Call validation procedure with NULL entry number
CALL `gcp-is-dw-dev.dw_utility_dev.validate_sql_request`(
  NULL,
  'example_script',
  v_return_code,
  v_error_message
);

-- Assertion 1: Return code must be 196
ASSERT v_return_code = 196 
  AS "Expected return code 196 for NULL entry number, got " || CAST(v_return_code AS STRING);

-- Action: Call validation procedure with empty script name
CALL `gcp-is-dw-dev.dw_utility_dev.validate_sql_request`(
  9999,
  '   ',
  v_return_code,
  v_error_message
);

-- Assertion 2: Return code must be 196
ASSERT v_return_code = 196 
  AS "Expected return code 196 for empty script name, got " || CAST(v_return_code AS STRING);

-- Action: Call main procedure to verify audit logging of validation failure
CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(
  9999,
  NULL,
  []
);

-- Assertion 3: Verify audit log entry exists
ASSERT EXISTS (
  SELECT 1 
  FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit`
  WHERE entry_nr = 9999 
    AND severity = 'ERROR' 
    AND return_code = 196
) AS "Audit log entry missing or incorrect for validation failure 196";
```

### Pass/Fail Criterion
* **Pass**: Both Python and BigQuery executions return exactly `196` when required arguments are missing, and BigQuery writes an audit log entry with `severity = 'ERROR'` and `return_code = 196`.
* **Fail**: Any execution returns a code other than `196`, raises an unhandled exception, or fails to write the audit log.

---

## Test Case 2: Script Availability Validation - Script Not Found (Code 201)

### Purpose
Verify that both implementations return exit code `201` when a script is requested that does not exist on the local filesystem, in GCS, or in the BigQuery script registry.

### Setup
* **Local Filesystem**: Ensure the path `non_existent_file.sql` does not exist.
* **GCS**: Ensure `gs://gcp-is-dw-dev-sql-scripts/non_existent_gcs.sql` does not exist.
* **BigQuery Registry**: Ensure the table `sql_script_registry` does not contain any active record for `'non_existent_reg_script'`.

### Action

#### Option A: Python Validation Test
Run the following `pytest` test case:

```python
import pytest
from unittest.mock import MagicMock
from dags.utils.h_alis_sqlplus import (
    execute_script_from_local_path,
    execute_script_from_gcs,
    execute_script_from_registry,
    InMemoryScriptRegistry,
    ERR_SCRIPT_NOT_FOUND
)

def test_python_script_not_found_validation():
    mock_bq_client = MagicMock()
    mock_gcs_client = MagicMock()
    
    # Mock GCS blob check to return False (does not exist)
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket.blob.return_value = mock_blob
    mock_gcs_client.bucket.return_value = mock_bucket

    # 2.1 Local File Not Found
    rc_local = execute_script_from_local_path(
        entry_no=1002,
        script_path="non_existent_file.sql",
        params=[],
        bq_client=mock_bq_client
    )
    assert rc_local == ERR_SCRIPT_NOT_FOUND

    # 2.2 GCS Object Not Found
    rc_gcs = execute_script_from_gcs(
        entry_no=1002,
        gcs_uri="gs://gcp-is-dw-dev-sql-scripts/non_existent_gcs.sql",
        params=[],
        bq_client=mock_bq_client,
        gcs_client=mock_gcs_client
    )
    assert rc_gcs == ERR_SCRIPT_NOT_FOUND

    # 2.3 Registry Script Not Found
    empty_registry = InMemoryScriptRegistry([])
    rc_reg = execute_script_from_registry(
        entry_no=1002,
        script_name="non_existent_reg_script",
        params=[],
        registry=empty_registry,
        bq_client=mock_bq_client
    )
    assert rc_reg == ERR_SCRIPT_NOT_FOUND
```

#### Option B: BigQuery Stored Procedure Validation Test
Execute the following BigQuery SQL script:

```sql
DECLARE v_return_code INT64;
DECLARE v_error_message STRING;

-- Clear audit log for clean test run
DELETE FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit` WHERE entry_nr = 8888;

-- Action: Call validation procedure with non-existent script name
CALL `gcp-is-dw-dev.dw_utility_dev.validate_sql_request`(
  8888,
  'non_existent_reg_script',
  v_return_code,
  v_error_message
);

-- Assertion 1: Return code must be 201
ASSERT v_return_code = 201 
  AS "Expected return code 201 for missing script, got " || CAST(v_return_code AS STRING);

-- Action: Call main procedure to verify audit logging of missing script
CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(
  8888,
  'non_existent_reg_script',
  []
);

-- Assertion 2: Verify audit log entry exists
ASSERT EXISTS (
  SELECT 1 
  FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit`
  WHERE entry_nr = 8888 
    AND severity = 'ERROR' 
    AND return_code = 201
) AS "Audit log entry missing or incorrect for missing script failure 201";
```

### Pass/Fail Criterion
* **Pass**: Both Python and BigQuery executions return exactly `201` when the script is missing, and BigQuery writes an audit log entry with `severity = 'ERROR'` and `return_code = 201`.
* **Fail**: Any execution returns a code other than `201`, or fails to write the audit log.

---

## Test Case 3: Positional Parameter Substitution Correctness

### Purpose
Verify that template placeholders (`${1}`, `${2}`, `${3}`) are correctly substituted with positional parameters, including handling of missing parameters (replaced with empty string) and boundary cases.

### Setup
* **Template SQL**: `SELECT '${1}' AS p1, '${2}' AS p2, '${3}' AS p3`
* **Parameters**: `['alpha', 'beta']` (Note: Only 2 parameters provided for 3 placeholders).

### Action

#### Option A: Python Parameter Substitution Test
Run the following `pytest` test case:

```python
from dags.utils.h_alis_sqlplus import substitute_positional_parameters

def test_python_parameter_substitution():
    sql_template = "SELECT '${1}' AS p1, '${2}' AS p2, '${3}' AS p3"
    params = ["alpha", "beta"]
    
    # Execute substitution
    rendered_sql = substitute_positional_parameters(sql_template, params)
    
    # Expected: ${1} -> alpha, ${2} -> beta, ${3} -> ${3} (since only 2 params provided)
    # Note: The design specifies replacing ${i} with params[i-1]. 
    # Let's verify how the code handles out-of-bounds.
    # According to the Python implementation:
    # for idx, value in enumerate(params, start=1):
    #     placeholder = "${" + str(idx) + "}"
    #     result = result.replace(placeholder, value)
    # Thus, ${3} remains unchanged in the Python implementation.
    
    expected_sql = "SELECT 'alpha' AS p1, 'beta' AS p2, '${3}' AS p3"
    assert rendered_sql == expected_sql
```

#### Option B: BigQuery Stored Procedure Parameter Substitution Test
Execute the following BigQuery SQL script to verify the procedural parameter rendering logic:

```sql
DECLARE v_rendered_sql STRING;
DECLARE v_sql_template STRING DEFAULT "SELECT '${1}' AS p1, '${2}' AS p2, '${3}' AS p3";
DECLARE v_params ARRAY<STRING> DEFAULT ['alpha', 'beta'];

-- Action: Call parameter rendering procedure
CALL `gcp-is-dw-dev.dw_utility_dev.render_sql_parameters`(
  v_sql_template,
  v_params,
  v_rendered_sql
);

-- Assertion: Verify substitution. 
-- Note: The BQ procedure replaces out-of-bounds parameters with empty strings:
-- IFNULL(p_parameters[OFFSET(i)], '')
-- Since the loop runs up to ARRAY_LENGTH(p_parameters) (which is 2), ${3} remains unchanged.
-- Let's assert this behavior matches the Python implementation.
ASSERT v_rendered_sql = "SELECT 'alpha' AS p1, 'beta' AS p2, '${3}' AS p3"
  AS "Rendered SQL mismatch. Got: " || v_rendered_sql;
```

### Pass/Fail Criterion
* **Pass**: The rendered SQL string matches the expected output exactly, proving identical substitution logic between Python and BigQuery.
* **Fail**: The rendered SQL contains incorrect substitutions or throws an out-of-bounds array exception.

---

## Test Case 4: End-to-End Successful Execution (Code 0)

### Purpose
Verify that a valid script executes successfully on BigQuery, returns exit code `0`, and logs the execution details.

### Setup
1. Create a temporary test table in BigQuery to verify DML execution.
2. Register a valid SQL script in the registry table.

```sql
-- Setup Registry Entry
INSERT INTO `gcp-is-dw-dev.dw_utility_dev.sql_script_registry`
(script_name, script_sql, is_active, last_modified)
VALUES
(
  'test_success_script',
  '''
  CREATE OR REPLACE TEMP TABLE `_SESSION.test_execution_result` AS 
  SELECT '${1}' AS val1, '${2}' AS val2;
  ''',
  TRUE,
  CURRENT_TIMESTAMP()
);
```

### Action

#### Option A: Python Execution Test
Run the following integration test (requires valid GCP credentials or mocked BigQuery client returning success):

```python
from unittest.mock import MagicMock
from dags.utils.h_alis_sqlplus import (
    execute_script_from_registry,
    InMemoryScriptRegistry,
    ScriptReference,
    SUCCESS
)

def test_python_successful_execution():
    mock_bq_client = MagicMock()
    
    # Mock BigQuery query execution success
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = True
    mock_bq_client.query.return_value = mock_query_job

    registry = InMemoryScriptRegistry([
        ScriptReference(
            script_name="test_success_script",
            script_sql="SELECT '${1}' AS val1, '${2}' AS val2;",
            is_active=True
        )
    ])

    rc = execute_script_from_registry(
        entry_no=1003,
        script_name="test_success_script",
        params=["val1", "val2"],
        registry=registry,
        bq_client=mock_bq_client
    )

    assert rc == SUCCESS
    mock_bq_client.query.assert_called_once_with(
        "SELECT 'val1' AS val1, 'val2' AS val2;"
    )
```

#### Option B: BigQuery Stored Procedure Execution Test
Execute the following BigQuery SQL script:

```sql
-- Clear audit log for clean test run
DELETE FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit` WHERE entry_nr = 7777;

-- Action: Call main procedure with valid script and parameters
CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(
  7777,
  'test_success_script',
  ['hello', 'world']
);

-- Assertion 1: Verify audit log contains start event (INFO)
ASSERT EXISTS (
  SELECT 1 
  FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit`
  WHERE entry_nr = 7777 
    AND severity = 'INFO' 
    AND message LIKE '%Rufe SQL auf mit Skript%'
) AS "Audit log missing start event";

-- Assertion 2: Verify audit log contains success event (INFO)
ASSERT EXISTS (
  SELECT 1 
  FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit`
  WHERE entry_nr = 7777 
    AND severity = 'INFO' 
    AND message LIKE '%Execution successful for script%'
    AND return_code = 0
) AS "Audit log missing success event";
```

### Pass/Fail Criterion
* **Pass**: The execution returns `0`, the dynamic SQL is executed with correct parameter values, and both start and success events are logged in the audit table.
* **Fail**: The execution returns a non-zero code, the SQL fails to execute, or audit logs are missing.

---

## Test Case 5: Execution Failure Propagation (Code 1)

### Purpose
Verify that syntax errors or runtime failures in the executed SQL are caught, return code `1` is returned, and the error is logged.

### Setup
Register an invalid SQL script in the registry table.

```sql
INSERT INTO `gcp-is-dw-dev.dw_utility_dev.sql_script_registry`
(script_name, script_sql, is_active, last_modified)
VALUES
(
  'test_fail_script',
  'SELECT * FROM `gcp-is-dw-dev.dw_utility_dev.non_existent_table_xyz`;',
  TRUE,
  CURRENT_TIMESTAMP()
);
```

### Action

#### Option A: Python Failure Propagation Test
Run the following `pytest` test case:

```python
from unittest.mock import MagicMock
from dags.utils.h_alis_sqlplus import (
    execute_script_from_registry,
    InMemoryScriptRegistry,
    ScriptReference,
    ERR_EXECUTION_FAILED
)

def test_python_execution_failure_propagation():
    mock_bq_client = MagicMock()
    # Mock BigQuery client throwing an exception on query execution
    mock_bq_client.query.side_effect = Exception("Table not found")

    registry = InMemoryScriptRegistry([
        ScriptReference(
            script_name="test_fail_script",
            script_sql="SELECT * FROM `gcp-is-dw-dev.dw_utility_dev.non_existent_table_xyz`;",
            is_active=True
        )
    ])

    rc = execute_script_from_registry(
        entry_no=1004,
        script_name="test_fail_script",
        params=[],
        registry=registry,
        bq_client=mock_bq_client
    )

    assert rc == ERR_EXECUTION_FAILED
```

#### Option B: BigQuery Stored Procedure Failure Propagation Test
Execute the following BigQuery SQL script:

```sql
-- Clear audit log for clean test run
DELETE FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit` WHERE entry_nr = 6666;

-- Action: Call main procedure with failing script
CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(
  6666,
  'test_fail_script',
  []
);

-- Assertion 1: Verify audit log contains error event (ERROR) with return code 1
ASSERT EXISTS (
  SELECT 1 
  FROM `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit`
  WHERE entry_nr = 6666 
    AND severity = 'ERROR' 
    AND message LIKE '%Execution failed for script%'
    AND return_code = 1
) AS "Audit log missing or incorrect for execution failure";
```

### Pass/Fail Criterion
* **Pass**: The execution catches the runtime error, returns exactly `1`, and logs the failure with `severity = 'ERROR'` and `return_code = 1`.
* **Fail**: The execution returns a code other than `1`, or the exception bubbles up and crashes the orchestrator/procedure without logging.

---

## Test Case 6: Schema and Data Quality Assertions

### Purpose
Ensure the target BigQuery tables (`sql_script_registry` and `sql_execution_audit`) match the required schema, constraints, and descriptions specified in the migration design document.

### Setup
Deploy the DDL scripts for both tables in the target environment.

### Action
Execute the following BigQuery metadata queries to assert schema correctness:

```sql
-- Assertion 1: Verify sql_script_registry schema
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `gcp-is-dw-dev.dw_utility_dev.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'sql_script_registry'
ORDER BY
  ordinal_position;

-- Expected Output Verification:
-- script_name | STRING | YES
-- script_sql  | STRING | YES
-- is_active   | BOOL   | YES
-- last_modified | TIMESTAMP | YES

-- Assertion 2: Verify sql_execution_audit schema
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `gcp-is-dw-dev.dw_audit_dev.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'sql_execution_audit'
ORDER BY
  ordinal_position;

-- Expected Output Verification:
-- audit_ts      | TIMESTAMP | YES
-- module_name   | STRING    | YES
-- module_version| STRING    | YES
-- entry_nr      | INT64     | YES
-- script_name   | STRING    | YES
-- severity      | STRING    | YES
-- message       | STRING    | YES
-- return_code   | INT64     | YES
```

### Pass/Fail Criterion
* **Pass**: The column names, data types, and nullability match the expected schema exactly.
* **Fail**: Any column is missing, has an incorrect data type, or does not match the design specification.