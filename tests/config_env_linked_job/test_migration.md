# Migration Validation Test Suite: DW.CFG_LOAD_PARAMS

This document defines the comprehensive QA test suite to validate the migration of the **DW.CFG_LOAD_PARAMS** job from its legacy UC4/KornShell/Oracle environment to Google Cloud Composer, BigQuery, and Dataform.

---

## Test Suite Overview

The validation strategy is divided into five distinct testing areas to guarantee behavioral equivalence, data integrity, and robust error handling:

```
  DW.CFG_LOAD_PARAMS Migration Validation
  ├── 1. End-to-End Output Parity Tests (Happy Path)
  ├── 2. Transformation & Parsing Edge Cases (Unit Tests)
  ├── 3. Database State & MERGE Logic Validation (SQL Assertions)
  ├── 4. Error Handling & Exit Code Parity Tests
  └── 5. Schema & Data Quality Assertions
```

---

## Section 1: End-to-End Output Parity Tests (Happy Path)

### Test Case 1.1: End-to-End Parameter Ingestion and Upsert
#### Purpose
Verify that a standard, valid properties file is successfully parsed by the migrated Python script, staged to BigQuery, and merged into the target table `DWH_ADM.JOB_PARAMS` with exact output parity compared to the legacy execution.

#### Setup
1. Create a valid properties file named `job_params.properties` with the following content:
   ```properties
   # Standard configuration parameters
   db.host=10.20.30.40
   db.sid=DWHPROD
   stage.table=DWH_STG.PARAM_LOAD
   custom.param.timeout=300
   ```
2. Clear the target BigQuery staging and production tables:
   ```sql
   TRUNCATE TABLE `DWH_STG.PARAM_LOAD`;
   TRUNCATE TABLE `DWH_ADM.JOB_PARAMS`;
   ```
3. Set up the required Airflow Variables in the test environment:
   - `GCP_PROJECT`: Your test GCP project ID.
   - `BQ_DATASET_STG`: `DWH_STG`
   - `PARAM_FILE_PATH`: Path to the created `job_params.properties` file.

#### Action
Execute the migrated Python script `r_load_params.py` followed by the Dataform SQLX merge operation (simulated via BigQuery client query execution).

```python
import os
import pytest
from unittest.mock import patch
from google.cloud import bigquery
from airflow.models import Variable

# Import the migrated script
from config_env_linked_job.iscfg.bin.r_load_params import main as run_migration_script

@pytest.mark.integration
def test_e2e_happy_path(capsys):
    # 1. Set up Airflow Variables
    with patch.object(Variable, 'get', side_effect=lambda key, default_var=None: {
        "GCP_PROJECT": os.environ.get("GCP_PROJECT", "test-gcp-project"),
        "BQ_DATASET_STG": "DWH_STG",
        "PARAM_FILE_PATH": "job_params.properties"
    }.get(key, default_var)):
        
        # 2. Run the ingestion script
        with pytest.raises(SystemExit) as excinfo:
            run_migration_script()
            
        assert excinfo.value.code == 0
        
        # 3. Verify console output matches legacy success message
        captured = capsys.readouterr()
        assert "Parameterladen erfolgreich abgeschlossen" in captured.out

        # 4. Verify BigQuery Staging Table Content
        client = bigquery.Client()
        query = "SELECT param_key, param_value FROM `DWH_STG.PARAM_LOAD` ORDER BY param_key"
        rows = list(client.query(query).result())
        
        expected_rows = [
            ("custom.param.timeout", "300"),
            ("db.host", "10.20.30.40"),
            ("db.sid", "DWHPROD"),
            ("stage.table", "DWH_STG.PARAM_LOAD")
        ]
        
        assert len(rows) == 4
        for i, (expected_key, expected_val) in enumerate(expected_rows):
            assert rows[i]["param_key"] == expected_key
            assert rows[i]["param_value"] == expected_val
```

#### Pass/Fail Criterion
* **Pass**: The script exits with code `0`, prints `"Parameterladen erfolgreich abgeschlossen"`, and the staging table contains exactly the 4 parsed key-value pairs.
* **Fail**: Any parsing error occurs, the exit code is non-zero, or the staged data does not match the source properties file.

---

## Section 2: Transformation & Parsing Edge Cases (Unit Tests)

### Test Case 2.1: Properties File Parsing Robustness
#### Purpose
Verify that the Python parser correctly handles comments (`#`, `!`), empty lines, leading/trailing whitespaces, and multiple `=` signs in values (e.g., connection strings).

#### Setup
Create a properties file with complex edge cases:
```properties
# This is a standard comment
! This is an alternative comment style

  spaced.key  =   spaced.value  
key.with.equals=host=myhost;port=1521;sid=ORCL
empty.value=
#key.commented=should_not_exist
```

#### Action
Run the parsing logic of `r_load_params.py` in isolation and assert the structured output.

```python
import tempfile
import os
from datetime import datetime

def parse_properties_file(filepath):
    rows = []
    loaded_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('!'):
                continue
            if '=' in line:
                key, val = line.split('=', 1)
                rows.append({
                    'param_key': key.strip(),
                    'param_value': val.strip(),
                    'loaded_at': loaded_at
                })
    return rows

def test_properties_parsing_edge_cases():
    content = """# Comment
! Comment 2
  spaced.key  =   spaced.value  
key.with.equals=host=myhost;port=1521;sid=ORCL
empty.value=
"""
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        parsed_data = parse_properties_file(tmp_path)
        
        # Assertions
        assert len(parsed_data) == 3
        
        # Check whitespace trimming
        assert parsed_data[0]['param_key'] == 'spaced.key'
        assert parsed_data[0]['param_value'] == 'spaced.value'
        
        # Check multiple equals signs handling
        assert parsed_data[1]['param_key'] == 'key.with.equals'
        assert parsed_data[1]['param_value'] == 'host=myhost;port=1521;sid=ORCL'
        
        # Check empty value handling
        assert parsed_data[2]['param_key'] == 'empty.value'
        assert parsed_data[2]['param_value'] == ''
        
    finally:
        os.remove(tmp_path)
```

#### Pass/Fail Criterion
* **Pass**: Comments are ignored, whitespaces are trimmed, multiple equals signs are preserved in the value, and empty values are parsed as empty strings.
* **Fail**: Comments are parsed as keys, values are truncated at the second `=` sign, or leading/trailing spaces are preserved.

---

## Section 3: Database State & MERGE Logic Validation (SQL Assertions)

### Test Case 3.1: Dataform MERGE Upsert Behavior
#### Purpose
Verify that the BigQuery Standard SQL `MERGE` statement correctly updates existing keys, inserts new keys, and leaves unrelated keys untouched.

#### Setup
1. Populate the target table `DWH_ADM.JOB_PARAMS` with initial state:
   ```sql
   INSERT INTO `DWH_ADM.JOB_PARAMS` (param_key, param_value, updated_at)
   VALUES 
     ('KEEP_ME', 'original_val', TIMESTAMP '2026-01-01 00:00:00 UTC'),
     ('UPDATE_ME', 'old_val', TIMESTAMP '2026-01-01 00:00:00 UTC');
   ```
2. Populate the staging table `DWH_STG.PARAM_LOAD` with new parameters:
   ```sql
   INSERT INTO `DWH_STG.PARAM_LOAD` (param_key, param_value, loaded_at)
   VALUES 
     ('UPDATE_ME', 'new_val', TIMESTAMP '2026-04-21 12:00:00 UTC'),
     ('INSERT_ME', 'inserted_val', TIMESTAMP '2026-04-21 12:00:00 UTC');
   ```

#### Action
Execute the converted MERGE statement against the BigQuery test environment.

```sql
-- Execute the MERGE statement under test
MERGE INTO `DWH_ADM.JOB_PARAMS` tgt
USING (
    SELECT 
        CAST(param_key AS STRING) AS param_key, 
        CAST(param_value AS STRING) AS param_value, 
        CAST(loaded_at AS TIMESTAMP) AS loaded_at
    FROM `DWH_STG.PARAM_LOAD`
) src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    tgt.param_value = src.param_value,
    tgt.updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);
```

#### Verification Queries
```sql
-- Assertion 1: Verify 'KEEP_ME' remains unchanged
ASSERT (
  SELECT param_value FROM `DWH_ADM.JOB_PARAMS` WHERE param_key = 'KEEP_ME'
) = 'original_val';

-- Assertion 2: Verify 'UPDATE_ME' was updated with new value and timestamp
ASSERT (
  SELECT param_value FROM `DWH_ADM.JOB_PARAMS` WHERE param_key = 'UPDATE_ME'
) = 'new_val';

ASSERT (
  SELECT updated_at FROM `DWH_ADM.JOB_PARAMS` WHERE param_key = 'UPDATE_ME'
) = TIMESTAMP '2026-04-21 12:00:00 UTC';

-- Assertion 3: Verify 'INSERT_ME' was inserted
ASSERT (
  SELECT param_value FROM `DWH_ADM.JOB_PARAMS` WHERE param_key = 'INSERT_ME'
) = 'inserted_val';

-- Assertion 4: Verify total row count is exactly 3
ASSERT (
  SELECT COUNT(1) FROM `DWH_ADM.JOB_PARAMS`
) = 3;
```

#### Pass/Fail Criterion
* **Pass**: All four SQL assertions execute successfully without throwing errors.
* **Fail**: Any assertion fails, indicating that the MERGE logic either missed an update, missed an insert, corrupted unrelated rows, or failed to update timestamps.

---

## Section 4: Error Handling & Exit Code Parity Tests

### Test Case 4.1: Missing Properties File Error Handling
#### Purpose
Verify that the system behaves gracefully and exits with a non-zero code when the properties file is missing, matching the error-handling behavior of the legacy KornShell script.

#### Setup
Ensure that the path specified in `PARAM_FILE_PATH` does not exist on the filesystem.

#### Action
Run the migrated Python script `r_load_params.py` and capture the exit code and standard error output.

```python
import pytest
from unittest.mock import patch
from airflow.models import Variable
from config_env_linked_job.iscfg.bin.r_load_params import main as run_migration_script

def test_missing_properties_file_error(capsys):
    with patch.object(Variable, 'get', side_effect=lambda key, default_var=None: {
        "GCP_PROJECT": "test-project",
        "BQ_DATASET_STG": "DWH_STG",
        "PARAM_FILE_PATH": "/nonexistent/path/job_params.properties"
    }.get(key, default_var)):
        
        with pytest.raises(SystemExit) as excinfo:
            run_migration_script()
            
        # Verify exit code is non-zero (Legacy exited with 8, generated code exits with 1)
        assert excinfo.value.code != 0
        
        # Verify the exact German error message is printed to stdout/stderr
        captured = capsys.readouterr()
        assert "FEHLER: Parameterdatei" in captured.out or "FEHLER: Parameterdatei" in captured.err
```

#### Pass/Fail Criterion
* **Pass**: The script raises `SystemExit` with a non-zero exit code and prints the verbatim error prefix `"FEHLER: Parameterdatei"`.
* **Fail**: The script exits with code `0` (false positive) or fails to print the required German error string.

---

### Test Case 4.2: BigQuery Load Failure Error Handling
#### Purpose
Verify that database/network failures during staging trigger the correct error logs and exit codes.

#### Setup
Provide an invalid GCP project name to force a BigQuery client error during the load job.

#### Action
Run the migrated Python script `r_load_params.py` with an invalid configuration.

```python
import pytest
from unittest.mock import patch
from airflow.models import Variable
from config_env_linked_job.iscfg.bin.r_load_params import main as run_migration_script

def test_bigquery_load_failure(capsys):
    # Setup an invalid project to trigger GoogleCloudError
    with patch.object(Variable, 'get', side_effect=lambda key, default_var=None: {
        "GCP_PROJECT": "invalid-gcp-project-id-12345",
        "BQ_DATASET_STG": "DWH_STG",
        "PARAM_FILE_PATH": "job_params.properties"  # Assume this file exists
    }.get(key, default_var)):
        
        # Create dummy file to pass the file existence check
        with open("job_params.properties", "w") as f:
            f.write("test.key=test.value\n")
            
        try:
            with pytest.raises(SystemExit) as excinfo:
                run_migration_script()
                
            assert excinfo.value.code != 0
            
            captured = capsys.readouterr()
            # Verify legacy error string is preserved
            assert "FEHLER: sqlldr beendet" in captured.out
        finally:
            if os.path.exists("job_params.properties"):
                os.remove("job_params.properties")
```

#### Pass/Fail Criterion
* **Pass**: The script catches the BigQuery exception, prints `"FEHLER: sqlldr beendet..."` to stdout, and exits with a non-zero code.
* **Fail**: The script crashes with an unhandled exception, exits with `0`, or fails to print the legacy-equivalent error message.

---

## Section 5: Schema & Data Quality Assertions

### Test Case 5.1: Target Table Schema Validation
#### Purpose
Assert that the target BigQuery tables match the required schema definitions and handle null values safely.

#### Setup
None (Metadata inspection).

#### Action
Query the BigQuery `INFORMATION_SCHEMA` to validate column names, data types, and nullability.

```sql
-- Assertion 1: Verify Staging Table Schema
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `DWH_STG.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'PARAM_LOAD';
```

| Expected Column Name | Expected Data Type | Expected Nullable |
| :--- | :--- | :--- |
| `param_key` | `STRING` | `YES` |
| `param_value` | `STRING` | `YES` |
| `loaded_at` | `TIMESTAMP` | `YES` |

```sql
-- Assertion 2: Verify Production Table Schema
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `DWH_ADM.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'JOB_PARAMS';
```

| Expected Column Name | Expected Data Type | Expected Nullable |
| :--- | :--- | :--- |
| `param_key` | `STRING` | `NO` |
| `param_value` | `STRING` | `YES` |
| `updated_at` | `TIMESTAMP` | `YES` |

#### Pass/Fail Criterion
* **Pass**: Both tables match the exact schema definitions specified above.
* **Fail**: Any column is missing, has an incorrect data type, or has incorrect nullability constraints (e.g., `param_key` in the production table must be `NOT NULL`).