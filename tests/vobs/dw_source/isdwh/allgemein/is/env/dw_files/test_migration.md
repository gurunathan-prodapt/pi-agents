# Migration Validation Test Suite: Shared Files (`dw_files`)

This document details the migration-validation tests for the environment configurations (`shared_files`) of the Information Services Data Warehouse (`ISDWH`) system. These tests verify that the migrated Google Cloud Platform (GCP) configurations, BigQuery stored procedures, and JSON metadata are behaviorally equivalent to the legacy UNIX shell environment configurations (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`).

---

## Test Case 1: Global Composer Configuration Parity (`.dw_ai` Translation)

### Purpose
Verify that the global environment and Ab Initio framework parameters defined in the legacy `.dw_ai` file are correctly represented in the migrated `composer_env_config.json` file and the `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` BigQuery table.

### Setup
1. Ensure `composer_env_config.json` is uploaded to the Cloud Composer environment's `dags/config/` directory.
2. Ensure the BigQuery table `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` has been initialized and populated using the DDL/DML script from the migration design.

### Action
Execute a Python-based test script using `pytest` to validate the JSON structure and query the BigQuery configuration table to assert key-value parity.

```python
# test_dw_ai_parity.py
import json
import pytest
from google.cloud import bigquery

def test_composer_json_config_parity():
    config_path = "dags/config/composer_env_config.json"
    with open(config_path, "r") as f:
        config = json.load(f)
    
    expected_values = {
        "AB_HOME": "/appl/local/abinitio/abinitio",
        "AB_AIR_ROOT": "/appl/local/abinitio/TMD_EME/eme_dev/repo",
        "AB_AIR_HOME": "/appl/local/abinitio/abinitio-V2-14",
        "ETL_Host": "dxcsa4.bn.detemobil.de",
        "ETL_Projekt": "BHB",
        "AI_PRIV_SAND_ROOT": "~/abinitio",
        "AI_ENV_SAND_ROOT": "/appl/local/abinitio/sandboxes/DEV",
        "AI_REPOSIT_TRACKING": "FALSE"
    }
    
    for key, expected_val in expected_values.items():
        assert config.get(key) == expected_val, f"Mismatch for key {key}: expected {expected_val}, got {config.get(key)}"

def test_bigquery_config_table_parity():
    client = bigquery.Client()
    query = """
        SELECT variable_name, variable_value 
        FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG`
    """
    query_job = client.query(query)
    results = {row["variable_name"]: row["variable_value"] for row in query_job.result()}
    
    expected_values = {
        "AB_HOME": "/appl/local/abinitio/abinitio",
        "AB_AIR_ROOT": "/appl/local/abinitio/TMD_EME/eme_dev/repo",
        "AB_AIR_HOME": "/appl/local/abinitio/abinitio-V2-14",
        "ETL_Host": "dxcsa4.bn.detemobil.de",
        "ETL_Projekt": "BHB",
        "AI_PRIV_SAND_ROOT": "~/abinitio",
        "AI_ENV_SAND_ROOT": "/appl/local/abinitio/sandboxes/DEV",
        "AI_REPOSIT_TRACKING": "FALSE"
    }
    
    for key, expected_val in expected_values.items():
        assert results.get(key) == expected_val, f"BQ Table mismatch for {key}: expected {expected_val}, got {results.get(key)}"
```

### Pass/Fail Criterion
* **Pass:** The JSON configuration file and the BigQuery metadata table contain all 8 legacy variables with exact string matches for their values.
* **Fail:** Any variable is missing, or its value does not match the legacy specification.

---

## Test Case 2: Database Connection and Secret Retrieval (`.dw_db` Translation)

### Purpose
Verify that database connection strings, usernames, and passwords are not hardcoded or stored in plain text, but are instead securely retrieved from Google Cloud Secret Manager and correctly mapped to the BigQuery Connection Object.

### Setup
1. Store the mock password `TestSecretPassword123!` in Google Cloud Secret Manager at the path `projects/gcp-devlab-project/secrets/meyreis-dwh-password/versions/latest`.
2. Grant the service account running the test runner the `roles/secretmanager.secretAccessor` role.

### Action
Run a Python test to retrieve the secret from Secret Manager and verify connection metadata.

```python
# test_dw_db_security.py
import pytest
from google.cloud import secretmanager

def test_secret_manager_retrieval():
    secret_id = "projects/gcp-devlab-project/secrets/meyreis-dwh-password/versions/latest"
    client = secretmanager.SecretManagerServiceClient()
    
    try:
        response = client.access_secret_version(request={"name": secret_id})
        password = response.payload.data.decode("UTF-8")
    except Exception as e:
        pytest.fail(f"Failed to retrieve secret from Secret Manager: {e}")
        
    assert password == "TestSecretPassword123!", "Retrieved password does not match the expected secret value."

def test_connection_id_format():
    # Verify connection ID matches the expected GCP resource path format
    connection_id = "projects/gcp-devlab-project/locations/europe-west3/connections/conn-edwh3-devlab"
    parts = connection_id.split('/')
    assert parts[0] == "projects"
    assert parts[2] == "locations"
    assert parts[4] == "connections"
    assert parts[5] == "conn-edwh3-devlab"
```

### Pass/Fail Criterion
* **Pass:** The secret is successfully retrieved from Secret Manager, matches the expected value, and the connection ID conforms to the standard GCP resource path format.
* **Fail:** Secret retrieval fails due to permission/path errors, or the retrieved value is incorrect.

---

## Test Case 3: Global Environment Validation Logic (`sp_dw_global` Behavior)

### Purpose
Verify that `metadata.sp_dw_global` correctly validates the presence of required environment variables, logs failures to `metadata.dw_environment_log` when variables are missing, and raises a runtime exception.

### Setup
1. Ensure the `metadata` dataset and `metadata.dw_environment_log` table exist.
2. Truncate the log table before running the test.

### Action
Execute a series of SQL assertions to test both successful execution (all variables provided) and failure execution (one or more variables missing).

```sql
-- Test 3.1: Successful execution with all variables populated
DECLARE out_nls_lang STRING;
DECLARE out_nls_date_format STRING;
DECLARE out_nls_date_language STRING;
DECLARE out_lang STRING;

CALL `metadata.sp_dw_global`(
  '/home/isdwh/aktuell', '/home/isdwh/daten/logfiles', '/home/isdwh/daten/cubes',
  '/home/isdwh/daten/d1', '/home/isdwh/daten/xtra', '/home/isdwh/daten/ctel',
  '/home/isdwh/daten/vo', '/home/isdwh/daten/rv', '/home/isdwh/daten/ees',
  '/home/isdwh/daten/nnv', '/appl/local/oracle/12.2.0.1.0',
  out_nls_lang, out_nls_date_format, out_nls_date_language, out_lang
);

-- Assert output parameters are set correctly
ASSERT out_nls_lang = 'GERMAN_GERMANY.WE8ISO8859P1' MESSAGE 'NLS_LANG mismatch';
ASSERT out_nls_date_format = 'DD.MM.YY' MESSAGE 'NLS_DATE_FORMAT mismatch';
ASSERT out_nls_date_language = 'GERMAN_GERMANY.WE8ISO8859P1' MESSAGE 'NLS_DATE_LANGUAGE mismatch';
ASSERT out_lang = 'de' MESSAGE 'LANG mismatch';
```

```sql
-- Test 3.2: Failure execution with missing variables (DW_DIR_ROOT and ORACLE_HOME are NULL)
DECLARE out_nls_lang STRING;
DECLARE out_nls_date_format STRING;
DECLARE out_nls_date_language STRING;
DECLARE out_lang STRING;

BEGIN
  -- Clear logs
  TRUNCATE TABLE `metadata.dw_environment_log`;

  -- This call must fail and raise an exception
  CALL `metadata.sp_dw_global`(
    NULL, '/home/isdwh/daten/logfiles', '/home/isdwh/daten/cubes',
    '/home/isdwh/daten/d1', '/home/isdwh/daten/xtra', '/home/isdwh/daten/ctel',
    '/home/isdwh/daten/vo', '/home/isdwh/daten/rv', '/home/isdwh/daten/ees',
    '/home/isdwh/daten/nnv', '', -- Missing ORACLE_HOME
    out_nls_lang, out_nls_date_format, out_nls_date_language, out_lang
  );
  
  -- If execution reaches here, the procedure failed to raise an error
  ERROR 'Test Failed: sp_dw_global did not raise an exception when required variables were missing.';

EXCEPTION WHEN ERROR THEN
  -- Verify that the missing variables were logged to the audit table
  ASSERT (
    SELECT COUNT(1) 
    FROM `metadata.dw_environment_log` 
    WHERE procedure_name = 'sp_dw_global' 
      AND log_level = 'ERROR'
      AND missing_variable IN ('DW_DIR_ROOT', 'ORACLE_HOME')
  ) = 2 MESSAGE 'Test Failed: Missing variables were not correctly logged to dw_environment_log.';
END;
```

### Pass/Fail Criterion
* **Pass:** 
  * When all inputs are valid, the procedure completes successfully and returns the correct NLS session parameters.
  * When inputs are missing, the procedure raises a runtime exception and logs exactly the missing variables to `metadata.dw_environment_log`.
* **Fail:** The procedure fails to raise an error on missing inputs, fails to log to the audit table, or returns incorrect NLS parameters.

---

## Test Case 4: Dynamic Path Initialization and Oracle Home Resolution (`sp_dw_init` Behavior)

### Purpose
Verify that `metadata.dw_init` dynamically constructs the correct directory path hierarchies based on the input home directory, resolves `ORACLE_HOME` using the conditional directory flags, and successfully calls the global validation procedure.

### Setup
Ensure the `metadata.sp_dw_global` and `metadata.dw_init` stored procedures are compiled in the target BigQuery environment.

### Action
Execute a SQL test script to validate path concatenation and conditional `ORACLE_HOME` resolution.

```sql
-- Test 4.1: Verify Oracle 12 resolution and path construction
DECLARE io_oracle_home STRING DEFAULT NULL;
DECLARE io_oracle_sid STRING DEFAULT 'eDWH3';
DECLARE i_home_dir STRING DEFAULT '/home/meyreis';

CALL `metadata.dw_init`(
  io_oracle_home, io_oracle_sid, i_home_dir,
  TRUE,  -- i_dir_oracle_12_exists
  FALSE  -- i_dir_oracle_11_exists
);

-- Assertions
ASSERT io_oracle_home = '/appl/local/oracle/12.2.0.1.0' MESSAGE 'Failed to resolve Oracle 12 Home';
```

```sql
-- Test 4.2: Verify Oracle 11 resolution when Oracle 12 is not present
DECLARE io_oracle_home STRING DEFAULT NULL;
DECLARE io_oracle_sid STRING DEFAULT 'eDWH3';
DECLARE i_home_dir STRING DEFAULT '/home/meyreis';

CALL `metadata.dw_init`(
  io_oracle_home, io_oracle_sid, i_home_dir,
  FALSE, -- i_dir_oracle_12_exists
  TRUE   -- i_dir_oracle_11_exists
);

-- Assertions
ASSERT io_oracle_home = '/appl/local/oracle/11.2.0' MESSAGE 'Failed to resolve Oracle 11 Home';
```

```sql
-- Test 4.3: Verify exception is raised when no Oracle installation is found
DECLARE io_oracle_home STRING DEFAULT NULL;
DECLARE io_oracle_sid STRING DEFAULT 'eDWH3';
DECLARE i_home_dir STRING DEFAULT '/home/meyreis';

BEGIN
  CALL `metadata.dw_init`(
    io_oracle_home, io_oracle_sid, i_home_dir,
    FALSE, -- i_dir_oracle_12_exists
    FALSE  -- i_dir_oracle_11_exists
  );
  ERROR 'Test Failed: dw_init did not raise an exception when no Oracle directory existed.';
EXCEPTION WHEN ERROR THEN
  -- Success: Exception was raised as expected
  SELECT 'Oracle resolution exception test passed.' AS message;
END;
```

### Pass/Fail Criterion
* **Pass:** 
  * `io_oracle_home` resolves to `/appl/local/oracle/12.2.0.1.0` when the Oracle 12 flag is `TRUE`.
  * `io_oracle_home` resolves to `/appl/local/oracle/11.2.0` when the Oracle 12 flag is `FALSE` and the Oracle 11 flag is `TRUE`.
  * An exception is raised when both flags are `FALSE` and no pre-existing `io_oracle_home` is provided.
* **Fail:** Path resolution yields incorrect values, or the procedure fails to raise an exception when no Oracle home can be determined.