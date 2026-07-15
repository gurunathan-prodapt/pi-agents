Here is a comprehensive suite of migration-validation tests designed to prove the behavioral equivalence of the migrated configuration files and stored procedures against the legacy source.

---

# Test Suite: Shared Files Configuration Migration Validation
**Target Job**: `Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Target Platform**: Google Cloud Composer (Airflow) & BigQuery  

---

## Section 1: Output Parity & Variable Mapping Tests

### Test Case 1.1: Airflow Variable Store Parity Validation
#### Purpose
Verify that the JSON configuration file (`dw_env_config.json`) and the Airflow bootstrap task (`dw_shared_files_bootstrap.py`) correctly map and load all legacy environment variables into the Cloud Composer Variable Store with exact value parity (substituting legacy paths with equivalent GCS URIs).

#### Setup
1. Deploy `dw_env_config.json` to the Airflow environment.
2. Run the `dw_shared_files_bootstrap` DAG in a sandbox Cloud Composer environment.
3. Ensure the Airflow Variable metadata database is accessible via the Airflow CLI or Python API.

#### Action
Execute the following `pytest` test suite within the Composer environment to validate that every legacy variable has been successfully registered and matches the expected target GCS URI structure.

```python
import pytest
import json
from airflow.models import Variable

def test_airflow_variables_parity():
    # Load the target JSON configuration file
    config_path = "dags/config/dw_env_config.json"
    with open(config_path, "r") as f:
        expected_vars = json.load(f)
    
    # Assert each variable exists in Airflow and matches the expected target value
    for var_key, expected_val in expected_vars.items():
        actual_val = Variable.get(var_key, default_var=None)
        assert actual_val is not None, f"Variable {var_key} was not bootstrapped into Airflow."
        assert actual_val == expected_val, f"Value mismatch for {var_key}: Expected '{expected_val}', got '{actual_val}'"

def test_gcs_uri_structure():
    # Verify that the base root URI is correctly propagated to downstream directories
    gcs_bucket = Variable.get("GCS_BUCKET", default_var="gcp-dwh-environment-bucket")
    expected_root = f"gs://{gcs_bucket}/dw_source/aktuell"
    
    assert Variable.get("DW_DIR_ROOT") == expected_root
```

#### Pass/Fail Criterion
* **Pass**: All 51 variables defined in `dw_env_config.json` are successfully loaded into the Airflow Variable Store, and their values match the expected target GCS URIs exactly.
* **Fail**: Any variable is missing, or its value deviates from the mapped target configuration.

---

## Section 2: Transformation & Procedural Correctness Tests

### Test Case 2.1: BigQuery `dw_init` Directory Initialization Logic
#### Purpose
Verify that calling the BigQuery stored procedure `dw_init` with a base GCS URI correctly derives and outputs all 49 downstream directory paths, matching the exact string concatenation logic of the legacy `.dw_init` shell script.

#### Setup
1. Deploy the `dw_init.sql` stored procedure to the target BigQuery dataset.
2. Ensure the test runner has execution permissions on the dataset.

#### Action
Execute the following SQL assertion script in BigQuery. This script calls the stored procedure with a test URI and asserts that the output parameters match the expected concatenated paths.

```sql
DECLARE test_home_uri STRING DEFAULT 'gs://test-migration-bucket/dw_source';
DECLARE test_oracle_sid STRING DEFAULT 'eDWH3';

-- Declare output variables matching the procedure signature
DECLARE out_dw_dir_root STRING; DECLARE out_dw_dir_prot STRING; DECLARE out_dw_dir_cubes STRING;
DECLARE out_dw_dir_imp_d1 STRING; DECLARE out_dw_dir_imp_bwa STRING; DECLARE out_dw_dir_imp_xtra STRING;
DECLARE out_dw_dir_imp_ctel STRING; DECLARE out_dw_dir_imp_vo STRING; DECLARE out_dw_dir_imp_rv STRING;
DECLARE out_dw_dir_imp_if STRING; DECLARE out_dw_dir_imp_nnv STRING; DECLARE out_dw_dir_imp_sigma STRING;
DECLARE out_dw_dir_exp_sigma STRING; DECLARE out_dw_dir_imp_trf STRING; DECLARE out_dw_dir_imp_auf STRING;
DECLARE out_dw_dir_imp_gut STRING; DECLARE out_dw_dir_imp_kdg STRING; DECLARE out_dw_dir_imp_mp_kdg STRING;
DECLARE out_dw_dir_imp_mp_ts STRING; DECLARE out_dw_dir_imp_mp_zm STRING; DECLARE out_dw_dir_imp_ts STRING;
DECLARE out_dw_dir_imp_zm STRING; DECLARE out_dw_dir_exp STRING; DECLARE out_dw_dir_imp_bpm STRING;
DECLARE out_dw_dir_imp_zts STRING; DECLARE out_dw_dir_imp_vrs STRING; DECLARE out_dw_dir_imp_brunet STRING;
DECLARE out_dw_dir_imp_dwh STRING; DECLARE out_dw_dir_imp_plato STRING; DECLARE out_dw_dir_imp_carmen STRING;
DECLARE out_dw_dir_imp_sap STRING; DECLARE out_dw_dir_imp_sr_rv STRING; DECLARE out_dw_dir_imp_sap_l STRING;
DECLARE out_dw_dir_imp_l_mahnstyp_ist STRING; DECLARE out_dw_dir_imp_l_mahnv_fi STRING; DECLARE out_dw_dir_imp_l_mahnv_ist STRING;
DECLARE out_dw_dir_imp_l_gutgr STRING; DECLARE out_dw_dir_imp_l_leist STRING; DECLARE out_dw_dir_imp_l_prod STRING;
DECLARE out_dw_dir_imp_lkode STRING; DECLARE out_dw_dir_imp_subse STRING; DECLARE out_dw_dir_sms_prg STRING;
DECLARE out_dw_dir_sms_adr STRING; DECLARE out_dw_dir_sms_tmp STRING; DECLARE out_dw_dir_imp_dpps STRING;
DECLARE out_dw_dir_imp_planf2 STRING; DECLARE out_dw_host_customer STRING; DECLARE io_oracle_home STRING DEFAULT '/appl/local/oracle/12.2.0.1.0';
DECLARE out_dw_dir_utl_file STRING;

CALL `GCP_PROJECT.BQ_DATASET.dw_init`(
  test_home_uri, test_oracle_sid,
  out_dw_dir_root, out_dw_dir_prot, out_dw_dir_cubes, out_dw_dir_imp_d1, out_dw_dir_imp_bwa,
  out_dw_dir_imp_xtra, out_dw_dir_imp_ctel, out_dw_dir_imp_vo, out_dw_dir_imp_rv, out_dw_dir_imp_if,
  out_dw_dir_imp_nnv, out_dw_dir_imp_sigma, out_dw_dir_exp_sigma, out_dw_dir_imp_trf, out_dw_dir_imp_auf,
  out_dw_dir_imp_gut, out_dw_dir_imp_kdg, out_dw_dir_imp_mp_kdg, out_dw_dir_imp_mp_ts, out_dw_dir_imp_mp_zm,
  out_dw_dir_imp_ts, out_dw_dir_imp_zm, out_dw_dir_exp, out_dw_dir_imp_bpm, out_dw_dir_imp_zts,
  out_dw_dir_imp_vrs, out_dw_dir_imp_brunet, out_dw_dir_imp_dwh, out_dw_dir_imp_plato, out_dw_dir_imp_carmen,
  out_dw_dir_imp_sap, out_dw_dir_imp_sr_rv, out_dw_dir_imp_sap_l, out_dw_dir_imp_l_mahnstyp_ist,
  out_dw_dir_imp_l_mahnv_fi, out_dw_dir_imp_l_mahnv_ist, out_dw_dir_imp_l_gutgr, out_dw_dir_imp_l_leist,
  out_dw_dir_imp_l_prod, out_dw_dir_imp_lkode, out_dw_dir_imp_subse, out_dw_dir_sms_prg, out_dw_dir_sms_adr,
  out_dw_dir_sms_tmp, out_dw_dir_imp_dpps, out_dw_dir_imp_planf2, out_dw_host_customer, io_oracle_home,
  out_dw_dir_utl_file
);

-- Assertions to verify correct path derivation
ASSERT out_dw_dir_root = 'gs://test-migration-bucket/dw_source/aktuell' AS 'Root path mismatch';
ASSERT out_dw_dir_prot = 'gs://test-migration-bucket/dw_source/daten/logfiles' AS 'Log path mismatch';
ASSERT out_dw_dir_imp_bwa = 'gs://test-migration-bucket/dw_source/daten/dpps/bwa' AS 'BWA path mismatch';
ASSERT out_dw_dir_imp_sap_l = 'gs://test-migration-bucket/dw_source/daten/sap/sap_l_gutgr' AS 'SAP_L path mismatch';
ASSERT out_dw_dir_utl_file = '/appl/local/oracle/admin/eDWH3/utl_file' AS 'UTL_FILE path mismatch';
ASSERT out_dw_host_customer = 'dxcst3.bn.detemobil.de' AS 'Customer host mismatch';
```

#### Pass/Fail Criterion
* **Pass**: The stored procedure executes successfully, and all assertions evaluate to `TRUE`.
* **Fail**: Any path concatenation fails to match the expected string structure, or the procedure throws a runtime error.

---

## Section 3: External-System Replacements & Fallback Tests

### Test Case 3.1: Oracle Home Path Fallback Resolution
#### Purpose
Verify that the `dw_init` stored procedure correctly resolves the `ORACLE_HOME` path using the BigQuery system configuration table `system_paths` when `v_oracle_home` is passed as `NULL` or empty.

#### Setup
1. Create and populate the mock `system_paths` configuration table in BigQuery:

```sql
CREATE OR REPLACE TABLE `GCP_PROJECT.BQ_DATASET.system_paths` (
  path STRING,
  active BOOLEAN
);

INSERT INTO `GCP_PROJECT.BQ_DATASET.system_paths` (path, active)
VALUES 
  ('/appl/local/oracle/12.2.0.1.0', TRUE),
  ('/appl/local/oracle/11.2.0', FALSE);
```

#### Action
Execute the stored procedure with `v_oracle_home` initialized to `NULL` and assert that it falls back to the active path in the configuration table.

```sql
DECLARE io_oracle_home STRING DEFAULT NULL;
-- Declare other required dummy variables for signature compatibility
DECLARE dummy_str STRING;

CALL `GCP_PROJECT.BQ_DATASET.dw_init`(
  'gs://test-bucket', 'eDWH3',
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, io_oracle_home,
  dummy_str
);

ASSERT io_oracle_home = '/appl/local/oracle/12.2.0.1.0' AS 'Oracle Home fallback failed to resolve to active 12.2 path';
```

#### Pass/Fail Criterion
* **Pass**: The procedure successfully queries `system_paths` and assigns `/appl/local/oracle/12.2.0.1.0` to the `v_oracle_home` variable.
* **Fail**: The variable remains `NULL`, resolves to the inactive path, or throws an unhandled exception.

---

## Section 4: Data Quality, Error Handling & Schema Assertions

### Test Case 4.1: Verbatim Error Retainment & Logging Validation
#### Purpose
Verify that if `dw_init` fails to resolve `ORACLE_HOME` (i.e., no active paths exist in the metadata table), it logs the exact verbatim German error message to the `error_logs` table.

#### Setup
1. Truncate or disable active paths in the `system_paths` table:
```sql
UPDATE `GCP_PROJECT.BQ_DATASET.system_paths` SET active = FALSE WHERE TRUE;
```
2. Ensure the `error_logs` table exists:
```sql
CREATE OR REPLACE TABLE `GCP_PROJECT.BQ_DATASET.error_logs` (
  log_time TIMESTAMP,
  module STRING,
  message STRING
);
```

#### Action
Execute the stored procedure with `v_oracle_home` set to `NULL` to trigger the error state, then query the log table.

```sql
DECLARE io_oracle_home STRING DEFAULT NULL;
DECLARE dummy_str STRING;

CALL `GCP_PROJECT.BQ_DATASET.dw_init`(
  'gs://test-bucket', 'eDWH3',
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str,
  dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, dummy_str, io_oracle_home,
  dummy_str
);

-- Assert that the exact legacy error string was written to the log table
ASSERT EXISTS (
  SELECT 1 FROM `GCP_PROJECT.BQ_DATASET.error_logs`
  WHERE module = '.dw_init'
    AND message = 'Fehler in .dw_init:\n   Konnte ORACLE_HOME nicht setzen !'
) AS 'Verbatim error message for ORACLE_HOME was not logged correctly';
```

#### Pass/Fail Criterion
* **Pass**: The procedure executes without crashing, and writes a log entry with the exact string: `"Fehler in .dw_init:\n   Konnte ORACLE_HOME nicht setzen !"`.
* **Fail**: No log entry is written, or the logged message does not match the legacy German text verbatim.

---

### Test Case 4.2: Parameter Validation & Error Raising in `dw_global`
#### Purpose
Verify that the `dw_global` stored procedure correctly identifies missing or empty environment variables and raises a BigQuery system error containing the exact list of missing parameters.

#### Setup
1. Deploy `dw_global.sql` to the target BigQuery dataset.

#### Action
Execute the stored procedure with two missing parameters (`IN_DW_DIR_ROOT` and `IN_ORACLE_HOME` set to `NULL` or empty strings) and capture the raised error.

```sql
BEGIN
  CALL `GCP_PROJECT.BQ_DATASET.dw_global`(
    '' -- IN_DW_DIR_ROOT (Empty)
    , 'gs://test/prot', 'gs://test/cubes', 'gs://test/d1', 'gs://test/xtra', 'gs://test/ctel',
    'gs://test/vo', 'gs://test/rv', 'gs://test/if', 'gs://test/nnv',
    NULL -- IN_ORACLE_HOME (NULL)
  );
  
  -- If execution reaches this point, the procedure failed to raise an error
  ERROR 'Validation failed: dw_global did not raise an error for missing parameters.';
EXCEPTION WHEN ERROR THEN
  -- Assert that the error message contains the exact legacy formatting and lists the missing variables
  IF @@error.message LIKE '%Fehler in .dw_global:\nDie folgenden Umgebungsvariablen sind nicht gesetzt: DW_DIR_ROOT ORACLE_HOME%' THEN
    SELECT 'SUCCESS: Correct error raised' AS status;
  ELSE
    RAISE USING message = CONCAT('Incorrect error message raised: ', @@error.message);
  END IF;
END;
```

#### Pass/Fail Criterion
* **Pass**: The procedure aborts execution and raises an error containing the exact verbatim string: `"Fehler in .dw_global:\nDie folgenden Umgebungsvariablen sind nicht gesetzt: DW_DIR_ROOT ORACLE_HOME"`.
* **Fail**: The procedure completes successfully without raising an error, or the error message does not list the missing variables accurately.