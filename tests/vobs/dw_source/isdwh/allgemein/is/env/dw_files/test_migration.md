# Migration Validation Test Suite
## Job: Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`

This test suite validates the behavioral equivalence of the migrated environment and orchestration initialization configuration against the legacy KornShell scripts (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`). 

---

## Section 1: Output Parity & Configuration Completeness

### Test Case 1.1: Configuration Key and Value Parity
#### Purpose
Verify that all legacy environment variables defined in `.dw_ai` and `.dw_init` are preserved in the target JSON configuration files (`env_ai_config.json` and `gcs_paths.json`) with accurate values, and that POSIX paths are correctly mapped to GCS URIs.

#### Setup
*   Locate the legacy source files: `.dw_ai` and `.dw_init`.
*   Locate the migrated target configuration files: `env_ai_config.json` and `gcs_paths.json`.
*   Set up a Python test environment with `pytest` installed.

#### Action
Execute a validation script that parses both legacy shell scripts and target JSON files, asserting that every legacy variable has a corresponding target key and that the path structures match the target GCS pattern.

```python
import json
import re
import pytest

def test_ai_config_parity():
    legacy_ai = {
        "AB_HOME": "/appl/local/abinitio/abinitio",
        "AB_AIR_ROOT": "/appl/local/abinitio/TMD_EME/eme_dev/repo",
        "AB_AIR_HOME": "/appl/local/abinitio/abinitio-V2-14",
        "ETL_Host": "dxcsa4.bn.detemobil.de",
        "ETL_Projekt": "BHB",
        "AI_PRIV_SAND_ROOT": "$HOME/abinitio",
        "AI_ENV_SAND_ROOT": "/appl/local/abinitio/sandboxes/DEV"
    }
    
    with open("vobs/dw_source/isdwh/allgemein/is/env/dw_files/env_ai_config.json", "r") as f:
        migrated_ai = json.load(f)
        
    for key, expected_val in legacy_ai.items():
        assert migrated_ai.get(key) == expected_val, f"Mismatch for {key}: expected {expected_val}, got {migrated_ai.get(key)}"

def test_gcs_paths_parity():
    # Map of legacy variables to their expected relative path suffix from $HOME
    legacy_init_paths = {
        "DW_DIR_ROOT": "aktuell",
        "DW_DIR_PROT": "daten/logfiles",
        "DW_DIR_CUBES": "daten/cubes",
        "DW_DIR_IMP_D1": "daten/d1",
        "DW_DIR_IMP_BWA": "daten/dpps/bwa",
        "DW_DIR_IMP_XTRA": "daten/xtra",
        "DW_DIR_IMP_CTEL": "daten/ctel",
        "DW_DIR_IMP_VO": "daten/vo",
        "DW_DIR_IMP_RV": "daten/rv",
        "DW_DIR_IMP_IF": "daten/ees",
        "DW_DIR_IMP_NNV": "daten/nnv",
        "DW_DIR_IMP_SIGMA": "daten/gd/sigma",
        "DW_DIR_EXP_SIGMA": "daten/gd/sigma/export",
        "DW_DIR_IMP_TRF": "daten/trf",
        "DW_DIR_IMP_AUF": "daten/sd/auf",
        "DW_DIR_IMP_GUT": "daten/sd/gut",
        "DW_DIR_IMP_KDG": "daten/sd/kdg",
        "DW_DIR_IMP_MP_KDG": "daten/mp/kdg",
        "DW_DIR_IMP_MP_TS": "daten/mp/ts",
        "DW_DIR_IMP_MP_ZM": "daten/mp/zm",
        "DW_DIR_IMP_TS": "daten/sd/ts",
        "DW_DIR_IMP_ZM": "daten/sd/zm",
        "DW_DIR_EXP": "daten/exporter",
        "DW_DIR_IMP_BPM": "daten/bm",
        "DW_DIR_IMP_ZTS": "daten/zts",
        "DW_DIR_IMP_VRS": "daten/vrs",
        "DW_DIR_IMP_BRUNET": "daten/brunet",
        "DW_DIR_IMP_DWH": "daten/dwh",
        "DW_DIR_IMP_PLATO": "daten/dwh/plato",
        "DW_DIR_IMP_CARMEN": "daten/carmen",
        "DW_DIR_IMP_SAP": "daten/sap",
        "DW_DIR_IMP_SR_RV": "daten/sap/sr_rv_dpps",
        "DW_DIR_IMP_SAP_L": "daten/sap/sap_l_gutgr",
        "DW_DIR_IMP_L_MAHNSTYP_IST": "daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_FI": "daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_IST": "daten/sap/mahn",
        "DW_DIR_IMP_L_GUTGR": "daten/sd/l_gutschr",
        "DW_DIR_IMP_L_LEIST": "daten/sd/l_leist",
        "DW_DIR_IMP_L_PROD": "daten/sd/l_prod",
        "DW_DIR_IMP_LKODE": "daten/sd/lkode",
        "DW_DIR_IMP_SUBSE": "daten/subse",
        "DW_DIR_SMS_PRG": "aktuell/allgemein/is/util",
        "DW_DIR_SMS_ADR": "daten/sms/adressen",
        "DW_DIR_SMS_TMP": "daten/sms/tmp",
        "DW_DIR_IMP_DPPS": "daten/dpps",
        "DW_DIR_IMP_PLANF2": "daten/planf2"
    }

    with open("vobs/dw_source/isdwh/allgemein/is/env/dw_files/gcs_paths.json", "r") as f:
        migrated_paths = json.load(f)

    for key, suffix in legacy_init_paths.items():
        expected_gcs_pattern = f"{{GCS_BUCKET}}/{suffix}"
        assert migrated_paths.get(key) == expected_gcs_pattern, \
            f"Path mismatch for {key}: expected {expected_gcs_pattern}, got {migrated_paths.get(key)}"
```

#### Pass/Fail Criterion
*   **Pass:** All keys from the legacy configurations exist in the migrated JSON files, and their values match the expected target patterns exactly.
*   **Fail:** Any key is missing, or a path mapping does not match the `{GCS_BUCKET}/<suffix>` pattern.

---

## Section 2: Transformation Correctness & Dynamic Resolution

### Test Case 2.1: Dynamic GCS Bucket Injection and Path Resolution
#### Purpose
Verify that the `ConfigurationEngine` correctly resolves the `{GCS_BUCKET}` placeholder at runtime using environment variables or Airflow variables, and that no unresolved placeholders remain.

#### Setup
*   Instantiate the `ConfigurationEngine` from `env_validator.py`.
*   Set a mock environment variable `GCS_BUCKET` to `gs://dwh-isdwh-prod-migration`.

#### Action
Run a test that loads `gcs_paths.json` through the engine and asserts that all values are resolved to absolute GCS URIs starting with the specified bucket prefix.

```python
import os
from vobs.dw_source.isdwh.allgemein.is.env.dw_files.env_validator import ConfigurationEngine

def test_dynamic_bucket_resolution(tmp_path):
    # Create a temporary gcs_paths.json for testing
    test_paths_file = tmp_path / "gcs_paths.json"
    test_data = {
        "DW_DIR_ROOT": "{GCS_BUCKET}/aktuell",
        "DW_DIR_PROT": "{GCS_BUCKET}/daten/logfiles"
    }
    test_paths_file.write_text(json.dumps(test_data))

    # Set environment variable
    os.environ["GCS_BUCKET"] = "gs://dwh-isdwh-test-bucket"
    
    engine = ConfigurationEngine()
    resolved = engine.load_path_config(str(test_paths_file))

    assert resolved["DW_DIR_ROOT"] == "gs://dwh-isdwh-test-bucket/aktuell"
    assert resolved["DW_DIR_PROT"] == "gs://dwh-isdwh-test-bucket/daten/logfiles"
    assert "{GCS_BUCKET}" not in resolved["DW_DIR_ROOT"]
```

#### Pass/Fail Criterion
*   **Pass:** The engine successfully replaces `{GCS_BUCKET}` with the active environment's bucket URI across all configuration keys.
*   **Fail:** The placeholder `{GCS_BUCKET}` remains unresolved, or the engine fails to load the configuration.

---

### Test Case 2.2: Runtime Environment Validation (`.dw_global` Parity)
#### Purpose
Verify that the Python `ConfigurationEngine` correctly identifies missing mandatory environment variables, replicating the validation logic of the legacy `.dw_global` script.

#### Setup
*   Instantiate the `ConfigurationEngine`.
*   Prepare a mock path configuration where some mandatory keys (e.g., `DW_DIR_ROOT`) are missing.

#### Action
Call `validate_runtime_environment()` and assert that it returns `False` when mandatory keys are missing, and `True` when all keys are present.

```python
def test_runtime_environment_validation():
    engine = ConfigurationEngine(gcs_bucket="gs://dwh-isdwh-test")
    
    # Case 1: Missing mandatory keys
    engine.resolved_paths = {
        "DW_DIR_ROOT": "gs://dwh-isdwh-test/aktuell"
        # Other mandatory keys are missing
    }
    assert engine.validate_runtime_environment() is False

    # Case 2: All mandatory keys present
    engine.resolved_paths = {key: f"gs://dwh-isdwh-test/{key}" for key in ConfigurationEngine.MANDATORY_KEYS}
    assert engine.validate_runtime_environment() is True
```

#### Pass/Fail Criterion
*   **Pass:** The validation engine returns `False` when any of the 10 mandatory keys are missing, and logs the missing variables to standard error/log outputs. It returns `True` only when all mandatory keys are populated.
*   **Fail:** The validation engine returns `True` despite missing mandatory keys, or fails to log the specific missing variables.

---

## Section 3: External-System Replacements & Security

### Test Case 3.1: Retirement of Legacy Oracle Parameters and Secret Manager Integration
#### Purpose
Verify that legacy Oracle connection parameters (specifically the plaintext/encrypted database password `DB_PASSWD_DWH` and TNS alias `DB_TNS_NAME_DWH`) are not stored in plaintext files, and that BigQuery localization settings are correctly provided by the engine.

#### Setup
*   Inspect the migrated configuration files (`env_ai_config.json`, `gcs_paths.json`) to ensure no database credentials are present.
*   Instantiate the `ConfigurationEngine`.

#### Action
1. Scan configuration files for sensitive keys (`DB_PASSWD_DWH`, `DB_USER_DWH`, `password`).
2. Call `get_database_runtime_context()` to verify that BigQuery-compatible localization settings are returned.

```python
def test_database_context_and_credential_absence():
    # Ensure no credentials exist in the JSON configs
    for config_file in ["env_ai_config.json", "gcs_paths.json"]:
        with open(f"vobs/dw_source/isdwh/allgemein/is/env/dw_files/{config_file}", "r") as f:
            content = f.read()
            assert "DB_PASSWD_DWH" not in content
            assert "meyreis" not in content
            assert "devlab.de.tmo" not in content

    # Verify BigQuery localization context
    engine = ConfigurationEngine()
    db_ctx = engine.get_database_runtime_context()
    assert db_ctx["NLS_LANG"] == "GERMAN_GERMANY.WE8ISO8859P1"
    assert db_ctx["LANG"] == "de"
```

#### Pass/Fail Criterion
*   **Pass:** No legacy database credentials or TNS names exist in the target configuration files. The configuration engine successfully returns the required BigQuery localization context.
*   **Fail:** Legacy credentials or hostnames are found in the JSON files, or the localization context is missing or incorrect.

---

## Section 4: Data Quality & Schema Assertions

### Test Case 4.1: JSON Schema Validation
#### Purpose
Ensure that the migrated configuration files conform to strict JSON schemas, preventing syntax errors or type mismatches from breaking downstream Airflow DAGs.

#### Setup
*   Install `jsonschema` in the test environment.
*   Define the expected schema for `env_ai_config.json` and `gcs_paths.json`.

#### Action
Validate both JSON files against their respective schemas.

```python
from jsonschema import validate

def test_json_schema_conformance():
    ai_schema = {
        "type": "object",
        "properties": {
            "AB_HOME": {"type": "string"},
            "AB_AIR_ROOT": {"type": "string"},
            "AB_AIR_HOME": {"type": "string"},
            "ETL_Host": {"type": "string"},
            "ETL_Projekt": {"type": "string"},
            "AI_PRIV_SAND_ROOT": {"type": "string"},
            "AI_ENV_SAND_ROOT": {"type": "string"}
        },
        "required": ["AB_HOME", "AB_AIR_ROOT", "ETL_Host", "ETL_Projekt"]
    }

    paths_schema = {
        "type": "object",
        "additionalProperties": {"type": "string", "pattern": "^\\{GCS_BUCKET\\}/.+$"}
    }

    with open("vobs/dw_source/isdwh/allgemein/is/env/dw_files/env_ai_config.json", "r") as f:
        validate(instance=json.load(f), schema=ai_schema)

    with open("vobs/dw_source/isdwh/allgemein/is/env/dw_files/gcs_paths.json", "r") as f:
        validate(instance=json.load(f), schema=paths_schema)
```

#### Pass/Fail Criterion
*   **Pass:** Both configuration files are syntactically valid JSON and conform strictly to the defined schemas (e.g., all paths in `gcs_paths.json` start with the `{GCS_BUCKET}/` prefix).
*   **Fail:** Any schema validation error is raised, indicating malformed JSON or invalid path structures.