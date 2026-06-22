The migration of `k_ausd_bp_ta_bpr_apn.ksh` to a BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_bpr_apn`) primarily involves transforming an orchestration script. The core data transformation logic is assumed to reside in `d_ausd_bp_ta_bpr_apn.sql`, which is also migrated to a BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_bpr_apn`).

The following tests focus on validating the behavior of the orchestrator, including parameter handling, error logging, date calculations, and the correct invocation of the core data processing logic.

**Assumptions for Testing Environment:**

*   **BigQuery Project/Dataset:** A BigQuery project (`my_project`) and dataset (`my_dataset`) are configured.
*   **BigQuery Tables:**
    *   `my_project.my_dataset.job_error_log`: Exists with schema `(job_name STRING, error_nr INT64, error_arg STRING, created_ts TIMESTAMP)`.
    *   `my_project.my_dataset.job_tracking`: Exists with schema `(tab_name STRING, status STRING, mode STRING, stichtag_from DATE, stichtag_to DATE, job_type STRING, restart_flag STRING, record_count INT64, description STRING, created_ts TIMESTAMP)`.
    *   `my_project.my_dataset.target_result_table`: Exists with schema `(stichtag DATE, some_col STRING)` (minimal schema for record count validation).
*   **Mock BigQuery Stored Procedure (`d_ausd_bp_ta_bpr_apn`):** A mock version of `project.dataset.d_ausd_bp_ta_bpr_apn` is deployed. This mock SP logs its input parameters to `my_project.my_dataset.d_ausd_bp_ta_bpr_apn_params_log` and simulates inserting a fixed number of rows into `my_project.my_dataset.target_result_table` for successful execution scenarios.
*   **Mock BigQuery Table (`d_ausd_bp_ta_bpr_apn_params_log`):** Exists with schema `(p_EintragsNr_1 STRING, p_EintragsNr_2 STRING, p_JobKennung STRING, p_Stichtag STRING, v_datum_heute DATE, v_datum_gestern DATE, p_wiederanlaufWert STRING)`.
*   **Legacy Environment:**
    *   A KornShell environment is available to execute the legacy script.
    *   Mock versions of the sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) and environment variables (`$HOME/.dw_init`, `BERT_DIR_ROOT`, `DW_DIR_UTL`) are set up in a controlled directory (e.g., `/tmp/legacy_mock_env`).
    *   The mock `starteSQLSkript` function (within `h_alis_sqlplus.ksh`) is configured to:
        *   Write a record count to `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`.
        *   Simulate successful execution (exit code 0).
        *   (Optionally) Log parameters it receives for detailed comparison.
    *   The mock `DWMSG_MeldeFehler` function (within `f_alis_msgerr.ksh`) is configured to print error messages to `stderr`/`stdout` and exit with the specified error code.
    *   The mock `gestern.ksh` script is configured to output `YYYY-MM-DD YYYY-MM-DD` for today and yesterday's dates.

---

### Mock BigQuery Objects Setup

These SQL statements define the mock BigQuery objects required for the tests.

```sql
-- Mock d_ausd_bp_ta_bpr_apn Stored Procedure for testing purposes
-- This procedure logs its input parameters and simulates a data insertion
-- to allow testing of the orchestrator's parameter passing and record counting.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.d_ausd_bp_ta_bpr_apn`(
  IN p_EintragsNr_1 STRING,
  IN p_EintragsNr_2 STRING, -- As per design document, EintragsNr passed twice
  IN p_JobKennung STRING,
  IN p_Stichtag STRING,
  IN v_datum_heute DATE,
  IN v_datum_gestern DATE,
  IN p_wiederanlaufWert STRING
)
BEGIN
  -- Log parameters to a dedicated table for verification
  INSERT INTO `my_project.my_dataset.d_ausd_bp_ta_bpr_apn_params_log` (
    p_EintragsNr_1, p_EintragsNr_2, p_JobKennung, p_Stichtag, v_datum_heute, v_datum_gestern, p_wiederanlaufWert
  )
  VALUES (
    p_EintragsNr_1, p_EintragsNr_2, p_JobKennung, p_Stichtag, v_datum_heute, v_datum_gestern, p_wiederanlaufWert
  );

  -- Simulate data insertion into the target table
  -- This is crucial for the record count assertion in the main orchestrator SP
  -- For successful execution, insert a fixed number of rows.
  -- For other tests, this might be skipped or insert 0 rows.
  IF p_JobKennung IN ('TEST_JOB_SUCCESS', 'TEST_JOB_DEFAULT_RESTART', 'TEST_JOB_DATE_DERIVATION', 'TEST_JOB_CORE_PARAMS', 'TEST_JOB_NO_FILE_PROC') THEN
    INSERT INTO `my_project.my_dataset.target_result_table` (stichtag, some_col)
    SELECT PARSE_DATE('%d%m%Y', p_Stichtag), 'mock_data_1' UNION ALL
    SELECT PARSE_DATE('%d%m%Y', p_Stichtag), 'mock_data_2' UNION ALL
    SELECT PARSE_DATE('%d%m%Y', p_Stichtag), 'mock_data_3' UNION ALL
    SELECT PARSE_DATE('%d%m%Y', p_Stichtag), 'mock_data_4' UNION ALL
    SELECT PARSE_DATE('%d%m%Y', p_Stichtag), 'mock_data_5';
  END IF;

END;

-- Schema for the mock parameters log table
CREATE OR REPLACE TABLE `my_project.my_dataset.d_ausd_bp_ta_bpr_apn_params_log` (
  p_EintragsNr_1 STRING,
  p_EintragsNr_2 STRING,
  p_JobKennung STRING,
  p_Stichtag STRING,
  v_datum_heute DATE,
  v_datum_gestern DATE,
  p_wiederanlaufWert STRING
);

-- Schema for the target result table (minimal for testing record count)
CREATE OR REPLACE TABLE `my_project.my_dataset.target_result_table` (
  stichtag DATE,
  some_col STRING
);

-- Schema for job error log table
CREATE OR REPLACE TABLE `my_project.my_dataset.job_error_log` (
  job_name STRING,
  error_nr INT64,
  error_arg STRING,
  created_ts TIMESTAMP
);

-- Schema for job tracking table
CREATE OR REPLACE TABLE `my_project.my_dataset.job_tracking` (
  tab_name STRING,
  status STRING,
  mode STRING,
  stichtag_from DATE,
  stichtag_to DATE,
  job_type STRING,
  restart_flag STRING,
  record_count INT64,
  description STRING,
  created_ts TIMESTAMP
);
```

---

### Python Pytest Setup

```python
import subprocess
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta
import os

# --- Configuration ---
LEGACY_SCRIPT_PATH = "/tmp/legacy_mock_env/bin/k_ausd_bp_ta_bpr_apn.ksh"
LEGACY_TMP_FILE = "/tmp/legacy_mock_env/DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp"
LEGACY_MOCK_ENV_ROOT = "/tmp/legacy_mock_env" # Base directory for mock legacy environment
LEGACY_BERT_DIR_ROOT = f"{LEGACY_MOCK_ENV_ROOT}/bert" # Mock BERT_DIR_ROOT
LEGACY_DW_DIR_UTL = f"{LEGACY_MOCK_ENV_ROOT}/DW_DIR_UTL" # Mock DW_DIR_UTL

BIGQUERY_PROJECT = "my_project"
BIGQUERY_DATASET = "my_dataset"
BIGQUERY_SP_NAME = "r_ausd_bp_ta_bpr_apn"
BIGQUERY_TARGET_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.target_result_table"
BIGQUERY_JOB_TRACKING_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.job_tracking"
BIGQUERY_ERROR_LOG_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.job_error_log"
BIGQUERY_PARAMS_LOG_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.d_ausd_bp_ta_bpr_apn_params_log"

# --- Pytest Fixtures ---

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=BIGQUERY_PROJECT)

@pytest.fixture(scope="module", autouse=True)
def setup_legacy_mock_env():
    """Sets up a mock legacy environment for ksh script execution."""
    os.makedirs(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin", exist_ok=True)
    os.makedirs(f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/bin", exist_ok=True)
    os.makedirs(f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/sql", exist_ok=True)
    os.makedirs(f"{LEGACY_DW_DIR_UTL}", exist_ok=True)
    os.makedirs(f"{LEGACY_MOCK_ENV_ROOT}/bin", exist_ok=True) # For the main ksh script

    # Create mock .dw_init
    with open(f"{os.environ['HOME']}/.dw_init", "w") as f:
        f.write(f"export BERT_DIR_ROOT={LEGACY_BERT_DIR_ROOT}\n")
        f.write(f"export DW_DIR_UTL={LEGACY_DW_DIR_UTL}\n")

    # Create mock f_alis_msgerr.ksh
    with open(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh", "w") as f:
        f.write("#!/bin/ksh\n")
        f.write("DWMSG_MeldeFehler() { echo \"FEHLER: $1 $2 $3 $4\" >&2; exit $3; }\n")
    os.chmod(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh", 0o755)

    # Create mock h_alis_date.ksh
    with open(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh", "w") as f:
        f.write("#!/bin/ksh\n")
        f.write("DWDate_Datum_Check() { \n")
        f.write("  if [[ \"$2\" == \"DDMMYYYY\" && \"$1\" =~ ^[0-9]{8}$ ]]; then \n")
        f.write("    # Simple check, assume valid if 8 digits for DDMMYYYY\n")
        f.write("    return 0\n")
        f.write("  else\n")
        f.write("    # Simulate error for invalid format\n")
        f.write("    ErrNr=193; ErrArg=\"$1\"; DWMSG_MeldeFehler 0 E $ErrNr \"$ErrArg\";\n")
        f.write("    return 1\n")
        f.write("  fi\n")
        f.write("}\n")
    os.chmod(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh", 0o755)

    # Create mock h_alis_parameter.ksh
    with open(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh", "w") as f:
        f.write("#!/bin/ksh\n")
        f.write("pruefeParameterGesetzt() { \n")
        f.write("  local param_name=$1\n")
        f.write("  local param_value_var=$2\n")
        f.write("  local param_value=$(eval echo \"\$$param_value_var\")\n")
        f.write("  if [[ -z \"$param_value\" ]]; then\n")
        f.write("    ErrNr=1; ErrArg=\"$param_name\";\n")
        f.write("  fi\n")
        f.write("}\n")
    os.chmod(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh", 0o755)

    # Create mock h_alis_sqlplus.ksh (containing starteSQLSkript)
    with open(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh", "w") as f:
        f.write("#!/bin/ksh\n")
        f.write("starteSQLSkript() { \n")
        f.write("  echo \"Mock starteSQLSkript called with args: $@\" >&2\n")
        f.write("  local tmpFile=$6\n") # $6 is tmpFile in the ksh script call
        f.write("  echo \"5\" > \"$tmpFile\"\n") # Simulate 5 records written
        f.write("  return 0\n") # Simulate success
        f.write("}\n")
    os.chmod(f"{LEGACY_BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh", 0o755)

    # Create mock gestern.ksh
    with open(f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh", "w") as f:
        f.write("#!/bin/ksh\n")
        f.write("heute=$(date +%Y-%m-%d)\n")
        f.write("gestern=$(date -d '1 day ago' +%Y-%m-%d)\n")
        f.write("echo \"$heute $gestern\"\n")
    os.chmod(f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh", 0o755)

    # Copy the main ksh script to the mock environment
    with open(LEGACY_SCRIPT_PATH, "w") as f_dest:
        with open("vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh", "r") as f_src:
            f_dest.write(f_src.read())
    os.chmod(LEGACY_SCRIPT_PATH, 0o755)

    yield
    # Cleanup mock environment
    subprocess.run(f"rm -rf {LEGACY_MOCK_ENV_ROOT}", shell=True, check=False)
    subprocess.run(f"rm -f {os.environ['HOME']}/.dw_init", shell=True, check=False)


@pytest.fixture(autouse=True)
def cleanup_bq_tables(bq_client):
    """Cleans up BigQuery tables before each test."""
    bq_client.query(f"TRUNCATE TABLE {BIGQUERY_TARGET_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {BIGQUERY_JOB_TRACKING_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {BIGQUERY_ERROR_LOG_TABLE}").result()
    bq_client.query(f"TRUNCATE TABLE {BIGQUERY_PARAMS_LOG_TABLE}").result()
    # Clean up legacy tmp file
    subprocess.run(f"rm -f {LEGACY_TMP_FILE}", shell=True, check=False)
    yield

# --- Helper Functions ---

def run_legacy_script(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
    """Helper to run the legacy ksh script and capture outputs."""
    cmd_parts = [LEGACY_SCRIPT_PATH]
    if job_kennung is not None:
        cmd_parts.extend(["-j", f'"{job_kennung}"'])
    if eintrags_nr is not None:
        cmd_parts.extend(["-f", f'"{eintrags_nr}"'])
    if stichtag is not None:
        cmd_parts.extend(["-s", f'"{stichtag}"'])
    if wiederanlauf_wert is not None:
        cmd_parts.extend(["-l", f'"{wiederanlauf_wert}"'])

    cmd = " ".join(cmd_parts)
    print(f"Running legacy command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False,
                            env={**os.environ, "BERT_DIR_ROOT": LEGACY_BERT_DIR_ROOT, "DW_DIR_UTL": LEGACY_DW_DIR_UTL})

    legacy_record_count = None
    if result.returncode == 0:
        try:
            with open(LEGACY_TMP_FILE, 'r') as f:
                legacy_record_count = int(f.read().strip())
        except FileNotFoundError:
            print(f"Warning: Legacy temporary file {LEGACY_TMP_FILE} not found.")
        except ValueError:
            print(f"Warning: Could not parse record count from {LEGACY_TMP_FILE}.")

    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "record_count": legacy_record_count
    }

def run_migrated_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
    """Helper to run the BigQuery Stored Procedure."""
    # Handle NULL/empty string for BigQuery parameters
    job_kennung_param = f"'{job_kennung}'" if job_kennung is not None else "NULL"
    eintrags_nr_param = f"'{eintrags_nr}'" if eintrags_nr is not None else "NULL"
    stichtag_param = f"'{stichtag}'" if stichtag is not None else "NULL"
    wiederanlauf_wert_param = f"'{wiederanlauf_wert}'" if wiederanlauf_wert is not None else "NULL"

    query = f"""
        CALL `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_SP_NAME}`(
            p_JobKennung => {job_kennung_param},
            p_EintragsNr => {eintrags_nr_param},
            p_Stichtag => {stichtag_param},
            p_wiederanlaufWert => {wiederanlauf_wert_param}
        );
    """
    print(f"Running BigQuery SP: {query}")
    try:
        job = bq_client.query(query)
        job.result() # Wait for the job to complete
        return {"success": True, "error": None}
    except Exception as e:
        return {"success": False, "error": str(e)}

```

---

### Test Cases

#### 1. Output Parity & Job Tracking (Successful Execution)

*   **Purpose**: Verify that with valid inputs, the migrated BigQuery Stored Procedure executes successfully, produces the same record count, and correctly logs job tracking information, mirroring the legacy script's behavior.
*   **Setup**:
    *   Legacy mock environment is configured.
    *   BigQuery mock `d_ausd_bp_ta_bpr_apn` SP is deployed to simulate inserting 5 rows.
    *   All relevant BigQuery tables (`target_result_table`, `job_tracking`, `job_error_log`, `d_ausd_bp_ta_bpr_apn_params_log`) are empty.
    *   Legacy temporary file (`LEGACY_TMP_FILE`) is cleared.
*   **Action**:
    1.  Execute the legacy script: `k_ausd_bp_ta_bpr_apn.ksh -j "JOB1" -f "100" -s "01012023" -l "0"`
    2.  Execute the migrated BigQuery Stored Procedure: `CALL my_project.my_dataset.r_ausd_bp_ta_bpr_apn('JOB1', '100', '01012023', '0');`
*   **Pass/Fail Criterion**:
    1.  **Legacy Script Exit Status**: Legacy script exits with return code 0.
    2.  **Record Count Parity**: The record count read from `LEGACY_TMP_FILE` (expected: 5) must match the `record_count` in the `my_project.my_dataset.job_tracking` table for the corresponding run.
    3.  **Job Tracking Entry**: A record must exist in `my_project.my_dataset.job_tracking` with:
        *   `tab_name = 'PoolBasisprodukt'`
        *   `status = 'A'`, `mode = 'I'`, `job_type = 'J'`, `restart_flag = 'N'`
        *   `stichtag_from = '2023-01-01'`, `stichtag_to = '2023-01-01'`
        *   `record_count = 5`
        *   `description = 'Initialbefuellung'`
        *   `created_ts` within a reasonable time window.
    4.  **BigQuery Target Table**: `my_project.my_dataset.target_result_table` must contain 5 rows for `stichtag = '2023-01-01'`.

```python
def test_successful_execution_and_output_parity(bq_client, cleanup_bq_tables, setup_legacy_mock_env):
    """
    Purpose: Verify successful execution, output data parity (record count), and correct job tracking.
    """
    job_kennung = "TEST_JOB_SUCCESS"
    eintrags_nr = "101"
    stichtag = "01012023"
    wiederanlauf_wert = "0"
    stichtag_date_bq = "2023-01-01"
    expected_record_count = 5 # As simulated by mock d_ausd_bp_ta_bpr_apn

    # Action: Run legacy script
    legacy_result = run_legacy_script(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    assert legacy_result["returncode"] == 0, f"Legacy script failed: {legacy_result['stderr']}"
    assert "ENDE Datenverarbeitung" in legacy_result["stdout"]

    # Action: Run migrated SP
    migrated_result = run_migrated_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    assert migrated_result["success"], f"Migrated SP failed: {migrated_result['error']}"

    # Pass/Fail Criterion 1: BigQuery target table record count
    bq_target_count_query = f"SELECT COUNT(*) FROM {BIGQUERY_TARGET_TABLE} WHERE stichtag = '{stichtag_date_bq}'"
    bq_target_count = bq_client.query(bq_target_count_query).result().scalar_iter().next()
    assert bq_target_count == expected_record_count, "BigQuery target table record count mismatch."

    # Pass/Fail Criterion 2: Record Count from tmp file vs. job_tracking
    assert legacy_result["record_count"] is not None, "Legacy record count not captured from temporary file."
    assert legacy_result["record_count"] == expected_record_count, "Legacy record count mismatch with expected."

    bq_job_tracking_query = f"""
        SELECT tab_name, status, mode, stichtag_from, stichtag_to, job_type, restart_flag, record_count, description
        FROM {BIGQUERY_JOB_TRACKING_TABLE}
        WHERE tab_name = 'PoolBasisprodukt' AND stichtag_from = '{stichtag_date_bq}'
        ORDER BY created_ts DESC LIMIT 1
    """
    job_tracking_entry = list(bq_client.query(bq_job_tracking_query).result())
    assert len(job_tracking_entry) == 1, "Job tracking entry not found or multiple entries."
    entry = job_tracking_entry[0]

    assert entry.tab_name == 'PoolBasisprodukt'
    assert entry.status == 'A'
    assert entry.mode == 'I'
    assert str(entry.stichtag_from) == stichtag_date_bq
    assert str(entry.stichtag_to) == stichtag_date_bq
    assert entry.job_type == 'J'
    assert entry.restart_flag == 'N'
    assert entry.record_count == expected_record_count
    assert entry.description == 'Initialbefuellung'

    # Ensure the record count from legacy matches the one in BQ job tracking
    assert legacy_result["record_count"] == entry.record_count, "Legacy record count differs from BQ job tracking."

```

#### 2. Transformation Correctness: Parameter Validation - Missing Job ID

*   **Purpose**: Verify that the migrated SP correctly handles a missing Job ID parameter, logging an error and exiting, mirroring the legacy script.
*   **Setup**:
    *   Legacy mock environment is configured.
    *   BigQuery `job_error_log` table is empty.
    *   BigQuery `target_result_table` and `job_tracking` are empty.
*   **Action**:
    1.  Execute the legacy script: `k_ausd_bp_ta_bpr_apn.ksh -f "100" -s "01012023"` (missing `-j`)
    2.  Execute the migrated BigQuery Stored Procedure: `CALL my_project.my_dataset.r_ausd_bp_ta_bpr_apn(p_JobKennung => NULL, p_EintragsNr => '100', p_Stichtag => '01012023', p_wiederanlaufWert => '0');`
*   **Pass/Fail Criterion**:
    1.  **Legacy Script Exit Status**: Script exits with `returncode = 1`. `stderr` or `stdout` contains "FEHLER: 0 E 1 Jobkennung".
    2.  **Migrated SP Behavior**:
        *   The SP completes without populating `target_result_table` or `job_tracking`.
        *   A record exists in `my_project.my_dataset.job_error_log` with:
            *   `job_name = 'r_ausd_bp_ta_bpr_apn'`
            *   `error_nr = 1`
            *   `error_arg = 'Jobkennung'`
            *   `created_ts` within a reasonable time window.
        *   The SP output (if captured) should contain "FEHLER: 0 E 1 Jobkennung".

```python
def test_missing_job_kennung_parameter(bq_client, cleanup_bq_tables, setup_legacy_mock_env):
    """
    Purpose: Verify correct error handling for a missing Job ID parameter.
    """
    eintrags_nr = "102"
    stichtag = "01012023"
    wiederanlauf_wert = "0"

    # Action: Run legacy script (missing -j)
    legacy_result = run_legacy_script(None, eintrags_nr, stichtag, wiederanlauf_wert)
    assert legacy_result["returncode"] == 1, f"Legacy script did not exit with error 1: {legacy_result['stderr']}"
    assert "FEHLER: 0 E 1 Jobkennung" in legacy_result["stdout"] or "FEHLER: 0 E 1 Jobkennung" in legacy_result["stderr"]

    # Action: Run migrated SP (passing NULL for p_JobKennung)
    migrated_result = run_migrated_sp(bq_client, None, eintrags_nr, stichtag, wiederanlauf_wert)
    assert migrated_result["success"], f"Migrated SP unexpectedly failed with an exception: {migrated_result['error']}"

    # Pass/Fail Criterion: Check error log
    bq_error_log_query = f"""
        SELECT job_name, error_nr, error_arg
        FROM {BIGQUERY_ERROR_LOG_TABLE}
        WHERE job_name = '{BIGQUERY_SP_NAME}' AND error_nr = 1 AND error_arg = 'Jobkennung'
        ORDER BY created_ts DESC LIMIT 1
    """
    error_entry = list(bq_client.query(bq_error_log_query).result())
    assert len(error_entry) == 1, "Error log entry for missing Jobkennung not found."
    assert error_entry[0].job_name == BIGQUERY_SP_NAME
    assert error_entry[0].error_nr == 1
    assert error_entry[0].error_arg == 'Jobkennung'

    # Ensure no data was processed
    bq_target_count_query = f"SELECT COUNT(*) FROM {BIGQUERY_TARGET_TABLE}"
    bq_target_count = bq_client.query(bq_target_count_query).result().scalar_iter().next()
    assert bq_target_count == 0, "Target table should be empty after error."

    bq_job_tracking_count_query = f"SELECT COUNT(*) FROM {BIGQUERY_JOB_TRACKING_TABLE}"
    bq_job_tracking_count = bq_client.query(bq_job_tracking_count_query).result().scalar_iter().next()
    assert bq_job_tracking_count == 0, "Job tracking table should be empty after error."

```

#### 3. Transformation Correctness: Parameter Validation - Invalid Stichtag Format

*   **Purpose**: Verify that the migrated SP correctly handles an invalid `Stichtag` format, logging an error and exiting, mirroring the legacy script.
*   **Setup**: Same as Test Case 2.
*   **Action**:
    1.  Execute the legacy script: `k_ausd_bp_ta_bpr_apn.ksh -j "JOB2" -f "100" -s "2023-01-01"` (invalid format `YYYY-MM-DD` instead of `DDMMYYYY`)
    2.  Execute the migrated BigQuery Stored Procedure: `CALL my_project.my_dataset.r_ausd_bp_ta_bpr_apn('JOB2', '100', '2023-01-01', '0');`
*   **Pass/Fail Criterion**:
    1.  **Legacy Script Exit Status**: Script exits with `returncode = 193`. `stderr` or `stdout` contains "FEHLER: 0 E 193 2023-01-01".
    2.  **Migrated SP Behavior**:
        *   The SP completes without populating `target_result_table` or `job_tracking`.
        *   A record exists in `my_project.my_dataset.job_error_log` with:
            *   `job_name = 'r_ausd_bp_ta_bpr_apn'`
            *   `error_nr = 193`
            *   `error_arg = '2023-01-01'`
            *   `created_ts` within a reasonable time window.
        *   The SP output should contain "FEHLER: 0 E 193 2023-01-01".

```python
def test_invalid_stichtag_format(bq_client, cleanup_bq_tables, setup_legacy_mock_env):
    """
    Purpose: Verify correct error handling for an invalid Stichtag date format.
    """
    job_kennung = "TEST_JOB_INVALID_DATE"
    eintrags_nr = "103"
    stichtag_invalid = "2023-01-01" # Expected DDMMYYYY
    wiederanlauf_wert = "0"

    # Action: Run legacy script
    legacy_result = run_legacy_script(job_kennung, eintrags_nr, stichtag_invalid, wiederanlauf_wert)
    assert legacy_result["returncode"] == 193, f"Legacy script did not exit with error 193: {legacy_result['stderr']}"
    assert "FEHLER: 0 E 193 2023-01-01" in legacy_result["stdout"] or "FEHLER: 0 E 193 2023-01-01" in legacy_result["stderr"]

    # Action: Run migrated SP
    migrated_result = run_migrated_sp(bq_client, job_kennung, eintrags_nr, stichtag_invalid, wiederanlauf_wert)
    assert migrated_result["success"], f"Migrated SP unexpectedly failed with an exception: {migrated_result['error']}"

    # Pass/Fail Criterion: Check error log
    bq_error_log_query = f"""
        SELECT job_name, error_nr, error_arg
        FROM {BIGQUERY_ERROR_LOG_TABLE}
        WHERE job_name = '{BIGQUERY_SP_NAME}' AND error_nr = 193 AND error_arg = '{stichtag_invalid}'
        ORDER BY created_ts DESC LIMIT 1
    """
    error_entry = list(bq_client.query(bq_error_log_query).result())
    assert len(error_entry) == 1, "Error log entry for invalid Stichtag not found."
    assert error_entry[0].job_name == BIGQUERY_SP_NAME
    assert error_entry[0].error_nr == 193
    assert error_entry[0].error_arg == stichtag_invalid

    # Ensure no data was processed
    bq_target_count_query = f"SELECT COUNT(*) FROM {BIGQUERY_TARGET_TABLE}"
    bq_target_count = bq_client.query(bq_target_count_query).result().scalar_iter().next()
    assert bq_target_count == 0, "Target table should be empty after error."

    bq_job_tracking_count_query = f"SELECT COUNT(*) FROM {BIGQUERY_JOB_TRACKING_TABLE}"
    bq_job_tracking_count = bq_client.query(bq_job_tracking_count_query).result().scalar_iter().next()
    assert bq_job_tracking_count == 0, "Job tracking table should be empty after error."

```

#### 4. Transformation Correctness: Default `p_wiederanlaufWert` Handling

*   **Purpose**: Verify that `p_wiederanlaufWert` defaults to '0' if not provided (or provided as an empty string), and this value is correctly passed to the core SQL SP and reflected in job tracking.
*   **Setup**:
    *   Legacy mock environment is configured.
    *   BigQuery mock `d_ausd_bp_ta_bpr_apn` SP is deployed to log parameters.
    *   All relevant BigQuery tables are empty.
*   **Action**:
    1.  Execute the legacy script: `k_ausd_bp_ta_bpr_apn.ksh -j "JOB3" -f "100" -s "02012023"` (missing `-l`)
    2.  Execute the migrated BigQuery Stored Procedure: `CALL my_project.my_dataset.r_ausd_bp_ta_bpr_apn('JOB3', '100', '02012023', NULL);`
*   **Pass/Fail Criterion**:
    1.  **Legacy Script Exit Status**: Script exits with return code 0.
    2.  **Migrated SP Parameter Passing**: The `my_project.my_dataset.d_ausd_bp_ta_bpr_apn_params_log` table should contain an entry for `JOB3` where `p_wiederanlaufWert` is '0'.
    3.  **Job Tracking Entry**: The `my_project.my_dataset.job_tracking` table should contain an entry for `JOB3` where `restart_flag = 'N'` (assuming '0' maps to 'N').

```python
def test_default_wiederanlauf_wert(bq_client, cleanup_bq_tables, setup_legacy_mock_env):
    """
    Purpose: Verify p_wiederanlaufWert defaults to '0' when not provided and is passed correctly.
    """
    job_kennung = "TEST_JOB_DEFAULT_RESTART"
    eintrags_nr = "104"
    stichtag = "02012023"
    stichtag_date_bq = "2023-01-02"

    # Action: Run legacy script (missing -l)
    legacy_result = run_legacy_script(job_kennung, eintrags_nr, stichtag, "") # Pass empty string for missing param
    assert legacy_result["returncode"] == 0, f"Legacy script failed: {legacy_result['stderr']}"

    # Action: Run migrated SP (passing NULL for p_wiederanlaufWert)
    migrated_result = run_migrated_sp(bq_client, job_kennung, eintrags_nr, stichtag, None)
    assert migrated_result["success"], f"Migrated SP failed: {migrated_result['error']}"

    # Pass/Fail Criterion 1: Check parameters passed to d_ausd_bp_ta_bpr_apn
    bq_params_log_query = f"""
        SELECT p_wiederanlaufWert
        FROM {BIGQUERY_PARAMS_LOG_TABLE}
        WHERE p_JobKennung = '{job_kennung}'
        ORDER BY v_datum_heute DESC LIMIT 1
    """
    params_entry = list(bq_client.query(bq_params_log_query).result())
    assert len(params_entry) == 1, "Parameters log entry not found."
    assert params_entry[0].p_wiederanlaufWert == '0', "p_wiederanlaufWert was not defaulted to '0'."

    # Pass/Fail Criterion 2: Check job_tracking entry for restart_flag
    bq_job_tracking_query = f"""
        SELECT restart_flag
        FROM {BIGQUERY_JOB_TRACKING_TABLE}
        WHERE tab_name = 'PoolBasisprodukt' AND stichtag_from = '{stichtag_date_bq}'
        ORDER BY created_ts DESC LIMIT 1
    """
    job_tracking_entry = list(bq_client.query(bq_job_tracking_query).result())
    assert len(job_tracking_entry) == 1, "Job tracking entry not found."
    assert job_tracking_entry[0].restart_flag == 'N', "restart_flag should be 'N' for default '0'."

```

#### 5. Transformation Correctness: Date Derivation

*   **Purpose**: Verify that `v_datum_heute` and `v_datum_gestern` are correctly calculated in the BigQuery SP, matching the `gestern.ksh` utility.
*   **Setup**:
    *   Legacy mock `gestern.ksh` is configured to output today's and yesterday's dates.
    *   BigQuery mock `d_ausd_bp_ta_bpr_apn` SP is deployed to log `v_datum_heute` and `v_datum_gestern`.
    *   All relevant BigQuery tables are empty.
*   **Action**:
    1.  Execute `gestern.ksh` directly and capture output.
    2.  Execute the migrated BigQuery Stored Procedure: `CALL my_project.my_dataset.r_ausd_bp_ta_bpr_apn('JOB4', '100', '03012023', '0');`
*   **Pass/Fail Criterion**:
    1.  **Legacy Date Output**: `gestern.ksh` output (e.g., "2023-01-03 2023-01-02") matches `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` for the execution date.
    2.  **Migrated SP Date Parameters**: The `d_ausd_bp_ta_bpr_apn_params_log` table should contain `v_datum_heute` as `CURRENT_DATE()` and `v_datum_gestern` as `CURRENT_DATE() - 1 day` for the date the SP was executed.

```python
def test_date_derivation_correctness(bq_client, cleanup_bq_tables, setup_legacy_mock_env):
    """
    Purpose: Verify that v_datum_heute and v_datum_gestern are correctly calculated.
    """
    job_kennung = "TEST_JOB_DATE_DERIVATION"
    eintrags_nr = "105"
    stichtag = "03012023"
    wiederanlauf_wert = "0"

    # Action: Run legacy gestern.ksh
    legacy_gestern_cmd = f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh"
    legacy_gestern_result = subprocess.run(legacy_gestern_cmd, shell=True, capture_output=True, text=True, check=True)
    legacy_heute_str, legacy_gestern_str = legacy_gestern_result.stdout.strip().split()
    legacy_heute_date = datetime.strptime(legacy_heute_str, "%Y-%m-%d").date()
    legacy_gestern_date = datetime.strptime(legacy_gestern_str, "%Y-%m-%d").date()

    # Action: Run migrated SP
    migrated_result = run_migrated_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    assert migrated_result["success"], f"Migrated SP failed: {migrated_result['error']}"

    # Pass/Fail Criterion: Check parameters passed to d_ausd_bp_ta_bpr_apn
    bq_params_log_query = f"""
        SELECT v_datum_heute, v_datum_gestern
        FROM {BIGQUERY_PARAMS_LOG_TABLE}
        WHERE p_JobKennung = '{job_kennung}'
        ORDER BY v_datum_heute DESC LIMIT 1
    """
    params_entry = list(bq_client.query(bq_params_log_query).result())
    assert len(params_entry) == 1, "Parameters log entry not found."

    # Compare with legacy output
    assert params_entry[0].v_datum_heute == legacy_heute_date
    assert params_entry[0].v_datum_gestern == legacy_gestern_date

    # Also verify against BigQuery's own date functions for robustness
    expected_bq_heute = datetime.now().date()
    expected_bq_gestern = (datetime.now() - timedelta(days=1)).date()
    assert params_entry[0].v_datum_heute == expected_bq_heute
    assert params_entry[0].v_datum_gestern == expected_bq_gestern

```

#### 6. External-System Replacements: Core SQL Invocation Parameters

*   **Purpose**: Verify that the parameters passed from the orchestrator to the core data processing logic (`d_ausd_bp_ta_bpr_apn`) are identical between legacy and migrated systems. This is crucial for ensuring the core transformation receives the correct inputs.
*   **Setup**:
    *   Legacy mock `starteSQLSkript` is configured to log its arguments to `stderr`.
    *   BigQuery mock `d_ausd_bp_ta_bpr_apn` SP is deployed to log all its arguments to `d_ausd_bp_ta_bpr_apn_params_log`.
    *   All relevant BigQuery tables are empty.
*   **Action**:
    1.  Execute the legacy script: `k_ausd_bp_ta_bpr_apn.ksh -j "JOB5" -f "106" -s "04012023" -l "1"`
    2.  Execute the migrated BigQuery Stored Procedure: `CALL my_project.my_dataset.r_ausd_bp_ta_bpr_apn('JOB5', '106', '04012023', '1');`
*   **Pass/Fail Criterion**:
    1.  **Legacy Script Output**: The `stderr` of the legacy script should contain the parameters passed to `starteSQLSkript`.
    2.  **Migrated SP Parameter Logging**: The `my_project.my_dataset.d_ausd_bp_ta_bpr_apn_params_log` table should contain an entry for `JOB5` with parameters matching the expected values derived from the legacy script's invocation.
    3.  Specifically, the values for `p_EintragsNr`, `p_JobKennung`, `p_Stichtag`, `p_datum_heute`, `p_datum_gestern`, and `p_wiederanlaufWert` (from legacy) must map correctly to `p_EintragsNr_1`, `p_EintragsNr_2`, `p_JobKennung`, `p_Stichtag`, `v_datum_heute`, `v_datum_gestern`, `p_wiederanlaufWert` (migrated).

```python
def test_core_sql_invocation_parameters(bq_client, cleanup_bq_tables, setup_legacy_mock_env):
    """
    Purpose: Verify that parameters passed to the core SQL logic are identical.
    """
    job_kennung = "TEST_JOB_CORE_PARAMS"
    eintrags_nr = "106"
    stichtag = "04012023"
    wiederanlauf_wert = "1"
    stichtag_date_bq = "2023-01-04"

    # Get expected dates for comparison (based on current execution time)
    expected_heute = datetime.now().date()
    expected_gestern = (datetime.now() - timedelta(days=1)).date()

    # Action: Run legacy script
    legacy_result = run_legacy_script(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    assert legacy_result["returncode"] == 0, f"Legacy script failed: {legacy_result['stderr']}"
    assert f"Mock starteSQLSkript called with args: {eintrags_nr} {LEGACY_BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_apn.sql {eintrags_nr} {job_kennung} {stichtag} {LEGACY_DW_DIR_UTL}/bert_k_ausd_bp_ta_bpr_apn.tmp {LEGACY_BERT_DIR_ROOT} {expected_heute} {expected_gestern}" in legacy_result["stderr"]

    # Action: Run migrated SP
    migrated_result = run_migrated_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    assert migrated_result["success"], f"Migrated SP failed: {migrated_result['error']}"

    # Pass/Fail Criterion: Check parameters passed to d_ausd_bp_ta_bpr_apn
    bq_params_log_query = f"""
        SELECT p_EintragsNr_1, p_EintragsNr_2, p_JobKennung, p_Stichtag, v_datum_heute, v_datum_gestern, p_wiederanlaufWert
        FROM {BIGQUERY_PARAMS_LOG_TABLE}
        WHERE p_JobKennung = '{job_kennung}'
        ORDER BY v_datum_heute DESC LIMIT 1
    """
    params_entry = list(bq_client.query(bq_params_log_query).result())
    assert len(params_entry) == 1, "Parameters log entry not found."

    # Compare parameters
    assert params_entry[0].p_EintragsNr_1 == eintrags_nr
    assert params_entry[0].p_EintragsNr_2 == eintrags_nr # As per design document
    assert params_entry[0].p_JobKennung == job_kennung
    assert params_entry[0].p_Stichtag == stichtag
    assert params_entry[0].v_datum_heute == expected_heute
    assert params_entry[0].v_datum_gestern == expected_gestern
    assert params_entry[0].p_wiederanlaufWert == wiederanlauf_wert

```

#### 7. Commented-Out Logic (Non-Execution)

*   **Purpose**: Verify that the commented-out file processing logic (`sed`, `sort`, `join`) is *not* executed in the migrated solution, unless explicitly enabled by design. This ensures no unintended side effects.
*   **Setup**:
    *   Legacy mock environment is configured.
    *   BigQuery mock `d_ausd_bp_ta_bpr_apn` SP is deployed.
    *   All relevant BigQuery tables are empty.
*   **Action**:
    1.  Execute the legacy script: `k_ausd_bp_ta_bpr_apn.ksh -j "JOB_NO_FILE_PROC" -f "107" -s "05012023" -l "0"`
    2.  Execute the migrated BigQuery Stored Procedure: `CALL my_project.my_dataset.r_ausd_bp_ta_bpr_apn('JOB_NO_FILE_PROC', '107', '05012023', '0');`
*   **Pass/Fail Criterion**:
    1.  **Legacy Script**: No files like `cibasis_data24.sed`, `cibasis_24_96.tmp`, `cibasisprodukt.csv` are created or modified in the `BERT_DIR_ROOT/aufbereitung/sql` directory.
    2.  **Migrated SP**: No BigQuery tables (e.g., `cibasis_data24_clean`, `cibasis_24_96_tmp`, `cibasisprodukt_csv`) related to the commented-out file processing logic are created or modified in `my_project.my_dataset`.
    3.  The main job should still process data and log tracking as expected.

```python
def test_commented_out_file_processing_not_executed(bq_client, cleanup_bq_tables, setup_legacy_mock_env):
    """
    Purpose: Verify that commented-out file processing logic is not executed in the migrated solution.
    """
    job_kennung = "TEST_JOB_NO_FILE_PROC"
    eintrags_nr = "107"
    stichtag = "05012023"
    wiederanlauf_wert = "0"
    stichtag_date_bq = "2023-01-05"

    # Action: Run legacy script
    # Ensure no file artifacts are created by the legacy script.
    # This requires a clean environment for the legacy script run.
    legacy_file_paths = [
        f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/sql/cibasis_data24.sed",
        f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/sql/cibasis_data96.sed",
        f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/sql/cibasis_fax.sed",
        f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/sql/cibasis_24_96.tmp",
        f"{LEGACY_BERT_DIR_ROOT}/aufbereitung/sql/cibasisprodukt.csv"
    ]
    for f_path in legacy_file_paths:
        subprocess.run(f"rm -f {f_path}", shell=True, check=False) # Ensure they don't exist before run

    legacy_result = run_legacy_script(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    assert legacy_result["returncode"] == 0, f"Legacy script failed: {legacy_result['stderr']}"

    # Pass/Fail Criterion 1 (Legacy): Check for absence of files
    for f_path in legacy_file_paths:
        assert not os.path.exists(f_path), f"Legacy file {f_path} was unexpectedly created."

    # Action: Run migrated SP
    migrated_result = run_migrated_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    assert migrated_result["success"], f"Migrated SP failed: {migrated_result['error']}"

    # Pass/Fail Criterion 2 (Migrated): Check BigQuery for absence of related tables
    bq_file_processing_tables = [
        f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cibasis_data24_clean",
        f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cibasis_24_96_tmp",
        f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cibasisprodukt_csv"
    ]

    for table_full_name in bq_file_processing_tables:
        table_name = table_full_name.split('.')[-1]
        query = f"""
            SELECT COUNT(*)
            FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table_name}'
        """
        table_exists = bq_client.query(query).result().scalar_iter().next() > 0
        assert not table_exists, f"BigQuery table {table_full_name} related to commented-out logic was unexpectedly created."

    # Ensure the main job still processed data and logged tracking
    bq_target_count_query = f"SELECT COUNT(*) FROM {BIGQUERY_TARGET_TABLE} WHERE stichtag = '{stichtag_date_bq}'"
    bq_target_count = bq_client.query(bq_target_count_query).result().scalar_iter().next()
    assert bq_target_count > 0, "Main job did not process data."

    bq_job_tracking_count_query = f"SELECT COUNT(*) FROM {BIGQUERY_JOB_TRACKING_TABLE} WHERE stichtag_from = '{stichtag_date_bq}'"
    bq_job_tracking_count = bq_client.query(bq_job_tracking_count_query).result().scalar_iter().next()
    assert bq_job_tracking_count == 1, "Job tracking entry not found."
```