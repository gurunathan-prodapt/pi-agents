As a senior data-migration QA engineer, I've developed a suite of validation tests for the migration of `k_ausd_bp_ta_bpr_apn.ksh` to a BigQuery Stored Procedure. These tests aim to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

The tests are designed to be run using `pytest` and interact with Google Cloud BigQuery.

---

# Migration Validation Tests for `k_ausd_bp_ta_bpr_apn.ksh`

This document outlines the test cases designed to validate the migration of the KornShell script `k_ausd_bp_ta_bpr_apn.ksh` to a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_apn`). The tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality/schema assertions.

**Assumptions & Scope:**

*   The core business logic from `d_ausd_bp_ta_bpr_apn.sql` is assumed to be correctly translated into BigQuery Standard SQL and embedded within the `r_ausd_bp_ta_bpr_apn` stored procedure's `EXECUTE IMMEDIATE` block. The provided placeholder `UPDATE` statement in the generated SP code will be used as the target behavior for data transformation.
*   The `gestern.ksh` logic for `p_datum_heute` and `p_datum_gestern` is replaced by BigQuery's native `CURRENT_DATE()` and `DATE_SUB` functions, whose correctness is assumed. The tests will verify their availability and usage in the dynamic SQL.
*   External system replacements (e.g., `h_alis_sqlplus.ksh` to `EXECUTE IMMEDIATE`) are validated by observing the successful execution and data changes in BigQuery.
*   The commented-out `sed`, `sort`, `join` operations are not part of the current migration scope and are therefore not explicitly tested.
*   The Airflow DAG (`k_ausd_bp_ta_bpr_apn_dag.py`) is responsible for *triggering* the BigQuery SP. The tests below focus on the SP's internal logic and its interaction with BigQuery tables, assuming the DAG successfully invokes the SP.

**Pre-requisites:**

*   A Google Cloud Project (`your_gcp_project`) and BigQuery Dataset (`your_bigquery_dataset`) are set up.
*   The DDLs for `error_log`, `job_log`, and `PoolBasisprodukt` tables have been executed in the target BigQuery dataset.
*   The BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` has been deployed to the target BigQuery dataset.
*   Python environment with `pytest` and `google-cloud-bigquery` library installed.
*   `GCP_PROJECT_ID` and `BIGQUERY_DATASET` environment variables are set or replaced in the test code.
*   Authentication to GCP is configured (e.g., `gcloud auth application-default login`).

---

### Common Pytest Fixtures and Helper Functions

These fixtures and helper functions should be placed in a `conftest.py` file or a dedicated test utility file (`test_utils.py`) in your pytest test suite.

```python
# conftest.py or test_utils.py
import pytest
from google.cloud import bigquery
import os
from datetime import datetime, timedelta

# --- Configuration ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your_gcp_project")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "your_bigquery_dataset")
BQ_SP_NAME = "r_ausd_bp_ta_bpr_apn"
BQ_ERROR_LOG_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.error_log"
BQ_JOB_LOG_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.job_log"
BQ_POOLBASISPRODUKT_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.PoolBasisprodukt"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    yield client
    client.close()

@pytest.fixture(autouse=True)
def cleanup_tables(bq_client):
    """Cleans up log and PoolBasisprodukt tables before and after each test."""
    # Before test
    bq_client.query(f"TRUNCATE TABLE `{BQ_ERROR_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_POOLBASISPRODUKT_TABLE}`").result()

    # Insert some initial data into PoolBasisprodukt for testing updates
    # This ensures there's always some data to work with, even if not directly targeted.
    initial_data_query = f"""
    INSERT INTO `{BQ_POOLBASISPRODUKT_TABLE}` (produkt_id, basis_datum, wert, beschreibung, last_updated) VALUES
    ('PROD_INIT_01', CURRENT_DATE(), 100.0, 'Initial product data', CURRENT_TIMESTAMP()),
    ('PROD_INIT_02', CURRENT_DATE(), 200.0, 'Initial product data', CURRENT_TIMESTAMP()),
    ('PROD_INIT_03', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), 150.0, 'Old product data', CURRENT_TIMESTAMP()),
    ('PROD_INIT_04', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), 250.0, 'Older product data', CURRENT_TIMESTAMP());
    """
    bq_client.query(initial_data_query).result()

    yield # Run the test

    # After test (optional, but good for ensuring clean state for subsequent runs if autouse=False)
    bq_client.query(f"TRUNCATE TABLE `{BQ_ERROR_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_POOLBASISPRODUKT_TABLE}`").result()

def call_bq_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert=None):
    """Helper function to call the BigQuery Stored Procedure."""
    params = [
        bigquery.ScalarQueryParameter("p_jobkennung", "STRING", job_kennung),
        bigquery.ScalarQueryParameter("p_eintragsnr", "STRING", eintrags_nr),
        bigquery.ScalarQueryParameter("p_stichtag", "STRING", stichtag),
        bigquery.ScalarQueryParameter("p_wiederanlaufwert", "STRING", wiederanlauf_wert),
    ]
    query = f"CALL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BQ_SP_NAME}`(?, ?, ?, ?)"
    try:
        job = bq_client.query(query, query_parameters=params)
        job.result() # Wait for the job to complete
        return True, None
    except Exception as e:
        return False, str(e)

def get_table_rows(bq_client, table_id):
    """Helper to fetch all rows from a table."""
    query = f"SELECT * FROM `{table_id}` ORDER BY created_at DESC"
    rows = bq_client.query(query).result()
    return list(rows)

def get_table_row_count(bq_client, table_id):
    """Helper to fetch row count from a table."""
    query = f"SELECT COUNT(*) FROM `{table_id}`"
    rows = bq_client.query(query).result()
    return next(iter(rows))[0]

```

---

## 1. Output Parity & Job Logging: Successful Execution

**Purpose:** To verify that a successful execution of the BigQuery Stored Procedure with valid parameters results in the expected updates to the `PoolBasisprodukt` table and a correct entry in the `job_log` table, mirroring the legacy script's intended behavior. This covers output parity and job logging.

**Setup:**
1.  The `cleanup_tables` fixture ensures `error_log`, `job_log`, and `PoolBasisprodukt` tables are clean and `PoolBasisprodukt` has initial data.
2.  Additional data is inserted into `PoolBasisprodukt` specifically for the `p_stichtag` to be processed.

**Action:**
1.  Call the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` with valid parameters: `p_jobkennung='TEST_JOB_001'`, `p_eintragsnr='123'`, `p_stichtag='DDMMYYYY'` (for a date with existing data), `p_wiederanlaufwert=NULL`.

**Pass/Fail Criterion:**
*   The BigQuery Stored Procedure executes successfully without raising an error.
*   One record is inserted into the `job_log` table.
*   The `job_log` entry contains:
    *   `tab_name = 'PoolBasisprodukt'`
    *   `status = 'SUCCESS'`
    *   `record_count` matches the number of records updated in `PoolBasisprodukt` for the given `p_stichtag`.
    *   `stichtag_from` and `stichtag_to` match the parsed `p_stichtag`.
    *   `job_kennung`, `eintragsnr` match input parameters.
    *   `restart_flag = 'N'` (since `p_wiederanlaufwert` was NULL).
*   The `PoolBasisprodukt` table shows that records with `basis_datum` matching the `p_stichtag` have their `beschreibung` and `last_updated` fields updated as per the placeholder SQL logic. Records for other dates remain unchanged.
*   The `error_log` table remains empty.

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, cleanup_tables, call_bq_sp, get_table_rows, get_table_row_count, BQ_POOLBASISPRODUKT_TABLE, BQ_JOB_LOG_TABLE, BQ_ERROR_LOG_TABLE
from datetime import datetime, timedelta

def test_successful_execution_and_logging(bq_client):
    """
    Tests successful execution of the SP, verifying job_log entry and data updates.
    """
    # Setup: Ensure initial data for a specific stichtag
    test_stichtag_str = (datetime.now() - timedelta(days=5)).strftime("%d%m%Y")
    test_stichtag_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    bq_client.query(f"""
    INSERT INTO `{BQ_POOLBASISPRODUKT_TABLE}` (produkt_id, basis_datum, wert, beschreibung, last_updated) VALUES
    ('PROD_ST_01', DATE('{test_stichtag_date}'), 300.0, 'Stichtag data 1', CURRENT_TIMESTAMP()),
    ('PROD_ST_02', DATE('{test_stichtag_date}'), 400.0, 'Stichtag data 2', CURRENT_TIMESTAMP());
    """).result()

    initial_poolbasisprodukt_count = get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE)
    initial_job_log_count = get_table_row_count(bq_client, BQ_JOB_LOG_TABLE)
    initial_error_log_count = get_table_row_count(bq_client, BQ_ERROR_LOG_TABLE)

    # Action
    job_kennung = 'TEST_JOB_001'
    eintrags_nr = '123'
    wiederanlauf_wert = None # Should default to 'N' in job_log

    success, error_msg = call_bq_sp(bq_client, job_kennung, eintrags_nr, test_stichtag_str, wiederanlauf_wert)

    # Assertions
    assert success, f"Stored Procedure execution failed: {error_msg}"

    # Verify job_log entry
    job_logs = get_table_rows(bq_client, BQ_JOB_LOG_TABLE)
    assert len(job_logs) == initial_job_log_count + 1, "Expected one new entry in job_log"
    job_log_entry = job_logs[0] # Most recent entry

    assert job_log_entry.tab_name == 'PoolBasisprodukt'
    assert job_log_entry.status == 'SUCCESS'
    assert job_log_entry.job_kennung == job_kennung
    assert job_log_entry.eintragsnr == eintrags_nr
    assert job_log_entry.stichtag_from.strftime("%Y-%m-%d") == test_stichtag_date
    assert job_log_entry.stichtag_to.strftime("%Y-%m-%d") == test_stichtag_date
    assert job_log_entry.restart_flag == 'N' # Defaulted from NULL

    # Verify record_count in job_log
    expected_updated_records = bq_client.query(f"SELECT COUNT(*) FROM `{BQ_POOLBASISPRODUKT_TABLE}` WHERE basis_datum = DATE('{test_stichtag_date}')").result().next()[0]
    assert job_log_entry.record_count == expected_updated_records, "Record count in job_log does not match updated records"

    # Verify PoolBasisprodukt updates
    updated_records_query = f"""
    SELECT produkt_id, beschreibung FROM `{BQ_POOLBASISPRODUKT_TABLE}`
    WHERE basis_datum = DATE('{test_stichtag_date}')
    """
    updated_records = list(bq_client.query(updated_records_query).result())
    assert len(updated_records) == expected_updated_records

    for record in updated_records:
        assert "Processed on" in record.beschreibung

    # Verify other records are untouched (initial data + any other test-specific data)
    untouched_records_query = f"""
    SELECT COUNT(*) FROM `{BQ_POOLBASISPRODUKT_TABLE}`
    WHERE basis_datum != DATE('{test_stichtag_date}') AND beschreibung LIKE 'Initial product data%'
    """
    untouched_count = bq_client.query(untouched_records_query).result().next()[0]
    # Assuming 4 initial records, and 2 were for the stichtag, 2 should remain untouched.
    assert untouched_count == 4 - (initial_poolbasisprodukt_count - expected_updated_records), "Other records should not be updated"


    # Verify error_log is empty
    error_logs = get_table_rows(bq_client, BQ_ERROR_LOG_TABLE)
    assert len(error_logs) == initial_error_log_count, "error_log should be empty on success"

```

---

## 2. Transformation Correctness: Parameter Validation - Missing `p_JobKennung`

**Purpose:** To verify that the BigQuery Stored Procedure correctly identifies and handles missing required parameters, specifically `p_JobKennung`, by raising an error and logging it to the `error_log` table, consistent with the legacy script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.

**Setup:**
1.  The `cleanup_tables` fixture ensures `error_log`, `job_log`, and `PoolBasisprodukt` tables are clean and `PoolBasisprodukt` has initial data.

**Action:**
1.  Call the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` with `p_jobkennung=NULL` and other valid parameters: `p_eintragsnr='123'`, `p_stichtag='01012024'`, `p_wiederanlaufwert=NULL`.

**Pass/Fail Criterion:**
*   The BigQuery Stored Procedure execution fails and raises an error.
*   One record is inserted into the `error_log` table.
*   The `error_log` entry contains:
    *   `job_name = 'k_ausd_bp_ta_bpr_apn'`
    *   `error_nr = 192` (as per design document for missing/unknown parameter).
    *   `error_message` containing "Required parameters (Jobkennung, EintragsNr, Stichtag) are missing."
*   The `job_log` table remains empty.
*   The `PoolBasisprodukt` table remains unchanged.

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, cleanup_tables, call_bq_sp, get_table_rows, get_table_row_count, BQ_POOLBASISPRODUKT_TABLE, BQ_JOB_LOG_TABLE, BQ_ERROR_LOG_TABLE

def test_missing_jobkennung_parameter(bq_client):
    """
    Tests that the SP fails and logs an error when p_jobkennung is missing.
    """
    initial_job_log_count = get_table_row_count(bq_client, BQ_JOB_LOG_TABLE)
    initial_error_log_count = get_table_row_count(bq_client, BQ_ERROR_LOG_TABLE)
    initial_poolbasisprodukt_count = get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE)

    # Action
    job_kennung = None # Missing parameter
    eintrags_nr = '123'
    stichtag = '01012024'
    wiederanlauf_wert = None

    success, error_msg = call_bq_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)

    # Assertions
    assert not success, "Stored Procedure should have failed due to missing p_jobkennung"
    assert "Required parameters (Jobkennung, EintragsNr, Stichtag) are missing." in error_msg

    # Verify error_log entry
    error_logs = get_table_rows(bq_client, BQ_ERROR_LOG_TABLE)
    assert len(error_logs) == initial_error_log_count + 1, "Expected one new entry in error_log"
    error_log_entry = error_logs[0]

    assert error_log_entry.job_name == 'k_ausd_bp_ta_bpr_apn'
    assert error_log_entry.error_nr == 192 # As per design, 192 for unknown/missing param
    assert "Required parameters (Jobkennung, EintragsNr, Stichtag) are missing." in error_log_entry.error_message

    # Verify job_log is empty
    job_logs = get_table_rows(bq_client, BQ_JOB_LOG_TABLE)
    assert len(job_logs) == initial_job_log_count, "job_log should be empty on failure"

    # Verify PoolBasisprodukt is unchanged
    assert get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE) == initial_poolbasisprodukt_count

```

---

## 3. Transformation Correctness: Parameter Validation - Invalid `p_Stichtag` Format

**Purpose:** To verify that the BigQuery Stored Procedure correctly validates the `p_Stichtag` format (DDMMYYYY), raising an error and logging it if the format is incorrect, mirroring the legacy script's `DWDate_Datum_Check` behavior.

**Setup:**
1.  The `cleanup_tables` fixture ensures `error_log`, `job_log`, and `PoolBasisprodukt` tables are clean and `PoolBasisprodukt` has initial data.

**Action:**
1.  Call the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` with an invalid `p_stichtag` format (e.g., `YYYY-MM-DD` or `DD/MM/YYYY`): `p_jobkennung='TEST_JOB_002'`, `p_eintragsnr='124'`, `p_stichtag='2024-01-01'`, `p_wiederanlaufwert=NULL`.

**Pass/Fail Criterion:**
*   The BigQuery Stored Procedure execution fails and raises an error.
*   One record is inserted into the `error_log` table.
*   The `error_log` entry contains:
    *   `job_name = 'k_ausd_bp_ta_bpr_apn'`
    *   `error_nr = 193` (as per design document for invalid argument/format).
    *   `error_message` containing "Stichtag format invalid. Expected DDMMYYYY."
*   The `job_log` table remains empty.
*   The `PoolBasisprodukt` table remains unchanged.

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, cleanup_tables, call_bq_sp, get_table_rows, get_table_row_count, BQ_POOLBASISPRODUKT_TABLE, BQ_JOB_LOG_TABLE, BQ_ERROR_LOG_TABLE

def test_invalid_stichtag_format(bq_client):
    """
    Tests that the SP fails and logs an error when p_stichtag has an invalid format.
    """
    initial_job_log_count = get_table_row_count(bq_client, BQ_JOB_LOG_TABLE)
    initial_error_log_count = get_table_row_count(bq_client, BQ_ERROR_LOG_TABLE)
    initial_poolbasisprodukt_count = get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE)

    # Action
    job_kennung = 'TEST_JOB_002'
    eintrags_nr = '124'
    stichtag = '2024-01-01' # Invalid format
    wiederanlauf_wert = None

    success, error_msg = call_bq_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)

    # Assertions
    assert not success, "Stored Procedure should have failed due to invalid stichtag format"
    assert "Stichtag format invalid. Expected DDMMYYYY." in error_msg

    # Verify error_log entry
    error_logs = get_table_rows(bq_client, BQ_ERROR_LOG_TABLE)
    assert len(error_logs) == initial_error_log_count + 1, "Expected one new entry in error_log"
    error_log_entry = error_logs[0]

    assert error_log_entry.job_name == 'k_ausd_bp_ta_bpr_apn'
    assert error_log_entry.error_nr == 193 # As per design, 193 for missing arg, here used for invalid format
    assert "Stichtag format invalid. Expected DDMMYYYY." in error_log_entry.error_message

    # Verify job_log is empty
    job_logs = get_table_rows(bq_client, BQ_JOB_LOG_TABLE)
    assert len(job_logs) == initial_job_log_count, "job_log should be empty on failure"

    # Verify PoolBasisprodukt is unchanged
    assert get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE) == initial_poolbasisprodukt_count

```

---

## 4. Transformation Correctness: Parameter Validation - Invalid `p_Stichtag` Date Value

**Purpose:** To verify that the BigQuery Stored Procedure correctly validates the semantic correctness of `p_Stichtag` (e.g., '31022023' is not a real date), raising an error and logging it, mirroring the legacy script's `DWDate_Datum_Check` behavior.

**Setup:**
1.  The `cleanup_tables` fixture ensures `error_log`, `job_log`, and `PoolBasisprodukt` tables are clean and `PoolBasisprodukt` has initial data.

**Action:**
1.  Call the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` with a semantically invalid `p_stichtag` (e.g., '31022023'): `p_jobkennung='TEST_JOB_003'`, `p_eintragsnr='125'`, `p_stichtag='31022023'`, `p_wiederanlaufwert=NULL`.

**Pass/Fail Criterion:**
*   The BigQuery Stored Procedure execution fails and raises an error.
*   One record is inserted into the `error_log` table.
*   The `error_log` entry contains:
    *   `job_name = 'k_ausd_bp_ta_bpr_apn'`
    *   `error_nr = 193` (as per design document for invalid argument/format).
    *   `error_message` containing "Stichtag is not a valid date."
*   The `job_log` table remains empty.
*   The `PoolBasisprodukt` table remains unchanged.

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, cleanup_tables, call_bq_sp, get_table_rows, get_table_row_count, BQ_POOLBASISPRODUKT_TABLE, BQ_JOB_LOG_TABLE, BQ_ERROR_LOG_TABLE

def test_invalid_stichtag_date_value(bq_client):
    """
    Tests that the SP fails and logs an error when p_stichtag is not a valid date.
    """
    initial_job_log_count = get_table_row_count(bq_client, BQ_JOB_LOG_TABLE)
    initial_error_log_count = get_table_row_count(bq_client, BQ_ERROR_LOG_TABLE)
    initial_poolbasisprodukt_count = get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE)

    # Action
    job_kennung = 'TEST_JOB_003'
    eintrags_nr = '125'
    stichtag = '31022023' # Invalid date (February has no 31st)
    wiederanlauf_wert = None

    success, error_msg = call_bq_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)

    # Assertions
    assert not success, "Stored Procedure should have failed due to invalid stichtag date value"
    assert "Stichtag is not a valid date." in error_msg

    # Verify error_log entry
    error_logs = get_table_rows(bq_client, BQ_ERROR_LOG_TABLE)
    assert len(error_logs) == initial_error_log_count + 1, "Expected one new entry in error_log"
    error_log_entry = error_logs[0]

    assert error_log_entry.job_name == 'k_ausd_bp_ta_bpr_apn'
    assert error_log_entry.error_nr == 193 # As per design, 193 for missing arg, here used for invalid date
    assert "Stichtag is not a valid date." in error_log_entry.error_message

    # Verify job_log is empty
    job_logs = get_table_rows(bq_client, BQ_JOB_LOG_TABLE)
    assert len(job_logs) == initial_job_log_count, "job_log should be empty on failure"

    # Verify PoolBasisprodukt is unchanged
    assert get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE) == initial_poolbasisprodukt_count

```

---

## 5. Transformation Correctness: Date Parsing and Usage

**Purpose:** To verify that the BigQuery Stored Procedure correctly parses the `p_Stichtag` into a `DATE` type and uses it consistently for logging (`stichtag_from`, `stichtag_to`) and for the core SQL logic (e.g., filtering `PoolBasisprodukt`). This implicitly validates the date parsing and handling.

**Setup:**
1.  The `cleanup_tables` fixture ensures `error_log`, `job_log`, and `PoolBasisprodukt` tables are clean and `PoolBasisprodukt` has initial data.
2.  Additional data is inserted into `PoolBasisprodukt` specifically for the `p_stichtag` to be processed, and for other dates to ensure isolation.

**Action:**
1.  Call the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` with valid parameters, including a specific `p_stichtag` and a non-NULL `p_wiederanlaufwert`.
2.  Retrieve the `job_log` entry and the updated `PoolBasisprodukt` records.

**Pass/Fail Criterion:**
*   The `job_log` entry's `stichtag_from` and `stichtag_to` fields correctly reflect the `p_stichtag` converted to a `DATE` type.
*   The `job_log` entry's `restart_flag` correctly reflects the `p_wiederanlaufwert` input.
*   The `PoolBasisprodukt` table's records updated by the SP are precisely those matching the parsed `p_stichtag` (i.e., `basis_datum = v_stichtag_date`).

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, cleanup_tables, call_bq_sp, get_table_rows, get_table_row_count, BQ_POOLBASISPRODUKT_TABLE, BQ_JOB_LOG_TABLE
from datetime import datetime, timedelta

def test_stichtag_date_parsing_and_usage(bq_client):
    """
    Tests that p_stichtag is correctly parsed and used in job_log and core SQL.
    """
    # Setup: Insert data for a specific stichtag
    test_stichtag_str = "15032023" # DDMMYYYY
    test_stichtag_date_obj = datetime.strptime(test_stichtag_str, "%d%m%Y").date()
    test_stichtag_date_sql = test_stichtag_date_obj.strftime("%Y-%m-%d")

    bq_client.query(f"""
    INSERT INTO `{BQ_POOLBASISPRODUKT_TABLE}` (produkt_id, basis_datum, wert, beschreibung, last_updated) VALUES
    ('PROD_DATE_01', DATE('{test_stichtag_date_sql}'), 500.0, 'Stichtag data for date test', CURRENT_TIMESTAMP()),
    ('PROD_DATE_02', DATE('{test_stichtag_date_sql}'), 600.0, 'Stichtag data for date test', CURRENT_TIMESTAMP()),
    ('PROD_OTHER_01', DATE_SUB(DATE('{test_stichtag_date_sql}'), INTERVAL 1 DAY), 700.0, 'Other date data', CURRENT_TIMESTAMP());
    """).result()

    initial_poolbasisprodukt_count = get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE)
    expected_updated_count = bq_client.query(f"SELECT COUNT(*) FROM `{BQ_POOLBASISPRODUKT_TABLE}` WHERE basis_datum = DATE('{test_stichtag_date_sql}')").result().next()[0]

    # Action
    job_kennung = 'TEST_JOB_004'
    eintrags_nr = '126'
    wiederanlauf_wert = 'Y' # Test with a non-NULL wiederanlaufWert

    success, error_msg = call_bq_sp(bq_client, job_kennung, eintrags_nr, test_stichtag_str, wiederanlauf_wert)

    # Assertions
    assert success, f"Stored Procedure execution failed: {error_msg}"

    # Verify job_log entry for stichtag_from/to and restart_flag
    job_logs = get_table_rows(bq_client, BQ_JOB_LOG_TABLE)
    assert len(job_logs) == 1
    job_log_entry = job_logs[0]

    assert job_log_entry.stichtag_from.strftime("%Y-%m-%d") == test_stichtag_date_sql
    assert job_log_entry.stichtag_to.strftime("%Y-%m-%d") == test_stichtag_date_sql
    assert job_log_entry.restart_flag == 'Y' # Should reflect the input

    # Verify PoolBasisprodukt updates based on stichtag
    updated_records_query = f"""
    SELECT produkt_id, beschreibung FROM `{BQ_POOLBASISPRODUKT_TABLE}`
    WHERE basis_datum = DATE('{test_stichtag_date_sql}')
    """
    updated_records = list(bq_client.query(updated_records_query).result())
    assert len(updated_records) == expected_updated_count

    for record in updated_records:
        assert "Processed on" in record.beschreibung

    # Verify records for other dates are untouched
    untouched_records_query = f"""
    SELECT COUNT(*) FROM `{BQ_POOLBASISPRODUKT_TABLE}`
    WHERE basis_datum != DATE('{test_stichtag_date_sql}') AND beschreibung = 'Other date data'
    """
    untouched_count = bq_client.query(untouched_records_query).result().next()[0]
    assert untouched_count == 1 # Only one record with 'Other date data'

```

---

## 6. External System Replacements: SQL Execution (`starteSQLSkript` to `EXECUTE IMMEDIATE`)

**Purpose:** To verify that the BigQuery Stored Procedure's `EXECUTE IMMEDIATE` mechanism correctly executes the migrated SQL logic, effectively replacing the legacy `starteSQLSkript` (which used `sqlplus`). This tests the core transformation execution and parameter passing to the internal SQL.

**Setup:**
1.  The `cleanup_tables` fixture ensures `error_log`, `job_log`, and `PoolBasisprodukt` tables are clean and `PoolBasisprodukt` has initial data.
2.  Populate `PoolBasisprodukt` with diverse sample data, including records that should and should not be affected by the `p_stichtag`.

**Action:**
1.  Call the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` with valid parameters.
2.  Query the `PoolBasisprodukt` table to observe the changes.

**Pass/Fail Criterion:**
*   The `PoolBasisprodukt` table is updated precisely according to the logic embedded in the `EXECUTE IMMEDIATE` block.
*   Specifically, only records where `basis_datum` matches the `p_stichtag` are updated.
*   The `beschreibung` column of these records is updated to include "Processed on" and a timestamp.
*   The `last_updated` column of these records is updated.
*   Records with `basis_datum` not matching `p_stichtag` remain unchanged.
*   The `record_count` in `job_log` accurately reflects the number of updated records.

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, cleanup_tables, call_bq_sp, get_table_rows, get_table_row_count, BQ_POOLBASISPRODUKT_TABLE, BQ_JOB_LOG_TABLE
from datetime import datetime, timedelta

def test_sql_execution_and_transformation(bq_client):
    """
    Tests that the EXECUTE IMMEDIATE block correctly applies transformations
    to PoolBasisprodukt based on parameters.
    """
    # Setup: Insert data for different stichtags
    current_date_str = datetime.now().strftime("%Y-%m-%d")
    yesterday_date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    future_date_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    test_stichtag_str = (datetime.now() - timedelta(days=1)).strftime("%d%m%Y") # Yesterday
    test_stichtag_date_sql = yesterday_date_str

    bq_client.query(f"""
    INSERT INTO `{BQ_POOLBASISPRODUKT_TABLE}` (produkt_id, basis_datum, wert, beschreibung, last_updated) VALUES
    ('PROD_TODAY_01', DATE('{current_date_str}'), 10.0, 'Today data', CURRENT_TIMESTAMP()),
    ('PROD_YEST_01', DATE('{yesterday_date_str}'), 20.0, 'Yesterday data 1', CURRENT_TIMESTAMP()),
    ('PROD_YEST_02', DATE('{yesterday_date_str}'), 30.0, 'Yesterday data 2', CURRENT_TIMESTAMP()),
    ('PROD_FUTURE_01', DATE('{future_date_str}'), 40.0, 'Future data', CURRENT_TIMESTAMP());
    """).result()

    # Get initial state for comparison
    initial_poolbasisprodukt_state = list(bq_client.query(f"SELECT produkt_id, basis_datum, beschreibung, last_updated FROM `{BQ_POOLBASISPRODUKT_TABLE}` ORDER BY produkt_id").result())
    initial_yest_records = [r for r in initial_poolbasisprodukt_state if r.basis_datum.strftime("%Y-%m-%d") == yesterday_date_str]
    initial_today_records = [r for r in initial_poolbasisprodukt_state if r.basis_datum.strftime("%Y-%m-%d") == current_date_str]
    initial_future_records = [r for r in initial_poolbasisprodukt_state if r.basis_datum.strftime("%Y-%m-%d") == future_date_str]

    # Action
    job_kennung = 'TEST_SQL_EXEC'
    eintrags_nr = '127'
    wiederanlauf_wert = None

    success, error_msg = call_bq_sp(bq_client, job_kennung, eintrags_nr, test_stichtag_str, wiederanlauf_wert)

    # Assertions
    assert success, f"Stored Procedure execution failed: {error_msg}"

    # Verify job_log record count
    job_logs = get_table_rows(bq_client, BQ_JOB_LOG_TABLE)
    assert len(job_logs) == 1
    job_log_entry = job_logs[0]
    assert job_log_entry.record_count == len(initial_yest_records), "Job log record count should match updated records for stichtag"

    # Verify PoolBasisprodukt updates
    final_poolbasisprodukt_state = list(bq_client.query(f"SELECT produkt_id, basis_datum, beschreibung, last_updated FROM `{BQ_POOLBASISPRODUKT_TABLE}` ORDER BY produkt_id").result())

    # Check records for the stichtag (yesterday)
    for initial_record in initial_yest_records:
        final_record = next(r for r in final_poolbasisprodukt_state if r.produkt_id == initial_record.produkt_id)
        assert "Processed on" in final_record.beschreibung
        assert final_record.last_updated > initial_record.last_updated # Timestamp should be updated

    # Check records for today (should be untouched)
    for initial_record in initial_today_records:
        final_record = next(r for r in final_poolbasisprodukt_state if r.produkt_id == initial_record.produkt_id)
        assert final_record.beschreibung == initial_record.beschreibung
        assert final_record.last_updated == initial_record.last_updated

    # Check records for future (should be untouched)
    for initial_record in initial_future_records:
        final_record = next(r for r in final_poolbasisprodukt_state if r.produkt_id == initial_record.produkt_id)
        assert final_record.beschreibung == initial_record.beschreibung
        assert final_record.last_updated == initial_record.last_updated

```

---

## 7. Data Quality / Row Count: No Matching Data

**Purpose:** To verify that the BigQuery Stored Procedure correctly handles scenarios where no records match the processing criteria (e.g., `p_stichtag` has no corresponding data in `PoolBasisprodukt`), resulting in a `record_count` of 0 in the `job_log` and no changes to the target table.

**Setup:**
1.  The `cleanup_tables` fixture ensures `error_log`, `job_log`, and `PoolBasisprodukt` tables are clean and `PoolBasisprodukt` has initial data.
2.  Populate `PoolBasisprodukt` with sample data, but *none* for the `p_stichtag` that will be used in the test.

**Action:**
1.  Call the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_apn` with valid parameters, but for a `p_stichtag` that has no matching data in `PoolBasisprodukt`.

**Pass/Fail Criterion:**
*   The BigQuery Stored Procedure executes successfully without raising an error.
*   One record is inserted into the `job_log` table.
*   The `job_log` entry's `record_count` is `0`.
*   The `PoolBasisprodukt` table remains entirely unchanged.
*   The `error_log` table remains empty.

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, cleanup_tables, call_bq_sp, get_table_rows, get_table_row_count, BQ_POOLBASISPRODUKT_TABLE, BQ_JOB_LOG_TABLE, BQ_ERROR_LOG_TABLE
from datetime import datetime, timedelta

def test_no_matching_data_scenario(bq_client):
    """
    Tests that the SP correctly handles a scenario where no data matches the stichtag,
    resulting in 0 records processed and no table changes.
    """
    # Setup: Insert data, but NOT for the test_stichtag
    current_date_str = datetime.now().strftime("%Y-%m-%d")
    test_stichtag_str = (datetime.now() - timedelta(days=10)).strftime("%d%m%Y") # A date far in the past
    test_stichtag_date_sql = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

    bq_client.query(f"""
    INSERT INTO `{BQ_POOLBASISPRODUKT_TABLE}` (produkt_id, basis_datum, wert, beschreibung, last_updated) VALUES
    ('PROD_A', DATE('{current_date_str}'), 100.0, 'Current data', CURRENT_TIMESTAMP()),
    ('PROD_B', DATE_SUB(DATE('{current_date_str}'), INTERVAL 1 DAY), 200.0, 'Yesterday data', CURRENT_TIMESTAMP());
    """).result()

    initial_poolbasisprodukt_state = list(bq_client.query(f"SELECT produkt_id, basis_datum, beschreibung, last_updated FROM `{BQ_POOLBASISPRODUKT_TABLE}` ORDER BY produkt_id").result())
    initial_poolbasisprodukt_count = get_table_row_count(bq_client, BQ_POOLBASISPRODUKT_TABLE)
    initial_job_log_count = get_table_row_count(bq_client, BQ_JOB_LOG_TABLE)
    initial_error_log_count = get_table_row_count(bq_client, BQ_ERROR_LOG_TABLE)

    # Action
    job_kennung = 'TEST_NO_MATCH'
    eintrags_nr = '128'
    wiederanlauf_wert = None

    success, error_msg = call_bq_sp(bq_client, job_kennung, eintrags_nr, test_stichtag_str, wiederanlauf_wert)

    # Assertions
    assert success, f"Stored Procedure execution failed: {error_msg}"

    # Verify job_log entry
    job_logs = get_table_rows(bq_client, BQ_JOB_LOG_TABLE)
    assert len(job_logs) == initial_job_log_count + 1, "Expected one new entry in job_log"
    job_log_entry = job_logs[0]

    assert job_log_entry.status == 'SUCCESS'
    assert job_log_entry.record_count == 0, "Record count should be 0 when no matching data"
    assert job_log_entry.stichtag_from.strftime("%Y-%m-%d") == test_stichtag_date_sql

    # Verify PoolBasisprodukt is unchanged
    final_poolbasisprodukt_state = list(bq_client.query(f"SELECT produkt_id, basis_datum, beschreibung, last_updated FROM `{BQ_POOLBASISPRODUKT_TABLE}` ORDER BY produkt_id").result())
    assert len(final_poolbasisprodukt_state) == initial_poolbasisprodukt_count
    # Deep comparison to ensure no changes
    for i in range(len(initial_poolbasisprodukt_state)):
        assert initial_poolbasisprodukt_state[i].produkt_id == final_poolbasisprodukt_state[i].produkt_id
        assert initial_poolbasisprodukt_state[i].beschreibung == final_poolbasisprodukt_state[i].beschreibung
        assert initial_poolbasisprodukt_state[i].last_updated == final_poolbasisprodukt_state[i].last_updated

    # Verify error_log is empty
    error_logs = get_table_rows(bq_client, BQ_ERROR_LOG_TABLE)
    assert len(error_logs) == initial_error_log_count, "error_log should be empty on success with 0 records"

```

---

## 8. Schema Assertions

**Purpose:** To verify that the DDLs for the `error_log`, `job_log`, and `PoolBasisprodukt` tables are correctly applied and that the tables exist with the expected column names and data types in BigQuery.

**Setup:**
1.  Ensure the BigQuery dataset exists.
2.  The DDLs for `error_log`, `job_log`, and `PoolBasisprodukt` are executed as part of the deployment process.

**Action:**
1.  Query BigQuery's `INFORMATION_SCHEMA` to retrieve the schema details for each table.

**Pass/Fail Criterion:**
*   The `error_log` table exists and has columns: `job_name` (STRING), `error_nr` (INT64), `error_arg` (STRING), `error_message` (STRING), `created_at` (TIMESTAMP).
*   The `job_log` table exists and has columns: `tab_name` (STRING), `status` (STRING), `mode` (STRING), `stichtag_from` (DATE), `stichtag_to` (DATE), `job_type` (STRING), `restart_flag` (STRING), `record_count` (INT64), `description` (STRING), `job_kennung` (STRING), `eintragsnr` (STRING), `created_at` (TIMESTAMP).
*   The `PoolBasisprodukt` table exists and has columns: `produkt_id` (STRING), `basis_datum` (DATE), `wert` (NUMERIC), `beschreibung` (STRING), `last_updated` (TIMESTAMP) (based on the placeholder DDL).

**Runnable Test Code (pytest):**

```python
# test_k_ausd_bp_ta_bpr_apn.py
from .test_utils import bq_client, BQ_ERROR_LOG_TABLE, BQ_JOB_LOG_TABLE, BQ_POOLBASISPRODUKT_TABLE

def test_schema_assertions(bq_client):
    """
    Tests that the required tables exist and have the correct schema.
    """
    expected_error_log_schema = {
        'job_name': 'STRING',
        'error_nr': 'INT64',
        'error_arg': 'STRING',
        'error_message': 'STRING',
        'created_at': 'TIMESTAMP'
    }
    expected_job_log_schema = {
        'tab_name': 'STRING',
        'status': 'STRING',
        'mode': 'STRING',
        'stichtag_from': 'DATE',
        'stichtag_to': 'DATE',
        'job_type': 'STRING',
        'restart_flag': 'STRING',
        'record_count': 'INT64',
        'description': 'STRING',
        'job_kennung': 'STRING',
        'eintragsnr': 'STRING',
        'created_at': 'TIMESTAMP'
    }
    expected_poolbasisprodukt_schema = {
        'produkt_id': 'STRING',
        'basis_datum': 'DATE',
        'wert': 'NUMERIC',
        'beschreibung': 'STRING',
        'last_updated': 'TIMESTAMP'
    }

    def get_table_schema(client, table_id):
        table = client.get_table(table_id)
        schema_dict = {field.name: field.field_type for field in table.schema}
        return schema_dict

    # Test error_log schema
    error_log_schema = get_table_schema(bq_client, BQ_ERROR_LOG_TABLE)
    assert error_log_schema == expected_error_log_schema, f"Schema mismatch for {BQ_ERROR_LOG_TABLE}"

    # Test job_log schema
    job_log_schema = get_table_schema(bq_client, BQ_JOB_LOG_TABLE)
    assert job_log_schema == expected_job_log_schema, f"Schema mismatch for {BQ_JOB_LOG_TABLE}"

    # Test PoolBasisprodukt schema
    poolbasisprodukt_schema = get_table_schema(bq_client, BQ_POOLBASISPRODUKT_TABLE)
    assert poolbasisprodukt_schema == expected_poolbasisprodukt_schema, f"Schema mismatch for {BQ_POOLBASISPRODUKT_TABLE}"

```