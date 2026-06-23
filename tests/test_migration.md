The following migration validation tests are designed to ensure that the migrated BigQuery Stored Procedure and Airflow DAG for `r_ausd_bp_ta_bpr_optionen.ksh` are behaviorally equivalent to the legacy KornShell/Oracle job.

## Test Environment Setup

Before running the tests, ensure the following are in place:

*   **GCP Project:** Replace `your_gcp_project` with your actual GCP project ID.
*   **BigQuery Dataset:** Replace `your_bq_dataset` with your actual BigQuery dataset ID.
*   **BigQuery Tables:** The following tables must be created in your BigQuery dataset using the provided `create_*.sql` scripts:
    *   `your_gcp_project.your_bq_dataset.dwtk_meldungen`
    *   `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` (Target table)
    *   `your_gcp_project.your_bq_dataset.sof_ta_bpr_instance` (Source table)
    *   `your_gcp_project.your_bq_dataset.job_audit_log` (Logging table)
*   **BigQuery Stored Procedure:** The `your_bq_dataset.r_ausd_bp_ta_bpr_optionen` stored procedure must be deployed.
*   **Airflow DAG:** The `r_ausd_bp_ta_bpr_optionen` DAG must be deployed and accessible.
*   **Legacy System Access:** A mechanism to execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script and query the Oracle `sof$ta_bpr_optionen` table is required for true output parity. For the purpose of these test descriptions, we will simulate the expected legacy output based on the design document's interpretation.

**Assumptions for Testing:**

*   The `DWH_VERTRAG_ID` filter (`bp.DWH_VERTRAG_ID > p_wiederanlaufWert`) is an **intended behavioral change/clarification** from the legacy system's `p_wiederanlaufWert` parameter description, even if not explicitly present in the provided Oracle SQL snippet. This filter will be applied in both legacy (simulated) and migrated tests for output parity.
*   The date-based filtering (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`) mentioned in the legacy ksh description is **not applied** in the core data insertion logic of either the legacy Oracle SQL (as per the provided snippet) or the migrated BigQuery Stored Procedure. `p_stichtag` primarily influences `v_stichtag` for potential future use or logging, but not the `INSERT...SELECT` filter.
*   The `dwtk_meldungen` table contains an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` which is used to derive `v_datum` in the BigQuery SP. This `v_datum` is currently not used in the `INSERT` statement.
*   The `DWH_VERTRAG_ID` column in `sof_ta_bpr_instance` is `NOT NULL` as per the `create_sof_ta_bpr_instance_table.sql` schema.

---

## Python Test Framework (Pytest)

The following Python code provides a `pytest` framework for executing the migrated BigQuery Stored Procedure and asserting its behavior. This framework includes helper functions for BigQuery interaction and fixtures for setup/teardown.

```python
import pytest
from google.cloud import bigquery
import os
import json
import subprocess
import time
from datetime import datetime

# --- Configuration ---
# IMPORTANT: Replace these placeholders with your actual GCP project ID and BigQuery dataset ID
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your_gcp_project")
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_bq_dataset")
BQ_CLIENT = bigquery.Client(project=GCP_PROJECT_ID)

# Table references
DWTK_MELDUNGEN_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.dwtk_meldungen"
SOF_TA_BPR_OPTIONEN_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_bpr_optionen"
SOF_TA_BPR_INSTANCE_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_bpr_instance"
JOB_AUDIT_LOG_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log"
STORED_PROCEDURE_ID = f"{BQ_DATASET_ID}.r_ausd_bp_ta_bpr_optionen"

# --- Helper Functions for Test Setup/Teardown ---

def _clear_bq_table(table_id):
    """Truncates a BigQuery table."""
    query = f"TRUNCATE TABLE `{table_id}`"
    BQ_CLIENT.query(query).result()

def _insert_dwtk_meldungen_data(job_kennung, value):
    """Inserts data into the dwtk_meldungen table."""
    _clear_bq_table(DWTK_MELDUNGEN_TABLE)
    query = f"""
    INSERT INTO `{DWTK_MELDUNGEN_TABLE}` (job_kennung, value)
    VALUES ('{job_kennung}', '{value}')
    """
    BQ_CLIENT.query(query).result()

def _insert_sof_ta_bpr_instance_data(data):
    """Inserts data into the sof_ta_bpr_instance table."""
    _clear_bq_table(SOF_TA_BPR_INSTANCE_TABLE)
    rows_to_insert = [
        bigquery.Row(row) for row in data
    ]
    errors = BQ_CLIENT.insert_rows_json(SOF_TA_BPR_INSTANCE_TABLE, rows_to_insert)
    if errors:
        raise Exception(f"Errors inserting data into {SOF_TA_BPR_INSTANCE_TABLE}: {errors}")

def _run_migrated_job(stichtag=None, wiederanlaufWert=0):
    """
    Executes the BigQuery Stored Procedure, simulating the Airflow DAG call.
    """
    # Construct parameters for the BQ SP call
    # The SP expects p_stichtag STRING, p_wiederanlaufWert INT64
    sp_params_str = []
    if stichtag is not None:
        sp_params_str.append(f"'{stichtag}'")
    else:
        sp_params_str.append("NULL") # Pass NULL if stichtag is not provided
    
    sp_params_str.append(str(wiederanlaufWert))

    query = f"CALL `{STORED_PROCEDURE_ID}`({', '.join(sp_params_str)})"
    print(f"Executing BigQuery Stored Procedure: {query}")
    job = BQ_CLIENT.query(query)
    job.result() # Wait for the job to complete
    print(f"BigQuery Stored Procedure completed. Job ID: {job.job_id}")

def _get_bq_table_data(table_id):
    """Fetches all data from a BigQuery table, ordered for consistent comparison."""
    query = f"SELECT * FROM `{table_id}` ORDER BY CNTRCT_ID, BPR_ID"
    rows = BQ_CLIENT.query(query).result()
    return [dict(row) for row in rows]

def _get_bq_row_count(table_id):
    """Returns the row count of a BigQuery table."""
    query = f"SELECT COUNT(1) FROM `{table_id}`"
    row = BQ_CLIENT.query(query).result().next()
    return row[0]

def _get_latest_audit_log_entry(job_name='r_ausd_bp_ta_bpr_optionen'):
    """Fetches the latest audit log entry for a given job name."""
    query = f"""
    SELECT * FROM `{JOB_AUDIT_LOG_TABLE}`
    WHERE job_name = '{job_name}'
    ORDER BY start_time DESC
    LIMIT 1
    """
    rows = BQ_CLIENT.query(query).result()
    return [dict(row) for row in rows]

# --- Fixtures ---
@pytest.fixture(scope="module", autouse=True)
def setup_bq_tables_module():
    """Ensures BigQuery tables exist before running tests.
    In a real setup, these would be part of your CI/CD or deployment.
    For testing, we assume they are pre-created by running the provided SQL files.
    """
    pass

@pytest.fixture(autouse=True)
def cleanup_and_setup_for_test():
    """Clears and sets up tables before each test."""
    _clear_bq_table(SOF_TA_BPR_OPTIONEN_TABLE)
    _clear_bq_table(JOB_AUDIT_LOG_TABLE)
    _clear_bq_table(DWTK_MELDUNGEN_TABLE)
    _clear_bq_table(SOF_TA_BPR_INSTANCE_TABLE)
    
    # Default dwtk_meldungen entry for date derivation
    _insert_dwtk_meldungen_data('BERT_DROP_TEMP_TABLE', '01012023') # Example date
    
    yield # Run the test
    
    # Cleanup after test
    _clear_bq_table(SOF_TA_BPR_OPTIONEN_TABLE)
    _clear_bq_table(JOB_AUDIT_LOG_TABLE)
    _clear_bq_table(DWTK_MELDUNGEN_TABLE)
    _clear_bq_table(SOF_TA_BPR_INSTANCE_TABLE)

# --- Legacy System Mock/Interface (Conceptual) ---
def _get_expected_legacy_output(source_data, wiederanlaufWert=0):
    """
    Simulates the expected output of the legacy ksh script based on the design.
    This function needs to be replaced by actual execution and data extraction
    from the Oracle legacy system for true output parity testing.
    
    For this example, we assume the legacy system applies the same DWH_VERTRAG_ID filter
    as described in the migration design document for the `-l` parameter.
    """
    expected_output = []
    for row in source_data:
        if row['DWH_VERTRAG_ID'] > wiederanlaufWert:
            expected_output.append({'CNTRCT_ID': row['CNTRCT_ID'], 'BPR_ID': row['BPR_ID']})
    
    # Sort for consistent comparison
    expected_output.sort(key=lambda x: (x['CNTRCT_ID'], x['BPR_ID']))
    return expected_output
```

---

## Test Case 1: Default Execution (No Parameters)

**Purpose:** Verify the basic functionality of the migrated job when no `stichtag` or `wiederanlaufWert` parameters are provided. This tests the default behavior, including date determination (defaults to system date) and no restart filtering (i.e., `DWH_VERTRAG_ID > 0`).

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear and `dwtk_meldungen` is populated.
2.  Populate `sof_ta_bpr_instance` with diverse test data:
    ```sql
    INSERT INTO `your_gcp_project.your_bq_dataset.sof_ta_bpr_instance` (CNTRCT_ID, BPR_ID, DWH_VERTRAG_ID) VALUES
    ('C001', 'BP001', 10),
    ('C002', 'BP002', 20),
    ('C003', 'BP003', 0), -- DWH_VERTRAG_ID = 0
    ('C004', 'BP004', 5),
    ('C005', 'BP005', 15);
    ```

**Action:**
1.  **Legacy:** Execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script without any `-s` or `-l` parameters.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` with `stichtag=None` and `wiederanlaufWert=0` (default values).

**Pass/Fail Criterion:**
*   **Output Parity:** The data in `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` must be identical to the data in the legacy Oracle `sof$ta_bpr_optionen` table. Specifically, all rows from `sof_ta_bpr_instance` where `DWH_VERTRAG_ID > 0` should be present.
    *   Expected rows: `C001, BP001`, `C002, BP002`, `C004, BP004`, `C005, BP005`. (`C003, BP003` is excluded because 0 is not > 0).
*   **Row Count:** The number of rows in `sof_ta_bpr_optionen` must be 4.
*   **Logging:** A `SUCCESS` entry must be present in `job_audit_log` for `r_ausd_bp_ta_bpr_optionen`, indicating `inserted_rows = 4`. The `parameters` field should reflect `{'p_stichtag': None, 'p_wiederanlaufWert': 0}`.

```python
def test_default_execution(cleanup_and_setup_for_test):
    source_data = [
        {'CNTRCT_ID': 'C001', 'BPR_ID': 'BP001', 'DWH_VERTRAG_ID': 10},
        {'CNTRCT_ID': 'C002', 'BPR_ID': 'BP002', 'DWH_VERTRAG_ID': 20},
        {'CNTRCT_ID': 'C003', 'BPR_ID': 'BP003', 'DWH_VERTRAG_ID': 0},
        {'CNTRCT_ID': 'C004', 'BPR_ID': 'BP004', 'DWH_VERTRAG_ID': 5},
        {'CNTRCT_ID': 'C005', 'BPR_ID': 'BP005', 'DWH_VERTRAG_ID': 15},
    ]
    _insert_sof_ta_bpr_instance_data(source_data)

    expected_output_legacy = _get_expected_legacy_output(source_data, wiederanlaufWert=0)

    _run_migrated_job(stichtag=None, wiederanlaufWert=0)
    migrated_output = _get_bq_table_data(SOF_TA_BPR_OPTIONEN_TABLE)
    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    audit_log = _get_latest_audit_log_entry()

    assert migrated_output == expected_output_legacy
    assert migrated_row_count == 4
    assert audit_log[0]['status'] == 'SUCCESS'
    assert audit_log[0]['inserted_rows'] == 4
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': None, 'p_wiederanlaufWert': 0})
```

---

## Test Case 2: Specific Stichtag, Default Wiederanlaufwert

**Purpose:** Verify that providing a `p_stichtag` parameter is correctly handled by the stored procedure, even if it doesn't directly filter data in the `INSERT` statement (as per current design). It validates parameter passing and internal date variable setting.

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear and `dwtk_meldungen` is populated.
2.  Populate `sof_ta_bpr_instance` with the same data as Test Case 1.

**Action:**
1.  **Legacy:** Execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script with a specific `stichtag`.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh -s 15032023
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` with `stichtag='15032023'` and `wiederanlaufWert=0`.

**Pass/Fail Criterion:**
*   **Output Parity:** The data in `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` must be identical to the data in the legacy Oracle `sof$ta_bpr_optionen` table. Since `p_stichtag` doesn't filter, the output should be the same as Test Case 1.
*   **Row Count:** The number of rows in `sof_ta_bpr_optionen` must be 4.
*   **Logging:** A `SUCCESS` entry must be present in `job_audit_log`, with `parameters` reflecting `{'p_stichtag': '15032023', 'p_wiederanlaufWert': 0}`.

```python
def test_specific_stichtag_default_wiederanlaufwert(cleanup_and_setup_for_test):
    source_data = [
        {'CNTRCT_ID': 'C001', 'BPR_ID': 'BP001', 'DWH_VERTRAG_ID': 10},
        {'CNTRCT_ID': 'C002', 'BPR_ID': 'BP002', 'DWH_VERTRAG_ID': 20},
        {'CNTRCT_ID': 'C003', 'BPR_ID': 'BP003', 'DWH_VERTRAG_ID': 0},
        {'CNTRCT_ID': 'C004', 'BPR_ID': 'BP004', 'DWH_VERTRAG_ID': 5},
        {'CNTRCT_ID': 'C005', 'BPR_ID': 'BP005', 'DWH_VERTRAG_ID': 15},
    ]
    _insert_sof_ta_bpr_instance_data(source_data)

    expected_output_legacy = _get_expected_legacy_output(source_data, wiederanlaufWert=0)

    _run_migrated_job(stichtag='15032023', wiederanlaufWert=0)
    migrated_output = _get_bq_table_data(SOF_TA_BPR_OPTIONEN_TABLE)
    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    audit_log = _get_latest_audit_log_entry()

    assert migrated_output == expected_output_legacy
    assert migrated_row_count == 4
    assert audit_log[0]['status'] == 'SUCCESS'
    assert audit_log[0]['inserted_rows'] == 4
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': '15032023', 'p_wiederanlaufWert': 0})
```

---

## Test Case 3: Specific Wiederanlaufwert, Default Stichtag

**Purpose:** Verify the `p_wiederanlaufWert` filtering logic is correctly applied, resulting in a subset of the data.

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear and `dwtk_meldungen` is populated.
2.  Populate `sof_ta_bpr_instance` with the same data as Test Case 1.

**Action:**
1.  **Legacy:** Execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script with a specific `wiederanlaufWert`.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh -l 10
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` with `stichtag=None` and `wiederanlaufWert=10`.

**Pass/Fail Criterion:**
*   **Output Parity:** The data in `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` must be identical to the data in the legacy Oracle `sof$ta_bpr_optionen` table. Only rows where `DWH_VERTRAG_ID > 10` should be present.
    *   Expected rows: `C002, BP002`, `C005, BP005`.
*   **Row Count:** The number of rows in `sof_ta_bpr_optionen` must be 2.
*   **Logging:** A `SUCCESS` entry must be present in `job_audit_log`, indicating `inserted_rows = 2`.

```python
def test_specific_wiederanlaufwert_default_stichtag(cleanup_and_setup_for_test):
    source_data = [
        {'CNTRCT_ID': 'C001', 'BPR_ID': 'BP001', 'DWH_VERTRAG_ID': 10},
        {'CNTRCT_ID': 'C002', 'BPR_ID': 'BP002', 'DWH_VERTRAG_ID': 20},
        {'CNTRCT_ID': 'C003', 'BPR_ID': 'BP003', 'DWH_VERTRAG_ID': 0},
        {'CNTRCT_ID': 'C004', 'BPR_ID': 'BP004', 'DWH_VERTRAG_ID': 5},
        {'CNTRCT_ID': 'C005', 'BPR_ID': 'BP005', 'DWH_VERTRAG_ID': 15},
    ]
    _insert_sof_ta_bpr_instance_data(source_data)

    expected_output_legacy = _get_expected_legacy_output(source_data, wiederanlaufWert=10)

    _run_migrated_job(stichtag=None, wiederanlaufWert=10)
    migrated_output = _get_bq_table_data(SOF_TA_BPR_OPTIONEN_TABLE)
    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    audit_log = _get_latest_audit_log_entry()

    assert migrated_output == expected_output_legacy
    assert migrated_row_count == 2
    assert audit_log[0]['status'] == 'SUCCESS'
    assert audit_log[0]['inserted_rows'] == 2
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': None, 'p_wiederanlaufWert': 10})
```

---

## Test Case 4: Specific Stichtag and Wiederanlaufwert

**Purpose:** Verify the combined handling of both `p_stichtag` and `p_wiederanlaufWert` parameters.

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear and `dwtk_meldungen` is populated.
2.  Populate `sof_ta_bpr_instance` with the same data as Test Case 1.

**Action:**
1.  **Legacy:** Execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script with both parameters.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh -s 01012024 -l 15
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` with `stichtag='01012024'` and `wiederanlaufWert=15`.

**Pass/Fail Criterion:**
*   **Output Parity:** The data in `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` must be identical to the data in the legacy Oracle `sof$ta_bpr_optionen` table. Only rows where `DWH_VERTRAG_ID > 15` should be present.
    *   Expected rows: `C002, BP002`.
*   **Row Count:** The number of rows in `sof_ta_bpr_optionen` must be 1.
*   **Logging:** A `SUCCESS` entry must be present in `job_audit_log`, indicating `inserted_rows = 1`.

```python
def test_specific_stichtag_and_wiederanlaufwert(cleanup_and_setup_for_test):
    source_data = [
        {'CNTRCT_ID': 'C001', 'BPR_ID': 'BP001', 'DWH_VERTRAG_ID': 10},
        {'CNTRCT_ID': 'C002', 'BPR_ID': 'BP002', 'DWH_VERTRAG_ID': 20},
        {'CNTRCT_ID': 'C003', 'BPR_ID': 'BP003', 'DWH_VERTRAG_ID': 0},
        {'CNTRCT_ID': 'C004', 'BPR_ID': 'BP004', 'DWH_VERTRAG_ID': 5},
        {'CNTRCT_ID': 'C005', 'BPR_ID': 'BP005', 'DWH_VERTRAG_ID': 15},
    ]
    _insert_sof_ta_bpr_instance_data(source_data)

    expected_output_legacy = _get_expected_legacy_output(source_data, wiederanlaufWert=15)

    _run_migrated_job(stichtag='01012024', wiederanlaufWert=15)
    migrated_output = _get_bq_table_data(SOF_TA_BPR_OPTIONEN_TABLE)
    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    audit_log = _get_latest_audit_log_entry()

    assert migrated_output == expected_output_legacy
    assert migrated_row_count == 1
    assert audit_log[0]['status'] == 'SUCCESS'
    assert audit_log[0]['inserted_rows'] == 1
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': '01012024', 'p_wiederanlaufWert': 15})
```

---

## Test Case 5: Edge Case - High Wiederanlaufwert (No Rows Inserted)

**Purpose:** Verify that the `p_wiederanlaufWert` filter correctly handles scenarios where no rows meet the criteria, resulting in an empty target table.

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear and `dwtk_meldungen` is populated.
2.  Populate `sof_ta_bpr_instance` with some data (e.g., `DWH_VERTRAG_ID` values 10 and 20).

**Action:**
1.  **Legacy:** Execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script with a very high `wiederanlaufWert`.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh -l 999999999
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` with `wiederanlaufWert=999999999`.

**Pass/Fail Criterion:**
*   **Output Parity:** The `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` table must be empty.
*   **Row Count:** The number of rows in `sof_ta_bpr_optionen` must be 0.
*   **Logging:** A `SUCCESS` entry must be present in `job_audit_log`, indicating `inserted_rows = 0`.

```python
def test_high_wiederanlaufwert_no_rows(cleanup_and_setup_for_test):
    source_data = [
        {'CNTRCT_ID': 'C001', 'BPR_ID': 'BP001', 'DWH_VERTRAG_ID': 10},
        {'CNTRCT_ID': 'C002', 'BPR_ID': 'BP002', 'DWH_VERTRAG_ID': 20},
    ]
    _insert_sof_ta_bpr_instance_data(source_data)

    expected_output_legacy = _get_expected_legacy_output(source_data, wiederanlaufWert=999999999)

    _run_migrated_job(stichtag=None, wiederanlaufWert=999999999)
    migrated_output = _get_bq_table_data(SOF_TA_BPR_OPTIONEN_TABLE)
    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    audit_log = _get_latest_audit_log_entry()

    assert migrated_output == expected_output_legacy # Should be []
    assert migrated_row_count == 0
    assert audit_log[0]['status'] == 'SUCCESS'
    assert audit_log[0]['inserted_rows'] == 0
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': None, 'p_wiederanlaufWert': 999999999})
```

---

## Test Case 6: Invalid Stichtag Format

**Purpose:** Verify the error handling and default `stichtag` behavior when an invalid date format is provided for `p_stichtag`. The design states it should default to `v_sysdate`.

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear and `dwtk_meldungen` is populated.
2.  Populate `sof_ta_bpr_instance` with the same data as Test Case 1.

**Action:**
1.  **Legacy:** Execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script with an invalid `stichtag`. The ksh script's `pruefeParameterGesetzt Stichtag p_stichtag` might catch this, or date parsing utilities might fail. The design implies it defaults to `sysdate`.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh -s INVALIDDATE
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` with `stichtag='INVALIDDATE'` and `wiederanlaufWert=0`.

**Pass/Fail Criterion:**
*   **Output Parity:** The data in `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` must be identical to the output of Test Case 1 (default execution), as `v_stichtag` should default to `v_sysdate`.
*   **Row Count:** The number of rows in `sof_ta_bpr_optionen` must be 4.
*   **Logging:** A `SUCCESS` entry must be present in `job_audit_log`. The `parameters` field should reflect `{'p_stichtag': 'INVALIDDATE', 'p_wiederanlaufWert': 0}` as it was passed, but the internal logic should have handled the invalid date gracefully by defaulting.

```python
def test_invalid_stichtag_format(cleanup_and_setup_for_test):
    source_data = [
        {'CNTRCT_ID': 'C001', 'BPR_ID': 'BP001', 'DWH_VERTRAG_ID': 10},
        {'CNTRCT_ID': 'C002', 'BPR_ID': 'BP002', 'DWH_VERTRAG_ID': 20},
        {'CNTRCT_ID': 'C003', 'BPR_ID': 'BP003', 'DWH_VERTRAG_ID': 0},
        {'CNTRCT_ID': 'C004', 'BPR_ID': 'BP004', 'DWH_VERTRAG_ID': 5},
        {'CNTRCT_ID': 'C005', 'BPR_ID': 'BP005', 'DWH_VERTRAG_ID': 15},
    ]
    _insert_sof_ta_bpr_instance_data(source_data)

    expected_output_legacy = _get_expected_legacy_output(source_data, wiederanlaufWert=0)

    _run_migrated_job(stichtag='INVALIDDATE', wiederanlaufWert=0)
    migrated_output = _get_bq_table_data(SOF_TA_BPR_OPTIONEN_TABLE)
    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    audit_log = _get_latest_audit_log_entry()

    assert migrated_output == expected_output_legacy
    assert migrated_row_count == 4
    assert audit_log[0]['status'] == 'SUCCESS'
    assert audit_log[0]['inserted_rows'] == 4
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': 'INVALIDDATE', 'p_wiederanlaufWert': 0})
```

---

## Test Case 7: Source Table Empty

**Purpose:** Verify the job's behavior when the source table (`sof_ta_bpr_instance`) contains no data.

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear and `dwtk_meldungen` is populated.
2.  Ensure `sof_ta_bpr_instance` is empty.

**Action:**
1.  **Legacy:** Execute the original `r_ausd_bp_ta_bpr_optionen.ksh` script (e.g., with default parameters).
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` (e.g., with default parameters).

**Pass/Fail Criterion:**
*   **Output Parity:** The `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` table must be empty.
*   **Row Count:** The number of rows in `sof_ta_bpr_optionen` must be 0.
*   **Logging:** A `SUCCESS` entry must be present in `job_audit_log`, indicating `inserted_rows = 0`.

```python
def test_source_table_empty(cleanup_and_setup_for_test):
    # sof_ta_bpr_instance is already empty from cleanup_and_setup_for_test
    
    expected_output_legacy = _get_expected_legacy_output([], wiederanlaufWert=0)

    _run_migrated_job(stichtag=None, wiederanlaufWert=0)
    migrated_output = _get_bq_table_data(SOF_TA_BPR_OPTIONEN_TABLE)
    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    audit_log = _get_latest_audit_log_entry()

    assert migrated_output == expected_output_legacy # Should be []
    assert migrated_row_count == 0
    assert audit_log[0]['status'] == 'SUCCESS'
    assert audit_log[0]['inserted_rows'] == 0
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': None, 'p_wiederanlaufWert': 0})
```

---

## Test Case 8: Error Handling - `dwtk_meldungen` Missing Entry

**Purpose:** Verify that the migrated job correctly handles errors during the `v_datum` derivation step (e.g., if the required entry is missing from `dwtk_meldungen`) and logs the failure.

**Setup:**
1.  `cleanup_and_setup_for_test` fixture ensures tables are clear.
2.  **Crucially, clear `dwtk_meldungen`** so it does NOT contain an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
3.  Populate `sof_ta_bpr_instance` with some data.

**Action:**
1.  **Legacy:** Simulate the failure. The ksh script would likely fail if the underlying SQL for `v_datum` derivation failed.
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_optionen` (e.g., with default parameters). This should cause the BigQuery Stored Procedure to raise an error.

**Pass/Fail Criterion:**
*   **Error Propagation:** The Airflow task (or direct SP call) must fail and propagate an error. The `pytest.raises(Exception)` should catch it.
*   **Target Table State:** The `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` table must remain empty (as the `TRUNCATE` happens before the `INSERT` but after `v_datum` derivation, so it would have been truncated).
*   **Logging:** A `FAILED` entry must be present in `job_audit_log` for `r_ausd_bp_ta_bpr_optionen`, with a descriptive `message` about the error (e.g., "Scalar subquery produced no rows" or similar, depending on BigQuery's exact error for `SELECT INTO` with no results).

```python
def test_error_dwtk_meldungen_missing_entry(cleanup_and_setup_for_test):
    _clear_bq_table(DWTK_MELDUNGEN_TABLE) # Ensure no entry for BERT_DROP_TEMP_TABLE
    _insert_sof_ta_bpr_instance_data([
        {'CNTRCT_ID': 'C001', 'BPR_ID': 'BP001', 'DWH_VERTRAG_ID': 10},
    ])

    with pytest.raises(Exception) as excinfo:
        _run_migrated_job(stichtag=None, wiederanlaufWert=0)
    
    # Check error message (BigQuery specific)
    assert "Scalar subquery produced no rows" in str(excinfo.value) or \
           "No rows found for assignment" in str(excinfo.value) # Depending on BQ version/exact error

    migrated_row_count = _get_bq_row_count(SOF_TA_BPR_OPTIONEN_TABLE)
    assert migrated_row_count == 0

    audit_log = _get_latest_audit_log_entry()
    assert audit_log[0]['status'] == 'FAILED'
    assert "Job failed with error" in audit_log[0]['message']
    assert audit_log[0]['parameters'] == json.dumps({'p_stichtag': None, 'p_wiederanlaufWert': 0})
```

---

## Test Case 9: Schema and Data Type Validation

**Purpose:** Assert that the BigQuery table schemas (column names, data types, and nullability) match the expected design and are consistent with the legacy system's implicit schema.

**Setup:**
*   Ensure all BigQuery tables (`dwtk_meldungen`, `sof_ta_bpr_optionen`, `sof_ta_bpr_instance`, `job_audit_log`) are created as per the `create_*.sql` scripts.

**Action:**
1.  Retrieve the schema definitions for each BigQuery table using BigQuery's API or `INFORMATION_SCHEMA`.

**Pass/Fail Criterion:**
*   **`sof_ta_bpr_optionen` Schema:**
    *   `CNTRCT_ID`: STRING, REQUIRED
    *   `BPR_ID`: STRING, REQUIRED
*   **`sof_ta_bpr_instance` Schema:**
    *   `CNTRCT_ID`: STRING, REQUIRED
    *   `BPR_ID`: STRING, REQUIRED
    *   `DWH_VERTRAG_ID`: INT64, REQUIRED
*   **`dwtk_meldungen` Schema:**
    *   `job_kennung`: STRING, REQUIRED
    *   `value`: STRING, REQUIRED
*   **`job_audit_log` Schema:**
    *   `job_name`: STRING, REQUIRED
    *   `run_id`: STRING, REQUIRED
    *   `start_time`: TIMESTAMP, NULLABLE
    *   `end_time`: TIMESTAMP, NULLABLE
    *   `status`: STRING, REQUIRED
    *   `message`: STRING, NULLABLE
    *   `parameters`: JSON, NULLABLE
    *   `inserted_rows`: INT64, NULLABLE
    *   `updated_at`: TIMESTAMP, NULLABLE

```python
def test_schema_validation():
    def get_table_schema(table_id):
        table = BQ_CLIENT.get_table(table_id)
        return {field.name: {'field_type': field.field_type, 'mode': field.mode} for field in table.schema}

    expected_sof_ta_bpr_optionen_schema = {
        'CNTRCT_ID': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'BPR_ID': {'field_type': 'STRING', 'mode': 'REQUIRED'},
    }
    expected_sof_ta_bpr_instance_schema = {
        'CNTRCT_ID': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'BPR_ID': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'DWH_VERTRAG_ID': {'field_type': 'INT64', 'mode': 'REQUIRED'},
    }
    expected_dwtk_meldungen_schema = {
        'job_kennung': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'value': {'field_type': 'STRING', 'mode': 'REQUIRED'},
    }
    expected_job_audit_log_schema = {
        'job_name': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'run_id': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'start_time': {'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        'end_time': {'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        'status': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'message': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'parameters': {'field_type': 'JSON', 'mode': 'NULLABLE'},
        'inserted_rows': {'field_type': 'INT64', 'mode': 'NULLABLE'},
        'updated_at': {'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    }

    assert get_table_schema(SOF_TA_BPR_OPTIONEN_TABLE) == expected_sof_ta_bpr_optionen_schema
    assert get_table_schema(SOF_TA_BPR_INSTANCE_TABLE) == expected_sof_ta_bpr_instance_schema
    assert get_table_schema(DWTK_MELDUNGEN_TABLE) == expected_dwtk_meldungen_schema
    assert get_table_schema(JOB_AUDIT_LOG_TABLE) == expected_job_audit_log_schema
```