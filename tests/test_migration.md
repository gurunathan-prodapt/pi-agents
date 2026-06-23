As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_bp_ta_bpr_beschr.ksh` to a BigQuery Stored Procedure. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

The tests are structured using a `pytest` framework, with helper functions to interact with BigQuery and simulate the legacy KornShell script's behavior.

---

### Test Environment Setup

Before running the tests, ensure the following BigQuery tables and stored procedures are deployed:

**DDL for Tables:**
*   `project.dataset.DWH_TA_C_VERTRAG`
*   `project.dataset.FOS_TABLE`
*   `project.dataset.job_log`
*   `project.dataset.job_audit`

**Stored Procedures:**
*   `project.dataset.process_contract_cache_data`
*   `project.dataset.ausd_bp_ta_bpr_beschr`

**Python Test Setup (using `pytest` and `google-cloud-bigquery` library):**

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta
import os

# --- Configuration ---
# Replace with your actual GCP project and dataset IDs
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")

DWH_SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.DWH_TA_C_VERTRAG"
FOS_TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.FOS_TABLE"
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
JOB_AUDIT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_audit"
ORCHESTRATION_SP = f"{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_beschr"
CORE_PROCESSING_SP = f"{PROJECT_ID}.{DATASET_ID}.process_contract_cache_data" # Used for potential mocking

# Initialize BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions ---
def setup_bigquery_tables():
    """Ensures tables exist and are empty for a clean test run."""
    print(f"Clearing tables: {DWH_SOURCE_TABLE}, {FOS_TARGET_TABLE}, {JOB_LOG_TABLE}, {JOB_AUDIT_TABLE}")
    bq_client.query(f"TRUNCATE TABLE `{DWH_SOURCE_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{FOS_TARGET_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{JOB_AUDIT_TABLE}`").result()

def insert_dwh_source_data(data):
    """Inserts test data into DWH_TA_C_VERTRAG."""
    rows_to_insert = []
    for row in data:
        rows_to_insert.append(
            (row['DWH_VERTRAG_ID'], row['Gueltig_von'], row['Gueltig_bis'], row['Ladedatum'], row['column1'], row['column2'])
        )
    errors = bq_client.insert_rows(bq_client.get_table(DWH_SOURCE_TABLE), rows_to_insert)
    if errors:
        raise Exception(f"Error inserting DWH source data: {errors}")
    print(f"Inserted {len(data)} rows into {DWH_SOURCE_TABLE}")

def get_fos_table_data():
    """Retrieves all data from FOS_TABLE."""
    query = f"SELECT DWH_VERTRAG_ID, column1, column2 FROM `{FOS_TARGET_TABLE}` ORDER BY DWH_VERTRAG_ID"
    rows = bq_client.query(query).result()
    result = [dict(row) for row in rows]
    print(f"Retrieved {len(result)} rows from {FOS_TARGET_TABLE}")
    return result

def get_job_audit_entries(job_kennung, stichtag, restart_value):
    """Retrieves audit entries for a specific job run."""
    query = f"""
    SELECT status, message, created_at, finished_at
    FROM `{JOB_AUDIT_TABLE}`
    WHERE job_kennung = '{job_kennung}'
      AND stichtag = '{stichtag}'
      AND restart_value = {restart_value}
    ORDER BY created_at
    """
    rows = bq_client.query(query).result()
    result = [dict(row) for row in rows]
    print(f"Retrieved {len(result)} audit entries for job_kennung={job_kennung}, stichtag={stichtag}, restart_value={restart_value}")
    return result

def get_job_log_entries(job_kennung):
    """Retrieves job_log entries for a specific job_kennung."""
    query = f"""
    SELECT status, message, created_at
    FROM `{JOB_LOG_TABLE}`
    WHERE job_kennung = '{job_kennung}'
    ORDER BY created_at
    """
    rows = bq_client.query(query).result()
    result = [dict(row) for row in rows]
    print(f"Retrieved {len(result)} job_log entries for job_kennung={job_kennung}")
    return result

def run_legacy_script(stichtag=None, wiederanlaufWert=None):
    """
    Mocks running the legacy KornShell script and determining its expected output.
    In a real migration test, this function would execute the actual .ksh script
    (e.g., via SSH or a wrapper) and then query the legacy FOS_TABLE to get the
    'ground truth' data.

    For this exercise, it simulates the expected behavior based on the design
    document and the provided DWH data structure.
    """
    print(f"Simulating legacy script run with stichtag={stichtag}, wiederanlaufWert={wiederanlaufWert}")

    # This DWH data is a fixed representation for simulation.
    # In a real test, this would be dynamically populated or read from a fixture.
    dwh_data_fixture = [
        {'DWH_VERTRAG_ID': 100, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-12-31', 'Ladedatum': '2023-01-05', 'column1': 'A', 'column2': 'X'},
        {'DWH_VERTRAG_ID': 101, 'Gueltig_von': '2023-01-15', 'Gueltig_bis': '2023-06-30', 'Ladedatum': '2023-01-20', 'column1': 'B', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 102, 'Gueltig_von': '2023-02-01', 'Gueltig_bis': '2023-02-10', 'Ladedatum': '2023-02-05', 'column1': 'C', 'column2': 'Z'},
        {'DWH_VERTRAG_ID': 103, 'Gueltig_von': '2023-03-01', 'Gueltig_bis': '2023-03-05', 'Ladedatum': '2023-03-02', 'column1': 'D', 'column2': 'W'},
        {'DWH_VERTRAG_ID': 104, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-02-20', 'Ladedatum': '2023-02-16', 'column1': 'E', 'column2': 'V'},
        {'DWH_VERTRAG_ID': 105, 'Gueltig_von': '2023-02-15', 'Gueltig_bis': '2023-03-15', 'Ladedatum': '2023-02-10', 'column1': 'F', 'column2': 'U'},
        {'DWH_VERTRAG_ID': 106, 'Gueltig_von': '2023-02-15', 'Gueltig_bis': '2023-03-15', 'Ladedatum': '2023-02-10', 'column1': 'G', 'column2': 'T'},
        {'DWH_VERTRAG_ID': 200, 'Gueltig_von': '2024-01-01', 'Gueltig_bis': '2024-01-25', 'Ladedatum': '2024-01-10', 'column1': 'X', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 201, 'Gueltig_von': '2024-01-15', 'Gueltig_bis': '2024-01-19', 'Ladedatum': '2024-01-18', 'column1': 'A', 'column2': 'B'},
        {'DWH_VERTRAG_ID': 202, 'Gueltig_von': '2024-01-15', 'Gueltig_bis': '2024-02-15', 'Ladedatum': '2024-01-20', 'column1': 'C', 'column2': 'D'},
        {'DWH_VERTRAG_ID': 203, 'Gueltig_von': '2024-01-15', 'Gueltig_bis': '2024-02-15', 'Ladedatum': '2024-01-19', 'column1': 'E', 'column2': 'F'},
        {'DWH_VERTRAG_ID': 300, 'Gueltig_von': '2024-01-10', 'Gueltig_bis': '2024-01-11', 'Ladedatum': '2024-01-09', 'column1': 'A', 'column2': 'A'},
        {'DWH_VERTRAG_ID': 301, 'Gueltig_von': '2024-01-09', 'Gueltig_bis': '2024-01-10', 'Ladedatum': '2024-01-08', 'column1': 'B', 'column2': 'B'},
        {'DWH_VERTRAG_ID': 302, 'Gueltig_von': '2024-01-11', 'Gueltig_bis': '2024-01-12', 'Ladedatum': '2024-01-09', 'column1': 'C', 'column2': 'C'},
        {'DWH_VERTRAG_ID': 303, 'Gueltig_von': '2024-01-09', 'Gueltig_bis': '2024-01-11', 'Ladedatum': '2024-01-10', 'column1': 'D', 'column2': 'D'},
        {'DWH_VERTRAG_ID': 1, 'Gueltig_von': '2022-12-01', 'Gueltig_bis': '2023-01-02', 'Ladedatum': '2022-12-15', 'column1': 'ShortString', 'column2': 'LongerStringValue'},
        {'DWH_VERTRAG_ID': 2, 'Gueltig_von': '2022-12-01', 'Gueltig_bis': '2023-01-02', 'Ladedatum': '2022-12-15', 'column1': 'Another', 'column2': 'Value'},
    ]

    effective_stichtag_str = stichtag if stichtag else datetime.now().strftime('%d%m%Y')
    effective_stichtag_date = datetime.strptime(effective_stichtag_str, '%d%m%Y').date()
    effective_wiederanlaufWert = wiederanlaufWert if wiederanlaufWert is not None else 0

    # Simulate DELETE for restart (this would be based on initial FOS_TABLE state in legacy)
    # For this simulation, we'll assume the FOS_TABLE was empty or only contained data
    # that would be overwritten by the current run's logic.
    # The actual legacy FOS_TABLE would be queried after the run.
    
    expected_fos_data = []
    for row in dwh_data_fixture:
        # Only consider data relevant to the current test's DWH_TA_C_VERTRAG setup
        # This is a simplification; a real test would need to match the exact DWH data used in setup.
        # For now, we'll filter based on the DWH_VERTRAG_ID ranges used in the test cases.
        
        gueltig_von = datetime.strptime(row['Gueltig_von'], '%Y-%m-%d').date()
        gueltig_bis = datetime.strptime(row['Gueltig_bis'], '%Y-%m-%d').date()
        ladedatum = datetime.strptime(row['Ladedatum'], '%Y-%m-%d').date()

        if (gueltig_von <= effective_stichtag_date and
            effective_stichtag_date < gueltig_bis and
            ladedatum < effective_stichtag_date and
            row['DWH_VERTRAG_ID'] > effective_wiederanlaufWert):
            expected_fos_data.append({
                'DWH_VERTRAG_ID': row['DWH_VERTRAG_ID'],
                'column1': row['column1'],
                'column2': row['column2']
            })
    
    # Sort for consistent comparison
    expected_fos_data.sort(key=lambda x: x['DWH_VERTRAG_ID'])
    return expected_fos_data

def call_bigquery_sp(stichtag_in=None, wiederanlaufWert_in=None):
    """Calls the BigQuery orchestration stored procedure."""
    stichtag_param = f"'{stichtag_in}'" if stichtag_in is not None else "NULL"
    wiederanlauf_param = str(wiederanlaufWert_in) if wiederanlaufWert_in is not None else "NULL"
    
    query = f"CALL `{ORCHESTRATION_SP}`({stichtag_param}, {wiederanlauf_param})"
    print(f"Executing BigQuery SP: {query}")
    try:
        bq_client.query(query).result()
        return True # Success
    except Exception as e:
        print(f"BigQuery SP call failed: {e}")
        return False # Failure

# --- Pytest Fixture for common setup ---
@pytest.fixture(autouse=True)
def run_around_tests():
    """Fixture to ensure a clean state before each test."""
    setup_bigquery_tables()
    yield # Run the test
    # Teardown (optional, tables are truncated at start of next test)
```

---

### Test Cases

#### Test Case 1: Full Load (No Restart Value, Specific Stichtag)

*   **Purpose:** Verify that the migrated job correctly processes data for a given `Stichtag` when no restart value is provided, ensuring output parity with the legacy system. This covers the basic data filtering and insertion logic.
*   **Setup:**
    1.  Clear `DWH_TA_C_VERTRAG`, `FOS_TABLE`, `job_log`, `job_audit` tables (handled by `run_around_tests` fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with a diverse set of test data, including rows that should and should not be selected based on `Gueltig_von`, `Gueltig_bis`, and `Ladedatum` filters for `Stichtag = '15022023'` (2023-02-15).
        ```sql
        -- Data for DWH_TA_C_VERTRAG
        INSERT INTO `project.dataset.DWH_TA_C_VERTRAG` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, Ladedatum, column1, column2) VALUES
        (100, '2023-01-01', '2023-12-31', '2023-01-05', 'A', 'X'), -- Selected
        (101, '2023-01-15', '2023-06-30', '2023-01-20', 'B', 'Y'), -- Selected
        (102, '2023-02-01', '2023-02-10', '2023-02-05', 'C', 'Z'), -- Not selected (Stichtag >= Gueltig_bis)
        (103, '2023-03-01', '2023-03-05', '2023-03-02', 'D', 'W'), -- Not selected (Gueltig_von > Stichtag)
        (104, '2023-01-01', '2023-02-20', '2023-02-16', 'E', 'V'), -- Not selected (Ladedatum >= Stichtag)
        (105, '2023-02-15', '2023-03-15', '2023-02-10', 'F', 'U'); -- Selected (Gueltig_von = Stichtag)
        ```
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh -s 15022023`.
    2.  Capture the resulting data in the legacy `FOS_TABLE`.
    3.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('15022023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The BigQuery `FOS_TABLE` must contain the exact same rows (DWH_VERTRAG_ID, column1, column2) as the legacy `FOS_TABLE`.
    2.  The `job_audit` table should contain one 'STARTED' entry and one 'OK' entry for the job run.

```python
def test_full_load_specific_stichtag():
    test_data = [
        {'DWH_VERTRAG_ID': 100, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-12-31', 'Ladedatum': '2023-01-05', 'column1': 'A', 'column2': 'X'},
        {'DWH_VERTRAG_ID': 101, 'Gueltig_von': '2023-01-15', 'Gueltig_bis': '2023-06-30', 'Ladedatum': '2023-01-20', 'column1': 'B', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 102, 'Gueltig_von': '2023-02-01', 'Gueltig_bis': '2023-02-10', 'Ladedatum': '2023-02-05', 'column1': 'C', 'column2': 'Z'},
        {'DWH_VERTRAG_ID': 103, 'Gueltig_von': '2023-03-01', 'Gueltig_bis': '2023-03-05', 'Ladedatum': '2023-03-02', 'column1': 'D', 'column2': 'W'},
        {'DWH_VERTRAG_ID': 104, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-02-20', 'Ladedatum': '2023-02-16', 'column1': 'E', 'column2': 'V'},
        {'DWH_VERTRAG_ID': 105, 'Gueltig_von': '2023-02-15', 'Gueltig_bis': '2023-03-15', 'Ladedatum': '2023-02-10', 'column1': 'F', 'column2': 'U'},
    ]
    insert_dwh_source_data(test_data)

    stichtag = '15022023' # 2023-02-15
    
    expected_legacy_output = sorted([
        {'DWH_VERTRAG_ID': 100, 'column1': 'A', 'column2': 'X'},
        {'DWH_VERTRAG_ID': 101, 'column1': 'B', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 105, 'column1': 'F', 'column2': 'U'},
    ], key=lambda x: x['DWH_VERTRAG_ID'])

    legacy_fos_data = run_legacy_script(stichtag=stichtag, wiederanlaufWert=0)
    assert legacy_fos_data == expected_legacy_output, "Legacy script simulation mismatch"

    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=None)
    assert success, "BigQuery SP execution failed"

    migrated_fos_data = get_fos_table_data()
    assert migrated_fos_data == expected_legacy_output, "Migrated FOS_TABLE data does not match legacy output"

    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', stichtag, 0)
    assert len(audit_entries) == 2 # STARTED and OK
    assert audit_entries[0]['status'] == 'STARTED'
    assert audit_entries[1]['status'] == 'OK'
    assert audit_entries[1]['finished_at'] is not None
```

#### Test Case 2: Restart Mechanism (Incremental Load)

*   **Purpose:** Verify that the `p_wiederanlaufWert` parameter correctly triggers the `DELETE` and subsequent `INSERT` with the `DWH_VERTRAG_ID > p_restart_value` filter, ensuring correct incremental loading and restart behavior.
*   **Setup:**
    1.  Clear all tables (handled by fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with data.
    3.  Pre-populate `FOS_TABLE` with some data that would be affected by the `DELETE` clause (i.e., `DWH_VERTRAG_ID >= p_wiederanlaufWert`).
        ```sql
        -- DWH_TA_C_VERTRAG (similar to Test 1, plus one new row)
        INSERT INTO `project.dataset.DWH_TA_C_VERTRAG` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, Ladedatum, column1, column2) VALUES
        (100, '2023-01-01', '2023-12-31', '2023-01-05', 'A', 'X'),
        (101, '2023-01-15', '2023-06-30', '2023-01-20', 'B', 'Y'),
        (102, '2023-02-01', '2023-02-10', '2023-02-05', 'C', 'Z'),
        (103, '2023-03-01', '2023-03-05', '2023-03-02', 'D', 'W'),
        (104, '2023-01-01', '2023-02-20', '2023-02-16', 'E', 'V'),
        (105, '2023-02-15', '2023-03-15', '2023-02-10', 'F', 'U'),
        (106, '2023-02-15', '2023-03-15', '2023-02-10', 'G', 'T'); -- New row for restart
        
        -- Pre-populate FOS_TABLE
        INSERT INTO `project.dataset.FOS_TABLE` (DWH_VERTRAG_ID, column1, column2) VALUES
        (99, 'P', 'Q'),         -- Should remain
        (100, 'A_old', 'X_old'), -- Should be deleted and re-inserted (if > restart_value)
        (101, 'B_old', 'Y_old'), -- Should be deleted and re-inserted (if > restart_value)
        (105, 'F_old', 'U_old'); -- Should be deleted and re-inserted (if > restart_value)
        ```
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh -s 15022023 -l 100`.
    2.  Capture the resulting data in the legacy `FOS_TABLE`.
    3.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('15022023', 100)`.
*   **Pass/Fail Criterion:**
    1.  The BigQuery `FOS_TABLE` must contain the exact same rows as the legacy `FOS_TABLE`. Specifically, rows with `DWH_VERTRAG_ID >= 100` from the initial `FOS_TABLE` should be deleted, and then new rows satisfying the filters and `DWH_VERTRAG_ID > 100` should be inserted.
    2.  The `job_audit` table should show a successful run.

```python
def test_restart_mechanism():
    test_data = [
        {'DWH_VERTRAG_ID': 100, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-12-31', 'Ladedatum': '2023-01-05', 'column1': 'A', 'column2': 'X'},
        {'DWH_VERTRAG_ID': 101, 'Gueltig_von': '2023-01-15', 'Gueltig_bis': '2023-06-30', 'Ladedatum': '2023-01-20', 'column1': 'B', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 102, 'Gueltig_von': '2023-02-01', 'Gueltig_bis': '2023-02-10', 'Ladedatum': '2023-02-05', 'column1': 'C', 'column2': 'Z'},
        {'DWH_VERTRAG_ID': 103, 'Gueltig_von': '2023-03-01', 'Gueltig_bis': '2023-03-05', 'Ladedatum': '2023-03-02', 'column1': 'D', 'column2': 'W'},
        {'DWH_VERTRAG_ID': 104, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-02-20', 'Ladedatum': '2023-02-16', 'column1': 'E', 'column2': 'V'},
        {'DWH_VERTRAG_ID': 105, 'Gueltig_von': '2023-02-15', 'Gueltig_bis': '2023-03-15', 'Ladedatum': '2023-02-10', 'column1': 'F', 'column2': 'U'},
        {'DWH_VERTRAG_ID': 106, 'Gueltig_von': '2023-02-15', 'Gueltig_bis': '2023-03-15', 'Ladedatum': '2023-02-10', 'column1': 'G', 'column2': 'T'}, # New row
    ]
    insert_dwh_source_data(test_data)

    # Pre-populate FOS_TABLE
    bq_client.query(f"""
        INSERT INTO `{FOS_TARGET_TABLE}` (DWH_VERTRAG_ID, column1, column2) VALUES
        (99, 'P', 'Q'),
        (100, 'A_old', 'X_old'),
        (101, 'B_old', 'Y_old'),
        (105, 'F_old', 'U_old');
    """).result()

    stichtag = '15022023'
    wiederanlaufWert = 100 # Delete >= 100, Insert > 100

    expected_legacy_output = sorted([
        {'DWH_VERTRAG_ID': 99, 'column1': 'P', 'column2': 'Q'},
        {'DWH_VERTRAG_ID': 101, 'column1': 'B', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 105, 'column1': 'F', 'column2': 'U'},
        {'DWH_VERTRAG_ID': 106, 'column1': 'G', 'column2': 'T'},
    ], key=lambda x: x['DWH_VERTRAG_ID'])

    legacy_fos_data = run_legacy_script(stichtag=stichtag, wiederanlaufWert=wiederanlaufWert)
    # The run_legacy_script mock does not simulate pre-existing data, so we manually add the unaffected row.
    legacy_fos_data.append({'DWH_VERTRAG_ID': 99, 'column1': 'P', 'column2': 'Q'})
    legacy_fos_data.sort(key=lambda x: x['DWH_VERTRAG_ID'])
    assert legacy_fos_data == expected_legacy_output, "Legacy script simulation mismatch for restart"

    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=wiederanlaufWert)
    assert success, "BigQuery SP execution failed for restart"

    migrated_fos_data = get_fos_table_data()
    assert migrated_fos_data == expected_legacy_output, "Migrated FOS_TABLE data does not match legacy output for restart"

    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', stichtag, wiederanlaufWert)
    assert len(audit_entries) == 2
    assert audit_entries[0]['status'] == 'STARTED'
    assert audit_entries[1]['status'] == 'OK'
```

#### Test Case 3: Default Stichtag (System Date)

*   **Purpose:** Verify that when `p_stichtag` is not provided, the job correctly defaults to the current system date and applies the filters accordingly.
*   **Setup:**
    1.  Clear all tables (handled by fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with data where some rows are valid for `CURRENT_DATE` and some are not.
        ```sql
        -- Assume CURRENT_DATE is '2024-01-20' (20012024) for example.
        -- Data for DWH_TA_C_VERTRAG, relative to current date
        INSERT INTO `project.dataset.DWH_TA_C_VERTRAG` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, Ladedatum, column1, column2) VALUES
        (200, '2024-01-01', '2024-01-25', '2024-01-10', 'X', 'Y'), -- Selected (if current_date is 2024-01-20)
        (201, '2024-01-15', '2024-01-19', '2024-01-18', 'A', 'B'), -- Not selected (Stichtag >= Gueltig_bis)
        (202, '2024-01-15', '2024-02-15', '2024-01-20', 'C', 'D'), -- Not selected (Ladedatum >= Stichtag)
        (203, '2024-01-15', '2024-02-15', '2024-01-19', 'E', 'F'); -- Selected (if current_date is 2024-01-20)
        ```
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh`.
    2.  Capture the resulting data in the legacy `FOS_TABLE`.
    3.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr(NULL, NULL)`.
*   **Pass/Fail Criterion:**
    1.  The BigQuery `FOS_TABLE` must contain the exact same rows as the legacy `FOS_TABLE`, based on `CURRENT_DATE` as the `Stichtag`.
    2.  The `job_audit` table should show a successful run with `stichtag` reflecting `CURRENT_DATE` in `DDMMYYYY` format.

```python
def test_default_stichtag():
    current_date_str = datetime.now().strftime('%d%m%Y')
    current_date_obj = datetime.now().date()

    test_data = [
        {'DWH_VERTRAG_ID': 200, 'Gueltig_von': (current_date_obj - timedelta(days=19)).strftime('%Y-%m-%d'), 'Gueltig_bis': (current_date_obj + timedelta(days=5)).strftime('%Y-%m-%d'), 'Ladedatum': (current_date_obj - timedelta(days=10)).strftime('%Y-%m-%d'), 'column1': 'X', 'column2': 'Y'}, # Selected
        {'DWH_VERTRAG_ID': 201, 'Gueltig_von': (current_date_obj - timedelta(days=5)).strftime('%Y-%m-%d'), 'Gueltig_bis': (current_date_obj - timedelta(days=1)).strftime('%Y-%m-%d'), 'Ladedatum': (current_date_obj - timedelta(days=2)).strftime('%Y-%m-%d'), 'column1': 'A', 'column2': 'B'}, # Not selected (Stichtag >= Gueltig_bis)
        {'DWH_VERTRAG_ID': 202, 'Gueltig_von': (current_date_obj - timedelta(days=5)).strftime('%Y-%m-%d'), 'Gueltig_bis': (current_date_obj + timedelta(days=20)).strftime('%Y-%m-%d'), 'Ladedatum': current_date_obj.strftime('%Y-%m-%d'), 'column1': 'C', 'column2': 'D'}, # Not selected (Ladedatum >= Stichtag)
        {'DWH_VERTRAG_ID': 203, 'Gueltig_von': (current_date_obj - timedelta(days=5)).strftime('%Y-%m-%d'), 'Gueltig_bis': (current_date_obj + timedelta(days=20)).strftime('%Y-%m-%d'), 'Ladedatum': (current_date_obj - timedelta(days=1)).strftime('%Y-%m-%d'), 'column1': 'E', 'column2': 'F'}, # Selected
    ]
    insert_dwh_source_data(test_data)

    expected_legacy_output = sorted([
        {'DWH_VERTRAG_ID': 200, 'column1': 'X', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 203, 'column1': 'E', 'column2': 'F'},
    ], key=lambda x: x['DWH_VERTRAG_ID'])

    legacy_fos_data = run_legacy_script(stichtag=None, wiederanlaufWert=0)
    assert legacy_fos_data == expected_legacy_output, "Legacy script simulation mismatch for default stichtag"

    success = call_bigquery_sp(stichtag_in=None, wiederanlaufWert_in=None)
    assert success, "BigQuery SP execution failed for default stichtag"

    migrated_fos_data = get_fos_table_data()
    assert migrated_fos_data == expected_legacy_output, "Migrated FOS_TABLE data does not match legacy output for default stichtag"

    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', current_date_str, 0)
    assert len(audit_entries) == 2
    assert audit_entries[0]['status'] == 'STARTED'
    assert audit_entries[1]['status'] == 'OK'
```

#### Test Case 4: Parameter Validation (Missing Stichtag Error)

*   **Purpose:** Verify that the migrated job correctly handles the error condition when `p_stichtag` is explicitly provided as an empty string or invalid format, matching the legacy script's `pruefeParameterGesetzt` behavior.
*   **Setup:**
    1.  Clear `job_log` and `job_audit` tables (handled by fixture).
    2.  `DWH_TA_C_VERTRAG` can be empty or populated, as the error should occur before data processing.
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh -s ""`. The legacy script should exit with an error code and log an error.
    2.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The BigQuery stored procedure call must fail (raise an error).
    2.  The `job_audit` table must contain an 'ERROR' entry for the job run, with a message indicating a missing or invalid `Stichtag`.
    3.  The `job_log` table should contain an 'ERROR' entry.

```python
def test_parameter_validation_missing_stichtag():
    # No DWH data needed, error occurs before processing

    # Simulate legacy run (expecting error)
    # In a real test, you'd check the exit code of the .ksh script
    # and its log file for error messages.
    legacy_script_would_fail = True 

    # Execute migrated BigQuery SP (expecting failure)
    success = call_bigquery_sp(stichtag_in='', wiederanlaufWert_in=None)
    assert not success, "BigQuery SP execution should have failed for missing stichtag"

    # Verify audit log for error
    current_date_str = datetime.now().strftime('%d%m%Y') # Stichtag defaults to sysdate before validation fails
    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', current_date_str, 0)
    assert any(e['status'] == 'STARTED' for e in audit_entries), "Audit log should contain a STARTED entry"
    assert any(e['status'] == 'ERROR' for e in audit_entries), "Audit log should contain an ERROR entry"
    assert any('Stichtag missing' in e['message'] for e in audit_entries), "Error message should indicate missing Stichtag"
    
    # Verify job_log table
    job_log_entries = get_job_log_entries('AUSD_BP_TA_BPR_BESCHR')
    assert any(e['status'] == 'ERROR' for e in job_log_entries), "job_log should contain an ERROR entry"
    assert any('Stichtag missing' in e['message'] for e in job_log_entries), "job_log error message should indicate missing Stichtag"
```

#### Test Case 5: Empty Source Table

*   **Purpose:** Verify that the job handles an empty source table gracefully, resulting in an empty target table and a successful audit entry.
*   **Setup:**
    1.  Clear all tables (handled by fixture).
    2.  `DWH_TA_C_VERTRAG` remains empty.
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh -s 01012023`.
    2.  Capture the resulting data in the legacy `FOS_TABLE`.
    3.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  Both legacy and BigQuery `FOS_TABLE` must be empty.
    2.  The `job_audit` table should show a successful run.

```python
def test_empty_source_table():
    # DWH_TA_C_VERTRAG remains empty

    stichtag = '01012023'

    expected_legacy_output = [] # Empty FOS_TABLE
    legacy_fos_data = run_legacy_script(stichtag=stichtag, wiederanlaufWert=0)
    assert legacy_fos_data == expected_legacy_output, "Legacy script simulation mismatch for empty source"

    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=None)
    assert success, "BigQuery SP execution failed for empty source"

    migrated_fos_data = get_fos_table_data()
    assert not migrated_fos_data, "Migrated FOS_TABLE should be empty"

    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', stichtag, 0)
    assert len(audit_entries) == 2
    assert audit_entries[0]['status'] == 'STARTED'
    assert audit_entries[1]['status'] == 'OK'
```

#### Test Case 6: No Rows Selected by Filters

*   **Purpose:** Verify that if source data exists but no rows satisfy the filtering criteria, the target table remains empty (or becomes empty if pre-existing data was deleted by restart logic).
*   **Setup:**
    1.  Clear all tables (handled by fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with data that *will not* satisfy the filters for the chosen `Stichtag = '01012023'` (2023-01-01).
        ```sql
        -- Stichtag '01012023' (2023-01-01)
        INSERT INTO `project.dataset.DWH_TA_C_VERTRAG` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, Ladedatum, column1, column2) VALUES
        (100, '2023-01-01', '2023-12-31', '2023-01-05', 'A', 'X'), -- Ladedatum >= Stichtag
        (101, '2023-01-15', '2023-06-30', '2023-01-20', 'B', 'Y'); -- Gueltig_von > Stichtag
        ```
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh -s 01012023`.
    2.  Capture the resulting data in the legacy `FOS_TABLE`.
    3.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  Both legacy and BigQuery `FOS_TABLE` must be empty.
    2.  The `job_audit` table should show a successful run.

```python
def test_no_rows_selected_by_filters():
    stichtag = '01012023' # 2023-01-01

    test_data = [
        {'DWH_VERTRAG_ID': 100, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-12-31', 'Ladedatum': '2023-01-05', 'column1': 'A', 'column2': 'X'}, # Ladedatum >= Stichtag
        {'DWH_VERTRAG_ID': 101, 'Gueltig_von': '2023-01-15', 'Gueltig_bis': '2023-06-30', 'Ladedatum': '2023-01-20', 'column1': 'B', 'column2': 'Y'}, # Gueltig_von > Stichtag
    ]
    insert_dwh_source_data(test_data)

    expected_legacy_output = [] # Empty FOS_TABLE
    legacy_fos_data = run_legacy_script(stichtag=stichtag, wiederanlaufWert=0)
    assert legacy_fos_data == expected_legacy_output, "Legacy script simulation mismatch for no selected rows"

    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=None)
    assert success, "BigQuery SP execution failed for no selected rows"

    migrated_fos_data = get_fos_table_data()
    assert not migrated_fos_data, "Migrated FOS_TABLE should be empty when no rows selected"

    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', stichtag, 0)
    assert len(audit_entries) == 2
    assert audit_entries[0]['status'] == 'STARTED'
    assert audit_entries[1]['status'] == 'OK'
```

#### Test Case 7: Date Boundary Conditions (Gueltig_von, Gueltig_bis, Ladedatum)

*   **Purpose:** Specifically test the strict inequality (`<`) and equality (`<=`) in date filters to ensure correct interpretation of date boundaries.
    *   `Gueltig_von <= Stichtag`
    *   `Stichtag < Gueltig_bis`
    *   `Ladedatum < Stichtag`
*   **Setup:**
    1.  Clear all tables (handled by fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with data that specifically hits these boundaries for `Stichtag = '10012024'` (2024-01-10).
        ```sql
        -- Stichtag '10012024' (2024-01-10)
        INSERT INTO `project.dataset.DWH_TA_C_VERTRAG` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, Ladedatum, column1, column2) VALUES
        (300, '2024-01-10', '2024-01-11', '2024-01-09', 'A', 'A'), -- Selected (Gueltig_von = Stichtag, Stichtag < Gueltig_bis, Ladedatum < Stichtag)
        (301, '2024-01-09', '2024-01-10', '2024-01-08', 'B', 'B'), -- Not Selected (Stichtag < Gueltig_bis is false)
        (302, '2024-01-11', '2024-01-12', '2024-01-09', 'C', 'C'), -- Not Selected (Gueltig_von <= Stichtag is false)
        (303, '2024-01-09', '2024-01-11', '2024-01-10', 'D', 'D'); -- Not Selected (Ladedatum < Stichtag is false)
        ```
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh -s 10012024`.
    2.  Capture the resulting data in the legacy `FOS_TABLE`.
    3.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('10012024', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The BigQuery `FOS_TABLE` must contain the exact same rows as the legacy `FOS_TABLE`, verifying correct handling of date boundary conditions.
    2.  The `job_audit` table should show a successful run.

```python
def test_date_boundary_conditions():
    stichtag = '10012024' # 2024-01-10

    test_data = [
        {'DWH_VERTRAG_ID': 300, 'Gueltig_von': '2024-01-10', 'Gueltig_bis': '2024-01-11', 'Ladedatum': '2024-01-09', 'column1': 'A', 'column2': 'A'}, # Selected
        {'DWH_VERTRAG_ID': 301, 'Gueltig_von': '2024-01-09', 'Gueltig_bis': '2024-01-10', 'Ladedatum': '2024-01-08', 'column1': 'B', 'column2': 'B'}, # Not Selected (Stichtag < Gueltig_bis is false)
        {'DWH_VERTRAG_ID': 302, 'Gueltig_von': '2024-01-11', 'Gueltig_bis': '2024-01-12', 'Ladedatum': '2024-01-09', 'column1': 'C', 'column2': 'C'}, # Not Selected (Gueltig_von <= Stichtag is false)
        {'DWH_VERTRAG_ID': 303, 'Gueltig_von': '2024-01-09', 'Gueltig_bis': '2024-01-11', 'Ladedatum': '2024-01-10', 'column1': 'D', 'column2': 'D'}, # Not Selected (Ladedatum < Stichtag is false)
    ]
    insert_dwh_source_data(test_data)

    expected_legacy_output = sorted([
        {'DWH_VERTRAG_ID': 300, 'column1': 'A', 'column2': 'A'},
    ], key=lambda x: x['DWH_VERTRAG_ID'])

    legacy_fos_data = run_legacy_script(stichtag=stichtag, wiederanlaufWert=0)
    assert legacy_fos_data == expected_legacy_output, "Legacy script simulation mismatch for date boundaries"

    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=None)
    assert success, "BigQuery SP execution failed for date boundaries"

    migrated_fos_data = get_fos_table_data()
    assert migrated_fos_data == expected_legacy_output, "Migrated FOS_TABLE data does not match legacy output for date boundaries"

    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', stichtag, 0)
    assert len(audit_entries) == 2
    assert audit_entries[0]['status'] == 'STARTED'
    assert audit_entries[1]['status'] == 'OK'
```

#### Test Case 8: Schema and Data Type Integrity

*   **Purpose:** Verify that the schema of the target `FOS_TABLE` matches the expected structure and that data types are correctly handled during insertion.
*   **Setup:**
    1.  Ensure `FOS_TABLE` is created with the expected DDL (handled by fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with data that includes various string lengths and integer values.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  Query the schema of `FOS_TABLE` and assert that column names and data types match the DDL.
    2.  Verify that the inserted data in `FOS_TABLE` retains its original values and types.

```python
def test_schema_and_data_type_integrity():
    stichtag = '01012023' # 2023-01-01

    test_data = [
        {'DWH_VERTRAG_ID': 1, 'Gueltig_von': '2022-12-01', 'Gueltig_bis': '2023-01-02', 'Ladedatum': '2022-12-15', 'column1': 'ShortString', 'column2': 'LongerStringValue'},
        {'DWH_VERTRAG_ID': 2, 'Gueltig_von': '2022-12-01', 'Gueltig_bis': '2023-01-02', 'Ladedatum': '2022-12-15', 'column1': 'Another', 'column2': 'Value'},
    ]
    insert_dwh_source_data(test_data)

    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=None)
    assert success, "BigQuery SP execution failed for schema test"

    # 1. Verify FOS_TABLE schema
    table = bq_client.get_table(FOS_TARGET_TABLE)
    schema_fields = {field.name: field.field_type for field in table.schema}
    expected_schema = {
        'DWH_VERTRAG_ID': 'INT64',
        'column1': 'STRING',
        'column2': 'STRING'
    }
    assert schema_fields == expected_schema, f"FOS_TABLE schema mismatch. Expected: {expected_schema}, Got: {schema_fields}"

    # 2. Verify data types and values are preserved
    migrated_fos_data = get_fos_table_data()
    expected_data_in_fos = sorted([
        {'DWH_VERTRAG_ID': 1, 'column1': 'ShortString', 'column2': 'LongerStringValue'},
        {'DWH_VERTRAG_ID': 2, 'column1': 'Another', 'column2': 'Value'},
    ], key=lambda x: x['DWH_VERTRAG_ID'])
    assert migrated_fos_data == expected_data_in_fos, "Data values or types not preserved in FOS_TABLE"
```

#### Test Case 9: Audit Logging for Error Scenario

*   **Purpose:** Verify that the `job_audit` table correctly captures error states and messages when the core processing logic fails.
*   **Setup:**
    1.  Clear `job_audit` table (handled by fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with data to ensure the core processing logic is invoked.
    3.  *Crucially, this test requires a mechanism to force `process_contract_cache_data` to fail.* This could involve temporarily deploying a modified version of `process_contract_cache_data` that always raises an error, or setting up data that causes a specific, known BigQuery error within that procedure.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('01012023', NULL)`.
*   **Pass/Fail Criterion:**
    1.  The BigQuery stored procedure call must fail.
    2.  The `job_audit` table must contain a 'STARTED' entry and an 'ERROR' entry for the job run, with a relevant error message.

```python
# This test requires a way to *force* the called procedure (process_contract_cache_data) to fail.
# In a real environment, this might involve:
# 1. Temporarily deploying a version of process_contract_cache_data that always raises an error.
# 2. Setting up data that causes a specific, known error in process_contract_cache_data (e.g., invalid date format if not handled).
# For this example, we'll assume a mock or temporary SP deployment.

def test_audit_logging_for_error_scenario():
    stichtag = '01012023'

    # Insert some data to ensure process_contract_cache_data would be called
    insert_dwh_source_data([
        {'DWH_VERTRAG_ID': 1, 'Gueltig_von': '2022-12-01', 'Gueltig_bis': '2023-01-02', 'Ladedatum': '2022-12-15', 'column1': 'A', 'column2': 'X'}
    ])

    # --- MOCKING/SIMULATION OF FAILURE ---
    # To make this test runnable, we would temporarily redeploy process_contract_cache_data
    # to always raise an error. Example:
    # bq_client.query(f"""
    #     CREATE OR REPLACE PROCEDURE `{CORE_PROCESSING_SP}`(IN p_stichtag STRING, IN p_restart_value INT64, IN p_job_nr INT64)
    #     BEGIN
    #         RAISE USING MESSAGE = 'Simulated core SP error for testing purposes.';
    #     END;
    # """).result()
    
    # Execute migrated BigQuery SP (expecting failure)
    # The call_bigquery_sp helper already catches exceptions and returns False.
    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=None)
    assert not success, "BigQuery SP execution should have failed to test error logging"

    # Verify audit log for error
    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', stichtag, 0)
    assert len(audit_entries) >= 2 # At least STARTED and ERROR
    assert audit_entries[0]['status'] == 'STARTED'
    assert any(e['status'] == 'ERROR' for e in audit_entries), "Audit log should contain an ERROR entry"
    error_entry = next((e for e in audit_entries if e['status'] == 'ERROR'), None)
    assert error_entry is not None
    assert error_entry['message'] is not None and len(error_entry['message']) > 0, "Error message should be present"
    assert error_entry['finished_at'] is None # Error entry should not have finished_at set by the success update
    
    # --- CLEANUP MOCK ---
    # After the test, restore the original process_contract_cache_data.
    # This would involve redeploying the correct version of process_contract_cache_data.
```

#### Test Case 10: `p_wiederanlaufWert` with no matching `DWH_VERTRAG_ID` for delete

*   **Purpose:** Verify that the `DELETE` operation for restart logic correctly handles cases where `p_wiederanlaufWert` is set, but no rows in `FOS_TABLE` match the deletion criteria (`DWH_VERTRAG_ID >= p_wiederanlaufWert`).
*   **Setup:**
    1.  Clear all tables (handled by fixture).
    2.  Populate `DWH_TA_C_VERTRAG` with data.
    3.  Pre-populate `FOS_TABLE` with data, but ensure all `DWH_VERTRAG_ID`s are *less than* the `p_wiederanlaufWert` to be used.
        ```sql
        -- DWH_TA_C_VERTRAG (same as Test 1)
        INSERT INTO `project.dataset.DWH_TA_C_VERTRAG` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, Ladedatum, column1, column2) VALUES
        (100, '2023-01-01', '2023-12-31', '2023-01-05', 'A', 'X'),
        (101, '2023-01-15', '2023-06-30', '2023-01-20', 'B', 'Y'),
        (105, '2023-02-15', '2023-03-15', '2023-02-10', 'F', 'U');
        
        -- Pre-populate FOS_TABLE
        INSERT INTO `project.dataset.FOS_TABLE` (DWH_VERTRAG_ID, column1, column2) VALUES
        (10, 'P', 'Q'),
        (20, 'R', 'S');
        ```
*   **Action:**
    1.  Execute the legacy KornShell script: `r_ausd_bp_ta_bpr_beschr.ksh -s 15022023 -l 50`.
    2.  Capture the resulting data in the legacy `FOS_TABLE`.
    3.  Execute the BigQuery Stored Procedure: `CALL project.dataset.ausd_bp_ta_bpr_beschr('15022023', 50)`.
*   **Pass/Fail Criterion:**
    1.  The BigQuery `FOS_TABLE` must contain the exact same rows as the legacy `FOS_TABLE`. The pre-existing rows (10, 20) should remain, and new rows (100, 101, 105) should be inserted, as the `DELETE` operation should have affected no rows.
    2.  The `job_audit` table should show a successful run.

```python
def test_restart_no_delete_match():
    test_data = [
        {'DWH_VERTRAG_ID': 100, 'Gueltig_von': '2023-01-01', 'Gueltig_bis': '2023-12-31', 'Ladedatum': '2023-01-05', 'column1': 'A', 'column2': 'X'},
        {'DWH_VERTRAG_ID': 101, 'Gueltig_von': '2023-01-15', 'Gueltig_bis': '2023-06-30', 'Ladedatum': '2023-01-20', 'column1': 'B', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 105, 'Gueltig_von': '2023-02-15', 'Gueltig_bis': '2023-03-15', 'Ladedatum': '2023-02-10', 'column1': 'F', 'column2': 'U'},
    ]
    insert_dwh_source_data(test_data)

    # Pre-populate FOS_TABLE with IDs less than restart_value
    bq_client.query(f"""
        INSERT INTO `{FOS_TARGET_TABLE}` (DWH_VERTRAG_ID, column1, column2) VALUES
        (10, 'P', 'Q'),
        (20, 'R', 'S');
    """).result()

    stichtag = '15022023'
    wiederanlaufWert = 50 # No delete should occur, insert > 50

    expected_legacy_output = sorted([
        {'DWH_VERTRAG_ID': 10, 'column1': 'P', 'column2': 'Q'},
        {'DWH_VERTRAG_ID': 20, 'column1': 'R', 'column2': 'S'},
        {'DWH_VERTRAG_ID': 100, 'column1': 'A', 'column2': 'X'},
        {'DWH_VERTRAG_ID': 101, 'column1': 'B', 'column2': 'Y'},
        {'DWH_VERTRAG_ID': 105, 'column1': 'F', 'column2': 'U'},
    ], key=lambda x: x['DWH_VERTRAG_ID'])

    legacy_fos_data = run_legacy_script(stichtag=stichtag, wiederanlaufWert=wiederanlaufWert)
    # The run_legacy_script mock does not simulate pre-existing data, so we manually add the unaffected rows.
    legacy_fos_data.extend([
        {'DWH_VERTRAG_ID': 10, 'column1': 'P', 'column2': 'Q'},
        {'DWH_VERTRAG_ID': 20, 'column1': 'R', 'column2': 'S'}
    ])
    legacy_fos_data.sort(key=lambda x: x['DWH_VERTRAG_ID'])
    assert legacy_fos_data == expected_legacy_output, "Legacy script simulation mismatch for restart with no delete match"

    success = call_bigquery_sp(stichtag_in=stichtag, wiederanlaufWert_in=wiederanlaufWert)
    assert success, "BigQuery SP execution failed for restart with no delete match"

    migrated_fos_data = get_fos_table_data()
    assert migrated_fos_data == expected_legacy_output, "Migrated FOS_TABLE data does not match legacy output for restart with no delete match"

    audit_entries = get_job_audit_entries('AUSD_BP_TA_BPR_BESCHR', stichtag, wiederanlaufWert)
    assert len(audit_entries) == 2
    assert audit_entries[0]['status'] == 'STARTED'
    assert audit_entries[1]['status'] == 'OK'
```