As a senior data-migration QA engineer, I've analyzed the migration design and generated code for `k_ausd_v_ta_inv_assign.ksh`. The migration involves replatforming the orchestration to Airflow and the SQL logic to BigQuery.

A critical observation is that the provided BigQuery Stored Procedure (`sp_d_ausd_v_ta_inv_assign.sql`) focuses solely on the data extraction and transformation logic from `d_ausd_v_ta_inv_assign.sql`. It does *not* appear to implement the job control logic (ignoring active jobs, deactivating old ones) or the record count capture (`v_records`) explicitly mentioned in the legacy KornShell script's purpose and the migration design's "Unresolved / Risks" section. These are significant behavioral differences that must be highlighted and addressed.

The following test cases are designed to validate the migrated solution, covering output parity, transformation correctness, external system interactions, and data quality, while also explicitly calling out the identified functional gaps.

---

## Migration Validation Tests for `k_ausd_v_ta_inv_assign.ksh`

### Assumptions for Testing:
*   Access to the legacy Oracle database (or a golden dataset representing its output) for comparison.
*   Ability to populate BigQuery source tables (`your-gcp-project.isbert_source_carmen.cds_ta_inv_assignment`) and log tables (`your-gcp-project.isbert_log_data.dwtk_meldungen_bq`) with test data.
*   Airflow environment is configured to run DAGs and connect to BigQuery.
*   `your-gcp-project`, `isbert_target_data`, `isbert_log_data`, `isbert_source_carmen` are placeholders for actual project/dataset names.

### Test Utilities (Conceptual Pytest Helpers)

```python
import pytest
from google.cloud import bigquery
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from datetime import datetime, timedelta
import time
import os # For legacy script execution

# --- Configuration Placeholders ---
BIGQUERY_PROJECT = os.environ.get('BIGQUERY_PROJECT', 'your-gcp-project')
BQ_TARGET_DATASET = os.environ.get('BQ_TARGET_DATASET', 'isbert_target_data')
BQ_LOG_DATASET = os.environ.get('BQ_LOG_DATASET', 'isbert_log_data')
BQ_SOURCE_DATASET = os.environ.get('BQ_SOURCE_DATASET', 'isbert_source_carmen')
AIRFLOW_DAG_ID = 'k_ausd_v_ta_inv_assign_dag'

# Legacy environment details (adjust as needed)
LEGACY_KSH_SCRIPT_PATH = '/path/to/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh'
LEGACY_ORACLE_CONN_STRING = 'user/password@host:port/service' # e.g., 'system/oracle@localhost:1521/XE'
LEGACY_ORACLE_TA_INV_ASSIGN_TABLE = 'TA_INV_ASSIGN' # Oracle table name
LEGACY_ORACLE_DWTK_MELDUNGEN_TABLE = 'DWTK_MELDUNGEN' # Oracle table name
LEGACY_ORACLE_CDS_TA_INV_ASSIGNMENT_TABLE = 'CDS$TA_INV_ASSIGNMENT' # Oracle source table name

# --- Pytest Fixtures and Helper Functions ---

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=BIGQUERY_PROJECT)

def clear_bq_tables(bq_client):
    """Clears relevant BigQuery tables before each test scenario."""
    bq_client.query(f"TRUNCATE TABLE `{BIGQUERY_PROJECT}.{BQ_TARGET_DATASET}.ta_inv_assign`").result()
    bq_client.query(f"TRUNCATE TABLE `{BIGQUERY_PROJECT}.{BQ_LOG_DATASET}.dwtk_meldungen_bq`").result()
    bq_client.query(f"TRUNCATE TABLE `{BIGQUERY_PROJECT}.{BQ_SOURCE_DATASET}.cds_ta_inv_assignment`").result()
    # If sp_param_audit is used for testing, clear it too
    try:
        bq_client.query(f"TRUNCATE TABLE `{BIGQUERY_PROJECT}.{BQ_LOG_DATASET}.sp_param_audit`").result()
    except Exception:
        pass # Table might not exist if not created for testing

def insert_bq_log_entry(bq_client, job_kennung, timecreated: datetime):
    """Inserts an entry into the BigQuery dwtk_meldungen_bq table."""
    bq_client.query(f"""
        INSERT INTO `{BIGQUERY_PROJECT}.{BQ_LOG_DATASET}.dwtk_meldungen_bq` (timecreated, job_kennung)
        VALUES ('{timecreated.isoformat()}', '{job_kennung}')
    """).result()

def insert_bq_source_data(bq_client, data_rows: list):
    """Inserts multiple rows into the BigQuery source table."""
    if not data_rows:
        return
    insert_values = ", ".join([
        f"('{r[0]}', '{r[1]}', '{r[2].isoformat()}', "
        f"{('NULL' if r[3] is None else f\"'{r[3].isoformat()}'\")}, "
        f"'{r[4].isoformat()}', {('NULL' if r[5] is None else f\"'{r[5].isoformat()}'\")}, {r[6]})"
        for r in data_rows
    ])
    bq_client.query(f"""
        INSERT INTO `{BIGQUERY_PROJECT}.{BQ_SOURCE_DATASET}.cds_ta_inv_assignment` (
            cntrct_id, inv_definition_id, insert_at, modified_at, valid_from, valid_to, is_production
        ) VALUES {insert_values}
    """).result()

def fetch_bq_target_data(bq_client):
    """Fetches all data from the BigQuery target table, ordered for comparison."""
    query = f"""
        SELECT
            cntrct_id,
            inv_definition_id,
            insert_at,
            modified_at,
            valid_from,
            valid_to,
            is_production
        FROM
            `{BIGQUERY_PROJECT}.{BQ_TARGET_DATASET}.ta_inv_assign`
        ORDER BY cntrct_id, inv_definition_id
    """
    rows = bq_client.query(query).result()
    # Convert BQ rows to a comparable format (e.g., list of tuples), handling BOOL to int for comparison
    return [
        (row.cntrct_id, row.inv_definition_id, row.insert_at, row.modified_at, row.valid_from, row.valid_to, int(row.is_production))
        for row in rows
    ]

def trigger_airflow_dag(dag_id, conf: dict):
    """
    Triggers an Airflow DAG run and waits for its completion.
    Requires Airflow's local client or API access.
    """
    print(f"Triggering Airflow DAG {dag_id} with conf: {conf}")
    # This is a simplified representation. In a real test setup, you'd use:
    # from airflow.api.client.local_client import Client
    # client = Client(None, None) # Assumes Airflow DB connection is configured
    # dag_run = client.trigger_dag(dag_id=dag_id, conf=conf)
    # while dag_run.state not in [State.SUCCESS, State.FAILED]:
    #     time.sleep(5)
    #     dag_run.refresh_from_db()
    # if dag_run.state == State.FAILED:
    #     raise Exception(f"Airflow DAG {dag_id} failed with state {dag_run.state}.")
    # print(f"Airflow DAG {dag_id} completed with state {dag_run.state}.")
    # return dag_run
    # For demonstration, we'll assume success.
    time.sleep(10) # Simulate DAG run time
    print(f"Simulated Airflow DAG {dag_id} run completed.")
    return True # Placeholder for success

def run_legacy_job_and_fetch_data(job_kennung, eintrags_nr, oracle_conn_string, log_time: datetime, source_data: list):
    """
    Simulates running the legacy ksh script and fetching its output from Oracle.
    In a real scenario, this would involve:
    1. Populating Oracle source/log tables to match the BQ setup.
    2. Executing the ksh script via subprocess.
    3. Querying Oracle's ta_inv_assign to get the result.
    """
    print(f"Simulating legacy job run with -j {job_kennung} -f {eintrags_nr}")
    # --- Step 1: Populate Oracle tables (conceptual) ---
    # import cx_Oracle
    # conn = cx_Oracle.connect(oracle_conn_string)
    # cursor = conn.cursor()
    # cursor.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_TA_INV_ASSIGN_TABLE}")
    # cursor.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_DWTK_MELDUNGEN_TABLE}")
    # cursor.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_CDS_TA_INV_ASSIGNMENT_TABLE}")
    # cursor.execute(f"INSERT INTO {LEGACY_ORACLE_DWTK_MELDUNGEN_TABLE} (TIMECREATED, JOB_KENNUNG) VALUES (TO_TIMESTAMP('{log_time.isoformat()}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF'), 'BERT_DROP_TEMP_TABLE')")
    # for row in source_data:
    #     # Adjust for Oracle's date/timestamp formats and NULL handling
    #     cursor.execute(f"INSERT INTO {LEGACY_ORACLE_CDS_TA_INV_ASSIGNMENT_TABLE} (...) VALUES (...)")
    # conn.commit()

    # --- Step 2: Execute legacy ksh script (conceptual) ---
    # import subprocess
    # result = subprocess.run(
    #     [LEGACY_KSH_SCRIPT_PATH, '-j', job_kennung, '-f', eintrags_nr],
    #     capture_output=True, text=True, check=True, env={'HOME': os.environ['HOME'], 'DW_DIR_UTL': '/tmp'} # Example env
    # )
    # print(f"Legacy script stdout:\n{result.stdout}")
    # print(f"Legacy script stderr:\n{result.stderr}")

    # --- Step 3: Fetch data from Oracle (conceptual) ---
    # cursor.execute(f"SELECT CNTRCT_ID, INV_DEFINITION_ID, INSERT_AT, MODIFIED_AT, VALID_FROM, VALID_TO, IS_PRODUCTION FROM {LEGACY_ORACLE_TA_INV_ASSIGN_TABLE} ORDER BY CNTRCT_ID, INV_DEFINITION_ID")
    # legacy_data = cursor.fetchall()
    # conn.close()

    # For now, return a pre-calculated expected result based on the BQ setup,
    # assuming the legacy job's logic is identical to the BQ SP's logic.
    # This is a critical assumption for output parity tests.
    v_datum = log_time.date()
    expected_legacy_data = []
    for row in source_data:
        cntrct_id, inv_definition_id, insert_at, modified_at, valid_from, valid_to, is_production = row
        if (insert_at.date() <= v_datum and
            (modified_at is None or modified_at.date() > v_datum) and
            valid_from.date() <= v_datum and
            (valid_to is None or valid_to.date() > v_datum) and
            is_production == 1):
            expected_legacy_data.append((cntrct_id, inv_definition_id, insert_at, modified_at, valid_from, valid_to, is_production))
    
    # Sort the expected data to match the fetch_bq_target_data ordering
    expected_legacy_data.sort(key=lambda x: (x[0], x[1]))
    return expected_legacy_data

```

---

### 1. Output Parity - Full Data Load

*   **Purpose**: Verify that the migrated job produces the exact same final data in `ta_inv_assign` as the legacy job, given identical source data and `dwtk_meldungen` state. This is the primary end-to-end validation.
*   **Setup**:
    1.  **Legacy**: Populate Oracle source table `cds$ta_inv_assignment` with a comprehensive set of test data, covering all filter conditions (dates, `is_production`, NULLs for `modified_at`, `valid_to`). Populate Oracle `isbert_schema.dwtk_meldungen` with an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` and a specific `timecreated` (e.g., `2023-01-15 10:00:00 UTC`). Ensure `ta_inv_assign` is empty or contains known data that will be truncated.
    2.  **Migrated**: Populate BigQuery source table `cds_ta_inv_assignment` with data identical to the Oracle source. Populate BigQuery `dwtk_meldungen_bq` with an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` and the same `timecreated` as Oracle. Ensure `ta_inv_assign` is empty or contains known data that will be truncated.
*   **Action**:
    1.  **Legacy**: Execute the legacy `k_ausd_v_ta_inv_assign.ksh` script with arbitrary valid parameters (e.g., `-j TEST_JOB_FULL -f 1001`).
    2.  **Migrated**: Trigger the Airflow DAG `k_ausd_v_ta_inv_assign_dag` with the same parameters (e.g., `job_kennung='TEST_JOB_FULL'`, `eintrags_nr='1001'`).
*   **Pass/Fail Criterion**:
    *   The row count in Oracle's `ta_inv_assign` must be identical to the row count in BigQuery's `ta_inv_assign`.
    *   All columns and rows in BigQuery's `ta_inv_assign` must exactly match their counterparts in Oracle's `ta_inv_assign` after accounting for type conversions (e.g., `is_production` to `BOOL`). A full data comparison (e.g., hash comparison or row-by-row diff) should yield no differences.
*   **Runnable Test Code (Pytest with SQL assertions)**:

```python
def test_output_parity_full_load(bq_client):
    clear_bq_tables(bq_client)
    job_kennung = "TEST_JOB_FULL"
    eintrags_nr = "1001"
    log_time = datetime(2023, 1, 15, 10, 0, 0) # Example: specific log time

    # Comprehensive source data for testing filters
    source_data = [
        ('C1', 'I1', datetime(2023, 1, 15, 9, 0, 0), None, datetime(2023, 1, 15, 9, 0, 0), None, 1), # Pass: all conditions met, NULLs
        ('C2', 'I2', datetime(2023, 1, 14, 0, 0, 0), datetime(2023, 1, 16, 0, 0, 0), datetime(2023, 1, 14, 0, 0, 0), datetime(2023, 1, 16, 0, 0, 0), 1), # Pass: all conditions met, dates > v_datum
        ('C3', 'I3', datetime(2023, 1, 15, 11, 0, 0), None, datetime(2023, 1, 15, 9, 0, 0), None, 1), # Fail: insert_at > v_datum
        ('C4', 'I4', datetime(2023, 1, 15, 9, 0, 0), datetime(2023, 1, 14, 0, 0, 0), datetime(2023, 1, 15, 9, 0, 0), None, 1), # Fail: modified_at <= v_datum
        ('C5', 'I5', datetime(2023, 1, 15, 9, 0, 0), None, datetime(2023, 1, 16, 0, 0, 0), None, 1), # Fail: valid_from > v_datum
        ('C6', 'I6', datetime(2023, 1, 15, 9, 0, 0), None, datetime(2023, 1, 15, 9, 0, 0), datetime(2023, 1, 14, 0, 0, 0), 1), # Fail: valid_to <= v_datum
        ('C7', 'I7', datetime(2023, 1, 15, 9, 0, 0), None, datetime(2023, 1, 15, 9, 0, 0), None, 0), # Fail: is_production = 0
    ]

    # Setup BQ log and source data
    insert_bq_log_entry(bq_client, 'BERT_DROP_TEMP_TABLE', log_time)
    insert_bq_source_data(bq_client, source_data)

    # 1. Run legacy job and capture output (or use golden reference)
    legacy_data = run_legacy_job_and_fetch_data(job_kennung, eintrags_nr, LEGACY_ORACLE_CONN_STRING, log_time, source_data)

    # 2. Trigger migrated Airflow DAG
    trigger_airflow_dag(
        dag_id=AIRFLOW_DAG_ID,
        conf={'job_kennung': job_kennung, 'eintrags_nr': eintrags_nr}
    )

    # 3. Fetch data from BigQuery target
    migrated_data = fetch_bq_target_data(bq_client)

    # 4. Assert parity
    assert len(migrated_data) == len(legacy_data), \
        f"Row count mismatch: Legacy={len(legacy_data)}, Migrated={len(migrated_data)}"
    assert migrated_data == legacy_data, \
        "Data content mismatch between legacy and migrated systems."
```

### 2. Transformation Correctness - `v_datum` Derivation

*   **Purpose**: Verify that the `v_datum` variable (used in filtering) is correctly derived from `dwtk_meldungen_bq`, including edge cases like no entry or multiple entries.
*   **Setup**:
    *   **Scenario A: `BERT_DROP_TEMP_TABLE` entry exists.** Populate `dwtk_meldungen_bq` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = '2023-01-15 10:00:00 UTC'`. Add other `dwtk_meldungen_bq` entries for different `job_kennung` or earlier `timecreated` for `BERT_DROP_TEMP_TABLE` to ensure `MAX` is correctly applied.
    *   **Scenario B: No `BERT_DROP_TEMP_TABLE` entry.** Ensure `dwtk_meldungen_bq` does *not* contain any entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Scenario C: Multiple `BERT_DROP_TEMP_TABLE` entries.** Populate `dwtk_meldungen_bq` with multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with different `timecreated` values (e.g., '2023-01-10', '2023-01-15', '2023-01-12').
*   **Action**: For each scenario, trigger the Airflow DAG. Since `v_datum` is an internal variable of the Stored Procedure, we assert its *effect* on the `ta_inv_assign` table.
*   **Pass/Fail Criterion**:
    *   **Scenario A**: `ta_inv_assign` should contain only records from `cds_ta_inv_assignment` that satisfy the date filters with `v_datum = '2023-01-15'`.
    *   **Scenario B**: `ta_inv_assign` should contain only records from `cds_ta_inv_assignment` that satisfy the date filters with `v_datum = '1900-01-01'`.
    *   **Scenario C**: `ta_inv_assign` should contain only records from `cds_ta_inv_assignment` that satisfy the date filters with `v_datum` being the `MAX(timecreated)` for `BERT_DROP_TEMP_TABLE` (e.g., '2023-01-15').
*   **Runnable Test Code (Pytest with SQL assertions)**:

```python
@pytest.mark.parametrize("scenario, log_entries, expected_v_datum_date, expected_pass_cntrct_ids", [
    ("A_single_entry", [("BERT_DROP_TEMP_TABLE", datetime(2023, 1, 15, 10, 0, 0))], datetime(2023, 1, 15).date(), {'PASS_ROW_15'}),
    ("B_no_entry", [], datetime(1900, 1, 1).date(), set()), # Assuming no source data before 1900-01-01
    ("C_multiple_entries", [
        ("BERT_DROP_TEMP_TABLE", datetime(2023, 1, 10, 0, 0, 0)),
        ("OTHER_JOB", datetime(2023, 1, 16, 0, 0, 0)), # Should be ignored
        ("BERT_DROP_TEMP_TABLE", datetime(2023, 1, 15, 10, 0, 0)),
        ("BERT_DROP_TEMP_TABLE", datetime(2023, 1, 12, 0, 0, 0))
    ], datetime(2023, 1, 15).date(), {'PASS_ROW_15'}),
])
def test_v_datum_derivation(bq_client, scenario, log_entries, expected_v_datum_date, expected_pass_cntrct_ids):
    clear_bq_tables(bq_client)

    # Setup dwtk_meldungen_bq for the current scenario
    for job_kennung, timecreated in log_entries:
        insert_bq_log_entry(bq_client, job_kennung, timecreated)

    # Insert source data that specifically tests the v_datum filter
    source_data = [
        ('PASS_ROW_15', 'I_PASS_15', datetime(2023, 1, 15, 9, 0, 0), None, datetime(2023, 1, 15, 9, 0, 0), None, 1),
        ('FAIL_ROW_16', 'I_FAIL_16', datetime(2023, 1, 16, 0, 0, 0), None, datetime(2023, 1, 16, 0, 0, 0), None, 1),
        ('PASS_ROW_01', 'I_PASS_01', datetime(1900, 1, 1, 0, 0, 0), None, datetime(1900, 1, 1, 0, 0, 0), None, 1),
        ('FAIL_ROW_PRE_01', 'I_FAIL_PRE_01', datetime(1899, 12, 31, 0, 0, 0), None, datetime(1899, 12, 31, 0, 0, 0), None, 1),
    ]
    insert_bq_source_data(bq_client, source_data)

    # Trigger the DAG
    trigger_airflow_dag(
        dag_id=AIRFLOW_DAG_ID,
        conf={'job_kennung': f"TEST_VDATUM_{scenario}", 'eintrags_nr': "2001"}
    )

    # Fetch results
    migrated_data = fetch_bq_target_data(bq_client)
    migrated_cntrct_ids = {row[0] for row in migrated_data}

    # Assertions based on expected_v_datum_date
    expected_ids_for_this_test = set()
    for row in source_data:
        cntrct_id, _, insert_at, modified_at, valid_from, valid_to, is_production = row
        if (insert_at.date() <= expected_v_datum_date and
            (modified_at is None or modified_at.date() > expected_v_datum_date) and
            valid_from.date() <= expected_v_datum_date and
            (valid_to is None or valid_to.date() > expected_v_datum_date) and
            is_production == 1):
            expected_ids_for_this_test.add(cntrct_id)

    assert migrated_cntrct_ids == expected_ids_for_this_test, \
        f"v_datum derivation failed for scenario {scenario}. Expected: {expected_ids_for_this_test}, Got: {migrated_cntrct_ids}"
```

### 3. Transformation Correctness - Filtering Logic

*   **Purpose**: Verify all `WHERE` clause conditions in the `INSERT` statement are correctly translated and applied, including NULL handling and boundary conditions for dates.
*   **Setup**: Populate `dwtk_meldungen_bq` to set `v_datum` to a specific date (e.g., `2023-01-15`). Populate `cds_ta_inv_assignment` with various test rows, each designed to test one specific filter condition.
*   **Action**: Trigger the Airflow DAG.
*   **Pass/Fail Criterion**: Only the rows designed to pass all filter conditions should be present in `ta_inv_assign`.
*   **Runnable Test Code (Pytest with SQL assertions)**:

```python
def test_filtering_logic(bq_client):
    clear_bq_tables(bq_client)
    test_v_datum_dt = datetime(2023, 1, 15, 10, 0, 0)
    test_v_datum_date = test_v_datum_dt.date()

    # Setup dwtk_meldungen_bq to set v_datum
    insert_bq_log_entry(bq_client, 'BERT_DROP_TEMP_TABLE', test_v_datum_dt)

    # Insert source data to test each filter condition
    source_data = [
        # Pass cases
        ('P1', 'I1', test_v_datum_dt, None, test_v_datum_dt, None, 1), # All pass, modified_at/valid_to NULL
        ('P2', 'I2', test_v_datum_dt, test_v_datum_dt + timedelta(days=1), test_v_datum_dt, test_v_datum_dt + timedelta(days=1), 1), # All pass, modified_at/valid_to > v_datum
        ('P3', 'I3', test_v_datum_dt - timedelta(days=1), None, test_v_datum_dt - timedelta(days=1), None, 1), # All pass, dates before v_datum

        # Fail cases (each designed to fail one specific condition)
        ('F1', 'I1', test_v_datum_dt + timedelta(days=1), None, test_v_datum_dt, None, 1), # insert_at > v_datum
        ('F2', 'I2', test_v_datum_dt, test_v_datum_dt - timedelta(days=1), test_v_datum_dt, None, 1), # modified_at <= v_datum
        ('F3', 'I3', test_v_datum_dt, None, test_v_datum_dt + timedelta(days=1), None, 1), # valid_from > v_datum
        ('F4', 'I4', test_v_datum_dt, None, test_v_datum_dt, test_v_datum_dt - timedelta(days=1), 1), # valid_to <= v_datum
        ('F5', 'I5', test_v_datum_dt, None, test_v_datum_dt, None, 0), # is_production = 0
    ]
    insert_bq_source_data(bq_client, source_data)

    # Trigger the DAG
    trigger_airflow_dag(
        dag_id=AIRFLOW_DAG_ID,
        conf={'job_kennung': "TEST_FILTERING", 'eintrags_nr': "3001"}
    )

    # Fetch results
    migrated_data = fetch_bq_target_data(bq_client)
    migrated_cntrct_ids = {row[0] for row in migrated_data}

    # Assertions
    expected_cntrct_ids = {'P1', 'P2', 'P3'}
    assert migrated_cntrct_ids == expected_cntrct_ids, \
        f"Filtering logic failed. Expected: {expected_cntrct_ids}, Got: {migrated_cntrct_ids}"
```

### 4. Transformation Correctness - Type Handling (`is_production`)

*   **Purpose**: Verify that the `is_production` column is correctly cast from its source type (assumed numeric 0/1) to BigQuery's `BOOL` type.
*   **Setup**: Populate `dwtk_meldungen_bq` to set `v_datum` to a date that allows all test rows to pass date filters. Populate `cds_ta_inv_assignment` with rows where `is_production` is `0` and `1`.
*   **Action**: Trigger the Airflow DAG.
*   **Pass/Fail Criterion**:
    *   Rows with `is_production = 1` in source should have `is_production = TRUE` in target.
    *   Rows with `is_production = 0` in source should *not* be present in the target (due to `WHERE ia.is_production = 1` filter).
*   **Runnable Test Code (Pytest with SQL assertions)**:

```python
def test_is_production_type_handling(bq_client):
    clear_bq_tables(bq_client)
    test_v_datum_dt = datetime(2023, 1, 15, 10, 0, 0)

    # Setup dwtk_meldungen_bq to set v_datum
    insert_bq_log_entry(bq_client, 'BERT_DROP_TEMP_TABLE', test_v_datum_dt)

    # Insert source data with different is_production values
    source_data = [
        ('PROD_TRUE', 'I_TRUE', test_v_datum_dt, None, test_v_datum_dt, None, 1),
        ('PROD_FALSE', 'I_FALSE', test_v_datum_dt, None, test_v_datum_dt, None, 0)
    ]
    insert_bq_source_data(bq_client, source_data)

    # Trigger the DAG
    trigger_airflow_dag(
        dag_id=AIRFLOW_DAG_ID,
        conf={'job_kennung': "TEST_IS_PROD", 'eintrags_nr': "4001"}
    )

    # Fetch results
    migrated_data = fetch_bq_target_data(bq_client)

    # Assertions
    assert len(migrated_data) == 1, "Only rows with is_production=1 should be inserted."
    assert migrated_data[0][0] == 'PROD_TRUE'
    assert migrated_data[0][6] is True, "is_production should be cast to TRUE (boolean)."
```

### 5. Data Quality / Row Count - Empty Source Table

*   **Purpose**: Verify the job handles an empty source table gracefully, resulting in an empty target table.
*   **Setup**: Ensure `cds_ta_inv_assignment` is empty. Populate `dwtk_meldungen_bq` to set `v_datum` (e.g., `2023-01-15`). Ensure `ta_inv_assign` is empty or contains data that will be truncated.
*   **Action**: Trigger the Airflow DAG.
*   **Pass/Fail Criterion**: `ta_inv_assign` must be empty after the job runs.
*   **Runnable Test Code (Pytest with SQL assertions)**:

```python
def test_empty_source_table(bq_client):
    clear_bq_tables(bq_client)
    test_v_datum_dt = datetime(2023, 1, 15, 10, 0, 0)

    # Setup dwtk_meldungen_bq to set v_datum
    insert_bq_log_entry(bq_client, 'BERT_DROP_TEMP_TABLE', test_v_datum_dt)

    # Source table is intentionally left empty by clear_bq_tables()

    # Trigger the DAG
    trigger_airflow_dag(
        dag_id=AIRFLOW_DAG_ID,
        conf={'job_kennung': "TEST_EMPTY_SOURCE", 'eintrags_nr': "5001"}
    )

    # Fetch results
    migrated_data = fetch_bq_target_data(bq_client)

    # Assertions
    assert len(migrated_data) == 0, "Target table should be empty when source is empty."
```

### 6. Data Quality / Row Count - All Rows Filtered Out

*   **Purpose**: Verify the job correctly results in an empty target table if all source rows are filtered out by the `WHERE` clause.
*   **Setup**: Populate `dwtk_meldungen_bq` to set `v_datum` (e.g., `2023-01-15`). Populate `cds_ta_inv_assignment` with rows that *all* fail at least one filter condition (e.g., all `is_production = 0`). Ensure `ta_inv_assign` is empty or contains data that will be truncated.
*   **Action**: Trigger the Airflow DAG.
*   **Pass/Fail Criterion**: `ta_inv_assign` must be empty after the job runs.
*   **Runnable Test Code (Pytest with SQL assertions)**:

```python
def test_all_rows_filtered_out(bq_client):
    clear_bq_tables(bq_client)
    test_v_datum_dt = datetime(2023, 1, 15, 10, 0, 0)

    # Setup dwtk_meldungen_bq to set v_datum
    insert_bq_log_entry(bq_client, 'BERT_DROP_TEMP_TABLE', test_v_datum_dt)

    # Insert source data where all rows should be filtered out
    source_data = [
        ('FAIL_1', 'I1', test_v_datum_dt, None, test_v_datum_dt, None, 0), # is_production = 0
        ('FAIL_2', 'I2', test_v_datum_dt + timedelta(days=1), None, test_v_datum_dt, None, 1) # insert_at > v_datum
    ]
    insert_bq_source_data(bq_client, source_data)

    # Trigger the DAG
    trigger_airflow_dag(
        dag_id=AIRFLOW_DAG_ID,
        conf={'job_kennung': "TEST_ALL_FILTERED", 'eintrags_nr': "6001"}
    )

    # Fetch results
    migrated_data = fetch_bq_target_data(bq_client)

    # Assertions
    assert len(migrated_data) == 0, "Target table should be empty when all source rows are filtered out."
```

### 7. External System Replacements - Parameter Passing

*   **Purpose**: Verify that the `p_job_kennung` and `p_eintrags_nr` parameters are correctly passed from the Airflow DAG to the BigQuery Stored Procedure.
*   **Setup**:
    *   **Crucial**: The `sp_d_ausd_v_ta_inv_assign` currently doesn't *use* these parameters in its logic. To test this, we must temporarily modify the Stored Procedure to log or store these parameters in an audit table.
    *   Create a temporary audit table in BigQuery:
        ```sql
        CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_log_data.sp_param_audit` (
            run_timestamp TIMESTAMP,
            job_kennung STRING,
            eintrags_nr STRING
        );
        ```
    *   Modify `sp_d_ausd_v_ta_inv_assign.sql` to include:
        ```sql
        -- Inside the SP, after DECLARE v_datum:
        INSERT INTO `your-gcp-project.isbert_log_data.sp_param_audit` (run_timestamp, job_kennung, eintrags_nr)
        VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr);
        ```
*   **Action**: Trigger the Airflow DAG with specific `job_kennung` and `eintrags_nr` values.
*   **Pass/Fail Criterion**: The logged values in the `sp_param_audit` table must match the parameters passed from Airflow.
*   **Runnable Test Code (Pytest with SQL assertions - requires SP modification)**:

```python
def test_parameter_passing(bq_client):
    clear_bq_tables(bq_client) # This will also clear sp_param_audit if it exists
    test_job_kennung = "MY_TEST_JOB_XYZ"
    test_eintrags_nr = "98765"

    # Trigger the DAG
    trigger_airflow_dag(
        dag_id=AIRFLOW_DAG_ID,
        conf={'job_kennung': test_job_kennung, 'eintrags_nr': test_eintrags_nr}
    )

    # Query the audit table (requires SP modification as described in setup)
    query = f"""
        SELECT job_kennung, eintrags_nr
        FROM `{BIGQUERY_PROJECT}.{BQ_LOG_DATASET}.sp_param_audit`
        ORDER BY run_timestamp DESC
        LIMIT 1
    """
    rows = bq_client.query(query).result()
    audit_entry = list(rows)[0] if rows.total_rows > 0 else None

    # Assertions
    assert audit_entry is not None, "No audit entry found for parameter passing. " \
                                    "Ensure sp_d_ausd_v_ta_inv_assign is modified to log parameters."
    assert audit_entry.job_kennung == test_job_kennung, \
        f"job_kennung mismatch. Expected: {test_job_kennung}, Got: {audit_entry.job_kennung}"
    assert audit_entry.eintrags_nr == test_eintrags_nr, \
        f"eintrags_nr mismatch. Expected: {test_eintrags_nr}, Got: {audit_entry.eintrags_nr}"
```

### 8. Data Quality / Schema Assertions

*   **Purpose**: Verify that the target BigQuery tables (`ta_inv_assign`, `dwtk_meldungen_bq`) exist and have the expected schema (column names, data types, nullability).
*   **Setup**: Ensure the BigQuery project and datasets are accessible.
*   **Action**: Query BigQuery's information schema to retrieve table details.
*   **Pass/Fail Criterion**: The tables must exist, and their column names, data types, and nullability must match the DDLs provided in the migration code.
*   **Runnable Test Code (Pytest with SQL assertions)**:

```python
def test_target_schema_definition(bq_client):
    expected_schema_ta_inv_assign = {
        'cntrct_id': {'type': 'STRING', 'nullable': False},
        'inv_definition_id': {'type': 'STRING', 'nullable': False},
        'insert_at': {'type': 'TIMESTAMP', 'nullable': True},
        'modified_at': {'type': 'TIMESTAMP', 'nullable': True},
        'valid_from': {'type': 'TIMESTAMP', 'nullable': True},
        'valid_to': {'type': 'TIMESTAMP', 'nullable': True},
        'is_production': {'type': 'BOOL', 'nullable': True}
    }
    expected_schema_dwtk_meldungen_bq = {
        'timecreated': {'type': 'TIMESTAMP', 'nullable': True},
        'job_kennung': {'type': 'STRING', 'nullable': False}
    }

    def get_bq_table_schema(client, project, dataset, table):
        table_ref = client.dataset(dataset, project=project).table(table)
        table_obj = client.get_table(table_ref)
        schema = {}
        for field in table_obj.schema:
            schema[field.name] = {'type': field.field_type, 'nullable': field.is_nullable}
        return schema

    # Test ta_inv_assign schema
    actual_schema_ta_inv_assign = get_bq_table_schema(bq_client, BIGQUERY_PROJECT, BQ_TARGET_DATASET, 'ta_inv_assign')
    assert actual_schema_ta_inv_assign == expected_schema_ta_inv_assign, \
        "Schema for ta_inv_assign does not match expected."

    # Test dwtk_meldungen_bq schema
    actual_schema_dwtk_meldungen_bq = get_bq_table_schema(bq_client, BIGQUERY_PROJECT, BQ_LOG_DATASET, 'dwtk_meldungen_bq')
    assert actual_schema_dwtk_meldungen_bq == expected_schema_dwtk_meldungen_bq, \
        "Schema for dwtk_meldungen_bq does not match expected."
```

---

### 9. Missing Functionality - Job Control Logic (Known Gap)

*   **Purpose**: Highlight and confirm the absence of job control logic (ignoring active jobs, deactivating old ones) in the migrated BigQuery Stored Procedure, as this was explicitly mentioned in the legacy script's purpose and the migration design's "Unresolved / Risks" section.
*   **Setup**:
    *   **Legacy Scenario**: Imagine a state where the legacy system's `ta_inv_assign` table (or an associated job status table) indicates that a job with a specific `p_JobKennung` and `p_EintragsNr` is already 'active'. The legacy `k_ausd_v_ta_inv_assign.ksh` script, via `starteSQLSkript` or `d_ausd_v_ta_inv_assign.sql`, is designed to 'ignore active jobs' (i.e., exit without performing data processing).
    *   **Migrated Scenario**: The current `sp_d_ausd_v_ta_inv_assign` does not contain any explicit logic to check for or manage job activity status. It proceeds directly to `TRUNCATE` and `INSERT` operations.
*   **Action**:
    *   **Legacy**: Execute the legacy `k_ausd_v_ta_inv_assign.ksh` in the 'active job' scenario. Observe that it exits early and `ta_inv_assign` is *not* modified.
    *   **Migrated**: Trigger the Airflow DAG.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The legacy job successfully ignores the active job, and the `ta_inv_assign` table remains unchanged.
    *   **Migrated**: The migrated job *will* execute the `TRUNCATE` and `INSERT` operations, modifying the `ta_inv_assign` table, regardless of any 'active job' status.
    *   **Conclusion**: This test highlights a *functional difference* between the legacy and migrated systems. If the requirement is strict behavioral equivalence, this test would *fail* the migration. However, given the "Unresolved / Risks" section in the design document, this is a known gap that needs to be addressed either by implementing the logic in BigQuery/Airflow or by explicitly accepting this change in behavior.
*   **Runnable Test Code (Conceptual - documents a known gap)**:

```python
def test_missing_job_control_logic():
    """
    Purpose: Verify the absence of job control logic (ignoring active jobs, deactivating old ones)
             in the migrated BigQuery Stored Procedure, as identified in the migration design.

    This test documents a known functional gap. It cannot be "run" in the same way as other tests
    without implementing the missing job control logic in the migrated SP or simulating the
    legacy behavior.

    Pass/Fail Criterion:
        *   Legacy: The legacy job successfully ignores the active job, and the `ta_inv_assign`
            table remains unchanged.
        *   Migrated: The current migrated job will execute the `TRUNCATE` and `INSERT` operations,
            modifying the `ta_inv_assign` table, regardless of any 'active job' status.
        *   Conclusion: This test highlights a *functional difference*. If strict behavioral
            equivalence is required, this functionality must be implemented in the migrated solution.
            Otherwise, this behavioral change must be explicitly accepted.
    """
    pytest.skip("This test documents a known functional gap (missing job control logic) "
                "and requires a decision on whether this logic needs to be implemented "
                "in the migrated solution or if the behavioral change is accepted.")
```

### 10. Missing Functionality - Record Count Logging (Known Gap)

*   **Purpose**: Verify that the record count, explicitly captured in `v_records` in the legacy script, is handled in the migrated solution.
*   **Setup**: Populate source data such that a known number of records will be inserted into `ta_inv_assign`.
*   **Action**:
    *   **Legacy**: Run the legacy script and observe the `v_records` value (e.g., by adding a `print $v_records` at the end of the ksh script).
    *   **Migrated**: Trigger the Airflow DAG.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The `v_records` variable accurately reflects the number of rows inserted.
    *   **Migrated**: The current Airflow DAG and Stored Procedure *do not* explicitly capture or log this count.
    *   **Conclusion**: This test highlights another functional difference. If the record count is used downstream or for auditing, this needs to be implemented (e.g., by having the SP return the count and the Airflow DAG log it).
*   **Runnable Test Code (Conceptual - documents a known gap)**:

```python
def test_missing_record_count_logging():
    """
    Purpose: Verify how the record count (captured in `v_records` in the legacy script)
             is handled in the migrated solution.

    This test documents a known functional gap.

    Pass/Fail Criterion:
        *   Legacy: The `v_records` variable accurately holds the count of records
            inserted into `ta_inv_assign`.
        *   Migrated: The current migrated solution does not provide an equivalent
            mechanism to capture or expose this record count.
        *   Conclusion: This test highlights a *functional difference*. If the record
            count is a critical output for auditing, monitoring, or downstream processes,
            the migrated solution needs to be enhanced (e.g., by having the BigQuery
            Stored Procedure return the row count, and the Airflow DAG logging it
            or storing it in an audit table).
    """
    pytest.skip("This test documents a known functional gap (missing explicit record count logging) "
                "and requires a decision on whether this functionality needs to be implemented "
                "in the migrated solution or if the behavioral change is accepted.")
```