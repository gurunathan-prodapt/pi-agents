As a senior data-migration QA engineer, I've analyzed the provided KornShell script, its dependencies, and the proposed BigQuery/Airflow migration design. The tests below are designed to ensure behavioral equivalence, data integrity, and correct functionality of the migrated solution on Google Cloud Platform.

The tests are structured to cover output parity, transformation correctness, external system replacements, and data quality assertions. Each test case includes its purpose, setup, action, and clear pass/fail criteria. Where applicable, runnable SQL assertions are provided.

**Assumptions for Test Execution:**

*   A GCP project and BigQuery dataset (`project.dataset`) are configured for the target tables (`sof$ta_bpr_basis_his`, `job_log`).
*   A BigQuery dataset (`isbert_schema`) is configured for the source tables (`cds$ta_cntrct`, `pds$ta_bpri_com`).
*   The BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_basis_his` has been deployed.
*   The Airflow DAG `k_ausd_bp_ta_bpr_basis_his_dag` has been deployed to a Cloud Composer environment.
*   `pytest` is used for test orchestration, and `google-cloud-bigquery` client for BigQuery interactions.
*   The `conftest.py` and helper functions (e.g., `populate_source_data`, `call_bigquery_stored_procedure`, `trigger_airflow_dag`) are available to manage BigQuery setup and DAG execution. (Conceptual implementations are provided for context).

---

### Conceptual `conftest.py` and Helper Functions

```python
# conftest.py (Conceptual - for BigQuery client and cleanup)
import pytest
from google.cloud import bigquery
import os
import time

# --- Configuration ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id") # Replace with your project ID
BQ_TARGET_DATASET_ID = os.getenv("BQ_TARGET_DATASET_ID", "dw_migration_test") # Target dataset for sof$ta_bpr_basis_his and job_log
BQ_SOURCE_DATASET_ID = os.getenv("BQ_SOURCE_DATASET_ID", "isbert_source_data") # Source dataset for cds$ta_cntrct and pds$ta_bpri_com
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1") # BigQuery dataset location

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    return client

@pytest.fixture(scope="session")
def bq_target_dataset_ref(bq_client):
    """Provides the target BigQuery dataset reference."""
    dataset_ref = bq_client.dataset(BQ_TARGET_DATASET_ID)
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    return dataset_ref

@pytest.fixture(scope="session")
def bq_source_dataset_ref(bq_client):
    """Provides the source BigQuery dataset reference."""
    dataset_ref = bq_client.dataset(BQ_SOURCE_DATASET_ID)
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    return dataset_ref

@pytest.fixture(autouse=True)
def cleanup_target_tables(bq_client, bq_target_dataset_ref):
    """Fixture to clean up target tables before each test."""
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    job_log_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.job_log"

    # Ensure tables exist with correct schema (DDL from migration design)
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{target_table_id}` (
            CNTRCT_ID STRING, BPR_ID INT64, BPRI_COM_ID INT64, ICCID STRING,
            IMSI_MCC STRING, IMSI_MNC STRING, IMSI_HLR STRING, IMSI_SI STRING,
            CNTRCT_ID_REF STRING, VALID_FROM DATE, VALID_TO DATE, MODIFIED_AT TIMESTAMP,
            INSERT_AT TIMESTAMP, SLAVE_NUMBER INT64, E_ID STRING
        );
    """).result()
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{job_log_table_id}` (
            job_name STRING, status STRING, error_nr INT64, error_arg STRING,
            stichtag DATE, records_processed INT64, created_at TIMESTAMP
        );
    """).result()

    # Truncate tables for a clean slate before each test
    bq_client.query(f"TRUNCATE TABLE `{target_table_id}`").result()
    bq_client.query(f"TRUNCATE TABLE `{job_log_table_id}`").result()
    yield # Yield control to the test function
    # No post-test cleanup needed if autouse=True and cleanup is pre-test.

# --- Helper Functions ---

def populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data):
    """Populates the source tables with provided data."""
    cds_table_id = f"{bq_client.project}.{bq_source_dataset_ref.dataset_id}.cds$ta_cntrct"
    pds_table_id = f"{bq_client.project}.{bq_source_dataset_ref.dataset_id}.pds$ta_bpri_com"

    # Create tables if they don't exist (with appropriate schema)
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{cds_table_id}` (
            cntrct_id STRING, cntrct_st INT64, redundant_owner_id INT64,
            insert_at TIMESTAMP, modified_at TIMESTAMP, valid_from DATE, valid_to DATE,
            is_production INT64, cntrct_ty INT64, cntrct_parent STRING
        );
    """).result()
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{pds_table_id}` (
            cntrct_id STRING, bpr_id INT64, bpri_com_id INT64,
            iccid_mi STRING, iccid_ii STRING, iccid_iai STRING, iccid_nr STRING, iccid_cd STRING,
            imsi_mcc STRING, imsi_mnc STRING, imsi_hlr STRING, imsi_si STRING,
            cntrct_id_ref STRING, valid_from DATE, valid_to DATE, modified_at TIMESTAMP,
            insert_at TIMESTAMP, slave_number INT64, e_id STRING, is_production INT64
        );
    """).result()

    # Truncate existing data
    bq_client.query(f"TRUNCATE TABLE `{cds_table_id}`").result()
    bq_client.query(f"TRUNCATE TABLE `{pds_table_id}`").result()

    # Insert new data
    if cds_data:
        rows_to_insert = [tuple(row.values()) for row in cds_data]
        errors = bq_client.insert_rows(bq_client.get_table(cds_table_id), rows_to_insert)
        if errors:
            raise Exception(f"Error inserting into {cds_table_id}: {errors}")
    if pds_data:
        rows_to_insert = [tuple(row.values()) for row in pds_data]
        errors = bq_client.insert_rows(bq_client.get_table(pds_table_id), rows_to_insert)
        if errors:
            raise Exception(f"Error inserting into {pds_table_id}: {errors}")

def call_bigquery_stored_procedure(bq_client, bq_target_dataset_ref, sp_name, params):
    """Calls the BigQuery stored procedure directly for testing."""
    param_strings = []
    for k, v in params.items():
        if isinstance(v, str):
            param_strings.append(f"{k} => '{v}'")
        elif isinstance(v, int):
            param_strings.append(f"{k} => {v}")
        elif isinstance(v, bool):
            param_strings.append(f"{k} => {str(v).upper()}")
        elif isinstance(v, dict) and v.get('type') == 'DATE':
            param_strings.append(f"{k} => PARSE_DATE('%Y-%m-%d', '{v['value']}')")
        elif v is None:
            param_strings.append(f"{k} => NULL")
        else:
            param_strings.append(f"{k} => {v}") # Fallback for other types

    query = f"""
    CALL `{bq_client.project}.{bq_target_dataset_ref.dataset_id}.{sp_name}`({', '.join(param_strings)});
    """
    print(f"Executing BigQuery SP: {query}")
    try:
        bq_client.query(query).result()
        return True, None
    except Exception as e:
        print(f"BigQuery SP call failed: {e}")
        return False, str(e)

def trigger_airflow_dag(dag_id, params):
    """
    Simulates triggering an Airflow DAG. In a real test environment, this would
    involve using Airflow's REST API or CLI to trigger the DAG and then
    polling for its completion status. For this exercise, we'll
    assume a successful trigger and focus on BigQuery state.
    """
    print(f"Simulating Airflow DAG trigger for {dag_id} with params: {params}")
    # In a real scenario, you'd use Airflow's API or CLI:
    # from airflow.api.client.local_client import Client
    # client = Client(None, None)
    # client.trigger_dag(dag_id=dag_id, conf=params)
    # Then poll for DAG run status.
    # For this exercise, we'll directly call the SP as if Airflow did it.
    # This function is mostly a placeholder for the "Action" step in tests.
    pass

```

---

### Test Case 1: Happy Path - Full Data Load & Output Parity

*   **Purpose:** Verify the end-to-end process with valid inputs, ensuring the migrated job produces the exact same output data and record count as the legacy job would for a given set of source data. This is the primary output parity test.
*   **Setup:**
    1.  Populate `isbert_schema.cds$ta_cntrct` and `isbert_schema.pds$ta_bpri_com` with a diverse set of sample data, including records that should pass and fail the various filters and join conditions.
    2.  Define the expected output for `project.dataset.sof$ta_bpr_basis_his` based on the sample data and the transformation logic.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his_dag` with valid parameters:
        *   `p_job_kennung`: "TEST_HAPPY_PATH"
        *   `p_eintrags_nr`: "100"
        *   `p_stichtag`: "2023-03-15" (as `YYYY-MM-DD`)
        *   `p_wiederanlauf_wert`: "0"
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run completes successfully.
    2.  The `project.dataset.sof$ta_bpr_basis_his` table contains exactly the expected number of rows.
    3.  The data in `project.dataset.sof$ta_bpr_basis_his` exactly matches the predefined expected output, considering all transformations (joins, filters, ICCID concatenation).
    4.  A 'SUCCESS' entry is recorded in `project.dataset.job_log` for `job_name = 'TEST_HAPPY_PATH'` with `records_processed` matching the count in `sof$ta_bpr_basis_his`.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_happy_path_full_data_load(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: Sample source data
    cds_data = [
        {'cntrct_id': 'C1', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C2', 'cntrct_st': 6, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 5, 10, 0, 0), 'modified_at': datetime(2023, 3, 1, 10, 0, 0), 'valid_from': date(2023, 1, 5), 'valid_to': date(2023, 12, 31), 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C3', 'cntrct_st': 5, 'redundant_owner_id': 2, 'insert_at': datetime(2023, 1, 10, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 1, 10), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Fails redundant_owner_id
        {'cntrct_id': 'C4', 'cntrct_st': 7, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 15, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 1, 15), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Fails cntrct_st
        {'cntrct_id': 'C5', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 3, 20, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 3, 20), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Fails insert_at > p_stichtag
        {'cntrct_id': 'C6', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'modified_at': datetime(2023, 3, 16, 10, 0, 0), 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Fails modified_at > p_stichtag
        {'cntrct_id': 'C7', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 3, 20), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Fails valid_from > p_stichtag
        {'cntrct_id': 'C8', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': date(2023, 3, 10), 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Fails valid_to <= p_stichtag
        {'cntrct_id': 'C9', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 0, 'cntrct_ty': 10, 'cntrct_parent': None}, # Fails is_production
        {'cntrct_id': 'C10', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 1, 'cntrct_parent': None}, # Fails cntrct_ty NOT IN (1,2,5) AND cntrct_parent IS NULL
        {'cntrct_id': 'C11', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 1, 'cntrct_parent': 'P1'}, # Passes cntrct_ty (due to cntrct_parent)
    ]
    pds_data = [
        {'cntrct_id': 'C1', 'bpr_id': 31, 'bpri_com_id': 101, 'iccid_mi': '123', 'iccid_ii': '456', 'iccid_iai': '789', 'iccid_nr': '012', 'iccid_cd': '3', 'imsi_mcc': '262', 'imsi_mnc': '01', 'imsi_hlr': 'HLR1', 'imsi_si': 'SI1', 'cntrct_id_ref': 'REF1', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'slave_number': 1, 'e_id': 'E1', 'is_production': 1},
        {'cntrct_id': 'C2', 'bpr_id': 2759, 'bpri_com_id': 102, 'iccid_mi': '456', 'iccid_ii': '789', 'iccid_iai': '012', 'iccid_nr': '345', 'iccid_cd': '6', 'imsi_mcc': '262', 'imsi_mnc': '02', 'imsi_hlr': 'HLR2', 'imsi_si': 'SI2', 'cntrct_id_ref': 'REF2', 'valid_from': date(2023, 1, 5), 'valid_to': date(2023, 12, 31), 'modified_at': datetime(2023, 3, 1, 10, 0, 0), 'insert_at': datetime(2023, 1, 5, 10, 0, 0), 'slave_number': 2, 'e_id': 'E2', 'is_production': 1},
        {'cntrct_id': 'C11', 'bpr_id': 3848, 'bpri_com_id': 103, 'iccid_mi': '789', 'iccid_ii': '012', 'iccid_iai': '345', 'iccid_nr': '678', 'iccid_cd': '9', 'imsi_mcc': '262', 'imsi_mnc': '03', 'imsi_hlr': 'HLR3', 'imsi_si': 'SI3', 'cntrct_id_ref': 'REF3', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'slave_number': 3, 'e_id': 'E3', 'is_production': 1},
        {'cntrct_id': 'C1', 'bpr_id': 9999, 'bpri_com_id': 104, 'iccid_mi': '999', 'iccid_ii': '999', 'iccid_iai': '999', 'iccid_nr': '999', 'iccid_cd': '9', 'imsi_mcc': '262', 'imsi_mnc': '04', 'imsi_hlr': 'HLR4', 'imsi_si': 'SI4', 'cntrct_id_ref': 'REF4', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1, 10, 0, 0), 'slave_number': 4, 'e_id': 'E4', 'is_production': 1}, # Fails bpr_id
        {'cntrct_id': 'C1', 'bpr_id': 31, 'bpri_com_id': 105, 'iccid_mi': '111', 'iccid_ii': '222', 'iccid_iai': '333', 'iccid_nr': '444', 'iccid_cd': '5', 'imsi_mcc': '262', 'imsi_mnc': '05', 'imsi_hlr': 'HLR5', 'imsi_si': 'SI5', 'cntrct_id_ref': 'REF5', 'valid_from': date(2023, 3, 20), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 3, 20, 10, 0, 0), 'slave_number': 5, 'e_id': 'E5', 'is_production': 1}, # Fails pds_valid_from > p_stichtag
    ]
    populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data)

    # Expected output based on the filters and join
    expected_output = [
        {'CNTRCT_ID': 'C1', 'BPR_ID': 31, 'BPRI_COM_ID': 101, 'ICCID': '123-456-789-012-3', 'IMSI_MCC': '262', 'IMSI_MNC': '01', 'IMSI_HLR': 'HLR1', 'IMSI_SI': 'SI1', 'CNTRCT_ID_REF': 'REF1', 'VALID_FROM': date(2023, 1, 1), 'VALID_TO': None, 'MODIFIED_AT': None, 'INSERT_AT': datetime(2023, 1, 1, 10, 0, 0), 'SLAVE_NUMBER': 1, 'E_ID': 'E1'},
        {'CNTRCT_ID': 'C2', 'BPR_ID': 2759, 'BPRI_COM_ID': 102, 'ICCID': '456-789-012-345-6', 'IMSI_MCC': '262', 'IMSI_MNC': '02', 'IMSI_HLR': 'HLR2', 'IMSI_SI': 'SI2', 'CNTRCT_ID_REF': 'REF2', 'VALID_FROM': date(2023, 1, 5), 'VALID_TO': date(2023, 12, 31), 'MODIFIED_AT': datetime(2023, 3, 1, 10, 0, 0), 'INSERT_AT': datetime(2023, 1, 5, 10, 0, 0), 'SLAVE_NUMBER': 2, 'E_ID': 'E2'},
        {'CNTRCT_ID': 'C11', 'BPR_ID': 3848, 'BPRI_COM_ID': 103, 'ICCID': '789-012-345-678-9', 'IMSI_MCC': '262', 'IMSI_MNC': '03', 'IMSI_HLR': 'HLR3', 'IMSI_SI': 'SI3', 'CNTRCT_ID_REF': 'REF3', 'VALID_FROM': date(2023, 1, 1), 'VALID_TO': None, 'MODIFIED_AT': None, 'INSERT_AT': datetime(2023, 1, 1, 10, 0, 0), 'SLAVE_NUMBER': 3, 'E_ID': 'E3'},
    ]
    expected_record_count = len(expected_output)

    # Action: Trigger the DAG (or call SP directly for testing core logic)
    dag_params = {
        "p_job_kennung": "TEST_HAPPY_PATH",
        "p_eintrags_nr": "100",
        "p_stichtag": {'type': 'DATE', 'value': "2023-03-15"},
        "p_wiederanlauf_wert": "0",
    }
    # In a real test, you'd trigger Airflow DAG. For this exercise, direct SP call.
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert success, f"Stored procedure failed: {error_msg}"

    # Pass/Fail Criterion 1, 2, 3: Check target table content
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    query_result = bq_client.query(f"SELECT * FROM `{target_table_id}` ORDER BY CNTRCT_ID").result()
    actual_output = [dict(row) for row in query_result]

    assert len(actual_output) == expected_record_count, \
        f"Expected {expected_record_count} records, got {len(actual_output)}"
    
    # Convert timestamps in actual_output to match expected_output's datetime objects
    for row in actual_output:
        if 'INSERT_AT' in row and row['INSERT_AT']:
            row['INSERT_AT'] = row['INSERT_AT'].replace(tzinfo=None)
        if 'MODIFIED_AT' in row and row['MODIFIED_AT']:
            row['MODIFIED_AT'] = row['MODIFIED_AT'].replace(tzinfo=None)

    assert actual_output == sorted(expected_output, key=lambda x: x['CNTRCT_ID']), \
        "Actual output does not match expected output"

    # Pass/Fail Criterion 4: Check job_log entry
    job_log_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.job_log"
    log_query = f"""
        SELECT job_name, status, error_nr, error_arg, stichtag, records_processed
        FROM `{job_log_table_id}`
        WHERE job_name = 'TEST_HAPPY_PATH'
        ORDER BY created_at DESC LIMIT 1
    """
    log_result = list(bq_client.query(log_query).result())
    assert len(log_result) == 1, "Expected one log entry for TEST_HAPPY_PATH"
    log_entry = log_result[0]
    assert log_entry['status'] == 'SUCCESS'
    assert log_entry['error_nr'] == 0
    assert log_entry['error_arg'] is None
    assert log_entry['stichtag'] == date(2023, 3, 15)
    assert log_entry['records_processed'] == expected_record_count
```

---

### Test Case 2: Parameter Validation - Missing Mandatory Parameters

*   **Purpose:** Verify that the migrated system correctly identifies and handles missing mandatory parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`), similar to the legacy `pruefeParameterGesetzt` function.
*   **Setup:**
    1.  Ensure source tables are empty or contain irrelevant data (this test focuses on parameter validation, not data processing).
*   **Action:**
    1.  Attempt to trigger the Airflow DAG (or call the BigQuery Stored Procedure directly) with `p_job_kennung` set to `NULL` (or omitted if Airflow allows).
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run fails (or the direct SP call raises an error).
    2.  A 'FAILED' entry is recorded in `project.dataset.job_log` with an appropriate `error_nr` (e.g., BigQuery's default error code) and `error_arg` indicating a missing or invalid parameter.

```python
import pytest
from google.cloud import bigquery
from datetime import date

def test_missing_mandatory_parameter(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: No specific source data needed, but ensure tables exist.
    populate_source_data(bq_client, bq_source_dataset_ref, [], [])

    # Action: Call SP with a missing mandatory parameter (e.g., p_job_kennung as NULL)
    dag_params = {
        "p_job_kennung": None, # Simulating a missing mandatory parameter
        "p_eintrags_nr": "100",
        "p_stichtag": {'type': 'DATE', 'value': "2023-03-15"},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert not success, "Stored procedure was expected to fail due to missing parameter"

    # Pass/Fail Criterion: Check job_log entry for failure
    job_log_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.job_log"
    log_query = f"""
        SELECT job_name, status, error_nr, error_arg, stichtag, records_processed
        FROM `{job_log_table_id}`
        WHERE job_name IS NULL -- Since p_job_kennung was NULL
        ORDER BY created_at DESC LIMIT 1
    """
    log_result = list(bq_client.query(log_query).result())
    assert len(log_result) == 1, "Expected one log entry for the failed run"
    log_entry = log_result[0]
    assert log_entry['status'] == 'FAILED'
    assert log_entry['error_nr'] is not None and log_entry['error_nr'] != 0 # BigQuery error code
    assert "NULL value for argument p_job_kennung" in log_entry['error_arg'] or \
           "Cannot cast NULL to type STRING" in log_entry['error_arg'] # Specific error message might vary
    assert log_entry['stichtag'] == date(2023, 3, 15)
    assert log_entry['records_processed'] is None
```

---

### Test Case 3: Parameter Validation - Invalid `Stichtag` Format

*   **Purpose:** Verify that the migrated system correctly handles an invalid `Stichtag` format, which would have been caught by `DWDate_Datum_Check` in the legacy script. The Airflow DAG's `PARSE_DATE` function should catch this.
*   **Setup:**
    1.  Ensure source tables are empty or contain irrelevant data.
*   **Action:**
    1.  Trigger the Airflow DAG with `p_stichtag` set to an invalid date format (e.g., "2023/03/15" or "15-03-2023").
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run fails (specifically the `BigQueryExecuteQueryOperator` task).
    2.  A 'FAILED' entry is recorded in `project.dataset.job_log` with an appropriate `error_nr` and `error_arg` indicating a date parsing error.

```python
import pytest
from google.cloud import bigquery
from datetime import date

def test_invalid_stichtag_format(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: No specific source data needed.
    populate_source_data(bq_client, bq_source_dataset_ref, [], [])

    # Action: Call SP with an invalid date format for p_stichtag
    dag_params = {
        "p_job_kennung": "TEST_INVALID_DATE",
        "p_eintrags_nr": "101",
        "p_stichtag": {'type': 'STRING', 'value': "15/03/2023"}, # Invalid format for PARSE_DATE('%Y-%m-%d', ...)
        "p_wiederanlauf_wert": "0",
    }
    # Note: The DAG's BigQueryExecuteQueryOperator uses PARSE_DATE('%Y-%m-%d', '{{ params.p_stichtag }}').
    # If we pass a string directly to the SP, it expects a DATE type.
    # To simulate the DAG's behavior, we'd need to pass the string and let the SP's internal PARSE_DATE fail.
    # However, the SP itself expects a DATE type. The error would occur in Airflow's BQ operator.
    # For direct SP call, we'll simulate the error by passing a string where a DATE is expected.
    # Or, more accurately, if the SP had internal PARSE_DATE, we'd test that.
    # Given the SP signature, the Airflow operator is responsible for PARSE_DATE.
    # So, this test should ideally trigger the Airflow DAG with an invalid string parameter.
    # For this exercise, we'll simulate the SP failing if it received an unparseable date string.
    # Let's adjust the `call_bigquery_stored_procedure` to reflect the DAG's `PARSE_DATE` logic.

    # Re-simulating the Airflow operator's call:
    # The Airflow DAG's `BigQueryExecuteQueryOperator` would construct:
    # CALL project.dataset.r_ausd_bp_ta_bpr_basis_his(..., p_stichtag => PARSE_DATE('%Y-%m-%d', '15/03/2023'), ...)
    # This `PARSE_DATE` call would fail *before* the SP even starts.
    # So, we need to test that the `PARSE_DATE` in the Airflow operator's SQL fails.
    # This requires a different helper function or direct Airflow API call.

    # For the purpose of this exercise, we'll assume the SP itself would handle this if it were passed a string.
    # However, the SP is typed to DATE. So the error would be in the Airflow operator.
    # Let's simulate the error by passing a string that PARSE_DATE would fail on.
    # This test is more about the Airflow operator's parameter handling.

    # Simulating the Airflow operator's SQL directly:
    sp_call_sql = f"""
    CALL `{bq_client.project}.{bq_target_dataset_ref.dataset_id}.r_ausd_bp_ta_bpr_basis_his`(
      p_job_kennung => 'TEST_INVALID_DATE',
      p_eintrags_nr => '101',
      p_stichtag => PARSE_DATE('%Y-%m-%d', '15/03/2023'), -- This PARSE_DATE will fail
      p_wiederanlauf_wert => '0'
    );
    """
    print(f"Executing BigQuery SP call with invalid date format: {sp_call_sql}")
    try:
        bq_client.query(sp_call_sql).result()
        success = True
        error_msg = None
    except Exception as e:
        success = False
        error_msg = str(e)
    
    assert not success, "BigQuery query was expected to fail due to invalid date format"
    assert "Failed to parse input string" in error_msg or "Invalid date" in error_msg

    # Pass/Fail Criterion: Check job_log entry (if the error is caught by SP's EXCEPTION block)
    # In this specific case, the error happens *before* the SP execution, so the SP's EXCEPTION block
    # might not be hit. Airflow's BigQueryExecuteQueryOperator would catch this.
    # If the SP were designed to take a STRING and PARSE_DATE internally, then the log would be populated.
    # Given the current SP design, the Airflow operator would fail and log to Airflow logs.
    # For the sake of demonstrating the logging aspect, let's assume the SP was designed to take STRING and PARSE_DATE.
    # If the SP signature was `p_stichtag STRING`, then the following log check would be valid.
    # As it stands, the SP expects DATE, so the error is upstream.

    # If the SP were: CREATE OR REPLACE PROCEDURE ... (p_stichtag STRING) BEGIN DECLARE v_stichtag DATE; SET v_stichtag = PARSE_DATE('%Y-%m-%d', p_stichtag); ...
    # Then the log check would be:
    job_log_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.job_log"
    log_query = f"""
        SELECT job_name, status, error_nr, error_arg, stichtag, records_processed
        FROM `{job_log_table_id}`
        WHERE job_name = 'TEST_INVALID_DATE'
        ORDER BY created_at DESC LIMIT 1
    """
    log_result = list(bq_client.query(log_query).result())
    # This assertion would likely fail with the current SP design, as the error is upstream.
    # It highlights a behavioral difference: legacy script validates date format *before* SQL.
    # New system validates date format *during* Airflow's BQ operator call.
    # The current SP's EXCEPTION block would not catch this.
    # For a robust migration, the Airflow DAG should have a task to validate date format *before* calling BQ SP.
    # Or the SP should accept STRING and validate internally.
    # Let's assume for this test that the Airflow operator's failure is the expected outcome.
    assert len(log_result) == 0, "No log entry expected in job_log table as error occurs upstream of SP"
```

---

### Test Case 4: Filter Logic - `cds$ta_cntrct` Status Filter (`cntrct_st`)

*   **Purpose:** Verify that only records from `cds$ta_cntrct` with `cntrct_st` values of 5 or 6 are included in the final output.
*   **Setup:**
    1.  Populate `cds$ta_cntrct` with records having `cntrct_st` values: 5, 6, and others (e.g., 1, 7).
    2.  Populate `pds$ta_bpri_com` with matching `cntrct_id`s for all `cds$ta_cntrct` records, ensuring other filters would pass.
*   **Action:**
    1.  Trigger the Airflow DAG with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof$ta_bpr_basis_his` table contains only records where the original `cds$ta_cntrct.cntrct_st` was 5 or 6.
    2.  The count of records matches the expected count based on this filter.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_filter_cntrct_status(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: Source data with various cntrct_st values
    stichtag_val = date(2023, 3, 15)
    cds_data = [
        {'cntrct_id': 'C_ST_5', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C_ST_6', 'cntrct_st': 6, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C_ST_1', 'cntrct_st': 1, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Should be filtered out
        {'cntrct_id': 'C_ST_7', 'cntrct_st': 7, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Should be filtered out
    ]
    pds_data = [
        {'cntrct_id': 'C_ST_5', 'bpr_id': 31, 'bpri_com_id': 1, 'iccid_mi': '1', 'iccid_ii': '1', 'iccid_iai': '1', 'iccid_nr': '1', 'iccid_cd': '1', 'imsi_mcc': '1', 'imsi_mnc': '1', 'imsi_hlr': '1', 'imsi_si': '1', 'cntrct_id_ref': '1', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 1, 'e_id': '1', 'is_production': 1},
        {'cntrct_id': 'C_ST_6', 'bpr_id': 31, 'bpri_com_id': 2, 'iccid_mi': '2', 'iccid_ii': '2', 'iccid_iai': '2', 'iccid_nr': '2', 'iccid_cd': '2', 'imsi_mcc': '2', 'imsi_mnc': '2', 'imsi_hlr': '2', 'imsi_si': '2', 'cntrct_id_ref': '2', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 2, 'e_id': '2', 'is_production': 1},
        {'cntrct_id': 'C_ST_1', 'bpr_id': 31, 'bpri_com_id': 3, 'iccid_mi': '3', 'iccid_ii': '3', 'iccid_iai': '3', 'iccid_nr': '3', 'iccid_cd': '3', 'imsi_mcc': '3', 'imsi_mnc': '3', 'imsi_hlr': '3', 'imsi_si': '3', 'cntrct_id_ref': '3', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 3, 'e_id': '3', 'is_production': 1},
        {'cntrct_id': 'C_ST_7', 'bpr_id': 31, 'bpri_com_id': 4, 'iccid_mi': '4', 'iccid_ii': '4', 'iccid_iai': '4', 'iccid_nr': '4', 'iccid_cd': '4', 'imsi_mcc': '4', 'imsi_mnc': '4', 'imsi_hlr': '4', 'imsi_si': '4', 'cntrct_id_ref': '4', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 4, 'e_id': '4', 'is_production': 1},
    ]
    populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data)

    # Action: Call SP
    dag_params = {
        "p_job_kennung": "TEST_CNTRCT_ST",
        "p_eintrags_nr": "102",
        "p_stichtag": {'type': 'DATE', 'value': stichtag_val.strftime('%Y-%m-%d')},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert success, f"Stored procedure failed: {error_msg}"

    # Pass/Fail Criterion: Check target table content
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    query = f"SELECT CNTRCT_ID FROM `{target_table_id}` ORDER BY CNTRCT_ID"
    result = [row['CNTRCT_ID'] for row in bq_client.query(query).result()]

    assert len(result) == 2, f"Expected 2 records, got {len(result)}"
    assert 'C_ST_5' in result
    assert 'C_ST_6' in result
    assert 'C_ST_1' not in result
    assert 'C_ST_7' not in result
```

---

### Test Case 5: Filter Logic - `cds$ta_cntrct` Owner Filter (`redundant_owner_id`)

*   **Purpose:** Verify that only records from `cds$ta_cntrct` with `redundant_owner_id = 1` are included.
*   **Setup:**
    1.  Populate `cds$ta_cntrct` with records having `redundant_owner_id` values: 1 and others (e.g., 2, 3).
    2.  Populate `pds$ta_bpri_com` with matching `cntrct_id`s, ensuring other filters pass.
*   **Action:**
    1.  Trigger the Airflow DAG with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof$ta_bpr_basis_his` table contains only records where the original `cds$ta_cntrct.redundant_owner_id` was 1.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_filter_redundant_owner_id(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: Source data with various redundant_owner_id values
    stichtag_val = date(2023, 3, 15)
    cds_data = [
        {'cntrct_id': 'C_OWN_1', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C_OWN_2', 'cntrct_st': 5, 'redundant_owner_id': 2, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None}, # Should be filtered out
    ]
    pds_data = [
        {'cntrct_id': 'C_OWN_1', 'bpr_id': 31, 'bpri_com_id': 1, 'iccid_mi': '1', 'iccid_ii': '1', 'iccid_iai': '1', 'iccid_nr': '1', 'iccid_cd': '1', 'imsi_mcc': '1', 'imsi_mnc': '1', 'imsi_hlr': '1', 'imsi_si': '1', 'cntrct_id_ref': '1', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 1, 'e_id': '1', 'is_production': 1},
        {'cntrct_id': 'C_OWN_2', 'bpr_id': 31, 'bpri_com_id': 2, 'iccid_mi': '2', 'iccid_ii': '2', 'iccid_iai': '2', 'iccid_nr': '2', 'iccid_cd': '2', 'imsi_mcc': '2', 'imsi_mnc': '2', 'imsi_hlr': '2', 'imsi_si': '2', 'cntrct_id_ref': '2', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 2, 'e_id': '2', 'is_production': 1},
    ]
    populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data)

    # Action: Call SP
    dag_params = {
        "p_job_kennung": "TEST_OWNER_ID",
        "p_eintrags_nr": "103",
        "p_stichtag": {'type': 'DATE', 'value': stichtag_val.strftime('%Y-%m-%d')},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert success, f"Stored procedure failed: {error_msg}"

    # Pass/Fail Criterion: Check target table content
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    query = f"SELECT CNTRCT_ID FROM `{target_table_id}`"
    result = [row['CNTRCT_ID'] for row in bq_client.query(query).result()]

    assert len(result) == 1, f"Expected 1 record, got {len(result)}"
    assert 'C_OWN_1' in result
    assert 'C_OWN_2' not in result
```

---

### Test Case 6: Filter Logic - `cds$ta_cntrct` Date Filters (Boundary Conditions & NULLs)

*   **Purpose:** Verify the correct application of date filters on `cds$ta_cntrct` (`insert_at`, `modified_at`, `valid_from`, `valid_to`), especially around the `p_stichtag` boundary and NULL values.
*   **Setup:**
    1.  Populate `cds$ta_cntrct` with records testing:
        *   `insert_at <= p_stichtag` (equal, before, after)
        *   `(modified_at IS NULL OR modified_at > p_stichtag)` (NULL, after, before)
        *   `valid_from <= p_stichtag` (equal, before, after)
        *   `(valid_to IS NULL OR valid_to > p_stichtag)` (NULL, after, before)
    2.  Populate `pds$ta_bpri_com` with matching `cntrct_id`s, ensuring other filters pass.
*   **Action:**
    1.  Trigger the Airflow DAG with `p_stichtag = "2023-03-15"`.
*   **Pass/Fail Criterion:**
    1.  Only records satisfying all date conditions are present in `project.dataset.sof$ta_bpr_basis_his`.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_filter_cds_date_logic(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: Source data to test date filters
    stichtag_val = date(2023, 3, 15)
    cds_data = [
        # PASS: All conditions met
        {'cntrct_id': 'C_DATE_P1', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 3, 15, 0, 0, 0), 'modified_at': None, 'valid_from': date(2023, 3, 15), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C_DATE_P2', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 3, 14, 23, 59, 59), 'modified_at': datetime(2023, 3, 16, 0, 0, 0), 'valid_from': date(2023, 3, 14), 'valid_to': date(2023, 3, 16), 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        # FAIL: insert_at > p_stichtag
        {'cntrct_id': 'C_DATE_F1', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 3, 15, 0, 0, 1), 'modified_at': None, 'valid_from': date(2023, 3, 15), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        # FAIL: modified_at <= p_stichtag (and not NULL)
        {'cntrct_id': 'C_DATE_F2', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 3, 14), 'modified_at': datetime(2023, 3, 15, 0, 0, 0), 'valid_from': date(2023, 3, 14), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        # FAIL: valid_from > p_stichtag
        {'cntrct_id': 'C_DATE_F3', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 3, 14), 'modified_at': None, 'valid_from': date(2023, 3, 16), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        # FAIL: valid_to <= p_stichtag (and not NULL)
        {'cntrct_id': 'C_DATE_F4', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 3, 14), 'modified_at': None, 'valid_from': date(2023, 3, 14), 'valid_to': date(2023, 3, 15), 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
    ]
    pds_data = [
        {'cntrct_id': 'C_DATE_P1', 'bpr_id': 31, 'bpri_com_id': 1, 'iccid_mi': '1', 'iccid_ii': '1', 'iccid_iai': '1', 'iccid_nr': '1', 'iccid_cd': '1', 'imsi_mcc': '1', 'imsi_mnc': '1', 'imsi_hlr': '1', 'imsi_si': '1', 'cntrct_id_ref': '1', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 1, 'e_id': '1', 'is_production': 1},
        {'cntrct_id': 'C_DATE_P2', 'bpr_id': 31, 'bpri_com_id': 2, 'iccid_mi': '2', 'iccid_ii': '2', 'iccid_iai': '2', 'iccid_nr': '2', 'iccid_cd': '2', 'imsi_mcc': '2', 'imsi_mnc': '2', 'imsi_hlr': '2', 'imsi_si': '2', 'cntrct_id_ref': '2', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 2, 'e_id': '2', 'is_production': 1},
        {'cntrct_id': 'C_DATE_F1', 'bpr_id': 31, 'bpri_com_id': 3, 'iccid_mi': '3', 'iccid_ii': '3', 'iccid_iai': '3', 'iccid_nr': '3', 'iccid_cd': '3', 'imsi_mcc': '3', 'imsi_mnc': '3', 'imsi_hlr': '3', 'imsi_si': '3', 'cntrct_id_ref': '3', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 3, 'e_id': '3', 'is_production': 1},
        {'cntrct_id': 'C_DATE_F2', 'bpr_id': 31, 'bpri_com_id': 4, 'iccid_mi': '4', 'iccid_ii': '4', 'iccid_iai': '4', 'iccid_nr': '4', 'iccid_cd': '4', 'imsi_mcc': '4', 'imsi_mnc': '4', 'imsi_hlr': '4', 'imsi_si': '4', 'cntrct_id_ref': '4', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 4, 'e_id': '4', 'is_production': 1},
        {'cntrct_id': 'C_DATE_F3', 'bpr_id': 31, 'bpri_com_id': 5, 'iccid_mi': '5', 'iccid_ii': '5', 'iccid_iai': '5', 'iccid_nr': '5', 'iccid_cd': '5', 'imsi_mcc': '5', 'imsi_mnc': '5', 'imsi_hlr': '5', 'imsi_si': '5', 'cntrct_id_ref': '5', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 5, 'e_id': '5', 'is_production': 1},
        {'cntrct_id': 'C_DATE_F4', 'bpr_id': 31, 'bpri_com_id': 6, 'iccid_mi': '6', 'iccid_ii': '6', 'iccid_iai': '6', 'iccid_nr': '6', 'iccid_cd': '6', 'imsi_mcc': '6', 'imsi_mnc': '6', 'imsi_hlr': '6', 'imsi_si': '6', 'cntrct_id_ref': '6', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 6, 'e_id': '6', 'is_production': 1},
    ]
    populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data)

    # Action: Call SP
    dag_params = {
        "p_job_kennung": "TEST_CDS_DATE",
        "p_eintrags_nr": "104",
        "p_stichtag": {'type': 'DATE', 'value': stichtag_val.strftime('%Y-%m-%d')},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert success, f"Stored procedure failed: {error_msg}"

    # Pass/Fail Criterion: Check target table content
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    query = f"SELECT CNTRCT_ID FROM `{target_table_id}` ORDER BY CNTRCT_ID"
    result = [row['CNTRCT_ID'] for row in bq_client.query(query).result()]

    assert len(result) == 2, f"Expected 2 records, got {len(result)}"
    assert 'C_DATE_P1' in result
    assert 'C_DATE_P2' in result
    assert 'C_DATE_F1' not in result
    assert 'C_DATE_F2' not in result
    assert 'C_DATE_F3' not in result
    assert 'C_DATE_F4' not in result
```

---

### Test Case 7: ICCID Concatenation Transformation

*   **Purpose:** Verify that the `ICCID` column is correctly constructed by concatenating `iccid_mi`, `iccid_ii`, `iccid_iai`, `iccid_nr`, and `iccid_cd` with hyphens. Also, test NULL handling for individual `iccid_` parts.
*   **Setup:**
    1.  Populate `cds$ta_cntrct` with a single passing record.
    2.  Populate `pds$ta_bpri_com` with records having various combinations of `iccid_` parts, including some NULLs.
*   **Action:**
    1.  Trigger the Airflow DAG with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The `ICCID` column in `project.dataset.sof$ta_bpr_basis_his` matches the expected concatenated string for each record. NULL parts should result in an empty string in the concatenation.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_iccid_concatenation(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: Source data to test ICCID concatenation
    stichtag_val = date(2023, 3, 15)
    cds_data = [
        {'cntrct_id': 'C_ICCID_1', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C_ICCID_2', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
        {'cntrct_id': 'C_ICCID_3', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
    ]
    pds_data = [
        # Full ICCID
        {'cntrct_id': 'C_ICCID_1', 'bpr_id': 31, 'bpri_com_id': 1, 'iccid_mi': '111', 'iccid_ii': '222', 'iccid_iai': '333', 'iccid_nr': '444', 'iccid_cd': '5', 'imsi_mcc': '1', 'imsi_mnc': '1', 'imsi_hlr': '1', 'imsi_si': '1', 'cntrct_id_ref': '1', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 1, 'e_id': '1', 'is_production': 1},
        # ICCID with some NULL parts
        {'cntrct_id': 'C_ICCID_2', 'bpr_id': 31, 'bpri_com_id': 2, 'iccid_mi': '666', 'iccid_ii': None, 'iccid_iai': '777', 'iccid_nr': None, 'iccid_cd': '8', 'imsi_mcc': '2', 'imsi_mnc': '2', 'imsi_hlr': '2', 'imsi_si': '2', 'cntrct_id_ref': '2', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 2, 'e_id': '2', 'is_production': 1},
        # All ICCID parts NULL
        {'cntrct_id': 'C_ICCID_3', 'bpr_id': 31, 'bpri_com_id': 3, 'iccid_mi': None, 'iccid_ii': None, 'iccid_iai': None, 'iccid_nr': None, 'iccid_cd': None, 'imsi_mcc': '3', 'imsi_mnc': '3', 'imsi_hlr': '3', 'imsi_si': '3', 'cntrct_id_ref': '3', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 3, 'e_id': '3', 'is_production': 1},
    ]
    populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data)

    # Action: Call SP
    dag_params = {
        "p_job_kennung": "TEST_ICCID_CONCAT",
        "p_eintrags_nr": "105",
        "p_stichtag": {'type': 'DATE', 'value': stichtag_val.strftime('%Y-%m-%d')},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert success, f"Stored procedure failed: {error_msg}"

    # Pass/Fail Criterion: Check ICCID values
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    query = f"SELECT CNTRCT_ID, ICCID FROM `{target_table_id}` ORDER BY CNTRCT_ID"
    result = {row['CNTRCT_ID']: row['ICCID'] for row in bq_client.query(query).result()}

    assert result['C_ICCID_1'] == '111-222-333-444-5'
    assert result['C_ICCID_2'] == '666--777--8' # CONCAT treats NULL as empty string
    assert result['C_ICCID_3'] == '----'
```

---

### Test Case 8: Empty Source Tables

*   **Purpose:** Verify that the job runs successfully when source tables are empty, resulting in an empty target table and a correct log entry.
*   **Setup:**
    1.  Ensure `isbert_schema.cds$ta_cntrct` and `isbert_schema.pds$ta_bpri_com` are empty.
*   **Action:**
    1.  Trigger the Airflow DAG with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run completes successfully.
    2.  The `project.dataset.sof$ta_bpr_basis_his` table is empty (0 rows).
    3.  A 'SUCCESS' entry is recorded in `project.dataset.job_log` with `records_processed = 0`.

```python
import pytest
from google.cloud import bigquery
from datetime import date

def test_empty_source_tables(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: Ensure source tables are empty
    populate_source_data(bq_client, bq_source_dataset_ref, [], [])

    # Action: Call SP
    dag_params = {
        "p_job_kennung": "TEST_EMPTY_SOURCES",
        "p_eintrags_nr": "106",
        "p_stichtag": {'type': 'DATE', 'value': "2023-03-15"},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert success, f"Stored procedure failed: {error_msg}"

    # Pass/Fail Criterion 1 & 2: Check target table content
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    query_result = bq_client.query(f"SELECT COUNT(*) FROM `{target_table_id}`").result()
    actual_count = list(query_result)[0][0]
    assert actual_count == 0, f"Expected 0 records, got {actual_count}"

    # Pass/Fail Criterion 3: Check job_log entry
    job_log_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.job_log"
    log_query = f"""
        SELECT job_name, status, records_processed
        FROM `{job_log_table_id}`
        WHERE job_name = 'TEST_EMPTY_SOURCES'
        ORDER BY created_at DESC LIMIT 1
    """
    log_result = list(bq_client.query(log_query).result())
    assert len(log_result) == 1, "Expected one log entry for TEST_EMPTY_SOURCES"
    log_entry = log_result[0]
    assert log_entry['status'] == 'SUCCESS'
    assert log_entry['records_processed'] == 0
```

---

### Test Case 9: Target Table Truncation

*   **Purpose:** Verify that the `project.dataset.sof$ta_bpr_basis_his` table is truncated before new data is inserted, ensuring idempotency and preventing duplicate data on re-runs.
*   **Setup:**
    1.  Pre-populate `project.dataset.sof$ta_bpr_basis_his` with some dummy data.
    2.  Populate source tables with data that should result in a *different* set of records than the dummy data.
*   **Action:**
    1.  Trigger the Airflow DAG with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof$ta_bpr_basis_his` table contains only the data generated by the current run, and none of the pre-existing dummy data.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_target_table_truncation(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup 1: Pre-populate target table with dummy data
    target_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.sof$ta_bpr_basis_his"
    dummy_data = [
        {'CNTRCT_ID': 'DUMMY1', 'BPR_ID': 1, 'BPRI_COM_ID': 1, 'ICCID': 'DUMMY-ICCID-1', 'IMSI_MCC': '1', 'IMSI_MNC': '1', 'IMSI_HLR': '1', 'IMSI_SI': '1', 'CNTRCT_ID_REF': '1', 'VALID_FROM': date(2022, 1, 1), 'VALID_TO': None, 'MODIFIED_AT': None, 'INSERT_AT': datetime(2022, 1, 1), 'SLAVE_NUMBER': 1, 'E_ID': '1'},
        {'CNTRCT_ID': 'DUMMY2', 'BPR_ID': 2, 'BPRI_COM_ID': 2, 'ICCID': 'DUMMY-ICCID-2', 'IMSI_MCC': '2', 'IMSI_MNC': '2', 'IMSI_HLR': '2', 'IMSI_SI': '2', 'CNTRCT_ID_REF': '2', 'VALID_FROM': date(2022, 1, 1), 'VALID_TO': None, 'MODIFIED_AT': None, 'INSERT_AT': datetime(2022, 1, 1), 'SLAVE_NUMBER': 2, 'E_ID': '2'},
    ]
    rows_to_insert = [tuple(row.values()) for row in dummy_data]
    errors = bq_client.insert_rows(bq_client.get_table(target_table_id), rows_to_insert)
    assert not errors, f"Error inserting dummy data: {errors}"
    
    # Verify dummy data is present initially
    initial_count = list(bq_client.query(f"SELECT COUNT(*) FROM `{target_table_id}`").result())[0][0]
    assert initial_count == 2, "Dummy data not inserted correctly"

    # Setup 2: Populate source tables with new data
    stichtag_val = date(2023, 3, 15)
    cds_data = [
        {'cntrct_id': 'C_NEW_1', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
    ]
    pds_data = [
        {'cntrct_id': 'C_NEW_1', 'bpr_id': 31, 'bpri_com_id': 100, 'iccid_mi': 'N1', 'iccid_ii': 'N2', 'iccid_iai': 'N3', 'iccid_nr': 'N4', 'iccid_cd': 'N5', 'imsi_mcc': 'N', 'imsi_mnc': 'N', 'imsi_hlr': 'N', 'imsi_si': 'N', 'cntrct_id_ref': 'N', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 1, 'e_id': 'N', 'is_production': 1},
    ]
    populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data)

    # Action: Call SP
    dag_params = {
        "p_job_kennung": "TEST_TRUNCATION",
        "p_eintrags_nr": "107",
        "p_stichtag": {'type': 'DATE', 'value': stichtag_val.strftime('%Y-%m-%d')},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert success, f"Stored procedure failed: {error_msg}"

    # Pass/Fail Criterion: Check target table content - only new data should be present
    query_result = bq_client.query(f"SELECT CNTRCT_ID FROM `{target_table_id}`").result()
    actual_ids = [row['CNTRCT_ID'] for row in query_result]

    assert len(actual_ids) == 1, f"Expected 1 new record, got {len(actual_ids)}"
    assert 'C_NEW_1' in actual_ids
    assert 'DUMMY1' not in actual_ids, "Dummy data was not truncated!"
    assert 'DUMMY2' not in actual_ids, "Dummy data was not truncated!"
```

---

### Test Case 10: `job_log` Table - Failure Entry

*   **Purpose:** Verify that if the core SQL logic within the stored procedure fails (e.g., due to a data type mismatch or a non-existent table), a 'FAILED' entry is correctly recorded in the `job_log` table with error details.
*   **Setup:**
    1.  Populate source tables with data that would normally pass.
    2.  **Introduce a controlled error:** This is tricky for a generic test. A common way is to temporarily rename a source table or alter its schema in a way that breaks the SP's `INSERT` statement. For this test, we'll simulate a failure by calling a non-existent SP or by passing an invalid parameter that the SP's `EXCEPTION` block would catch if it were designed to. Given the current SP, we'll simulate a failure *within* the SP by attempting to insert into a non-existent column or by causing a data type error.
*   **Action:**
    1.  Trigger the Airflow DAG (or call the BigQuery Stored Procedure) with parameters that will cause the SP to fail during its execution.
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run fails (or the direct SP call raises an error).
    2.  A 'FAILED' entry is recorded in `project.dataset.job_log` with the correct `job_name`, `status = 'FAILED'`, a non-zero `error_nr`, and a descriptive `error_arg` (error message).
    3.  The `records_processed` field in the log entry is `NULL`.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_job_log_failure_entry(bq_client, bq_target_dataset_ref, bq_source_dataset_ref):
    # Setup: Populate source tables with valid data
    stichtag_val = date(2023, 3, 15)
    cds_data = [
        {'cntrct_id': 'C_FAIL_1', 'cntrct_st': 5, 'redundant_owner_id': 1, 'insert_at': datetime(2023, 1, 1), 'modified_at': None, 'valid_from': date(2023, 1, 1), 'valid_to': None, 'is_production': 1, 'cntrct_ty': 10, 'cntrct_parent': None},
    ]
    pds_data = [
        {'cntrct_id': 'C_FAIL_1', 'bpr_id': 31, 'bpri_com_id': 100, 'iccid_mi': '1', 'iccid_ii': '2', 'iccid_iai': '3', 'iccid_nr': '4', 'iccid_cd': '5', 'imsi_mcc': '1', 'imsi_mnc': '1', 'imsi_hlr': '1', 'imsi_si': '1', 'cntrct_id_ref': '1', 'valid_from': date(2023, 1, 1), 'valid_to': None, 'modified_at': None, 'insert_at': datetime(2023, 1, 1), 'slave_number': 1, 'e_id': '1', 'is_production': 1},
    ]
    populate_source_data(bq_client, bq_source_dataset_ref, cds_data, pds_data)

    # Action: Simulate a failure within the SP.
    # This is done by calling a non-existent SP, or by modifying the SP to force an error.
    # For this test, we'll call a non-existent SP to ensure the `call_bigquery_stored_procedure`
    # helper correctly captures the failure and that the `job_log` would be updated if the
    # SP's EXCEPTION block were hit.
    # A more realistic test would involve temporarily breaking the target table schema
    # (e.g., dropping a column) and then calling the actual SP.

    # Let's assume we temporarily modify the SP to cause an error, e.g., by trying to
    # insert a STRING into an INT64 column.
    # Since we cannot dynamically alter the deployed SP for a test, we'll simulate the
    # failure and check the log. The SP's EXCEPTION block is designed to catch errors.

    # To force a failure in the SP, we could temporarily rename a source table.
    # This would cause the SELECT statement to fail.
    # Let's rename `cds$ta_cntrct` to `cds$ta_cntrct_temp` and then call the SP.
    cds_table_id = f"{bq_client.project}.{bq_source_dataset_ref.dataset_id}.cds$ta_cntrct"
    temp_cds_table_id = f"{bq_client.project}.{bq_source_dataset_ref.dataset_id}.cds$ta_cntrct_temp"
    bq_client.query(f"ALTER TABLE `{cds_table_id}` RENAME TO `{temp_cds_table_id}`").result()

    dag_params = {
        "p_job_kennung": "TEST_FAILURE_SCENARIO",
        "p_eintrags_nr": "108",
        "p_stichtag": {'type': 'DATE', 'value': stichtag_val.strftime('%Y-%m-%d')},
        "p_wiederanlauf_wert": "0",
    }
    success, error_msg = call_bigquery_stored_procedure(
        bq_client, bq_target_dataset_ref, "r_ausd_bp_ta_bpr_basis_his", dag_params
    )
    assert not success, "Stored procedure was expected to fail"
    assert "Not found: Table" in error_msg # Verify the expected error message

    # Cleanup: Rename the table back
    bq_client.query(f"ALTER TABLE `{temp_cds_table_id}` RENAME TO `{cds_table_id}`").result()

    # Pass/Fail Criterion: Check job_log entry for failure
    job_log_table_id = f"{bq_client.project}.{bq_target_dataset_ref.dataset_id}.job_log"
    log_query = f"""
        SELECT job_name, status, error_nr, error_arg, stichtag, records_processed
        FROM `{job_log_table_id}`
        WHERE job_name = 'TEST_FAILURE_SCENARIO'
        ORDER BY created_at DESC LIMIT 1
    """
    log_result = list(bq_client.query(log_query).result())
    assert len(log_result) == 1, "Expected one log entry for TEST_FAILURE_SCENARIO"
    log_entry = log_result[0]
    assert log_entry['status'] == 'FAILED'
    assert log_entry['error_nr'] is not None and log_entry['error_nr'] != 0
    assert "Not found: Table" in log_entry['error_arg']
    assert log_entry['stichtag'] == stichtag_val
    assert log_entry['records_processed'] is None
```