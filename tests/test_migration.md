As a senior data-migration QA engineer, I have analyzed the provided migration design and the generated BigQuery Stored Procedure (`r_ausd_bp_ta_rn_vertrag`). The core transformation logic is inferred to be a `GROUP BY cntrct_id` with `MAX` aggregations from `SOF_TA_RN_EINZELN` to `SOF_TA_RN_VERTRAG`, preceded by a `TRUNCATE`. The orchestration logic handles parameter validation, date parsing, and logging.

A critical observation is that the `v_datum_from_dwtk` variable, derived from `DWTK_MELDUNGEN`, and the input parameters `p_eintrags_nr` and `p_wiederanlauf_wert` are *not used* in the main `INSERT` statement within the provided BigQuery Stored Procedure. This suggests a potential incompleteness in the migration of the `d_ausd_bp_ta_rn_vertrag.sql` logic, as these elements likely had a functional purpose in the legacy Oracle SQL or `DWPA_UTIL_SKRIPT` package. The tests below will validate the behavior of the *provided* BigQuery SP, including the inert parts, but this observation should be highlighted for further investigation with the original `d_ausd_bp_ta_rn_vertrag.sql` content.

The tests are designed to be run using `pytest` and interact with Google BigQuery using the `google-cloud-bigquery` client library.

---

## Test Suite: `k_ausd_bp_ta_rn_vertrag.ksh` to BigQuery Migration

### Prerequisites:
*   Google Cloud Project configured with BigQuery API enabled.
*   BigQuery dataset (`my_project.my_dataset`) created.
*   All DDLs (`dwtk_meldungen_ddl.sql`, `sof_ta_rn_einzeln_ddl.sql`, `sof_ta_rn_vertrag_ddl.sql`, `job_log_ddl.sql`, `error_log_ddl.sql`) executed to create the necessary tables.
*   The BigQuery Stored Procedure `r_ausd_bp_ta_rn_vertrag` deployed.
*   Python environment with `pytest` and `google-cloud-bigquery` installed.
*   `BIGQUERY_PROJECT_ID` environment variable set to your project ID (e.g., `my_project`).

### Test Setup (Python `pytest` code)

```python
import pytest
from google.cloud import bigquery
import os
import uuid
from datetime import datetime, date, timezone

# --- Configuration ---
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "my_project")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "my_dataset")
SP_NAME = "r_ausd_bp_ta_rn_vertrag"
FULL_SP_PATH = f"{PROJECT_ID}.{DATASET_ID}.{SP_NAME}"

# Table references
DWTK_MELDUNGEN_TABLE = f"{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen"
SOF_TA_RN_EINZELN_TABLE = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_einzeln"
SOF_TA_RN_VERTRAG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag"
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
ERROR_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.error_log"

client = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions for Test Setup/Teardown ---
def _clear_table(table_id):
    """Clears all data from a BigQuery table."""
    query = f"TRUNCATE TABLE `{table_id}`"
    client.query(query).result()

def _insert_rows(table_id, rows):
    """Inserts rows into a BigQuery table."""
    # BigQuery insert_rows_json expects datetime objects to be timezone-aware
    # Convert any naive datetime objects to UTC
    processed_rows = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, datetime) and v.tzinfo is None:
                new_row[k] = v.replace(tzinfo=timezone.utc)
            else:
                new_row[k] = v
        processed_rows.append(new_row)

    errors = client.insert_rows_json(table_id, processed_rows)
    if errors:
        raise Exception(f"Errors inserting rows into {table_id}: {errors}")

def _get_table_data(table_id, order_by=None):
    """Fetches all data from a table."""
    query = f"SELECT * FROM `{table_id}`"
    if order_by:
        query += f" ORDER BY {order_by}"
    rows = client.query(query).result()
    # Convert BigQuery Row objects to dicts for easier comparison
    return [dict(row) for row in rows]

def _get_row_count(table_id):
    """Fetches row count from a table."""
    query = f"SELECT COUNT(*) FROM `{table_id}`"
    row = client.query(query).result().to_dataframe().iloc[0, 0]
    return row

def _call_sp(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
    """Calls the BigQuery Stored Procedure."""
    params = [
        bigquery.ScalarQueryParameter("p_job_kennung", "STRING", job_kennung),
        bigquery.ScalarQueryParameter("p_eintrags_nr", "STRING", eintrags_nr),
        bigquery.ScalarQueryParameter("p_stichtag", "STRING", stichtag),
        bigquery.ScalarQueryParameter("p_wiederanlauf_wert", "STRING", wiederanlauf_wert),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query = f"CALL `{FULL_SP_PATH}`(?, ?, ?, ?)"
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result() # Wait for job to complete
        return True, None
    except Exception as e:
        return False, str(e)

# --- Pytest Fixtures ---
@pytest.fixture(autouse=True)
def setup_and_teardown_tables():
    """Fixture to clear tables before and after each test."""
    _clear_table(DWTK_MELDUNGEN_TABLE)
    _clear_table(SOF_TA_RN_EINZELN_TABLE)
    _clear_table(SOF_TA_RN_VERTRAG_TABLE)
    _clear_table(JOB_LOG_TABLE)
    _clear_table(ERROR_LOG_TABLE)
    yield
    # Optional: clear tables after tests if needed, but autouse handles before.
    # For CI/CD, it's good to ensure clean state for next run.
    _clear_table(DWTK_MELDUNGEN_TABLE)
    _clear_table(SOF_TA_RN_EINZELN_TABLE)
    _clear_table(SOF_TA_RN_VERTRAG_TABLE)
    _clear_table(JOB_LOG_TABLE)
    _clear_table(ERROR_LOG_TABLE)
```

---

### Test Case 1: Output Parity - Full Data Transformation

*   **Purpose:** Verify that with valid inputs, the BigQuery SP produces the exact same output in `SOF_TA_RN_VERTRAG` as the legacy job. This test covers the core `GROUP BY` and `MAX` aggregation logic, including handling of `NULL` values and various data types (STRING, DATE).
*   **Setup:**
    1.  Populate `DWTK_MELDUNGEN` with a record for `job_kennung = 'BERT_DROP_TEMP_TABLE'` to ensure `v_datum_from_dwtk` is calculated.
    2.  Populate `SOF_TA_RN_EINZELN` with a diverse set of data:
        *   Multiple rows for `cntrct_id 'C1'` to test `MAX` aggregation with mixed `NULL` and non-`NULL` values for different columns (strings and dates).
        *   A single row for `cntrct_id 'C2'` to test cases where no effective aggregation occurs.
    3.  Define `expected_output` based on the expected `MAX` aggregation behavior. (In a real migration, this would be derived from the legacy system's output for the same input data).
*   **Action:** Call the BigQuery Stored Procedure `r_ausd_bp_ta_rn_vertrag` with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure executes successfully without raising an error.
    2.  The row count in `SOF_TA_RN_VERTRAG` matches the expected row count (2 rows).
    3.  The content of `SOF_TA_RN_VERTRAG` (ordered by `cntrct_id`) exactly matches the `expected_output` dictionary list, ensuring all columns are correctly aggregated.
    4.  The `job_log` table contains a `SUCCESS` entry for the run, with `records_processed` matching the actual count.

```python
def test_output_parity_full_transformation():
    """
    Purpose: Verify that the BigQuery SP produces the exact same output in SOF_TA_RN_VERTRAG
             as the legacy job for a representative dataset, covering aggregation and various data types.
    """
    # Setup: Populate DWTK_MELDUNGEN and SOF_TA_RN_EINZELN
    _insert_rows(DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0)},
        {"job_kennung": "OTHER_JOB", "timecreated": datetime(2023, 1, 10, 9, 0, 0)},
    ])

    source_data = [
        # cntrct_id C1: Aggregation test with NULLs and different dates
        {"cntrct_id": "C1", "TN_multi_single": "S", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": "D1", "TN_DAT_status": "X", "TN_DAT_valid_to": date(2023, 1, 10),
         "TC_multi_single": "M", "TC_TEL_msisdn": "222", "TC_TEL_status": "B", "TC_TEL_valid_to": date(2023, 1, 2),
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": "S", "TB_TEL_msisdn": "333", "TB_TEL_status": "C", "TB_TEL_valid_to": date(2023, 1, 3),
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": "M1", "MS_RN_1_status": "P", "MS_RN_1_valid_to": date(2023, 1, 4),
         "MS_RN_2_msisdn": "M2", "MS_RN_2_status": "Q", "MS_RN_2_valid_to": date(2023, 1, 5)},
        {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "444", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2),
         "TN_FAX_msisdn": "F1", "TN_FAX_status": "Y", "TN_FAX_valid_to": date(2023, 1, 11),
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": "S", "TC_TEL_msisdn": "555", "TC_TEL_status": "A", "TC_TEL_valid_to": date(2023, 1, 1),
         "TC_FAX_msisdn": "F2", "TC_FAX_status": "Z", "TC_FAX_valid_to": date(2023, 1, 12),
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": "M", "TB_TEL_msisdn": "666", "TB_TEL_status": "D", "TB_TEL_valid_to": date(2023, 1, 4),
         "TB_FAX_msisdn": "F3", "TB_FAX_status": "W", "TB_FAX_valid_to": date(2023, 1, 13),
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": "M3", "MS_RN_1_status": "R", "MS_RN_1_valid_to": date(2023, 1, 6),
         "MS_RN_2_msisdn": "M4", "MS_RN_2_status": "S", "MS_RN_2_valid_to": date(2023, 1, 7)},
        # cntrct_id C2: Single row
        {"cntrct_id": "C2", "TN_multi_single": "S", "TN_TEL_msisdn": "777", "TN_TEL_status": "E", "TN_TEL_valid_to": date(2023, 2, 1),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": "S", "TC_TEL_msisdn": "888", "TC_TEL_status": "F", "TC_TEL_valid_to": date(2023, 2, 2),
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": "S", "TB_TEL_msisdn": "999", "TB_TEL_status": "G", "TB_TEL_valid_to": date(2023, 2, 3),
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": "M5", "MS_RN_1_status": "T", "MS_RN_1_valid_to": date(2023, 2, 4),
         "MS_RN_2_msisdn": "M6", "MS_RN_2_status": "U", "MS_RN_2_valid_to": date(2023, 2, 5)},
    ]
    _insert_rows(SOF_TA_RN_EINZELN_TABLE, source_data)

    # Expected output based on MAX aggregation
    expected_output = [
        {"cntrct_id": "C1", "TN_multi_single": "S", "TN_TEL_msisdn": "444", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2),
         "TN_FAX_msisdn": "F1", "TN_FAX_status": "Y", "TN_FAX_valid_to": date(2023, 1, 11),
         "TN_DAT_msisdn": "D1", "TN_DAT_status": "X", "TN_DAT_valid_to": date(2023, 1, 10),
         "TC_multi_single": "M", "TC_TEL_msisdn": "555", "TC_TEL_status": "B", "TC_TEL_valid_to": date(2023, 1, 2),
         "TC_FAX_msisdn": "F2", "TC_FAX_status": "Z", "TC_FAX_valid_to": date(2023, 1, 12),
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": "S", "TB_TEL_msisdn": "666", "TB_TEL_status": "D", "TB_TEL_valid_to": date(2023, 1, 4),
         "TB_FAX_msisdn": "F3", "TB_FAX_status": "W", "TB_FAX_valid_to": date(2023, 1, 13),
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": "M3", "MS_RN_1_status": "R", "MS_RN_1_valid_to": date(2023, 1, 6),
         "MS_RN_2_msisdn": "M4", "MS_RN_2_status": "S", "MS_RN_2_valid_to": date(2023, 1, 7)},
        {"cntrct_id": "C2", "TN_multi_single": "S", "TN_TEL_msisdn": "777", "TN_TEL_status": "E", "TN_TEL_valid_to": date(2023, 2, 1),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": "S", "TC_TEL_msisdn": "888", "TC_TEL_status": "F", "TC_TEL_valid_to": date(2023, 2, 2),
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": "S", "TB_TEL_msisdn": "999", "TB_TEL_status": "G", "TB_TEL_valid_to": date(2023, 2, 3),
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": "M5", "MS_RN_1_status": "T", "MS_RN_1_valid_to": date(2023, 2, 4),
         "MS_RN_2_msisdn": "M6", "MS_RN_2_status": "U", "MS_RN_2_valid_to": date(2023, 2, 5)},
    ]

    # Action: Call SP
    success, error_msg = _call_sp("JOB1", "ENTRY1", "01012023", "0")
    assert success, f"SP call failed: {error_msg}"

    # Pass/Fail Criterion
    actual_output = _get_table_data(SOF_TA_RN_VERTRAG_TABLE, order_by="cntrct_id")
    assert len(actual_output) == len(expected_output), "Row count mismatch in target table."
    
    # Sort both lists by cntrct_id for reliable comparison
    actual_output_sorted = sorted(actual_output, key=lambda x: x['cntrct_id'])
    expected_output_sorted = sorted(expected_output, key=lambda x: x['cntrct_id'])

    for i, actual_row in enumerate(actual_output_sorted):
        expected_row = expected_output_sorted[i]
        for col in expected_row:
            # Handle potential differences in how BigQuery returns datetimes vs. Python date objects
            actual_value = actual_row.get(col)
            expected_value = expected_row.get(col)
            if isinstance(actual_value, datetime):
                actual_value = actual_value.date() # Compare only date part
            assert actual_value == expected_value, \
                f"Mismatch in row {i}, column '{col}': Expected '{expected_value}', Got '{actual_value}'"

    # Verify job_log
    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2 # RUNNING and SUCCESS
    success_log = next((log for log in job_logs if log['status'] == 'SUCCESS'), None)
    assert success_log is not None
    assert success_log['records_processed'] == len(expected_output)
    assert success_log['message'] == 'Job completed successfully'
    assert success_log['end_time'] is not None
```

---

### Test Case 2: Parameter Validation - Missing `p_job_kennung`

*   **Purpose:** Ensure the Stored Procedure correctly identifies and raises an error for a missing required parameter (`p_job_kennung`).
*   **Setup:** None specific.
*   **Action:** Call the Stored Procedure with `p_job_kennung = NULL`.
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure call fails.
    2.  The error message returned contains "Parameter p_job_kennung cannot be NULL or empty."
    3.  The `job_log` table contains an entry for the run with `status = 'FAILED'`.
    4.  The `error_log` table contains an entry with the specific error message.

```python
def test_parameter_validation_missing_job_kennung():
    """
    Purpose: Ensure the SP correctly identifies and raises an error for a missing required parameter.
    """
    # Action: Call SP with p_job_kennung = NULL
    success, error_msg = _call_sp(None, "ENTRY1", "01012023", "0")
    assert not success, "SP call unexpectedly succeeded."
    assert "Parameter p_job_kennung cannot be NULL or empty." in error_msg

    # Pass/Fail Criterion: Verify logs
    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2 # RUNNING and FAILED
    failed_log = next((log for log in job_logs if log['status'] == 'FAILED'), None)
    assert failed_log is not None
    assert "Job failed: Parameter p_job_kennung cannot be NULL or empty." in failed_log['message']
    assert failed_log['end_time'] is not None

    error_logs = _get_table_data(ERROR_LOG_TABLE)
    assert len(error_logs) == 1
    assert "Parameter p_job_kennung cannot be NULL or empty." in error_logs[0]['error_message']
```

---

### Test Case 3: Parameter Validation - Missing `p_eintrags_nr`

*   **Purpose:** Ensure the Stored Procedure correctly identifies and raises an error for a missing required parameter (`p_eintrags_nr`).
*   **Setup:** None specific.
*   **Action:** Call the Stored Procedure with `p_eintrags_nr = NULL`.
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure call fails.
    2.  The error message returned contains "Parameter p_eintrags_nr cannot be NULL or empty."
    3.  The `job_log` table contains an entry for the run with `status = 'FAILED'`.
    4.  The `error_log` table contains an entry with the specific error message.

```python
def test_parameter_validation_missing_eintrags_nr():
    """
    Purpose: Ensure the SP correctly identifies and raises an error for a missing p_eintrags_nr parameter.
    """
    # Action: Call SP with p_eintrags_nr = NULL
    success, error_msg = _call_sp("JOB1", None, "01012023", "0")
    assert not success, "SP call unexpectedly succeeded."
    assert "Parameter p_eintrags_nr cannot be NULL or empty." in error_msg

    # Pass/Fail Criterion: Verify logs
    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2 # RUNNING and FAILED
    failed_log = next((log for log in job_logs if log['status'] == 'FAILED'), None)
    assert failed_log is not None
    assert "Job failed: Parameter p_eintrags_nr cannot be NULL or empty." in failed_log['message']

    error_logs = _get_table_data(ERROR_LOG_TABLE)
    assert len(error_logs) == 1
    assert "Parameter p_eintrags_nr cannot be NULL or empty." in error_logs[0]['error_message']
```

---

### Test Case 4: Parameter Validation - Invalid `p_stichtag` Format

*   **Purpose:** Ensure the Stored Procedure correctly validates the `p_stichtag` parameter's format (`DDMMYYYY`).
*   **Setup:** None.
*   **Action:** Call the Stored Procedure with `p_stichtag = '2023-01-01'` (an invalid format).
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure call fails.
    2.  The error message returned contains "Parameter p_stichtag ... has invalid format. Expected DDMMYYYY."
    3.  The `job_log` table contains an entry for the run with `status = 'FAILED'`.
    4.  The `error_log` table contains an entry with the specific error message.

```python
def test_parameter_validation_invalid_stichtag_format():
    """
    Purpose: Ensure the SP correctly validates the p_stichtag format.
    """
    # Action: Call SP with p_stichtag = '2023-01-01' (wrong format)
    success, error_msg = _call_sp("JOB1", "ENTRY1", "2023-01-01", "0")
    assert not success, "SP call unexpectedly succeeded."
    assert "Parameter p_stichtag \"2023-01-01\" has invalid format. Expected DDMMYYYY." in error_msg

    # Pass/Fail Criterion: Verify logs
    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2 # RUNNING and FAILED
    failed_log = next((log for log in job_logs if log['status'] == 'FAILED'), None)
    assert failed_log is not None
    assert "Job failed: Parameter p_stichtag \"2023-01-01\" has invalid format." in failed_log['message']

    error_logs = _get_table_data(ERROR_LOG_TABLE)
    assert len(error_logs) == 1
    assert "Parameter p_stichtag \"2023-01-01\" has invalid format." in error_logs[0]['error_message']
```

---

### Test Case 5: `DWTK_MELDUNGEN` Logic - No Matching `job_kennung`

*   **Purpose:** Verify that the `v_datum_from_dwtk` variable correctly defaults to '19000101' when no matching `job_kennung = 'BERT_DROP_TEMP_TABLE'` is found in `DWTK_MELDUNGEN`. This also ensures the job completes successfully despite this condition.
*   **Setup:**
    1.  `DWTK_MELDUNGEN` contains data, but *not* for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  `SOF_TA_RN_EINZELN` has some data to ensure the main transformation runs and produces output.
*   **Action:** Call the Stored Procedure with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure completes successfully.
    2.  `SOF_TA_RN_VERTRAG` is populated correctly based on `SOF_TA_RN_EINZELN` (as `v_datum_from_dwtk` does not influence the main transformation in the provided SP).
    3.  The `job_log` table shows a successful entry.

```python
def test_dwtk_meldungen_no_matching_job_kennung():
    """
    Purpose: Verify v_datum_from_dwtk defaults to '19000101' when 'BERT_DROP_TEMP_TABLE'
             is not found in DWTK_MELDUNGEN, and the job still completes successfully.
    """
    # Setup: DWTK_MELDUNGEN contains other data, SOF_TA_RN_EINZELN has data.
    _insert_rows(DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "ANOTHER_JOB", "timecreated": datetime(2023, 1, 10, 9, 0, 0)},
    ])
    _insert_rows(SOF_TA_RN_EINZELN_TABLE, [
        {"cntrct_id": "C3", "TN_multi_single": "S", "TN_TEL_msisdn": "100", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 3, 1)},
    ])
    expected_output = [
        {"cntrct_id": "C3", "TN_multi_single": "S", "TN_TEL_msisdn": "100", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 3, 1),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": None, "TC_TEL_msisdn": None, "TC_TEL_status": None, "TC_TEL_valid_to": None,
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": None, "TB_TEL_msisdn": None, "TB_TEL_status": None, "TB_TEL_valid_to": None,
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": None, "MS_RN_1_status": None, "MS_RN_1_valid_to": None,
         "MS_RN_2_msisdn": None, "MS_RN_2_status": None, "MS_RN_2_valid_to": None},
    ]

    # Action: Call SP
    success, error_msg = _call_sp("JOB1", "ENTRY1", "01012023", "0")
    assert success, f"SP call failed: {error_msg}"

    # Pass/Fail Criterion
    actual_output = _get_table_data(SOF_TA_RN_VERTRAG_TABLE, order_by="cntrct_id")
    assert len(actual_output) == len(expected_output)
    
    actual_output_sorted = sorted(actual_output, key=lambda x: x['cntrct_id'])
    expected_output_sorted = sorted(expected_output, key=lambda x: x['cntrct_id'])

    for i, actual_row in enumerate(actual_output_sorted):
        expected_row = expected_output_sorted[i]
        for col in expected_row:
            actual_value = actual_row.get(col)
            expected_value = expected_row.get(col)
            if isinstance(actual_value, datetime):
                actual_value = actual_value.date()
            assert actual_value == expected_value, \
                f"Mismatch in row {i}, column '{col}': Expected '{expected_value}', Got '{actual_value}'"

    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2
    success_log = next((log for log in job_logs if log['status'] == 'SUCCESS'), None)
    assert success_log is not None
    assert success_log['records_processed'] == len(expected_output)
```

---

### Test Case 6: `DWTK_MELDUNGEN` Logic - Matching `job_kennung`

*   **Purpose:** Verify that the `v_datum_from_dwtk` variable is correctly derived from `DWTK_MELDUNGEN` when a matching `job_kennung = 'BERT_DROP_TEMP_TABLE'` exists. This also ensures the job completes successfully.
*   **Setup:**
    1.  `DWTK_MELDUNGEN` contains a row for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a specific `timecreated`.
    2.  `SOF_TA_RN_EINZELN` has some data.
*   **Action:** Call the Stored Procedure with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure completes successfully.
    2.  `SOF_TA_RN_VERTRAG` is populated correctly.
    3.  The `job_log` table shows a successful entry.

```python
def test_dwtk_meldungen_matching_job_kennung():
    """
    Purpose: Verify v_datum_from_dwtk is correctly derived when 'BERT_DROP_TEMP_TABLE' exists.
             (Note: v_datum_from_dwtk is not used in the main transformation in the provided SP,
             so this test primarily ensures its calculation doesn't cause errors and job succeeds).
    """
    # Setup: DWTK_MELDUNGEN contains a row for BERT_DROP_TEMP_TABLE.
    _insert_rows(DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 5, 20, 12, 0, 0)},
        {"job_kennung": "OTHER_JOB", "timecreated": datetime(2023, 5, 10, 9, 0, 0)},
    ])
    _insert_rows(SOF_TA_RN_EINZELN_TABLE, [
        {"cntrct_id": "C4", "TN_multi_single": "S", "TN_TEL_msisdn": "200", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 4, 1)},
    ])
    expected_output = [
        {"cntrct_id": "C4", "TN_multi_single": "S", "TN_TEL_msisdn": "200", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 4, 1),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": None, "TC_TEL_msisdn": None, "TC_TEL_status": None, "TC_TEL_valid_to": None,
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": None, "TB_TEL_msisdn": None, "TB_TEL_status": None, "TB_TEL_valid_to": None,
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": None, "MS_RN_1_status": None, "MS_RN_1_valid_to": None,
         "MS_RN_2_msisdn": None, "MS_RN_2_status": None, "MS_RN_2_valid_to": None},
    ]

    # Action: Call SP
    success, error_msg = _call_sp("JOB1", "ENTRY1", "01012023", "0")
    assert success, f"SP call failed: {error_msg}"

    # Pass/Fail Criterion
    actual_output = _get_table_data(SOF_TA_RN_VERTRAG_TABLE, order_by="cntrct_id")
    assert len(actual_output) == len(expected_output)
    
    actual_output_sorted = sorted(actual_output, key=lambda x: x['cntrct_id'])
    expected_output_sorted = sorted(expected_output, key=lambda x: x['cntrct_id'])

    for i, actual_row in enumerate(actual_output_sorted):
        expected_row = expected_output_sorted[i]
        for col in expected_row:
            actual_value = actual_row.get(col)
            expected_value = expected_row.get(col)
            if isinstance(actual_value, datetime):
                actual_value = actual_value.date()
            assert actual_value == expected_value, \
                f"Mismatch in row {i}, column '{col}': Expected '{expected_value}', Got '{actual_value}'"

    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2
    success_log = next((log for log in job_logs if log['status'] == 'SUCCESS'), None)
    assert success_log is not None
    assert success_log['records_processed'] == len(expected_output)
```

---

### Test Case 7: Transformation - Empty Source Table (`SOF_TA_RN_EINZELN`)

*   **Purpose:** Ensure the job handles an empty source table (`SOF_TA_RN_EINZELN`) gracefully, resulting in an empty target table and a successful log entry.
*   **Setup:** `SOF_TA_RN_EINZELN` is empty (ensured by fixture). `DWTK_MELDUNGEN` can be empty or populated, as it doesn't affect the main transformation.
*   **Action:** Call the Stored Procedure with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure completes successfully.
    2.  `SOF_TA_RN_VERTRAG` is empty.
    3.  The `job_log` table shows a successful entry with `records_processed = 0`.

```python
def test_transformation_empty_source_table():
    """
    Purpose: Ensure the job handles an empty source table gracefully.
    """
    # Setup: SOF_TA_RN_EINZELN is empty (handled by fixture)
    _insert_rows(DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0)},
    ])

    # Action: Call SP
    success, error_msg = _call_sp("JOB1", "ENTRY1", "01012023", "0")
    assert success, f"SP call failed: {error_msg}"

    # Pass/Fail Criterion
    actual_output = _get_table_data(SOF_TA_RN_VERTRAG_TABLE)
    assert len(actual_output) == 0, "Target table should be empty."

    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2
    success_log = next((log for log in job_logs if log['status'] == 'SUCCESS'), None)
    assert success_log is not None
    assert success_log['records_processed'] == 0
```

---

### Test Case 8: Transformation - All `cntrct_id`s are Unique

*   **Purpose:** Verify the job works correctly when no aggregation is effectively needed (each `cntrct_id` in `SOF_TA_RN_EINZELN` has only one source row). This ensures the `GROUP BY` clause doesn't introduce unexpected behavior for non-aggregated data.
*   **Setup:** `SOF_TA_RN_EINZELN` has unique `cntrct_id`s, each with a single row.
*   **Action:** Call the Stored Procedure with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The Stored Procedure completes successfully.
    2.  `SOF_TA_RN_VERTRAG` contains the same number of rows as `SOF_TA_RN_EINZELN`.
    3.  The content of `SOF_TA_RN_VERTRAG` (ordered by `cntrct_id`) exactly matches the `expected_output` (which is essentially the source data, with `MAX` applied to single values).

```python
def test_transformation_unique_contract_ids():
    """
    Purpose: Verify the job works correctly when no aggregation is effectively needed
             (each cntrct_id has only one source row).
    """
    # Setup: SOF_TA_RN_EINZELN has unique cntrct_ids
    source_data = [
        {"cntrct_id": "U1", "TN_TEL_msisdn": "1", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)},
        {"cntrct_id": "U2", "TN_TEL_msisdn": "2", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2)},
        {"cntrct_id": "U3", "TN_TEL_msisdn": "3", "TN_TEL_status": "C", "TN_TEL_valid_to": date(2023, 1, 3)},
    ]
    _insert_rows(SOF_TA_RN_EINZELN_TABLE, source_data)
    # Expected output is identical to source data (after MAX on single row)
    expected_output = [
        {"cntrct_id": "U1", "TN_multi_single": None, "TN_TEL_msisdn": "1", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": None, "TC_TEL_msisdn": None, "TC_TEL_status": None, "TC_TEL_valid_to": None,
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": None, "TB_TEL_msisdn": None, "TB_TEL_status": None, "TB_TEL_valid_to": None,
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": None, "MS_RN_1_status": None, "MS_RN_1_valid_to": None,
         "MS_RN_2_msisdn": None, "MS_RN_2_status": None, "MS_RN_2_valid_to": None},
        {"cntrct_id": "U2", "TN_multi_single": None, "TN_TEL_msisdn": "2", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": None, "TC_TEL_msisdn": None, "TC_TEL_status": None, "TC_TEL_valid_to": None,
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": None, "TB_TEL_msisdn": None, "TB_TEL_status": None, "TB_TEL_valid_to": None,
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": None, "MS_RN_1_status": None, "MS_RN_1_valid_to": None,
         "MS_RN_2_msisdn": None, "MS_RN_2_status": None, "MS_RN_2_valid_to": None},
        {"cntrct_id": "U3", "TN_multi_single": None, "TN_TEL_msisdn": "3", "TN_TEL_status": "C", "TN_TEL_valid_to": date(2023, 1, 3),
         "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None,
         "TN_DAT_msisdn": None, "TN_DAT_status": None, "TN_DAT_valid_to": None,
         "TC_multi_single": None, "TC_TEL_msisdn": None, "TC_TEL_status": None, "TC_TEL_valid_to": None,
         "TC_FAX_msisdn": None, "TC_FAX_status": None, "TC_FAX_valid_to": None,
         "TC_DAT_msisdn": None, "TC_DAT_status": None, "TC_DAT_valid_to": None,
         "TB_multi_single": None, "TB_TEL_msisdn": None, "TB_TEL_status": None, "TB_TEL_valid_to": None,
         "TB_FAX_msisdn": None, "TB_FAX_status": None, "TB_FAX_valid_to": None,
         "TB_DAT_msisdn": None, "TB_DAT_status": None, "TB_DAT_valid_to": None,
         "MS_RN_1_msisdn": None, "MS_RN_1_status": None, "MS_RN_1_valid_to": None,
         "MS_RN_2_msisdn": None, "MS_RN_2_status": None, "MS_RN_2_valid_to": None},
    ]

    # Action: Call SP
    success, error_msg = _call_sp("JOB1", "ENTRY1", "01012023", "0")
    assert success, f"SP call failed: {error_msg}"

    # Pass/Fail Criterion
    actual_output = _get_table_data(SOF_TA_RN_VERTRAG_TABLE, order_by="cntrct_id")
    assert len(actual_output) == len(expected_output), "Row count mismatch in target table."
    
    actual_output_sorted = sorted(actual_output, key=lambda x: x['cntrct_id'])
    expected_output_sorted = sorted(expected_output, key=lambda x: x['cntrct_id'])

    for i, actual_row in enumerate(actual_output_sorted):
        expected_row = expected_output_sorted[i]
        for col in expected_row:
            actual_value = actual_row.get(col)
            expected_value = expected_row.get(col)
            if isinstance(actual_value, datetime):
                actual_value = actual_value.date()
            assert actual_value == expected_value, \
                f"Mismatch in row {i}, column '{col}': Expected '{expected_value}', Got '{actual_value}'"
    
    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2
    success_log = next((log for log in job_logs if log['status'] == 'SUCCESS'), None)
    assert success_log is not None
    assert success_log['records_processed'] == len(expected_output)
```

---

### Test Case 9: Logging - Successful Run

*   **Purpose:** Verify that the `job_log` table is correctly updated with `SUCCESS` status, `records_processed` count, and timestamps for a successful execution.
*   **Setup:** Populate `SOF_TA_RN_EINZELN` with minimal valid data.
*   **Action:** Call the Stored Procedure with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The `job_log` table contains two entries for the run (one `RUNNING`, one `SUCCESS`).
    2.  The `SUCCESS` entry has `job_name` matching the SP, `status = 'SUCCESS'`, `records_processed` matching the number of rows inserted, and valid `start_time` and `end_time` values.

```python
def test_logging_successful_run():
    """
    Purpose: Verify job_log is correctly updated for a successful run.
    """
    # Setup: Minimal valid data
    _insert_rows(SOF_TA_RN_EINZELN_TABLE, [
        {"cntrct_id": "L1", "TN_TEL_msisdn": "1", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)},
    ])
    expected_records = 1

    # Action: Call SP
    success, error_msg = _call_sp("JOB_LOG_SUCCESS", "ENTRY_LOG", "01012023", "0")
    assert success, f"SP call failed: {error_msg}"

    # Pass/Fail Criterion: Verify job_log
    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2 # RUNNING and SUCCESS
    success_log = next((log for log in job_logs if log['status'] == 'SUCCESS'), None)
    assert success_log is not None
    assert success_log['job_name'] == SP_NAME
    assert success_log['status'] == 'SUCCESS'
    assert success_log['records_processed'] == expected_records
    assert success_log['message'] == 'Job completed successfully'
    assert success_log['start_time'] is not None
    assert success_log['end_time'] is not None
    assert success_log['end_time'] > success_log['start_time']
    assert success_log['run_id'] is not None
```

---

### Test Case 10: Logging - Failed Run

*   **Purpose:** Verify that both `job_log` and `error_log` tables are correctly updated for a failed execution, capturing the error details.
*   **Setup:** None.
*   **Action:** Call the Stored Procedure with invalid parameters (e.g., missing `p_job_kennung`) to force a failure.
*   **Pass/Fail Criterion:**
    1.  The `job_log` table contains two entries for the run (one `RUNNING`, one `FAILED`).
    2.  The `FAILED` entry has `job_name` matching the SP, `status = 'FAILED'`, `records_processed` as `NULL`, an error message, and valid `start_time` and `end_time` values.
    3.  The `error_log` table contains one entry for the run, with the `job_name`, `error_time`, `error_message`, and `stack_trace`.

```python
def test_logging_failed_run():
    """
    Purpose: Verify job_log and error_log are correctly updated for a failed run.
    """
    # Action: Call SP with invalid parameters (missing p_job_kennung)
    success, error_msg = _call_sp(None, "ENTRY_FAIL", "01012023", "0")
    assert not success, "SP call unexpectedly succeeded."

    # Pass/Fail Criterion: Verify job_log and error_log
    job_logs = _get_table_data(JOB_LOG_TABLE)
    assert len(job_logs) == 2 # RUNNING and FAILED
    failed_log = next((log for log in job_logs if log['status'] == 'FAILED'), None)
    assert failed_log is not None
    assert failed_log['job_name'] == SP_NAME
    assert failed_log['status'] == 'FAILED'
    assert failed_log['records_processed'] is None # No records processed on failure
    assert "Job failed: Parameter p_job_kennung cannot be NULL or empty." in failed_log['message']
    assert failed_log['start_time'] is not None
    assert failed_log['end_time'] is not None
    assert failed_log['end_time'] > failed_log['start_time']
    assert failed_log['run_id'] is not None

    error_logs = _get_table_data(ERROR_LOG_TABLE)
    assert len(error_logs) == 1
    assert error_logs[0]['job_name'] == SP_NAME
    assert "Parameter p_job_kennung cannot be NULL or empty." in error_logs[0]['error_message']
    assert error_logs[0]['error_time'] is not None
    assert error_logs[0]['run_id'] == failed_log['run_id']
    assert error_logs[0]['stack_trace'] is not None # Stack trace should be present
```

---

### Test Case 11: Schema Assertion for `SOF_TA_RN_VERTRAG`

*   **Purpose:** Verify that the schema of the target table `SOF_TA_RN_VERTRAG` in BigQuery matches the expected DDL, ensuring correct column names, data types, and nullability.
*   **Setup:** None (schema is static and created via DDL).
*   **Action:** Query BigQuery metadata for the `SOF_TA_RN_VERTRAG` table schema.
*   **Pass/Fail Criterion:** The retrieved schema (column names, types, and nullability) for `my_project.my_dataset.sof_ta_rn_vertrag` exactly matches the `expected_schema` defined based on `sof_ta_rn_vertrag_ddl.sql`.

```python
def test_schema_sof_ta_rn_vertrag():
    """
    Purpose: Verify the schema of the target table SOF_TA_RN_VERTRAG matches the expected DDL.
    """
    table = client.get_table(SOF_TA_RN_VERTRAG_TABLE)
    actual_schema = {field.name: (field.field_type, field.is_nullable) for field in table.schema}

    # Define expected schema based on sof_ta_rn_vertrag_ddl.sql
    expected_schema = {
        "cntrct_id": ("STRING", False),
        "TN_multi_single": ("STRING", True),
        "TN_TEL_msisdn": ("STRING", True),
        "TN_TEL_status": ("STRING", True),
        "TN_TEL_valid_to": ("DATE", True),
        "TN_FAX_msisdn": ("STRING", True),
        "TN_FAX_status": ("STRING", True),
        "TN_FAX_valid_to": ("DATE", True),
        "TN_DAT_msisdn": ("STRING", True),
        "TN_DAT_status": ("STRING", True),
        "TN_DAT_valid_to": ("DATE", True),
        "TC_multi_single": ("STRING", True),
        "TC_TEL_msisdn": ("STRING", True),
        "TC_TEL_status": ("STRING", True),
        "TC_TEL_valid_to": ("DATE", True),
        "TC_FAX_msisdn": ("STRING", True),
        "TC_FAX_status": ("STRING", True),
        "TC_FAX_valid_to": ("DATE", True),
        "TC_DAT_msisdn": ("STRING", True),
        "TC_DAT_status": ("STRING", True),
        "TC_DAT_valid_to": ("DATE", True),
        "TB_multi_single": ("STRING", True),
        "TB_TEL_msisdn": ("STRING", True),
        "TB_TEL_status": ("STRING", True),
        "TB_TEL_valid_to": ("DATE", True),
        "TB_FAX_msisdn": ("STRING", True),
        "TB_FAX_status": ("STRING", True),
        "TB_FAX_valid_to": ("DATE", True),
        "TB_DAT_msisdn": ("STRING", True),
        "TB_DAT_status": ("STRING", True),
        "TB_DAT_valid_to": ("DATE", True),
        "MS_RN_1_msisdn": ("STRING", True),
        "MS_RN_1_status": ("STRING", True),
        "MS_RN_1_valid_to": ("DATE", True),
        "MS_RN_2_msisdn": ("STRING", True),
        "MS_RN_2_status": ("STRING", True),
        "MS_RN_2_valid_to": ("DATE", True),
    }

    # Pass/Fail Criterion
    assert actual_schema == expected_schema, \
        f"Schema mismatch for {SOF_TA_RN_VERTRAG_TABLE}. Expected: {expected_schema}, Actual: {actual_schema}"
```

---

### Test Case 12: Idempotency / Truncate Behavior

*   **Purpose:** Verify that running the job multiple times with the same inputs produces the identical result in the target table. This confirms that the `TRUNCATE TABLE` statement correctly clears previous data before insertion.
*   **Setup:** Populate `SOF_TA_RN_EINZELN` with sample data.
*   **Action:**
    1.  Call the Stored Procedure once.
    2.  Record the content and row count of `SOF_TA_RN_VERTRAG`.
    3.  Call the Stored Procedure a second time with the exact same parameters.
*   **Pass/Fail Criterion:**
    1.  Both Stored Procedure calls complete successfully.
    2.  The row count in `SOF_TA_RN_VERTRAG` after the first run is identical to the row count after the second run.
    3.  The content of `SOF_TA_RN_VERTRAG` (ordered by `cntrct_id`) after the first run is identical to the content after the second run.
    4.  The `job_log` entries for both successful runs show the same `records_processed` count.

```python
def test_idempotency_truncate_behavior():
    """
    Purpose: Verify that running the job multiple times produces the same result,
             implying the TRUNCATE works as expected.
    """
    # Setup: Populate SOF_TA_RN_EINZELN with data
    source_data = [
        {"cntrct_id": "IDEM1", "TN_TEL_msisdn": "1", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)},
        {"cntrct_id": "IDEM2", "TN_TEL_msisdn": "2", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2)},
    ]
    _insert_rows(SOF_TA_RN_EINZELN_TABLE, source_data)

    # Action 1: Call SP first time
    success1, error_msg1 = _call_sp("JOB_IDEM", "ENTRY_IDEM", "01012023", "0")
    assert success1, f"First SP call failed: {error_msg1}"
    first_run_output = _get_table_data(SOF_TA_RN_VERTRAG_TABLE, order_by="cntrct_id")
    first_run_count = _get_row_count(SOF_TA_RN_VERTRAG_TABLE)

    # Action 2: Call SP second time
    success2, error_msg2 = _call_sp("JOB_IDEM", "ENTRY_IDEM", "01012023", "0")
    assert success2, f"Second SP call failed: {error_msg2}"
    second_run_output = _get_table_data(SOF_TA_RN_VERTRAG_TABLE, order_by="cntrct_id")
    second_run_count = _get_row_count(SOF_TA_RN_VERTRAG_TABLE)

    # Pass/Fail Criterion
    assert first_run_count == len(source_data), "First run row count mismatch."
    assert second_run_count == len(source_data), "Second run row count mismatch."
    
    # Compare content
    # Convert datetime objects in actual output to date objects for consistent comparison
    def _normalize_output(output_list):
        normalized = []
        for row in output_list:
            new_row = {}
            for k, v in row.items():
                if isinstance(v, datetime):
                    new_row[k] = v.date()
                else:
                    new_row[k] = v
            normalized.append(new_row)
        return normalized

    assert _normalize_output(first_run_output) == _normalize_output(second_run_output), \
        "Output tables are not identical after two runs."

    # Verify job_log entries for records_processed
    job_logs = _get_table_data(JOB_LOG_TABLE)
    success_logs = [log for log in job_logs if log['status'] == 'SUCCESS']
    assert len(success_logs) == 2 # Two successful runs
    assert success_logs[0]['records_processed'] == len(source_data)
    assert success_logs[1]['records_processed'] == len(source_data)
```