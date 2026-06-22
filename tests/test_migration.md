As a senior data-migration QA engineer, I've developed a suite of validation tests for the `k_ausd_v_ta_action_assoc.ksh` job migration to BigQuery. These tests aim to ensure the new BigQuery implementation is behaviourally equivalent to the legacy system, covering output parity, transformation correctness, external system replacements, and data quality.

The tests are structured using `pytest` for orchestration and `google-cloud-bigquery` client for interacting with BigQuery, allowing for programmatic setup of test data and assertion of results.

---

### Prerequisites for Running Tests

1.  **GCP Project and Dataset**: A Google Cloud Project and a BigQuery dataset must be available for testing. The `PROJECT_ID` and `DATASET_ID` placeholders in the test code should be replaced with actual values or configured via environment variables.
2.  **BigQuery Client Library**: The `google-cloud-bigquery` Python library must be installed (`pip install google-cloud-bigquery`).
3.  **Authentication**: Your environment must be authenticated to GCP (e.g., via `gcloud auth application-default login` or by setting `GOOGLE_APPLICATION_CREDENTIALS`).
4.  **Airflow Environment**: For the Airflow DAG execution test, an Airflow environment with the `google_cloud_default` connection configured is required.

### Test Setup Helper Code

The following Python code provides the necessary fixtures and helper functions for setting up and tearing down BigQuery tables, inserting test data, executing the migrated job, and fetching results. This code should be placed in a `test_ta_action_assoc_migration.py` file.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta
import os
import time

# --- Configuration ---
# Replace with your actual GCP Project ID and BigQuery Dataset ID
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("GCP_DATASET_ID", "your_test_dataset")

# Fully qualified table names for the BigQuery environment
DWTK_MELDUNGEN_TABLE = f"{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen"
CDS_TA_ACTION_ASSOC_TABLE = f"{PROJECT_ID}.{DATASET_ID}.cds_ta_action_assoc"
SOF_TA_ACTION_ASSOC_TABLE = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_action_assoc"

# The BigQuery SQL job to be tested, with fully qualified table names
BIGQUERY_JOB_SQL = """
BEGIN
  DECLARE v_datum STRING;
  SET v_datum = (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `{dwtk_meldungen_table}` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  TRUNCATE TABLE `{sof_ta_action_assoc_table}`;

  INSERT INTO `{sof_ta_action_assoc_table}`(cntrct_id, rv_action_id)
  SELECT
    ac.cntrct_id,
    ac.rv_action_id
  FROM `{cds_ta_action_assoc_table}` ac
  WHERE DATE(ac.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
    AND DATE(ac.valid_from) <= PARSE_DATE('%Y%m%d', v_datum)
    AND ac.is_production = 1
    AND (ac.modified_at IS NULL OR DATE(ac.modified_at) > PARSE_DATE('%Y%m%d', v_datum))
    AND (ac.valid_to IS NULL OR DATE(ac.valid_to) > PARSE_DATE('%Y%m%d', v_datum));
END;
""".format(
    dwtk_meldungen_table=DWTK_MELDUNGEN_TABLE,
    cds_ta_action_assoc_table=CDS_TA_ACTION_ASSOC_TABLE,
    sof_ta_action_assoc_table=SOF_TA_ACTION_ASSOC_TABLE
)

# --- Pytest Fixtures and Helper Functions ---

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    client = bigquery.Client(project=PROJECT_ID)
    # Ensure dataset exists
    try:
        client.get_dataset(DATASET_ID)
    except Exception:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        client.create_dataset(dataset, exists_ok=True)
    yield client

@pytest.fixture(autouse=True)
def setup_teardown_tables(bq_client):
    """Clears and creates tables before each test, then cleans up."""
    # Define schemas
    dwtk_meldungen_schema = [
        bigquery.SchemaField("job_kennung", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timecreated", "TIMESTAMP", mode="NULLABLE"),
    ]
    cds_ta_action_assoc_schema = [
        bigquery.SchemaField("cntrct_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("rv_action_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("insert_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("valid_from", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("is_production", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("modified_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("valid_to", "TIMESTAMP", mode="NULLABLE"),
    ]
    sof_ta_action_assoc_schema = [
        bigquery.SchemaField("cntrct_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("rv_action_id", "INT64", mode="REQUIRED"),
    ]

    # Create tables (or ensure they exist with correct schema) and truncate data
    for table_id, schema in [
        (DWTK_MELDUNGEN_TABLE, dwtk_meldungen_schema),
        (CDS_TA_ACTION_ASSOC_TABLE, cds_ta_action_assoc_schema),
        (SOF_TA_ACTION_ASSOC_TABLE, sof_ta_action_assoc_schema),
    ]:
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table, exists_ok=True)
        bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()

    yield # Run the test

    # Teardown: Truncate tables after each test
    for table_id in [DWTK_MELDUNGEN_TABLE, CDS_TA_ACTION_ASSOC_TABLE, SOF_TA_ACTION_ASSOC_TABLE]:
        bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()

def insert_data(bq_client, table_id, rows):
    """Helper to insert data into a BigQuery table."""
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"Errors inserting rows into {table_id}: {errors}")
    # Give BigQuery a moment to reflect the inserts, especially for subsequent queries
    time.sleep(1)

def execute_job(bq_client):
    """Executes the BigQuery job SQL."""
    query_job = bq_client.query(BIGQUERY_JOB_SQL)
    query_job.result() # Wait for the job to complete

def fetch_target_data(bq_client):
    """Fetches all data from the target table, ordered for consistent comparison."""
    query_job = bq_client.query(f"SELECT cntrct_id, rv_action_id FROM `{SOF_TA_ACTION_ASSOC_TABLE}` ORDER BY cntrct_id, rv_action_id")
    return [dict(row) for row in query_job.result()]

def get_row_count(bq_client, table_id):
    """Gets the row count of a table."""
    query_job = bq_client.query(f"SELECT COUNT(*) FROM `{table_id}`")
    return list(query_job.result())[0][0]

def get_v_datum_value(bq_client):
    """Helper to get the v_datum value that the job would calculate."""
    query = """
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `{dwtk_meldungen_table}` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """.format(dwtk_meldungen_table=DWTK_MELDUNGEN_TABLE)
    query_job = bq_client.query(query)
    return list(query_job.result())[0][0]

```

---

### Test Cases

#### Test Case 1: Basic Output Parity - All Conditions Met

*   **Purpose**: Verify the job correctly processes records when all filtering conditions are met, ensuring basic output parity with the legacy system.
*   **Setup**:
    *   `dwtk_meldungen` table contains a record for `BERT_DROP_TEMP_TABLE` with `timecreated` set to `2023-01-15 10:00:00 UTC`. This will set `v_datum` to '20230115'.
    *   `cds_ta_action_assoc` table contains one record where `insert_at`, `valid_from` are before or on `v_datum`, `is_production` is 1, and `modified_at`, `valid_to` are NULL or after `v_datum`.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The `sof_ta_action_assoc` table must contain exactly one record: `{'cntrct_id': 101, 'rv_action_id': 201}`.

```python
def test_output_parity_all_conditions_met(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
        {"job_kennung": "OTHER_JOB", "timecreated": datetime(2023, 1, 14, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": None,
            "valid_to": None
        },
        # This record should be filtered out (is_production = 0)
        {
            "cntrct_id": 102, "rv_action_id": 202,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 0,
            "modified_at": None,
            "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1, "Target table row count mismatch"

```

#### Test Case 2: `v_datum` Determination - Max `timecreated`

*   **Purpose**: Verify that `v_datum` is correctly determined as the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from `dwtk_meldungen`.
*   **Setup**: `dwtk_meldungen` table contains multiple records, with varying `timecreated` and `job_kennung` values.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The `v_datum` calculated internally by the job (which can be verified by inspecting job logs or by running a separate query to determine what `v_datum` *should* be) must be '20230120'. The `sof_ta_action_assoc` table should reflect filtering based on this `v_datum`.

```python
def test_v_datum_max_timecreated(bq_client):
    # Setup dwtk_meldungen to ensure v_datum is '20230120'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
        {"job_kennung": "OTHER_JOB", "timecreated": datetime(2023, 1, 25, 10, 0, 0).isoformat()}, # Should be ignored
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 20, 12, 0, 0).isoformat()}, # Max for BERT_DROP_TEMP_TABLE
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 18, 11, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc: one record should pass with v_datum = '20230120'
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 19, 0, 0, 0).isoformat(), # <= 20230120
            "valid_from": datetime(2023, 1, 19, 0, 0, 0).isoformat(), # <= 20230120
            "is_production": 1,
            "modified_at": None,
            "valid_to": None
        },
        {
            "cntrct_id": 102, "rv_action_id": 202,
            "insert_at": datetime(2023, 1, 21, 0, 0, 0).isoformat(), # > 20230120, should be filtered
            "valid_from": datetime(2023, 1, 19, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": None,
            "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_v_datum_value(bq_client) == '20230120', "v_datum was not calculated correctly"

```

#### Test Case 3: `v_datum` Determination - Default Value

*   **Purpose**: Verify that `v_datum` defaults to '19000101' when no records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` exist in `dwtk_meldungen`.
*   **Setup**: `dwtk_meldungen` table is empty or contains no records with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The `v_datum` calculated internally by the job must be '19000101'. The `sof_ta_action_assoc` table should be empty, as all `insert_at` and `valid_from` dates will be after '19000101'.

```python
def test_v_datum_default_value(bq_client):
    # Setup dwtk_meldungen with no relevant records
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "OTHER_JOB_1", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
        {"job_kennung": "OTHER_JOB_2", "timecreated": datetime(2023, 1, 14, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc: all records should be filtered out by v_datum = '19000101'
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": None,
            "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = []
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 0, "Target table should be empty"
    assert get_v_datum_value(bq_client) == '19000101', "v_datum was not defaulted correctly"

```

#### Test Case 4: Filtering - `insert_at` Condition

*   **Purpose**: Verify that records are filtered out if their `insert_at` date is strictly after `v_datum`.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `insert_at` = '2023-01-16' (after `v_datum`) and another with `insert_at` = '2023-01-15' (on `v_datum`).
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: Only the record with `insert_at` on or before `v_datum` should be present in `sof_ta_action_assoc`.

```python
def test_filtering_insert_at(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 15, 0, 0, 0).isoformat(), # <= v_datum, should pass
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1, "modified_at": None, "valid_to": None
        },
        {
            "cntrct_id": 102, "rv_action_id": 202,
            "insert_at": datetime(2023, 1, 16, 0, 0, 0).isoformat(), # > v_datum, should be filtered
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1, "modified_at": None, "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 5: Filtering - `valid_from` Condition

*   **Purpose**: Verify that records are filtered out if their `valid_from` date is strictly after `v_datum`.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `valid_from` = '2023-01-16' (after `v_datum`) and another with `valid_from` = '2023-01-15' (on `v_datum`).
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: Only the record with `valid_from` on or before `v_datum` should be present in `sof_ta_action_assoc`.

```python
def test_filtering_valid_from(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 15, 0, 0, 0).isoformat(), # <= v_datum, should pass
            "is_production": 1, "modified_at": None, "valid_to": None
        },
        {
            "cntrct_id": 102, "rv_action_id": 202,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 16, 0, 0, 0).isoformat(), # > v_datum, should be filtered
            "is_production": 1, "modified_at": None, "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 6: Filtering - `is_production` Condition

*   **Purpose**: Verify that only records with `is_production = 1` are included.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `is_production = 1` and another with `is_production = 0`.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: Only the record with `is_production = 1` should be present in `sof_ta_action_assoc`.

```python
def test_filtering_is_production(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1, # Should pass
            "modified_at": None, "valid_to": None
        },
        {
            "cntrct_id": 102, "rv_action_id": 202,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 0, # Should be filtered
            "modified_at": None, "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 7: Filtering - `modified_at` Condition (NULL)

*   **Purpose**: Verify that records with `modified_at` as NULL are included.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `modified_at` set to NULL.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The record with `modified_at` as NULL should be present in `sof_ta_action_assoc`.

```python
def test_filtering_modified_at_null(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": None, # Should pass
            "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 8: Filtering - `modified_at` Condition (Greater than `v_datum`)

*   **Purpose**: Verify that records with `modified_at` strictly greater than `v_datum` are included.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `modified_at` = '2023-01-16' (after `v_datum`).
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The record with `modified_at` greater than `v_datum` should be present in `sof_ta_action_assoc`.

```python
def test_filtering_modified_at_greater_than_v_datum(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": datetime(2023, 1, 16, 0, 0, 0).isoformat(), # > v_datum, should pass
            "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 9: Filtering - `modified_at` Condition (Less than or equal to `v_datum`)

*   **Purpose**: Verify that records with `modified_at` less than or equal to `v_datum` are filtered out.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `modified_at` = '2023-01-15' (on `v_datum`).
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The record with `modified_at` on or before `v_datum` should *not* be present in `sof_ta_action_assoc`.

```python
def test_filtering_modified_at_less_than_or_equal_v_datum(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": datetime(2023, 1, 15, 0, 0, 0).isoformat(), # <= v_datum, should be filtered
            "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = []
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 0

```

#### Test Case 10: Filtering - `valid_to` Condition (NULL)

*   **Purpose**: Verify that records with `valid_to` as NULL are included.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `valid_to` set to NULL.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The record with `valid_to` as NULL should be present in `sof_ta_action_assoc`.

```python
def test_filtering_valid_to_null(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": None,
            "valid_to": None # Should pass
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 11: Filtering - `valid_to` Condition (Greater than `v_datum`)

*   **Purpose**: Verify that records with `valid_to` strictly greater than `v_datum` are included.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `valid_to` = '2023-01-16' (after `v_datum`).
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The record with `valid_to` greater than `v_datum` should be present in `sof_ta_action_assoc`.

```python
def test_filtering_valid_to_greater_than_v_datum(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": None,
            "valid_to": datetime(2023, 1, 16, 0, 0, 0).isoformat() # > v_datum, should pass
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 12: Filtering - `valid_to` Condition (Less than or equal to `v_datum`)

*   **Purpose**: Verify that records with `valid_to` less than or equal to `v_datum` are filtered out.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record with `valid_to` = '2023-01-15' (on `v_datum`).
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The record with `valid_to` on or before `v_datum` should *not* be present in `sof_ta_action_assoc`.

```python
def test_filtering_valid_to_less_than_or_equal_v_datum(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1,
            "modified_at": None,
            "valid_to": datetime(2023, 1, 15, 0, 0, 0).isoformat() # <= v_datum, should be filtered
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = []
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 0

```

#### Test Case 13: Edge Case - Dates Exactly Equal to `v_datum`

*   **Purpose**: Verify that records with `insert_at` and `valid_from` dates exactly equal to `v_datum` are included, confirming the inclusive nature of the `<=` operator.
*   **Setup**:
    *   `v_datum` is '20230115'.
    *   `cds_ta_action_assoc` contains one record where `insert_at` and `valid_from` are both '2023-01-15'.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The record should be present in `sof_ta_action_assoc`.

```python
def test_edge_case_dates_equal_v_datum(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 15, 0, 0, 0).isoformat(), # Equal to v_datum, should pass
            "valid_from": datetime(2023, 1, 15, 0, 0, 0).isoformat(), # Equal to v_datum, should pass
            "is_production": 1,
            "modified_at": None,
            "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1

```

#### Test Case 14: No Source Data

*   **Purpose**: Verify the job runs successfully and results in an empty target table when the source `cds_ta_action_assoc` table is empty.
*   **Setup**:
    *   `dwtk_meldungen` contains a valid record for `v_datum`.
    *   `cds_ta_action_assoc` table is empty.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The `sof_ta_action_assoc` table must be empty.

```python
def test_no_source_data(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # cds_ta_action_assoc is empty by fixture setup_teardown_tables

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = []
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 0, "Target table should be empty"

```

#### Test Case 15: Target Table Truncation

*   **Purpose**: Verify that the `sof_ta_action_assoc` table is truncated before new data is inserted, preventing duplicate or stale records.
*   **Setup**:
    *   `dwtk_meldungen` contains a valid record for `v_datum`.
    *   `cds_ta_action_assoc` contains one record that should be inserted.
    *   `sof_ta_action_assoc` initially contains a "stale" record that should be removed by truncation.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**: The `sof_ta_action_assoc` table must contain only the record from `cds_ta_action_assoc`, and the "stale" record must be absent.

```python
def test_target_table_truncation(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Pre-populate target table with a "stale" record
    insert_data(bq_client, SOF_TA_ACTION_ASSOC_TABLE, [
        {"cntrct_id": 999, "rv_action_id": 888}
    ])
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1, "Pre-population failed"

    # Setup cds_ta_action_assoc with one valid record
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 101, "rv_action_id": 201,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1, "modified_at": None, "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion
    expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"
    assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1, "Target table should only contain new data"

```

#### Test Case 16: Schema and Column Mapping

*   **Purpose**: Verify that the target table `sof_ta_action_assoc` has the expected schema and that `cntrct_id` and `rv_action_id` are correctly mapped from the source.
*   **Setup**:
    *   `dwtk_meldungen` contains a valid record for `v_datum`.
    *   `cds_ta_action_assoc` contains one record with specific `cntrct_id` and `rv_action_id` values.
*   **Action**: Execute the BigQuery job SQL.
*   **Pass/Fail Criterion**:
    1.  The `sof_ta_action_assoc` table schema must contain `cntrct_id` (INT64) and `rv_action_id` (INT64).
    2.  The inserted record in `sof_ta_action_assoc` must have the exact `cntrct_id` and `rv_action_id` values from the source.

```python
def test_schema_and_column_mapping(bq_client):
    # Setup dwtk_meldungen for v_datum = '20230115'
    insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
    ])

    # Setup cds_ta_action_assoc
    insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
        {
            "cntrct_id": 500, "rv_action_id": 600,
            "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
            "is_production": 1, "modified_at": None, "valid_to": None
        }
    ])

    # Action
    execute_job(bq_client)

    # Pass/Fail Criterion 1: Schema check
    table = bq_client.get_table(SOF_TA_ACTION_ASSOC_TABLE)
    schema_fields = {field.name: field.field_type for field in table.schema}
    assert 'cntrct_id' in schema_fields and schema_fields['cntrct_id'] == 'INT64'
    assert 'rv_action_id' in schema_fields and schema_fields['rv_action_id'] == 'INT64'
    assert len(schema_fields) == 2, "Target table has unexpected columns"

    # Pass/Fail Criterion 2: Column mapping
    expected_data = [{'cntrct_id': 500, 'rv_action_id': 600}]
    actual_data = fetch_target_data(bq_client)
    assert actual_data == expected_data, f"Expected {expected_data}, got {actual_data}"

```

#### Test Case 17: Airflow DAG Execution (External System Replacement)

*   **Purpose**: Verify that the Airflow DAG successfully triggers the BigQuery job, replacing the legacy UNIX host and KornShell orchestration. This is an integration test for the Airflow component.
*   **Setup**:
    *   An Airflow environment is running.
    *   The `dw_bert_ausd_v_ta_action_assoc_dag.py` DAG is deployed to Airflow.
    *   The `google_cloud_default` connection is configured in Airflow with appropriate BigQuery permissions.
    *   Source BigQuery tables (`dwtk_meldungen`, `cds_ta_action_assoc`) are populated with test data (e.g., similar to Test Case 1).
*   **Action**: Manually trigger the `dw_bert_ausd_v_ta_action_assoc_dag` in the Airflow UI or via the Airflow CLI.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run completes successfully (all tasks green).
    2.  The `execute_ta_action_assoc_update` task logs indicate successful BigQuery job execution.
    3.  A post-execution query against `my_gcp_project.my_dataset.sof_ta_action_assoc` confirms the expected data has been loaded, matching the output parity tests.

```python
# This is a conceptual test case, as direct Airflow DAG triggering and status checking
# from a pytest script is complex and usually handled by Airflow's own testing utilities
# or manual observation in a CI/CD pipeline.

# Example of how you might verify the outcome after triggering the DAG:

# def test_airflow_dag_execution_and_output(bq_client):
#     # This test assumes the Airflow DAG has been triggered externally
#     # and has completed its run. In a real CI/CD, this would be a separate
#     # step after DAG deployment and triggering.

#     # Setup source data (same as Test Case 1 for consistency)
#     insert_data(bq_client, DWTK_MELDUNGEN_TABLE, [
#         {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 15, 10, 0, 0).isoformat()},
#     ])
#     insert_data(bq_client, CDS_TA_ACTION_ASSOC_TABLE, [
#         {
#             "cntrct_id": 101, "rv_action_id": 201,
#             "insert_at": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
#             "valid_from": datetime(2023, 1, 10, 0, 0, 0).isoformat(),
#             "is_production": 1,
#             "modified_at": None,
#             "valid_to": None
#         }
#     ])

#     # --- Manual Action / CI Trigger Point ---
#     # At this point, the Airflow DAG 'dw_bert_ausd_v_ta_action_assoc_dag'
#     # would be triggered. The test would then wait for its completion
#     # or be run as a separate post-DAG-execution verification step.
#     # For demonstration, we'll simulate the job execution directly.
#     # In a real scenario, you'd skip execute_job(bq_client) here and
#     # directly fetch_target_data after the DAG run.
#     execute_job(bq_client) # Simulating DAG's BigQueryOperator task

#     # Pass/Fail Criterion
#     expected_data = [{'cntrct_id': 101, 'rv_action_id': 201}]
#     actual_data = fetch_target_data(bq_client)
#     assert actual_data == expected_data, f"Airflow DAG output mismatch. Expected {expected_data}, got {actual_data}"
#     assert get_row_count(bq_client, SOF_TA_ACTION_ASSOC_TABLE) == 1, "Airflow DAG target table row count mismatch"

```