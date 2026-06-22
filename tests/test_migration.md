The migration of `DW.BERT_AUSD_BP_TA_RN_VERTRAG` involves significant changes across orchestration, scripting, and data storage. The following test cases are designed to ensure the migrated job is functionally equivalent and robust in its new Google Cloud environment.

---

## 1. Output Parity Tests

### Test Case 1.1: End-to-End Data Parity (Full Load)

*   **Purpose**: To verify that for identical input data, the migrated job produces the exact same output data in BigQuery as the legacy job produced in Oracle. This is the most critical test for behavioral equivalence.
*   **Setup**:
    1.  **Data Snapshot**: Capture a full snapshot of the Oracle source tables `SOF$TA_RN_EINZELN` and `DWTK_MELDUNGEN` at a specific point in time.
    2.  **Legacy Run**: Execute the legacy `DW.BERT_AUSD_BP_TA_RN_VERTRAG` job in the Oracle environment using the captured input data. Record the `Stichtag` used.
    3.  **Oracle Output Capture**: Extract the entire content of the Oracle target table `SOF$TA_RN_VERTRAG` into a CSV or JSON file.
    4.  **BigQuery Input Load**: Load the captured Oracle `SOF$TA_RN_EINZELN` and `DWTK_MELDUNGEN` data into their respective BigQuery counterparts: `isbert_schema.SOF_TA_RN_EINZELN` and `isbert_schema.DWTK_MELDUNGEN`.
    5.  **Migrated Run**: Trigger the Airflow DAG `dw_bert_ausd_bp_ta_rn_vertrag` with the same `Stichtag` parameter as used in the legacy run.
*   **Action**: After the migrated job completes, query the `isbert_schema.SOF_TA_RN_VERTRAG` table in BigQuery.
*   **Pass/Fail Criterion**:
    *   The row count of `isbert_schema.SOF_TA_RN_VERTRAG` must be identical to the row count of the captured Oracle `SOF$TA_RN_VERTRAG`.
    *   A full data comparison (e.g., using checksums, hash values per row, or direct row-by-row comparison after ordering) between the BigQuery output and the captured Oracle output must show no differences. All column values must match exactly, considering potential data type conversions (e.g., `NUMBER` to `INT64`/`BIGNUMERIC`, `VARCHAR2` to `STRING`).

```python
# Example Python (pytest) assertion for data parity
import pandas as pd
from google.cloud import bigquery
import hashlib

def calculate_df_checksum(df: pd.DataFrame) -> str:
    """Calculates a checksum for a DataFrame for comparison."""
    # Sort by all columns to ensure consistent order for checksum
    df_sorted = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    # Convert all columns to string to handle mixed types consistently
    df_str = df_sorted.astype(str)
    # Concatenate all string values and hash
    full_string = df_str.apply(lambda x: ''.join(x), axis=1).str.cat(sep='|')
    return hashlib.md5(full_string.encode('utf-8')).hexdigest()

def test_end_to_end_data_parity(bigquery_client, oracle_client, gcp_project_id, bq_dataset_id):
    """
    Tests full data parity between legacy Oracle output and migrated BigQuery output.
    Assumes setup steps (data loading, job runs) have been completed.
    """
    # --- Setup (conceptual, actual setup would involve external tools/scripts) ---
    # 1. Load Oracle snapshot data into BQ source tables (SOF_TA_RN_EINZELN, DWTK_MELDUNGEN)
    # 2. Run legacy Oracle job, capture output to 'legacy_oracle_output.csv'
    # 3. Trigger Airflow DAG for migrated job

    # --- Action & Assertion ---
    # Load captured Oracle output
    legacy_df = pd.read_csv("legacy_oracle_output.csv")

    # Query BigQuery output
    bq_query = f"""
        SELECT * FROM `{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG`
        ORDER BY CNTRCT_ID -- Ensure consistent ordering for comparison
    """
    bq_df = bigquery_client.query(bq_query).to_dataframe()

    # Ensure column names and types are consistent for comparison
    # (e.g., convert BQ column names to match Oracle's if necessary, handle case sensitivity)
    bq_df.columns = [col.upper() for col in bq_df.columns] # Example: make BQ columns uppercase

    # Check row counts
    assert len(legacy_df) == len(bq_df), \
        f"Row count mismatch: Legacy={len(legacy_df)}, Migrated={len(bq_df)}"

    # Check data parity using checksums
    legacy_checksum = calculate_df_checksum(legacy_df)
    bq_checksum = calculate_df_checksum(bq_df)

    assert legacy_checksum == bq_checksum, \
        "Data content mismatch between legacy Oracle and migrated BigQuery output."

    print(f"Legacy row count: {len(legacy_df)}, Migrated row count: {len(bq_df)}")
    print(f"Legacy checksum: {legacy_checksum}")
    print(f"Migrated checksum: {bq_checksum}")

# Note: bigquery_client, oracle_client, gcp_project_id, bq_dataset_id would be fixtures
# providing authenticated clients and configuration.
```

---

## 2. Transformation Correctness Tests

### Test Case 2.1: Aggregation Logic (`MAX` function and `GROUP BY`)

*   **Purpose**: To verify that the `MAX()` aggregation across various fields and the `GROUP BY CNTRCT_ID` logic is correctly translated from Oracle SQL to BigQuery SQL and behaves identically. This is crucial for the "consolidated view" purpose.
*   **Setup**:
    1.  **Controlled Input**: Create a small, controlled dataset for `isbert_schema.SOF_TA_RN_EINZELN` in BigQuery. This dataset should include:
        *   Multiple rows for the same `CNTRCT_ID` with varying values for aggregated columns (e.g., `TN_TEL_msisdn`, `TN_FAX_status`, `MS_RN_1_msisdn`).
        *   Scenarios with `NULL` values mixed with non-`NULL` values within a `CNTRCT_ID` group.
        *   Scenarios where all values for an aggregated column within a `CNTRCT_ID` group are `NULL`.
        *   Different data types (e.g., numbers, strings, dates) in aggregated columns to test `MAX` behavior across types.
    2.  **Expected Output**: Manually calculate the expected output for `isbert_schema.SOF_TA_RN_VERTRAG` based on Oracle's `MAX` function behavior for the controlled input.
*   **Action**: Trigger the Airflow DAG `dw_bert_ausd_bp_ta_rn_vertrag` (or directly execute the BigQuery SQL transformation). Query `isbert_schema.SOF_TA_RN_VERTRAG`.
*   **Pass/Fail Criterion**:
    *   For each `CNTRCT_ID`, the values in `isbert_schema.SOF_TA_RN_VERTRAG` for all aggregated columns must exactly match the manually calculated expected output.
    *   Specifically, `MAX(NULL, value)` should result in `value`, and `MAX(NULL, NULL)` should result in `NULL`, consistent with SQL `MAX` behavior.

```python
# Example Python (pytest) assertion for aggregation logic
from google.cloud import bigquery
import pandas as pd

def test_aggregation_logic(bigquery_client, gcp_project_id, bq_dataset_id):
    """
    Tests the MAX() aggregation and GROUP BY CNTRCT_ID logic.
    """
    source_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_EINZELN`"
    target_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG`"

    # --- Setup: Controlled Input Data ---
    # Clear existing data in source and target for a clean test
    bigquery_client.query(f"TRUNCATE TABLE {source_table}").result()
    bigquery_client.query(f"TRUNCATE TABLE {target_table}").result()

    # Insert controlled test data into SOF_TA_RN_EINZELN
    test_data = [
        # CNTRCT_ID 1: Mixed values, NULLs, different types
        (101, '111', 'ACTIVE', '222', 'OPEN', '333', 'ON', '444', 'OFF'),
        (101, '112', 'INACTIVE', None, 'CLOSED', '334', 'OFF', '445', 'ON'),
        (101, '110', 'ACTIVE', '220', 'OPEN', None, 'ON', '440', 'OFF'),
        # CNTRCT_ID 2: All NULLs for some columns
        (102, '555', 'ACTIVE', None, 'OPEN', '666', 'ON', None, 'OFF'),
        (102, '550', 'ACTIVE', None, 'OPEN', '660', 'ON', None, 'OFF'),
        # CNTRCT_ID 3: Single row
        (103, '777', 'ACTIVE', '888', 'OPEN', '999', 'ON', '000', 'OFF'),
    ]
    # Assuming a helper to insert data, or direct BQ insert
    # For simplicity, let's assume direct SQL insert for test data
    insert_sql = f"""
        INSERT INTO {source_table} (CNTRCT_ID, TN_TEL_msisdn, TN_TEL_status, TN_FAX_msisdn, TN_FAX_status, MS_RN_1_msisdn, MS_RN_1_status, MS_RN_2_msisdn, MS_RN_2_status) VALUES
        {', '.join([str(row) for row in test_data])}
    """
    bigquery_client.query(insert_sql).result()

    # --- Action: Run the migrated job (conceptual) ---
    # In a real test, you'd trigger the Airflow DAG or call the dataproc_job.py script
    # For this example, we'll simulate by directly executing the core SQL
    core_sql_template = """
        TRUNCATE TABLE {target_table};
        INSERT INTO {target_table} (
            CNTRCT_ID, TN_TEL_msisdn, TN_TEL_status, TN_FAX_msisdn, TN_FAX_status,
            MS_RN_1_msisdn, MS_RN_1_status, MS_RN_2_msisdn, MS_RN_2_status
        )
        SELECT
            s.CNTRCT_ID,
            MAX(s.TN_TEL_msisdn) AS TN_TEL_msisdn,
            MAX(s.TN_TEL_status) AS TN_TEL_status,
            MAX(s.TN_FAX_msisdn) AS TN_FAX_msisdn,
            MAX(s.TN_FAX_status) AS TN_FAX_status,
            MAX(s.MS_RN_1_msisdn) AS MS_RN_1_msisdn,
            MAX(s.MS_RN_1_status) AS MS_RN_1_status,
            MAX(s.MS_RN_2_msisdn) AS MS_RN_2_msisdn,
            MAX(s.MS_RN_2_status) AS MS_RN_2_status
        FROM {source_table} AS s
        GROUP BY s.CNTRCT_ID;
    """
    bigquery_client.query(core_sql_template.format(source_table=source_table, target_table=target_table)).result()

    # --- Assertion: Query and Compare ---
    result_df = bigquery_client.query(f"SELECT * FROM {target_table} ORDER BY CNTRCT_ID").to_dataframe()

    # Expected results based on MAX logic
    expected_data = pd.DataFrame([
        # CNTRCT_ID 101: MAX of '110', '111', '112' is '112'. MAX of 'INACTIVE', 'ACTIVE' is 'INACTIVE' (lexicographical).
        # For TN_FAX_msisdn, MAX(None, '220', '222') is '222'.
        # For MS_RN_1_msisdn, MAX(None, '333', '334') is '334'.
        # For MS_RN_2_msisdn, MAX('440', '444', '445') is '445'.
        {'CNTRCT_ID': 101, 'TN_TEL_msisdn': '112', 'TN_TEL_status': 'INACTIVE', 'TN_FAX_msisdn': '222', 'TN_FAX_status': 'OPEN', 'MS_RN_1_msisdn': '334', 'MS_RN_1_status': 'ON', 'MS_RN_2_msisdn': '445', 'MS_RN_2_status': 'OFF'},
        # CNTRCT_ID 102: MAX of '550', '555' is '555'. MAX of None is None.
        {'CNTRCT_ID': 102, 'TN_TEL_msisdn': '555', 'TN_TEL_status': 'ACTIVE', 'TN_FAX_msisdn': None, 'TN_FAX_status': 'OPEN', 'MS_RN_1_msisdn': '666', 'MS_RN_1_status': 'ON', 'MS_RN_2_msisdn': None, 'MS_RN_2_status': 'OFF'},
        # CNTRCT_ID 103: Single row, so values are as is.
        {'CNTRCT_ID': 103, 'TN_TEL_msisdn': '777', 'TN_TEL_status': 'ACTIVE', 'TN_FAX_msisdn': '888', 'TN_FAX_status': 'OPEN', 'MS_RN_1_msisdn': '999', 'MS_RN_1_status': 'ON', 'MS_RN_2_msisdn': '000', 'MS_RN_2_status': 'OFF'},
    ])
    expected_data = expected_data.sort_values(by='CNTRCT_ID').reset_index(drop=True)

    # Convert result_df columns to match expected_data types if necessary (e.g., BQ INT64 to Python int)
    # For direct comparison, ensure dtypes are compatible or convert to string
    pd.testing.assert_frame_equal(result_df, expected_data, check_dtype=False, check_like=True)
    print("Aggregation logic test passed.")
```

### Test Case 2.2: `Stichtag` Parameter Handling (Design vs. Implementation Discrepancy)

*   **Purpose**: To verify that the `Stichtag` parameter is correctly parsed, validated, and *passed to the BigQuery SQL transformation* as specified in the design document.
*   **Setup**:
    1.  **Scenario A (Explicit `Stichtag`)**: Trigger the Airflow DAG with an explicit `Stichtag` (e.g., `20230115`).
    2.  **Scenario B (Default `Stichtag`)**: Trigger the Airflow DAG without providing a `Stichtag` parameter.
    3.  **Scenario C (Invalid `Stichtag`)**: Trigger the Airflow DAG with an invalid `Stichtag` format (e.g., `2023-01-15`).
*   **Action**:
    *   **A & B**: Review the Dataproc job logs for the `dataproc_job.py` script to confirm the `Stichtag` value being used. **Crucially, inspect the BigQuery SQL executed by the job to confirm if and how `Stichtag` is incorporated (e.g., in a `WHERE` clause).**
    *   **C**: Observe the Airflow task status and Dataproc job logs for error messages.
*   **Pass/Fail Criterion**:
    *   **A**: The `Stichtag` logged by `dataproc_job.py` must match the input `20230115`. **The executed BigQuery SQL must contain a `WHERE` clause or similar logic that utilizes this `Stichtag` value, as per the design.**
    *   **B**: The `Stichtag` logged by `dataproc_job.py` must default to today's date (DDMMYYYY format). **The executed BigQuery SQL must reflect this default `Stichtag` if it's used.**
    *   **C**: The Airflow task `run_bert_aggregation_dataproc` must fail, and the Dataproc job logs must show an error message indicating an invalid `Stichtag` format (e.g., "Invalid Stichtag format: ... Expected DDMMYYYY.").

*   **Note on Discrepancy**: The provided `dataproc_job.py` currently parses `stichtag` but only uses it for logging. The comment `_get_last_timecreated_from_dwtk_meldungen` also states `v_datum` is "not used in the core SQL". The design document explicitly states: "The `Stichtag` parameter... will be passed to the BigQuery SQL." This test will highlight if the implementation deviates from the design's intent regarding `Stichtag`'s use in the core SQL transformation. If `Stichtag` is not used in the SQL, this test should fail the "Pass/Fail Criterion" for A and B regarding SQL usage, prompting a design/implementation review.

```python
# Example Python (pytest) for Stichtag parameter handling (unit test for dataproc_job.py)
import pytest
from unittest.mock import MagicMock, patch
from datetime import date
from src.dataproc.dataproc_job import BertJob # Assuming src/dataproc is in PYTHONPATH

@patch('google.cloud.bigquery.Client')
def test_stichtag_parsing_explicit(mock_bq_client):
    """Test explicit Stichtag parsing."""
    job = BertJob(
        project_id="test-project",
        dataset_id="test_dataset",
        sql_file_path="dummy.sql",
        stichtag_str="15012023"
    )
    assert job.stichtag == date(2023, 1, 15)

@patch('google.cloud.bigquery.Client')
def test_stichtag_parsing_default(mock_bq_client):
    """Test default Stichtag parsing (today's date)."""
    with patch('src.dataproc.dataproc_job.datetime') as mock_dt:
        mock_dt.date.today.return_value = date(2023, 10, 26)
        mock_dt.datetime = MagicMock(wraps=mock_dt.datetime) # Ensure datetime.datetime is still available
        job = BertJob(
            project_id="test-project",
            dataset_id="test_dataset",
            sql_file_path="dummy.sql",
            stichtag_str=None
        )
        assert job.stichtag == date(2023, 10, 26)

@patch('google.cloud.bigquery.Client')
def test_stichtag_parsing_invalid_format(mock_bq_client):
    """Test invalid Stichtag format."""
    with pytest.raises(ValueError, match="Invalid Stichtag format: 2023-01-15. Expected DDMMYYYY."):
        BertJob(
            project_id="test-project",
            dataset_id="test_dataset",
            sql_file_path="dummy.sql",
            stichtag_str="2023-01-15"
        )

# Integration test for Stichtag usage in SQL (conceptual, requires inspecting BQ job details)
def test_stichtag_passed_to_bigquery_sql(airflow_client, gcp_project_id, bq_dataset_id):
    """
    Verifies that the Stichtag parameter is actually used in the BigQuery SQL.
    This test requires inspecting the executed BigQuery job details.
    """
    # Trigger DAG with a specific stichtag
    stichtag_to_test = "01022023"
    # airflow_client.trigger_dag("dw_bert_ausd_bp_ta_rn_vertrag", conf={"stichtag": stichtag_to_test})
    # Wait for DAG to complete and Dataproc job to run

    # --- Action: Retrieve BigQuery job details ---
    # This part is conceptual. You'd need to find the BigQuery job ID
    # associated with the Dataproc job and inspect its query text.
    # Example (pseudo-code):
    # bq_job_id = get_bigquery_job_id_from_dataproc_logs(dataproc_job_id)
    # bq_job = bigquery_client.get_job(bq_job_id, project=gcp_project_id)
    # executed_sql = bq_job.query

    # --- Pass/Fail Criterion ---
    # assert f"WHERE some_date_column <= '{stichtag_to_test}'" in executed_sql
    # OR assert f"WHERE some_date_column <= PARSE_DATE('%d%m%Y', '{stichtag_to_test}')" in executed_sql
    # This assertion would fail if the SQL does not contain the Stichtag.
    # This highlights the discrepancy between design and current implementation.
    pytest.fail("Manual verification required: Confirm BigQuery SQL uses Stichtag. Current Python code does not pass it to SQL.")
```

### Test Case 2.3: `TRUNCATE TABLE` Behavior

*   **Purpose**: To verify that the target table `isbert_schema.SOF_TA_RN_VERTRAG` is correctly truncated before new data is inserted, ensuring a clean slate for each run.
*   **Setup**:
    1.  **Pre-populate Target**: Insert a known set of dummy data into `isbert_schema.SOF_TA_RN_VERTRAG`. This data should be distinct from any data that would be generated by the actual job run.
    2.  **Source Data**: Ensure `isbert_schema.SOF_TA_RN_EINZELN` contains data that will result in a non-empty `SOF_TA_RN_VERTRAG` after transformation.
*   **Action**: Trigger the Airflow DAG `dw_bert_ausd_bp_ta_rn_vertrag`. After completion, query `isbert_schema.SOF_TA_RN_VERTRAG`.
*   **Pass/Fail Criterion**:
    *   The dummy data inserted during setup must no longer be present in `isbert_schema.SOF_TA_RN_VERTRAG`.
    *   The table must only contain the data generated by the current job run.

```python
# Example Python (pytest) assertion for TRUNCATE TABLE behavior
from google.cloud import bigquery
import pandas as pd

def test_truncate_table_behavior(bigquery_client, gcp_project_id, bq_dataset_id):
    """
    Verifies that the target table is truncated before insertion.
    """
    target_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG`"
    source_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_EINZELN`"

    # --- Setup: Pre-populate target with dummy data ---
    bigquery_client.query(f"TRUNCATE TABLE {source_table}").result() # Clear source for clean run
    bigquery_client.query(f"TRUNCATE TABLE {target_table}").result()

    dummy_data = [(999, 'DUMMY_MSISDN', 'DUMMY_STATUS', None, None, None, None, None, None)]
    dummy_insert_sql = f"""
        INSERT INTO {target_table} (CNTRCT_ID, TN_TEL_msisdn, TN_TEL_status, TN_FAX_msisdn, TN_FAX_status, MS_RN_1_msisdn, MS_RN_1_status, MS_RN_2_msisdn, MS_RN_2_status) VALUES
        {', '.join([str(row) for row in dummy_data])}
    """
    bigquery_client.query(dummy_insert_sql).result()
    initial_count = bigquery_client.query(f"SELECT COUNT(*) FROM {target_table}").to_dataframe().iloc[0, 0]
    assert initial_count == 1, "Setup failed: Dummy data not inserted."

    # Insert some actual source data that will be processed
    test_source_data = [(101, '111', 'ACTIVE', '222', 'OPEN', '333', 'ON', '444', 'OFF')]
    source_insert_sql = f"""
        INSERT INTO {source_table} (CNTRCT_ID, TN_TEL_msisdn, TN_TEL_status, TN_FAX_msisdn, TN_FAX_status, MS_RN_1_msisdn, MS_RN_1_status, MS_RN_2_msisdn, MS_RN_2_status) VALUES
        {', '.join([str(row) for row in test_source_data])}
    """
    bigquery_client.query(source_insert_sql).result()

    # --- Action: Run the migrated job (conceptual) ---
    # Trigger Airflow DAG or execute core SQL
    core_sql_template = """
        TRUNCATE TABLE {target_table};
        INSERT INTO {target_table} (
            CNTRCT_ID, TN_TEL_msisdn, TN_TEL_status, TN_FAX_msisdn, TN_FAX_status,
            MS_RN_1_msisdn, MS_RN_1_status, MS_RN_2_msisdn, MS_RN_2_status
        )
        SELECT
            s.CNTRCT_ID,
            MAX(s.TN_TEL_msisdn) AS TN_TEL_msisdn,
            MAX(s.TN_TEL_status) AS TN_TEL_status,
            MAX(s.TN_FAX_msisdn) AS TN_FAX_msisdn,
            MAX(s.TN_FAX_status) AS TN_FAX_status,
            MAX(s.MS_RN_1_msisdn) AS MS_RN_1_msisdn,
            MAX(s.MS_RN_1_status) AS MS_RN_1_status,
            MAX(s.MS_RN_2_msisdn) AS MS_RN_2_msisdn,
            MAX(s.MS_RN_2_status) AS MS_RN_2_status
        FROM {source_table} AS s
        GROUP BY s.CNTRCT_ID;
    """
    bigquery_client.query(core_sql_template.format(source_table=source_table, target_table=target_table)).result()

    # --- Assertion: Check content ---
    final_df = bigquery_client.query(f"SELECT CNTRCT_ID FROM {target_table}").to_dataframe()

    assert len(final_df) == 1, f"Expected 1 row after run, got {len(final_df)}"
    assert final_df['CNTRCT_ID'].iloc[0] == 101, "Dummy data was not truncated or incorrect data inserted."
    print("TRUNCATE TABLE behavior test passed.")
```

### Test Case 2.4: Data Type Handling and Implicit Conversions

*   **Purpose**: To ensure that data types are correctly handled between the Oracle source and BigQuery target, and any implicit conversions (e.g., during `MAX` aggregation) behave as expected without data loss or errors.
*   **Setup**:
    1.  **Diverse Input**: Create `isbert_schema.SOF_TA_RN_EINZELN` data with values that might challenge type handling:
        *   Numbers stored as strings (if applicable in Oracle).
        *   Dates/timestamps in various formats (if applicable).
        *   Large numeric values that might exceed standard integer limits.
        *   Strings with special characters or different encodings.
        *   Columns with mixed data types (if Oracle allowed this and BigQuery needs to handle it).
    2.  **Schema Review**: Document the exact Oracle data types for all relevant columns in `SOF$TA_RN_EINZELN` and `SOF$TA_RN_VERTRAG`.
*   **Action**: Run the migrated job. Query `isbert_schema.SOF_TA_RN_VERTRAG` and inspect the column types and values.
*   **Pass/Fail Criterion**:
    *   All columns in `isbert_schema.SOF_TA_RN_VERTRAG` must have the correct BigQuery data types (e.g., `STRING` for `VARCHAR2`, `INT64` for `NUMBER` without decimals, `BIGNUMERIC` for large numbers, `TIMESTAMP` for `DATE`/`TIMESTAMP`).
    *   Values must be accurately represented without truncation, loss of precision, or unexpected conversion errors.
    *   The `MAX` function should correctly operate on these types (e.g., lexicographical `MAX` for strings, numerical `MAX` for numbers, chronological `MAX` for dates).

```python
# Example Python (pytest) for data type handling
from google.cloud import bigquery
import pandas as pd

def test_data_type_handling(bigquery_client, gcp_project_id, bq_dataset_id):
    """
    Verifies correct data type handling and implicit conversions.
    This test assumes the BigQuery table schemas are already defined correctly.
    """
    source_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_EINZELN`"
    target_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG`"

    # --- Setup: Controlled Input Data with various types/edge cases ---
    bigquery_client.query(f"TRUNCATE TABLE {source_table}").result()
    bigquery_client.query(f"TRUNCATE TABLE {target_table}").result()

    # Example: CNTRCT_ID 101 has a large number, a string with special chars, a date string
    # Assuming TN_TEL_msisdn is STRING, TN_TEL_status is STRING, MS_RN_1_msisdn is STRING
    test_data = [
        (101, '99999999999999999999', 'ACTIVE', '2023-01-01', 'OPEN', 'ÄÖÜß', 'ON', '123.45', 'OFF'), # Large number as string, special chars, date string
        (101, '12345678901234567890', 'INACTIVE', '2023-01-02', 'CLOSED', 'abc', 'OFF', '123.46', 'ON'),
        (102, '1', 'ACTIVE', '2022-12-31', 'OPEN', 'xyz', 'ON', '100', 'OFF'),
    ]
    # Note: The actual schema of SOF_TA_RN_EINZELN and SOF_TA_RN_VERTRAG in BQ
    # needs to be defined to match the expected types. For example, if TN_FAX_msisdn
    # was a DATE in Oracle, it should be a DATE/TIMESTAMP in BQ.
    # Here, I'm treating all phone numbers/statuses as STRING based on the example.
    insert_sql = f"""
        INSERT INTO {source_table} (CNTRCT_ID, TN_TEL_msisdn, TN_TEL_status, TN_FAX_msisdn, TN_FAX_status, MS_RN_1_msisdn, MS_RN_1_status, MS_RN_2_msisdn, MS_RN_2_status) VALUES
        {', '.join([str(row) for row in test_data])}
    """
    bigquery_client.query(insert_sql).result()

    # --- Action: Run the migrated job (conceptual) ---
    core_sql_template = """
        TRUNCATE TABLE {target_table};
        INSERT INTO {target_table} (
            CNTRCT_ID, TN_TEL_msisdn, TN_TEL_status, TN_FAX_msisdn, TN_FAX_status,
            MS_RN_1_msisdn, MS_RN_1_status, MS_RN_2_msisdn, MS_RN_2_status
        )
        SELECT
            s.CNTRCT_ID,
            MAX(s.TN_TEL_msisdn) AS TN_TEL_msisdn,
            MAX(s.TN_TEL_status) AS TN_TEL_status,
            MAX(s.TN_FAX_msisdn) AS TN_FAX_msisdn,
            MAX(s.TN_FAX_status) AS TN_FAX_status,
            MAX(s.MS_RN_1_msisdn) AS MS_RN_1_msisdn,
            MAX(s.MS_RN_1_status) AS MS_RN_1_status,
            MAX(s.MS_RN_2_msisdn) AS MS_RN_2_msisdn,
            MAX(s.MS_RN_2_status) AS MS_RN_2_status
        FROM {source_table} AS s
        GROUP BY s.CNTRCT_ID;
    """
    bigquery_client.query(core_sql_template.format(source_table=source_table, target_table=target_table)).result()

    # --- Assertion: Query and Compare Types/Values ---
    result_df = bigquery_client.query(f"SELECT * FROM {target_table} ORDER BY CNTRCT_ID").to_dataframe()

    # Check data types in BigQuery
    schema = bigquery_client.get_table(f"{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG").schema
    schema_dict = {field.name: field.field_type for field in schema}

    assert schema_dict['CNTRCT_ID'] == 'INT64'
    assert schema_dict['TN_TEL_msisdn'] == 'STRING'
    assert schema_dict['TN_TEL_status'] == 'STRING'
    # Assuming TN_FAX_msisdn is STRING in BQ, even if it was a date string in source
    assert schema_dict['TN_FAX_msisdn'] == 'STRING'
    assert schema_dict['MS_RN_1_msisdn'] == 'STRING'
    assert schema_dict['MS_RN_2_msisdn'] == 'STRING' # If this was numeric in Oracle, it should be NUMERIC/BIGNUMERIC in BQ

    # Check aggregated values for CNTRCT_ID 101
    # MAX of '99999999999999999999' and '12345678901234567890' (as strings) is '999...'
    # MAX of 'ACTIVE' and 'INACTIVE' is 'INACTIVE' (lexicographical)
    # MAX of '2023-01-01' and '2023-01-02' (as strings) is '2023-01-02'
    # MAX of 'ÄÖÜß' and 'abc' is 'abc' (lexicographical)
    # MAX of '123.45' and '123.46' is '123.46' (lexicographical as strings)
    row_101 = result_df[result_df['CNTRCT_ID'] == 101].iloc[0]
    assert row_101['TN_TEL_msisdn'] == '99999999999999999999'
    assert row_101['TN_TEL_status'] == 'INACTIVE'
    assert row_101['TN_FAX_msisdn'] == '2023-01-02'
    assert row_101['MS_RN_1_msisdn'] == 'abc'
    assert row_101['MS_RN_2_msisdn'] == '123.46'

    print("Data type handling test passed.")
```

---

## 3. External-System Replacements Tests

### Test Case 3.1: Oracle Source Table Replication (`SOF$TA_RN_EINZELN`)

*   **Purpose**: To verify that the data ingestion pipeline (e.g., DataStream, Cloud Data Fusion) accurately and timely replicates data from the Oracle `SOF$TA_RN_EINZELN` table to `isbert_schema.SOF_TA_RN_EINZELN` in BigQuery.
*   **Setup**:
    1.  **Baseline**: Record the current state (row count, checksum) of both Oracle `SOF$TA_RN_EINZELN` and BigQuery `isbert_schema.SOF_TA_RN_EINZELN`.
    2.  **Test Data**: Insert, update, and delete a specific set of test records in the Oracle `SOF$TA_RN_EINZELN` table.
*   **Action**:
    1.  Allow the data ingestion pipeline to run for its configured latency period.
    2.  Query both Oracle `SOF$TA_RN_EINZELN` and BigQuery `isbert_schema.SOF_TA_RN_EINZELN`.
*   **Pass/Fail Criterion**:
    *   The row count in BigQuery `isbert_schema.SOF_TA_RN_EINZELN` must match the row count in Oracle `SOF$TA_RN_EINZELN`.
    *   A full data comparison (e.g., checksums or row-by-row comparison) must show that the data in BigQuery is an exact replica of the data in Oracle, reflecting all inserts, updates, and deletes.
    *   The replication latency must be within acceptable business limits.

```python
# Example Python (pytest) for Oracle to BigQuery replication
from google.cloud import bigquery
import pandas as pd
import time

def test_sof_ta_rn_einzeln_replication(bigquery_client, oracle_client, gcp_project_id, bq_dataset_id):
    """
    Verifies data replication from Oracle SOF$TA_RN_EINZELN to BigQuery.
    Assumes oracle_client is a fixture for connecting to Oracle.
    """
    oracle_source_table = "SOF$TA_RN_EINZELN" # Oracle table name
    bq_source_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_EINZELN`"

    # --- Setup: Insert test data into Oracle ---
    # Clear Oracle table (if possible in test environment) and BQ table
    # oracle_client.execute(f"TRUNCATE TABLE {oracle_source_table}")
    bigquery_client.query(f"TRUNCATE TABLE {bq_source_table}").result()

    # Insert new data into Oracle
    oracle_client.execute(f"INSERT INTO {oracle_source_table} (CNTRCT_ID, TN_TEL_msisdn) VALUES (1, '111')")
    oracle_client.execute(f"INSERT INTO {oracle_source_table} (CNTRCT_ID, TN_TEL_msisdn) VALUES (2, '222')")
    oracle_client.commit()

    # --- Action: Wait for replication ---
    replication_latency_seconds = 60 # Adjust based on actual pipeline latency
    print(f"Waiting {replication_latency_seconds} seconds for replication...")
    time.sleep(replication_latency_seconds)

    # --- Assertion: Compare data ---
    oracle_df = pd.read_sql(f"SELECT CNTRCT_ID, TN_TEL_msisdn FROM {oracle_source_table} ORDER BY CNTRCT_ID", oracle_client.connection)
    bq_df = bigquery_client.query(f"SELECT CNTRCT_ID, TN_TEL_msisdn FROM {bq_source_table} ORDER BY CNTRCT_ID").to_dataframe()

    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False) # check_dtype=False due to potential type differences

    # Update data in Oracle
    oracle_client.execute(f"UPDATE {oracle_source_table} SET TN_TEL_msisdn = '111_UPDATED' WHERE CNTRCT_ID = 1")
    oracle_client.commit()
    print(f"Waiting {replication_latency_seconds} seconds for replication of update...")
    time.sleep(replication_latency_seconds)

    oracle_df_updated = pd.read_sql(f"SELECT CNTRCT_ID, TN_TEL_msisdn FROM {oracle_source_table} ORDER BY CNTRCT_ID", oracle_client.connection)
    bq_df_updated = bigquery_client.query(f"SELECT CNTRCT_ID, TN_TEL_msisdn FROM {bq_source_table} ORDER BY CNTRCT_ID").to_dataframe()

    pd.testing.assert_frame_equal(oracle_df_updated, bq_df_updated, check_dtype=False)

    print("SOF_TA_RN_EINZELN replication test passed.")
```

### Test Case 3.2: Oracle Source Table Replication (`DWTK_MELDUNGEN`)

*   **Purpose**: To verify that `isbert_schema.DWTK_MELDUNGEN` in BigQuery accurately reflects the data from the Oracle `DWTK_MELDUNGEN` source, especially for entries relevant to `v_datum` calculation.
*   **Setup**:
    1.  **Baseline**: Record the current state of both Oracle `DWTK_MELDUNGEN` and BigQuery `isbert_schema.DWTK_MELDUNGEN`.
    2.  **Test Data**: Insert a new record into Oracle `DWTK_MELDUNGEN` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and a specific `timecreated` value.
*   **Action**:
    1.  Allow the data ingestion pipeline to run.
    2.  Query BigQuery `isbert_schema.DWTK_MELDUNGEN` for the newly inserted record.
    3.  Execute the `_get_last_timecreated_from_dwtk_meldungen` method of the `dataproc_job.py` script (or run the full job and check logs).
*   **Pass/Fail Criterion**:
    *   The new record must be present in BigQuery `isbert_schema.DWTK_MELDUNGEN`.
    *   The `_get_last_timecreated_from_dwtk_meldungen` function (or job logs) must correctly identify the `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from the BigQuery table.

```python
# Example Python (pytest) for DWTK_MELDUNGEN replication and v_datum calculation
from google.cloud import bigquery
import pandas as pd
import time
from datetime import datetime
from src.dataproc.dataproc_job import BertJob

def test_dwtk_meldungen_replication_and_v_datum(bigquery_client, oracle_client, gcp_project_id, bq_dataset_id):
    """
    Verifies DWTK_MELDUNGEN replication and correct v_datum calculation.
    """
    oracle_source_table = "DWTK_MELDUNGEN"
    bq_source_table = f"`{gcp_project_id}.{bq_dataset_id}.DWTK_MELDUNGEN`"

    # --- Setup: Insert test data into Oracle ---
    # Clear BQ table
    bigquery_client.query(f"TRUNCATE TABLE {bq_source_table}").result()

    # Insert new data into Oracle DWTK_MELDUNGEN
    # Assuming DWTK_MELDUNGEN has columns like JOB_KENNUNG, TIMECREATED
    test_timecreated = datetime(2023, 1, 15, 10, 30, 0)
    oracle_client.execute(f"INSERT INTO {oracle_source_table} (JOB_KENNUNG, TIMECREATED) VALUES ('BERT_DROP_TEMP_TABLE', TO_DATE('{test_timecreated.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS'))")
    oracle_client.execute(f"INSERT INTO {oracle_source_table} (JOB_KENNUNG, TIMECREATED) VALUES ('OTHER_JOB', TO_DATE('2023-01-14 09:00:00', 'YYYY-MM-DD HH24:MI:SS'))")
    oracle_client.commit()

    # --- Action: Wait for replication and run job ---
    replication_latency_seconds = 60
    print(f"Waiting {replication_latency_seconds} seconds for DWTK_MELDUNGEN replication...")
    time.sleep(replication_latency_seconds)

    # Instantiate BertJob to test _get_last_timecreated_from_dwtk_meldungen
    job = BertJob(
        project_id=gcp_project_id,
        dataset_id=bq_dataset_id,
        sql_file_path="dummy.sql" # dummy path, not used for this method
    )
    calculated_s_datum = job._get_last_timecreated_from_dwtk_meldungen()

    # --- Assertion ---
    expected_s_datum = test_timecreated.strftime('%Y%m%d')
    assert calculated_s_datum == expected_s_datum, \
        f"Calculated v_datum mismatch. Expected {expected_s_datum}, got {calculated_s_datum}"

    print("DWTK_MELDUNGEN replication and v_datum calculation test passed.")
```

---

## 4. Data Quality / Row-Count / Schema Assertions

### Test Case 4.1: Row Count Parity

*   **Purpose**: To verify that the number of rows in the target table `isbert_schema.SOF_TA_RN_VERTRAG` matches the legacy output for the same input data.
*   **Setup**: Use the same input data and job execution as in Test Case 1.1 (End-to-End Data Parity).
*   **Action**: Query the row count of `isbert_schema.SOF_TA_RN_VERTRAG` after the migrated job completes.
*   **Pass/Fail Criterion**: The `COUNT(*)` from `isbert_schema.SOF_TA_RN_VERTRAG` must be identical to the `COUNT(*)` from the captured Oracle `SOF$TA_RN_VERTRAG` output.

```python
# Example Python (pytest) assertion for row count parity
from google.cloud import bigquery
import pandas as pd

def test_row_count_parity(bigquery_client, gcp_project_id, bq_dataset_id):
    """
    Verifies that the row count of the target table matches the legacy output.
    Assumes legacy_oracle_output.csv contains the captured Oracle output.
    """
    # --- Setup (conceptual, as per Test Case 1.1) ---
    # Load Oracle snapshot data into BQ source tables
    # Run legacy Oracle job, capture output to 'legacy_oracle_output.csv'
    # Trigger Airflow DAG for migrated job

    # --- Action & Assertion ---
    legacy_df = pd.read_csv("legacy_oracle_output.csv")
    expected_row_count = len(legacy_df)

    bq_query = f"SELECT COUNT(*) FROM `{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG`"
    actual_row_count = bigquery_client.query(bq_query).to_dataframe().iloc[0, 0]

    assert actual_row_count == expected_row_count, \
        f"Row count mismatch: Expected {expected_row_count}, Actual {actual_row_count}"
    print(f"Row count parity test passed. Count: {actual_row_count}")
```

### Test Case 4.2: Schema Parity

*   **Purpose**: To verify that the schema (column names, data types, nullability) of `isbert_schema.SOF_TA_RN_VERTRAG` in BigQuery matches the legacy `SOF$TA_RN_VERTRAG` in Oracle.
*   **Setup**: Obtain the schema definition for Oracle `SOF$TA_RN_VERTRAG` (e.g., using `DESCRIBE` or `ALL_TAB_COLUMNS`).
*   **Action**: Retrieve the schema definition for BigQuery `isbert_schema.SOF_TA_RN_VERTRAG`.
*   **Pass/Fail Criterion**:
    *   All column names must match (case-insensitivity might need to be considered if Oracle names were case-sensitive).
    *   BigQuery data types must be the appropriate equivalent of Oracle data types (e.g., `VARCHAR2(N)` -> `STRING`, `NUMBER` -> `INT64`/`BIGNUMERIC`, `DATE` -> `TIMESTAMP`/`DATE`).
    *   Nullability constraints (`NOT NULL` vs. nullable) should be consistent.
    *   Column order should ideally be preserved for easier comparison, though not strictly a functional requirement.

```python
# Example Python (pytest) assertion for schema parity
from google.cloud import bigquery
import pandas as pd

def test_schema_parity(bigquery_client, oracle_client, gcp_project_id, bq_dataset_id):
    """
    Verifies that the BigQuery target table schema matches the Oracle legacy schema.
    """
    oracle_target_table = "SOF$TA_RN_VERTRAG"
    bq_target_table_id = f"{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG"

    # --- Setup: Get Oracle schema ---
    # This requires a connection to Oracle and querying its metadata views
    # Example (conceptual):
    # oracle_schema_query = f"""
    #     SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
    #     FROM ALL_TAB_COLUMNS
    #     WHERE OWNER = 'ISBERT_SCHEMA' AND TABLE_NAME = '{oracle_target_table}'
    #     ORDER BY COLUMN_ID
    # """
    # oracle_schema_df = pd.read_sql(oracle_schema_query, oracle_client.connection)
    # For this example, let's define a mock Oracle schema
    oracle_schema_data = {
        'COLUMN_NAME': ['CNTRCT_ID', 'TN_TEL_MSISDN', 'TN_TEL_STATUS', 'TN_FAX_MSISDN', 'TN_FAX_STATUS'],
        'DATA_TYPE': ['NUMBER', 'VARCHAR2(20)', 'VARCHAR2(10)', 'VARCHAR2(20)', 'VARCHAR2(10)'],
        'NULLABLE': ['N', 'Y', 'Y', 'Y', 'Y']
    }
    oracle_schema_df = pd.DataFrame(oracle_schema_data)
    oracle_schema_df['COLUMN_NAME'] = oracle_schema_df['COLUMN_NAME'].str.upper() # Standardize case

    # --- Action: Get BigQuery schema ---
    bq_table = bigquery_client.get_table(bq_target_table_id)
    bq_schema_data = []
    for field in bq_table.schema:
        bq_schema_data.append({
            'COLUMN_NAME': field.name.upper(), # Standardize case
            'DATA_TYPE': field.field_type,
            'NULLABLE': 'Y' if field.mode == 'NULLABLE' else 'N'
        })
    bq_schema_df = pd.DataFrame(bq_schema_data)

    # --- Pass/Fail Criterion ---
    # Check column names
    assert set(oracle_schema_df['COLUMN_NAME']) == set(bq_schema_df['COLUMN_NAME']), \
        "Column names mismatch between Oracle and BigQuery."

    # Check data types and nullability for matching columns
    for _, oracle_col in oracle_schema_df.iterrows():
        bq_col = bq_schema_df[bq_schema_df['COLUMN_NAME'] == oracle_col['COLUMN_NAME']].iloc[0]

        # Map Oracle types to expected BigQuery types
        expected_bq_type = None
        if 'NUMBER' in oracle_col['DATA_TYPE']:
            expected_bq_type = 'INT64' # Or BIGNUMERIC if precision is needed
        elif 'VARCHAR2' in oracle_col['DATA_TYPE']:
            expected_bq_type = 'STRING'
        elif 'DATE' in oracle_col['DATA_TYPE']:
            expected_bq_type = 'TIMESTAMP' # Or DATE if no time component

        assert bq_col['DATA_TYPE'] == expected_bq_type, \
            f"Data type mismatch for column {oracle_col['COLUMN_NAME']}: Expected BQ {expected_bq_type}, Got {bq_col['DATA_TYPE']}"
        assert bq_col['NULLABLE'] == oracle_col['NULLABLE'], \
            f"Nullability mismatch for column {oracle_col['COLUMN_NAME']}: Expected {oracle_col['NULLABLE']}, Got {bq_col['NULLABLE']}"

    print("Schema parity test passed.")
```

### Test Case 4.3: Data Quality - Uniqueness of `CNTRCT_ID`

*   **Purpose**: To ensure that `CNTRCT_ID` remains unique in the target table `isbert_schema.SOF_TA_RN_VERTRAG`, as it is the `GROUP BY` key in the transformation logic.
*   **Setup**: Ensure `isbert_schema.SOF_TA_RN_EINZELN` contains a representative dataset, including multiple entries for the same `CNTRCT_ID` to thoroughly test the `GROUP BY` aggregation.
*   **Action**: After a successful job run, execute a BigQuery SQL query to check for duplicate `CNTRCT_ID`s in `isbert_schema.SOF_TA_RN_VERTRAG`.
*   **Pass/Fail Criterion**: The query `SELECT CNTRCT_ID FROM isbert_schema.SOF_TA_RN_VERTRAG GROUP BY CNTRCT_ID HAVING COUNT(*) > 1` must return zero rows.

```python
# Example Python (pytest) assertion for CNTRCT_ID uniqueness
from google.cloud import bigquery

def test_cntrct_id_uniqueness(bigquery_client, gcp_project_id, bq_dataset_id):
    """
    Verifies that CNTRCT_ID is unique in the target table after aggregation.
    """
    target_table = f"`{gcp_project_id}.{bq_dataset_id}.SOF_TA_RN_VERTRAG`"

    # --- Setup (conceptual) ---
    # Ensure the job has run with sufficient input data to test aggregation.
    # (e.g., by running the full DAG or executing the core SQL)

    # --- Action & Assertion ---
    duplicate_check_query = f"""
        SELECT CNTRCT_ID, COUNT(*) as cnt
        FROM {target_table}
        GROUP BY CNTRCT_ID
        HAVING COUNT(*) > 1
    """
    duplicates_df = bigquery_client.query(duplicate_check_query).to_dataframe()

    assert len(duplicates_df) == 0, \
        f"Duplicate CNTRCT_ID(s) found in {target_table}: {duplicates_df['CNTRCT_ID'].tolist()}"
    print("CNTRCT_ID uniqueness test passed.")
```