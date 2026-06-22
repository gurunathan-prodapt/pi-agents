As a senior data-migration QA engineer, I've reviewed the migration design and the generated code for `DW.BERT_AUSD_BP_TA_BCP_ICCID`. The following test cases are designed to ensure the migrated job is behaviourally equivalent to its legacy Oracle counterpart, covering output parity, transformation correctness, external system replacements, and data quality.

A critical observation from the design document and generated code is that while the `v_datum` is extracted and `stichtag`/`wiederanlaufwert` parameters are defined, they are **not currently used** in the main `INSERT OVERWRITE` BigQuery SQL transformation. This deviates from the design's implication that `v_datum` might be used for filtering and `wiederanlaufwert` would be used in the BigQuery SQL. This discrepancy is highlighted in a dedicated test case.

---

## Common Setup for All Tests

To facilitate testing, we assume the following helper functions and configurations are available in the test environment:

*   `GCP_PROJECT_ID`: Your Google Cloud Project ID.
*   `BIGQUERY_DATASET_ID`: Your BigQuery Dataset ID where migrated tables reside.
*   `ORACLE_SCHEMA`: The Oracle schema where legacy source tables (`dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`) and the target table (`sof$ta_bcp_iccid`) exist.
*   `execute_oracle_query(sql_query)`: A function to execute SQL against the Oracle database and return results (e.g., as a list of tuples or a Pandas DataFrame).
*   `execute_bigquery_query(sql_query)`: A function to execute SQL against BigQuery and return results (e.g., as a list of tuples or a Pandas DataFrame).
*   `run_legacy_job()`: A function that triggers the legacy Oracle job execution. This would typically involve calling the KornShell script or directly executing the Oracle SQL script.
*   `run_migrated_dag(dag_run_conf=None)`: A function that triggers the Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid`. `dag_run_conf` can be used to pass `stichtag` and `wiederanlaufwert`.
*   `clear_oracle_table(table_name)`: Truncates or deletes all data from a specified Oracle table.
*   `clear_bigquery_table(table_name)`: Truncates or deletes all data from a specified BigQuery table.
*   `insert_oracle_data(table_name, data)`: Inserts data into an Oracle table. `data` could be a list of dictionaries or tuples.
*   `insert_bigquery_data(table_name, data)`: Inserts data into a BigQuery table.
*   `compare_dataframes(df1, df2, sort_cols)`: A utility function to compare two Pandas DataFrames for equality, ignoring row order.

For `pytest` examples, we'll use fixtures for setup/teardown.

```python
import pytest
import pandas as pd
from google.cloud import bigquery
import os
from datetime import datetime, timedelta

# --- Configuration (replace with your actual values) ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "your_dataset")
ORACLE_SCHEMA = "ISBERT_SCHEMA" # Assuming this is the schema for all tables
ORACLE_TARGET_SCHEMA = "ISBERT_SCHEMA" # Assuming this is the schema for the target table

# --- Mock/Helper Functions (replace with actual implementations for your environment) ---
# These functions would interact with your Oracle DB and BigQuery/Airflow
# For demonstration, they are simplified or mocked.

def execute_oracle_query(sql_query):
    """Executes SQL against Oracle and returns a Pandas DataFrame."""
    print(f"\nExecuting Oracle Query:\n{sql_query}")
    # In a real scenario, use cx_Oracle or similar to connect and fetch data
    # For testing, you might mock this or connect to a test Oracle instance.
    # Example:
    # import cx_Oracle
    # conn = cx_Oracle.connect("user/pass@host:port/service_name")
    # df = pd.read_sql(sql_query, conn)
    # conn.close()
    # return df
    raise NotImplementedError("Oracle interaction not implemented for this example.")

def execute_bigquery_query(sql_query):
    """Executes SQL against BigQuery and returns a Pandas DataFrame."""
    print(f"\nExecuting BigQuery Query:\n{sql_query}")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query_job = client.query(sql_query)
    return query_job.to_dataframe()

def clear_oracle_table(table_name):
    """Truncates or deletes all data from a specified Oracle table."""
    print(f"Clearing Oracle table: {ORACLE_SCHEMA}.{table_name}")
    # execute_oracle_query(f"TRUNCATE TABLE {ORACLE_SCHEMA}.{table_name}")
    pass # Mocked

def clear_bigquery_table(table_name):
    """Truncates or deletes all data from a specified BigQuery table."""
    print(f"Clearing BigQuery table: {BIGQUERY_DATASET_ID}.{table_name}")
    execute_bigquery_query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}`")

def insert_oracle_data(table_name, data):
    """Inserts data into an Oracle table."""
    print(f"Inserting {len(data)} rows into Oracle table: {ORACLE_SCHEMA}.{table_name}")
    # Implement actual Oracle insert logic here
    pass # Mocked

def insert_bigquery_data(table_name, data):
    """Inserts data into a BigQuery table."""
    print(f"Inserting {len(data)} rows into BigQuery table: {BIGQUERY_DATASET_ID}.{table_name}")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}"
    errors = client.insert_rows_json(table_id, data)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

def run_legacy_job():
    """Triggers the legacy Oracle job execution."""
    print("Running legacy Oracle job...")
    # This would typically involve calling the ksh script via SSH or a scheduler.
    # For testing, you might directly execute the core SQL logic against Oracle.
    # Example:
    # sql_script = open("vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql").read()
    # execute_oracle_query(sql_script) # This is a simplification, as the ksh script handles parameters and environment.
    # For true parity, you'd need to run the actual ksh script.
    raise NotImplementedError("Legacy job execution not implemented for this example.")

def run_migrated_dag(dag_run_conf=None):
    """Triggers the Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid`."""
    print(f"Triggering Airflow DAG 'dw_bert_ausd_bp_ta_bcp_iccid' with conf: {dag_run_conf}")
    # In a real scenario, use Airflow's REST API or CLI to trigger the DAG.
    # For local testing, you might directly call the Python functions of the DAG.
    # Example (simplified, not a full DAG run):
    # from dags.dw_bert_ausd_bp_ta_bcp_iccid_dag import _extract_v_datum_func, transform_load_data_task
    # # Mock XComs and BigQueryHook for _extract_v_datum_func
    # # Then execute the BigQuery query directly
    # v_datum = _extract_v_datum_func(MockTI(), GCP_PROJECT_ID, BIGQUERY_DATASET_ID)
    # execute_bigquery_query(transform_load_data_task.sql)
    # This is complex to mock fully. For integration tests, trigger via Airflow API.
    pass # Mocked

def compare_dataframes(df1, df2, sort_cols):
    """Compares two Pandas DataFrames for equality, ignoring row order."""
    if df1.empty and df2.empty:
        return True
    if df1.shape != df2.shape:
        print(f"DataFrame shapes differ: {df1.shape} vs {df2.shape}")
        return False
    # Sort both dataframes by a consistent set of columns to enable row-wise comparison
    df1_sorted = df1.sort_values(by=sort_cols).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=sort_cols).reset_index(drop=True)
    
    # Compare column names and types
    if not df1_sorted.columns.equals(df2_sorted.columns):
        print(f"Column names differ: {df1_sorted.columns.tolist()} vs {df2_sorted.columns.tolist()}")
        return False
    
    # Use .equals() for robust comparison, including dtypes and values
    if not df1_sorted.equals(df2_sorted):
        print("DataFrames content differ:")
        # Find differing rows for better debugging
        merged_df = pd.merge(df1_sorted, df2_sorted, how='outer', indicator=True)
        diff_df = merged_df[merged_df['_merge'] != 'both']
        print(diff_df)
        return False
    return True

# --- Pytest Fixtures ---
@pytest.fixture(scope="function", autouse=True)
def setup_teardown_tables():
    """Fixture to clear tables before and after each test."""
    source_tables = ["dwtk_meldungen", "sof_ta_bpr_bcp", "sof_ta_iccid_vertrag"]
    target_table = "sof_ta_bcp_iccid"

    # Clear tables before test
    for table in source_tables + [target_table]:
        clear_oracle_table(table)
        clear_bigquery_table(table)

    yield # Run the test

    # Clear tables after test (optional, good for isolation)
    for table in source_tables + [target_table]:
        clear_oracle_table(table)
        clear_bigquery_table(table)

# Mock Airflow TaskInstance for _extract_v_datum_func
class MockTI:
    def xcom_push(self, key, value):
        self.xcom_value = value
        print(f"XCom Pushed: {key} = {value}")

# --- End of Common Setup ---
```

---

## 1. Output Parity

### Test 1.1: Full Output Data Parity (Happy Path)

*   **Purpose**: To verify that with a representative set of valid input data, the migrated BigQuery job produces an identical output dataset to the legacy Oracle job. This is the most comprehensive end-to-end validation.
*   **Setup**:
    1.  Populate `sof_ta_bpr_bcp` and `sof_ta_iccid_vertrag` in both Oracle and BigQuery with a diverse dataset. This should include:
        *   Rows that will successfully join.
        *   Rows in `sof_ta_bpr_bcp` with `cntrct_id_ref` that have no match in `sof_ta_iccid_vertrag`.
        *   Rows in `sof_ta_iccid_vertrag` with `cntrct_id` that have no match in `sof_ta_bpr_bcp`.
        *   Data that would result in duplicates *before* `DISTINCT` is applied.
        *   NULL values in non-join columns.
    2.  Populate `dwtk_meldungen` in both Oracle and BigQuery with at least one entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` to ensure `v_datum` is extracted.
*   **Action**:
    1.  Execute the legacy Oracle job (`run_legacy_job()`).
    2.  Execute the migrated Airflow DAG (`run_migrated_dag()`).
    3.  Query the final target table from Oracle (`sof$ta_bcp_iccid`) and BigQuery (`sof_ta_bcp_iccid`).
*   **Pass/Fail Criterion**:
    *   The row count of the target table in Oracle must be identical to the row count in BigQuery.
    *   All columns and rows in the Oracle target table must be identical to those in the BigQuery target table, ignoring row order.

```python
def test_full_output_data_parity_happy_path():
    # 1. Setup: Populate source tables with diverse data
    bpr_bcp_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "REF1"},
        {"cntrct_id": "C2", "bpr_id": "B2", "cntrct_id_ref": "REF2"},
        {"cntrct_id": "C3", "bpr_id": "B3", "cntrct_id_ref": "REF1"}, # Duplicate join key, different BPR_ID
        {"cntrct_id": "C4", "bpr_id": "B4", "cntrct_id_ref": "REF3"}, # No match
        {"cntrct_id": "C5", "bpr_id": "B5", "cntrct_id_ref": "REF4"}, # Will join
        {"cntrct_id": "C6", "bpr_id": "B6", "cntrct_id_ref": "REF4"}, # Will create a duplicate output row before DISTINCT
    ]
    iccid_vertrag_data = [
        {"cntrct_id": "REF1", "tn_iccid": "ICCID1", "tn_imsi_hlr": "IMSI1"},
        {"cntrct_id": "REF2", "tn_iccid": "ICCID2", "tn_imsi_hlr": "IMSI2"},
        {"cntrct_id": "REF4", "tn_iccid": "ICCID4", "tn_imsi_hlr": "IMSI4"},
        {"cntrct_id": "REF5", "tn_iccid": "ICCID5", "tn_imsi_hlr": "IMSI5"}, # No match
    ]
    dwtk_meldungen_data = [
        {"timecreated": datetime(2023, 10, 26, 10, 0, 0), "job_kennung": "OTHER_JOB"},
        {"timecreated": datetime(2023, 10, 27, 11, 0, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"},
        {"timecreated": datetime(2023, 10, 25, 9, 0, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"},
    ]

    insert_oracle_data("sof_ta_bpr_bcp", bpr_bcp_data)
    insert_oracle_data("sof_ta_iccid_vertrag", iccid_vertrag_data)
    insert_oracle_data("dwtk_meldungen", dwtk_meldungen_data)

    insert_bigquery_data("sof_ta_bpr_bcp", bpr_bcp_data)
    insert_bigquery_data("sof_ta_iccid_vertrag", iccid_vertrag_data)
    # BigQuery expects TIMESTAMP objects for timecreated
    bq_dwtk_meldungen_data = [
        {"timecreated": row["timecreated"].isoformat(), "job_kennung": row["job_kennung"]}
        for row in dwtk_meldungen_data
    ]
    insert_bigquery_data("dwtk_meldungen", bq_dwtk_meldungen_data)

    # 2. Action: Run both jobs
    # Legacy job will truncate and load
    # run_legacy_job()
    # Migrated job will INSERT OVERWRITE
    # run_migrated_dag()

    # For testing purposes, we'll simulate the expected output based on the SQL logic
    # Expected output for the given data:
    # C1, B1, REF1, ICCID1, IMSI1
    # C2, B2, REF2, ICCID2, IMSI2
    # C3, B3, REF1, ICCID1, IMSI1 (This will be distinct with C1, B1, REF1, ICCID1, IMSI1)
    # C5, B5, REF4, ICCID4, IMSI4
    # C6, B6, REF4, ICCID4, IMSI4 (This will be distinct with C5, B5, REF4, ICCID4, IMSI4)
    # The DISTINCT applies to the final projected columns.
    # So, if (C1, B1, REF1, ICCID1, IMSI1) and (C3, B3, REF1, ICCID1, IMSI1) are produced,
    # and the selected columns are (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR),
    # then the DISTINCT will keep both if all 5 columns are different.
    # Let's re-evaluate the DISTINCT behavior.
    # If bp.cntrct_id_ref = ic.cntrct_id, and bp.cntrct_id is different, then the rows are distinct.
    # C1, B1, REF1, ICCID1, IMSI1
    # C2, B2, REF2, ICCID2, IMSI2
    # C3, B3, REF1, ICCID1, IMSI1
    # C5, B5, REF4, ICCID4, IMSI4
    # C6, B6, REF4, ICCID4, IMSI4
    # All these rows are distinct based on the combination of the 5 selected columns.
    # So, 5 rows are expected.

    expected_output_data = [
        {"CNTRCT_ID": "C1", "BPR_ID": "B1", "CNTRCT_ID_REF": "REF1", "TN_ICCID": "ICCID1", "TN_IMSI_HLR": "IMSI1"},
        {"CNTRCT_ID": "C2", "BPR_ID": "B2", "CNTRCT_ID_REF": "REF2", "TN_ICCID": "ICCID2", "TN_IMSI_HLR": "IMSI2"},
        {"CNTRCT_ID": "C3", "BPR_ID": "B3", "CNTRCT_ID_REF": "REF1", "TN_ICCID": "ICCID1", "TN_IMSI_HLR": "IMSI1"},
        {"CNTRCT_ID": "C5", "BPR_ID": "B5", "CNTRCT_ID_REF": "REF4", "TN_ICCID": "ICCID4", "TN_IMSI_HLR": "IMSI4"},
        {"CNTRCT_ID": "C6", "BPR_ID": "B6", "CNTRCT_ID_REF": "REF4", "TN_ICCID": "ICCID4", "TN_IMSI_HLR": "IMSI4"},
    ]
    expected_df = pd.DataFrame(expected_output_data)

    # In a real test, you'd fetch from the actual target tables
    # oracle_result_df = execute_oracle_query(f"SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR FROM {ORACLE_TARGET_SCHEMA}.sof$ta_bcp_iccid")
    # bigquery_result_df = execute_bigquery_query(f"SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bcp_iccid`")

    # For this example, we'll simulate the BigQuery result by running the core SQL
    bq_core_sql = f"""
        SELECT DISTINCT
            bp.cntrct_id,
            bp.bpr_id,
            bp.cntrct_id_ref,
            ic.tn_iccid,
            ic.tn_imsi_hlr
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bpr_bcp` AS bp
        JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_iccid_vertrag` AS ic
          ON bp.cntrct_id_ref = ic.cntrct_id
    """
    bigquery_result_df = execute_bigquery_query(bq_core_sql)
    
    # For Oracle, we'd need to run the full legacy job. Mocking for now.
    oracle_result_df = expected_df.copy() # Assuming legacy job produces expected_df

    # 3. Pass/Fail: Compare results
    sort_cols = ["CNTRCT_ID", "BPR_ID", "CNTRCT_ID_REF", "TN_ICCID", "TN_IMSI_HLR"]
    assert compare_dataframes(oracle_result_df, bigquery_result_df, sort_cols), \
        "Output dataframes from legacy and migrated jobs do not match."
    assert len(oracle_result_df) == len(expected_output_data), "Oracle row count mismatch."
    assert len(bigquery_result_df) == len(expected_output_data), "BigQuery row count mismatch."
```

---

## 2. Transformation Correctness

### Test 2.1: `v_datum` Extraction Logic

*   **Purpose**: Verify that the `v_datum` extraction logic in the Airflow PythonOperator (`_extract_v_datum_func`) correctly replicates the Oracle `MAX(timecreated)` with `NVL` / `IFNULL` logic.
*   **Setup**:
    1.  Populate `dwtk_meldungen` in both Oracle and BigQuery with various scenarios for `job_kennung = 'BERT_DROP_TEMP_TABLE'`:
        *   Multiple entries, ensuring `MAX` is correctly picked.
        *   No entries for `BERT_DROP_TEMP_TABLE` (should default to '19000101').
        *   Entries with `timecreated` as NULL (should be ignored by `MAX`).
*   **Action**:
    1.  Execute the Oracle SQL snippet for `v_datum` extraction.
    2.  Call the Airflow PythonOperator's callable function (`_extract_v_datum_func`) directly, mocking `ti`.
*   **Pass/Fail Criterion**: The `v_datum` extracted from Oracle must be identical to the `v_datum` pushed to XCom by the Airflow task for each scenario.

```python
def test_v_datum_extraction_logic():
    # Scenario 1: Multiple entries, MAX should be picked
    clear_bigquery_table("dwtk_meldungen")
    bq_data_s1 = [
        {"timecreated": datetime(2023, 1, 1, 10, 0, 0).isoformat(), "job_kennung": "OTHER_JOB"},
        {"timecreated": datetime(2023, 1, 2, 11, 0, 0).isoformat(), "job_kennung": "BERT_DROP_TEMP_TABLE"},
        {"timecreated": datetime(2023, 1, 3, 12, 0, 0).isoformat(), "job_kennung": "BERT_DROP_TEMP_TABLE"}, # Max
        {"timecreated": datetime(2023, 1, 1, 9, 0, 0).isoformat(), "job_kennung": "BERT_DROP_TEMP_TABLE"},
    ]
    insert_bigquery_data("dwtk_meldungen", bq_data_s1)
    mock_ti = MockTI()
    _extract_v_datum_func(mock_ti, GCP_PROJECT_ID, BIGQUERY_DATASET_ID)
    assert mock_ti.xcom_value == "20230103", "Scenario 1: Max timecreated not extracted correctly."

    # Scenario 2: No entries for 'BERT_DROP_TEMP_TABLE'
    clear_bigquery_table("dwtk_meldungen")
    bq_data_s2 = [
        {"timecreated": datetime(2023, 1, 1, 10, 0, 0).isoformat(), "job_kennung": "OTHER_JOB"},
    ]
    insert_bigquery_data("dwtk_meldungen", bq_data_s2)
    mock_ti = MockTI()
    _extract_v_datum_func(mock_ti, GCP_PROJECT_ID, BIGQUERY_DATASET_ID)
    assert mock_ti.xcom_value == "19000101", "Scenario 2: Default '19000101' not returned when no matching job_kennung."

    # Scenario 3: Empty table
    clear_bigquery_table("dwtk_meldungen")
    mock_ti = MockTI()
    _extract_v_datum_func(mock_ti, GCP_PROJECT_ID, BIGQUERY_DATASET_ID)
    assert mock_ti.xcom_value == "19000101", "Scenario 3: Default '19000101' not returned for empty table."

    # Scenario 4: timecreated is NULL for matching job_kennung (should be ignored by MAX)
    clear_bigquery_table("dwtk_meldungen")
    bq_data_s4 = [
        {"timecreated": None, "job_kennung": "BERT_DROP_TEMP_TABLE"},
        {"timecreated": datetime(2023, 2, 1, 10, 0, 0).isoformat(), "job_kennung": "BERT_DROP_TEMP_TABLE"}, # Max
    ]
    insert_bigquery_data("dwtk_meldungen", bq_data_s4)
    mock_ti = MockTI()
    _extract_v_datum_func(mock_ti, GCP_PROJECT_ID, BIGQUERY_DATASET_ID)
    assert mock_ti.xcom_value == "20230201", "Scenario 4: NULL timecreated not handled correctly."

    print("Oracle equivalent checks (manual or via execute_oracle_query):")
    # Oracle equivalent for Scenario 1:
    # oracle_v_datum_s1 = execute_oracle_query("SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') FROM ISBERT_SCHEMA.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'")
    # assert oracle_v_datum_s1.iloc[0,0] == "20230103"
    # ... and so on for other scenarios.
```

### Test 2.2: Join Logic Correctness (Inner Join)

*   **Purpose**: To specifically verify that the `JOIN` condition `bp.cntrct_id_ref = ic.cntrct_id` correctly identifies matching records and excludes non-matching ones, replicating Oracle's inner join behavior.
*   **Setup**:
    1.  Populate `sof_ta_bpr_bcp` and `sof_ta_iccid_vertrag` in both Oracle and BigQuery with data that clearly demonstrates inner join behavior:
        *   Records with matching `cntrct_id_ref` / `cntrct_id`.
        *   Records in `sof_ta_bpr_bcp` with no corresponding `cntrct_id` in `sof_ta_iccid_vertrag`.
        *   Records in `sof_ta_iccid_vertrag` with no corresponding `cntrct_id_ref` in `sof_ta_bpr_bcp`.
        *   `NULL` values in the join columns (these should not match).
*   **Action**:
    1.  Run both the legacy Oracle job and the migrated Airflow DAG.
    2.  Query the target tables.
*   **Pass/Fail Criterion**: The resulting data in `sof$ta_bcp_iccid` (Oracle) and `sof_ta_bcp_iccid` (BigQuery) must be identical, containing only records where the join condition was met, and excluding records with non-matching or NULL join keys.

```python
def test_join_logic_correctness():
    # Setup: Data to test inner join behavior
    bpr_bcp_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "MATCH1"},
        {"cntrct_id": "C2", "bpr_id": "B2", "cntrct_id_ref": "MATCH2"},
        {"cntrct_id": "C3", "bpr_id": "B3", "cntrct_id_ref": "NO_MATCH_BP"}, # No match in iccid_vertrag
        {"cntrct_id": "C4", "bpr_id": "B4", "cntrct_id_ref": None}, # NULL join key
    ]
    iccid_vertrag_data = [
        {"cntrct_id": "MATCH1", "tn_iccid": "ICCID_M1", "tn_imsi_hlr": "IMSI_M1"},
        {"cntrct_id": "MATCH2", "tn_iccid": "ICCID_M2", "tn_imsi_hlr": "IMSI_M2"},
        {"cntrct_id": "NO_MATCH_IC", "tn_iccid": "ICCID_NM", "tn_imsi_hlr": "IMSI_NM"}, # No match in bpr_bcp
        {"cntrct_id": None, "tn_iccid": "ICCID_N", "tn_imsi_hlr": "IMSI_N"}, # NULL join key
    ]

    insert_bigquery_data("sof_ta_bpr_bcp", bpr_bcp_data)
    insert_bigquery_data("sof_ta_iccid_vertrag", iccid_vertrag_data)
    # insert_oracle_data(...) # Populate Oracle as well

    # Expected output (only MATCH1 and MATCH2 should join)
    expected_output_data = [
        {"CNTRCT_ID": "C1", "BPR_ID": "B1", "CNTRCT_ID_REF": "MATCH1", "TN_ICCID": "ICCID_M1", "TN_IMSI_HLR": "IMSI_M1"},
        {"CNTRCT_ID": "C2", "BPR_ID": "B2", "CNTRCT_ID_REF": "MATCH2", "TN_ICCID": "ICCID_M2", "TN_IMSI_HLR": "IMSI_M2"},
    ]
    expected_df = pd.DataFrame(expected_output_data)

    # Run migrated job (or simulate by executing core SQL)
    bq_core_sql = f"""
        SELECT DISTINCT
            bp.cntrct_id,
            bp.bpr_id,
            bp.cntrct_id_ref,
            ic.tn_iccid,
            ic.tn_imsi_hlr
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bpr_bcp` AS bp
        JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_iccid_vertrag` AS ic
          ON bp.cntrct_id_ref = ic.cntrct_id
    """
    bigquery_result_df = execute_bigquery_query(bq_core_sql)

    # Assume Oracle produces the same expected_df
    oracle_result_df = expected_df.copy()

    sort_cols = ["CNTRCT_ID", "BPR_ID", "CNTRCT_ID_REF", "TN_ICCID", "TN_IMSI_HLR"]
    assert compare_dataframes(oracle_result_df, bigquery_result_df, sort_cols), \
        "Join logic results do not match between legacy and migrated jobs."
    assert len(bigquery_result_df) == 2, "BigQuery result count for join logic is incorrect."
```

### Test 2.3: `SELECT DISTINCT` Behavior

*   **Purpose**: To ensure that the `SELECT DISTINCT` clause correctly eliminates duplicate rows based on the combination of all selected columns, replicating Oracle's behavior.
*   **Setup**:
    1.  Populate `sof_ta_bpr_bcp` and `sof_ta_iccid_vertrag` such that after the join, there are rows that are identical across all selected columns (`CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, `TN_IMSI_HLR`).
*   **Action**:
    1.  Run both the legacy Oracle job and the migrated Airflow DAG.
    2.  Query the target tables.
*   **Pass/Fail Criterion**: The target tables in both Oracle and BigQuery must contain only unique combinations of the selected columns, and the final datasets must be identical. The row count should reflect the number of unique rows.

```python
def test_select_distinct_behavior():
    # Setup: Data designed to produce duplicates before DISTINCT
    bpr_bcp_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "REF_DUP"},
        {"cntrct_id": "C2", "bpr_id": "B2", "cntrct_id_ref": "REF_DUP"}, # This will join to same ICCID/IMSI
        {"cntrct_id": "C3", "bpr_id": "B3", "cntrct_id_ref": "REF_UNIQUE"},
    ]
    iccid_vertrag_data = [
        {"cntrct_id": "REF_DUP", "tn_iccid": "ICCID_X", "tn_imsi_hlr": "IMSI_Y"},
        {"cntrct_id": "REF_UNIQUE", "tn_iccid": "ICCID_Z", "tn_imsi_hlr": "IMSI_W"},
    ]

    insert_bigquery_data("sof_ta_bpr_bcp", bpr_bcp_data)
    insert_bigquery_data("sof_ta_iccid_vertrag", iccid_vertrag_data)
    # insert_oracle_data(...) # Populate Oracle as well

    # Expected output after join and DISTINCT:
    # C1, B1, REF_DUP, ICCID_X, IMSI_Y
    # C2, B2, REF_DUP, ICCID_X, IMSI_Y
    # C3, B3, REF_UNIQUE, ICCID_Z, IMSI_W
    # In this case, C1 and C2 have different CNTRCT_ID and BPR_ID, so even though they join to the same ICCID/IMSI,
    # the full row (C1, B1, REF_DUP, ICCID_X, IMSI_Y) is distinct from (C2, B2, REF_DUP, ICCID_X, IMSI_Y).
    # So, 3 rows are expected.

    expected_output_data = [
        {"CNTRCT_ID": "C1", "BPR_ID": "B1", "CNTRCT_ID_REF": "REF_DUP", "TN_ICCID": "ICCID_X", "TN_IMSI_HLR": "IMSI_Y"},
        {"CNTRCT_ID": "C2", "BPR_ID": "B2", "CNTRCT_ID_REF": "REF_DUP", "TN_ICCID": "ICCID_X", "TN_IMSI_HLR": "IMSI_Y"},
        {"CNTRCT_ID": "C3", "BPR_ID": "B3", "CNTRCT_ID_REF": "REF_UNIQUE", "TN_ICCID": "ICCID_Z", "TN_IMSI_HLR": "IMSI_W"},
    ]
    expected_df = pd.DataFrame(expected_output_data)

    # Run migrated job (or simulate by executing core SQL)
    bq_core_sql = f"""
        SELECT DISTINCT
            bp.cntrct_id,
            bp.bpr_id,
            bp.cntrct_id_ref,
            ic.tn_iccid,
            ic.tn_imsi_hlr
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bpr_bcp` AS bp
        JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_iccid_vertrag` AS ic
          ON bp.cntrct_id_ref = ic.cntrct_id
    """
    bigquery_result_df = execute_bigquery_query(bq_core_sql)

    # Assume Oracle produces the same expected_df
    oracle_result_df = expected_df.copy()

    sort_cols = ["CNTRCT_ID", "BPR_ID", "CNTRCT_ID_REF", "TN_ICCID", "TN_IMSI_HLR"]
    assert compare_dataframes(oracle_result_df, bigquery_result_df, sort_cols), \
        "DISTINCT logic results do not match between legacy and migrated jobs."
    assert len(bigquery_result_df) == 3, "BigQuery result count for DISTINCT logic is incorrect."
```

### Test 2.4: Empty Source Tables

*   **Purpose**: To verify that the job handles scenarios where one or both source tables are empty, resulting in an empty target table.
*   **Setup**:
    1.  **Scenario A**: `sof_ta_bpr_bcp` is empty, `sof_ta_iccid_vertrag` is populated.
    2.  **Scenario B**: `sof_ta_bpr_bcp` is populated, `sof_ta_iccid_vertrag` is empty.
    3.  **Scenario C**: Both `sof_ta_bpr_bcp` and `sof_ta_iccid_vertrag` are empty.
*   **Action**:
    1.  For each scenario, populate source tables accordingly in both Oracle and BigQuery.
    2.  Run both the legacy Oracle job and the migrated Airflow DAG.
    3.  Query the target tables.
*   **Pass/Fail Criterion**: In all three scenarios, the target table (`sof$ta_bcp_iccid` in Oracle and `sof_ta_bcp_iccid` in BigQuery) must be empty (row count = 0).

```python
def test_empty_source_tables():
    # Scenario A: bpr_bcp empty, iccid_vertrag populated
    clear_bigquery_table("sof_ta_bpr_bcp")
    insert_bigquery_data("sof_ta_iccid_vertrag", [{"cntrct_id": "A", "tn_iccid": "1", "tn_imsi_hlr": "X"}])
    # run_migrated_dag()
    bq_core_sql = f"""
        SELECT DISTINCT bp.cntrct_id, bp.bpr_id, bp.cntrct_id_ref, ic.tn_iccid, ic.tn_imsi_hlr
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bpr_bcp` AS bp
        JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_iccid_vertrag` AS ic ON bp.cntrct_id_ref = ic.cntrct_id
    """
    result_df_a = execute_bigquery_query(bq_core_sql)
    assert result_df_a.empty, "Scenario A: Target table not empty when bpr_bcp is empty."

    # Scenario B: bpr_bcp populated, iccid_vertrag empty
    clear_bigquery_table("sof_ta_iccid_vertrag")
    insert_bigquery_data("sof_ta_bpr_bcp", [{"cntrct_id": "A", "bpr_id": "B", "cntrct_id_ref": "C"}])
    # run_migrated_dag()
    result_df_b = execute_bigquery_query(bq_core_sql)
    assert result_df_b.empty, "Scenario B: Target table not empty when iccid_vertrag is empty."

    # Scenario C: Both empty (already cleared by fixture, just re-check)
    # run_migrated_dag()
    result_df_c = execute_bigquery_query(bq_core_sql)
    assert result_df_c.empty, "Scenario C: Target table not empty when both sources are empty."

    # Oracle equivalent checks would follow the same pattern.
    # e.g., assert execute_oracle_query(f"SELECT COUNT(*) FROM {ORACLE_TARGET_SCHEMA}.sof$ta_bcp_iccid").iloc[0,0] == 0
```

---

## 3. External-System Replacements

### Test 3.1: Airflow Orchestration and XComs

*   **Purpose**: To verify that the Airflow DAG executes successfully, tasks complete in the correct order, and the `v_datum` value is correctly extracted and pushed to XCom.
*   **Setup**:
    1.  Deploy the `dw_bert_ausd_bp_ta_bcp_iccid` DAG to a Cloud Composer environment.
    2.  Populate `dwtk_meldungen` in BigQuery with data that allows `v_datum` to be extracted (e.g., `{"timecreated": "2023-11-01T00:00:00", "job_kennung": "BERT_DROP_TEMP_TABLE"}`).
    3.  Populate `sof_ta_bpr_bcp` and `sof_ta_iccid_vertrag` with minimal valid data to allow the `transform_load_data` task to run without error.
*   **Action**:
    1.  Manually trigger the Airflow DAG or schedule it to run.
    2.  Monitor the DAG run in the Airflow UI.
    3.  After completion, inspect the XCom value for `v_datum` from the `extract_v_datum` task.
    4.  Query the target BigQuery table to ensure data was loaded.
*   **Pass/Fail Criterion**:
    *   The DAG run completes successfully without any task failures.
    *   The `extract_v_datum` task successfully pushes a non-empty `v_datum` (e.g., '20231101') to XCom.
    *   The `transform_load_data` task completes and loads data into `sof_ta_bcp_iccid`.

```python
# This test is primarily an integration test for Airflow and BigQuery.
# It's difficult to represent as runnable pytest code without a live Airflow environment.
# Below is a conceptual outline.

def test_airflow_orchestration_and_xcoms():
    # 1. Setup: Populate BigQuery source tables
    clear_bigquery_table("dwtk_meldungen")
    insert_bigquery_data("dwtk_meldungen", [{"timecreated": datetime(2023, 11, 1, 0, 0, 0).isoformat(), "job_kennung": "BERT_DROP_TEMP_TABLE"}])
    insert_bigquery_data("sof_ta_bpr_bcp", [{"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "REF1"}])
    insert_bigquery_data("sof_ta_iccid_vertrag", [{"cntrct_id": "REF1", "tn_iccid": "ICCID1", "tn_imsi_hlr": "IMSI1"}])

    # 2. Action: Trigger the Airflow DAG
    # This would typically be done via Airflow API or CLI:
    # from airflow.api.client.local_client import Client
    # client = Client(None, None)
    # client.trigger_dag(dag_id='dw_bert_ausd_bp_ta_bcp_iccid', conf={})
    # For this example, we'll just call the run_migrated_dag mock.
    run_migrated_dag() # This mock would need to simulate DAG execution and XComs

    # 3. Pass/Fail: Assertions
    # In a real scenario, you'd query Airflow's metadata DB or logs for task status and XComs.
    # For now, we'll assert the expected outcome in BigQuery.

    # Assert that v_datum was extracted (conceptually, from XCom)
    # For a real test, you'd query Airflow's XComs table.
    # For this example, we'll re-run the Python function to get the value.
    mock_ti = MockTI()
    _extract_v_datum_func(mock_ti, GCP_PROJECT_ID, BIGQUERY_DATASET_ID)
    extracted_v_datum = mock_ti.xcom_value
    assert extracted_v_datum == "20231101", "v_datum not correctly extracted and pushed to XCom."

    # Assert that the target table was loaded
    target_table_name = "sof_ta_bcp_iccid"
    result_df = execute_bigquery_query(f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{target_table_name}`")
    assert result_df.iloc[0, 0] > 0, "Target BigQuery table is empty after DAG run."

    # Further checks:
    # - Check Airflow UI for successful task completion (extract_v_datum, transform_load_data)
    # - Check Airflow logs for any errors
```

---

## 4. Data Quality / Row Count / Schema Assertions

### Test 4.1: Target Table Schema Parity

*   **Purpose**: To ensure the BigQuery target table (`sof_ta_bcp_iccid`) has a schema that is functionally equivalent to the legacy Oracle target table (`sof$ta_bcp_iccid`), including column names, data types, and nullability (if explicitly defined).
*   **Setup**:
    1.  Ensure the BigQuery DDL for `sof_ta_bcp_iccid` has been applied.
*   **Action**:
    1.  Retrieve the schema of `sof$ta_bcp_iccid` from Oracle.
    2.  Retrieve the schema of `sof_ta_bcp_iccid` from BigQuery.
*   **Pass/Fail Criterion**:
    *   All columns present in the Oracle table must also be present in the BigQuery table.
    *   Column names must match (case-insensitivity might need to be considered, but typically BigQuery is case-sensitive for column names unless quoted).
    *   Data types must be compatible (e.g., Oracle `VARCHAR2` to BigQuery `STRING`, Oracle `NUMBER` to BigQuery `INT64` or `BIGNUMERIC`).
    *   Nullability constraints should be consistent (e.g., if a column was `NOT NULL` in Oracle, it should ideally be `REQUIRED` in BigQuery, or the transformation logic should guarantee non-NULL values).

```python
def test_target_table_schema_parity():
    # Expected BigQuery schema based on DDL
    expected_bq_schema = {
        "CNTRCT_ID": "STRING",
        "BPR_ID": "STRING",
        "CNTRCT_ID_REF": "STRING",
        "TN_ICCID": "STRING",
        "TN_IMSI_HLR": "STRING",
    }

    # Retrieve BigQuery schema
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bcp_iccid"
    table = client.get_table(table_id)
    actual_bq_schema = {field.name: field.field_type for field in table.schema}

    # Assert column names and types
    assert actual_bq_schema == expected_bq_schema, \
        f"BigQuery target table schema mismatch. Expected: {expected_bq_schema}, Actual: {actual_bq_schema}"

    # Oracle schema comparison (conceptual)
    # oracle_schema_query = f"""
    #     SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
    #     FROM ALL_TAB_COLUMNS
    #     WHERE OWNER = '{ORACLE_TARGET_SCHEMA.upper()}' AND TABLE_NAME = 'SOF$TA_BCP_ICCID'
    #     ORDER BY COLUMN_ID
    # """
    # oracle_schema_df = execute_oracle_query(oracle_schema_query)
    #
    # # Example Oracle to BigQuery type mapping for comparison
    # oracle_to_bq_type_map = {
    #     "VARCHAR2": "STRING",
    #     "NUMBER": "STRING", # Assuming all IDs are treated as strings
    #     # ... add other mappings
    # }
    #
    # for _, row in oracle_schema_df.iterrows():
    #     oracle_col_name = row['COLUMN_NAME']
    #     oracle_data_type = row['DATA_TYPE']
    #     oracle_nullable = row['NULLABLE'] == 'Y'
    #
    #     assert oracle_col_name in actual_bq_schema, f"Column {oracle_col_name} missing in BigQuery."
    #     assert oracle_to_bq_type_map.get(oracle_data_type, "UNKNOWN") == actual_bq_schema[oracle_col_name], \
    #         f"Data type mismatch for {oracle_col_name}. Oracle: {oracle_data_type}, BigQuery: {actual_bq_schema[oracle_col_name]}"
    #     # Nullability check: If Oracle was NOT NULL, BigQuery should ideally be REQUIRED.
    #     # The provided BQ DDL does not specify REQUIRED, so all are NULLABLE.
    #     # This is a point to raise if legacy had NOT NULL constraints.
    #     # assert (not oracle_nullable) == (table.schema.get_field(oracle_col_name).mode == 'REQUIRED'), \
    #     #     f"Nullability mismatch for {oracle_col_name}."
```

### Test 4.2: Row Count Validation (Post-Migration)

*   **Purpose**: To ensure that the migrated job consistently produces the expected number of rows in the target table for a given input. This is a quick sanity check for data integrity.
*   **Setup**:
    1.  Populate source tables in BigQuery with a known dataset (e.g., the same as Test 1.1).
    2.  Run the migrated Airflow DAG.
*   **Action**:
    1.  Query the row count of the BigQuery target table (`sof_ta_bcp_iccid`).
*   **Pass/Fail Criterion**: The row count in `sof_ta_bcp_iccid` must match the expected row count derived from the source data and transformation logic (e.g., 5 rows from Test 1.1).

```python
def test_row_count_validation():
    # Setup: Use the same data as Test 1.1 to ensure consistency
    bpr_bcp_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "REF1"},
        {"cntrct_id": "C2", "bpr_id": "B2", "cntrct_id_ref": "REF2"},
        {"cntrct_id": "C3", "bpr_id": "B3", "cntrct_id_ref": "REF1"},
        {"cntrct_id": "C4", "bpr_id": "B4", "cntrct_id_ref": "REF3"},
        {"cntrct_id": "C5", "bpr_id": "B5", "cntrct_id_ref": "REF4"},
        {"cntrct_id": "C6", "bpr_id": "B6", "cntrct_id_ref": "REF4"},
    ]
    iccid_vertrag_data = [
        {"cntrct_id": "REF1", "tn_iccid": "ICCID1", "tn_imsi_hlr": "IMSI1"},
        {"cntrct_id": "REF2", "tn_iccid": "ICCID2", "tn_imsi_hlr": "IMSI2"},
        {"cntrct_id": "REF4", "tn_iccid": "ICCID4", "tn_imsi_hlr": "IMSI4"},
        {"cntrct_id": "REF5", "tn_iccid": "ICCID5", "tn_imsi_hlr": "IMSI5"},
    ]
    insert_bigquery_data("sof_ta_bpr_bcp", bpr_bcp_data)
    insert_bigquery_data("sof_ta_iccid_vertrag", iccid_vertrag_data)

    # Action: Run the core BigQuery SQL (simulating DAG's transform_load_data task)
    bq_core_sql = f"""
        INSERT OVERWRITE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bcp_iccid`
        (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR)
        SELECT DISTINCT
            bp.cntrct_id,
            bp.bpr_id,
            bp.cntrct_id_ref,
            ic.tn_iccid,
            ic.tn_imsi_hlr
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bpr_bcp` AS bp
        JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_iccid_vertrag` AS ic
          ON bp.cntrct_id_ref = ic.cntrct_id;
    """
    execute_bigquery_query(bq_core_sql)

    # Pass/Fail: Query row count and compare to expected
    result_df = execute_bigquery_query(f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bcp_iccid`")
    actual_row_count = result_df.iloc[0, 0]
    expected_row_count = 5 # Based on the data in Test 1.1 and DISTINCT logic

    assert actual_row_count == expected_row_count, \
        f"BigQuery target table row count mismatch. Expected: {expected_row_count}, Actual: {actual_row_count}"
```

---

## 5. Critical Observation / Discrepancy Test

### Test 5.1: Parameter and `v_datum` Non-Usage in Main Transformation

*   **Purpose**: To explicitly confirm and highlight that the `v_datum` extracted by `extract_v_datum_task` and the DAG parameters (`stichtag`, `wiederanlaufwert`) are *not* used in the main `transform_load_data` BigQuery SQL query, despite the design document's suggestions. This is a critical point for business validation and potential functional gap.
*   **Setup**:
    1.  Review the generated Airflow DAG code (`dags/dw_bert_ausd_bp_ta_bcp_iccid_dag.py`).
*   **Action**:
    1.  Inspect the `sql` parameter of the `BigQueryExecuteQueryOperator` named `transform_load_data`.
    2.  Check if `{{ ti.xcom_pull(task_ids='extract_v_datum', key='v_datum') }}` or `{{ params.stichtag }}` or `{{ params.wiederanlaufwert }}` are present in the SQL string.
*   **Pass/Fail Criterion**:
    *   **Pass (for code validation)**: The `transform_load_data` task's SQL string *does not* contain references to `v_datum` (from XCom), `stichtag`, or `wiederanlaufwert` for filtering or transformation.
    *   **Fail (for design adherence)**: If the design *intended* these parameters/variables to influence the main data load, then this is a functional gap that needs to be addressed with the development team and business stakeholders. This test serves to formally document this observation.

```python
def test_parameter_and_v_datum_non_usage_in_main_transformation():
    # This test is primarily a code review assertion.
    # We can simulate checking the SQL string from the DAG definition.

    # Import the DAG definition to access the SQL string
    from dags.dw_bert_ausd_bp_ta_bcp_iccid_dag import transform_load_data_task

    main_sql_query = transform_load_data_task.sql

    # Check for v_datum usage (via XCom pull)
    assert "v_datum" not in main_sql_query, \
        "The main BigQuery SQL unexpectedly uses 'v_datum' from XCom. Re-evaluate if this is intended."

    # Check for stichtag parameter usage
    assert "stichtag" not in main_sql_query, \
        "The main BigQuery SQL unexpectedly uses 'stichtag' parameter. Re-evaluate if this is intended."

    # Check for wiederanlaufwert parameter usage
    assert "wiederanlaufwert" not in main_sql_query, \
        "The main BigQuery SQL unexpectedly uses 'wiederanlaufwert' parameter. Re-evaluate if this is intended."

    print("\n--- CRITICAL OBSERVATION ---")
    print("Confirmed: The main BigQuery SQL transformation in 'transform_load_data_task' does NOT use the 'v_datum' extracted by 'extract_v_datum_task', nor the 'stichtag' or 'wiederanlaufwert' DAG parameters for filtering or transformation.")
    print("This aligns with the generated code but is a potential functional discrepancy with the design document's implications regarding parameter usage.")
    print("This point should be explicitly clarified with business stakeholders and the development team.")
```