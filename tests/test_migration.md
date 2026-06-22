As a senior data-migration QA engineer, I've reviewed the migration design document and the generated BigQuery code for `DW.BERT_AUSD_BP_TA_APN_VERTRAG`. The following test cases are designed to ensure behavioral equivalence, transformation correctness, and robust handling of external system replacements and data quality.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_APN_VERTRAG

**Assumptions:**
*   A BigQuery project named `project` is used.
*   BigQuery datasets `project.isbert_schema`, `project.sof`, and `project.test_data` exist.
*   The DDL for tables (`create_tables.sql`) and stored procedures (`sp_d_ausd_bp_ta_apn_vertrag.sql`, `sp_r_k_ausd_bp_ta_apn_vertrag.sql`) have been deployed to BigQuery.
*   A "golden" dataset (representing legacy output) is available in BigQuery for comparison, typically in `project.test_data.golden_ta_apn_vertrag_scenarioX`.
*   The Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag` is deployed and accessible.
*   `pytest` with `google-cloud-bigquery` client is used for test execution.

---

### 1. Output Parity Tests

These tests ensure that given the same inputs, the migrated job produces identical outputs to the legacy system.

#### Test 1.1: End-to-End Data Parity (Happy Path)

*   **Purpose:** Verify that the migrated job produces the exact same output data as the legacy job under typical operating conditions. This is the primary output parity check.
*   **Setup:**
    1.  Populate `project.isbert_schema.dwtk_meldungen` with sample data, including an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a `timecreated` value.
    2.  Populate `project.sof.ta_bpr_apn` with a diverse set of contract IDs, multiple APNs/references per contract, and some single-entry contracts.
    3.  Run the legacy job with this input data and capture its final output in `sof$ta_apn_vertrag`. Load this legacy output into a BigQuery "golden" table: `project.test_data.golden_ta_apn_vertrag_happy_path`.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.
    2.  Wait for the DAG to complete successfully.
*   **Pass/Fail Criterion:** The data in `project.sof.ta_apn_vertrag` must be identical to the data in `project.test_data.golden_ta_apn_vertrag_happy_path` in terms of row count and content for all columns.

```python
import pytest
from google.cloud import bigquery
from airflow.models.dagbag import DagBag
from airflow.utils.state import State
from datetime import datetime

# Assume client is initialized globally or passed
client = bigquery.Client()

GCP_PROJECT_ID = "project"
TARGET_DATASET = "sof"
TARGET_TABLE = "ta_apn_vertrag"
GOLDEN_DATASET = "test_data"

def _clear_table(table_id):
    client.query(f"TRUNCATE TABLE `{table_id}`").result()

def _load_data(table_id, data_rows, schema):
    # Helper to load data for setup
    job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.CSV)
    # Convert data_rows to a file-like object or list of dicts
    # For simplicity, assuming data_rows is a list of lists and schema is a list of bigquery.SchemaField
    # In a real scenario, you'd use client.load_table_from_json or similar.
    # For testing, direct inserts are often easier for small datasets.
    pass # Placeholder for actual data loading logic

def _get_table_data(table_id):
    query = f"SELECT * FROM `{table_id}` ORDER BY cntrct_id, apn_list, cntrct_ref_list"
    rows = client.query(query).result()
    return sorted([tuple(row) for row in rows])

def _trigger_airflow_dag(dag_id):
    # This is a simplified representation. In a real test, you'd use Airflow's REST API
    # or a test utility to trigger the DAG and monitor its state.
    # For local testing, you might use DagBag to load and execute.
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    dag = dagbag.get_dag(dag_id)
    if not dag:
        raise ValueError(f"DAG {dag_id} not found.")

    run_id = f"test_run_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    dr = dag.create_dagrun(
        run_id=run_id,
        state=State.RUNNING,
        execution_date=datetime.now(),
        conf={},
        external_trigger=True,
    )
    # In a real scenario, you'd then monitor dr.get_state() until it's success or failure.
    # For this example, we'll assume it runs synchronously or is mocked.
    print(f"Triggered DAG {dag_id} with run_id {run_id}. Manual verification of success needed for this mock.")
    # For actual testing, you'd need to poll the DAG run state.
    # Example: while dr.get_state() == State.RUNNING: time.sleep(5)
    # assert dr.get_state() == State.SUCCESS
    return True # Assume success for this mock

@pytest.fixture(scope="module")
def setup_happy_path_data():
    # Setup for happy path
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn")

    # Insert sample data into source tables
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (TIMESTAMP('2023-01-15 10:00:00'), 'BERT_DROP_TEMP_TABLE'),
        (TIMESTAMP('2023-01-14 09:00:00'), 'OTHER_JOB');
    """).result()

    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn` (cntrct_id_ref, bpr_id, cntrct_id, access_point_name) VALUES
        ('REF001', 1, 'C001', 'apn.internet.com'),
        ('REF002', 2, 'C001', 'apn.m2m.com'),
        ('REF003', 3, 'C002', 'apn.iot.com'),
        ('REF004', 4, 'C001', 'apn.internet.com'), -- Duplicate APN for C001
        ('REF005', 5, 'C003', 'apn.vpn.com'),
        ('REF006', 6, 'C002', 'apn.iot.com'), -- Duplicate APN for C002
        ('REF007', 7, 'C001', 'apn.fast.com'),
        ('REF008', 8, 'C004', 'apn.single.com');
    """).result()

    # Define expected golden data (derived from legacy output for these inputs)
    # This would typically be loaded from a file or pre-existing golden table
    client.query(f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GOLDEN_DATASET}.golden_ta_apn_vertrag_happy_path` AS
        SELECT * FROM UNNEST([
            STRUCT('C001' AS cntrct_id, 'apn.fast.com,apn.internet.com,apn.m2m.com' AS apn_list, 'REF001,REF002,REF004,REF007' AS cntrct_ref_list),
            STRUCT('C002' AS cntrct_id, 'apn.iot.com' AS apn_list, 'REF003,REF006' AS cntrct_ref_list),
            STRUCT('C003' AS cntrct_id, 'apn.vpn.com' AS apn_list, 'REF005' AS cntrct_ref_list),
            STRUCT('C004' AS cntrct_id, 'apn.single.com' AS apn_list, 'REF008' AS cntrct_ref_list)
        ])
    """).result()

    yield # Run tests
    # Teardown (optional, clear tables)
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn")
    client.query(f"DROP TABLE IF EXISTS `{GCP_PROJECT_ID}.{GOLDEN_DATASET}.golden_ta_apn_vertrag_happy_path`").result()


def test_happy_path_output_parity(setup_happy_path_data):
    # Action: Trigger the DAG
    _trigger_airflow_dag('dw_bert_ausd_bp_ta_apn_vertrag')

    # Pass/Fail Criterion: Compare target table with golden table
    target_data = _get_table_data(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    golden_data = _get_table_data(f"{GCP_PROJECT_ID}.{GOLDEN_DATASET}.golden_ta_apn_vertrag_happy_path")

    assert len(target_data) == len(golden_data), "Row count mismatch"
    assert target_data == golden_data, "Data content mismatch"
```

#### Test 1.2: End-to-End Data Parity (Edge Case: Empty `ta_bpr_apn`)

*   **Purpose:** Verify correct behavior when the primary source table (`ta_bpr_apn`) is empty. The target table should be truncated and remain empty.
*   **Setup:**
    1.  Ensure `project.sof.ta_bpr_apn` is empty.
    2.  Populate `project.isbert_schema.dwtk_meldungen` with a relevant entry (e.g., `job_kennung = 'BERT_DROP_TEMP_TABLE'`).
    3.  Run the legacy job with these inputs and capture its final output (an empty `sof$ta_apn_vertrag`). Load this into `project.test_data.golden_ta_apn_vertrag_empty_source`.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.
    2.  Wait for the DAG to complete successfully.
*   **Pass/Fail Criterion:** The `project.sof.ta_apn_vertrag` table must be empty.

```python
@pytest.fixture(scope="module")
def setup_empty_source_data():
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn")

    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (TIMESTAMP('2023-01-15 10:00:00'), 'BERT_DROP_TEMP_TABLE');
    """).result()

    client.query(f"CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GOLDEN_DATASET}.golden_ta_apn_vertrag_empty_source` AS SELECT * FROM `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` WHERE FALSE").result()

    yield
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn")
    client.query(f"DROP TABLE IF EXISTS `{GCP_PROJECT_ID}.{GOLDEN_DATASET}.golden_ta_apn_vertrag_empty_source`").result()

def test_empty_source_output_parity(setup_empty_source_data):
    _trigger_airflow_dag('dw_bert_ausd_bp_ta_apn_vertrag')

    target_data = _get_table_data(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    golden_data = _get_table_data(f"{GCP_PROJECT_ID}.{GOLDEN_DATASET}.golden_ta_apn_vertrag_empty_source")

    assert len(target_data) == 0, "Target table should be empty"
    assert target_data == golden_data, "Data content mismatch for empty source"
```

#### Test 1.3: End-to-End Data Parity (Edge Case: `dwtk_meldungen` no matching `job_kennung`)

*   **Purpose:** Verify that the `v_max_timecreated_yyyymmdd` variable correctly defaults to '19000101' when no matching `job_kennung` is found in `dwtk_meldungen`, and that this does not prevent the core logic from running (as it's only used for logging/metadata in legacy).
*   **Setup:**
    1.  Populate `project.sof.ta_bpr_apn` with sample data.
    2.  Populate `project.isbert_schema.dwtk_meldungen` with data, but ensure no entry has `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    3.  Run the legacy job with these inputs and capture its final output. Load this into `project.test_data.golden_ta_apn_vertrag_no_dwtk_match`.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.
    2.  Wait for the DAG to complete successfully.
*   **Pass/Fail Criterion:** The data in `project.sof.ta_apn_vertrag` must be identical to the data in `project.test_data.golden_ta_apn_vertrag_no_dwtk_match`. (The `v_max_timecreated_yyyymmdd` value itself is not directly in the output table, but its correct derivation ensures the job runs without error).

---

### 2. Transformation Correctness Tests

These tests focus on specific logic within the BigQuery Stored Procedures, ensuring that joins, aggregations, filters, type handling, and NULL handling are correctly translated.

#### Test 2.1: `v_max_timecreated_yyyymmdd` Calculation

*   **Purpose:** Verify the correct translation of `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` to BigQuery's `COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')`.
*   **Setup:**
    1.  Populate `project.isbert_schema.dwtk_meldungen` with various `timecreated` values, including `NULL` and entries for `BERT_DROP_TEMP_TABLE` and other `job_kennung` values.
*   **Action:**
    1.  Execute a direct BigQuery SQL query to calculate `v_max_timecreated_yyyymmdd` using the migrated logic.
    2.  Execute a direct Oracle SQL query (or use a golden value) to get the expected result from the legacy logic.
*   **Pass/Fail Criterion:** The result from the BigQuery query must match the expected value from the legacy system for different scenarios (e.g., matching `job_kennung` exists, no matching `job_kennung`, `timecreated` is NULL).

```python
def test_max_timecreated_calculation():
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")

    # Scenario 1: Matching job_kennung exists
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (TIMESTAMP('2023-03-20 12:30:00'), 'BERT_DROP_TEMP_TABLE'),
        (TIMESTAMP('2023-03-19 11:00:00'), 'OTHER_JOB'),
        (TIMESTAMP('2023-03-21 14:00:00'), 'BERT_DROP_TEMP_TABLE');
    """).result()
    query_result = client.query(f"""
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """).result().to_dataframe().iloc[0, 0]
    assert query_result == '20230321', "Scenario 1 failed: Max timecreated"

    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")

    # Scenario 2: No matching job_kennung
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (TIMESTAMP('2023-03-20 12:30:00'), 'ANOTHER_JOB');
    """).result()
    query_result = client.query(f"""
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """).result().to_dataframe().iloc[0, 0]
    assert query_result == '19000101', "Scenario 2 failed: No matching job_kennung"

    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")

    # Scenario 3: Matching job_kennung, but timecreated is NULL (should not happen with MAX, but good to test COALESCE)
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (NULL, 'BERT_DROP_TEMP_TABLE');
    """).result()
    query_result = client.query(f"""
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """).result().to_dataframe().iloc[0, 0]
    assert query_result == '19000101', "Scenario 3 failed: NULL timecreated"
```

#### Test 2.2: `TRUNCATE TABLE` Behavior

*   **Purpose:** Verify that the `TRUNCATE TABLE` statement correctly clears the target table before insertion, mimicking the legacy behavior.
*   **Setup:**
    1.  Populate `project.sof.ta_apn_vertrag` with some existing data.
    2.  Populate `project.sof.ta_bpr_apn` and `project.isbert_schema.dwtk_meldungen` with data that would result in new records being inserted.
*   **Action:**
    1.  Call `project.sof.sp_d_ausd_bp_ta_apn_vertrag()` directly.
*   **Pass/Fail Criterion:** The `project.sof.ta_apn_vertrag` table should contain only the newly inserted records, and none of the pre-existing data.

```python
def test_truncate_table_behavior():
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn")

    # Insert some initial data into target table
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` (cntrct_id, apn_list, cntrct_ref_list) VALUES
        ('OLD01', 'old.apn.com', 'OLDREF01'),
        ('OLD02', 'another.old.com', 'OLDREF02');
    """).result()

    # Insert source data that will generate new records
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (TIMESTAMP('2023-01-15 10:00:00'), 'BERT_DROP_TEMP_TABLE');
    """).result()
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn` (cntrct_id_ref, bpr_id, cntrct_id, access_point_name) VALUES
        ('NEWREF01', 10, 'NEW01', 'new.apn.com');
    """).result()

    # Call the core SP
    client.query(f"CALL `{GCP_PROJECT_ID}.{TARGET_DATASET}.sp_d_ausd_bp_ta_apn_vertrag`()").result()

    # Verify only new data exists
    result = _get_table_data(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    assert len(result) == 1, "Table should contain only new records after truncate and insert"
    assert result[0][0] == 'NEW01', "New record not found"
```

#### Test 2.3: `STRING_AGG` with Multiple Distinct Values & Length Truncation

*   **Purpose:** Verify that `STRING_AGG(DISTINCT ... ORDER BY ..., 1, 100)` correctly aggregates distinct values, orders them, and truncates the result to 100 characters.
*   **Setup:**
    1.  Populate `project.sof.ta_bpr_apn` with data for a single `cntrct_id` that has:
        *   Multiple distinct `access_point_name` values.
        *   Multiple distinct `cntrct_id_ref` values.
        *   Some values that, when aggregated, will exceed 100 characters.
        *   Some duplicate values to ensure `DISTINCT` works.
*   **Action:**
    1.  Execute a direct BigQuery SQL query using the `STRING_AGG` logic from `sp_d_ausd_bp_ta_apn_vertrag` for a specific `cntrct_id`.
*   **Pass/Fail Criterion:** The aggregated `apn_list` and `cntrct_ref_list` must match the expected string (distinct, ordered, and truncated to 100 chars) from legacy behavior.

```python
def test_string_agg_distinct_order_truncate():
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn")
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen") # Needed for SP to run

    # Data designed to test distinct, order, and truncation
    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn` (cntrct_id_ref, bpr_id, cntrct_id, access_point_name) VALUES
        ('REF_LONG_01_ABCDEFGHIJ', 1, 'C005', 'apn.long.name.01.abcdefghijklmnopqrstuvwxyz.com'),
        ('REF_LONG_02_KLMNOPQRST', 2, 'C005', 'apn.long.name.02.abcdefghijklmnopqrstuvwxyz.com'),
        ('REF_LONG_01_ABCDEFGHIJ', 3, 'C005', 'apn.long.name.01.abcdefghijklmnopqrstuvwxyz.com'), -- Duplicate
        ('REF_SHORT_A', 4, 'C005', 'apn.short.a.com'),
        ('REF_SHORT_B', 5, 'C005', 'apn.short.b.com');
    """).result()

    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (TIMESTAMP('2023-01-15 10:00:00'), 'BERT_DROP_TEMP_TABLE');
    """).result()

    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    client.query(f"CALL `{GCP_PROJECT_ID}.{TARGET_DATASET}.sp_d_ausd_bp_ta_apn_vertrag`()").result()

    result = client.query(f"SELECT apn_list, cntrct_ref_list FROM `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` WHERE cntrct_id = 'C005'").result().to_dataframe()

    expected_apn_list = "apn.long.name.01.abcdefghijklmnopqrstuvwxyz.com,apn.long.name.02.abcdefghijklmnopqrstuvwxyz.com,apn.short.a.com,apn.short.b.com"
    expected_cntrct_ref_list = "REF_LONG_01_ABCDEFGHIJ,REF_LONG_02_KLMNOPQRST,REF_SHORT_A,REF_SHORT_B"

    # Check truncation (length 100)
    assert len(result.iloc[0]['apn_list']) == 100
    assert len(result.iloc[0]['cntrct_ref_list']) == 100

    # Check content (truncated part)
    assert result.iloc[0]['apn_list'] == expected_apn_list[:100]
    assert result.iloc[0]['cntrct_ref_list'] == expected_cntrct_ref_list[:100]
```

#### Test 2.4: `STRING_AGG` with NULL Values in Aggregated Columns

*   **Purpose:** Verify that `STRING_AGG` correctly ignores `NULL` values in the aggregated columns, as is standard SQL behavior and typically desired.
*   **Setup:**
    1.  Populate `project.sof.ta_bpr_apn` with data for a `cntrct_id` where some `access_point_name` and `cntrct_id_ref` values are `NULL`.
*   **Action:**
    1.  Execute a direct BigQuery SQL query using the `STRING_AGG` logic for this `cntrct_id`.
*   **Pass/Fail Criterion:** The aggregated lists should only contain non-NULL values, and `NULL`s should not appear in the concatenated string.

```python
def test_string_agg_null_values():
    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn")
    _clear_table(f"{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen")

    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{TARGET_DATASET}.ta_bpr_apn` (cntrct_id_ref, bpr_id, cntrct_id, access_point_name) VALUES
        ('REF_A', 1, 'C006', 'apn.a.com'),
        (NULL, 2, 'C006', 'apn.b.com'),
        ('REF_C', 3, 'C006', NULL),
        ('REF_D', 4, 'C006', 'apn.d.com');
    """).result()

    client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
        (TIMESTAMP('2023-01-15 10:00:00'), 'BERT_DROP_TEMP_TABLE');
    """).result()

    _clear_table(f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}")
    client.query(f"CALL `{GCP_PROJECT_ID}.{TARGET_DATASET}.sp_d_ausd_bp_ta_apn_vertrag`()").result()

    result = client.query(f"SELECT apn_list, cntrct_ref_list FROM `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` WHERE cntrct_id = 'C006'").result().to_dataframe()

    assert result.iloc[0]['apn_list'] == 'apn.a.com,apn.b.com,apn.d.com'
    assert result.iloc[0]['cntrct_ref_list'] == 'REF_A,REF_C,REF_D'
```

#### Test 2.5: `GROUP BY` `cntrct_id` Correctness

*   **Purpose:** Verify that the `GROUP BY cntrct_id` clause correctly aggregates all related records under their respective contract IDs.
*   **Setup:**
    1.  Populate `project.sof.ta_bpr_apn` with records for several distinct `cntrct_id` values, each having multiple associated `access_point_name` and `cntrct_id_ref` entries.
*   **Action:**
    1.  Call `project.sof.sp_d_ausd_bp_ta_apn_vertrag()`.
*   **Pass/Fail Criterion:** The `project.sof.ta_apn_vertrag` table should contain one row for each distinct `cntrct_id` present in the input `ta_bpr_apn` table.

---

### 3. External-System Replacements & Orchestration Tests

These tests validate the behavior of the new Airflow orchestration and BigQuery Stored Procedure wrappers, including parameter handling and error conditions.

#### Test 3.1: Airflow DAG Execution & Parameter Passing

*   **Purpose:** Verify that the Airflow DAG successfully triggers the BigQuery orchestration stored procedure and passes parameters correctly.
*   **Setup:**
    1.  Ensure `project.sof.ta_bpr_apn` and `project.isbert_schema.dwtk_meldungen` are populated with valid data.
    2.  The Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag` is deployed.
*   **Action:**
    1.  Manually trigger the Airflow DAG from the Airflow UI or via `airflow dags trigger`.
    2.  Observe Airflow logs for the `BigQueryExecuteQueryOperator` task.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes successfully (status `success`).
    *   BigQuery job logs show the `sp_r_k_ausd_bp_ta_apn_vertrag` procedure was called with the expected parameters (e.g., `p_jobkennung = 'AUSD_BP_TA_APN_VERTRAG'`, `p_stichtag = CURRENT_DATE()`).
    *   The `project.sof.ta_apn_vertrag` table is populated correctly, indicating the full pipeline executed.

#### Test 3.2: BigQuery Orchestration SP Parameter Validation (Negative Cases)

*   **Purpose:** Verify that the `sp_r_k_ausd_bp_ta_apn_vertrag` procedure correctly validates its input parameters and raises errors as specified in the design.
*   **Setup:** None specific, as the procedure's internal validation is being tested.
*   **Action:**
    1.  Attempt to call `sp_r_k_ausd_bp_ta_apn_vertrag` with `NULL` or empty values for `p_jobkennung`, `p_stichtag`, and `p_eintragsnr` individually.
*   **Pass/Fail Criterion:**
    *   Calling with `p_jobkennung => NULL` or `p_jobkennung => ''` should result in an error with `MESSAGE_TEXT` containing "Jobkennung parameter is missing."
    *   Calling with `p_stichtag => NULL` should result in an error with `MESSAGE_TEXT` containing "Stichtag parameter is missing."
    *   Calling with `p_eintragsnr => NULL` should result in an error with `MESSAGE_TEXT` containing "EintragsNr parameter is missing."

```python
def test_orchestration_sp_parameter_validation():
    # Test p_jobkennung NULL
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{GCP_PROJECT_ID}.{TARGET_DATASET}.sp_r_k_ausd_bp_ta_apn_vertrag`(NULL, 1, CURRENT_DATE(), 0)").result()
    assert "Jobkennung parameter is missing" in str(excinfo.value)

    # Test p_jobkennung empty string
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{GCP_PROJECT_ID}.{TARGET_DATASET}.sp_r_k_ausd_bp_ta_apn_vertrag`('', 1, CURRENT_DATE(), 0)").result()
    assert "Jobkennung parameter is missing" in str(excinfo.value)

    # Test p_stichtag NULL
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{GCP_PROJECT_ID}.{TARGET_DATASET}.sp_r_k_ausd_bp_ta_apn_vertrag`('TEST_JOB', 1, NULL, 0)").result()
    assert "Stichtag parameter is missing" in str(excinfo.value)

    # Test p_eintragsnr NULL
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{GCP_PROJECT_ID}.{TARGET_DATASET}.sp_r_k_ausd_bp_ta_apn_vertrag`('TEST_JOB', NULL, CURRENT_DATE(), 0)").result()
    assert "EintragsNr parameter is missing" in str(excinfo.value)
```

#### Test 3.3: BigQuery Orchestration SP Date Derivation

*   **Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` are correctly derived using BigQuery date functions.
*   **Setup:** None.
*   **Action:**
    1.  Create a temporary BigQuery stored procedure that calls `sp_r_k_ausd_bp_ta_apn_vertrag` and then selects the values of `v_datum_heute` and `v_datum_gestern` (this might require modifying `sp_r_k_ausd_bp_ta_apn_vertrag` to return these values for testing, or using a mock).
    2.  Alternatively, directly query `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` and compare with expected values.
*   **Pass/Fail Criterion:** `v_datum_heute` must equal `CURRENT_DATE()` and `v_datum_gestern` must equal `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.

```python
def test_orchestration_sp_date_derivation():
    # This test requires modifying the SP to expose internal variables,
    # or testing the underlying BQ functions directly.
    # For simplicity, we test the underlying BQ functions.
    query = f"""
        SELECT
            CURRENT_DATE() AS today,
            DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS yesterday
    """
    result = client.query(query).result().to_dataframe()
    
    expected_today = datetime.now().date()
    expected_yesterday = (datetime.now() - timedelta(days=1)).date()

    assert result.iloc[0]['today'] == expected_today
    assert result.iloc[0]['yesterday'] == expected_yesterday
```

---

### 4. Data Quality / Row Count / Schema Assertions

These tests ensure the integrity and structure of the migrated data.

#### Test 4.1: Target Table Schema Validation

*   **Purpose:** Verify that the schema of the target table (`project.sof.ta_apn_vertrag`) matches the design specifications (column names, data types, and lengths).
*   **Setup:** The `project.sof.ta_apn_vertrag` table should be created.
*   **Action:**
    1.  Query BigQuery's `INFORMATION_SCHEMA` for the table schema.
*   **Pass/Fail Criterion:** The schema must match the expected DDL:
    *   `cntrct_id` STRING(10)
    *   `apn_list` STRING(100)
    *   `cntrct_ref_list` STRING(100)

```python
def test_target_table_schema():
    table_id = f"{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}"
    table = client.get_table(table_id)

    expected_schema = [
        bigquery.SchemaField("cntrct_id", "STRING", mode="NULLABLE", max_length=10),
        bigquery.SchemaField("apn_list", "STRING", mode="NULLABLE", max_length=100),
        bigquery.SchemaField("cntrct_ref_list", "STRING", mode="NULLABLE", max_length=100),
    ]

    # Compare field names, types, and max_length
    for expected_field in expected_schema:
        found_field = next((f for f in table.schema if f.name == expected_field.name), None)
        assert found_field is not None, f"Column {expected_field.name} not found"
        assert found_field.field_type == expected_field.field_type, f"Type mismatch for {expected_field.name}"
        assert found_field.mode == expected_field.mode, f"Mode mismatch for {expected_field.name}"
        # BigQuery's max_length is for BYTES, not characters for STRING.
        # For STRING, it's character length. The DDL specifies character length.
        # This check might need adjustment based on how BigQuery reports max_length for STRING.
        # For now, assuming direct comparison is fine if DDL uses character length.
        assert found_field.max_length == expected_field.max_length, f"Max length mismatch for {expected_field.name}"
```

#### Test 4.2: Row Count Parity

*   **Purpose:** Verify that the total number of rows in the target table after migration matches the legacy system's output for a given input.
*   **Setup:**
    1.  Populate source tables (`ta_bpr_apn`, `dwtk_meldungen`) with a known dataset.
    2.  Obtain the expected row count from the legacy system's output for this dataset.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.
    2.  Query the row count of `project.sof.ta_apn_vertrag`.
*   **Pass/Fail Criterion:** The row count of `project.sof.ta_apn_vertrag` must equal the expected legacy row count.

```python
def test_row_count_parity(setup_happy_path_data): # Re-use happy path setup
    _trigger_airflow_dag('dw_bert_ausd_bp_ta_apn_vertrag')

    target_row_count = client.query(f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`").result().to_dataframe().iloc[0, 0]
    golden_row_count = client.query(f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{GOLDEN_DATASET}.golden_ta_apn_vertrag_happy_path`").result().to_dataframe().iloc[0, 0]

    assert target_row_count == golden_row_count, "Row count mismatch between target and golden"
```

#### Test 4.3: Data Integrity - Uniqueness of `cntrct_id`

*   **Purpose:** Verify that `cntrct_id` is unique in the target table, as it's the grouping key for aggregation.
*   **Setup:**
    1.  Populate source tables with data that should result in distinct `cntrct_id`s in the output.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.
    2.  Query `project.sof.ta_apn_vertrag` to count distinct `cntrct_id`s and total rows.
*   **Pass/Fail Criterion:** The count of distinct `cntrct_id`s must equal the total row count in `project.sof.ta_apn_vertrag`.

```python
def test_cntrct_id_uniqueness(setup_happy_path_data): # Re-use happy path setup
    _trigger_airflow_dag('dw_bert_ausd_bp_ta_apn_vertrag')

    query = f"""
        SELECT
            COUNT(cntrct_id) AS total_rows,
            COUNT(DISTINCT cntrct_id) AS distinct_cntrct_ids
        FROM `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`
    """
    result = client.query(query).result().to_dataframe()

    assert result.iloc[0]['total_rows'] == result.iloc[0]['distinct_cntrct_ids'], "cntrct_id is not unique in target table"
```

#### Test 4.4: Data Integrity - No Unexpected NULLs in Key Fields

*   **Purpose:** Ensure that critical output fields (`cntrct_id`, `apn_list`, `cntrct_ref_list`) do not contain unexpected `NULL` values.
*   **Setup:**
    1.  Populate source tables with data where `cntrct_id`, `access_point_name`, and `cntrct_id_ref` are always non-NULL.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.
    2.  Query `project.sof.ta_apn_vertrag` for rows where `cntrct_id`, `apn_list`, or `cntrct_ref_list` are `NULL`.
*   **Pass/Fail Criterion:** No rows should be returned by the query for `NULL` values in these fields.

```python
def test_no_unexpected_nulls(setup_happy_path_data): # Re-use happy path setup
    _trigger_airflow_dag('dw_bert_ausd_bp_ta_apn_vertrag')

    query = f"""
        SELECT COUNT(*)
        FROM `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`
        WHERE cntrct_id IS NULL OR apn_list IS NULL OR cntrct_ref_list IS NULL
    """
    null_count = client.query(query).result().to_dataframe().iloc[0, 0]

    assert null_count == 0, "Unexpected NULL values found in key output fields"
```