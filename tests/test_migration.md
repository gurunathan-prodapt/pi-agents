# Migration Validation Tests for `r_ausd_v_ta_disc_zusgf.ksh`

This document outlines migration validation tests for the BigQuery stored procedure `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`, which replaces the legacy KornShell job `r_ausd_v_ta_disc_zusgf.ksh`. The tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

**Assumptions:**
*   A BigQuery project and dataset (`isbert_ds`) are configured.
*   All DDLs for target tables (`sof_ta_disc_zusgf`, `job_control`, `error_log`, `job_message_log`, `job_result_log`) have been executed.
*   All BigQuery stored procedures (`d_ausd_v_ta_disc_zusgf_sql_logic`, `k_ausd_v_ta_disc_zusgf_controller`, `r_ausd_v_ta_disc_zusgf_wrapper`) have been created.
*   Source data from Oracle (`isbert_schema.dwtk_meldungen`, `sof$ta_discount`) has been ingested into their respective BigQuery tables (`isbert_ds.dwtk_meldungen`, `isbert_ds.sof_ta_discount`).
*   The test execution environment can interact with BigQuery (e.g., Python with `google-cloud-bigquery` client library).
*   For output parity, a "golden" dataset from the legacy system (Oracle `sof$ta_disc_zusgf`) is available, generated with a known input.

---

## Test Case 1: Schema Validation of Target Table

**Purpose:** Verify that the target table `isbert_ds.sof_ta_disc_zusgf` has the correct schema (column names and data types) as defined in the migration design.

**Setup:**
1.  Ensure the `isbert_ds.sof_ta_disc_zusgf` table exists in BigQuery.

**Action:**
1.  Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `isbert_ds.sof_ta_disc_zusgf` table.

**Pass/Fail Criterion:**
*   The table `isbert_ds.sof_ta_disc_zusgf` must exist.
*   It must contain exactly four columns with the following names and data types:
    *   `cntrct_id`: `INT64`
    *   `cntrct_obj_version`: `INT64`
    *   `disc_vector_ty`: `STRING`
    *   `rabatt_alle`: `STRING`

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    return bigquery.Client()

def test_sof_ta_disc_zusgf_schema(bq_client):
    """Verifies the schema of the target table."""
    table_id = "isbert_ds.sof_ta_disc_zusgf"
    try:
        table = bq_client.get_table(table_id)
    except Exception as e:
        pytest.fail(f"Table {table_id} not found or inaccessible: {e}")

    expected_schema = {
        "cntrct_id": "INT64",
        "cntrct_obj_version": "INT64",
        "disc_vector_ty": "STRING",
        "rabatt_alle": "STRING"
    }

    assert len(table.schema) == len(expected_schema), \
        f"Number of columns mismatch. Expected {len(expected_schema)}, got {len(table.schema)}"

    for field in table.schema:
        assert field.name in expected_schema, f"Unexpected column: {field.name}"
        assert field.field_type == expected_schema[field.name], \
            f"Data type mismatch for column {field.name}: Expected {expected_schema[field.name]}, got {field.field_type}"

    print(f"Schema for {table_id} is correct.")
```

---

## Test Case 2: Output Parity - Full Data Set

**Purpose:** Verify that the migrated job produces identical output data in `isbert_ds.sof_ta_disc_zusgf` compared to the legacy job's `sof$ta_disc_zusgf` for a comprehensive, representative dataset. This is the primary test for behavioral equivalence.

**Setup:**
1.  **Legacy Golden Data:**
    *   Prepare a comprehensive set of test data for `isbert_schema.dwtk_meldungen` and `sof$ta_discount` in the Oracle legacy environment. This should cover various scenarios: single/multiple discounts per contract, NULL values, different `disc_vector_ty` values, and `BERT_DROP_TEMP_TABLE` entries.
    *   Execute the legacy job (`r_ausd_v_ta_disc_zusgf.ksh`) with this test data.
    *   Extract the resulting data from `sof$ta_disc_zusgf` into a "golden" CSV or JSON file.
2.  **Migrated Environment:**
    *   Clear `isbert_ds.sof_ta_disc_zusgf`.
    *   Populate `isbert_ds.dwtk_meldungen` and `isbert_ds.sof_ta_discount` with the *exact same* test data used in the legacy run.
    *   Clear all logging tables (`job_control`, `error_log`, `job_message_log`, `job_result_log`).

**Action:**
1.  Execute the migrated BigQuery wrapper stored procedure: `CALL isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper();`
2.  Retrieve all data from `isbert_ds.sof_ta_disc_zusgf`.
3.  Retrieve relevant logs from `isbert_ds.job_control`, `isbert_ds.job_message_log`, `isbert_ds.job_result_log`.

**Pass/Fail Criterion:**
*   The data retrieved from `isbert_ds.sof_ta_disc_zusgf` must be *identical* (row count and content, ignoring column order if not explicitly specified) to the "golden" data extracted from the legacy `sof$ta_disc_zusgf`.
*   The `job_control` table for the executed `v_job_kennung` and `v_eintrags_nr` must show `status = 'SUCCESS'`.
*   The `job_result_log` must contain a `RECORD_COUNT` entry matching the number of rows in the target table.
*   No errors should be logged in `isbert_ds.error_log`.

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.fixture
def setup_full_data_test(bq_client):
    """Sets up the BigQuery environment for a full data parity test."""
    # Clear target and logging tables
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_control`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.error_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_message_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_result_log`").result()

    # Insert representative test data into source tables
    # This data should be carefully crafted to match the legacy golden run input.
    # Example data (replace with actual comprehensive test data):
    bq_client.query("""
        INSERT INTO `isbert_ds.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00')),
        ('OTHER_JOB', TIMESTAMP('2023-01-14 09:00:00')),
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-16 11:00:00'));
    """).result()

    bq_client.query("""
        INSERT INTO `isbert_ds.sof_ta_discount` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt, rabatthoehe) VALUES
        (100, 1, 'TYPE_A', 'Discount A', 10),
        (100, 1, 'TYPE_A', 'Discount B', 5),
        (101, 1, 'TYPE_B', 'Special Disc', 20),
        (102, 1, 'TYPE_A', 'Single Disc', 15),
        (103, 1, 'TYPE_C', NULL, 0), -- NULL rabatt
        (104, 1, 'TYPE_D', 'No Height', NULL), -- NULL rabatthoehe
        (105, 1, 'TYPE_E', 'Long Discount Name 1', 12),
        (105, 1, 'TYPE_E', 'Long Discount Name 2', 8),
        (105, 1, 'TYPE_E', 'Long Discount Name 3', 15),
        (105, 1, 'TYPE_E', 'Long Discount Name 4', 20),
        (105, 1, 'TYPE_E', 'Long Discount Name 5', 25),
        (105, 1, 'TYPE_E', 'Long Discount Name 6', 30),
        (105, 1, 'TYPE_E', 'Long Discount Name 7', 35),
        (105, 1, 'TYPE_E', 'Long Discount Name 8', 40),
        (105, 1, 'TYPE_E', 'Long Discount Name 9', 45),
        (105, 1, 'TYPE_E', 'Long Discount Name 10', 50),
        (105, 1, 'TYPE_E', 'Long Discount Name 11', 55),
        (105, 1, 'TYPE_E', 'Long Discount Name 12', 60),
        (105, 1, 'TYPE_E', 'Long Discount Name 13', 65),
        (105, 1, 'TYPE_E', 'Long Discount Name 14', 70),
        (105, 1, 'TYPE_E', 'Long Discount Name 15', 75),
        (105, 1, 'TYPE_E', 'Long Discount Name 16', 80),
        (105, 1, 'TYPE_E', 'Long Discount Name 17', 85),
        (105, 1, 'TYPE_E', 'Long Discount Name 18', 90),
        (105, 1, 'TYPE_E', 'Long Discount Name 19', 95),
        (105, 1, 'TYPE_E', 'Long Discount Name 20', 100),
        (106, 1, 'TYPE_F', 'Discount X', 25),
        (106, 2, 'TYPE_F', 'Discount Y', 30); -- Different version
    """).result()

    # Load golden data from a file (e.g., CSV) generated by the legacy job
    # IMPORTANT: Replace "path/to/legacy_sof_ta_disc_zusgf_golden.csv" with your actual golden file path.
    try:
        golden_data_df = pd.read_csv("path/to/legacy_sof_ta_disc_zusgf_golden.csv")
    except FileNotFoundError:
        pytest.fail("Golden data file not found. Please provide the path to 'legacy_sof_ta_disc_zusgf_golden.csv'.")

    # Ensure column types match BigQuery's expected types for comparison
    golden_data_df['cntrct_id'] = golden_data_df['cntrct_id'].astype('Int64')
    golden_data_df['cntrct_obj_version'] = golden_data_df['cntrct_obj_version'].astype('Int64')
    golden_data_df['disc_vector_ty'] = golden_data_df['disc_vector_ty'].astype(str)
    golden_data_df['rabatt_alle'] = golden_data_df['rabatt_alle'].astype(str).replace('None', None) # Handle potential 'None' string from CSV for NULLs

    yield golden_data_df

def test_full_output_parity(bq_client, setup_full_data_test):
    """Compares the output of the migrated job with golden data and checks logs."""
    golden_data_df = setup_full_data_test

    # Execute the wrapper stored procedure
    bq_client.query("CALL `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`()").result()

    # Retrieve results from BigQuery
    query_job = bq_client.query("SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle FROM `isbert_ds.sof_ta_disc_zusgf` ORDER BY cntrct_id, cntrct_obj_version, disc_vector_ty")
    migrated_data_df = query_job.to_dataframe()

    # Ensure column types match for comparison
    migrated_data_df['cntrct_id'] = migrated_data_df['cntrct_id'].astype('Int64')
    migrated_data_df['cntrct_obj_version'] = migrated_data_df['cntrct_obj_version'].astype('Int64')
    migrated_data_df['disc_vector_ty'] = migrated_data_df['disc_vector_ty'].astype(str)
    migrated_data_df['rabatt_alle'] = migrated_data_df['rabatt_alle'].astype(str).replace('None', None)

    # Compare dataframes
    assert_frame_equal(golden_data_df.sort_values(by=['cntrct_id', 'cntrct_obj_version', 'disc_vector_ty']).reset_index(drop=True),
                       migrated_data_df.sort_values(by=['cntrct_id', 'cntrct_obj_version', 'disc_vector_ty']).reset_index(drop=True),
                       check_dtype=True,
                       obj_as_bytes=False) # Important for string comparison

    # Verify job status and logs
    job_control_query = """
        SELECT status, message FROM `isbert_ds.job_control`
        WHERE job_kennung = 'R_AUSD_V_TA_DISC_ZUSGF'
        ORDER BY start_time DESC LIMIT 1
    """
    job_control_result = bq_client.query(job_control_query).to_dataframe()
    assert not job_control_result.empty, "No job control entry found."
    assert job_control_result.iloc[0]['status'] == 'SUCCESS', f"Job status was not SUCCESS: {job_control_result.iloc[0]['status']}"

    error_log_query = "SELECT COUNT(*) FROM `isbert_ds.error_log` WHERE job_kennung = 'R_AUSD_V_TA_DISC_ZUSGF'"
    error_count = bq_client.query(error_log_query).to_dataframe().iloc[0, 0]
    assert error_count == 0, f"Errors found in error_log: {error_count}"

    record_count_query = """
        SELECT metric_value FROM `isbert_ds.job_result_log`
        WHERE job_kennung = 'BERT_V_TA_DISC_ZUSGF' AND metric_name = 'RECORD_COUNT'
        ORDER BY log_time DESC LIMIT 1
    """
    logged_record_count_df = bq_client.query(record_count_query).to_dataframe()
    assert not logged_record_count_df.empty, "No record count logged."
    logged_record_count = logged_record_count_df.iloc[0, 0]
    assert logged_record_count == len(migrated_data_df), "Logged record count mismatch."

    print("Full output parity and logging checks passed.")
```

---

## Test Case 3: Transformation Correctness - `v_datum` Calculation

**Purpose:** Verify that the `v_datum` variable, derived from `isbert_ds.dwtk_meldungen`, is correctly calculated as the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

**Note:** The provided BigQuery stored procedure `d_ausd_v_ta_disc_zusgf_sql_logic` calculates `v_datum` but does not explicitly use it in the `INSERT` statement. This test verifies the calculation itself, as it's part of the translated logic. If `v_datum` was intended to be used as a filter or parameter in the core logic, this would be a functional discrepancy.

**Setup:**
1.  Clear `isbert_ds.dwtk_meldungen`.
2.  Insert specific test data into `isbert_ds.dwtk_meldungen` with varying `timecreated` values and `job_kennung`.

**Action:**
1.  Directly query the `dwtk_meldungen` table using the `v_datum` calculation logic from the stored procedure.

**Pass/Fail Criterion:**
*   The calculated `v_datum` (as `FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated)))` for `BERT_DROP_TEMP_TABLE`) must match the expected value based on the inserted test data.
*   If no matching `job_kennung` is found, `v_datum` should default to `'19000101'`.

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def setup_v_datum_test(bq_client):
    """Sets up the BigQuery environment for v_datum calculation tests."""
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()
    # Clear other tables to ensure clean state for any potential side effects
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()
    yield

def test_v_datum_calculation(bq_client, setup_v_datum_test):
    """Verifies the correct calculation of v_datum."""
    query_v_datum = """
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
        FROM
            `isbert_ds.dwtk_meldungen`
        WHERE
            job_kennung = 'BERT_DROP_TEMP_TABLE';
    """

    # Scenario 1: Multiple BERT_DROP_TEMP_TABLE entries
    bq_client.query("""
        INSERT INTO `isbert_ds.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00')),
        ('OTHER_JOB', TIMESTAMP('2023-01-14 09:00:00')),
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-16 11:00:00')),
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-10 08:00:00'));
    """).result()
    result = bq_client.query(query_v_datum).to_dataframe()
    calculated_v_datum = result.iloc[0, 0]
    assert calculated_v_datum == '20230116', \
        f"v_datum calculation incorrect for multiple entries: Expected '20230116', got {calculated_v_datum}"

    # Scenario 2: No BERT_DROP_TEMP_TABLE entries
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result() # Clear for new scenario
    bq_client.query("""
        INSERT INTO `isbert_ds.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('OTHER_JOB', TIMESTAMP('2023-01-14 09:00:00'));
    """).result()
    result = bq_client.query(query_v_datum).to_dataframe()
    calculated_v_datum_no_match = result.iloc[0, 0]
    assert calculated_v_datum_no_match == '19000101', \
        f"v_datum calculation incorrect for no match: Expected '19000101', got {calculated_v_datum_no_match}"

    # Scenario 3: Empty dwtk_meldungen table
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()
    result = bq_client.query(query_v_datum).to_dataframe()
    calculated_v_datum_empty = result.iloc[0, 0]
    assert calculated_v_datum_empty == '19000101', \
        f"v_datum calculation incorrect for empty table: Expected '19000101', got {calculated_v_datum_empty}"

    print("v_datum calculation tests passed.")
```

---

## Test Case 4: Transformation Correctness - `STRING_AGG` and Concatenation

**Purpose:** Verify that the `STRING_AGG` function correctly concatenates discount information, including formatting, ordering, and handling of NULLs, replicating the Oracle pipelined function's behavior. Also, verify the `LEFT(..., 500)` truncation.

**Note on NULL Handling:** Oracle's `||` operator treats `NULL` as an empty string for concatenation. BigQuery's `CONCAT` function returns `NULL` if *any* argument is `NULL`. The provided BigQuery code uses `CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)')`. This will result in `NULL` if `rabatt` or `rabatthoehe` is `NULL`. `STRING_AGG` by default ignores `NULL` values. This test verifies the behavior of the *migrated BigQuery code as written*. If strict Oracle `||` equivalence (where `NULL` becomes `''`) is required, the BigQuery `CONCAT` expressions would need to use `IFNULL(rabatt, '')` and `IFNULL(CAST(rabatthoehe AS STRING), '')`.

**Setup:**
1.  Clear `isbert_ds.sof_ta_discount` and `isbert_ds.sof_ta_disc_zusgf`.
2.  Insert specific test data into `isbert_ds.sof_ta_discount` covering:
    *   Single discount per contract.
    *   Multiple discounts per contract.
    *   NULL `rabatt` values.
    *   NULL `rabatthoehe` values.
    *   Combinations that result in a `rabatt_alle` string exceeding 500 characters.
    *   Different `cntrct_obj_version` for the same `cntrct_id`.

**Action:**
1.  Execute the `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` stored procedure.
2.  Query `isbert_ds.sof_ta_disc_zusgf` to inspect the `rabatt_alle` column.

**Pass/Fail Criterion:**
*   For contracts with multiple discounts, `rabatt_alle` must be a comma-separated string of `rabatt (rabatthoehe%)` values, ordered alphabetically by the concatenated string.
*   If `rabatt` or `rabatthoehe` is `NULL`, the corresponding discount entry should not appear in `rabatt_alle` (due to `CONCAT` returning `NULL` and `STRING_AGG` ignoring `NULL`s). If all discounts for a contract are `NULL`-producing, `rabatt_alle` should be `NULL`.
*   The `rabatt_alle` string must be truncated to a maximum of 500 characters.

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.fixture
def setup_string_agg_test(bq_client):
    """Sets up the BigQuery environment for STRING_AGG and truncation tests."""
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result() # For v_datum, though not used here

    bq_client.query("""
        INSERT INTO `isbert_ds.sof_ta_discount` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt, rabatthoehe) VALUES
        (100, 1, 'TYPE_A', 'Discount A', 10),
        (100, 1, 'TYPE_A', 'Discount B', 5),
        (101, 1, 'TYPE_B', 'Special Disc', 20),
        (102, 1, 'TYPE_A', 'Single Disc', 15),
        (103, 1, 'TYPE_C', NULL, 0), -- rabatt is NULL -> CONCAT is NULL -> STRING_AGG ignores
        (104, 1, 'TYPE_D', 'No Height', NULL), -- rabatthoehe is NULL -> CONCAT is NULL -> STRING_AGG ignores
        (105, 1, 'TYPE_E', 'Long Discount Name 1', 12),
        (105, 1, 'TYPE_E', 'Long Discount Name 2', 8),
        (105, 1, 'TYPE_E', 'Long Discount Name 3', 15),
        (105, 1, 'TYPE_E', 'Long Discount Name 4', 20),
        (105, 1, 'TYPE_E', 'Long Discount Name 5', 25),
        (105, 1, 'TYPE_E', 'Long Discount Name 6', 30),
        (105, 1, 'TYPE_E', 'Long Discount Name 7', 35),
        (105, 1, 'TYPE_E', 'Long Discount Name 8', 40),
        (105, 1, 'TYPE_E', 'Long Discount Name 9', 45),
        (105, 1, 'TYPE_E', 'Long Discount Name 10', 50),
        (105, 1, 'TYPE_E', 'Long Discount Name 11', 55),
        (105, 1, 'TYPE_E', 'Long Discount Name 12', 60),
        (105, 1, 'TYPE_E', 'Long Discount Name 13', 65),
        (105, 1, 'TYPE_E', 'Long Discount Name 14', 70),
        (105, 1, 'TYPE_E', 'Long Discount Name 15', 75),
        (105, 1, 'TYPE_E', 'Long Discount Name 16', 80),
        (105, 1, 'TYPE_E', 'Long Discount Name 17', 85),
        (105, 1, 'TYPE_E', 'Long Discount Name 18', 90),
        (105, 1, 'TYPE_E', 'Long Discount Name 19', 95),
        (105, 1, 'TYPE_E', 'Long Discount Name 20', 100),
        (106, 1, 'TYPE_F', 'Discount X', 25),
        (106, 2, 'TYPE_F', 'Discount Y', 30);
    """).result()

    yield

def test_string_agg_and_truncation(bq_client, setup_string_agg_test):
    """Verifies STRING_AGG logic, including NULL handling and truncation."""
    bq_client.query("CALL `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`()").result()

    query_results = bq_client.query("""
        SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle
        FROM `isbert_ds.sof_ta_disc_zusgf`
        ORDER BY cntrct_id, cntrct_obj_version, disc_vector_ty
    """).to_dataframe()

    # Expected results based on BigQuery's CONCAT behavior (NULL if any part is NULL)
    # and STRING_AGG ordering.
    long_string_base = 'Long Discount Name 1 (12%), Long Discount Name 10 (50%), Long Discount Name 11 (55%), Long Discount Name 12 (60%), Long Discount Name 13 (65%), Long Discount Name 14 (70%), Long Discount Name 15 (75%), Long Discount Name 16 (80%), Long Discount Name 17 (85%), Long Discount Name 18 (90%), Long Discount Name 19 (95%), Long Discount Name 2 (8%), Long Discount Name 20 (100%), Long Discount Name 3 (15%), Long Discount Name 4 (20%), Long Discount Name 5 (25%), Long Discount Name 6 (30%), Long Discount Name 7 (35%), Long Discount Name 8 (40%), Long Discount Name 9 (45%)'

    expected_data = pd.DataFrame([
        {'cntrct_id': 100, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_A', 'rabatt_alle': 'Discount A (10%), Discount B (5%)'},
        {'cntrct_id': 101, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_B', 'rabatt_alle': 'Special Disc (20%)'},
        {'cntrct_id': 102, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_A', 'rabatt_alle': 'Single Disc (15%)'},
        {'cntrct_id': 103, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_C', 'rabatt_alle': None}, # rabatt is NULL, CONCAT is NULL, STRING_AGG ignores
        {'cntrct_id': 104, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_D', 'rabatt_alle': None}, # rabatthoehe is NULL, CONCAT is NULL, STRING_AGG ignores
        {'cntrct_id': 105, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_E', 'rabatt_alle': long_string_base[:500]}, # This string will be truncated
        {'cntrct_id': 106, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_F', 'rabatt_alle': 'Discount X (25%)'},
        {'cntrct_id': 106, 'cntrct_obj_version': 2, 'disc_vector_ty': 'TYPE_F', 'rabatt_alle': 'Discount Y (30%)'}
    ])

    # Ensure column types match for comparison
    expected_data['cntrct_id'] = expected_data['cntrct_id'].astype('Int64')
    expected_data['cntrct_obj_version'] = expected_data['cntrct_obj_version'].astype('Int64')
    expected_data['disc_vector_ty'] = expected_data['disc_vector_ty'].astype(str)
    expected_data['rabatt_alle'] = expected_data['rabatt_alle'].astype(str).replace('None', None)

    migrated_data_df = query_results
    migrated_data_df['cntrct_id'] = migrated_data_df['cntrct_id'].astype('Int64')
    migrated_data_df['cntrct_obj_version'] = migrated_data_df['cntrct_obj_version'].astype('Int64')
    migrated_data_df['disc_vector_ty'] = migrated_data_df['disc_vector_ty'].astype(str)
    migrated_data_df['rabatt_alle'] = migrated_data_df['rabatt_alle'].astype(str).replace('None', None)

    assert_frame_equal(expected_data.sort_values(by=['cntrct_id', 'cntrct_obj_version', 'disc_vector_ty']).reset_index(drop=True),
                       migrated_data_df.sort_values(by=['cntrct_id', 'cntrct_obj_version', 'disc_vector_ty']).reset_index(drop=True),
                       check_dtype=True,
                       obj_as_bytes=False)

    # Additional check for truncation length
    long_string_row = migrated_data_df[migrated_data_df['cntrct_id'] == 105]
    assert len(long_string_row['rabatt_alle'].iloc[0]) <= 500, "rabatt_alle string not truncated correctly."

    print("STRING_AGG and truncation tests passed.")
```

---

## Test Case 5: External System Replacement - Logging and Job Control

**Purpose:** Verify that the BigQuery logging and job control tables (`job_control`, `job_message_log`, `error_log`, `job_result_log`) are correctly populated and updated, replacing the shell script's logging and job control mechanisms.

**Setup:**
1.  Clear all logging tables: `job_control`, `error_log`, `job_message_log`, `job_result_log`.
2.  Insert minimal data into `isbert_ds.dwtk_meldungen` and `isbert_ds.sof_ta_discount` to allow the job to run successfully.
3.  Insert a 'RUNNING' job into `job_control` with an older `eintrags_nr` to test the deactivation logic.

**Action:**
1.  Execute the `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper` stored procedure.
2.  Query each logging table to inspect its contents.

**Pass/Fail Criterion:**
*   `isbert_ds.job_control`:
    *   One entry for the current job with `status = 'SUCCESS'`.
    *   If an older 'RUNNING' job was present, its status should be updated to 'DEACTIVATED'.
*   `isbert_ds.job_message_log`:
    *   Contains 'INFO' messages for job start and completion from both wrapper and controller.
*   `isbert_ds.job_result_log`:
    *   Contains one entry with `metric_name = 'RECORD_COUNT'` and `metric_value` matching the actual row count in `isbert_ds.sof_ta_disc_zusgf`.
*   `isbert_ds.error_log`:
    *   Must be empty (for a successful run).

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def setup_logging_test(bq_client):
    """Sets up the BigQuery environment for logging and job control tests."""
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_control`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.error_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_message_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_result_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()

    # Insert minimal data for successful run
    bq_client.query("""
        INSERT INTO `isbert_ds.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-01 00:00:00'));
    """).result()
    bq_client.query("""
        INSERT INTO `isbert_ds.sof_ta_discount` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt, rabatthoehe) VALUES
        (1, 1, 'TYPE_X', 'Test Disc', 1);
    """).result()

    # Insert a dummy running job to test deactivation
    bq_client.query("""
        INSERT INTO `isbert_ds.job_control` (job_kennung, eintrags_nr, start_time, status, message) VALUES
        ('R_AUSD_V_TA_DISC_ZUSGF', 12345, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'RUNNING', 'Old job running');
    """).result()

    yield

def test_logging_and_job_control(bq_client, setup_logging_test):
    """Verifies correct logging and job control table updates for a successful run."""
    # Execute the wrapper stored procedure
    bq_client.query("CALL `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`()").result()

    # 1. Verify job_control table
    job_control_df = bq_client.query("""
        SELECT job_kennung, eintrags_nr, status, message
        FROM `isbert_ds.job_control`
        ORDER BY start_time DESC
    """).to_dataframe()

    assert len(job_control_df) >= 2, "Expected at least two entries in job_control (new job + deactivated old job)"

    # Check new job status (most recent entry for R_AUSD_V_TA_DISC_ZUSGF)
    new_job_entry = job_control_df[job_control_df['job_kennung'] == 'R_AUSD_V_TA_DISC_ZUSGF'].iloc[0]
    assert new_job_entry['status'] == 'SUCCESS', f"New job status is not SUCCESS: {new_job_entry['status']}"
    assert 'Job completed successfully' in new_job_entry['message'], "New job success message missing"

    # Check old job deactivation (entry with eintrags_nr = 12345)
    old_job_entry = job_control_df[job_control_df['eintrags_nr'] == 12345]
    assert not old_job_entry.empty, "Old job entry (eintrags_nr 12345) not found."
    assert old_job_entry.iloc[0]['status'] == 'DEACTIVATED', f"Old job not deactivated: {old_job_entry.iloc[0]['status']}"
    assert 'Deactivated by new job run' in old_job_entry.iloc[0]['message'], "Old job deactivation message missing"

    # 2. Verify job_message_log
    message_log_df = bq_client.query("""
        SELECT message, log_level
        FROM `isbert_ds.job_message_log`
        WHERE job_kennung = 'R_AUSD_V_TA_DISC_ZUSGF' OR job_kennung = 'BERT_V_TA_DISC_ZUSGF'
        ORDER BY log_time
    """).to_dataframe()

    assert len(message_log_df) >= 4, "Expected at least 4 message log entries (wrapper start/end, controller start/end)"
    assert any('Starting r_ausd_v_ta_disc_zusgf_wrapper' in msg for msg in message_log_df['message']), "Wrapper start message missing"
    assert any('Starting k_ausd_v_ta_disc_zusgf_controller' in msg for msg in message_log_df['message']), "Controller start message missing"
    assert any('k_ausd_v_ta_disc_zusgf_controller completed successfully' in msg for msg in message_log_df['message']), "Controller success message missing"
    assert any('r_ausd_v_ta_disc_zusgf_wrapper completed successfully' in msg for msg in message_log_df['message']), "Wrapper success message missing"
    assert all(level == 'INFO' for level in message_log_df['log_level']), "Unexpected log level in message log"

    # 3. Verify job_result_log
    result_log_df = bq_client.query("""
        SELECT metric_name, metric_value, table_name
        FROM `isbert_ds.job_result_log`
        WHERE job_kennung = 'BERT_V_TA_DISC_ZUSGF'
        ORDER BY log_time DESC LIMIT 1
    """).to_dataframe()

    assert not result_log_df.empty, "No entry found in job_result_log"
    assert result_log_df.iloc[0]['metric_name'] == 'RECORD_COUNT', "Metric name mismatch in job_result_log"
    assert result_log_df.iloc[0]['table_name'] == 'sof_ta_disc_zusgf', "Table name mismatch in job_result_log"

    target_row_count = bq_client.query("SELECT COUNT(*) FROM `isbert_ds.sof_ta_disc_zusgf`").to_dataframe().iloc[0, 0]
    assert result_log_df.iloc[0]['metric_value'] == target_row_count, "Logged record count mismatch."

    # 4. Verify error_log (should be empty for successful run)
    error_count = bq_client.query("SELECT COUNT(*) FROM `isbert_ds.error_log`").to_dataframe().iloc[0, 0]
    assert error_count == 0, f"Errors found in error_log: {error_count}"

    print("Logging and job control tests passed for successful run.")
```

---

## Test Case 6: Error Handling and Logging

**Purpose:** Verify that the job correctly handles errors during execution, logs them appropriately, and updates the job status to 'FAILED'.

**Setup:**
1.  Clear all logging tables.
2.  Insert minimal data into source tables to allow the job to start.
3.  Temporarily modify the `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` stored procedure to introduce an intentional error (e.g., `SIGNAL SQLSTATE`). This modification will be reverted in the teardown.

**Action:**
1.  Execute the `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper` stored procedure.
2.  Query `job_control`, `error_log`, and `job_message_log`.

**Pass/Fail Criterion:**
*   `isbert_ds.job_control`:
    *   The current job entry must have `status = 'FAILED'`.
    *   The `message` column should contain the error details.
*   `isbert_ds.error_log`:
    *   Must contain at least one entry related to the failed job, with the error message and stack trace.
*   `isbert_ds.job_message_log`:
    *   Must contain 'ERROR' level messages indicating the failure from both the controller and wrapper.

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def setup_error_handling_test(bq_client):
    """Sets up the BigQuery environment for error handling tests, including temporary SP modification."""
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_control`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.error_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_message_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()

    # Insert minimal data to allow the job to start
    bq_client.query("""
        INSERT INTO `isbert_ds.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-01 00:00:00'));
    """).result()
    bq_client.query("""
        INSERT INTO `isbert_ds.sof_ta_discount` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt, rabatthoehe) VALUES
        (1, 1, 'TYPE_X', 'Test Disc', 1);
    """).result()

    # Temporarily modify d_ausd_v_ta_disc_zusgf_sql_logic to force an error
    bq_client.query("""
        CREATE OR REPLACE PROCEDURE `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`()
        BEGIN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error during core SQL logic execution.';
        END;
    """).result()

    yield

    # Revert d_ausd_v_ta_disc_zusgf_sql_logic to its original, correct version
    # This is crucial for subsequent tests.
    bq_client.query("""
        CREATE OR REPLACE PROCEDURE `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`()
        BEGIN
            DECLARE v_datum STRING;
            SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101') INTO v_datum FROM `isbert_ds.dwtk_meldungen` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';
            TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`;
            INSERT INTO `isbert_ds.sof_ta_disc_zusgf` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle)
            SELECT dzg.cntrct_id, dzg.cntrct_obj_version, dzg.disc_vector_ty, LEFT(con.rabatt_alle, 500)
            FROM (SELECT DISTINCT cntrct_id, disc_vector_ty, cntrct_obj_version FROM `isbert_ds.sof_ta_discount`) AS dzg
            LEFT JOIN (SELECT cntrct_id, cntrct_obj_version, STRING_AGG(CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)'), ', ' ORDER BY CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)')) AS rabatt_alle FROM `isbert_ds.sof_ta_discount` GROUP BY cntrct_id, cntrct_obj_version) AS con
            ON dzg.cntrct_id = con.cntrct_id AND dzg.cntrct_obj_version = con.cntrct_obj_version;
        END;
    """).result()


def test_error_handling(bq_client, setup_error_handling_test):
    """Verifies that the job correctly handles and logs errors."""
    # Execute the wrapper stored procedure, expecting it to fail
    with pytest.raises(Exception) as excinfo: # BigQuery client will raise an exception for failed SP
        bq_client.query("CALL `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`()").result()
    assert "Simulated error during core SQL logic execution." in str(excinfo.value)

    # 1. Verify job_control table
    job_control_df = bq_client.query("""
        SELECT job_kennung, status, message
        FROM `isbert_ds.job_control`
        WHERE job_kennung = 'R_AUSD_V_TA_DISC_ZUSGF'
        ORDER BY start_time DESC LIMIT 1
    """).to_dataframe()

    assert not job_control_df.empty, "No job control entry found for the failed job."
    assert job_control_df.iloc[0]['status'] == 'FAILED', f"Job status is not FAILED: {job_control_df.iloc[0]['status']}"
    assert 'Simulated error' in job_control_df.iloc[0]['message'], "Error message not found in job_control."

    # 2. Verify error_log
    error_log_df = bq_client.query("""
        SELECT error_message, stack_trace
        FROM `isbert_ds.error_log`
        WHERE job_kennung = 'R_AUSD_V_TA_DISC_ZUSGF'
        ORDER BY log_time DESC LIMIT 1
    """).to_dataframe()

    assert not error_log_df.empty, "No error entry found in error_log."
    assert 'Simulated error' in error_log_df.iloc[0]['error_message'], "Error message not found in error_log."
    assert error_log_df.iloc[0]['stack_trace'] is not None, "Stack trace is missing from error_log."

    # 3. Verify job_message_log
    message_log_df = bq_client.query("""
        SELECT message, log_level
        FROM `isbert_ds.job_message_log`
        WHERE (job_kennung = 'R_AUSD_V_TA_DISC_ZUSGF' OR job_kennung = 'BERT_V_TA_DISC_ZUSGF')
          AND log_level = 'ERROR'
        ORDER BY log_time DESC LIMIT 2
    """).to_dataframe()

    assert len(message_log_df) >= 2, "Expected at least two ERROR messages (controller and wrapper)."
    assert any('failed: Simulated error' in msg for msg in message_log_df['message']), "Error message not found in message_log."
    assert all(level == 'ERROR' for level in message_log_df['log_level']), "Unexpected log level in message log."

    print("Error handling and logging tests passed.")
```

---

## Test Case 7: Data Quality - NULL Handling in Source Data

**Purpose:** Verify that the job correctly handles `NULL` values in `rabatt` and `rabatthoehe` columns during the `STRING_AGG` concatenation, ensuring the output is consistent with BigQuery's `CONCAT` behavior (which returns `NULL` if any argument is `NULL`).

**Setup:**
1.  Clear `isbert_ds.sof_ta_discount` and `isbert_ds.sof_ta_disc_zusgf`.
2.  Insert test data into `isbert_ds.sof_ta_discount` with:
    *   `rabatt` is `NULL`, `rabatthoehe` is not `NULL`.
    *   `rabatthoehe` is `NULL`, `rabatt` is not `NULL`.
    *   Both `rabatt` and `rabatthoehe` are `NULL`.
    *   All values are not `NULL` (baseline).

**Action:**
1.  Execute the `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` stored procedure.
2.  Query `isbert_ds.sof_ta_disc_zusgf` to inspect the `rabatt_alle` column for these specific `cntrct_id`s.

**Pass/Fail Criterion:**
*   If `rabatt` is `NULL` or `rabatthoehe` is `NULL` for a given discount entry, the `CONCAT` expression `CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)')` should evaluate to `NULL`.
*   `STRING_AGG` should then ignore these `NULL` values. If all discounts for a contract result in `NULL`, `rabatt_alle` should be `NULL`.

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.fixture
def setup_null_handling_test(bq_client):
    """Sets up the BigQuery environment for NULL handling tests."""
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()

    bq_client.query("""
        INSERT INTO `isbert_ds.sof_ta_discount` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt, rabatthoehe) VALUES
        (200, 1, 'TYPE_N1', NULL, 10),       -- rabatt is NULL
        (201, 1, 'TYPE_N2', 'Discount X', NULL), -- rabatthoehe is NULL
        (202, 1, 'TYPE_N3', NULL, NULL),     -- both are NULL
        (203, 1, 'TYPE_N4', 'Valid Disc', 20), -- baseline valid
        (204, 1, 'TYPE_N5', 'Partially Null', 5),
        (204, 1, 'TYPE_N5', NULL, 15);       -- one valid, one null-producing
    """).result()

    yield

def test_null_handling_in_string_agg(bq_client, setup_null_handling_test):
    """Verifies correct NULL handling in STRING_AGG concatenation."""
    bq_client.query("CALL `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`()").result()

    query_results = bq_client.query("""
        SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle
        FROM `isbert_ds.sof_ta_disc_zusgf`
        ORDER BY cntrct_id
    """).to_dataframe()

    expected_data = pd.DataFrame([
        {'cntrct_id': 200, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_N1', 'rabatt_alle': None}, # CONCAT(NULL, ...) -> NULL, STRING_AGG ignores
        {'cntrct_id': 201, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_N2', 'rabatt_alle': None}, # CONCAT(..., NULL) -> NULL, STRING_AGG ignores
        {'cntrct_id': 202, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_N3', 'rabatt_alle': None}, # CONCAT(NULL, NULL) -> NULL, STRING_AGG ignores
        {'cntrct_id': 203, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_N4', 'rabatt_alle': 'Valid Disc (20%)'},
        {'cntrct_id': 204, 'cntrct_obj_version': 1, 'disc_vector_ty': 'TYPE_N5', 'rabatt_alle': 'Partially Null (5%)'} # Only the valid one is aggregated
    ])

    # Ensure column types match for comparison
    expected_data['cntrct_id'] = expected_data['cntrct_id'].astype('Int64')
    expected_data['cntrct_obj_version'] = expected_data['cntrct_obj_version'].astype('Int64')
    expected_data['disc_vector_ty'] = expected_data['disc_vector_ty'].astype(str)
    expected_data['rabatt_alle'] = expected_data['rabatt_alle'].astype(str).replace('None', None)

    migrated_data_df = query_results
    migrated_data_df['cntrct_id'] = migrated_data_df['cntrct_id'].astype('Int64')
    migrated_data_df['cntrct_obj_version'] = migrated_data_df['cntrct_obj_version'].astype('Int64')
    migrated_data_df['disc_vector_ty'] = migrated_data_df['disc_vector_ty'].astype(str)
    migrated_data_df['rabatt_alle'] = migrated_data_df['rabatt_alle'].astype(str).replace('None', None)

    assert_frame_equal(expected_data.sort_values(by=['cntrct_id']).reset_index(drop=True),
                       migrated_data_df.sort_values(by=['cntrct_id']).reset_index(drop=True),
                       check_dtype=True,
                       obj_as_bytes=False)

    print("NULL handling in STRING_AGG tests passed.")
```

---

## Test Case 8: Data Quality - Row Count Assertion

**Purpose:** Verify that the number of rows inserted into `isbert_ds.sof_ta_disc_zusgf` matches the expected count based on the distinct `(cntrct_id, cntrct_obj_version, disc_vector_ty)` combinations in the source `isbert_ds.sof_ta_discount` table.

**Setup:**
1.  Clear `isbert_ds.sof_ta_discount` and `isbert_ds.sof_ta_disc_zusgf`.
2.  Insert test data into `isbert_ds.sof_ta_discount` with various `cntrct_id`, `cntrct_obj_version`, and `disc_vector_ty` combinations, including duplicates that should be collapsed by `DISTINCT`.

**Action:**
1.  Execute the `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` stored procedure.
2.  Query the row count of `isbert_ds.sof_ta_disc_zusgf`.
3.  Calculate the expected row count by performing the `SELECT DISTINCT cntrct_id, disc_vector_ty, cntrct_obj_version FROM isbert_ds.sof_ta_discount` query.

**Pass/Fail Criterion:**
*   The actual row count in `isbert_ds.sof_ta_disc_zusgf` must be equal to the expected distinct count from the source.

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def setup_row_count_test(bq_client):
    """Sets up the BigQuery environment for row count assertion tests."""
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()

    bq_client.query("""
        INSERT INTO `isbert_ds.sof_ta_discount` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt, rabatthoehe) VALUES
        (300, 1, 'TYPE_A', 'D1', 10),
        (300, 1, 'TYPE_A', 'D2', 5),  -- Same distinct group as above
        (301, 1, 'TYPE_B', 'D3', 20),
        (301, 2, 'TYPE_B', 'D4', 25), -- Different version
        (302, 1, 'TYPE_C', 'D5', 30),
        (302, 1, 'TYPE_D', 'D6', 35), -- Different disc_vector_ty
        (303, 1, 'TYPE_E', 'D7', 40),
        (303, 1, 'TYPE_E', 'D8', 45),
        (303, 1, 'TYPE_E', 'D9', 50);
    """).result()

    yield

def test_row_count_assertion(bq_client, setup_row_count_test):
    """Verifies that the row count in the target table matches the expected distinct count from source."""
    # Calculate expected row count from source logic
    expected_count_query = """
        SELECT COUNT(DISTINCT CONCAT(CAST(cntrct_id AS STRING), '-', CAST(cntrct_obj_version AS STRING), '-', disc_vector_ty))
        FROM `isbert_ds.sof_ta_discount`;
    """
    expected_row_count = bq_client.query(expected_count_query).to_dataframe().iloc[0, 0]

    # Execute the core SQL logic
    bq_client.query("CALL `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`()").result()

    # Get actual row count from target table
    actual_row_count_query = "SELECT COUNT(*) FROM `isbert_ds.sof_ta_disc_zusgf`;"
    actual_row_count = bq_client.query(actual_row_count_query).to_dataframe().iloc[0, 0]

    assert actual_row_count == expected_row_count, \
        f"Row count mismatch: Expected {expected_row_count}, got {actual_row_count}"

    print("Row count assertion passed.")
```

---

## Test Case 9: Edge Case - Empty Source Tables

**Purpose:** Verify that the job handles empty source tables gracefully, resulting in an empty target table and correct logging.

**Setup:**
1.  Clear `isbert_ds.dwtk_meldungen`, `isbert_ds.sof_ta_discount`, and `isbert_ds.sof_ta_disc_zusgf`.
2.  Clear all logging tables.

**Action:**
1.  Execute the `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper` stored procedure.
2.  Query `isbert_ds.sof_ta_disc_zusgf` for its row count.
3.  Query logging tables for status and messages.

**Pass/Fail Criterion:**
*   `isbert_ds.sof_ta_disc_zusgf` must have 0 rows.
*   `isbert_ds.job_control` must show `status = 'SUCCESS'`.
*   `isbert_ds.job_result_log` must show `RECORD_COUNT = 0`.
*   `isbert_ds.error_log` must be empty.

**Runnable Test Code (pytest / SQL assertions):**

```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def setup_empty_source_test(bq_client):
    """Sets up the BigQuery environment with empty source tables."""
    bq_client.query("TRUNCATE TABLE `isbert_ds.dwtk_meldungen`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_discount`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_control`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.error_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_message_log`").result()
    bq_client.query("TRUNCATE TABLE `isbert_ds.job_result_log`").result()
    yield

def test_empty_source_tables(bq_client, setup_empty_source_test):
    """Verifies job behavior when source tables are empty."""
    # Execute the wrapper stored procedure
    bq_client.query("CALL `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`()").result()

    # Verify target table is empty
    target_row_count = bq_client.query("SELECT COUNT(*) FROM `isbert_ds.sof_ta_disc_zusgf`").to_dataframe().iloc[0, 0]
    assert target_row_count == 0, f"Target table not empty: {target_row_count} rows found."

    # Verify job status
    job_control_query = """
        SELECT status, message FROM `isbert_ds.job_control`
        WHERE job_kennung = 'R_AUSD_V_TA_DISC_ZUSGF'
        ORDER BY start_time DESC LIMIT 1
    """
    job_control_result = bq_client.query(job_control_query).to_dataframe()
    assert not job_control_result.empty, "No job control entry found."
    assert job_control_result.iloc[0]['status'] == 'SUCCESS', f"Job status was not SUCCESS: {job_control_result.iloc[0]['status']}"

    # Verify logged record count
    record_count_query = """
        SELECT metric_value FROM `isbert_ds.job_result_log`
        WHERE job_kennung = 'BERT_V_TA_DISC_ZUSGF' AND metric_name = 'RECORD_COUNT'
        ORDER BY log_time DESC LIMIT 1
    """
    logged_record_count_df = bq_client.query(record_count_query).to_dataframe()
    assert not logged_record_count_df.empty, "No record count logged."
    logged_record_count = logged_record_count_df.iloc[0, 0]
    assert logged_record_count == 0, "Logged record count mismatch for empty source."

    # Verify no errors
    error_count = bq_client.query("SELECT COUNT(*) FROM `isbert_ds.error_log`").to_dataframe().iloc[0, 0]
    assert error_count == 0, f"Errors found in error_log: {error_count}"

    print("Empty source tables test passed.")
```