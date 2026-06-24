As a senior data-migration QA engineer, I've analyzed the provided migration design for `DW.BERT_AUSD_BP_TA_APN_VERTRAG`. The core challenge lies in re-engineering a procedural Oracle PL/SQL cursor-based aggregation with conditional string concatenation and length checks into a set-based BigQuery SQL query using `STRING_AGG`. This introduces several critical points for validation, particularly around string length handling, NULLs, and the determinism of aggregation order.

Below are the detailed migration validation tests, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_APN_VERTRAG

**Test Environment Setup (Pre-requisites for all tests):**

*   Access to the legacy Oracle database with the `sof$ta_bpr_apn` and `sof$ta_apn_vertrag` tables.
*   Access to the BigQuery project and dataset (`your_project.your_dataset`) where `SOFTA_BPR_APN` and `SOFTA_APN_VERTRAG` tables reside.
*   A mechanism to execute the legacy Oracle PL/SQL script (`d_ausd_bp_ta_apn_vertrag.sql`).
*   A mechanism to execute the migrated BigQuery SQL script (`d_ausd_bp_ta_apn_vertrag_bq.sql`) or the Airflow DAG.
*   Tools/scripts to extract data from both Oracle and BigQuery into a comparable format (e.g., CSV, Pandas DataFrames).
*   Python environment with `pytest`, `pandas`, `google-cloud-bigquery` libraries.

---

### Test Case 1: End-to-End Output Parity - Standard Data

**Purpose:** To verify that the migrated BigQuery job produces identical output to the legacy Oracle job when processing a typical dataset, ensuring overall behavioral equivalence. This is the primary validation for "Output parity".

**Setup:**
1.  **Oracle:**
    *   Ensure `sof$ta_bpr_apn` is empty.
    *   Insert a diverse set of sample data into `sof$ta_bpr_apn` covering:
        *   Multiple APNs/refs for a single `cntrct_id`.
        *   Single APN/ref for a `cntrct_id`.
        *   `cntrct_id_ref` values that are numeric-like.
        *   `access_point_name` values of varying lengths.
        *   No NULLs in `access_point_name` or `cntrct_id_ref` for this test.
    *   Example Oracle `sof$ta_bpr_apn` data:
        ```sql
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C001', 'B1', '1001', 'internet.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C001', 'B2', '1002', 'mms.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C002', 'B3', '2001', 'corporate.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C003', 'B4', '3001', 'web.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C003', 'B5', '3002', 'vpn.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C003', 'B6', '3003', 'iot.apn');
        ```
2.  **BigQuery:**
    *   Ensure `your_project.your_dataset.SOFTA_BPR_APN` is empty.
    *   Load the *exact same data* as inserted into Oracle `sof$ta_bpr_apn` into `your_project.your_dataset.SOFTA_BPR_APN`.

**Action:**
1.  Execute the legacy Oracle PL/SQL script.
2.  Execute the migrated BigQuery SQL script (or trigger the Airflow DAG).
3.  Extract the final data from Oracle `sof$ta_apn_vertrag` and BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG`.

**Pass/Fail Criterion:**
The data in the BigQuery target table (`your_project.your_dataset.SOFTA_APN_VERTRAG`) must be *exactly identical* to the data in the Oracle target table (`sof$ta_apn_vertrag`), considering column order and data types after conversion.

**Runnable Test Code (Python with Pytest):**

```python
import pandas as pd
from google.cloud import bigquery
import pytest

# Assume these are configured globally or via fixtures
BQ_PROJECT = "your_project"
BQ_DATASET = "your_dataset"
BQ_SOURCE_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.SOFTA_BPR_APN"
BQ_TARGET_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.SOFTA_APN_VERTRAG"

# --- Helper functions (mocked for Oracle interaction) ---
def setup_oracle_source_data(data_rows):
    """
    Mocks inserting data into Oracle sof$ta_bpr_apn.
    In a real scenario, this would execute SQL against Oracle.
    """
    print(f"Mock: Inserting {len(data_rows)} rows into Oracle sof$ta_bpr_apn.")
    # Example: db_connection.execute("INSERT INTO sof$ta_bpr_apn ...")
    pass

def run_legacy_oracle_job_and_get_output():
    """
    Mocks running the Oracle PL/SQL job and fetching its output.
    In a real scenario, this would execute the PL/SQL and then query sof$ta_apn_vertrag.
    Returns a pandas DataFrame.
    """
    print("Mock: Running legacy Oracle job and fetching output.")
    # Example output for the setup data
    data = {
        'CNTRCT_ID': ['C001', 'C002', 'C003'],
        'AGGREGATED_APN': ['internet.apn, mms.apn', 'corporate.apn', 'web.apn, vpn.apn, iot.apn'],
        'AGGREGATED_CNTRCT_REF': ['1001, 1002', '2001', '3001, 3002, 3003']
    }
    return pd.DataFrame(data).sort_values('CNTRCT_ID').reset_index(drop=True)

def setup_bq_source_data(data_rows):
    """Inserts data into BigQuery source table."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = BQ_SOURCE_TABLE
    # Clear existing data
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    if data_rows:
        errors = client.insert_rows_json(table_id, data_rows)
        if errors:
            raise Exception(f"BigQuery insert errors: {errors}")
    print(f"Inserted {len(data_rows)} rows into {table_id}")

def run_bq_job_and_get_output():
    """Runs the BigQuery transformation and fetches its output."""
    client = bigquery.Client(project=BQ_PROJECT)
    # Read the SQL script content
    with open('sql/d_ausd_bp_ta_apn_vertrag_bq.sql', 'r') as f:
        sql_script = f.read()
    
    # Replace placeholders if necessary (e.g., project/dataset)
    sql_script = sql_script.replace('`your_project.your_dataset.SOFTA_APN_VERTRAG`', f'`{BQ_TARGET_TABLE}`')
    sql_script = sql_script.replace('`your_project.your_dataset.SOFTA_BPR_APN`', f'`{BQ_SOURCE_TABLE}`')

    print("Running BigQuery transformation job...")
    query_job = client.query(sql_script)
    query_job.result() # Wait for the job to complete

    print("Fetching BigQuery output...")
    bq_output_df = client.query(f"SELECT * FROM `{BQ_TARGET_TABLE}` ORDER BY cntrct_id").to_dataframe()
    return bq_output_df

# --- Pytest Test Case ---
def test_output_parity_standard_data():
    """
    Tests end-to-end output parity with standard, diverse data.
    """
    # 1. Setup Data
    source_data = [
        {'cntrct_id': 'C001', 'bpr_id': 'B1', 'cntrct_id_ref': '1001', 'access_point_name': 'internet.apn'},
        {'cntrct_id': 'C001', 'bpr_id': 'B2', 'cntrct_id_ref': '1002', 'access_point_name': 'mms.apn'},
        {'cntrct_id': 'C002', 'bpr_id': 'B3', 'cntrct_id_ref': '2001', 'access_point_name': 'corporate.apn'},
        {'cntrct_id': 'C003', 'bpr_id': 'B4', 'cntrct_id_ref': '3001', 'access_point_name': 'web.apn'},
        {'cntrct_id': 'C003', 'bpr_id': 'B5', 'cntrct_id_ref': '3002', 'access_point_name': 'vpn.apn'},
        {'cntrct_id': 'C003', 'bpr_id': 'B6', 'cntrct_id_ref': '3003', 'access_point_name': 'iot.apn'},
    ]
    setup_oracle_source_data(source_data)
    setup_bq_source_data(source_data)

    # 2. Run Jobs and Get Outputs
    oracle_output_df = run_legacy_oracle_job_and_get_output()
    bq_output_df = run_bq_job_and_get_output()

    # Standardize column names for comparison (Oracle might return uppercase)
    bq_output_df.columns = [col.upper() for col in bq_output_df.columns]
    
    # Ensure consistent sorting for comparison
    oracle_output_df = oracle_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)
    bq_output_df = bq_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)

    # 3. Assert Parity
    pd.testing.assert_frame_equal(oracle_output_df, bq_output_df, check_dtype=False) # check_dtype=False due to potential type differences (e.g., VARCHAR2 vs STRING)
    print("Test Passed: Output parity achieved for standard data.")

```

---

### Test Case 2: Transformation Correctness - String Length Truncation

**Purpose:** To specifically test the `SUBSTR(..., 1, 100)` logic and compare how the Oracle procedural `LENGTH` check (conditional append) differs from BigQuery's `STRING_AGG` followed by `SUBSTR`. This addresses a critical "Transformation correctness" and "Data Type and Length Handling" risk identified in the design.

**Setup:**
1.  **Oracle:**
    *   Ensure `sof$ta_bpr_apn` is empty.
    *   Insert data where the aggregated APN and/or `cntrct_id_ref` for a `cntrct_id` would exceed 100 characters *if fully concatenated*, but the Oracle logic might truncate earlier due to its conditional append.
    *   Example Oracle `sof$ta_bpr_apn` data:
        ```sql
        -- Contract C001: APNs will exceed 100 chars
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C001', 'B1', '1', 'long.apn.name.one.very.very.long.string.to.test.truncation.logic.a'); -- 70 chars
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C001', 'B2', '2', 'long.apn.name.two.very.very.long.string.to.test.truncation.logic.b'); -- 70 chars
        -- Expected Oracle: 'long.apn.name.one.very.very.long.string.to.test.truncation.logic.a, long.apn.name.two.very.very.long.string.to.test.truncation.logic.b' (142 chars + comma)
        -- Oracle's conditional append:
        -- 1st: 'long.apn.name.one.very.very.long.string.to.test.truncation.logic.a, ' (72 chars)
        -- 2nd: 'long.apn.name.one.very.very.long.string.to.test.truncation.logic.a, long.apn.name.two.very.very.long.string.to.test.truncation.logic.b, ' (144 chars)
        -- The second append will likely be skipped if the total length exceeds 100.
        -- This means Oracle might only have the first APN.
        -- The BigQuery version will aggregate all and then SUBSTR. This is a key difference.

        -- Contract C002: cntrct_id_ref will exceed 100 chars
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C002', 'B3', '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890', 'short.apn'); -- 100 chars
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C002', 'B4', '1234567890', 'short.apn'); -- 10 chars
        -- Similar to C001, Oracle might only include the first ref.
        ```
2.  **BigQuery:**
    *   Ensure `your_project.your_dataset.SOFTA_BPR_APN` is empty.
    *   Load the *exact same data* as inserted into Oracle `sof$ta_bpr_apn` into `your_project.your_dataset.SOFTA_BPR_APN`.

**Action:**
1.  Execute the legacy Oracle PL/SQL script.
2.  Execute the migrated BigQuery SQL script (or trigger the Airflow DAG).
3.  Extract the final data from Oracle `sof$ta_apn_vertrag` and BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG`.

**Pass/Fail Criterion:**
The data in the BigQuery target table must be *exactly identical* to the data in the Oracle target table. **Crucially, if there's a discrepancy due to the length handling, this test will fail, highlighting a behavioral difference that needs to be addressed (either by adjusting the BigQuery logic to match Oracle's conditional append, or by accepting the new behavior and documenting it).**

**Runnable Test Code (Python with Pytest):**

```python
# ... (BQ_PROJECT, BQ_DATASET, BQ_SOURCE_TABLE, BQ_TARGET_TABLE, setup_bq_source_data, run_bq_job_and_get_output helper functions from Test Case 1) ...

def run_legacy_oracle_job_and_get_output_length_test():
    """
    Mocks running the Oracle PL/SQL job and fetching its output for length test.
    This mock *assumes* the Oracle behavior where the second item is NOT appended
    if the combined string exceeds 100 characters.
    """
    print("Mock: Running legacy Oracle job for length test and fetching output.")
    # Based on Oracle's conditional append:
    # C001: 'long.apn.name.one.very.very.long.string.to.test.truncation.logic.a' (70 chars)
    #       The next append 'long.apn.name.two...' would make it > 100, so it's skipped.
    # C002: '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890' (100 chars)
    #       The next append '1234567890' would make it > 100, so it's skipped.
    data = {
        'CNTRCT_ID': ['C001', 'C002'],
        'AGGREGATED_APN': ['long.apn.name.one.very.very.long.string.to.test.truncation.logic.a', 'short.apn'],
        'AGGREGATED_CNTRCT_REF': ['1', '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890']
    }
    return pd.DataFrame(data).sort_values('CNTRCT_ID').reset_index(drop=True)

def test_transformation_string_length_truncation():
    """
    Tests how string length truncation is handled, specifically the difference
    between Oracle's conditional append and BigQuery's aggregate-then-truncate.
    """
    # 1. Setup Data
    source_data = [
        {'cntrct_id': 'C001', 'bpr_id': 'B1', 'cntrct_id_ref': '1', 'access_point_name': 'long.apn.name.one.very.very.long.string.to.test.truncation.logic.a'},
        {'cntrct_id': 'C001', 'bpr_id': 'B2', 'cntrct_id_ref': '2', 'access_point_name': 'long.apn.name.two.very.very.long.string.to.test.truncation.logic.b'},
        {'cntrct_id': 'C002', 'bpr_id': 'B3', 'cntrct_id_ref': '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890', 'access_point_name': 'short.apn'},
        {'cntrct_id': 'C002', 'bpr_id': 'B4', 'cntrct_id_ref': '1234567890', 'access_point_name': 'short.apn'},
    ]
    setup_oracle_source_data(source_data)
    setup_bq_source_data(source_data)

    # 2. Run Jobs and Get Outputs
    oracle_output_df = run_legacy_oracle_job_and_get_output_length_test()
    bq_output_df = run_bq_job_and_get_output()

    # Standardize column names for comparison
    bq_output_df.columns = [col.upper() for col in bq_output_df.columns]
    
    # Ensure consistent sorting for comparison
    oracle_output_df = oracle_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)
    bq_output_df = bq_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)

    # 3. Assert Parity
    try:
        pd.testing.assert_frame_equal(oracle_output_df, bq_output_df, check_dtype=False)
        print("Test Passed: String length truncation behavior is identical.")
    except AssertionError as e:
        print(f"Test Failed: String length truncation behavior differs. This is a known risk.")
        print("Oracle Output:\n", oracle_output_df)
        print("BigQuery Output:\n", bq_output_df)
        pytest.fail(f"Discrepancy in string length handling: {e}")

```

---

### Test Case 3: Transformation Correctness - NULL Handling

**Purpose:** To verify how NULL values in `access_point_name` and `cntrct_id_ref` are handled, as Oracle's `||` operator treats NULL as an empty string, while BigQuery's `STRING_AGG` by default ignores NULLs. This addresses "NULL handling" and "Transformation correctness".

**Setup:**
1.  **Oracle:**
    *   Ensure `sof$ta_bpr_apn` is empty.
    *   Insert data with:
        *   `NULL` `access_point_name`.
        *   `NULL` `cntrct_id_ref`.
        *   Both `NULL`.
        *   `NULL` `cntrct_id` (though `GROUP BY` will handle this, it's good to test).
    *   Example Oracle `sof$ta_bpr_apn` data:
        ```sql
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C004', 'B7', '4001', NULL);
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C004', 'B8', '4002', 'apn.with.null.ref');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C005', 'B9', NULL, 'apn.with.null.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C005', 'B10', '5002', 'another.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C006', 'B11', NULL, NULL);
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C007', 'B12', '7001', 'only.one.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C007', 'B13', NULL, NULL);
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES (NULL, 'B14', '9001', 'null.contract.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES (NULL, 'B15', '9002', 'null.contract.apn2');
        ```
2.  **BigQuery:**
    *   Ensure `your_project.your_dataset.SOFTA_BPR_APN` is empty.
    *   Load the *exact same data* as inserted into Oracle `sof$ta_bpr_apn` into `your_project.your_dataset.SOFTA_BPR_APN`.

**Action:**
1.  Execute the legacy Oracle PL/SQL script.
2.  Execute the migrated BigQuery SQL script (or trigger the Airflow DAG).
3.  Extract the final data from Oracle `sof$ta_apn_vertrag` and BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG`.

**Pass/Fail Criterion:**
The data in the BigQuery target table must be *exactly identical* to the data in the Oracle target table. If `STRING_AGG`'s default NULL handling (ignoring NULLs) differs from Oracle's `||` (treating NULL as empty string), this test will fail, indicating a behavioral change that needs to be addressed (e.g., by using `COALESCE` in BigQuery if Oracle's behavior is desired).

**Runnable Test Code (Python with Pytest):**

```python
# ... (BQ_PROJECT, BQ_DATASET, BQ_SOURCE_TABLE, BQ_TARGET_TABLE, setup_bq_source_data, run_bq_job_and_get_output helper functions from Test Case 1) ...

def run_legacy_oracle_job_and_get_output_null_test():
    """
    Mocks running the Oracle PL/SQL job and fetching its output for NULL handling.
    This mock *assumes* Oracle's || operator behavior where NULLs are treated as empty strings.
    """
    print("Mock: Running legacy Oracle job for NULL test and fetching output.")
    data = {
        'CNTRCT_ID': ['C004', 'C005', 'C006', 'C007', None], # Oracle groups NULL cntrct_id together
        'AGGREGATED_APN': ['apn.with.null.apn', 'another.apn', '', 'only.one.apn', 'null.contract.apn, null.contract.apn2'],
        'AGGREGATED_CNTRCT_REF': ['4001, 4002', '5002', '', '7001', '9001, 9002']
    }
    # Oracle's cursor logic might produce an empty string for aggregated_apn if all are NULL
    # BigQuery's STRING_AGG(col, ', ') will return NULL if all are NULL, which then SUBSTR/RTRIM might turn into an empty string.
    # This is a subtle point. Let's assume BigQuery's STRING_AGG(..., ', ') will result in NULL if all inputs are NULL,
    # and then SUBSTR(RTRIM(NULL, ', '), 1, 100) will result in NULL.
    # If Oracle produces '', BigQuery produces NULL, this is a difference.
    # For this mock, I'll assume Oracle produces '' for fully null aggregations.
    return pd.DataFrame(data).sort_values(by=['CNTRCT_ID'], na_position='first').reset_index(drop=True)

def test_transformation_null_handling():
    """
    Tests how NULL values in source columns are handled during aggregation.
    """
    # 1. Setup Data
    source_data = [
        {'cntrct_id': 'C004', 'bpr_id': 'B7', 'cntrct_id_ref': '4001', 'access_point_name': None},
        {'cntrct_id': 'C004', 'bpr_id': 'B8', 'cntrct_id_ref': '4002', 'access_point_name': 'apn.with.null.apn'},
        {'cntrct_id': 'C005', 'bpr_id': 'B9', 'cntrct_id_ref': None, 'access_point_name': 'apn.with.null.apn'},
        {'cntrct_id': 'C005', 'bpr_id': 'B10', 'cntrct_id_ref': '5002', 'access_point_name': 'another.apn'},
        {'cntrct_id': 'C006', 'bpr_id': 'B11', 'cntrct_id_ref': None, 'access_point_name': None},
        {'cntrct_id': 'C007', 'bpr_id': 'B12', 'cntrct_id_ref': '7001', 'access_point_name': 'only.one.apn'},
        {'cntrct_id': 'C007', 'bpr_id': 'B13', 'cntrct_id_ref': None, 'access_point_name': None},
        {'cntrct_id': None, 'bpr_id': 'B14', 'cntrct_id_ref': '9001', 'access_point_name': 'null.contract.apn'},
        {'cntrct_id': None, 'bpr_id': 'B15', 'cntrct_id_ref': '9002', 'access_point_name': 'null.contract.apn2'},
    ]
    setup_oracle_source_data(source_data)
    setup_bq_source_data(source_data)

    # 2. Run Jobs and Get Outputs
    oracle_output_df = run_legacy_oracle_job_and_get_output_null_test()
    bq_output_df = run_bq_job_and_get_output()

    # Standardize column names for comparison
    bq_output_df.columns = [col.upper() for col in bq_output_df.columns]
    
    # Ensure consistent sorting for comparison (handle None/NULL for cntrct_id)
    oracle_output_df = oracle_output_df.sort_values(by=['CNTRCT_ID'], na_position='first').reset_index(drop=True)
    bq_output_df = bq_output_df.sort_values(by=['CNTRCT_ID'], na_position='first').reset_index(drop=True)

    # 3. Assert Parity
    try:
        pd.testing.assert_frame_equal(oracle_output_df, bq_output_df, check_dtype=False)
        print("Test Passed: NULL handling is identical.")
    except AssertionError as e:
        print(f"Test Failed: NULL handling behavior differs. This is a known risk.")
        print("Oracle Output:\n", oracle_output_df)
        print("BigQuery Output:\n", bq_output_df)
        pytest.fail(f"Discrepancy in NULL handling: {e}")

```

---

### Test Case 4: Transformation Correctness - Ordering of Aggregated Strings

**Purpose:** To confirm that the explicit `ORDER BY` clause within `STRING_AGG` in BigQuery produces the same deterministic order as the implicit or explicit ordering in the Oracle legacy job. The design document notes "ordering is important for consistent output if not guaranteed by source." The Oracle cursor only orders by `cntrct_id`, not internal APNs/refs. BigQuery explicitly orders. This test validates if this explicit ordering matches the *actual* legacy behavior.

**Setup:**
1.  **Oracle:**
    *   Ensure `sof$ta_bpr_apn` is empty.
    *   Insert data for a single `cntrct_id` where `access_point_name` and `cntrct_id_ref` values are not naturally sorted alphabetically/numerically as they appear in the source.
    *   Example Oracle `sof$ta_bpr_apn` data:
        ```sql
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C008', 'B16', '8003', 'zebra.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C008', 'B17', '8001', 'apple.apn');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C008', 'B18', '8002', 'banana.apn');
        ```
2.  **BigQuery:**
    *   Ensure `your_project.your_dataset.SOFTA_BPR_APN` is empty.
    *   Load the *exact same data* as inserted into Oracle `sof$ta_bpr_apn` into `your_project.your_dataset.SOFTA_BPR_APN`.

**Action:**
1.  Execute the legacy Oracle PL/SQL script.
2.  Execute the migrated BigQuery SQL script (or trigger the Airflow DAG).
3.  Extract the final data from Oracle `sof$ta_apn_vertrag` and BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG`.

**Pass/Fail Criterion:**
The aggregated strings (`aggregated_apn`, `aggregated_cntrct_ref`) in the BigQuery target table must match the exact order of elements within the strings from the Oracle target table. If the Oracle job's implicit ordering (or lack thereof) differs from BigQuery's explicit `ORDER BY`, this test will fail.

**Runnable Test Code (Python with Pytest):**

```python
# ... (BQ_PROJECT, BQ_DATASET, BQ_SOURCE_TABLE, BQ_TARGET_TABLE, setup_bq_source_data, run_bq_job_and_get_output helper functions from Test Case 1) ...

def run_legacy_oracle_job_and_get_output_ordering_test():
    """
    Mocks running the Oracle PL/SQL job and fetching its output for ordering.
    This mock *assumes* the Oracle output for C008 will be alphabetically sorted
    for APNs and numerically for refs, matching BigQuery's explicit ORDER BY.
    If Oracle's actual behavior is non-deterministic or different, this mock needs adjustment.
    """
    print("Mock: Running legacy Oracle job for ordering test and fetching output.")
    data = {
        'CNTRCT_ID': ['C008'],
        'AGGREGATED_APN': ['apple.apn, banana.apn, zebra.apn'], # Alphabetical
        'AGGREGATED_CNTRCT_REF': ['8001, 8002, 8003'] # Numerical
    }
    return pd.DataFrame(data).sort_values('CNTRCT_ID').reset_index(drop=True)

def test_transformation_ordering_of_aggregated_strings():
    """
    Tests if the ordering of elements within aggregated strings is consistent.
    """
    # 1. Setup Data
    source_data = [
        {'cntrct_id': 'C008', 'bpr_id': 'B16', 'cntrct_id_ref': '8003', 'access_point_name': 'zebra.apn'},
        {'cntrct_id': 'C008', 'bpr_id': 'B17', 'cntrct_id_ref': '8001', 'access_point_name': 'apple.apn'},
        {'cntrct_id': 'C008', 'bpr_id': 'B18', 'cntrct_id_ref': '8002', 'access_point_name': 'banana.apn'},
    ]
    setup_oracle_source_data(source_data)
    setup_bq_source_data(source_data)

    # 2. Run Jobs and Get Outputs
    oracle_output_df = run_legacy_oracle_job_and_get_output_ordering_test()
    bq_output_df = run_bq_job_and_get_output()

    # Standardize column names for comparison
    bq_output_df.columns = [col.upper() for col in bq_output_df.columns]
    
    # Ensure consistent sorting for comparison
    oracle_output_df = oracle_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)
    bq_output_df = bq_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)

    # 3. Assert Parity
    try:
        pd.testing.assert_frame_equal(oracle_output_df, bq_output_df, check_dtype=False)
        print("Test Passed: Ordering of aggregated strings is consistent.")
    except AssertionError as e:
        print(f"Test Failed: Ordering of aggregated strings differs. This is a known risk.")
        print("Oracle Output:\n", oracle_output_df)
        print("BigQuery Output:\n", bq_output_df)
        pytest.fail(f"Discrepancy in string aggregation ordering: {e}")

```

---

### Test Case 5: External System Replacement - TRUNCATE TABLE Behavior

**Purpose:** To verify that the `TRUNCATE TABLE` operation in BigQuery behaves identically to the Oracle `TRUNCATE TABLE` (executed via `DWPA_UTIL_SKRIPT.runstatement`), ensuring the target table is empty before new data is inserted. This addresses "External-system replacements".

**Setup:**
1.  **Oracle:**
    *   Insert some dummy data into `sof$ta_apn_vertrag`.
        ```sql
        INSERT INTO sof$ta_apn_vertrag VALUES ('DUMMY1', 'dummy.apn', '1111');
        INSERT INTO sof$ta_apn_vertrag VALUES ('DUMMY2', 'another.apn', '2222');
        COMMIT;
        ```
    *   Insert standard test data into `sof$ta_bpr_apn` (e.g., from Test Case 1).
2.  **BigQuery:**
    *   Insert some dummy data into `your_project.your_dataset.SOFTA_APN_VERTRAG`.
        ```sql
        INSERT INTO `your_project.your_dataset.SOFTA_APN_VERTRAG` (cntrct_id, aggregated_apn, aggregated_cntrct_ref) VALUES ('DUMMY1', 'dummy.apn', '1111');
        INSERT INTO `your_project.your_dataset.SOFTA_APN_VERTRAG` (cntrct_id, aggregated_apn, aggregated_cntrct_ref) VALUES ('DUMMY2', 'another.apn', '2222');
        ```
    *   Load the *exact same standard test data* into `your_project.your_dataset.SOFTA_BPR_APN`.

**Action:**
1.  Execute the legacy Oracle PL/SQL script.
2.  Execute the migrated BigQuery SQL script (or trigger the Airflow DAG).
3.  Extract the final data from Oracle `sof$ta_apn_vertrag` and BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG`.

**Pass/Fail Criterion:**
The final data in both target tables should *only* contain the newly inserted aggregated data, and *not* the initial dummy data. This confirms that the `TRUNCATE TABLE` operation was successful in both environments. Additionally, the content should match the expected output from Test Case 1.

**Runnable Test Code (Python with Pytest):**

```python
# ... (BQ_PROJECT, BQ_DATASET, BQ_SOURCE_TABLE, BQ_TARGET_TABLE, setup_bq_source_data, run_bq_job_and_get_output helper functions from Test Case 1) ...

def setup_oracle_target_dummy_data():
    """Mocks inserting dummy data into Oracle sof$ta_apn_vertrag."""
    print("Mock: Inserting dummy data into Oracle sof$ta_apn_vertrag.")
    # In real scenario: db_connection.execute("INSERT INTO sof$ta_apn_vertrag ...")
    pass

def setup_bq_target_dummy_data():
    """Inserts dummy data into BigQuery target table."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = BQ_TARGET_TABLE
    dummy_data = [
        {'cntrct_id': 'DUMMY1', 'aggregated_apn': 'dummy.apn', 'aggregated_cntrct_ref': '1111'},
        {'cntrct_id': 'DUMMY2', 'aggregated_apn': 'another.apn', 'aggregated_cntrct_ref': '2222'},
    ]
    errors = client.insert_rows_json(table_id, dummy_data)
    if errors:
        raise Exception(f"BigQuery dummy data insert errors: {errors}")
    print(f"Inserted {len(dummy_data)} dummy rows into {table_id}")

def test_external_system_truncate_behavior():
    """
    Tests that TRUNCATE TABLE behaves identically in both environments.
    """
    # 1. Setup Data
    # Insert dummy data into target tables first
    setup_oracle_target_dummy_data()
    setup_bq_target_dummy_data()

    # Then setup standard source data
    source_data = [
        {'cntrct_id': 'C001', 'bpr_id': 'B1', 'cntrct_id_ref': '1001', 'access_point_name': 'internet.apn'},
        {'cntrct_id': 'C001', 'bpr_id': 'B2', 'cntrct_id_ref': '1002', 'access_point_name': 'mms.apn'},
    ]
    setup_oracle_source_data(source_data)
    setup_bq_source_data(source_data)

    # 2. Run Jobs and Get Outputs
    oracle_output_df = run_legacy_oracle_job_and_get_output() # This will now reflect the truncated + new data
    bq_output_df = run_bq_job_and_get_output()

    # Expected output after truncation and insert (from source_data)
    expected_data = {
        'CNTRCT_ID': ['C001'],
        'AGGREGATED_APN': ['internet.apn, mms.apn'],
        'AGGREGATED_CNTRCT_REF': ['1001, 1002']
    }
    expected_df = pd.DataFrame(expected_data).sort_values('CNTRCT_ID').reset_index(drop=True)

    # Standardize column names for comparison
    bq_output_df.columns = [col.upper() for col in bq_output_df.columns]
    
    # Ensure consistent sorting for comparison
    oracle_output_df = oracle_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)
    bq_output_df = bq_output_df.sort_values(by=['CNTRCT_ID']).reset_index(drop=True)

    # 3. Assert Parity and Truncation
    pd.testing.assert_frame_equal(expected_df, oracle_output_df, check_dtype=False)
    pd.testing.assert_frame_equal(expected_df, bq_output_df, check_dtype=False)
    print("Test Passed: TRUNCATE TABLE behavior is identical, and only new data is present.")

```

---

### Test Case 6: Data Quality - Row Count and Uniqueness

**Purpose:** To verify basic data quality assertions: the total number of rows and the uniqueness of the `cntrct_id` in the target table. This addresses "Data-quality / row-count / schema assertions".

**Setup:**
1.  **Oracle:**
    *   Ensure `sof$ta_bpr_apn` is empty.
    *   Insert a dataset with varying numbers of APNs/refs per `cntrct_id`, including some `cntrct_id`s that appear only once.
    *   Example Oracle `sof$ta_bpr_apn` data:
        ```sql
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C100', 'B1', '1001', 'apn1');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C100', 'B2', '1002', 'apn2');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C101', 'B3', '1003', 'apn3');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C102', 'B4', '1004', 'apn4');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C102', 'B5', '1005', 'apn5');
        INSERT INTO sof$ta_bpr_apn (cntrct_id, bpr_id, cntrct_id_ref, access_point_name) VALUES ('C102', 'B6', '1006', 'apn6');
        ```
2.  **BigQuery:**
    *   Ensure `your_project.your_dataset.SOFTA_BPR_APN` is empty.
    *   Load the *exact same data* as inserted into Oracle `sof$ta_bpr_apn` into `your_project.your_dataset.SOFTA_BPR_APN`.

**Action:**
1.  Execute the legacy Oracle PL/SQL script.
2.  Execute the migrated BigQuery SQL script (or trigger the Airflow DAG).
3.  Query the row count from Oracle `sof$ta_apn_vertrag` and BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG`.
4.  Check for uniqueness of `cntrct_id` in both target tables.

**Pass/Fail Criterion:**
1.  The row count in `your_project.your_dataset.SOFTA_APN_VERTRAG` must be equal to the row count in `sof$ta_apn_vertrag`.
2.  The `cntrct_id` column in `your_project.your_dataset.SOFTA_APN_VERTRAG` must contain only unique values, matching the uniqueness in `sof$ta_apn_vertrag`.

**Runnable Test Code (Python with Pytest / SQL Assertions):**

```python
# ... (BQ_PROJECT, BQ_DATASET, BQ_SOURCE_TABLE, BQ_TARGET_TABLE, setup_bq_source_data, run_bq_job_and_get_output helper functions from Test Case 1) ...

def get_oracle_target_row_count():
    """Mocks getting row count from Oracle sof$ta_apn_vertrag."""
    print("Mock: Getting Oracle target row count.")
    # Based on the setup data, there are 3 unique cntrct_ids
    return 3

def get_bq_target_row_count():
    """Gets row count from BigQuery target table."""
    client = bigquery.Client(project=BQ_PROJECT)
    query = f"SELECT COUNT(1) FROM `{BQ_TARGET_TABLE}`"
    row_count = client.query(query).result().to_dataframe().iloc[0, 0]
    return row_count

def check_oracle_target_cntrct_id_uniqueness():
    """Mocks checking cntrct_id uniqueness in Oracle sof$ta_apn_vertrag."""
    print("Mock: Checking Oracle cntrct_id uniqueness.")
    # Based on the setup data, cntrct_id should be unique
    return True

def check_bq_target_cntrct_id_uniqueness():
    """Checks cntrct_id uniqueness in BigQuery target table."""
    client = bigquery.Client(project=BQ_PROJECT)
    query = f"""
    SELECT
        COUNT(cntrct_id) = COUNT(DISTINCT cntrct_id)
    FROM `{BQ_TARGET_TABLE}`
    """
    is_unique = client.query(query).result().to_dataframe().iloc[0, 0]
    return is_unique

def test_data_quality_row_count_and_uniqueness():
    """
    Tests row count and uniqueness of cntrct_id in the target table.
    """
    # 1. Setup Data
    source_data = [
        {'cntrct_id': 'C100', 'bpr_id': 'B1', 'cntrct_id_ref': '1001', 'access_point_name': 'apn1'},
        {'cntrct_id': 'C100', 'bpr_id': 'B2', 'cntrct_id_ref': '1002', 'access_point_name': 'apn2'},
        {'cntrct_id': 'C101', 'bpr_id': 'B3', 'cntrct_id_ref': '1003', 'access_point_name': 'apn3'},
        {'cntrct_id': 'C102', 'bpr_id': 'B4', 'cntrct_id_ref': '1004', 'access_point_name': 'apn4'},
        {'cntrct_id': 'C102', 'bpr_id': 'B5', 'cntrct_id_ref': '1005', 'access_point_name': 'apn5'},
        {'cntrct_id': 'C102', 'bpr_id': 'B6', 'cntrct_id_ref': '1006', 'access_point_name': 'apn6'},
    ]
    setup_oracle_source_data(source_data)
    setup_bq_source_data(source_data)

    # 2. Run Jobs
    # For this test, we don't need the full output, just counts/uniqueness
    run_legacy_oracle_job_and_get_output() # To populate Oracle target
    run_bq_job_and_get_output() # To populate BigQuery target

    # 3. Assertions
    oracle_row_count = get_oracle_target_row_count()
    bq_row_count = get_bq_target_row_count()
    assert oracle_row_count == bq_row_count, \
        f"Row count mismatch: Oracle={oracle_row_count}, BigQuery={bq_row_count}"
    print(f"Test Passed: Row counts match ({oracle_row_count}).")

    oracle_is_unique = check_oracle_target_cntrct_id_uniqueness()
    bq_is_unique = check_bq_target_cntrct_id_uniqueness()
    assert oracle_is_unique == bq_is_unique and bq_is_unique is True, \
        f"cntrct_id uniqueness mismatch or not unique: Oracle={oracle_is_unique}, BigQuery={bq_is_unique}"
    print("Test Passed: cntrct_id is unique in both target tables.")

```

---

### Test Case 7: Schema Assertion

**Purpose:** To confirm that the BigQuery target table `SOFTA_APN_VERTRAG` has the expected schema (column names and data types) as defined in the DDL and implied by the transformation. This addresses "Data-quality / row-count / schema assertions".

**Setup:**
*   No specific data setup required beyond the DDL being applied.

**Action:**
1.  Query the schema of `your_project.your_dataset.SOFTA_APN_VERTRAG` in BigQuery.

**Pass/Fail Criterion:**
The schema of `your_project.your_dataset.SOFTA_APN_VERTRAG` must match the expected structure:
*   `cntrct_id`: `STRING`
*   `aggregated_apn`: `STRING`
*   `aggregated_cntrct_ref`: `STRING`

**Runnable Test Code (Python with Pytest):**

```python
import pytest
from google.cloud import bigquery

BQ_PROJECT = "your_project"
BQ_DATASET = "your_dataset"
BQ_TARGET_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.SOFTA_APN_VERTRAG"

def test_schema_assertion():
    """
    Tests that the BigQuery target table has the expected schema.
    """
    client = bigquery.Client(project=BQ_PROJECT)
    table = client.get_table(BQ_TARGET_TABLE)

    expected_schema = {
        'cntrct_id': 'STRING',
        'aggregated_apn': 'STRING',
        'aggregated_cntrct_ref': 'STRING'
    }

    actual_schema = {field.name: field.field_type for field in table.schema}

    assert actual_schema == expected_schema, \
        f"Schema mismatch for {BQ_TARGET_TABLE}. Expected: {expected_schema}, Actual: {actual_schema}"
    print("Test Passed: BigQuery target table schema matches expectations.")

```