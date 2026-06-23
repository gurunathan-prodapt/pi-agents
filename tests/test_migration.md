As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `r_ausd_bp_ta_apn_carmen.ksh` job migration to Google Cloud BigQuery. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

**Global Setup & Assumptions:**

*   **Environment:** Access to both the legacy Oracle environment and the new GCP BigQuery/Airflow environment.
*   **Data Replication:** It is assumed that for each test case, the *exact same* source data is loaded into both the Oracle source tables and their corresponding BigQuery replicated tables. This is critical for direct comparison.
*   **Table Clearing:** Before each test run, the target table (`sof$ta_apn_carmen`) in both Oracle and BigQuery, and potentially source tables, should be cleared to ensure isolation.
*   **BigQuery DDLs:** The provided BigQuery DDLs for `dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`, and `sof$ta_apn_carmen` are assumed to be applied.
*   **Airflow DAG:** The `r_ausd_bp_ta_apn_carmen_dag` is deployed, unpaused, and configured to point to the correct GCP project and dataset.
*   **`v_datum` Derivation:** The `v_datum` in the BigQuery SQL is derived *internally* from `dwtk_meldungen`, consistent with the design document. The `p_stichtag` parameter of the original ksh script is not directly used to determine `v_datum` in the SQL.
*   **Oracle `TRUNCATE`:** The legacy job uses `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for truncation. For testing, we'll assume this is equivalent to a direct `TRUNCATE TABLE` in Oracle for comparison purposes.

---

## Test Case 1: Output Parity - Standard Data Flow

**Purpose:** Verify that with a typical set of valid input data, the migrated job produces an identical output in the target table as the legacy job. This is the primary end-to-end validation.

**Setup:**
1.  Clear `sof$ta_apn_carmen` in both Oracle and BigQuery.
2.  Populate Oracle source tables (`dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`) with a diverse set of valid data, including multiple matching records across joins and various date scenarios (some matching `v_datum`, some not).
    *   Example `dwtk_meldungen`: `('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00')` -> `v_datum = '20230115'`
    *   Example `pds$ta_pdp_context_assoc`: `('C1', 101, '2023-01-01', NULL, '2023-01-01', NULL)`
    *   Example `pds$ta_pdp_context`: `(101, 201, '2023-01-01', NULL, TRUE)`
    *   Example `pds$ta_access_point`: `(201, 'internet.apn', '2023-01-01', NULL)`
3.  Replicate this exact data into the corresponding BigQuery tables.

**Action:**
1.  Execute the legacy job: `r_ausd_bp_ta_apn_carmen.ksh` (without `-s` or `-l` parameters, allowing default behavior).
2.  Trigger the Airflow DAG: `r_ausd_bp_ta_apn_carmen_dag` for an appropriate `execution_date` (e.g., `2023-01-16`).

**Pass/Fail Criterion:**
*   The number of rows in `sof$ta_apn_carmen` in Oracle must be equal to the number of rows in `sof$ta_apn_carmen` in BigQuery.
*   The content of `sof$ta_apn_carmen` in Oracle must be identical to the content of `sof$ta_apn_carmen` in BigQuery, ignoring row order.

**Runnable Test Code (Conceptual Python with SQL assertions):**

```python
import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine # For Oracle connection

# --- Configuration ---
ORACLE_CONN_STR = "oracle+cx_oracle://user:password@host:port/service_name"
BQ_PROJECT_ID = "your_project"
BQ_DATASET_ID = "your_dataset"

# --- Helper Functions (to be implemented) ---
def setup_oracle_data(data_dict):
    # Connect to Oracle and insert data into tables
    # Example: engine.execute("INSERT INTO ...")
    pass

def setup_bigquery_data(data_dict):
    # Connect to BigQuery and insert data into tables
    # Example: bq_client.query("INSERT INTO ...")
    pass

def clear_oracle_table(table_name):
    # Execute TRUNCATE TABLE on Oracle
    pass

def clear_bigquery_table(table_name):
    # Execute TRUNCATE TABLE on BigQuery
    pass

def run_legacy_job():
    # Execute the ksh script, e.g., via subprocess.run
    pass

def trigger_airflow_dag(dag_id, execution_date):
    # Use Airflow REST API or CLI to trigger DAG
    pass

def fetch_oracle_data(table_name):
    # Fetch data from Oracle table into a Pandas DataFrame
    pass

def fetch_bigquery_data(table_name):
    # Fetch data from BigQuery table into a Pandas DataFrame
    pass

# --- Test Case 1 Implementation ---
def test_output_parity_standard_data():
    # 1. Clear target tables
    clear_oracle_table("sof$ta_apn_carmen")
    clear_bigquery_table(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_apn_carmen")

    # 2. Define and populate source data
    oracle_source_data = {
        "dwtk_meldungen": [
            {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": pd.Timestamp('2023-01-15 10:00:00')}
        ],
        "pds$ta_pdp_context_assoc": [
            {"cntrct_id": "C1", "pdp_context_id": 101, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "valid_from": pd.Timestamp('2023-01-01'), "valid_to": None},
            {"cntrct_id": "C2", "pdp_context_id": 102, "insert_at": pd.Timestamp('2023-01-10'), "modified_at": pd.Timestamp('2023-01-16'), "valid_from": pd.Timestamp('2023-01-01'), "valid_to": None},
            {"cntrct_id": "C3", "pdp_context_id": 103, "insert_at": pd.Timestamp('2023-01-16'), "modified_at": None, "valid_from": pd.Timestamp('2023-01-10'), "valid_to": None}, # Should be excluded by insert_at
            {"cntrct_id": "C4", "pdp_context_id": 104, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "valid_from": pd.Timestamp('2023-01-01'), "valid_to": pd.Timestamp('2023-01-14')}, # Should be excluded by valid_to
            {"cntrct_id": "C5", "pdp_context_id": 105, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "valid_from": pd.Timestamp('2023-01-01'), "valid_to": pd.Timestamp('2023-01-15')}, # Should be excluded by valid_to
            {"cntrct_id": "C6", "pdp_context_id": 106, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "valid_from": pd.Timestamp('2023-01-01'), "valid_to": pd.Timestamp('2023-01-16')}, # Should be included
            {"cntrct_id": None, "pdp_context_id": 107, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "valid_from": pd.Timestamp('2023-01-01'), "valid_to": None}, # Should be excluded by cntrct_id IS NOT NULL
        ],
        "pds$ta_pdp_context": [
            {"pdp_context_id": 101, "access_point_id": 201, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "is_production": True},
            {"pdp_context_id": 102, "access_point_id": 202, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "is_production": True},
            {"pdp_context_id": 106, "access_point_id": 206, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "is_production": True},
            {"pdp_context_id": 107, "access_point_id": 207, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "is_production": True},
            {"pdp_context_id": 108, "access_point_id": 208, "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None, "is_production": False}, # Should be excluded by is_production
        ],
        "pds$ta_access_point": [
            {"access_point_id": 201, "access_point_name": "apn.net", "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None},
            {"access_point_id": 202, "access_point_name": "apn.gprs", "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None},
            {"access_point_id": 206, "access_point_name": "apn.lte", "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None},
            {"access_point_id": 207, "access_point_name": "apn.iot", "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None},
            {"access_point_id": 208, "access_point_name": "apn.test", "insert_at": pd.Timestamp('2023-01-01'), "modified_at": None},
        ]
    }
    setup_oracle_data(oracle_source_data)
    setup_bigquery_data(oracle_source_data) # Replicate exact data

    # 3. Execute jobs
    run_legacy_job()
    trigger_airflow_dag("r_ausd_bp_ta_apn_carmen_dag", "2023-01-16") # Execution date for DAG

    # 4. Fetch results
    oracle_result = fetch_oracle_data("sof$ta_apn_carmen")
    bigquery_result = fetch_bigquery_data(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_apn_carmen")

    # 5. Assertions
    assert len(oracle_result) == len(bigquery_result), "Row counts do not match"
    # Sort and compare DataFrames for content equivalence
    pd.testing.assert_frame_equal(
        oracle_result.sort_values(by=list(oracle_result.columns)).reset_index(drop=True),
        bigquery_result.sort_values(by=list(bigquery_result.columns)).reset_index(drop=True),
        check_dtype=False # Data types might differ slightly (e.g., string vs object)
    )
    print("Test Case 1 (Output Parity - Standard Data) Passed!")

```

---

## Test Case 2: Transformation Correctness - `v_datum` Edge Cases

**Purpose:** Verify the correct handling of `v_datum` determination, especially when `dwtk_meldungen` is empty or has specific data.

**Setup:**
1.  Clear all source and target tables in both Oracle and BigQuery.

**Action (Sub-test A: `dwtk_meldungen` is empty):**
1.  Ensure `dwtk_meldungen` is empty in both Oracle and BigQuery.
2.  Populate other source tables with data that *would* match if `v_datum` were '19000101'.
    *   Example `pds$ta_pdp_context_assoc`: `('C1', 101, '1899-12-31', NULL, '1899-12-31', NULL)`
    *   Example `pds$ta_pdp_context`: `(101, 201, '1899-12-31', NULL, TRUE)`
    *   Example `pds$ta_access_point`: `(201, 'old.apn', '1899-12-31', NULL)`
3.  Run legacy job.
4.  Trigger Airflow DAG.

**Pass/Fail Criterion (Sub-test A):**
*   Both Oracle and BigQuery `sof$ta_apn_carmen` tables must contain the expected records (those matching `v_datum = '19000101'`).
*   Row counts and content must be identical.

**Action (Sub-test B: `dwtk_meldungen` has multiple records, `MAX` logic):**
1.  Populate `dwtk_meldungen` with:
    *   `('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00')`
    *   `('OTHER_JOB', '2023-01-10 12:00:00')`
    *   `('BERT_DROP_TEMP_TABLE', '2023-01-05 08:00:00')`
    *   `('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00')`
    (Expected `v_datum = '20230115'`)
2.  Populate other source tables with data relevant to `v_datum = '20230115'`.
3.  Run legacy job.
4.  Trigger Airflow DAG.

**Pass/Fail Criterion (Sub-test B):**
*   Both Oracle and BigQuery `sof$ta_apn_carmen` tables must contain records filtered by `v_datum = '20230115'`.
*   Row counts and content must be identical.

**Runnable Test Code (Conceptual SQL assertions for `v_datum`):**

```sql
-- Sub-test A: Verify v_datum when dwtk_meldungen is empty
-- Expected v_datum: '19000101'
SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
FROM `your_project.your_dataset.dwtk_meldungen` m
WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
-- Expected result: '19000101'

-- Sub-test B: Verify v_datum with multiple records
-- Expected v_datum: '20230115'
SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
FROM `your_project.your_dataset.dwtk_meldungen` m
WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
-- Expected result: '20230115'
```

---

## Test Case 3: Transformation Correctness - Date Filter Logic & NULL Handling

**Purpose:** Validate the complex date filtering conditions and NULL handling for `modified_at` and `valid_to` columns across all three joined tables.

**Setup:**
1.  Clear all source and target tables in both Oracle and BigQuery.
2.  Set `v_datum` by inserting `('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00')` into `dwtk_meldungen`.
3.  Populate source tables with data covering various date scenarios relative to `v_datum = '20230115'`:
    *   `insert_at`: `< v_datum`, `= v_datum`, `> v_datum`
    *   `modified_at`: `NULL`, `< v_datum`, `= v_datum`, `> v_datum`
    *   `valid_from`: `< v_datum`, `= v_datum`, `> v_datum`
    *   `valid_to`: `NULL`, `< v_datum`, `= v_datum`, `> v_datum`
    *   Include cases where `cntrct_id IS NULL` and `pc.is_production = FALSE`.

**Action:**
1.  Run legacy job.
2.  Trigger Airflow DAG.

**Pass/Fail Criterion:**
*   The `sof$ta_apn_carmen` tables in Oracle and BigQuery must contain only those records that satisfy *all* date and `is_production`/`cntrct_id` conditions.
*   Row counts and content must be identical.

**Runnable Test Code (Conceptual data for `pds$ta_pdp_context_assoc` for `v_datum = '20230115'`):**

```sql
-- Example data for pds$ta_pdp_context_assoc to test filters
-- Assume pdp_context_id and access_point_id join correctly and is_production=TRUE, cntrct_id IS NOT NULL
INSERT INTO `your_project.your_dataset.pds$ta_pdp_context_assoc` (cntrct_id, pdp_context_id, insert_at, modified_at, valid_from, valid_to) VALUES
('C_PASS_1', 1, '2023-01-01', NULL, '2023-01-01', NULL), -- PASS: all conditions met
('C_PASS_2', 2, '2023-01-15', NULL, '2023-01-15', NULL), -- PASS: insert_at = v_datum, valid_from = v_datum
('C_PASS_3', 3, '2023-01-01', '2023-01-16', '2023-01-01', '2023-01-16'), -- PASS: modified_at > v_datum, valid_to > v_datum
('C_FAIL_1', 4, '2023-01-16', NULL, '2023-01-01', NULL), -- FAIL: insert_at > v_datum
('C_FAIL_2', 5, '2023-01-01', '2023-01-15', '2023-01-01', NULL), -- FAIL: modified_at = v_datum (not >)
('C_FAIL_3', 6, '2023-01-01', NULL, '2023-01-16', NULL), -- FAIL: valid_from > v_datum
('C_FAIL_4', 7, '2023-01-01', NULL, '2023-01-01', '2023-01-14'), -- FAIL: valid_to < v_datum
('C_FAIL_5', 8, '2023-01-01', NULL, '2023-01-01', '2023-01-15'); -- FAIL: valid_to = v_datum (not >)

-- Expected output for the above data (assuming other tables match): C_PASS_1, C_PASS_2, C_PASS_3
```

---

## Test Case 4: Transformation Correctness - Join Logic and Missing Data

**Purpose:** Verify that the inner join logic correctly handles cases where records are missing in one or more of the joined tables.

**Setup:**
1.  Clear all source and target tables in both Oracle and BigQuery.
2.  Set `v_datum` (e.g., `20230115`).
3.  Populate source tables with data that includes:
    *   Records present in all three tables (expected to be joined).
    *   Records in `pds$ta_pdp_context_assoc` but no matching `pdp_context_id` in `pds$ta_pdp_context`.
    *   Records in `pds$ta_pdp_context` but no matching `access_point_id` in `pds$ta_access_point`.
    *   Records in `pds$ta_access_point` but no matching `access_point_id` in `pds$ta_pdp_context`.

**Action:**
1.  Run legacy job.
2.  Trigger Airflow DAG.

**Pass/Fail Criterion:**
*   Only records that have a match across all three tables (`pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`) and satisfy all filtering conditions should be present in `sof$ta_apn_carmen`.
*   Row counts and content must be identical.

**Runnable Test Code (Conceptual data):**

```sql
-- Example data to test joins (simplified dates for clarity, assume v_datum allows all)
INSERT INTO `your_project.your_dataset.pds$ta_pdp_context_assoc` (cntrct_id, pdp_context_id, insert_at, modified_at, valid_from, valid_to) VALUES
('C_FULL_MATCH', 101, '2023-01-01', NULL, '2023-01-01', NULL),
('C_NO_PC', 102, '2023-01-01', NULL, '2023-01-01', NULL), -- pdp_context_id 102 not in pds$ta_pdp_context
('C_NO_AP', 103, '2023-01-01', NULL, '2023-01-01', NULL);

INSERT INTO `your_project.your_dataset.pds$ta_pdp_context` (pdp_context_id, access_point_id, insert_at, modified_at, is_production) VALUES
(101, 201, '2023-01-01', NULL, TRUE),
(103, 203, '2023-01-01', NULL, TRUE), -- access_point_id 203 not in pds$ta_access_point
(104, 204, '2023-01-01', NULL, TRUE); -- pdp_context_id 104 not in pds$ta_pdp_context_assoc

INSERT INTO `your_project.your_dataset.pds$ta_access_point` (access_point_id, access_point_name, insert_at, modified_at) VALUES
(201, 'full.apn', '2023-01-01', NULL),
(202, 'orphan.apn', '2023-01-01', NULL), -- access_point_id 202 not in pds$ta_pdp_context
(204, 'another.apn', '2023-01-01', NULL);

-- Expected output: Only ('C_FULL_MATCH', 'full.apn')
```

---

## Test Case 5: External System Replacements - `TRUNCATE TABLE`

**Purpose:** Verify that the BigQuery `TRUNCATE TABLE` operation correctly replaces the Oracle stored procedure call for clearing the target table.

**Setup:**
1.  Populate `sof$ta_apn_carmen` in both Oracle and BigQuery with some dummy data.
    *   Example: `('DUMMY1', 'dummy.apn')`, `('DUMMY2', 'test.apn')`
2.  Populate source tables with data that *will* result in new insertions (e.g., a single valid record).
3.  Set `v_datum` (e.g., `20230115`).

**Action:**
1.  Run legacy job.
2.  Trigger Airflow DAG.

**Pass/Fail Criterion:**
*   After both jobs complete, the `sof$ta_apn_carmen` tables in Oracle and BigQuery must contain *only* the newly inserted records, and *not* the dummy data from the setup.
*   Row counts and content must be identical.

**Runnable Test Code (Conceptual SQL assertion):**

```sql
-- Before job execution:
SELECT COUNT(*) FROM `your_project.your_dataset.sof$ta_apn_carmen`; -- Expected: 2 (dummy data)

-- After job execution:
SELECT COUNT(*) FROM `your_project.your_dataset.sof$ta_apn_carmen`; -- Expected: 1 (newly inserted record)
SELECT CNTRCT_ID, ACCESS_POINT_NAME FROM `your_project.your_dataset.sof$ta_apn_carmen`;
-- Expected: ('NEW_CONTRACT', 'new.apn')
```

---

## Test Case 6: Data Quality / Row Count / Schema Assertions

**Purpose:** Verify that the target table schema is correct, no data is truncated, and row counts are consistent.

**Setup:**
1.  Clear all source and target tables in both Oracle and BigQuery.
2.  Populate source tables with a large volume of data (e.g., 100,000+ records) that will result in a significant number of records being inserted into `sof$ta_apn_carmen`. Include long `ACCESS_POINT_NAME` values to test for truncation.
3.  Set `v_datum` to ensure most data is processed.

**Action:**
1.  Run legacy job.
2.  Trigger Airflow DAG.

**Pass/Fail Criterion:**
*   **Schema:** The BigQuery `sof$ta_apn_carmen` table schema must match the expected DDL (e.g., `CNTRCT_ID STRING NOT NULL`, `ACCESS_POINT_NAME STRING`).
*   **Row Count:** The total number of rows in `sof$ta_apn_carmen` in Oracle must exactly match the total number of rows in BigQuery.
*   **Data Integrity:** No `ACCESS_POINT_NAME` values should be truncated in BigQuery compared to Oracle. All `CNTRCT_ID` values should be present and not NULL.
*   **Output Parity:** The content of `sof$ta_apn_carmen` in Oracle must be identical to BigQuery.

**Runnable Test Code (Conceptual Python with BigQuery API and SQL):**

```python
from google.cloud import bigquery

def test_data_quality_and_schema():
    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    table_ref = bq_client.dataset(BQ_DATASET_ID).table("sof$ta_apn_carmen")
    table = bq_client.get_table(table_ref)

    # 1. Schema Assertion
    expected_schema = {
        "CNTRCT_ID": {"field_type": "STRING", "mode": "REQUIRED"},
        "ACCESS_POINT_NAME": {"field_type": "STRING", "mode": "NULLABLE"},
    }
    actual_schema = {field.name: {"field_type": field.field_type, "mode": field.mode} for field in table.schema}

    for field_name, expected_props in expected_schema.items():
        assert field_name in actual_schema, f"Field {field_name} missing in BigQuery schema."
        assert actual_schema[field_name]["field_type"] == expected_props["field_type"], \
            f"Field {field_name} type mismatch: Expected {expected_props['field_type']}, Got {actual_schema[field_name]['field_type']}"
        assert actual_schema[field_name]["mode"] == expected_props["mode"], \
            f"Field {field_name} mode mismatch: Expected {expected_props['mode']}, Got {actual_schema[field_name]['mode']}"
    print("Schema assertion passed.")

    # (Assume setup and action from Test Case 1 or similar large data run)

    # 2. Row Count Assertion
    oracle_row_count_query = "SELECT COUNT(*) FROM sof$ta_apn_carmen"
    # oracle_row_count = execute_oracle_query(oracle_row_count_query).fetchone()[0]
    # For this example, let's assume we got 100000 rows from Oracle
    oracle_row_count = 100000

    bq_row_count_query = f"SELECT COUNT(*) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_apn_carmen`"
    bq_row_count = bq_client.query(bq_row_count_query).result().to_dataframe().iloc[0, 0]

    assert oracle_row_count == bq_row_count, f"Row count mismatch: Oracle={oracle_row_count}, BigQuery={bq_row_count}"
    print(f"Row count assertion passed: {oracle_row_count} rows.")

    # 3. Data Integrity (e.g., no NULLs for NOT NULL columns, no truncation)
    # This would typically be part of the full output parity check, but can be isolated.
    bq_null_cntrct_id_query = f"SELECT COUNT(*) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_apn_carmen` WHERE CNTRCT_ID IS NULL"
    bq_null_cntrct_id_count = bq_client.query(bq_null_cntrct_id_query).result().to_dataframe().iloc[0, 0]
    assert bq_null_cntrct_id_count == 0, "CNTRCT_ID column contains NULL values, but should be NOT NULL."
    print("CNTRCT_ID NOT NULL assertion passed.")

    # For truncation, compare max length of ACCESS_POINT_NAME in both systems
    # oracle_max_len_apn = execute_oracle_query("SELECT MAX(LENGTH(ACCESS_POINT_NAME)) FROM sof$ta_apn_carmen").fetchone()[0]
    # bq_max_len_apn = bq_client.query(f"SELECT MAX(LENGTH(ACCESS_POINT_NAME)) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_apn_carmen`").result().to_dataframe().iloc[0, 0]
    # assert oracle_max_len_apn == bq_max_len_apn, "ACCESS_POINT_NAME max length mismatch (potential truncation)."
    # print("ACCESS_POINT_NAME truncation check passed.")

    print("Test Case 6 (Data Quality / Row Count / Schema) Passed!")

```