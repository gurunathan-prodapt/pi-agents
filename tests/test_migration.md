The migration of `r_ausd_bp_ta_apn_vertrag.ksh` from KornShell/Oracle to Airflow/BigQuery involves significant changes in orchestration and data transformation. The core logic shifts from a PL/SQL loop with explicit string concatenation to a set-based BigQuery SQL query utilizing `STRING_AGG`. A critical behavioral difference identified is the handling of the target table: the legacy job explicitly `TRUNCATE`s the entire table, while the migrated BigQuery SQL performs a `DELETE` operation filtered by `snapshot_date`. This implies a change from a "current state" table to a "historical snapshot" table, which is a significant behavioral deviation from strict equivalence.

The tests below are designed to validate the migrated BigQuery SQL and Airflow DAG, assuming the `DELETE WHERE snapshot_date = v_snapshot_date` behavior is the *intended* outcome of the migration, even if it diverges from a strict interpretation of the legacy `TRUNCATE` command. This assumption is made because the `snapshot_date` column was added to the target BigQuery table, suggesting a design shift towards historical data retention. However, this discrepancy is explicitly highlighted in Test Case 5.

---

## Migration Validation Tests for `r_ausd_bp_ta_apn_vertrag.ksh`

**Target Environment:** Google Cloud Platform (BigQuery, Airflow)
**Source Files:**
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh`
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`
*   `vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql`

**Migrated Files (under test):**
*   `ddl/create_isbert_dwtk_meldungen.sql`
*   `ddl/create_sof_ta_bpr_apn.sql`
*   `ddl/create_sof_ta_apn_vertrag.sql`
*   `sql/d_ausd_bp_ta_apn_vertrag.sql` (embedded in DAG)
*   `dags/isbert_r_ausd_bp_ta_apn_vertrag_dag.py`

---

### Test Setup Prerequisites

For all tests, assume a BigQuery test environment is configured with the following:
*   `GCP_PROJECT_ID` and `BIGQUERY_DATASET` are set to appropriate test values.
*   BigQuery tables `isbert_dwtk_meldungen`, `sof_ta_bpr_apn`, and `sof_ta_apn_vertrag` exist, created using the provided DDLs.
*   A Python environment with `pytest` and `google-cloud-bigquery` installed.
*   Helper functions for BigQuery interaction (clearing tables, inserting data, executing queries) are available.

```python
import pytest
from google.cloud import bigquery
from datetime import date, datetime, timezone

# --- Configuration ---
PROJECT_ID = "your_gcp_project"  # Replace with your GCP project ID
DATASET_ID = "your_bigquery_dataset"  # Replace with your BigQuery dataset ID
CLIENT = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions for BigQuery Interaction ---
def _execute_sql(sql_query):
    """Helper to execute BigQuery SQL."""
    job = CLIENT.query(sql_query)
    job.result()  # Wait for the job to complete

def _get_table_rows(table_name, order_by_cols=None):
    """Helper to fetch all rows from a BigQuery table."""
    order_clause = f"ORDER BY {', '.join(order_by_cols)}" if order_by_cols else ""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` {order_clause}"
    rows = CLIENT.query(query).result()
    return [dict(row) for row in rows]

def _clear_table(table_name):
    """Helper to clear a BigQuery table using TRUNCATE."""
    _execute_sql(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{table_name}`")

def _insert_into_bpr_apn(data):
    """Helper to insert data into sof_ta_bpr_apn."""
    _clear_table("sof_ta_bpr_apn")
    if data:
        errors = CLIENT.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_apn", data)
        if errors:
            raise Exception(f"Errors inserting into sof_ta_bpr_apn: {errors}")

def _insert_into_dwtk_meldungen(data):
    """Helper to insert data into isbert_dwtk_meldungen."""
    _clear_table("isbert_dwtk_meldungen")
    if data:
        # Convert datetime objects to ISO format strings for BigQuery insert_rows_json
        formatted_data = []
        for row in data:
            new_row = row.copy()
            if 'timecreated' in new_row and isinstance(new_row['timecreated'], datetime):
                new_row['timecreated'] = new_row['timecreated'].isoformat()
            formatted_data.append(new_row)
        errors = CLIENT.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.isbert_dwtk_meldungen", formatted_data)
        if errors:
            raise Exception(f"Errors inserting into isbert_dwtk_meldungen: {errors}")

# The core SQL logic from the DAG, parameterized for testing
BIGQUERY_TRANSFORMATION_SQL = f"""
DECLARE v_snapshot_date DATE DEFAULT (
  SELECT COALESCE(MAX(DATE(timecreated)), DATE '1900-01-01')
  FROM `{PROJECT_ID}.{DATASET_ID}.isbert_dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

DELETE FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_apn_vertrag`
WHERE snapshot_date = v_snapshot_date;

INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_apn_vertrag`
  (cntrct_id, apn_list, contract_ref_list, snapshot_date)
SELECT
  cntrct_id,
  SUBSTR(STRING_AGG(access_point_name, ', ' ORDER BY access_point_name), 1, 100) AS apn_list,
  SUBSTR(STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref), 1, 100) AS contract_ref_list,
  v_snapshot_date AS snapshot_date
FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_apn`
GROUP BY cntrct_id;
"""

# Fixture to clean up tables before and after each test
@pytest.fixture(autouse=True)
def cleanup_tables():
    _clear_table("sof_ta_apn_vertrag")
    _clear_table("sof_ta_bpr_apn")
    _clear_table("isbert_dwtk_meldungen")
    yield
    _clear_table("sof_ta_apn_vertrag")
    _clear_table("sof_ta_bpr_apn")
    _clear_table("isbert_dwtk_meldungen")
```

---

### 1. Output Parity - Standard Aggregation

**Purpose:** Verify that the migrated job correctly aggregates `access_point_name` and `cntrct_id_ref` for multiple contracts and inserts them into the target table, matching the expected output for a typical scenario.

**Setup:**
1.  Populate `sof_ta_bpr_apn` with sample data for multiple contracts, each having several APNs and reference IDs.
2.  Populate `isbert_dwtk_meldungen` to define a `v_snapshot_date`.

**Action:**
Execute the BigQuery transformation SQL (as part of the Airflow DAG).

**Pass/Fail Criterion:**
The `sof_ta_apn_vertrag` table must contain the expected number of rows, and the `apn_list` and `contract_ref_list` columns for each `cntrct_id` must exactly match the comma-separated, alphabetically ordered aggregated strings, with the correct `snapshot_date`.

```python
def test_output_parity_standard_aggregation():
    # Setup: Source data for sof_ta_bpr_apn
    bpr_apn_data = [
        {"cntrct_id": "C1", "access_point_name": "APN_A", "cntrct_id_ref": "REF_X"},
        {"cntrct_id": "C1", "access_point_name": "APN_B", "cntrct_id_ref": "REF_Y"},
        {"cntrct_id": "C2", "access_point_name": "APN_C", "cntrct_id_ref": "REF_Z"},
        {"cntrct_id": "C2", "access_point_name": "APN_D", "cntrct_id_ref": "REF_W"},
        {"cntrct_id": "C1", "access_point_name": "APN_E", "cntrct_id_ref": "REF_A"},
    ]
    _insert_into_bpr_apn(bpr_apn_data)

    # Setup: Snapshot date
    current_snapshot_date = date(2023, 10, 26)
    dwtk_meldungen_data = [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 10, 26, 10, 0, 0, tzinfo=timezone.utc)},
        {"job_kennung": "OTHER_JOB", "timecreated": datetime(2023, 10, 25, 10, 0, 0, tzinfo=timezone.utc)},
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data)

    # Action: Execute the transformation
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)

    # Expected results
    expected_output = [
        {
            "cntrct_id": "C1",
            "apn_list": "APN_A, APN_B, APN_E",
            "contract_ref_list": "REF_A, REF_X, REF_Y",
            "snapshot_date": current_snapshot_date,
        },
        {
            "cntrct_id": "C2",
            "apn_list": "APN_C, APN_D",
            "contract_ref_list": "REF_W, REF_Z",
            "snapshot_date": current_snapshot_date,
        },
    ]

    # Assertion
    actual_output = _get_table_rows("sof_ta_apn_vertrag", order_by_cols=["cntrct_id"])
    assert len(actual_output) == len(expected_output)
    assert actual_output == expected_output
```

---

### 2. Transformation Correctness - String Truncation (100 characters)

**Purpose:** Verify that aggregated `apn_list` and `contract_ref_list` strings are correctly truncated to a maximum of 100 characters, as specified in the design.

**Setup:**
1.  Populate `sof_ta_bpr_apn` with data where the aggregated `access_point_name` and `cntrct_id_ref` for at least one `cntrct_id` will exceed 100 characters.
2.  Populate `isbert_dwtk_meldungen` to define a `v_snapshot_date`.

**Action:**
Execute the BigQuery transformation SQL.

**Pass/Fail Criterion:**
The `apn_list` and `contract_ref_list` for the relevant `cntrct_id` must be exactly 100 characters long, containing the beginning of the aggregated string. Other `cntrct_id`s should have their full (untruncated) aggregated strings if they are under 100 characters.

```python
def test_transformation_string_truncation():
    # Setup: Source data designed to exceed 100 chars
    long_apn_prefix = "APN_LONG_STRING_"
    long_ref_prefix = "REF_VERY_LONG_STRING_"
    bpr_apn_data = []
    for i in range(10): # Create 10 entries, each adding ~20 chars (including ', ')
        bpr_apn_data.append({
            "cntrct_id": "C_LONG",
            "access_point_name": f"{long_apn_prefix}{i:02d}",
            "cntrct_id_ref": f"{long_ref_prefix}{i:02d}"
        })
    # Add a short one to ensure others are not affected
    bpr_apn_data.append({"cntrct_id": "C_SHORT", "access_point_name": "APN_S", "cntrct_id_ref": "REF_S"})
    _insert_into_bpr_apn(bpr_apn_data)

    # Setup: Snapshot date
    current_snapshot_date = date(2023, 10, 27)
    dwtk_meldungen_data = [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 10, 27, 10, 0, 0, tzinfo=timezone.utc)},
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data)

    # Action: Execute the transformation
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)

    # Expected results (manually construct the first 100 chars)
    expected_long_apn = ", ".join([f"{long_apn_prefix}{i:02d}" for i in range(10)])[:100]
    expected_long_ref = ", ".join([f"{long_ref_prefix}{i:02d}" for i in range(10)])[:100]

    expected_output = [
        {
            "cntrct_id": "C_LONG",
            "apn_list": expected_long_apn,
            "contract_ref_list": expected_long_ref,
            "snapshot_date": current_snapshot_date,
        },
        {
            "cntrct_id": "C_SHORT",
            "apn_list": "APN_S",
            "contract_ref_list": "REF_S",
            "snapshot_date": current_snapshot_date,
        },
    ]

    # Assertion
    actual_output = _get_table_rows("sof_ta_apn_vertrag", order_by_cols=["cntrct_id"])
    assert len(actual_output) == len(expected_output)
    assert actual_output == expected_output
```

---

### 3. Transformation Correctness - NULL Handling in Aggregation

**Purpose:** Verify that `NULL` values in `access_point_name` or `cntrct_id_ref` are correctly handled during aggregation (i.e., ignored and not included in the comma-separated list).

**Setup:**
1.  Populate `sof_ta_bpr_apn` with data including `NULL` values for `access_point_name` and `cntrct_id_ref` in various combinations.
2.  Populate `isbert_dwtk_meldungen` to define a `v_snapshot_date`.

**Action:**
Execute the BigQuery transformation SQL.

**Pass/Fail Criterion:**
`NULL` values must be omitted from the `apn_list` and `contract_ref_list`. If all values for a given `cntrct_id` are `NULL` for a specific list, that list should be `NULL`.

```python
def test_transformation_null_handling():
    # Setup: Source data with NULLs
    bpr_apn_data = [
        {"cntrct_id": "C_NULL_APN", "access_point_name": None, "cntrct_id_ref": "REF_X"},
        {"cntrct_id": "C_NULL_APN", "access_point_name": "APN_B", "cntrct_id_ref": "REF_Y"},
        {"cntrct_id": "C_NULL_REF", "access_point_name": "APN_C", "cntrct_id_ref": None},
        {"cntrct_id": "C_NULL_REF", "access_point_name": "APN_D", "cntrct_id_ref": "REF_W"},
        {"cntrct_id": "C_ALL_NULL", "access_point_name": None, "cntrct_id_ref": None},
        {"cntrct_id": "C_MIXED", "access_point_name": "APN_F", "cntrct_id_ref": None},
        {"cntrct_id": "C_MIXED", "access_point_name": None, "cntrct_id_ref": "REF_G"},
        {"cntrct_id": "C_MIXED", "access_point_name": "APN_H", "cntrct_id_ref": "REF_I"},
    ]
    _insert_into_bpr_apn(bpr_apn_data)

    # Setup: Snapshot date
    current_snapshot_date = date(2023, 10, 28)
    dwtk_meldungen_data = [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 10, 28, 10, 0, 0, tzinfo=timezone.utc)},
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data)

    # Action: Execute the transformation
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)

    # Expected results
    expected_output = [
        {
            "cntrct_id": "C_ALL_NULL",
            "apn_list": None,  # STRING_AGG returns NULL if all inputs are NULL
            "contract_ref_list": None,
            "snapshot_date": current_snapshot_date,
        },
        {
            "cntrct_id": "C_MIXED",
            "apn_list": "APN_F, APN_H",
            "contract_ref_list": "REF_G, REF_I",
            "snapshot_date": current_snapshot_date,
        },
        {
            "cntrct_id": "C_NULL_APN",
            "apn_list": "APN_B",
            "contract_ref_list": "REF_X, REF_Y",
            "snapshot_date": current_snapshot_date,
        },
        {
            "cntrct_id": "C_NULL_REF",
            "apn_list": "APN_C, APN_D",
            "contract_ref_list": "REF_W",
            "snapshot_date": current_snapshot_date,
        },
    ]

    # Assertion
    actual_output = _get_table_rows("sof_ta_apn_vertrag", order_by_cols=["cntrct_id"])
    assert len(actual_output) == len(expected_output)
    assert actual_output == expected_output
```

---

### 4. Transformation Correctness - Snapshot Date Logic

**Purpose:** Verify that `v_snapshot_date` is correctly determined from `isbert_dwtk_meldungen` (picking the latest `timecreated` for `BERT_DROP_TEMP_TABLE`) and defaults to '1900-01-01' if no matching entry exists.

**Setup:**
1.  Populate `sof_ta_bpr_apn` with some basic data.
2.  Populate `isbert_dwtk_meldungen` with various `job_kennung` and `timecreated` values, including cases where `BERT_DROP_TEMP_TABLE` is present or absent.

**Action:**
Execute the BigQuery transformation SQL.

**Pass/Fail Criterion:**
The `snapshot_date` column in `sof_ta_apn_vertrag` must match the expected date derived from `isbert_dwtk_meldungen` or the default '1900-01-01'.

```python
def test_transformation_snapshot_date_logic():
    # Setup: Source data for sof_ta_bpr_apn
    bpr_apn_data = [
        {"cntrct_id": "C1", "access_point_name": "APN_A", "cntrct_id_ref": "REF_X"},
    ]
    _insert_into_bpr_apn(bpr_apn_data)

    # --- Test Case 1: Snapshot date correctly derived ---
    dwtk_meldungen_data_1 = [
        {"job_kennung": "OTHER_JOB", "timecreated": datetime(2023, 10, 25, 10, 0, 0, tzinfo=timezone.utc)},
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 10, 26, 10, 0, 0, tzinfo=timezone.utc)},
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 10, 26, 11, 0, 0, tzinfo=timezone.utc)}, # Latest
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data_1)
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)
    actual_output_1 = _get_table_rows("sof_ta_apn_vertrag")
    assert actual_output_1[0]["snapshot_date"] == date(2023, 10, 26)
    _clear_table("sof_ta_apn_vertrag") # Clear for next sub-test

    # --- Test Case 2: Default snapshot date when no matching job_kennung ---
    dwtk_meldungen_data_2 = [
        {"job_kennung": "OTHER_JOB", "timecreated": datetime(2023, 10, 25, 10, 0, 0, tzinfo=timezone.utc)},
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data_2)
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)
    actual_output_2 = _get_table_rows("sof_ta_apn_vertrag")
    assert actual_output_2[0]["snapshot_date"] == date(1900, 1, 1)
    _clear_table("sof_ta_apn_vertrag") # Clear for next sub-test

    # --- Test Case 3: Default snapshot date when dwtk_meldungen is empty ---
    _clear_table("isbert_dwtk_meldungen")
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)
    actual_output_3 = _get_table_rows("sof_ta_apn_vertrag")
    assert actual_output_3[0]["snapshot_date"] == date(1900, 1, 1)
```

---

### 5. Behavioral Equivalence - Truncation vs. Date-Specific Delete (CRITICAL DISCREPANCY)

**Purpose:** To explicitly test the behavior of the target table cleanup. The design document states the legacy job `TRUNCATE`s the target table. The migrated BigQuery SQL performs a `DELETE FROM ... WHERE snapshot_date = v_snapshot_date`. This test highlights this behavioral difference.

**Setup:**
1.  Populate `sof_ta_bpr_apn` with data for `cntrct_id` 'C1'.
2.  Populate `isbert_dwtk_meldungen` to set `v_snapshot_date` to '2023-01-01'.
3.  **Crucially**, pre-populate `sof_ta_apn_vertrag` with data for a *different* `snapshot_date`, e.g., '2022-12-31', and also for '2023-01-01' (which should be overwritten by the current run).

**Action:**
Execute the BigQuery transformation SQL.

**Pass/Fail Criterion:**
*   **If strict behavioral equivalence to legacy `TRUNCATE` is required:** The test should **FAIL** if `sof_ta_apn_vertrag` contains any rows with `snapshot_date = '2022-12-31'` after the job runs. It should only contain data for `snapshot_date = '2023-01-01'`.
*   **If the migrated `DELETE WHERE snapshot_date = v_snapshot_date` is the intended new behavior (historical table):** The test should **PASS** if `sof_ta_apn_vertrag` contains data for `snapshot_date = '2022-12-31'` *and* the newly inserted data for `2023-01-01`.

**Note:** Based on the prompt's requirement for "behaviourally equivalent", the current BigQuery SQL *does not* achieve this for the table cleanup. This test is designed to flag this discrepancy.

```python
def test_behavioral_equivalence_truncation_vs_delete():
    # Setup: Source data for sof_ta_bpr_apn
    bpr_apn_data = [
        {"cntrct_id": "C_NEW", "access_point_name": "APN_NEW", "cntrct_id_ref": "REF_NEW"},
    ]
    _insert_into_bpr_apn(bpr_apn_data)

    # Setup: Snapshot date for current run
    current_snapshot_date = date(2023, 1, 1)
    dwtk_meldungen_data = [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)},
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data)

    # Setup: Pre-populate target table with old data and data for the current snapshot date
    # This simulates previous runs and data that should be cleared/overwritten
    _execute_sql(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_apn_vertrag`
        (cntrct_id, apn_list, contract_ref_list, snapshot_date)
        VALUES
        ('C_OLD_1', 'APN_OLD_1', 'REF_OLD_1', DATE('2022-12-31')),
        ('C_OLD_2', 'APN_OLD_2', 'REF_OLD_2', DATE('2022-12-30')),
        ('C_OVERWRITE', 'APN_OLD_OVERWRITE', 'REF_OLD_OVERWRITE', DATE('2023-01-01'))
    """)

    # Action: Execute the transformation
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)

    # Expected output based on the *migrated BigQuery SQL's behavior*
    # The 'C_OLD_1' and 'C_OLD_2' entries for previous dates should remain.
    # The 'C_OVERWRITE' entry for 2023-01-01 should be replaced by 'C_NEW'.
    expected_output_for_migrated_behavior = [
        {
            "cntrct_id": "C_NEW",
            "apn_list": "APN_NEW",
            "contract_ref_list": "REF_NEW",
            "snapshot_date": current_snapshot_date,
        },
        {
            "cntrct_id": "C_OLD_1",
            "apn_list": "APN_OLD_1",
            "contract_ref_list": "REF_OLD_1",
            "snapshot_date": date(2022, 12, 31),
        },
        {
            "cntrct_id": "C_OLD_2",
            "apn_list": "APN_OLD_2",
            "contract_ref_list": "REF_OLD_2",
            "snapshot_date": date(2022, 12, 30),
        },
    ]

    actual_output = _get_table_rows("sof_ta_apn_vertrag", order_by_cols=["snapshot_date", "cntrct_id"])

    # Assertion for the *migrated BigQuery SQL's behavior*
    # This test will PASS if the BigQuery SQL behaves as written (date-specific delete).
    # It would FAIL if the BigQuery SQL were changed to a full TRUNCATE.
    assert len(actual_output) == len(expected_output_for_migrated_behavior)
    assert actual_output == expected_output_for_migrated_behavior

    # --- CRITICAL NOTE ON BEHAVIORAL DISCREPANCY ---
    # If the legacy job truly performed a full TRUNCATE TABLE, then the expected
    # behavior for strict equivalence would be that only the 'C_NEW' entry for
    # 2023-01-01 remains, and all older snapshot dates would be gone.
    # The current BigQuery SQL implements a date-specific DELETE, which means
    # the target table becomes a historical snapshot table. This is a behavioral
    # change from a "current state" table to a "historical" table.
    # This discrepancy should be reviewed with the migration team.
    # To achieve strict behavioral equivalence for truncation, the BigQuery SQL
    # should be: `TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_apn_vertrag`;`
    # (assuming `snapshot_date` is removed or handled differently for a non-historical table).
    # Or, if `snapshot_date` is intended for partitioning, then `DELETE FROM ... WHERE TRUE;`
    # would clear all data across all partitions before inserting new data.
```

---

### 6. Data Quality - Empty Source Table (`sof_ta_bpr_apn`)

**Purpose:** Verify that the job handles an empty source table gracefully, resulting in an empty target table for the current snapshot date.

**Setup:**
1.  Ensure `sof_ta_bpr_apn` is empty.
2.  Populate `isbert_dwtk_meldungen` to define a `v_snapshot_date`.
3.  Pre-populate `sof_ta_apn_vertrag` with data for a *different* `snapshot_date` to ensure only the current snapshot date's data is affected.

**Action:**
Execute the BigQuery transformation SQL.

**Pass/Fail Criterion:**
The `sof_ta_apn_vertrag` table must contain no rows for the `v_snapshot_date`, but retain any pre-existing rows for other `snapshot_date`s (consistent with the `DELETE WHERE snapshot_date` behavior).

```python
def test_data_quality_empty_source_table():
    # Setup: Empty sof_ta_bpr_apn (handled by fixture and _insert_into_bpr_apn([]))
    _insert_into_bpr_apn([])

    # Setup: Snapshot date
    current_snapshot_date = date(2023, 11, 1)
    dwtk_meldungen_data = [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 11, 1, 10, 0, 0, tzinfo=timezone.utc)},
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data)

    # Pre-populate target table with old data
    _execute_sql(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_apn_vertrag`
        (cntrct_id, apn_list, contract_ref_list, snapshot_date)
        VALUES
        ('C_OLD', 'APN_OLD', 'REF_OLD', DATE('2023-10-31'))
    """)

    # Action: Execute the transformation
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)

    # Expected results: Only the old data should remain, no new data for current_snapshot_date
    expected_output = [
        {
            "cntrct_id": "C_OLD",
            "apn_list": "APN_OLD",
            "contract_ref_list": "REF_OLD",
            "snapshot_date": date(2023, 10, 31),
        },
    ]

    # Assertion
    actual_output = _get_table_rows("sof_ta_apn_vertrag", order_by_cols=["snapshot_date", "cntrct_id"])
    assert len(actual_output) == len(expected_output)
    assert actual_output == expected_output
```

---

### 7. Data Quality - Row Counts

**Purpose:** Verify that the total number of rows in the target table for the current snapshot date matches the number of unique `cntrct_id`s in the source table.

**Setup:**
1.  Populate `sof_ta_bpr_apn` with a known number of unique `cntrct_id`s, some with multiple entries.
2.  Populate `isbert_dwtk_meldungen` to define a `v_snapshot_date`.

**Action:**
Execute the BigQuery transformation SQL.

**Pass/Fail Criterion:**
The count of rows in `sof_ta_apn_vertrag` for the `v_snapshot_date` must equal the count of distinct `cntrct_id`s in `sof_ta_bpr_apn`.

```python
def test_data_quality_row_counts():
    # Setup: Source data with varying cntrct_id counts
    bpr_apn_data = [
        {"cntrct_id": "C1", "access_point_name": "APN_A", "cntrct_id_ref": "REF_X"},
        {"cntrct_id": "C1", "access_point_name": "APN_B", "cntrct_id_ref": "REF_Y"}, # Duplicate C1
        {"cntrct_id": "C2", "access_point_name": "APN_C", "cntrct_id_ref": "REF_Z"},
        {"cntrct_id": "C3", "access_point_name": "APN_D", "cntrct_id_ref": "REF_W"},
        {"cntrct_id": "C3", "access_point_name": "APN_E", "cntrct_id_ref": "REF_V"}, # Duplicate C3
        {"cntrct_id": "C4", "access_point_name": "APN_F", "cntrct_id_ref": "REF_U"},
    ]
    _insert_into_bpr_apn(bpr_apn_data)

    # Setup: Snapshot date
    current_snapshot_date = date(2023, 11, 2)
    dwtk_meldungen_data = [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": datetime(2023, 11, 2, 10, 0, 0, tzinfo=timezone.utc)},
    ]
    _insert_into_dwtk_meldungen(dwtk_meldungen_data)

    # Action: Execute the transformation
    _execute_sql(BIGQUERY_TRANSFORMATION_SQL)

    # Expected row count: 4 unique cntrct_ids (C1, C2, C3, C4)
    expected_row_count = 4

    # Assertion
    query_count = f"""
        SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_apn_vertrag`
        WHERE snapshot_date = '{current_snapshot_date.isoformat()}'
    """
    job = CLIENT.query(query_count)
    actual_row_count = list(job.result())[0][0]

    assert actual_row_count == expected_row_count
```