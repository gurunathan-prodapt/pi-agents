The migration of `r_ausd_bp_ta_iccid_einzeln.ksh` to Google Cloud Platform involves several components: BigQuery stored procedures for logic and data storage, and Cloud Composer for orchestration. The following tests aim to validate the behavioral equivalence and correctness of this migration.

**Assumptions & Discrepancies Noted:**

1.  **`p_wiederanlaufWert` Default Behavior:**
    *   **Legacy:** If `-l` is not provided, `p_wiederanlaufWert` defaults to `0`. The filter becomes `DWH_VERTRAG_ID > 0`.
    *   **Migrated:** If `p_wiederanlaufWert_raw` is `NULL` or empty, `v_wiederanlaufWert` in the wrapper SPROC remains `NULL`. The kernel SPROC's filter `(p_wiederanlaufWert IS NULL OR dcc.dwh_vertrag_id > p_wiederanlaufWert)` effectively becomes `TRUE` when `p_wiederanlaufWert` is `NULL`, meaning **no filter is applied on `dwh_vertrag_id`**.
    *   **Impact:** This is a significant behavioral difference. The migrated job will extract records with `dwh_vertrag_id <= 0` if `p_wiederanlaufWert_raw` is not provided, which the legacy job would not. This is flagged as a **parity discrepancy**. The tests below will validate the *migrated* behavior as currently implemented. To achieve parity, the wrapper SPROC should explicitly set `v_wiederanlaufWert = 0` if `p_wiederanlaufWert_raw` is `NULL` or empty.

2.  **Idempotency / Target Table Handling:**
    *   **Legacy:** The description states: "Zu beachten ist hierbei, dass eine bereits bereitgestellte Tabelle dann geloescht wird, wenn keine aktive Vertragscache existiert, die noch nicht abgeholt worden ist." This implies a conditional deletion/truncation of the target table.
    *   **Migrated:** The `k_ausd_bp_ta_iccid_einzeln` SPROC currently only performs an `INSERT` into `fos_contract_data`. The `UNIQUE (contract_id, stichtag) NOT ENFORCED` constraint in BigQuery allows duplicate rows to be inserted if the job is run multiple times with the same `stichtag` and source data.
    *   **Impact:** This is a **major parity discrepancy**. The migrated job is not idempotent and will append data, potentially leading to duplicates, whereas the legacy job implies a conditional overwrite. The tests below will validate the *migrated* behavior (appending duplicates). A `MERGE` statement or explicit `TRUNCATE` (if full reload is always intended) would be required for parity or idempotency.

3.  **`p_stichtag` Default:** Both legacy (using `v_sysdate`) and migrated (using `CURRENT_DATE()`) correctly default to the current system date. This is behaviorally equivalent.

---

### Test Setup (Common for all tests)

```python
import pytest
from google.cloud import bigquery
import datetime
import uuid
import json
import time

# --- Configuration ---
# Replace with your actual GCP project and dataset IDs
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_bq_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions ---

def execute_bq_sql(sql_query):
    """Executes a BigQuery SQL query and returns the results."""
    print(f"\nExecuting SQL:\n{sql_query}")
    query_job = BQ_CLIENT.query(sql_query)
    try:
        results = list(query_job.result())
        print(f"SQL execution successful. Rows affected/returned: {len(results) if results else 'N/A'}")
        return results
    except Exception as e:
        print(f"SQL execution failed: {e}")
        raise

def call_sproc(sproc_name, params):
    """Calls a BigQuery stored procedure."""
    param_str = ", ".join([f"{k} => {v}" for k, v in params.items()])
    sql = f"CALL `{PROJECT_ID}.{DATASET_ID}.{sproc_name}`({param_str});"
    return execute_bq_sql(sql)

def clear_table(table_id):
    """Truncates a BigQuery table."""
    execute_bq_sql(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{table_id}`")
    print(f"Table `{PROJECT_ID}.{DATASET_ID}.{table_id}` truncated.")

def insert_dwh_contract_cache(data):
    """Inserts data into the dwh_contract_cache table."""
    table_ref = BQ_CLIENT.get_table(f"{PROJECT_ID}.{DATASET_ID}.dwh_contract_cache")
    rows_to_insert = []
    for row in data:
        rows_to_insert.append(
            (
                row.get("dwh_vertrag_id"),
                row.get("gueltig_von"),
                row.get("gueltig_bis"),
                row.get("ladedatum"),
                row.get("product_id"),
                row.get("customer_id"),
                datetime.datetime.now(datetime.timezone.utc), # _metadata_load_timestamp
            )
        )
    errors = BQ_CLIENT.insert_rows(
        table_ref,
        rows_to_insert,
        selected_fields=[
            bigquery.SchemaField("dwh_vertrag_id", "INT64"),
            bigquery.SchemaField("gueltig_von", "DATE"),
            bigquery.SchemaField("gueltig_bis", "DATE"),
            bigquery.SchemaField("ladedatum", "DATE"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("_metadata_load_timestamp", "TIMESTAMP"),
        ],
    )
    if errors:
        raise Exception(f"Error inserting rows into dwh_contract_cache: {errors}")
    print(f"Inserted {len(data)} rows into dwh_contract_cache.")

def get_audit_log_entries(run_id):
    """Fetches audit log entries for a given run_id."""
    sql = f"""
        SELECT job_name, status, message, parameters_json, start_time, end_time
        FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log`
        WHERE run_id = '{run_id}'
        ORDER BY start_time
    """
    return list(execute_bq_sql(sql))

def get_fos_contract_data():
    """Fetches all data from the fos_contract_data table."""
    sql = f"""
        SELECT contract_id, product_id, customer_id, stichtag
        FROM `{PROJECT_ID}.{DATASET_ID}.fos_contract_data`
        ORDER BY contract_id, stichtag
    """
    return list(execute_bq_sql(sql))

def get_current_date_ddmmyyyy():
    """Returns current date in DDMMYYYY string format."""
    return datetime.date.today().strftime('%d%m%Y')

def get_current_date_date_obj():
    """Returns current date as a datetime.date object."""
    return datetime.date.today()

# --- Pytest Fixtures ---

@pytest.fixture(autouse=True)
def setup_and_teardown_tables():
    """Fixture to clear tables before and after each test."""
    clear_table("dwh_contract_cache")
    clear_table("fos_contract_data")
    clear_table("job_audit_log")
    yield
    # Optional: clear tables after tests if needed, but autouse handles before
    # clear_table("dwh_contract_cache")
    # clear_table("fos_contract_data")
    # clear_table("job_audit_log")

```

---

### 1. Output Parity & Transformation Correctness

#### Test Case 1.1: Basic Data Extraction - All Filters Applied

*   **Purpose:** Verify the core filtering logic (`gueltig_von`, `gueltig_bis`, `ladedatum`, `dwh_vertrag_id > wiederanlaufWert`) produces the expected output.
*   **Setup:**
    *   Populate `dwh_contract_cache` with a diverse set of records, including some that should match the filters and some that should not.
    *   Define a `p_stichtag` and `p_wiederanlaufWert` that will activate all filter conditions.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with specific `p_stichtag_raw` and `p_wiederanlaufWert_raw`.
*   **Pass/Fail Criterion:** The `fos_contract_data` table contains exactly the expected records, and the row count matches the baseline from the legacy system.

```python
def test_basic_data_extraction_all_filters():
    stichtag_str = "15032023" # March 15, 2023
    stichtag_date = datetime.date(2023, 3, 15)
    wiederanlauf_val = "100"

    # Data for dwh_contract_cache
    # Expected to be selected: C1, C3
    # Expected to be excluded: C2 (ladedatum >= stichtag), C4 (gueltig_bis <= stichtag), C5 (gueltig_von > stichtag), C6 (dwh_vertrag_id <= wiederanlauf_val)
    dwh_data = [
        {"dwh_vertrag_id": 101, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 4, 1), "ladedatum": datetime.date(2023, 3, 1), "product_id": "PROD_A", "customer_id": "CUST_1"}, # C1 - Match
        {"dwh_vertrag_id": 102, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 4, 1), "ladedatum": datetime.date(2023, 3, 15), "product_id": "PROD_B", "customer_id": "CUST_2"}, # C2 - ladedatum >= stichtag
        {"dwh_vertrag_id": 103, "gueltig_von": datetime.date(2023, 2, 1), "gueltig_bis": datetime.date(2023, 5, 1), "ladedatum": datetime.date(2023, 3, 10), "product_id": "PROD_C", "customer_id": "CUST_3"}, # C3 - Match
        {"dwh_vertrag_id": 104, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 3, 15), "ladedatum": datetime.date(2023, 3, 1), "product_id": "PROD_D", "customer_id": "CUST_4"}, # C4 - gueltig_bis <= stichtag
        {"dwh_vertrag_id": 105, "gueltig_von": datetime.date(2023, 3, 16), "gueltig_bis": datetime.date(2023, 5, 1), "ladedatum": datetime.date(2023, 3, 1), "product_id": "PROD_E", "customer_id": "CUST_5"}, # C5 - gueltig_von > stichtag
        {"dwh_vertrag_id": 99,  "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 4, 1), "ladedatum": datetime.date(2023, 3, 1), "product_id": "PROD_F", "customer_id": "CUST_6"}, # C6 - dwh_vertrag_id <= wiederanlauf_val
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": f"'{wiederanlauf_val}'"}
    )

    # Assertions
    result_data = get_fos_contract_data()
    expected_data = [
        bigquery.Row((101, "PROD_A", "CUST_1", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
        bigquery.Row((103, "PROD_C", "CUST_3", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
    ]
    assert len(result_data) == len(expected_data)
    assert sorted(result_data, key=lambda x: x.contract_id) == sorted(expected_data, key=lambda x: x.contract_id)
```

#### Test Case 1.2: `p_stichtag` Default Behavior

*   **Purpose:** Verify `p_stichtag` defaults to `CURRENT_DATE()` when not provided.
*   **Setup:**
    *   Populate `dwh_contract_cache` such that `CURRENT_DATE()` yields specific results.
    *   Ensure some records match `CURRENT_DATE()` and `ladedatum < CURRENT_DATE()`.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with `p_stichtag_raw = NULL` or empty string.
*   **Pass/Fail Criterion:** `fos_contract_data` contains records filtered by `CURRENT_DATE()`, and the `stichtag` column in the output is `CURRENT_DATE()`.

```python
def test_stichtag_default_to_current_date():
    current_date_obj = get_current_date_date_obj()
    yesterday = current_date_obj - datetime.timedelta(days=1)
    tomorrow = current_date_obj + datetime.timedelta(days=1)
    day_after_tomorrow = current_date_obj + datetime.timedelta(days=2)

    # Data for dwh_contract_cache
    # Expected to be selected: C1 (matches current_date)
    # Expected to be excluded: C2 (gueltig_bis <= current_date), C3 (ladedatum >= current_date)
    dwh_data = [
        {"dwh_vertrag_id": 1, "gueltig_von": yesterday, "gueltig_bis": tomorrow, "ladedatum": yesterday, "product_id": "P1", "customer_id": "C1"}, # C1 - Match
        {"dwh_vertrag_id": 2, "gueltig_von": yesterday, "gueltig_bis": current_date_obj, "ladedatum": yesterday, "product_id": "P2", "customer_id": "C2"}, # C2 - gueltig_bis <= current_date
        {"dwh_vertrag_id": 3, "gueltig_von": yesterday, "gueltig_bis": day_after_tomorrow, "ladedatum": current_date_obj, "product_id": "P3", "customer_id": "C3"}, # C3 - ladedatum >= current_date
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action: Call with empty stichtag_raw
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": "''", "p_wiederanlaufWert_raw": "''"} # Empty strings for defaults
    )

    # Assertions
    result_data = get_fos_contract_data()
    expected_data = [
        bigquery.Row((1, "P1", "C1", current_date_obj), ("contract_id", "product_id", "customer_id", "stichtag")),
    ]
    assert len(result_data) == len(expected_data)
    assert sorted(result_data, key=lambda x: x.contract_id) == sorted(expected_data, key=lambda x: x.contract_id)
```

#### Test Case 1.3: `p_wiederanlaufWert` Default Behavior (Migrated: NULL, Legacy: 0)

*   **Purpose:** Verify `p_wiederanlaufWert` defaults to `NULL` (meaning no `dwh_vertrag_id` filter) when not provided in the migrated code. **This highlights the parity discrepancy.**
*   **Setup:**
    *   Populate `dwh_contract_cache` with `dwh_vertrag_id` values including 0, 1, and values greater than 0.
    *   Set `p_stichtag` to ensure all records would otherwise match.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with `p_wiederanlaufWert_raw = NULL` or empty string.
*   **Pass/Fail Criterion:** `fos_contract_data` contains records where `dwh_vertrag_id <= 0` (e.g., `dwh_vertrag_id = 0`), which would have been excluded by the legacy job's `dwh_vertrag_id > 0` filter. This test passes if the migrated behavior (no filter on `dwh_vertrag_id` when `p_wiederanlaufWert` is `NULL`) is observed.

```python
def test_wiederanlaufwert_default_null_behavior():
    stichtag_str = "01012024"
    stichtag_date = datetime.date(2024, 1, 1)
    
    # Data for dwh_contract_cache
    # All records match date filters.
    # Expected to be selected by migrated (no wiederanlaufWert filter): C1, C2, C3
    # Expected to be selected by legacy (wiederanlaufWert=0): C2, C3 (C1 excluded as dwh_vertrag_id <= 0)
    dwh_data = [
        {"dwh_vertrag_id": 0, "gueltig_von": datetime.date(2023, 12, 1), "gueltig_bis": datetime.date(2024, 2, 1), "ladedatum": datetime.date(2023, 12, 15), "product_id": "P_ZERO", "customer_id": "C_ZERO"}, # C1 - dwh_vertrag_id = 0
        {"dwh_vertrag_id": 1, "gueltig_von": datetime.date(2023, 12, 1), "gueltig_bis": datetime.date(2024, 2, 1), "ladedatum": datetime.date(2023, 12, 15), "product_id": "P_ONE", "customer_id": "C_ONE"},   # C2 - dwh_vertrag_id = 1
        {"dwh_vertrag_id": 100, "gueltig_von": datetime.date(2023, 12, 1), "gueltig_bis": datetime.date(2024, 2, 1), "ladedatum": datetime.date(2023, 12, 15), "product_id": "P_HUNDRED", "customer_id": "C_HUNDRED"}, # C3 - dwh_vertrag_id = 100
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action: Call with empty wiederanlaufWert_raw
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": "''"}
    )

    # Assertions
    result_data = get_fos_contract_data()
    expected_data_migrated = [
        bigquery.Row((0, "P_ZERO", "C_ZERO", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
        bigquery.Row((1, "P_ONE", "C_ONE", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
        bigquery.Row((100, "P_HUNDRED", "C_HUNDRED", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
    ]
    
    # This assertion validates the *migrated* behavior, which differs from legacy.
    assert len(result_data) == len(expected_data_migrated)
    assert sorted(result_data, key=lambda x: x.contract_id) == sorted(expected_data_migrated, key=lambda x: x.contract_id)

    print("\n--- IMPORTANT PARITY DISCREPANCY NOTED ---")
    print("Legacy job would filter out dwh_vertrag_id <= 0 when wiederanlaufWert is not provided (defaults to 0).")
    print("Migrated job, as implemented, applies no dwh_vertrag_id filter when wiederanlaufWert is not provided (defaults to NULL).")
    print("This test confirms the migrated behavior. Adjust wrapper SPROC if legacy parity is required.")
```

#### Test Case 1.4: Edge Case - `p_stichtag` at boundary of `gueltig_bis`

*   **Purpose:** Verify strict inequality `p_stichtag < dcc.gueltig_bis` is correctly applied.
*   **Setup:**
    *   Populate `dwh_contract_cache` with a record where `gueltig_bis` is exactly equal to `p_stichtag`.
*   **Action:** Call wrapper SPROC with this `p_stichtag`.
*   **Pass/Fail Criterion:** The record with `gueltig_bis = p_stichtag` is *excluded* from `fos_contract_data`.

```python
def test_stichtag_gueltig_bis_boundary():
    stichtag_str = "01042023" # April 1, 2023
    stichtag_date = datetime.date(2023, 4, 1)

    # Data for dwh_contract_cache
    # Expected to be selected: C1
    # Expected to be excluded: C2 (gueltig_bis == stichtag_date)
    dwh_data = [
        {"dwh_vertrag_id": 1, "gueltig_von": datetime.date(2023, 3, 1), "gueltig_bis": datetime.date(2023, 4, 2), "ladedatum": datetime.date(2023, 3, 15), "product_id": "P1", "customer_id": "C1"}, # C1 - Match
        {"dwh_vertrag_id": 2, "gueltig_von": datetime.date(2023, 3, 1), "gueltig_bis": stichtag_date, "ladedatum": datetime.date(2023, 3, 15), "product_id": "P2", "customer_id": "C2"}, # C2 - gueltig_bis == stichtag_date, should be excluded
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": "''"}
    )

    # Assertions
    result_data = get_fos_contract_data()
    expected_data = [
        bigquery.Row((1, "P1", "C1", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
    ]
    assert len(result_data) == len(expected_data)
    assert sorted(result_data, key=lambda x: x.contract_id) == sorted(expected_data, key=lambda x: x.contract_id)
```

#### Test Case 1.5: Edge Case - No matching data

*   **Purpose:** Verify the job runs successfully and produces no output when no data matches the filters.
*   **Setup:**
    *   Populate `dwh_contract_cache` with data that intentionally does not match the chosen `p_stichtag`.
*   **Action:** Call wrapper SPROC.
*   **Pass/Fail Criterion:** `fos_contract_data` is empty, and the audit log shows a successful run for both wrapper and kernel.

```python
def test_no_matching_data():
    stichtag_str = "01012025" # A future date
    stichtag_date = datetime.date(2025, 1, 1)

    # Data for dwh_contract_cache (all records are old and won't match future stichtag)
    dwh_data = [
        {"dwh_vertrag_id": 1, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 2, 1), "ladedatum": datetime.date(2023, 1, 15), "product_id": "P1", "customer_id": "C1"},
        {"dwh_vertrag_id": 2, "gueltig_von": datetime.date(2023, 3, 1), "gueltig_bis": datetime.date(2023, 4, 1), "ladedatum": datetime.date(2023, 3, 15), "product_id": "P2", "customer_id": "C2"},
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": "''"}
    )

    # Assertions
    result_data = get_fos_contract_data()
    assert len(result_data) == 0, "fos_contract_data should be empty when no data matches filters."

    audit_logs = get_audit_log_entries(BQ_CLIENT.query("SELECT run_id FROM `project.dataset.job_audit_log` WHERE job_name = 'ausd_bp_ta_iccid_einzeln_wrapper'").result().to_dataframe().iloc[0]['run_id'])
    assert len(audit_logs) >= 2 # Wrapper start, kernel start, kernel success, wrapper success, plus INFO logs
    assert all(log.status in ['SUCCEEDED', 'INFO', 'RUNNING'] for log in audit_logs)
    assert any(log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'SUCCEEDED' for log in audit_logs)
    assert any(log.job_name == 'k_ausd_bp_ta_iccid_einzeln' and log.status == 'SUCCEEDED' for log in audit_logs)
```

#### Test Case 1.6: Duplicate Handling (Appending Behavior)

*   **Purpose:** Verify the current appending behavior of the migrated job when run multiple times with the same inputs. **This highlights the parity discrepancy with legacy's implied conditional overwrite.**
*   **Setup:**
    *   Populate `dwh_contract_cache` with data.
    *   Run the wrapper SPROC once.
*   **Action:** Run the wrapper SPROC a second time with the *exact same parameters*.
*   **Pass/Fail Criterion:** `fos_contract_data` contains duplicate records (i.e., the count is double the expected single-run count). This test passes if the current appending behavior is observed.

```python
def test_duplicate_handling_appending_behavior():
    stichtag_str = "01022023"
    stichtag_date = datetime.date(2023, 2, 1)

    dwh_data = [
        {"dwh_vertrag_id": 1, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 3, 1), "ladedatum": datetime.date(2023, 1, 15), "product_id": "P1", "customer_id": "C1"},
        {"dwh_vertrag_id": 2, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 3, 1), "ladedatum": datetime.date(2023, 1, 15), "product_id": "P2", "customer_id": "C2"},
    ]
    insert_dwh_contract_cache(dwh_data)

    # First run
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": "''"}
    )
    first_run_data = get_fos_contract_data()
    assert len(first_run_data) == 2

    # Second run with same parameters
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": "''"}
    )

    # Assertions
    final_data = get_fos_contract_data()
    assert len(final_data) == 4, "Expected 4 records (2 duplicates) due to appending behavior."
    
    print("\n--- IMPORTANT PARITY DISCREPANCY NOTED ---")
    print("The migrated job currently appends data, leading to duplicates if run multiple times with the same inputs.")
    print("Legacy job description implies conditional overwrite/truncation. This test confirms the current appending behavior.")
    print("Consider implementing a MERGE statement or conditional TRUNCATE in k_ausd_bp_ta_iccid_einzeln for idempotency/parity.")
```

---

### 2. Parameter Handling & Error Cases

#### Test Case 2.1: Invalid `p_stichtag_raw` Format

*   **Purpose:** Verify robust error handling for malformed `stichtag` input.
*   **Setup:** None.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with an invalid `p_stichtag_raw` format (e.g., `YYYY-MM-DD`).
*   **Pass/Fail Criterion:** The wrapper SPROC fails, and `job_audit_log` records a `FAILED` status for the wrapper with an informative error message about the invalid date format. No kernel SPROC entry should be present.

```python
def test_invalid_stichtag_format():
    invalid_stichtag = "2023-01-01" # Expected DDMMYYYY

    # Action
    with pytest.raises(Exception) as excinfo:
        call_sproc(
            "ausd_bp_ta_iccid_einzeln_wrapper",
            {"p_stichtag_raw": f"'{invalid_stichtag}'", "p_wiederanlaufWert_raw": "''"}
        )
    
    # Assertions
    assert "Invalid Stichtag format" in str(excinfo.value)

    # Verify audit log
    audit_logs = get_audit_log_entries(BQ_CLIENT.query("SELECT run_id FROM `project.dataset.job_audit_log` WHERE job_name = 'ausd_bp_ta_iccid_einzeln_wrapper'").result().to_dataframe().iloc[0]['run_id'])
    assert any(log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'FAILED' and "Invalid Stichtag format" in log.message for log in audit_logs)
    assert not any(log.job_name == 'k_ausd_bp_ta_iccid_einzeln' for log in audit_logs) # Kernel should not be called
```

#### Test Case 2.2: Invalid `p_wiederanlaufWert_raw` Format

*   **Purpose:** Verify robust error handling for malformed `wiederanlaufWert` input.
*   **Setup:** None.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with an invalid `p_wiederanlaufWert_raw` format (e.g., non-numeric string).
*   **Pass/Fail Criterion:** The wrapper SPROC fails, and `job_audit_log` records a `FAILED` status for the wrapper with an informative error message about the invalid integer format. No kernel SPROC entry should be present.

```python
def test_invalid_wiederanlaufwert_format():
    stichtag_str = "01012023"
    invalid_wiederanlaufwert = "abc"

    # Action
    with pytest.raises(Exception) as excinfo:
        call_sproc(
            "ausd_bp_ta_iccid_einzeln_wrapper",
            {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": f"'{invalid_wiederanlaufwert}'"}
        )
    
    # Assertions
    assert "Invalid Wiederanlaufwert format" in str(excinfo.value)

    # Verify audit log
    audit_logs = get_audit_log_entries(BQ_CLIENT.query("SELECT run_id FROM `project.dataset.job_audit_log` WHERE job_name = 'ausd_bp_ta_iccid_einzeln_wrapper'").result().to_dataframe().iloc[0]['run_id'])
    assert any(log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'FAILED' and "Invalid Wiederanlaufwert format" in log.message for log in audit_logs)
    assert not any(log.job_name == 'k_ausd_bp_ta_iccid_einzeln' for log in audit_logs) # Kernel should not be called
```

#### Test Case 2.3: `p_wiederanlaufWert` with 0

*   **Purpose:** Verify that explicitly providing `p_wiederanlaufWert = 0` correctly applies the filter `dwh_vertrag_id > 0`.
*   **Setup:**
    *   Populate `dwh_contract_cache` with records including `dwh_vertrag_id = 0` and `dwh_vertrag_id > 0`.
*   **Action:** Call wrapper SPROC with `p_wiederanlaufWert_raw = '0'`.
*   **Pass/Fail Criterion:** `fos_contract_data` contains only records where `dwh_vertrag_id > 0`.

```python
def test_wiederanlaufwert_explicit_zero():
    stichtag_str = "01012024"
    stichtag_date = datetime.date(2024, 1, 1)
    wiederanlauf_val = "0"

    # Data for dwh_contract_cache
    # Expected to be selected: C2, C3 (dwh_vertrag_id > 0)
    # Expected to be excluded: C1 (dwh_vertrag_id = 0)
    dwh_data = [
        {"dwh_vertrag_id": 0, "gueltig_von": datetime.date(2023, 12, 1), "gueltig_bis": datetime.date(2024, 2, 1), "ladedatum": datetime.date(2023, 12, 15), "product_id": "P_ZERO", "customer_id": "C_ZERO"},
        {"dwh_vertrag_id": 1, "gueltig_von": datetime.date(2023, 12, 1), "gueltig_bis": datetime.date(2024, 2, 1), "ladedatum": datetime.date(2023, 12, 15), "product_id": "P_ONE", "customer_id": "C_ONE"},
        {"dwh_vertrag_id": 100, "gueltig_von": datetime.date(2023, 12, 1), "gueltig_bis": datetime.date(2024, 2, 1), "ladedatum": datetime.date(2023, 12, 15), "product_id": "P_HUNDRED", "customer_id": "C_HUNDRED"},
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": f"'{wiederanlauf_val}'"}
    )

    # Assertions
    result_data = get_fos_contract_data()
    expected_data = [
        bigquery.Row((1, "P_ONE", "C_ONE", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
        bigquery.Row((100, "P_HUNDRED", "C_HUNDRED", stichtag_date), ("contract_id", "product_id", "customer_id", "stichtag")),
    ]
    assert len(result_data) == len(expected_data)
    assert sorted(result_data, key=lambda x: x.contract_id) == sorted(expected_data, key=lambda x: x.contract_id)
```

---

### 3. External-System Replacements (Audit Logging)

#### Test Case 3.1: Audit Log - Successful Run

*   **Purpose:** Verify `job_audit_log` correctly records start, end, and success status for both wrapper and kernel SPROCs.
*   **Setup:**
    *   Populate `dwh_contract_cache` with some data that will be processed.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with valid parameters.
*   **Pass/Fail Criterion:** `job_audit_log` contains at least four entries for the same `run_id`:
    1.  `ausd_bp_ta_iccid_einzeln_wrapper` with `RUNNING` status.
    2.  `k_ausd_bp_ta_iccid_einzeln` with `RUNNING` status.
    3.  `k_ausd_bp_ta_iccid_einzeln` with `SUCCEEDED` status.
    4.  `ausd_bp_ta_iccid_einzeln_wrapper` with `SUCCEEDED` status.
    All entries should have correct `start_time`/`end_time` and relevant messages.

```python
def test_audit_log_successful_run():
    stichtag_str = "01012023"
    dwh_data = [
        {"dwh_vertrag_id": 1, "gueltig_von": datetime.date(2022, 12, 1), "gueltig_bis": datetime.date(2023, 2, 1), "ladedatum": datetime.date(2022, 12, 15), "product_id": "P1", "customer_id": "C1"},
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": "''"}
    )

    # Assertions: Retrieve the run_id from the first wrapper entry
    wrapper_start_log = execute_bq_sql(f"SELECT run_id FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` WHERE job_name = 'ausd_bp_ta_iccid_einzeln_wrapper' AND status = 'RUNNING' ORDER BY start_time LIMIT 1")
    assert len(wrapper_start_log) == 1
    run_id = wrapper_start_log[0].run_id

    audit_logs = get_audit_log_entries(run_id)
    
    # Check for wrapper start/success
    wrapper_start = next((log for log in audit_logs if log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'RUNNING'), None)
    wrapper_success = next((log for log in audit_logs if log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'SUCCEEDED'), None)
    assert wrapper_start is not None
    assert wrapper_success is not None
    assert wrapper_success.end_time > wrapper_start.start_time
    assert "Starting wrapper stored procedure." in wrapper_start.message
    assert "Wrapper stored procedure completed successfully." in wrapper_success.message

    # Check for kernel start/success
    kernel_start = next((log for log in audit_logs if log.job_name == 'k_ausd_bp_ta_iccid_einzeln' and log.status == 'RUNNING'), None)
    kernel_success = next((log for log in audit_logs if log.job_name == 'k_ausd_bp_ta_iccid_einzeln' and log.status == 'SUCCEEDED'), None)
    assert kernel_start is not None
    assert kernel_success is not None
    assert kernel_success.end_time > kernel_start.start_time
    assert "Starting core logic" in kernel_start.message
    assert "Successfully extracted 1 records for FOS." in kernel_success.message # Based on dwh_data
    
    # Ensure correct order (wrapper starts, kernel runs, kernel succeeds, wrapper succeeds)
    assert wrapper_start.start_time < kernel_start.start_time
    assert kernel_start.start_time < kernel_success.end_time
    assert kernel_success.end_time < wrapper_success.end_time
```

#### Test Case 3.2: Audit Log - Failed Run (Wrapper)

*   **Purpose:** Verify `job_audit_log` correctly records failure in the wrapper SPROC.
*   **Setup:** None.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with an invalid `p_stichtag_raw` (triggering wrapper failure).
*   **Pass/Fail Criterion:** `job_audit_log` contains one entry for the wrapper SPROC with `FAILED` status and an error message. No entries for the kernel SPROC should exist.

```python
def test_audit_log_failed_run_wrapper():
    invalid_stichtag = "2023/01/01"

    # Action
    with pytest.raises(Exception): # Expecting the wrapper to raise an error
        call_sproc(
            "ausd_bp_ta_iccid_einzeln_wrapper",
            {"p_stichtag_raw": f"'{invalid_stichtag}'", "p_wiederanlaufWert_raw": "''"}
        )
    
    # Assertions: Retrieve the run_id from the first wrapper entry
    wrapper_start_log = execute_bq_sql(f"SELECT run_id FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` WHERE job_name = 'ausd_bp_ta_iccid_einzeln_wrapper' AND status = 'RUNNING' ORDER BY start_time LIMIT 1")
    assert len(wrapper_start_log) == 1
    run_id = wrapper_start_log[0].run_id

    audit_logs = get_audit_log_entries(run_id)

    wrapper_failed = next((log for log in audit_logs if log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'FAILED'), None)
    assert wrapper_failed is not None
    assert "Invalid Stichtag format" in wrapper_failed.message
    
    # Ensure no kernel logs
    assert not any(log.job_name == 'k_ausd_bp_ta_iccid_einzeln' for log in audit_logs)
```

#### Test Case 3.3: Audit Log - Failed Run (Kernel)

*   **Purpose:** Verify `job_audit_log` correctly records failure in the kernel SPROC and propagates it to the wrapper.
*   **Setup:**
    *   **Manual Intervention/Mocking:** To simulate a kernel failure, one would typically need to temporarily modify the `k_ausd_bp_ta_iccid_einzeln` SPROC to intentionally cause an error (e.g., `SELECT ERROR('Simulated kernel error');` or attempt to insert data with a type mismatch). For this test, we'll assume such a mechanism exists or that the test environment allows for temporary SPROC modification.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with valid parameters, triggering the (simulated) kernel failure.
*   **Pass/Fail Criterion:** `job_audit_log` contains entries for both wrapper and kernel. The kernel entry should be `FAILED`, and the wrapper entry should also be `FAILED` (due to the `RAISE` in the kernel).

```python
# NOTE: This test requires a mechanism to *force* the kernel SPROC to fail.
# In a real scenario, you might temporarily deploy a modified k_ausd_bp_ta_iccid_einzeln
# that includes a `SELECT ERROR('Simulated failure');` statement.
# For demonstration, we'll assume the kernel SPROC has been modified to fail.

# Example of how you might temporarily modify the kernel SPROC for testing:
# CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_iccid_einzeln`(
#     p_stichtag DATE, p_wiederanlaufWert INT64, p_wrapper_run_id STRING
# )
# BEGIN
#     -- ... existing code ...
#     IF p_stichtag = DATE('2023-01-01') THEN -- Trigger condition for test
#         SELECT ERROR('Simulated kernel failure for test_audit_log_failed_run_kernel');
#     END IF;
#     -- ... rest of the existing code ...
# END;

def test_audit_log_failed_run_kernel():
    stichtag_str = "01012023" # This date will trigger the simulated kernel failure
    dwh_data = [
        {"dwh_vertrag_id": 1, "gueltig_von": datetime.date(2022, 12, 1), "gueltig_bis": datetime.date(2023, 2, 1), "ladedatum": datetime.date(2022, 12, 15), "product_id": "P1", "customer_id": "C1"},
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    with pytest.raises(Exception): # Expecting the wrapper (and thus the call) to raise an error
        call_sproc(
            "ausd_bp_ta_iccid_einzeln_wrapper",
            {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": "''"}
        )
    
    # Assertions: Retrieve the run_id from the first wrapper entry
    wrapper_start_log = execute_bq_sql(f"SELECT run_id FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` WHERE job_name = 'ausd_bp_ta_iccid_einzeln_wrapper' AND status = 'RUNNING' ORDER BY start_time LIMIT 1")
    assert len(wrapper_start_log) == 1
    run_id = wrapper_start_log[0].run_id

    audit_logs = get_audit_log_entries(run_id)

    # Check for kernel failure
    kernel_failed = next((log for log in audit_logs if log.job_name == 'k_ausd_bp_ta_iccid_einzeln' and log.status == 'FAILED'), None)
    assert kernel_failed is not None
    assert "Kernel stored procedure failed with error" in kernel_failed.message
    # If using the simulated error, check for its specific message
    # assert "Simulated kernel failure" in kernel_failed.message

    # Check for wrapper failure (due to kernel raising error)
    wrapper_failed = next((log for log in audit_logs if log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'FAILED'), None)
    assert wrapper_failed is not None
    assert "Wrapper stored procedure failed with error" in wrapper_failed.message
    
    # Ensure correct sequence of events
    wrapper_start = next((log for log in audit_logs if log.job_name == 'ausd_bp_ta_iccid_einzeln_wrapper' and log.status == 'RUNNING'), None)
    kernel_start = next((log for log in audit_logs if log.job_name == 'k_ausd_bp_ta_iccid_einzeln' and log.status == 'RUNNING'), None)
    assert wrapper_start.start_time < kernel_start.start_time
    assert kernel_start.start_time < kernel_failed.end_time
    assert kernel_failed.end_time < wrapper_failed.end_time
```

---

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Row Count Parity

*   **Purpose:** Verify the number of records processed matches the legacy system for a given input.
*   **Setup:**
    *   Populate `dwh_contract_cache` with a known dataset.
    *   Obtain a baseline row count from the legacy system for this dataset and specific parameters.
*   **Action:** Call `ausd_bp_ta_iccid_einzeln_wrapper` with the same parameters used for the baseline.
*   **Pass/Fail Criterion:** `SELECT COUNT(*) FROM project.dataset.fos_contract_data` matches the baseline count.

```python
def test_row_count_parity():
    stichtag_str = "10032023"
    stichtag_date = datetime.date(2023, 3, 10)
    wiederanlauf_val = "50"

    # Data for dwh_contract_cache
    # Expected to be selected: C1, C2, C4 (3 records)
    dwh_data = [
        {"dwh_vertrag_id": 51, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 4, 1), "ladedatum": datetime.date(2023, 3, 1), "product_id": "P1", "customer_id": "C1"},
        {"dwh_vertrag_id": 52, "gueltig_von": datetime.date(2023, 2, 1), "gueltig_bis": datetime.date(2023, 5, 1), "ladedatum": datetime.date(2023, 3, 5), "product_id": "P2", "customer_id": "C2"},
        {"dwh_vertrag_id": 49, "gueltig_von": datetime.date(2023, 1, 1), "gueltig_bis": datetime.date(2023, 4, 1), "ladedatum": datetime.date(2023, 3, 1), "product_id": "P3", "customer_id": "C3"}, # Excluded by wiederanlauf_val
        {"dwh_vertrag_id": 100, "gueltig_von": datetime.date(2023, 3, 1), "gueltig_bis": datetime.date(2023, 3, 11), "ladedatum": datetime.date(2023, 3, 9), "product_id": "P4", "customer_id": "C4"}, # Excluded by gueltig_bis
        {"dwh_vertrag_id": 100, "gueltig_von": datetime.date(2023, 3, 1), "gueltig_bis": datetime.date(2023, 3, 15), "ladedatum": datetime.date(2023, 3, 9), "product_id": "P4", "customer_id": "C4_match"}, # Match
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": f"'{wiederanlauf_val}'"}
    )

    # Assertions
    result_data = get_fos_contract_data()
    expected_count = 3 # Based on the dwh_data and filters
    assert len(result_data) == expected_count, f"Expected {expected_count} rows, but got {len(result_data)}."
    
    # For full parity, you would compare this count against a known output from the legacy system.
    # For example: assert len(result_data) == legacy_baseline_row_count
```

#### Test Case 4.2: Schema Conformance

*   **Purpose:** Verify the output table `fos_contract_data` conforms to the expected schema (data types, nullability).
*   **Setup:** None (schema is defined by DDL).
*   **Action:** Inspect `fos_contract_data` schema using BigQuery API.
*   **Pass/Fail Criterion:** The schema matches the DDL definition, specifically for critical columns like `contract_id` (INT64 NOT NULL), `stichtag` (DATE NOT NULL), etc.

```python
def test_schema_conformance():
    table_ref = BQ_CLIENT.get_table(f"{PROJECT_ID}.{DATASET_ID}.fos_contract_data")
    schema = table_ref.schema

    # Expected schema definition based on ddl/fos_contract_data.sql
    expected_schema = {
        "contract_id": {"field_type": "INT64", "mode": "REQUIRED"},
        "product_id": {"field_type": "STRING", "mode": "NULLABLE"},
        "customer_id": {"field_type": "STRING", "mode": "NULLABLE"},
        "stichtag": {"field_type": "DATE", "mode": "REQUIRED"},
        "load_timestamp": {"field_type": "TIMESTAMP", "mode": "REQUIRED"},
    }

    actual_schema = {field.name: {"field_type": field.field_type, "mode": field.mode} for field in schema}

    for field_name, expected_props in expected_schema.items():
        assert field_name in actual_schema, f"Field {field_name} missing from actual schema."
        actual_props = actual_schema[field_name]
        assert actual_props["field_type"] == expected_props["field_type"], \
            f"Field {field_name}: Expected type {expected_props['field_type']}, got {actual_props['field_type']}."
        assert actual_props["mode"] == expected_props["mode"], \
            f"Field {field_name}: Expected mode {expected_props['mode']}, got {actual_props['mode']}."
```

#### Test Case 4.3: Data Type Correctness

*   **Purpose:** Verify data types are correctly handled during transformation (e.g., `DATE` parsing, `INT64` casting).
*   **Setup:**
    *   Populate `dwh_contract_cache` with valid data for all relevant columns.
*   **Action:** Call wrapper SPROC.
*   **Pass/Fail Criterion:** `SELECT * FROM project.dataset.fos_contract_data` shows values correctly cast and stored in their respective columns, matching the input types.

```python
def test_data_type_correctness():
    stichtag_str = "01012023"
    stichtag_date = datetime.date(2023, 1, 1)
    wiederanlauf_val = "0"

    dwh_data = [
        {"dwh_vertrag_id": 123456789012345, # Large INT64
         "gueltig_von": datetime.date(2022, 1, 1),
         "gueltig_bis": datetime.date(2024, 1, 1),
         "ladedatum": datetime.date(2022, 12, 15),
         "product_id": "FAX_SERVICE",
         "customer_id": "CUST_XYZ_123"},
    ]
    insert_dwh_contract_cache(dwh_data)

    # Action
    call_sproc(
        "ausd_bp_ta_iccid_einzeln_wrapper",
        {"p_stichtag_raw": f"'{stichtag_str}'", "p_wiederanlaufWert_raw": f"'{wiederanlauf_val}'"}
    )

    # Assertions
    result_data = get_fos_contract_data()
    assert len(result_data) == 1
    
    record = result_data[0]
    assert isinstance(record.contract_id, int)
    assert record.contract_id == 123456789012345
    assert isinstance(record.product_id, str)
    assert record.product_id == "FAX_SERVICE"
    assert isinstance(record.customer_id, str)
    assert record.customer_id == "CUST_XYZ_123"
    assert isinstance(record.stichtag, datetime.date)
    assert record.stichtag == stichtag_date
    # load_timestamp is generated by BQ, so just check its type
    assert isinstance(record.load_timestamp, datetime.datetime)
```