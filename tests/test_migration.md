The following migration validation tests are designed to ensure that the BigQuery implementation of `r_ausd_bp_ta_cntrct_evn.ksh` is behaviourally equivalent to the legacy KornShell script. These tests cover parameter handling, logging, core data transformation, and interaction with external systems (represented by BigQuery tables).

**Important Note on `k_ausd_bp_ta_cntrct_evn.ksh` Logic:**
The migration design document explicitly states that the core logic within `k_ausd_bp_ta_cntrct_evn.ksh` was not available and the `ausd_bp_ta_cntrct_evn_core` procedure is a placeholder. The tests for core logic transformation (Test Cases 4, 5, 10) are based on the *assumed* logic derived from the `usage` section of the original script and the example SQL provided in the `ausd_bp_ta_cntrct_evn_core` DDL. **These tests will only be fully valid once the actual `k_ausd_bp_ta_cntrct_evn.ksh` logic has been thoroughly analyzed and accurately translated into the `ausd_bp_ta_cntrct_evn_core` BigQuery stored procedure.** Specifically, the `DELETE` logic for `p_wiederanlaufWert` mentioned in the original script's `usage` section has been incorporated into the `ausd_bp_ta_cntrct_evn_core` for Test Case 10, as it was missing in the provided generated code.

---

### General Setup for All Tests

Before running any tests, ensure the following:

1.  **BigQuery Environment**: A BigQuery project and dataset (`project.dataset`) are configured.
2.  **Table DDLs Deployed**: The DDLs for `job_log`, `job_control`, `dwh_ta_c_vertrag_source`, and `fos_target_table` are executed in BigQuery.
3.  **Stored Procedures Deployed**: The `ausd_bp_ta_cntrct_evn_wrapper` and `ausd_bp_ta_cntrct_evn_core` stored procedures are deployed in BigQuery.
4.  **Pytest Environment**: A Python environment with `pytest` and `google-cloud-bigquery` installed.
5.  **BigQuery Client**: A `google.cloud.bigquery.Client` instance is available for executing queries and procedure calls.

```python
# conftest.py or test_utils.py (example setup)
import pytest
from google.cloud import bigquery
import datetime
import decimal

PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    yield client
    client.close()

@pytest.fixture(autouse=True)
def setup_and_teardown_tables(bq_client):
    # Clear tables before each test
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.dwh_ta_c_vertrag_source`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.fos_target_table`").result()
    yield
    # Optional: Clear tables after each test if not using autouse=True for setup_and_teardown_tables
    # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
    # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.dwh_ta_c_vertrag_source`").result()
    # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.fos_target_table`").result()

def call_wrapper_procedure(bq_client, stichtag_str, wiederanlaufWert_input):
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`(@stichtag_str, @wiederanlaufWert_input)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("stichtag_str", "STRING", stichtag_str),
            bigquery.ScalarQueryParameter("wiederanlaufWert_input", "INT64", wiederanlaufWert_input),
        ]
    )
    return bq_client.query(query, job_config=job_config).result()

def get_table_data(bq_client, table_name, order_by=None):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
    if order_by:
        query += f" ORDER BY {order_by}"
    return list(bq_client.query(query).result())

def insert_dwh_data(bq_client, data):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.dwh_ta_c_vertrag_source"
    errors = bq_client.insert_rows_json(table_id, data)
    if errors:
        raise Exception(f"Errors inserting DWH data: {errors}")

```

---

### Test Case 1: Default Parameter Handling - No Input Parameters

*   **Purpose**: Verify that when no `stichtag` or `wiederanlaufWert` is provided, the wrapper correctly defaults `stichtag` to `CURRENT_DATE()` and `wiederanlaufWert` to `0`, and the job executes successfully.
*   **Setup**:
    1.  Ensure `job_log`, `job_control`, `dwh_ta_c_vertrag_source`, `fos_target_table` are empty (handled by `setup_and_teardown_tables` fixture).
    2.  Populate `dwh_ta_c_vertrag_source` with sample data where some records match `CURRENT_DATE()` and `DWH_VERTRAG_ID > 0`.
*   **Action**: Call the wrapper procedure without any parameters (passing `NULL` for both).
*   **Pass/Fail Criterion**:
    1.  The call completes successfully without error.
    2.  Verify `job_control` table contains one record for `job_kennung = 'ausd_bp_ta_cntrct_evn'` with `status = 'OK'`.
    3.  Verify `job_log` table contains entries for job start, core processing success, and job completion, all with `log_level = 'INFO'`.
    4.  The `stichtag` in `job_log` for the run matches `CURRENT_DATE()`.
    5.  The `restart_value` in `job_log` for the run is `0`.
    6.  `fos_target_table` contains the expected records, filtered by `CURRENT_DATE()` and `DWH_VERTRAG_ID > 0`.

```python
# test_migration.py
def test_default_parameters(bq_client):
    today = datetime.date.today()
    dwh_data = [
        {"DWH_VERTRAG_ID": 101, "Gueltig_von": today.strftime('%Y-%m-%d'), "Gueltig_bis": (today + datetime.timedelta(days=365)).strftime('%Y-%m-%d'), "LADEDATUM": (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d'), "VERTRAGSNUMMER": "V101", "PRODUKT_TYP": "PROD_A", "BETRAG": 100.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 102, "Gueltig_von": (today - datetime.timedelta(days=30)).strftime('%Y-%m-%d'), "Gueltig_bis": (today + datetime.timedelta(days=180)).strftime('%Y-%m-%d'), "LADEDATUM": (today - datetime.timedelta(days=2)).strftime('%Y-%m-%d'), "VERTRAGSNUMMER": "V102", "PRODUKT_TYP": "PROD_B", "BETRAG": 200.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 103, "Gueltig_von": (today - datetime.timedelta(days=365)).strftime('%Y-%m-%d'), "Gueltig_bis": today.strftime('%Y-%m-%d'), "LADEDATUM": (today - datetime.timedelta(days=3)).strftime('%Y-%m-%d'), "VERTRAGSNUMMER": "V103", "PRODUKT_TYP": "PROD_C", "BETRAG": 300.00, "WAEHRUNG": "EUR"}, # Gueltig_bis = Stichtag, should NOT be included
        {"DWH_VERTRAG_ID": 104, "Gueltig_von": (today + datetime.timedelta(days=1)).strftime('%Y-%m-%d'), "Gueltig_bis": (today + datetime.timedelta(days=365)).strftime('%Y-%m-%d'), "LADEDATUM": (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d'), "VERTRAGSNUMMER": "V104", "PRODUKT_TYP": "PROD_D", "BETRAG": 400.00, "WAEHRUNG": "EUR"}, # Gueltig_von > Stichtag, should NOT be included
        {"DWH_VERTRAG_ID": 105, "Gueltig_von": today.strftime('%Y-%m-%d'), "Gueltig_bis": (today + datetime.timedelta(days=365)).strftime('%Y-%m-%d'), "LADEDATUM": today.strftime('%Y-%m-%d'), "VERTRAGSNUMMER": "V105", "PRODUKT_TYP": "PROD_E", "BETRAG": 500.00, "WAEHRUNG": "EUR"}, # LADEDATUM = Stichtag, should be included based on <=
    ]
    insert_dwh_data(bq_client, dwh_data)

    call_wrapper_procedure(bq_client, None, None)

    # Assertions
    job_control_records = get_table_data(bq_client, "job_control")
    assert len(job_control_records) == 1
    assert job_control_records[0]["status"] == "OK"

    job_log_records = get_table_data(bq_client, "job_log", order_by="log_ts")
    assert len(job_log_records) >= 3 # Start, Core Success, End
    assert all(r["log_level"] == "INFO" for r in job_log_records)
    assert job_log_records[0]["stichtag"] == today
    assert job_log_records[0]["restart_value"] == 0

    fos_target_records = get_table_data(bq_client, "fos_target_table", order_by="DWH_VERTRAG_ID")
    assert len(fos_target_records) == 3
    assert [r["DWH_VERTRAG_ID"] for r in fos_target_records] == [101, 102, 105]
    assert all(r["STICH_TAG"] == today for r in fos_target_records)
```

---

### Test Case 2: Explicit Stichtag and Wiederanlaufwert

*   **Purpose**: Verify that the wrapper correctly uses explicitly provided `stichtag` and `wiederanlaufWert` parameters.
*   **Setup**:
    1.  Clear tables.
    2.  Populate `dwh_ta_c_vertrag_source` with diverse data, including records that would match a specific `stichtag` and `wiederanlaufWert`.
*   **Action**: Call the wrapper procedure with `p_stichtag_str = '15012023'` and `p_wiederanlaufWert_input = 50`.
*   **Pass/Fail Criterion**:
    1.  The call completes successfully.
    2.  `job_control` has one `OK` record.
    3.  `job_log` shows `stichtag = '2023-01-15'` and `restart_value = 50`.
    4.  `fos_target_table` contains records for `DWH_VERTRAG_ID` 100, 150, 250, and `STICH_TAG = '2023-01-15'`.

```python
# test_migration.py
def test_explicit_parameters(bq_client):
    stichtag = datetime.date(2023, 1, 15)
    stichtag_str = "15012023"
    wiederanlauf_value = 50

    dwh_data = [
        {"DWH_VERTRAG_ID": 100, "Gueltig_von": "2023-01-01", "Gueltig_bis": "2023-02-01", "LADEDATUM": "2023-01-10", "VERTRAGSNUMMER": "V100", "PRODUKT_TYP": "PROD_X", "BETRAG": 100.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 150, "Gueltig_von": "2023-01-15", "Gueltig_bis": "2023-03-01", "LADEDATUM": "2023-01-14", "VERTRAGSNUMMER": "V150", "PRODUKT_TYP": "PROD_Y", "BETRAG": 150.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 200, "Gueltig_von": "2023-01-10", "Gueltig_bis": "2023-01-15", "LADEDATUM": "2023-01-12", "VERTRAGSNUMMER": "V200", "PRODUKT_TYP": "PROD_Z", "BETRAG": 200.00, "WAEHRUNG": "EUR"}, # Gueltig_bis = Stichtag, should NOT be included
        {"DWH_VERTRAG_ID": 250, "Gueltig_von": "2023-01-01", "Gueltig_bis": "2023-02-01", "LADEDATUM": "2023-01-15", "VERTRAGSNUMMER": "V250", "PRODUKT_TYP": "PROD_A", "BETRAG": 250.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 300, "Gueltig_von": "2022-12-01", "Gueltig_bis": "2023-01-14", "LADEDATUM": "2023-01-05", "VERTRAGSNUMMER": "V300", "PRODUKT_TYP": "PROD_B", "BETRAG": 300.00, "WAEHRUNG": "EUR"}, # Gueltig_bis < Stichtag, should NOT be included
        {"DWH_VERTRAG_ID": 40, "Gueltig_von": "2023-01-01", "Gueltig_bis": "2023-02-01", "LADEDATUM": "2023-01-10", "VERTRAGSNUMMER": "V040", "PRODUKT_TYP": "PROD_C", "BETRAG": 40.00, "WAEHRUNG": "EUR"}, # DWH_VERTRAG_ID <= 50, should NOT be included
    ]
    insert_dwh_data(bq_client, dwh_data)

    call_wrapper_procedure(bq_client, stichtag_str, wiederanlauf_value)

    # Assertions
    job_control_records = get_table_data(bq_client, "job_control")
    assert len(job_control_records) == 1
    assert job_control_records[0]["status"] == "OK"

    job_log_records = get_table_data(bq_client, "job_log", order_by="log_ts")
    assert len(job_log_records) >= 3
    assert all(r["log_level"] == "INFO" for r in job_log_records)
    assert job_log_records[0]["stichtag"] == stichtag
    assert job_log_records[0]["restart_value"] == wiederanlauf_value

    fos_target_records = get_table_data(bq_client, "fos_target_table", order_by="DWH_VERTRAG_ID")
    assert len(fos_target_records) == 3
    assert [r["DWH_VERTRAG_ID"] for r in fos_target_records] == [100, 150, 250]
    assert all(r["STICH_TAG"] == stichtag for r in fos_target_records)
```

---

### Test Case 3: Invalid Stichtag Format

*   **Purpose**: Verify that the wrapper correctly handles an invalid `stichtag` format, logs an error, and fails gracefully.
*   **Setup**: Clear tables.
*   **Action**: Call the wrapper procedure with `p_stichtag_str = '2023-01-15'` (invalid format, expects DDMMYYYY) and `p_wiederanlaufWert_input = NULL`.
*   **Pass/Fail Criterion**:
    1.  The call fails with an error (e.g., `invalid date format` from `PARSE_DATE` or the custom `SIGNAL SQLSTATE '45000'` if `v_stichtag` becomes NULL).
    2.  `job_control` table contains one record for the job with `status = 'ERROR'`.
    3.  `job_log` table contains an `ERROR` entry indicating the invalid `stichtag` or parsing failure. The `error_message` column should contain relevant details.

```python
# test_migration.py
def test_invalid_stichtag_format(bq_client):
    invalid_stichtag_str = "2023-01-15" # Expected DDMMYYYY

    with pytest.raises(Exception) as excinfo:
        call_wrapper_procedure(bq_client, invalid_stichtag_str, None)
    
    # Check for specific error messages from BigQuery or custom SIGNAL
    assert "invalid date format" in str(excinfo.value).lower() or "45000" in str(excinfo.value)

    # Assertions for logging and control tables
    job_control_records = get_table_data(bq_client, "job_control")
    assert len(job_control_records) == 1
    assert job_control_records[0]["status"] == "ERROR"

    job_log_records = get_table_data(bq_client, "job_log")
    assert any(r["log_level"] == "ERROR" for r in job_log_records)
    error_log = next(r for r in job_log_records if r["log_level"] == "ERROR")
    assert "invalid date format" in error_log["message"].lower() or "required parameter stichtag is missing or invalid" in error_log["message"].lower()
    assert error_log["error_message"] is not None
```

---

### Test Case 4: Core Logic - No Matching Records

*   **Purpose**: Verify that the job runs successfully even if the core logic finds no records matching the criteria, and the target table remains empty.
*   **Setup**:
    1.  Clear tables.
    2.  Populate `dwh_ta_c_vertrag_source` with data that *does not* match the intended `stichtag` and `wiederanlaufWert` filters.
*   **Action**: Call the wrapper procedure with `p_stichtag_str = '15012023'` and `p_wiederanlaufWert_input = 50`.
*   **Pass/Fail Criterion**:
    1.  The call completes successfully.
    2.  `job_control` has one `OK` record.
    3.  `job_log` shows `stichtag = '2023-01-15'` and `restart_value = 50`, and all log entries are `INFO`.
    4.  `fos_target_table` is empty.

```python
# test_migration.py
def test_no_matching_records(bq_client):
    stichtag = datetime.date(2023, 1, 15)
    stichtag_str = "15012023"
    wiederanlauf_value = 50

    dwh_data = [
        {"DWH_VERTRAG_ID": 10, "Gueltig_von": "2023-01-01", "Gueltig_bis": "2023-02-01", "LADEDATUM": "2023-01-10", "VERTRAGSNUMMER": "V010", "PRODUKT_TYP": "PROD_X", "BETRAG": 100.00, "WAEHRUNG": "EUR"}, # DWH_VERTRAG_ID <= 50
        {"DWH_VERTRAG_ID": 20, "Gueltig_von": "2023-01-15", "Gueltig_bis": "2023-03-01", "LADEDATUM": "2023-01-14", "VERTRAGSNUMMER": "V020", "PRODUKT_TYP": "PROD_Y", "BETRAG": 150.00, "WAEHRUNG": "EUR"}, # DWH_VERTRAG_ID <= 50
        {"DWH_VERTRAG_ID": 300, "Gueltig_von": "2023-01-10", "Gueltig_bis": "2023-01-15", "LADEDATUM": "2023-01-12", "VERTRAGSNUMMER": "V300", "PRODUKT_TYP": "PROD_Z", "BETRAG": 200.00, "WAEHRUNG": "EUR"}, # Gueltig_bis = Stichtag
    ]
    insert_dwh_data(bq_client, dwh_data)

    call_wrapper_procedure(bq_client, stichtag_str, wiederanlauf_value)

    # Assertions
    job_control_records = get_table_data(bq_client, "job_control")
    assert len(job_control_records) == 1
    assert job_control_records[0]["status"] == "OK"

    job_log_records = get_table_data(bq_client, "job_log", order_by="log_ts")
    assert len(job_log_records) >= 3
    assert all(r["log_level"] == "INFO" for r in job_log_records)
    assert job_log_records[0]["stichtag"] == stichtag
    assert job_log_records[0]["restart_value"] == wiederanlauf_value

    fos_target_records = get_table_data(bq_client, "fos_target_table")
    assert len(fos_target_records) == 0
```

---

### Test Case 5: Core Logic - Data Transformation and Filtering Correctness

*   **Purpose**: Verify that the core logic (`ausd_bp_ta_cntrct_evn_core`) correctly applies all specified filters (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `DWH_VERTRAG_ID`) and maps data to the target table as per the design.
*   **Setup**:
    1.  Clear tables.
    2.  Populate `dwh_ta_c_vertrag_source` with a comprehensive set of test data covering all filter conditions and edge cases.
*   **Action**: Call the wrapper procedure with `p_stichtag_str = '15062023'` and `p_wiederanlaufWert_input = 100`.
*   **Pass/Fail Criterion**:
    1.  The call completes successfully.
    2.  `job_control` has one `OK` record.
    3.  `fos_target_table` contains exactly 3 records.
    4.  The `DWH_VERTRAG_ID`s in `fos_target_table` are 101, 150, 200.
    5.  For each of these records, `STICH_TAG` is '2023-06-15'.
    6.  Verify data mapping: `VERTRAGSNUMMER`, `PRODUKT_CODE` (from `PRODUKT_TYP`), `SCORE_RELEVANT_VALUE` (from `BETRAG`), and `LAST_UPDATE_TS` are correct.

```python
# test_migration.py
def test_core_logic_transformation_and_filtering(bq_client):
    stichtag = datetime.date(2023, 6, 15)
    stichtag_str = "15062023"
    wiederanlauf_value = 100

    dwh_data = [
        # Expected to be included (DWH_VERTRAG_ID > 100, Gueltig_von <= '2023-06-15' < Gueltig_bis, LADEDATUM <= '2023-06-15')
        {"DWH_VERTRAG_ID": 101, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V101", "PRODUKT_TYP": "PROD_A", "BETRAG": 100.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 150, "Gueltig_von": "2023-01-01", "Gueltig_bis": "2024-01-01", "LADEDATUM": "2023-06-15", "VERTRAGSNUMMER": "V150", "PRODUKT_TYP": "PROD_B", "BETRAG": 150.50, "WAEHRUNG": "USD"},
        {"DWH_VERTRAG_ID": 200, "Gueltig_von": "2023-06-15", "Gueltig_bis": "2023-08-01", "LADEDATUM": "2023-06-01", "VERTRAGSNUMMER": "V200", "PRODUKT_TYP": "PROD_C", "BETRAG": 200.75, "WAEHRUNG": "CHF"},
        # Not included: DWH_VERTRAG_ID <= 100
        {"DWH_VERTRAG_ID": 50, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V050", "PRODUKT_TYP": "PROD_X", "BETRAG": 50.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 100, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V100", "PRODUKT_TYP": "PROD_Y", "BETRAG": 100.00, "WAEHRUNG": "EUR"},
        # Not included: Gueltig_von > Stichtag
        {"DWH_VERTRAG_ID": 201, "Gueltig_von": "2023-06-16", "Gueltig_bis": "2023-07-16", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V201", "PRODUKT_TYP": "PROD_D", "BETRAG": 201.00, "WAEHRUNG": "EUR"},
        # Not included: Stichtag >= Gueltig_bis
        {"DWH_VERTRAG_ID": 202, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-06-15", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V202", "PRODUKT_TYP": "PROD_E", "BETRAG": 202.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 203, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-06-14", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V203", "PRODUKT_TYP": "PROD_F", "BETRAG": 203.00, "WAEHRUNG": "EUR"},
        # Not included: LADEDATUM > Stichtag
        {"DWH_VERTRAG_ID": 204, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-16", "VERTRAGSNUMMER": "V204", "PRODUKT_TYP": "PROD_G", "BETRAG": 204.00, "WAEHRUNG": "EUR"},
    ]
    insert_dwh_data(bq_client, dwh_data)

    call_wrapper_procedure(bq_client, stichtag_str, wiederanlauf_value)

    # Assertions
    job_control_records = get_table_data(bq_client, "job_control")
    assert len(job_control_records) == 1
    assert job_control_records[0]["status"] == "OK"

    fos_target_records = get_table_data(bq_client, "fos_target_table", order_by="DWH_VERTRAG_ID")
    assert len(fos_target_records) == 3

    expected_ids = [101, 150, 200]
    actual_ids = [r["DWH_VERTRAG_ID"] for r in fos_target_records]
    assert actual_ids == expected_ids

    # Detailed data mapping check
    expected_data = {
        101: {"VERTRAGSNUMMER": "V101", "PRODUKT_CODE": "PROD_A", "SCORE_RELEVANT_VALUE": decimal.Decimal("100.00")},
        150: {"VERTRAGSNUMMER": "V150", "PRODUKT_CODE": "PROD_B", "SCORE_RELEVANT_VALUE": decimal.Decimal("150.50")},
        200: {"VERTRAGSNUMMER": "V200", "PRODUKT_CODE": "PROD_C", "SCORE_RELEVANT_VALUE": decimal.Decimal("200.75")},
    }
    for record in fos_target_records:
        assert record["STICH_TAG"] == stichtag
        assert record["LAST_UPDATE_TS"] is not None # Check it's set
        expected_row = expected_data[record["DWH_VERTRAG_ID"]]
        assert record["VERTRAGSNUMMER"] == expected_row["VERTRAGSNUMMER"]
        assert record["PRODUKT_CODE"] == expected_row["PRODUKT_CODE"]
        assert record["SCORE_RELEVANT_VALUE"] == expected_row["SCORE_RELEVANT_VALUE"]
```

---

### Test Case 6: External System Replacement - DWH Source Data Ingestion (Pre-requisite)

*   **Purpose**: Verify that the `dwh_ta_c_vertrag_source` table can be successfully populated from the legacy DWH, ensuring the "Oracle reads" aspect of the migration is functional. This is an integration test for the ingestion pipeline, crucial for the job's functionality.
*   **Setup**:
    1.  Access to a representative snapshot of the legacy `DWH$TA_C_VERTRAG` table.
    2.  An established data ingestion pipeline (e.g., Dataflow, Cloud Data Fusion) from the legacy DWH to `project.dataset.dwh_ta_c_vertrag_source`.
*   **Action**: Trigger the DWH ingestion pipeline to load data into `project.dataset.dwh_ta_c_vertrag_source`.
*   **Pass/Fail Criterion**:
    1.  The ingestion pipeline completes successfully.
    2.  The row count of `project.dataset.dwh_ta_c_vertrag_source` matches the row count of the source `DWH$TA_C_VERTRAG` for the ingested snapshot.
    3.  A sample of data (e.g., 100 random rows or rows for specific `DWH_VERTRAG_ID`s) from `project.dataset.dwh_ta_c_vertrag_source` matches the corresponding data in the legacy `DWH$TA_C_VERTRAG` for key columns (`DWH_VERTRAG_ID`, `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `VERTRAGSNUMMER`, `BETRAG`).

```python
# This test case is conceptual and requires interaction with the actual DWH ingestion pipeline.
# It cannot be fully automated with just BigQuery and Python client.

# Example conceptual Python code for verification:
# def test_dwh_source_data_ingestion(bq_client, legacy_dwh_connector):
#     # Action: Trigger DWH ingestion pipeline (manual or via API call)
#     # This part depends on your specific ingestion tool (e.g., Dataflow job, Cloud Data Fusion pipeline)
#     # For testing, you might manually run the pipeline or mock its success.
#     print("Manually trigger DWH ingestion pipeline and then proceed with assertions.")

#     # Pass/Fail Criterion 2: Row count parity
#     bq_row_count = bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.dwh_ta_c_vertrag_source`").result().total_rows
#     legacy_dwh_row_count = legacy_dwh_connector.get_row_count("DWH$TA_C_VERTRAG") # Assumes a connector to legacy DWH
#     assert bq_row_count == legacy_dwh_row_count, f"Row count mismatch: BQ={bq_row_count}, Legacy={legacy_dwh_row_count}"

#     # Pass/Fail Criterion 3: Data sample comparison
#     sample_ids = [123, 456, 789] # Choose representative IDs
#     bq_sample_data = list(bq_client.query(f"SELECT DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, VERTRAGSNUMMER, BETRAG FROM `{PROJECT_ID}.{DATASET_ID}.dwh_ta_c_vertrag_source` WHERE DWH_VERTRAG_ID IN ({','.join(map(str, sample_ids))}) ORDER BY DWH_VERTRAG_ID").result())
#     legacy_sample_data = legacy_dwh_connector.get_sample_data("DWH$TA_C_VERTRAG", sample_ids) # Assumes a method to get data from legacy DWH

#     # Perform deep comparison of the sample data
#     assert len(bq_sample_data) == len(legacy_sample_data)
#     for bq_row, legacy_row in zip(bq_sample_data, legacy_sample_data):
#         assert bq_row["DWH_VERTRAG_ID"] == legacy_row["DWH_VERTRAG_ID"]
#         assert bq_row["Gueltig_von"] == legacy_row["Gueltig_von"]
#         # ... compare other key fields
```

---

### Test Case 7: External System Replacement - FOS Target Data Availability

*   **Purpose**: Verify that the `fos_target_table` is accessible and consumable by the downstream FOS system, replacing the original mechanism of making data available.
*   **Setup**:
    1.  Run the `ausd_bp_ta_cntrct_evn_wrapper` procedure successfully to populate `fos_target_table` with a known set of data (e.g., using Test Case 2's setup and action).
    2.  Ensure the FOS system (or a mock FOS client) has the necessary BigQuery permissions and connection details.
*   **Action**: From the FOS system (or mock client), attempt to query `project.dataset.fos_target_table`. If the FOS system consumes data via export (e.g., to Cloud Storage), trigger the export mechanism.
*   **Pass/Fail Criterion**:
    1.  The FOS system (or mock client) successfully connects to BigQuery and queries `project.dataset.fos_target_table`.
    2.  The FOS system (or mock client) retrieves the expected number of rows and data content from `fos_target_table`.
    3.  If an export mechanism is used, the data is successfully exported to the specified Cloud Storage location in the correct format, and the FOS system can consume it from there.

```python
# This test case is conceptual and requires interaction with the actual FOS system or a mock.
# It cannot be fully automated with just BigQuery and Python client without a mock FOS.

# Example conceptual Python code for verification:
# def test_fos_target_data_availability(bq_client, mock_fos_client):
#     # Setup: Populate fos_target_table first (e.g., by calling test_explicit_parameters)
#     test_explicit_parameters(bq_client) # Run a previous test to populate data

#     stichtag = datetime.date(2023, 1, 15)
#     expected_fos_data = [
#         {"DWH_VERTRAG_ID": 100, "STICH_TAG": stichtag, "VERTRAGSNUMMER": "V100", "PRODUKT_CODE": "PROD_X", "SCORE_RELEVANT_VALUE": decimal.Decimal("100.00")},
#         {"DWH_VERTRAG_ID": 150, "STICH_TAG": stichtag, "VERTRAGSNUMMER": "V150", "PRODUKT_CODE": "PROD_Y", "SCORE_RELEVANT_VALUE": decimal.Decimal("150.00")},
#         {"DWH_VERTRAG_ID": 250, "STICH_TAG": stichtag, "VERTRAGSNUMMER": "V250", "PRODUKT_CODE": "PROD_A", "SCORE_RELEVANT_VALUE": decimal.Decimal("250.00")},
#     ]

#     # Action: FOS system queries the target table
#     actual_fos_data = mock_fos_client.query_fos_target_table(PROJECT_ID, DATASET_ID, "fos_target_table", stichtag)

#     # Pass/Fail Criterion: Data content and count match
#     assert len(actual_fos_data) == len(expected_fos_data)
#     # Further assertions to compare content, potentially ignoring LAST_UPDATE_TS for comparison
#     # (e.g., convert to dicts and compare relevant keys)
#     actual_fos_data_simplified = [{k: v for k, v in row.items() if k != 'LAST_UPDATE_TS'} for row in actual_fos_data]
#     expected_fos_data_simplified = [{k: v for k, v in row.items() if k != 'LAST_UPDATE_TS'} for row in expected_fos_data]
#     assert all(item in actual_fos_data_simplified for item in expected_fos_data_simplified)
```

---

### Test Case 8: Data Quality - Schema and Type Assertions

*   **Purpose**: Verify that the schema of the target `fos_target_table` matches expectations and that data types are correctly handled during transformation.
*   **Setup**:
    1.  Ensure `fos_target_table` is created as per the DDL.
    2.  Run the `ausd_bp_ta_cntrct_evn_wrapper` procedure successfully to populate `fos_target_table` with diverse data (e.g., using Test Case 5's setup and action).
*   **Action**: Query the schema of `fos_target_table` and inspect data types of populated records.
*   **Pass/Fail Criterion**:
    1.  The schema of `fos_target_table` matches the defined DDL (e.g., `DWH_VERTRAG_ID` is `INT64`, `STICH_TAG` is `DATE`, `VERTRAGSNUMMER` is `STRING`, `SCORE_RELEVANT_VALUE` is `NUMERIC`).
    2.  No type conversion errors occur during data insertion.
    3.  Values in `SCORE_RELEVANT_VALUE` (mapped from `BETRAG`) retain their precision and scale.

```python
# test_migration.py
from google.cloud.bigquery import SchemaField

def test_data_quality_schema_and_types(bq_client):
    # Setup: Populate fos_target_table
    test_core_logic_transformation_and_filtering(bq_client)

    # Action: Query schema and sample data
    table_ref = bq_client.dataset(DATASET_ID).table("fos_target_table")
    table = bq_client.get_table(table_ref)
    schema = table.schema

    # Pass/Fail Criterion 1: Schema matches DDL
    expected_schema = [
        SchemaField("DWH_VERTRAG_ID", "INT64", mode="REQUIRED"),
        SchemaField("STICH_TAG", "DATE", mode="REQUIRED"),
        SchemaField("VERTRAGSNUMMER", "STRING", mode="NULLABLE"),
        SchemaField("PRODUKT_CODE", "STRING", mode="NULLABLE"),
        SchemaField("SCORE_RELEVANT_VALUE", "NUMERIC", mode="NULLABLE"),
        SchemaField("LAST_UPDATE_TS", "TIMESTAMP", mode="NULLABLE")
    ]
    # Note: BigQuery client might return fields in a different order, compare sets or iterate
    assert len(schema) == len(expected_schema)
    for expected_field in expected_schema:
        found = False
        for actual_field in schema:
            if (actual_field.name == expected_field.name and
                actual_field.field_type == expected_field.field_type and
                actual_field.mode == expected_field.mode):
                found = True
                break
        assert found, f"Expected field {expected_field.name} with type {expected_field.field_type} and mode {expected_field.mode} not found or mismatched in actual schema."

    # Pass/Fail Criterion 3: Data types of actual records
    results = bq_client.query(f"SELECT DWH_VERTRAG_ID, STICH_TAG, SCORE_RELEVANT_VALUE FROM `{PROJECT_ID}.{DATASET_ID}.fos_target_table` LIMIT 1").result()
    for row in results:
        assert isinstance(row.DWH_VERTRAG_ID, int)
        assert isinstance(row.STICH_TAG, datetime.date)
        assert isinstance(row.SCORE_RELEVANT_VALUE, decimal.Decimal) # NUMERIC type in BQ maps to decimal.Decimal in Python client
```

---

### Test Case 9: Error Handling - Core Procedure Failure

*   **Purpose**: Verify that if the `ausd_bp_ta_cntrct_evn_core` procedure encounters an error, the wrapper correctly catches it, logs the error, updates `job_control` to `ERROR`, and re-raises the exception.
*   **Setup**:
    1.  Clear tables.
    2.  **Temporarily modify `ausd_bp_ta_cntrct_evn_core` to force an error.** This requires redeploying the procedure with the error-inducing code.
        ```sql
        -- Redeploy `ausd_bp_ta_cntrct_evn_core` with this temporary change:
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn_core`(
          IN p_jobkennung STRING,
          IN p_stichtag DATE,
          IN p_job_nr INT64,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          -- Force an error (e.g., division by zero)
          SELECT 1 / 0;
          -- Original INSERT statement follows, but won't be reached
          -- INSERT INTO `project.dataset.fos_target_table` (...) SELECT ...;
          -- INSERT INTO `project.dataset.job_log` (...) VALUES (...);
        END;
        ```
*   **Action**: Call the wrapper procedure with valid parameters (e.g., `p_stichtag_str = '01012023'`, `p_wiederanlaufWert_input = 0`).
*   **Pass/Fail Criterion**:
    1.  The call to the wrapper procedure fails and re-raises the underlying exception.
    2.  `job_control` table contains one record for the job with `status = 'ERROR'`.
    3.  `job_log` table contains an `ERROR` entry for the job, and its `error_message` column contains details about the core procedure failure (e.g., "division by zero").
    4.  `fos_target_table` remains unchanged (no partial inserts from the failed core procedure).

```python
# test_migration.py
def test_error_handling_core_procedure_failure(bq_client):
    stichtag_str = "01012023"
    wiederanlauf_value = 0

    # IMPORTANT: Manually redeploy ausd_bp_ta_cntrct_evn_core with the error-inducing line: SELECT 1 / 0;
    # After running this test, redeploy the correct ausd_bp_ta_cntrct_evn_core.

    with pytest.raises(Exception) as excinfo:
        call_wrapper_procedure(bq_client, stichtag_str, wiederanlauf_value)
    
    # Check for specific error messages from BigQuery
    assert "division by zero" in str(excinfo.value).lower() or "apperror: abbruch" in str(excinfo.value).lower()

    # Assertions for logging and control tables
    job_control_records = get_table_data(bq_client, "job_control")
    assert len(job_control_records) == 1
    assert job_control_records[0]["status"] == "ERROR"
    assert job_control_records[0]["error_message"] is not None
    assert "division by zero" in job_control_records[0]["error_message"].lower()

    job_log_records = get_table_data(bq_client, "job_log")
    assert any(r["log_level"] == "ERROR" for r in job_log_records)
    error_log = next(r for r in job_log_records if r["log_level"] == "ERROR")
    assert "job failed" in error_log["message"].lower()
    assert "division by zero" in error_log["error_message"].lower()

    fos_target_records = get_table_data(bq_client, "fos_target_table")
    assert len(fos_target_records) == 0 # Target table should be empty or unchanged
```

---

### Test Case 10: Idempotency / Restart Logic

*   **Purpose**: Verify that running the job multiple times with the same `stichtag` and `wiederanlaufWert` (or increasing `wiederanlaufWert`) correctly handles existing data by deleting and re-inserting records for `DWH_VERTRAG_ID >= p_wiederanlaufWert` for the given `stichtag`.
*   **Setup**:
    1.  Clear tables.
    2.  **Ensure `ausd_bp_ta_cntrct_evn_core` includes the `DELETE` logic** as described in the "Important Note" above.
    3.  Populate `dwh_ta_c_vertrag_source` with sample data.
*   **Action**:
    1.  **Run 1 (Initial Load)**: Call wrapper with `p_stichtag_str = '15062023'`, `p_wiederanlaufWert_input = 0`.
    2.  **Run 2 (Partial Update/Restart)**: Call wrapper with `p_stichtag_str = '15062023'`, `p_wiederanlaufWert_input = 30`.
    3.  **Run 3 (Full Reload for Stichtag)**: Call wrapper with `p_stichtag_str = '15062023'`, `p_wiederanlaufWert_input = 0`.
*   **Pass/Fail Criterion**:
    1.  All calls complete successfully.
    2.  **After Run 1**: `fos_target_table` contains 5 records (10, 20, 30, 40, 50) for `STICH_TAG = '2023-06-15'`.
    3.  **After Run 2**: `fos_target_table` still contains 5 records. Records with `DWH_VERTRAG_ID` 30, 40, 50 should have been deleted and re-inserted, resulting in newer `LAST_UPDATE_TS` for them, while 10, 20 remain with their original `LAST_UPDATE_TS`.
    4.  **After Run 3**: `fos_target_table` still contains 5 records. All records (10, 20, 30, 40, 50) should have been deleted and re-inserted, reflecting the latest `LAST_UPDATE_TS`.

```python
# test_migration.py
def test_idempotency_restart_logic(bq_client):
    stichtag = datetime.date(2023, 6, 15)
    stichtag_str = "15062023"

    # IMPORTANT: Ensure ausd_bp_ta_cntrct_evn_core has the DELETE logic implemented.

    dwh_data = [
        {"DWH_VERTRAG_ID": 10, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V010", "PRODUKT_TYP": "PROD_A", "BETRAG": 10.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 20, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V020", "PRODUKT_TYP": "PROD_B", "BETRAG": 20.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 30, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V030", "PRODUKT_TYP": "PROD_C", "BETRAG": 30.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 40, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V040", "PRODUKT_TYP": "PROD_D", "BETRAG": 40.00, "WAEHRUNG": "EUR"},
        {"DWH_VERTRAG_ID": 50, "Gueltig_von": "2023-06-01", "Gueltig_bis": "2023-07-01", "LADEDATUM": "2023-06-10", "VERTRAGSNUMMER": "V050", "PRODUKT_TYP": "PROD_E", "BETRAG": 50.00, "WAEHRUNG": "EUR"},
    ]
    insert_dwh_data(bq_client, dwh_data)

    # Run 1 (Initial Load)
    call_wrapper_procedure(bq_client, stichtag_str, 0)
    fos_records_run1 = get_table_data(bq_client, "fos_target_table", order_by="DWH_VERTRAG_ID")
    assert len(fos_records_run1) == 5
    assert [r["DWH_VERTRAG_ID"] for r in fos_records_run1] == [10, 20, 30, 40, 50]
    last_update_ts_run1 = {r["DWH_VERTRAG_ID"]: r["LAST_UPDATE_TS"] for r in fos_records_run1}

    # Run 2 (Partial Update/Restart)
    # Clear job_log and job_control for clean state, but keep fos_target_table
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    call_wrapper_procedure(bq_client, stichtag_str, 30) # Restart from 30

    fos_records_run2 = get_table_data(bq_client, "fos_target_table", order_by="DWH_VERTRAG_ID")
    assert len(fos_records_run2) == 5
    assert [r["DWH_VERTRAG_ID"] for r in fos_records_run2] == [10, 20, 30, 40, 50]
    last_update_ts_run2 = {r["DWH_VERTRAG_ID"]: r["LAST_UPDATE_TS"] for r in fos_records_run2}

    # Verify LAST_UPDATE_TS: 10, 20 should be same; 30, 40, 50 should be newer
    assert last_update_ts_run2[10] == last_update_ts_run1[10]
    assert last_update_ts_run2[20] == last_update_ts_run1[20]
    assert last_update_ts_run2[30] > last_update_ts_run1[30]
    assert last_update_ts_run2[40] > last_update_ts_run1[40]
    assert last_update_ts_run2[50] > last_update_ts_run1[50]

    # Run 3 (Full Reload for Stichtag)
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    call_wrapper_procedure(bq_client, stichtag_str, 0) # Restart from 0

    fos_records_run3 = get_table_data(bq_client, "fos_target_table", order_by="DWH_VERTRAG_ID")
    assert len(fos_records_run3) == 5
    assert [r["DWH_VERTRAG_ID"] for r in fos_records_run3] == [10, 20, 30, 40, 50]
    last_update_ts_run3 = {r["DWH_VERTRAG_ID"]: r["LAST_UPDATE_TS"] for r in fos_records_run3}

    # Verify LAST_UPDATE_TS: all should be newer than Run 2
    assert all(last_update_ts_run3[id_] > last_update_ts_run2[id_] for id_ in [10, 20, 30, 40, 50])
```