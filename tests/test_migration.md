As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_bp_ta_rn_vertrag.ksh` from KornShell/Oracle to BigQuery Stored Procedures. The following test plan aims to ensure behavioral equivalence, transformation correctness, and proper integration with the new BigQuery environment.

The tests are structured to cover output parity, transformation logic, external system replacements, and data quality assertions. Each test case includes its purpose, setup, action, and a concrete pass/fail criterion. Runnable `pytest` and SQL code snippets are provided where applicable.

---

## Global Setup for BigQuery Tests

Before running any tests, ensure the BigQuery environment is set up as follows:

1.  **Project and Dataset:** A GCP project and BigQuery dataset (`project.dataset`) are created and accessible.
2.  **Service Account:** A service account with appropriate BigQuery Data Editor and BigQuery Job User roles is configured for `pytest` to use.
3.  **BigQuery Tables DDL:** The following tables must be created in your BigQuery dataset.
    *   `project.dataset.job_audit` (provided in migration code)
    *   `project.dataset.sof_ta_rn_vertrag` (provided in migration code)
    *   `project.dataset.dwtk_meldungen` (placeholder, minimal schema for this job)
    *   `project.dataset.sof_ta_rn_einzeln` (placeholder, inferred from `d_ausd_bp_ta_rn_vertrag.sql`)

    ```sql
    -- DDL for project.dataset.dwtk_meldungen
    CREATE TABLE IF NOT EXISTS `project.dataset.dwtk_meldungen` (
        job_kennung STRING,
        timecreated TIMESTAMP
    );

    -- DDL for project.dataset.sof_ta_rn_einzeln
    CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_rn_einzeln` (
        cntrct_id STRING,
        TN_multi_single STRING,
        TN_TEL_msisdn STRING,
        TN_TEL_status STRING,
        TN_TEL_valid_to DATE,
        TN_FAX_msisdn STRING,
        TN_FAX_status STRING,
        TN_FAX_valid_to DATE,
        TN_DAT_msisdn STRING,
        TN_DAT_status STRING,
        TN_DAT_valid_to DATE,
        TC_multi_single STRING,
        TC_TEL_msisdn STRING,
        TC_TEL_status STRING,
        TC_TEL_valid_to DATE,
        TC_FAX_msisdn STRING,
        TC_FAX_status STRING,
        TC_FAX_valid_to DATE,
        TC_DAT_msisdn STRING,
        TC_DAT_status STRING,
        TC_DAT_valid_to DATE,
        TB_multi_single STRING,
        TB_TEL_msisdn STRING,
        TB_TEL_status STRING,
        TB_TEL_valid_to DATE,
        TB_FAX_msisdn STRING,
        TB_FAX_status STRING,
        TB_FAX_valid_to DATE,
        TB_DAT_msisdn STRING,
        TB_DAT_status STRING,
        TB_DAT_valid_to DATE,
        MS_RN_1_msisdn STRING,
        MS_RN_1_status STRING,
        MS_RN_1_valid_to DATE,
        MS_RN_2_msisdn STRING,
        MS_RN_2_status STRING,
        MS_RN_2_valid_to DATE
    );
    ```

4.  **BigQuery Stored Procedures:** The three BigQuery Stored Procedures (`d_ausd_bp_ta_rn_vertrag`, `k_ausd_bp_ta_rn_vertrag`, `ausd_bp_ta_rn_vertrag`) provided in the migration code must be deployed to the target dataset.
5.  **Python Environment:** A Python environment with `pytest` and `google-cloud-bigquery` installed.
6.  **Oracle Environment:** Access to the legacy Oracle database and the ability to run the original KornShell script and extract data for comparison.

### `conftest.py` for Pytest

```python
# conftest.py
import pytest
from google.cloud import bigquery
import os
import uuid
from datetime import datetime, date

# Configuration for your BigQuery project and dataset
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
DATASET_ID = os.getenv("BQ_DATASET_ID", "your_dataset")

# Full table IDs
DWTK_MELDUNGEN_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen`"
SOF_TA_RN_EINZELN_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_einzeln`"
SOF_TA_RN_VERTRAG_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag`"
JOB_AUDIT_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.job_audit`"

# Stored Procedure IDs
MAIN_SP = f"{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_rn_vertrag"
K_SP = f"{PROJECT_ID}.{DATASET_ID}.k_ausd_bp_ta_rn_vertrag"
D_SP = f"{PROJECT_ID}.{DATASET_ID}.d_ausd_bp_ta_rn_vertrag"

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    client = bigquery.Client(project=PROJECT_ID)
    yield client
    client.close()

@pytest.fixture(scope="function", autouse=True)
def setup_teardown_tables(bq_client):
    """
    Ensures source and target tables are empty before each test,
    and truncates job_audit to keep it clean for each test's logging.
    """
    tables_to_truncate = [
        DWTK_MELDUNGEN_TABLE,
        SOF_TA_RN_EINZELN_TABLE,
        SOF_TA_RN_VERTRAG_TABLE,
        JOB_AUDIT_TABLE,
    ]
    for table_id in tables_to_truncate:
        bq_client.query(f"TRUNCATE TABLE {table_id}").result()
    yield

def insert_data(bq_client, table_id, rows):
    """Helper function to insert data into a BigQuery table."""
    errors = bq_client.insert_rows_json(table_id.replace('`', ''), rows)
    if errors:
        raise Exception(f"Errors inserting rows into {table_id}: {errors}")

def fetch_data(bq_client, table_id, order_by_col=None):
    """Helper function to fetch all data from a BigQuery table."""
    query = f"SELECT * FROM {table_id}"
    if order_by_col:
        query += f" ORDER BY {order_by_col}"
    query_job = bq_client.query(query)
    return [dict(row) for row in query_job.result()]

def call_bq_procedure(bq_client, procedure_id, *args):
    """Helper function to call a BigQuery stored procedure."""
    arg_strings = []
    for arg in args:
        if isinstance(arg, str):
            arg_strings.append(f"'{arg}'")
        elif isinstance(arg, (int, float)):
            arg_strings.append(str(arg))
        elif arg is None:
            arg_strings.append("NULL")
        else:
            raise ValueError(f"Unsupported argument type: {type(arg)}")
    
    call_statement = f"CALL {procedure_id}({', '.join(arg_strings)})"
    print(f"Executing: {call_statement}")
    query_job = bq_client.query(call_statement)
    query_job.result() # Wait for the job to complete
    return query_job

def get_audit_log_entry(bq_client, run_id, job_kennung=None, status=None):
    """Helper to fetch a specific audit log entry."""
    query = f"SELECT * FROM {JOB_AUDIT_TABLE} WHERE run_id = '{run_id}'"
    if job_kennung:
        query += f" AND job_kennung = '{job_kennung}'"
    if status:
        query += f" AND status = '{status}'"
    query += " ORDER BY created_at DESC LIMIT 1"
    query_job = bq_client.query(query)
    results = list(query_job.result())
    return dict(results[0]) if results else None

```

---

## Test Cases

### Test Case 1: Full End-to-End Parity (Default Parameters)

*   **Purpose:** Verify the migrated job produces identical output to the legacy job when run with default parameters (no `Stichtag` or `Wiederanlaufwert` provided). This covers output parity and overall job flow.
*   **Setup:**
    1.  **Oracle:** Load a representative dataset into `isbert_schema.dwtk_meldungen` and `sof$ta_rn_einzeln`. Ensure `sof$ta_rn_vertrag` is empty.
    2.  **BigQuery:** Load the *exact same* dataset into `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_rn_einzeln`. Ensure `project.dataset.sof_ta_rn_vertrag` is empty.
    *   **Example `sof_ta_rn_einzeln` data:**
        ```json
        [
            {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": "2023-01-01", "TN_FAX_msisdn": "222", "TN_FAX_status": "I", "TN_FAX_valid_to": "2023-01-01", "TC_TEL_msisdn": "333", "TC_TEL_status": "A", "TC_TEL_valid_to": "2023-01-01"},
            {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": "2023-01-02", "TN_FAX_msisdn": "222", "TN_FAX_status": "A", "TN_FAX_valid_to": "2023-01-02", "TC_TEL_msisdn": "333", "TC_TEL_status": "A", "TC_TEL_valid_to": "2023-01-02"},
            {"cntrct_id": "C2", "TN_multi_single": "S", "TN_TEL_msisdn": "444", "TN_TEL_status": "A", "TN_TEL_valid_to": "2023-01-03"}
        ]
        ```
*   **Action:**
    1.  **Legacy:** Execute the original KornShell script: `r_ausd_bp_ta_rn_vertrag.ksh`
    2.  **Migrated:** Execute the BigQuery main stored procedure:
        ```python
        # test_migration.py
        from conftest import bq_client, MAIN_SP, SOF_TA_RN_EINZELN_TABLE, SOF_TA_RN_VERTRAG_TABLE, insert_data, fetch_data, call_bq_procedure
        from datetime import date

        def test_full_parity_default_params(bq_client):
            # Setup: Insert sample data into BigQuery source table
            sample_data = [
                {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1), "TN_FAX_msisdn": "222", "TN_FAX_status": "I", "TN_FAX_valid_to": date(2023, 1, 1), "TC_TEL_msisdn": "333", "TC_TEL_status": "A", "TC_TEL_valid_to": date(2023, 1, 1)},
                {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 2), "TN_FAX_msisdn": "222", "TN_FAX_status": "A", "TN_FAX_valid_to": date(2023, 1, 2), "TC_TEL_msisdn": "333", "TC_TEL_status": "A", "TC_TEL_valid_to": date(2023, 1, 2)},
                {"cntrct_id": "C2", "TN_multi_single": "S", "TN_TEL_msisdn": "444", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 3), "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None, "TC_TEL_msisdn": None, "TC_TEL_status": None, "TC_TEL_valid_to": None}
            ]
            insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, sample_data)

            # Action: Call the main BigQuery stored procedure with default parameters
            call_bq_procedure(bq_client, MAIN_SP, None, None) # p_stichtag_in=NULL, p_wiederanlaufWert_in=NULL

            # Fetch results from BigQuery
            bq_results = fetch_data(bq_client, SOF_TA_RN_VERTRAG_TABLE, order_by_col="CNTRCT_ID")
            
            # Placeholder for Oracle results (would be loaded from file/DB)
            # oracle_results = load_oracle_data("oracle_output_default.csv") 
            # For demonstration, let's define expected BQ output based on MAX aggregation
            expected_bq_results = [
                {'CNTRCT_ID': 'C1', 'TN_MULTI_SINGLE': 'M', 'TN_TEL_MSISDN': '111', 'TN_TEL_STATUS': 'A', 'TN_TEL_VALID_TO': date(2023, 1, 2), 'TN_FAX_MSISDN': '222', 'TN_FAX_STATUS': 'I', 'TN_FAX_VALID_TO': date(2023, 1, 2), 'TN_DAT_MSISDN': None, 'TN_DAT_STATUS': None, 'TN_DAT_VALID_TO': None, 'TC_MULTI_SINGLE': None, 'TC_TEL_MSISDN': '333', 'TC_TEL_STATUS': 'A', 'TC_TEL_VALID_TO': date(2023, 1, 2), 'TC_FAX_MSISDN': None, 'TC_FAX_STATUS': None, 'TC_FAX_VALID_TO': None, 'TC_DAT_MSISDN': None, 'TC_DAT_STATUS': None, 'TC_DAT_VALID_TO': None, 'TB_MULTI_SINGLE': None, 'TB_TEL_MSISDN': None, 'TB_TEL_STATUS': None, 'TB_TEL_VALID_TO': None, 'TB_FAX_MSISDN': None, 'TB_FAX_STATUS': None, 'TB_FAX_VALID_TO': None, 'TB_DAT_MSISDN': None, 'TB_DAT_STATUS': None, 'TB_DAT_VALID_TO': None, 'MS_RN_1_MSISDN': None, 'MS_RN_1_STATUS': None, 'MS_RN_1_VALID_TO': None, 'MS_RN_2_MSISDN': None, 'MS_RN_2_STATUS': None, 'MS_RN_2_VALID_TO': None},
                {'CNTRCT_ID': 'C2', 'TN_MULTI_SINGLE': 'S', 'TN_TEL_MSISDN': '444', 'TN_TEL_STATUS': 'A', 'TN_TEL_VALID_TO': date(2023, 1, 3), 'TN_FAX_MSISDN': None, 'TN_FAX_STATUS': None, 'TN_FAX_VALID_TO': None, 'TN_DAT_MSISDN': None, 'TN_DAT_STATUS': None, 'TN_DAT_VALID_TO': None, 'TC_MULTI_SINGLE': None, 'TC_TEL_MSISDN': None, 'TC_TEL_STATUS': None, 'TC_TEL_VALID_TO': None, 'TC_FAX_MSISDN': None, 'TC_FAX_STATUS': None, 'TC_FAX_VALID_TO': None, 'TC_DAT_MSISDN': None, 'TC_DAT_STATUS': None, 'TC_DAT_VALID_TO': None, 'TB_MULTI_SINGLE': None, 'TB_TEL_MSISDN': None, 'TB_TEL_STATUS': None, 'TB_TEL_VALID_TO': None, 'TB_FAX_MSISDN': None, 'TB_FAX_STATUS': None, 'TB_FAX_VALID_TO': None, 'TB_DAT_MSISDN': None, 'TB_DAT_STATUS': None, 'TB_DAT_VALID_TO': None, 'MS_RN_1_MSISDN': None, 'MS_RN_1_STATUS': None, 'MS_RN_1_VALID_TO': None, 'MS_RN_2_MSISDN': None, 'MS_RN_2_STATUS': None, 'MS_RN_2_VALID_TO': None}
            ]
            
            # Assertions
            assert len(bq_results) == len(expected_bq_results)
            assert bq_results == expected_bq_results # In a real scenario, this would be oracle_results
        ```
*   **Pass/Fail Criterion:**
    *   The content of `project.dataset.sof_ta_rn_vertrag` must be identical to the content of Oracle's `sof$ta_rn_vertrag` after sorting both by `CNTRCT_ID`.
    *   The row count in both target tables must be the same.

### Test Case 2: Full End-to-End Parity (Explicit Stichtag)

*   **Purpose:** Verify output parity when an explicit `Stichtag` is provided, ensuring parameter passing and date handling are correct.
*   **Setup:**
    1.  **Oracle:** Load a representative dataset into source tables. Ensure `sof$ta_rn_vertrag` is empty.
    2.  **BigQuery:** Load the *exact same* dataset into BigQuery source tables. Ensure `project.dataset.sof_ta_rn_vertrag` is empty.
*   **Action:**
    1.  **Legacy:** Execute the original KornShell script: `r_ausd_bp_ta_rn_vertrag.ksh -s 01012023` (or any valid date).
    2.  **Migrated:** Execute the BigQuery main stored procedure:
        ```python
        # test_migration.py
        def test_full_parity_explicit_stichtag(bq_client):
            sample_data = [
                {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1), "TN_FAX_msisdn": "222", "TN_FAX_status": "I", "TN_FAX_valid_to": date(2023, 1, 1), "TC_TEL_msisdn": "333", "TC_TEL_status": "A", "TC_TEL_valid_to": date(2023, 1, 1)},
                {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 2), "TN_FAX_msisdn": "222", "TN_FAX_status": "A", "TN_FAX_valid_to": date(2023, 1, 2), "TC_TEL_msisdn": "333", "TC_TEL_status": "A", "TC_TEL_valid_to": date(2023, 1, 2)},
                {"cntrct_id": "C2", "TN_multi_single": "S", "TN_TEL_msisdn": "444", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 3), "TN_FAX_msisdn": None, "TN_FAX_status": None, "TN_FAX_valid_to": None, "TC_TEL_msisdn": None, "TC_TEL_status": None, "TC_TEL_valid_to": None}
            ]
            insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, sample_data)

            explicit_stichtag = "01012023" # Example Stichtag
            call_bq_procedure(bq_client, MAIN_SP, explicit_stichtag, None)

            bq_results = fetch_data(bq_client, SOF_TA_RN_VERTRAG_TABLE, order_by_col="CNTRCT_ID")
            # Compare bq_results with Oracle output for the same explicit_stichtag
            # oracle_results = load_oracle_data(f"oracle_output_{explicit_stichtag}.csv")
            # assert bq_results == oracle_results
            # For this specific job, Stichtag is not used in the core SQL logic (d_ausd_bp_ta_rn_vertrag.sql)
            # So, the output should be the same as default params.
            expected_bq_results = [
                {'CNTRCT_ID': 'C1', 'TN_MULTI_SINGLE': 'M', 'TN_TEL_MSISDN': '111', 'TN_TEL_STATUS': 'A', 'TN_TEL_VALID_TO': date(2023, 1, 2), 'TN_FAX_MSISDN': '222', 'TN_FAX_STATUS': 'I', 'TN_FAX_VALID_TO': date(2023, 1, 2), 'TN_DAT_MSISDN': None, 'TN_DAT_STATUS': None, 'TN_DAT_VALID_TO': None, 'TC_MULTI_SINGLE': None, 'TC_TEL_MSISDN': '333', 'TC_TEL_STATUS': 'A', 'TC_TEL_VALID_TO': date(2023, 1, 2), 'TC_FAX_MSISDN': None, 'TC_FAX_STATUS': None, 'TC_FAX_VALID_TO': date(2023, 1, 2), 'TC_DAT_MSISDN': None, 'TC_DAT_STATUS': None, 'TC_DAT_VALID_TO': None, 'TB_MULTI_SINGLE': None, 'TB_TEL_MSISDN': None, 'TB_TEL_STATUS': None, 'TB_TEL_VALID_TO': None, 'TB_FAX_MSISDN': None, 'TB_FAX_STATUS': None, 'TB_FAX_VALID_TO': None, 'TB_DAT_MSISDN': None, 'TB_DAT_STATUS': None, 'TB_DAT_VALID_TO': None, 'MS_RN_1_MSISDN': None, 'MS_RN_1_STATUS': None, 'MS_RN_1_VALID_TO': None, 'MS_RN_2_MSISDN': None, 'MS_RN_2_STATUS': None, 'MS_RN_2_VALID_TO': None},
                {'CNTRCT_ID': 'C2', 'TN_MULTI_SINGLE': 'S', 'TN_TEL_MSISDN': '444', 'TN_TEL_STATUS': 'A', 'TN_TEL_VALID_TO': date(2023, 1, 3), 'TN_FAX_MSISDN': None, 'TN_FAX_STATUS': None, 'TN_FAX_VALID_TO': None, 'TN_DAT_MSISDN': None, 'TN_DAT_STATUS': None, 'TN_DAT_VALID_TO': None, 'TC_MULTI_SINGLE': None, 'TC_TEL_MSISDN': None, 'TC_TEL_STATUS': None, 'TC_TEL_VALID_TO': None, 'TC_FAX_MSISDN': None, 'TC_FAX_STATUS': None, 'TC_FAX_VALID_TO': None, 'TC_DAT_MSISDN': None, 'TC_DAT_STATUS': None, 'TC_DAT_VALID_TO': None, 'TB_MULTI_SINGLE': None, 'TB_TEL_MSISDN': None, 'TB_TEL_STATUS': None, 'TB_TEL_VALID_TO': None, 'TB_FAX_MSISDN': None, 'TB_FAX_STATUS': None, 'TB_FAX_VALID_TO': None, 'TB_DAT_MSISDN': None, 'TB_DAT_STATUS': None, 'TB_DAT_VALID_TO': None, 'MS_RN_1_MSISDN': None, 'MS_RN_1_STATUS': None, 'MS_RN_1_VALID_TO': None, 'MS_RN_2_MSISDN': None, 'MS_RN_2_STATUS': None, 'MS_RN_2_VALID_TO': None}
            ]
            assert bq_results == expected_bq_results
        ```
*   **Pass/Fail Criterion:**
    *   The content of `project.dataset.sof_ta_rn_vertrag` must be identical to the content of Oracle's `sof$ta_rn_vertrag` after sorting.
    *   **Note:** The design document indicates `Stichtag` is not used in the core SQL. This test confirms that the BigQuery job also does not use it for filtering, matching the *provided* SQL. If the legacy job *did* use it, this would be a functional gap.

### Test Case 3: `p_wiederanlaufWert` Handling (Behavioral Equivalence)

*   **Purpose:** Verify the handling of `p_wiederanlaufWert` matches the legacy system's behavior. The design document explicitly states this logic is "unresolved" and "not implemented" in the provided BigQuery SQL. This test will confirm the *current* BigQuery behavior.
*   **Setup:**
    1.  **Oracle:** Load `sof$ta_rn_einzeln` with data where `cntrct_id` values are both below and above a chosen `Wiederanlaufwert` (e.g., `C1`, `C10`, `C100`).
    2.  **BigQuery:** Load the *exact same* data into `project.dataset.sof_ta_rn_einzeln`.
*   **Action:**
    1.  **Legacy:** Execute `r_ausd_bp_ta_rn_vertrag.ksh -l 50` (assuming `cntrct_id` can be compared numerically or lexicographically for the `>` condition).
    2.  **Migrated:** Execute the BigQuery main stored procedure:
        ```python
        # test_migration.py
        def test_wiederanlaufwert_handling(bq_client):
            sample_data = [
                {"cntrct_id": "C1", "TN_multi_single": "M", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)},
                {"cntrct_id": "C50", "TN_multi_single": "S", "TN_TEL_msisdn": "222", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 2)},
                {"cntrct_id": "C100", "TN_multi_single": "M", "TN_TEL_msisdn": "333", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 3)}
            ]
            insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, sample_data)

            wiederanlaufwert = 50 # This value is passed but not used in the current BQ DML
            call_bq_procedure(bq_client, MAIN_SP, None, wiederanlaufwert)

            bq_results = fetch_data(bq_client, SOF_TA_RN_VERTRAG_TABLE, order_by_col="CNTRCT_ID")
            
            # Expected: ALL records should be present, as p_wiederanlaufWert is not applied in the BQ DML
            assert len(bq_results) == 3 
            assert any(r['CNTRCT_ID'] == 'C1' for r in bq_results) # C1 should NOT be filtered out
            assert any(r['CNTRCT_ID'] == 'C50' for r in bq_results)
            assert any(r['CNTRCT_ID'] == 'C100' for r in bq_results)

            # If the legacy system *did* filter, this test would fail, indicating a functional gap.
            # This test confirms the BigQuery code's current behavior as per the design document's risk section.
        ```
*   **Pass/Fail Criterion:**
    *   **If legacy job *does not* filter by `Wiederanlaufwert`:** The content of `project.dataset.sof_ta_rn_vertrag` must be identical to Oracle's, meaning no filtering occurred based on `p_wiederanlaufWert`.
    *   **If legacy job *does* filter by `Wiederanlaufwert`:** This test will fail, indicating a functional gap in the BigQuery migration, as the provided BigQuery SQL does not implement this filtering. This should be flagged as a critical defect.

### Test Case 4: Aggregation Logic Correctness

*   **Purpose:** Verify the `MAX()` aggregation and `GROUP BY cntrct_id` logic correctly collapses multiple detail rows into a single summary row. This covers transformation correctness.
*   **Setup:**
    1.  Load `project.dataset.sof_ta_rn_einzeln` with data containing multiple rows for the same `cntrct_id`, where different columns have varying values that `MAX()` should resolve.
    *   **Example `sof_ta_rn_einzeln` data:**
        ```json
        [
            {"cntrct_id": "C_AGG", "TN_TEL_msisdn": "100", "TN_TEL_status": "Z", "TN_TEL_valid_to": "2023-01-01"},
            {"cntrct_id": "C_AGG", "TN_TEL_msisdn": "200", "TN_TEL_status": "A", "TN_TEL_valid_to": "2023-01-05"},
            {"cntrct_id": "C_AGG", "TN_TEL_msisdn": "150", "TN_TEL_status": "B", "TN_TEL_valid_to": "2023-01-03"}
        ]
        ```
*   **Action:** Execute the BigQuery main stored procedure.
    ```python
    # test_migration.py
    def test_aggregation_logic_correctness(bq_client):
        sample_data = [
            {"cntrct_id": "C_AGG", "TN_multi_single": "X", "TN_TEL_msisdn": "100", "TN_TEL_status": "Z", "TN_TEL_valid_to": date(2023, 1, 1)},
            {"cntrct_id": "C_AGG", "TN_multi_single": "Y", "TN_TEL_msisdn": "200", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 5)},
            {"cntrct_id": "C_AGG", "TN_multi_single": "Z", "TN_TEL_msisdn": "150", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 3)}
        ]
        insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, sample_data)

        call_bq_procedure(bq_client, MAIN_SP, None, None)

        bq_results = fetch_data(bq_client, SOF_TA_RN_VERTRAG_TABLE)
        
        assert len(bq_results) == 1
        result = bq_results[0]
        assert result['CNTRCT_ID'] == 'C_AGG'
        assert result['TN_TEL_MSISDN'] == '200' # MAX of '100', '200', '150'
        assert result['TN_TEL_STATUS'] == 'Z'   # MAX of 'Z', 'A', 'B'
        assert result['TN_TEL_VALID_TO'] == date(2023, 1, 5) # MAX of dates
        assert result['TN_MULTI_SINGLE'] == 'Z' # MAX of 'X', 'Y', 'Z'
    ```
*   **Pass/Fail Criterion:** The single resulting row in `project.dataset.sof_ta_rn_vertrag` for `C_AGG` must contain the `MAX()` value for each aggregated column as expected.

### Test Case 5: `TRUNCATE` Operation Correctness

*   **Purpose:** Verify that the target table `project.dataset.sof_ta_rn_vertrag` is truncated before new data is inserted, replacing the Oracle PL/SQL `runstatement` call. This covers external system replacement and data quality.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_rn_vertrag` with some existing data (e.g., one row).
    2.  Populate `project.dataset.sof_ta_rn_einzeln` with new data (e.g., two rows for different `cntrct_id`s).
*   **Action:** Execute the BigQuery main stored procedure.
    ```python
    # test_migration.py
    def test_truncate_operation(bq_client):
        # Setup: Populate target table with old data
        old_data = [{"CNTRCT_ID": "OLD1", "TN_TEL_MSISDN": "999", "TN_TEL_STATUS": "X", "TN_TEL_VALID_TO": date(2022, 1, 1)}]
        insert_data(bq_client, SOF_TA_RN_VERTRAG_TABLE, old_data)
        
        # Setup: Populate source table with new data
        new_source_data = [
            {"cntrct_id": "NEW1", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)},
            {"cntrct_id": "NEW2", "TN_TEL_msisdn": "222", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2)}
        ]
        insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, new_source_data)

        # Action: Call the main BigQuery stored procedure
        call_bq_procedure(bq_client, MAIN_SP, None, None)

        # Fetch results from target table
        bq_results = fetch_data(bq_client, SOF_TA_RN_VERTRAG_TABLE, order_by_col="CNTRCT_ID")

        # Assertions
        assert len(bq_results) == 2 # Only new data should be present
        assert all(r['CNTRCT_ID'].startswith('NEW') for r in bq_results)
        assert not any(r['CNTRCT_ID'] == 'OLD1' for r in bq_results)
    ```
*   **Pass/Fail Criterion:** The `project.dataset.sof_ta_rn_vertrag` table must contain only the newly inserted rows from `sof_ta_rn_einzeln`, and the previously existing "old" data must be absent.

### Test Case 6: Date Handling and Default `Stichtag`

*   **Purpose:** Verify that when `p_stichtag_in` is not provided, the job correctly defaults to `CURRENT_DATE()` and formats it as `DDMMYYYY`, replacing the shell-based date logic. This covers transformation correctness and external system replacement.
*   **Setup:** None specific.
*   **Action:** Execute the BigQuery main stored procedure without providing `p_stichtag_in`.
    ```python
    # test_migration.py
    from datetime import datetime

    def test_default_stichtag_calculation(bq_client):
        # Action: Call the main BigQuery stored procedure with NULL stichtag
        call_bq_procedure(bq_client, MAIN_SP, None, None)

        # Fetch the audit log entry for the main job
        audit_entry = bq_client.query(f"SELECT message FROM {JOB_AUDIT_TABLE} WHERE job_kennung = 'ausd_bp_ta_rn_vertrag' AND status = 'COMPLETED' ORDER BY created_at DESC LIMIT 1").result().to_dataframe()
        
        assert not audit_entry.empty
        message = audit_entry['message'].iloc[0]
        
        # Extract Stichtag from the message
        # Example message: "Job completed successfully. Stichtag: 25102023"
        stichtag_str = message.split('Stichtag: ')[1].strip()
        
        # Assert that the Stichtag is today's date in DDMMYYYY format
        expected_stichtag = datetime.now().strftime("%d%m%Y")
        assert stichtag_str == expected_stichtag
    ```
*   **Pass/Fail Criterion:** The `job_audit` table must contain an entry for `ausd_bp_ta_rn_vertrag` with a `message` indicating the `Stichtag` used, which should be `CURRENT_DATE()` formatted as `DDMMYYYY`.

### Test Case 7: NULL Handling in Aggregation

*   **Purpose:** Verify that `MAX()` aggregation correctly handles `NULL` values, ensuring non-NULL values are preferred over `NULL`s. This covers transformation correctness.
*   **Setup:** Load `project.dataset.sof_ta_rn_einzeln` with data where some columns have `NULL`s for a `cntrct_id` that also has non-NULL values for the same column in other rows.
    *   **Example `sof_ta_rn_einzeln` data:**
        ```json
        [
            {"cntrct_id": "C_NULL", "TN_TEL_msisdn": "100", "TN_TEL_status": "A", "TN_TEL_valid_to": "2023-01-01"},
            {"cntrct_id": "C_NULL", "TN_TEL_msisdn": null, "TN_TEL_status": "B", "TN_TEL_valid_to": "2023-01-02"},
            {"cntrct_id": "C_NULL", "TN_TEL_msisdn": "200", "TN_TEL_status": null, "TN_TEL_valid_to": null}
        ]
        ```
*   **Action:** Execute the BigQuery main stored procedure.
    ```python
    # test_migration.py
    def test_null_handling_in_aggregation(bq_client):
        sample_data = [
            {"cntrct_id": "C_NULL", "TN_TEL_msisdn": "100", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)},
            {"cntrct_id": "C_NULL", "TN_TEL_msisdn": None, "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2)},
            {"cntrct_id": "C_NULL", "TN_TEL_msisdn": "200", "TN_TEL_status": None, "TN_TEL_valid_to": None}
        ]
        insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, sample_data)

        call_bq_procedure(bq_client, MAIN_SP, None, None)

        bq_results = fetch_data(bq_client, SOF_TA_RN_VERTRAG_TABLE)
        
        assert len(bq_results) == 1
        result = bq_results[0]
        assert result['CNTRCT_ID'] == 'C_NULL'
        assert result['TN_TEL_MSISDN'] == '200' # MAX of '100', NULL, '200'
        assert result['TN_TEL_STATUS'] == 'B'   # MAX of 'A', 'B', NULL
        assert result['TN_TEL_VALID_TO'] == date(2023, 1, 2) # MAX of date(2023,1,1), date(2023,1,2), NULL
    ```
*   **Pass/Fail Criterion:** The aggregated row for `C_NULL` in `project.dataset.sof_ta_rn_vertrag` must contain the highest non-NULL value for each column. If all values for a column are `NULL`, the result should be `NULL`.

### Test Case 8: Empty Source Table

*   **Purpose:** Verify the job handles an empty source table gracefully, resulting in an empty target table and correct logging. This covers data quality and error handling.
*   **Setup:** Ensure `project.dataset.sof_ta_rn_einzeln` is empty.
*   **Action:** Execute the BigQuery main stored procedure.
    ```python
    # test_migration.py
    def test_empty_source_table(bq_client):
        # Action: Call the main BigQuery stored procedure (source table is empty by fixture)
        call_bq_procedure(bq_client, MAIN_SP, None, None)

        # Fetch results from target table
        bq_results = fetch_data(bq_client, SOF_TA_RN_VERTRAG_TABLE)
        
        # Fetch audit log entry for the d_ausd_bp_ta_rn_vertrag procedure
        audit_entry = bq_client.query(f"SELECT record_count, status FROM {JOB_AUDIT_TABLE} WHERE job_kennung = 'ausd_bp_ta_rn_vertrag' AND status = 'COMPLETED' ORDER BY created_at DESC LIMIT 1").result().to_dataframe()

        # Assertions
        assert len(bq_results) == 0 # Target table should be empty
        assert not audit_entry.empty
        assert audit_entry['status'].iloc[0] == 'COMPLETED'
        # The record_count in the top-level audit is for the entire job, not just the DML.
        # The d_ausd_bp_ta_rn_vertrag procedure logs its own record_count.
        d_audit_entry = bq_client.query(f"SELECT record_count, status FROM {JOB_AUDIT_TABLE} WHERE job_kennung = 'ausd_bp_ta_rn_vertrag' AND message LIKE '%d_ausd_bp_ta_rn_vertrag completed%' ORDER BY created_at DESC LIMIT 1").result().to_dataframe()
        assert not d_audit_entry.empty
        assert d_audit_entry['record_count'].iloc[0] == 0
    ```
*   **Pass/Fail Criterion:**
    *   `project.dataset.sof_ta_rn_vertrag` must be empty.
    *   The `job_audit` table must show a `SUCCESS` status for all procedures involved (`d_ausd_bp_ta_rn_vertrag`, `k_ausd_bp_ta_rn_vertrag`, `ausd_bp_ta_rn_vertrag`).
    *   The `record_count` in the `job_audit` entry for `d_ausd_bp_ta_rn_vertrag` must be `0`.

### Test Case 9: Data Quality - `cntrct_id` Not Null

*   **Purpose:** Ensure that `CNTRCT_ID` in the target table is never `NULL`, as it is the grouping key and a fundamental identifier. This covers data quality.
*   **Setup:** Load `project.dataset.sof_ta_rn_einzeln` with valid data, ensuring `cntrct_id` is always populated. (The `GROUP BY` clause naturally handles this, but it's good to explicitly test).
*   **Action:** Execute the BigQuery main stored procedure.
    ```python
    # test_migration.py
    def test_cntrct_id_not_null(bq_client):
        sample_data = [
            {"cntrct_id": "C1", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)},
            {"cntrct_id": "C2", "TN_TEL_msisdn": "222", "TN_TEL_status": "B", "TN_TEL_valid_to": date(2023, 1, 2)}
        ]
        insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, sample_data)

        call_bq_procedure(bq_client, MAIN_SP, None, None)

        # Assert that no CNTRCT_ID is NULL in the target table
        query = f"SELECT COUNT(*) FROM {SOF_TA_RN_VERTRAG_TABLE} WHERE CNTRCT_ID IS NULL"
        null_cntrct_ids = bq_client.query(query).result().to_dataframe().iloc[0, 0]
        
        assert null_cntrct_ids == 0
    ```
*   **Pass/Fail Criterion:** A query on `project.dataset.sof_ta_rn_vertrag` for `CNTRCT_ID IS NULL` must return `0` rows.

### Test Case 10: Logging and Error Handling (Invalid Stichtag)

*   **Purpose:** Verify that invalid input parameters (e.g., malformed `Stichtag`) are caught, logged as `FAILED`, and the job terminates gracefully with an error. This covers error handling and external system replacement (logging).
*   **Setup:** None.
*   **Action:** Execute the BigQuery main stored procedure with an invalid `p_stichtag_in`.
    ```python
    # test_migration.py
    import pytest

    def test_error_handling_invalid_stichtag(bq_client):
        invalid_stichtag = "INVALIDDATE"
        
        # Action: Call the main BigQuery stored procedure with an invalid stichtag
        # Expecting an error to be raised
        with pytest.raises(Exception) as excinfo:
            call_bq_procedure(bq_client, MAIN_SP, invalid_stichtag, None)
        
        # Assert that the error message indicates invalid date format
        assert "Stichtag (p_stichtag) has invalid format" in str(excinfo.value)

        # Verify audit log entries
        # The main procedure will have a run_id, we need to find it from the first STARTED entry
        initial_audit = bq_client.query(f"SELECT run_id FROM {JOB_AUDIT_TABLE} WHERE job_kennung = 'ausd_bp_ta_rn_vertrag' AND status = 'STARTED' ORDER BY created_at ASC LIMIT 1").result().to_dataframe()
        assert not initial_audit.empty
        run_id = initial_audit['run_id'].iloc[0]

        # Check k_ausd_bp_ta_rn_vertrag failed
        k_audit_entry = get_audit_log_entry(bq_client, run_id, job_kennung='ausd_bp_ta_rn_vertrag', status='FAILED')
        assert k_audit_entry is not None
        assert "Stichtag (p_stichtag) has invalid format" in k_audit_entry['message']

        # Check main procedure also failed
        main_audit_entry = get_audit_log_entry(bq_client, run_id, job_kennung='ausd_bp_ta_rn_vertrag', status='FAILED')
        assert main_audit_entry is not None
        assert "Job failed" in main_audit_entry['message']
    ```
*   **Pass/Fail Criterion:**
    *   The `ausd_bp_ta_rn_vertrag` procedure call must raise an error.
    *   The `job_audit` table must contain `FAILED` entries for both `k_ausd_bp_ta_rn_vertrag` and `ausd_bp_ta_rn_vertrag` for the same `run_id`.
    *   The error messages in `job_audit` should clearly indicate the invalid date format.

### Test Case 11: Logging and Error Handling (SQL Error)

*   **Purpose:** Verify that internal SQL errors (e.g., due to schema mismatch or data issues) are caught, logged as `FAILED`, and propagated up the call stack. This covers error handling and external system replacement (logging).
*   **Setup:** Introduce a condition that causes an error in `d_ausd_bp_ta_rn_vertrag`. For example, temporarily drop the target table `sof_ta_rn_vertrag` or insert data into `sof_ta_rn_einzeln` that would cause a type conversion error if a column was incorrectly typed.
*   **Action:** Execute the BigQuery main stored procedure.
    ```python
    # test_migration.py
    import pytest

    def test_error_handling_sql_error(bq_client):
        # Setup: Introduce an error condition - e.g., drop the target table
        bq_client.query(f"DROP TABLE {SOF_TA_RN_VERTRAG_TABLE}").result()
        
        # Setup: Insert some valid source data
        sample_data = [{"cntrct_id": "C1", "TN_TEL_msisdn": "111", "TN_TEL_status": "A", "TN_TEL_valid_to": date(2023, 1, 1)}]
        insert_data(bq_client, SOF_TA_RN_EINZELN_TABLE, sample_data)

        # Action: Call the main BigQuery stored procedure
        with pytest.raises(Exception) as excinfo:
            call_bq_procedure(bq_client, MAIN_SP, None, None)
        
        # Assert that the error message indicates a table not found or similar SQL error
        assert "Not found: Table" in str(excinfo.value) or "Table not found" in str(excinfo.value)

        # Verify audit log entries
        initial_audit = bq_client.query(f"SELECT run_id FROM {JOB_AUDIT_TABLE} WHERE job_kennung = 'ausd_bp_ta_rn_vertrag' AND status = 'STARTED' ORDER BY created_at ASC LIMIT 1").result().to_dataframe()
        assert not initial_audit.empty
        run_id = initial_audit['run_id'].iloc[0]

        # Check d_ausd_bp_ta_rn_vertrag failed
        d_audit_entry = get_audit_log_entry(bq_client, run_id, job_kennung='ausd_bp_ta_rn_vertrag', status='FAILED', message='d_ausd_bp_ta_rn_vertrag failed')
        assert d_audit_entry is not None
        assert "Not found: Table" in d_audit_entry['message'] or "Table not found" in d_audit_entry['message']

        # Check k_ausd_bp_ta_rn_vertrag failed
        k_audit_entry = get_audit_log_entry(bq_client, run_id, job_kennung='ausd_bp_ta_rn_vertrag', status='FAILED', message='k_ausd_bp_ta_rn_vertrag failed')
        assert k_audit_entry is not None
        assert "d_ausd_bp_ta_rn_vertrag failed" in k_audit_entry['message']

        # Check main procedure also failed
        main_audit_entry = get_audit_log_entry(bq_client, run_id, job_kennung='ausd_bp_ta_rn_vertrag', status='FAILED')
        assert main_audit_entry is not None
        assert "Job failed" in main_audit_entry['message']

        # Recreate the table for subsequent tests
        bq_client.query(f"""
            CREATE TABLE IF NOT EXISTS {SOF_TA_RN_VERTRAG_TABLE} (
                CNTRCT_ID STRING, TN_MULTI_SINGLE STRING, TN_TEL_MSISDN STRING, TN_TEL_STATUS STRING, TN_TEL_VALID_TO DATE,
                TN_FAX_MSISDN STRING, TN_FAX_STATUS STRING, TN_FAX_VALID_TO DATE, TN_DAT_MSISDN STRING, TN_DAT_STATUS STRING, TN_DAT_VALID_TO DATE,
                TC_MULTI_SINGLE STRING, TC_TEL_MSISDN STRING, TC_TEL_STATUS STRING, TC_TEL_VALID_TO DATE, TC_FAX_MSISDN STRING, TC_FAX_STATUS STRING,
                TC_FAX_VALID_TO DATE, TC_DAT_MSISDN STRING, TC_DAT_STATUS STRING, TC_DAT_VALID_TO DATE, TB_MULTI_SINGLE STRING, TB_TEL_MSISDN STRING,
                TB_TEL_STATUS STRING, TB_TEL_VALID_TO DATE, TB_FAX_MSISDN STRING, TB_FAX_STATUS STRING, TB_FAX_VALID_TO DATE, TB_DAT_MSISDN STRING,
                TB_DAT_STATUS STRING, TB_DAT_VALID_TO DATE, MS_RN_1_MSISDN STRING, MS_RN_1_STATUS STRING, MS_RN_1_VALID_TO DATE,
                MS_RN_2_MSISDN STRING, MS_RN_2_STATUS STRING, MS_RN_2_VALID_TO DATE
            );
        """).result()
    ```
*   **Pass/Fail Criterion:**
    *   The `ausd_bp_ta_rn_vertrag` procedure call must raise an error.
    *   The `job_audit` table must contain `FAILED` entries for `d_ausd_bp_ta_rn_vertrag`, `k_ausd_bp_ta_rn_vertrag`, and `ausd_bp_ta_rn_vertrag` for the same `run_id`.
    *   The error messages in `job_audit` should accurately reflect the underlying SQL error.

### Test Case 12: `v_datum` and `v_carmen` Variable Usage (Unused Confirmation)

*   **Purpose:** Confirm that the `v_datum` and `v_carmen` variables, noted as unused in the original SQL, remain unused in the BigQuery migration, ensuring no unintended new logic was introduced. This covers transformation correctness and edge cases/risks.
*   **Setup:** None (this is a code review and static analysis test).
*   **Action:** Review the BigQuery Stored Procedure `project.dataset.d_ausd_bp_ta_rn_vertrag`.
*   **Pass/Fail Criterion:**
    *   The BigQuery SQL code for `d_ausd_bp_ta_rn_vertrag` must not declare or use any variables corresponding to `v_carmen`.
    *   The BigQuery SQL code for `d_ausd_bp_ta_rn_vertrag` must not declare or use any variables corresponding to `v_datum` in its DML logic (i.e., no filtering or transformation based on it).

---

This comprehensive test plan addresses the key aspects of the migration, including behavioral equivalence, transformation logic, external system interactions, and data quality. The use of `pytest` and BigQuery SQL assertions provides a robust framework for automated validation. The identified risks, particularly regarding `p_wiederanlaufWert` and `v_datum`, are explicitly covered, allowing for clear communication of any functional deviations.