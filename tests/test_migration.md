As a senior data-migration QA engineer, I have reviewed the `DW.BERT_AUSD_BP_TA_BCP_MSISDN` migration design document. The following test cases are designed to ensure the migrated job is behaviourally equivalent to its legacy counterpart, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for DW.BERT_AUSD_BP_TA_BCP_MSISDN

### Test Case 1: End-to-End Output Parity (Golden Dataset)

*   **Purpose:** To verify that for an identical set of input data, the migrated BigQuery job produces an output table (`sof_ta_bcp_msisdn`) that is byte-for-byte identical to the output produced by the legacy Oracle job. This is the primary validation of overall correctness.
*   **Setup:**
    1.  **Prepare Golden Dataset:** Select a representative, non-trivial dataset from the legacy Oracle source tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `isbert_schema.dwtk_meldungen`). This dataset should include various scenarios like matching records, non-matching records, and records with NULL values in `tn_tel_msisdn`.
    2.  **Load Legacy Sources:** Load this golden dataset into the legacy Oracle source tables.
    3.  **Load Migrated Sources:** Load the *exact same* golden dataset into the corresponding BigQuery source tables (`your_project.bert_raw.sof_ta_bpr_bcp`, `your_project.bert_raw.sof_ta_rn_vertrag`, `your_project.bert_raw.dwtk_meldungen`).
    4.  **Run Legacy Job:** Execute the legacy `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job in the Oracle/UNIX environment.
    5.  **Run Migrated Job:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` in Airflow, ensuring it processes the BigQuery golden dataset.
*   **Action:**
    1.  Extract the full content of the legacy target table (`legacy_oracle.sof$ta_bcp_msisdn`).
    2.  Extract the full content of the migrated target table (`your_project.bert_raw.sof_ta_bcp_msisdn`).
    3.  Compare the two datasets row by row, column by column.
*   **Pass/Fail Criterion:** The row count and the content of all columns in `your_project.bert_raw.sof_ta_bcp_msisdn` must be *identical* to `legacy_oracle.sof$ta_bcp_msisdn`.

```python
# Example Python (pytest) assertion for output parity
import pandas as pd
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for legacy DB access

def test_output_parity_golden_dataset():
    # --- Setup (simplified for example, actual data loading would be more complex) ---
    # Assume legacy_df and migrated_df are populated from the respective databases
    # For demonstration, let's create dummy dataframes
    legacy_data = {
        'CNTRCT_ID': [101, 102, 103],
        'BPR_ID': ['B1', 'B2', 'B3'],
        'CNTRCT_ID_REF': [1001, 1002, 1003],
        'TN_TEL_MSISDN': ['+1234567890', '+1234567891', None]
    }
    migrated_data = {
        'CNTRCT_ID': [101, 102, 103],
        'BPR_ID': ['B1', 'B2', 'B3'],
        'CNTRCT_ID_REF': [1001, 1002, 1003],
        'TN_TEL_MSISDN': ['+1234567890', '+1234567891', None]
    }
    legacy_df = pd.DataFrame(legacy_data).sort_values(by=['CNTRCT_ID', 'BPR_ID']).reset_index(drop=True)
    migrated_df = pd.DataFrame(migrated_data).sort_values(by=['CNTRCT_ID', 'BPR_ID']).reset_index(drop=True)

    # In a real scenario, you'd fetch data like this:
    # oracle_conn = cx_Oracle.connect("user/pass@host:port/service_name")
    # legacy_df = pd.read_sql("SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN FROM legacy_oracle.sof$ta_bcp_msisdn ORDER BY CNTRCT_ID, BPR_ID", oracle_conn)
    # oracle_conn.close()

    # bq_client = bigquery.Client(project='your_project')
    # query = "SELECT cntrct_id, bpr_id, cntrct_id_ref, tn_tel_msisdn FROM `your_project.bert_raw.sof_ta_bcp_msisdn` ORDER BY cntrct_id, bpr_id"
    # migrated_df = bq_client.query(query).to_dataframe()

    # --- Action & Assertion ---
    pd.testing.assert_frame_equal(legacy_df, migrated_df, check_dtype=True, check_exact=True)
```

### Test Case 2: Transformation Correctness - Join Logic

*   **Purpose:** To specifically validate that the `INNER JOIN` condition (`bp.cntrct_id_ref = rv.cntrct_id`) is correctly translated and executed in BigQuery, handling matching and non-matching records as per SQL standard.
*   **Setup:**
    1.  **Populate Source Tables (BigQuery):**
        *   `your_project.bert_raw.sof_ta_bpr_bcp`:
            *   Record A: `cntrct_id_ref = 1001` (matching)
            *   Record B: `cntrct_id_ref = 1002` (matching)
            *   Record C: `cntrct_id_ref = 1003` (no match in `sof_ta_rn_vertrag`)
            *   Record D: `cntrct_id_ref = NULL`
        *   `your_project.bert_raw.sof_ta_rn_vertrag`:
            *   Record X: `cntrct_id = 1001` (matching)
            *   Record Y: `cntrct_id = 1002` (matching)
            *   Record Z: `cntrct_id = 1004` (no match in `sof_ta_bpr_bcp`)
            *   Record W: `cntrct_id = NULL`
    2.  **Run Migrated Job:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` in Airflow.
*   **Action:** Query the `your_project.bert_raw.sof_ta_bcp_msisdn` table.
*   **Pass/Fail Criterion:**
    *   The output table must contain records corresponding to the successful joins (e.g., A-X, B-Y).
    *   Records C, D, Z, W (those without a join partner or with NULL join keys) must *not* appear in the output.
    *   The total row count should match the number of successful inner joins.

```sql
-- BigQuery SQL Assertion for Join Logic
-- Expected result for the setup: 2 rows (from A-X and B-Y joins)
SELECT
    COUNT(1) AS actual_row_count,
    SUM(CASE WHEN cntrct_id_ref = 1001 THEN 1 ELSE 0 END) AS count_1001,
    SUM(CASE WHEN cntrct_id_ref = 1002 THEN 1 ELSE 0 END) AS count_1002,
    SUM(CASE WHEN cntrct_id_ref = 1003 THEN 1 ELSE 0 END) AS count_1003_nomatch,
    SUM(CASE WHEN cntrct_id_ref IS NULL THEN 1 ELSE 0 END) AS count_null_ref
FROM
    `your_project.bert_raw.sof_ta_bcp_msisdn`;

-- Expected Output:
-- actual_row_count | count_1001 | count_1002 | count_1003_nomatch | count_null_ref
-- -----------------|------------|------------|--------------------|---------------
-- 2                | 1          | 1          | 0                  | 0
```

### Test Case 3: Transformation Correctness - Column Selection & Data Types

*   **Purpose:** To ensure that the correct columns (`cntrct_id`, `bpr_id`, `cntrct_id_ref`, `tn_tel_msisdn`) are selected and their data types are correctly mapped and preserved in the BigQuery target table.
*   **Setup:**
    1.  **Populate Source Tables (BigQuery):** Load `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` with data that includes diverse values for each selected column, including maximum lengths for strings, large numbers for IDs, and NULLs for `tn_tel_msisdn`.
    2.  **Run Migrated Job:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` in Airflow.
*   **Action:**
    1.  Query the schema of `your_project.bert_raw.sof_ta_bcp_msisdn`.
    2.  Select all columns from `your_project.bert_raw.sof_ta_bcp_msisdn` and verify their values.
*   **Pass/Fail Criterion:**
    *   The schema of `your_project.bert_raw.sof_ta_bcp_msisdn` must contain exactly the four specified columns with appropriate BigQuery data types (e.g., `INTEGER` for IDs, `STRING` for `MSISDN`).
    *   The values in the output columns must exactly match the expected values derived from the source data and the join logic.

```sql
-- BigQuery SQL Assertion for Schema and Data Types
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your_project.bert_raw.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'sof_ta_bcp_msisdn'
    AND table_schema = 'bert_raw'
ORDER BY
    column_name;

-- Expected Output (example, assuming IDs are INT64 and MSISDN is STRING)
-- column_name   | data_type | is_nullable
-- --------------|-----------|------------
-- bpr_id        | STRING    | YES
-- cntrct_id     | INT64     | YES
-- cntrct_id_ref | INT64     | YES
-- tn_tel_msisdn | STRING    | YES

-- BigQuery SQL Assertion for Data Value Integrity (example for a specific record)
SELECT
    cntrct_id,
    bpr_id,
    cntrct_id_ref,
    tn_tel_msisdn
FROM
    `your_project.bert_raw.sof_ta_bcp_msisdn`
WHERE
    cntrct_id = <expected_cntrct_id_from_setup>;

-- Assert that the returned values match the expected values from the source data.
```

### Test Case 4: Transformation Correctness - `v_datum` Derivation

*   **Purpose:** To verify that the `v_datum` (metadata date) is correctly determined from `your_project.bert_raw.dwtk_meldungen` using `MAX(timecreated)` and any specified `job_kennung` filter, matching the legacy logic.
*   **Setup:**
    1.  **Populate `dwtk_meldungen` (BigQuery):**
        *   Insert multiple records into `your_project.bert_raw.dwtk_meldungen` with varying `timecreated` values.
        *   Include records for the specific `job_kennung` that the job is expected to filter on (e.g., 'BERT_MSISDN_JOB').
        *   Include records for other `job_kennung`s.
        *   Ensure there's a clear `MAX(timecreated)` for the target `job_kennung`.
    2.  **Run Migrated Job:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` in Airflow.
*   **Action:**
    1.  Inspect the Airflow XComs or logs for the `determine_metadata_date_task` to retrieve the `v_datum` value.
    2.  Manually execute the equivalent BigQuery query to determine the expected `MAX(timecreated)` for the relevant `job_kennung`.
*   **Pass/Fail Criterion:** The `v_datum` value determined by the `determine_metadata_date_task` in Airflow must be identical to the manually verified `MAX(timecreated)` from `your_project.bert_raw.dwtk_meldungen` for the specified `job_kennung`.

```python
# Example Python (pytest) assertion for v_datum
from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.models import XCom
from datetime import datetime

@provide_session
def get_xcom_value(dag_id, task_id, key, session=None):
    dr = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date.desc()).first()
    if dr:
        xcom = session.query(XCom).filter(
            XCom.dag_id == dag_id,
            XCom.task_id == task_id,
            XCom.run_id == dr.run_id,
            XCom.key == key
        ).first()
        if xcom:
            return xcom.value
    return None

def test_v_datum_derivation():
    # --- Setup ---
    # Assume BigQuery dwtk_meldungen is populated as described in the test case.
    # Manually determine expected_v_datum based on BigQuery query:
    # SELECT MAX(timecreated) FROM `your_project.bert_raw.dwtk_meldungen` WHERE job_kennung = 'BERT_MSISDN_JOB';
    expected_v_datum = datetime(2023, 10, 26, 10, 30, 0) # Example expected value

    # --- Action ---
    # Trigger Airflow DAG and wait for completion (not shown here)
    # Retrieve v_datum from XCom
    actual_v_datum_str = get_xcom_value(
        dag_id='bert_ausd_bp_ta_bcp_msisdn_dag',
        task_id='determine_metadata_date_task',
        key='return_value' # Assuming the task returns the date
    )
    actual_v_datum = datetime.fromisoformat(actual_v_datum_str) # Convert if stored as string

    # --- Assertion ---
    assert actual_v_datum == expected_v_datum, f"Expected v_datum {expected_v_datum}, but got {actual_v_datum}"
```

### Test Case 5: External System Replacement - BigQuery Read/Write Operations

*   **Purpose:** To confirm that the Airflow DAG successfully connects to and performs read operations on BigQuery source tables and write operations (truncate and insert) on the BigQuery target table. This validates the replacement of Oracle with BigQuery.
*   **Setup:**
    1.  **Ensure BigQuery Connectivity:** Verify that the Airflow environment has the necessary GCP credentials and BigQuery connection configured.
    2.  **Populate Source Tables:** Ensure `your_project.bert_raw.sof_ta_bpr_bcp`, `your_project.bert_raw.sof_ta_rn_vertrag`, and `your_project.bert_raw.dwtk_meldungen` are populated with some test data.
    3.  **Pre-populate Target Table:** Insert a few dummy rows into `your_project.bert_raw.sof_ta_bcp_msisdn` to confirm the `TRUNCATE` operation.
    4.  **Run Migrated Job:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` in Airflow.
*   **Action:**
    1.  Monitor the Airflow DAG run for successful task completion.
    2.  Check Airflow logs for BigQuery job IDs and any error messages.
    3.  Query the `your_project.bert_raw.sof_ta_bcp_msisdn` table after the job completes.
*   **Pass/Fail Criterion:**
    *   All Airflow tasks (`init_parameters_task`, `determine_metadata_date_task`, `transform_and_load_data_task`, `post_processing_task`) must complete successfully without errors.
    *   The `transform_and_load_data_task` must have executed a BigQuery job that first truncated and then inserted data into `your_project.bert_raw.sof_ta_bcp_msisdn`.
    *   The `sof_ta_bcp_msisdn` table must contain the data expected from the source tables, and none of the pre-populated dummy rows.

```python
# Example Python (pytest) assertion for BigQuery operations
from google.cloud import bigquery

def test_bigquery_read_write_operations():
    bq_client = bigquery.Client(project='your_project')
    target_table_id = "your_project.bert_raw.sof_ta_bcp_msisdn"

    # --- Setup (pre-populate target table) ---
    # This part would be executed before the DAG run
    bq_client.query(f"TRUNCATE TABLE `{target_table_id}`").result()
    bq_client.query(f"""
        INSERT INTO `{target_table_id}` (cntrct_id, bpr_id, cntrct_id_ref, tn_tel_msisdn)
        VALUES (999, 'DUMMY', 9999, '+0000000000')
    """).result()
    initial_count = bq_client.query(f"SELECT COUNT(1) FROM `{target_table_id}`").result().to_dataframe().iloc[0,0]
    assert initial_count == 1, "Pre-population failed"

    # --- Action ---
    # Trigger Airflow DAG and wait for completion (not shown here)

    # --- Assertion ---
    # Verify the dummy row is gone and new data is present
    final_count = bq_client.query(f"SELECT COUNT(1) FROM `{target_table_id}`").result().to_dataframe().iloc[0,0]
    dummy_row_exists = bq_client.query(f"SELECT COUNT(1) FROM `{target_table_id}` WHERE cntrct_id = 999").result().to_dataframe().iloc[0,0]

    assert dummy_row_exists == 0, "Dummy row was not truncated from target table."
    assert final_count > 0, "Target table is empty after job run, expected data."
    # Further assertions would compare actual data with expected data from source.
```

### Test Case 6: Data Quality - Row Count Assertion

*   **Purpose:** To verify that the total number of rows processed and loaded into the target table by the migrated job matches the row count from the legacy job for the same input data.
*   **Setup:**
    1.  **Prepare Golden Dataset:** Use the same golden dataset as in Test Case 1.
    2.  **Load Sources:** Load the golden dataset into both legacy Oracle and BigQuery source tables.
    3.  **Run Legacy Job:** Execute the legacy job.
    4.  **Run Migrated Job:** Trigger the Airflow DAG.
*   **Action:**
    1.  Count the rows in `legacy_oracle.sof$ta_bcp_msisdn`.
    2.  Count the rows in `your_project.bert_raw.sof_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:** The row count in `your_project.bert_raw.sof_ta_bcp_msisdn` must be *exactly equal* to the row count in `legacy_oracle.sof$ta_bcp_msisdn`.

```sql
-- BigQuery SQL Assertion for Row Count
SELECT COUNT(1) AS actual_row_count FROM `your_project.bert_raw.sof_ta_bcp_msisdn`;

-- Legacy Oracle SQL (for comparison)
SELECT COUNT(1) AS legacy_row_count FROM legacy_oracle.sof$ta_bcp_msisdn;

-- In Python (pytest)
def test_row_count_parity():
    # Assume legacy_row_count is obtained from Oracle
    legacy_row_count = 1000 # Example value

    bq_client = bigquery.Client(project='your_project')
    query = "SELECT COUNT(1) FROM `your_project.bert_raw.sof_ta_bcp_msisdn`"
    migrated_row_count = bq_client.query(query).result().to_dataframe().iloc[0,0]

    assert migrated_row_count == legacy_row_count, \
        f"Row count mismatch: Legacy={legacy_row_count}, Migrated={migrated_row_count}"
```

### Test Case 7: Data Quality - NULL Handling for `tn_tel_msisdn`

*   **Purpose:** To verify that `NULL` values in the `tn_tel_msisdn` column from `sof_ta_rn_vertrag` are correctly propagated to the target `sof_ta_bcp_msisdn` table.
*   **Setup:**
    1.  **Populate Source Tables (BigQuery):**
        *   `your_project.bert_raw.sof_ta_bpr_bcp`: Insert records with valid `cntrct_id_ref` values.
        *   `your_project.bert_raw.sof_ta_rn_vertrag`: Insert records that will successfully join, but where `tn_tel_msisdn` is explicitly `NULL` for some records, and valid for others.
    2.  **Run Migrated Job:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` in Airflow.
*   **Action:** Query `your_project.bert_raw.sof_ta_bcp_msisdn` and check the `tn_tel_msisdn` column.
*   **Pass/Fail Criterion:**
    *   Records where `tn_tel_msisdn` was `NULL` in the source `sof_ta_rn_vertrag` must have `NULL` in the target `sof_ta_bcp_msisdn`.
    *   Records where `tn_tel_msisdn` was populated in the source must have the correct value in the target.

```sql
-- BigQuery SQL Assertion for NULL handling
SELECT
    COUNT(1) AS total_rows,
    COUNT(CASE WHEN tn_tel_msisdn IS NULL THEN 1 END) AS null_msisdn_count,
    COUNT(CASE WHEN tn_tel_msisdn IS NOT NULL THEN 1 END) AS non_null_msisdn_count
FROM
    `your_project.bert_raw.sof_ta_bcp_msisdn`;

-- Expected Output (example, assuming 10 total rows, 3 with NULL MSISDN from setup)
-- total_rows | null_msisdn_count | non_null_msisdn_count
-- -----------|-------------------|----------------------
-- 10         | 3                 | 7
```

### Test Case 8: Edge Case - Empty Source Tables

*   **Purpose:** To verify the job's behavior when one or both of the primary source tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`) are empty. Given the `TRUNCATE` and `INNER JOIN` logic, the target table should always be empty in such scenarios.
*   **Setup:**
    1.  **Scenario A (Empty `sof_ta_bpr_bcp`):**
        *   `your_project.bert_raw.sof_ta_bpr_bcp`: Empty.
        *   `your_project.bert_raw.sof_ta_rn_vertrag`: Populated with valid data.
        *   `your_project.bert_raw.dwtk_meldungen`: Populated.
    2.  **Scenario B (Empty `sof_ta_rn_vertrag`):**
        *   `your_project.bert_raw.sof_ta_bpr_bcp`: Populated with valid data.
        *   `your_project.bert_raw.sof_ta_rn_vertrag`: Empty.
        *   `your_project.bert_raw.dwtk_meldungen`: Populated.
    3.  **Scenario C (Both Empty):**
        *   `your_project.bert_raw.sof_ta_bpr_bcp`: Empty.
        *   `your_project.bert_raw.sof_ta_rn_vertrag`: Empty.
        *   `your_project.bert_raw.dwtk_meldungen`: Populated.
    4.  **Run Migrated Job:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` for each scenario.
*   **Action:** After each scenario run, query the row count of `your_project.bert_raw.sof_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:** In all three scenarios (A, B, and C), the `your_project.bert_raw.sof_ta_bcp_msisdn` table must have a row count of `0`.

```sql
-- BigQuery SQL Assertion for Empty Source Tables
SELECT COUNT(1) AS actual_row_count FROM `your_project.bert_raw.sof_ta_bcp_msisdn`;

-- Expected Output for all scenarios:
-- actual_row_count
-- ----------------
-- 0
```

### Test Case 9: Airflow Orchestration - Parameter Handling and Date Logic

*   **Purpose:** To verify that the Airflow DAG correctly initializes parameters (e.g., `Stichtag`, `Wiederanlaufwert` if used for logic) and performs date calculations using Python's `datetime` module, replacing the legacy KornShell logic.
*   **Setup:**
    1.  **Configure DAG Run:** Trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` with specific `conf` parameters (e.g., `{"stichtag": "2023-01-01"}`).
    2.  **Populate `dwtk_meldungen`:** Ensure `your_project.bert_raw.dwtk_meldungen` has data that would result in a specific `v_datum` when queried.
*   **Action:**
    1.  Inspect the Airflow task logs for `init_parameters_task` and `determine_metadata_date_task`.
    2.  Check Airflow XComs for values passed between tasks (e.g., `v_datum`).
*   **Pass/Fail Criterion:**
    *   The `init_parameters_task` must correctly parse and log the input parameters.
    *   The `determine_metadata_date_task` must calculate and pass the `v_datum` value that matches the expected result based on the `dwtk_meldungen` content and the `MAX(timecreated)` logic.
    *   No errors related to date parsing or parameter handling should occur.

```python
# Example Python (pytest) assertion for parameter handling
from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.models import XCom
from datetime import datetime

@provide_session
def get_dag_run_conf(dag_id, session=None):
    dr = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date.desc()).first()
    return dr.conf if dr else {}

def test_airflow_parameter_handling():
    # --- Setup ---
    # Assume DAG is triggered with conf={"stichtag": "2023-01-01"}
    expected_stichtag = "2023-01-01"
    expected_v_datum = datetime(2023, 10, 26, 10, 30, 0) # From Test Case 4

    # --- Action ---
    # Trigger Airflow DAG and wait for completion (not shown here)
    dag_conf = get_dag_run_conf(dag_id='bert_ausd_bp_ta_bcp_msisdn_dag')
    actual_stichtag = dag_conf.get('stichtag')

    actual_v_datum_str = get_xcom_value( # Reusing helper from Test Case 4
        dag_id='bert_ausd_bp_ta_bcp_msisdn_dag',
        task_id='determine_metadata_date_task',
        key='return_value'
    )
    actual_v_datum = datetime.fromisoformat(actual_v_datum_str)

    # --- Assertion ---
    assert actual_stichtag == expected_stichtag, \
        f"Stichtag parameter mismatch: Expected {expected_stichtag}, Got {actual_stichtag}"
    assert actual_v_datum == expected_v_datum, \
        f"v_datum calculation mismatch: Expected {expected_v_datum}, Got {actual_v_datum}"
```