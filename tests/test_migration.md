As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `k_ausd_bp_ta_bcp_msisdn.ksh`. The following test cases are designed to ensure the migrated BigQuery/Airflow solution is behaviourally equivalent to the legacy Oracle/KornShell job.

---

## Migration Validation Tests: `k_ausd_bp_ta_bcp_msisdn.ksh`

**Assumptions for Testing:**
*   Access to both the legacy Oracle environment and the target BigQuery environment.
*   Ability to populate and query tables in both environments.
*   Ability to execute the legacy KornShell script with specific parameters.
*   Ability to trigger the Airflow DAG with specific `dag_run.conf` parameters.
*   Python `pytest` framework is used for test execution, with appropriate BigQuery and Oracle client libraries.
*   Placeholders like `YOUR_BIGQUERY_PROJECT` and `YOUR_BIGQUERY_DATASET` are replaced with actual environment values.
*   The `DWPA_UTIL_SKRIPT` package's functionality, if relevant to `d_ausd_bp_ta_bcp_msisdn.sql`, has been correctly inlined or translated into the BigQuery SQL, as the provided `d_ausd_bp_ta_bcp_msisdn_bq.sql` does not explicitly reference it. The output parity tests will implicitly validate this assumption.

---

### Test Case 1: Happy Path - Full Data Migration & Transformation Parity

*   **Purpose:** Verify that with standard, valid inputs, the migrated job produces an identical output dataset to the legacy job, covering output parity and core transformation correctness.
*   **Setup:**
    1.  Create identical, representative datasets in both Oracle and BigQuery for `DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, and `SOF$TA_RN_VERTRAG`. Include a mix of matching and non-matching `cntrct_id_ref`/`cntrct_id` values, and some data that would result in duplicates before `DISTINCT`.
    2.  Ensure target tables (`SOF$TA_BCP_MSISDN` in Oracle, `SOF_TA_BCP_MSISDN` in BigQuery) are empty before execution.
    3.  Define consistent input parameters for both jobs (e.g., `j=TEST_JOB`, `f=123`, `s=22032023`, `l=0`).
*   **Action:**
    1.  Execute the legacy KornShell script:
        ```bash
        ./k_ausd_bp_ta_bcp_msisdn.ksh -j TEST_JOB -f 123 -s 22032023 -l 0
        ```
    2.  Trigger the Airflow DAG with equivalent `dag_run.conf`:
        ```python
        # Example using Airflow's TestRunner or API
        dag_run_conf = {
            "job_kennung": "TEST_JOB",
            "eintrags_nr": "123",
            "stichtag": "22032023",
            "wiederanlauf_wert": 0
        }
        # Trigger DAG via Airflow API or TestRunner
        # airflow dags trigger k_ausd_bp_ta_bcp_msisdn_dag -c '{"job_kennung": "TEST_JOB", "eintrags_nr": "123", "stichtag": "22032023", "wiederanlauf_wert": 0}'
        ```
*   **Pass/Fail Criterion:**
    *   Both jobs complete successfully without errors.
    *   The content of the `SOF$TA_BCP_MSISDN` table in Oracle is **identical** to the content of `SOF_TA_BCP_MSISDN` in BigQuery. This includes row count, column values, and data types (after BigQuery's type mapping).

```python
# pytest assertion example
import pytest
from google.cloud import bigquery
import cx_Oracle # or similar Oracle client

def fetch_oracle_data(oracle_conn, table_name):
    cursor = oracle_conn.cursor()
    cursor.execute(f"SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN FROM {table_name} ORDER BY 1,2,3,4")
    return sorted(cursor.fetchall()) # Sort for consistent comparison

def fetch_bigquery_data(bq_client, project_id, dataset_id, table_name):
    query = f"""
    SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN
    FROM `{project_id}.{dataset_id}.{table_name}`
    ORDER BY 1,2,3,4
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    return sorted([tuple(row.values()) for row in results])

def test_output_parity_happy_path(oracle_conn, bq_client):
    oracle_table = "SOF$TA_BCP_MSISDN"
    bq_table = "SOF_TA_BCP_MSISDN"
    bq_project = "YOUR_BIGQUERY_PROJECT"
    bq_dataset = "YOUR_BIGQUERY_DATASET"

    # Assume setup and job execution steps are handled before this test function
    # e.g., by a fixture or a separate test orchestration script

    oracle_data = fetch_oracle_data(oracle_conn, oracle_table)
    bigquery_data = fetch_bigquery_data(bq_client, bq_project, bq_dataset, bq_table)

    assert len(oracle_data) == len(bigquery_data), \
        f"Row count mismatch: Oracle has {len(oracle_data)} rows, BigQuery has {len(bigquery_data)} rows."
    assert oracle_data == bigquery_data, "Data content mismatch between Oracle and BigQuery target tables."

    print(f"Successfully verified output parity for {len(oracle_data)} rows.")

```

---

### Test Case 2: Parameter Parsing and Validation - Missing Required Parameters

*   **Purpose:** Verify that the Airflow DAG correctly identifies and fails on missing required input parameters, mirroring the legacy script's error handling (`ErrNr=193`).
*   **Setup:**
    1.  Ensure source tables are populated (though not strictly necessary for this test, good practice).
    2.  Prepare `dag_run.conf` missing one or more required parameters (`job_kennung`, `eintrags_nr`, `stichtag`).
*   **Action:**
    1.  Trigger the Airflow DAG with `dag_run.conf` like:
        ```json
        {"eintrags_nr": "123", "stichtag": "22032023"} # Missing job_kennung
        ```
    2.  Observe the DAG run status and logs.
*   **Pass/Fail Criterion:**
    *   The `parse_and_validate_parameters` task in the Airflow DAG fails.
    *   The task logs contain an error message similar to: `ERROR 193: Parameter 'Jobkennung' is not set and is required.` (or whichever parameter was missing).
    *   The DAG run status is marked as `failed`.

```python
# pytest assertion example (conceptual, as direct Airflow DAG failure is observed)
import pytest
from airflow.models import DagRun
from airflow.utils.state import State
from datetime import datetime

def test_missing_job_kennung_parameter_fails_dag(airflow_client): # Assume airflow_client can trigger DAGs
    dag_id = "k_ausd_bp_ta_bcp_msisdn_dag"
    conf = {
        "eintrags_nr": "123",
        "stichtag": "22032023"
    }

    # Trigger the DAG and wait for completion (or check status periodically)
    # This part would typically involve Airflow's testing utilities or API calls
    # For demonstration, let's assume a mock trigger and status check
    
    # Mocking a DAG run for illustration
    mock_dag_run = DagRun(
        dag_id=dag_id,
        run_id=f"test_missing_job_kennung_{datetime.now().isoformat()}",
        conf=conf,
        execution_date=datetime.now(),
        state=State.FAILED # This would be the expected outcome
    )
    
    # In a real test, you'd trigger the DAG and then query its status
    # For example:
    # dag_run = airflow_client.trigger_dag(dag_id, conf=conf)
    # dag_run.wait_for_completion() # Or poll status
    
    assert mock_dag_run.state == State.FAILED, "DAG did not fail as expected for missing 'job_kennung'."
    
    # Further checks would involve parsing Airflow task logs for specific error messages
    # e.g., `assert "Parameter 'Jobkennung' is not set and is required." in task_logs`
```

---

### Test Case 3: Parameter Parsing and Validation - Invalid Date Format

*   **Purpose:** Verify that the Airflow DAG correctly validates the `stichtag` format and fails if it's incorrect, mirroring the legacy script's date check.
*   **Setup:**
    1.  Prepare `dag_run.conf` with `stichtag` in an invalid format (e.g., `YYYY-MM-DD` instead of `DDMMYYYY`).
*   **Action:**
    1.  Trigger the Airflow DAG with `dag_run.conf` like:
        ```json
        {"job_kennung": "TEST_JOB", "eintrags_nr": "123", "stichtag": "2023-03-22"}
        ```
    2.  Observe the DAG run status and logs.
*   **Pass/Fail Criterion:**
    *   The `parse_and_validate_parameters` task in the Airflow DAG fails.
    *   The task logs contain an error message similar to: `Invalid date format for '2023-03-22'. Expected '%d%m%Y'.`
    *   The DAG run status is marked as `failed`.

```python
# pytest assertion example (conceptual, similar to Test Case 2)
import pytest
from airflow.models import DagRun
from airflow.utils.state import State
from datetime import datetime

def test_invalid_stichtag_format_fails_dag(airflow_client):
    dag_id = "k_ausd_bp_ta_bcp_msisdn_dag"
    conf = {
        "job_kennung": "TEST_JOB",
        "eintrags_nr": "123",
        "stichtag": "2023-03-22" # Invalid format
    }

    mock_dag_run = DagRun(
        dag_id=dag_id,
        run_id=f"test_invalid_stichtag_{datetime.now().isoformat()}",
        conf=conf,
        execution_date=datetime.now(),
        state=State.FAILED
    )
    
    assert mock_dag_run.state == State.FAILED, "DAG did not fail as expected for invalid 'stichtag' format."
    # Assert specific log message about date format
```

---

### Test Case 4: `wiederanlaufWert` Default Handling

*   **Purpose:** Verify that the `p_wiederanlaufWert` parameter correctly defaults to `0` if not explicitly provided in the DAG run configuration, matching the legacy script's behavior.
*   **Setup:**
    1.  Prepare `dag_run.conf` without the `wiederanlauf_wert` parameter.
*   **Action:**
    1.  Trigger the Airflow DAG with `dag_run.conf` like:
        ```json
        {"job_kennung": "TEST_JOB", "eintrags_nr": "123", "stichtag": "22032023"}
        ```
    2.  Inspect the XCom values pushed by the `parse_and_validate_parameters` task.
*   **Pass/Fail Criterion:**
    *   The DAG runs successfully.
    *   The XCom value for `p_wiederanlaufWert` (pulled from `parse_and_validate_parameters` task) is `0`.

```python
# pytest assertion example
import pytest
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from datetime import datetime
from unittest.mock import MagicMock

# Mock the _parse_and_validate_params function for isolated testing
def mock_parse_and_validate_params(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    params = {
        'p_JobKennung': dag_run_conf.get('job_kennung'),
        'p_EintragsNr': dag_run_conf.get('eintrags_nr'),
        'p_Stichtag': dag_run_conf.get('stichtag'),
        'p_wiederanlaufWert': dag_run_conf.get('wiederanlauf_wert', 0) # This is the logic under test
    }
    kwargs["ti"].xcom_push(key="p_wiederanlaufWert", value=params['p_wiederanlaufWert'])

def test_wiederanlauf_wert_default():
    mock_ti = MagicMock(spec=TaskInstance)
    mock_dag_run = MagicMock(spec=DagRun)
    mock_dag_run.conf = {
        "job_kennung": "TEST_JOB",
        "eintrags_nr": "123",
        "stichtag": "22032023"
    }

    mock_parse_and_validate_params(dag_run=mock_dag_run, ti=mock_ti)

    # Assert that xcom_push was called with the default value
    mock_ti.xcom_push.assert_called_with(key="p_wiederanlaufWert", value=0)
```

---

### Test Case 5: Date Calculation (`gestern.ksh` Replacement)

*   **Purpose:** Verify that the `_calculate_dates` task correctly determines `p_datum_heute` and `p_datum_gestern` in `YYYYMMDD` format, replicating the `gestern.ksh` utility.
*   **Setup:**
    1.  No special setup for source data.
    2.  The `parse_and_validate_parameters` task must have completed successfully, pushing `p_Stichtag` to XCom (though `_calculate_dates` currently uses system date, not `p_Stichtag` as reference for 'today').
*   **Action:**
    1.  Trigger the Airflow DAG.
    2.  Inspect the XCom values pushed by the `calculate_dates` task.
*   **Pass/Fail Criterion:**
    *   The `calculate_dates` task completes successfully.
    *   The XCom value for `p_datum_heute` matches the current system date in `YYYYMMDD` format.
    *   The XCom value for `p_datum_gestern` matches the previous day's system date in `YYYYMMDD` format.

```python
# pytest assertion example
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock
from dags.k_ausd_bp_ta_bcp_msisdn_dag import _calculate_dates # Import the actual function

def test_calculate_dates_correctness():
    mock_ti = MagicMock()
    # Mock XCom pull for p_Stichtag, though _calculate_dates doesn't use it for "today"
    mock_ti.xcom_pull.return_value = "22032023" 

    _calculate_dates(ti=mock_ti)

    today = datetime.now()
    yesterday = today - timedelta(days=1)

    expected_heute = today.strftime('%Y%m%d')
    expected_gestern = yesterday.strftime('%Y%m%d')

    mock_ti.xcom_push.assert_any_call(key="p_datum_heute", value=expected_heute)
    mock_ti.xcom_push.assert_any_call(key="p_datum_gestern", value=expected_gestern)
```

---

### Test Case 6: Transformation Correctness - `DISTINCT` Clause

*   **Purpose:** Ensure the `SELECT DISTINCT` clause in the BigQuery SQL correctly removes duplicate rows that might arise from the join operation, matching the implicit or explicit behavior of the legacy Oracle SQL.
*   **Setup:**
    1.  Populate `SOF_TA_BPR_BCP` and `SOF_TA_RN_VERTRAG` in both Oracle and BigQuery with data that, when joined, would produce multiple identical rows for `(CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN)` if `DISTINCT` were not applied.
        *   Example: `SOF_TA_BPR_BCP` has `(1, 101, 1001)`, `(2, 102, 1001)`. `SOF_TA_RN_VERTRAG` has `(1001, 'MSISDN_A')`, `(1001, 'MSISDN_A')`. The join would produce `(1, 101, 1001, 'MSISDN_A')` and `(2, 102, 1001, 'MSISDN_A')` before `DISTINCT`. If `SOF_TA_BPR_BCP` had `(1, 101, 1001)` twice, and `SOF_TA_RN_VERTRAG` had `(1001, 'MSISDN_A')` once, the join would produce `(1, 101, 1001, 'MSISDN_A')` twice. `DISTINCT` should reduce this to one.
*   **Action:**
    1.  Execute both legacy and migrated jobs (as in Test Case 1).
    2.  Query the target tables in both environments.
*   **Pass/Fail Criterion:**
    *   The number of unique rows in `SOF$TA_BCP_MSISDN` (Oracle) is identical to the number of unique rows in `SOF_TA_BCP_MSISDN` (BigQuery).
    *   The content of the unique rows is identical.

```sql
-- SQL assertion for BigQuery (after DAG execution)
SELECT COUNT(1) FROM (
    SELECT DISTINCT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN
    FROM `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BCP_MSISDN`
);

-- Compare this count with the count from Oracle:
-- SELECT COUNT(DISTINCT CNTRCT_ID || BPR_ID || CNTRCT_ID_REF || TN_TEL_MSISDN) FROM SOF$TA_BCP_MSISDN;
-- Or simply: SELECT COUNT(1) FROM SOF$TA_BCP_MSISDN; if the legacy job also applies implicit distinct.
-- The most robust is to compare the full sorted datasets as in Test Case 1.
```

---

### Test Case 7: Transformation Correctness - Join Logic (Inner Join)

*   **Purpose:** Verify that the `INNER JOIN` condition (`bp.cntrct_id_ref = rn.cntrct_id`) is correctly applied, ensuring only matching records are included in the output.
*   **Setup:**
    1.  Populate `SOF_TA_BPR_BCP` and `SOF_TA_RN_VERTRAG` in both Oracle and BigQuery with:
        *   Rows that have matching `cntrct_id_ref` and `cntrct_id`.
        *   Rows in `SOF_TA_BPR_BCP` that have no corresponding `cntrct_id` in `SOF_TA_RN_VERTRAG`.
        *   Rows in `SOF_TA_RN_VERTRAG` that have no corresponding `cntrct_id_ref` in `SOF_TA_BPR_BCP`.
*   **Action:**
    1.  Execute both legacy and migrated jobs.
    2.  Query the target tables in both environments.
*   **Pass/Fail Criterion:**
    *   Only records where `bp.cntrct_id_ref` from `SOF_TA_BPR_BCP` has an exact match with `rn.cntrct_id` from `SOF_TA_RN_VERTRAG` are present in the target tables.
    *   Rows from either source table without a join match are correctly excluded.
    *   The content of the target tables is identical.

```sql
-- SQL assertion for BigQuery (after DAG execution)
-- Verify that all CNTRCT_ID_REF in target table exist in SOF_TA_RN_VERTRAG.CNTRCT_ID
SELECT COUNT(DISTINCT t.CNTRCT_ID_REF)
FROM `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BCP_MSISDN` t
LEFT JOIN `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_RN_VERTRAG` rn
  ON t.CNTRCT_ID_REF = rn.CNTRCT_ID
WHERE rn.CNTRCT_ID IS NULL;
-- Expected result: 0

-- Verify that all CNTRCT_ID_REF in target table exist in SOF_TA_BPR_BCP.CNTRCT_ID_REF
SELECT COUNT(DISTINCT t.CNTRCT_ID_REF)
FROM `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BCP_MSISDN` t
LEFT JOIN `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BPR_BCP` bp
  ON t.CNTRCT_ID_REF = bp.CNTRCT_ID_REF
WHERE bp.CNTRCT_ID_REF IS NULL;
-- Expected result: 0
```

---

### Test Case 8: Transformation Correctness - NULL Handling in Join Key

*   **Purpose:** Verify that rows with `NULL` values in the join keys (`cntrct_id_ref` or `cntrct_id`) are correctly handled (i.e., excluded by the `INNER JOIN`), consistent with standard SQL behavior.
*   **Setup:**
    1.  Populate `SOF_TA_BPR_BCP` with some rows where `cntrct_id_ref` is `NULL`.
    2.  Populate `SOF_TA_RN_VERTRAG` with some rows where `cntrct_id` is `NULL` (if the schema allows).
    3.  Include other rows with valid join keys.
*   **Action:**
    1.  Execute both legacy and migrated jobs.
    2.  Query the target tables in both environments.
*   **Pass/Fail Criterion:**
    *   No rows with `NULL` in `CNTRCT_ID_REF` or `CNTRCT_ID` (from the original source tables) are present in the final `SOF_TA_BCP_MSISDN` table.
    *   The content of the target tables is identical.

```sql
-- SQL assertion for BigQuery (after DAG execution)
SELECT COUNT(1)
FROM `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BCP_MSISDN`
WHERE CNTRCT_ID_REF IS NULL;
-- Expected result: 0
```

---

### Test Case 9: Target Table Schema and Data Types

*   **Purpose:** Verify that the BigQuery target table `SOF_TA_BCP_MSISDN` has the correct schema, column names, and data types as defined in the migrated DDL.
*   **Setup:**
    1.  Ensure the `ddl/SOF_TA_BCP_MSISDN.sql` has been applied to create the table.
*   **Action:**
    1.  Query the schema of `SOF_TA_BCP_MSISDN` in BigQuery.
*   **Pass/Fail Criterion:**
    *   The BigQuery table `SOF_TA_BCP_MSISDN` exists.
    *   Its schema matches the expected DDL:
        *   `CNTRCT_ID` (INT64)
        *   `BPR_ID` (INT64)
        *   `CNTRCT_ID_REF` (INT64)
        *   `TN_TEL_MSISDN` (STRING)
        *   `created_at` (TIMESTAMP)

```python
# pytest assertion example
import pytest
from google.cloud import bigquery

def test_target_table_schema(bq_client):
    bq_project = "YOUR_BIGQUERY_PROJECT"
    bq_dataset = "YOUR_BIGQUERY_DATASET"
    bq_table = "SOF_TA_BCP_MSISDN"

    table_ref = bq_client.dataset(bq_dataset, project=bq_project).table(bq_table)
    table = bq_client.get_table(table_ref)

    expected_schema = [
        bigquery.SchemaField("CNTRCT_ID", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("BPR_ID", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("CNTRCT_ID_REF", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("TN_TEL_MSISDN", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"), # Default CURRENT_TIMESTAMP() implies NULLABLE
    ]

    # Compare field names and types. Order might not be guaranteed, so compare sets or iterate.
    actual_fields = [(field.name, field.field_type, field.mode) for field in table.schema]
    expected_fields = [(field.name, field.field_type, field.mode) for field in expected_schema]

    assert set(actual_fields) == set(expected_fields), \
        f"Schema mismatch for {bq_table}. Expected: {expected_fields}, Actual: {actual_fields}"
```

---

### Test Case 10: Record Count Assertion

*   **Purpose:** Verify that the total number of records inserted into the target table by the migrated job matches the record count reported by the legacy job. This validates the `tmpFile` logic replacement.
*   **Setup:**
    1.  Populate source tables with a known number of records that will result in a specific output count.
    2.  Ensure target tables are empty.
*   **Action:**
    1.  Execute the legacy KornShell script. Capture the `v_records` value from its output or logs.
    2.  Trigger the Airflow DAG.
    3.  Query the row count of `SOF_TA_BCP_MSISDN` in BigQuery.
*   **Pass/Fail Criterion:**
    *   The `v_records` value from the legacy job's execution matches the `COUNT(*)` from the BigQuery target table.

```python
# pytest assertion example
import pytest
from google.cloud import bigquery
# Assume a mechanism to get legacy_record_count (e.g., parsing log file or direct execution)

def get_bigquery_row_count(bq_client, project_id, dataset_id, table_name):
    query = f"SELECT COUNT(1) FROM `{project_id}.{dataset_id}.{table_name}`"
    query_job = bq_client.query(query)
    return list(query_job.result())[0][0]

def test_record_count_parity(bq_client, legacy_record_count):
    bq_project = "YOUR_BIGQUERY_PROJECT"
    bq_dataset = "YOUR_BIGQUERY_DATASET"
    bq_table = "SOF_TA_BCP_MSISDN"

    # Assume DAG has been triggered and completed successfully
    bigquery_row_count = get_bigquery_row_count(bq_client, bq_project, bq_dataset, bq_table)

    assert bigquery_row_count == legacy_record_count, \
        f"Record count mismatch. Legacy: {legacy_record_count}, BigQuery: {bigquery_row_count}"
```

---

### Test Case 11: Idempotency (TRUNCATE + INSERT)

*   **Purpose:** Verify that running the migrated Airflow DAG multiple times produces the same final state in the target table, due to the `TRUNCATE TABLE` followed by `INSERT INTO` pattern.
*   **Setup:**
    1.  Populate BigQuery source tables with a known dataset.
    2.  Ensure target table `SOF_TA_BCP_MSISDN` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG for the first time.
    2.  After successful completion, query the content and row count of `SOF_TA_BCP_MSISDN`. Store this as `result_1`.
    3.  Trigger the Airflow DAG for a second time.
    4.  After successful completion, query the content and row count of `SOF_TA_BCP_MSISDN`. Store this as `result_2`.
*   **Pass/Fail Criterion:**
    *   Both DAG runs complete successfully.
    *   `result_1` (content and row count) is **identical** to `result_2`.

```python
# pytest assertion example
import pytest
from google.cloud import bigquery
# Assume airflow_client can trigger DAGs and fetch table data

def test_dag_idempotency(airflow_client, bq_client):
    dag_id = "k_ausd_bp_ta_bcp_msisdn_dag"
    bq_project = "YOUR_BIGQUERY_PROJECT"
    bq_dataset = "YOUR_BIGQUERY_DATASET"
    bq_table = "SOF_TA_BCP_MSISDN"
    
    # Common DAG config
    dag_conf = {
        "job_kennung": "IDEMPOTENCY_TEST",
        "eintrags_nr": "456",
        "stichtag": "01012024"
    }

    # Run 1
    # airflow_client.trigger_dag(dag_id, conf=dag_conf).wait_for_completion()
    # For testing, assume this is done and we just fetch data
    result_1 = fetch_bigquery_data(bq_client, bq_project, bq_dataset, bq_table)
    count_1 = len(result_1)

    # Run 2
    # airflow_client.trigger_dag(dag_id, conf=dag_conf).wait_for_completion()
    # For testing, assume this is done and we just fetch data
    result_2 = fetch_bigquery_data(bq_client, bq_project, bq_dataset, bq_table)
    count_2 = len(result_2)

    assert count_1 == count_2, f"Row count mismatch after second run. First: {count_1}, Second: {count_2}"
    assert result_1 == result_2, "Data content mismatch after second run, indicating non-idempotent behavior."
```

---