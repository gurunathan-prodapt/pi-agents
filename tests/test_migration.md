The migration of `k_ausd_bp_ta_bpr_apn.ksh` to Google Cloud Platform involves re-platforming the orchestration logic to Cloud Composer (Airflow) and the data processing to BigQuery Stored Procedures. The following tests are designed to ensure the migrated solution is behaviourally equivalent to the legacy system.

---

## Migration Validation Tests for `k_ausd_bp_ta_bpr_apn.ksh`

### Test 1: Output Parity - Core Transformation Logic

**Purpose:** To verify that the core data transformation logic, migrated from `d_ausd_bp_ta_bpr_apn.sql` to `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp`, produces identical output data in `prod_dw_isrpt.PoolBasisprodukt` as the legacy Oracle SQL script would produce in its `PoolBasisprodukt` table, given the same source data. This covers transformation correctness (joins, filters, aggregations).

**Setup:**
1.  **Legacy Environment:**
    *   Ensure the Oracle database contains source tables (`sof$ta_bpr_instance`, `sof$ta_apn_carmen`, `dwtk_meldungen`) populated with a specific, known dataset (e.g., a snapshot from a production environment or a carefully crafted test dataset).
    *   Ensure the `PoolBasisprodukt` table in Oracle is empty before execution.
2.  **Migrated Environment (BigQuery):**
    *   Create and populate BigQuery source tables (`prod_dw_source.sof_ta_bpr_instance`, `prod_dw_source.sof_ta_apn_carmen`, `prod_dw_source.dwtk_meldungen`) with data that is an exact replica of the Oracle source tables used in the legacy setup.
    *   Ensure the `prod_dw_isrpt.PoolBasisprodukt` BigQuery table is empty before execution.
    *   Deploy `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp` to BigQuery.

**Action:**
1.  **Legacy Execution:**
    *   Manually execute the core SQL script `d_ausd_bp_ta_bpr_apn.sql` directly against the Oracle database. Capture the resulting data in the Oracle `PoolBasisprodukt` table.
    *   *Note:* The original `k_ausd_bp_ta_bpr_apn.ksh` passes parameters to `d_ausd_bp_ta_bpr_apn.sql`. For this test, we assume `d_ausd_bp_ta_bpr_apn.sql` does not directly use these parameters for its core logic (as implied by the migrated `d_ausd_bp_ta_bpr_apn_sp` not using them). If it does, those parameters must be simulated or passed to the Oracle script.
2.  **Migrated Execution:**
    *   Execute the BigQuery Stored Procedure `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp` with dummy parameters (since they are not used by the SP's logic as provided).
    *   `CALL prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp('1', 'TEST_JOB', '20231026', 0, '20231027', '20231026');`

**Pass/Fail Criterion:**
*   The data content of `prod_dw_isrpt.PoolBasisprodukt` in BigQuery must be identical to the data content of `PoolBasisprodukt` in the Oracle legacy environment. This includes row count, column values, and data types.

**Runnable Test Code (SQL Assertions):**

```sql
-- After running both legacy and migrated processes:

-- 1. Compare row counts
SELECT
    (SELECT COUNT(*) FROM oracle_legacy_db.PoolBasisprodukt) AS legacy_count,
    (SELECT COUNT(*) FROM prod_dw_isrpt.PoolBasisprodukt) AS migrated_count;

-- Expected: legacy_count = migrated_count

-- 2. Compare data content (assuming a common way to represent data for comparison, e.g., CSV export or direct query if linked)
-- This example uses a full outer join to find discrepancies.
-- Replace `oracle_legacy_db.PoolBasisprodukt` with your actual legacy data source.
-- For a real test, you'd likely export both to CSV and use a diff tool, or load legacy data into a temporary BQ table.

WITH LegacyData AS (
    -- Simulate loading legacy data into a temporary BigQuery table for comparison
    -- In a real scenario, this would be a SELECT from your Oracle-to-BQ replicated table
    SELECT
        CAST(CNTRCT_ID AS STRING) AS CNTRCT_ID,
        CAST(BPR_ID AS INT64) AS BPR_ID,
        CAST(CNTRCT_ID_REF AS STRING) AS CNTRCT_ID_REF,
        CAST(ACCESS_POINT_NAME AS STRING) AS ACCESS_POINT_NAME
    FROM
        `your_gcp_project.temp_dataset.legacy_PoolBasisprodukt_snapshot` -- Assume legacy data is loaded here
),
MigratedData AS (
    SELECT
        CNTRCT_ID,
        BPR_ID,
        CNTRCT_ID_REF,
        ACCESS_POINT_NAME
    FROM
        prod_dw_isrpt.PoolBasisprodukt
)
SELECT
    'Discrepancy Found' AS status,
    COALESCE(L.CNTRCT_ID, M.CNTRCT_ID) AS CNTRCT_ID,
    COALESCE(L.BPR_ID, M.BPR_ID) AS BPR_ID,
    COALESCE(L.CNTRCT_ID_REF, M.CNTRCT_ID_REF) AS CNTRCT_ID_REF,
    COALESCE(L.ACCESS_POINT_NAME, M.ACCESS_POINT_NAME) AS ACCESS_POINT_NAME,
    CASE WHEN L.CNTRCT_ID IS NULL THEN 'Missing in Legacy'
         WHEN M.CNTRCT_ID IS NULL THEN 'Missing in Migrated'
         ELSE 'Value Mismatch' END AS discrepancy_type
FROM
    LegacyData L
FULL OUTER JOIN
    MigratedData M
ON
    L.CNTRCT_ID = M.CNTRCT_ID AND
    L.BPR_ID = M.BPR_ID AND
    L.CNTRCT_ID_REF = M.CNTRCT_ID_REF AND
    L.ACCESS_POINT_NAME = M.ACCESS_POINT_NAME
WHERE
    L.CNTRCT_ID IS NULL OR M.CNTRCT_ID IS NULL OR -- Rows present in one but not the other
    L.CNTRCT_ID != M.CNTRCT_ID OR
    L.BPR_ID != M.BPR_ID OR
    L.CNTRCT_ID_REF != M.CNTRCT_ID_REF OR
    L.ACCESS_POINT_NAME != M.ACCESS_POINT_NAME;

-- Expected: No rows returned by the discrepancy query.
```

### Test 2: Orchestration - Parameter Validation and Error Handling

**Purpose:** To verify that the migrated orchestration logic (`prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp`) correctly validates input parameters, logs errors to `prod_dw_logs.error_log`, and terminates execution when validation fails, mirroring the behavior of the legacy KornShell script.

**Setup:**
1.  **Legacy Environment:**
    *   Ensure `f_alis_msgerr.ksh` and `h_alis_parameter.ksh` are available.
    *   Prepare a dummy `d_ausd_bp_ta_bpr_apn.sql` that just prints its parameters and exits successfully, to isolate parameter validation.
2.  **Migrated Environment (BigQuery):**
    *   Ensure `prod_dw_logs.error_log` is empty before each test run.
    *   Deploy `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp` and `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp`.

**Action (Multiple Scenarios):**

**Scenario 2.1: Missing `p_JobKennung`**
1.  **Legacy:** Execute `k_ausd_bp_ta_bpr_apn.ksh -s 26102023 -f 1`. Observe output and exit code.
2.  **Migrated:** Execute `CALL prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp(NULL, '1', '26102023', 0);`. Observe if it fails and check `prod_dw_logs.error_log`.

**Scenario 2.2: Missing `p_Stichtag`**
1.  **Legacy:** Execute `k_ausd_bp_ta_bpr_apn.ksh -j TEST_JOB -f 1`. Observe output and exit code.
2.  **Migrated:** Execute `CALL prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp('TEST_JOB', '1', NULL, 0);`. Observe if it fails and check `prod_dw_logs.error_log`.

**Scenario 2.3: Missing `p_EintragsNr`**
1.  **Legacy:** Execute `k_ausd_bp_ta_bpr_apn.ksh -j TEST_JOB -s 26102023`. Observe output and exit code.
2.  **Migrated:** Execute `CALL prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp('TEST_JOB', NULL, '26102023', 0);`. Observe if it fails and check `prod_dw_logs.error_log`.

**Scenario 2.4: Invalid `p_Stichtag` Format (e.g., YYYY-MM-DD)**
1.  **Legacy:** Execute `k_ausd_bp_ta_bpr_apn.ksh -j TEST_JOB -f 1 -s 2023-10-26`. Observe output and exit code. (The `DWDate_Datum_Check` function should catch this).
2.  **Migrated:** Execute `CALL prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp('TEST_JOB', '1', '2023-10-26', 0);`. Observe if it fails and check `prod_dw_logs.error_log`.

**Scenario 2.5: Invalid `p_Stichtag` Value (e.g., 32nd day)**
1.  **Legacy:** Execute `k_ausd_bp_ta_bpr_apn.ksh -j TEST_JOB -f 1 -s 32102023`. Observe output and exit code.
2.  **Migrated:** Execute `CALL prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp('TEST_JOB', '1', '32102023', 0);`. Observe if it fails and check `prod_dw_logs.error_log`.

**Pass/Fail Criterion:**
*   For each scenario, the migrated BigQuery Stored Procedure must terminate with an error (signaled SQLSTATE '45000').
*   An entry must be present in `prod_dw_logs.error_log` with the correct `job_id`, `source_script`, `error_number` (193 for missing, 194/195 for date issues), `error_argument`, and `error_message` matching the expected validation failure.
*   The legacy script should also exit with a non-zero status and print an error message.

**Runnable Test Code (SQL Assertions for Migrated):**

```sql
-- Example for Scenario 2.1 (Missing JobKennung)
-- This query should be run after attempting to CALL the SP with NULL p_JobKennung.
SELECT
    log_timestamp,
    job_id,
    source_script,
    error_number,
    error_argument,
    error_message
FROM
    prod_dw_logs.error_log
WHERE
    source_script = 'k_ausd_bp_ta_bpr_apn_sp'
    AND error_number = 193
    AND error_argument = 'Jobkennung'
    AND error_message LIKE '%Required parameter Jobkennung is missing%';

-- Expected: Exactly one row returned matching the error details.
-- Similar queries for other scenarios, adjusting error_number and error_argument.
```

### Test 3: Orchestration - Date Derivation and Job Tracking

**Purpose:** To verify that the migrated orchestration logic correctly derives "today" and "yesterday" dates, and that the job tracking table (`prod_dw_logs.job_tracking`) is updated with the correct record count and metadata upon successful completion. This covers the `gestern.ksh` and (commented out) `FOSJobErzeugeEintrag` functionality.

**Setup:**
1.  **Legacy Environment:**
    *   Ensure `gestern.ksh` is available.
    *   Prepare a dummy `d_ausd_bp_ta_bpr_apn.sql` that inserts a known number of rows (e.g., 5 rows) into `PoolBasisprodukt` and writes this count to the temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`).
    *   Ensure the legacy job tracking mechanism (if reactivated for testing) is configured.
2.  **Migrated Environment (BigQuery):**
    *   Ensure `prod_dw_isrpt.PoolBasisprodukt` is empty.
    *   Ensure `prod_dw_logs.job_tracking` is empty.
    *   Deploy `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp` and `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp`.
    *   Populate source tables for `d_ausd_bp_ta_bpr_apn_sp` such that it will insert a known number of rows (e.g., 5 rows) into `prod_dw_isrpt.PoolBasisprodukt`.

**Action:**
1.  **Legacy Execution:**
    *   Execute `k_ausd_bp_ta_bpr_apn.ksh -j TEST_JOB_DATE -f 1 -s 26102023`.
    *   Capture the output of `gestern.ksh` (today and yesterday dates).
    *   Verify the content of `PoolBasisprodukt` and the temporary file.
    *   If `FOSJobErzeugeEintrag` is uncommented and active, check the job tracking table.
2.  **Migrated Execution:**
    *   Execute `CALL prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp('TEST_JOB_DATE', '1', '26102023', 0);`.

**Pass/Fail Criterion:**
*   **Date Derivation:** The `v_datum_heute_YYYYMMDD` and `v_datum_gestern_YYYYMMDD` values passed to `d_ausd_bp_ta_bpr_apn_sp` (and implicitly derived by `k_ausd_bp_ta_bpr_apn_sp`) must correspond to the actual current date and previous day, respectively, in 'YYYYMMDD' format. (This can be verified by inspecting BigQuery logs or by adding `SELECT` statements within the SP for debugging).
*   **Record Count:** The number of rows inserted into `prod_dw_isrpt.PoolBasisprodukt` must match the expected number (e.g., 5).
*   **Job Tracking:** A single entry must be present in `prod_dw_logs.job_tracking` with:
    *   `job_id = 'TEST_JOB_DATE'`
    *   `entry_number = '1'`
    *   `table_name = 'PoolBasisprodukt'`
    *   `status = 'COMPLETED'`
    *   `start_date = '2023-10-26'` (parsed from `p_Stichtag`)
    *   `end_date = '2023-10-26'`
    *   `record_count` matching the actual number of rows inserted into `PoolBasisprodukt`.

**Runnable Test Code (SQL Assertions for Migrated):**

```sql
-- After successful execution of CALL prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp('TEST_JOB_DATE', '1', '26102023', 0);

-- 1. Verify record count in target table
SELECT COUNT(*) FROM prod_dw_isrpt.PoolBasisprodukt;
-- Expected: 5 (or whatever known number of rows the d_ausd_bp_ta_bpr_apn_sp inserts)

-- 2. Verify job tracking entry
SELECT
    track_timestamp,
    job_id,
    entry_number,
    table_name,
    status,
    FORMAT_DATE('%Y-%m-%d', start_date) AS start_date_fmt,
    FORMAT_DATE('%Y-%m-%d', end_date) AS end_date_fmt,
    record_count,
    notes
FROM
    prod_dw_logs.job_tracking
WHERE
    job_id = 'TEST_JOB_DATE'
    AND entry_number = '1'
ORDER BY track_timestamp DESC
LIMIT 1;

-- Expected: One row with:
-- job_id = 'TEST_JOB_DATE'
-- entry_number = '1'
-- table_name = 'PoolBasisprodukt'
-- status = 'COMPLETED'
-- start_date_fmt = '2023-10-26'
-- end_date_fmt = '2023-10-26'
-- record_count = [Expected number of rows, e.g., 5]
-- notes = 'Initialbefuellung'
```

### Test 4: External System Replacements - Source Data Reads

**Purpose:** To confirm that the BigQuery source tables (`prod_dw_source.sof_ta_bpr_instance`, `prod_dw_source.sof_ta_apn_carmen`, `prod_dw_source.dwtk_meldungen`) accurately reflect the data from their legacy Oracle counterparts, ensuring that the "reads" from external systems (Oracle in this case) are correctly replaced by BigQuery table reads.

**Setup:**
1.  **Legacy Environment:**
    *   Identify the exact schema and data content of `sof$ta_bpr_instance`, `sof$ta_apn_carmen`, and `dwtk_meldungen` in the Oracle database at a specific point in time.
2.  **Migrated Environment (BigQuery):**
    *   Ensure `prod_dw_source.sof_ta_bpr_instance`, `prod_dw_source.sof_ta_apn_carmen`, and `prod_dw_source.dwtk_meldungen` BigQuery tables exist and are populated.

**Action:**
1.  **Data Extraction:** Extract data from the Oracle source tables.
2.  **Data Comparison:** Compare the extracted Oracle data with the data in the corresponding BigQuery source tables. This can be done via row-by-row comparison, hash comparison, or by loading both datasets into a temporary comparison table and running diff queries.

**Pass/Fail Criterion:**
*   The schema (column names, data types, nullability) of each BigQuery source table must match its Oracle counterpart.
*   The data content (row count and all column values) of each BigQuery source table must be identical to its Oracle counterpart.

**Runnable Test Code (SQL Assertions):**

```sql
-- Assuming legacy Oracle data has been loaded into temporary BigQuery tables
-- (e.g., `your_gcp_project.temp_dataset.legacy_sof_ta_bpr_instance_snapshot`)

-- Example for sof_ta_bpr_instance:
WITH LegacySource AS (
    SELECT * FROM `your_gcp_project.temp_dataset.legacy_sof_ta_bpr_instance_snapshot`
),
MigratedSource AS (
    SELECT * FROM prod_dw_source.sof_ta_bpr_instance
)
SELECT
    'Discrepancy Found in sof_ta_bpr_instance' AS status,
    COALESCE(L.cntrct_id, M.cntrct_id) AS cntrct_id,
    -- Add other key columns for identification
    CASE WHEN L.cntrct_id IS NULL THEN 'Missing in Legacy'
         WHEN M.cntrct_id IS NULL THEN 'Missing in Migrated'
         ELSE 'Value Mismatch' END AS discrepancy_type
FROM
    LegacySource L
FULL OUTER JOIN
    MigratedSource M
ON
    L.cntrct_id = M.cntrct_id AND
    L.bpr_id = M.bpr_id AND
    L.cntrct_id_ref = M.cntrct_id_ref
    -- Add all relevant join conditions for other columns
WHERE
    L.cntrct_id IS NULL OR M.cntrct_id IS NULL OR
    L.bpr_id != M.bpr_id OR
    L.cntrct_id_ref != M.cntrct_id_ref;
    -- Add all other column comparisons for value mismatches

-- Expected: No rows returned. Repeat for sof_ta_apn_carmen and dwtk_meldungen.

-- Schema comparison (manual or using information_schema)
SELECT column_name, data_type, is_nullable
FROM `your_gcp_project.prod_dw_source.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_bpr_instance'
ORDER BY ordinal_position;
-- Compare this output with Oracle's DESCRIBE table_name or equivalent.
```

### Test 5: Data Quality and Schema Assertions - Target Table

**Purpose:** To verify that the schema of the target BigQuery table `prod_dw_isrpt.PoolBasisprodukt` matches the expected design and that the data types and nullability are correctly enforced.

**Setup:**
1.  **Migrated Environment (BigQuery):**
    *   Ensure `prod_dw_isrpt.PoolBasisprodukt` table is created.
    *   Run `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp` with valid parameters to populate the table.

**Action:**
1.  Query BigQuery's `INFORMATION_SCHEMA` for the `prod_dw_isrpt.PoolBasisprodukt` table.
2.  Inspect the data types and nullability of the columns.

**Pass/Fail Criterion:**
*   The `prod_dw_isrpt.PoolBasisprodukt` table must exist.
*   The schema must match the DDL provided:
    *   `CNTRCT_ID STRING`
    *   `BPR_ID INT64`
    *   `CNTRCT_ID_REF STRING`
    *   `ACCESS_POINT_NAME STRING`
*   All columns should be nullable by default unless explicitly specified otherwise in the DDL (which they are not in the provided DDL).

**Runnable Test Code (SQL Assertions):**

```sql
-- Query BigQuery INFORMATION_SCHEMA
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your_gcp_project.prod_dw_isrpt.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'PoolBasisprodukt'
ORDER BY
    ordinal_position;

-- Expected Output:
-- column_name    data_type    is_nullable
-- CNTRCT_ID      STRING       YES
-- BPR_ID         INT64        YES
-- CNTRCT_ID_REF  STRING       YES
-- ACCESS_POINT_NAME STRING    YES
```

### Test 6: End-to-End Airflow DAG Execution

**Purpose:** To verify that the Airflow DAG (`k_ausd_bp_ta_bpr_apn_dag.py`) can successfully trigger and execute the BigQuery Stored Procedure `prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp` with valid parameters, and that the overall workflow completes successfully.

**Setup:**
1.  **Migrated Environment (BigQuery & Airflow):**
    *   Ensure all BigQuery procedures (`prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp`, `prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp`) and tables (`prod_dw_isrpt.PoolBasisprodukt`, `prod_dw_logs.error_log`, `prod_dw_logs.job_tracking`, source tables) are deployed and configured.
    *   Ensure the Airflow environment is running and the `k_ausd_bp_ta_bpr_apn_dag.py` DAG is deployed.
    *   Configure the `gcp_project_id` Airflow variable and `google_cloud_default` connection.
    *   Ensure `prod_dw_isrpt.PoolBasisprodukt` and `prod_dw_logs.job_tracking` are empty before the run.

**Action:**
1.  Trigger the `k_ausd_bp_ta_bpr_apn_dag` in Airflow with valid parameters:
    *   `job_kennung`: `AIRFLOW_TEST_JOB`
    *   `eintrags_nr`: `100`
    *   `stichtag`: `26102023` (or current date in DDMMYYYY)
    *   `wiederanlauf_wert`: `0`

**Pass/Fail Criterion:**
*   The Airflow DAG run must complete successfully (all tasks green).
*   The `prod_dw_isrpt.PoolBasisprodukt` table must be populated with data.
*   A successful entry must be recorded in `prod_dw_logs.job_tracking` for `job_id = 'AIRFLOW_TEST_JOB'`.
*   No new entries should appear in `prod_dw_logs.error_log`.

**Runnable Test Code (Pytest for Airflow DAG):**

```python
import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.state import State
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from datetime import datetime

# Mock BigQuery client for testing operator calls without actual BQ execution
# For full integration test, you'd need a live BQ environment or a more sophisticated mock.
class MockBigQueryHook:
    def __init__(self, gcp_conn_id=None):
        pass
    def run_stored_procedure(self, project_id, dataset_id, procedure_id, parameters):
        print(f"Mock BigQuery SP call: {project_id}.{dataset_id}.{procedure_id} with {parameters}")
        # Simulate success
        return None

@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loading(dag_bag):
    """Verify the DAG loads correctly."""
    dag = dag_bag.get_dag("k_ausd_bp_ta_bpr_apn_dag")
    assert dag is not None
    assert len(dag.tasks) == 3 # start, call_main_bigquery_sp, end

def test_dag_structure(dag_bag):
    """Verify task dependencies and operator types."""
    dag = dag_bag.get_dag("k_ausd_bp_ta_bpr_apn_dag")
    call_sp_task = dag.get_task("call_main_bigquery_stored_procedure")
    assert isinstance(call_sp_task, BigQueryExecuteStoredProcedureOperator)
    assert call_sp_task.dataset_id == "prod_dw_isrpt"
    assert call_sp_task.procedure_id == "k_ausd_bp_ta_bpr_apn_sp"

def test_dag_execution_success(mocker, dag_bag):
    """Simulate a successful DAG run."""
    dag = dag_bag.get_dag("k_ausd_bp_ta_bpr_apn_dag")

    # Mock the BigQuery hook to simulate successful SP execution
    mocker.patch('airflow.providers.google.cloud.hooks.bigquery.BigQueryHook', return_value=MockBigQueryHook())

    # Set parameters for the DAG run
    run_id = f"test_run_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    execution_date = datetime(2023, 10, 26)
    dag_run = dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf={
            "job_kennung": "AIRFLOW_TEST_JOB",
            "eintrags_nr": "100",
            "stichtag": "26102023",
            "wiederanlauf_wert": 0,
        }
    )

    # Execute the tasks
    for task in dag.tasks:
        ti = dag_run.get_task_instance(task.task_id)
        ti.run(session=dag_run.get_session())

    # Assert task states
    assert dag_run.get_task_instance("start").current_state() == State.SUCCESS
    assert dag_run.get_task_instance("call_main_bigquery_stored_procedure").current_state() == State.SUCCESS
    assert dag_run.get_task_instance("end").current_state() == State.SUCCESS

    # Further assertions would involve querying BigQuery directly after a real run
    # For example, check prod_dw_isrpt.PoolBasisprodukt and prod_dw_logs.job_tracking
    # This requires a live BigQuery environment and is typically done in integration tests.
    # For unit testing the DAG, mocking the BQ call is sufficient.
```