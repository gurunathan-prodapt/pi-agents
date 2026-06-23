The migration of `r_ausd_geschaeftspartner.ksh` to BigQuery involves replacing shell orchestration with a BigQuery stored procedure (`sp_initial_befuellung_vertrags_cache_fos`), file-based logging with BigQuery tables, and delegating the core logic to another BigQuery stored procedure (`sp_ausd_geschaeftspartner`). The following tests validate the behavioral equivalence of the migrated orchestration layer.

---

## Test Case 1: Happy Path - All Parameters Provided

**Purpose:**
To verify that the BigQuery orchestration stored procedure `sp_initial_befuellung_vertrags_cache_fos` executes successfully when both `p_stichtag` and `p_wiederanlaufWert` are provided, correctly logs the job's progress, updates the job registry, and invokes the core logic procedure with the expected parameters.

**Setup:**
1.  Ensure the `project.dataset.job_log` and `project.dataset.job_registry` tables exist with the schema defined in the design document.
2.  Ensure the `project.dataset.sp_ausd_geschaeftspartner` stored procedure exists. For this test, it can be a dummy procedure that simply logs its invocation and returns successfully, as the focus is on the orchestrator.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_geschaeftspartner`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      INSERT INTO `project.dataset.job_log`
        (job_kennung, eintragsnr, event_ts, level, message)
      VALUES
        (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
         CONCAT('sp_ausd_geschaeftspartner invoked with Stichtag=', p_stichtag, ', Restart=', CAST(p_wiederanlaufWert AS STRING)));
      -- Simulate successful execution
    END;
    ```
3.  Clear the `job_log` and `job_registry` tables before execution to ensure a clean state.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_registry`;
    ```

**Action:**
Execute the `sp_initial_befuellung_vertrags_cache_fos` procedure with a specific `stichtag` and `wiederanlaufWert`.

```sql
CALL `project.dataset.sp_initial_befuellung_vertrags_cache_fos`('01012023', 12345);
```

**Pass/Fail Criterion:**
1.  The `job_registry` table must contain one entry for `job_kennung = 'BERT_P_GESCHAEFTSP'` with `status = 'OK'`, `stichtag = '01012023'`, and `restart_value = 12345`.
2.  The `job_log` table must contain at least 3 `INFO` entries for the executed job:
    *   One indicating "Job started. Stichtag=01012023, Restart=12345".
    *   One indicating "sp_ausd_geschaeftspartner invoked with Stichtag=01012023, Restart=12345" (from the dummy core procedure).
    *   One indicating "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
3.  No `ERROR` entries should be present in `job_log`.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from datetime import datetime

# Assume bigquery_client is an initialized BigQuery client
# Assume project_id and dataset_id are configured

@pytest.fixture(scope="module")
def bigquery_client():
    return bigquery.Client()

@pytest.fixture(autouse=True)
def setup_teardown_tables(bigquery_client, project_id, dataset_id):
    # Ensure dummy core procedure exists
    core_proc_ddl = f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_ausd_geschaeftspartner`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      INSERT INTO `{project_id}.{dataset_id}.job_log`
        (job_kennung, eintragsnr, event_ts, level, message)
      VALUES
        (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
         CONCAT('sp_ausd_geschaeftspartner invoked with Stichtag=', p_stichtag, ', Restart=', CAST(p_wiederanlaufWert AS STRING)));
    END;
    """
    bigquery_client.query(core_proc_ddl).result()

    # Clear tables before each test
    bigquery_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    bigquery_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_registry`").result()
    yield
    # Optional: Clean up after tests if needed, e.g., drop dummy proc
    # bigquery_client.query(f"DROP PROCEDURE `{project_id}.{dataset_id}.sp_ausd_geschaeftspartner`").result()

def test_happy_path_all_params_provided(bigquery_client, project_id, dataset_id):
    stichtag = '01012023'
    restart_value = 12345
    job_kennung = 'BERT_P_GESCHAEFTSP'

    # Action: Execute the stored procedure
    call_proc_sql = f"""
    CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('{stichtag}', {restart_value});
    """
    bigquery_client.query(call_proc_sql).result()

    # Assertions
    # 1. Check job_registry
    registry_query = f"""
    SELECT status, stichtag, restart_value
    FROM `{project_id}.{dataset_id}.job_registry`
    WHERE job_kennung = '{job_kennung}'
    """
    registry_rows = list(bigquery_client.query(registry_query).result())
    assert len(registry_rows) == 1
    assert registry_rows[0].status == 'OK'
    assert registry_rows[0].stichtag == stichtag
    assert registry_rows[0].restart_value == restart_value

    # 2. Check job_log entries
    log_query = f"""
    SELECT message, level
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_kennung = '{job_kennung}'
    ORDER BY event_ts
    """
    log_rows = list(bigquery_client.query(log_query).result())
    
    assert any(f"Job started. Stichtag={stichtag}, Restart={restart_value}" in row.message for row in log_rows)
    assert any(f"sp_ausd_geschaeftspartner invoked with Stichtag={stichtag}, Restart={restart_value}" in row.message for row in log_rows)
    assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in row.message for row in log_rows)
    assert not any(row.level == 'ERROR' for row in log_rows)
```

---

## Test Case 2: Default Stichtag and Wiederanlaufwert

**Purpose:**
To verify that the BigQuery orchestration procedure correctly applies default values for `p_stichtag` (current system date) and `p_wiederanlaufWert` (0) when they are not explicitly provided (i.e., passed as `NULL`).

**Setup:**
1.  Same as Test Case 1.
2.  Clear `job_log` and `job_registry` tables.

**Action:**
Execute the `sp_initial_befuellung_vertrags_cache_fos` procedure with `NULL` for both parameters.

```sql
CALL `project.dataset.sp_initial_befuellung_vertrags_cache_fos`(NULL, NULL);
```

**Pass/Fail Criterion:**
1.  The `job_registry` table must contain one entry for `job_kennung = 'BERT_P_GESCHAEFTSP'` with `status = 'OK'`.
2.  The `stichtag` in `job_registry` must match today's date in `DDMMYYYY` format.
3.  The `restart_value` in `job_registry` must be `0`.
4.  The `job_log` table must contain `INFO` entries reflecting the default values:
    *   One indicating "Job started. Stichtag=<today's date DDMMYYYY>, Restart=0".
    *   One indicating "sp_ausd_geschaeftspartner invoked with Stichtag=<today's date DDMMYYYY>, Restart=0".
    *   One indicating "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
5.  No `ERROR` entries should be present in `job_log`.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from datetime import datetime

# ... (bigquery_client and setup_teardown_tables fixtures as above) ...

def test_default_stichtag_and_restart_value(bigquery_client, project_id, dataset_id):
    job_kennung = 'BERT_P_GESCHAEFTSP'
    expected_stichtag = datetime.now().strftime('%d%m%Y')
    expected_restart_value = 0

    # Action: Execute the stored procedure with NULLs
    call_proc_sql = f"""
    CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`(NULL, NULL);
    """
    bigquery_client.query(call_proc_sql).result()

    # Assertions
    # 1. Check job_registry
    registry_query = f"""
    SELECT status, stichtag, restart_value
    FROM `{project_id}.{dataset_id}.job_registry`
    WHERE job_kennung = '{job_kennung}'
    """
    registry_rows = list(bigquery_client.query(registry_query).result())
    assert len(registry_rows) == 1
    assert registry_rows[0].status == 'OK'
    assert registry_rows[0].stichtag == expected_stichtag
    assert registry_rows[0].restart_value == expected_restart_value

    # 2. Check job_log entries
    log_query = f"""
    SELECT message, level
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_kennung = '{job_kennung}'
    ORDER BY event_ts
    """
    log_rows = list(bigquery_client.query(log_query).result())
    
    assert any(f"Job started. Stichtag={expected_stichtag}, Restart={expected_restart_value}" in row.message for row in log_rows)
    assert any(f"sp_ausd_geschaeftspartner invoked with Stichtag={expected_stichtag}, Restart={expected_restart_value}" in row.message for row in log_rows)
    assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in row.message for row in log_rows)
    assert not any(row.level == 'ERROR' for row in log_rows)
```

---

## Test Case 3: Missing Required Parameter (Stichtag)

**Purpose:**
To verify that the BigQuery orchestration procedure correctly handles the case where the `p_stichtag` parameter is `NULL` or an empty string, which is considered a missing required parameter by the legacy script's `pruefeParameterGesetzt` function. It should log an error and raise an exception.

**Setup:**
1.  Same as Test Case 1.
2.  Clear `job_log` and `job_registry` tables.

**Action:**
Execute the `sp_initial_befuellung_vertrags_cache_fos` procedure with `p_stichtag` as an empty string and `p_wiederanlaufWert` provided.

```sql
-- This will cause the procedure to raise an error
CALL `project.dataset.sp_initial_befuellung_vertrags_cache_fos`('', 100);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement must result in an error being raised by BigQuery (e.g., `RAISE USING MESSAGE = 'Error 193: Stichtag'`).
2.  The `job_registry` table must contain one entry for `job_kennung = 'BERT_P_GESCHAEFTSP'` with `status = 'STARTED'` (or similar initial status, as the error occurs before final status update) and `stichtag = ''` (or `NULL` if `NULL` was passed). The `finished_ts` should be `NULL`.
3.  The `job_log` table must contain one `ERROR` entry with `errnr = 193`, `errarg = 'Stichtag'`, and `message = 'Missing required parameter'`.
4.  No `INFO` entry indicating successful completion ("Die Abarbeitung wurde ohne erkennbare Fehler beendet") should be present.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest # For catching BQ errors

# ... (bigquery_client and setup_teardown_tables fixtures as above) ...

def test_missing_required_stichtag(bigquery_client, project_id, dataset_id):
    job_kennung = 'BERT_P_GESCHAEFTSP'
    restart_value = 100

    # Action: Execute the stored procedure with empty stichtag
    call_proc_sql = f"""
    CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('', {restart_value});
    """
    
    # Expecting a BadRequest error due to RAISE
    with pytest.raises(BadRequest) as excinfo:
        bigquery_client.query(call_proc_sql).result()
    
    assert "Error 193: Stichtag" in str(excinfo.value)

    # Assertions
    # 1. Check job_registry
    registry_query = f"""
    SELECT status, stichtag, restart_value, finished_ts
    FROM `{project_id}.{dataset_id}.job_registry`
    WHERE job_kennung = '{job_kennung}'
    """
    registry_rows = list(bigquery_client.query(registry_query).result())
    assert len(registry_rows) == 1
    assert registry_rows[0].status == 'STARTED' # Or whatever the initial status is before the RAISE
    assert registry_rows[0].stichtag == ''
    assert registry_rows[0].restart_value == restart_value
    assert registry_rows[0].finished_ts is None # Should not be updated to finished

    # 2. Check job_log entries
    log_query = f"""
    SELECT message, level, errnr, errarg
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_kennung = '{job_kennung}'
    ORDER BY event_ts
    """
    log_rows = list(bigquery_client.query(log_query).result())
    
    assert any(row.level == 'ERROR' and row.errnr == 193 and row.errarg == 'Stichtag' and 'Missing required parameter' in row.message for row in log_rows)
    assert not any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in row.message for row in log_rows)
```

---

## Test Case 4: Core Logic Procedure Failure

**Purpose:**
To verify that if the delegated core logic procedure (`sp_ausd_geschaeftspartner`) fails, the orchestrator (`sp_initial_befuellung_vertrags_cache_fos`) correctly propagates the error, logs the failure, and updates the job registry status accordingly (or leaves it in a non-OK state).

**Setup:**
1.  Same as Test Case 1.
2.  Modify the dummy `sp_ausd_geschaeftspartner` to simulate a failure (e.g., by raising an error).
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_geschaeftspartner`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      INSERT INTO `project.dataset.job_log`
        (job_kennung, eintragsnr, event_ts, level, message)
      VALUES
        (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'ERROR',
         CONCAT('Simulated failure in sp_ausd_geschaeftspartner for Stichtag=', p_stichtag));
      RAISE USING MESSAGE = 'Simulated core logic failure';
    END;
    ```
3.  Clear `job_log` and `job_registry` tables.

**Action:**
Execute the `sp_initial_befuellung_vertrags_cache_fos` procedure with valid parameters.

```sql
-- This will cause the procedure to raise an error due to the called sub-procedure
CALL `project.dataset.sp_initial_befuellung_vertrags_cache_fos`('01012023', 0);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement must result in an error being raised by BigQuery, originating from the `sp_ausd_geschaeftspartner` procedure.
2.  The `job_registry` table must contain one entry for `job_kennung = 'BERT_P_GESCHAEFTSP'` with `status = 'STARTED'` (or similar initial status, not 'OK') and `finished_ts` as `NULL`.
3.  The `job_log` table must contain:
    *   An `INFO` entry for job start.
    *   An `ERROR` entry from `sp_ausd_geschaeftspartner` (e.g., "Simulated failure in sp_ausd_geschaeftspartner...").
    *   No `INFO` entry indicating successful completion ("Die Abarbeitung wurde ohne erkennbare Fehler beendet").

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

# ... (bigquery_client fixture as above) ...

@pytest.fixture(autouse=True)
def setup_teardown_tables_with_failing_core(bigquery_client, project_id, dataset_id):
    # Ensure failing dummy core procedure exists
    failing_core_proc_ddl = f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_ausd_geschaeftspartner`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      INSERT INTO `{project_id}.{dataset_id}.job_log`
        (job_kennung, eintragsnr, event_ts, level, message)
      VALUES
        (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'ERROR',
         CONCAT('Simulated failure in sp_ausd_geschaeftspartner for Stichtag=', p_stichtag));
      RAISE USING MESSAGE = 'Simulated core logic failure';
    END;
    """
    bigquery_client.query(failing_core_proc_ddl).result()

    # Clear tables before each test
    bigquery_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    bigquery_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_registry`").result()
    yield
    # Restore successful dummy core procedure for other tests if needed, or drop
    # bigquery_client.query(f"DROP PROCEDURE `{project_id}.{dataset_id}.sp_ausd_geschaeftspartner`").result()

def test_core_logic_procedure_failure(bigquery_client, project_id, dataset_id):
    stichtag = '01012023'
    restart_value = 0
    job_kennung = 'BERT_P_GESCHAEFTSP'

    # Action: Execute the stored procedure
    call_proc_sql = f"""
    CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('{stichtag}', {restart_value});
    """
    
    # Expecting a BadRequest error due to RAISE from the called procedure
    with pytest.raises(BadRequest) as excinfo:
        bigquery_client.query(call_proc_sql).result()
    
    assert "Simulated core logic failure" in str(excinfo.value)

    # Assertions
    # 1. Check job_registry
    registry_query = f"""
    SELECT status, finished_ts
    FROM `{project_id}.{dataset_id}.job_registry`
    WHERE job_kennung = '{job_kennung}'
    """
    registry_rows = list(bigquery_client.query(registry_query).result())
    assert len(registry_rows) == 1
    assert registry_rows[0].status == 'STARTED' # Should not be 'OK'
    assert registry_rows[0].finished_ts is None

    # 2. Check job_log entries
    log_query = f"""
    SELECT message, level
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_kennung = '{job_kennung}'
    ORDER BY event_ts
    """
    log_rows = list(bigquery_client.query(log_query).result())
    
    assert any(f"Job started. Stichtag={stichtag}, Restart={restart_value}" in row.message for row in log_rows)
    assert any(f"Simulated failure in sp_ausd_geschaeftspartner for Stichtag={stichtag}" in row.message and row.level == 'ERROR' for row in log_rows)
    assert not any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in row.message for row in log_rows)
```

---

## Test Case 5: Schema and Data Quality Assertions for Logging Tables

**Purpose:**
To verify that the `job_log` and `job_registry` tables conform to the expected schema and that basic data quality (e.g., non-nullability for critical fields) is maintained.

**Setup:**
1.  Ensure the `project.dataset.job_log` and `project.dataset.job_registry` tables exist.
2.  Execute a successful run of `sp_initial_befuellung_vertrags_cache_fos` (e.g., using the setup from Test Case 1).

**Action:**
Query the information schema for the tables and select data to check for nulls.

```sql
-- No direct action, relies on previous successful run
```

**Pass/Fail Criterion:**
1.  **Schema Conformance:**
    *   `job_log` table must have columns: `job_kennung STRING`, `eintragsnr INT64`, `event_ts TIMESTAMP`, `level STRING`, `errnr INT64`, `errarg STRING`, `message STRING`.
    *   `job_registry` table must have columns: `job_kennung STRING`, `created_ts TIMESTAMP`, `finished_ts TIMESTAMP`, `stichtag STRING`, `sysdate STRING`, `restart_value INT64`, `status STRING`.
2.  **Data Quality (Non-Nullability for critical fields):**
    *   For `job_log` entries, `job_kennung`, `eintragsnr`, `event_ts`, `level`, and `message` must not be `NULL`.
    *   For `job_registry` entries, `job_kennung`, `created_ts`, `stichtag`, `sysdate`, `restart_value`, and `status` must not be `NULL` (except `finished_ts` which can be `NULL` for in-progress jobs).

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from datetime import datetime

# ... (bigquery_client and setup_teardown_tables fixtures as above) ...

def test_logging_table_schema_and_data_quality(bigquery_client, project_id, dataset_id):
    # Ensure a successful run has occurred to populate tables
    stichtag = '01012023'
    restart_value = 12345
    call_proc_sql = f"""
    CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('{stichtag}', {restart_value});
    """
    bigquery_client.query(call_proc_sql).result()

    # 1. Schema Conformance
    # Check job_log schema
    job_log_schema_query = f"""
    SELECT column_name, data_type
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log'
    ORDER BY ordinal_position
    """
    job_log_schema_rows = list(bigquery_client.query(job_log_schema_query).result())
    expected_job_log_schema = [
        ('job_kennung', 'STRING'),
        ('eintragsnr', 'INT64'),
        ('event_ts', 'TIMESTAMP'),
        ('level', 'STRING'),
        ('errnr', 'INT64'),
        ('errarg', 'STRING'),
        ('message', 'STRING'),
    ]
    assert [(row.column_name, row.data_type) for row in job_log_schema_rows] == expected_job_log_schema

    # Check job_registry schema
    job_registry_schema_query = f"""
    SELECT column_name, data_type
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_registry'
    ORDER BY ordinal_position
    """
    job_registry_schema_rows = list(bigquery_client.query(job_registry_schema_query).result())
    expected_job_registry_schema = [
        ('job_kennung', 'STRING'),
        ('created_ts', 'TIMESTAMP'),
        ('finished_ts', 'TIMESTAMP'),
        ('stichtag', 'STRING'),
        ('sysdate', 'STRING'),
        ('restart_value', 'INT64'),
        ('status', 'STRING'),
    ]
    assert [(row.column_name, row.data_type) for row in job_registry_schema_rows] == expected_job_registry_schema

    # 2. Data Quality (Non-Nullability)
    # Check job_log for critical nulls
    job_log_null_check_query = f"""
    SELECT COUNT(*)
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_kennung IS NULL OR eintragsnr IS NULL OR event_ts IS NULL OR level IS NULL OR message IS NULL
    """
    job_log_null_count = bigquery_client.query(job_log_null_check_query).result().total_rows
    assert job_log_null_count == 0, "Critical fields in job_log should not be NULL"

    # Check job_registry for critical nulls (finished_ts can be null)
    job_registry_null_check_query = f"""
    SELECT COUNT(*)
    FROM `{project_id}.{dataset_id}.job_registry`
    WHERE job_kennung IS NULL OR created_ts IS NULL OR stichtag IS NULL OR sysdate IS NULL OR restart_value IS NULL OR status IS NULL
    """
    job_registry_null_count = bigquery_client.query(job_registry_null_check_query).result().total_rows
    assert job_registry_null_count == 0, "Critical fields in job_registry should not be NULL (excluding finished_ts)"
```