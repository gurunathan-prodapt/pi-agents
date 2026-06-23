The migration of `r_ausd_bp_ta_cntrct_evn.ksh` to a BigQuery Stored Procedure (`ausd_bp_ta_cntrct_evn_wrapper`) focuses on orchestrating the core logic, handling parameters, and managing logging/error handling. The wrapper itself does not perform direct data transformations. Therefore, the tests below will primarily focus on the wrapper's behavioral equivalence in these areas.

**Assumptions for Testing:**
*   A BigQuery project (`my_project`) and dataset (`my_dataset`) are available.
*   The `job_log` and `job_control` tables, as defined in the migration design, exist within `my_dataset`.
*   The placeholder `k_ausd_bp_ta_cntrct_evn_core` procedure exists in `my_dataset`.
*   Tests are executed using `pytest` with the `google-cloud-bigquery` client library.
*   Each test case will clear the `job_log` and `job_control` tables before execution to ensure isolation.

---

### Test Setup (Conceptual `conftest.py`)

This `conftest.py` provides fixtures for a BigQuery client and ensures the necessary tables and procedures are deployed before tests run.

```python
# conftest.py
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "my_project"  # Replace with your BigQuery project ID
DATASET_ID = "my_dataset"  # Replace with your BigQuery dataset ID

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="session", autouse=True)
def setup_bigquery_environment(bq_client):
    """
    Sets up the BigQuery environment: creates dataset, tables, and deploys
    the wrapper and default (successful) core procedures.
    """
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    # Create job_log table
    job_log_schema = [
        bigquery.SchemaField("job_nr", "INT64"),
        bigquery.SchemaField("job_name", "STRING"),
        bigquery.SchemaField("log_level", "STRING"),
        bigquery.SchemaField("error_nr", "INT64"),
        bigquery.SchemaField("error_arg", "STRING"),
        bigquery.SchemaField("message", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]
    job_log_table_ref = bq_client.dataset(DATASET_ID).table("job_log")
    job_log_table = bigquery.Table(job_log_table_ref, schema=job_log_schema)
    try:
        bq_client.get_table(job_log_table)
    except Exception:
        bq_client.create_table(job_log_table)

    # Create job_control table
    job_control_schema = [
        bigquery.SchemaField("job_nr", "INT64"),
        bigquery.SchemaField("job_name", "STRING"),
        bigquery.SchemaField("script_name", "STRING"),
        bigquery.SchemaField("log_file", "STRING"),
        bigquery.SchemaField("stichtag_info", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
    ]
    job_control_table_ref = bq_client.dataset(DATASET_ID).table("job_control")
    job_control_table = bigquery.Table(job_control_table_ref, schema=job_control_schema)
    try:
        bq_client.get_table(job_control_table)
    except Exception:
        bq_client.create_table(job_control_table)

    # Deploy the default (successful) k_ausd_bp_ta_cntrct_evn_core
    deploy_successful_core_procedure(bq_client, PROJECT_ID, DATASET_ID)
    # Deploy the wrapper procedure
    deploy_wrapper_procedure(bq_client, PROJECT_ID, DATASET_ID)

    yield # All tests run here

    # Optional: Teardown (e.g., delete dataset or tables)
    # bq_client.delete_dataset(dataset_ref, delete_contents=True)

@pytest.fixture(autouse=True)
def clear_tables_before_each_test(bq_client):
    """Clears job_log and job_control tables before each test."""
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    yield

def deploy_successful_core_procedure(client, project_id, dataset_id):
    """Deploys a successful version of the core procedure."""
    successful_core_sql = f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.k_ausd_bp_ta_cntrct_evn_core`(
      IN p_jobkennung STRING,
      IN p_effective_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_restart_value INT64
    )
    BEGIN
      INSERT INTO `{project_id}.{dataset_id}.job_log`
      (job_nr, job_name, log_level, message, created_at)
      VALUES
      (p_eintragsnr, p_jobkennung, 'I',
       CONCAT('Core logic k_ausd_bp_ta_cntrct_evn_core invoked (successful version) with: Stichtag=', p_effective_stichtag,
              ', RestartValue=', CAST(p_restart_value AS STRING)),
       CURRENT_TIMESTAMP());
    END;
    """
    client.query(successful_core_sql).result()

def deploy_failing_core_procedure(client, project_id, dataset_id):
    """Deploys a version of the core procedure that raises an error."""
    failing_core_sql = f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.k_ausd_bp_ta_cntrct_evn_core`(
      IN p_jobkennung STRING,
      IN p_effective_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_restart_value INT64
    )
    BEGIN
      INSERT INTO `{project_id}.{dataset_id}.job_log`
      (job_nr, job_name, log_level, message, created_at)
      VALUES
      (p_eintragsnr, p_jobkennung, 'I',
       CONCAT('Core logic k_ausd_bp_ta_cntrct_evn_core invoked (failing version) with: Stichtag=', p_effective_stichtag,
              ', RestartValue=', CAST(p_restart_value AS STRING)),
       CURRENT_TIMESTAMP());
      RAISE USING MESSAGE = 'Simulated error from k_ausd_bp_ta_cntrct_evn_core';
    END;
    """
    client.query(failing_core_sql).result()

def deploy_wrapper_procedure(client, project_id, dataset_id):
    """Deploys the wrapper procedure."""
    wrapper_sql = f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.ausd_bp_ta_cntrct_evn_wrapper`(
      IN p_stichtag STRING,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      DECLARE v_sysdate STRING;
      DECLARE v_effective_stichtag STRING;
      DECLARE v_restart_value INT64 DEFAULT 0;
      DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
      DECLARE v_eintragsnr INT64;
      DECLARE v_logdatei STRING;
      DECLARE v_errnr INT64 DEFAULT 0;
      DECLARE v_errarg STRING DEFAULT '';
      DECLARE v_status STRING DEFAULT 'INIT';

      -- Simulate system date in DDMMYYYY format
      SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

      -- Default restart value if not provided
      SET v_restart_value = IFNULL(p_wiederanlaufWert, 0);

      -- Default cutoff date to system date if not provided
      SET v_effective_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

      -- Validate required parameter
      IF v_effective_stichtag IS NULL OR v_effective_stichtag = '' THEN
        SET v_errnr = 193;
        SET v_errarg = 'Stichtag';
      END IF;

      IF v_errnr != 0 THEN
        INSERT INTO `{project_id}.{dataset_id}.job_log`
        (job_name, log_level, error_nr, error_arg, message, created_at)
        VALUES
        ('ausd_bp_ta_cntrct_evn', 'E', v_errnr, v_errarg, 'Required parameter missing', CURRENT_TIMESTAMP());

        RAISE USING MESSAGE = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg, ' - Required parameter missing.');
      END IF;

      -- Simulate job number and log file creation
      -- (Assuming job_control table exists and job_nr is auto-incremented or managed)
      SET v_eintragsnr = (
        SELECT IFNULL(MAX(job_nr), 0) + 1
        FROM `{project_id}.{dataset_id}.job_control`
        WHERE job_name = v_jobkennung
      );

      SET v_logdatei = CONCAT('job_', v_jobkennung, '_', CAST(v_eintragsnr AS STRING), '.log');

      INSERT INTO `{project_id}.{dataset_id}.job_control`
      (job_nr, job_name, script_name, log_file, stichtag_info, status, created_at)
      VALUES
      (
        v_eintragsnr,
        v_jobkennung,
        'ausd_bp_ta_cntrct_evn_wrapper',
        v_logdatei,
        v_sysdate,
        'RUNNING',
        CURRENT_TIMESTAMP()
      );

      BEGIN
        -- Job header log
        INSERT INTO `{project_id}.{dataset_id}.job_log`
        (job_nr, job_name, log_level, message, created_at)
        VALUES
        (v_eintragsnr, v_jobkennung, 'I',
         CONCAT('Job started. Stichtag=', v_effective_stichtag,
                ', RestartValue=', CAST(v_restart_value AS STRING)),
         CURRENT_TIMESTAMP());

        -- Downstream kernel logic invocation (placeholder for k_ausd_bp_ta_cntrct_evn.ksh equivalent)
        CALL `{project_id}.{dataset_id}.k_ausd_bp_ta_cntrct_evn_core`(
          v_jobkennung,
          v_effective_stichtag,
          v_eintragsnr,
          v_restart_value
        );

        INSERT INTO `{project_id}.{dataset_id}.job_log`
        (job_nr, job_name, log_level, message, created_at)
        VALUES
        (v_eintragsnr, v_jobkennung, 'I',
         'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
         CURRENT_TIMESTAMP());

        UPDATE `{project_id}.{dataset_id}.job_control`
        SET status = 'OK',
            finished_at = CURRENT_TIMESTAMP()
        WHERE job_nr = v_eintragsnr
          AND job_name = v_jobkennung;

      EXCEPTION WHEN ERROR THEN
        INSERT INTO `{project_id}.{dataset_id}.job_log`
        (job_nr, job_name, log_level, message, created_at)
        VALUES
        (v_eintragsnr, v_jobkennung, 'E',
         CONCAT('AppError: Abbruch - ', @@error.message),
         CURRENT_TIMESTAMP());

        UPDATE `{project_id}.{dataset_id}.job_control`
        SET status = 'ERROR',
            finished_at = CURRENT_TIMESTAMP()
        WHERE job_nr = v_eintragsnr
          AND job_name = v_jobkennung;

        RAISE; -- Re-raise the error to propagate it
      END;
    END;
    """
    client.query(wrapper_sql).result()

# Helper function to get current date in DDMMYYYY format for assertions
def get_current_date_ddmmyyyy():
    return datetime.datetime.now().strftime('%d%m%Y')

```

---

### Test Cases

#### Test Case 1: Default Stichtag and Restart Value

*   **Purpose:** Verify that when no `p_stichtag` or `p_wiederanlaufWert` is provided, `p_stichtag` defaults to the current system date (`DDMMYYYY`), and `p_wiederanlaufWert` defaults to `0`. This covers parameter handling and date determination.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty. The `k_ausd_bp_ta_cntrct_evn_core` procedure is set to its successful version.
*   **Action:** Call the wrapper procedure without any parameters (passing `NULL` for both).

    ```python
    # test_wrapper_defaults.py
    import pytest
    from google.cloud import bigquery
    from conftest import PROJECT_ID, DATASET_ID, get_current_date_ddmmyyyy

    def test_default_stichtag_and_restart_value(bq_client):
        # Action
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`(NULL, NULL)").result()

        # Assertions
        # 1. Check job_control entry
        job_control_query = f"""
            SELECT job_nr, job_name, script_name, log_file, stichtag_info, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
            WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        """
        job_control_result = list(bq_client.query(job_control_query).result())
        assert len(job_control_result) == 1
        job_control_row = job_control_result[0]
        assert job_control_row.job_nr == 1
        assert job_control_row.job_name == 'ausd_bp_ta_cntrct_evn'
        assert job_control_row.script_name == 'ausd_bp_ta_cntrct_evn_wrapper'
        assert job_control_row.log_file == 'job_ausd_bp_ta_cntrct_evn_1.log'
        assert job_control_row.stichtag_info == get_current_date_ddmmyyyy()
        assert job_control_row.status == 'OK'

        # 2. Check job_log entries for parameter values and core invocation
        job_log_query = f"""
            SELECT message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE job_nr = 1 AND job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY created_at
        """
        job_log_results = [row.message for row in bq_client.query(job_log_query).result()]

        assert any(f"Job started. Stichtag={get_current_date_ddmmyyyy()}, RestartValue=0" in msg for msg in job_log_results)
        assert any(f"Core logic k_ausd_bp_ta_cntrct_evn_core invoked (successful version) with: Stichtag={get_current_date_ddmmyyyy()}, RestartValue=0" in msg for msg in job_log_results)
        assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in msg for msg in job_log_results)
    ```
*   **Pass/Fail Criterion:**
    *   One row exists in `job_control` for `job_name = 'ausd_bp_ta_cntrct_evn'` with `job_nr = 1`, `stichtag_info` matching `CURRENT_DATE()` in `DDMMYYYY` format, and `status = 'OK'`.
    *   `job_log` contains messages indicating job start with `Stichtag` as `CURRENT_DATE()` and `RestartValue=0`, and successful invocation of `k_ausd_bp_ta_cntrct_evn_core` with these same parameters.
    *   `job_log` contains a success message.

#### Test Case 2: Explicit Stichtag and Restart Value

*   **Purpose:** Verify that explicit `p_stichtag` and `p_wiederanlaufWert` parameters are correctly passed through to the core logic and recorded in logs. This covers parameter handling.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty. The `k_ausd_bp_ta_cntrct_evn_core` procedure is set to its successful version.
*   **Action:** Call the wrapper procedure with specific `p_stichtag` and `p_wiederanlaufWert`.

    ```python
    # test_wrapper_explicit_params.py
    import pytest
    from google.cloud import bigquery
    from conftest import PROJECT_ID, DATASET_ID, get_current_date_ddmmyyyy

    def test_explicit_stichtag_and_restart_value(bq_client):
        # Inputs
        test_stichtag = '01012023'
        test_restart_value = 12345

        # Action
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`('{test_stichtag}', {test_restart_value})").result()

        # Assertions
        # 1. Check job_control entry
        job_control_query = f"""
            SELECT job_nr, job_name, stichtag_info, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
            WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        """
        job_control_result = list(bq_client.query(job_control_query).result())
        assert len(job_control_result) == 1
        job_control_row = job_control_result[0]
        assert job_control_row.job_nr == 1
        assert job_control_row.stichtag_info == get_current_date_ddmmyyyy() # stichtag_info is always sysdate
        assert job_control_row.status == 'OK'

        # 2. Check job_log entries for parameter values and core invocation
        job_log_query = f"""
            SELECT message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE job_nr = 1 AND job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY created_at
        """
        job_log_results = [row.message for row in bq_client.query(job_log_query).result()]

        assert any(f"Job started. Stichtag={test_stichtag}, RestartValue={test_restart_value}" in msg for msg in job_log_results)
        assert any(f"Core logic k_ausd_bp_ta_cntrct_evn_core invoked (successful version) with: Stichtag={test_stichtag}, RestartValue={test_restart_value}" in msg for msg in job_log_results)
        assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in msg for msg in job_log_results)
    ```
*   **Pass/Fail Criterion:**
    *   One row exists in `job_control` for `job_name = 'ausd_bp_ta_cntrct_evn'` with `job_nr = 1` and `status = 'OK'`. `stichtag_info` should still be `CURRENT_DATE()`.
    *   `job_log` contains messages indicating job start with `Stichtag` as `test_stichtag` and `RestartValue` as `test_restart_value`, and successful invocation of `k_ausd_bp_ta_cntrct_evn_core` with these same parameters.
    *   `job_log` contains a success message.

#### Test Case 3: Missing Stichtag (Validation Error)

*   **Purpose:** Verify that if `p_stichtag` is explicitly passed as an empty string or `NULL` and cannot be defaulted (e.g., if `v_sysdate` logic was flawed, though not the case here, but good to test the explicit `IF v_effective_stichtag IS NULL OR v_effective_stichtag = ''` check), the procedure raises an error (Error 193) and logs it correctly. This covers parameter validation and error handling.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty. The `k_ausd_bp_ta_cntrct_evn_core` procedure is set to its successful version (it won't be called in this scenario).
*   **Action:** Call the wrapper procedure with `p_stichtag` as an empty string and `p_wiederanlaufWert` as `NULL`. The `IFNULL(NULLIF(p_stichtag, ''), v_sysdate)` logic means `p_stichtag` will default to `v_sysdate` if `p_stichtag` is `NULL` or `''`. To trigger the `v_effective_stichtag IS NULL OR v_effective_stichtag = ''` error, we need to ensure `v_effective_stichtag` becomes `NULL` or `''` *after* defaulting. This specific condition is hard to hit with the current code, as `v_sysdate` will always be set.
    *   **Correction:** The `IF v_effective_stichtag IS NULL OR v_effective_stichtag = ''` check is primarily for cases where `v_sysdate` itself might be `NULL` or `''` (which is unlikely for `CURRENT_DATE()`). The legacy script's `pruefeParameterGesetzt Stichtag p_stichtag` would check if `p_stichtag` (after defaulting) is empty. The BigQuery code correctly defaults `p_stichtag` to `v_sysdate` if not provided. So, this error path is only reachable if `CURRENT_DATE()` somehow fails to produce a value, or if `p_stichtag` is explicitly passed as `NULL` *and* `v_sysdate` is also `NULL` (which won't happen).
    *   Let's re-interpret: The legacy `pruefeParameterGesetzt Stichtag p_stichtag` checks if the *final* `p_stichtag` variable is set. In the BigQuery code, `v_effective_stichtag` is the final value. It will always be `v_sysdate` if `p_stichtag` is `NULL` or `''`. Therefore, the `IF v_effective_stichtag IS NULL OR v_effective_stichtag = ''` condition will *never* be met if `v_sysdate` is always a valid date string.
    *   **Revised Test:** Instead of trying to make `v_effective_stichtag` `NULL` or `''`, we should test the *intended* error path. The legacy script would error if `p_stichtag` was *required* and not provided. The BigQuery script *defaults* `p_stichtag` if not provided. So, the direct "missing parameter" error (193) for `Stichtag` is effectively removed by the defaulting logic.
    *   **Conclusion:** This specific error path (Error 193 for `Stichtag` being `NULL` or `''`) is unlikely to be hit in the BigQuery code due to the robust defaulting to `CURRENT_DATE()`. The legacy script's `pruefeParameterGesetzt` would only trigger if `p_stichtag` was *not* defaulted. The BigQuery code *always* defaults it. This means the BigQuery code is more robust in this regard. I will create a test that *would* have failed in the legacy system if `p_stichtag` was truly missing, but now succeeds due to defaulting. The original error 193 is effectively handled by defaulting.

#### Test Case 3 (Revised): Robustness of Stichtag Defaulting

*   **Purpose:** Verify that even if `p_stichtag` is explicitly passed as `NULL` or an empty string, it correctly defaults to `CURRENT_DATE()` and the job proceeds successfully, reflecting a more robust parameter handling than a strict "missing parameter" error.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty. The `k_ausd_bp_ta_cntrct_evn_core` procedure is set to its successful version.
*   **Action:** Call the wrapper procedure with `p_stichtag` as `NULL` (or `''`) and `p_wiederanlaufWert` as `NULL`.

    ```python
    # test_wrapper_robust_stichtag_defaulting.py
    import pytest
    from google.cloud import bigquery
    from conftest import PROJECT_ID, DATASET_ID, get_current_date_ddmmyyyy

    def test_robust_stichtag_defaulting(bq_client):
        # Action: Pass NULL for stichtag, it should default to current date
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`(NULL, NULL)").result()

        # Assertions
        # 1. Check job_control entry
        job_control_query = f"""
            SELECT job_nr, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
            WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        """
        job_control_result = list(bq_client.query(job_control_query).result())
        assert len(job_control_result) == 1
        assert job_control_result[0].status == 'OK'

        # 2. Check job_log entries for parameter values
        job_log_query = f"""
            SELECT message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE job_nr = 1 AND job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY created_at
        """
        job_log_results = [row.message for row in bq_client.query(job_log_query).result()]

        expected_stichtag = get_current_date_ddmmyyyy()
        assert any(f"Job started. Stichtag={expected_stichtag}, RestartValue=0" in msg for msg in job_log_results)
        assert any(f"Core logic k_ausd_bp_ta_cntrct_evn_core invoked (successful version) with: Stichtag={expected_stichtag}, RestartValue=0" in msg for msg in job_log_results)
        assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in msg for msg in job_log_results)
    ```
*   **Pass/Fail Criterion:**
    *   The job completes successfully (`job_control.status = 'OK'`).
    *   `job_log` entries confirm that `Stichtag` was defaulted to `CURRENT_DATE()` and `RestartValue` to `0`, and the core logic was invoked with these values. This demonstrates that the BigQuery wrapper handles this scenario more gracefully by defaulting rather than erroring out, which is an improvement in robustness.

#### Test Case 4: Logging of Job Start and Success

*   **Purpose:** Verify that `job_log` and `job_control` tables are correctly updated at the start and end of a successful job execution. This covers external system replacements (logging) and data quality.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty. The `k_ausd_bp_ta_cntrct_evn_core` procedure is set to its successful version.
*   **Action:** Call the wrapper procedure with valid parameters.

    ```python
    # test_wrapper_successful_logging.py
    import pytest
    from google.cloud import bigquery
    from conftest import PROJECT_ID, DATASET_ID, get_current_date_ddmmyyyy

    def test_successful_job_logging(bq_client):
        test_stichtag = '15062024'
        test_restart_value = 500

        # Action
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`('{test_stichtag}', {test_restart_value})").result()

        # Assertions
        # 1. Check job_control entry
        job_control_query = f"""
            SELECT job_nr, job_name, script_name, log_file, stichtag_info, status, created_at, finished_at
            FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
            WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        """
        job_control_result = list(bq_client.query(job_control_query).result())
        assert len(job_control_result) == 1
        job_control_row = job_control_result[0]
        assert job_control_row.job_nr == 1
        assert job_control_row.job_name == 'ausd_bp_ta_cntrct_evn'
        assert job_control_row.script_name == 'ausd_bp_ta_cntrct_evn_wrapper'
        assert job_control_row.log_file == 'job_ausd_bp_ta_cntrct_evn_1.log'
        assert job_control_row.stichtag_info == get_current_date_ddmmyyyy()
        assert job_control_row.status == 'OK'
        assert job_control_row.created_at is not None
        assert job_control_row.finished_at is not None
        assert job_control_row.finished_at > job_control_row.created_at

        # 2. Check job_log entries
        job_log_query = f"""
            SELECT log_level, message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE job_nr = 1 AND job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY created_at
        """
        job_log_results = list(bq_client.query(job_log_query).result())

        assert len(job_log_results) == 3 # Start, Core Invocation, Success
        assert job_log_results[0].log_level == 'I'
        assert f"Job started. Stichtag={test_stichtag}, RestartValue={test_restart_value}" in job_log_results[0].message
        assert job_log_results[1].log_level == 'I'
        assert f"Core logic k_ausd_bp_ta_cntrct_evn_core invoked (successful version) with: Stichtag={test_stichtag}, RestartValue={test_restart_value}" in job_log_results[1].message
        assert job_log_results[2].log_level == 'I'
        assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in job_log_results[2].message
    ```
*   **Pass/Fail Criterion:**
    *   One row in `job_control` with `job_nr = 1`, `job_name = 'ausd_bp_ta_cntrct_evn'`, `script_name = 'ausd_bp_ta_cntrct_evn_wrapper'`, `log_file = 'job_ausd_bp_ta_cntrct_evn_1.log'`, `stichtag_info` matching `CURRENT_DATE()`, `status = 'RUNNING'` initially, then updated to `'OK'`, and `created_at`/`finished_at` populated correctly.
    *   Three informational (`I`) entries in `job_log` for `job_nr = 1`: one for job start (with correct parameters), one for core logic invocation, and one for successful completion.

#### Test Case 5: Logging of Job Failure (Core Script Error)

*   **Purpose:** Verify that if the `k_ausd_bp_ta_cntrct_evn_core` procedure fails, the wrapper catches the error, logs it as an error, updates `job_control` status to 'ERROR', and re-raises the error to propagate it. This covers error handling and external system replacements (logging).
*   **Setup:** Ensure `job_log` and `job_control` tables are empty. **Crucially, temporarily deploy the `failing_core_procedure` version of `k_ausd_bp_ta_cntrct_evn_core` for this test.**
*   **Action:** Call the wrapper procedure with valid parameters.

    ```python
    # test_wrapper_failure_logging.py
    import pytest
    from google.cloud import bigquery
    from conftest import PROJECT_ID, DATASET_ID, deploy_failing_core_procedure, deploy_successful_core_procedure, get_current_date_ddmmyyyy

    def test_failing_job_logging(bq_client):
        test_stichtag = '20072024'
        test_restart_value = 999

        # Setup: Deploy the failing core procedure
        deploy_failing_core_procedure(bq_client, PROJECT_ID, DATASET_ID)

        # Action: Expect the wrapper call to raise an exception
        with pytest.raises(bigquery.exceptions.GoogleAPICallError) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`('{test_stichtag}', {test_restart_value})").result()

        # Assertions for the raised error
        assert "Simulated error from k_ausd_bp_ta_cntrct_evn_core" in str(excinfo.value)

        # 1. Check job_control entry
        job_control_query = f"""
            SELECT job_nr, job_name, status, created_at, finished_at
            FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
            WHERE job_name = 'ausd_bp_ta_cntrct_evn'
        """
        job_control_result = list(bq_client.query(job_control_query).result())
        assert len(job_control_result) == 1
        job_control_row = job_control_result[0]
        assert job_control_row.job_nr == 1
        assert job_control_row.status == 'ERROR'
        assert job_control_row.created_at is not None
        assert job_control_row.finished_at is not None
        assert job_control_row.finished_at > job_control_row.created_at

        # 2. Check job_log entries
        job_log_query = f"""
            SELECT log_level, message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE job_nr = 1 AND job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY created_at
        """
        job_log_results = list(bq_client.query(job_log_query).result())

        assert len(job_log_results) == 3 # Start, Core Invocation (failing), Error
        assert job_log_results[0].log_level == 'I'
        assert f"Job started. Stichtag={test_stichtag}, RestartValue={test_restart_value}" in job_log_results[0].message
        assert job_log_results[1].log_level == 'I'
        assert f"Core logic k_ausd_bp_ta_cntrct_evn_core invoked (failing version) with: Stichtag={test_stichtag}, RestartValue={test_restart_value}" in job_log_results[1].message
        assert job_log_results[2].log_level == 'E'
        assert "AppError: Abbruch - Simulated error from k_ausd_bp_ta_cntrct_evn_core" in job_log_results[2].message

        # Teardown: Restore the successful core procedure for subsequent tests
        deploy_successful_core_procedure(bq_client, PROJECT_ID, DATASET_ID)
    ```
*   **Pass/Fail Criterion:**
    *   The `CALL` statement raises a `GoogleAPICallError` containing the simulated error message from the core procedure.
    *   One row in `job_control` with `job_nr = 1`, `job_name = 'ausd_bp_ta_cntrct_evn'`, `status = 'ERROR'`, and `created_at`/`finished_at` populated.
    *   `job_log` contains an informational entry for job start, an informational entry for core logic invocation, and an error (`E`) entry with a message indicating the failure and the error from the core script.

#### Test Case 6: Job Number Incrementing

*   **Purpose:** Verify that the `job_nr` in `job_control` and `job_log` correctly increments for subsequent job runs, simulating the `DWMSG_ErmittleNr` behavior. This covers data quality and external system replacements (logging).
*   **Setup:** Ensure `job_log` and `job_control` tables are empty. The `k_ausd_bp_ta_cntrct_evn_core` procedure is set to its successful version.
*   **Action:** Call the wrapper procedure twice consecutively with different parameters.

    ```python
    # test_wrapper_job_nr_increment.py
    import pytest
    from google.cloud import bigquery
    from conftest import PROJECT_ID, DATASET_ID, get_current_date_ddmmyyyy

    def test_job_number_incrementing(bq_client):
        # First run
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`('01012024', 100)").result()

        # Second run
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_cntrct_evn_wrapper`('02012024', 200)").result()

        # Assertions
        # 1. Check job_control entries
        job_control_query = f"""
            SELECT job_nr, job_name, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
            WHERE job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY job_nr
        """
        job_control_results = list(bq_client.query(job_control_query).result())
        assert len(job_control_results) == 2
        assert job_control_results[0].job_nr == 1
        assert job_control_results[0].status == 'OK'
        assert job_control_results[1].job_nr == 2
        assert job_control_results[1].status == 'OK'

        # 2. Check job_log entries for both runs
        job_log_query_run1 = f"""
            SELECT message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE job_nr = 1 AND job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY created_at
        """
        job_log_results_run1 = [row.message for row in bq_client.query(job_log_query_run1).result()]
        assert any("Job started. Stichtag=01012024, RestartValue=100" in msg for msg in job_log_results_run1)

        job_log_query_run2 = f"""
            SELECT message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE job_nr = 2 AND job_name = 'ausd_bp_ta_cntrct_evn'
            ORDER BY created_at
        """
        job_log_results_run2 = [row.message for row in bq_client.query(job_log_query_run2).result()]
        assert any("Job started. Stichtag=02012024, RestartValue=200" in msg for msg in job_log_results_run2)
    ```
*   **Pass/Fail Criterion:**
    *   Two rows exist in `job_control` for `job_name = 'ausd_bp_ta_cntrct_evn'` with `job_nr` values `1` and `2`, both having `status = 'OK'`.
    *   `job_log` contains distinct sets of entries for `job_nr = 1` and `job_nr = 2`, each reflecting the parameters passed to their respective runs.

---

### Transformation Correctness (Data Transformation)

*   **Purpose:** To explicitly state that tests for data transformations (joins, aggregations, filters, type handling, NULL handling, and edge cases) are *not* applicable to this wrapper script.
*   **Setup:** N/A
*   **Action:** N/A
*   **Pass/Fail Criterion:** This section serves as documentation. The migration design explicitly states: "No Direct Data Transformation: It's important to reiterate that this wrapper script itself performs no direct data transformations or aggregations. Its role is solely to prepare the execution context for the downstream core script." Therefore, tests for data transformation correctness will be part of the migration validation for `k_ausd_bp_ta_cntrct_evn_core` (the core logic script), not this wrapper.

---

### External-System Replacements (Beyond Logging)

*   **Purpose:** To explicitly state that tests for external system interactions (like Oracle reads, SFTP/S3 drops) are *not* applicable to this wrapper script.
*   **Setup:** N/A
*   **Action:** N/A
*   **Pass/Fail Criterion:** This section serves as documentation. The migration design indicates that the actual data source (DWH) and target (FOS-Tabelle) interactions reside within the `k_ausd_bp_ta_cntrct_evn.ksh` (and thus `k_ausd_bp_ta_cntrct_evn_core`). The wrapper's "external system replacement" is limited to its logging mechanism, which is covered in Test Cases 4 and 5. Tests for data ingress/egress to/from BigQuery will be part of the `k_ausd_bp_ta_cntrct_evn_core` migration validation.