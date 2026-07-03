# Migration Validation Test Suite: `k_ausd_bp_ta_msisdn.ksh`

This document defines the migration-validation tests to prove that the migrated Google Cloud BigQuery stored procedures and Apache Airflow DAG are behaviorally equivalent to the legacy Oracle/Unix shell-based job `k_ausd_bp_ta_msisdn.ksh`.

---

## Section 1: Parameter Validation & Error Logging Tests

### Test Case 1.1: Missing Required Parameters (Jobkennung)
* **Purpose:** Verify that the stored procedure rejects execution and logs a structured error when `p_JobKennung` is missing or empty, matching the legacy shell script's validation behavior.
* **Setup:** Ensure the target dataset `dw_isbert_dataset` exists and the tables `error_log`, `job_table`, and `process_log` are truncated.
* **Action:** Execute the stored procedure with an empty `p_JobKennung`.
  ```sql
  -- Run with empty Jobkennung
  DECLARE v_error_thrown BOOLEAN DEFAULT FALSE;
  BEGIN
    CALL `gcp-project-placeholder.dw_isbert_dataset.r_ausd_bp_ta_msisdn`(
      '',     -- p_JobKennung (Empty)
      '12345', -- p_EintragsNr
      '31122024', -- p_Stichtag
      '0'     -- p_wiederanlaufWert
    );
  EXCEPTION WHEN ERROR THEN
    SET v_error_thrown = TRUE;
  END;
  ```
* **Pass/Fail Criterion:** 
  * The procedure must raise an exception (`v_error_thrown` is `TRUE`).
  * A query to the `error_log` table must return exactly 1 row matching the validation failure:
    ```sql
    SELECT COUNT(1) FROM `gcp-project-placeholder.dw_isbert_dataset.error_log`
    WHERE job_name = 'r_ausd_bp_ta_msisdn'
      AND error_nr = 1
      AND error_arg = 'Jobkennung'
      AND error_text = 'Bitte ueber Rahmenscript aufrufen';
    -- ASSERT count == 1
    ```

### Test Case 1.2: Invalid Date Format Validation
* **Purpose:** Verify that the stored procedure validates the `p_Stichtag` parameter against the legacy format `DDMMYYYY` and raises a structured error on failure.
* **Setup:** Truncate the `error_log` table.
* **Action:** Execute the stored procedure with an invalid date format (e.g., ISO format `2024-12-31` or alphanumeric string).
  ```sql
  DECLARE v_error_thrown BOOLEAN DEFAULT FALSE;
  BEGIN
    CALL `gcp-project-placeholder.dw_isbert_dataset.r_ausd_bp_ta_msisdn`(
      'JOB_TEST_01',
      '12345',
      '2024-12-31', -- Invalid format (Expected: DDMMYYYY)
      '0'
    );
  EXCEPTION WHEN ERROR THEN
    SET v_error_thrown = TRUE;
  END;
  ```
* **Pass/Fail Criterion:**
  * The procedure must raise an exception.
  * The `error_log` table must capture the invalid date error:
    ```sql
    SELECT COUNT(1) FROM `gcp-project-placeholder.dw_isbert_dataset.error_log`
    WHERE job_name = 'r_ausd_bp_ta_msisdn'
      AND error_nr = 2
      AND error_arg = '2024-12-31'
      AND error_text = 'Ungueltiges Datum';
    -- ASSERT count == 1
    ```

---

## Section 2: Transformation & Date-Handling Correctness

### Test Case 2.1: Defaulting of Restart Value
* **Purpose:** Verify that if the restart value (`p_wiederanlaufWert`) is passed as `NULL` or an empty string, it defaults to `'0'` in downstream processing and logging.
* **Setup:** Truncate `process_log` and `job_table`.
* **Action:** Execute the stored procedure with `p_wiederanlaufWert` set to `NULL`.
  ```sql
  CALL `gcp-project-placeholder.dw_isbert_dataset.r_ausd_bp_ta_msisdn`(
    'JOB_TEST_RESTART',
    '99999',
    '15102024',
    NULL -- Null restart value
  );
  ```
* **Pass/Fail Criterion:**
  * The procedure completes successfully.
  * The `process_log` table must record the defaulted value `'0'`:
    ```sql
    SELECT restart_value 
    FROM `gcp-project-placeholder.dw_isbert_dataset.process_log`
    WHERE job_kennung = 'JOB_TEST_RESTART' AND eintrags_nr = '99999';
    -- ASSERT result == '0'
    ```

### Test Case 2.2: Date Arithmetic and Staging Context
* **Purpose:** Verify that the core execution wrapper (`sp_execute_core_sql`) correctly parses the `p_Stichtag` parameter and computes relative dates (`v_today`, `v_yesterday`) matching legacy behavior.
* **Setup:** Truncate `process_log`.
* **Action:** Call the core execution procedure directly with a fixed date.
  ```sql
  DECLARE v_records INT64;
  CALL `gcp-project-placeholder.dw_isbert_dataset.sp_execute_core_sql`(
    'r_ausd_bp_ta_msisdn',
    'PoolBasisprodukt',
    DATE '2024-10-15', -- Parsed from '15102024'
    CURRENT_DATE(),
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
    '0',
    v_records
  );
  ```
* **Pass/Fail Criterion:**
  * The temporary table `tmp_core_result` must contain the correctly mapped date variables:
    ```sql
    SELECT stichtag_date, today_date, yesterday_date 
    FROM tmp_core_result;
    -- ASSERT stichtag_date == DATE '2024-10-15'
    -- ASSERT today_date == CURRENT_DATE()
    -- ASSERT yesterday_date == CURRENT_DATE() - 1
    ```

---

## Section 3: End-to-End Execution & Audit Logging

### Test Case 3.1: Successful Job Execution and Audit Footprint
* **Purpose:** Verify that a successful execution of the stored procedure creates the correct audit entries in both `job_table` (equivalent to the legacy `FOSJobErzeugeEintrag` catalog) and `process_log`.
* **Setup:** Truncate `job_table` and `process_log`.
* **Action:** Execute the stored procedure with valid parameters.
  ```sql
  CALL `gcp-project-placeholder.dw_isbert_dataset.r_ausd_bp_ta_msisdn`(
    'JOB_SUCCESS_01',
    '88888',
    '25122024',
    '1'
  );
  ```
* **Pass/Fail Criterion:**
  * Exactly one entry is created in `job_table` with status flags `'A'` and `'I'`, matching the legacy catalog registration:
    ```sql
    SELECT tab_name, status_a, status_i, start_date, end_date, job_type, restart_flag, record_count, description
    FROM `gcp-project-placeholder.dw_isbert_dataset.job_table`
    WHERE tab_name = 'PoolBasisprodukt' AND start_date = DATE '2024-12-25';
    -- ASSERT status_a == 'A'
    -- ASSERT status_i == 'I'
    -- ASSERT job_type == 'J'
    -- ASSERT restart_flag == 'N'
    -- ASSERT description == 'Initialbefuellung'
    ```
  * Exactly one entry is created in `process_log` capturing the execution metadata:
    ```sql
    SELECT job_name, job_kennung, eintrags_nr, stichtag, restart_value
    FROM `gcp-project-placeholder.dw_isbert_dataset.process_log`
    WHERE job_kennung = 'JOB_SUCCESS_01';
    -- ASSERT job_name == 'r_ausd_bp_ta_msisdn'
    -- ASSERT eintrags_nr == '88888'
    -- ASSERT stichtag == DATE '2024-12-25'
    -- ASSERT restart_value == '1'
    ```

---

## Section 4: Airflow DAG Integration Tests

### Test Case 4.1: DAG Parameter Parsing and Execution
* **Purpose:** Verify that the Airflow DAG `k_ausd_bp_ta_msisdn` correctly parses runtime configuration parameters and passes them to the BigQuery stored procedure.
* **Setup:** Deploy the DAG `k_ausd_bp_ta_msisdn_dag.py` to a Cloud Composer environment. Truncate `process_log`.
* **Action:** Trigger the DAG via the Airflow CLI or UI with a custom JSON configuration:
  ```json
  {
    "p_JobKennung": "AIRFLOW_TEST_JOB",
    "p_EintragsNr": "77777",
    "p_Stichtag": "01012025",
    "p_wiederanlaufWert": "2"
  }
  ```
* **Pass/Fail Criterion:**
  * The Airflow task `execute_r_ausd_bp_ta_msisdn` completes with a `SUCCESS` status.
  * The BigQuery `process_log` table contains the record matching the parameters passed via the Airflow conf:
    ```sql
    SELECT COUNT(1) 
    FROM `gcp-project-placeholder.dw_isbert_dataset.process_log`
    WHERE job_kennung = 'AIRFLOW_TEST_JOB'
      AND eintrags_nr = '77777'
      AND stichtag = DATE '2025-01-01'
      AND restart_value = '2';
    -- ASSERT count == 1
    ```

---

## Section 5: Automated Pytest Validation Suite

The following Python test suite uses `pytest` and the Google Cloud BigQuery client library to automate the execution of the validation scenarios defined above.

```python
# File: test_k_ausd_bp_ta_msisdn.py
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

PROJECT_ID = "gcp-project-placeholder"
DATASET_ID = "dw_isbert_dataset"
PROCEDURE_NAME = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_msisdn"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def clean_logs(bq_client):
    """Truncate log tables before each test run."""
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.process_log`").result()
    yield

def test_missing_job_kennung_raises_error(bq_client):
    sql = f"CALL `{PROCEDURE_NAME}`('', '12345', '31122024', '0')"
    
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(sql).result()
    
    assert "FEHLER: 0 E 1 Jobkennung" in str(excinfo.value)

    # Verify error log entry
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.error_log` WHERE error_arg = 'Jobkennung'"
    results = list(bq_client.query(query).result())
    assert len(results) == 1
    assert results[0]["error_nr"] == 1
    assert results[0]["error_text"] == "Bitte ueber Rahmenscript aufrufen"

def test_invalid_date_format_raises_error(bq_client):
    sql = f"CALL `{PROCEDURE_NAME}`('JOB_TEST', '12345', '2024-12-31', '0')"
    
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(sql).result()
        
    assert "Ungueltiges Datum" in str(excinfo.value)

    # Verify error log entry
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.error_log` WHERE error_nr = 2"
    results = list(bq_client.query(query).result())
    assert len(results) == 1
    assert results[0]["error_arg"] == "2024-12-31"

def test_successful_execution_creates_audit_trail(bq_client):
    sql = f"CALL `{PROCEDURE_NAME}`('JOB_OK', '55555', '15102024', '')"
    bq_client.query(sql).result()

    # Verify job_table entry
    job_query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_table` 
        WHERE tab_name = 'PoolBasisprodukt' AND start_date = '2024-10-15'
    """
    job_results = list(bq_client.query(job_query).result())
    assert len(job_results) == 1
    assert job_results[0]["status_a"] == "A"
    assert job_results[0]["status_i"] == "I"
    assert job_results[0]["restart_flag"] == "N"

    # Verify process_log entry (including defaulted restart value)
    proc_query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.process_log` 
        WHERE job_kennung = 'JOB_OK'
    """
    proc_results = list(bq_client.query(proc_query).result())
    assert len(proc_results) == 1
    assert proc_results[0]["eintrags_nr"] == "55555"
    assert proc_results[0]["restart_value"] == "0"
```