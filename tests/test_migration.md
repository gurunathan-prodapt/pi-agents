The migration of `k_ausd_v_ta_c_bfc.ksh` to a BigQuery Stored Procedure involves significant changes in orchestration, error handling, and data processing. The following tests aim to ensure behavioral equivalence and correctness across these areas.

**Assumptions:**
*   `your_project.your_dataset` is the target BigQuery project and dataset.
*   A separate `your_project.test_dataset` exists for setting up test data and validating results.
*   The actual content of `d_ausd_v_ta_c_bfc.sql` was correctly translated into the BigQuery stored procedure's DML logic.
*   The `bfc_get_bindefrist` UDF's logic, while a placeholder in the provided DDL, will be correctly implemented to match the Oracle `Cds$vr_Bindefrist.GetBindeFrist` function. For testing purposes, we will use the placeholder logic.
*   The `v_max_update_limit` (Oracle `ROWNUM` equivalent) behavior difference is acknowledged. The BigQuery procedure will update all matching rows, not just a limited set. This is a known divergence.
*   The `v_bfc_procedure_date` in BigQuery uses `CURRENT_DATE()`, which might differ from the Oracle `all_objects.created` date. For output parity, this date needs to be controlled or understood. For these tests, we'll assume `CURRENT_DATE()` is the intended behavior for the migrated job.

---

## 1. Test: Parameter Validation - Missing `p_job_kennung`

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles missing `p_job_kennung` by signaling an error and logging it, mirroring the legacy script's `ErrNr=192` behavior.
*   **Setup:**
    1.  Ensure `job_error_log` and `job_run_log` tables are empty.
    2.  No specific data in source or target tables is required for this test.
*   **Action:** Execute the BigQuery Stored Procedure `r_ausd_ta_c_bfc` with `p_job_kennung` as `NULL` or an empty string, and a valid `p_eintrags_nr`.
*   **Pass/Fail Criteria:**
    *   The procedure execution fails with an error message indicating a missing `p_job_kennung`.
    *   An entry is recorded in `your_project.your_dataset.job_error_log` with `error_code` '192' and an appropriate `error_message`.
    *   No `START` or `END` entries are found in `job_run_log` for this `run_id`.
    *   No changes occur in `job_control_table`.

*   **Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery
import time

PROJECT_ID = "your_project"
DATASET_ID = "your_dataset"
PROCEDURE_ID = "r_ausd_ta_c_bfc"

client = bigquery.Client(project=PROJECT_ID)

def test_missing_job_kennung_parameter():
    job_kennung = "" # Test with empty string, or None for NULL
    eintrags_nr = "TEST_ENTRY_001"
    
    # Clear log tables for a clean test run
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control_table`").result()

    # Action: Call the stored procedure
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_ID}`('{job_kennung}', '{eintrags_nr}');
    """
    
    job = client.query(query)
    
    # Pass/Fail Criteria: Procedure should fail
    with pytest.raises(Exception) as excinfo:
        job.result() # This will raise an exception if the procedure signals an error
    
    assert "Parameter p_job_kennung cannot be NULL or empty." in str(excinfo.value)

    # Verify error log entry
    error_log_query = f"""
    SELECT error_code, error_message, job_kenn_ung, eintrags_nr
    FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
    WHERE eintrags_nr = '{eintrags_nr}'
    ORDER BY error_timestamp DESC
    LIMIT 1;
    """
    error_log_results = client.query(error_log_query).result()
    assert error_log_results.total_rows == 1
    error_row = next(iter(error_log_results))
    assert error_row.error_code == '192'
    assert "Parameter p_job_kennung cannot be NULL or empty." in error_row.error_message
    assert error_row.job_kenn_ung == job_kennung # Should log the empty string
    assert error_row.eintrags_nr == eintrags_nr

    # Verify no run log entries
    run_log_query = f"""
    SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE eintrags_nr = '{eintrags_nr}';
    """
    run_log_results = client.query(run_log_query).result()
    assert next(iter(run_log_results)).f0_ == 0

    # Verify no job control entries
    job_control_query = f"""
    SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table` WHERE eintrags_nr = '{eintrags_nr}';
    """
    job_control_results = client.query(job_control_query).result()
    assert next(iter(job_control_results)).f0_ == 0

```

---

## 2. Test: Parameter Validation - Missing `p_eintrags_nr`

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles missing `p_eintrags_nr` by signaling an error and logging it, mirroring the legacy script's `ErrNr=193` behavior.
*   **Setup:**
    1.  Ensure `job_error_log` and `job_run_log` tables are empty.
    2.  No specific data in source or target tables is required.
*   **Action:** Execute the BigQuery Stored Procedure `r_ausd_ta_c_bfc` with a valid `p_job_kennung` and `p_eintrags_nr` as `NULL` or an empty string.
*   **Pass/Fail Criteria:**
    *   The procedure execution fails with an error message indicating a missing `p_eintrags_nr`.
    *   An entry is recorded in `your_project.your_dataset.job_error_log` with `error_code` '193' and an appropriate `error_message`.
    *   No `START` or `END` entries are found in `job_run_log` for this `run_id`.
    *   No changes occur in `job_control_table`.

*   **Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery
import time

PROJECT_ID = "your_project"
DATASET_ID = "your_dataset"
PROCEDURE_ID = "r_ausd_ta_c_bfc"

client = bigquery.Client(project=PROJECT_ID)

def test_missing_eintrags_nr_parameter():
    job_kennung = "TEST_JOB_002"
    eintrags_nr = "" # Test with empty string, or None for NULL
    
    # Clear log tables for a clean test run
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control_table`").result()

    # Action: Call the stored procedure
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_ID}`('{job_kennung}', '{eintrags_nr}');
    """
    
    job = client.query(query)
    
    # Pass/Fail Criteria: Procedure should fail
    with pytest.raises(Exception) as excinfo:
        job.result()
    
    assert "Parameter p_eintrags_nr cannot be NULL or empty." in str(excinfo.value)

    # Verify error log entry
    error_log_query = f"""
    SELECT error_code, error_message, job_kenn_ung, eintrags_nr
    FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
    WHERE job_kenn_ung = '{job_kennung}'
    ORDER BY error_timestamp DESC
    LIMIT 1;
    """
    error_log_results = client.query(error_log_query).result()
    assert error_log_results.total_rows == 1
    error_row = next(iter(error_log_results))
    assert error_row.error_code == '193'
    assert "Parameter p_eintrags_nr cannot be NULL or empty." in error_row.error_message
    assert error_row.job_kenn_ung == job_kennung
    assert error_row.eintrags_nr == eintrags_nr # Should log the empty string

    # Verify no run log entries
    run_log_query = f"""
    SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kenn_ung = '{job_kennung}';
    """
    run_log_results = client.query(run_log_query).result()
    assert next(iter(run_log_results)).f0_ == 0

    # Verify no job control entries
    job_control_query = f"""
    SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table` WHERE job_kenn_ung = '{job_kennung}';
    """
    job_control_results = client.query(job_control_query).result()
    assert next(iter(job_control_results)).f0_ == 0

```

---

## 3. Test: Job State Management - Deactivate Old Jobs

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly deactivates previously active jobs with the same `p_job_kennung` before starting a new run.
*   **Setup:**
    1.  Insert a record into `your_project.your_dataset.job_control_table` with `status = 'ACTIVE'` for a specific `p_job_kennung`.
    2.  Ensure `job_error_log` and `job_run_log` tables are empty.
    3.  Populate source tables with minimal valid data to allow the procedure to run successfully (e.g., one row in `sof_ta_cntrct_crs`).
*   **Action:** Execute the BigQuery Stored Procedure `r_ausd_ta_c_bfc` with the same `p_job_kennung` as the pre-existing active job, and a new `p_eintrags_nr`.
*   **Pass/Fail Criteria:**
    *   The procedure completes successfully.
    *   The pre-existing job in `job_control_table` with the same `p_job_kennung` has its `status` updated to 'INACTIVE' and `end_timestamp`, `last_update_timestamp` populated.
    *   A new entry for the current run is created in `job_control_table` with `status = 'COMPLETED'` (after successful run).
    *   `START` and `END` entries are present in `job_run_log` for the new run.

*   **Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery
import datetime
import time

PROJECT_ID = "your_project"
DATASET_ID = "your_dataset"
PROCEDURE_ID = "r_ausd_ta_c_bfc"
TEST_DATASET_ID = "test_dataset" # Assuming a test dataset for source data

client = bigquery.Client(project=PROJECT_ID)

def setup_source_data():
    # Ensure source tables exist and have minimal data for successful run
    client.query(f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs` (
        cntrct_id STRING NOT NULL, commitment_reference_date DATE, cntrct_validity_id STRING, bfc_age DATE
    );
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_barrier` (cntrct_id STRING NOT NULL, bfc_age DATE);
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_valid` (cntrct_validity_id STRING NOT NULL, first_period_id STRING, following_period_id STRING, first_notice_period_id STRING, follow_notice_period_id STRING, bfc_age DATE);
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_period` (period_id STRING NOT NULL, bfc_age DATE);
    """).result()

    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_barrier`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_valid`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_period`").result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age)
    VALUES ('C1', '2023-01-01', 'V1', '2023-01-15');
    """).result()
    
    # Also ensure target table is empty or has data that will be merged
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc`").result()


def test_deactivate_old_jobs():
    job_kennung = "JOB_DEACTIVATION_TEST"
    old_eintrags_nr = "OLD_ENTRY_001"
    new_eintrags_nr = "NEW_ENTRY_002"
    
    setup_source_data()

    # Clear log and control tables
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control_table`").result()

    # Setup: Insert an active job entry
    old_run_id = "OLD_RUN_UUID_123"
    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_control_table`
    (job_kenn_ung, eintrags_nr, run_id, status, start_timestamp, last_update_timestamp)
    VALUES
    ('{job_kennung}', '{old_eintrags_nr}', '{old_run_id}', 'ACTIVE', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
    """).result()
    time.sleep(1) # Ensure timestamps are different for new run

    # Action: Call the stored procedure
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_ID}`('{job_kennung}', '{new_eintrags_nr}');
    """
    job = client.query(query)
    job.result() # Should complete successfully

    # Pass/Fail Criteria: Verify old job status
    old_job_status_query = f"""
    SELECT status, end_timestamp, last_update_timestamp
    FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{old_eintrags_nr}';
    """
    old_job_results = client.query(old_job_status_query).result()
    assert old_job_results.total_rows == 1
    old_job_row = next(iter(old_job_results))
    assert old_job_row.status == 'INACTIVE'
    assert old_job_row.end_timestamp is not None
    assert old_job_row.last_update_timestamp is not None

    # Verify new job entry
    new_job_status_query = f"""
    SELECT status, run_id
    FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{new_eintrags_nr}';
    """
    new_job_results = client.query(new_job_status_query).result()
    assert new_job_results.total_rows == 1
    new_job_row = next(iter(new_job_results))
    assert new_job_row.status == 'COMPLETED'
    assert new_job_row.run_id is not None

    # Verify run log entries for new job
    run_log_query = f"""
    SELECT event_type, event_message, record_count
    FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{new_eintrags_nr}'
    ORDER BY event_timestamp;
    """
    run_log_results = client.query(run_log_query).result()
    assert run_log_results.total_rows == 2 # START and END
    log_entries = list(run_log_results)
    assert log_entries[0].event_type == 'START'
    assert log_entries[1].event_type == 'END'
    assert log_entries[1].record_count >= 0 # At least 0 records processed by the final UPDATE

```

---

## 4. Test: Core Data Transformation - Happy Path (Output Parity)

*   **Purpose:** Verify that the core data transformation logic within the BigQuery Stored Procedure produces the same output in `ta_c_bfc` as the legacy `d_ausd_v_ta_c_bfc.sql` script, given identical input data. This covers joins, aggregations, filters, and UDF usage.
*   **Setup:**
    1.  **Crucial:** Obtain a representative dataset from the legacy Oracle source tables (`sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`) and load it into `your_project.test_dataset`.
    2.  Obtain the expected output for `ta_c_bfc` from a successful run of the legacy job with the *exact same* input data, and load it into a reference table: `your_project.test_dataset.ta_c_bfc_expected`.
    3.  Ensure `your_project.your_dataset.ta_c_bfc` is empty.
    4.  Ensure `job_error_log`, `job_run_log`, `job_control_table` are empty.
    5.  **Important:** If `v_bfc_procedure_date` (which is `CURRENT_DATE()` in BQ) or the `bfc_get_bindefrist` UDF's logic depends on the current date, ensure consistency. For true parity, the `v_bfc_procedure_date` in the BigQuery procedure might need to be hardcoded to match the date of the legacy run, or the UDF logic must be deterministic. For this test, we assume `CURRENT_DATE()` is acceptable for the migrated behavior.
*   **Action:** Execute the BigQuery Stored Procedure `r_ausd_ta_c_bfc` with valid `p_job_kennung` and `p_eintrags_nr`.
*   **Pass/Fail Criteria:**
    *   The procedure completes successfully.
    *   The content of `your_project.your_dataset.ta_c_bfc` is identical to `your_project.test_dataset.ta_c_bfc_expected`. This includes row counts, all column values, and data types.
    *   `START` and `END` entries are present in `job_run_log`.
    *   `job_control_table` shows the job as `COMPLETED`.

*   **Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "your_project"
DATASET_ID = "your_dataset"
PROCEDURE_ID = "r_ausd_ta_c_bfc"
TEST_DATASET_ID = "test_dataset"

client = bigquery.Client(project=PROJECT_ID)

def setup_parity_test_data():
    # Clear and populate source tables in test_dataset
    # This data should be a faithful representation of the Oracle source data
    # used to generate the legacy expected output.
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_barrier`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_valid`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_period`").result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age)
    VALUES
        ('C1', '2023-01-01', 'V1', '2023-01-15'),
        ('C2', '2023-02-01', 'V2', '2023-02-10'),
        ('C3', '2023-03-01', 'V1', '2023-03-05'),
        ('C4', '2023-04-01', NULL, '2023-04-01');
    """).result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_barrier` (cntrct_id, bfc_age)
    VALUES
        ('C1', '2023-01-20'),
        ('C3', '2023-03-10');
    """).result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_valid` (cntrct_validity_id, first_period_id, bfc_age)
    VALUES
        ('V1', 'P1', '2023-01-25'),
        ('V2', 'P2', '2023-02-12');
    """).result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_period` (period_id, bfc_age)
    VALUES
        ('P1', '2023-01-28'),
        ('P2', '2023-02-15');
    """).result()

    # Clear and populate the target table in the main dataset
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc`").result()

    # Populate expected output table in test_dataset
    # This data should come from a legacy run with the exact same input.
    # Adjust bfc_procedure and bindefrist based on CURRENT_DATE() and UDF logic.
    # Assuming CURRENT_DATE() is '2023-05-01' for this example.
    # bfc_get_bindefrist adds 30 days.
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected`").result()
    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected` (
        cntrct_id, bindefrist, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id
    )
    VALUES
        ('C1', '2023-01-31', '2023-01-28', 1, CURRENT_DATE(), '2023-01-01', 'V1'),
        ('C2', '2023-03-03', '2023-02-15', 1, CURRENT_DATE(), '2023-02-01', 'V2'),
        ('C3', '2023-03-31', '2023-03-10', 1, CURRENT_DATE(), '2023-03-01', 'V1'),
        ('C4', '2023-05-01', '2023-04-01', 1, CURRENT_DATE(), '2023-04-01', NULL);
    """).result()

    # Clear log and control tables
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control_table`").result()


def test_core_transformation_parity():
    job_kennung = "PARITY_TEST_JOB"
    eintrags_nr = "PARITY_ENTRY_001"
    
    setup_parity_test_data()

    # Action: Call the stored procedure
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_ID}`('{job_kennung}', '{eintrags_nr}');
    """
    job = client.query(query)
    job.result() # Should complete successfully

    # Pass/Fail Criteria: Compare target table with expected
    compare_query = f"""
    SELECT
        (SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc`) = (SELECT COUNT(1) FROM `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected`) AS row_count_match,
        (SELECT COUNT(1) FROM (
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc` EXCEPT DISTINCT SELECT * FROM `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected`
        )) = 0 AS content_match
    """
    compare_results = client.query(compare_query).result()
    compare_row = next(iter(compare_results))

    assert compare_row.row_count_match, "Row counts do not match between actual and expected ta_c_bfc"
    assert compare_row.content_match, "Content of ta_c_bfc does not match expected output"

    # Verify run log entries
    run_log_query = f"""
    SELECT event_type FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
    ORDER BY event_timestamp;
    """
    run_log_results = client.query(run_log_query).result()
    assert run_log_results.total_rows == 2
    log_entries = list(run_log_results)
    assert log_entries[0].event_type == 'START'
    assert log_entries[1].event_type == 'END'

    # Verify job control table status
    job_control_query = f"""
    SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';
    """
    job_control_results = client.query(job_control_query).result()
    assert next(iter(job_control_results)).status == 'COMPLETED'

```

---

## 5. Test: Transformation Correctness - NULL Handling and Edge Cases

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles NULL values, empty source tables, and specific conditions in the `GREATEST` and `MERGE` statements.
*   **Setup:**
    1.  Populate source tables in `your_project.test_dataset` with specific NULLs, missing joins, and data that triggers different `MERGE` conditions.
        *   One `cntrct_id` with all `bfc_age` as NULL.
        *   One `cntrct_id` with no matching joins in `sof_ta_barrier` or `sof_ta_cntrct_valid`.
        *   Data in `ta_c_bfc` that will be updated by `MERGE` (e.g., `bfc_age` is less than new `S.bfc_age`).
        *   Data in `ta_c_bfc` that will *not* be updated by `MERGE` (e.g., `bfc_age` is already greater or equal).
        *   Data in `ta_c_bfc` that will be updated by the final `UPDATE` statement (e.g., `bfc_procedure` is older than `CURRENT_DATE()`).
    2.  Define the expected output in `your_project.test_dataset.ta_c_bfc_expected` for these specific scenarios.
    3.  Ensure `your_project.your_dataset.ta_c_bfc` is empty or pre-populated as needed for `MERGE` tests.
*   **Action:** Execute the BigQuery Stored Procedure `r_ausd_ta_c_bfc` with valid parameters.
*   **Pass/Fail Criteria:**
    *   The procedure completes successfully.
    *   The content of `your_project.your_dataset.ta_c_bfc` is identical to `your_project.test_dataset.ta_c_bfc_expected`.
    *   `job_run_log` and `job_control_table` reflect a successful run.

*   **Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "your_project"
DATASET_ID = "your_dataset"
PROCEDURE_ID = "r_ausd_ta_c_bfc"
TEST_DATASET_ID = "test_dataset"

client = bigquery.Client(project=PROJECT_ID)

def setup_null_and_edge_case_data():
    # Clear and populate source tables in test_dataset
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_barrier`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_valid`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_period`").result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age)
    VALUES
        ('C_NULL_AGE', '2023-01-01', 'V_NULL', NULL), -- All bfc_age NULL
        ('C_NO_JOIN', '2023-02-01', 'V_NO_JOIN', '2023-02-05'), -- No matching joins
        ('C_MERGE_UPDATE', '2023-03-01', 'V_MERGE', '2023-03-05'), -- Will update existing row
        ('C_MERGE_INSERT', '2023-04-01', 'V_MERGE', '2023-04-05'); -- Will insert new row
    """).result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_barrier` (cntrct_id, bfc_age)
    VALUES
        ('C_MERGE_UPDATE', '2023-03-10');
    """).result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_valid` (cntrct_validity_id, first_period_id, bfc_age)
    VALUES
        ('V_MERGE', 'P_MERGE', '2023-03-15');
    """).result()

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_period` (period_id, bfc_age)
    VALUES
        ('P_MERGE', '2023-03-20');
    """).result()

    # Clear and pre-populate the target table for MERGE and UPDATE tests
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc`").result()
    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc` (
        cntrct_id, bindefrist, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id
    )
    VALUES
        ('C_MERGE_UPDATE', '2023-03-20', '2023-03-01', 0, '2023-04-01', '2023-03-01', 'V_MERGE'), -- Will be updated by MERGE
        ('C_UPDATE_OLD_PROC', '2023-01-01', '2023-01-01', 1, '2023-01-01', '2023-01-01', 'V_OLD'); -- Will be updated by final UPDATE
    """).result()

    # Populate expected output table in test_dataset
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected`").result()
    # Assuming CURRENT_DATE() is '2023-05-01' for this example.
    # bfc_get_bindefrist adds 30 days.
    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected` (
        cntrct_id, bindefrist, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id
    )
    VALUES
        ('C_NULL_AGE', '2023-01-31', '1900-01-01', 1, CURRENT_DATE(), '2023-01-01', 'V_NULL'),
        ('C_NO_JOIN', '2023-03-03', '2023-02-05', 1, CURRENT_DATE(), '2023-02-01', 'V_NO_JOIN'),
        ('C_MERGE_UPDATE', '2023-04-20', '2023-03-20', 1, CURRENT_DATE(), '2023-03-01', 'V_MERGE'), -- Updated by MERGE
        ('C_MERGE_INSERT', '2023-05-01', '2023-04-05', 1, CURRENT_DATE(), '2023-04-01', 'V_MERGE'), -- Inserted by MERGE
        ('C_UPDATE_OLD_PROC', '2023-01-31', '2023-01-01', 1, CURRENT_DATE(), '2023-01-01', 'V_OLD'); -- Updated by final UPDATE
    """).result()

    # Clear log and control tables
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control_table`").result()


def test_transformation_null_and_edge_cases():
    job_kennung = "EDGE_CASE_TEST_JOB"
    eintrags_nr = "EDGE_CASE_ENTRY_001"
    
    setup_null_and_edge_case_data()

    # Action: Call the stored procedure
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_ID}`('{job_kennung}', '{eintrags_nr}');
    """
    job = client.query(query)
    job.result() # Should complete successfully

    # Pass/Fail Criteria: Compare target table with expected
    compare_query = f"""
    SELECT
        (SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc`) = (SELECT COUNT(1) FROM `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected`) AS row_count_match,
        (SELECT COUNT(1) FROM (
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc` EXCEPT DISTINCT SELECT * FROM `{PROJECT_ID}.{TEST_DATASET_ID}.ta_c_bfc_expected`
        )) = 0 AS content_match
    """
    compare_results = client.query(compare_query).result()
    compare_row = next(iter(compare_results))

    assert compare_row.row_count_match, "Row counts do not match for edge cases"
    assert compare_row.content_match, "Content of ta_c_bfc does not match expected for edge cases"

    # Verify run log entries and job control status
    # (Similar to Test 4, omitted for brevity but should be included)
```

---

## 6. Test: Record Count Capture

*   **Purpose:** Verify that the `v_records_processed` variable in the BigQuery Stored Procedure correctly captures the number of rows affected by the *final* DML statement (the `UPDATE` for outdated procedures) and logs it to `job_run_log`. This is a specific behavioral check as the legacy script's record count source is less clear.
*   **Setup:**
    1.  Populate `your_project.your_dataset.ta_c_bfc` with a known number of rows that will be updated by the final `UPDATE` statement (i.e., `bfc_procedure < CURRENT_DATE()`).
    2.  Ensure source tables are populated to allow the `MERGE` statement to run without errors, but ideally without affecting the rows that the final `UPDATE` will target, or ensure the `MERGE` also results in a predictable number of updates/inserts.
    3.  Ensure `job_run_log` is empty.
*   **Action:** Execute the BigQuery Stored Procedure `r_ausd_ta_c_bfc` with valid parameters.
*   **Pass/Fail Criteria:**
    *   The procedure completes successfully.
    *   The `record_count` in the `END` entry of `your_project.your_dataset.job_run_log` matches the number of rows updated by the final `UPDATE` statement in the procedure.

*   **Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery
import datetime

PROJECT_ID = "your_project"
DATASET_ID = "your_dataset"
PROCEDURE_ID = "r_ausd_ta_c_bfc"
TEST_DATASET_ID = "test_dataset"

client = bigquery.Client(project=PROJECT_ID)

def setup_record_count_data(num_rows_to_update):
    # Ensure source tables have minimal data to allow procedure to run
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs`").result()
    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age)
    VALUES ('C_DUMMY', '2023-01-01', 'V_DUMMY', '2023-01-01');
    """).result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_barrier`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_valid`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_period`").result()

    # Clear and pre-populate target table with rows that will be updated by the final UPDATE
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc`").result()
    insert_values = []
    for i in range(num_rows_to_update):
        # Ensure bfc_procedure is older than CURRENT_DATE()
        insert_values.append(f"('C_OLD_{i}', '2023-01-01', '2023-01-01', 1, '2023-01-01', '2023-01-01', 'V_OLD_{i}')")
    
    # Add one row that will NOT be updated by the final UPDATE
    insert_values.append(f"('C_RECENT', '2023-01-01', '2023-01-01', 1, CURRENT_DATE(), '2023-01-01', 'V_RECENT')")

    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc` (
        cntrct_id, bindefrist, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id
    )
    VALUES {','.join(insert_values)};
    """).result()

    # Clear log and control tables
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control_table`").result()


def test_record_count_capture():
    job_kennung = "RECORD_COUNT_TEST"
    eintrags_nr = "RC_ENTRY_001"
    expected_updated_rows = 5 # Number of rows we expect the final UPDATE to affect

    setup_record_count_data(expected_updated_rows)

    # Action: Call the stored procedure
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_ID}`('{job_kennung}', '{eintrags_nr}');
    """
    job = client.query(query)
    job.result() # Should complete successfully

    # Pass/Fail Criteria: Verify record_count in job_run_log
    run_log_query = f"""
    SELECT record_count
    FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}' AND event_type = 'END';
    """
    run_log_results = client.query(run_log_query).result()
    assert run_log_results.total_rows == 1
    log_row = next(iter(run_log_results))
    assert log_row.record_count == expected_updated_rows

```

---

## 7. Test: Error Handling - General SQL Error

*   **Purpose:** Verify that the BigQuery Stored Procedure's `EXCEPTION WHEN ERROR` block correctly catches general SQL errors during data processing, logs them to `job_error_log`, updates `job_control_table` to `FAILED`, and re-raises the error.
*   **Setup:**
    1.  Create a scenario that will cause a SQL error during the procedure's execution (e.g., attempt to insert a NULL into a `NOT NULL` column, or a data type mismatch if possible). For this test, we'll simulate an error by dropping a required source table *after* the procedure starts but *before* the DML. This requires a slight modification to the test setup to introduce a race condition or a separate step. A simpler way is to modify the UDF temporarily to always throw an error.
    2.  Ensure `job_error_log`, `job_run_log`, `job_control_table` are empty.
*   **Action:** Execute the BigQuery Stored Procedure `r_ausd_ta_c_bfc` with valid parameters, but with a setup that guarantees a SQL error.
*   **Pass/Fail Criteria:**
    *   The procedure execution fails with a BigQuery error.
    *   An entry is recorded in `your_project.your_dataset.job_error_log` with `error_code` 'SQL_ERROR' (or specific BigQuery error code) and a descriptive `error_message`.
    *   A `START` entry is present in `job_run_log`, but no `END` entry.
    *   The `job_control_table` entry for this run has `status = 'FAILED'`.

*   **Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery
import time

PROJECT_ID = "your_project"
DATASET_ID = "your_dataset"
PROCEDURE_ID = "r_ausd_ta_c_bfc"
TEST_DATASET_ID = "test_dataset"

client = bigquery.Client(project=PROJECT_ID)

def setup_error_scenario():
    # Ensure source tables exist and have minimal data for START log,
    # but then create a condition for error.
    # For simplicity, let's make the UDF always return an error for this test.
    # In a real scenario, you might drop a table or insert invalid data.

    # Temporarily replace the UDF with one that always errors
    client.query(f"""
    CREATE OR REPLACE FUNCTION `{PROJECT_ID}.{DATASET_ID}.bfc_get_bindefrist`(
        i_cntrct_id STRING, i_commitment_reference_date DATE, i_cntrct_validity_id STRING
    )
    RETURNS DATE
    LANGUAGE SQL
    AS (
        ERROR('Simulated UDF error for testing purposes.')
    );
    """).result()

    # Ensure source tables have data so the DML attempts to use the UDF
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs`").result()
    client.query(f"""
    INSERT INTO `{PROJECT_ID}.{TEST_DATASET_ID}.sof_ta_cntrct_crs` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age)
    VALUES ('C_ERROR', '2023-01-01', 'V_ERROR', '2023-01-01');
    """).result()

    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_c_bfc`").result() # Ensure target table is clear

    # Clear log and control tables
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control_table`").result()


def teardown_error_scenario():
    # Restore the original UDF after the test
    client.query(f"""
    CREATE OR REPLACE FUNCTION `{PROJECT_ID}.{DATASET_ID}.bfc_get_bindefrist`(
        i_cntrct_id STRING, i_commitment_reference_date DATE, i_cntrct_validity_id STRING
    )
    RETURNS DATE
    LANGUAGE SQL
    AS (
        CASE
            WHEN i_commitment_reference_date IS NULL THEN NULL
            ELSE DATE_ADD(i_commitment_reference_date, INTERVAL 30 DAY)
        END
    );
    """).result()


def test_general_sql_error_handling():
    job_kennung = "SQL_ERROR_TEST"
    eintrags_nr = "SQL_ERROR_ENTRY_001"
    
    setup_error_scenario()

    # Action: Call the stored procedure
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_ID}`('{job_kennung}', '{eintrags_nr}');
    """
    
    job = client.query(query)
    
    # Pass/Fail Criteria: Procedure should fail
    with pytest.raises(Exception) as excinfo:
        job.result()
    
    assert "Simulated UDF error for testing purposes." in str(excinfo.value)

    # Verify error log entry
    error_log_query = f"""
    SELECT error_code, error_message, job_kenn_ung, eintrags_nr
    FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
    WHERE job_kenn_ung = '{job_kennung}'
    ORDER BY error_timestamp DESC
    LIMIT 1;
    """
    error_log_results = client.query(error_log_query).result()
    assert error_log_results.total_rows == 1
    error_row = next(iter(error_log_results))
    assert error_row.error_code == 'SQL_ERROR' # Default error code in EXCEPTION block
    assert "Simulated UDF error for testing purposes." in error_row.error_message
    assert error_row.job_kenn_ung == job_kennung
    assert error_row.eintrags_nr == eintrags_nr

    # Verify run log entries (only START, no END)
    run_log_query = f"""
    SELECT event_type FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';
    """
    run_log_results = client.query(run_log_query).result()
    assert run_log_results.total_rows == 1
    assert next(iter(run_log_results)).event_type == 'START'

    # Verify job control table status
    job_control_query = f"""
    SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
    WHERE job_kenn_ung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';
    """
    job_control_results = client.query(job_control_query).result()
    assert next(iter(job_control_results)).status == 'FAILED'

    teardown_error_scenario()

```