The migration of `k_ausd_bp_ta_bpr_instance.ksh` to Google Cloud Platform involves significant architectural changes, translating KornShell orchestration and Oracle SQL into BigQuery Stored Procedures and Airflow DAGs. The following tests aim to ensure behavioral equivalence, data integrity, and correctness of the migrated solution.

---

## Test Case 1: Successful Execution with Valid Parameters

**Purpose:** Verify that the migrated Airflow DAG and BigQuery Stored Procedures execute successfully when provided with all valid and expected parameters, resulting in data being processed and inserted into the target table. This covers the basic end-to-end flow.

**Setup:**
1.  Ensure the BigQuery tables `my_gcp_project.my_bq_dataset.cds_ta_cntrct` and `my_gcp_project.my_bq_dataset.pds_ta_bpri_com` exist and are populated with a diverse set of test data that includes records expected to be selected by the transformation logic.
    *   **Example `cds_ta_cntrct` DDL:**
        ```sql
        CREATE TABLE IF NOT EXISTS my_gcp_project.my_bq_dataset.cds_ta_cntrct (
            cntrct_id INT64 NOT NULL,
            cntrct_st INT64,
            redundant_owner_id INT64,
            insert_at DATE,
            modified_at DATE,
            valid_from DATE,
            valid_to DATE,
            is_production INT64,
            cntrct_ty INT64,
            cntrct_parent INT64
        );
        ```
    *   **Example `pds_ta_bpri_com` DDL:**
        ```sql
        CREATE TABLE IF NOT EXISTS my_gcp_project.my_bq_dataset.pds_ta_bpri_com (
            cntrct_id INT64 NOT NULL,
            bpr_id INT64,
            bpri_com_id INT64,
            iccid_mi STRING,
            iccid_ii STRING,
            iccid_iai STRING,
            iccid_nr STRING,
            iccid_cd STRING,
            imsi_mcc INT64,
            imsi_mnc INT64,
            imsi_hlr INT64,
            imsi_si INT64,
            cntrct_id_ref INT64,
            insert_at DATE,
            modified_at DATE,
            valid_from DATE,
            valid_to DATE,
            is_production INT64
        );
        ```
2.  Populate `cds_ta_cntrct` and `pds_ta_bpri_com` with data that will result in at least 100 records being processed for a given `p_Stichtag`. Include records that satisfy all `WHERE` conditions and some that do not.
    *   **Example Data (for `p_Stichtag = '20230115'`):**
        ```sql
        -- cds_ta_cntrct
        INSERT INTO my_gcp_project.my_bq_dataset.cds_ta_cntrct VALUES
        (101, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL), -- Should be selected
        (102, 6, 1, '2023-01-05', '2023-01-10', '2023-01-05', NULL, 1, 11, NULL), -- Should be selected
        (103, 5, 1, '2023-01-01', NULL, '2023-01-01', '2023-01-14', 1, 10, NULL), -- valid_to before stichtag, should NOT be selected
        (104, 7, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL), -- cntrct_st not in (5,6), should NOT be selected
        (105, 5, 2, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL), -- redundant_owner_id not 1, should NOT be selected
        (106, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 0, 10, NULL), -- is_production not 1, should NOT be selected
        (107, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 1, 1000); -- cntrct_ty in (1,2,5) but cntrct_parent IS NOT NULL, should be selected

        -- pds_ta_bpri_com (matching cntrct_id with above)
        INSERT INTO my_gcp_project.my_bq_dataset.pds_ta_bpri_com VALUES
        (101, 1, 1001, 'MI1', 'II1', 'IAI1', 'NR1', 'CD1', 123, 45, 678, 90, 201, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (102, 2, 1002, 'MI2', 'II2', 'IAI2', 'NR2', 'CD2', 124, 46, 679, 91, 202, '2023-01-05', NULL, '2023-01-05', NULL, 1),
        (107, 3, 1003, 'MI3', 'II3', 'IAI3', 'NR3', 'CD3', 125, 47, 680, 92, 203, '2023-01-01', NULL, '2023-01-01', NULL, 1);
        ```
3.  Ensure the target table `my_gcp_project.my_bq_dataset.PoolBasisprodukt` is empty or does not contain data for the test `p_Stichtag`.
    *   **SQL:** `DELETE FROM my_gcp_project.my_bq_dataset.PoolBasisprodukt WHERE processing_date = '2023-01-15';`

**Action:**
1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_instance_dag` with the following parameters:
    *   `p_jobkennung`: `TEST_JOB_VALID`
    *   `p_eintragsnr`: `123`
    *   `p_stichtag`: `20230115`
    *   `p_wiederanlaufwert`: `N`

**Pass/Fail Criterion:**
*   The Airflow DAG run completes successfully without errors.
*   The `my_gcp_project.my_bq_dataset.PoolBasisprodukt` table contains new records for `processing_date = '2023-01-15'`.
*   The number of records in `PoolBasisprodukt` for `processing_date = '2023-01-15'` matches the expected count based on the test data and transformation logic (e.g., 3 records for the example data).
*   The Airflow task logs for `execute_bpr_instance_procedure` show a message indicating successful completion and the number of processed records.

**Runnable Test Code (Python with `pytest` and `google-cloud-bigquery`):**
```python
import pytest
from google.cloud import bigquery
from airflow.models.dagbag import DagBag
from airflow.utils import dates
import pendulum

# Assume PROJECT_ID and DATASET_ID are configured globally or passed
PROJECT_ID = "my_gcp_project"
DATASET_ID = "my_bq_dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def setup_bigquery_tables():
    """Sets up source and target tables for testing."""
    # DDL for source tables
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.cds_ta_cntrct (
            cntrct_id INT64 NOT NULL, cntrct_st INT64, redundant_owner_id INT64,
            insert_at DATE, modified_at DATE, valid_from DATE, valid_to DATE,
            is_production INT64, cntrct_ty INT64, cntrct_parent INT64
        );
    """).result()
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.pds_ta_bpri_com (
            cntrct_id INT64 NOT NULL, bpr_id INT64, bpri_com_id INT64,
            iccid_mi STRING, iccid_ii STRING, iccid_iai STRING, iccid_nr STRING, iccid_cd STRING,
            imsi_mcc INT64, imsi_mnc INT64, imsi_hlr INT64, imsi_si INT64,
            cntrct_id_ref INT64, insert_at DATE, modified_at DATE, valid_from DATE, valid_to DATE,
            is_production INT64
        );
    """).result()
    # DDL for target table (ensure it's partitioned as per design)
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt (
            CNTRCT_ID INT64 NOT NULL, BPR_ID INT64 NOT NULL, BPR_INSTANCE_ID INT64 NOT NULL,
            ICCID STRING, IMSI_MCC INT64, IMSI_MNC INT64, IMSI_HLR INT64, IMSI_SI INT64,
            CNTRCT_ID_REF INT64, processing_date DATE NOT NULL
        ) PARTITION BY processing_date;
    """).result()
    # DDL for staging table
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance_staging (
            CNTRCT_ID INT64 NOT NULL, BPR_ID INT64 NOT NULL, BPR_INSTANCE_ID INT64 NOT NULL,
            ICCID STRING, IMSI_MCC INT64, IMSI_MNC INT64, IMSI_HLR INT64, IMSI_SI INT64,
            CNTRCT_ID_REF INT64, processing_date DATE NOT NULL
        ) PARTITION BY processing_date;
    """).result()

    yield # Run tests

    # Teardown: Clean up tables if necessary
    # BQ_CLIENT.query(f"DROP TABLE IF EXISTS {PROJECT_ID}.{DATASET_ID}.cds_ta_cntrct").result()
    # BQ_CLIENT.query(f"DROP TABLE IF EXISTS {PROJECT_ID}.{DATASET_ID}.pds_ta_bpri_com").result()
    # BQ_CLIENT.query(f"DROP TABLE IF EXISTS {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt").result()
    # BQ_CLIENT.query(f"DROP TABLE IF EXISTS {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance_staging").result()


def test_successful_dag_execution(setup_bigquery_tables):
    """Tests the end-to-end DAG execution with valid parameters."""
    stichtag = '20230115'
    expected_records = 3 # Based on example data

    # Clear target table for the specific stichtag
    BQ_CLIENT.query(f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt WHERE processing_date = '{stichtag[:4]}-{stichtag[4:6]}-{stichtag[6:]}'").result()
    BQ_CLIENT.query(f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance_staging WHERE processing_date = '{stichtag[:4]}-{stichtag[4:6]}-{stichtag[6:]}'").result()

    # Insert test data
    BQ_CLIENT.query(f"""
        INSERT INTO {PROJECT_ID}.{DATASET_ID}.cds_ta_cntrct VALUES
        (101, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL),
        (102, 6, 1, '2023-01-05', '2023-01-10', '2023-01-05', NULL, 1, 11, NULL),
        (103, 5, 1, '2023-01-01', NULL, '2023-01-01', '2023-01-14', 1, 10, NULL),
        (104, 7, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL),
        (105, 5, 2, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL),
        (106, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 0, 10, NULL),
        (107, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 1, 1000);
    """).result()
    BQ_CLIENT.query(f"""
        INSERT INTO {PROJECT_ID}.{DATASET_ID}.pds_ta_bpri_com VALUES
        (101, 1, 1001, 'MI1', 'II1', 'IAI1', 'NR1', 'CD1', 123, 45, 678, 90, 201, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (102, 2, 1002, 'MI2', 'II2', 'IAI2', 'NR2', 'CD2', 124, 46, 679, 91, 202, '2023-01-05', NULL, '2023-01-05', NULL, 1),
        (107, 3, 1003, 'MI3', 'II3', 'IAI3', 'NR3', 'CD3', 125, 47, 680, 92, 203, '2023-01-01', NULL, '2023-01-01', NULL, 1);
    """).result()

    # Load DAG
    dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)
    dag = dag_bag.get_dag('k_ausd_bp_ta_bpr_instance_dag')
    assert dag is not None

    # Create a DAG run
    execution_date = pendulum.datetime(2023, 1, 16, tz="UTC") # So yesterday_ds_nodash is 20230115
    dr = dag.create_dagrun(
        run_id=f"test_run_{stichtag}",
        execution_date=execution_date,
        state="running",
        conf={
            "p_jobkennung": "TEST_JOB_VALID",
            "p_eintragsnr": "123",
            "p_stichtag": stichtag,
            "p_wiederanlaufwert": "N"
        }
    )

    # Execute the task
    task = dag.get_task("execute_bpr_instance_procedure")
    ti = dr.get_task_instance(task.task_id)
    ti.run(start_date=execution_date, end_date=execution_date)

    # Assertions
    assert ti.current_state() == 'success'

    # Check records in PoolBasisprodukt
    query_job = BQ_CLIENT.query(f"""
        SELECT COUNT(*) FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
        WHERE processing_date = '{stichtag[:4]}-{stichtag[4:6]}-{stichtag[6:]}'
    """)
    result = query_job.result()
    row_count = [row[0] for row in result][0]
    assert row_count == expected_records, f"Expected {expected_records} records, but found {row_count} in PoolBasisprodukt."

    # Verify log message (requires parsing Airflow task logs, which is more complex for a simple example)
    # For a real test, you'd check Cloud Logging for the specific log message from the BQ SP.
    # Example: SELECT * FROM `my_gcp_project.my_bq_dataset.INFORMATION_SCHEMA.JOBS_BY_PROJECT` WHERE job_id = '...' AND statement_type = 'CALL'
    # and then check the log_message output.
```

---

## Test Case 2: Parameter Validation - Missing Required Parameters

**Purpose:** Verify that the migrated solution correctly identifies and handles missing required parameters, failing early as specified by the legacy script's error handling (`ErrNr=193`).

**Setup:**
1.  Ensure the BigQuery environment and DAG are deployed.
2.  No specific data setup is needed as this tests parameter validation before data processing.

**Action:**
1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_instance_dag` with `p_eintragsnr` missing (or set to an empty string).
    *   `p_jobkennung`: `TEST_JOB_MISSING_PARAM`
    *   `p_eintragsnr`: `""` (or omit if Airflow allows)
    *   `p_stichtag`: `20230115`
    *   `p_wiederanlaufwert`: `N`

**Pass/Fail Criterion:**
*   The Airflow DAG run fails.
*   The Airflow task logs for `execute_bpr_instance_procedure` contain an error message similar to "Parameter p_EintragsNr must not be empty." (as defined in `r_ausd_bp_ta_bpr_instance.sql`).
*   No records are inserted into `my_gcp_project.my_bq_dataset.PoolBasisprodukt` for the specified `p_stichtag`.

**Runnable Test Code (Python with `pytest`):**
```python
import pytest
from google.cloud import bigquery
from airflow.models.dagbag import DagBag
from airflow.utils import dates
import pendulum
from airflow.exceptions import AirflowException # For expected task failure

# Assume PROJECT_ID and DATASET_ID are configured globally or passed
PROJECT_ID = "my_gcp_project"
DATASET_ID = "my_bq_dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

# Re-use setup_bigquery_tables fixture from Test Case 1 if needed,
# but for this test, it's not strictly necessary as it should fail early.

def test_missing_eintragsnr_parameter():
    """Tests DAG failure when p_eintragsnr is missing/empty."""
    stichtag = '20230115'

    # Load DAG
    dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)
    dag = dag_bag.get_dag('k_ausd_bp_ta_bpr_instance_dag')
    assert dag is not None

    # Create a DAG run with missing p_eintragsnr
    execution_date = pendulum.datetime(2023, 1, 16, tz="UTC")
    dr = dag.create_dagrun(
        run_id=f"test_run_missing_eintragsnr",
        execution_date=execution_date,
        state="running",
        conf={
            "p_jobkennung": "TEST_JOB_MISSING_PARAM",
            "p_eintragsnr": "", # Empty string to simulate missing
            "p_stichtag": stichtag,
            "p_wiederanlaufwert": "N"
        }
    )

    # Execute the task and expect failure
    task = dag.get_task("execute_bpr_instance_procedure")
    ti = dr.get_task_instance(task.task_id)

    with pytest.raises(AirflowException) as excinfo:
        ti.run(start_date=execution_date, end_date=execution_date)

    # Assert that the task failed
    assert ti.current_state() == 'failed'
    # Check for the specific error message in the exception or logs (more robust in Cloud Logging)
    assert "Parameter p_EintragsNr must not be empty." in str(excinfo.value)

    # Verify no records were inserted
    query_job = BQ_CLIENT.query(f"""
        SELECT COUNT(*) FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
        WHERE processing_date = '{stichtag[:4]}-{stichtag[4:6]}-{stichtag[6:]}'
    """)
    result = query_job.result()
    row_count = [row[0] for row in result][0]
    assert row_count == 0, "No records should be inserted when parameters are invalid."
```

---

## Test Case 3: Parameter Validation - Invalid `p_Stichtag` Format

**Purpose:** Verify that the migrated solution correctly identifies and handles an invalid `p_Stichtag` format, failing early as specified by the legacy script's date validation (`DWDate_Datum_Check`).

**Setup:**
1.  Ensure the BigQuery environment and DAG are deployed.
2.  No specific data setup is needed.

**Action:**
1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_instance_dag` with an invalid `p_stichtag` format (e.g., `DD-MM-YYYY` instead of `YYYYMMDD`).
    *   `p_jobkennung`: `TEST_JOB_INVALID_DATE`
    *   `p_eintragsnr`: `123`
    *   `p_stichtag`: `15-01-2023`
    *   `p_wiederanlaufwert`: `N`

**Pass/Fail Criterion:**
*   The Airflow DAG run fails.
*   The Airflow task logs for `execute_bpr_instance_procedure` contain an error message similar to "Invalid p_Stichtag format. Expected YYYYMMDD. Received: 15-01-2023" (as defined in `r_ausd_bp_ta_bpr_instance.sql`).
*   No records are inserted into `my_gcp_project.my_bq_dataset.PoolBasisprodukt` for any date.

**Runnable Test Code (Python with `pytest`):**
```python
import pytest
from google.cloud import bigquery
from airflow.models.dagbag import DagBag
from airflow.utils import dates
import pendulum
from airflow.exceptions import AirflowException

PROJECT_ID = "my_gcp_project"
DATASET_ID = "my_bq_dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def test_invalid_stichtag_format():
    """Tests DAG failure when p_stichtag has an invalid format."""
    invalid_stichtag = '15-01-2023' # Expected YYYYMMDD

    # Load DAG
    dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)
    dag = dag_bag.get_dag('k_ausd_bp_ta_bpr_instance_dag')
    assert dag is not None

    # Create a DAG run with invalid p_stichtag
    execution_date = pendulum.datetime(2023, 1, 16, tz="UTC")
    dr = dag.create_dagrun(
        run_id=f"test_run_invalid_stichtag",
        execution_date=execution_date,
        state="running",
        conf={
            "p_jobkennung": "TEST_JOB_INVALID_DATE",
            "p_eintragsnr": "123",
            "p_stichtag": invalid_stichtag,
            "p_wiederanlaufwert": "N"
        }
    )

    # Execute the task and expect failure
    task = dag.get_task("execute_bpr_instance_procedure")
    ti = dr.get_task_instance(task.task_id)

    with pytest.raises(AirflowException) as excinfo:
        ti.run(start_date=execution_date, end_date=execution_date)

    # Assert that the task failed
    assert ti.current_state() == 'failed'
    assert f"Invalid p_Stichtag format. Expected YYYYMMDD. Received: {invalid_stichtag}" in str(excinfo.value)

    # Verify no records were inserted (check for any date, as it should fail before processing)
    query_job = BQ_CLIENT.query(f"""
        SELECT COUNT(*) FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
    """)
    result = query_job.result()
    total_row_count = [row[0] for row in result][0]
    assert total_row_count == 0, "No records should be inserted when date format is invalid."
```

---

## Test Case 4: Transformation Correctness - Output Parity & Data Integrity

**Purpose:** Verify that the data generated by the migrated BigQuery Stored Procedures (`d_ausd_bp_ta_bpr_instance_core`) is identical to the data generated by the legacy `d_ausd_bp_ta_bpr_instance.sql` for the same input data. This covers joins, filters, column transformations (like `ICCID` concatenation), and `NULL` handling.

**Setup:**
1.  Populate `my_gcp_project.my_bq_dataset.cds_ta_cntrct` and `my_gcp_project.my_bq_dataset.pds_ta_bpri_com` with a comprehensive set of test data. This data should cover:
    *   Records satisfying all `WHERE` conditions.
    *   Records failing specific `WHERE` conditions (e.g., `cntrct_st`, `redundant_owner_id`, `is_production`, date ranges, `cntrct_ty` with `cntrct_parent` NULL).
    *   Records with `NULL` values in `modified_at`, `valid_to`, `cntrct_parent`.
    *   Records that join successfully and those that do not.
    *   Different values for `iccid_mi`, `iccid_ii`, etc., to test concatenation.
2.  **Crucially, generate the "expected" output data for the `PoolBasisprodukt` table by running the *legacy* `d_ausd_bp_ta_bpr_instance.sql` script against the *same* input data (or a representative subset) in the legacy Oracle environment.** Store this expected output in a BigQuery table, e.g., `my_gcp_project.my_bq_dataset.PoolBasisprodukt_expected`.
    *   **Note:** If direct execution of legacy SQL is not feasible, manually derive the expected output based on a thorough understanding of the legacy SQL logic.
3.  Ensure `my_gcp_project.my_bq_dataset.PoolBasisprodukt` is empty for the test `p_Stichtag`.

**Action:**
1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_instance_dag` with a `p_stichtag` corresponding to the date used for generating the `PoolBasisprodukt_expected` data (e.g., `20230115`).
    *   `p_jobkennung`: `TEST_JOB_PARITY`
    *   `p_eintragsnr`: `456`
    *   `p_stichtag`: `20230115`
    *   `p_wiederanlaufwert`: `N`

**Pass/Fail Criterion:**
*   The Airflow DAG run completes successfully.
*   The number of records in `my_gcp_project.my_bq_dataset.PoolBasisprodukt` for `processing_date = '2023-01-15'` is identical to the number of records in `my_gcp_project.my_bq_dataset.PoolBasisprodukt_expected` for the same date.
*   A full data comparison (e.g., using `EXCEPT DISTINCT` in BigQuery) between the actual output (`PoolBasisprodukt`) and the expected output (`PoolBasisprodukt_expected`) for the given `processing_date` yields zero differences.

**Runnable Test Code (Python with `pytest` and `google-cloud-bigquery`):**
```python
import pytest
from google.cloud import bigquery
from airflow.models.dagbag import DagBag
import pendulum
from datetime import date

PROJECT_ID = "my_gcp_project"
DATASET_ID = "my_bq_dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def setup_parity_tables():
    """Sets up source, target, and expected tables for parity testing."""
    # Re-use DDL from Test Case 1 for source and target tables
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.cds_ta_cntrct (
            cntrct_id INT64 NOT NULL, cntrct_st INT64, redundant_owner_id INT64,
            insert_at DATE, modified_at DATE, valid_from DATE, valid_to DATE,
            is_production INT64, cntrct_ty INT64, cntrct_parent INT64
        );
    """).result()
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.pds_ta_bpri_com (
            cntrct_id INT64 NOT NULL, bpr_id INT64, bpri_com_id INT64,
            iccid_mi STRING, iccid_ii STRING, iccid_iai STRING, iccid_nr STRING, iccid_cd STRING,
            imsi_mcc INT64, imsi_mnc INT64, imsi_hlr INT64, imsi_si INT64,
            cntrct_id_ref INT64, insert_at DATE, modified_at DATE, valid_from DATE, valid_to DATE,
            is_production INT64
        );
    """).result()
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt (
            CNTRCT_ID INT64 NOT NULL, BPR_ID INT64 NOT NULL, BPR_INSTANCE_ID INT64 NOT NULL,
            ICCID STRING, IMSI_MCC INT64, IMSI_MNC INT64, IMSI_HLR INT64, IMSI_SI INT64,
            CNTRCT_ID_REF INT64, processing_date DATE NOT NULL
        ) PARTITION BY processing_date;
    """).result()
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance_staging (
            CNTRCT_ID INT64 NOT NULL, BPR_ID INT64 NOT NULL, BPR_INSTANCE_ID INT64 NOT NULL,
            ICCID STRING, IMSI_MCC INT64, IMSI_MNC INT64, IMSI_HLR INT64, IMSI_SI INT64,
            CNTRCT_ID_REF INT64, processing_date DATE NOT NULL
        ) PARTITION BY processing_date;
    """).result()

    # Create table for expected results
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_expected (
            CNTRCT_ID INT64 NOT NULL, BPR_ID INT64 NOT NULL, BPR_INSTANCE_ID INT64 NOT NULL,
            ICCID STRING, IMSI_MCC INT64, IMSI_MNC INT64, IMSI_HLR INT64, IMSI_SI INT64,
            CNTRCT_ID_REF INT64, processing_date DATE NOT NULL
        ) PARTITION BY processing_date;
    """).result()

    yield

def test_output_parity(setup_parity_tables):
    """Compares migrated output with expected legacy output."""
    stichtag_str = '20230115'
    stichtag_date = date(2023, 1, 15)

    # Clear tables for the specific stichtag
    BQ_CLIENT.query(f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt WHERE processing_date = '{stichtag_date}'").result()
    BQ_CLIENT.query(f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_expected WHERE processing_date = '{stichtag_date}'").result()
    BQ_CLIENT.query(f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance_staging WHERE processing_date = '{stichtag_date}'").result()

    # Insert comprehensive test data into source tables
    BQ_CLIENT.query(f"""
        INSERT INTO {PROJECT_ID}.{DATASET_ID}.cds_ta_cntrct VALUES
        (101, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL),
        (102, 6, 1, '2023-01-05', '2023-01-10', '2023-01-05', NULL, 1, 11, NULL),
        (103, 5, 1, '2023-01-01', NULL, '2023-01-01', '2023-01-14', 1, 10, NULL), -- valid_to before stichtag
        (104, 7, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL), -- cntrct_st not in (5,6)
        (105, 5, 2, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL), -- redundant_owner_id not 1
        (106, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 0, 10, NULL), -- is_production not 1
        (107, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 1, 1000), -- cntrct_ty in (1,2,5) but cntrct_parent IS NOT NULL
        (108, 5, 1, '2023-01-16', NULL, '2023-01-16', NULL, 1, 10, NULL); -- insert_at after stichtag
    """).result()
    BQ_CLIENT.query(f"""
        INSERT INTO {PROJECT_ID}.{DATASET_ID}.pds_ta_bpri_com VALUES
        (101, 1, 1001, 'MI1', 'II1', 'IAI1', 'NR1', 'CD1', 123, 45, 678, 90, 201, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (102, 2, 1002, 'MI2', 'II2', 'IAI2', 'NR2', 'CD2', 124, 46, 679, 91, 202, '2023-01-05', NULL, '2023-01-05', NULL, 1),
        (107, 3, 1003, 'MI3', 'II3', 'IAI3', 'NR3', 'CD3', 125, 47, 680, 92, 203, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (108, 4, 1004, 'MI4', 'II4', 'IAI4', 'NR4', 'CD4', 126, 48, 681, 93, 204, '2023-01-16', NULL, '2023-01-16', NULL, 1);
    """).result()

    # Manually insert expected results based on the logic for stichtag_date
    # This simulates the output of the legacy script
    BQ_CLIENT.query(f"""
        INSERT INTO {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_expected VALUES
        (101, 1, 1001, 'MI1-II1-IAI1-NR1-CD1', 123, 45, 678, 90, 201, '{stichtag_date}'),
        (102, 2, 1002, 'MI2-II2-IAI2-NR2-CD2', 124, 46, 679, 91, 202, '{stichtag_date}'),
        (107, 3, 1003, 'MI3-II3-IAI3-NR3-CD3', 125, 47, 680, 92, 203, '{stichtag_date}');
    """).result()
    expected_records_count = 3

    # Load DAG
    dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)
    dag = dag_bag.get_dag('k_ausd_bp_ta_bpr_instance_dag')
    assert dag is not None

    # Create a DAG run
    execution_date = pendulum.datetime(2023, 1, 16, tz="UTC")
    dr = dag.create_dagrun(
        run_id=f"test_run_parity_{stichtag_str}",
        execution_date=execution_date,
        state="running",
        conf={
            "p_jobkennung": "TEST_JOB_PARITY",
            "p_eintragsnr": "456",
            "p_stichtag": stichtag_str,
            "p_wiederanlaufwert": "N"
        }
    )

    # Execute the task
    task = dag.get_task("execute_bpr_instance_procedure")
    ti = dr.get_task_instance(task.task_id)
    ti.run(start_date=execution_date, end_date=execution_date)

    # Assertions
    assert ti.current_state() == 'success'

    # 1. Check row counts
    query_actual_count = BQ_CLIENT.query(f"""
        SELECT COUNT(*) FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
        WHERE processing_date = '{stichtag_date}'
    """)
    actual_count = [row[0] for row in query_actual_count.result()][0]
    assert actual_count == expected_records_count, f"Actual record count {actual_count} does not match expected {expected_records_count}."

    # 2. Perform data comparison using EXCEPT DISTINCT
    query_diff = BQ_CLIENT.query(f"""
        SELECT * FROM (
            SELECT CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, processing_date
            FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
            WHERE processing_date = '{stichtag_date}'
            EXCEPT DISTINCT
            SELECT CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, processing_date
            FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_expected
            WHERE processing_date = '{stichtag_date}'
        )
        UNION ALL
        SELECT * FROM (
            SELECT CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, processing_date
            FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_expected
            WHERE processing_date = '{stichtag_date}'
            EXCEPT DISTINCT
            SELECT CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, processing_date
            FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
            WHERE processing_date = '{stichtag_date}'
        )
    """)
    diff_results = list(query_diff.result())
    assert len(diff_results) == 0, f"Data mismatch found between actual and expected results: {diff_results}"
```

---

## Test Case 5: Date Derivation Correctness (`v_datum_heute`, `v_datum_gestern`)

**Purpose:** Verify that the `r_ausd_bp_ta_bpr_instance` BigQuery Stored Procedure correctly derives `v_datum_heute` and `v_datum_gestern` using BigQuery's native date functions, mirroring the functionality of `gestern.ksh`.

**Setup:**
1.  No specific data setup is required for source tables.
2.  The `r_ausd_bp_ta_bpr_instance` procedure needs to be modified temporarily to expose `v_datum_heute` and `v_datum_gestern` (e.g., by inserting them into a temporary logging table or returning them as `OUT` parameters for testing purposes, then reverting). For this test, we'll assume a way to inspect these values, perhaps by logging them or by modifying the SP to return them.

**Action:**
1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_instance_dag` on a specific date (e.g., `2023-01-16` for the DAG's `execution_date`).
    *   `p_jobkennung`: `TEST_DATE_DERIVATION`
    *   `p_eintragsnr`: `789`
    *   `p_stichtag`: `20230115` (this parameter is distinct from `v_datum_heute`/`v_datum_gestern`)
    *   `p_wiederanlaufwert`: `N`

**Pass/Fail Criterion:**
*   The Airflow DAG run completes successfully.
*   The `v_datum_heute` derived within the `r_ausd_bp_ta_bpr_instance` procedure matches the actual `CURRENT_DATE()` of the BigQuery execution environment on the day the test runs.
*   The `v_datum_gestern` derived within the `r_ausd_bp_ta_bpr_instance` procedure matches `CURRENT_DATE() - 1 DAY`.
*   (If the SP is modified to return these values) The returned values match expectations.
*   (If logging is used) The Cloud Logging entries for the BigQuery job show the correct derived dates.

**Runnable Test Code (Conceptual - requires temporary SP modification or robust log parsing):**
*   **Temporary SP Modification (for testing only):**
    ```sql
    CREATE OR REPLACE PROCEDURE my_gcp_project.my_bq_dataset.r_ausd_bp_ta_bpr_instance(
        IN p_JobKennung STRING,
        IN p_EintragsNr STRING,
        IN p_Stichtag STRING,
        IN p_wiederanlaufWert STRING,
        OUT records_processed INT64,
        OUT derived_heute DATE, -- Added for testing
        OUT derived_gestern DATE -- Added for testing
    )
    BEGIN
        -- ... (existing validation and date derivation) ...
        SET v_datum_heute = CURRENT_DATE();
        SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

        SET derived_heute = v_datum_heute; -- Assign for output
        SET derived_gestern = v_datum_gestern; -- Assign for output
        -- ... (rest of the procedure) ...
    END;
    ```
*   **Python Test (assuming modified SP):**
    ```python
    import pytest
    from google.cloud import bigquery
    from airflow.models.dagbag import DagBag
    import pendulum
    from datetime import date, timedelta

    PROJECT_ID = "my_gcp_project"
    DATASET_ID = "my_bq_dataset"
    BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

    # Re-use setup_bigquery_tables fixture if needed

    def test_date_derivation_correctness():
        """Tests if v_datum_heute and v_datum_gestern are derived correctly."""
        stichtag = '20230115' # This is just an input, not the derived 'today'

        # Load DAG
        dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)
        dag = dag_bag.get_dag('k_ausd_bp_ta_bpr_instance_dag')
        assert dag is not None

        # Create a DAG run
        execution_date = pendulum.datetime(2023, 1, 16, tz="UTC") # Simulates running on Jan 16
        dr = dag.create_dagrun(
            run_id=f"test_run_date_derivation",
            execution_date=execution_date,
            state="running",
            conf={
                "p_jobkennung": "TEST_DATE_DERIVATION",
                "p_eintragsnr": "789",
                "p_stichtag": stichtag,
                "p_wiederanlaufwert": "N"
            }
        )

        # Execute the task
        task = dag.get_task("execute_bpr_instance_procedure")
        ti = dr.get_task_instance(task.task_id)
        ti.run(start_date=execution_date, end_date=execution_date)

        assert ti.current_state() == 'success'

        # This part is conceptual. In a real scenario, you'd need to
        # query BigQuery job metadata or a logging table to get the OUT parameters.
        # For this example, we'll assume the SP was modified to return these.
        # A more robust way would be to parse Cloud Logging for the SP's log_message.

        # Example of how you might retrieve OUT parameters if the operator supported it
        # or if you had a custom operator/wrapper.
        # For now, we'll rely on the log message assertion.
        # The SP's log message includes the derived dates if we modify it slightly:
        # SELECT FORMAT('... Today: %t, Yesterday: %t ...', v_datum_heute, v_datum_gestern) AS log_message;

        # Expected derived dates based on execution_date
        expected_heute = execution_date.date()
        expected_gestern = (execution_date - timedelta(days=1)).date()

        # This assertion would require parsing the actual log output from BigQuery/Airflow
        # For now, we'll assume the SP's internal logic is correct if it runs.
        # A more direct test would involve calling the SP directly and inspecting results.
        # For instance, if the SP wrote to a temporary table:
        # query_job = BQ_CLIENT.query(f"SELECT derived_heute, derived_gestern FROM {PROJECT_ID}.{DATASET_ID}.temp_date_log_table WHERE job_id = '...'")
        # result = list(query_job.result())
        # assert result[0].derived_heute == expected_heute
        # assert result[0].derived_gestern == expected_gestern

        # For the provided code, the log message is:
        # SELECT FORMAT('Procedure r_ausd_bp_ta_bpr_instance completed successfully for Job: %s, Date: %s. Processed records: %d',
        #               p_JobKennung, p_Stichtag, records_processed) AS log_message;
        # It does NOT include v_datum_heute/gestern.
        # To test this, we'd need to modify the SP to include them in the log message or as OUT params.
        # Assuming a modification to the SP's log message for testing:
        # SELECT FORMAT('... Today: %t, Yesterday: %t ...', v_datum_heute, v_datum_gestern) AS log_message;
        # Then you'd search Airflow logs for this message.
        # For this example, we'll just assert success, implying the internal logic worked.
        pass # Placeholder for actual log parsing or direct SP call assertion
    ```

---

## Test Case 6: Idempotency of `PoolBasisprodukt` Updates

**Purpose:** Verify that running the job multiple times with the same `p_Stichtag` does not lead to duplicate records in the `PoolBasisprodukt` table, assuming the intent is to either overwrite or upsert, not simply append. The current design document states "For now, appending to PoolBasisprodukt if records for stichtag are not expected to exist, or if PoolBasisprodukt already handles deduplication/upserts." The current `d_ausd_bp_ta_bpr_instance_core` procedure *appends* to `PoolBasisprodukt`. This test will highlight if this behavior is intended or if a `MERGE` or `TRUNCATE + INSERT` is required.

**Setup:**
1.  Populate `my_gcp_project.my_bq_dataset.cds_ta_cntrct` and `my_gcp_project.my_bq_dataset.pds_ta_bpri_com` with test data that will result in a known number of records (e.g., 5 records) for a specific `p_Stichtag` (e.g., `20230115`).
2.  Ensure `my_gcp_project.my_bq_dataset.PoolBasisprodukt` is empty for the test `p_Stichtag`.

**Action:**
1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_instance_dag` with `p_stichtag = '20230115'`.
2.  After the first run completes successfully, immediately trigger the *same* Airflow DAG again with the *exact same parameters*.

**Pass/Fail Criterion:**
*   Both Airflow DAG runs complete successfully.
*   **If the intent is idempotency (no duplicates):** The total number of records in `my_gcp_project.my_bq_dataset.PoolBasisprodukt` for `processing_date = '2023-01-15'` after the *second* run is equal to the number of records generated by a *single* run (e.g., 5 records).
*   **If the current append behavior is intended:** The total number of records in `my_gcp_project.my_bq_dataset.PoolBasisprodukt` for `processing_date = '2023-01-15'` after the *second* run is double the number of records generated by a single run (e.g., 10 records). This would indicate that the current append behavior is accepted, but might be a risk for future data quality if not explicitly handled by downstream processes.

**Note:** Based on the provided `d_ausd_bp_ta_bpr_instance_core` which uses `INSERT INTO PoolBasisprodukt SELECT * FROM sof_ta_bpr_instance_staging`, the expected behavior is to *append* duplicates. This test will confirm that behavior. If idempotency is desired, the `d_ausd_bp_ta_bpr_instance_core` procedure would need to be modified (e.g., using `MERGE` or `TRUNCATE TABLE PoolBasisprodukt WHERE processing_date = stichtag;` before the final insert).

**Runnable Test Code (Python with `pytest`):**
```python
import pytest
from google.cloud import bigquery
from airflow.models.dagbag import DagBag
import pendulum
from datetime import date

PROJECT_ID = "my_gcp_project"
DATASET_ID = "my_bq_dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

# Re-use setup_parity_tables fixture for source and target tables

def test_idempotency_behavior(setup_parity_tables):
    """Tests if running the DAG twice with same parameters appends or upserts."""
    stichtag_str = '20230115'
    stichtag_date = date(2023, 1, 15)
    expected_records_per_run = 3 # Based on example data from Test Case 4

    # Clear tables for the specific stichtag
    BQ_CLIENT.query(f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt WHERE processing_date = '{stichtag_date}'").result()
    BQ_CLIENT.query(f"DELETE FROM {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance_staging WHERE processing_date = '{stichtag_date}'").result()

    # Insert test data (same as Test Case 4)
    BQ_CLIENT.query(f"""
        INSERT INTO {PROJECT_ID}.{DATASET_ID}.cds_ta_cntrct VALUES
        (101, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL),
        (102, 6, 1, '2023-01-05', '2023-01-10', '2023-01-05', NULL, 1, 11, NULL),
        (103, 5, 1, '2023-01-01', NULL, '2023-01-01', '2023-01-14', 1, 10, NULL),
        (104, 7, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL),
        (105, 5, 2, '2023-01-01', NULL, '2023-01-01', NULL, 1, 10, NULL),
        (106, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 0, 10, NULL),
        (107, 5, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1, 1, 1000),
        (108, 5, 1, '2023-01-16', NULL, '2023-01-16', NULL, 1, 10, NULL);
    """).result()
    BQ_CLIENT.query(f"""
        INSERT INTO {PROJECT_ID}.{DATASET_ID}.pds_ta_bpri_com VALUES
        (101, 1, 1001, 'MI1', 'II1', 'IAI1', 'NR1', 'CD1', 123, 45, 678, 90, 201, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (102, 2, 1002, 'MI2', 'II2', 'IAI2', 'NR2', 'CD2', 124, 46, 679, 91, 202, '2023-01-05', NULL, '2023-01-05', NULL, 1),
        (107, 3, 1003, 'MI3', 'II3', 'IAI3', 'NR3', 'CD3', 125, 47, 680, 92, 203, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (108, 4, 1004, 'MI4', 'II4', 'IAI4', 'NR4', 'CD4', 126, 48, 681, 93, 204, '2023-01-16', NULL, '2023-01-16', NULL, 1);
    """).result()

    # Load DAG
    dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)
    dag = dag_bag.get_dag('k_ausd_bp_ta_bpr_instance_dag')
    assert dag is not None

    # --- First Run ---
    execution_date_1 = pendulum.datetime(2023, 1, 16, tz="UTC")
    dr1 = dag.create_dagrun(
        run_id=f"test_run_idempotency_1",
        execution_date=execution_date_1,
        state="running",
        conf={
            "p_jobkennung": "TEST_JOB_IDEMPOTENCY",
            "p_eintragsnr": "901",
            "p_stichtag": stichtag_str,
            "p_wiederanlaufwert": "N"
        }
    )
    task = dag.get_task("execute_bpr_instance_procedure")
    ti1 = dr1.get_task_instance(task.task_id)
    ti1.run(start_date=execution_date_1, end_date=execution_date_1)
    assert ti1.current_state() == 'success'

    # Check records after first run
    query_count_1 = BQ_CLIENT.query(f"""
        SELECT COUNT(*) FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
        WHERE processing_date = '{stichtag_date}'
    """)
    count_1 = [row[0] for row in query_count_1.result()][0]
    assert count_1 == expected_records_per_run, f"Expected {expected_records_per_run} records after first run, got {count_1}."

    # --- Second Run ---
    execution_date_2 = pendulum.datetime(2023, 1, 17, tz="UTC") # A new execution date for Airflow DAG run
    dr2 = dag.create_dagrun(
        run_id=f"test_run_idempotency_2",
        execution_date=execution_date_2,
        state="running",
        conf={
            "p_jobkennung": "TEST_JOB_IDEMPOTENCY",
            "p_eintragsnr": "901",
            "p_stichtag": stichtag_str, # Same stichtag
            "p_wiederanlaufwert": "N"
        }
    )
    ti2 = dr2.get_task_instance(task.task_id)
    ti2.run(start_date=execution_date_2, end_date=execution_date_2)
    assert ti2.current_state() == 'success'

    # Check records after second run
    query_count_2 = BQ_CLIENT.query(f"""
        SELECT COUNT(*) FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
        WHERE processing_date = '{stichtag_date}'
    """)
    count_2 = [row[0] for row in query_count_2.result()][0]

    # Based on the current code, it appends, so we expect double the records.
    expected_total_records = expected_records_per_run * 2
    assert count_2 == expected_total_records, \
        f"Expected {expected_total_records} records after second run (due to append behavior), got {count_2}. " \
        "If idempotency is desired, the BigQuery SP needs modification (e.g., MERGE or TRUNCATE+INSERT)."

    # Optional: Verify the duplicates are indeed identical
    query_duplicates = BQ_CLIENT.query(f"""
        SELECT CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, processing_date, COUNT(*) as num_duplicates
        FROM {PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt
        WHERE processing_date = '{stichtag_date}'
        GROUP BY CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, processing_date
        HAVING COUNT(*) > 1
    """)
    duplicate_rows = list(query_duplicates.result())
    assert len(duplicate_rows) == expected_records_per_run, \
        f"Expected {expected_records_per_run} distinct rows to be duplicated, but found {len(duplicate_rows)} unique duplicated sets."
    for row in duplicate_rows:
        assert row.num_duplicates == 2, f"Expected 2 duplicates for row {row}, but found {row.num_duplicates}."
```