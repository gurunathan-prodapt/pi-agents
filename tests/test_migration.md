As a senior data-migration QA engineer, I've analyzed the migration design for `k_ausd_adressen.ksh` and `d_ausd_adressen.sql` to BigQuery. The migration involves translating KornShell orchestration to a BigQuery Stored Procedure (`k_ausd_adressen_control.bq.sql`) and Oracle SQL*Plus data processing to another BigQuery Stored Procedure (`d_ausd_adressen.bq.sql`).

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality.

---

## Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **BigQuery Project and Datasets**:
    *   A Google Cloud Project (e.g., `your-gcp-project-id`).
    *   BigQuery datasets created: `cds`, `glv`, `bpd`, `sof`, `isbert_schema`.
2.  **Source Data Ingestion**:
    *   The Oracle source tables (`cds$ta_`, `glv$ta_`, `bpd$ta_`) have been ingested into their respective BigQuery datasets (e.g., `your-gcp-project-id.cds.ta_bp_ref`, `your-gcp-project-id.glv.ta_country`, etc.).
    *   This ingestion must be verified for data accuracy and completeness for the test period.
    *   For output parity tests, a specific snapshot of Oracle data (sources and outputs) should be available.
3.  **Target Table DDLs**:
    *   The `job_table_ddl.sql` has been executed to create `your-gcp-project-id.isbert_schema.job_table`.
    *   All target tables in the `sof` dataset (e.g., `ta_bp_ref_gp`, `ta_e_reach_gp`, etc.) have been created with the correct schemas.
4.  **Stored Procedures Deployed**:
    *   `d_ausd_adressen.bq.sql` has been deployed as `your-gcp-project-id.sof.d_ausd_adressen_proc`.
    *   `k_ausd_adressen_control.bq.sql` has been deployed as `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`.
5.  **Test Data**:
    *   For each test, specific test data will be described. For end-to-end parity, a full snapshot of legacy Oracle source data and corresponding legacy output data is crucial.

---

## Test Case 1: End-to-End Output Parity (Happy Path)

**Purpose**: To verify that the migrated BigQuery job, when executed with valid inputs, produces identical final output data in all target tables as the legacy KornShell job for the same inputs. This is the most critical test for behavioral equivalence.

**Setup**:
1.  **Legacy Environment**:
    *   Identify a specific `Stichtag` (e.g., `20230101`).
    *   Ensure the Oracle source tables (`cds$ta_`, `glv$ta_`, `bpd$ta_`) are in a known state corresponding to this `Stichtag`.
    *   Run the legacy `k_ausd_adressen.ksh` job with `Stichtag=20230101` and other required parameters.
    *   Extract the full contents of all final target tables (`sof$ta_e_reach_gp`, `sof$ta_e_business_gp`, `sof$ta_e_regulierer`, etc.) from Oracle into flat files (e.g., CSV, JSON) or a temporary database.
2.  **BigQuery Environment**:
    *   Load the exact same snapshot of Oracle source data into the corresponding BigQuery source tables (`your-gcp-project-id.cds.ta_bp_ref`, etc.).
    *   Ensure all BigQuery target tables in the `sof` dataset are empty before execution.
    *   Ensure `your-gcp-project-id.isbert_schema.job_table` is empty.

**Action**:
Execute the migrated BigQuery control procedure with the same parameters as the legacy job:
```sql
CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
  p_JobKennung => 'TEST_JOB',
  p_EintragsNr => '12345',
  p_Stichtag => '01012023', -- Corresponds to 20230101
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion**:
*   The BigQuery job completes successfully without errors.
*   For *every* final target table in BigQuery (e.g., `your-gcp-project-id.sof.ta_e_reach_gp`, `your-gcp-project-id.sof.ta_e_business_gp`, `your-gcp-project-id.sof.ta_e_regulierer`), the data content (row count, column values, order-independent comparison) is *identical* to the corresponding legacy Oracle output table.

**Runnable Test Code (Python with `pytest` and `google-cloud-bigquery`)**:
```python
import pytest
from google.cloud import bigquery
import pandas as pd

# Configuration
PROJECT_ID = "your-gcp-project-id"
BQ_CONTROL_PROC = f"{PROJECT_ID}.isbert_schema.k_ausd_adressen_control"
STICH_TAG = "01012023" # DDMMYYYY format
JOB_KENNUNG = "TEST_JOB"
EINTRAGS_NR = "12345"
WIEDERANLAUF_WERT = 0

# List of final target tables to compare
FINAL_TARGET_TABLES = [
    f"{PROJECT_ID}.sof.ta_e_reach_gp",
    f"{PROJECT_ID}.sof.ta_e_reach_re",
    f"{PROJECT_ID}.sof.ta_e_reach_ev",
    f"{PROJECT_ID}.sof.ta_e_reach_dn",
    f"{PROJECT_ID}.sof.ta_e_business_gp",
    f"{PROJECT_ID}.sof.ta_e_business_re",
    f"{PROJECT_ID}.sof.ta_e_business_ev",
    f"{PROJECT_ID}.sof.ta_e_business_dn",
    f"{PROJECT_ID}.sof.ta_e_regulierer",
]

# Paths to legacy output data (e.g., CSV files)
LEGACY_OUTPUT_PATHS = {
    "ta_e_reach_gp": "legacy_output/ta_e_reach_gp_20230101.csv",
    # ... add paths for all other final target tables
}

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module", autouse=True)
def run_bigquery_job(bq_client):
    """Fixture to run the BigQuery control procedure once for all tests."""
    print(f"\nRunning BigQuery control procedure: {BQ_CONTROL_PROC}...")
    query = f"""
    CALL {BQ_CONTROL_PROC}(
      p_JobKennung => '{JOB_KENNUNG}',
      p_EintragsNr => '{EINTRAGS_NR}',
      p_Stichtag => '{STICH_TAG}',
      p_wiederanlaufWert => {WIEDERANLAUF_WERT}
    );
    """
    job = bq_client.query(query)
    job.result() # Wait for the job to complete
    print("BigQuery job completed.")
    yield # Allow tests to run
    # Optional: Cleanup BQ target tables if needed after all tests

def fetch_bq_table_data(bq_client, table_id):
    """Fetches all data from a BigQuery table as a Pandas DataFrame."""
    query = f"SELECT * FROM `{table_id}` ORDER BY 1, 2, 3" # Order for consistent comparison
    return bq_client.query(query).to_dataframe()

def load_legacy_data(file_path):
    """Loads legacy data from a CSV file into a Pandas DataFrame."""
    # Adjust dtype/parsing as necessary for your specific legacy data format
    return pd.read_csv(file_path).sort_values(by=list(pd.read_csv(file_path).columns[:3])).reset_index(drop=True)

def test_output_parity_all_tables(bq_client):
    """Compares all final BigQuery target tables with legacy outputs."""
    for table_name_suffix in LEGACY_OUTPUT_PATHS.keys():
        bq_table_id = f"{PROJECT_ID}.sof.{table_name_suffix}"
        legacy_file_path = LEGACY_OUTPUT_PATHS[table_name_suffix]

        print(f"Comparing table: {bq_table_id}")

        bq_df = fetch_bq_table_data(bq_client, bq_table_id)
        legacy_df = load_legacy_data(legacy_file_path)

        # Basic checks
        assert len(bq_df) == len(legacy_df), f"Row count mismatch for {table_name_suffix}"
        assert list(bq_df.columns) == list(legacy_df.columns), f"Column mismatch for {table_name_suffix}"

        # Detailed data comparison
        # Convert all columns to string to handle potential type differences (e.g., int vs float for NULLs)
        # and ensure consistent comparison.
        bq_df_str = bq_df.astype(str)
        legacy_df_str = legacy_df.astype(str)

        pd.testing.assert_frame_equal(bq_df_str, legacy_df_str, check_dtype=False, check_exact=False,
                                      rtol=1e-5, atol=1e-8,
                                      obj=f"Data mismatch for {table_name_suffix}")
        print(f"Comparison successful for {table_name_suffix}")

# To run this test:
# 1. Install google-cloud-bigquery and pandas: pip install google-cloud-bigquery pandas pytest
# 2. Set up your GCP credentials (e.g., `gcloud auth application-default login`)
# 3. Create 'legacy_output' directory and place your legacy CSV files.
# 4. Replace 'your-gcp-project-id' with your actual project ID.
# 5. Run pytest: pytest your_test_file.py
```

---

## Test Case 2: Parameter Validation and Error Handling

**Purpose**: To ensure the `k_ausd_adressen_control` procedure correctly validates input parameters and raises errors as specified in the migration design, mimicking the KornShell script's behavior.

**Setup**:
1.  Ensure `your-gcp-project-id.isbert_schema.job_table` is empty or can be cleared.
2.  The `d_ausd_adressen_proc` does not need to contain actual logic for this test, as the control procedure should fail before calling it.

**Action & Pass/Fail Criteria**:

### 2.1 Missing `p_JobKennung`
*   **Action**:
    ```sql
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => NULL, -- Missing
      p_EintragsNr => '12345',
      p_Stichtag => '01012023',
      p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail**: The call fails with an error message containing "FEHLER: Jobkennung fehlt. Parameter -j ist erforderlich."

### 2.2 Missing `p_EintragsNr`
*   **Action**:
    ```sql
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => 'TEST_JOB',
      p_EintragsNr => '', -- Empty string, also considered missing
      p_Stichtag => '01012023',
      p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail**: The call fails with an error message containing "FEHLER: EintragsNr fehlt. Parameter -f ist erforderlich."

### 2.3 Missing `p_Stichtag`
*   **Action**:
    ```sql
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => 'TEST_JOB',
      p_EintragsNr => '12345',
      p_Stichtag => NULL, -- Missing
      p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail**: The call fails with an error message containing "FEHLER: Stichtag fehlt. Parameter -s ist erforderlich."

### 2.4 Invalid `p_Stichtag` Format
*   **Action**:
    ```sql
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => 'TEST_JOB',
      p_EintragsNr => '12345',
      p_Stichtag => '2023-01-01', -- Invalid format
      p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail**: The call fails with an error message containing "FEHLER: Stichtag hat kein gueltiges Format DDMMYYYY (expected DDMMYYYY)."

**Runnable Test Code (Python with `pytest` and `google-cloud-bigquery`)**:
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

PROJECT_ID = "your-gcp-project-id"
BQ_CONTROL_PROC = f"{PROJECT_ID}.isbert_schema.k_ausd_adressen_control"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

@pytest.mark.parametrize("job_kennung, eintrags_nr, stichtag, expected_error_msg", [
    (None, "12345", "01012023", "FEHLER: Jobkennung fehlt"),
    ("TEST_JOB", "", "01012023", "FEHLER: EintragsNr fehlt"),
    ("TEST_JOB", "12345", None, "FEHLER: Stichtag fehlt"),
    ("TEST_JOB", "12345", "2023-01-01", "FEHLER: Stichtag hat kein gueltiges Format DDMMYYYY"),
    ("TEST_JOB", "12345", "01/01/2023", "FEHLER: Stichtag hat kein gueltiges Format DDMMYYYY"),
    ("TEST_JOB", "12345", "010123", "FEHLER: Stichtag hat kein gueltiges Format DDMMYYYY"), # Too short
])
def test_parameter_validation(bq_client, job_kennung, eintrags_nr, stichtag, expected_error_msg):
    """Tests various invalid parameter combinations."""
    query = f"""
    CALL {BQ_CONTROL_PROC}(
      p_JobKennung => {f"'{job_kennung}'" if job_kennung is not None else 'NULL'},
      p_EintragsNr => {f"'{eintrags_nr}'" if eintrags_nr is not None else 'NULL'},
      p_Stichtag => {f"'{stichtag}'" if stichtag is not None else 'NULL'},
      p_wiederanlaufWert => 0
    );
    """
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(query).result()
    assert expected_error_msg in str(excinfo.value)
```

---

## Test Case 3: Job Logging and Status Tracking

**Purpose**: To verify that the `k_ausd_adressen_control` procedure correctly logs job status, parameters, and record counts into `your-gcp-project-id.isbert_schema.job_table` at different stages (start, completion, failure).

**Setup**:
1.  Ensure `your-gcp-project-id.isbert_schema.job_table` is empty before each test run.
2.  Populate BigQuery source tables with minimal data to allow `d_ausd_adressen_proc` to process some records (e.g., 10-20 rows).

**Action & Pass/Fail Criteria**:

### 3.1 Successful Job Completion
*   **Action**: Execute the control procedure with valid parameters.
    ```sql
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => 'LOG_TEST_SUCCESS',
      p_EintragsNr => '10001',
      p_Stichtag => '01012023',
      p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail**:
    1.  Query `your-gcp-project-id.isbert_schema.job_table`.
    2.  Verify there are two entries for `job_kennung = 'LOG_TEST_SUCCESS'` and `eintrags_nr = '10001'`:
        *   One with `job_status = 'RUNNING'` (created at start).
        *   One with `job_status = 'COMPLETED'` (updated/inserted at end).
    3.  The `COMPLETED` entry should have `record_count` reflecting the actual number of records in `your-gcp-project-id.sof.ta_e_business_gp` after the run.
    4.  `stichtag` should be `'01012023'`, `process_date` should be `'2023-01-01'`, `restart_flag` should be `'N'`.

### 3.2 Job Failure
*   **Action**: Introduce an intentional error in `d_ausd_adressen_proc` (e.g., by temporarily changing a table name to a non-existent one) or simulate an error by raising one within `d_ausd_adressen_proc`. Then execute the control procedure:
    ```sql
    -- Assume d_ausd_adressen_proc is modified to raise an error
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => 'LOG_TEST_FAIL',
      p_EintragsNr => '10002',
      p_Stichtag => '01012023',
      p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail**:
    1.  The call fails with an error.
    2.  Query `your-gcp-project-id.isbert_schema.job_table`.
    3.  Verify there is an entry for `job_kennung = 'LOG_TEST_FAIL'` and `eintrags_nr = '10002'` with `job_status = 'FAILED'`.
    4.  The `description` column should contain the error message.

### 3.3 `p_wiederanlaufWert` Handling
*   **Action**: Execute the control procedure with `p_wiederanlaufWert = 1`.
    ```sql
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => 'LOG_TEST_RESTART',
      p_EintragsNr => '10003',
      p_Stichtag => '01012023',
      p_wiederanlaufWert => 1
    );
    ```
*   **Pass/Fail**:
    1.  The job completes successfully.
    2.  Query `your-gcp-project-id.isbert_schema.job_table`.
    3.  Verify the `COMPLETED` entry for `job_kennung = 'LOG_TEST_RESTART'` has `restart_flag = 'Y'`.
    4.  Verify if `p_wiederanlaufWert` is `NULL`, `restart_flag` is `'N'`.

**Runnable Test Code (Python with `pytest` and `google-cloud-bigquery`)**:
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import time

PROJECT_ID = "your-gcp-project-id"
BQ_CONTROL_PROC = f"{PROJECT_ID}.isbert_schema.k_ausd_adressen_control"
JOB_TABLE = f"{PROJECT_ID}.isbert_schema.job_table"
D_AUSD_PROC = f"{PROJECT_ID}.sof.d_ausd_adressen_proc" # The data processing proc

@pytest.fixture(scope="function")
def bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    # Clear job_table before each test
    client.query(f"TRUNCATE TABLE `{JOB_TABLE}`").result()
    # Ensure d_ausd_adressen_proc is in a working state for success tests
    # (This might involve re-deploying the original d_ausd_adressen.bq.sql if it was modified for failure tests)
    # For simplicity, assume d_ausd_adressen_proc is always deployed correctly for these tests.
    yield client
    client.query(f"TRUNCATE TABLE `{JOB_TABLE}`").result() # Clean up after test

def test_job_logging_success(bq_client):
    """Tests successful job logging."""
    job_kennung = "LOG_TEST_SUCCESS"
    eintrags_nr = "10001"
    stichtag = "01012023"
    wiederanlauf_wert = 0

    query = f"""
    CALL {BQ_CONTROL_PROC}(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    bq_client.query(query).result()

    # Verify job_table entries
    job_entries_query = f"""
    SELECT job_status, record_count, stichtag, process_date, restart_flag
    FROM `{JOB_TABLE}`
    WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
    ORDER BY created_at
    """
    job_entries = bq_client.query(job_entries_query).to_dataframe()

    assert len(job_entries) == 2
    assert job_entries.iloc[0]['job_status'] == 'RUNNING'
    assert job_entries.iloc[1]['job_status'] == 'COMPLETED'
    assert job_entries.iloc[1]['stichtag'] == stichtag
    assert job_entries.iloc[1]['process_date'] == '2023-01-01'
    assert job_entries.iloc[1]['restart_flag'] == 'N'
    assert job_entries.iloc[1]['record_count'] >= 0 # Should be actual count, depends on data

def test_job_logging_failure(bq_client):
    """Tests job logging on failure."""
    job_kennung = "LOG_TEST_FAIL"
    eintrags_nr = "10002"
    stichtag = "01012023"

    # Temporarily modify d_ausd_adressen_proc to force an error
    # This is a mock-up. In a real scenario, you might have a test version of the proc
    # or use a different mechanism to induce failure.
    # For this example, we'll assume the error is raised within the control proc itself
    # or that d_ausd_adressen_proc is designed to fail under certain conditions.
    # For a robust test, you'd deploy a failing version of d_ausd_adressen_proc.
    # Example:
    # bq_client.query(f"CREATE OR REPLACE PROCEDURE {D_AUSD_PROC}(p_stichtag STRING) BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Forced error'; END;").result()

    query = f"""
    CALL {BQ_CONTROL_PROC}(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => 0
    );
    """
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(query).result()

    assert "Job failed for LOG_TEST_FAIL" in str(excinfo.value)

    # Verify job_table entries
    job_entries_query = f"""
    SELECT job_status, description
    FROM `{JOB_TABLE}`
    WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
    ORDER BY created_at DESC
    """
    job_entries = bq_client.query(job_entries_query).to_dataframe()

    assert len(job_entries) >= 1 # Could be 1 (failed immediately) or 2 (running then failed)
    assert job_entries.iloc[0]['job_status'] == 'FAILED'
    assert "Processing failed" in job_entries.iloc[0]['description']

    # Restore d_ausd_adressen_proc if it was modified
    # bq_client.query(f"CREATE OR REPLACE PROCEDURE {D_AUSD_PROC}(p_stichtag STRING) BEGIN ... original code ... END;").result()


def test_job_logging_restart_flag(bq_client):
    """Tests restart_flag handling."""
    job_kennung = "LOG_TEST_RESTART"
    eintrags_nr = "10003"
    stichtag = "01012023"
    wiederanlauf_wert = 1 # Simulate restart

    query = f"""
    CALL {BQ_CONTROL_PROC}(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    bq_client.query(query).result()

    job_entries_query = f"""
    SELECT job_status, restart_flag
    FROM `{JOB_TABLE}`
    WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}' AND job_status = 'COMPLETED'
    """
    job_entry = bq_client.query(job_entries_query).to_dataframe()

    assert not job_entry.empty
    assert job_entry.iloc[0]['restart_flag'] == 'Y'

    # Test with NULL wiederanlaufWert
    job_kennung_null_restart = "LOG_TEST_NULL_RESTART"
    eintrags_nr_null_restart = "10004"
    query_null_restart = f"""
    CALL {BQ_CONTROL_PROC}(
      p_JobKennung => '{job_kennung_null_restart}',
      p_EintragsNr => '{eintrags_nr_null_restart}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => NULL
    );
    """
    bq_client.query(query_null_restart).result()

    job_entry_null_restart = bq_client.query(f"""
    SELECT job_status, restart_flag
    FROM `{JOB_TABLE}`
    WHERE job_kennung = '{job_kennung_null_restart}' AND eintrags_nr = '{eintrags_nr_null_restart}' AND job_status = 'COMPLETED'
    """).to_dataframe()

    assert not job_entry_null_restart.empty
    assert job_entry_null_restart.iloc[0]['restart_flag'] == 'N'
```

---

## Test Case 4: `d_ausd_adressen_proc` - Source Filtering Logic

**Purpose**: To verify the "as-of-date" filtering logic (`DATE(col) <= v_stichtag_date AND (col_modified IS NULL OR DATE(col_modified) > v_stichtag_date) AND DATE(col_valid_from) <= v_stichtag_date AND (col_valid_to IS NULL OR DATE(col_valid_to) > v_stichtag_date) AND is_production = 1`) is correctly applied across all source tables.

**Setup**:
1.  Create a dedicated test table `your-gcp-project-id.cds.test_bp_ref` with the same schema as `ta_bp_ref`.
2.  Populate `test_bp_ref` with specific test data to cover various date scenarios and `is_production` flags.

**Test Data Example (`your-gcp-project-id.cds.test_bp_ref`)**:

| bp_id | insert_at  | modified_at | valid_from | valid_to   | is_production | bp_ref_ty | address_ref_ty | ...other_cols | Expected (Stichtag=2023-01-01) |
| :---- | :--------- | :---------- | :--------- | :--------- | :------------ | :-------- | :------------- | :------------ | :----------------------------- |
| 1     | 2022-12-01 | NULL        | 2022-12-01 | NULL       | 1             | 4         | 6              | ...           | YES (Active, no modification, no end date) |
| 2     | 2022-12-01 | 2023-01-02  | 2022-12-01 | NULL       | 1             | 4         | 6              | ...           | YES (Modified after Stichtag) |
| 3     | 2022-12-01 | 2022-12-15  | 2022-12-01 | NULL       | 1             | 4         | 6              | ...           | NO (Modified before/on Stichtag) |
| 4     | 2022-12-01 | NULL        | 2022-12-01 | 2022-12-31 | 1             | 4         | 6              | ...           | NO (Valid_to before/on Stichtag) |
| 5     | 2022-12-01 | NULL        | 2022-12-01 | 2023-01-02 | 1             | 4         | 6              | ...           | YES (Valid_to after Stichtag) |
| 6     | 2023-01-02 | NULL        | 2023-01-02 | NULL       | 1             | 4         | 6              | ...           | NO (Inserted after Stichtag) |
| 7     | 2022-12-01 | NULL        | 2023-01-02 | NULL       | 1             | 4         | 6              | ...           | NO (Valid_from after Stichtag) |
| 8     | 2022-12-01 | NULL        | 2022-12-01 | NULL       | 0             | 4         | 6              | ...           | NO (is_production = 0) |

**Action**:
1.  Temporarily modify `d_ausd_adressen_proc` to use `your-gcp-project-id.cds.test_bp_ref` instead of `your-gcp-project-id.cds.ta_bp_ref` for one of the `INSERT` statements (e.g., `Step02a`).
2.  Call `your-gcp-project-id.sof.d_ausd_adressen_proc` with `p_stichtag = '01012023'`.
    ```sql
    CALL `your-gcp-project-id.sof.d_ausd_adressen_proc`(p_stichtag => '01012023');
    ```
3.  Query the affected target table (e.g., `your-gcp-project-id.sof.ta_bp_ref_gp`).

**Pass/Fail Criterion**:
*   The `your-gcp-project-id.sof.ta_bp_ref_gp` table contains only `bp_id` 1, 2, and 5, matching the expected output from the test data.
*   Repeat this test for other source tables (`ta_inv_definition`, `ta_reachability`, `ta_business_partner`) to ensure consistent date filtering.

**Runnable Test Code (SQL Assertion)**:
```sql
-- Setup: Create and populate test_bp_ref
CREATE OR REPLACE TABLE `your-gcp-project-id.cds.test_bp_ref` (
  bp_id INT64,
  insert_at DATE,
  modified_at DATE,
  valid_from DATE,
  valid_to DATE,
  is_production INT64,
  bp_ref_ty INT64,
  address_ref_ty INT64,
  reachability_id INT64, -- Add other required columns for the insert
  cntrct_cp2_id INT64,
  inv_def_invrec_id INT64,
  bpr_inst_evnrec_id INT64,
  bpr_inst_srvusr_id INT64
);

INSERT INTO `your-gcp-project-id.cds.test_bp_ref` VALUES
(1, '2022-12-01', NULL, '2022-12-01', NULL, 1, 4, 6, 101, 201, 301, 401, 501),
(2, '2022-12-01', '2023-01-02', '2022-12-01', NULL, 1, 4, 6, 102, 202, 302, 402, 502),
(3, '2022-12-01', '2022-12-15', '2022-12-01', NULL, 1, 4, 6, 103, 203, 303, 403, 503), -- Should be filtered out by modified_at
(4, '2022-12-01', NULL, '2022-12-01', '2022-12-31', 1, 4, 6, 104, 204, 304, 404, 504), -- Should be filtered out by valid_to
(5, '2022-12-01', NULL, '2022-12-01', '2023-01-02', 1, 4, 6, 105, 205, 305, 405, 505),
(6, '2023-01-02', NULL, '2023-01-02', NULL, 1, 4, 6, 106, 206, 306, 406, 506), -- Should be filtered out by insert_at
(7, '2022-12-01', NULL, '2023-01-02', NULL, 1, 4, 6, 107, 207, 307, 407, 507), -- Should be filtered out by valid_from
(8, '2022-12-01', NULL, '2022-12-01', NULL, 0, 4, 6, 108, 208, 308, 408, 508); -- Should be filtered out by is_production

-- Action: Temporarily modify d_ausd_adressen_proc to use test_bp_ref
-- (This would be done by deploying a modified version of d_ausd_adressen.bq.sql)
-- Example modification for Step02a:
/*
  INSERT INTO `project.sof.ta_bp_ref_gp` (...)
  SELECT ...
  FROM `project.cds.test_bp_ref` AS bpr -- Changed from ta_bp_ref
  WHERE DATE(bpr.insert_at) <= v_stichtag_date
    AND (bpr.modified_at IS NULL OR DATE(bpr.modified_at) > v_stichtag_date)
    AND DATE(bpr.valid_from) <= v_stichtag_date
    AND (bpr.valid_to IS NULL OR DATE(bpr.valid_to) > v_stichtag_date)
    AND bpr.is_production = 1
    AND bpr.bp_ref_ty = 4
    AND bpr.address_ref_ty = 6;
*/

-- Call the procedure
CALL `your-gcp-project-id.sof.d_ausd_adressen_proc`(p_stichtag => '01012023');

-- Pass/Fail Criterion: SQL Assertion
SELECT
  CASE
    WHEN (SELECT COUNT(DISTINCT bp_id) FROM `your-gcp-project-id.sof.ta_bp_ref_gp`) = 3
         AND (SELECT COUNT(DISTINCT bp_id) FROM `your-gcp-project-id.sof.ta_bp_ref_gp` WHERE bp_id IN (1, 2, 5)) = 3
    THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result;

-- Cleanup: Truncate target table and restore d_ausd_adressen_proc
TRUNCATE TABLE `your-gcp-project-id.sof.ta_bp_ref_gp`;
DROP TABLE `your-gcp-project-id.cds.test_bp_ref`;
-- Re-deploy original d_ausd_adressen.bq.sql
```

---

## Test Case 5: `d_ausd_adressen_proc` - `UNION ALL` and Join Correctness

**Purpose**: To verify the `UNION ALL` logic in `ta_bp_ref_re` population and the correctness of various `JOIN` operations, including `LEFT JOIN` and `SUBSTR` function usage.

**Setup**:
1.  Populate minimal, controlled data into `your-gcp-project-id.cds.ta_bp_ref`, `your-gcp-project-id.cds.ta_inv_definition`, `your-gcp-project-id.glv.ta_country`, `your-gcp-project-id.glv.ta_description`, `your-gcp-project-id.bpd.ta_reachability`, and `your-gcp-project-id.bpd.ta_business_partner`.
2.  Ensure data covers cases for matching and non-matching join keys, and `NULL` values where `LEFT JOIN` is used.

**Test Data Example (Simplified for `ta_bp_ref_re` and `ta_e_reach_re`)**:

*   `your-gcp-project-id.cds.ta_bp_ref` (relevant for `BP_REF_TY = 1, ADDRESS_REF_TY = 5`):
    *   `bp_id=10`, `reachability_id=100`, `inv_def_invrec_id=NULL`, `is_production=1`, `valid_from=2023-01-01`
*   `your-gcp-project-id.cds.ta_inv_definition` (relevant for `rdndant_invrec = 0`):
    *   `inv_definition_id=200`, `rdndnt_cp2_bp_id=11`, `rdndnt_cp2_reachability_id=101`, `is_production=1`, `valid_from=2023-01-01`
*   `your-gcp-project-id.bpd.ta_reachability`:
    *   `bp_id=10`, `reachability_id=100`, `country_code='DEU'`, `short_description='Germany'`
    *   `bp_id=11`, `reachability_id=101`, `country_code='USA'`, `short_description='United States'`
*   `your-gcp-project-id.glv.ta_country`:
    *   `country_code='DEU'`, `description_id=1`, `valid=1`
    *   `country_code='USA'`, `description_id=2`, `valid=1`
    *   `country_code='XYZ'`, `description_id=3`, `valid=1` (no matching reachability)
*   `your-gcp-project-id.glv.ta_description`:
    *   `description_id=1`, `language='EN'`, `short_description='Germany'`
    *   `description_id=2`, `language='EN'`, `short_description='United States'`
    *   `description_id=3`, `language='EN'`, `short_description='Xyzland'`

**Action**:
1.  Call `your-gcp-project-id.sof.d_ausd_adressen_proc` with `p_stichtag = '01012023'`.
    ```sql
    CALL `your-gcp-project-id.sof.d_ausd_adressen_proc`(p_stichtag => '01012023');
    ```
2.  Query `your-gcp-project-id.sof.ta_bp_ref_re` and `your-gcp-project-id.sof.ta_e_reach_re`.

**Pass/Fail Criterion**:
*   `your-gcp-project-id.sof.ta_bp_ref_re` contains two rows: one for `bp_id=10` (from `ta_bp_ref`) and one for `bp_id=11` (from `ta_inv_definition`).
*   `your-gcp-project-id.sof.ta_e_reach_re` contains two rows, correctly joined with `ta_reachability` and `ta_laender_kng`.
    *   For `bp_id=10`, `land_sd` should be `'Ger'`.
    *   For `bp_id=11`, `land_sd` should be `'Uni'`.
*   Verify that `LEFT JOIN` behavior is correct when `ta_laender_kng` has no match (e.g., if a `country_code` in `ta_reachability` doesn't exist in `ta_laender_kng`, `land_sd` should be `NULL`).

**Runnable Test Code (SQL Assertion)**:
```sql
-- Setup: Populate minimal test data
-- Clear all relevant tables first
TRUNCATE TABLE `your-gcp-project-id.cds.ta_bp_ref`;
TRUNCATE TABLE `your-gcp-project-id.cds.ta_inv_definition`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_country`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_description`;
TRUNCATE TABLE `your-gcp-project-id.bpd.ta_reachability`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_bp_ref_re`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_e_reach_re`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_laender_kng`; -- Also clear intermediate tables

INSERT INTO `your-gcp-project-id.cds.ta_bp_ref` VALUES
(10, 100, NULL, NULL, NULL, '2023-01-01', NULL, NULL, 1, 1, 5, '2023-01-01', NULL); -- bp_ref_ty=1, address_ref_ty=5

INSERT INTO `your-gcp-project-id.cds.ta_inv_definition` VALUES
(200, 11, 101, 0, 1, '2023-01-01', NULL, '2023-01-01', NULL); -- rdndant_invrec=0

INSERT INTO `your-gcp-project-id.bpd.ta_reachability` VALUES
(10, 100, 1, 'DEU', 'Attn1', 'Attach1', 'Org1', 'Corp1', 'Surname1', 'First1', '12345', 'City1', 'Pobox1', 'Street1', 'House1', 'AreaA1', 'AreaP1', 'OU1', 'AddrLine1', 'AddrLine2', '2023-01-01', NULL, '2023-01-01', NULL, 1),
(11, 101, 1, 'USA', 'Attn2', 'Attach2', 'Org2', 'Corp2', 'Surname2', 'First2', '67890', 'City2', 'Pobox2', 'Street2', 'House2', 'AreaA2', 'AreaP2', 'OU2', 'AddrLine3', 'AddrLine4', '2023-01-01', NULL, '2023-01-01', NULL, 1),
(12, 102, 1, 'XXX', 'Attn3', 'Attach3', 'Org3', 'Corp3', 'Surname3', 'First3', '11111', 'City3', 'Pobox3', 'Street3', 'House3', 'AreaA3', 'AreaP3', 'OU3', 'AddrLine5', 'AddrLine6', '2023-01-01', NULL, '2023-01-01', NULL, 1); -- Country code 'XXX' will not match

INSERT INTO `your-gcp-project-id.glv.ta_country` VALUES
('DEU', 1, NULL, 1, 'DE', 'DE', 1),
('USA', 2, NULL, 0, 'US', 'US', 1);

INSERT INTO `your-gcp-project-id.glv.ta_description` VALUES
(1, 'EN', 'Germany', 'Germany Long', 'Germany Very Long'),
(2, 'EN', 'United States', 'United States Long', 'United States Very Long');

-- Action: Call the procedure
CALL `your-gcp-project-id.sof.d_ausd_adressen_proc`(p_stichtag => '01012023');

-- Pass/Fail Criterion: SQL Assertions
-- Check ta_bp_ref_re (UNION ALL)
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_bp_ref_re`) = 2
         AND (SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_bp_ref_re` WHERE bp_id = 10 AND inv_def_invrec_id IS NULL) = 1
         AND (SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_bp_ref_re` WHERE bp_id = 11 AND inv_def_invrec_id = 200) = 1
    THEN 'PASS: ta_bp_ref_re UNION ALL'
    ELSE 'FAIL: ta_bp_ref_re UNION ALL'
  END AS test_result_union_all;

-- Check ta_e_reach_re (JOINs and SUBSTR)
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_e_reach_re`) = 3 -- Should include the XXX country code with NULL land_sd
         AND (SELECT land_sd FROM `your-gcp-project-id.sof.ta_e_reach_re` WHERE bp_id = 10) = 'Ger'
         AND (SELECT land_sd FROM `your-gcp-project-id.sof.ta_e_reach_re` WHERE bp_id = 11) = 'Uni'
         AND (SELECT land_sd FROM `your-gcp-project-id.sof.ta_e_reach_re` WHERE bp_id = 12) IS NULL
    THEN 'PASS: ta_e_reach_re JOINs and SUBSTR'
    ELSE 'FAIL: ta_e_reach_re JOINs and SUBSTR'
  END AS test_result_joins;

-- Cleanup: Truncate all tables used in this test
TRUNCATE TABLE `your-gcp-project-id.cds.ta_bp_ref`;
TRUNCATE TABLE `your-gcp-project-id.cds.ta_inv_definition`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_country`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_description`;
TRUNCATE TABLE `your-gcp-project-id.bpd.ta_reachability`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_bp_ref_re`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_e_reach_re`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_laender_kng`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_country`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_country_desc`;
```

---

## Test Case 6: Data Quality - Row Counts and Uniqueness

**Purpose**: To verify that row counts in intermediate and final tables match expectations (e.g., based on legacy job logs or detailed analysis) and that primary key columns maintain uniqueness where expected.

**Setup**:
1.  Populate BigQuery source tables with a diverse set of data, including duplicates in non-key columns, and data that should result in specific row counts after filtering/joining.
2.  Have access to legacy row counts for each intermediate and final table for a given `Stichtag`.

**Action**:
1.  Execute the full BigQuery job:
    ```sql
    CALL `your-gcp-project-id.isbert_schema.k_ausd_adressen_control`(
      p_JobKennung => 'DQ_TEST',
      p_EintragsNr => '20001',
      p_Stichtag => '01012023',
      p_wiederanlaufWert => 0
    );
    ```
2.  Query row counts for all intermediate and final `sof` tables.
3.  Query for duplicate `BP_ID` in tables where `DISTINCT BP_ID` was applied (e.g., `ta_bp_ref_gp_nodp`, `ta_e_business_gp`).

**Pass/Fail Criterion**:
*   **Row Counts**: The row count for each `your-gcp-project-id.sof.*` table matches the expected row count from the legacy system for the same `Stichtag` and source data.
*   **Uniqueness**: For tables like `ta_e_business_gp`, `ta_e_business_re`, `ta_e_business_ev`, `ta_e_business_dn`, `BP_ID` should be unique.
    ```sql
    -- Example for ta_e_business_gp
    SELECT
      CASE
        WHEN (SELECT COUNT(BP_ID) FROM `your-gcp-project-id.sof.ta_e_business_gp`) =
             (SELECT COUNT(DISTINCT BP_ID) FROM `your-gcp-project-id.sof.ta_e_business_gp`)
        THEN 'PASS: BP_ID is unique in ta_e_business_gp'
        ELSE 'FAIL: Duplicate BP_ID found in ta_e_business_gp'
      END AS uniqueness_test_result;
    ```

**Runnable Test Code (Python with `pytest` and `google-cloud-bigquery`)**:
```python
import pytest
from google.cloud import bigquery

PROJECT_ID = "your-gcp-project-id"
BQ_CONTROL_PROC = f"{PROJECT_ID}.isbert_schema.k_ausd_adressen_control"
STICH_TAG = "01012023"

# Expected row counts for a specific Stichtag and source data snapshot
# These values would come from legacy system analysis or previous runs.
EXPECTED_ROW_COUNTS = {
    f"{PROJECT_ID}.sof.ta_bp_ref_gp": 100,
    f"{PROJECT_ID}.sof.ta_bp_ref_re": 150,
    f"{PROJECT_ID}.sof.ta_e_reach_gp": 100,
    f"{PROJECT_ID}.sof.ta_e_business_gp": 80, # Example: some BP_IDs might not have business_partner details
    f"{PROJECT_ID}.sof.ta_e_regulierer": 50,
    # ... add all other intermediate and final tables
}

# Tables where BP_ID is expected to be unique
UNIQUE_BP_ID_TABLES = [
    f"{PROJECT_ID}.sof.ta_e_business_gp",
    f"{PROJECT_ID}.sof.ta_e_business_re",
    f"{PROJECT_ID}.sof.ta_e_business_ev",
    f"{PROJECT_ID}.sof.ta_e_business_dn",
]

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module", autouse=True)
def run_bigquery_job_for_dq(bq_client):
    """Fixture to run the BigQuery control procedure once for DQ tests."""
    print(f"\nRunning BigQuery control procedure for DQ tests: {BQ_CONTROL_PROC}...")
    query = f"""
    CALL {BQ_CONTROL_PROC}(
      p_JobKennung => 'DQ_TEST',
      p_EintragsNr => '20001',
      p_Stichtag => '{STICH_TAG}',
      p_wiederanlaufWert => 0
    );
    """
    job = bq_client.query(query)
    job.result()
    print("BigQuery DQ job completed.")
    yield

def test_row_counts(bq_client):
    """Verifies row counts for all relevant tables."""
    for table_id, expected_count in EXPECTED_ROW_COUNTS.items():
        query = f"SELECT COUNT(*) FROM `{table_id}`"
        row_count = bq_client.query(query).to_dataframe().iloc[0, 0]
        assert row_count == expected_count, f"Row count mismatch for {table_id}. Expected {expected_count}, got {row_count}."
        print(f"Row count for {table_id} is {row_count} (Expected: {expected_count}) - PASS")

def test_bp_id_uniqueness(bq_client):
    """Verifies BP_ID uniqueness in specified tables."""
    for table_id in UNIQUE_BP_ID_TABLES:
        query = f"""
        SELECT
          COUNT(BP_ID) AS total_count,
          COUNT(DISTINCT BP_ID) AS distinct_count
        FROM `{table_id}`
        """
        result = bq_client.query(query).to_dataframe().iloc[0]
        assert result['total_count'] == result['distinct_count'], f"Duplicate BP_ID found in {table_id}."
        print(f"BP_ID uniqueness for {table_id} - PASS")
```

---

## Test Case 7: NULL Handling and Data Type Consistency

**Purpose**: To ensure `NULL` values are propagated or handled as expected (e.g., `NVL` equivalents, `LEFT JOIN` behavior) and that data types remain consistent throughout the transformation, preventing data loss or unexpected conversions.

**Setup**:
1.  Populate source tables with data containing `NULL` values in various columns that are part of joins, filters, or direct selections.
2.  Include columns with different data types (e.g., `STRING`, `INT64`, `DATE`, `TIMESTAMP`) to verify type preservation.

**Action**:
1.  Execute the BigQuery job with a `Stichtag` that processes the test data.
2.  Query specific target tables and columns to inspect `NULL` values and data types.

**Pass/Fail Criterion**:
*   **NULL Propagation**: If a `NULL` value in a source column is expected to propagate to a target column (e.g., through a direct `SELECT` or `LEFT JOIN` where the right side is `NULL`), verify it does.
*   **`LEFT JOIN` with `NULL`**: In `ta_e_reach_*` tables, if `re.country_code` has no match in `lk.country_code`, `lk.short_description` (and thus `land_sd`) should be `NULL`.
*   **Data Type Match**: The data types of columns in the final target tables match the expected types, and no implicit conversions have altered data (e.g., a `STRING` column in Oracle should map to `STRING` in BQ, `NUMBER` to `INT64` or `BIGNUMERIC`).

**Runnable Test Code (SQL Assertion)**:
```sql
-- Setup: Populate source tables with NULLs and diverse types
-- (Example for ta_e_reach_gp, focusing on NULLs from LEFT JOIN)
TRUNCATE TABLE `your-gcp-project-id.cds.ta_bp_ref`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_country`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_description`;
TRUNCATE TABLE `your-gcp-project-id.bpd.ta_reachability`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_e_reach_gp`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_laender_kng`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_bp_ref_gp`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_country`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_country_desc`;

INSERT INTO `your-gcp-project-id.cds.ta_bp_ref` VALUES
(1, 101, NULL, NULL, NULL, '2023-01-01', NULL, NULL, 1, 4, 6, '2023-01-01', NULL), -- Valid BP_REF
(2, 102, NULL, NULL, NULL, '2023-01-01', NULL, NULL, 1, 4, 6, '2023-01-01', NULL); -- Valid BP_REF

INSERT INTO `your-gcp-project-id.bpd.ta_reachability` VALUES
(1, 101, 1, 'DEU', 'Attn1', 'Attach1', 'Org1', 'Corp1', 'Surname1', 'First1', '12345', 'City1', 'Pobox1', 'Street1', 'House1', 'AreaA1', 'AreaP1', 'OU1', 'AddrLine1', 'AddrLine2', '2023-01-01', NULL, '2023-01-01', NULL, 1),
(2, 102, 1, 'NON', 'Attn2', 'Attach2', 'Org2', 'Corp2', 'Surname2', 'First2', '67890', 'City2', 'Pobox2', 'Street2', 'House2', 'AreaA2', 'AreaP2', 'OU2', 'AddrLine3', 'AddrLine4', '2023-01-01', NULL, '2023-01-01', NULL, 1); -- 'NON' country code will not match

INSERT INTO `your-gcp-project-id.glv.ta_country` VALUES
('DEU', 1, NULL, 1, 'DE', 'DE', 1);

INSERT INTO `your-gcp-project-id.glv.ta_description` VALUES
(1, 'EN', 'Germany', 'Germany Long', 'Germany Very Long');

-- Action: Call the procedure
CALL `your-gcp-project-id.sof.d_ausd_adressen_proc`(p_stichtag => '01012023');

-- Pass/Fail Criterion: SQL Assertions
-- Check NULL handling for land_sd from LEFT JOIN
SELECT
  CASE
    WHEN (SELECT land_sd FROM `your-gcp-project-id.sof.ta_e_reach_gp` WHERE bp_id = 1) = 'Ger'
         AND (SELECT land_sd FROM `your-gcp-project-id.sof.ta_e_reach_gp` WHERE bp_id = 2) IS NULL
    THEN 'PASS: LEFT JOIN NULL handling for land_sd'
    ELSE 'FAIL: LEFT JOIN NULL handling for land_sd'
  END AS test_result_null_handling;

-- Check data type of a specific column (e.g., BP_ID should be INT64)
SELECT
  CASE
    WHEN (SELECT data_type FROM `your-gcp-project-id.sof.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'ta_e_reach_gp' AND column_name = 'BP_ID') = 'INT64'
    THEN 'PASS: BP_ID data type is INT64'
    ELSE 'FAIL: BP_ID data type is not INT64'
  END AS test_result_data_type;

-- Cleanup
TRUNCATE TABLE `your-gcp-project-id.cds.ta_bp_ref`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_country`;
TRUNCATE TABLE `your-gcp-project-id.glv.ta_description`;
TRUNCATE TABLE `your-gcp-project-id.bpd.ta_reachability`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_e_reach_gp`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_laender_kng`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_bp_ref_gp`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_country`;
TRUNCATE TABLE `your-gcp-project-id.sof.ta_country_desc`;
```