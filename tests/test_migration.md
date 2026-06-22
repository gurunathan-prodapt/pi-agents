As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `r_ausd_v_ta_inv_def.ksh` job migration. These tests aim to ensure the new Google Cloud Platform (GCP) implementation, leveraging Cloud Composer, Dataform, and BigQuery, is behaviourally equivalent to the legacy KornShell and Oracle SQL*Plus workflow.

The tests are categorized according to the requirements: Output Parity, Transformation Correctness, External-System Replacements, and Data Quality/Schema Assertions. Each test case includes its purpose, setup, action, and a concrete pass/fail criterion, with runnable code examples where appropriate.

---

## Migration Validation Tests: `r_ausd_v_ta_inv_def.ksh`

### Prerequisites for Testing

Before executing these tests, ensure the following:

1.  **GCP Environment Setup**: Cloud Composer, BigQuery, and Dataform are configured and accessible.
2.  **Data Ingestion Pipeline**: The pipeline responsible for ingesting data from the Carmen database into BigQuery staging tables (`stg_carmen.dwtk_meldungen`, `stg_carmen.cds_ta_inv_definition`, `stg_carmen.cds_ta_inv_cont_config`, `stg_carmen.cds_ta_care_description`) is operational and has loaded representative data.
3.  **Legacy Environment Access**: Access to the legacy Oracle database and the ability to execute the original KornShell scripts and SQL*Plus queries for comparison.
4.  **Test Data**: A set of controlled, representative test data (including edge cases) is available in both Oracle and BigQuery staging tables. For output parity, a snapshot of production-like data is crucial.
5.  **Airflow DAG Deployment**: The `r_ausd_v_ta_inv_def_dag.py` DAG is deployed to Cloud Composer.
6.  **Dataform Project Deployment**: The Dataform project (including `sof_ta_inv_def.sqlx` and declarations) is deployed and compiled.

---

### Test Setup (Conceptual `conftest.py` for Pytest)

```python
# conftest.py
import pytest
from google.cloud import bigquery
from airflow.api.client.local_client import Client as AirflowClient
import os
import subprocess
import json
import time

# --- Configuration ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BIGQUERY_DATASET_STG = "stg_carmen"
BIGQUERY_DATASET_DWH = "dwh"
BIGQUERY_TABLE_TARGET = "sof_ta_inv_def"
DATAFORM_REPOSITORY_ID = os.environ.get("DATAFORM_REPOSITORY_ID", "your-dataform-repository-id")
DATAFORM_REGION = os.environ.get("DATAFORM_REGION", "us-central1")
AIRFLOW_DAG_ID = "r_ausd_v_ta_inv_def_dag"
# Assume Oracle connection details are available via environment variables or a config file
# ORACLE_CONN_STR = os.environ.get("ORACLE_CONN_STR", "user/pass@host:port/service")

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client."""
    return bigquery.Client(project=GCP_PROJECT_ID)

@pytest.fixture(scope="session")
def airflow_client():
    """Provides an Airflow API client (for local testing, might need adjustment for Composer)."""
    # For Cloud Composer, you'd typically use the Airflow REST API or gcloud commands.
    # This is a placeholder for local Airflow testing.
    # For Composer, consider using `gcloud composer environments run <env_name> --location <location> dags trigger ...`
    # or the Airflow REST API directly.
    # For simplicity in this example, we'll assume direct interaction or shell commands.
    return None # Placeholder, actual interaction will be via subprocess calls

@pytest.fixture
def clean_bigquery_target_table(bq_client):
    """Ensures the target BigQuery table is empty before each test."""
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_DWH}.{BIGQUERY_TABLE_TARGET}"
    bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()
    print(f"Truncated BigQuery table: {table_id}")
    yield
    # Optional: Clean up after test if needed, but TRUNCATE before is usually sufficient for idempotency.

def run_legacy_oracle_job(job_kennung: str, v_datum: str, oracle_conn_str: str, source_data_path: str):
    """
    Simulates running the legacy KornShell job and captures Oracle output.
    This function would involve:
    1. Setting up the Oracle source tables with data from `source_data_path`.
    2. Executing the r_ausd_v_ta_inv_def.ksh script with appropriate parameters.
       This might involve SSHing to the legacy server or using a local KornShell emulator.
    3. Capturing the final state of SOF$TA_INV_DEF in Oracle.
    Returns: Oracle table data (e.g., list of dicts or pandas DataFrame).
    """
    print(f"Simulating legacy Oracle job run for job_kennung={job_kennung}, v_datum={v_datum}")
    # Placeholder for actual legacy job execution and data extraction
    # Example:
    # subprocess.run(["ssh", "legacy_server", f"/path/to/r_ausd_v_ta_inv_def.ksh -j {job_kennung} ..."])
    # Then connect to Oracle and fetch data:
    # import cx_Oracle
    # conn = cx_Oracle.connect(oracle_conn_str)
    # cursor = conn.cursor()
    # cursor.execute("SELECT * FROM SOF$TA_INV_DEF ORDER BY 1,2,3") # Order for consistent comparison
    # rows = cursor.fetchall()
    # cols = [col[0] for col in cursor.description]
    # conn.close()
    # return [dict(zip(cols, row)) for row in rows]
    return [] # Mock return

def trigger_airflow_dag(dag_id: str, conf: dict):
    """Triggers an Airflow DAG on Cloud Composer."""
    print(f"Triggering Airflow DAG: {dag_id} with conf: {conf}")
    # Example using gcloud CLI for Composer
    command = [
        "gcloud", "composer", "environments", "run",
        os.environ.get("COMPOSER_ENV_NAME", "your-composer-env-name"),
        "--location", os.environ.get("COMPOSER_LOCATION", "us-central1"),
        "dags", "trigger", dag_id,
        "--json",
        "--conf", json.dumps(conf)
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print("Airflow DAG trigger output:", result.stdout)
        # You might need to parse the output to get the DAG run ID and then poll for completion
        # For simplicity, we'll assume it triggers and we can wait for the Dataform job.
        time.sleep(60) # Give some time for the DAG to start and Dataform to run
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error triggering Airflow DAG: {e.stderr}")
        raise

def get_bigquery_table_data(bq_client, dataset: str, table: str):
    """Fetches all data from a BigQuery table."""
    table_id = f"{GCP_PROJECT_ID}.{dataset}.{table}"
    query = f"SELECT * FROM `{table_id}` ORDER BY 1,2,3" # Order for consistent comparison
    rows = bq_client.query(query).result()
    data = [dict(row) for row in rows]
    print(f"Fetched {len(data)} rows from BigQuery table: {table_id}")
    return data

def get_bigquery_row_count(bq_client, dataset: str, table: str):
    """Fetches row count from a BigQuery table."""
    table_id = f"{GCP_PROJECT_ID}.{dataset}.{table}"
    query = f"SELECT COUNT(*) FROM `{table_id}`"
    row_count = bq_client.query(query).result().single_value
    print(f"Row count for {table_id}: {row_count}")
    return row_count

def get_bigquery_table_schema(bq_client, dataset: str, table: str):
    """Fetches schema from a BigQuery table."""
    table_id = f"{GCP_PROJECT_ID}.{dataset}.{table}"
    table_ref = bq_client.get_table(table_id)
    schema = []
    for field in table_ref.schema:
        schema.append({
            "name": field.name,
            "field_type": field.field_type,
            "mode": field.mode, # NULLABLE, REQUIRED, REPEATED
        })
    print(f"Fetched schema for {table_id}: {schema}")
    return schema

def run_dataform_action(action_name: str, compilation_vars: dict = None):
    """
    Triggers a specific Dataform action (e.g., a model or assertion)
    using the Dataform CLI or API.
    """
    print(f"Running Dataform action: {action_name} with vars: {compilation_vars}")
    # This would typically involve `dataform run --actions <action_name> --vars '{"v_datum": "..."}'`
    # or using the Dataform API.
    # For simplicity, we'll assume the Airflow DAG handles the Dataform run.
    # If testing Dataform models in isolation, you'd use the Dataform CLI.
    # Example:
    # command = ["dataform", "run", "--project", GCP_PROJECT_ID, "--repository", DATAFORM_REPOSITORY_ID,
    #            "--actions", action_name, "--vars", json.dumps(compilation_vars)]
    # subprocess.run(command, check=True)
    pass

```

---

### Test Cases (`test_migration.py`)

```python
# test_migration.py
import pytest
from google.cloud import bigquery
import pandas as pd
import json

# Assume conftest.py is in the same directory or accessible via pytest config
from conftest import (
    GCP_PROJECT_ID, BIGQUERY_DATASET_STG, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET,
    DATAFORM_REPOSITORY_ID, DATAFORM_REGION, AIRFLOW_DAG_ID,
    run_legacy_oracle_job, trigger_airflow_dag, get_bigquery_table_data,
    get_bigquery_row_count, get_bigquery_table_schema, run_dataform_action
)

# --- Helper for data comparison ---
def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, sort_cols=None):
    """Compares two pandas DataFrames for equality, handling column order and data types."""
    if df1.empty and df2.empty:
        return True, "Both dataframes are empty."

    if df1.shape[0] != df2.shape[0]:
        return False, f"Row count mismatch: {df1.shape[0]} vs {df2.shape[0]}"

    # Ensure columns are in the same order for comparison
    common_cols = sorted(list(set(df1.columns) & set(df2.columns)))
    if not common_cols:
        return False, "No common columns found between dataframes."
    
    df1_filtered = df1[common_cols]
    df2_filtered = df2[common_cols]

    # Sort for consistent row comparison
    if sort_cols:
        df1_filtered = df1_filtered.sort_values(by=sort_cols).reset_index(drop=True)
        df2_filtered = df2_filtered.sort_values(by=sort_cols).reset_index(drop=True)
    else: # If no sort_cols, try to sort by all columns
        try:
            df1_filtered = df1_filtered.sort_values(by=list(df1_filtered.columns)).reset_index(drop=True)
            df2_filtered = df2_filtered.sort_values(by=list(df2_filtered.columns)).reset_index(drop=True)
        except TypeError: # Handle uncomparable types if sorting all columns fails
            pass # Fallback to unsorted comparison if sorting all columns is not feasible

    # Compare values, handling potential type differences (e.g., int vs float for NULLs)
    # Use .equals() for strict comparison, or more tolerant methods if needed
    if not df1_filtered.equals(df2_filtered):
        diff = df1_filtered.compare(df2_filtered)
        return False, f"Data mismatch found. Differences:\n{diff}"
    
    return True, "Dataframes are identical."

# --- Test Cases ---

@pytest.mark.output_parity
def test_e2e_output_parity(bq_client, clean_bigquery_target_table):
    """
    Purpose: Verify that the final output table `dwh.sof_ta_inv_def` in BigQuery
             is identical to the legacy `SOF$TA_INV_DEF` in Oracle, given the same
             source data and `v_datum`.
    """
    # Setup:
    # 1. Assume source data has been loaded into BigQuery staging tables.
    #    (This is handled by the ingestion pipeline, a prerequisite).
    # 2. Define a specific v_datum and job_kennung for this test run.
    test_job_kennung = "BERT_V_TA_INV_DEF_TEST"
    test_v_datum = "20230115" # Example date, ensure it exists in DWTK_MELDUNGEN for job_kennung

    # Action (Legacy): Run the legacy job and capture its output.
    # This would involve connecting to Oracle and executing the original script.
    # For this example, we'll mock the Oracle data or assume it's pre-captured.
    # In a real scenario, you'd call a function that executes the legacy job and extracts data.
    oracle_output_data = run_legacy_oracle_job(
        job_kennung=test_job_kennung,
        v_datum=test_v_datum,
        oracle_conn_str=os.environ.get("ORACLE_CONN_STR"), # From conftest
        source_data_path="path/to/legacy_test_data_snapshot"
    )
    df_oracle = pd.DataFrame(oracle_output_data)
    print(f"Legacy Oracle output rows: {len(df_oracle)}")

    # Action (Migrated): Trigger the Airflow DAG.
    # The DAG will fetch v_datum from stg_carmen.dwtk_meldungen based on job_kennung.
    # Ensure stg_carmen.dwtk_meldungen contains a record for test_job_kennung
    # with timecreated corresponding to test_v_datum.
    airflow_conf = {
        "job_kennung": test_job_kennung,
        "default_v_datum": "19000101" # Should not be used if job_kennung found
    }
    trigger_airflow_dag(AIRFLOW_DAG_ID, airflow_conf)

    # Action (Migrated): Extract data from BigQuery target table.
    bq_output_data = get_bigquery_table_data(bq_client, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET)
    df_bq = pd.DataFrame(bq_output_data)
    print(f"Migrated BigQuery output rows: {len(df_bq)}")

    # Pass/Fail Criterion:
    # Compare the two DataFrames. Column names might differ in case (Oracle often uppercase).
    # Ensure consistent column names for comparison.
    df_oracle.columns = [col.lower() for col in df_oracle.columns] # Normalize column names
    
    is_identical, message = compare_dataframes(df_oracle, df_bq, sort_cols=['inv_definition_id', 'acc_ref_id'])
    assert is_identical, f"Output parity test failed: {message}"
    print("Output parity test passed: BigQuery output is identical to Oracle output.")


@pytest.mark.transformation
def test_join_logic_correctness(bq_client, clean_bigquery_target_table):
    """
    Purpose: Verify that the LEFT JOIN logic in BigQuery correctly replicates
             Oracle's (+) behavior, especially with conditions on the outer-joined tables.
    """
    # Setup:
    # Create controlled, small datasets for staging tables in BigQuery.
    # This would typically be done by inserting data directly into BQ for the test.
    test_v_datum = "20230101" # A date that allows all test records to pass initial filters

    # Scenario 1: All tables match
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_definition` (inv_definition_id, acc_ref_id, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (1, 'ACC1', 101, '2022-12-01', NULL, '2022-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_cont_config` (inv_cont_config_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (101, 201, '2022-12-01', NULL, '2022-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_care_description` (cds_description_id, cds_description) VALUES
        (201, 'Description A');
    """).result()

    # Scenario 2: cds_ta_inv_cont_config has no match
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_definition` (inv_definition_id, acc_ref_id, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (2, 'ACC2', 999, '2022-12-01', NULL, '2022-01-01', NULL, 1); -- No match for 999
    """).result()

    # Scenario 3: cds_ta_care_description has no match
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_definition` (inv_definition_id, acc_ref_id, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (3, 'ACC3', 103, '2022-12-01', NULL, '2022-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_cont_config` (inv_cont_config_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (103, 999, '2022-12-01', NULL, '2022-01-01', NULL, 1); -- No match for 999
    """).result()

    # Action: Run the Dataform model directly (or via a simplified Airflow DAG).
    # For direct Dataform execution, you'd use the Dataform CLI or API.
    # Here, we'll trigger the full DAG, assuming it processes the staging data.
    airflow_conf = {
        "job_kennung": "DUMMY_JOB_FOR_JOIN_TEST", # Needs a dummy entry in dwtk_meldungen
        "default_v_datum": test_v_datum
    }
    # Ensure a dummy entry exists in dwtk_meldungen for this job_kennung
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('{airflow_conf['job_kennung']}', PARSE_TIMESTAMP('%Y%m%d', '{test_v_datum}'));
    """).result()
    trigger_airflow_dag(AIRFLOW_DAG_ID, airflow_conf)

    # Action: Get BigQuery results.
    bq_results = get_bigquery_table_data(bq_client, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET)
    df_bq = pd.DataFrame(bq_results)

    # Expected Oracle-equivalent results (manually derived for these scenarios)
    expected_data = [
        {'inv_definition_id': 1, 'acc_ref_id': 'ACC1', 'inv_pay_ty_cv': None, 'inv_media_cv': None, 'billcycle_id': None, 'sales_tax_freed': None, 'inv_cont_config_id': 101, 'rechn_inh_konfig_text': 'Description A'},
        {'inv_definition_id': 2, 'acc_ref_id': 'ACC2', 'inv_pay_ty_cv': None, 'inv_media_cv': None, 'billcycle_id': None, 'sales_tax_freed': None, 'inv_cont_config_id': 999, 'rechn_inh_konfig_text': None},
        {'inv_definition_id': 3, 'acc_ref_id': 'ACC3', 'inv_pay_ty_cv': None, 'inv_media_cv': None, 'billcycle_id': None, 'sales_tax_freed': None, 'inv_cont_config_id': 103, 'rechn_inh_konfig_text': None}
    ]
    df_expected = pd.DataFrame(expected_data)

    # Pass/Fail Criterion:
    is_identical, message = compare_dataframes(df_expected, df_bq, sort_cols=['inv_definition_id'])
    assert is_identical, f"Join logic correctness test failed: {message}"
    print("Join logic correctness test passed.")


@pytest.mark.transformation
def test_filter_conditions_and_null_handling(bq_client, clean_bigquery_target_table):
    """
    Purpose: Verify that date filters and is_production filters, including NULL handling
             and COALESCE translations, work correctly.
    """
    # Setup:
    test_v_datum = "20230115" # Reference date for filters

    # Insert controlled data into staging tables
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_definition` (inv_definition_id, acc_ref_id, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (10, 'A1', 1001, '2023-01-10', NULL, '2023-01-01', NULL, 1), -- Should pass (modified_at NULL, valid_to NULL)
        (11, 'A2', 1002, '2023-01-16', NULL, '2023-01-01', NULL, 1), -- Should fail (insert_at > v_datum)
        (12, 'A3', 1003, '2023-01-10', '2023-01-10', '2023-01-01', NULL, 1), -- Should fail (modified_at <= v_datum)
        (13, 'A4', 1004, '2023-01-10', '2023-01-20', '2023-01-01', NULL, 1), -- Should pass
        (14, 'A5', 1005, '2023-01-10', NULL, '2023-01-01', '2023-01-10', 1), -- Should fail (valid_to <= v_datum)
        (15, 'A6', 1006, '2023-01-10', NULL, '2023-01-01', NULL, 0); -- Should fail (is_production = 0)

        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_cont_config` (inv_cont_config_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (1001, 2001, '2023-01-10', NULL, '2023-01-01', NULL, 1), -- Matches ID 10, passes icc filters
        (1003, 2003, '2023-01-10', '2023-01-10', '2023-01-01', NULL, 1), -- Matches ID 12, fails icc.modified_at filter
        (1004, 2004, '2023-01-10', NULL, '2023-01-01', NULL, 0); -- Matches ID 13, fails icc.is_production filter

        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_care_description` (cds_description_id, cds_description) VALUES
        (2001, 'Desc 1'), (2003, 'Desc 3'), (2004, 'Desc 4');

        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('DUMMY_JOB_FOR_FILTER_TEST', PARSE_TIMESTAMP('%Y%m%d', '{test_v_datum}'));
    """).result()

    # Action: Trigger the Airflow DAG.
    airflow_conf = {
        "job_kennung": "DUMMY_JOB_FOR_FILTER_TEST",
        "default_v_datum": "19000101"
    }
    trigger_airflow_dag(AIRFLOW_DAG_ID, airflow_conf)

    # Action: Get BigQuery results.
    bq_results = get_bigquery_table_data(bq_client, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET)
    df_bq = pd.DataFrame(bq_results)

    # Expected results based on the filters and NULL handling
    expected_data = [
        {'inv_definition_id': 10, 'acc_ref_id': 'A1', 'inv_pay_ty_cv': None, 'inv_media_cv': None, 'billcycle_id': None, 'sales_tax_freed': None, 'inv_cont_config_id': 1001, 'rechn_inh_konfig_text': 'Desc 1'}
    ]
    df_expected = pd.DataFrame(expected_data)

    # Pass/Fail Criterion:
    is_identical, message = compare_dataframes(df_expected, df_bq, sort_cols=['inv_definition_id'])
    assert is_identical, f"Filter conditions and NULL handling test failed: {message}"
    print("Filter conditions and NULL handling test passed.")


@pytest.mark.external_systems
def test_v_datum_derivation(bq_client):
    """
    Purpose: Verify that the Airflow task `get_v_datum` correctly derives the
             `v_datum` parameter, replicating the Oracle `DWTK_MELDUNGEN` lookup.
    """
    # Setup:
    test_job_kennung = "BERT_V_TA_INV_DEF_VDATUM_TEST"
    expected_v_datum = "20230325"
    default_v_datum = "19000101"

    # Populate stg_carmen.dwtk_meldungen with test data
    bq_client.query(f"""
        TRUNCATE TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen`;
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('{test_job_kennung}', '2023-03-25 10:00:00 UTC'),
        ('{test_job_kennung}', '2023-03-20 12:00:00 UTC'),
        ('OTHER_JOB', '2023-04-01 08:00:00 UTC');
    """).result()

    # Action: Manually execute the `get_v_datum` task's SQL query.
    # In a real pytest, you might mock the Airflow task or trigger a minimal DAG.
    # Here, we'll directly execute the SQL that the task would run.
    query = f"""
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '{default_v_datum}')
        FROM
            `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` m
        WHERE
            m.job_kennung = '{test_job_kennung}';
    """
    result = bq_client.query(query).result()
    derived_v_datum = result.single_value
    print(f"Derived v_datum: {derived_v_datum}")

    # Pass/Fail Criterion:
    assert derived_v_datum == expected_v_datum, \
        f"v_datum derivation failed. Expected '{expected_v_datum}', got '{derived_v_datum}'."

    # Test case for no matching job_kennung (should use default)
    no_match_job_kennung = "NON_EXISTENT_JOB"
    query_no_match = f"""
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '{default_v_datum}')
        FROM
            `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` m
        WHERE
            m.job_kennung = '{no_match_job_kennung}';
    """
    result_no_match = bq_client.query(query_no_match).result()
    derived_v_datum_no_match = result_no_match.single_value
    print(f"Derived v_datum for no match: {derived_v_datum_no_match}")
    assert derived_v_datum_no_match == default_v_datum, \
        f"v_datum derivation for no match failed. Expected '{default_v_datum}', got '{derived_v_datum_no_match}'."
    print("v_datum derivation test passed.")


@pytest.mark.data_quality
def test_row_count_parity(bq_client, clean_bigquery_target_table):
    """
    Purpose: Verify that the final `dwh.sof_ta_inv_def` table in BigQuery has
             the same number of rows as the legacy `SOF$TA_INV_DEF` table in Oracle.
    """
    # Setup:
    # 1. Assume a known dataset is loaded into staging tables.
    # 2. Run the legacy Oracle job with this dataset and get its row count.
    test_job_kennung = "BERT_V_TA_INV_DEF_ROWCOUNT_TEST"
    test_v_datum = "20230201"
    
    # Populate staging tables with a known number of rows that should pass filters
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_definition` (inv_definition_id, acc_ref_id, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (1, 'A', 10, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (2, 'B', 20, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (3, 'C', 30, '2023-01-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_cont_config` (inv_cont_config_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (10, 100, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (20, 200, '2023-01-01', NULL, '2023-01-01', NULL, 1),
        (30, 300, '2023-01-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_care_description` (cds_description_id, cds_description) VALUES
        (100, 'Desc1'), (200, 'Desc2'), (300, 'Desc3');
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('{test_job_kennung}', PARSE_TIMESTAMP('%Y%m%d', '{test_v_datum}'));
    """).result()

    # Assume legacy job would produce 3 rows for this setup
    expected_row_count_oracle = 3 # This would come from actual Oracle run

    # Action: Trigger the Airflow DAG.
    airflow_conf = {
        "job_kennung": test_job_kennung,
        "default_v_datum": "19000101"
    }
    trigger_airflow_dag(AIRFLOW_DAG_ID, airflow_conf)

    # Action: Get row count from BigQuery target table.
    bq_row_count = get_bigquery_row_count(bq_client, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET)

    # Pass/Fail Criterion:
    assert bq_row_count == expected_row_count_oracle, \
        f"Row count parity failed. Expected {expected_row_count_oracle} rows, got {bq_row_count}."
    print("Row count parity test passed.")


@pytest.mark.data_quality
def test_schema_parity(bq_client):
    """
    Purpose: Verify that the schema (column names, data types, nullability) of
             `dwh.sof_ta_inv_def` in BigQuery matches the legacy `SOF$TA_INV_DEF` in Oracle.
    """
    # Setup:
    # Obtain the schema definition of SOF$TA_INV_DEF from Oracle.
    # This would typically be done by querying Oracle's data dictionary (e.g., ALL_TAB_COLUMNS).
    # For this example, we'll define a representative Oracle schema.
    oracle_schema = [
        {"name": "INV_DEFINITION_ID", "type": "NUMBER", "nullable": False},
        {"name": "ACC_REF_ID", "type": "VARCHAR2", "nullable": True},
        {"name": "INV_PAY_TY_CV", "type": "VARCHAR2", "nullable": True},
        {"name": "INV_MEDIA_CV", "type": "VARCHAR2", "nullable": True},
        {"name": "BILLCYCLE_ID", "type": "NUMBER", "nullable": True},
        {"name": "SALES_TAX_FREED", "type": "NUMBER", "nullable": True},
        {"name": "INV_CONT_CONFIG_ID", "type": "NUMBER", "nullable": True},
        {"name": "RECHN_INH_KONFIG_TEXT", "type": "VARCHAR2", "nullable": True},
    ]

    # Action: Get schema from BigQuery target table.
    bq_schema = get_bigquery_table_schema(bq_client, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET)

    # Pass/Fail Criterion:
    # Convert BigQuery schema to a comparable format and check.
    # BigQuery types are not 1:1 with Oracle, but should be functionally equivalent.
    # Nullability in BigQuery is often 'NULLABLE' by default unless explicitly 'REQUIRED'.
    bq_schema_map = {col['name'].upper(): col for col in bq_schema}

    errors = []
    for o_col in oracle_schema:
        bq_col = bq_schema_map.get(o_col['name'].upper())
        if not bq_col:
            errors.append(f"Column '{o_col['name']}' missing in BigQuery.")
            continue

        # Check data type equivalence
        type_mapping = {
            "NUMBER": ["INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64"],
            "VARCHAR2": ["STRING"],
            "DATE": ["DATE"],
            "TIMESTAMP": ["TIMESTAMP"],
        }
        oracle_type_upper = o_col['type'].upper()
        if oracle_type_upper in type_mapping and bq_col['field_type'] not in type_mapping[oracle_type_upper]:
            errors.append(f"Type mismatch for column '{o_col['name']}': Oracle '{o_col['type']}', BigQuery '{bq_col['field_type']}'.")
        elif oracle_type_upper not in type_mapping:
             print(f"Warning: No explicit type mapping for Oracle type '{oracle_type_upper}'. Manual check needed for '{o_col['name']}'.")

        # Check nullability (BigQuery 'REQUIRED' is equivalent to Oracle 'NOT NULL')
        if not o_col['nullable'] and bq_col['mode'] != 'REQUIRED':
            errors.append(f"Nullability mismatch for column '{o_col['name']}': Oracle 'NOT NULL', BigQuery '{bq_col['mode']}'.")
        elif o_col['nullable'] and bq_col['mode'] == 'REQUIRED':
             errors.append(f"Nullability mismatch for column '{o_col['name']}': Oracle 'NULLABLE', BigQuery '{bq_col['mode']}'.")

    assert not errors, f"Schema parity test failed with errors:\n" + "\n".join(errors)
    print("Schema parity test passed.")


@pytest.mark.data_quality
def test_dataform_assertions(bq_client, clean_bigquery_target_table):
    """
    Purpose: Verify basic data quality constraints on the output table using Dataform assertions.
    """
    # Setup:
    # Populate staging tables with data that should pass the assertions.
    # For 'sof_ta_inv_def_not_empty', ensure at least one row is generated.
    test_job_kennung = "BERT_V_TA_INV_DEF_ASSERTION_TEST"
    test_v_datum = "20230401"

    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_definition` (inv_definition_id, acc_ref_id, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (1, 'A', 10, '2023-03-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_cont_config` (inv_cont_config_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (10, 100, '2023-03-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_care_description` (cds_description_id, cds_description) VALUES
        (100, 'Desc1');
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('{test_job_kennung}', PARSE_TIMESTAMP('%Y%m%d', '{test_v_datum}'));
    """).result()

    # Action: Trigger the Airflow DAG, which includes Dataform assertions.
    airflow_conf = {
        "job_kennung": test_job_kennung,
        "default_v_datum": "19000101"
    }
    trigger_airflow_dag(AIRFLOW_DAG_ID, airflow_conf)

    # Action: Check Dataform assertion results.
    # Dataform assertions create tables in the assertion schema. If a test fails,
    # the table will contain rows. If it passes, it will be empty.
    assertion_table_id = f"{GCP_PROJECT_ID}.dataform_assertions.sof_ta_inv_def_not_empty"
    assertion_failures = get_bigquery_row_count(bq_client, "dataform_assertions", "sof_ta_inv_def_not_empty")

    # Pass/Fail Criterion:
    assert assertion_failures == 0, \
        f"Dataform assertion 'sof_ta_inv_def_not_empty' failed. Found {assertion_failures} failing rows."
    print("Dataform assertion 'sof_ta_inv_def_not_empty' passed.")

    # Example of adding more assertions (e.g., uniqueness)
    # You would add more .sqlx files in Dataform for these.
    # For example, an assertion for unique inv_definition_id:
    # SELECT inv_definition_id FROM ${ref("dwh", "sof_ta_inv_def")} GROUP BY 1 HAVING COUNT(*) > 1
    # Then check its corresponding assertion table.


@pytest.mark.idempotency
def test_idempotency_full_refresh(bq_client, clean_bigquery_target_table):
    """
    Purpose: Verify that running the job multiple times with the same inputs
             produces the same output, confirming the "truncate and load" behavior.
    """
    # Setup:
    test_job_kennung = "BERT_V_TA_INV_DEF_IDEMPOTENCY_TEST"
    test_v_datum = "20230501"

    # Populate staging tables with a known dataset
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_definition` (inv_definition_id, acc_ref_id, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (1, 'X', 10, '2023-04-01', NULL, '2023-01-01', NULL, 1),
        (2, 'Y', 20, '2023-04-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_inv_cont_config` (inv_cont_config_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
        (10, 100, '2023-04-01', NULL, '2023-01-01', NULL, 1),
        (20, 200, '2023-04-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.cds_ta_care_description` (cds_description_id, cds_description) VALUES
        (100, 'DescX'), (200, 'DescY');
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STG}.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('{test_job_kennung}', PARSE_TIMESTAMP('%Y%m%d', '{test_v_datum}'));
    """).result()

    airflow_conf = {
        "job_kennung": test_job_kennung,
        "default_v_datum": "19000101"
    }

    # Action 1: Run the DAG for the first time.
    print("Running DAG for the first time...")
    trigger_airflow_dag(AIRFLOW_DAG_ID, airflow_conf)
    first_run_data = get_bigquery_table_data(bq_client, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET)
    df_first_run = pd.DataFrame(first_run_data)
    print(f"First run produced {len(df_first_run)} rows.")

    # Action 2: Run the DAG for the second time without changing inputs.
    print("Running DAG for the second time...")
    trigger_airflow_dag(AIRFLOW_DAG_ID, airflow_conf)
    second_run_data = get_bigquery_table_data(bq_client, BIGQUERY_DATASET_DWH, BIGQUERY_TABLE_TARGET)
    df_second_run = pd.DataFrame(second_run_data)
    print(f"Second run produced {len(df_second_run)} rows.")

    # Pass/Fail Criterion:
    is_identical, message = compare_dataframes(df_first_run, df_second_run, sort_cols=['inv_definition_id'])
    assert is_identical, f"Idempotency test failed: {message}"
    print("Idempotency test passed: Multiple runs produce identical results.")

```