As a senior data-migration QA engineer, I've reviewed the migration design and the generated Airflow DAG for `r_ausd_adressen.ksh`. The following test cases are designed to ensure behavioral equivalence, data integrity, and correctness of the migrated ETL job.

A critical observation from the migration design and generated code is the explicit non-implementation of the `p_wiederanlaufWert` logic. This is a significant behavioral difference from the legacy script and will be highlighted in a dedicated test case.

## Test Environment Setup

Before executing these tests, the following setup is required:

1.  **Golden Source Environment:**
    *   An Oracle database instance containing the original source tables (`cds$ta_bp_ref`, `glv$ta_country`, `bpd$ta_reachability`, `bpd$ta_business_partner`, `isbert_schema.dwtk_meldungen`, `cds$ta_inv_definition`).
    *   The original `r_ausd_adressen.ksh` script and its dependencies (`k_ausd_adressen.ksh`, `d_ausd_adressen.sql`, utility scripts) must be runnable in this environment.
    *   A dedicated schema/database (e.g., `ORACLE_REFERENCE_DB`) to store the output of the legacy job for comparison.

2.  **Target BigQuery Environment:**
    *   A GCP project with BigQuery enabled.
    *   BigQuery datasets created: `staging`, `temp_address_processing`, `reporting_address_data`.
    *   The Airflow DAG `r_ausd_adressen_ksh_to_bq_dag` deployed to a Cloud Composer environment.
    *   BigQuery staging tables (e.g., `staging.stg_cds_bp_ref`, `staging.stg_glv_country`) populated with data *identical* to the Oracle source tables at the time of the golden source run. This is crucial for output parity.

3.  **Test Data:**
    *   **Baseline Dataset:** A comprehensive dataset in the Oracle source tables covering:
        *   Records with various combinations of `insert_at`, `modified_at`, `valid_from`, `valid_to` (active, historical, future).
        *   Records with `is_production = 0` and `1`.
        *   All `bp_ref_ty`, `address_ref_ty`, `rdndant_invrec`, `mop_ref_ty` values used in `WHERE` clauses.
        *   NULL values in relevant date/timestamp columns (`modified_at`, `valid_to`).
        *   Countries with and without descriptions.
        *   Business partners with and without organization names, titles, etc.
        *   `short_description` values of varying lengths (including < 3 chars).
        *   Data that would be affected by the `p_wiederanlaufWert` filter (e.g., `DWH_VERTRAG_ID` values both above and below a sample `wiederanlaufWert`).
    *   **Edge Case Datasets:**
        *   Empty source tables.
        *   Source tables where all records are filtered out by `WHERE` clauses.
        *   Source tables where all records are included.
        *   Dates at boundaries (e.g., `stichtag` equals `insert_at`, `modified_at`, `valid_from`, `valid_to`).

4.  **Comparison Tooling:**
    *   A Python environment with `pytest`, `pandas`, `google-cloud-bigquery` libraries.
    *   SQL client for Oracle to extract golden source data.

---

## Migration Validation Tests

### Test Case 1: Full Output Parity - Standard Run

**Purpose:** To verify that the migrated Airflow DAG produces identical data in the final BigQuery tables as the legacy KornShell job running `d_ausd_adressen.sql` in Oracle, given the same inputs. This covers overall transformation correctness, joins, filters, and data types.

**Setup:**
1.  Populate the Oracle source tables with the **Baseline Dataset**.
2.  Populate the BigQuery staging tables (`staging.*`) with data *identical* to the Oracle source tables.
3.  Choose a `stichtag` (e.g., `20230101`) and a `wiederanlaufWert` (e.g., `0` to include all records, or a specific value if the legacy script's behavior for this parameter is to be fully replicated for comparison, though the DAG explicitly ignores it).

**Action:**
1.  **Run Legacy Job:** Execute `r_ausd_adressen.ksh -s 20230101 -l 0` in the Oracle environment.
    *   Capture the final output from the Oracle `sof$ta_e_*` tables into flat files or a dedicated `ORACLE_REFERENCE_DB` schema.
2.  **Run Migrated Job:** Trigger the `r_ausd_adressen_ksh_to_bq_dag` in Airflow with `logical_date = '2023-01-01'` (which will set `stichtag_yyyymmdd` to `20230101`).
    *   Ensure the `wiederanlaufWert` parameter is handled as per the DAG's current implementation (i.e., ignored in the BigQuery SQL).

**Pass/Fail Criterion:**
*   For each final output table (`sof_ta_e_reach_gp`, `sof_ta_e_reach_re`, `sof_ta_e_reach_dn`, `sof_ta_e_reach_ev`, `sof_ta_e_business_gp`, `sof_ta_e_business_re`, `sof_ta_e_business_ev`, `sof_ta_e_business_dn`, `sof_ta_e_regulierer`):
    *   The row count in the BigQuery target table must be identical to the row count in the corresponding Oracle reference table.
    *   A full data comparison (all columns, all rows, order-agnostic) between the BigQuery target table and the Oracle reference table must show no differences.

**Runnable Test Code (Pytest / SQL Assertions):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from pandas.testing import assert_frame_equal
import os

# Configuration
PROJECT_ID = 'your-gcp-project-id'
TARGET_DATASET = 'reporting_address_data'
ORACLE_REFERENCE_SCHEMA = 'ORACLE_REFERENCE_DB' # Assuming Oracle output is in a separate schema/database
STAGING_DATASET = 'staging'
TEMP_DATASET = 'temp_address_processing'

# Define the final output tables to compare
FINAL_TABLES = [
    'sof_ta_e_reach_gp', 'sof_ta_e_reach_re', 'sof_ta_e_reach_dn', 'sof_ta_e_reach_ev',
    'sof_ta_e_business_gp', 'sof_ta_e_business_re', 'sof_ta_e_business_ev', 'sof_ta_e_business_dn',
    'sof_ta_e_regulierer'
]

# Initialize BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)

# Function to fetch data from BigQuery
def fetch_bq_data(table_name, dataset_id):
    query = f"SELECT * FROM `{PROJECT_ID}.{dataset_id}.{table_name}` ORDER BY 1, 2" # Order for consistent comparison
    df = bq_client.query(query).to_dataframe()
    return df

# Function to fetch data from Oracle (placeholder - actual implementation depends on Oracle client/connection)
def fetch_oracle_data(table_name, oracle_schema):
    # This is a placeholder. In a real scenario, you'd use cx_Oracle or similar.
    # For testing, assume you have a way to extract Oracle data into a pandas DataFrame.
    # Example:
    # import cx_Oracle
    # conn = cx_Oracle.connect("user/password@host:port/service_name")
    # query = f"SELECT * FROM {oracle_schema}.{table_name} ORDER BY 1, 2"
    # df = pd.read_sql(query, conn)
    # conn.close()
    # For now, let's assume data is loaded from a CSV or similar for comparison
    # In a real test, this would connect to the Oracle DB and fetch.
    print(f"Fetching data from Oracle reference for {oracle_schema}.{table_name}")
    # Placeholder: Load from a pre-generated CSV for demonstration
    # You would replace this with actual Oracle DB connection and query
    try:
        df = pd.read_csv(f"oracle_reference_data/{table_name}.csv")
        # Ensure column names match BigQuery (e.g., uppercase, no special chars)
        df.columns = [col.upper().replace('$', '_') for col in df.columns]
        return df
    except FileNotFoundError:
        pytest.fail(f"Oracle reference data file not found for {table_name}.csv. "
                    "Ensure Oracle output is extracted and available.")


@pytest.mark.parametrize("table_name", FINAL_TABLES)
def test_output_parity(table_name):
    """
    Compares row counts and full data content between Oracle reference and BigQuery target tables.
    """
    print(f"\n--- Testing table: {table_name} ---")

    # Fetch data from BigQuery
    bq_df = fetch_bq_data(table_name, TARGET_DATASET)
    print(f"BigQuery '{table_name}' row count: {len(bq_df)}")

    # Fetch data from Oracle reference
    oracle_df = fetch_oracle_data(table_name, ORACLE_REFERENCE_SCHEMA)
    print(f"Oracle '{table_name}' row count: {len(oracle_df)}")

    # 1. Row Count Check
    assert len(bq_df) == len(oracle_df), \
        f"Row count mismatch for table {table_name}: BigQuery has {len(bq_df)} rows, Oracle has {len(oracle_df)} rows."

    # 2. Data Content Check (order-agnostic)
    # Sort both dataframes by all columns to ensure order-agnostic comparison
    # Handle potential type differences (e.g., BigQuery INT64 vs Oracle NUMBER)
    # Convert all columns to string for robust comparison if types are slightly different but values are same
    # Or, more robustly, ensure schema alignment and then use assert_frame_equal with check_dtype=False
    
    # For robust comparison, ensure column order and types are aligned.
    # A common strategy is to cast all columns to string for comparison if types might differ but values should be identical.
    # Or, explicitly cast to common types.
    
    # Let's assume schema alignment is handled in a separate test (Test Case 4)
    # For data comparison, we'll sort and then compare.
    
    # Ensure column names are identical before sorting
    assert set(bq_df.columns) == set(oracle_df.columns), \
        f"Column mismatch for table {table_name}. BQ columns: {bq_df.columns.tolist()}, Oracle columns: {oracle_df.columns.tolist()}"
    
    # Reorder columns to match for comparison
    oracle_df = oracle_df[bq_df.columns]

    # Sort both dataframes by all columns for an order-agnostic comparison
    bq_df_sorted = bq_df.sort_values(by=list(bq_df.columns)).reset_index(drop=True)
    oracle_df_sorted = oracle_df.sort_values(by=list(oracle_df.columns)).reset_index(drop=True)

    try:
        assert_frame_equal(bq_df_sorted, oracle_df_sorted, check_dtype=False, check_exact=False, rtol=1e-9)
        print(f"Data content for table {table_name} is identical.")
    except AssertionError as e:
        pytest.fail(f"Data content mismatch for table {table_name}: {e}")

# To run this test:
# 1. Ensure Oracle reference data is extracted (e.g., into oracle_reference_data/sof_ta_e_reach_gp.csv, etc.)
# 2. Set PROJECT_ID correctly.
# 3. Run `pytest your_test_file.py`
```

### Test Case 2: Transformation Correctness - Specific Logic

**Purpose:** To verify the correct translation and execution of key transformation logic elements, including `WHERE` clauses, `UNION ALL`, `JOIN` conditions, and string functions.

**Setup:**
1.  Populate Oracle source tables and BigQuery staging tables with the **Baseline Dataset**, ensuring specific data points exist to test each condition (e.g., records with `bp_ref_ty=4`, `address_ref_ty=6`, records with `rdndant_invrec=0`, records with `short_description` < 3 chars, records with `valid_to` NULL, etc.).
2.  Choose a `stichtag` (e.g., `20230101`).

**Action:**
1.  **Run Legacy Job:** Execute `r_ausd_adressen.ksh -s 20230101 -l 0` and capture intermediate and final results.
2.  **Run Migrated Job:** Trigger `r_ausd_adressen_ksh_to_bq_dag` with `logical_date = '2023-01-01'`.

**Pass/Fail Criterion:**

*   **2.1 `WHERE` Clause Logic (e.g., `stg_cds_bp_ref` filters):**
    *   **Purpose:** Verify `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`, `bp_ref_ty`, `address_ref_ty` filters are correctly applied.
    *   **Criterion:** The row counts and data content of `temp_address_processing.sof_ta_bp_ref_gp` (and other `sof_ta_bp_ref_*` tables) in BigQuery must exactly match the corresponding Oracle intermediate tables after applying these filters.
    *   **SQL Assertion Example (for `sof_ta_bp_ref_gp`):**
        ```sql
        -- BigQuery
        SELECT COUNT(*) FROM `your-gcp-project-id.temp_address_processing.sof_ta_bp_ref_gp`;
        -- Oracle (run against intermediate table after step 2a)
        SELECT COUNT(*) FROM ORACLE_REFERENCE_DB.sof$ta_bp_ref_gp;
        ```

*   **2.2 `UNION ALL` Logic (e.g., `sof_ta_bp_ref_re`):**
    *   **Purpose:** Verify the `UNION ALL` operation correctly combines data from `stg_cds_bp_ref` and `stg_cds_inv_definition` for invoice recipients.
    *   **Criterion:** The row count and data content of `temp_address_processing.sof_ta_bp_ref_re` in BigQuery must exactly match the corresponding Oracle intermediate table.
    *   **SQL Assertion Example:** Compare `SELECT * FROM ...sof_ta_bp_ref_re` from both systems.

*   **2.3 `JOIN` Conditions (e.g., `sof_ta_e_reach_gp`):**
    *   **Purpose:** Verify joins between `sof_ta_bp_ref_gp`, `sof_ta_reachability`, and `sof_ta_laender_kng` are correct.
    *   **Criterion:** The row count and data content of `reporting_address_data.sof_ta_e_reach_gp` (and other `sof_ta_e_reach_*` tables) in BigQuery must exactly match the corresponding Oracle reference table.
    *   **SQL Assertion Example:** Compare `SELECT * FROM ...sof_ta_e_reach_gp` from both systems.

*   **2.4 `SUBSTR` Function (e.g., `LAND_SD` column):**
    *   **Purpose:** Verify `SUBSTR(lk.short_description, 1, 3)` is correctly translated and applied.
    *   **Criterion:** For records in `reporting_address_data.sof_ta_e_reach_gp` (and other `sof_ta_e_reach_*` tables), the `LAND_SD` column in BigQuery must match the Oracle reference. Specifically test cases where `short_description` is < 3 characters (should return the full string) and >= 3 characters (should return the first 3).
    *   **SQL Assertion Example:**
        ```sql
        -- BigQuery
        SELECT BP_ID, LAND_SD FROM `your-gcp-project-id.reporting_address_data.sof_ta_e_reach_gp` WHERE COUNTRY_CODE = 'XYZ';
        -- Oracle
        SELECT BP_ID, LAND_SD FROM ORACLE_REFERENCE_DB.sof$ta_e_reach_gp WHERE COUNTRY_CODE = 'XYZ';
        ```

*   **2.5 `DISTINCT` Logic (e.g., `sof_ta_bp_ref_gp_nodp`):**
    *   **Purpose:** Verify `SELECT DISTINCT bp_id` correctly identifies unique business partners.
    *   **Criterion:** The row count and data content of `temp_address_processing.sof_ta_bp_ref_gp_nodp` (and other `_nodp` tables) in BigQuery must exactly match the corresponding Oracle intermediate table.
    *   **SQL Assertion Example:** Compare `SELECT * FROM ...sof_ta_bp_ref_gp_nodp` from both systems.

### Test Case 3: External System Replacements - Date Derivation

**Purpose:** To verify that the Airflow DAG's `_get_processing_dates` Python function correctly replaces the Oracle `isbert_schema.dwtk_meldungen` logic for `v_datum` (stichtag) and the `h_alis_date.ksh` / `gestern.ksh` logic for `today` and `yesterday`.

**Setup:**
1.  Ensure the Oracle `isbert_schema.dwtk_meldungen` table contains specific `timecreated` values for `job_kennung = 'BERT_DROP_TEMP_TABLE'` that would result in a particular `v_datum`.
2.  Set the Airflow DAG's `logical_date` to a value that would correspond to the `v_sysdate` in the legacy script.

**Action:**
1.  **Run Legacy Job:** Execute `r_ausd_adressen.ksh` without the `-s` parameter.
    *   Log the `Stichtag` and `v_sysdate` values printed by the script.
2.  **Run Migrated Job:** Trigger the `r_ausd_adressen_ksh_to_bq_dag` with a specific `logical_date` (e.g., `2023-01-01`).
    *   Inspect the Airflow task logs for `get_processing_dates` to see the `stichtag_yyyymmdd`, `today_yyyymmdd`, `yesterday_yyyymmdd` values.
    *   Alternatively, pull these values from XComs.

**Pass/Fail Criterion:**
*   The `stichtag_yyyymmdd` value derived by the Airflow DAG must match the `Stichtag` value determined by the legacy script (which uses `MIN(sysdate,maxladedatum)` logic, where `maxladedatum` comes from `dwtk_meldungen`).
*   The `today_yyyymmdd` and `yesterday_yyyymmdd` values derived by the Airflow DAG must correctly correspond to the `logical_date` and its preceding day.

**Runnable Test Code (Pytest / Airflow XComs):**

```python
import pytest
from airflow.models import DagBag
from airflow.utils.state import State
import pendulum

# Configuration
DAG_ID = 'r_ausd_adressen_ksh_to_bq_dag'

def test_date_derivation_logic():
    """
    Verifies that the _get_processing_dates PythonOperator correctly derives dates.
    This test assumes a specific logical_date and compares against expected values.
    For a full comparison, you'd need to mock or run the Oracle logic to get the expected stichtag.
    """
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None, f"DAG {DAG_ID} not found."

    # Simulate a logical_date for testing
    test_logical_date_str = '2023-01-15'
    test_logical_date = pendulum.parse(test_logical_date_str)

    # Create a DagRun and TaskInstance for the 'get_processing_dates' task
    dr = dag.create_dagrun(
        run_id=f"test_run_{test_logical_date_str}",
        start_date=test_logical_date,
        execution_date=test_logical_date,
        state=State.RUNNING,
        conf={}
    )
    ti = dr.get_task_instance(task_id='get_processing_dates')
    ti.run(ignore_ti_state=True, test_mode=True) # Run the task in test mode

    # Pull values from XComs
    stichtag_yyyymmdd = ti.xcom_pull(task_ids='get_processing_dates', key='stichtag_yyyymmdd')
    today_yyyymmdd = ti.xcom_pull(task_ids='get_processing_dates', key='today_yyyymmdd')
    yesterday_yyyymmdd = ti.xcom_pull(task_ids='get_processing_dates', key='yesterday_yyyymmdd')

    print(f"XComs: Stichtag={stichtag_yyyymmdd}, Today={today_yyyymmdd}, Yesterday={yesterday_yyyymmdd}")

    # Assertions based on the logical_date
    # For 'stichtag_yyyymmdd', the current DAG implementation uses logical_date directly.
    # If the original 'dwtk_meldungen' logic were more complex, this would need a more sophisticated mock/comparison.
    expected_stichtag = test_logical_date.strftime('%Y%m%d')
    expected_today = test_logical_date.strftime('%Y%m%d')
    expected_yesterday = test_logical_date.subtract(days=1).strftime('%Y%m%d')

    assert stichtag_yyyymmdd == expected_stichtag, \
        f"Stichtag mismatch: Expected {expected_stichtag}, got {stichtag_yyyymmdd}"
    assert today_yyyymmdd == expected_today, \
        f"Today's date mismatch: Expected {expected_today}, got {today_yyyymmdd}"
    assert yesterday_yyyymmdd == expected_yesterday, \
        f"Yesterday's date mismatch: Expected {expected_yesterday}, got {yesterday_yyyymmdd}"

    print("Date derivation logic verified successfully.")

# To run this test:
# 1. Ensure your DAGs are in a 'dags' folder relative to your test file.
# 2. Run `pytest your_test_file.py`
```

### Test Case 4: Data Quality - Schema and Nullability

**Purpose:** To verify that the schema (column names, data types, nullability) of the BigQuery target tables matches the Oracle reference tables, and that data quality constraints (e.g., non-nullable columns) are preserved.

**Setup:**
1.  Ensure Oracle reference tables are available.
2.  Run the migrated Airflow DAG to populate BigQuery target tables.

**Action:**
1.  Extract schema information (column name, data type, nullability) for all final output tables from both Oracle and BigQuery.
2.  Compare the schemas.

**Pass/Fail Criterion:**
*   For each final output table:
    *   All column names must match (case-insensitivity might be needed depending on Oracle setup, but BigQuery is case-sensitive for column names).
    *   Data types must be functionally equivalent (e.g., Oracle `NUMBER` to BigQuery `INT64` or `NUMERIC`, Oracle `VARCHAR2` to BigQuery `STRING`, Oracle `DATE` to BigQuery `DATE`).
    *   Nullability constraints must match. If a column is `NOT NULL` in Oracle, it should be `NOT NULL` in BigQuery.

**Runnable Test Code (Pytest / SQL Assertions):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd

# Configuration (same as Test Case 1)
PROJECT_ID = 'your-gcp-project-id'
TARGET_DATASET = 'reporting_address_data'
ORACLE_REFERENCE_SCHEMA = 'ORACLE_REFERENCE_DB'

# Define the final output tables to compare
FINAL_TABLES = [
    'sof_ta_e_reach_gp', 'sof_ta_e_reach_re', 'sof_ta_e_reach_dn', 'sof_ta_e_reach_ev',
    'sof_ta_e_business_gp', 'sof_ta_e_business_re', 'sof_ta_e_business_ev', 'sof_ta_e_business_dn',
    'sof_ta_e_regulierer'
]

bq_client = bigquery.Client(project=PROJECT_ID)

def get_bq_schema(table_name, dataset_id):
    table_ref = bq_client.dataset(dataset_id).table(table_name)
    table = bq_client.get_table(table_ref)
    schema_info = []
    for field in table.schema:
        schema_info.append({
            'name': field.name.upper(), # Normalize to uppercase for comparison
            'type': field.field_type,
            'mode': field.mode # NULLABLE, REQUIRED
        })
    return pd.DataFrame(schema_info).set_index('name').sort_index()

def get_oracle_schema(table_name, oracle_schema):
    # Placeholder for Oracle schema extraction.
    # In a real scenario, you'd query ALL_TAB_COLUMNS or USER_TAB_COLUMNS.
    # Example:
    # conn = cx_Oracle.connect("user/password@host:port/service_name")
    # query = f"""
    #     SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
    #     FROM ALL_TAB_COLUMNS
    #     WHERE OWNER = '{oracle_schema.upper()}' AND TABLE_NAME = '{table_name.upper()}'
    #     ORDER BY COLUMN_ID
    # """
    # df = pd.read_sql(query, conn)
    # conn.close()
    # df['NULLABLE'] = df['NULLABLE'].apply(lambda x: 'NULLABLE' if x == 'Y' else 'REQUIRED')
    # df.rename(columns={'DATA_TYPE': 'type', 'NULLABLE': 'mode'}, inplace=True)
    # df.set_index('COLUMN_NAME', inplace=True)
    # return df.sort_index()

    # For demonstration, assume a pre-defined schema mapping or loaded from CSV
    print(f"Fetching Oracle schema for {oracle_schema}.{table_name}")
    # This would be replaced by actual Oracle DB connection and schema query
    # Example mock schema for 'sof_ta_e_reach_gp'
    if table_name == 'sof_ta_e_reach_gp':
        return pd.DataFrame([
            {'name': 'BP_ID', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'REACHABILITY_ID', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'OBJ_VERSION', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'COUNTRY_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FOR_THE_ATTENTION_OF', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ADDRESS_ATTACHMENT', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ADDRESS_ATTACHMENT_ORG', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CORP_UNIT', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'SURNAME_S', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FIRST_NAME_G', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ZIP_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CITY', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'POBOX', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'STREET', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'HOUSE_NR', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PUBLIC_AREA_A', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PRIVATE_AREA_P', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CORP_UNIT_OU1', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ADDRESS_LINE_1', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ADDRESS_LINE_2', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'REACHABLE_FROM', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'REACHABLE_THRU', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'CNTRCT_CP2_ID', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'INV_DEF_INVREC_ID', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'BPR_INST_EVNREC_ID', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'BPR_INST_SRVUSR_ID', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'LAND_SD', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]).set_index('name').sort_index()
    # Add similar mock schemas for other tables
    return pd.DataFrame() # Return empty for other tables if not mocked

@pytest.mark.parametrize("table_name", FINAL_TABLES)
def test_schema_parity(table_name):
    """
    Compares schema (column names, types, nullability) between Oracle reference and BigQuery target tables.
    """
    print(f"\n--- Testing schema for table: {table_name} ---")

    bq_schema = get_bq_schema(table_name, TARGET_DATASET)
    oracle_schema = get_oracle_schema(table_name, ORACLE_REFERENCE_SCHEMA)

    assert not bq_schema.empty, f"BigQuery schema for {table_name} is empty or table does not exist."
    assert not oracle_schema.empty, f"Oracle schema for {table_name} is empty or table does not exist (mock data missing?)."

    # Compare column names
    assert set(bq_schema.index) == set(oracle_schema.index), \
        f"Column name mismatch for table {table_name}. BQ columns: {bq_schema.index.tolist()}, Oracle columns: {oracle_schema.index.tolist()}"

    # Compare types and modes
    # Reorder Oracle schema to match BQ for direct comparison
    oracle_schema_reordered = oracle_schema.reindex(bq_schema.index)

    # Map Oracle types to BigQuery types for comparison if direct string match isn't expected
    type_mapping = {
        'NUMBER': 'INT64', # Or NUMERIC, FLOAT64 depending on precision
        'VARCHAR2': 'STRING',
        'DATE': 'DATE',
        # Add other mappings as needed
    }
    # Apply mapping to Oracle types for comparison
    # oracle_schema_reordered['type_mapped'] = oracle_schema_reordered['type'].apply(lambda x: type_mapping.get(x, x))

    # For simplicity, let's assume direct type names are comparable after normalization
    # If not, a custom comparison function for types would be needed.
    
    # Compare types and modes directly
    try:
        pd.testing.assert_frame_equal(bq_schema, oracle_schema_reordered, check_dtype=False) # check_dtype=False if types are functionally equivalent but string names differ
        print(f"Schema for table {table_name} is identical (column names, types, nullability).")
    except AssertionError as e:
        pytest.fail(f"Schema mismatch for table {table_name}: {e}")

```

### Test Case 5: `Wiederanlaufwert` (Restart Value) - Behavioral Difference

**Purpose:** To explicitly test and document the behavioral difference regarding the `p_wiederanlaufWert` parameter, which is implemented in the legacy script but explicitly *not* in the migrated Airflow DAG.

**Setup:**
1.  Populate Oracle source tables and BigQuery staging tables with data including `DWH_VERTRAG_ID` values both above and below a chosen `wiederanlaufWert` (e.g., `1000`).
2.  Ensure the `stichtag` is set (e.g., `20230101`).

**Action:**
1.  **Run Legacy Job (with `wiederanlaufWert`):** Execute `r_ausd_adressen.ksh -s 20230101 -l 1000` in the Oracle environment.
    *   Capture the final output from the Oracle `sof$ta_e_*` tables.
2.  **Run Legacy Job (without `wiederanlaufWert`):** Execute `r_ausd_adressen.ksh -s 20230101 -l 0` (or without `-l` parameter, assuming default `0`).
    *   Capture the final output from the Oracle `sof$ta_e_*` tables.
3.  **Run Migrated Job:** Trigger the `r_ausd_adressen_ksh_to_bq_dag` in Airflow with `logical_date = '2023-01-01'`.

**Pass/Fail Criterion:**
*   **Expected Failure (Behavioral Difference):** The row counts and data content of the BigQuery target tables (from the migrated DAG run) will **not** match the Oracle reference tables from the legacy run *with* `p_wiederanlaufWert` applied.
*   **Expected Pass (Current DAG behavior):** The row counts and data content of the BigQuery target tables (from the migrated DAG run) **will** match the Oracle reference tables from the legacy run *without* `p_wiederanlaufWert` (i.e., `p_wiederanlaufWert=0`).
*   **Documentation:** This test case serves to formally document this known behavioral difference and confirm that the DAG behaves as currently implemented (ignoring `p_wiederanlaufWert`). If this functionality is later added to the DAG, this test case would need to be updated to expect parity.

**Runnable Test Code (Pytest / SQL Assertions):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from pandas.testing import assert_frame_equal

# Configuration (same as Test Case 1)
PROJECT_ID = 'your-gcp-project-id'
TARGET_DATASET = 'reporting_address_data'
ORACLE_REFERENCE_SCHEMA = 'ORACLE_REFERENCE_DB'
FINAL_TABLES = [
    'sof_ta_e_reach_gp', 'sof_ta_e_reach_re', 'sof_ta_e_reach_dn', 'sof_ta_e_reach_ev',
    'sof_ta_e_business_gp', 'sof_ta_e_business_re', 'sof_ta_e_business_ev', 'sof_ta_e_business_dn',
    'sof_ta_e_regulierer'
]
bq_client = bigquery.Client(project=PROJECT_ID)

def fetch_bq_data(table_name, dataset_id):
    query = f"SELECT * FROM `{PROJECT_ID}.{dataset_id}.{table_name}` ORDER BY 1, 2"
    df = bq_client.query(query).to_dataframe()
    return df

def fetch_oracle_data_with_wiederanlaufwert(table_name, oracle_schema, wiederanlaufwert):
    # Placeholder: Load from a pre-generated CSV for demonstration, specific to a wiederanlaufwert run
    # In a real test, this would connect to the Oracle DB and query after running the ksh with -l parameter.
    print(f"Fetching Oracle reference data for {table_name} with wiederanlaufwert={wiederanlaufwert}")
    try:
        df = pd.read_csv(f"oracle_reference_data/{table_name}_wiederanlauf_{wiederanlaufwert}.csv")
        df.columns = [col.upper().replace('$', '_') for col in df.columns]
        return df
    except FileNotFoundError:
        pytest.fail(f"Oracle reference data file not found for {table_name}_wiederanlauf_{wiederanlaufwert}.csv. "
                    "Ensure Oracle output is extracted and available for this scenario.")

def fetch_oracle_data_without_wiederanlaufwert(table_name, oracle_schema):
    # This would be the same as fetch_oracle_data from Test Case 1, representing -l 0 or no -l
    print(f"Fetching Oracle reference data for {table_name} without wiederanlaufwert (default 0)")
    try:
        df = pd.read_csv(f"oracle_reference_data/{table_name}.csv") # Assuming default reference is for -l 0
        df.columns = [col.upper().replace('$', '_') for col in df.columns]
        return df
    except FileNotFoundError:
        pytest.fail(f"Oracle reference data file not found for {table_name}.csv. "
                    "Ensure Oracle output is extracted and available for default scenario.")


@pytest.mark.parametrize("table_name", FINAL_TABLES)
def test_wiederanlaufwert_behavioral_difference(table_name):
    """
    Tests the known behavioral difference where the migrated DAG does not implement
    the 'wiederanlaufWert' filter, comparing its output to Oracle runs both with and without the filter.
    """
    print(f"\n--- Testing wiederanlaufWert behavior for table: {table_name} ---")

    # Define a sample wiederanlaufwert for which Oracle data is prepared
    sample_wiederanlaufwert = 1000

    # 1. Fetch BigQuery output (migrated DAG's behavior - no wiederanlaufWert filter)
    bq_df = fetch_bq_data(table_name, TARGET_DATASET)
    print(f"BigQuery '{table_name}' row count (no wiederanlaufWert filter): {len(bq_df)}")

    # 2. Fetch Oracle output with wiederanlaufWert applied (e.g., -l 1000)
    oracle_df_with_filter = fetch_oracle_data_with_wiederanlaufwert(table_name, ORACLE_REFERENCE_SCHEMA, sample_wiederanlaufwert)
    print(f"Oracle '{table_name}' row count (with -l {sample_wiederanlaufwert}): {len(oracle_df_with_filter)}")

    # 3. Fetch Oracle output without wiederanlaufWert (e.g., -l 0 or default)
    oracle_df_without_filter = fetch_oracle_data_without_wiederanlaufwert(table_name, ORACLE_REFERENCE_SCHEMA)
    print(f"Oracle '{table_name}' row count (without -l parameter): {len(oracle_df_without_filter)}")

    # Assertion 1: BigQuery output should NOT match Oracle output when filter is applied
    if len(bq_df) == len(oracle_df_with_filter):
        try:
            # Sort and compare to be sure
            bq_df_sorted = bq_df.sort_values(by=list(bq_df.columns)).reset_index(drop=True)
            oracle_df_with_filter_sorted = oracle_df_with_filter.sort_values(by=list(oracle_df_with_filter.columns)).reset_index(drop=True)
            assert_frame_equal(bq_df_sorted, oracle_df_with_filter_sorted, check_dtype=False, check_exact=False, rtol=1e-9)
            pytest.fail(f"ERROR: BigQuery output for {table_name} unexpectedly matched Oracle output with wiederanlaufWert={sample_wiederanlaufwert}. "
                        "The DAG is designed to ignore this parameter.")
        except AssertionError:
            print(f"SUCCESS: BigQuery output for {table_name} correctly did NOT match Oracle output with wiederanlaufWert={sample_wiederanlaufwert}.")
    else:
        print(f"SUCCESS: BigQuery output for {table_name} correctly did NOT match Oracle output with wiederanlaufWert={sample_wiederanlaufwert} (row count difference).")

    # Assertion 2: BigQuery output SHOULD match Oracle output when no filter is applied (default behavior)
    assert len(bq_df) == len(oracle_df_without_filter), \
        f"Row count mismatch for table {table_name} when comparing BigQuery (no filter) to Oracle (no filter): " \
        f"BigQuery has {len(bq_df)} rows, Oracle has {len(oracle_df_without_filter)} rows."

    try:
        bq_df_sorted = bq_df.sort_values(by=list(bq_df.columns)).reset_index(drop=True)
        oracle_df_without_filter_sorted = oracle_df_without_filter.sort_values(by=list(oracle_df_without_filter.columns)).reset_index(drop=True)
        assert_frame_equal(bq_df_sorted, oracle_df_without_filter_sorted, check_dtype=False, check_exact=False, rtol=1e-9)
        print(f"SUCCESS: BigQuery output for {table_name} matched Oracle output without wiederanlaufWert filter.")
    except AssertionError as e:
        pytest.fail(f"ERROR: BigQuery output for {table_name} did NOT match Oracle output without wiederanlaufWert filter: {e}")

```

### Test Case 6: Empty Source Tables

**Purpose:** To ensure the migrated DAG handles scenarios where source tables are empty gracefully, resulting in empty target tables without errors.

**Setup:**
1.  Ensure all Oracle source tables and BigQuery staging tables are empty.
2.  Choose a `stichtag` (e.g., `20230101`).

**Action:**
1.  **Run Legacy Job:** Execute `r_ausd_adressen.ksh -s 20230101 -l 0`.
    *   Verify that the job completes successfully and all final output tables are empty.
2.  **Run Migrated Job:** Trigger `r_ausd_adressen_ksh_to_bq_dag` with `logical_date = '2023-01-01'`.
    *   Verify that the DAG completes successfully.

**Pass/Fail Criterion:**
*   The Airflow DAG must complete successfully (status `success`).
*   All intermediate (`temp_address_processing.*`) and final (`reporting_address_data.*`) BigQuery tables must have a row count of 0.

**SQL Assertion Example:**
```sql
SELECT COUNT(*) FROM `your-gcp-project-id.reporting_address_data.sof_ta_e_reach_gp`; -- Should be 0
SELECT COUNT(*) FROM `your-gcp-project-id.reporting_address_data.sof_ta_e_regulierer`; -- Should be 0
-- Repeat for all final and intermediate tables
```

### Test Case 7: Error Handling and Logging

**Purpose:** To verify that the migrated Airflow DAG correctly handles errors during BigQuery SQL execution and logs them appropriately, replacing the `f_alis_msgerr.ksh` and `DWMSG_Fehlerbehandlung` logic.

**Setup:**
1.  Introduce a deliberate error in one of the BigQuery SQL scripts (e.g., a syntax error, a non-existent column reference, or a data type mismatch that would cause an insert to fail).
2.  Choose a `stichtag` (e.g., `20230101`).

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_to_bq_dag` with `logical_date = '2023-01-01'`.
2.  Monitor the Airflow UI and Cloud Logging for the DAG run.

**Pass/Fail Criterion:**
*   The Airflow task containing the erroneous SQL must fail.
*   The DAG run must be marked as `failed`.
*   Relevant error messages (e.g., BigQuery error details) must be visible in the task logs in Airflow and propagated to Cloud Logging.
*   No subsequent tasks in the DAG (after the failing task) should execute.

---

These test cases provide a comprehensive approach to validating the migration of `r_ausd_adressen.ksh` to BigQuery and Airflow, covering the specified requirements and highlighting critical behavioral differences.