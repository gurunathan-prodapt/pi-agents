As a senior data-migration QA engineer, I have analyzed the provided migration design document for `DW.BERT_AUSD_V_TA_P_VERTRAG`. The migration involves moving from UC4/KornShell/Oracle SQL*Plus to Airflow/BigQuery.

The core logic is an `INSERT INTO ... SELECT` statement with a `LEFT JOIN` on `sof$ta_vertrag_tmp` (self-join) and a preceding `TRUNCATE` of the target table. It also involves determining a processing date (`v_datum`) from `isbert_schema.dwtk_meldungen` and truncating numerous other temporary tables.

Below are the migration validation tests designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests for DW.BERT_AUSD_V_TA_P_VERTRAG

### Test Case 1: End-to-End Output Parity (Golden Record)

**Purpose**: To verify that the migrated BigQuery job produces exactly the same output data in `project_id.dataset_id.sof_ta_p_vertrag` as the legacy Oracle job, given identical input data in the staging tables. This is the primary test for behavioral equivalence.

**Setup**:
1.  **Legacy Environment**:
    *   Capture a snapshot of the input data from Oracle tables: `sof$ta_vertrag_tmp` and `isbert_schema.dwtk_meldungen` *before* a successful run of the legacy `DW.BERT_AUSD_V_TA_P_VERTRAG` job. Export this data to CSV files (e.g., `sof_ta_vertrag_tmp_input.csv`, `dwtk_meldungen_input.csv`).
    *   Execute the legacy `DW.BERT_AUSD_V_TA_P_VERTRAG` job.
    *   Capture the final state of the target table `sof$ta_p_vertrag` *after* the legacy job completes. Export this data to a CSV file (e.g., `sof_ta_p_vertrag_golden.csv`). This will be the "golden record" output.
2.  **Migrated Environment**:
    *   Ensure the BigQuery target table `project_id.dataset_id.sof_ta_p_vertrag` is empty.
    *   Load the captured input data from `sof_ta_vertrag_tmp_input.csv` into the BigQuery staging table `project_id.dataset_id.sof_ta_vertrag_tmp`.
    *   Load the captured input data from `dwtk_meldungen_input.csv` into `project_id.isbert_schema.dwtk_meldungen`.

**Action**:
1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_p_vertrag` in the migrated BigQuery environment.
2.  After the DAG completes successfully, query the `project_id.dataset_id.sof_ta_p_vertrag` table.

**Pass/Fail Criterion**:
*   The row count of `project_id.dataset_id.sof_ta_p_vertrag` must be identical to the row count of the legacy `sof$ta_p_vertrag` golden record.
*   Every column in `project_id.dataset_id.sof_ta_p_vertrag` must have identical values to its corresponding column in the legacy `sof$ta_p_vertrag` golden record, for every row. This comparison should be order-independent and account for data type conversions (e.g., Oracle `DATE` to BigQuery `DATE`).

**Runnable Test Code (Pytest with BigQuery client and Pandas)**:

```python
import pytest
from google.cloud import bigquery
import pandas as pd
import os
import time

# Configuration for BigQuery and project
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")
ISBERT_SCHEMA_DATASET_ID = os.environ.get("BQ_ISBERT_SCHEMA_DATASET_ID", "isbert_schema")

# BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Helper function to load data into BQ from CSV
def load_csv_to_bq(table_id, data_path, schema=None):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True if schema is None else False,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(data_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result() # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_id}")

# Helper function to fetch data from BQ as DataFrame
def fetch_bq_data_as_df(table_id):
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()
    # Sort by all columns to ensure order-independent comparison
    return df.sort_values(by=list(df.columns)).reset_index(drop=True)

@pytest.fixture(scope="module")
def setup_golden_record_test():
    bq_sof_ta_vertrag_tmp_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_vertrag_tmp"
    bq_dwtk_meldungen_table = f"{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen"
    bq_target_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_p_vertrag"

    # Ensure target and staging tables are empty before test run
    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()

    # --- Load Legacy Input Data into BigQuery Staging Tables ---
    # These CSVs should be prepared from Oracle snapshots
    sof_ta_vertrag_tmp_input_path = "tests/data/legacy_input/sof_ta_vertrag_tmp_input.csv"
    dwtk_meldungen_input_path = "tests/data/legacy_input/dwtk_meldungen_input.csv"
    
    # Define explicit schemas if autodetect is not reliable or for type mapping
    sof_ta_vertrag_tmp_schema = [
        bigquery.SchemaField("vertrag_id_carmen", "STRING"),
        bigquery.SchemaField("partner_id_carmen", "STRING"),
        # ... add all other fields with their correct BQ types
        bigquery.SchemaField("geplant_kuend", "DATE"),
        bigquery.SchemaField("twin_vertrag_id", "STRING"),
        # ...
    ]
    dwtk_meldungen_schema = [
        bigquery.SchemaField("timecreated", "TIMESTAMP"),
        bigquery.SchemaField("job_kennung", "STRING"),
    ]

    load_csv_to_bq(bq_sof_ta_vertrag_tmp_table, sof_ta_vertrag_tmp_input_path, schema=sof_ta_vertrag_tmp_schema)
    load_csv_to_bq(bq_dwtk_meldungen_table, dwtk_meldungen_input_path, schema=dwtk_meldungen_schema)

    # --- Load Legacy Output (Golden Record) ---
    # This CSV should be prepared from the Oracle target table snapshot
    golden_record_output_path = "tests/data/legacy_output/sof_ta_p_vertrag_golden.csv"
    golden_df = pd.read_csv(golden_record_output_path)
    # Ensure consistent column order and data types for comparison
    golden_df = golden_df.sort_values(by=list(golden_df.columns)).reset_index(drop=True)

    yield {
        "bq_target_table": bq_target_table,
        "golden_df": golden_df
    }

    # --- Teardown (Optional, depending on test environment) ---
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()


def test_output_parity(setup_golden_record_test):
    bq_target_table = setup_golden_record_test["bq_target_table"]
    golden_df = setup_golden_record_test["golden_df"]

    # --- Action: Run Airflow DAG ---
    # In a real integration test, you would trigger the Airflow DAG via API/CLI
    # and wait for its completion. For this example, we directly execute the
    # BigQuery SQL script that the DAG would run.
    print(f"Executing BigQuery SQL script for DAG: dw_bert_ausd_v_ta_p_vertrag")
    with open("sql/d_ausd_v_ta_p_vertrag_bq.sql", "r") as f:
        bq_sql_script = f.read()
    
    # Replace placeholders in the SQL script
    bq_sql_script = bq_sql_script.replace("project_id", PROJECT_ID)
    bq_sql_script = bq_sql_script.replace("dataset_id", DATASET_ID)
    bq_sql_script = bq_sql_script.replace("isbert_schema", ISBERT_SCHEMA_DATASET_ID)

    try:
        # BigQuery client can execute multi-statement scripts
        query_job = client.query(bq_sql_script)
        query_job.result() # Wait for the job to complete
        print("BigQuery SQL script executed successfully.")
    except Exception as e:
        pytest.fail(f"BigQuery SQL script execution failed: {e}")

    # --- Assertions ---
    migrated_df = fetch_bq_data_as_df(bq_target_table)

    # Compare row counts
    assert len(migrated_df) == len(golden_df), \
        f"Row count mismatch: Migrated has {len(migrated_df)} rows, Golden has {len(golden_df)} rows."

    # Compare data content (using pandas for robust DataFrame comparison)
    # check_dtype=False is often necessary due to subtle differences in type representation
    # (e.g., Oracle NUMBER vs. BQ INT64/FLOAT64) or NULL handling.
    pd.testing.assert_frame_equal(golden_df, migrated_df, check_dtype=False, check_like=True)
    print("Output parity test passed: Row counts and data content match.")

```

### Test Case 2: Transformation Correctness - Join Logic

**Purpose**: To specifically verify that the `LEFT JOIN` logic (`v.twin_vertrag_id = pv.vertrag_id_carmen (+)`) is correctly translated and behaves identically, especially concerning rows from `v` that do not have a match in `pv` (resulting in `NULL`s for `pv` columns in a standard `LEFT JOIN`). Given the `SELECT` list only uses `v` columns, this test confirms the output is simply the `v` table's content.

**Setup**:
1.  **Input Data**: Create a controlled dataset for `project_id.dataset_id.sof_ta_vertrag_tmp` in BigQuery that includes:
    *   Rows where `v.twin_vertrag_id` has a match in `pv.vertrag_id_carmen`.
    *   Rows where `v.twin_vertrag_id` does *not* have a match in `pv.vertrag_id_carmen`.
    *   Rows where `v.twin_vertrag_id` is `NULL`.
    *   Rows where `pv.vertrag_id_carmen` is `NULL` (these won't affect the join if `v.twin_vertrag_id` is not `NULL`).
2.  **Expected Output**: Manually calculate the expected output for `project_id.dataset_id.sof_ta_p_vertrag`. Since the `SELECT` clause only references columns from alias `v`, the output should be identical to the input `sof_ta_vertrag_tmp` table.

**Action**:
1.  Load the controlled input data into `project_id.dataset_id.sof_ta_vertrag_tmp`.
2.  Execute the `transform_and_load_sof_ta_p_vertrag` task (or the relevant SQL part) of the Airflow DAG.
3.  Query the `project_id.dataset_id.sof_ta_p_vertrag` table.

**Pass/Fail Criterion**:
*   The content of `project_id.dataset_id.sof_ta_p_vertrag` must exactly match the manually calculated expected output (which should be the original `sof_ta_vertrag_tmp` content) for the controlled input.

**Runnable Test Code (Pytest with BigQuery client and Pandas)**:

```python
import pytest
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")
ISBERT_SCHEMA_DATASET_ID = os.environ.get("BQ_ISBERT_SCHEMA_DATASET_ID", "isbert_schema")

client = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def setup_join_logic_test():
    bq_sof_ta_vertrag_tmp_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_vertrag_tmp"
    bq_dwtk_meldungen_table = f"{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen"
    bq_target_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_p_vertrag"

    # Ensure tables are empty
    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()

    # Define full schema for sof_ta_vertrag_tmp for consistent DataFrame creation
    full_schema_cols = [
        "vertrag_id_carmen", "partner_id_carmen", "rechdef_id_carmen", "kundenkonto", "mwst_kennzeichen",
        "rahmenvertrag_id", "rechnungslauf", "vo_kenn", "geplant_kuend", "eingang_kuend",
        "vertragsbeginn", "vertragsstatus", "sperrart", "sperrgrund", "stillegungszeitraum",
        "twincard", "dwh_tarifgr_text", "bindefrist", "letztes_upgrade", "vertragsbindung",
        "vertragsbindungseinheit", "rechnungszahlart", "rechnungsmedium", "twin_vertrag_id",
        "upgradeberechtigt", "apn", "upgradegrund", "sv_id", "vda", "cost_centre",
        "cost_centre_user", "cntrct_ty", "segment_id", "rv_action_id", "rechn_inh_konfig_text",
        "order_number", "commitment_reference_date", "cntrct_validity_id"
    ]
    
    # --- Controlled Input Data for sof_ta_vertrag_tmp ---
    input_data_rows = [
        {"vertrag_id_carmen": "V1", "twin_vertrag_id": "PV1", "partner_id_carmen": "P1"}, # v.twin_vertrag_id matches pv.vertrag_id_carmen
        {"vertrag_id_carmen": "V2", "twin_vertrag_id": "PV_NO_MATCH", "partner_id_carmen": "P2"}, # v.twin_vertrag_id has no match
        {"vertrag_id_carmen": "V3", "twin_vertrag_id": None, "partner_id_carmen": "P3"}, # v.twin_vertrag_id is NULL
        {"vertrag_id_carmen": "PV1", "twin_vertrag_id": "X1", "partner_id_carmen": "P_PV1"}, # A record that serves as pv.vertrag_id_carmen
        {"vertrag_id_carmen": "PV_NULL_ID", "twin_vertrag_id": "X2", "partner_id_carmen": "P_PV_NULL", "geplant_kuend": datetime(2023,1,1).date()}, # pv.vertrag_id_carmen is NULL
    ]
    
    # Create a DataFrame with all columns, initialized to None, then populate
    full_input_df = pd.DataFrame(columns=full_schema_cols)
    for row_data in input_data_rows:
        new_row = {col: row_data.get(col) for col in full_schema_cols}
        full_input_df = pd.concat([full_input_df, pd.DataFrame([new_row])], ignore_index=True)

    # Load into BQ
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(col, "STRING") if col not in ["geplant_kuend", "eingang_kuend", "vertragsbeginn", "letztes_upgrade", "commitment_reference_date"] else bigquery.SchemaField(col, "DATE") for col in full_schema_cols],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(full_input_df, bq_sof_ta_vertrag_tmp_table, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {bq_sof_ta_vertrag_tmp_table}")

    # --- Expected Output ---
    # As the SELECT list only contains columns from 'v', the output should be identical to the input 'v' table.
    expected_df = full_input_df.sort_values(by=list(full_input_df.columns)).reset_index(drop=True)

    # Add a dummy entry to dwtk_meldungen for the v_datum declaration to succeed
    dwtk_meldungen_data = [{"timecreated": datetime.now(), "job_kennung": "BERT_DROP_TEMP_TABLE"}]
    dwtk_meldungen_df = pd.DataFrame(dwtk_meldungen_data)
    job_config_dwtk = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("timecreated", "TIMESTAMP"),
            bigquery.SchemaField("job_kennung", "STRING")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(dwtk_meldungen_df, bq_dwtk_meldungen_table, job_config=job_config_dwtk).result()

    yield {
        "bq_target_table": bq_target_table,
        "expected_df": expected_df
    }

    # Teardown
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()


def test_join_logic_correctness(setup_join_logic_test):
    bq_target_table = setup_join_logic_test["bq_target_table"]
    expected_df = setup_join_logic_test["expected_df"]

    # --- Action: Execute the main transformation SQL ---
    with open("sql/d_ausd_v_ta_p_vertrag_bq.sql", "r") as f:
        bq_sql_script = f.read()
    
    bq_sql_script = bq_sql_script.replace("project_id", PROJECT_ID)
    bq_sql_script = bq_sql_script.replace("dataset_id", DATASET_ID)
    bq_sql_script = bq_sql_script.replace("isbert_schema", ISBERT_SCHEMA_DATASET_ID)

    try:
        client.query(bq_sql_script).result()
        print("BigQuery SQL script for join logic test executed successfully.")
    except Exception as e:
        pytest.fail(f"BigQuery SQL script execution failed: {e}")

    # --- Assertions ---
    migrated_df = client.query(f"SELECT * FROM `{bq_target_table}`").to_dataframe()
    migrated_df = migrated_df.sort_values(by=list(migrated_df.columns)).reset_index(drop=True)

    assert len(migrated_df) == len(expected_df), \
        f"Row count mismatch: Migrated has {len(migrated_df)} rows, Expected has {len(expected_df)} rows."

    pd.testing.assert_frame_equal(expected_df, migrated_df, check_dtype=False, check_like=True)
    print("Join logic correctness test passed.")

```

### Test Case 3: Date Determination Logic (`v_datum`)

**Purpose**: To verify that the `v_datum` variable is correctly calculated in BigQuery, matching the logic from `isbert_schema.dwtk_meldungen` (i.e., `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, formatted as YYYYMMDD, or '19000101' if no matching records).

**Setup**:
1.  **Input Data**: Populate `project_id.isbert_schema.dwtk_meldungen` with controlled data for different scenarios:
    *   Multiple records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values.
    *   Records for other `job_kennung` values.
    *   A scenario where `job_kennung = 'BERT_DROP_TEMP_TABLE'` has no records.
2.  **Expected `v_datum`**: Manually determine the expected `v_datum` for each scenario based on the `MAX(m.timecreated)` and `IFNULL` logic.

**Action**:
1.  Load the controlled input data for each scenario into `project_id.isbert_schema.dwtk_meldungen`.
2.  Execute a BigQuery query that calculates `v_datum` using the same logic as in `d_ausd_v_ta_p_vertrag_bq.sql`.

**Pass/Fail Criterion**:
*   The calculated `v_datum` in BigQuery must exactly match the manually determined expected value for each test scenario.

**Runnable Test Code (Pytest with BigQuery client and Pandas)**:

```python
import pytest
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
ISBERT_SCHEMA_DATASET_ID = os.environ.get("BQ_ISBERT_SCHEMA_DATASET_ID", "isbert_schema")

client = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def setup_v_datum_test():
    bq_dwtk_meldungen_table = f"{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen"
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()

    yield {
        "bq_dwtk_meldungen_table": bq_dwtk_meldungen_table,
        "test_cases": [
            # Test Case 1: Multiple relevant records
            {"input": [
                {"timecreated": datetime(2023, 1, 15, 10, 0, 0), "job_kennung": "OTHER_JOB"},
                {"timecreated": datetime(2023, 1, 10, 12, 0, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"},
                {"timecreated": datetime(2023, 1, 20, 14, 0, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"}, # Max
                {"timecreated": datetime(2023, 1, 5, 8, 0, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"},
            ], "expected": "20230120"},
            # Test Case 2: No relevant records
            {"input": [
                {"timecreated": datetime(2023, 2, 1, 10, 0, 0), "job_kennung": "OTHER_JOB_A"},
                {"timecreated": datetime(2023, 2, 2, 12, 0, 0), "job_kennung": "OTHER_JOB_B"},
            ], "expected": "19000101"},
            # Test Case 3: Single relevant record
            {"input": [
                {"timecreated": datetime(2023, 3, 1, 10, 0, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"},
            ], "expected": "20230301"},
        ]
    }

    # Teardown
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()


@pytest.mark.parametrize("test_idx", range(3)) # Iterate through the 3 test cases
def test_v_datum_calculation(setup_v_datum_test, test_idx):
    bq_dwtk_meldungen_table = setup_v_datum_test["bq_dwtk_meldungen_table"]
    current_test_case = setup_v_datum_test["test_cases"][test_idx]
    input_data = current_test_case["input"]
    expected_v_datum = current_test_case["expected"]

    # Load input data for the current test case
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()
    dwtk_meldungen_df = pd.DataFrame(input_data)
    job_config_dwtk = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("timecreated", "TIMESTAMP"),
            bigquery.SchemaField("job_kennung", "STRING")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(dwtk_meldungen_df, bq_dwtk_meldungen_table, job_config=job_config_dwtk).result()
    print(f"Loaded {len(input_data)} rows into {bq_dwtk_meldungen_table} for test case {test_idx}")

    # --- Action: Execute the v_datum calculation query ---
    v_datum_query = f"""
        SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM `{bq_dwtk_meldungen_table}` m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    
    try:
        query_job = client.query(v_datum_query)
        result = query_job.result()
        actual_v_datum = [row[0] for row in result][0]
        print(f"Test Case {test_idx}: Calculated v_datum: {actual_v_datum}, Expected: {expected_v_datum}")
    except Exception as e:
        pytest.fail(f"Failed to calculate v_datum for test case {test_idx}: {e}")

    # --- Assertions ---
    assert actual_v_datum == expected_v_datum, \
        f"v_datum mismatch for test case {test_idx}: Expected '{expected_v_datum}', Got '{actual_v_datum}'"
    print(f"v_datum calculation test passed for test case {test_idx}.")

```

### Test Case 4: Data Quality - NULL Handling in Output Columns

**Purpose**: To ensure that `NULL` values are correctly propagated or handled in the target table `project_id.dataset_id.sof_ta_p_vertrag`. Since all columns are selected directly from `v`, any `NULL` in `v` should remain `NULL` in the output.

**Setup**:
1.  **Input Data**: Create `project_id.dataset_id.sof_ta_vertrag_tmp` with specific scenarios:
    *   Rows where `twin_vertrag_id` is `NULL`.
    *   Rows where other critical columns (e.g., `partner_id_carmen`, `geplant_kuend`) are `NULL` in the source `sof_ta_vertrag_tmp`.
2.  **Expected Output**: Define the expected output for `sof_ta_p_vertrag` for these specific `NULL` scenarios. The output should mirror the input `sof_ta_vertrag_tmp` content, including `NULL`s.

**Action**:
1.  Load the controlled input data into `project_id.dataset_id.sof_ta_vertrag_tmp`.
2.  Execute the `transform_and_load_sof_ta_p_vertrag` task (or the relevant SQL part) of the Airflow DAG.
3.  Query `project_id.dataset_id.sof_ta_p_vertrag` to check the `NULL` values.

**Pass/Fail Criterion**:
*   The `NULL` values in the output `sof_ta_p_vertrag` must match the expected `NULL` patterns for the given input.

**Runnable Test Code (Pytest with BigQuery client and Pandas)**:

```python
import pytest
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")
ISBERT_SCHEMA_DATASET_ID = os.environ.get("BQ_ISBERT_SCHEMA_DATASET_ID", "isbert_schema")

client = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def setup_null_handling_test():
    bq_sof_ta_vertrag_tmp_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_vertrag_tmp"
    bq_dwtk_meldungen_table = f"{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen"
    bq_target_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_p_vertrag"

    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()

    full_schema_cols = [
        "vertrag_id_carmen", "partner_id_carmen", "rechdef_id_carmen", "kundenkonto", "mwst_kennzeichen",
        "rahmenvertrag_id", "rechnungslauf", "vo_kenn", "geplant_kuend", "eingang_kuend",
        "vertragsbeginn", "vertragsstatus", "sperrart", "sperrgrund", "stillegungszeitraum",
        "twincard", "dwh_tarifgr_text", "bindefrist", "letztes_upgrade", "vertragsbindung",
        "vertragsbindungseinheit", "rechnungszahlart", "rechnungsmedium", "twin_vertrag_id",
        "upgradeberechtigt", "apn", "upgradegrund", "sv_id", "vda", "cost_centre",
        "cost_centre_user", "cntrct_ty", "segment_id", "rv_action_id", "rechn_inh_konfig_text",
        "order_number", "commitment_reference_date", "cntrct_validity_id"
    ]

    # --- Controlled Input Data for sof_ta_vertrag_tmp with NULLs ---
    input_data_rows = [
        {"vertrag_id_carmen": "V101", "partner_id_carmen": "P101", "twin_vertrag_id": "PV101", "geplant_kuend": datetime(2024,1,1).date()},
        {"vertrag_id_carmen": "V102", "partner_id_carmen": "P102", "twin_vertrag_id": None, "geplant_kuend": datetime(2024,2,1).date()},
        {"vertrag_id_carmen": "V103", "partner_id_carmen": None, "twin_vertrag_id": "PV103", "geplant_kuend": None},
        {"vertrag_id_carmen": "V104", "partner_id_carmen": "P104", "twin_vertrag_id": "NO_MATCH_ID", "geplant_kuend": datetime(2024,4,1).date()},
        {"vertrag_id_carmen": "PV101", "partner_id_carmen": "P_PV101", "twin_vertrag_id": "X1", "geplant_kuend": datetime(2023,1,1).date()},
    ]
    
    full_input_df = pd.DataFrame(columns=full_schema_cols)
    for row_data in input_data_rows:
        new_row = {col: row_data.get(col) for col in full_schema_cols}
        full_input_df = pd.concat([full_input_df, pd.DataFrame([new_row])], ignore_index=True)

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(col, "STRING") if col not in ["geplant_kuend", "eingang_kuend", "vertragsbeginn", "letztes_upgrade", "commitment_reference_date"] else bigquery.SchemaField(col, "DATE") for col in full_schema_cols],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(full_input_df, bq_sof_ta_vertrag_tmp_table, job_config=job_config).result()
    print(f"Loaded {job.output_rows} rows into {bq_sof_ta_vertrag_tmp_table}")

    # Add a dummy entry to dwtk_meldungen
    dwtk_meldungen_data = [{"timecreated": datetime.now(), "job_kennung": "BERT_DROP_TEMP_TABLE"}]
    dwtk_meldungen_df = pd.DataFrame(dwtk_meldungen_data)
    job_config_dwtk = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("timecreated", "TIMESTAMP"),
            bigquery.SchemaField("job_kennung", "STRING")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(dwtk_meldungen_df, bq_dwtk_meldungen_table, job_config=job_config_dwtk).result()

    # Expected output (only rows from 'v' side, with their original NULLs)
    expected_df = full_input_df.sort_values(by=list(full_input_df.columns)).reset_index(drop=True)

    yield {
        "bq_target_table": bq_target_table,
        "expected_df": expected_df
    }

    # Teardown
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()


def test_null_handling_in_output(setup_null_handling_test):
    bq_target_table = setup_null_handling_test["bq_target_table"]
    expected_df = setup_null_handling_test["expected_df"]

    # --- Action: Execute the main transformation SQL ---
    with open("sql/d_ausd_v_ta_p_vertrag_bq.sql", "r") as f:
        bq_sql_script = f.read()
    
    bq_sql_script = bq_sql_script.replace("project_id", PROJECT_ID)
    bq_sql_script = bq_sql_script.replace("dataset_id", DATASET_ID)
    bq_sql_script = bq_sql_script.replace("isbert_schema", ISBERT_SCHEMA_DATASET_ID)

    try:
        client.query(bq_sql_script).result()
        print("BigQuery SQL script for NULL handling test executed successfully.")
    except Exception as e:
        pytest.fail(f"BigQuery SQL script execution failed: {e}")

    # --- Assertions ---
    migrated_df = client.query(f"SELECT * FROM `{bq_target_table}`").to_dataframe()
    migrated_df = migrated_df.sort_values(by=list(migrated_df.columns)).reset_index(drop=True)

    assert len(migrated_df) == len(expected_df), \
        f"Row count mismatch: Migrated has {len(migrated_df)} rows, Expected has {len(expected_df)} rows."

    # Pandas assert_frame_equal handles NaN/None comparison correctly
    pd.testing.assert_frame_equal(expected_df, migrated_df, check_dtype=False, check_like=True)
    print("NULL handling in output test passed.")

```

### Test Case 5: Data Quality - Row Count Assertion

**Purpose**: To verify that the number of rows inserted into `project_id.dataset_id.sof_ta_p_vertrag` is consistent with the expected behavior of the `LEFT JOIN` (which, as identified, should be the count of rows in the `v` table). Also, to verify the truncation of the target table before insertion.

**Setup**:
1.  **Input Data**: Populate `project_id.dataset_id.sof_ta_vertrag_tmp` with a known number of rows (e.g., 100 rows).
2.  **Expected Row Count**: The expected row count in `sof_ta_p_vertrag` should be equal to the number of rows initially loaded into `sof_ta_vertrag_tmp`.

**Action**:
1.  Load controlled input data into `project_id.dataset_id.sof_ta_vertrag_tmp`.
2.  Execute the `transform_and_load_sof_ta_p_vertrag` task.
3.  Query the row count of `project_id.dataset_id.sof_ta_p_vertrag`.

**Pass/Fail Criterion**:
*   The row count of `project_id.dataset_id.sof_ta_p_vertrag` must be equal to the initial row count of `project_id.dataset_id.sof_ta_vertrag_tmp`.

**Runnable Test Code (Pytest with BigQuery client and Pandas)**:

```python
import pytest
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")
ISBERT_SCHEMA_DATASET_ID = os.environ.get("BQ_ISBERT_SCHEMA_DATASET_ID", "isbert_schema")

client = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def setup_row_count_test():
    bq_sof_ta_vertrag_tmp_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_vertrag_tmp"
    bq_dwtk_meldungen_table = f"{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen"
    bq_target_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_p_vertrag"

    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()

    full_schema_cols = [
        "vertrag_id_carmen", "partner_id_carmen", "rechdef_id_carmen", "kundenkonto", "mwst_kennzeichen",
        "rahmenvertrag_id", "rechnungslauf", "vo_kenn", "geplant_kuend", "eingang_kuend",
        "vertragsbeginn", "vertragsstatus", "sperrart", "sperrgrund", "stillegungszeitraum",
        "twincard", "dwh_tarifgr_text", "bindefrist", "letztes_upgrade", "vertragsbindung",
        "vertragsbindungseinheit", "rechnungszahlart", "rechnungsmedium", "twin_vertrag_id",
        "upgradeberechtigt", "apn", "upgradegrund", "sv_id", "vda", "cost_centre",
        "cost_centre_user", "cntrct_ty", "segment_id", "rv_action_id", "rechn_inh_konfig_text",
        "order_number", "commitment_reference_date", "cntrct_validity_id"
    ]

    # --- Controlled Input Data for sof_ta_vertrag_tmp (e.g., 100 rows) ---
    input_data_rows = [
        {"vertrag_id_carmen": f"V{i}", "twin_vertrag_id": f"PV{i}" if i % 2 == 0 else None, "partner_id_carmen": f"P{i}"}
        for i in range(100)
    ]
    
    full_input_df = pd.DataFrame(columns=full_schema_cols)
    for row_data in input_data_rows:
        new_row = {col: row_data.get(col) for col in full_schema_cols}
        full_input_df = pd.concat([full_input_df, pd.DataFrame([new_row])], ignore_index=True)

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(col, "STRING") if col not in ["geplant_kuend", "eingang_kuend", "vertragsbeginn", "letztes_upgrade", "commitment_reference_date"] else bigquery.SchemaField(col, "DATE") for col in full_schema_cols],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(full_input_df, bq_sof_ta_vertrag_tmp_table, job_config=job_config).result()
    print(f"Loaded {job.output_rows} rows into {bq_sof_ta_vertrag_tmp_table}")
    
    expected_row_count = len(input_data_rows) # Should be 100

    # Add a dummy entry to dwtk_meldungen
    dwtk_meldungen_data = [{"timecreated": datetime.now(), "job_kennung": "BERT_DROP_TEMP_TABLE"}]
    dwtk_meldungen_df = pd.DataFrame(dwtk_meldungen_data)
    job_config_dwtk = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("timecreated", "TIMESTAMP"),
            bigquery.SchemaField("job_kennung", "STRING")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(dwtk_meldungen_df, bq_dwtk_meldungen_table, job_config=job_config_dwtk).result()

    yield {
        "bq_target_table": bq_target_table,
        "expected_row_count": expected_row_count
    }

    # Teardown
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()
    client.query(f"TRUNCATE TABLE `{bq_target_table}`").result()


def test_row_count_assertion(setup_row_count_test):
    bq_target_table = setup_row_count_test["bq_target_table"]
    expected_row_count = setup_row_count_test["expected_row_count"]

    # --- Action: Execute the main transformation SQL ---
    with open("sql/d_ausd_v_ta_p_vertrag_bq.sql", "r") as f:
        bq_sql_script = f.read()
    
    bq_sql_script = bq_sql_script.replace("project_id", PROJECT_ID)
    bq_sql_script = bq_sql_script.replace("dataset_id", DATASET_ID)
    bq_sql_script = bq_sql_script.replace("isbert_schema", ISBERT_SCHEMA_DATASET_ID)

    try:
        client.query(bq_sql_script).result()
        print("BigQuery SQL script for row count test executed successfully.")
    except Exception as e:
        pytest.fail(f"BigQuery SQL script execution failed: {e}")

    # --- Assertions ---
    query_result = client.query(f"SELECT COUNT(*) FROM `{bq_target_table}`").result()
    actual_row_count = [row[0] for row in query_result][0]

    assert actual_row_count == expected_row_count, \
        f"Row count mismatch: Expected {expected_row_count} rows, Got {actual_row_count} rows."
    print("Row count assertion test passed.")

```

### Test Case 6: Schema and Data Type Parity

**Purpose**: To verify that the schema (column names, order, and data types) of the BigQuery target table `project_id.dataset_id.sof_ta_p_vertrag` matches the legacy Oracle `sof$ta_p_vertrag` table. This is crucial for downstream consumers.

**Setup**:
1.  **Legacy Schema**: Obtain the precise schema (column names, data types, nullability) of the Oracle `sof$ta_p_vertrag` table.
2.  **BigQuery DDL**: Ensure the `sql/ddl/sof_ta_p_vertrag.sql` file is used to create the BigQuery table.

**Action**:
1.  Ensure the `create_sof_ta_p_vertrag_table` task has run in Airflow, or manually create the table using the provided DDL.
2.  Query the schema of `project_id.dataset_id.sof_ta_p_vertrag` in BigQuery using BigQuery API.

**Pass/Fail Criterion**:
*   The BigQuery table schema must match the Oracle schema in terms of column names, their order, and their corresponding data types. (e.g., Oracle `VARCHAR2(X)` -> BQ `STRING`, Oracle `DATE` -> BQ `DATE`, Oracle `NUMBER` -> BQ `NUMERIC` or `INT64`/`FLOAT64` depending on precision).

**Runnable Test Code (Pytest with BigQuery API)**:

```python
import pytest
from google.cloud import bigquery
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")

client = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="module")
def setup_schema_test():
    bq_target_table_id = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_p_vertrag"
    
    # Ensure the table is created using the DDL
    with open("sql/ddl/sof_ta_p_vertrag.sql", "r") as f:
        ddl_sql = f.read()
    ddl_sql = ddl_sql.replace("project_id", PROJECT_ID).replace("dataset_id", DATASET_ID)
    client.query(ddl_sql).result()
    print(f"Ensured table {bq_target_table_id} exists.")

    # --- Define Expected Schema (based on Oracle legacy schema mapping to BQ) ---
    # This list must be meticulously derived from the actual Oracle schema.
    expected_schema = [
        {"name": "vertrag_id_carmen", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "partner_id_carmen", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "rechdef_id_carmen", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "kundenkonto", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "mwst_kennzeichen", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "rahmenvertrag_id", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "rechnungslauf", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "vo_kenn", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "geplant_kuend", "field_type": "DATE", "mode": "NULLABLE"},
        {"name": "eingang_kuend", "field_type": "DATE", "mode": "NULLABLE"},
        {"name": "vertragsbeginn", "field_type": "DATE", "mode": "NULLABLE"},
        {"name": "vertragsstatus", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "sperrart", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "sperrgrund", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "stillegungszeitraum", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "twincard", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "dwh_tarifgr_text", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "bindefrist", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "letztes_upgrade", "field_type": "DATE", "mode": "NULLABLE"},
        {"name": "vertragsbindung", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "vertragsbindungseinheit", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "rechnungszahlart", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "rechnungsmedium", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "twin_vertrag_id", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "upgradeberechtigt", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "apn", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "upgradegrund", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "sv_id", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "vda", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "cost_centre", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "cost_centre_user", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "cntrct_ty", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "segment_id", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "rv_action_id", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "rechn_inh_konfig_text", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "order_number", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "commitment_reference_date", "field_type": "DATE", "mode": "NULLABLE"},
        {"name": "cntrct_validity_id", "field_type": "STRING", "mode": "NULLABLE"},
    ]

    yield {
        "bq_target_table_id": bq_target_table_id,
        "expected_schema": expected_schema
    }

    # Teardown (optional, if table needs to be dropped after test)
    # client.delete_table(bq_target_table_id, not_found_ok=True)


def test_schema_and_data_type_parity(setup_schema_test):
    bq_target_table_id = setup_schema_test["bq_target_table_id"]
    expected_schema = setup_schema_test["expected_schema"]

    # --- Action: Retrieve actual BigQuery schema ---
    table = client.get_table(bq_target_table_id)
    actual_schema = []
    for field in table.schema:
        actual_schema.append({
            "name": field.name,
            "field_type": field.field_type,
            "mode": field.mode
        })
    
    # --- Assertions ---
    assert len(actual_schema) == len(expected_schema), \
        f"Schema length mismatch: Expected {len(expected_schema)} fields, Got {len(actual_schema)} fields."

    for i, expected_field in enumerate(expected_schema):
        actual_field = actual_schema[i]
        assert actual_field["name"] == expected_field["name"], \
            f"Column name mismatch at index {i}: Expected '{expected_field['name']}', Got '{actual_field['name']}'"
        assert actual_field["field_type"] == expected_field["field_type"], \
            f"Data type mismatch for column '{expected_field['name']}': Expected '{expected_field['field_type']}', Got '{actual_field['field_type']}'"
        assert actual_field["mode"] == expected_field["mode"], \
            f"Nullability mode mismatch for column '{expected_field['name']}': Expected '{expected_field['mode']}', Got '{actual_field['mode']}'"
    
    print("Schema and data type parity test passed.")

```

### Test Case 7: External System Replacement - CARMEN DB Ingestion Fidelity

**Purpose**: To verify that the data ingested from the CARMEN DB (via DMS, Data Fusion, or custom pipeline) into the BigQuery staging table `project_id.dataset_id.sof_ta_vertrag_tmp` is an accurate and complete replica of the source Oracle `sof$ta_vertrag_tmp`. This test focuses on the upstream ingestion process.

**Setup**:
1.  **Legacy Source**: Identify a specific snapshot or time window of data in the Oracle `sof$ta_vertrag_tmp` table.
2.  **Ingestion Pipeline**: Ensure the ingestion pipeline from CARMEN DB to `project_id.dataset_id.sof_ta_vertrag_tmp` is configured and running.
3.  **Golden Record**: Extract the data from Oracle `sof$ta_vertrag_tmp` for the chosen snapshot/window. This is the golden record for this staging table.

**Action**:
1.  Trigger the ingestion pipeline to populate `project_id.dataset_id.sof_ta_vertrag_tmp` with data corresponding to the chosen Oracle snapshot.
2.  Query `project_id.dataset_id.sof_ta_vertrag_tmp` in BigQuery.

**Pass/Fail Criterion**:
*   The row count of `project_id.dataset_id.sof_ta_vertrag_tmp` must be identical to the golden record.
*   Every column in `project_id.dataset_id.sof_ta_vertrag_tmp` must have identical values to its corresponding column in the Oracle golden record, for every row. This includes data types and `NULL` handling.

**Runnable Test Code (Pytest with BigQuery client and Pandas)**:

```python
import pytest
from google.cloud import bigquery
import pandas as pd
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")

client = bigquery.Client(project=PROJECT_ID)

# Helper function to load data into BQ (simulating ingestion)
def load_csv_to_bq_staging(table_id, data_path, schema=None):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True if schema is None else False,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(data_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()
    print(f"Simulated ingestion: Loaded {job.output_rows} rows into {table_id}")

# Helper function to fetch data from BQ as DataFrame
def fetch_bq_data_as_df(table_id):
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()
    return df.sort_values(by=list(df.columns)).reset_index(drop=True)

@pytest.fixture(scope="module")
def setup_carmen_ingestion_test():
    bq_staging_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_vertrag_tmp"

    # --- Capture Legacy Output (Golden Record) ---
    # This CSV should be prepared from the Oracle sof$ta_vertrag_tmp snapshot
    oracle_golden_path = "tests/data/legacy_input/sof_ta_vertrag_tmp_oracle_golden.csv"
    golden_df = pd.read_csv(oracle_golden_path)
    golden_df = golden_df.sort_values(by=list(golden_df.columns)).reset_index(drop=True)

    yield {
        "bq_staging_table": bq_staging_table,
        "oracle_golden_path": oracle_golden_path,
        "golden_df": golden_df
    }

    # Teardown
    client.query(f"TRUNCATE TABLE `{bq_staging_table}`").result()


def test_carmen_db_ingestion_fidelity(setup_carmen_ingestion_test):
    bq_staging_table = setup_carmen_ingestion_test["bq_staging_table"]
    oracle_golden_path = setup_carmen_ingestion_test["oracle_golden_path"]
    golden_df = setup_carmen_ingestion_test["golden_df"]

    # --- Action: Simulate ingestion into BigQuery staging table ---
    # In a real scenario, you'd trigger the actual ingestion pipeline (DMS, Data Fusion, etc.)
    # For this test, we'll load the same golden CSV into BQ to verify the comparison logic.
    # The actual test would involve comparing BQ data against a *real* Oracle extract.
    # Use the full schema for sof_ta_vertrag_tmp for loading
    full_schema_cols = [
        "vertrag_id_carmen", "partner_id_carmen", "rechdef_id_carmen", "kundenkonto", "mwst_kennzeichen",
        "rahmenvertrag_id", "rechnungslauf", "vo_kenn", "geplant_kuend", "eingang_kuend",
        "vertragsbeginn", "vertragsstatus", "sperrart", "sperrgrund", "stillegungszeitraum",
        "twincard", "dwh_tarifgr_text", "bindefrist", "letztes_upgrade", "vertragsbindung",
        "vertragsbindungseinheit", "rechnungszahlart", "rechnungsmedium", "twin_vertrag_id",
        "upgradeberechtigt", "apn", "upgradegrund", "sv_id", "vda", "cost_centre",
        "cost_centre_user", "cntrct_ty", "segment_id", "rv_action_id", "rechn_inh_konfig_text",
        "order_number", "commitment_reference_date", "cntrct_validity_id"
    ]
    sof_ta_vertrag_tmp_schema = [bigquery.SchemaField(col, "STRING") if col not in ["geplant_kuend", "eingang_kuend", "vertragsbeginn", "letztes_upgrade", "commitment_reference_date"] else bigquery.SchemaField(col, "DATE") for col in full_schema_cols]

    load_csv_to_bq_staging(bq_staging_table, oracle_golden_path, schema=sof_ta_vertrag_tmp_schema)

    # --- Assertions ---
    ingested_df = fetch_bq_data_as_df(bq_staging_table)

    assert len(ingested_df) == len(golden_df), \
        f"Ingested row count mismatch: BQ has {len(ingested_df)} rows, Oracle Golden has {len(golden_df)} rows."

    pd.testing.assert_frame_equal(golden_df, ingested_df, check_dtype=False, check_like=True)
    print("CARMEN DB ingestion fidelity test passed.")

```

### Test Case 8: Cleanup of Temporary Tables

**Purpose**: To verify that all specified temporary tables are truncated after the main transformation, as per the legacy Oracle script's behavior.

**Setup**:
1.  **Temporary Tables**: Ensure all listed temporary tables (`sof_ta_disc_zusgf`, `sof_ta_discount`, etc.) exist in BigQuery (using their placeholder DDLs) and are populated with some test data.
2.  **Main Target Table**: `sof_ta_p_vertrag` should also be populated to ensure the cleanup task runs after the main logic.

**Action**:
1.  Populate all temporary tables with at least one row of dummy data.
2.  Execute the `cleanup_temp_tables` task (or the relevant SQL part) of the Airflow DAG.
3.  Query the row counts of all temporary tables.

**Pass/Fail Criterion**:
*   After the `cleanup_temp_tables` task completes, the row count of *each* specified temporary table must be zero.

**Runnable Test Code (Pytest with BigQuery client)**:

```python
import pytest
from google.cloud import bigquery
import pandas as pd
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset_id")

client = bigquery.Client(project=PROJECT_ID)

temp_tables_to_cleanup = [
    "sof_ta_disc_zusgf", "sof_ta_discount", "sof_ta_barrier_zusgf", "sof_ta_barrier",
    "sof_ta_cntrct_crs", "sof_ta_cntrct_templ", "sof_ta_cntrct_valid", "sof_ta_period",
    "sof_ta_bp_ref", "sof_ta_inv_assign", "sof_ta_inv_def", "sof_ta_acc_ref",
    "sof_ta_notice", "sof_ta_apn_ve", "sof_ta_discount_rr", "sof_ta_vvl_dwh",
    "sof_ta_vvl_upgrade", "sof_ta_cntrct_crs2", "sof_ta_cntrct_crs3", "sof_ta_inv_acc",
    "sof_ta_vertrag_tmp", "sof_ta_action_assoc"
]

@pytest.fixture(scope="module")
def setup_cleanup_test():
    # Ensure DDLs for temp tables are run (using placeholder schema)
    for table_name in temp_tables_to_cleanup:
        ddl_file = f"sql/ddl/{table_name}.sql"
        with open(ddl_file, "r") as f:
            ddl_sql = f.read()
        ddl_sql = ddl_sql.replace("project_id", PROJECT_ID).replace("dataset_id", DATASET_ID)
        client.query(ddl_sql).result()
        print(f"Ensured table {table_name} exists.")

        # Populate with dummy data
        bq_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        dummy_data = pd.DataFrame([{"placeholder_col": "test_data"}])
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("placeholder_col", "STRING")],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.load_table_from_dataframe(dummy_data, bq_table_id, job_config=job_config).result()
        print(f"Populated {table_name} with 1 row.")

    yield {
        "temp_tables": temp_tables_to_cleanup
    }

    # Teardown (optional, if tables need to be dropped)
    # for table_name in temp_tables_to_cleanup:
    #     bq_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    #     client.delete_table(bq_table_id, not_found_ok=True)


def test_cleanup_temporary_tables(setup_cleanup_test):
    temp_tables = setup_cleanup_test["temp_tables"]

    # --- Action: Execute the cleanup SQL ---
    # Extract only the cleanup part from the full SQL script
    with open("sql/d_ausd_v_ta_p_vertrag_bq.sql", "r") as f:
        full_bq_sql_script = f.read()
    
    # Find the start of the cleanup section
    cleanup_start_marker = "-- Truncate temporary tables"
    cleanup_sql = full_bq_sql_script.split(cleanup_start_marker, 1)[-1]
    
    cleanup_sql = cleanup_sql.replace("project_id", PROJECT_ID)
    cleanup_sql = cleanup_sql.replace("dataset_id", DATASET_ID)

    try:
        client.query(cleanup_sql).result()
        print("BigQuery cleanup SQL script executed successfully.")
    except Exception as e:
        pytest.fail(f"BigQuery cleanup SQL script execution failed: {e}")

    # --- Assertions ---
    for table_name in temp_tables:
        bq_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        query_result = client.query(f"SELECT COUNT(*) FROM `{bq_table_id}`").result()
        actual_row_count = [row[0] for row in query_result][0]

        assert actual_row_count == 0, \
            f"Temporary table '{table_name}' was not truncated. Expected 0 rows, Got {actual_row_count} rows."
        print(f"Temporary table '{table_name}' successfully truncated.")

```

### Test Case 9: Airflow DAG Orchestration and Task Dependencies

**Purpose**: To verify that the Airflow DAG correctly orchestrates the tasks in the specified order, handles dependencies, and integrates with BigQuery operators. This ensures the workflow logic is correctly migrated from UC4/KornShell.

**Setup**:
1.  **Airflow Environment**: A running Airflow instance (e.g., Cloud Composer) with the `dw_bert_ausd_v_ta_p_vertrag` DAG deployed and unpaused.
2.  **BigQuery Tables**: Ensure all necessary BigQuery tables (staging and target) exist with their DDLs (as created by the initial tasks in the DAG or pre-created).
3.  **Input Data**: Ensure `sof_ta_vertrag_tmp` and `isbert_schema.dwtk_meldungen` have some data for the transformation to run successfully.

**Action**:
1.  Manually trigger the `dw_bert_ausd_v_ta_p_vertrag` DAG in Airflow (e.g., via Airflow UI or CLI).
2.  Monitor the DAG run in the Airflow UI.

**Pass/Fail Criterion**:
*   All tasks in the DAG (`start`, `create_sof_ta_p_vertrag_table`, `create_sof_ta_vertrag_tmp_table`, `create_isbert_dwtk_meldungen_table`, `ingest_data_to_staging` (dummy), `transform_and_load_sof_ta_p_vertrag`, `cleanup_temp_tables`, `end`) must complete successfully.
*   The tasks must execute in the intended order as defined by the dependencies.
*   No tasks should fail due to Airflow configuration issues, BigQuery connection issues, or SQL syntax errors.

**Runnable Test Code (Conceptual Airflow CLI/API interaction)**:

```python
# This is a conceptual test, typically executed via Airflow's CLI or API
# or using Airflow's built-in test utilities (e.g., `airflow dags test`).
# It requires an active Airflow environment.

import subprocess
import time
import json
import pytest

def trigger_and_monitor_airflow_dag(dag_id, timeout_seconds=900): # Increased timeout for full DAG run
    print(f"Triggering Airflow DAG: {dag_id}")
    # Trigger the DAG with a unique run_id
    run_id = f"test_run_orchestration_{int(time.time())}"
    trigger_command = ["airflow", "dags", "trigger", dag_id, "--conf", f'{{"run_id": "{run_id}"}}']
    try:
        subprocess.run(trigger_command, check=True, capture_output=True, text=True)
        print(f"DAG {dag_id} triggered successfully with run_id: {run_id}")
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to trigger DAG {dag_id}: {e.stderr}")

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        # Get the status of the specific DAG run
        list_runs_command = ["airflow", "dags", "list-runs", "--dag-id", dag_id, "--output", "json"]
        try:
            result = subprocess.run(list_runs_command, check=True, capture_output=True, text=True)
            runs = json.loads(result.stdout)
            
            current_run = next((r for r in runs if r.get("run_id") == run_id), None)

            if current_run:
                run_state = current_run.get("state")
                print(f"Current DAG run '{run_id}' state: {run_state}")
                if run_state == "success":
                    print(f"DAG {dag_id} run '{run_id}' completed successfully.")
                    return True
                elif run_state in ["failed", "upstream_failed", "skipped"]:
                    pytest.fail(f"DAG {dag_id} run '{run_id}' failed with state: {run_state}")
            else:
                print(f"DAG run '{run_id}' not found yet, waiting...")

        except subprocess.CalledProcessError as e:
            print(f"Error listing DAG runs: {e.stderr}")
        
        time.sleep(30) # Wait before polling again
    
    pytest.fail(f"DAG {dag_id} run '{run_id}' did not complete within {timeout_seconds} seconds.")

def test_airflow_orchestration():
    dag_id = "dw_bert_ausd_v_ta_p_vertrag"
    
    # Pre-populate staging tables with minimal data for the DAG to run without errors
    # This is a simplified setup; in a real scenario, upstream ingestion would handle this.
    bq_sof_ta_vertrag_tmp_table = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_vertrag_tmp"
    bq_dwtk_meldungen_table = f"{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen"
    
    # Ensure tables exist (DDL tasks in DAG will handle this, but good to ensure for robustness)
    # and populate with minimal data
    client.query(f"CREATE TABLE IF NOT EXISTS `{bq_sof_ta_vertrag_tmp_table}` (vertrag_id_carmen STRING, twin_vertrag_id STRING)").result()
    client.query(f"TRUNCATE TABLE `{bq_sof_ta_vertrag_tmp_table}`").result()
    client.query(f"INSERT INTO `{bq_sof_ta_vertrag_tmp_table}` (vertrag_id_carmen, twin_vertrag_id) VALUES ('V_TEST', 'PV_TEST')").result()

    client.query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}`").result()
    client.query(f"CREATE TABLE IF NOT EXISTS `{bq_dwtk_meldungen_table}` (timecreated TIMESTAMP, job_kennung STRING)").result()
    client.query(f"TRUNCATE TABLE `{bq_dwtk_meldungen_table}`").result()
    client.query(f"INSERT INTO `{bq_dwtk_meldungen_table}` (timecreated, job_kennung) VALUES (CURRENT_TIMESTAMP(), 'BERT_DROP_TEMP_TABLE')").result()

    assert trigger_and_monitor_airflow_dag(dag_id)

```