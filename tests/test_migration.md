As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `k_ausd_v_ta_disc_zusgf.ksh` to BigQuery and Airflow. These tests aim to ensure behavioral equivalence, data integrity, and correctness across all aspects of the migration.

---

## Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **Environment Access**:
    *   Access to the legacy Oracle database where `isbert_schema.dwtk_meldungen` and `sof$ta_discount` reside, and where `sof$ta_disc_zusgf` is populated.
    *   Access to the migrated Google Cloud Project with BigQuery and Cloud Composer (Airflow).
2.  **Data Synchronization**:
    *   For each test case, a controlled set of input data must be loaded into both the Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_discount`) and their corresponding BigQuery tables (`your_gcp_project_id.isbert_schema.dwtk_meldungen`, `your_gcp_project_id.raw_sof.sof$ta_discount`). This ensures a consistent starting point.
    *   The target table `sof$ta_disc_zusgf` (both Oracle and BigQuery) should be empty before each test run, or its state should be known and accounted for.
3.  **Execution Mechanism**:
    *   A way to execute the legacy KornShell script (`k_ausd_v_ta_disc_zusgf.ksh`) with specific parameters.
    *   A way to trigger the migrated Airflow DAG (`k_ausd_v_ta_disc_zusgf_dag`) with specific parameters.
4.  **Test Harness**:
    *   A Python testing framework (e.g., `pytest`) with database connection utilities for both Oracle and BigQuery.
    *   Helper functions to execute legacy scripts and Airflow DAGs programmatically.

---

## Test Cases

### Test Case 1: Schema and Data Type Parity

**Purpose**: Verify that the target BigQuery table `sof$ta_disc_zusgf` has the correct schema (column names, data types) matching the Oracle legacy table. This ensures basic structural integrity.

**Setup**:
1.  Ensure the `sof$ta_disc_zusgf` table exists in both Oracle and BigQuery.
2.  Have a utility function to fetch schema details from both databases.

**Action**:
1.  Retrieve the schema (column names and their data types) for `sof$ta_disc_zusgf` from the Oracle database.
2.  Retrieve the schema for `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf` from BigQuery.

**Pass/Fail Criterion**:
The column names and their corresponding data types (or their BigQuery equivalents, e.g., `NUMBER` to `INT64`, `VARCHAR2` to `STRING`) must match exactly between the Oracle and BigQuery tables.

**Runnable Test Code (Python/Pytest)**:

```python
import pytest
from your_test_utils import get_oracle_schema, get_bigquery_schema

def test_sof_ta_disc_zusgf_schema_parity():
    """
    Compares the schema of the target table in Oracle and BigQuery.
    """
    oracle_table_name = "SOF$TA_DISC_ZUSGF"
    bigquery_table_id = "your_gcp_project_id.raw_sof.sof$ta_disc_zusgf"

    # Expected BigQuery schema based on migration design
    expected_bigquery_schema = {
        "CNTRCT_ID": "INT64",
        "CNTRCT_OBJ_VERSION": "INT64",
        "DISC_VECTOR_TY": "STRING",
        "RABATT_ALLE": "STRING",
    }

    # Fetch Oracle schema (example output format: {'COLUMN_NAME': 'DATA_TYPE'})
    oracle_schema = get_oracle_schema(oracle_table_name)

    # Fetch BigQuery schema
    bigquery_schema = get_bigquery_schema(bigquery_table_id)

    # Assert column count
    assert len(oracle_schema) == len(bigquery_schema), \
        f"Column count mismatch: Oracle has {len(oracle_schema)}, BigQuery has {len(bigquery_schema)}"

    # Assert each column's type
    for col_name, oracle_type in oracle_schema.items():
        assert col_name in bigquery_schema, f"Column {col_name} missing in BigQuery."
        bigquery_type = bigquery_schema[col_name]

        # Map Oracle types to expected BigQuery types for comparison
        if "NUMBER" in oracle_type:
            expected_type = "INT64" # Assuming NUMBER(10) maps to INT64
        elif "VARCHAR2" in oracle_type:
            expected_type = "STRING"
        else:
            expected_type = oracle_type # Fallback for other types

        assert bigquery_type == expected_type, \
            f"Type mismatch for column {col_name}: Oracle '{oracle_type}' (expected BigQuery '{expected_type}'), BigQuery '{bigquery_type}'"

    print(f"Schema parity confirmed for {oracle_table_name} and {bigquery_table_id}")

```

---

### Test Case 2: Full Data Parity (Output Parity)

**Purpose**: Verify that for identical input data, the migrated BigQuery job produces the exact same output data in `sof$ta_disc_zusgf` as the legacy Oracle job. This is the most critical test for behavioral equivalence.

**Setup**:
1.  Prepare a comprehensive test dataset for `isbert_schema.dwtk_meldungen` and `sof$ta_discount` covering various scenarios (e.g., multiple discounts for a contract, single discount, no discounts, NULL values in `rabatt`/`rabatthoehe`, different `disc_vector_ty`).
2.  Load this dataset into both Oracle source tables and BigQuery source tables.
3.  Ensure both target tables (`sof$ta_disc_zusgf`) are empty before execution.

**Action**:
1.  Execute the legacy KornShell script (`k_ausd_v_ta_disc_zusgf.ksh`) with appropriate parameters.
2.  Extract all data from the Oracle `sof$ta_disc_zusgf` table.
3.  Trigger the Airflow DAG (`k_ausd_v_ta_disc_zusgf_dag`) with equivalent parameters.
4.  Extract all data from the BigQuery `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf` table.

**Pass/Fail Criterion**:
The number of rows and the content of all columns in the BigQuery target table must be identical to the Oracle target table. Order of rows does not matter, but the set of rows must be the same.

**Runnable Test Code (Python/Pytest)**:

```python
import pytest
from your_test_utils import (
    load_oracle_test_data, clear_oracle_table, get_oracle_data,
    load_bigquery_test_data, clear_bigquery_table, get_bigquery_data,
    execute_legacy_ksh_script, trigger_airflow_dag
)
import pandas as pd

def test_full_data_parity_sof_ta_disc_zusgf():
    """
    Compares the full output data of the legacy and migrated jobs.
    """
    oracle_target_table = "SOF$TA_DISC_ZUSGF"
    bigquery_target_table = "your_gcp_project_id.raw_sof.sof$ta_disc_zusgf"

    # 1. Setup: Load test data and clear target tables
    test_data_scenario = "scenario_A_complex_discounts" # Define a specific test data set
    load_oracle_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "sof$ta_discount"])
    load_bigquery_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "raw_sof.sof$ta_discount"])

    clear_oracle_table(oracle_target_table)
    clear_bigquery_table(bigquery_target_table)

    # 2. Action: Execute both jobs
    # Legacy job execution (assuming it takes -j and -f parameters)
    legacy_job_params = {"j": "TEST_JOB_ID", "f": "123"}
    execute_legacy_ksh_script("k_ausd_v_ta_disc_zusgf.ksh", legacy_job_params)

    # Migrated job execution (Airflow DAG)
    airflow_dag_params = {"job_kennung": "TEST_JOB_ID", "entry_nr": "123"}
    trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", airflow_dag_params)

    # 3. Extract results
    oracle_results_df = get_oracle_data(oracle_target_table)
    bigquery_results_df = get_bigquery_data(bigquery_target_table)

    # Standardize column names and sort for comparison
    # Oracle column names might be uppercase, BigQuery might be lowercase or mixed
    oracle_results_df.columns = [col.upper() for col in oracle_results_df.columns]
    bigquery_results_df.columns = [col.upper() for col in bigquery_results_df.columns]

    # Sort both DataFrames by primary keys to ensure consistent comparison
    sort_cols = ['CNTRCT_ID', 'CNTRCT_OBJ_VERSION', 'DISC_VECTOR_TY']
    oracle_results_df = oracle_results_df.sort_values(by=sort_cols).reset_index(drop=True)
    bigquery_results_df = bigquery_results_df.sort_values(by=sort_cols).reset_index(drop=True)

    # 4. Pass/Fail Criterion
    pd.testing.assert_frame_equal(oracle_results_df, bigquery_results_df, check_dtype=True)

    print(f"Full data parity confirmed for {oracle_target_table} and {bigquery_target_table}")

```

---

### Test Case 3: `v_datum` Calculation Correctness (Transformation Correctness)

**Purpose**: Verify that the `v_datum` variable, used for filtering, is calculated identically in both environments, including `COALESCE` (Oracle `NVL`) behavior.

**Setup**:
1.  Prepare `isbert_schema.dwtk_meldungen` data in both Oracle and BigQuery with various `timecreated` values, including:
    *   Multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   No entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   `NULL` `timecreated` values (if possible in Oracle schema).
2.  Ensure `sof$ta_disc_zusgf` is empty.

**Action**:
1.  In Oracle, execute the `v_datum` calculation part of `d_ausd_v_ta_disc_zusgf.sql` in isolation (or simulate its logic).
    ```sql
    SELECT COALESCE(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101')
    FROM isbert_schema.dwtk_meldungen m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```
2.  In BigQuery, execute the `v_datum` declaration part of `d_ausd_v_ta_disc_zusgf_transformation.sql` in isolation.
    ```sql
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
    FROM `your_gcp_project_id.isbert_schema.dwtk_meldungen` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```

**Pass/Fail Criterion**:
The `v_datum` value returned by both queries must be identical for each test data scenario.

**Runnable Test Code (Python/Pytest)**:

```python
import pytest
from your_test_utils import execute_oracle_query, execute_bigquery_query, load_oracle_test_data, load_bigquery_test_data

@pytest.mark.parametrize("scenario, expected_v_datum", [
    ("max_timecreated_exists", "20231026"),
    ("no_matching_job_kennung", "19000101"),
    ("null_timecreated_for_job", "19000101"), # Assuming MAX(NULL) is NULL, then COALESCE to default
])
def test_v_datum_calculation_parity(scenario, expected_v_datum):
    """
    Verifies the v_datum calculation matches between Oracle and BigQuery.
    """
    # Load specific test data for the scenario
    load_oracle_test_data(f"dwtk_meldungen_{scenario}", ["isbert_schema.dwtk_meldungen"])
    load_bigquery_test_data(f"dwtk_meldungen_{scenario}", ["isbert_schema.dwtk_meldungen"])

    oracle_query = """
        SELECT COALESCE(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101')
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    bigquery_query = """
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
        FROM `your_gcp_project_id.isbert_schema.dwtk_meldungen` m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """

    oracle_result = execute_oracle_query(oracle_query).iloc[0, 0]
    bigquery_result = execute_bigquery_query(bigquery_query).iloc[0, 0]

    assert str(oracle_result) == expected_v_datum, \
        f"Oracle v_datum mismatch for scenario {scenario}: Expected {expected_v_datum}, Got {oracle_result}"
    assert str(bigquery_result) == expected_v_datum, \
        f"BigQuery v_datum mismatch for scenario {scenario}: Expected {expected_v_datum}, Got {bigquery_result}"
    assert str(oracle_result) == str(bigquery_result), \
        f"v_datum parity failed for scenario {scenario}: Oracle '{oracle_result}', BigQuery '{bigquery_result}'"

    print(f"v_datum calculation parity confirmed for scenario: {scenario}")

```

---

### Test Case 4: Discount Aggregation Logic (Transformation Correctness)

**Purpose**: Validate the `STRING_AGG` functionality in BigQuery correctly replicates the Oracle pipelined function's concatenation logic, including ordering and handling of multiple discounts.

**Setup**:
1.  Prepare `sof$ta_discount` data in both Oracle and BigQuery with various scenarios:
    *   A contract with a single discount.
    *   A contract with multiple discounts, ensuring `rabatt_text` ordering (`ORDER BY rabatt_text`).
    *   A contract with `NULL` values for `rabatt` or `rabatthoehe` (to check `CONCAT` behavior).
    *   A contract with no discounts (should not appear in the aggregated output).
2.  Ensure `sof$ta_disc_zusgf` is empty.

**Action**:
1.  Execute the legacy job.
2.  Execute the migrated job.
3.  Query `sof$ta_disc_zusgf` from both databases, focusing on the `rabatt_alle` column for specific `cntrct_id`, `cntrct_obj_version` combinations.

**Pass/Fail Criterion**:
The `rabatt_alle` string generated by BigQuery must be identical to the one generated by Oracle for each `cntrct_id`, `cntrct_obj_version` pair.

**Runnable Test Code (Python/Pytest)**:

```python
import pytest
from your_test_utils import (
    load_oracle_test_data, clear_oracle_table, get_oracle_data,
    load_bigquery_test_data, clear_bigquery_table, get_bigquery_data,
    execute_legacy_ksh_script, trigger_airflow_dag
)
import pandas as pd

@pytest.mark.parametrize("scenario, cntrct_id, cntrct_obj_version, expected_rabatt_alle", [
    ("single_discount", 1001, 1, "DISC1 (10%)"),
    ("multiple_discounts_ordered", 1002, 1, "DISC1 (5%), DISC2 (15%), DISC3 (20%)"), # Assumes alphabetical order
    ("null_rabatt_value", 1003, 1, " (12%)"), # CONCAT(' (', '12', '%)')
    ("null_rabatthoehe_value", 1004, 1, "DISC4 ()"), # CONCAT('DISC4 (', '', '%)')
    ("no_discounts_for_contract", 1005, 1, None), # Should not have an entry in rabatt_alle
])
def test_discount_aggregation_logic(scenario, cntrct_id, cntrct_obj_version, expected_rabatt_alle):
    """
    Verifies the discount aggregation (STRING_AGG) logic.
    """
    oracle_target_table = "SOF$TA_DISC_ZUSGF"
    bigquery_target_table = "your_gcp_project_id.raw_sof.sof$ta_disc_zusgf"

    # Load specific test data for sof$ta_discount
    load_oracle_test_data(f"sof_ta_discount_{scenario}", ["sof$ta_discount"])
    load_bigquery_test_data(f"sof_ta_discount_{scenario}", ["raw_sof.sof$ta_discount"])
    # Also need some dummy data for dwtk_meldungen to allow job to run
    load_oracle_test_data("dwtk_meldungen_default", ["isbert_schema.dwtk_meldungen"])
    load_bigquery_test_data("dwtk_meldungen_default", ["isbert_schema.dwtk_meldungen"])


    clear_oracle_table(oracle_target_table)
    clear_bigquery_table(bigquery_target_table)

    # Execute both jobs
    execute_legacy_ksh_script("k_ausd_v_ta_disc_zusgf.ksh", {"j": "TEST_AGG", "f": "1"})
    trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", {"job_kennung": "TEST_AGG", "entry_nr": "1"})

    # Extract results for the specific contract
    oracle_rabatt_alle = get_oracle_data(
        f"SELECT RABATT_ALLE FROM {oracle_target_table} WHERE CNTRCT_ID = {cntrct_id} AND CNTRCT_OBJ_VERSION = {cntrct_obj_version}"
    ).iloc[0, 0] if not get_oracle_data(f"SELECT 1 FROM {oracle_target_table} WHERE CNTRCT_ID = {cntrct_id} AND CNTRCT_OBJ_VERSION = {cntrct_obj_version}").empty else None

    bigquery_rabatt_alle = get_bigquery_data(
        f"SELECT rabatt_alle FROM {bigquery_target_table} WHERE cntrct_id = {cntrct_id} AND cntrct_obj_version = {cntrct_obj_version}"
    ).iloc[0, 0] if not get_bigquery_data(f"SELECT 1 FROM {bigquery_target_table} WHERE cntrct_id = {cntrct_id} AND cntrct_obj_version = {cntrct_obj_version}").empty else None

    assert oracle_rabatt_alle == expected_rabatt_alle, \
        f"Oracle rabatt_alle mismatch for scenario {scenario}: Expected '{expected_rabatt_alle}', Got '{oracle_rabatt_alle}'"
    assert bigquery_rabatt_alle == expected_rabatt_alle, \
        f"BigQuery rabatt_alle mismatch for scenario {scenario}: Expected '{expected_rabatt_alle}', Got '{bigquery_rabatt_alle}'"
    assert oracle_rabatt_alle == bigquery_rabatt_alle, \
        f"rabatt_alle parity failed for scenario {scenario}: Oracle '{oracle_rabatt_alle}', BigQuery '{bigquery_rabatt_alle}'"

    print(f"Discount aggregation logic parity confirmed for scenario: {scenario}")

```

---

### Test Case 5: Join Logic and `DISTINCT` Handling (Transformation Correctness)

**Purpose**: Verify that the `LEFT JOIN` and `DISTINCT` operations in BigQuery correctly replicate the Oracle logic, especially when dealing with contracts that might have `disc_vector_ty` but no corresponding discounts for aggregation, or vice-versa.

**Setup**:
1.  Prepare `sof$ta_discount` data in both Oracle and BigQuery with scenarios:
    *   Contracts with multiple `disc_vector_ty` values for the same `cntrct_id`/`cntrct_obj_version`.
    *   Contracts with discounts but no `disc_vector_ty` (if `disc_vector_ty` can be NULL).
    *   Contracts with `disc_vector_ty` but no `rabatt`/`rabatthoehe` entries.
    *   Contracts that exist in `sof$ta_discount` but have no discount details (i.e., only `cntrct_id`, `cntrct_obj_version`, `disc_vector_ty` are present, but no `rabatt` rows).

**Action**:
1.  Execute the legacy job.
2.  Execute the migrated job.
3.  Compare the full output of `sof$ta_disc_zusgf` as in Test Case 2, but with a specific focus on the `disc_vector_ty` and `rabatt_alle` columns for the prepared scenarios.

**Pass/Fail Criterion**:
The `cntrct_id`, `cntrct_obj_version`, `disc_vector_ty`, and `rabatt_alle` combinations in the BigQuery target table must be identical to the Oracle target table. Specifically, ensure that `DISTINCT` on `(cntrct_id, cntrct_obj_version, disc_vector_ty)` from `sof$ta_discount` forms the left side of the join, and `rabatt_alle` is correctly joined (or `NULL` if no match).

**Runnable Test Code (Python/Pytest)**:
This test is largely covered by Test Case 2 (Full Data Parity) if the comprehensive test data includes these scenarios. However, a specific scenario can be highlighted.

```python
import pytest
from your_test_utils import (
    load_oracle_test_data, clear_oracle_table, get_oracle_data,
    load_bigquery_test_data, clear_bigquery_table, get_bigquery_data,
    execute_legacy_ksh_script, trigger_airflow_dag
)
import pandas as pd

def test_join_and_distinct_logic():
    """
    Verifies the LEFT JOIN and DISTINCT logic, especially for contracts with varying discount data.
    Scenario: Contract 2001 has two distinct disc_vector_ty entries, but only one has discount details.
    """
    oracle_target_table = "SOF$TA_DISC_ZUSGF"
    bigquery_target_table = "your_gcp_project_id.raw_sof.sof$ta_disc_zusgf"

    # Test data for sof$ta_discount:
    # cntrct_id | cntrct_obj_version | disc_vector_ty | rabatt | rabatthoehe
    # ----------|--------------------|----------------|--------|------------
    # 2001      | 1                  | TYPE_A         | DISC_X | 10
    # 2001      | 1                  | TYPE_B         | NULL   | NULL
    # 2002      | 1                  | TYPE_C         | DISC_Y | 20
    # 2002      | 1                  | TYPE_C         | DISC_Z | 30
    # Expected output for 2001,1,TYPE_A: DISC_X (10%)
    # Expected output for 2001,1,TYPE_B: NULL
    # Expected output for 2002,1,TYPE_C: DISC_Y (20%), DISC_Z (30%)

    test_data_scenario = "join_distinct_edge_cases"
    load_oracle_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "sof$ta_discount"])
    load_bigquery_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "raw_sof.sof$ta_discount"])

    clear_oracle_table(oracle_target_table)
    clear_bigquery_table(bigquery_target_table)

    execute_legacy_ksh_script("k_ausd_v_ta_disc_zusgf.ksh", {"j": "TEST_JOIN", "f": "1"})
    trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", {"job_kennung": "TEST_JOIN", "entry_nr": "1"})

    oracle_results_df = get_oracle_data(oracle_target_table)
    bigquery_results_df = get_bigquery_data(bigquery_target_table)

    oracle_results_df.columns = [col.upper() for col in oracle_results_df.columns]
    bigquery_results_df.columns = [col.upper() for col in bigquery_results_df.columns]

    sort_cols = ['CNTRCT_ID', 'CNTRCT_OBJ_VERSION', 'DISC_VECTOR_TY']
    oracle_results_df = oracle_results_df.sort_values(by=sort_cols).reset_index(drop=True)
    bigquery_results_df = bigquery_results_df.sort_values(by=sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(oracle_results_df, bigquery_results_df, check_dtype=True)

    print(f"Join and DISTINCT logic parity confirmed for scenario: {test_data_scenario}")

```

---

### Test Case 6: Idempotency and Truncation (External System Replacement / Data Quality)

**Purpose**: Verify that the `TRUNCATE TABLE` operation in the Airflow DAG correctly clears the target table before insertion, ensuring the job is idempotent and produces consistent results on repeated runs. This replaces the Oracle `DWPA_UTIL_SKRIPT.runstatement` call.

**Setup**:
1.  Prepare a standard test dataset for `isbert_schema.dwtk_meldungen` and `sof$ta_discount`.
2.  Load this dataset into both Oracle and BigQuery source tables.
3.  Ensure both target tables (`sof$ta_disc_zusgf`) are empty initially.

**Action**:
1.  Execute the legacy KornShell script once.
2.  Execute the legacy KornShell script a second time immediately after the first.
3.  Extract data from Oracle `sof$ta_disc_zusgf`.
4.  Trigger the Airflow DAG once.
5.  Trigger the Airflow DAG a second time immediately after the first.
6.  Extract data from BigQuery `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf`.

**Pass/Fail Criterion**:
1.  The data in the Oracle `sof$ta_disc_zusgf` after two runs must be identical to the data after one run (i.e., no duplicate rows, indicating the Oracle job also truncates or handles existing data).
2.  The data in the BigQuery `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf` after two runs must be identical to the data after one run.
3.  The final data in Oracle and BigQuery must be identical (full data parity).

**Runnable Test Code (Python/Pytest)**:

```python
import pytest
from your_test_utils import (
    load_oracle_test_data, clear_oracle_table, get_oracle_data,
    load_bigquery_test_data, clear_bigquery_table, get_bigquery_data,
    execute_legacy_ksh_script, trigger_airflow_dag
)
import pandas as pd

def test_idempotency_and_truncation():
    """
    Verifies that both legacy and migrated jobs are idempotent due to truncation.
    """
    oracle_target_table = "SOF$TA_DISC_ZUSGF"
    bigquery_target_table = "your_gcp_project_id.raw_sof.sof$ta_disc_zusgf"

    test_data_scenario = "standard_idempotency_test"
    load_oracle_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "sof$ta_discount"])
    load_bigquery_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "raw_sof.sof$ta_discount"])

    clear_oracle_table(oracle_target_table)
    clear_bigquery_table(bigquery_target_table)

    # --- Legacy Job Idempotency ---
    execute_legacy_ksh_script("k_ausd_v_ta_disc_zusgf.ksh", {"j": "TEST_IDEM", "f": "1"})
    oracle_results_df_run1 = get_oracle_data(oracle_target_table)

    execute_legacy_ksh_script("k_ausd_v_ta_disc_zusgf.ksh", {"j": "TEST_IDEM", "f": "1"}) # Run again
    oracle_results_df_run2 = get_oracle_data(oracle_target_table)

    pd.testing.assert_frame_equal(oracle_results_df_run1, oracle_results_df_run2, check_dtype=True,
                                  obj="Legacy Oracle job is not idempotent.")

    # --- Migrated Job Idempotency ---
    trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", {"job_kennung": "TEST_IDEM", "entry_nr": "1"})
    bigquery_results_df_run1 = get_bigquery_data(bigquery_target_table)

    trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", {"job_kennung": "TEST_IDEM", "entry_nr": "1"}) # Run again
    bigquery_results_df_run2 = get_bigquery_data(bigquery_target_table)

    pd.testing.assert_frame_equal(bigquery_results_df_run1, bigquery_results_df_run2, check_dtype=True,
                                  obj="Migrated BigQuery job is not idempotent.")

    # --- Final Parity Check ---
    oracle_results_df_run2.columns = [col.upper() for col in oracle_results_df_run2.columns]
    bigquery_results_df_run2.columns = [col.upper() for col in bigquery_results_df_run2.columns]

    sort_cols = ['CNTRCT_ID', 'CNTRCT_OBJ_VERSION', 'DISC_VECTOR_TY']
    oracle_results_df_run2 = oracle_results_df_run2.sort_values(by=sort_cols).reset_index(drop=True)
    bigquery_results_df_run2 = bigquery_results_df_run2.sort_values(by=sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(oracle_results_df_run2, bigquery_results_df_run2, check_dtype=True,
                                  obj="Final data parity failed after idempotent runs.")

    print("Idempotency and truncation confirmed for both legacy and migrated jobs.")

```

---

### Test Case 7: Row Count Parity (Data Quality / Row Count Assertions)

**Purpose**: Verify that the total number of rows inserted into `sof$ta_disc_zusgf` is identical between the legacy and migrated jobs. This is a quick sanity check for overall data volume.

**Setup**:
1.  Prepare a standard test dataset for `isbert_schema.dwtk_meldungen` and `sof$ta_discount`.
2.  Load this dataset into both Oracle and BigQuery source tables.
3.  Ensure both target tables (`sof$ta_disc_zusgf`) are empty before execution.

**Action**:
1.  Execute the legacy KornShell script.
2.  Count rows in Oracle `sof$ta_disc_zusgf`.
3.  Trigger the Airflow DAG.
4.  Count rows in BigQuery `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf`.

**Pass/Fail Criterion**:
The row count in the BigQuery target table must be exactly equal to the row count in the Oracle target table.

**Runnable Test Code (Python/Pytest)**:

```python
import pytest
from your_test_utils import (
    load_oracle_test_data, clear_oracle_table, get_oracle_row_count,
    load_bigquery_test_data, clear_bigquery_table, get_bigquery_row_count,
    execute_legacy_ksh_script, trigger_airflow_dag
)

def test_row_count_parity():
    """
    Compares the row count of the target table in Oracle and BigQuery.
    """
    oracle_target_table = "SOF$TA_DISC_ZUSGF"
    bigquery_target_table = "your_gcp_project_id.raw_sof.sof$ta_disc_zusgf"

    test_data_scenario = "standard_row_count_test"
    load_oracle_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "sof$ta_discount"])
    load_bigquery_test_data(test_data_scenario, ["isbert_schema.dwtk_meldungen", "raw_sof.sof$ta_discount"])

    clear_oracle_table(oracle_target_table)
    clear_bigquery_table(bigquery_target_table)

    execute_legacy_ksh_script("k_ausd_v_ta_disc_zusgf.ksh", {"j": "TEST_COUNT", "f": "1"})
    oracle_row_count = get_oracle_row_count(oracle_target_table)

    trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", {"job_kennung": "TEST_COUNT", "entry_nr": "1"})
    bigquery_row_count = get_bigquery_row_count(bigquery_target_table)

    assert oracle_row_count == bigquery_row_count, \
        f"Row count mismatch: Oracle has {oracle_row_count} rows, BigQuery has {bigquery_row_count} rows."

    print(f"Row count parity confirmed: {oracle_row_count} rows in both systems.")

```

---

### Test Case 8: Parameter Handling (External System Replacement)

**Purpose**: Verify that the Airflow DAG correctly defines and handles the parameters (`job_kennung`, `entry_nr`) that were originally passed to the KornShell script. While the provided BigQuery SQL doesn't directly use these parameters, the DAG definition includes them, and the original KSH script used them for `starteSQLSkript` and logging. This test ensures the orchestration layer correctly receives and makes available these parameters, even if the SQL itself doesn't consume them.

**Setup**:
1.  Ensure the Airflow DAG is deployed.
2.  Have a mechanism to inspect Airflow task logs or XComs.

**Action**:
1.  Trigger the Airflow DAG (`k_ausd_v_ta_disc_zusgf_dag`) with specific, non-default values for `job_kennung` and `entry_nr` (e.g., `job_kennung="MY_CUSTOM_JOB"`, `entry_nr="999"`).
2.  Inspect the Airflow task logs for the `truncate_target_table` or `execute_main_transformation` tasks, or use XComs if the DAG were modified to push these parameters.

**Pass/Fail Criterion**:
The Airflow DAG must successfully execute without errors related to parameter parsing. If the DAG were modified to log or use these parameters, their values in the logs/XComs must match the input values. (Note: Based on the provided DAG, the parameters are defined but not explicitly used in the BigQuery operators. A more robust test would involve modifying the DAG to log or use these parameters in a PythonOperator to confirm their availability.)

**Runnable Test Code (Conceptual - requires DAG modification for full verification)**:

```python
import pytest
from your_test_utils import trigger_airflow_dag, get_airflow_task_logs

def test_airflow_parameter_handling():
    """
    Verifies that Airflow DAG parameters are correctly passed and available.
    (Note: This test assumes the DAG is modified to log or use these parameters
    within a PythonOperator for full verification, as the current BQ operators
    do not directly consume them).
    """
    custom_job_kennung = "CUSTOM_JOB_ABC"
    custom_entry_nr = "789"

    airflow_dag_params = {
        "job_kennung": custom_job_kennung,
        "entry_nr": custom_entry_nr
    }

    # Trigger the DAG
    dag_run_id = trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", airflow_dag_params)

    # Check if the DAG run completed successfully
    assert dag_run_id is not None, "Airflow DAG failed to trigger or complete."

    # --- Conceptual Check (requires DAG modification) ---
    # If the DAG had a PythonOperator like this:
    # def log_params(**kwargs):
    #     ti = kwargs['ti']
    #     job_id = ti.xcom_pull(task_ids='start', key='job_kennung') # Example if pushed to XCom
    #     entry_num = kwargs['params']['entry_nr'] # Direct access
    #     print(f"Job Kennung: {job_id}, Entry Nr: {entry_num}")
    #
    # log_params_task = PythonOperator(task_id='log_params', python_callable=log_params, provide_context=True)
    #
    # Then you would check the logs of 'log_params_task'
    # logs = get_airflow_task_logs(dag_run_id, 'log_params_task')
    # assert f"Job Kennung: {custom_job_kennung}" in logs
    # assert f"Entry Nr: {custom_entry_nr}" in logs

    print(f"Airflow DAG triggered successfully with parameters: {airflow_dag_params}")
    print("Manual verification of Airflow logs for parameter usage is recommended.")

```