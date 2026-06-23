As a senior data-migration QA engineer, I have analyzed the provided migration design document and the legacy/migrated code for `r_ausd_bp_ta_bcp_msisdn.ksh`.

A critical observation is that the "Transformation Logic" section of the design document, and the provided `d_ausd_bp_ta_bcp_msisdn.sql` and its BigQuery translation, *do not include any filtering logic based on `stichtag`*, despite the "Beschreibung" in `r_ausd_bp_ta_bcp_msisdn.ksh` stating: "Es werden jeweils Records selektiert, fuer die Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag gilt."

For the purpose of these validation tests, I will assume that the *provided SQL code* (`d_ausd_bp_ta_bcp_msisdn.sql`) represents the definitive transformation logic to be migrated. If the `stichtag` filtering described in the KSH script's comments is indeed a required functional aspect, then the provided SQL is incomplete, and the migration based on this SQL would also be functionally incomplete. This should be clarified with the business/source system owners. My tests will validate the migration of the *actual SQL provided*.

The tests are organized into categories covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests: `r_ausd_bp_ta_bcp_msisdn.ksh` to Airflow/BigQuery

### Setup Prerequisites for All Tests

Before running any tests, ensure the following:

1.  **Data Migration:**
    *   `isbert_schema.dwtk_meldungen` (Oracle) has been migrated to `project_id.isbert_schema.dwtk_meldungen_bq` (BigQuery).
    *   `sof$ta_bpr_bcp` (Oracle) has been migrated to `project_id.sof_schema.ta_bpr_bcp_bq` (BigQuery).
    *   `sof$ta_rn_vertrag` (Oracle) has been migrated to `project_id.sof_schema.ta_rn_vertrag_bq` (BigQuery).
2.  **Target Table Creation:**
    *   An empty BigQuery table `project_id.sof_schema.ta_bcp_msisdn_bq` exists with the expected schema: `CNTRCT_ID` (INT64), `BPR_ID` (INT64), `CNTRCT_ID_REF` (INT64), `TN_TEL_MSISDN` (STRING).
3.  **Test Environment:**
    *   Access to the legacy Oracle database and the ability to execute the `.ksh` scripts.
    *   Access to the GCP BigQuery environment and the ability to trigger the Airflow DAG.
    *   A Python environment with `pytest` and necessary BigQuery client libraries configured.
    *   Helper functions/scripts to:
        *   `clear_oracle_target_table()`: Truncate `sof$ta_bcp_msisdn`.
        *   `insert_oracle_data(table_name, data)`: Insert test data into Oracle tables.
        *   `fetch_oracle_data(table_name)`: Fetch all data from an Oracle table.
        *   `run_legacy_job(stichtag=None, wiederanlaufwert=None)`: Execute `r_ausd_bp_ta_bcp_msisdn.ksh` with parameters.
        *   `clear_bq_target_table()`: Truncate `project_id.sof_schema.ta_bcp_msisdn_bq`.
        *   `insert_bq_data(table_name, data)`: Insert test data into BigQuery tables.
        *   `fetch_bq_data(table_name)`: Fetch all data from a BigQuery table.
        *   `run_airflow_dag(dag_id, conf={})`: Trigger the Airflow DAG with parameters.

---

### 1. Output Parity & Transformation Correctness

These tests focus on ensuring the core data transformation logic produces identical results.

#### Test Case 1.1: Happy Path - Full Data Match

*   **Purpose:** Verify that with typical input data, the migrated job produces an identical output dataset (row count and content) to the legacy job. This is the primary end-to-end parity check.
*   **Setup:**
    1.  Clear both legacy Oracle target table (`sof$ta_bcp_msisdn`) and BigQuery target table (`sof_schema.ta_bcp_msisdn_bq`).
    2.  Populate `sof$ta_bpr_bcp` (Oracle) and `sof_schema.ta_bpr_bcp_bq` (BigQuery) with identical sample data.
    3.  Populate `sof$ta_rn_vertrag` (Oracle) and `sof_schema.ta_rn_vertrag_bq` (BigQuery) with identical sample data, ensuring some matching `CNTRCT_ID_REF` and `CNTRCT_ID` values.
    4.  Populate `isbert_schema.dwtk_meldungen` (Oracle) and `isbert_schema.dwtk_meldungen_bq` (BigQuery) with a record for `BERT_DROP_TEMP_TABLE` to ensure `s_datum` is retrieved.
*   **Action:**
    1.  Execute the legacy job: `run_legacy_job(stichtag='20230101')`.
    2.  Execute the migrated Airflow DAG: `run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})`.
    3.  Fetch all data from `sof$ta_bcp_msisdn` (Oracle) and `sof_schema.ta_bcp_msisdn_bq` (BigQuery).
*   **Pass/Fail Criterion:**
    *   The number of rows in the Oracle target table must be equal to the number of rows in the BigQuery target table.
    *   The content (all columns) of the two result sets, when sorted identically, must be exactly the same.

```python
import pytest
import pandas as pd
from your_test_utils import (
    clear_oracle_target_table, insert_oracle_data, fetch_oracle_data, run_legacy_job,
    clear_bq_target_table, insert_bq_data, fetch_bq_data, run_airflow_dag
)

PROJECT_ID = "gcp-project-id"

@pytest.fixture(autouse=True)
def setup_teardown_tables():
    # Clear target tables before each test
    clear_oracle_target_table()
    clear_bq_target_table()
    yield
    # Optional: Clear source tables after tests if they are modified, or if you want fresh state
    # clear_oracle_source_tables()
    # clear_bq_source_tables()

def test_happy_path_full_data_match():
    # Setup source data
    bpr_bcp_data = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001},
        {'CNTRCT_ID': 102, 'BPR_ID': 2, 'CNTRCT_ID_REF': 1002},
        {'CNTRCT_ID': 103, 'BPR_ID': 3, 'CNTRCT_ID_REF': 1001}, # Duplicate CNTRCT_ID_REF
        {'CNTRCT_ID': 104, 'BPR_ID': 4, 'CNTRCT_ID_REF': 1003},
    ]
    rn_vertrag_data = [
        {'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '1234567890'},
        {'CNTRCT_ID': 1002, 'TN_TEL_MSISDN': '0987654321'},
        {'CNTRCT_ID': 1004, 'TN_TEL_MSISDN': '1122334455'}, # No matching CNTRCT_ID_REF in bpr_bcp
    ]
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}
    ]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action
    run_legacy_job(stichtag='20230101')
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    # Fetch results
    oracle_result = fetch_oracle_data('sof$ta_bcp_msisdn')
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')

    # Pass/Fail Criterion
    assert len(oracle_result) == len(bq_result), "Row counts do not match"
    
    # Sort and compare dataframes
    oracle_df = pd.DataFrame(oracle_result).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)
    bq_df = pd.DataFrame(bq_result).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)

    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False) # check_dtype=False due to potential int/float differences
```

#### Test Case 1.2: No Matching Join Keys

*   **Purpose:** Verify correct handling when `bp.CNTRCT_ID_REF` has no corresponding `rn.CNTRCT_ID`, resulting in zero output rows.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate `sof$ta_bpr_bcp` and `sof_schema.ta_bpr_bcp_bq` with data where `CNTRCT_ID_REF` values have no matches in `sof$ta_rn_vertrag` / `sof_schema.ta_rn_vertrag_bq`.
    3.  Populate `sof$ta_rn_vertrag` and `sof_schema.ta_rn_vertrag_bq` with data where `CNTRCT_ID` values have no matches in `sof$ta_bpr_bcp` / `sof_schema.ta_bpr_bcp_bq`.
    4.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute legacy job.
    2.  Execute migrated DAG.
    3.  Fetch results.
*   **Pass/Fail Criterion:** Both target tables must be empty (0 rows).

```python
def test_no_matching_join_keys():
    bpr_bcp_data = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 9001}, # No match
        {'CNTRCT_ID': 102, 'BPR_ID': 2, 'CNTRCT_ID_REF': 9002}, # No match
    ]
    rn_vertrag_data = [
        {'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '1234567890'},
        {'CNTRCT_ID': 1002, 'TN_TEL_MSISDN': '0987654321'},
    ]
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}
    ]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    run_legacy_job(stichtag='20230101')
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    oracle_result = fetch_oracle_data('sof$ta_bcp_msisdn')
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')

    assert len(oracle_result) == 0, "Oracle target table should be empty"
    assert len(bq_result) == 0, "BigQuery target table should be empty"
```

#### Test Case 1.3: Duplicates in Source Tables

*   **Purpose:** Verify the `DISTINCT` clause correctly eliminates duplicate rows from the final output.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate source tables such that the join operation would produce duplicate rows *before* the `DISTINCT` clause is applied. For example, multiple `bp` records joining to the same `rn` record, resulting in identical output rows.
    3.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute legacy job.
    2.  Execute migrated DAG.
    3.  Fetch results.
*   **Pass/Fail Criterion:**
    *   The number of rows in both target tables must be equal and reflect the count *after* `DISTINCT` is applied.
    *   The content of the two result sets must be identical.

```python
def test_duplicates_in_source_tables():
    bpr_bcp_data = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001},
        {'CNTRCT_ID': 102, 'BPR_ID': 2, 'CNTRCT_ID_REF': 1001}, # This will create a duplicate output row with 101 if not for DISTINCT
        {'CNTRCT_ID': 103, 'BPR_ID': 3, 'CNTRCT_ID_REF': 1002},
    ]
    rn_vertrag_data = [
        {'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '1234567890'},
        {'CNTRCT_ID': 1002, 'TN_TEL_MSISDN': '0987654321'},
    ]
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}
    ]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    run_legacy_job(stichtag='20230101')
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    oracle_result = fetch_oracle_data('sof$ta_bcp_msisdn')
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')

    # Expected distinct output rows:
    # (101, 1, 1001, '1234567890')
    # (102, 2, 1001, '1234567890')
    # (103, 3, 1002, '0987654321')
    # Note: The DISTINCT applies to the *selected columns*. If CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN are all unique, then no rows are removed.
    # Let's adjust data to force a true duplicate in the *output*
    bpr_bcp_data_for_distinct = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001},
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001}, # Identical source row
    ]
    rn_vertrag_data_for_distinct = [
        {'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '1234567890'},
    ]
    clear_oracle_target_table()
    clear_bq_target_table()
    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data_for_distinct)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data_for_distinct)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data_for_distinct)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data_for_distinct)

    run_legacy_job(stichtag='20230101')
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    oracle_result_distinct = fetch_oracle_data('sof$ta_bcp_msisdn')
    bq_result_distinct = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')

    assert len(oracle_result_distinct) == 1, "Oracle target table should have 1 distinct row"
    assert len(bq_result_distinct) == 1, "BigQuery target table should have 1 distinct row"
    
    oracle_df = pd.DataFrame(oracle_result_distinct).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)
    bq_df = pd.DataFrame(bq_result_distinct).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)
    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False)
```

#### Test Case 1.4: NULLs in Join Keys

*   **Purpose:** Verify that rows with `NULL` values in `CNTRCT_ID_REF` (from `ta_bpr_bcp`) or `CNTRCT_ID` (from `ta_rn_vertrag`) are correctly excluded from the join result, as Oracle's `WHERE` clause treats `NULL = NULL` as unknown (false).
*   **Setup:**
    1.  Clear target tables.
    2.  Populate `sof$ta_bpr_bcp` and `sof_schema.ta_bpr_bcp_bq` with records where `CNTRCT_ID_REF` is `NULL`.
    3.  Populate `sof$ta_rn_vertrag` and `sof_schema.ta_rn_vertrag_bq` with records where `CNTRCT_ID` is `NULL`.
    4.  Include some valid join data to ensure other data is processed.
    5.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute legacy job.
    2.  Execute migrated DAG.
    3.  Fetch results.
*   **Pass/Fail Criterion:**
    *   Rows with `NULL` in the join keys must not appear in the output of either job.
    *   The output from both jobs must be identical.

```python
def test_nulls_in_join_keys():
    bpr_bcp_data = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001},
        {'CNTRCT_ID': 102, 'BPR_ID': 2, 'CNTRCT_ID_REF': None}, # NULL in join key
    ]
    rn_vertrag_data = [
        {'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '1234567890'},
        {'CNTRCT_ID': None, 'TN_TEL_MSISDN': '0000000000'}, # NULL in join key
    ]
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}
    ]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    run_legacy_job(stichtag='20230101')
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    oracle_result = fetch_oracle_data('sof$ta_bcp_msisdn')
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')

    # Expected output: only the row where CNTRCT_ID_REF = 1001 and CNTRCT_ID = 1001
    expected_output = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001, 'TN_TEL_MSISDN': '1234567890'}
    ]
    
    assert len(oracle_result) == len(expected_output), "Oracle target table row count mismatch"
    assert len(bq_result) == len(expected_output), "BigQuery target table row count mismatch"

    oracle_df = pd.DataFrame(oracle_result).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)
    bq_df = pd.DataFrame(bq_result).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)
    expected_df = pd.DataFrame(expected_output).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)

    pd.testing.assert_frame_equal(oracle_df, expected_df, check_dtype=False)
    pd.testing.assert_frame_equal(bq_df, expected_df, check_dtype=False)
```

#### Test Case 1.5: Empty Source Tables

*   **Purpose:** Verify the job gracefully handles empty input tables, resulting in an empty target table.
*   **Setup:**
    1.  Clear target tables.
    2.  Ensure `sof$ta_bpr_bcp` / `sof_schema.ta_bpr_bcp_bq` and `sof$ta_rn_vertrag` / `sof_schema.ta_rn_vertrag_bq` are empty.
    3.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute legacy job.
    2.  Execute migrated DAG.
    3.  Fetch results.
*   **Pass/Fail Criterion:** Both target tables must be empty (0 rows).

```python
def test_empty_source_tables():
    # Source tables are already empty due to setup_teardown_tables fixture
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}
    ]
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    run_legacy_job(stichtag='20230101')
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    oracle_result = fetch_oracle_data('sof$ta_bcp_msisdn')
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')

    assert len(oracle_result) == 0, "Oracle target table should be empty"
    assert len(bq_result) == 0, "BigQuery target table should be empty"
```

---

### 2. External System Replacements & Orchestration

These tests validate the correct functioning of components that replace legacy helper scripts and Oracle-specific features.

#### Test Case 2.1: `s_datum` Retrieval - Normal Case

*   **Purpose:** Verify the `s_datum` value is correctly retrieved from `dwtk_meldungen_bq` when a matching record exists, mirroring Oracle's `MAX(m.timecreated)` and `NVL` logic.
*   **Setup:**
    1.  Clear `isbert_schema.dwtk_meldungen` and `isbert_schema.dwtk_meldungen_bq`.
    2.  Insert multiple records into both tables for `JOB_KENNUNG = 'BERT_DROP_TEMP_TABLE'` with varying `TIMECREATED` values.
*   **Action:**
    1.  Execute the `retrieve_s_datum` task of the Airflow DAG directly (or run the full DAG and inspect XComs).
    2.  Manually query Oracle for `s_datum` using the legacy logic.
*   **Pass/Fail Criterion:** The `s_datum` value pushed to XCom by the Airflow task must match the `s_datum` value retrieved from Oracle.

```python
def test_s_datum_retrieval_normal_case():
    # Setup source data
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'OTHER_JOB', 'TIMECREATED': pd.Timestamp('2022-12-31 08:00:00')},
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')},
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-02 12:30:00')}, # Max timecreated
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 09:00:00')},
    ]
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action (simulating Airflow task execution)
    # This would typically be done by running the DAG and inspecting XComs,
    # or by unit testing the @task function directly if possible.
    # For demonstration, we'll simulate the BigQuery query directly.
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = bq_hook.get_client(project_id=PROJECT_ID)
    bq_query = f"""
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
        FROM
            `{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq` m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    bq_s_datum = client.query(bq_query).result().to_dataframe().iloc[0]['s_datum']

    # Manually query Oracle (or use a helper function)
    oracle_query = """
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    oracle_s_datum = fetch_oracle_data_raw(oracle_query)[0]['S_DATUM'] # Assuming fetch_oracle_data_raw returns list of dicts

    # Pass/Fail Criterion
    assert bq_s_datum == '20230102', "BigQuery s_datum mismatch"
    assert oracle_s_datum == '20230102', "Oracle s_datum mismatch"
    assert bq_s_datum == oracle_s_datum, "s_datum values from Oracle and BigQuery do not match"
```

#### Test Case 2.2: `s_datum` Retrieval - No Matching Record

*   **Purpose:** Verify `s_datum` defaults to '19000101' when no matching `job_kennung` is found in `dwtk_meldungen_bq`, mirroring Oracle's `NVL` behavior.
*   **Setup:**
    1.  Clear `isbert_schema.dwtk_meldungen` and `isbert_schema.dwtk_meldungen_bq`.
    2.  Insert records for a *different* `JOB_KENNUNG`, or leave the table empty.
*   **Action:**
    1.  Execute the `retrieve_s_datum` task of the Airflow DAG.
    2.  Manually query Oracle for `s_datum` using the legacy logic.
*   **Pass/Fail Criterion:** Both the Airflow task's `s_datum` and the Oracle query result must be '19000101'.

```python
def test_s_datum_retrieval_no_matching_record():
    # Setup source data (no 'BERT_DROP_TEMP_TABLE' records)
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'OTHER_JOB', 'TIMECREATED': pd.Timestamp('2022-12-31 08:00:00')},
    ]
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action (simulating BigQuery query)
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = bq_hook.get_client(project_id=PROJECT_ID)
    bq_query = f"""
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
        FROM
            `{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq` m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    bq_s_datum = client.query(bq_query).result().to_dataframe().iloc[0]['s_datum']

    # Manually query Oracle
    oracle_query = """
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    oracle_s_datum = fetch_oracle_data_raw(oracle_query)[0]['S_DATUM']

    # Pass/Fail Criterion
    assert bq_s_datum == '19000101', "BigQuery s_datum should default to '19000101'"
    assert oracle_s_datum == '19000101', "Oracle s_datum should default to '19000101'"
    assert bq_s_datum == oracle_s_datum, "s_datum values from Oracle and BigQuery do not match"
```

#### Test Case 2.3: Target Table Truncation

*   **Purpose:** Verify that the target table `sof_schema.ta_bcp_msisdn_bq` is truncated before data insertion, replacing the `DWPA_UTIL_SKRIPT.runstatement` call.
*   **Setup:**
    1.  Populate `sof$ta_bcp_msisdn` (Oracle) and `sof_schema.ta_bcp_msisdn_bq` (BigQuery) with some initial data.
    2.  Populate source tables (`ta_bpr_bcp`, `ta_rn_vertrag`) with data that would result in *different* output rows than the initial data.
    3.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute the legacy job.
    2.  Execute the migrated DAG.
    3.  Fetch data from both target tables.
*   **Pass/Fail Criterion:**
    *   The final content of both target tables must reflect *only* the data inserted by the current run, not a combination of initial data and new data.
    *   The row count should match the expected output from the current run, not the initial + current.

```python
def test_target_table_truncation():
    # Setup initial data in target tables
    initial_target_data = [
        {'CNTRCT_ID': 999, 'BPR_ID': 99, 'CNTRCT_ID_REF': 9999, 'TN_TEL_MSISDN': '9999999999'}
    ]
    insert_oracle_data('sof$ta_bcp_msisdn', initial_target_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq', initial_target_data)

    # Setup source data that will be inserted after truncation
    bpr_bcp_data = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001},
    ]
    rn_vertrag_data = [
        {'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '1234567890'},
    ]
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}
    ]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action
    run_legacy_job(stichtag='20230101')
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    # Fetch results
    oracle_result = fetch_oracle_data('sof$ta_bcp_msisdn')
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')

    expected_output = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001, 'TN_TEL_MSISDN': '1234567890'}
    ]

    # Pass/Fail Criterion
    assert len(oracle_result) == len(expected_output), "Oracle target table row count mismatch (truncation failed or data not inserted)"
    assert len(bq_result) == len(expected_output), "BigQuery target table row count mismatch (truncation failed or data not inserted)"
    
    oracle_df = pd.DataFrame(oracle_result).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)
    bq_df = pd.DataFrame(bq_result).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)
    expected_df = pd.DataFrame(expected_output).sort_values(by=['CNTRCT_ID', 'BPR_ID', 'CNTRCT_ID_REF', 'TN_TEL_MSISDN']).reset_index(drop=True)

    pd.testing.assert_frame_equal(oracle_df, expected_df, check_dtype=False)
    pd.testing.assert_frame_equal(bq_df, expected_df, check_dtype=False)
```

#### Test Case 2.4: Parameter Handling - `stichtag` Default

*   **Purpose:** Verify that if `stichtag` is not provided, the DAG correctly defaults it to yesterday's date (as per migrated code), replacing `gestern.ksh` logic.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate source tables with data.
    3.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute the legacy job *without* the `-s` parameter: `run_legacy_job()`.
    2.  Execute the migrated Airflow DAG *without* the `stichtag` parameter: `run_airflow_dag('bert_bp_ta_bcp_msisdn_dag')`.
    3.  Fetch the `stichtag` value from the Airflow DAG's XComs (from `get_stichtag_and_wiederanlaufwert` task).
    4.  Determine the `stichtag` used by the legacy job (this might require inspecting logs or modifying the KSH script for logging).
*   **Pass/Fail Criterion:**
    *   The `stichtag` value derived by the Airflow DAG must be `(today - 1 day)` in `YYYYMMDD` format.
    *   **NOTE ON DISCREPANCY:** The legacy KSH script defaults `stichtag` to `MIN(sysdate, max_load_date)` or `sysdate`. The migrated DAG defaults to `yesterday`. This is a functional difference that needs to be explicitly accepted or corrected. For this test, we validate the *migrated* behavior. If the legacy default was critical, this test would fail.

```python
import pendulum

def test_stichtag_default_behavior():
    # Setup minimal source data for job to run
    bpr_bcp_data = [{'CNTRCT_ID': 1, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001}]
    rn_vertrag_data = [{'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '123'}]
    dwtk_meldungen_data = [{'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action
    # Run legacy job without -s. Its default logic is complex (MIN(sysdate, max_load_date) or sysdate).
    # For this test, we focus on the migrated DAG's default.
    # The legacy job's actual 'stichtag' used would need to be extracted from its logs.
    # For the purpose of this test, we acknowledge the functional difference in default.
    # run_legacy_job() # This would run, but its stichtag might differ from migrated.

    dag_run_id = run_airflow_dag('bert_bp_ta_bcp_msisdn_dag') # No conf means no stichtag param

    # Fetch stichtag from XComs
    # This requires Airflow API access or direct XCom inspection
    # For a unit test, you might call the @task function directly and inspect its return/pushed XCom
    from airflow.models import DagRun
    from airflow.utils.session import provide_session
    
    @provide_session
    def get_xcom_value(dag_id, task_id, key, session=None):
        dr = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id).first()
        if dr:
            ti = dr.get_task_instance(task_id, session=session)
            if ti:
                return ti.xcom_pull(task_ids=task_id, key=key)
        return None

    actual_stichtag = get_xcom_value('bert_bp_ta_bcp_msisdn_dag', 'get_stichtag_and_wiederanlaufwert', 'stichtag')

    expected_stichtag = (pendulum.today("UTC") - pendulum.duration(days=1)).strftime("%Y%m%d")

    # Pass/Fail Criterion
    assert actual_stichtag == expected_stichtag, f"Default stichtag mismatch. Expected {expected_stichtag}, got {actual_stichtag}"

    # Verify data was processed (even if stichtag doesn't filter the core SQL)
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')
    assert len(bq_result) > 0, "BigQuery job did not process data with default stichtag"

    # NOTE: The legacy KSH script's default stichtag calculation is different.
    # This test validates the *migrated* DAG's default behavior.
    # A separate discussion is needed if the legacy default logic is critical.
```

#### Test Case 2.5: Parameter Handling - `stichtag` Provided

*   **Purpose:** Verify that if `stichtag` is provided, the DAG uses the provided value.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate source tables with data.
    3.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute the legacy job with a specific `stichtag`: `run_legacy_job(stichtag='20230315')`.
    2.  Execute the migrated Airflow DAG with the same `stichtag`: `run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230315'})`.
    3.  Fetch the `stichtag` value from the Airflow DAG's XComs.
*   **Pass/Fail Criterion:** The `stichtag` value pushed to XCom by the Airflow task must match the provided input `stichtag`.

```python
def test_stichtag_provided_behavior():
    # Setup minimal source data for job to run
    bpr_bcp_data = [{'CNTRCT_ID': 1, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001}]
    rn_vertrag_data = [{'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '123'}]
    dwtk_meldungen_data = [{'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action
    provided_stichtag = '20230315'
    run_legacy_job(stichtag=provided_stichtag)
    dag_run_id = run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': provided_stichtag})

    actual_stichtag = get_xcom_value('bert_bp_ta_bcp_msisdn_dag', 'get_stichtag_and_wiederanlaufwert', 'stichtag')

    # Pass/Fail Criterion
    assert actual_stichtag == provided_stichtag, f"Provided stichtag mismatch. Expected {provided_stichtag}, got {actual_stichtag}"

    # Verify data was processed
    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')
    assert len(bq_result) > 0, "BigQuery job did not process data with provided stichtag"
```

#### Test Case 2.6: Parameter Handling - `wiederanlaufwert`

*   **Purpose:** Verify `wiederanlaufwert` is correctly parsed and passed, even if not used in core SQL.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate source tables with data.
    3.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute the legacy job with a specific `wiederanlaufwert`: `run_legacy_job(wiederanlaufwert='12345')`.
    2.  Execute the migrated Airflow DAG with the same `wiederanlaufwert`: `run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'wiederanlaufwert': '12345'})`.
    3.  Fetch the `wiederanlaufwert` value from the Airflow DAG's XComs.
*   **Pass/Fail Criterion:** The `wiederanlaufwert` value pushed to XCom by the Airflow task must match the provided input.

```python
def test_wiederanlaufwert_handling():
    # Setup minimal source data for job to run
    bpr_bcp_data = [{'CNTRCT_ID': 1, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001}]
    rn_vertrag_data = [{'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '123'}]
    dwtk_meldungen_data = [{'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}]

    insert_oracle_data('sof$ta_bpr_bcp', bpr_bcp_data)
    insert_oracle_data('sof$ta_rn_vertrag', rn_vertrag_data)
    insert_oracle_data('isbert_schema.dwtk_meldungen', dwtk_meldungen_data)

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action
    provided_wiederanlaufwert = '12345'
    run_legacy_job(wiederanlaufwert=provided_wiederanlaufwert)
    dag_run_id = run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'wiederanlaufwert': provided_wiederanlaufwert})

    actual_wiederanlaufwert = get_xcom_value('bert_bp_ta_bcp_msisdn_dag', 'get_stichtag_and_wiederanlaufwert', 'wiederanlaufwert')

    # Pass/Fail Criterion
    assert actual_wiederanlaufwert == provided_wiederanlaufwert, f"Provided wiederanlaufwert mismatch. Expected {provided_wiederanlaufwert}, got {actual_wiederanlaufwert}"

    # NOTE: The legacy KSH script defaults wiederanlaufwert to 0 if not set,
    # while the migrated DAG defaults to an empty string. This is a minor difference
    # as the value is not used in the core SQL. If it were used, this would need
    # to be aligned.
```

---

### 3. Data Quality / Row Count / Schema Assertions

These tests ensure the structural and quality aspects of the migrated data.

#### Test Case 3.1: Output Schema Validation

*   **Purpose:** Verify the schema of the target BigQuery table matches the expected structure (column names, data types) derived from the Oracle source.
*   **Setup:**
    1.  Ensure `project_id.sof_schema.ta_bcp_msisdn_bq` exists.
    2.  (Optional) Run a full data load (e.g., using Test Case 1.1 setup) to ensure the table is populated.
*   **Action:**
    1.  Query the schema of `project_id.sof_schema.ta_bcp_msisdn_bq` in BigQuery.
    2.  Query the schema of `sof$ta_bcp_msisdn` in Oracle.
*   **Pass/Fail Criterion:**
    *   The column names and their order must match.
    *   The data types must be compatible (e.g., Oracle `NUMBER` to BigQuery `INT64`, Oracle `VARCHAR2` to BigQuery `STRING`).

```python
def test_output_schema_validation():
    # Action: Query BigQuery schema
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = bq_hook.get_client(project_id=PROJECT_ID)
    table = client.get_table(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')
    bq_schema = {field.name: field.field_type for field in table.schema}

    # Expected schema based on Oracle and BigQuery type mapping
    expected_bq_schema = {
        'CNTRCT_ID': 'INT64',
        'BPR_ID': 'INT64',
        'CNTRCT_ID_REF': 'INT64',
        'TN_TEL_MSISDN': 'STRING'
    }

    # Pass/Fail Criterion
    assert bq_schema == expected_bq_schema, "BigQuery target table schema mismatch"

    # Optional: Compare with Oracle schema (requires Oracle metadata access)
    # oracle_schema = fetch_oracle_schema('sof$ta_bcp_msisdn')
    # assert oracle_schema_matches_bq_schema(oracle_schema, bq_schema)
```

#### Test Case 3.2: Row Count Parity

*   **Purpose:** Verify the total number of rows in the migrated target table matches the legacy target table under various conditions. (This is implicitly covered by 1.1, 1.2, 1.3, 1.4, 1.5 but good to explicitly call out).
*   **Setup:** Use the same setups as Test Cases 1.1, 1.2, 1.3, 1.4, 1.5.
*   **Action:** After each run, query the row count of both target tables.
*   **Pass/Fail Criterion:** The row count in `sof$ta_bcp_msisdn` must be equal to the row count in `sof_schema.ta_bcp_msisdn_bq`.

```python
# This is covered by the assertions in Test Cases 1.1, 1.2, 1.3, 1.4, 1.5.
# Example assertion:
# assert len(oracle_result) == len(bq_result), "Row counts do not match"
```

#### Test Case 3.3: Uniqueness of Output Records

*   **Purpose:** Verify that all records in the target table are unique across the selected columns, as implied by the `DISTINCT` keyword in the source SQL.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate source tables with data that *would* produce duplicates if `DISTINCT` were not applied (similar to Test Case 1.3).
    3.  Populate `dwtk_meldungen` tables as in 1.1.
*   **Action:**
    1.  Execute the migrated DAG.
    2.  Fetch all data from `sof_schema.ta_bcp_msisdn_bq`.
    3.  Count the number of unique rows in the fetched data.
*   **Pass/Fail Criterion:** The number of fetched rows must be equal to the number of unique rows (i.e., no duplicates exist in the target table).

```python
def test_uniqueness_of_output_records():
    # Setup source data to potentially create duplicates if DISTINCT fails
    bpr_bcp_data = [
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001},
        {'CNTRCT_ID': 101, 'BPR_ID': 1, 'CNTRCT_ID_REF': 1001}, # Identical source row
        {'CNTRCT_ID': 102, 'BPR_ID': 2, 'CNTRCT_ID_REF': 1002},
    ]
    rn_vertrag_data = [
        {'CNTRCT_ID': 1001, 'TN_TEL_MSISDN': '1234567890'},
        {'CNTRCT_ID': 1002, 'TN_TEL_MSISDN': '0987654321'},
    ]
    dwtk_meldungen_data = [
        {'JOB_KENNUNG': 'BERT_DROP_TEMP_TABLE', 'TIMECREATED': pd.Timestamp('2023-01-01 10:00:00')}
    ]

    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq', bpr_bcp_data)
    insert_bq_data(f'{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq', rn_vertrag_data)
    insert_bq_data(f'{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq', dwtk_meldungen_data)

    # Action
    run_airflow_dag('bert_bp_ta_bcp_msisdn_dag', conf={'stichtag': '20230101'})

    bq_result = fetch_bq_data(f'{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq')
    bq_df = pd.DataFrame(bq_result)

    # Pass/Fail Criterion
    # The number of rows should be equal to the number of unique rows
    assert len(bq_df) == len(bq_df.drop_duplicates()), "BigQuery target table contains duplicate rows"
    assert len(bq_df) == 2, "Expected 2 unique rows after DISTINCT" # (101,1,1001,1234567890), (102,2,1002,0987654321)
```