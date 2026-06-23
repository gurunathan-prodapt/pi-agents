As a senior data-migration QA engineer, I've analyzed the provided migration design for `r_ausd_v_ta_vvl_upgrade.ksh`. The migration involves transforming KornShell scripts and Oracle SQL into BigQuery stored procedures orchestrated by Cloud Composer.

Below are detailed test cases designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality. Each test case includes its purpose, setup, action, and concrete pass/fail criteria, along with runnable Python/Pytest code snippets for BigQuery interactions.

---

## Migration Validation Tests for `r_ausd_v_ta_vvl_upgrade.ksh`

### Test Case 1: End-to-End Data Parity (Golden Dataset)

*   **Purpose**: To verify that the migrated BigQuery job produces identical output data in the `sof_ta_vvl_upgrade` table as the legacy Oracle job, given the same initial source data. This is the most critical test for overall behavioral equivalence.
*   **Setup**:
    1.  **Legacy Environment**:
        *   Populate Oracle tables `sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`, and `isbert_schema.dwtk_meldungen` with a comprehensive "golden dataset". This dataset should cover various scenarios, including:
            *   Multiple `vertrags_id` entries with different `aenderung_am` dates to test the `MAX(aenderung_am)` subquery.
            *   `vvl_aendgrund_id` values that match and do not match entries in `dwh$ta_l_bindefr_aendgr_carm`.
            *   `beschreibung` values in `dwh$ta_l_bindefr_aendgr_carm` that trigger the `CASE` statement (e.g., 'DPPS Diensttyp A13 (EG-Upgrade)') and others.
            *   `isbert_schema.dwtk_meldungen` entries with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` values, as well as entries with other `job_kennung` values, to test `v_datum` logic.
            *   Edge cases like NULLs in relevant columns (e.g., `aenderung_am`) and empty result sets for specific joins.
        *   Ensure the Oracle target table `sof$ta_vvl_upgrade` is empty or truncated before running the legacy job.
    2.  **Target Environment**:
        *   Load the *exact same* "golden dataset" into the corresponding BigQuery tables: `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_dwh`, `your_gcp_project_id.your_bq_dataset_id.dwh_ta_l_bindefr_aendgr_carm`, and `your_gcp_project_id.isbert_schema.dwtk_meldungen`.
        *   Ensure the BigQuery target table `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` is empty or truncated before running the BigQuery job.
        *   Ensure the `your_gcp_project_id.your_bq_dataset_id.job_log` table is empty.
*   **Action**:
    1.  **Legacy**: Execute the legacy KornShell job: `r_ausd_v_ta_vvl_upgrade.ksh -j BERT_V_TA_VVL_UPGRADE -f 12345`.
    2.  **Target**: Execute the BigQuery job via Cloud Composer (or directly call the wrapper SP for testing): `CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_sp('BERT_V_TA_VVL_UPGRADE', '12345');`.
    3.  Extract all data from Oracle's `sof$ta_vvl_upgrade` and BigQuery's `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade`.
*   **Pass/Fail Criterion**:
    *   The number of rows in Oracle's `sof$ta_vvl_upgrade` must be identical to the number of rows in BigQuery's `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade`.
    *   After sorting both datasets by `vertrags_id` and `upgradedatum`, all column values for all rows must be identical.
    *   Data types (e.g., `upgradedatum` as TIMESTAMP, `vertrags_id` and `upgradegrund` as STRING) should be consistent and correctly migrated.

```python
import pytest
from google.cloud import bigquery
import cx_Oracle # Requires Oracle client and cx_Oracle library

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
ORACLE_CONN_STR = "user/password@host:port/service_name"
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"
ORACLE_SCHEMA = "ISBERT_SCHEMA" # Or the schema where sof$ta_vvl_upgrade resides

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(scope="module")
def oracle_conn():
    """Provides an Oracle connection for the test module."""
    conn = cx_Oracle.connect(ORACLE_CONN_STR)
    yield conn
    conn.close()

def _fetch_oracle_data(oracle_conn, table_name):
    """Fetches and formats data from an Oracle table."""
    cursor = oracle_conn.cursor()
    # Ensure consistent date formatting for comparison
    cursor.execute(f"""
        SELECT vertrags_id, upgradegrund, TO_CHAR(upgradedatum, 'YYYY-MM-DD HH24:MI:SS')
        FROM {ORACLE_SCHEMA}.{table_name}
        ORDER BY vertrags_id, upgradedatum
    """)
    return [tuple(row) for row in cursor.fetchall()]

def _fetch_bq_data(bq_client, table_id):
    """Fetches and formats data from a BigQuery table."""
    query = f"""
    SELECT
        vertrags_id,
        upgradegrund,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', upgradedatum)
    FROM
        `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}`
    ORDER BY
        vertrags_id, upgradedatum
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    return [tuple(row.values()) for row in results]

def test_e2e_data_parity(bq_client, oracle_conn):
    """
    Tests end-to-end data parity by comparing output from legacy Oracle job
    and migrated BigQuery job.
    NOTE: This test assumes the 'Setup' and 'Action' steps (populating data
    and running both jobs) have been performed externally to create the
    final state in the target tables.
    """
    # --- Action & Assertion ---
    oracle_results = _fetch_oracle_data(oracle_conn, "SOF$TA_VVL_UPGRADE")
    bq_results = _fetch_bq_data(bq_client, "sof_ta_vvl_upgrade")

    assert len(oracle_results) == len(bq_results), \
        f"Row count mismatch: Oracle has {len(oracle_results)} rows, BigQuery has {len(bq_results)} rows."

    # Compare sorted results to ensure order-independent comparison
    assert oracle_results == bq_results, \
        "Data mismatch between Oracle and BigQuery output tables."

    print(f"Successfully verified {len(oracle_results)} rows for data parity.")

```

---

### Test Case 2: `d_ausd_v_ta_vvl_upgrade_sp` - `v_datum` Calculation

*   **Purpose**: To verify that the `v_datum` calculation in BigQuery correctly replicates the Oracle `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` logic, including `MAX`, `NVL`/`IFNULL`, and date formatting.
*   **Setup**:
    1.  **Legacy Environment**: Populate Oracle `isbert_schema.dwtk_meldungen` with various `timecreated` values, including:
        *   Rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and valid `timecreated`.
        *   Rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` as NULL (if Oracle schema allows).
        *   Rows with other `job_kennung` values.
        *   An empty table scenario (no rows for `BERT_DROP_TEMP_TABLE`).
    2.  **Target Environment**: Load the *exact same* data into BigQuery `your_gcp_project_id.isbert_schema.dwtk_meldungen`.
*   **Action**:
    1.  **Legacy**: Manually execute the Oracle SQL snippet to get `v_datum`.
    2.  **Target**: Manually execute the BigQuery SQL snippet to get `v_datum`.
*   **Pass/Fail Criterion**: The `v_datum` string returned by the Oracle query must be identical to the `v_datum` string returned by the BigQuery query for each setup scenario.

```python
import pytest
from google.cloud import bigquery
import cx_Oracle # Requires Oracle client and cx_Oracle library

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
ORACLE_CONN_STR = "user/password@host:port/service_name"
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"
ORACLE_SCHEMA = "ISBERT_SCHEMA"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(scope="module")
def oracle_conn():
    conn = cx_Oracle.connect(ORACLE_CONN_STR)
    yield conn
    conn.close()

@pytest.mark.parametrize("test_case_name, oracle_data, expected_v_datum", [
    ("normal_data",
     [('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00'), ('OTHER_JOB', '2023-01-16 11:00:00'), ('BERT_DROP_TEMP_TABLE', '2023-01-14 09:00:00')],
     '20230115'),
    ("empty_for_job_kennung",
     [('OTHER_JOB', '2023-01-16 11:00:00')],
     '19000101'),
    ("null_timecreated_for_job_kennung", # Assumes Oracle allows NULL timecreated
     [('BERT_DROP_TEMP_TABLE', None), ('OTHER_JOB', '2023-01-16 11:00:00')],
     '19000101'),
    ("single_row",
     [('BERT_DROP_TEMP_TABLE', '2022-12-25 08:30:00')],
     '20221225'),
])
def test_v_datum_calculation(bq_client, oracle_conn, test_case_name, oracle_data, expected_v_datum):
    """
    Tests the v_datum calculation logic for various scenarios.
    """
    # --- Setup ---
    # Clear and populate Oracle dwtk_meldungen
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute(f"TRUNCATE TABLE {ORACLE_SCHEMA}.DWTK_MELDUNGEN")
    for job_kennung, timecreated in oracle_data:
        if timecreated:
            oracle_cursor.execute(f"INSERT INTO {ORACLE_SCHEMA}.DWTK_MELDUNGEN (job_kennung, timecreated) VALUES (:1, TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS'))", (job_kennung, timecreated))
        else:
            oracle_cursor.execute(f"INSERT INTO {ORACLE_SCHEMA}.DWTK_MELDUNGEN (job_kennung, timecreated) VALUES (:1, NULL)", (job_kennung,))
    oracle_conn.commit()

    # Clear and populate BigQuery dwtk_meldungen
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{ORACLE_SCHEMA}.dwtk_meldungen`").result()
    rows_to_insert = []
    for job_kennung, timecreated in oracle_data:
        rows_to_insert.append({"job_kennung": job_kennung, "timecreated": timecreated})
    if rows_to_insert:
        bq_client.insert_rows_json(f"{BQ_PROJECT_ID}.{ORACLE_SCHEMA}.dwtk_meldungen", rows_to_insert)

    # --- Action ---
    # Get v_datum from Oracle
    oracle_query = f"""
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
    FROM {ORACLE_SCHEMA}.DWTK_MELDUNGEN m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    oracle_cursor.execute(oracle_query)
    oracle_v_datum = oracle_cursor.fetchone()[0]

    # Get v_datum from BigQuery
    bq_query = f"""
    SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
    FROM `{BQ_PROJECT_ID}.{ORACLE_SCHEMA}.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    bq_query_job = bq_client.query(bq_query)
    bq_v_datum = [row[0] for row in bq_query_job.result()][0]

    # --- Pass/Fail Criterion ---
    assert oracle_v_datum == expected_v_datum, f"Oracle v_datum mismatch for {test_case_name}: Expected {expected_v_datum}, Got {oracle_v_datum}"
    assert bq_v_datum == expected_v_datum, f"BigQuery v_datum mismatch for {test_case_name}: Expected {expected_v_datum}, Got {bq_v_datum}"
    assert oracle_v_datum == bq_v_datum, f"Oracle and BigQuery v_datum differ for {test_case_name}: Oracle={oracle_v_datum}, BQ={bq_v_datum}"

```

---

### Test Case 3: `d_ausd_v_ta_vvl_upgrade_sp` - `TRUNCATE` Behavior

*   **Purpose**: To verify that the BigQuery `TRUNCATE TABLE` statement correctly clears the target table before insertion, replicating the Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` behavior.
*   **Setup**:
    1.  **Target Environment**:
        *   Populate `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` with some dummy data.
        *   Populate source tables (`sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`, `dwtk_meldungen`) such that `d_ausd_v_ta_vvl_upgrade_sp` would insert at least one row.
*   **Action**:
    1.  Call the BigQuery stored procedure: `CALL your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_upgrade_sp();`
    2.  Query the target table: `SELECT COUNT(*) FROM your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade;`
*   **Pass/Fail Criterion**: The count of rows in `sof_ta_vvl_upgrade` after the procedure execution must be equal to the number of rows that *should* be inserted by the `INSERT` statement, and must *not* include any of the pre-existing dummy data.

```python
import pytest
from google.cloud import bigquery

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

def test_truncate_behavior(bq_client):
    """
    Tests that the target table is correctly truncated before new data insertion.
    """
    target_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`"
    source_dwh = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_dwh`"
    source_carm = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.dwh_ta_l_bindefr_aendgr_carm`"
    source_meldungen = f"`{BQ_PROJECT_ID}.isbert_schema.dwtk_meldungen`"

    # --- Setup ---
    # 1. Populate target table with dummy data
    bq_client.query(f"TRUNCATE TABLE {target_table}").result()
    bq_client.query(f"INSERT INTO {target_table} (vertrags_id, upgradegrund, upgradedatum) VALUES ('DUMMY1', 'Old Data', CURRENT_TIMESTAMP())").result()
    bq_client.query(f"INSERT INTO {target_table} (vertrags_id, upgradegrund, upgradedatum) VALUES ('DUMMY2', 'More Old Data', CURRENT_TIMESTAMP())").result()
    initial_dummy_count = bq_client.query(f"SELECT COUNT(*) FROM {target_table}").result().to_dataframe().iloc[0,0]
    assert initial_dummy_count == 2, "Setup failed: Dummy data not inserted into target table."

    # 2. Populate source tables to ensure some data is inserted by the SP
    bq_client.query(f"TRUNCATE TABLE {source_dwh}").result()
    bq_client.query(f"TRUNCATE TABLE {source_carm}").result()
    bq_client.query(f"TRUNCATE TABLE {source_meldungen}").result()

    bq_client.insert_rows_json(source_dwh, [
        {"vertrags_id": "V1", "vvl_aendgrund_id": 1, "aenderung_am": "2023-01-01 10:00:00"},
        {"vertrags_id": "V1", "vvl_aendgrund_id": 2, "aenderung_am": "2023-01-02 11:00:00"}, # Latest for V1
        {"vertrags_id": "V2", "vvl_aendgrund_id": 3, "aenderung_am": "2023-01-03 12:00:00"},
    ]).result()
    bq_client.insert_rows_json(source_carm, [
        {"vvl_aendgrund_id": 1, "beschreibung": "Standard Upgrade"},
        {"vvl_aendgrund_id": 2, "beschreibung": "DPPS Diensttyp A13 (EG-Upgrade)"},
        {"vvl_aendgrund_id": 3, "beschreibung": "Other Upgrade Reason"},
    ]).result()
    bq_client.insert_rows_json(source_meldungen, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": "2023-01-01 00:00:00"},
    ]).result()

    # Calculate expected rows from the insert logic (should be 2 based on V1 latest and V2)
    expected_insert_count_query = f"""
    SELECT COUNT(*)
    FROM {source_dwh} AS vvl
    JOIN {source_carm} AS ba ON ba.vvl_aendgrund_id = vvl.vvl_aendgrund_id
    JOIN (
        SELECT vertrags_id, MAX(aenderung_am) AS upgr_datum
        FROM {source_dwh} AS vvlt
        GROUP BY vertrags_id
    ) AS vvl2 ON vvl.vertrags_id = vvl2.vertrags_id AND vvl.aenderung_am = vvl2.upgr_datum;
    """
    expected_insert_count = bq_client.query(expected_insert_count_query).result().to_dataframe().iloc[0,0]
    assert expected_insert_count > 0, "Setup failed: Expected insert count is 0, adjust source data."

    # --- Action ---
    bq_client.query(f"CALL `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.d_ausd_v_ta_vvl_upgrade_sp`()").result()

    # --- Pass/Fail Criterion ---
    final_row_count = bq_client.query(f"SELECT COUNT(*) FROM {target_table}").result().to_dataframe().iloc[0,0]

    assert final_row_count == expected_insert_count, \
        f"Row count mismatch after SP execution. Expected {expected_insert_count} (from insert logic), but found {final_row_count}. This indicates TRUNCATE or INSERT issue."
    assert final_row_count != initial_dummy_count, "TRUNCATE did not occur, dummy data still present."

```

---

### Test Case 4: `d_ausd_v_ta_vvl_upgrade_sp` - `CASE` Statement and Join Logic

*   **Purpose**: To verify the correctness of the `JOIN` conditions and the `CASE` statement logic for `upgradegrund`, as well as the subquery for `upgr_datum`. This covers core data transformation correctness.
*   **Setup**:
    1.  **Target Environment**: Populate BigQuery tables `sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`, and `dwtk_meldungen` with specific test data to exercise:
        *   `vertrags_id` with multiple `aenderung_am` dates to test `MAX(aenderung_am)` subquery.
        *   `vvl_aendgrund_id` values that map to 'DPPS Diensttyp A13 (EG-Upgrade)' in `dwh_ta_l_bindefr_aendgr_carm`.
        *   `vvl_aendgrund_id` values that map to other descriptions.
        *   `vvl_aendgrund_id` values that do *not* have a match in `dwh_ta_l_bindefr_aendgr_carm` (these should be filtered out by the `JOIN`).
        *   NULL `aenderung_am` (should not be picked by MAX).
        *   `dwtk_meldungen`: At least one row for `BERT_DROP_TEMP_TABLE` to ensure `v_datum` is not '19000101'.
    2.  Define the precise expected output for `sof_ta_vvl_upgrade` based on this test data and the transformation logic.
*   **Action**:
    1.  Call the BigQuery stored procedure: `CALL your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_upgrade_sp();`
    2.  Query the target table: `SELECT vertrags_id, upgradegrund, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', upgradedatum) FROM your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade ORDER BY vertrags_id, upgradedatum;`
*   **Pass/Fail Criterion**: The actual output from `sof_ta_vvl_upgrade` must exactly match the predefined expected output, both in terms of row count and column values.

```python
import pytest
from google.cloud import bigquery

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(autouse=True)
def clear_tables_for_transformation_logic(bq_client):
    """Clears relevant tables before each test run."""
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_dwh`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.dwh_ta_l_bindefr_aendgr_carm`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.isbert_schema.dwtk_meldungen`").result()

def test_transformation_logic(bq_client):
    """
    Tests the core transformation logic including joins, MAX subquery, and CASE statement.
    """
    target_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`"
    source_dwh = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_dwh`"
    source_carm = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.dwh_ta_l_bindefr_aendgr_carm`"
    source_meldungen = f"`{BQ_PROJECT_ID}.isbert_schema.dwtk_meldungen`"

    # --- Setup ---
    # Populate sof_ta_vvl_dwh
    bq_client.insert_rows_json(source_dwh, [
        {"vertrags_id": "V001", "vvl_aendgrund_id": 10, "aenderung_am": "2023-01-01 10:00:00"},
        {"vertrags_id": "V001", "vvl_aendgrund_id": 20, "aenderung_am": "2023-01-05 11:00:00"}, # Latest for V001
        {"vertrags_id": "V002", "vvl_aendgrund_id": 30, "aenderung_am": "2023-02-10 12:00:00"}, # Latest for V002
        {"vertrags_id": "V002", "vvl_aendgrund_id": 40, "aenderung_am": "2023-02-08 09:00:00"},
        {"vertrags_id": "V003", "vvl_aendgrund_id": 20, "aenderung_am": "2023-03-15 13:00:00"}, # Another EG-Upgrade
        {"vertrags_id": "V004", "vvl_aendgrund_id": 50, "aenderung_am": "2023-04-20 14:00:00"}, # No matching vvl_aendgrund_id in carm
        {"vertrags_id": "V005", "vvl_aendgrund_id": 10, "aenderung_am": "2023-05-01 15:00:00"}, # Single latest entry
        {"vertrags_id": "V006", "vvl_aendgrund_id": 10, "aenderung_am": "2023-06-01 16:00:00"},
        {"vertrags_id": "V006", "vvl_aendgrund_id": 60, "aenderung_am": "2023-06-01 16:00:00"}, # Two rows with same max date for V006
    ]).result()

    # Populate dwh_ta_l_bindefr_aendgr_carm
    bq_client.insert_rows_json(source_carm, [
        {"vvl_aendgrund_id": 10, "beschreibung": "Standard VVL"},
        {"vvl_aendgrund_id": 20, "beschreibung": "DPPS Diensttyp A13 (EG-Upgrade)"},
        {"vvl_aendgrund_id": 30, "beschreibung": "Tarifwechsel"},
        {"vvl_aendgrund_id": 40, "beschreibung": "Kundenwunsch"},
        {"vvl_aendgrund_id": 60, "beschreibung": "Zusatzoption"},
    ]).result()

    # Populate dwtk_meldungen (just to ensure v_datum is not default)
    bq_client.insert_rows_json(source_meldungen, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": "2023-01-01 00:00:00"},
    ]).result()

    # Define expected output based on the logic
    expected_output = [
        ("V001", "Endgeraeteupgrade", "2023-01-05 11:00:00"),
        ("V002", "Tarifwechsel", "2023-02-10 12:00:00"),
        ("V003", "Endgeraeteupgrade", "2023-03-15 13:00:00"),
        ("V005", "Standard VVL", "2023-05-01 15:00:00"),
        ("V006", "Standard VVL", "2023-06-01 16:00:00"), # From vvl_aendgrund_id 10
        ("V006", "Zusatzoption", "2023-06-01 16:00:00"), # From vvl_aendgrund_id 60
    ]
    # V004 is excluded due to no join match.
    # V006 has two rows in sof_ta_vvl_dwh with the same MAX(aenderung_am) but different vvl_aendgrund_id,
    # so both should be inserted as per the join logic.

    # --- Action ---
    bq_client.query(f"CALL `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.d_ausd_v_ta_vvl_upgrade_sp`()").result()

    # --- Pass/Fail Criterion ---
    actual_output_query = f"""
    SELECT
        vertrags_id,
        upgradegrund,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', upgradedatum)
    FROM {target_table}
    ORDER BY vertrags_id, upgradegrund, upgradedatum;
    """
    actual_results = bq_client.query(actual_output_query).result()
    actual_output = [tuple(row.values()) for row in actual_results]

    assert len(actual_output) == len(expected_output), \
        f"Row count mismatch. Expected {len(expected_output)}, got {len(actual_output)}."
    assert sorted(actual_output) == sorted(expected_output), \
        "Transformed data mismatch for core logic."

```

---

### Test Case 5: `vertragsdatenabgleich_wrapper_sp` - Parameter Validation

*   **Purpose**: To verify that the wrapper stored procedure correctly validates input parameters (`p_job_kennung`, `p_eintrags_nr`) and logs errors, replicating the shell script's `getopts` and `DWMSG_MeldeFehler` behavior.
*   **Setup**:
    1.  **Target Environment**: Ensure `your_gcp_project_id.your_bq_dataset_id.job_log` is empty before each test.
*   **Action**:
    1.  Attempt to call `vertragsdatenabgleich_wrapper_sp` with:
        *   Missing `p_job_kennung` (e.g., `NULL`).
        *   Empty `p_job_kennung` (e.g., `''`).
        *   Missing `p_eintrags_nr` (e.g., `NULL`).
        *   Empty `p_eintrags_nr` (e.g., `''`).
    2.  Query the `job_log` table for entries.
*   **Pass/Fail Criterion**:
    *   Each call with invalid parameters must raise a BigQuery exception (`BadRequest`).
    *   The `job_log` table must contain an `ERROR` entry for each failed call, with `status = 'FAILED'`, `error_code = 193`, and the correct `error_argument` (e.g., 'p_job_kennung' or 'p_eintrags_nr').

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest # For BQ exceptions

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(autouse=True) # Clears log table before each test
def clear_job_log(bq_client):
    """Clears the job log table before each test."""
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`").result()

@pytest.mark.parametrize("job_kennung, eintrags_nr, expected_error_arg", [
    (None, '123', 'p_job_kennung'),
    ('', '123', 'p_job_kennung'),
    ('JOB1', None, 'p_eintrags_nr'),
    ('JOB1', '', 'p_eintrags_nr'),
])
def test_wrapper_parameter_validation(bq_client, job_kennung, eintrags_nr, expected_error_arg):
    """
    Tests that the wrapper SP correctly validates input parameters and logs errors.
    """
    wrapper_sp = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.vertragsdatenabgleich_wrapper_sp`"
    job_log_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`"

    # --- Action ---
    with pytest.raises(BadRequest) as excinfo:
        # Construct the CALL statement dynamically, handling NULLs
        params = []
        params.append("NULL" if job_kennung is None else f"'{job_kennung}'")
        params.append("NULL" if eintrags_nr is None else f"'{eintrags_nr}'")

        call_statement = f"CALL {wrapper_sp}({', '.join(params)})"
        bq_client.query(call_statement).result()

    # --- Pass/Fail Criterion (Exception) ---
    assert f"parameter is missing or empty" in str(excinfo.value), \
        f"Expected parameter validation error, but got: {excinfo.value}"

    # --- Pass/Fail Criterion (Logging) ---
    log_query = f"""
    SELECT log_level, status, error_code, error_argument, message
    FROM {job_log_table}
    WHERE log_level = 'ERROR'
    ORDER BY log_timestamp DESC
    LIMIT 1
    """
    log_results = bq_client.query(log_query).result()
    log_entries = [tuple(row.values()) for row in log_results]

    assert len(log_entries) == 1, "Expected exactly one ERROR log entry."
    log_entry = log_entries[0]
    assert log_entry[0] == 'ERROR'
    assert log_entry[1] == 'FAILED'
    assert log_entry[2] == 193
    assert log_entry[3] == expected_error_arg
    assert f"{expected_error_arg.replace('p_', '')} parameter is missing or empty" in log_entry[4]

```

---

### Test Case 6: `vertragsdatenabgleich_wrapper_sp` - Successful Execution and Logging

*   **Purpose**: To verify that the wrapper stored procedure executes successfully, calls the core transformation procedure, and logs the start and completion status correctly. This covers external system replacements (logging) and orchestration flow.
*   **Setup**:
    1.  **Target Environment**:
        *   Ensure `your_gcp_project_id.your_bq_dataset_id.job_log` is empty.
        *   Populate source tables (`sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`, `dwtk_meldungen`) with valid data to allow `d_ausd_v_ta_vvl_upgrade_sp` to run successfully and insert some rows.
        *   Ensure `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` is empty.
*   **Action**:
    1.  Call the BigQuery stored procedure: `CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_sp('TEST_JOB', '98765');`
    2.  Query the `job_log` table for entries.
    3.  Query the `sof_ta_vvl_upgrade` table for inserted rows.
*   **Pass/Fail Criterion**:
    *   The call must complete without raising an exception.
    *   The `job_log` table must contain at least two entries for the specified `job_kennung` and `eintrags_nr`: one with `status = 'STARTED'` and one with `status = 'COMPLETED'`.
    *   The `sof_ta_vvl_upgrade` table must contain the expected number of rows (i.e., the core transformation procedure was successfully executed).

```python
import pytest
from google.cloud import bigquery
import time

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(autouse=True)
def clear_job_log_and_target_table_for_success(bq_client):
    """Clears log and target tables, and populates source tables for a successful run."""
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`").result()

    source_dwh = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_dwh`"
    source_carm = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.dwh_ta_l_bindefr_aendgr_carm`"
    source_meldungen = f"`{BQ_PROJECT_ID}.isbert_schema.dwtk_meldungen`"
    bq_client.query(f"TRUNCATE TABLE {source_dwh}").result()
    bq_client.query(f"TRUNCATE TABLE {source_carm}").result()
    bq_client.query(f"TRUNCATE TABLE {source_meldungen}").result()
    bq_client.insert_rows_json(source_dwh, [
        {"vertrags_id": "V100", "vvl_aendgrund_id": 1, "aenderung_am": "2023-01-01 10:00:00"},
    ]).result()
    bq_client.insert_rows_json(source_carm, [
        {"vvl_aendgrund_id": 1, "beschreibung": "Test Upgrade"},
    ]).result()
    bq_client.insert_rows_json(source_meldungen, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": "2023-01-01 00:00:00"},
    ]).result()

def test_wrapper_successful_execution_and_logging(bq_client):
    """
    Tests that the wrapper SP executes successfully and logs start/completion.
    """
    wrapper_sp = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.vertragsdatenabgleich_wrapper_sp`"
    job_log_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`"
    target_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`"

    test_job_kennung = 'TEST_JOB_SUCCESS'
    test_eintrags_nr = '98765'

    # --- Action ---
    bq_client.query(f"CALL {wrapper_sp}('{test_job_kennung}', '{test_eintrags_nr}')").result()

    # --- Pass/Fail Criterion (Logging) ---
    log_query = f"""
    SELECT log_level, status, job_kennung, eintrags_nr
    FROM {job_log_table}
    WHERE job_kennung = '{test_job_kennung}' AND eintrags_nr = '{test_eintrags_nr}'
    ORDER BY log_timestamp ASC
    """
    time.sleep(2) # Give BigQuery streaming inserts a moment to propagate
    log_results = bq_client.query(log_query).result()
    log_entries = [tuple(row.values()) for row in log_results]

    assert len(log_entries) >= 2, "Expected at least 'STARTED' and 'COMPLETED' log entries."

    started_entry = next((entry for entry in log_entries if entry[1] == 'STARTED'), None)
    assert started_entry is not None, "Expected 'STARTED' log entry not found."
    assert started_entry[0] == 'INFO'
    assert started_entry[2] == test_job_kennung
    assert started_entry[3] == test_eintrags_nr

    completed_entry = next((entry for entry in log_entries if entry[1] == 'COMPLETED'), None)
    assert completed_entry is not None, "Expected 'COMPLETED' log entry not found."
    assert completed_entry[0] == 'INFO'
    assert completed_entry[2] == test_job_kennung
    assert completed_entry[3] == test_eintrags_nr

    # --- Pass/Fail Criterion (Core Transformation) ---
    final_row_count = bq_client.query(f"SELECT COUNT(*) FROM {target_table}").result().to_dataframe().iloc[0,0]
    assert final_row_count > 0, "Core transformation did not insert any rows, indicating a failure or empty source data."

```

---

### Test Case 7: `vertragsdatenabgleich_wrapper_sp` - Error Handling and Logging

*   **Purpose**: To verify that the wrapper stored procedure correctly handles errors from the called `d_ausd_v_ta_vvl_upgrade_sp` and logs the error details, replicating the shell script's `trap ERR` behavior.
*   **Setup**:
    1.  **Target Environment**:
        *   Ensure `your_gcp_project_id.your_bq_dataset_id.job_log` is empty.
        *   Temporarily modify `d_ausd_v_ta_vvl_upgrade_sp` to force an error (e.g., `RAISE BQ EXCEPTION 'Forced error for testing'`). This is a common practice for testing error handling in sub-procedures.
*   **Action**:
    1.  Call the BigQuery stored procedure: `CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_sp('TEST_JOB_ERROR', '11111');`
    2.  Query the `job_log` table for entries.
*   **Pass/Fail Criterion**:
    *   The call must raise a BigQuery exception (`BadRequest`).
    *   The `job_log` table must contain an `ERROR` entry for the specified `job_kennung` and `eintrags_nr`, with `status = 'FAILED'`, and `message` containing the error details. It should also contain a `STARTED` entry.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import time

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(autouse=True)
def clear_job_log_and_restore_d_ausd_sp(bq_client):
    """
    Clears the job log table and ensures d_ausd_v_ta_vvl_upgrade_sp is restored
    to its original definition after the test.
    """
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`").result()

    # Store original d_ausd_v_ta_vvl_upgrade_sp definition
    original_sp_query = f"""
    SELECT routine_definition
    FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.INFORMATION_SCHEMA.ROUTINES`
    WHERE routine_name = 'd_ausd_v_ta_vvl_upgrade_sp' AND routine_type = 'PROCEDURE';
    """
    original_sp_def = bq_client.query(original_sp_query).result().to_dataframe().iloc[0,0]

    yield # Run the test

    # Restore original d_ausd_v_ta_vvl_upgrade_sp definition
    bq_client.query(original_sp_def).result()
    print(f"\nRestored d_ausd_v_ta_vvl_upgrade_sp to original definition.")


def test_wrapper_error_handling_and_logging(bq_client):
    """
    Tests that the wrapper SP correctly handles errors from a called sub-procedure
    and logs the error details.
    """
    wrapper_sp = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.vertragsdatenabgleich_wrapper_sp`"
    d_ausd_sp = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.d_ausd_v_ta_vvl_upgrade_sp`"
    job_log_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`"

    test_job_kennung = 'TEST_JOB_ERROR'
    test_eintrags_nr = '11111'
    forced_error_message = 'Forced error for testing d_ausd_v_ta_vvl_upgrade_sp'

    # --- Setup: Temporarily modify d_ausd_v_ta_vvl_upgrade_sp to raise an error ---
    temp_error_sp_definition = f"""
    CREATE OR REPLACE PROCEDURE {d_ausd_sp}()
    BEGIN
        RAISE BQ EXCEPTION '{forced_error_message}';
    END;
    """
    bq_client.query(temp_error_sp_definition).result()
    print(f"Temporarily modified {d_ausd_sp} to raise an error.")

    # --- Action ---
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(f"CALL {wrapper_sp}('{test_job_kennung}', '{test_eintrags_nr}')").result()

    # --- Pass/Fail Criterion (Exception) ---
    assert forced_error_message in str(excinfo.value), \
        f"Expected forced error message, but got: {excinfo.value}"

    # --- Pass/Fail Criterion (Logging) ---
    log_query = f"""
    SELECT log_level, status, job_kennung, eintrags_nr, message, error_code, error_argument
    FROM {job_log_table}
    WHERE job_kennung = '{test_job_kennung}' AND eintrags_nr = '{test_eintrags_nr}'
    ORDER BY log_timestamp ASC
    """
    time.sleep(2) # Give BigQuery streaming inserts a moment to propagate
    log_results = bq_client.query(log_query).result()
    log_entries = [tuple(row.values()) for row in log_results]

    assert len(log_entries) >= 2, "Expected at least 'STARTED' and 'FAILED' log entries."

    started_entry = next((entry for entry in log_entries if entry[1] == 'STARTED'), None)
    assert started_entry is not None, "Expected 'STARTED' log entry not found."
    assert started_entry[0] == 'INFO'

    failed_entry = next((entry for entry in log_entries if entry[1] == 'FAILED'), None)
    assert failed_entry is not None, "Expected 'FAILED' log entry not found."
    assert failed_entry[0] == 'ERROR'
    assert failed_entry[2] == test_job_kennung
    assert failed_entry[3] == test_eintrags_nr
    assert f"Job failed: {forced_error_message}" in failed_entry[4]
    assert failed_entry[5] == -1 # Generic error code for BQ exceptions
    assert failed_entry[6] is not None and len(failed_entry[6]) > 0 # Should contain stack trace

```

---

### Test Case 8: Schema and Data Type Integrity

*   **Purpose**: To verify that the target BigQuery table `sof_ta_vvl_upgrade` has the correct schema and data types as implied by the Oracle source and the transformation logic.
*   **Setup**:
    1.  **Target Environment**:
        *   Ensure `sof_ta_vvl_upgrade` table exists (via `create_sof_ta_vvl_upgrade_table.sql`).
        *   Run the `vertragsdatenabgleich_wrapper_sp` at least once with valid data to populate the table, ensuring all columns receive data.
*   **Action**:
    1.  Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for `sof_ta_vvl_upgrade`.
*   **Pass/Fail Criterion**:
    *   The table `sof_ta_vvl_upgrade` must exist.
    *   It must contain columns: `vertrags_id`, `upgradegrund`, `upgradedatum`.
    *   `vertrags_id` and `upgradegrund` must be of type `STRING`.
    *   `upgradedatum` must be of type `TIMESTAMP`.

```python
import pytest
from google.cloud import bigquery

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

def test_target_table_schema_and_types(bq_client):
    """
    Tests that the target BigQuery table has the expected schema and data types.
    """
    target_table_id = "sof_ta_vvl_upgrade"
    full_table_path = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{target_table_id}"

    # --- Action ---
    # Check if table exists
    try:
        table = bq_client.get_table(full_table_path)
    except Exception as e:
        pytest.fail(f"Target table {full_table_path} does not exist: {e}")

    # Query schema from INFORMATION_SCHEMA
    schema_query = f"""
    SELECT column_name, data_type
    FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{target_table_id}'
    ORDER BY ordinal_position;
    """
    query_job = bq_client.query(schema_query)
    results = query_job.result()
    actual_schema = {row.column_name: row.data_type for row in results}

    # --- Pass/Fail Criterion ---
    expected_schema = {
        "vertrags_id": "STRING",
        "upgradegrund": "STRING",
        "upgradedatum": "TIMESTAMP",
    }

    assert actual_schema == expected_schema, \
        f"Schema mismatch for {target_table_id}. Expected: {expected_schema}, Actual: {actual_schema}"

    print(f"Schema for {target_table_id} verified successfully.")

```

---

### Test Case 9: Character Encoding for `upgradegrund`

*   **Purpose**: To specifically test the `CASE` statement for `upgradegrund` to ensure that special characters (like 'ä', 'ö', 'ü', 'ß') are handled correctly, especially the `Endgeräteupgrade` vs `Endgerteupgrade` scenario mentioned in the risks.
*   **Setup**:
    1.  **Target Environment**:
        *   Populate `sof_ta_vvl_dwh` with `vertrags_id` entries that map to `vvl_aendgrund_id` values whose `beschreibung` in `dwh_ta_l_bindefr_aendgr_carm` includes:
            *   Exactly `'DPPS Diensttyp A13 (EG-Upgrade)'`.
            *   Other `beschreibung` values containing various German umlauts and special characters (e.g., 'Änderung', 'Größenanpassung', 'Fußball-Spezial').
        *   Populate `dwtk_meldungen` to ensure `v_datum` is not default.
*   **Action**:
    1.  Call the BigQuery stored procedure: `CALL your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_upgrade_sp();`
    2.  Query `sof_ta_vvl_upgrade` for the `upgradegrund` values.
*   **Pass/Fail Criterion**:
    *   The `upgradegrund` for the 'DPPS Diensttyp A13 (EG-Upgrade)' case must be exactly `'Endgeraeteupgrade'` (as per the BigQuery code, which uses 'e' not 'ä').
    *   Other `upgradegrund` values with special characters must retain their original special characters (e.g., 'Änderung' should remain 'Änderung', not 'Aenderung').

```python
import pytest
from google.cloud import bigquery

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(autouse=True)
def clear_tables_for_char_encoding(bq_client):
    """Clears relevant tables before each test run for character encoding."""
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_dwh`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.dwh_ta_l_bindefr_aendgr_carm`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.isbert_schema.dwtk_meldungen`").result()

def test_character_encoding_and_case_statement(bq_client):
    """
    Tests the character encoding and CASE statement logic for upgradegrund.
    """
    target_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`"
    source_dwh = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_dwh`"
    source_carm = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.dwh_ta_l_bindefr_aendgr_carm`"
    source_meldungen = f"`{BQ_PROJECT_ID}.isbert_schema.dwtk_meldungen`"

    # --- Setup ---
    bq_client.insert_rows_json(source_dwh, [
        {"vertrags_id": "CHAR01", "vvl_aendgrund_id": 1, "aenderung_am": "2023-01-01 10:00:00"},
        {"vertrags_id": "CHAR02", "vvl_aendgrund_id": 2, "aenderung_am": "2023-01-02 11:00:00"},
        {"vertrags_id": "CHAR03", "vvl_aendgrund_id": 3, "aenderung_am": "2023-01-03 12:00:00"},
        {"vertrags_id": "CHAR04", "vvl_aendgrund_id": 4, "aenderung_am": "2023-01-04 13:00:00"},
    ]).result()

    bq_client.insert_rows_json(source_carm, [
        {"vvl_aendgrund_id": 1, "beschreibung": "DPPS Diensttyp A13 (EG-Upgrade)"}, # Should become 'Endgeraeteupgrade'
        {"vvl_aendgrund_id": 2, "beschreibung": "Änderung des Tarifs"}, # Should retain 'Ä'
        {"vvl_aendgrund_id": 3, "beschreibung": "Größenanpassung"}, # Should retain 'ö'
        {"vvl_aendgrund_id": 4, "beschreibung": "Fußball-Spezial"}, # Should retain 'ß'
    ]).result()

    bq_client.insert_rows_json(source_meldungen, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": "2023-01-01 00:00:00"},
    ]).result()

    # --- Action ---
    bq_client.query(f"CALL `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.d_ausd_v_ta_vvl_upgrade_sp`()").result()

    # --- Pass/Fail Criterion ---
    actual_output_query = f"""
    SELECT vertrags_id, upgradegrund
    FROM {target_table}
    ORDER BY vertrags_id;
    """
    actual_results = bq_client.query(actual_output_query).result()
    actual_output = {row.vertrags_id: row.upgradegrund for row in actual_results}

    expected_output = {
        "CHAR01": "Endgeraeteupgrade",
        "CHAR02": "Änderung des Tarifs",
        "CHAR03": "Größenanpassung",
        "CHAR04": "Fußball-Spezial",
    }

    assert actual_output == expected_output, \
        f"Character encoding or CASE statement logic mismatch. Expected: {expected_output}, Actual: {actual_output}"

```

---

### Test Case 10: Cloud Composer DAG Trigger and Parameter Passing

*   **Purpose**: To verify that the Cloud Composer DAG correctly triggers the BigQuery wrapper stored procedure and passes the `job_kennung` and `eintrags_nr` parameters as expected. This covers the orchestration layer replacement.
*   **Setup**:
    1.  **Target Environment**:
        *   Deploy the `vertragsdatenabgleich_vvl_upgrade_dag` to Cloud Composer.
        *   Ensure `your_gcp_project_id.your_bq_dataset_id.job_log` is empty.
        *   Ensure source tables are populated with valid data for a successful run.
*   **Action**:
    1.  Manually trigger the `vertragsdatenabgleich_vvl_upgrade_dag` in Cloud Composer.
    2.  Monitor the DAG run until completion.
    3.  Query the `job_log` table in BigQuery.
*   **Pass/Fail Criterion**:
    *   The DAG run in Cloud Composer must succeed.
    *   The `job_log` table must contain `STARTED` and `COMPLETED` entries for the `job_kennung` ('BERT_V_TA_VVL_UPGRADE') and `eintrags_nr` (derived from `ts_nodash` or `run_id` by Airflow) passed by the DAG.
    *   The `sof_ta_vvl_upgrade` table should be populated with data, indicating the core transformation was executed.

```python
import pytest
from google.cloud import bigquery
import time
import datetime
# For a full Airflow test, you'd integrate with Airflow's API or test harness.
# This example simulates the BQ interaction that Airflow would perform.

# Configuration placeholders - REPLACE WITH ACTUAL VALUES
BQ_PROJECT_ID = "your_gcp_project_id"
BQ_DATASET_ID = "your_bq_dataset_id"
AIRFLOW_DAG_ID = "vertragsdatenabgleich_vvl_upgrade_dag" # The DAG ID from the Python file

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT_ID)

@pytest.fixture(autouse=True)
def clear_tables_for_dag_test(bq_client):
    """Clears log and target tables, and populates source tables for a DAG test run."""
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`").result()

    source_dwh = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_dwh`"
    source_carm = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.dwh_ta_l_bindefr_aendgr_carm`"
    source_meldungen = f"`{BQ_PROJECT_ID}.isbert_schema.dwtk_meldungen`"
    bq_client.query(f"TRUNCATE TABLE {source_dwh}").result()
    bq_client.query(f"TRUNCATE TABLE {source_carm}").result()
    bq_client.query(f"TRUNCATE TABLE {source_meldungen}").result()
    bq_client.insert_rows_json(source_dwh, [
        {"vertrags_id": "DAG_V1", "vvl_aendgrund_id": 1, "aenderung_am": "2023-01-01 10:00:00"},
    ]).result()
    bq_client.insert_rows_json(source_carm, [
        {"vvl_aendgrund_id": 1, "beschreibung": "DAG Test Upgrade"},
    ]).result()
    bq_client.insert_rows_json(source_meldungen, [
        {"job_kennung": "BERT_DROP_TEMP_TABLE", "timecreated": "2023-01-01 00:00:00"},
    ]).result()


def test_airflow_dag_trigger_and_parameter_passing(bq_client):
    """
    Tests that the Airflow DAG correctly triggers the BQ SP and passes parameters.
    NOTE: This test directly calls the BQ SP, simulating the Airflow task.
    For a true Airflow integration test, you would use Airflow's API or CLI to trigger
    the DAG and then poll its status and check logs.
    """
    job_log_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.job_log`"
    target_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_vvl_upgrade`"

    # --- Action ---
    # Simulate parameters that Airflow's BigQueryExecuteStoredProcedureOperator would pass.
    # Airflow's `ts_nodash` format is YYYYMMDDTHHMMSS.
    current_time_str = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    mock_eintrags_nr = current_time_str
    mock_job_kennung = 'BERT_V_TA_VVL_UPGRADE' # As defined in the DAG

    wrapper_sp = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.vertragsdatenabgleich_wrapper_sp`"
    bq_client.query(f"CALL {wrapper_sp}('{mock_job_kennung}', '{mock_eintrags_nr}')").result()

    # --- Pass/Fail Criterion (Logging) ---
    log_query = f"""
    SELECT log_level, status, job_kennung, eintrags_nr
    FROM {job_log_table}
    WHERE job_kennung = '{mock_job_kennung}' AND eintrags_nr = '{mock_eintrags_nr}'
    ORDER BY log_timestamp ASC
    """
    time.sleep(5) # Give BigQuery streaming inserts a moment to propagate
    log_results = bq_client.query(log_query).result()
    log_entries = [tuple(row.values()) for row in log_results]

    assert len(log_entries) >= 2, "Expected at least 'STARTED' and 'COMPLETED' log entries from DAG run."

    started_entry = next((entry for entry in log_entries if entry[1] == 'STARTED'), None)
    assert started_entry is not None, "Expected 'STARTED' log entry not found."
    assert started_entry[2] == mock_job_kennung
    assert started_entry[3] == mock_eintrags_nr

    completed_entry = next((entry for entry in log_entries if entry[1] == 'COMPLETED'), None)
    assert completed_entry is not None, "Expected 'COMPLETED' log entry not found."
    assert completed_entry[2] == mock_job_kennung
    assert completed_entry[3] == mock_eintrags_nr

    # --- Pass/Fail Criterion (Data Population) ---
    final_row_count = bq_client.query(f"SELECT COUNT(*) FROM {target_table}").result().to_dataframe().iloc[0,0]
    assert final_row_count > 0, "Target table was not populated by the DAG-triggered job."

```