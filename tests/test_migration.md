As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `k_ausd_v_ta_action_assoc.ksh` to an Airflow DAG with BigQuery SQL. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

Each test case is structured with a clear purpose, detailed setup instructions, the actions to be performed, and concrete pass/fail criteria. Where applicable, runnable SQL or Python code snippets are provided to illustrate the assertions.

---

## Migration Validation Tests: `d_ausd_v_ta_action_assoc`

### Test Case 1: End-to-End Output Parity (Full Data Set)

*   **Purpose:** To verify that the migrated Airflow DAG produces an identical final dataset in the BigQuery target table (`sof_ta_action_assoc`) as the legacy KornShell script produces in its Oracle target table (`sof$ta_action_assoc`), given the same initial source data. This is the most comprehensive test for behavioral equivalence.

*   **Setup:**
    1.  **Source Data Snapshot:** Take a consistent snapshot of all relevant source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_action_assoc`) from the Oracle legacy environment.
    2.  **BigQuery Source Ingestion:** Load this exact snapshot data into the corresponding BigQuery source tables (`isbert_schema.dwtk_meldungen`, `cds_ta_action_assoc`). Ensure data types and values are accurately preserved during this ingestion.
    3.  **Legacy Job Execution:** Execute the legacy `k_ausd_v_ta_action_assoc.ksh` job in the Oracle environment.
    4.  **Extract Legacy Output:** Extract the entire content of the `sof$ta_action_assoc` table from Oracle into a temporary, canonical format (e.g., a CSV file, a staging table in BigQuery, or a comparison database).
    5.  **Clean BigQuery Target:** Ensure the `sof_ta_action_assoc` table in BigQuery is empty before the migrated job runs.

*   **Action:**
    1.  Trigger the Airflow DAG `d_ausd_v_ta_action_assoc` in the GCP Airflow environment.
    2.  After the DAG completes successfully, extract the entire content of the `sof_ta_action_assoc` table from BigQuery.

*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully without any errors.
    *   The extracted data from BigQuery's `sof_ta_action_assoc` table is **byte-for-byte identical** (or row-for-row identical when considering column order and data types) to the extracted data from Oracle's `sof$ta_action_assoc` table. This includes matching row counts and all column values.
    *   A robust data comparison tool or SQL `EXCEPT DISTINCT` queries (as shown below) should yield no differences.

```sql
-- Example SQL for comparing two tables (assuming both are in BigQuery,
-- 'legacy_sof_ta_action_assoc' contains the extracted Oracle data,
-- and 'migrated_sof_ta_action_assoc' contains the BigQuery job output).

-- Check for rows present in legacy output but missing in migrated output
SELECT 'Only in Legacy' AS source, *
FROM `your_project.your_dataset.legacy_sof_ta_action_assoc`
EXCEPT DISTINCT
SELECT 'Only in Legacy' AS source, *
FROM `your_project.your_dataset.migrated_sof_ta_action_assoc`;

-- Check for rows present in migrated output but missing in legacy output
SELECT 'Only in Migrated' AS source, *
FROM `your_project.your_dataset.migrated_sof_ta_action_assoc`
EXCEPT DISTINCT
SELECT 'Only in Migrated' AS source, *
FROM `your_project.your_dataset.legacy_sof_ta_action_assoc`;

-- Assert row counts are identical
SELECT
    (SELECT COUNT(*) FROM `your_project.your_dataset.legacy_sof_ta_action_assoc`) AS legacy_row_count,
    (SELECT COUNT(*) FROM `your_project.your_dataset.migrated_sof_ta_action_assoc`) AS migrated_row_count,
    CASE
        WHEN (SELECT COUNT(*) FROM `your_project.your_dataset.legacy_sof_ta_action_assoc`) =
             (SELECT COUNT(*) FROM `your_project.your_dataset.migrated_sof_ta_action_assoc`)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS row_count_status;
```

### Test Case 2: `v_datum` Calculation Correctness

*   **Purpose:** To verify that the `v_datum` variable, which serves as a critical cutoff date for filtering, is calculated identically in both the legacy Oracle SQL and the migrated BigQuery SQL. This specifically tests the `MAX(timecreated)`, `COALESCE`/`NVL`, and date formatting logic.

*   **Setup:**
    1.  **Source Data Scenarios:** Populate the `isbert_schema.dwtk_meldungen` table in both Oracle and BigQuery with various data scenarios to test edge cases for `v_datum` calculation:
        *   Multiple records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with different `timecreated` values (to test `MAX`).
        *   A single record for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   No records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` (to test `COALESCE`/`NVL` defaulting to '19000101').
        *   `timecreated` values that are NULL for `job_kennung = 'BERT_DROP_TEMP_TABLE'` (if the schema allows NULLs for this column).
    2.  **Isolate Calculation:** Prepare SQL queries to execute only the `v_datum` calculation logic in both environments, without running the full job.

*   **Action:**
    1.  Execute the Oracle SQL query to determine `v_datum` for each scenario and record the resulting string.
    2.  Execute the BigQuery SQL query to determine `v_datum` for each scenario and record the resulting string.

*   **Pass/Fail Criterion:**
    *   For each test scenario, the `v_datum` string value obtained from Oracle must be **identical** to the `v_datum` string value obtained from BigQuery.

```sql
-- Oracle SQL to get v_datum (simplified for testing)
SELECT NVL(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101') AS v_datum_oracle
FROM isbert_schema.dwtk_meldungen m
WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- BigQuery SQL to get v_datum (simplified for testing)
DECLARE v_datum_bq STRING;
SET v_datum_bq = (
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM
        `isbert_schema.dwtk_meldungen` m
    WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);
SELECT v_datum_bq;
```

### Test Case 3: Filtering Logic Correctness (Edge Cases & NULLs)

*   **Purpose:** To verify that all filtering conditions, particularly those involving date comparisons and NULL handling, behave identically in BigQuery as they do in Oracle. This covers `DATE(column) <= v_datum`, `is_production = 1`, and the `OR` conditions for `modified_at` and `valid_to`.

*   **Setup:**
    1.  **Controlled Source Data:** Create a small, highly controlled dataset in `cds$ta_action_assoc` (Oracle) and `cds_ta_action_assoc` (BigQuery) that specifically targets each filter condition and its boundaries. This should include:
        *   `insert_at`: values exactly on `v_datum`, before `v_datum`, and after `v_datum`.
        *   `valid_from`: values exactly on `v_datum`, before `v_datum`, and after `v_datum`.
        *   `is_production`: values `1` and `0`.
        *   `modified_at`: `NULL`, values exactly on `v_datum`, before `v_datum`, and after `v_datum`.
        *   `valid_to`: `NULL`, values exactly on `v_datum`, before `v_datum`, and after `v_datum`.
        *   Combinations of these conditions (e.g., `modified_at IS NULL` AND `valid_to` after `v_datum`).
    2.  **Fixed `v_datum`:** For this test, set `v_datum` to a known, fixed date string (e.g., '20230115') in both environments to isolate the filtering logic from the `v_datum` calculation itself.
    3.  **Extract Filtered Data:** Run *only* the `SELECT` part of the `INSERT` statement in both Oracle and BigQuery to get the rows that *would* be inserted.

*   **Action:**
    1.  Execute the Oracle `SELECT` query with the controlled data and record the resulting `(cntrct_id, rv_action_id)` pairs.
    2.  Execute the BigQuery `SELECT` query with the controlled data and record the resulting `(cntrct_id, rv_action_id)` pairs.

*   **Pass/Fail Criterion:**
    *   The set of `(cntrct_id, rv_action_id)` pairs returned by the Oracle `SELECT` query must be **identical** to the set returned by the BigQuery `SELECT` query for each controlled data scenario.

```sql
-- Oracle SQL to test filtering (replace '&v_datum' with a fixed date string like '20230115')
SELECT
    ac.cntrct_id,
    ac.rv_action_id
FROM
    cds$ta_action_assoc ac
WHERE
    TRUNC(ac.insert_at)      <= TO_DATE('&v_datum', 'YYYYMMDD')
    AND TRUNC(ac.valid_from)     <= TO_DATE('&v_datum', 'YYYYMMDD')
    AND ac.is_production   = 1
    AND ( ac.modified_at IS NULL OR TRUNC(ac.modified_at) > TO_DATE('&v_datum', 'YYYYMMDD') )
    AND ( ac.valid_to    IS NULL OR TRUNC(ac.valid_to)    > TO_DATE('&v_datum', 'YYYYMMDD') );

-- BigQuery SQL to test filtering (replace 'v_datum_fixed' with a fixed date string like '20230115')
DECLARE v_datum_fixed STRING DEFAULT '20230115';
SELECT
    ac.cntrct_id,
    ac.rv_action_id
FROM
    `cds_ta_action_assoc` ac
WHERE
    DATE(ac.insert_at)      <= PARSE_DATE('%Y%m%d', v_datum_fixed)
    AND DATE(ac.valid_from)     <= PARSE_DATE('%Y%m%d', v_datum_fixed)
    AND ac.is_production   = 1
    AND ( ac.modified_at IS NULL OR DATE(ac.modified_at) > PARSE_DATE('%Y%m%d', v_datum_fixed) )
    AND ( ac.valid_to    IS NULL OR DATE(ac.valid_to)    > PARSE_DATE('%Y%m%d', v_datum_fixed) );
```

### Test Case 4: Target Table Truncation Behavior

*   **Purpose:** To verify that the `TRUNCATE TABLE` operation in BigQuery behaves identically to the `DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_action_assoc')` call in Oracle, ensuring the target table is empty before new data is inserted.

*   **Setup:**
    1.  **Populate Target:** Insert a known number of dummy records (e.g., 5-10 rows) into `sof$ta_action_assoc` (Oracle) and `sof_ta_action_assoc` (BigQuery).
    2.  **Isolate Truncation:** For Oracle, prepare to execute only the `DWPA_UTIL_SKRIPT.runstatement` call. For BigQuery, prepare to execute only the `TRUNCATE TABLE` statement.

*   **Action:**
    1.  Execute the truncation command in Oracle.
    2.  Execute the truncation command in BigQuery.
    3.  Query the row count of both tables immediately after truncation.

*   **Pass/Fail Criterion:**
    *   Both `sof$ta_action_assoc` in Oracle and `sof_ta_action_assoc` in BigQuery must report a row count of **0** after their respective truncation operations.

```sql
-- Oracle SQL to verify truncation
-- Pre-condition: INSERT INTO sof$ta_action_assoc (cntrct_id, rv_action_id) VALUES (1,1); COMMIT;
-- Action: EXEC isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_action_assoc');
-- Verification:
SELECT COUNT(*) FROM sof$ta_action_assoc;

-- BigQuery SQL to verify truncation
-- Pre-condition: INSERT INTO `sof_ta_action_assoc` (cntrct_id, rv_action_id) VALUES (1,1);
-- Action:
TRUNCATE TABLE `sof_ta_action_assoc`;
-- Verification:
SELECT COUNT(*) FROM `sof_ta_action_assoc`;
```

### Test Case 5: Row Count and Schema Assertions

*   **Purpose:** To verify that the total number of rows inserted into the target table is consistent between the legacy and migrated jobs, and that the target table schema (column names and data types) is preserved or correctly mapped.

*   **Setup:**
    1.  **Identical Source Data:** Use the same source data setup as described in Test Case 1 (End-to-End Output Parity).
    2.  **Run Both Jobs:** Execute both the legacy KornShell job and the migrated Airflow DAG to completion.

*   **Action:**
    1.  Retrieve the final row count from Oracle's `sof$ta_action_assoc`.
    2.  Retrieve the final row count from BigQuery's `sof_ta_action_assoc`.
    3.  Retrieve the schema (column names and their logical data types) for both tables.

*   **Pass/Fail Criterion:**
    *   The row count in `sof_ta_action_assoc` (BigQuery) must **exactly match** the row count in `sof$ta_action_assoc` (Oracle).
    *   The schema of `sof_ta_action_assoc` (BigQuery) must logically match the schema of `sof$ta_action_assoc` (Oracle). This means column names must be identical, and data types must be equivalent (e.g., Oracle `NUMBER(X,0)` maps to BigQuery `INT64`, Oracle `VARCHAR2(X)` maps to BigQuery `STRING`, Oracle `DATE`/`TIMESTAMP` maps to BigQuery `DATE`/`TIMESTAMP`).

```python
# Example pytest-based assertion for row counts and schema comparison
import pytest
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for Oracle connection

# Helper functions (to be implemented or adapted based on actual environment)
def get_oracle_row_count(connection_string, table_name):
    # Connect to Oracle, execute COUNT(*), fetch result
    pass

def get_bigquery_row_count(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return table.num_rows

def get_oracle_schema(connection_string, table_name):
    # Connect to Oracle, query ALL_TAB_COLUMNS, return dict {col_name: data_type}
    pass

def get_bigquery_schema(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return {field.name.lower(): field.field_type.lower() for field in table.schema}

def test_row_count_and_schema_parity():
    oracle_conn_str = "user/password@host:port/service_name"
    bq_project_id = "your-gcp-project-id"
    bq_dataset_id = "your_dataset"
    oracle_target_table = "SOF$TA_ACTION_ASSOC" # Oracle table names are often uppercase
    bq_target_table = "sof_ta_action_assoc"

    # --- Row Count Assertion ---
    legacy_row_count = get_oracle_row_count(oracle_conn_str, oracle_target_table)
    migrated_row_count = get_bigquery_row_count(bq_project_id, bq_dataset_id, bq_target_table)

    assert legacy_row_count == migrated_row_count, \
        f"Row count mismatch: Oracle={legacy_row_count}, BigQuery={migrated_row_count}"
    print(f"PASS: Row count matches ({legacy_row_count} rows).")

    # --- Schema Assertion ---
    legacy_schema = get_oracle_schema(oracle_conn_str, oracle_target_table)
    migrated_schema = get_bigquery_schema(bq_project_id, bq_dataset_id, bq_target_table)

    # Basic check for column names
    assert set(legacy_schema.keys()) == set(migrated_schema.keys()), \
        f"Column name mismatch: Oracle={sorted(legacy_schema.keys())}, BigQuery={sorted(migrated_schema.keys())}"

    # More detailed type mapping check (requires a predefined mapping)
    # Example mapping:
    # type_map = {
    #     'number': 'int64',
    #     'varchar2': 'string',
    #     'date': 'date', # Note: Oracle DATE can include time, BigQuery DATE is date-only
    #     'timestamp(6)': 'timestamp'
    # }
    # for col_name, oracle_type in legacy_schema.items():
    #     expected_bq_type = type_map.get(oracle_type.split('(')[0].lower()) # Handle types like NUMBER(X,Y)
    #     actual_bq_type = migrated_schema.get(col_name)
    #     assert expected_bq_type == actual_bq_type, \
    #         f"Type mismatch for column '{col_name}': Oracle '{oracle_type}' -> Expected BQ '{expected_bq_type}', Actual BQ '{actual_bq_type}'"

    print(f"PASS: Schema column names match: {list(legacy_schema.keys())}")
    # Add more specific type comparison assertions here if a detailed type map is available.
```

### Test Case 6: External System Replacement - Source Data Ingestion

*   **Purpose:** To verify that the data ingested from the original Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_action_assoc`) into their BigQuery counterparts (`isbert_schema.dwtk_meldungen`, `cds_ta_action_assoc`) is complete and accurate. This is a crucial prerequisite for all other data-centric tests.

*   **Setup:**
    1.  **Oracle Source Snapshot:** Take a snapshot of `isbert_schema.dwtk_meldungen` and `cds$ta_action_assoc` from the Oracle environment at a specific point in time.
    2.  **BigQuery Ingestion:** Ensure the chosen ingestion method (e.g., BigQuery Data Transfer Service, custom ETL process, one-time data load) has completed for these tables.

*   **Action:**
    1.  Query `isbert_schema.dwtk_meldungen` and `cds$ta_action_assoc` in Oracle to obtain row counts and checksums (e.g., sum of a numeric column, hash of concatenated primary key columns).
    2.  Query `isbert_schema.dwtk_meldungen` and `cds_ta_action_assoc` in BigQuery to obtain row counts and checksums.
    3.  Perform spot checks by querying specific records or ranges of data in both systems and comparing their values.

*   **Pass/Fail Criterion:**
    *   For each source table, the row count in BigQuery must **exactly match** the row count in Oracle.
    *   For each source table, the checksums (if implemented) must **exactly match**, indicating data integrity.
    *   Spot checks confirm that data values for critical columns are identical.

```sql
-- Example SQL for row count and basic checksum comparison for a source table (e.g., cds_ta_action_assoc)
-- Replace 'cntrct_id' with an appropriate numeric column for SUM checksum, or use HASH for multiple columns.

-- Oracle Query
SELECT
    COUNT(*) AS oracle_count,
    SUM(cntrct_id) AS oracle_checksum_cntrct_id -- Example checksum
FROM cds$ta_action_assoc;

-- BigQuery Query
SELECT
    COUNT(*) AS bq_count,
    SUM(cntrct_id) AS bq_checksum_cntrct_id -- Example checksum
FROM `cds_ta_action_assoc`;

-- Pass/Fail: Manually compare the results from both queries.
-- Expected: oracle_count = bq_count AND oracle_checksum_cntrct_id = bq_checksum_cntrct_id
```

### Test Case 7: Airflow Orchestration and Parameter Handling

*   **Purpose:** To verify that the Airflow DAG correctly orchestrates the BigQuery job, handles parameters (even if `p_EintragsNr` is explicitly omitted), and logs execution details appropriately, effectively replacing the KornShell script's role as the job orchestrator.

*   **Setup:**
    1.  Deploy the `d_ausd_v_ta_action_assoc.py` DAG to a functional Airflow environment (e.g., GCP Composer).
    2.  Ensure the `google_cloud_default` BigQuery connection is correctly configured in Airflow.
    3.  Ensure necessary BigQuery permissions are granted to the Airflow service account.

*   **Action:**
    1.  Manually trigger the `d_ausd_v_ta_action_assoc` DAG via the Airflow UI or Airflow CLI.
    2.  Monitor the DAG run in the Airflow UI for task status, logs, and overall duration.
    3.  Review the BigQuery job history in the GCP Console to confirm the execution of the SQL query.

*   **Pass/Fail Criterion:**
    *   The Airflow DAG runs successfully to completion (all tasks show a 'success' status).
    *   Airflow task logs for `process_ta_action_assoc` clearly show the BigQuery query being executed without errors.
    *   BigQuery job history confirms the successful execution of the SQL query submitted by Airflow.
    *   No errors related to missing parameters, environment setup, or BigQuery connectivity are observed in Airflow or BigQuery logs.
    *   (Optional but recommended) Verify that the `job_kennung = 'BERT_DROP_TEMP_TABLE'` filter in the BigQuery query is correctly applied, confirming the implicit handling of `p_JobKennung`.

```python
# This test case primarily involves observation and verification through Airflow and GCP consoles.
# Automated testing could involve Airflow API calls to trigger and monitor DAG runs.

# Example conceptual Python code for triggering and monitoring (requires Airflow API client setup):
# from airflow.api.client.local_client import Client
# import time
#
# client = Client(None, None) # Or configure for remote Airflow API
#
# try:
#     dag_run = client.trigger_dag(dag_id="d_ausd_v_ta_action_assoc")
#     print(f"DAG run triggered: {dag_run.run_id}")
#
#     # Poll for DAG run status (simplified)
#     timeout_seconds = 300 # 5 minutes
#     start_time = time.time()
#     while time.time() - start_time < timeout_seconds:
#         current_state = client.get_dag_run_state(dag_id="d_ausd_v_ta_action_assoc", run_id=dag_run.run_id)
#         if current_state in ["success", "failed"]:
#             break
#         time.sleep(10)
#
#     assert current_state == "success", f"Airflow DAG run failed with state: {current_state}"
#     print(f"PASS: Airflow DAG '{dag_run.dag_id}' completed successfully.")
#
#     # Further checks would involve querying BigQuery job history via GCP API
#     # and parsing Airflow task logs for specific content.
#
# except Exception as e:
#     pytest.fail(f"Airflow orchestration test failed: {e}")
```