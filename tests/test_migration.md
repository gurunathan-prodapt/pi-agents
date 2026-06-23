Here are migration validation tests for the `k_ausd_bp_ta_cntrct_dist.ksh` job, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests for `k_ausd_bp_ta_cntrct_dist.ksh`

**Context:** These tests validate the migration of a KornShell orchestration script and its associated SQL logic to Google Cloud Platform, specifically BigQuery stored procedures orchestrated by Airflow. The tests aim to ensure behavioral equivalence between the legacy and migrated systems.

**Assumptions:**
*   A test environment is set up with BigQuery and Airflow.
*   `your_project_id` and `your_dataset_id` are placeholders for actual GCP project and BigQuery dataset IDs.
*   The legacy `d_ausd_bp_ta_cntrct_dist.sql` has been accurately translated into `d_ausd_bp_ta_cntrct_dist_core` BigQuery stored procedure. (Since the original SQL was not provided, tests for `d_ausd_bp_ta_cntrct_dist_core` are based on its placeholder implementation in the generated code).
*   Access to run the legacy ksh script and inspect its outputs (database state, log files, temporary files) is available for comparison.

---

### Test Case 1: Schema Validation of Target Tables

*   **Purpose:** Verify that the DDL for the target `PoolBasisprodukt` table and the logging tables (`error_log`, `job_log`) are correctly deployed in BigQuery and match the expected structure.
*   **Setup:**
    1.  Ensure the BigQuery dataset `your_dataset_id` exists in `your_project_id`.
    2.  Run the provided DDL scripts for `PoolBasisprodukt`, `error_log`, and `job_log` in the target BigQuery environment.
*   **Action:**
    1.  Inspect the schema of the created tables using BigQuery UI or `bq` command-line tool.
*   **Pass/Fail Criterion:**
    *   **Pass:** The schemas of `PoolBasisprodukt`, `error_log`, and `job_log` tables exist and match the DDL provided in the migration design document. Specifically:
        *   `PoolBasisprodukt` contains `contract_id`, `product_type`, `start_date`, `end_date`, `value`, `stichtag`, `processing_timestamp`.
        *   `error_log` contains `job_id`, `entry_number`, `reference_date`, `error_timestamp`, `error_message`, `component`, `severity`.
        *   `job_log` contains `job_id`, `entry_number`, `reference_date`, `start_time`, `end_time`, `status`, `record_count`, `restart_value`, `comment`, `processing_timestamp`.
    *   **Fail:** Any of the tables are missing, or their schemas do not match the DDL.

```sql
-- Example SQL to check schema (run for each table)
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'PoolBasisprodukt'
ORDER BY
    ordinal_position;

-- Expected output for PoolBasisprodukt (example):
-- column_name        data_type    is_nullable
-- contract_id        STRING       NO
-- product_type       STRING       YES
-- start_date         DATE         YES
-- end_date           DATE         YES
-- value              NUMERIC      YES
-- stichtag           DATE         NO
-- processing_timestamp TIMESTAMP    YES
```

---

### Test Case 2: Parameter Validation - Missing Required Arguments

*   **Purpose:** Verify that the migrated `r_ausd_bp_ta_cntrct_dist` stored procedure correctly identifies and raises an error for missing mandatory parameters, mirroring the legacy script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.
*   **Setup:**
    1.  Ensure `error_log` and `job_log` tables are empty.
    2.  Identify the expected error message and exit code from the legacy script when a parameter is missing (e.g., `ErrNr=193`).
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_cntrct_dist.ksh` with one or more mandatory parameters omitted (e.g., `./k_ausd_bp_ta_cntrct_dist.ksh -f 001 -s 01012023`). Capture the console output and exit code.
    2.  **Migrated:** Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` via BigQuery console or a test script, intentionally omitting a mandatory parameter (e.g., `p_JobKennung`).

```sql
-- Migrated: Call with missing p_JobKennung
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  NULL, -- Missing p_JobKennung
  '001',
  '01012023',
  '0'
);

-- Migrated: Call with missing p_Stichtag_raw
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'JOB_ID_TEST',
  '001',
  NULL, -- Missing p_Stichtag_raw
  '0'
);

-- Migrated: Call with missing p_EintragsNr
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'JOB_ID_TEST',
  NULL, -- Missing p_EintragsNr
  '01012023',
  '0'
);
```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   **Legacy:** The script exits with `ErrNr=193` and prints an error message similar to "FEHLER: 0 E 193 Jobkennung - Bitte ueber Rahmenscript aufrufen".
        *   **Migrated:** The BigQuery stored procedure call fails with an error message indicating a missing parameter (e.g., "Parameter validation failed: Missing required argument Jobkennung").
        *   An entry is recorded in `your_project_id.your_dataset_id.error_log` with `severity = 'ERROR'` and an appropriate `error_message`.
        *   An entry is recorded in `your_project_id.your_dataset_id.job_log` with `status = 'FAILED'` and `comment` reflecting the error.
    *   **Fail:** The migrated procedure executes successfully, or the error message/logging does not match the expected behavior.

---

### Test Case 3: Parameter Validation - Invalid Date Format for `p_Stichtag`

*   **Purpose:** Verify that the migrated `r_ausd_bp_ta_cntrct_dist` stored procedure correctly validates the `p_Stichtag_raw` format, mirroring the legacy script's `DWDate_Datum_Check` behavior.
*   **Setup:**
    1.  Ensure `error_log` and `job_log` tables are empty.
    2.  Identify the expected error message and exit code from the legacy script when `DWDate_Datum_Check` fails.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_cntrct_dist.ksh` with an invalid date format for `-s` (e.g., `./k_ausd_bp_ta_cntrct_dist.ksh -j JOB_ID -f 001 -s 2023-01-01`). Capture the console output and exit code.
    2.  **Migrated:** Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` with an invalid date format for `p_Stichtag_raw`.

```sql
-- Migrated: Call with invalid date format
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'JOB_ID_DATE_TEST',
  '002',
  '2023-01-01', -- Invalid DDMMYYYY format
  '0'
);
```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   **Legacy:** The script exits with an error related to date format validation.
        *   **Migrated:** The BigQuery stored procedure call fails with an error message similar to "Invalid date format for Stichtag: 2023-01-01. Expected DDMMYYYY."
        *   An entry is recorded in `your_project_id.your_dataset_id.error_log` with `severity = 'ERROR'` and an appropriate `error_message`.
        *   An entry is recorded in `your_project_id.your_dataset_id.job_log` with `status = 'FAILED'` and `comment` reflecting the error.
    *   **Fail:** The migrated procedure executes successfully, or the error message/logging does not match the expected behavior.

---

### Test Case 4: Default `p_wiederanlaufWert` Handling

*   **Purpose:** Verify that the migrated `r_ausd_bp_ta_cntrct_dist` stored procedure correctly defaults `p_wiederanlaufWert` to '0' if it's not provided, mirroring the legacy script's `if [[ -z "$p_wiederanlaufWert" ]]` logic.
*   **Setup:**
    1.  Ensure `job_log` table is empty.
    2.  Prepare a valid set of parameters for a successful run.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_cntrct_dist.ksh` without the `-l` parameter (e.g., `./k_ausd_bp_ta_cntrct_dist.ksh -j JOB_ID -f 001 -s 01012023`). Observe the internal value of `p_wiederanlaufWert` if possible (e.g., by adding a `print` statement in the legacy script).
    2.  **Migrated:** Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` with `p_wiederanlaufWert_raw` set to `NULL` or an empty string.

```sql
-- Migrated: Call with p_wiederanlaufWert_raw as NULL
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'JOB_ID_RESTART_TEST',
  '003',
  '01012023',
  NULL -- Omit or set to NULL
);

-- Migrated: Call with p_wiederanlaufWert_raw as empty string
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'JOB_ID_RESTART_TEST_EMPTY',
  '004',
  '01012023',
  '' -- Empty string
);
```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   **Legacy:** The script internally uses `p_wiederanlaufWert=0`.
        *   **Migrated:** The `restart_value` column in the `your_project_id.your_dataset_id.job_log` entry for the successful run is '0'.
    *   **Fail:** The `restart_value` in `job_log` is not '0' when `p_wiederanlaufWert_raw` is omitted or empty.

---

### Test Case 5: Date Derivation Correctness (`gestern.ksh` replacement)

*   **Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` in the migrated `r_ausd_bp_ta_cntrct_dist` procedure are correctly derived and passed to the core procedure, matching the behavior of `gestern.ksh`.
*   **Setup:**
    1.  Ensure `PoolBasisprodukt` table is empty.
    2.  Determine the current date and yesterday's date for the test execution day.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_cntrct_dist.ksh` with valid parameters. If possible, modify the legacy script temporarily to print `p_datum_heute` and `p_datum_gestern` before calling `starteSQLSkript`.
    2.  **Migrated:** Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` with valid parameters.

```sql
-- Migrated: Call with valid parameters
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'JOB_ID_DATE_DERIVATION',
  '005',
  '01012023', -- Example Stichtag
  '0'
);

-- After execution, check the PoolBasisprodukt table
SELECT
    start_date,
    end_date
FROM
    `your_project_id.your_dataset_id.PoolBasisprodukt`
WHERE
    contract_id = 'CONTRACT_005' AND stichtag = '2023-01-01';
```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   **Legacy:** `p_datum_heute` and `p_datum_gestern` are correctly derived (e.g., if run on 2023-10-27, `p_datum_heute` is '20231027' and `p_datum_gestern` is '20231026').
        *   **Migrated:** The `start_date` column in `PoolBasisprodukt` for the inserted record matches `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` and `end_date` matches `CURRENT_DATE()` at the time of execution of the stored procedure.
    *   **Fail:** The derived dates do not match the expected values.

---

### Test Case 6: Core Transformation Logic (`d_ausd_bp_ta_cntrct_dist_core`) - Output Parity

*   **Purpose:** Verify that the `d_ausd_bp_ta_cntrct_dist_core` BigQuery stored procedure produces the exact same data in `PoolBasisprodukt` as the legacy `d_ausd_bp_ta_cntrct_dist.sql` script for a given set of inputs. This is the primary test for "output parity" and "transformation correctness".
*   **Setup:**
    1.  **Legacy:** Set up a test environment with the legacy database and the `d_ausd_bp_ta_cntrct_dist.sql` script. Populate all source tables that `d_ausd_bp_ta_cntrct_dist.sql` reads from with a controlled, representative dataset (e.g., 100-1000 rows).
    2.  **Migrated:** Migrate the exact same source data into the corresponding BigQuery source tables. Ensure `PoolBasisprodukt` is empty in BigQuery.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_cntrct_dist.ksh` with a specific set of parameters (e.g., `-j JOB_A -f 100 -s 01012023 -l 0`). After execution, extract all data from the legacy `PoolBasisprodukt` table.
    2.  **Migrated:** Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` with the *exact same logical parameters* (e.g., `p_JobKennung='JOB_A'`, `p_EintragsNr='100'`, `p_Stichtag_raw='01012023'`, `p_wiederanlaufWert_raw='0'`). After execution, extract all data from the BigQuery `your_project_id.your_dataset_id.PoolBasisprodukt` table.
*   **Pass/Fail Criterion:**
    *   **Pass:** The data extracted from the legacy `PoolBasisprodukt` table is *identical* to the data extracted from the BigQuery `your_project_id.your_dataset_id.PoolBasisprodukt` table. This includes row count, column values, and data types (after BigQuery type mapping).
    *   **Fail:** Any discrepancy in row count or data content between the legacy and migrated target tables.

```python
# Example pytest assertion for data parity (assuming data is loaded into pandas DataFrames)
import pandas as pd
from google.cloud import bigquery

def test_pool_basisprodukt_data_parity():
    # --- Setup: Load legacy data (replace with actual legacy data extraction) ---
    # For demonstration, let's assume legacy_df is obtained from the legacy system
    # In a real scenario, this would involve connecting to the legacy DB and querying.
    legacy_data = {
        'contract_id': ['CONTRACT_100'],
        'product_type': ['JOB_A'],
        'start_date': [pd.to_datetime('2023-01-01').date()], # Assuming run on 2023-01-02
        'end_date': [pd.to_datetime('2023-01-02').date()],
        'value': [10000],
        'stichtag': [pd.to_datetime('2023-01-01').date()]
    }
    legacy_df = pd.DataFrame(legacy_data)
    # Add processing_timestamp if it was part of the legacy output and needs comparison
    # For this example, we'll ignore it as it's CURRENT_TIMESTAMP() in BQ.

    # --- Action: Run migrated job (assuming it's already run via Airflow or direct call) ---
    # For this test, we'd typically call the BQ SP directly or trigger the DAG.
    # Let's assume the BQ SP was called with:
    # CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
    #   'JOB_A', '100', '01012023', '0'
    # );

    # --- Action: Extract migrated data ---
    client = bigquery.Client(project='your_project_id')
    query = f"""
        SELECT
            contract_id,
            product_type,
            start_date,
            end_date,
            value,
            stichtag
        FROM
            `your_project_id.your_dataset_id.PoolBasisprodukt`
        WHERE
            stichtag = '2023-01-01' AND contract_id = 'CONTRACT_100'
        ORDER BY contract_id
    """
    migrated_df = client.query(query).to_dataframe()

    # Ensure consistent data types for comparison
    migrated_df['start_date'] = migrated_df['start_date'].dt.date
    migrated_df['end_date'] = migrated_df['end_date'].dt.date

    # --- Pass/Fail Criterion ---
    pd.testing.assert_frame_equal(
        legacy_df.sort_values(by=['contract_id']).reset_index(drop=True),
        migrated_df.sort_values(by=['contract_id']).reset_index(drop=True),
        check_dtype=True,
        check_exact=False # Use check_exact=False for float comparisons if needed
    )
```

---

### Test Case 7: Record Count Capture and Logging

*   **Purpose:** Verify that the migrated `r_ausd_bp_ta_cntrct_dist` procedure accurately captures the number of records processed/inserted into `PoolBasisprodukt` and logs it in the `job_log` table, replacing the legacy `cat $tmpFile` mechanism.
*   **Setup:**
    1.  Ensure `PoolBasisprodukt` and `job_log` tables are empty.
    2.  Prepare source data that will result in a known number of records being inserted into `PoolBasisprodukt` by `d_ausd_bp_ta_cntrct_dist_core`.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_cntrct_dist.ksh` with valid parameters. Capture the value of `v_records` from the script's output or the content of `bert_k_ausd_bp_ta_cntrct_dist.tmp`.
    2.  **Migrated:** Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` with valid parameters.
    3.  Query the `your_project_id.your_dataset_id.job_log` table for the corresponding job entry.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   **Legacy:** The `v_records` value (or `tmpFile` content) matches the actual number of rows inserted into the legacy `PoolBasisprodukt` table.
        *   **Migrated:** The `record_count` column in the `job_log` entry for the successful run matches the actual `COUNT(*)` of rows in `your_project_id.your_dataset_id.PoolBasisprodukt` for the given `stichtag`.
    *   **Fail:** The recorded `record_count` in `job_log` does not match the actual count in `PoolBasisprodukt`.

```sql
-- After running the migrated stored procedure:
SELECT
    record_count
FROM
    `your_project_id.your_dataset_id.job_log`
WHERE
    job_id = 'JOB_ID_RECORD_COUNT' AND entry_number = '006' AND reference_date = '2023-01-01'
ORDER BY start_time DESC
LIMIT 1;

-- Compare with:
SELECT
    COUNT(*)
FROM
    `your_project_id.your_dataset_id.PoolBasisprodukt`
WHERE
    stichtag = '2023-01-01';
```

---

### Test Case 8: End-to-End Success Scenario (Airflow DAG Execution)

*   **Purpose:** Verify that the Airflow DAG successfully orchestrates the BigQuery stored procedure, resulting in a complete and correct data load and logging.
*   **Setup:**
    1.  Deploy the `k_ausd_bp_ta_cntrct_dist_dag.py` to your Airflow environment.
    2.  Ensure all BigQuery target tables (`PoolBasisprodukt`, `error_log`, `job_log`) are empty.
    3.  Ensure BigQuery source tables are populated with a representative dataset.
*   **Action:**
    1.  Trigger the `k_ausd_bp_ta_cntrct_dist_dag` in Airflow with appropriate parameters (e.g., `execution_date` for `stichtag_raw`).
    2.  Monitor the DAG run in the Airflow UI.
    3.  After completion, query `PoolBasisprodukt`, `job_log`, and `error_log` tables in BigQuery.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The Airflow DAG run completes successfully (green status).
        *   `your_project_id.your_dataset_id.PoolBasisprodukt` contains the expected data (refer to Test Case 6 for data parity).
        *   `your_project_id.your_dataset_id.job_log` contains one entry for the job with `status = 'SUCCESS'`, correct `start_time`, `end_time`, `record_count`, and `comment`.
        *   `your_project_id.your_dataset_id.error_log` contains no entries related to this job run.
    *   **Fail:** The DAG fails, `PoolBasisprodukt` data is incorrect or incomplete, or logging entries are missing/incorrect.

---

### Test Case 9: Error Handling and Logging during Core Processing

*   **Purpose:** Verify that errors occurring within the `d_ausd_bp_ta_cntrct_dist_core` procedure are correctly caught, logged to `error_log` and `job_log`, and propagated up to the orchestrating `r_ausd_bp_ta_cntrct_dist` procedure and ultimately to Airflow.
*   **Setup:**
    1.  Modify the `d_ausd_bp_ta_cntrct_dist_core` procedure temporarily to force an error under specific conditions (e.g., `RAISE` if `p_EintragsNr` is a specific value, or cause a data type mismatch).
    2.  Ensure `error_log` and `job_log` tables are empty.
*   **Action:**
    1.  **Legacy:** Simulate an error in `d_ausd_bp_ta_cntrct_dist.sql` (if possible) and observe the `k_ausd_bp_ta_cntrct_dist.ksh` script's error handling and exit code.
    2.  **Migrated:** Trigger the Airflow DAG (or directly call `r_ausd_bp_ta_cntrct_dist`) with parameters that will activate the forced error in `d_ausd_bp_ta_cntrct_dist_core`.
    3.  Monitor the DAG run in Airflow.
    4.  Query `job_log` and `error_log` tables.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   **Legacy:** Script exits with an error code, and error messages are logged.
        *   **Migrated:** The Airflow DAG run fails (red status).
        *   `your_project_id.your_dataset_id.job_log` contains an entry for the job with `status = 'FAILED'` and `comment` reflecting the error from `d_ausd_bp_ta_cntrct_dist_core`.
        *   `your_project_id.your_dataset_id.error_log` contains an entry with `severity = 'ERROR'`, `component = 'd_ausd_bp_ta_cntrct_dist_core'`, and an `error_message` detailing the failure.
    *   **Fail:** The DAG completes successfully despite the error, or error logging is incomplete/incorrect.

---

### Test Case 10: Idempotency of `d_ausd_bp_ta_cntrct_dist_core`

*   **Purpose:** Verify that running the `d_ausd_bp_ta_cntrct_dist_core` procedure multiple times with the same parameters does not result in duplicate records or unintended side effects, given its `WHERE NOT EXISTS` clause.
*   **Setup:**
    1.  Ensure `PoolBasisprodukt` table is empty.
    2.  Prepare source data for a single successful run.
*   **Action:**
    1.  Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` with a specific set of parameters (e.g., `p_JobKennung='IDEMPOTENCY_TEST'`, `p_EintragsNr='007'`, `p_Stichtag_raw='01012023'`, `p_wiederanlaufWert_raw='0'`).
    2.  Immediately call the *same procedure with the exact same parameters* a second time.
    3.  Query the `your_project_id.your_dataset_id.PoolBasisprodukt` table for records matching these parameters.
*   **Pass/Fail Criterion:**
    *   **Pass:** After both calls, `your_project_id.your_dataset_id.PoolBasisprodukt` contains only *one* record for `contract_id = 'CONTRACT_007'` and `stichtag = '2023-01-01'`. The `job_log` should show two successful runs, but the `record_count` for the second run might be 0 if no new data was processed, or 1 if it counts existing rows. The key is that the target table state is consistent.
    *   **Fail:** Duplicate records are found in `PoolBasisprodukt` for the same `contract_id` and `stichtag`.

```sql
-- First call
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'IDEMPOTENCY_TEST',
  '007',
  '01012023',
  '0'
);

-- Second call (immediately after the first)
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  'IDEMPOTENCY_TEST',
  '007',
  '01012023',
  '0'
);

-- Check for duplicates
SELECT
    contract_id,
    stichtag,
    COUNT(*) as record_count
FROM
    `your_project_id.your_dataset_id.PoolBasisprodukt`
WHERE
    contract_id = 'CONTRACT_007' AND stichtag = '2023-01-01'
GROUP BY
    contract_id, stichtag
HAVING
    COUNT(*) > 1;

-- Expected result: 0 rows (no duplicates)
```