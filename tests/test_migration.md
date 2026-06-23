As a senior data-migration QA engineer, I've analyzed the provided KornShell script and the BigQuery migration design. The core of the KornShell script is orchestration, parameter handling, and error management, with the actual data transformation delegated to an external SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`). The migration design correctly identifies that the SQL script's logic needs separate migration.

My test plan will focus on validating the behavioral equivalence of the *orchestration* logic, parameter handling, date management, error handling, and logging mechanisms as they transition from KornShell to a BigQuery Stored Procedure.

**Assumptions for Testing:**

1.  **`your_project_id.your_dataset_id`:** This placeholder will be replaced with actual BigQuery project and dataset IDs during execution.
2.  **`target_table`:** A BigQuery table named `your_project_id.your_dataset_id.target_table` exists, with at least a `process_date` column (DATE type) and other columns as required by the migrated `d_ausd_bp_ta_bcp_msisdn.sql` logic. This table will be used to simulate the output of the core data processing.
3.  **Core SQL Logic:** The placeholder for the core data processing logic (from `d_ausd_bp_ta_bcp_msisdn.sql`) within the BigQuery stored procedure is assumed to be correctly implemented and will produce a predictable number of records based on input parameters and pre-loaded source data. For these tests, we will simulate its effect on `target_table`.
4.  **`job_log` table:** The `your_project_id.your_dataset_id.job_log` table is created as per the provided DDL.

---

## Migration Validation Tests for `k_ausd_bp_ta_bcp_msisdn.ksh` to BigQuery Stored Procedure

### Test Case 1: Successful Execution - Happy Path

**Purpose:** Verify that the BigQuery Stored Procedure executes successfully with valid parameters, performs its orchestration role, and logs a successful entry with the correct record count.

**Setup:**
1.  Ensure `your_project_id.your_dataset_id.job_log` table is empty.
2.  Populate `your_project_id.your_dataset_id.target_table` with sample data that would result from a successful run of the core SQL logic for a specific `Stichtag`. For example, insert 10 records with `process_date = '2023-01-01'`.

    ```sql
    -- Clear previous test data
    DELETE FROM `your_project_id.your_dataset_id.job_log` WHERE TRUE;
    DELETE FROM `your_project_id.your_dataset_id.target_table` WHERE TRUE;

    -- Insert sample data into target_table for a specific Stichtag
    INSERT INTO `your_project_id.your_dataset_id.target_table` (col1, col2, process_date)
    SELECT 'data_a', 1, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_b', 2, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_c', 3, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_d', 4, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_e', 5, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_f', 6, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_g', 7, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_h', 8, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_i', 9, PARSE_DATE('%Y-%m-%d', '2023-01-01') UNION ALL
    SELECT 'data_j', 10, PARSE_DATE('%Y-%m-%d', '2023-01-01');
    ```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters:

```sql
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_01', 'ENTRY_01', '01012023', '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without error.
2.  A single record is inserted into `your_project_id.your_dataset_id.job_log` with the following characteristics:
    *   `job_name` = 'PoolBasisprodukt'
    *   `entry_nr` = 'ENTRY_01'
    *   `stichtag` = '01012023'
    *   `restart_value` = '0'
    *   `records_processed` = 10 (matching the setup data)
    *   `status` = 'SUCCESS'
    *   `created_at` is a recent timestamp.
    *   `error_message` IS NULL.

```sql
-- Pytest assertion (example)
def test_successful_execution(bq_client):
    # ... setup code ...
    bq_client.query("""
        CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
            'TEST_JOB_ID_01', 'ENTRY_01', '01012023', '0'
        );
    """).result()

    results = bq_client.query("""
        SELECT job_name, entry_nr, stichtag, restart_value, records_processed, status, error_message
        FROM `your_project_id.your_dataset_id.job_log`
        WHERE entry_nr = 'ENTRY_01'
    """).to_dataframe()

    assert len(results) == 1
    assert results.iloc[0]['job_name'] == 'PoolBasisprodukt'
    assert results.iloc[0]['entry_nr'] == 'ENTRY_01'
    assert results.iloc[0]['stichtag'] == '01012023'
    assert results.iloc[0]['restart_value'] == '0'
    assert results.iloc[0]['records_processed'] == 10
    assert results.iloc[0]['status'] == 'SUCCESS'
    assert results.iloc[0]['error_message'] is None
```

### Test Case 2: Missing `JobKennung` Parameter

**Purpose:** Verify that the BigQuery Stored Procedure correctly identifies and handles a missing `JobKennung` parameter, failing with the expected error message and logging the failure. This mirrors the `pruefeParameterGesetzt Jobkennung p_JobKennung` check in the legacy script.

**Setup:**
1.  Ensure `your_project_id.your_dataset_id.job_log` table is empty.

**Action:**
Execute the BigQuery Stored Procedure with `p_JobKennung` as NULL or empty string.

```sql
-- Attempt 1: NULL
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  NULL, 'ENTRY_02', '02012023', '0'
);

-- Attempt 2: Empty string
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  '', 'ENTRY_02', '02012023', '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure execution fails with an error message containing "Jobkennung fehlt".
2.  A single record is inserted into `your_project_id.your_dataset_id.job_log` with:
    *   `job_name` = 'PoolBasisprodukt'
    *   `entry_nr` = 'ENTRY_02'
    *   `stichtag` = '02012023'
    *   `status` = 'FAILED'
    *   `error_message` containing "Jobkennung fehlt".
    *   `records_processed` = 0 (as the core logic wasn't executed).

```sql
-- Pytest assertion (example)
import pytest

def test_missing_jobkennung(bq_client):
    # ... setup code ...
    with pytest.raises(Exception, match="Jobkennung fehlt"):
        bq_client.query("""
            CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
                NULL, 'ENTRY_02', '02012023', '0'
            );
        """).result()

    results = bq_client.query("""
        SELECT job_name, entry_nr, stichtag, status, error_message, records_processed
        FROM `your_project_id.your_dataset_id.job_log`
        WHERE entry_nr = 'ENTRY_02' AND status = 'FAILED'
    """).to_dataframe()

    assert len(results) == 1
    assert results.iloc[0]['job_name'] == 'PoolBasisprodukt'
    assert results.iloc[0]['entry_nr'] == 'ENTRY_02'
    assert results.iloc[0]['stichtag'] == '02012023'
    assert results.iloc[0]['status'] == 'FAILED'
    assert "Jobkennung fehlt" in results.iloc[0]['error_message']
    assert results.iloc[0]['records_processed'] == 0
```

### Test Case 3: Missing `EintragsNr` Parameter

**Purpose:** Verify that the BigQuery Stored Procedure correctly identifies and handles a missing `EintragsNr` parameter, failing with the expected error message and logging the failure. This mirrors the `pruefeParameterGesetzt EintragsNr p_EintragsNr` check.

**Setup:**
1.  Ensure `your_project_id.your_dataset_id.job_log` table is empty.

**Action:**
Execute the BigQuery Stored Procedure with `p_EintragsNr` as NULL or empty string.

```sql
-- Attempt 1: NULL
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_03', NULL, '03012023', '0'
);

-- Attempt 2: Empty string
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_03', '', '03012023', '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure execution fails with an error message containing "EintragsNr fehlt".
2.  A single record is inserted into `your_project_id.your_dataset_id.job_log` with:
    *   `job_name` = 'PoolBasisprodukt'
    *   `entry_nr` = NULL or '' (depending on how BigQuery handles the `RAISE` before `entry_nr` is fully processed for logging)
    *   `stichtag` = '03012023'
    *   `status` = 'FAILED'
    *   `error_message` containing "EintragsNr fehlt".
    *   `records_processed` = 0.

### Test Case 4: Missing `Stichtag` Parameter

**Purpose:** Verify that the BigQuery Stored Procedure correctly identifies and handles a missing `Stichtag` parameter, failing with the expected error message and logging the failure. This mirrors the `pruefeParameterGesetzt Stichtag p_Stichtag` check.

**Setup:**
1.  Ensure `your_project_id.your_dataset_id.job_log` table is empty.

**Action:**
Execute the BigQuery Stored Procedure with `p_Stichtag` as NULL or empty string.

```sql
-- Attempt 1: NULL
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_04', 'ENTRY_04', NULL, '0'
);

-- Attempt 2: Empty string
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_04', 'ENTRY_04', '', '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure execution fails with an error message containing "Stichtag fehlt".
2.  A single record is inserted into `your_project_id.your_dataset_id.job_log` with:
    *   `job_name` = 'PoolBasisprodukt'
    *   `entry_nr` = 'ENTRY_04'
    *   `stichtag` = NULL or ''
    *   `status` = 'FAILED'
    *   `error_message` containing "Stichtag fehlt".
    *   `records_processed` = 0.

### Test Case 5: Invalid `Stichtag` Format

**Purpose:** Verify that the BigQuery Stored Procedure correctly validates the `Stichtag` format (DDMMYYYY), failing with the expected error message if the format is incorrect. This mirrors the `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` in the legacy script.

**Setup:**
1.  Ensure `your_project_id.your_dataset_id.job_log` table is empty.

**Action:**
Execute the BigQuery Stored Procedure with an invalid `Stichtag` format (e.g., YYYYMMDD, or non-date string).

```sql
-- Invalid format: YYYYMMDD
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_05', 'ENTRY_05', '20230105', '0'
);

-- Invalid format: non-date string
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_05', 'ENTRY_05', 'NOTADATE', '0'
);

-- Invalid format: partial date
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_05', 'ENTRY_05', '050123', '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure execution fails with an error message containing "Stichtag hat kein gueltiges Format DDMMYYYY".
2.  A single record is inserted into `your_project_id.your_dataset_id.job_log` with:
    *   `job_name` = 'PoolBasisprodukt'
    *   `entry_nr` = 'ENTRY_05'
    *   `stichtag` = (the invalid date string provided)
    *   `status` = 'FAILED'
    *   `error_message` containing "Stichtag hat kein gueltiges Format DDMMYYYY".
    *   `records_processed` = 0.

### Test Case 6: `wiederanlaufWert` Default Handling

**Purpose:** Verify that the `p_wiederanlaufWert` parameter correctly defaults to '0' if not provided or empty, matching the legacy script's `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi` logic.

**Setup:**
1.  Ensure `your_project_id.your_dataset_id.job_log` table is empty.
2.  Populate `your_project_id.your_dataset_id.target_table` with sample data for `06012023` (e.g., 5 records).

**Action:**
Execute the BigQuery Stored Procedure with `p_wiederanlaufWert` as NULL or empty string.

```sql
-- Attempt 1: NULL
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_06', 'ENTRY_06', '06012023', NULL
);

-- Attempt 2: Empty string
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_06', 'ENTRY_06', '06012023', ''
);
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully in both attempts.
2.  Two records are inserted into `your_project_id.your_dataset_id.job_log`, both with:
    *   `job_name` = 'PoolBasisprodukt'
    *   `entry_nr` = 'ENTRY_06'
    *   `stichtag` = '06012023'
    *   `restart_value` = '0' (this is the key check)
    *   `records_processed` = 5
    *   `status` = 'SUCCESS'
    *   `error_message` IS NULL.

### Test Case 7: Date Derivation Parity (`gestern.ksh` replacement)

**Purpose:** Verify that the BigQuery Stored Procedure's internal date variables (`v_datum_heute`, `v_datum_gestern`) correctly reflect today's and yesterday's dates, equivalent to the `gestern.ksh` utility.

**Setup:**
1.  This test requires a slight modification to the BigQuery Stored Procedure to expose `v_datum_heute` and `v_datum_gestern` in the `job_log` or as a `SELECT` statement at the end for verification. For example, add them to the `job_log` table or return them in the final `SELECT` statement.
2.  Alternatively, the test can rely on the `created_at` timestamp in `job_log` and the `stichtag` to infer the correct date logic, but direct exposure is better for this specific test.
3.  For this test, let's assume `target_table` is populated such that `COUNT(*)` for `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` would yield 0.

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```sql
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_07', 'ENTRY_07', '07012023', '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully.
2.  If `v_datum_heute` and `v_datum_gestern` were added to the `job_log` or returned:
    *   `v_datum_heute` should be `CURRENT_DATE()` at the time of execution.
    *   `v_datum_gestern` should be `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` at the time of execution.
3.  The `job_log` entry for this run should reflect these dates if they were used in the core logic (e.g., if the core logic filtered by `v_datum_heute` or `v_datum_gestern`).

*(Note: The current BigQuery pseudocode doesn't directly use `v_datum_heute` or `v_datum_gestern` in the `target_table` query, so this test would require modifying the procedure to log or return these values explicitly for direct verification, or ensuring the core logic uses them in a way that impacts the `records_processed` count.)*

### Test Case 8: Core SQL Logic Failure Simulation

**Purpose:** Verify that if the internal BigQuery SQL logic (migrated from `d_ausd_bp_ta_bcp_msisdn.sql`) encounters an error, the stored procedure catches it, logs the failure, and re-raises the error. This tests the `BEGIN...EXCEPTION WHEN ERROR THEN...END` block.

**Setup:**
1.  Ensure `your_project_id.your_dataset_id.job_log` table is empty.
2.  **Modify the BigQuery Stored Procedure temporarily** to force an error within the `BEGIN...END` block that contains the core processing logic. For example, attempt to divide by zero or insert into a non-existent column.

    ```sql
    -- TEMPORARY MODIFICATION FOR TESTING ONLY
    -- Inside the BEGIN...END block for core processing:
    -- SET v_records = (
    --   SELECT COUNT(*)
    --   FROM `your_project_id.your_dataset_id.target_table`
    --   WHERE DATE(process_date) = v_stichtag_date
    -- );
    -- Replace with:
    SELECT 1/0; -- This will cause a division by zero error
    ```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```sql
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  'TEST_JOB_ID_08', 'ENTRY_08', '08012023', '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure execution fails with an error (e.g., "Division by zero").
2.  A single record is inserted into `your_project_id.your_dataset_id.job_log` with:
    *   `job_name` = 'PoolBasisprodukt'
    *   `entry_nr` = 'ENTRY_08'
    *   `stichtag` = '08012023'
    *   `status` = 'FAILED'
    *   `error_message` containing the specific error from the core logic (e.g., "Division by zero").
    *   `records_processed` = 0 (or the value it had before the error, if any was set).

### Test Case 9: `job_log` Schema and Data Quality

**Purpose:** Verify that the `job_log` table schema is correct and that data types and NULL handling are as expected for all logged fields.

**Setup:**
1.  Execute a variety of test cases (success, different failure modes) to populate `your_project_id.your_dataset_id.job_log` with diverse data.

**Action:**
Query the `INFORMATION_SCHEMA` for the `job_log` table and inspect its contents.

```sql
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your_project_id.your_dataset_id`.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'job_log'
ORDER BY
    ordinal_position;

SELECT * FROM `your_project_id.your_dataset_id.job_log` LIMIT 100;
```

**Pass/Fail Criterion:**
1.  The `job_log` table schema matches the DDL provided in the migration design:
    *   `job_name` STRING (NOT NULL or effectively NOT NULL if always provided)
    *   `entry_nr` STRING (NOT NULL or effectively NOT NULL)
    *   `stichtag` STRING (NOT NULL or effectively NOT NULL)
    *   `restart_value` STRING (NOT NULL or effectively NOT NULL)
    *   `records_processed` INT64 (NOT NULL)
    *   `status` STRING (NOT NULL)
    *   `created_at` TIMESTAMP (NOT NULL)
    *   `error_message` STRING (NULLABLE)
2.  Review the data in `job_log`:
    *   `records_processed` is always a non-negative integer.
    *   `status` is either 'SUCCESS' or 'FAILED'.
    *   `error_message` is NULL for 'SUCCESS' entries and contains a meaningful message for 'FAILED' entries.
    *   All other fields contain the expected string values passed as parameters.

---

These tests cover the critical aspects of the KornShell script's migration to a BigQuery Stored Procedure, focusing on its orchestration, parameter handling, and logging behavior. The explicit dependency on the `d_ausd_bp_ta_bcp_msisdn.sql` migration is acknowledged, and its successful integration is assumed for the `records_processed` count.