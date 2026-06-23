As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_bp_ta_bpr_opt_text.ksh` to a BigQuery Stored Procedure. These tests aim to ensure behavioral equivalence, data integrity, and correct implementation of the new architecture.

---

# Migration Validation Tests: `k_ausd_bp_ta_bpr_opt_text.ksh` to BigQuery Stored Procedure

## Test Environment Setup

Before executing the tests, ensure the following:

*   **Legacy Environment:**
    *   Access to the original Oracle database with `DWTK_MELDUNGEN`, `SOF$TA_BPR_OPTIONEN`, and `SOF$TA_BPR_OPT_TEXT` tables.
    *   Access to the server where `k_ausd_bp_ta_bpr_opt_text.ksh` can be executed with its original dependencies.
    *   A mechanism to capture the standard output, error output, and the content of the temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_opt_text.tmp`).
*   **Migrated Environment:**
    *   Access to the Google Cloud Project containing the BigQuery dataset (`project.dataset`).
    *   The BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_opt_text` (and any sub-procedures like `p_bpr_opt_text_processing`) deployed.
    *   BigQuery tables `project.dataset.DWTK_MELDUNGEN`, `project.dataset.SOF_TA_BPR_OPTIONEN`, `project.dataset.SOF_TA_BPR_OPT_TEXT`, and `project.dataset.job_log` created and accessible.
    *   Source data from Oracle (`DWTK_MELDUNGEN`, `SOF$TA_BPR_OPTIONEN`) has been migrated to their respective BigQuery tables, ensuring exact data parity (schema, data types, values, NULLs).
*   **Test Data:** Prepare a set of diverse test data for `DWTK_MELDUNGEN` and `SOF$TA_BPR_OPTIONEN` that covers:
    *   Typical "happy path" scenarios.
    *   Edge cases (e.g., empty tables, tables with single rows, all NULLs in certain columns, maximum length strings).
    *   Data that specifically triggers different branches of logic within `d_ausd_bp_ta_bpr_opt_text.sql` (if known).
    *   Data that might cause type conversion issues or constraint violations.

---

## Test Case 1: Output Parity - Happy Path (Full Data Match)

**Purpose:** To verify that for a standard, valid execution, the migrated BigQuery Stored Procedure produces an identical output dataset in the target table (`SOF_TA_BPR_OPT_TEXT`) as the legacy KornShell script. This is the primary test for behavioral equivalence.

**Setup:**
1.  Ensure the BigQuery source tables (`project.dataset.DWTK_MELDUNGEN`, `project.dataset.SOF_TA_BPR_OPTIONEN`) contain an exact replica of a representative "golden dataset" from the Oracle source tables.
2.  Clear the target tables in both environments:
    *   Oracle: `TRUNCATE TABLE SOF$TA_BPR_OPT_TEXT;`
    *   BigQuery: `TRUNCATE TABLE project.dataset.SOF_TA_BPR_OPT_TEXT;`
3.  Define a set of valid input parameters for the job, e.g.:
    *   `p_JobKennung="TEST_JOB"`
    *   `p_EintragsNr="001"`
    *   `p_Stichtag="01012023"`
    *   `p_wiederanlaufWert=""` (or `0`)

**Action:**
1.  **Execute Legacy Job:** Run the original KornShell script with the defined parameters.
    ```bash
    # On legacy server
    /vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh \
        -j "TEST_JOB" -f "001" -s "01012023" -l "0"
    ```
2.  **Execute Migrated Job:** Call the BigQuery Stored Procedure with equivalent parameters.
    ```sql
    -- In BigQuery console or via bq command-line
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        'TEST_JOB', '001', '01012023', '0'
    );
    ```
3.  **Extract Results:**
    *   Extract all data from Oracle's `SOF$TA_BPR_OPT_TEXT` into a CSV file.
    *   Extract all data from BigQuery's `project.dataset.SOF_TA_BPR_OPT_TEXT` into a CSV file.
    *   Ensure consistent ordering (e.g., `ORDER BY` all columns) and data formatting (e.g., date formats, numeric precision) during extraction.

**Pass/Fail Criterion:**
*   The content of the extracted CSV file from Oracle's `SOF$TA_BPR_OPT_TEXT` must be **byte-for-byte identical** to the content of the extracted CSV file from BigQuery's `project.dataset.SOF_TA_BPR_OPT_TEXT`.
*   **Row counts** in both target tables must be identical.

**Example Python (Pytest) Assertion (Conceptual):**
```python
import pandas as pd
from google.cloud import bigquery
import subprocess

def test_output_parity_happy_path(oracle_conn, bq_client):
    job_kennung = "TEST_JOB"
    eintrags_nr = "001"
    stichtag = "01012023"
    wiederanlauf_wert = "0"

    # 1. Clear target tables
    oracle_conn.execute("TRUNCATE TABLE SOF$TA_BPR_OPT_TEXT")
    bq_client.query("TRUNCATE TABLE project.dataset.SOF_TA_BPR_OPT_TEXT").result()

    # 2. Execute legacy job
    legacy_command = [
        "/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh",
        "-j", job_kennung, "-f", eintrags_nr, "-s", stichtag, "-l", wiederanlauf_wert
    ]
    legacy_result = subprocess.run(legacy_command, capture_output=True, text=True, check=True)
    assert legacy_result.returncode == 0, f"Legacy job failed: {legacy_result.stderr}"

    # 3. Execute migrated job
    bq_query = f"""
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        '{job_kennung}', '{eintrags_nr}', '{stichtag}', '{wiederanlauf_wert}'
    );
    """
    bq_client.query(bq_query).result()

    # 4. Extract and compare results
    oracle_df = pd.read_sql("SELECT * FROM SOF$TA_BPR_OPT_TEXT ORDER BY 1,2,3", oracle_conn)
    bq_df = bq_client.query("SELECT * FROM project.dataset.SOF_TA_BPR_OPT_TEXT ORDER BY 1,2,3").to_dataframe()

    # Ensure column names and types are consistent for comparison
    # (e.g., convert all column names to lowercase, handle date/timestamp types)
    oracle_df.columns = oracle_df.columns.str.lower()
    bq_df.columns = bq_df.columns.str.lower()

    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=True, check_exact=False, rtol=1e-9)
    print("Output parity test passed: Data frames are identical.")
```

---

## Test Case 2: Transformation Correctness - Core SQL Logic (Joins, Filters, Aggregations)

**Purpose:** To specifically validate that the complex data transformation logic (joins, filters, aggregations, type handling, NULL handling) originally in `d_ausd_bp_ta_bpr_opt_text.sql` is correctly translated and executed in BigQuery. This test uses targeted data scenarios.

**Setup:**
1.  Prepare specific "mini-datasets" for `DWTK_MELDUNGEN` and `SOF$TA_BPR_OPTIONEN` that isolate particular transformation scenarios:
    *   **Scenario A:** Data that should result in a specific join match.
    *   **Scenario B:** Data that should be filtered out.
    *   **Scenario C:** Data with NULL values in join keys or critical columns.
    *   **Scenario D:** Data that tests specific `CASE` statements or complex calculations.
    *   **Scenario E:** Data with varying data types (e.g., numbers as strings, dates in different formats if applicable).
2.  Load these mini-datasets into both Oracle and BigQuery source tables.
3.  Clear target tables as in Test Case 1.
4.  Use the same valid input parameters as in Test Case 1.

**Action:**
1.  Execute both legacy and migrated jobs for each scenario (A-E).
2.  Extract results from `SOF$TA_BPR_OPT_TEXT` (Oracle) and `project.dataset.SOF_TA_BPR_OPT_TEXT` (BigQuery) for each scenario.

**Pass/Fail Criterion:**
*   For each scenario (A-E), the extracted data from the BigQuery target table must be **identical** to the data from the Oracle target table.
*   Row counts must match for each scenario.

**Example SQL Assertion (Conceptual, for Scenario C - NULL Handling):**
```sql
-- After running both jobs with data containing NULLs in join keys
-- Compare row counts
SELECT COUNT(*) FROM SOF$TA_BPR_OPT_TEXT; -- Legacy
SELECT COUNT(*) FROM project.dataset.SOF_TA_BPR_OPT_TEXT; -- Migrated

-- Compare specific rows (assuming a primary key or unique combination of columns)
SELECT
    (SELECT COUNT(*) FROM SOF$TA_BPR_OPT_TEXT t1 JOIN project.dataset.SOF_TA_BPR_OPT_TEXT t2 ON t1.PK_COL = t2.PK_COL AND t1.COL_A = t2.COL_A AND t1.COL_B IS NOT DISTINCT FROM t2.COL_B) AS matching_rows,
    (SELECT COUNT(*) FROM SOF$TA_BPR_OPT_TEXT) AS legacy_rows,
    (SELECT COUNT(*) FROM project.dataset.SOF_TA_BPR_OPT_TEXT) AS migrated_rows;

-- Identify discrepancies (rows in legacy but not in migrated, or vice-versa)
SELECT 'Legacy Only' AS source, t1.*
FROM SOF$TA_BPR_OPT_TEXT t1
LEFT JOIN project.dataset.SOF_TA_BPR_OPT_TEXT t2 ON t1.PK_COL = t2.PK_COL AND t1.COL_A = t2.COL_A AND t1.COL_B IS NOT DISTINCT FROM t2.COL_B
WHERE t2.PK_COL IS NULL;

SELECT 'Migrated Only' AS source, t2.*
FROM project.dataset.SOF_TA_BPR_OPT_TEXT t2
LEFT JOIN SOF$TA_BPR_OPT_TEXT t1 ON t1.PK_COL = t2.PK_COL AND t1.COL_A = t2.COL_A AND t1.COL_B IS NOT DISTINCT FROM t2.COL_B
WHERE t1.PK_COL IS NULL;
```

---

## Test Case 3: External System Replacements - Source Data Integrity

**Purpose:** To ensure that the data migrated from Oracle source tables to BigQuery source tables is an exact, faithful replica, including schema, data types, and all values (including NULLs). This is a prerequisite for all other data-related tests.

**Setup:**
1.  Identify the Oracle source tables: `DWTK_MELDUNGEN`, `SOF$TA_BPR_OPTIONEN`.
2.  Identify their BigQuery counterparts: `project.dataset.DWTK_MELDUNGEN`, `project.dataset.SOF_TA_BPR_OPTIONEN`.
3.  Ensure a recent, consistent snapshot of data exists in both Oracle and BigQuery for these tables.

**Action:**
1.  For each source table:
    *   Extract schema information (column names, data types, nullability) from Oracle.
    *   Extract schema information from BigQuery.
    *   Extract all data from Oracle into a canonical format (e.g., sorted CSV).
    *   Extract all data from BigQuery into the same canonical format.

**Pass/Fail Criterion:**
*   **Schema Parity:** Column names, data types, and nullability must be equivalent between Oracle and BigQuery for each table. (Note: Oracle `NUMBER` might map to BQ `NUMERIC` or `BIGNUMERIC`, `VARCHAR2` to `STRING`, `DATE` to `DATE` or `TIMESTAMP` - these mappings should be documented and verified).
*   **Data Parity:** The extracted data files (e.g., sorted CSVs) for each table must be byte-for-byte identical.
*   **Row Counts:** `COUNT(*)` for each table must be identical in Oracle and BigQuery.

**Example Python (Pytest) Assertion:**
```python
import pandas as pd
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for Oracle connection

def test_source_data_integrity(oracle_conn, bq_client):
    source_tables = {
        "DWTK_MELDUNGEN": "project.dataset.DWTK_MELDUNGEN",
        "SOF$TA_BPR_OPTIONEN": "project.dataset.SOF_TA_BPR_OPTIONEN"
    }

    for oracle_table, bq_table in source_tables.items():
        print(f"Checking table: {oracle_table} / {bq_table}")

        # Compare row counts
        oracle_row_count = pd.read_sql(f"SELECT COUNT(*) FROM {oracle_table}", oracle_conn).iloc[0, 0]
        bq_row_count = bq_client.query(f"SELECT COUNT(*) FROM {bq_table}").to_dataframe().iloc[0, 0]
        assert oracle_row_count == bq_row_count, \
            f"Row count mismatch for {oracle_table}: Oracle={oracle_row_count}, BQ={bq_row_count}"

        # Compare data content
        # Order by all columns to ensure consistent comparison
        oracle_df = pd.read_sql(f"SELECT * FROM {oracle_table} ORDER BY 1,2,3", oracle_conn)
        bq_df = bq_client.query(f"SELECT * FROM {bq_table} ORDER BY 1,2,3").to_dataframe()

        # Standardize column names and types for comparison
        oracle_df.columns = oracle_df.columns.str.lower()
        bq_df.columns = bq_df.columns.str.lower()
        # Add type conversion logic if necessary, e.g., for dates/timestamps
        # for col in common_date_cols:
        #     oracle_df[col] = pd.to_datetime(oracle_df[col])
        #     bq_df[col] = pd.to_datetime(bq_df[col])

        pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=True, check_exact=False, rtol=1e-9)
        print(f"  Data integrity for {oracle_table} passed.")

    print("All source data integrity checks passed.")
```

---

## Test Case 4: Parameter Validation - Missing Required Parameters

**Purpose:** To verify that the BigQuery Stored Procedure correctly identifies and handles missing required parameters, mirroring the error behavior of the legacy script.

**Setup:**
1.  Identify the required parameters: `p_JobKennung`, `p_Stichtag`, `p_EintragsNr`.
2.  Clear the `project.dataset.job_log` table.

**Action:**
1.  **Execute Legacy Job:** Attempt to run the legacy script with one or more required parameters missing.
    ```bash
    # Missing Stichtag
    /vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh \
        -j "TEST_JOB" -f "001"
    # Capture stderr and exit code
    ```
2.  **Execute Migrated Job:** Call the BigQuery Stored Procedure with equivalent missing parameters (e.g., passing `NULL` or an empty string for a required parameter).
    ```sql
    -- Missing Stichtag (passing NULL)
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        'TEST_JOB', '001', NULL, '0'
    );
    -- Or if empty string is considered missing:
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        'TEST_JOB', '001', '', '0'
    );
    ```
3.  **Check Logging:** Query the `project.dataset.job_log` table for the error entry.

**Pass/Fail Criterion:**
*   **Legacy:** The legacy script must exit with a non-zero status code (specifically `ErrNr=193` for missing argument) and log an error message to stderr.
*   **Migrated:** The BigQuery Stored Procedure must terminate execution (e.g., via `RAISE` or `ASSERT` failure) and log an entry to `project.dataset.job_log` indicating a failure, including the specific missing parameter and an appropriate error message. The error message should be functionally equivalent to the legacy one.

**Example SQL Assertion (for Migrated Job Log):**
```sql
SELECT
    job_id,
    status,
    error_message,
    parameters
FROM
    project.dataset.job_log
WHERE
    job_id = 'TEST_JOB' AND status = 'FAILED'
ORDER BY
    start_time DESC
LIMIT 1;

-- Expected output for missing Stichtag:
-- job_id: TEST_JOB
-- status: FAILED
-- error_message: "Parameter 'p_Stichtag' is required but was not provided." (or similar)
-- parameters: {job_kennung: "TEST_JOB", eintrags_nr: "001", stichtag: NULL, wiederanlauf_wert: "0"}
```

---

## Test Case 5: Parameter Validation - Invalid Date Format (`p_Stichtag`)

**Purpose:** To verify that the BigQuery Stored Procedure correctly validates the `p_Stichtag` format, mirroring the `DWDate_Datum_Check` behavior of the legacy script.

**Setup:**
1.  Clear the `project.dataset.job_log` table.
2.  Define invalid `p_Stichtag` values, e.g., "2023-01-01", "01/01/2023", "ABCDEFGH", "010123".

**Action:**
1.  **Execute Legacy Job:** Attempt to run the legacy script with an invalid `p_Stichtag`.
    ```bash
    # Invalid Stichtag format
    /vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh \
        -j "TEST_JOB" -f "001" -s "2023-01-01" -l "0"
    # Capture stderr and exit code
    ```
2.  **Execute Migrated Job:** Call the BigQuery Stored Procedure with an invalid `p_Stichtag`.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        'TEST_JOB', '001', '2023-01-01', '0'
    );
    ```
3.  **Check Logging:** Query the `project.dataset.job_log` table.

**Pass/Fail Criterion:**
*   **Legacy:** The legacy script must exit with a non-zero status code and log an error related to date format validation.
*   **Migrated:** The BigQuery Stored Procedure must terminate execution and log an entry to `project.dataset.job_log` indicating a failure due to invalid date format, with an appropriate error message.

---

## Test Case 6: Date Derivation Correctness (`p_datum_heute`, `p_datum_gestern`)

**Purpose:** To verify that the BigQuery Stored Procedure correctly derives `v_datum_heute` and `v_datum_gestern` using BigQuery native functions, matching the output of `gestern.ksh`.

**Setup:**
1.  Set the system date on the legacy server to a known date (e.g., `2023-01-02`). This might require a dedicated test environment or mocking.
2.  Clear the target tables and `job_log`.
3.  Use valid input parameters.

**Action:**
1.  **Execute Legacy Job:** Run the legacy script.
    ```bash
    # On legacy server, ensure system date is 2023-01-02
    /vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh \
        -j "TEST_JOB" -f "001" -s "01012023" -l "0"
    # Capture the values of p_datum_heute and p_datum_gestern as they are passed to SQL script
    # (This might require temporarily modifying the ksh script to echo these values)
    ```
2.  **Execute Migrated Job:** Call the BigQuery Stored Procedure.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        'TEST_JOB', '001', '01012023', '0'
    );
    ```
3.  **Verify Usage:** The `d_ausd_bp_ta_bpr_opt_text.sql` logic (now in BQ SP) uses these dates. The best way to verify is to ensure the final output data (Test Case 1) is correct, implying the dates were used correctly. Additionally, if the BigQuery SP logs these derived dates, check the log.

**Pass/Fail Criterion:**
*   The `p_datum_heute` derived by the legacy script must correspond to the `CURRENT_DATE()` in BigQuery on the day of execution.
*   The `p_datum_gestern` derived by the legacy script must correspond to `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` in BigQuery.
*   The final data in `SOF_TA_BPR_OPT_TEXT` (BigQuery) must reflect the correct use of these derived dates, matching the legacy output.

---

## Test Case 7: Record Counting and Logging - Success Scenario

**Purpose:** To verify that the BigQuery Stored Procedure correctly calculates the number of records processed and logs this information, along with job status, to the `project.dataset.job_log` table.

**Setup:**
1.  Ensure source tables contain data that will result in a non-zero record count in the target table.
2.  Clear the target tables and `project.dataset.job_log`.
3.  Use valid input parameters.

**Action:**
1.  **Execute Legacy Job:** Run the legacy script.
    ```bash
    /vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh \
        -j "TEST_JOB_COUNT" -f "002" -s "01012023" -l "0"
    # Capture the value of v_records from the script's output or temporary file.
    ```
2.  **Execute Migrated Job:** Call the BigQuery Stored Procedure.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        'TEST_JOB_COUNT', '002', '01012023', '0'
    );
    ```
3.  **Check Target Table:** Get the `COUNT(*)` from `project.dataset.SOF_TA_BPR_OPT_TEXT`.
4.  **Check Logging:** Query `project.dataset.job_log` for the latest entry for `TEST_JOB_COUNT`.

**Pass/Fail Criterion:**
*   The `v_records` value captured from the legacy script must match the `record_count` logged in `project.dataset.job_log` for the migrated job.
*   The `status` in `job_log` must be 'SUCCESS'.
*   The `record_count` in `job_log` must also match the `COUNT(*)` of the `project.dataset.SOF_TA_BPR_OPT_TEXT` table after the migrated job runs.

**Example SQL Assertion (for Migrated Job Log):**
```sql
SELECT
    job_id,
    status,
    record_count,
    start_time,
    end_time
FROM
    project.dataset.job_log
WHERE
    job_id = 'TEST_JOB_COUNT' AND status = 'SUCCESS'
ORDER BY
    start_time DESC
LIMIT 1;

-- Expected output:
-- job_id: TEST_JOB_COUNT
-- status: SUCCESS
-- record_count: [Expected number of records]
-- start_time: [Timestamp]
-- end_time: [Timestamp]
```

---

## Test Case 8: Record Counting and Logging - Empty Source Tables

**Purpose:** To verify the job's behavior and logging when source tables are empty, resulting in zero records processed.

**Setup:**
1.  Ensure `DWTK_MELDUNGEN` and `SOF$TA_BPR_OPTIONEN` are empty in both Oracle and BigQuery.
2.  Clear the target tables and `project.dataset.job_log`.
3.  Use valid input parameters.

**Action:**
1.  **Execute Legacy Job:** Run the legacy script.
    ```bash
    /vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh \
        -j "TEST_JOB_EMPTY" -f "003" -s "01012023" -l "0"
    # Capture v_records.
    ```
2.  **Execute Migrated Job:** Call the BigQuery Stored Procedure.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_bpr_opt_text(
        'TEST_JOB_EMPTY', '003', '01012023', '0'
    );
    ```
3.  **Check Target Table:** Get the `COUNT(*)` from `project.dataset.SOF_TA_BPR_OPT_TEXT`.
4.  **Check Logging:** Query `project.dataset.job_log`.

**Pass/Fail Criterion:**
*   The `v_records` value captured from the legacy script must be `0`.
*   The `record_count` logged in `project.dataset.job_log` for the migrated job must be `0`.
*   The `status` in `job_log` must be 'SUCCESS'.
*   The `COUNT(*)` of the `project.dataset.SOF_TA_BPR_OPT_TEXT` table must be `0`.

---

## Test Case 9: Legacy Commented-Out Logic (Sed, Sort, Join)

**Purpose:** To confirm that the commented-out `sed`, `sort`, `join` operations in the legacy script are indeed not part of the migrated BigQuery Stored Procedure logic, as per the design document.

**Setup:**
1.  Review the BigQuery Stored Procedure code (`project.dataset.r_ausd_bp_ta_bpr_opt_text` and any sub-procedures).

**Action:**
1.  Manually inspect the BigQuery Stored Procedure code.
2.  Search for any BigQuery equivalents of file processing operations (e.g., reading/writing to Cloud Storage, complex string manipulations that mimic `sed`, or external sorts/joins that are not standard SQL).

**Pass/Fail Criterion:**
*   There must be **no code** in the BigQuery Stored Procedure that implements the functionality of the commented-out `sed`, `sort`, or `join` commands from the original KornShell script. This confirms the design decision to treat them as inactive legacy logic.

---

## Test Case 10: `DWPA_UTIL_SKRIPT` Package Replacement

**Purpose:** To verify that any critical functions or procedures from the Oracle `DWPA_UTIL_SKRIPT` package, if used by `d_ausd_bp_ta_bpr_opt_text.sql`, have been correctly re-implemented as BigQuery UDFs or Stored Procedures.

**Setup:**
1.  Identify specific functions/procedures from `DWPA_UTIL_SKRIPT` that are called within `d_ausd_bp_ta_bpr_opt_text.sql`. (This requires access to the original SQL script).
2.  For each identified function, understand its input, output, and core logic.
3.  Identify the corresponding BigQuery UDFs or Stored Procedures.

**Action:**
1.  For each identified function/UDF:
    *   Create a series of test cases with various inputs (valid, edge cases, NULLs) for the original Oracle function.
    *   Execute the Oracle function with these inputs and record the outputs.
    *   Execute the corresponding BigQuery UDF/SP with the same inputs and record the outputs.

**Pass/Fail Criterion:**
*   For every test case, the output of the BigQuery UDF/SP must be **identical** to the output of the original Oracle `DWPA_UTIL_SKRIPT` function.
*   Error handling for invalid inputs must also be equivalent.

**Example SQL Assertion (Conceptual for a UDF `DWPA_UTIL_SKRIPT.CALCULATE_VALUE`):**
```sql
-- Test Case: Valid input
SELECT DWPA_UTIL_SKRIPT.CALCULATE_VALUE(10, 5) FROM DUAL; -- Oracle
SELECT project.dataset.calculate_value_udf(10, 5); -- BigQuery

-- Test Case: NULL input
SELECT DWPA_UTIL_SKRIPT.CALCULATE_VALUE(NULL, 5) FROM DUAL; -- Oracle
SELECT project.dataset.calculate_value_udf(NULL, 5); -- BigQuery

-- Test Case: Edge case (e.g., division by zero if applicable)
SELECT DWPA_UTIL_SKRIPT.CALCULATE_VALUE(10, 0) FROM DUAL; -- Oracle
SELECT project.dataset.calculate_value_udf(10, 0); -- BigQuery
```

---