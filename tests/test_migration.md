As a senior data-migration QA engineer, I've designed a comprehensive suite of tests to validate the migration of `k_ausd_v_ta_disc_zusgf.ksh` to Google Cloud Platform. These tests aim to ensure behavioral equivalence, data integrity, and correct functionality of the new BigQuery and Airflow-based solution.

---

## Migration Validation Tests: `k_ausd_v_ta_disc_zusgf.ksh`

### Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **Legacy Environment**:
    *   Access to the original KornShell script (`k_ausd_v_ta_disc_zusgf.ksh`) and its dependencies (utility scripts, `d_ausd_v_ta_disc_zusgf.sql`).
    *   Access to the Oracle database containing `sof$ta_disc_zusgf`, `sof$ta_discount`, and `isbert_schema.dwtk_meldungen`.
    *   A mechanism to execute the KSH script and capture its output (stdout, stderr, exit code) and the contents of the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_disc_zusgf_$$.tmp`).
    *   A mechanism to query the Oracle database.
2.  **Migrated Environment**:
    *   A Google Cloud Project (`my_project`) and BigQuery Dataset (`my_dataset`) with the DDLs for `job_error_log`, `job_run_log`, `ta_disc_zusgf`, `ta_discount`, and `dwtk_meldungen` applied.
    *   The BigQuery Stored Procedures `d_ausd_v_ta_disc_zusgf_sp` and `r_ausd_vertrag_control` deployed.
    *   An Airflow environment with the `k_ausd_v_ta_disc_zusgf_dag.py` DAG deployed and configured with the correct `PROJECT_ID` and `DATASET_ID`.
    *   A mechanism to trigger the Airflow DAG with parameters and monitor its execution.
    *   A mechanism to query BigQuery tables.
3.  **Test Data Management**:
    *   Helper functions or scripts to populate both Oracle and BigQuery tables with identical test data for comparison.
    *   Helper functions or scripts to clear test data from tables before each test run.

---

### 1. Output Parity Tests

#### Test Case 1.1: Successful Run - Data Parity

*   **Purpose**: To verify that the `ta_disc_zusgf` table in BigQuery contains the exact same data as the `sof$ta_disc_zusgf` table in Oracle after a successful run with identical inputs.
*   **Setup**:
    1.  Clear `sof$ta_disc_zusgf` (Oracle) and `my_dataset.ta_disc_zusgf` (BigQuery).
    2.  Populate `sof$ta_discount` (Oracle) and `my_dataset.ta_discount` (BigQuery) with a diverse set of test data, including:
        *   Multiple `rabatt` entries for the same `cntrct_id`/`cntrct_obj_version` to test `STRING_AGG` and `SUBSTR`.
        *   Single `rabatt` entries.
        *   `rabatthoehe` values (0, positive, NULL).
        *   `rabatt` values (empty string, non-empty string, NULL).
        *   `disc_vector_ty` values.
        *   Cases where `cntrct_id`/`cntrct_obj_version` in `ta_discount` have no corresponding `rabatt` entries (to test `LEFT JOIN` behavior).
    3.  Define `JobKennung` (e.g., "TEST_JOB_1") and `EintragsNr` (e.g., "ENTRY_001").
*   **Action**:
    1.  **Legacy**: Execute `k_ausd_v_ta_disc_zusgf.ksh -j "TEST_JOB_1" -f "ENTRY_001"`.
    2.  **Migrated**: Trigger the Airflow DAG `k_ausd_v_ta_disc_zusgf_dag` with `job_kennung="TEST_JOB_1"` and `eintrags_nr="ENTRY_001"`.
*   **Pass/Fail Criterion**:
    *   Both jobs complete successfully (exit code 0 for legacy, DAG succeeds for migrated).
    *   The number of rows in Oracle's `sof$ta_disc_zusgf` is equal to the number of rows in BigQuery's `my_dataset.ta_disc_zusgf`.
    *   A row-by-row comparison of `sof$ta_disc_zusgf` and `my_dataset.ta_disc_zusgf` shows identical data for all columns, considering potential type conversions (e.g., Oracle `VARCHAR2` to BigQuery `STRING`).

    ```python
    # Example Python (pytest) assertion
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming Oracle client is set up

    def test_data_parity_successful_run():
        job_kennung = "TEST_JOB_1"
        eintrags_nr = "ENTRY_001"

        # --- Setup (assumed helper functions) ---
        clear_oracle_table("sof$ta_disc_zusgf")
        clear_bigquery_table("my_project.my_dataset.ta_disc_zusgf")
        populate_oracle_ta_discount_with_test_data()
        populate_bigquery_ta_discount_with_test_data()

        # --- Action ---
        # Legacy execution (pseudocode)
        run_ksh_script("k_ausd_v_ta_disc_zusgf.ksh", f"-j {job_kennung} -f {eintrags_nr}")
        
        # Migrated execution (pseudocode)
        trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", {"job_kennung": job_kennung, "eintrags_nr": eintrags_nr})
        wait_for_airflow_dag_completion("k_ausd_v_ta_disc_zusgf_dag")

        # --- Verification ---
        # Fetch data from Oracle
        oracle_conn = cx_Oracle.connect("user/pass@host:port/service")
        oracle_df = pd.read_sql("SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle FROM sof$ta_disc_zusgf ORDER BY 1,2,3", oracle_conn)
        oracle_conn.close()

        # Fetch data from BigQuery
        bq_client = bigquery.Client(project="my_project")
        bq_query = """
            SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle
            FROM `my_project.my_dataset.ta_disc_zusgf`
            ORDER BY 1,2,3
        """
        bq_df = bq_client.query(bq_query).to_dataframe()

        # Compare DataFrames
        pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False) # check_dtype=False due to potential subtle type differences (e.g., int64 vs int32)
    ```

#### Test Case 1.2: Successful Run - Record Count Parity

*   **Purpose**: To ensure the record count reported by the migrated job (in `job_run_log`) matches the count reported by the legacy job (via temporary file).
*   **Setup**: Same as Test Case 1.1.
*   **Action**: Same as Test Case 1.1.
*   **Pass/Fail Criterion**:
    *   The `v_records` value read from `$DW_DIR_UTL/bert_k_ausd_v_ta_disc_zusgf_$$.tmp` by the legacy script must match the `records_processed` value recorded in `my_dataset.job_run_log` for the corresponding `job_kennung` and `eintrags_nr`.

    ```python
    # Example Python (pytest) assertion
    def test_record_count_parity_successful_run():
        job_kennung = "TEST_JOB_1"
        eintrags_nr = "ENTRY_001"

        # --- Setup (assumed helper functions) ---
        clear_oracle_table("sof$ta_disc_zusgf")
        clear_bigquery_table("my_project.my_dataset.ta_disc_zusgf")
        clear_bigquery_table("my_project.my_dataset.job_run_log") # Clear log for clean assertion
        populate_oracle_ta_discount_with_test_data()
        populate_bigquery_ta_discount_with_test_data()

        # --- Action ---
        # Legacy execution (pseudocode)
        legacy_output = run_ksh_script_and_capture_tmpfile("k_ausd_v_ta_disc_zusgf.ksh", f"-j {job_kennung} -f {eintrags_nr}")
        legacy_records_processed = int(legacy_output['tmp_file_content'])
        
        # Migrated execution (pseudocode)
        trigger_airflow_dag("k_ausd_v_ta_disc_zusgf_dag", {"job_kennung": job_kennung, "eintrags_nr": eintrags_nr})
        wait_for_airflow_dag_completion("k_ausd_v_ta_disc_zusgf_dag")

        # --- Verification ---
        bq_client = bigquery.Client(project="my_project")
        bq_query = f"""
            SELECT records_processed
            FROM `my_project.my_dataset.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY created_ts DESC
            LIMIT 1
        """
        bq_records_processed = bq_client.query(bq_query).to_dataframe()['records_processed'].iloc[0]

        assert legacy_records_processed == bq_records_processed, \
            f"Record count mismatch: Legacy={legacy_records_processed}, Migrated={bq_records_processed}"
    ```

---

### 2. Transformation Correctness Tests

#### Test Case 2.1: `rabatt_alle` Concatenation Logic - Multiple Discounts

*   **Purpose**: Verify the `STRING_AGG` and `SUBSTR` logic correctly concatenates multiple discounts for a single contract, including the format `rabatt (rabatthoehe%)`.
*   **Setup**:
    1.  Clear `ta_discount` (both Oracle and BigQuery).
    2.  Insert data into `ta_discount` where one `(cntrct_id, cntrct_obj_version)` pair has multiple `rabatt` entries with different `rabatthoehe`.
        *   Example: `(1, 1, 'TYPE_A', 'DiscountX', 10)`, `(1, 1, 'TYPE_A', 'DiscountY', 5)`, `(1, 1, 'TYPE_A', 'DiscountZ', 15)`
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   The `rabatt_alle` column for the test `(cntrct_id, cntrct_obj_version, disc_vector_ty)` in `ta_disc_zusgf` must contain the correctly concatenated string, e.g., `"DiscountX (10%),DiscountY (5%),DiscountZ (15%)"` (order might vary based on `STRING_AGG` implementation without explicit `ORDER BY`, but the generated code has `ORDER BY rabatt_formatted`).

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT rabatt_alle
    FROM `my_project.my_dataset.ta_disc_zusgf`
    WHERE cntrct_id = 1 AND cntrct_obj_version = 1 AND disc_vector_ty = 'TYPE_A';
    -- Expected: "DiscountX (10%),DiscountY (5%),DiscountZ (15%)" (or similar ordered string)
    ```

#### Test Case 2.2: `rabatt_alle` Concatenation Logic - Length Limit (500 chars)

*   **Purpose**: Verify that the `SUBSTR(..., 1, 500)` correctly truncates the concatenated `rabatt_alle` string if it exceeds 500 characters.
*   **Setup**:
    1.  Clear `ta_discount`.
    2.  Insert data into `ta_discount` such that the concatenated `rabatt_alle` for a specific `(cntrct_id, cntrct_obj_version)` pair would exceed 500 characters.
        *   Example: Many small discounts, or a few very long `rabatt` strings.
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   The `LENGTH(rabatt_alle)` for the test `(cntrct_id, cntrct_obj_version, disc_vector_ty)` in `ta_disc_zusgf` must be exactly 500.
    *   The content should be the truncated version of the full concatenation.

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT LENGTH(rabatt_alle)
    FROM `my_project.my_dataset.ta_disc_zusgf`
    WHERE cntrct_id = <test_id> AND cntrct_obj_version = <test_version> AND disc_vector_ty = <test_type>;
    -- Expected: 500
    ```

#### Test Case 2.3: `DISTINCT` Clause in `dzg` Subquery

*   **Purpose**: Verify that the `DISTINCT` clause in the `dzg` subquery correctly identifies unique combinations of `cntrct_id`, `disc_vector_ty`, and `cntrct_obj_version` from `ta_discount` before joining.
*   **Setup**:
    1.  Clear `ta_discount`.
    2.  Insert data into `ta_discount` with duplicate `(cntrct_id, disc_vector_ty, cntrct_obj_version)` but different `rabatt` or `rabatthoehe`.
        *   Example: `(1, 1, 'TYPE_A', 'Disc1', 10)`, `(1, 1, 'TYPE_A', 'Disc2', 20)` (these are distinct by `rabatt`/`rabatthoehe`, but the `dzg` subquery would produce only one row `(1, 'TYPE_A', 1)`).
        *   Example: `(2, 1, 'TYPE_B', 'Disc3', 30)`, `(2, 1, 'TYPE_B', 'Disc3', 30)` (true duplicates, `dzg` should still produce one row).
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   The `ta_disc_zusgf` table should contain one row for each unique `(cntrct_id, disc_vector_ty, cntrct_obj_version)` combination found in `ta_discount`, regardless of how many `rabatt` entries existed for that combination.
    *   No duplicate `(cntrct_id, disc_vector_ty, cntrct_obj_version)` rows should exist in `ta_disc_zusgf`.

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT COUNT(*) FROM (
        SELECT cntrct_id, disc_vector_ty, cntrct_obj_version, COUNT(*)
        FROM `my_project.my_dataset.ta_disc_zusgf`
        GROUP BY cntrct_id, disc_vector_ty, cntrct_obj_version
        HAVING COUNT(*) > 1
    );
    -- Expected: 0
    ```

#### Test Case 2.4: NULL Handling in `rabatt` and `rabatthoehe`

*   **Purpose**: Verify how `NULL` values in `rabatt` and `rabatthoehe` are handled during concatenation.
*   **Setup**:
    1.  Clear `ta_discount`.
    2.  Insert data into `ta_discount` with:
        *   `rabatt` is `NULL`, `rabatthoehe` is `NULL`.
        *   `rabatt` is `NULL`, `rabatthoehe` is `0`.
        *   `rabatt` is `NULL`, `rabatthoehe` is `10`.
        *   `rabatt` is `'Some Discount'`, `rabatthoehe` is `NULL`.
        *   `rabatt` is `'Some Discount'`, `rabatthoehe` is `0`.
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   Compare the `rabatt_alle` output for these specific cases between Oracle and BigQuery.
    *   BigQuery's `CONCAT` treats `NULL` as `NULL` for string concatenation, so `CONCAT(NULL, ' (', CAST(NULL AS STRING), '%)')` would result in `NULL`. The Oracle behavior for `NULL` in `TO_CHAR` and string concatenation needs to be matched. Assuming Oracle's `TO_CHAR(NULL)` is `NULL` and `NULL || 'string'` is `NULL`, the BigQuery `CONCAT` should produce `NULL` if any part is `NULL`. If Oracle treats `NULL` as empty string in concatenation, BigQuery's `CONCAT` might need `IFNULL` or `COALESCE`. The generated code uses `CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)')`. If `rabatt` is NULL, the whole `rabatt_formatted` will be NULL. If `rabatthoehe` is NULL, `CAST(NULL AS STRING)` is NULL, making `rabatt_formatted` NULL. This seems like a potential difference if Oracle treats NULLs as empty strings in concatenation.
    *   **Refinement**: The `CONCAT` function in BigQuery returns `NULL` if any argument is `NULL`. Oracle's `||` operator (and `CONCAT` function) treats `NULL` as an empty string. This is a critical difference. The BigQuery code `CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)')` needs to be adjusted to `CONCAT(IFNULL(rabatt, ''), ' (', IFNULL(CAST(rabatthoehe AS STRING), ''), '%)')` to match Oracle's behavior if `rabatt` or `rabatthoehe` can be `NULL` and should still appear in the string.
    *   **Revised Pass/Fail Criterion**: The `rabatt_alle` column in BigQuery's `ta_disc_zusgf` must exactly match Oracle's `sof$ta_disc_zusgf` for all test cases involving `NULL` `rabatt` and `rabatthoehe`. If the generated code's `CONCAT` behavior is different from Oracle's `||` with `NULL`s, this test will fail, highlighting a necessary code fix.

    ```sql
    -- BigQuery Assertion (after job run)
    -- Example for rabatt='Some Discount', rabatthoehe=NULL
    SELECT rabatt_alle
    FROM `my_project.my_dataset.ta_disc_zusgf`
    WHERE cntrct_id = <test_id> AND cntrct_obj_version = <test_version> AND disc_vector_ty = <test_type>;
    -- If Oracle produces 'Some Discount (%)', BigQuery should too.
    -- If BigQuery produces NULL, it's a mismatch.
    ```

#### Test Case 2.5: Empty Source Table (`ta_discount`)

*   **Purpose**: Verify correct behavior when the source table `ta_discount` is empty.
*   **Setup**:
    1.  Clear `ta_discount` (both Oracle and BigQuery).
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   Both jobs complete successfully.
    *   `ta_disc_zusgf` (both Oracle and BigQuery) must be empty (0 rows).
    *   The `records_processed` count in `job_run_log` (BigQuery) and the temporary file (Legacy) must be 0.

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT COUNT(*) FROM `my_project.my_dataset.ta_disc_zusgf`;
    -- Expected: 0
    ```

---

### 3. External-System Replacements Tests

#### Test Case 3.1: Parameter Validation - Missing `JobKennung`

*   **Purpose**: Verify that missing required parameters are handled identically, leading to an error and logging.
*   **Setup**:
    1.  Clear `job_error_log` (BigQuery).
*   **Action**:
    1.  **Legacy**: Execute `k_ausd_v_ta_disc_zusgf.ksh -f "ENTRY_001"` (missing `-j`).
    2.  **Migrated**: Trigger the Airflow DAG `k_ausd_v_ta_disc_zusgf_dag` with `eintrags_nr="ENTRY_001"` but `job_kennung` set to `NULL` or an empty string (or omit it if Airflow allows, which it usually doesn't for required params).
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script must exit with `ErrNr=193` (exit code 193). The output should contain "FEHLER: 0 E 193 Jobkennung".
    *   **Migrated**: The Airflow DAG must fail. `my_dataset.job_error_log` must contain an entry with `err_nr=193`, `err_arg='Jobkennung'`, and `job_kennung` as `NULL` or empty. The BigQuery Stored Procedure `r_ausd_vertrag_control` should raise a `SCRIPT_EXCEPTION`.

    ```sql
    -- BigQuery Assertion (after DAG run)
    SELECT err_nr, err_arg, job_kennung, eintrags_nr
    FROM `my_project.my_dataset.job_error_log`
    WHERE err_nr = 193 AND err_arg = 'Jobkennung'
    ORDER BY created_ts DESC
    LIMIT 1;
    -- Expected: err_nr=193, err_arg='Jobkennung', job_kennung=NULL or ''
    ```

#### Test Case 3.2: Parameter Validation - Missing `EintragsNr`

*   **Purpose**: Similar to 3.1, but for `EintragsNr`.
*   **Setup**:
    1.  Clear `job_error_log` (BigQuery).
*   **Action**:
    1.  **Legacy**: Execute `k_ausd_v_ta_disc_zusgf.ksh -j "TEST_JOB_1"` (missing `-f`).
    2.  **Migrated**: Trigger the Airflow DAG `k_ausd_v_ta_disc_zusgf_dag` with `job_kennung="TEST_JOB_1"` but `eintrags_nr` set to `NULL` or an empty string.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script must exit with `ErrNr=193` (exit code 193). The output should contain "FEHLER: 0 E 193 EintragsNr".
    *   **Migrated**: The Airflow DAG must fail. `my_dataset.job_error_log` must contain an entry with `err_nr=193`, `err_arg='EintragsNr'`, and `eintrags_nr` as `NULL` or empty. The BigQuery Stored Procedure `r_ausd_vertrag_control` should raise a `SCRIPT_EXCEPTION`.

    ```sql
    -- BigQuery Assertion (after DAG run)
    SELECT err_nr, err_arg, job_kennung, eintrags_nr
    FROM `my_project.my_dataset.job_error_log`
    WHERE err_nr = 193 AND err_arg = 'EintragsNr'
    ORDER BY created_ts DESC
    LIMIT 1;
    -- Expected: err_nr=193, err_arg='EintragsNr', eintrags_nr=NULL or ''
    ```

#### Test Case 3.3: Error Logging for SQL Exceptions

*   **Purpose**: Verify that runtime errors within the core SQL logic are caught, logged to `job_error_log`, and propagated.
*   **Setup**:
    1.  Clear `job_error_log` (BigQuery).
    2.  Introduce a temporary, controlled error into `d_ausd_v_ta_disc_zusgf_sp` (e.g., attempt to insert a string into an `INT64` column, or divide by zero if possible). This would typically be done by temporarily modifying the SP for the test.
*   **Action**:
    1.  **Legacy**: Execute `k_ausd_v_ta_disc_zusgf.ksh -j "TEST_JOB_ERROR" -f "ENTRY_ERROR"`. (Assuming `starteSQLSkript` would capture SQL*Plus errors and propagate them, or the SQL script itself would exit with an error).
    2.  **Migrated**: Trigger the Airflow DAG `k_ausd_v_ta_disc_zusgf_dag` with `job_kennung="TEST_JOB_ERROR"` and `eintrags_nr="ENTRY_ERROR"`.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script should fail, likely with a non-zero exit code and error messages on stderr.
    *   **Migrated**: The Airflow DAG must fail. `my_dataset.job_error_log` must contain an entry with `err_nr=999` (or similar generic SQL error code), `err_arg='SQL_EXCEPTION'`, and a detailed `error_message` reflecting the BigQuery SQL error. The `r_ausd_vertrag_control` procedure should re-raise the exception.

    ```sql
    -- BigQuery Assertion (after DAG run)
    SELECT err_nr, err_arg, error_message
    FROM `my_project.my_dataset.job_error_log`
    WHERE job_kennung = 'TEST_JOB_ERROR' AND eintrags_nr = 'ENTRY_ERROR'
    ORDER BY created_ts DESC
    LIMIT 1;
    -- Expected: err_nr=999, err_arg='SQL_EXCEPTION', error_message containing BigQuery error details.
    ```

#### Test Case 3.4: `TRUNCATE` Behavior

*   **Purpose**: Verify that the `ta_disc_zusgf` table is truncated before new data is inserted, ensuring idempotency and clean state.
*   **Setup**:
    1.  Populate `ta_disc_zusgf` (both Oracle and BigQuery) with some dummy data.
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   After the job runs, the `ta_disc_zusgf` table (both Oracle and BigQuery) should only contain the newly inserted data, and none of the pre-existing dummy data. The row count should match the number of records inserted from `ta_discount`, not the sum of dummy data and new data.

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT COUNT(*) FROM `my_project.my_dataset.ta_disc_zusgf`;
    -- Expected: Count of rows derived from ta_discount, not including any pre-existing dummy data.
    ```

---

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Schema Parity of `ta_disc_zusgf`

*   **Purpose**: To ensure the schema of the migrated `ta_disc_zusgf` table in BigQuery matches the legacy Oracle table, including column names, data types, and nullability (where applicable).
*   **Setup**:
    1.  Ensure both Oracle and BigQuery tables exist.
*   **Action**: Retrieve schema information from both Oracle and BigQuery.
*   **Pass/Fail Criterion**:
    *   All column names must match.
    *   Data types must be functionally equivalent (e.g., Oracle `NUMBER` to BigQuery `INT64`, Oracle `VARCHAR2` to BigQuery `STRING`).
    *   Nullability constraints should be consistent.

    ```python
    # Example Python (pytest) assertion
    def test_ta_disc_zusgf_schema_parity():
        # --- Fetch Oracle schema (pseudocode) ---
        oracle_schema = get_oracle_table_schema("sof$ta_disc_zusgf") # Returns list of dicts: [{'name': 'COL1', 'type': 'VARCHAR2', 'nullable': True}, ...]

        # --- Fetch BigQuery schema ---
        bq_client = bigquery.Client(project="my_project")
        table_ref = bq_client.dataset("my_dataset").table("ta_disc_zusgf")
        bq_table = bq_client.get_table(table_ref)
        bq_schema = [{'name': field.name, 'type': field.field_type, 'nullable': field.is_nullable} for field in bq_table.schema]

        # --- Compare schemas ---
        # Convert to a comparable format, e.g., dict of {name: (type, nullable)}
        oracle_schema_map = {col['name'].upper(): (map_oracle_type_to_bq(col['type']), col['nullable']) for col in oracle_schema}
        bq_schema_map = {col['name'].upper(): (col['type'], col['nullable']) for col in bq_schema}

        assert oracle_schema_map == bq_schema_map, "Schema mismatch for ta_disc_zusgf"
    ```

#### Test Case 4.2: Row Count - No Source Data

*   **Purpose**: Verify that if `ta_discount` is empty, `ta_disc_zusgf` remains empty.
*   **Setup**:
    1.  Clear `ta_discount` (both Oracle and BigQuery).
    2.  Clear `ta_disc_zusgf` (both Oracle and BigQuery).
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   `COUNT(*)` on `ta_disc_zusgf` (both Oracle and BigQuery) must return 0.
    *   `records_processed` in `job_run_log` (BigQuery) and temporary file (Legacy) must be 0.

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT COUNT(*) FROM `my_project.my_dataset.ta_disc_zusgf`;
    -- Expected: 0
    ```

#### Test Case 4.3: Row Count - With Source Data

*   **Purpose**: Verify that the final row count in `ta_disc_zusgf` is correct based on the distinct combinations from `ta_discount`.
*   **Setup**:
    1.  Populate `ta_discount` (both Oracle and BigQuery) with a known set of data, including duplicates that would be collapsed by the `DISTINCT` clause in the `dzg` subquery.
    2.  Clear `ta_disc_zusgf` (both Oracle and BigQuery).
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   The `COUNT(*)` on `ta_disc_zusgf` (both Oracle and BigQuery) must equal the count of `DISTINCT (cntrct_id, disc_vector_ty, cntrct_obj_version)` from the `ta_discount` source table.
    *   `records_processed` in `job_run_log` (BigQuery) and temporary file (Legacy) must match this count.

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT COUNT(*) FROM `my_project.my_dataset.ta_disc_zusgf`;
    -- Expected: (SELECT COUNT(DISTINCT CONCAT(CAST(cntrct_id AS STRING), '-', disc_vector_ty, '-', CAST(cntrct_obj_version AS STRING))) FROM `my_project.my_dataset.ta_discount`)
    ```

#### Test Case 4.4: Data Integrity - No Duplicate Primary Keys

*   **Purpose**: Ensure that the target table `ta_disc_zusgf` does not contain duplicate rows based on its natural key (likely `cntrct_id, cntrct_obj_version, disc_vector_ty`).
*   **Setup**:
    1.  Populate `ta_discount` (both Oracle and BigQuery) with data that might lead to duplicates if the `DISTINCT` clause or join logic is flawed.
    2.  Clear `ta_disc_zusgf` (both Oracle and BigQuery).
*   **Action**: Execute both legacy and migrated jobs with valid parameters.
*   **Pass/Fail Criterion**:
    *   A query counting duplicates in `ta_disc_zusgf` must return 0.

    ```sql
    -- BigQuery Assertion (after job run)
    SELECT COUNT(*) FROM (
        SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, COUNT(*)
        FROM `my_project.my_dataset.ta_disc_zusgf`
        GROUP BY cntrct_id, cntrct_obj_version, disc_vector_ty
        HAVING COUNT(*) > 1
    );
    -- Expected: 0
    ```

---