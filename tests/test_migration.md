The migration of `k_ausd_v_ta_bp_ref.ksh` to BigQuery involves converting a KornShell orchestration script and its dependent SQL logic into BigQuery Stored Procedures, along with dedicated logging tables. The following tests aim to validate this migration across output parity, transformation correctness, external system replacements, and data quality.

**Assumptions for Testing:**
*   The BigQuery DDLs for `project.dataset.error_log` and `project.dataset.job_run_log` have been executed.
*   A source table `project.dataset.cds_ta_bp_ref` and a target table `project.dataset.sof_ta_bp_ref` exist with the following schemas (or compatible ones):

    ```sql
    -- Source table schema (cds_ta_bp_ref)
    CREATE TABLE IF NOT EXISTS project.dataset.cds_ta_bp_ref (
        cntrct_cp2_id STRING,
        bp_id STRING,
        insert_at TIMESTAMP,
        modified_at TIMESTAMP,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_production INT64,
        bp_ref_ty INT64
    );

    -- Target table schema (sof_ta_bp_ref)
    CREATE TABLE IF NOT EXISTS project.dataset.sof_ta_bp_ref (
        cntrct_cp2_id STRING,
        bp_id STRING
    );
    ```
*   The BigQuery Stored Procedures `project.dataset.d_ausd_v_ta_bp_ref_logic` and `project.dataset.r_ausd_vertrag_control` have been deployed.
*   The tests assume a BigQuery environment where `project.dataset` refers to the appropriate project and dataset.

---

### Test Case 1: Successful End-to-End Execution (Output Parity & Transformation)

*   **Purpose:** Verify that the `r_ausd_vertrag_control` procedure successfully orchestrates the `d_ausd_v_ta_bp_ref_logic` procedure, resulting in correct data in the target table and accurate logging for a happy path.
*   **Setup:**
    1.  Clear all relevant tables:
        ```sql
        TRUNCATE TABLE project.dataset.cds_ta_bp_ref;
        TRUNCATE TABLE project.dataset.sof_ta_bp_ref;
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
    2.  Insert sample data into `project.dataset.cds_ta_bp_ref` that should be processed:
        ```sql
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('C1', 'BP1', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        ('C2', 'BP2', '2023-01-12 11:00:00 UTC', '2023-01-20 12:00:00 UTC', '2023-01-05 00:00:00 UTC', '2023-01-25 00:00:00 UTC', 1, 4),
        ('C3', 'BP3', '2023-01-15 13:00:00 UTC', NULL, '2023-01-10 00:00:00 UTC', NULL, 1, 4);
        -- Data that should NOT be processed (for p_stichtag = '2023-01-15'):
        -- insert_at > p_stichtag
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('C4', 'BP4', '2023-01-16 14:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
        -- modified_at <= p_stichtag
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('C5', 'BP5', '2023-01-10 10:00:00 UTC', '2023-01-14 12:00:00 UTC', '2023-01-01 00:00:00 UTC', NULL, 1, 4);
        -- is_production = 0
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('C6', 'BP6', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 0, 4);
        -- bp_ref_ty != 4
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('C7', 'BP7', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 5);
        ```
*   **Action:** Call `project.dataset.r_ausd_vertrag_control` with specific parameters.
    ```sql
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_001',
        p_eintrags_nr => 'ENTRY_001',
        p_stichtag => '2023-01-15'
    );
    ```
*   **Pass/Fail Criterion:**
    *   `project.dataset.sof_ta_bp_ref` contains exactly 3 rows: `('C1', 'BP1')`, `('C2', 'BP2')`, `('C3', 'BP3')`.
    *   `project.dataset.job_run_log` contains one entry for `TEST_JOB_001`/`ENTRY_001` with `records_processed = 3`.
    *   `project.dataset.error_log` contains no entries.

    ```python
    # Pytest assertion example
    from google.cloud import bigquery

    client = bigquery.Client()
    job_kennung = 'TEST_JOB_001'
    eintrags_nr = 'ENTRY_001'
    stichtag = '2023-01-15'

    # Assert target table content
    query_target = f"SELECT cntrct_cp2_id, bp_id FROM project.dataset.sof_ta_bp_ref ORDER BY cntrct_cp2_id;"
    rows_target = list(client.query(query_target).result())
    expected_target = [
        ('C1', 'BP1'),
        ('C2', 'BP2'),
        ('C3', 'BP3')
    ]
    assert len(rows_target) == len(expected_target)
    assert sorted(rows_target) == sorted(expected_target)

    # Assert job_run_log content
    query_log = f"SELECT job_kennung, eintrags_nr, records_processed FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_log = list(client.query(query_log).result())
    assert len(rows_log) == 1
    assert rows_log[0].job_kennung == job_kennung
    assert rows_log[0].eintrags_nr == eintrags_nr
    assert rows_log[0].records_processed == 3

    # Assert error_log is empty
    query_error = f"SELECT COUNT(*) FROM project.dataset.error_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_error = list(client.query(query_error).result())
    assert rows_error[0][0] == 0
    ```

---

### Test Case 2: Parameter Validation - Missing JobKennung (Transformation Correctness - Error Handling)

*   **Purpose:** Verify that the `r_ausd_vertrag_control` procedure correctly handles missing `p_job_kennung` as per the legacy `pruefeParameterGesetzt` functionality.
*   **Setup:**
    1.  Clear error log:
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
*   **Action:** Attempt to call `project.dataset.r_ausd_vertrag_control` with `p_job_kennung = NULL`.
    ```sql
    -- This call is expected to fail. In BigQuery, you might need to wrap it in a BEGIN...EXCEPTION block
    -- or just observe the error from the client.
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => NULL,
        p_eintrags_nr => 'ENTRY_002',
        p_stichtag => '2023-01-15'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The call fails with an error message indicating missing `JobKennung` (e.g., "FEHLER: JobKennung parameter is missing or empty.").
    *   `project.dataset.error_log` contains one entry with `error_code = 193`, `error_arg = 'JobKennung'`, and the expected message.
    *   `project.dataset.job_run_log` contains no entries for this attempted run.
    *   `project.dataset.sof_ta_bp_ref` remains unchanged.

    ```python
    # Pytest assertion example
    import pytest
    from google.cloud import bigquery

    client = bigquery.Client()
    eintrags_nr = 'ENTRY_002'

    # Assert that the call raises an exception
    with pytest.raises(Exception) as excinfo:
        client.query(f"""
            CALL project.dataset.r_ausd_vertrag_control(
                p_job_kennung => NULL,
                p_eintrags_nr => '{eintrags_nr}',
                p_stichtag => '2023-01-15'
            );
        """).result()
    assert "FEHLER: JobKennung parameter is missing or empty." in str(excinfo.value)

    # Assert error_log content
    query_error = f"SELECT error_code, error_arg, message FROM project.dataset.error_log WHERE eintrags_nr = '{eintrags_nr}';"
    rows_error = list(client.query(query_error).result())
    assert len(rows_error) == 1
    assert rows_error[0].error_code == 193
    assert rows_error[0].error_arg == 'JobKennung'
    assert "FEHLER: JobKennung parameter is missing or empty." in rows_error[0].message

    # Assert job_run_log is empty
    query_log = f"SELECT COUNT(*) FROM project.dataset.job_run_log WHERE eintrags_nr = '{eintrags_nr}';"
    rows_log = list(client.query(query_log).result())
    assert rows_log[0][0] == 0
    ```

---

### Test Case 3: Parameter Validation - Missing EintragsNr (Transformation Correctness - Error Handling)

*   **Purpose:** Verify that the `r_ausd_vertrag_control` procedure correctly handles missing `p_eintrags_nr`.
*   **Setup:**
    1.  Clear error log:
        ```sql
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
*   **Action:** Attempt to call `project.dataset.r_ausd_vertrag_control` with `p_eintrags_nr = NULL`.
    ```sql
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_003',
        p_eintrags_nr => NULL,
        p_stichtag => '2023-01-15'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The call fails with an error message indicating missing `EintragsNr`.
    *   `project.dataset.error_log` contains one entry with `error_code = 193`, `error_arg = 'EintragsNr'`, and the expected message.
    *   `project.dataset.job_run_log` contains no entries for this attempted run.
    *   `project.dataset.sof_ta_bp_ref` remains unchanged.

    ```python
    # Pytest assertion example (similar to Test Case 2)
    import pytest
    from google.cloud import bigquery

    client = bigquery.Client()
    job_kennung = 'TEST_JOB_003'

    with pytest.raises(Exception) as excinfo:
        client.query(f"""
            CALL project.dataset.r_ausd_vertrag_control(
                p_job_kennung => '{job_kennung}',
                p_eintrags_nr => NULL,
                p_stichtag => '2023-01-15'
            );
        """).result()
    assert "FEHLER: EintragsNr parameter is missing or empty." in str(excinfo.value)

    query_error = f"SELECT error_code, error_arg, message FROM project.dataset.error_log WHERE job_kennung = '{job_kennung}';"
    rows_error = list(client.query(query_error).result())
    assert len(rows_error) == 1
    assert rows_error[0].error_code == 193
    assert rows_error[0].error_arg == 'EintragsNr'
    assert "FEHLER: EintragsNr parameter is missing or empty." in rows_error[0].message

    query_log = f"SELECT COUNT(*) FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung}';"
    rows_log = list(client.query(query_log).result())
    assert rows_log[0][0] == 0
    ```

---

### Test Case 4: `d_ausd_v_ta_bp_ref_logic` - Data Filtering and NULL Handling (Transformation Correctness)

*   **Purpose:** Verify the correctness of the `WHERE` clause logic in `d_ausd_v_ta_bp_ref_logic`, specifically date comparisons and NULL handling for `modified_at` and `valid_to`.
*   **Setup:**
    1.  Clear all relevant tables:
        ```sql
        TRUNCATE TABLE project.dataset.cds_ta_bp_ref;
        TRUNCATE TABLE project.dataset.sof_ta_bp_ref;
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
    2.  Insert diverse data into `project.dataset.cds_ta_bp_ref` to test all filter conditions.
        ```sql
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        -- 1. Should match (all conditions met, NULLs handled)
        ('M1', 'BP_M1', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        ('M2', 'BP_M2', '2023-01-10 10:00:00 UTC', '2023-01-20 10:00:00 UTC', '2023-01-01 00:00:00 UTC', '2023-01-25 00:00:00 UTC', 1, 4),
        -- 2. Should NOT match: insert_at > p_stichtag
        ('NM1', 'BP_NM1', '2023-01-16 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        -- 3. Should NOT match: modified_at <= p_stichtag (and not NULL)
        ('NM2', 'BP_NM2', '2023-01-10 10:00:00 UTC', '2023-01-14 10:00:00 UTC', '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        -- 4. Should NOT match: valid_from > p_stichtag
        ('NM3', 'BP_NM3', '2023-01-10 10:00:00 UTC', NULL, '2023-01-16 00:00:00 UTC', NULL, 1, 4),
        -- 5. Should NOT match: valid_to <= p_stichtag (and not NULL)
        ('NM4', 'BP_NM4', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', '2023-01-14 00:00:00 UTC', 1, 4),
        -- 6. Should NOT match: is_production = 0
        ('NM5', 'BP_NM5', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 0, 4),
        -- 7. Should NOT match: bp_ref_ty != 4
        ('NM6', 'BP_NM6', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 5);
        ```
*   **Action:** Call `project.dataset.r_ausd_vertrag_control` with `p_stichtag = '2023-01-15'`.
    ```sql
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_004',
        p_eintrags_nr => 'ENTRY_004',
        p_stichtag => '2023-01-15'
    );
    ```
*   **Pass/Fail Criterion:**
    *   `project.dataset.sof_ta_bp_ref` contains exactly 2 rows: `('M1', 'BP_M1')`, `('M2', 'BP_M2')`.
    *   `project.dataset.job_run_log` contains one entry for `TEST_JOB_004`/`ENTRY_004` with `records_processed = 2`.
    *   `project.dataset.error_log` contains no entries.

    ```python
    # Pytest assertion example
    from google.cloud import bigquery

    client = bigquery.Client()
    job_kennung = 'TEST_JOB_004'
    eintrags_nr = 'ENTRY_004'
    stichtag = '2023-01-15'

    query_target = f"SELECT cntrct_cp2_id, bp_id FROM project.dataset.sof_ta_bp_ref ORDER BY cntrct_cp2_id;"
    rows_target = list(client.query(query_target).result())
    expected_target = [
        ('M1', 'BP_M1'),
        ('M2', 'BP_M2')
    ]
    assert len(rows_target) == len(expected_target)
    assert sorted(rows_target) == sorted(expected_target)

    query_log = f"SELECT records_processed FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_log = list(client.query(query_log).result())
    assert len(rows_log) == 1
    assert rows_log[0].records_processed == 2

    query_error = f"SELECT COUNT(*) FROM project.dataset.error_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_error = list(client.query(query_error).result())
    assert rows_error[0][0] == 0
    ```

---

### Test Case 5: Error during `d_ausd_v_ta_bp_ref_logic` execution (Transformation Correctness - Error Handling)

*   **Purpose:** Verify that errors occurring within the business logic procedure are caught, logged, and propagated by the control procedure.
*   **Setup:**
    1.  Clear all relevant tables:
        ```sql
        TRUNCATE TABLE project.dataset.cds_ta_bp_ref;
        TRUNCATE TABLE project.dataset.sof_ta_bp_ref;
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
    2.  Insert some data into `cds_ta_bp_ref`.
        ```sql
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('C1', 'BP1', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
        ```
    3.  **Simulate an error:** Temporarily modify `project.dataset.d_ausd_v_ta_bp_ref_logic` to cause an error during the `INSERT` statement (e.g., by attempting to insert a `STRING` into an `INT64` column, or by explicitly raising an error). For this test, we'll assume a temporary modification to `d_ausd_v_ta_bp_ref_logic` to `RAISE` an error.
        ```sql
        -- TEMPORARY MODIFICATION FOR TESTING ONLY
        CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_v_ta_bp_ref_logic(
            IN p_stichtag DATE,
            IN p_job_kennung STRING,
            IN p_eintrags_nr STRING
        )
        BEGIN
            DECLARE v_error_code INT64 DEFAULT 0;
            DECLARE v_error_message STRING;

            TRUNCATE TABLE project.dataset.sof_ta_bp_ref;

            -- Simulate an error during insert
            BEGIN
                RAISE USING MESSAGE 'Simulated error during data insertion for testing.';
            EXCEPTION WHEN ERROR THEN
                SET v_error_code = @@error.code;
                SET v_error_message = @@error.message;
                INSERT INTO project.dataset.error_log (error_ts, error_code, error_arg, job_kennung, eintrags_nr, script_name, message)
                VALUES (CURRENT_TIMESTAMP(), v_error_code, NULL, p_job_kennung, p_eintrags_nr, 'd_ausd_v_ta_bp_ref_logic', 'Error inserting data: ' || v_error_message);
                RAISE;
            END;
        END;
        ```
*   **Action:** Call `project.dataset.r_ausd_vertrag_control` with valid parameters.
    ```sql
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_005',
        p_eintrags_nr => 'ENTRY_005',
        p_stichtag => '2023-01-15'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The call to `r_ausd_vertrag_control` fails and propagates the error.
    *   `project.dataset.error_log` contains one entry related to the simulated error in `d_ausd_v_ta_bp_ref_logic`, with `script_name = 'd_ausd_v_ta_bp_ref_logic'` and a message like 'Simulated error during data insertion for testing.'.
    *   `project.dataset.job_run_log` contains no entries for this run.
    *   `project.dataset.sof_ta_bp_ref` remains empty (due to `TRUNCATE` and then error during `INSERT`).

    ```python
    # Pytest assertion example
    import pytest
    from google.cloud import bigquery

    client = bigquery.Client()
    job_kennung = 'TEST_JOB_005'
    eintrags_nr = 'ENTRY_005'

    with pytest.raises(Exception) as excinfo:
        client.query(f"""
            CALL project.dataset.r_ausd_vertrag_control(
                p_job_kennung => '{job_kennung}',
                p_eintrags_nr => '{eintrags_nr}',
                p_stichtag => '2023-01-15'
            );
        """).result()
    assert "Simulated error during data insertion for testing." in str(excinfo.value)

    query_error = f"SELECT script_name, message FROM project.dataset.error_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_error = list(client.query(query_error).result())
    assert len(rows_error) == 1
    assert rows_error[0].script_name == 'd_ausd_v_ta_bp_ref_logic'
    assert "Simulated error during data insertion for testing." in rows_error[0].message

    query_log = f"SELECT COUNT(*) FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_log = list(client.query(query_log).result())
    assert rows_log[0][0] == 0

    query_target = f"SELECT COUNT(*) FROM project.dataset.sof_ta_bp_ref;"
    rows_target = list(client.query(query_target).result())
    assert rows_target[0][0] == 0

    # IMPORTANT: Revert the temporary modification to d_ausd_v_ta_bp_ref_logic after this test.
    ```

---

### Test Case 6: Schema and Data Type Assertions (Data Quality / Schema Assertions)

*   **Purpose:** Verify that the schemas of the target table and log tables match the design and expected data types.
*   **Setup:** Ensure all DDLs (`error_log.sql`, `job_run_log.sql`, and the assumed `sof_ta_bp_ref` DDL) have been executed.
*   **Action:** Query the BigQuery `INFORMATION_SCHEMA` for table and column details.
*   **Pass/Fail Criterion:**
    *   `project.dataset.error_log` schema matches:
        `error_ts TIMESTAMP`, `error_code INT64`, `error_arg STRING`, `job_kennung STRING`, `eintrags_nr STRING`, `script_name STRING`, `message STRING`.
    *   `project.dataset.job_run_log` schema matches:
        `run_ts TIMESTAMP`, `job_kennung STRING`, `eintrags_nr STRING`, `tab_name STRING`, `records_processed INT64`.
    *   `project.dataset.sof_ta_bp_ref` schema matches:
        `cntrct_cp2_id STRING`, `bp_id STRING`.

    ```sql
    -- SQL to check error_log schema
    SELECT column_name, data_type FROM project.dataset.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'error_log' ORDER BY ordinal_position;
    -- Expected output:
    -- error_ts, TIMESTAMP
    -- error_code, INT64
    -- error_arg, STRING
    -- job_kennung, STRING
    -- eintrags_nr, STRING
    -- script_name, STRING
    -- message, STRING

    -- SQL to check job_run_log schema
    SELECT column_name, data_type FROM project.dataset.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'job_run_log' ORDER BY ordinal_position;
    -- Expected output:
    -- run_ts, TIMESTAMP
    -- job_kennung, STRING
    -- eintrags_nr, STRING
    -- tab_name, STRING
    -- records_processed, INT64

    -- SQL to check sof_ta_bp_ref schema
    SELECT column_name, data_type FROM project.dataset.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'sof_ta_bp_ref' ORDER BY ordinal_position;
    -- Expected output:
    -- cntrct_cp2_id, STRING
    -- bp_id, STRING
    ```

---

### Test Case 7: Default `p_stichtag` behavior (Transformation Correctness)

*   **Purpose:** Verify that if `p_stichtag` is not explicitly provided (or provided as NULL) to `r_ausd_vertrag_control`, it defaults to `CURRENT_DATE()`.
*   **Setup:**
    1.  Clear all relevant tables:
        ```sql
        TRUNCATE TABLE project.dataset.cds_ta_bp_ref;
        TRUNCATE TABLE project.dataset.sof_ta_bp_ref;
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
    2.  Insert data into `project.dataset.cds_ta_bp_ref` where some rows would only match if `p_stichtag` is `CURRENT_DATE()`.
        ```sql
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        -- Should match if p_stichtag = CURRENT_DATE() (assuming today is 2023-01-20 or later)
        ('D1', 'BP_D1', '2023-01-20 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        -- Should NOT match if p_stichtag = CURRENT_DATE() (insert_at > CURRENT_DATE())
        ('D2', 'BP_D2', CURRENT_TIMESTAMP() + INTERVAL 1 DAY, NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
        ```
*   **Action:** Call `project.dataset.r_ausd_vertrag_control` with `p_stichtag = NULL`.
    ```sql
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_007',
        p_eintrags_nr => 'ENTRY_007',
        p_stichtag => NULL
    );
    ```
*   **Pass/Fail Criterion:**
    *   `project.dataset.sof_ta_bp_ref` contains exactly 1 row: `('D1', 'BP_D1')` (assuming `CURRENT_DATE()` is `2023-01-20` or later).
    *   `project.dataset.job_run_log` contains one entry for `TEST_JOB_007`/`ENTRY_007` with `records_processed = 1`.
    *   `project.dataset.error_log` contains no entries.

    ```python
    # Pytest assertion example
    from google.cloud import bigquery
    import datetime

    client = bigquery.Client()
    job_kennung = 'TEST_JOB_007'
    eintrags_nr = 'ENTRY_007'
    current_date_str = datetime.date.today().isoformat() # For comparison

    query_target = f"SELECT cntrct_cp2_id, bp_id FROM project.dataset.sof_ta_bp_ref ORDER BY cntrct_cp2_id;"
    rows_target = list(client.query(query_target).result())
    expected_target = [('D1', 'BP_D1')]
    assert len(rows_target) == len(expected_target)
    assert sorted(rows_target) == sorted(expected_target)

    query_log = f"SELECT records_processed FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_log = list(client.query(query_log).result())
    assert len(rows_log) == 1
    assert rows_log[0].records_processed == 1

    query_error = f"SELECT COUNT(*) FROM project.dataset.error_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_error = list(client.query(query_error).result())
    assert rows_error[0][0] == 0
    ```

---

### Test Case 8: Idempotency (Data Quality)

*   **Purpose:** Verify that running the job multiple times with the same parameters produces the same final state in the target table and correctly logs each run.
*   **Setup:**
    1.  Clear all relevant tables:
        ```sql
        TRUNCATE TABLE project.dataset.cds_ta_bp_ref;
        TRUNCATE TABLE project.dataset.sof_ta_bp_ref;
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
    2.  Insert sample data into `project.dataset.cds_ta_bp_ref`.
        ```sql
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('I1', 'BP_I1', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
        ('I2', 'BP_I2', '2023-01-12 11:00:00 UTC', '2023-01-20 12:00:00 UTC', '2023-01-05 00:00:00 UTC', '2023-01-25 00:00:00 UTC', 1, 4);
        ```
*   **Action:**
    1.  Call `project.dataset.r_ausd_vertrag_control` with valid parameters.
    2.  Call `project.dataset.r_ausd_vertrag_control` again with the *same* valid parameters.
    ```sql
    -- First run
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_008',
        p_eintrags_nr => 'ENTRY_008',
        p_stichtag => '2023-01-15'
    );
    -- Second run
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_008',
        p_eintrags_nr => 'ENTRY_008',
        p_stichtag => '2023-01-15'
    );
    ```
*   **Pass/Fail Criterion:**
    *   `project.dataset.sof_ta_bp_ref` contains exactly 2 rows: `('I1', 'BP_I1')`, `('I2', 'BP_I2')`. The content should be identical after both runs.
    *   `project.dataset.job_run_log` contains two entries for `TEST_JOB_008`/`ENTRY_008`, each with `records_processed = 2`.
    *   `project.dataset.error_log` contains no entries.

    ```python
    # Pytest assertion example
    from google.cloud import bigquery

    client = bigquery.Client()
    job_kennung = 'TEST_JOB_008'
    eintrags_nr = 'ENTRY_008'
    stichtag = '2023-01-15'

    query_target = f"SELECT cntrct_cp2_id, bp_id FROM project.dataset.sof_ta_bp_ref ORDER BY cntrct_cp2_id;"
    rows_target = list(client.query(query_target).result())
    expected_target = [
        ('I1', 'BP_I1'),
        ('I2', 'BP_I2')
    ]
    assert len(rows_target) == len(expected_target)
    assert sorted(rows_target) == sorted(expected_target)

    query_log = f"SELECT records_processed FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_log = list(client.query(query_log).result())
    assert len(rows_log) == 2 # Two entries for two runs
    for row in rows_log:
        assert row.records_processed == 2

    query_error = f"SELECT COUNT(*) FROM project.dataset.error_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_error = list(client.query(query_error).result())
    assert rows_error[0][0] == 0
    ```

---

### Test Case 9: Job Management Logic (External-system replacements - Placeholder)

*   **Purpose:** Acknowledge and test the current state of the "Job Management Logic" (ignore active jobs, deactivate old jobs). As per the design and generated code, this is a placeholder. This test confirms its current no-op behavior.
*   **Setup:**
    1.  Clear all relevant tables:
        ```sql
        TRUNCATE TABLE project.dataset.cds_ta_bp_ref;
        TRUNCATE TABLE project.dataset.sof_ta_bp_ref;
        TRUNCATE TABLE project.dataset.error_log;
        TRUNCATE TABLE project.dataset.job_run_log;
        ```
    2.  Insert sample data into `project.dataset.cds_ta_bp_ref`.
        ```sql
        INSERT INTO project.dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty) VALUES
        ('J1', 'BP_J1', '2023-01-10 10:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
        ```
*   **Action:** Run the `r_ausd_vertrag_control` procedure.
    ```sql
    CALL project.dataset.r_ausd_vertrag_control(
        p_job_kennung => 'TEST_JOB_009',
        p_eintrags_nr => 'ENTRY_009',
        p_stichtag => '2023-01-15'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully, processing data and logging the run, without any errors or warnings related to "active jobs" or "deactivating old jobs".
    *   No interaction with a `job_metadata` table (or similar) occurs, confirming the placeholder is currently a no-op.
    *   `project.dataset.sof_ta_bp_ref` contains the expected data.
    *   `project.dataset.job_run_log` contains one entry.
    *   `project.dataset.error_log` contains no entries.

    ```python
    # Pytest assertion example
    from google.cloud import bigquery

    client = bigquery.Client()
    job_kennung = 'TEST_JOB_009'
    eintrags_nr = 'ENTRY_009'

    # This test primarily confirms the absence of unexpected behavior.
    # A successful run implies the placeholder logic didn't interfere.
    # Further tests would be needed once the job management logic is implemented.
    query_log = f"SELECT records_processed FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_log = list(client.query(query_log).result())
    assert len(rows_log) == 1
    assert rows_log[0].records_processed == 1

    query_error = f"SELECT COUNT(*) FROM project.dataset.error_log WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}';"
    rows_error = list(client.query(query_error).result())
    assert rows_error[0][0] == 0
    ```
    **Note:** This test highlights an unresolved risk. Once the "Job Management Logic" is defined and implemented, this test case needs to be updated to verify that logic (e.g., if a job is marked 'ACTIVE', subsequent runs are skipped or an error is raised). For now, it confirms the current state of the migration.

---

### Test Case 10: Cloud Composer DAG Invocation (External-system replacements)

*   **Purpose:** Verify that the generated Cloud Composer DAG can successfully invoke the `r_ausd_vertrag_control` BigQuery Stored Procedure, replacing the legacy KornShell script's external invocation mechanism.
*   **Setup:**
    1.  Deploy the `airflow/dags/k_ausd_v_ta_bp_ref_migration.py` DAG to a Cloud Composer environment.
    2.  Update the DAG's parameters (`p_job_kennung`, `p_eintrags_nr`) to unique values for this test run (e.g., `DAG_JOB_001`, `DAG_ENTRY_001`).
    3.  Ensure `project.dataset.cds_ta_bp_ref` has sample data (e.g., from Test Case 1 setup).
    4.  Clear `project.dataset.sof_ta_bp_ref`, `project.dataset.error_log`, and `project.dataset.job_run_log`.
*   **Action:** Trigger the `k_ausd_v_ta_bp_ref_migration` DAG in Cloud Composer.
*   **Pass/Fail Criterion:**
    *   The DAG runs successfully in Cloud Composer (all tasks complete with green status).
    *   `project.dataset.sof_ta_bp_ref` contains the expected data based on the `cds_ta_bp_ref` setup and the `p_stichtag` used in the DAG.
    *   `project.dataset.job_run_log` contains one entry for the DAG's `job_kennung`/`eintrags_nr`, with `records_processed` matching the count in `sof_ta_bp_ref`.
    *   `project.dataset.error_log` contains no entries.

    ```python
    # Pytest assertion example (after triggering the DAG manually or via schedule)
    from google.cloud import bigquery
    from airflow.models import DagRun
    from airflow.utils.session import provide_session
    from airflow.utils.state import State

    client = bigquery.Client()
    dag_id = "k_ausd_v_ta_bp_ref_migration"
    job_kennung_dag = 'DAG_JOB_001' # As configured in the DAG
    eintrags_nr_dag = 'DAG_ENTRY_001' # As configured in the DAG

    @provide_session
    def check_dag_run_status(session=None):
        # Find the most recent DAG run for the specified DAG ID
        dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date.desc()).first()
        assert dag_run is not None, f"No DAG run found for {dag_id}"
        assert dag_run.state == State.SUCCESS, f"DAG run for {dag_id} failed with state {dag_run.state}"

    # Call the function to check DAG status
    check_dag_run_status()

    # Assert target table content (assuming 3 rows from Test Case 1 setup)
    query_target = f"SELECT COUNT(*) FROM project.dataset.sof_ta_bp_ref;"
    rows_target = list(client.query(query_target).result())
    assert rows_target[0][0] == 3 # Or whatever count is expected from your setup

    # Assert job_run_log content
    query_log = f"SELECT job_kennung, eintrags_nr, records_processed FROM project.dataset.job_run_log WHERE job_kennung = '{job_kennung_dag}' AND eintrags_nr = '{eintrags_nr_dag}';"
    rows_log = list(client.query(query_log).result())
    assert len(rows_log) == 1
    assert rows_log[0].job_kennung == job_kennung_dag
    assert rows_log[0].eintrags_nr == eintrags_nr_dag
    assert rows_log[0].records_processed == 3

    # Assert error_log is empty
    query_error = f"SELECT COUNT(*) FROM project.dataset.error_log WHERE job_kennung = '{job_kennung_dag}' AND eintrags_nr = '{eintrags_nr_dag}';"
    rows_error = list(client.query(query_error).result())
    assert rows_error[0][0] == 0
    ```