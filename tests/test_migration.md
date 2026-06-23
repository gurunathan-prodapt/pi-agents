As a senior data-migration QA engineer, I have designed a comprehensive suite of migration validation tests for the `r_ausd_bp_ta_iccid_vertrag.ksh` script, now migrated to a BigQuery Stored Procedure `ausd_bp_ta_iccid_vertrag_wrapper`. These tests aim to ensure behavioral equivalence, transformation correctness, and proper integration with the new BigQuery environment.

The core logic of `k_ausd_bp_ta_iccid_vertrag.ksh` is assumed to be migrated to a separate BigQuery Stored Procedure `k_ausd_bp_ta_iccid_vertrag`. For the purpose of testing the *wrapper* (`ausd_bp_ta_iccid_vertrag_wrapper`), a test stub for `k_ausd_bp_ta_iccid_vertrag` will be used. This stub will log its invocation and parameters to the `job_audit` table, allowing us to verify the wrapper's interaction with its downstream component.

**Assumptions & Pre-requisites:**
*   BigQuery project and dataset (`project.dataset`) are configured.
*   The `job_audit` table has been created using the provided DDL.
*   The `k_ausd_bp_ta_iccid_vertrag` BigQuery Stored Procedure (test stub) has been deployed.
*   The `ausd_bp_ta_iccid_vertrag_wrapper` BigQuery Stored Procedure has been deployed.
*   Tests are executed in an environment with BigQuery access (e.g., via `bq` CLI, Python client, or a testing framework like `pytest-bigquery`).
*   `CURRENT_DATE()` in BigQuery is assumed to align with the system date used by the original ksh script for `v_sysdate`.

---

### Test Stub for `k_ausd_bp_ta_iccid_vertrag`

To effectively test the wrapper, we need a version of `k_ausd_bp_ta_iccid_vertrag` that allows us to observe its invocation and parameters.

```sql
-- FILE: sql/stored_procedures/k_ausd_bp_ta_iccid_vertrag.sql (Test Stub)
-- This stub is used for testing the wrapper's invocation and parameter passing.
-- It logs its call to the job_audit table and can optionally simulate failure.

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_iccid_vertrag`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_dwh_eintragsnr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Log invocation and parameters for testing
  INSERT INTO `project.dataset.job_audit` (
    job_nr,
    job_kennung,
    script_name,
    log_timestamp,
    stichtag,
    status,
    message
  )
  VALUES (
    p_dwh_eintragsnr,
    p_jobkennung,
    'k_ausd_bp_ta_iccid_vertrag',
    CURRENT_TIMESTAMP(),
    p_stichtag,
    'INVOKED',
    FORMAT('Core script invoked with job_kennung=%s, stichtag=%s, dwh_eintragsnr=%d, wiederanlaufWert=%d',
           p_jobkennung, p_stichtag, p_dwh_eintragsnr, p_wiederanlaufWert)
  );

  -- By default, simulate success.
  -- To simulate failure for specific test cases, uncomment the following line:
  -- RAISE USING MESSAGE = 'Simulated failure in k_ausd_bp_ta_iccid_vertrag';
END;
```

---

### Test Case 1: Default Parameters - No Stichtag, No Wiederanlaufwert

*   **Purpose**: Verify the wrapper correctly defaults `p_stichtag` to `CURRENT_DATE()` (DDMMYYYY) and `p_wiederanlaufWert` to `0` when no parameters are explicitly provided. This covers transformation correctness for parameter defaulting.
*   **Setup**:
    1.  Ensure the `k_ausd_bp_ta_iccid_vertrag` stub is configured for successful execution.
    2.  Clear the `project.dataset.job_audit` table to ensure a clean state for `job_nr` generation.
        ```sql
        TRUNCATE TABLE `project.dataset.job_audit`;
        ```
*   **Action**: Execute the wrapper procedure without any parameters (passing `NULL` for both).
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully.
    2.  Query the `job_audit` table and assert the presence and content of log entries.
        ```sql
        SELECT job_nr, job_kennung, script_name, stichtag, status, message
        FROM `project.dataset.job_audit`
        ORDER BY log_timestamp;
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        today_ddmmyyyy = datetime.now().strftime('%d%m%Y')
        results = bq_client.query("""
            SELECT job_nr, job_kennung, script_name, stichtag, status, message
            FROM `project.dataset.job_audit`
            ORDER BY log_timestamp
        """).result()
        rows = list(results)

        assert len(rows) == 3
        assert rows[0].job_nr == 1 and rows[0].job_kennung == 'ausd_bp_ta_iccid_vertrag' and rows[0].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[0].status == 'STARTED' and rows[0].stichtag == today_ddmmyyyy and 'Wiederanlaufwert: 0' in rows[0].message
        assert rows[1].job_nr == 1 and rows[1].job_kennung == 'ausd_bp_ta_iccid_vertrag' and rows[1].script_name == 'k_ausd_bp_ta_iccid_vertrag' and rows[1].status == 'INVOKED' and rows[1].stichtag == today_ddmmyyyy and 'wiederanlaufWert=0' in rows[1].message
        assert rows[2].job_nr == 1 and rows[2].job_kennung == 'ausd_bp_ta_iccid_vertrag' and rows[2].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[2].status == 'OK' and rows[2].stichtag == today_ddmmyyyy
        ```

---

### Test Case 2: Explicit Stichtag, Default Wiederanlaufwert

*   **Purpose**: Verify the wrapper uses a provided `p_stichtag` and defaults `p_wiederanlaufWert` to `0`. This covers transformation correctness for parameter handling.
*   **Setup**:
    1.  Ensure the `k_ausd_bp_ta_iccid_vertrag` stub is configured for successful execution.
    2.  Clear the `project.dataset.job_audit` table.
        ```sql
        TRUNCATE TABLE `project.dataset.job_audit`;
        ```
*   **Action**: Execute the wrapper procedure with an explicit `Stichtag` and `NULL` for `Wiederanlaufwert`.
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('01012023', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully.
    2.  Query the `job_audit` table and assert the presence and content of log entries.
        ```sql
        SELECT job_nr, job_kennung, script_name, stichtag, status, message
        FROM `project.dataset.job_audit`
        ORDER BY log_timestamp;
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        results = bq_client.query("""
            SELECT job_nr, job_kennung, script_name, stichtag, status, message
            FROM `project.dataset.job_audit`
            ORDER BY log_timestamp
        """).result()
        rows = list(results)

        assert len(rows) == 3
        assert rows[0].job_nr == 1 and rows[0].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[0].status == 'STARTED' and rows[0].stichtag == '01012023' and 'Wiederanlaufwert: 0' in rows[0].message
        assert rows[1].job_nr == 1 and rows[1].script_name == 'k_ausd_bp_ta_iccid_vertrag' and rows[1].status == 'INVOKED' and rows[1].stichtag == '01012023' and 'wiederanlaufWert=0' in rows[1].message
        assert rows[2].job_nr == 1 and rows[2].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[2].status == 'OK' and rows[2].stichtag == '01012023'
        ```

---

### Test Case 3: Explicit Wiederanlaufwert, Default Stichtag

*   **Purpose**: Verify the wrapper uses a provided `p_wiederanlaufWert` and defaults `p_stichtag` to `CURRENT_DATE()`. This covers transformation correctness for parameter handling.
*   **Setup**:
    1.  Ensure the `k_ausd_bp_ta_iccid_vertrag` stub is configured for successful execution.
    2.  Clear the `project.dataset.job_audit` table.
        ```sql
        TRUNCATE TABLE `project.dataset.job_audit`;
        ```
*   **Action**: Execute the wrapper procedure with `NULL` for `Stichtag` and an explicit `Wiederanlaufwert`.
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`(NULL, 12345);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully.
    2.  Query the `job_audit` table and assert the presence and content of log entries.
        ```sql
        SELECT job_nr, job_kennung, script_name, stichtag, status, message
        FROM `project.dataset.job_audit`
        ORDER BY log_timestamp;
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        today_ddmmyyyy = datetime.now().strftime('%d%m%Y')
        results = bq_client.query("""
            SELECT job_nr, job_kennung, script_name, stichtag, status, message
            FROM `project.dataset.job_audit`
            ORDER BY log_timestamp
        """).result()
        rows = list(results)

        assert len(rows) == 3
        assert rows[0].job_nr == 1 and rows[0].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[0].status == 'STARTED' and rows[0].stichtag == today_ddmmyyyy and 'Wiederanlaufwert: 12345' in rows[0].message
        assert rows[1].job_nr == 1 and rows[1].script_name == 'k_ausd_bp_ta_iccid_vertrag' and rows[1].status == 'INVOKED' and rows[1].stichtag == today_ddmmyyyy and 'wiederanlaufWert=12345' in rows[1].message
        assert rows[2].job_nr == 1 and rows[2].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[2].status == 'OK' and rows[2].stichtag == today_ddmmyyyy
        ```

---

### Test Case 4: Both Explicit Parameters

*   **Purpose**: Verify the wrapper uses both provided `p_stichtag` and `p_wiederanlaufWert`. This covers output parity and transformation correctness.
*   **Setup**:
    1.  Ensure the `k_ausd_bp_ta_iccid_vertrag` stub is configured for successful execution.
    2.  Clear the `project.dataset.job_audit` table.
        ```sql
        TRUNCATE TABLE `project.dataset.job_audit`;
        ```
*   **Action**: Execute the wrapper procedure with both explicit parameters.
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('15062024', 98765);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully.
    2.  Query the `job_audit` table and assert the presence and content of log entries.
        ```sql
        SELECT job_nr, job_kennung, script_name, stichtag, status, message
        FROM `project.dataset.job_audit`
        ORDER BY log_timestamp;
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        results = bq_client.query("""
            SELECT job_nr, job_kennung, script_name, stichtag, status, message
            FROM `project.dataset.job_audit`
            ORDER BY log_timestamp
        """).result()
        rows = list(results)

        assert len(rows) == 3
        assert rows[0].job_nr == 1 and rows[0].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[0].status == 'STARTED' and rows[0].stichtag == '15062024' and 'Wiederanlaufwert: 98765' in rows[0].message
        assert rows[1].job_nr == 1 and rows[1].script_name == 'k_ausd_bp_ta_iccid_vertrag' and rows[1].status == 'INVOKED' and rows[1].stichtag == '15062024' and 'wiederanlaufWert=98765' in rows[1].message
        assert rows[2].job_nr == 1 and rows[2].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[2].status == 'OK' and rows[2].stichtag == '15062024'
        ```

---

### Test Case 5: Invalid Stichtag Format (Length)

*   **Purpose**: Verify the `ASSERT` statement for `Stichtag` length correctly catches invalid input, preventing further execution and logging. This covers transformation correctness for parameter validation.
*   **Setup**: Clear the `project.dataset.job_audit` table.
    ```sql
    TRUNCATE TABLE `project.dataset.job_audit`;
    ```
*   **Action**: Execute the wrapper procedure with a `Stichtag` in `YYYYMMDD` format (incorrect length for `DDMMYYYY`).
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('20230101', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement should fail with an error. The error message should contain "Stichtag must be provided in DDMMYYYY format."
    2.  Query the `job_audit` table. It should contain **no** entries for this `job_kennung` or `job_nr`, as the `ASSERT` occurs before the first `INSERT`.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.job_audit` WHERE job_kennung = 'ausd_bp_ta_iccid_vertrag';
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        try:
            bq_client.query("CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('20230101', NULL);").result()
            assert False, "Expected BigQuery CALL to fail due to invalid Stichtag format"
        except Exception as e:
            assert "Stichtag must be provided in DDMMYYYY format." in str(e)

        results = bq_client.query("SELECT COUNT(*) as count FROM `project.dataset.job_audit` WHERE job_kennung = 'ausd_bp_ta_iccid_vertrag';").result()
        assert list(results)[0].count == 0
        ```

---

### Test Case 6: Invalid Stichtag Format (Parseability)

*   **Purpose**: Verify the `ASSERT` statement for `Stichtag` parseability correctly catches invalid input, preventing further execution and logging. This covers transformation correctness for parameter validation.
*   **Setup**: Clear the `project.dataset.job_audit` table.
    ```sql
    TRUNCATE TABLE `project.dataset.job_audit`;
    ```
*   **Action**: Execute the wrapper procedure with a `Stichtag` of correct length but invalid date content.
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('99999999', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement should fail with an error. The error message should contain "Stichtag is not a valid DDMMYYYY date."
    2.  Query the `job_audit` table. It should contain **no** entries for this `job_kennung` or `job_nr`.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.job_audit` WHERE job_kennung = 'ausd_bp_ta_iccid_vertrag';
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        try:
            bq_client.query("CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('99999999', NULL);").result()
            assert False, "Expected BigQuery CALL to fail due to unparseable Stichtag"
        except Exception as e:
            assert "Stichtag is not a valid DDMMYYYY date." in str(e)

        results = bq_client.query("SELECT COUNT(*) as count FROM `project.dataset.job_audit` WHERE job_kennung = 'ausd_bp_ta_iccid_vertrag';").result()
        assert list(results)[0].count == 0
        ```

---

### Test Case 7: Core Script Failure Handling

*   **Purpose**: Verify the wrapper's `EXCEPTION WHEN ERROR` block correctly logs an `ERROR` status and re-raises the exception when the downstream core script (`k_ausd_bp_ta_iccid_vertrag`) fails. This covers transformation correctness for error handling.
*   **Setup**:
    1.  Modify the `k_ausd_bp_ta_iccid_vertrag` stub to `RAISE` an error.
        ```sql
        -- Re-deploy this stub for the test
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_iccid_vertrag`(
          IN p_jobkennung STRING, IN p_stichtag STRING, IN p_dwh_eintragsnr INT64, IN p_wiederanlaufWert INT64
        )
        BEGIN
          INSERT INTO `project.dataset.job_audit` (job_nr, job_kennung, script_name, log_timestamp, stichtag, status, message)
          VALUES (p_dwh_eintragsnr, p_jobkennung, 'k_ausd_bp_ta_iccid_vertrag', CURRENT_TIMESTAMP(), p_stichtag, 'INVOKED', 'Core script invoked, now simulating failure.');
          RAISE USING MESSAGE = 'Simulated failure in k_ausd_bp_ta_iccid_vertrag';
        END;
        ```
    2.  Clear the `project.dataset.job_audit` table.
        ```sql
        TRUNCATE TABLE `project.dataset.job_audit`;
        ```
*   **Action**: Execute the wrapper procedure with valid parameters.
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('01012023', 0);
    ```
*   **Pass/Fail Criterion**:
    1.  The `CALL` statement should fail with an error. The error message should contain "Job failed" or "AppError: Abbruch".
    2.  Query the `job_audit` table and assert the presence and content of log entries.
        ```sql
        SELECT job_nr, job_kennung, script_name, stichtag, status, message
        FROM `project.dataset.job_audit`
        ORDER BY log_timestamp;
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        try:
            bq_client.query("CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('01012023', 0);").result()
            assert False, "Expected BigQuery CALL to fail due to core script error"
        except Exception as e:
            assert "Job failed" in str(e) or "AppError: Abbruch" in str(e)

        results = bq_client.query("""
            SELECT job_nr, job_kennung, script_name, stichtag, status, message
            FROM `project.dataset.job_audit`
            ORDER BY log_timestamp
        """).result()
        rows = list(results)

        assert len(rows) == 3
        assert rows[0].job_nr == 1 and rows[0].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[0].status == 'STARTED' and rows[0].stichtag == '01012023'
        assert rows[1].job_nr == 1 and rows[1].script_name == 'k_ausd_bp_ta_iccid_vertrag' and rows[1].status == 'INVOKED' and rows[1].stichtag == '01012023' and 'Simulated failure' in rows[1].message
        assert rows[2].job_nr == 1 and rows[2].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[2].status == 'ERROR' and rows[2].stichtag == '01012023' and 'AppError: Abbruch - Simulated failure' in rows[2].message
        ```
    *(Remember to revert the `k_ausd_bp_ta_iccid_vertrag` stub to its successful state after this test.)*

---

### Test Case 8: Job Number Generation (Sequence)

*   **Purpose**: Verify that the `job_nr` is correctly incremented for consecutive runs of the same `job_kennung`, reflecting the original script's `DWMSG_ErmittleNr` behavior. This covers transformation correctness and data quality.
*   **Setup**:
    1.  Ensure the `k_ausd_bp_ta_iccid_vertrag` stub is configured for successful execution.
    2.  Clear the `project.dataset.job_audit` table.
        ```sql
        TRUNCATE TABLE `project.dataset.job_audit`;
        ```
*   **Action**: Execute the wrapper procedure multiple times with different parameters.
    ```sql
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('01012023', 0);
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`('02012023', 10);
    CALL `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`(NULL, NULL); -- Uses current date
    ```
*   **Pass/Fail Criterion**:
    1.  All `CALL` statements complete successfully.
    2.  Query the `job_audit` table and assert the `job_nr` sequence and parameter values.
        ```sql
        SELECT job_nr, job_kennung, script_name, stichtag, status, message
        FROM `project.dataset.job_audit`
        WHERE job_kennung = 'ausd_bp_ta_iccid_vertrag'
        ORDER BY job_nr, log_timestamp;
        ```
        **Expected Result (Python/Pytest assertion):**
        ```python
        today_ddmmyyyy = datetime.now().strftime('%d%m%Y')
        results = bq_client.query("""
            SELECT job_nr, job_kennung, script_name, stichtag, status, message
            FROM `project.dataset.job_audit`
            WHERE job_kennung = 'ausd_bp_ta_iccid_vertrag'
            ORDER BY job_nr, log_timestamp
        """).result()
        rows = list(results)

        assert len(rows) == 9 # 3 runs * 3 entries per run

        # Run 1 (job_nr = 1)
        assert rows[0].job_nr == 1 and rows[0].status == 'STARTED' and rows[0].stichtag == '01012023' and 'Wiederanlaufwert: 0' in rows[0].message
        assert rows[1].job_nr == 1 and rows[1].status == 'INVOKED' and rows[1].stichtag == '01012023' and 'wiederanlaufWert=0' in rows[1].message
        assert rows[2].job_nr == 1 and rows[2].status == 'OK' and rows[2].stichtag == '01012023'

        # Run 2 (job_nr = 2)
        assert rows[3].job_nr == 2 and rows[3].status == 'STARTED' and rows[3].stichtag == '02012023' and 'Wiederanlaufwert: 10' in rows[3].message
        assert rows[4].job_nr == 2 and rows[4].status == 'INVOKED' and rows[4].stichtag == '02012023' and 'wiederanlaufWert=10' in rows[4].message
        assert rows[5].job_nr == 2 and rows[5].status == 'OK' and rows[5].stichtag == '02012023'

        # Run 3 (job_nr = 3)
        assert rows[6].job_nr == 3 and rows[6].status == 'STARTED' and rows[6].stichtag == today_ddmmyyyy and 'Wiederanlaufwert: 0' in rows[6].message
        assert rows[7].job_nr == 3 and rows[7].status == 'INVOKED' and rows[7].stichtag == today_ddmmyyyy and 'wiederanlaufWert=0' in rows[7].message
        assert rows[8].job_nr == 3 and rows[8].status == 'OK' and rows[8].stichtag == today_ddmmyyyy
        ```

---

### Test Case 9: Schema Assertion for `job_audit` Table

*   **Purpose**: Verify that the `job_audit` table schema matches the DDL specified in the migration design, ensuring data quality and structural integrity.
*   **Setup**: Ensure the `project.dataset.job_audit` table exists.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `job_audit` table.
    ```sql
    SELECT column_name, data_type, is_nullable
    FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_audit'
    ORDER BY ordinal_position;
    ```
*   **Pass/Fail Criterion**: The query results match the expected schema definition.
    **Expected Result (Python/Pytest assertion):**
    ```python
    results = bq_client.query("""
        SELECT column_name, data_type, is_nullable
        FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_audit'
        ORDER BY ordinal_position
    """).result()
    schema_rows = {row.column_name: {'data_type': row.data_type, 'is_nullable': row.is_nullable} for row in results}

    expected_schema = {
        'job_nr': {'data_type': 'INT64', 'is_nullable': 'NO'},
        'job_kennung': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'script_name': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'log_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
        'stichtag': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'status': {'data_type': 'STRING', 'is_nullable': 'NO'},
        'message': {'data_type': 'STRING', 'is_nullable': 'YES'}
    }

    assert len(schema_rows) == len(expected_schema)
    for col_name, expected_props in expected_schema.items():
        assert col_name in schema_rows
        assert schema_rows[col_name]['data_type'] == expected_props['data_type']
        assert schema_rows[col_name]['is_nullable'] == expected_props['is_nullable']
    ```

---

### Test Case 10: Airflow DAG Invocation (Conceptual)

*   **Purpose**: Verify that the generated Airflow DAG correctly orchestrates the BigQuery Stored Procedure, passing parameters derived from the Airflow context (e.g., execution date). This covers external-system replacements.
*   **Setup**:
    1.  Deploy the `dags/ausd_bp_ta_iccid_vertrag_orchestrator.py` DAG to an Airflow environment.
    2.  Ensure the `k_ausd_bp_ta_iccid_vertrag` stub is configured for successful execution.
    3.  Clear the `project.dataset.job_audit` table.
        ```sql
        TRUNCATE TABLE `project.dataset.job_audit`;
        ```
*   **Action**: Manually trigger the Airflow DAG for a specific `execution_date`, e.g., `2024-06-15`.
*   **Pass/Fail Criterion**:
    1.  The Airflow task `call_ausd_bp_ta_iccid_vertrag_wrapper` completes successfully.
    2.  Inspect Airflow task logs for the BigQuery job ID and successful execution message.
    3.  Query the `job_audit` table and assert the presence and content of log entries.
        ```sql
        SELECT job_nr, job_kennung, script_name, stichtag, status, message
        FROM `project.dataset.job_audit`
        ORDER BY log_timestamp;
        ```
        **Expected Result (Python/Pytest assertion, assuming `execution_date` was `2024-06-15`):**
        ```python
        # This would typically be part of an Airflow integration test or manual verification
        # after triggering the DAG.
        results = bq_client.query("""
            SELECT job_nr, job_kennung, script_name, stichtag, status, message
            FROM `project.dataset.job_audit`
            ORDER BY log_timestamp
        """).result()
        rows = list(results)

        assert len(rows) == 3
        assert rows[0].job_nr == 1 and rows[0].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[0].status == 'STARTED' and rows[0].stichtag == '15062024' and 'Wiederanlaufwert: 0' in rows[0].message
        assert rows[1].job_nr == 1 and rows[1].script_name == 'k_ausd_bp_ta_iccid_vertrag' and rows[1].status == 'INVOKED' and rows[1].stichtag == '15062024' and 'wiederanlaufWert=0' in rows[1].message
        assert rows[2].job_nr == 1 and rows[2].script_name == 'ausd_bp_ta_iccid_vertrag_wrapper' and rows[2].status == 'OK' and rows[2].stichtag == '15062024'
        ```