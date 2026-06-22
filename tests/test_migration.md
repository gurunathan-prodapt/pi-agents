As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery code for `k_ausd_v_ta_apn_ve.ksh`. The migration aims to re-implement the orchestration logic and underlying SQL processing on BigQuery.

A critical observation is that the core data transformation logic from `d_ausd_v_ta_apn_ve.sql` is not provided, and its BigQuery equivalent (`d_ausd_v_ta_apn_ve_sp`) is a placeholder. For the purpose of these tests, I will define a **mock `d_ausd_v_ta_apn_ve_sp` and a mock `ta_apn_ve` table** to simulate predictable data changes and allow for testing the orchestration logic, error handling, and record counting.

Another significant point is the legacy script's stated purpose: "a) aktive Jobs werden ignoriert" and "c) alte aktive Jobs werden einfach dekativiert". This job control logic is **not explicitly implemented** in the provided BigQuery stored procedure `control_r_ausd_vertrag`. This represents a behavioral discrepancy that will be highlighted in a dedicated test case.

---

## Global Setup

Before running any tests, ensure the following BigQuery resources are created. These form the foundation for the migrated job.

1.  **BigQuery Project and Dataset:**
    *   Assume `project` and `dataset` are placeholders for your actual GCP project ID and BigQuery dataset name.
    *   Ensure the dataset exists.

2.  **Logging Tables DDL:**
    *   Execute the provided DDL for `error_log`, `job_log`, and `record_count_log`.

    ```sql
    -- project.dataset.error_log.sql
    CREATE TABLE IF NOT EXISTS `project.dataset.error_log`
    (
        error_source STRING NOT NULL,
        error_type STRING NOT NULL,
        error_number INT64 NOT NULL,
        error_argument STRING,
        created_at TIMESTAMP NOT NULL
    )
    OPTIONS(
        description="Logs error messages from BigQuery stored procedures."
    );

    -- project.dataset.job_log.sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_log`
    (
        job_kennung STRING NOT NULL,
        eintrags_nr STRING NOT NULL,
        tab_name STRING,
        status STRING NOT NULL,
        created_at TIMESTAMP NOT NULL
    )
    OPTIONS(
        description="Logs status and metadata for BigQuery stored procedure executions."
    );

    -- project.dataset.record_count_log.sql
    CREATE TABLE IF NOT EXISTS `project.dataset.record_count_log`
    (
        job_kennung STRING NOT NULL,
        eintrags_nr STRING NOT NULL,
        tab_name STRING,
        record_count INT64 NOT NULL,
        created_at TIMESTAMP NOT NULL
    )
    OPTIONS(
        description="Logs the number of records processed by BigQuery stored procedures."
    );
    ```

3.  **Mock Target Table `ta_apn_ve` DDL:**
    *   This table simulates the target table that `d_ausd_v_ta_apn_ve.sql` (and thus `d_ausd_v_ta_apn_ve_sp`) would modify.

    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.ta_apn_ve` (
        eintrags_nr STRING,
        job_kennung STRING,
        status STRING,
        value INT64,
        created_at TIMESTAMP
    );
    ```

4.  **Mock `d_ausd_v_ta_apn_ve_sp` Stored Procedure:**
    *   This mock procedure simulates the behavior of the original `d_ausd_v_ta_apn_ve.sql`. It will either insert a new record or update an existing one based on `eintrags_nr` and `job_kennung`. It also includes a mechanism to simulate failure for testing error handling.

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_apn_ve_sp`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING
    )
    BEGIN
      -- Simulate the core SQL logic: insert a new record or update an existing one.
      IF EXISTS (SELECT 1 FROM `project.dataset.ta_apn_ve` WHERE eintrags_nr = p_EintragsNr AND job_kennung = p_JobKennung) THEN
        UPDATE `project.dataset.ta_apn_ve`
        SET status = 'PROCESSED', value = value + 1, created_at = CURRENT_TIMESTAMP()
        WHERE eintrags_nr = p_EintragsNr AND job_kennung = p_JobKennung;
      ELSE
        INSERT INTO `project.dataset.ta_apn_ve` (eintrags_nr, job_kennung, status, value, created_at)
        VALUES (p_EintragsNr, p_JobKennung, 'NEW', 1, CURRENT_TIMESTAMP());
      END IF;

      -- Simulate a failure for specific test cases
      IF p_EintragsNr = 'FAIL_ME' THEN
        RAISE USING MESSAGE = 'Simulated failure in d_ausd_v_ta_apn_ve_sp';
      END IF;

    END;
    ```

5.  **`control_r_ausd_vertrag` Stored Procedure:**
    *   Execute the provided BigQuery SP code for `control_r_ausd_vertrag`.

    ```sql
    -- project.dataset.control_r_ausd_vertrag.sql (as provided in the prompt)
    CREATE OR REPLACE PROCEDURE `project.dataset.control_r_ausd_vertrag`(
      IN p_JobKennung STRING,
      IN p_EintragsNr STRING
    )
    BEGIN
      DECLARE ErrNr INT64 DEFAULT 0;
      DECLARE ErrArg STRING DEFAULT '';
      DECLARE v_TabName STRING DEFAULT 'ta_apn_ve'; -- Hardcoded table name from design
      DECLARE v_records INT64 DEFAULT 0;
      DECLARE error_message STRING;

      -- Parameter validation (replaces pruefeParameterGesetzt)
      IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET ErrNr = 193; -- Example error code
        SET ErrArg = 'Jobkennung';
      END IF;

      IF p_EintragsNr IS NULL OR p_EintragsNr = '' AND ErrNr = 0 THEN
        SET ErrNr = 193; -- Example error code
        SET ErrArg = 'EintragsNr';
      END IF;

      -- Error handling (replaces DWMSG_MeldeFehler and shell exit)
      IF ErrNr != 0 THEN
        SET error_message = CONCAT('FEHLER: 0 E ', CAST(ErrNr AS STRING), ' Parameter ', ErrArg, ' ist nicht gesetzt.');
        -- Log to a BigQuery error table
        INSERT INTO `project.dataset.error_log`
          (error_source, error_type, error_number, error_argument, created_at)
        VALUES
          ('control_r_ausd_vertrag', 'E', ErrNr, ErrArg, CURRENT_TIMESTAMP());
        RAISE USING MESSAGE = error_message;
      END IF;

      BEGIN
        -- Main SQL execution wrapper replacement (replaces starteSQLSkript)
        -- Calls the BigQuery Stored Procedure that encapsulates the converted d_ausd_v_ta_apn_ve.sql logic
        CALL `project.dataset.d_ausd_v_ta_apn_ve_sp`(
          p_EintragsNr,
          p_JobKennung
        );

        -- Log job completion
        INSERT INTO `project.dataset.job_log`
          (job_kennung, eintrags_nr, tab_name, status, created_at)
        VALUES
          (p_JobKennung, p_EintragsNr, v_TabName, 'ENDE', CURRENT_TIMESTAMP());

        -- Replace temp-file read with direct query result for record count
        -- This assumes `project.dataset.ta_apn_ve` is the target table where records are added/updated
        -- The actual filter criteria might need adjustment based on the original d_ausd_v_ta_apn_ve.sql logic.
        -- For this example, we assume `eintrags_nr` is a valid filter.
        EXECUTE IMMEDIATE FORMAT("""
          SELECT COUNT(*)
          FROM `%s.%s.ta_apn_ve` -- Target table in BQ
          WHERE eintrags_nr = '%s'
        """, @@project_id, @@dataset_id, p_EintragsNr) INTO v_records;


        -- Persist record count
        INSERT INTO `project.dataset.record_count_log`
          (job_kennung, eintrags_nr, tab_name, record_count, created_at)
        VALUES
          (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

      EXCEPTION WHEN ERROR THEN
        SET error_message = CONCAT('An error occurred during execution: ', @@error.message);
        -- Log any unhandled exceptions during the core SQL execution or record counting.
        INSERT INTO `project.dataset.error_log`
          (error_source, error_type, error_number, error_argument, created_at)
        VALUES
          ('control_r_ausd_vertrag', 'E', 9999, error_message, CURRENT_TIMESTAMP());
        RAISE USING MESSAGE = error_message;
      END;

    END;
    ```

---

## Test Cases

### Test Case 1: Successful Execution - Initial Data Transformation & Logging

**Purpose:** Verify that a successful execution of the migrated job correctly triggers the core data transformation, updates the target table, and logs the job status and processed record count. This covers output parity for data and basic logging.

**Setup:**
1.  Clear all logging tables and the mock `ta_apn_ve` table.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.record_count_log`;
    TRUNCATE TABLE `project.dataset.ta_apn_ve`;
    ```
2.  Define input parameters: `p_JobKennung = 'JOB_A'`, `p_EintragsNr = 'ENTRY_001'`.

**Action:**
1.  Execute the BigQuery stored procedure `control_r_ausd_vertrag` with the defined parameters.
    ```sql
    CALL `project.dataset.control_r_ausd_vertrag`('JOB_A', 'ENTRY_001');
    ```

**Pass/Fail Criterion:**
*   The call completes successfully without raising an error.
*   **`ta_apn_ve` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.ta_apn_ve` WHERE eintrags_nr = 'ENTRY_001' AND job_kennung = 'JOB_A';` returns `1`.
    *   `SELECT status, value FROM `project.dataset.ta_apn_ve` WHERE eintrags_nr = 'ENTRY_001' AND job_kennung = 'JOB_A';` returns `status = 'NEW'` and `value = 1`.
*   **`job_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_001' AND status = 'ENDE';` returns `1`.
*   **`record_count_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.record_count_log` WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_001' AND record_count = 1;` returns `1`.

### Test Case 2: Successful Execution - Subsequent Data Transformation & Record Count Update

**Purpose:** Verify that subsequent executions with the same `eintrags_nr` and `job_kennung` correctly update existing records and reflect the updated record count. This tests the `d_ausd_v_ta_apn_ve_sp`'s update logic and the `control_r_ausd_vertrag`'s record counting mechanism.

**Setup:**
1.  Ensure Test Case 1 has been run successfully.
2.  Define input parameters: `p_JobKennung = 'JOB_A'`, `p_EintragsNr = 'ENTRY_001'`.

**Action:**
1.  Execute the BigQuery stored procedure `control_r_ausd_vertrag` again with the same parameters.
    ```sql
    CALL `project.dataset.control_r_ausd_vertrag`('JOB_A', 'ENTRY_001');
    ```

**Pass/Fail Criterion:**
*   The call completes successfully without raising an error.
*   **`ta_apn_ve` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.ta_apn_ve` WHERE eintrags_nr = 'ENTRY_001' AND job_kennung = 'JOB_A';` still returns `1` (no new row, existing row updated).
    *   `SELECT status, value FROM `project.dataset.ta_apn_ve` WHERE eintrags_nr = 'ENTRY_001' AND job_kennung = 'JOB_A';` returns `status = 'PROCESSED'` and `value = 2`.
*   **`job_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_001' AND status = 'ENDE';` returns `2` (one for each execution).
*   **`record_count_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.record_count_log` WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_001' AND record_count = 1;` returns `2` (the count is based on `WHERE eintrags_nr = p_EintragsNr`, which still yields 1 row, but the log entry itself is new). This confirms the record count logic is based on the filter, not the number of rows *changed*.

### Test Case 3: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the migrated job correctly identifies and handles a missing `p_JobKennung` parameter, matching the legacy script's error reporting behavior.

**Setup:**
1.  Clear `error_log` table.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    ```
2.  Define input parameters: `p_JobKennung = NULL` (or empty string), `p_EintragsNr = 'ENTRY_002'`.

**Action:**
1.  Attempt to execute the BigQuery stored procedure `control_r_ausd_vertrag` with a missing `p_JobKennung`.
    ```sql
    -- Using NULL
    CALL `project.dataset.control_r_ausd_vertrag`(NULL, 'ENTRY_002');
    -- Or using empty string
    -- CALL `project.dataset.control_r_ausd_vertrag`('', 'ENTRY_002');
    ```

**Pass/Fail Criterion:**
*   The call **fails** and raises an error.
*   The error message contains: `FEHLER: 0 E 193 Parameter Jobkennung ist nicht gesetzt.` (or similar, matching the legacy `DWMSG_MeldeFehler` output).
*   **`error_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_source = 'control_r_ausd_vertrag' AND error_type = 'E' AND error_number = 193 AND error_argument = 'Jobkennung';` returns `1`.
*   **No `job_log` or `record_count_log` entries** are created for this execution.

### Test Case 4: Parameter Validation - Missing `p_EintragsNr`

**Purpose:** Verify that the migrated job correctly identifies and handles a missing `p_EintragsNr` parameter, matching the legacy script's error reporting behavior (specifically, that it reports the *first* missing parameter if multiple are missing, but here we test only `p_EintragsNr` missing).

**Setup:**
1.  Clear `error_log` table.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    ```
2.  Define input parameters: `p_JobKennung = 'JOB_B'`, `p_EintragsNr = NULL` (or empty string).

**Action:**
1.  Attempt to execute the BigQuery stored procedure `control_r_ausd_vertrag` with a missing `p_EintragsNr`.
    ```sql
    -- Using NULL
    CALL `project.dataset.control_r_ausd_vertrag`('JOB_B', NULL);
    -- Or using empty string
    -- CALL `project.dataset.control_r_ausd_vertrag`('JOB_B', '');
    ```

**Pass/Fail Criterion:**
*   The call **fails** and raises an error.
*   The error message contains: `FEHLER: 0 E 193 Parameter EintragsNr ist nicht gesetzt.`
*   **`error_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_source = 'control_r_ausd_vertrag' AND error_type = 'E' AND error_number = 193 AND error_argument = 'EintragsNr';` returns `1`.
*   **No `job_log` or `record_count_log` entries** are created for this execution.

### Test Case 5: Parameter Validation - Both `p_JobKennung` and `p_EintragsNr` Missing

**Purpose:** Verify that when both parameters are missing, the migrated job reports only the *first* missing parameter, mirroring the sequential validation logic of the legacy `pruefeParameterGesetzt` function.

**Setup:**
1.  Clear `error_log` table.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    ```
2.  Define input parameters: `p_JobKennung = NULL`, `p_EintragsNr = NULL`.

**Action:**
1.  Attempt to execute the BigQuery stored procedure `control_r_ausd_vertrag` with both parameters missing.
    ```sql
    CALL `project.dataset.control_r_ausd_vertrag`(NULL, NULL);
    ```

**Pass/Fail Criterion:**
*   The call **fails** and raises an error.
*   The error message contains: `FEHLER: 0 E 193 Parameter Jobkennung ist nicht gesetzt.` (It should report `Jobkennung` first, as per the `AND ErrNr = 0` logic in the BQ SP).
*   **`error_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_source = 'control_r_ausd_vertrag' AND error_type = 'E' AND error_number = 193 AND error_argument = 'Jobkennung';` returns `1`.
    *   `SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_argument = 'EintragsNr';` returns `0`.
*   **No `job_log` or `record_count_log` entries** are created for this execution.

### Test Case 6: Core SQL SP Failure - Error Handling

**Purpose:** Verify that if the underlying data transformation stored procedure (`d_ausd_v_ta_apn_ve_sp`) fails, the orchestrating `control_r_ausd_vertrag` catches the error, logs it, and propagates a meaningful error message.

**Setup:**
1.  Clear all logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.record_count_log`;
    ```
2.  Define input parameters: `p_JobKennung = 'JOB_FAIL'`, `p_EintragsNr = 'FAIL_ME'`. (The mock `d_ausd_v_ta_apn_ve_sp` is designed to fail for `p_EintragsNr = 'FAIL_ME'`).

**Action:**
1.  Execute the BigQuery stored procedure `control_r_ausd_vertrag` with parameters designed to trigger a failure in `d_ausd_v_ta_apn_ve_sp`.
    ```sql
    CALL `project.dataset.control_r_ausd_vertrag`('JOB_FAIL', 'FAIL_ME');
    ```

**Pass/Fail Criterion:**
*   The call **fails** and raises an error.
*   The error message contains: `An error occurred during execution: Simulated failure in d_ausd_v_ta_apn_ve_sp`.
*   **`error_log` content:**
    *   `SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_source = 'control_r_ausd_vertrag' AND error_type = 'E' AND error_number = 9999 AND error_argument LIKE '%Simulated failure in d_ausd_v_ta_apn_ve_sp%';` returns `1`.
*   **No `job_log` or `record_count_log` entries** are created for this specific execution (as the `CALL` failed before these steps).

### Test Case 7: Schema Assertions for Logging Tables

**Purpose:** Verify that the DDL for the logging tables (`error_log`, `job_log`, `record_count_log`) has been correctly applied and matches the expected schema. This is a data quality/schema assertion.

**Setup:**
1.  Ensure the logging tables have been created as part of the global setup.

**Action:**
1.  Query the BigQuery `INFORMATION_SCHEMA` for table and column details.

**Pass/Fail Criterion:**
*   **`error_log` schema:**
    ```python
    # Example using BigQuery client library (pytest-bigquery)
    def test_error_log_schema(bq_client):
        table_ref = bq_client.dataset('dataset').table('error_log')
        table = bq_client.get_table(table_ref)
        expected_schema = {
            'error_source': ('STRING', 'REQUIRED'),
            'error_type': ('STRING', 'REQUIRED'),
            'error_number': ('INT64', 'REQUIRED'),
            'error_argument': ('STRING', 'NULLABLE'),
            'created_at': ('TIMESTAMP', 'REQUIRED'),
        }
        for field in table.schema:
            assert field.name in expected_schema
            assert field.field_type == expected_schema[field.name][0]
            assert field.mode == expected_schema[field.name][1]
        assert len(table.schema) == len(expected_schema)
    ```
*   **`job_log` schema:** Similar assertion for `job_log` table.
    *   Expected columns: `job_kennung` (STRING, REQUIRED), `eintrags_nr` (STRING, REQUIRED), `tab_name` (STRING, NULLABLE), `status` (STRING, REQUIRED), `created_at` (TIMESTAMP, REQUIRED).
*   **`record_count_log` schema:** Similar assertion for `record_count_log` table.
    *   Expected columns: `job_kennung` (STRING, REQUIRED), `eintrags_nr` (STRING, REQUIRED), `tab_name` (STRING, NULLABLE), `record_count` (INT64, REQUIRED), `created_at` (TIMESTAMP, REQUIRED).

### Test Case 8: Behavioral Discrepancy - Job Control Logic (Active/Deactivate Jobs)

**Purpose:** Highlight that the migrated `control_r_ausd_vertrag` stored procedure does *not* implement the "ignoring already active jobs" and "deactivating old active jobs" logic explicitly mentioned in the legacy script's purpose. This is a critical behavioral difference that needs to be acknowledged and potentially addressed.

**Setup:**
1.  No specific setup required beyond the global setup. This is a conceptual test.

**Action:**
1.  Review the BigQuery stored procedure `project.dataset.control_r_ausd_vertrag`.
2.  Compare its logic against the legacy script's stated purpose:
    *   "a) aktive Jobs werden ignoriert"
    *   "c) alte aktive Jobs werden einfach dekativiert"

**Pass/Fail Criterion:**
*   **Fail (as a discrepancy):** The `control_r_ausd_vertrag` stored procedure **does not contain any explicit logic** to check for "active jobs" in a job control table, nor does it contain logic to "deactivate old active jobs." It proceeds directly to parameter validation and then calls `d_ausd_v_ta_apn_ve_sp`.
*   **Recommendation:** This behavior needs to be explicitly confirmed with stakeholders. If this job control logic is still required, it must be added to `control_r_ausd_vertrag` (e.g., by querying a `job_status` table before calling `d_ausd_v_ta_apn_ve_sp` and potentially updating it). If it's no longer needed, this should be documented as an intentional change in behavior.

---

These tests cover the main aspects of the migration, focusing on parameter handling, error reporting, data transformation orchestration, record counting, and logging. The mock `d_ausd_v_ta_apn_ve_sp` allows for robust testing of the `control_r_ausd_vertrag`'s behavior even without the full details of the original SQL script. The identified behavioral discrepancy regarding job control is crucial for a complete migration assessment.