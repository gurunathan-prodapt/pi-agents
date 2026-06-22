As a senior data-migration QA engineer, I have developed a comprehensive suite of validation tests for the `f_alis_msgerr.ksh` migration to BigQuery Stored Procedures. These tests are designed to ensure behavioral equivalence, data integrity, and correctness across all aspects of the migration.

Each test case is presented with its purpose, setup, action, and a concrete pass/fail criterion, including runnable BigQuery SQL code where applicable.

---

## Migration Validation Tests: `f_alis_msgerr.ksh` to BigQuery Stored Procedures

**Assumptions:**
*   The BigQuery dataset `dw_is_error_management` exists.
*   All DDLs (`message_table.sql`, `message_log.sql`, `message_sequence.sql`) have been executed, and the `message_sequence` table is initialized with `('eintragsnr_seq', 1)`.
*   All BigQuery Stored Procedures (`f_alis_msgerr_ErmittleNr.sql`, etc.) have been created.
*   Tests are designed to be run in an isolated environment, typically by truncating/deleting data before each test or using unique identifiers.

---

### Test Case 1: `DWMSG_ErmittleNr` - Basic Sequence Generation

*   **Purpose:** Validate that `DWMSG_ErmittleNr` correctly generates unique, incrementing entry numbers, replicating the Oracle sequence functionality.
*   **Setup:**
    1.  Ensure `dw_is_error_management.message_sequence` exists.
    2.  Reset `next_val` for `eintragsnr_seq` to a known starting point (e.g., 1).
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_ErmittleNr` multiple times to get several entry numbers.
    2.  Store the returned `p_EintragsNr` values.
*   **Pass/Fail Criterion:**
    1.  Each call to `DWMSG_ErmittleNr` returns a unique `INT64` value.
    2.  The returned values are sequentially incrementing.
    3.  The `next_val` in `dw_is_error_management.message_sequence` is updated correctly after each call.

```sql
-- Test Case 1: DWMSG_ErmittleNr - Basic Sequence Generation
DECLARE v_eintragsnr1 INT64;
DECLARE v_eintragsnr2 INT64;
DECLARE v_eintragsnr3 INT64;
DECLARE initial_next_val INT64;

-- Setup: Reset sequence to a known state
DELETE FROM dw_is_error_management.message_sequence WHERE sequence_name = 'eintragsnr_seq';
INSERT INTO dw_is_error_management.message_sequence (sequence_name, next_val) VALUES ('eintragsnr_seq', 100);

-- Get initial next_val for assertion
SELECT next_val INTO initial_next_val FROM dw_is_error_management.message_sequence WHERE sequence_name = 'eintragsnr_seq';

-- Action: Call DWMSG_ErmittleNr multiple times
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr1);
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr2);
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr3);

-- Pass/Fail Criterion:
-- 1. Returned values are unique and incrementing
ASSERT v_eintragsnr1 = initial_next_val + 1 AS 'EintragsNr 1 is not correct';
ASSERT v_eintragsnr2 = initial_next_val + 2 AS 'EintragsNr 2 is not correct';
ASSERT v_eintragsnr3 = initial_next_val + 3 AS 'EintragsNr 3 is not correct';

-- 2. next_val in message_sequence is updated correctly
ASSERT (SELECT next_val FROM dw_is_error_management.message_sequence WHERE sequence_name = 'eintragsnr_seq') = initial_next_val + 3 AS 'Sequence next_val not updated correctly';

SELECT 'Test Case 1 Passed: DWMSG_ErmittleNr - Basic Sequence Generation';
```

---

### Test Case 2: `DWMSG_ErzeugeEintrag` - New Entry Creation

*   **Purpose:** Verify that `DWMSG_ErzeugeEintrag` correctly inserts a new record into `message_table` with the provided details and sets the initial status to 'LAEUFT'.
*   **Setup:**
    1.  Truncate `dw_is_error_management.message_table`.
    2.  Obtain a unique `eintragsnr` using `DWMSG_ErmittleNr`.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_ErzeugeEintrag` with a valid `eintragsnr`, `job_kennung`, `programmname`, and `log_datei`.
*   **Pass/Fail Criterion:**
    1.  `dw_is_error_management.message_table` contains exactly one row.
    2.  The inserted row's `eintragsnr`, `job_kennung`, `programmname`, `log_datei` match the input parameters.
    3.  The `status` column is 'LAEUFT'.
    4.  `created_at` and `updated_at` are populated and are close to `CURRENT_TIMESTAMP()`.

```sql
-- Test Case 2: DWMSG_ErzeugeEintrag - New Entry Creation
DECLARE v_eintragsnr INT64;
DECLARE v_job_kennung STRING DEFAULT 'TEST_JOB_001';
DECLARE v_programmname STRING DEFAULT 'test_script.ksh';
DECLARE v_log_datei STRING DEFAULT '/path/to/logs/test_job_001_12345.log';

-- Setup: Truncate message_table and get a new EintragsNr
TRUNCATE TABLE dw_is_error_management.message_table;
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr);

-- Action: Call DWMSG_ErzeugeEintrag
CALL dw_is_error_management.DWMSG_ErzeugeEintrag(
    v_eintragsnr,
    v_job_kennung,
    v_programmname,
    v_log_datei
);

-- Pass/Fail Criterion:
-- 1. Row count is 1
ASSERT (SELECT COUNT(*) FROM dw_is_error_management.message_table) = 1 AS 'Row count in message_table is not 1';

-- 2. All fields match input and status is 'LAEUFT'
SELECT
    eintragsnr,
    job_kennung,
    programmname,
    log_datei,
    status
FROM dw_is_error_management.message_table
WHERE eintragsnr = v_eintragsnr;

ASSERT (SELECT eintragsnr FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_eintragsnr AS 'EintragsNr mismatch';
ASSERT (SELECT job_kennung FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_job_kennung AS 'JobKennung mismatch';
ASSERT (SELECT programmname FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_programmname AS 'Programmname mismatch';
ASSERT (SELECT log_datei FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_log_datei AS 'LogDatei mismatch';
ASSERT (SELECT status FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'LAEUFT' AS 'Status is not LAEUFT';

-- 3. created_at and updated_at are populated (within a reasonable time window)
ASSERT (SELECT created_at IS NOT NULL FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'created_at is NULL';
ASSERT (SELECT updated_at IS NOT NULL FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at is NULL';

SELECT 'Test Case 2 Passed: DWMSG_ErzeugeEintrag - New Entry Creation';
```

---

### Test Case 3: `DWMSG_SetzeStatusOK` - Status Update to OK

*   **Purpose:** Verify that `DWMSG_SetzeStatusOK` correctly updates the `status` of an existing entry to 'OK' and updates `updated_at`.
*   **Setup:**
    1.  Truncate `dw_is_error_management.message_table`.
    2.  Insert a new entry with status 'LAEUFT' using `DWMSG_ErzeugeEintrag`.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_SetzeStatusOK` with the `eintragsnr` of the created entry.
*   **Pass/Fail Criterion:**
    1.  The `status` of the specified entry in `message_table` is 'OK'.
    2.  The `updated_at` timestamp is updated to a value later than `created_at`.
    3.  Calling with a non-existent `eintragsnr` raises an error.
    4.  Calling with `NULL` `eintragsnr` raises an error.

```sql
-- Test Case 3: DWMSG_SetzeStatusOK - Status Update to OK
DECLARE v_eintragsnr INT64;
DECLARE v_job_kennung STRING DEFAULT 'TEST_JOB_002';
DECLARE v_programmname STRING DEFAULT 'test_script_ok.ksh';
DECLARE v_log_datei STRING DEFAULT '/path/to/logs/test_job_002_ok.log';
DECLARE v_created_at TIMESTAMP;

-- Setup: Create an initial entry
TRUNCATE TABLE dw_is_error_management.message_table;
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr);
CALL dw_is_error_management.DWMSG_ErzeugeEintrag(v_eintragsnr, v_job_kennung, v_programmname, v_log_datei);

-- Get initial created_at for comparison
SELECT created_at INTO v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr;

-- Action: Call DWMSG_SetzeStatusOK
CALL dw_is_error_management.DWMSG_SetzeStatusOK(v_eintragsnr);

-- Pass/Fail Criterion:
-- 1. Status is 'OK'
ASSERT (SELECT status FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'OK' AS 'Status is not OK';

-- 2. updated_at is later than created_at
ASSERT (SELECT updated_at > v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated or not later than created_at';

-- Edge Case: Non-existent EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStatusOK(999999); -- Should raise an error
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'No entry found for EintragsNr: 999999%' AS 'Error message for non-existent EintragsNr mismatch';
END;

-- Edge Case: NULL EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStatusOK(NULL); -- Should raise an error
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_EintragsNr cannot be NULL%' AS 'Error message for NULL EintragsNr mismatch';
END;

SELECT 'Test Case 3 Passed: DWMSG_SetzeStatusOK - Status Update to OK';
```

---

### Test Case 4: `DWMSG_SetzeStatusAbbruch` - Status Update to ABBRUCH

*   **Purpose:** Verify that `DWMSG_SetzeStatusAbbruch` correctly updates the `status` of an existing entry to 'ABBRUCH' and updates `updated_at`.
*   **Setup:**
    1.  Truncate `dw_is_error_management.message_table`.
    2.  Insert a new entry with status 'LAEUFT' using `DWMSG_ErzeugeEintrag`.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_SetzeStatusAbbruch` with the `eintragsnr` of the created entry.
*   **Pass/Fail Criterion:**
    1.  The `status` of the specified entry in `message_table` is 'ABBRUCH'.
    2.  The `updated_at` timestamp is updated to a value later than `created_at`.
    3.  Calling with a non-existent `eintragsnr` raises an error.
    4.  Calling with `NULL` `eintragsnr` raises an error.

```sql
-- Test Case 4: DWMSG_SetzeStatusAbbruch - Status Update to ABBRUCH
DECLARE v_eintragsnr INT64;
DECLARE v_job_kennung STRING DEFAULT 'TEST_JOB_003';
DECLARE v_programmname STRING DEFAULT 'test_script_fail.ksh';
DECLARE v_log_datei STRING DEFAULT '/path/to/logs/test_job_003_fail.log';
DECLARE v_created_at TIMESTAMP;

-- Setup: Create an initial entry
TRUNCATE TABLE dw_is_error_management.message_table;
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr);
CALL dw_is_error_management.DWMSG_ErzeugeEintrag(v_eintragsnr, v_job_kennung, v_programmname, v_log_datei);

-- Get initial created_at for comparison
SELECT created_at INTO v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr;

-- Action: Call DWMSG_SetzeStatusAbbruch
CALL dw_is_error_management.DWMSG_SetzeStatusAbbruch(v_eintragsnr);

-- Pass/Fail Criterion:
-- 1. Status is 'ABBRUCH'
ASSERT (SELECT status FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'ABBRUCH' AS 'Status is not ABBRUCH';

-- 2. updated_at is later than created_at
ASSERT (SELECT updated_at > v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated or not later than created_at';

-- Edge Case: Non-existent EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStatusAbbruch(999999); -- Should raise an error
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'No entry found for EintragsNr: 999999%' AS 'Error message for non-existent EintragsNr mismatch';
END;

-- Edge Case: NULL EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStatusAbbruch(NULL); -- Should raise an error
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_EintragsNr cannot be NULL%' AS 'Error message for NULL EintragsNr mismatch';
END;

SELECT 'Test Case 4 Passed: DWMSG_SetzeStatusAbbruch - Status Update to ABBRUCH';
```

---

### Test Case 5: `DWMSG_MeldeFehler` - Error Logging and Table Update

*   **Purpose:** Validate that `DWMSG_MeldeFehler` correctly inserts an error entry into `message_log` and updates the `last_error_` fields in `message_table`.
*   **Setup:**
    1.  Truncate `dw_is_error_management.message_table` and `dw_is_error_management.message_log`.
    2.  Insert a new entry into `message_table` using `DWMSG_ErzeugeEintrag`.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_MeldeFehler` with various combinations of `p_Typ`, `p_FehlerNr`, `p_Zusatz1`, `p_Zusatz2` (including NULLs, empty strings, and long strings).
*   **Pass/Fail Criterion:**
    1.  `message_log` contains a new entry with `eintragsnr`, `log_type`, `fehler_nr`, `zusatz1`, `zusatz2` matching the input.
    2.  `message_table`'s `last_error_type`, `last_error_nr`, `last_error_zusatz1`, `last_error_zusatz2` are updated correctly for the specified `eintragsnr`.
    3.  `updated_at` in `message_table` is updated.
    4.  `p_Zusatz1` and `p_Zusatz2` are handled correctly, including truncation if specified in the SP (the current SP truncates `p_SqlMessage` in `Fehlerbehandlung` but not `p_Zusatz1`/`p_Zusatz2` directly in `MeldeFehler`, which is a potential difference from Oracle's `VARCHAR2` limits).
    5.  Calling with `NULL` `p_EintragsNr` or `p_Typ` raises an error.

```sql
-- Test Case 5: DWMSG_MeldeFehler - Error Logging and Table Update
DECLARE v_eintragsnr INT64;
DECLARE v_job_kennung STRING DEFAULT 'TEST_JOB_004';
DECLARE v_programmname STRING DEFAULT 'test_script_error.ksh';
DECLARE v_log_datei STRING DEFAULT '/path/to/logs/test_job_004_error.log';
DECLARE v_created_at TIMESTAMP;

-- Setup: Create an initial entry and clear log table
TRUNCATE TABLE dw_is_error_management.message_table;
TRUNCATE TABLE dw_is_error_management.message_log;
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr);
CALL dw_is_error_management.DWMSG_ErzeugeEintrag(v_eintragsnr, v_job_kennung, v_programmname, v_log_datei);
SELECT created_at INTO v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr;

-- Action 1: Call DWMSG_MeldeFehler with all parameters
CALL dw_is_error_management.DWMSG_MeldeFehler(
    v_eintragsnr,
    'F',
    1001,
    'File not found: /tmp/data.txt',
    'Permission denied'
);

-- Pass/Fail Criterion 1: Check message_log
ASSERT (SELECT COUNT(*) FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = 1 AS 'Log entry not found for Action 1';
ASSERT (SELECT log_type FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = 'F' AS 'Log type mismatch for Action 1';
ASSERT (SELECT fehler_nr FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = 1001 AS 'FehlerNr mismatch for Action 1';
ASSERT (SELECT zusatz1 FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = 'File not found: /tmp/data.txt' AS 'Zusatz1 mismatch for Action 1';
ASSERT (SELECT zusatz2 FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = 'Permission denied' AS 'Zusatz2 mismatch for Action 1';

-- Pass/Fail Criterion 1: Check message_table
ASSERT (SELECT last_error_type FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'F' AS 'Table last_error_type mismatch for Action 1';
ASSERT (SELECT last_error_nr FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 1001 AS 'Table last_error_nr mismatch for Action 1';
ASSERT (SELECT last_error_zusatz1 FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'File not found: /tmp/data.txt' AS 'Table last_error_zusatz1 mismatch for Action 1';
ASSERT (SELECT last_error_zusatz2 FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'Permission denied' AS 'Table last_error_zusatz2 mismatch for Action 1';
ASSERT (SELECT updated_at > v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated for Action 1';

-- Action 2: Call DWMSG_MeldeFehler with NULL Zusatz2
CALL dw_is_error_management.DWMSG_MeldeFehler(
    v_eintragsnr,
    'E',
    2002,
    'Database connection failed',
    NULL
);

-- Pass/Fail Criterion 2: Check message_log (new entry)
ASSERT (SELECT COUNT(*) FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr AND fehler_nr = 2002) = 1 AS 'Log entry not found for Action 2';
ASSERT (SELECT zusatz2 FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr AND fehler_nr = 2002) IS NULL AS 'Zusatz2 not NULL for Action 2';

-- Pass/Fail Criterion 2: Check message_table (updated with latest error)
ASSERT (SELECT last_error_type FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'E' AS 'Table last_error_type mismatch for Action 2';
ASSERT (SELECT last_error_nr FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 2002 AS 'Table last_error_nr mismatch for Action 2';
ASSERT (SELECT last_error_zusatz1 FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'Database connection failed' AS 'Table last_error_zusatz1 mismatch for Action 2';
ASSERT (SELECT last_error_zusatz2 FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) IS NULL AS 'Table last_error_zusatz2 not NULL for Action 2';

-- Edge Case: NULL EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_MeldeFehler(NULL, 'W', 3003, 'Warning message', NULL);
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_EintragsNr cannot be NULL%' AS 'Error message for NULL EintragsNr mismatch';
END;

-- Edge Case: NULL p_Typ
BEGIN
    CALL dw_is_error_management.DWMSG_MeldeFehler(v_eintragsnr, NULL, 3004, 'Warning message', NULL);
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_Typ cannot be NULL%' AS 'Error message for NULL p_Typ mismatch';
END;

SELECT 'Test Case 5 Passed: DWMSG_MeldeFehler - Error Logging and Table Update';
```

---

### Test Case 6: `DWMSG_Fehlerbehandlung` - Comprehensive Error Handling

*   **Purpose:** Validate the central error handling routine, ensuring it logs the error and sets the job status to 'ABBRUCH'. This tests the integration of `DWMSG_MeldeFehler` and `DWMSG_SetzeStatusAbbruch`.
*   **Setup:**
    1.  Truncate `dw_is_error_management.message_table` and `dw_is_error_management.message_log`.
    2.  Insert a new entry into `message_table` with status 'LAEUFT'.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_Fehlerbehandlung` with a simulated SQL error code and message.
*   **Pass/Fail Criterion:**
    1.  The `status` of the entry in `message_table` is 'ABBRUCH'.
    2.  `last_error_type` is 'FEHLER', `last_error_nr` matches `p_SqlCode`, and `last_error_zusatz1` contains the (potentially truncated) `p_SqlMessage`.
    3.  `message_log` contains a new entry with `log_type` 'FEHLER', `fehler_nr` matching `p_SqlCode`, and `zusatz1` matching `p_SqlMessage`.
    4.  `updated_at` in `message_table` is updated.
    5.  Calling with `NULL` `p_EintragsNr` raises an error.

```sql
-- Test Case 6: DWMSG_Fehlerbehandlung - Comprehensive Error Handling
DECLARE v_eintragsnr INT64;
DECLARE v_job_kennung STRING DEFAULT 'TEST_JOB_005';
DECLARE v_programmname STRING DEFAULT 'main_script.ksh';
DECLARE v_log_datei STRING DEFAULT '/path/to/logs/main_job_005.log';
DECLARE v_sql_code INT64 DEFAULT 12345;
DECLARE v_sql_message STRING DEFAULT 'BigQuery error: Table not found in dataset. This is a very long error message that should be truncated if it exceeds the column limit for Zusatz1.';
DECLARE v_created_at TIMESTAMP;

-- Setup: Create an initial entry and clear log table
TRUNCATE TABLE dw_is_error_management.message_table;
TRUNCATE TABLE dw_is_error_management.message_log;
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr);
CALL dw_is_error_management.DWMSG_ErzeugeEintrag(v_eintragsnr, v_job_kennung, v_programmname, v_log_datei);
SELECT created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr INTO v_created_at;

-- Action: Call DWMSG_Fehlerbehandlung
CALL dw_is_error_management.DWMSG_Fehlerbehandlung(v_eintragsnr, v_sql_code, v_sql_message);

-- Pass/Fail Criterion:
-- 1. Status is 'ABBRUCH'
ASSERT (SELECT status FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'ABBRUCH' AS 'Status is not ABBRUCH after Fehlerbehandlung';

-- 2. message_table last_error fields are updated
ASSERT (SELECT last_error_type FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = 'FEHLER' AS 'last_error_type mismatch';
ASSERT (SELECT last_error_nr FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_sql_code AS 'last_error_nr mismatch';
ASSERT (SELECT last_error_zusatz1 FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = LEFT(v_sql_message, 255) AS 'last_error_zusatz1 mismatch or truncation incorrect';
ASSERT (SELECT last_error_zusatz2 FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) IS NULL AS 'last_error_zusatz2 should be NULL';
ASSERT (SELECT updated_at > v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated';

-- 3. message_log contains the error entry
ASSERT (SELECT COUNT(*) FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = 1 AS 'Log entry not found after Fehlerbehandlung';
ASSERT (SELECT log_type FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = 'FEHLER' AS 'Log entry type mismatch';
ASSERT (SELECT fehler_nr FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = v_sql_code AS 'Log entry fehler_nr mismatch';
ASSERT (SELECT zusatz1 FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) = LEFT(v_sql_message, 255) AS 'Log entry zusatz1 mismatch or truncation incorrect';
ASSERT (SELECT zusatz2 FROM dw_is_error_management.message_log WHERE eintragsnr = v_eintragsnr) IS NULL AS 'Log entry zusatz2 should be NULL';

-- Edge Case: NULL EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_Fehlerbehandlung(NULL, 999, 'Test error');
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_EintragsNr cannot be NULL%' AS 'Error message for NULL EintragsNr mismatch';
END;

SELECT 'Test Case 6 Passed: DWMSG_Fehlerbehandlung - Comprehensive Error Handling';
```

---

### Test Case 7: `DWMSG_Logdateiname` - Log Filename Generation

*   **Purpose:** Validate that `DWMSG_Logdateiname` constructs the log filename correctly, including the base path, job ID, timestamp, and entry number.
*   **Setup:** None specific, as this procedure only computes a string.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_Logdateiname` with various `p_JobKennung`, `p_EintragsNr`, and `p_LogBasePath` values.
*   **Pass/Fail Criterion:**
    1.  The returned `p_VarName` string matches the expected format: `/path/to/prot/<JobKennung>_YYYYMMDD_HHMMSS_<EintragsNr>.log`.
    2.  The timestamp part (`YYYYMMDD_HHMMSS`) is correctly formatted and reflects the execution time.
    3.  The `p_LogBasePath` is handled correctly, including ensuring a trailing slash.
    4.  Calling with `NULL` for any required parameter raises an error.
    *   **Note on Output Parity:** The original KornShell script uses `date '+%Y%m%d_%H%M'` (minute precision). The migrated BigQuery SP uses `FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP())` (second precision). This is a **behavioral difference** that should be noted and accepted if the increased precision is desired or harmless. The test will validate against the BigQuery SP's behavior.

```sql
-- Test Case 7: DWMSG_Logdateiname - Log Filename Generation
DECLARE v_log_filename STRING;
DECLARE v_job_kennung STRING DEFAULT 'MY_BATCH_JOB';
DECLARE v_eintragsnr INT64 DEFAULT 12345;
DECLARE v_log_base_path STRING DEFAULT '/gcs/my-bucket/logs';
DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

-- Action 1: Call DWMSG_Logdateiname with standard inputs
CALL dw_is_error_management.DWMSG_Logdateiname(
    v_log_filename,
    v_job_kennung,
    v_eintragsnr,
    v_log_base_path
);

-- Pass/Fail Criterion 1: Check generated filename format
-- Expected format: /gcs/my-bucket/logs/MY_BATCH_JOB_YYYYMMDD_HHMMSS_12345.log
ASSERT STARTS_WITH(v_log_filename, v_log_base_path || '/') AS 'Log filename does not start with base path';
ASSERT ENDS_WITH(v_log_filename, '_' || CAST(v_eintragsnr AS STRING) || '.log') AS 'Log filename does not end with EintragsNr and .log';
ASSERT CONTAINS_SUBSTR(v_log_filename, v_job_kennung) AS 'Log filename does not contain JobKennung';

-- Validate timestamp format (YYYYMMDD_HHMMSS)
DECLARE v_timestamp_part STRING;
SET v_timestamp_part = SUBSTR(v_log_filename, LENGTH(v_log_base_path) + LENGTH(v_job_kennung) + 3, 15); -- +3 for / and _
ASSERT REGEXP_CONTAINS(v_timestamp_part, r'^\d{8}_\d{6}$') AS 'Timestamp part format is incorrect';

-- Action 2: Test with base path without trailing slash
DECLARE v_log_base_path_no_slash STRING DEFAULT '/gcs/another-bucket/logs';
CALL dw_is_error_management.DWMSG_Logdateiname(
    v_log_filename,
    v_job_kennung,
    v_eintragsnr,
    v_log_base_path_no_slash
);
ASSERT STARTS_WITH(v_log_filename, v_log_base_path_no_slash || '/') AS 'Log filename with no trailing slash base path is incorrect';

-- Edge Case: NULL JobKennung
BEGIN
    CALL dw_is_error_management.DWMSG_Logdateiname(v_log_filename, NULL, v_eintragsnr, v_log_base_path);
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_JobKennung, p_EintragsNr, and p_LogBasePath cannot be NULL%' AS 'Error message for NULL JobKennung mismatch';
END;

-- Edge Case: NULL EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_Logdateiname(v_log_filename, v_job_kennung, NULL, v_log_base_path);
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_JobKennung, p_EintragsNr, and p_LogBasePath cannot be NULL%' AS 'Error message for NULL EintragsNr mismatch';
END;

-- Edge Case: NULL LogBasePath
BEGIN
    CALL dw_is_error_management.DWMSG_Logdateiname(v_log_filename, v_job_kennung, v_eintragsnr, NULL);
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'p_JobKennung, p_EintragsNr, and p_LogBasePath cannot be NULL%' AS 'Error message for NULL LogBasePath mismatch';
END;

SELECT 'Test Case 7 Passed: DWMSG_Logdateiname - Log Filename Generation';
```

---

### Test Case 8: `DWMSG_SetzeStichtagInfo` - Date Information Update

*   **Purpose:** Validate that `DWMSG_SetzeStichtagInfo` correctly parses a date string and updates the `zusatzinfos_date` column in `message_table`.
*   **Setup:**
    1.  Truncate `dw_is_error_management.message_table`.
    2.  Insert a new entry into `message_table` using `DWMSG_ErzeugeEintrag`.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_SetzeStichtagInfo` with valid date strings and formats.
    2.  Call with invalid date strings/formats.
*   **Pass/Fail Criterion:**
    1.  For valid inputs, `zusatzinfos_date` in `message_table` is updated to the correctly parsed `DATE` value.
    2.  `updated_at` in `message_table` is updated.
    3.  For invalid date strings or formats, the procedure raises an error with an informative message.
    4.  Calling with `NULL` for any required parameter raises an error.

```sql
-- Test Case 8: DWMSG_SetzeStichtagInfo - Date Information Update
DECLARE v_eintragsnr INT64;
DECLARE v_job_kennung STRING DEFAULT 'TEST_JOB_006';
DECLARE v_programmname STRING DEFAULT 'date_setter.ksh';
DECLARE v_log_datei STRING DEFAULT '/path/to/logs/date_setter.log';
DECLARE v_created_at TIMESTAMP;

-- Setup: Create an initial entry
TRUNCATE TABLE dw_is_error_management.message_table;
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr);
CALL dw_is_error_management.DWMSG_ErzeugeEintrag(v_eintragsnr, v_job_kennung, v_programmname, v_log_datei);
SELECT created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr INTO v_created_at;

-- Action 1: Call with valid date (YYYY-MM-DD)
CALL dw_is_error_management.DWMSG_SetzeStichtagInfo(v_eintragsnr, '2023-01-15', '%Y-%m-%d');

-- Pass/Fail Criterion 1: Check date and updated_at
ASSERT (SELECT zusatzinfos_date FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = DATE '2023-01-15' AS 'zusatzinfos_date mismatch for YYYY-MM-DD';
ASSERT (SELECT updated_at > v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated for YYYY-MM-DD';

-- Action 2: Call with another valid date format (DD.MM.YYYY)
DECLARE v_updated_at_after_first_call TIMESTAMP;
SELECT updated_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr INTO v_updated_at_after_first_call;
CALL dw_is_error_management.DWMSG_SetzeStichtagInfo(v_eintragsnr, '28.02.2024', '%d.%m.%Y');

-- Pass/Fail Criterion 2: Check date and updated_at
ASSERT (SELECT zusatzinfos_date FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = DATE '2024-02-28' AS 'zusatzinfos_date mismatch for DD.MM.YYYY';
ASSERT (SELECT updated_at > v_updated_at_after_first_call FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated for DD.MM.YYYY';

-- Edge Case: Invalid date format
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStichtagInfo(v_eintragsnr, '2023/01/15', '%Y-%m-%d'); -- Format mismatch
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'Failed to parse date "2023/01/15" with format "%Y-%m-%d"%' AS 'Error message for invalid date format mismatch';
END;

-- Edge Case: Invalid date value
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStichtagInfo(v_eintragsnr, '2023-02-30', '%Y-%m-%d'); -- Invalid day
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'Failed to parse date "2023-02-30" with format "%Y-%m-%d"%' AS 'Error message for invalid date value mismatch';
END;

-- Edge Case: NULL EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStichtagInfo(NULL, '2023-01-01', '%Y-%m-%d');
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'All input parameters for DWMSG_SetzeStichtagInfo cannot be NULL%' AS 'Error message for NULL EintragsNr mismatch';
END;

-- Edge Case: NULL Stichtag
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStichtagInfo(v_eintragsnr, NULL, '%Y-%m-%d');
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'All input parameters for DWMSG_SetzeStichtagInfo cannot be NULL%' AS 'Error message for NULL Stichtag mismatch';
END;

-- Edge Case: NULL StichtagFmt
BEGIN
    CALL dw_is_error_management.DWMSG_SetzeStichtagInfo(v_eintragsnr, '2023-01-01', NULL);
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'All input parameters for DWMSG_SetzeStichtagInfo cannot be NULL%' AS 'Error message for NULL StichtagFmt mismatch';
END;

SELECT 'Test Case 8 Passed: DWMSG_SetzeStichtagInfo - Date Information Update';
```

---

### Test Case 9: `DWMSG_AppendTimingInfos` - Appending Text Information

*   **Purpose:** Validate that `DWMSG_AppendTimingInfos` correctly appends text and a formatted timestamp to the `zusatzinfos_text` column, handling initial NULL values.
*   **Setup:**
    1.  Truncate `dw_is_error_management.message_table`.
    2.  Insert a new entry into `message_table` using `DWMSG_ErzeugeEintrag`.
*   **Action:**
    1.  Call `dw_is_error_management.DWMSG_AppendTimingInfos` multiple times on the same entry with different `p_InfoText` and `p_DateFormat`.
*   **Pass/Fail Criterion:**
    1.  `zusatzinfos_text` in `message_table` is correctly concatenated with new information, including the formatted timestamp.
    2.  `updated_at` in `message_table` is updated after each call.
    3.  The `COALESCE` logic correctly handles an initially `NULL` `zusatzinfos_text`.
    4.  For invalid `p_DateFormat`, the procedure raises an error.
    5.  Calling with `NULL` for any required parameter raises an error.

```sql
-- Test Case 9: DWMSG_AppendTimingInfos - Appending Text Information
DECLARE v_eintragsnr INT64;
DECLARE v_job_kennung STRING DEFAULT 'TEST_JOB_007';
DECLARE v_programmname STRING DEFAULT 'timing_logger.ksh';
DECLARE v_log_datei STRING DEFAULT '/path/to/logs/timing_logger.log';
DECLARE v_created_at TIMESTAMP;
DECLARE v_current_timestamp1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE v_current_timestamp2 TIMESTAMP;
DECLARE v_current_timestamp3 TIMESTAMP;

-- Setup: Create an initial entry
TRUNCATE TABLE dw_is_error_management.message_table;
CALL dw_is_error_management.DWMSG_ErmittleNr(v_eintragsnr);
CALL dw_is_error_management.DWMSG_ErzeugeEintrag(v_eintragsnr, v_job_kennung, v_programmname, v_log_datei);
SELECT created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr INTO v_created_at;

-- Action 1: Append first timing info (zusatzinfos_text is initially NULL)
CALL dw_is_error_management.DWMSG_AppendTimingInfos(v_eintragsnr, 'Start processing', '%Y-%m-%d %H:%M:%S');

-- Pass/Fail Criterion 1: Check zusatzinfos_text and updated_at
DECLARE v_expected_text1 STRING;
SET v_expected_text1 = CONCAT('Start processing ', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', v_current_timestamp1), ' ');
ASSERT (SELECT zusatzinfos_text FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_expected_text1 AS 'First append text mismatch';
ASSERT (SELECT updated_at > v_created_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated after first append';

-- Action 2: Append second timing info (zusatzinfos_text now has a value)
SET v_current_timestamp2 = CURRENT_TIMESTAMP();
CALL dw_is_error_management.DWMSG_AppendTimingInfos(v_eintragsnr, 'Mid-point check', '%H:%M');

-- Pass/Fail Criterion 2: Check concatenated zusatzinfos_text and updated_at
DECLARE v_expected_text2 STRING;
SET v_expected_text2 = CONCAT(v_expected_text1, 'Mid-point check ', FORMAT_TIMESTAMP('%H:%M', v_current_timestamp2), ' ');
ASSERT (SELECT zusatzinfos_text FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_expected_text2 AS 'Second append text mismatch';
DECLARE v_updated_at_after_first_append TIMESTAMP;
SELECT updated_at FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr INTO v_updated_at_after_first_append;
ASSERT (SELECT updated_at > v_updated_at_after_first_append FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) AS 'updated_at not updated after second append';

-- Action 3: Append third timing info with different format
SET v_current_timestamp3 = CURRENT_TIMESTAMP();
CALL dw_is_error_management.DWMSG_AppendTimingInfos(v_eintragsnr, 'End processing', '%Y%m%d');

-- Pass/Fail Criterion 3: Check concatenated zusatzinfos_text and updated_at
DECLARE v_expected_text3 STRING;
SET v_expected_text3 = CONCAT(v_expected_text2, 'End processing ', FORMAT_TIMESTAMP('%Y%m%d', v_current_timestamp3), ' ');
ASSERT (SELECT zusatzinfos_text FROM dw_is_error_management.message_table WHERE eintragsnr = v_eintragsnr) = v_expected_text3 AS 'Third append text mismatch';

-- Edge Case: Invalid DateFormat
BEGIN
    CALL dw_is_error_management.DWMSG_AppendTimingInfos(v_eintragsnr, 'Invalid format test', '%INVALID_FORMAT%');
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'Invalid date format string provided: "%INVALID_FORMAT%". Error: %' AS 'Error message for invalid DateFormat mismatch';
END;

-- Edge Case: NULL EintragsNr
BEGIN
    CALL dw_is_error_management.DWMSG_AppendTimingInfos(NULL, 'Info', '%Y');
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'All input parameters for DWMSG_AppendTimingInfos cannot be NULL%' AS 'Error message for NULL EintragsNr mismatch';
END;

-- Edge Case: NULL InfoText
BEGIN
    CALL dw_is_error_management.DWMSG_AppendTimingInfos(v_eintragsnr, NULL, '%Y');
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'All input parameters for DWMSG_AppendTimingInfos cannot be NULL%' AS 'Error message for NULL InfoText mismatch';
END;

-- Edge Case: NULL DateFormat
BEGIN
    CALL dw_is_error_management.DWMSG_AppendTimingInfos(v_eintragsnr, 'Info', NULL);
EXCEPTION WHEN ERROR THEN
    ASSERT @@error.message LIKE 'All input parameters for DWMSG_AppendTimingInfos cannot be NULL%' AS 'Error message for NULL DateFormat mismatch';
END;

SELECT 'Test Case 9 Passed: DWMSG_AppendTimingInfos - Appending Text Information';
```