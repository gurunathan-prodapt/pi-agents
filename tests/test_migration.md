The `r_ausd_v_ta_cntrct_valid.ksh` script is an orchestration wrapper. Its migration to BigQuery involves translating shell logic, custom logging, and error handling into BigQuery stored procedures and audit tables. The tests below focus on validating this orchestration logic, parameter handling, and the behavior of the custom framework components.

We assume the following BigQuery resources are in place as per the migration design:
*   A BigQuery dataset: `project.dataset`
*   Audit tables: `project.dataset.job_log`, `project.dataset.job_status`
*   Migrated `DWMSG_` stored procedures: `sp_dwmsg_ermittlenr`, `sp_dwmsg_logdateiname`, `sp_dwmsg_erzeugeeintrag`, `sp_dwmsg_setzestichtaginfo`, `sp_dwmsg_meldefehler`, `sp_dwmsg_fehlerbehandlung`, `sp_dwmsg_setzestatusok`.
*   The main migrated orchestration procedure: `project.dataset.sp_r_ausd_v_ta_cntrct_valid`.
*   A mock or actual migrated core script procedure: `project.dataset.sp_k_ausd_v_ta_cntrct_valid`.

For the `sp_r_ausd_v_ta_cntrct_valid` procedure, we assume it takes a single `STRING` parameter, `p_options`, to mimic command-line arguments, and internally handles the `JobKennung` and `DW_EintragsNr` as the legacy script does.

---

### Test Case 1: Successful Execution - Happy Path

**Purpose:** Verify that the migrated BigQuery stored procedure executes successfully, performs all logging steps, correctly invokes the core logic procedure, and updates the job status to 'OK'. This covers output parity and transformation correctness for the main flow.

**Setup:**
1.  Ensure the BigQuery audit tables (`job_log`, `job_status`) are empty or in a known clean state.
2.  The `sp_k_ausd_v_ta_cntrct_valid` (migrated core script) is mocked to simulate successful execution.
    ```sql
    -- Mock sp_k_ausd_v_ta_cntrct_valid for testing successful execution
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_cntrct_valid`(p_job_kennung STRING, p_eintrags_nr INT64)
    BEGIN
        -- Simulate successful execution of the core script
        INSERT INTO `project.dataset.job_log` (job_entry_number, job_kennung, log_timestamp, log_level, log_message)
        VALUES (p_eintrags_nr, p_job_kennung, CURRENT_TIMESTAMP(), 'INFO', FORMAT("Mock core script executed successfully for JobKennung: %s, EintragsNr: %d", p_job_kennung, p_eintrags_nr));
    END;
    ```
3.  All `DWMSG_` BigQuery stored procedures are implemented and functional.

**Action:**
Execute the migrated BigQuery stored procedure `sp_r_ausd_v_ta_cntrct_valid` with no options (representing a default successful run).

```sql
CALL `project.dataset.sp_r_ausd_v_ta_cntrct_valid`(NULL);
```

**Pass/Fail Criterion:**
1.  The procedure completes without raising an unhandled error.
2.  The `job_log` table contains entries corresponding to:
    *   Job start (`DWMSG_ErzeugeEintrag`).
    *   Stichtag info (`DWMSG_SetzeStichtagInfo`).
    *   Core script execution (from the mock `sp_k_ausd_v_ta_cntrct_valid`).
    *   Success message ("Die Abarbeitung wurde ohne erkennbare Fehler beendet").
    *   Job status update to 'OK' (`DWMSG_SetzeStatusOK`).
3.  The `job_status` table for `JobKennung = 'BERT_V_TA_CNTRCT_VALID'` and the generated `DW_EintragsNr` shows a final `status_code` of 'OK'.
4.  The `sp_k_ausd_v_ta_cntrct_valid` mock/procedure was called exactly once with the correct `JobKennung` and `DW_EintragsNr`.

**Example SQL Assertions:**
```sql
-- Check for expected log entries and their order
SELECT
    log_message
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
ORDER BY
    log_timestamp ASC;
/* Expected messages (order matters):
    - "Job gestartet..." (from DWMSG_ErzeugeEintrag)
    - "Stichtag gesetzt..." (from DWMSG_SetzeStichtagInfo)
    - "Mock core script executed successfully..." (from sp_k_ausd_v_ta_cntrct_valid)
    - "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
    - "Status OK gesetzt..." (from DWMSG_SetzeStatusOK)
*/

-- Check final job status
SELECT
    status_code
FROM
    `project.dataset.job_status`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
ORDER BY
    status_timestamp DESC
LIMIT 1;
-- Expected result: 'OK'
```

---

### Test Case 2: Parameter Handling - Help Option

**Purpose:** Verify that the migrated procedure correctly handles the help parameter (`-h`), displaying usage information and exiting without further processing. This covers transformation correctness for parameter parsing.

**Setup:**
1.  Ensure audit tables are in a known clean state.
2.  The `sp_k_ausd_v_ta_cntrct_valid` mock is in place (as in Test Case 1).

**Action:**
Execute the migrated BigQuery stored procedure `sp_r_ausd_v_ta_cntrct_valid` with the help option.

```sql
CALL `project.dataset.sp_r_ausd_v_ta_cntrct_valid`('-h');
```

**Pass/Fail Criterion:**
1.  The procedure completes without raising an unhandled error (it should exit gracefully).
2.  The `job_log` table contains entries related to the usage message ("Programm: Vertragsdatenabgleich", "-h zeigt diese Seite an").
3.  No entries related to job start, stichtag info, core script execution, or final status updates are found in `job_log` or `job_status` for this run.
4.  The `sp_k_ausd_v_ta_cntrct_valid` mock/procedure was *not* called.

**Example SQL Assertions:**
```sql
-- Check for usage message in logs
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    log_message LIKE '%Programm: Vertragsdatenabgleich%'
    AND log_message LIKE '%-h     zeigt diese Seite an%';
-- Expected result: 1 (or more, depending on how the usage message is logged)

-- Check that no job processing occurred
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
    AND log_message LIKE '%Job gestartet%';
-- Expected result: 0
```

---

### Test Case 3: Parameter Handling - Invalid Parameter

**Purpose:** Verify that the migrated procedure correctly handles an unknown command-line parameter, logs an error (`ErrNr=192`), displays usage, and exits with an error status. This covers transformation correctness for parameter parsing and error handling.

**Setup:**
1.  Ensure audit tables are in a known clean state.
2.  The `sp_k_ausd_v_ta_cntrct_valid` mock is in place.

**Action:**
Execute the migrated BigQuery stored procedure `sp_r_ausd_v_ta_cntrct_valid` with an invalid parameter.

```sql
CALL `project.dataset.sp_r_ausd_v_ta_cntrct_valid`('-x');
```

**Pass/Fail Criterion:**
1.  The procedure raises an error or completes with an error status.
2.  The `job_log` table contains an entry from `DWMSG_MeldeFehler` with `ErrNr=192` ("Parameter unbekannt") and `ErrArg="-x"`.
3.  The `job_log` table contains the usage message after the error.
4.  No entries related to job start, stichtag info, or core script execution are found.
5.  The `sp_k_ausd_v_ta_cntrct_valid` mock/procedure was *not* called.
6.  The `job_status` table for this run shows a final `status_code` of 'ERROR'.

**Example SQL Assertions:**
```sql
-- Check for error log entry
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    log_message LIKE '%Fehler: 192%'
    AND log_message LIKE '%Parameter unbekannt%'
    AND log_message LIKE '%-x%';
-- Expected result: 1

-- Check for usage message after error
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    log_message LIKE '%Programm: Vertragsdatenabgleich%'
    AND log_message LIKE '%-h     zeigt diese Seite an%'
    AND log_timestamp > (SELECT MAX(log_timestamp) FROM `project.dataset.job_log` WHERE log_message LIKE '%Fehler: 192%');
-- Expected result: 1

-- Check that no job processing occurred
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
    AND log_message LIKE '%Job gestartet%';
-- Expected result: 0

-- Check final status
SELECT
    status_code
FROM
    `project.dataset.job_status`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
ORDER BY
    status_timestamp DESC
LIMIT 1;
-- Expected result: 'ERROR'
```

---

### Test Case 4: Parameter Handling - Missing Argument for Parameter

**Purpose:** Verify that the migrated procedure correctly handles parameters requiring arguments when an argument is missing, logs an error (`ErrNr=193`), displays usage, and exits with an error status. This covers transformation correctness for parameter parsing and error handling.

**Setup:**
1.  Ensure audit tables are in a known clean state.
2.  The `sp_k_ausd_v_ta_cntrct_valid` mock is in place.

**Action:**
Execute the migrated BigQuery stored procedure `sp_r_ausd_v_ta_cntrct_valid` with a parameter that expects an argument but doesn't provide one (e.g., `-s`, as `s:` is in `ParamList`).

```sql
CALL `project.dataset.sp_r_ausd_v_ta_cntrct_valid`('-s');
```

**Pass/Fail Criterion:**
1.  The procedure raises an error or completes with an error status.
2.  The `job_log` table contains an entry from `DWMSG_MeldeFehler` with `ErrNr=193` ("Notwendiges Argument fehlt") and `ErrArg="s"`.
3.  The `job_log` table contains the usage message after the error.
4.  No entries related to job start, stichtag info, or core script execution are found.
5.  The `sp_k_ausd_v_ta_cntrct_valid` mock/procedure was *not* called.
6.  The `job_status` table for this run shows a final `status_code` of 'ERROR'.

**Example SQL Assertions:**
```sql
-- Check for error log entry
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    log_message LIKE '%Fehler: 193%'
    AND log_message LIKE '%Notwendiges Argument fehlt%'
    AND log_message LIKE '%s%';
-- Expected result: 1

-- Check for usage message after error
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    log_message LIKE '%Programm: Vertragsdatenabgleich%'
    AND log_message LIKE '%-h     zeigt diese Seite an%'
    AND log_timestamp > (SELECT MAX(log_timestamp) FROM `project.dataset.job_log` WHERE log_message LIKE '%Fehler: 193%');
-- Expected result: 1

-- Check that no job processing occurred
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
    AND log_message LIKE '%Job gestartet%';
-- Expected result: 0

-- Check final status
SELECT
    status_code
FROM
    `project.dataset.job_status`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
ORDER BY
    status_timestamp DESC
LIMIT 1;
-- Expected result: 'ERROR'
```

---

### Test Case 5: Error Handling - Core Script Failure

**Purpose:** Verify that the migrated procedure correctly handles an error originating from the invoked core script, logs the error using `DWMSG_Fehlerbehandlung`, and updates the job status to 'ERROR'. This directly tests the BigQuery equivalent of the `trap ERR` mechanism.

**Setup:**
1.  Ensure audit tables are in a known clean state.
2.  Modify the `sp_k_ausd_v_ta_cntrct_valid` mock/procedure to simulate a failure.
    ```sql
    -- Mock sp_k_ausd_v_ta_cntrct_valid to simulate failure
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_cntrct_valid`(p_job_kennung STRING, p_eintrags_nr INT64)
    BEGIN
        -- Simulate an error during core script execution
        RAISE USING MESSAGE = FORMAT("Simulated error in core script for JobKennung: %s, EintragsNr: %d", p_job_kennung, p_eintrags_nr);
    END;
    ```

**Action:**
Execute the migrated BigQuery stored procedure `sp_r_ausd_v_ta_cntrct_valid` with no options.

```sql
CALL `project.dataset.sp_r_ausd_v_ta_cntrct_valid`(NULL);
```

**Pass/Fail Criterion:**
1.  The procedure raises an error or completes with an error status.
2.  The `job_log` table contains entries for job start (`DWMSG_ErzeugeEintrag`) and stichtag info (`DWMSG_SetzeStichtagInfo`).
3.  The `job_log` table contains an entry from `DWMSG_Fehlerbehandlung` indicating the error from the core script, and the message "AppError: Abbruch" (or its BigQuery equivalent).
4.  The `job_status` table for `JobKennung = 'BERT_V_TA_CNTRCT_VALID'` and the generated `DW_EintragsNr` shows a final `status_code` of 'ERROR'.
5.  The success message ("Die Abarbeitung wurde ohne erkennbare Fehler beendet") and `DWMSG_SetzeStatusOK` are *not* executed, and no corresponding log entries exist.

**Example SQL Assertions:**
```sql
-- Check for job start and stichtag info
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
    AND (log_message LIKE '%Job gestartet%' OR log_message LIKE '%Stichtag gesetzt%');
-- Expected result: 2

-- Check for error handling log entry
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
    AND log_message LIKE '%DWMSG_Fehlerbehandlung%' -- Or specific error message from the mock
    AND log_message LIKE '%AppError: Abbruch%';
-- Expected result: 1 (or more, depending on how DWMSG_Fehlerbehandlung logs)

-- Check that success messages are NOT present
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
    AND (log_message LIKE '%Abarbeitung wurde ohne erkennbare Fehler beendet%' OR log_message LIKE '%Status OK gesetzt%');
-- Expected result: 0

-- Check final status
SELECT
    status_code
FROM
    `project.dataset.job_status`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
ORDER BY
    status_timestamp DESC
LIMIT 1;
-- Expected result: 'ERROR'
```

---

### Test Case 6: External System Replacement - DWMSG_ErmittleNr

**Purpose:** Verify that the migrated `sp_dwmsg_ermittlenr` BigQuery stored procedure correctly generates a new, unique job entry number, mimicking the legacy shell function `DWMSG_ErmittleNr`. This covers external system replacement.

**Setup:**
1.  The `sp_dwmsg_ermittlenr` BigQuery stored procedure is implemented to generate unique, sequential numbers (e.g., using a sequence table or `GENERATE_UUID()`).

**Action:**
Call the `sp_dwmsg_ermittlenr` procedure multiple times and observe the returned values.

```sql
-- Example of calling and capturing output in BigQuery Scripting
DECLARE v_entry_nr1 INT64;
DECLARE v_entry_nr2 INT64;

CALL `project.dataset.sp_dwmsg_ermittlenr`(v_entry_nr1);
CALL `project.dataset.sp_dwmsg_ermittlenr`(v_entry_nr2);

SELECT v_entry_nr1 AS first_entry, v_entry_nr2 AS second_entry;
```

**Pass/Fail Criterion:**
1.  Each call to `sp_dwmsg_ermittlenr` returns a unique integer value.
2.  If the legacy system used sequential numbers, the BigQuery equivalent should also produce sequential numbers (e.g., `v_entry_nr2` should be `v_entry_nr1 + 1` if no other jobs ran in between).

**Example SQL Assertion (after running the main procedure multiple times):**
```sql
-- Check for uniqueness and sequence of job entry numbers
SELECT
    job_entry_number,
    LAG(job_entry_number) OVER (ORDER BY status_timestamp) AS previous_entry_number
FROM
    `project.dataset.job_status`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
ORDER BY
    status_timestamp ASC;
-- Expected result: Each job_entry_number should be unique. If sequential, then job_entry_number = previous_entry_number + 1.
```

---

### Test Case 7: External System Replacement - DWMSG_Logdateiname

**Purpose:** Verify that the migrated `sp_dwmsg_logdateiname` BigQuery stored procedure correctly constructs a log file name based on `JobKennung` and `DW_EintragsNr`, mimicking the legacy shell function. This covers external system replacement.

**Setup:**
1.  The `sp_dwmsg_logdateiname` BigQuery stored procedure is implemented.

**Action:**
Call the `sp_dwmsg_logdateiname` procedure with sample `JobKennung` and `DW_EintragsNr` values.

```sql
DECLARE v_log_filename STRING;
CALL `project.dataset.sp_dwmsg_logdateiname`('TEST_JOB', 12345, v_log_filename);
SELECT v_log_filename AS generated_log_filename;
```

**Pass/Fail Criterion:**
1.  The returned `v_log_filename` matches the expected pattern from the legacy script, e.g., `TEST_JOB_12345.log`.

**Example SQL Assertion (after running the main procedure, e.g., Test Case 1):**
```sql
-- Check the format of the log file name stored in the audit table
SELECT
    log_file_name
FROM
    `project.dataset.job_log`
WHERE
    job_kennung = 'BERT_V_TA_CNTRCT_VALID'
ORDER BY
    log_timestamp DESC
LIMIT 1;
-- Expected result: A string matching the pattern 'BERT_V_TA_CNTRCT_VALID_XXXXX.log' where XXXXX is the generated entry number.
```

---

### Test Case 8: Data Quality / Schema - Audit Tables

**Purpose:** Verify that the schema of the BigQuery audit tables (`job_log`, `job_status`) is correctly defined and that data inserted into them adheres to expected types and constraints. This covers data quality and schema assertions.

**Setup:**
1.  The BigQuery audit tables (`job_log`, `job_status`) are created as per the migration design.
2.  The `DWMSG_` BigQuery stored procedures are implemented and functional.
3.  Execute the main orchestration procedure `sp_r_ausd_v_ta_cntrct_valid` for at least one successful run (Test Case 1) and one error run (Test Case 5) to populate the audit tables with diverse data.

**Action:**
Inspect the schema of the audit tables and query their contents for data quality.

```sql
-- Inspect schema for job_log
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'job_log'
ORDER BY
    ordinal_position;

-- Inspect schema for job_status
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'job_status'
ORDER BY
    ordinal_position;

-- Query sample data for quick visual inspection
SELECT * FROM `project.dataset.job_log` LIMIT 10;
SELECT * FROM `project.dataset.job_status` LIMIT 10;
```

**Pass/Fail Criterion:**
1.  **Schema:**
    *   `job_log` table contains columns like `job_entry_number` (INT64), `job_kennung` (STRING), `log_timestamp` (TIMESTAMP), `log_level` (STRING, e.g., 'INFO', 'ERROR'), `log_message` (STRING), `log_file_name` (STRING).
    *   `job_status` table contains columns like `job_entry_number` (INT64), `job_kennung` (STRING), `status_timestamp` (TIMESTAMP), `status_code` (STRING, e.g., 'STARTED', 'OK', 'ERROR'), `stichtag_info` (DATE or STRING).
    *   All columns have appropriate BigQuery data types.
    *   Critical columns (`job_entry_number`, `job_kennung`, `log_timestamp`, `status_timestamp`, `status_code`) are non-nullable.
2.  **Data Quality:**
    *   `job_entry_number` values are positive integers.
    *   `job_kennung` values are uppercase strings.
    *   `log_timestamp` and `status_timestamp` are valid timestamps.
    *   `log_level` and `status_code` values conform to expected enumerations (e.g., 'INFO', 'WARN', 'ERROR', 'STARTED', 'OK').
    *   No unexpected NULLs in critical fields.

**Example SQL Assertions (for data quality):**
```sql
-- Check for invalid job_entry_number in job_log
SELECT
    COUNT(1)
FROM
    `project.dataset.job_log`
WHERE
    job_entry_number IS NULL OR job_entry_number <= 0;
-- Expected result: 0

-- Check for unexpected log_level values
SELECT
    DISTINCT log_level
FROM
    `project.dataset.job_log`
WHERE
    log_level NOT IN ('INFO', 'WARN', 'ERROR', 'DEBUG');
-- Expected result: 0 rows

-- Check for consistency between job_log and job_status for a given run
-- (Ensures a 'STARTED' status exists for every job that logged a start message)
SELECT
    COUNT(1)
FROM
    (SELECT job_entry_number, job_kennung FROM `project.dataset.job_log` WHERE log_message LIKE '%Job gestartet%') AS logged_starts
LEFT JOIN
    (SELECT job_entry_number, job_kennung FROM `project.dataset.job_status` WHERE status_code = 'STARTED') AS status_starts
ON
    logged_starts.job_entry_number = status_starts.job_entry_number
    AND logged_starts.job_kennung = status_starts.job_kennung
WHERE
    status_starts.job_entry_number IS NULL;
-- Expected result: 0 (all logged starts should have a corresponding 'STARTED' status)
```