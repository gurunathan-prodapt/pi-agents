As a senior data-migration QA engineer, I've developed a comprehensive suite of validation tests for the migration of `k_ausd_bp_ta_iccid_einzeln.ksh` to Google BigQuery. These tests aim to ensure behavioral equivalence, data integrity, and correct integration with the new BigQuery environment.

The tests are organized into distinct cases, each with a clear purpose, setup instructions, actions to perform, and concrete pass/fail criteria, including runnable SQL or conceptual Python/pytest code.

---

## Test Environment Setup (Prerequisites)

Before executing any tests, ensure the following BigQuery objects are created and accessible within your GCP project and dataset (replace `project.dataset` with your actual project and dataset IDs):

1.  **Logging Tables:**
    *   `project.dataset.error_log`
    *   `project.dataset.job_log`
    *   `project.dataset.process_log`
    (Use the DDLs provided in the migration design document.)

2.  **Source Data Table:**
    *   `project.dataset.sof_ta_bpr_basis`
    (DDL inferred from the `d_ausd_bp_ta_iccid_einzeln.sql` procedure. Adjust types as necessary based on actual source data.)

    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_bpr_basis` (
      cntrct_id INT64,
      bpr_id INT64,
      slave_number INT64,
      iccid STRING,
      imsi_mcc STRING,
      imsi_mnc STRING,
      imsi_hlr STRING,
      imsi_si STRING,
      valid_to DATE,
      E_ID STRING,
      CARD_TYPE_NAME STRING
    );
    ```

3.  **Target Data Table:**
    *   `project.dataset.sof_ta_iccid_einzeln`
    (DDL inferred from the `INSERT` statement in `d_ausd_bp_ta_iccid_einzeln.sql`. All `_ICCID`, `_IMSI_MCC`, etc. columns are `STRING`, `_VALID_TO` is `DATE`, `CNTRCT_ID` is `INT64`.)

    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_iccid_einzeln` (
      CNTRCT_ID INT64,
      TN_ICCID STRING, TN_IMSI_MCC STRING, TN_IMSI_MNC STRING, TN_IMSI_HLR STRING, TN_IMSI_SI STRING, TN_STATUS STRING, TN_VALID_TO DATE, TN_E_ID STRING, TN_CARD_TYPE_NAME STRING,
      TC_ICCID STRING, TC_IMSI_MCC STRING, TC_IMSI_MNC STRING, TC_IMSI_HLR STRING, TC_IMSI_SI STRING, TC_STATUS STRING, TC_VALID_TO DATE, TC_E_ID STRING, TC_CARD_TYPE_NAME STRING,
      TB_ICCID STRING, TB_IMSI_MCC STRING, TB_IMSI_MNC STRING, TB_IMSI_HLR STRING, TB_IMSI_SI STRING, TB_STATUS STRING, TB_VALID_TO DATE, TB_E_ID STRING, TB_CARD_TYPE_NAME STRING,
      MS1_ICCID STRING, MS1_IMSI_MCC STRING, MS1_IMSI_MNC STRING, MS1_IMSI_HLR STRING, MS1_IMSI_SI STRING, MS1_STATUS STRING, MS1_VALID_TO DATE, MS1_E_ID STRING, MS1_CARD_TYPE_NAME STRING,
      MS2_ICCID STRING, MS2_IMSI_MCC STRING, MS2_IMSI_MNC STRING, MS2_IMSI_HLR STRING, MS2_IMSI_SI STRING, MS2_STATUS STRING, MS2_VALID_TO DATE, MS2_E_ID STRING, MS2_CARD_TYPE_NAME STRING,
      MS3_ICCID STRING, MS3_IMSI_MCC STRING, MS3_IMSI_MNC STRING, MS3_IMSI_HLR STRING, MS3_IMSI_SI STRING, MS3_STATUS STRING, MS3_VALID_TO DATE, MS3_E_ID STRING, MS3_CARD_TYPE_NAME STRING,
      MS4_ICCID STRING, MS4_IMSI_MCC STRING, MS4_IMSI_MNC STRING, MS4_IMSI_HLR STRING, MS4_IMSI_SI STRING, MS4_STATUS STRING, MS4_VALID_TO DATE, MS4_E_ID STRING, MS4_CARD_TYPE_NAME STRING,
      MS5_ICCID STRING, MS5_IMSI_MCC STRING, MS5_IMSI_MNC STRING, MS5_IMSI_HLR STRING, MS5_IMSI_SI STRING, MS5_STATUS STRING, MS5_VALID_TO DATE, MS5_E_ID STRING, MS5_CARD_TYPE_NAME STRING,
      MS6_ICCID STRING, MS6_IMSI_MCC STRING, MS6_IMSI_MNC STRING, MS6_IMSI_HLR STRING, MS6_IMSI_SI STRING, MS6_STATUS STRING, MS6_VALID_TO DATE, MS6_E_ID STRING, MS6_CARD_TYPE_NAME STRING,
      MS7_ICCID STRING, MS7_IMSI_MCC STRING, MS7_IMSI_MNC STRING, MS7_IMSI_HLR STRING, MS7_IMSI_SI STRING, MS7_STATUS STRING, MS7_VALID_TO DATE, MS7_E_ID STRING, MS7_CARD_TYPE_NAME STRING,
      MS8_ICCID STRING, MS8_IMSI_MCC STRING, MS8_IMSI_MNC STRING, MS8_IMSI_HLR STRING, MS8_IMSI_SI STRING, MS8_STATUS STRING, MS8_VALID_TO DATE, MS8_E_ID STRING, MS8_CARD_TYPE_NAME STRING,
      MS9_ICCID STRING, MS9_IMSI_MCC STRING, MS9_IMSI_MNC STRING, MS9_IMSI_HLR STRING, MS9_IMSI_SI STRING, MS9_STATUS STRING, MS9_VALID_TO DATE, MS9_E_ID STRING, MS9_CARD_TYPE_NAME STRING,
      MS10_ICCID STRING, MS10_IMSI_MCC STRING, MS10_IMSI_MNC STRING, MS10_IMSI_HLR STRING, MS10_IMSI_SI STRING, MS10_STATUS STRING, MS10_VALID_TO DATE, MS10_E_ID STRING, MS10_CARD_TYPE_NAME STRING
    );
    ```

4.  **Stored Procedures:**
    *   `project.dataset.d_ausd_bp_ta_iccid_einzeln`
    *   `project.dataset.r_ausd_bp_ta_iccid_einzeln`
    (Use the DDLs provided in the migration design document.)

---

## Test Cases

### Test Case 1: Successful Execution - Output Parity & Transformation Correctness

*   **Purpose:** Verify that the migrated job executes successfully with valid parameters, produces the correct output in `sof_ta_iccid_einzeln`, and logs the correct metadata. This covers output parity and core transformation logic.
*   **Setup:**
    1.  Clear all logging tables and the target table.
    2.  Populate `project.dataset.sof_ta_bpr_basis` with diverse test data, including rows matching `bpr_id` 31, 2759, 2800, and 3848 (with various `slave_number` values), and rows that don't match. Include cases where `valid_to` is before and after the `p_Stichtag` to test status logic.

    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.process_log`;
    TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
    TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;

    INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME) VALUES
    (1001, 31, 0, 'ICCID_TN_001', '262', '01', 'HLR1', 'SI1', '2023-01-15', 'EID_TN1', 'TypeA'), -- TN, Active
    (1002, 31, 0, 'ICCID_TN_002', '262', '02', 'HLR2', 'SI2', '2023-01-01', 'EID_TN2', 'TypeB'), -- TN, Legacy
    (1003, 2759, 0, 'ICCID_TC_001', '262', '03', 'HLR3', 'SI3', '2023-01-15', 'EID_TC1', 'TypeC'), -- TC, Active
    (1004, 2800, 0, 'ICCID_TB_001', '262', '04', 'HLR4', 'SI4', '2023-01-15', 'EID_TB1', 'TypeD'), -- TB, Active
    (1005, 3848, 1, 'ICCID_MS1_001', '262', '05', 'HLR5', 'SI5', '2023-01-15', 'EID_MS1', 'TypeE'), -- MS1, Active
    (1006, 3848, 2, 'ICCID_MS2_001', '262', '06', 'HLR6', 'SI6', '2023-01-15', 'EID_MS2', 'TypeF'), -- MS2, Active
    (1007, 3848, 10, 'ICCID_MS10_001', '262', '10', 'HLR10', 'SI10', '2023-01-15', 'EID_MS10', 'TypeJ'), -- MS10, Active
    (1008, 3848, 1, 'ICCID_MS1_002', '262', '05', 'HLR5', 'SI5', '2023-01-01', 'EID_MS1_L', 'TypeE'), -- MS1, Legacy
    (1009, 9999, 0, 'ICCID_OTHER', 'XXX', 'YY', 'ZZZ', 'WW', '2023-01-15', 'EID_OTHER', 'TypeOther'); -- Not matching bpr_id
    ```
*   **Action:** Execute the main stored procedure with valid parameters.
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '15012023', -- p_Stichtag (DDMMYYYY)
      0           -- p_wiederanlaufWert
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **No error raised** by the stored procedure.
    2.  **`project.dataset.error_log` is empty.**
    3.  **`project.dataset.sof_ta_iccid_einzeln` contains 8 rows.** (The row with `bpr_id = 9999` should be filtered out).
    4.  **Data in `sof_ta_iccid_einzeln` matches expected transformations:**
        *   For `cntrct_id = 1001` (bpr_id=31, valid_to=2023-01-15, stichtag=2023-01-15): `TN_ICCID` should be 'ICCID_TN_001', `TN_STATUS` should be 'A'. All other `_ICCID` columns for this row should be NULL.
        *   For `cntrct_id = 1002` (bpr_id=31, valid_to=2023-01-01, stichtag=2023-01-15): `TN_ICCID` should be 'ICCID_TN_002', `TN_STATUS` should be 'L'. All other `_ICCID` columns for this row should be NULL.
        *   For `cntrct_id = 1005` (bpr_id=3848, slave_number=1, valid_to=2023-01-15, stichtag=2023-01-15): `MS1_ICCID` should be 'ICCID_MS1_001', `MS1_STATUS` should be 'A'. All other `_ICCID` columns for this row should be NULL.
        *   For `cntrct_id = 1008` (bpr_id=3848, slave_number=1, valid_to=2023-01-01, stichtag=2023-01-15): `MS1_ICCID` should be 'ICCID_MS1_002', `MS1_STATUS` should be 'L'. All other `_ICCID` columns for this row should be NULL.
        *   Rows with `bpr_id = 9999` should not appear in `sof_ta_iccid_einzeln`.
    5.  **`project.dataset.job_log` contains one entry:**
        *   `tab_name` = 'PoolBasisprodukt'
        *   `job_status` = 'A'
        *   `job_type` = 'I'
        *   `stichtag` = `DATE '2023-01-15'`
        *   `run_date` = `DATE '2023-01-15'`
        *   `record_count` = 8 (matching the count of rows inserted)
        *   `message` = 'Initialbefuellung'
    6.  **`project.dataset.process_log` contains one entry:**
        *   `job_kennung` = 'TEST_JOB'
        *   `eintrags_nr` = 'ENTRY_001'
        *   `stichtag` = `DATE '2023-01-15'`
        *   `records` = 8 (matching the count of rows inserted)

    ```sql
    -- Pass/Fail Criteria SQL (example assertions)
    SELECT COUNT(*) FROM `project.dataset.error_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 8

    SELECT
      CNTRCT_ID, TN_ICCID, TN_STATUS, MS1_ICCID, MS1_STATUS
    FROM `project.dataset.sof_ta_iccid_einzeln`
    WHERE CNTRCT_ID IN (1001, 1002, 1005, 1008)
    ORDER BY CNTRCT_ID;
    /* Expected Result:
    CNTRCT_ID | TN_ICCID       | TN_STATUS | MS1_ICCID      | MS1_STATUS
    ----------|----------------|-----------|----------------|-----------
    1001      | ICCID_TN_001   | A         | NULL           | NULL
    1002      | ICCID_TN_002   | L         | NULL           | NULL
    1005      | NULL           | NULL      | ICCID_MS1_001  | A
    1008      | NULL           | NULL      | ICCID_MS1_002  | L
    */

    SELECT
      tab_name, job_status, job_type, stichtag, run_date, record_count, message
    FROM `project.dataset.job_log`;
    /* Expected Result:
    tab_name       | job_status | job_type | stichtag   | run_date   | record_count | message
    ---------------|------------|----------|------------|------------|--------------|----------------
    PoolBasisprodukt | A          | I        | 2023-01-15 | 2023-01-15 | 8            | Initialbefuellung
    */

    SELECT
      job_kennung, eintrags_nr, stichtag, records
    FROM `project.dataset.process_log`;
    /* Expected Result:
    job_kennung | eintrags_nr | stichtag   | records
    ------------|-------------|------------|--------
    TEST_JOB    | ENTRY_001   | 2023-01-15 | 8
    */
    ```

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify that the job correctly handles a missing `p_JobKennung` parameter, raises an error, and logs it.
*   **Setup:** Clear all logging tables and the target table. `sof_ta_bpr_basis` can be empty as the error occurs before data processing.
*   **Action:** Execute the main stored procedure with `p_JobKennung` as NULL or an empty string.
    ```sql
    -- Test with NULL
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      NULL,        -- p_JobKennung is NULL
      'ENTRY_001',
      '15012023',
      0
    );

    -- Test with empty string (after clearing logs again)
    -- TRUNCATE TABLE `project.dataset.error_log`;
    -- CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
    --   '',          -- p_JobKennung is empty string
    --   'ENTRY_001',
    --   '15012023',
    --   0
    -- );
    ```
*   **Pass/Fail Criteria:**
    1.  **Stored procedure raises an error** with message `FEHLER: 0 E 193 Jobkennung fehlt`.
    2.  **`project.dataset.error_log` contains one entry:**
        *   `tab_name` = 'PoolBasisprodukt'
        *   `error_type` = 'E'
        *   `error_code` = 193
        *   `message` = 'Jobkennung fehlt'
    3.  **`project.dataset.job_log` and `project.dataset.process_log` are empty.**
    4.  **`project.dataset.sof_ta_iccid_einzeln` is empty.**

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_code = 193 AND message = 'Jobkennung fehlt'; -- Expected: 1
    SELECT COUNT(*) FROM `project.dataset.job_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.process_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0
    ```

### Test Case 3: Parameter Validation - Missing `p_Stichtag`

*   **Purpose:** Verify that the job correctly handles a missing `p_Stichtag` parameter, raises an error, and logs it.
*   **Setup:** Clear all logging tables and the target table.
*   **Action:** Execute the main stored procedure with `p_Stichtag` as NULL or an empty string.
    ```sql
    -- Test with NULL
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      NULL,        -- p_Stichtag is NULL
      0
    );

    -- Test with empty string (after clearing logs again)
    -- TRUNCATE TABLE `project.dataset.error_log`;
    -- CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
    --   'TEST_JOB',
    --   'ENTRY_001',
    --   '',          -- p_Stichtag is empty string
    --   0
    -- );
    ```
*   **Pass/Fail Criteria:**
    1.  **Stored procedure raises an error** with message `FEHLER: 0 E 193 Stichtag fehlt`.
    2.  **`project.dataset.error_log` contains one entry:**
        *   `tab_name` = 'PoolBasisprodukt'
        *   `error_type` = 'E'
        *   `error_code` = 193
        *   `message` = 'Stichtag fehlt'
    3.  **`project.dataset.job_log` and `project.dataset.process_log` are empty.**
    4.  **`project.dataset.sof_ta_iccid_einzeln` is empty.**

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_code = 193 AND message = 'Stichtag fehlt'; -- Expected: 1
    SELECT COUNT(*) FROM `project.dataset.job_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.process_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0
    ```

### Test Case 4: Parameter Validation - Invalid `p_Stichtag` Format

*   **Purpose:** Verify that the job correctly handles an invalid `p_Stichtag` format, raises an error, and logs it.
*   **Setup:** Clear all logging tables and the target table.
*   **Action:** Execute the main stored procedure with an invalid `p_Stichtag` format (e.g., YYYYMMDD instead of DDMMYYYY).
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '20230115',  -- Invalid format
      0
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **Stored procedure raises an error** with message `FEHLER: 0 E 194 Ungueltiges Datum`.
    2.  **`project.dataset.error_log` contains one entry:**
        *   `tab_name` = 'PoolBasisprodukt'
        *   `error_type` = 'E'
        *   `error_code` = 194
        *   `message` = 'Ungueltiges Datum: 20230115' (or similar, including the invalid date string)
    3.  **`project.dataset.job_log` and `project.dataset.process_log` are empty.**
    4.  **`project.dataset.sof_ta_iccid_einzeln` is empty.**

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_code = 194 AND message LIKE 'Ungueltiges Datum%'; -- Expected: 1
    SELECT COUNT(*) FROM `project.dataset.job_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.process_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0
    ```

### Test Case 5: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose:** Verify that the job correctly handles a missing `p_EintragsNr` parameter, raises an error, and logs it.
*   **Setup:** Clear all logging tables and the target table.
*   **Action:** Execute the main stored procedure with `p_EintragsNr` as NULL or an empty string.
    ```sql
    -- Test with NULL
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      NULL,        -- p_EintragsNr is NULL
      '15012023',
      0
    );

    -- Test with empty string (after clearing logs again)
    -- TRUNCATE TABLE `project.dataset.error_log`;
    -- CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
    --   'TEST_JOB',
    --   '',          -- p_EintragsNr is empty string
    --   '15012023',
    --   0
    -- );
    ```
*   **Pass/Fail Criteria:**
    1.  **Stored procedure raises an error** with message `FEHLER: 0 E 193 EintragsNr fehlt`.
    2.  **`project.dataset.error_log` contains one entry:**
        *   `tab_name` = 'PoolBasisprodukt'
        *   `error_type` = 'E'
        *   `error_code` = 193
        *   `message` = 'EintragsNr fehlt'
    3.  **`project.dataset.job_log` and `project.dataset.process_log` are empty.**
    4.  **`project.dataset.sof_ta_iccid_einzeln` is empty.**

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_code = 193 AND message = 'EintragsNr fehlt'; -- Expected: 1
    SELECT COUNT(*) FROM `project.dataset.job_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.process_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0
    ```

### Test Case 6: `p_wiederanlaufWert` Default Handling

*   **Purpose:** Verify that `p_wiederanlaufWert` defaults to 0 if NULL is passed, ensuring the procedure continues execution as intended.
*   **Setup:** Same as Test Case 1, including populating `sof_ta_bpr_basis` with test data. Clear logging tables.
*   **Action:** Execute the main stored procedure with `p_wiederanlaufWert` as NULL.
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '15012023',
      NULL        -- p_wiederanlaufWert is NULL
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **No error raised.**
    2.  **`project.dataset.job_log` and `project.dataset.process_log` contain entries** with `record_count` and `records` matching the successful execution (as in Test Case 1). This implicitly confirms `v_restart` was set to 0 and the procedure continued.
    3.  **`project.dataset.sof_ta_iccid_einzeln` contains the correct data** as in Test Case 1.

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT COUNT(*) FROM `project.dataset.error_log`; -- Expected: 0
    SELECT record_count FROM `project.dataset.job_log`; -- Expected: 8
    SELECT records FROM `project.dataset.process_log`; -- Expected: 8
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 8
    ```

### Test Case 7: Date Derivation (`v_datum_heute`, `v_datum_gestern`)

*   **Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` are correctly calculated within `r_ausd_bp_ta_iccid_einzeln` and passed to `d_ausd_bp_ta_iccid_einzeln`, replacing the `gestern.ksh` utility.
*   **Setup:**
    1.  Create a temporary logging table to capture the parameters passed to `d_ausd_bp_ta_iccid_einzeln`.
    2.  Temporarily modify `d_ausd_bp_ta_iccid_einzeln` to insert the received parameters into this log table at the beginning of its execution.
    3.  Clear the temporary log table and other logging tables.

    ```sql
    -- Temporary DDL for d_ausd_bp_ta_iccid_einzeln_params_log
    CREATE TABLE IF NOT EXISTS `project.dataset.d_ausd_bp_ta_iccid_einzeln_params_log` (
      run_timestamp TIMESTAMP,
      p_EintragsNr STRING,
      p_JobKennung STRING,
      p_Stichtag_str STRING,
      p_restart INT64,
      p_datum_heute DATE,
      p_datum_gestern DATE
    );

    -- Temporarily modify d_ausd_bp_ta_iccid_einzeln to log parameters
    -- IMPORTANT: Revert this change after the test.
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_iccid_einzeln`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN p_Stichtag_str STRING,
      IN p_restart INT64,
      IN p_datum_heute DATE,
      IN p_datum_gestern DATE
    )
    BEGIN
      DECLARE v_stichtag_date DATE;

      INSERT INTO `project.dataset.d_ausd_bp_ta_iccid_einzeln_params_log`
      VALUES (CURRENT_TIMESTAMP(), p_EintragsNr, p_JobKennung, p_Stichtag_str, p_restart, p_datum_heute, p_datum_gestern);

      -- Original logic of d_ausd_bp_ta_iccid_einzeln follows here...
      SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag_str);
      TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
      INSERT INTO `project.dataset.sof_ta_iccid_einzeln` (...) SELECT ... FROM `project.dataset.sof_ta_bpr_basis` ...;
    END;

    TRUNCATE TABLE `project.dataset.d_ausd_bp_ta_iccid_einzeln_params_log`;
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.process_log`;
    ```
*   **Action:** Execute `r_ausd_bp_ta_iccid_einzeln` with valid parameters.
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '15012023',
      0
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **`project.dataset.d_ausd_bp_ta_iccid_einzeln_params_log` contains one entry.**
    2.  The `p_datum_heute` in the log entry should be `CURRENT_DATE()` of the execution day.
    3.  The `p_datum_gestern` in the log entry should be `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` of the execution day.

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT
      p_datum_heute = CURRENT_DATE() AS is_heute_correct,
      p_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS is_gestern_correct
    FROM `project.dataset.d_ausd_bp_ta_iccid_einzeln_params_log`;
    -- Expected: is_heute_correct = TRUE, is_gestern_correct = TRUE
    ```
*   **Cleanup:** Revert `d_ausd_bp_ta_iccid_einzeln` to its original state (without the logging insert) and drop `d_ausd_bp_ta_iccid_einzeln_params_log`.

### Test Case 8: `TRUNCATE` behavior in `d_ausd_bp_ta_iccid_einzeln`

*   **Purpose:** Verify that `d_ausd_bp_ta_iccid_einzeln` truncates the target table (`sof_ta_iccid_einzeln`) before inserting new data, mimicking the "restart" behavior or clean run of the legacy script.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_iccid_einzeln` with some dummy data *before* calling the main procedure.
    2.  Populate `project.dataset.sof_ta_bpr_basis` with a small set of test data (e.g., 2 rows that would match the filter).
    3.  Clear logging tables.

    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.process_log`;
    TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;

    -- Pre-populate target table with dummy data
    INSERT INTO `project.dataset.sof_ta_iccid_einzeln` (CNTRCT_ID, TN_ICCID) VALUES (9999, 'DUMMY_ICCID_1'), (9998, 'DUMMY_ICCID_2');

    -- Populate source table with data that will be inserted
    INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME) VALUES
    (1001, 31, 0, 'ICCID_TN_001', '262', '01', 'HLR1', 'SI1', '2023-01-15', 'EID_TN1', 'TypeA'),
    (1003, 2759, 0, 'ICCID_TC_001', '262', '03', 'HLR3', 'SI3', '2023-01-15', 'EID_TC1', 'TypeC');
    ```
*   **Action:** Execute the main stored procedure.
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '15012023',
      0
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **`project.dataset.sof_ta_iccid_einzeln` should contain only the newly inserted rows** (2 rows in this case), and the dummy rows (CNTRCT_ID 9999, 9998) should be gone.
    2.  The `record_count` in `job_log` and `records` in `process_log` should reflect the count of *newly inserted* rows (2), not including the pre-existing dummy data.

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 2
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln` WHERE CNTRCT_ID IN (9999, 9998); -- Expected: 0
    SELECT record_count FROM `project.dataset.job_log`; -- Expected: 2
    ```

### Test Case 9: NULL Handling in `d_ausd_bp_ta_iccid_einzeln`

*   **Purpose:** Verify that NULL values in source columns are correctly propagated or handled by the `CASE` statements, resulting in NULLs in the target table where expected.
*   **Setup:** Populate `project.dataset.sof_ta_bpr_basis` with rows containing NULLs for `iccid`, `imsi_mcc`, etc., for relevant `bpr_id`s. Clear logging tables and target table.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.process_log`;
    TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
    TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;

    INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME) VALUES
    (2001, 31, 0, NULL, '262', NULL, 'HLR1', 'SI1', '2023-01-15', 'EID_TN1', 'TypeA'), -- TN, ICCID is NULL, IMSI_MNC is NULL
    (2002, 3848, 1, 'ICCID_MS1_001', NULL, '05', 'HLR5', NULL, '2023-01-15', 'EID_MS1', 'TypeE'); -- MS1, IMSI_MCC is NULL, IMSI_SI is NULL
    ```
*   **Action:** Execute the main stored procedure.
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '15012023',
      0
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **`project.dataset.sof_ta_iccid_einzeln` contains 2 rows.**
    2.  For `cntrct_id = 2001`: `TN_ICCID` should be NULL, `TN_IMSI_MNC` should be NULL, `TN_IMSI_MCC` should be '262'. All other `_ICCID` columns should be NULL.
    3.  For `cntrct_id = 2002`: `MS1_ICCID` should be 'ICCID_MS1_001', `MS1_IMSI_MCC` should be NULL, `MS1_IMSI_SI` should be NULL. All other `_ICCID` columns should be NULL.

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT
      CNTRCT_ID, TN_ICCID, TN_IMSI_MCC, TN_IMSI_MNC,
      MS1_ICCID, MS1_IMSI_MCC, MS1_IMSI_SI
    FROM `project.dataset.sof_ta_iccid_einzeln`
    WHERE CNTRCT_ID IN (2001, 2002)
    ORDER BY CNTRCT_ID;
    /* Expected Result:
    CNTRCT_ID | TN_ICCID | TN_IMSI_MCC | TN_IMSI_MNC | MS1_ICCID     | MS1_IMSI_MCC | MS1_IMSI_SI
    ----------|----------|-------------|-------------|---------------|--------------|------------
    2001      | NULL     | 262         | NULL        | NULL          | NULL         | NULL
    2002      | NULL     | NULL        | NULL        | ICCID_MS1_001 | NULL         | NULL
    */
    ```

### Test Case 10: Edge Case - No Matching `bpr_id`s

*   **Purpose:** Verify that if no rows in `sof_ta_bpr_basis` match the `bpr_id` filter, the target table remains empty, and log entries reflect 0 records.
*   **Setup:** Populate `project.dataset.sof_ta_bpr_basis` with data that does *not* match the `bpr_id` filter (`IN (31, 2759, 2800, 3848)`). Clear logging tables and target table.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.process_log`;
    TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
    TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;

    INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME) VALUES
    (3001, 1, 0, 'ICCID_NOMATCH', '111', '22', 'HLR_X', 'SI_Y', '2023-01-15', 'EID_Z', 'Type_N');
    ```
*   **Action:** Execute the main stored procedure.
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '15012023',
      0
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **No error raised.**
    2.  **`project.dataset.sof_ta_iccid_einzeln` is empty.**
    3.  **`project.dataset.job_log` contains one entry** with `record_count = 0`.
    4.  **`project.dataset.process_log` contains one entry** with `records = 0`.

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT COUNT(*) FROM `project.dataset.error_log`; -- Expected: 0
    SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0
    SELECT record_count FROM `project.dataset.job_log`; -- Expected: 0
    SELECT records FROM `project.dataset.process_log`; -- Expected: 0
    ```

### Test Case 11: Data Type Handling (Implicit Conversion & Schema Adherence)

*   **Purpose:** Verify that data types are handled correctly, especially for `INT64` and `STRING` conversions, and that the target table schema is respected.
*   **Setup:** Populate `project.dataset.sof_ta_bpr_basis` with data where `cntrct_id` is an integer, and other string fields are strings. Clear logging tables and target table.
    ```sql
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.process_log`;
    TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
    TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;

    INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME) VALUES
    (4001, 31, 0, 'ICCID_STR', '262', '01', 'HLR1', 'SI1', '2023-01-15', 'EID_STR', 'TypeA');
    ```
*   **Action:** Execute the main stored procedure.
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
      'TEST_JOB',
      'ENTRY_001',
      '15012023',
      0
    );
    ```
*   **Pass/Fail Criteria:**
    1.  **No error raised** due to type mismatch.
    2.  **`project.dataset.sof_ta_iccid_einzeln` contains 1 row.**
    3.  The `CNTRCT_ID` column should be `INT64` with value `4001`.
    4.  `TN_ICCID`, `TN_IMSI_MCC`, `TN_E_ID`, `TN_CARD_TYPE_NAME` should be `STRING` values.
    5.  `TN_VALID_TO` should be a `DATE` value.

    ```sql
    -- Pass/Fail Criteria SQL
    SELECT
      CNTRCT_ID,
      TN_ICCID,
      TN_IMSI_MCC,
      TN_VALID_TO,
      TYPEOF(CNTRCT_ID) AS type_cntrct_id,
      TYPEOF(TN_ICCID) AS type_tn_iccid,
      TYPEOF(TN_VALID_TO) AS type_tn_valid_to
    FROM `project.dataset.sof_ta_iccid_einzeln`;
    /* Expected Result:
    CNTRCT_ID | TN_ICCID  | TN_IMSI_MCC | TN_VALID_TO | type_cntrct_id | type_tn_iccid | type_tn_valid_to
    ----------|-----------|-------------|-------------|----------------|---------------|------------------
    4001      | ICCID_STR | 262         | 2023-01-15  | INT64          | STRING        | DATE
    */
    ```

### Test Case 12: Airflow DAG Integration (External System Replacement)

*   **Purpose:** Verify that the Airflow DAG can successfully invoke the BigQuery Stored Procedure with correctly formatted parameters, replacing the KornShell orchestration.
*   **Setup:**
    1.  Ensure the Airflow environment is set up with a BigQuery connection (`your-gcp-project-id` and `dataset` in the DAG must be configured correctly).
    2.  Deploy the `dags/k_ausd_bp_ta_iccid_einzeln_dag.py` DAG to your Airflow environment.
    3.  Populate `project.dataset.sof_ta_bpr_basis` with test data (e.g., 1 row).
    4.  Clear logging tables and target table.
*   **Action:** Trigger the Airflow DAG `k_ausd_bp_ta_iccid_einzeln_migration` for a specific `ds` (e.g., '2023-01-15'). This can be done via the Airflow UI or CLI.
*   **Pass/Fail Criteria:**
    1.  **The Airflow task `call_r_ausd_bp_ta_iccid_einzeln` completes successfully** in the Airflow UI.
    2.  **`project.dataset.job_log` contains one entry:**
        *   `job_kennung` = 'DEFAULT_JOB'
        *   `eintrags_nr` = '001'
        *   `stichtag` = `DATE '2023-01-15'` (derived from Airflow's `ds`)
        *   `record_count` reflects the number of rows inserted (e.g., 1).
    3.  **`project.dataset.sof_ta_iccid_einzeln` contains the expected data** (1 row from the setup).

    ```python
    # This is a conceptual test, actual Airflow testing involves UI/CLI interaction.
    # Verification would be done by querying BigQuery after the DAG run.

    from google.cloud import bigquery
    import datetime

    # Assuming a BigQuery client is initialized (e.g., in a pytest fixture)
    # bq_client = bigquery.Client()

    def test_airflow_dag_execution(bq_client):
        # Assume DAG was triggered for '2023-01-15'
        expected_stichtag_date = datetime.date(2023, 1, 15)

        # Query job_log to verify parameters and record count
        query_job_log = f"""
        SELECT job_kennung, eintrags_nr, stichtag, record_count
        FROM `project.dataset.job_log`
        WHERE job_kennung = 'DEFAULT_JOB' AND eintrags_nr = '001'
        ORDER BY created_at DESC
        LIMIT 1
        """
        rows = list(bq_client.query(query_job_log).result())
        assert len(rows) == 1, "Expected one entry in job_log from Airflow run."
        assert rows[0].job_kennung == 'DEFAULT_JOB'
        assert rows[0].eintrags_nr == '001'
        assert rows[0].stichtag == expected_stichtag_date
        assert rows[0].record_count == 1 # Based on setup of 1 row in sof_ta_bpr_basis

        # Query target table to verify data insertion
        query_target_table = f"SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`"
        target_count = bq_client.query(query_target_table).result().single_value
        assert target_count == 1, "Expected 1 row in target table after Airflow run."
    ```