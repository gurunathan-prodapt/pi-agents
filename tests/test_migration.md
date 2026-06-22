As a senior data-migration QA engineer, I've developed a comprehensive suite of migration validation tests for the `k_ausd_bp_ta_iccid_einzeln.ksh` job, now migrated to a BigQuery Stored Procedure. These tests aim to ensure behavioral equivalence, data integrity, and correctness across all specified migration aspects.

The tests are organized into sections, each with a clear purpose, setup, action, and pass/fail criteria. Where applicable, runnable BigQuery SQL assertions are provided.

**Assumptions for Testing:**
*   BigQuery tables (`project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_bpr_basis`, `project.dataset.sof_ta_iccid_einzeln`, `project.dataset.job_error_log`, `project.dataset.job_run_result`) and the stored procedure (`project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`) have been deployed.
*   The `project.dataset` placeholder refers to the actual BigQuery project and dataset where the migrated assets reside.
*   Test data can be inserted into the source tables (`dwtk_meldungen`, `sof_ta_bpr_basis`) and cleared from target/log tables before each relevant test.
*   The `CURRENT_DATE()` function in BigQuery will be consistent during test execution for date derivations.

---

## Migration Validation Tests for `k_ausd_bp_ta_iccid_einzeln.ksh`

### Test Case 1: Successful Execution - Output Parity & Transformation Correctness (Standard Flow)

**Purpose:** Verify that with valid inputs, the migrated BigQuery Stored Procedure executes successfully, populates the target table `sof_ta_iccid_einzeln` with correct data, and logs the run results, matching the expected behavior of the legacy job. This covers core transformation logic, including `CASE` statements and status derivation.

**Setup:**
1.  Clear data from `project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_bpr_basis`, `project.dataset.sof_ta_iccid_einzeln`, `project.dataset.job_error_log`, `project.dataset.job_run_result`.
2.  Insert sample data into `dwtk_meldungen` to define `v_max_timecreated_datum`.
3.  Insert diverse sample data into `sof_ta_bpr_basis` covering various `bpr_id` values (31, 2759, 2800, 3848) and `slave_number` combinations, including cases where `valid_to` is before and after the `v_max_timecreated_datum`.

```sql
-- Clear tables
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_error_log`;
TRUNCATE TABLE `project.dataset.job_run_result`;

-- Insert data into dwtk_meldungen (to set v_max_timecreated_datum)
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00')); -- This will make v_max_timecreated_datum = '20230115'

-- Insert data into sof_ta_bpr_basis
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME)
VALUES
  ('C1001', 31, 0, 'ICCID_TN_01', '262', '01', 'HLR1', 'SI1', DATE('2023-01-10'), 'EID1', 'TypeA'), -- TN, valid_to < v_max_timecreated_datum -> L
  ('C1002', 31, 0, 'ICCID_TN_02', '262', '02', 'HLR2', 'SI2', DATE('2023-01-20'), 'EID2', 'TypeB'), -- TN, valid_to > v_max_timecreated_datum -> A
  ('C1003', 2759, 0, 'ICCID_TC_01', '262', '03', 'HLR3', 'SI3', DATE('2023-01-12'), 'EID3', 'TypeC'), -- TC, valid_to < v_max_timecreated_datum -> L
  ('C1004', 2759, 0, 'ICCID_TC_02', '262', '04', 'HLR4', 'SI4', DATE('2023-01-18'), 'EID4', 'TypeD'), -- TC, valid_to > v_max_timecreated_datum -> A
  ('C1005', 2800, 0, 'ICCID_TB_01', '262', '05', 'HLR5', 'SI5', DATE('2023-01-14'), 'EID5', 'TypeE'), -- TB, valid_to < v_max_timecreated_datum -> L
  ('C1006', 2800, 0, 'ICCID_TB_02', '262', '06', 'HLR6', 'SI6', DATE('2023-01-16'), 'EID6', 'TypeF'), -- TB, valid_to > v_max_timecreated_datum -> A
  ('C1007', 3848, 1, 'ICCID_MS1_01', '262', '07', 'HLR7', 'SI7', DATE('2023-01-11'), 'EID7', 'TypeG'), -- MS1, valid_to < v_max_timecreated_datum -> L
  ('C1008', 3848, 2, 'ICCID_MS2_01', '262', '08', 'HLR8', 'SI8', DATE('2023-01-17'), 'EID8', 'TypeH'), -- MS2, valid_to > v_max_timecreated_datum -> A
  ('C1009', 3848, 11, 'ICCID_OTHER', '262', '09', 'HLR9', 'SI9', DATE('2023-01-13'), 'EID9', 'TypeI'); -- Other bpr_id*100+slave_number, should be NULL
```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```sql
CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_A', 'ENTRY_1', '01012023', 0);
```

**Pass/Fail Criterion:**
1.  **Target Table Content:** `sof_ta_iccid_einzeln` contains 9 rows, and the data in each column (especially `STATUS` and `ICCID` for specific `bpr_id`s) matches the expected transformation logic.
2.  **Run Log:** `job_run_result` contains one entry for this run, with `record_count` = '9' and correct `datum_heute`, `datum_gestern`, `stichtag`.
3.  **Error Log:** `job_error_log` is empty.

```sql
-- Assertion 1: Check row count in target table
SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 9

-- Assertion 2: Check specific transformed values
SELECT
    CNTRCT_ID,
    TN_ICCID, TN_STATUS,
    TC_ICCID, TC_STATUS,
    TB_ICCID, TB_STATUS,
    MS1_ICCID, MS1_STATUS,
    MS2_ICCID, MS2_STATUS
FROM `project.dataset.sof_ta_iccid_einzeln`
ORDER BY CNTRCT_ID;
/* Expected results (simplified for key columns):
CNTRCT_ID | TN_ICCID    | TN_STATUS | TC_ICCID    | TC_STATUS | TB_ICCID    | TB_STATUS | MS1_ICCID    | MS1_STATUS | MS2_ICCID    | MS2_STATUS
----------|-------------|-----------|-------------|-----------|-------------|-----------|--------------|------------|--------------|------------
C1001     | ICCID_TN_01 | L         | NULL        | NULL      | NULL        | NULL      | NULL         | NULL       | NULL         | NULL
C1002     | ICCID_TN_02 | A         | NULL        | NULL      | NULL        | NULL      | NULL         | NULL       | NULL         | NULL
C1003     | NULL        | NULL      | ICCID_TC_01 | L         | NULL        | NULL      | NULL         | NULL       | NULL         | NULL
C1004     | NULL        | NULL      | ICCID_TC_02 | A         | NULL        | NULL      | NULL         | NULL       | NULL         | NULL
C1005     | NULL        | NULL      | NULL        | NULL      | ICCID_TB_01 | L         | NULL         | NULL       | NULL         | NULL
C1006     | NULL        | NULL      | NULL        | NULL      | ICCID_TB_02 | A         | NULL         | NULL       | NULL         | NULL
C1007     | NULL        | NULL      | NULL        | NULL      | NULL        | NULL      | ICCID_MS1_01 | L          | NULL         | NULL
C1008     | NULL        | NULL      | NULL        | NULL      | NULL        | NULL      | NULL         | NULL       | ICCID_MS2_01 | A
C1009     | NULL        | NULL      | NULL        | NULL      | NULL        | NULL      | NULL         | NULL       | NULL         | NULL
*/

-- Assertion 3: Check job_run_result
SELECT
    job_kennung,
    eintragsnr,
    stichtag,
    tab_name,
    datum_heute,
    datum_gestern,
    restart_value,
    record_count
FROM `project.dataset.job_run_result`;
/* Expected:
job_kennung | eintragsnr | stichtag   | tab_name         | datum_heute | datum_gestern | restart_value | record_count
------------|------------|------------|------------------|-------------|---------------|---------------|--------------
JOB_A       | ENTRY_1    | 01012023   | PoolBasisprodukt | CURRENT_DATE| YESTERDAY     | 0             | 9
*/

-- Assertion 4: Check job_error_log
SELECT COUNT(*) FROM `project.dataset.job_error_log`; -- Expected: 0
```

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the stored procedure correctly handles missing required parameters, logs the error, and exits without processing data, mirroring the shell script's `pruefeParameterGesetzt` logic.

**Setup:**
1.  Clear data from `project.dataset.sof_ta_iccid_einzeln`, `project.dataset.job_error_log`, `project.dataset.job_run_result`.
2.  (Optional) Insert some data into `sof_ta_iccid_einzeln` to ensure it's truncated if the procedure reaches that point (it shouldn't in this case).

**Action:**
Execute the BigQuery Stored Procedure with `p_JobKennung` as `NULL` or an empty string.

```sql
-- Example with NULL
CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`(NULL, 'ENTRY_1', '01012023', 0);

-- Example with empty string
-- CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('', 'ENTRY_1', '01012023', 0);
```

**Pass/Fail Criterion:**
1.  **Error Log:** `job_error_log` contains one entry with `err_nr = 193` and `err_arg = 'Jobkennung'`.
2.  **Target Table:** `sof_ta_iccid_einzeln` remains empty (or unchanged if it had data before).
3.  **Run Log:** `job_run_result` is empty.
4.  **Output Message:** The procedure returns a message like `FEHLER: 0 E 193 Jobkennung`.

```sql
-- Assertion 1: Check job_error_log
SELECT err_nr, err_arg FROM `project.dataset.job_error_log`; -- Expected: err_nr=193, err_arg='Jobkennung'

-- Assertion 2: Check target table
SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0

-- Assertion 3: Check job_run_result
SELECT COUNT(*) FROM `project.dataset.job_run_result`; -- Expected: 0
```

### Test Case 3: Parameter Validation - Invalid `p_Stichtag` Format

**Purpose:** Verify that the stored procedure correctly handles an invalid date format for `p_Stichtag`, logs the error, and exits, mirroring the shell script's `DWDate_Datum_Check` logic.

**Setup:**
1.  Clear data from `project.dataset.sof_ta_iccid_einzeln`, `project.dataset.job_error_log`, `project.dataset.job_run_result`.

**Action:**
Execute the BigQuery Stored Procedure with an invalid `p_Stichtag` format (e.g., '2023-01-01' instead of '01012023').

```sql
CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_A', 'ENTRY_1', '2023-01-01', 0);
```

**Pass/Fail Criterion:**
1.  **Error Log:** `job_error_log` contains one entry with `err_nr = 193` and `err_arg = 'Stichtag ungültig - Format DDMMYYYY erwartet'`.
2.  **Target Table:** `sof_ta_iccid_einzeln` remains empty.
3.  **Run Log:** `job_run_result` is empty.
4.  **Output Message:** The procedure returns a message like `FEHLER: 0 E 193 Stichtag ungültig - Format DDMMYYYY erwartet`.

```sql
-- Assertion 1: Check job_error_log
SELECT err_nr, err_arg FROM `project.dataset.job_error_log`; -- Expected: err_nr=193, err_arg='Stichtag ungültig - Format DDMMYYYY erwartet'

-- Assertion 2: Check target table
SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0

-- Assertion 3: Check job_run_result
SELECT COUNT(*) FROM `project.dataset.job_run_result`; -- Expected: 0
```

### Test Case 4: `p_wiederanlaufWert` Default Handling

**Purpose:** Verify that `p_wiederanlaufWert` defaults to 0 if `NULL` is passed, as specified in the migration design.

**Setup:**
1.  Clear data from `project.dataset.job_run_result`.
2.  Insert minimal data into source tables to allow successful execution (e.g., one row in `dwtk_meldungen`, one row in `sof_ta_bpr_basis`).

**Action:**
Execute the BigQuery Stored Procedure with `p_wiederanlaufWert` as `NULL`.

```sql
-- Setup for minimal data
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_error_log`;
TRUNCATE TABLE `project.dataset.job_run_result`;

INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-01 00:00:00'));
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME)
VALUES ('C1000', 31, 0, 'ICCID_TEST', '111', '22', 'HLR_T', 'SI_T', DATE('2023-01-02'), 'EID_T', 'TypeT');

CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_B', 'ENTRY_2', '01012023', NULL);
```

**Pass/Fail Criterion:**
1.  **Run Log:** `job_run_result` contains one entry where `restart_value` is `0`.

```sql
-- Assertion: Check restart_value in job_run_result
SELECT restart_value FROM `project.dataset.job_run_result` WHERE job_kennung = 'JOB_B'; -- Expected: 0
```

### Test Case 5: Date Derivation - `v_datum_heute` and `v_datum_gestern`

**Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` are correctly derived using `CURRENT_DATE()` and `DATE_SUB()`.

**Setup:**
1.  Clear data from `project.dataset.job_run_result`.
2.  Insert minimal data into source tables to allow successful execution.

**Action:**
Execute the BigQuery Stored Procedure.

```sql
-- Setup for minimal data (same as Test Case 4)
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_error_log`;
TRUNCATE TABLE `project.dataset.job_run_result`;

INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-01 00:00:00'));
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME)
VALUES ('C1000', 31, 0, 'ICCID_TEST', '111', '22', 'HLR_T', 'SI_T', DATE('2023-01-02'), 'EID_T', 'TypeT');

CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_C', 'ENTRY_3', '01012023', 0);
```

**Pass/Fail Criterion:**
1.  **Run Log:** `job_run_result` contains one entry where `datum_heute` is `CURRENT_DATE()` and `datum_gestern` is `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.

```sql
-- Assertion: Check datum_heute and datum_gestern in job_run_result
SELECT
    datum_heute = CURRENT_DATE() AS is_heute_correct,
    datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS is_gestern_correct
FROM `project.dataset.job_run_result`
WHERE job_kennung = 'JOB_C';
-- Expected: is_heute_correct=TRUE, is_gestern_correct=TRUE
```

### Test Case 6: `v_max_timecreated_datum` Derivation and Default

**Purpose:** Verify that `v_max_timecreated_datum` is correctly derived from `dwtk_meldungen` or defaults to '19000101' if no matching records are found. This is critical for the `STATUS` column logic.

**Setup:**
1.  Clear data from `project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_iccid_einzeln`, `project.dataset.job_run_result`.
2.  **Scenario A (Default):** `dwtk_meldungen` is empty or has no matching `job_kennung`.
3.  **Scenario B (Derived):** `dwtk_meldungen` has a matching `job_kennung` with `timecreated`.

**Action:**
Execute the stored procedure for both scenarios.

```sql
-- Scenario A: No matching record in dwtk_meldungen (should default to '19000101')
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_run_result`;

INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME)
VALUES ('C_DEF', 31, 0, 'ICCID_DEF', '111', '22', 'HLR_D', 'SI_D', DATE('1950-01-01'), 'EID_D', 'TypeD'); -- valid_to < '19000101' is false, so 'A'

CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_D_A', 'ENTRY_4', '01012023', 0);

-- Scenario B: Matching record in dwtk_meldungen (should derive '20230120')
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_run_result`;

INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-20 10:00:00'));
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) -- Another record to test MAX
VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00'));

INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME)
VALUES
  ('C_DRV1', 31, 0, 'ICCID_DRV1', '111', '22', 'HLR_D1', 'SI_D1', DATE('2023-01-10'), 'EID_D1', 'TypeD1'), -- valid_to < '20230120' -> L
  ('C_DRV2', 31, 0, 'ICCID_DRV2', '111', '22', 'HLR_D2', 'SI_D2', DATE('2023-01-25'), 'EID_D2', 'TypeD2'); -- valid_to > '20230120' -> A

CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_D_B', 'ENTRY_5', '01012023', 0);
```

**Pass/Fail Criterion:**
1.  **Scenario A:** The `STATUS` column in `sof_ta_iccid_einzeln` for `CNTRCT_ID = 'C_DEF'` should be 'A' (since `DATE('1950-01-01')` is not `<= PARSE_DATE('%Y%m%d', '19000101')` is false, so it's 'A').
2.  **Scenario B:**
    *   For `CNTRCT_ID = 'C_DRV1'`, `STATUS` should be 'L'.
    *   For `CNTRCT_ID = 'C_DRV2'`, `STATUS` should be 'A'.

```sql
-- Assertion 1 (Scenario A): Check status for default v_max_timecreated_datum
SELECT TN_STATUS FROM `project.dataset.sof_ta_iccid_einzeln` WHERE CNTRCT_ID = 'C_DEF'; -- Expected: 'A'

-- Assertion 2 (Scenario B): Check status for derived v_max_timecreated_datum
SELECT CNTRCT_ID, TN_STATUS FROM `project.dataset.sof_ta_iccid_einzeln` WHERE CNTRCT_ID IN ('C_DRV1', 'C_DRV2') ORDER BY CNTRCT_ID;
/* Expected:
CNTRCT_ID | TN_STATUS
----------|-----------
C_DRV1    | L
C_DRV2    | A
*/
```

### Test Case 7: NULL Handling in Source Data

**Purpose:** Verify that NULL values in source columns are correctly propagated or handled by `CASE` statements, resulting in NULLs in the target table where appropriate.

**Setup:**
1.  Clear data from `project.dataset.sof_ta_iccid_einzeln`.
2.  Insert data into `sof_ta_bpr_basis` where some `iccid`, `imsi_mcc`, `valid_to`, `E_ID`, `CARD_TYPE_NAME` are NULL.

```sql
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_error_log`;
TRUNCATE TABLE `project.dataset.job_run_result`;

INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00'));

INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME)
VALUES
  ('C_NULL1', 31, 0, NULL, '262', '01', 'HLR1', 'SI1', DATE('2023-01-10'), 'EID1', 'TypeA'), -- ICCID is NULL
  ('C_NULL2', 2759, 0, 'ICCID_TC_01', NULL, '03', 'HLR3', 'SI3', DATE('2023-01-12'), 'EID3', NULL), -- IMSI_MCC, CARD_TYPE_NAME are NULL
  ('C_NULL3', 3848, 1, 'ICCID_MS1_01', '262', '07', 'HLR7', 'SI7', NULL, NULL, 'TypeG'); -- VALID_TO, E_ID are NULL
```

**Action:**
Execute the BigQuery Stored Procedure.

```sql
CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_E', 'ENTRY_6', '01012023', 0);
```

**Pass/Fail Criterion:**
1.  **Target Table Content:** Verify that the NULL values from the source are correctly reflected in the corresponding target columns, and that `STATUS` derivation handles `NULL` `valid_to` as expected (it will be `NULL` if `valid_to` is `NULL`).

```sql
-- Assertion: Check NULL propagation
SELECT
    CNTRCT_ID,
    TN_ICCID, TN_IMSI_MCC, TN_CARD_TYPE_NAME, TN_VALID_TO, TN_E_ID, TN_STATUS,
    TC_ICCID, TC_IMSI_MCC, TC_CARD_TYPE_NAME, TC_VALID_TO, TC_E_ID, TC_STATUS,
    MS1_ICCID, MS1_IMSI_MCC, MS1_CARD_TYPE_NAME, MS1_VALID_TO, MS1_E_ID, MS1_STATUS
FROM `project.dataset.sof_ta_iccid_einzeln`
ORDER BY CNTRCT_ID;
/* Expected (simplified):
CNTRCT_ID | TN_ICCID | TN_IMSI_MCC | TN_CARD_TYPE_NAME | TN_VALID_TO | TN_E_ID | TN_STATUS | TC_ICCID    | TC_IMSI_MCC | TC_CARD_TYPE_NAME | TC_VALID_TO | TC_E_ID | TC_STATUS | MS1_ICCID    | MS1_IMSI_MCC | MS1_CARD_TYPE_NAME | MS1_VALID_TO | MS1_E_ID | MS1_STATUS
----------|----------|-------------|-------------------|-------------|---------|-----------|-------------|-------------|-------------------|-------------|---------|-----------|--------------|--------------|--------------------|--------------|----------|------------
C_NULL1   | NULL     | 262         | TypeA             | 2023-01-10  | EID1    | L         | NULL        | NULL        | NULL              | NULL        | NULL    | NULL      | NULL         | NULL         | NULL               | NULL         | NULL     | NULL
C_NULL2   | NULL     | NULL        | NULL              | NULL        | NULL    | NULL      | ICCID_TC_01 | NULL        | NULL              | 2023-01-12  | EID3    | L         | NULL         | NULL         | NULL               | NULL         | NULL     | NULL
C_NULL3   | NULL     | NULL        | NULL              | NULL        | NULL    | NULL      | NULL        | NULL        | NULL              | NULL        | NULL    | NULL      | ICCID_MS1_01 | 262          | TypeG              | NULL         | NULL     | NULL
*/
```

### Test Case 8: Idempotency - Running the Job Multiple Times

**Purpose:** Verify that running the stored procedure multiple times with the same inputs produces the same final state in the target table and logs, demonstrating idempotency. This is crucial due to the `TRUNCATE TABLE` operation.

**Setup:**
1.  Clear all relevant tables.
2.  Insert initial data into `dwtk_meldungen` and `sof_ta_bpr_basis`.

```sql
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_error_log`;
TRUNCATE TABLE `project.dataset.job_run_result`;

INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00'));
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id, bpr_id, slave_number, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME)
VALUES ('C_IDEM', 31, 0, 'ICCID_IDEM', '111', '22', 'HLR_I', 'SI_I', DATE('2023-01-10'), 'EID_I', 'TypeI');
```

**Action:**
Execute the BigQuery Stored Procedure twice with the same parameters.

```sql
CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_F', 'ENTRY_7', '01012023', 0);
CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_F', 'ENTRY_7', '01012023', 0); -- Run again
```

**Pass/Fail Criterion:**
1.  **Target Table Content:** `sof_ta_iccid_einzeln` contains exactly one row, and its content is identical to the expected output for a single run.
2.  **Run Log:** `job_run_result` contains two entries for `JOB_F`, each with `record_count` = '1'.

```sql
-- Assertion 1: Check row count in target table
SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 1

-- Assertion 2: Check job_run_result entries
SELECT COUNT(*) FROM `project.dataset.job_run_result` WHERE job_kennung = 'JOB_F'; -- Expected: 2
SELECT record_count FROM `project.dataset.job_run_result` WHERE job_kennung = 'JOB_F'; -- All should be '1'
```

### Test Case 9: Schema and Data Type Validation

**Purpose:** Verify that the target table `sof_ta_iccid_einzeln` has the correct schema and data types as defined in the DDL and expected by the transformation.

**Setup:**
1.  Ensure the DDL for `project.dataset.sof_ta_iccid_einzeln` is applied.
2.  Execute a successful run of the stored procedure (e.g., using the setup from Test Case 1).

**Action:**
Query the BigQuery information schema for the target table.

```sql
-- This is a metadata query, not part of the procedure execution.
-- It can be run after any successful execution.
```

**Pass/Fail Criterion:**
1.  The schema of `project.dataset.sof_ta_iccid_einzeln` matches the DDL provided in the migration design document, specifically for column names and data types (e.g., `CNTRCT_ID` as `STRING`, `TN_VALID_TO` as `DATE`, `MS1_STATUS` as `STRING`).

```sql
-- Assertion: Check table schema
SELECT
    column_name,
    data_type
FROM
    `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'sof_ta_iccid_einzeln'
ORDER BY
    ordinal_position;
/* Expected (partial):
column_name   | data_type
--------------|-----------
CNTRCT_ID     | STRING
TN_ICCID      | STRING
TN_IMSI_MCC   | STRING
...
TN_VALID_TO   | DATE
...
MS10_CARD_TYPE_NAME | STRING
*/
```

### Test Case 10: Empty Source Tables

**Purpose:** Verify that the stored procedure handles empty source tables gracefully, resulting in an empty target table and a run log entry with a record count of 0.

**Setup:**
1.  Clear all relevant tables: `dwtk_meldungen`, `sof_ta_bpr_basis`, `sof_ta_iccid_einzeln`, `job_error_log`, `job_run_result`.

```sql
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;
TRUNCATE TABLE `project.dataset.job_error_log`;
TRUNCATE TABLE `project.dataset.job_run_result`;
```

**Action:**
Execute the BigQuery Stored Procedure with empty source tables.

```sql
CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`('JOB_G', 'ENTRY_8', '01012023', 0);
```

**Pass/Fail Criterion:**
1.  **Target Table:** `sof_ta_iccid_einzeln` is empty.
2.  **Run Log:** `job_run_result` contains one entry with `record_count` = '0'.
3.  **Error Log:** `job_error_log` is empty.

```sql
-- Assertion 1: Check target table
SELECT COUNT(*) FROM `project.dataset.sof_ta_iccid_einzeln`; -- Expected: 0

-- Assertion 2: Check job_run_result
SELECT record_count FROM `project.dataset.job_run_result` WHERE job_kennung = 'JOB_G'; -- Expected: '0'

-- Assertion 3: Check job_error_log
SELECT COUNT(*) FROM `project.dataset.job_error_log`; -- Expected: 0
```

---