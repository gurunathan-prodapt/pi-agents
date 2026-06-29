Here is a comprehensive suite of migration-validation tests designed to verify that the Google Cloud Composer (Airflow) and BigQuery target implementation is behaviorally equivalent to the legacy KornShell/Oracle implementation.

---

# Migration Validation Test Suite

## Test Case 1: End-to-End Happy Path & Output Parity
### Purpose
Verify that when valid staging data is present, executing the BigQuery stored procedure correctly processes, transforms, and loads the data into the target table `PoolBasisprodukt` with correct metadata, and writes a success record to `job_audit_log`.

### Setup
1. Clear any existing data in `PoolBasisprodukt` and `job_audit_log` for the test date.
2. Populate the staging table `PoolBasisprodukt_Staging` with mock records for `stichtag = '2023-12-31'`.

```sql
-- Clean up
DELETE FROM `project.dataset.PoolBasisprodukt` WHERE stichtag = '2023-12-31';
DELETE FROM `project.dataset.PoolBasisprodukt_Staging` WHERE stichtag = '2023-12-31';
DELETE FROM `project.dataset.job_audit_log` WHERE stichtag_from = '2023-12-31';

-- Insert Mock Staging Data
INSERT INTO `project.dataset.PoolBasisprodukt_Staging` (stichtag, contract_id, distribution_channel, account_balance)
VALUES 
('2023-12-31', 'CON-10001', 'ONLINE', 1500.50),
('2023-12-31', 'CON-10002', 'BRANCH', 25000.00),
('2023-12-31', 'CON-10003', 'PARTNER', 0.00);
```

### Action
Execute the stored procedure with valid parameters matching the staging date (`31122023`):

```sql
CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`(
  'JOB_TEST_01', 
  'ENTRY_01', 
  '31122023', 
  '0'
);
```

### Pass/Fail Criterion
**Pass Criteria:**
1. Target table `PoolBasisprodukt` contains exactly 3 records for `stichtag = '2023-12-31'`.
2. Target columns `datum_heute` and `datum_gestern` are correctly populated as `2023-12-31` and `2023-12-30` respectively.
3. Metadata columns (`job_kennung`, `eintrags_nr`) match the input parameters.
4. `job_audit_log` contains exactly one entry for this run with `status = 'S'`, `record_count = 3`, and `active_flag = 'N'`.

**Fail Criteria:**
- Any row count mismatch, incorrect date calculations, or missing audit log entries.

#### Validation SQL Assertion
```sql
-- Assert Target Table Parity
ASSERT (
  SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt` 
  WHERE stichtag = '2023-12-31'
) = 3 
AS 'ERROR: Target table row count mismatch!';

ASSERT (
  SELECT COUNT(DISTINCT datum_gestern) FROM `project.dataset.PoolBasisprodukt` 
  WHERE stichtag = '2023-12-31' AND datum_gestern = '2023-12-30'
) = 1 
AS 'ERROR: Relative date calculation (datum_gestern) is incorrect!';

-- Assert Audit Log Parity
ASSERT (
  SELECT COUNT(*) FROM `project.dataset.job_audit_log`
  WHERE stichtag_from = '2023-12-31' 
    AND status = 'S' 
    AND record_count = 3
    AND job_kennung = 'JOB_TEST_01'
) = 1 
AS 'ERROR: Audit log entry missing or incorrect!';
```

---

## Test Case 2: Idempotency & Target Table Cleanup
### Purpose
Verify that the job is fully idempotent. Running the stored procedure multiple times for the same `Stichtag` must not result in duplicate records in the target table. It must clean up previous runs for that date before inserting new ones (matching the legacy behavior of overwriting target partitions).

### Setup
1. Populate staging table with 3 records for `2023-12-31`.
2. Run the stored procedure once to establish a baseline state.
3. Modify the staging table (e.g., change the number of records to 2).

```sql
-- Establish baseline
DELETE FROM `project.dataset.PoolBasisprodukt_Staging` WHERE stichtag = '2023-12-31';
INSERT INTO `project.dataset.PoolBasisprodukt_Staging` (stichtag, contract_id, distribution_channel, account_balance)
VALUES 
('2023-12-31', 'CON-10001', 'ONLINE', 1500.50),
('2023-12-31', 'CON-10002', 'BRANCH', 25000.00);

-- Run baseline
CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`('JOB_IDEMP', 'ENTRY_01', '31122023', '0');

-- Modify staging (Simulating a re-run with updated source data)
DELETE FROM `project.dataset.PoolBasisprodukt_Staging` WHERE stichtag = '2023-12-31';
INSERT INTO `project.dataset.PoolBasisprodukt_Staging` (stichtag, contract_id, distribution_channel, account_balance)
VALUES 
('2023-12-31', 'CON-99999', 'MOBILE', 999.99);
```

### Action
Execute the stored procedure again for the same `Stichtag` (`31122023`):

```sql
CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`(
  'JOB_IDEMP', 
  'ENTRY_01', 
  '31122023', 
  '0'
);
```

### Pass/Fail Criterion
**Pass Criteria:**
1. Target table `PoolBasisprodukt` contains exactly 1 record (the new record `CON-99999`).
2. The old records (`CON-10001`, `CON-10002`) are completely removed.
3. The audit log contains a new entry reflecting `record_count = 1`.

**Fail Criteria:**
- Target table contains duplicate records or a mix of old and new records (totaling 3 records).

#### Validation SQL Assertion
```sql
ASSERT (
  SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt` 
  WHERE stichtag = '2023-12-31'
) = 1 
AS 'ERROR: Idempotency check failed! Old records were not purged.';

ASSERT (
  SELECT contract_id FROM `project.dataset.PoolBasisprodukt` 
  WHERE stichtag = '2023-12-31'
) = 'CON-99999' 
AS 'ERROR: Target table contains incorrect data after re-run.';
```

---

## Test Case 3: Parameter Validation & Error Handling
### Purpose
Verify that invalid parameter inputs (missing mandatory fields or incorrect date formats) are rejected by both the Airflow DAG validation layer and the BigQuery stored procedure, matching the legacy `DWDate_Datum_Check` and `pruefeParameterGesetzt` behaviors.

### Setup
No database setup required.

### Action & Concrete Pass/Fail Criteria

#### Test Scenario 3.1: Invalid Date Format (Stored Procedure)
*   **Action**: Call stored procedure with an invalid date format (e.g., YYYY-MM-DD instead of DDMMYYYY).
    ```sql
    DECLARE error_occurred BOOL DEFAULT FALSE;
    BEGIN
      CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`('JOB_ERR', 'ENTRY_01', '2023-12-31', '0');
    EXCEPTION WHEN ERROR THEN
      SET error_occurred = TRUE;
    END;
    ASSERT error_occurred = TRUE AS 'ERROR: Stored procedure accepted invalid date format!';
    ```
*   **Pass Criterion**: Stored procedure raises an exception containing the message: `Invalid Stichtag format. Expected DDMMYYYY.`

#### Test Scenario 3.2: Missing Mandatory Parameters (Stored Procedure)
*   **Action**: Call stored procedure with a `NULL` Job ID.
    ```sql
    DECLARE error_occurred BOOL DEFAULT FALSE;
    BEGIN
      CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`(NULL, 'ENTRY_01', '31122023', '0');
    EXCEPTION WHEN ERROR THEN
      SET error_occurred = TRUE;
    END;
    ASSERT error_occurred = TRUE AS 'ERROR: Stored procedure accepted NULL Job ID!';
    ```
*   **Pass Criterion**: Stored procedure raises an exception containing the message: `Missing mandatory parameter: p_JobKennung`.

#### Test Scenario 3.3: Airflow DAG Parameter Validation (Pytest)
*   **Action**: Run a unit test against the Airflow DAG's validation function using invalid parameters.
*   **Pass/Fail Code (pytest)**:
    ```python
    import pytest
    from airflow.exceptions import AirflowFailException
    # Import the validation function from the DAG file
    from src.dags.dag_k_ausd_bp_ta_cntrct_dist import validate_params

    def test_airflow_validation_invalid_date():
        mock_context = {
            "params": {
                "p_JobKennung": "JOB_1",
                "p_EintragsNr": "ENTRY_1",
                "p_Stichtag": "20231231", # Invalid format (YYYYMMDD)
                "p_wiederanlaufWert": ""
            },
            "ti": type('MockTI', (object,), {'xcom_push': lambda self, key, value: None})()
        }
        with pytest.raises(AirflowFailException) as exc_info:
            validate_params(**mock_context)
        assert "Invalid p_Stichtag format. Expected DDMMYYYY." in str(exc_info.value)

    def test_airflow_validation_missing_param():
        mock_context = {
            "params": {
                "p_JobKennung": "", # Missing
                "p_EintragsNr": "ENTRY_1",
                "p_Stichtag": "31122023",
                "p_wiederanlaufWert": ""
            },
            "ti": type('MockTI', (object,), {'xcom_push': lambda self, key, value: None})()
        }
        with pytest.raises(AirflowFailException) as exc_info:
            validate_params(**mock_context)
        assert "Missing mandatory parameter: p_JobKennung" in str(exc_info.value)
    ```

---

## Test Case 4: Date Calculation Logic (Leap Years & Month Boundaries)
### Purpose
Verify that relative date calculations (`v_datum_heute`, `v_datum_gestern`) handle leap years and month boundaries correctly, replacing the legacy `gestern.ksh` utility.

### Setup
Insert staging records for boundary dates:
1. Leap Year Leap Day: `29022024` (Stichtag `2024-02-29`)
2. First Day of Year: `01012024` (Stichtag `2024-01-01`)

```sql
DELETE FROM `project.dataset.PoolBasisprodukt_Staging` WHERE stichtag IN ('2024-02-29', '2024-01-01');
DELETE FROM `project.dataset.PoolBasisprodukt` WHERE stichtag IN ('2024-02-29', '2024-01-01');

INSERT INTO `project.dataset.PoolBasisprodukt_Staging` (stichtag, contract_id, distribution_channel, account_balance)
VALUES 
('2024-02-29', 'CON-LEAP', 'ONLINE', 100.00),
('2024-01-01', 'CON-NY', 'BRANCH', 200.00);
```

### Action
Execute the stored procedure for both dates:

```sql
-- Run for Leap Year boundary
CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`('JOB_LEAP', 'ENTRY_01', '29022024', '0');

-- Run for New Year boundary
CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`('JOB_NY', 'ENTRY_01', '01012024', '0');
```

### Pass/Fail Criterion
**Pass Criteria:**
1. For Leap Year run: `datum_heute` must be `2024-02-29` and `datum_gestern` must be `2024-02-28`.
2. For New Year run: `datum_heute` must be `2024-01-01` and `datum_gestern` must be `2023-12-31`.

#### Validation SQL Assertion
```sql
-- Assert Leap Year Calculations
ASSERT (
  SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt`
  WHERE stichtag = '2024-02-29' 
    AND datum_heute = '2024-02-29' 
    AND datum_gestern = '2024-02-28'
) = 1 AS 'ERROR: Leap year date calculation failed!';

-- Assert New Year Calculations
ASSERT (
  SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt`
  WHERE stichtag = '2024-01-01' 
    AND datum_heute = '2024-01-01' 
    AND datum_gestern = '2023-12-31'
) = 1 AS 'ERROR: Month/Year boundary date calculation failed!';
```

---

## Test Case 5: Audit Log Integrity on Failure
### Purpose
Verify that if the core transformation fails (e.g., due to database issues or schema constraints), a failure record is written to `job_audit_log` with status `'F'` and the error is propagated to the orchestrator.

### Setup
To simulate a database failure, we will temporarily rename the target table or alter a column constraint, or run a test harness that forces an exception inside the transaction block. 

*Alternative approach*: Since we cannot easily rename tables dynamically in a standard test run, we can test the exception block logic by passing a value that causes an arithmetic overflow or constraint violation during execution.

```sql
-- Setup staging data with an invalid balance that exceeds NUMERIC precision (if applicable) 
-- or simply force a failure by dropping the staging table temporarily.
ALTER TABLE `project.dataset.PoolBasisprodukt_Staging` RENAME TO `project.dataset.PoolBasisprodukt_Staging_TEMP`;
```

### Action
Execute the stored procedure:

```sql
DECLARE error_thrown BOOL DEFAULT FALSE;
BEGIN
  CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`('JOB_FAIL_TEST', 'ENTRY_01', '15052023', '0');
EXCEPTION WHEN ERROR THEN
  SET error_thrown = TRUE;
END;
```

### Pass/Fail Criterion
**Pass Criteria:**
1. The stored procedure execution fails and throws an exception back to the caller (`error_thrown` is `TRUE`).
2. An entry is written to `job_audit_log` with `status = 'F'`, `job_kennung = 'JOB_FAIL_TEST'`, and `stichtag_from = '2023-05-15'`.

#### Validation SQL Assertion
```sql
-- Restore the table first to allow assertions to run
ALTER TABLE `project.dataset.PoolBasisprodukt_Staging_TEMP` RENAME TO `project.dataset.PoolBasisprodukt_Staging`;

-- Assert failure was logged
ASSERT (
  SELECT COUNT(*) FROM `project.dataset.job_audit_log`
  WHERE job_kennung = 'JOB_FAIL_TEST' 
    AND status = 'F'
    AND stichtag_from = '2023-05-15'
) = 1 AS 'ERROR: Failure was not logged in job_audit_log!';
```

---

## Test Case 6: Schema and Data Quality Assertions
### Purpose
Verify that the target table `PoolBasisprodukt` conforms to strict schema constraints, nullability rules, and data quality standards.

### Setup
Ensure the target table has been populated by running Test Case 1.

### Action
Execute structural and data quality validation queries.

### Pass/Fail Criterion
**Pass Criteria:**
1. No records in `PoolBasisprodukt` have `NULL` values in mandatory columns (`stichtag`, `contract_id`, `datum_heute`, `datum_gestern`).
2. All `account_balance` values are non-negative (assuming business logic dictates balances cannot be negative, or matches legacy domain rules).
3. No duplicate `contract_id` values exist for the same `stichtag`.

#### Validation SQL Assertion
```sql
-- 1. Nullability Check
ASSERT (
  SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt`
  WHERE stichtag IS NULL 
     OR contract_id IS NULL 
     OR datum_heute IS NULL 
     OR datum_gestern IS NULL
) = 0 AS 'ERROR: Mandatory columns contain NULL values!';

-- 2. Uniqueness Check (Primary Key Constraint Validation)
ASSERT (
  SELECT COUNT(*) FROM (
    SELECT contract_id, stichtag, COUNT(*) 
    FROM `project.dataset.PoolBasisprodukt`
    GROUP BY contract_id, stichtag
    HAVING COUNT(*) > 1
  )
) = 0 AS 'ERROR: Duplicate contract_id found for the same stichtag!';
```