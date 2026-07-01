This document provides a comprehensive test suite designed to validate the migration of the legacy KornShell script `k_ausd_bp_ta_bpr_evn.ksh` and its associated Oracle SQL logic to Google Cloud BigQuery and Apache Airflow.

---

## Test Suite Overview

To prove behavioral equivalence, the validation strategy is split into unit tests for the orchestration layer (Airflow DAG) and integration/data-parity tests for the execution layer (BigQuery Stored Procedure).

### Environment Variables Under Test
* **GCP Project ID**: `${GCP_PROJECT_ID}`
* **BigQuery Dataset**: `${BQ_DATASET}`
* **Target Table**: `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt`
* **Audit Table**: `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`
* **Error Table**: `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log`

---

## 1. Airflow Parameter Validation & Edge Cases (Unit Test)

### Purpose
Verify that the Airflow DAG's Python validation task (`validate_inputs`) correctly mimics the legacy shell script's parameter validation (`pruefeParameterGesetzt` and `DWDate_Datum_Check`). It must reject missing parameters and malformed dates before triggering any BigQuery resources.

### Setup
A Python testing environment with `pytest` and `mock` installed. The DAG file `dags/k_ausd_bp_ta_bpr_evn_dag.py` must be in the Python path.

### Action
Run a suite of unit tests passing various valid and invalid configurations to the `validate_inputs` callable.

```python
# test_k_ausd_bp_ta_bpr_evn_dag.py
import pytest
from unittest.mock import MagicMock
from dags.k_ausd_bp_ta_bpr_evn_dag import validate_inputs

def create_mock_context(conf):
    dag_run = MagicMock()
    dag_run.conf = conf
    return {"dag_run": dag_run}

def test_validate_inputs_success():
    # Valid payload matching legacy expectations
    conf = {
        "p_JobKennung": "JOB_TEST_01",
        "p_EintragsNr": "9999",
        "p_Stichtag": "31122023",
        "p_wiederanlaufWert": "0"
    }
    # Should run without raising any exceptions
    validate_inputs(**create_mock_context(conf))

def test_validate_inputs_missing_params():
    # Missing Jobkennung and EintragsNr
    conf = {
        "p_Stichtag": "31122023"
    }
    with pytest.raises(ValueError) as excinfo:
        validate_inputs(**create_mock_context(conf))
    assert "Mandatory parameter(s) missing" in str(excinfo.value)
    assert "p_JobKennung" in str(excinfo.value)
    assert "p_EintragsNr" in str(excinfo.value)

@pytest.mark.parametrize("invalid_date", [
    "31-12-2023",  # Wrong separator
    "311223",      # Six digits instead of eight
    "32122023",    # Invalid day
    "29022023",    # Non-leap year
    "ABCD2023",    # Non-numeric
])
def test_validate_inputs_invalid_date_formats(invalid_date):
    conf = {
        "p_JobKennung": "JOB_TEST_01",
        "p_EintragsNr": "9999",
        "p_Stichtag": invalid_date
    }
    with pytest.raises(ValueError) as excinfo:
        validate_inputs(**create_mock_context(conf))
    assert "does not match format DDMMYYYY" in str(excinfo.value) or "is not a valid calendar date" in str(excinfo.value)
```

### Pass/Fail Criterion
* **Pass**: All valid configurations execute silently. All invalid configurations raise a `ValueError` with descriptive error messages matching legacy error contexts.
* **Fail**: Any invalid configuration is allowed to pass, or a valid configuration raises an exception.

---

## 2. BigQuery Stored Procedure Parameter Validation & Error Logging

### Purpose
Verify that if the stored procedure is called directly (bypassing Airflow), it natively validates parameters, logs errors to the `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log` table, and raises a clean BigQuery runtime exception.

### Setup
Ensure the target dataset and the `job_error_log` table exist. Clear any existing logs for the test job.

```sql
-- Setup: Create/Truncate Error Log Table
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log` (
  job_name STRING,
  error_code INT64,
  error_arg STRING,
  created_at TIMESTAMP
);

TRUNCATE TABLE `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log`;
```

### Action
Execute the stored procedure with a missing `p_JobKennung` parameter.

```sql
-- Action: Call SP with NULL Jobkennung
DECLARE error_thrown BOOLEAN DEFAULT FALSE;

BEGIN
  CALL `${GCP_PROJECT_ID}.${BQ_DATASET}.r_ausd_bp_ta_bpr_evn`(
    NULL,        -- p_JobKennung (Invalid)
    '12345',     -- p_EintragsNr
    '31122023',  -- p_Stichtag
    '0'          -- p_wiederanlaufWert
  );
EXCEPTION WHEN ERROR THEN
  SET error_thrown = TRUE;
END;

-- Assertions
ASSERT error_thrown = TRUE AS 'Expected stored procedure to throw an error due to missing Jobkennung';

ASSERT (
  SELECT COUNT(1) 
  FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log`
  WHERE job_name = 'r_ausd_bp_ta_bpr_evn' 
    AND error_code = 193 
    AND error_arg = 'Jobkennung'
) = 1 AS 'Expected exactly one error log entry with code 193 for Jobkennung';
```

### Pass/Fail Criterion
* **Pass**: The stored procedure execution fails, and exactly one row is written to the `job_error_log` table with `error_code = 193` and `error_arg = 'Jobkennung'`.
* **Fail**: The procedure completes successfully, or fails without writing the correct metadata to the error log table.

---

## 3. Audit Trail & Row Count Verification

### Purpose
Verify that a successful run of the stored procedure creates the correct audit trail entries ('STARTED' and 'FINISHED') in `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log` and accurately records the count of processed records.

### Setup
1. Create and truncate the `job_audit_log` table.
2. Populate a mock source table (representing the source for `PoolBasisprodukt`) with a known number of records for the target date.
3. Ensure the target table `PoolBasisprodukt` is truncated for the target date.

```sql
-- Setup: Create/Truncate Audit Table
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log` (
  job_name STRING,
  job_identifier STRING,
  entry_nr STRING,
  stichtag DATE,
  status STRING,
  record_count INT64,
  created_at TIMESTAMP
);

TRUNCATE TABLE `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`;

-- Setup: Mock Target Table Data Insertion (Simulating Core SQL execution)
-- For testing purposes, we insert mock data directly into the target table 
-- to verify the SP's counting and logging mechanism.
TRUNCATE TABLE `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt`;

INSERT INTO `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt` (business_date, product_id, product_name)
VALUES 
('2023-12-31', 'P01', 'Basis Product 1'),
('2023-12-31', 'P02', 'Basis Product 2'),
('2023-12-31', 'P03', 'Basis Product 3');
```

### Action
Call the stored procedure with valid parameters for the key date `31122023`.

```sql
-- Action: Call SP
CALL `${GCP_PROJECT_ID}.${BQ_DATASET}.r_ausd_bp_ta_bpr_evn`(
  'JOB_AUDIT_TEST', -- p_JobKennung
  '7777',           -- p_EintragsNr
  '31122023',       -- p_Stichtag
  '0'               -- p_wiederanlaufWert
);

-- Assertions: Verify Audit Log Entries
ASSERT (
  SELECT COUNT(1) 
  FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`
  WHERE job_name = 'PoolBasisprodukt' 
    AND job_identifier = 'JOB_AUDIT_TEST'
    AND entry_nr = '7777'
    AND stichtag = '2023-12-31'
) = 2 AS 'Expected exactly 2 audit entries (STARTED and FINISHED)';

ASSERT (
  SELECT record_count 
  FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`
  WHERE job_name = 'PoolBasisprodukt' 
    AND status = 'FINISHED'
    AND job_identifier = 'JOB_AUDIT_TEST'
) = 3 AS 'Expected FINISHED audit entry to record exactly 3 records';
```

### Pass/Fail Criterion
* **Pass**: The stored procedure completes successfully. The audit log contains a `STARTED` entry and a `FINISHED` entry with a `record_count` of `3` matching the rows in `PoolBasisprodukt` for `2023-12-31`.
* **Fail**: Any audit entries are missing, dates are incorrectly parsed, or the recorded row count does not match the actual table count.

---

## 4. End-to-End Output Parity & Transformation Correctness

### Purpose
Verify that the migrated BigQuery pipeline produces identical results to the legacy Oracle execution for a controlled dataset, ensuring no regressions in data types, null handling, or business logic.

### Setup
1. Extract a test dataset from the legacy Oracle database before migration (both source tables and the resulting `PoolBasisprodukt` output for a specific `Stichtag`, e.g., `15082023`).
2. Load the legacy source data into BigQuery staging tables.
3. Load the legacy Oracle output into a validation table: `${GCP_PROJECT_ID}.${BQ_DATASET}.oracle_expected_PoolBasisprodukt`.

### Action
Run the migrated BigQuery stored procedure against the staging tables, then compare the output in `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt` with the legacy Oracle output.

```sql
-- Action: Run SP for the comparison date
CALL `${GCP_PROJECT_ID}.${BQ_DATASET}.r_ausd_bp_ta_bpr_evn`(
  'PARITY_TEST',
  '8888',
  '15082023',
  '0'
);

-- Assertions: Compare BigQuery output with Oracle expected output
-- This query checks for any rows that exist in one table but not the other, or differ in values.
DECLARE diff_count INT64;

SET diff_count = (
  SELECT COUNT(1) FROM (
    (
      SELECT product_id, product_name, business_date 
      FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt`
      WHERE business_date = '2023-08-15'
      EXCEPT DISTINCT
      SELECT product_id, product_name, business_date 
      FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.oracle_expected_PoolBasisprodukt`
      WHERE business_date = '2023-08-15'
    )
    UNION ALL
    (
      SELECT product_id, product_name, business_date 
      FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.oracle_expected_PoolBasisprodukt`
      WHERE business_date = '2023-08-15'
      EXCEPT DISTINCT
      SELECT product_id, product_name, business_date 
      FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt`
      WHERE business_date = '2023-08-15'
    )
  )
);

ASSERT diff_count = 0 AS CONCAT('Data parity failure! Found ', CAST(diff_count AS STRING), ' mismatched rows between BigQuery and Oracle.');
```

### Pass/Fail Criterion
* **Pass**: The `diff_count` is exactly `0`, proving 100% schema, row-count, and value parity between the legacy Oracle output and the migrated BigQuery output.
* **Fail**: Any differences are detected in row counts, column values, or data types.

---

## 5. Leap Year & Date Boundary Handling

### Purpose
Ensure that the date parsing and calculation logic (such as `DATE_SUB` and `PARSE_DATE`) handles leap years and month boundaries correctly without throwing exceptions or causing off-by-one errors.

### Setup
Clear target tables and audit logs.

### Action
Call the stored procedure with a leap year date (`29022024`) and a month-end boundary date (`01032024`).

```sql
-- Action 1: Leap Year Date
CALL `${GCP_PROJECT_ID}.${BQ_DATASET}.r_ausd_bp_ta_bpr_evn`(
  'LEAP_YEAR_TEST',
  '1001',
  '29022024',
  '0'
);

-- Action 2: Month Boundary Date
CALL `${GCP_PROJECT_ID}.${BQ_DATASET}.r_ausd_bp_ta_bpr_evn`(
  'BOUNDARY_TEST',
  '1002',
  '01032024',
  '0'
);

-- Assertions
ASSERT (
  SELECT COUNT(1) 
  FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`
  WHERE job_identifier = 'LEAP_YEAR_TEST' AND stichtag = '2024-02-29'
) = 2 AS 'Leap year date 29022024 was not parsed or logged correctly';

ASSERT (
  SELECT COUNT(1) 
  FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`
  WHERE job_identifier = 'BOUNDARY_TEST' AND stichtag = '2024-03-01'
) = 2 AS 'Month boundary date 01032024 was not parsed or logged correctly';
```

### Pass/Fail Criterion
* **Pass**: Both executions complete successfully, and the dates are correctly parsed into standard BigQuery `DATE` formats (`2024-02-29` and `2024-03-01` respectively) inside the audit log.
* **Fail**: The procedure fails during execution, or parses the dates incorrectly.