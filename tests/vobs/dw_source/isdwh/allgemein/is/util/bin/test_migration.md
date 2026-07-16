# Migration Validation Test Suite: Shared Files — `vobs/dw_source/isdwh/allgemein/is/util/bin`

This document defines the migration-validation tests to prove behavioral equivalence between the legacy KornShell/Oracle SQL*Plus utility libraries and the migrated BigQuery native stored procedures.

---

## Test Environment Setup & Mocking

To execute these validation tests, we must mock the external package procedures and tables referenced by the logging and parameter libraries. Run the following SQL script in your test dataset to establish the mock environment:

```sql
-- Create Mock Target Table for Logging
CREATE OR REPLACE TABLE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen` (
  entrynr STRING,
  job_kennung STRING,
  programmname STRING,
  log_datei STRING,
  parameter STRING,
  stichtag DATE,
  dateiname STRING,
  status STRING,
  zusatzinfos STRING
);

-- Create Mock Table for SQL Execution Logs
CREATE OR REPLACE TABLE `@gcp_project.@bq_dataset.sql_execution_file_logs` (
  entrynr INT64,
  script_name STRING,
  directory STRING,
  file_name STRING,
  job_kennung STRING,
  execution_timestamp TIMESTAMP,
  status STRING
);

-- Create Mock Package Procedures
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwpa_meldung__setzestatusok`(p_entry_nr STRING)
BEGIN
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET status = 'OK'
  WHERE entrynr = p_entry_nr;
END;

CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwpa_meldung__setzestatusabbruch`(p_entry_nr STRING)
BEGIN
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET status = 'ABBRUCH'
  WHERE entrynr = p_entry_nr;
END;

CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwpa_meldung__erzeuge_eintrag_p4`(
  p_entry_nr STRING, p_job_kennung STRING, p_programmname STRING, p_log_datei STRING
)
BEGIN
  INSERT INTO `@gcp_project.@bq_dataset.dwh_ta_k_meldungen` (entrynr, job_kennung, programmname, log_datei, status)
  VALUES (p_entry_nr, p_job_kennung, p_programmname, p_log_datei, 'RUNNING');
END;

CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwpa_meldung__erzeuge_eintrag_p5`(
  p_entry_nr STRING, p_job_kennung STRING, p_programmname STRING, p_log_datei STRING, p_parameter STRING
)
BEGIN
  INSERT INTO `@gcp_project.@bq_dataset.dwh_ta_k_meldungen` (entrynr, job_kennung, programmname, log_datei, parameter, status)
  VALUES (p_entry_nr, p_job_kennung, p_programmname, p_log_datei, p_parameter, 'RUNNING');
END;

CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwpa_meldung__fehler`(
  p_typ STRING, p_entry_nr STRING, p_fehler_nr INT64, p_zusatz1 STRING, p_zusatz2 STRING
)
BEGIN
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET status = CONCAT('FEHLER_', p_typ, '_', CAST(p_fehler_nr AS STRING)),
      zusatzinfos = CONCAT(COALESCE(p_zusatz1, ''), ' | ', COALESCE(p_zusatz2, ''))
  WHERE entrynr = p_entry_nr;
END;

CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwpa_meldung__setzezusatzinfos`(
  p_entry_nr STRING, p_stichtag DATE, p_text STRING, p_val1 STRING, p_val2 STRING
)
BEGIN
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET stichtag = COALESCE(p_stichtag, stichtag),
      zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), ' ', COALESCE(p_text, ''), ' ', COALESCE(p_val1, ''), ' ', COALESCE(p_val2, ''))
  WHERE entrynr = p_entry_nr;
END;

CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwh_vs_meldung__logausgabe_debug`(p_entry_nr STRING, p_text STRING)
BEGIN
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), ' [DEBUG] ', p_text)
  WHERE entrynr = p_entry_nr;
END;

CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.dwpa_meldung__logausgabe_info`(p_entry_nr STRING, p_text STRING)
BEGIN
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), ' [INFO] ', p_text)
  WHERE entrynr = p_entry_nr;
END;

-- Create Mock Target Dynamic Procedure for Script Execution Tests
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.mock_target_script`()
BEGIN
  -- Simple operational assertion
  SELECT 1;
END;
```

---

## Section 1: Output Parity & Parameter Validation Tests

### Test Case 1.1: Parameter Presence Validation (`pruefeParameterGesetzt`)
* **Purpose**: Verify that `pruefeParameterGesetzt` correctly flags unset or empty parameters with the exact legacy error codes (`196` for missing parameter metadata, `194` for missing parameter value).
* **Setup**: Ensure the mock environment is deployed.
* **Action**: Execute the procedure with valid, empty, and NULL parameters.
* **Pass/Fail Criterion**: 
  * Passing `('param_name', 'value')` returns `ErrNr = 0`.
  * Passing `('', 'value')` returns `ErrNr = 196` and `ErrArg = 'alis_parameter V8.3.1 pruefeParameterGesetzt'`.
  * Passing `('param_name', '')` or `('param_name', NULL)` returns `ErrNr = 194` and `ErrArg = 'param_name'`.

```sql
-- Test Execution Script
DECLARE v_err_nr INT64 DEFAULT 0;
DECLARE v_err_arg STRING DEFAULT '';

-- Case A: Valid Parameter
CALL `@gcp_project.@bq_dataset.pruefeParameterGesetzt`('TEST_PARAM', 'VALID_VALUE', v_err_nr, v_err_arg);
ASSERT v_err_nr = 0 AND v_err_arg = '' AS 'Case A Failed';

-- Case B: Missing Parameter Name
SET v_err_nr = 0; SET v_err_arg = '';
CALL `@gcp_project.@bq_dataset.pruefeParameterGesetzt`('', 'VALID_VALUE', v_err_nr, v_err_arg);
ASSERT v_err_nr = 196 AND v_err_arg = 'alis_parameter V8.3.1 pruefeParameterGesetzt' AS 'Case B Failed';

-- Case C: Missing Parameter Value
SET v_err_nr = 0; SET v_err_arg = '';
CALL `@gcp_project.@bq_dataset.pruefeParameterGesetzt`('MY_PARAM', NULL, v_err_nr, v_err_arg);
ASSERT v_err_nr = 194 AND v_err_arg = 'MY_PARAM' AS 'Case C Failed';
```

---

### Test Case 1.2: Metric Code Normalization (`konvertiereKennzahl`)
* **Purpose**: Verify that legacy metric names are correctly mapped to their abbreviated codes, and unrecognized metrics return error code `198`.
* **Setup**: None.
* **Action**: Call `konvertiereKennzahl` with various inputs (mixed casing, spaces, valid, and invalid values).
* **Pass/Fail Criterion**:
  * `'abgang'` maps to `'abg'`.
  * `'CARMEN_RECHNUNG_SAP_XKOPF'` maps to `'crsxk'`.
  * `'invalid_metric'` returns `ErrNr = 198` and `ErrArg = 'invalid_metric'`.

```sql
-- Test Execution Script
DECLARE v_kennzahl STRING;
DECLARE v_err_nr INT64 DEFAULT 0;
DECLARE v_err_arg STRING DEFAULT '';

-- Case A: Standard Mapping with Casing
SET v_kennzahl = '  Abgang  ';
CALL `@gcp_project.@bq_dataset.konvertiereKennzahl`(v_kennzahl, v_err_nr, v_err_arg);
ASSERT v_kennzahl = 'abg' AND v_err_nr = 0 AS 'Case A Failed';

-- Case B: Complex Mapping
SET v_kennzahl = 'carmen_rechnung_sap_xkopf';
SET v_err_nr = 0; SET v_err_arg = '';
CALL `@gcp_project.@bq_dataset.konvertiereKennzahl`(v_kennzahl, v_err_nr, v_err_arg);
ASSERT v_kennzahl = 'crsxk' AND v_err_nr = 0 AS 'Case B Failed';

-- Case C: Invalid Metric
SET v_kennzahl = 'unknown_metric';
SET v_err_nr = 0; SET v_err_arg = '';
CALL `@gcp_project.@bq_dataset.konvertiereKennzahl`(v_kennzahl, v_err_nr, v_err_arg);
ASSERT v_kennzahl = 'unknown_metric' AND v_err_nr = 198 AND v_err_arg = 'unknown_metric' AS 'Case C Failed';
```

---

## Section 2: Transformation Correctness & Date Handling

### Test Case 2.1: Date Range Validation (`pruefeZeitraum`)
* **Purpose**: Prove that date range validations correctly enforce the `YYYYMMDD` format and ensure that the start date is less than or equal to the end date.
* **Setup**: None.
* **Action**: Call `pruefeZeitraum` with valid, invalid format, and logically inverted date ranges.
* **Pass/Fail Criterion**:
  * `'20231001'` to `'20231010'` returns `ErrNr = 0`.
  * `'2023-10-01'` (hyphenated) returns `ErrNr = 195` and `ErrArg = 'Anfangsdatum entspricht nicht dem Format YYYYMMDD'`.
  * `'20231010'` to `'20231001'` (inverted) returns `ErrNr = 195` and `ErrArg = 'Anfangsdatum ist nicht kleiner gleich Endedatum'`.

```sql
-- Test Execution Script
DECLARE v_err_nr INT64 DEFAULT 0;
DECLARE v_err_arg STRING DEFAULT '';

-- Case A: Valid Range
CALL `@gcp_project.@bq_dataset.pruefeZeitraum`('20231001', '20231010', v_err_nr, v_err_arg);
ASSERT v_err_nr = 0 AS 'Case A Failed';

-- Case B: Invalid Format
SET v_err_nr = 0; SET v_err_arg = '';
CALL `@gcp_project.@bq_dataset.pruefeZeitraum`('2023-10-01', '20231010', v_err_nr, v_err_arg);
ASSERT v_err_nr = 195 AND v_err_arg = 'Anfangsdatum entspricht nicht dem Format YYYYMMDD' AS 'Case B Failed';

-- Case C: Inverted Dates
SET v_err_nr = 0; SET v_err_arg = '';
CALL `@gcp_project.@bq_dataset.pruefeZeitraum`('20231010', '20231001', v_err_nr, v_err_arg);
ASSERT v_err_nr = 195 AND v_err_arg = 'Anfangsdatum ist nicht kleiner gleich Endedatum' AS 'Case C Failed';
```

---

### Test Case 2.2: Dynamic Date Span Calculations (`konvertiereZeitspanne`)
* **Purpose**: Verify that `konvertiereZeitspanne` calculates the correct start and end date strings based on the metric type (`bst` uses months, others use days).
* **Setup**: None.
* **Action**: Call `konvertiereZeitspanne` with metric `'bst'` (months) and `'abg'` (days).
* **Pass/Fail Criterion**:
  * For `'bst'` with span `2`, `p_VarAnfang` must equal `FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH))`.
  * For `'abg'` with span `15`, `p_VarAnfang` must equal `FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY))`.

```sql
-- Test Execution Script
DECLARE v_start STRING;
DECLARE v_ende STRING;
DECLARE v_err_nr INT64 DEFAULT 0;
DECLARE v_err_arg STRING DEFAULT '';

-- Case A: Month-based span (bst)
CALL `@gcp_project.@bq_dataset.konvertiereZeitspanne`(v_start, v_ende, 2, 'bst', v_err_nr, v_err_arg);
ASSERT v_start = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)) 
   AND v_ende = FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS 'Case A Failed';

-- Case B: Day-based span (abg)
CALL `@gcp_project.@bq_dataset.konvertiereZeitspanne`(v_start, v_ende, 15, 'abg', v_err_nr, v_err_arg);
ASSERT v_start = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)) 
   AND v_ende = FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS 'Case B Failed';
```

---

## Section 3: External-System Replacements & SQL*Plus Wrappers

### Test Case 3.1: Silent Script Execution (`starteSQLSkriptSilent`)
* **Purpose**: Prove that the SQL*Plus execution wrapper correctly routes calls to native BigQuery procedures using dynamic SQL.
* **Setup**: Ensure `mock_target_script` is created in the target dataset.
* **Action**: Call `starteSQLSkriptSilent` passing `'mock_target_script'`.
* **Pass/Fail Criterion**: The procedure executes successfully without throwing compilation or runtime errors.

```sql
-- Test Execution Script
BEGIN
  -- This call should dynamically resolve to: CALL `@gcp_project.@bq_dataset.mock_target_script`()
  CALL `@gcp_project.@bq_dataset.starteSQLSkriptSilent`('mock_target_script');
END;
```

---

### Test Case 3.2: Directory Existence Assertion (`starte_sql_skript_silent_file`)
* **Purpose**: Verify that the directory validation logic in `starte_sql_skript_silent_file` raises the exact legacy error message when the directory is missing (NULL).
* **Setup**: None.
* **Action**: Call `starte_sql_skript_silent_file` with a NULL directory parameter.
* **Pass/Fail Criterion**: The procedure must fail with the exact German error message: `"Directory $p_Workdir exitiert nicht"`.

```sql
-- Test Execution Script
DECLARE v_error_thrown BOOL DEFAULT FALSE;

BEGIN
  CALL `@gcp_project.@bq_dataset.starte_sql_skript_silent_file`(NULL, 'mock_target_script');
EXCEPTION WHEN ERROR THEN
  IF @@error.message LIKE '%Directory $p_Workdir exitiert nicht%' THEN
    SET v_error_thrown = TRUE;
  END IF;
END;

ASSERT v_error_thrown = TRUE AS 'Failed to raise exact legacy directory error message';
```

---

## Section 4: Logging, Error Trapping & Preserved Literals

### Test Case 4.1: Error Trapping & Status Abort (`DWMSG_Fehlerbehandlung`)
* **Purpose**: Verify that the error-trapping routine logs a fatal error with the exact legacy German warning message and transitions the job status to aborted.
* **Setup**: Insert a running job record into the mock tracking table.
* **Action**: Call `DWMSG_Fehlerbehandlung` with error code `127`.
* **Pass/Fail Criterion**:
  * The tracking table record for the job must have its status updated to `'ABBRUCH'`.
  * The `zusatzinfos` column must contain the exact string: `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus. ErrorCode ist: 127"`.

```sql
-- Test Execution Script
DECLARE v_entry_id STRING DEFAULT 'TEST_ERR_TRAP_001';

-- Initialize running job
INSERT INTO `@gcp_project.@bq_dataset.dwh_ta_k_meldungen` (entrynr, job_kennung, status)
VALUES (v_entry_id, 'JOB_VAL_TEST', 'RUNNING');

-- Trigger error handler
CALL `@gcp_project.@bq_dataset.DWMSG_Fehlerbehandlung`(v_entry_id, 127);

-- Assertions
BEGIN
  DECLARE v_status STRING;
  DECLARE v_zusatz STRING;

  SELECT status, zusatzinfos INTO v_status, v_zusatz
  FROM `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  WHERE entrynr = v_entry_id;

  ASSERT v_status = 'ABBRUCH' AS 'Job status was not set to ABBRUCH';
  ASSERT v_zusatz LIKE '%Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus. ErrorCode ist: 127%' 
    AS CONCAT('Preserved literal mismatch. Found: ', COALESCE(v_zusatz, 'NULL'));
END;
```

---

### Test Case 4.2: File Path Extraction (`DWMSG_AppendDateiInfo`)
* **Purpose**: Verify that `DWMSG_AppendDateiInfo` correctly extracts the base filename from a full Unix path and appends it to the tracking table.
* **Setup**: Insert a running job record into the mock tracking table.
* **Action**: Call `DWMSG_AppendDateiInfo` with path `'/var/tmp/data/input_file_2023.csv'`.
* **Pass/Fail Criterion**: The `zusatzinfos` column for the tracking record must contain the exact formatted string: `"Datei: input_file_2023.csv | "`.

```sql
-- Test Execution Script
DECLARE v_entry_id STRING DEFAULT 'TEST_FILE_EXTRACT_001';

-- Initialize running job
INSERT INTO `@gcp_project.@bq_dataset.dwh_ta_k_meldungen` (entrynr, job_kennung, status, zusatzinfos)
VALUES (v_entry_id, 'JOB_FILE_TEST', 'RUNNING', '');

-- Append file info
CALL `@gcp_project.@bq_dataset.DWMSG_AppendDateiInfo`(v_entry_id, '/var/tmp/data/input_file_2023.csv');

-- Assertions
BEGIN
  DECLARE v_zusatz STRING;

  SELECT zusatzinfos INTO v_zusatz
  FROM `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  WHERE entrynr = v_entry_id;

  ASSERT TRIM(v_zusatz) = 'Datei: input_file_2023.csv |' 
    AS CONCAT('File name extraction failed. Found: ', COALESCE(v_zusatz, 'NULL'));
END;
```

---

## Section 5: Automated Integration Test Runner (Pytest)

The following Python script uses `pytest` and the Google Cloud BigQuery client library to run the validation suite programmatically.

```python
import os
import pytest
from google.cloud import bigquery

# Ensure environment variables are set
PROJECT_ID = os.getenv("GCP_PROJECT", "your-gcp-project")
DATASET_ID = os.getenv("BQ_DATASET", "your_dataset")

@pytest.fixture(scope="session")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

def test_parameter_presence_validation(bq_client):
    query = f"""
    DECLARE v_err_nr INT64 DEFAULT 0;
    DECLARE v_err_arg STRING DEFAULT '';
    CALL `{PROJECT_ID}.{DATASET_ID}.pruefeParameterGesetzt`('MY_PARAM', NULL, v_err_nr, v_err_arg);
    SELECT v_err_nr AS err_nr, v_err_arg AS err_arg;
    """
    query_job = bq_client.query(query)
    results = list(query_job.result())
    assert len(results) == 1
    assert results[0]["err_nr"] == 194
    assert results[0]["err_arg"] == "MY_PARAM"

def test_metric_code_normalization(bq_client):
    query = f"""
    DECLARE v_kennzahl STRING DEFAULT 'carmen_rechnung_sap_xkopf';
    DECLARE v_err_nr INT64 DEFAULT 0;
    DECLARE v_err_arg STRING DEFAULT '';
    CALL `{PROJECT_ID}.{DATASET_ID}.konvertiereKennzahl`(v_kennzahl, v_err_nr, v_err_arg);
    SELECT v_kennzahl AS mapped, v_err_nr AS err_nr;
    """
    query_job = bq_client.query(query)
    results = list(query_job.result())
    assert len(results) == 1
    assert results[0]["mapped"] == "crsxk"
    assert results[0]["err_nr"] == 0

def test_date_range_validation(bq_client):
    query = f"""
    DECLARE v_err_nr INT64 DEFAULT 0;
    DECLARE v_err_arg STRING DEFAULT '';
    CALL `{PROJECT_ID}.{DATASET_ID}.pruefeZeitraum`('20231010', '20231001', v_err_nr, v_err_arg);
    SELECT v_err_nr AS err_nr, v_err_arg AS err_arg;
    """
    query_job = bq_client.query(query)
    results = list(query_job.result())
    assert len(results) == 1
    assert results[0]["err_nr"] == 195
    assert "Anfangsdatum ist nicht kleiner gleich Endedatum" in results[0]["err_arg"]

def test_error_trapping_and_preserved_literals(bq_client):
    entry_id = "PYTEST_ENTRY_001"
    
    # Setup running job
    setup_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dwh_ta_k_meldungen` (entrynr, job_kennung, status)
    VALUES ('{entry_id}', 'PYTEST_JOB', 'RUNNING');
    """
    bq_client.query(setup_query).result()

    # Execute error handler
    test_query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.DWMSG_Fehlerbehandlung`('{entry_id}', 99);
    SELECT status, zusatzinfos FROM `{PROJECT_ID}.{DATASET_ID}.dwh_ta_k_meldungen` WHERE entrynr = '{entry_id}';
    """
    query_job = bq_client.query(test_query)
    results = list(query_job.result())
    
    # Cleanup
    cleanup_query = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dwh_ta_k_meldungen` WHERE entrynr = '{entry_id}';"
    bq_client.query(cleanup_query).result()

    assert len(results) == 1
    assert results[0]["status"] == "ABBRUCH"
    assert "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus" in results[0]["zusatzinfos"]
```