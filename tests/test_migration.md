As a senior data-migration QA engineer, I've analyzed the migration design and the generated BigQuery code for `k_ausd_bp_ta_rn_vertrag.ksh`. The migration involves transforming a KornShell orchestrator with embedded Oracle SQL into a BigQuery Stored Procedure, leveraging BigQuery's native capabilities for data storage, transformation, and error handling.

Below are the detailed migration validation tests, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests: `k_ausd_bp_ta_rn_vertrag.ksh` to BigQuery

**Project/Dataset Placeholders:**
*   `your_gcp_project`
*   `your_bq_dataset`

**Testing Framework:**
These tests are designed to be executed using a Python-based testing framework like `pytest`, interacting with BigQuery via the `google-cloud-bigquery` client library. SQL assertions can be run directly in BigQuery or via the client.

**Pre-requisites for all tests:**
1.  BigQuery project (`your_gcp_project`) and dataset (`your_bq_dataset`) exist.
2.  The DDL for `SOF$TA_RN_VERTRAG`, `error_log`, and `job_tracking` tables has been executed in `your_bq_dataset`.
3.  The BigQuery Stored Procedure `your_bq_dataset.r_ausd_bp_ta_rn_vertrag` has been deployed.
4.  The source tables `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` exist in `your_bq_dataset` with appropriate schemas matching the legacy Oracle tables.

---

### Test Case 1: Successful Data Transformation - Output Parity & Transformation Correctness

**Purpose:**
To verify that the BigQuery Stored Procedure correctly processes data from source tables and inserts it into the target table, matching the expected output of the legacy system for a standard scenario. This covers the core `JOIN` and `SELECT` logic.

**Setup:**
1.  Clear the target table `SOF$TA_RN_VERTRAG`, `error_log`, and `job_tracking` tables.
2.  Populate `DWTK_MELDUNGEN` with sample data.
3.  Populate `SOF$TA_RN_EINZELN` with sample data that will result in successful joins.

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;

-- Populate DWTK_MELDUNGEN
INSERT INTO `your_gcp_project.your_bq_dataset.DWTK_MELDUNGEN` (MELDUNG_CD, MELDUNG_TEXT, MELDUNG_GUELTIG_CD) VALUES
('M001', 'Message One', 'G01'),
('M002', 'Message Two', 'G02'),
('M003', 'Message Three', 'G01');

-- Populate SOF$TA_RN_EINZELN
INSERT INTO `your_gcp_project.your_bq_dataset.SOF$TA_RN_EINZELN` (MELDUNG_CD, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT) VALUES
('M001', 'E001', 'V001', 'G01', 'Valid One'),
('M002', 'E002', 'V002', 'G02', 'Valid Two'),
('M001', 'E003', 'V003', 'G01', 'Valid Three'),
('M004', 'E004', 'V004', 'G03', 'Valid Four'); -- No match in DWTK_MELDUNGEN
```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```python
# Python (pytest) example
from google.cloud import bigquery

client = bigquery.Client(project='your_gcp_project')
procedure_id = "your_bq_dataset.r_ausd_bp_ta_rn_vertrag"
job_kennung = "TEST_JOB_001"
eintrags_nr = "ENTRY_001"
stichtag = "01012023" # DDMMYYYY
wiederanlauf_wert = "0"

query = f"""
CALL {procedure_id}(
    p_JobKennung => '{job_kennung}',
    p_EintragsNr => '{eintrags_nr}',
    p_Stichtag => '{stichtag}',
    p_wiederanlaufWert => '{wiederanlauf_wert}'
);
"""
query_job = client.query(query)
query_job.result() # Wait for the procedure to complete
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully without errors.
2.  Exactly 3 rows are inserted into `SOF$TA_RN_VERTRAG`.
3.  The data in `SOF$TA_RN_VERTRAG` matches the expected output based on the join logic.
4.  One entry is present in `job_tracking` with `records_processed = 3`.
5.  No entries are present in `error_log`.

```sql
-- SQL Assertions
-- 1. Check row count in target table
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
-- Expected: 3

-- 2. Check content of target table
SELECT MELDUNG_CD, MELDUNG_TEXT, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT
FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`
ORDER BY MELDUNG_CD, EINTRAG_NR;
/* Expected Output:
MELDUNG_CD | MELDUNG_TEXT  | EINTRAG_NR | VERTRAG_NR | MELDUNG_GUELTIG_CD | MELDUNG_GUELTIG_TEXT
-----------|---------------|------------|------------|--------------------|---------------------
M001       | Message One   | E001       | V001       | G01                | Valid One
M001       | Message One   | E003       | V003       | G01                | Valid Three
M002       | Message Two   | E002       | V002       | G02                | Valid Two
*/

-- 3. Check job_tracking entry
SELECT job_name, job_kennung, eintrags_nr, stichtag, records_processed, description
FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_kennung = 'TEST_JOB_001' AND eintrags_nr = 'ENTRY_001';
/* Expected Output:
job_name       | job_kennung  | eintrags_nr | stichtag   | records_processed | description
---------------|--------------|-------------|------------|-------------------|--------------
PoolBasisprodukt | TEST_JOB_001 | ENTRY_001   | 2023-01-01 | 3                 | Initialbefuellung
*/

-- 4. Check error_log (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.error_log`;
-- Expected: 0
```

---

### Test Case 2: Transformation Correctness - No Matching Records

**Purpose:**
To verify the procedure handles cases where the join condition yields no matches, resulting in zero records inserted into the target table. This ensures the `INSERT` and `COUNT` logic behaves correctly under an empty result set.

**Setup:**
1.  Clear the target table `SOF$TA_RN_VERTRAG`, `error_log`, and `job_tracking` tables.
2.  Populate `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` such that no records satisfy the join condition.

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;

-- Populate DWTK_MELDUNGEN with data that won't match SOF$TA_RN_EINZELN
INSERT INTO `your_gcp_project.your_bq_dataset.DWTK_MELDUNGEN` (MELDUNG_CD, MELDUNG_TEXT, MELDUNG_GUELTIG_CD) VALUES
('M100', 'Unique Message', 'G99');

-- Populate SOF$TA_RN_EINZELN with data that won't match DWTK_MELDUNGEN
INSERT INTO `your_gcp_project.your_bq_dataset.SOF$TA_RN_EINZELN` (MELDUNG_CD, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT) VALUES
('M200', 'E200', 'V200', 'G88', 'Another Unique');
```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```python
# Python (pytest) example
# ... (same as Test Case 1, just change job_kennung/eintrags_nr if desired)
job_kennung = "TEST_JOB_002"
eintrags_nr = "ENTRY_002"
stichtag = "02012023"
# ... execute procedure
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully without errors.
2.  Exactly 0 rows are inserted into `SOF$TA_RN_VERTRAG`.
3.  One entry is present in `job_tracking` with `records_processed = 0`.
4.  No entries are present in `error_log`.

```sql
-- SQL Assertions
-- 1. Check row count in target table
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
-- Expected: 0

-- 2. Check job_tracking entry
SELECT records_processed
FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_kennung = 'TEST_JOB_002' AND eintrags_nr = 'ENTRY_002';
-- Expected: 0

-- 3. Check error_log (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.error_log`;
-- Expected: 0
```

---

### Test Case 3: Parameter Validation - Missing Mandatory Parameter

**Purpose:**
To verify that the stored procedure correctly identifies and handles missing mandatory input parameters, logging the error and raising an exception, mimicking the legacy KornShell script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.

**Setup:**
1.  Clear `error_log` and `job_tracking` tables.
2.  Ensure `SOF$TA_RN_VERTRAG` is empty (though it shouldn't be affected by this test).

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;
```

**Action:**
Attempt to execute the BigQuery Stored Procedure with a missing `p_JobKennung`. Repeat for `p_EintragsNr` and `p_Stichtag`.

```python
# Python (pytest) example
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

client = bigquery.Client(project='your_gcp_project')
procedure_id = "your_bq_dataset.r_ausd_bp_ta_rn_vertrag"

# Test missing p_JobKennung
query_missing_jobkennung = f"""
CALL {procedure_id}(
    p_JobKennung => '', -- Missing
    p_EintragsNr => 'ENTRY_003',
    p_Stichtag => '03012023',
    p_wiederanlaufWert => '0'
);
"""
with pytest.raises(BadRequest) as excinfo:
    client.query(query_missing_jobkennung).result()
assert "FEHLER: Notwendiges Argument fehlt - Jobkennung" in str(excinfo.value)

# Test missing p_Stichtag
query_missing_stichtag = f"""
CALL {procedure_id}(
    p_JobKennung => 'TEST_JOB_003',
    p_EintragsNr => 'ENTRY_003',
    p_Stichtag => '', -- Missing
    p_wiederanlaufWert => '0'
);
"""
with pytest.raises(BadRequest) as excinfo:
    client.query(query_missing_stichtag).result()
assert "FEHLER: Notwendiges Argument fehlt - Stichtag" in str(excinfo.value)

# Test missing p_EintragsNr
query_missing_eintragsnr = f"""
CALL {procedure_id}(
    p_JobKennung => 'TEST_JOB_003',
    p_EintragsNr => '', -- Missing
    p_Stichtag => '03012023',
    p_wiederanlaufWert => '0'
);
"""
with pytest.raises(BadRequest) as excinfo:
    client.query(query_missing_eintragsnr).result()
assert "FEHLER: Notwendiges Argument fehlt - EintragsNr" in str(excinfo.value)
```

**Pass/Fail Criterion:**
1.  Each execution attempt raises a `BadRequest` (or equivalent BigQuery error) containing the specific error message for the missing parameter.
2.  For each failed attempt, one entry is present in `error_log` with `error_code = 1` and the correct `error_argument` (`Jobkennung`, `Stichtag`, `EintragsNr`).
3.  No entries are present in `job_tracking`.
4.  No data is inserted into `SOF$TA_RN_VERTRAG`.

```sql
-- SQL Assertions (after all attempts)
-- 1. Check error_log entries
SELECT error_code, error_argument, error_message
FROM `your_gcp_project.your_bq_dataset.error_log`
ORDER BY error_argument;
/* Expected Output (order might vary):
error_code | error_argument | error_message
-----------|----------------|----------------------------------------------
1          | EintragsNr     | FEHLER: Notwendiges Argument fehlt - EintragsNr
1          | Jobkennung     | FEHLER: Notwendiges Argument fehlt - Jobkennung
1          | Stichtag       | FEHLER: Notwendiges Argument fehlt - Stichtag
*/

-- 2. Check job_tracking (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.job_tracking`;
-- Expected: 0

-- 3. Check target table (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
-- Expected: 0
```

---

### Test Case 4: Parameter Validation - Invalid Date Format (`p_Stichtag`)

**Purpose:**
To verify that the stored procedure correctly validates the `p_Stichtag` format (`DDMMYYYY`), logging an error and raising an exception if the format is incorrect, mirroring the `DWDate_Datum_Check` functionality.

**Setup:**
1.  Clear `error_log` and `job_tracking` tables.

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;
```

**Action:**
Attempt to execute the BigQuery Stored Procedure with an invalid `p_Stichtag` format (e.g., `YYYY-MM-DD`, `DD/MM/YYYY`, or malformed string).

```python
# Python (pytest) example
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

client = bigquery.Client(project='your_gcp_project')
procedure_id = "your_bq_dataset.r_ausd_bp_ta_rn_vertrag"

# Test invalid date format
query_invalid_stichtag = f"""
CALL {procedure_id}(
    p_JobKennung => 'TEST_JOB_004',
    p_EintragsNr => 'ENTRY_004',
    p_Stichtag => '2023-01-04', -- Invalid format
    p_wiederanlaufWert => '0'
);
"""
with pytest.raises(BadRequest) as excinfo:
    client.query(query_invalid_stichtag).result()
assert "Ungueltiges Datumformat fuer Stichtag, erwartet DDMMYYYY" in str(excinfo.value)
```

**Pass/Fail Criterion:**
1.  The execution attempt raises a `BadRequest` (or equivalent BigQuery error) containing the specific error message for the invalid date format.
2.  One entry is present in `error_log` with `error_code = 2` (or similar, based on generated code) and `error_argument = 'StichtagFormat'`.
3.  No entries are present in `job_tracking`.
4.  No data is inserted into `SOF$TA_RN_VERTRAG`.

```sql
-- SQL Assertions
-- 1. Check error_log entry
SELECT error_code, error_argument, error_message
FROM `your_gcp_project.your_bq_dataset.error_log`
WHERE job_kennung = 'TEST_JOB_004';
/* Expected Output:
error_code | error_argument | error_message
-----------|----------------|--------------------------------------------------
2          | StichtagFormat | Ungueltiges Datumformat fuer Stichtag, erwartet DDMMYYYY
*/

-- 2. Check job_tracking (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.job_tracking`;
-- Expected: 0

-- 3. Check target table (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
-- Expected: 0
```

---

### Test Case 5: Date Derivation Correctness

**Purpose:**
To verify that `v_datum_heute` and `v_datum_gestern` are correctly derived within the stored procedure using BigQuery's `CURRENT_DATE()` and `DATE_SUB()`. This replaces the `gestern.ksh` utility.

**Setup:**
1.  Clear `job_tracking` table.
2.  Populate source tables with minimal data to allow a successful run (e.g., one matching record).

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;

-- Populate DWTK_MELDUNGEN
INSERT INTO `your_gcp_project.your_bq_dataset.DWTK_MELDUNGEN` (MELDUNG_CD, MELDUNG_TEXT, MELDUNG_GUELTIG_CD) VALUES
('M001', 'Test Message', 'G01');

-- Populate SOF$TA_RN_EINZELN
INSERT INTO `your_gcp_project.your_bq_dataset.SOF$TA_RN_EINZELN` (MELDUNG_CD, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT) VALUES
('M001', 'E001', 'V001', 'G01', 'Test Valid');
```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```python
# Python (pytest) example
from google.cloud import bigquery
import datetime

client = bigquery.Client(project='your_gcp_project')
procedure_id = "your_bq_dataset.r_ausd_bp_ta_rn_vertrag"
job_kennung = "TEST_JOB_005"
eintrags_nr = "ENTRY_005"
stichtag = "05012023" # DDMMYYYY
wiederanlauf_wert = "0"

query = f"""
CALL {procedure_id}(
    p_JobKennung => '{job_kennung}',
    p_EintragsNr => '{eintrags_nr}',
    p_Stichtag => '{stichtag}',
    p_wiederanlaufWert => '{wiederanlauf_wert}'
);
"""
client.query(query).result()
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully.
2.  The `job_tracking` entry for this run has `stichtag` matching the parsed `p_Stichtag` (`2023-01-05`).
3.  The `job_tracking` entry's `timestamp` (which implicitly uses `CURRENT_TIMESTAMP()`) should be consistent with the execution time. (While `v_datum_heute` and `v_datum_gestern` are not directly logged, their correct derivation is crucial for any future logic that might use them. For this specific job, they are derived but not used in the main `INSERT` or logging. If they were used, we'd assert their values in the output.)

```sql
-- SQL Assertions
-- 1. Check job_tracking entry for correct stichtag
SELECT stichtag
FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_kennung = 'TEST_JOB_005' AND eintrags_nr = 'ENTRY_005';
-- Expected: 2023-01-05 (DATE type)

-- 2. Verify that the procedure ran successfully and inserted 1 record
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
-- Expected: 1
```

---

### Test Case 6: `p_wiederanlaufWert` Default Handling

**Purpose:**
To verify that `p_wiederanlaufWert` correctly defaults to '0' if not provided or provided as an empty string, as specified in the legacy script.

**Setup:**
1.  Clear `job_tracking` table.
2.  Populate source tables with minimal data to allow a successful run.

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;

-- Populate DWTK_MELDUNGEN
INSERT INTO `your_gcp_project.your_bq_dataset.DWTK_MELDUNGEN` (MELDUNG_CD, MELDUNG_TEXT, MELDUNG_GUELTIG_CD) VALUES
('M001', 'Test Message', 'G01');

-- Populate SOF$TA_RN_EINZELN
INSERT INTO `your_gcp_project.your_bq_dataset.SOF$TA_RN_EINZELN` (MELDUNG_CD, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT) VALUES
('M001', 'E001', 'V001', 'G01', 'Test Valid');
```

**Action:**
Execute the BigQuery Stored Procedure, explicitly passing `NULL` or an empty string for `p_wiederanlaufWert`.

```python
# Python (pytest) example
from google.cloud import bigquery

client = bigquery.Client(project='your_gcp_project')
procedure_id = "your_bq_dataset.r_ausd_bp_ta_rn_vertrag"
job_kennung = "TEST_JOB_006"
eintrags_nr = "ENTRY_006"
stichtag = "06012023" # DDMMYYYY

# Case 1: p_wiederanlaufWert is NULL
query_null_wiederanlauf = f"""
CALL {procedure_id}(
    p_JobKennung => '{job_kennung}_NULL',
    p_EintragsNr => '{eintrags_nr}_NULL',
    p_Stichtag => '{stichtag}',
    p_wiederanlaufWert => NULL
);
"""
client.query(query_null_wiederanlauf).result()

# Case 2: p_wiederanlaufWert is empty string
query_empty_wiederanlauf = f"""
CALL {procedure_id}(
    p_JobKennung => '{job_kennung}_EMPTY',
    p_EintragsNr => '{eintrags_nr}_EMPTY',
    p_Stichtag => '{stichtag}',
    p_wiederanlaufWert => ''
);
"""
client.query(query_empty_wiederanlauf).result()
```

**Pass/Fail Criterion:**
1.  Both procedure calls complete successfully.
2.  No errors are logged in `error_log`.
3.  Two entries are present in `job_tracking`, indicating successful runs. (The `p_wiederanlaufWert` is not logged, but its correct handling is crucial for internal logic. If it were used in a `WHERE` clause, we'd verify the filtered output.)

```sql
-- SQL Assertions
-- 1. Check job_tracking entries (should be two successful runs)
SELECT COUNT(*)
FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_kennung LIKE 'TEST_JOB_006%';
-- Expected: 2

-- 2. Check error_log (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.error_log`;
-- Expected: 0
```

---

### Test Case 7: Transformation Correctness - NULL Handling in Join Keys

**Purpose:**
To verify that records with `NULL` values in the join keys (`MELDUNG_CD`, `MELDUNG_GUELTIG_CD`) are correctly excluded from the join, as `NULL = NULL` evaluates to `NULL` (unknown) in SQL and thus does not match.

**Setup:**
1.  Clear the target table `SOF$TA_RN_VERTRAG`, `error_log`, and `job_tracking` tables.
2.  Populate source tables with records where join keys are `NULL` or only partially match.

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;

-- Populate DWTK_MELDUNGEN
INSERT INTO `your_gcp_project.your_bq_dataset.DWTK_MELDUNGEN` (MELDUNG_CD, MELDUNG_TEXT, MELDUNG_GUELTIG_CD) VALUES
('M001', 'Valid Match', 'G01'),
(NULL, 'Null CD', 'G02'),
('M003', 'Null Gueltig CD', NULL);

-- Populate SOF$TA_RN_EINZELN
INSERT INTO `your_gcp_project.your_bq_dataset.SOF$TA_RN_EINZELN` (MELDUNG_CD, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT) VALUES
('M001', 'E001', 'V001', 'G01', 'Valid Match'),
(NULL, 'E002', 'V002', 'G02', 'Null CD Match Attempt'),
('M003', 'E003', 'V003', NULL, 'Null Gueltig CD Match Attempt'),
('M004', 'E004', 'V004', 'G04', 'No Match');
```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.

```python
# Python (pytest) example
from google.cloud import bigquery

client = bigquery.Client(project='your_gcp_project')
procedure_id = "your_bq_dataset.r_ausd_bp_ta_rn_vertrag"
job_kennung = "TEST_JOB_007"
eintrags_nr = "ENTRY_007"
stichtag = "07012023" # DDMMYYYY
wiederanlauf_wert = "0"

query = f"""
CALL {procedure_id}(
    p_JobKennung => '{job_kennung}',
    p_EintragsNr => '{eintrags_nr}',
    p_Stichtag => '{stichtag}',
    p_wiederanlaufWert => '{wiederanlauf_wert}'
);
"""
client.query(query).result()
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully.
2.  Exactly 1 row is inserted into `SOF$TA_RN_VERTRAG` (only the `M001/G01` match).
3.  The `job_tracking` entry shows `records_processed = 1`.
4.  No errors are logged.

```sql
-- SQL Assertions
-- 1. Check row count in target table
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
-- Expected: 1

-- 2. Check content of target table
SELECT MELDUNG_CD, MELDUNG_TEXT, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT
FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
/* Expected Output:
MELDUNG_CD | MELDUNG_TEXT | EINTRAG_NR | VERTRAG_NR | MELDUNG_GUELTIG_CD | MELDUNG_GUELTIG_TEXT
-----------|--------------|------------|------------|--------------------|---------------------
M001       | Valid Match  | E001       | V001       | G01                | Valid Match
*/

-- 3. Check job_tracking entry
SELECT records_processed
FROM `your_gcp_project.your_bq_dataset.job_tracking`
WHERE job_kennung = 'TEST_JOB_007';
-- Expected: 1
```

---

### Test Case 8: Schema and Data Quality - Target Table Structure

**Purpose:**
To verify that the target table `SOF$TA_RN_VERTRAG` has the correct schema, including data types and the newly added `PROCESSING_TIMESTAMP` column, ensuring data quality and auditability.

**Setup:**
No specific setup beyond ensuring the table exists as per DDL.

**Action:**
Query the BigQuery information schema for the target table.

```python
# Python (pytest) example
from google.cloud import bigquery

client = bigquery.Client(project='your_gcp_project')
table_id = "your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG"
table = client.get_table(table_id)

# Extract schema information
schema_fields = {field.name: field.field_type for field in table.schema}

# Expected schema
expected_schema = {
    'MELDUNG_CD': 'STRING',
    'MELDUNG_TEXT': 'STRING',
    'EINTRAG_NR': 'STRING',
    'VERTRAG_NR': 'STRING',
    'MELDUNG_GUELTIG_CD': 'STRING',
    'MELDUNG_GUELTIG_TEXT': 'STRING',
    'PROCESSING_TIMESTAMP': 'TIMESTAMP'
}

# Assertions
assert schema_fields == expected_schema, f"Schema mismatch: {schema_fields} vs {expected_schema}"
```

**Pass/Fail Criterion:**
1.  The schema of `SOF$TA_RN_VERTRAG` exactly matches the expected schema, including column names, data types, and the presence of `PROCESSING_TIMESTAMP` as a `TIMESTAMP` type.

---

### Test Case 9: External System Replacements - Verification of Internalization

**Purpose:**
To confirm that the migration successfully replaced external KornShell utilities and Oracle-specific constructs with native BigQuery Stored Procedure logic, and that no unintended external calls remain. This addresses Section 3 (Target Architecture) and Section 6 (External Dependencies) of the design.

**Setup:**
No specific setup beyond the deployed BigQuery Stored Procedure.

**Action:**
This test is primarily a review of the generated BigQuery Stored Procedure code and its execution logs.
1.  Review the BigQuery Stored Procedure code (`procedures/your_bq_dataset.r_ausd_bp_ta_rn_vertrag.sql`).
2.  Execute the procedure (e.g., Test Case 1).
3.  Monitor BigQuery job logs and Cloud Logging for any unexpected external calls or errors related to missing external dependencies.

**Pass/Fail Criterion:**
1.  The BigQuery Stored Procedure code explicitly uses BigQuery SQL functions for date handling (`CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE`, `REGEXP_CONTAINS`) and parameter validation (`IF`, `SIGNAL SQLSTATE`).
2.  There are no calls to external systems (e.g., `EXTERNAL_QUERY` to Oracle, `EXPORT DATA` to SFTP/S3, or any UDFs that might wrap external calls) within the procedure that are not explicitly part of the migration design.
3.  BigQuery job logs for the procedure execution show only BigQuery internal operations (e.g., `query`, `dml`, `procedure_call`).
4.  The `DWPA_UTIL_SKRIPT` functionality is confirmed to be either removed (if it was a simple dynamic SQL executor) or re-implemented natively within the BigQuery Stored Procedure, without external calls. (Based on the generated code, it appears to have been removed, with the core SQL embedded directly).

**Note:** This test is more about code review and log analysis rather than a single runnable assertion. A comprehensive CI/CD pipeline might include static analysis tools to detect disallowed external calls.

---

### Test Case 10: Data Transformation Error Handling

**Purpose:**
To verify that if an error occurs during the main data transformation (e.g., due to data type mismatch if not handled by schema, or other SQL execution issues), the procedure catches it, logs it to `error_log`, and raises an appropriate exception.

**Setup:**
1.  Clear `SOF$TA_RN_VERTRAG`, `error_log`, and `job_tracking` tables.
2.  Introduce data into source tables that would cause a data transformation error if possible (e.g., attempting to insert a string into a numeric column, though the current schema is all STRING). A more realistic scenario might involve a UDF or more complex logic that could fail. For this specific, simple `INSERT INTO ... SELECT` with matching string types, it's harder to force a transformation error without schema changes.
    *   **Alternative Setup (Simulated Error):** For demonstration, we'll assume a hypothetical scenario where `MELDUNG_CD` in `SOF$TA_RN_VERTRAG` was `INT64` and `DWTK_MELDUNGEN.MELDUNG_CD` was `STRING` with non-numeric values. Since the generated DDL has `MELDUNG_CD` as `STRING`, we'll simulate a different type of error, e.g., a division by zero if there was a calculation. As there isn't one, we'll rely on the `EXCEPTION WHEN ERROR THEN` block to catch *any* SQL error during the `INSERT` statement.

```sql
-- Clear tables
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.error_log`;
TRUNCATE TABLE `your_gcp_project.your_bq_dataset.job_tracking`;

-- Populate DWTK_MELDUNGEN and SOF$TA_RN_EINZELN with valid data
-- This test relies on the EXCEPTION block catching a *hypothetical* error
-- within the INSERT statement. For this simple INSERT, a direct error is unlikely
-- unless source table schemas are incompatible or target table is locked/unavailable.
-- We assume the EXCEPTION block itself is being tested.
INSERT INTO `your_gcp_project.your_bq_dataset.DWTK_MELDUNGEN` (MELDUNG_CD, MELDUNG_TEXT, MELDUNG_GUELTIG_CD) VALUES
('M001', 'Message One', 'G01');
INSERT INTO `your_gcp_project.your_bq_dataset.SOF$TA_RN_EINZELN` (MELDUNG_CD, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT) VALUES
('M001', 'E001', 'V001', 'G01', 'Valid One');
```

**Action:**
Execute the BigQuery Stored Procedure. To *force* a transformation error for testing purposes, one might temporarily modify the target table schema to be incompatible (e.g., make `MELDUNG_CD` an `INT64` and then try to insert 'M001'). However, for a production-ready test, we assume the `EXCEPTION` block is correctly implemented to catch *any* unexpected SQL error during the `INSERT`.

```python
# Python (pytest) example
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

client = bigquery.Client(project='your_gcp_project')
procedure_id = "your_bq_dataset.r_ausd_bp_ta_rn_vertrag"
job_kennung = "TEST_JOB_008"
eintrags_nr = "ENTRY_008"
stichtag = "08012023" # DDMMYYYY
wiederanlauf_wert = "0"

# To simulate an error, you would need to temporarily alter the target table schema
# or introduce a failing UDF/logic within the SELECT statement.
# For this test, we assume the EXCEPTION block is functional.
# If the target table's MELDUNG_CD was INT64, and source MELDUNG_CD was 'M001',
# this would cause an error.
# Example of how to force an error (DO NOT RUN ON PROD):
# client.query("ALTER TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG` ALTER COLUMN MELDUNG_CD SET DATA TYPE INT64;").result()

query = f"""
CALL {procedure_id}(
    p_JobKennung => '{job_kennung}',
    p_EintragsNr => '{eintrags_nr}',
    p_Stichtag => '{stichtag}',
    p_wiederanlaufWert => '{wiederanlauf_wert}'
);
"""
with pytest.raises(BadRequest) as excinfo:
    client.query(query).result()

# Assert that the error message indicates a data transformation failure
assert "FEHLER: Data transformation failed" in str(excinfo.value)

# Revert schema change if it was done for testing (DO NOT RUN ON PROD)
# client.query("ALTER TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG` ALTER COLUMN MELDUNG_CD SET DATA TYPE STRING;").result()
```

**Pass/Fail Criterion:**
1.  The procedure execution raises a `BadRequest` (or equivalent BigQuery error) containing the specific error message indicating a data transformation failure.
2.  One entry is present in `error_log` with `error_code = 3` (or similar), `error_argument = 'DataTransformation'`, and a detailed `error_message`.
3.  No entries are present in `job_tracking`.
4.  No data is inserted into `SOF$TA_RN_VERTRAG` (or only partial data if the error occurred mid-batch, but the `EXCEPTION` block should prevent this for a single `INSERT` statement).

```sql
-- SQL Assertions
-- 1. Check error_log entry
SELECT error_code, error_argument, error_message
FROM `your_gcp_project.your_bq_dataset.error_log`
WHERE job_kennung = 'TEST_JOB_008';
/* Expected Output (error_message will vary based on actual error):
error_code | error_argument     | error_message
-----------|--------------------|--------------------------------------------------
3          | DataTransformation | FEHLER: Data transformation failed - <BigQuery error details>
*/

-- 2. Check job_tracking (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.job_tracking`;
-- Expected: 0

-- 3. Check target table (should be empty)
SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
-- Expected: 0
```