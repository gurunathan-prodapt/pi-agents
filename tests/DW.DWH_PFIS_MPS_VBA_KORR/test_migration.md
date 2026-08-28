# Migration Validation Test Suite: DW.DWH_PFIS_MPS_VBA_KORR

This document defines the migration-validation test suite to verify the behavioral equivalence of the migrated Apache Airflow DAG, Python wrapper, and BigQuery SQL script against the legacy UC4 / Oracle SQL*Plus implementation.

---

## Section 1: End-to-End Orchestration & Wrapper Validation

### Test Case 1.1: Airflow DAG Structure and Dependency Validation
#### Purpose
Verify that the migrated Airflow DAG (`dw_dwh_pfis_mps_vba_korr`) compiles without errors, contains all required tasks, preserves the legacy execution sequence, and correctly defines job-specific parameters.

#### Setup
* A Python environment with `apache-airflow` installed.
* The DAG file `dw_dwh_pfis_mps_vba_korr.py` placed in the Airflow `dags/` directory.
* Airflow Variable `GCP_PROJECT` set to a mock value (e.g., `test-gcp-project`).

#### Action
Run a programmatic test suite using `pytest` to validate the DAG structure:

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(scope="session", autouse=True)
def setup_airflow_variables():
    # Set required global variables for compilation
    Variable.set("GCP_PROJECT", "test-gcp-project")

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_pfis_mps_vba_korr")
    assert dag_bag.import_errors == {}
    assert dag is not None

def test_dag_tasks_and_dependencies():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_pfis_mps_vba_korr")
    
    expected_tasks = {
        "dw_dwh_pfis_mps_vba_korr_task",
        "run_r_pfis_mps_vba_korrektur",
        "run_d_pfis_mps_vba_korrektur_sql"
    }
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks

    # Verify sequential dependency chain: Task 1 >> Task 2 >> Task 3
    t1 = dag.get_task("dw_dwh_pfis_mps_vba_korr_task")
    t2 = dag.get_task("run_r_pfis_mps_vba_korrektur")
    t3 = dag.get_task("run_d_pfis_mps_vba_korrektur_sql")

    assert t2 in t1.downstream_list
    assert t3 in t2.downstream_list
    assert not t3.downstream_list

def test_task_environment_variables():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_pfis_mps_vba_korr")
    t2 = dag.get_task("run_r_pfis_mps_vba_korrektur")
    
    assert t2.env is not None
    assert t2.env.get("DWH_JOB_KENNUNG") == "PFIS_MPS_VBA_KORR"
    assert t2.env.get("GCP_PROJECT") == "test-gcp-project"
```

#### Pass/Fail Criterion
* **Pass**: The DAG loads with zero import errors, contains exactly the three expected tasks, enforces the strict linear dependency chain, and injects the correct environment variables.
* **Fail**: Any compilation errors occur, tasks are missing, dependencies are misaligned, or environment variables are absent.

---

### Test Case 1.2: Python Wrapper Execution and Log Generation
#### Purpose
Verify that the Python wrapper script `r_pfis_mps_vba_korrektur.py` executes successfully, handles command-line arguments, manages environment variables, and writes structured logs to the designated file path.

#### Setup
* Python 3.x environment.
* Environment variables set:
  * `DW_DIR_ROOT` pointing to a valid directory containing a mock SQL file path structure: `pruef/is/sql/d_pfis_mps_vba_korrektur.sql`.
  * `DW_EintragsNr` set to `99999`.
  * `LogDatei` set to `/tmp/test_pfis_mps_vba_korr.log`.

#### Action
Execute the Python script via the command line with the verbose (`-v`) flag enabled:

```bash
# Setup mock directory structure
export DW_DIR_ROOT="/tmp/mock_dw_root"
mkdir -p ${DW_DIR_ROOT}/pruef/is/sql
touch ${DW_DIR_ROOT}/pruef/is/sql/d_pfis_mps_vba_korrektur.sql

export DW_EintragsNr="99999"
export LogDatei="/tmp/test_pfis_mps_vba_korr.log"

# Run the migrated Python script
python3 local/home/gurunathan_t/single_job_demo_v3/r_pfis_mps_vba_korrektur.py -v
```

#### Pass/Fail Criterion
* **Pass**: 
  * The script exits with code `0`.
  * The log file `/tmp/test_pfis_mps_vba_korr.log` is created.
  * The log file contains the correct header metadata:
    ```text
    --------------------------- Job ------------------------------------
    Jobkennung :  PFIS_MPS_VBA_KORR
    Job-Nr     :  99999
    Logdatei   :  /tmp/test_pfis_mps_vba_korrektur.log
    --------------------------------------------------------------------
    ```
  * The log file ends with `Abarbeitung ohne erkennbare Fehler beendet`.
* **Fail**: The script exits with a non-zero code, the log file is not generated, or the log content is missing the required metadata headers.

---

## Section 2: Database Transformation & Parity Validation

### Test Case 2.1: SQL Transformation - Happy Path (Ebene 6 & 7 ID Correction)
#### Purpose
Verify that records in the fact table `dwh$ta_f_mps_nutzung` with valid text descriptions are correctly matched to the lookup view `dwh$vi_l_m2_vba`, their IDs are updated, and their raw text columns are nullified.

#### Setup
Create temporary test tables in BigQuery and populate them with happy-path test data:

```sql
-- Create Mock Fact Table
CREATE OR REPLACE TABLE `test_dataset.dwh$ta_f_mps_nutzung` AS (
  SELECT 
    1 AS row_id,
    9999 AS m2_vba_ebene6_id,
    'Vertriebsart_A' AS m2_vba_ebene6_text,
    8888 AS m2_vba_ebene7_id,
    'Vertriebsart_B' AS m2_vba_ebene7_text
);

-- Create Mock Lookup View/Table
CREATE OR REPLACE TABLE `test_dataset.dwh$vi_l_m2_vba` AS (
  SELECT 'Vertriebsart_A' AS m2_vba_ebene6_text, 'Vertriebsart_B' AS m2_vba_ebene7_text, 101 AS m2_vba_ebene7_id
  UNION ALL
  SELECT 'UNBEKANNT' AS m2_vba_ebene6_text, 'UNBEKANNT' AS m2_vba_ebene7_text, 9999 AS m2_vba_ebene7_id
);
```

#### Action
Execute the core logic of the migrated BigQuery SQL script (substituting the table names with the mock test tables):

```sql
-- Step 1: Update Ebene 6 ID
MERGE INTO `test_dataset.dwh$ta_f_mps_nutzung` AS n
USING (
  SELECT UPPER(m2_vba_ebene6_text) AS lookup_ebene6_text,
         MIN(m2_vba_ebene7_id) AS min_ebene7_id
    FROM `test_dataset.dwh$vi_l_m2_vba`
   GROUP BY 1
) AS v
ON UPPER(n.m2_vba_ebene6_text) = v.lookup_ebene6_text
   AND n.m2_vba_ebene6_text IS NOT NULL
WHEN MATCHED THEN
  UPDATE SET m2_vba_ebene6_id = COALESCE(v.min_ebene7_id, n.m2_vba_ebene6_id);

-- Step 2: Nullify Ebene 6 Text
UPDATE `test_dataset.dwh$ta_f_mps_nutzung` AS n
   SET m2_vba_ebene6_text = NULL
 WHERE n.m2_vba_ebene6_text IS NOT NULL
   AND n.m2_vba_ebene6_id <> (
      SELECT v.m2_vba_ebene7_id
        FROM `test_dataset.dwh$vi_l_m2_vba` AS v
       WHERE UPPER(v.m2_vba_ebene6_text) = 'UNBEKANNT'
       LIMIT 1
   );

-- Step 3: Update Ebene 7 ID
MERGE INTO `test_dataset.dwh$ta_f_mps_nutzung` AS n
USING (
  SELECT UPPER(m2_vba_ebene7_text) AS lookup_ebene7_text,
         MIN(m2_vba_ebene7_id) AS min_ebene7_id
    FROM `test_dataset.dwh$vi_l_m2_vba`
   GROUP BY 1
) AS v
ON UPPER(n.m2_vba_ebene7_text) = v.lookup_ebene7_text
   AND n.m2_vba_ebene7_text IS NOT NULL
WHEN MATCHED THEN
  UPDATE SET m2_vba_ebene7_id = COALESCE(v.min_ebene7_id, n.m2_vba_ebene7_id);

-- Step 4: Nullify Ebene 7 Text
UPDATE `test_dataset.dwh$ta_f_mps_nutzung` AS n
   SET m2_vba_ebene7_text = NULL
 WHERE n.m2_vba_ebene7_text IS NOT NULL
   AND n.m2_vba_ebene7_id <> (
      SELECT v.m2_vba_ebene7_id
        FROM `test_dataset.dwh$vi_l_m2_vba` AS v
       WHERE UPPER(v.m2_vba_ebene7_text) = 'UNBEKANNT'
       LIMIT 1
   );
```

Verify the results with the following assertion query:

```sql
SELECT 
  row_id,
  m2_vba_ebene6_id,
  m2_vba_ebene6_text,
  m2_vba_ebene7_id,
  m2_vba_ebene7_text
FROM `test_dataset.dwh$ta_f_mps_nutzung`
WHERE row_id = 1;
```

#### Pass/Fail Criterion
* **Pass**: The assertion query returns:
  * `m2_vba_ebene6_id` = `101`
  * `m2_vba_ebene6_text` IS `NULL`
  * `m2_vba_ebene7_id` = `101`
  * `m2_vba_ebene7_text` IS `NULL`
* **Fail**: Any of the IDs are not updated to `101`, or the text columns are not nullified.

---

### Test Case 2.2: SQL Transformation - 'UNBEKANNT' Fallback and Unmatched Records
#### Purpose
Verify that unmatched records retain their default IDs and text descriptions, and records matched to 'UNBEKANNT' retain their text descriptions (i.e., they are not nullified).

#### Setup
Populate the mock tables with unmatched and 'UNBEKANNT' test records:

```sql
INSERT INTO `test_dataset.dwh$ta_f_mps_nutzung` (row_id, m2_vba_ebene6_id, m2_vba_ebene6_text, m2_vba_ebene7_id, m2_vba_ebene7_text)
VALUES 
  -- Row 2: Unmatched text
  (2, 9999, 'NOT_IN_LOOKUP_6', 8888, 'NOT_IN_LOOKUP_7'),
  -- Row 3: Matched to UNBEKANNT
  (3, 9999, 'UNBEKANNT', 9999, 'UNBEKANNT');
```

#### Action
Execute the BigQuery SQL script (as defined in Test Case 2.1 Action) and run the assertion query:

```sql
SELECT 
  row_id,
  m2_vba_ebene6_id,
  m2_vba_ebene6_text,
  m2_vba_ebene7_id,
  m2_vba_ebene7_text
FROM `test_dataset.dwh$ta_f_mps_nutzung`
WHERE row_id IN (2, 3)
ORDER BY row_id;
```

#### Pass/Fail Criterion
* **Pass**: The assertion query returns:
  * **Row 2 (Unmatched)**:
    * `m2_vba_ebene6_id` = `9999` (unchanged)
    * `m2_vba_ebene6_text` = `'NOT_IN_LOOKUP_6'` (unchanged)
    * `m2_vba_ebene7_id` = `8888` (unchanged)
    * `m2_vba_ebene7_text` = `'NOT_IN_LOOKUP_7'` (unchanged)
  * **Row 3 (UNBEKANNT)**:
    * `m2_vba_ebene6_id` = `9999` (matched to UNBEKANNT ID)
    * `m2_vba_ebene6_text` = `'UNBEKANNT'` (NOT nullified)
    * `m2_vba_ebene7_id` = `9999` (matched to UNBEKANNT ID)
    * `m2_vba_ebene7_text` = `'UNBEKANNT'` (NOT nullified)
* **Fail**: Unmatched records are modified, or 'UNBEKANNT' text descriptions are nullified.

---

## Section 3: Edge Cases & Exception Handling

### Test Case 3.1: SQL Transformation - Case Insensitivity and NULL Handling
#### Purpose
Verify that text matching is case-insensitive (using `UPPER`) and that records with `NULL` text descriptions are completely ignored by the update logic.

#### Setup
Populate the mock tables with mixed-case and NULL test records:

```sql
INSERT INTO `test_dataset.dwh$ta_f_mps_nutzung` (row_id, m2_vba_ebene6_id, m2_vba_ebene6_text, m2_vba_ebene7_id, m2_vba_ebene7_text)
VALUES 
  -- Row 4: Mixed-case text (should match 'Vertriebsart_A' / 'Vertriebsart_B')
  (4, 9999, 'vErTrIeBsArT_a', 8888, 'VeRtRiEbSaRt_B'),
  -- Row 5: NULL text
  (5, 9999, NULL, 8888, NULL);
```

#### Action
Execute the BigQuery SQL script (as defined in Test Case 2.1 Action) and run the assertion query:

```sql
SELECT 
  row_id,
  m2_vba_ebene6_id,
  m2_vba_ebene6_text,
  m2_vba_ebene7_id,
  m2_vba_ebene7_text
FROM `test_dataset.dwh$ta_f_mps_nutzung`
WHERE row_id IN (4, 5)
ORDER BY row_id;
```

#### Pass/Fail Criterion
* **Pass**: The assertion query returns:
  * **Row 4 (Mixed-case)**:
    * `m2_vba_ebene6_id` = `101` (successfully matched)
    * `m2_vba_ebene6_text` IS `NULL` (nullified after match)
    * `m2_vba_ebene7_id` = `101` (successfully matched)
    * `m2_vba_ebene7_text` IS `NULL` (nullified after match)
  * **Row 5 (NULL)**:
    * `m2_vba_ebene6_id` = `9999` (unchanged)
    * `m2_vba_ebene6_text` IS `NULL`
    * `m2_vba_ebene7_id` = `8888` (unchanged)
    * `m2_vba_ebene7_text` IS `NULL`
* **Fail**: Mixed-case records fail to match, or NULL records are modified.

---

### Test Case 3.2: Transaction Rollback and Error Logging
#### Purpose
Verify that if an error occurs during execution, the transaction rolls back completely (no partial updates are committed) and the error logging procedure `dwpa_meldung_fehler` is called with the correct error details.

#### Setup
* Create a mock `dwpa_meldung_fehler` stored procedure in the test dataset to capture error logs:

```sql
CREATE OR REPLACE TABLE `test_dataset.error_log_table` (
  art STRING,
  eintrags_nr INT64,
  fehler_nr INT64,
  err_text STRING,
  err_code STRING,
  logged_at TIMESTAMP
);

CREATE OR REPLACE PROCEDURE `test_dataset.dwpa_meldung_fehler`(
  art STRING, 
  eintrags_nr INT64, 
  fehler_nr INT64, 
  err_text STRING, 
  err_code STRING
)
BEGIN
  INSERT INTO `test_dataset.error_log_table` (art, eintrags_nr, fehler_nr, err_text, err_code, logged_at)
  VALUES (art, eintrags_nr, fehler_nr, err_text, err_code, CURRENT_TIMESTAMP());
END;
```

* Reset the mock fact table to a known state:

```sql
CREATE OR REPLACE TABLE `test_dataset.dwh$ta_f_mps_nutzung` AS (
  SELECT 1 AS row_id, 9999 AS m2_vba_ebene6_id, 'Vertriebsart_A' AS m2_vba_ebene6_text, 8888 AS m2_vba_ebene7_id, 'Vertriebsart_B' AS m2_vba_ebene7_text
);
```

#### Action
Execute the migrated BigQuery SQL script inside a wrapper that forces an error (e.g., referencing a non-existent column or forcing a division by zero) after the first update statement:

```sql
DECLARE EintragsNr INT64;
DECLARE ErrText STRING;
DECLARE ErrC INT64;
DECLARE FehlerNr INT64;

SET EintragsNr = 12345;

BEGIN
  BEGIN TRANSACTION;

  -- This update should succeed initially
  MERGE INTO `test_dataset.dwh$ta_f_mps_nutzung` AS n
  USING (
    SELECT UPPER(m2_vba_ebene6_text) AS lookup_ebene6_text, MIN(m2_vba_ebene7_id) AS min_ebene7_id
      FROM `test_dataset.dwh$vi_l_m2_vba`
     GROUP BY 1
  ) AS v
  ON UPPER(n.m2_vba_ebene6_text) = v.lookup_ebene6_text AND n.m2_vba_ebene6_text IS NOT NULL
  WHEN MATCHED THEN
    UPDATE SET m2_vba_ebene6_id = COALESCE(v.min_ebene7_id, n.m2_vba_ebene6_id);

  -- FORCE AN ERROR: Division by zero
  SELECT 1 / 0;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;

  SET ErrText = @@error.message;
  SET ErrC = CAST(@@error.code AS INT64);
  SET FehlerNr = -20001;

  CALL `test_dataset.dwpa_meldung_fehler`('F', EintragsNr, FehlerNr, ErrText, CAST(ErrC AS STRING));
END;
```

Verify the rollback and logging with the following assertion queries:

```sql
-- Assertion 1: Verify no changes were committed to the fact table
SELECT m2_vba_ebene6_id 
  FROM `test_dataset.dwh$ta_f_mps_nutzung` 
 WHERE row_id = 1;

-- Assertion 2: Verify the error was logged in the error log table
SELECT art, eintrags_nr, fehler_nr, err_code 
  FROM `test_dataset.error_log_table`;
```

#### Pass/Fail Criterion
* **Pass**:
  * Assertion 1 returns `9999` (the update was successfully rolled back and not committed).
  * Assertion 2 returns exactly 1 row with `art` = `'F'`, `eintrags_nr` = `12345`, `fehler_nr` = `-20001`, and a non-null `err_code`.
* **Fail**: The fact table contains the updated ID `101` (rollback failed), or no error was logged in `error_log_table`.