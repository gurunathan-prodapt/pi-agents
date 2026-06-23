As a senior data-migration QA engineer, I've analyzed the migration design for `k_ausd_v_ta_action_assoc.ksh` to `sp_ausd_v_ta_action_assoc` in BigQuery. The migration involves refactoring a KornShell orchestration script into a BigQuery Stored Procedure, replacing shell utilities with BigQuery native features, and managing job state through dedicated control tables.

A critical observation is the absence of the core SQL logic from `d_ausd_v_ta_action_assoc.sql`. For the purpose of these tests, I will assume a simplified, testable behavior for this core logic: it updates rows in `project.dataset.ta_action_assoc` where `entry_nr` matches `p_EintragsNr` and `status` is 'pending', setting their `status` to 'processed'. The record count will then be based on these 'processed' rows.

Another significant point is the "ignoring active jobs" and "deactivating old active jobs" logic mentioned in the legacy script's purpose. This logic is not explicitly translated into the provided BigQuery Stored Procedure pseudocode. This represents a potential behavioral divergence that needs to be addressed in testing.

The following test cases are designed to validate the migrated BigQuery Stored Procedure against the legacy KornShell script's behavior, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests for `sp_ausd_v_ta_action_assoc`

### Pre-requisites for all Tests

Before running any tests, ensure the following BigQuery DDLs and the Stored Procedure are deployed to the `project.dataset` environment.

```sql
-- DDL for job_control table
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING NOT NULL,
    status STRING NOT NULL,
    created_ts TIMESTAMP NOT NULL,
    finished_ts TIMESTAMP,
    record_count INT64
);

-- DDL for job_result table
CREATE TABLE IF NOT EXISTS `project.dataset.job_result` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING NOT NULL,
    records INT64 NOT NULL,
    result_ts TIMESTAMP NOT NULL
);

-- DDL for error_log table
CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    error_ts TIMESTAMP NOT NULL,
    error_code INT64 NOT NULL,
    error_arg STRING,
    job_kennung STRING,
    eintrags_nr STRING,
    message STRING NOT NULL
);

-- DDL for ta_action_assoc table (mock schema for testing)
CREATE TABLE IF NOT EXISTS `project.dataset.ta_action_assoc` (
    entry_nr STRING NOT NULL,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- BigQuery Stored Procedure (modified EXECUTE IMMEDIATE for testing)
CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_action_assoc`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_action_assoc';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_sql STRING;
  DECLARE v_error_message STRING DEFAULT '';
  DECLARE v_error_code INT64 DEFAULT 0;

  -- Parameter validation (replaces shell getopts and pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_error_code = 193;
    SET v_error_message = 'Jobkennung';
  END IF;

  IF v_error_code = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_error_code = 193;
    SET v_error_message = 'EintragsNr';
  END IF;

  IF v_error_code != 0 THEN
    -- Equivalent to DWMSG_MeldeFehler / echo / exit
    INSERT INTO `project.dataset.error_log`
      (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
    VALUES
      (CURRENT_TIMESTAMP(), v_error_code, v_error_message, p_JobKennung, p_EintragsNr,
       CONCAT('FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message));

    RAISE USING MESSAGE = CONCAT('Bitte ueber Rahmenscript aufrufen | FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message);
  END IF;

  -- Optional job control initialization (replaces job table updates)
  INSERT INTO `project.dataset.job_control`
    (job_kennung, eintrags_nr, tab_name, status, created_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'STARTED', CURRENT_TIMESTAMP());

  BEGIN
    -- Downstream SQL script equivalent (replaces starteSQLSkript)
    -- MOCK: Simulates d_ausd_v_ta_action_assoc.sql updating 'pending' records
    EXECUTE IMMEDIATE """
      UPDATE `project.dataset.ta_action_assoc`
      SET status = 'processed', updated_at = CURRENT_TIMESTAMP()
      WHERE entry_nr = @p_EintragsNr AND status = 'pending';
    """
    USING p_EintragsNr AS p_EintragsNr;

    -- Record count equivalent to temp file read
    -- Counts records that were just 'processed' by this job execution
    SET v_records = (
      SELECT COUNT(*)
      FROM `project.dataset.ta_action_assoc`
      WHERE entry_nr = p_EintragsNr
        AND status = 'processed'
        AND updated_at >= (SELECT created_ts FROM `project.dataset.job_control` WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr ORDER BY created_ts DESC LIMIT 1)
    );

    -- Persist record count instead of temp file
    INSERT INTO `project.dataset.job_result`
      (job_kennung, eintrags_nr, tab_name, records, result_ts)
    VALUES
      (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

    -- Mark job complete
    UPDATE `project.dataset.job_control`
    SET status = 'FINISHED',
        finished_ts = CURRENT_TIMESTAMP(),
        record_count = v_records
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND tab_name = v_TabName;

  EXCEPTION WHEN ERROR THEN
    -- Handle errors during SQL execution
    SET v_error_message = @@error.message;
    SET v_error_code = -1; -- Or derive a more specific code

    INSERT INTO `project.dataset.error_log`
      (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
    VALUES
      (CURRENT_TIMESTAMP(), v_error_code, 'SQL Execution Error', p_JobKennung, p_EintragsNr, v_error_message);

    UPDATE `project.dataset.job_control`
    SET status = 'FAILED',
        finished_ts = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND tab_name = v_TabName;

    RAISE USING MESSAGE = CONCAT('SQL Execution Failed for job ', p_JobKennung, ' / ', p_EintragsNr, ': ', v_error_message);
  END;

END;
```

---

### Test Case 1: Successful Execution (Happy Path)

**Purpose:** To verify that the BigQuery Stored Procedure executes successfully with valid parameters, updates the target table, records the correct row count, and correctly logs job status. This covers output parity, transformation correctness (record count), and external system replacement (job control tables).

**Setup:**
1.  Clear `job_control`, `job_result`, `error_log`, and `ta_action_assoc` tables.
2.  Insert mock data into `ta_action_assoc`:
    ```sql
    INSERT INTO `project.dataset.ta_action_assoc` (entry_nr, status, created_at) VALUES
    ('ENTRY_001', 'pending', CURRENT_TIMESTAMP()),
    ('ENTRY_001', 'pending', CURRENT_TIMESTAMP()),
    ('ENTRY_002', 'pending', CURRENT_TIMESTAMP()),
    ('ENTRY_003', 'completed', CURRENT_TIMESTAMP());
    ```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters:
```sql
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('JOB_A', 'ENTRY_001');
```

**Pass/Fail Criterion:**
*   The procedure completes without raising an error.
*   `project.dataset.job_control` contains one entry for `('JOB_A', 'ENTRY_001', 'ta_action_assoc')` with `status = 'FINISHED'` and `record_count = 2`.
*   `project.dataset.job_result` contains one entry for `('JOB_A', 'ENTRY_001', 'ta_action_assoc')` with `records = 2`.
*   `project.dataset.error_log` is empty.
*   `project.dataset.ta_action_assoc` has two rows with `entry_nr = 'ENTRY_001'` and `status = 'processed'`. The row with `entry_nr = 'ENTRY_002'` should remain 'pending', and 'ENTRY_003' should remain 'completed'.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
import time

client = bigquery.Client()
PROJECT_ID = "project" # Replace with your project ID
DATASET_ID = "dataset" # Replace with your dataset ID

def _clear_tables():
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_result`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`").result()

def test_successful_execution():
    _clear_tables()
    job_kennung = 'JOB_A'
    eintrags_nr = 'ENTRY_001'
    expected_records = 2

    # Setup: Insert mock data
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc` (entry_nr, status, created_at) VALUES
        ('{eintrags_nr}', 'pending', CURRENT_TIMESTAMP()),
        ('{eintrags_nr}', 'pending', CURRENT_TIMESTAMP()),
        ('ENTRY_002', 'pending', CURRENT_TIMESTAMP()),
        ('ENTRY_003', 'completed', CURRENT_TIMESTAMP());
    """).result()

    # Action: Execute SP
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung}', '{eintrags_nr}');").result()

    # Assertions
    job_control_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    job_result_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_result`").result())
    error_log_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.error_log`").result())
    ta_action_assoc_rows = list(client.query(f"SELECT entry_nr, status FROM `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc` ORDER BY entry_nr, status").result())

    assert len(job_control_rows) == 1
    assert job_control_rows[0].job_kennung == job_kennung
    assert job_control_rows[0].eintrags_nr == eintrags_nr
    assert job_control_rows[0].status == 'FINISHED'
    assert job_control_rows[0].record_count == expected_records

    assert len(job_result_rows) == 1
    assert job_result_rows[0].job_kennung == job_kennung
    assert job_result_rows[0].eintrags_nr == eintrags_nr
    assert job_result_rows[0].records == expected_records

    assert len(error_log_rows) == 0

    processed_count = sum(1 for row in ta_action_assoc_rows if row.entry_nr == eintrags_nr and row.status == 'processed')
    pending_count = sum(1 for row in ta_action_assoc_rows if row.entry_nr == 'ENTRY_002' and row.status == 'pending')
    completed_count = sum(1 for row in ta_action_assoc_rows if row.entry_nr == 'ENTRY_003' and row.status == 'completed')

    assert processed_count == expected_records
    assert pending_count == 1
    assert completed_count == 1
```

---

### Test Case 2: Missing `p_JobKennung` Parameter

**Purpose:** To verify that the BigQuery Stored Procedure correctly handles missing `p_JobKennung` by raising an error and logging it, mirroring the legacy script's parameter validation. This covers transformation correctness (parameter handling) and external system replacement (error logging).

**Setup:**
1.  Clear `job_control`, `job_result`, and `error_log` tables.

**Action:**
Attempt to execute the BigQuery Stored Procedure with `p_JobKennung` as `NULL` or an empty string.
```sql
-- Using NULL
CALL `project.dataset.sp_ausd_v_ta_action_assoc`(NULL, 'ENTRY_001');

-- Using empty string
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('', 'ENTRY_001');
```

**Pass/Fail Criterion:**
*   The procedure raises an error (e.g., `RAISE USING MESSAGE`) with a message indicating a missing parameter, similar to "Bitte ueber Rahmenscript aufrufen | FEHLER: 0 E 193 Jobkennung".
*   `project.dataset.job_control` remains empty (no `STARTED` entry).
*   `project.dataset.job_result` remains empty.
*   `project.dataset.error_log` contains one entry with `error_code = 193` and `error_arg = 'Jobkennung'`.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest # For catching BigQuery errors

client = bigquery.Client()
PROJECT_ID = "project"
DATASET_ID = "dataset"

def _clear_tables():
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_result`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`").result()

@pytest.mark.parametrize("job_kennung_param", [None, ''])
def test_missing_job_kennung_parameter(job_kennung_param):
    _clear_tables()
    eintrags_nr = 'ENTRY_001'
    expected_error_code = 193
    expected_error_arg = 'Jobkennung'

    # Action: Execute SP with missing JobKennung
    with pytest.raises(BadRequest) as excinfo:
        if job_kennung_param is None:
            client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`(NULL, '{eintrags_nr}');").result()
        else:
            client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung_param}', '{eintrags_nr}');").result()

    # Assertions on error message
    assert f"FEHLER: 0 E {expected_error_code} {expected_error_arg}" in str(excinfo.value)

    # Assertions on table states
    job_control_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    job_result_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_result`").result())
    error_log_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.error_log`").result())

    assert len(job_control_rows) == 0
    assert len(job_result_rows) == 0
    assert len(error_log_rows) == 1
    assert error_log_rows[0].error_code == expected_error_code
    assert error_log_rows[0].error_arg == expected_error_arg
```

---

### Test Case 3: Missing `p_EintragsNr` Parameter

**Purpose:** To verify that the BigQuery Stored Procedure correctly handles missing `p_EintragsNr` by raising an error and logging it, mirroring the legacy script's parameter validation. This covers transformation correctness (parameter handling) and external system replacement (error logging).

**Setup:**
1.  Clear `job_control`, `job_result`, and `error_log` tables.

**Action:**
Attempt to execute the BigQuery Stored Procedure with `p_EintragsNr` as `NULL` or an empty string.
```sql
-- Using NULL
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('JOB_B', NULL);

-- Using empty string
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('JOB_B', '');
```

**Pass/Fail Criterion:**
*   The procedure raises an error (e.g., `RAISE USING MESSAGE`) with a message indicating a missing parameter, similar to "Bitte ueber Rahmenscript aufrufen | FEHLER: 0 E 193 EintragsNr".
*   `project.dataset.job_control` remains empty (no `STARTED` entry).
*   `project.dataset.job_result` remains empty.
*   `project.dataset.error_log` contains one entry with `error_code = 193` and `error_arg = 'EintragsNr'`.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

client = bigquery.Client()
PROJECT_ID = "project"
DATASET_ID = "dataset"

def _clear_tables():
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_result`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`").result()

@pytest.mark.parametrize("eintrags_nr_param", [None, ''])
def test_missing_eintrags_nr_parameter(eintrags_nr_param):
    _clear_tables()
    job_kennung = 'JOB_B'
    expected_error_code = 193
    expected_error_arg = 'EintragsNr'

    # Action: Execute SP with missing EintragsNr
    with pytest.raises(BadRequest) as excinfo:
        if eintrags_nr_param is None:
            client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung}', NULL);").result()
        else:
            client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung}', '{eintrags_nr_param}');").result()

    # Assertions on error message
    assert f"FEHLER: 0 E {expected_error_code} {expected_error_arg}" in str(excinfo.value)

    # Assertions on table states
    job_control_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    job_result_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_result`").result())
    error_log_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.error_log`").result())

    assert len(job_control_rows) == 0
    assert len(job_result_rows) == 0
    assert len(error_log_rows) == 1
    assert error_log_rows[0].error_code == expected_error_code
    assert error_log_rows[0].error_arg == expected_error_arg
```

---

### Test Case 4: Core SQL Logic Failure

**Purpose:** To verify that the BigQuery Stored Procedure correctly handles errors originating from the `EXECUTE IMMEDIATE` block (representing `d_ausd_v_ta_action_assoc.sql`), logging the error and marking the job as `FAILED`. This covers transformation correctness (error handling) and external system replacement (job control and error logging).

**Setup:**
1.  Clear `job_control`, `job_result`, and `error_log` tables.
2.  **Temporarily modify `sp_ausd_v_ta_action_assoc` to force an error in the `EXECUTE IMMEDIATE` block.** For example, introduce a syntax error or a division by zero.
    ```sql
    -- Example modification to force error (DO NOT DEPLOY TO PROD)
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_action_assoc`(
      IN p_JobKennung STRING,
      IN p_EintragsNr STRING
    )
    BEGIN
      -- ... (previous code) ...
      BEGIN
        EXECUTE IMMEDIATE """
          SELECT 1 / 0; -- This will cause a division by zero error
        """;
        -- ... (rest of the block) ...
      EXCEPTION WHEN ERROR THEN
        -- ... (error handling) ...
      END;
    END;
    ```

**Action:**
Execute the BigQuery Stored Procedure with valid parameters.
```sql
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('JOB_C', 'ENTRY_003');
```

**Pass/Fail Criterion:**
*   The procedure raises an error (due to the `RAISE` in the `EXCEPTION` block).
*   `project.dataset.job_control` contains one entry for `('JOB_C', 'ENTRY_003', 'ta_action_assoc')` with `status = 'FAILED'`. `record_count` should be `NULL`.
*   `project.dataset.job_result` remains empty (as the core logic failed before recording results).
*   `project.dataset.error_log` contains one entry with `error_code = -1` (or a more specific code if implemented) and `error_arg = 'SQL Execution Error'`, and a message containing the BigQuery error details (e.g., "division by zero").

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

client = bigquery.Client()
PROJECT_ID = "project"
DATASET_ID = "dataset"

def _clear_tables():
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_result`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`").result()

# Helper to deploy the SP with a forced error
def _deploy_error_sp():
    error_sp_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`(
      IN p_JobKennung STRING,
      IN p_EintragsNr STRING
    )
    BEGIN
      DECLARE v_TabName STRING DEFAULT 'ta_action_assoc';
      DECLARE v_records INT64 DEFAULT 0;
      DECLARE v_sql STRING;
      DECLARE v_error_message STRING DEFAULT '';
      DECLARE v_error_code INT64 DEFAULT 0;

      IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
        SET v_error_code = 193;
        SET v_error_message = 'Jobkennung';
      END IF;

      IF v_error_code = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
        SET v_error_code = 193;
        SET v_error_message = 'EintragsNr';
      END IF;

      IF v_error_code != 0 THEN
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.error_log`
          (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
        VALUES
          (CURRENT_TIMESTAMP(), v_error_code, v_error_message, p_JobKennung, p_EintragsNr,
           CONCAT('FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message));
        RAISE USING MESSAGE = CONCAT('Bitte ueber Rahmenscript aufrufen | FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message);
      END IF;

      INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_control`
        (job_kennung, eintrags_nr, tab_name, status, created_ts)
      VALUES
        (p_JobKennung, p_EintragsNr, v_TabName, 'STARTED', CURRENT_TIMESTAMP());

      BEGIN
        EXECUTE IMMEDIATE \"\"\"
          SELECT 1 / 0; -- FORCED ERROR: Division by zero
        \"\"\";

        SET v_records = (
          SELECT COUNT(*)
          FROM `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`
          WHERE entry_nr = p_EintragsNr
            AND status = 'processed'
            AND updated_at >= (SELECT created_ts FROM `{PROJECT_ID}.{DATASET_ID}.job_control` WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr ORDER BY created_ts DESC LIMIT 1)
        );

        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_result`
          (job_kennung, eintrags_nr, tab_name, records, result_ts)
        VALUES
          (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

        UPDATE `{PROJECT_ID}.{DATASET_ID}.job_control`
        SET status = 'FINISHED',
            finished_ts = CURRENT_TIMESTAMP(),
            record_count = v_records
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND tab_name = v_TabName;

      EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_code = -1;

        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.error_log`
          (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
        VALUES
          (CURRENT_TIMESTAMP(), v_error_code, 'SQL Execution Error', p_JobKennung, p_EintragsNr, v_error_message);

        UPDATE `{PROJECT_ID}.{DATASET_ID}.job_control`
        SET status = 'FAILED',
            finished_ts = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND tab_name = v_TabName;

        RAISE USING MESSAGE = CONCAT('SQL Execution Failed for job ', p_JobKennung, ' / ', p_EintragsNr, ': ', v_error_message);
      END;
    END;
    """
    client.query(error_sp_sql).result()

# Helper to deploy the original SP (without forced error)
def _deploy_original_sp():
    original_sp_sql = """
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_action_assoc`(
      IN p_JobKennung STRING,
      IN p_EintragsNr STRING
    )
    BEGIN
      DECLARE v_TabName STRING DEFAULT 'ta_action_assoc';
      DECLARE v_records INT64 DEFAULT 0;
      DECLARE v_sql STRING;
      DECLARE v_error_message STRING DEFAULT '';
      DECLARE v_error_code INT64 DEFAULT 0;

      IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
        SET v_error_code = 193;
        SET v_error_message = 'Jobkennung';
      END IF;

      IF v_error_code = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
        SET v_error_code = 193;
        SET v_error_message = 'EintragsNr';
      END IF;

      IF v_error_code != 0 THEN
        INSERT INTO `project.dataset.error_log`
          (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
        VALUES
          (CURRENT_TIMESTAMP(), v_error_code, v_error_message, p_JobKennung, p_EintragsNr,
           CONCAT('FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message));
        RAISE USING MESSAGE = CONCAT('Bitte ueber Rahmenscript aufrufen | FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message);
      END IF;

      INSERT INTO `project.dataset.job_control`
        (job_kennung, eintrags_nr, tab_name, status, created_ts)
      VALUES
        (p_JobKennung, p_EintragsNr, v_TabName, 'STARTED', CURRENT_TIMESTAMP());

      BEGIN
        EXECUTE IMMEDIATE \"\"\"
          UPDATE `project.dataset.ta_action_assoc`
          SET status = 'processed', updated_at = CURRENT_TIMESTAMP()
          WHERE entry_nr = @p_EintragsNr AND status = 'pending';
        \"\"\"
        USING p_EintragsNr AS p_EintragsNr;

        SET v_records = (
          SELECT COUNT(*)
          FROM `project.dataset.ta_action_assoc`
          WHERE entry_nr = p_EintragsNr
            AND status = 'processed'
            AND updated_at >= (SELECT created_ts FROM `project.dataset.job_control` WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr ORDER BY created_ts DESC LIMIT 1)
        );

        INSERT INTO `project.dataset.job_result`
          (job_kennung, eintrags_nr, tab_name, records, result_ts)
        VALUES
          (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

        UPDATE `project.dataset.job_control`
        SET status = 'FINISHED',
            finished_ts = CURRENT_TIMESTAMP(),
            record_count = v_records
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND tab_name = v_TabName;

      EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_code = -1;

        INSERT INTO `project.dataset.error_log`
          (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
        VALUES
          (CURRENT_TIMESTAMP(), v_error_code, 'SQL Execution Error', p_JobKennung, p_EintragsNr, v_error_message);

        UPDATE `project.dataset.job_control`
        SET status = 'FAILED',
            finished_ts = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND tab_name = v_TabName;

        RAISE USING MESSAGE = CONCAT('SQL Execution Failed for job ', p_JobKennung, ' / ', p_EintragsNr, ': ', v_error_message);
      END;
    END;
    """
    client.query(original_sp_sql).result()


def test_core_sql_logic_failure():
    _clear_tables()
    _deploy_error_sp() # Deploy the SP with the forced error
    job_kennung = 'JOB_C'
    eintrags_nr = 'ENTRY_003'

    # Action: Execute SP
    with pytest.raises(BadRequest) as excinfo:
        client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung}', '{eintrags_nr}');").result()

    # Assertions on error message
    assert "SQL Execution Failed" in str(excinfo.value)
    assert "division by zero" in str(excinfo.value) # Specific error from the forced failure

    # Assertions on table states
    job_control_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    job_result_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_result`").result())
    error_log_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.error_log`").result())

    assert len(job_control_rows) == 1
    assert job_control_rows[0].job_kennung == job_kennung
    assert job_control_rows[0].eintrags_nr == eintrags_nr
    assert job_control_rows[0].status == 'FAILED'
    assert job_control_rows[0].record_count is None # No record count on failure

    assert len(job_result_rows) == 0 # No job result on failure

    assert len(error_log_rows) == 1
    assert error_log_rows[0].error_code == -1
    assert error_log_rows[0].error_arg == 'SQL Execution Error'
    assert "division by zero" in error_log_rows[0].message

    _deploy_original_sp() # Revert to the original SP after the test
```

---

### Test Case 5: Data Quality - Fixed Table Name and Record Count Logic

**Purpose:** To verify that the `v_TabName` variable is correctly used and that the record count logic accurately reflects the changes made by the core SQL, even when no records are processed. This covers transformation correctness (fixed table name, record count) and data quality/row count assertions.

**Setup:**
1.  Clear `job_control`, `job_result`, `error_log`, and `ta_action_assoc` tables.
2.  Insert mock data into `ta_action_assoc` where no rows will match the `p_EintragsNr` for processing.
    ```sql
    INSERT INTO `project.dataset.ta_action_assoc` (entry_nr, status, created_at) VALUES
    ('ENTRY_005', 'pending', CURRENT_TIMESTAMP()),
    ('ENTRY_006', 'completed', CURRENT_TIMESTAMP());
    ```

**Action:**
Execute the BigQuery Stored Procedure with parameters that will result in zero processed records.
```sql
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('JOB_D', 'ENTRY_004');
```

**Pass/Fail Criterion:**
*   The procedure completes without raising an error.
*   `project.dataset.job_control` contains one entry for `('JOB_D', 'ENTRY_004', 'ta_action_assoc')` with `status = 'FINISHED'` and `record_count = 0`.
*   `project.dataset.job_result` contains one entry for `('JOB_D', 'ENTRY_004', 'ta_action_assoc')` with `records = 0`.
*   `project.dataset.error_log` is empty.
*   `project.dataset.ta_action_assoc` remains unchanged.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
import time

client = bigquery.Client()
PROJECT_ID = "project"
DATASET_ID = "dataset"

def _clear_tables():
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_result`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`").result()

def test_zero_records_processed():
    _clear_tables()
    job_kennung = 'JOB_D'
    eintrags_nr = 'ENTRY_004' # This entry_nr does not exist in ta_action_assoc
    expected_records = 0

    # Setup: Insert mock data that won't be processed by this job
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc` (entry_nr, status, created_at) VALUES
        ('ENTRY_005', 'pending', CURRENT_TIMESTAMP()),
        ('ENTRY_006', 'completed', CURRENT_TIMESTAMP());
    """).result()

    # Action: Execute SP
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung}', '{eintrags_nr}');").result()

    # Assertions
    job_control_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    job_result_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_result`").result())
    error_log_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.error_log`").result())
    ta_action_assoc_rows = list(client.query(f"SELECT entry_nr, status FROM `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc` ORDER BY entry_nr, status").result())

    assert len(job_control_rows) == 1
    assert job_control_rows[0].job_kennung == job_kennung
    assert job_control_rows[0].eintrags_nr == eintrags_nr
    assert job_control_rows[0].tab_name == 'ta_action_assoc' # Verify fixed table name
    assert job_control_rows[0].status == 'FINISHED'
    assert job_control_rows[0].record_count == expected_records

    assert len(job_result_rows) == 1
    assert job_result_rows[0].job_kennung == job_kennung
    assert job_result_rows[0].eintrags_nr == eintrags_nr
    assert job_result_rows[0].tab_name == 'ta_action_assoc' # Verify fixed table name
    assert job_result_rows[0].records == expected_records

    assert len(error_log_rows) == 0

    # Verify ta_action_assoc remains unchanged
    assert len(ta_action_assoc_rows) == 2
    assert all(row.status != 'processed' for row in ta_action_assoc_rows)
```

---

### Test Case 6: Behavioral Divergence - Concurrent Execution / "Ignoring Active Jobs"

**Purpose:** To highlight and verify the behavioral difference regarding concurrent job execution and the "ignoring active jobs" logic. The legacy script explicitly states "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert," which is not implemented in the provided BigQuery SP pseudocode. This test will demonstrate the current BigQuery behavior.

**Setup:**
1.  Clear `job_control`, `job_result`, `error_log`, and `ta_action_assoc` tables.
2.  Insert mock data into `ta_action_assoc`:
    ```sql
    INSERT INTO `project.dataset.ta_action_assoc` (entry_nr, status, created_at) VALUES
    ('ENTRY_CONCURRENT', 'pending', CURRENT_TIMESTAMP()),
    ('ENTRY_CONCURRENT', 'pending', CURRENT_TIMESTAMP());
    ```

**Action:**
Execute the BigQuery Stored Procedure twice in quick succession with the *same* `JobKennung` and `EintragsNr`.
```sql
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('JOB_CONCURRENT', 'ENTRY_CONCURRENT');
CALL `project.dataset.sp_ausd_v_ta_action_assoc`('JOB_CONCURRENT', 'ENTRY_CONCURRENT');
```

**Pass/Fail Criterion:**
*   Both procedure calls complete successfully (unless the core SQL logic has unique constraint issues).
*   `project.dataset.job_control` contains **two** entries for `('JOB_CONCURRENT', 'ENTRY_CONCURRENT', 'ta_action_assoc')`, both with `status = 'FINISHED'`. This indicates the job was *not* ignored.
*   `project.dataset.job_result` contains **two** entries for `('JOB_CONCURRENT', 'ENTRY_CONCURRENT', 'ta_action_assoc')`.
*   `project.dataset.ta_action_assoc` rows with `entry_nr = 'ENTRY_CONCURRENT'` will have their `status` set to 'processed' by the first run, and the second run will attempt to update them again (potentially affecting `updated_at` timestamp or doing nothing if `status` is already 'processed' and the `WHERE` clause prevents re-processing). The `record_count` for the second run might be 0 if the `WHERE` clause `status = 'pending'` prevents re-updates.

**Expected Outcome (Behavioral Divergence):**
The BigQuery SP will execute both times, creating duplicate job control/result entries. The `record_count` for the second execution will likely be 0 because the `status = 'pending'` condition in the mock `EXECUTE IMMEDIATE` block will no longer be met. This is a **FAIL** against the legacy script's stated purpose of "aktive Jobs werden ignoriert".

**Recommendation:** The BigQuery Stored Procedure needs additional logic (e.g., a check against `job_control` for an existing `STARTED` or `FINISHED` job with the same `JobKennung` and `EintragsNr` within a recent timeframe) to replicate the "ignoring active jobs" and "deactivating old active jobs" behavior.

**Runnable Test Code (Pytest with BigQuery client):**
```python
import pytest
from google.cloud import bigquery
import time

client = bigquery.Client()
PROJECT_ID = "project"
DATASET_ID = "dataset"

def _clear_tables():
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_result`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`").result()

def test_concurrent_execution_behavioral_divergence():
    _clear_tables()
    job_kennung = 'JOB_CONCURRENT'
    eintrags_nr = 'ENTRY_CONCURRENT'
    initial_pending_records = 2

    # Setup: Insert mock data
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc` (entry_nr, status, created_at) VALUES
        ('{eintrags_nr}', 'pending', CURRENT_TIMESTAMP()),
        ('{eintrags_nr}', 'pending', CURRENT_TIMESTAMP());
    """).result()

    # Action: Execute SP twice
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung}', '{eintrags_nr}');").result()
    # Introduce a small delay if needed, but for this test, immediate re-run is fine
    time.sleep(1) # Ensure timestamps are slightly different for job_control
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_action_assoc`('{job_kennung}', '{eintrags_nr}');").result()

    # Assertions
    job_control_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control` ORDER BY created_ts").result())
    job_result_rows = list(client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_result` ORDER BY result_ts").result())
    ta_action_assoc_rows = list(client.query(f"SELECT entry_nr, status FROM `{PROJECT_ID}.{DATASET_ID}.ta_action_assoc`").result())

    # This is the key divergence: two entries instead of one or an ignored second run
    assert len(job_control_rows) == 2
    assert job_control_rows[0].job_kennung == job_kennung
    assert job_control_rows[0].eintrags_nr == eintrags_nr
    assert job_control_rows[0].status == 'FINISHED'
    assert job_control_rows[0].record_count == initial_pending_records # First run processes all

    assert job_control_rows[1].job_kennung == job_kennung
    assert job_control_rows[1].eintrags_nr == eintrags_nr
    assert job_control_rows[1].status == 'FINISHED'
    assert job_control_rows[1].record_count == 0 # Second run finds no 'pending' records

    assert len(job_result_rows) == 2
    assert job_result_rows[0].records == initial_pending_records
    assert job_result_rows[1].records == 0

    # Verify ta_action_assoc state
    processed_count = sum(1 for row in ta_action_assoc_rows if row.entry_nr == eintrags_nr and row.status == 'processed')
    assert processed_count == initial_pending_records # All should be processed by the first run

    # This test case is designed to FAIL if the legacy behavior of "ignoring active jobs" is expected.
    # The current BigQuery SP behavior is to run both, which is a divergence.
    # A passing test here means the current BigQuery SP behavior is confirmed,
    # but it highlights a missing feature compared to the legacy system.
    print("\n--- Behavioral Divergence Detected ---")
    print("Legacy system: 'aktive Jobs werden ignoriert'.")
    print(f"BigQuery SP: Executed twice, resulting in {len(job_control_rows)} job_control entries.")
    print(f"First run processed {job_control_rows[0].record_count} records.")
    print(f"Second run processed {job_control_rows[1].record_count} records (as no 'pending' records remained).")
    print("This indicates the 'ignoring active jobs' logic is NOT implemented in the BigQuery SP.")
    assert False, "Behavioral divergence: Concurrent jobs are not ignored by the BigQuery SP."
```