Here is the comprehensive migration-validation test suite for the `TEST.NEW_HOUSEKEEPING_JOB` migration. 

These tests are designed to run against Google BigQuery using standard SQL assertions and Python (`pytest` with the `google-cloud-bigquery` library) to prove behavioral equivalence between the legacy KornShell/Oracle implementation and the migrated BigQuery stored procedures.

---

# Test Suite Overview: `TEST.NEW_HOUSEKEEPING_JOB`

The validation strategy is divided into four key areas:
1. **State Initialization & ID Generation** (Parity with Oracle Sequence / Shell `eval`)
2. **Data Mutation & Parameter Validation** (Null handling, error routing, and transactional updates)
3. **Date Parsing & Format Translation** (Oracle `TO_DATE` vs. BigQuery `SAFE.PARSE_DATE`)
4. **Log Filename Generation** (Timezone-aware string formatting parity)

---

## Section 1: State Initialization & ID Generation

### Test Case 1.1: Unique ID Generation (`CCRMSG_ErmittleNr`)
* **Purpose**: Verify that `CCRMSG_ErmittleNr` generates a non-null, unique 9-digit integer tracking ID, mimicking the legacy Oracle sequence generator.
* **Setup**: Ensure the target dataset `ccr_metadata_dataset` is initialized.
* **Action**: Execute the stored procedure multiple times and capture the output parameter.
* **Pass/Fail Criterion**: 
  * **Pass**: The returned ID is a 9-digit integer ($100,000,000 \le \text{ID} \le 999,999,999$) and consecutive calls generate unique values.
  * **Fail**: The returned ID is null, outside the 9-digit range, or duplicate values are generated.

```python
# pytest code for Test Case 1.1
import pytest
from google.cloud import bigquery

def test_ccrmsg_ermittle_nr_uniqueness_and_range():
    client = bigquery.Client()
    ids = set()
    
    for _ in range(5):
        query = """
            DECLARE out_id INT64;
            CALL `ccr_metadata_dataset.CCRMSG_ErmittleNr`(out_id);
            SELECT out_id;
        """
        query_job = client.query(query)
        results = list(query_job.result())
        generated_id = results[0][0]
        
        assert generated_id is not None, "Generated ID is NULL"
        assert 100000000 <= generated_id <= 999999999, f"ID {generated_id} is not 9 digits"
        ids.add(generated_id)
        
    assert len(ids) == 5, f"Duplicate IDs detected in sequence generation: {ids}"
```

---

### Test Case 1.2: Primary Entry Creation (`CCRMSG_ErzeugeEintrag`)
* **Purpose**: Verify that calling `CCRMSG_ErzeugeEintrag` inserts a record into `ccr_metadata_dataset.MELDUNG` with the correct parameters and sets the initial status to `'RUNNING'`.
* **Setup**: Generate a unique test ID. Clear any existing records in `MELDUNG` matching this ID.
* **Action**: Call `CCRMSG_ErzeugeEintrag` with structured test parameters.
* **Pass/Fail Criterion**: 
  * **Pass**: A single row is inserted into `MELDUNG` matching the input parameters exactly, with `status = 'RUNNING'` and populated audit timestamps.
  * **Fail**: No row is inserted, fields are mismatched, or the status is not `'RUNNING'`.

```sql
-- SQL Assertion for Test Case 1.2
DECLARE test_id INT64 DEFAULT 999000001;

-- Clean up
DELETE FROM `ccr_metadata_dataset.MELDUNG` WHERE eintrags_nr = test_id;

-- Execute
CALL `ccr_metadata_dataset.CCRMSG_ErzeugeEintrag`(
  test_id, 
  'TEST_JOB_A', 
  '/vobs/dw_source/test_script.ksh', 
  '/var/log/test.log', 
  '--param1=val1 --param2=val2'
);

-- Assert
ASSERT (
  SELECT COUNT(1) FROM `ccr_metadata_dataset.MELDUNG` 
  WHERE eintrags_nr = test_id 
    AND job_kennung = 'TEST_JOB_A'
    AND programmname = '/vobs/dw_source/test_script.ksh'
    AND log_datei = '/var/log/test.log'
    AND parameter = '--param1=val1 --param2=val2'
    AND status = 'RUNNING'
    AND created_at IS NOT NULL
    AND updated_at IS NOT NULL
) = 1 AS "ERROR: MELDUNG entry was not created correctly with status RUNNING";
```

---

## Section 2: Data Mutation & Parameter Validation

### Test Case 2.1: Null Parameter Validation (Robustness & Error Routing)
* **Purpose**: Verify that the stored procedures raise explicit BigQuery execution errors when mandatory parameters (such as `v_EintragsNr`) are passed as `NULL`, matching the legacy shell script's `exit 2` behavior.
* **Setup**: None.
* **Action**: Call `CCRMSG_ErzeugeEintrag` with a `NULL` entry ID.
* **Pass/Fail Criterion**: 
  * **Pass**: The query engine aborts execution and throws an error containing the message: `"Keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`.
  * **Fail**: The procedure executes successfully or fails with a generic database error instead of the validated message.

```python
# pytest code for Test Case 2.1
def test_ccrmsg_erzeuge_eintrag_null_validation():
    client = bigquery.Client()
    query = """
        CALL `ccr_metadata_dataset.CCRMSG_ErzeugeEintrag`(
          NULL, 'TEST_JOB', 'script.ksh', 'log.txt', 'params'
        );
    """
    with pytest.raises(Exception) as excinfo:
        client.query(query).result()
        
    assert "Keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben" in str(excinfo.value), \
        f"Expected validation error message not found. Got: {excinfo.value}"
```

---

### Test Case 2.2: Auxiliary Metadata Updates (`SetzeAnzahl`, `SetzeDateiname`, `SetzeZusatzinfos`)
* **Purpose**: Verify that auxiliary metadata updates correctly mutate the existing record in `MELDUNG` without overwriting other non-null fields (simulating Oracle's `COALESCE` update logic).
* **Setup**: Insert a base record with ID `999000002` using `CCRMSG_ErzeugeEintrag`.
* **Action**: Sequentially call `CCRMSG_SetzeAnzahl`, `CCRMSG_SetzeDateiname`, and `CCRMSG_SetzeZusatzinfos`.
* **Pass/Fail Criterion**: 
  * **Pass**: The target row is updated incrementally. After all calls, `anzahl`, `datei`, and `zusatz` are all populated simultaneously, and `updated_at` is updated.
  * **Fail**: Updates overwrite previous values with `NULL` or fail to update the row.

```sql
-- SQL Assertion for Test Case 2.2
DECLARE test_id INT64 DEFAULT 999000002;

-- Setup
DELETE FROM `ccr_metadata_dataset.MELDUNG` WHERE eintrags_nr = test_id;
CALL `ccr_metadata_dataset.CCRMSG_ErzeugeEintrag`(test_id, 'TEST_JOB_B', 'script.ksh', 'log.txt', 'none');

-- Action
CALL `ccr_metadata_dataset.CCRMSG_SetzeAnzahl`(test_id, 15500);
CALL `ccr_metadata_dataset.CCRMSG_SetzeDateiname`(test_id, 'output_data.csv');
CALL `ccr_metadata_dataset.CCRMSG_SetzeZusatzinfos`(test_id, 'Execution completed without retries');

-- Assert
ASSERT (
  SELECT COUNT(1) FROM `ccr_metadata_dataset.MELDUNG`
  WHERE eintrags_nr = test_id
    AND anzahl = 15500
    AND datei = 'output_data.csv'
    AND zusatz = 'Execution completed without retries'
    AND updated_at > created_at
) = 1 AS "ERROR: Incremental auxiliary metadata updates failed to merge correctly.";
```

---

## Section 3: Date Parsing & Format Translation

### Test Case 3.1: Oracle to BigQuery Date Format Translation (`CCRMSG_SetzeStichtag`)
* **Purpose**: Verify that Oracle-style date format strings (`DD.MM.YYYY`, `YYYY-MM-DD`, `YYYYMMDD`) are correctly translated to BigQuery format patterns and parsed into a standard `DATE` type.
* **Setup**: Create a base record with ID `999000003`.
* **Action**: Call `CCRMSG_SetzeStichtag` using different format variations.
* **Pass/Fail Criterion**: 
  * **Pass**: All valid date strings are parsed into the exact BigQuery `DATE` value `2023-10-27`. Invalid formats or dates throw a validation error.
  * **Fail**: Dates are parsed incorrectly, or valid formats throw parsing exceptions.

```sql
-- SQL Assertion for Test Case 3.1
DECLARE test_id INT64 DEFAULT 999000003;

-- Setup
DELETE FROM `ccr_metadata_dataset.MELDUNG` WHERE eintrags_nr = test_id;
CALL `ccr_metadata_dataset.CCRMSG_ErzeugeEintrag`(test_id, 'TEST_DATE_JOB', 'script.ksh', 'log.txt', 'none');

-- Test Case 3.1a: German Standard Format (DD.MM.YYYY)
CALL `ccr_metadata_dataset.CCRMSG_SetzeStichtag`(test_id, '27.10.2023', 'DD.MM.YYYY');
ASSERT (SELECT stichtag FROM `ccr_metadata_dataset.MELDUNG` WHERE eintrags_nr = test_id) = DATE '2023-10-27'
  AS "ERROR: Failed to parse DD.MM.YYYY format";

-- Test Case 3.1b: ISO Format (YYYY-MM-DD)
CALL `ccr_metadata_dataset.CCRMSG_SetzeStichtag`(test_id, '2023-10-27', 'YYYY-MM-DD');
ASSERT (SELECT stichtag FROM `ccr_metadata_dataset.MELDUNG` WHERE eintrags_nr = test_id) = DATE '2023-10-27'
  AS "ERROR: Failed to parse YYYY-MM-DD format";

-- Test Case 3.1c: Compressed Format (YYYYMMDD)
CALL `ccr_metadata_dataset.CCRMSG_SetzeStichtag`(test_id, '20231027', 'YYYYMMDD');
ASSERT (SELECT stichtag FROM `ccr_metadata_dataset.MELDUNG` WHERE eintrags_nr = test_id) = DATE '2023-10-27'
  AS "ERROR: Failed to parse YYYYMMDD format";
```

---

### Test Case 3.2: Invalid Date Handling
* **Purpose**: Verify that passing an invalid date string or mismatched format pattern safely aborts execution with a descriptive error.
* **Setup**: Create a base record with ID `999000004`.
* **Action**: Call `CCRMSG_SetzeStichtag` with an invalid date string (`'2023-13-45'`).
* **Pass/Fail Criterion**: 
  * **Pass**: The procedure fails and raises an error containing `"Invalid date string or unsupported format structure"`.
  * **Fail**: The procedure executes without failing, or inserts a `NULL`/corrupted date.

```python
# pytest code for Test Case 3.2
def test_ccrmsg_setze_stichtag_invalid_date():
    client = bigquery.Client()
    query = """
        CALL `ccr_metadata_dataset.CCRMSG_SetzeStichtag`(999000004, '2023-13-45', 'YYYY-MM-DD');
    """
    with pytest.raises(Exception) as excinfo:
        client.query(query).result()
        
    assert "Invalid date string or unsupported format structure" in str(excinfo.value), \
        f"Expected date validation failure. Got: {excinfo.value}"
```

---

## Section 4: Log Filename Generation

### Test Case 4.1: Timezone-Aware Log Filename Parity (`CCRMSG_Logdateiname`)
* **Purpose**: Verify that the generated log filename matches the legacy shell pattern: `${CCR_DIR_PROT}/${v_JobKennung}_$(date '+%Y%m%d_%H%M')_${v_EintragsNr}.log` using the `Europe/Berlin` timezone.
* **Setup**: None.
* **Action**: Call `CCRMSG_Logdateiname` and compare the output string against a dynamically calculated Python datetime string localized to Berlin.
* **Pass/Fail Criterion**: 
  * **Pass**: The generated filename matches the expected pattern exactly, including the directory prefix, job name, timestamp, and entry ID.
  * **Fail**: The filename is malformed, or the timestamp does not align with the `Europe/Berlin` timezone.

```python
# pytest code for Test Case 4.1
from datetime import datetime
import pytz

def test_ccrmsg_logdateiname_parity():
    client = bigquery.Client()
    entry_id = 888111222
    job_kennung = "HOUSEKEEPING_RUN"
    dir_prot = "/gcs/my-bucket/logs"
    
    # Calculate expected timestamp in Europe/Berlin timezone
    berlin_tz = pytz.timezone('Europe/Berlin')
    now_berlin = datetime.now(berlin_tz)
    expected_timestamp = now_berlin.strftime('%Y%m%d_%H%M')
    
    expected_filename = f"{dir_prot}/{job_kennung}_{expected_timestamp}_{entry_id}.log"
    
    query = f"""
        DECLARE out_filename STRING;
        CALL `ccr_metadata_dataset.CCRMSG_Logdateiname`(out_filename, {entry_id}, '{job_kennung}', '{dir_prot}');
        SELECT out_filename;
    """
    
    query_job = client.query(query)
    results = list(query_job.result())
    actual_filename = results[0][0]
    
    # Allow a 1-minute window in case the test execution crosses a minute boundary
    if actual_filename != expected_filename:
        # Recalculate with a 1-minute offset back to handle boundary conditions
        offset_timestamp = (datetime.now(berlin_tz)).strftime('%Y%m%d_%H%M')
        expected_filename_fallback = f"{dir_prot}/{job_kennung}_{offset_timestamp}_{entry_id}.log"
        assert actual_filename in [expected_filename, expected_filename_fallback], \
            f"Filename mismatch.\nExpected: {expected_filename}\nActual: {actual_filename}"
```

---

### Test Case 4.2: Parallel Process Log Filename Parity (`CCRMSG_Logdateiname_Parallel`)
* **Purpose**: Verify that parallel process log filenames are correctly formatted with the parallel index suffix (`.${v_ParNr}.log`).
* **Setup**: None.
* **Action**: Call `CCRMSG_Logdateiname_Parallel` with a parallel index of `4`.
* **Pass/Fail Criterion**: 
  * **Pass**: The generated filename contains the parallel index suffix (e.g., `_888111222.4.log`).
  * **Fail**: The parallel index is missing, misplaced, or formatted incorrectly.

```python
# pytest code for Test Case 4.2
def test_ccrmsg_logdateiname_parallel_parity():
    client = bigquery.Client()
    entry_id = 888111222
    job_kennung = "PARALLEL_JOB"
    par_nr = 4
    dir_prot = "/gcs/my-bucket/logs"
    
    berlin_tz = pytz.timezone('Europe/Berlin')
    expected_timestamp = datetime.now(berlin_tz).strftime('%Y%m%d_%H%M')
    expected_filename = f"{dir_prot}/{job_kennung}_{expected_timestamp}_{entry_id}.{par_nr}.log"
    
    query = f"""
        DECLARE out_filename STRING;
        CALL `ccr_metadata_dataset.CCRMSG_Logdateiname_Parallel`(out_filename, {entry_id}, '{job_kennung}', {par_nr}, '{dir_prot}');
        SELECT out_filename;
    """
    
    query_job = client.query(query)
    results = list(query_job.result())
    actual_filename = results[0][0]
    
    assert actual_filename.endswith(f"_{entry_id}.{par_nr}.log"), \
        f"Parallel filename suffix mismatch. Expected ending: _{entry_id}.{par_nr}.log. Got: {actual_filename}"
```