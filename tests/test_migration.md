As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `DW.BERT_AUSD_BP_TA_P_BASISPROD` job migration. These tests aim to prove behavioral equivalence between the legacy Oracle-based ETL and the new GCP (Airflow, PySpark, BigQuery) implementation.

Given that the "legacy source" and "generated migration code" are unavailable, these tests are designed based on the detailed "Migration Design Document." The runnable code examples are illustrative and conceptual, demonstrating the *type* of assertions and comparisons that would be made once the actual code and data are available.

---

## Migration Validation Tests for DW.BERT_AUSD_BP_TA_P_BASISPROD

### Test Case 1: End-to-End Output Parity (Golden Dataset Comparison)

*   **Purpose:** To verify that the migrated job, when executed with identical logical inputs, produces an output in the target BigQuery table (`bert_dwh.SOF_TA_P_BASISPROD`) that is byte-for-byte identical to the output produced by the legacy Oracle job (`SOF$TA_P_BASISPROD`). This is the ultimate test of behavioral equivalence.
*   **Setup:**
    1.  **Legacy Golden Run:** Identify a specific, successful execution of the legacy `DW.BERT_AUSD_BP_TA_P_BASISPROD` job in the Oracle environment. Record all input parameters (e.g., `Stichtag`).
    2.  **Source Data Snapshot:** Create a precise snapshot of all Oracle source tables (`SOF$TA_CNTRCT_DIST`, `SOF$TA_CNTRCT_EVN`, `SOF$TA_ICCID_VERTRAG`, `SOF$TA_RN_VERTRAG`, `SOF$TA_RN_DA_VDA_TK`, `SOF$TA_TARIFOPTION`, `SOF$TA_APN_VERTRAG`, `SOF$TA_BCP_ICCID`, `SOF$TA_BCP_MSISDN`, `isbert_schema.dwtk_meldungen`) *immediately before* the chosen legacy run.
    3.  **Legacy Target Output:** Capture the full content of the Oracle target table `SOF$TA_P_BASISPROD` *immediately after* the chosen legacy run. This is the "golden output."
    4.  **Migrated Source Data:** Load the source data snapshots from step 2 into their corresponding BigQuery tables (`bert_dwh.SOF_TA_CNTRCT_DIST`, etc.). Ensure data types, NULLs, and values are accurately preserved.
    5.  **Clean Target:** Ensure the BigQuery target table `bert_dwh.SOF_TA_P_BASISPROD` is empty before the migrated job execution.
*   **Action:**
    1.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` using the *exact same logical parameters* (e.g., `Stichtag`) as the legacy golden run.
    2.  Wait for the Airflow DAG and its associated Dataproc job to complete successfully.
*   **Pass/Fail Criterion:**
    *   The migrated job completes successfully without any errors.
    *   A deep data comparison between the content of `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` (after migration) and the `SOF$TA_P_BASISPROD` legacy golden output yields *zero differences*. This includes row count, column values, and data types.

```sql
-- Conceptual SQL for deep data comparison in BigQuery
-- Assumes the legacy golden output has been loaded into a BigQuery staging table
-- e.g., `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD_LEGACY_GOLDEN`

-- Find rows present in migrated but not in legacy golden
SELECT 'Only in Migrated' AS source, *
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`
EXCEPT DISTINCT
SELECT 'Only in Migrated' AS source, *
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD_LEGACY_GOLDEN`;

-- Find rows present in legacy golden but not in migrated
SELECT 'Only in Legacy Golden' AS source, *
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD_LEGACY_GOLDEN`
EXCEPT DISTINCT
SELECT 'Only in Legacy Golden' AS source, *
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`;

-- Pass if both queries return 0 rows.
```

### Test Case 2: Row Count Parity

*   **Purpose:** To quickly verify that the total number of records inserted into the target table is consistent between the legacy and migrated jobs. This serves as a primary sanity check for data completeness.
*   **Setup:**
    1.  Use the same setup as Test Case 1, ensuring the legacy golden output row count is known.
*   **Action:**
    1.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` with the same parameters as the legacy golden run.
*   **Pass/Fail Criterion:**
    *   The row count of `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` in BigQuery must exactly match the row count of the legacy `SOF$TA_P_BASISPROD` golden output.

```sql
-- BigQuery assertion
SELECT COUNT(*) FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`;
-- Expected result: [Legacy_Golden_Row_Count]
```

### Test Case 3: Schema Parity and Data Type Handling

*   **Purpose:** To ensure that the BigQuery target table schema (`bert_dwh.SOF_TA_P_BASISPROD`) accurately reflects the Oracle legacy table schema (`SOF$TA_P_BASISPROD`), including column names, data types, and nullability, and that data type conversions are correct.
*   **Setup:**
    1.  Obtain the precise schema definition (DDL or metadata) for the Oracle `SOF$TA_P_BASISPROD` table.
    2.  Ensure the migrated `bert_dwh.SOF_TA_P_BASISPROD` table exists in BigQuery.
*   **Action:**
    1.  Query the schema of `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` using BigQuery's `INFORMATION_SCHEMA` or client libraries.
    2.  Compare the BigQuery schema definition against the documented Oracle schema.
*   **Pass/Fail Criterion:**
    *   All columns from the Oracle schema are present in the BigQuery schema.
    *   Column names match (case-insensitivity should be handled if applicable, BigQuery preserves case but queries are often case-insensitive).
    *   BigQuery data types are appropriate and lossless mappings of Oracle data types (e.g., `VARCHAR2(X)` to `STRING`, `NUMBER(P,S)` to `NUMERIC`/`BIGNUMERIC`/`FLOAT64` based on precision/scale, `DATE` to `DATE`, `TIMESTAMP` to `TIMESTAMP`).
    *   Nullability constraints are correctly translated (e.g., Oracle `NOT NULL` maps to BigQuery `REQUIRED` mode).
    *   Specifically verify the `MS3` through `MS10` MultiSIM fields for correct type mapping.

```python
# Example pytest assertion (conceptual, requires BigQuery client and Oracle metadata access)
import pytest
from google.cloud import bigquery

def test_target_schema_parity():
    bq_client = bigquery.Client()
    bq_table_id = "your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD"
    bq_table = bq_client.get_table(bq_table_id)
    bq_schema = {field.name.upper(): field for field in bq_table.schema} # Normalize for comparison

    # This dictionary would be populated from the Oracle DDL or metadata
    oracle_schema_expected = {
        "VERTRAG_ID": {"type": "NUMBER(10)", "nullable": False},
        "BASISPRODUKT_NAME": {"type": "VARCHAR2(100)", "nullable": True},
        "GUELTIG_AB": {"type": "DATE", "nullable": False},
        "MS3": {"type": "VARCHAR2(20)", "nullable": True}, # Example of specific field
        # ... all other expected columns and their Oracle types/nullability
    }

    assert len(bq_schema) == len(oracle_schema_expected), \
        f"Column count mismatch. Expected {len(oracle_schema_expected)}, got {len(bq_schema)}"

    for col_name, oracle_def in oracle_schema_expected.items():
        assert col_name in bq_schema, f"Column '{col_name}' missing in BigQuery schema."
        bq_field = bq_schema[col_name]

        # Assert type mapping (simplified, needs detailed mapping logic)
        if "VARCHAR2" in oracle_def["type"]:
            assert bq_field.field_type == "STRING", \
                f"Type mismatch for '{col_name}'. Expected STRING, got {bq_field.field_type}"
        elif "NUMBER" in oracle_def["type"]:
            # More robust logic needed for precision/scale to map to INT64/NUMERIC/FLOAT64
            assert bq_field.field_type in ["INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64"], \
                f"Type mismatch for '{col_name}'. Expected numeric type, got {bq_field.field_type}"
        elif "DATE" in oracle_def["type"]:
            assert bq_field.field_type == "DATE", \
                f"Type mismatch for '{col_name}'. Expected DATE, got {bq_field.field_type}"
        # Add more type mappings as needed (e.g., TIMESTAMP, BOOLEAN)

        # Assert nullability
        if not oracle_def["nullable"]:
            assert bq_field.mode == "REQUIRED", \
                f"Nullability mismatch for '{col_name}'. Expected REQUIRED, got {bq_field.mode}"
        else:
            assert bq_field.mode == "NULLABLE", \
                f"Nullability mismatch for '{col_name}'. Expected NULLABLE, got {bq_field.mode}"
```

### Test Case 4: Transformation Correctness - `v_datum` Calculation

*   **Purpose:** To verify that the critical `v_datum` variable, derived from `isbert_schema.dwtk_meldungen.timecreated`, is calculated identically in BigQuery SQL as it was in the Oracle SQLPlus script. This variable often influences date-based filtering or partitioning.
*   **Setup:**
    1.  Populate `your_gcp_project.bert_dwh.dwtk_meldungen` with a controlled dataset, including various `timecreated` values (e.g., different dates, NULLs, multiple entries for the same date).
    2.  Execute the Oracle SQL snippet `SELECT TO_CHAR(MAX(m.timecreated),'YYYYMMDD') FROM isbert_schema.dwtk_meldungen m;` against the corresponding Oracle table and record the exact result.
*   **Action:**
    1.  Execute the BigQuery SQL equivalent: `SELECT FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)) FROM your_gcp_project.bert_dwh.dwtk_meldungen AS m;`
*   **Pass/Fail Criterion:**
    *   The `v_datum` value returned by the BigQuery query must exactly match the value obtained from the Oracle legacy system for the same input data.

```sql
-- BigQuery assertion for v_datum calculation
SELECT FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)) AS v_datum_bq
FROM `your_gcp_project.bert_dwh.dwtk_meldungen` AS m;
-- Expected result: 'YYYYMMDD' (e.g., '20231026'), matching Oracle output.
```

### Test Case 5: Transformation Correctness - Joins, Filters, and Function Mappings

*   **Purpose:** To verify that complex SQL logic, including Oracle `(+)` outer joins, `DECODE` functions, `NVL` functions, and date formatting, are correctly translated to BigQuery Standard SQL (`LEFT JOIN`, `CASE WHEN`, `IFNULL`/`COALESCE`, `FORMAT_TIMESTAMP`/`FORMAT_DATE`) and produce identical results.
*   **Setup:**
    1.  Create small, controlled datasets for all relevant source tables in BigQuery. These datasets should specifically include scenarios that test:
        *   Rows with matching and non-matching join keys for `LEFT JOIN` (Oracle `(+)`).
        *   Rows with NULL values in join keys.
        *   Values that trigger different branches of `DECODE`/`CASE WHEN`.
        *   Columns with NULLs for `NVL`/`IFNULL` testing.
        *   Various date values for `TO_CHAR`/`FORMAT_DATE` testing.
        *   Rows that should be filtered out by `WHERE` clauses.
    2.  Manually execute the core `SELECT` statement (without `INSERT INTO` or `TRUNCATE`) in Oracle SQLPlus against these controlled datasets and record the exact result set.
*   **Action:**
    1.  Execute the equivalent core `SELECT` statement from `d_ausd_bp_ta_p_basisprod.bqsql` in BigQuery against the controlled datasets.
*   **Pass/Fail Criterion:**
    *   The result set (row count, column values, data types) from the BigQuery `SELECT` query must exactly match the result set from the Oracle `SELECT` query for all test scenarios.

```sql
-- Example BigQuery SELECT statement (illustrative, based on design doc)
-- This represents the core logic of d_ausd_bp_ta_p_basisprod.bqsql
SELECT
    cd.CONTRACT_ID,
    cd.DISTRIBUTION_KEY,
    ev.EVENT_TYPE,
    CASE
        WHEN iv.STATUS_CODE = 'A' THEN 'Active'
        WHEN iv.STATUS_CODE = 'I' THEN 'Inactive'
        ELSE 'Unknown'
    END AS CONTRACT_STATUS_DESC, -- Oracle DECODE to BQ CASE WHEN
    IFNULL(rv.REVENUE_AMOUNT, 0.0) AS REVENUE_AMOUNT_SAFE, -- Oracle NVL to BQ IFNULL
    FORMAT_DATE('%Y%m%d', cd.START_DATE) AS FORMATTED_START_DATE, -- Oracle TO_CHAR to BQ FORMAT_DATE
    apn.APN_NAME,
    bci.ICCID_VALUE,
    bcm.MSISDN_VALUE,
    -- ... all other columns and transformations, including MS3-MS10
FROM
    `your_gcp_project.bert_dwh.SOF_TA_CNTRCT_DIST` AS cd
LEFT JOIN
    `your_gcp_project.bert_dwh.SOF_TA_CNTRCT_EVN` AS ev ON cd.CONTRACT_ID = ev.CONTRACT_ID
LEFT JOIN
    `your_gcp_project.bert_dwh.SOF_TA_ICCID_VERTRAG` AS iv ON cd.ICCID = iv.ICCID -- Oracle (+) becomes LEFT JOIN
LEFT JOIN
    `your_gcp_project.bert_dwh.SOF_TA_RN_VERTRAG` AS rv ON cd.RN_ID = rv.RN_ID
LEFT JOIN
    `your_gcp_project.bert_dwh.SOF_TA_APN_VERTRAG` AS apn ON cd.APN_ID = apn.APN_ID
LEFT JOIN
    `your_gcp_project.bert_dwh.SOF_TA_BCP_ICCID` AS bci ON cd.BCP_ICCID_ID = bci.ID
LEFT JOIN
    `your_gcp_project.bert_dwh.SOF_TA_BCP_MSISDN` AS bcm ON cd.BCP_MSISDN_ID = bcm.ID
WHERE
    cd.EFFECTIVE_DATE <= PARSE_DATE('%Y%m%d', '20231026') -- Example filter using v_datum (hardcoded for test)
    AND cd.IS_ACTIVE = TRUE;

-- Compare this result set to the Oracle equivalent.
```

### Test Case 6: Parameter Handling and Date Validation (PySpark Wrapper)

*   **Purpose:** To verify that the `r_ausd_bp_ta_p_basisprod.py` PySpark script correctly parses command-line parameters (`-j`, `-f`, `-s`, `-l`), applies default values, and performs date validation exactly as the original KornShell scripts (`k_ausd_bp_ta_p_basisprod.ksh`, `r_ausd_bp_ta_p_basisprod.ksh`).
*   **Setup:**
    1.  Prepare a test environment (e.g., local machine with Spark installed, or a Dataproc cluster) where the PySpark script can be executed directly.
    2.  Define a comprehensive set of test cases for parameters:
        *   All parameters provided with valid values.
        *   Missing optional parameters (expecting correct default values).
        *   Invalid date format for `Stichtag` (e.g., `YYYY-MM-DD` instead of `YYYYMMDD`).
        *   Valid date for `Stichtag` (e.g., `20231026`).
        *   Edge case dates for `Stichtag` (e.g., `20240229` for a leap year, `20230230` for an invalid date).
*   **Action:**
    1.  Execute the `r_ausd_bp_ta_p_basisprod.py` script with each defined parameter combination.
    2.  Capture the script's standard output, standard error, and exit code.
*   **Pass/Fail Criterion:**
    *   The script successfully parses all valid parameters and correctly assigns values to internal variables.
    *   Default values for optional parameters (`EintragsNr`, `Wiederanlaufwert`) are correctly applied when omitted.
    *   Invalid date inputs for `Stichtag` result in an error message (matching legacy behavior) and a non-zero exit code.
    *   Valid date inputs are correctly processed and formatted for subsequent BigQuery SQL execution.
    *   Logging output (e.g., parameter values, execution steps) matches the expected behavior of the legacy KornShell scripts.

```python
# Example pytest for PySpark script (conceptual, requires a test harness for the script)
import pytest
import subprocess
import datetime

def run_pyspark_script_and_capture(params):
    """Simulates running the PySpark script and captures output."""
    command = ["spark-submit", "r_ausd_bp_ta_p_basisprod.py"] + params
    # In a real test, you might mock BigQuery client calls to avoid actual BQ execution
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return result

def test_pyspark_parameter_parsing_and_date_validation():
    # Test 1: All valid parameters provided
    result = run_pyspark_script_and_capture(["-j", "TESTJOB", "-f", "20231026", "-s", "1", "-l", "/tmp/LOGFILE.log"])
    assert result.returncode == 0, f"Script failed with valid params: {result.stderr}"
    assert "JobKennung: TESTJOB" in result.stdout # Assuming script logs parsed params
    assert "Stichtag: 20231026" in result.stdout
    assert "EintragsNr: 1" in result.stdout
    assert "Logfile: /tmp/LOGFILE.log" in result.stdout

    # Test 2: Missing optional parameters (expecting defaults)
    result = run_pyspark_script_and_capture(["-j", "TESTJOB", "-f", "20231026"])
    assert result.returncode == 0, f"Script failed with missing params: {result.stderr}"
    assert "EintragsNr: 0" in result.stdout # Assuming 0 is the default
    assert "Wiederanlaufwert: 0" in result.stdout # Assuming 0 is the default

    # Test 3: Invalid date format for Stichtag
    result = run_pyspark_script_and_capture(["-j", "TESTJOB", "-f", "2023-10-26"])
    assert result.returncode != 0, "Script should fail for invalid date format"
    assert "Invalid date format" in result.stderr or "Date validation failed" in result.stderr # Check for specific error message

    # Test 4: Valid edge case date (leap year)
    result = run_pyspark_script_and_capture(["-j", "TESTJOB", "-f", "20240229"])
    assert result.returncode == 0, f"Script failed for leap year date: {result.stderr}"
    assert "Stichtag: 20240229" in result.stdout

    # Test 5: Date validation for non-existent date
    result = run_pyspark_script_and_capture(["-j", "TESTJOB", "-f", "20230230"])
    assert result.returncode != 0, "Script should fail for non-existent date"
    assert "Invalid date" in result.stderr or "Date validation failed" in result.stderr
```

### Test Case 7: External System Replacement - BigQuery Access and DDL Operations

*   **Purpose:** To verify that the PySpark script can successfully connect to BigQuery, execute DDL (specifically `TRUNCATE TABLE`), and perform DML (`INSERT`) operations as intended by the `d_ausd_bp_ta_p_basisprod.bqsql` script.
*   **Setup:**
    1.  Ensure the Dataproc cluster's service account has the necessary BigQuery permissions (e.g., `bigquery.dataEditor`, `bigquery.metadataViewer`) for the `bert_dwh` dataset.
    2.  Pre-populate `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` with some dummy data to confirm truncation.
*   **Action:**
    1.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    2.  Monitor Dataproc job logs for BigQuery client initialization, query execution messages, and any permission errors.
    3.  Immediately after the job starts (before the `INSERT`), query `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` to check if it's truncated.
    4.  After the job completes, query `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` to confirm data has been inserted.
*   **Pass/Fail Criterion:**
    *   The PySpark script successfully initializes a BigQuery client.
    *   The `TRUNCATE TABLE` command is executed successfully, resulting in an empty `bert_dwh.SOF_TA_P_BASISPROD` table before the `INSERT` operation.
    *   The `INSERT` operation completes without BigQuery-related errors.
    *   No permission errors are reported in Dataproc or BigQuery logs.
    *   The final row count of `bert_dwh.SOF_TA_P_BASISPROD` matches the expected count (from Test Case 2).

```sql
-- BigQuery assertion (to be run after the job starts, before INSERT)
SELECT COUNT(*) FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`;
-- Expected result: 0 (confirming truncation)

-- BigQuery assertion (to be run after the job completes)
SELECT COUNT(*) FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`;
-- Expected result: > 0 (confirming data insertion, matching legacy row count)
```

### Test Case 8: NULL Handling and Edge Cases

*   **Purpose:** To specifically test how the migrated job handles NULL values in various contexts (join keys, function inputs, filter conditions) and other edge cases (e.g., empty source tables, all NULLs in a critical column), ensuring consistency with legacy Oracle behavior.
*   **Setup:**
    1.  Create multiple small, targeted BigQuery datasets for source tables, each designed to test a specific NULL or edge case scenario:
        *   **Scenario A:** Source table with NULLs in a column used as a join key.
        *   **Scenario B:** Source table with NULLs in a column used as input to `IFNULL`/`COALESCE`.
        *   **Scenario C:** Source table with NULLs in a column used in a `CASE WHEN` statement.
        *   **Scenario D:** A source table that is completely empty.
        *   **Scenario E:** `your_gcp_project.bert_dwh.dwtk_meldungen` is empty or `timecreated` column is all NULLs (to test `v_datum` calculation under stress).
    2.  For each scenario, determine the expected output in `SOF$TA_P_BASISPROD` based on Oracle's known behavior.
*   **Action:**
    1.  For each scenario, load the specific test dataset into BigQuery source tables.
    2.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    3.  Inspect the resulting data in `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`.
*   **Pass/Fail Criterion:**
    *   The output `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` for each test scenario exactly matches the expected output derived from Oracle's behavior.
    *   Specifically, `LEFT JOIN` correctly introduces NULLs for non-matching right-side rows.
    *   `IFNULL`/`COALESCE` correctly substitutes NULLs with the specified default value.
    *   `CASE WHEN` statements handle NULL inputs as expected (e.g., `ELSE` branch or specific NULL handling).
    *   Empty source tables result in an empty target table or specific error handling, consistent with legacy.
    *   `v_datum` calculation handles empty/all-NULL `timecreated` gracefully (e.g., `MAX(NULL)` results in `NULL`, which might then be handled by subsequent logic or cause an error if not expected).

```sql
-- Example: Test for NULL in join key and IFNULL
-- Setup (conceptual):
-- bert_dwh.SOF_TA_CNTRCT_DIST: (contract_id='C1', value='V1'), (contract_id='C2', value='V2', description=NULL)
-- bert_dwh.SOF_TA_CNTRCT_EVN: (contract_id='C1', event_desc='E1'), (contract_id=NULL, event_desc='E2')
-- Expected output after join and IFNULL(description, 'N/A'):
-- C1, V1, E1, 'N/A'
-- C2, V2, NULL, 'N/A'

-- SQL to verify (simplified):
SELECT
    cd.CONTRACT_ID,
    cd.VALUE,
    ev.EVENT_DESC,
    IFNULL(cd.DESCRIPTION, 'N/A') AS DESCRIPTION_CLEAN
FROM
    `your_gcp_project.bert_dwh.SOF_TA_CNTRCT_DIST` AS cd
LEFT JOIN
    `your_gcp_project.bert_dwh.SOF_TA_CNTRCT_EVN` AS ev ON cd.CONTRACT_ID = ev.CONTRACT_ID;
```

### Test Case 9: Data Quality Assertions

*   **Purpose:** To ensure the migrated data adheres to expected data quality rules, checking for unexpected NULLs, duplicates, or out-of-range values that might indicate a transformation error.
*   **Setup:**
    1.  Define key data quality rules based on business requirements and observations from the legacy system (e.g., `CONTRACT_ID` should never be NULL, `GUELTIG_AB` should be a valid date, no duplicate primary keys).
    2.  Run the migrated job with a representative dataset (e.g., the golden dataset from Test Case 1).
*   **Action:**
    1.  After the migrated job completes, execute a series of BigQuery SQL queries to validate data quality rules on `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`.
*   **Pass/Fail Criterion:**
    *   All data quality assertion queries return 0 rows, indicating no violations.

```sql
-- Example BigQuery data quality assertions
-- Check for unexpected NULLs in critical columns
SELECT 'FAIL: NULL in CONTRACT_ID' AS issue, COUNT(*)
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`
WHERE CONTRACT_ID IS NULL;
-- Expected: 0

-- Check for duplicate primary keys (assuming a primary key exists)
SELECT 'FAIL: Duplicate Primary Key' AS issue, PRIMARY_KEY_COLUMN, COUNT(*)
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`
GROUP BY PRIMARY_KEY_COLUMN
HAVING COUNT(*) > 1;
-- Expected: 0

-- Check for invalid date ranges (e.g., dates in the future if not expected)
SELECT 'FAIL: Future Date in GUELTIG_AB' AS issue, COUNT(*)
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`
WHERE GUELTIG_AB > CURRENT_DATE();
-- Expected: 0 (or specific count if future dates are allowed)

-- Check for unexpected values in categorical columns
SELECT 'FAIL: Invalid STATUS_CODE' AS issue, STATUS_CODE, COUNT(*)
FROM `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD`
WHERE STATUS_CODE NOT IN ('Active', 'Inactive', 'Pending'); -- Example valid values
-- Expected: 0
```