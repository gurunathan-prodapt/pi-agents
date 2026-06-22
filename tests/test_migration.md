As a senior data-migration QA engineer, I've analyzed the migration design and the provided source/target code for `k_ausd_bp_ta_bcp_iccid.ksh`. The migration involves re-platforming a KornShell-orchestrated Oracle PL/SQL job to Airflow and BigQuery.

The following test cases are designed to ensure behavioral equivalence, data integrity, and correctness across all aspects of the migration.

---

## Migration Validation Tests for `k_ausd_bp_ta_bcp_iccid.ksh`

### Test Setup Prerequisites (Applies to all tests)

Before running any tests, ensure the following:

1.  **Environment Setup:**
    *   An Oracle database instance with the original source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, `sof$ta_bcp_iccid`) populated with representative test data.
    *   A Google Cloud Platform project with BigQuery enabled.
    *   BigQuery datasets `isbert_schema` and `sof` created.
    *   BigQuery tables (`isbert_schema.dwtk_meldungen`, `sof.ta_bpr_bcp`, `sof.ta_iccid_vertrag`, `sof.ta_bcp_iccid`) created using the provided DDLs.
    *   An Airflow environment (Cloud Composer) with the `k_ausd_bp_ta_bcp_iccid_dag.py` DAG deployed.
    *   Appropriate BigQuery connections configured in Airflow.

2.  **Data Synchronization:**
    *   For each test case, ensure that the *input* data in the BigQuery source tables (`isbert_schema.dwtk_meldungen`, `sof.ta_bpr_bcp`, `sof.ta_iccid_vertrag`) is an exact, byte-for-byte replica of the input data in their corresponding Oracle source tables. This is critical for output parity validation.
    *   The target table (`sof$ta_bcp_iccid` in Oracle, `sof.ta_bcp_iccid` in BigQuery) should be empty before each test run, or its state should be known and accounted for.

3.  **Test Data Scenarios:**
    *   **Happy Path:** Representative data covering typical scenarios.
    *   **Edge Cases:**
        *   Empty source tables.
        *   No matching records in join.
        *   Source tables with duplicate records (to test `DISTINCT`).
        *   `NULL` values in join keys and selected columns.
        *   `timecreated` in `dwtk_meldungen` being `NULL` or having no matching `job_kennung`.

---

### 1. Output Parity & Transformation Correctness Tests

#### Test Case 1.1: Full Data Parity (Happy Path)

*   **Purpose:** To verify that the BigQuery transformation produces an identical dataset in the target table (`sof.ta_bcp_iccid`) compared to the Oracle legacy job (`sof$ta_bcp_iccid`) for a typical, valid input. This covers the core join, `DISTINCT`, and column selection logic.
*   **Setup:**
    1.  Populate Oracle tables `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` with a diverse set of valid records, including some that will join and some that won't, and some that would result in duplicates without `DISTINCT`.
    2.  Populate Oracle table `isbert_schema.dwtk_meldungen` with at least one record where `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` is a valid timestamp.
    3.  Ensure BigQuery source tables (`sof.ta_bpr_bcp`, `sof.ta_iccid_vertrag`, `isbert_schema.dwtk_meldungen`) contain *identical* data to their Oracle counterparts.
    4.  Ensure both Oracle `sof$ta_bcp_iccid` and BigQuery `sof.ta_bcp_iccid` tables are empty.
*   **Action:**
    1.  Execute the legacy Oracle job (`k_ausd_bp_ta_bcp_iccid.ksh`) with standard parameters (e.g., `p_JobKennung='TEST_JOB'`, `p_EintragsNr='1'`, `p_Stichtag='20230101'`).
    2.  Execute the migrated Airflow DAG (`k_ausd_bp_ta_bcp_iccid_dag.py`) with equivalent parameters.
*   **Pass/Fail Criterion:**
    *   The number of rows in Oracle `sof$ta_bcp_iccid` must be equal to the number of rows in BigQuery `sof.ta_bcp_iccid`.
    *   A full data comparison (e.g., using checksums or row-by-row comparison after sorting) between the two target tables must show no differences.

    ```sql
    -- BigQuery Assertion (after both jobs have run)
    -- This query assumes a temporary table or CTE for Oracle results if direct comparison isn't possible.
    -- For actual testing, you'd typically extract Oracle data to a file and load it into a BigQuery temp table.

    -- Step 1: Get row counts
    SELECT COUNT(*) FROM `sof.ta_bcp_iccid`; -- BigQuery count
    -- Compare with Oracle count: SELECT COUNT(*) FROM sof$ta_bcp_iccid;

    -- Step 2: Full data comparison (assuming Oracle data is loaded into a temp_oracle_sof_ta_bcp_iccid table in BQ)
    -- This query identifies rows present in BigQuery but not in Oracle, or vice-versa.
    -- It assumes column names and types are identical.
    SELECT 'Only in BigQuery' AS source, * FROM `sof.ta_bcp_iccid`
    EXCEPT DISTINCT
    SELECT 'Only in BigQuery' AS source, * FROM `temp_oracle_sof_ta_bcp_iccid`

    UNION ALL

    SELECT 'Only in Oracle' AS source, * FROM `temp_oracle_sof_ta_bcp_iccid`
    EXCEPT DISTINCT
    SELECT 'Only in Oracle' AS source, * FROM `sof.ta_bcp_iccid`;

    -- Pass if the above query returns 0 rows.
    ```

#### Test Case 1.2: `v_datum` Calculation Parity

*   **Purpose:** To verify that the `v_datum` variable, derived from `isbert_schema.dwtk_meldungen`, is calculated identically in BigQuery as in Oracle, including `NVL`/`COALESCE` and date formatting.
*   **Setup:**
    1.  Populate Oracle `isbert_schema.dwtk_meldungen` with various `timecreated` values, including:
        *   Multiple records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with different `timecreated`.
        *   Records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` where `timecreated` is `NULL`.
        *   No records for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  Ensure BigQuery `isbert_schema.dwtk_meldungen` contains *identical* data.
*   **Action:**
    1.  Manually execute the `v_datum` calculation part in Oracle SQL*Plus:
        ```sql
        COLUMN s_datum new_value v_datum noprint
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
          FROM isbert_schema.dwtk_meldungen m
         WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        SELECT '&v_datum' FROM DUAL; -- To see the value
        ```
    2.  Manually execute the `v_datum` declaration part in BigQuery:
        ```sql
        DECLARE v_datum STRING DEFAULT (
          SELECT COALESCE(
            FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))),
            '19000101'
          )
          FROM `isbert_schema.dwtk_meldungen` AS m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        SELECT v_datum;
        ```
*   **Pass/Fail Criterion:** The `v_datum` value returned by the BigQuery query must be exactly the same as the value returned by the Oracle query for each setup scenario.

#### Test Case 1.3: Empty Source Tables

*   **Purpose:** To ensure the job handles empty source tables gracefully, resulting in an empty target table.
*   **Setup:**
    1.  Ensure Oracle tables `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, and `isbert_schema.dwtk_meldungen` are all empty.
    2.  Ensure BigQuery source tables (`sof.ta_bpr_bcp`, `sof.ta_iccid_vertrag`, `isbert_schema.dwtk_meldungen`) are all empty.
    3.  Ensure both Oracle `sof$ta_bcp_iccid` and BigQuery `sof.ta_bcp_iccid` tables are empty.
*   **Action:**
    1.  Execute the legacy Oracle job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion:** Both Oracle `sof$ta_bcp_iccid` and BigQuery `sof.ta_bcp_iccid` must contain 0 rows after execution.

    ```sql
    -- BigQuery Assertion
    SELECT COUNT(*) FROM `sof.ta_bcp_iccid`;
    -- Pass if result is 0.
    ```

#### Test Case 1.4: No Join Matches

*   **Purpose:** To verify correct behavior when the join condition (`bp.cntrct_id_ref = ic.cntrct_id`) yields no matches, even if source tables have data.
*   **Setup:**
    1.  Populate Oracle `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` with data such that `cntrct_id_ref` values in `sof$ta_bpr_bcp` do *not* match any `cntrct_id` values in `sof$ta_iccid_vertrag`.
    2.  Ensure BigQuery source tables contain *identical* data.
    3.  Ensure both target tables are empty.
*   **Action:**
    1.  Execute the legacy Oracle job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion:** Both Oracle `sof$ta_bcp_iccid` and BigQuery `sof.ta_bcp_iccid` must contain 0 rows after execution.

    ```sql
    -- BigQuery Assertion
    SELECT COUNT(*) FROM `sof.ta_bcp_iccid`;
    -- Pass if result is 0.
    ```

#### Test Case 1.5: Duplicate Records in Source (Testing `DISTINCT`)

*   **Purpose:** To verify that the `DISTINCT` clause in the `SELECT` statement correctly removes duplicate rows from the final output, ensuring output parity.
*   **Setup:**
    1.  Populate Oracle `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` such that, after the join, there are multiple rows that are identical across all selected columns (`cntrct_id`, `bpr_id`, `cntrct_id_ref`, `tn_iccid`, `tn_imsi_hlr`).
    2.  Ensure BigQuery source tables contain *identical* data.
    3.  Ensure both target tables are empty.
*   **Action:**
    1.  Execute the legacy Oracle job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The number of rows in Oracle `sof$ta_bcp_iccid` must be equal to the number of rows in BigQuery `sof.ta_bcp_iccid`.
    *   A full data comparison (as in Test Case 1.1) must show no differences.
    *   Crucially, the count of rows in the target table should be less than the count of rows *before* applying `DISTINCT` in the source query.

#### Test Case 1.6: NULLs in Join Key / Selected Columns

*   **Purpose:** To ensure `NULL` values in join keys (`cntrct_id_ref`, `cntrct_id`) or selected columns are handled consistently between Oracle and BigQuery. Oracle's `NULL` handling in joins (no match) and `SELECT` behavior should be replicated.
*   **Setup:**
    1.  Populate Oracle `sof$ta_bpr_bcp` with records where `cntrct_id_ref` is `NULL`.
    2.  Populate Oracle `sof$ta_iccid_vertrag` with records where `cntrct_id` is `NULL`.
    3.  Populate both tables with records where `tn_iccid` or `tn_imsi_hlr` are `NULL`.
    4.  Ensure BigQuery source tables contain *identical* data, including `NULL` values.
    5.  Ensure both target tables are empty.
*   **Action:**
    1.  Execute the legacy Oracle job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion:**
    *   Rows with `NULL` in `cntrct_id_ref` or `cntrct_id` (if it's the join key) should not appear in the output, as `NULL = NULL` is false in SQL.
    *   Rows with `NULL` in `tn_iccid` or `tn_imsi_hlr` should appear in the output if they satisfy the join condition, with `NULL` values preserved.
    *   A full data comparison (as in Test Case 1.1) must show no differences.

---

### 2. Orchestration & External System Replacements Tests

#### Test Case 2.1: Parameter Handling and Date Calculation

*   **Purpose:** To verify the Airflow DAG correctly parses input parameters and accurately calculates `p_datum_heute` and `p_datum_gestern` based on the DAG's execution date, replicating `gestern.ksh` and `h_alis_parameter.ksh` logic.
*   **Setup:**
    1.  Define a specific DAG run date (e.g., `ds_date = '2023-03-15'`).
    2.  Prepare expected values for `p_datum_heute` (20230315) and `p_datum_gestern` (20230314).
*   **Action:**
    1.  Trigger the Airflow DAG (`k_ausd_bp_ta_bcp_iccid_dag.py`) with specific parameters (e.g., `p_JobKennung='TEST'`, `p_EintragsNr='1'`, `p_Stichtag='20230315'`) and a specific logical date (e.g., `2023-03-15`).
    2.  Monitor the Airflow task logs for `validate_and_prepare_parameters`.
*   **Pass/Fail Criterion:**
    *   The Airflow task logs for `validate_and_prepare_parameters` must show the correct parsing of `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`.
    *   The logs must explicitly state `p_datum_heute` as `20230315` and `p_datum_gestern` as `20230314` (or equivalent for the chosen execution date).
    *   The task must complete successfully.

    ```python
    # Example of how you might assert this in a Python unit test for the _validate_and_prepare_parameters function
    from unittest.mock import MagicMock
    from datetime import date, timedelta
    import pendulum

    # Assuming _validate_and_prepare_parameters is imported or accessible
    # from your DAG file.

    def test_validate_and_prepare_parameters_happy_path():
        mock_ti = MagicMock()
        mock_context = {
            "params": {
                "p_JobKennung": "MY_JOB",
                "p_EintragsNr": "123",
                "p_Stichtag": "20230315",
                "p_wiederanlaufWert": "0",
            },
            "ds_date": date(2023, 3, 15),
            "ti": mock_ti,
        }

        _validate_and_prepare_parameters(**mock_context)

        # Assert XCom pushes (if used by downstream tasks)
        mock_ti.xcom_push.assert_any_call(key="p_datum_heute", value="20230315")
        mock_ti.xcom_push.assert_any_call(key="p_datum_gestern", value="20230314")

        # You would also check log output for the info messages
        # This requires mocking the logger, which is more complex for a simple example.
    ```

#### Test Case 2.2: Invalid `p_Stichtag` Format

*   **Purpose:** To ensure the Airflow DAG fails gracefully when `p_Stichtag` is provided in an invalid format, mimicking the `DWDate_Datum_Check` in the legacy script.
*   **Setup:** None specific, just trigger the DAG.
*   **Action:**
    1.  Trigger the Airflow DAG with an invalid `p_Stichtag` (e.g., `p_Stichtag='2023-01-01'` or `p_Stichtag='INVALID_DATE'`).
*   **Pass/Fail Criterion:**
    *   The `validate_and_prepare_parameters` task must fail.
    *   The task logs must contain an error message indicating an invalid date format (e.g., `ValueError: Invalid p_Stichtag format. Expected YYYYMMDD, got ...`).

#### Test Case 2.3: Missing Required Parameters

*   **Purpose:** To verify the Airflow DAG fails if required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`) are not provided, mimicking the `pruefeParameterGesetzt` logic.
*   **Setup:** None specific, just trigger the DAG.
*   **Action:**
    1.  Trigger the Airflow DAG, omitting one or more of `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` from the DAG run configuration.
*   **Pass/Fail Criterion:**
    *   The `validate_and_prepare_parameters` task must fail.
    *   The task logs must contain an error message indicating the missing parameter (e.g., `KeyError` or `ValueError` if the Python logic explicitly checks for existence). The current Python code uses `.get()` which returns `None` if not present, so the validation logic needs to be robust enough to fail if `None` is passed for required parameters. (The current DAG code only validates `p_Stichtag` format, not presence of all required parameters, this is a potential gap in the migration).

    *Self-correction during test writing*: The provided Python DAG code uses `.get()` for parameters and only explicitly validates `p_Stichtag`'s *format*, not the presence of `p_JobKennung` or `p_EintragsNr`. This is a gap compared to the legacy `pruefeParameterGesetzt` calls. A test case for this would highlight this discrepancy.

    **Revised Pass/Fail Criterion for Test Case 2.3:**
    *   The `validate_and_prepare_parameters` task should fail.
    *   The task logs should contain an error message indicating the missing required parameter (e.g., "Required parameter p_JobKennung is missing").
    *   *If the current DAG implementation does not fail for missing `p_JobKennung` or `p_EintragsNr` (due to `.get()` and no explicit check), this test case will FAIL, indicating a defect in the migrated DAG's parameter validation logic.*

#### Test Case 2.4: `v_carmen` Variable Preservation

*   **Purpose:** To confirm that the `v_carmen` variable, though its functional impact is unclear, is correctly declared and its value `@pcrs1` is preserved in the BigQuery SQL, as specified in the migration design.
*   **Setup:** None.
*   **Action:**
    1.  Inspect the `src/sql/bigquery/d_ausd_bp_ta_bcp_iccid.bqsql` file.
    2.  Execute the Airflow DAG and review BigQuery job details or logs if possible to confirm the SQL executed includes the declaration.
*   **Pass/Fail Criterion:**
    *   The `d_ausd_bp_ta_bcp_iccid.bqsql` file must contain the line `DECLARE v_carmen STRING DEFAULT '@pcrs1';`.
    *   The BigQuery job executed by the Airflow DAG must successfully declare this variable without error. (Direct assertion of its value in BigQuery is not straightforward as it's a session variable, but its presence confirms the migration intent).

---

### 3. Data Quality / Row Count / Schema Assertions

#### Test Case 3.1: Row Count Parity (Overall)

*   **Purpose:** To verify that the total number of rows inserted into the target table is identical between the legacy Oracle job and the migrated BigQuery job under various data conditions. This is a high-level check for data completeness.
*   **Setup:**
    1.  Populate Oracle source tables with a representative dataset (e.g., the same as Test Case 1.1).
    2.  Ensure BigQuery source tables contain *identical* data.
    3.  Ensure both target tables are empty.
*   **Action:**
    1.  Execute the legacy Oracle job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The `COUNT(*)` from Oracle `sof$ta_bcp_iccid` must be exactly equal to the `COUNT(*)` from BigQuery `sof.ta_bcp_iccid`.

    ```sql
    -- BigQuery Assertion
    SELECT COUNT(*) FROM `sof.ta_bcp_iccid`;
    -- Compare with Oracle: SELECT COUNT(*) FROM sof$ta_bcp_iccid;
    ```

#### Test Case 3.2: Schema Parity (Target Table)

*   **Purpose:** To confirm that the BigQuery target table `sof.ta_bcp_iccid` has the same columns, data types, and nullability as the Oracle source table `sof$ta_bcp_iccid`.
*   **Setup:**
    1.  Obtain the schema definition for Oracle `sof$ta_bcp_iccid` (e.g., `DESCRIBE sof$ta_bcp_iccid;`).
    2.  Obtain the schema definition for BigQuery `sof.ta_bcp_iccid` (e.g., `bq show --schema --format=prettyjson project_id:sof.ta_bcp_iccid`).
*   **Action:**
    1.  Compare the column names, data types, and nullability constraints between the Oracle and BigQuery schemas.
*   **Pass/Fail Criterion:**
    *   All column names must match.
    *   Data types must be functionally equivalent (e.g., Oracle `VARCHAR2(N)` to BigQuery `STRING`, Oracle `NUMBER` to BigQuery `INT64` or `NUMERIC` depending on precision/scale).
    *   Nullability constraints (e.g., `NOT NULL` in Oracle, `REQUIRED` in BigQuery) must match for each column.

#### Test Case 3.3: Data Type Handling (Specific Columns)

*   **Purpose:** To verify that specific data types, especially those that might involve implicit conversions or precision changes (e.g., numbers, dates/timestamps if they were part of the `SELECT` list), are correctly handled without data loss or corruption.
*   **Setup:**
    1.  Populate Oracle source tables with data that specifically tests data type boundaries or edge cases (e.g., maximum length strings, numbers with many decimal places if applicable, specific date formats).
    2.  Ensure BigQuery source tables contain *identical* data.
    3.  Ensure both target tables are empty.
*   **Action:**
    1.  Execute the legacy Oracle job.
    2.  Execute the migrated Airflow DAG.
    3.  Perform a row-by-row comparison (as in Test Case 1.1), paying close attention to the exact values and their types in the target tables.
*   **Pass/Fail Criterion:**
    *   The data in each column of BigQuery `sof.ta_bcp_iccid` must exactly match the data in the corresponding column of Oracle `sof$ta_bcp_iccid`, confirming no data truncation, rounding, or type conversion errors.

---