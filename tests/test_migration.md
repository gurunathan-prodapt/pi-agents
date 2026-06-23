The migration of `k_ausd_bp_ta_bpr_instance.ksh` to BigQuery involves significant changes in technology and architecture. The following test cases are designed to ensure behavioral equivalence, transformation correctness, and proper integration with the new BigQuery environment.

---

## 1. End-to-End Output Parity (Golden Record Comparison)

*   **Purpose:** Verify that the migrated BigQuery job produces an identical `sof$ta_bpr_instance` table as the legacy Oracle job when given the same logical input data. This is the primary behavioral equivalence test.
*   **Setup:**
    1.  **Data Synchronization:** Ensure that the BigQuery source tables (`isbert_schema.dwtk_meldungen`, `dw_source_isrpt_isbert.cds_ta_cntrct`, `dw_source_isrpt_isbert.pds_ta_bpri_com`) contain an exact snapshot of the corresponding Oracle source tables at a specific point in time. This is crucial for a fair comparison.
    2.  **Legacy Environment:** Have the legacy Oracle database and KornShell environment ready to execute `k_ausd_bp_ta_bpr_instance.ksh`.
    3.  **BigQuery Environment:** Have the BigQuery tables and Stored Procedures deployed.
    4.  **Clean State:** Ensure both target tables (`sof$ta_bpr_instance` in Oracle and BigQuery) are empty or cleared for the specific `processing_date` before execution.
*   **Action:**
    1.  **Legacy Run:** Execute the legacy KornShell script with a specific set of parameters, e.g., `k_ausd_bp_ta_bpr_instance.ksh -j "TEST_JOB_FULL" -f "123" -s "01012023" -l "0"`.
    2.  **Capture Legacy Output:** After the legacy job completes, extract all data from the Oracle `sof$ta_bpr_instance` table into a canonical format (e.g., CSV, JSON, or load into a temporary BigQuery table, `legacy_project.legacy_dataset.legacy_sof_ta_bpr_instance`).
    3.  **Migrated Run:** Execute the BigQuery orchestrator Stored Procedure with the *exact same logical parameters*:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_JOB_FULL', '123', '01012023', '0');
        ```
    4.  **Capture Migrated Output:** After the BigQuery job completes, extract all data from the BigQuery `dw_source_isrpt_isbert.sof_ta_bpr_instance` table for the `processing_date = '2023-01-01'`.
*   **Pass/Fail Criterion:**
    *   The number of rows in the legacy `sof$ta_bpr_instance` table must be identical to the number of rows in the BigQuery `dw_source_isrpt_isbert.sof_ta_bpr_instance` table for the specified `processing_date`.
    *   A row-by-row comparison (after sorting by a unique key or all columns) of the data from both target tables must show no differences.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming legacy_sof_ta_bpr_instance is a temporary BigQuery table loaded with legacy data
        -- And dw_source_isrpt_isbert.sof_ta_bpr_instance is the migrated target table

        -- 1. Row Count Check
        SELECT
          (SELECT COUNT(*) FROM `legacy_project.legacy_dataset.legacy_sof_ta_bpr_instance`) AS legacy_count,
          (SELECT COUNT(*) FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance` WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023')) AS migrated_count;
        -- Expected: legacy_count = migrated_count

        -- 2. Data Parity Check (using EXCEPT DISTINCT)
        SELECT 'Mismatch found in legacy_only' AS mismatch_type, * FROM (
          SELECT * FROM `legacy_project.legacy_dataset.legacy_sof_ta_bpr_instance`
          EXCEPT DISTINCT
          SELECT * FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance` WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023')
        )
        UNION ALL
        SELECT 'Mismatch found in migrated_only' AS mismatch_type, * FROM (
          SELECT * FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance` WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023')
          EXCEPT DISTINCT
          SELECT * FROM `legacy_project.legacy_dataset.legacy_sof_ta_bpr_instance`
        );
        -- Expected: No rows returned by this query.
        ```

---

## 2. Transformation Correctness - ICCID Concatenation

*   **Purpose:** Verify that the `ICCID` string concatenation logic, including `LPAD` and `CAST` functions, correctly replicates the Oracle behavior, especially concerning padding and NULL handling.
*   **Setup:**
    1.  Populate `dw_source_isrpt_isbert.pds_ta_bpri_com` with various `ICCID` component values, including NULLs, single-digit numbers, and numbers requiring padding to different lengths (2, 6, 1, 9).
    2.  Populate `dw_source_isrpt_isbert.cds_ta_cntrct` with minimal data to allow joins, ensuring the general filters (`cntrct_st`, `redundant_owner_id`, `is_production`) are met.
    3.  Ensure `isbert_schema.dwtk_meldungen` has an entry for `p_JobKennung = 'TEST_ICCID'` to allow `v_datum` to be derived (e.g., `timecreated = '2023-01-01 00:00:00'`).
*   **Action:**
    1.  Call the BigQuery orchestrator SP:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_ICCID', '1', '01012023', '0');
        ```
    2.  Query the `ICCID` column from the target table `dw_source_isrpt_isbert.sof_ta_bpr_instance` for `processing_date = '2023-01-01'`.
*   **Pass/Fail Criterion:**
    *   The generated `ICCID` values in `dw_source_isrpt_isbert.sof_ta_bpr_instance` must match the expected values based on the `LPAD` and `CONCAT` logic.
    *   **Example Data & Expected Output:**
        | `iccid_mi` | `iccid_ii` | `iccid_iai` | `iccid_nr` | `iccid_cd` | Expected `ICCID` (BigQuery) |
        |------------|------------|-------------|------------|------------|-----------------------------|
        | 89         | 490123     | 4           | 123456789  | 0          | 89-049012-4-123456789-0     |
        | 1          | 1          | 1           | 1          | 1          | 01-000001-1-000000001-1     |
        | NULL       | 123        | NULL        | 456        | NULL       | --000123--000000456-        |
        | 10         | NULL       | 5           | 789        | 2          | 10---5-000000789-2          |
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT
          bp.iccid_mi,
          bp.iccid_ii,
          bp.iccid_iai,
          bp.iccid_nr,
          bp.iccid_cd,
          t.ICCID AS actual_iccid,
          CONCAT(
            LPAD(CAST(bp.iccid_mi AS STRING), 2, '0'), '-',
            LPAD(CAST(bp.iccid_ii AS STRING), 6, '0'), '-',
            LPAD(CAST(bp.iccid_iai AS STRING), 1, '0'), '-',
            LPAD(CAST(bp.iccid_nr AS STRING), 9, '0'), '-',
            CAST(bp.iccid_cd AS STRING)
          ) AS expected_iccid
        FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance` t
        JOIN `dw_source_isrpt_isbert.pds_ta_bpri_com` bp ON t.BPR_INSTANCE_ID = bp.bpri_com_id
        WHERE t.processing_date = PARSE_DATE('%d%m%Y', '01012023')
        AND t.ICCID != CONCAT(
            LPAD(CAST(bp.iccid_mi AS STRING), 2, '0'), '-',
            LPAD(CAST(bp.iccid_ii AS STRING), 6, '0'), '-',
            LPAD(CAST(bp.iccid_iai AS STRING), 1, '0'), '-',
            LPAD(CAST(bp.iccid_nr AS STRING), 9, '0'), '-',
            CAST(bp.iccid_cd AS STRING)
          );
        -- Expected: No rows returned.
        ```

---

## 3. Transformation Correctness - Date Filtering and NULL Handling

*   **Purpose:** Verify that the date-based filtering conditions (`insert_at`, `modified_at`, `valid_from`, `valid_to`) and their NULL handling (`IS NULL OR ... > v_datum`) are correctly translated from Oracle to BigQuery.
*   **Setup:**
    1.  Populate `dw_source_isrpt_isbert.cds_ta_cntrct` and `dw_source_isrpt_isbert.pds_ta_bpri_com` with a diverse set of date values for `insert_at`, `modified_at`, `valid_from`, `valid_to`, including:
        *   Rows where all dates are before `v_datum`.
        *   Rows where `modified_at` or `valid_to` are NULL.
        *   Rows where `modified_at` or `valid_to` are after `v_datum`.
        *   Rows where `insert_at` or `valid_from` are after `v_datum`.
    2.  Ensure `isbert_schema.dwtk_meldungen` has an entry for `p_JobKennung = 'TEST_DATE_FILTER'` such that `v_datum` is a specific date (e.g., '20230101').
*   **Action:**
    1.  Call the BigQuery orchestrator SP:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_DATE_FILTER', '2', '01012023', '0');
        ```
    2.  Query the `dw_source_isrpt_isbert.sof_ta_bpr_instance` table for `processing_date = '2023-01-01'` and compare its contents against expected results based on the filter logic.
*   **Pass/Fail Criterion:**
    *   Only rows satisfying *all* date conditions (including NULL handling) should be present in the target table.
    *   **SQL Assertion (BigQuery - conceptual, would need to compare against a pre-calculated expected set):**
        ```sql
        -- Example: Check a specific row that *should* be included
        SELECT COUNT(*)
        FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023')
          AND CNTRCT_ID = <expected_contract_id_1>
          AND BPR_ID = <expected_bpr_id_1>;
        -- Expected: 1

        -- Example: Check a specific row that *should NOT* be included (e.g., valid_from > v_datum)
        SELECT COUNT(*)
        FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023')
          AND CNTRCT_ID = <excluded_contract_id_1>
          AND BPR_ID = <excluded_bpr_id_1>;
        -- Expected: 0

        -- Example: Check a specific row that *should NOT* be included (e.g., modified_at is not NULL and <= v_datum)
        SELECT COUNT(*)
        FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023')
          AND CNTRCT_ID = <excluded_contract_id_2>
          AND BPR_ID = <excluded_bpr_id_2>;
        -- Expected: 0
        ```
    *   A more robust test would involve running the original Oracle SQL with the same data and comparing the result sets.

---

## 4. Orchestrator - Parameter Validation and Error Handling

*   **Purpose:** Verify that the `r_ausd_bp_ta_bpr_instance` Stored Procedure correctly validates input parameters and raises errors as specified in the design.
*   **Setup:** BigQuery environment with the orchestrator SP deployed.
*   **Action & Pass/Fail Criterion:**
    1.  **Missing `p_JobKennung`:**
        *   Action: `CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`(NULL, '123', '01012023', '0');`
        *   Expected: Procedure fails with `MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen'` and an error message containing `FEHLER: 0 E 1 Jobkennung`.
    2.  **Missing `p_Stichtag`:**
        *   Action: `CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_JOB', '123', NULL, '0');`
        *   Expected: Procedure fails with `MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen'` and an error message containing `FEHLER: 0 E 1 Stichtag`.
    3.  **Missing `p_EintragsNr`:**
        *   Action: `CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_JOB', NULL, '01012023', '0');`
        *   Expected: Procedure fails with `MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen'` and an error message containing `FEHLER: 0 E 1 EintragsNr`.
    4.  **Invalid `p_Stichtag` format:**
        *   Action: `CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_JOB', '123', '2023-01-01', '0');` (or '01/01/2023', 'ABC')
        *   Expected: Procedure fails with `MESSAGE_TEXT = 'Datum hat ungueltiges Format DDMMYYYY'`.
    *   **Pytest Example (conceptual, using a BigQuery client library):**
        ```python
        import pytest
        from google.cloud import bigquery

        client = bigquery.Client()
        project_id = "your-gcp-project"
        dataset_id = "dw_source_isrpt_isbert"
        sp_name = "r_ausd_bp_ta_bpr_instance"

        def call_sp(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
            # Construct the SQL query, handling NULLs correctly for BigQuery
            params = []
            params.append(f"'{job_kennung}'" if job_kennung is not None else "NULL")
            params.append(f"'{eintrags_nr}'" if eintrags_nr is not None else "NULL")
            params.append(f"'{stichtag}'" if stichtag is not None else "NULL")
            params.append(f"'{wiederanlauf_wert}'" if wiederanlauf_wert is not None else "NULL")

            query = f"CALL `{project_id}.{dataset_id}.{sp_name}`({', '.join(params)});"
            
            # Execute the query and return results (or raise exception)
            return client.query(query).result()

        def test_missing_jobkennung_parameter():
            with pytest.raises(Exception) as excinfo:
                call_sp(None, '123', '01012023', '0')
            assert "Bitte ueber Rahmenscript aufrufen" in str(excinfo.value)
            # Further check for specific error message in logs if possible

        def test_invalid_stichtag_format():
            with pytest.raises(Exception) as excinfo:
                call_sp('TEST_JOB', '123', '2023-01-01', '0')
            assert "Datum hat ungueltiges Format DDMMYYYY" in str(excinfo.value)

        def test_missing_eintragsnr_parameter():
            with pytest.raises(Exception) as excinfo:
                call_sp('TEST_JOB', None, '01012023', '0')
            assert "Bitte ueber Rahmenscript aufrufen" in str(excinfo.value)
        ```

---

## 5. Orchestrator - Logging and Record Count

*   **Purpose:** Verify that the `r_ausd_bp_ta_bpr_instance` Stored Procedure correctly logs job execution details, including the final record count, into the `control_log.job_log` table.
*   **Setup:**
    1.  BigQuery environment with all tables and SPs deployed.
    2.  Populate source tables (`cds_ta_cntrct`, `pds_ta_bpri_com`, `dwtk_meldungen`) such that the transformation logic will produce a known number of records (e.g., 50 records) for `processing_date = '2023-01-01'`.
    3.  Ensure `control_log.job_log` is empty or cleared for `tab_name = 'PoolBasisprodukt'` and `stichtag = '2023-01-01'` before the test.
*   **Action:**
    1.  Call the BigQuery orchestrator SP with valid parameters:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_LOGGING', '3', '01012023', '0');
        ```
*   **Pass/Fail Criterion:**
    *   Exactly one new entry should be present in `control_log.job_log` for `tab_name = 'PoolBasisprodukt'` and `stichtag = '2023-01-01'`.
    *   The `job_status` should be 'A'.
    *   The `record_count` in the log entry must match the actual number of records inserted into `dw_source_isrpt_isbert.sof_ta_bpr_instance` for `processing_date = '2023-01-01'`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Check log entry
        SELECT
          tab_name,
          job_status,
          record_count,
          stichtag
        FROM `control_log.job_log`
        WHERE tab_name = 'PoolBasisprodukt'
          AND stichtag = PARSE_DATE('%d%m%Y', '01012023')
        ORDER BY created_at DESC
        LIMIT 1;
        -- Expected: tab_name='PoolBasisprodukt', job_status='A', record_count=<expected_count_from_source_data>, stichtag='2023-01-01'

        -- Verify record count in target table
        SELECT COUNT(*)
        FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023');
        -- Expected: <expected_count_from_source_data> (must match record_count from log)
        ```

---

## 6. External System Replacement - `v_datum` Derivation from `dwtk_meldungen`

*   **Purpose:** Verify that the BigQuery `v_datum` derivation logic (`COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101') WHERE job_kennung = p_JobKennung`) correctly replaces the Oracle `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'` logic, specifically noting the change in `job_kennung` filtering (from hardcoded to parameterized).
*   **Setup:**
    1.  Populate `isbert_schema.dwtk_meldungen` with various `timecreated` values. Crucially, include entries for *different* `job_kennung` values, e.g., 'BERT_DROP_TEMP_TABLE' and 'MY_TEST_JOB_KENNUNG'.
        *   Example Data in `dwtk_meldungen`:
            *   `job_kennung = 'BERT_DROP_TEMP_TABLE'`, `timecreated = '2022-12-31 10:00:00'`
            *   `job_kennung = 'MY_TEST_JOB_KENNUNG'`, `timecreated = '2023-01-15 12:00:00'`
            *   `job_kennung = 'MY_TEST_JOB_KENNUNG'`, `timecreated = '2023-01-10 08:00:00'`
            *   No entries for a third `job_kennung` (e.g., 'NON_EXISTENT_JOB').
    2.  Populate `cds_ta_cntrct` and `pds_ta_bpri_com` with data that would be filtered differently based on `v_datum` values of '20230115', '20221231', and '19000101'.
*   **Action:**
    1.  **Run 1 (using 'MY_TEST_JOB_KENNUNG'):** Call the BigQuery orchestrator SP:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('MY_TEST_JOB_KENNUNG', '4', '01012023', '0');
        ```
        *   Expected `v_datum` in `d_ausd_bp_ta_bpr_instance` should be '20230115'.
    2.  **Run 2 (using 'BERT_DROP_TEMP_TABLE'):** Call the BigQuery orchestrator SP:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('BERT_DROP_TEMP_TABLE', '5', '01012023', '0');
        ```
        *   Expected `v_datum` in `d_ausd_bp_ta_bpr_instance` should be '20221231'.
    3.  **Run 3 (using 'NON_EXISTENT_JOB'):** Call the BigQuery orchestrator SP:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('NON_EXISTENT_JOB', '6', '01012023', '0');
        ```
        *   Expected `v_datum` in `d_ausd_bp_ta_bpr_instance` should be '19000101'.
*   **Pass/Fail Criterion:**
    *   For each run, the records inserted into `sof_ta_bpr_instance` (for `processing_date = '2023-01-01'`) must reflect the filtering based on the *correctly derived* `v_datum` for the `p_JobKennung` used in that run.
    *   This requires comparing the final `sof_ta_bpr_instance` content for each run against a pre-calculated expected result set based on the `v_datum` that *should* have been derived.
    *   **SQL Assertion (BigQuery - conceptual):**
        ```sql
        -- After Run 1 (MY_TEST_JOB_KENNUNG, v_datum = '20230115'):
        SELECT COUNT(*) FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023');
        -- Expected: <count_for_v_datum_20230115>

        -- After Run 2 (BERT_DROP_TEMP_TABLE, v_datum = '20221231'):
        SELECT COUNT(*) FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023');
        -- Expected: <count_for_v_datum_20221231>

        -- After Run 3 (NON_EXISTENT_JOB, v_datum = '19000101'):
        SELECT COUNT(*) FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023');
        -- Expected: <count_for_v_datum_19000101>
        ```

---

## 7. Schema and Data Type Assertions

*   **Purpose:** Verify that the schema of the target table `dw_source_isrpt_isbert.sof_ta_bpr_instance` matches the expected design and that data types are correctly handled during insertion.
*   **Setup:** BigQuery environment with the target table deployed.
*   **Action:**
    1.  Execute the BigQuery orchestrator SP with valid parameters to populate the table (e.g., `CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_SCHEMA', '7', '01012023', '0');`).
    2.  Query the schema information for `dw_source_isrpt_isbert.sof_ta_bpr_instance`.
*   **Pass/Fail Criterion:**
    *   The table `dw_source_isrpt_isbert.sof_ta_bpr_instance` must exist.
    *   All columns must have the expected names and data types as defined in the DDL.
    *   `CNTRCT_ID`, `BPR_ID`, `BPR_INSTANCE_ID` should be `INT64 NOT NULL`.
    *   `ICCID`, `IMSI_MCC`, `IMSI_MNC`, `IMSI_HLR`, `IMSI_SI` should be `STRING`.
    *   `CNTRCT_ID_REF` should be `INT64`.
    *   `processing_date` should be `DATE NOT NULL`.
    *   The table should be partitioned by `processing_date`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT
          column_name,
          data_type,
          is_nullable
        FROM `dw_source_isrpt_isbert.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'sof_ta_bpr_instance'
        ORDER BY ordinal_position;
        /* Expected Output (example):
        column_name     data_type   is_nullable
        CNTRCT_ID       INT64       NO
        BPR_ID          INT64       NO
        BPR_INSTANCE_ID INT64       NO
        ICCID           STRING      YES
        IMSI_MCC        STRING      YES
        IMSI_MNC        STRING      YES
        IMSI_HLR        STRING      YES
        IMSI_SI         STRING      YES
        CNTRCT_ID_REF   INT64       YES
        processing_date DATE        NO
        */

        -- Check partitioning
        SELECT
          option_value
        FROM `dw_source_isrpt_isbert.INFORMATION_SCHEMA.TABLE_OPTIONS`
        WHERE table_name = 'sof_ta_bpr_instance'
          AND option_name = 'partitioning_expression';
        -- Expected: option_value = 'processing_date'
        ```

---

## 8. Empty Source Tables (Edge Case)

*   **Purpose:** Verify the job's behavior when source tables (`cds_ta_cntrct`, `pds_ta_bpri_com`) are empty. It should run without error and result in an empty target table.
*   **Setup:**
    1.  Ensure `dw_source_isrpt_isbert.cds_ta_cntrct` and `dw_source_isrpt_isbert.pds_ta_bpri_com` are empty.
    2.  Ensure `isbert_schema.dwtk_meldungen` has an entry for `p_JobKennung = 'TEST_EMPTY_SOURCES'` to allow `v_datum` to be derived (or test with `v_datum` defaulting to '19000101').
    3.  Ensure `dw_source_isrpt_isbert.sof_ta_bpr_instance` is empty for `processing_date = '2023-01-01'`.
*   **Action:**
    1.  Call the BigQuery orchestrator SP:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_EMPTY_SOURCES', '8', '01012023', '0');
        ```
*   **Pass/Fail Criterion:**
    *   The procedure must complete successfully without errors.
    *   The `dw_source_isrpt_isbert.sof_ta_bpr_instance` table must remain empty for the given `processing_date`.
    *   The `control_log.job_log` entry for this run should show `record_count = 0`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*)
        FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023');
        -- Expected: 0

        SELECT record_count
        FROM `control_log.job_log`
        WHERE tab_name = 'PoolBasisprodukt'
          AND stichtag = PARSE_DATE('%d%m%Y', '01012023')
        ORDER BY created_at DESC
        LIMIT 1;
        -- Expected: 0
        ```

---

## 9. `TRUNCATE` Behavior

*   **Purpose:** Verify that the `TRUNCATE TABLE` statement in `d_ausd_bp_ta_bpr_instance` correctly clears the target table for the specific `processing_date` before insertion.
*   **Setup:**
    1.  Populate `dw_source_isrpt_isbert.sof_ta_bpr_instance` with some existing data (e.g., 10 records) for `processing_date = '2023-01-01'`.
    2.  Populate source tables (`cds_ta_cntrct`, `pds_ta_bpri_com`, `dwtk_meldungen`) such that the job will insert a *different* set of records (e.g., 5 records) for the same `processing_date`.
*   **Action:**
    1.  Call the BigQuery orchestrator SP:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_TRUNCATE', '9', '01012023', '0');
        ```
*   **Pass/Fail Criterion:**
    *   The `dw_source_isrpt_isbert.sof_ta_bpr_instance` table for `processing_date = '2023-01-01'` should only contain the 5 records inserted by the current run, and none of the pre-existing 10 records.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*)
        FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
        WHERE processing_date = PARSE_DATE('%d%m%Y', '01012023');
        -- Expected: 5 (the number of records inserted by the current run, not the initial 10)
        ```

---

## 10. `p_wiederanlaufWert` Defaulting

*   **Purpose:** Verify that `p_wiederanlaufWert` defaults to '0' if not provided or empty, and that this does not cause the orchestrator to fail.
*   **Setup:** BigQuery environment with the orchestrator SP deployed.
*   **Action:**
    1.  Call the SP without `p_wiederanlaufWert`:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_W_NULL', '10', '01012023', NULL);
        ```
    2.  Call the SP with empty `p_wiederanlaufWert`:
        ```sql
        CALL `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`('TEST_W_EMPTY', '11', '01012023', '');
        ```
*   **Pass/Fail Criterion:**
    *   Both calls must execute successfully without raising any errors.
    *   The `job_log` entries for these runs should reflect successful completion (status 'A'). (Since `p_wiederanlaufWert` is not logged, we infer its correct handling by the absence of errors and successful execution of the subsequent steps).
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Check for successful completion in job_log
        SELECT COUNT(*)
        FROM `control_log.job_log`
        WHERE tab_name = 'PoolBasisprodukt'
          AND stichtag = PARSE_DATE('%d%m%Y', '01012023')
          AND job_status = 'A'
          AND (
            -- Assuming a way to identify these specific runs, e.g., by a unique EintragsNr
            -- or by checking the latest entries if runs are sequential.
            -- For this example, we'll assume the EintragsNr is unique for the test.
            EXISTS (SELECT 1 FROM `control_log.job_log` WHERE stichtag = PARSE_DATE('%d%m%Y', '01012023') AND record_count >= 0 AND created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
          );
        -- Expected: 2 (indicating two successful log entries for the recent runs)
        ```