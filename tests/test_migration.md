The migration of `k_ausd_v_ta_c_bfc.ksh` to Google Cloud BigQuery involves re-implementing shell orchestration logic and Oracle SQL data processing into BigQuery Stored Procedures and UDFs. The following tests aim to ensure behavioral equivalence and correctness across the migration.

---

## Migration Validation Tests for `k_ausd_v_ta_c_bfc.ksh`

### General Setup for All Tests

Before running any tests, ensure the following:
1.  The BigQuery dataset `isbert_dataset` exists.
2.  All DDLs for tables (`job_status_log`, `ta_c_bfc`, `ta_c_bfc_akt`, `dwtk_meldungen`, `ta_cntrct_crs`, `ta_barrier`, `ta_cntrct_valid`, `ta_period`) are deployed in `isbert_dataset`.
3.  All BigQuery Stored Procedures (`r_ausd_ta_c_bfc`, `d_ausd_v_ta_c_bfc`) and UDFs (`bfc_get_bindefrist`) are deployed.
4.  For tests requiring specific data states, tables should be truncated and populated as described in the individual test setups.
5.  A BigQuery client (e.g., `google.cloud.bigquery.Client` in Python) is configured for interaction.

---

### 1. Output Parity - End-to-End Data Transformation

*   **Purpose**: Verify that the BigQuery orchestration procedure (`r_ausd_ta_c_bfc`) produces the same final data state in the target table (`ta_c_bfc`) and logs the same record count as the legacy KSH script when given identical logical inputs.
*   **Setup**:
    1.  **Golden Data Generation**: Execute the legacy `k_ausd_v_ta_c_bfc.ksh` script with a carefully chosen, representative set of input data in its Oracle source tables. Capture the following "golden" outputs:
        *   The complete final state of the Oracle `ta_c_bfc` table.
        *   The `v_records` value reported by the KSH script (captured from `$DW_DIR_UTL/bert_k_ausd_v_ta_c_bfc_$$.tmp`).
    2.  **BigQuery Source Data Population**: Populate the BigQuery equivalent source tables (`isbert_dataset.ta_cntrct_crs`, `ta_barrier`, `ta_cntrct_valid`, `ta_period`, `dwtk_meldungen`) with data that is logically identical to the Oracle source data used for golden output generation.
    3.  Ensure `isbert_dataset.ta_c_bfc` and `isbert_dataset.ta_c_bfc_akt` are empty.
    4.  Ensure `isbert_dataset.job_status_log` is empty.
    5.  Define `p_JobKennung` and `p_EintragsNr` values (e.g., `'GOLDEN_JOB'`, `'GOLDEN_ENTRY'`).
*   **Action**:
    1.  Execute the BigQuery orchestration procedure:
        ```sql
        CALL `isbert_dataset.r_ausd_ta_c_bfc`('GOLDEN_JOB', 'GOLDEN_ENTRY');
        ```
*   **Pass/Fail Criterion**:
    1.  The `isbert_dataset.ta_c_bfc` table must contain exactly the same rows and values (including data types and NULLs) as the legacy Oracle `ta_c_bfc` table (golden output).
    2.  The `isbert_dataset.job_status_log` table must contain an entry for `job_kennung = 'GOLDEN_JOB'` and `eintrags_nr = 'GOLDEN_ENTRY'` with `status = 'COMPLETED'` and `record_count` matching the `v_records` obtained from the legacy system.
    3.  No errors should be raised during the BigQuery procedure execution.

    ```python
    import pandas as pd
    import pytest
    from google.cloud import bigquery

    def test_output_parity_end_to_end(bq_client: bigquery.Client, golden_data_ta_c_bfc: pd.DataFrame, golden_record_count: int):
        # Assume bq_client is a configured BigQuery client instance
        # Assume golden_data_ta_c_bfc is a pandas DataFrame representing the golden output of ta_c_bfc
        # Assume golden_record_count is the integer value of v_records from the legacy run

        # Setup: Populate BigQuery source tables with data matching the legacy run
        # (This part would involve loading data from files or direct inserts, omitted for brevity)
        # Example: bq_client.query("INSERT INTO `isbert_dataset.ta_cntrct_crs` ...").result()
        # ...

        # Action: Call the main orchestration procedure
        bq_client.query("CALL `isbert_dataset.r_ausd_ta_c_bfc`('GOLDEN_JOB', 'GOLDEN_ENTRY')").result()

        # Assert 1: Data in ta_c_bfc
        query_result_ta_c_bfc = bq_client.query("SELECT * FROM `isbert_dataset.ta_c_bfc` ORDER BY cntrct_id").to_dataframe()
        pd.testing.assert_frame_equal(query_result_ta_c_bfc, golden_data_ta_c_bfc, check_dtype=False)

        # Assert 2: Record count and status in job_status_log
        job_log_query = """
            SELECT record_count, status
            FROM `isbert_dataset.job_status_log`
            WHERE job_kennung = 'GOLDEN_JOB' AND eintrags_nr = 'GOLDEN_ENTRY'
        """
        job_log_result = bq_client.query(job_log_query).to_dataframe()
        assert not job_log_result.empty, "Job log entry not found."
        assert job_log_result['status'].iloc[0] == 'COMPLETED', f"Expected status 'COMPLETED', got {job_log_result['status'].iloc[0]}"
        assert job_log_result['record_count'].iloc[0] == golden_record_count, \
            f"Expected record_count {golden_record_count}, got {job_log_result['record_count'].iloc[0]}"
    ```

---

### 2. Transformation Correctness - Parameter Validation

*   **Purpose**: Verify that the BigQuery orchestration procedure (`r_ausd_ta_c_bfc`) correctly validates input parameters (`p_jobkennung`, `p_eintragsnr`), mirroring the KSH script's `pruefeParameterGesetzt` logic.
*   **Setup**:
    1.  Ensure `isbert_dataset.job_status_log` is empty.
*   **Action**:
    1.  Attempt to call `r_ausd_ta_c_bfc` with `p_jobkennung` as `NULL`.
    2.  Attempt to call `r_ausd_ta_c_bfc` with `p_eintragsnr` as `NULL`.
    3.  Attempt to call `r_ausd_ta_c_bfc` with `p_jobkennung` as an empty string (`''`).
    4.  Attempt to call `r_ausd_ta_c_bfc` with `p_eintragsnr` as an empty string (`''`).
*   **Pass/Fail Criterion**:
    1.  Each call must raise an `SQLSTATE '45000'` error.
    2.  The error message must specifically contain "ERROR: Parameter p_jobkennung is missing or empty." or "ERROR: Parameter p_eintragsnr is missing or empty." respectively.
    3.  No entries should be created in `isbert_dataset.job_status_log` for these failed attempts.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_parameter_validation(bq_client: bigquery.Client):
        # Clear job log before tests
        bq_client.query("TRUNCATE TABLE `isbert_dataset.job_status_log`").result()

        test_cases = [
            (None, 'TEST_ENTRY_NR', "ERROR: Parameter p_jobkennung is missing or empty."),
            ('TEST_JOB_KENNUNG', None, "ERROR: Parameter p_eintragsnr is missing or empty."),
            ('', 'TEST_ENTRY_NR', "ERROR: Parameter p_jobkennung is missing or empty."),
            ('TEST_JOB_KENNUNG', '', "ERROR: Parameter p_eintragsnr is missing or empty."),
        ]

        for job_kennung, eintrags_nr, expected_error_msg in test_cases:
            with pytest.raises(Exception) as excinfo:
                bq_client.query(f"CALL `isbert_dataset.r_ausd_ta_c_bfc`({repr(job_kennung)}, {repr(eintrags_nr)})").result()
            assert expected_error_msg in str(excinfo.value)

        # Verify no job_status_log entries for failed calls
        job_log_count_query = "SELECT COUNT(1) FROM `isbert_dataset.job_status_log`"
        job_log_count = bq_client.query(job_log_count_query).to_dataframe().iloc[0, 0]
        assert job_log_count == 0, f"Expected 0 job log entries, but found {job_log_count}."
    ```

---

### 3. Transformation Correctness - Job Status Management (Deactivation)

*   **Purpose**: Verify that the BigQuery orchestration procedure correctly deactivates previously 'RUNNING' jobs for the same `p_jobkennung` before starting a new one, as per the legacy script's comment "alte aktive Jobs werden einfach dekativiert".
*   **Setup**:
    1.  Clear `isbert_dataset.job_status_log`.
    2.  Insert a 'RUNNING' entry into `job_status_log` for a specific `job_kennung` and `eintrags_nr` (the "old" job).
        ```sql
        INSERT INTO `isbert_dataset.job_status_log` (job_kennung, eintrags_nr, start_timestamp, status, message)
        VALUES ('TEST_JOB_KENNUNG_DEACTIVATE', 'OLD_ENTRY_NR', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'RUNNING', 'Old job running');
        ```
    3.  Insert another 'RUNNING' entry for a *different* `job_kennung` to ensure it's not affected by the deactivation logic.
        ```sql
        INSERT INTO `isbert_dataset.job_status_log` (job_kennung, eintrags_nr, start_timestamp, status, message)
        VALUES ('OTHER_JOB_KENNUNG', 'OTHER_ENTRY_NR', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'RUNNING', 'Another job running');
        ```
*   **Action**:
    1.  Execute the BigQuery orchestration procedure for `TEST_JOB_KENNUNG_DEACTIVATE` with a *new* `eintrags_nr`.
        ```sql
        CALL `isbert_dataset.r_ausd_ta_c_bfc`('TEST_JOB_KENNUNG_DEACTIVATE', 'NEW_ENTRY_NR');
        ```
*   **Pass/Fail Criterion**:
    1.  The `job_status_log` entry for `('TEST_JOB_KENNUNG_DEACTIVATE', 'OLD_ENTRY_NR')` must have `status = 'DEACTIVATED'` and `end_timestamp` populated.
    2.  A new entry for `('TEST_JOB_KENNUNG_DEACTIVATE', 'NEW_ENTRY_NR')` must exist with `status = 'COMPLETED'`.
    3.  The `job_status_log` entry for `('OTHER_JOB_KENNUNG', 'OTHER_ENTRY_NR')` must remain `status = 'RUNNING'`.

    ```sql
    -- SQL assertions after running the action
    SELECT status, message FROM `isbert_dataset.job_status_log` WHERE job_kennung = 'TEST_JOB_KENNUNG_DEACTIVATE' AND eintrags_nr = 'OLD_ENTRY_NR';
    -- Expected result: status = 'DEACTIVATED', message = 'Deactivated by new job run.'

    SELECT status FROM `isbert_dataset.job_status_log` WHERE job_kennung = 'TEST_JOB_KENNUNG_DEACTIVATE' AND eintrags_nr = 'NEW_ENTRY_NR';
    -- Expected result: status = 'COMPLETED'

    SELECT status FROM `isbert_dataset.job_status_log` WHERE job_kennung = 'OTHER_JOB_KENNUNG' AND eintrags_nr = 'OTHER_ENTRY_NR';
    -- Expected result: status = 'RUNNING'
    ```

---

### 4. Transformation Correctness - `d_ausd_v_ta_c_bfc` Core Logic (INSERT into `ta_c_bfc_akt`)

*   **Purpose**: Verify the correctness of the `INSERT INTO isbert_dataset.ta_c_bfc_akt` statement within `d_ausd_v_ta_c_bfc`, including joins, aggregations (`MAX`, `COUNT`), and `GREATEST`/`COALESCE` logic for `bfc_age`.
*   **Setup**:
    1.  Clear `isbert_dataset.ta_c_bfc_akt`.
    2.  Populate `isbert_dataset.ta_cntrct_crs`, `ta_barrier`, `ta_cntrct_valid`, `ta_period` with specific test data designed to exercise various join conditions, NULLs, and `GREATEST` scenarios for `bfc_age` calculation.
        *   Example data:
            *   `ta_cntrct_crs`: `('C1', DATE '2023-01-01', 'V1')`, `('C2', DATE '2023-02-01', 'V2')`
            *   `ta_barrier`: `('C1', DATE '2023-03-01')`
            *   `ta_cntrct_valid`: `('V1', DATE '2023-02-15', 'P1', NULL, NULL, NULL)`
            *   `ta_period`: `('P1', DATE '2023-02-20')`
*   **Action**:
    1.  Execute the `d_ausd_v_ta_c_bfc` procedure. (For focused unit testing, one might extract and run the `INSERT` statement directly, but for migration validation, testing via the procedure is preferred).
        ```sql
        -- Note: This requires mocking or ensuring the other parts of d_ausd_v_ta_c_bfc don't interfere.
        -- For a true unit test, you'd run just the INSERT statement.
        -- For integration, the full procedure is called.
        CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_AKT', 'TEST_JOB_AKT', @records_processed);
        ```
*   **Pass/Fail Criterion**:
    1.  The data in `isbert_dataset.ta_c_bfc_akt` must exactly match the expected output based on the test data and the transformation logic.

    ```sql
    -- SQL assertion example (assuming the example data from setup)
    WITH expected_ta_c_bfc_akt AS (
        SELECT 'C1' AS cntrct_id, DATE '2023-01-01' AS commitment_reference_date, 'V1' AS cntrct_validity_id, DATE '2023-03-01' AS bfc_age, 1 AS bfc_count UNION ALL
        SELECT 'C2' AS cntrct_id, DATE '2023-02-01' AS commitment_reference_date, 'V2' AS cntrct_validity_id, DATE '1900-01-01' AS bfc_age, 1 AS bfc_count -- Assuming no matching bfc_age for C2
    )
    SELECT
        (SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc_akt`) = (SELECT COUNT(1) FROM expected_ta_c_bfc_akt) AND
        (SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc_akt` ACTUAL JOIN expected_ta_c_bfc_akt EXPECTED
            ON ACTUAL.cntrct_id = EXPECTED.cntrct_id
            AND ACTUAL.commitment_reference_date = EXPECTED.commitment_reference_date
            AND ACTUAL.cntrct_validity_id = EXPECTED.cntrct_validity_id
            AND ACTUAL.bfc_age = EXPECTED.bfc_age
            AND ACTUAL.bfc_count = EXPECTED.bfc_count
        ) = (SELECT COUNT(1) FROM expected_ta_c_bfc_akt)
    AS all_match;
    -- Expected result: all_match = TRUE
    ```

---

### 5. Transformation Correctness - `bfc_get_bindefrist` UDF

*   **Purpose**: Verify the behavior of the `bfc_get_bindefrist` UDF, especially its placeholder logic for `NULL` `i_commitment_reference_date` and the dummy date return for valid inputs.
*   **Setup**: None specific, as it's a UDF.
*   **Action**:
    1.  Call the UDF with `NULL` for `i_commitment_reference_date`.
    2.  Call the UDF with a valid date for `i_commitment_reference_date`.
*   **Pass/Fail Criterion**:
    1.  `isbert_dataset.bfc_get_bindefrist('C1', NULL, 'V1')` must return `NULL`.
    2.  `isbert_dataset.bfc_get_bindefrist('C1', DATE '2023-01-01', 'V1')` must return `DATE '2023-01-01'` (based on the current placeholder logic).
    3.  **Important**: If the UDF is later updated with actual business logic from `Cds$vr_Bindefrist.GetBindeFrist`, this test must be updated to reflect that specific logic.

    ```sql
    -- SQL assertions
    SELECT `isbert_dataset.bfc_get_bindefrist`('C1', NULL, 'V1') IS NULL AS test_null_input;
    -- Expected result: test_null_input = TRUE

    SELECT `isbert_dataset.bfc_get_bindefrist`('C1', DATE '2023-01-01', 'V1') = DATE '2023-01-01' AS test_valid_input;
    -- Expected result: test_valid_input = TRUE (based on current placeholder logic)
    ```

---

### 6. Transformation Correctness - Initial `ta_c_bfc` Population

*   **Purpose**: Verify that `isbert_dataset.ta_c_bfc` is correctly populated from `isbert_dataset.ta_c_bfc_akt` only when `ta_c_bfc` is initially empty.
*   **Setup**:
    1.  Clear `isbert_dataset.ta_c_bfc`.
    2.  Populate `isbert_dataset.ta_c_bfc_akt` with some test data.
        ```sql
        TRUNCATE TABLE `isbert_dataset.ta_c_bfc_akt`;
        INSERT INTO `isbert_dataset.ta_c_bfc_akt` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age, bfc_count)
        VALUES ('C_INIT_1', DATE '2023-01-01', 'V_INIT_1', DATE '2023-01-01', 1);
        ```
*   **Action**:
    1.  Call `isbert_dataset.d_ausd_v_ta_c_bfc`.
        ```sql
        CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_INIT', 'TEST_JOB_INIT', @records_processed);
        ```
*   **Pass/Fail Criterion**:
    1.  `isbert_dataset.ta_c_bfc` must contain the same data as `isbert_dataset.ta_c_bfc_akt` (with `bfc_procedure` set to `DATE '1900-01-01'`).
    2.  If `isbert_dataset.ta_c_bfc` is *not* empty before the call, no new rows should be inserted into `ta_c_bfc` by this specific `IF` block.

    ```sql
    -- SQL assertion after running the action
    SELECT
        (SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc`) = 1 AND
        (SELECT cntrct_id FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_INIT_1') = 'C_INIT_1' AND
        (SELECT bfc_procedure FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_INIT_1') = DATE '1900-01-01'
    AS initial_population_correct;
    -- Expected result: initial_population_correct = TRUE

    -- Additional check: If ta_c_bfc is NOT empty, this block should not insert.
    -- Setup: Populate ta_c_bfc with one row, then ta_c_bfc_akt with another.
    TRUNCATE TABLE `isbert_dataset.ta_c_bfc`;
    INSERT INTO `isbert_dataset.ta_c_bfc` (cntrct_id, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id)
    VALUES ('EXISTING_C', DATE '2020-01-01', 5, DATE '2020-01-01', DATE '2019-12-01', 'V_EXIST');
    TRUNCATE TABLE `isbert_dataset.ta_c_bfc_akt`;
    INSERT INTO `isbert_dataset.ta_c_bfc_akt` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age, bfc_count)
    VALUES ('C_INIT_2', DATE '2023-01-01', 'V_INIT_2', DATE '2023-01-01', 1);
    CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_INIT2', 'TEST_JOB_INIT2', @records_processed);
    SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_INIT_2';
    -- Expected result: 0 (the IF block should be skipped, MERGE will handle it later)
    ```

---

### 7. Transformation Correctness - MERGE Statement Logic

*   **Purpose**: Verify the `MERGE` statement's `WHEN MATCHED` (update conditions `D.bfc_age < S.bfc_age OR D.bfc_count <> S.bfc_count`) and `WHEN NOT MATCHED` (insert) clauses within `d_ausd_v_ta_c_bfc`.
*   **Setup**:
    1.  Clear `isbert_dataset.ta_c_bfc` and `isbert_dataset.ta_c_bfc_akt`.
    2.  Populate `isbert_dataset.ta_c_bfc` with initial data.
    3.  Populate `isbert_dataset.ta_c_bfc_akt` with data that includes:
        *   New `cntrct_id`s (for `WHEN NOT MATCHED`).
        *   Existing `cntrct_id`s where `bfc_age` is greater in `ta_c_bfc_akt` (for `WHEN MATCHED` update).
        *   Existing `cntrct_id`s where `bfc_count` is different in `ta_c_bfc_akt` (for `WHEN MATCHED` update).
        *   Existing `cntrct_id`s where neither update condition is met (should not update).
        ```sql
        -- Data in ta_c_bfc (target)
        INSERT INTO `isbert_dataset.ta_c_bfc` (cntrct_id, bindefrist, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id)
        VALUES
            ('C_MERGE_1', DATE '2023-01-01', DATE '2023-01-01', 10, DATE '2023-01-01', DATE '2022-12-01', 'V1'), -- Will be updated (bfc_age >)
            ('C_MERGE_2', DATE '2023-01-01', DATE '2023-01-01', 20, DATE '2023-01-01', DATE '2022-12-01', 'V2'), -- Will be updated (bfc_count <>)
            ('C_MERGE_3', DATE '2023-01-01', DATE '2023-02-01', 30, DATE '2023-01-01', DATE '2022-12-01', 'V3'); -- No update (conditions not met)

        -- Data in ta_c_bfc_akt (source for merge)
        INSERT INTO `isbert_dataset.ta_c_bfc_akt` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age, bfc_count)
        VALUES
            ('C_MERGE_1', DATE '2022-12-01', 'V1', DATE '2023-03-01', 10), -- bfc_age >
            ('C_MERGE_2', DATE '2022-12-01', 'V2', DATE '2023-01-01', 25), -- bfc_count <>
            ('C_MERGE_3', DATE '2022-12-01', 'V3', DATE '2023-02-01', 30), -- No change
            ('C_MERGE_4', DATE '2022-12-01', 'V4', DATE '2023-04-01', 40); -- New record
        ```
*   **Action**:
    1.  Call `isbert_dataset.d_ausd_v_ta_c_bfc`.
        ```sql
        CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_MERGE', 'TEST_JOB_MERGE', @records_processed);
        ```
*   **Pass/Fail Criterion**:
    1.  Row `C_MERGE_1` must be updated: `bfc_age` to `DATE '2023-03-01'`, `bindefrist` re-calculated, `bfc_procedure` to `CURRENT_DATE()`.
    2.  Row `C_MERGE_2` must be updated: `bfc_count` to `25`, `bindefrist` re-calculated, `bfc_procedure` to `CURRENT_DATE()`.
    3.  Row `C_MERGE_3` must remain unchanged.
    4.  Row `C_MERGE_4` must be inserted with `bindefrist` calculated and `bfc_procedure` to `CURRENT_DATE()`.

    ```sql
    -- SQL assertions after running the action
    SELECT
        (SELECT bfc_age FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_MERGE_1') = DATE '2023-03-01' AS C1_updated_age,
        (SELECT bfc_count FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_MERGE_2') = 25 AS C2_updated_count,
        (SELECT bfc_age FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_MERGE_3') = DATE '2023-02-01' AS C3_not_updated_age,
        (SELECT bfc_count FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_MERGE_3') = 30 AS C3_not_updated_count,
        (SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_MERGE_4') = 1 AS C4_inserted,
        (SELECT bfc_procedure FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_MERGE_1') = CURRENT_DATE() AS C1_bfc_procedure_updated,
        (SELECT bfc_procedure FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_MERGE_4') = CURRENT_DATE() AS C4_bfc_procedure_set
    ;
    -- Expected result: All boolean columns should be TRUE.
    ```

---

### 8. Transformation Correctness - Batch Update (`QUALIFY ROW_NUMBER()`)

*   **Purpose**: Verify the `UPDATE` statement that uses `QUALIFY ROW_NUMBER() OVER(ORDER BY cntrct_id) <= v_max_update` to update `bindefrist` and `bfc_procedure` for a limited number of rows.
*   **Setup**:
    1.  Clear `isbert_dataset.ta_c_bfc`.
    2.  Populate `isbert_dataset.ta_c_bfc` with more rows than `v_max_update` (e.g., 5 rows, assuming `v_max_update` is effectively 2 for this test, which might require temporarily modifying the procedure for testing purposes). All rows should have `bfc_procedure` older than `CURRENT_DATE()`.
        ```sql
        INSERT INTO `isbert_dataset.ta_c_bfc` (cntrct_id, bfc_procedure, commitment_reference_date, cntrct_validity_id)
        VALUES
            ('C_BATCH_1', DATE '2022-01-01', DATE '2023-01-01', 'V1'),
            ('C_BATCH_2', DATE '2022-01-01', DATE '2023-01-01', 'V2'),
            ('C_BATCH_3', DATE '2022-01-01', DATE '2023-01-01', 'V3'),
            ('C_BATCH_4', DATE '2022-01-01', DATE '2023-01-01', 'V4'),
            ('C_BATCH_5', DATE '2022-01-01', DATE '2023-01-01', 'V5');
        ```
*   **Action**:
    1.  Call `isbert_dataset.d_ausd_v_ta_c_bfc`.
        ```sql
        CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_BATCH', 'TEST_JOB_BATCH', @records_processed);
        ```
*   **Pass/Fail Criterion**:
    1.  Exactly `v_max_update` rows in `isbert_dataset.ta_c_bfc` (based on `ORDER BY cntrct_id`) must have their `bfc_procedure` updated to `CURRENT_DATE()`.
    2.  The `bindefrist` for these updated rows must be re-calculated by `bfc_get_bindefrist`.
    3.  The remaining rows (those not selected by `QUALIFY`) must retain their old `bfc_procedure` date.

    ```sql
    -- SQL assertions after running the action (assuming v_max_update effectively 2 for this test)
    SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc` WHERE bfc_procedure = CURRENT_DATE();
    -- Expected result: 2

    SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc` WHERE bfc_procedure = DATE '2022-01-01';
    -- Expected result: 3

    -- Verify bindefrist for updated rows (e.g., C_BATCH_1, C_BATCH_2 if ordered by cntrct_id)
    SELECT `isbert_dataset.bfc_get_bindefrist`('C_BATCH_1', DATE '2023-01-01', 'V1') = (SELECT bindefrist FROM `isbert_dataset.ta_c_bfc` WHERE cntrct_id = 'C_BATCH_1') AS bindefrist_C1_correct;
    -- Expected result: TRUE
    ```

---

### 9. External-system replacements - `dwtk_meldungen` date derivation

*   **Purpose**: Verify that the `v_datum` variable in `d_ausd_v_ta_c_bfc` correctly derives the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from `isbert_dataset.dwtk_meldungen`, mirroring the Oracle `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` logic.
*   **Setup**:
    1.  Clear `isbert_dataset.dwtk_meldungen`.
    2.  Populate `isbert_dataset.dwtk_meldungen` with various `timecreated` values and `job_kennung`s, including some for `BERT_DROP_TEMP_TABLE` and some for other job kennungs.
        ```sql
        INSERT INTO `isbert_dataset.dwtk_meldungen` (timecreated, job_kennung)
        VALUES
            (TIMESTAMP '2023-01-01 10:00:00', 'OTHER_JOB'),
            (TIMESTAMP '2023-01-02 11:00:00', 'BERT_DROP_TEMP_TABLE'),
            (TIMESTAMP '2023-01-03 12:00:00', 'OTHER_JOB'),
            (TIMESTAMP '2023-01-04 13:00:00', 'BERT_DROP_TEMP_TABLE');
        ```
    3.  Also prepare a scenario where no matching `job_kennung` exists.
*   **Action**:
    1.  Call `isbert_dataset.d_ausd_v_ta_c_bfc`.
        ```sql
        CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_DATUM', 'TEST_JOB_DATUM', @records_processed);
        ```
*   **Pass/Fail Criterion**:
    1.  **Note**: The `v_datum` variable is declared but not used in the provided BigQuery SQL for `d_ausd_v_ta_c_bfc`. This is a potential discrepancy in the migration. The test should verify the *derivation* is correct, even if the variable is unused.
    2.  The internal derivation of `v_datum` should result in `DATE '2023-01-04'` for the first setup.
    3.  For the scenario with no matching `BERT_DROP_TEMP_TABLE` entries, the internal derivation of `v_datum` should default to `DATE '1900-01-01'`.

    ```sql
    -- SQL assertion (requires temporary modification of d_ausd_v_ta_c_bfc to return v_datum)
    -- If d_ausd_v_ta_c_bfc was modified to return v_datum as an OUT parameter:
    -- CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_DATUM', 'TEST_JOB_DATUM', @records_processed, @v_derived_datum);
    -- SELECT @v_derived_datum = DATE '2023-01-04' AS datum_correct;
    -- Expected result: datum_correct = TRUE

    -- Test case for default value (requires similar modification or inspection)
    TRUNCATE TABLE `isbert_dataset.dwtk_meldungen`;
    INSERT INTO `isbert_dataset.dwtk_meldungen` (timecreated, job_kennung)
    VALUES (TIMESTAMP '2023-01-01 10:00:00', 'OTHER_JOB');
    -- CALL `isbert_dataset.d_ausd_v_ta_c_bfc`('TEST_ENTRY_DATUM_DEFAULT', 'TEST_JOB_DATUM_DEFAULT', @records_processed, @v_derived_datum_default);
    -- SELECT @v_derived_datum_default = DATE '1900-01-01' AS datum_default_correct;
    -- Expected result: datum_default_correct = TRUE
    ```

---

### 10. Data-quality / Row-count / Schema Assertions - Schema Validation

*   **Purpose**: Verify that the BigQuery table schemas (column names, data types, nullability) match the expected design and inferred types from the migration document.
*   **Setup**: All DDLs deployed.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA` for the schemas of all relevant tables.
*   **Pass/Fail Criterion**:
    1.  All tables (`ta_c_bfc`, `ta_c_bfc_akt`, `job_status_log`, `dwtk_meldungen`, `ta_cntrct_crs`, `ta_barrier`, `ta_cntrct_valid`, `ta_period`) must exist in `isbert_dataset`.
    2.  Their column names, data types, and nullability properties must precisely match the DDLs provided in the migration design document.

    ```sql
    -- SQL assertion example for ta_c_bfc
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `isbert_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'ta_c_bfc'
    ORDER BY
        ordinal_position;

    -- Expected output for ta_c_bfc:
    -- column_name             data_type   is_nullable
    -- cntrct_id               STRING      NO
    -- bindefrist              DATE        YES
    -- bfc_age                 DATE        YES
    -- bfc_count               INT64       YES
    -- bfc_procedure           DATE        YES
    -- commitment_reference_date DATE        YES
    -- cntrct_validity_id      STRING      YES

    -- Repeat similar queries for all other tables and compare against expected schemas.
    ```

---

### 11. Data-quality / Row-count / Schema Assertions - Final Row Count

*   **Purpose**: Verify that the `records_processed` output parameter of `d_ausd_v_ta_c_bfc` and the `record_count` in `job_status_log` accurately reflect the final row count of `ta_c_bfc`.
*   **Setup**:
    1.  Populate source tables with a known set of data that will result in a predictable number of rows in `isbert_dataset.ta_c_bfc` after processing.
    2.  Ensure `isbert_dataset.ta_c_bfc` and `isbert_dataset.job_status_log` are empty.
*   **Action**:
    1.  Execute the BigQuery orchestration procedure:
        ```sql
        CALL `isbert_dataset.r_ausd_ta_c_bfc`('ROW_COUNT_JOB', 'ROW_COUNT_ENTRY');
        ```
*   **Pass/Fail Criterion**:
    1.  The `record_count` in `isbert_dataset.job_status_log` for `ROW_COUNT_JOB`/`ROW_COUNT_ENTRY` must equal the `COUNT(1)` from `isbert_dataset.ta_c_bfc`.
    2.  This count should also match the expected count derived from the legacy system's `v_records` for the same input data (as established in Test Case 1).

    ```sql
    -- SQL assertion after running the action
    SELECT
        (SELECT record_count FROM `isbert_dataset.job_status_log` WHERE job_kennung = 'ROW_COUNT_JOB' AND eintrags_nr = 'ROW_COUNT_ENTRY') =
        (SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc`)
    AS row_count_match;
    -- Expected result: row_count_match = TRUE
    ```

---

### 12. Error Handling - Core SQL Procedure Failure

*   **Purpose**: Verify that if the core data processing procedure (`d_ausd_v_ta_c_bfc`) fails, the orchestration procedure (`r_ausd_ta_c_bfc`) correctly catches the error, logs it, and marks the job as 'FAILED'.
*   **Setup**:
    1.  **Introduce a deliberate error**: Temporarily modify `isbert_dataset.d_ausd_v_ta_c_bfc` to cause a predictable failure (e.g., attempt to insert a `STRING` into an `INT64` column, or reference a non-existent table). This modification should be reverted after the test.
    2.  Ensure `isbert_dataset.job_status_log` is empty.
*   **Action**:
    1.  Execute the BigQuery orchestration procedure:
        ```sql
        CALL `isbert_dataset.r_ausd_ta_c_bfc`('FAIL_JOB', 'FAIL_ENTRY');
        ```
*   **Pass/Fail Criterion**:
    1.  The call to `r_ausd_ta_c_bfc` must raise an error.
    2.  An entry must exist in `isbert_dataset.job_status_log` for `FAIL_JOB`/`FAIL_ENTRY` with `status = 'FAILED'` and `message` containing the specific error details from the failed `d_ausd_v_ta_c_bfc` execution.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_core_procedure_failure(bq_client: bigquery.Client):
        # Setup: Temporarily deploy a faulty version of d_ausd_v_ta_c_bfc
        # Example: Modify d_ausd_v_ta_c_bfc to cause a type mismatch error
        # (This step is highly dependent on your CI/CD and testing environment)
        # For instance, you might deploy a version that tries to insert 'abc' into bfc_count (INT64)

        bq_client.query("TRUNCATE TABLE `isbert_dataset.job_status_log`").result()

        with pytest.raises(Exception) as excinfo:
            bq_client.query("CALL `isbert_dataset.r_ausd_ta_c_bfc`('FAIL_JOB', 'FAIL_ENTRY')").result()

        # Assert 1: Error message contains expected details (e.g., from the deliberate error)
        assert "Failed to convert STRING value" in str(excinfo.value) or "invalid table" in str(excinfo.value) # Adjust based on injected error

        # Assert 2: Job status log reflects failure
        job_log_query = """
            SELECT status, message
            FROM `isbert_dataset.job_status_log`
            WHERE job_kennung = 'FAIL_JOB' AND eintrags_nr = 'FAIL_ENTRY'
        """
        job_log_result = bq_client.query(job_log_query).to_dataframe()
        assert not job_log_result.empty, "Job log entry not found for failed job."
        assert job_log_result['status'].iloc[0] == 'FAILED', f"Expected status 'FAILED', got {job_log_result['status'].iloc[0]}"
        assert "Failed to convert STRING value" in job_log_result['message'].iloc[0] or "invalid table" in job_log_result['message'].iloc[0]
    ```