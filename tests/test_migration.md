As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_v_ta_c_bfc.ksh` and its associated Oracle SQL to Google BigQuery. These tests aim to ensure behavioral equivalence, data integrity, and correct external system interactions.

---

## Migration Validation Tests: `k_ausd_v_ta_c_bfc.ksh` to BigQuery

**Project ID Placeholder:** Throughout these tests, replace `your-gcp-project` with your actual Google Cloud Project ID and `isbert_schema` with your BigQuery dataset ID.

---

### Test Case 1: End-to-End Output Parity (Full Job Run)

*   **Purpose:** To verify that running the migrated BigQuery job with a given set of input data produces an identical final state in the target table (`sof$ta_c_bfc`) as the legacy Oracle job. This is the ultimate behavioral equivalence test.
*   **Setup:**
    1.  **Baseline Data Generation:** Create a comprehensive dataset for all source Oracle tables (`dwtk_meldungen`, `all_objects`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`). This dataset should cover:
        *   Initial inserts into `sof$ta_c_bfc`.
        *   Updates to `sof$ta_c_bfc` based on `bfc_age` and `bfc_count` changes.
        *   Updates to `sof$ta_c_bfc` based on outdated `bfc_procedure`.
        *   Contracts with `NULL` values in key date/ID fields.
        *   Scenarios for `v_datum` and `v_bfc_procedure` determination (e.g., `dwtk_meldungen` entries, `all_objects` entries, and missing entries).
        *   Data that triggers both `NULL` and non-`NULL` results from the `bfc_get_bindefrist` function.
    2.  **Oracle Environment:**
        *   Load the baseline data into the Oracle source tables.
        *   Ensure `sof$ta_c_bfc` is initially empty.
    3.  **BigQuery Environment:**
        *   Load the *exact same* baseline data into the corresponding BigQuery source tables.
        *   Ensure `your-gcp-project.isbert_schema.sof$ta_c_bfc` is initially empty.
        *   Ensure the `bfc_get_bindefrist` UDF and both stored procedures (`d_ausd_v_ta_c_bfc_sp`, `k_ausd_v_ta_c_bfc_sp`) are deployed.
*   **Action:**
    1.  **Run Legacy Job:** Execute the legacy KornShell script:
        ```bash
        ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh -j "TEST_JOB" -f "12345"
        ```
    2.  **Capture Oracle Output:** After the Oracle job completes, extract the entire contents of `sof$ta_c_bfc` into a canonical format (e.g., CSV, JSON, or a temporary table).
    3.  **Run Migrated Job:** Execute the BigQuery orchestration stored procedure:
        ```sql
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('TEST_JOB', '12345', NULL);
        ```
    4.  **Capture BigQuery Output:** After the BigQuery job completes, extract the entire contents of `your-gcp-project.isbert_schema.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   The `sof$ta_c_bfc` table in BigQuery must be an exact, row-for-row, column-for-column match with the `sof$ta_c_bfc` table in Oracle.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming Oracle data is loaded into a BigQuery staging table, e.g., `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`) = (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                AND NOT EXISTS (
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                    UNION ALL
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`)
                )
                THEN 'PASS: sof$ta_c_bfc tables are identical.'
                ELSE 'FAIL: sof$ta_c_bfc tables differ.'
            END AS result;
        ```

---

### Test Case 2: Parameter Handling and Validation

*   **Purpose:** To verify that the BigQuery orchestration stored procedure (`k_ausd_v_ta_c_bfc_sp`) correctly handles input parameters, including validation and error reporting, mirroring the KSH script's behavior.
*   **Setup:**
    *   Ensure `your-gcp-project.isbert_schema.job_error_log` and `your-gcp-project.isbert_schema.job_run_log` tables are accessible and initially empty for clean testing.
*   **Action:**
    1.  **Missing `p_job_kennung`:**
        ```sql
        -- Expected to fail due to missing p_job_kennung
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`(NULL, '12345', NULL);
        ```
    2.  **Missing `p_eintrags_nr`:**
        ```sql
        -- Expected to fail due to missing p_eintrags_nr
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('TEST_JOB', NULL, NULL);
        ```
    3.  **Valid Parameters:**
        ```sql
        -- Expected to succeed (assuming d_ausd_v_ta_c_bfc_sp also succeeds)
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('VALID_JOB', '67890', NULL);
        ```
*   **Pass/Fail Criterion:**
    *   **Missing Parameters:** The calls with `NULL` parameters must raise an error with a message similar to "FEHLER: Parameter p_job_kennung must be set." or "FEHLER: Parameter p_eintrags_nr must be set."
    *   **Error Logging:** For failed runs, `job_error_log` must contain an entry with `severity = 'ERROR'` and an appropriate message. `job_run_log` must show `status = 'FAILED'`.
    *   **Valid Parameters:** The call with valid parameters must complete successfully, and `job_run_log` must show `status = 'SUCCEEDED'`.
    *   **SQL Assertion (for error logging):**
        ```sql
        -- After running test actions 1 and 2
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_error_log` WHERE error_message LIKE '%p_job_kennung must be set%') = 1
                AND (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_error_log` WHERE error_message LIKE '%p_eintrags_nr must be set%') = 1
                AND (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_run_log` WHERE status = 'FAILED') >= 2 -- At least 2 failed runs
                THEN 'PASS: Parameter validation and error logging work as expected.'
                ELSE 'FAIL: Parameter validation or error logging is incorrect.'
            END AS result;
        ```

---

### Test Case 3: `v_datum` and `v_bfc_procedure` Determination

*   **Purpose:** To verify that the internal variables `v_datum` and `v_bfc_procedure` are correctly calculated within `d_ausd_v_ta_c_bfc_sp`, matching the Oracle logic, including edge cases like missing source data.
*   **Setup:**
    1.  **Oracle:**
        *   Insert `('BERT_DROP_TEMP_TABLE', TO_TIMESTAMP('2023-01-15 10:00:00', 'YYYY-MM-DD HH24:MI:SS'))` into `isbert_schema.dwtk_meldungen`.
        *   Insert `('CDS$VR_BINDEFRIST', 'PACKAGE', TO_TIMESTAMP('2022-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))` into `isbert_schema.all_objects`.
        *   Also, prepare scenarios where these entries are missing or have different dates.
    2.  **BigQuery:**
        *   Mirror the Oracle data into `your-gcp-project.isbert_schema.dwtk_meldungen` and `your-gcp-project.isbert_schema.all_objects`.
        *   Ensure `d_ausd_v_ta_c_bfc_sp` is deployed.
*   **Action:**
    1.  **Oracle Manual Check:** Execute the original Oracle `SELECT` statements to determine `v_datum` and `v_bfc_procedure`.
        ```sql
        -- For v_datum
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        -- For v_bfc_procedure
        SELECT TO_CHAR(created, 'YYYYMMDD') FROM all_objects WHERE object_name = 'CDS$VR_BINDEFRIST' AND object_type = 'PACKAGE';
        ```
    2.  **BigQuery Execution:** Call `d_ausd_v_ta_c_bfc_sp`. (Note: Direct inspection of `DECLARE` variables within a BQSP is not straightforward. We will infer correctness by observing their impact on subsequent steps, or by temporarily modifying the SP to log these values).
    3.  **Test Scenarios:**
        *   Both `dwtk_meldungen` and `all_objects` have matching entries.
        *   `dwtk_meldungen` has no matching entry (expect `v_datum` = `1900-01-01`).
        *   `all_objects` has no matching entry (expect `v_bfc_procedure` = `NULL`).
*   **Pass/Fail Criterion:**
    *   The values used for `v_datum` and `v_bfc_procedure` in the BigQuery SP (as inferred from the output or logged values) must exactly match the values derived from the Oracle queries for each scenario.
    *   For `v_datum`: `DATE '2023-01-15'` (or `DATE '1900-01-01'` if no match).
    *   For `v_bfc_procedure`: `DATE '2022-03-01'` (or `NULL` if no match).

---

### Test Case 4: `sof$ta_c_bfc_akt` Population (Intermediate Table)

*   **Purpose:** To verify the correctness of the `TRUNCATE` and `INSERT ... SELECT` logic that populates the temporary table `sof$ta_c_bfc_akt`, including complex joins, aggregations (`MAX`, `COUNT`), `GREATEST`, and `COALESCE` (NVL).
*   **Setup:**
    1.  **Baseline Data:** Populate `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period` in both Oracle and BigQuery with data covering:
        *   Contracts with all joins successful.
        *   Contracts where some `LEFT JOIN`s do not find matches (testing `COALESCE` with `DATE '1900-01-01'`).
        *   Contracts with `NULL` values in `bfc_age` columns across different tables.
        *   Multiple `sof$ta_cntrct_crs` records for the same `cntrct_id` to test `MAX` and `COUNT` aggregation.
        *   Ensure `sof$ta_c_bfc_akt` is initially empty.
    2.  **BigQuery:** Ensure `d_ausd_v_ta_c_bfc_sp` is deployed.
*   **Action:**
    1.  **Oracle Execution:** Manually execute the `TRUNCATE TABLE sof$ta_c_bfc_akt;` and the `INSERT INTO sof$ta_c_bfc_akt ... SELECT ...` statement from `d_ausd_v_ta_c_bfc.sql`.
    2.  **Capture Oracle Output:** Extract the contents of Oracle's `sof$ta_c_bfc_akt`.
    3.  **BigQuery Execution:** Call `d_ausd_v_ta_c_bfc_sp`. (For precise testing, you might need to comment out subsequent steps in `d_ausd_v_ta_c_bfc_sp` or run it in a transaction that you can rollback after inspection).
    4.  **Capture BigQuery Output:** Extract the contents of `your-gcp-project.isbert_schema.sof$ta_c_bfc_akt`.
*   **Pass/Fail Criterion:**
    *   The `sof$ta_c_bfc_akt` table in BigQuery must be an exact match (row count, column values, data types) to the `sof$ta_c_bfc_akt` table in Oracle.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming Oracle data is loaded into a BigQuery staging table, e.g., `your-gcp-project.isbert_schema.sof_ta_c_bfc_akt_oracle_baseline`
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc_akt`) = (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_akt_oracle_baseline`)
                AND NOT EXISTS (
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc_akt` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_akt_oracle_baseline`)
                    UNION ALL
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_akt_oracle_baseline` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc_akt`)
                )
                THEN 'PASS: sof$ta_c_bfc_akt tables are identical.'
                ELSE 'FAIL: sof$ta_c_bfc_akt tables differ.'
            END AS result;
        ```

---

### Test Case 5: Initial Population of `sof$ta_c_bfc`

*   **Purpose:** To verify that `sof$ta_c_bfc` is correctly populated from `sof$ta_c_bfc_akt` when the target table is initially empty, including the default `bfc_procedure` value.
*   **Setup:**
    1.  **Oracle:**
        *   Ensure `sof$ta_c_bfc` is empty.
        *   Populate `sof$ta_c_bfc_akt` with a set of test records.
    2.  **BigQuery:**
        *   Ensure `your-gcp-project.isbert_schema.sof$ta_c_bfc` is empty.
        *   Mirror the `sof$ta_c_bfc_akt` data into `your-gcp-project.isbert_schema.sof$ta_c_bfc_akt`.
        *   Ensure `d_ausd_v_ta_c_bfc_sp` is deployed.
*   **Action:**
    1.  **Oracle Execution:** Manually execute the Oracle `DECLARE v_rows ... IF v_rows = 0 THEN INSERT... END IF;` block from `d_ausd_v_ta_c_bfc.sql`.
    2.  **Capture Oracle Output:** Extract the contents of Oracle's `sof$ta_c_bfc`.
    3.  **BigQuery Execution:** Call `d_ausd_v_ta_c_bfc_sp`. (Again, isolate this step if possible, or ensure `sof$ta_c_bfc` is empty before the call).
    4.  **Capture BigQuery Output:** Extract the contents of `your-gcp-project.isbert_schema.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   The `sof$ta_c_bfc` table in BigQuery must contain all records from `sof$ta_c_bfc_akt`, with `bfc_procedure` set to `DATE '1900-01-01'`, matching Oracle's output.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming Oracle data is loaded into a BigQuery staging table, e.g., `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`) = (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                AND NOT EXISTS (
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                    UNION ALL
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`)
                )
                THEN 'PASS: Initial population of sof$ta_c_bfc is correct.'
                ELSE 'FAIL: Initial population of sof$ta_c_bfc is incorrect.'
            END AS result;
        ```

---

### Test Case 6: `MERGE` Logic (Updates and Inserts)

*   **Purpose:** To verify the BigQuery `MERGE` statement's behavior, covering `WHEN MATCHED THEN UPDATE` (based on `bfc_age` or `bfc_count` changes) and `WHEN NOT MATCHED THEN INSERT`, including the `bfc_get_bindefrist` UDF call.
*   **Setup:**
    1.  **Oracle:**
        *   Populate `sof$ta_c_bfc` with existing records.
        *   Populate `sof$ta_c_bfc_akt` with data that includes:
            *   New `cntrct_id`s (to trigger `NOT MATCHED` inserts).
            *   Existing `cntrct_id`s where `bfc_age` is greater in `_akt` (to trigger `MATCHED` updates).
            *   Existing `cntrct_id`s where `bfc_count` is different in `_akt` (to trigger `MATCHED` updates).
            *   Existing `cntrct_id`s where `bfc_age` and `bfc_count` are identical (to trigger no update).
            *   Existing `cntrct_id`s where `bfc_age` is *less* in `_akt` (to trigger no update).
            *   Data that will result in `NULL` `bindefrist` from the `bfc_get_bindefrist` UDF.
    2.  **BigQuery:**
        *   Mirror the `sof$ta_c_bfc` and `sof$ta_c_bfc_akt` data into the BigQuery tables.
        *   Ensure `d_ausd_v_ta_c_bfc_sp` and `bfc_get_bindefrist` UDF are deployed.
*   **Action:**
    1.  **Oracle Execution:** Manually execute the Oracle `MERGE` statement from `d_ausd_v_ta_c_bfc.sql`.
    2.  **Capture Oracle Output:** Extract the contents of Oracle's `sof$ta_c_bfc`.
    3.  **BigQuery Execution:** Call `d_ausd_v_ta_c_bfc_sp`. (Isolate the `MERGE` step if possible for focused testing).
    4.  **Capture BigQuery Output:** Extract the contents of `your-gcp-project.isbert_schema.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   The final state of `sof$ta_c_bfc` in BigQuery must be an exact match to Oracle's `sof$ta_c_bfc`, specifically verifying correct updates, inserts, and unchanged rows based on the `MERGE` conditions.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming Oracle data is loaded into a BigQuery staging table, e.g., `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`) = (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                AND NOT EXISTS (
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                    UNION ALL
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`)
                )
                THEN 'PASS: MERGE logic is correct.'
                ELSE 'FAIL: MERGE logic is incorrect.'
            END AS result;
        ```

---

### Test Case 7: Partial Update of `sof$ta_c_bfc` (Outdated `bfc_procedure`)

*   **Purpose:** To verify the `UPDATE` statement that re-calculates `bindefrist` for records where `bfc_procedure` is older than the current `v_bfc_procedure`. This also addresses the `ROWNUM` difference.
*   **Setup:**
    1.  **Oracle:**
        *   Populate `sof$ta_c_bfc` with records where:
            *   `bfc_procedure` is older than the current `v_bfc_procedure` (to be updated).
            *   `bfc_procedure` is current or newer (not to be updated).
        *   Ensure `v_bfc_procedure` is set to a specific date (e.g., `DATE '2023-01-01'`) for the test.
    2.  **BigQuery:**
        *   Mirror the `sof$ta_c_bfc` data into the BigQuery table.
        *   Ensure `d_ausd_v_ta_c_bfc_sp` and `bfc_get_bindefrist` UDF are deployed.
*   **Action:**
    1.  **Oracle Execution:** Manually execute the Oracle `UPDATE` statement from `d_ausd_v_ta_c_bfc.sql`. Note the `ROWNUM <= &v_max_update` clause.
    2.  **Capture Oracle Output:** Extract the contents of Oracle's `sof$ta_c_bfc`.
    3.  **BigQuery Execution:** Call `d_ausd_v_ta_c_bfc_sp`. (Isolate the `UPDATE` step if possible).
    4.  **Capture BigQuery Output:** Extract the contents of `your-gcp-project.isbert_schema.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   **Behavioral Difference Acknowledged:** The design document notes that BigQuery's `UPDATE` will not replicate Oracle's `ROWNUM` behavior. Therefore, BigQuery is expected to update *all* matching rows where `bfc_procedure < v_bfc_procedure`, whereas Oracle might only update `v_max_update` rows.
    *   **If `v_max_update` was a safeguard (not strict batching):** The BigQuery `sof$ta_c_bfc` should have all relevant `bindefrist` and `bfc_procedure` values updated, matching what Oracle *would* have done if `ROWNUM` wasn't limiting.
    *   **If `v_max_update` was for strict batching:** This is a **FAIL** for behavioral equivalence, and the BigQuery SP needs redesign to implement batching (e.g., using `OFFSET`/`LIMIT` with a cursor or iterative calls).
    *   For this test, assuming `v_max_update` was a safeguard, the BigQuery `sof$ta_c_bfc` should match the Oracle `sof$ta_c_bfc` *if Oracle had updated all matching rows*.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming Oracle data (after its update) is loaded into a BigQuery staging table, e.g., `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`
        -- This assertion needs to account for the ROWNUM difference. If Oracle updated fewer rows, this comparison will fail.
        -- A more robust test would compare the *intended* state (all matching rows updated) rather than the ROWNUM-limited Oracle state.
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`) = (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                AND NOT EXISTS (
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`)
                    UNION ALL
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`)
                )
                THEN 'PASS: Partial update logic is correct (considering ROWNUM difference).'
                ELSE 'FAIL: Partial update logic is incorrect.'
            END AS result;
        ```

---

### Test Case 8: `bfc_get_bindefrist` UDF Logic

*   **Purpose:** To verify that the BigQuery `bfc_get_bindefrist` UDF produces identical results to the original Oracle function, especially for `NULL` inputs and the placeholder logic.
*   **Setup:**
    *   Ensure the `your-gcp-project.isbert_schema.bfc_get_bindefrist` UDF is deployed.
    *   **Critical Note:** As per the design document, the internal logic of the Oracle `Cds$vr_Bindefrist.GetBindeFrist` is unknown. This test assumes the placeholder UDF logic (`NULL` for `NULL` input, `DATE '3999-12-31'` otherwise) is the current expected behavior. A full migration requires reverse-engineering the Oracle function.
*   **Action:**
    1.  **Test Cases:**
        *   `i_commitment_reference_date` is `NULL`.
        *   `i_commitment_reference_date` is a valid date (e.g., `DATE '2023-01-01'`).
        *   Various combinations of `i_cntrct_id` and `i_cntrct_validity_id` (though the placeholder UDF doesn't use them).
    2.  **BigQuery Execution:** Call the UDF directly with these inputs.
        ```sql
        SELECT `your-gcp-project.isbert_schema.bfc_get_bindefrist`(1, NULL, 1) AS result_null_date;
        SELECT `your-gcp-project.isbert_schema.bfc_get_bindefrist`(2, DATE '2023-01-01', 2) AS result_valid_date;
        ```
    3.  **Oracle Execution:** If possible, execute the original Oracle `bfc_get_bindefrist` function with the same inputs.
*   **Pass/Fail Criterion:**
    *   The BigQuery UDF output must match the Oracle function's output for all test cases.
    *   Specifically:
        *   `result_null_date` should be `NULL`.
        *   `result_valid_date` should be `DATE '3999-12-31'` (based on the placeholder logic).
    *   **If Oracle logic is more complex:** This test will likely fail, indicating the UDF needs to be updated with the actual Oracle logic.

---

### Test Case 9: External System Replacement (`DB_LINK:PCRS1` for `all_objects`)

*   **Purpose:** To verify that the BigQuery `all_objects` table, which replaces the Oracle `all_objects@PCRS1` accessed via DB_LINK, provides the correct data for `v_bfc_procedure` determination.
*   **Setup:**
    1.  **Oracle:** Ensure `all_objects@PCRS1` contains the expected entry for `CDS$VR_BINDEFRIST` with a specific `created` timestamp.
    2.  **BigQuery:** Ensure `your-gcp-project.isbert_schema.all_objects` is populated with an identical entry for `CDS$VR_BINDEFRIST` and its `created` timestamp.
*   **Action:**
    1.  **Oracle Manual Check:** Execute the Oracle query to get `v_bfc_procedure`.
        ```sql
        SELECT TO_CHAR(created, 'YYYYMMDD') FROM all_objects@PCRS1 WHERE object_name = 'CDS$VR_BINDEFRIST' AND object_type = 'PACKAGE';
        ```
    2.  **BigQuery Execution:** Call `d_ausd_v_ta_c_bfc_sp`. (The `v_bfc_procedure` value is used in the `MERGE` and `UPDATE` steps).
*   **Pass/Fail Criterion:**
    *   The `v_bfc_procedure` value used in the BigQuery SP (as inferred from the output or logged values) must exactly match the value derived from the Oracle `all_objects@PCRS1` query.
    *   If the BigQuery `all_objects` table is empty or missing the entry, `v_bfc_procedure` should be `NULL`, and subsequent steps should handle this gracefully (as per Oracle's behavior).

---

### Test Case 10: Helper Script Replacements (Job Control & Logging)

*   **Purpose:** To verify that the BigQuery stored procedures correctly implement the logging and job control functionalities previously handled by KSH helper scripts (`f_alis_msgerr.ksh`, `h_alis_job.ksh`, etc.).
*   **Setup:**
    *   Ensure `your-gcp-project.isbert_schema.job_error_log` and `your-gcp-project.isbert_schema.job_run_log` tables are accessible.
    *   Ensure `k_ausd_v_ta_c_bfc_sp` and `d_ausd_v_ta_c_bfc_sp` are deployed.
*   **Action:**
    1.  **Successful Run:** Call `k_ausd_v_ta_c_bfc_sp` with valid parameters, ensuring `d_ausd_v_ta_c_bfc_sp` also completes successfully.
        ```sql
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('SUCCESS_JOB', '111', NULL);
        ```
    2.  **Parameter Validation Failure:** Call `k_ausd_v_ta_c_bfc_sp` with a missing parameter (as in Test Case 2).
        ```sql
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`(NULL, '222', NULL);
        ```
    3.  **Internal SQL Error:** Temporarily introduce an error into `d_ausd_v_ta_c_bfc_sp` (e.g., `SELECT 1/0;` or reference a non-existent table) and then call `k_ausd_v_ta_c_bfc_sp`.
        ```sql
        -- After modifying d_ausd_v_ta_c_bfc_sp to fail
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('INTERNAL_ERROR_JOB', '333', NULL);
        ```
*   **Pass/Fail Criterion:**
    *   **Successful Run:**
        *   `job_run_log` must contain an entry for `SUCCESS_JOB` with `status = 'SUCCEEDED'`, `start_time`, `end_time`, and `parameters` correctly populated.
        *   No entry for `SUCCESS_JOB` in `job_error_log`.
    *   **Parameter Validation Failure:**
        *   `job_run_log` must contain an entry for the failed job with `status = 'FAILED'`.
        *   `job_error_log` must contain an entry with `job_id` (if captured before failure), `error_message` indicating parameter validation failure, and `severity = 'ERROR'`.
    *   **Internal SQL Error:**
        *   `job_run_log` must contain an entry for `INTERNAL_ERROR_JOB` with `status = 'FAILED'`.
        *   `job_error_log` must contain an entry for `INTERNAL_ERROR_JOB` with `error_message` detailing the SQL error and `severity = 'ERROR'`.
    *   **SQL Assertion (for logging):**
        ```sql
        SELECT
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_run_log` WHERE job_id = 'SUCCESS_JOB' AND status = 'SUCCEEDED') AS success_runs,
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_run_log` WHERE job_id IS NULL AND status = 'FAILED' AND JSON_EXTRACT_SCALAR(parameters, '$.p_eintrags_nr') = '222') AS param_fail_runs,
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_run_log` WHERE job_id = 'INTERNAL_ERROR_JOB' AND status = 'FAILED') AS internal_fail_runs,
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_error_log` WHERE error_message LIKE '%p_job_kennung must be set%') AS param_error_logs,
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.job_error_log` WHERE job_id = 'INTERNAL_ERROR_JOB' AND error_message LIKE '%division by zero%' OR error_message LIKE '%table not found%') AS internal_error_logs;
        -- Expected: success_runs=1, param_fail_runs=1, internal_fail_runs=1, param_error_logs=1, internal_error_logs=1
        ```

---

### Test Case 11: Data Quality & Schema Assertions

*   **Purpose:** To verify that the BigQuery target table (`sof$ta_c_bfc`) maintains data quality, correct data types, and schema integrity after migration.
*   **Setup:**
    *   Run the full end-to-end job (Test Case 1) to populate `sof$ta_c_bfc` in BigQuery.
    *   Have the Oracle `sof$ta_c_bfc` schema and data available for comparison.
*   **Action:**
    1.  **Schema Comparison:** Compare the DDL of BigQuery `your-gcp-project.isbert_schema.sof$ta_c_bfc` with Oracle `sof$ta_c_bfc`.
    2.  **Data Type Validation:** Query `sof$ta_c_bfc` in BigQuery and inspect data types and values for specific columns (e.g., `bindefrist` as `DATE`, `bfc_count` as `INTEGER`).
    3.  **NULL Handling:** Query for `NULL` values in columns that can be `NULL` (e.g., `bindefrist` if `i_commitment_reference_date` was `NULL`) and compare with Oracle.
    4.  **Row Count:** Get the total row count.
*   **Pass/Fail Criterion:**
    *   **Schema:** BigQuery schema must be functionally equivalent to Oracle, with appropriate BigQuery data types (e.g., `DATE` for Oracle `DATE`, `INT64` for Oracle `NUMBER`).
    *   **Data Types:** All columns in BigQuery `sof$ta_c_bfc` must have the correct BigQuery data types and contain valid data for those types.
    *   **NULL Handling:** The distribution and placement of `NULL` values in BigQuery `sof$ta_c_bfc` must match Oracle `sof$ta_c_bfc`.
    *   **Row Count:** The total row count of `your-gcp-project.isbert_schema.sof$ta_c_bfc` must match Oracle `sof$ta_c_bfc`.
    *   **SQL Assertion (Example for NULLs and Row Count):**
        ```sql
        SELECT
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`) AS bq_row_count,
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc` WHERE bindefrist IS NULL) AS bq_null_bindefrist_count,
            -- Assuming Oracle data is loaded into a BigQuery staging table for comparison
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline`) AS oracle_row_count,
            (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_oracle_baseline` WHERE bindefrist IS NULL) AS oracle_null_bindefrist_count;
        -- Expected: bq_row_count = oracle_row_count AND bq_null_bindefrist_count = oracle_null_bindefrist_count
        ```

---

### Test Case 12: Idempotency

*   **Purpose:** To verify that running the BigQuery job multiple times with the same input data yields the same result in `sof$ta_c_bfc`, ensuring consistency and preventing unintended side effects.
*   **Setup:**
    *   Populate all BigQuery source tables with a fixed, comprehensive dataset.
    *   Ensure `your-gcp-project.isbert_schema.sof$ta_c_bfc` is initially empty.
*   **Action:**
    1.  **First Run:** Execute `k_ausd_v_ta_c_bfc_sp`.
        ```sql
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('IDEMPOTENCY_TEST', '1', NULL);
        ```
    2.  **Capture State 1:** Extract the entire contents of `your-gcp-project.isbert_schema.sof$ta_c_bfc`.
    3.  **Second Run:** Execute `k_ausd_v_ta_c_bfc_sp` again without changing any source data.
        ```sql
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('IDEMPOTENCY_TEST', '1', NULL);
        ```
    4.  **Capture State 2:** Extract the entire contents of `your-gcp-project.isbert_schema.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   The contents of `sof$ta_c_bfc` after the second run (State 2) must be an exact match to the contents after the first run (State 1).
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming State 1 is saved to `your-gcp-project.isbert_schema.sof_ta_c_bfc_state1`
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`) = (SELECT COUNT(*) FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_state1`)
                AND NOT EXISTS (
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_state1`)
                    UNION ALL
                    (SELECT * FROM `your-gcp-project.isbert_schema.sof_ta_c_bfc_state1` EXCEPT DISTINCT SELECT * FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`)
                )
                THEN 'PASS: Job is idempotent.'
                ELSE 'FAIL: Job is not idempotent.'
            END AS result;
        ```