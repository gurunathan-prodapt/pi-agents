As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery stored procedure. The following test cases are designed to ensure behavioral equivalence between the legacy Oracle/KSH job and the new BigQuery solution.

---

## Migration Validation Tests: `k_ausd_v_ta_barrier_zusgf`

**Assumptions for Testing:**
*   **Data Synchronization**: It is assumed that the source tables (`sof$ta_barrier`, `isbert_schema.dwtk_meldungen`) have been migrated to BigQuery (`project.dataset.sof_ta_barrier`, `project.dataset.dwtk_meldungen`) and contain identical data for testing purposes.
*   **Oracle Environment**: A stable Oracle environment with the legacy job and its dependencies is available for baseline comparisons.
*   **BigQuery Environment**: A BigQuery project with the migrated stored procedure and target tables is available.
*   **String Length Limits**: The BigQuery stored procedure, as provided, does *not* include explicit `SUBSTR` functions for `sperrart_alle`, `sperrgrund_alle`, and `stilllegungszeitraum_alle`. This implies that if the Oracle target table had implicit length constraints, the BigQuery output might be longer. This will be explicitly tested.
*   **`v_datum` Usage**: The BigQuery code retrieves `v_datum` but comments out its usage as a filter for `sof_ta_barrier`. This test suite assumes `v_datum` is *not* used as a filter in the legacy Oracle SQL either, or that this is an accepted behavioral change. The retrieval itself will be verified.

---

### Test Case 1: Output Parity - Full Data Comparison

*   **Purpose**: To verify that, given identical input data, the BigQuery stored procedure produces an identical final output table (`sof_ta_barrier_zusgf`) to the legacy Oracle job. This is the most comprehensive test for overall behavioral equivalence.
*   **Setup**:
    1.  Ensure `project.dataset.sof_ta_barrier` and `project.dataset.dwtk_meldungen` in BigQuery contain data identical to their Oracle counterparts (`sof$ta_barrier`, `isbert_schema.dwtk_meldungen`).
    2.  Record the state of the Oracle `sof$ta_barrier_zusgf` table *before* running the legacy job (if it's not truncated and repopulated).
    3.  Ensure both target tables (`sof$ta_barrier_zusgf` in Oracle, `project.dataset.sof_ta_barrier_zusgf` in BigQuery) are empty or in a known state before execution.
*   **Action**:
    1.  Execute the legacy KSH script (`k_ausd_v_ta_barrier_zusgf.ksh`) in the Oracle environment.
    2.  Execute the BigQuery stored procedure (`project.dataset.k_ausd_v_ta_barrier_zusgf`) with the same logical parameters (e.g., `p_JobKennung='TEST_JOB'`, `p_EintragsNr='123'`).
*   **Pass/Fail Criterion**:
    *   The number of rows in `project.dataset.sof_ta_barrier_zusgf` must be identical to the number of rows in Oracle `sof$ta_barrier_zusgf`.
    *   A row-by-row comparison of all columns in `project.dataset.sof_ta_barrier_zusgf` against Oracle `sof$ta_barrier_zusgf` must show no differences.
    *   **SQL Assertion (BigQuery vs. Oracle):**
        ```sql
        -- BigQuery side:
        SELECT * FROM `project.dataset.sof_ta_barrier_zusgf`
        ORDER BY cntrct_id; -- Order for consistent comparison

        -- Oracle side:
        SELECT * FROM sof$ta_barrier_zusgf
        ORDER BY cntrct_id;
        ```
        The results of these two queries, when exported and compared (e.g., using `diff` on CSVs, or a data comparison tool), must be identical.

---

### Test Case 2: Transformation Correctness - `sperrart_alle` Aggregation

*   **Purpose**: To verify the correct aggregation and transformation logic for the `sperrart_alle` column, including `REPLACE` operations and concatenation.
*   **Setup**:
    1.  Populate `project.dataset.sof_ta_barrier` with diverse `sperrart` values for a single `cntrct_id`, including:
        *   "Rufnummern"
        *   Values with leading/trailing/internal spaces
        *   `NULL` values
        *   Multiple distinct values
        *   Values that, after replacement, become empty strings.
    2.  Ensure `project.dataset.dwtk_meldungen` is populated to allow `v_datum` retrieval.
*   **Action**:
    1.  Execute the BigQuery stored procedure.
    2.  Query `project.dataset.sof_ta_barrier_zusgf` for the specific `cntrct_id`.
*   **Pass/Fail Criterion**:
    *   The `sperrart_alle` value in BigQuery must match the expected concatenated string, with "Rufnummern" and all spaces removed from individual `sperrart` components before concatenation, and components ordered alphabetically. `NULL` `sperrart` values should be ignored in the concatenation.
    *   **Example Scenario & Expected Output:**
        *   **Input `sof_ta_barrier` for `cntrct_id = 101`:**
            | cntrct_id | sperrart         |
            | :-------- | :--------------- |
            | 101       | "Aktiv"          |
            | 101       | "Rufnummern"     |
            | 101       | "  Passiv  "     |
            | 101       | NULL             |
            | 101       | "Test Rufnummern"|
        *   **Expected `sperrart_alle` (BigQuery):** "Aktiv,Passiv,Test" (Order: Aktiv, Passiv, Test)
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT cntrct_id, sperrart_alle
        FROM `project.dataset.sof_ta_barrier_zusgf`
        WHERE cntrct_id = 101;
        -- Expected result: (101, 'Aktiv,Passiv,Test')
        ```

---

### Test Case 3: Transformation Correctness - `stilllegungszeitraum_alle` Derivation & Aggregation

*   **Purpose**: To verify the conditional derivation of `stilllegungszeitraum_alle` based on `ist_stillegung`, `sperr_beginn`, `sperr_ende`, and its subsequent aggregation.
*   **Setup**:
    1.  Populate `project.dataset.sof_ta_barrier` with records for a `cntrct_id` covering all `stilllegungszeitraum_alle` derivation cases:
        *   `ist_stillegung = 1`, `sperr_ende IS NULL`
        *   `ist_stillegung = 1`, `sperr_ende IS NOT NULL`
        *   `ist_stillegung = 0` (or any value other than 1)
        *   `sperr_beginn` or `sperr_ende` are `NULL` when `ist_stillegung = 1` (should result in `NULL` for that component).
    2.  Ensure `project.dataset.dwtk_meldungen` is populated.
*   **Action**:
    1.  Execute the BigQuery stored procedure.
    2.  Query `project.dataset.sof_ta_barrier_zusgf` for the specific `cntrct_id`.
*   **Pass/Fail Criterion**:
    *   The `stilllegungszeitraum_alle` value must correctly reflect the concatenated and formatted date strings, with `NULL` components ignored.
    *   **Example Scenario & Expected Output:**
        *   **Input `sof_ta_barrier` for `cntrct_id = 202`:**
            | cntrct_id | ist_stillegung | sperr_beginn | sperr_ende | sperrart (for ordering) |
            | :-------- | :------------- | :----------- | :--------- | :---------------------- |
            | 202       | 1              | '2023-01-01' | NULL       | 'A'                     |
            | 202       | 1              | '2023-02-15' | '2023-03-15'| 'B'                     |
            | 202       | 0              | '2023-04-01' | '2023-04-30'| 'C'                     |
            | 202       | 1              | NULL         | '2023-05-31'| 'D'                     |
        *   **Expected `stilllegungszeitraum_alle` (BigQuery):** "ab 01.01.2023, 15.02.2023 - 15.03.2023" (Note: The `NULL` sperr_beginn for D would make that component NULL, and C is ist_stillegung=0, so both are ignored. Ordering is based on `sperrart` as per the BQ code.)
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT cntrct_id, stilllegungszeitraum_alle
        FROM `project.dataset.sof_ta_barrier_zusgf`
        WHERE cntrct_id = 202;
        -- Expected result: (202, 'ab 01.01.2023, 15.02.2023 - 15.03.2023')
        ```

---

### Test Case 4: Transformation Correctness - `sperrgrund_zusgf` Derivation & Aggregation

*   **Purpose**: To verify the complex aggregation logic for `sperrgrund_zusgf`: if any underlying record for a `cntrct_id` has a `barrier_reason_cv` that results in `sperrgrund_zusgf != 2`, then the final aggregated value is 3; otherwise, it's 2.
*   **Setup**:
    1.  Populate `project.dataset.sof_ta_barrier` with records for two `cntrct_id`s:
        *   `cntrct_id = 303`: All records have `barrier_reason_cv = 2`.
        *   `cntrct_id = 304`: At least one record has `barrier_reason_cv != 2` (e.g., 1, 3, or NULL), while others might be 2.
    2.  Ensure `project.dataset.dwtk_meldungen` is populated.
*   **Action**:
    1.  Execute the BigQuery stored procedure.
    2.  Query `project.dataset.sof_ta_barrier_zusgf` for `cntrct_id = 303` and `cntrct_id = 304`.
*   **Pass/Fail Criterion**:
    *   For `cntrct_id = 303`, `sperrgrund_zusgf` must be 2.
    *   For `cntrct_id = 304`, `sperrgrund_zusgf` must be 3.
    *   **Example Scenario & Expected Output:**
        *   **Input `sof_ta_barrier` for `cntrct_id = 303`:**
            | cntrct_id | barrier_reason_cv |
            | :-------- | :---------------- |
            | 303       | 2                 |
            | 303       | 2                 |
        *   **Input `sof_ta_barrier` for `cntrct_id = 304`:**
            | cntrct_id | barrier_reason_cv |
            | :-------- | :---------------- |
            | 304       | 2                 |
            | 304       | 1                 |
            | 304       | 2                 |
        *   **Expected `sperrgrund_zusgf` (BigQuery):**
            *   `cntrct_id = 303`: 2
            *   `cntrct_id = 304`: 3
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT cntrct_id, sperrgrund_zusgf
        FROM `project.dataset.sof_ta_barrier_zusgf`
        WHERE cntrct_id IN (303, 304)
        ORDER BY cntrct_id;
        -- Expected result: (303, 2), (304, 3)
        ```

---

### Test Case 5: External System Replacement - `v_datum` Retrieval

*   **Purpose**: To verify that the BigQuery stored procedure correctly retrieves the `v_datum` value from `project.dataset.dwtk_meldungen`, mirroring the KSH script's behavior.
*   **Setup**:
    1.  Populate `project.dataset.dwtk_meldungen` with several records for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, ensuring different `timecreated` values.
    2.  Include a record with the maximum `timecreated` for this `job_kennung`.
    3.  Include records for other `job_kennung` values to ensure correct filtering.
*   **Action**:
    1.  Execute the BigQuery stored procedure.
    2.  Query the `project.dataset.execution_log` table for the specific `p_JobKennung` and `p_EintragsNr` used in the execution.
*   **Pass/Fail Criterion**:
    *   The `execution_log` should contain a message indicating the `v_datum` value if it's logged (the provided code logs a warning if `v_datum` is NULL, but not its value if found).
    *   Alternatively, if direct inspection of `v_datum` is not possible via logs, a temporary `RAISE` or `INSERT` into a debug table within the procedure can be used during testing to confirm its value.
    *   The `v_datum` retrieved by the procedure must be equal to the `MAX(DATE(timecreated))` from `project.dataset.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **SQL Assertion (BigQuery - manual check or debug log):**
        ```sql
        -- Expected v_datum:
        SELECT MAX(DATE(timecreated))
        FROM `project.dataset.dwtk_meldungen`
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
        The value obtained from the procedure (via debug or log) must match this query's result.

---

### Test Case 6: External System Replacement - Table Truncation

*   **Purpose**: To verify that the `TRUNCATE TABLE` operation is correctly executed before data insertion, matching the Oracle `DWPA_UTIL_SKRIPT.runstatement` behavior.
*   **Setup**:
    1.  Populate `project.dataset.sof_ta_barrier_zusgf` with some dummy data.
    2.  Ensure `project.dataset.sof_ta_barrier` and `project.dataset.dwtk_meldungen` are populated with valid data that will result in new records being inserted.
*   **Action**:
    1.  Execute the BigQuery stored procedure.
    2.  Immediately after execution, query `project.dataset.sof_ta_barrier_zusgf`.
*   **Pass/Fail Criterion**:
    *   The `project.dataset.sof_ta_barrier_zusgf` table must contain only the records inserted by the current procedure run, and none of the dummy data that existed before execution.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Before procedure execution:
        SELECT COUNT(*) FROM `project.dataset.sof_ta_barrier_zusgf`; -- Should be > 0

        -- After procedure execution:
        SELECT COUNT(*) FROM `project.dataset.sof_ta_barrier_zusgf`; -- Should match the count of records inserted by the procedure
        ```

---

### Test Case 7: Data Quality / Row Count / Schema Assertions

*   **Purpose**: To verify the final row count, schema integrity, and data types of the output table, and to check for potential string truncation issues.
*   **Setup**:
    1.  Execute the legacy KSH job and the BigQuery stored procedure with a representative dataset (as in Test Case 1).
    2.  Ensure `project.dataset.sof_ta_barrier` contains data that would generate long concatenated strings for `sperrart_alle`, `sperrgrund_alle`, and `stilllegungszeitraum_alle` (e.g., many distinct values for a single `cntrct_id`).
*   **Action**:
    1.  Query row counts from both Oracle and BigQuery target tables.
    2.  Inspect the schema of `project.dataset.sof_ta_barrier_zusgf`.
    3.  Query BigQuery `project.dataset.sof_ta_barrier_zusgf` for records with potentially long concatenated strings.
*   **Pass/Fail Criterion**:
    *   **Row Count**: `COUNT(*)` from `project.dataset.sof_ta_barrier_zusgf` must equal `COUNT(*)` from Oracle `sof$ta_barrier_zusgf`.
    *   **Schema**: The column names, data types, and nullability in BigQuery `project.dataset.sof_ta_barrier_zusgf` must align with the design and the Oracle table's effective schema. `cntrct_id` should be `INT64`, `sperrgrund_zusgf` `INT64`, and `sperrart_alle`, `sperrgrund_alle`, `stilllegungszeitraum_alle` should be `STRING`.
    *   **String Lengths**:
        *   If Oracle implicitly or explicitly truncates strings (e.g., `sperrart_alle` to 500 chars), then the BigQuery output for these columns must also be truncated to the same length. If the BigQuery output is longer, this is a **FAIL** as it represents a behavioral difference and potential data integrity issue if the downstream systems expect truncated strings.
        *   If Oracle does *not* truncate (e.g., `VARCHAR2(4000)` or `CLOB`), then BigQuery's longer strings are acceptable.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Row Count
        SELECT COUNT(*) FROM `project.dataset.sof_ta_barrier_zusgf`;

        -- Schema (manual inspection or using INFORMATION_SCHEMA)
        SELECT column_name, data_type, is_nullable
        FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'sof_ta_barrier_zusgf'
        ORDER BY ordinal_position;

        -- String Length Check (example for sperrart_alle)
        SELECT cntrct_id, LENGTH(sperrart_alle) AS actual_length, sperrart_alle
        FROM `project.dataset.sof_ta_barrier_zusgf`
        WHERE LENGTH(sperrart_alle) > 500; -- Check if any exceed Oracle's potential limit
        -- If this query returns rows and Oracle truncates, it's a FAIL.
        ```

---

### Test Case 8: Error Handling and Logging

*   **Purpose**: To verify that the BigQuery stored procedure handles invalid parameters and runtime errors gracefully, logging appropriate messages to `project.dataset.execution_log`.
*   **Setup**:
    1.  Ensure `project.dataset.execution_log` table exists.
    2.  Ensure `project.dataset.sof_ta_barrier` and `project.dataset.dwtk_meldungen` are in a state that might cause an error (e.g., a data type mismatch if possible, or an empty `dwtk_meldungen` to trigger the `v_datum` warning).
*   **Action**:
    1.  **Invalid Parameters**: Execute the stored procedure with `p_JobKennung = NULL` or `p_EintragsNr = ''`.
    2.  **Runtime Error**: (Simulate if possible, e.g., by temporarily altering a table schema to cause a type error, or by making `dwtk_meldungen` empty). Execute the stored procedure.
    3.  **Successful Run**: Execute the stored procedure with valid parameters and data.
*   **Pass/Fail Criterion**:
    *   **Invalid Parameters**: The procedure must `RAISE` an error, and `project.dataset.execution_log` must contain an entry with `status = 'FAILED'` and a descriptive `message` for the parameter validation error.
    *   **Runtime Error**: The procedure must `RAISE` an error, and `project.dataset.execution_log` must contain an entry with `status = 'FAILED'` and a descriptive `message` from `@@error.message`.
    *   **Successful Run**: `project.dataset.execution_log` must contain entries for 'RUNNING' and 'SUCCESS', including the `record_count` for the successful run.
    *   **`v_datum` Warning**: If `dwtk_meldungen` is empty for `BERT_DROP_TEMP_TABLE`, `execution_log` must contain a 'WARNING' entry.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- After running with invalid parameters:
        SELECT status, message
        FROM `project.dataset.execution_log`
        WHERE job_kennung IS NULL OR eintrags_nr = ''
        ORDER BY execution_timestamp DESC
        LIMIT 1;
        -- Expected: status = 'FAILED', message contains 'cannot be NULL or empty'

        -- After running with a simulated runtime error:
        SELECT status, message
        FROM `project.dataset.execution_log`
        WHERE job_kennung = 'ERROR_TEST' -- Use a specific job_kennung for this test
        ORDER BY execution_timestamp DESC
        LIMIT 1;
        -- Expected: status = 'FAILED', message contains 'Procedure failed:' and BigQuery error details

        -- After a successful run:
        SELECT status, record_count, message
        FROM `project.dataset.execution_log`
        WHERE job_kennung = 'SUCCESS_TEST' -- Use a specific job_kennung for this test
        ORDER BY execution_timestamp DESC
        LIMIT 1;
        -- Expected: status = 'SUCCESS', record_count > 0, message = 'Procedure completed successfully.'
        ```

---