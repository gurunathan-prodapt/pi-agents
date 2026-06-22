The migration of `k_ausd_v_ta_discount.ksh` and its associated Oracle SQL to Google Cloud Platform (GCP) BigQuery and Cloud Composer requires thorough validation. The following test cases are designed to ensure behavioral equivalence, transformation correctness, and data integrity.

---

## Migration Validation Tests for `k_ausd_v_ta_discount.ksh`

### Test Case 1.1: Full Data Parity (End-to-End)

*   **Purpose:** To verify that the migrated BigQuery stored procedure, when executed via Cloud Composer, produces an identical dataset in `project.dataset.sof_ta_discount` as the legacy Oracle job produces in `SOF$TA_DISCOUNT`, given the same input data. This is the ultimate output parity test.
*   **Setup:**
    1.  **Legacy Environment:** Populate all source Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_discount_bc_assoc`, `cds$ta_discount`, `cds$ta_care_description`, `cds$ta_disc_vector`) with a comprehensive, representative dataset. This dataset should include various scenarios for joins, filters, NULL values, and date ranges.
    2.  **Target Environment:** Ingest the *exact same* dataset from the Oracle source tables into their respective BigQuery counterparts (`project.isbert_schema.dwtk_meldungen`, `project.source.cds_ta_discount_bc_assoc`, etc.). Ensure schema and data type fidelity during ingestion.
    3.  Ensure both target tables (`SOF$TA_DISCOUNT` in Oracle and `project.dataset.sof_ta_discount` in BigQuery) are empty before execution.
*   **Action:**
    1.  **Legacy Job:** Execute the legacy KornShell script:
        ```bash
        ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh -j "TEST_JOB_ID" -f "TEST_ENTRY_NUM"
        ```
    2.  **Migrated Job:** Trigger the Cloud Composer DAG `dag_k_ausd_v_ta_discount_migration` with the corresponding parameters:
        ```python
        # Example of triggering via Airflow CLI (or UI)
        airflow dags trigger k_ausd_v_ta_discount_migration \
            -c '{"job_kennung": "TEST_JOB_ID", "eintrags_nr": "TEST_ENTRY_NUM"}'
        ```
*   **Pass/Fail Criterion:**
    *   The number of rows in `SOF$TA_DISCOUNT` (Oracle) must be identical to the number of rows in `project.dataset.sof_ta_discount` (BigQuery).
    *   Every column value for every row in `SOF$TA_DISCOUNT` must exactly match the corresponding column value in `project.dataset.sof_ta_discount`. Order of rows does not matter, so a set-based comparison is appropriate.

    ```python
    # Example Python (pytest) assertion using pandas for comparison
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming Oracle client is set up

    def test_full_data_parity():
        # Fetch data from Oracle
        oracle_conn_str = "user/password@host:port/service_name"
        oracle_conn = cx_Oracle.connect(oracle_conn_str)
        oracle_query = "SELECT cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, rabatt, rabatthoehe FROM SOF$TA_DISCOUNT ORDER BY cntrct_id, discount_id"
        oracle_df = pd.read_sql(oracle_query, oracle_conn)
        oracle_conn.close()

        # Fetch data from BigQuery
        bq_client = bigquery.Client()
        bq_query = "SELECT cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, rabatt, rabatthoehe FROM `project.dataset.sof_ta_discount` ORDER BY cntrct_id, discount_id"
        bq_df = bq_client.query(bq_query).to_dataframe()

        # Convert column types to ensure consistent comparison (e.g., BQ INT64 to Oracle NUMBER)
        # This step is crucial if implicit type conversions differ or if BQ schema is slightly different
        # Example: bq_df['cntrct_id'] = bq_df['cntrct_id'].astype(oracle_df['cntrct_id'].dtype)
        # Ensure string columns are trimmed if Oracle might have padded them.

        # Assert row counts
        assert len(oracle_df) == len(bq_df), f"Row count mismatch: Oracle={len(oracle_df)}, BigQuery={len(bq_df)}"

        # Assert data equality (after sorting and type harmonization)
        pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=True, check_exact=False, rtol=1e-5) # rtol for float comparisons
    ```

### Test Case 2.1: Cutoff Date (`v_datum`) Logic

*   **Purpose:** To verify that the `v_datum` (cutoff date) is correctly determined from `project.isbert_schema.dwtk_meldungen` and that the default value (`19000101`) is applied when no matching records are found.
*   **Setup:**
    1.  **Scenario A (Matching Record):** Populate `project.isbert_schema.dwtk_meldungen` with:
        ```sql
        INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('OTHER_JOB', '2023-01-01 10:00:00'),
        ('BERT_DROP_TEMP_TABLE', '2023-03-15 12:30:00'),
        ('BERT_DROP_TEMP_TABLE', '2023-02-20 09:00:00');
        ```
    2.  **Scenario B (No Matching Record):** Empty or ensure no `job_kennung = 'BERT_DROP_TEMP_TABLE'` records in `project.isbert_schema.dwtk_meldungen`.
*   **Action:**
    1.  **Scenario A:** Execute the BigQuery stored procedure.
    2.  **Scenario B:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The `v_datum_string` variable within the stored procedure (or its logged output) must be `'20230315'`.
    *   **Scenario B:** The `v_datum_string` variable within the stored procedure (or its logged output) must be `'19000101'`.

    ```sql
    -- BigQuery SQL to test v_datum logic directly (can be run as part of a test script)
    DECLARE v_datum_string STRING;
    DECLARE v_datum_date DATE;

    -- Scenario A: Test with data
    TRUNCATE TABLE `project.isbert_schema.dwtk_meldungen`;
    INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-01 10:00:00'),
    ('BERT_DROP_TEMP_TABLE', '2023-03-15 12:30:00'),
    ('BERT_DROP_TEMP_TABLE', '2023-02-20 09:00:00');

    SET v_datum_string = (
      SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
      FROM `project.isbert_schema.dwtk_meldungen`
      WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT 'Scenario A v_datum_string:', v_datum_string; -- Expected: '20230315'

    -- Scenario B: Test with no matching data
    TRUNCATE TABLE `project.isbert_schema.dwtk_meldungen`;
    INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-01 10:00:00'); -- No 'BERT_DROP_TEMP_TABLE'

    SET v_datum_string = (
      SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
      FROM `project.isbert_schema.dwtk_meldungen`
      WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT 'Scenario B v_datum_string:', v_datum_string; -- Expected: '19000101'
    ```

### Test Case 2.2: Join Logic and Filter Conditions

*   **Purpose:** To verify that all join conditions and static filter conditions (`cd.LANGUAGE = 1`, `d.is_production = 1`) are correctly translated and applied, producing the expected intermediate result set before date filtering.
*   **Setup:**
    1.  Populate BigQuery source tables with specific data to test each join and filter condition.
        *   `cds_ta_discount_bc_assoc`: `cntrct_id=1, discount_id=101, cntrct_obj_version=1`
        *   `cds_ta_discount`: `discount_id=101, cds_description_id=201, disc_vector_ty='TYPE_A', obj_version=1, is_production=1`
        *   `cds_ta_care_description`: `cds_description_id=201, language=1, cds_description='Discount A'`
        *   `cds_ta_disc_vector`: `discount_id=101, disc_vector_ty='TYPE_A', discount_obj_version=1, calc_rule_value=10.5`
    2.  Include rows that *should not* join or *should* be filtered out (e.g., `d.is_production=0`, `cd.language=2`, mismatching join keys).
    3.  Set `v_datum_date` to a value that will *not* filter out any of the above records (e.g., `DATE '2050-01-01'`).
*   **Action:** Execute the `SELECT` part of the BigQuery stored procedure (excluding the `INSERT INTO` and `TRUNCATE`) with the specified `v_datum_date`.
*   **Pass/Fail Criterion:** The `SELECT` statement must return exactly the expected rows, demonstrating correct application of all join conditions and static filters.

    ```sql
    -- BigQuery SQL to test join and static filter logic
    -- Assume v_datum_date is set to a future date like '2050-01-01' for this test
    TRUNCATE TABLE `project.source.cds_ta_discount_bc_assoc`;
    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES
    (1, 101, 1, '2023-01-01', NULL),
    (2, 102, 1, '2023-01-01', NULL); -- Will not join with d.discount_id=101

    TRUNCATE TABLE `project.source.cds_ta_discount`;
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES
    (101, 201, 'TYPE_A', 1, 1, '2023-01-01', NULL, '2023-01-01', NULL),
    (103, 203, 'TYPE_B', 1, 0, '2023-01-01', NULL, '2023-01-01', NULL); -- is_production=0, should be filtered

    TRUNCATE TABLE `project.source.cds_ta_care_description`;
    INSERT INTO `project.source.cds_ta_care_description` (cds_description_id, language, cds_description) VALUES
    (201, 1, 'Discount A'),
    (202, 2, 'Discount B'); -- language=2, should be filtered

    TRUNCATE TABLE `project.source.cds_ta_disc_vector`;
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES
    (101, 'TYPE_A', 1, 10.5, '2023-01-01', NULL),
    (104, 'TYPE_C', 1, 20.0, '2023-01-01', NULL); -- Will not join

    -- Execute the core SELECT statement with a dummy v_datum_date
    DECLARE v_datum_date DATE DEFAULT '2050-01-01';
    SELECT
      da.cntrct_id,
      da.discount_id,
      d.disc_vector_ty,
      da.cntrct_obj_version,
      cd.cds_description AS rabatt,
      CAST(dv.calc_rule_value AS STRING) AS rabatthoehe
    FROM `project.source.cds_ta_discount_bc_assoc` AS da
    JOIN `project.source.cds_ta_discount` AS d
      ON da.discount_id = d.discount_id
    JOIN `project.source.cds_ta_care_description` AS cd
      ON cd.cds_description_id = d.cds_description_id
     AND cd.language = 1 -- Static filter
    JOIN `project.source.cds_ta_disc_vector` AS dv
      ON d.discount_id = dv.discount_id
     AND d.disc_vector_ty = dv.disc_vector_ty
     AND d.obj_version = dv.discount_obj_version
    WHERE
      (da.insert_at <= v_datum_date AND (da.modified_at IS NULL OR da.modified_at > v_datum_date))
      AND (d.insert_at <= v_datum_date AND (d.modified_at IS NULL OR d.modified_at > v_datum_date))
      AND (d.valid_from <= v_datum_date AND (d.valid_to IS NULL OR d.valid_to > v_datum_date))
      AND (dv.insert_at <= v_datum_date AND (dv.modified_at IS NULL OR dv.modified_at > v_datum_date))
      AND d.is_production = 1; -- Static filter

    -- Expected result:
    -- cntrct_id | discount_id | disc_vector_ty | cntrct_obj_version | rabatt     | rabatthoehe
    -- ----------|-------------|----------------|--------------------|------------|------------
    -- 1         | 101         | TYPE_A         | 1                  | Discount A | 10.5
    ```

### Test Case 2.3: Date Filtering Logic (Edge Cases)

*   **Purpose:** To verify the correct application of all date-based filter conditions, especially around `v_datum_date` and `NULL` values for `modified_at` and `valid_to`.
*   **Setup:**
    1.  Set `v_datum_date` to a specific date, e.g., `DATE '2023-03-01'`.
    2.  Populate source tables with data that tests various date scenarios:
        *   `insert_at` before `v_datum_date`, `modified_at` is `NULL`. (Should pass)
        *   `insert_at` before `v_datum_date`, `modified_at` after `v_datum_date`. (Should pass)
        *   `insert_at` before `v_datum_date`, `modified_at` before `v_datum_date`. (Should fail)
        *   `insert_at` after `v_datum_date`. (Should fail)
        *   `valid_from` before `v_datum_date`, `valid_to` is `NULL`. (Should pass)
        *   `valid_from` before `v_datum_date`, `valid_to` after `v_datum_date`. (Should pass)
        *   `valid_from` before `v_datum_date`, `valid_to` before `v_datum_date`. (Should fail)
        *   `valid_from` after `v_datum_date`. (Should fail)
    3.  Ensure other join and static filter conditions are met for these test rows.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:** Only rows matching the expected date filter logic should be inserted into `project.dataset.sof_ta_discount`.

    ```sql
    -- BigQuery SQL to test date filtering logic
    -- Setup: Clear target and source tables, then insert specific test data
    TRUNCATE TABLE `project.dataset.sof_ta_discount`;
    TRUNCATE TABLE `project.isbert_schema.dwtk_meldungen`;
    TRUNCATE TABLE `project.source.cds_ta_discount_bc_assoc`;
    TRUNCATE TABLE `project.source.cds_ta_discount`;
    TRUNCATE TABLE `project.source.cds_ta_care_description`;
    TRUNCATE TABLE `project.source.cds_ta_disc_vector`;

    -- Set v_datum_date to '2023-03-01'
    INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-03-01 00:00:00');

    -- Test data for da, d, dv (all must pass their respective date filters)
    -- Scenario 1: All dates pass (insert_at < v_datum, modified_at IS NULL)
    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (1, 101, 1, '2023-02-01', NULL);
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (101, 201, 'TYPE_A', 1, 1, '2023-02-01', NULL, '2023-02-01', NULL);
    INSERT INTO `project.source.cds_ta_care_description` (cds_description_id, language, cds_description) VALUES (201, 1, 'Discount A');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (101, 'TYPE_A', 1, 10.5, '2023-02-01', NULL);

    -- Scenario 2: modified_at > v_datum (should pass)
    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (2, 102, 1, '2023-02-01', '2023-03-02');
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (102, 201, 'TYPE_A', 1, 1, '2023-02-01', '2023-03-02', '2023-02-01', '2023-03-02');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (102, 'TYPE_A', 1, 11.5, '2023-02-01', '2023-03-02');

    -- Scenario 3: modified_at < v_datum (should fail)
    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (3, 103, 1, '2023-02-01', '2023-02-28');
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (103, 201, 'TYPE_A', 1, 1, '2023-02-01', '2023-02-28', '2023-02-01', '2023-02-28');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (103, 'TYPE_A', 1, 12.5, '2023-02-01', '2023-02-28');

    -- Scenario 4: valid_to > v_datum (should pass)
    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (4, 104, 1, '2023-02-01', NULL);
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (104, 201, 'TYPE_A', 1, 1, '2023-02-01', NULL, '2023-02-01', '2023-03-02');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (104, 'TYPE_A', 1, 13.5, '2023-02-01', NULL);

    -- Scenario 5: valid_to < v_datum (should fail)
    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (5, 105, 1, '2023-02-01', NULL);
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (105, 201, 'TYPE_A', 1, 1, '2023-02-01', NULL, '2023-02-01', '2023-02-28');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (105, 'TYPE_A', 1, 14.5, '2023-02-01', NULL);

    -- Call the stored procedure
    CALL `project.dataset.r_ausd_v_ta_discount`('TEST_JOB_ID', 'TEST_ENTRY_NUM');

    -- Assert the final count
    SELECT COUNT(*) FROM `project.dataset.sof_ta_discount`; -- Expected: 3 (Scenarios 1, 2, 4)
    SELECT cntrct_id FROM `project.dataset.sof_ta_discount` ORDER BY cntrct_id; -- Expected: 1, 2, 4
    ```

### Test Case 2.4: Column Transformations and Type Handling

*   **Purpose:** To verify that `rabatt` and `rabatthoehe` columns are correctly transformed and cast, especially `CAST(dv.calc_rule_value AS STRING)`.
*   **Setup:**
    1.  Populate source tables with data where `cds_description` contains special characters or varying lengths, and `calc_rule_value` has different numeric formats (integer, decimal, potentially large numbers).
        *   `cds_ta_care_description`: `cds_description='Special Discount €10.00'`
        *   `cds_ta_disc_vector`: `calc_rule_value=100`, `calc_rule_value=123.45`, `calc_rule_value=0.00`, `calc_rule_value=-5.5`
    2.  Ensure all other filter and join conditions are met for these test rows.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    *   The `rabatt` column in `project.dataset.sof_ta_discount` must exactly match `cds_description` from `cds_ta_care_description`.
    *   The `rabatthoehe` column must correctly represent the `calc_rule_value` as a string, preserving decimal places and sign.

    ```sql
    -- BigQuery SQL to test column transformations
    TRUNCATE TABLE `project.dataset.sof_ta_discount`;
    TRUNCATE TABLE `project.isbert_schema.dwtk_meldungen`;
    TRUNCATE TABLE `project.source.cds_ta_discount_bc_assoc`;
    TRUNCATE TABLE `project.source.cds_ta_discount`;
    TRUNCATE TABLE `project.source.cds_ta_care_description`;
    TRUNCATE TABLE `project.source.cds_ta_disc_vector`;

    INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00');

    -- Test data for rabatt and rabatthoehe
    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (1, 101, 1, '2022-01-01', NULL);
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (101, 201, 'TYPE_A', 1, 1, '2022-01-01', NULL, '2022-01-01', NULL);
    INSERT INTO `project.source.cds_ta_care_description` (cds_description_id, language, cds_description) VALUES (201, 1, 'Special Discount €10.00');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (101, 'TYPE_A', 1, 123.45, '2022-01-01', NULL);

    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (2, 102, 1, '2022-01-01', NULL);
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (102, 202, 'TYPE_B', 1, 1, '2022-01-01', NULL, '2022-01-01', NULL);
    INSERT INTO `project.source.cds_ta_care_description` (cds_description_id, language, cds_description) VALUES (202, 1, 'Zero Discount');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (102, 'TYPE_B', 1, 0.00, '2022-01-01', NULL);

    CALL `project.dataset.r_ausd_v_ta_discount`('TEST_JOB_ID', 'TEST_ENTRY_NUM');

    SELECT rabatt, rabatthoehe FROM `project.dataset.sof_ta_discount` ORDER BY cntrct_id;
    -- Expected:
    -- rabatt                  | rabatthoehe
    -- ------------------------|------------
    -- Special Discount €10.00 | 123.45
    -- Zero Discount           | 0.0
    ```

### Test Case 2.5: NULL Handling for `calc_rule_value`

*   **Purpose:** To verify that `CAST(dv.calc_rule_value AS STRING)` correctly handles `NULL` values for `calc_rule_value`.
*   **Setup:**
    1.  Populate source tables with a record where `cds_ta_disc_vector.calc_rule_value` is `NULL`.
    2.  Ensure all other filter and join conditions are met for this test row.
*   **Action:** Execute the BigQuery stored procedure.
*   **Pass/Fail Criterion:** The `rabatthoehe` column in `project.dataset.sof_ta_discount` for the test record must be `NULL`.

    ```sql
    -- BigQuery SQL to test NULL calc_rule_value
    TRUNCATE TABLE `project.dataset.sof_ta_discount`;
    TRUNCATE TABLE `project.isbert_schema.dwtk_meldungen`;
    TRUNCATE TABLE `project.source.cds_ta_discount_bc_assoc`;
    TRUNCATE TABLE `project.source.cds_ta_discount`;
    TRUNCATE TABLE `project.source.cds_ta_care_description`;
    TRUNCATE TABLE `project.source.cds_ta_disc_vector`;

    INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00');

    INSERT INTO `project.source.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, cntrct_obj_version, insert_at, modified_at) VALUES (1, 101, 1, '2022-01-01', NULL);
    INSERT INTO `project.source.cds_ta_discount` (discount_id, cds_description_id, disc_vector_ty, obj_version, is_production, insert_at, modified_at, valid_from, valid_to) VALUES (101, 201, 'TYPE_A', 1, 1, '2022-01-01', NULL, '2022-01-01', NULL);
    INSERT INTO `project.source.cds_ta_care_description` (cds_description_id, language, cds_description) VALUES (201, 1, 'Null Value Test');
    INSERT INTO `project.source.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, calc_rule_value, insert_at, modified_at) VALUES (101, 'TYPE_A', 1, NULL, '2022-01-01', NULL);

    CALL `project.dataset.r_ausd_v_ta_discount`('TEST_JOB_ID', 'TEST_ENTRY_NUM');

    SELECT rabatthoehe FROM `project.dataset.sof_ta_discount` WHERE cntrct_id = 1;
    -- Expected: NULL
    ```

### Test Case 3.1: Source Data Ingestion Fidelity

*   **Purpose:** To confirm that the data ingested from Oracle source tables into BigQuery maintains schema and data integrity, which is a prerequisite for the transformation logic.
*   **Setup:**
    1.  Populate Oracle source tables (`cds$ta_discount_bc_assoc`, `cds$ta_discount`, `cds$ta_care_description`, `cds$ta_disc_vector`, `isbert_schema.dwtk_meldungen`) with a diverse set of data, including various data types, NULLs, and edge cases.
*   **Action:** Execute the ingestion pipelines that move data from Oracle to BigQuery for these tables.
*   **Pass/Fail Criterion:**
    *   **Schema Parity:** The BigQuery table schemas (`project.source.cds_ta_discount_bc_assoc`, etc.) must match their Oracle counterparts in terms of column names, data types, and nullability.
    *   **Data Parity:** For each source table, a full data comparison (row count and content) between the Oracle table and its BigQuery ingested equivalent must show 100% match.

    ```python
    # Example Python (pytest) assertion for a single source table
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle

    def test_source_table_ingestion_parity(table_name_oracle, table_name_bq):
        # Fetch data from Oracle
        oracle_conn_str = "user/password@host:port/service_name"
        oracle_conn = cx_Oracle.connect(oracle_conn_str)
        oracle_query = f"SELECT * FROM {table_name_oracle} ORDER BY 1" # Order by first column for consistency
        oracle_df = pd.read_sql(oracle_query, oracle_conn)
        oracle_conn.close()

        # Fetch data from BigQuery
        bq_client = bigquery.Client()
        bq_query = f"SELECT * FROM `{table_name_bq}` ORDER BY 1"
        bq_df = bq_client.query(bq_query).to_dataframe()

        # Assert row counts
        assert len(oracle_df) == len(bq_df), f"Row count mismatch for {table_name_oracle}: Oracle={len(oracle_df)}, BigQuery={len(bq_df)}"

        # Assert data equality (after sorting and type harmonization if needed)
        # Note: Type harmonization might be complex and depend on ingestion tool.
        # For example, Oracle NUMBER(p,s) might become BQ NUMERIC or FLOAT64.
        # Ensure string columns are trimmed if Oracle might have padded them.
        pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False, check_exact=False, rtol=1e-5) # check_dtype=False if types are intentionally different but compatible
    ```

### Test Case 4.1: Target Table Schema and Data Types

*   **Purpose:** To verify that the BigQuery target table `project.dataset.sof_ta_discount` has the correct schema (column names, data types, nullability) as derived from the Oracle `SOF$TA_DISCOUNT` table.
*   **Setup:**
    1.  Ensure the `project.dataset.sof_ta_discount` table has been created in BigQuery using the provided DDL.
    2.  Obtain the schema definition for the original Oracle `SOF$TA_DISCOUNT` table.
*   **Action:** Query the schema of `project.dataset.sof_ta_discount` in BigQuery.
*   **Pass/Fail Criterion:** The BigQuery table schema must match the Oracle schema. This includes:
    *   All expected columns are present.
    *   Column names match (case-insensitivity might be a factor depending on Oracle setup, but BigQuery is case-sensitive for column names).
    *   Data types are appropriate BigQuery equivalents (e.g., Oracle `NUMBER` to BigQuery `INT64` or `NUMERIC`, Oracle `VARCHAR2` to BigQuery `STRING`).
    *   Nullability constraints are correctly applied.

    ```sql
    -- BigQuery SQL to check schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_discount'
    ORDER BY
        ordinal_position;

    -- Expected Output (example, based on design document):
    -- column_name        | data_type | is_nullable
    -- -------------------|-----------|------------
    -- cntrct_id          | INT64     | YES
    -- discount_id        | INT64     | YES
    -- disc_vector_ty     | STRING    | YES
    -- cntrct_obj_version | INT64     | YES
    -- rabatt             | STRING    | YES
    -- rabatthoehe        | STRING    | YES
    ```
    *(Note: Nullability for `cntrct_id`, `discount_id`, `disc_vector_ty`, `cntrct_obj_version` might be `NO` if they are primary keys or not nullable in Oracle. The provided DDL doesn't specify, so `YES` is assumed based on default BigQuery behavior for `CREATE TABLE` without `NOT NULL`.)*

### Test Case 4.2: Row Count Parity

*   **Purpose:** To verify that the total number of rows inserted into `project.dataset.sof_ta_discount` by the migrated job is identical to the number of rows inserted by the legacy job.
*   **Setup:**
    1.  Use the same comprehensive dataset as in Test Case 1.1.
    2.  Ensure both target tables are empty before execution.
*   **Action:**
    1.  Execute the legacy KornShell script. Capture the record count from `tmpFile`.
    2.  Execute the Cloud Composer DAG. Capture the `records_loaded` from the stored procedure's final `SELECT` statement (e.g., via Airflow task logs or by querying a logging table if implemented).
*   **Pass/Fail Criterion:** The record count reported by the legacy job must exactly match the `records_loaded` value returned by the BigQuery stored procedure.

    ```python
    # Example Python (pytest) assertion
    def test_row_count_parity(legacy_record_count, migrated_record_count):
        assert legacy_record_count == migrated_record_count, \
            f"Row count mismatch: Legacy={legacy_record_count}, Migrated={migrated_record_count}"
    ```

### Test Case 4.3: Parameter Validation

*   **Purpose:** To verify that the BigQuery stored procedure correctly validates input parameters (`p_JobKennung`, `p_EintragsNr`) and raises an error if they are `NULL` or empty, mimicking the KornShell script's `pruefeParameterGesetzt` logic.
*   **Setup:**
    1.  Ensure source tables are populated minimally to allow the procedure to run past `v_datum` determination.
*   **Action:**
    1.  Attempt to call the BigQuery stored procedure with `p_JobKennung` as `NULL`.
    2.  Attempt to call the BigQuery stored procedure with `p_JobKennung` as an empty string `''`.
    3.  Repeat for `p_EintragsNr`.
*   **Pass/Fail Criterion:**
    *   Each attempt with `NULL` or empty parameters must result in the stored procedure raising an error (e.g., `RAISE` statement in BigQuery) with the expected error message.
    *   The Cloud Composer task calling the stored procedure should fail in these scenarios.

    ```sql
    -- BigQuery SQL to test parameter validation
    -- Test 1: p_JobKennung IS NULL
    BEGIN
      CALL `project.dataset.r_ausd_v_ta_discount`(NULL, '12345');
      EXCEPTION WHEN ERROR THEN
        SELECT @@error.message; -- Expected: "FEHLER: 1 Jobkennung ist ein Pflichtparameter und darf nicht leer sein."
    END;

    -- Test 2: p_JobKennung IS EMPTY
    BEGIN
      CALL `project.dataset.r_ausd_v_ta_discount`('', '12345');
      EXCEPTION WHEN ERROR THEN
        SELECT @@error.message; -- Expected: "FEHLER: 1 Jobkennung ist ein Pflichtparameter und darf nicht leer sein."
    END;

    -- Test 3: p_EintragsNr IS NULL
    BEGIN
      CALL `project.dataset.r_ausd_v_ta_discount`('TEST_JOB_ID', NULL);
      EXCEPTION WHEN ERROR THEN
        SELECT @@error.message; -- Expected: "FEHLER: 1 EintragsNr ist ein Pflichtparameter und darf nicht leer sein."
    END;

    -- Test 4: p_EintragsNr IS EMPTY
    BEGIN
      CALL `project.dataset.r_ausd_v_ta_discount`('TEST_JOB_ID', '');
      EXCEPTION WHEN ERROR THEN
        SELECT @@error.message; -- Expected: "FEHLER: 1 EintragsNr ist ein Pflichtparameter und darf nicht leer sein."
    END;
    ```