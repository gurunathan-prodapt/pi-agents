As a senior data-migration QA engineer, I have reviewed the migration design for `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL`. The following test cases are designed to ensure the migrated job is behaviourally equivalent to the legacy source, covering output parity, transformation correctness, external system replacements, and data quality assertions.

For the purpose of these tests, `your-gcp-project-id` will be used as a placeholder for the actual GCP Project ID.

---

## Migration Validation Tests: DW.BERT_AUSD_V_TA_CNTRCT_TEMPL

### 1. Output Parity

#### Test Case 1.1: Full Data Parity (End-to-End)

*   **Purpose:** To verify that the final output table in BigQuery (`curated.final_fact_table`) is identical in content to the legacy Oracle target table (`sof$ta_cntrct_templ`) when processed with the same source data and processing date.
*   **Setup:**
    1.  **Legacy Snapshot:** Capture a full snapshot of the source tables (`cds$ta_cntrct_template`, `cds$ta_care_description`) and the metadata table (`isbert_schema.dwtk_meldungen`) from the Oracle environment.
    2.  **Legacy Run:** Execute the legacy `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` job in the Oracle environment using the captured source data. Record the contents of the `sof$ta_cntrct_templ` table.
    3.  **Migrated Data Load:** Load the captured Oracle source data into the corresponding BigQuery staging tables (`staging.cds_ta_cntrct_template_stg`, `staging.cds_ta_care_description_stg`) and metadata table (`metadata.dwtk_meldungen`).
    4.  **Migrated Run:** Trigger the `dw_bert_ausd_v_ta_cntrct_templ_dag` Airflow DAG in the GCP environment. Ensure the `v_datum` determined by the DAG matches the one used in the legacy run.
*   **Action:** Compare every row and column of the legacy `sof$ta_cntrct_templ` table with the migrated `your-gcp-project-id.curated.final_fact_table` table.
*   **Pass/Fail Criterion:** The two tables must be byte-for-byte identical. This includes row count, column values, and data types.

    ```sql
    -- Example SQL for BigQuery to compare with a legacy snapshot table (e.g., loaded as a temporary BQ table)
    -- Assume 'legacy_sof_ta_cntrct_templ_snapshot' is a BigQuery table containing the exact data from the Oracle legacy run.

    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM `your-gcp-project-id.curated.final_fact_table`) = (SELECT COUNT(*) FROM `your-gcp-project-id.legacy_snapshot.sof_ta_cntrct_templ_snapshot`)
            THEN 'Row counts match.'
            ELSE 'Row counts mismatch!'
        END AS row_count_check,
        (
            SELECT COUNT(*)
            FROM (
                (SELECT * FROM `your-gcp-project-id.curated.final_fact_table` EXCEPT DISTINCT SELECT * FROM `your-gcp-project-id.legacy_snapshot.sof_ta_cntrct_templ_snapshot`)
                UNION ALL
                (SELECT * FROM `your-gcp-project-id.legacy_snapshot.sof_ta_cntrct_templ_snapshot` EXCEPT DISTINCT SELECT * FROM `your-gcp-project-id.curated.final_fact_table`)
            )
        ) AS differing_rows_count;
    ```
    **Pass:** `row_count_check` is 'Row counts match.' AND `differing_rows_count` is 0.

---

### 2. Transformation Correctness

#### Test Case 2.1: `v_datum` Determination Logic

*   **Purpose:** To verify that the `get_processing_date_from_bq` function (and thus the `get_processing_date` Airflow task) correctly determines the `v_datum` from `metadata.dwtk_meldungen`, including the default '19000101' behavior.
*   **Setup:**
    1.  **Scenario A (Normal):** Populate `your-gcp-project-id.metadata.dwtk_meldungen` with multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values, ensuring a clear maximum.
    2.  **Scenario B (No Match):** Clear `your-gcp-project-id.metadata.dwtk_meldungen` or ensure no entries match `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    3.  **Scenario C (NULL timecreated):** Populate `your-gcp-project-id.metadata.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` but `timecreated` is NULL.
*   **Action:**
    *   For each scenario, execute the `get_processing_date_task` in the Airflow DAG (or directly call `utils.get_processing_date_from_bq`).
    *   Retrieve the `v_datum` value pushed to XCom.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The `v_datum` must be `FORMAT_DATE('%Y%m%d', MAX(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Scenario B & C:** The `v_datum` must be '19000101'.

    ```python
    # Example pytest for utils.get_processing_date_from_bq
    import pytest
    from unittest.mock import MagicMock
    from google.cloud import bigquery
    from datetime import datetime
    import python.utils as utils

    @pytest.fixture
    def mock_bq_client():
        with MagicMock(spec=bigquery.Client) as mock_client:
            yield mock_client

    def test_get_processing_date_normal(mock_bq_client):
        # Mock query result for Scenario A
        mock_row = MagicMock()
        mock_row.__getitem__.side_effect = ['20231026'] # MAX(timecreated)
        mock_bq_client.query.return_value.result.return_value = [mock_row]

        result = utils.get_processing_date_from_bq("test-project", "BERT_DROP_TEMP_TABLE")
        assert result == '20231026'

    def test_get_processing_date_no_match(mock_bq_client):
        # Mock query result for Scenario B (no rows returned)
        mock_bq_client.query.return_value.result.return_value = []

        result = utils.get_processing_date_from_bq("test-project", "BERT_DROP_TEMP_TABLE")
        assert result == '19000101'

    def test_get_processing_date_null_timecreated(mock_bq_client):
        # Mock query result for Scenario C (row returned, but value is None)
        mock_row = MagicMock()
        mock_row.__getitem__.side_effect = [None]
        mock_bq_client.query.return_value.result.return_value = [mock_row]

        result = utils.get_processing_date_from_bq("test-project", "BERT_DROP_TEMP_TABLE")
        assert result == '19000101'
    ```

#### Test Case 2.2: Join Logic (`cds_description_id`)

*   **Purpose:** To verify that the `JOIN` condition `ct.cds_description_id = cd.cds_description_id` is correctly applied, behaving as an `INNER JOIN`.
*   **Setup:**
    1.  Populate `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` with `cntrct_template_id` and `cds_description_id` values.
    2.  Populate `your-gcp-project-id.staging.cds_ta_care_description_stg` with `cds_description_id` and `cds_description` values.
    3.  Include data that:
        *   Has matching `cds_description_id` in both tables.
        *   Exists in `cds_ta_cntrct_template_stg` but not in `cds_ta_care_description_stg`.
        *   Exists in `cds_ta_care_description_stg` but not in `cds_ta_cntrct_template_stg`.
    4.  Set `v_datum` and other filter conditions to ensure all rows would otherwise pass.
*   **Action:** Run the `transform_and_load_data_task` (or the full DAG).
*   **Pass/Fail Criterion:** The `your-gcp-project-id.curated.final_fact_table` must only contain rows where `cds_description_id` exists in both `cds_ta_cntrct_template_stg` and `cds_ta_care_description_stg`.

    ```sql
    -- After running the DAG, verify join logic
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    LEFT JOIN
        `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` AS ct
        ON final.CNTRCT_TEMPLATE_ID = ct.cntrct_template_id AND final.CDS_DESCRIPTION_ID = ct.cds_description_id
    LEFT JOIN
        `your-gcp-project-id.staging.cds_ta_care_description_stg` AS cd
        ON final.CDS_DESCRIPTION_ID = cd.cds_description_id
    WHERE
        ct.cntrct_template_id IS NULL OR cd.cds_description_id IS NULL;
    ```
    **Pass:** The query returns 0, indicating all rows in the final table have matching entries in both staging tables.

#### Test Case 2.3: Date Filtering - `insert_at`

*   **Purpose:** To verify the `ct.insert_at <= PARSE_DATE('%Y%m%d', '{{ params.v_datum }}')` filter.
*   **Setup:**
    1.  Populate `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` with rows having `insert_at` values:
        *   Before `v_datum` (e.g., `2023-01-01` if `v_datum` is `20230115`).
        *   Equal to `v_datum` (e.g., `2023-01-15` if `v_datum` is `20230115`).
        *   After `v_datum` (e.g., `2023-01-30` if `v_datum` is `20230115`).
    2.  Ensure all other filter conditions and join criteria are met for these rows.
    3.  Set `v_datum` to a specific date (e.g., '20230115').
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** Only rows where `insert_at` is less than or equal to `v_datum` should be present in `your-gcp-project-id.curated.final_fact_table`.

    ```sql
    -- After running the DAG with v_datum='20230115'
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    JOIN
        `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` AS ct
        ON final.CNTRCT_TEMPLATE_ID = ct.cntrct_template_id
    WHERE
        ct.insert_at > PARSE_DATE('%Y%m%d', '20230115');
    ```
    **Pass:** The query returns 0.

#### Test Case 2.4: Date Filtering - `modified_at` (NULL and Greater Than)

*   **Purpose:** To verify the `(ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', '{{ params.v_datum }}'))` filter.
*   **Setup:**
    1.  Populate `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` with rows having `modified_at` values:
        *   `NULL`.
        *   After `v_datum`.
        *   Equal to `v_datum`.
        *   Before `v_datum`.
    2.  Ensure all other filter conditions and join criteria are met.
    3.  Set `v_datum` to a specific date (e.g., '20230115').
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** Only rows where `modified_at IS NULL` or `modified_at` is strictly greater than `v_datum` should be present in `your-gcp-project-id.curated.final_fact_table`.

    ```sql
    -- After running the DAG with v_datum='20230115'
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    JOIN
        `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` AS ct
        ON final.CNTRCT_TEMPLATE_ID = ct.cntrct_template_id
    WHERE
        ct.modified_at IS NOT NULL
        AND ct.modified_at <= PARSE_DATE('%Y%m%d', '20230115');
    ```
    **Pass:** The query returns 0.

#### Test Case 2.5: Date Filtering - `valid_from`

*   **Purpose:** To verify the `ct.valid_from <= PARSE_DATE('%Y%m%d', '{{ params.v_datum }}')` filter.
*   **Setup:** Similar to Test Case 2.3, but for the `valid_from` column.
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** Only rows where `valid_from` is less than or equal to `v_datum` should be present in `your-gcp-project-id.curated.final_fact_table`.

    ```sql
    -- After running the DAG with v_datum='20230115'
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    JOIN
        `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` AS ct
        ON final.CNTRCT_TEMPLATE_ID = ct.cntrct_template_id
    WHERE
        ct.valid_from > PARSE_DATE('%Y%m%d', '20230115');
    ```
    **Pass:** The query returns 0.

#### Test Case 2.6: Date Filtering - `valid_to` (NULL and Greater Than)

*   **Purpose:** To verify the `(ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', '{{ params.v_datum }}'))` filter.
*   **Setup:** Similar to Test Case 2.4, but for the `valid_to` column.
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** Only rows where `valid_to IS NULL` or `valid_to` is strictly greater than `v_datum` should be present in `your-gcp-project-id.curated.final_fact_table`.

    ```sql
    -- After running the DAG with v_datum='20230115'
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    JOIN
        `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` AS ct
        ON final.CNTRCT_TEMPLATE_ID = ct.cntrct_template_id
    WHERE
        ct.valid_to IS NOT NULL
        AND ct.valid_to <= PARSE_DATE('%Y%m%d', '20230115');
    ```
    **Pass:** The query returns 0.

#### Test Case 2.7: `is_production` Filter

*   **Purpose:** To verify the `ct.is_production = 1` filter.
*   **Setup:**
    1.  Populate `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` with rows where `is_production` is `0` and `1`.
    2.  Ensure all other filter conditions and join criteria are met for these rows.
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** Only rows where `is_production = 1` should be present in `your-gcp-project-id.curated.final_fact_table`.

    ```sql
    -- After running the DAG
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    JOIN
        `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` AS ct
        ON final.CNTRCT_TEMPLATE_ID = ct.cntrct_template_id
    WHERE
        ct.is_production = 0;
    ```
    **Pass:** The query returns 0.

#### Test Case 2.8: `language` Filter

*   **Purpose:** To verify the `cd.language = 1` filter.
*   **Setup:**
    1.  Populate `your-gcp-project-id.staging.cds_ta_care_description_stg` with rows where `language` is `1` and other values (e.g., `2`, `3`).
    2.  Ensure all other filter conditions and join criteria are met for these rows.
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** Only rows where `language = 1` should be present in `your-gcp-project-id.curated.final_fact_table`.

    ```sql
    -- After running the DAG
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    JOIN
        `your-gcp-project-id.staging.cds_ta_care_description_stg` AS cd
        ON final.CDS_DESCRIPTION_ID = cd.cds_description_id
    WHERE
        cd.language != 1;
    ```
    **Pass:** The query returns 0.

#### Test Case 2.9: NULL Handling in Output Columns (`CDS_DESCRIPTION`)

*   **Purpose:** To verify that NULL values in the source `cds_description` column are correctly propagated to the target `CDS_DESCRIPTION` column.
*   **Setup:**
    1.  Populate `your-gcp-project-id.staging.cds_ta_care_description_stg` with some rows where `cds_description` is `NULL`.
    2.  Ensure these rows would otherwise pass all join and filter conditions.
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** The `CDS_DESCRIPTION` column in `your-gcp-project-id.curated.final_fact_table` must contain `NULL` values for the corresponding rows where `cds_description` was `NULL` in the source.

    ```sql
    -- After running the DAG
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table` AS final
    JOIN
        `your-gcp-project-id.staging.cds_ta_care_description_stg` AS cd
        ON final.CDS_DESCRIPTION_ID = cd.cds_description_id
    WHERE
        final.CDS_DESCRIPTION IS NOT NULL AND cd.cds_description IS NULL;
    ```
    **Pass:** The query returns 0. (And conversely, check that if `cd.cds_description` is NOT NULL, `final.CDS_DESCRIPTION` is also NOT NULL and matches).

#### Test Case 2.10: Data Type Handling

*   **Purpose:** To verify that data types are correctly mapped and handled during transformation, specifically for `INT64` and `TIMESTAMP` conversions from Oracle types.
*   **Setup:**
    1.  Ensure the DDLs for staging and curated tables in BigQuery correctly reflect the intended data types (e.g., `INT64` for IDs, `TIMESTAMP` for dates).
    2.  Populate staging tables with values that test type boundaries or potential conversion issues (e.g., very large `INT64` values, specific date formats that might be ambiguous).
*   **Action:** Run the `transform_and_load_data_task`.
*   **Pass/Fail Criterion:** The DAG must complete without type conversion errors. All values in `your-gcp-project-id.curated.final_fact_table` must be stored with the correct BigQuery data types as defined in the DDL, and their values must accurately reflect the source data.

    ```sql
    -- Check schema of the target table
    SELECT
        column_name,
        data_type
    FROM
        `your-gcp-project-id.curated.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'final_fact_table';
    ```
    **Pass:** The output schema matches the expected DDL (`CNTRCT_TEMPLATE_ID INT64`, `CDS_DESCRIPTION_ID INT64`, `CDS_DESCRIPTION STRING`). Further, no runtime errors related to type conversion should occur during the DAG run.

---

### 3. External-System Replacements

#### Test Case 3.1: Carmen DB Extraction (`cds$ta_cntrct_template`)

*   **Purpose:** To verify that `data_ingestion.extract_cds_ta_cntrct_template_to_bq` successfully connects to the Carmen Oracle DB and extracts data into `staging.cds_ta_cntrct_template_stg`.
*   **Setup:**
    1.  Ensure a test Carmen Oracle DB is accessible from the Airflow environment.
    2.  Populate the Oracle `cds$ta_cntrct_template` table with a known set of test data.
    3.  Configure the Airflow `oracle_conn_id` (e.g., `oracle_carmen_db`) to point to this test Oracle DB.
*   **Action:** Manually trigger the `extract_cds_ta_cntrct_template_task` in Airflow.
*   **Pass/Fail Criterion:**
    1.  The task must complete successfully without errors.
    2.  The `your-gcp-project-id.staging.cds_ta_cntrct_template_stg` table in BigQuery must be populated.
    3.  The row count and a representative sample of data in the BigQuery staging table must exactly match the Oracle `cds$ta_cntrct_template` table.

    ```sql
    -- After task execution, compare row counts
    -- (Requires a mechanism to get Oracle row count, e.g., via SQL Developer or a separate script)
    SELECT COUNT(*) FROM `your-gcp-project-id.staging.cds_ta_cntrct_template_stg`;
    ```
    **Pass:** BigQuery row count matches Oracle row count, and data sample comparison confirms content parity.

#### Test Case 3.2: Carmen DB Extraction (`cds$ta_care_description`)

*   **Purpose:** To verify that `data_ingestion.extract_cds_ta_care_description_to_bq` successfully connects to the Carmen Oracle DB and extracts data into `staging.cds_ta_care_description_stg`.
*   **Setup:** Similar to Test Case 3.1, but for the `cds$ta_care_description` table.
*   **Action:** Manually trigger the `extract_cds_ta_care_description_task` in Airflow.
*   **Pass/Fail Criterion:**
    1.  The task must complete successfully without errors.
    2.  The `your-gcp-project-id.staging.cds_ta_care_description_stg` table in BigQuery must be populated.
    3.  The row count and a representative sample of data in the BigQuery staging table must exactly match the Oracle `cds$ta_care_description` table.

    ```sql
    -- After task execution, compare row counts
    SELECT COUNT(*) FROM `your-gcp-project-id.staging.cds_ta_care_description_stg`;
    ```
    **Pass:** BigQuery row count matches Oracle row count, and data sample comparison confirms content parity.

---

### 4. Data-Quality / Row-Count / Schema Assertions

#### Test Case 4.1: Target Table Schema Validation

*   **Purpose:** To verify that the `your-gcp-project-id.curated.final_fact_table` schema matches the expected schema (based on `sof$ta_cntrct_templ` and the provided DDL).
*   **Setup:**
    1.  Ensure the `curated_final_fact_table.sql` DDL has been applied.
    2.  Run the full `dw_bert_ausd_v_ta_cntrct_templ_dag` Airflow DAG at least once to ensure the table is created/replaced.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for `your-gcp-project-id.curated.final_fact_table`.
*   **Pass/Fail Criterion:** The column names, data types, and nullability (if applicable) must match the DDL:
    *   `CNTRCT_TEMPLATE_ID` (INT64, NOT NULL)
    *   `CDS_DESCRIPTION_ID` (INT64, NOT NULL)
    *   `CDS_DESCRIPTION` (STRING, NULLABLE)

    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your-gcp-project-id.curated.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'final_fact_table'
    ORDER BY
        ordinal_position;
    ```
    **Pass:** The query result matches the expected schema definition.

#### Test Case 4.2: Row Count Parity (Full Load)

*   **Purpose:** To verify that the total number of rows in the target BigQuery table matches the legacy Oracle table after a full load.
*   **Setup:**
    1.  Use the same setup as Test Case 1.1 (Full Data Parity).
    2.  Record the row count of the legacy `sof$ta_cntrct_templ` table.
*   **Action:** Query the row count of `your-gcp-project-id.curated.final_fact_table` after the DAG completes.
*   **Pass/Fail Criterion:** The row count in BigQuery must be identical to the Oracle row count.

    ```sql
    SELECT COUNT(*) FROM `your-gcp-project-id.curated.final_fact_table`;
    ```
    **Pass:** The count matches the legacy Oracle table's row count.

#### Test Case 4.3: Row Count Parity (Incremental Load - `v_datum` impact)

*   **Purpose:** To verify that changing `v_datum` correctly impacts the number of rows loaded, matching legacy behavior.
*   **Setup:**
    1.  Populate staging tables with data such that different `v_datum` values would result in different numbers of filtered rows.
    2.  **Scenario 1:** Run the legacy job with `v_datum_1` (e.g., '20230101'), record the row count of `sof$ta_cntrct_templ`.
    3.  **Scenario 2:** Run the legacy job with `v_datum_2` (e.g., '20230601'), record the row count of `sof$ta_cntrct_templ`.
    4.  **Scenario 3:** Run the migrated DAG with `v_datum_1` (by setting `metadata.dwtk_meldungen` appropriately), record the row count of `final_fact_table`.
    5.  **Scenario 4:** Run the migrated DAG with `v_datum_2` (by setting `metadata.dwtk_meldungen` appropriately), record the row count of `final_fact_table`.
*   **Action:** Compare row counts for each `v_datum` between legacy and migrated.
*   **Pass/Fail Criterion:** The row count for `v_datum_1` in BigQuery must match the legacy `v_datum_1` count. The row count for `v_datum_2` in BigQuery must match the legacy `v_datum_2` count.

#### Test Case 4.4: Uniqueness Constraint (Implicit)

*   **Purpose:** To verify that the combination of `CNTRCT_TEMPLATE_ID` and `CDS_DESCRIPTION_ID` (which appears to form a composite key or at least a unique identifier in the source) remains unique in the target table if it was unique in the source.
*   **Setup:**
    1.  Run the full `dw_bert_ausd_v_ta_cntrct_templ_dag` Airflow DAG with a representative dataset.
    2.  Confirm the uniqueness of `(CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID)` in the legacy `sof$ta_cntrct_templ` table.
*   **Action:** Query `your-gcp-project-id.curated.final_fact_table` for duplicate combinations of `CNTRCT_TEMPLATE_ID` and `CDS_DESCRIPTION_ID`.
*   **Pass/Fail Criterion:** The query should return 0 rows, indicating no duplicates for the specified combination.

    ```sql
    SELECT
        CNTRCT_TEMPLATE_ID,
        CDS_DESCRIPTION_ID,
        COUNT(*) AS duplicate_count
    FROM
        `your-gcp-project-id.curated.final_fact_table`
    GROUP BY
        CNTRCT_TEMPLATE_ID,
        CDS_DESCRIPTION_ID
    HAVING
        COUNT(*) > 1;
    ```
    **Pass:** The query returns an empty result set.

#### Test Case 4.5: Data Integrity - No Missing Values in Key Columns

*   **Purpose:** To verify that critical identifier columns (`CNTRCT_TEMPLATE_ID`, `CDS_DESCRIPTION_ID`) are never NULL in the target table, as they are derived from non-nullable source columns and are essential for joins.
*   **Setup:** Run the full `dw_bert_ausd_v_ta_cntrct_templ_dag` Airflow DAG.
*   **Action:** Query `your-gcp-project-id.curated.final_fact_table` for NULL values in `CNTRCT_TEMPLATE_ID` and `CDS_DESCRIPTION_ID`.
*   **Pass/Fail Criterion:** The count of rows with NULLs in these columns must be 0.

    ```sql
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.curated.final_fact_table`
    WHERE
        CNTRCT_TEMPLATE_ID IS NULL OR CDS_DESCRIPTION_ID IS NULL;
    ```
    **Pass:** The query returns 0.

#### Test Case 4.6: `control.etl_job_run` and `control.etl_watermark` Updates

*   **Purpose:** To verify that the control tables (`control.etl_job_run`, `control.etl_watermark`) are correctly updated after a successful DAG run.
*   **Setup:**
    1.  Ensure the DDLs for `control_etl_job_run.sql` and `control_etl_watermark.sql` have been applied.
    2.  Run the full `dw_bert_ausd_v_ta_cntrct_templ_dag` Airflow DAG.
*   **Action:** Query `your-gcp-project-id.control.etl_job_run` and `your-gcp-project-id.control.etl_watermark`.
*   **Pass/Fail Criterion:**
    *   `your-gcp-project-id.control.etl_job_run` must contain an entry for `job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL'` with `status = 'SUCCESS'`, and valid `start_time`/`end_time`.
    *   `your-gcp-project-id.control.etl_watermark` must have an entry for `table_name = 'final_fact_table'` with `last_watermark_value` matching the `v_datum` used in the run (converted to `TIMESTAMP`).

    ```sql
    -- Check etl_job_run
    SELECT
        job_id,
        status,
        start_time,
        end_time
    FROM
        `your-gcp-project-id.control.etl_job_run`
    WHERE
        job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL'
    ORDER BY
        start_time DESC
    LIMIT 1;

    -- Check etl_watermark
    SELECT
        table_name,
        last_watermark_value
    FROM
        `your-gcp-project-id.control.etl_watermark`
    WHERE
        table_name = 'final_fact_table';
    ```
    **Pass:** The `etl_job_run` entry shows `SUCCESS` for the latest run, and `etl_watermark` shows the correct `last_watermark_value` corresponding to the `v_datum` used.