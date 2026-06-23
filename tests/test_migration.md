As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `r_ausd_rechempf.ksh` job migration to Google Cloud BigQuery and Airflow. These tests aim to ensure behavioral equivalence, data integrity, and correctness across all aspects of the migration.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## General Test Setup & Prerequisites

Before executing any of the following tests, the following setup must be in place:

1.  **Golden Data Snapshot:** A snapshot of the source Oracle database tables (`bpd$ta_means_of_payment`, `bpd$ta_bank`, `bpd$ta_bank_international`, `isbert_schema.dwtk_meldungen`, `sof$ta_e_reach_re`, `sof$ta_e_business_re`, `sof$ta_e_regulierer`, `dwh$vi_s_ibasisprodukt`) must be taken for a specific `Stichtag` (e.g., `20230101`). This snapshot represents the "legacy truth."
2.  **Legacy Job Execution:** The original `r_ausd_rechempf.ksh` job must be executed against the golden data snapshot in the Oracle environment for the chosen `Stichtag`. The resulting data in the Oracle target tables (`sof$ta_means_of_pay`, `sof$ta_bank`, `sof$ta_bank_verb`, `sof$ta_bank_zuord`, `sof$ta_p_rech_empf`, `sof$ta_p_d1_vpn`) will serve as the "legacy expected output."
3.  **BigQuery Source Data Ingestion:** The golden data snapshot from Oracle must be accurately ingested into the corresponding BigQuery source tables (`carmen_bpd.*`, `fos_source.*`, `dwh_view.*`, `isbert_schema.dwtk_meldungen`). Data types and values must match precisely.
4.  **BigQuery DDL Deployment:** All BigQuery DDLs for source and target tables must be deployed to the target GCP project.
5.  **Airflow DAG Deployment:** The `r_ausd_rechempf_dag.py` must be deployed to a Cloud Composer environment.
6.  **Migrated Job Execution:** The `r_ausd_rechempf_dag` in Airflow must be triggered with the same `Stichtag` parameter used for the legacy job.

**Testing Tools:**
*   **Pytest:** For orchestrating Python-based tests and interacting with BigQuery.
*   **Google Cloud BigQuery Client Library:** For querying BigQuery.
*   **SQL Developer/Toad (or similar):** For querying the legacy Oracle database.
*   **Data Comparison Tools:** For comparing large datasets (e.g., `pandas` in Python, `diff` utilities, or specialized data comparison tools).

---

## Test Cases

### 1. Output Parity: End-to-End Data Comparison for `sof_ta_means_of_pay`

*   **Purpose:** Verify that the `fos_snapshots.sof_ta_means_of_pay` table in BigQuery contains exactly the same data as the `sof$ta_means_of_pay` table in the legacy Oracle environment after a full job run. This validates the initial data loading and filtering logic for this table.
*   **Setup:**
    *   Complete General Test Setup.
    *   Ensure the `Stichtag` used for both legacy and migrated runs is `20230101`.
*   **Action:**
    1.  Execute the legacy `r_ausd_rechempf.ksh` job with `Stichtag=20230101`.
    2.  Execute the Airflow `r_ausd_rechempf_dag` with `stichtag=20230101`.
    3.  Extract all data from `sof$ta_means_of_pay` (Oracle) and `fos_snapshots.sof_ta_means_of_pay` (BigQuery).
*   **Pass/Fail Criterion:** The extracted datasets from Oracle and BigQuery must be identical in terms of row count, column values, and data types. Order of rows does not matter for this comparison.

    ```python
    # pytest_output_parity.py
    import pandas as pd
    from google.cloud import bigquery
    from your_oracle_connector import fetch_oracle_data # Placeholder for Oracle connection

    def test_sof_ta_means_of_pay_parity(stichtag="20230101"):
        bq_client = bigquery.Client()

        # 1. Fetch data from BigQuery
        bq_query = f"SELECT * FROM `your-gcp-project-id.fos_snapshots.sof_ta_means_of_pay` ORDER BY BP_ID, MEANS_OF_PAYMENT_ID"
        bq_df = bq_client.query(bq_query).to_dataframe()

        # 2. Fetch data from Legacy Oracle (assuming a function to get data)
        # This function would connect to Oracle and run a SELECT * query
        oracle_df = fetch_oracle_data("sof$ta_means_of_pay", stichtag).sort_values(by=['BP_ID', 'MEANS_OF_PAYMENT_ID'])

        # Standardize column names and types for comparison if necessary
        # (e.g., Oracle might return all caps, BigQuery might be mixed case)
        bq_df.columns = bq_df.columns.str.upper()
        oracle_df.columns = oracle_df.columns.str.upper()

        # Convert timestamps/dates to a common format (e.g., string or UTC datetime)
        for col in ['INSERT_AT', 'MODIFIED_AT', 'MANDATE_DATE', 'VALID_FROM', 'VALID_TO']:
            if col in bq_df.columns and col in oracle_df.columns:
                bq_df[col] = bq_df[col].astype(str)
                oracle_df[col] = oracle_df[col].astype(str)

        # 3. Compare DataFrames
        pd.testing.assert_frame_equal(bq_df, oracle_df, check_dtype=True, check_exact=False, rtol=1e-9, atol=1e-9)
        print(f"Data parity confirmed for sof_ta_means_of_pay for Stichtag {stichtag}")

    ```

### 2. Output Parity: End-to-End Data Comparison for `sof_ta_bank`

*   **Purpose:** Verify that the `fos_snapshots.sof_ta_bank` table in BigQuery contains exactly the same data as the `sof$ta_bank` table in the legacy Oracle environment. This validates the `UNION ALL` logic and filtering.
*   **Setup:** Same as Test Case 1.
*   **Action:** Same as Test Case 1, but for `sof_ta_bank`.
*   **Pass/Fail Criterion:** The extracted datasets from Oracle and BigQuery must be identical.

    ```python
    # pytest_output_parity.py (continued)
    def test_sof_ta_bank_parity(stichtag="20230101"):
        bq_client = bigquery.Client()
        bq_query = f"SELECT * FROM `your-gcp-project-id.fos_snapshots.sof_ta_bank` ORDER BY BANK_ID, INSERT_AT"
        bq_df = bq_client.query(bq_query).to_dataframe()
        oracle_df = fetch_oracle_data("sof$ta_bank", stichtag).sort_values(by=['BANK_ID', 'INSERT_AT'])

        bq_df.columns = bq_df.columns.str.upper()
        oracle_df.columns = oracle_df.columns.str.upper()

        for col in ['INSERT_AT', 'MODIFIED_AT']:
            if col in bq_df.columns and col in oracle_df.columns:
                bq_df[col] = bq_df[col].astype(str)
                oracle_df[col] = oracle_df[col].astype(str)

        pd.testing.assert_frame_equal(bq_df, oracle_df, check_dtype=True, check_exact=False, rtol=1e-9, atol=1e-9)
        print(f"Data parity confirmed for sof_ta_bank for Stichtag {stichtag}")
    ```

### 3. Output Parity: End-to-End Data Comparison for `sof_ta_bank_verb`

*   **Purpose:** Verify data parity for `sof_ta_bank_verb`, specifically validating the join condition (`ON mp.BANK_ID_ACC = ba.BANK_ID OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID`).
*   **Setup:** Same as Test Case 1.
*   **Action:** Same as Test Case 1, but for `sof_ta_bank_verb`.
*   **Pass/Fail Criterion:** The extracted datasets from Oracle and BigQuery must be identical.

### 4. Output Parity: End-to-End Data Comparison for `sof_ta_bank_zuord`

*   **Purpose:** Verify data parity for `sof_ta_bank_zuord`, validating the join with `sof_ta_e_regulierer`.
*   **Setup:** Same as Test Case 1.
*   **Action:** Same as Test Case 1, but for `sof_ta_bank_zuord`.
*   **Pass/Fail Criterion:** The extracted datasets from Oracle and BigQuery must be identical.

### 5. Output Parity: End-to-End Data Comparison for `sof_ta_p_rech_empf`

*   **Purpose:** Verify data parity for `sof_ta_p_rech_empf`, which involves the most complex `CASE` logic, multiple joins, and NULL handling. This is a critical test for transformation correctness.
*   **Setup:** Same as Test Case 1.
*   **Action:** Same as Test Case 1, but for `sof_ta_p_rech_empf`.
*   **Pass/Fail Criterion:** The extracted datasets from Oracle and BigQuery must be identical.

### 6. Output Parity: End-to-End Data Comparison for `sof_ta_p_d1_vpn`

*   **Purpose:** Verify data parity for `sof_ta_p_d1_vpn`, validating the filtering conditions (`vpn_id IS NOT NULL` and `basisprodukt_id IN (2828, 2831)`).
*   **Setup:** Same as Test Case 1.
*   **Action:** Same as Test Case 1, but for `sof_ta_p_d1_vpn`.
*   **Pass/Fail Criterion:** The extracted datasets from Oracle and BigQuery must be identical.

### 7. Transformation Correctness: `v_datum` Derivation

*   **Purpose:** Ensure the `v_datum` variable, which drives date-based filtering, is derived identically in BigQuery as it would be in the legacy KornShell/SQL*Plus environment.
*   **Setup:**
    *   Populate `isbert_schema.dwtk_meldungen` in BigQuery with a diverse set of `job_kennung` and `timecreated` values, including `BERT_DROP_TEMP_TABLE` entries, NULL `timecreated`, and future dates.
    *   Ensure the Oracle `dwtk_meldungen` table has identical data.
*   **Action:**
    1.  Manually determine the `v_datum` value from the Oracle `dwtk_meldungen` table using the original logic (MAX `timecreated` for `BERT_DROP_TEMP_TABLE`, then `IFNULL` to '19000101').
    2.  Execute the BigQuery SQL snippet that calculates `v_datum`.
*   **Pass/Fail Criterion:** The `v_datum` value calculated by BigQuery must exactly match the value derived from the Oracle source.

    ```sql
    -- BigQuery SQL to verify v_datum
    SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101') AS derived_v_datum
    FROM `your-gcp-project-id.isbert_schema.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```

### 8. Transformation Correctness: Date Filtering Logic

*   **Purpose:** Validate that the date filtering conditions (`DATE(col) <= PARSE_DATE(...) AND (col IS NULL OR DATE(col) > PARSE_DATE(...))`) behave identically for `insert_at`, `modified_at`, `valid_from`, `valid_to` columns.
*   **Setup:**
    *   Create a small, controlled dataset in `carmen_bpd.ta_means_of_payment` (and its Oracle equivalent) with various combinations of `insert_at`, `modified_at`, `valid_from`, `valid_to` dates relative to the `Stichtag` (e.g., before, on, after, NULL).
    *   Set `v_datum` to a known value (e.g., `20230101`).
*   **Action:**
    1.  Run the `INSERT INTO sof_ta_means_of_pay` statement in BigQuery against the controlled dataset.
    2.  Run the equivalent Oracle `INSERT` statement.
*   **Pass/Fail Criterion:** The number of rows and the specific rows inserted into `sof_ta_means_of_pay` must be identical in both environments.

### 9. Transformation Correctness: `sof_ta_bank` `UNION ALL` and Hardcoded `BANK_ID`

*   **Purpose:** Verify the correct behavior of the `UNION ALL` operation and the hardcoded `BANK_ID` (`-99999`) and `NULL` assignments for `BIC` and `BANK_INTERNATIONAL_ID` in the `sof_ta_bank` population.
*   **Setup:**
    *   Populate `carmen_bpd.ta_bank` and `carmen_bpd.ta_bank_international` with overlapping and distinct data, including cases where `BIC` or `BANK_INTERNATIONAL_ID` might be NULL in the source.
    *   Ensure identical data in Oracle `bpd$ta_bank` and `bpd$ta_bank_international`.
*   **Action:**
    1.  Execute the BigQuery `INSERT INTO sof_snapshots.sof_ta_bank` statement.
    2.  Execute the equivalent Oracle `INSERT` statement.
*   **Pass/Fail Criterion:** The resulting `sof_ta_bank` tables in BigQuery and Oracle must be identical, specifically checking the `BANK_ID` for rows originating from `ta_bank_international` and the `NULL` values for `BIC` and `BANK_INTERNATIONAL_ID` for rows from `ta_bank`.

### 10. Transformation Correctness: `sof_ta_bank_verb` Join Logic (OR condition)

*   **Purpose:** Validate the complex `OR` condition in the join for `sof_ta_bank_verb`: `ON mp.BANK_ID_ACC = ba.BANK_ID OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID`.
*   **Setup:**
    *   Create a controlled dataset in `sof_snapshots.sof_ta_means_of_pay` and `sof_snapshots.sof_ta_bank` (and their Oracle equivalents) with scenarios covering:
        *   Match on `BANK_ID_ACC` only.
        *   Match on `BANK_INTERNATIONAL_ID` only.
        *   Match on both.
        *   Match on neither.
        *   `NULL` values in one or both join keys.
*   **Action:**
    1.  Execute the BigQuery `INSERT INTO fos_snapshots.sof_ta_bank_verb` statement.
    2.  Execute the equivalent Oracle `INSERT` statement.
*   **Pass/Fail Criterion:** The resulting `sof_ta_bank_verb` tables in BigQuery and Oracle must be identical.

### 11. Transformation Correctness: `sof_ta_p_rech_empf` Complex `CASE` Logic

*   **Purpose:** Verify the intricate `CASE` statements used to derive `rechnungsempfaenger`, `akad_titel`, `firma`, `vorname`, `nachname`, and `strasse` columns.
*   **Setup:**
    *   Populate `fos_source.sof_ta_e_reach_re` and `fos_source.sof_ta_e_business_re` (and their Oracle equivalents) with data that exercises all branches of the `CASE` statements:
        *   `corp_unit` IS NULL, `organisation_name` IS NULL, `surname_s` IS NULL
        *   `corp_unit` IS NULL, `organisation_name` IS NULL, `surname_s` IS NOT NULL
        *   `corp_unit` IS NULL, `organisation_name` IS NOT NULL
        *   `corp_unit` IS NOT NULL
        *   `street` IS NULL, `pobox` IS NULL
        *   `street` IS NULL, `pobox` IS NOT NULL
        *   `street` IS NOT NULL
*   **Action:**
    1.  Execute the BigQuery `INSERT INTO fos_snapshots.sof_ta_p_rech_empf` statement.
    2.  Execute the equivalent Oracle `INSERT` statement.
*   **Pass/Fail Criterion:** The values for these specific columns in `sof_ta_p_rech_empf` must be identical between BigQuery and Oracle for all test cases.

### 12. Transformation Correctness: NULL Handling

*   **Purpose:** Verify that `NULL` values are handled consistently across all transformations, including `IFNULL`, `IS NULL` conditions, and `CASE` statements.
*   **Setup:**
    *   Ensure source data includes `NULL` values in columns that are part of transformations (e.g., `modified_at`, `valid_to`, `pobox`, `house_nr`, `surname_s`, `organisation_name`, `vpn_id`).
*   **Action:**
    1.  Run the full migrated job.
    2.  Run the full legacy job.
    3.  Query target tables for specific rows where `NULL` handling is critical.
*   **Pass/Fail Criterion:** The presence and absence of `NULL` values in the target tables must be identical between BigQuery and Oracle.

    ```sql
    -- Example BigQuery SQL to check NULLs in a specific column
    SELECT COUNT(*) FROM `your-gcp-project-id.fos_snapshots.sof_ta_p_rech_empf` WHERE BLZ IS NULL;
    -- Compare with Oracle: SELECT COUNT(*) FROM sof$ta_p_rech_empf WHERE BLZ IS NULL;
    ```

### 13. Transformation Correctness: `sof_ta_p_d1_vpn` Filtering

*   **Purpose:** Validate the `WHERE` clause for `sof_ta_p_d1_vpn`: `bp.vpn_id IS NOT NULL AND bp.basisprodukt_id IN (2828, 2831)`.
*   **Setup:**
    *   Populate `dwh_view.vi_s_ibasisprodukt` (and its Oracle equivalent) with data covering:
        *   `vpn_id` IS NULL
        *   `vpn_id` IS NOT NULL, `basisprodukt_id` = 2828
        *   `vpn_id` IS NOT NULL, `basisprodukt_id` = 2831
        *   `vpn_id` IS NOT NULL, `basisprodukt_id` = other_value
*   **Action:**
    1.  Execute the BigQuery `INSERT INTO fos_snapshots.sof_ta_p_d1_vpn` statement.
    2.  Execute the equivalent Oracle `INSERT` statement.
*   **Pass/Fail Criterion:** The resulting `sof_ta_p_d1_vpn` tables in BigQuery and Oracle must be identical.

### 14. Data Quality: Row Count Parity for All Target Tables

*   **Purpose:** Ensure that each target table in BigQuery has the exact same number of rows as its corresponding legacy Oracle table. This is a quick sanity check for overall data volume.
*   **Setup:** Complete General Test Setup.
*   **Action:**
    1.  Query `COUNT(*)` for each target table in BigQuery.
    2.  Query `COUNT(*)` for each target table in Oracle.
*   **Pass/Fail Criterion:** The row counts for each pair of tables (BigQuery vs. Oracle) must be identical.

    ```python
    # pytest_row_counts.py
    def test_row_counts_parity(stichtag="20230101"):
        bq_client = bigquery.Client()
        target_tables = [
            "sof_ta_means_of_pay", "sof_ta_bank", "sof_ta_bank_verb",
            "sof_ta_bank_zuord", "sof_ta_p_rech_empf", "sof_ta_p_d1_vpn"
        ]
        for table_name in target_tables:
            bq_count_query = f"SELECT COUNT(*) FROM `your-gcp-project-id.fos_snapshots.{table_name}`"
            bq_count = bq_client.query(bq_count_query).result().to_dataframe().iloc[0, 0]

            # Assuming a function to get Oracle row count
            oracle_count = fetch_oracle_row_count(f"sof${table_name}", stichtag)

            assert bq_count == oracle_count, \
                f"Row count mismatch for {table_name}: BigQuery={bq_count}, Oracle={oracle_count}"
            print(f"Row count for {table_name} matches: {bq_count}")
    ```

### 15. Data Quality: Schema and Data Type Validation

*   **Purpose:** Verify that the BigQuery target table schemas (column names, data types, NULLability) accurately reflect the legacy Oracle schemas and the migration design.
*   **Setup:** BigQuery DDLs deployed.
*   **Action:**
    1.  Extract schema information (column name, data type, NULLability) for all target tables from BigQuery's `INFORMATION_SCHEMA`.
    2.  Extract equivalent schema information from Oracle's `ALL_TAB_COLUMNS` or `USER_TAB_COLUMNS`.
    3.  Compare the extracted schemas.
*   **Pass/Fail Criterion:** Column names, BigQuery data types (e.g., `INT64` for Oracle `NUMBER`, `STRING` for `VARCHAR2`, `TIMESTAMP` for `DATE`/`TIMESTAMP`), and NULLability must be consistent with the migration design and functional requirements.

    ```sql
    -- BigQuery SQL to inspect schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your-gcp-project-id.fos_snapshots.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_means_of_pay'
    ORDER BY
        ordinal_position;
    ```

### 16. Data Quality: Uniqueness Constraints

*   **Purpose:** Verify that any implied uniqueness constraints from the legacy system are maintained in the migrated BigQuery tables. While BigQuery doesn't enforce primary keys, the data should still be unique where expected.
*   **Setup:** Complete General Test Setup.
*   **Action:**
    1.  Identify potential unique key combinations for each target table based on the legacy schema or business understanding.
    2.  Run queries in BigQuery to check for duplicate rows based on these key combinations.
*   **Pass/Fail Criterion:** No duplicate rows should be found in BigQuery for identified unique key combinations.

    ```sql
    -- BigQuery SQL to check for duplicates in sof_ta_means_of_pay (assuming BP_ID, MEANS_OF_PAYMENT_ID is unique)
    SELECT
        BP_ID,
        MEANS_OF_PAYMENT_ID,
        COUNT(*)
    FROM
        `your-gcp-project-id.fos_snapshots.sof_ta_means_of_pay`
    GROUP BY
        BP_ID,
        MEANS_OF_PAYMENT_ID
    HAVING
        COUNT(*) > 1;
    ```

### 17. External System Replacements: Airflow DAG Parameter Passing (`Stichtag`)

*   **Purpose:** Verify that the `Stichtag` parameter is correctly passed from the Airflow DAG to the BigQuery SQL script and used in the `v_datum` derivation.
*   **Setup:**
    *   Modify the `dwtk_meldungen` table to have a `timecreated` value that would result in a specific `v_datum` if the `Stichtag` parameter was NOT used, and another `timecreated` that would be overridden by the `Stichtag` if it was used.
    *   Run the DAG with a specific `Stichtag` (e.g., `20230115`).
*   **Action:**
    1.  Inspect Airflow task logs for the `execute_bigquery_transformation` task to see the `v_datum` value used.
    2.  Query the BigQuery target tables and verify that the data reflects filtering based on the *passed* `Stichtag` and not the `dwtk_meldungen` derived `v_datum` (if `Stichtag` overrides it, which it does in the legacy script).
    *Self-correction: The design document states "The DAG will handle the `Stichtag` and `Wiederanlaufwert` parameters... The `v_datum` derivation from `isbert_schema.dwtk_meldungen` will be translated into a BigQuery SQL query executed by Airflow." This implies `v_datum` is derived *within* the SQL, and the `Stichtag` from the DAG is a separate parameter. The original ksh script *sets* `p_stichtag` based on `sysdate` or `maxladedatum` if not provided. The BigQuery SQL uses `v_datum` derived from `dwtk_meldungen`. This is a potential discrepancy. The test should clarify this.*

    **Revised Purpose:** Verify that the `v_datum` variable used in the BigQuery SQL is correctly derived from `isbert_schema.dwtk_meldungen` as specified in the BigQuery SQL, and that the Airflow DAG's `stichtag` parameter (if used) does not implicitly override this internal `v_datum` calculation unless explicitly designed to. The current BigQuery SQL *always* derives `v_datum` from `dwtk_meldungen`. The Airflow DAG's `params` for `stichtag` is currently unused in the provided `d_ausd_rechempf_bq.sql`. This is a **risk/unresolved item** that needs clarification in the design. For now, I will test the *current* BigQuery SQL's `v_datum` derivation.

*   **Revised Action:**
    1.  Insert a specific `timecreated` for `BERT_DROP_TEMP_TABLE` into `isbert_schema.dwtk_meldungen` (e.g., `2023-01-10 10:00:00 UTC`).
    2.  Run the Airflow DAG.
    3.  After the DAG completes, query one of the target tables (e.g., `sof_ta_means_of_pay`) and check the `INSERT_AT` and `VALID_FROM` filters.
*   **Pass/Fail Criterion:** The filtering in the BigQuery SQL should correctly use `PARSE_DATE('%Y%m%d', v_datum)` where `v_datum` is `20230110` (from `dwtk_meldungen`). If the DAG's `stichtag` parameter is intended to override this, the BigQuery SQL needs modification, and this test would change.

### 18. External System Replacements: Airflow DAG Execution Order and Idempotency

*   **Purpose:** Verify that the Airflow DAG executes the BigQuery SQL steps in the correct order (truncation then inserts) and that running the DAG multiple times produces the same final state (idempotency, assuming source data doesn't change).
*   **Setup:**
    *   Populate BigQuery source tables.
    *   Run the DAG once.
*   **Action:**
    1.  After the first successful DAG run, check the row counts and a sample of data in the target tables.
    2.  Run the DAG a second time immediately without changing source data.
    3.  Check row counts and data again.
*   **Pass/Fail Criterion:**
    *   The Airflow task logs must show the `TRUNCATE` statements executing before the `INSERT` statements.
    *   The row counts and data in the target tables must be identical after the first and second runs.

### 19. External System Replacements: Error Handling and Logging Parity

*   **Purpose:** Verify that Airflow's logging and error handling mechanisms provide comparable visibility and robustness to the legacy KornShell scripts (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`, `trap`).
*   **Setup:**
    *   Introduce a controlled error condition in the BigQuery SQL (e.g., attempt to insert a string into an `INT64` column, or reference a non-existent table).
*   **Action:**
    1.  Run the Airflow DAG with the error condition.
    2.  Observe Airflow task logs, DAG status, and any configured alerts/notifications.
    3.  Compare with how the legacy job would log and report the same error.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG must fail gracefully, marking the relevant task as failed.
    *   Error messages in Airflow logs should be clear and actionable, indicating the BigQuery error.
    *   Configured Airflow alerts (if any) should trigger.
    *   The level of detail and clarity of error reporting should be comparable to the legacy system.

### 20. External System Replacements: Source Data Ingestion Parity (Oracle to BigQuery)

*   **Purpose:** This is a prerequisite test, but crucial. Verify that the data ingested from Oracle into the BigQuery source tables (`carmen_bpd.*`, `fos_source.*`, `dwh_view.*`, `isbert_schema.dwtk_meldungen`) is an exact, byte-for-byte (or logically equivalent) copy of the Oracle source data for the given `Stichtag`.
*   **Setup:**
    *   Choose a `Stichtag`.
    *   Run the data ingestion pipeline (e.g., Dataflow job) to load Oracle data into BigQuery source tables.
*   **Action:**
    1.  For each source table, extract all data from Oracle.
    2.  For each source table, extract all data from its corresponding BigQuery table.
    3.  Compare the datasets.
*   **Pass/Fail Criterion:** All source tables in BigQuery must be identical to their Oracle counterparts for the chosen `Stichtag` snapshot. This includes row counts, column values, and data types (after appropriate BigQuery type mapping). Any discrepancies here will invalidate all subsequent transformation tests.

    ```python
    # pytest_source_ingestion_parity.py
    def test_source_table_ingestion_parity(table_name, oracle_table_name, stichtag="20230101"):
        bq_client = bigquery.Client()
        bq_query = f"SELECT * FROM `your-gcp-project-id.carmen_bpd.{table_name}` ORDER BY 1" # Order by first column
        bq_df = bq_client.query(bq_query).to_dataframe()
        oracle_df = fetch_oracle_data(oracle_table_name, stichtag).sort_values(by=oracle_df.columns[0])

        # ... (similar type and column name standardization as in output parity tests) ...

        pd.testing.assert_frame_equal(bq_df, oracle_df, check_dtype=True, check_exact=False, rtol=1e-9, atol=1e-9)
        print(f"Source data ingestion parity confirmed for {table_name}")

    # Example usage in pytest
    def test_all_carmen_bpd_sources():
        test_source_table_ingestion_parity("ta_means_of_payment", "bpd$ta_means_of_payment")
        test_source_table_ingestion_parity("ta_bank", "bpd$ta_bank")
        test_source_table_ingestion_parity("ta_bank_international", "bpd$ta_bank_international")
        # Add other source tables as needed
    ```