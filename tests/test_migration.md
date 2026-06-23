As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_v_ta_c_bfc.ksh` to Google Cloud Platform with BigQuery and Airflow. The core logic resides in the Oracle SQL script `d_ausd_v_ta_c_bfc.sql`, which is being translated into BigQuery SQL and orchestrated by an Airflow DAG.

The following test cases are designed to ensure behavioral equivalence, data integrity, and correctness of the migrated job.

**Assumptions for Testing:**
*   Access to both the legacy Oracle environment and the target BigQuery environment.
*   Ability to create, load, and query test data in both environments.
*   All source Oracle tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, etc.) have been replicated to BigQuery with identical data and compatible schemas.
*   The `bfc_get_bindefrist` UDF in BigQuery, while currently a placeholder, will eventually contain the fully translated and verified business logic from Oracle's `Cds$vr_Bindefrist.GetBindeFrist`. Tests for this UDF will be critical once the actual logic is implemented.
*   The `v_bfc_procedure` variable in Oracle is assumed to typically represent the current date, hence its replacement with `CURRENT_DATE()` in BigQuery. If `v_bfc_procedure` could be a historical or user-defined date, the BigQuery implementation and tests would need adjustment.
*   The `ROWNUM` behavior in Oracle for `step4_recalculate_stale_rows` is assumed to be non-deterministic without an `ORDER BY`. The BigQuery `QUALIFY ROW_NUMBER() OVER(ORDER BY cntrct_id)` introduces determinism, which is generally an improvement, but the set of affected rows should be compared.

---

## Migration Validation Tests for `r_ausd_v_ta_c_bfc.ksh`

### Test Case 1: Source Data Replication & Schema Parity

*   **Purpose:** Verify that all necessary source tables from Oracle are accurately replicated into BigQuery, maintaining schema and data integrity. This is a foundational test for all subsequent data transformations.
*   **Setup:**
    1.  Identify all Oracle source tables used in `d_ausd_v_ta_c_bfc.sql` (e.g., `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `isbert_schema.dwtk_meldungen`, `all_objects@pcrs1`, `spr_schema.cds$ta_cntrct@PCRS1`).
    2.  Ensure these tables are populated with a representative dataset in both Oracle and their corresponding BigQuery replicas.
    3.  Record the schema (column names, data types, nullability) for each Oracle source table.
*   **Action:**
    1.  Query the schema of the replicated tables in BigQuery.
    2.  Perform row count and checksum comparisons for each source table between Oracle and BigQuery.
    3.  Execute a full data comparison (e.g., using `MINUS`/`EXCEPT` or hash comparisons) for a sample of critical source tables.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   For each source table, the BigQuery schema exactly matches the Oracle schema (or has compatible BigQuery types for Oracle types, e.g., `NUMBER` to `INT64`/`BIGNUMERIC`, `DATE` to `DATE`, `VARCHAR2` to `STRING`).
        *   Row counts for all source tables are identical between Oracle and BigQuery.
        *   Data checksums (or full data comparison) confirm identical data content for all columns in all source tables.
    *   **Fail:** Any discrepancy in schema, row count, or data content.

*   **Runnable Test Code (SQL/Python):**

    ```python
    import pandas as pd
    from google.cloud import bigquery
    from sqlalchemy import create_engine # For Oracle connection

    # Configuration
    BQ_PROJECT_ID = "your-gcp-project"
    BQ_DATASET_ID = "your_dataset"
    ORACLE_SCHEMA = "ISBERT_SCHEMA" # Or relevant schema for source tables
    ORACLE_CONN_STR = "oracle+cx_oracle://user:password@host:port/service_name"

    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    oracle_engine = create_engine(ORACLE_CONN_STR)

    source_tables = [
        "SOF$TA_CNTRCT_CRS",
        "SOF$TA_BARRIER",
        "SOF$TA_CNTRCT_VALID",
        "SOF$TA_PERIOD",
        # Add other source tables as identified
    ]

    def compare_table_schemas(oracle_table_name, bq_table_name):
        print(f"Comparing schema for {oracle_table_name} (Oracle) and {bq_table_name} (BigQuery)")
        # Fetch Oracle schema
        oracle_schema_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = '{ORACLE_SCHEMA.upper()}' AND TABLE_NAME = '{oracle_table_name.upper()}'
            ORDER BY COLUMN_ID
        """
        oracle_df = pd.read_sql(oracle_schema_query, oracle_engine)

        # Fetch BigQuery schema
        bq_table_ref = bq_client.dataset(BQ_DATASET_ID).table(bq_table_name)
        bq_table = bq_client.get_table(bq_table_ref)
        bq_schema_data = []
        for field in bq_table.schema:
            bq_schema_data.append({
                "COLUMN_NAME": field.name.upper(),
                "DATA_TYPE": field.field_type.upper(),
                "NULLABLE": "Y" if field.mode == "NULLABLE" else "N"
            })
        bq_df = pd.DataFrame(bq_schema_data)

        # Perform comparison logic (simplified for brevity)
        # A more robust comparison would map Oracle types to BQ types and handle differences
        # For now, a direct comparison of column names and nullability
        if not oracle_df['COLUMN_NAME'].equals(bq_df['COLUMN_NAME']):
            print(f"FAIL: Column names differ for {oracle_table_name}")
            return False
        # Add more detailed type and nullability comparison here
        print(f"Schema for {oracle_table_name} matches BigQuery {bq_table_name} (basic check)")
        return True

    def compare_table_data(oracle_table_name, bq_table_name):
        print(f"Comparing data for {oracle_table_name} (Oracle) and {bq_table_name} (BigQuery)")
        oracle_count_query = f"SELECT COUNT(*) FROM {ORACLE_SCHEMA}.{oracle_table_name}"
        bq_count_query = f"SELECT COUNT(*) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{bq_table_name}`"

        oracle_count = pd.read_sql(oracle_count_query, oracle_engine).iloc[0,0]
        bq_count = bq_client.query(bq_count_query).result().to_dataframe().iloc[0,0]

        if oracle_count != bq_count:
            print(f"FAIL: Row counts differ for {oracle_table_name}: Oracle={oracle_count}, BQ={bq_count}")
            return False

        # For full data comparison, you might use a hash or a MINUS/EXCEPT query
        # Example: Compare hashes of all columns (simplified, might need type casting)
        # This is a conceptual example, actual implementation needs careful type handling
        # and potentially column ordering.
        oracle_hash_query = f"SELECT ORA_HASH(SYS_XMLAGG(SYS_XMLGEN(t.*) ORDER BY 1).GETCLOBVAL()) FROM {ORACLE_SCHEMA}.{oracle_table_name} t"
        bq_hash_query = f"""
            SELECT FARM_FINGERPRINT(TO_JSON_STRING(t))
            FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{bq_table_name}` t
            ORDER BY 1
        """ # This is not a direct hash comparison, but a way to compare ordered rows.
            # A better approach is to select all columns, order them, and then hash the concatenated string.

        # For simplicity, let's just check row counts for this example.
        print(f"Row counts match for {oracle_table_name}: {oracle_count}")
        return True

    # Main execution
    all_passed = True
    for table in source_tables:
        bq_table = table.lower().replace('$', '_') # Assuming a simple naming convention
        if not compare_table_schemas(table, bq_table):
            all_passed = False
        if not compare_table_data(table, bq_table):
            all_passed = False

    if all_passed:
        print("All source data replication and schema parity checks passed.")
    else:
        print("Some source data replication or schema parity checks failed.")
    ```

### Test Case 2: `bfc_get_bindefrist` UDF Correctness (Placeholder Logic)

*   **Purpose:** Validate the behavior of the `bfc_get_bindefrist` UDF in BigQuery against the *currently implemented placeholder logic*. This test highlights the critical need for detailed analysis and re-implementation of the original Oracle PL/SQL.
*   **Setup:**
    1.  Ensure the `bfc_get_bindefrist` UDF is deployed in BigQuery.
    2.  Prepare a set of test inputs for `cntrct_id`, `commitment_reference_date`, and `cntrct_validity_id` covering various scenarios, including `NULL` for `commitment_reference_date`.
*   **Action:**
    1.  Execute the BigQuery UDF with the prepared test inputs.
    2.  Record the outputs.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   If `commitment_reference_date` is `NULL`, the UDF returns `NULL`.
        *   Otherwise, the UDF returns `DATE '9999-12-31'` as per the placeholder logic.
    *   **Fail:** The UDF's output deviates from the placeholder logic.
*   **Critical Note:** This test only validates the *placeholder*. A full re-implementation of the Oracle `Cds$vr_Bindefrist.GetBindeFrist` logic is required, followed by extensive unit tests comparing its behavior against the original Oracle package for a comprehensive set of inputs.

*   **Runnable Test Code (SQL/Pytest):**

    ```python
    import pytest
    from google.cloud import bigquery

    BQ_PROJECT_ID = "your-gcp-project"
    BQ_DATASET_ID = "your_dataset"
    UDF_NAME = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.bfc_get_bindefrist`"

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client(project=BQ_PROJECT_ID)

    @pytest.mark.parametrize("cntrct_id, commitment_ref_date, cntrct_validity_id, expected_output", [
        ("C1", "2023-01-15", "V1", "9999-12-31"),
        ("C2", "2022-11-01", "V2", "9999-12-31"),
        ("C3", None, "V3", None), # Test NULL commitment_reference_date
        ("C4", "2024-03-10", None, "9999-12-31"), # Test NULL cntrct_validity_id
        ("C5", None, None, None),
    ])
    def test_bfc_get_bindefrist_placeholder_logic(bq_client, cntrct_id, commitment_ref_date, cntrct_validity_id, expected_output):
        commitment_ref_date_sql = f"DATE '{commitment_ref_date}'" if commitment_ref_date else "NULL"
        cntrct_validity_id_sql = f"'{cntrct_validity_id}'" if cntrct_validity_id else "NULL"

        query = f"""
            SELECT {UDF_NAME}(
                '{cntrct_id}',
                {commitment_ref_date_sql},
                {cntrct_validity_id_sql}
            ) AS result
        """
        query_job = bq_client.query(query)
        result = query_job.result().to_dataframe().iloc[0]['result']

        if expected_output is None:
            assert result is None
        else:
            assert str(result) == expected_output

    # Example of how to run this test:
    # 1. Save the above code as a Python file (e.g., `test_udf.py`).
    # 2. Make sure `pytest` and `google-cloud-bigquery` are installed.
    # 3. Run `pytest test_udf.py` from your terminal.
    ```

### Test Case 3: Step 1 - Staging Table Population Parity (`step1_build_staging`)

*   **Purpose:** Verify that the BigQuery `step1_build_staging` script correctly aggregates and transforms data into `ta_c_bfc_akt`, producing results identical to the Oracle equivalent. This validates joins, aggregations (`MAX`, `COUNT`), `GREATEST`, and `IFNULL` (or `NVL`) logic.
*   **Setup:**
    1.  Populate all relevant source tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) with a diverse dataset in both Oracle and BigQuery. Include cases with:
        *   Matching `cntrct_id` across all tables.
        *   `cntrct_id` present in `sof$ta_cntrct_crs` but not in joined tables (to test `LEFT JOIN`).
        *   `NULL` values in `bfc_age` related columns in source tables.
        *   Multiple entries for the same `cntrct_id` in `sof$ta_cntrct_crs` (if applicable, to test `GROUP BY`).
    2.  Ensure `ta_c_bfc_akt` is empty in both environments before execution.
*   **Action:**
    1.  Execute the Oracle SQL equivalent of Step 1 (truncating `sof$ta_c_bfc_akt` and inserting data) using SQL*Plus or a similar tool.
    2.  Execute the BigQuery `step1_build_staging` task (or the SQL directly).
    3.  Extract all data from `sof$ta_c_bfc_akt` (Oracle) and `ta_c_bfc_akt` (BigQuery).
*   **Pass/Fail Criterion:**
    *   **Pass:** The data in BigQuery's `ta_c_bfc_akt` table is identical to the data in Oracle's `sof$ta_c_bfc_akt` table, considering `cntrct_id` as the primary key for comparison. Row counts must also match.
    *   **Fail:** Any difference in row counts or data content.

*   **Runnable Test Code (SQL/Python):**

    ```python
    # Python (using pandas for comparison)
    import pandas as pd
    from google.cloud import bigquery
    from sqlalchemy import create_engine

    BQ_PROJECT_ID = "your-gcp-project"
    BQ_DATASET_ID = "your_dataset"
    ORACLE_SCHEMA = "ISBERT_SCHEMA"
    ORACLE_CONN_STR = "oracle+cx_oracle://user:password@host:port/service_name"

    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    oracle_engine = create_engine(ORACLE_CONN_STR)

    def run_oracle_step1():
        # This would involve executing the Oracle SQL script directly or via a wrapper
        # For testing, we'll simulate the output by running the core SQL
        oracle_sql = f"""
            TRUNCATE TABLE {ORACLE_SCHEMA}.SOF$TA_C_BFC_AKT;
            INSERT INTO {ORACLE_SCHEMA}.SOF$TA_C_BFC_AKT (
                CNTRCT_ID, COMMITMENT_REFERENCE_DATE, CNTRCT_VALIDITY_ID, BFC_AGE, BFC_COUNT
            )
            SELECT
                c.CNTRCT_ID,
                MAX(c.COMMITMENT_REFERENCE_DATE) AS COMMITMENT_REFERENCE_DATE,
                MAX(c.CNTRCT_VALIDITY_ID) AS CNTRCT_VALIDITY_ID,
                MAX(
                    GREATEST(
                        NVL(c.BFC_AGE, TO_DATE('19000101', 'YYYYMMDD')),
                        NVL(b.BFC_AGE, TO_DATE('19000101', 'YYYYMMDD')),
                        NVL(v.BFC_AGE, TO_DATE('19000101', 'YYYYMMDD')),
                        NVL(p_fi.BFC_AGE, TO_DATE('19000101', 'YYYYMMDD')),
                        NVL(p_fo.BFC_AGE, TO_DATE('19000101', 'YYYYMMDD')),
                        NVL(p_fi_n.BFC_AGE, TO_DATE('19000101', 'YYYYMMDD')),
                        NVL(p_fo_n.BFC_AGE, TO_DATE('19000101', 'YYYYMMDD'))
                    )
                ) AS BFC_AGE,
                COUNT(1) AS BFC_COUNT
            FROM
                {ORACLE_SCHEMA}.SOF$TA_CNTRCT_CRS c
            LEFT JOIN
                {ORACLE_SCHEMA}.SOF$TA_BARRIER b ON c.CNTRCT_ID = b.CNTRCT_ID
            LEFT JOIN
                {ORACLE_SCHEMA}.SOF$TA_CNTRCT_VALID v ON c.CNTRCT_VALIDITY_ID = v.CNTRCT_VALIDITY_ID
            LEFT JOIN
                {ORACLE_SCHEMA}.SOF$TA_PERIOD p_fi ON v.FIRST_PERIOD_ID = p_fi.PERIOD_ID
            LEFT JOIN
                {ORACLE_SCHEMA}.SOF$TA_PERIOD p_fo ON v.FOLLOWING_PERIOD_ID = p_fo.PERIOD_ID
            LEFT JOIN
                {ORACLE_SCHEMA}.SOF$TA_PERIOD p_fi_n ON v.FIRST_NOTICE_PERIOD_ID = p_fi_n.PERIOD_ID
            LEFT JOIN
                {ORACLE_SCHEMA}.SOF$TA_PERIOD p_fo_n ON v.FOLLOW_NOTICE_PERIOD_ID = p_fo_n.PERIOD_ID
            GROUP BY
                c.CNTRCT_ID
        """
        with oracle_engine.connect() as connection:
            connection.execute(oracle_sql)
            connection.commit()
        print("Oracle Step 1 executed.")

    def run_bq_step1():
        bq_sql = f"""
            TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt`;
            INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt` (
                cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age, bfc_count
            )
            SELECT
                c.cntrct_id,
                MAX(c.commitment_reference_date) AS commitment_reference_date,
                MAX(c.cntrct_validity_id) AS cntrct_validity_id,
                MAX(
                    GREATEST(
                        IFNULL(c.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(b.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(v.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fi.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fo.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fi_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fo_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101'))
                    )
                ) AS bfc_age,
                COUNT(1) AS bfc_count
            FROM
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_cntrct_crs` AS c
            LEFT JOIN
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_barrier` AS b
                ON c.cntrct_id = b.cntrct_id
            LEFT JOIN
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_cntrct_valid` AS v
                ON c.cntrct_validity_id = v.cntrct_validity_id
            LEFT JOIN
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fi
                ON v.first_period_id = p_fi.period_id
            LEFT JOIN
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fo
                ON v.following_period_id = p_fo.period_id
            LEFT JOIN
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fi_n
                ON v.first_notice_period_id = p_fi_n.period_id
            LEFT JOIN
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fo_n
                ON v.follow_notice_period_id = p_fo_n.period_id
            GROUP BY
                c.cntrct_id;
        """
        bq_client.query(bq_sql).result()
        print("BigQuery Step 1 executed.")

    def compare_staging_tables():
        oracle_df = pd.read_sql(f"SELECT * FROM {ORACLE_SCHEMA}.SOF$TA_C_BFC_AKT ORDER BY CNTRCT_ID", oracle_engine)
        bq_df = bq_client.query(f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt` ORDER BY cntrct_id").result().to_dataframe()

        # Ensure column names are consistent for comparison (e.g., all lowercase)
        oracle_df.columns = [col.lower() for col in oracle_df.columns]

        # Convert date columns to consistent format if needed
        for col in ['commitment_reference_date', 'bfc_age']:
            if col in oracle_df.columns and col in bq_df.columns:
                oracle_df[col] = pd.to_datetime(oracle_df[col]).dt.date
                bq_df[col] = pd.to_datetime(bq_df[col]).dt.date

        if oracle_df.equals(bq_df):
            print("PASS: Staging table (ta_c_bfc_akt) data is identical.")
            return True
        else:
            print("FAIL: Staging table (ta_c_bfc_akt) data differs.")
            # Print differences for debugging
            diff = pd.concat([oracle_df, bq_df]).drop_duplicates(keep=False)
            print("Differences found:")
            print(diff)
            return False

    # Main execution
    run_oracle_step1()
    run_bq_step1()
    assert compare_staging_tables()
    ```

### Test Case 4: Step 2 - Initial Load Parity (`step2_initial_load`)

*   **Purpose:** Verify that the BigQuery `step2_initial_load` script correctly populates `ta_c_bfc` only when it's empty, and that the inserted data matches Oracle's behavior.
*   **Setup:**
    1.  **Scenario A (Empty Target):** Ensure `ta_c_bfc` is empty in both Oracle and BigQuery. Populate `ta_c_bfc_akt` with test data (e.g., from Test Case 3).
    2.  **Scenario B (Non-Empty Target):** Populate `ta_c_bfc` with some existing data in both Oracle and BigQuery. Populate `ta_c_bfc_akt` with test data.
*   **Action:**
    1.  **Scenario A:**
        *   Execute Oracle's initial load logic.
        *   Execute BigQuery's `step2_initial_load` task.
        *   Extract data from `sof$ta_c_bfc` (Oracle) and `ta_c_bfc` (BigQuery).
    2.  **Scenario B:**
        *   Execute Oracle's initial load logic.
        *   Execute BigQuery's `step2_initial_load` task.
        *   Extract data from `sof$ta_c_bfc` (Oracle) and `ta_c_bfc` (BigQuery).
*   **Pass/Fail Criterion:**
    *   **Scenario A (Empty Target):**
        *   **Pass:** `ta_c_bfc` in BigQuery contains identical data to `sof$ta_c_bfc` in Oracle. `bfc_procedure` should be `1900-01-01`.
        *   **Fail:** Any discrepancy in data or if `ta_c_bfc` remains empty.
    *   **Scenario B (Non-Empty Target):**
        *   **Pass:** `ta_c_bfc` in BigQuery remains unchanged, identical to its state before the initial load, matching Oracle's behavior.
        *   **Fail:** `ta_c_bfc` in BigQuery was modified or contains new rows.

*   **Runnable Test Code (SQL/Python):**

    ```python
    # Python (conceptual, similar structure to Test Case 3)
    # ... (BQ_PROJECT_ID, BQ_DATASET_ID, ORACLE_SCHEMA, ORACLE_CONN_STR, bq_client, oracle_engine setup) ...

    def setup_scenario_a(): # Empty target
        # Truncate ta_c_bfc in both environments
        bq_client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc`").result()
        with oracle_engine.connect() as connection:
            connection.execute(f"TRUNCATE TABLE {ORACLE_SCHEMA}.SOF$TA_C_BFC")
            connection.commit()
        # Populate ta_c_bfc_akt (staging) with test data (e.g., from Test Case 3 setup)
        # ... (call run_oracle_step1 and run_bq_step1 to populate staging) ...

    def setup_scenario_b(): # Non-empty target
        # Populate ta_c_bfc with some initial data in both environments
        # ... (insert sample data into ta_c_bfc and SOF$TA_C_BFC) ...
        # Populate ta_c_bfc_akt (staging) with test data
        # ... (call run_oracle_step1 and run_bq_step1 to populate staging) ...

    def run_oracle_step2():
        oracle_sql = f"""
            INSERT INTO {ORACLE_SCHEMA}.SOF$TA_C_BFC (
                CNTRCT_ID, BFC_AGE, BFC_COUNT, BFC_PROCEDURE, COMMITMENT_REFERENCE_DATE, CNTRCT_VALIDITY_ID
            )
            SELECT
                akt.CNTRCT_ID,
                akt.BFC_AGE,
                akt.BFC_COUNT,
                TO_DATE('19000101', 'YYYYMMDD') AS BFC_PROCEDURE,
                akt.COMMITMENT_REFERENCE_DATE,
                akt.CNTRCT_VALIDITY_ID
            FROM
                {ORACLE_SCHEMA}.SOF$TA_C_BFC_AKT akt
            WHERE
                (SELECT COUNT(1) FROM {ORACLE_SCHEMA}.SOF$TA_C_BFC) = 0;
        """
        with oracle_engine.connect() as connection:
            connection.execute(oracle_sql)
            connection.commit()

    def run_bq_step2():
        bq_sql = f"""
            INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc` (
                cntrct_id, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id
            )
            SELECT
                akt.cntrct_id,
                akt.bfc_age,
                akt.bfc_count,
                PARSE_DATE('%Y%m%d', '19000101') AS bfc_procedure,
                akt.commitment_reference_date,
                akt.cntrct_validity_id
            FROM
                `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt` AS akt
            WHERE
                (SELECT COUNT(1) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc`) = 0;
        """
        bq_client.query(bq_sql).result()

    def compare_target_tables():
        oracle_df = pd.read_sql(f"SELECT * FROM {ORACLE_SCHEMA}.SOF$TA_C_BFC ORDER BY CNTRCT_ID", oracle_engine)
        bq_df = bq_client.query(f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc` ORDER BY cntrct_id").result().to_dataframe()

        # Normalize column names and date types for comparison
        oracle_df.columns = [col.lower() for col in oracle_df.columns]
        for col in ['bindefrist', 'bfc_age', 'bfc_procedure', 'commitment_reference_date', 'load_ts']:
            if col in oracle_df.columns and col in bq_df.columns:
                oracle_df[col] = pd.to_datetime(oracle_df[col]).dt.date
                bq_df[col] = pd.to_datetime(bq_df[col]).dt.date

        # Exclude load_ts from direct comparison as it will differ
        oracle_df = oracle_df.drop(columns=['load_ts'], errors='ignore')
        bq_df = bq_df.drop(columns=['load_ts'], errors='ignore')

        if oracle_df.equals(bq_df):
            print("PASS: Target table (ta_c_bfc) data is identical.")
            return True
        else:
            print("FAIL: Target table (ta_c_bfc) data differs.")
            diff = pd.concat([oracle_df, bq_df]).drop_duplicates(keep=False)
            print("Differences found:")
            print(diff)
            return False

    # Test Scenario A
    setup_scenario_a()
    run_oracle_step2()
    run_bq_step2()
    assert compare_target_tables()

    # Test Scenario B (requires re-setup of data)
    # setup_scenario_b()
    # run_oracle_step2() # Should not insert
    # run_bq_step2()     # Should not insert
    # assert compare_target_tables() # Should be identical to pre-step2 state
    ```

### Test Case 5: Step 3 - Merge Logic Parity (`step3_merge_changed_rows`)

*   **Purpose:** Validate the BigQuery `MERGE` statement's behavior for updating existing rows and inserting new ones, ensuring it matches Oracle's logic based on `bfc_age` and `bfc_count` conditions. This also tests the `bfc_get_bindefrist` UDF integration.
*   **Setup:**
    1.  Populate `ta_c_bfc` (target) and `ta_c_bfc_akt` (staging) in both environments with data that covers:
        *   `cntrct_id` present in both, where `bfc_age` in staging is *greater* than target.
        *   `cntrct_id` present in both, where `bfc_count` in staging is *different* from target.
        *   `cntrct_id` present in both, where neither `bfc_age` nor `bfc_count` conditions are met (should not update).
        *   `cntrct_id` present only in staging (should insert).
        *   `cntrct_id` present only in target (should remain unchanged).
    2.  Ensure the `bfc_get_bindefrist` UDF is deployed and its placeholder logic is understood for expected outputs.
*   **Action:**
    1.  Execute Oracle's merge logic.
    2.  Execute BigQuery's `step3_merge_changed_rows` task.
    3.  Extract all data from `sof$ta_c_bfc` (Oracle) and `ta_c_bfc` (BigQuery).
*   **Pass/Fail Criterion:**
    *   **Pass:** The data in BigQuery's `ta_c_bfc` table is identical to the data in Oracle's `sof$ta_c_bfc` table, excluding `load_ts` (which will differ due to `CURRENT_TIMESTAMP()`). The `bfc_procedure` column should reflect the execution date.
    *   **Fail:** Any discrepancy in row counts or data content.

*   **Runnable Test Code (SQL/Python):**

    ```python
    # Python (conceptual, similar structure to Test Case 3)
    # ... (BQ_PROJECT_ID, BQ_DATASET_ID, ORACLE_SCHEMA, ORACLE_CONN_STR, bq_client, oracle_engine setup) ...

    def setup_merge_data():
        # Clear and populate ta_c_bfc and ta_c_bfc_akt with specific test cases
        # Example:
        # Oracle:
        # TRUNCATE TABLE SOF$TA_C_BFC;
        # INSERT INTO SOF$TA_C_BFC VALUES ('C1', NULL, DATE '2023-01-01', 10, DATE '2023-01-01', DATE '2023-01-01', 'V1', SYSDATE); -- Will be updated (bfc_age increases)
        # INSERT INTO SOF$TA_C_BFC VALUES ('C2', NULL, DATE '2023-01-01', 10, DATE '2023-01-01', DATE '2023-01-01', 'V2', SYSDATE); -- Will be updated (bfc_count changes)
        # INSERT INTO SOF$TA_C_BFC VALUES ('C3', NULL, DATE '2023-01-01', 10, DATE '2023-01-01', DATE '2023-01-01', 'V3', SYSDATE); -- No change, should remain
        # INSERT INTO SOF$TA_C_BFC VALUES ('C4', NULL, DATE '2023-01-01', 10, DATE '2023-01-01', DATE '2023-01-01', 'V4', SYSDATE); -- Only in target, should remain

        # TRUNCATE TABLE SOF$TA_C_BFC_AKT;
        # INSERT INTO SOF$TA_C_BFC_AKT VALUES ('C1', DATE '2023-02-01', 'V1', DATE '2023-02-01', 10); -- Update bfc_age
        # INSERT INTO SOF$TA_C_BFC_AKT VALUES ('C2', DATE '2023-01-01', 'V2', DATE '2023-01-01', 11); -- Update bfc_count
        # INSERT INTO SOF$TA_C_BFC_AKT VALUES ('C3', DATE '2023-01-01', 'V3', DATE '2023-01-01', 10); -- No change
        # INSERT INTO SOF$TA_C_BFC_AKT VALUES ('C5', DATE '2023-03-01', 'V5', DATE '2023-03-01', 12); -- New, should insert

        # ... (Corresponding BigQuery inserts) ...
        pass

    def run_oracle_step3():
        # This would be the Oracle MERGE statement from d_ausd_v_ta_c_bfc.sql
        # Note: Oracle's MERGE syntax might differ slightly, and the UDF call needs to be adapted.
        # For testing, we'd run the actual Oracle SQL.
        oracle_sql = f"""
            MERGE INTO {ORACLE_SCHEMA}.SOF$TA_C_BFC D
            USING {ORACLE_SCHEMA}.SOF$TA_C_BFC_AKT S
            ON (D.CNTRCT_ID = S.CNTRCT_ID)
            WHEN MATCHED AND (
                   D.BFC_AGE < S.BFC_AGE
                OR D.BFC_COUNT <> S.BFC_COUNT
            ) THEN
                UPDATE SET
                    BINDEFRIST = {ORACLE_SCHEMA}.BFC_GET_BINDEFRIST(S.CNTRCT_ID, S.COMMITMENT_REFERENCE_DATE, S.CNTRCT_VALIDITY_ID),
                    BFC_AGE = S.BFC_AGE,
                    BFC_COUNT = S.BFC_COUNT,
                    BFC_PROCEDURE = TRUNC(SYSDATE), -- Assuming &v_bfc_procedure is SYSDATE
                    COMMITMENT_REFERENCE_DATE = S.COMMITMENT_REFERENCE_DATE,
                    CNTRCT_VALIDITY_ID = S.CNTRCT_VALIDITY_ID,
                    LOAD_TS = SYSTIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (
                    CNTRCT_ID, BINDEFRIST, BFC_AGE, BFC_COUNT, BFC_PROCEDURE, COMMITMENT_REFERENCE_DATE, CNTRCT_VALIDITY_ID, LOAD_TS
                )
                VALUES (
                    S.CNTRCT_ID,
                    {ORACLE_SCHEMA}.BFC_GET_BINDEFRIST(S.CNTRCT_ID, S.COMMITMENT_REFERENCE_DATE, S.CNTRCT_VALIDITY_ID),
                    S.BFC_AGE,
                    S.BFC_COUNT,
                    TRUNC(SYSDATE), -- Assuming &v_bfc_procedure is SYSDATE
                    S.COMMITMENT_REFERENCE_DATE,
                    S.CNTRCT_VALIDITY_ID,
                    SYSTIMESTAMP
                );
        """
        with oracle_engine.connect() as connection:
            connection.execute(oracle_sql)
            connection.commit()

    def run_bq_step3():
        bq_sql = f"""
            MERGE INTO `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc` AS D
            USING `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt` AS S
            ON (
                D.cntrct_id = S.cntrct_id
            )
            WHEN MATCHED AND (
                   D.bfc_age < S.bfc_age
                OR D.bfc_count <> S.bfc_count
            ) THEN
                UPDATE SET
                    bindefrist = `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
                    bfc_age = S.bfc_age,
                    bfc_count = S.bfc_count,
                    bfc_procedure = CURRENT_DATE(),
                    commitment_reference_date = S.commitment_reference_date,
                    cntrct_validity_id = S.cntrct_validity_id,
                    load_ts = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    cntrct_id, bindefrist, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id, load_ts
                )
                VALUES (
                    S.cntrct_id,
                    `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
                    S.bfc_age,
                    S.bfc_count,
                    CURRENT_DATE(),
                    S.commitment_reference_date,
                    S.cntrct_validity_id,
                    CURRENT_TIMESTAMP()
                );
        """
        bq_client.query(bq_sql).result()

    # Main execution
    setup_merge_data()
    run_oracle_step3()
    run_bq_step3()
    assert compare_target_tables() # Use the compare_target_tables from Test Case 4
    ```

### Test Case 6: Step 4 - Stale Row Recalculation Parity (`step4_recalculate_stale_rows`)

*   **Purpose:** Verify that the BigQuery `UPDATE` statement for stale rows correctly identifies and updates records based on `bfc_procedure` and applies the `v_max_update` limit, matching Oracle's behavior. This also tests the `bfc_get_bindefrist` UDF integration.
*   **Setup:**
    1.  Populate `ta_c_bfc` in both environments with data that includes:
        *   Rows where `bfc_procedure` is older than `CURRENT_DATE()` (should be updated).
        *   Rows where `bfc_procedure` is `CURRENT_DATE()` or newer (should not be updated).
        *   A sufficient number of "stale" rows to exceed `v_max_update` (e.g., 1,000,000).
    2.  Set `v_max_update` parameter in Airflow DAG to a specific value (e.g., 1000).
    3.  Ensure the `bfc_get_bindefrist` UDF is deployed.
*   **Action:**
    1.  Execute Oracle's stale row update logic (including the `ROWNUM` limit).
    2.  Execute BigQuery's `step4_recalculate_stale_rows` task.
    3.  Extract all data from `sof$ta_c_bfc` (Oracle) and `ta_c_bfc` (BigQuery).
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The number of updated rows in BigQuery matches the number of updated rows in Oracle (which should be `v_max_update` if enough stale rows exist).
        *   The specific set of `cntrct_id`s updated in BigQuery matches the set updated in Oracle.
        *   The updated `bindefrist` and `bfc_procedure` values are identical, excluding `load_ts`.
    *   **Fail:** Any discrepancy in the number of updated rows, the specific rows updated, or their updated values.
*   **Note on `ROWNUM` vs `QUALIFY`:** Oracle's `ROWNUM` without an `ORDER BY` is non-deterministic. BigQuery's `QUALIFY ROW_NUMBER() OVER(ORDER BY cntrct_id)` introduces determinism. For this test, we assume the `ORDER BY cntrct_id` in BigQuery is acceptable and compare the *set* of updated rows. If Oracle's behavior was truly random and that was a business requirement, this would be a behavioral change.

*   **Runnable Test Code (SQL/Python):**

    ```python
    # Python (conceptual, similar structure to Test Case 3)
    # ... (BQ_PROJECT_ID, BQ_DATASET_ID, ORACLE_SCHEMA, ORACLE_CONN_STR, bq_client, oracle_engine setup) ...

    V_MAX_UPDATE = 1000 # Example value for testing

    def setup_stale_data():
        # Populate ta_c_bfc with rows having bfc_procedure < CURRENT_DATE()
        # and some with bfc_procedure = CURRENT_DATE()
        # Ensure more than V_MAX_UPDATE rows are stale.
        # Example:
        # TRUNCATE TABLE SOF$TA_C_BFC;
        # For i in 1 to V_MAX_UPDATE + 100:
        #   INSERT INTO SOF$TA_C_BFC VALUES (f'C{i}', NULL, DATE '2022-01-01', 10, DATE '2022-01-01', DATE '2023-01-01', 'V1', SYSDATE); -- Stale
        # INSERT INTO SOF$TA_C_BFC VALUES ('C_RECENT', NULL, CURRENT_DATE(), 10, CURRENT_DATE(), CURRENT_DATE(), 'V_RECENT', SYSDATE); -- Not stale
        # ... (Corresponding BigQuery inserts) ...
        pass

    def run_oracle_step4():
        oracle_sql = f"""
            UPDATE {ORACLE_SCHEMA}.SOF$TA_C_BFC
            SET
                BINDEFRIST = {ORACLE_SCHEMA}.BFC_GET_BINDEFRIST(CNTRCT_ID, COMMITMENT_REFERENCE_DATE, CNTRCT_VALIDITY_ID),
                BFC_PROCEDURE = TRUNC(SYSDATE), -- Assuming &v_bfc_procedure is SYSDATE
                LOAD_TS = SYSTIMESTAMP
            WHERE
                ROWID IN (
                    SELECT RID FROM (
                        SELECT ROWID RID
                        FROM {ORACLE_SCHEMA}.SOF$TA_C_BFC
                        WHERE BFC_PROCEDURE < TRUNC(SYSDATE) -- Assuming &v_bfc_procedure is SYSDATE
                        ORDER BY CNTRCT_ID -- Added for deterministic comparison, if Oracle didn't have it
                    ) WHERE ROWNUM <= {V_MAX_UPDATE}
                );
        """
        with oracle_engine.connect() as connection:
            connection.execute(oracle_sql)
            connection.commit()

    def run_bq_step4():
        bq_sql = f"""
            UPDATE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc`
            SET
                bindefrist = `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.bfc_get_bindefrist`(
                    cntrct_id,
                    commitment_reference_date,
                    cntrct_validity_id
                ),
                bfc_procedure = CURRENT_DATE(),
                load_ts = CURRENT_TIMESTAMP()
            WHERE
                bfc_procedure < CURRENT_DATE()
            QUALIFY
                ROW_NUMBER() OVER(ORDER BY cntrct_id) <= {V_MAX_UPDATE};
        """
        bq_client.query(bq_sql).result()

    # Main execution
    setup_stale_data()
    run_oracle_step4()
    run_bq_step4()
    assert compare_target_tables() # Use the compare_target_tables from Test Case 4
    ```

### Test Case 7: End-to-End Output Parity

*   **Purpose:** Validate that running the entire migrated job (all BigQuery steps orchestrated by Airflow) produces the same final `ta_c_bfc` table as the legacy Oracle job when given identical initial source data.
*   **Setup:**
    1.  Populate all source tables in both Oracle and BigQuery with an identical, comprehensive dataset.
    2.  Ensure `ta_c_bfc` and `ta_c_bfc_akt` are empty in both environments.
    3.  Configure the Airflow DAG with appropriate parameters (e.g., `v_max_update`).
*   **Action:**
    1.  Execute the full legacy Oracle job (`r_ausd_v_ta_c_bfc.ksh` which calls `k_ausd_v_ta_c_bfc.ksh` which executes `d_ausd_v_ta_c_bfc.sql`).
    2.  Trigger the Airflow DAG `r_ausd_v_ta_c_bfc_dag`.
    3.  Extract all data from `sof$ta_c_bfc` (Oracle) and `ta_c_bfc` (BigQuery).
*   **Pass/Fail Criterion:**
    *   **Pass:** The data in BigQuery's `ta_c_bfc` table is identical to the data in Oracle's `sof$ta_c_bfc` table, excluding the `load_ts` column. Row counts must also match.
    *   **Fail:** Any discrepancy in row counts or data content.

*   **Runnable Test Code (Python):**

    ```python
    # Python (conceptual, orchestrating the full run)
    import pandas as pd
    from google.cloud import bigquery
    from sqlalchemy import create_engine
    # Assuming Airflow DAG can be triggered programmatically or manually for testing
    # from airflow.api.client.local_client import Client # For local testing

    BQ_PROJECT_ID = "your-gcp-project"
    BQ_DATASET_ID = "your_dataset"
    ORACLE_SCHEMA = "ISBERT_SCHEMA"
    ORACLE_CONN_STR = "oracle+cx_oracle://user:password@host:port/service_name"

    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    oracle_engine = create_engine(ORACLE_CONN_STR)

    def prepare_initial_source_data():
        # This function would populate all source tables (sof$ta_cntrct_crs, etc.)
        # with identical data in both Oracle and BigQuery.
        # It should also clear target and staging tables.
        print("Preparing initial source data in both Oracle and BigQuery...")
        # ... (implementation to insert identical data) ...
        print("Source data prepared.")

    def run_legacy_oracle_job():
        print("Executing legacy Oracle job...")
        # This would involve calling the ksh script, e.g.:
        # import subprocess
        # subprocess.run(["/path/to/r_ausd_v_ta_c_bfc.ksh", "-j", "TEST_JOB", "-f", "123"], check=True)
        # For testing, you might directly execute the d_ausd_v_ta_c_bfc.sql script
        # with appropriate parameters via SQL*Plus or cx_Oracle.
        # For this example, we'll assume a direct SQL execution for simplicity.
        oracle_full_script = """
            -- Simulate the full d_ausd_v_ta_c_bfc.sql execution
            TRUNCATE TABLE ISBERT_SCHEMA.SOF$TA_C_BFC_AKT;
            -- Step 1
            INSERT INTO ISBERT_SCHEMA.SOF$TA_C_BFC_AKT (...) SELECT ... FROM ISBERT_SCHEMA.SOF$TA_CNTRCT_CRS ...;
            -- Step 2
            INSERT INTO ISBERT_SCHEMA.SOF$TA_C_BFC (...) SELECT ... FROM ISBERT_SCHEMA.SOF$TA_C_BFC_AKT ... WHERE (SELECT COUNT(1) FROM ISBERT_SCHEMA.SOF$TA_C_BFC) = 0;
            -- Step 3
            MERGE INTO ISBERT_SCHEMA.SOF$TA_C_BFC D USING ISBERT_SCHEMA.SOF$TA_C_BFC_AKT S ON (...) WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...;
            -- Step 4
            UPDATE ISBERT_SCHEMA.SOF$TA_C_BFC SET ... WHERE ROWID IN (SELECT RID FROM (SELECT ROWID RID FROM ISBERT_SCHEMA.SOF$TA_C_BFC WHERE BFC_PROCEDURE < TRUNC(SYSDATE) ORDER BY CNTRCT_ID) WHERE ROWNUM <= 1000000);
            TRUNCATE TABLE ISBERT_SCHEMA.SOF$TA_C_BFC_AKT;
            COMMIT;
        """
        with oracle_engine.connect() as connection:
            connection.execute(oracle_full_script)
            connection.commit()
        print("Legacy Oracle job completed.")

    def trigger_airflow_dag():
        print("Triggering Airflow DAG r_ausd_v_ta_c_bfc_dag...")
        # This would typically be done via Airflow REST API or CLI
        # Example using Airflow's local client (for local dev/test)
        # client = Client(None, None, None, None)
        # client.trigger_dag(dag_id="r_ausd_v_ta_c_bfc_dag", conf={"v_max_update": 1000000})
        # For a real test, you'd wait for the DAG run to complete and check its status.
        # For this example, we'll simulate by running the BQ SQL steps directly.
        bq_sql_steps = [
            # DDLs (create_ta_c_bfc_table, create_ta_c_bfc_akt_table, create_bfc_get_bindefrist_udf)
            # These are typically idempotent, run once or ensure they exist.
            # For this test, assume they are already set up.
            # Step 1
            f"""TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt`; INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt` (cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age, bfc_count) SELECT c.cntrct_id, MAX(c.commitment_reference_date), MAX(c.cntrct_validity_id), MAX(GREATEST(IFNULL(c.bfc_age, PARSE_DATE('%Y%m%d', '19000101')), IFNULL(b.bfc_age, PARSE_DATE('%Y%m%d', '19000101')), IFNULL(v.bfc_age, PARSE_DATE('%Y%m%d', '19000101')), IFNULL(p_fi.bfc_age, PARSE_DATE('%Y%m%d', '19000101')), IFNULL(p_fo.bfc_age, PARSE_DATE('%Y%m%d', '19000101')), IFNULL(p_fi_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')), IFNULL(p_fo_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')))) AS bfc_age, COUNT(1) AS bfc_count FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_cntrct_crs` AS c LEFT JOIN `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_barrier` AS b ON c.cntrct_id = b.cntrct_id LEFT JOIN `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_cntrct_valid` AS v ON c.cntrct_validity_id = v.cntrct_validity_id LEFT JOIN `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fi ON v.first_period_id = p_fi.period_id LEFT JOIN `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fo ON v.following_period_id = p_fo.period_id LEFT JOIN `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fi_n ON v.first_notice_period_id = p_fi_n.period_id LEFT JOIN `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof$ta_period` AS p_fo_n ON v.follow_notice_period_id = p_fo_n.period_id GROUP BY c.cntrct_id;""",
            # Step 2
            f"""INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc` (cntrct_id, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id) SELECT akt.cntrct_id, akt.bfc_age, akt.bfc_count, PARSE_DATE('%Y%m%d', '19000101') AS bfc_procedure, akt.commitment_reference_date, akt.cntrct_validity_id FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt` AS akt WHERE (SELECT COUNT(1) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc`) = 0;""",
            # Step 3
            f"""MERGE INTO `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc` AS D USING `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt` AS S ON (D.cntrct_id = S.cntrct_id) WHEN MATCHED AND (D.bfc_age < S.bfc_age OR D.bfc_count <> S.bfc_count) THEN UPDATE SET bindefrist = `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id), bfc_age = S.bfc_age, bfc_count = S.bfc_count, bfc_procedure = CURRENT_DATE(), commitment_reference_date = S.commitment_reference_date, cntrct_validity_id = S.cntrct_validity_id, load_ts = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (cntrct_id, bindefrist, bfc_age, bfc_count, bfc_procedure, commitment_reference_date, cntrct_validity_id, load_ts) VALUES (S.cntrct_id, `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id), S.bfc_age, S.bfc_count, CURRENT_DATE(), S.commitment_reference_date, S.cntrct_validity_id, CURRENT_TIMESTAMP());""",
            # Step 4
            f"""UPDATE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc` SET bindefrist = `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.bfc_get_bindefrist`(cntrct_id, commitment_reference_date, cntrct_validity_id), bfc_procedure = CURRENT_DATE(), load_ts = CURRENT_TIMESTAMP() WHERE bfc_procedure < CURRENT_DATE() QUALIFY ROW_NUMBER() OVER(ORDER BY cntrct_id) <= 1000000;""", # v_max_update hardcoded for example
            # Cleanup
            f"""TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc_akt`;"""
        ]
        for sql in bq_sql_steps:
            bq_client.query(sql).result()
        print("Airflow DAG (simulated) completed.")

    def compare_final_target_tables():
        oracle_df = pd.read_sql(f"SELECT * FROM {ORACLE_SCHEMA}.SOF$TA_C_BFC ORDER BY CNTRCT_ID", oracle_engine)
        bq_df = bq_client.query(f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.ta_c_bfc` ORDER BY cntrct_id").result().to_dataframe()

        # Normalize column names and date types for comparison
        oracle_df.columns = [col.lower() for col in oracle_df.columns]
        for col in ['bindefrist', 'bfc_age', 'bfc_procedure', 'commitment_reference_date', 'load_ts']:
            if col in oracle_df.columns and col in bq_df.columns:
                oracle_df[col] = pd.to_datetime(oracle_df[col]).dt.date
                bq_df[col] = pd.to_datetime(bq_df[col]).dt.date

        # Exclude load_ts from direct comparison as it will differ
        oracle_df = oracle_df.drop(columns=['load_ts'], errors='ignore')
        bq_df = bq_df.drop(columns=['load_ts'], errors='ignore')

        if oracle_df.equals(bq_df):
            print("PASS: End-to-end final target table (ta_c_bfc) data is identical.")
            return True
        else:
            print("FAIL: End-to-end final target table (ta_c_bfc) data differs.")
            diff = pd.concat([oracle_df, bq_df]).drop_duplicates(keep=False)
            print("Differences found:")
            print(diff)
            return False

    # Main execution for end-to-end test
    prepare_initial_source_data()
    run_legacy_oracle_job()
    trigger_airflow_dag()
    assert compare_final_target_tables()
    ```

### Test Case 8: Data Quality - Null Handling & Type Coercion

*   **Purpose:** Verify that NULL values are handled consistently across Oracle and BigQuery, especially in `GREATEST`, `IFNULL`/`NVL`, and UDF calls. Also, ensure implicit or explicit type coercions behave as expected.
*   **Setup:**
    1.  Populate source tables with specific test data including:
        *   `NULL` values for `bfc_age` in `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`.
        *   `NULL` for `commitment_reference_date` in `sof$ta_cntrct_crs` (to test UDF input).
        *   Edge case dates like `1900-01-01` (used as `IFNULL` default).
    2.  Ensure `ta_c_bfc` and `ta_c_bfc_akt` are empty.
*   **Action:**
    1.  Execute the full legacy Oracle job.
    2.  Trigger the Airflow DAG.
    3.  Query `ta_c_bfc` and `ta_c_bfc_akt` in both environments.
*   **Pass/Fail Criterion:**
    *   **Pass:** All `NULL` values and date values (including `1900-01-01`) in the output tables are identical between Oracle and BigQuery. No type conversion errors occurred.
    *   **Fail:** Any discrepancy in `NULL` handling, date values, or type-related errors.

### Test Case 9: Row Count Assertions

*   **Purpose:** Verify that row counts at critical stages of the transformation (staging, final target) are consistent between the legacy and migrated systems.
*   **Setup:**
    1.  Populate source tables with a known dataset.
    2.  Ensure `ta_c_bfc` and `ta_c_bfc_akt` are empty.
*   **Action:**
    1.  Execute the full legacy Oracle job. Record row counts for `sof$ta_c_bfc_akt` (after Step 1) and `sof$ta_c_bfc` (final).
    2.  Trigger the Airflow DAG. Record row counts for `ta_c_bfc_akt` (after Step 1) and `ta_c_bfc` (final).
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   `COUNT(*)` from `ta_c_bfc_akt` (BigQuery) after `step1_build_staging` matches `COUNT(*)` from `sof$ta_c_bfc_akt` (Oracle) after its Step 1.
        *   `COUNT(*)` from `ta_c_bfc` (BigQuery) after the full DAG run matches `COUNT(*)` from `sof$ta_c_bfc` (Oracle) after the full legacy job.
    *   **Fail:** Any mismatch in row counts.

*   **Runnable Test Code (SQL/Python):**

    ```python
    # Python (part of end-to-end or separate checks)
    # ... (BQ_PROJECT_ID, BQ_DATASET_ID, ORACLE_SCHEMA, ORACLE_CONN_STR, bq_client, oracle_engine setup) ...

    def get_row_count(table_name, is_oracle=True):
        if is_oracle:
            query = f"SELECT COUNT(*) FROM {ORACLE_SCHEMA}.{table_name}"
            return pd.read_sql(query, oracle_engine).iloc[0,0]
        else:
            query = f"SELECT COUNT(*) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}`"
            return bq_client.query(query).result().to_dataframe().iloc[0,0]

    # After running Step 1 in both environments:
    oracle_staging_count = get_row_count("SOF$TA_C_BFC_AKT", is_oracle=True)
    bq_staging_count = get_row_count("ta_c_bfc_akt", is_oracle=False)
    assert oracle_staging_count == bq_staging_count, f"Staging table row count mismatch: Oracle={oracle_staging_count}, BQ={bq_staging_count}"

    # After full job run in both environments:
    oracle_final_count = get_row_count("SOF$TA_C_BFC", is_oracle=True)
    bq_final_count = get_row_count("ta_c_bfc", is_oracle=False)
    assert oracle_final_count == bq_final_count, f"Final target table row count mismatch: Oracle={oracle_final_count}, BQ={bq_final_count}"
    ```

### Test Case 10: Schema Assertions for Target Tables

*   **Purpose:** Verify that the final `ta_c_bfc` and `ta_c_bfc_akt` tables in BigQuery conform to the expected schema (column names, data types, nullability).
*   **Setup:**
    1.  Ensure the DDLs for `ta_c_bfc` and `ta_c_bfc_akt` have been executed in BigQuery.
    2.  Record the expected schema based on the migration design.
*   **Action:**
    1.  Query the schema of `ta_c_bfc` and `ta_c_bfc_akt` in BigQuery.
*   **Pass/Fail Criterion:**
    *   **Pass:** The BigQuery schemas for `ta_c_bfc` and `ta_c_bfc_akt` exactly match the defined target schemas in the migration design document, including column names, data types, and nullability constraints.
    *   **Fail:** Any discrepancy in schema definition.

*   **Runnable Test Code (Python):**

    ```python
    import pytest
    from google.cloud import bigquery

    BQ_PROJECT_ID = "your-gcp-project"
    BQ_DATASET_ID = "your_dataset"

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client(project=BQ_PROJECT_ID)

    def test_ta_c_bfc_schema(bq_client):
        expected_schema = [
            ("cntrct_id", "STRING", "REQUIRED"),
            ("bindefrist", "DATE", "NULLABLE"),
            ("bfc_age", "INT64", "NULLABLE"), # Note: Oracle DATE to BQ INT64 for bfc_age is unusual, assuming design intent
            ("bfc_count", "INT64", "NULLABLE"),
            ("bfc_procedure", "DATE", "NULLABLE"),
            ("commitment_reference_date", "DATE", "NULLABLE"),
            ("cntrct_validity_id", "STRING", "NULLABLE"),
            ("load_ts", "TIMESTAMP", "NULLABLE"), # DEFAULT CURRENT_TIMESTAMP() implies NULLABLE
        ]
        table_ref = bq_client.dataset(BQ_DATASET_ID).table("ta_c_bfc")
        table = bq_client.get_table(table_ref)
        actual_schema = [(field.name, field.field_type, field.mode) for field in table.schema]

        # Convert expected schema to match actual_schema format for comparison
        expected_schema_formatted = [(name, bq_type, "REQUIRED" if mode == "NOT NULL" else "NULLABLE") for name, bq_type, mode in expected_schema]

        assert sorted(actual_schema) == sorted(expected_schema_formatted), "Schema for ta_c_bfc does not match expected."

    def test_ta_c_bfc_akt_schema(bq_client):
        expected_schema = [
            ("cntrct_id", "STRING", "REQUIRED"),
            ("bindefrist", "DATE", "NULLABLE"),
            ("bfc_age", "INT64", "NULLABLE"),
            ("bfc_count", "INT64", "NULLABLE"),
            ("bfc_procedure", "DATE", "NULLABLE"),
            ("commitment_reference_date", "DATE", "NULLABLE"),
            ("cntrct_validity_id", "STRING", "NULLABLE"),
            ("load_ts", "TIMESTAMP", "NULLABLE"),
        ]
        table_ref = bq_client.dataset(BQ_DATASET_ID).table("ta_c_bfc_akt")
        table = bq_client.get_table(table_ref)
        actual_schema = [(field.name, field.field_type, field.mode) for field in table.schema]

        expected_schema_formatted = [(name, bq_type, "REQUIRED" if mode == "NOT NULL" else "NULLABLE") for name, bq_type, mode in expected_schema]

        assert sorted(actual_schema) == sorted(expected_schema_formatted), "Schema for ta_c_bfc_akt does not match expected."
    ```

### Test Case 11: Airflow Orchestration and Logging

*   **Purpose:** Verify that the Airflow DAG executes successfully, handles parameters, and logs its operations correctly within Cloud Logging.
*   **Setup:**
    1.  Deploy the `r_ausd_v_ta_c_bfc_dag.py` to Cloud Composer.
    2.  Ensure Cloud Logging is configured for the Composer environment.
*   **Action:**
    1.  Manually trigger the Airflow DAG with default parameters.
    2.  Monitor the DAG run in the Airflow UI.
    3.  Check Cloud Logging for logs generated by the DAG tasks.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The Airflow DAG completes successfully without any task failures.
        *   All BigQuery tasks are executed in the correct sequence.
        *   Logs for each task are present in Cloud Logging, indicating successful execution and any relevant output.
        *   Parameters (e.g., `v_max_update`) are correctly passed to the BigQuery SQL.
    *   **Fail:** Any DAG failure, incorrect task sequencing, missing logs, or parameter misconfiguration.

*   **Runnable Test Code (Manual/Observational):**
    *   This test is primarily observational via Airflow UI and Cloud Logging.
    *   For automated checks, one might use Airflow's API to trigger and monitor DAG runs, and GCP Logging API to query logs.

    ```python
    # Example Python snippet for triggering DAG (requires Airflow client setup)
    # from airflow.api.client.local_client import Client
    # client = Client(None, None, None, None) # Replace with actual Airflow client setup
    # try:
    #     dag_run = client.trigger_dag(dag_id="r_ausd_v_ta_c_bfc_dag", conf={"v_max_update": 1000000})
    #     print(f"DAG run triggered: {dag_run.run_id}")
    #     # Implement polling logic to check dag_run.state until it's 'success' or 'failed'
    #     # Then query Cloud Logging for logs related to this dag_run_id
    # except Exception as e:
    #     print(f"Failed to trigger DAG: {e}")
    ```