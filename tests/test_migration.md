As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `DW.BERT_ABLAUFSTEUERUNG`. This job, primarily an orchestrator, requires a multi-faceted testing approach, focusing on the Airflow DAG's scheduling and dependencies, the correctness of the BigQuery SQL transformations, and the proper functioning of external system replacements.

The following test cases are designed to validate the migrated solution against the specified requirements.

---

## Migration Validation Tests for DW.BERT_ABLAUFSTEUERUNG

### 1. Orchestration and Scheduling Tests

#### Test Case 1.1: DAG Deployment and Basic Execution
*   **Purpose:** Verify that the `bert_ablaufsteuerung_dag` can be successfully deployed to Cloud Composer and executes without syntax errors or immediate failures.
*   **Setup:**
    *   A Cloud Composer environment is provisioned and running.
    *   All DAG files (`bert_ablaufsteuerung_dag.py`, `sub_dags/*.py`) and supporting scripts (`scripts/*.py`, `sql/*.sql`) are deployed to the DAGs folder in the Composer bucket.
    *   Airflow variables (`SOURCE_BQ_PROJECT`, `SOURCE_BQ_DATASET`, `TARGET_BQ_PROJECT`, `TARGET_BQ_DATASET`, `GCS_EXPORT_BUCKET`) are configured in Airflow UI.
*   **Action:**
    1.  Trigger the `bert_ablaufsteuerung_dag` manually in the Airflow UI for a specific `execution_date` (e.g., `2023-01-01`).
    2.  Monitor the DAG run in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   **Pass:** The DAG run completes successfully with all tasks marked as 'success'. No tasks are skipped unexpectedly.
    *   **Fail:** The DAG run fails, or any task within the DAG fails or enters a 'skipped' state without explicit design.

#### Test Case 1.2: Task Dependencies and Execution Order
*   **Purpose:** Verify that the sequence of tasks and TaskGroups within the `bert_ablaufsteuerung_dag` matches the intended orchestration logic from the UC4 XML, ensuring correct data flow and process order.
*   **Setup:**
    *   `bert_ablaufsteuerung_dag` is deployed and configured as in Test Case 1.1.
*   **Action:**
    1.  Trigger the `bert_ablaufsteuerung_dag` manually.
    2.  Observe the task execution order in the Airflow UI's Graph View or Gantt Chart.
*   **Pass/Fail Criterion:**
    *   **Pass:** The tasks and TaskGroups execute in the following order:
        `start` -> `bert_run_adm_check_jp_evt` -> `bert_monatlich_jp_group` -> `bert_adm_housekeeping_jp_group` -> `dwh_apt_export_taeglich_jp_group` -> `bert_stammdaten_jp_group` -> `dwh_run_apt_export_monatlich_jp_evt` -> `end`.
    *   **Pass (within TaskGroups):**
        *   `bert_monatlich_jp_group`: `start_monatlich` -> `bert_rechnungsdaten` -> `bert_log`.
        *   `bert_adm_housekeeping_jp_group`: `start_housekeeping` -> `adm_check_task` -> `housekeeping_task`.
        *   `dwh_apt_export_taeglich_jp_group`: `start_apt_export` -> `[bert_bestandsdaten, bert_nna_daten, bert_nna_voice, bert_rabattdaten]` (parallel) -> `[export_bestandsdaten_to_gcs, export_nna_daten_to_gcs, export_nna_voice_to_gcs, export_rabattdaten_to_gcs]` (each export task depends on its respective BQ task).
        *   `bert_stammdaten_jp_group`: `start_stammdaten` -> `bert_drop_temp_table` -> `bert_p_adressen` -> `bert_p_austausch` -> `bert_p_basisprodukt_jp_group`.
    *   **Fail:** Any task executes out of order or a dependency is not respected.

#### Test Case 1.3: Daily Scheduling and `max_active_runs`
*   **Purpose:** Verify that the DAG adheres to its daily schedule (`0 0 * * *`) and that `max_active_runs=1` prevents concurrent runs, mimicking UC4's single instance behavior.
*   **Setup:**
    *   `bert_ablaufsteuerung_dag` is deployed and configured.
    *   Ensure the `schedule_interval` is set to `0 0 * * *` and `max_active_runs=1`.
*   **Action:**
    1.  Allow the DAG to run naturally for at least 2 consecutive days.
    2.  Attempt to manually trigger the DAG while an existing run is still in progress (e.g., by pausing a task in a running instance).
*   **Pass/Fail Criterion:**
    *   **Pass:** The DAG automatically triggers once every 24 hours at midnight UTC. When a run is active, any attempt to trigger a new run (manual or scheduled) results in the new run being queued or prevented until the active run completes.
    *   **Fail:** The DAG triggers more or less frequently than daily, or multiple instances of the DAG run concurrently.

#### Test Case 1.4: Monthly Task Conditional Execution (Conceptual)
*   **Purpose:** Verify that tasks intended for monthly execution (e.g., `bert_rechnungsdaten` within `bert_monatlich_jp_group`) are correctly handled. The current code runs it daily; this test highlights the need for explicit conditional logic if the legacy job was truly monthly.
*   **Setup:**
    *   `bert_ablaufsteuerung_dag` is deployed.
    *   **Note:** The current `bert_monatlich_jp.py` code executes `bert_rechnungsdaten` daily. If the legacy `DW.BERT_MONATLICH_JP` truly ran only monthly, the Airflow DAG needs a `BranchPythonOperator` or similar logic to skip this task on non-monthly days. For this test, we'll assume the current code is a placeholder and the *design intent* is monthly.
*   **Action:**
    1.  Run the DAG for a day that is *not* the designated monthly execution day (e.g., `2023-01-02` if monthly is `2023-01-01`).
    2.  Run the DAG for the designated monthly execution day (e.g., `2023-01-01`).
*   **Pass/Fail Criterion (Based on Design Intent):**
    *   **Pass:** On non-monthly days, the `bert_rechnungsdaten` task is skipped. On the designated monthly day, the `bert_rechnungsdaten` task executes successfully.
    *   **Fail:** The `bert_rechnungsdaten` task executes daily, or fails to execute on the designated monthly day.
    *   **Note:** Given the current code, this test will likely *fail* against the "monthly" intent, indicating a gap in the migration of calendar dependencies. This test serves to identify that gap.

### 2. Transformation Correctness and Output Parity Tests (SQL)

For these tests, "Golden Record" comparison is crucial. This involves:
1.  **Legacy Data Snapshot:** Extracting a representative dataset from the legacy Oracle source tables (e.g., `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, etc.) for a specific `execution_date` (e.g., `2023-01-01`).
2.  **BQ Source Ingestion:** Loading this exact snapshot into the corresponding BigQuery source tables (`SOURCE_BQ_PROJECT.SOURCE_BQ_DATASET.RPT_TA_S_D1_VERTRAG`, etc.).
3.  **Legacy Output Generation:** Running the legacy UC4 job (or its SQL components) for the same `execution_date` to produce its output files/tables.
4.  **Migrated Output Generation:** Running the `dwh_apt_export_taeglich_jp_group` TaskGroup (or the individual BigQuery SQL tasks) for the same `execution_date`.
5.  **Comparison:** Comparing the output of the legacy system with the output of the migrated BigQuery tables.

#### Test Case 2.1: `bert_bestandsdaten.sql` - Output Parity and Logic
*   **Purpose:** Verify that `bert_bestandsdaten.sql` produces identical output to its legacy counterpart (`d_exis_apt_bestandsdaten.sql`) for the same input data, including joins, `STRING_AGG` aggregation, `VERTRAGSSTATUS` filter, and `FORMAT_DATE` type handling.
*   **Setup:**
    *   Populate BigQuery source tables (`RPT_TA_S_D1_VERTRAG`, `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`) with a representative snapshot of legacy data.
    *   Obtain the "golden record" output from the legacy `d_exis_apt_bestandsdaten.sql` for this specific input.
*   **Action:**
    1.  Execute the `bert_bestandsdaten` task within the `dwh_apt_export_taeglich_jp_group` (or run the SQL directly in BigQuery).
    2.  Query the resulting `TARGET_BQ_PROJECT.TARGET_BQ_DATASET.bert_bestandsdaten` table.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The row count of `bert_bestandsdaten` matches the legacy output.
        *   A deep comparison (e.g., using `EXCEPT DISTINCT` in SQL or a data comparison tool) shows no differences in data content, column order, or data types between the migrated `bert_bestandsdaten` table and the golden record legacy output.
        *   Specifically, verify:
            *   `STRING_AGG(A.BPR_ID, ',' ORDER BY RPT.RAHMENVERTRAG_ID)` correctly aggregates multiple `BPR_ID`s.
            *   `RPT.VERTRAGSSTATUS = 'ACTIVE'` filter is applied correctly.
            *   `FORMAT_DATE('%d.%m.%Y', RPT.VERTRAGSBEGINN)` correctly formats the date.
    *   **Fail:** Any discrepancy in row count or data content.

    ```sql
    -- Example SQL for output parity check (assuming legacy output is in a BQ table named 'legacy_bert_bestandsdaten')
    SELECT 'Migrated_Only' as source, * FROM `{{ var.value.TARGET_BQ_PROJECT }}.{{ var.value.TARGET_BQ_DATASET }}.bert_bestandsdaten`
    EXCEPT DISTINCT
    SELECT 'Migrated_Only' as source, * FROM `{{ var.value.LEGACY_OUTPUT_BQ_PROJECT }}.{{ var.value.LEGACY_OUTPUT_BQ_DATASET }}.legacy_bert_bestandsdaten`

    UNION ALL

    SELECT 'Legacy_Only' as source, * FROM `{{ var.value.LEGACY_OUTPUT_BQ_PROJECT }}.{{ var.value.LEGACY_OUTPUT_BQ_DATASET }}.legacy_bert_bestandsdaten`
    EXCEPT DISTINCT
    SELECT 'Legacy_Only' as source, * FROM `{{ var.value.TARGET_BQ_PROJECT }}.{{ var.value.TARGET_BQ_DATASET }}.bert_bestandsdaten`;
    ```

#### Test Case 2.2: `bert_nna_daten.sql` - Output Parity and Logic
*   **Purpose:** Verify `bert_nna_daten.sql` produces identical output to its legacy counterpart (`d_exis_apt_nna_daten.sql`), including complex joins, `MONATS_ID` filtering, `GUELTIG_BIS` filter, `ROUND` for numeric conversions, and `CONCAT` for string concatenation.
*   **Setup:**
    *   Populate BigQuery source tables (`DWH_VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH_VI_C_VERTRAG`, `DWH_TA_F_NNV_GPRS`) with a representative snapshot of legacy data.
    *   Obtain the "golden record" output from the legacy `d_exis_apt_nna_daten.sql` for a specific `execution_date` (e.g., `2023-01-01`), ensuring the legacy `MONATS_ID` filter matches `202301`.
*   **Action:**
    1.  Execute the `bert_nna_daten` task (or run the SQL directly) with `ds_nodash` set to `20230101`.
    2.  Query the resulting `TARGET_BQ_PROJECT.TARGET_BQ_DATASET.bert_nna_daten` table.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The row count of `bert_nna_daten` matches the legacy output.
        *   A deep comparison shows no differences in data content.
        *   Specifically, verify:
            *   `NNA.RAHMENVERTRAG IS NOT NULL` filter is applied.
            *   `NNA.MONATS_ID = CAST('{{ ds_nodash[:6] }}' AS INT64)` correctly filters for the month.
            *   `TAR.GUELTIG_BIS = PARSE_DATE('%Y%m%d', '47121231')` filter is applied.
            *   `ROUND(NNA.GESAMTVOLUMEN_BYTE / 1024 / 1024, 0)` and `ROUND(NNA.RBETRAG_VBUD_NETTO_CENT_VOL / 100, 2)` perform correct numeric conversions and rounding.
            *   `CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ)` concatenates strings as expected, including NULL handling if any component is NULL.
    *   **Fail:** Any discrepancy in row count or data content.

#### Test Case 2.3: `bert_nna_voice.sql` - Output Parity and Logic
*   **Purpose:** Verify `bert_nna_voice.sql` produces identical output to its legacy counterpart (`d_exis_apt_nna_voice.sql`), including complex joins, `MONATS_ID` filtering, `GUELTIG_BIS` filter, `ROUND` for numeric conversions, `CONCAT` for string concatenation, and the specific `LEISTUNGSKLASSE_ID` conditional logic.
*   **Setup:**
    *   Populate BigQuery source tables (`DWH_VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH_VI_C_VERTRAG`, `DWH_VI_F_NNV_TVD_12_MONATE`, `DWH_VI_L_TVD_LEISTUNGSKLASSE`) with a representative snapshot of legacy data, including edge cases for `LEISTUNGSKLASSE_ID` conditions.
    *   Obtain the "golden record" output from the legacy `d_exis_apt_nna_voice.sql` for a specific `execution_date` (e.g., `2023-01-01`), ensuring the legacy `MONATS_ID` filter matches `202301`.
*   **Action:**
    1.  Execute the `bert_nna_voice` task (or run the SQL directly) with `ds_nodash` set to `20230101`.
    2.  Query the resulting `TARGET_BQ_PROJECT.TARGET_BQ_DATASET.bert_nna_voice` table.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The row count of `bert_nna_voice` matches the legacy output.
        *   A deep comparison shows no differences in data content.
        *   Specifically, verify:
            *   `NNA.RAHMENVERTRAG IS NOT NULL` filter is applied.
            *   `NNA.MONATS_ID = CAST('{{ ds_nodash[:6] }}' AS INT64)` correctly filters for the month.
            *   `TAR.GUELTIG_BIS = PARSE_DATE('%Y%m%d', '47121231')` filter is applied.
            *   `ROUND(NNA.DAUER_SEK / 60, 2)` and `ROUND(NNA.RBETRAG_VBUD_NETTO_CENT / 100, 2)` perform correct numeric conversions and rounding.
            *   The complex `LEISTUNGSKLASSE_ID` conditions are correctly applied:
                *   `(TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))`
                *   `OR (LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6 AND TVD.LEISTUNGSKLASSE_ID < 699999 AND TRUNC(CAST(TVD.LEISTUNGSKLASSE_ID AS BIGNUMERIC) / 1000) <> 622)`
    *   **Fail:** Any discrepancy in row count or data content.

#### Test Case 2.4: `bert_rabattdaten.sql` - Output Parity and Logic
*   **Purpose:** Verify `bert_rabattdaten.sql` produces identical output to its legacy counterpart (`d_exis_apt_rabattdaten.sql`), including joins, `STRING_AGG` aggregation, and handling of `DISTINCT` within the subquery.
*   **Setup:**
    *   Populate BigQuery source tables (`RPT_TA_S_D1_VERTRAG`, `RPT_TA_S_D1_DISCOUNT_RR`, `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`) with a representative snapshot of legacy data.
    *   Obtain the "golden record" output from the legacy `d_exis_apt_rabattdaten.sql`.
*   **Action:**
    1.  Execute the `bert_rabattdaten` task (or run the SQL directly).
    2.  Query the resulting `TARGET_BQ_PROJECT.TARGET_BQ_DATASET.bert_rabattdaten` table.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The row count of `bert_rabattdaten` matches the legacy output.
        *   A deep comparison shows no differences in data content.
        *   Specifically, verify:
            *   The joins between `RPT`, `DISC`, `BPR`, and `OPT` are correct.
            *   `STRING_AGG(BPR_ID, ',' ORDER BY BPR_ID)` correctly aggregates multiple `BPR_ID`s.
            *   The `DISTINCT` clause in the subquery correctly eliminates duplicate rows before aggregation.
    *   **Fail:** Any discrepancy in row count or data content.

### 3. External System Replacements Tests

#### Test Case 3.1: `r_exis_v2.py` - BigQuery to GCS Export
*   **Purpose:** Verify that the `r_exis_v2.py` script correctly exports data from BigQuery tables to Google Cloud Storage in the specified format, mimicking the legacy `r_exis_v2` shell script's functionality.
*   **Setup:**
    *   Ensure the `bert_bestandsdaten`, `bert_nna_daten`, `bert_nna_voice`, and `bert_rabattdaten` tables are populated in BigQuery (e.g., by running the respective SQL tasks).
    *   A GCS bucket (`GCS_EXPORT_BUCKET`) is configured and accessible by the Airflow service account.
    *   Obtain the "golden record" export files from the legacy `r_exis_v2` script for comparison.
*   **Action:**
    1.  Run the `dwh_apt_export_taeglich_jp_group` TaskGroup, which includes the `export_*_to_gcs` PythonOperator tasks.
    2.  Inspect the specified GCS bucket (`gs://{{ var.value.GCS_EXPORT_BUCKET }}/exports/`).
    3.  Download the exported files (e.g., `bert_bestandsdaten_20230101.csv`).
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   Four CSV files (`bert_bestandsdaten_*.csv`, `bert_nna_daten_*.csv`, `bert_nna_voice_*.csv`, `bert_rabattdaten_*.csv`) are created in the correct GCS path.
        *   The file names correctly incorporate the `ds_nodash` value.
        *   The content of each exported CSV file matches the corresponding BigQuery table data.
        *   The CSV format (delimiter, header presence) matches the legacy export format.
        *   The row count in each exported file matches the row count in its source BigQuery table.
    *   **Fail:** Files are missing, incorrectly named, have incorrect content, or the format is wrong.

    ```python
    # Example pytest assertion for file content (conceptual, requires GCS client and file reading)
    import pytest
    from google.cloud import storage
    import pandas as pd

    def test_gcs_export_content(gcs_bucket_name, gcs_path, bq_project, bq_dataset, bq_table, legacy_file_path):
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_path)
        
        # Download exported file
        exported_data = blob.download_as_text()
        exported_df = pd.read_csv(io.StringIO(exported_data))

        # Query BQ table
        bq_client = bigquery.Client(project=bq_project)
        query = f"SELECT * FROM `{bq_project}.{bq_dataset}.{bq_table}`"
        bq_df = bq_client.query(query).to_dataframe()

        # Load legacy file (assuming it's also CSV)
        legacy_df = pd.read_csv(legacy_file_path)

        # Compare
        pd.testing.assert_frame_equal(exported_df, bq_df, check_dtype=False, check_exact=False) # Check against BQ
        pd.testing.assert_frame_equal(exported_df, legacy_df, check_dtype=False, check_exact=False) # Check against legacy
    ```

#### Test Case 3.2: `bert_log.py` - Cloud Logging Integration
*   **Purpose:** Verify that the `bert_log.py` script correctly sends log messages to Google Cloud Logging, replacing the legacy UC4 internal logging.
*   **Setup:**
    *   `bert_log.py` is deployed.
    *   Cloud Logging is enabled for the GCP project.
*   **Action:**
    1.  Trigger a task that calls `bert_log.py` (e.g., the `bert_log` task within `bert_monatlich_jp_group`).
    2.  Navigate to Cloud Logging in the GCP console.
    3.  Filter logs for the Airflow DAG run and the specific task.
*   **Pass/Fail Criterion:**
    *   **Pass:** The log message "Running BERT_LOG for monthly job plan." (or any message passed to the script) appears in Cloud Logging with the correct severity level (INFO).
    *   **Fail:** The log message is not found, or its content/severity is incorrect.

#### Test Case 3.3: `sql_runner.py` - Generic SQL Execution
*   **Purpose:** Verify that the `sql_runner.py` script can successfully execute a BigQuery SQL file, replacing the legacy `SQL.KSH` if it had similar functionality.
*   **Setup:**
    *   `sql_runner.py` is deployed.
    *   Create a simple test SQL file (e.g., `test_query.sql`) that creates a dummy table or inserts data.
    *   `test_query.sql`: `CREATE OR REPLACE TABLE `{{ var.value.TARGET_BQ_PROJECT }}.{{ var.value.TARGET_BQ_DATASET }}.test_sql_runner` AS SELECT 1 AS id, 'test' AS value;`
*   **Action:**
    1.  Execute `sql_runner.py` via a `PythonOperator` or directly from a shell, passing the project ID and the path to `test_query.sql`.
    2.  Check BigQuery for the existence and content of the `test_sql_runner` table.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `test_sql_runner` table is created in BigQuery with the expected schema and data.
    *   **Fail:** The table is not created, or its content is incorrect.

### 4. Data Quality, Row Count, and Schema Assertions

#### Test Case 4.1: Source Table Schema Validation
*   **Purpose:** Verify that the DDLs for the BigQuery source tables (`ddl/source_tables.sql`) accurately reflect the schema of the legacy Oracle source tables. This is critical for ensuring data ingestion and subsequent transformations are correct.
*   **Setup:**
    *   Access to the legacy Oracle database schema definitions for all referenced tables (e.g., `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, etc.).
    *   The BigQuery source tables are created using `ddl/source_tables.sql`.
*   **Action:**
    1.  Compare the column names, data types, nullability, and primary/foreign key definitions (if applicable) from the legacy Oracle schema with the BigQuery DDLs.
    2.  Load a small sample of data into the BigQuery source tables and check for type conversion errors or data truncation.
*   **Pass/Fail Criterion:**
    *   **Pass:** All column names, data types, and nullability constraints in the BigQuery DDLs precisely match their Oracle counterparts, or any deviations are explicitly documented and approved as part of the migration strategy (e.g., `NUMBER` to `BIGNUMERIC`). Sample data loads successfully without errors.
    *   **Fail:** Discrepancies in schema that could lead to data loss, corruption, or incorrect query results.

#### Test Case 4.2: Target Table Schema Validation
*   **Purpose:** Verify that the schema of the generated BigQuery target tables (`bert_bestandsdaten`, `bert_nna_daten`, `bert_nna_voice`, `bert_rabattdaten`) matches the expected output schema of the legacy job.
*   **Setup:**
    *   The BigQuery SQL tasks have been executed, creating the target tables.
    *   Obtain the schema definition of the output files/tables from the legacy job.
*   **Action:**
    1.  Query the schema of the BigQuery target tables (e.g., `SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'bert_bestandsdaten'`).
    2.  Compare this schema with the legacy output schema.
*   **Pass/Fail Criterion:**
    *   **Pass:** Column names, data types, and order (if relevant for file exports) of the BigQuery target tables match the legacy output.
    *   **Fail:** Any mismatch in schema that would break downstream consumers or indicate incorrect transformation.

#### Test Case 4.3: Row Count Parity for Transformed Tables
*   **Purpose:** Verify that the number of rows in each migrated BigQuery target table is identical to the number of rows produced by the corresponding legacy job component for the same input data.
*   **Setup:**
    *   BigQuery source tables are populated with the golden record data.
    *   The `dwh_apt_export_taeglich_jp_group` TaskGroup has completed successfully.
    *   Obtain the row counts from the legacy output files/tables.
*   **Action:**
    1.  For each target table (`bert_bestandsdaten`, `bert_nna_daten`, `bert_nna_voice`, `bert_rabattdaten`), execute a `SELECT COUNT(*) FROM ...` query in BigQuery.
    2.  Compare these counts with the legacy row counts.
*   **Pass/Fail Criterion:**
    *   **Pass:** The row count for each BigQuery target table exactly matches the row count of its legacy counterpart.
    *   **Fail:** Any discrepancy in row counts.

    ```sql
    -- Example SQL assertion for row count parity
    SELECT
      (SELECT COUNT(*) FROM `{{ var.value.TARGET_BQ_PROJECT }}.{{ var.value.TARGET_BQ_DATASET }}.bert_bestandsdaten`) AS migrated_count,
      (SELECT COUNT(*) FROM `{{ var.value.LEGACY_OUTPUT_BQ_PROJECT }}.{{ var.value.LEGACY_OUTPUT_BQ_DATASET }}.legacy_bert_bestandsdaten`) AS legacy_count
    HAVING
      migrated_count = legacy_count;
    ```

#### Test Case 4.4: Column-Level Data Integrity and NULL Handling
*   **Purpose:** Verify that critical columns in the target tables contain valid data, adhere to expected ranges/formats, and that NULL values are handled correctly as per the transformation logic.
*   **Setup:**
    *   BigQuery source tables are populated with golden record data, including edge cases (e.g., NULLs in source, boundary values, invalid formats).
    *   The `dwh_apt_export_taeglich_jp_group` TaskGroup has completed successfully.
*   **Action:**
    1.  For each target table, run specific SQL queries to check data integrity:
        *   Check for unexpected NULLs in columns expected to be NOT NULL.
        *   Check for values outside expected ranges (e.g., negative `GESAMTVOLUMEN_BYTE` if not allowed).
        *   Validate date formats (e.g., `VERTRAGSBEGINN` in `bert_bestandsdaten`).
        *   Verify `STRING_AGG` output does not contain leading/trailing delimiters or unexpected NULLs if source values are NULL.
        *   Verify `ROUND` operations produce expected precision.
        *   Verify `CONCAT` operations handle NULLs gracefully (e.g., `CONCAT('A', NULL, 'B')` should result in 'A,B' or 'A,,B' depending on desired behavior, currently it would be 'A,,B').
*   **Pass/Fail Criterion:**
    *   **Pass:** All data integrity checks pass, and NULL handling aligns with the legacy system's behavior or explicit design decisions.
    *   **Fail:** Any data integrity violation or incorrect NULL handling.

    ```sql
    -- Example SQL assertion for bert_bestandsdaten: check for unexpected NULLs and date format
    SELECT COUNT(*)
    FROM `{{ var.value.TARGET_BQ_PROJECT }}.{{ var.value.TARGET_BQ_DATASET }}.bert_bestandsdaten`
    WHERE
      RAHMENVERTRAG_ID IS NULL OR
      TARIF_ID IS NULL OR
      T_MOBILE_KUNDENNUMMER IS NULL OR
      KUNDENKONTO IS NULL OR
      MSISDN IS NULL OR
      VERTRAGSBEGINN IS NULL OR -- VERTRAGSBEGINN is formatted string, should not be NULL if source is not.
      NOT REGEXP_CONTAINS(VERTRAGSBEGINN, r'^\d{2}\.\d{2}\.\d{4}$') OR -- Check date format
      BASISPRODUKTE IS NULL; -- STRING_AGG should not produce NULL unless all aggregated values are NULL

    -- Example SQL assertion for bert_nna_daten: check for negative volumes
    SELECT COUNT(*)
    FROM `{{ var.value.TARGET_BQ_PROJECT }}.{{ var.value.TARGET_BQ_DATASET }}.bert_nna_daten`
    WHERE
      GESAMTVOLUMEN_BYTE < 0 OR RBETRAG_VBUD_NETTO_EURO_VOL < 0;
    ```

---