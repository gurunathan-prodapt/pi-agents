# Migration Design — DW.BERT_AUSD_V_TA_VERTRAG_TMP

## 1. Purpose & Scope

The legacy job `DW.BERT_AUSD_V_TA_VERTRAG_TMP` is responsible for collecting and transforming contract-related information from various source systems into a temporary staging table named `sof$ta_vertrag_tmp`. This temporary table appears to be a crucial intermediate step for further reporting or processing within the `BERT` data warehouse domain.

The migration objective is to re-platform this ETL workflow from its current UC4 (Automic) scheduled KornShell and Oracle PL/SQL environment to Google Cloud Platform, utilizing Airflow for orchestration and BigQuery for data storage and transformation. The core transformation logic embedded in the Oracle SQL script will be converted to BigQuery SQL, and the shell scripting orchestration will be re-implemented using Python within the Airflow DAG.

## 2. Source Inventory

The `DW.BERT_AUSD_V_TA_VERTRAG_TMP` job is composed of the following files:

*   **UC4 Job Definition:**
    *   **Relative Path:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml`
    *   **Technology:** UC4/Automic
    *   **Role:** Job scheduler and orchestrator. It initiates the wrapper KornShell script.
    *   **Complexity Tier/Automation Bucket:** N/A (data not available in analysis tables).

*   **Wrapper KornShell Script:**
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh`
    *   **Technology:** KornShell
    *   **Role:** This script acts as a wrapper, handling environment initialization (`. $HOME/.dw_init`), parameter parsing, error trapping, and then invoking the core KornShell script `k_ausd_v_ta_vertrag_tmp.ksh`.
    *   **Complexity Tier/Automation Bucket:** N/A (data not available in analysis tables).

*   **Core KornShell Script:**
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh`
    *   **Technology:** KornShell
    *   **Role:** This script controls the execution of the primary Oracle PL/SQL script. It handles SQL*Plus execution routines (sourcing `h_alis_sqlplus.ksh`), sets up logging, and passes parameters to the SQL script.
    *   **Complexity Tier/Automation Bucket:** N/A (data not available in analysis tables).

*   **Oracle PL/SQL Script:**
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql`
    *   **Technology:** Oracle PL/SQL
    *   **Role:** Contains the core data extraction and transformation logic. It truncates a temporary table (`sof$ta_vertrag_tmp`) and inserts data into it from multiple source tables and views via a complex `SELECT` statement with `UNION ALL`.
    *   **Complexity Tier/Automation Bucket:** N/A (data not available in analysis tables).

## 3. Target Architecture

The migrated job will be implemented as an Airflow DAG on Google Cloud, leveraging BigQuery for all data storage and processing.

*   **Orchestration:** Apache Airflow DAG (`dag_id: dw_bert_ausd_v_ta_vertrag_tmp`).
*   **Compute:** BigQuery for SQL transformations. Python will be used for any scripting logic that replaces the KornShell environment setup, parameter handling, and job execution.
*   **Data Storage:** BigQuery datasets will house all source and target tables. The target temporary table `sof$ta_vertrag_tmp` will be created in an appropriate BigQuery dataset (e.g., `bert_staging.ta_vertrag_tmp`).
*   **Logging:** Airflow's native logging capabilities will be used, potentially integrated with Cloud Logging.

## 4. Data Flow & Lineage

The execution flow will be translated as follows:

1.  **Airflow DAG Trigger:** The `dw_bert_ausd_v_ta_vertrag_tmp` Airflow DAG will be scheduled or manually triggered.
2.  **Initialization and Parameter Task:** A PythonOperator within the DAG will handle the environment initialization and parameter extraction logic previously performed by `r_ausd_v_ta_vertrag_tmp.ksh` and `k_ausd_v_ta_vertrag_tmp.ksh`. This includes determining the `v_datum` variable from a BigQuery equivalent of `isbert_schema.dwtk_meldungen`.
3.  **BigQuery Data Transformation Task:** A BigQueryOperator will execute the converted BigQuery SQL script (`d_ausd_v_ta_vertrag_tmp.sql`). This task will:
    *   Truncate the target BigQuery table `bert_staging.ta_vertrag_tmp`.
    *   Insert the transformed data into `bert_staging.ta_vertrag_tmp`.

The data lineage will involve:
*   **Source Tables (Oracle -> BigQuery):**
    *   `isbert_schema.dwtk_meldungen` -> `bert_source.dwtk_meldungen` (or equivalent)
    *   `sof$ta_cntrct_crs3` -> `bert_source.ta_cntrct_crs3`
    *   `sof$ta_bp_ref` -> `bert_source.ta_bp_ref`
    *   `sof$ta_inv_acc` -> `bert_source.ta_inv_acc`
    *   `dwh$vi_s_rd_segment` -> `bert_source.vi_s_rd_segment`
    *   `sof$ta_notice` -> `bert_source.ta_notice`
    *   `sof$ta_barrier_zusgf` -> `bert_source.ta_barrier_zusgf`
    *   `sof$ta_cntrct_templ` -> `bert_source.ta_cntrct_templ`
    *   `sof$ta_cntrct_valid` -> `bert_source.ta_cntrct_valid`
    *   `sof$ta_period` -> `bert_source.ta_period`
    *   `sof$ta_vvl_upgrade` -> `bert_source.ta_vvl_upgrade`
    *   `sof$ta_apn_ve` -> `bert_source.ta_apn_ve`
    *   `sof$ta_action_assoc` -> `bert_source.ta_action_assoc`
    *   `sof$vi_c_bfc` -> `bert_source.vi_c_bfc`
*   **Target Table (BigQuery):** `bert_staging.ta_vertrag_tmp`

## 5. Transformation Logic

The core transformation logic from `d_ausd_v_ta_vertrag_tmp.sql` will be directly translated into BigQuery SQL. Key transformations include:

*   **Date Variable (`v_datum`):** The Oracle `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') ...` will be converted to a BigQuery `DECLARE`/`SET` statement using `COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')`.
*   **Target Table Truncation:** `TRUNCATE TABLE sof$ta_vertrag_tmp` will become `TRUNCATE TABLE \`bert_staging.ta_vertrag_tmp\``.
*   **Outer Joins:** All Oracle outer join syntax `(+)` will be converted to explicit `LEFT JOIN` clauses.
*   **Conditional Logic:** Oracle `DECODE` functions will be translated to BigQuery `CASE` statements. Complex `CASE` statements for `upgradeberechtigt` and `VDA` will be preserved with appropriate function mappings.
*   **Date Functions:** `MONTHS_BETWEEN` will be replaced with `DATE_DIFF(..., MONTH)`. `TO_DATE` and `TO_CHAR` for date formatting will be replaced with BigQuery equivalents like `PARSE_DATE` and `FORMAT_DATE`.
*   **Parallel Hints:** Oracle `/*+ parallel(...,4) */` hints will be removed as BigQuery automatically handles parallelism.
*   **Column Mappings:** All explicit column aliases (e.g., `c.cntrct_id VERTRAG_ID_CARMEN`) will be explicitly defined with `AS` (e.g., `c.cntrct_id AS vertrag_id_carmen`).
*   **`UNION ALL`:** The two main `SELECT` statements joined by `UNION ALL` will be preserved, ensuring both sets of contract types (`c.cntrct_ty <> 20` and `c.cntrct_ty = 20`) are processed and combined.

The BigQuery SQL script will be self-contained, including the variable declaration and the `INSERT INTO ... SELECT` statement.

## 6. External Dependencies

The original job has the following external dependencies:

*   **Oracle Database:** The primary source and target for the SQL operations. This will be fully replaced by Google BigQuery. Data will need to be migrated from the source Oracle tables/views into corresponding BigQuery tables (e.g., `bert_source.ta_cntrct_crs3`, `bert_source.vi_s_rd_segment`).
*   **UC4/Automic Scheduler:** Replaced by Apache Airflow.
*   **Legacy File System (`$HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL`):** Used for storing scripts, environment files (`.dw_init`), and temporary files (`tmpFile`). These will be replaced by:
    *   Airflow DAG definition files (Python).
    *   Environment variables or Airflow Variables/Connections for configuration.
    *   BigQuery for temporary data if needed, or in-memory processing within Python tasks. Temporary log files will be handled by Airflow's logging.
*   **`isbert_schema.dwtk_meldungen`:** This table is used to determine the `v_datum` (cutoff date). A corresponding table in BigQuery (e.g., `bert_source.dwtk_meldungen`) will be required.
*   **SQL*Plus and `h_alis_sqlplus.ksh`:** The execution of SQL scripts via `sqlplus` and the custom `starteSQLSkript` function will be replaced by Airflow's `BigQueryOperator`.

## 7. Unresolved / Risks

*   **Missing Complexity/Automation Data:** The `file_complexity` and `automation_rate` tables provided no data for this job's components. Therefore, the migration effort estimation and specific flags for potential challenges are not available.
*   **`DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`:** The UC4 XML includes these objects. Their exact functionality is unknown, but they appear to be includes. `DW.HOLE_PFAD` likely defines environment variables, and `DW.BERT_LESE_LOG` might be a logging mechanism. These functionalities need to be replicated in the Airflow DAG (e.g., setting Airflow Variables, using Airflow's native logging).
*   **`$HOME/.dw_init`:** This environment initialization file sourced by both KornShell scripts is critical. Its contents must be analyzed to identify necessary environment variables, paths, or configurations that need to be replicated in the Airflow environment (e.g., as Airflow Variables or within Python code).
*   **Source Table Availability and Schema:** The migration assumes that all source tables/views (e.g., `sof$ta_cntrct_crs3`, `dwh$vi_s_rd_segment`) from the Oracle database will be ingested into BigQuery with compatible schemas. Data type mapping and nullability constraints need to be verified.
*   **`v_carmen` DB-Link:** The Oracle SQL uses `DEFINE v_carmen = "@pcrs1"`. This indicates a database link to `pcrs1`. This external connection needs to be identified and handled; if `pcrs1` is another Oracle database, its data must also be migrated or made accessible in BigQuery.
*   **`isbert_schema.dwtk_meldungen`:** This table is crucial for the `v_datum` calculation. Its schema and data content must be accurately replicated in BigQuery.
*   **Error Handling and Logging:** The KornShell scripts use custom error handling (`f_alis_msgerr.ksh`, `DWMSG_` functions) and logging. While Airflow has built-in mechanisms, a custom `on_failure_callback` or equivalent might be needed to replicate specific legacy error reporting.
*   **Performance Tuning:** Oracle `parallel` hints are removed. BigQuery's auto-parallelization is powerful, but performance should be monitored and optimized post-migration.

## 8. Build Plan

The build plan outlines the steps to construct the Airflow DAG and its associated components:

1.  **Migrate Source Data to BigQuery:**
    *   Ingest all Oracle source tables and views (listed in Section 4) into appropriate BigQuery datasets (e.g., `bert_source`, `dwh_source`). Ensure schema compatibility and data integrity.
    *   Create the target staging table `bert_staging.ta_vertrag_tmp` in BigQuery, defining its schema based on the output of the Oracle SQL script.

2.  **Develop Airflow DAG (`dw_bert_ausd_v_ta_vertrag_tmp.py`):**
    *   **DAG Definition:** Create a new Python file for the Airflow DAG.
        *   `dag_id = "dw_bert_ausd_v_ta_vertrag_tmp"`
        *   `schedule = None` (or define a cron schedule if external information becomes available)
        *   `start_date = datetime(YYYY, MM, DD)` (set an appropriate start date)
        *   `catchup = False`
        *   `default_args` including `owner`, `retries`, `retry_delay`.
    *   **Task 1: `initialize_job_parameters` (PythonOperator):**
        *   This task will encapsulate the logic from `. $HOME/.dw_init`, `h_alis_parameter.ksh`, and the `v_datum` calculation from `dwtk_meldungen`.
        *   It will query `bert_source.dwtk_meldungen` to determine `v_datum` and store it in an Airflow XCom or pass it as a Jinja template variable to the subsequent task.
        *   Any other critical environment variables or parameters from `.dw_init` should be handled here.
    *   **Task 2: `execute_contract_transformation` (BigQueryOperator):**
        *   This task will execute the BigQuery SQL script derived from `d_ausd_v_ta_vertrag_tmp.sql`.
        *   The SQL will dynamically use the `v_datum` parameter obtained from the previous task.
        *   Ensure the `TRUNCATE` and `INSERT INTO ... SELECT` statements target the correct BigQuery dataset and table (`bert_staging.ta_vertrag_tmp`).
    *   **Task 3: `handle_job_completion` (PythonOperator / BashOperator):**
        *   Replicate any final logging or success status updates previously handled by `DWMSG_SetzeStatusOK` or `DW.BERT_LESE_LOG`.
    *   **Task Dependencies:** Define the task dependencies within the DAG:
        `initialize_job_parameters >> execute_contract_transformation >> handle_job_completion`

3.  **Refine BigQuery SQL:**
    *   Validate the generated BigQuery SQL for correctness and performance.
    *   Replace placeholder dataset/table names (`sof$ta_...`, `dwh$vi_...`) with actual BigQuery dataset and table names (e.g., `project.dataset.table`).
    *   Address any data type mismatches or implicit conversions that might arise.

4.  **Testing:**
    *   Unit test each Airflow task.
    *   Integration test the entire Airflow DAG on a development environment, comparing output with the legacy system for data accuracy.
    *   Performance test the BigQuery SQL to ensure it meets SLA requirements.

5.  **Deployment:** Deploy the Airflow DAG and associated BigQuery SQL to the production Airflow environment and BigQuery project.