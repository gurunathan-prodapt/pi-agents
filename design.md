# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

## 1. Purpose & Scope
This job, primarily driven by the `k_ausd_v_ta_cntrct_valid.ksh` KornShell script, serves as a control script for processing contract validity data. Its main purpose is to orchestrate the execution of an Oracle SQL script (`d_ausd_v_ta_cntrct_valid.sql`) that extracts and loads contract validity information into a target table. The script handles environment setup, parameter parsing, invocation of the SQL logic, and basic logging, including a mechanism to ignore active jobs and deactivate old ones. The scope of this migration is to re-implement this ETL workflow on the Google Cloud BigQuery platform.

## 2. Source Inventory
The job comprises two primary components:

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh**
    *   **Technology:** KornShell (ksh)
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Description:** Orchestration script. Handles environment, parameter passing, and execution of the embedded SQL logic. Includes error handling and logging.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql**
    *   **Technology:** Oracle SQL/PLSQL
    *   **Complexity Tier:** (Implicitly part of the overall "medium" complexity for the job)
    *   **Automation Bucket:** (Implicitly part of the overall "semi_auto" bucket for the job)
    *   **Description:** Data manipulation script. Connects to an external Oracle database via DB link, truncates a staging table, and inserts contract validity data based on a derived cutoff date.

## 3. Target Architecture
The target architecture on Google Cloud will leverage BigQuery for data storage and processing.

*   **BigQuery Stored Procedure:** The core logic of both the shell script and the SQL script will be encapsulated within a single BigQuery SQL stored procedure. This procedure will accept parameters, derive the cutoff date, perform the data load, and handle error conditions.
*   **BigQuery Datasets:**
    *   `project.isbert_schema`: Will house the `dwtk_meldungen` table (or its migrated equivalent) which is used to determine the processing cutoff date.
    *   `project.source_dataset`: Will host the `cds_ta_cntrct_validity` table, either as a native BigQuery table or an external table linked to a source system.
    *   `project.dataset`: Will contain the target staging table `sof_ta_cntrct_valid` and potentially a `job_audit_log` table.
*   **Orchestration:** Cloud Composer (managed Airflow) or native BigQuery scheduling will be used to invoke the BigQuery stored procedure, handling parameter passing and dependency management.
*   **Logging:** Cloud Logging will capture execution details and errors from the BigQuery stored procedure. A dedicated `job_audit_log` table in BigQuery can be used for custom auditing and record counts.

## 4. Data Flow & Lineage
The data flow for this migrated job will be as follows:

1.  **Orchestration Trigger:** The BigQuery stored procedure (e.g., `project.dataset.r_ausd_vertrag`) is triggered by a scheduler (e.g., Cloud Composer).
2.  **Parameter Injection:** `p_JobKennung` and `p_EintragsNr` parameters are passed to the stored procedure.
3.  **Cutoff Date Determination:** The stored procedure queries `project.isbert_schema.dwtk_meldungen` to determine the `v_datum` (cutoff date) based on the latest `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
4.  **Target Table Preparation:** The target table `project.dataset.sof_ta_cntrct_valid` is truncated.
5.  **Data Extraction & Loading:** Data is extracted from `project.source_dataset.cds_ta_cntrct_validity` and inserted into `project.dataset.sof_ta_cntrct_valid`.
6.  **Filtering:** The extraction includes filtering based on `insert_at` and `modified_at` relative to `v_datum`.
    *   `DATE(cv.insert_at) <= v_datum`
    *   `cv.modified_at IS NULL OR DATE(cv.modified_at) > v_datum`
7.  **Record Count & Audit:** The number of loaded records is counted, and an entry is made into an optional `project.dataset.job_audit_log` table.
8.  **Completion:** The stored procedure completes, and the orchestration workflow marks the job as successful.

## 5. Transformation Logic
The core transformation logic resides within the SQL portion of the job:

*   **Parameter Derivation:** The `v_datum` (cutoff date) is dynamically determined by querying the `isbert_schema.dwtk_meldungen` table. This value is crucial for filtering the source data.
*   **Truncate and Load:** The target table `sof_ta_cntrct_valid` is subjected to a full refresh. It's first truncated, then populated with data directly from `cds_ta_cntrct_validity`.
*   **Column Mapping:**
    *   `cntrct_validity_id` -> `cntrct_validity_id`
    *   `first_period_id` -> `first_period_id`
    *   `following_period_id` -> `following_period_id`
    *   `first_notice_period_id` -> `first_notice_period_id`
    *   `follow_notice_period_id` -> `follow_notice_period_id`
    *   `insert_at` -> `bfc_age` (Implicit renaming/aliasing during insertion).
*   **Date Filtering:** Records are selected from `cds_ta_cntrct_validity` only if their `insert_at` date is less than or equal to `v_datum` AND their `modified_at` is either NULL or greater than `v_datum`. This ensures a snapshot-like behavior based on the derived cutoff date.
*   **No Aggregations:** The current logic does not involve complex aggregations or joins, making the direct translation to BigQuery straightforward.

## 6. External Dependencies
The original job has the following external dependencies:

*   **Oracle Database (`@pcrs1`):** The primary source of data, `cds$ta_cntrct_validity`, is accessed via an Oracle DB link. Also, `isbert_schema.dwtk_meldungen` and the `isbert_schema.DWPA_UTIL_SKRIPT` stored procedure are on an Oracle instance.
    *   **Replacement Strategy:**
        *   **`cds$ta_cntrct_validity`:** This table must be made available in BigQuery. This can be achieved through:
            1.  **Batch Data Transfer:** Regular scheduled transfers (e.g., daily) from Oracle to BigQuery using services like Cloud Data Fusion, Dataflow, or custom ETL scripts.
            2.  **Federated Queries:** If real-time or near real-time access is critical and data volume permits, BigQuery Federated Queries to Cloud SQL (which mirrors Oracle) could be considered, though this is generally less performant than native BigQuery tables.
            3.  **BigQuery Native Table:** The recommended approach is to migrate this source table into a native BigQuery table.
        *   **`isbert_schema.dwtk_meldungen`:** Similar to `cds$ta_cntrct_validity`, this table should be migrated to a native BigQuery table.
        *   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** The `TRUNCATE TABLE` functionality provided by this procedure will be replaced by native BigQuery `TRUNCATE TABLE` statements within the stored procedure.
*   **KornShell Utilities:** Scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` provide error handling, date functions, parameter parsing, and SQL execution wrappers.
    *   **Replacement Strategy:**
        *   **Error Handling & Parameter Parsing:** Handled natively by BigQuery stored procedure logic (e.g., `RAISE` statements, `DECLARE` variables, `IF` conditions).
        *   **Date Functions:** Replaced by BigQuery's rich set of date and time functions.
        *   **SQL Execution:** The entire SQL execution logic is directly embedded in the BigQuery stored procedure.
*   **Local Filesystem (`$DW_DIR_UTL`, `./tmp`):** Used for temporary files (`tmpFile`) and SQL spool outputs.
    *   **Replacement Strategy:** Temporary record counts will be handled by BigQuery `DECLARE` variables and optionally persisted to an audit table. Spool outputs will be replaced by BigQuery's built-in logging or a custom logging mechanism writing to BigQuery tables.

## 7. Unresolved / Risks
*   **Complex Shell Logic:** While the core logic is migrating to BigQuery SQL, any non-trivial shell scripting outside the direct SQL invocation (e.g., complex file manipulations, external program calls) not explicitly covered by the design needs careful review and re-implementation in Python/Cloud Functions or other suitable GCP services. In this case, the shell script is primarily an orchestrator, so direct conversion to a BigQuery stored procedure should cover most of the logic.
*   **Environment Variables:** The reliance on `HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL` implies an existing environment setup. These will need to be configured as environment variables in the orchestration tool (e.g., Airflow variables) or as parameters to the BigQuery stored procedure.
*   **Oracle `TO_DATE` Function:** The exact date format strings in `TO_DATE` (`YYYYMMDD`) must be accurately replicated in BigQuery's date functions to ensure correct date parsing.
*   **SQL `PROMPT` and `COLUMN ... NEW_VALUE`:** These are Oracle SQL*Plus specific commands for user interaction and variable assignment. They have no direct BigQuery equivalent and will be replaced by BigQuery's variable declaration and assignment (`DECLARE`, `SET`).
*   **Oracle PL/SQL `runstatement`:** The invocation of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE TABLE` will be directly replaced by BigQuery's native `TRUNCATE TABLE` statement.
*   **Data Latency:** If the Oracle source data (e.g., `cds_ta_cntrct_validity`) is migrated via batch transfer, consider the impact on data freshness if the original job ran more frequently than the batch transfer.

## 8. Build Plan

The migration will involve the following steps:

1.  **Data Ingestion for Source Tables:**
    *   **Source:** Oracle `isbert_schema.dwtk_meldungen`
    *   **Target:** BigQuery table `project.isbert_schema.dwtk_meldungen`
    *   **Method:** Establish a batch data transfer pipeline (e.g., Cloud Data Fusion, Dataflow) to move data from Oracle to BigQuery.
    *   **Source:** Oracle `cds$ta_cntrct_validity` (from Carmen DB)
    *   **Target:** BigQuery table `project.source_dataset.cds_ta_cntrct_validity`
    *   **Method:** Establish a batch data transfer pipeline from Oracle to BigQuery.
2.  **BigQuery Target Table Creation:**
    *   **Language:** BigQuery DDL
    *   **File:** `bq_ddl_sof_ta_cntrct_valid.sql`
    *   **Content:** Create the `project.dataset.sof_ta_cntrct_valid` table with appropriate schema (derived from the `INSERT` statement in `d_ausd_v_ta_cntrct_valid.sql`).
    *   **Content:** (Optional) Create `project.dataset.job_audit_log` table for auditing.
3.  **BigQuery Stored Procedure Generation:**
    *   **Language:** BigQuery SQL
    *   **File:** `bq_sp_r_ausd_vertrag.sql`
    *   **Content:** Implement the logic derived from `k_ausd_v_ta_cntrct_valid.ksh` and `d_ausd_v_ta_cntrct_valid.sql` as a BigQuery stored procedure, `CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag(...)`. This will include:
        *   Parameter declarations (`p_JobKennung`, `p_EintragsNr`).
        *   Variable declarations (`v_datum`, `v_records`).
        *   Parameter validation logic.
        *   Query to `project.isbert_schema.dwtk_meldungen` for `v_datum`.
        *   `TRUNCATE TABLE project.dataset.sof_ta_cntrct_valid;`
        *   `INSERT INTO project.dataset.sof_ta_cntrct_valid SELECT ... FROM project.source_dataset.cds_ta_cntrct_validity ...` with the correct filtering and column mapping.
        *   `SELECT COUNT(*)` into `v_records`.
        *   Optional `INSERT` into `project.dataset.job_audit_log`.
4.  **Orchestration (Cloud Composer/Airflow DAG):**
    *   **Language:** Python
    *   **File:** `composer_dag_k_ausd_v_ta_cntrct_valid.py`
    *   **Content:** A Cloud Composer DAG that:
        *   Defines parameters for `p_JobKennung` and `p_EintragsNr`.
        *   Uses `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator` to call the `project.dataset.r_ausd_vertrag` stored procedure.
        *   Configures scheduling, retry mechanisms, and notifications.
5.  **Testing and Validation:**
    *   Develop test cases to ensure data integrity and functional equivalence between the legacy and BigQuery implementations.
    *   Verify cutoff date logic, record counts, and target table content.