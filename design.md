# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh

## 1. Purpose & Scope

This job is responsible for the reconciliation and processing of contract data, specifically for the `ta_barrier` table. It involves an orchestration layer implemented in KornShell and a core data transformation logic implemented in Oracle PL/SQL. The primary purpose is to truncate and then populate the `sof$ta_barrier` table with transformed data from several source tables, applying filtering and reformatting based on a derived date variable.

The migration aims to re-implement this entire workflow on the Google Cloud BigQuery platform, converting all components to BigQuery-native constructs while preserving the original business logic and data integrity.

## 2. Source Inventory

The job consists of three inter-dependent components:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh`
    *   **Technology:** KornShell
    *   **Tier:** Simple (based on analysis of a wrapper script)
    *   **Automation Bucket:** semi_auto (B2)
    *   **Purpose:** Top-level wrapper script. Sets up the environment, handles parameters, initializes logging, and orchestrates the execution of the core processing script (`k_ausd_v_ta_barrier.ksh`).
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier.ksh`
    *   **Technology:** KornShell
    *   **Tier:** Simple (based on analysis of a control script)
    *   **Automation Bucket:** semi_auto (B2)
    *   **Purpose:** Control script, invoked by `r_ausd_v_ta_barrier.ksh`. It handles further parameter parsing, environment setup, error checking, and orchestrates the execution of the actual SQL transformation script (`d_ausd_v_ta_barrier.sql`). It also manages job activation/deactivation logic.
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_barrier.sql`
    *   **Technology:** Oracle PL/SQL
    *   **Tier:** Simple (based on analysis of a data transformation script)
    *   **Automation Bucket:** semi_auto (B2)
    *   **Purpose:** Core data transformation script. This script truncates the target table `sof$ta_barrier` and populates it by selecting and transforming data from various `CDS$TA_BARRIER` related tables. It applies complex filtering and data reformatting, including a `DECODE` statement for `SPERRGRUND` and date-based filtering.

## 3. Target Architecture

The target platform for this migration is Google Cloud BigQuery.

*   **Orchestration Layer:** The KornShell wrapper (`r_ausd_v_ta_barrier.ksh`) and control script (`k_ausd_v_ta_barrier.ksh`) will be migrated to BigQuery Stored Procedures. These procedures will manage job flow, parameter passing, error handling, and logging entirely within BigQuery's scripting capabilities.
*   **Data Transformation Layer:** The Oracle PL/SQL script (`d_ausd_v_ta_barrier.sql`) will be converted into a BigQuery SQL script, ideally encapsulated within its own BigQuery Stored Procedure, leveraging BigQuery's SQL dialect and optimized for its columnar storage and distributed execution.
*   **Logging and Auditing:** Existing file-based logging and Oracle job tables will be replaced by dedicated BigQuery audit/log tables. This will provide centralized, scalable, and queryable logging for job execution status, errors, and metadata.
*   **Source Data:** All source tables (e.g., `cds$ta_barrier`, `isbert_schema.dwtk_meldungen`) currently residing in Oracle via DB links will be ingested and maintained as BigQuery tables. This design assumes these source datasets are already available or will be made available in BigQuery.
*   **Target Data:** The target table `sof$ta_barrier` will be a native BigQuery table.

## 4. Data Flow & Lineage

The migrated job will execute in the following sequence within BigQuery:

1.  **`r_ausd_v_ta_barrier_wrapper` (BigQuery Stored Procedure):** This top-level procedure initiates the job. It will handle initial setup, define job metadata, and log the job start.
2.  **`k_ausd_v_ta_barrier_control` (BigQuery Stored Procedure):** Invoked by the wrapper. This procedure manages the control flow, including parameter validation, job activation/deactivation logic (migrated to BigQuery tables), and error handling. It then triggers the core data transformation.
3.  **`d_ausd_v_ta_barrier_etl` (BigQuery Stored Procedure):** Invoked by `k_ausd_v_ta_barrier_control`. This procedure executes the core data transformation.
    *   It first queries `isbert_schema.dwtk_meldungen` (BigQuery table) to determine the `v_datum` variable.
    *   It then `TRUNCATE`s the target table `sof$ta_barrier` (BigQuery table).
    *   It `INSERT`s data into `sof$ta_barrier` by joining and transforming data from:
        *   `cds$ta_barrier` (BigQuery table)
        *   `cds$ta_barrier_class` (BigQuery table)
        *   `cds$ta_barrier_kind` (BigQuery table)
        *   `cds$ta_care_description` (BigQuery table)
    *   It applies complex `WHERE` clause filtering based on date fields (`insert_at`, `modified_at`, `valid_from`, `valid_to`) and `is_production` flag.
4.  **Logging and Status Updates:** Throughout the execution, dedicated BigQuery logging tables will be updated to reflect job status, errors, and processed record counts.

## 5. Transformation Logic

### a. Orchestration Logic (KornShell to BigQuery Stored Procedures)

*   **Parameter Handling:**
    *   Legacy `getopts` for command-line arguments (e.g., `-j`, `-f`, `-h`) will be replaced by direct input parameters of the BigQuery Stored Procedures.
*   **Environment Setup:**
    *   Sourcing shell scripts like `. $HOME/.dw_init` and utility functions will be replaced by BigQuery script variables, `SET` statements, and potentially configuration data stored in BigQuery tables.
*   **Error Handling:**
    *   Shell `trap` statements and custom error functions (`f_alis_msgerr.ksh`) will be mapped to BigQuery's `EXCEPTION WHEN ERROR THEN` blocks. Errors will be logged to dedicated BigQuery error tables.
*   **Logging:**
    *   Custom logging functions (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`) will be re-implemented as `INSERT` and `UPDATE` statements against BigQuery logging/audit tables (e.g., `project.dataset.job_log`, `project.dataset.job_error_log`).
*   **Script Invocation:**
    *   Calls to other KornShell scripts (`${Name_Kernskript}`) will be replaced by `CALL` statements to invoke the corresponding BigQuery Stored Procedures.
*   **Job Control:**
    *   Logic for ignoring active jobs and deactivating older jobs in `k_ausd_v_ta_barrier.ksh` will be implemented using BigQuery DML (`UPDATE`, `INSERT`) against BigQuery job status tables (e.g., `project.dataset.job_table`) within the control stored procedure.
*   **Temporary Files:**
    *   The use of temporary files (e.g., `$tmpFile` for record counts) will be replaced by BigQuery scripting variables or direct queries against result sets.

### b. Data Transformation Logic (Oracle PL/SQL to BigQuery SQL)

*   **Variable Definitions:**
    *   Oracle `DEFINE v_carmen = "@pcrs1"` will be removed. Source tables will be directly referenced with their full BigQuery `project.dataset.table` paths.
    *   Oracle `COLUMN s_datum new_value v_datum noprint` and the `SELECT` to derive `v_datum` will be converted to a BigQuery `DECLARE v_datum STRING DEFAULT (...)` statement, using BigQuery functions like `FORMAT_DATE` and `PARSE_DATE`.
*   **DDL Operations:**
    *   Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_barrier');` will be replaced by a direct BigQuery `TRUNCATE TABLE sof$ta_barrier;`.
*   **DML Operations (INSERT...SELECT):**
    *   Oracle `NVL` function will be replaced by BigQuery `COALESCE`.
    *   Oracle `DECODE` function (used for `SPERRGRUND`) will be translated to a BigQuery `CASE` expression.
    *   Oracle `TO_DATE` calls in the `WHERE` clause will be replaced by BigQuery `PARSE_DATE` with appropriate format strings.
    *   The `GREATEST` function is directly supported in BigQuery.
    *   `JOIN` conditions and `WHERE` clause predicates will be directly translated to BigQuery SQL syntax.
*   **Oracle Specifics (Removed/Replaced):**
    *   Oracle SQL*Plus commands (`START`, `SPOOL`, `PROMPT`, `WHENEVER SQLERROR`, `SET TIMING`, `SET SERVEROUTPUT`) are not applicable in BigQuery SQL and will be removed. Error handling and logging will be managed by BigQuery scripting features and the new logging tables.
    *   The `commit;` statement is implicitly handled in BigQuery DML or explicit in transactions, and can be removed for single DML statements.

## 6. External Dependencies

### Source System Dependencies

*   **Oracle Database (`@pcrs1`):** The legacy job heavily relies on data from an Oracle database instance referenced via `&v_carmen` (defined as `@pcrs1`). This includes source tables like `cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, `cds$ta_care_description`, and `isbert_schema.dwtk_meldungen`.
*   **Filesystem / Shared Storage:** The KornShell scripts source various utility scripts (e.g., `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`). They also output log files to a local or shared filesystem.

### Target System Replacements (BigQuery)

*   **Oracle Database:** All data previously accessed via the Oracle database link must be migrated to BigQuery. This could involve batch data ingestion (e.g., using BigQuery Data Transfer Service, custom ETL) or real-time replication from Oracle to BigQuery. Once in BigQuery, these will be referenced as native BigQuery tables (e.g., `project.dataset.cds_ta_barrier`).
*   **Filesystem / Shared Storage:**
    *   Sourced utility scripts will be replaced by BigQuery Stored Procedures for shared logic or absorbed into the main migration target procedures. Environment variables will become BigQuery procedure parameters or configuration data in BigQuery tables.
    *   File-based logging will be replaced by `INSERT` statements into BigQuery logging tables (`project.dataset.job_log`, `project.dataset.job_error_log`).

## 7. Unresolved / Risks

*   **Missing Complexity Data:** The `file_complexity` and `migration_flags` tables returned no rows for all components. This suggests that automated complexity analysis may not have run or did not detect specific flags, which could hide unaddressed migration challenges. Manual review is required to ensure no hidden complexities.
*   **Dynamic Behavior:** While the static analysis is robust, any parts of the KornShell scripts that generate SQL dynamically or perform complex OS interactions not explicitly visible in the provided code snippets would require careful manual examination and potentially different migration strategies (e.g., Python UDFs, Cloud Functions).
*   **Oracle Data Migration Strategy:** The plan assumes that all necessary Oracle source data will be fully migrated and available in BigQuery. The specific strategy and timeline for this data migration are critical and need to be defined.
*   **Schema Details for Logging Tables:** The BigQuery logging tables (`job_log`, `job_error_log`, etc.) are conceptual. Their precise schema (column names, data types, partitioning, clustering) needs to be defined and implemented.
*   **Oracle SQL*Plus Features:** While most SQL*Plus features can be omitted or handled by BigQuery scripting, specific interactions with `START ../trace.sql.cfg` and custom `WHENEVER SQLERROR` blocks might have subtle effects that need careful BigQuery equivalent implementation or testing.

## 8. Build Plan

1.  **Define BigQuery Dataset:** Create a dedicated BigQuery dataset for the migrated job artifacts, e.g., `isrpt_isbert_data_processing`.
2.  **Migrate Source Data Schemas:**
    *   Define and create BigQuery schemas for all Oracle source tables: `cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, `cds$ta_care_description`, and `isbert_schema.dwtk_meldungen`.
    *   Define and create the target table: `sof$ta_barrier`.
3.  **Implement BigQuery Logging and Job Control Tables:**
    *   Create BigQuery tables for operational logging and job control, such as:
        *   `project.dataset.job_log` (for general job status, start/end times)
        *   `project.dataset.job_error_log` (for detailed error messages)
        *   `project.dataset.job_table` (to manage active jobs and deactivation logic)
        *   `project.dataset.sql_execution_results` (to store metrics like records processed)
        *   `project.dataset.job_run_summary` (for overall run summaries)
4.  **Develop `d_ausd_v_ta_barrier_etl` Stored Procedure:**
    *   Translate `d_ausd_v_ta_barrier.sql` into a BigQuery Stored Procedure (e.g., `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`).
    *   Implement the `DECLARE v_datum` variable.
    *   Convert `TRUNCATE TABLE` and `INSERT INTO ... SELECT ...` statements, replacing Oracle-specific functions (`NVL`, `DECODE`, `TO_DATE`) with BigQuery equivalents (`COALESCE`, `CASE`, `PARSE_DATE`).
    *   Ensure all table references point to the newly created BigQuery tables.
    *   **(Language: BigQuery SQL)**
5.  **Develop `starteSQLSkript` Auxiliary Procedure:**
    *   Create a BigQuery Stored Procedure (`isrpt_isbert_data_processing.starteSQLSkript`) to encapsulate the job activation/deactivation and SQL execution logic previously handled by the KornShell function. This procedure will call `d_ausd_v_ta_barrier_etl`.
    *   **(Language: BigQuery SQL)**
6.  **Develop `k_ausd_v_ta_barrier_control` Stored Procedure:**
    *   Translate `k_ausd_v_ta_barrier.ksh` into a BigQuery Stored Procedure (e.g., `isrpt_isbert_data_processing.k_ausd_v_ta_barrier_control`).
    *   Implement parameter validation using BigQuery scripting `IF` statements.
    *   Replace shell environment sourcing and error handling with BigQuery constructs and calls to the logging tables.
    *   Integrate calls to `starteSQLSkript` procedure.
    *   **(Language: BigQuery SQL)**
7.  **Develop `r_ausd_v_ta_barrier_wrapper` Stored Procedure:**
    *   Translate `r_ausd_v_ta_barrier.ksh` into a BigQuery Stored Procedure (e.g., `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`).
    *   Implement the top-level orchestration, parameter handling, and logging.
    *   Include a `CALL` statement to invoke `k_ausd_v_ta_barrier_control`.
    *   **(Language: BigQuery SQL)**
8.  **Orchestration (Cloud Composer / Workflows):**
    *   If scheduled execution and external dependency management are required, create a Cloud Composer DAG or a Cloud Workflow definition that orchestrates the `CALL` to the main `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper` BigQuery Stored Procedure.
    *   **(Language: Python for Airflow DAG, YAML for Workflows)**
9.  **Testing and Validation:** Thoroughly test each migrated component and the end-to-end workflow to ensure functional equivalence and performance on BigQuery.