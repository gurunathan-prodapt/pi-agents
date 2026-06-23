# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh

## 1. Purpose & Scope

This migration design document outlines the strategy for re-platforming the legacy ETL job identified by `run_id 6d73ee79-8207-4271-b787-9644c913bf51` and `seed_name vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh` to Google Cloud Platform's BigQuery.

The original job is designed for synchronizing contract data into an Oracle table named `ta_p_vertrag`. It is structured as a multi-layered script execution: a primary KornShell wrapper (`r_ausd_v_ta_p_vertrag.ksh`) orchestrates a secondary KornShell script (`k_ausd_v_ta_p_vertrag.ksh`), which in turn executes a core Oracle SQL*Plus script (`d_ausd_v_ta_p_vertrag.sql`) that performs the actual data transformation.

The scope of this migration includes converting all components of this job: the shell scripting logic for orchestration, environment setup, and error handling, and the core SQL transformation logic, to their respective BigQuery-native or GCP-native equivalents.

## 2. Source Inventory

The assembled job consists of three interconnected files, forming a sequential execution chain:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`**
    *   **Technology:** KornShell (wrapper script)
    *   **Summary:** Framework script for synchronizing contract data in the 'ta_p_vertrag' table, handling environment setup, parameter parsing, and error trapping before calling a core script.
    *   **Complexity Tier:** `medium`
    *   **Migration Automation Bucket:** `semi_auto`
    *   **Role:** Top-level entry point, environment initialization, logging setup, and invocation of the orchestration script.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`**
    *   **Technology:** KornShell (orchestration script)
    *   **Summary:** KornShell script for controlling the execution of a SQL script (`d_ausd_v_ta_p_vertrag.sql`) to process contract data, including parameter parsing, error handling, and job status management.
    *   **Complexity Tier:** `medium`
    *   **Migration Automation Bucket:** `semi_auto`
    *   **Role:** Intermediate orchestration layer, parameter validation, setting up SQL*Plus execution environment, and invoking the core SQL script.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`**
    *   **Technology:** Oracle SQL*Plus (core logic script)
    *   **Summary:** This SQL*Plus script processes twinbill contracts, populating the `SOF$TA_P_VERTRAG` table from a temporary table `SOF$TA_VERTRAG_TMP`. It includes variable definitions, tracing, and cleanup of temporary tables.
    *   **Complexity Tier:** `medium`
    *   **Migration Automation Bucket:** `semi_auto`
    *   **Role:** Performs the main data transformation, reads from source tables, writes to target tables, and manages temporary tables.

## 3. Target Architecture

The migrated job will leverage Google Cloud Platform services, primarily BigQuery for data warehousing and transformations, and Cloud Composer (managed Apache Airflow) for workflow orchestration.

*   **Data Storage and Transformation:** All Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_p_vertrag`, `sof$ta_vertrag_tmp`, and other `sof$ta_*` temporary tables) will be migrated to BigQuery datasets and tables. The core SQL transformation logic will be converted into BigQuery Standard SQL.
*   **Orchestration:** The KornShell wrapper scripts (`r_ausd_v_ta_p_vertrag.ksh` and `k_ausd_v_ta_p_vertrag.ksh`) will be replaced by an Apache Airflow DAG written in Python. This DAG will manage the execution flow, parameter passing, logging, and error handling.
*   **Logging and Monitoring:** BigQuery logs, Cloud Logging, and Airflow's native logging will provide comprehensive operational visibility.
*   **Temporary Data Management:** BigQuery's native capabilities for temporary tables or Common Table Expressions (CTEs) will replace the Oracle temporary table patterns.

**Target Components:**
*   **BigQuery Datasets/Tables:**
    *   `isbert_schema.dwtk_meldungen` (source table, possibly renamed to `isbert_dwh.dwtk_meldungen`)
    *   `sof$ta_p_vertrag` (target table, possibly renamed to `sof_dwh.ta_p_vertrag`)
    *   `sof$ta_vertrag_tmp` (temporary table, possibly `sof_dwh.ta_vertrag_tmp` or implemented as a CTE)
    *   All other `sof$ta_*` tables currently truncated will become BigQuery tables or managed differently based on their usage.
*   **Cloud Composer (Airflow):**
    *   One Python DAG for the entire workflow.
    *   BigQuery Operators (`BigQueryInsertJobOperator`, `BigQueryDeleteTableOperator`, etc.) to execute SQL.
    *   Python Operators for any custom logic or parameter manipulation.

## 4. Data Flow & Lineage

**Current Data Flow (Oracle/KornShell):**

1.  `r_ausd_v_ta_p_vertrag.ksh` starts:
    *   Initializes environment variables.
    *   Sets up custom error handling and logging (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    *   Determines `JobKennung` and `DW_EintragsNr` for logging.
    *   Invokes `k_ausd_v_ta_p_vertrag.ksh` with parameters `-j $JobKennung -f $DW_EintragsNr`.
2.  `k_ausd_v_ta_p_vertrag.ksh` executes:
    *   Initializes environment variables (similar utilities).
    *   Parses passed parameters (`p_JobKennung`, `p_EintragsNr`).
    *   Sets `v_TabName='ta_p_vertrag'`.
    *   Invokes `starteSQLSkript` which executes `d_ausd_v_ta_p_vertrag.sql` with relevant parameters.
3.  `d_ausd_v_ta_p_vertrag.sql` executes (via SQL*Plus):
    *   **Reads** from `isbert_schema.dwtk_meldungen` to determine `v_datum`.
    *   **Truncates** the target table `sof$ta_p_vertrag`.
    *   **Reads** from `sof$ta_vertrag_tmp` (twice, aliased `v` and `pv`) to process "twinbill contracts".
    *   **Writes** the processed data into `sof$ta_p_vertrag`.
    *   **Truncates** numerous other temporary tables (`sof$ta_disc_zusgf`, `sof$ta_discount`, etc.) using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.

**Target Data Flow (BigQuery/Airflow):**

1.  An Airflow DAG `dag_ta_p_vertrag_sync` starts.
2.  **Task 1: Orchestration/Parameter Setup (PythonOperator/BigQueryOperator)**
    *   Simulate environment setup and parameter parsing. Parameters like `JobKennung` and `EintragsNr` will be Airflow DAG parameters.
    *   Determine the `v_datum` equivalent by querying `isbert_dwh.dwtk_meldungen` in BigQuery.
3.  **Task 2: Truncate Target Table (BigQueryOperator)**
    *   Execute `TRUNCATE TABLE sof_dwh.ta_p_vertrag;`
4.  **Task 3: Core Transformation (BigQueryOperator)**
    *   Execute the converted BigQuery SQL `INSERT INTO sof_dwh.ta_p_vertrag ... SELECT ... FROM sof_dwh.ta_vertrag_tmp ...` (as detailed in section 5.1).
5.  **Task 4: Truncate Temporary Tables (Multiple BigQueryOperators or a single BigQuery script)**
    *   Execute `TRUNCATE TABLE` commands for all `sof$ta_*` temporary tables identified (e.g., `TRUNCATE TABLE sof_dwh.ta_disc_zusgf;`, etc.). These can be parallelized if no dependencies exist.
6.  **Task 5: Finalization/Status Update (PythonOperator/BigQueryOperator)**
    *   Update job status in a BigQuery logging table (if applicable) or use Airflow's native success/failure mechanisms.

## 5. Transformation Logic

### 5.1 `d_ausd_v_ta_p_vertrag.sql` (Core SQL Logic)

The Oracle SQL*Plus script performs the primary data transformation.

**Key Transformations and Migrations:**

*   **Variable Derivation:**
    *   Oracle: `v_datum` is derived from `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';`
    *   BigQuery: This will be converted to `COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')` within a CTE or subquery.
*   **Main `INSERT INTO ... SELECT` Statement:**
    *   The primary operation inserts into `sof$ta_p_vertrag` by selecting from `sof$ta_vertrag_tmp` (aliased as `v` and `pv`).
    *   **Oracle Outer Join Syntax:** The `WHERE v.twin_vertrag_id = pv.vertrag_id_carmen (+);` is Oracle-specific.
    *   **BigQuery Equivalent:** This will be converted to a `LEFT JOIN`: `FROM sof_dwh.ta_vertrag_tmp v LEFT JOIN sof_dwh.ta_vertrag_tmp pv ON v.twin_vertrag_id = pv.vertrag_id_carmen`.
    *   **Column Mappings:** Direct column-to-column mapping will be preserved. Any date-related columns will use `DATE` or `TIMESTAMP` data types in BigQuery, and numeric types will map to `INT64`, `NUMERIC`, or `BIGNUMERIC` as appropriate.
    *   **Oracle `PARALLEL` Hint:** `/*+ parallel(v,4) parallel(pv,4) */` will be removed as BigQuery automatically handles query parallelism.
*   **`TRUNCATE TABLE` Operations:**
    *   Oracle: `TRUNCATE TABLE sof$ta_p_vertrag` and `TRUNCATE TABLE <table_name> DROP STORAGE`/`REUSE STORAGE` (via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`).
    *   BigQuery: These will be direct `TRUNCATE TABLE dataset.table_name;` statements. The `DROP STORAGE`/`REUSE STORAGE` clauses are not applicable and will be omitted.
*   **Oracle SQL*Plus Specific Directives:**
    *   `DEFINE`, `COLUMN ... NEW_VALUE`, `PROMPT`, `START`, `SPOOL`, `WHENEVER SQLERROR`, `SET TIMING`, `SET SERVEROUTPUT` will be removed as they are client-side commands and not part of standard SQL. Logging and execution flow will be handled by Airflow.
*   **Procedural Blocks (`BEGIN ... END;`):** The calls to `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` from within PL/SQL blocks will be externalized. Each `TRUNCATE TABLE` command will become a distinct BigQuery task in the Airflow DAG.

### 5.2 KornShell Scripts (`r_ausd_v_ta_p_vertrag.ksh`, `k_ausd_v_ta_p_vertrag.ksh`)

The logic in these scripts primarily focuses on job orchestration, environment management, parameter handling, and error trapping. This functionality will be reimplemented in Python as part of the Airflow DAG.

*   **Environment Setup:** Sourced scripts (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/...`) defining environment variables will be replaced by Airflow environment variables, connections, or Python logic.
*   **Parameter Parsing:** `getopts` logic will be replaced by Airflow DAG parameters or Python argument parsing for `JobKennung` and `EintragsNr`.
*   **Error Handling:** `f_alis_msgerr.ksh` and `trap` commands will be replaced by Airflow's robust error handling, retry mechanisms, and native logging to Cloud Logging.
*   **Script Invocation:** The sequential invocation of scripts will be translated into a series of tasks within the Airflow DAG, defining their dependencies.
*   **Logging:** `DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK` will be replaced by Airflow logging, pushing logs to Cloud Logging.

## 6. External Dependencies

*   **Oracle Database:** The source Oracle database is the primary external dependency. All relevant tables from `isbert_schema` and `sof$` will be migrated to BigQuery.
*   **`isbert_schema.dwtk_meldungen`:** This Oracle table, used to determine the `v_datum` variable, must be migrated to BigQuery. Its equivalent in BigQuery will be `isbert_dwh.dwtk_meldungen`.
*   **`isbert_schema.DWPA_UTIL_SKRIPT` Package/Procedure:** This Oracle stored procedure is used to execute DDL (`TRUNCATE TABLE`) commands. In BigQuery, these will be direct `TRUNCATE TABLE` SQL statements executed by Airflow operators. If `DWPA_UTIL_SKRIPT` contains more complex logic than just DDL execution, that logic will need to be re-implemented as a BigQuery stored procedure, UDF, or Python task. Initial analysis suggests direct DDL execution, simplifying migration.
*   **`DB-Link @pcrs1` (from `DEFINE v_carmen = "@pcrs1"`):** This hints at a potential database link to a "Carmen DB". The function and necessity of this link need further investigation.
    *   **Resolution:** If `v_carmen` is actively used to access data from another Oracle instance, that source system itself becomes an external dependency. It would need to be migrated to BigQuery or integrated via BigQuery federated queries (e.g., to Cloud SQL for Oracle) or a separate ingestion pipeline (e.g., Dataflow, DMS). Without further information on its usage, it's assumed to be currently unused or for logging/metadata lookup that will be re-platformed.
*   **Filesystem Utilities and Directories (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/...`, `$DW_DIR_UTL/...`):** These are shell-specific environment setups and temporary file locations. They will be replaced by Cloud Composer's environment management, Airflow XComs for inter-task communication, and potentially Cloud Storage for any temporary file storage needs.

## 7. Unresolved / Risks

*   **Implicit Dependencies:** The invocation chain (ksh -> ksh -> sql) was identified through code review rather than explicit `INVOKES` edges in `lineage_edges`. This indicates that automated lineage tools might not fully capture all execution dependencies in such layered shell/SQL setups, posing a risk for future automation or impact analysis if not thoroughly documented.
*   **`v_carmen` DB-Link:** The actual usage and criticality of the `v_carmen` database link and the "Carmen DB" it points to are unclear. This needs further analysis to determine if it's an active data source or an unused artifact. If active, it presents a significant external dependency and requires a dedicated migration strategy.
*   **`isbert_schema.DWPA_UTIL_SKRIPT` Complexity:** While assumed to be simple DDL execution, the `runstatement` procedure in `DWPA_UTIL_SKRIPT` might encapsulate more complex logic or validations. This needs to be verified to ensure a direct `TRUNCATE TABLE` replacement is sufficient.
*   **Error Handling Equivalency:** The custom shell-based error handling and messaging system (`DWMSG_*` functions) will need careful re-implementation in Python/Airflow to ensure equivalent robustness and alerting.
*   **Temporary Table Management:** The current use of `sof$ta_vertrag_tmp` and other `sof$ta_*` tables which are truncated might imply specific data staging patterns. While BigQuery CTEs or temporary tables can substitute, understanding the exact lifecycle and data retention needs for these intermediate tables is crucial.
*   **Date Formats and Time Zones:** Ensure consistent date and time zone handling between the Oracle source system and BigQuery, especially for `m.timecreated` and derived `v_datum`.

## 8. Build Plan

The migration build plan will proceed in the following ordered steps:

1.  **Data Migration:** Migrate all source Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_p_vertrag`, `sof$ta_vertrag_tmp`, and all `sof$ta_*` temporary tables) to dedicated BigQuery datasets (e.g., `isbert_dwh`, `sof_dwh`). This can be done using GCP Data Migration Service (DMS) or custom ETL processes.
2.  **SQL Conversion:** Convert `d_ausd_v_ta_p_vertrag.sql` to BigQuery Standard SQL based on the design document's "Equivalent BigQuery SQL Query" (Section 5.1). This will result in a BigQuery SQL script (e.g., `bq_d_ausd_v_ta_p_vertrag.sql`).
3.  **Airflow DAG Development:**
    *   Create an Airflow DAG file (`dag_ta_p_vertrag_sync.py`) in Python.
    *   Define DAG parameters that map to the original shell script parameters (e.g., `JobKennung`, `EintragsNr`).
    *   Implement tasks for:
        *   Deriving `v_datum` using a BigQueryOperator against `isbert_dwh.dwtk_meldungen`.
        *   Executing `TRUNCATE TABLE sof_dwh.ta_p_vertrag;` using a `BigQueryOperator`.
        *   Executing the core `bq_d_ausd_v_ta_p_vertrag.sql` (INSERT INTO SELECT) using a `BigQueryInsertJobOperator`.
        *   Executing individual `TRUNCATE TABLE` statements for each of the `sof$ta_*` temporary tables (e.g., `sof_dwh.ta_disc_zusgf`, `sof_dwh.ta_discount`, etc.) using separate `BigQueryOperator` tasks. These can be grouped or parallelized where appropriate.
    *   Configure Airflow connections for BigQuery.
    *   Implement logging and error handling using Airflow's native mechanisms.
4.  **Unit and Integration Testing:** Develop and execute comprehensive test cases for the BigQuery SQL and the Airflow DAG to ensure functional equivalence and performance.
5.  **Deployment:** Deploy the BigQuery tables, the SQL script, and the Airflow DAG to the production GCP environment.
6.  **Monitoring and Alerting:** Set up Cloud Monitoring and Alerting for the BigQuery jobs and Airflow DAG runs.

**Generated Build Artifacts (Examples):**

*   `bq_d_ausd_v_ta_p_vertrag.sql` (BigQuery SQL)
*   `dag_ta_p_vertrag_sync.py` (Airflow DAG)