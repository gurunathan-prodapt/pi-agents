# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

## 1. Purpose & Scope
This job, orchestrated by the KornShell script `k_ausd_v_ta_c_bfc.ksh`, is responsible for calculating and managing "bindefrist" (binding period) data. The shell script acts as a control wrapper, handling parameter validation, environmental setup, and the execution of the core SQL logic. The embedded Oracle SQL script, `d_ausd_v_ta_c_bfc.sql`, performs the actual data processing, which involves reading contract-related information, calculating or updating binding period values, and persisting them in designated tables. The overall purpose is to maintain an up-to-date view of contract binding periods, likely for reporting or further downstream processes.

## 2. Source Inventory

### File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh`
*   **Technology:** KornShell (KSH) script
*   **Role:** Orchestration, parameter handling, error reporting, invocation of SQL script.
*   **Complexity Tier:** Medium (Orchestration script with external dependencies and parameter parsing logic).
*   **Automation Bucket:** Semi-auto/Redesign (refactoring to Cloud Composer/BQ Stored Proc for orchestration).

### File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_c_bfc.sql`
*   **Technology:** Oracle SQL (PL/SQL, SQL*Plus features, DB Links)
*   **Role:** Data Transformation, Function Definition, Table Updates (MERGE, INSERT, TRUNCATE).
*   **Complexity Tier:** Medium (Complex SQL with procedural elements, external database links, custom function).
*   **Automation Bucket:** Semi-auto/Manual (requires careful translation of Oracle-specific SQL and procedural logic).

## 3. Target Architecture
The migration target platform is Google BigQuery.
*   **Data Storage:** All source Oracle tables (`isbert_schema.dwtk_meldungen`, `all_objects`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_c_bfc_akt`, `sof$ta_c_bfc`) will be migrated to BigQuery tables. The temporary table `sof$ta_c_bfc_akt` will also be a BigQuery temporary table or recreated as needed.
*   **Transformation Logic:** The core SQL transformation logic from `d_ausd_v_ta_c_bfc.sql` will be converted into a BigQuery Stored Procedure (BQSP). The Oracle `bfc_get_bindefrist` function will be re-implemented as a BigQuery User-Defined Function (UDF) or integrated directly into the BQSP.
*   **Orchestration:** The KornShell orchestration logic from `k_ausd_v_ta_c_bfc.ksh` will be refactored into a BigQuery Stored Procedure, which will call the data transformation BQSP. Alternatively, for more complex scheduling and monitoring, Google Cloud Composer (Airflow) can be used to manage the execution of the BigQuery Stored Procedures.
*   **External Data Access:** The external Oracle database accessed via `DB_LINK:PCRS1` will be handled through BigQuery Federated Queries if the external system remains in Oracle and read-only access is sufficient, or via data replication/streaming services (e.g., Datastream, Fivetran) to bring the external data into BigQuery.

## 4. Data Flow & Lineage
1.  **KornShell Script (`k_ausd_v_ta_c_bfc.ksh`)**:
    *   **Inputs:** Command-line parameters (`p_JobKennung`, `p_EintragsNr`).
    *   **Processes:**
        *   Initializes environment (sourcing `.dw_init` and various helper scripts).
        *   Parses and validates input parameters.
        *   Defines target table name `ta_c_bfc` and temporary file path.
        *   Invokes the SQL script (`d_ausd_v_ta_c_bfc.sql`) via `starteSQLSkript` (an abstraction over SQL*Plus).
        *   Reads a record count from a temporary file `tmpFile` (populated by the SQL script execution).
    *   **Outputs:** Console messages, writes to temporary file (indirectly via SQL script), updates job control tables (indirectly via SQL script).

2.  **Oracle SQL Script (`d_ausd_v_ta_c_bfc.sql`)**:
    *   **Inputs:**
        *   `isbert_schema.dwtk_meldungen` (to determine `v_datum`).
        *   `all_objects` (to determine `v_bfc_procedure`).
        *   `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period` (for contract data).
        *   External Oracle system via `DB_LINK:PCRS1` for `spr_schema.cds$vr_Bindefrist@PCRS1`.
    *   **Processes:**
        *   **Step 1: Determine `v_datum` and `v_bfc_procedure`**: Queries system tables/logs.
        *   **Step 2: Populate `sof$ta_c_bfc_akt`**: Inserts aggregated contract data (cntrct_id, bfc_age, bfc_count etc.) into a temporary table `sof$ta_c_bfc_akt` by joining `sof$ta_cntrct_crs` with several other tables. This step involves `TRUNCATE` and `INSERT ... SELECT`.
        *   **Step 3: Initial Population of `sof$ta_c_bfc`**: If `sof$ta_c_bfc` is empty, it inserts all records from `sof$ta_c_bfc_akt`.
        *   **Step 4: Merge `sof$ta_c_bfc_akt` into `sof$ta_c_bfc`**: Updates existing records or inserts new ones based on changes in `bfc_age` or `bfc_count`. This step uses the `bfc_get_bindefrist` function.
        *   **Step 5: Update `sof$ta_c_bfc` (partial update)**: Updates `bindefrist` for records where `bfc_procedure` is outdated, limited by `v_max_update`.
        *   **Step 6: Cleanup**: `TRUNCATE` the temporary table `sof$ta_c_bfc_akt`.
    *   **Outputs:**
        *   Writes to `sof$ta_c_bfc_akt` (temporary table).
        *   Writes to `sof$ta_c_bfc` (main target table).
        *   Defines `bfc_get_bindefrist` function (temporary or persistent depending on Oracle context).

## 5. Transformation Logic

### KornShell Script (`k_ausd_v_ta_c_bfc.ksh`) to BigQuery Stored Procedure (BQSP)
*   **Parameter Handling:** Shell `getopts` and positional parameters (`p_JobKennung`, `p_EintragsNr`) will be directly mapped to BQSP input parameters (e.g., `p_job_kennung STRING`, `p_eintrags_nr STRING`).
*   **Environment Initialization:** Sourcing `*.ksh` files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be replaced by BigQuery's intrinsic environment or by setting global variables/parameters within the BQSP, or by equivalent BigQuery-native functions for date and parameter checks. Error logging (`DWMSG_MeldeFehler`) will be replaced with BigQuery `RAISE` statements or logging to BigQuery audit tables.
*   **Parameter Validation:** The `pruefeParameterGesetzt` calls and `if [ ! $ErrNr -eq 0 ]` logic will be translated to BigQuery `IF` statements and `ASSERT` statements within the BQSP.
*   **SQL Script Execution:** The `starteSQLSkript` call, which wraps `d_ausd_v_ta_c_bfc.sql`, will be replaced by a direct call to the corresponding BigQuery Stored Procedure that implements `d_ausd_v_ta_c_bfc.sql`'s logic.
*   **Temporary File Handling:** The usage of `tmpFile` to store and retrieve `v_records` will be replaced by a `DECLARE` variable within the BQSP, populated by a `SELECT COUNT(*)` query from the target table.
*   **Overall Orchestration:** The sequential execution flow will be maintained within the BQSP.

### Oracle SQL Script (`d_ausd_v_ta_c_bfc.sql`) to BigQuery SQL / Stored Procedure
*   **Variables/Defines:** Oracle `DEFINE` variables (`v_carmen`, `v_max_update`) will be translated to BigQuery `DECLARE` variables or procedure parameters. Oracle `COLUMN new_value` constructs will be replaced by `SELECT ... INTO` statements with `DECLARE` variables.
*   **Oracle-specific Features:**
    *   `DB_LINK:PCRS1`: This is a significant external dependency and will require special handling (see Section 6).
    *   `CREATE OR REPLACE SYNONYM`: Synonyms will be replaced by fully qualified BigQuery table names or views if they point to BigQuery tables. For external systems, Federated Queries or data ingestion will be used.
    *   `CREATE OR REPLACE FUNCTION bfc_get_bindefrist`: This function needs to be reimplemented as a BigQuery UDF (User-Defined Function) in SQL or JavaScript, ensuring equivalent logic.
    *   `WHENEVER SQLERROR EXIT FAILURE`/`CONTINUE`: These SQL*Plus directives will be handled by BigQuery's default error handling within procedures (`TRANSACTION` blocks with `ROLLBACK` on error, or `RAISE` for explicit error handling).
    *   `SET TIMING ON`, `SET SERVEROUTPUT ON`, `SPOOL`: These are SQL*Plus client-side commands and will not be directly translated. Logging and timing in BigQuery are typically handled by monitoring tools (Cloud Monitoring, Cloud Logging) and query statistics.
    *   `TRUNCATE TABLE ... REUSE STORAGE`: BigQuery `TRUNCATE TABLE` achieves similar effect.
    *   `INSERT /*+ append */`: BigQuery handles large inserts efficiently; no direct hint equivalent needed.
    *   `/*+ full(c) parallel(c,4) */`: Oracle hints will be removed; BigQuery query optimizer handles execution plans automatically.
    *   Oracle-style Outer Join `(+):` will be converted to explicit `LEFT JOIN` syntax.
    *   `NVL()`: will be converted to BigQuery `COALESCE()`.
    *   `TO_DATE()`, `TO_CHAR()`, `TRUNC()`: will be converted to BigQuery date/timestamp functions like `PARSE_DATE()`, `FORMAT_DATE()`, `DATE()`, `DATE_TRUNC()`.
    *   `ROWNUM`: will be replaced by `ROW_NUMBER() OVER (...)` in a subquery or by `LIMIT` clause.
    *   `DECLARE v_rows NUMBER; BEGIN SELECT COUNT(1) INTO v_rows FROM ... WHERE rownum = 1; IF v_rows = 0 THEN INSERT... END IF; END;`: This procedural block for conditional insert will be translated into BigQuery `IF EXISTS (SELECT 1 FROM ... LIMIT 1) THEN ... ELSE ... END IF;` or simply `INSERT ... WHERE NOT EXISTS (SELECT 1 FROM ... LIMIT 1)`.
    *   `MERGE INTO`: BigQuery has a direct `MERGE` statement with `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT` clauses.

## 6. External Dependencies

*   **Oracle System via `DB_LINK:PCRS1`**: The SQL script accesses `spr_schema.cds$vr_Bindefrist@PCRS1`. This indicates a dependency on a remote Oracle database.
    *   **Migration Strategy:**
        1.  **Data Replication:** If data from `PCRS1` is needed in BigQuery, consider replicating the relevant tables from `PCRS1` into BigQuery using tools like Google Cloud Datastream, Fivetran, or by setting up regular batch extracts and loads.
        2.  **Federated Queries:** If `PCRS1` remains in Oracle and only limited, read-only access to `spr_schema.cds$vr_Bindefrist` and other objects (e.g., `all_objects`) is required, BigQuery Federated Queries to Cloud SQL for PostgreSQL (if Oracle data can be migrated there) or directly to Oracle (via third-party connectors) could be an option. However, this often adds latency and complexity.
        3.  **Application Refactoring:** If `spr_schema.cds$vr_Bindefrist` is a custom function or package, its logic might need to be re-implemented directly within BigQuery as a UDF or BQSP if it's not simply querying data.
*   **Helper Shell Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These utility scripts provide common functions.
    *   **Migration Strategy:** Their functionalities will be absorbed into the BigQuery Stored Procedures (e.g., parameter parsing, error handling, date functions) or replaced by BigQuery's native capabilities.

## 7. Unresolved / Risks

*   **`bfc_get_bindefrist` Function Re-implementation:** The Oracle function `bfc_get_bindefrist` is critical to the data transformation logic. Its exact internal logic, especially how it calls `Cds$vr_Bindefrist.GetBindeFrist`, is not fully detailed in the provided SQL. This function needs careful analysis and accurate re-implementation as a BigQuery UDF (SQL or JavaScript) or directly within the BQSP.
*   **External Oracle `PCRS1` System:** The exact nature and data volumes of `spr_schema.cds$vr_Bindefrist@PCRS1` and `all_objects@PCRS1` are unknown. The complexity of migrating or integrating this external dependency needs to be fully assessed.
*   **Missing Complexity/Automation Data:** The `file_complexity` and `automation_rate` tables did not provide entries for this job. This means the actual effort and migration bucket for the files are estimates based on manual analysis. A more precise assessment would require this data.
*   **Job Control/Logging Tables:** The shell script implicitly interacts with job control and logging mechanisms. These need to be identified and migrated to BigQuery tables (e.g., `job_error_log`, `job_table`, `job_run_log` as suggested in the KSH design).
*   **`starteSQLSkript` Abstraction:** The `starteSQLSkript` function in the KSH script is an abstraction that likely handles connection, error handling, and possibly job status updates for SQL*Plus. Its full functionality needs to be understood and integrated into the BigQuery orchestration.

## 8. Build Plan

1.  **BigQuery Table Schema Creation (SQL/DDL):**
    *   Define DDL for all target BigQuery tables: `isbert_schema.dwtk_meldungen`, `all_objects`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_c_bfc`, and auxiliary tables like `job_error_log`, `job_table`, `job_run_log`.
    *   **Language:** BigQuery DDL.
2.  **Data Ingestion for Source Tables (Data Migration Tools):**
    *   Ingest data from source Oracle tables into the newly created BigQuery tables. This may involve one-time loads and/or continuous replication.
    *   **Language:** Varies by tool (e.g., Datastream, Fivetran, `bq load`).
3.  **BigQuery User-Defined Function (`bfc_get_bindefrist`) Implementation (SQL/JavaScript UDF):**
    *   Re-implement the logic of the Oracle `bfc_get_bindefrist` function as a BigQuery UDF.
    *   **Language:** BigQuery SQL UDF or JavaScript UDF.
4.  **External `PCRS1` Dependency Handling:**
    *   Implement data replication or federated query setup for the `PCRS1` external system, ensuring `spr_schema.cds$vr_Bindefrist` and related objects are accessible in BigQuery.
    *   **Language:** Varies by tool/method.
5.  **BigQuery Stored Procedure for Data Transformation (`d_ausd_v_ta_c_bfc_sp`) (SQL):**
    *   Convert the `d_ausd_v_ta_c_bfc.sql` logic into a BigQuery Stored Procedure, incorporating the `bfc_get_bindefrist` UDF and handling all Oracle-to-BigQuery SQL conversions.
    *   **Language:** BigQuery SQL (within a stored procedure).
6.  **BigQuery Stored Procedure for Orchestration (`k_ausd_v_ta_c_bfc_sp`) (SQL):**
    *   Convert the `k_ausd_v_ta_c_bfc.ksh` logic into a BigQuery Stored Procedure, including parameter handling, validation, and calling the `d_ausd_v_ta_c_bfc_sp`.
    *   **Language:** BigQuery SQL (within a stored procedure).
7.  **Optional: Cloud Composer (Airflow) DAG (Python):**
    *   If more robust scheduling, dependency management, or integration with other GCP services is needed, create an Airflow DAG in Python to orchestrate the execution of the BigQuery Stored Procedure `k_ausd_v_ta_c_bfc_sp`.
    *   **Language:** Python.