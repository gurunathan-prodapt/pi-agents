# Migration Design — k_ausd_adressen.ksh

## 1. Purpose & Scope
This job, `k_ausd_adressen.ksh`, is a KornShell control script responsible for orchestrating the preparation of address data. Its primary function is to:
- Parse and validate input parameters (Job ID, Entry Number, Key Date, Restart Value).
- Validate the format of the key date.
- Execute a core Oracle SQL script, `d_ausd_adressen.sql`, passing relevant parameters.
- Handle error conditions and log messages.
- (Intended but currently commented out in the source) Manage job table entries (deactivating old jobs, creating new ones).
The `d_ausd_adressen.sql` script performs several data manipulation steps, primarily involving truncation and insertion into various temporary tables (`sof$ta_bp_ref_gp`, `sof$ta_reachability`, `sof$ta_business_pt`, etc.), derived from source tables like `cds$ta_bp_ref`, `glv$ta_country`, `glv$ta_description`, and `bpd$ta_reachability`. The overall business purpose is the preparation and transformation of business partner and address-related data, likely for reporting or downstream systems.

## 2. Source Inventory
The job `k_ausd_adressen.ksh` consists of the following key source files:

| File Name                                                 | Technology | Tier   | Automation Bucket | Purpose                                                                                                                                                                                                 |
| :-------------------------------------------------------- | :--------- | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh` | KornShell  | medium | semi_auto         | Orchestration script: parameter parsing, validation, date calculations, and execution of `d_ausd_adressen.sql`.                                                                                           |
| `vobs/dw_source/isrpt/isbert/aufbereitung/sql/d_ausd_adressen.sql` | Oracle SQL | N/A    | N/A               | Core data transformation logic: Truncates and inserts data into numerous temporary tables for business partner references, reachability, country data, and business partner details, using `SELECT` statements. |

The shell script also indirectly relies on several utility KornShell scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) for common functions like error handling, date checks, parameter parsing, and SQL*Plus invocation. These helper scripts will need to be either re-implemented in BigQuery SQL/Python or their functionality absorbed into the main BigQuery solution.

## 3. Target Architecture
The migration will target Google Cloud's BigQuery for data storage and SQL transformations, and Cloud Composer (Apache Airflow) for job orchestration.

-   **BigQuery Datasets**: Dedicated datasets will be created for raw source data (if ingested directly), staging tables, and final output tables. Temporary tables created in Oracle will be replaced by temporary tables or Common Table Expressions (CTEs) within BigQuery SQL scripts, or persistent staging tables if intermediate data needs to be retained.
-   **BigQuery Stored Procedures**: The core data transformation logic from `d_ausd_adressen.sql` will be converted into one or more BigQuery Stored Procedures. These procedures will encapsulate the `TRUNCATE` and `INSERT` logic.
-   **Airflow DAG**: An Airflow DAG will be developed to replace the orchestration logic of `k_ausd_adressen.ksh`. This DAG will handle:
    -   Parameter passing (DagRundate for `p_Stichtag`, other parameters via Airflow variables or passed at runtime).
    -   Execution of BigQuery Stored Procedures.
    -   Logging and error handling.
    -   (Potentially) calls to other BigQuery components or external systems.
-   **BigQuery Tables**: All Oracle source tables (e.g., `cds$ta_bp_ref`, `glv$ta_country`, `glv$ta_description`, `bpd$ta_reachability`) will be migrated to BigQuery tables. The `isbert_schema.dwtk_meldungen` table will also be migrated or replaced by a BigQuery logging table.
-   **Job Logging Table**: The functionality of the 'Job-Tabelle' (which was commented out in source but intended) will be implemented as a dedicated BigQuery table for tracking job execution status and metrics.

## 4. Data Flow & Lineage
The original data flow is controlled by `k_ausd_adressen.ksh` executing `d_ausd_adressen.sql`.

**Original Flow:**
1.  `k_ausd_adressen.ksh` starts execution, loads environment, and utility scripts.
2.  It parses command-line parameters: `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
3.  Performs parameter validation and date format check (`DDMMYYYY`) on `p_Stichtag`.
4.  Determines `p_datum_heute` and `p_datum_gestern` using `gestern.ksh`.
5.  Invokes `d_ausd_adressen.sql` using a `starteSQLSkript` function (presumably from `h_alis_sqlplus.ksh`), passing parameters like `p_EintragsNr`, `p_JobKennung`, `p_Stichtag`, and temp file path (`$tmpFile`).
6.  `d_ausd_adressen.sql` connects to the Oracle database and executes a series of SQL steps:
    *   **Step 00**: Variable definitions, retrieves `v_datum` from `isbert_schema.dwtk_meldungen`.
    *   **Step 01**: Truncates various temporary `sof$ta_` tables (e.g., `sof$ta_bp_ref_gp`, `sof$ta_reachability`, `sof$ta_country`, etc.) using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   **Step 02**: Populates temporary tables for business partner references (`sof$ta_bp_ref_gp`, `sof$ta_bp_ref_re`, `sof$ta_bp_ref_ev`, `sof$ta_bp_ref_dn`) from `cds$ta_bp_ref` and `cds$ta_inv_definition`, filtered by `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production` and specific `bp_ref_ty`/`address_ref_ty` values, using `&v_datum` as a date filter.
    *   **Step 03**: Populates temporary tables for country and reachability data (`sof$ta_country`, `sof$ta_country_desc`, `sof$ta_laender_kng`, `sof$ta_reachability`) from `glv$ta_country`, `glv$ta_description`, and `bpd$ta_reachability`. Joins these to create `sof$ta_e_reach_gp`, `sof$ta_e_reach_re`, `sof$ta_e_reach_ev`, `sof$ta_e_reach_dn`. Cleans up some temporary tables.
    *   **Step 04**: Populates `sof$ta_business_pt` from `bpd$ta_business_partner`. Then, it populates `sof$ta_e_business_gp`, `sof$ta_e_business_re`, `sof$ta_e_business_ev`, `sof$ta_e_business_dn` by joining with `sof$ta_business_pt` and various `sof$ta_bp_ref_*_nodp` tables. Cleans up more temporary tables.
    *   **Step 05**: Populates `sof$ta_e_regulierer` from `cds$ta_bp_ref`.
    *   **Step 06**: (Commented out) Further cleanup of temporary tables.
7.  `k_ausd_adressen.ksh` reads the record count from the temp file written by SQL*Plus (implicitly) and (intended) creates an entry in the job table.

**Target BigQuery / Airflow Flow:**
1.  **Airflow DAG (`k_ausd_adressen_dag`)**:
    *   **Parameter Task**: An initial task to define and parse Airflow parameters for `JobKennung`, `EintragsNr`, `Stichtag` (likely a `ds` or `data_interval_end` Airflow variable), and `wiederanlaufWert`.
    *   **BigQuery Stored Procedure Task**: A BigQuery operator executes a main stored procedure, e.g., `project.dataset.sp_ausd_adressen_main`.
    *   **Logging Task**: A final task to insert execution details, including record counts, into a BigQuery job logging table.
2.  **BigQuery Stored Procedure (`sp_ausd_adressen_main`)**:
    *   Receives `p_JobKennung`, `p_EintragsNr`, `p_Stichtag_date`, `p_wiederanlaufWert` as input.
    *   Performs parameter validation and date parsing internally.
    *   **Step 01 (Cleanup)**: Executes `TRUNCATE TABLE` statements for all intermediate staging tables (e.g., `staging.sof_ta_bp_ref_gp`, `staging.sof_ta_reachability`).
    *   **Step 02 (BP Ref Population)**: `INSERT` statements to populate `staging.sof_ta_bp_ref_gp`, `staging.sof_ta_bp_ref_re`, `staging.sof_ta_bp_ref_ev`, `staging.sof_ta_bp_ref_dn` from `raw.cds_ta_bp_ref` and `raw.cds_ta_inv_definition`, applying the date and filtering logic.
    *   **Step 03 (Reachability and Country)**: `INSERT` statements to populate `staging.sof_ta_country`, `staging.sof_ta_country_desc`, `staging.sof_ta_laender_kng`, `staging.sof_ta_reachability` from `raw.glv_ta_country`, `raw.glv_ta_description`, `raw.bpd_ta_reachability`. Then `INSERT` into `target.sof_ta_e_reach_gp`, `target.sof_ta_e_reach_re`, `target.sof_ta_e_reach_ev`, `target.sof_ta_e_reach_dn` by joining these staging tables.
    *   **Step 04 (Business Partner Details)**: `INSERT` into `staging.sof_ta_business_pt` from `raw.bpd_ta_business_partner`. Then `INSERT` into `target.sof_ta_e_business_gp`, `target.sof_ta_e_business_re`, `target.sof_ta_e_business_ev`, `target.sof_ta_e_business_dn` by joining with `staging.sof_ta_business_pt`.
    *   **Step 05 (Regulierer)**: `INSERT` into `target.sof_ta_e_regulierer` from `raw.cds_ta_bp_ref`.
    *   **Cleanup**: Truncates intermediate staging tables within the stored procedure.
    *   **Record Count**: Calculates the final record count from relevant target tables.
    *   **Job Log**: Inserts a record into a BigQuery job logging table (`metrics.job_log`) with execution details and record counts.

## 5. Transformation Logic

**`k_ausd_adressen.ksh` (Orchestration Logic to Airflow DAG / BigQuery Stored Procedure)**:
-   **Parameter Parsing**: `getopts` will be replaced by Airflow DAG parameters or BigQuery Stored Procedure input parameters.
-   **Date Validation**: `DWDate_Datum_Check` and `gestern.ksh` logic will be replaced by BigQuery's native date functions (e.g., `PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB`).
-   **Error Handling**: `f_alis_msgerr.ksh` and custom error checks will be mapped to BigQuery's `RAISE` statement for exceptions and Airflow's built-in logging and alert mechanisms.
-   **SQL Execution**: The `starteSQLSkript` call will be replaced by direct invocation of BigQuery Stored Procedures via Airflow's `BigQueryExecuteStoredProcedureOperator` or `BigQueryExecuteQueryOperator`.
-   **Temporary File**: Reading record counts from `$tmpFile` will be replaced by `SELECT COUNT(*)` within the BigQuery Stored Procedure and captured in an output parameter or logging table.
-   **Job Table Management**: The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls will be implemented as `UPDATE` and `INSERT` statements into a BigQuery job logging table.

**`d_ausd_adressen.sql` (Oracle SQL to BigQuery SQL)**:
-   **Temporary Tables**: All `sof$ta_` temporary tables will be translated into BigQuery staging tables (e.g., `project.staging.sof_ta_bp_ref_gp`) or as CTEs within a larger BigQuery SQL script. Given the multiple steps and implicit dependencies, persistent staging tables in BigQuery might be a safer approach initially.
-   **`TRUNCATE TABLE`**: Will be directly mapped to BigQuery's `TRUNCATE TABLE` or `DELETE FROM` statements.
-   **`INSERT /*+ APPEND */ INTO`**: Will be mapped to BigQuery's `INSERT INTO` statements. BigQuery handles append operations efficiently.
-   **`DEFINE v_carmen = "@pcrs1"`**: This variable, likely an Oracle database link or schema qualifier, will be removed. Source tables will directly reference their BigQuery project/dataset (e.g., `raw.cds_ta_bp_ref`).
-   **`COLUMN s_datum new_value v_datum noprint`**: This SQL*Plus variable substitution will be handled by passing the `p_Stichtag` parameter into the BigQuery Stored Procedure and using it directly.
-   **`NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')`**: This will be converted to BigQuery's `IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')`.
-   **`TO_DATE('&v_datum','YYYYMMDD')`**: Will be converted to `PARSE_DATE('%Y%m%d', p_Stichtag_input_parameter)`.
-   **`parallel(bpr,4)` and `use_hash(br)`**: Oracle hints will be removed. BigQuery's query optimizer automatically handles parallelization and join strategies.
-   **`substr(lk.short_description,1,3)`**: Will be converted to `SUBSTR(lk.short_description, 1, 3)` in BigQuery.
-   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...')`**: This stored procedure call will be replaced by direct BigQuery `TRUNCATE TABLE` statements.
-   **`UNION ALL`**: Directly convertible to BigQuery `UNION ALL`.

## 6. External Dependencies
The original job has the following external dependencies:

-   **Oracle Database**: The primary source of data and the execution environment for the SQL script. This will be migrated to BigQuery. All Oracle tables (`cds$ta_bp_ref`, `glv$ta_country`, `glv$ta_description`, `bpd$ta_reachability`, `isbert_schema.dwtk_meldungen`) will be ingested into BigQuery.
-   **Shell Environment (`$HOME/.dw_init`, `BERT_DIR_ROOT`, `DW_DIR_UTL`)**: Environment variables and initialisation scripts. These will be replaced by Airflow environment variables, BigQuery project/dataset configurations, or Airflow Variables/Connections.
-   **External Shell Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`)**: These utility scripts provide common functions. Their logic will be reimplemented in the Airflow DAG (e.g., parameter parsing, date calculations) or directly within BigQuery Stored Procedures (e.g., date checks). The `gestern.ksh` functionality to derive current and previous dates will use BigQuery date functions.
-   **Oracle SQL*Plus**: The execution engine for `d_ausd_adressen.sql`. This will be replaced by BigQuery's SQL engine.

## 7. Unresolved / Risks
-   **Commented-out Job Management**: The original script had commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. These were treated as intended functionality in the design. A decision needs to be made whether this functionality was truly desired and should be implemented in BigQuery, or if it was intentionally disabled and can be omitted.
-   **`v_carmen = "@pcrs1"`**: The exact nature and purpose of `@pcrs1` in the Oracle context is not fully clear from the code. It is assumed to be a database link or schema qualifier that needs to be addressed during source table migration to BigQuery. If it represents a connection to another database, that external system will need a dedicated migration strategy. Assuming it's an internal schema reference.
-   **Exact Error Code Semantics**: The original script uses specific error numbers (`ErrNr=193`, `ErrNr=192`). If precise error code mapping is required, a dedicated error handling mechanism with a lookup table for error messages might be needed in BigQuery/Airflow.
-   **Performance Optimization**: While BigQuery handles parallelism automatically, the Oracle hints (`/*+ parallel(bpr,4) */`, `/*+ use_hash(br) */`) indicate a focus on performance in the original system. Careful review and potential optimization of the generated BigQuery SQL will be necessary to match or exceed original performance, especially with large datasets.
-   **Idempotency and Restartability**: The `p_wiederanlaufWert` parameter and `WHENEVER SQLERROR CONTINUE`/`EXIT FAILURE` indicate some level of restartability. The BigQuery/Airflow solution should maintain or improve this. The current design assumes a simple `REUSE STORAGE` for temporary tables, which might need more sophisticated handling in a production BigQuery environment.

## 8. Build Plan
The build plan will involve creating the necessary BigQuery assets and an Airflow DAG.

1.  **BigQuery DDL for Source Tables**: Generate DDL for BigQuery tables corresponding to Oracle source tables (`cds_ta_bp_ref`, `glv_ta_country`, `glv_ta_description`, `bpd_ta_reachability`, `dwtk_meldungen`) in the `raw` dataset. (Language: BigQuery SQL)
2.  **BigQuery DDL for Staging Tables**: Generate DDL for all intermediate `sof_ta_` staging tables (e.g., `sof_ta_bp_ref_gp`, `sof_ta_reachability`, `sof_ta_country`, etc.) in the `staging` dataset. (Language: BigQuery SQL)
3.  **BigQuery DDL for Target Tables**: Generate DDL for the final target tables (`sof_ta_e_reach_gp`, `sof_ta_e_reach_re`, `sof_ta_e_reach_ev`, `sof_ta_e_reach_dn`, `sof_ta_e_business_gp`, `sof_ta_e_business_re`, `sof_ta_e_business_ev`, `sof_ta_e_business_dn`, `sof_ta_e_regulierer`) in the `target` dataset. (Language: BigQuery SQL)
4.  **BigQuery DDL for Job Logging Table**: Create DDL for a job logging table (`metrics.job_log`) to capture execution metadata. (Language: BigQuery SQL)
5.  **BigQuery Stored Procedure**: Create a BigQuery Stored Procedure (`project.dataset.sp_ausd_adressen_main`) containing the migrated SQL logic from `d_ausd_adressen.sql`, incorporating the parameter handling and date logic from `k_ausd_adressen.ksh`. (Language: BigQuery SQL)
6.  **Airflow DAG**: Develop an Airflow DAG (`k_ausd_adressen_dag.py`) to orchestrate the BigQuery Stored Procedure execution, handle parameters, and manage logging. (Language: Python)
7.  **Helper Functions/Modules**: (If necessary) Create Python modules for any complex parameter validation or date calculations that are better handled outside the BigQuery Stored Procedure or directly in the Airflow DAG. (Language: Python)
