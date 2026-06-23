# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope
This migration job focuses on the `k_ausd_v_ta_bp_ref.ksh` KornShell script and its invoked SQL script `d_ausd_v_ta_bp_ref.sql`. The primary purpose of this job is to control the execution of an Oracle SQL script that processes and updates business partner reference data (`ta_bp_ref`). The job handles parameter parsing, error checking, and records processing metrics. The SQL script itself reads data from `dwtk_meldungen` and `cds$ta_bp_ref` to populate `sof$ta_bp_ref` and perform a merge operation on `VIA`, based on a determined cutoff date.

The scope of this migration is to convert the existing KornShell and Oracle SQL logic into Google BigQuery SQL and, if necessary, BigQuery Stored Procedures for orchestration, ensuring equivalent functionality and data integrity on the BigQuery platform.

## 2. Source Inventory

### File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`
*   **Technology**: KornShell (scripting/orchestration)
*   **Complexity Tier**: Medium
*   **Automation Bucket**: Semi-Auto
*   **Purpose**: Control script for SQL execution, parameter validation, job registration, and record count logging.

### File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_bp_ref.sql`
*   **Technology**: Oracle SQL (data processing)
*   **Complexity Tier**: (Derived as part of `shellscript_to_bqsql_design`)
*   **Automation Bucket**: (Derived as part of `shellscript_to_bqsql_design`)
*   **Purpose**: Extracts and transforms business partner reference data, then loads it into `sof$ta_bp_ref` and merges into `VIA`. Determines a cutoff date from `dwtk_meldungen`.

## 3. Target Architecture

The target architecture in BigQuery will involve:
*   **BigQuery Stored Procedures**: To encapsulate the control logic currently in the KornShell script and the data processing logic of the SQL script. This will enable parameter passing, error handling, and sequential execution within BigQuery.
*   **BigQuery Tables**: All source Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_bp_ref`) and target Oracle tables (`sof$ta_bp_ref`, `VIA`) will be migrated to BigQuery tables. A dedicated `job_control_table` and `job_error_log` table will be created to replace the shell script's job registration and error logging mechanisms.
*   **Scheduling**: The job will be scheduled using native BigQuery scheduling (scheduled queries) or a cloud-native orchestrator like Cloud Composer (Airflow) if more complex dependencies or external integrations are required.

## 4. Data Flow & Lineage

The migrated job will follow this data flow:

1.  **Orchestration (BigQuery Stored Procedure `sp_ausd_v_ta_bp_ref`)**:
    *   Receives job identifier (`p_JobKennung`) and entry number (`p_EintragsNr`) as parameters.
    *   Validates parameters.
    *   Updates the `job_control_table` to deactivate older active jobs and register the current job.
    *   Determines the `v_datum` (cutoff date) by querying the `dwtk_meldungen` BigQuery table.
    *   Calls the core data processing BigQuery Stored Procedure `sp_d_ausd_v_ta_bp_ref`.
    *   Updates the `job_control_table` with the record count processed by `sp_d_ausd_v_ta_bp_ref`.
    *   Logs status messages to a `job_error_log` table or BigQuery's built-in logging.

2.  **Data Processing (BigQuery Stored Procedure `sp_d_ausd_v_ta_bp_ref`)**:
    *   Receives `v_datum`, `p_EintragsNr`, and `p_JobKennung` as input parameters.
    *   Truncates (or `DELETE`s from) the target `sof$ta_bp_ref` BigQuery table.
    *   Inserts data into `sof$ta_bp_ref` by selecting from `cds$ta_bp_ref` filtered by `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`, and `bp_ref_ty` relative to `v_datum`.
    *   Performs the `MERGE` operation on the `VIA` BigQuery table.
    *   Returns the count of processed records.

**Legacy Lineage:**
*   `k_ausd_v_ta_bp_ref.ksh` (FILE) `EXECUTES_SQL` `d_ausd_v_ta_bp_ref.sql` (SQL_SCRIPT)
*   `d_ausd_v_ta_bp_ref.sql` (SQL_SCRIPT) `READS_TABLE` `DWTK_MELDUNGEN` (TABLE)
*   `d_ausd_v_ta_bp_ref.sql` (SQL_SCRIPT) `READS_TABLE` `CDS$TA_BP_REF` (TABLE)
*   `d_ausd_v_ta_bp_ref.sql` (SQL_SCRIPT) `WRITES_TABLE` `SOF$TA_BP_REF` (TABLE)
*   `d_ausd_v_ta_bp_ref.sql` (SQL_SCRIPT) `WRITES_TABLE` `VIA` (TABLE)
*   `d_ausd_v_ta_bp_ref.sql` (SQL_SCRIPT) `USES_PACKAGE` `DWPA_UTIL_SKRIPT` (PACKAGE)

## 5. Transformation Logic

### `k_ausd_v_ta_bp_ref.ksh` (Shell Control Logic) to BigQuery Stored Procedure
*   **Parameter Parsing (`getopts`)**: Replaced by BigQuery Stored Procedure input parameters.
*   **Environment Sourcing (`. $HOME/.dw_init`)**: Replaced by explicit configuration parameters or static values within the BigQuery Stored Procedure, or managed externally by the orchestrator.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**: Their functionalities (error handling, date handling, parameter validation, SQL execution) will be rewritten using BigQuery SQL functions, `IF` statements, `ASSERT`, `RAISE`, and calls to other BigQuery Stored Procedures.
*   **Temporary File (`tmpFile`)**: Replaced by `OUT` parameters for the called stored procedure, or direct inserts/updates to a BigQuery `job_control_table`.
*   **Job Management**: The logic for checking active jobs, registering new jobs, and deactivating old jobs will be implemented as DML operations on a `job_control_table` in BigQuery.
*   **SQL Script Execution (`starteSQLSkript`)**: The invocation of the SQL script will become a `CALL` to a dedicated BigQuery Stored Procedure.

### `d_ausd_v_ta_bp_ref.sql` (Oracle SQL) to BigQuery SQL
*   **Date Function (`TO_CHAR`, `TO_DATE`, `NVL`)**:
    *   `TO_CHAR(MAX(m.timecreated),'YYYYMMDD')` -> `FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated)))`
    *   `TO_DATE('&v_datum','YYYYMMDD')` -> `PARSE_DATE('%Y%m%d', v_datum)`
    *   `NVL(expr, default)` -> `COALESCE(expr, default)`
*   **SQL*Plus Directives (`DEFINE`, `COLUMN new_value`, `START`, `SPOOL`, `WHENEVER SQLERROR`, `SET TIMING ON`, `SET SERVEROUTPUT ON`, `prompt`, `commit`, PL/SQL block)**: These client-side/proprietary commands will be removed. The logic will be handled by BigQuery's procedural language features (e.g., `DECLARE`, `BEGIN...END`, `CALL`).
*   **`TRUNCATE TABLE sof$ta_bp_ref`**: Translated directly to BigQuery's `TRUNCATE TABLE` or `DELETE FROM` statement.
*   **`INSERT INTO ... SELECT`**: Directly portable with syntax adjustments for table references (e.g., `project.dataset.table_name`).
*   **Database Link (`&v_carmen`)**: The reference to `&v_carmen` (e.g., `cds$ta_bp_ref &v_carmen br`) indicates a database link. In BigQuery, this will be handled by ensuring `cds$ta_bp_ref` is already present as a BigQuery table in the appropriate dataset.

## 6. External Dependencies

*   **Oracle Database**: The primary external dependency is the Oracle database from which `isbert_schema.dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`, and `VIA` originate.
    *   **Replacement**: All these tables will be migrated to BigQuery tables. Initial data loading will be performed via one-time data transfers (e.g., BigQuery Data Transfer Service, ETL tools). Subsequent deltas will be handled by incremental loading strategies. The `DWPA_UTIL_SKRIPT` package functionality used for `runstatement` will be reimplemented within BigQuery Stored Procedures.
*   **Filesystem (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/...`, `trace.sql.cfg`, `tmp` directory)**: The legacy script relies on sourcing environment variables and utility scripts from the filesystem, and creating temporary files.
    *   **Replacement**: Environment variables will become BigQuery Stored Procedure parameters or be managed by the orchestrator. Utility script logic will be re-coded into BigQuery SQL or Python functions. Temporary file usage will be replaced by in-memory variables, `OUT` parameters, or direct BigQuery table operations.

## 7. Unresolved / Risks

*   **`r_ausd_vertrag.ksh` reference**: The initial comments in `k_ausd_v_ta_bp_ref.ksh` mention it as a control script for `r_ausd_vertrag.ksh`. While this specific job seems self-contained, a complete understanding of `r_ausd_vertrag.ksh` might be needed for broader context or if `k_ausd_v_ta_bp_ref.ksh` is part of a larger workflow. No direct invocation of `r_ausd_vertrag.ksh` was found in the analyzed script itself.
*   **`DWPA_UTIL_SKRIPT.runstatement`**: The exact functionality of this Oracle package procedure, especially its parameters and potential side effects, needs to be thoroughly understood to ensure accurate replication in BigQuery. Assuming `TRUNCATE TABLE` is its primary action in this context.
*   **`VIA` table `MERGE` operation**: The full `MERGE` statement for the `VIA` table is not present in the provided SQL. Its details are crucial for accurate migration. The `lineage_edges` indicated `WRITES_TABLE:VIA`, suggesting it's part of `d_ausd_v_ta_bp_ref.sql`. This is an assumed detail that needs to be verified.
*   **`v_carmen` DB Link**: While the target BigQuery environment will have all tables locally, the original `&v_carmen` implies `cds$ta_bp_ref` might have been on a different Oracle instance, potentially affecting performance or data synchronization in the legacy setup. This is a non-issue for BigQuery once all data is consolidated.
*   **"Deactivate older active jobs" logic**: The pseudocode covers this with an `UPDATE` on a `job_table`. The exact business rules for what constitutes an "active job" and how they should be deactivated should be confirmed.

## 8. Build Plan

The migration will be executed in the following steps:

1.  **Schema Migration**:
    *   Define DDL for BigQuery tables: `dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`, `VIA`.
    *   Create `job_control_table` and `job_error_log` BigQuery tables.

2.  **Initial Data Load**:
    *   Load historical data from Oracle `dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`, and `VIA` into their respective BigQuery tables.

3.  **BigQuery Stored Procedure Development - Data Processing**:
    *   Create BigQuery Stored Procedure `sp_d_ausd_v_ta_bp_ref.sql` based on the provided BigQuery SQL equivalent from the `hql_sql_to_bqsql_design` tool, ensuring the `MERGE` logic for `VIA` is fully implemented.

4.  **BigQuery Stored Procedure Development - Control Logic**:
    *   Create BigQuery Stored Procedure `sp_ausd_v_ta_bp_ref` using the pseudocode provided by `shellscript_to_bqsql_design`. This includes:
        *   Parameter handling.
        *   Validation logic.
        *   `job_control_table` updates for activation/deactivation.
        *   Call to `sp_d_ausd_v_ta_bp_ref`.
        *   Error logging to `job_error_log`.

5.  **Testing**:
    *   Unit test `sp_d_ausd_v_ta_bp_ref` with various data scenarios.
    *   Unit test `sp_ausd_v_ta_bp_ref` with valid/invalid parameters and different job states.
    *   Integration test the full workflow from `sp_ausd_v_ta_bp_ref` through to `sp_d_ausd_v_ta_bp_ref`.
    *   Validate output data in `sof$ta_bp_ref` and `VIA` against the legacy system's output.

6.  **Scheduling**:
    *   Configure a BigQuery Scheduled Query to execute `CALL project.dataset.sp_ausd_v_ta_bp_ref(p_JobKennung => '...', p_EintragsNr => '...')` with the appropriate parameters. Or, integrate into a Cloud Composer DAG.

This build plan is specific to the migration to BigQuery SQL and Stored Procedures. The build outputs will primarily be `.sql` files containing the BigQuery DDL and Stored Procedure definitions.