# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

## 1. Purpose & Scope

This job, orchestrated by `k_ausd_bp_ta_p_basisprod.ksh`, is a control wrapper for an Oracle SQL script, `d_ausd_bp_ta_p_basisprod.sql`. Its primary purpose is to prepare and populate the `sof$ta_p_basisprod` table, which appears to be a core "Basisprodukt" (base product) table in the legacy data warehouse. The job handles parameter parsing, environment setup, date validation, and the execution of the main SQL logic. It also includes mechanisms for tracking record counts and potential job auditing. The migration scope is to translate this entire workflow, including both orchestration and data transformation, to Google Cloud's BigQuery platform.

## 2. Source Inventory

The job consists of two primary source files:

1.  **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh**
    *   **Technology:** KornShell (ksh) script
    *   **Category:** Shell, Control Script
    *   **Summary:** Acts as an orchestration layer, handling parameter validation, date checks, environment setup, and calling an underlying SQL script. It also sources several utility ksh scripts.
    *   **Complexity Tier:** Undetermined (information not available from `file_complexity`). Given its role as a wrapper with parameter handling and external script calls, it is likely to be at least `medium` complexity.
    *   **Automation Bucket:** Undetermined (information not available from `automation_rate`).

2.  **vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql**
    *   **Technology:** Oracle SQL (PL/SQL implicitly for the stored procedure call)
    *   **Category:** SQL, Data Transformation
    *   **Summary:** This script performs the core data transformation and loading. It truncates and inserts data into the `sof$ta_p_basisprod` table by joining several source tables. It uses Oracle-specific syntax and hints.
    *   **Complexity Tier:** Undetermined (information not available from `file_complexity`). Given the complex `INSERT ... SELECT` statement with multiple joins, a subquery, and Oracle-specific features, it is likely `complex`.
    *   **Automation Bucket:** Undetermined (information not available from `automation_rate`).

**Auxiliary Scripts (sourced by `k_ausd_bp_ta_p_basisprod.ksh`):**
*   `. $HOME/.dw_init` (environment setup)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date validation)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (date calculation: yesterday/today)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus invocation helper)

## 3. Target Architecture

The target platform is BigQuery.
*   **Orchestration:** The shell script's orchestration logic will be re-implemented using BigQuery Scripting within a BigQuery Stored Procedure. This will handle parameter passing, validation, and control flow.
*   **Data Transformation:** The Oracle SQL `INSERT ... SELECT` statement will be translated into a BigQuery SQL query, likely encapsulated within another BigQuery Stored Procedure or a scheduled query.
*   **Data Storage:** All source and target tables (`sof$ta_cntrct_dist`, `sof$ta_iccid_vertrag`, `sof$ta_rn_vertrag`, `sof$ta_rn_da_vda_tk`, `sof$ta_tarifoption`, `sof$ta_apn_vertrag`, `SOF$TA_BCP_ICCID`, `SOF$TA_BCP_MSISDN`, `isbert_schema.dwtk_meldungen`, `sof$ta_p_basisprod`) will be migrated to BigQuery tables within appropriate datasets (e.g., `legacy_dw_raw`, `legacy_dw_stage`, `legacy_dw_prod`).
*   **Utility Functions:** Shell script utilities will be replaced by equivalent BigQuery Scripting constructs or custom BigQuery UDFs/stored procedures where complex logic dictates. For example, date calculations will use BigQuery's `CURRENT_DATE()` and `DATE_SUB()`. Error handling will leverage BigQuery Scripting's `ASSERT` and `RAISE` statements, along with logging to an audit table.

## 4. Data Flow & Lineage

The original job flow:
1.  **`k_ausd_bp_ta_p_basisprod.ksh` (Control Script):**
    *   Initializes environment by sourcing various utility KSH scripts.
    *   Parses command-line arguments for job identification, entry number, and a key date (`p_Stichtag`).
    *   Validates mandatory parameters and the format of `p_Stichtag`.
    *   Calls `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`.
    *   Reads `MAX(m.timecreated)` from `isbert_schema.dwtk_meldungen` where `m.job_kennung = 'BERT_DROP_TEMP_TABLE'` to define `v_datum`.
    *   Invokes a helper function (`starteSQLSkript` via `h_alis_sqlplus.ksh`) to execute `d_ausd_bp_ta_p_basisprod.sql`, passing several parameters.
    *   Captures a record count from a temporary file (`tmpFile`).
    *   Potentially creates an entry in a job-tracking table (commented out in source).

2.  **`d_ausd_bp_ta_p_basisprod.sql` (Data Transformation Script):**
    *   Defines a variable `v_carmen`.
    *   Connects to an Oracle database (implied by `SQL*Plus` commands).
    *   Truncates the target table `sof$ta_p_basisprod` using an Oracle stored procedure (`isbert_schema.dwpa_util_skript.runstatement`).
    *   Executes a large `INSERT /*+ APPEND */ INTO sof$ta_p_basisprod SELECT ...` statement.
        *   **Reads from:** `sof$ta_cntrct_dist cn`, `sof$ta_iccid_vertrag icc`, `sof$ta_rn_vertrag msi`, `sof$ta_rn_da_vda_tk msd`, `sof$ta_tarifoption opt`, `sof$ta_apn_vertrag av`, and an inline view derived from `SOF$TA_BCP_ICCID BC` and `SOF$TA_BCP_MSISDN BCM`.
        *   **Writes to:** `sof$ta_p_basisprod`.
    *   Commits the transaction.
    *   Includes commented-out `TRUNCATE` and `DROP` statements for various intermediate tables.

**Target BigQuery Data Flow:**
1.  A top-level BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_bp_ta_p_basisprod`, will act as the orchestrator.
2.  This procedure will handle parameter validation and derive date values using BigQuery functions.
3.  It will call another BigQuery Stored Procedure, e.g., `project.dataset.d_ausd_bp_ta_p_basisprod`, for the main data transformation.
4.  The `d_ausd_bp_ta_p_basisprod` procedure will:
    *   Truncate the target BigQuery table `project.dataset.sof_ta_p_basisprod`.
    *   Execute the translated `INSERT INTO ... SELECT ...` statement, joining the corresponding BigQuery source tables.
5.  The orchestrating procedure will capture the `COUNT(*)` from the target table after insertion.
6.  It will then insert an audit record into a BigQuery audit table (e.g., `project.dataset.job_audit`).

## 5. Transformation Logic

**Orchestration Logic (`k_ausd_bp_ta_p_basisprod.ksh` to BigQuery Scripting):**
*   **Parameter Handling:** Shell `getopts` will be replaced by BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING`, `IN p_EintragsNr STRING`, `IN p_Stichtag STRING`, `IN p_wiederanlaufWert INT64`).
*   **Variable Declarations:** Shell variables (`v_TabName`, `tmpFile`, `p_datum_heute`, `p_datum_gestern`) will become `DECLARE` statements in BigQuery Scripting.
*   **Date Logic:** `gestern.ksh` and `h_alis_date.ksh` will be replaced by BigQuery's native date functions: `CURRENT_DATE()`, `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`, and `REGEXP_CONTAINS` for date format validation.
*   **Error Handling:** `if [ ! $ErrNr -eq 0 ]` and `DWMSG_MeldeFehler` will be translated to `IF ... THEN RAISE USING MESSAGE ... END IF;` statements. `set -e`/`set +e` logic will be managed by BigQuery's transaction and error handling mechanisms.
*   **SQL Script Execution:** The `starteSQLSkript` invocation will be replaced by a `CALL` statement to the BigQuery Stored Procedure that encapsulates the `d_ausd_bp_ta_p_basisprod.sql` logic.
*   **Record Count:** `eval "v_records=\`cat $tmpFile\`"` will be replaced by `SELECT COUNT(*) FROM target_table` assigned to a `DECLARE`d variable.
*   **Job Auditing:** The commented-out `FOSJobErzeugeEintrag` will be implemented as an `INSERT` statement into a dedicated BigQuery audit table.

**Data Transformation Logic (`d_ausd_bp_ta_p_basisprod.sql` to BigQuery SQL):**
*   **Table Names:** Oracle table names like `sof$ta_p_basisprod` will be mapped to BigQuery-compliant names, e.g., `sof_ta_p_basisprod`, prefixed with `project.dataset.`.
*   **`DEFINE` variables:** Oracle `DEFINE` will be replaced by BigQuery `DECLARE` or literal values.
*   **`COLUMN ... new_value`:** This mechanism to fetch a value into a SQL*Plus variable will be replaced by a `DECLARE` variable and `SET variable = (SELECT ...)`.
*   **`TRUNCATE TABLE ... REUSE STORAGE`:** Will map directly to BigQuery's `TRUNCATE TABLE` statement. The call to `isbert_schema.dwpa_util_skript.runstatement` needs to be investigated further if `runstatement` has complex logic beyond simple DDL. For basic truncate, a direct BigQuery `TRUNCATE` is sufficient. If `runstatement` performs dynamic SQL or other complex operations, it would need a dedicated BigQuery stored procedure.
*   **`INSERT /*+ APPEND */ INTO ... SELECT ...`:** This will be directly translated to BigQuery's `INSERT INTO ... SELECT ...` statement.
    *   **Oracle Hints:** `/*+ ORDERED NO_SWAP_JOIN_INPUTS(...) FULL(...) parallel(...) */` are Oracle-specific and will be removed. BigQuery's optimizer handles query execution plans automatically.
    *   **`decode` function:** Will be translated to BigQuery's `CASE` statement. `decode(av.apn, null,av.apn, av.apn||\',\'||av.apn_cntrct)` becomes `CASE WHEN av.apn IS NULL THEN av.apn ELSE CONCAT(av.apn, ',', av.apn_cntrct) END`.
    *   **Joins:** Oracle `(+)` for outer joins will be explicitly translated to `LEFT JOIN` in BigQuery.
    *   **Date Functions:** `TO_CHAR(MAX(m.timecreated),'YYYYMMDD')` will be translated to BigQuery's `FORMAT_DATE('%Y%m%d', MAX(m.timecreated))`.
*   **`COMMIT;`:** BigQuery operates on an auto-commit model for DML statements within scripts, or explicit transactions can be used if multiple statements need to be atomic.
*   **Commented-out `TRUNCATE`/`DROP` statements:** These indicate temporary table cleanup, which should be managed in the BigQuery environment using temporary tables, or explicit `DROP TABLE` at the end of a session/procedure.

## 6. External Dependencies

1.  **Oracle Database:** The primary external dependency is the Oracle database from which the source tables (`sof$ta_cntrct_dist`, `sof$ta_iccid_vertrag`, `sof$ta_rn_vertrag`, `sof$ta_rn_da_vda_tk`, `sof$ta_tarifoption`, `sof$ta_apn_vertrag`, `SOF$TA_BCP_ICCID`, `SOF$TA_BCP_MSISDN`, `isbert_schema.dwtk_meldungen`) are read and the target table (`sof$ta_p_basisprod`) is written to.
    *   **Replacement:** These tables must be migrated to BigQuery. This typically involves an initial data load, followed by incremental data synchronization mechanisms (e.g., CDC from Oracle to GCS/BigQuery, or batch extracts and loads).
2.  **Oracle Stored Procedure `isbert_schema.dwpa_util_skript.runstatement`:** Used for truncating `sof$ta_p_basisprod`.
    *   **Replacement:** If its function is solely `TRUNCATE TABLE`, it can be replaced by a direct `TRUNCATE TABLE` statement in BigQuery. If it involves complex dynamic SQL or other logic, a dedicated BigQuery stored procedure would be required.
3.  **Local File System / Shell Utilities:**
    *   `$HOME/.dw_init`: Environment setup script.
    *   `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`: Generic utility scripts.
    *   `gestern.ksh`: Script to calculate yesterday's and today's date.
    *   Temporary files (`tmpFile`): Used for inter-process communication (e.g., storing record counts).
    *   Commented `sed`, `sort`, `join` commands: Indicate potential historical or alternative file-based processing.
    *   **Replacement:** These will be replaced by BigQuery Scripting variables, functions, and standard BigQuery SQL constructs. Logging will be handled by BigQuery's native logging capabilities or a dedicated audit table.

## 7. Unresolved / Risks

*   **Missing Complexity/Automation Data:** The `file_complexity` and `automation_rate` for both `k_ausd_bp_ta_p_basisprod.ksh` and `d_ausd_bp_ta_p_basisprod.sql` are unavailable. This might impact the overall effort estimation and migration strategy. Manual assessment determined them to be at least `medium` and `complex` respectively.
*   **Oracle-Specific Syntax/Features:** The Oracle SQL script contains specific syntax (e.g., `decode`, `(+)` for outer join, `/*+ ... */` hints) and potentially functions not directly available in BigQuery. These will require careful translation and testing.
*   **`isbert_schema.dwpa_util_skript.runstatement` Functionality:** A deeper dive is needed to confirm `dwpa_util_skript.runstatement` solely executes DDL. If it has more complex logic or dependencies, this represents a hidden dependency.
*   **Commented-out Code:** The script contains significant commented-out sections (e.g., `sed`, `sort`, `join` commands, `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`). These indicate potential legacy functionalities or alternative paths that should be clarified to ensure no business logic is missed during migration.
*   **Environment Variables:** The reliance on `BERT_DIR_ROOT`, `DW_DIR_UTL`, etc., indicates a dependency on the legacy environment setup. These paths and configurations must be mapped to BigQuery project/dataset structure or configuration variables.
*   **Character Encoding:** Potential issues with character encoding (e.g., "ä", "ö", "ü") from the legacy system to BigQuery.

## 8. Build Plan

The migration will involve the following steps and generated components:

1.  **BigQuery DDL for Target Tables:**
    *   **Language:** BigQuery DDL
    *   **Files:** `sof_ta_p_basisprod.ddl`, `job_audit_table.ddl`
    *   **Description:** Create the `sof_ta_p_basisprod` table in BigQuery, mirroring the structure of the Oracle `sof$ta_p_basisprod`. Create an audit table for job tracking.

2.  **BigQuery Stored Procedure for Data Transformation:**
    *   **Language:** BigQuery SQL (within a Stored Procedure)
    *   **File:** `d_ausd_bp_ta_p_basisprod_sp.sql`
    *   **Description:** Translate `d_ausd_bp_ta_p_basisprod.sql` into a BigQuery Stored Procedure. This procedure will truncate `sof_ta_p_basisprod` and execute the `INSERT INTO ... SELECT ...` query, with all Oracle-specific syntax converted to BigQuery equivalents.

3.  **BigQuery Stored Procedure for Orchestration:**
    *   **Language:** BigQuery SQL (BigQuery Scripting within a Stored Procedure)
    *   **File:** `k_ausd_bp_ta_p_basisprod_sp.sql`
    *   **Description:** Re-implement the logic of `k_ausd_bp_ta_p_basisprod.ksh` as a BigQuery Stored Procedure. This will handle parameter validation, date calculations, calling `d_ausd_bp_ta_p_basisprod_sp`, capturing record counts, and logging to the `job_audit` table.

4.  **Data Ingestion Plan:**
    *   **Language:** GCS/Cloud Storage, Data Transfer Service, or other ETL tools.
    *   **Description:** Define a strategy to ingest the source Oracle tables (`sof$ta_cntrct_dist`, `sof$ta_iccid_vertrag`, etc.) into BigQuery. This could involve batch exports, CDC, or a combination depending on data volume and latency requirements.

5.  **Scheduling/Orchestration Configuration:**
    *   **Language:** YAML/JSON for Cloud Composer DAG, Cloud Workflows, or BigQuery Scheduled Queries.
    *   **Description:** Configure a mechanism to schedule and run the top-level BigQuery Stored Procedure (`k_ausd_bp_ta_p_basisprod_sp`) according to its original schedule and parameter requirements.

6.  **Unit and Integration Tests:**
    *   **Language:** BigQuery SQL, Python (for calling BigQuery API).
    *   **Description:** Develop test cases to ensure the migrated logic produces the same results as the legacy system, covering various parameter inputs and data scenarios.