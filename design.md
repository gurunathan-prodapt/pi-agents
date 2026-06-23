# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration plan for the KornShell script `k_ausd_bp_ta_apn_vertrag.ksh` to Google BigQuery.

The original script (`k_ausd_bp_ta_apn_vertrag.ksh`) is a control script that orchestrates parameter validation, date validation, SQL execution, and post-processing related to record counts. Its primary purpose is to manage a database extraction/load workflow for a logical entity referred to as `PoolBasisprodukt`. The job was assembled from 1 component(s) and its stage distribution is noted as medium complexity.

The core business transformation logic is assumed to reside within the SQL script `d_ausd_bp_ta_apn_vertrag.sql` that this shell script executes. This migration design focuses on porting the orchestration, parameter handling, and data flow of the shell script to a BigQuery environment, including BigQuery SQL scripting and potentially external orchestration if complex shell logic requires it.

## 2. Source Inventory
The job consists of a single KornShell script:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`
    *   **Technology:** KornShell (`ksh`)
    *   **Category:** shell
    *   **Complexity Tier:** medium
    *   **Migration Bucket:** semi_auto
    *   **Purpose:** ETL Orchestration / Control Script

## 3. Target Architecture
The migrated solution will primarily leverage Google BigQuery's capabilities, aiming for a serverless and managed environment.

*   **Core Logic:** The orchestration logic (parameter parsing, validation, date derivation) will be translated into a BigQuery Stored Procedure written in BigQuery SQL Scripting.
*   **Data Processing:** The SQL script `d_ausd_bp_ta_apn_vertrag.sql` will be refactored into a BigQuery SQL script or another BigQuery Stored Procedure, performing the actual data transformations and loads.
*   **Temporary Data:** File-based temporary outputs (like `bert_k_ausd_bp_ta_apn_vertrag.tmp`) will be replaced by BigQuery temporary tables or variables within the stored procedure.
*   **Logging and Error Handling:** The custom framework logging (`DWMSG_MeldeFehler`) and parameter validation (`pruefeParameterGesetzt`) will be replaced with BigQuery `ASSERT` statements, `BEGIN...EXCEPTION` blocks, and potentially dedicated BigQuery logging tables.
*   **Orchestration:** The execution of the BigQuery Stored Procedure will be managed by a modern orchestrator like Cloud Composer (Airflow) or Cloud Workflows, replacing the original shell-based job scheduling and management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`).

## 4. Data Flow & Lineage
The original script `k_ausd_bp_ta_apn_vertrag.ksh` acts as an orchestrator, driving the execution of an underlying SQL script.

*   **Input Parameters:** The script accepts `j` (Jobkennung), `f` (EintragsNr), `s` (Stichtag), and `l` (Wiederanlaufwert) as command-line arguments. These parameters are crucial for controlling the data extraction/load process.
*   **Environment Initialization:** It sources `$HOME/.dw_init` for environment variables and several utility KornShell scripts from `${BERT_DIR_ROOT}/allgemein/is/util/bin/` for error handling (`f_alis_msgerr.ksh`), date utilities (`h_alis_date.ksh`), parameter parsing (`h_alis_parameter.ksh`), and SQLPlus interaction (`h_alis_sqlplus.ksh`).
*   **Date Derivation:** It invokes `gestern.ksh` to derive `p_datum_heute` and `p_datum_gestern`.
*   **SQL Execution:** The script's primary function is to execute the SQL script `d_ausd_bp_ta_apn_vertrag.sql` via a `starteSQLSkript` function call. This SQL script is presumed to perform the actual data processing and output generation.
*   **Record Count:** The script reads a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_vertrag.tmp`) which is populated by the `starteSQLSkript` function.
*   **Audit/Job Entry (commented):** There is commented-out logic to create an entry in a job table via `FOSJobErzeugeEintrag`.
*   **Commented Post-Processing:** There are commented-out sections indicating shell commands (`sed`, `sort`, `join`) for post-processing data files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`). These are currently inactive but represent potential future scope if re-enabled.

In BigQuery, this flow will be re-engineered as:
1.  **Orchestrator (Cloud Composer/Workflows):** Triggers the main BigQuery Stored Procedure, passing required parameters.
2.  **BigQuery Stored Procedure (Orchestration Layer):**
    *   Receives parameters from the orchestrator.
    *   Performs parameter and date validations using BigQuery SQL scripting constructs.
    *   Derives `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` for today and yesterday.
    *   Calls another BigQuery Stored Procedure (the migrated `d_ausd_bp_ta_apn_vertrag.sql`) with relevant parameters.
    *   Retrieves record counts directly from the target tables using `COUNT(*)`.
    *   Inserts audit/job entries into a BigQuery audit table.
    *   Handles errors and logging within BigQuery SQL Scripting.

## 5. Transformation Logic

The KornShell script itself is an orchestration layer, so the transformation logic focuses on its control flow and parameter handling.

*   **Parameter Parsing (`getopts`):**
    *   **Legacy:** `while getopts ":h$ParamList" param do case $param in ... esac done`
    *   **Target:** Replaced by parameters of the BigQuery Stored Procedure (e.g., `p_JobKennung STRING`, `p_EintragsNr STRING`, `p_Stichtag STRING`, `p_wiederanlaufWert INT64`).
*   **Parameter Validation (`pruefeParameterGesetzt`):**
    *   **Legacy:** Custom shell function `pruefeParameterGesetzt Jobkennung p_JobKennung`
    *   **Target:** `IF p_JobKennung IS NULL OR p_JobKennung = '' THEN ... END IF;` with `ASSERT` or custom error logging in BigQuery.
*   **Date Validation (`DWDate_Datum_Check`):**
    *   **Legacy:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'`
    *   **Target:** `IF SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL THEN ... END IF;`
*   **Restart Value Initialization:**
    *   **Legacy:** `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi`
    *   **Target:** `DECLARE p_wiederanlaufWert INT64 DEFAULT 0;` or `SET p_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);`
*   **Date Derivation (`gestern.ksh`):**
    *   **Legacy:** `set `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh``
    *   **Target:** `DECLARE v_datum_heute DATE = CURRENT_DATE(); DECLARE v_datum_gestern DATE = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`
*   **SQL Script Execution (`starteSQLSkript`):**
    *   **Legacy:** `starteSQLSkript $p_EintragsNr $Name_SQLskript ...`
    *   **Target:** `CALL dataset.d_ausd_bp_ta_apn_vertrag_proc(...)` where `d_ausd_bp_ta_apn_vertrag_proc` is the migrated BigQuery Stored Procedure corresponding to `d_ausd_bp_ta_apn_vertrag.sql`.
*   **Record Count from Temp File (`cat $tmpFile`):**
    *   **Legacy:** `eval "v_records=`cat $tmpFile`"`
    *   **Target:** `SET v_records = (SELECT COUNT(*) FROM dataset.PoolBasisprodukt_staging WHERE ...);`
*   **Error Handling:**
    *   **Legacy:** `DWMSG_MeldeFehler`, `exit $ErrNr`
    *   **Target:** `INSERT INTO dataset.error_log (...)`, `SELECT 'FEHLER' AS status, ...`, `LEAVE;` (or `RAISE` if within a `BEGIN...EXCEPTION` block).
*   **Commented File Manipulation (`sed`, `sort`, `join`):**
    *   **Legacy:** If these sections are reactivated, the shell commands would need to be translated to BigQuery SQL functions (string manipulation, `ORDER BY`, joins, window functions) or managed through Cloud Dataflow/Dataproc for complex file processing if it cannot be handled purely in SQL.

## 6. External Dependencies
The original script has several external dependencies, mainly other shell scripts and an SQL script. There are no explicit external systems like Oracle or SFTP found in the `lineage_assembled_jobs` record, but the use of `SQL-Skript` implies a database interaction.

*   **Utility KornShell Scripts:**
    *   `$HOME/.dw_init` (environment setup)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQLPlus routines)
    *   **Replacement:** These will be replaced by native BigQuery SQL scripting features, BigQuery functions, and BigQuery stored procedures. Environment variables will become BigQuery script variables or parameters.
*   **Date Derivation Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (derives yesterday's date)
    *   **Replacement:** Replaced by BigQuery date functions: `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **Core SQL Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql` (performs core data logic)
    *   **Replacement:** This SQL script will be migrated to a BigQuery SQL script or a BigQuery Stored Procedure, adapting its SQL syntax and database interactions to BigQuery.
*   **Temporary Files:**
    *   `$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_vertrag.tmp`
    *   **Replacement:** BigQuery temporary tables, table variables, or direct `COUNT(*)` operations on target tables.
*   **Job Management Framework:**
    *   Implicit calls like `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`.
    *   **Replacement:** Replaced by an audit/job logging table in BigQuery and orchestration-level job management (e.g., Cloud Composer).
*   **Commented External Shell Utilities:** `sed`, `sort`, `join`, `cat`.
    *   **Replacement:** If these sections become active, their functionality will be replaced by BigQuery SQL string functions, window functions, `ORDER BY`, and SQL JOINs.

## 7. Unresolved / Risks
*   **Lineage Detail:** The `lineage_edges` query for this job returned no direct relationships for the script or its referenced files. This indicates that the automated lineage detection might not have captured all dependencies for this specific job. The design is based on static code analysis. If the actual runtime behavior involves more complex dynamic dependencies, further manual analysis may be required during implementation.
*   **`r_ausd_bp_ta_apn_vertrag.ksh`:** The purpose note indicates the script is a "Kontrollscript zu r_ausd_bp_ta_apn_vertrag.ksh". The exact role and whether `r_ausd_bp_ta_apn_vertrag.ksh` is part of this migration or an external dependency that also needs migration is unclear. Assuming `k_ausd_bp_ta_apn_vertrag.ksh` is the main entry point for this job for now.
*   **`BERT_DIR_ROOT` and `DW_DIR_UTL`:** These environment variables will need to be configured in the target environment, either as parameters to the stored procedure, environment variables in the orchestrator, or constants within the BigQuery script.
*   **`d_ausd_bp_ta_apn_vertrag.sql` content:** The actual SQL in this file is crucial and will need separate migration and testing to ensure functional equivalence in BigQuery. The current design assumes this SQL can be directly translated to BigQuery SQL.
*   **Commented Code:** The sections with `sed`, `sort`, `join` are commented out. This design assumes they remain inactive. If they need to be activated, their migration will add complexity and potential new requirements for BigQuery SQL or external processing (e.g., Dataflow).
*   **Error Codes and Messages:** The specific error codes (192, 193) and messages from the original framework will need to be mapped to a BigQuery-compatible logging mechanism, possibly with a custom error code mapping table.
*   **`PoolBasisprodukt` table:** The script refers to `v_TabName='PoolBasisprodukt'`. The exact definition and usage of this table, and whether it's a source, target, or intermediate table, needs to be confirmed during the `d_ausd_bp_ta_apn_vertrag.sql` migration.

## 8. Build Plan
The migration will involve creating BigQuery objects and potentially orchestration components.

1.  **Migrate `d_ausd_bp_ta_apn_vertrag.sql` to BigQuery SQL Stored Procedure:**
    *   **Language:** BigQuery SQL Scripting
    *   **Output:** `dataset.d_ausd_bp_ta_apn_vertrag_proc` (BigQuery Stored Procedure)
    *   **Details:** This will involve converting the original SQL syntax, schema references, and any procedural logic to BigQuery compatible constructs.
2.  **Create Orchestration BigQuery Stored Procedure for `k_ausd_bp_ta_apn_vertrag.ksh`:**
    *   **Language:** BigQuery SQL Scripting
    *   **Output:** `dataset.k_ausd_bp_ta_apn_vertrag_sp` (BigQuery Stored Procedure)
    *   **Details:** This procedure will implement the parameter parsing, validation, date derivation, calling of `d_ausd_bp_ta_apn_vertrag_proc`, record counting, and audit logging as described in Section 5.
3.  **Define BigQuery Audit/Error Logging Tables:**
    *   **Language:** BigQuery DDL
    *   **Output:** `dataset.job_audit`, `dataset.error_log` (BigQuery Tables)
    *   **Details:** These tables will capture job execution status, parameters, record counts, and any errors encountered during the BigQuery job.
4.  **Develop Orchestration (e.g., Cloud Composer/Workflows):**
    *   **Language:** Python (for Airflow DAGs) or YAML/JSON (for Cloud Workflows)
    *   **Output:** Cloud Composer DAG or Cloud Workflow definition
    *   **Details:** This will be responsible for triggering `dataset.k_ausd_bp_ta_apn_vertrag_sp`, passing the necessary parameters, and handling scheduling and retries.
5.  **Configuration:**
    *   **Language:** YAML/JSON (for configuration files)
    *   **Output:** Configuration files for project ID, dataset name, target table names, and parameter mappings.

This plan assumes a semi-automated migration approach, where the bulk of the logic can be translated, but some manual refactoring and integration work will be required.