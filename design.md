# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

## 1. Purpose & Scope
This shell script, `k_ausd_bp_ta_bpr_basis_his.ksh`, serves as a control script for a batch process. Its primary purpose is to orchestrate the execution of a SQL script (`d_ausd_bp_ta_bpr_basis_his.sql`) that processes data for the `PoolBasisprodukt` table. The script handles environment setup, command-line parameter parsing and validation (job ID, entry number, key date), date validation, and execution of the core SQL logic. It also includes logic to capture the number of processed records and, historically, to log job execution details, although this logging is currently commented out.

The scope of this migration is to re-platform this KornShell script and its dependencies to Google Cloud Platform, leveraging BigQuery for data processing and potentially Cloud Composer (Airflow) for orchestration.

## 2. Source Inventory
The job is composed of a single main file: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh`.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh`
    *   **Technology:** KornShell
    *   **Category:** Shell Script (Pipeline Orchestrator, Environment Bootstrapper, Data Validator)
    *   **Complexity Tier:** Medium
    *   **Migration Bucket:** Semi-Auto
    *   **Purpose:** This script acts as an active controller for an ETL process. It initializes the environment, parses parameters, validates dates, and most importantly, calls an external SQL script (`d_ausd_bp_ta_bpr_basis_his.sql`) to perform data processing. It also handles basic error reporting and attempts to record processed row counts.

## 3. Target Architecture
The target architecture on Google Cloud Platform will consist of:

*   **Orchestration:** Cloud Composer (Apache Airflow) will manage the workflow execution, including parameter passing, task dependencies, and error handling.
*   **Data Processing:** BigQuery will be the primary data warehouse and processing engine. The core SQL logic currently within `d_ausd_bp_ta_bpr_basis_his.sql` will be migrated into BigQuery SQL, likely within a BigQuery Stored Procedure.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring will replace the script's native logging and error reporting mechanisms. A dedicated BigQuery table (`project.dataset.job_log`) will be used to store execution metadata and record counts, replacing the intended but commented-out job table entry.
*   **Parameter Management:** Runtime parameters will be managed via Airflow DAG parameters or BigQuery stored procedure input parameters.
*   **Date Utilities:** Built-in BigQuery date functions (`CURRENT_DATE`, `DATE_SUB`, `PARSE_DATE`) will replace custom shell date scripts.

## 4. Data Flow & Lineage
The original shell script orchestrates the following data flow:

1.  **Parameter Input:** The script accepts `Jobkennung`, `EintragsNr`, `Stichtag`, and `wiederanlaufWert` as command-line arguments.
2.  **Environment & Utilities:** It sources several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) to set up the environment and provide helper functions.
3.  **Validation:** Input parameters and the `Stichtag` date format are validated. Errors lead to script termination.
4.  **SQL Execution:** The script invokes `starteSQLSkript` which in turn executes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_basis_his.sql`.
    *   **Input to SQL:** The SQL script receives parameters like `EintragsNr`, `JobKennung`, `Stichtag`, `p_datum_heute`, and `p_datum_gestern`.
    *   **Output from SQL:** The SQL script reads from unspecified source tables and writes to `PoolBasisprodukt` and possibly other intermediate tables.
    *   **Interim Output:** A temporary file `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis_his.tmp` is created by the SQL execution, containing the count of processed records.
5.  **Record Count & Logging:** The script reads the record count from the temporary file and then (though commented out) would insert this information into a job table.

**Target Lineage:**

*   **Orchestration:** Cloud Composer DAG will be the top-level orchestrator.
*   **Input Parameters:** Airflow DAG parameters will pass `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` to the BigQuery Stored Procedure.
*   **Core Logic:** A BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_bpr_basis_his`) will encapsulate the migrated SQL logic from `d_ausd_bp_ta_bpr_basis_his.sql`. This procedure will read from source tables and write to `PoolBasisprodukt`.
*   **Logging:** The stored procedure will insert execution details, including record counts, into a BigQuery `job_log` table.
*   **Date Functions:** BigQuery's native `CURRENT_DATE()`, `DATE_SUB()`, and `PARSE_DATE()` functions will handle date computations and validations.

## 5. Transformation Logic
The shell script itself contains minimal data transformation logic; its primary role is orchestration and parameter handling. The core data transformations reside within `d_ausd_bp_ta_bpr_basis_his.sql`.

**Shell Script Transformation Logic (Migration to BigQuery Stored Procedure):**

*   **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters.
*   **Parameter Validation (`pruefeParameterGesetzt`):** Replaced by `IF ... THEN SELECT ERROR(...) END IF;` constructs within the BigQuery Stored Procedure.
*   **Date Validation (`DWDate_Datum_Check`):** Replaced by `PARSE_DATE` with error handling (`EXCEPTION WHEN ERROR`) or `REGEXP_CONTAINS` for format validation.
*   **Environment Loading (`. $HOME/.dw_init`):** Replaced by BigQuery dataset/project constants or variables passed via the orchestration layer.
*   **Date Calculation (`gestern.ksh`):** Replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` functions.
*   **SQL Execution (`starteSQLSkript`):** Replaced by direct calls to the BigQuery Stored Procedure or by embedding the SQL logic directly into the procedure.
*   **Record Count (`cat $tmpFile`):** Replaced by `DECLARE v_records INT64; SELECT COUNT(*) ... INTO v_records;` within the BigQuery Stored Procedure.
*   **Job Logging (`FOSJobErzeugeEintrag`):** Replaced by `INSERT INTO project.dataset.job_log (...)` statements.

**SQL Script Transformation Logic (from `d_ausd_bp_ta_bpr_basis_his.sql` - will need separate migration):**

*   This SQL script is responsible for the actual ETL_LOAD into the `PoolBasisprodukt` table. Its specific transformation details are not visible in the shell script analysis, but it will be migrated to BigQuery SQL, potentially within the same stored procedure or as a separate SQL script executed by the orchestrator.

## 6. External Dependencies
The original script has the following external dependencies:

*   **File System:**
    *   `$HOME/.dw_init`: Environment initialization file.
    *   `bert_k_ausd_bp_ta_bpr_basis_his.tmp`: Temporary file for record counts.
    *   Commented-out references to `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat` for post-processing.
*   **External Scripts/Utilities:**
    *   `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`: Helper KornShell scripts.
    *   `gestern.ksh`: Date calculation utility.
    *   `d_ausd_bp_ta_bpr_basis_his.sql`: The main SQL script containing the ETL logic.
    *   `getopts`, `set`, `print`, `cat`, `eval`: Standard shell built-ins/commands.
    *   Commented-out `sed`, `sort`, `join`: Standard Unix utilities for file manipulation.
*   **Job Management System:** (Commented out in the source)
    *   `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`: Functions likely interacting with a legacy job management system.

**Replacement in Target Architecture:**

*   **File System Dependencies:** Replaced by BigQuery tables (for temporary data and `PoolBasisprodukt`), Cloud Storage (if external file exports/imports are required), or BigQuery stored procedure variables/temp tables.
*   **External Scripts/Utilities:**
    *   All KornShell helper scripts will be replaced by BigQuery SQL functions, procedures, or logic embedded within the main BigQuery stored procedure.
    *   `gestern.ksh` will be replaced by BigQuery's native date functions.
    *   `d_ausd_bp_ta_bpr_basis_his.sql` will be refactored into BigQuery SQL, forming the core of the BigQuery Stored Procedure.
    *   Shell built-ins (`getopts`, `cat`, `eval`, etc.) will be replaced by BigQuery SQL scripting constructs, stored procedure parameters, and BigQuery DML/DDL.
    *   `sed`, `sort`, `join` (if ever reactivated) will be replaced by BigQuery SQL transformations on tables.
*   **Job Management System:** Replaced by the `project.dataset.job_log` BigQuery table for audit trails and Cloud Composer's native monitoring and logging features.

## 7. Unresolved / Risks
*   **SQL Script `d_ausd_bp_ta_bpr_basis_his.sql`:** The actual SQL code for this script was not provided in the analysis. Its complexity, specific database interactions, and use of proprietary SQL features (if any) are currently unknown. This is the main piece of the ETL logic that needs detailed analysis and migration.
*   **Legacy `starteSQLSkript` Implementation:** The `starteSQLSkript` function's exact implementation is unknown. It likely involves connecting to a legacy database (e.g., Oracle via SQL*Plus). The migration will assume this function primarily wraps a SQL execution. Any complex logic within it would need to be identified and re-implemented.
*   **Commented-out Logic:** The script contains significant commented-out sections related to `FOSJob` functions and file post-processing (sed, sort, join). It is assumed this logic is not currently active and will not be migrated unless explicitly requested and validated as still necessary. If reactivated, it would constitute additional work.
*   **Error Code Semantics:** The script uses specific error codes (192, 193). While the migration will replicate error handling, ensuring exact semantic compatibility of error codes with upstream systems might require careful mapping or adjustment in the target BigQuery environment.
*   **`dw_init` contents:** The contents of `$HOME/.dw_init` are unknown. This file likely defines environment variables and paths crucial for the script's execution. These definitions need to be identified and translated into Cloud Composer environment variables or BigQuery constants.

## 8. Build Plan
The migration will result in a Cloud Composer DAG orchestrating a BigQuery Stored Procedure.

1.  **Extract `d_ausd_bp_ta_bpr_basis_his.sql`:**
    *   **Tool:** `read_source_file` (if available in lineage) or manual extraction.
    *   **Language:** SQL
    *   **Description:** Obtain the content of the main SQL script.

2.  **Migrate SQL Script to BigQuery Stored Procedure SQL:**
    *   **Tool:** `hql_sql_to_bqsql_design` (or manual if specific features not covered)
    *   **Language:** BQSQL
    *   **Description:** Convert the SQL script (`d_ausd_bp_ta_bpr_basis_his.sql`) into a BigQuery Stored Procedure. This will involve converting syntax, data types, and any proprietary functions to BigQuery equivalents. Parameters previously passed to `starteSQLSkript` will become `IN` parameters for the stored procedure. This procedure will be named `project.dataset.r_ausd_bp_ta_bpr_basis_his`.

3.  **Create BigQuery Job Log Table:**
    *   **Tool:** Manual DDL
    *   **Language:** BQSQL DDL
    *   **Description:** Create a table `project.dataset.job_log` to store job execution metadata, status, and record counts.
    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
      job_name STRING,
      status STRING,
      error_nr INT64,
      error_arg STRING,
      stichtag DATE,
      records_processed INT64,
      created_at TIMESTAMP
    );
    ```

4.  **Develop Cloud Composer (Airflow) DAG:**
    *   **Tool:** Manual development, potentially `uc4_to_airflow_dag_design` if more complex orchestration patterns are present in the full context.
    *   **Language:** Python
    *   **Description:** Create an Airflow DAG that:
        *   Accepts `Jobkennung`, `EintragsNr`, `Stichtag`, and `wiederanlaufWert` as DAG parameters.
        *   Contains a BigQuery operator (e.g., `BigQueryExecuteQueryOperator`) to call the `project.dataset.r_ausd_bp_ta_bpr_basis_his` stored procedure, passing the parameters.
        *   Configures retry mechanisms and dependency handling.

5.  **Refactor Date and Utility Logic:**
    *   **Tool:** Manual BQSQL/Python implementation
    *   **Language:** BQSQL/Python
    *   **Description:** Replace the functionality of `gestern.ksh`, `h_alis_date.ksh`, and parameter validation scripts with native BigQuery SQL functions within the stored procedure and/or Python logic within the Airflow DAG.

6.  **Deployment:**
    *   Deploy the BigQuery Stored Procedure.
    *   Deploy the Airflow DAG to Cloud Composer.

This build plan focuses on the current active logic. If any commented-out sections are to be revived, additional build steps for those components will be required.