# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_vvl_upgrade.ksh`, serves as a control wrapper for a data-processing SQL job. Its primary purpose is to orchestrate the execution of a SQL script (`d_ausd_v_ta_vvl_upgrade.sql`) that updates the `ta_vvl_upgrade` table. The script handles environment setup, parameter parsing and validation, error handling, and logging the number of processed records. It also incorporates logic to ignore already active jobs and deactivate old ones.

The scope of this migration involves re-implementing this orchestration logic and the underlying SQL data transformation in Google Cloud's BigQuery platform, aiming for a fully managed and scalable solution.

## 2. Source Inventory
The job consists of a single primary source file, a KornShell script, with several dependent utility scripts and a core SQL script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh`
    *   **Technology:** Shell (KornShell)
    *   **Tier:** Medium (inferred, as detailed complexity data was not available for this file. The script involves parameter parsing, error handling, and external script invocation, which contributes to medium complexity.)
    *   **Automation Bucket:** B2 (Semi-Automated) - This category is chosen due to the need to refactor shell scripting constructs (e.g., `getopts`, `eval`, sourcing other scripts) into BigQuery-compatible logic (e.g., stored procedures, Python orchestration).

## 3. Target Architecture
The migrated solution will reside entirely within Google BigQuery.

*   **Orchestration:** The control logic currently in `k_ausd_v_ta_vvl_upgrade.ksh` will be migrated into a BigQuery Stored Procedure. This procedure will handle parameter validation, error logging, and the invocation of the core data transformation logic. For external orchestration (e.g., scheduling), Google Cloud Composer (Apache Airflow) or Workflows could be used, or Dataform for data pipeline orchestration if applicable.
*   **Data Transformation:** The SQL logic from `d_ausd_v_ta_vvl_upgrade.sql` will be converted into a separate BigQuery Stored Procedure or a series of SQL statements within the main control procedure, directly operating on BigQuery tables.
*   **Parameter Management:** Command-line parameters (`p_JobKennung`, `p_EintragsNr`) will be implemented as `IN` parameters for the BigQuery Stored Procedure.
*   **Logging and Error Handling:** BigQuery's native scripting capabilities will be used for error handling (`RAISE USING MESSAGE`) and logging (e.g., inserting into dedicated BigQuery logging/audit tables).
*   **Temporary Data:** The temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_upgrade_$$.tmp`) used to store record counts will be replaced by a BigQuery scripting variable, an `OUT` parameter from a sub-procedure, or by querying an intermediate staging/result table.
*   **Data Storage:** The `ta_vvl_upgrade` table (and any source tables used by `d_ausd_v_ta_vvl_upgrade.sql`) will be migrated to BigQuery tables.

## 4. Data Flow & Lineage
The original process flow:

1.  **Environment Setup:** `k_ausd_v_ta_vvl_upgrade.ksh` sources `$HOME/.dw_init` and utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Input:** The script receives `p_JobKennung` and `p_EintragsNr` as command-line arguments.
3.  **Parameter Validation:** `h_alis_parameter.ksh` provides `pruefeParameterGesetzt` for validation.
4.  **SQL Script Execution:** The script dynamically constructs the path to `d_ausd_v_ta_vvl_upgrade.sql` and executes it via the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`), passing `p_EintragsNr` and `p_JobKennung`.
5.  **Data Update:** `d_ausd_v_ta_vvl_upgrade.sql` (assumed) reads from source tables (unknown from provided info) and writes/updates `ta_vvl_upgrade`.
6.  **Record Count Capture:** The number of records is read from a temporary file `tmpFile`.
7.  **Error Handling/Logging:** `f_alis_msgerr.ksh` provides `DWMSG_MeldeFehler` for error reporting.

**Migrated Data Flow in BigQuery:**

1.  **Orchestrator Procedure:** A main BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`) will be the entry point, accepting `p_JobKennung` and `p_EintragsNr` as `IN` parameters.
2.  **Parameter Validation:** Internal BigQuery `IF` conditions and `ASSERT` statements will replace shell-based validation.
3.  **Error Logging:** Errors will be captured and logged into a dedicated BigQuery error log table.
4.  **Core Logic Procedure:** The main procedure will call a sub-procedure (e.g., `project.dataset.d_ausd_v_ta_vvl_upgrade`) which encapsulates the business logic originally in `d_ausd_v_ta_vvl_upgrade.sql`. This sub-procedure will perform the data transformations and updates on BigQuery tables, including `ta_vvl_upgrade`.
5.  **Job Status Management:** Logic to check for and manage "active jobs" (ignoring or deactivating) will be integrated into the BigQuery procedures, likely interacting with a job status/metadata table.
6.  **Record Count:** The sub-procedure can return the count of processed records via an `OUT` parameter, or the main procedure can query the updated `ta_vvl_upgrade` table or a staging table to derive this count.
7.  **Completion Logging:** Completion status and record counts will be logged into a BigQuery job run log table.

## 5. Transformation Logic
The primary data transformation logic resides within the `d_ausd_v_ta_vvl_upgrade.sql` script, which is invoked by the KornShell wrapper. The shell script itself primarily provides control flow and parameterization.

*   **Original Shell Script Logic:**
    *   Reads `p_JobKennung` and `p_EintragsNr`.
    *   Sets `v_TabName='ta_vvl_upgrade'`.
    *   Calls an external SQL script.
    *   Captures the record count from a temporary file.
    *   Error checks and exits.

*   **BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control`) Logic:**
    *   Accepts `p_JobKennung` STRING, `p_EintragsNr` STRING as `IN` parameters.
    *   Declares variables for `v_TabName`, `v_records`, `v_error_message`, `v_error_code`.
    *   Performs parameter validation using `IF` statements and raises an error (`RAISE USING MESSAGE`) if validation fails, logging to a `job_error_log` table.
    *   (Optional) Logs start of processing to a `job_run_log` table.
    *   Calls a dedicated BigQuery Stored Procedure `project.dataset.d_ausd_v_ta_vvl_upgrade` to execute the core SQL logic.
    *   Retrieves `v_records` (e.g., by querying a result staging table or an `OUT` parameter from the called procedure).
    *   (Optional) Logs completion to `job_run_log`.

*   **BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_vvl_upgrade`) Logic:**
    *   Accepts `p_EintragsNr` STRING, `p_JobKennung` STRING as `IN` parameters.
    *   Includes logic to check for and manage active jobs (e.g., by querying a `job_table`).
    *   If the job is active, it's skipped, and a log entry is made.
    *   If not active, existing active jobs with the same parameters are marked `INACTIVE`.
    *   The core data manipulation (INSERT/UPDATE/MERGE) will be performed here, reading from source tables (e.g., `project.dataset.source_table`) and writing to the target `project.dataset.ta_vvl_upgrade` table or an intermediate staging table.
    *   Updates the `job_table` with the completion status.

## 6. External Dependencies
The original script has several external dependencies:

*   **Oracle Database:** The script implicitly interacts with an Oracle database through the `sqlplus` utility (via `h_alis_sqlplus.ksh` and `starteSQLSkript`) and the SQL script `d_ausd_v_ta_vvl_upgrade.sql`.
    *   **Replacement:** All Oracle database interactions will be directly migrated to BigQuery SQL, utilizing BigQuery tables and syntax. The SQL script will be refactored to be BigQuery compliant.
*   **Shell Utilities/Environment:**
    *   `$HOME/.dw_init`, `BERT_DIR_ROOT`, `DW_DIR_UTL`: These provide environment variables and paths.
        *   **Replacement:** These environment-specific configurations will be managed within BigQuery using constants in stored procedures, or via configuration metadata tables if dynamic configuration is required. Paths will be replaced by BigQuery dataset.table references.
    *   `f_alis_msgerr.ksh` (error handling), `h_alis_date.ksh` (date utilities), `h_alis_parameter.ksh` (parameter parsing/validation), `h_alis_sqlplus.ksh` (SQL execution wrapper).
        *   **Replacement:** These functions will be reimplemented using BigQuery's built-in SQL functions, scripting features (e.g., `IF`, `CASE`, `ASSERT`), and stored procedures. Parameter parsing and validation will be handled by the main stored procedure's input parameters. Error logging will be directed to BigQuery tables.
*   **Temporary File:** `$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_upgrade_$$.tmp`
    *   **Replacement:** BigQuery scripting variables or temporary tables will be used to store intermediate data such as record counts.

## 7. Unresolved / Risks
*   **Original SQL Script Logic:** The content of `d_ausd_v_ta_vvl_upgrade.sql` was not provided. A significant risk lies in the complexity and Oracle-specific features (e.g., PL/SQL, specific functions, data types) within this SQL script. It is assumed that this SQL will be migrated separately into BigQuery SQL.
*   **"Active Jobs Ignored" Logic:** The exact implementation of how "active jobs are ignored" and "old active jobs are deactivated" needs careful analysis from the original SQL script and the `starteSQLSkript` function. This logic will need to be accurately replicated in BigQuery, possibly requiring a dedicated `job_status` table.
*   **Utility Script Functionality:** The specific functionality of utility scripts like `h_alis_date.ksh` (e.g., custom date formats, calculations) needs to be understood to ensure correct BigQuery SQL equivalents are used.
*   **Orchestration Beyond BigQuery:** While the core logic can be migrated to BigQuery Stored Procedures, external scheduling and monitoring (equivalent to a scheduler calling the `.ksh` script) will require a separate tool like Cloud Composer, Workflows, or Dataform.

## 8. Build Plan
1.  **Database Object Migration (Phase 1 - Data):**
    *   Migrate source tables used by `d_ausd_v_ta_vvl_upgrade.sql` to BigQuery.
    *   Migrate the target table `ta_vvl_upgrade` to BigQuery.
    *   Create `job_error_log`, `job_run_log`, and `job_table` (for managing job status) in BigQuery.
2.  **SQL Script Migration (Phase 2 - Transformation):**
    *   Analyze `d_ausd_v_ta_vvl_upgrade.sql` for Oracle-specific syntax and logic.
    *   Rewrite `d_ausd_v_ta_vvl_upgrade.sql` as a BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_v_ta_vvl_upgrade.sql`) or a series of BigQuery SQL statements. This may involve creating BigQuery UDFs if complex reusable logic is identified. (Language: BigQuery SQL)
3.  **Shell Script Control Logic Migration (Phase 3 - Orchestration):**
    *   Develop the main BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control` to encapsulate the parameter parsing, validation, job status management, and invocation of the `project.dataset.d_ausd_v_ta_vvl_upgrade` procedure. (Language: BigQuery SQL Scripting)
    *   Implement error logging into `job_error_log` and job run logging into `job_run_log`.
4.  **External Orchestration (Phase 4 - Scheduling):**
    *   If external scheduling is required, develop a Cloud Composer DAG or a Dataform workflow that invokes the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure, passing the necessary parameters. (Language: Python for Cloud Composer, SQL for Dataform)
5.  **Testing and Validation:**
    *   Unit test each BigQuery Stored Procedure.
    *   Integration test the entire BigQuery workflow.
    *   Perform data validation to ensure migrated data and transformations are accurate compared to the legacy system.