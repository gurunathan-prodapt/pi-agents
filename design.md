# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_bp_ta_bpr_beschr.ksh` to Google BigQuery. The script serves as a control mechanism within an ETL workflow. Its primary responsibilities include:
- Initializing the execution environment.
- Parsing and validating input parameters such as Job ID, Entry Number, and a Reference Date.
- Performing stringent date format checks on the provided reference date.
- Orchestrating the execution of an associated SQL script, `d_ausd_bp_ta_bpr_beschr.sql`, by passing validated parameters.
- Capturing the count of processed records.
- (Potentially) Interacting with a job tracking system to log execution status and record counts, although this functionality appears commented out in the provided source.

This script is itself invoked by `r_ausd_bp_ta_bpr_beschr.ksh`, indicating its role as a component within a larger parent process.

## 2. Source Inventory
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`
  - **Technology:** KornShell (ksh) script
  - **Complexity Tier:** Medium
  - **Automation Bucket:** Semi-automatic (migration is expected to be partially automated, requiring some manual effort for full transition)
  - **Summary:** A control script responsible for environment setup, parameter validation, date checks, and the execution of a core SQL data processing script. It relies on several helper scripts for common functions.
  - **Key Internal Dependencies (identified from code):**
    - `f_alis_msgerr.ksh`: Utility for error handling and message reporting.
    - `h_alis_date.ksh`: Provides functions for date validation.
    - `h_alis_parameter.ksh`: Used for parsing and validating script input parameters.
    - `h_alis_sqlplus.ksh`: Contains routines for SQL*Plus interactions, specifically the `starteSQLSkript` function which executes the core SQL logic.
    - `gestern.ksh`: A script used to derive yesterday's and today's dates.
    - `d_ausd_bp_ta_bpr_beschr.sql`: The primary SQL script whose execution is orchestrated by `k_ausd_bp_ta_bpr_beschr.ksh`. This script is assumed to contain the core data manipulation logic.
  - **External Dependencies (identified from code):**
    - `$HOME/.dw_init`: An environment initialization file.

## 3. Target Architecture
The migration target is Google BigQuery. The existing functionality will be refactored into BigQuery-native components:

- **Main BigQuery Stored Procedure:** The `k_ausd_bp_ta_bpr_beschr.ksh` script will be converted into a BigQuery Stored Procedure. This procedure will encapsulate the parameter handling, validation, date logic, and orchestration of the core data processing.
- **Core Data Processing BigQuery Stored Procedure/Script:** The `d_ausd_bp_ta_bpr_beschr.sql` content will be migrated into a separate BigQuery SQL script or another BigQuery Stored Procedure. This will hold the primary data transformation and loading logic.
- **Error Logging Table:** A dedicated BigQuery table (e.g., `project.dataset.error_log`) will capture execution errors and messages, replacing the legacy `DWMSG_MeldeFehler` mechanism.
- **Job Tracking/Audit Table:** A BigQuery table (e.g., `project.dataset.job_table`) will store metadata about job executions, including status, start/end times, and record counts. This replaces any legacy job table interaction.
- **Orchestration Layer:** Cloud Composer (managed Airflow) or Google Cloud Workflows will be used to schedule and manage the execution of the BigQuery Stored Procedures, replacing the current shell-based invocation.
- **Configuration Storage:** Environment variables and constants currently sourced from files (e.g., `.dw_init`) will be managed as BigQuery procedure parameters, BigQuery constants, or potentially values stored in a dedicated BigQuery configuration table.

## 4. Data Flow & Lineage
The migrated data flow will be fully integrated within the BigQuery ecosystem.

- **Trigger:** The BigQuery Stored Procedure (representing `k_ausd_bp_ta_bpr_beschr.ksh`) is invoked by an orchestrator (e.g., Cloud Composer), typically with parameters for JobKennung, EintragsNr, Stichtag, and Wiederanlaufwert.
- **Parameter Validation & Preparation:**
    - The main procedure validates input parameters.
    - Date format for `Stichtag` is verified.
    - Today's and yesterday's dates are derived using BigQuery's native date functions.
- **Core Data Processing Execution:**
    - The main procedure calls the BigQuery Stored Procedure representing `d_ausd_bp_ta_bpr_beschr.sql`, passing all necessary runtime parameters.
    - This inner procedure then performs its data reading, transformation, and writing operations within BigQuery, interacting with source and target tables. (Specific tables READ/WRITES are dependent on `d_ausd_bp_ta_bpr_beschr.sql` content.)
- **Post-Processing & Logging:**
    - Upon successful execution of the core data processing, the main procedure calculates the record count (e.g., `SELECT COUNT(*) FROM target_table`).
    - This record count, along with other job metadata, is inserted into the `project.dataset.job_table`.
    - Any errors encountered during validation or execution are logged to `project.dataset.error_log` and result in a `RAISE` exception to the orchestrator.
- **Legacy Source Data:** All data sources that feed the original `d_ausd_bp_ta_bpr_beschr.sql` must be migrated or ingested into BigQuery tables prior to this script's migration.

## 5. Transformation Logic
The migration involves re-implementing the shell script's logic using BigQuery SQL features:

- **Parameter Handling:**
    - `getopts` command-line parsing will be replaced by direct input parameters of the BigQuery Stored Procedure.
- **Parameter Validation (`pruefeParameterGesetzt`):**
    - Shell `if` conditions for checking `NULL` or empty parameters will be translated to `IF ... THEN RAISE` or `ASSERT` statements in BigQuery SQL, raising an error if a required parameter is missing.
- **Date Validation (`DWDate_Datum_Check`):**
    - The date format check will use BigQuery functions like `REGEXP_CONTAINS` to ensure the `DDMMYYYY` format and `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` to attempt parsing and validate the date's validity.
- **Date Derivation (`gestern.ksh`):**
    - The external script call will be replaced by BigQuery native date functions: `CURRENT_DATE()` for today and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` for yesterday.
- **SQL Script Execution (`starteSQLSkript`):**
    - The shell call to execute `d_ausd_bp_ta_bpr_beschr.sql` will be directly translated to a `CALL` statement to the newly created BigQuery Stored Procedure for `d_ausd_bp_ta_bpr_beschr`.
- **Record Count (`cat $tmpFile`):**
    - The file-based retrieval of record count from `$tmpFile` will be replaced by a `SELECT COUNT(*)` query executed directly on the target BigQuery table after the data processing.
- **Job Table Update (`FOSJobErzeugeEintrag` - commented):**
    - This placeholder will be implemented as an `INSERT` statement into the new `project.dataset.job_table`, providing auditability for each execution.
- **Commented-out Post-processing (`sed`, `sort`, `join`):**
    - These file manipulation steps are currently commented out in the source. If this functionality becomes required in the future for specific outputs (e.g., generating flat files for external systems), it would be implemented using BigQuery `EXPORT DATA` to Cloud Storage, possibly followed by Cloud Functions or Dataflow for any further transformation of the exported files.

## 6. External Dependencies
The original script has minimal external system dependencies beyond its reliance on the operating system and a potential Oracle database implicitly linked by `h_alis_sqlplus.ksh`. The migration simplifies this by consolidating everything within Google Cloud.

- **Legacy Database (Implicit):** The current database accessed via `SQL*Plus` (likely Oracle) will be replaced entirely by Google BigQuery. All data will be migrated to and processed within BigQuery.
- **Environment Files (`$HOME/.dw_init`):** Environment variables will be replaced by explicitly defined BigQuery Stored Procedure parameters, BigQuery constants, or values from a configuration table in BigQuery.
- **Legacy Job Management System (`FOSJob*` calls):** The commented-out references to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` suggest interaction with a specific job management framework. In BigQuery, this will be replaced by inserts into a custom `job_table` for audit and tracking. Any broader job orchestration functionality would be handled by Cloud Composer.

No other external systems (like SFTP, S3, etc.) were identified in the `lineage_assembled_jobs` analysis for this specific job.

## 7. Unresolved / Risks
- **`d_ausd_bp_ta_bpr_beschr.sql` Content:** The most significant unresolved item is the actual SQL code within `d_ausd_bp_ta_bpr_beschr.sql`. Its complexity, data sources, transformations, and target tables are critical to the migration and are currently a black box. A separate, detailed analysis and conversion effort for this SQL script is required.
- **Full Scope of `starteSQLSkript`:** The `starteSQLSkript` function (likely in `h_alis_sqlplus.ksh`) could encapsulate complex logic beyond simple SQL execution, such as connection handling, error capturing, or pre/post-execution steps. A thorough review of `h_alis_sqlplus.ksh` is needed to ensure all such logic is correctly replicated in the BigQuery migration.
- **Purpose of Commented-out Code:** The commented-out `sed`, `sort`, `join` commands suggest a potential historical requirement for flat-file processing. If this requirement resurfaces or was inadvertently disabled, it represents a risk. Clarification on its necessity is required. If necessary, it would transition to BigQuery `EXPORT DATA` combined with Cloud Functions or Dataflow.
- **Error Code Consistency:** The script uses specific error numbers (e.g., 192, 193). If these specific error codes are consumed by downstream monitoring or alerting systems, ensure they are replicated in the BigQuery error logging table and `RAISE` messages.
- **Scalability of `gestern.ksh`:** While `gestern.ksh` is a simple date calculation, in a large-scale or high-frequency environment, relying on external shell calls for basic utilities can introduce overhead. Migrating it to native BigQuery date functions is best practice.

## 8. Build Plan
The following steps outline the ordered build plan for migrating `k_ausd_bp_ta_bpr_beschr.ksh` to BigQuery:

1.  **Analyze and Migrate `d_ausd_bp_ta_bpr_beschr.sql`:**
    -   **Language:** BigQuery SQL (Stored Procedure or Script)
    -   **Description:** This is the foundational step. Extract the full content of `d_ausd_bp_ta_bpr_beschr.sql`, analyze its dependencies (tables, views, functions), and convert it to idiomatic BigQuery SQL. Define its input parameters and expected outputs.
    -   **Deliverable:** `bqsql/d_ausd_bp_ta_bpr_beschr.sql` (BigQuery Stored Procedure DDL).

2.  **Create BigQuery Error Logging Table:**
    -   **Language:** BigQuery DDL
    -   **Description:** Define and create the `project.dataset.error_log` table for centralized error reporting.
    -   **Deliverable:** `bqsql/create_error_log_table.sql`.

3.  **Create BigQuery Job Tracking Table:**
    -   **Language:** BigQuery DDL
    -   **Description:** Define and create the `project.dataset.job_table` for auditing job executions, statuses, and record counts.
    -   **Deliverable:** `bqsql/create_job_table.sql`.

4.  **Migrate `k_ausd_bp_ta_bpr_beschr.ksh` to BigQuery Stored Procedure:**
    -   **Language:** BigQuery SQL
    -   **Description:** Translate the entire control flow, parameter validation, date logic, and calls to `d_ausd_bp_ta_bpr_beschr` into a BigQuery Stored Procedure. Incorporate error logging and job table updates.
    -   **Deliverable:** `bqsql/k_ausd_bp_ta_bpr_beschr_sp.sql` (BigQuery Stored Procedure DDL based on pseudocode).

5.  **Develop Orchestration (Cloud Composer DAG):**
    -   **Language:** Python (Airflow DAG)
    -   **Description:** Create an Airflow DAG to trigger the `k_ausd_bp_ta_bpr_beschr_sp` BigQuery Stored Procedure, passing runtime parameters. Configure appropriate scheduling and error handling.
    -   **Deliverable:** `airflow/dags/k_ausd_bp_ta_bpr_beschr_dag.py`.

This structured build plan ensures a systematic migration from the legacy KornShell environment to a BigQuery-native solution.