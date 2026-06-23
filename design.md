# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_adressen.ksh`, which is part of an assembled ETL workflow. The primary purpose of this script is to act as a control mechanism, orchestrating the execution of an SQL script (`d_ausd_adressen.sql`). It handles parameter parsing, validates input dates, loads environment variables and helper scripts, and conceptually manages job table entries (deactivating old jobs and creating new ones, though some of this functionality is currently commented out). The business intent is to perform SQL-based data preparation for address data, ignoring already active jobs, and logging execution metadata and record counts.

## 2. Source Inventory
The job is comprised of a single main component:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh`
    *   **Technology:** KornShell Script
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** This control script validates parameters, performs date checks, includes several helper shell scripts, executes an external SQL script (`d_ausd_adressen.sql`), and manages temporary files for record counts. It also contains commented-out logic for job table management.

**Key Files Involved:**
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh` (Main control script)
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql` (Core SQL logic, executed by the main script)
*   `f_alis_msgerr.ksh` (Helper script for error messaging)
*   `h_alis_date.ksh` (Helper script for date validation)
*   `h_alis_parameter.ksh` (Helper script for parameter parsing)
*   `h_alis_sqlplus.ksh` (Helper script for SQL*Plus interaction)
*   `gestern.ksh` (Helper script to derive yesterday/today dates)

## 3. Target Architecture
The migration target is Google BigQuery. The existing KornShell script and its dependencies will be re-engineered into a BigQuery-native solution, primarily leveraging BigQuery Stored Procedures and BigQuery SQL scripts.

*   **Main Control Logic:** The `k_ausd_adressen.ksh` script will be translated into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_adressen_control`). This stored procedure will handle parameter validation, date derivations, and orchestration of the core data processing.
*   **Core Data Logic:** The `d_ausd_adressen.sql` script will be translated into a separate BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_adressen`). This procedure will contain the DML/DDL statements for data reading and writing.
*   **Helper Functions:** Shell helper scripts (like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) will be replaced by:
    *   BigQuery scripting constructs (e.g., `DECLARE`, `SET`, `IF`, `RAISE`).
    *   Built-in BigQuery functions (e.g., `CURRENT_DATE()`, `DATE_SUB`, `PARSE_DATE`, `REGEXP_CONTAINS`).
    *   Potentially custom BigQuery UDFs if complex logic requires it.
*   **Temporary Files:** The use of temporary local files for record counts will be replaced by temporary tables or direct variable assignments within BigQuery procedures, or by writing to dedicated control/logging tables.
*   **Job Management:** The commented-out job management logic will be implemented as explicit DML operations against a BigQuery job control table.
*   **Orchestration:** The entire workflow will likely be orchestrated using Google Cloud Composer (Apache Airflow) or BigQuery Scheduled Queries to manage execution, dependencies, and scheduling.

## 4. Data Flow & Lineage
The original data flow involves `k_ausd_adressen.ksh` invoking `d_ausd_adressen.sql`.

**Original Flow:**
1.  `k_ausd_adressen.ksh` starts execution.
2.  Loads environment variables and helper shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  Parses input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
4.  Validates input parameters and `p_Stichtag`'s date format.
5.  Calculates `p_datum_heute` and `p_datum_gestern` using `gestern.ksh`.
6.  Executes `d_ausd_adressen.sql` via a wrapper function `starteSQLSkript`, passing numerous parameters.
7.  The `d_ausd_adressen.sql` script:
    *   **Reads from:** `DWTK_MELDUNGEN`, `CDS$TA_BP_REF`, `CDS$TA_INV_DEFINITION`, `GLV$TA_COUNTRY`, `GLV$TA_DESCRIPTION`, `SOF$TA_COUNTRY`, `BPD$TA_REACHABILITY`, `SOF$TA_BP_REF_GP`, `SOF$TA_BP_REF_RE`, `SOF$TA_BP_REF_EV`, `SOF$TA_BP_REF_DN`, `BPD$TA_BUSINESS_PARTNER`, `SOF$TA_BP_REF_GP_NODP`, `SOF$TA_BP_REF_RE_NODP`, `SOF$TA_BP_REF_EV_NODP`, `SOF$TA_BP_REF_DN_NODP`.
    *   **Writes to:** `SOF$TA_BP_REF_GP`, `SOF$TA_BP_REF_RE`, `SOF$TA_BP_REF_EV`, `SOF$TA_BP_REF_DN`, `SOF$TA_COUNTRY`, `SOF$TA_COUNTRY_DESC`, `SOF$TA_LAENDER_KNG`, `SOF$TA_REACHABILITY`, `SOF$TA_E_REACH_GP`, `SOF$TA_E_REACH_RE`, `SOF$TA_E_REACH_EV`, `SOF$TA_E_REACH_DN`, `SOF$TA_BUSINESS_PT`, `SOF$TA_BP_REF_GP_NODP`, `SOF$TA_E_BUSINESS_GP`, `SOF$TA_BP_REF_RE_NODP`, `SOF$TA_E_BUSINESS_RE`, `SOF$TA_BP_REF_EV_NODP`, `SOF$TA_E_BUSINESS_EV`, `SOF$TA_BP_REF_DN_NODP`, `SOF$TA_E_BUSINESS_DN`, `SOF$TA_E_REGULIERER`.
    *   **Uses packages:** `DWPA_UTIL_SKRIPT`, `LK`.
8.  `k_ausd_adressen.ksh` reads the record count from a temporary file created during `d_ausd_adressen.sql` execution.
9.  `k_ausd_adressen.ksh` conceptually creates a job-table entry.

**Target BigQuery Flow:**
1.  A Cloud Composer DAG or Scheduled Query triggers the `project.dataset.r_ausd_adressen_control` BigQuery Stored Procedure.
2.  `r_ausd_adressen_control` validates its input parameters.
3.  It derives `p_datum_heute` and `p_datum_gestern` using `CURRENT_DATE()` and `DATE_SUB`.
4.  It calls the `project.dataset.d_ausd_adressen` BigQuery Stored Procedure, passing the necessary parameters.
5.  `d_ausd_adressen` performs the data transformation logic, reading from and writing to the corresponding BigQuery tables (migrated from Oracle/source database).
6.  Upon `d_ausd_adressen` completion, `r_ausd_adressen_control` queries the target table(s) to obtain the processed record count.
7.  `r_ausd_adressen_control` updates a BigQuery job control table with execution metadata and the record count.

## 5. Transformation Logic
The core transformation logic resides in `d_ausd_adressen.sql`, which will be translated to BigQuery SQL. The `k_ausd_adressen.ksh` script provides orchestration and data validation.

**From KornShell to BigQuery Stored Procedure (`r_ausd_adressen_control`):**
*   **Parameter Handling:** Shell `getopts` will be replaced by `IN` parameters to the BigQuery Stored Procedure. Default values for parameters will be handled using `DEFAULT` clauses or `IF IS NULL` checks within the procedure.
*   **Environment Variables:** Sourcing `$HOME/.dw_init` and `${BERT_DIR_ROOT}/...` will be replaced by BigQuery procedure parameters, dataset/project constants, or configuration managed by the orchestration layer.
*   **Error Handling:** Shell `set -e` / `set +e` and `DWMSG_MeldeFehler` will be replaced by BigQuery scripting's `DECLARE` for error messages and `RAISE` statements for critical errors, potentially logging to a BigQuery error logging table.
*   **Date Validation:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will be translated into `REGEXP_CONTAINS` and `PARSE_DATE` BigQuery functions.
*   **External Script Execution:** The `starteSQLSkript` function executing `d_ausd_adressen.sql` will be replaced by a `CALL` statement to the translated BigQuery Stored Procedure `project.dataset.d_ausd_adressen`.
*   **Temporary File Handling:** Reading from `$DW_DIR_UTL/bert_k_ausd_adressen_$$.tmp` will be replaced by directly querying the target table(s) for the record count, or by storing the count in a control table that is then queried.
*   **Job Table Management:** The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` will be replaced by explicit `UPDATE` and `INSERT` statements against a BigQuery job control table.

**From SQL Script (`d_ausd_adressen.sql`) to BigQuery Stored Procedure (`d_ausd_adressen`):**
*   All `SELECT`, `INSERT`, `UPDATE` statements will be translated from their current SQL dialect to BigQuery SQL syntax.
*   Table names (e.g., `DWTK_MELDUNGEN`, `CDS$TA_BP_REF`, `SOF$TA_BP_REF_GP`) will be mapped to their corresponding BigQuery table names (e.g., `project.dataset.dwtk_meldungen`).
*   Database packages (`DWPA_UTIL_SKRIPT`, `LK`) will need to be analyzed for their functionality and re-implemented using BigQuery functions, UDFs, or separate procedures.
*   Specific SQL functions, data types, and syntax will be converted to their BigQuery equivalents.

## 6. External Dependencies
*   **Current State:**
    *   The script sources several local KornShell helper scripts within the same repository structure (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`).
    *   It executes an external SQL file `d_ausd_adressen.sql`.
    *   The SQL script `d_ausd_adressen.sql` reads from and writes to various tables in an unspecified relational database (implied Oracle by context, given common legacy patterns).
    *   The environment initialization file `$HOME/.dw_init` is sourced.
*   **Replacement in BigQuery:**
    *   **Helper Scripts:** Logic from helper scripts will be incorporated directly into the BigQuery Stored Procedures using BigQuery SQL scripting or UDFs, eliminating the need for separate files.
    *   **SQL File:** `d_ausd_adressen.sql` becomes a dedicated BigQuery Stored Procedure.
    *   **Database Tables:** All source and target tables (`DWTK_MELDUNGEN`, `CDS$TA_BP_REF`, `SOF$TA_BP_REF_GP`, etc.) must be migrated to BigQuery as native BigQuery tables.
    *   **Environment Initialization:** Environment variables will be replaced by direct parameters to the BigQuery Stored Procedure, BigQuery dataset/project configuration, or parameters managed by the orchestration layer (e.g., Airflow variables).
    *   No external systems like Oracle, SFTP, or S3 were explicitly identified as direct dependencies of `k_ausd_adressen.ksh` or `d_ausd_adressen.sql` from the lineage data, besides the database tables themselves.

## 7. Unresolved / Risks
*   **Unresolved:**
    *   The exact logic within the sourced helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) needs to be fully understood to ensure accurate translation to BigQuery logic.
    *   The detailed DML/DDL inside `d_ausd_adressen.sql` requires thorough analysis and translation to BigQuery SQL, including any specific SQL dialect features or complex logic.
    *   The specific implementation of `starteSQLSkript` and its interaction with `d_ausd_adressen.sql` (how parameters are passed, how `tmpFile` is populated) needs careful re-creation in BigQuery.
    *   The `AL??` comments in the original script suggest some commented-out but potentially intended job management functionality (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`). This functionality will need to be designed and implemented in BigQuery if it's required for the target state.
*   **Risks:**
    *   **Performance:** Direct translation of complex SQL from `d_ausd_adressen.sql` might not be optimal in BigQuery. Performance tuning and re-architecting of SQL queries may be required.
    *   **Data Type Mismatches:** Differences in data types and implicit conversions between the source database and BigQuery could lead to unexpected behavior.
    *   **Functional Parity:** Ensuring that all edge cases and error handling mechanisms in the original shell script and SQL are precisely replicated in BigQuery.
    *   **Orchestration Complexity:** Integrating the BigQuery Stored Procedures into a robust orchestration framework (e.g., Cloud Composer) will require careful design of task dependencies and error handling.

## 8. Build Plan
The build plan involves creating BigQuery assets and an orchestration mechanism.

1.  **BigQuery Schema Migration:**
    *   Migrate all source and target tables identified in `d_ausd_adressen.sql` (e.g., `DWTK_MELDUNGEN`, `CDS$TA_BP_REF`, `SOF$TA_BP_REF_GP`, etc.) from the legacy database to BigQuery. This includes data types, partitioning, and clustering strategies.
    *   Create a BigQuery job control table to replace the job table management functionality.
    *   Create a temporary/control table for recording the count of processed records.

2.  **BigQuery Stored Procedure for Core Logic (`d_ausd_adressen`):**
    *   **Language:** BigQuery SQL (within a Stored Procedure).
    *   **Content:** Translate the entire `d_ausd_adressen.sql` script into BigQuery SQL. This includes converting DML (INSERT, UPDATE, DELETE), DDL (if any), and SQL functions.
    *   **Parameters:** Define parameters for inputs like `p_EintragsNr`, `p_JobKennung`, `p_Stichtag`, `p_wiederanlaufWert`, `p_datum_heute`, `p_datum_gestern`.
    *   **Refactorings:** Implement BigQuery best practices for performance and scalability (e.g., using CTEs, appropriate joins, window functions).

3.  **BigQuery Stored Procedure for Control Logic (`r_ausd_adressen_control`):**
    *   **Language:** BigQuery SQL (within a Stored Procedure).
    *   **Content:** Implement the orchestration, parameter validation, date derivations, and error handling logic of `k_ausd_adressen.ksh` using BigQuery scripting.
    *   **Calls:** Include a `CALL` statement to the `d_ausd_adressen` BigQuery Stored Procedure.
    *   **Logging:** Implement logic to capture record counts and update the BigQuery job control table.

4.  **Orchestration (Cloud Composer / Apache Airflow):**
    *   **Language:** Python (for Airflow DAG).
    *   **Content:** Create an Airflow DAG that:
        *   Defines the schedule for the job.
        *   Sets any necessary Airflow variables or connections.
        *   Includes a `BigQueryOperator` or `BigQueryExecuteQueryOperator` to execute the `project.dataset.r_ausd_adressen_control` Stored Procedure, passing the required parameters.
        *   Manages task dependencies and retries.
        *   Implements monitoring and alerting.

5.  **Testing:**
    *   Unit tests for individual BigQuery Stored Procedures.
    *   Integration tests to verify the end-to-end data flow.
    *   Performance tests to ensure the migrated solution meets performance requirements.
    *   Data validation tests to ensure data integrity and functional parity with the legacy system.