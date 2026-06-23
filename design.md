# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_cntrct_dist.ksh`. This script serves as a control and orchestration component within the legacy ETL framework. Its primary purpose is to parse command-line parameters, validate input (job identifier, entry number, and a key date in `DDMMYYYY` format), set up the execution environment by sourcing various utility scripts, derive 'today' and 'yesterday' dates, and then execute a core SQL script (`d_ausd_bp_ta_cntrct_dist.sql`) with the collected parameters. Post-SQL execution, it reads a temporary file to capture record counts, which can then be used for job tracking (though the corresponding job table update logic is commented out in the provided source). The overall job is categorized as a "medium" complexity, "semi-auto" migration candidate.

The scope of this migration is to convert the existing KornShell orchestration logic and its invoked SQL script to a BigQuery-native solution, primarily utilizing BigQuery Stored Procedures for the orchestration and BigQuery SQL for data transformations, along with an appropriate BigQuery-native scheduling mechanism.

## 2. Source Inventory
The job `5af228f1` consists of one primary component file:
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh`
    *   **Category**: shell
    *   **Tool**: KornShell
    *   **Summary**: "This ksh script acts as a control script, parsing parameters, setting up the environment, performing date and parameter validation, and orchestrating the execution of a core SQL script for data processing."
    *   **Complexity Tier**: medium
    *   **Migration Bucket**: semi_auto
    *   **Migration Flags**: None

The main business logic resides in the SQL script invoked by this KornShell script, `d_ausd_bp_ta_cntrct_dist.sql`.

## 3. Target Architecture
The migrated solution will primarily leverage Google Cloud Platform (GCP) services, focusing on BigQuery for data processing and a suitable orchestration service.

*   **Orchestration**: Cloud Composer (Apache Airflow) or Cloud Workflows will manage the execution of the BigQuery stored procedure, handling parameter passing, scheduling, and error handling.
*   **Data Processing**: BigQuery Stored Procedures will encapsulate the shell script's orchestration logic (parameter parsing, validation, date derivation) and trigger the core data transformation logic.
*   **Data Transformation**: The logic from `d_ausd_bp_ta_cntrct_dist.sql` will be directly translated into BigQuery SQL statements within the BigQuery Stored Procedure or as separate BigQuery SQL scripts executed by the stored procedure.
*   **Logging & Auditing**: BigQuery tables will replace temporary files and job tables for recording record counts, job status, and audit information. Cloud Logging will capture execution logs.
*   **Parameter Management**: Parameters will be passed as arguments to the BigQuery Stored Procedure, and environment-specific configurations will be managed via Composer/Workflows environment variables or GCP Secret Manager.

## 4. Data Flow & Lineage
The original data flow is as follows:
1.  An external script (`r_ausd_bp_ta_cntrct_dist.ksh`) invokes `k_ausd_bp_ta_cntrct_dist.ksh`.
2.  `k_ausd_bp_ta_cntrct_dist.ksh` initializes environment, validates parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`), derives `p_datum_heute` and `p_datum_gestern`.
3.  `k_ausd_bp_ta_cntrct_dist.ksh` then executes `d_ausd_bp_ta_cntrct_dist.sql`, passing it the validated parameters.
4.  `d_ausd_bp_ta_cntrct_dist.sql` performs the following actions:
    *   `READS_TABLE`: `TABLE:DWTK_MELDUNGEN`
    *   `READS_TABLE`: `TABLE:SOF$TA_BPR_BASIS`
    *   `WRITES_TABLE`: `TABLE:SOF$TA_CNTRCT_DIST`
    *   `USES_PACKAGE`: `PACKAGE:DWPA_UTIL_SKRIPT`
5.  After the SQL script, `k_ausd_bp_ta_cntrct_dist.ksh` reads a record count from `$DW_DIR_UTL/bert_k_ausd_bp_ta_cntrct_dist.tmp`.

**Target Data Flow:**
1.  **Orchestrator (Cloud Composer/Workflows)**: Schedules and triggers a BigQuery Stored Procedure.
2.  **BigQuery Stored Procedure (`project.dataset.sp_k_ausd_bp_ta_cntrct_dist`)**:
    *   Receives `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` as input parameters.
    *   Performs parameter validation and date format checks using BigQuery scripting `IF/RAISE` statements.
    *   Derives `v_datum_heute` and `v_datum_gestern` using BigQuery date functions.
    *   Executes the migrated BigQuery SQL statements corresponding to `d_ausd_bp_ta_cntrct_dist.sql`.
    *   `READS`: `project.dataset.DWTK_MELDUNGEN` (or equivalent BQ table)
    *   `READS`: `project.dataset.SOF_TA_BPR_BASIS` (or equivalent BQ table)
    *   `WRITES`: `project.dataset.SOF_TA_CNTRCT_DIST` (or equivalent BQ table)
    *   Captures record counts directly within the SQL logic or a `SELECT COUNT(*)` statement into a BigQuery variable.
    *   Logs execution status and record counts to an audit table (`project.dataset.job_audit_table`).

## 5. Transformation Logic
The transformation logic is primarily contained within the `d_ausd_bp_ta_cntrct_dist.sql` script, which is executed by the shell script. The shell script itself mainly handles control flow and parameterization.

**Shell Script Logic Migration (to BigQuery Stored Procedure):**
*   **Parameter Parsing**: `getopts` logic will be replaced by direct input parameters to the BigQuery Stored Procedure.
*   **Environment Sourcing**: Sourcing `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` will be replaced by BigQuery scripting variables, UDFs, or direct BigQuery functions (e.g., for date operations). Error handling (`f_alis_msgerr.ksh`) will be mapped to `RAISE` statements or logging to an audit table.
*   **Parameter Validation**: `pruefeParameterGesetzt` calls will be translated into `IF IS NULL OR TRIM(...) = '' THEN RAISE USING MESSAGE = '...' END IF;` statements.
*   **Date Validation**: `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will use `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL` for validation.
*   **Date Derivation**: The `gestern.ksh` call for `p_datum_heute` and `p_datum_gestern` will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` functions.
*   **SQL Script Execution**: The `starteSQLSkript` call will be replaced by the direct execution of the migrated BigQuery SQL from `d_ausd_bp_ta_cntrct_dist.sql` within the stored procedure.
*   **Record Count**: Reading `tmpFile` will be replaced by capturing the `COUNT(*)` of the target table or the number of `INSERTED` rows into a BigQuery variable.
*   **Job Table Update**: The commented-out `FOSJobErzeugeEintrag` will be implemented as an `INSERT` into a BigQuery audit table.
*   **Commented File Post-processing**: The `sed`, `sort`, `join` operations in the commented section suggest potential downstream file transformations. If these are reactivated, they should be implemented using BigQuery's data manipulation capabilities (e.g., `REGEXP_REPLACE`, `SELECT DISTINCT`, `ORDER BY`, `JOIN` on temporary tables or CTEs).

**SQL Script Logic Migration (to BigQuery SQL):**
The SQL script `d_ausd_bp_ta_cntrct_dist.sql` will be directly converted to BigQuery SQL, ensuring all Oracle-specific syntax (if any) is translated to its BigQuery equivalent.
*   `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_BPR_BASIS` will be mapped to corresponding BigQuery tables.
*   `TABLE:SOF$TA_CNTRCT_DIST` will be the target BigQuery table for `WRITES`.
*   `PACKAGE:DWPA_UTIL_SKRIPT` suggests a PL/SQL package with utility functions. These functions will need to be re-implemented as BigQuery UDFs (User-Defined Functions) or integrated directly into the stored procedure logic where feasible.

## 6. External Dependencies
*   **External Systems**: The initial query for `lineage_assembled_jobs` showed no explicit `external_systems` for this job.
*   **Legacy Environment Variables and Paths**: `$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL` are environment-specific. In BigQuery, these will be replaced by:
    *   BigQuery dataset/table paths (`project.dataset.table`).
    *   Orchestration-level variables (e.g., Airflow Variables/XComs) for dynamic path components.
    *   BigQuery Scripting variables for internal use.
*   **KornShell Utility Scripts (`.ksh`)**: All sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) will be migrated into BigQuery Stored Procedure logic (parameters, validation, date functions) or BigQuery UDFs.
*   **Oracle `sqlplus` (implied by `h_alis_sqlplus.ksh`)**: The SQL execution will directly use BigQuery's execution engine.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_bp_ta_cntrct_dist.tmp`)**: This will be replaced by BigQuery variables (`DECLARE v_records INT64;`) or by writing to a BigQuery audit table.
*   **`PACKAGE:DWPA_UTIL_SKRIPT`**: This Oracle package will need to be re-implemented as BigQuery UDFs or integrated into the main BigQuery Stored Procedure.

## 7. Unresolved / Risks
*   **Dynamic SQL/Complex Scripting**: While the current KSH script is mainly orchestration, if the invoked `d_ausd_bp_ta_cntrct_dist.sql` contains highly dynamic SQL or complex procedural constructs not directly supported by BigQuery SQL, it might require a partial re-design using Python/PySpark for the complex parts, orchestrated by Cloud Composer.
*   **Exact Behavior of Sourced Scripts**: The exact functionality of the sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) would need to be fully understood to ensure accurate migration to BigQuery-native constructs or UDFs. Assuming standard utility functions.
*   **Commented Code**: The commented-out sections (job deactivation, file post-processing for `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) are currently not part of the active job flow. If these become active or relevant, they represent additional scope and complexity, likely requiring new BigQuery tables and ETL logic (e.g., for `sed`, `sort`, `join` operations using BigQuery SQL).
*   **Oracle Package `DWPA_UTIL_SKRIPT`**: This package could contain critical business logic. A detailed analysis of this package is required to determine the best migration path (BQ UDFs, BQ Stored Procedure logic, or external Python functions).

## 8. Build Plan
The migration will proceed in the following steps, focusing on generating BigQuery Stored Procedures and BigQuery SQL.

1.  **BigQuery DDL for Target Tables**:
    *   Create `SOF_TA_CNTRCT_DIST` table in BigQuery.
    *   Ensure `DWTK_MELDUNGEN` and `SOF_TA_BPR_BASIS` tables (or their BigQuery equivalents) exist.
    *   Create `job_audit_table` for logging.
    *(Language: BigQuery DDL)*

2.  **Migrate `d_ausd_bp_ta_cntrct_dist.sql` to BigQuery SQL**:
    *   Translate the SQL script's logic into a BigQuery-compatible SQL script or integrate it directly into the stored procedure.
    *(Language: BigQuery SQL)*

3.  **Create BigQuery Stored Procedure for Orchestration**:
    *   Develop a BigQuery Stored Procedure named `sp_k_ausd_bp_ta_cntrct_dist` (or similar).
    *   Implement parameter handling, validation, and date derivation as described in Section 5.
    *   Embed or call the migrated BigQuery SQL from step 2.
    *   Include logic for capturing record counts and writing to the `job_audit_table`.
    *(Language: BigQuery SQL Scripting)*

4.  **Migrate Utility Scripts/Functions (as needed)**:
    *   Analyze `DWPA_UTIL_SKRIPT` and other sourced `.ksh` utilities.
    *   Create BigQuery UDFs or additional BigQuery Stored Procedures for reusable logic.
    *(Language: BigQuery SQL UDF/Scripting)*

5.  **Develop Orchestration DAG (Cloud Composer)**:
    *   Create an Apache Airflow DAG to schedule and invoke the `sp_k_ausd_bp_ta_cntrct_dist` BigQuery Stored Procedure.
    *   Configure task dependencies, retry mechanisms, and parameter passing.
    *(Language: Python for Airflow DAG)*

6.  **Implement Audit/Logging**:
    *   Finalize the schema for the `job_audit_table` to capture job execution details, parameters, and record counts.
    *(Language: BigQuery DDL, BigQuery SQL for Inserts)*

7.  **Review and Test**:
    *   Conduct thorough testing to ensure functional equivalence, performance, and data integrity.