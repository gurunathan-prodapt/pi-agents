# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_v_ta_p_vertrag.ksh` to Google BigQuery. The original script serves as a control script for processing contract data, specifically related to `ta_p_vertrag`. Its primary functions include:
- Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`).
- Managing job status by deactivating old active jobs and registering the current job in a job table.
- Executing an external SQL script (`d_ausd_v_ta_p_vertrag.sql`) which performs the core data processing.
- Capturing the number of processed records from a temporary file.
- Handling errors and providing structured output.
The job was assembled from one component and has a medium complexity stage distribution.

## 2. Source Inventory
The job consists of a single KornShell script:

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`
- **Technology:** KornShell
- **Complexity Tier:** Medium
- **Automation Bucket:** Semi-auto
- **Summary:** KornShell script for controlling the execution of a SQL script (`d_ausd_v_ta_p_vertrag.sql`) to process contract data, including parameter parsing, error handling, and job status management.

## 3. Target Architecture
The migrated solution will primarily leverage BigQuery stored procedures and tables:
- **Main Stored Procedure:** A BigQuery stored procedure (e.g., `dataset.r_ausd_vertrag_control`) will encapsulate the control logic of the original ksh script, handling parameter input, validation, job management, and orchestration of the data processing logic.
- **Job Management Table:** A BigQuery table (e.g., `dataset.job_table`) will store job status, replacing the implicit job table management in the legacy environment. This table will be updated for job activation/deactivation and registration.
- **Error Logging Table:** A BigQuery table (e.g., `dataset.job_error_log`) will capture error messages and details, replacing the shell script's console error reporting.
- **Job Run Log Table:** A BigQuery table (e.g., `dataset.job_run_log`) will log details of each job run, including records processed.
- **Configuration:** Environment variables and sourced helper scripts will be replaced by either parameters to the stored procedure, configuration tables, or deployment-time constants.
- **SQL Processing:** The logic from `d_ausd_v_ta_p_vertrag.sql` will be migrated into a BigQuery SQL script, potentially as part of the main stored procedure using `EXECUTE IMMEDIATE` or as a separate BigQuery stored procedure.

## 4. Data Flow & Lineage
The original script executes the following flow:
1.  Initializes environment and sources several utility shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  Parses command-line parameters `j` (Job ID) and `f` (Entry Number).
3.  Validates the presence of required parameters (`p_JobKennung`, `p_EintragsNr`). If validation fails, it logs an error and exits.
4.  Sets a table name variable `v_TabName` to 'ta_p_vertrag'.
5.  Calls a function `starteSQLSkript` (start SQL script), passing `p_EintragsNr`, the path to `d_ausd_v_ta_p_vertrag.sql`, and `p_JobKennung`. This function likely handles the actual execution of the SQL script via `sqlplus` or a similar tool.
6.  A temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_$$.tmp`) is used to store the count of processed records, which is then read into the `v_records` variable.

In the BigQuery target architecture:
- Parameter passing will be handled directly by stored procedure arguments.
- Job status updates will be DML operations on the `job_table`.
- The execution of `d_ausd_v_ta_p_vertrag.sql` logic will be an inline BigQuery SQL block or a call to another BigQuery stored procedure.
- The temporary file for record counting will be replaced by a BigQuery `DECLARE` variable or by querying the target table directly.

The lineage analysis did not explicitly capture direct file-to-file invocation edges for `k_ausd_v_ta_p_vertrag.ksh` and `d_ausd_v_ta_p_vertrag.sql`, likely due to the dynamic nature of shell script execution. However, the script content clearly shows this dependency.

## 5. Transformation Logic
The transformation will involve converting shell script constructs to BigQuery SQL scripting capabilities:

-   **Parameter Handling:** `getopts` will be replaced by `IN` parameters in the BigQuery stored procedure definition.
-   **Environment Variables:** Shell environment variables like `BERT_DIR_ROOT` and `DW_DIR_UTL` will be replaced by BigQuery script variables, configuration tables, or deployment-time environment variables in the orchestration layer.
-   **Sourced Scripts:** The functionality of utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will need to be re-implemented in BigQuery SQL (e.g., as helper functions, separate stored procedures, or inlined logic) or by BigQuery's native capabilities.
-   **Conditional Logic:** Shell `if` and `case` statements will be mapped to BigQuery `IF/THEN/ELSEIF` and `CASE` statements.
-   **Error Handling:** The `DWMSG_MeldeFehler` function and `exit` calls will be replaced by BigQuery's `RAISE`, `ASSERT`, and `INSERT` statements into a dedicated error logging table.
-   **Temporary Files:** The use of `tmpFile` for record counts will be replaced by BigQuery `DECLARE` variables and direct `SELECT COUNT(*)` queries on the target table.
-   **SQL Script Execution:** The `starteSQLSkript` function's role in executing `d_ausd_v_ta_p_vertrag.sql` will be replaced by `EXECUTE IMMEDIATE` within the BigQuery stored procedure, or by directly calling a migrated `d_ausd_v_ta_p_vertrag` BigQuery stored procedure.
-   **Strict Mode (`set -eu`):** BigQuery stored procedures offer transactional control and error handling mechanisms that provide similar robustness.

## 6. External Dependencies
The original script has the following external dependencies:

-   **Environment Initialization File:** `$HOME/.dw_init` (shell script).
    -   **Replacement:** Configuration in the BigQuery execution environment or via dedicated BigQuery configuration tables.
-   **Utility Scripts:**
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
    -   ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utility)
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing)
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus wrapper)
    -   **Replacement:** These functions will need to be re-implemented using BigQuery SQL scripting (functions, procedures) or utilize BigQuery's built-in date functions and error handling. The `h_alis_sqlplus.ksh` indicates interaction with an Oracle/SQL*Plus database, which will be entirely replaced by BigQuery's native SQL processing.
-   **External SQL File:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`
    -   **Replacement:** This SQL script's content will be migrated into BigQuery SQL, forming the core data processing logic within the BigQuery stored procedure or as a separate BigQuery SQL script.
-   **Oracle Database:** Implied by the use of `sqlplus` (via `h_alis_sqlplus.ksh` and `starteSQLSkript`).
    -   **Replacement:** All data processed by `d_ausd_v_ta_p_vertrag.sql` that currently resides in an Oracle database will need to be migrated to BigQuery tables.

## 7. Unresolved / Risks
-   **`d_ausd_v_ta_p_vertrag.sql` Content:** The actual SQL logic within `d_ausd_v_ta_p_vertrag.sql` is unknown. Its content needs to be thoroughly analyzed and migrated separately to BigQuery SQL, potentially involving complex SQL conversions or data type mapping.
-   **`starteSQLSkript` Wrapper:** The exact functionality of `starteSQLSkript` is not fully known. It is assumed to handle SQL execution, but if it performs additional logging, transaction control, or job-table updates beyond what's evident, these hidden functionalities must be recreated in BigQuery procedures.
-   **Legacy Job Table:** The structure and content of the "Job-Tabelle" referenced by the script are not known. A schema for `dataset.job_table` (and potentially `dataset.job_error_log`, `dataset.job_run_log`) needs to be defined based on the existing job management system.
-   **Data Volume:** The impact of data volume and performance on the migrated BigQuery SQL script for `d_ausd_v_ta_p_vertrag.sql` needs to be assessed.
-   **Idempotency:** The original script deactivates old active jobs and registers new ones. The idempotency of the overall process needs to be ensured in the BigQuery implementation.
-   **Error Codes:** The specific error codes (e.g., 193, 192) and their corresponding messages need to be translated or mapped to BigQuery's error handling mechanisms.

## 8. Build Plan
1.  **Migrate `d_ausd_v_ta_p_vertrag.sql`:**
    -   **Language:** BigQuery SQL
    -   **Description:** Analyze and convert the SQL statements within `d_ausd_v_ta_p_vertrag.sql` to BigQuery-compliant SQL. This might involve creating new BigQuery tables, views, or functions.
2.  **Create BigQuery Job Management and Logging Tables:**
    -   **Language:** BigQuery DDL
    -   **Description:** Define and create schemas for `dataset.job_table`, `dataset.job_error_log`, and `dataset.job_run_log` to manage job execution status, errors, and logs.
3.  **Develop `dataset.r_ausd_vertrag_control` Stored Procedure:**
    -   **Language:** BigQuery SQL (Scripting)
    -   **Description:**
        -   Implement parameter parsing and validation using `IN` parameters and `IF/THEN/ELSEIF` statements.
        -   Implement job deactivation and registration logic using DML on `dataset.job_table`.
        -   Integrate the migrated SQL logic from `d_ausd_v_ta_p_vertrag.sql` (either inline via `EXECUTE IMMEDIATE` or by calling a sub-procedure).
        -   Implement record counting and logging into `dataset.job_run_log`.
        -   Implement error handling and logging into `dataset.job_error_log`.
4.  **Configuration Management:**
    -   **Language:** BigQuery (or Orchestration Tool Configuration)
    -   **Description:** Define and manage any necessary configuration values (e.g., `BERT_DIR_ROOT`, `DW_DIR_UTL` equivalents) for the BigQuery environment.
5.  **Orchestration Integration:**
    -   **Language:** Airflow (or similar orchestration tool)
    -   **Description:** Create an Airflow DAG or similar orchestration to schedule and execute the `dataset.r_ausd_vertrag_control` stored procedure, passing the necessary parameters.