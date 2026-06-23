# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_cntrct_crs2.ksh`, serves as a control script for a broader process (`r_ausd_vertrag.ksh` is mentioned in comments). Its primary function is to orchestrate the execution of a SQL script (`d_ausd_v_ta_cntrct_crs2.sql`), manage job status in a central job table (ignoring active jobs and deactivating old ones), and handle runtime parameters. It reads command-line parameters, performs validation, executes the database SQL, and records the number of processed records.

## 2. Source Inventory
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`
*   **Technology:** KornShell (ksh)
*   **Complexity Tier:** `medium`
*   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The target architecture in BigQuery will involve:
*   A **BigQuery Stored Procedure** named `project.dataset.k_ausd_v_ta_cntrct_crs2` to encapsulate the orchestration logic, parameter handling, and error management.
*   Translated **BigQuery SQL scripts** or another **BigQuery Stored Procedure** for the core data processing logic originally found in `d_ausd_v_ta_cntrct_crs2.sql`.
*   **BigQuery tables** for job control (e.g., `project.dataset.job_table`) and error logging (e.dataset.error_log), replacing the shell script's internal job management and temporary file usage.
*   **Cloud Composer** or **Cloud Workflows** for external scheduling and orchestration of the BigQuery Stored Procedure, managing parameters and dependencies.

## 4. Data Flow & Lineage
The original shell script has the following data flow and dependencies:
*   **Inputs:** Command-line parameters `j` (for `p_JobKennung`) and `f` (for `p_EintragsNr`).
*   **Environment Initialization:** Sources `$HOME/.dw_init` for environment setup.
*   **Utility Scripts:**
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (for error messaging).
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (for date-related functions).
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (for parameter validation).
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (for SQL execution).
*   **Configuration:** Sets an internal variable `v_TabName` to `'ta_cntrct_crs2'`.
*   **SQL Execution:** Invokes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_crs2.sql` via the `starteSQLSkript` function, passing `p_EintragsNr` and `p_JobKennung`.
*   **Record Count:** The `starteSQLSkript` is expected to write a record count to a temporary file: `$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs2_$$.tmp`. The script then reads this file into the `v_records` variable.
*   **Outputs:** Error messages to console/log, success messages to console, and the final record count `v_records`.

In BigQuery, this flow will be:
*   **Parameters:** `p_JobKennung` and `p_EintragsNr` passed as arguments to the BigQuery Stored Procedure.
*   **Configuration:** `v_TabName` will be a `DECLARE` variable within the Stored Procedure.
*   **Job Control:** Inserts into `project.dataset.job_table` for status tracking.
*   **SQL Execution:** Direct execution of the translated BigQuery SQL from `d_ausd_v_ta_cntrct_crs2.sql`.
*   **Record Count:** Achieved by `SELECT COUNT(*) INTO v_records` or similar BigQuery scripting constructs after data processing.
*   **Logging:** Inserts into `project.dataset.error_log` for error events and console output replaced by logging within Cloud Logging if orchestrated via Cloud Composer.

## 5. Transformation Logic
The transformation logic within `k_ausd_v_ta_cntrct_crs2.ksh` is primarily control flow and orchestration:
1.  **Environment Setup:** Sourcing various `.ksh` files for common functions. This will be replaced by direct BigQuery scripting logic or by separate BigQuery functions/procedures.
2.  **Parameter Parsing:** Uses `getopts` to read `-j` and `-f`. In BigQuery, these will be direct input parameters to the Stored Procedure.
3.  **Parameter Validation:** `pruefeParameterGesetzt` function is called. This will be translated to `IF` conditions and `RAISE` statements in BigQuery SQL scripting.
4.  **Job Table Interaction:** Logic to "ignore active jobs" and "deactivate old active jobs" will be translated into `INSERT` and `UPDATE` statements on the `project.dataset.job_table`.
5.  **SQL Script Invocation:** `starteSQLSkript` function executes the `d_ausd_v_ta_cntrct_crs2.sql` script. This will be replaced by direct execution of the translated BigQuery SQL or calling another BigQuery Stored Procedure.
6.  **Record Count:** Reads a temporary file for record count. This will be replaced by `SELECT COUNT(*)` queries into BigQuery script variables.

The actual data transformation logic is assumed to reside within the `d_ausd_v_ta_cntrct_crs2.sql` file, which requires separate analysis and migration to BigQuery SQL.

## 6. External Dependencies
*   **Shell Utilities:** The script relies on several sourced KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These functionalities must be re-implemented in BigQuery SQL scripting or Python wrappers as needed.
*   **SQL Script:** Invokes an external SQL script (`d_ausd_v_ta_cntrct_crs2.sql`). The content of this script is critical and will form the core data processing logic in BigQuery.
*   **Job Table:** Implicit dependency on a database job table for status management. This will be migrated to a dedicated BigQuery table.
*   **Temporary File System:** Uses a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs2_$$.tmp`) for inter-process communication (record count). This will be replaced by BigQuery scripting variables or logging tables.
*   No other external systems (like Oracle, SFTP, S3) were explicitly identified in the lineage analysis for this specific job.

## 7. Unresolved / Risks
*   **`d_ausd_v_ta_cntrct_crs2.sql` Content:** The most significant unknown is the specific SQL code within `d_ausd_v_ta_cntrct_crs2.sql`. Its complexity, use of proprietary SQL features, or procedural logic will dictate the effort required for translation to BigQuery SQL. This needs a separate analysis.
*   **Sourced Shell Scripts:** The exact functionality and dependencies of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` need to be fully understood to ensure their functionalities are correctly re-implemented or replaced in the BigQuery environment.
*   **Job Table Schema:** The schema and specific update/insert patterns for the legacy "job table" need to be mapped to the new `project.dataset.job_table` in BigQuery.
*   **Error Handling Framework:** The `DWMSG_MeldeFehler` call points to a custom error handling framework that needs a BigQuery-native equivalent (e.g., using `RAISE` and an error logging table).
*   **`semi_auto` Migration Bucket:** This indicates that the migration will require manual intervention, especially for translating the core SQL logic and adapting the shell-specific control flow.

## 8. Build Plan
1.  **Define BigQuery Tables:**
    *   Create `project.dataset.job_table` (DDL) to track job executions, statuses, and record counts.
    *   Create `project.dataset.error_log` (DDL) for centralized error logging.
2.  **Analyze `d_ausd_v_ta_cntrct_crs2.sql`:**
    *   Obtain the source code for `d_ausd_v_ta_cntrct_crs2.sql`.
    *   Analyze its SQL for BigQuery compatibility.
    *   Translate the SQL into a BigQuery-compatible script or a separate BigQuery Stored Procedure (e.g., `project.dataset.p_ausd_v_ta_cntrct_crs2_data_logic`).
3.  **Develop BigQuery Stored Procedure (`k_ausd_v_ta_cntrct_crs2`):**
    *   Implement parameter handling for `p_JobKennung` and `p_EintragsNr`.
    *   Translate parameter validation logic using `IF` statements and `RAISE` for errors.
    *   Integrate job status updates into `project.dataset.job_table`.
    *   Replace `starteSQLSkript` call with the execution of the translated BigQuery SQL from step 2 (either inline or as a separate `CALL` to a stored procedure).
    *   Implement record counting using `SELECT COUNT(*)` into a declared variable.
    *   Replace temporary file operations with BigQuery script variables or inserts into logging tables.
    *   Implement BigQuery-native error logging using `INSERT INTO project.dataset.error_log`.
    *   Replicate essential functionalities of sourced KSH utility scripts (e.g., date formatting, additional parameter handling) directly within this Stored Procedure or as separate BigQuery UDFs/procedures.
4.  **Orchestration (Optional but Recommended):**
    *   If complex scheduling or external triggers are required, create a Cloud Composer DAG or Cloud Workflows definition to invoke the `project.dataset.k_ausd_v_ta_cntrct_crs2` BigQuery Stored Procedure, passing the necessary parameters.

**Language:** All BigQuery components will be implemented in **BigQuery SQL** (including scripting language for procedures). Orchestration will be in **Python** (for Cloud Composer) or **YAML** (for Cloud Workflows).