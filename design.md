# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh

## 1. Purpose & Scope
This shell script (`k_ausd_bp_ta_rn_einzeln.ksh`) serves as an orchestration and control script for a core SQL process. Its primary business purpose is to prepare and execute a specific SQL script (`d_ausd_bp_ta_rn_einzeln.sql`) with dynamic parameters (Job identifier, Entry number, Key date, and an optional restart value). It handles parameter parsing, validates the input date format, executes the SQL logic, records the count of processed records, and logs job metadata. The script ensures that the necessary environment and utility functions are loaded before execution. The scope of this migration is to re-implement this orchestration logic and its dependencies within the Google Cloud BigQuery ecosystem.

## 2. Source Inventory
The job consists of a single KornShell script.
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh`
- **Technology:** KornShell
- **Complexity Tier:** medium
- **Automation Bucket:** semi_auto

## 3. Target Architecture
The migrated solution will primarily reside in Google BigQuery.
- **Core Logic:** A BigQuery Stored Procedure will encapsulate the orchestration logic previously handled by the KornShell script. This procedure will accept input parameters, perform validations, and manage the execution of the underlying data processing logic.
- **Data Processing:** The SQL logic previously contained in `d_ausd_bp_ta_rn_einzeln.sql` will be migrated into either inline SQL within the BigQuery Stored Procedure, a separate BigQuery Stored Procedure, or BigQuery views/tables depending on its complexity and reusability.
- **Error Logging:** A dedicated BigQuery table (`project.dataset.error_log`) will be created to store error messages and execution details, replacing the shell script's `DWMSG_MeldeFehler` function.
- **Job Auditing:** A BigQuery table (`project.dataset.job_table`) will be created to log job execution metadata, replacing the functionality of `FOSJobErzeugeEintrag`.
- **Orchestration (External):** If complex scheduling or inter-job dependencies exist, Cloud Composer (Airflow) or Cloud Workflows can be used to invoke the BigQuery Stored Procedure and manage the overall workflow. For simple scheduling, Cloud Scheduler can be used to trigger the BigQuery Stored Procedure directly.

## 4. Data Flow & Lineage
The data flow in the target BigQuery environment will be as follows:
1.  **Input Parameters:** The BigQuery Stored Procedure will receive parameters such as `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` directly.
2.  **Validation:**
    *   Parameter presence validation will be performed using BigQuery SQL conditional logic (`IFNULL`, `NULLIF`).
    *   Date format validation (`DDMMYYYY`) will be handled using BigQuery's `SAFE.PARSE_DATE` function.
3.  **Core SQL Execution:** The migrated `d_ausd_bp_ta_rn_einzeln.sql` logic will be executed. This will likely involve `INSERT`, `UPDATE`, `MERGE` statements operating on BigQuery tables, transforming source data and populating target tables.
4.  **Record Counting:** After the core data processing, a `COUNT(*)` query will determine the number of records processed or inserted, storing this value in a variable within the stored procedure.
5.  **Logging:**
    *   Errors will be `INSERT`ed into the `project.dataset.error_log` table.
    *   Job audit information, including the record count, will be `INSERT`ed into the `project.dataset.job_table`.
6.  **Output:** The stored procedure will complete, providing status messages and signaling success or failure via its execution status.

**Lineage:**
-   **Source:** External systems or upstream BigQuery tables providing data for the `d_ausd_bp_ta_rn_einzeln.sql` logic.
-   **Transformation:** The BigQuery Stored Procedure `r_ausd_bp_ta_rn_einzeln` containing the migrated logic.
-   **Target:** BigQuery tables populated by the core SQL logic, as well as the `error_log` and `job_table` for audit purposes.

## 5. Transformation Logic
The original KornShell script's logic will be transformed into BigQuery SQL and stored procedures as follows:

*   **Parameter Parsing (`getopts`):** Replaced by input parameters to the BigQuery Stored Procedure.
*   **Environment Initialization (`. $HOME/.dw_init`):** Not directly applicable in BigQuery. Environment variables will either be hardcoded, passed as parameters, or managed via BigQuery dataset/project configuration.
*   **Utility Script Sourcing:**
    *   `f_alis_msgerr.ksh`: Replaced by `INSERT` statements into the BigQuery `error_log` table.
    *   `h_alis_date.ksh` (`DWDate_Datum_Check`): Replaced by BigQuery's `SAFE.PARSE_DATE` and conditional logic.
    *   `h_alis_parameter.ksh` (`pruefeParameterGesetzt`): Replaced by `IF` statements and `IS NULL`/`='' `checks within the stored procedure.
    *   `h_alis_sqlplus.ksh`: The concept of `sqlplus` wrapper is replaced by native BigQuery SQL execution.
    *   `gestern.ksh`: Replaced by BigQuery date functions like `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Script Execution (`starteSQLSkript`):** The contents of `d_ausd_bp_ta_rn_einzeln.sql` will be refactored into BigQuery SQL statements, either directly embedded or called as other procedures/views/functions.
*   **Temporary File (`tmpFile`) for Record Count:** Replaced by a BigQuery variable within the stored procedure, assigned by a `SELECT COUNT(*)` query.
*   **Job Deactivation (`FOSJobDeaktivate`):** If this refers to a job control system, it will be handled by the chosen orchestration tool (e.g., pausing a Cloud Composer DAG) or through updates to a BigQuery job status table if the control logic is migrated to BigQuery.
*   **Job Table Entry (`FOSJobErzeugeEintrag`):** Replaced by an `INSERT` statement into the `project.dataset.job_table`.
*   **Commented-out `sed`, `sort`, `join`:** These operations, if ever activated, would need to be re-implemented using BigQuery SQL functions (e.g., `REPLACE`, `ORDER BY`, `JOIN` clauses) for data manipulation.

## 6. External Dependencies
The original script had several implicit and explicit external dependencies:

*   **External Utility Scripts (`.ksh` files):**
    *   `$HOME/.dw_init`: Replaced by BigQuery environment configuration or explicit parameters.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Replaced by `INSERT` statements into a BigQuery error log table.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Replaced by BigQuery's native date functions.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Replaced by BigQuery Stored Procedure parameter handling and conditional logic.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: Functionality absorbed into direct BigQuery SQL execution.
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Replaced by `CURRENT_DATE()` and `DATE_SUB()` in BigQuery.
*   **SQL Script (`d_ausd_bp_ta_rn_einzeln.sql`):** This is a primary dependency. Its content needs to be migrated to BigQuery SQL and will form the core data processing logic within the BigQuery Stored Procedure.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_einzeln.tmp`):** Replaced by BigQuery variables within the stored procedure.
*   **Job Control System (implied by `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`):** Replaced by BigQuery audit tables and potentially an external orchestrator like Cloud Composer if complex job control is required.
*   **Underlying Database (implied by `sqlplus` and SQL script):** This will be migrated to BigQuery tables.

## 7. Unresolved / Risks
*   **Content of `d_ausd_bp_ta_rn_einzeln.sql`:** The most significant unresolved item is the actual SQL logic within `d_ausd_bp_ta_rn_einzeln.sql`. This file needs to be analyzed and migrated separately to BigQuery SQL. The success of the overall migration heavily depends on this.
*   **Exact functionality of `starteSQLSkript`:** While assumed to be a SQL*Plus wrapper, its precise behavior (e.g., error handling, connection details, specific flags) would need confirmation for an exact BigQuery equivalent.
*   **`FOSJobDeaktivate` functionality:** The exact behavior and scope of this job deactivation (e.g., disabling specific jobs, updating a status in a metadata table) need to be understood to provide a correct BigQuery or orchestration equivalent.
*   **Data Model of `PoolBasisprodukt` and related tables:** The BigQuery migration assumes that the source tables referenced in `d_ausd_bp_ta_rn_einzeln.sql` will be available in BigQuery with a compatible schema.
*   **Potential for Oracle-specific SQL:** If `d_ausd_bp_ta_rn_einzeln.sql` contains Oracle-specific SQL syntax, it will require conversion to BigQuery Standard SQL.
*   **Commented-out `sed`, `sort`, `join`:** If these were ever part of a data pipeline or might be reactivated, their functionality would need to be migrated to BigQuery SQL as well.

## 8. Build Plan
1.  **Migrate `d_ausd_bp_ta_rn_einzeln.sql`:**
    *   Analyze `d_ausd_bp_ta_rn_einzeln.sql` for its specific logic, source tables, and target tables.
    *   Convert the SQL to BigQuery Standard SQL, optimizing for BigQuery best practices.
    *   Create necessary BigQuery tables/views for source and target data if they don't already exist.
    *   *(Language: BigQuery SQL)*

2.  **Create BigQuery Error Log Table:**
    *   Define DDL for `project.dataset.error_log` table (e.g., `error_ts`, `job_name`, `error_nr`, `error_arg`, `message`).
    *   *(Language: BigQuery DDL)*

3.  **Create BigQuery Job Audit Table:**
    *   Define DDL for `project.dataset.job_table` (e.g., `tab_name`, `status_a`, `status_i`, `stichtag_from`, `stichtag_to`, `job_type`, `restart_flag`, `record_count`, `description`, `job_kennung`, `eintrags_nr`, `created_ts`).
    *   *(Language: BigQuery DDL)*

4.  **Create BigQuery Stored Procedure (`r_ausd_bp_ta_rn_einzeln`):**
    *   Implement the parameter parsing, validation, date checks, and logging logic as outlined in the "Transformation Logic" section.
    *   Embed or call the migrated SQL from step 1 within this stored procedure.
    *   Include logic to calculate and store the record count.
    *   Include `INSERT` statements for error and job audit logging.
    *   *(Language: BigQuery SQL - Stored Procedure)*

5.  **Develop Orchestration (Optional):**
    *   If scheduled execution and external dependencies are complex, create a Cloud Composer DAG or Cloud Workflow to invoke the BigQuery Stored Procedure.
    *   *(Language: Python for Airflow DAGs or YAML for Cloud Workflows)*

6.  **Testing:** Develop and execute unit and integration tests to ensure functional equivalence and data integrity.