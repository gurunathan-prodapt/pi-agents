# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh

## 1. Purpose & Scope

This job, `k_ausd_v_ta_inv_acc.ksh`, serves as a control script for a data processing workflow. Its primary responsibilities include ignoring active jobs, invoking a core SQL script (`d_ausd_v_ta_inv_acc.sql`) for data manipulation, registering the job in a job table, and deactivating old active jobs. The script operates within the context of the `ta_inv_acc` table.

The scope of this migration is to re-platform this KornShell script and its invoked SQL logic to Google Cloud's BigQuery environment. The shell script's orchestration and parameter handling logic will be translated into a BigQuery Stored Procedure, while the data transformation logic within the SQL script will be adapted to BigQuery Standard SQL.

## 2. Source Inventory

The job consists of a single primary KornShell script that orchestrates the execution of an SQL script.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh`
    *   **Technology**: KornShell
    *   **Tool**: KornShell
    *   **Category**: shell
    *   **Purpose**: Orchestration, control flow, parameter validation.
    *   **Complexity Tier**: Not available in `file_complexity` table. Based on content, it's a moderately complex shell script due to parameter parsing, error handling, and sourcing of helper scripts.
    *   **Migration Bucket**: Not available in `automation_rate` table. Likely `semi_auto` (B2) given the need to translate shell logic to procedural SQL and re-evaluate dependencies.

*   **Invoked File**: `d_ausd_v_ta_inv_acc.sql` (implicitly invoked by the ksh script)
    *   **Technology**: SQL (likely Oracle PL/SQL given `sqlplus` wrapper in ksh)
    *   **Purpose**: Core data transformation, reads from source tables, writes to target tables.

## 3. Target Architecture

The migration will target a BigQuery-centric architecture.

*   **Primary Component**: A BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_vertrag_control`, will encapsulate the logic of the original KornShell script. This procedure will handle parameter validation, job management (updating/inserting into a job control table), and invoke the migrated SQL logic.
*   **Data Processing**: The business logic originally in `d_ausd_v_ta_inv_acc.sql` will be migrated into either:
    *   An inline DML statement within the `r_ausd_vertrag_control` stored procedure.
    *   A separate BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_v_ta_inv_acc`) called by the control procedure.
*   **Job Control**: A dedicated BigQuery table (e.g., `project.dataset.job_table`) will manage job status, active flags, entry numbers, and record counts, replacing the shell script's implicit job management and temporary file usage.
*   **Error Logging**: A BigQuery error logging table (e.g., `project.dataset.error_log`) will capture errors, replacing the `DWMSG_MeldeFehler` calls.
*   **Data Storage**: All source and target tables (e.g., `DWTK_MELDUNGEN`, `SOF$TA_INV_ASSIGN`, `SOF$TA_INV_ACC`, `VIA`) will reside in BigQuery.
*   **Orchestration (Optional)**: If external scheduling or more complex workflow management is required, Google Cloud Composer (Apache Airflow) or Workflows could be used to trigger the BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The data flow primarily involves the KornShell script orchestrating the execution of an SQL script which then interacts with database tables.

1.  **`k_ausd_v_ta_inv_acc.ksh` (KornShell Script)**:
    *   **Inputs**: Command-line parameters (`-j` for `p_JobKennung`, `-f` for `p_EintragsNr`).
    *   **Internal Actions**:
        *   Sources several utility KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
        *   Parses input parameters using `getopts`.
        *   Validates parameters.
        *   Sets `v_TabName` to `'ta_inv_acc'`.
        *   Defines `Name_SQLskript` as `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_inv_acc.sql`.
        *   Defines `tmpFile` as `$DW_DIR_UTL/bert_k_ausd_v_ta_inv_acc_$$.tmp`.
        *   Invokes an SQL script via `starteSQLSkript` function, passing `$p_EintragsNr`, `$Name_SQLskript`, `$p_EintragsNr`, `$p_JobKennung`.
    *   **Outputs**:
        *   Prints status messages to console.
        *   Writes record count to a temporary file, which is then read into `v_records`.
        *   Exits with an error code if parameter validation fails.

2.  **`d_ausd_v_ta_inv_acc.sql` (SQL Script - executed by ksh)**:
    *   **Reads From**:
        *   `TABLE:DWTK_MELDUNGEN` (via `FROM isbert_schema.dwtk_meldungen`)
        *   `TABLE:SOF$TA_INV_ASSIGN` (via `FROM sof$ta_inv_assign`)
    *   **Writes To**:
        *   `TABLE:SOF$TA_INV_ACC` (via `INTO sof$ta_inv_acc`)
        *   `TABLE:VIA` (via `merge via`)
    *   **Uses**:
        *   `PACKAGE:DWPA_UTIL_SKRIPT` (via `DWPA_UTIL_SKRIPT.runstatement`)

**Execution Order**: The KornShell script (`k_ausd_v_ta_inv_acc.ksh`) initiates the process. After parameter validation and setup, it executes the SQL script (`d_ausd_v_ta_inv_acc.sql`). The SQL script performs the actual data reads and writes. Finally, the ksh script captures the record count from the SQL execution.

## 5. Transformation Logic

The KornShell script itself contains minimal data transformation logic; its primary role is orchestration and parameter handling. The core data transformations are expected to be within the invoked SQL script, `d_ausd_v_ta_inv_acc.sql`.

**KornShell Logic to BigQuery Stored Procedure (Pseudocode mapping from MCP):**

*   **Parameter Handling (`getopts`)**: Replaced by direct `IN` parameters in the BigQuery Stored Procedure: `p_JobKennung STRING`, `p_EintragsNr STRING`.
*   **Parameter Validation**: `if [ ! $ErrNr -eq 0 ]` and `pruefeParameterGesetzt` calls will be translated to `IF` conditions within the Stored Procedure, checking for `NULL` or empty parameter values.
*   **Error Reporting (`DWMSG_MeldeFehler`)**: Will be mapped to `INSERT` statements into a BigQuery error logging table and potentially `SIGNAL SQLSTATE` for controlled procedure termination.
*   **Job Status Management**: The implicit job activation/deactivation and entry in a "job table" will be handled by `UPDATE` and `INSERT` statements against a dedicated `job_table` in BigQuery.
*   **Temporary File (`tmpFile`) for Record Count**: Replaced by using `DECLARE` and `SET` statements to capture `COUNT(*)` from the target table after the data load, or using an `OUT` parameter from a sub-procedure.
*   **SQL Script Execution (`starteSQLSkript`)**: The invocation of `d_ausd_v_ta_inv_acc.sql` will be replaced by either inlining the SQL script's logic directly into the main stored procedure or calling a separate BigQuery Stored Procedure that contains the migrated SQL logic.

**SQL Script Logic (`d_ausd_v_ta_inv_acc.sql`)**:
This script needs to be fully analyzed for its specific DML (e.g., `INSERT`, `UPDATE`, `MERGE`) and any procedural constructs.
*   **`READS_TABLE`**: Data will be read from BigQuery equivalents of `DWTK_MELDUNGEN` and `SOF$TA_INV_ASSIGN`.
*   **`WRITES_TABLE`**: Data will be written/merged into BigQuery equivalents of `SOF$TA_INV_ACC` and `VIA`.
*   **`USES_PACKAGE:DWPA_UTIL_SKRIPT`**: The functionality of this Oracle package needs to be identified. If it contains generic utilities, these might be reimplemented as BigQuery UDFs or functions. If it's specific to the process, its logic will be absorbed into the main BigQuery stored procedure.

## 6. External Dependencies

The `lineage_assembled_jobs` record indicates no explicit external systems are connected to this job. However, based on the script content and inferred Oracle environment:

*   **Oracle Database**: The sourcing of `h_alis_sqlplus.ksh` in the original script suggests interaction with an Oracle database via SQL*Plus. In BigQuery, this will be replaced by native BigQuery DML/DDL operations within the stored procedures. No direct "connection" to Oracle will be maintained.
*   **KornShell Utility Scripts**: The sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will have their relevant functionalities reimplemented directly in BigQuery SQL procedural logic or in helper functions/UDFs within BigQuery if necessary.
*   **Temporary Files**: The use of `$DW_DIR_UTL/bert_k_ausd_v_ta_inv_acc_$$.tmp` for inter-process communication will be replaced by BigQuery's native variable handling or temporary table mechanisms.

## 7. Unresolved / Risks

*   **Missing Complexity/Automation Rate Data**: The `file_complexity` and `automation_rate` tables did not return any rows for `k_ausd_v_ta_inv_acc.ksh`. This means a formal assessment of migration effort and automation potential is currently missing. A manual assessment or deeper analysis would be required.
*   **Detailed SQL Script Analysis**: While the `lineage_edges` provides table interactions for `d_ausd_v_ta_inv_acc.sql`, the exact transformation logic within this SQL script is not detailed. This is the most critical part for successful data migration and needs thorough review and conversion to BigQuery Standard SQL, potentially including rewriting Oracle-specific syntax or functions.
*   **`DWPA_UTIL_SKRIPT` Package**: The specific functionalities of the `DWPA_UTIL_SKRIPT` package (used by `d_ausd_v_ta_inv_acc.sql`) are unknown. Its functionality needs to be reverse-engineered and translated into BigQuery equivalents or custom UDFs/functions.
*   **"Ignoring Active Jobs" / "Deactivating Old Active Jobs" Logic**: The exact implementation of this logic, especially how it identifies and manages "active jobs," needs to be thoroughly understood to ensure accurate replication in the BigQuery `job_table` and associated logic.
*   **Environmental Variables**: The shell script relies heavily on environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`). These will need to be mapped to BigQuery procedure parameters, session variables, or configuration tables.
*   **Orchestration Context**: The ksh script implies it's "called by a wrapper script." The larger orchestration context needs to be understood to determine if a simple BigQuery stored procedure call is sufficient or if a Cloud Composer/Workflows DAG is required to integrate into a broader data pipeline.

## 8. Build Plan

1.  **Define BigQuery Schema**:
    *   Create `project.dataset.job_table` for job control (columns: `job_kennung`, `eintrags_nr`, `tab_name`, `active_flag`, `created_ts`, `completed_ts`, `record_count`, `error_code`, `error_message`).
    *   Create `project.dataset.error_log` for detailed error messages (columns: `error_ts`, `error_nr`, `error_arg`, `procedure_name`, `message`).
    *   Ensure all referenced source and target tables (`DWTK_MELDUNGEN`, `SOF$TA_INV_ASSIGN`, `SOF$TA_INV_ACC`, `VIA`) are created or already exist in BigQuery with appropriate schemas.

2.  **Migrate `d_ausd_v_ta_inv_acc.sql`**:
    *   Analyze the SQL script's content, identifying all DML operations, functions, and any Oracle-specific constructs.
    *   Rewrite the SQL script into BigQuery Standard SQL.
    *   Encapsulate this migrated SQL logic within a BigQuery Stored Procedure, e.g., `project.dataset.d_ausd_v_ta_inv_acc(p_EintragsNr, p_JobKennung)`.

3.  **Create BigQuery Control Stored Procedure**:
    *   Develop the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure.
    *   Implement parameter validation logic using `IF` statements.
    *   Integrate `UPDATE` and `INSERT` statements to manage `project.dataset.job_table` for job status tracking.
    *   Replace `DWMSG_MeldeFehler` calls with `INSERT` statements into `project.dataset.error_log` and `SIGNAL SQLSTATE` for error handling.
    *   Call the migrated SQL stored procedure `project.dataset.d_ausd_v_ta_inv_acc` from within the control procedure.
    *   Implement logic to capture the record count from the target table (`SOF$TA_INV_ACC`) and update `job_table`.

4.  **Migrate Utility Script Functionality**:
    *   Review the content of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` and `DWPA_UTIL_SKRIPT` (if possible).
    *   Implement any necessary common functions as BigQuery UDFs or incorporate their logic directly into the stored procedures.

5.  **Develop Orchestration (if needed)**:
    *   If the job requires external scheduling or dependency management, create a Cloud Composer DAG or Workflows definition to trigger the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure.

6.  **Testing**:
    *   Unit test the migrated SQL logic (`d_ausd_v_ta_inv_acc` SP).
    *   Unit test the control flow and parameter handling of `r_ausd_vertrag_control` SP.
    *   Integration test the entire BigQuery solution, verifying data accuracy and job status updates.