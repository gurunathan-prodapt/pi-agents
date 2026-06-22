# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh

## 1. Purpose & Scope

The `k_ausd_adressen.ksh` script serves as a control script for `r_ausd_adressen.ksh` (though it calls `d_ausd_adressen.sql` for its main logic). Its primary purpose is to:
- Validate input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
- Perform date validation for the `Stichtag`.
- Orchestrate the execution of an external SQL script (`d_ausd_adressen.sql`).
- Handle error reporting and exit with appropriate codes.
- Capture the number of records processed by the SQL script.
- Optionally, manage job entries in a job-tracking table (currently commented out).

The scope of this migration focuses on translating the control flow, parameter handling, date logic, and external script invocation from KornShell to a BigQuery-native solution. The logic within `d_ausd_adressen.sql` itself is considered an external dependency requiring separate migration.

## 2. Source Inventory

- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh`
  - **Technology**: KornShell Script
  - **Tier**: Medium (based on the need to translate shell scripting constructs and external dependencies)
  - **Automation Bucket**: Semi-Auto (B2) - due to the need for manual translation of shell logic, external script calls, and environment variables into BigQuery constructs.

## 3. Target Architecture

The target architecture in BigQuery will consist of the following components:
- **BigQuery Stored Procedure**: A primary stored procedure, e.g., `project.dataset.r_ausd_adressen_control`, will encapsulate the control flow, parameter validation, date logic, and the execution of the main data processing SQL (migrated from `d_ausd_adressen.sql`).
- **BigQuery Tables**:
    - **Configuration Table**: To store values for environment variables like `BERT_DIR_ROOT`, `DW_DIR_UTL`, etc., that are sourced in the original script.
    - **Error Log Table**: A table, e.g., `project.dataset.error_log`, to capture error messages and codes, replacing the shell's `DWMSG_MeldeFehler` and `echo` to stderr.
    - **Job Tracking Table**: A table, e.g., `project.dataset.job_table`, to log job execution details, replacing the intended `FOSJobErzeugeEintrag` functionality. This table would store `job_kennung`, `eintrags_nr`, `stichtag`, `status`, `record_count`, and relevant timestamps/dates.
- **Orchestration**: A Cloud Composer (Apache Airflow) DAG or BigQuery Scheduled Queries will be used to orchestrate the execution of the BigQuery stored procedure, passing the necessary parameters.

## 4. Data Flow & Lineage

The data flow for the migrated job will be:

1.  **Parameter Input**: The BigQuery stored procedure `project.dataset.r_ausd_adressen_control` receives input parameters: `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert`.
2.  **Parameter Validation**: The stored procedure performs checks for mandatory parameters (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`). If validation fails, an error is logged to `project.dataset.error_log`, and the procedure exits.
3.  **Date Validation**: The `p_Stichtag` is parsed and validated for the `DDMMYYYY` format. Invalid dates lead to an error log entry and exit.
4.  **Date Derivation**: Current date and yesterday's date are derived using BigQuery's date functions (`CURRENT_DATE()`, `DATE_SUB`).
5.  **Main SQL Logic Execution**: The core data processing logic, which was originally in `d_ausd_adressen.sql`, will be embedded directly within or called from the stored procedure as BigQuery SQL statements. This logic will operate on source tables and write to target tables within BigQuery.
6.  **Record Count Capture**: The number of records processed by the main SQL logic is captured (e.g., using `COUNT(*)` or `@@row_count`).
7.  **Job Logging**: An entry is made into the `project.dataset.job_table` with the job details, including the captured record count.

**Original Lineage (from script analysis):**
- Inputs: Command-line arguments (`j`, `f`, `s`, `l`), `$HOME/.dw_init` (environment).
- Internal Dependencies:
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date validation)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter validation)
    - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (date derivation)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL execution wrapper)
- External SQL Invocation: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_adressen.sql`
- Outputs: `tmpFile` for record count, `Job-Tabelle` (intended, commented out).

## 5. Transformation Logic

The core transformation logic will involve mapping shell script constructs to BigQuery SQL procedural language features.

**Original Shell Script Logic -> BigQuery Equivalent:**

-   **Parameter Parsing (`getopts`)**:
    -   **Legacy**: `while getopts ":h$ParamList" param do case $param in ... esac done`
    -   **Target**: BigQuery stored procedure parameters (`IN p_JobKennung STRING, IN p_EintragsNr STRING, IN p_Stichtag STRING, IN p_wiederanlaufWert INT64`).
-   **Environment Sourcing (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/...`)**:
    -   **Legacy**: Shell `.` command to source environment variables and utility functions.
    -   **Target**: Replace with BigQuery stored procedure parameters, configuration tables, or BigQuery native functions.
-   **Parameter Validation (`pruefeParameterGesetzt`)**:
    -   **Legacy**: Shell function calls like `pruefeParameterGesetzt Jobkennung p_JobKennung`.
    -   **Target**: `IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN ... END IF;` statements within the stored procedure.
-   **Error Handling (`DWMSG_MeldeFehler`, `exit`)**:
    -   **Legacy**: `DWMSG_MeldeFehler 0 E $ErrNr "$ErrArg"`, `exit $ErrNr`.
    -   **Target**: `INSERT INTO project.dataset.error_log (...) VALUES ...;` and `LEAVE;` (or `RAISE;` for hard stops) within the stored procedure.
-   **Date Validation (`DWDate_Datum_Check`)**:
    -   **Legacy**: `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'`.
    -   **Target**: `SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag); IF v_stichtag_date IS NULL THEN ... END IF;` using BigQuery's `SAFE.PARSE_DATE`.
-   **Date Derivation (`gestern.ksh`)**:
    -   **Legacy**: `set `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh``.
    -   **Target**: `DECLARE p_datum_heute DATE DEFAULT CURRENT_DATE(); DECLARE p_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`.
-   **SQL Script Execution (`starteSQLSkript`)**:
    -   **Legacy**: `starteSQLSkript $p_EintragsNr $Name_SQLskript ...`. This implies a wrapper around `sqlplus` or similar.
    -   **Target**: The migrated SQL logic from `d_ausd_adressen.sql` will be directly incorporated into the BigQuery stored procedure or called as another BigQuery stored procedure/SQL statement. Dynamic SQL via `EXECUTE IMMEDIATE` might be used if the `Name_SQLskript` were dynamic, but here it's fixed.
-   **Temporary File for Record Count (`tmpFile`, `cat $tmpFile`, `eval`)**:
    -   **Legacy**: `$DW_DIR_UTL/bert_k_ausd_adressen_$$.tmp`, `eval "v_records=`cat $tmpFile`"`.
    -   **Target**: `DECLARE v_records INT64; SET v_records = (SELECT COUNT(*) FROM project.dataset.target_table WHERE ...);` The record count will be stored in a variable within the stored procedure.
-   **Job Table Entry (`FOSJobErzeugeEintrag`)**:
    -   **Legacy**: Commented out, but `FOSJobErzeugeEintrag $v_TabName 'A' 'I' ...`.
    -   **Target**: `INSERT INTO project.dataset.job_table (...) VALUES ...;`.

## 6. External Dependencies

| Original External System/Dependency          | Proposed BigQuery Replacement / Handling                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| :------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HOME/.dw_init` (environment sourcing)       | **Configuration Table/Parameters**: Environment variables will be replaced by direct parameters to the stored procedure or values retrieved from a BigQuery configuration table.                                                                                                                                                                                                                                                                                                                                                     |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error logging) | **BigQuery Error Log Table**: `project.dataset.error_log` for structured error reporting.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities) | **BigQuery Date Functions**: Use `SAFE.PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB`, etc., for date manipulation and validation.                                                                                                                                                                                                                                                                                                                                                                                     |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (param parsing) | **BigQuery Stored Procedure Parameters**: Direct parameters for input, and `IF`/`CASE` statements for validation.                                                                                                                                                                                                                                                                                                                                                                                               |
| `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (yesterday's date) | **BigQuery Date Functions**: `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL wrapper) | **BigQuery Native SQL Execution**: The SQL logic will be directly integrated into the BigQuery stored procedure or called as separate BigQuery SQL scripts/procedures. The `starteSQLSkript` function is effectively replaced by the BigQuery execution engine itself.                                                                                                                                                                                                                                               |
| `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_adressen.sql` (main SQL script) | **BigQuery SQL Logic**: This external SQL file contains the core business logic. It needs to be migrated separately into BigQuery SQL statements, potentially forming a sub-procedure or a series of DML statements within the main control procedure. This is the most critical external dependency to migrate. The tables and columns referenced within this SQL script will need to be identified and mapped to their BigQuery equivalents. |
| Job-Tabelle (`FOSJobErzeugeEintrag`)                               | **BigQuery Job Tracking Table**: `project.dataset.job_table` will be created to store job execution metadata, if this functionality is required (it was commented out in the source script).                                                                                                                                                                                                                                                                                                                            |

## 7. Unresolved / Risks

-   **`d_ausd_adressen.sql` Migration**: The most significant unresolved item is the detailed migration of the `d_ausd_adressen.sql` script. Its contents were not provided, but it is clear it contains the main data processing logic. This will require separate analysis and migration into BigQuery SQL. The success of this overall migration hinges on the successful migration of this specific SQL.
-   **Commented-out Job Management**: The original script has commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. It is unclear if this job management functionality is still desired or needs to be activated in BigQuery. Clarification is needed. If needed, a dedicated BigQuery job control table and DML operations will be implemented.
-   **Error Handling Granularity**: While a general error log table is proposed, specific error codes (`ErrNr=193`, `ErrNr=192`) and their exact messages should be meticulously mapped to BigQuery error handling, potentially using `RAISE` or more detailed logging.
-   **Data Types and Implicit Conversions**: Ensure all data types (especially for parameters like `p_Stichtag` which is converted to `DATE`) are correctly handled during conversion to BigQuery, paying attention to potential implicit conversions in the original SQL.

## 8. Build Plan

The build plan outlines the ordered steps to implement the migration in BigQuery.

1.  **Define BigQuery Dataset**: Create a dedicated BigQuery dataset (e.g., `project.dataset`) to house all migrated objects.
2.  **Create Configuration Table**: If needed, create a BigQuery table (e.g., `config.environment_variables`) to store environment-like configurations (e.g., `BERT_DIR_ROOT` equivalent values).
3.  **Create Error Logging Table**: Implement the `project.dataset.error_log` table with columns like `error_code`, `error_argument`, `error_message`, `created_at`.
4.  **Create Job Tracking Table**: If the commented-out job management is to be implemented, create the `project.dataset.job_table` with relevant columns (`job_kennung`, `eintrags_nr`, `stichtag`, `status`, `record_count`, `created_at`, etc.).
5.  **Migrate `d_ausd_adressen.sql`**:
    -   Analyze `d_ausd_adressen.sql` to understand its tables, joins, and DML operations.
    -   Translate the SQL into BigQuery SQL, optimizing for BigQuery best practices.
    -   This might result in one or more BigQuery DML statements, views, or even a separate BigQuery stored procedure.
6.  **Develop BigQuery Stored Procedure (`project.dataset.r_ausd_adressen_control`)**:
    -   Define the stored procedure with `IN` parameters corresponding to the original script's arguments.
    -   Implement parameter validation using `IF ... THEN ... END IF;` constructs.
    -   Implement date validation using `SAFE.PARSE_DATE`.
    -   Incorporate the migrated `d_ausd_adressen.sql` logic.
    -   Implement record counting and job logging (`INSERT` into `job_table` and `error_log`).
    -   Use `DECLARE` for internal variables like `ErrNr`, `v_records`, `v_stichtag_date`, etc.
    -   **Language**: BigQuery SQL (Procedural Language).
7.  **Develop Orchestration (Cloud Composer/Scheduled Query)**:
    -   Create a Cloud Composer DAG (Python) or BigQuery Scheduled Query to invoke `project.dataset.r_ausd_adressen_control`.
    -   Pass the required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    -   **Language**: Python (for Airflow DAG) or BigQuery Scheduled Query JSON configuration.
8.  **Testing**: Unit test and integration test the BigQuery stored procedure and the orchestration.