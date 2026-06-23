# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh

## 1. Purpose & Scope
This migration design document addresses the `k_ausd_v_ta_cntrct_crs2.ksh` KornShell script. The script serves as a control and orchestration component within the legacy ETL environment. Its primary responsibilities include parsing input parameters (`JobKennung`, `EintragsNr`), sourcing various utility functions (for environment setup, error handling, date operations, parameter parsing, and SQL*Plus routines), validating parameters, and ultimately executing a core SQL script named `d_ausd_v_ta_cntrct_crs2.sql`. It also handles job registration, deactivation of older active jobs, error reporting, and capturing the count of processed records from the SQL script's output. The overall business purpose of this job is to prepare and manage the execution of data processing for the `ta_cntrct_crs2` table.

## 2. Source Inventory
The job is composed of a single primary source file:
*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`
*   **Technology**: KornShell Script
*   **Summary**: This KornShell script is an orchestration script that sets up the environment, validates parameters, and executes a dependent SQL script (`d_ausd_v_ta_cntrct_crs2.sql`) for processing data related to `ta_cntrct_crs2`. It manages job lifecycle aspects such as activation/deactivation and error handling.
*   **Complexity Tier**: Medium (inferred from `semi_auto` classification and the script's orchestration role)
*   **Automation Bucket**: semi_auto (final_rate: 0.65)
*   **Migration Flags**: None explicitly identified, but the semi-automated status suggests the need for some manual intervention or redesign for optimal BigQuery implementation.

## 3. Target Architecture
The migrated job will leverage BigQuery's native capabilities for data processing and orchestration.
*   **Orchestration**: The control flow and parameter handling logic of the KornShell script will be reimplemented as a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`). This stored procedure will manage the execution, parameter validation, error logging, and job status updates.
*   **Data Processing**: The logic embedded in the `d_ausd_v_ta_cntrct_crs2.sql` script (which is invoked by the ksh script) will be translated into a separate BigQuery Stored Procedure or a series of BigQuery DML/DDL statements. This will perform the actual data transformations and insertions into the target `ta_cntrct_crs2` table.
*   **Data Storage**:
    *   `ta_cntrct_crs2`: The target table for processed contract data in BigQuery.
    *   `job_table`: A BigQuery table to manage job status, activation, and deactivation.
    *   `job_error_log`: A BigQuery table for logging errors encountered during job execution.
    *   `job_run_audit`: A BigQuery table to store audit information, including the count of records processed.

## 4. Data Flow & Lineage
The original KornShell script `k_ausd_v_ta_cntrct_crs2.ksh` acts as an orchestrator for the SQL script `d_ausd_v_ta_cntrct_crs2.sql`.

1.  **Input**: The script accepts two primary parameters, `p_JobKennung` (Job Identifier) and `p_EintragsNr` (Entry Number).
2.  **Initialization & Utilities**: It sources several utility `.ksh` scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) which provide environment setup, error handling, date functions, parameter parsing, and SQL*Plus interaction capabilities.
3.  **Parameter Validation**: The script validates the presence of `p_JobKennung` and `p_EintragsNr`. If validation fails, it logs an error and exits.
4.  **Job Management (Implied)**: Based on the summary, it handles "active jobs" and "job-table entries," including deactivating older active jobs and registering the current job. This implies interaction with a job control table.
5.  **SQL Script Execution**: The script constructs the path to `d_ausd_v_ta_cntrct_crs2.sql` and then calls a function `starteSQLSkript` to execute this SQL file. This SQL script is the main data processing component.
6.  **Record Count & Audit**: After the SQL script execution, the KornShell script reads a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs2_$$.tmp`) to retrieve the count of processed records and prints a completion message.

**Target BigQuery Data Flow**:
*   A main BigQuery Stored Procedure (e.g., `r_ausd_vertrag_control`) will receive `p_JobKennung` and `p_EintragsNr` as input parameters.
*   It will perform parameter validation using BigQuery scripting `IF` statements.
*   Job management (deactivating/registering) will be handled via DML operations on the `job_table`.
*   The procedure will then invoke another BigQuery Stored Procedure (representing `d_ausd_v_ta_cntrct_crs2.sql`) which contains the core data transformation logic.
*   The record count will be captured using `@@row_count` or similar mechanisms and stored in the `job_run_audit` table.
*   Errors will be logged to the `job_error_log` table.

## 5. Transformation Logic
The `k_ausd_v_ta_cntrct_crs2.ksh` script's primary role is orchestration. The actual data transformation logic is delegated to the `d_ausd_v_ta_cntrct_crs2.sql` script.

**Orchestration Logic Migration (`k_ausd_v_ta_cntrct_crs2.ksh` -> BigQuery Stored Procedure):**
*   **Parameter Handling**: Shell `getopts` will be replaced by direct parameters to the BigQuery Stored Procedure.
*   **Environment Variables**: `$HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL` will be replaced by BigQuery Stored Procedure variables, configuration tables, or session variables.
*   **Utility Sourcing**: The functionality of sourced `*.ksh` files (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) will be reimplemented as BigQuery scripting logic or helper UDFs/procedures within BigQuery.
*   **Parameter Validation**: Shell `pruefeParameterGesetzt` and `if [ ! $ErrNr -eq 0 ]` logic will translate to BigQuery `IF` statements and `IS NULL` checks.
*   **Error Reporting**: `DWMSG_MeldeFehler` will be replaced by `INSERT` statements into a `job_error_log` table and `SIGNAL SQLSTATE` for signaling critical errors.
*   **Job Management**: The implied logic for job activation/deactivation will be implemented using BigQuery `UPDATE` and `INSERT` DML on the `job_table`.
*   **SQL Script Invocation**: The `starteSQLSkript` call will be replaced by a `CALL` statement to the BigQuery Stored Procedure that encapsulates the `d_ausd_v_ta_cntrct_crs2.sql` logic.
*   **Record Counting**: Reading from a temporary file (`cat $tmpFile`) will be replaced by using BigQuery's `@@row_count` after the core DML operation and inserting the result into a `job_run_audit` table.

**Core Data Transformation Logic (`d_ausd_v_ta_cntrct_crs2.sql`):**
*   This SQL script's content is not provided, but it is expected to contain the core business logic for processing data for `ta_cntrct_crs2`. This will need to be analyzed separately and converted from its original SQL dialect (likely Oracle SQL, given the context of `SQL*Plus`) to BigQuery Standard SQL.

## 6. External Dependencies
**Legacy System Dependencies:**
*   **Shell Utilities**: Dependence on various KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
*   **Environment Variables**: Reliance on `$HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL` for path resolution.
*   **Database**: Implicit dependency on an Oracle database, likely accessed via `SQL*Plus` through `h_alis_sqlplus.ksh` for executing `d_ausd_v_ta_cntrct_crs2.sql`.
*   **Filesystem**: Use of a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs2_$$.tmp`) for inter-process communication (record count).

**Target BigQuery Replacements:**
*   **Shell Utilities**: Their functionalities will be absorbed into the BigQuery Stored Procedure itself or by creating small, focused BigQuery helper procedures/UDFs.
*   **Environment Variables**: Replaced by BigQuery Stored Procedure parameters, dataset/project configuration, or dedicated configuration tables.
*   **Database**: BigQuery becomes the native data processing engine, eliminating the need for external database connections for core ETL.
*   **Filesystem (Temp Files)**: Replaced by BigQuery internal variables, temporary tables, or audit/logging tables for persistent storage of metadata like record counts.

## 7. Unresolved / Risks
*   **`d_ausd_v_ta_cntrct_crs2.sql` Content**: The most significant unresolved item is the actual SQL code within `d_ausd_v_ta_cntrct_crs2.sql`. Its complexity, SQL dialect (e.g., Oracle-specific constructs), and dependencies will dictate the effort required for its conversion to BigQuery Standard SQL.
*   **Job Table Definition**: The precise schema and business rules governing the legacy "job table" for `job_kennung` and `eintrags_nr` are unknown. These need to be defined accurately in BigQuery to ensure correct job lifecycle management.
*   **`starteSQLSkript` Functionality**: The full implementation details of `starteSQLSkript` (e.g., how it passes parameters to `SQL*Plus`, handles SQL errors, or interacts with the job table) are not fully explicit in the provided shell script. This will require further investigation during the migration of `h_alis_sqlplus.ksh` or direct implementation in BigQuery.
*   **Utility Script Logic**: While the shell script sources several utilities, their internal logic is not provided. Any complex logic within these utilities that directly impacts the control flow or data processing will need to be analyzed and reimplemented.

## 8. Build Plan
1.  **Schema Definition (BigQuery)**:
    *   Create the target `ta_cntrct_crs2` table schema.
    *   Define the `job_table` schema (e.g., `job_kennung`, `eintrags_nr`, `active_flag`, `created_at`, `updated_at`).
    *   Define the `job_error_log` table schema (e.g., `job_kennung`, `eintrags_nr`, `err_nr`, `err_arg`, `created_at`).
    *   Define the `job_run_audit` table schema (e.g., `job_kennung`, `eintrags_nr`, `tab_name`, `records_processed`, `created_at`).
2.  **Migrate `d_ausd_v_ta_cntrct_crs2.sql` (to BigQuery Stored Procedure)**:
    *   **Analyze**: Obtain and thoroughly analyze the source `d_ausd_v_ta_cntrct_crs2.sql` script.
    *   **Convert**: Translate the Oracle SQL (or other dialect) to BigQuery Standard SQL, addressing any proprietary functions or syntax.
    *   **Encapsulate**: Create a BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_v_ta_cntrct_crs2_sp`) to encapsulate this core data transformation logic.
3.  **Develop Orchestration Stored Procedure (BigQuery SQL)**:
    *   Create the main BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`).
    *   Implement parameter validation logic.
    *   Recreate job management logic (deactivating old jobs, registering current job) using DML against `job_table`.
    *   Integrate error logging into `job_error_log`.
    *   Add a `CALL` statement to invoke `project.dataset.d_ausd_v_ta_cntrct_crs2_sp`.
    *   Capture `@@row_count` after the transformation and insert into `job_run_audit`.
4.  **Testing**:
    *   **Unit Tests**: Test the parameter validation, job management, and error logging within `r_ausd_vertrag_control`.
    *   **Integration Tests**: Test the full flow, including the invocation and successful execution of `d_ausd_v_ta_cntrct_crs2_sp`, and correct audit logging.
    *   **Data Validation**: Verify that the `ta_cntrct_crs2` table is populated correctly and completely, matching legacy output.
5.  **Deployment**: Deploy the BigQuery tables and stored procedures to the target environment.
6.  **Scheduling**: Integrate the new BigQuery Stored Procedure into the target orchestrator (e.g., Airflow, Cloud Composer).