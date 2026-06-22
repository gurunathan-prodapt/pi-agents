# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_cntrct_crs3.ksh`. The original script acts as a control script for `r_ausd_vertrag.ksh`, responsible for managing job execution, parsing parameters, and orchestrating the execution of an underlying SQL script, `d_ausd_v_ta_cntrct_crs3.sql`. Its primary functions include ignoring already active jobs, invoking the SQL script, registering job details in a job table, and deactivating previously active jobs. The scope of this migration is to re-implement this control and orchestration logic in Google BigQuery, specifically leveraging BigQuery Stored Procedures, while ensuring the embedded SQL logic is also translated to BigQuery SQL.

## 2. Source Inventory
The job is comprised of a single KornShell script:

- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh`
  - **Technology**: KornShell
  - **Tool**: KornShell
  - **Summary**: This ksh script acts as a control script, handling job activation/deactivation, parsing parameters, and orchestrating the execution of an SQL script.
  - **Complexity Tier**: Not available (No rows returned from `file_complexity` table).
  - **Automation Bucket**: Not available (No rows returned from `automation_rate` table).

The script's primary role is orchestration, calling other utility shell scripts and a core SQL script.

## 3. Target Architecture
The migrated solution will primarily reside within Google BigQuery as a Stored Procedure.

- **BigQuery Stored Procedure**: `project.dataset.sp_ausd_v_ta_cntrct_crs3`
  - This stored procedure will encapsulate the parameter parsing, validation, job control logic, and the execution of the core business logic originally in `d_ausd_v_ta_cntrct_crs3.sql`.
  - Parameters `p_JobKennung` (STRING) and `p_EintragsNr` (STRING) will be passed directly to the stored procedure.
- **Job Control Table**: `project.dataset.job_table` (DDL to be created)
  - This table will manage job activation/deactivation, status updates, and history, replacing the shell script's implicit job management. Columns would include `job_name`, `entry_nr`, `tab_name`, `active_flag` (BOOLEAN), `created_ts`, `updated_ts`, `completed_ts`.
- **Error Logging Table**: `project.dataset.error_log` (DDL to be created)
  - A dedicated table to log errors, replacing the shell's `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` functionality. Columns would include `log_ts`, `procedure_name`, `err_nr`, `err_arg`, `message`.
- **Execution Logging Table**: `project.dataset.execution_log` (DDL to be created)
  - To record execution details, including `records_processed`, similar to how `v_records` was captured from a temporary file in the original script. Columns would include `log_ts`, `procedure_name`, `job_name`, `entry_nr`, `tab_name`, `records_processed`, `status`.
- **Core SQL Logic**: The SQL content of `d_ausd_v_ta_cntrct_crs3.sql` will be translated into BigQuery SQL and either inlined within `sp_ausd_v_ta_cntrct_crs3` or called as a separate BigQuery script/stored procedure.

## 4. Data Flow & Lineage
The original script's data flow involves:
1.  **Environment Setup**: Sourcing `.dw_init` and utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Input**: Command-line arguments `j` (JobKennung) and `f` (EintragsNr).
3.  **Parameter Validation**: Internal shell logic to check for mandatory parameters.
4.  **Job Control**: Implicit logic to manage job status (activate/deactivate), likely through the `starteSQLSkript` function and the SQL script.
5.  **SQL Script Execution**: Invokes `d_ausd_v_ta_cntrct_crs3.sql` via `starteSQLSkript`.
6.  **Record Count Capture**: Reads a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs3_$$.tmp`) into `v_records`.

In the BigQuery target architecture, this will be transformed to:
1.  **Stored Procedure Invocation**: `CALL project.dataset.sp_ausd_v_ta_cntrct_crs3('job_id_val', 'entry_nr_val')`.
2.  **Parameter Passing & Validation**: The `p_JobKennung` and `p_EintragsNr` parameters are passed directly to the SP and validated using BigQuery SQL `IF` statements.
3.  **Job Control**: `UPDATE` and `INSERT` statements against the `project.dataset.job_table` will manage job status.
4.  **Core SQL Logic Execution**: The translated BigQuery SQL from `d_ausd_v_ta_cntrct_crs3.sql` will be executed within or by the stored procedure, interacting with source and target tables (e.g., `ta_cntrct_crs3`).
5.  **Record Count**: The count of processed records will be captured directly via a `SELECT COUNT(*)` statement and stored in a `DECLARE` variable `v_records`, and logged to `execution_log`.
6.  **Error Handling**: Errors during validation or execution will be caught and logged to `project.dataset.error_log` using `INSERT` statements, and `RAISE USING MESSAGE` will be used for signaling.

## 5. Transformation Logic
The transformation logic involves converting shell script control flow and utility calls into BigQuery stored procedure constructs.

- **Shell Environment Variables**: These will be replaced by BigQuery stored procedure parameters (e.g., `p_JobKennung`, `p_EintragsNr`) or BigQuery project/dataset configurations.
- **`getopts` Parameter Parsing**: Replaced by direct parameter passing to the BigQuery Stored Procedure. Validation logic is replicated using `IF` statements.
- **Utility Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**:
  - `.dw_init`: Its function of setting up the environment will be handled by BigQuery's execution context and explicit parameter passing.
  - `f_alis_msgerr.ksh` (Error Handling): Replaced by `INSERT` into `project.dataset.error_log` and `RAISE USING MESSAGE`.
  - `h_alis_date.ksh` (Date Utilities): Replaced by BigQuery's built-in date functions (e.g., `CURRENT_TIMESTAMP()`).
  - `h_alis_parameter.ksh` (Parameter Utilities): Redundant, as parameters are directly passed.
  - `h_alis_sqlplus.ksh` (`starteSQLSkript`): The wrapper function will be replaced by direct BigQuery SQL execution within the stored procedure, or by calling another BigQuery routine.
- **Temporary File (`tmpFile`) for Record Count**: Replaced by a `DECLARE` variable (`v_records` INT64) and direct `COUNT(*)` in BigQuery SQL.
- **Job Control Logic**: The logic for activating/deactivating jobs will be translated into `UPDATE` statements on the `project.dataset.job_table`.
- **Core SQL Script (`d_ausd_v_ta_cntrct_crs3.sql`)**: This script's content will need to be translated from its current dialect (likely Oracle SQL or similar, given the `sqlplus` utility script) to BigQuery Standard SQL. This is a crucial sub-task of the migration. The table `v_TabName='ta_cntrct_crs3'` suggests `d_ausd_v_ta_cntrct_crs3.sql` operates on this table.

## 6. External Dependencies
The original script has the following dependencies:

- **Other KornShell Scripts**:
  - `$HOME/.dw_init`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
  - **Replacement**: All these will be replaced by BigQuery's native capabilities: environment configuration, BigQuery SQL for error logging, built-in date functions, direct parameter handling, and direct BigQuery SQL execution. No external shell script dependencies will remain in the BigQuery solution.
- **SQL Script**:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_crs3.sql`
  - **Replacement**: This script's content is a critical dependency and must be migrated to BigQuery Standard SQL. It will either be inlined into the `sp_ausd_v_ta_cntrct_crs3` stored procedure or implemented as a separate BigQuery stored procedure/script called by `sp_ausd_v_ta_cntrct_crs3`.
- **Temporary File System**:
  - `$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs3_$$.tmp`
  - **Replacement**: The need for temporary files will be eliminated by using BigQuery `DECLARE` variables within the stored procedure or by direct table operations.

No `lineage_external_systems` were identified for this job.

## 7. Unresolved / Risks
- **Missing File Complexity and Automation Rate Data**: The `file_complexity` and `automation_rate` tables did not return data for the source file. This means there's no automated assessment of the script's migration effort or recommended bucket (e.g., auto, semi-auto, manual). The current design relies on manual analysis.
- **`d_ausd_v_ta_cntrct_crs3.sql` Content**: The core business logic resides in this external SQL script, which was not provided. Its complexity and specific SQL dialect (e.g., Oracle PL/SQL, T-SQL) will heavily influence the migration effort. It is assumed that this script will be independently migrated to BigQuery Standard SQL.
- **`r_ausd_vertrag.ksh` Context**: The summary states the script is a control script *for* `r_ausd_vertrag.ksh`. The role and interaction with this parent script are not fully detailed, but given `k_ausd_v_ta_cntrct_crs3.ksh` is the seed, we focus on its internal logic first. If `r_ausd_vertrag.ksh` is part of the same job, it would imply further orchestration.
- **Job Control Table Details**: The exact schema and existing data in the legacy "job table" are unknown. A BigQuery DDL for `job_table` must be created to accurately reflect its structure and ensure data consistency during migration.
- **Error Number Mapping**: The error numbers (e.g., 192, 193) used in the KornShell script are specific to the legacy system. These should be mapped to an appropriate BigQuery error handling or logging convention.

## 8. Build Plan
The build plan will focus on creating the necessary BigQuery assets.

1.  **DDL for BigQuery Control Tables**:
    -   Create DDL for `project.dataset.job_table`.
    -   Create DDL for `project.dataset.error_log`.
    -   Create DDL for `project.dataset.execution_log`.
2.  **Migrate Core SQL Logic**:
    -   Translate the contents of `d_ausd_v_ta_cntrct_crs3.sql` to BigQuery Standard SQL.
    -   Identify the target table(s) (e.g., `ta_cntrct_crs3`) and ensure their DDLs exist in BigQuery.
    -   This could result in a separate BigQuery script or another stored procedure, depending on its complexity.
3.  **BigQuery Stored Procedure Development**:
    -   Develop `project.dataset.sp_ausd_v_ta_cntrct_crs3` in BigQuery Standard SQL.
    -   Implement parameter validation logic.
    -   Integrate job control (update `job_table`) at the beginning and end of the procedure.
    -   Incorporate error logging logic (insert into `error_log`).
    -   Embed or call the migrated SQL logic from `d_ausd_v_ta_cntrct_crs3.sql`.
    -   Implement record counting and execution logging (insert into `execution_log`).
4.  **Testing**:
    -   Unit test the `sp_ausd_v_ta_cntrct_crs3` stored procedure with various valid and invalid parameters.
    -   Integrate test to ensure correct interaction with `job_table`, `error_log`, and `execution_log`.
    -   Validate data produced by the migrated SQL logic against the legacy system's output.
5.  **Orchestration Integration**:
    -   If an external orchestrator (e.g., Airflow) is used, define the DAG to call `sp_ausd_v_ta_cntrct_crs3` with the appropriate parameters.

**Language**: BigQuery Standard SQL for all BigQuery components.